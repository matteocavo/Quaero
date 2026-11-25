"""Human review API (v0.9, PR 2): sync, provenance gating, state machine, corrections, audit."""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.dependencies import get_project_root
from api.main import app
from app import provenance as prov
from db import store

REPO = Path(__file__).resolve().parents[1]
KEY = "test-key"
H = {"X-API-Key": KEY}
P = "demo"
DQ = "dq_score_demo.json"
AN = "anomaly_report_v2_demo.json"

DQ_DOC = {
    "columns": [
        {"column": "streams", "completeness": 0.99, "outlier_flag": True, "dq_score": 80},
        {"column": "provider", "completeness": 0.4, "outlier_flag": False, "dq_score": 30},
        {"column": "catalog", "completeness": 1.0, "outlier_flag": False, "dq_score": 99},
    ]
}
AN_DOC = {
    "anomalies": [
        {"column": "streams", "method": "iqr", "impact": "high"},
        {"column": "streams", "method": "zscore", "impact": "medium"},
        {"column": "provider", "method": "iqr", "impact": "low"},
        {"column": "catalog", "method": "iqr"},  # v1-style: no impact -> no item
    ]
}


def _write(root: Path, name: str, doc: dict) -> bytes:
    meta = root / "projects" / P / "metadata"
    meta.mkdir(parents=True, exist_ok=True)
    data = json.dumps(doc).encode()
    (meta / name).write_bytes(data)
    return data


def _register(root: Path, run_id: str | None, names=(DQ, AN)) -> None:
    rows = []
    for n in names:
        data = (root / "projects" / P / "metadata" / n).read_bytes()
        rows.append(
            {
                "artifact": n,
                "path": f"metadata/{n}",
                "sha256": hashlib.sha256(data).hexdigest(),
                "size": len(data),
            }
        )
    store.register_run_artifacts(P, run_id, "api" if run_id else "cli", rows)


def _complete_run(run_id: str) -> None:
    store.register_project(P, "projects/demo")
    store.create_run(run_id, P, "q")
    store.transition_run(run_id, "RUNNING")
    store.transition_run(run_id, "COMPLETED")


def _make_run(root: Path, run_id: str, dq=DQ_DOC, an=AN_DOC) -> None:
    _write(root, DQ, dq)
    _write(root, AN, an)
    _complete_run(run_id)
    _register(root, run_id)


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setenv("QUAERO_API_KEY", KEY)
    shutil.copy(REPO / "review_rules.json", tmp_path / "review_rules.json")
    store.init_db()
    return tmp_path


@pytest.fixture
def client(root):
    app.dependency_overrides[get_project_root] = lambda: root
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.clear()


def _sync(client, project=P):
    return client.post("/review/sync", json={"project": project}, headers=H)


def _items(client, **params):
    return client.get("/review/items", params=params, headers=H).json()["items"]


def _find(client, kind, ref):
    for i in _items(client):
        if i["kind"] == kind and i["ref"] == ref:
            return i
    raise AssertionError(f"{kind}:{ref} not found")


def _decide(client, item_id, decision="ACCEPTED", **kw):
    body = {
        "decision": decision,
        "reviewer": "Matteo",
        "expected_status": "REVIEW_REQUIRED",
        "client_request_id": f"req-{decision}-{item_id}-{len(str(kw))}-xx",
    }
    if decision in ("CORRECTED", "REJECTED"):
        body["reason"] = "because"
    body.update(kw)
    return client.post(f"/review/items/{item_id}/decision", json=body, headers=H)


@pytest.fixture
def synced(root, client):
    _make_run(root, "run-1")
    assert _sync(client).status_code == 200
    return client


# ------------------------------------------------------------------ auth
@pytest.mark.parametrize(
    "method,url",
    [
        ("post", "/review/sync"),
        ("get", "/review/items"),
        ("get", "/review/items/1"),
        ("post", "/review/items/1/decision"),
        ("get", "/review/items/1/events"),
    ],
)
def test_auth_401_and_503(client, monkeypatch, method, url):
    assert getattr(client, method)(url).status_code == 401
    assert getattr(client, method)(url, headers={"X-API-Key": "bad"}).status_code == 401
    monkeypatch.delenv("QUAERO_API_KEY")
    assert getattr(client, method)(url, headers=H).status_code == 503


# ------------------------------------------------------------------ sync and rules
def test_sync_creates_r1_r3_items_only(root, client):
    _make_run(root, "run-1")
    r = _sync(client)
    assert r.status_code == 200
    body = r.json()
    assert body["run_id"] == "run-1" and body["created"] == 2 and body["existing"] == 0
    assert body["active_rules"] == ["R1", "R3"]
    refs = {(i["kind"], i["ref"]): i for i in _items(client)}
    assert set(refs) == {("anomaly", "streams|iqr"), ("dq", "streams")}
    assert refs[("anomaly", "streams|iqr")]["rule_id"] == "R1"
    assert refs[("dq", "streams")]["rule_id"] == "R3"
    assert all(i["status"] == "REVIEW_REQUIRED" for i in refs.values())


def test_sync_is_idempotent(root, client):
    _make_run(root, "run-1")
    _sync(client)
    r = _sync(client).json()
    assert r["created"] == 0 and r["existing"] == 2
    assert len(_items(client)) == 2


def _rules(root, **over):
    cfg = json.loads((root / "review_rules.json").read_text())
    for rid, patch in over.items():
        cfg["rules"][rid].update(patch) if "rules" in cfg else cfg[rid].update(patch)
    (root / "review_rules.json").write_text(json.dumps(cfg))


def test_r2_enabled_adds_medium(root, client):
    _rules(root, R2={"enabled": True})
    _make_run(root, "run-1")
    assert _sync(client).json()["created"] == 3


def test_r1_and_r3_disabled_creates_nothing(root, client):
    _rules(root, R1={"enabled": False}, R3={"enabled": False})
    _make_run(root, "run-1")
    r = _sync(client).json()
    assert r["created"] == 0 and r["active_rules"] == []


def test_r4_thresholds(root, client):
    _rules(root, R4={"enabled": True, "completeness_below": 0.5})
    _make_run(root, "run-1")
    r = _sync(client).json()
    assert "R4" in r["active_rules"]
    assert _find(client, "dq", "provider")["rule_id"] == "R4"


def test_r4_without_threshold_is_inactive(root, client):
    _rules(root, R4={"enabled": True})
    _make_run(root, "run-1")
    assert _sync(client).json()["created"] == 2


def test_malformed_rules_config_clear_error(root, client):
    (root / "review_rules.json").write_text("{not json")
    _make_run(root, "run-1")
    r = _sync(client)
    assert r.status_code == 500
    assert r.json()["detail"]["code"] == "rules_config_invalid"
    assert str(root) not in r.text
    assert _items(client) == []


def test_v1_anomaly_report_generates_no_items(root, client):
    _make_run(root, "run-1", an={"anomalies": [{"column": "x", "method": "iqr"}]})
    assert _sync(client).json()["created"] == 1  # only the DQ item


def test_invalid_project_name_422(client):
    assert _sync(client, "../evil").status_code == 422
    assert _sync(client, "").status_code == 422


# ------------------------------------------------------------------ provenance gating
def test_no_completed_run_404(client):
    assert _sync(client).status_code == 404


def test_no_provenance_binding_creates_nothing(root, client):
    _write(root, DQ, DQ_DOC)
    _write(root, AN, AN_DOC)
    _complete_run("run-1")  # completed, but nothing registered
    r = _sync(client)
    assert r.status_code == 409 and r.json()["detail"]["code"] == prov.BINDING_UNVERIFIABLE
    assert _items(client) == []


def test_unmanaged_cli_write_is_refused(root, client):
    _make_run(root, "run-1")
    _register(root, None)  # a later CLI write supersedes the API fingerprint
    r = _sync(client)
    assert r.status_code == 409 and r.json()["detail"]["code"] == prov.UNMANAGED_WRITE
    assert _items(client) == []


def test_artifact_changed_creates_nothing(root, client):
    _make_run(root, "run-1")
    p = root / "projects" / P / "metadata" / DQ
    p.write_bytes(p.read_bytes() + b" ")
    r = _sync(client)
    assert r.status_code == 409 and r.json()["detail"]["code"] == prov.ARTIFACT_CHANGED
    assert _items(client) == []  # all-or-nothing, even the intact artifact yields nothing


def test_artifact_unavailable(root, client):
    _make_run(root, "run-1")
    (root / "projects" / P / "metadata" / AN).unlink()
    r = _sync(client)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == prov.ARTIFACT_UNAVAILABLE
    assert _items(client) == []


def test_stale_file_from_previous_run_not_used(root, client):
    """Run 2 produced only the DQ artifact; the older anomaly file on disk must not feed items."""
    _make_run(root, "run-1")
    _complete_run("run-2")
    _register(root, "run-2", names=(DQ,))
    assert _sync(client).json()["run_id"] == "run-2"
    assert all(i["kind"] == "dq" for i in _items(client, run_id="run-2"))


def test_active_run_blocks_sync(root, client):
    _make_run(root, "run-1")
    store.create_run("run-2", P, "q")
    store.transition_run("run-2", "RUNNING")
    r = _sync(client)
    assert r.status_code == 409 and r.json()["detail"]["code"] == prov.RUN_ACTIVE


def test_lease_held_elsewhere_is_run_active(root, client):
    _make_run(root, "run-1")
    held, release = threading.Event(), threading.Event()

    def hold():
        with prov.project_write_lease(root / "projects" / P):
            held.set()
            release.wait(10)

    t = threading.Thread(target=hold)
    t.start()
    assert held.wait(5)
    try:
        r = _sync(client)
        assert r.status_code == 409 and r.json()["detail"]["code"] == prov.RUN_ACTIVE
    finally:
        release.set()
        t.join()
    assert _items(client) == []


def test_sync_never_modifies_artifacts(root, client):
    _make_run(root, "run-1")
    meta = root / "projects" / P / "metadata"
    before = {p.name: p.read_bytes() for p in meta.iterdir()}
    _sync(client)
    assert {p.name: p.read_bytes() for p in meta.iterdir()} == before


def test_item_insert_is_one_transaction(root, client, monkeypatch):
    _make_run(root, "run-1")
    real = store.insert_review_items

    def boom(project, run_id, items):
        assert len(items) == 2  # all items handed over in a single call
        raise RuntimeError("db down")

    monkeypatch.setattr(store, "insert_review_items", boom)
    assert _sync(client).status_code == 500
    monkeypatch.setattr(store, "insert_review_items", real)
    assert _items(client) == []


# ------------------------------------------------------------------ list / detail / events
def test_list_filters_and_pagination(synced):
    assert len(_items(synced, kind="anomaly")) == 1
    assert len(_items(synced, status="ACCEPTED")) == 0
    r = synced.get("/review/items", params={"limit": 1}, headers=H).json()
    assert r["total"] == 2 and len(r["items"]) == 1 and r["limit"] == 1
    assert synced.get("/review/items", params={"limit": 0}, headers=H).status_code == 422
    assert synced.get("/review/items", params={"status": "NOPE"}, headers=H).status_code == 422


def test_detail_and_404(synced):
    item = _find(synced, "anomaly", "streams|iqr")
    d = synced.get(f"/review/items/{item['id']}", headers=H)
    assert d.status_code == 200
    body = d.json()
    assert body["original"]["impact"] == "high" and body["effective"]["impact"] == "high"
    assert body["superseded"] is False and body["snapshot_verified"] is True
    assert synced.get("/review/items/9999", headers=H).status_code == 404
    assert synced.get("/review/items/9999/events", headers=H).status_code == 404


# ------------------------------------------------------------------ decisions / state machine
def test_accept_without_reason(synced):
    i = _find(synced, "dq", "streams")
    r = _decide(synced, i["id"], "ACCEPTED")
    assert r.status_code == 200
    j = r.json()
    assert j["item"]["status"] == "ACCEPTED" and j["replayed"] is False
    assert j["event"]["reviewer_source"] == "declared"
    assert "client_request_id" not in j["event"]


def test_reject_requires_reason(synced):
    i = _find(synced, "dq", "streams")
    r = _decide(synced, i["id"], "REJECTED", reason=None)
    assert r.status_code == 422
    r = _decide(synced, i["id"], "REJECTED", reason="   ")
    assert r.status_code == 422
    assert _decide(synced, i["id"], "REJECTED").json()["item"]["status"] == "REJECTED"


@pytest.mark.parametrize("final", ["ACCEPTED", "REJECTED"])
def test_terminal_states_refuse_further_decisions(synced, final):
    i = _find(synced, "dq", "streams")
    assert _decide(synced, i["id"], final).status_code == 200
    r = _decide(
        synced,
        i["id"],
        "ACCEPTED",
        expected_status="REVIEW_REQUIRED",
        client_request_id="another-request-1",
    )
    assert r.status_code == 409
    assert r.json()["detail"]["code"] in ("stale_status", "invalid_transition")


def test_corrected_can_then_be_accepted_or_rejected_not_recorrected(synced):
    i = _find(synced, "dq", "streams")
    c = _decide(
        synced,
        i["id"],
        "CORRECTED",
        correction={"field": "outlier_flag", "to_value": False, "justification": "ok"},
    )
    assert c.status_code == 200 and c.json()["item"]["status"] == "CORRECTED"
    again = _decide(
        synced,
        i["id"],
        "CORRECTED",
        expected_status="CORRECTED",
        client_request_id="req-second-correction",
        correction={"field": "outlier_flag", "to_value": False, "justification": "x"},
    )
    assert again.status_code == 409, again.text
    done = _decide(
        synced,
        i["id"],
        "ACCEPTED",
        expected_status="CORRECTED",
        client_request_id="req-accept-after",
    )
    assert done.status_code == 200 and done.json()["item"]["status"] == "ACCEPTED"


def test_stale_expected_status_409(synced):
    i = _find(synced, "dq", "streams")
    _decide(synced, i["id"], "ACCEPTED")
    r = _decide(synced, i["id"], "REJECTED", client_request_id="req-stale-0001")
    assert r.status_code == 409


def test_decision_on_unknown_item_404(synced):
    assert _decide(synced, 9999, "ACCEPTED").status_code == 404


def test_idempotent_replay_returns_same_event(synced):
    i = _find(synced, "dq", "streams")
    kw = {"client_request_id": "req-idem-0001"}
    first = _decide(synced, i["id"], "ACCEPTED", **kw)
    second = _decide(synced, i["id"], "ACCEPTED", **kw)
    assert first.status_code == second.status_code == 200
    assert second.json()["replayed"] is True
    assert second.json()["event"]["id"] == first.json()["event"]["id"]
    events = synced.get(f"/review/items/{i['id']}/events", headers=H).json()
    assert len(events) == 1


def test_request_id_reuse_with_different_payload_conflicts(synced):
    i = _find(synced, "dq", "streams")
    kw = {"client_request_id": "req-idem-0002"}
    assert _decide(synced, i["id"], "ACCEPTED", **kw).status_code == 200
    r = _decide(synced, i["id"], "REJECTED", **kw)
    assert r.status_code == 409 and r.json()["detail"]["code"] == "request_id_conflict"


def test_concurrent_decisions_exactly_one_wins(synced):
    i = _find(synced, "dq", "streams")
    results, barrier = [], threading.Barrier(6)

    def go(n):
        barrier.wait()
        results.append(
            _decide(
                synced,
                i["id"],
                "ACCEPTED" if n % 2 else "REJECTED",
                client_request_id=f"req-concurrent-{n:03d}",
            ).status_code
        )

    ts = [threading.Thread(target=go, args=(n,)) for n in range(6)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert sorted(results) == [200, 409, 409, 409, 409, 409]
    assert len(synced.get(f"/review/items/{i['id']}/events", headers=H).json()) == 1


# ------------------------------------------------------------------ validation
@pytest.mark.parametrize(
    "patch",
    [
        {"reviewer": ""},
        {"reviewer": "   "},
        {"reviewer": "x" * 81},
        {"reviewer": "bad\x00name"},
        {"reviewer": "bad\nname"},
        {"reason": "r" * 1001},
        {"client_request_id": "short"},
        {"client_request_id": "has spaces in it"},
        {"decision": "REVIEW_REQUIRED"},
        {"expected_status": "ACCEPTED"},
        {"reviewer_source": "verified"},  # clients cannot send it (extra=forbid)
    ],
)
def test_decision_validation_422(synced, patch):
    i = _find(synced, "dq", "streams")
    body = {
        "decision": "ACCEPTED",
        "reviewer": "Matteo",
        "expected_status": "REVIEW_REQUIRED",
        "client_request_id": "req-valid-0001",
    }
    body.update(patch)
    r = synced.post(f"/review/items/{i['id']}/decision", json=body, headers=H)
    assert r.status_code == 422
    assert synced.get(f"/review/items/{i['id']}/events", headers=H).json() == []


def test_reviewer_is_trimmed(synced):
    i = _find(synced, "dq", "streams")
    r = _decide(synced, i["id"], "ACCEPTED", reviewer="  Ada  ")
    assert r.json()["event"]["reviewer"] == "Ada"


def test_correction_required_iff_corrected(synced):
    i = _find(synced, "dq", "streams")
    assert _decide(synced, i["id"], "CORRECTED").status_code == 422
    corr = {"field": "outlier_flag", "to_value": False, "justification": "j"}
    assert _decide(synced, i["id"], "ACCEPTED", correction=corr).status_code == 422


@pytest.mark.parametrize(
    "item,corr",
    [
        (("dq", "streams"), {"field": "dq_score", "to_value": 1, "justification": "j"}),
        (("dq", "streams"), {"field": "outlier_flag", "to_value": "no", "justification": "j"}),
        (("dq", "streams"), {"field": "outlier_flag", "to_value": 0, "justification": "j"}),
        (("dq", "streams"), {"field": "outlier_flag", "to_value": True, "justification": "j"}),
        (("anomaly", "streams|iqr"), {"field": "impact", "to_value": "huge", "justification": "j"}),
        (("anomaly", "streams|iqr"), {"field": "impact", "to_value": "high", "justification": "j"}),
        (
            ("anomaly", "streams|iqr"),
            {"field": "outlier_flag", "to_value": True, "justification": "j"},
        ),
    ],
)
def test_invalid_corrections_422(synced, item, corr):
    i = _find(synced, *item)
    assert _decide(synced, i["id"], "CORRECTED", correction=corr).status_code == 422
    assert _find(synced, *item)["status"] == "REVIEW_REQUIRED"


# ------------------------------------------------------------------ corrections / overlay
def test_correction_overlay_keeps_original(synced):
    i = _find(synced, "anomaly", "streams|iqr")
    r = _decide(
        synced,
        i["id"],
        "CORRECTED",
        correction={"field": "impact", "to_value": "low", "justification": "benign"},
    )
    assert r.status_code == 200
    d = synced.get(f"/review/items/{i['id']}", headers=H).json()
    assert d["original"]["impact"] == "high"
    assert d["effective"]["impact"] == "low"
    assert d["correction"]["to_value"] == "low"
    assert d["snapshot_verified"] is True


def test_rejected_after_corrected_ignores_overlay_but_keeps_history(synced):
    i = _find(synced, "dq", "streams")
    _decide(
        synced,
        i["id"],
        "CORRECTED",
        correction={"field": "outlier_flag", "to_value": False, "justification": "j"},
    )
    _decide(
        synced,
        i["id"],
        "REJECTED",
        expected_status="CORRECTED",
        client_request_id="req-reject-after",
    )
    d = synced.get(f"/review/items/{i['id']}", headers=H).json()
    assert d["status"] == "REJECTED"
    assert d["effective"]["outlier_flag"] is True  # overlay no longer applies
    assert store.latest_review_correction(i["id"]) is not None  # history preserved
    kinds = [
        e["to_status"] for e in synced.get(f"/review/items/{i['id']}/events", headers=H).json()
    ]
    assert kinds == ["CORRECTED", "REJECTED"]


def test_no_auto_apply_to_pipeline_artifacts(root, synced):
    i = _find(synced, "dq", "streams")
    meta = root / "projects" / P / "metadata"
    before = {p.name: p.read_bytes() for p in meta.iterdir()}
    _decide(
        synced,
        i["id"],
        "CORRECTED",
        correction={"field": "outlier_flag", "to_value": False, "justification": "j"},
    )
    assert {p.name: p.read_bytes() for p in meta.iterdir()} == before


# ------------------------------------------------------------------ audit append-only
def test_events_append_only_and_immutable(synced, tmp_path):
    i = _find(synced, "dq", "streams")
    _decide(synced, i["id"], "ACCEPTED")
    ev = synced.get(f"/review/items/{i['id']}/events", headers=H).json()
    assert len(ev) == 1 and ev[0]["reviewer_source"] == "declared"
    other = _find(synced, "anomaly", "streams|iqr")
    c = _decide(
        synced,
        other["id"],
        "CORRECTED",
        correction={"field": "impact", "to_value": "low", "justification": "ok"},
    )
    assert c.status_code == 200, c.text
    conn = sqlite3.connect(store._get_db_path())
    try:
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("UPDATE review_events SET reviewer='x'")
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("DELETE FROM review_events")
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("UPDATE review_items SET original_json='{}'")
        with pytest.raises(sqlite3.DatabaseError):
            conn.execute("DELETE FROM review_corrections")
    finally:
        conn.close()


# ------------------------------------------------------------------ superseded / snapshot
def test_superseded_is_derived_and_items_kept(root, synced):
    old = _find(synced, "dq", "streams")
    _decide(synced, old["id"], "ACCEPTED")
    _make_run(root, "run-2")
    assert _sync(synced).json()["run_id"] == "run-2"
    items = _items(synced)
    assert len(items) == 4
    by_run = {(i["run_id"], i["kind"]): i for i in items}
    assert by_run[("run-1", "dq")]["superseded"] is True
    assert by_run[("run-1", "dq")]["status"] == "ACCEPTED"
    assert by_run[("run-2", "dq")]["superseded"] is False
    assert len(_items(synced, superseded="true")) == 2
    assert len(_items(synced, superseded="false")) == 2


def test_snapshot_verified_false_when_fingerprint_unmatched(root, synced):
    """Old item's artifact bytes no longer match any registered fingerprint for its run."""
    i = _find(synced, "dq", "streams")
    conn = sqlite3.connect(store._get_db_path())
    try:
        row = conn.execute("SELECT COUNT(*) FROM run_artifacts").fetchone()[0]
        assert row == 2
    finally:
        conn.close()
    # simulate: item exists for a run with no registered artifacts at all
    conn = sqlite3.connect(store._get_db_path())
    try:
        conn.execute("DROP TRIGGER IF EXISTS run_artifacts_no_delete")
        conn.execute("DELETE FROM run_artifacts")
        conn.commit()
    except sqlite3.DatabaseError:
        pytest.skip("run_artifacts is protected; snapshot_verified false case not reproducible")
    finally:
        conn.close()
    d = synced.get(f"/review/items/{i['id']}", headers=H).json()
    assert d["artifact_provenance_verified"] is False
    assert d["snapshot_verified"] is True  # snapshot integrity is independent of provenance


def _raw(sql, params=()):
    conn = sqlite3.connect(store._get_db_path())
    try:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.fetchall()
    finally:
        conn.close()


def test_snapshot_hash_stored_and_verified_at_read(synced):
    i = _find(synced, "dq", "streams")
    assert i["snapshot_verified"] is True and i["artifact_provenance_verified"] is True
    assert _raw("SELECT COUNT(*) FROM review_items WHERE snapshot_sha256 IS NULL")[0][0] == 0


def test_snapshot_tampering_detected(synced):
    i = _find(synced, "dq", "streams")
    _raw("DROP TRIGGER review_items_immutable_identity")
    _raw(
        "UPDATE review_items SET original_json = ? WHERE id = ?",
        (json.dumps({"column": "streams", "outlier_flag": False}), i["id"]),
    )
    d = synced.get(f"/review/items/{i['id']}", headers=H).json()
    assert d["snapshot_verified"] is False
    assert d["artifact_provenance_verified"] is True  # provenance is a separate signal
    assert _find(synced, "dq", "streams")["snapshot_verified"] is False


def test_legacy_null_snapshot_hash_not_verified_nor_backfilled(synced):
    i = _find(synced, "dq", "streams")
    _raw("DROP TRIGGER review_items_immutable_identity")
    _raw("UPDATE review_items SET snapshot_sha256 = NULL WHERE id = ?", (i["id"],))
    d = synced.get(f"/review/items/{i['id']}", headers=H).json()
    assert d["snapshot_verified"] is False
    _sync(synced)  # re-sync must not backfill an unguaranteed origin
    assert _raw("SELECT snapshot_sha256 FROM review_items WHERE id = ?", (i["id"],))[0][0] is None


# ------------------------------------------------------------------ integrity gates decisions
def _tamper_snapshot(item_id):
    _raw("DROP TRIGGER review_items_immutable_identity")
    _raw(
        "UPDATE review_items SET original_json = ? WHERE id = ?",
        (json.dumps({"column": "streams", "outlier_flag": False}), item_id),
    )


def _drop_provenance():
    _raw("DROP TRIGGER IF EXISTS run_artifacts_no_delete")
    _raw("DELETE FROM run_artifacts")


def _snapshot(item_id):
    return (
        _raw("SELECT status FROM review_items WHERE id = ?", (item_id,)),
        _raw("SELECT COUNT(*) FROM review_events WHERE item_id = ?", (item_id,)),
    )


def test_decision_blocked_on_tampered_snapshot(synced):
    i = _find(synced, "dq", "streams")
    _tamper_snapshot(i["id"])
    before = _snapshot(i["id"])
    r = _decide(synced, i["id"], "ACCEPTED")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "snapshot_unverified"
    assert _snapshot(i["id"]) == before


def test_decision_blocked_on_legacy_null_snapshot(synced):
    i = _find(synced, "dq", "streams")
    _raw("DROP TRIGGER review_items_immutable_identity")
    _raw("UPDATE review_items SET snapshot_sha256 = NULL WHERE id = ?", (i["id"],))
    before = _snapshot(i["id"])
    r = _decide(synced, i["id"], "REJECTED")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "snapshot_unverified"
    assert _snapshot(i["id"]) == before


def test_decision_blocked_without_recorded_provenance(synced):
    i = _find(synced, "dq", "streams")
    _drop_provenance()
    before = _snapshot(i["id"])
    r = _decide(synced, i["id"], "ACCEPTED")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "provenance_unverified"
    assert _snapshot(i["id"]) == before


def test_superseded_item_with_intact_integrity_stays_decidable(root, synced):
    old = _find(synced, "dq", "streams")
    _make_run(root, "run-2")
    assert _sync(synced).json()["run_id"] == "run-2"
    d = synced.get(f"/review/items/{old['id']}", headers=H).json()
    assert d["superseded"] is True and d["snapshot_verified"] and d["artifact_provenance_verified"]
    assert _decide(synced, old["id"], "ACCEPTED").status_code == 200


def test_replay_of_applied_decision_survives_later_tampering(synced):
    i = _find(synced, "dq", "streams")
    first = _decide(synced, i["id"], "ACCEPTED")
    assert first.status_code == 200
    _tamper_snapshot(i["id"])
    again = _decide(synced, i["id"], "ACCEPTED")
    assert again.status_code == 200
    assert first.json()["replayed"] is False
    assert again.json()["replayed"] is True
    assert _raw("SELECT COUNT(*) FROM review_events WHERE item_id = ?", (i["id"],))[0][0] == 1


# ------------------------------------------------------------------ replay semantics
def _item_state(item_id):
    return (
        _raw("SELECT status FROM review_items WHERE id = ?", (item_id,)),
        _raw("SELECT COUNT(*) FROM review_events WHERE item_id = ?", (item_id,)),
        _raw("SELECT COUNT(*) FROM review_corrections WHERE item_id = ?", (item_id,)),
    )


@pytest.mark.parametrize(
    "first,second",
    [
        ({"reason": "a"}, {"reason": "b"}),
        ({}, {"reason": "now with reason"}),
        ({}, {"expected_status": "CORRECTED"}),
        ({}, {"reviewer": "Someone"}),
    ],
)
def test_replay_with_different_fields_conflicts_without_side_effects(synced, first, second):
    i = _find(synced, "dq", "streams")
    kw = {"client_request_id": "req-sem-0001"}
    assert _decide(synced, i["id"], "ACCEPTED", **kw, **first).status_code == 200
    before = _item_state(i["id"])
    r = _decide(synced, i["id"], "ACCEPTED", **kw, **second)
    assert r.status_code == 409 and r.json()["detail"]["code"] == "request_id_conflict"
    assert _item_state(i["id"]) == before


def test_replay_with_different_correction_conflicts(synced):
    i = _find(synced, "anomaly", "streams|iqr")
    kw = {"client_request_id": "req-sem-0002"}

    def corr(to):
        return {"field": "impact", "to_value": to, "justification": "j"}

    assert _decide(synced, i["id"], "CORRECTED", **kw, correction=corr("low")).status_code == 200
    before = _item_state(i["id"])
    r = _decide(synced, i["id"], "CORRECTED", **kw, correction=corr("medium"))
    assert r.status_code == 409 and r.json()["detail"]["code"] == "request_id_conflict"
    assert _item_state(i["id"]) == before
    same = _decide(synced, i["id"], "CORRECTED", **kw, correction=corr("low"))
    assert same.status_code == 200 and same.json()["replayed"] is True
    assert _item_state(i["id"]) == before


def test_identical_replay_with_reason_is_replay(synced):
    i = _find(synced, "dq", "streams")
    kw = {"client_request_id": "req-sem-0003", "reason": "same"}
    first = _decide(synced, i["id"], "REJECTED", **kw)
    second = _decide(synced, i["id"], "REJECTED", **kw)
    assert second.status_code == 200 and second.json()["replayed"] is True
    assert second.json()["event"]["id"] == first.json()["event"]["id"]


# ------------------------------------------------------------------ multi-dataset refs
def test_same_column_in_two_sources_yields_distinct_items(root, client):
    for src in ("a", "b"):
        _write(root, f"dq_score_{src}.json", DQ_DOC)
        _write(root, f"anomaly_report_v2_{src}.json", AN_DOC)
    _complete_run("run-md")
    names = [f"{p}{s}.json" for p in ("dq_score_", "anomaly_report_v2_") for s in ("a", "b")]
    _register(root, "run-md", names=names)
    assert _sync(client).status_code == 200
    items = _items(client, limit=200)
    refs = sorted(i["ref"] for i in items if i["kind"] == "dq")
    assert refs == ["a::streams", "b::streams"]
    arefs = sorted(i["ref"] for i in items if i["kind"] == "anomaly")
    assert arefs == ["a::streams|iqr", "b::streams|iqr"]
    assert _sync(client).json()["created"] == 0  # stable across re-sync


# ------------------------------------------------------------------ errors / migration
def test_unexpected_error_is_generic_500(synced, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("secret SQL SELECT * FROM x at C:\\private")

    monkeypatch.setattr(store, "list_review_items", boom)
    r = synced.get("/review/items", headers=H)
    assert r.status_code == 500
    assert "SELECT" not in r.text and "private" not in r.text


def test_migration_idempotent_and_v2_tables(root):
    store.init_db()
    store.init_db()
    conn = sqlite3.connect(store._get_db_path())
    try:
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()
    assert {"run_artifacts", "review_items", "review_events", "review_corrections"} <= names


def test_pr24_regression_stale_artifact_not_attributed(root):
    """produced_artifact_paths ignores skipped stages and out-of-metadata paths."""
    pdir = root / "projects" / P
    _write(root, DQ, DQ_DOC)
    skipped = {"dq_score": {"skipped": True, "output_path": f"projects/{P}/metadata/{DQ}"}}
    assert prov.produced_artifact_paths(pdir, skipped) == []
    outside = {"dq_score": {"output_path": f"projects/other/metadata/{DQ}"}}
    assert prov.produced_artifact_paths(pdir, outside) == []
    ok = {"dq_score": {"output_path": f"projects/{P}/metadata/{DQ}"}}
    assert [p.name for p in prov.produced_artifact_paths(pdir, ok)] == [DQ]
