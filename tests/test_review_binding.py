from __future__ import annotations

import hashlib
import json
import sqlite3

import pytest

from app import provenance as prov
from db import store

ART = "dq_score_demo.json"


@pytest.fixture
def proj(tmp_path):
    d = tmp_path / "projects" / "demo"
    (d / "metadata").mkdir(parents=True)
    (d / "metadata" / ART).write_text(json.dumps({"score": 1}), encoding="utf-8")
    (d / "metadata" / "anomaly_report_v2_demo.json").write_text("[]", encoding="utf-8")
    return d


def _rows():
    with store.get_connection() as c:
        return [dict(r) for r in c.execute("SELECT * FROM run_artifacts ORDER BY id")]


def test_record_registers_all_artifacts_atomically(proj):
    assert prov.record_artifacts(proj, "demo", "run-1") is True
    rows = _rows()
    assert {r["artifact"] for r in rows} == {ART, "anomaly_report_v2_demo.json"}
    assert all(r["writer"] == "api" and r["run_id"] == "run-1" for r in rows)
    data = (proj / "metadata" / ART).read_bytes()
    row = next(r for r in rows if r["artifact"] == ART)
    assert row["sha256"] == hashlib.sha256(data).hexdigest() and row["size"] == len(data)


def test_verified_returns_the_hashed_bytes(proj):
    prov.record_artifacts(proj, "demo", "run-1")
    res = prov.check_binding(proj, "demo", ART, "run-1")
    assert res.outcome == prov.VERIFIED
    assert res.data == (proj / "metadata" / ART).read_bytes()


def test_no_rows_is_unverifiable(proj):
    assert prov.check_binding(proj, "demo", ART, "run-1").outcome == prov.BINDING_UNVERIFIABLE


def test_uninitialised_db_fails_closed(proj, tmp_path, monkeypatch):
    monkeypatch.setenv("QUAERO_DB_PATH", str(tmp_path / "never_initialised.db"))
    assert prov.check_binding(proj, "demo", ART, "run-1").outcome == prov.BINDING_UNVERIFIABLE


def test_cli_write_after_api_is_unmanaged(proj):
    prov.record_artifacts(proj, "demo", "run-1")
    prov.record_artifacts(proj, "demo", None)
    assert store.latest_run_artifact("demo", ART)["run_id"] is None
    assert prov.check_binding(proj, "demo", ART, "run-1").outcome == prov.UNMANAGED_WRITE


def test_other_run_is_mismatch(proj):
    prov.record_artifacts(proj, "demo", "run-2")
    assert prov.check_binding(proj, "demo", ART, "run-1").outcome == prov.RUN_MISMATCH


def test_changed_artifact_detected(proj):
    prov.record_artifacts(proj, "demo", "run-1")
    (proj / "metadata" / ART).write_text("tampered", encoding="utf-8")
    assert prov.check_binding(proj, "demo", ART, "run-1").outcome == prov.ARTIFACT_CHANGED


def test_missing_artifact_unavailable(proj):
    prov.record_artifacts(proj, "demo", "run-1")
    (proj / "metadata" / ART).unlink()
    assert prov.check_binding(proj, "demo", ART, "run-1").outcome == prov.ARTIFACT_UNAVAILABLE


def test_edit_before_fingerprint_is_out_of_scope(proj):
    """Documented limit: a non-Quaero edit made BEFORE registration is fingerprinted as-is."""
    (proj / "metadata" / ART).write_text("edited-before", encoding="utf-8")
    prov.record_artifacts(proj, "demo", "run-1")
    assert prov.check_binding(proj, "demo", ART, "run-1").outcome == prov.VERIFIED


def test_run_active_while_lease_held(proj):
    prov.record_artifacts(proj, "demo", "run-1")
    import threading

    res = {}
    started, release = threading.Event(), threading.Event()

    def hold():
        with prov.project_write_lease(proj):
            started.set()
            release.wait(5)

    t = threading.Thread(target=hold)
    t.start()
    started.wait(5)
    try:
        res["r"] = prov.check_binding(proj, "demo", ART, "run-1")
    finally:
        release.set()
        t.join()
    assert res["r"].outcome == prov.RUN_ACTIVE


def test_partial_registration_never_visible(proj):
    bad = [
        {"artifact": "a", "path": "metadata/a", "sha256": "h", "size": 1},
        {"artifact": "b", "path": "metadata/b", "sha256": "h", "size": 1},
    ]
    store.init_db()
    with pytest.raises(sqlite3.IntegrityError):
        store.register_run_artifacts("demo", "r", "bogus-writer", bad)
    assert _rows() == []
    # second row invalid -> first must not persist either
    bad[1]["size"] = None
    with pytest.raises((TypeError, sqlite3.IntegrityError)):
        store.register_run_artifacts("demo", "r", "api", bad)
    assert _rows() == []


def test_registration_failure_never_raises(proj, monkeypatch):
    def boom(*a, **k):
        raise sqlite3.OperationalError("disk full")

    monkeypatch.setattr(store, "register_run_artifacts", boom)
    assert prov.record_artifacts(proj, "demo", "run-1") is False
    assert prov.check_binding(proj, "demo", ART, "run-1").outcome == prov.BINDING_UNVERIFIABLE
