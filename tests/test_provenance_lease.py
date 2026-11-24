from __future__ import annotations

import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from app import pipeline_runner as pr
from app import provenance as prov
from db import store

REPO = Path(__file__).resolve().parents[1]
CSV = "release_year,provider,catalog,streams\n2024,A,Pop,100\n2024,B,Pop,150\n2025,A,Rock,200\n2026,C,Rock,600\n"
ART = "dq_score_demo.json"


@pytest.fixture
def root(tmp_path):
    (tmp_path / "d.csv").write_text(CSV, encoding="utf-8")
    return tmp_path


def _run(root, run_id=None, name="Demo"):
    return pr.run_pipeline(
        source_path=root / "d.csv",
        source_name=name,
        question="Which release years generate the strongest average streams?",
        metric_column="streams",
        dimension_column="release_year",
        catalog_column="catalog",
        project_root=root,
        run_id=run_id,
    )


def _pdir(root):
    return root / "projects" / "demo"


def _fake_body(release, started, writes=None):
    """Replacement pipeline body that holds the lease until `release` is set."""

    def body(**kw):
        meta = _pdir(kw["project_root"]) / "metadata"
        meta.mkdir(parents=True, exist_ok=True)
        (meta / ART).write_text(writes or "api-output", encoding="utf-8")
        started.set()
        assert release.wait(10)
        return {}

    return body


# 1+2: entrypoints are serialised; CLI after API supersedes the API row
def test_cli_after_api_latest_row_is_unmanaged(root):
    _run(root, run_id="run-1")
    assert prov.check_binding(_pdir(root), "demo", ART, "run-1").outcome == prov.VERIFIED
    _run(root, run_id=None)
    assert store.latest_run_artifact("demo", ART)["run_id"] is None
    assert prov.check_binding(_pdir(root), "demo", ART, "run-1").outcome == prov.UNMANAGED_WRITE


# 1+3: a second entrypoint during a run fails fast; output and fingerprint stay the API's
def test_cli_during_api_refused_and_output_intact(root, monkeypatch):
    release, started = threading.Event(), threading.Event()
    monkeypatch.setattr(pr, "_run_pipeline_body", _fake_body(release, started))
    errors = []
    t = threading.Thread(target=lambda: _run(root, run_id="run-api"))
    t.start()
    assert started.wait(5)
    try:
        t0 = time.monotonic()
        with pytest.raises(prov.ProjectBusyError):
            _run(root, run_id=None)
        assert time.monotonic() - t0 < 2  # fail fast, never blocks
        assert (_pdir(root) / "metadata" / ART).read_text() == "api-output"
    finally:
        release.set()
        t.join()
    assert not errors
    assert prov.check_binding(_pdir(root), "demo", ART, "run-api").outcome == prov.VERIFIED


# 4: a writer blocked by the lease succeeds once it is released
def test_writer_succeeds_after_release(root, monkeypatch):
    release, started = threading.Event(), threading.Event()
    monkeypatch.setattr(pr, "_run_pipeline_body", _fake_body(release, started))
    t = threading.Thread(target=lambda: _run(root, run_id="run-1"))
    t.start()
    assert started.wait(5)
    with pytest.raises(prov.ProjectBusyError):
        _run(root)
    release.set()
    t.join()
    monkeypatch.undo()
    _run(root, run_id="run-2")
    assert prov.check_binding(_pdir(root), "demo", ART, "run-2").outcome == prov.VERIFIED


# 5: sync/verify vs running pipeline and vs another verifier
def test_verify_vs_running_pipeline_is_run_active(root, monkeypatch):
    _run(root, run_id="run-1")
    release, started = threading.Event(), threading.Event()
    monkeypatch.setattr(pr, "_run_pipeline_body", _fake_body(release, started))
    t = threading.Thread(target=lambda: _run(root, run_id="run-2"))
    t.start()
    assert started.wait(5)
    try:
        assert prov.check_binding(_pdir(root), "demo", ART, "run-1").outcome == prov.RUN_ACTIVE
    finally:
        release.set()
        t.join()


def test_verify_vs_verify_never_corrupts(root):
    _run(root, run_id="run-1")
    outcomes = []

    def v():
        for _ in range(20):
            outcomes.append(prov.check_binding(_pdir(root), "demo", ART, "run-1").outcome)

    ts = [threading.Thread(target=v) for _ in range(4)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert set(outcomes) <= {prov.VERIFIED, prov.RUN_ACTIVE}
    assert prov.VERIFIED in outcomes


# 6: a killed process (real OS process) releases the lease
def test_killed_process_releases_lease(tmp_path):
    ready = tmp_path / "ready"
    code = (
        "import sys,time,pathlib;sys.path.insert(0,sys.argv[1]);"
        "from app.provenance import project_write_lease;"
        "cm=project_write_lease(sys.argv[2]);cm.__enter__();"
        "pathlib.Path(sys.argv[3]).write_text('x');time.sleep(60)"
    )
    pd = tmp_path / "projects" / "demo"
    proc = subprocess.Popen([sys.executable, "-c", code, str(REPO), str(pd), str(ready)])
    try:
        for _ in range(100):
            if ready.exists():
                break
            time.sleep(0.1)
        assert ready.exists()
        assert not prov.lease_is_free(pd)  # another process holds it
        with pytest.raises(prov.ProjectBusyError), prov.project_write_lease(pd):
            pass
    finally:
        proc.kill()
        proc.wait()
    assert prov.lease_is_free(pd)


# 7: hash-then-parse uses the very same bytes (file swapped after the read is not observed)
def test_verified_bytes_are_the_hashed_bytes(root, monkeypatch):
    _run(root, run_id="run-1")
    real = prov.read_artifact_bytes_with_retry
    path = _pdir(root) / "metadata" / ART
    original = path.read_bytes()
    calls = []

    def reading_then_swapping(p, *a, **k):
        data = real(p, *a, **k)
        calls.append(p)
        path.write_bytes(b"changed-after-read")
        return data

    monkeypatch.setattr(prov, "read_artifact_bytes_with_retry", reading_then_swapping)
    res = prov.check_binding(_pdir(root), "demo", ART, "run-1")
    assert res.outcome == prov.VERIFIED and res.data == original
    assert len(calls) == 1  # read exactly once


# 8: external process holding the file (Windows sharing violation): bounded retry, explicit error
def test_read_retries_then_raises(tmp_path, monkeypatch):
    f = tmp_path / "x.json"
    f.write_text("a")
    n = {"c": 0}

    def locked(self):
        n["c"] += 1
        raise PermissionError("sharing violation")

    monkeypatch.setattr(Path, "read_bytes", locked)
    with pytest.raises(PermissionError):
        prov.read_artifact_bytes_with_retry(f, attempts=4, delay=0.001)
    assert n["c"] == 4


def test_read_succeeds_after_transient_lock(tmp_path, monkeypatch):
    f = tmp_path / "x.json"
    f.write_text("ok")
    real = Path.read_bytes
    n = {"c": 0}

    def flaky(self):
        n["c"] += 1
        if n["c"] < 3:
            raise PermissionError("busy")
        return real(self)

    monkeypatch.setattr(Path, "read_bytes", flaky)
    assert prov.read_artifact_bytes_with_retry(f, attempts=5, delay=0.001) == b"ok"


# 9: non-Quaero edit after the fingerprint is caught by the hash
def test_external_edit_after_fingerprint_detected(root):
    _run(root, run_id="run-1")
    p = _pdir(root) / "metadata" / ART
    p.write_bytes(p.read_bytes() + b" ")
    assert prov.check_binding(_pdir(root), "demo", ART, "run-1").outcome == prov.ARTIFACT_CHANGED


# 10: re-entrant, released on every exit path including exceptions
def test_reentrant_and_released_on_exception(tmp_path):
    pd = tmp_path / "projects" / "demo"
    with prov.project_write_lease(pd):
        with prov.project_write_lease(pd):
            assert not _free_from_other_thread(pd)
        assert not _free_from_other_thread(pd)  # outer still held
    assert prov.lease_is_free(pd)
    with pytest.raises(ValueError), prov.project_write_lease(pd):
        raise ValueError("boom")
    assert prov.lease_is_free(pd)


def test_lease_released_when_pipeline_body_fails(root, monkeypatch):
    def body(**kw):
        raise RuntimeError("pipeline failed")

    monkeypatch.setattr(pr, "_run_pipeline_body", body)
    with pytest.raises(RuntimeError):
        _run(root, run_id="run-1")
    assert prov.lease_is_free(_pdir(root))
    store.init_db()
    assert store.latest_run_artifact("demo", ART) is None  # nothing registered for a failed run


def _free_from_other_thread(pd):
    out = []
    t = threading.Thread(target=lambda: out.append(prov.lease_is_free(pd)))
    t.start()
    t.join()
    return out[0]


# 11: registration failure never fails a completed pipeline
def test_registration_failure_keeps_pipeline_completed(root, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(store, "register_run_artifacts", boom)
    result = _run(root, run_id="run-1")
    assert result  # pipeline result returned, no exception
    assert (
        prov.check_binding(_pdir(root), "demo", ART, "run-1").outcome == prov.BINDING_UNVERIFIABLE
    )
    assert prov.lease_is_free(_pdir(root))
