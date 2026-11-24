"""Run/artifact attribution: only artifacts a run reports producing are bound to it."""

from __future__ import annotations

import logging

import pytest

from app import pipeline_runner as pr
from app import provenance as prov
from db import store

DQ = "dq_score_demo.json"
AN = "anomaly_report_v2_demo.json"
REL = "projects/demo/metadata/"


@pytest.fixture
def root(tmp_path):
    (tmp_path / "projects" / "demo" / "metadata").mkdir(parents=True)
    return tmp_path


def _meta(root):
    return root / "projects" / "demo" / "metadata"


def _install(monkeypatch, plan):
    """Replace both bodies; `plan` is a list consumed one run at a time: (files_to_write, results_builder)."""
    calls = iter(plan)

    def body(*args, **kw):
        writes, results = next(calls)
        root = kw.get("project_root") or args[1]
        for name, text in writes.items():
            (_meta(__import__("pathlib").Path(root)) / name).write_text(text, encoding="utf-8")
        return results

    monkeypatch.setattr(pr, "_run_pipeline_body", body)
    monkeypatch.setattr(pr, "_run_project_from_config_body", body)
    monkeypatch.setattr(pr, "_load_project_config", lambda *a, **k: {"project": "demo"})


def _pipeline(root, run_id):
    return pr.run_pipeline(
        source_path=root / "d.csv",
        source_name="Demo",
        question="q",
        project_root=root,
        run_id=run_id,
    )


def _config(root, run_id):
    return pr.run_project_from_config("cfg.yaml", project_root=root, run_id=run_id)


ENTRYPOINTS = pytest.mark.parametrize(
    "entry", [_pipeline, _config], ids=["run_pipeline", "from_config"]
)


def _both():
    return {"dq_score": {"output_path": REL + DQ}, "anomaly_framing_v2": {"output_path": REL + AN}}


def _rows():
    store.init_db()
    with store.get_connection() as c:
        return [dict(r) for r in c.execute("SELECT * FROM run_artifacts ORDER BY id")]


@ENTRYPOINTS
def test_stale_artifact_not_attributed_when_stage_skipped(root, monkeypatch, entry):
    _install(
        monkeypatch,
        [
            ({DQ: "a", AN: "anomalies-A"}, _both()),
            (
                {DQ: "b"},
                {
                    "dq_score": {"output_path": REL + DQ},
                    "anomaly_framing_v2": {"output_path": REL + AN, "skipped": True},
                },
            ),
        ],
    )
    entry(root, "run-A")
    entry(root, "run-B")
    an_rows = [r for r in _rows() if r["artifact"] == AN]
    assert [r["run_id"] for r in an_rows] == ["run-A"]
    p = _meta(root).parent
    assert prov.check_binding(p, "demo", AN, "run-B").outcome != prov.VERIFIED
    assert prov.check_binding(p, "demo", AN, "run-A").outcome == prov.VERIFIED


@ENTRYPOINTS
def test_zero_artifacts_registers_nothing_and_logs(root, monkeypatch, caplog, entry):
    (_meta(root) / DQ).write_text("stale", encoding="utf-8")
    _install(monkeypatch, [({}, {})])
    with caplog.at_level(logging.WARNING):
        entry(root, "run-B")
    assert _rows() == []
    assert "produced no reviewable artifacts" in caplog.text
    res = prov.check_binding(_meta(root).parent, "demo", DQ, "run-B")
    assert res.outcome != prov.VERIFIED


@ENTRYPOINTS
def test_consecutive_runs_different_outputs_bind_to_own_run(root, monkeypatch, entry):
    _install(
        monkeypatch,
        [
            ({DQ: "one"}, _both() | {"anomaly_framing_v2": {"skipped": True}}),
            ({DQ: "two"}, _both() | {"anomaly_framing_v2": {"skipped": True}}),
        ],
    )
    p = _meta(root).parent
    entry(root, "run-A")
    assert prov.check_binding(p, "demo", DQ, "run-A").outcome == prov.VERIFIED
    entry(root, "run-B")
    assert prov.check_binding(p, "demo", DQ, "run-B").outcome == prov.VERIFIED
    assert prov.check_binding(p, "demo", DQ, "run-A").outcome != prov.VERIFIED


@ENTRYPOINTS
def test_partial_stage_failure_registers_nothing(root, monkeypatch, entry):
    def boom(*a, **k):
        (_meta(root) / DQ).write_text("half", encoding="utf-8")
        raise RuntimeError("anomaly stage failed")

    monkeypatch.setattr(pr, "_run_pipeline_body", boom)
    monkeypatch.setattr(pr, "_run_project_from_config_body", boom)
    monkeypatch.setattr(pr, "_load_project_config", lambda *a, **k: {"project": "demo"})
    with pytest.raises(RuntimeError):
        entry(root, "run-A")
    store.init_db()
    assert _rows() == []


def test_other_source_and_undeclared_files_ignored(root):
    meta = _meta(root)
    (meta / "dq_score_other.json").write_text("x", encoding="utf-8")
    (meta / DQ).write_text("y", encoding="utf-8")
    results = {"dq_score": {"output_path": REL + DQ}}
    assert [p.name for p in prov.produced_artifact_paths(meta.parent, results)] == [DQ]
    assert (
        prov.produced_artifact_paths(meta.parent, {"dq_score": {"output_path": REL + "nope.json"}})
        == []
    )
    # path traversal / foreign pattern is never accepted
    assert (
        prov.produced_artifact_paths(meta.parent, {"dq_score": {"output_path": "../../secret.txt"}})
        == []
    )
    assert prov.produced_artifact_paths(meta.parent, None) == []
