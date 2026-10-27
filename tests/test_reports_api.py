"""Tests for the operations-console read endpoints.

Artifacts are produced by the real pipeline writers (``score_dataframe``, ``detect_anomalies``,
``frame_anomalies``, ``build_lineage``) into a temporary project, so the API schemas are
checked against what the pipeline actually writes.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from api.dependencies import get_project_root
from api.main import app
from db import store
from kpi_engine.anomaly_detection import detect_anomalies
from pipelines.anomaly_framing import frame_anomalies
from pipelines.dq_scorer import score_dataframe
from pipelines.lineage import build_lineage

_KEY = "reports-test-key"
_ROUTES = ("overview", "data-quality", "anomalies", "lineage")


# ---------------------------------------------------------------------------
# Fixtures and builders
# ---------------------------------------------------------------------------


@pytest.fixture()
def root(tmp_path: Path) -> Path:
    return tmp_path.resolve()


@pytest.fixture()
def db_file(root: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = root / "console.db"
    monkeypatch.setenv("QUAERO_DB_PATH", str(path))
    store.init_db(path)
    return path


@pytest.fixture()
def client(root: Path, db_file: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("QUAERO_API_KEY", _KEY)
    app.dependency_overrides[get_project_root] = lambda: root
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.clear()


def _auth() -> dict[str, str]:
    return {"X-API-Key": _KEY}


def _staging_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "release_year": [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023],
            "streams": [100, 102, 98, 101, 99, 100, 5000, 103],
            "provider": ["a", "b", "a", "b", "a", "b", "a", "b"],
        }
    )


def _write_summary(project_dir: Path, **extra: object) -> None:
    metadata = project_dir / "metadata"
    metadata.mkdir(parents=True, exist_ok=True)
    summary = {"source": project_dir.name, "question": "Which year is strongest?", **extra}
    (metadata / "final_summary_report.json").write_text(json.dumps(summary), encoding="utf-8")


def make_single_dataset_project(root: Path, name: str = "demo") -> Path:
    """Mimic ``run_pipeline``: DQ score, anomalies v1 + v2 and lineage."""
    project_dir = root / "projects" / name
    frame = _staging_frame()
    score_dataframe(frame, name, project_root=project_dir)
    detect_anomalies(frame, name, project_root=project_dir)
    frame_anomalies(name, total_row_count=len(frame), project_root=project_dir)
    build_lineage(
        raw_columns=list(frame.columns),
        staging_dataframe=frame,
        marts={"mart_top": frame.head(3)},
        cleaning_log={"decisions": []},
        source_name=name,
        project_root=project_dir,
    )
    _write_summary(project_dir, primary_kpi="avg_streams")
    return project_dir


def make_multi_dataset_project(root: Path, name: str = "multi") -> Path:
    """Mimic ``run_project_from_config``: only the raw (v1) anomaly report is written."""
    project_dir = root / "projects" / name
    project_dir.mkdir(parents=True)
    (project_dir / "project_config.json").write_text("{}", encoding="utf-8")
    detect_anomalies(_staging_frame(), name, project_root=project_dir)
    _write_summary(
        project_dir,
        project_mode="multi_dataset",
        primary_mart="mart_gdp_trend",
        primary_kpi="avg_gdp",
    )
    return project_dir


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("route", _ROUTES)
class TestAuthentication:
    def test_wrong_key_is_401(self, client: TestClient, root: Path, route: str) -> None:
        make_single_dataset_project(root)
        resp = client.get(f"/projects/demo/{route}", headers={"X-API-Key": "nope"})
        assert resp.status_code == 401

    def test_missing_header_is_401(self, client: TestClient, root: Path, route: str) -> None:
        make_single_dataset_project(root)
        assert client.get(f"/projects/demo/{route}").status_code == 401

    def test_unset_server_key_is_503(
        self, client: TestClient, root: Path, route: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("QUAERO_API_KEY", raising=False)
        resp = client.get(f"/projects/demo/{route}", headers=_auth())
        assert resp.status_code == 503

    def test_correct_key_passes_auth(self, client: TestClient, root: Path, route: str) -> None:
        make_single_dataset_project(root)
        assert client.get(f"/projects/demo/{route}", headers=_auth()).status_code == 200


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------


class TestOverview:
    def test_single_dataset_project(self, client: TestClient, root: Path) -> None:
        make_single_dataset_project(root)
        body = client.get("/projects/demo/overview", headers=_auth()).json()
        assert body["project_mode"] == "single_dataset"
        assert body["question"] == "Which year is strongest?"
        assert body["primary_kpi"] == "avg_streams"
        assert body["has_config"] is False
        assert body["artifacts"] == {
            "summary": True,
            "data_quality": True,
            "anomalies": True,
            "lineage": True,
            "metrics_definitions": False,
        }
        assert body["last_run"] is None
        assert body["history_available"] is True

    def test_multi_dataset_project_differs(self, client: TestClient, root: Path) -> None:
        make_multi_dataset_project(root)
        body = client.get("/projects/multi/overview", headers=_auth()).json()
        assert body["project_mode"] == "multi_dataset"
        assert body["has_config"] is True
        assert body["primary_mart"] == "mart_gdp_trend"
        assert body["artifacts"]["anomalies"] is True
        assert body["artifacts"]["data_quality"] is False
        assert body["artifacts"]["lineage"] is False

    def test_marts_are_counted(self, client: TestClient, root: Path) -> None:
        project_dir = make_single_dataset_project(root)
        marts = project_dir / "marts"
        marts.mkdir()
        _staging_frame().to_parquet(marts / "mart_a.parquet", index=False)
        _staging_frame().to_parquet(marts / "mart_b.parquet", index=False)
        (marts / "notes.txt").write_text("x", encoding="utf-8")
        assert client.get("/projects/demo/overview", headers=_auth()).json()["marts_count"] == 2

    def test_last_run_comes_from_history(
        self, client: TestClient, root: Path, db_file: Path
    ) -> None:
        make_single_dataset_project(root)
        store.register_project("demo", "/demo", db_file)
        store.create_run("run-old", "demo", question="old?", db_path=db_file)
        time.sleep(0.01)
        store.create_run("run-new", "demo", question="new?", db_path=db_file)
        store.transition_run("run-new", "RUNNING", db_path=db_file)

        body = client.get("/projects/demo/overview", headers=_auth()).json()

        assert body["history_available"] is True
        assert body["last_run"]["run_id"] == "run-new"
        assert body["last_run"]["status"] == "RUNNING"
        assert body["last_run"]["started_at"] is not None

    def test_history_outage_is_flagged_not_hidden(
        self, client: TestClient, root: Path, db_file: Path
    ) -> None:
        make_single_dataset_project(root)
        for suffix in ("", "-wal", "-shm"):
            db_file.with_name(db_file.name + suffix).unlink(missing_ok=True)
        db_file.write_bytes(b"not a sqlite database" * 100)

        resp = client.get("/projects/demo/overview", headers=_auth())

        assert resp.status_code == 200
        assert resp.json()["history_available"] is False
        assert resp.json()["last_run"] is None

    def test_unreadable_summary_does_not_fail_overview(
        self, client: TestClient, root: Path
    ) -> None:
        project_dir = make_single_dataset_project(root)
        (project_dir / "metadata" / "final_summary_report.json").write_text("{oops", "utf-8")
        resp = client.get("/projects/demo/overview", headers=_auth())
        assert resp.status_code == 200
        assert resp.json()["project_mode"] is None
        assert resp.json()["question"] is None

    def test_unknown_project_is_404(self, client: TestClient) -> None:
        assert client.get("/projects/ghost/overview", headers=_auth()).status_code == 404


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------


class TestDataQuality:
    def test_matches_writer_output(self, client: TestClient, root: Path) -> None:
        project_dir = make_single_dataset_project(root)
        on_disk = json.loads((project_dir / "metadata" / "dq_score_demo.json").read_text("utf-8"))

        body = client.get("/projects/demo/data-quality", headers=_auth()).json()

        assert len(body["reports"]) == 1
        report = body["reports"][0]
        assert report["overall_score"] == on_disk["overall_score"]
        assert report["column_count"] == on_disk["column_count"] == 3
        assert {c["column"] for c in report["columns"]} == {"release_year", "streams", "provider"}
        streams = next(c for c in report["columns"] if c["column"] == "streams")
        streams_on_disk = next(c for c in on_disk["columns"] if c["column"] == "streams")
        assert streams == streams_on_disk
        assert set(streams) == {
            "column",
            "dtype",
            "completeness",
            "uniqueness",
            "outlier_flag",
            "format_score",
            "dq_score",
        }

    def test_multi_dataset_has_no_report(self, client: TestClient, root: Path) -> None:
        make_multi_dataset_project(root)
        resp = client.get("/projects/multi/data-quality", headers=_auth())
        assert resp.status_code == 404
        assert "single-dataset" in resp.json()["detail"]

    def test_corrupt_json_is_500_without_leaking_paths(
        self, client: TestClient, root: Path
    ) -> None:
        project_dir = make_single_dataset_project(root)
        (project_dir / "metadata" / "dq_score_demo.json").write_text("{broken", "utf-8")
        resp = client.get("/projects/demo/data-quality", headers=_auth())
        assert resp.status_code == 500
        assert "dq_score_demo" not in resp.text
        assert str(root) not in resp.text

    def test_unexpected_format_is_500(self, client: TestClient, root: Path) -> None:
        project_dir = make_single_dataset_project(root)
        (project_dir / "metadata" / "dq_score_demo.json").write_text(
            json.dumps({"source": "demo", "overall_score": "high", "columns": "n/a"}), "utf-8"
        )
        assert client.get("/projects/demo/data-quality", headers=_auth()).status_code == 500

    def test_non_object_json_is_500(self, client: TestClient, root: Path) -> None:
        project_dir = make_single_dataset_project(root)
        (project_dir / "metadata" / "dq_score_demo.json").write_text("[1, 2, 3]", "utf-8")
        assert client.get("/projects/demo/data-quality", headers=_auth()).status_code == 500

    def test_oversized_artifact_is_refused(
        self, client: TestClient, root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        make_single_dataset_project(root)
        monkeypatch.setattr("api.services.reports.MAX_ARTIFACT_BYTES", 10)
        assert client.get("/projects/demo/data-quality", headers=_auth()).status_code == 500


# ---------------------------------------------------------------------------
# Anomalies
# ---------------------------------------------------------------------------


class TestAnomalies:
    def test_single_dataset_returns_framed_v2(self, client: TestClient, root: Path) -> None:
        make_single_dataset_project(root)
        body = client.get("/projects/demo/anomalies", headers=_auth()).json()
        assert body["version"] == "v2"
        assert body["business_summary"]
        item = body["anomalies"][0]
        assert item["column"] == "streams"
        assert item["impact"] in {"low", "medium", "high"}
        assert item["suggested_action"]
        assert item["frequency"] == pytest.approx(1 / 8, abs=1e-3)

    def test_multi_dataset_falls_back_to_v1(self, client: TestClient, root: Path) -> None:
        make_multi_dataset_project(root)
        body = client.get("/projects/multi/anomalies", headers=_auth()).json()
        assert body["version"] == "v1"
        assert body["business_summary"] is None
        assert body["anomalies"][0]["impact"] is None
        assert body["anomalies"][0]["suggested_action"] is None
        assert body["anomalies"][0]["method"] in {"iqr", "zscore"}

    def test_v2_is_preferred_over_v1(self, client: TestClient, root: Path) -> None:
        project_dir = make_single_dataset_project(root)
        assert (project_dir / "metadata" / "anomaly_report_demo.json").is_file()
        assert client.get("/projects/demo/anomalies", headers=_auth()).json()["version"] == "v2"

    def test_most_recent_v1_report_wins(self, client: TestClient, root: Path) -> None:
        project_dir = make_multi_dataset_project(root)
        metadata = project_dir / "metadata"
        older = metadata / "anomaly_report_old.json"
        newer = metadata / "anomaly_report_new.json"
        for path, source in ((older, "old"), (newer, "new")):
            path.write_text(
                json.dumps({"source": source, "anomaly_row_count": 0, "anomalies": []}), "utf-8"
            )
        os.utime(older, (1_000_000_000, 1_000_000_000))
        os.utime(newer, (2_000_000_000, 2_000_000_000))
        (metadata / "anomaly_report_multi.json").unlink()

        body = client.get("/projects/multi/anomalies", headers=_auth()).json()

        assert body["source"] == "new"

    def test_missing_report_is_404(self, client: TestClient, root: Path) -> None:
        (root / "projects" / "empty" / "metadata").mkdir(parents=True)
        resp = client.get("/projects/empty/anomalies", headers=_auth())
        assert resp.status_code == 404
        assert "No anomaly report" in resp.json()["detail"]

    def test_unexpected_format_is_500(self, client: TestClient, root: Path) -> None:
        project_dir = make_multi_dataset_project(root)
        (project_dir / "metadata" / "anomaly_report_multi.json").write_text(
            json.dumps({"source": "multi", "anomalies": [{"column": "x"}]}), "utf-8"
        )
        assert client.get("/projects/multi/anomalies", headers=_auth()).status_code == 500


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------


class TestLineage:
    def test_matches_writer_output(self, client: TestClient, root: Path) -> None:
        project_dir = make_single_dataset_project(root)
        on_disk = json.loads((project_dir / "metadata" / "lineage.json").read_text("utf-8"))

        body = client.get("/projects/demo/lineage", headers=_auth()).json()

        assert body["node_count"] == on_disk["node_count"] == len(body["nodes"])
        assert body["edge_count"] == on_disk["edge_count"] == len(body["edges"])
        assert {n["layer"] for n in body["nodes"]} == {"raw", "staging", "mart"}
        assert all({"from", "to", "type"} <= set(edge) for edge in body["edges"])
        assert {e["type"] for e in body["edges"]} <= {
            "passthrough",
            "normalized",
            "type_cast",
            "aggregated",
        }

    def test_multi_dataset_has_no_lineage(self, client: TestClient, root: Path) -> None:
        make_multi_dataset_project(root)
        resp = client.get("/projects/multi/lineage", headers=_auth())
        assert resp.status_code == 404
        assert "single-dataset" in resp.json()["detail"]

    def test_corrupt_lineage_is_500(self, client: TestClient, root: Path) -> None:
        project_dir = make_single_dataset_project(root)
        (project_dir / "metadata" / "lineage.json").write_text("not json", "utf-8")
        assert client.get("/projects/demo/lineage", headers=_auth()).status_code == 500

    def test_edge_missing_endpoint_is_500(self, client: TestClient, root: Path) -> None:
        project_dir = make_single_dataset_project(root)
        (project_dir / "metadata" / "lineage.json").write_text(
            json.dumps({"nodes": [], "edges": [{"from": "a"}]}), "utf-8"
        )
        assert client.get("/projects/demo/lineage", headers=_auth()).status_code == 500


# ---------------------------------------------------------------------------
# Path safety
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("route", _ROUTES)
@pytest.mark.parametrize(
    "project",
    ["..", "...", "a.b", "a b", "demo;ls", "..%2F..%2Fetc", "%2e%2e", "demo%00", "d" * 5 + "/x"],
)
def test_invalid_identifiers_are_rejected(
    client: TestClient, root: Path, route: str, project: str
) -> None:
    make_single_dataset_project(root)
    resp = client.get(f"/projects/{project}/{route}", headers=_auth())
    assert resp.status_code in {400, 404}


def test_symlinked_artifact_outside_metadata_is_ignored(client: TestClient, root: Path) -> None:
    project_dir = make_single_dataset_project(root)
    outside = root / "outside_lineage.json"
    outside.write_text(json.dumps({"nodes": [], "edges": []}), encoding="utf-8")
    link = project_dir / "metadata" / "lineage.json"
    link.unlink()
    try:
        link.symlink_to(outside)
    except (OSError, NotImplementedError):
        pytest.skip("symlinks are not available in this environment")

    assert client.get("/projects/demo/lineage", headers=_auth()).status_code == 404


def test_artifact_resolving_outside_metadata_is_ignored(
    client: TestClient, root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same guarantee as the symlink test, without needing symlink privileges."""
    make_single_dataset_project(root)
    outside = root / "outside_lineage.json"
    outside.write_text(json.dumps({"nodes": [], "edges": []}), encoding="utf-8")
    real_resolve = Path.resolve

    def fake_resolve(self: Path, *args: object, **kwargs: object) -> Path:
        if self.name == "lineage.json":
            return real_resolve(outside)
        return real_resolve(self, *args, **kwargs)

    monkeypatch.setattr(Path, "resolve", fake_resolve)

    assert client.get("/projects/demo/lineage", headers=_auth()).status_code == 404


# ---------------------------------------------------------------------------
# Identifier validation on POST /projects/{project}/run
# ---------------------------------------------------------------------------


class TestRunIdentifierValidation:
    @pytest.mark.parametrize("project", ["bad.name", "bad name", "a;b", "a$b"])
    def test_invalid_project_name_is_422(self, client: TestClient, project: str) -> None:
        with patch("api.routers.runs.execute_run"):
            resp = client.post(f"/projects/{project}/run", json={"question": "q?"}, headers=_auth())
        assert resp.status_code == 422

    @pytest.mark.parametrize(
        "project", ["global_economic_indicators", "release_impact", "test_project", "Demo-1"]
    )
    def test_existing_style_names_are_still_accepted(
        self, client: TestClient, project: str
    ) -> None:
        with patch("api.routers.runs.execute_run"):
            resp = client.post(f"/projects/{project}/run", json={"question": "q?"}, headers=_auth())
        assert resp.status_code == 202
