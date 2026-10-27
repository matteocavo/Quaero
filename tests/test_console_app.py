"""AppTest-based tests for the operations console (stubbed client, no network)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

try:
    from streamlit.testing.v1 import AppTest
except Exception as exc:  # pragma: no cover - environment-dependent import guard
    pytest.skip(
        f"streamlit.testing.v1.AppTest unavailable in this environment: {exc}",
        allow_module_level=True,
    )

import console.client as client_module
from console.client import (
    ConflictError,
    NotFoundError,
    ServiceUnavailable,
)

APP_PATH = str(Path(__file__).resolve().parent.parent / "console" / "app.py")

ARTIFACTS_ALL = {
    "summary": True,
    "data_quality": True,
    "anomalies": True,
    "lineage": True,
    "metrics_definitions": False,
}


class FakeClient:
    """In-memory stand-in for QuaeroClient; ``current`` is the instance the app receives."""

    current: FakeClient

    def __init__(self) -> None:
        self.base_url = "http://127.0.0.1:8000"
        self.projects = [
            {"project": "demo", "marts_count": 1, "has_summary": True},
            {"project": "multi", "marts_count": 0, "has_summary": True},
        ]
        self.runs: list[dict[str, Any]] = []
        self.history_error: Exception | None = None
        self.start_error: Exception | None = None
        self.started: list[tuple[str, str, str | None]] = []
        self.history_calls = 0
        self.dq_error: Exception | None = None
        self.mart_rows = [{"year": 2020, "name": "alpha"}, {"year": 2021, "name": "beta"}]

    @classmethod
    def from_env(cls) -> FakeClient:
        return cls.current

    def list_projects(self) -> list[dict[str, Any]]:
        return self.projects

    def overview(self, project: str) -> dict[str, Any]:
        multi = project == "multi"
        return {
            "project": project,
            "project_mode": "multi_dataset" if multi else "single_dataset",
            "question": "Which year is strongest?",
            "primary_mart": "mart_a",
            "primary_kpi": "avg_x",
            "marts_count": 0 if multi else 1,
            "has_config": multi,
            "artifacts": {**ARTIFACTS_ALL, "data_quality": not multi, "lineage": not multi},
            "last_run": self.runs[0] if self.runs else None,
            "history_available": self.history_error is None,
        }

    def run_history(self, project: str, limit: int = 50) -> dict[str, Any]:
        self.history_calls += 1
        if self.history_error:
            raise self.history_error
        return {"project": project, "runs": self.runs, "total": len(self.runs)}

    def start_run(self, project: str, question: str, source_url: str | None = None) -> dict:
        if self.start_error:
            raise self.start_error
        self.started.append((project, question, source_url))
        return {"run_id": "run-123", "project": project, "status": "QUEUED"}

    def list_marts(self, project: str) -> list[dict[str, Any]]:
        return [{"name": "mart_a", "path": "p"}]

    def mart_preview(self, project: str, mart: str, page: int = 1, page_size: int = 100) -> dict:
        return {
            "project": project,
            "mart": mart,
            "columns": ["year", "name"],
            "rows": self.mart_rows,
            "total_rows": len(self.mart_rows),
            "page": page,
            "page_size": page_size,
        }

    def data_quality(self, project: str) -> dict[str, Any]:
        if self.dq_error:
            raise self.dq_error
        return {
            "project": project,
            "reports": [
                {
                    "source": project,
                    "generated_at": "2026-10-27T10:00:00+00:00",
                    "overall_score": 91.5,
                    "column_count": 1,
                    "columns": [
                        {
                            "column": "streams",
                            "dtype": "int64",
                            "completeness": 100.0,
                            "uniqueness": 80.0,
                            "outlier_flag": True,
                            "format_score": 100.0,
                            "dq_score": 92.0,
                        }
                    ],
                }
            ],
        }

    def anomalies(self, project: str) -> dict[str, Any]:
        return {
            "project": project,
            "source": project,
            "version": "v2",
            "anomaly_row_count": 1,
            "business_summary": "1 rows (12.5%) contain anomalies.",
            "anomalies": [
                {
                    "column": "streams",
                    "method": "iqr",
                    "threshold": "1 to 2",
                    "anomaly_count": 1,
                    "example_values": [5000],
                    "frequency": 0.125,
                    "impact": "high",
                    "suggested_action": "Review the distribution.",
                }
            ],
        }

    def lineage(self, project: str) -> dict[str, Any]:
        return {
            "project": project,
            "source": project,
            "generated_at": "t",
            "node_count": 2,
            "edge_count": 1,
            "nodes": [
                {"id": "raw::a", "layer": "raw", "name": "a"},
                {"id": "staging::a", "layer": "staging", "name": "a"},
            ],
            "edges": [{"from": "raw::a", "to": "staging::a", "type": "passthrough"}],
        }


@pytest.fixture()
def fake(monkeypatch: pytest.MonkeyPatch) -> FakeClient:
    instance = FakeClient()
    FakeClient.current = instance
    monkeypatch.setattr(client_module, "QuaeroClient", FakeClient)
    return instance


def _run() -> AppTest:
    app = AppTest.from_file(APP_PATH, default_timeout=15)
    app.run()
    return app


def _texts(elements: Any) -> str:
    return " | ".join(str(getattr(e, "value", "")) for e in elements)


RUNNING = {
    "run_id": "r1",
    "project": "demo",
    "status": "RUNNING",
    "question": "q?",
    "created_at": "2026-10-27T10:00:00+00:00",
    "started_at": "2026-10-27T10:00:01+00:00",
    "completed_at": None,
    "error": None,
}
FAILED = {
    **RUNNING,
    "run_id": "r0",
    "status": "FAILED",
    "completed_at": "2026-10-27T10:00:31+00:00",
    "error": "boom",
}


class TestStartup:
    def test_missing_api_key_shows_a_clear_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("QUAERO_API_KEY", raising=False)
        app = _run()
        assert "QUAERO_API_KEY" in _texts(app.error)
        assert len(app.tabs) == 0

    def test_renders_all_screens_without_exceptions(self, fake: FakeClient) -> None:
        app = _run()
        assert len(app.exception) == 0
        assert [t.label for t in app.tabs] == [
            "Projects",
            "Pipeline Operations",
            "Marts",
            "Data Quality & Anomalies",
            "Lineage",
        ]

    def test_non_loopback_api_url_is_flagged(self, fake: FakeClient) -> None:
        fake.base_url = "https://api.example.com"
        app = _run()
        assert "not on this machine" in _texts(app.sidebar.warning)

    def test_unavailable_api_stops_with_a_warning(
        self, fake: FakeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def boom() -> list:
            raise ServiceUnavailable("Cannot reach the Quaero API")

        monkeypatch.setattr(fake, "list_projects", boom)
        app = _run()
        assert "Cannot reach" in _texts(app.warning)
        assert len(app.tabs) == 0


class TestExplorer:
    def test_overview_and_artifact_state(self, fake: FakeClient) -> None:
        fake.runs = [RUNNING]
        app = _run()
        assert "Which year is strongest?" in _texts(app.markdown)
        assert any("data quality" in str(m.value) for m in app.markdown)

    def test_multi_dataset_shows_the_single_dataset_caveat(self, fake: FakeClient) -> None:
        app = AppTest.from_file(APP_PATH, default_timeout=15)
        app.run()
        app.sidebar.selectbox(key="selected_project").select("multi").run()
        assert "single-dataset runs only" in _texts(app.caption)

    def test_history_outage_is_not_shown_as_never_run(self, fake: FakeClient) -> None:
        fake.history_error = ServiceUnavailable("db down", 503)
        app = _run()
        assert "Run history is unavailable" in _texts(app.warning)
        assert "never been run" not in _texts(app.markdown)


class TestOperations:
    def test_run_button_is_disabled_while_a_run_is_active(self, fake: FakeClient) -> None:
        fake.runs = [RUNNING]
        app = _run()
        assert app.button(key="op_run").disabled is True
        assert "already RUNNING" in _texts(app.info)

    def test_run_button_is_enabled_when_idle(self, fake: FakeClient) -> None:
        fake.runs = [FAILED]
        assert _run().button(key="op_run").disabled is False

    def test_blank_question_is_rejected_client_side(self, fake: FakeClient) -> None:
        app = _run()
        app.button(key="op_run").click().run()
        assert "business question" in _texts(app.error)
        assert fake.started == []

    def test_project_without_config_needs_a_url(self, fake: FakeClient) -> None:
        app = _run()
        app.text_area(key="op_question").set_value("What?").run()
        app.button(key="op_run").click().run()
        assert "project_config.json" in _texts(app.error)
        assert fake.started == []

    def test_successful_start_calls_the_api_once_and_confirms(self, fake: FakeClient) -> None:
        app = _run()
        app.text_area(key="op_question").set_value("What?").run()
        app.text_input(key="op_url").set_value("https://example.com/d.csv").run()
        app.button(key="op_run").click().run()
        assert fake.started == [("demo", "What?", "https://example.com/d.csv")]
        assert "run-123" in _texts(app.success)

    def test_conflict_from_the_api_is_a_warning(self, fake: FakeClient) -> None:
        fake.start_error = ConflictError("A run is already active", 409)
        app = _run()
        app.text_area(key="op_question").set_value("What?").run()
        app.text_input(key="op_url").set_value("https://example.com/d.csv").run()
        app.button(key="op_run").click().run()
        assert "Not started" in _texts(app.warning)

    def test_history_outage_is_reported_not_hidden(self, fake: FakeClient) -> None:
        fake.history_error = ServiceUnavailable("db down", 503)
        app = _run()
        assert "Run history is unavailable: db down" in _texts(app.warning)

    def test_history_shows_error_and_duration(self, fake: FakeClient) -> None:
        fake.runs = [FAILED]
        app = _run()
        frames = [df.value for df in app.dataframe]
        history = next(df for df in frames if "duration" in df.columns)
        assert history.iloc[0]["duration"] == "30s"
        assert history.iloc[0]["error"] == "boom"

    def test_auto_refresh_is_bounded(
        self, fake: FakeClient, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import console.formatting as fmt
        from console import views

        monkeypatch.setattr(fmt, "MAX_POLLS", 2)
        monkeypatch.setattr(views.time, "sleep", lambda _seconds: None)
        fake.runs = [RUNNING]
        app = AppTest.from_file(APP_PATH, default_timeout=15)
        app.run()
        calls_before = fake.history_calls
        app.checkbox(key="op_auto").check().run()
        assert fake.history_calls - calls_before <= 4
        assert "Auto-refresh stopped" in _texts(app.info)

    def test_no_polling_when_auto_refresh_is_off(self, fake: FakeClient) -> None:
        fake.runs = [RUNNING]
        app = _run()
        assert fake.history_calls == 1
        assert app.checkbox(key="op_auto").value is False


class TestMarts:
    def test_preview_and_page_local_filter(self, fake: FakeClient) -> None:
        app = _run()
        assert "client-side" in str(app.text_input(key="mart_filter").label)
        app.text_input(key="mart_filter").set_value("alpha").run()
        assert "Showing 1 of 2 rows" in _texts(app.caption)


class TestQualityAndAnomalies:
    def test_available_artifacts_are_rendered(self, fake: FakeClient) -> None:
        app = _run()
        assert "Columns with outlier flag: `streams`" in _texts(app.markdown)
        assert "1 rows (12.5%) contain anomalies." in _texts(app.info)
        assert "no pass/fail threshold" in _texts(app.caption)

    def test_missing_dq_is_not_generated_not_an_error(self, fake: FakeClient) -> None:
        fake.dq_error = NotFoundError("No data-quality report found", 404)
        app = _run()
        assert "Not generated for this project" in _texts(app.info)
        assert len(app.error) == 0


class TestLineage:
    def test_graph_is_drawn_from_json_and_filterable(self, fake: FakeClient) -> None:
        app = _run()
        assert len(app.get("graphviz_chart")) == 1
        app.multiselect(key="lineage_types").set_value([]).run()
        graph = app.get("graphviz_chart")[0]
        assert "->" not in str(graph.proto.spec)
