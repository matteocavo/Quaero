"""Guards for the GitHub Pages workflow: it must never deploy before review or from a feature branch."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

yaml = pytest.importorskip("yaml")

WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "web.yml"
MAIN_ONLY = "github.ref == 'refs/heads/main'"

pytestmark = pytest.mark.skipif(not WORKFLOW.is_file(), reason="web workflow is not present")


@pytest.fixture(scope="module")
def workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _triggers(workflow: dict[str, Any]) -> dict[str, Any]:
    # PyYAML reads the bare key `on` as the boolean True (YAML 1.1).
    return workflow.get("on") or workflow.get(True)


def _steps(workflow: dict[str, Any]) -> list[dict[str, Any]]:
    return workflow["jobs"]["build"]["steps"]


def test_runs_on_pull_requests_and_manual_dispatch_only(workflow: dict[str, Any]) -> None:
    triggers = _triggers(workflow)
    assert set(triggers) == {"pull_request", "workflow_dispatch"}
    assert "push" not in triggers and "schedule" not in triggers


def test_deploy_job_runs_only_for_manual_runs_from_main(workflow: dict[str, Any]) -> None:
    condition = workflow["jobs"]["deploy"]["if"]
    assert "github.event_name == 'workflow_dispatch'" in condition
    assert MAIN_ONLY in condition
    assert workflow["jobs"]["deploy"]["needs"] == "build"


def test_pages_artifact_is_only_uploaded_from_main(workflow: dict[str, Any]) -> None:
    uploads = [s for s in _steps(workflow) if "upload-pages-artifact" in s.get("uses", "")]
    assert len(uploads) == 1
    assert "github.event_name == 'workflow_dispatch'" in uploads[0]["if"]
    assert MAIN_ONLY in uploads[0]["if"]


def test_manual_runs_from_other_refs_fail_explicitly(workflow: dict[str, Any]) -> None:
    first = _steps(workflow)[0]
    assert "workflow_dispatch" in first["if"]
    assert "github.ref != 'refs/heads/main'" in first["if"]
    assert "exit 1" in first["run"]
    assert "refs/heads/main" in first["run"]


def test_only_the_deploy_job_can_write_pages(workflow: dict[str, Any]) -> None:
    assert workflow["permissions"] == {"contents": "read"}
    assert "permissions" not in workflow["jobs"]["build"]
    assert workflow["jobs"]["deploy"]["permissions"] == {"pages": "write", "id-token": "write"}


def test_build_and_test_steps_run_for_pull_requests(workflow: dict[str, Any]) -> None:
    by_name = {s.get("name"): s for s in _steps(workflow)}
    for name in ("Install dependencies", "Type check", "Test", "Build for GitHub Pages"):
        assert name in by_name
        assert "if" not in by_name[name], f"{name} must not be conditional"
    assert by_name["Build for GitHub Pages"]["env"]["VITE_BASE_PATH"] == "/Quaero/"
