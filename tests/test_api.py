"""Tests for the Quaero FastAPI layer (api/main.py and supporting modules).

Isolation strategy
------------------
* All tests use a temporary directory as project_root so they never touch real
  project artifacts or the pipeline.
* The pipeline runner is monkey-patched to avoid real CSV/Parquet processing.
* No LLM calls, no network I/O.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from api.dependencies import get_project_root
from api.main import app
from api.schemas import RunStatus
from api.services.artifacts import (
    _validate_identifier,
    list_marts,
    list_projects,
    read_mart_preview,
    read_project_summary,
)
from api.services.runner import (
    _is_ssrf_risk,
    create_run,
    execute_run,
    get_run,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_TEST_KEY = "test-api-key-12345"


@pytest.fixture()
def tmp_project_root(tmp_path: Path) -> Path:
    """Return a temporary directory wired as the project root."""
    return tmp_path


@pytest.fixture()
def project_root_with_project(tmp_path: Path) -> tuple[Path, str]:
    """Create a minimal project structure and return (root, project_name)."""
    project = "test_project"
    project_dir = tmp_path / "projects" / project
    marts_dir = project_dir / "marts"
    metadata_dir = project_dir / "metadata"
    marts_dir.mkdir(parents=True)
    metadata_dir.mkdir(parents=True)

    # Write a minimal summary report
    summary = {
        "source": project,
        "question": "Test question?",
        "mart_paths": {"mart_test": f"projects/{project}/marts/mart_test.parquet"},
    }
    (metadata_dir / "final_summary_report.json").write_text(json.dumps(summary), encoding="utf-8")

    # Write a minimal Parquet mart
    df = pd.DataFrame({"year": [2020, 2021], "value": [100.0, 200.0]})
    df.to_parquet(marts_dir / "mart_test.parquet", index=False)

    return tmp_path, project


@pytest.fixture()
def api_client(tmp_project_root: Path) -> TestClient:
    """Return a TestClient with the project_root override and API key set."""
    app.dependency_overrides[get_project_root] = lambda: tmp_project_root
    client = TestClient(app, raise_server_exceptions=False)
    yield client
    app.dependency_overrides.clear()


@pytest.fixture()
def api_client_with_project(
    project_root_with_project: tuple[Path, str],
) -> tuple[TestClient, Path, str]:
    """Return a TestClient wired to a root that has one project."""
    root, project = project_root_with_project
    app.dependency_overrides[get_project_root] = lambda: root
    client = TestClient(app, raise_server_exceptions=False)
    yield client, root, project
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _auth(key: str = _TEST_KEY) -> dict[str, str]:
    return {"X-API-Key": key}


# ---------------------------------------------------------------------------
# Health endpoint (public)
# ---------------------------------------------------------------------------


class TestHealth:
    def test_health_is_public(self, api_client: TestClient) -> None:
        resp = api_client.get("/health")
        assert resp.status_code == 200

    def test_health_returns_ok(self, api_client: TestClient) -> None:
        data = api_client.get("/health").json()
        assert data["status"] == "ok"

    def test_health_no_auth_required(self, api_client: TestClient) -> None:
        """Health must work without X-API-Key."""
        resp = api_client.get("/health")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------


class TestAuthentication:
    def test_missing_key_returns_503_when_unconfigured(self, api_client: TestClient) -> None:
        """Without QUAERO_API_KEY env var, protected endpoints return 503."""
        import os

        os.environ.pop("QUAERO_API_KEY", None)
        resp = api_client.get("/projects")
        assert resp.status_code == 503

    def test_wrong_key_returns_401(self, api_client: TestClient) -> None:
        import os

        os.environ["QUAERO_API_KEY"] = _TEST_KEY
        resp = api_client.get("/projects", headers={"X-API-Key": "wrong-key"})
        assert resp.status_code == 401
        os.environ.pop("QUAERO_API_KEY", None)

    def test_correct_key_returns_200(self, api_client: TestClient) -> None:
        import os

        os.environ["QUAERO_API_KEY"] = _TEST_KEY
        resp = api_client.get("/projects", headers=_auth())
        assert resp.status_code == 200
        os.environ.pop("QUAERO_API_KEY", None)

    def test_no_key_header_returns_503_or_401(self, api_client: TestClient) -> None:
        import os

        os.environ["QUAERO_API_KEY"] = _TEST_KEY
        resp = api_client.get("/projects")  # no header at all
        assert resp.status_code in (401, 503)
        os.environ.pop("QUAERO_API_KEY", None)


# ---------------------------------------------------------------------------
# Projects list
# ---------------------------------------------------------------------------


class TestProjectsEndpoint:
    def setup_method(self) -> None:
        import os

        os.environ["QUAERO_API_KEY"] = _TEST_KEY

    def teardown_method(self) -> None:
        import os

        os.environ.pop("QUAERO_API_KEY", None)

    def test_empty_projects(self, api_client: TestClient) -> None:
        resp = api_client.get("/projects", headers=_auth())
        assert resp.status_code == 200
        assert resp.json()["projects"] == []

    def test_lists_existing_project(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        resp = client.get("/projects", headers=_auth())
        assert resp.status_code == 200
        names = [p["project"] for p in resp.json()["projects"]]
        assert project in names

    def test_project_has_expected_fields(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        projects = client.get("/projects", headers=_auth()).json()["projects"]
        p = next(p for p in projects if p["project"] == project)
        assert "marts_count" in p
        assert "has_summary" in p
        assert p["has_summary"] is True
        assert p["marts_count"] == 1


# ---------------------------------------------------------------------------
# Summary endpoint
# ---------------------------------------------------------------------------


class TestSummaryEndpoint:
    def setup_method(self) -> None:
        import os

        os.environ["QUAERO_API_KEY"] = _TEST_KEY

    def teardown_method(self) -> None:
        import os

        os.environ.pop("QUAERO_API_KEY", None)

    def test_returns_summary(self, api_client_with_project: tuple[TestClient, Path, str]) -> None:
        client, _root, project = api_client_with_project
        resp = client.get(f"/projects/{project}/summary", headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert data["project"] == project
        assert "summary" in data

    def test_missing_project_returns_404(self, api_client: TestClient) -> None:
        resp = api_client.get("/projects/nonexistent/summary", headers=_auth())
        assert resp.status_code == 404

    def test_path_traversal_rejected(self, api_client: TestClient) -> None:
        resp = api_client.get("/projects/../etc/summary", headers=_auth())
        assert resp.status_code in (400, 404, 422)


# ---------------------------------------------------------------------------
# Marts listing
# ---------------------------------------------------------------------------


class TestMartsEndpoint:
    def setup_method(self) -> None:
        import os

        os.environ["QUAERO_API_KEY"] = _TEST_KEY

    def teardown_method(self) -> None:
        import os

        os.environ.pop("QUAERO_API_KEY", None)

    def test_lists_marts(self, api_client_with_project: tuple[TestClient, Path, str]) -> None:
        client, _root, project = api_client_with_project
        resp = client.get(f"/projects/{project}/marts", headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert data["project"] == project
        assert len(data["marts"]) == 1
        assert data["marts"][0]["name"] == "mart_test"

    def test_missing_project_returns_404(self, api_client: TestClient) -> None:
        resp = api_client.get("/projects/ghost/marts", headers=_auth())
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Mart preview / pagination
# ---------------------------------------------------------------------------


class TestMartPreview:
    def setup_method(self) -> None:
        import os

        os.environ["QUAERO_API_KEY"] = _TEST_KEY

    def teardown_method(self) -> None:
        import os

        os.environ.pop("QUAERO_API_KEY", None)

    def test_returns_mart_preview(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        resp = client.get(f"/projects/{project}/marts/mart_test", headers=_auth())
        assert resp.status_code == 200
        data = resp.json()
        assert data["mart"] == "mart_test"
        assert "columns" in data
        assert "rows" in data
        assert data["total_rows"] == 2

    def test_pagination_page_one(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        resp = client.get(
            f"/projects/{project}/marts/mart_test?page=1&page_size=1",
            headers=_auth(),
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["page"] == 1
        assert data["page_size"] == 1
        assert len(data["rows"]) == 1

    def test_pagination_page_two(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        resp = client.get(
            f"/projects/{project}/marts/mart_test?page=2&page_size=1",
            headers=_auth(),
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["page"] == 2
        assert len(data["rows"]) == 1

    def test_page_beyond_end_returns_empty_rows(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        resp = client.get(
            f"/projects/{project}/marts/mart_test?page=999&page_size=100",
            headers=_auth(),
        )
        assert resp.status_code == 200
        assert resp.json()["rows"] == []

    def test_invalid_page_size_rejected(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        resp = client.get(
            f"/projects/{project}/marts/mart_test?page_size=9999",
            headers=_auth(),
        )
        assert resp.status_code == 422

    def test_missing_mart_returns_404(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        resp = client.get(f"/projects/{project}/marts/no_such_mart", headers=_auth())
        assert resp.status_code == 404

    def test_mart_path_traversal_rejected(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        resp = client.get(
            f"/projects/{project}/marts/../../../secrets",
            headers=_auth(),
        )
        assert resp.status_code in (400, 404, 422)


# ---------------------------------------------------------------------------
# Run creation
# ---------------------------------------------------------------------------


class TestRunCreation:
    def setup_method(self) -> None:
        import os

        os.environ["QUAERO_API_KEY"] = _TEST_KEY

    def teardown_method(self) -> None:
        import os

        os.environ.pop("QUAERO_API_KEY", None)

    def test_run_returns_202(self, api_client_with_project: tuple[TestClient, Path, str]) -> None:
        client, _root, project = api_client_with_project
        resp = client.post(
            f"/projects/{project}/run",
            json={"question": "What are the trends?"},
            headers=_auth(),
        )
        assert resp.status_code == 202

    def test_run_returns_run_id(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        resp = client.post(
            f"/projects/{project}/run",
            json={"question": "What are the trends?"},
            headers=_auth(),
        )
        data = resp.json()
        assert "run_id" in data
        assert len(data["run_id"]) > 0

    def test_run_initial_status_is_queued(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        resp = client.post(
            f"/projects/{project}/run",
            json={"question": "What are the trends?"},
            headers=_auth(),
        )
        assert resp.json()["status"] == "QUEUED"

    def test_invalid_payload_rejected(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        resp = client.post(
            f"/projects/{project}/run",
            json={},  # missing required 'question'
            headers=_auth(),
        )
        assert resp.status_code == 422

    def test_blank_question_rejected(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        resp = client.post(
            f"/projects/{project}/run",
            json={"question": "   "},
            headers=_auth(),
        )
        assert resp.status_code == 422

    def test_concurrent_run_rejected(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        """A second run on the same project while one is active returns 409.

        TestClient executes BackgroundTasks synchronously, so we mock
        execute_run to a no-op so the first run stays QUEUED when the
        second request arrives.
        """
        client, _root, project = api_client_with_project
        with patch("api.routers.runs.execute_run"):
            # First run — background task mocked, run stays QUEUED
            client.post(
                f"/projects/{project}/run",
                json={"question": "First run"},
                headers=_auth(),
            )
            # Second run while first is still QUEUED
            resp = client.post(
                f"/projects/{project}/run",
                json={"question": "Second run"},
                headers=_auth(),
            )
        assert resp.status_code == 409


# ---------------------------------------------------------------------------
# Run status polling
# ---------------------------------------------------------------------------


class TestRunStatus:
    def setup_method(self) -> None:
        import os

        os.environ["QUAERO_API_KEY"] = _TEST_KEY

    def teardown_method(self) -> None:
        import os

        os.environ.pop("QUAERO_API_KEY", None)

    def test_can_poll_run_status(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        create_resp = client.post(
            f"/projects/{project}/run",
            json={"question": "Trend?"},
            headers=_auth(),
        )
        run_id = create_resp.json()["run_id"]

        poll_resp = client.get(
            f"/projects/{project}/runs/{run_id}",
            headers=_auth(),
        )
        assert poll_resp.status_code == 200
        data = poll_resp.json()
        assert data["run_id"] == run_id
        assert data["project"] == project
        assert data["status"] in ("QUEUED", "RUNNING", "COMPLETED", "FAILED")

    def test_unknown_run_returns_404(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        resp = client.get(
            f"/projects/{project}/runs/{uuid.uuid4()}",
            headers=_auth(),
        )
        assert resp.status_code == 404

    def test_run_for_wrong_project_returns_404(
        self, api_client_with_project: tuple[TestClient, Path, str]
    ) -> None:
        client, _root, project = api_client_with_project
        create_resp = client.post(
            f"/projects/{project}/run",
            json={"question": "Trend?"},
            headers=_auth(),
        )
        run_id = create_resp.json()["run_id"]

        resp = client.get(
            f"/projects/wrong_project/runs/{run_id}",
            headers=_auth(),
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Run state: FAILED on pipeline error
# ---------------------------------------------------------------------------


class TestRunFailedState:
    def test_failed_run_state_persists(self, tmp_path: Path) -> None:
        """A run that raises transitions to FAILED and persists the error."""
        project = "fail_project"
        run_state = create_run(tmp_path, project)
        run_id = run_state["run_id"]

        # Execute without a real pipeline — will fail (no config, no source_url)
        execute_run(
            project_root=tmp_path,
            run_id=run_id,
            project=project,
            question="irrelevant",
            source_url=None,
        )

        persisted = get_run(tmp_path, run_id)
        assert persisted is not None
        assert persisted["status"] == RunStatus.FAILED
        assert persisted["error"] is not None

    def test_failed_run_survives_new_run_store_instance(self, tmp_path: Path) -> None:
        """State stored as JSON must survive reading from a fresh get_run call."""
        project = "persist_project"
        run_state = create_run(tmp_path, project)
        run_id = run_state["run_id"]
        execute_run(
            project_root=tmp_path,
            run_id=run_id,
            project=project,
            question="q",
            source_url=None,
        )
        # Simulate restart: fetch state from disk (no in-memory cache)
        loaded = get_run(tmp_path, run_id)
        assert loaded is not None
        assert loaded["status"] == RunStatus.FAILED


# ---------------------------------------------------------------------------
# SSRF protection
# ---------------------------------------------------------------------------


class TestSSRFProtection:
    def test_loopback_blocked(self) -> None:
        assert _is_ssrf_risk("http://127.0.0.1/")

    def test_private_class_a_blocked(self) -> None:
        assert _is_ssrf_risk("http://10.0.0.1/")

    def test_private_class_b_blocked(self) -> None:
        assert _is_ssrf_risk("http://172.16.0.1/")

    def test_private_class_c_blocked(self) -> None:
        assert _is_ssrf_risk("http://192.168.1.1/")

    def test_link_local_blocked(self) -> None:
        assert _is_ssrf_risk("http://169.254.169.254/latest/meta-data/")

    def test_public_url_allowed(self) -> None:
        assert not _is_ssrf_risk("https://raw.githubusercontent.com/data/file.csv")


# ---------------------------------------------------------------------------
# Path traversal (unit)
# ---------------------------------------------------------------------------


class TestPathTraversal:
    def test_dotdot_rejected(self) -> None:
        with pytest.raises(ValueError):
            _validate_identifier("../etc/passwd")

    def test_slash_rejected(self) -> None:
        with pytest.raises(ValueError):
            _validate_identifier("foo/bar")

    def test_space_rejected(self) -> None:
        with pytest.raises(ValueError):
            _validate_identifier("foo bar")

    def test_valid_identifier_accepted(self) -> None:
        assert _validate_identifier("release_impact") == "release_impact"

    def test_valid_hyphenated_accepted(self) -> None:
        assert _validate_identifier("my-project-01") == "my-project-01"


# ---------------------------------------------------------------------------
# Artifact service unit tests
# ---------------------------------------------------------------------------


class TestArtifactService:
    def test_list_projects_empty(self, tmp_path: Path) -> None:
        assert list_projects(tmp_path) == []

    def test_list_projects_finds_project(self, project_root_with_project: tuple[Path, str]) -> None:
        root, project = project_root_with_project
        projects = list_projects(root)
        assert any(p.project == project for p in projects)

    def test_read_summary_returns_dict(self, project_root_with_project: tuple[Path, str]) -> None:
        root, project = project_root_with_project
        summary = read_project_summary(root, project)
        assert isinstance(summary, dict)
        assert summary["source"] == project

    def test_read_summary_missing_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            read_project_summary(tmp_path, "ghost")

    def test_list_marts_returns_info(self, project_root_with_project: tuple[Path, str]) -> None:
        root, project = project_root_with_project
        marts = list_marts(root, project)
        assert len(marts) == 1
        assert marts[0].name == "mart_test"

    def test_mart_preview_returns_rows(self, project_root_with_project: tuple[Path, str]) -> None:
        root, project = project_root_with_project
        preview = read_mart_preview(root, project, "mart_test")
        assert preview["total_rows"] == 2
        assert len(preview["rows"]) == 2

    def test_mart_preview_pagination(self, project_root_with_project: tuple[Path, str]) -> None:
        root, project = project_root_with_project
        preview = read_mart_preview(root, project, "mart_test", page=1, page_size=1)
        assert len(preview["rows"]) == 1

    def test_mart_preview_invalid_page_size(
        self, project_root_with_project: tuple[Path, str]
    ) -> None:
        root, project = project_root_with_project
        with pytest.raises(ValueError):
            read_mart_preview(root, project, "mart_test", page=1, page_size=9999)

    def test_mart_missing_raises(self, project_root_with_project: tuple[Path, str]) -> None:
        root, project = project_root_with_project
        with pytest.raises(FileNotFoundError):
            read_mart_preview(root, project, "no_such_mart")


# ---------------------------------------------------------------------------
# Pipeline runner compatibility
# ---------------------------------------------------------------------------


class TestPipelineRunnerCompat:
    def test_run_pipeline_import_works(self) -> None:
        """Verify the pipeline runner is still importable without side effects."""
        from app.pipeline_runner import run_pipeline as _run_pipeline  # noqa: F401

    def test_run_project_from_config_import_works(self) -> None:
        from app.pipeline_runner import run_project_from_config  # noqa: F401

    def test_pipeline_api_contract_unchanged(self) -> None:
        """The original contract module still exposes its planned status."""
        from api.pipeline_api import implementation_status

        assert implementation_status() == "planned"
