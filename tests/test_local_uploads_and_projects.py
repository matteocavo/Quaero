"""Unit, security, and end-to-end integration tests for local CSV uploads and project creation."""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api.dependencies import get_project_root
from api.main import app
from db import store

_TEST_KEY = "local-ops-secret-key"

_SAMPLE_CSV = (
    "order_year,region,channel,orders,revenue\n"
    "2021,North,Direct,120,14500.50\n"
    "2021,South,Partner,95,11200.00\n"
    "2022,North,Direct,150,18900.75\n"
    "2022,South,Partner,110,13400.25\n"
    "2023,East,Online,180,22100.00\n"
    "2023,West,Direct,140,17300.50\n"
    "2024,North,Online,210,26400.00\n"
    "2024,East,Online,990,285000.00\n"
)


@pytest.fixture()
def workspace_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path.resolve()
    db_path = root / "test_quaero.db"
    monkeypatch.setenv("QUAERO_DB_PATH", str(db_path))
    monkeypatch.setenv("QUAERO_API_KEY", _TEST_KEY)
    store.init_db(db_path)
    return root


@pytest.fixture()
def client(workspace_root: Path):
    app.dependency_overrides[get_project_root] = lambda: workspace_root
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.clear()


def _auth() -> dict[str, str]:
    return {"X-API-Key": _TEST_KEY}


class TestUploadAuthentication:
    def test_upload_requires_api_key(self, client: TestClient) -> None:
        files = {"file": ("sales.csv", io.BytesIO(_SAMPLE_CSV.encode("utf-8")), "text/csv")}
        assert client.post("/uploads", files=files).status_code == 401
        assert client.get("/uploads").status_code == 401
        assert client.get("/uploads/upload_1234567890ab").status_code == 401

    def test_create_project_requires_api_key(self, client: TestClient) -> None:
        payload = {
            "project": "demo_proj",
            "upload_id": "upload_1234567890ab",
            "question": "Which region has the highest revenue?",
        }
        assert client.post("/projects", json=payload).status_code == 401


class TestUploadValidationAndSecurity:
    def test_valid_csv_upload_succeeds_and_isolates_storage(
        self, client: TestClient, workspace_root: Path
    ) -> None:
        files = {
            "file": ("regional_sales.csv", io.BytesIO(_SAMPLE_CSV.encode("utf-8")), "text/csv")
        }
        resp = client.post("/uploads", files=files, headers=_auth())
        assert resp.status_code == 201, resp.text
        data = resp.json()

        assert data["upload_id"].startswith("upload_")
        assert data["original_filename"] == "regional_sales.csv"
        assert data["row_count"] == 8
        assert data["column_count"] == 5
        assert data["delimiter"] == ","
        assert "revenue" in data["numeric_columns"]
        assert "orders" in data["numeric_columns"]
        assert "region" in data["candidate_dimensions"]
        assert "order_year" in data["candidate_time_columns"]

        # Untrusted original filename is NEVER written to disk
        assert not (workspace_root / "uploads" / "regional_sales.csv").exists()
        stored_csv = workspace_root / "uploads" / f"{data['upload_id']}.csv"
        stored_meta = workspace_root / "uploads" / f"{data['upload_id']}.json"
        assert stored_csv.is_file()
        assert stored_meta.is_file()

        # Listing and detail endpoints return the upload
        list_resp = client.get("/uploads", headers=_auth())
        assert list_resp.status_code == 200
        assert [u["upload_id"] for u in list_resp.json()["uploads"]] == [data["upload_id"]]

        detail_resp = client.get(f"/uploads/{data['upload_id']}", headers=_auth())
        assert detail_resp.status_code == 200
        assert detail_resp.json()["upload_id"] == data["upload_id"]

    @pytest.mark.parametrize(
        "bad_filename",
        [
            "../secret.csv",
            "..\\secret.csv",
            "nested/path.csv",
            "nested\\path.csv",
            "C:drive.csv",
            "malicious.exe",
            "data.json",
            "archive.zip",
            "no_extension",
        ],
    )
    def test_rejects_unsafe_or_unsupported_filenames(
        self, client: TestClient, workspace_root: Path, bad_filename: str
    ) -> None:
        files = {"file": (bad_filename, io.BytesIO(_SAMPLE_CSV.encode("utf-8")), "text/csv")}
        resp = client.post("/uploads", files=files, headers=_auth())
        assert resp.status_code == 400
        uploads_dir = workspace_root / "uploads"
        assert not uploads_dir.exists() or list(uploads_dir.glob("*")) == []

    def test_rejects_disallowed_content_type(self, client: TestClient) -> None:
        files = {"file": ("sales.csv", io.BytesIO(_SAMPLE_CSV.encode("utf-8")), "application/json")}
        resp = client.post("/uploads", files=files, headers=_auth())
        assert resp.status_code == 400
        assert "content type" in resp.json()["detail"].lower()

    def test_enforces_configurable_max_upload_size(
        self, client: TestClient, workspace_root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("QUAERO_MAX_UPLOAD_BYTES", "64")
        files = {"file": ("large.csv", io.BytesIO(_SAMPLE_CSV.encode("utf-8")), "text/csv")}
        resp = client.post("/uploads", files=files, headers=_auth())
        assert resp.status_code == 413
        assert "maximum allowed size" in resp.json()["detail"].lower()
        uploads_dir = workspace_root / "uploads"
        assert not uploads_dir.exists() or list(uploads_dir.glob("*")) == []

    @pytest.mark.parametrize(
        ("payload", "expected_substring"),
        [
            (b"", "empty"),
            (b"   \n  \n", "whitespace"),
            (b"a,b\x00c\n1,2\n", "binary null bytes"),
            (b"\xff\xfe\xfd\xfc", "utf-8"),
            (b"<!DOCTYPE html><html><body>not a csv</body></html>", "html"),
            (b'{"a": 1, "b": 2}\n{"a": 3, "b": 4}', "json"),
            (b"col_a,col_b\n", "at least one data row"),
            (b"only_one_column\n10\n20\n", "at least 2 columns"),
            (b"col_a,col_b,col_c\n1,2,3\n4,5\n", "expected 3 columns"),
            (b"col_a,col_b\n1,2\n", None),
            (b"dup,DUP\n1,2\n3,4\n", "duplicate column names"),
            (b'col_a,col_b\n"unclosed_quote,2\n', "malformed csv"),
        ],
    )
    def test_validates_csv_content_not_just_extension(
        self,
        client: TestClient,
        payload: bytes,
        expected_substring: str | None,
    ) -> None:
        files = {"file": ("dataset.csv", io.BytesIO(payload), "text/csv")}
        resp = client.post("/uploads", files=files, headers=_auth())
        if expected_substring is None:
            assert resp.status_code == 201
        else:
            assert resp.status_code == 400, resp.text
            assert expected_substring in resp.json()["detail"].lower()

    def test_supports_semicolon_delimited_csv(self, client: TestClient) -> None:
        semicolon_csv = "region;revenue;orders\nNorth;1200;10\nSouth;1500;12\n"
        files = {"file": ("euro_sales.csv", io.BytesIO(semicolon_csv.encode("utf-8")), "text/csv")}
        resp = client.post("/uploads", files=files, headers=_auth())
        assert resp.status_code == 201
        body = resp.json()
        assert body["delimiter"] == ";"
        assert body["row_count"] == 2
        assert body["column_count"] == 3


class TestProjectCreationAndEndToEndPipeline:
    def _upload_sample(self, client: TestClient) -> dict:
        files = {"file": ("sales_2024.csv", io.BytesIO(_SAMPLE_CSV.encode("utf-8")), "text/csv")}
        resp = client.post("/uploads", files=files, headers=_auth())
        assert resp.status_code == 201
        return resp.json()

    def test_create_project_preserves_existing_projects(
        self, client: TestClient, workspace_root: Path
    ) -> None:
        upload = self._upload_sample(client)
        existing_dir = workspace_root / "projects" / "existing_project"
        existing_dir.mkdir(parents=True)
        marker = existing_dir / "project_config.json"
        marker.write_text('{"preserve": true}', encoding="utf-8")

        resp = client.post(
            "/projects",
            json={
                "project": "existing_project",
                "upload_id": upload["upload_id"],
                "question": "How does revenue vary by region?",
            },
            headers=_auth(),
        )
        assert resp.status_code == 409
        assert marker.read_text(encoding="utf-8") == '{"preserve": true}'

    def test_create_project_validates_columns_and_identifiers(self, client: TestClient) -> None:
        upload = self._upload_sample(client)

        # Path traversal project name rejected
        bad_name = client.post(
            "/projects",
            json={
                "project": "../escape",
                "upload_id": upload["upload_id"],
                "question": "Question?",
            },
            headers=_auth(),
        )
        assert bad_name.status_code == 400

        # Non-existent upload_id rejected with 404
        missing_upload = client.post(
            "/projects",
            json={
                "project": "valid_project",
                "upload_id": "upload_000000000000",
                "question": "Question?",
            },
            headers=_auth(),
        )
        assert missing_upload.status_code == 404

        # Non-numeric metric column rejected with 400
        bad_metric = client.post(
            "/projects",
            json={
                "project": "valid_project",
                "upload_id": upload["upload_id"],
                "question": "Question?",
                "metric_column": "region",
            },
            headers=_auth(),
        )
        assert bad_metric.status_code == 400
        assert "numeric" in bad_metric.json()["detail"].lower()

        # Unknown dimension column rejected with 400
        bad_dim = client.post(
            "/projects",
            json={
                "project": "valid_project",
                "upload_id": upload["upload_id"],
                "question": "Question?",
                "dimension_column": "nonexistent_col",
            },
            headers=_auth(),
        )
        assert bad_dim.status_code == 400
        assert "not found" in bad_dim.json()["detail"].lower()

    def test_end_to_end_upload_create_configure_run_and_explore_results(
        self, client: TestClient, workspace_root: Path
    ) -> None:
        # 1. Upload CSV outside repository
        upload = self._upload_sample(client)
        upload_id = upload["upload_id"]

        # 2. Create local project
        create_resp = client.post(
            "/projects",
            json={
                "project": "Regional Revenue 2024",
                "upload_id": upload_id,
                "question": "Which region generates the highest total revenue?",
                "metric_column": "revenue",
                "dimension_column": "region",
            },
            headers=_auth(),
        )
        assert create_resp.status_code == 201, create_resp.text
        created = create_resp.json()
        project_id = created["project"]
        assert project_id == "regional_revenue_2024"
        assert created["metric_column"] == "revenue"
        assert created["dimension_column"] == "region"
        assert created["time_column"] == "order_year"
        assert created["has_config"] is True

        # Pre-run overview shows project configuration immediately
        pre_overview = client.get(f"/projects/{project_id}/overview", headers=_auth())
        assert pre_overview.status_code == 200
        assert pre_overview.json()["project_mode"] == "single_dataset"
        assert (
            pre_overview.json()["question"] == "Which region generates the highest total revenue?"
        )
        assert pre_overview.json()["has_config"] is True
        assert pre_overview.json()["marts_count"] == 0

        # 3 & 4. Launch real Python pipeline
        run_resp = client.post(
            f"/projects/{project_id}/run",
            json={
                "question": "Which region generates the highest total revenue?",
                "metric_column": "revenue",
                "dimension_column": "region",
            },
            headers=_auth(),
        )
        assert run_resp.status_code == 202, run_resp.text
        run_id = run_resp.json()["run_id"]

        # 5. Monitor run status
        status_resp = client.get(f"/projects/{project_id}/runs/{run_id}", headers=_auth())
        assert status_resp.status_code == 200
        status_data = status_resp.json()
        assert status_data["status"] == "COMPLETED", status_data

        # Overview reflects generated artifacts
        post_overview = client.get(f"/projects/{project_id}/overview", headers=_auth()).json()
        assert post_overview["marts_count"] >= 4
        assert post_overview["artifacts"] == {
            "summary": True,
            "data_quality": True,
            "anomalies": True,
            "lineage": True,
            "metrics_definitions": True,
        }
        assert post_overview["last_run"]["status"] == "COMPLETED"

        # 6. Explore generated marts
        marts_resp = client.get(f"/projects/{project_id}/marts", headers=_auth())
        assert marts_resp.status_code == 200
        mart_names = {m["name"] for m in marts_resp.json()["marts"]}
        assert "mart_question_answer" in mart_names
        assert "mart_top_entities" in mart_names
        assert "mart_time_trend" in mart_names

        preview_resp = client.get(
            f"/projects/{project_id}/marts/mart_question_answer",
            headers=_auth(),
        )
        assert preview_resp.status_code == 200
        preview = preview_resp.json()
        assert preview["total_rows"] == 4
        assert "region" in preview["columns"]

        # 7. Inspect Data Quality and anomalies
        dq_resp = client.get(f"/projects/{project_id}/data-quality", headers=_auth())
        assert dq_resp.status_code == 200
        dq_data = dq_resp.json()
        assert len(dq_data["reports"]) == 1
        assert dq_data["reports"][0]["overall_score"] > 0

        anom_resp = client.get(f"/projects/{project_id}/anomalies", headers=_auth())
        assert anom_resp.status_code == 200
        anom_data = anom_resp.json()
        assert anom_data["version"] == "v2"
        assert anom_data["anomaly_row_count"] >= 1
        assert anom_data["business_summary"] is not None

        # 8. View lineage
        lineage_resp = client.get(f"/projects/{project_id}/lineage", headers=_auth())
        assert lineage_resp.status_code == 200
        lineage_data = lineage_resp.json()
        assert lineage_data["node_count"] > 0
        assert lineage_data["edge_count"] > 0

        # 9. Summary report available for download/inspection
        summary_resp = client.get(f"/projects/{project_id}/summary", headers=_auth())
        assert summary_resp.status_code == 200
        summary_data = summary_resp.json()["summary"]
        assert summary_data["metric_column"] == "revenue"
        assert summary_data["dimension_column"] == "region"
