"""Real HTTP integration tests of every endpoint the web frontend uses.

A real ``uvicorn`` process serves the FastAPI app over a temporary project root, so these
tests exercise the same HTTP surface as the Vite proxy in Local Operations mode, including a
real pipeline run started with ``POST /projects/{project}/run``.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator

import httpx
import pytest
from _ts_interfaces import REPO_ROOT, TYPES_FILE, read_interfaces
from test_reports_api import make_single_dataset_project

pytestmark = pytest.mark.skipif(not TYPES_FILE.is_file(), reason="frontend/ is not present")

KEY = "frontend-integration-key"

_LOCAL_CSV = (
    "release_year,provider,catalog,streams\n"
    "2019,Provider A,Catalog 1,100\n"
    "2020,Provider B,Catalog 1,150\n"
    "2021,Provider A,Catalog 2,210\n"
    "2022,Provider C,Catalog 2,260\n"
    "2023,Provider B,Catalog 3,330\n"
)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def live_api(tmp_path_factory: pytest.TempPathFactory) -> Iterator[httpx.Client]:
    root = tmp_path_factory.mktemp("frontend_integration").resolve()

    # "demo": artifacts written by the real pipeline writers (single-dataset layout).
    make_single_dataset_project(root, "demo")
    marts = root / "projects" / "demo" / "marts"
    marts.mkdir(parents=True, exist_ok=True)
    import pandas as pd

    pd.DataFrame({"year": [2020, 2021, 2022], "streams": [1, 2, 3]}).to_parquet(
        marts / "mart_trend.parquet", index=False
    )

    # "localdemo": a config-driven project over a local CSV, runnable through POST /run.
    local = root / "projects" / "localdemo"
    local.mkdir(parents=True)
    (root / "data.csv").write_text(_LOCAL_CSV, encoding="utf-8")
    (local / "project_config.json").write_text(
        json.dumps(
            {
                "project": "localdemo",
                "question": "Which release years generate the strongest average streams?",
                "datasets": [
                    {
                        "name": "streams_data",
                        "path": str(root / "data.csv"),
                        "time_column": "release_year",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    port = _free_port()
    env = {
        **os.environ,
        "QUAERO_API_KEY": KEY,
        "QUAERO_PROJECT_ROOT": str(root),
        "QUAERO_DB_PATH": str(root / "quaero.db"),
        "PYTHONPATH": str(REPO_ROOT),
    }
    server = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "api.main:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    client = httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=15)
    try:
        for _ in range(80):
            try:
                if client.get("/health").status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.25)
        else:
            raise RuntimeError("uvicorn did not start")
        yield client
    finally:
        client.close()
        server.terminate()
        server.wait(timeout=15)


def _auth() -> dict[str, str]:
    return {"X-API-Key": KEY}


def _keys(interface: str) -> set[str]:
    return set(read_interfaces()[interface])


def test_health_is_public_and_matches_the_frontend_type(live_api: httpx.Client) -> None:
    resp = live_api.get("/health")
    assert resp.status_code == 200
    assert set(resp.json()) == _keys("HealthResponse")


def test_protected_endpoints_require_the_key(live_api: httpx.Client) -> None:
    assert live_api.get("/projects").status_code == 401
    assert live_api.get("/projects", headers={"X-API-Key": "wrong"}).status_code == 401


def test_project_explorer(live_api: httpx.Client) -> None:
    body = live_api.get("/projects", headers=_auth()).json()
    assert set(body) == _keys("ProjectsResponse")
    names = {p["project"] for p in body["projects"]}
    assert {"demo", "localdemo"} <= names
    assert set(body["projects"][0]) == _keys("ProjectSummary")

    overview = live_api.get("/projects/demo/overview", headers=_auth()).json()
    assert set(overview) == _keys("ProjectOverviewResponse")
    assert set(overview["artifacts"]) == _keys("ArtifactAvailability")
    assert overview["artifacts"]["data_quality"] is True
    assert overview["marts_count"] == 1

    summary = live_api.get("/projects/demo/summary", headers=_auth()).json()
    assert set(summary) == _keys("ProjectDetailResponse")


def test_mart_explorer(live_api: httpx.Client) -> None:
    marts = live_api.get("/projects/demo/marts", headers=_auth()).json()
    assert set(marts) == _keys("MartsResponse")
    assert set(marts["marts"][0]) == _keys("MartInfo")
    assert marts["marts"][0]["name"] == "mart_trend"

    preview = live_api.get(
        "/projects/demo/marts/mart_trend", params={"page": 1, "page_size": 2}, headers=_auth()
    ).json()
    assert set(preview) == _keys("MartPreviewResponse")
    assert preview["total_rows"] == 3
    assert len(preview["rows"]) == 2
    assert preview["columns"] == ["year", "streams"]


def test_data_quality_anomalies_and_lineage(live_api: httpx.Client) -> None:
    dq = live_api.get("/projects/demo/data-quality", headers=_auth()).json()
    assert set(dq) == _keys("DataQualityResponse")
    assert set(dq["reports"][0]) == _keys("DataQualityReport")
    assert set(dq["reports"][0]["columns"][0]) == _keys("DataQualityColumn")

    anomalies = live_api.get("/projects/demo/anomalies", headers=_auth()).json()
    assert set(anomalies) == _keys("AnomalyResponse")
    assert anomalies["version"] == "v2"
    assert set(anomalies["anomalies"][0]) == _keys("AnomalyItem")

    lineage = live_api.get("/projects/demo/lineage", headers=_auth()).json()
    assert set(lineage) == _keys("LineageResponse")
    assert set(lineage["nodes"][0]) >= _keys("LineageNode") - {"dtype", "mart"}
    assert set(lineage["edges"][0]) == _keys("LineageEdge")  # "from" is serialized by alias


def test_missing_artifacts_are_404_with_a_message(live_api: httpx.Client) -> None:
    for path in ("data-quality", "lineage", "anomalies"):
        resp = live_api.get(f"/projects/localdemo/{path}", headers=_auth())
        assert resp.status_code == 404, path
        assert isinstance(resp.json()["detail"], str)
    assert live_api.get("/projects/ghost/overview", headers=_auth()).status_code == 404


def test_run_pipeline_from_a_project_config_end_to_end(live_api: httpx.Client) -> None:
    before = live_api.get("/projects/localdemo/overview", headers=_auth()).json()
    assert before["has_config"] is True
    assert before["last_run"] is None
    assert before["history_available"] is True

    accepted = live_api.post(
        "/projects/localdemo/run",
        json={"question": "Which release years generate the strongest average streams?"},
        headers=_auth(),
    )
    assert accepted.status_code == 202
    assert set(accepted.json()) == _keys("RunAcceptedResponse")
    run_id = accepted.json()["run_id"]

    status = {}
    for _ in range(120):
        status = live_api.get(f"/projects/localdemo/runs/{run_id}", headers=_auth()).json()
        if status["status"] in {"COMPLETED", "FAILED"}:
            break
        time.sleep(0.5)
    assert set(status) == _keys("RunStatusResponse")
    assert status["status"] == "COMPLETED", status.get("error")
    assert "marts" in status["result_keys"]

    history = live_api.get("/projects/localdemo/runs", params={"limit": 10}, headers=_auth()).json()
    assert set(history) == _keys("RunHistoryResponse")
    assert set(history["runs"][0]) == _keys("RunHistoryItem")
    assert history["total"] == 1
    assert history["runs"][0]["status"] == "COMPLETED"
    assert history["runs"][0]["started_at"] and history["runs"][0]["completed_at"]

    after = live_api.get("/projects/localdemo/overview", headers=_auth()).json()
    assert after["last_run"]["run_id"] == run_id
    assert after["marts_count"] >= 1

    marts = live_api.get("/projects/localdemo/marts", headers=_auth()).json()["marts"]
    assert marts
    preview = live_api.get(f"/projects/localdemo/marts/{marts[0]['name']}", headers=_auth()).json()
    assert preview["total_rows"] >= 1


def test_run_input_validation(live_api: httpx.Client) -> None:
    bad_name = live_api.post("/projects/bad.name/run", json={"question": "q?"}, headers=_auth())
    assert bad_name.status_code == 422
    blank = live_api.post("/projects/localdemo/run", json={"question": "   "}, headers=_auth())
    assert blank.status_code == 422
    bad_url = live_api.post(
        "/projects/localdemo/run",
        json={"question": "q?", "source_url": "file:///etc/passwd"},
        headers=_auth(),
    )
    assert bad_url.status_code == 422
