"""The interpretation API and the real pipeline: a question is answered as asked, or refused with the reason."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from api.dependencies import get_project_root
from api.main import app
from db import store

KEY = "interpretation-test-key"

CSV = "\n".join(
    ["order_id,region,product,units,revenue,order_date"]
    + [
        f"{i},{['North', 'South', 'East', 'West'][i % 4]},P{i % 3 + 1},{i % 7 + 1},{100 + (i * 37) % 90 + (i % 4) * 30},2024-0{i % 9 + 1}-1{i % 9}"
        for i in range(1, 49)
    ]
)
FRAME = pd.read_csv(io.StringIO(CSV))


@pytest.fixture()
def root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("QUAERO_API_KEY", KEY)
    db_path = tmp_path / "t.db"
    monkeypatch.setenv("QUAERO_DB_PATH", str(db_path))
    store.init_db(db_path)
    return tmp_path.resolve()


@pytest.fixture()
def client(root: Path):
    app.dependency_overrides[get_project_root] = lambda: root
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.clear()


def auth() -> dict[str, str]:
    return {"X-API-Key": KEY}


def upload(client: TestClient) -> str:
    res = client.post(
        "/uploads",
        files={"file": ("sales.csv", io.BytesIO(CSV.encode()), "text/csv")},
        headers=auth(),
    )
    assert res.status_code == 201, res.text
    return res.json()["upload_id"]


def interpret(client: TestClient, upload_id: str, question: str, **explicit):
    return client.post(
        f"/uploads/{upload_id}/interpretation",
        json={"question": question, **explicit},
        headers=auth(),
    )


def run_project(
    client: TestClient, root: Path, upload_id: str, name: str, question: str, **explicit
) -> dict:
    created = client.post(
        "/projects",
        json={"project": name, "upload_id": upload_id, "question": question, **explicit},
        headers=auth(),
    )
    assert created.status_code == 201, created.text
    accepted = client.post(f"/projects/{name}/run", json={"question": question}, headers=auth())
    assert accepted.status_code == 202, accepted.text
    state = client.get(f"/projects/{name}/runs/{accepted.json()['run_id']}", headers=auth()).json()
    out = {"state": state, "dir": root / "projects" / name}
    summary = out["dir"] / "metadata" / "final_summary_report.json"
    out["summary"] = json.loads(summary.read_text(encoding="utf-8")) if summary.is_file() else None
    answer = out["dir"] / "marts" / "mart_question_answer.parquet"
    out["answer"] = pd.read_parquet(answer) if answer.is_file() else None
    return out


# ------------------------------------------------------------------------------------ the preview
def test_the_preview_needs_the_api_key(client):
    assert (
        client.post(
            "/uploads/upload_0123456789ab/interpretation", json={"question": "x"}
        ).status_code
        == 401
    )


def test_a_supported_question_is_previewed_with_where_each_choice_came_from(client):
    body = interpret(client, upload(client), "Which region has the highest total revenue?").json()
    assert body["status"] == "ready"
    assert body["metric"] == {"column": "revenue", "origin": "auto_inferred"}
    assert body["dimension"] == {"column": "region", "origin": "auto_inferred"}
    assert body["aggregation"] == {"value": "sum", "origin": "auto_inferred"}
    assert body["summary"] == "Total of revenue for each region, highest first."


def test_explicit_choices_are_reported_as_the_users(client):
    body = interpret(
        client,
        upload(client),
        "What patterns are there?",
        metric_column="revenue",
        dimension_column="region",
        aggregation="sum",
    ).json()
    assert body["status"] == "ready"
    assert (
        body["metric"]["origin"]
        == body["dimension"]["origin"]
        == body["aggregation"]["origin"]
        == "user_provided"
    )


@pytest.mark.parametrize(
    ("question", "code"),
    [
        ("What is the median revenue per region?", "aggregation_not_supported"),
        ("How has revenue changed by month?", "time_grouping"),
        ("Which region had the highest revenue in 2024?", "period_filter"),
        ("Which region has the highest total revenue and units?", "multiple_metrics"),
        ("Why is revenue higher in the East?", "causal_question"),
        ("Compare revenue between North and South", "comparison_request"),
    ],
)
def test_unsupported_questions_are_refused_with_a_reason_and_a_way_out(client, question, code):
    body = interpret(client, upload(client), question).json()
    assert body["status"] == "unsupported"
    assert code in {i["code"] for i in body["issues"]}
    assert body["summary"] is None
    assert all(i["message"] for i in body["issues"] if i["severity"] == "unsupported")


def test_a_vague_question_asks_for_more(client):
    body = interpret(client, upload(client), "What patterns are there in the data?").json()
    assert body["status"] == "needs_input"
    assert body["metric"]["column"] is None


def test_the_preview_writes_nothing(client, root):
    upload_id = upload(client)
    before = sorted(str(p.relative_to(root)) for p in root.rglob("*"))
    interpret(client, upload_id, "Which region has the highest total revenue?")
    interpret(client, upload_id, "What is the median revenue per region?")
    assert sorted(str(p.relative_to(root)) for p in root.rglob("*")) == before


def test_unknown_and_malformed_uploads(client):
    assert interpret(client, "upload_0123456789ab", "Revenue by region").status_code == 404
    assert interpret(client, "not_an_upload", "Revenue by region").status_code in (400, 404)
    assert (
        client.post(
            "/uploads/upload_0123456789ab/interpretation", json={"question": "  "}, headers=auth()
        ).status_code
        == 422
    )
    assert (
        client.post(
            "/uploads/upload_0123456789ab/interpretation",
            json={"question": "x", "aggregation": "median"},
            headers=auth(),
        ).status_code
        == 422
    )


def test_the_preview_agrees_with_what_the_pipeline_then_does(client, root):
    upload_id = upload(client)
    question = "Which product has the highest average units?"
    preview = interpret(client, upload_id, question).json()
    ran = run_project(client, root, upload_id, "agree", question)
    chosen = ran["summary"]["interpretation"]
    assert ran["state"]["status"] == "COMPLETED"
    for key in ("metric", "dimension", "aggregation", "ordering"):
        assert chosen[key]["origin"] == preview[key]["origin"]
    assert (
        chosen["metric"]["column"],
        chosen["dimension"]["column"],
        chosen["aggregation"]["value"],
    ) == (
        preview["metric"]["column"],
        preview["dimension"]["column"],
        preview["aggregation"]["value"],
    )


# ----------------------------------------------------------------------------------- the real runs
def test_a_supported_question_runs_and_the_numbers_match_pandas(client, root):
    ran = run_project(
        client, root, upload(client), "totals", "Which region has the highest total revenue?"
    )
    assert ran["state"]["status"] == "COMPLETED", ran["state"]
    expected = FRAME.groupby("region")["revenue"].sum().sort_values(ascending=False)
    got = ran["answer"].set_index("region")["total_revenue"]
    assert list(ran["answer"]["region"]) == list(expected.index)
    assert got.round(2).to_dict() == expected.round(2).to_dict()
    assert ran["summary"]["interpretation"]["status"] == "ready"
    assert ran["summary"]["aggregation_selection_mode"] == "auto_inferred"


def test_most_revenue_is_a_total_not_a_count(client, root):
    ran = run_project(client, root, upload(client), "most", "Which product has the most revenue?")
    expected = FRAME.groupby("product")["revenue"].sum()
    assert ran["state"]["status"] == "COMPLETED"
    assert (
        ran["answer"].set_index("product")["total_revenue"].round(2).to_dict()
        == expected.round(2).to_dict()
    )
    assert "count_read_as_total" in {i["code"] for i in ran["summary"]["interpretation"]["issues"]}


def test_how_many_counts_rows(client, root):
    ran = run_project(
        client, root, upload(client), "howmany", "How many orders are there per region?"
    )
    assert ran["state"]["status"] == "COMPLETED"
    assert (
        ran["answer"].set_index("region")["record_count"].to_dict()
        == FRAME.groupby("region").size().to_dict()
    )
    assert ran["summary"]["metric_column"] == "record_count"


def test_an_explicit_aggregation_beats_the_wording_and_the_assumed_default(client, root):
    upload_id = upload(client)
    assumed = run_project(
        client,
        root,
        upload_id,
        "assumed",
        "Revenue by region",
        metric_column="revenue",
        dimension_column="region",
    )
    assert assumed["summary"]["aggregation_selection_mode"] == "assumed_default"
    assert "avg_revenue" in assumed["answer"].columns
    chosen = run_project(
        client,
        root,
        upload_id,
        "chosen",
        "Revenue by region",
        metric_column="revenue",
        dimension_column="region",
        aggregation="sum",
    )
    assert chosen["summary"]["aggregation_selection_mode"] == "user_provided"
    assert (
        chosen["answer"].set_index("region")["total_revenue"].round(2).to_dict()
        == FRAME.groupby("region")["revenue"].sum().round(2).to_dict()
    )
    assert (
        json.loads((chosen["dir"] / "project_config.json").read_text(encoding="utf-8"))["datasets"][
            0
        ]["aggregation"]
        == "sum"
    )


def test_a_run_can_override_the_aggregation_without_changing_the_wording(client, root):
    upload_id = upload(client)
    created = client.post(
        "/projects",
        json={
            "project": "override",
            "upload_id": upload_id,
            "question": "Which region has the highest total revenue?",
        },
        headers=auth(),
    )
    assert created.status_code == 201 and created.json()["aggregation"] is None
    accepted = client.post(
        "/projects/override/run",
        json={
            "question": "Revenue by region",
            "metric_column": "revenue",
            "dimension_column": "region",
            "aggregation": "count",
        },
        headers=auth(),
    )
    state = client.get(
        f"/projects/override/runs/{accepted.json()['run_id']}", headers=auth()
    ).json()
    assert state["status"] == "COMPLETED", state
    answer = pd.read_parquet(
        root / "projects" / "override" / "marts" / "mart_question_answer.parquet"
    )
    assert (
        answer.set_index("region")["record_count"].to_dict()
        == FRAME.groupby("region").size().to_dict()
    )


@pytest.mark.parametrize(
    ("question", "word"),
    [
        ("What is the median revenue per region?", "median"),
        ("How has revenue changed by month?", "month"),
        ("Which region had the highest revenue in 2024?", "period"),
        ("Why is revenue higher in the East?", "why"),
        ("What patterns are there in the data?", "specific"),
    ],
)
def test_a_question_the_pipeline_cannot_answer_fails_with_an_explanation_and_no_answer(
    client, root, question, word
):
    ran = run_project(client, root, upload(client), "refused", question)
    assert ran["state"]["status"] == "FAILED"
    assert word in ran["state"]["error"].lower()
    assert (
        "cannot be answered" in ran["state"]["error"]
        or "not specific enough" in ran["state"]["error"]
    )
    assert ran["answer"] is None  # no mart that could be mistaken for the answer


def test_project_creation_rejects_an_aggregation_that_does_not_exist(client):
    res = client.post(
        "/projects",
        json={
            "project": "bad",
            "upload_id": upload(client),
            "question": "q",
            "aggregation": "median",
        },
        headers=auth(),
    )
    assert res.status_code == 422


def test_existing_projects_without_an_aggregation_still_run(client, root):
    """A configuration written before this field existed has no aggregation and must keep working."""
    upload_id = upload(client)
    ran = run_project(
        client, root, upload_id, "legacy", "Which region has the highest average revenue?"
    )
    config = json.loads((ran["dir"] / "project_config.json").read_text(encoding="utf-8"))
    assert "aggregation" not in config["datasets"][0]
    assert ran["state"]["status"] == "COMPLETED"
