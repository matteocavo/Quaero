"""Pure helpers for the operations console (no Streamlit, no network)."""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import datetime
from typing import Any
from urllib.parse import urlparse

ACTIVE_STATUSES = frozenset({"QUEUED", "RUNNING"})
TERMINAL_STATUSES = frozenset({"COMPLETED", "FAILED"})

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_-]+$")
MAX_QUESTION_LENGTH = 1000
MAX_POLLS = 100
POLL_INTERVAL_SECONDS = 3

_STATUS_LABELS = {
    "QUEUED": "⏳ QUEUED",
    "RUNNING": "▶️ RUNNING",
    "COMPLETED": "✅ COMPLETED",
    "FAILED": "❌ FAILED",
}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_identifier(value: str) -> str | None:
    """Return an error message when *value* is not a valid project identifier."""
    if not value:
        return "Project name is required."
    if not _IDENTIFIER_RE.match(value):
        return "Project name may contain only letters, digits, underscores and hyphens."
    return None


def validate_run_request(
    project: str,
    question: str,
    source_url: str,
    has_config: bool,
) -> list[str]:
    """Mirror the API's input rules so obvious mistakes are caught before the request."""
    errors: list[str] = []
    project_error = validate_identifier(project.strip())
    if project_error:
        errors.append(project_error)

    question = question.strip()
    if not question:
        errors.append("A business question is required.")
    elif len(question) > MAX_QUESTION_LENGTH:
        errors.append(f"The question is limited to {MAX_QUESTION_LENGTH} characters.")

    url = source_url.strip()
    if url:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            errors.append("The source URL must be a valid http(s) URL.")
    elif not has_config:
        errors.append(
            "This project has no project_config.json: provide a source URL to run the pipeline."
        )
    return errors


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


def status_label(status: str) -> str:
    return _STATUS_LABELS.get(status, status)


def is_active(status: str | None) -> bool:
    return status in ACTIVE_STATUSES


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def format_duration(seconds: float) -> str:
    total = max(int(seconds), 0)
    minutes, secs = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def run_duration(run: dict[str, Any]) -> str:
    """Duration between ``started_at`` and ``completed_at``; ``-`` when not computable."""
    started = parse_timestamp(run.get("started_at"))
    finished = parse_timestamp(run.get("completed_at"))
    if started is None or finished is None:
        return "-"
    return format_duration((finished - started).total_seconds())


def latest_active_run(runs: Iterable[dict[str, Any]]) -> dict[str, Any] | None:
    """First run (history is newest first) that is still QUEUED or RUNNING."""
    for run in runs:
        if is_active(run.get("status")):
            return run
    return None


def runs_to_rows(runs: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "status": status_label(run.get("status", "")),
            "created": run.get("created_at"),
            "started": run.get("started_at"),
            "completed": run.get("completed_at"),
            "duration": run_duration(run),
            "question": run.get("question"),
            "error": run.get("error"),
            "run_id": run.get("run_id"),
        }
        for run in runs
    ]


def should_poll(auto_refresh: bool, runs: Iterable[dict[str, Any]], polls_done: int) -> bool:
    """Bounded polling: only while enabled, a run is active and the poll budget is left."""
    return auto_refresh and polls_done < MAX_POLLS and latest_active_run(runs) is not None


# ---------------------------------------------------------------------------
# Tables and filters
# ---------------------------------------------------------------------------


def filter_rows(rows: list[dict[str, Any]], text: str) -> list[dict[str, Any]]:
    """Case-insensitive substring filter across all values of the given rows."""
    needle = text.strip().lower()
    if not needle:
        return rows
    return [row for row in rows if any(needle in str(value).lower() for value in row.values())]


def total_pages(total_rows: int, page_size: int) -> int:
    if page_size < 1:
        raise ValueError("page_size must be >= 1")
    return max(1, -(-total_rows // page_size))


def dq_columns_to_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "column": col["column"],
            "dtype": col.get("dtype"),
            "completeness": col["completeness"],
            "uniqueness": col["uniqueness"],
            "format_score": col["format_score"],
            "dq_score": col["dq_score"],
            "outlier_flag": col["outlier_flag"],
        }
        for col in report.get("columns", [])
    ]


def anomalies_to_rows(report: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for item in report.get("anomalies", []):
        row = {
            "column": item["column"],
            "method": item["method"],
            "threshold": item.get("threshold"),
            "anomaly_count": item["anomaly_count"],
            "example_values": ", ".join(str(v) for v in item.get("example_values", [])),
        }
        if report.get("version") == "v2":
            row["frequency"] = item.get("frequency")
            row["impact"] = item.get("impact")
            row["suggested_action"] = item.get("suggested_action")
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------

_LAYER_COLORS = {"raw": "#f4d6d6", "staging": "#d6e4f4", "mart": "#d6f4de"}
_EDGE_COLORS = {
    "passthrough": "#888888",
    "normalized": "#1f77b4",
    "type_cast": "#d62728",
    "aggregated": "#2ca02c",
}


def _dot_escape(value: Any) -> str:
    text = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return text.replace("\r", " ").replace("\n", " ")


def lineage_edge_types(lineage: dict[str, Any]) -> list[str]:
    return sorted({edge["type"] for edge in lineage.get("edges", [])})


def lineage_to_dot(lineage: dict[str, Any], edge_types: Iterable[str] | None = None) -> str:
    """Build a Graphviz DOT string from ``lineage.json`` data.

    Node ids are synthetic (``n0``, ``n1``...) and every label is escaped, so artifact
    content can never inject DOT syntax or markup. Edges referencing unknown nodes are
    skipped.
    """
    allowed = set(edge_types) if edge_types is not None else None
    node_ids: dict[str, str] = {}
    lines = ["digraph lineage {", "  rankdir=LR;", '  node [shape=box, style="rounded,filled"];']

    for index, node in enumerate(lineage.get("nodes", [])):
        node_ids[node["id"]] = f"n{index}"
        label = node["name"] if node["layer"] != "mart" else f"{node.get('mart')}.{node['name']}"
        color = _LAYER_COLORS.get(node["layer"], "#eeeeee")
        lines.append(
            f'  n{index} [label="{_dot_escape(label)}", fillcolor="{color}", '
            f'tooltip="{_dot_escape(node["layer"])}"];'
        )

    for edge in lineage.get("edges", []):
        if allowed is not None and edge["type"] not in allowed:
            continue
        source, target = node_ids.get(edge["from"]), node_ids.get(edge["to"])
        if source is None or target is None:
            continue
        color = _EDGE_COLORS.get(edge["type"], "#888888")
        lines.append(
            f'  {source} -> {target} [color="{color}", label="{_dot_escape(edge["type"])}"];'
        )

    lines.append("}")
    return "\n".join(lines)
