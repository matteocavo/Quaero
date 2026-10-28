"""Read-only access to the generated metadata artifacts used by the operations console.

Artifacts are located by file-name convention inside ``projects/<project>/metadata/``
(the summary report is not trusted for paths: its schema differs across versions).

Which artifacts exist depends on how the project was run:

* single-dataset runs (``run_pipeline``) write the data-quality report, the lineage
  document and the framed (v2) anomaly report;
* config-driven multi-dataset runs (``run_project_from_config``) write only the raw (v1)
  anomaly report.

A missing artifact is therefore a normal state and is reported with ``FileNotFoundError``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from api.schemas import (
    AnomalyResponse,
    ArtifactAvailability,
    DataQualityReport,
    DataQualityResponse,
    LineageResponse,
)
from api.services.artifacts import _safe_project_dir

MAX_ARTIFACT_BYTES = 10 * 1024 * 1024

_DQ_PATTERN = "dq_score_*.json"
_ANOMALY_V2_PATTERN = "anomaly_report_v2_*.json"
_ANOMALY_V1_PATTERN = "anomaly_report_*.json"
_LINEAGE_FILE = "lineage.json"
_METRICS_FILE = "metrics_definitions.json"
_SUMMARY_FILE = "final_summary_report.json"


class ArtifactReadError(Exception):
    """An artifact exists but cannot be read or does not have the expected format."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _project_dir(project_root: Path, project: str) -> Path:
    project_dir = _safe_project_dir(project_root, project)
    if not project_dir.is_dir():
        raise FileNotFoundError(f"Project '{project}' not found.")
    return project_dir


def _find(metadata_dir: Path, pattern: str) -> list[Path]:
    """Return regular files matching *pattern* that really live inside *metadata_dir*."""
    if not metadata_dir.is_dir():
        return []
    root = metadata_dir.resolve()
    found: list[Path] = []
    for path in sorted(metadata_dir.glob(pattern)):
        if path.is_file() and path.resolve().is_relative_to(root):
            found.append(path)
    return found


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        if path.stat().st_size > MAX_ARTIFACT_BYTES:
            raise ArtifactReadError(f"Artifact '{path.name}' exceeds the size limit.")
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ArtifactReadError(f"Artifact '{path.name}' is not readable JSON.") from exc
    if not isinstance(data, dict):
        raise ArtifactReadError(f"Artifact '{path.name}' is not a JSON object.")
    return data


def _validate(model: Any, data: dict[str, Any], name: str) -> Any:
    try:
        return model.model_validate(data)
    except ValidationError as exc:
        raise ArtifactReadError(f"Artifact '{name}' has an unexpected format.") from exc


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------


def read_project_overview(project_root: Path, project: str) -> dict[str, Any]:
    """Return the project facts the console needs, without the run history."""
    project_dir = _project_dir(project_root, project)
    metadata_dir = project_dir / "metadata"

    summary: dict[str, Any] | None = None
    summary_path = metadata_dir / _SUMMARY_FILE
    if summary_path.is_file():
        try:
            summary = _read_json_object(summary_path)
        except ArtifactReadError:
            summary = None

    config_path = project_dir / "project_config.json"
    config_fallback: dict[str, Any] | None = None
    if summary is None and not summary_path.is_file() and config_path.is_file():
        try:
            config_fallback = _read_json_object(config_path)
        except ArtifactReadError:
            config_fallback = None

    marts_dir = project_dir / "marts"
    marts_count = len(list(marts_dir.glob("*.parquet"))) if marts_dir.is_dir() else 0

    if summary is not None:
        project_mode: str | None = str(summary.get("project_mode") or "single_dataset")
    elif config_fallback is not None and isinstance(config_fallback.get("datasets"), list):
        project_mode = "multi_dataset" if len(config_fallback["datasets"]) > 1 else "single_dataset"
    else:
        project_mode = None

    availability = ArtifactAvailability(
        summary=summary_path.is_file(),
        data_quality=bool(_find(metadata_dir, _DQ_PATTERN)),
        anomalies=bool(_find(metadata_dir, _ANOMALY_V1_PATTERN)),
        lineage=bool(_find(metadata_dir, _LINEAGE_FILE)),
        metrics_definitions=bool(_find(metadata_dir, _METRICS_FILE)),
    )

    def _text(key: str) -> str | None:
        value = summary.get(key) if summary else None
        if isinstance(value, str):
            return value
        if config_fallback is not None and key == "question":
            fallback_val = config_fallback.get("question")
            if isinstance(fallback_val, str):
                return fallback_val
        return None

    return {
        "project": project,
        "project_mode": project_mode,
        "question": _text("question"),
        "primary_mart": _text("primary_mart"),
        "primary_kpi": _text("primary_kpi"),
        "marts_count": marts_count,
        "has_config": config_path.is_file(),
        "artifacts": availability,
    }


# ---------------------------------------------------------------------------
# Data quality
# ---------------------------------------------------------------------------


def read_data_quality(project_root: Path, project: str) -> DataQualityResponse:
    """Return every data-quality report found for the project (usually one)."""
    project_dir = _project_dir(project_root, project)
    files = _find(project_dir / "metadata", _DQ_PATTERN)
    if not files:
        raise FileNotFoundError(
            f"No data-quality report found for project '{project}'. "
            "It is generated by single-dataset runs only."
        )
    reports: list[DataQualityReport] = [
        _validate(DataQualityReport, _read_json_object(path), path.name) for path in files
    ]
    return DataQualityResponse(project=project, reports=reports)


# ---------------------------------------------------------------------------
# Anomalies
# ---------------------------------------------------------------------------


def read_anomalies(project_root: Path, project: str) -> AnomalyResponse:
    """Return the framed (v2) anomaly report, falling back to the raw (v1) one.

    When several reports of the same version exist, the most recently modified wins.
    """
    project_dir = _project_dir(project_root, project)
    metadata_dir = project_dir / "metadata"

    version = "v2"
    candidates = _find(metadata_dir, _ANOMALY_V2_PATTERN)
    if not candidates:
        version = "v1"
        candidates = [
            path
            for path in _find(metadata_dir, _ANOMALY_V1_PATTERN)
            if not path.name.startswith("anomaly_report_v2_")
        ]
    if not candidates:
        raise FileNotFoundError(f"No anomaly report found for project '{project}'.")

    path = max(candidates, key=lambda p: (p.stat().st_mtime, p.name))
    data = _read_json_object(path)
    payload = {
        "project": project,
        "source": data.get("source"),
        "version": version,
        "anomaly_row_count": data.get("anomaly_row_count", 0),
        "business_summary": data.get("business_summary"),
        "anomalies": data.get("anomalies", []),
    }
    return _validate(AnomalyResponse, payload, path.name)


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------


def read_lineage(project_root: Path, project: str) -> LineageResponse:
    """Return the column-level lineage document (``lineage.json``)."""
    project_dir = _project_dir(project_root, project)
    files = _find(project_dir / "metadata", _LINEAGE_FILE)
    if not files:
        raise FileNotFoundError(
            f"No lineage found for project '{project}'. "
            "It is generated by single-dataset runs only."
        )
    data = _read_json_object(files[0])
    payload = {
        "project": project,
        "source": data.get("source"),
        "generated_at": data.get("generated_at"),
        "node_count": data.get("node_count", len(data.get("nodes", []))),
        "edge_count": data.get("edge_count", len(data.get("edges", []))),
        "nodes": data.get("nodes", []),
        "edges": data.get("edges", []),
    }
    return _validate(LineageResponse, payload, files[0].name)
