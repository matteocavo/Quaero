"""Read-only endpoints feeding the operations console: overview, data quality, anomalies, lineage."""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from api.dependencies import get_project_root, require_api_key
from api.schemas import (
    AnomalyResponse,
    DataQualityResponse,
    LineageResponse,
    ProjectOverviewResponse,
    RunHistoryItem,
    RunStatus,
)
from api.services.reports import (
    ArtifactReadError,
    read_anomalies,
    read_data_quality,
    read_lineage,
    read_project_overview,
)

LOGGER = logging.getLogger(__name__)

router = APIRouter(
    prefix="/projects",
    tags=["reports"],
    dependencies=[Depends(require_api_key)],
)


def _http_error(exc: Exception) -> HTTPException:
    """Map service exceptions to HTTP errors without leaking internals."""
    if isinstance(exc, (FileNotFoundError, ValueError)):
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    if isinstance(exc, PermissionError):
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    LOGGER.warning("Unreadable artifact: %s", exc)
    return HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail="The requested artifact exists but could not be read.",
    )


def _history_item(row: dict[str, Any]) -> RunHistoryItem:
    return RunHistoryItem(
        run_id=row["run_id"],
        project=row["project_name"],
        status=RunStatus(row["status"]),
        question=row.get("question"),
        source_url=row.get("source_url"),
        created_at=row["created_at"],
        started_at=row.get("started_at"),
        completed_at=row.get("completed_at"),
        error=row.get("error"),
        artifact_path=row.get("artifact_path"),
    )


@router.get("/{project}/overview", response_model=ProjectOverviewResponse)
async def get_project_overview(
    project: str,
    project_root: Path = Depends(get_project_root),
) -> ProjectOverviewResponse:
    """Project facts, artifact availability and the latest run.

    ``history_available`` is false (and ``last_run`` null) when the SQLite history cannot
    be read, so an outage is distinguishable from a project that never ran.
    """
    try:
        overview = read_project_overview(project_root, project)
    except (FileNotFoundError, ValueError, PermissionError, ArtifactReadError) as exc:
        raise _http_error(exc) from exc

    last_run: RunHistoryItem | None = None
    history_available = True
    try:
        from db import store

        rows = store.list_runs(project_name=project, limit=1)
        last_run = _history_item(rows[0]) if rows else None
    except sqlite3.Error:
        history_available = False

    return ProjectOverviewResponse(
        **overview,
        last_run=last_run,
        history_available=history_available,
    )


@router.get("/{project}/data-quality", response_model=DataQualityResponse)
async def get_data_quality(
    project: str,
    project_root: Path = Depends(get_project_root),
) -> DataQualityResponse:
    """Data-quality score reports (single-dataset runs only)."""
    try:
        return read_data_quality(project_root, project)
    except (FileNotFoundError, ValueError, PermissionError, ArtifactReadError) as exc:
        raise _http_error(exc) from exc


@router.get("/{project}/anomalies", response_model=AnomalyResponse)
async def get_anomalies(
    project: str,
    project_root: Path = Depends(get_project_root),
) -> AnomalyResponse:
    """Anomaly report: framed (v2) when available, otherwise the raw (v1) report."""
    try:
        return read_anomalies(project_root, project)
    except (FileNotFoundError, ValueError, PermissionError, ArtifactReadError) as exc:
        raise _http_error(exc) from exc


@router.get("/{project}/lineage", response_model=LineageResponse)
async def get_lineage(
    project: str,
    project_root: Path = Depends(get_project_root),
) -> LineageResponse:
    """Column-level lineage graph from ``lineage.json`` (single-dataset runs only)."""
    try:
        return read_lineage(project_root, project)
    except (FileNotFoundError, ValueError, PermissionError, ArtifactReadError) as exc:
        raise _http_error(exc) from exc
