"""Endpoints for browsing Quaero projects, summaries, and mart previews."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, status

from api.dependencies import get_project_root, require_api_key
from api.schemas import (
    MartPreviewResponse,
    MartsResponse,
    ProjectCreateRequest,
    ProjectCreateResponse,
    ProjectDetailResponse,
    ProjectsResponse,
)
from api.services.artifacts import (
    list_marts,
    list_projects,
    read_mart_preview,
    read_project_summary,
)
from api.services.uploads import (
    ProjectConflictError,
    UploadValidationError,
    create_project_from_upload,
)

router = APIRouter(
    prefix="/projects",
    tags=["projects"],
    dependencies=[Depends(require_api_key)],
)


@router.get("", response_model=ProjectsResponse)
async def get_projects(
    project_root: Path = Depends(get_project_root),
) -> ProjectsResponse:
    """List all available generated projects."""
    projects = list_projects(project_root)
    return ProjectsResponse(projects=projects)


@router.post(
    "",
    response_model=ProjectCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a local Quaero project from an uploaded CSV dataset",
)
async def create_project(
    body: ProjectCreateRequest,
    project_root: Path = Depends(get_project_root),
) -> ProjectCreateResponse:
    """Create a new config-driven Quaero project backed by a validated CSV upload."""
    try:
        return create_project_from_upload(project_root=project_root, request=body)
    except ProjectConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except (UploadValidationError, ValueError, PermissionError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get("/{project}/summary", response_model=ProjectDetailResponse)
async def get_project_summary(
    project: str,
    project_root: Path = Depends(get_project_root),
) -> ProjectDetailResponse:
    """Return the final summary report for a project."""
    try:
        summary = read_project_summary(project_root, project)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except PermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    return ProjectDetailResponse(project=project, summary=summary)


@router.get("/{project}/marts", response_model=MartsResponse)
async def get_project_marts(
    project: str,
    project_root: Path = Depends(get_project_root),
) -> MartsResponse:
    """List all mart tables for a project."""
    try:
        marts = list_marts(project_root, project)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except PermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    return MartsResponse(project=project, marts=marts)


@router.get("/{project}/marts/{mart}", response_model=MartPreviewResponse)
async def get_mart_preview(
    project: str,
    mart: str,
    page: int = Query(default=1, ge=1, description="1-based page number"),
    page_size: int = Query(default=100, ge=1, le=1000, description="Rows per page"),
    project_root: Path = Depends(get_project_root),
) -> MartPreviewResponse:
    """Return a paginated preview of a mart Parquet file."""
    try:
        data = read_mart_preview(project_root, project, mart, page=page, page_size=page_size)
    except (FileNotFoundError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except PermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    return MartPreviewResponse(**data)
