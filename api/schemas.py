"""Pydantic request and response models for the Quaero FastAPI layer."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, HttpUrl, field_validator

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class RunStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str = "1.0.0"


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------


class ProjectSummary(BaseModel):
    project: str
    marts_count: int
    has_summary: bool


class ProjectsResponse(BaseModel):
    projects: list[ProjectSummary]


# ---------------------------------------------------------------------------
# Project detail
# ---------------------------------------------------------------------------


class ProjectDetailResponse(BaseModel):
    project: str
    summary: dict[str, Any]


# ---------------------------------------------------------------------------
# Marts
# ---------------------------------------------------------------------------


class MartInfo(BaseModel):
    name: str
    path: str


class MartsResponse(BaseModel):
    project: str
    marts: list[MartInfo]


class MartPreviewResponse(BaseModel):
    project: str
    mart: str
    columns: list[str]
    rows: list[dict[str, Any]]
    total_rows: int
    page: int
    page_size: int


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


class RunRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=1000)
    source_url: HttpUrl | None = Field(
        default=None,
        description="Optional dataset URL to re-run the pipeline against.",
    )

    @field_validator("question")
    @classmethod
    def question_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("question must not be blank")
        return v.strip()


class RunAcceptedResponse(BaseModel):
    run_id: str
    project: str
    status: RunStatus = RunStatus.QUEUED
    message: str = "Run accepted and queued."


class RunStatusResponse(BaseModel):
    run_id: str
    project: str
    status: RunStatus
    error: str | None = None
    result_keys: list[str] = Field(
        default_factory=list,
        description="Top-level keys returned by the pipeline on success.",
    )


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ErrorResponse(BaseModel):
    detail: str


# ---------------------------------------------------------------------------
# Run history (Phase 2)
# ---------------------------------------------------------------------------


class RunHistoryItem(BaseModel):
    run_id: str
    project: str
    status: RunStatus
    question: str | None = None
    source_url: str | None = None
    created_at: str
    started_at: str | None = None
    completed_at: str | None = None
    error: str | None = None
    artifact_path: str | None = None


class RunHistoryResponse(BaseModel):
    project: str
    runs: list[RunHistoryItem]
    total: int
