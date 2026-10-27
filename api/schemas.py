"""Pydantic request and response models for the Quaero FastAPI layer."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

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
    total: int = Field(
        description=(
            "Total number of runs recorded for the project, independent of ``limit``. "
            "``len(runs)`` may be smaller than ``total`` when the history is truncated."
        ),
    )


# ---------------------------------------------------------------------------
# Operations console: project overview and artifact readers (Phase 3)
# ---------------------------------------------------------------------------


class ArtifactAvailability(BaseModel):
    """Which generated artifacts exist on disk for a project."""

    summary: bool
    data_quality: bool
    anomalies: bool
    lineage: bool
    metrics_definitions: bool


class ProjectOverviewResponse(BaseModel):
    project: str
    project_mode: str | None = Field(
        default=None,
        description=(
            "``multi_dataset`` when the summary says so; ``single_dataset`` when a summary "
            "exists without a ``project_mode`` key (single-dataset summaries carry none); "
            "null when there is no readable summary."
        ),
    )
    question: str | None = None
    primary_mart: str | None = None
    primary_kpi: str | None = None
    marts_count: int
    has_config: bool = Field(
        description="True when the project has a project_config.json (re-runnable without a URL)."
    )
    artifacts: ArtifactAvailability
    last_run: RunHistoryItem | None = None
    history_available: bool = Field(
        description="False when the SQLite run history could not be read; last_run is then null."
    )


class DataQualityColumn(BaseModel):
    column: str
    dtype: str | None = None
    completeness: float
    uniqueness: float
    outlier_flag: bool
    format_score: float
    dq_score: float

    @field_validator("column", mode="before")
    @classmethod
    def _column_to_str(cls, value: Any) -> str:
        return str(value)


class DataQualityReport(BaseModel):
    source: str
    generated_at: str | None = None
    overall_score: float
    column_count: int
    columns: list[DataQualityColumn]


class DataQualityResponse(BaseModel):
    project: str
    reports: list[DataQualityReport]


class AnomalyItem(BaseModel):
    column: str
    method: str
    threshold: str | None = None
    anomaly_count: int
    example_values: list[Any] = Field(default_factory=list)
    frequency: float | None = Field(default=None, description="v2 only: share of flagged rows.")
    impact: str | None = Field(default=None, description="v2 only: low, medium or high.")
    suggested_action: str | None = Field(default=None, description="v2 only.")

    @field_validator("column", mode="before")
    @classmethod
    def _column_to_str(cls, value: Any) -> str:
        return str(value)


class AnomalyResponse(BaseModel):
    project: str
    source: str | None = None
    version: str = Field(description="``v2`` (framed report) or ``v1`` (raw detection report).")
    anomaly_row_count: int
    business_summary: str | None = Field(default=None, description="v2 only.")
    anomalies: list[AnomalyItem]


class LineageNode(BaseModel):
    id: str
    layer: str
    name: str
    dtype: str | None = None
    mart: str | None = None

    @field_validator("name", mode="before")
    @classmethod
    def _name_to_str(cls, value: Any) -> str:
        return str(value)


class LineageEdge(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    from_: str = Field(alias="from")
    to: str
    type: str


class LineageResponse(BaseModel):
    project: str
    source: str | None = None
    generated_at: str | None = None
    node_count: int
    edge_count: int
    nodes: list[LineageNode]
    edges: list[LineageEdge]
