"""Pydantic request and response models for the Quaero FastAPI layer."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

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
    metric_column: str | None = Field(
        default=None,
        max_length=128,
        description="Optional explicit metric column override for single-dataset projects.",
    )
    dimension_column: str | None = Field(
        default=None,
        max_length=128,
        description="Optional explicit dimension column override for single-dataset projects.",
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


# ---------------------------------------------------------------------------
# Local CSV Uploads & Project Creation
# ---------------------------------------------------------------------------


class UploadColumnInfo(BaseModel):
    name: str
    normalized_name: str
    dtype: str
    null_count: int
    unique_count: int
    sample_values: list[str]


class UploadResponse(BaseModel):
    upload_id: str
    original_filename: str
    size_bytes: int
    row_count: int
    column_count: int
    delimiter: str
    columns: list[UploadColumnInfo]
    numeric_columns: list[str]
    candidate_dimensions: list[str]
    candidate_time_columns: list[str]
    created_at: str
    #: Server session that created the upload; lets the lifecycle tell this session's temporary files apart.
    session_id: str | None = None


class UploadsResponse(BaseModel):
    uploads: list[UploadResponse]


class ProjectCreateRequest(BaseModel):
    project: str = Field(..., min_length=1, max_length=64)
    upload_id: str = Field(..., min_length=1, max_length=64)
    question: str = Field(..., min_length=1, max_length=1000)
    dataset_name: str | None = Field(default=None, max_length=64)
    metric_column: str | None = Field(default=None, max_length=128)
    dimension_column: str | None = Field(default=None, max_length=128)
    time_column: str | None = Field(default=None, max_length=128)

    @field_validator("project", "upload_id", "question")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("field must not be blank")
        return v.strip()


class ProjectCreateResponse(BaseModel):
    project: str
    upload_id: str
    dataset_name: str
    question: str
    metric_column: str | None
    dimension_column: str | None
    time_column: str | None
    has_config: bool
    created_at: str


# ---------------------------------------------------------------------------
# Local data lifecycle
# ---------------------------------------------------------------------------

LifecyclePolicy = Literal["ask_on_exit", "auto_cleanup", "keep_files"]
CleanupScope = Literal["current_session", "orphaned", "single_upload", "project_artifacts"]


class StorageUploadItem(BaseModel):
    upload_id: str
    original_filename: str | None
    size_bytes: int
    created_at: str | None
    #: ``saved`` = a project uses it; otherwise temporary, owned by this session or left by an earlier one.
    state: Literal["current_session", "orphaned", "saved"]
    linked_projects: list[str]
    #: One of the two files (CSV or metadata) is missing, usually after an interrupted deletion.
    incomplete: bool


class StorageProjectItem(BaseModel):
    project: str
    #: ``upload_project`` was created from an upload; ``preexisting`` projects are never deleted here.
    kind: Literal["upload_project", "preexisting"]
    upload_id: str | None
    config_bytes: int
    artifacts_bytes: int
    artifact_files: int
    marts_count: int
    can_clear_artifacts: bool


class StorageTotals(BaseModel):
    temporary_uploads_bytes: int
    saved_uploads_bytes: int
    project_configs_bytes: int
    artifacts_bytes: int
    total_bytes: int


class StorageStatusResponse(BaseModel):
    session_id: str
    lifecycle_policy: LifecyclePolicy
    totals: StorageTotals
    current_session_uploads: int
    orphaned_uploads: int
    uploads: list[StorageUploadItem]
    projects: list[StorageProjectItem]


class LifecyclePolicyRequest(BaseModel):
    lifecycle_policy: LifecyclePolicy


class LifecyclePolicyResponse(BaseModel):
    lifecycle_policy: LifecyclePolicy


class CleanupRequest(BaseModel):
    scope: CleanupScope
    #: Must be true: a permanent deletion is never implied.
    confirm: bool
    #: ``single_upload``: an upload id; ``project_artifacts``: a project name. Never a path.
    target_id: str | None = Field(default=None, max_length=64)


class CleanupSkipped(BaseModel):
    id: str
    reason: str


class CleanupResponse(BaseModel):
    scope: CleanupScope
    deleted: list[str]
    freed_bytes: int
    skipped: list[CleanupSkipped]
    message: str
