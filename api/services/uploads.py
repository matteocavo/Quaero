"""Secure local CSV upload and project creation services for Quaero API.

Security invariants
-------------------
* Uploaded filenames are validated for extension and illegal characters, and
  are NEVER used to construct filesystem paths.
* Every upload is assigned a server-generated identifier (``upload_<12hex>``)
  and stored strictly under ``<project_root>/uploads/`` (which is untracked
  and outside any publicly served directory).
* Every resolved path is checked with ``Path.is_relative_to`` before I/O.
* File size is bounded by ``QUAERO_MAX_UPLOAD_BYTES`` (default 10 MiB) and
  enforced while reading the upload stream.
* CSV content is validated structurally (UTF-8 encoding, no null bytes,
  consistent column counts across rows, valid headers, at least one data row,
  and clean pandas parsing).
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import UploadFile
from pandas.api.types import is_bool_dtype, is_numeric_dtype

from api.schemas import (
    ProjectCreateRequest,
    ProjectCreateResponse,
    UploadColumnInfo,
    UploadResponse,
)
from api.services.artifacts import _validate_identifier
from api.services.lifecycle import LIFECYCLE_LOCK, current_session_id
from pipelines.mart_builder import find_time_column
from pipelines.utils import normalize_name

LOGGER = logging.getLogger(__name__)

DEFAULT_MAX_UPLOAD_BYTES = 10 * 1024 * 1024  # 10 MiB
MAX_FILENAME_LENGTH = 255
MAX_UPLOAD_ROWS = 200_000
MAX_UPLOAD_COLUMNS = 200
_CHUNK_SIZE = 64 * 1024

_ALLOWED_EXTENSIONS = {".csv", ".tsv", ".txt"}
_ALLOWED_CONTENT_TYPES = {
    "",
    "text/csv",
    "text/plain",
    "text/tab-separated-values",
    "application/csv",
    "application/x-csv",
    "application/vnd.ms-excel",
    "application/octet-stream",
}
_CANDIDATE_DELIMITERS = [",", ";", "\t", "|"]
_PROJECT_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _-]{0,63}$")
_DISGUISED_MARKUP_PREFIXES = ("<!doctype", "<html", "<?xml", "<script")


class UploadValidationError(ValueError):
    """Raised when an uploaded file fails filename, type, or content checks."""


class UploadTooLargeError(ValueError):
    """Raised when an uploaded file exceeds the configured byte limit."""


class ProjectConflictError(ValueError):
    """Raised when attempting to create a project that already exists."""


def get_max_upload_bytes() -> int:
    """Return the configured maximum upload size in bytes."""
    raw = os.environ.get("QUAERO_MAX_UPLOAD_BYTES", "").strip()
    if not raw:
        return DEFAULT_MAX_UPLOAD_BYTES
    try:
        value = int(raw)
        if value <= 0:
            return DEFAULT_MAX_UPLOAD_BYTES
        return value
    except ValueError:
        return DEFAULT_MAX_UPLOAD_BYTES


def _uploads_dir(project_root: Path) -> Path:
    """Return the resolved ``<project_root>/uploads`` directory, creating it if needed."""
    root = project_root.resolve()
    uploads = (root / "uploads").resolve()
    if not uploads.is_relative_to(root):
        raise UploadValidationError("Uploads directory escaped project root.")
    uploads.mkdir(parents=True, exist_ok=True)
    return uploads


def _safe_upload_paths(project_root: Path, upload_id: str) -> tuple[Path, Path]:
    """Return ``(csv_path, meta_path)`` inside ``<project_root>/uploads``."""
    _validate_identifier(upload_id)
    if not upload_id.startswith("upload_"):
        raise UploadValidationError(f"Invalid upload identifier '{upload_id}'.")
    uploads = _uploads_dir(project_root)
    csv_path = (uploads / f"{upload_id}.csv").resolve()
    meta_path = (uploads / f"{upload_id}.json").resolve()
    if not csv_path.is_relative_to(uploads) or not meta_path.is_relative_to(uploads):
        raise UploadValidationError("Resolved upload path escaped uploads directory.")
    return csv_path, meta_path


def validate_upload_filename(filename: str | None, content_type: str | None = None) -> str:
    """Validate the untrusted original filename and optional MIME content type.

    Returns the sanitized display filename (never used as a filesystem path).
    """
    if not filename or not filename.strip():
        raise UploadValidationError("Uploaded file must have a filename.")

    raw = filename.strip()
    if len(raw) > MAX_FILENAME_LENGTH:
        raise UploadValidationError(
            f"Filename exceeds maximum length of {MAX_FILENAME_LENGTH} characters."
        )

    if "\x00" in raw or "/" in raw or "\\" in raw or ":" in raw or ".." in raw:
        raise UploadValidationError("Filename contains illegal path or traversal characters.")

    ext = Path(raw).suffix.lower()
    if ext not in _ALLOWED_EXTENSIONS:
        allowed = ", ".join(sorted(_ALLOWED_EXTENSIONS))
        raise UploadValidationError(
            f"Unsupported file extension '{ext or '(none)'}'. Allowed extensions: {allowed}."
        )

    if content_type is not None:
        normalized_ct = content_type.split(";")[0].strip().lower()
        if normalized_ct not in _ALLOWED_CONTENT_TYPES:
            raise UploadValidationError(
                f"Unsupported content type '{normalized_ct}'. Upload a valid CSV file."
            )

    return raw


def _detect_delimiter(sample_lines: list[str], filename: str) -> str:
    """Detect the CSV delimiter from header and initial data rows."""
    if not sample_lines:
        return ","

    header_line = sample_lines[0]
    best_delim = ","
    best_count = -1

    preferred = (
        ["\t", ",", ";", "|"] if filename.lower().endswith(".tsv") else _CANDIDATE_DELIMITERS
    )
    for delim in preferred:
        try:
            parsed_rows = list(csv.reader(sample_lines[:10], delimiter=delim, strict=True))
        except csv.Error:
            continue
        if not parsed_rows or len(parsed_rows[0]) < 2:
            continue
        col_count = len(parsed_rows[0])
        if all(len(row) == col_count for row in parsed_rows if any(cell.strip() for cell in row)):
            if col_count > best_count:
                best_count = col_count
                best_delim = delim

    if best_count >= 2:
        return best_delim

    # Fallback count on header line
    for delim in preferred:
        count = header_line.count(delim)
        if count > best_count:
            best_count = count
            best_delim = delim
    return best_delim if best_count > 0 else ","


def _validate_and_parse_csv_bytes(
    raw_bytes: bytes,
    filename: str,
) -> tuple[pd.DataFrame, str]:
    """Validate raw bytes as UTF-8 CSV content and return ``(dataframe, delimiter)``."""
    if not raw_bytes:
        raise UploadValidationError("Uploaded CSV file is empty.")

    if b"\x00" in raw_bytes:
        raise UploadValidationError(
            "Uploaded file contains binary null bytes and is not a valid CSV."
        )

    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise UploadValidationError(
            "Uploaded file is not valid UTF-8 text. Save the CSV with UTF-8 encoding."
        ) from exc

    stripped = text.strip()
    if not stripped:
        raise UploadValidationError("Uploaded CSV file contains only whitespace.")

    lower_prefix = stripped[:64].lstrip().lower()
    if lower_prefix.startswith(_DISGUISED_MARKUP_PREFIXES) or lower_prefix.startswith(("{", "[")):
        raise UploadValidationError(
            "Uploaded file appears to be HTML, XML, or JSON rather than tabular CSV data."
        )

    non_empty_lines = [line for line in stripped.splitlines() if line.strip()]
    if len(non_empty_lines) < 2:
        raise UploadValidationError(
            "Uploaded CSV must contain a header row and at least one data row."
        )

    delimiter = _detect_delimiter(non_empty_lines[:25], filename)

    try:
        reader = csv.reader(io.StringIO(stripped), delimiter=delimiter, strict=True)
        rows = [row for row in reader if any(cell.strip() != "" for cell in row)]
    except csv.Error as exc:
        raise UploadValidationError(f"Malformed CSV structure: {exc}") from exc

    if len(rows) < 2:
        raise UploadValidationError(
            "Uploaded CSV must contain a header row and at least one non-empty data row."
        )

    header = [cell.strip() for cell in rows[0]]
    col_count = len(header)
    if col_count < 2:
        raise UploadValidationError(
            "Uploaded CSV must contain at least 2 columns (e.g. a dimension and a metric)."
        )
    if col_count > MAX_UPLOAD_COLUMNS:
        raise UploadValidationError(
            f"Uploaded CSV has {col_count} columns, exceeding limit of {MAX_UPLOAD_COLUMNS}."
        )

    if any(not col for col in header):
        raise UploadValidationError("Uploaded CSV contains empty column header names.")

    normalized_headers = [normalize_name(col) for col in header]
    if any(not col for col in normalized_headers):
        raise UploadValidationError(
            "Uploaded CSV contains column headers with no alphanumeric characters."
        )
    if len(set(normalized_headers)) != len(normalized_headers):
        raise UploadValidationError(
            "Uploaded CSV contains duplicate column names after normalization."
        )

    data_row_count = len(rows) - 1
    if data_row_count > MAX_UPLOAD_ROWS:
        raise UploadValidationError(
            f"Uploaded CSV has {data_row_count} rows, exceeding limit of {MAX_UPLOAD_ROWS}."
        )

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) != col_count:
            raise UploadValidationError(
                f"Malformed CSV at line {idx}: expected {col_count} columns, found {len(row)}."
            )

    try:
        dataframe = pd.read_csv(
            io.StringIO(stripped),
            sep=delimiter,
            on_bad_lines="error",
            low_memory=False,
        )
    except Exception as exc:
        raise UploadValidationError(f"Failed to parse CSV content: {exc}") from exc

    dataframe = dataframe.dropna(how="all")
    if dataframe.empty:
        raise UploadValidationError("Uploaded CSV has no valid data rows after parsing.")

    return dataframe, delimiter


def _build_column_metadata(
    dataframe: pd.DataFrame,
) -> tuple[
    list[UploadColumnInfo],
    list[str],
    list[str],
    list[str],
]:
    """Extract column summaries and candidate metric/dimension/time lists."""
    columns_info: list[UploadColumnInfo] = []
    numeric_columns: list[str] = []
    candidate_dimensions: list[str] = []
    candidate_time_columns: list[str] = []

    normalized_df = dataframe.copy()
    normalized_df.columns = [normalize_name(str(c)) for c in dataframe.columns]
    inferred_time = find_time_column(normalized_df)

    for raw_col in dataframe.columns:
        series = dataframe[raw_col]
        norm_col = normalize_name(str(raw_col))
        non_null = series.dropna()
        null_count = int(series.isna().sum())
        unique_count = int(non_null.nunique())
        samples = [str(v) for v in non_null.head(3).tolist()]

        is_num = bool(is_numeric_dtype(series) and not is_bool_dtype(series))
        is_time = bool(
            norm_col == inferred_time
            or any(
                token in norm_col
                for token in ("year", "date", "month", "quarter", "period", "timestamp")
            )
        )

        if is_num:
            dtype_label = "integer" if "int" in str(series.dtype).lower() else "float"
            if not (
                norm_col in {"year", "release_year"} or norm_col.endswith("_id") or norm_col == "id"
            ):
                numeric_columns.append(norm_col)
            elif norm_col not in numeric_columns:
                # Keep year/id columns available as fallback numeric or dimension
                pass
        elif is_time:
            dtype_label = "datetime"
        elif is_bool_dtype(series):
            dtype_label = "boolean"
        else:
            dtype_label = "string"

        if is_time and norm_col not in candidate_time_columns:
            candidate_time_columns.append(norm_col)

        if not is_num or is_time or norm_col in {"year", "release_year"}:
            candidate_dimensions.append(norm_col)

        columns_info.append(
            UploadColumnInfo(
                name=str(raw_col).strip(),
                normalized_name=norm_col,
                dtype=dtype_label,
                null_count=null_count,
                unique_count=unique_count,
                sample_values=samples,
            )
        )

    # Ensure at least all numeric columns are listed if only year/id was numeric
    if not numeric_columns:
        for raw_col in dataframe.columns:
            series = dataframe[raw_col]
            if is_numeric_dtype(series) and not is_bool_dtype(series):
                numeric_columns.append(normalize_name(str(raw_col)))

    # Ensure candidate_dimensions has fallback options if all columns were numeric
    if not candidate_dimensions:
        candidate_dimensions = [c.normalized_name for c in columns_info]

    return columns_info, numeric_columns, candidate_dimensions, candidate_time_columns


async def store_uploaded_csv(
    project_root: Path,
    upload_file: UploadFile,
    max_bytes: int | None = None,
) -> UploadResponse:
    """Validate and persist an uploaded CSV file inside ``<project_root>/uploads/``."""
    effective_max_bytes = max_bytes if max_bytes is not None else get_max_upload_bytes()
    safe_display_name = validate_upload_filename(upload_file.filename, upload_file.content_type)

    chunks: list[bytes] = []
    total_bytes = 0
    while True:
        chunk = await upload_file.read(_CHUNK_SIZE)
        if not chunk:
            break
        total_bytes += len(chunk)
        if total_bytes > effective_max_bytes:
            raise UploadTooLargeError(
                f"Uploaded file exceeds maximum allowed size of {effective_max_bytes} bytes."
            )
        chunks.append(chunk)

    raw_bytes = b"".join(chunks)
    dataframe, delimiter = _validate_and_parse_csv_bytes(raw_bytes, safe_display_name)
    (
        columns_info,
        numeric_columns,
        candidate_dimensions,
        candidate_time_columns,
    ) = _build_column_metadata(dataframe)

    upload_id = f"upload_{uuid.uuid4().hex[:12]}"
    csv_path, meta_path = _safe_upload_paths(project_root, upload_id)

    # Store canonical UTF-8 comma-separated CSV so downstream ingestion is deterministic
    dataframe.to_csv(csv_path, index=False, encoding="utf-8")

    created_at = datetime.now(timezone.utc).isoformat()
    response = UploadResponse(
        upload_id=upload_id,
        original_filename=safe_display_name,
        size_bytes=total_bytes,
        row_count=int(len(dataframe)),
        column_count=int(len(dataframe.columns)),
        delimiter=delimiter,
        columns=columns_info,
        numeric_columns=numeric_columns,
        candidate_dimensions=candidate_dimensions,
        candidate_time_columns=candidate_time_columns,
        created_at=created_at,
        session_id=current_session_id(),
    )

    meta_path.write_text(
        json.dumps(response.model_dump(), indent=2),
        encoding="utf-8",
    )
    LOGGER.info(
        "Stored uploaded CSV %s (%s, %d rows, %d cols)",
        upload_id,
        safe_display_name,
        response.row_count,
        response.column_count,
    )
    return response


def list_uploads(project_root: Path) -> list[UploadResponse]:
    """Return metadata for all validated uploads in ``<project_root>/uploads/``."""
    uploads = _uploads_dir(project_root)
    results: list[UploadResponse] = []
    for meta_file in sorted(uploads.glob("upload_*.json"), reverse=True):
        try:
            if not meta_file.resolve().is_relative_to(uploads):
                continue
            csv_peer = uploads / f"{meta_file.stem}.csv"
            if not csv_peer.is_file():
                continue
            data = json.loads(meta_file.read_text(encoding="utf-8"))
            results.append(UploadResponse.model_validate(data))
        except Exception as exc:
            LOGGER.warning("Skipping unreadable upload metadata %s: %s", meta_file.name, exc)
    results.sort(key=lambda item: item.created_at, reverse=True)
    return results


def get_upload(project_root: Path, upload_id: str) -> UploadResponse:
    """Return metadata for a single upload by ``upload_id``."""
    csv_path, meta_path = _safe_upload_paths(project_root, upload_id)
    if not csv_path.is_file() or not meta_path.is_file():
        raise FileNotFoundError(f"Upload '{upload_id}' not found.")
    data = json.loads(meta_path.read_text(encoding="utf-8"))
    return UploadResponse.model_validate(data)


def _validate_and_normalize_project_name(raw_name: str) -> str:
    """Validate a user-supplied project name and return its canonical project slug."""
    cleaned = raw_name.strip()
    if not cleaned or "\x00" in cleaned or "/" in cleaned or "\\" in cleaned or ".." in cleaned:
        raise ValueError(f"Invalid project name '{raw_name}'.")
    if not _PROJECT_NAME_RE.match(cleaned):
        raise ValueError(
            f"Invalid project name '{raw_name}'. Use letters, digits, spaces, hyphens, or underscores."
        )
    normalized = normalize_name(cleaned)
    if not normalized:
        raise ValueError(f"Invalid project name '{raw_name}'.")
    _validate_identifier(normalized)
    return normalized


def _resolve_optional_column(
    requested_column: str | None,
    upload_meta: UploadResponse,
    role: str,
) -> str | None:
    """Validate an optional column name against the uploaded dataset's columns."""
    if requested_column is None or not requested_column.strip():
        return None
    target_norm = normalize_name(requested_column.strip())
    by_norm = {col.normalized_name: col for col in upload_meta.columns}
    by_raw = {col.name: col for col in upload_meta.columns}

    matched = by_norm.get(target_norm) or by_raw.get(requested_column.strip())
    if matched is None:
        available = ", ".join(c.normalized_name for c in upload_meta.columns[:10])
        raise ValueError(
            f"{role} column '{requested_column}' was not found in upload '{upload_meta.upload_id}'. "
            f"Available columns: {available}."
        )
    if role == "Metric" and matched.dtype not in {"integer", "float"}:
        raise ValueError(
            f"Metric column '{matched.normalized_name}' must be numeric (found {matched.dtype})."
        )
    return matched.normalized_name


def create_project_from_upload(
    project_root: Path,
    request: ProjectCreateRequest,
) -> ProjectCreateResponse:
    """Create a new local Quaero project backed by a validated CSV upload."""
    # Held so an upload cannot be deleted as temporary between the checks below and the config being written.
    with LIFECYCLE_LOCK:
        return _create_project_from_upload(project_root, request)


def _create_project_from_upload(
    project_root: Path,
    request: ProjectCreateRequest,
) -> ProjectCreateResponse:
    project_id = _validate_and_normalize_project_name(request.project)
    upload_meta = get_upload(project_root, request.upload_id)
    csv_path, _ = _safe_upload_paths(project_root, upload_meta.upload_id)

    projects_root = (project_root / "projects").resolve()
    projects_root.mkdir(parents=True, exist_ok=True)
    project_dir = (projects_root / project_id).resolve()
    if not project_dir.is_relative_to(projects_root):
        raise ValueError("Resolved project directory escaped projects root.")

    if project_dir.exists():
        raise ProjectConflictError(
            f"Project '{project_id}' already exists. Choose a different project name."
        )

    metric_col = _resolve_optional_column(request.metric_column, upload_meta, "Metric")
    dimension_col = _resolve_optional_column(request.dimension_column, upload_meta, "Dimension")
    time_col = _resolve_optional_column(request.time_column, upload_meta, "Time")
    if time_col is None and upload_meta.candidate_time_columns:
        time_col = upload_meta.candidate_time_columns[0]

    raw_dataset_name = (
        request.dataset_name or Path(upload_meta.original_filename).stem or project_id
    )
    dataset_name = normalize_name(raw_dataset_name) or project_id

    dataset_entry: dict[str, Any] = {
        "name": dataset_name,
        "path": f"uploads/{upload_meta.upload_id}.csv",
        "source_format": "csv",
        "read_options": {
            "sep": ",",
            "encoding": "utf-8",
        },
    }
    if metric_col:
        dataset_entry["metric_column"] = metric_col
    if dimension_col:
        dataset_entry["dimension_column"] = dimension_col
    if time_col:
        dataset_entry["time_column"] = time_col

    config_payload: dict[str, Any] = {
        "project": project_id,
        "question": request.question.strip(),
        "upload_id": upload_meta.upload_id,
        "datasets": [dataset_entry],
    }

    for subdir in ("raw", "staging", "marts", "metadata"):
        (project_dir / subdir).mkdir(parents=True, exist_ok=True)

    config_path = project_dir / "project_config.json"
    config_path.write_text(json.dumps(config_payload, indent=2), encoding="utf-8")

    # Best-effort SQLite registration
    try:
        from db import store

        store.register_project(name=project_id, root_path=str(project_dir))
    except Exception as exc:
        LOGGER.warning("DB: could not register newly created project %r: %s", project_id, exc)

    created_at = datetime.now(timezone.utc).isoformat()
    LOGGER.info(
        "Created local project %r from upload %r (%s)",
        project_id,
        upload_meta.upload_id,
        csv_path.name,
    )
    return ProjectCreateResponse(
        project=project_id,
        upload_id=upload_meta.upload_id,
        dataset_name=dataset_name,
        question=config_payload["question"],
        metric_column=metric_col,
        dimension_column=dimension_col,
        time_column=time_col,
        has_config=True,
        created_at=created_at,
    )
