"""Cleaning pipeline utilities for the staging layer."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pipelines.utils import normalize_name, resolve_project_root

from pandas.api.types import (
    is_bool_dtype,
    is_numeric_dtype,
    is_object_dtype,
    is_string_dtype,
)


LOGGER = logging.getLogger(__name__)

NUMERIC_CONVERSION_THRESHOLD = 0.8
DATE_COLUMN_HINTS = (
    "date",
    "datetime",
    "timestamp",
    "created_at",
    "updated_at",
    "release_date",
)


def clean_dataframe(
    dataframe: pd.DataFrame,
    source_name: str,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Clean a DataFrame, persist staging output, and write cleaning metadata.

    In addition to the existing cleaning_summary JSON, this function now emits
    a ``cleaning_log_{source}.json`` file in ``metadata/``.  Each entry in the
    log describes one discrete transformation: which step, which column, how
    many rows were affected, and a human-readable rationale.  This log is the
    foundation for the Data Lineage Tracker (see ``pipelines/lineage.py``).
    """
    root_dir = resolve_project_root(project_root, __file__)
    normalized_source_name = normalize_name(source_name)
    staging_dir = root_dir / "staging"
    metadata_dir = root_dir / "metadata"
    staging_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Cleaning started for source %s", normalized_source_name)

    run_id = str(uuid.uuid4())[:8]
    decisions: list[dict[str, Any]] = []

    cleaned = dataframe.copy()
    original_columns = list(cleaned.columns)

    # Step 1 — column name normalisation
    old_to_new = {col: normalize_name(col) for col in cleaned.columns}
    renamed = {old: new for old, new in old_to_new.items() if old != new}
    cleaned.columns = [old_to_new[c] for c in cleaned.columns]
    if renamed:
        decisions.append(
            _decision(
                step="column_normalization",
                action="normalize_column_names",
                columns=list(renamed.keys()),
                rows_affected=None,
                details=f"Renamed {len(renamed)} column(s): "
                + ", ".join(f'"{k}" -> "{v}"' for k, v in renamed.items()),
            )
        )

    # Step 2 — missing value standardisation
    cleaned, blank_decisions = _standardize_missing_values_logged(cleaned)
    decisions.extend(blank_decisions)

    # Step 3 — type enforcement
    cleaned, type_conversion_issue, type_decisions = _apply_type_enforcement_logged(
        cleaned
    )
    decisions.extend(type_decisions)

    # Step 4 — date parsing
    cleaned, invalid_date_parse, date_decisions = _apply_date_parsing_logged(cleaned)
    decisions.extend(date_decisions)

    # Step 5 — deduplication
    duplicate_mask = cleaned.duplicated(keep="first")
    deduplicated = cleaned.loc[~duplicate_mask].reset_index(drop=True)
    deduplicated_type_issue = type_conversion_issue.loc[~duplicate_mask].reset_index(
        drop=True
    )
    deduplicated_date_issue = invalid_date_parse.loc[~duplicate_mask].reset_index(
        drop=True
    )
    duplicate_row_count = int(duplicate_mask.sum())
    if duplicate_row_count > 0:
        decisions.append(
            _decision(
                step="deduplication",
                action="remove_duplicate_rows",
                columns=None,
                rows_affected=duplicate_row_count,
                details=f"{duplicate_row_count} duplicate row(s) removed (keep='first').",
            )
        )

    deduplicated["has_nulls"] = deduplicated.isna().any(axis=1)
    deduplicated["invalid_date_parse"] = deduplicated_date_issue.astype(bool)
    deduplicated["type_conversion_issue"] = deduplicated_type_issue.astype(bool)

    run_timestamp = datetime.now(timezone.utc)
    run_suffix = run_id
    output_path = (
        staging_dir
        / f"staging_{run_timestamp.strftime('%Y_%m_%d_%H%M%S')}_{run_suffix}.parquet"
    )
    metadata_path = metadata_dir / f"cleaning_summary_{normalized_source_name}.json"
    log_path = metadata_dir / f"cleaning_log_{normalized_source_name}.json"
    deduplicated.to_parquet(output_path, index=False, engine="pyarrow")

    summary = _build_cleaning_summary(
        dataframe=deduplicated,
        source_name=normalized_source_name,
        original_columns=original_columns,
        duplicate_row_count=duplicate_row_count,
        output_path=output_path,
        metadata_path=metadata_path,
        project_root=root_dir,
    )
    metadata_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    cleaning_log: dict[str, Any] = {
        "source": normalized_source_name,
        "run_id": run_id,
        "run_timestamp": run_timestamp.isoformat(),
        "total_decisions": len(decisions),
        "decisions": decisions,
    }
    log_path.write_text(json.dumps(cleaning_log, indent=2), encoding="utf-8")

    LOGGER.info(
        "Cleaning completed for %s rows x %s columns",
        summary["row_count"],
        summary["column_count"],
    )
    LOGGER.info("Staging output written to %s", output_path)
    LOGGER.info("Cleaning metadata written to %s", metadata_path)
    LOGGER.info("Cleaning log written to %s (%d decisions)", log_path, len(decisions))

    return {
        "dataframe": deduplicated,
        "summary": summary,
        "cleaning_log": cleaning_log,
    }


# ---------------------------------------------------------------------------
# Logged transformation helpers (used by clean_dataframe)
# ---------------------------------------------------------------------------


def _decision(
    step: str,
    action: str,
    columns: list[str] | None,
    rows_affected: int | None,
    details: str,
) -> dict[str, Any]:
    """Build a single cleaning decision entry for the log."""
    entry: dict[str, Any] = {"step": step, "action": action, "details": details}
    if columns is not None:
        entry["columns"] = columns
    if rows_affected is not None:
        entry["rows_affected"] = rows_affected
    return entry


def _standardize_missing_values_logged(
    dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Normalize blank strings to pd.NA, emit one decision entry per affected column."""
    cleaned = dataframe.copy()
    decisions: list[dict[str, Any]] = []
    for column_name in cleaned.columns:
        series = cleaned[column_name]
        if is_object_dtype(series) or is_string_dtype(series):
            before_nulls = int(series.isna().sum())
            cleaned[column_name] = series.replace(r"^\s*$", pd.NA, regex=True)
            new_nulls = int(cleaned[column_name].isna().sum()) - before_nulls
            if new_nulls > 0:
                decisions.append(
                    _decision(
                        step="missing_value_standardization",
                        action="replace_blank_with_null",
                        columns=[column_name],
                        rows_affected=new_nulls,
                        details=f"{new_nulls} blank string(s) in '{column_name}' replaced with pd.NA.",
                    )
                )
    return cleaned, decisions


def _apply_type_enforcement_logged(
    dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series, list[dict[str, Any]]]:
    """Apply conservative type enforcement and log per-column decisions."""
    cleaned = dataframe.copy()
    type_conversion_issue = pd.Series(False, index=cleaned.index)
    decisions: list[dict[str, Any]] = []

    for column_name in cleaned.columns:
        series = cleaned[column_name]
        if is_bool_dtype(series) or is_numeric_dtype(series):
            continue
        if not (is_object_dtype(series) or is_string_dtype(series)):
            continue

        prepared_series = series.astype("string")
        numeric_series = pd.to_numeric(prepared_series.str.strip(), errors="coerce")
        non_null_mask = prepared_series.notna()
        non_null_count = int(non_null_mask.sum())

        if non_null_count == 0:
            cleaned[column_name] = prepared_series
            continue

        numeric_success_mask = non_null_mask & numeric_series.notna()
        numeric_ratio = float(numeric_success_mask.sum() / non_null_count)

        if numeric_ratio >= NUMERIC_CONVERSION_THRESHOLD:
            cleaned[column_name] = numeric_series
            issue_mask = non_null_mask & numeric_series.isna()
            failed = int(issue_mask.sum())
            type_conversion_issue = type_conversion_issue | issue_mask
            decisions.append(
                _decision(
                    step="type_enforcement",
                    action="cast_to_numeric",
                    columns=[column_name],
                    rows_affected=failed if failed > 0 else None,
                    details=(
                        f"'{column_name}' cast to numeric (ratio: {numeric_ratio:.0%})."
                        + (
                            f" {failed} value(s) could not convert."
                            if failed > 0
                            else ""
                        )
                    ),
                )
            )
        else:
            cleaned[column_name] = prepared_series

    return cleaned, type_conversion_issue, decisions


def _apply_date_parsing_logged(
    dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.Series, list[dict[str, Any]]]:
    """Parse date-like columns and log per-column outcomes."""
    cleaned = dataframe.copy()
    invalid_date_parse = pd.Series(False, index=cleaned.index)
    decisions: list[dict[str, Any]] = []

    for column_name in cleaned.columns:
        if not _looks_like_date_column(column_name):
            continue

        series = cleaned[column_name]
        if is_bool_dtype(series) or is_numeric_dtype(series):
            continue
        if not (is_object_dtype(series) or is_string_dtype(series)):
            continue

        prepared_series = series.astype("string")
        parsed_series = pd.to_datetime(prepared_series, errors="coerce", utc=True)
        non_null_mask = prepared_series.notna()
        invalid_mask = non_null_mask & parsed_series.isna()

        if bool(invalid_mask.any()):
            invalid_count = int(invalid_mask.sum())
            invalid_date_parse = invalid_date_parse | invalid_mask
            cleaned[column_name] = prepared_series
            decisions.append(
                _decision(
                    step="date_parsing",
                    action="skip_date_parse",
                    columns=[column_name],
                    rows_affected=invalid_count,
                    details=f"'{column_name}' skipped: {invalid_count} value(s) unparseable as datetime.",
                )
            )
            continue

        cleaned[column_name] = parsed_series
        decisions.append(
            _decision(
                step="date_parsing",
                action="cast_to_datetime",
                columns=[column_name],
                rows_affected=None,
                details=f"'{column_name}' cast to datetime (UTC).",
            )
        )

    return cleaned, invalid_date_parse, decisions


# ---------------------------------------------------------------------------
# Legacy non-logged helpers (kept for backward compatibility with tests)
# ---------------------------------------------------------------------------


def _standardize_missing_values(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Normalize blank string values to pandas missing values."""
    cleaned, _ = _standardize_missing_values_logged(dataframe)
    return cleaned


def _apply_type_enforcement(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Apply conservative type enforcement (non-logged variant)."""
    cleaned, issues, _ = _apply_type_enforcement_logged(dataframe)
    return cleaned, issues


def _apply_date_parsing(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Parse date-like columns (non-logged variant)."""
    cleaned, issues, _ = _apply_date_parsing_logged(dataframe)
    return cleaned, issues


def _build_cleaning_summary(
    dataframe: pd.DataFrame,
    source_name: str,
    original_columns: list[str],
    duplicate_row_count: int,
    output_path: Path,
    metadata_path: Path,
    project_root: Path,
) -> dict[str, Any]:
    """Build a JSON-serializable cleaning summary."""
    cleaned_columns = list(dataframe.columns)
    base_columns = [
        column_name
        for column_name in cleaned_columns
        if column_name
        not in {"has_nulls", "invalid_date_parse", "type_conversion_issue"}
    ]

    return {
        "source": source_name,
        "row_count": int(len(dataframe)),
        "column_count": int(len(base_columns)),
        "input_column_count": int(len(original_columns)),
        "duplicate_row_count": duplicate_row_count,
        "rows_with_nulls": int(dataframe["has_nulls"].sum()),
        "rows_with_invalid_date_parse": int(dataframe["invalid_date_parse"].sum()),
        "rows_with_type_conversion_issue": int(
            dataframe["type_conversion_issue"].sum()
        ),
        "output_path": output_path.relative_to(project_root).as_posix(),
        "metadata_path": metadata_path.relative_to(project_root).as_posix(),
        "original_columns": original_columns,
        "cleaned_columns": cleaned_columns,
        "null_counts_by_column": {
            column_name: int(dataframe[column_name].isna().sum())
            for column_name in base_columns
        },
    }


def _looks_like_date_column(column_name: str) -> bool:
    """Use the normalized column name as a simple signal for date parsing."""
    lowered = column_name.lower()
    return any(token in lowered for token in DATE_COLUMN_HINTS)
