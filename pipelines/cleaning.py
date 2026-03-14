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
    """Clean a DataFrame, persist staging output, and write cleaning metadata."""
    root_dir = resolve_project_root(project_root, __file__)
    normalized_source_name = normalize_name(source_name)
    staging_dir = root_dir / "staging"
    metadata_dir = root_dir / "metadata"
    staging_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Cleaning started for source %s", normalized_source_name)

    cleaned = dataframe.copy()
    original_columns = list(cleaned.columns)

    cleaned.columns = [normalize_name(column) for column in cleaned.columns]
    cleaned = _standardize_missing_values(cleaned)

    cleaned, type_conversion_issue = _apply_type_enforcement(cleaned)
    cleaned, invalid_date_parse = _apply_date_parsing(cleaned)

    duplicate_mask = cleaned.duplicated(keep="first")
    deduplicated = cleaned.loc[~duplicate_mask].reset_index(drop=True)
    deduplicated_type_issue = type_conversion_issue.loc[~duplicate_mask].reset_index(
        drop=True
    )
    deduplicated_date_issue = invalid_date_parse.loc[~duplicate_mask].reset_index(
        drop=True
    )

    duplicate_row_count = int(duplicate_mask.sum())

    deduplicated["has_nulls"] = deduplicated.isna().any(axis=1)
    deduplicated["invalid_date_parse"] = deduplicated_date_issue.astype(bool)
    deduplicated["type_conversion_issue"] = deduplicated_type_issue.astype(bool)

    run_timestamp = datetime.now(timezone.utc)
    run_suffix = str(uuid.uuid4())[:8]
    output_path = (
        staging_dir
        / f"staging_{run_timestamp.strftime('%Y_%m_%d_%H%M%S')}_{run_suffix}.parquet"
    )
    metadata_path = metadata_dir / f"cleaning_summary_{normalized_source_name}.json"
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

    LOGGER.info(
        "Cleaning completed for %s rows x %s columns",
        summary["row_count"],
        summary["column_count"],
    )
    LOGGER.info("Staging output written to %s", output_path)
    LOGGER.info("Cleaning metadata written to %s", metadata_path)

    return {
        "dataframe": deduplicated,
        "summary": summary,
    }


def _standardize_missing_values(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Normalize blank string values to pandas missing values."""
    cleaned = dataframe.copy()

    for column_name in cleaned.columns:
        series = cleaned[column_name]
        if is_object_dtype(series) or is_string_dtype(series):
            cleaned[column_name] = series.replace(r"^\s*$", pd.NA, regex=True)

    return cleaned


def _apply_type_enforcement(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Apply conservative type enforcement and track row-level conversion issues."""
    cleaned = dataframe.copy()
    type_conversion_issue = pd.Series(False, index=cleaned.index)

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
            type_conversion_issue = type_conversion_issue | issue_mask
        else:
            cleaned[column_name] = prepared_series

    return cleaned, type_conversion_issue


def _apply_date_parsing(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Parse date-like columns when all non-null values convert successfully."""
    cleaned = dataframe.copy()
    invalid_date_parse = pd.Series(False, index=cleaned.index)

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
            invalid_date_parse = invalid_date_parse | invalid_mask
            cleaned[column_name] = prepared_series
            continue

        cleaned[column_name] = parsed_series

    return cleaned, invalid_date_parse


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
