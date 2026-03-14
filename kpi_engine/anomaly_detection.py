"""Lightweight anomaly detection helpers for analytics datasets."""

from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import Any

from pipelines.utils import normalize_name, resolve_project_root

import pandas as pd
from pandas.api.types import is_bool_dtype, is_numeric_dtype


LOGGER = logging.getLogger(__name__)

MIN_VALUES_FOR_ANALYSIS = 4
IQR_MULTIPLIER = 1.5
ZSCORE_THRESHOLD = 3.0


def detect_anomalies(
    dataframe: pd.DataFrame,
    source_name: str,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Detect simple numeric anomalies and persist a JSON report."""
    root_dir = resolve_project_root(project_root, __file__)
    normalized_source_name = normalize_name(source_name)
    metadata_dir = root_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Anomaly detection started for source %s", normalized_source_name)

    flagged = dataframe.copy()
    flagged["has_anomaly"] = False
    flagged["anomaly_columns"] = ""

    numeric_columns = [
        column_name
        for column_name in dataframe.columns
        if is_numeric_dtype(dataframe[column_name])
        and not is_bool_dtype(dataframe[column_name])
    ]

    anomalies: list[dict[str, Any]] = []

    for column_name in numeric_columns:
        column_result = _detect_numeric_column_anomalies(dataframe[column_name])
        if column_result is None:
            continue

        anomalies.append(
            {
                "column": column_name,
                "method": column_result["method"],
                "threshold": column_result["threshold"],
                "anomaly_count": int(column_result["mask"].sum()),
                "example_values": column_result["example_values"],
            }
        )
        _apply_column_anomaly_flags(
            flagged,
            column_name=column_name,
            mask=column_result["mask"],
        )

    flagged_rows = flagged.loc[flagged["has_anomaly"]].reset_index(drop=True)
    report = {
        "source": normalized_source_name,
        "anomaly_row_count": int(len(flagged_rows)),
        "anomalies": anomalies,
    }

    report_path = metadata_dir / f"anomaly_report_{normalized_source_name}.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    LOGGER.info(
        "Anomaly detection completed with %s flagged rows",
        report["anomaly_row_count"],
    )

    return {
        "report": report,
        "flagged_rows": flagged_rows,
        "report_path": report_path.relative_to(root_dir).as_posix(),
    }


def _detect_numeric_column_anomalies(series: pd.Series) -> dict[str, Any] | None:
    """Detect anomalies in a numeric series using simple statistical rules."""
    non_null_series = series.dropna()
    if len(non_null_series) < MIN_VALUES_FOR_ANALYSIS:
        return None

    iqr_mask, lower_bound, upper_bound = _iqr_outlier_mask(series)
    if bool(iqr_mask.any()):
        return {
            "mask": iqr_mask,
            "method": "iqr",
            "threshold": f"{lower_bound:.4f} to {upper_bound:.4f}",
            "example_values": _example_values(series, iqr_mask),
        }

    zscore_mask, zscore_threshold = _zscore_outlier_mask(series)
    if bool(zscore_mask.any()):
        return {
            "mask": zscore_mask,
            "method": "zscore",
            "threshold": f"|z| > {zscore_threshold:.1f}",
            "example_values": _example_values(series, zscore_mask),
        }

    return None


def _iqr_outlier_mask(
    series: pd.Series,
) -> tuple[pd.Series, float | None, float | None]:
    """Return an IQR-based outlier mask and bounds."""
    non_null_series = series.dropna()
    if len(non_null_series) < MIN_VALUES_FOR_ANALYSIS:
        return pd.Series(False, index=series.index), None, None

    q1 = float(non_null_series.quantile(0.25))
    q3 = float(non_null_series.quantile(0.75))
    iqr = q3 - q1

    if math.isclose(iqr, 0.0):
        return pd.Series(False, index=series.index), q1, q3

    lower_bound = q1 - (IQR_MULTIPLIER * iqr)
    upper_bound = q3 + (IQR_MULTIPLIER * iqr)
    mask = series.lt(lower_bound) | series.gt(upper_bound)
    return mask.fillna(False), lower_bound, upper_bound


def _zscore_outlier_mask(series: pd.Series) -> tuple[pd.Series, float]:
    """Return a z-score-based outlier mask and threshold."""
    non_null_series = series.dropna()
    if len(non_null_series) < MIN_VALUES_FOR_ANALYSIS:
        return pd.Series(False, index=series.index), ZSCORE_THRESHOLD

    mean = float(non_null_series.mean())
    std = float(non_null_series.std(ddof=0))
    if math.isclose(std, 0.0):
        return pd.Series(False, index=series.index), ZSCORE_THRESHOLD

    zscores = (series - mean) / std
    mask = zscores.abs().gt(ZSCORE_THRESHOLD)
    return mask.fillna(False), ZSCORE_THRESHOLD


def _example_values(series: pd.Series, mask: pd.Series, limit: int = 3) -> list[Any]:
    """Return a few flagged values for reporting."""
    values = series.loc[mask.astype(bool)].head(limit).tolist()
    return [_to_json_compatible(value) for value in values]


def _to_json_compatible(value: Any) -> Any:
    """Convert NumPy or pandas scalar values into JSON-friendly types."""
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    return value


def _apply_column_anomaly_flags(
    dataframe: pd.DataFrame,
    column_name: str,
    mask: pd.Series,
) -> None:
    """Mark flagged rows and append the column name to the anomaly summary."""
    boolean_mask = mask.astype(bool)
    dataframe.loc[boolean_mask, "has_anomaly"] = True
    dataframe.loc[boolean_mask, "anomaly_columns"] = dataframe.loc[
        boolean_mask, "anomaly_columns"
    ].apply(lambda value: _append_column_name(value, column_name))


def _append_column_name(existing_value: str, column_name: str) -> str:
    """Append a column name to a comma-separated anomaly column list."""
    if not existing_value:
        return column_name

    values = existing_value.split(",")
    if column_name in values:
        return existing_value

    return f"{existing_value},{column_name}"
