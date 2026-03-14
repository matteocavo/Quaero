"""Utilities for optional multi-dataset schema normalization and integrated datasets."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype

from kpi_engine.semantic_contract import infer_metric_unit
from pipelines.utils import normalize_name, resolve_project_root


REQUIRED_STANDARD_COLUMNS = ("year", "source_metric_value", "dataset_name")


def standardize_dataset_for_integration(
    dataframe: pd.DataFrame,
    dataset_name: str,
    time_column: str,
    metric_column: str | None = None,
) -> dict[str, Any]:
    """Add a shared integration schema while preserving original cleaned columns."""
    normalized_time_column = normalize_name(time_column)
    if normalized_time_column not in dataframe.columns:
        raise ValueError(
            f"Time column not found for dataset '{dataset_name}': {normalized_time_column}"
        )

    resolved_metric_column = metric_column and normalize_name(metric_column)
    if (
        resolved_metric_column is not None
        and resolved_metric_column not in dataframe.columns
    ):
        raise ValueError(
            f"Metric column not found for dataset '{dataset_name}': {resolved_metric_column}"
        )
    if resolved_metric_column is None:
        resolved_metric_column = _infer_dataset_metric_column(
            dataframe=dataframe,
            time_column=normalized_time_column,
        )

    working = dataframe.copy()
    working["year"] = _normalize_time_series(working[normalized_time_column])
    working["source_metric_value"] = pd.to_numeric(
        working[resolved_metric_column], errors="coerce"
    )
    working["dataset_name"] = normalize_name(dataset_name)

    if working["source_metric_value"].dropna().empty:
        raise ValueError(
            f"Dataset '{dataset_name}' does not contain usable numeric values for integration."
        )

    return {
        "dataframe": working,
        "dataset_name": normalize_name(dataset_name),
        "time_column": normalized_time_column,
        "metric_column": resolved_metric_column,
        "unit": infer_metric_unit(resolved_metric_column),
    }


def build_integrated_master_dataset(
    standardized_datasets: list[dict[str, Any]],
    project_root: str | Path | None = None,
    output_name: str = "master_dataset",
) -> dict[str, Any]:
    """Build a wide integrated dataset aligned on year.

    The current MVP defaults to ``master_dataset.parquet`` but callers may
    provide a project-specific output name when that is more descriptive.
    """
    if not standardized_datasets:
        raise ValueError(
            "At least one standardized dataset is required for integration."
        )

    long_frames: list[pd.DataFrame] = []
    dataset_metrics: list[dict[str, Any]] = []
    for dataset in standardized_datasets:
        frame = dataset["dataframe"]
        long_frames.append(frame)
        dataset_metrics.append(
            {
                "dataset_name": dataset["dataset_name"],
                "metric_column": dataset["metric_column"],
                "unit": dataset.get("unit"),
                "time_column": dataset["time_column"],
            }
        )

    long_dataframe = pd.concat(long_frames, ignore_index=True, sort=False)
    aggregated = (
        long_dataframe.groupby(["year", "dataset_name"], dropna=False)[
            "source_metric_value"
        ]
        .mean()
        .reset_index()
    )
    integrated = (
        aggregated.pivot(
            index="year", columns="dataset_name", values="source_metric_value"
        )
        .reset_index()
        .rename_axis(columns=None)
        .sort_values("year")
        .reset_index(drop=True)
    )

    root_dir = resolve_project_root(project_root, __file__)
    integrated_dir = root_dir / "integrated"
    integrated_dir.mkdir(parents=True, exist_ok=True)
    output_path = integrated_dir / f"{output_name}.parquet"
    integrated.to_parquet(output_path, index=False, engine="pyarrow")

    return {
        "dataframe": integrated,
        "long_dataframe": long_dataframe,
        "output_path": output_path.relative_to(root_dir).as_posix(),
        "dataset_metrics": dataset_metrics,
    }


def _infer_dataset_metric_column(dataframe: pd.DataFrame, time_column: str) -> str:
    """Pick the strongest numeric metric column for integration."""
    numeric_candidates = [
        column
        for column in dataframe.columns
        if column != time_column and is_numeric_dtype(dataframe[column])
    ]
    if not numeric_candidates:
        raise ValueError(
            "Could not infer a numeric metric column for multi-dataset integration."
        )
    preferred = [
        column
        for column in numeric_candidates
        if "year" not in column
        and "id" not in column
        and dataframe[column].notna().any()
    ]
    return preferred[0] if preferred else numeric_candidates[0]


def _normalize_time_series(series: pd.Series) -> pd.Series:
    """Normalize time values to a stable string key used for integration."""
    if is_datetime64_any_dtype(series):
        return series.dt.strftime("%Y-%m-%d")

    if is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors="coerce")
        non_null = numeric.dropna()
        if not non_null.empty and non_null.between(1800, 2200).all():
            return numeric.round().astype("Int64").astype(str)
        return series.astype(str)

    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    if float(parsed.notna().mean()) >= 0.8:
        return parsed.dt.strftime("%Y-%m-%d").fillna(series.astype(str))

    return series.astype(str)
