"""Mart builder utilities for analytics-ready outputs."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_bool_dtype, is_datetime64_any_dtype, is_numeric_dtype

from kpi_engine.semantic_contract import build_semantic_metric_name
from pipelines.utils import resolve_project_root


LOGGER = logging.getLogger(__name__)

TIME_COLUMN_HINTS = ("date", "time", "timestamp", "year", "month", "week")
QUALITY_FLAG_COLUMNS = {
    "has_nulls",
    "invalid_date_parse",
    "type_conversion_issue",
}


def build_release_impact_mart(
    dataframe: pd.DataFrame,
    metric_column: str,
    project_root: str | Path | None = None,
    release_year_column: str = "release_year",
) -> dict[str, Any]:
    """Aggregate dataset performance by release year."""
    _validate_columns(dataframe, [release_year_column, metric_column])
    total_metric_name = build_semantic_metric_name(metric_column, "sum")
    average_metric_name = build_semantic_metric_name(metric_column, "average")

    mart = (
        dataframe.groupby(release_year_column, dropna=False)[metric_column]
        .agg(
            record_count="count",
            **{total_metric_name: "sum", average_metric_name: "mean"},
        )
        .reset_index()
        .sort_values(release_year_column)
    )

    output_path = _write_mart(
        mart,
        mart_name="mart_release_impact",
        project_root=project_root,
    )
    return {"dataframe": mart, "output_path": output_path}


def build_provider_performance_mart(
    dataframe: pd.DataFrame,
    metric_column: str,
    project_root: str | Path | None = None,
    provider_column: str = "provider",
) -> dict[str, Any]:
    """Aggregate dataset performance by provider."""
    _validate_columns(dataframe, [provider_column, metric_column])
    total_metric_name = build_semantic_metric_name(metric_column, "sum")
    average_metric_name = build_semantic_metric_name(metric_column, "average")

    mart = (
        dataframe.groupby(provider_column, dropna=False)[metric_column]
        .agg(
            record_count="count",
            **{total_metric_name: "sum", average_metric_name: "mean"},
        )
        .reset_index()
        .sort_values(total_metric_name, ascending=False)
    )

    output_path = _write_mart(
        mart,
        mart_name="mart_provider_performance",
        project_root=project_root,
    )
    return {"dataframe": mart, "output_path": output_path}


def build_catalog_summary_mart(
    dataframe: pd.DataFrame,
    catalog_column: str,
    project_root: str | Path | None = None,
    metric_column: str | None = None,
) -> dict[str, Any]:
    """Aggregate a simple catalog summary mart."""
    required_columns = [catalog_column]
    if metric_column is not None:
        required_columns.append(metric_column)

    _validate_columns(dataframe, required_columns)

    grouped = dataframe.groupby(catalog_column, dropna=False)
    mart = grouped.size().reset_index(name="record_count")

    if metric_column is not None:
        total_metric_name = build_semantic_metric_name(metric_column, "sum")
        average_metric_name = build_semantic_metric_name(metric_column, "average")
        metric_summary = (
            grouped[metric_column]
            .agg(**{total_metric_name: "sum", average_metric_name: "mean"})
            .reset_index()
        )
        mart = mart.merge(metric_summary, on=catalog_column, how="left")

    mart = mart.sort_values("record_count", ascending=False)

    output_path = _write_mart(
        mart,
        mart_name="mart_catalog_summary",
        project_root=project_root,
    )
    return {"dataframe": mart, "output_path": output_path}


def build_question_answer_mart(
    dataframe: pd.DataFrame,
    dimension_column: str,
    metric_column: str,
    aggregation: str,
    ordering: str = "highest",
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Build the primary mart answering the user question."""
    mart = _aggregate_by_dimension(
        dataframe=dataframe,
        dimension_column=dimension_column,
        metric_column=metric_column,
        aggregation=aggregation,
        ordering=ordering,
    )
    output_path = _write_mart(
        mart,
        mart_name="mart_question_answer",
        project_root=project_root,
    )
    return {"dataframe": mart, "output_path": output_path}


def build_top_entities_mart(
    dataframe: pd.DataFrame,
    dimension_column: str,
    metric_column: str,
    aggregation: str,
    ordering: str = "highest",
    limit: int = 20,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Build a top-entities table ranked by the inferred metric."""
    mart = _aggregate_by_dimension(
        dataframe=dataframe,
        dimension_column=dimension_column,
        metric_column=metric_column,
        aggregation=aggregation,
        ordering=ordering,
    ).head(limit)
    mart = mart.reset_index(drop=True)
    mart.insert(0, "rank", range(1, len(mart) + 1))

    output_path = _write_mart(
        mart,
        mart_name="mart_top_entities",
        project_root=project_root,
    )
    return {"dataframe": mart, "output_path": output_path}


def build_distribution_mart(
    dataframe: pd.DataFrame,
    metric_column: str,
    project_root: str | Path | None = None,
    bins: int = 10,
) -> dict[str, Any]:
    """Build a histogram-like distribution table for a numeric metric."""
    _validate_columns(dataframe, [metric_column])
    series = dataframe[metric_column].dropna()
    if series.empty:
        raise ValueError("Cannot build a distribution mart from an empty metric.")

    unique_count = int(series.nunique())
    if unique_count <= 1:
        value = float(series.iloc[0])
        mart = pd.DataFrame(
            {
                "bin_start": [value],
                "bin_end": [value],
                "record_count": [int(len(series))],
            }
        )
    else:
        effective_bins = min(bins, unique_count)
        intervals = pd.cut(series, bins=effective_bins, duplicates="drop")
        grouped = intervals.value_counts(sort=False)
        mart = grouped.reset_index()
        mart.columns = ["bin", "record_count"]
        mart["bin_start"] = mart["bin"].map(_interval_start)
        mart["bin_end"] = mart["bin"].map(_interval_end)
        mart = mart[["bin_start", "bin_end", "record_count"]]

    output_path = _write_mart(
        mart,
        mart_name="mart_distribution",
        project_root=project_root,
    )
    return {"dataframe": mart, "output_path": output_path}


def build_summary_stats_mart(
    dataframe: pd.DataFrame,
    metric_column: str,
    project_root: str | Path | None = None,
    mart_name: str = "mart_summary_stats",
) -> dict[str, Any]:
    """Build a one-row summary statistics mart for the metric column."""
    _validate_columns(dataframe, [metric_column])
    series = dataframe[metric_column].dropna()
    if series.empty:
        raise ValueError("Cannot build summary statistics from an empty metric.")

    min_name = build_semantic_metric_name(metric_column, "min")
    max_name = build_semantic_metric_name(metric_column, "max")
    mean_name = build_semantic_metric_name(metric_column, "average")
    median_name = build_semantic_metric_name(metric_column, "median")
    std_name = build_semantic_metric_name(metric_column, "std")
    p05_name = build_semantic_metric_name(metric_column, "p05")
    p25_name = build_semantic_metric_name(metric_column, "p25")
    p75_name = build_semantic_metric_name(metric_column, "p75")
    p95_name = build_semantic_metric_name(metric_column, "p95")

    mart = pd.DataFrame(
        [
            {
                "source_metric_column": metric_column,
                min_name: float(series.min()),
                max_name: float(series.max()),
                mean_name: float(series.mean()),
                median_name: float(series.median()),
                std_name: float(series.std(ddof=1)) if len(series) > 1 else 0.0,
                p05_name: float(series.quantile(0.05)),
                p25_name: float(series.quantile(0.25)),
                p75_name: float(series.quantile(0.75)),
                p95_name: float(series.quantile(0.95)),
            }
        ]
    )

    output_path = _write_mart(
        mart,
        mart_name=mart_name,
        project_root=project_root,
    )
    return {"dataframe": mart, "output_path": output_path}


def build_time_trend_mart(
    dataframe: pd.DataFrame,
    metric_column: str,
    aggregation: str,
    ordering: str = "highest",
    time_column: str | None = None,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Build a time-based trend mart when a time-like column exists."""
    selected_time_column = time_column or _find_time_column(dataframe)
    if selected_time_column is None:
        raise ValueError("No time-like column is available for a trend mart.")

    working = dataframe.copy()
    time_series = working[selected_time_column]
    if is_datetime64_any_dtype(time_series):
        working["_time_period"] = time_series.dt.strftime("%Y-%m-%d")
    else:
        working["_time_period"] = time_series

    mart = _aggregate_by_dimension(
        dataframe=working,
        dimension_column="_time_period",
        metric_column=metric_column,
        aggregation=aggregation,
        ordering=ordering,
    ).rename(columns={"_time_period": selected_time_column})

    output_path = _write_mart(
        mart,
        mart_name="mart_time_trend",
        project_root=project_root,
    )
    return {"dataframe": mart, "output_path": output_path}


def find_time_column(dataframe: pd.DataFrame) -> str | None:
    """Public helper to find a time-like column in a dataset."""
    return _find_time_column(dataframe)


def find_integrated_metric_columns(dataframe: pd.DataFrame) -> list[str]:
    """Return usable numeric metric columns from an integrated dataset."""
    metric_columns: list[str] = []
    for column_name in dataframe.columns:
        if column_name == "year":
            continue
        series = dataframe[column_name]
        if is_bool_dtype(series) or not is_numeric_dtype(series):
            continue
        if series.dropna().empty:
            continue
        metric_columns.append(column_name)
    return metric_columns


def build_integrated_trend_mart(
    dataframe: pd.DataFrame,
    dataset_column: str,
    mart_name: str,
    aggregation: str = "average",
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Build a trend mart for one dataset column from an integrated table."""
    _validate_columns(dataframe, ["year", dataset_column])
    grouped = dataframe.groupby("year", dropna=False)[dataset_column]
    metric_column_name = build_semantic_metric_name(dataset_column, aggregation)
    if aggregation == "average":
        metric_values = grouped.mean()
    elif aggregation == "sum":
        metric_values = grouped.sum()
    elif aggregation == "count":
        metric_values = grouped.count()
    else:
        raise ValueError(f"Unsupported integrated trend aggregation: {aggregation}")

    mart = (
        grouped.count()
        .reset_index(name="record_count")
        .merge(
            metric_values.reset_index(name=metric_column_name),
            on="year",
            how="left",
        )
        .sort_values("year")
        .reset_index(drop=True)
    )
    mart = mart[mart["record_count"] > 0].reset_index(drop=True)

    output_path = _write_mart(
        mart,
        mart_name=mart_name,
        project_root=project_root,
    )
    return {"dataframe": mart, "output_path": output_path}


def build_integrated_correlation_mart(
    dataframe: pd.DataFrame,
    dataset_columns: list[str],
    mart_name: str = "mart_metric_correlation",
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Build a long-form correlation mart across integrated metric columns."""
    available_columns = [
        column for column in dataset_columns if column in dataframe.columns
    ]
    if len(available_columns) < 2:
        raise ValueError(
            "At least two integrated dataset columns are required for correlation."
        )

    correlation = dataframe[available_columns].corr(numeric_only=True)
    mart = (
        correlation.stack()
        .reset_index()
        .rename(
            columns={
                "level_0": "left_metric",
                "level_1": "right_metric",
                0: "correlation",
            }
        )
    )
    mart = mart[mart["left_metric"] < mart["right_metric"]].reset_index(drop=True)

    output_path = _write_mart(
        mart,
        mart_name=mart_name,
        project_root=project_root,
    )
    return {"dataframe": mart, "output_path": output_path}


def build_integrated_top_ranked_mart(
    dataframe: pd.DataFrame,
    dataset_column: str,
    mart_name: str,
    aggregation: str = "average",
    time_column: str = "year",
    ordering: str = "highest",
    limit: int = 10,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Build a ranked mart over time periods for one integrated metric."""
    _validate_columns(dataframe, [time_column, dataset_column])
    grouped = dataframe.groupby(time_column, dropna=False)[dataset_column]
    metric_column_name = build_semantic_metric_name(dataset_column, aggregation)

    if aggregation == "average":
        metric_values = grouped.mean()
    elif aggregation == "sum":
        metric_values = grouped.sum()
    elif aggregation == "count":
        metric_values = grouped.count()
    else:
        raise ValueError(f"Unsupported integrated ranking aggregation: {aggregation}")

    mart = (
        grouped.count()
        .reset_index(name="record_count")
        .merge(
            metric_values.reset_index(name=metric_column_name),
            on=time_column,
            how="left",
        )
    )

    ascending = ordering == "lowest"
    mart = (
        mart.sort_values([metric_column_name, time_column], ascending=[ascending, True])
        .head(limit)
        .reset_index(drop=True)
    )

    output_path = _write_mart(
        mart,
        mart_name=mart_name,
        project_root=project_root,
    )
    return {"dataframe": mart, "output_path": output_path}


def build_integrated_decade_mart(
    dataframe: pd.DataFrame,
    dataset_column: str,
    mart_name: str,
    aggregation: str = "average",
    year_column: str = "year",
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Aggregate an integrated metric by decade."""
    _validate_columns(dataframe, [year_column, dataset_column])
    working = dataframe.copy()
    numeric_year = pd.to_numeric(working[year_column], errors="coerce")
    working = working[numeric_year.notna()].copy()
    working["decade"] = (
        (numeric_year[numeric_year.notna()].astype(int) // 10) * 10
    ).astype(int)

    grouped = working.groupby("decade", dropna=False)[dataset_column]
    metric_column_name = build_semantic_metric_name(dataset_column, aggregation)
    if aggregation == "average":
        metric_values = grouped.mean()
    elif aggregation == "sum":
        metric_values = grouped.sum()
    elif aggregation == "count":
        metric_values = grouped.count()
    else:
        raise ValueError(f"Unsupported integrated decade aggregation: {aggregation}")

    mart = (
        grouped.count()
        .reset_index(name="record_count")
        .merge(
            metric_values.reset_index(name=metric_column_name),
            on="decade",
            how="left",
        )
        .sort_values("decade")
        .reset_index(drop=True)
    )

    output_path = _write_mart(
        mart,
        mart_name=mart_name,
        project_root=project_root,
    )
    return {"dataframe": mart, "output_path": output_path}


def build_climate_correlation_mart(
    dataframe: pd.DataFrame,
    dataset_columns: list[str],
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Build a climate-specific correlation mart on top of integrated metrics."""
    return build_integrated_correlation_mart(
        dataframe=dataframe,
        dataset_columns=dataset_columns,
        mart_name="mart_climate_correlation",
        project_root=project_root,
    )


def _write_mart(
    dataframe: pd.DataFrame,
    mart_name: str,
    project_root: str | Path | None = None,
) -> str:
    """Write a mart parquet file and return its project-relative path."""
    root_dir = resolve_project_root(project_root, __file__)
    mart_dir = root_dir / "marts"
    mart_dir.mkdir(parents=True, exist_ok=True)

    output_path = mart_dir / f"{mart_name}.parquet"
    dataframe.to_parquet(output_path, index=False, engine="pyarrow")

    LOGGER.info("Mart %s written to %s", mart_name, output_path)
    return output_path.relative_to(root_dir).as_posix()


def _interval_start(interval: pd.Interval) -> float:
    """Return the left edge of a histogram interval as a float."""
    return float(interval.left)


def _interval_end(interval: pd.Interval) -> float:
    """Return the right edge of a histogram interval as a float."""
    return float(interval.right)


def _aggregate_by_dimension(
    dataframe: pd.DataFrame,
    dimension_column: str,
    metric_column: str,
    aggregation: str,
    ordering: str,
) -> pd.DataFrame:
    """Aggregate the dataset by a dimension using the inferred analytical intent."""
    _validate_columns(dataframe, [dimension_column])
    grouped = dataframe.groupby(dimension_column, dropna=False)
    metric_column_name = build_semantic_metric_name(metric_column, aggregation)

    if metric_column == "record_count":
        mart = grouped.size().reset_index(name="record_count")
    else:
        _validate_columns(dataframe, [metric_column])
        mart = grouped.size().reset_index(name="record_count")
        metric_series = grouped[metric_column]

        if aggregation == "average":
            metric_values = metric_series.mean().reset_index(name=metric_column_name)
        elif aggregation == "sum":
            metric_values = metric_series.sum().reset_index(name=metric_column_name)
        elif aggregation == "count":
            metric_values = grouped.size().reset_index(name=metric_column_name)
        else:
            raise ValueError(f"Unsupported aggregation: {aggregation}")

        mart = mart.merge(metric_values, on=dimension_column, how="left")

    ascending = ordering == "lowest"
    sort_column = (
        "record_count" if metric_column == "record_count" else metric_column_name
    )
    return mart.sort_values(
        by=[sort_column, dimension_column],
        ascending=[ascending, True],
    ).reset_index(drop=True)


def _validate_columns(dataframe: pd.DataFrame, required_columns: list[str]) -> None:
    """Fail fast when a required column is missing from the source DataFrame."""
    missing_columns = [
        column for column in required_columns if column not in dataframe.columns
    ]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def _find_time_column(dataframe: pd.DataFrame) -> str | None:
    """Pick the strongest time-like column in the dataset."""
    for column_name in dataframe.columns:
        series = dataframe[column_name]
        lowered = column_name.lower()
        if lowered in QUALITY_FLAG_COLUMNS or is_bool_dtype(series):
            continue
        if is_datetime64_any_dtype(series):
            return column_name
        if any(token in lowered for token in TIME_COLUMN_HINTS):
            return column_name
        if (
            is_numeric_dtype(series)
            and "year" in lowered
            and int(series.nunique(dropna=True)) <= max(100, len(series))
        ):
            return column_name

    return None
