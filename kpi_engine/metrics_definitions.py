"""Project-scoped semantic metrics definitions for mart outputs."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from kpi_engine.semantic_contract import infer_metric_unit, parse_semantic_metric_column
from pipelines.utils import normalize_name, resolve_project_root


LOGGER = logging.getLogger(__name__)

NON_METRIC_COLUMNS = {
    "year",
    "month",
    "date",
    "decade",
    "rank",
    "left_metric",
    "right_metric",
    "source_metric_column",
    "bin_start",
    "bin_end",
}

SOURCE_METRIC_DESCRIPTIONS = {
    "temperature_anomaly": (
        "global temperature anomaly in degrees Celsius relative to the Berkeley Earth baseline"
    ),
    "co2": "annual world CO2 emissions in million tonnes of CO2",
    "sea_level": "global mean sea level variation in millimeters",
    "ice_extent": "Arctic sea ice extent in million square kilometers",
}

AGGREGATION_LABELS = {
    "average": "Average",
    "sum": "Total",
    "count": "Count",
    "median": "Median",
    "min": "Minimum",
    "max": "Maximum",
    "std": "Standard deviation",
    "p05": "5th percentile",
    "p25": "25th percentile",
    "p75": "75th percentile",
    "p95": "95th percentile",
}


def write_metrics_definitions(
    mart_names: list[str],
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Write project-scoped metrics definitions for the available marts."""
    root_dir = resolve_project_root(project_root, __file__)
    metadata_dir = root_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    metrics = _build_metrics_definitions(
        mart_names=mart_names,
        marts_dir=root_dir / "marts",
    )

    payload = {"metrics": metrics}
    output_path = metadata_dir / "metrics_definitions.json"
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return {
        **payload,
        "output_path": output_path.relative_to(root_dir).as_posix(),
    }


def _build_metrics_definitions(
    mart_names: list[str],
    marts_dir: Path,
) -> list[dict[str, Any]]:
    """Derive metric definitions from the schemas of generated mart files."""
    metrics: list[dict[str, Any]] = []

    for mart_name in sorted(set(mart_names)):
        mart_path = marts_dir / f"{mart_name}.parquet"
        if not mart_path.exists():
            LOGGER.warning("Skipping metric definition for missing mart: %s", mart_path)
            continue

        mart_dataframe = pd.read_parquet(mart_path)
        metrics.extend(
            _build_metric_definitions_from_mart(
                mart_name=mart_name,
                mart_dataframe=mart_dataframe,
            )
        )

    return metrics


def _build_metric_definitions_from_mart(
    mart_name: str,
    mart_dataframe: pd.DataFrame,
) -> list[dict[str, Any]]:
    """Extract metric definitions from one mart dataframe."""
    definitions: list[dict[str, Any]] = []

    for column_name in mart_dataframe.columns:
        if column_name in NON_METRIC_COLUMNS:
            continue

        definition = _build_metric_definition(
            mart_name=mart_name,
            column_name=column_name,
            mart_dataframe=mart_dataframe,
        )
        if definition is not None:
            definitions.append(definition)

    return definitions


def _build_metric_definition(
    mart_name: str,
    column_name: str,
    mart_dataframe: pd.DataFrame,
) -> dict[str, Any] | None:
    """Build one metric definition when the column represents a metric."""
    parsed_metric = parse_semantic_metric_column(column_name)
    if parsed_metric is not None:
        source_metric_column = parsed_metric["source_column"]
        aggregation = parsed_metric["aggregation"]
        return {
            "metric_name": _build_metric_name(mart_name, column_name),
            "source_mart": mart_name,
            "source_column": column_name,
            "source_metric_column": source_metric_column,
            "formula_description": _build_formula_description(
                mart_name=mart_name,
                column_name=column_name,
                source_metric_column=source_metric_column,
                aggregation=aggregation,
            ),
            "default_aggregation": aggregation,
            "unit": parsed_metric["unit"],
        }

    if column_name == "correlation" and {
        "left_metric",
        "right_metric",
    }.issubset(mart_dataframe.columns):
        return {
            "metric_name": _build_metric_name(mart_name, column_name),
            "source_mart": mart_name,
            "source_column": column_name,
            "source_metric_column": None,
            "formula_description": _build_correlation_description(mart_name),
            # Correlation is already a fully computed pairwise statistic and
            # should not be re-aggregated downstream.
            "default_aggregation": "none",
            "unit": None,
        }

    return None


def _build_metric_name(mart_name: str, column_name: str) -> str:
    """Create a unique metric identifier scoped to the mart output."""
    mart_suffix = mart_name.removeprefix("mart_")
    return normalize_name(f"{mart_suffix}_{column_name}")


def _build_formula_description(
    mart_name: str,
    column_name: str,
    source_metric_column: str | None,
    aggregation: str,
) -> str:
    """Describe how one mart metric is derived."""
    aggregation_label = AGGREGATION_LABELS.get(aggregation, aggregation.title())

    if column_name == "record_count":
        if mart_name.endswith("_trend"):
            return "Count of aligned records contributing to each yearly aggregate."
        if mart_name.endswith("_by_decade"):
            return (
                "Count of aligned yearly records contributing to each decade aggregate."
            )
        if mart_name == "mart_top_warmest_years":
            return "Count of aligned records contributing to each ranked warmest year."
        return f"Count of records represented in `{mart_name}`."

    metric_description = _describe_source_metric(source_metric_column)

    if mart_name.endswith("_trend"):
        return f"{aggregation_label} of {metric_description} aggregated by year."
    if mart_name.endswith("_summary_stats"):
        return f"{aggregation_label} statistic for {metric_description} across the full mart input."
    if mart_name.endswith("_by_decade"):
        return f"{aggregation_label} of {metric_description} aggregated by decade."
    if mart_name == "mart_top_warmest_years":
        return f"{aggregation_label} of {metric_description} used to rank the warmest years."

    return f"{aggregation_label} of {metric_description} in `{mart_name}`."


def _describe_source_metric(source_metric_column: str | None) -> str:
    """Render a human-readable source metric description."""
    if source_metric_column is None:
        return "the mart output metric"

    if source_metric_column in SOURCE_METRIC_DESCRIPTIONS:
        return SOURCE_METRIC_DESCRIPTIONS[source_metric_column]

    unit = infer_metric_unit(source_metric_column)
    if unit is None or unit == "count":
        return f"`{source_metric_column}`"

    return f"`{source_metric_column}` ({unit})"


def _build_correlation_description(mart_name: str) -> str:
    """Describe correlation marts in business-readable language."""
    if mart_name == "mart_climate_correlation":
        return "Pearson correlation coefficient between paired climate metrics across aligned years."

    return "Pearson correlation coefficient between paired numeric metrics across the aligned integrated dataset."
