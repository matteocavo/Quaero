"""Analytical metadata helpers for dataset interpretation."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_bool_dtype, is_datetime64_any_dtype, is_numeric_dtype

from pipelines.utils import normalize_name, resolve_project_root


LOGGER = logging.getLogger(__name__)

METRIC_HINTS = ("streams", "sales", "revenue", "amount", "value", "score", "count")
TIME_HINTS = ("date", "time", "timestamp", "year", "month", "week", "day")
GROUPING_HINTS = (
    "provider",
    "artist",
    "category",
    "region",
    "type",
    "segment",
)


def build_analytics_metadata(
    dataframe: pd.DataFrame,
    source_name: str,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Generate analytical metadata and persist it as JSON."""
    root_dir = resolve_project_root(project_root, __file__)
    metadata_dir = root_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    normalized_source_name = normalize_name(source_name)
    schema = _build_schema(dataframe)
    metadata = {
        "source": normalized_source_name,
        "column_importance_candidates": _column_importance_candidates(schema),
        "possible_kpis": _possible_kpis(schema),
        "suggested_dimensions": _suggested_dimensions(schema),
        "suggested_time_columns": _suggested_time_columns(schema),
        "suggested_grouping_fields": _suggested_grouping_fields(schema),
    }

    output_path = metadata_dir / f"analytics_metadata_{normalized_source_name}.json"
    output_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    LOGGER.info("Analytics metadata written to %s", output_path)
    return {
        **metadata,
        "output_path": output_path.relative_to(root_dir).as_posix(),
    }


def _build_schema(dataframe: pd.DataFrame) -> list[dict[str, Any]]:
    """Build a lightweight schema summary for analytical heuristics."""
    return [
        {
            "name": column_name,
            "dtype": str(dataframe[column_name].dtype),
            "is_bool": bool(is_bool_dtype(dataframe[column_name])),
            "is_numeric": bool(
                is_numeric_dtype(dataframe[column_name])
                and not is_bool_dtype(dataframe[column_name])
            ),
            "is_datetime": bool(is_datetime64_any_dtype(dataframe[column_name])),
        }
        for column_name in dataframe.columns
    ]


def _column_importance_candidates(schema: list[dict[str, Any]]) -> list[str]:
    """Rank columns that are likely to matter most analytically."""
    scored_columns = sorted(
        schema,
        key=lambda column: (
            _name_hint_score(
                column["name"], METRIC_HINTS + TIME_HINTS + GROUPING_HINTS
            ),
            int(column["is_numeric"] or column["is_datetime"]),
        ),
        reverse=True,
    )
    return [column["name"] for column in scored_columns[:5]]


def _possible_kpis(schema: list[dict[str, Any]]) -> list[str]:
    """Suggest simple KPI names based on numeric columns."""
    kpis: list[str] = []

    for column in schema:
        if not column["is_numeric"]:
            continue

        column_name = column["name"]
        if any(hint in column_name.lower() for hint in METRIC_HINTS):
            kpis.extend(
                [
                    f"total_{column_name}",
                    f"avg_{column_name}",
                ]
            )

    if not kpis:
        return ["record_count"]

    return kpis[:6]


def _suggested_dimensions(schema: list[dict[str, Any]]) -> list[str]:
    """Suggest likely dimension fields."""
    dimensions = [
        column["name"]
        for column in schema
        if (not column["is_numeric"]) and (not column["is_bool"])
    ]
    return dimensions[:5]


def _suggested_time_columns(schema: list[dict[str, Any]]) -> list[str]:
    """Suggest likely time fields."""
    return [
        column["name"]
        for column in schema
        if column["is_datetime"] or _name_hint_score(column["name"], TIME_HINTS) > 0
    ][:5]


def _suggested_grouping_fields(schema: list[dict[str, Any]]) -> list[str]:
    """Suggest likely grouping fields for analysis."""
    candidates = sorted(
        [
            column["name"]
            for column in schema
            if ((not column["is_numeric"]) and (not column["is_bool"]))
            or _name_hint_score(column["name"], GROUPING_HINTS)
        ],
        key=lambda name: _name_hint_score(name, GROUPING_HINTS),
        reverse=True,
    )
    return candidates[:5]


def _name_hint_score(column_name: str, hints: tuple[str, ...]) -> int:
    """Score a column name by the number of matching semantic hints."""
    lowered = column_name.lower()
    return sum(1 for hint in hints if hint in lowered)
