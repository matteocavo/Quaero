"""Simple KPI suggestion heuristics for analytics datasets."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd
from pandas.api.types import is_numeric_dtype


METRIC_HINTS = ("streams", "sales", "revenue", "amount", "value", "score", "count")
DIMENSION_HINTS = ("year", "date", "provider", "artist", "category", "type", "region")


def suggest_kpis(
    question: str,
    dataframe: pd.DataFrame | None = None,
    schema: list[dict[str, Any]] | None = None,
    sample_rows: list[dict[str, Any]] | None = None,
    metric_column: str | None = None,
    aggregation_intent: str | None = None,
) -> dict[str, Any]:
    """Suggest simple KPI and dimension candidates from a dataset and user question."""
    dataset_schema = schema or _build_schema_from_dataframe(dataframe)
    dataset_sample_rows = sample_rows or _build_sample_rows(dataframe)

    question_tokens = _tokenize(question)
    numeric_columns = [
        column["name"] for column in dataset_schema if column["is_numeric"]
    ]
    dimension_columns = _rank_dimension_columns(dataset_schema, question_tokens)

    primary_measure = metric_column or _select_primary_measure(
        numeric_columns, question_tokens
    )
    primary_kpi = _build_primary_kpi_name(
        primary_measure,
        question_tokens,
        aggregation_intent=aggregation_intent,
    )
    secondary_kpis = _build_secondary_kpis(
        primary_measure,
        numeric_columns,
        aggregation_intent=aggregation_intent,
    )

    return {
        "primary_kpi": primary_kpi,
        "secondary_kpis": secondary_kpis,
        "suggested_dimensions": dimension_columns[:3],
        "supporting_schema": dataset_schema,
        "sample_rows": dataset_sample_rows,
    }


def _build_schema_from_dataframe(
    dataframe: pd.DataFrame | None,
) -> list[dict[str, Any]]:
    """Create a small schema summary when a DataFrame is provided."""
    if dataframe is None:
        return []

    return [
        {
            "name": column_name,
            "dtype": str(dataframe[column_name].dtype),
            "is_numeric": bool(is_numeric_dtype(dataframe[column_name])),
        }
        for column_name in dataframe.columns
    ]


def _build_sample_rows(
    dataframe: pd.DataFrame | None, sample_size: int = 3
) -> list[dict[str, Any]]:
    """Return a few sample rows from the DataFrame when available."""
    if dataframe is None:
        return []

    return dataframe.head(sample_size).to_dict(orient="records")


def _tokenize(text: str) -> set[str]:
    """Lowercase and split free-text input into simple keyword tokens."""
    raw_tokens = re.findall(r"[a-z0-9]+", text.lower().replace("_", " "))
    normalized_tokens = set(raw_tokens)

    for token in raw_tokens:
        if token.endswith("s") and len(token) > 1:
            normalized_tokens.add(token[:-1])

    return normalized_tokens


def _select_primary_measure(
    numeric_columns: list[str],
    question_tokens: set[str],
) -> str | None:
    """Choose the strongest numeric measure candidate for KPI suggestions."""
    if not numeric_columns:
        return None

    def score(column_name: str) -> tuple[int, int]:
        tokens = _tokenize(column_name)
        keyword_score = len(tokens & question_tokens)
        metric_score = sum(1 for hint in METRIC_HINTS if hint in column_name.lower())
        return (keyword_score, metric_score)

    return max(numeric_columns, key=score)


def _build_primary_kpi_name(
    primary_measure: str | None,
    question_tokens: set[str],
    aggregation_intent: str | None = None,
) -> str:
    """Create a primary KPI name from the selected measure and question intent."""
    if primary_measure is None:
        return "record_count"

    resolved_aggregation = aggregation_intent or _resolve_aggregation_from_question(
        question_tokens
    )
    if primary_measure == "record_count":
        return "record_count"

    if resolved_aggregation == "count":
        return "record_count"

    if {"average", "avg", "mean"} & question_tokens:
        return f"avg_{primary_measure}"

    if "median" in question_tokens:
        return f"median_{primary_measure}"

    return f"total_{primary_measure}"


def _build_secondary_kpis(
    primary_measure: str | None,
    numeric_columns: list[str],
    aggregation_intent: str | None = None,
) -> list[str]:
    """Create a small list of secondary KPI suggestions."""
    if primary_measure is None:
        return ["unique_record_count"]

    if primary_measure == "record_count" or aggregation_intent == "count":
        return ["record_count", "unique_record_count", "avg_records_per_group"]

    secondary = [
        f"avg_{primary_measure}",
        f"median_{primary_measure}",
        "record_count",
    ]

    for column_name in numeric_columns:
        if column_name != primary_measure:
            secondary.append(f"total_{column_name}")

    return secondary[:3]


def _resolve_aggregation_from_question(question_tokens: set[str]) -> str:
    """Infer a minimal aggregation label from question tokens."""
    if {"count", "number", "most", "largest"} & question_tokens:
        return "count"
    if {"average", "avg", "mean"} & question_tokens:
        return "average"
    if {"total", "sum"} & question_tokens:
        return "sum"
    return "sum"


def _rank_dimension_columns(
    schema: list[dict[str, Any]],
    question_tokens: set[str],
) -> list[str]:
    """Rank likely dimensions based on dtype and name hints."""
    candidates: list[tuple[tuple[int, int], str]] = []

    for column in schema:
        column_name = column["name"]
        lowered = column_name.lower()
        question_score = len(_tokenize(column_name) & question_tokens)
        hint_score = sum(1 for hint in DIMENSION_HINTS if hint in lowered)

        if not column["is_numeric"] or hint_score > 0:
            candidates.append(((question_score, hint_score), column_name))

    candidates.sort(key=lambda item: item[0], reverse=True)
    return [column_name for _, column_name in candidates]
