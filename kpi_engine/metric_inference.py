"""Deterministic metric, dimension, and aggregation inference helpers."""

from __future__ import annotations

import re
from typing import Any

import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
)


METRIC_HINTS = (
    "amount",
    "total",
    "price",
    "revenue",
    "sales",
    "streams",
    "count",
    "score",
    "distance",
    "duration",
    "fare",
    "tip",
    "population",
    "value",
    "metric",
    "volume",
    "quantity",
)

DIMENSION_HINTS = (
    "category",
    "type",
    "provider",
    "artist",
    "country",
    "city",
    "region",
    "sector",
    "year",
    "month",
    "date",
    "location",
    "pickup",
    "dropoff",
    "vendor",
    "genre",
    "state",
    "segment",
    "class",
    "group",
)

NUMERIC_DIMENSION_HINTS = (
    "id",
    "year",
    "month",
    "date",
    "location",
    "pickup",
    "dropoff",
    "vendor",
    "city",
    "region",
    "state",
    "country",
    "zone",
)


def infer_metric_column(
    question: str,
    dataframe: pd.DataFrame,
) -> dict[str, Any]:
    """Infer the strongest numeric metric column from a dataset and question."""
    aggregation_intent = infer_aggregation_intent(question)
    if aggregation_intent["aggregation"] == "count":
        return {
            "metric_column": "record_count",
            "selection_mode": "auto_inferred",
            "candidates": [
                {"column": "record_count", "score": ("count", "question_intent", None)}
            ],
        }

    candidates = _score_metric_candidates(question=question, dataframe=dataframe)
    if not candidates:
        raise ValueError(
            "Could not infer a metric column because the dataset has no numeric columns. "
            "Provide --metric-column manually."
        )

    top_candidate = candidates[0]
    top_score = top_candidate["score"]
    second_score = candidates[1]["score"] if len(candidates) > 1 else None

    has_signal = top_score[0] > 0 or top_score[1] > 0 or top_score[2] > 0
    is_unique = second_score is None or top_score > second_score

    if len(candidates) == 1 or (has_signal and is_unique):
        return {
            "metric_column": top_candidate["column"],
            "selection_mode": "auto_inferred",
            "candidates": candidates,
        }

    import os

    api_key = os.getenv("QUAERO_ANTHROPIC_API_KEY")
    if api_key:
        from kpi_engine.llm_inference import infer_column_with_llm

        schema_with_types = [
            f"{col} ({dataframe[col].dtype})" for col in dataframe.columns
        ]
        try:
            llm_col = infer_column_with_llm(
                question, schema_with_types, "numeric metric", api_key
            )
        except Exception:
            llm_col = None
        if llm_col in dataframe.columns:
            return {
                "metric_column": llm_col,
                "selection_mode": "llm_assisted",
                "candidates": candidates,
            }

    top_candidates = ", ".join(candidate["column"] for candidate in candidates[:5])
    raise ValueError(
        "Could not confidently infer a metric column from the question. "
        f"Top candidate metric columns: {top_candidates}. "
        "Provide --metric-column manually."
    )


def infer_dimension_column(
    question: str,
    dataframe: pd.DataFrame,
    metric_column: str | None = None,
) -> dict[str, Any]:
    """Infer the strongest grouping dimension from a dataset and question."""
    candidates = score_dimension_candidates(
        question=question,
        dataframe=dataframe,
        metric_column=metric_column,
    )
    if not candidates:
        raise ValueError(
            "Could not infer a dimension column because the dataset has no "
            "groupable columns. Provide --dimension-column manually."
        )

    top_candidate = candidates[0]
    top_score = top_candidate["score"]
    second_score = candidates[1]["score"] if len(candidates) > 1 else None

    is_strong = (
        len(candidates) == 1 or top_score[0] > 0 or top_score[1] > 0 or top_score[3] > 1
    )
    is_unique = second_score is None or top_score > second_score

    if is_strong and is_unique:
        return {
            "dimension_column": top_candidate["column"],
            "selection_mode": "auto_inferred",
            "candidates": candidates,
        }

    import os

    api_key = os.getenv("QUAERO_ANTHROPIC_API_KEY")
    if api_key:
        from kpi_engine.llm_inference import infer_column_with_llm

        schema_with_types = [
            f"{col} ({dataframe[col].dtype})" for col in dataframe.columns
        ]
        try:
            llm_col = infer_column_with_llm(
                question, schema_with_types, "grouping dimension", api_key
            )
        except Exception:
            llm_col = None
        if llm_col in dataframe.columns:
            return {
                "dimension_column": llm_col,
                "selection_mode": "llm_assisted",
                "candidates": candidates,
            }

    top_candidates = ", ".join(candidate["column"] for candidate in candidates[:5])
    raise ValueError(
        "Could not confidently infer a dimension column from the question. "
        f"Top candidate dimension columns: {top_candidates}. "
        "Provide --dimension-column manually."
    )


def infer_aggregation_intent(question: str) -> dict[str, str]:
    """Infer a small aggregation intent summary from a question."""
    question_tokens = _tokenize(question)

    if {"count", "number"} & question_tokens:
        aggregation = "count"
    elif {"most", "largest"} & question_tokens and not (
        {"average", "avg", "mean", "total", "sum"} & question_tokens
    ):
        aggregation = "count"
    elif {"total", "sum"} & question_tokens:
        aggregation = "sum"
    else:
        aggregation = "average"

    if {"lowest", "bottom", "least", "minimum", "min", "fewest"} & question_tokens:
        ordering = "lowest"
    else:
        ordering = "highest"

    return {
        "aggregation": aggregation,
        "ordering": ordering,
        "selection_mode": "auto_inferred",
    }


def score_metric_candidates(
    question: str,
    dataframe: pd.DataFrame,
) -> list[dict[str, Any]]:
    """Score numeric columns by overlap with the question and metric hints."""
    question_tokens = _tokenize(question)
    candidates: list[dict[str, Any]] = []

    for column_name in dataframe.columns:
        series = dataframe[column_name]
        if not is_numeric_dtype(series) or is_bool_dtype(series):
            continue

        column_tokens = _tokenize(column_name)
        question_overlap = len(column_tokens & question_tokens)
        hint_overlap = sum(
            1
            for hint in METRIC_HINTS
            if hint in question_tokens and hint in column_tokens
        )
        column_hint_score = sum(1 for hint in METRIC_HINTS if hint in column_tokens)

        candidates.append(
            {
                "column": column_name,
                "score": (question_overlap, hint_overlap, column_hint_score),
            }
        )

    candidates.sort(
        key=lambda candidate: (candidate["score"], candidate["column"]),
        reverse=True,
    )
    return candidates


def score_dimension_candidates(
    question: str,
    dataframe: pd.DataFrame,
    metric_column: str | None = None,
) -> list[dict[str, Any]]:
    """Score likely grouping dimensions using question hints and column shape."""
    question_tokens = _tokenize(question)
    candidates: list[dict[str, Any]] = []

    for column_name in dataframe.columns:
        if column_name == metric_column:
            continue

        series = dataframe[column_name]
        if is_bool_dtype(series):
            continue

        is_numeric = bool(is_numeric_dtype(series) and not is_bool_dtype(series))
        is_datetime = bool(is_datetime64_any_dtype(series))
        column_tokens = _tokenize(column_name)
        question_overlap = len(column_tokens & question_tokens)
        exact_name_match = 1 if column_name.lower() in question_tokens else 0
        hint_overlap = sum(
            1
            for hint in DIMENSION_HINTS
            if hint in question_tokens and hint in column_tokens
        )
        type_score = _dimension_type_score(
            series=series,
            column_tokens=column_tokens,
            is_numeric=is_numeric,
            is_datetime=is_datetime,
        )
        grouping_score = _grouping_cardinality_score(series)

        if type_score == 0:
            continue

        candidates.append(
            {
                "column": column_name,
                "score": (
                    question_overlap,
                    exact_name_match,
                    hint_overlap,
                    type_score,
                    grouping_score,
                ),
            }
        )

    candidates.sort(
        key=lambda candidate: (candidate["score"], candidate["column"]),
        reverse=True,
    )
    return candidates


def _score_metric_candidates(
    question: str,
    dataframe: pd.DataFrame,
) -> list[dict[str, Any]]:
    """Backward-compatible alias for metric candidate scoring."""
    return score_metric_candidates(question=question, dataframe=dataframe)


def _tokenize(text: str) -> set[str]:
    """Lowercase and split free-text input into simple normalized tokens."""
    raw_tokens = re.findall(r"[a-z0-9]+", text.lower().replace("_", " "))
    normalized_tokens = set(raw_tokens)

    for token in raw_tokens:
        if token.endswith("ies") and len(token) > 3:
            normalized_tokens.add(f"{token[:-3]}y")
        if token.endswith("s") and len(token) > 1:
            normalized_tokens.add(token[:-1])
        normalized_tokens.update(_expand_compound_dimension_tokens(token))

    return normalized_tokens


def _dimension_type_score(
    series: pd.Series,
    column_tokens: set[str],
    is_numeric: bool,
    is_datetime: bool,
) -> int:
    """Assign a small type score for likely grouping columns."""
    if is_datetime or {"year", "month", "date"} & column_tokens:
        return 3

    if not is_numeric:
        return 2

    if _is_numeric_dimension_candidate(column_tokens) and (
        not any(hint in column_tokens for hint in METRIC_HINTS)
        or series.nunique(dropna=True) <= 500
    ):
        return 1

    return 0


def _grouping_cardinality_score(series: pd.Series) -> int:
    """Favor columns with a useful number of groups for aggregation."""
    non_null_count = int(series.notna().sum())
    if non_null_count == 0:
        return 0

    unique_count = int(series.nunique(dropna=True))
    if unique_count <= 1:
        return 0

    unique_ratio = unique_count / non_null_count
    if unique_count <= 24 or unique_ratio <= 0.5:
        return 2
    if unique_count <= 100 or unique_ratio <= 0.9:
        return 1
    return 0


def _is_numeric_dimension_candidate(column_tokens: set[str]) -> bool:
    """Return whether a numeric column still looks like a valid dimension."""
    return bool(column_tokens & set(NUMERIC_DIMENSION_HINTS))


def _expand_compound_dimension_tokens(token: str) -> set[str]:
    """Add useful dimension hints for compact column names such as PULocationID."""
    expanded_tokens: set[str] = set()

    if token.startswith("pu"):
        expanded_tokens.add("pickup")
    if token.startswith("do"):
        expanded_tokens.add("dropoff")
    if token.endswith("id"):
        expanded_tokens.add("id")

    for hint in set(DIMENSION_HINTS + NUMERIC_DIMENSION_HINTS):
        if len(hint) >= 4 and hint in token:
            expanded_tokens.add(hint)

    return expanded_tokens
