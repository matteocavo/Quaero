"""Dashboard suggestion helpers based on dataset structure."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from pipelines.utils import normalize_name, resolve_project_root

import pandas as pd
from pandas.api.types import is_bool_dtype, is_datetime64_any_dtype, is_numeric_dtype


LOGGER = logging.getLogger(__name__)


def generate_dashboard_suggestions(
    dataframe: pd.DataFrame,
    source_name: str,
    question: str | None = None,
    primary_mart_name: str | None = None,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Suggest simple dashboards from a dataset's structure and write JSON output."""
    root_dir = resolve_project_root(project_root, __file__)
    metadata_dir = root_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    normalized_source_name = normalize_name(source_name)

    schema = _build_schema(dataframe)
    time_dimensions = _time_dimensions(schema)
    category_dimensions = _category_dimensions(schema)
    measures = _measures(schema, question=question)

    suggestions = {"dashboard_suggestions": []}

    if time_dimensions and measures:
        suggestions["dashboard_suggestions"].append(
            {
                "type": "time_series",
                "metric": measures[0],
                "dimension": time_dimensions[0],
            }
        )

    if category_dimensions and measures:
        suggestions["dashboard_suggestions"].append(
            {
                "type": "category_comparison",
                "metric": measures[0],
                "dimension": category_dimensions[0],
            }
        )

    if len(measures) >= 2:
        suggestions["dashboard_suggestions"].append(
            {
                "type": "dual_metric",
                "metric": measures[0],
                "second_metric": measures[1],
            }
        )

    if question is not None:
        suggestions["question"] = question
    if primary_mart_name is not None:
        suggestions["primary_mart"] = primary_mart_name
    if measures and (question is not None or primary_mart_name is not None):
        suggestions["narrative_focus"] = {
            "primary_metric": measures[0],
            "primary_dimension": (time_dimensions or category_dimensions or [None])[0],
        }

    output_path = metadata_dir / f"dashboard_suggestions_{normalized_source_name}.json"
    output_path.write_text(json.dumps(suggestions, indent=2), encoding="utf-8")

    LOGGER.info("Dashboard suggestions written to %s", output_path)
    return {
        **suggestions,
        "output_path": output_path.relative_to(root_dir).as_posix(),
    }


def _build_schema(dataframe: pd.DataFrame) -> list[dict[str, Any]]:
    """Build a small schema summary from a pandas DataFrame."""
    return [
        {
            "name": column_name,
            "is_numeric": bool(
                is_numeric_dtype(dataframe[column_name])
                and not is_bool_dtype(dataframe[column_name])
            ),
            "is_datetime": bool(is_datetime64_any_dtype(dataframe[column_name])),
        }
        for column_name in dataframe.columns
    ]


def _time_dimensions(schema: list[dict[str, Any]]) -> list[str]:
    """Return columns that look like time dimensions."""
    return [
        column["name"]
        for column in schema
        if column["is_datetime"]
        or any(
            token in column["name"].lower()
            for token in ("date", "time", "timestamp", "year", "month", "week")
        )
    ]


def _category_dimensions(schema: list[dict[str, Any]]) -> list[str]:
    """Return columns that look like categorical dimensions."""
    return [
        column["name"]
        for column in schema
        if not column["is_numeric"] and not column["is_datetime"]
    ]


def _measures(schema: list[dict[str, Any]], question: str | None = None) -> list[str]:
    """Return numeric columns that can serve as chart measures."""
    measures = [
        column["name"]
        for column in schema
        if column["is_numeric"]
        and not any(
            token in column["name"].lower()
            for token in ("date", "time", "timestamp", "year", "month", "week")
        )
    ]
    if question is None:
        return measures

    question_tokens = _tokenize(question)
    return sorted(
        measures,
        key=lambda column_name: _score_measure_against_question(
            column_name=column_name,
            question=question,
            question_tokens=question_tokens,
        ),
        reverse=True,
    )


def _tokenize(text: str) -> set[str]:
    """Split free text into a small lowercase token set for ranking."""
    normalized = text.lower().replace("_", " ")
    raw_tokens = [
        token
        for token in "".join(
            character if character.isalnum() or character.isspace() else " "
            for character in normalized
        ).split()
        if token
    ]
    return set(raw_tokens)


def _score_measure_against_question(
    column_name: str,
    question: str,
    question_tokens: set[str],
) -> tuple[int, int]:
    """Score one measure by token overlap and question mention order."""
    overlap_score = len(_tokenize(column_name) & question_tokens)
    phrase = column_name.replace("_", " ").lower()
    position = question.lower().find(phrase)
    position_score = -position if position >= 0 else -10_000
    return (overlap_score, position_score)
