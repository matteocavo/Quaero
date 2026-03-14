from __future__ import annotations

import pandas as pd
import pytest

from kpi_engine.metric_inference import (
    infer_aggregation_intent,
    infer_dimension_column,
    infer_metric_column,
)


def test_infer_metric_column_prefers_clear_question_match() -> None:
    dataframe = pd.DataFrame(
        {
            "pickup_location_id": [1, 2, 3],
            "trip_distance": [1.5, 2.0, 3.5],
            "fare_amount": [10.0, 12.0, 18.0],
        }
    )

    result = infer_metric_column(
        question="Which pickup locations generate the highest average trip distance?",
        dataframe=dataframe,
    )

    assert result["metric_column"] == "trip_distance"
    assert result["selection_mode"] == "auto_inferred"


def test_infer_metric_column_handles_multiple_numeric_columns() -> None:
    dataframe = pd.DataFrame(
        {
            "population": [100, 120, 150],
            "revenue": [50, 60, 70],
            "score_value": [1.0, 2.0, 3.0],
        }
    )

    result = infer_metric_column(
        question="Which regions have the highest population?",
        dataframe=dataframe,
    )

    assert result["metric_column"] == "population"


def test_infer_metric_column_raises_clear_error_for_ambiguous_case() -> None:
    dataframe = pd.DataFrame(
        {
            "x1": [50, 60, 70],
            "x2": [1.0, 2.0, 3.0],
            "x3": [10, 20, 30],
        }
    )

    with pytest.raises(ValueError) as error:
        infer_metric_column(
            question="Which regions performed best overall?",
            dataframe=dataframe,
        )

    message = str(error.value)
    assert "Could not confidently infer a metric column" in message
    assert "x1" in message
    assert "x2" in message


def test_infer_metric_column_uses_token_based_hint_matching() -> None:
    dataframe = pd.DataFrame(
        {
            "discount_amount": [1.0, 2.0, 3.0],
            "trip_count": [10, 12, 14],
        }
    )

    result = infer_metric_column(
        question="Which rows have the highest discount amount?",
        dataframe=dataframe,
    )

    assert result["metric_column"] == "discount_amount"
    discount_candidate = next(
        candidate
        for candidate in result["candidates"]
        if candidate["column"] == "discount_amount"
    )
    assert discount_candidate["score"] == (2, 1, 1)


def test_infer_metric_column_accepts_single_numeric_column_without_hint_match() -> None:
    dataframe = pd.DataFrame(
        {
            "category": ["A", "B", "C"],
            "x": [1.0, 2.0, 3.0],
        }
    )

    result = infer_metric_column(
        question="Which categories performed best overall?",
        dataframe=dataframe,
    )

    assert result["metric_column"] == "x"
    assert result["selection_mode"] == "auto_inferred"


def test_infer_dimension_column_prefers_question_matched_grouping_field() -> None:
    dataframe = pd.DataFrame(
        {
            "pickup_location_id": [101, 102, 103],
            "provider": ["A", "B", "C"],
            "trip_distance": [1.5, 2.0, 3.5],
        }
    )

    result = infer_dimension_column(
        question="Which pickup locations generate the highest average trip distance?",
        dataframe=dataframe,
        metric_column="trip_distance",
    )

    assert result["dimension_column"] == "pickup_location_id"
    assert result["selection_mode"] == "auto_inferred"


def test_infer_dimension_column_handles_compact_location_id_names() -> None:
    dataframe = pd.DataFrame(
        {
            "PULocationID": [101, 102, 103],
            "DOLocationID": [201, 202, 203],
            "trip_distance": [1.5, 2.0, 3.5],
        }
    )

    result = infer_dimension_column(
        question="Which pickup locations generate the highest average trip distance?",
        dataframe=dataframe,
        metric_column="trip_distance",
    )

    assert result["dimension_column"] == "PULocationID"


def test_infer_dimension_column_raises_clear_error_for_ambiguous_case() -> None:
    dataframe = pd.DataFrame(
        {
            "provider": ["A", "B", "C"],
            "region": ["North", "South", "East"],
            "category": ["X", "Y", "Z"],
            "value": [10.0, 20.0, 30.0],
        }
    )

    with pytest.raises(ValueError) as error:
        infer_dimension_column(
            question="Which groups performed best overall?",
            dataframe=dataframe,
            metric_column="value",
        )

    message = str(error.value)
    assert "Could not confidently infer a dimension column" in message
    assert "provider" in message
    assert "region" in message


def test_infer_aggregation_intent_detects_average_highest() -> None:
    result = infer_aggregation_intent(
        "Which pickup locations generate the highest average trip distance?"
    )

    assert result["aggregation"] == "average"
    assert result["ordering"] == "highest"
    assert result["selection_mode"] == "auto_inferred"


def test_infer_aggregation_intent_treats_most_as_count_when_no_other_agg() -> None:
    result = infer_aggregation_intent("Which release years contain the most titles?")

    assert result["aggregation"] == "count"
    assert result["ordering"] == "highest"


def test_infer_metric_column_uses_record_count_for_count_questions() -> None:
    dataframe = pd.DataFrame(
        {
            "sector": ["Tech", "Health", "Tech"],
            "founded": [1999, 2005, 2010],
        }
    )

    result = infer_metric_column(
        question="Which sectors contain the highest number of companies?",
        dataframe=dataframe,
    )

    assert result["metric_column"] == "record_count"
    assert result["selection_mode"] == "auto_inferred"


def test_infer_dimension_column_handles_plural_country_terms() -> None:
    dataframe = pd.DataFrame(
        {
            "country": ["Italy", "France", "Italy"],
            "subcountry": ["Lazio", "Ile-de-France", "Lombardy"],
            "geonameid": [1, 2, 3],
        }
    )

    result = infer_dimension_column(
        question="Which countries contain the largest number of cities?",
        dataframe=dataframe,
        metric_column="record_count",
    )

    assert result["dimension_column"] == "country"


def test_infer_dimension_column_accepts_structural_candidate_with_type_score() -> None:
    dataframe = pd.DataFrame(
        {
            "provider": ["A", "B", "C"],
            "value": [10.0, 20.0, 30.0],
        }
    )

    result = infer_dimension_column(
        question="Show the breakdown overall.",
        dataframe=dataframe,
        metric_column="value",
    )

    assert result["dimension_column"] == "provider"


def test_infer_aggregation_intent_detects_lowest_ordering() -> None:
    result = infer_aggregation_intent("Which regions have the lowest average revenue?")

    assert result["aggregation"] == "average"
    assert result["ordering"] == "lowest"
    assert result["selection_mode"] == "auto_inferred"


def test_infer_aggregation_intent_detects_count_aggregation() -> None:
    result = infer_aggregation_intent(
        "Which providers have the highest number of trips?"
    )

    assert result["aggregation"] == "count"
    assert result["selection_mode"] == "auto_inferred"


def test_infer_aggregation_intent_detects_sum_aggregation() -> None:
    result = infer_aggregation_intent(
        "Which release years generated the highest total streams?"
    )

    assert result["aggregation"] == "sum"
    assert result["ordering"] == "highest"
    assert result["selection_mode"] == "auto_inferred"
