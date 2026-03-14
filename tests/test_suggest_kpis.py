from __future__ import annotations

import pandas as pd

from kpi_engine.suggest_kpis import suggest_kpis


def test_suggest_kpis_prefers_relevant_metric_and_dimensions() -> None:
    dataframe = pd.DataFrame(
        {
            "release_year": [2023, 2024, 2025],
            "provider": ["A", "B", "A"],
            "artist": ["X", "Y", "Z"],
            "streams": [1000, 1500, 2000],
            "track_count": [10, 12, 14],
        }
    )

    suggestions = suggest_kpis(
        question="Which release years generate the strongest average streams by provider?",
        dataframe=dataframe,
    )

    assert suggestions["primary_kpi"] == "avg_streams"
    assert suggestions["secondary_kpis"] == [
        "avg_streams",
        "median_streams",
        "record_count",
    ]
    assert suggestions["suggested_dimensions"][:2] == ["release_year", "provider"]
    assert len(suggestions["sample_rows"]) == 3


def test_suggest_kpis_uses_count_appropriate_names_for_record_count() -> None:
    dataframe = pd.DataFrame(
        {
            "sector": ["Tech", "Health", "Tech"],
            "company": ["A", "B", "C"],
            "cik": [1, 2, 3],
        }
    )

    suggestions = suggest_kpis(
        question="Which sectors contain the highest number of companies?",
        dataframe=dataframe,
        metric_column="record_count",
        aggregation_intent="count",
    )

    assert suggestions["primary_kpi"] == "record_count"
    assert suggestions["secondary_kpis"] == [
        "record_count",
        "unique_record_count",
        "avg_records_per_group",
    ]
