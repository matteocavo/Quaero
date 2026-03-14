from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pipelines.dashboard_suggestions import generate_dashboard_suggestions


def test_generate_dashboard_suggestions_writes_expected_json(
    tmp_path: Path,
) -> None:
    dataframe = pd.DataFrame(
        {
            "release_year": [2024, 2025],
            "provider": ["A", "B"],
            "streams": [250, 400],
            "track_count": [10, 20],
        }
    )

    suggestions = generate_dashboard_suggestions(
        dataframe=dataframe,
        source_name="Release Impact",
        project_root=tmp_path,
    )

    output_path = tmp_path / "metadata" / "dashboard_suggestions_release_impact.json"
    assert output_path.exists()

    saved = json.loads(output_path.read_text(encoding="utf-8"))
    assert saved == {
        "dashboard_suggestions": [
            {
                "type": "time_series",
                "metric": "streams",
                "dimension": "release_year",
            },
            {
                "type": "category_comparison",
                "metric": "streams",
                "dimension": "provider",
            },
            {
                "type": "dual_metric",
                "metric": "streams",
                "second_metric": "track_count",
            },
        ]
    }
    assert suggestions == {
        "dashboard_suggestions": saved["dashboard_suggestions"],
        "output_path": "metadata/dashboard_suggestions_release_impact.json",
    }


def test_generate_dashboard_suggestions_includes_question_context_when_provided(
    tmp_path: Path,
) -> None:
    dataframe = pd.DataFrame(
        {
            "year": [2020, 2021],
            "temperature_anomaly": [0.8, 0.9],
            "co2": [410, 412],
        }
    )

    suggestions = generate_dashboard_suggestions(
        dataframe=dataframe,
        source_name="Climate Change Analysis",
        question="How has temperature anomaly changed over time, and how does it relate to CO2?",
        primary_mart_name="mart_temperature_anomaly_trend",
        project_root=tmp_path,
    )

    output_path = (
        tmp_path / "metadata" / "dashboard_suggestions_climate_change_analysis.json"
    )
    saved = json.loads(output_path.read_text(encoding="utf-8"))

    assert saved["question"].startswith("How has temperature anomaly changed")
    assert saved["primary_mart"] == "mart_temperature_anomaly_trend"
    assert saved["narrative_focus"] == {
        "primary_metric": "temperature_anomaly",
        "primary_dimension": "year",
    }
    assert suggestions["question"] == saved["question"]
    assert suggestions["primary_mart"] == saved["primary_mart"]
