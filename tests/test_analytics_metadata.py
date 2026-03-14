from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from kpi_engine.analytics_metadata import build_analytics_metadata


def test_build_analytics_metadata_writes_expected_output(tmp_path: Path) -> None:
    dataframe = pd.DataFrame(
        {
            "release_year": [2024, 2025],
            "provider": ["A", "B"],
            "streams": [250, 400],
            "score_value": [10.5, 12.0],
            "created_at": pd.to_datetime(["2026-01-01", "2026-01-02"]),
        }
    )

    metadata = build_analytics_metadata(
        dataframe=dataframe,
        source_name="Release Impact",
        project_root=tmp_path,
    )

    output_path = tmp_path / "metadata" / "analytics_metadata_release_impact.json"
    assert output_path.exists()

    saved = json.loads(output_path.read_text(encoding="utf-8"))
    assert saved == {
        key: value for key, value in metadata.items() if key != "output_path"
    }
    assert metadata["source"] == "release_impact"
    assert metadata["output_path"] == "metadata/analytics_metadata_release_impact.json"
    assert "streams" in metadata["column_importance_candidates"]
    assert "total_streams" in metadata["possible_kpis"]
    assert "provider" in metadata["suggested_dimensions"]
    assert "created_at" in metadata["suggested_time_columns"]
    assert "provider" in metadata["suggested_grouping_fields"]


def test_build_analytics_metadata_excludes_boolean_quality_flags_from_dimensions(
    tmp_path: Path,
) -> None:
    dataframe = pd.DataFrame(
        {
            "provider": ["A", "B"],
            "streams": [100, 200],
            "has_nulls": [False, True],
            "invalid_date_parse": [False, False],
        }
    )

    metadata = build_analytics_metadata(
        dataframe=dataframe,
        source_name="Quality Flags",
        project_root=tmp_path,
    )

    assert "has_nulls" not in metadata["suggested_dimensions"]
    assert "invalid_date_parse" not in metadata["suggested_dimensions"]
