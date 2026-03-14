from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from pipelines.cleaning import clean_dataframe


def test_clean_dataframe_applies_mvp_cleaning_strategy(tmp_path: Path) -> None:
    dataframe = pd.DataFrame(
        {
            " Release Date ": [
                "2026-01-01",
                "2026-01-01",
                "bad-date",
                "2026-03-15",
                "2026-04-20",
                None,
            ],
            "Score Value": ["100", "100", "200", "oops", "300", "400"],
            "Category Name": ["A", "A", "B", "C", "D", " "],
            "is_active": [True, True, False, True, False, True],
            "Created At": [
                "2026-01-01T10:00:00",
                "2026-01-01T10:00:00",
                "2026-02-01T09:30:00",
                "not-a-date",
                "2026-04-20T12:00:00",
                None,
            ],
        }
    )

    result = clean_dataframe(
        dataframe=dataframe,
        source_name="Sales Export",
        project_root=tmp_path,
    )

    cleaned = result["dataframe"]
    summary = result["summary"]
    output_path = tmp_path / summary["output_path"]
    metadata_path = tmp_path / summary["metadata_path"]

    assert list(cleaned.columns) == [
        "release_date",
        "score_value",
        "category_name",
        "is_active",
        "created_at",
        "has_nulls",
        "invalid_date_parse",
        "type_conversion_issue",
    ]
    assert cleaned.shape == (5, 8)
    assert cleaned["release_date"].dtype == "string"
    assert pd.api.types.is_integer_dtype(cleaned["score_value"])
    assert pd.api.types.is_bool_dtype(cleaned["is_active"])
    assert cleaned["created_at"].dtype == "string"
    assert cleaned["has_nulls"].tolist() == [False, False, True, False, True]
    assert cleaned["invalid_date_parse"].tolist() == [False, True, True, False, False]
    assert cleaned["type_conversion_issue"].tolist() == [
        False,
        False,
        True,
        False,
        False,
    ]

    assert output_path.exists()
    assert metadata_path.exists()
    assert summary["source"] == "sales_export"
    assert summary["row_count"] == 5
    assert summary["column_count"] == 5
    assert summary["duplicate_row_count"] == 1
    assert summary["rows_with_nulls"] == 2
    assert summary["rows_with_invalid_date_parse"] == 2
    assert summary["rows_with_type_conversion_issue"] == 1
    assert summary["output_path"].startswith("staging/staging_")
    assert summary["output_path"].endswith(".parquet")
    assert re.match(
        r"staging/staging_\d{4}_\d{2}_\d{2}_\d{6}_[0-9a-f]{8}\.parquet",
        summary["output_path"],
    )
    assert summary["metadata_path"] == "metadata/cleaning_summary_sales_export.json"
    assert summary["null_counts_by_column"] == {
        "release_date": 1,
        "score_value": 1,
        "category_name": 1,
        "is_active": 0,
        "created_at": 1,
    }

    saved_summary = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert saved_summary == summary

    written_frame = pd.read_parquet(output_path)
    assert written_frame.shape == (5, 8)


def test_clean_dataframe_parses_valid_dates_and_preserves_string_columns(
    tmp_path: Path,
) -> None:
    dataframe = pd.DataFrame(
        {
            "Updated At": ["2026-01-01", "2026-01-02"],
            "Mixed Code": ["10", "ABC"],
            "Amount": [1.5, 2.5],
            "is_active": [True, False],
        }
    )

    result = clean_dataframe(
        dataframe=dataframe,
        source_name="Example Source",
        project_root=tmp_path,
    )

    cleaned = result["dataframe"]
    summary = result["summary"]

    assert pd.api.types.is_datetime64_any_dtype(cleaned["updated_at"])
    assert str(cleaned["updated_at"].dtype) == "datetime64[ns, UTC]"
    assert cleaned["mixed_code"].dtype == "string"
    assert pd.api.types.is_float_dtype(cleaned["amount"])
    assert pd.api.types.is_bool_dtype(cleaned["is_active"])
    assert cleaned["invalid_date_parse"].tolist() == [False, False]
    assert cleaned["type_conversion_issue"].tolist() == [False, False]
    assert cleaned["has_nulls"].tolist() == [False, False]
    assert summary["column_count"] == 4
    assert summary["duplicate_row_count"] == 0


def test_clean_dataframe_generates_unique_staging_paths_per_run(tmp_path: Path) -> None:
    dataframe = pd.DataFrame({"Updated At": ["2026-01-01"], "Amount": [1.5]})

    first = clean_dataframe(
        dataframe=dataframe,
        source_name="Unique Example",
        project_root=tmp_path,
    )
    second = clean_dataframe(
        dataframe=dataframe,
        source_name="Unique Example",
        project_root=tmp_path,
    )

    assert first["summary"]["output_path"] != second["summary"]["output_path"]
