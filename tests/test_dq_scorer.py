"""Tests for the Data Quality scorer."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from pipelines.dq_scorer import (
    score_dataframe,
    _completeness,
    _uniqueness,
    _outlier_flag,
    _format_score,
)


# ---------------------------------------------------------------------------
# Unit tests for individual dimension helpers
# ---------------------------------------------------------------------------

class TestCompleteness:
    def test_full_series_scores_100(self):
        s = pd.Series([1, 2, 3])
        assert _completeness(s) == 100.0

    def test_half_null_scores_50(self):
        s = pd.Series([1, None, 3, None])
        assert _completeness(s) == 50.0

    def test_empty_series_scores_100(self):
        assert _completeness(pd.Series([], dtype=float)) == 100.0


class TestUniqueness:
    def test_all_unique_scores_100(self):
        s = pd.Series([1, 2, 3])
        assert _uniqueness(s) == 100.0

    def test_all_same_scores_low(self):
        s = pd.Series([1, 1, 1, 1])
        assert _uniqueness(s) == 25.0

    def test_null_only_scores_100(self):
        s = pd.Series([None, None])
        assert _uniqueness(s) == 100.0


class TestOutlierFlag:
    def test_no_outlier_returns_false(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
        assert _outlier_flag(s) is False

    def test_extreme_outlier_returns_true(self):
        s = pd.Series([1.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 1000.0])
        assert _outlier_flag(s) is True

    def test_string_series_returns_false(self):
        s = pd.Series(["a", "b", "c"])
        assert _outlier_flag(s) is False

    def test_too_few_values_returns_false(self):
        s = pd.Series([1.0, 2.0])
        assert _outlier_flag(s) is False


class TestFormatScore:
    def test_numeric_scores_100(self):
        s = pd.Series([1.0, 2.0, 3.0])
        assert _format_score(s) == 100.0

    def test_clean_string_scores_100(self):
        s = pd.Series(["a", "b", "c"])
        assert _format_score(s) == 100.0

    def test_empty_string_series_scores_100(self):
        s = pd.Series([], dtype="object")
        assert _format_score(s) == 100.0


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------

class TestScoreDataframe:
    def test_output_keys(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        result = score_dataframe(df, source_name="test", project_root=tmp_path)
        assert "overall_score" in result
        assert "columns" in result
        assert "output_path" in result
        assert len(result["columns"]) == 2

    def test_file_written(self, tmp_path):
        df = pd.DataFrame({"val": [1.0, 2.0, None]})
        score_dataframe(df, source_name="myds", project_root=tmp_path)
        out = tmp_path / "metadata" / "dq_score_myds.json"
        assert out.exists()
        data = json.loads(out.read_text())
        assert "overall_score" in data

    def test_quality_flag_columns_excluded(self, tmp_path):
        df = pd.DataFrame({
            "value": [1, 2, 3],
            "has_nulls": [False, False, True],
            "invalid_date_parse": [False, False, False],
            "type_conversion_issue": [False, False, False],
        })
        result = score_dataframe(df, source_name="flags", project_root=tmp_path)
        col_names = [c["column"] for c in result["columns"]]
        assert "has_nulls" not in col_names
        assert "value" in col_names

    def test_overall_score_between_0_and_100(self, tmp_path):
        df = pd.DataFrame({"x": [1, None, 3], "y": ["a", "b", "a"]})
        result = score_dataframe(df, source_name="range_check", project_root=tmp_path)
        assert 0.0 <= result["overall_score"] <= 100.0
