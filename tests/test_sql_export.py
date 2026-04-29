"""Tests for the SQL mart export utilities."""

from __future__ import annotations

import pandas as pd
import pytest

from pipelines.sql_export import export_marts_to_sql, _infer_pg_type, _format_value


class TestInferPgType:
    def test_integer(self):
        assert _infer_pg_type(pd.Series([1, 2, 3])) == "BIGINT"

    def test_float(self):
        assert _infer_pg_type(pd.Series([1.0, 2.5])) == "DOUBLE PRECISION"

    def test_bool(self):
        assert _infer_pg_type(pd.Series([True, False])) == "BOOLEAN"

    def test_datetime(self):
        s = pd.to_datetime(pd.Series(["2024-01-01", "2024-06-01"]))
        assert _infer_pg_type(s) == "TIMESTAMPTZ"

    def test_string(self):
        assert _infer_pg_type(pd.Series(["a", "b"])) == "TEXT"


class TestFormatValue:
    def test_none_returns_null(self):
        assert _format_value(None) == "NULL"

    def test_float_nan_returns_null(self):
        assert _format_value(float("nan")) == "NULL"

    def test_integer(self):
        assert _format_value(42) == "42"

    def test_string_with_quote(self):
        assert _format_value("it's") == "'it''s'"

    def test_bool_true(self):
        assert _format_value(True) == "TRUE"

    def test_bool_false(self):
        assert _format_value(False) == "FALSE"


class TestExportMartsToSql:
    def test_creates_sql_file(self, tmp_path):
        df = pd.DataFrame({"year": [2020, 2021], "total": [100.0, 200.0]})
        paths = export_marts_to_sql({"mart_test": df}, project_root=tmp_path)
        assert "mart_test" in paths
        sql_file = tmp_path / "marts" / "mart_test.sql"
        assert sql_file.exists()

    def test_sql_contains_create_and_insert(self, tmp_path):
        df = pd.DataFrame({"id": [1, 2], "label": ["a", "b"]})
        export_marts_to_sql({"mart_labels": df}, project_root=tmp_path)
        content = (tmp_path / "marts" / "mart_labels.sql").read_text()
        assert "CREATE TABLE IF NOT EXISTS mart_labels" in content
        assert "INSERT INTO mart_labels" in content

    def test_empty_dataframe_skipped(self, tmp_path):
        paths = export_marts_to_sql({"empty": pd.DataFrame()}, project_root=tmp_path)
        assert "empty" not in paths

    def test_multiple_marts(self, tmp_path):
        marts = {
            "mart_a": pd.DataFrame({"x": [1, 2]}),
            "mart_b": pd.DataFrame({"y": [3, 4]}),
        }
        paths = export_marts_to_sql(marts, project_root=tmp_path)
        assert len(paths) == 2
