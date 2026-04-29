"""Tests for the Power BI schema export utilities."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from pipelines.pbi_export import export_pbi_schema


class TestExportPbiSchema:
    def test_output_keys_present(self, tmp_path):
        df = pd.DataFrame({"region": ["IT", "DE"], "sales": [100.0, 200.0]})
        result = export_pbi_schema({"mart_sales": df}, project_root=tmp_path)
        assert "tables" in result
        assert "suggested_relationships" in result
        assert "suggested_dax_measures" in result
        assert "output_path" in result

    def test_file_written(self, tmp_path):
        df = pd.DataFrame({"a": [1, 2]})
        export_pbi_schema({"mart_a": df}, project_root=tmp_path)
        out = tmp_path / "metadata" / "pbi_schema.json"
        assert out.exists()
        data = json.loads(out.read_text())
        assert "tables" in data

    def test_dax_measures_for_numeric_columns(self, tmp_path):
        df = pd.DataFrame({"region": ["IT"], "revenue": [500.0]})
        result = export_pbi_schema({"mart_rev": df}, project_root=tmp_path)
        measure_names = [m["measure_name"] for m in result["suggested_dax_measures"]]
        assert "Total revenue" in measure_names
        assert "Avg revenue" in measure_names

    def test_no_measures_for_text_columns(self, tmp_path):
        df = pd.DataFrame({"label": ["a", "b"]})
        result = export_pbi_schema({"mart_text": df}, project_root=tmp_path)
        assert result["suggested_dax_measures"] == []

    def test_relationship_suggested_for_shared_column(self, tmp_path):
        df1 = pd.DataFrame({"region": ["IT"], "sales": [100.0]})
        df2 = pd.DataFrame({"region": ["IT"], "costs": [50.0]})
        result = export_pbi_schema({"mart_a": df1, "mart_b": df2}, project_root=tmp_path)
        rel_cols = [r["from_column"] for r in result["suggested_relationships"]]
        assert "region" in rel_cols

    def test_empty_mart_skipped(self, tmp_path):
        result = export_pbi_schema({"empty": pd.DataFrame()}, project_root=tmp_path)
        assert result["tables"] == []
