"""Tests for the data lineage tracker."""

from __future__ import annotations

import json

import pandas as pd
import pytest

from pipelines.lineage import build_lineage


def _make_cleaning_log(decisions=None):
    return {
        "source": "test",
        "run_id": "abc12345",
        "run_timestamp": "2024-01-01T00:00:00+00:00",
        "total_decisions": len(decisions or []),
        "decisions": decisions or [],
    }


class TestBuildLineage:
    def test_output_keys(self, tmp_path):
        staging = pd.DataFrame({"revenue": [1.0, 2.0], "region": ["IT", "DE"]})
        mart = pd.DataFrame({"region": ["IT"], "total_revenue": [1.0]})
        result = build_lineage(
            raw_columns=["revenue", "region"],
            staging_dataframe=staging,
            marts={"mart_a": mart},
            cleaning_log=_make_cleaning_log(),
            source_name="test",
            project_root=tmp_path,
        )
        assert "nodes" in result
        assert "edges" in result
        assert "node_count" in result
        assert "edge_count" in result

    def test_file_written(self, tmp_path):
        staging = pd.DataFrame({"val": [1, 2]})
        build_lineage(
            raw_columns=["val"],
            staging_dataframe=staging,
            marts={},
            cleaning_log=_make_cleaning_log(),
            source_name="ds",
            project_root=tmp_path,
        )
        out = tmp_path / "metadata" / "lineage.json"
        assert out.exists()
        data = json.loads(out.read_text())
        assert "nodes" in data

    def test_raw_nodes_created(self, tmp_path):
        staging = pd.DataFrame({"a": [1]})
        result = build_lineage(
            raw_columns=["a", "b"],
            staging_dataframe=staging,
            marts={},
            cleaning_log=_make_cleaning_log(),
            source_name="t",
            project_root=tmp_path,
        )
        raw_node_ids = [n["id"] for n in result["nodes"] if n["layer"] == "raw"]
        assert "raw::a" in raw_node_ids
        assert "raw::b" in raw_node_ids

    def test_passthrough_edge_created(self, tmp_path):
        staging = pd.DataFrame({"revenue": [1.0]})
        result = build_lineage(
            raw_columns=["revenue"],
            staging_dataframe=staging,
            marts={},
            cleaning_log=_make_cleaning_log(),
            source_name="t",
            project_root=tmp_path,
        )
        edges = result["edges"]
        assert any(
            e["from"] == "raw::revenue" and e["to"] == "staging::revenue"
            for e in edges
        )

    def test_mart_nodes_created(self, tmp_path):
        staging = pd.DataFrame({"streams": [1.0, 2.0]})
        mart = pd.DataFrame({"avg_streams": [1.5]})
        result = build_lineage(
            raw_columns=["streams"],
            staging_dataframe=staging,
            marts={"mart_trend": mart},
            cleaning_log=_make_cleaning_log(),
            source_name="t",
            project_root=tmp_path,
        )
        mart_node_ids = [n["id"] for n in result["nodes"] if n["layer"] == "mart"]
        assert "mart::mart_trend::avg_streams" in mart_node_ids

    def test_quality_flag_columns_excluded_from_staging(self, tmp_path):
        staging = pd.DataFrame({
            "revenue": [1.0],
            "has_nulls": [False],
            "invalid_date_parse": [False],
            "type_conversion_issue": [False],
        })
        result = build_lineage(
            raw_columns=["revenue"],
            staging_dataframe=staging,
            marts={},
            cleaning_log=_make_cleaning_log(),
            source_name="t",
            project_root=tmp_path,
        )
        staging_node_names = [n["name"] for n in result["nodes"] if n["layer"] == "staging"]
        assert "has_nulls" not in staging_node_names
        assert "revenue" in staging_node_names
