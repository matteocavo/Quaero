"""Tests for the YAML metric definitions generator."""

from __future__ import annotations


import pandas as pd

from kpi_engine.metrics_yaml import write_metrics_yaml, _infer_agg_and_source


class TestInferAggAndSource:
    def test_total_prefix(self):
        agg, src = _infer_agg_and_source("total_revenue")
        assert agg == "sum"
        assert src == "revenue"

    def test_avg_prefix(self):
        agg, src = _infer_agg_and_source("avg_streams")
        assert agg == "average"
        assert src == "streams"

    def test_no_prefix_defaults_to_sum(self):
        agg, src = _infer_agg_and_source("revenue")
        assert agg == "sum"
        assert src == "revenue"


class TestWriteMetricsYaml:
    def test_file_created(self, tmp_path):
        df = pd.DataFrame({"region": ["IT"], "total_sales": [100.0]})
        write_metrics_yaml({"mart_sales": df}, project_root=tmp_path)
        out = tmp_path / "metrics" / "metrics.yml"
        assert out.exists()

    def test_yaml_contains_metric_name(self, tmp_path):
        df = pd.DataFrame({"avg_streams": [1.5, 2.5]})
        write_metrics_yaml({"mart_trend": df}, project_root=tmp_path)
        content = (tmp_path / "metrics" / "metrics.yml").read_text()
        assert "avg_streams" in content

    def test_skips_non_numeric_columns(self, tmp_path):
        df = pd.DataFrame({"region": ["IT", "DE"], "total_sales": [100.0, 200.0]})
        write_metrics_yaml({"mart_sales": df}, project_root=tmp_path)
        content = (tmp_path / "metrics" / "metrics.yml").read_text()
        # region is text — should not appear as a metric name: value pair
        assert "name: region" not in content
        assert "name: total_sales" in content

    def test_skips_record_count(self, tmp_path):
        df = pd.DataFrame({"record_count": [10, 20]})
        write_metrics_yaml({"mart_counts": df}, project_root=tmp_path)
        content = (tmp_path / "metrics" / "metrics.yml").read_text()
        assert "name: record_count" not in content

    def test_empty_mart_produces_empty_metrics(self, tmp_path):
        write_metrics_yaml({"empty": pd.DataFrame()}, project_root=tmp_path)
        content = (tmp_path / "metrics" / "metrics.yml").read_text()
        assert "- name:" not in content

    def test_yaml_version_header_present(self, tmp_path):
        df = pd.DataFrame({"total_x": [1.0]})
        write_metrics_yaml({"mart_x": df}, project_root=tmp_path)
        content = (tmp_path / "metrics" / "metrics.yml").read_text()
        assert "version: 1" in content
