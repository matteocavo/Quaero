"""Tests for the Anomaly Framing v2 module."""

from __future__ import annotations

import json

from pipelines.anomaly_framing import (
    frame_anomalies,
    _classify_impact,
    _suggest_action,
    _build_business_summary,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_v1_report(
    tmp_path, anomalies=None, anomaly_row_count=0, source="test_source"
):
    metadata_dir = tmp_path / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "source": source,
        "anomaly_row_count": anomaly_row_count,
        "anomalies": anomalies or [],
    }
    (metadata_dir / f"anomaly_report_{source}.json").write_text(
        json.dumps(report), encoding="utf-8"
    )
    return report


def _sample_anomaly(column="price", count=5, method="iqr"):
    return {
        "column": column,
        "method": method,
        "threshold": "1.0 to 9.0",
        "anomaly_count": count,
        "example_values": [15.0, 0.1],
    }


# ---------------------------------------------------------------------------
# Unit tests — _classify_impact
# ---------------------------------------------------------------------------


class TestClassifyImpact:
    def test_low_below_threshold(self):
        assert _classify_impact(0.005) == "low"

    def test_low_at_zero(self):
        assert _classify_impact(0.0) == "low"

    def test_medium_at_threshold(self):
        assert _classify_impact(0.02) == "medium"

    def test_medium_in_range(self):
        assert _classify_impact(0.07) == "medium"

    def test_high_at_threshold(self):
        assert _classify_impact(0.10) == "high"

    def test_high_above_threshold(self):
        assert _classify_impact(0.50) == "high"


# ---------------------------------------------------------------------------
# Unit tests — _suggest_action
# ---------------------------------------------------------------------------


class TestSuggestAction:
    def test_high_impact_mentions_column(self):
        result = _suggest_action("revenue", impact="high", method="iqr")
        assert "revenue" in result

    def test_medium_impact_mentions_method(self):
        result = _suggest_action("price", impact="medium", method="zscore")
        assert "zscore" in result

    def test_low_impact_mentions_low_frequency(self):
        result = _suggest_action("qty", impact="low", method="iqr")
        assert "low" in result.lower() or "isolated" in result.lower()


# ---------------------------------------------------------------------------
# Unit tests — _build_business_summary
# ---------------------------------------------------------------------------


class TestBuildBusinessSummary:
    def test_no_anomalies_returns_clean_message(self):
        result = _build_business_summary(0, 100, [])
        assert "No anomalies" in result

    def test_high_impact_summary_includes_column(self):
        enriched = [{"column": "price", "impact": "high", "frequency": 0.15}]
        result = _build_business_summary(15, 100, enriched)
        assert "price" in result
        assert "high" in result

    def test_medium_impact_summary(self):
        enriched = [{"column": "qty", "impact": "medium", "frequency": 0.05}]
        result = _build_business_summary(5, 100, enriched)
        assert "qty" in result

    def test_low_impact_summary(self):
        enriched = [{"column": "weight", "impact": "low", "frequency": 0.01}]
        result = _build_business_summary(1, 100, enriched)
        assert "low" in result.lower() or "no immediate" in result.lower()


# ---------------------------------------------------------------------------
# Integration tests — frame_anomalies (file I/O)
# ---------------------------------------------------------------------------


class TestFrameAnomalies:
    def test_output_file_created(self, tmp_path):
        _write_v1_report(tmp_path, anomaly_row_count=5)
        frame_anomalies("test_source", total_row_count=100, project_root=tmp_path)
        assert (tmp_path / "metadata" / "anomaly_report_v2_test_source.json").exists()

    def test_returns_output_path(self, tmp_path):
        _write_v1_report(tmp_path)
        result = frame_anomalies(
            "test_source", total_row_count=100, project_root=tmp_path
        )
        assert result["output_path"] == "metadata/anomaly_report_v2_test_source.json"

    def test_enriched_fields_present(self, tmp_path):
        _write_v1_report(
            tmp_path, anomalies=[_sample_anomaly(count=3)], anomaly_row_count=3
        )
        result = frame_anomalies(
            "test_source", total_row_count=100, project_root=tmp_path
        )
        entry = result["report"]["anomalies"][0]
        assert "frequency" in entry
        assert "impact" in entry
        assert "suggested_action" in entry

    def test_frequency_computed_correctly(self, tmp_path):
        _write_v1_report(
            tmp_path, anomalies=[_sample_anomaly(count=10)], anomaly_row_count=10
        )
        result = frame_anomalies(
            "test_source", total_row_count=100, project_root=tmp_path
        )
        assert result["report"]["anomalies"][0]["frequency"] == 0.10

    def test_business_summary_present(self, tmp_path):
        _write_v1_report(tmp_path, anomaly_row_count=0)
        result = frame_anomalies(
            "test_source", total_row_count=50, project_root=tmp_path
        )
        assert "business_summary" in result["report"]

    def test_no_anomalies_produces_clean_summary(self, tmp_path):
        _write_v1_report(tmp_path, anomalies=[], anomaly_row_count=0)
        result = frame_anomalies(
            "test_source", total_row_count=100, project_root=tmp_path
        )
        assert "No anomalies" in result["report"]["business_summary"]

    def test_skips_when_v1_report_absent(self, tmp_path):
        (tmp_path / "metadata").mkdir(parents=True, exist_ok=True)
        result = frame_anomalies(
            "test_source", total_row_count=100, project_root=tmp_path
        )
        assert result.get("skipped") is True
        assert result["report"] is None

    def test_original_fields_preserved(self, tmp_path):
        _write_v1_report(
            tmp_path,
            anomalies=[_sample_anomaly(column="revenue", count=2)],
            anomaly_row_count=2,
        )
        result = frame_anomalies(
            "test_source", total_row_count=200, project_root=tmp_path
        )
        entry = result["report"]["anomalies"][0]
        assert entry["column"] == "revenue"
        assert entry["anomaly_count"] == 2
        assert entry["method"] == "iqr"
