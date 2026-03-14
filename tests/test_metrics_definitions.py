from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from kpi_engine.metrics_definitions import write_metrics_definitions


def test_write_metrics_definitions_derives_metrics_from_generated_marts(
    tmp_path: Path,
) -> None:
    marts_dir = tmp_path / "marts"
    marts_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "release_year": [2020, 2021],
            "record_count": [2, 3],
            "total_streams": [250, 400],
            "avg_streams": [125.0, 133.3],
        }
    ).to_parquet(marts_dir / "mart_release_impact.parquet", index=False)

    pd.DataFrame(
        {
            "provider": ["A", "B"],
            "record_count": [2, 3],
            "total_streams": [250, 400],
            "avg_streams": [125.0, 133.3],
        }
    ).to_parquet(marts_dir / "mart_provider_performance.parquet", index=False)

    result = write_metrics_definitions(
        mart_names=["mart_release_impact", "mart_provider_performance"],
        project_root=tmp_path,
    )

    output_path = tmp_path / "metadata" / "metrics_definitions.json"

    assert output_path.exists()
    assert result["output_path"] == "metadata/metrics_definitions.json"

    saved = json.loads(output_path.read_text(encoding="utf-8"))
    assert saved == {"metrics": result["metrics"]}
    assert len(result["metrics"]) == 6
    assert {metric["source_mart"] for metric in result["metrics"]} == {
        "mart_release_impact",
        "mart_provider_performance",
    }

    average_streams_metric = next(
        metric
        for metric in result["metrics"]
        if metric["source_mart"] == "mart_release_impact"
        and metric["source_column"] == "avg_streams"
    )
    assert average_streams_metric["source_metric_column"] == "streams"
    assert average_streams_metric["default_aggregation"] == "average"
    assert average_streams_metric["formula_description"] == (
        "Average of `streams` in `mart_release_impact`."
    )


def test_write_metrics_definitions_includes_climate_trend_and_correlation_context(
    tmp_path: Path,
) -> None:
    marts_dir = tmp_path / "marts"
    marts_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        {
            "year": [2020, 2021],
            "record_count": [1, 1],
            "avg_temperature_anomaly": [0.8, 0.9],
        }
    ).to_parquet(marts_dir / "mart_temperature_anomaly_trend.parquet", index=False)

    pd.DataFrame(
        {
            "left_metric": ["co2"],
            "right_metric": ["temperature_anomaly"],
            "correlation": [0.81],
        }
    ).to_parquet(marts_dir / "mart_climate_correlation.parquet", index=False)

    result = write_metrics_definitions(
        mart_names=[
            "mart_temperature_anomaly_trend",
            "mart_climate_correlation",
        ],
        project_root=tmp_path,
    )

    temperature_metric = next(
        metric
        for metric in result["metrics"]
        if metric["source_mart"] == "mart_temperature_anomaly_trend"
        and metric["source_column"] == "avg_temperature_anomaly"
    )
    assert temperature_metric["unit"] == "celsius"
    assert "Berkeley Earth baseline" in temperature_metric["formula_description"]
    assert temperature_metric["default_aggregation"] == "average"

    correlation_metric = next(
        metric
        for metric in result["metrics"]
        if metric["source_mart"] == "mart_climate_correlation"
        and metric["source_column"] == "correlation"
    )
    assert correlation_metric["default_aggregation"] == "none"
    assert "climate metrics" in correlation_metric["formula_description"]
