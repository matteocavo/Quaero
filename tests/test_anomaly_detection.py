from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from kpi_engine.anomaly_detection import detect_anomalies


def test_detect_anomalies_writes_report_and_exports_flagged_rows(
    tmp_path: Path,
) -> None:
    dataframe = pd.DataFrame(
        {
            "release_year": [2023, 2024, 2025, 2026, 2027],
            "streams": [100, 110, 105, 115, 600],
            "track_count": [10, 11, 10, 12, 50],
            "provider": ["A", "A", "B", "B", "C"],
        }
    )

    result = detect_anomalies(
        dataframe=dataframe,
        source_name="Release Impact",
        project_root=tmp_path,
    )

    report = result["report"]
    flagged_rows = result["flagged_rows"]
    report_path = tmp_path / result["report_path"]

    assert report["source"] == "release_impact"
    assert report["anomaly_row_count"] == 1
    assert result["report_path"] == "metadata/anomaly_report_release_impact.json"
    assert report_path.exists()
    assert len(report["anomalies"]) == 2

    saved_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved_report == report

    assert flagged_rows.shape[0] == 1
    assert bool(flagged_rows.loc[0, "has_anomaly"]) is True
    assert flagged_rows.loc[0, "anomaly_columns"] == "streams,track_count"
    assert flagged_rows.loc[0, "provider"] == "C"

    streams_result = next(
        anomaly for anomaly in report["anomalies"] if anomaly["column"] == "streams"
    )
    assert streams_result["method"] == "iqr"
    assert streams_result["anomaly_count"] == 1
    assert streams_result["example_values"] == [600]
