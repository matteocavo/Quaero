from __future__ import annotations

import json
from pathlib import Path

from app.pipeline_runner import run_pipeline


def test_end_to_end_pipeline_with_sample_dataset(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[1]
    csv_path = project_root / "sample_data" / "release_impact_sample.csv"

    result = run_pipeline(
        source_path=csv_path,
        source_name="Release Impact Sample",
        question="Which release years generate the strongest average streams?",
        catalog_column="catalog",
        project_root=tmp_path,
    )

    summary_path = (
        tmp_path
        / "projects"
        / "release_impact_sample"
        / "metadata"
        / "final_summary_report.json"
    )
    analytics_metadata_path = (
        tmp_path
        / "projects"
        / "release_impact_sample"
        / "metadata"
        / "analytics_metadata_release_impact_sample.json"
    )
    dashboard_suggestions_path = (
        tmp_path
        / "projects"
        / "release_impact_sample"
        / "metadata"
        / "dashboard_suggestions_release_impact_sample.json"
    )
    metrics_definitions_path = (
        tmp_path
        / "projects"
        / "release_impact_sample"
        / "metadata"
        / "metrics_definitions.json"
    )
    project_readme_path = tmp_path / "projects" / "release_impact_sample" / "README.md"

    assert summary_path.exists()
    assert analytics_metadata_path.exists()
    assert dashboard_suggestions_path.exists()
    assert metrics_definitions_path.exists()
    assert project_readme_path.exists()
    assert (tmp_path / result["cleaning"]["output_path"]).exists()

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["source"] == "release_impact_sample"
    assert summary["primary_kpi"] == "avg_streams"
    assert summary["metric_column"] == "streams"
    assert summary["metric_unit"] is None
    assert summary["metric_selection_mode"] == "auto_inferred"
    assert summary["metric_candidates"][0] == "streams"
    assert summary["dimension_column"] == "release_year"
    assert summary["dimension_selection_mode"] == "auto_inferred"
    assert summary["dimension_candidates"][0] == "release_year"
    assert summary["aggregation_intent"] == {
        "aggregation": "average",
        "ordering": "highest",
    }
    assert summary["aggregation_selection_mode"] == "auto_inferred"
    assert (
        summary["profile_path"]
        == "projects/release_impact_sample/metadata/release_impact_sample_profile.json"
    )
    assert summary["analytics_metadata_path"] == (
        "projects/release_impact_sample/metadata/"
        "analytics_metadata_release_impact_sample.json"
    )
    assert (
        summary["metrics_definitions_path"]
        == "projects/release_impact_sample/metadata/metrics_definitions.json"
    )
    assert summary["dashboard_suggestions_path"] == (
        "projects/release_impact_sample/metadata/"
        "dashboard_suggestions_release_impact_sample.json"
    )
    assert summary["project_readme_path"] == "projects/release_impact_sample/README.md"

    project_readme = project_readme_path.read_text(encoding="utf-8")
    assert "# Release Impact Sample Analysis" in project_readme
    assert "## Dataset Overview" in project_readme
    assert "## Dataset Statistics" in project_readme
    assert "## Dataset Description" in project_readme
    assert "## Metric Interpretation" in project_readme
    assert "## Column Meaning" in project_readme
    assert "## Analysis Results" in project_readme
    assert f"- Dataset source: `{csv_path}`" in project_readme
    assert "- Dataset name: `release_impact_sample`" in project_readme
    assert "- Rows processed: `5`" in project_readme
    assert "- Number of columns: `4`" in project_readme
    assert (
        "| Column | Type | Null % | Unique Values | Example Values |" in project_readme
    )
    assert "| release_year | integer | 0% | 5 |" in project_readme
    assert "| streams | integer | 0% | 5 |" in project_readme
    assert (
        "| provider | Provider or platform associated with the record |"
        in project_readme
    )
    assert "metric_value" not in project_readme
    assert "exports/bi" not in project_readme
