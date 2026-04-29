from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from app.pipeline_runner import run_pipeline, run_project_from_config


def test_run_pipeline_executes_modules_in_sequence(tmp_path: Path) -> None:
    csv_path = tmp_path / "release_impact.csv"
    csv_path.write_text(
        (
            "release_year,provider,catalog,streams\n"
            "2024,A,Pop,100\n"
            "2024,B,Pop,150\n"
            "2025,A,Rock,200\n"
            "2026,C,Rock,600\n"
        ),
        encoding="utf-8",
    )

    result = run_pipeline(
        source_path=csv_path,
        source_name="Release Impact",
        question="Which release years generate the strongest average streams?",
        metric_column="streams",
        dimension_column="release_year",
        catalog_column="catalog",
        project_root=tmp_path,
    )

    assert result["ingestion"]["source"] == "release_impact"
    assert result["profiling"]["row_count"] == 4
    assert result["cleaning"]["source"] == "release_impact"
    assert "mart_question_answer" in result["marts"]
    assert "mart_top_entities" in result["marts"]
    assert "mart_distribution" in result["marts"]
    assert "mart_summary_stats" in result["marts"]
    assert "mart_time_trend" in result["marts"]
    assert "mart_release_impact" in result["marts"]
    assert result["kpi_suggestions"]["primary_kpi"] == "avg_streams"
    assert result["anomaly_detection"]["source"] == "release_impact"
    assert (
        result["lineage_html"]["output_path"]
        == "projects/release_impact/metadata/lineage.html"
    )
    assert (
        result["anomaly_framing_v2"]["output_path"]
        == "projects/release_impact/metadata/anomaly_report_v2_release_impact.json"
    )
    assert (
        tmp_path / "projects" / "release_impact" / "metadata" / "lineage.html"
    ).exists()
    assert (
        tmp_path
        / "projects"
        / "release_impact"
        / "metadata"
        / "anomaly_report_v2_release_impact.json"
    ).exists()
    assert "possible_kpis" in result["analytics_metadata"]
    assert "dashboard_suggestions" in result["dashboard_suggestions"]
    assert (
        result["dashboard_suggestions"]["question"]
        == "Which release years generate the strongest average streams?"
    )
    assert result["dashboard_suggestions"]["primary_mart"] == "mart_question_answer"
    assert result["ingestion"]["snapshot_path"].startswith(
        "projects/release_impact/raw/snapshot_"
    )
    assert (
        result["metrics_definitions"]["output_path"]
        == "projects/release_impact/metadata/metrics_definitions.json"
    )
    assert (
        result["profiling"]["output_path"]
        == "projects/release_impact/metadata/release_impact_profile.json"
    )
    assert (
        result["final_summary_report"]["summary"]["profile_path"]
        == "projects/release_impact/metadata/release_impact_profile.json"
    )
    assert (
        result["final_summary_report"]["summary"]["analytics_metadata_path"]
        == "projects/release_impact/metadata/analytics_metadata_release_impact.json"
    )
    assert (
        result["final_summary_report"]["summary"]["metrics_definitions_path"]
        == "projects/release_impact/metadata/metrics_definitions.json"
    )
    assert (
        result["final_summary_report"]["summary"]["dashboard_suggestions_path"]
        == "projects/release_impact/metadata/dashboard_suggestions_release_impact.json"
    )
    assert (
        result["final_summary_report"]["summary"]["lineage_html_path"]
        == "projects/release_impact/metadata/lineage.html"
    )
    assert (
        result["final_summary_report"]["summary"]["anomaly_report_v2_path"]
        == "projects/release_impact/metadata/anomaly_report_v2_release_impact.json"
    )
    assert result["cleaning"]["output_path"].startswith(
        "projects/release_impact/staging/staging_"
    )
    assert result["final_summary_report"]["summary"]["metric_column"] == "streams"
    assert result["final_summary_report"]["summary"]["metric_unit"] is None
    assert (
        result["final_summary_report"]["summary"]["metric_selection_mode"]
        == "user_provided"
    )
    assert result["final_summary_report"]["summary"]["metric_candidates"] == ["streams"]
    assert (
        result["final_summary_report"]["summary"]["dimension_column"] == "release_year"
    )
    assert (
        result["final_summary_report"]["summary"]["dimension_selection_mode"]
        == "user_provided"
    )
    assert result["final_summary_report"]["summary"]["dimension_candidates"] == [
        "release_year"
    ]
    assert result["final_summary_report"]["summary"]["aggregation_intent"] == {
        "aggregation": "average",
        "ordering": "highest",
    }
    assert (
        result["final_summary_report"]["summary"]["aggregation_selection_mode"]
        == "auto_inferred"
    )
    assert result["final_summary_report"]["path"] == (
        "projects/release_impact/metadata/final_summary_report.json"
    )
    assert (
        result["final_summary_report"]["summary"]["project_readme_path"]
        == "projects/release_impact/README.md"
    )
    assert result["project_readme"]["path"] == "projects/release_impact/README.md"
    project_readme = (tmp_path / result["project_readme"]["path"]).read_text(
        encoding="utf-8"
    )
    assert "## Question" in project_readme
    assert (
        "Which release years generate the strongest average streams?" in project_readme
    )
    assert "## Dataset Overview" in project_readme
    assert "## Dataset Statistics" in project_readme
    assert "## Dataset Description" in project_readme
    assert "## Metric Interpretation" in project_readme
    assert "## Column Meaning" in project_readme
    assert "## Analysis Results" in project_readme
    assert f"- Dataset source: `{csv_path}`" in project_readme
    assert "- Dataset name: `release_impact`" in project_readme
    assert "- Rows processed: `4`" in project_readme
    assert "- Number of columns: `4`" in project_readme
    assert "- Detected numeric columns: `release_year`, `streams`" in project_readme
    assert "- Detected categorical columns: `provider`, `catalog`" in project_readme
    assert "- Detected datetime columns: none" in project_readme
    assert (
        "| Column | Type | Null % | Unique Values | Example Values |" in project_readme
    )
    assert "| release_year | integer | 0% | 3 |" in project_readme
    assert "| provider | string | 0% | 3 |" in project_readme
    assert "| streams | integer | 0% | 4 |" in project_readme
    assert "- Total rows: `4`" in project_readme
    assert "- Total columns: `4`" in project_readme
    assert "- Numeric columns count: `2`" in project_readme
    assert "- Categorical columns count: `2`" in project_readme
    assert "- Datetime columns count: `0`" in project_readme
    assert "`streams`" in project_readme
    assert "`release_year`" in project_readme
    assert (
        "The analysis combines numeric measures such as `release_year`, `streams`"
        in project_readme
    )
    assert (
        "This analysis uses the `streams` column and the `average` aggregation"
        in project_readme
    )
    assert "- Metric column: `streams`" in project_readme
    assert "- Aggregation: `average`" in project_readme
    assert "- Unit: `unknown`" in project_readme
    assert (
        "| provider | Provider or platform associated with the record |"
        in project_readme
    )
    assert "mart_release_impact" in project_readme
    assert (
        "- Lineage graph: `projects/release_impact/metadata/lineage.html`"
        in project_readme
    )
    assert (
        "- Anomaly framing v2: `projects/release_impact/metadata/anomaly_report_v2_release_impact.json`"
        in project_readme
    )
    assert '--project-root "."' in project_readme
    assert "Analytical marts live under `marts/`" in project_readme
    assert "exports/bi" not in project_readme


def test_run_pipeline_auto_infers_metric_column_when_not_provided(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "trip_metrics.csv"
    csv_path.write_text(
        (
            "release_year,provider,trip_distance,fare_amount\n"
            "2024,A,1.5,10.0\n"
            "2024,B,2.5,12.0\n"
            "2025,A,3.0,13.0\n"
        ),
        encoding="utf-8",
    )

    result = run_pipeline(
        source_path=csv_path,
        source_name="Trip Metrics",
        question="Which providers generate the highest average trip distance?",
        project_root=tmp_path,
    )

    assert result["final_summary_report"]["summary"]["metric_column"] == "trip_distance"
    assert (
        result["final_summary_report"]["summary"]["metric_selection_mode"]
        == "auto_inferred"
    )
    assert result["final_summary_report"]["summary"]["dimension_column"] == "provider"
    assert (
        result["final_summary_report"]["summary"]["dimension_selection_mode"]
        == "auto_inferred"
    )
    assert result["final_summary_report"]["summary"]["metric_candidates"][:2] == [
        "trip_distance",
        "fare_amount",
    ]
    assert result["final_summary_report"]["summary"]["dimension_candidates"][:2] == [
        "provider",
        "release_year",
    ]
    assert result["final_summary_report"]["summary"]["aggregation_intent"] == {
        "aggregation": "average",
        "ordering": "highest",
    }


def test_run_pipeline_manual_metric_override_still_wins(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "trip_metrics.csv"
    csv_path.write_text(
        (
            "release_year,provider,trip_distance,fare_amount\n"
            "2024,A,1.5,10.0\n"
            "2024,B,2.5,12.0\n"
            "2025,A,3.0,13.0\n"
        ),
        encoding="utf-8",
    )

    result = run_pipeline(
        source_path=csv_path,
        source_name="Trip Metrics",
        question="Which providers generate the highest average trip distance?",
        metric_column="fare_amount",
        dimension_column="release_year",
        project_root=tmp_path,
    )

    assert result["final_summary_report"]["summary"]["metric_column"] == "fare_amount"
    assert (
        result["final_summary_report"]["summary"]["metric_selection_mode"]
        == "user_provided"
    )
    assert result["final_summary_report"]["summary"]["metric_candidates"] == [
        "fare_amount"
    ]
    assert (
        result["final_summary_report"]["summary"]["dimension_column"] == "release_year"
    )
    assert (
        result["final_summary_report"]["summary"]["dimension_selection_mode"]
        == "user_provided"
    )
    assert result["final_summary_report"]["summary"]["dimension_candidates"] == [
        "release_year"
    ]


def test_run_pipeline_raises_clear_error_for_ambiguous_metric_and_dimension(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "ambiguous.csv"
    csv_path.write_text(
        ("provider,region,x1,x2\nA,North,10,20\nB,South,15,25\nC,East,12,22\n"),
        encoding="utf-8",
    )

    try:
        run_pipeline(
            source_path=csv_path,
            source_name="Ambiguous",
            question="Which groups performed best overall?",
            project_root=tmp_path,
        )
    except ValueError as error:
        message = str(error)
    else:
        raise AssertionError("Expected the pipeline to raise for ambiguous inference.")

    assert "Top candidate metric columns" in message
    assert "Top candidate dimension columns" in message
    assert "x1" in message
    assert "provider" in message


def test_run_pipeline_builds_grouped_count_mart_from_inferred_dimension(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "companies.csv"
    csv_path.write_text(
        ("sector,company,founded\nTech,A,1999\nHealth,B,2005\nTech,C,2010\n"),
        encoding="utf-8",
    )

    result = run_pipeline(
        source_path=csv_path,
        source_name="Companies",
        question="Which sectors contain the highest number of companies?",
        project_root=tmp_path,
    )

    assert result["final_summary_report"]["summary"]["metric_column"] == "record_count"
    assert result["final_summary_report"]["summary"]["dimension_column"] == "sector"
    assert "mart_question_answer" in result["marts"]
    assert "mart_top_entities" in result["marts"]
    assert "mart_catalog_summary" in result["marts"]
    assert result["kpi_suggestions"]["primary_kpi"] == "record_count"
    skipped_names = {item["mart_name"] for item in result["skipped_marts"]}
    assert skipped_names == {
        "mart_distribution",
        "mart_summary_stats",
    }


def test_run_pipeline_reports_skipped_marts_for_virtual_record_count(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "titles.csv"
    csv_path.write_text(
        ("release_year,provider,title\n2023,A,X\n2024,B,Y\n2024,A,Z\n"),
        encoding="utf-8",
    )

    result = run_pipeline(
        source_path=csv_path,
        source_name="Titles",
        question="Which release years contain the most titles?",
        project_root=tmp_path,
    )

    skipped_marts = result["skipped_marts"]
    assert [item["mart_name"] for item in skipped_marts] == [
        "mart_release_impact",
        "mart_provider_performance",
        "mart_distribution",
        "mart_summary_stats",
    ]
    assert "record_count" in skipped_marts[0]["reason"]
    assert result["final_summary_report"]["summary"]["primary_kpi"] == "record_count"
    assert result["final_summary_report"]["summary"]["skipped_marts"] == skipped_marts
    project_readme = (tmp_path / result["project_readme"]["path"]).read_text(
        encoding="utf-8"
    )
    assert "## Reproducibility" in project_readme
    assert "--metric-column" not in project_readme


def test_project_readme_includes_manual_overrides_in_repro_command(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "manual.csv"
    csv_path.write_text(
        (
            "release_year,provider,trip_distance,fare_amount\n"
            "2024,A,1.5,10.0\n"
            "2024,B,2.5,12.0\n"
            "2025,A,3.0,13.0\n"
        ),
        encoding="utf-8",
    )

    result = run_pipeline(
        source_path=csv_path,
        source_name="Manual",
        question="Which providers generate the highest average trip distance?",
        metric_column="fare_amount",
        dimension_column="release_year",
        project_root=tmp_path,
    )

    project_readme = (tmp_path / result["project_readme"]["path"]).read_text(
        encoding="utf-8"
    )
    assert '--metric-column "fare_amount"' in project_readme
    assert '--dimension-column "release_year"' in project_readme
    assert '--project-root "."' in project_readme


def test_project_readme_profiles_datetime_columns_dynamically(tmp_path: Path) -> None:
    csv_path = tmp_path / "events.csv"
    csv_path.write_text(
        (
            "event_date,region,trip_distance\n"
            "2024-01-01,North,10.5\n"
            "2024-01-02,South,12.0\n"
            "2024-01-03,North,8.5\n"
        ),
        encoding="utf-8",
    )

    result = run_pipeline(
        source_path=csv_path,
        source_name="Events",
        question="Which event dates generate the highest average trip distance?",
        project_root=tmp_path,
    )

    project_readme = (tmp_path / result["project_readme"]["path"]).read_text(
        encoding="utf-8"
    )

    assert "- Dataset name: `events`" in project_readme
    assert "- Detected numeric columns: `trip_distance`" in project_readme
    assert "- Detected categorical columns: `region`" in project_readme
    assert "- Detected datetime columns: `event_date`" in project_readme
    assert "| event_date | datetime | 0% | 3 |" in project_readme
    assert "- Numeric columns count: `1`" in project_readme
    assert "- Categorical columns count: `1`" in project_readme
    assert "- Datetime columns count: `1`" in project_readme


def test_project_readme_explains_global_temperature_metric(tmp_path: Path) -> None:
    csv_path = tmp_path / "global_temperature.csv"
    csv_path.write_text(
        (
            "Source,Year,Mean\n"
            "GISTEMP,2023-01,1.10\n"
            "gcag,2023-02,1.20\n"
            "GISTEMP,2024-01,1.30\n"
        ),
        encoding="utf-8",
    )

    result = run_pipeline(
        source_path=csv_path,
        source_name="global_temperature",
        question="Which years show the highest average temperature?",
        project_root=tmp_path,
    )

    project_readme = (tmp_path / result["project_readme"]["path"]).read_text(
        encoding="utf-8"
    )

    assert "# Global Temperature Analysis" in project_readme
    assert "## Metric Interpretation" in project_readme
    assert "- Detected numeric columns: `Mean`" in project_readme
    assert "- Detected categorical columns: `Source`" in project_readme
    assert "- Detected datetime columns: `Year`" in project_readme
    assert "| Year | datetime | 0% |" in project_readme
    assert "- Metric column: `mean`" in project_readme
    assert "- Aggregation: `average`" in project_readme
    assert (
        "- Unit: `°C temperature anomaly relative to the baseline.`" in project_readme
    )
    assert "Positive values indicate warmer-than-baseline periods" in project_readme
    assert "## Column Meaning" in project_readme
    assert "| Year | Year and month of observation |" in project_readme
    assert "| Source | Temperature dataset source |" in project_readme
    assert "| Mean | Global temperature anomaly in °C |" in project_readme
    assert "## Dataset Description" in project_readme
    assert (
        "This dataset contains temperature observations across time." in project_readme
    )


def test_run_project_from_config_builds_integrated_dataset_and_climate_marts(
    tmp_path: Path,
) -> None:
    datasets = {
        "temperature": "year,mean\n2020,0.8\n2021,0.9\n2022,1.0\n",
        "co2": "year,co2\n2020,410\n2021,412\n2022,415\n",
        "sea_level": "year,sea_level\n2020,3.1\n2021,3.3\n2022,3.6\n",
        "ice_extent": "year,ice_extent\n2020,14.2\n2021,14.0\n2022,13.7\n",
    }
    for dataset_name, csv_text in datasets.items():
        (tmp_path / f"{dataset_name}.csv").write_text(csv_text, encoding="utf-8")

    config_path = tmp_path / "project_config.json"
    config_path.write_text(
        json.dumps(
            {
                "project": "climate_change_analysis",
                "question": "How do climate indicators change over time?",
                "datasets": [
                    {
                        "name": dataset_name,
                        "path": str(tmp_path / f"{dataset_name}.csv"),
                        "time_column": "year",
                    }
                    for dataset_name in datasets
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    result = run_project_from_config(config_path=config_path, project_root=tmp_path)

    assert result["project_mode"] == "multi_dataset"
    assert result["integrated_dataset"]["output_path"] == (
        "projects/climate_change_analysis/integrated/master_dataset.parquet"
    )
    assert (tmp_path / result["integrated_dataset"]["output_path"]).exists()
    assert {dataset["name"] for dataset in result["datasets"]} == set(datasets)
    for dataset_name in datasets:
        assert (
            tmp_path
            / "projects"
            / "climate_change_analysis"
            / "raw"
            / f"{dataset_name}.parquet"
        ).exists()
    expected_marts = {
        "mart_metric_correlation",
        "mart_temperature_trend",
        "mart_temperature_summary_stats",
        "mart_co2_trend",
        "mart_co2_summary_stats",
        "mart_sea_level_trend",
        "mart_sea_level_summary_stats",
        "mart_ice_extent_trend",
        "mart_ice_extent_summary_stats",
        "mart_climate_correlation",
    }
    assert expected_marts.issubset(result["marts"])
    assert result["final_summary_report"]["summary"]["integrated_dataset_path"] == (
        "projects/climate_change_analysis/integrated/master_dataset.parquet"
    )
    assert result["final_summary_report"]["summary"]["question"] == (
        "How do climate indicators change over time?"
    )
    assert result["final_summary_report"]["summary"]["primary_mart"] == (
        "mart_temperature_trend"
    )
    assert result["final_summary_report"]["summary"]["primary_kpi"] == (
        "avg_temperature"
    )
    coverage_by_dataset = {
        dataset["name"]: dataset["coverage"]
        for dataset in result["final_summary_report"]["summary"]["datasets"]
    }
    assert coverage_by_dataset == {
        "temperature": {"min_year": 2020, "max_year": 2022},
        "co2": {"min_year": 2020, "max_year": 2022},
        "sea_level": {"min_year": 2020, "max_year": 2022},
        "ice_extent": {"min_year": 2020, "max_year": 2022},
    }
    project_readme = (tmp_path / result["project_readme"]["path"]).read_text(
        encoding="utf-8"
    )
    assert "# Climate Change Analysis" in project_readme
    assert "Config-driven project with `4` datasets" in project_readme
    assert "## Question" in project_readme
    assert "How do climate indicators change over time?" in project_readme
    assert "The question emphasizes change over time" in project_readme
    assert "## Data Coverage" in project_readme
    assert "| temperature | 2020 | 2022 |" in project_readme
    assert (
        "standardized into `year`, `source_metric_value`, and `dataset_name`"
        in project_readme
    )
    assert "master_dataset.parquet" in project_readme
    integrated_dataframe = pd.read_parquet(
        tmp_path / result["integrated_dataset"]["output_path"]
    )
    assert list(integrated_dataframe.columns) == [
        "year",
        "co2",
        "ice_extent",
        "sea_level",
        "temperature",
    ]
    assert integrated_dataframe["year"].tolist() == ["2020", "2021", "2022"]


def test_run_project_from_config_builds_generic_marts_for_non_climate_multi_dataset_project(
    tmp_path: Path,
) -> None:
    datasets = {
        "revenue": "year,revenue\n2020,100\n2021,120\n2022,140\n",
        "cost": "year,cost\n2020,60\n2021,70\n2022,85\n",
    }
    for dataset_name, csv_text in datasets.items():
        (tmp_path / f"{dataset_name}.csv").write_text(csv_text, encoding="utf-8")

    config_path = tmp_path / "project_config.json"
    config_path.write_text(
        json.dumps(
            {
                "project": "finance_overview",
                "question": "How do revenue and cost change over time?",
                "datasets": [
                    {
                        "name": dataset_name,
                        "path": str(tmp_path / f"{dataset_name}.csv"),
                        "time_column": "year",
                    }
                    for dataset_name in datasets
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    result = run_project_from_config(config_path=config_path, project_root=tmp_path)

    assert result["project_mode"] == "multi_dataset"
    assert result["integrated_dataset"]["output_path"] == (
        "projects/finance_overview/integrated/master_dataset.parquet"
    )
    assert {
        "mart_revenue_trend",
        "mart_revenue_summary_stats",
        "mart_cost_trend",
        "mart_cost_summary_stats",
        "mart_metric_correlation",
    }.issubset(result["marts"])
    assert "mart_climate_correlation" not in result["marts"]
    assert result["final_summary_report"]["summary"]["primary_mart"] == (
        "mart_revenue_trend"
    )
    assert result["final_summary_report"]["summary"]["primary_kpi"] == ("avg_revenue")
    integrated_dataframe = pd.read_parquet(
        tmp_path / result["integrated_dataset"]["output_path"]
    )
    assert list(integrated_dataframe.columns) == ["year", "cost", "revenue"]
    project_readme = (tmp_path / result["project_readme"]["path"]).read_text(
        encoding="utf-8"
    )
    assert "Config-driven project with `2` datasets" in project_readme
    assert "mart_revenue_trend" in project_readme
    assert "mart_cost_summary_stats" in project_readme
    assert "| revenue | 2020 | 2022 |" in project_readme
    dashboard_suggestions = result["dashboard_suggestions"]
    assert (
        dashboard_suggestions["question"] == "How do revenue and cost change over time?"
    )
    assert dashboard_suggestions["primary_mart"] == "mart_revenue_trend"


def test_run_project_from_config_reports_coverage_for_date_like_time_columns(
    tmp_path: Path,
) -> None:
    temperature_csv = (
        "date,temperature_anomaly\n2024-01,0.80\n2024-02,0.90\n2025-01,1.10\n"
    )
    co2_csv = "date,co2\n2024-01,410\n2024-02,412\n2025-01,415\n"
    (tmp_path / "temperature_anomaly.csv").write_text(temperature_csv, encoding="utf-8")
    (tmp_path / "co2.csv").write_text(co2_csv, encoding="utf-8")

    config_path = tmp_path / "project_config.json"
    config_path.write_text(
        json.dumps(
            {
                "project": "monthly_climate",
                "question": "How has temperature anomaly changed over time?",
                "datasets": [
                    {
                        "name": "temperature_anomaly",
                        "path": str(tmp_path / "temperature_anomaly.csv"),
                        "time_column": "date",
                    },
                    {
                        "name": "co2",
                        "path": str(tmp_path / "co2.csv"),
                        "time_column": "date",
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    result = run_project_from_config(config_path=config_path, project_root=tmp_path)

    coverage_by_dataset = {
        dataset["name"]: dataset["coverage"]
        for dataset in result["final_summary_report"]["summary"]["datasets"]
    }
    assert coverage_by_dataset == {
        "temperature_anomaly": {"min_year": 2024, "max_year": 2025},
        "co2": {"min_year": 2024, "max_year": 2025},
    }
    project_readme = (tmp_path / result["project_readme"]["path"]).read_text(
        encoding="utf-8"
    )
    assert "| temperature_anomaly | 2024 | 2025 |" in project_readme
    assert "| co2 | 2024 | 2025 |" in project_readme


def test_run_project_from_config_readme_explains_later_starting_datasets(
    tmp_path: Path,
) -> None:
    revenue_csv = "year,revenue\n2020,100\n2021,120\n2022,140\n"
    cost_csv = "year,cost\n2021,60\n2022,70\n"
    (tmp_path / "revenue.csv").write_text(revenue_csv, encoding="utf-8")
    (tmp_path / "cost.csv").write_text(cost_csv, encoding="utf-8")

    config_path = tmp_path / "project_config.json"
    config_path.write_text(
        json.dumps(
            {
                "project": "staggered_finance",
                "question": "How do revenue and cost change over time?",
                "datasets": [
                    {
                        "name": "revenue",
                        "path": str(tmp_path / "revenue.csv"),
                        "time_column": "year",
                    },
                    {
                        "name": "cost",
                        "path": str(tmp_path / "cost.csv"),
                        "time_column": "year",
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    result = run_project_from_config(config_path=config_path, project_root=tmp_path)

    cost_trend = pd.read_parquet(tmp_path / result["marts"]["mart_cost_trend"])
    assert cost_trend["year"].tolist() == ["2021", "2022"]
    project_readme = (tmp_path / result["project_readme"]["path"]).read_text(
        encoding="utf-8"
    )
    assert "Coverage notes:" in project_readme
    assert (
        "- The integrated dataset keeps the union of available years across sources, so the project timeline begins at `2020`."
        in project_readme
    )
    assert (
        "- `cost` has no source data before `2021`, so `cost` marts begin at `2021`."
        in project_readme
    )


def test_run_project_from_config_cleans_obsolete_project_artifacts(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "sales.csv"
    csv_path.write_text(
        "year,revenue\n2023,100\n2024,120\n",
        encoding="utf-8",
    )

    project_dir = tmp_path / "projects" / "finance_cleanup"
    (project_dir / "raw").mkdir(parents=True)
    (project_dir / "staging").mkdir()
    (project_dir / "integrated").mkdir()
    (project_dir / "marts").mkdir()
    (project_dir / "metadata").mkdir()

    stale_paths = [
        project_dir / "raw" / "old_metric.parquet",
        project_dir / "staging" / "staging_old.parquet",
        project_dir / "integrated" / "old_master.parquet",
        project_dir / "marts" / "mart_old_metric_trend.parquet",
        project_dir / "metadata" / "old_metric_profile.json",
    ]
    for stale_path in stale_paths:
        stale_path.write_text("stale", encoding="utf-8")
    stale_summary_path = project_dir / "metadata" / "final_summary_report.json"
    stale_summary_path.write_text("stale", encoding="utf-8")

    manifest_path = project_dir / "metadata" / "run_manifest_keep.json"
    manifest_path.write_text("{}", encoding="utf-8")

    config_path = project_dir / "project_config.json"
    config_path.write_text(
        json.dumps(
            {
                "project": "finance_cleanup",
                "question": "How does revenue change over time?",
                "datasets": [
                    {
                        "name": "revenue",
                        "path": str(csv_path),
                        "time_column": "year",
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    result = run_project_from_config(config_path=config_path, project_root=tmp_path)

    for stale_path in stale_paths:
        assert not stale_path.exists()
    assert manifest_path.exists()
    assert (tmp_path / result["marts"]["mart_time_trend"]).exists()
    refreshed_summary_path = (
        tmp_path
        / "projects"
        / "finance_cleanup"
        / "metadata"
        / "final_summary_report.json"
    )
    assert refreshed_summary_path.exists()
    assert refreshed_summary_path.read_text(encoding="utf-8") != "stale"


def test_run_project_from_config_keeps_single_dataset_projects_off_integrated_layer(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "sales.csv"
    csv_path.write_text(
        ("release_year,provider,streams\n2023,A,100\n2024,B,150\n2025,A,175\n"),
        encoding="utf-8",
    )

    config_path = tmp_path / "project_config.json"
    config_path.write_text(
        json.dumps(
            {
                "project": "single_dataset_project",
                "question": "Which providers generate the highest average streams?",
                "datasets": [
                    {
                        "name": "sales",
                        "path": str(csv_path),
                        "time_column": "release_year",
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    result = run_project_from_config(config_path=config_path, project_root=tmp_path)

    assert result["project_mode"] == "single_dataset"
    assert "integrated_dataset" not in result
    assert "mart_question_answer" in result["marts"]
    assert "mart_top_entities" in result["marts"]
    assert not (
        tmp_path / "projects" / "single_dataset_project" / "integrated"
    ).exists()
    assert (
        result["final_summary_report"]["summary"]["config_path"]
        == "project_config.json"
    )
    assert result["final_summary_report"]["summary"]["primary_mart"] == (
        "mart_question_answer"
    )
    assert result["final_summary_report"]["summary"]["cleaned_data_path"].startswith(
        "projects/single_dataset_project/staging/staging_"
    )
    project_readme = (tmp_path / result["project_readme"]["path"]).read_text(
        encoding="utf-8"
    )
    assert (
        "does not create `integrated/` because only one dataset is configured"
        in project_readme
    )
    assert '--config "project_config.json"' in project_readme
