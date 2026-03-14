from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipelines.mart_builder import (
    build_catalog_summary_mart,
    build_climate_correlation_mart,
    build_distribution_mart,
    build_integrated_correlation_mart,
    build_integrated_trend_mart,
    build_provider_performance_mart,
    build_question_answer_mart,
    build_release_impact_mart,
    build_summary_stats_mart,
    build_time_trend_mart,
    build_top_entities_mart,
    find_integrated_metric_columns,
    find_time_column,
)


def test_build_marts_writes_expected_outputs(tmp_path: Path) -> None:
    dataframe = pd.DataFrame(
        {
            "release_year": [2024, 2024, 2025],
            "provider": ["A", "B", "A"],
            "catalog": ["Pop", "Pop", "Rock"],
            "streams": [100, 150, 200],
        }
    )

    release_result = build_release_impact_mart(
        dataframe=dataframe,
        metric_column="streams",
        project_root=tmp_path,
    )
    provider_result = build_provider_performance_mart(
        dataframe=dataframe,
        metric_column="streams",
        project_root=tmp_path,
    )
    catalog_result = build_catalog_summary_mart(
        dataframe=dataframe,
        catalog_column="catalog",
        metric_column="streams",
        project_root=tmp_path,
    )
    question_result = build_question_answer_mart(
        dataframe=dataframe,
        dimension_column="release_year",
        metric_column="streams",
        aggregation="average",
        project_root=tmp_path,
    )
    top_entities_result = build_top_entities_mart(
        dataframe=dataframe,
        dimension_column="provider",
        metric_column="streams",
        aggregation="sum",
        project_root=tmp_path,
    )
    distribution_result = build_distribution_mart(
        dataframe=dataframe,
        metric_column="streams",
        project_root=tmp_path,
    )
    summary_stats_result = build_summary_stats_mart(
        dataframe=dataframe,
        metric_column="streams",
        project_root=tmp_path,
    )
    time_trend_result = build_time_trend_mart(
        dataframe=dataframe,
        metric_column="streams",
        aggregation="sum",
        time_column="release_year",
        project_root=tmp_path,
    )

    release_mart = release_result["dataframe"]
    provider_mart = provider_result["dataframe"]
    catalog_mart = catalog_result["dataframe"]
    question_mart = question_result["dataframe"]
    top_entities_mart = top_entities_result["dataframe"]
    distribution_mart = distribution_result["dataframe"]
    summary_stats_mart = summary_stats_result["dataframe"]
    time_trend_mart = time_trend_result["dataframe"]

    assert release_result["output_path"] == "marts/mart_release_impact.parquet"
    assert provider_result["output_path"] == "marts/mart_provider_performance.parquet"
    assert catalog_result["output_path"] == "marts/mart_catalog_summary.parquet"
    assert question_result["output_path"] == "marts/mart_question_answer.parquet"
    assert top_entities_result["output_path"] == "marts/mart_top_entities.parquet"
    assert distribution_result["output_path"] == "marts/mart_distribution.parquet"
    assert summary_stats_result["output_path"] == "marts/mart_summary_stats.parquet"
    assert time_trend_result["output_path"] == "marts/mart_time_trend.parquet"

    assert (tmp_path / release_result["output_path"]).exists()
    assert (tmp_path / provider_result["output_path"]).exists()
    assert (tmp_path / catalog_result["output_path"]).exists()
    assert (tmp_path / question_result["output_path"]).exists()
    assert (tmp_path / top_entities_result["output_path"]).exists()
    assert (tmp_path / distribution_result["output_path"]).exists()
    assert (tmp_path / summary_stats_result["output_path"]).exists()
    assert (tmp_path / time_trend_result["output_path"]).exists()

    assert release_mart.to_dict(orient="records") == [
        {
            "release_year": 2024,
            "record_count": 2,
            "total_streams": 250,
            "avg_streams": 125.0,
        },
        {
            "release_year": 2025,
            "record_count": 1,
            "total_streams": 200,
            "avg_streams": 200.0,
        },
    ]
    assert provider_mart.to_dict(orient="records") == [
        {
            "provider": "A",
            "record_count": 2,
            "total_streams": 300,
            "avg_streams": 150.0,
        },
        {
            "provider": "B",
            "record_count": 1,
            "total_streams": 150,
            "avg_streams": 150.0,
        },
    ]
    assert catalog_mart.to_dict(orient="records") == [
        {
            "catalog": "Pop",
            "record_count": 2,
            "total_streams": 250,
            "avg_streams": 125.0,
        },
        {
            "catalog": "Rock",
            "record_count": 1,
            "total_streams": 200,
            "avg_streams": 200.0,
        },
    ]
    assert list(question_mart.columns) == [
        "release_year",
        "record_count",
        "avg_streams",
    ]
    assert list(top_entities_mart.columns) == [
        "rank",
        "provider",
        "record_count",
        "total_streams",
    ]
    assert list(distribution_mart.columns) == ["bin_start", "bin_end", "record_count"]
    assert list(summary_stats_mart.columns) == [
        "source_metric_column",
        "min_streams",
        "max_streams",
        "avg_streams",
        "median_streams",
        "std_streams",
        "p05_streams",
        "p25_streams",
        "p75_streams",
        "p95_streams",
    ]
    assert list(time_trend_mart.columns) == [
        "release_year",
        "record_count",
        "total_streams",
    ]


def test_find_time_column_ignores_quality_flags() -> None:
    dataframe = pd.DataFrame(
        {
            "country": ["A", "B"],
            "invalid_date_parse": [False, True],
            "has_nulls": [False, False],
        }
    )

    assert find_time_column(dataframe) is None


def test_build_time_trend_mart_respects_requested_ordering(tmp_path: Path) -> None:
    dataframe = pd.DataFrame(
        {
            "release_year": [2023, 2024, 2024],
            "streams": [300, 100, 100],
        }
    )

    result = build_time_trend_mart(
        dataframe=dataframe,
        metric_column="streams",
        aggregation="sum",
        ordering="highest",
        time_column="release_year",
        project_root=tmp_path,
    )

    assert result["dataframe"]["release_year"].tolist() == [2023, 2024]
    assert result["dataframe"]["total_streams"].tolist() == [300, 200]


def test_count_aggregation_matches_record_count_when_metric_has_nulls(
    tmp_path: Path,
) -> None:
    dataframe = pd.DataFrame(
        {
            "provider": ["A", "A", "B"],
            "streams": [100.0, None, 50.0],
        }
    )

    result = build_question_answer_mart(
        dataframe=dataframe,
        dimension_column="provider",
        metric_column="streams",
        aggregation="count",
        project_root=tmp_path,
    )

    assert result["dataframe"].to_dict(orient="records") == [
        {"provider": "A", "record_count": 2, "count_streams": 2},
        {"provider": "B", "record_count": 1, "count_streams": 1},
    ]


def test_build_multi_dataset_climate_marts(tmp_path: Path) -> None:
    dataframe = pd.DataFrame(
        {
            "year": ["2020", "2021", "2022"],
            "temperature": [0.8, 0.9, 1.0],
            "co2": [410.0, 412.0, 415.0],
            "sea_level": [3.1, 3.3, 3.6],
            "ice_extent": [14.2, 14.0, 13.7],
        }
    )

    temperature_result = build_integrated_trend_mart(
        dataframe=dataframe,
        dataset_column="temperature",
        mart_name="mart_temperature_trend",
        project_root=tmp_path,
    )
    correlation_result = build_climate_correlation_mart(
        dataframe=dataframe,
        dataset_columns=["temperature", "co2", "sea_level", "ice_extent"],
        project_root=tmp_path,
    )

    assert temperature_result["output_path"] == "marts/mart_temperature_trend.parquet"
    assert (tmp_path / temperature_result["output_path"]).exists()
    assert list(temperature_result["dataframe"].columns) == [
        "year",
        "record_count",
        "avg_temperature",
    ]

    assert correlation_result["output_path"] == "marts/mart_climate_correlation.parquet"
    assert (tmp_path / correlation_result["output_path"]).exists()
    assert list(correlation_result["dataframe"].columns) == [
        "left_metric",
        "right_metric",
        "correlation",
    ]


def test_build_generic_integrated_multi_dataset_marts(tmp_path: Path) -> None:
    dataframe = pd.DataFrame(
        {
            "year": ["2020", "2021", "2022"],
            "revenue": [100.0, 120.0, 140.0],
            "cost": [60.0, 70.0, 85.0],
            "category": ["A", "A", "B"],
        }
    )

    metric_columns = find_integrated_metric_columns(dataframe)
    revenue_trend_result = build_integrated_trend_mart(
        dataframe=dataframe,
        dataset_column="revenue",
        mart_name="mart_revenue_trend",
        project_root=tmp_path,
    )
    cost_summary_result = build_summary_stats_mart(
        dataframe=dataframe,
        metric_column="cost",
        mart_name="mart_cost_summary_stats",
        project_root=tmp_path,
    )
    correlation_result = build_integrated_correlation_mart(
        dataframe=dataframe,
        dataset_columns=metric_columns,
        project_root=tmp_path,
    )

    assert metric_columns == ["revenue", "cost"]
    assert revenue_trend_result["output_path"] == "marts/mart_revenue_trend.parquet"
    assert cost_summary_result["output_path"] == "marts/mart_cost_summary_stats.parquet"
    assert correlation_result["output_path"] == "marts/mart_metric_correlation.parquet"
    assert (tmp_path / revenue_trend_result["output_path"]).exists()
    assert (tmp_path / cost_summary_result["output_path"]).exists()
    assert (tmp_path / correlation_result["output_path"]).exists()
    assert list(revenue_trend_result["dataframe"].columns) == [
        "year",
        "record_count",
        "avg_revenue",
    ]
    assert list(cost_summary_result["dataframe"].columns) == [
        "source_metric_column",
        "min_cost",
        "max_cost",
        "avg_cost",
        "median_cost",
        "std_cost",
        "p05_cost",
        "p25_cost",
        "p75_cost",
        "p95_cost",
    ]
    assert list(correlation_result["dataframe"].columns) == [
        "left_metric",
        "right_metric",
        "correlation",
    ]


def test_build_integrated_trend_mart_filters_years_without_values(
    tmp_path: Path,
) -> None:
    dataframe = pd.DataFrame(
        {
            "year": ["2020", "2021", "2022"],
            "renewable_share": [None, 5.0, 6.0],
        }
    )

    result = build_integrated_trend_mart(
        dataframe=dataframe,
        dataset_column="renewable_share",
        mart_name="mart_renewable_share_trend",
        project_root=tmp_path,
    )

    assert result["dataframe"]["year"].tolist() == ["2021", "2022"]
    assert result["dataframe"]["record_count"].tolist() == [1, 1]
    assert result["dataframe"]["avg_renewable_share"].tolist() == [5.0, 6.0]
