from __future__ import annotations

from pipelines.utils import format_analysis_title, humanize_name


def test_humanize_name_converts_snake_case_to_title_case() -> None:
    assert humanize_name("climate_change_analysis") == "Climate Change Analysis"


def test_humanize_name_preserves_known_acronyms() -> None:
    assert humanize_name("bi_api_metrics") == "BI API Metrics"
    assert humanize_name("my_co2_analysis") == "My CO2 Analysis"
    assert humanize_name("owid_nasa_gdp_usd_kpi_json_sql_csv") == (
        "OWID NASA GDP USD KPI JSON SQL CSV"
    )


def test_format_analysis_title_adds_suffix_when_missing() -> None:
    assert format_analysis_title("global_temperature") == "Global Temperature Analysis"


def test_format_analysis_title_avoids_duplicate_analysis_suffix() -> None:
    assert format_analysis_title("climate_change_analysis") == "Climate Change Analysis"
    assert format_analysis_title("analysis") == "Analysis"
    assert format_analysis_title("my_co2_analysis") == "My CO2 Analysis"
