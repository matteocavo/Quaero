from __future__ import annotations

from kpi_engine.semantic_contract import infer_metric_unit


def test_infer_metric_unit_supports_economic_metrics() -> None:
    assert infer_metric_unit("gdp") == "current_usd"
    assert infer_metric_unit("avg_gdp") == "current_usd"
    assert infer_metric_unit("inflation") == "percent"
    assert infer_metric_unit("annual_inflation") == "percent"
    assert infer_metric_unit("unemployment") == "percent"
    assert infer_metric_unit("unemployment_rate") == "percent"


def test_infer_metric_unit_supports_energy_and_music_metrics() -> None:
    assert infer_metric_unit("renewable_share") == "percent"
    assert infer_metric_unit("energy_consumption") == "twh"
    assert infer_metric_unit("global_music_revenue") == "usd"
    assert infer_metric_unit("total_artists") == "count"
