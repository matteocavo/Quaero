from __future__ import annotations

import ssl
from io import StringIO
from urllib.error import HTTPError

import pandas as pd

from pipelines.project_loaders import (
    _download_text,
    load_aggregated_time_series,
    load_nsidc_monthly_ice_extent,
    load_owid_world_co2,
    load_world_indicator_series,
)


class _FakeResponse:
    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return b"ok"


def test_download_text_uses_verified_ssl_by_default(monkeypatch) -> None:
    captured: dict[str, object] = {}
    sentinel_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    def fake_create_default_context() -> ssl.SSLContext:
        captured["created"] = True
        return sentinel_context

    def fake_urlopen(request, context: ssl.SSLContext):
        captured["url"] = request.full_url
        captured["user_agent"] = request.headers.get("User-agent")
        captured["context"] = context
        return _FakeResponse()

    monkeypatch.setattr(
        "pipelines.project_loaders.ssl.create_default_context",
        fake_create_default_context,
    )
    monkeypatch.setattr("pipelines.project_loaders.urlopen", fake_urlopen)

    result = _download_text("https://example.com/data.txt")

    assert result == "ok"
    assert captured["created"] is True
    assert captured["context"] is sentinel_context
    assert captured["user_agent"] == "Mozilla/5.0"
    assert sentinel_context.verify_mode == ssl.CERT_REQUIRED
    assert sentinel_context.check_hostname is True


def test_download_text_only_disables_ssl_checks_when_explicit(monkeypatch) -> None:
    captured: dict[str, ssl.SSLContext] = {}
    sentinel_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    def fake_create_default_context() -> ssl.SSLContext:
        return sentinel_context

    def fake_urlopen(request, context: ssl.SSLContext):
        captured["context"] = context
        return _FakeResponse()

    monkeypatch.setattr(
        "pipelines.project_loaders.ssl.create_default_context",
        fake_create_default_context,
    )
    monkeypatch.setattr("pipelines.project_loaders.urlopen", fake_urlopen)

    _download_text("https://example.com/data.txt", allow_insecure_ssl=True)

    assert captured["context"] is sentinel_context
    assert sentinel_context.check_hostname is False
    assert sentinel_context.verify_mode == ssl.CERT_NONE


def test_load_owid_world_co2_skips_null_year_rows(monkeypatch) -> None:
    dataframe = pd.DataFrame(
        {
            "country": ["World", "World", "France"],
            "year": [2020, None, 2021],
            "iso_code": ["OWID_WRL", "OWID_WRL", "FRA"],
            "co2": [34000.0, 35000.0, 300.0],
            "co2_growth_abs": [1.0, 1.2, 0.3],
            "co2_growth_prct": [0.5, 0.6, 0.2],
            "co2_per_capita": [4.3, 4.4, 5.1],
            "share_global_co2": [100.0, 100.0, 0.8],
            "population": [7_800_000_000, 7_810_000_000, 67_000_000],
        }
    )

    monkeypatch.setattr(
        "pipelines.project_loaders.pd.read_csv", lambda *args, **kwargs: dataframe
    )

    result = load_owid_world_co2("https://example.com/owid.csv", {})

    assert result["country"].tolist() == ["World"]
    assert result["year"].tolist() == [2020]
    assert result["date"].tolist() == ["2020-01"]


def test_load_nsidc_monthly_ice_extent_skips_missing_files_with_warning(
    monkeypatch,
) -> None:
    requested_urls: list[str] = []

    def fake_read_csv(url: str, skipinitialspace: bool = True) -> pd.DataFrame:
        requested_urls.append(url)
        if "N_02_extent_v3.0.csv" in url:
            raise HTTPError(url, 404, "Not Found", hdrs=None, fp=None)
        return pd.DataFrame(
            {
                "year": [2020],
                " mo": [1],
                "source_dataset": ["NSIDC-0051"],
                " region": ["N"],
                " extent": [12.3],
                "   area": [10.1],
            }
        )

    monkeypatch.setattr("pipelines.project_loaders.pd.read_csv", fake_read_csv)

    result = load_nsidc_monthly_ice_extent(
        "https://example.com/N_09_extent_v3.0.csv",
        {},
    )

    assert any("N_02_extent_v3.0.csv" in url for url in requested_urls)
    assert any("N_02_extent_v4.0.csv" in url for url in requested_urls)
    assert result["ice_extent"].tolist()
    assert result.attrs["loader_warnings"]
    assert "N_02_extent_v4.0.csv" in result.attrs["loader_warnings"][0]


def test_load_world_indicator_series_filters_world_from_csv(monkeypatch) -> None:
    dataframe = pd.DataFrame(
        {
            "Country Name": ["World", "France"],
            "Year": [2023, 2023],
            "Value": [105.5, 99.1],
        }
    )

    monkeypatch.setattr(
        "pipelines.project_loaders.pd.read_csv", lambda *args, **kwargs: dataframe
    )

    result = load_world_indicator_series(
        "https://example.com/gdp.csv",
        {
            "metric_column": "gdp",
            "country_column": "Country Name",
            "country_value": "World",
            "year_column": "Year",
            "value_column": "Value",
        },
    )

    assert result["country_name"].tolist() == ["World"]
    assert result["year"].tolist() == [2023]
    assert result["gdp"].tolist() == [105.5]
    assert result["date"].tolist() == ["2023-01"]


def test_load_world_indicator_series_falls_back_to_text_download_on_http_error(
    monkeypatch,
) -> None:
    original_read_csv = pd.read_csv

    def fake_read_csv(source, *args, **kwargs) -> pd.DataFrame:
        if isinstance(source, str):
            raise HTTPError(source, 403, "Forbidden", hdrs=None, fp=None)
        if isinstance(source, StringIO):
            return original_read_csv(StringIO(source.getvalue()))
        raise AssertionError(f"Unexpected source type: {type(source)!r}")

    csv_text = "Entity,Year,Renewables\nWorld,2024,13.4\nFrance,2024,21.0\n"

    monkeypatch.setattr("pipelines.project_loaders.pd.read_csv", fake_read_csv)
    monkeypatch.setattr(
        "pipelines.project_loaders._download_text", lambda *args, **kwargs: csv_text
    )

    result = load_world_indicator_series(
        "https://ourworldindata.org/grapher/renewable-share-energy.csv",
        {
            "metric_column": "renewable_share",
            "country_column": "Entity",
            "country_value": "World",
            "year_column": "Year",
            "value_column": "Renewables",
        },
    )

    assert result["country_name"].tolist() == ["World"]
    assert result["year"].tolist() == [2024]
    assert result["renewable_share"].tolist() == [13.4]


def test_load_world_indicator_series_supports_world_bank_json(monkeypatch) -> None:
    payload = (
        '[{"page":1,"pages":1},'
        '[{"country":{"id":"WLD","value":"World"},'
        '"indicator":{"id":"FP.CPI.TOTL.ZG","value":"Inflation"},'
        '"date":"2024","value":4.2},'
        '{"country":{"id":"FRA","value":"France"},'
        '"indicator":{"id":"FP.CPI.TOTL.ZG","value":"Inflation"},'
        '"date":"2024","value":2.1}]]'
    )

    monkeypatch.setattr(
        "pipelines.project_loaders._download_text", lambda *args, **kwargs: payload
    )

    result = load_world_indicator_series(
        "https://api.worldbank.org/v2/country/all/indicator/FP.CPI.TOTL.ZG?format=json",
        {
            "metric_column": "inflation",
            "country_column": "Country Name",
            "country_value": "World",
            "year_column": "Year",
            "value_column": "Value",
            "source_format": "json",
        },
    )

    assert result["country_name"].tolist() == ["World"]
    assert result["year"].tolist() == [2024]
    assert result["inflation"].tolist() == [4.2]


def test_load_aggregated_time_series_averages_values_by_year(monkeypatch) -> None:
    dataframe = pd.DataFrame(
        {
            "original_publication_year": [2000, 2000, 2001],
            "average_rating": [4.0, 3.0, 5.0],
        }
    )

    monkeypatch.setattr(
        "pipelines.project_loaders.pd.read_csv", lambda *args, **kwargs: dataframe
    )

    result = load_aggregated_time_series(
        "https://example.com/books.csv",
        {
            "metric_column": "avg_popularity",
            "time_source_column": "original_publication_year",
            "value_column": "average_rating",
            "aggregation": "average",
        },
    )

    assert result["year"].tolist() == [2000, 2001]
    assert result["avg_popularity"].tolist() == [3.5, 5.0]
    assert result["date"].tolist() == ["2000-01", "2001-01"]


def test_load_aggregated_time_series_sums_date_valued_series(monkeypatch) -> None:
    dataframe = pd.DataFrame(
        {
            "year": ["1973-01-01", "1973-01-01", "1974-01-01"],
            "revenue": [10.0, 15.0, 20.0],
        }
    )

    monkeypatch.setattr(
        "pipelines.project_loaders.pd.read_csv", lambda *args, **kwargs: dataframe
    )

    result = load_aggregated_time_series(
        "https://example.com/revenue.csv",
        {
            "metric_column": "global_music_revenue",
            "time_source_column": "year",
            "value_column": "revenue",
            "aggregation": "sum",
        },
    )

    assert result["year"].tolist() == [1973, 1974]
    assert result["global_music_revenue"].tolist() == [25.0, 20.0]


def test_load_aggregated_time_series_respects_year_window(monkeypatch) -> None:
    dataframe = pd.DataFrame(
        {
            "original_publication_year": [1899, 1973, 1974],
            "ratings_count": [10, 20, 30],
        }
    )

    monkeypatch.setattr(
        "pipelines.project_loaders.pd.read_csv", lambda *args, **kwargs: dataframe
    )

    result = load_aggregated_time_series(
        "https://example.com/books.csv",
        {
            "metric_column": "streaming_volume_proxy",
            "time_source_column": "original_publication_year",
            "value_column": "ratings_count",
            "aggregation": "sum",
            "min_year": 1973,
        },
    )

    assert result["year"].tolist() == [1973, 1974]
    assert result["streaming_volume_proxy"].tolist() == [20, 30]


def test_load_aggregated_time_series_counts_distinct_entities_from_python_lists(
    monkeypatch,
) -> None:
    dataframe = pd.DataFrame(
        {
            "year": [2000, 2000, 2001],
            "artists": [
                "['Artist A', 'Artist B']",
                "['Artist B', 'Artist C']",
                "['Artist A']",
            ],
        }
    )

    monkeypatch.setattr(
        "pipelines.project_loaders.pd.read_csv", lambda *args, **kwargs: dataframe
    )

    result = load_aggregated_time_series(
        "https://example.com/artists.csv",
        {
            "metric_column": "total_artists",
            "time_source_column": "year",
            "entity_column": "artists",
            "aggregation": "count_distinct",
            "entity_parser": "python_list",
        },
    )

    assert result["year"].tolist() == [2000, 2001]
    assert result["total_artists"].tolist() == [3, 1]
