"""Project-facing dataset loaders for public sources that are not plain CSV/Parquet files."""

from __future__ import annotations

import json
import logging
import math
import re
import ssl
import ast
from datetime import datetime, timedelta
from io import StringIO
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import pandas as pd


LOGGER = logging.getLogger(__name__)

SEA_LEVEL_PAGE_URL = (
    "https://sealevel.nasa.gov/understanding-sea-level/key-indicators/"
    "global-mean-sea-level/"
)

TEMPERATURE_COLUMN_NAMES = [
    "year",
    "month",
    "temperature_anomaly",
    "monthly_uncertainty",
    "annual_temperature_anomaly",
    "annual_uncertainty",
    "five_year_temperature_anomaly",
    "five_year_uncertainty",
    "ten_year_temperature_anomaly",
    "ten_year_uncertainty",
    "twenty_year_temperature_anomaly",
    "twenty_year_uncertainty",
]


def load_dataset_from_config(dataset: dict[str, Any]) -> pd.DataFrame:
    """Dispatch a config dataset to its declared custom loader."""
    loader_name = dataset.get("loader")
    if not loader_name:
        raise ValueError("Dataset config is missing a loader name.")

    loader = CONFIG_DATASET_LOADERS.get(loader_name)
    if loader is None:
        raise ValueError(f"Unsupported dataset loader: {loader_name}")

    source_value = str(
        dataset.get("source") or dataset.get("url") or dataset.get("path")
    )
    if not source_value:
        raise ValueError("Dataset config is missing a source, url, or path value.")

    dataframe = loader(source_value, dataset)
    dataframe.attrs.setdefault("loader_warnings", [])
    dataframe.attrs["loader_name"] = loader_name
    return dataframe


def load_berkeley_temperature(
    source_url: str,
    dataset_config: dict[str, Any],
) -> pd.DataFrame:
    """Load Berkeley Earth global temperature anomalies from the text feed."""
    dataframe = pd.read_csv(
        source_url,
        sep=r"\s+",
        comment="%",
        header=None,
        names=TEMPERATURE_COLUMN_NAMES,
        engine="python",
    )
    dataframe = dataframe.dropna(how="all").reset_index(drop=True)
    dataframe["year"] = pd.to_numeric(dataframe["year"], errors="coerce").astype(
        "Int64"
    )
    dataframe["month"] = pd.to_numeric(dataframe["month"], errors="coerce").astype(
        "Int64"
    )
    dataframe["date"] = dataframe.apply(
        lambda row: (
            f"{int(row['year']):04d}-{int(row['month']):02d}"
            if pd.notna(row["year"]) and pd.notna(row["month"])
            else pd.NA
        ),
        axis=1,
    )
    dataframe["source"] = "berkeley_earth"
    return dataframe


def load_owid_world_co2(
    source_url: str,
    dataset_config: dict[str, Any],
) -> pd.DataFrame:
    """Load global CO2 emissions from the OWID source and keep the World series."""
    dataframe = pd.read_csv(
        source_url,
        usecols=[
            "country",
            "year",
            "iso_code",
            "co2",
            "co2_growth_abs",
            "co2_growth_prct",
            "co2_per_capita",
            "share_global_co2",
            "population",
        ],
        low_memory=False,
    )
    dataframe["year"] = pd.to_numeric(dataframe["year"], errors="coerce")
    dataframe = dataframe[dataframe["country"] == "World"].reset_index(drop=True)
    dataframe = dataframe[dataframe["year"].notna()].copy()
    if dataframe.empty:
        raise ValueError("OWID CO2 source does not contain the World series.")
    dataframe["year"] = dataframe["year"].astype("Int64")
    dataframe["month"] = 1
    dataframe["date"] = dataframe["year"].map(
        lambda value: f"{int(value):04d}-01" if pd.notna(value) else pd.NA
    )
    return dataframe


def load_nasa_sea_level(
    source_url: str,
    dataset_config: dict[str, Any],
) -> pd.DataFrame:
    """Load global mean sea level observations from the public NASA page data arrays."""
    page_url = SEA_LEVEL_PAGE_URL if "climate.nasa.gov" in source_url else source_url
    html = _download_text(
        page_url,
        allow_insecure_ssl=bool(dataset_config.get("allow_insecure_ssl", False)),
    )
    chart_x = _extract_js_float_array(html, "chart_x")
    chart_y = _extract_js_float_array(html, "chart_y")
    if len(chart_x) != len(chart_y):
        raise ValueError("NASA sea level page returned mismatched chart arrays.")

    records: list[dict[str, Any]] = []
    for decimal_year, sea_level in zip(chart_x, chart_y, strict=False):
        observed_at = _decimal_year_to_datetime(decimal_year)
        records.append(
            {
                "year_decimal": decimal_year,
                "year": observed_at.year,
                "month": observed_at.month,
                "date": observed_at.strftime("%Y-%m"),
                "sea_level": sea_level,
                "source": "nasa_satellite_altimetry",
            }
        )

    return pd.DataFrame.from_records(records)


def load_nsidc_monthly_ice_extent(
    source_url: str,
    dataset_config: dict[str, Any],
) -> pd.DataFrame:
    """Load and concatenate the NSIDC monthly Northern Hemisphere ice extent files."""
    parsed = urlparse(source_url)
    base_path = parsed.path.rsplit("/", 1)[0]
    base_url = f"{parsed.scheme}://{parsed.netloc}{base_path}"
    version_match = re.search(r"_v(\d+\.\d+)\.csv$", parsed.path)
    configured_version = version_match.group(1) if version_match else "4.0"
    version_candidates = [configured_version]
    if configured_version != "4.0":
        version_candidates.append("4.0")

    frames: list[pd.DataFrame] = []
    skipped_files: list[str] = []
    fallback_files: list[str] = []
    for month in range(1, 13):
        frame: pd.DataFrame | None = None
        attempted_urls: list[str] = []
        for version in version_candidates:
            monthly_url = f"{base_url}/N_{month:02d}_extent_v{version}.csv"
            attempted_urls.append(monthly_url)
            try:
                frame = pd.read_csv(monthly_url, skipinitialspace=True)
                if version != configured_version:
                    fallback_message = (
                        f"Configured NSIDC file for month {month:02d} was unavailable; "
                        f"used fallback version v{version} from {monthly_url}."
                    )
                    LOGGER.warning(fallback_message)
                    fallback_files.append(monthly_url)
                break
            except (HTTPError, URLError, FileNotFoundError):
                continue
        if frame is None:
            warning_message = (
                f"Skipped NSIDC month {month:02d} because all candidate files were unavailable: "
                + ", ".join(attempted_urls)
            )
            LOGGER.warning(warning_message)
            skipped_files.extend(attempted_urls)
            continue
        frames.append(frame)

    if not frames:
        raise ValueError(
            "No NSIDC monthly ice extent files were available for the configured source."
        )

    dataframe = pd.concat(frames, ignore_index=True)
    dataframe.columns = [
        column.strip().lower().replace(" ", "_") for column in dataframe.columns
    ]
    dataframe = dataframe.rename(columns={"mo": "month", "extent": "ice_extent"})
    dataframe["year"] = pd.to_numeric(dataframe["year"], errors="coerce").astype(
        "Int64"
    )
    dataframe["month"] = pd.to_numeric(dataframe["month"], errors="coerce").astype(
        "Int64"
    )
    dataframe["ice_extent"] = pd.to_numeric(dataframe["ice_extent"], errors="coerce")
    dataframe["area"] = pd.to_numeric(dataframe["area"], errors="coerce")
    dataframe = (
        dataframe.replace(-9999, pd.NA)
        .dropna(subset=["ice_extent"])
        .reset_index(drop=True)
    )
    dataframe["date"] = dataframe.apply(
        lambda row: (
            f"{int(row['year']):04d}-{int(row['month']):02d}"
            if pd.notna(row["year"]) and pd.notna(row["month"])
            else pd.NA
        ),
        axis=1,
    )
    loader_warnings: list[str] = []
    if fallback_files:
        loader_warnings.append(
            "Some NSIDC monthly files required a fallback version: "
            + ", ".join(fallback_files)
        )
    if skipped_files:
        loader_warnings.append(
            "Missing NSIDC monthly files were skipped: " + ", ".join(skipped_files)
        )
    dataframe.attrs["loader_warnings"] = loader_warnings
    return dataframe


def load_world_indicator_series(
    source_url: str,
    dataset_config: dict[str, Any],
) -> pd.DataFrame:
    """Load a world-level indicator series from CSV or World Bank-style JSON."""
    metric_column = str(dataset_config.get("metric_column") or "").strip()
    if not metric_column:
        raise ValueError(
            "world_indicator_series loader requires metric_column in config."
        )

    source_format = str(dataset_config.get("source_format") or "").strip().lower()
    if (
        source_format == "json"
        or source_url.lower().endswith(".json")
        or "api.worldbank.org" in source_url
    ):
        dataframe = _load_world_bank_indicator_json(source_url)
    else:
        dataframe = _read_csv_from_public_source(source_url)

    country_column = str(dataset_config.get("country_column") or "Country Name")
    year_column = str(dataset_config.get("year_column") or "Year")
    value_column = str(dataset_config.get("value_column") or "Value")
    country_value = str(dataset_config.get("country_value") or "World")

    required_columns = {country_column, year_column, value_column}
    missing_columns = required_columns.difference(dataframe.columns)
    if missing_columns:
        raise ValueError(
            "world_indicator_series source is missing required columns: "
            + ", ".join(sorted(missing_columns))
        )

    dataframe = dataframe[dataframe[country_column] == country_value].copy()
    if dataframe.empty:
        raise ValueError(
            f"world_indicator_series source does not contain {country_value!r} rows."
        )

    dataframe = dataframe.rename(
        columns={
            country_column: "country_name",
            year_column: "year",
            value_column: metric_column,
        }
    )
    dataframe["year"] = pd.to_numeric(dataframe["year"], errors="coerce").astype(
        "Int64"
    )
    dataframe[metric_column] = pd.to_numeric(dataframe[metric_column], errors="coerce")
    dataframe = dataframe.dropna(subset=["year", metric_column]).reset_index(drop=True)
    dataframe["date"] = dataframe["year"].map(
        lambda value: f"{int(value):04d}-01" if pd.notna(value) else pd.NA
    )
    return dataframe


def load_aggregated_time_series(
    source_url: str,
    dataset_config: dict[str, Any],
) -> pd.DataFrame:
    """Load a public CSV and aggregate it into a single metric by year."""
    metric_column = str(dataset_config.get("metric_column") or "").strip()
    if not metric_column:
        raise ValueError(
            "aggregated_time_series loader requires metric_column in config."
        )

    time_source_column = str(dataset_config.get("time_source_column") or "year")
    aggregation = str(dataset_config.get("aggregation") or "sum").strip().lower()
    dataframe = _read_csv_from_public_source(source_url)

    if time_source_column not in dataframe.columns:
        raise ValueError(
            f"aggregated_time_series source is missing time column: {time_source_column}"
        )

    working = dataframe.copy()
    working["year"] = _coerce_year_series(working[time_source_column])
    working = working.dropna(subset=["year"]).copy()
    min_year = dataset_config.get("min_year")
    max_year = dataset_config.get("max_year")
    if min_year is not None:
        working = working[working["year"] >= int(min_year)].copy()
    if max_year is not None:
        working = working[working["year"] <= int(max_year)].copy()
    if working.empty:
        raise ValueError(
            "aggregated_time_series source did not contain usable year values."
        )

    if aggregation == "count_distinct":
        entity_column = str(dataset_config.get("entity_column") or "").strip()
        if not entity_column:
            raise ValueError(
                "aggregated_time_series count_distinct requires entity_column in config."
            )
        if entity_column not in working.columns:
            raise ValueError(
                f"aggregated_time_series source is missing entity column: {entity_column}"
            )

        entity_parser = str(dataset_config.get("entity_parser") or "").strip().lower()
        records: list[dict[str, Any]] = []
        for year, raw_value in zip(
            working["year"], working[entity_column], strict=False
        ):
            for entity_value in _parse_entity_values(raw_value, entity_parser):
                cleaned = str(entity_value).strip()
                if cleaned:
                    records.append({"year": int(year), metric_column: cleaned})

        if not records:
            raise ValueError(
                "aggregated_time_series count_distinct did not yield any entity values."
            )

        aggregated = (
            pd.DataFrame.from_records(records)
            .groupby("year", dropna=False)[metric_column]
            .nunique()
            .reset_index(name=metric_column)
        )
    else:
        value_column = str(dataset_config.get("value_column") or "").strip()
        if not value_column:
            raise ValueError(
                "aggregated_time_series numeric aggregations require value_column in config."
            )
        if value_column not in working.columns:
            raise ValueError(
                f"aggregated_time_series source is missing value column: {value_column}"
            )

        working[metric_column] = pd.to_numeric(working[value_column], errors="coerce")
        working = working.dropna(subset=[metric_column]).copy()
        if working.empty:
            raise ValueError(
                "aggregated_time_series source did not contain usable numeric values."
            )

        grouped = working.groupby("year", dropna=False)[metric_column]
        if aggregation == "average":
            aggregated = grouped.mean().reset_index(name=metric_column)
        elif aggregation == "sum":
            aggregated = grouped.sum(min_count=1).reset_index(name=metric_column)
        elif aggregation == "count":
            aggregated = grouped.count().reset_index(name=metric_column)
        else:
            raise ValueError(
                f"Unsupported aggregated_time_series aggregation: {aggregation}"
            )

    aggregated["year"] = pd.to_numeric(aggregated["year"], errors="coerce").astype(
        "Int64"
    )
    aggregated = (
        aggregated.dropna(subset=["year"]).sort_values("year").reset_index(drop=True)
    )
    aggregated["date"] = aggregated["year"].map(
        lambda value: f"{int(value):04d}-01" if pd.notna(value) else pd.NA
    )
    return aggregated


def _download_text(source_url: str, allow_insecure_ssl: bool = False) -> str:
    """Download text content for public data pages.

    The default path uses the platform certificate store via
    ``ssl.create_default_context()``. Insecure SSL is available only when a
    dataset config explicitly opts in with ``allow_insecure_ssl: true``.
    """
    context = ssl.create_default_context()
    if allow_insecure_ssl:
        # This is intentionally opt-in only for exceptional public-source cases.
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

    request = Request(source_url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, context=context) as response:
        return response.read().decode("utf-8", errors="replace")


def _read_csv_from_public_source(source_url: str) -> pd.DataFrame:
    """Read a CSV from a public URL, falling back to an explicit text download."""
    try:
        return pd.read_csv(source_url, low_memory=False)
    except (HTTPError, URLError):
        csv_text = _download_text(source_url)
        return pd.read_csv(StringIO(csv_text), low_memory=False)


def _coerce_year_series(series: pd.Series) -> pd.Series:
    """Coerce common year or date representations into integer years."""
    numeric = pd.to_numeric(series, errors="coerce")
    if float(numeric.notna().mean()) >= 0.8:
        return numeric.round().astype("Int64")

    parsed = pd.to_datetime(series, errors="coerce", utc=True)
    if parsed.notna().any():
        years = pd.Series(parsed.dt.year, index=series.index, dtype="Int64")
        missing_mask = years.isna()
        if missing_mask.any():
            extracted = (
                series.astype("string")
                .str.extract(r"(?P<year>\d{4})")["year"]
                .pipe(pd.to_numeric, errors="coerce")
                .astype("Int64")
            )
            years = years.fillna(extracted)
        return years

    return (
        series.astype("string")
        .str.extract(r"(?P<year>\d{4})")["year"]
        .pipe(pd.to_numeric, errors="coerce")
        .astype("Int64")
    )


def _parse_entity_values(raw_value: Any, parser_name: str) -> list[str]:
    """Parse entity cell values into a flat list used for distinct counting."""
    if pd.isna(raw_value):
        return []

    if parser_name == "python_list" and isinstance(raw_value, str):
        try:
            parsed = ast.literal_eval(raw_value)
        except (ValueError, SyntaxError):
            return [raw_value]
        if isinstance(parsed, (list, tuple, set)):
            return [str(item) for item in parsed if str(item).strip()]
        return [str(parsed)]

    return [str(raw_value)]


def _load_world_bank_indicator_json(source_url: str) -> pd.DataFrame:
    """Load a World Bank API indicator feed into a standard dataframe."""
    payload = json.loads(_download_text(source_url))
    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError(
            "World Bank indicator feed returned an unexpected response shape."
        )
    records = payload[1]
    if not isinstance(records, list):
        raise ValueError("World Bank indicator feed did not contain record data.")

    normalized_records: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        country = record.get("country", {})
        indicator = record.get("indicator", {})
        normalized_records.append(
            {
                "Country Name": country.get("value"),
                "Country Code": country.get("id"),
                "Indicator Name": indicator.get("value"),
                "Indicator Code": indicator.get("id"),
                "Year": record.get("date"),
                "Value": record.get("value"),
            }
        )

    return pd.DataFrame.from_records(normalized_records)


def _extract_js_float_array(html: str, variable_name: str) -> list[float]:
    """Extract a JavaScript numeric array from an HTML page."""
    match = re.search(
        rf"var {re.escape(variable_name)} = \[(.*?)\]\s*var ",
        html,
        re.DOTALL,
    )
    if match is None:
        raise ValueError(f"Could not find {variable_name} in the source page.")

    values = []
    for raw_value in match.group(1).replace("\n", " ").split(","):
        cleaned_value = raw_value.strip()
        if not cleaned_value:
            continue
        values.append(float(cleaned_value))
    return values


def _decimal_year_to_datetime(decimal_year: float) -> datetime:
    """Convert a decimal year value into an approximate UTC datetime."""
    year = int(math.floor(decimal_year))
    year_fraction = decimal_year - year
    day_offset = round(year_fraction * 365.2425)
    return datetime(year, 1, 1) + timedelta(days=day_offset)


CONFIG_DATASET_LOADERS: dict[str, Callable[[str, dict[str, Any]], pd.DataFrame]] = {
    "berkeley_temperature": load_berkeley_temperature,
    "owid_world_co2": load_owid_world_co2,
    "nasa_sea_level": load_nasa_sea_level,
    "nsidc_monthly_ice_extent": load_nsidc_monthly_ice_extent,
    "world_indicator_series": load_world_indicator_series,
    "aggregated_time_series": load_aggregated_time_series,
}
