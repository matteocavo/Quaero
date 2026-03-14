"""Dataset profiling utilities for the metadata layer."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_numeric_dtype

from pipelines.utils import normalize_name, resolve_project_root


LOGGER = logging.getLogger(__name__)


def profile_dataframe(
    dataframe: pd.DataFrame,
    source_name: str = "source",
    project_root: str | Path | None = None,
    sample_size: int = 5,
) -> dict[str, Any]:
    """Profile a pandas DataFrame and persist the result to metadata/{source_name}_profile.json."""
    root_dir = resolve_project_root(project_root, __file__)
    metadata_dir = root_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    normalized_source_name = normalize_name(source_name)
    profile_path = metadata_dir / f"{normalized_source_name}_profile.json"

    LOGGER.info("Profiling started")
    profile = {
        "source": normalized_source_name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "row_count": int(len(dataframe)),
        "column_count": int(len(dataframe.columns)),
        "columns": [
            _profile_column(dataframe[column_name], sample_size=sample_size)
            for column_name in dataframe.columns
        ],
    }

    profile_path.write_text(json.dumps(profile, indent=2), encoding="utf-8")

    LOGGER.info(
        "Profiling completed for %s rows x %s columns",
        profile["row_count"],
        profile["column_count"],
    )
    LOGGER.info("Profile written to %s", profile_path)

    return {
        **profile,
        "output_path": profile_path.relative_to(root_dir).as_posix(),
    }


def _profile_column(series: pd.Series, sample_size: int) -> dict[str, Any]:
    """Build a simple profile for a single pandas Series."""
    non_null_series = series.dropna()

    column_profile: dict[str, Any] = {
        "column_name": series.name,
        "dtype": str(series.dtype),
        "null_percentage": _calculate_null_percentage(series),
        "unique_count": int(non_null_series.nunique(dropna=True)),
        "sample_values": _extract_sample_values(non_null_series, sample_size),
    }

    if is_numeric_dtype(series):
        column_profile["min"] = _to_json_compatible(series.min())
        column_profile["max"] = _to_json_compatible(series.max())

    return column_profile


def _calculate_null_percentage(series: pd.Series) -> float:
    """Calculate the percentage of null values in a column."""
    if len(series) == 0:
        return 0.0

    return round(float(series.isna().mean() * 100), 2)


def _extract_sample_values(series: pd.Series, sample_size: int) -> list[Any]:
    """Return a small sample of non-null column values."""
    samples = series.sample(min(sample_size, len(series)), random_state=0).tolist()
    return [_to_json_compatible(value) for value in samples]


def _to_json_compatible(value: Any) -> Any:
    """Convert pandas and NumPy scalars into JSON-friendly Python values."""
    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    if isinstance(value, pd.Timestamp):
        return value.isoformat()

    return value
