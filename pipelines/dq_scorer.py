"""Data Quality scoring utilities.

Computes a per-column DQ score based on four dimensions:
  - completeness  : fraction of non-null values (0-100)
  - uniqueness    : fraction of distinct values relative to non-null count (0-100)
  - outlier_flag  : True when >5% of values fall beyond 3 sigma (numeric only)
  - format_score  : proxy for format consistency

Overall column score (weighted):
  completeness 40% + uniqueness 20% + format_score 40%
Outlier flag is informational only and does not penalise the score.

Scores range from 0 to 100. The dataset overall_score is the unweighted mean
of all column scores.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_numeric_dtype, is_object_dtype, is_string_dtype

from pipelines.utils import normalize_name, resolve_project_root


LOGGER = logging.getLogger(__name__)

_COMPLETENESS_WEIGHT = 0.40
_UNIQUENESS_WEIGHT = 0.20
_FORMAT_WEIGHT = 0.40

_OUTLIER_SIGMA = 3.0
_OUTLIER_THRESHOLD = 0.05

_QUALITY_FLAG_COLUMNS = {"has_nulls", "invalid_date_parse", "type_conversion_issue"}


def score_dataframe(
    dataframe: pd.DataFrame,
    source_name: str,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Compute DQ scores for every column and persist results to metadata/.

    Args:
        dataframe: The DataFrame to score (typically the staging output).
        source_name: Logical source name used to name the output file.
        project_root: Optional project root for output resolution.

    Returns:
        A dict with keys ``overall_score``, ``columns``, and ``output_path``.
    """
    root_dir = resolve_project_root(project_root, __file__)
    metadata_dir = root_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)

    normalized = normalize_name(source_name)
    output_path = metadata_dir / f"dq_score_{normalized}.json"

    scored_columns = [c for c in dataframe.columns if c not in _QUALITY_FLAG_COLUMNS]

    column_results: list[dict[str, Any]] = [
        _score_column(dataframe[col]) for col in scored_columns
    ]

    overall = (
        round(sum(r["dq_score"] for r in column_results) / len(column_results), 1)
        if column_results
        else 0.0
    )

    result: dict[str, Any] = {
        "source": normalized,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall_score": overall,
        "column_count": len(column_results),
        "columns": column_results,
    }

    output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
    LOGGER.info("DQ score written to %s (overall: %.1f)", output_path, overall)

    return {**result, "output_path": output_path.relative_to(root_dir).as_posix()}


def _score_column(series: pd.Series) -> dict[str, Any]:
    """Compute DQ dimensions for a single column."""
    completeness = _completeness(series)
    uniqueness = _uniqueness(series)
    outlier_flag = _outlier_flag(series)
    format_score = _format_score(series)

    dq_score = round(
        completeness * _COMPLETENESS_WEIGHT
        + uniqueness * _UNIQUENESS_WEIGHT
        + format_score * _FORMAT_WEIGHT,
        1,
    )

    return {
        "column": series.name,
        "dtype": str(series.dtype),
        "completeness": completeness,
        "uniqueness": uniqueness,
        "outlier_flag": outlier_flag,
        "format_score": format_score,
        "dq_score": dq_score,
    }


def _completeness(series: pd.Series) -> float:
    if len(series) == 0:
        return 100.0
    return round(float(series.notna().mean() * 100), 1)


def _uniqueness(series: pd.Series) -> float:
    non_null = series.dropna()
    if len(non_null) == 0:
        return 100.0
    return round(float(non_null.nunique() / len(non_null) * 100), 1)


def _outlier_flag(series: pd.Series) -> bool:
    if not is_numeric_dtype(series):
        return False
    non_null = series.dropna()
    if len(non_null) < 4:
        return False
    mean = float(non_null.mean())
    std = float(non_null.std())
    if std == 0:
        return False
    outliers = ((non_null - mean).abs() > _OUTLIER_SIGMA * std).sum()
    return float(outliers / len(non_null)) > _OUTLIER_THRESHOLD


def _format_score(series: pd.Series) -> float:
    """Proxy for format consistency.

    Numeric and datetime columns score 100.
    Object/string columns are penalised when they contain mixed-type values.
    """
    if is_numeric_dtype(series):
        return 100.0
    if is_object_dtype(series) or is_string_dtype(series):
        non_null = series.dropna()
        if len(non_null) == 0:
            return 100.0
        non_string_ratio = float(
            sum(1 for v in non_null if not isinstance(v, str)) / len(non_null)
        )
        return round(max(0.0, 100.0 - non_string_ratio * 100), 1)
    return 100.0
