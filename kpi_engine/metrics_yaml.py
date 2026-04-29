"""YAML metric definitions generator.

Produces a ``metrics/metrics.yml`` file under the project root using a
dbt-inspired structure.  Each metric entry contains:

  - name        : semantic metric name (e.g. ``avg_streams``)
  - label       : human-readable display name
  - description : auto-generated business description
  - type        : aggregation type (sum / average / count)
  - source_table: the mart table the metric is derived from
  - source_column: the underlying column in that mart

The YAML is intentionally minimal and human-editable — it serves as a
starting point that the analyst refines with real business context.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_numeric_dtype

from pipelines.utils import resolve_project_root


LOGGER = logging.getLogger(__name__)

_SKIP_COLUMNS = {
    "has_nulls",
    "invalid_date_parse",
    "type_conversion_issue",
    "record_count",
}

_AGG_PREFIXES = {
    "total_": "sum",
    "avg_": "average",
    "sum_": "sum",
    "average_": "average",
    "max_": "max",
    "min_": "min",
    "count_": "count",
}


def write_metrics_yaml(
    marts: dict[str, pd.DataFrame],
    analytics_metadata: dict[str, Any] | None = None,
    project_root: str | Path | None = None,
) -> str:
    """Generate metrics/metrics.yml from mart DataFrames and optional metadata.

    Args:
        marts: Mapping of mart name to DataFrame.
        analytics_metadata: Optional analytics_metadata dict to enrich descriptions.
        project_root: Optional project root for output resolution.

    Returns:
        The relative path to the written YAML file.
    """
    root_dir = resolve_project_root(project_root, __file__)
    metrics_dir = root_dir / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    output_path = metrics_dir / "metrics.yml"

    metrics: list[dict[str, Any]] = []

    for mart_name, df in marts.items():
        if df is None or df.empty:
            continue
        for col in df.columns:
            if col in _SKIP_COLUMNS or not is_numeric_dtype(df[col]):
                continue
            agg_type, source_col = _infer_agg_and_source(col)
            metrics.append(
                {
                    "name": col,
                    "label": col.replace("_", " ").title(),
                    "description": _build_description(
                        col, agg_type, source_col, mart_name
                    ),
                    "type": agg_type,
                    "source_table": mart_name,
                    "source_column": source_col,
                }
            )

    yaml_lines = [
        "# Quaero — auto-generated metric definitions",
        "# Edit descriptions and add filters before using in production.",
        "",
    ]
    yaml_lines.append("version: 1")
    yaml_lines.append("metrics:")

    for m in metrics:
        yaml_lines += [
            f"  - name: {m['name']}",
            f'    label: "{m["label"]}"',
            f'    description: "{m["description"]}"',
            f"    type: {m['type']}",
            f"    source_table: {m['source_table']}",
            f"    source_column: {m['source_column']}",
            "",
        ]

    output_path.write_text("\n".join(yaml_lines), encoding="utf-8")
    rel = output_path.relative_to(root_dir).as_posix()
    LOGGER.info("Metrics YAML written to %s (%d metrics)", output_path, len(metrics))
    return rel


def _infer_agg_and_source(col_name: str) -> tuple[str, str]:
    """Infer aggregation type and source column from a semantic metric name."""
    for prefix, agg in _AGG_PREFIXES.items():
        if col_name.startswith(prefix):
            return agg, col_name[len(prefix) :]
    return "sum", col_name


def _build_description(col: str, agg_type: str, source_col: str, mart: str) -> str:
    label = source_col.replace("_", " ")
    return f"{agg_type.capitalize()} of {label} computed in {mart}. Auto-generated — add business context."
