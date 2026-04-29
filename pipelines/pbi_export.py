"""Power BI schema export utilities.

Generates a ``pbi_schema.json`` file under ``projects/<name>/metadata/`` that
describes the mart tables in a format designed to accelerate Power BI data
model setup:

  - Table and column definitions with Power BI-compatible data types
  - Suggested relationships inferred by shared column names across tables
  - Starter DAX measure suggestions for every numeric column

The JSON is human-readable so the analyst can use it as a direct reference
while building the model in Power BI Desktop.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_float_dtype,
    is_integer_dtype,
)

from pipelines.utils import resolve_project_root


LOGGER = logging.getLogger(__name__)

_SKIP_COLUMNS = {"has_nulls", "invalid_date_parse", "type_conversion_issue"}
_SKIP_RELATIONSHIP_COLUMNS = {
    "has_nulls",
    "invalid_date_parse",
    "type_conversion_issue",
    "record_count",
}


def export_pbi_schema(
    marts: dict[str, pd.DataFrame],
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Generate a Power BI-oriented schema document for the provided mart tables.

    Args:
        marts: Mapping of mart name to DataFrame.
        project_root: Optional project root for output resolution.

    Returns:
        The schema dict (also written to ``metadata/pbi_schema.json``).
    """
    root_dir = resolve_project_root(project_root, __file__)
    metadata_dir = root_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    output_path = metadata_dir / "pbi_schema.json"

    tables = {
        name: _describe_table(name, df)
        for name, df in marts.items()
        if df is not None and not df.empty
    }
    relationships = _suggest_relationships(tables)
    measures = _suggest_measures(tables)

    schema: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "tool": "Quaero v1",
        "tables": list(tables.values()),
        "suggested_relationships": relationships,
        "suggested_dax_measures": measures,
    }

    output_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
    LOGGER.info("Power BI schema written to %s (%d tables)", output_path, len(tables))

    return {**schema, "output_path": output_path.relative_to(root_dir).as_posix()}


def _describe_table(table_name: str, dataframe: pd.DataFrame) -> dict[str, Any]:
    cols = [c for c in dataframe.columns if c not in _SKIP_COLUMNS]
    return {
        "name": table_name,
        "row_count": len(dataframe),
        "columns": [_describe_column(dataframe[col]) for col in cols],
    }


def _describe_column(series: pd.Series) -> dict[str, Any]:
    return {
        "name": series.name,
        "dtype_pandas": str(series.dtype),
        "dtype_pbi": _infer_pbi_type(series),
        "nullable": bool(series.isna().any()),
        "sample_values": [_safe_val(v) for v in series.dropna().head(3).tolist()],
    }


def _infer_pbi_type(series: pd.Series) -> str:
    if is_bool_dtype(series):
        return "True/False"
    if is_integer_dtype(series):
        return "Whole Number"
    if is_float_dtype(series):
        return "Decimal Number"
    if is_datetime64_any_dtype(series):
        return "Date/Time"
    return "Text"


def _suggest_relationships(
    tables: dict[str, dict[str, Any]],
) -> list[dict[str, str]]:
    """Identify columns with the same name across tables as candidate join keys."""
    col_to_tables: dict[str, list[str]] = {}
    for table_name, table_def in tables.items():
        for col_def in table_def["columns"]:
            col_name = col_def["name"]
            col_to_tables.setdefault(col_name, []).append(table_name)

    relationships: list[dict[str, str]] = []
    for col_name, table_list in col_to_tables.items():
        if len(table_list) < 2 or col_name in _SKIP_RELATIONSHIP_COLUMNS:
            continue
        for i in range(len(table_list) - 1):
            relationships.append(
                {
                    "from_table": table_list[i],
                    "from_column": col_name,
                    "to_table": table_list[i + 1],
                    "to_column": col_name,
                    "cardinality": "many-to-one (verify)",
                    "note": "Inferred by shared column name — validate before use.",
                }
            )

    return relationships


def _suggest_measures(tables: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    """Generate starter DAX measures for numeric columns in each table."""
    measures: list[dict[str, str]] = []
    for table_name, table_def in tables.items():
        for col_def in table_def["columns"]:
            if col_def["dtype_pbi"] not in {"Whole Number", "Decimal Number"}:
                continue
            col = col_def["name"]
            if col == "record_count":
                continue
            measures += [
                {
                    "table": table_name,
                    "measure_name": f"Total {col}",
                    "dax": f"Total {col} = SUM('{table_name}'[{col}])",
                },
                {
                    "table": table_name,
                    "measure_name": f"Avg {col}",
                    "dax": f"Avg {col} = AVERAGE('{table_name}'[{col}])",
                },
            ]
    return measures


def _safe_val(v: Any) -> Any:
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    if isinstance(v, float) and pd.isna(v):
        return None
    return v
