"""SQL export utilities — generate PostgreSQL DDL and INSERT statements from mart DataFrames.

For each mart passed to ``export_marts_to_sql`` a ``.sql`` file is written
under ``projects/<name>/marts/``.  The file contains:

  1. A ``DROP TABLE IF EXISTS`` + ``CREATE TABLE IF NOT EXISTS`` statement with
     PostgreSQL-compatible types inferred from the DataFrame dtypes.
  2. ``INSERT INTO`` statements for every row, batched in groups of 500.

The result is a self-contained SQL file that can be executed directly against
a PostgreSQL database to load the mart without any additional tooling.
"""

from __future__ import annotations

import logging
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

_INSERT_BATCH_SIZE = 500


def export_marts_to_sql(
    marts: dict[str, pd.DataFrame],
    project_root: str | Path | None = None,
) -> dict[str, str]:
    """Write one ``.sql`` file per mart DataFrame.

    Args:
        marts: Mapping of mart name (e.g. ``"mart_release_impact"``) to DataFrame.
        project_root: Optional project root for output resolution.

    Returns:
        Mapping of mart name to the relative path of the generated ``.sql`` file.
    """
    root_dir = resolve_project_root(project_root, __file__)
    marts_dir = root_dir / "marts"
    marts_dir.mkdir(parents=True, exist_ok=True)

    output_paths: dict[str, str] = {}

    for mart_name, dataframe in marts.items():
        if dataframe is None or dataframe.empty:
            LOGGER.warning("Skipping empty mart: %s", mart_name)
            continue

        sql_path = marts_dir / f"{mart_name}.sql"
        sql_content = _generate_sql(mart_name, dataframe)
        sql_path.write_text(sql_content, encoding="utf-8")
        rel = sql_path.relative_to(root_dir).as_posix()
        output_paths[mart_name] = rel
        LOGGER.info("SQL export written to %s", sql_path)

    return output_paths


def _generate_sql(table_name: str, dataframe: pd.DataFrame) -> str:
    """Generate DDL + INSERT statements for a single DataFrame."""
    lines: list[str] = [
        f"-- Quaero SQL export: {table_name}",
        f"-- Rows: {len(dataframe)}  |  Columns: {len(dataframe.columns)}",
        "",
        f"DROP TABLE IF EXISTS {table_name};",
        f"CREATE TABLE IF NOT EXISTS {table_name} (",
    ]

    col_defs = [f"    {col} {_infer_pg_type(dataframe[col])}" for col in dataframe.columns]
    lines.append(",\n".join(col_defs))
    lines.append(");")
    lines.append("")

    columns_csv = ", ".join(dataframe.columns)
    for batch_start in range(0, len(dataframe), _INSERT_BATCH_SIZE):
        batch = dataframe.iloc[batch_start : batch_start + _INSERT_BATCH_SIZE]
        value_rows = [
            "    (" + ", ".join(_format_value(v) for v in row) + ")"
            for _, row in batch.iterrows()
        ]
        lines.append(
            f"INSERT INTO {table_name} ({columns_csv}) VALUES\n"
            + ",\n".join(value_rows)
            + ";"
        )
        lines.append("")

    return "\n".join(lines)


def _infer_pg_type(series: pd.Series) -> str:
    if is_bool_dtype(series):
        return "BOOLEAN"
    if is_integer_dtype(series):
        return "BIGINT"
    if is_float_dtype(series):
        return "DOUBLE PRECISION"
    if is_datetime64_any_dtype(series):
        return "TIMESTAMPTZ"
    return "TEXT"


def _format_value(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return f"'{value.isoformat()}'"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"
