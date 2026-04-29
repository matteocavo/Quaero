"""Data lineage tracking utilities.

Builds a column-level lineage graph tracing how each column in a mart table
originates from the raw source through the staging layer.

Persisted as ``metadata/lineage.json`` using a simple node/edge structure
that can be rendered as a DAG in any graph tool (D3.js, Graphviz, etc.).

Node types:
  - ``raw``     : columns as they arrived in the source file
  - ``staging`` : columns after cleaning (name-normalised, typed)
  - ``mart``    : columns in the final analytical mart table

Edge types:
  - ``normalized``  : column name was changed during normalisation
  - ``type_cast``   : column dtype was changed during type enforcement
  - ``aggregated``  : column is an aggregation derived from a staging column
  - ``passthrough`` : column passed through unchanged
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from pipelines.utils import normalize_name, resolve_project_root


LOGGER = logging.getLogger(__name__)

_QUALITY_FLAG_COLUMNS = {"has_nulls", "invalid_date_parse", "type_conversion_issue"}


def build_lineage(
    raw_columns: list[str],
    staging_dataframe: pd.DataFrame,
    marts: dict[str, pd.DataFrame],
    cleaning_log: dict[str, Any],
    source_name: str,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Build and persist a column-level lineage document.

    Args:
        raw_columns: Column names as they appeared in the raw source.
        staging_dataframe: The cleaned staging DataFrame.
        marts: Mapping of mart name to mart DataFrame.
        cleaning_log: The cleaning_log dict returned by ``clean_dataframe``.
        source_name: Logical source name used for file naming.
        project_root: Optional project root for output resolution.

    Returns:
        The lineage dict (also written to ``metadata/lineage.json``).
    """
    root_dir = resolve_project_root(project_root, __file__)
    metadata_dir = root_dir / "metadata"
    metadata_dir.mkdir(parents=True, exist_ok=True)
    output_path = metadata_dir / "lineage.json"

    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []

    # Raw layer
    for col in raw_columns:
        nodes.append({"id": f"raw::{col}", "layer": "raw", "name": col})

    # Build lookup: normalized raw name → original raw name
    raw_normalized = {normalize_name(c): c for c in raw_columns}

    # Decisions index from cleaning log
    cast_decisions = {
        d["columns"][0]: d["action"]
        for d in cleaning_log.get("decisions", [])
        if d.get("columns") and d.get("action") in {"cast_to_numeric", "cast_to_datetime"}
    }

    # Staging layer
    staging_cols = [c for c in staging_dataframe.columns if c not in _QUALITY_FLAG_COLUMNS]
    for col in staging_cols:
        nodes.append(
            {
                "id": f"staging::{col}",
                "layer": "staging",
                "name": col,
                "dtype": str(staging_dataframe[col].dtype),
            }
        )
        raw_origin = raw_normalized.get(col)
        if raw_origin:
            edge_type = "type_cast" if col in cast_decisions else (
                "normalized" if raw_origin != col else "passthrough"
            )
            edges.append({"from": f"raw::{raw_origin}", "to": f"staging::{col}", "type": edge_type})

    # Mart layer
    for mart_name, mart_df in marts.items():
        if mart_df is None or mart_df.empty:
            continue
        for col in mart_df.columns:
            if col in _QUALITY_FLAG_COLUMNS:
                continue
            node_id = f"mart::{mart_name}::{col}"
            nodes.append(
                {
                    "id": node_id,
                    "layer": "mart",
                    "mart": mart_name,
                    "name": col,
                    "dtype": str(mart_df[col].dtype),
                }
            )
            if col in staging_cols:
                edges.append({"from": f"staging::{col}", "to": node_id, "type": "passthrough"})
            else:
                # Heuristic: find a staging column whose name is a substring of this mart column
                # e.g. staging::streams → mart::release_impact::avg_streams
                for s_col in staging_cols:
                    if s_col in col:
                        edges.append({"from": f"staging::{s_col}", "to": node_id, "type": "aggregated"})
                        break

    lineage: dict[str, Any] = {
        "source": normalize_name(source_name),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }

    output_path.write_text(json.dumps(lineage, indent=2), encoding="utf-8")
    LOGGER.info(
        "Lineage written to %s (%d nodes, %d edges)",
        output_path,
        len(nodes),
        len(edges),
    )

    return {**lineage, "output_path": output_path.relative_to(root_dir).as_posix()}
