"""Anomaly Framing v2.

Reads the existing ``anomaly_report_<source>.json`` produced by
``kpi_engine.anomaly_detection`` and enriches each anomaly entry with:

- ``frequency``       : fraction of total rows that are flagged (0.0–1.0)
- ``impact``          : "low" / "medium" / "high" derived from frequency
- ``suggested_action``: deterministic guidance based on impact and method

The enriched report also receives a top-level ``business_summary`` field with
a one-sentence human-readable overview of the anomaly situation.

Output: ``metadata/anomaly_report_v2_<source>.json``

This module is backward-compatible — the original v1 report is unchanged.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from pipelines.utils import normalize_name, resolve_project_root


LOGGER = logging.getLogger(__name__)

# Fraction of total rows flagged that determines impact tier
_HIGH_THRESHOLD: float = 0.10  # > 10 % → high
_MEDIUM_THRESHOLD: float = 0.02  # 2–10 % → medium; < 2 % → low


def frame_anomalies(
    source_name: str,
    total_row_count: int,
    project_root: str | Path | None = None,
) -> dict[str, Any]:
    """Enrich ``anomaly_report_<source>.json`` into ``anomaly_report_v2_<source>.json``.

    Args:
        source_name: Logical source name used for file naming.
        total_row_count: Row count of the analysed dataset; used to compute
            per-anomaly frequency ratios.
        project_root: Optional project root for file resolution.

    Returns:
        Dict with:
        - ``report``      : the enriched v2 report dict (or ``None`` when skipped)
        - ``output_path`` : relative path to the written file
        - ``skipped``     : present and ``True`` when the v1 report was absent
    """
    root_dir = resolve_project_root(project_root, __file__)
    normalized = normalize_name(source_name)
    metadata_dir = root_dir / "metadata"

    v1_path = metadata_dir / f"anomaly_report_{normalized}.json"
    output_path = metadata_dir / f"anomaly_report_v2_{normalized}.json"
    relative_output = output_path.relative_to(root_dir).as_posix()

    if not v1_path.exists():
        LOGGER.warning(
            "anomaly_report_%s.json not found — skipping framing", normalized
        )
        return {"report": None, "output_path": relative_output, "skipped": True}

    v1_report: dict[str, Any] = json.loads(v1_path.read_text(encoding="utf-8"))
    enriched_anomalies = [
        _enrich_anomaly(entry, total_row_count)
        for entry in v1_report.get("anomalies", [])
    ]

    v2_report: dict[str, Any] = {
        **v1_report,
        "anomalies": enriched_anomalies,
        "business_summary": _build_business_summary(
            anomaly_row_count=v1_report.get("anomaly_row_count", 0),
            total_row_count=total_row_count,
            enriched_anomalies=enriched_anomalies,
        ),
    }

    output_path.write_text(json.dumps(v2_report, indent=2), encoding="utf-8")
    LOGGER.info("Anomaly Framing v2 written to %s", output_path)

    return {"report": v2_report, "output_path": relative_output}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _enrich_anomaly(entry: dict[str, Any], total_row_count: int) -> dict[str, Any]:
    """Return the anomaly entry with ``frequency``, ``impact``, and
    ``suggested_action`` fields added.
    """
    anomaly_count: int = entry.get("anomaly_count", 0)
    frequency = anomaly_count / total_row_count if total_row_count > 0 else 0.0
    impact = _classify_impact(frequency)
    column: str = entry.get("column", "unknown")
    method: str = entry.get("method", "iqr")

    return {
        **entry,
        "frequency": round(frequency, 4),
        "impact": impact,
        "suggested_action": _suggest_action(
            column=column, impact=impact, method=method
        ),
    }


def _classify_impact(frequency: float) -> str:
    """Classify anomaly impact based on the fraction of flagged rows.

    Thresholds:
    - >= 10 % → "high"
    -  2–10 % → "medium"
    -   < 2 % → "low"
    """
    if frequency >= _HIGH_THRESHOLD:
        return "high"
    if frequency >= _MEDIUM_THRESHOLD:
        return "medium"
    return "low"


def _suggest_action(column: str, impact: str, method: str) -> str:
    """Generate a deterministic suggested action string for a single anomaly."""
    if impact == "high":
        return (
            f"Column `{column}` has a high concentration of {method}-detected outliers. "
            "Review the distribution before aggregating — consider capping extreme values "
            "or investigating the upstream data source for collection errors."
        )
    if impact == "medium":
        return (
            f"Column `{column}` contains a moderate number of {method}-detected outliers. "
            "Verify whether these are valid edge cases or data quality issues before "
            "including them in summary statistics or KPI calculations."
        )
    return (
        f"Column `{column}` has isolated {method}-detected outliers (low frequency). "
        "These are unlikely to materially affect aggregated metrics; monitor over time "
        "and re-evaluate if the dataset grows."
    )


def _build_business_summary(
    anomaly_row_count: int,
    total_row_count: int,
    enriched_anomalies: list[dict[str, Any]],
) -> str:
    """Build a single top-level sentence summarising the anomaly situation."""
    if not enriched_anomalies or anomaly_row_count == 0:
        return "No anomalies were detected in this dataset."

    affected_pct = (
        round(100.0 * anomaly_row_count / total_row_count, 1)
        if total_row_count > 0
        else 0.0
    )
    high_impact = [a for a in enriched_anomalies if a["impact"] == "high"]
    medium_impact = [a for a in enriched_anomalies if a["impact"] == "medium"]

    if high_impact:
        columns = ", ".join(f"`{a['column']}`" for a in high_impact[:3])
        return (
            f"{anomaly_row_count} rows ({affected_pct}%) contain anomalies; "
            f"high-impact columns requiring immediate attention: {columns}."
        )
    if medium_impact:
        columns = ", ".join(f"`{a['column']}`" for a in medium_impact[:3])
        return (
            f"{anomaly_row_count} rows ({affected_pct}%) contain anomalies; "
            f"medium-impact columns to review before publishing metrics: {columns}."
        )
    return (
        f"{anomaly_row_count} rows ({affected_pct}%) contain low-frequency anomalies "
        f"across {len(enriched_anomalies)} column(s) — no immediate action required."
    )
