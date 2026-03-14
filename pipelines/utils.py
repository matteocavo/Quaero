"""Shared path and naming helpers for pipeline modules."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any


def resolve_project_root(
    project_root: str | Path | None,
    current_file: str | Path,
) -> Path:
    """Resolve a project root, defaulting to the repository root for a module file."""
    if project_root is not None:
        return Path(project_root).expanduser().resolve()

    return Path(current_file).resolve().parents[1]


def normalize_name(value: Any) -> str:
    """Normalize a value to stable snake_case for paths and identifiers."""
    normalized = str(value).strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        raise ValueError("Normalized name cannot be empty.")
    return normalized


DISPLAY_TOKEN_OVERRIDES = {
    "ai": "AI",
    "api": "API",
    "bi": "BI",
    "co2": "CO2",
    "csv": "CSV",
    "eu": "EU",
    "gdp": "GDP",
    "json": "JSON",
    "kpi": "KPI",
    "nasa": "NASA",
    "nsidc": "NSIDC",
    "owid": "OWID",
    "sql": "SQL",
    "uk": "UK",
    "ui": "UI",
    "us": "US",
    "usd": "USD",
}


def humanize_name(value: Any) -> str:
    """Convert a project or dataset identifier into a human-readable title."""
    normalized = normalize_name(value)
    parts = normalized.split("_")
    return " ".join(
        DISPLAY_TOKEN_OVERRIDES.get(part, part.capitalize()) for part in parts
    )


def format_analysis_title(value: Any) -> str:
    """Render a README heading title without duplicating a trailing Analysis token."""
    title = humanize_name(value)
    if title == "Analysis" or title.endswith(" Analysis"):
        return title
    return f"{title} Analysis"
