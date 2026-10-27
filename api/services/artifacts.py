"""Read-only access helpers for Quaero project artifacts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from api.schemas import MartInfo, ProjectSummary

# ---------------------------------------------------------------------------
# Path safety
# ---------------------------------------------------------------------------

_ALLOWED_NAME_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")


def _validate_identifier(value: str, label: str = "identifier") -> str:
    """Reject identifiers that contain path-traversal characters."""
    if not value or not all(c in _ALLOWED_NAME_CHARS for c in value):
        raise ValueError(
            f"Invalid {label} '{value}': only letters, digits, underscores and hyphens are allowed."
        )
    return value


def _safe_project_dir(project_root: Path, project: str) -> Path:
    """Return the project directory after verifying it stays inside project_root."""
    project = _validate_identifier(project, "project name")
    candidate = (project_root / "projects" / project).resolve()
    if not candidate.is_relative_to(project_root):
        raise PermissionError(f"Path traversal detected for project '{project}'.")
    return candidate


def _safe_mart_path(project_dir: Path, mart_name: str) -> Path:
    """Return the Parquet path for a mart after verifying it stays inside project_dir."""
    mart_name = _validate_identifier(mart_name, "mart name")
    candidate = (project_dir / "marts" / mart_name).with_suffix(".parquet")
    candidate = candidate.resolve()
    if not candidate.is_relative_to(project_dir):
        raise PermissionError(f"Path traversal detected for mart '{mart_name}'.")
    return candidate


# ---------------------------------------------------------------------------
# Projects
# ---------------------------------------------------------------------------


def list_projects(project_root: Path) -> list[ProjectSummary]:
    """Return a summary for every project directory that exists under projects/."""
    projects_dir = project_root / "projects"
    if not projects_dir.is_dir():
        return []

    results: list[ProjectSummary] = []
    for entry in sorted(projects_dir.iterdir()):
        if not entry.is_dir():
            continue
        marts_dir = entry / "marts"
        mart_count = len(list(marts_dir.glob("*.parquet"))) if marts_dir.is_dir() else 0
        summary_path = entry / "metadata" / "final_summary_report.json"
        results.append(
            ProjectSummary(
                project=entry.name,
                marts_count=mart_count,
                has_summary=summary_path.is_file(),
            )
        )
    return results


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------


def read_project_summary(project_root: Path, project: str) -> dict[str, Any]:
    """Return the parsed final_summary_report.json for *project*."""
    project_dir = _safe_project_dir(project_root, project)
    if not project_dir.is_dir():
        raise FileNotFoundError(f"Project '{project}' not found.")

    summary_path = project_dir / "metadata" / "final_summary_report.json"
    if not summary_path.is_file():
        raise FileNotFoundError(f"No summary report found for project '{project}'.")

    with summary_path.open(encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Marts
# ---------------------------------------------------------------------------


def list_marts(project_root: Path, project: str) -> list[MartInfo]:
    """Return metadata for every Parquet mart in the project."""
    project_dir = _safe_project_dir(project_root, project)
    if not project_dir.is_dir():
        raise FileNotFoundError(f"Project '{project}' not found.")

    marts_dir = project_dir / "marts"
    if not marts_dir.is_dir():
        return []

    results: list[MartInfo] = []
    for parquet_file in sorted(marts_dir.glob("*.parquet")):
        results.append(
            MartInfo(
                name=parquet_file.stem,
                path=str(parquet_file.relative_to(project_root)),
            )
        )
    return results


def read_mart_preview(
    project_root: Path,
    project: str,
    mart_name: str,
    page: int = 1,
    page_size: int = 100,
) -> dict[str, Any]:
    """Return a paginated JSON preview of a Parquet mart."""
    if page < 1:
        raise ValueError("page must be >= 1")
    if not 1 <= page_size <= 1000:
        raise ValueError("page_size must be between 1 and 1000")

    project_dir = _safe_project_dir(project_root, project)
    if not project_dir.is_dir():
        raise FileNotFoundError(f"Project '{project}' not found.")

    mart_path = _safe_mart_path(project_dir, mart_name)
    if not mart_path.is_file():
        raise FileNotFoundError(f"Mart '{mart_name}' not found in project '{project}'.")

    # Read only the columns metadata first to avoid loading the full dataset
    df = pd.read_parquet(mart_path)
    total_rows = len(df)

    offset = (page - 1) * page_size
    page_df = df.iloc[offset : offset + page_size]

    # Convert to JSON-serialisable format (handle NaN, Timestamps, etc.)
    rows = json.loads(page_df.to_json(orient="records", date_format="iso"))

    return {
        "project": project,
        "mart": mart_name,
        "columns": list(df.columns),
        "rows": rows,
        "total_rows": total_rows,
        "page": page,
        "page_size": page_size,
    }
