"""Future API contract for frontend-facing access to project artifacts.

This module intentionally does not expose a live HTTP server yet.
It documents the stable endpoint contract the repository expects to support
once a frontend or external consumer needs an application layer on top of the
generated project artifacts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


HttpMethod = Literal["GET", "POST"]


@dataclass(frozen=True)
class EndpointContract:
    """Documents one planned API endpoint and the project artifact it serves."""

    method: HttpMethod
    path: str
    artifact_source: str
    description: str


PROJECT_API_CONTRACT: tuple[EndpointContract, ...] = (
    EndpointContract(
        method="GET",
        path="/projects",
        artifact_source="projects/*/",
        description="List available generated projects.",
    ),
    EndpointContract(
        method="GET",
        path="/projects/{project}/summary",
        artifact_source="projects/{project}/metadata/final_summary_report.json",
        description="Return the generated project summary metadata.",
    ),
    EndpointContract(
        method="GET",
        path="/projects/{project}/marts",
        artifact_source="projects/{project}/marts/",
        description="List mart tables generated for the selected project.",
    ),
    EndpointContract(
        method="GET",
        path="/projects/{project}/marts/{mart_name}",
        artifact_source="projects/{project}/marts/{mart_name}.parquet",
        description="Return one mart table as a frontend-consumable dataset.",
    ),
    EndpointContract(
        method="POST",
        path="/projects/{project}/run",
        artifact_source="python -m app.main --config projects/{project}/project_config.json",
        description="Trigger a future on-demand project rerun.",
    ),
)


def describe_contract() -> tuple[EndpointContract, ...]:
    """Return the documented endpoint contract for future API work."""

    return PROJECT_API_CONTRACT


def implementation_status() -> str:
    """Expose the current status so docs and tests can assert intent explicitly."""

    return "planned"
