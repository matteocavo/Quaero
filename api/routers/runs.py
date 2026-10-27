"""Endpoints for triggering and monitoring pipeline runs."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status

from api.dependencies import get_project_root, require_api_key
from api.schemas import RunAcceptedResponse, RunRequest, RunStatus, RunStatusResponse
from api.services.runner import create_run, execute_run, get_run

router = APIRouter(
    prefix="/projects",
    tags=["runs"],
    dependencies=[Depends(require_api_key)],
)


@router.post(
    "/{project}/run",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=RunAcceptedResponse,
)
async def trigger_run(
    project: str,
    payload: RunRequest,
    background_tasks: BackgroundTasks,
    project_root: Path = Depends(get_project_root),
) -> RunAcceptedResponse:
    """Trigger an async pipeline run for an existing project.

    Returns HTTP 202 immediately with a ``run_id`` for polling.

    Limitations
    -----------
    * Uses FastAPI BackgroundTasks: the run is tied to the API process.
      A process crash will leave the run in RUNNING state indefinitely;
      restart the API and resubmit.
    * Only one active (QUEUED or RUNNING) run is allowed per project at a time.
    """
    try:
        run_state = create_run(project_root, project)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc

    source_url = str(payload.source_url) if payload.source_url else None

    background_tasks.add_task(
        execute_run,
        project_root=project_root,
        run_id=run_state["run_id"],
        project=project,
        question=payload.question,
        source_url=source_url,
    )

    return RunAcceptedResponse(
        run_id=run_state["run_id"],
        project=project,
        status=RunStatus.QUEUED,
    )


@router.get(
    "/{project}/runs/{run_id}",
    response_model=RunStatusResponse,
)
async def get_run_status(
    project: str,
    run_id: str,
    project_root: Path = Depends(get_project_root),
) -> RunStatusResponse:
    """Return the current status of a pipeline run."""
    run_state = get_run(project_root, run_id)
    if run_state is None or run_state.get("project") != project:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Run '{run_id}' not found for project '{project}'.",
        )
    return RunStatusResponse(
        run_id=run_state["run_id"],
        project=run_state["project"],
        status=RunStatus(run_state["status"]),
        error=run_state.get("error"),
        result_keys=run_state.get("result_keys", []),
    )
