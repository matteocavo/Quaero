"""Router for the local data lifecycle: storage status, cleanup policy and confirmed deletion."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, status

from api.dependencies import get_project_root, require_api_key
from api.schemas import (
    CleanupRequest,
    CleanupResponse,
    LifecyclePolicyRequest,
    LifecyclePolicyResponse,
    StorageStatusResponse,
)
from api.services import lifecycle
from api.services.lifecycle import LifecycleError, LifecycleNotFound, LifecycleProtected

router = APIRouter(tags=["storage"], dependencies=[Depends(require_api_key)])


@router.get(
    "/storage",
    response_model=StorageStatusResponse,
    summary="Space used by local uploads and projects",
)
def get_storage(project_root: Path = Depends(get_project_root)) -> StorageStatusResponse:
    return lifecycle.get_storage_status(project_root)


@router.post(
    "/storage/policy",
    response_model=LifecyclePolicyResponse,
    summary="Choose what happens to temporary uploads on exit",
)
def post_policy(
    body: LifecyclePolicyRequest,
    project_root: Path = Depends(get_project_root),
) -> LifecyclePolicyResponse:
    try:
        return LifecyclePolicyResponse(
            lifecycle_policy=lifecycle.set_policy(project_root, body.lifecycle_policy)
        )
    except LifecycleProtected as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except LifecycleError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc


@router.post(
    "/storage/cleanup",
    response_model=CleanupResponse,
    summary="Permanently delete temporary data (needs confirm=true)",
)
def post_cleanup(
    body: CleanupRequest,
    project_root: Path = Depends(get_project_root),
) -> CleanupResponse:
    try:
        return lifecycle.execute_cleanup(project_root, body.scope, body.confirm, body.target_id)
    except LifecycleNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except LifecycleProtected as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except LifecycleError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)
        ) from exc
