"""Human review API (v0.9): sync, list, detail, decision, events. Local-first, declared reviewer."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_project_root, require_api_key
from api.schemas import (
    ReviewDecisionRequest,
    ReviewDecisionResponse,
    ReviewEvent,
    ReviewItemDetail,
    ReviewItemList,
    ReviewSyncRequest,
    ReviewSyncResponse,
)
from api.services import review as svc
from db import store

LOGGER = logging.getLogger(__name__)

router = APIRouter(
    prefix="/review",
    tags=["review"],
    dependencies=[Depends(require_api_key)],
)


def _http_error(exc: Exception) -> HTTPException:
    """Map domain errors to HTTP errors without leaking SQL or filesystem paths."""
    if isinstance(exc, svc.ReviewError):
        return HTTPException(exc.status_code, {"code": exc.code, "message": exc.message})
    LOGGER.exception("Unexpected review error")
    return HTTPException(500, {"code": "internal_error", "message": "Internal error."})


@router.post("/sync", response_model=ReviewSyncResponse)
def sync(
    body: ReviewSyncRequest, project_root: Path = Depends(get_project_root)
) -> ReviewSyncResponse:
    try:
        return ReviewSyncResponse(**svc.sync_project(project_root, body.project))
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/items", response_model=ReviewItemList)
def list_items(
    project: str | None = None,
    status: Literal["REVIEW_REQUIRED", "ACCEPTED", "CORRECTED", "REJECTED"] | None = None,
    kind: Literal["anomaly", "dq"] | None = None,
    run_id: str | None = None,
    superseded: bool | None = None,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> ReviewItemList:
    try:
        rows, total = store.list_review_items(
            project, status, kind, run_id, superseded, limit, offset
        )
        return ReviewItemList(
            items=[svc.item_view(r) for r in rows], total=total, limit=limit, offset=offset
        )
    except Exception as exc:
        raise _http_error(exc) from exc


def _item_or_404(item_id: int) -> dict:
    item = store.get_review_item(item_id)
    if item is None:
        raise HTTPException(404, {"code": "not_found", "message": "Review item not found."})
    return item


@router.get("/items/{item_id}", response_model=ReviewItemDetail)
def get_item(item_id: int) -> ReviewItemDetail:
    try:
        return ReviewItemDetail(**svc.item_view(_item_or_404(item_id), detail=True))
    except HTTPException:
        raise
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/items/{item_id}/decision", response_model=ReviewDecisionResponse)
def decide(item_id: int, body: ReviewDecisionRequest) -> ReviewDecisionResponse:
    try:
        item, event, replayed = svc.decide(item_id, body)
        return ReviewDecisionResponse(item=item, event=event, replayed=replayed)
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/items/{item_id}/events", response_model=list[ReviewEvent])
def get_events(item_id: int) -> list[ReviewEvent]:
    try:
        _item_or_404(item_id)
        return [ReviewEvent(**e) for e in store.list_review_events(item_id)]
    except HTTPException:
        raise
    except Exception as exc:
        raise _http_error(exc) from exc
