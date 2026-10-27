"""Health check endpoint — public, no authentication required."""

from __future__ import annotations

from fastapi import APIRouter

from api.schemas import HealthResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    """Return API health status.  No authentication required."""
    return HealthResponse()
