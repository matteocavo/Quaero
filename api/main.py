"""FastAPI application factory for the Quaero 2.0 API layer.

Start the server locally
------------------------
::

    # Install dependencies
    pip install fastapi uvicorn

    # Set the API key (required — no token = no access to protected endpoints)
    export QUAERO_API_KEY=your-secret-token-here

    # Optional: point to a custom project root
    # export QUAERO_PROJECT_ROOT=/path/to/quaero/repo

    # Start
    uvicorn api.main:app --reload --port 8000

PowerShell equivalent
---------------------
::

    $env:QUAERO_API_KEY="your-secret-token-here"
    uvicorn api.main:app --reload --port 8000

Request example (PowerShell)
-----------------------------
::

    $headers = @{"X-API-Key" = "your-secret-token-here"}
    Invoke-RestMethod -Uri "http://localhost:8000/projects" -Headers $headers

    Invoke-RestMethod -Uri "http://localhost:8000/health"

Retool / n8n integration note
------------------------------
Both tools support HTTP / REST resources with custom headers.  Configure the
``X-API-Key`` header in the resource/credential definition and point it at
``http://localhost:8000`` (local) or the deployed URL (future phases).
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.routers import health, projects, runs

_LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _lifespan(app: FastAPI):
    """Initialise the SQLite persistence layer on startup (idempotent)."""
    try:
        from db import store

        store.init_db()
        _LOGGER.info("DB: persistence layer initialised.")
    except Exception as exc:
        _LOGGER.warning("DB: could not initialise persistence layer: %s", exc)
    yield


app = FastAPI(
    lifespan=_lifespan,
    title="Quaero API",
    description=(
        "REST interface over the Quaero analytics pipeline. "
        "All endpoints except /health require the X-API-Key header."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------

_CORS_ORIGINS_ENV = "QUAERO_CORS_ORIGINS"
_DEFAULT_CORS_ORIGINS: list[str] = ["http://localhost:3000", "http://localhost:8080"]


def _get_cors_origins() -> list[str]:
    env_val = os.environ.get(_CORS_ORIGINS_ENV, "")
    if env_val.strip():
        return [o.strip() for o in env_val.split(",") if o.strip()]
    return _DEFAULT_CORS_ORIGINS


app.add_middleware(
    CORSMiddleware,
    allow_origins=_get_cors_origins(),
    allow_credentials=False,  # credentials=True + wildcard is disallowed by browsers
    allow_methods=["GET", "POST"],
    allow_headers=["X-API-Key", "Content-Type"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(health.router)
app.include_router(projects.router)
app.include_router(runs.router)

# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all handler that avoids leaking internal tracebacks to clients."""
    return JSONResponse(
        status_code=500,
        content={"detail": "An internal server error occurred."},
    )
