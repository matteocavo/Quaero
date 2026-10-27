"""FastAPI dependency providers for authentication and project root resolution."""

from __future__ import annotations

import os
import secrets
from pathlib import Path

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

_API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
_ENV_KEY = "QUAERO_API_KEY"


def _get_configured_key() -> str | None:
    """Return the API key from the environment, or None if not configured."""
    return os.environ.get(_ENV_KEY)


async def require_api_key(api_key: str | None = Security(_API_KEY_HEADER)) -> None:
    """Dependency that enforces API key authentication.

    If ``QUAERO_API_KEY`` is not set the application starts in *insecure* mode
    and rejects every request to protected endpoints with a 503 so that no data
    is inadvertently exposed without a secret in place.
    """
    configured = _get_configured_key()
    if not configured:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                "API authentication is not configured. "
                "Set the QUAERO_API_KEY environment variable before starting the server."
            ),
        )
    if api_key is None or not secrets.compare_digest(api_key, configured):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key.",
            headers={"WWW-Authenticate": "ApiKey"},
        )


# ---------------------------------------------------------------------------
# Project root
# ---------------------------------------------------------------------------

_PROJECT_ROOT_ENV = "QUAERO_PROJECT_ROOT"


def get_project_root() -> Path:
    """Return the repository root used for resolving project artifacts."""
    env_val = os.environ.get(_PROJECT_ROOT_ENV)
    if env_val:
        return Path(env_val).resolve()
    # Default: two levels up from this file (api/ → repo root)
    return Path(__file__).resolve().parents[1]
