"""Thin HTTP client used by the operations console.

The console never reads artifacts or the database directly: every piece of data comes from
the Quaero FastAPI. This keeps the API the single source of truth (and lets any other
frontend reuse the same endpoints).

Configuration comes from the environment only; nothing is stored in the repository:

* ``QUAERO_API_URL`` - base URL of the API (default ``http://127.0.0.1:8000``)
* ``QUAERO_API_KEY`` - value for the ``X-API-Key`` header (required)
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote, urlparse

import httpx

DEFAULT_API_URL = "http://127.0.0.1:8000"
DEFAULT_TIMEOUT = 15.0

_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


class ConsoleError(Exception):
    """Base class for errors raised by the console client."""

    def __init__(self, detail: str, status_code: int | None = None) -> None:
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code


class ConfigError(ConsoleError):
    """The console is not configured (missing key, invalid URL)."""


class AuthError(ConsoleError):
    """The API rejected the credentials (HTTP 401)."""


class NotFoundError(ConsoleError):
    """The resource does not exist (HTTP 404); often a normal state for artifacts."""


class ConflictError(ConsoleError):
    """A run is already active for the project (HTTP 409)."""


class ValidationRejected(ConsoleError):
    """The API rejected the input (HTTP 400 / 422)."""


class ServiceUnavailable(ConsoleError):
    """The API, or the store behind it, is unavailable (HTTP 503, network error, timeout)."""


class ApiError(ConsoleError):
    """Any other unexpected API failure (HTTP 5xx)."""


def is_loopback_url(url: str) -> bool:
    """True when *url* points at this machine."""
    return (urlparse(url).hostname or "") in _LOOPBACK_HOSTS


def _detail(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return f"HTTP {response.status_code}"
    detail = body.get("detail") if isinstance(body, dict) else None
    if isinstance(detail, str):
        return detail
    if detail is not None:
        return str(detail)
    return f"HTTP {response.status_code}"


class QuaeroClient:
    """Synchronous client for the Quaero API."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        timeout: float = DEFAULT_TIMEOUT,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        parsed = urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ConfigError(f"Invalid API URL: {base_url!r}")
        if not api_key:
            raise ConfigError("QUAERO_API_KEY is not set.")
        self.base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._timeout = timeout
        self._transport = transport

    @classmethod
    def from_env(cls) -> QuaeroClient:
        return cls(
            base_url=os.environ.get("QUAERO_API_URL", DEFAULT_API_URL),
            api_key=os.environ.get("QUAERO_API_KEY", ""),
        )

    # ------------------------------------------------------------------ core

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            with httpx.Client(
                base_url=self.base_url,
                headers={"X-API-Key": self._api_key},
                timeout=self._timeout,
                transport=self._transport,
            ) as http:
                response = http.request(method, path, **kwargs)
        except httpx.HTTPError as exc:
            raise ServiceUnavailable(
                f"Cannot reach the Quaero API: {exc.__class__.__name__}"
            ) from exc

        if response.is_success:
            return response.json()

        detail = _detail(response)
        code = response.status_code
        if code == 401:
            raise AuthError(detail, code)
        if code == 404:
            raise NotFoundError(detail, code)
        if code == 409:
            raise ConflictError(detail, code)
        if code in {400, 422}:
            raise ValidationRejected(detail, code)
        if code == 503:
            raise ServiceUnavailable(detail, code)
        raise ApiError(detail, code)

    @staticmethod
    def _p(project: str) -> str:
        return quote(project, safe="")

    # ------------------------------------------------------------- endpoints

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/health")

    def list_projects(self) -> list[dict[str, Any]]:
        return self._request("GET", "/projects")["projects"]

    def overview(self, project: str) -> dict[str, Any]:
        return self._request("GET", f"/projects/{self._p(project)}/overview")

    def run_history(self, project: str, limit: int = 50) -> dict[str, Any]:
        return self._request("GET", f"/projects/{self._p(project)}/runs", params={"limit": limit})

    def run_status(self, project: str, run_id: str) -> dict[str, Any]:
        return self._request("GET", f"/projects/{self._p(project)}/runs/{self._p(run_id)}")

    def start_run(
        self, project: str, question: str, source_url: str | None = None
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"question": question}
        if source_url:
            body["source_url"] = source_url
        return self._request("POST", f"/projects/{self._p(project)}/run", json=body)

    def list_marts(self, project: str) -> list[dict[str, Any]]:
        return self._request("GET", f"/projects/{self._p(project)}/marts")["marts"]

    def mart_preview(
        self, project: str, mart: str, page: int = 1, page_size: int = 100
    ) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/projects/{self._p(project)}/marts/{self._p(mart)}",
            params={"page": page, "page_size": page_size},
        )

    def data_quality(self, project: str) -> dict[str, Any]:
        return self._request("GET", f"/projects/{self._p(project)}/data-quality")

    def anomalies(self, project: str) -> dict[str, Any]:
        return self._request("GET", f"/projects/{self._p(project)}/anomalies")

    def lineage(self, project: str) -> dict[str, Any]:
        return self._request("GET", f"/projects/{self._p(project)}/lineage")
