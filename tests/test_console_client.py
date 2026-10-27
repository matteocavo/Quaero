"""Tests for the console HTTP client (no network: httpx.MockTransport)."""

from __future__ import annotations

import json

import httpx
import pytest

from console.client import (
    ApiError,
    AuthError,
    ConfigError,
    ConflictError,
    NotFoundError,
    QuaeroClient,
    ServiceUnavailable,
    ValidationRejected,
    is_loopback_url,
)


def _client(handler) -> QuaeroClient:
    return QuaeroClient(
        "http://127.0.0.1:8000", "secret-key", transport=httpx.MockTransport(handler)
    )


class TestConfiguration:
    def test_missing_key_is_a_config_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("QUAERO_API_KEY", raising=False)
        with pytest.raises(ConfigError, match="QUAERO_API_KEY"):
            QuaeroClient.from_env()

    def test_default_url_is_loopback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("QUAERO_API_KEY", "k")
        monkeypatch.delenv("QUAERO_API_URL", raising=False)
        assert QuaeroClient.from_env().base_url == "http://127.0.0.1:8000"

    def test_url_from_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("QUAERO_API_KEY", "k")
        monkeypatch.setenv("QUAERO_API_URL", "http://localhost:9000/")
        assert QuaeroClient.from_env().base_url == "http://localhost:9000"

    @pytest.mark.parametrize(
        "url", ["", "ftp://x", "localhost:8000", "file:///etc/passwd", "http://"]
    )
    def test_invalid_url_is_rejected(self, url: str) -> None:
        with pytest.raises(ConfigError):
            QuaeroClient(url, "k")

    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            ("http://127.0.0.1:8000", True),
            ("http://localhost:8000", True),
            ("http://[::1]:8000", True),
            ("https://api.example.com", False),
            ("http://192.168.1.10:8000", False),
        ],
    )
    def test_loopback_detection(self, url: str, expected: bool) -> None:
        assert is_loopback_url(url) is expected


class TestRequests:
    def test_api_key_header_is_sent(self) -> None:
        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["key"] = request.headers.get("X-API-Key", "")
            return httpx.Response(200, json={"projects": []})

        assert _client(handler).list_projects() == []
        assert seen["key"] == "secret-key"

    def test_project_names_are_url_quoted(self) -> None:
        seen: dict[str, str] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["path"] = request.url.raw_path.decode()
            return httpx.Response(200, json={})

        _client(handler).overview("a b/../c")
        assert "/projects/a%20b%2F..%2Fc/overview" == seen["path"]

    def test_start_run_body_with_and_without_url(self) -> None:
        bodies: list[dict] = []

        def handler(request: httpx.Request) -> httpx.Response:
            bodies.append(json.loads(request.content))
            return httpx.Response(202, json={"run_id": "r1"})

        client = _client(handler)
        client.start_run("demo", "q?")
        client.start_run("demo", "q?", "https://example.com/d.csv")
        assert bodies == [
            {"question": "q?"},
            {"question": "q?", "source_url": "https://example.com/d.csv"},
        ]

    def test_history_and_preview_send_query_params(self) -> None:
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(str(request.url.params))
            return httpx.Response(200, json={})

        client = _client(handler)
        client.run_history("demo", limit=7)
        client.mart_preview("demo", "mart_a", page=3, page_size=25)
        assert seen == ["limit=7", "page=3&page_size=25"]


class TestErrorMapping:
    @pytest.mark.parametrize(
        ("status", "error"),
        [
            (401, AuthError),
            (404, NotFoundError),
            (409, ConflictError),
            (400, ValidationRejected),
            (422, ValidationRejected),
            (503, ServiceUnavailable),
            (500, ApiError),
            (502, ApiError),
        ],
    )
    def test_status_codes(self, status: int, error: type[Exception]) -> None:
        client = _client(lambda request: httpx.Response(status, json={"detail": "boom"}))
        with pytest.raises(error) as info:
            client.overview("demo")
        assert info.value.detail == "boom"
        assert info.value.status_code == status

    def test_non_json_error_body(self) -> None:
        client = _client(lambda request: httpx.Response(500, text="<html>oops</html>"))
        with pytest.raises(ApiError) as info:
            client.overview("demo")
        assert info.value.detail == "HTTP 500"

    def test_structured_detail_is_stringified(self) -> None:
        client = _client(lambda request: httpx.Response(422, json={"detail": [{"msg": "bad"}]}))
        with pytest.raises(ValidationRejected) as info:
            client.start_run("demo", "q?")
        assert "bad" in info.value.detail

    @pytest.mark.parametrize("exc", [httpx.ConnectError("refused"), httpx.ReadTimeout("slow")])
    def test_network_failures_are_unavailable(self, exc: Exception) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise exc

        with pytest.raises(ServiceUnavailable):
            _client(handler).list_projects()
