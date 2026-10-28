"""Tiny reader for the TypeScript response interfaces used by the web frontend.

Only supports the flat ``export interface Name { field?: type; ... }`` shape used in
``frontend/src/types/api.ts``. It lets tests compare the frontend's view of the API with the
actual FastAPI schemas without needing Node.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TYPES_FILE = REPO_ROOT / "frontend" / "src" / "types" / "api.ts"
CLIENT_FILE = REPO_ROOT / "frontend" / "src" / "services" / "apiClient.ts"

_INTERFACE = re.compile(r"export interface (\w+)(?:<[^>]*>)? \{(.*?)\n\}", re.DOTALL)
_FIELD = re.compile(r"^\s*(\w+)(\?)?\s*:", re.MULTILINE)


def read_interfaces() -> dict[str, dict[str, bool]]:
    """Return ``{interface: {field: is_optional}}`` from the frontend type definitions."""
    text = TYPES_FILE.read_text(encoding="utf-8")
    interfaces: dict[str, dict[str, bool]] = {}
    for name, body in _INTERFACE.findall(text):
        interfaces[name] = {field: bool(opt) for field, opt in _FIELD.findall(body)}
    return interfaces


_REQUEST_CALL = re.compile(r"this\.request<\w+>\(\s*[`'\"]([^`'\"]+)[`'\"]", re.DOTALL)


def read_client_paths() -> set[str]:
    """Return the API paths the frontend client calls, normalised to OpenAPI style."""
    text = CLIENT_FILE.read_text(encoding="utf-8")
    paths: set[str] = set()
    for raw in _REQUEST_CALL.findall(text):
        path = raw.split("?")[0]
        path = re.sub(
            r"\$\{encodeURIComponent\((\w+)\)\}", lambda m: "{" + _param(m.group(1)) + "}", path
        )
        paths.add(path)
    return paths


def _param(name: str) -> str:
    return {
        "project": "project",
        "mart": "mart",
        "runId": "run_id",
        "uploadId": "upload_id",
    }.get(name, name)
