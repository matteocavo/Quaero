"""The web frontend's TypeScript types must match the real FastAPI schemas and routes."""

from __future__ import annotations

import pytest
from _ts_interfaces import CLIENT_FILE, TYPES_FILE, read_client_paths, read_interfaces
from pydantic import BaseModel

from api import schemas
from api.main import app

pytestmark = pytest.mark.skipif(
    not (TYPES_FILE.is_file() and CLIENT_FILE.is_file()),
    reason="frontend/ is not present",
)

# Frontend-only types that have no backend response model.
FRONTEND_ONLY = {"MetricDefinition"}


def _fields(model: type[BaseModel]) -> set[str]:
    return {info.alias or name for name, info in model.model_fields.items()}


def test_every_frontend_interface_has_a_matching_backend_schema() -> None:
    interfaces = read_interfaces()
    assert len(interfaces) >= 20
    checked = 0
    for name, ts_fields in interfaces.items():
        if name in FRONTEND_ONLY:
            continue
        model = getattr(schemas, name, None)
        assert model is not None, f"frontend interface {name} has no backend schema"
        assert set(ts_fields) == _fields(model), (
            f"{name}: frontend fields {sorted(ts_fields)} != backend fields {sorted(_fields(model))}"
        )
        checked += 1
    assert checked >= 20


def test_frontend_optional_fields_are_nullable_or_defaulted_in_the_backend() -> None:
    """A field the frontend treats as optional must never be required-and-non-null server side."""
    for name, ts_fields in read_interfaces().items():
        model = getattr(schemas, name, None)
        if model is None:
            continue
        by_alias = {info.alias or field: info for field, info in model.model_fields.items()}
        for field, optional in ts_fields.items():
            if optional:
                assert not by_alias[field].is_required(), f"{name}.{field} is required in Pydantic"


def test_frontend_only_calls_endpoints_that_exist() -> None:
    openapi_paths = set(app.openapi()["paths"])
    client_paths = read_client_paths()
    assert len(client_paths) == 14
    missing = client_paths - openapi_paths
    assert not missing, f"frontend calls endpoints the API does not have: {sorted(missing)}"


def test_run_request_matches_the_frontend_payload() -> None:
    interfaces = read_interfaces()
    assert set(interfaces["RunRequest"]) == _fields(schemas.RunRequest)


def test_api_exposes_upload_and_project_creation_endpoints() -> None:
    """Verifies that local CSV upload and project creation endpoints match the frontend contract."""
    openapi = app.openapi()
    paths = openapi["paths"]
    assert set(paths["/uploads"].keys()) == {"get", "post"}
    assert set(paths["/uploads/{upload_id}"].keys()) == {"get"}
    assert set(paths["/projects"].keys()) == {"get", "post"}
    interfaces = read_interfaces()
    assert set(interfaces["ProjectCreateRequest"]) == _fields(schemas.ProjectCreateRequest)
