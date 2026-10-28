"""Router for local CSV dataset uploads."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status

from api.dependencies import get_project_root, require_api_key
from api.schemas import (
    InterpretationRequest,
    QuestionInterpretation,
    UploadResponse,
    UploadsResponse,
)
from api.services.interpretation import interpret_upload_question
from api.services.uploads import (
    UploadTooLargeError,
    UploadValidationError,
    get_upload,
    list_uploads,
    store_uploaded_csv,
)

router = APIRouter(
    tags=["uploads"],
    dependencies=[Depends(require_api_key)],
)


@router.post(
    "/uploads",
    response_model=UploadResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Upload and validate a local CSV dataset",
)
async def upload_csv_dataset(
    project_root: Annotated[Path, Depends(get_project_root)],
    file: UploadFile = File(..., description="CSV file to validate and store locally."),
) -> UploadResponse:
    """Validate and store a local CSV file in the untracked ``uploads/`` workspace."""
    try:
        return await store_uploaded_csv(project_root=project_root, upload_file=file)
    except UploadTooLargeError as exc:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail=str(exc),
        ) from exc
    except UploadValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get(
    "/uploads",
    response_model=UploadsResponse,
    summary="List validated local CSV uploads",
)
def get_uploads(
    project_root: Annotated[Path, Depends(get_project_root)],
) -> UploadsResponse:
    """Return metadata for all validated CSV uploads available in the local workspace."""
    return UploadsResponse(uploads=list_uploads(project_root))


@router.get(
    "/uploads/{upload_id}",
    response_model=UploadResponse,
    summary="Get metadata for a single uploaded CSV dataset",
)
def get_upload_detail(
    upload_id: str,
    project_root: Annotated[Path, Depends(get_project_root)],
) -> UploadResponse:
    """Return metadata for a validated CSV upload by its safe upload identifier."""
    try:
        return get_upload(project_root, upload_id)
    except (UploadValidationError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post(
    "/uploads/{upload_id}/interpretation",
    response_model=QuestionInterpretation,
    summary="Preview how a question will be answered on an upload, or why it cannot be",
)
def interpret_upload(
    upload_id: str,
    payload: InterpretationRequest,
    project_root: Annotated[Path, Depends(get_project_root)],
) -> QuestionInterpretation:
    """Say which measure, grouping and aggregation a question maps to, what was chosen by the user and what was
    inferred, and which parts of the question Quaero cannot answer. Nothing is created or run."""
    try:
        return interpret_upload_question(project_root, upload_id, payload)
    except (UploadValidationError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
