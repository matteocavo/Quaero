"""Preview how a question will be answered on an uploaded CSV, before any project is created or run.

The data is prepared with the same pure cleaning steps the pipeline applies (column names, missing values,
types, dates), so the interpretation shown here is the one the pipeline will use. Nothing is written.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from api.schemas import InterpretationRequest, QuestionInterpretation
from api.services.uploads import _safe_upload_paths, get_upload
from kpi_engine.question_interpreter import interpret_question
from pipelines.cleaning import (
    _apply_date_parsing,
    _apply_type_enforcement,
    _standardize_missing_values,
)
from pipelines.utils import normalize_name


def _prepared_frame(csv_path: Path) -> pd.DataFrame:
    frame = pd.read_csv(csv_path)
    frame.columns = [normalize_name(str(column)) for column in frame.columns]
    frame = _standardize_missing_values(frame)
    frame, _ = _apply_type_enforcement(frame)
    frame, _ = _apply_date_parsing(frame)
    return frame


def _column(name: str | None) -> str | None:
    return normalize_name(name.strip()) if name and name.strip() else None


def interpret_upload_question(
    project_root: Path,
    upload_id: str,
    request: InterpretationRequest,
) -> QuestionInterpretation:
    """Interpret ``request.question`` against the upload; raises ``FileNotFoundError`` if it does not exist."""
    get_upload(project_root, upload_id)  # validates the id and that the upload exists
    csv_path, _ = _safe_upload_paths(project_root, upload_id)
    result = interpret_question(
        request.question,
        _prepared_frame(csv_path),
        metric_column=_column(request.metric_column),
        dimension_column=_column(request.dimension_column),
        aggregation=request.aggregation,
        time_column=_column(request.time_column),
    )
    return QuestionInterpretation.model_validate(result)
