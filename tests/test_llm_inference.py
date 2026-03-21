from __future__ import annotations

import os
from unittest.mock import patch

import pandas as pd
import pytest

from kpi_engine.metric_inference import infer_dimension_column, infer_metric_column


@pytest.fixture
def mock_metric_df() -> pd.DataFrame:
    """A dataframe where deterministic metric inference is ambiguous."""
    return pd.DataFrame(
        {
            "q_gdp_pc_cd": [1, 2, 3],
            "idx_val": [4, 5, 6],
            "category_code": ["A", "B", "C"],
            "unrelated_col": [10, 20, 30],
        }
    )


@pytest.fixture
def mock_dimension_df() -> pd.DataFrame:
    """A dataframe where deterministic dimension inference is ambiguous."""
    return pd.DataFrame(
        {
            "idx_val": [4, 5, 6],
            "alpha_group": ["A", "B", "C"],
            "beta_group": ["X", "Y", "Z"],
            "other_value": [10, 20, 30],
        }
    )


@patch.dict(os.environ, {"QUAERO_ANTHROPIC_API_KEY": "fake_key"})
@patch("kpi_engine.llm_inference.infer_column_with_llm", return_value="idx_val")
def test_llm_metric_inference_fallback_success(
    mock_llm, mock_metric_df: pd.DataFrame
) -> None:
    result = infer_metric_column("Which index value is highest?", mock_metric_df)

    assert result["metric_column"] == "idx_val"
    assert result["selection_mode"] == "llm_assisted"
    mock_llm.assert_called_once()


@patch.dict(os.environ, {"QUAERO_ANTHROPIC_API_KEY": "fake_key"})
@patch("kpi_engine.llm_inference.infer_column_with_llm", return_value="alpha_group")
def test_llm_dimension_inference_fallback_success(
    mock_llm, mock_dimension_df: pd.DataFrame
) -> None:
    result = infer_dimension_column(
        "Which group performs best?",
        mock_dimension_df,
        metric_column="idx_val",
    )

    assert result["dimension_column"] == "alpha_group"
    assert result["selection_mode"] == "llm_assisted"
    mock_llm.assert_called_once()


@patch.dict(os.environ, {"QUAERO_ANTHROPIC_API_KEY": "fake_key"})
@patch(
    "kpi_engine.llm_inference.infer_column_with_llm", return_value="not_a_real_column"
)
def test_llm_metric_inference_rejects_invalid_column(
    mock_llm, mock_metric_df: pd.DataFrame
) -> None:
    with pytest.raises(ValueError, match="Could not confidently infer a metric column"):
        infer_metric_column("Which index value is highest?", mock_metric_df)

    mock_llm.assert_called_once()


@patch.dict(os.environ, {"QUAERO_ANTHROPIC_API_KEY": "fake_key"})
@patch(
    "kpi_engine.llm_inference.infer_column_with_llm", side_effect=RuntimeError("boom")
)
def test_llm_dimension_inference_falls_back_to_existing_error_on_exception(
    mock_llm, mock_dimension_df: pd.DataFrame
) -> None:
    with pytest.raises(
        ValueError, match="Could not confidently infer a dimension column"
    ):
        infer_dimension_column(
            "Which group performs best?",
            mock_dimension_df,
            metric_column="idx_val",
        )

    mock_llm.assert_called_once()


@patch.dict(os.environ, {}, clear=True)
def test_deterministic_fails_cleanly_without_api_key(
    mock_metric_df: pd.DataFrame,
) -> None:
    with pytest.raises(ValueError, match="Could not confidently infer"):
        infer_metric_column("Which index value is highest?", mock_metric_df)
