from __future__ import annotations

import io
import json
import logging
import sys
import tempfile
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import streamlit as st  # noqa: E402

from app.main import run_pipeline, run_project_pipeline  # noqa: E402
from pipelines.utils import normalize_name  # noqa: E402

st.set_page_config(page_title="Quaero Pipeline", page_icon="🔍", layout="wide")

SINGLE_DATASET_MODE = "Single dataset"
MULTI_DATASET_MODE = "Multi-dataset project"
UPLOAD_SOURCE_MODE = "Upload file"
URL_SOURCE_MODE = "Fetch from URL"
TREND_AGGREGATIONS = ["average", "sum"]


def _inject_theme_css() -> None:
    """Inject custom CSS to match the portfolio aesthetic."""

    # Theme CSS — tested with Streamlit 1.x; verify selectors after upgrades.
    css = """
    <style>
    .stButton > button {
        border-radius: 28px;
        border: 1px solid rgba(255, 255, 255, 0.08);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
    }
    .stTextInput > div > div > input {
        border-radius: 18px;
        border: 1px solid rgba(255, 255, 255, 0.08);
    }
    .stFileUploader > div {
        border-radius: 18px;
        border: 1px solid rgba(255, 255, 255, 0.08);
    }
    div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlock"] {
        border-radius: 18px;
        padding: 1rem;
    }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def _render_header() -> None:
    st.title("🔍 Quaero - Data Pipeline")
    st.markdown(
        "Run a single dataset or configure a multi-dataset project to generate dashboard-ready marts."
    )


def _render_single_dataset_panel() -> dict[str, Any]:
    col1, col2 = st.columns([2, 1])
    with col1:
        question = st.text_input(
            "Business Question",
            placeholder="e.g. Which regions generate the most sales?",
        )
    with col2:
        source_name = st.text_input(
            "Source Name (Project Name)",
            placeholder="e.g. sales_data",
        )

    source_mode = st.radio(
        "Dataset Source",
        [UPLOAD_SOURCE_MODE, URL_SOURCE_MODE],
        horizontal=True,
        key="single_source_mode",
    )

    uploaded_file = None
    source_url = ""
    if source_mode == UPLOAD_SOURCE_MODE:
        uploaded_file = st.file_uploader(
            "Upload CSV or Parquet file",
            type=["csv", "parquet"],
            key="single_uploaded_file",
        )
    else:
        source_url = st.text_input(
            "Dataset URL",
            placeholder="https://example.com/dataset.csv",
            key="single_source_url",
        )

    return {
        "question": question,
        "source_name": source_name,
        "source_mode": source_mode,
        "uploaded_file": uploaded_file,
        "source_url": source_url,
    }


def _render_multi_dataset_panel() -> dict[str, Any]:
    col1, col2 = st.columns([2, 1])
    with col1:
        question = st.text_input(
            "Business Question",
            placeholder="e.g. How do GDP, inflation, and unemployment evolve over time?",
            key="multi_question",
        )
    with col2:
        project_name = st.text_input(
            "Project Name",
            placeholder="e.g. global_economic_indicators",
            key="multi_project_name",
        )

    settings_col1, settings_col2 = st.columns([1, 1])
    with settings_col1:
        dataset_count = int(
            st.number_input(
                "Number of datasets",
                min_value=2,
                max_value=6,
                value=2,
                step=1,
                key="multi_dataset_count",
            )
        )
    with settings_col2:
        integrated_output_name = st.text_input(
            "Integrated Output Name",
            value="master_dataset",
            key="multi_integrated_output_name",
        )

    st.caption(
        "Each dataset needs a name, time column, metric column, and either an uploaded file or a fetch URL."
    )

    datasets: list[dict[str, Any]] = []
    for index in range(dataset_count):
        with st.expander(f"Dataset {index + 1}", expanded=True):
            meta_col1, meta_col2, meta_col3 = st.columns([1, 1, 1])
            with meta_col1:
                dataset_name = st.text_input(
                    "Dataset Name",
                    placeholder=f"e.g. metric_{index + 1}",
                    key=f"dataset_name_{index}",
                )
            with meta_col2:
                time_column = st.text_input(
                    "Time Column",
                    placeholder="e.g. year",
                    key=f"dataset_time_column_{index}",
                )
            with meta_col3:
                metric_column = st.text_input(
                    "Metric Column",
                    placeholder="e.g. revenue",
                    key=f"dataset_metric_column_{index}",
                )

            options_col1, options_col2 = st.columns([1, 1])
            with options_col1:
                trend_aggregation = st.selectbox(
                    "Trend Aggregation",
                    TREND_AGGREGATIONS,
                    key=f"dataset_trend_aggregation_{index}",
                )
            with options_col2:
                source_mode = st.radio(
                    "Dataset Source",
                    [UPLOAD_SOURCE_MODE, URL_SOURCE_MODE],
                    horizontal=True,
                    key=f"dataset_source_mode_{index}",
                )

            uploaded_file = None
            source_url = ""
            if source_mode == UPLOAD_SOURCE_MODE:
                uploaded_file = st.file_uploader(
                    "Upload CSV or Parquet file",
                    type=["csv", "parquet"],
                    key=f"dataset_uploaded_file_{index}",
                )
            else:
                source_url = st.text_input(
                    "Dataset URL",
                    placeholder="https://example.com/dataset.csv",
                    key=f"dataset_source_url_{index}",
                )

            datasets.append(
                {
                    "name": dataset_name,
                    "time_column": time_column,
                    "metric_column": metric_column,
                    "trend_aggregation": trend_aggregation,
                    "source_mode": source_mode,
                    "uploaded_file": uploaded_file,
                    "source_url": source_url,
                }
            )

    return {
        "question": question,
        "project_name": project_name,
        "integrated_output_name": integrated_output_name,
        "datasets": datasets,
    }


def _render_input_panel() -> tuple[str, dict[str, Any]]:
    run_mode = st.radio(
        "Execution Mode",
        [SINGLE_DATASET_MODE, MULTI_DATASET_MODE],
        horizontal=True,
    )
    if run_mode == SINGLE_DATASET_MODE:
        return run_mode, _render_single_dataset_panel()
    return run_mode, _render_multi_dataset_panel()


def _build_log_handler(log_stream: io.StringIO) -> logging.Handler:
    handler = logging.StreamHandler(log_stream)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    )
    return handler


def _save_uploaded_file(
    uploaded_file: Any,
    destination_dir: Path,
    fallback_stem: str,
) -> Path:
    suffix = Path(uploaded_file.name).suffix or ".csv"
    destination_dir.mkdir(parents=True, exist_ok=True)
    target_path = destination_dir / f"{fallback_stem}{suffix}"
    target_path.write_bytes(uploaded_file.getbuffer())
    return target_path


def _build_multi_dataset_config(
    project_name: str,
    question: str,
    integrated_output_name: str,
    dataset_specs: list[dict[str, Any]],
    repo_root: Path,
) -> Path:
    if not project_name.strip() or not question.strip():
        raise ValueError(
            "Please provide a project name and a business question for the multi-dataset run."
        )

    project_slug = normalize_name(project_name)
    project_dir = repo_root / "projects" / project_slug
    ui_inputs_dir = project_dir / "ui_inputs"
    ui_inputs_dir.mkdir(parents=True, exist_ok=True)

    datasets: list[dict[str, str]] = []
    for dataset_spec in dataset_specs:
        dataset_name = dataset_spec["name"].strip()
        time_column = dataset_spec["time_column"].strip()
        metric_column = dataset_spec["metric_column"].strip()
        if not dataset_name or not time_column or not metric_column:
            raise ValueError(
                "Each dataset requires a dataset name, time column, and metric column."
            )

        if dataset_spec["source_mode"] == UPLOAD_SOURCE_MODE:
            uploaded_file = dataset_spec["uploaded_file"]
            if uploaded_file is None:
                raise ValueError(
                    f"Dataset '{dataset_name}' is missing an uploaded file."
                )
            source_reference = str(
                _save_uploaded_file(
                    uploaded_file=uploaded_file,
                    destination_dir=ui_inputs_dir,
                    fallback_stem=normalize_name(dataset_name),
                )
            )
        else:
            source_url = dataset_spec["source_url"].strip()
            if not source_url:
                raise ValueError(f"Dataset '{dataset_name}' is missing a dataset URL.")
            source_reference = source_url

        datasets.append(
            {
                "name": dataset_name,
                "source": source_reference,
                "time_column": time_column,
                "metric_column": metric_column,
                "trend_aggregation": dataset_spec["trend_aggregation"],
            }
        )

    config_payload = {
        "project": project_name.strip(),
        "question": question.strip(),
        "integrated_output_name": integrated_output_name.strip() or "master_dataset",
        "datasets": datasets,
    }
    config_path = project_dir / "project_config.json"
    config_path.write_text(json.dumps(config_payload, indent=2), encoding="utf-8")
    return config_path


def _render_pipeline_result(result: dict[str, Any]) -> None:
    st.success("✅ Pipeline completed successfully!")

    final_summary = result.get("final_summary_report", {}).get("summary")
    if final_summary:
        st.subheader("Summary Report")
        st.json(final_summary)

    marts = result.get("mart_paths", {})
    if marts:
        st.subheader("Generated Marts")
        for mart_name, mart_path in marts.items():
            st.write(f"📁 `{mart_name}` → `{Path(mart_path).name}`")


def _run_single_dataset(request: dict[str, Any]) -> dict[str, Any]:
    if not request["question"].strip() or not request["source_name"].strip():
        raise ValueError(
            "Please provide a business question and a source name for the single-dataset run."
        )

    if request["source_mode"] == UPLOAD_SOURCE_MODE:
        uploaded_file = request["uploaded_file"]
        if uploaded_file is None:
            raise ValueError("Please upload a CSV or Parquet file.")
        with tempfile.TemporaryDirectory() as temporary_dir:
            temp_path = _save_uploaded_file(
                uploaded_file=uploaded_file,
                destination_dir=Path(temporary_dir),
                fallback_stem=normalize_name(request["source_name"]),
            )
            return run_pipeline(
                dataset_path=str(temp_path),
                source_name=request["source_name"].strip(),
                question=request["question"].strip(),
                project_root=".",
            )

    source_url = request["source_url"].strip()
    if not source_url:
        raise ValueError("Please provide a dataset URL.")
    return run_pipeline(
        dataset_path=source_url,
        source_name=request["source_name"].strip(),
        question=request["question"].strip(),
        project_root=".",
    )


def _run_multi_dataset(request: dict[str, Any]) -> dict[str, Any]:
    config_path = _build_multi_dataset_config(
        project_name=request["project_name"],
        question=request["question"],
        integrated_output_name=request["integrated_output_name"],
        dataset_specs=request["datasets"],
        repo_root=Path(".").resolve(),
    )
    return run_project_pipeline(
        config_path=str(config_path),
        project_root=".",
    )


# --- Main UI Flow ---
_inject_theme_css()
_render_header()
run_mode, run_request = _render_input_panel()

if st.button("Run Pipeline"):
    log_stream = io.StringIO()
    stream_handler = _build_log_handler(log_stream)
    root_logger = logging.getLogger()
    root_logger.addHandler(stream_handler)

    try:
        run_name = (
            run_request.get("source_name")
            if run_mode == SINGLE_DATASET_MODE
            else run_request.get("project_name")
        )
        if run_name:
            st.info(f"Pipeline started for `{run_name}`...")

        with st.spinner("Processing pipeline..."):
            if run_mode == SINGLE_DATASET_MODE:
                result = _run_single_dataset(run_request)
            else:
                result = _run_multi_dataset(run_request)

        _render_pipeline_result(result)
    except Exception as exc:
        st.error(f"Pipeline failed: {exc}")
    finally:
        root_logger.removeHandler(stream_handler)
        with st.expander("Pipeline Logs"):
            st.text(log_stream.getvalue())
