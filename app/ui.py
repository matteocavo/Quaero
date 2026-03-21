from __future__ import annotations

import io
import logging
import os
import tempfile

import streamlit as st

from app.main import run_pipeline

st.set_page_config(page_title="Quaero Pipeline", page_icon="🔍", layout="wide")


def _inject_theme_css():
    """Inject custom CSS to match the portfolio aesthetic."""

    # Theme CSS — tested with Streamlit 1.x; verify selectors after upgrades.
    # Selectors target internal Streamlit classes: verify after upgrades.
    css = """
    <style>
    /* Button */
    .stButton > button {
        border-radius: 28px;
        border: 1px solid rgba(255, 255, 255, 0.08);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 24px rgba(0, 0, 0, 0.3);
    }

    /* Text inputs */
    .stTextInput > div > div > input {
        border-radius: 18px;
        border: 1px solid rgba(255, 255, 255, 0.08);
    }

    /* File uploader */
    .stFileUploader > div {
        border-radius: 18px;
        border: 1px solid rgba(255, 255, 255, 0.08);
    }

    /* Card container */
    div[data-testid="stVerticalBlock"] > div[data-testid="stVerticalBlock"] {
        border-radius: 18px;
        padding: 1rem;
    }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def _render_header():
    st.title("🔍 Quaero - Data Pipeline")
    st.markdown(
        "Upload a dataset and ask a business question to generate dashboard-ready marts."
    )


def _render_input_panel():
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

    uploaded_file = st.file_uploader(
        "Upload CSV or Parquet file", type=["csv", "parquet"]
    )

    return uploaded_file, question, source_name


# --- Main UI Flow ---
_inject_theme_css()
_render_header()
uploaded_file, question, source_name = _render_input_panel()

if st.button("Run Pipeline"):
    log_stream = io.StringIO()
    stream_handler = logging.StreamHandler(log_stream)
    stream_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")
    )
    root_logger = logging.getLogger()

    if uploaded_file and question and source_name:
        st.info(f"Pipeline started for `{source_name}`...")

        suffix = os.path.splitext(uploaded_file.name)[1]
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
            tmp_file.write(uploaded_file.getbuffer())
            tmp_path = tmp_file.name

        root_logger.addHandler(stream_handler)

        try:
            with st.spinner("Processing dataset in memory..."):
                result = run_pipeline(
                    dataset_path=tmp_path,
                    source_name=source_name,
                    question=question,
                    project_root=".",
                )

            st.success("✅ Pipeline completed successfully!")

            if (
                "final_summary_report" in result
                and "summary" in result["final_summary_report"]
            ):
                st.subheader("Summary Report")
                st.json(result["final_summary_report"]["summary"])

            marts_dir = os.path.join("projects", source_name, "marts")
            if os.path.exists(marts_dir):
                st.subheader("Generated Marts:")
                marts = os.listdir(marts_dir)
                for mart in marts:
                    st.write(f"📁 `{mart}`")

        except Exception as e:
            st.error(f"Pipeline failed: {e}")
        finally:
            root_logger.removeHandler(stream_handler)
            with st.expander("Pipeline Logs"):
                st.text(log_stream.getvalue())
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception as e:
                    st.warning(f"Failed to delete temp file: {e}")
    else:
        st.warning("Please provide all inputs: a file, a question, and a source name.")
