"""Quaero Operations Console - entry point.

Run (with the API already started on the same machine)::

    export QUAERO_API_KEY="your-secret-key"
    streamlit run console/app.py

The existing pipeline UI (``app/ui.py``) is a separate app and is unchanged.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import streamlit as st

from console import views
from console.client import ConfigError, ConsoleError, QuaeroClient, is_loopback_url

st.set_page_config(page_title="Quaero Operations Console", page_icon="🧭", layout="wide")
st.title("🧭 Quaero Operations Console")

try:
    client = QuaeroClient.from_env()
except ConfigError as exc:
    st.error(
        f"{exc.detail} Set QUAERO_API_KEY (and optionally QUAERO_API_URL) in the environment "
        "before starting the console."
    )
    st.stop()

st.sidebar.caption(f"API: {client.base_url}")
if not is_loopback_url(client.base_url):
    st.sidebar.warning(
        "The API is not on this machine: the API key is sent over the network. "
        "Use https and a trusted host."
    )

try:
    projects = client.list_projects()
except ConsoleError as exc:
    views.show_error(exc)
    st.stop()

names = [item["project"] for item in projects]
if not names:
    st.info("No projects found under the project root.")
    st.stop()

selected = st.sidebar.selectbox("Project", names, key="selected_project")

explorer_tab, operations_tab, marts_tab, quality_tab, lineage_tab = st.tabs(
    ["Projects", "Pipeline Operations", "Marts", "Data Quality & Anomalies", "Lineage"]
)

with explorer_tab:
    views.render_explorer(client, projects, selected)

with operations_tab:
    try:
        current_overview = client.overview(selected)
    except ConsoleError:
        current_overview = None
    views.render_operations(client, selected, current_overview)

with marts_tab:
    views.render_marts(client, selected)

with quality_tab:
    views.render_quality(client, selected)

with lineage_tab:
    views.render_lineage(client, selected)
