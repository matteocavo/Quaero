# Reproducibility Guide

This document provides the minimum reproducibility guidance for the public
Quaero repository.

---

## Prerequisites

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Required packages: `pandas`, `pyarrow`, `requests`, `polars`, `duckdb`

Current pipeline execution relies on `pandas`, `pyarrow`, and `requests`.
`polars` and `duckdb` remain in the environment as forward-looking dependencies
for future query and performance-oriented extensions, but they are not required
by the active pipeline paths documented here.

The current local UI and optional inference fallback also rely on:

- `streamlit`
- `anthropic`
- `pydantic`

Python 3.11+ recommended. All commands run from the repository root.

---

## Execution Modes

### Single-dataset (direct question-driven)

```bash
python -m app.main <source_path_or_url> \
  --source-name "<logical_name>" \
  --question "<your question>" \
  --project-root "."
```

### Python module wrapper

```python
from app.main import run_pipeline, run_project_pipeline

result = run_pipeline(
    dataset_path="sample_data/release_impact_sample.csv",
    source_name="release_impact",
    question="Which release years generate the strongest average streams?",
    project_root=".",
)

project_result = run_project_pipeline(
    config_path="projects/global_economic_indicators/project_config.json",
    project_root=".",
)
```

### Config-driven (public demo project)

```bash
python -m app.main --config projects/global_economic_indicators/project_config.json --project-root "."
```

### Streamlit UI (local)

```bash
streamlit run app/ui.py
```

The UI supports both:

- single-dataset runs from an uploaded CSV/Parquet file or direct URL
- multi-dataset config-driven runs, with one uploaded file or URL per dataset

---

## Quick Start (sample data, no network required)

```bash
python -m app.main sample_data/release_impact_sample.csv \
  --source-name "release_impact" \
  --question "Which release years generate the strongest average streams?" \
  --project-root "."
```

Expected generated outputs under `projects/release_impact/`:

- `raw/release_impact.parquet`
- `staging/staging_<timestamp>_<uuid>.parquet`
- `marts/mart_catalog_summary.parquet`
- `marts/mart_distribution.parquet`
- `marts/mart_provider_performance.parquet`
- `marts/mart_question_answer.parquet`
- `marts/mart_release_impact.parquet`
- `marts/mart_summary_stats.parquet`
- `marts/mart_time_trend.parquet`
- `marts/mart_top_entities.parquet`
- `metadata/final_summary_report.json`
- `metadata/metrics_definitions.json`
- `README.md`

---

## Public Demo Project

The public repository keeps one maintained config-driven demo project:
`global_economic_indicators`.

Run it with:

```bash
python -m app.main --config projects/global_economic_indicators/project_config.json --project-root "."
```

Question:

`How do global GDP, inflation, and unemployment evolve over time and how are they related?`

Expected outputs under `projects/global_economic_indicators/`:

```text
raw/                          3 parquet snapshots
staging/                      3 cleaned parquet files
integrated/master_dataset.parquet
marts/
  mart_gdp_trend.parquet
  mart_gdp_summary_stats.parquet
  mart_inflation_trend.parquet
  mart_inflation_summary_stats.parquet
  mart_unemployment_trend.parquet
  mart_unemployment_summary_stats.parquet
  mart_metric_correlation.parquet
metadata/
README.md
```

Coverage note:

- GDP ends at 2023
- inflation extends to 2024
- unemployment extends to 2025
- the integrated dataset uses the union of available years

---

## External Data Sources

The public demo fetches live data from public URLs at run time. A network
connection is required.

| Source | URL pattern | Used by |
| --- | --- | --- |
| World Bank API | `api.worldbank.org/v2/country/all/indicator/...` | global_economic_indicators |
| datasets/gdp | `raw.githubusercontent.com/datasets/gdp` | global_economic_indicators |

What can go wrong:

- URL changes: live public sources occasionally change structure or coverage.
- Data updates: coverage years will extend forward as sources publish new data.
- Result drift over time: reruns may change row counts, max years, or values even when the code stays unchanged.
- Rate limiting: avoid repeatedly hammering live public APIs in rapid succession.
- SSL: all loaders use the system CA bundle by default. `allow_insecure_ssl: true` is an opt-in escape hatch only for intercepted networks.

---

## Optional LLM fallback

Quaero remains deterministic by default.

For ambiguous metric or dimension questions, an optional fallback can be
enabled with:

```bash
# Windows
set QUAERO_ANTHROPIC_API_KEY=your_key_here

# macOS / Linux
export QUAERO_ANTHROPIC_API_KEY=your_key_here
```

Behavior:

- no key: deterministic inference only
- key present + valid LLM result: selection mode may become `llm_assisted`
- LLM failure or invalid column: Quaero falls back to the same deterministic
  error behavior and asks for a manual override

---

## Verifying Outputs

```bash
python -c "
import json, pandas as pd
rpt = json.load(open('projects/global_economic_indicators/metadata/final_summary_report.json'))
print('question:', rpt['question'])
for mart_name, mart_path in rpt['mart_paths'].items():
    df = pd.read_parquet(mart_path)
    print(mart_name, len(df))
"
```

---

## Running Tests

```bash
pytest tests/ -q
```

Expected: all tests pass. Tests use temporary directories and do not depend on
the committed `projects/` demo outputs.
