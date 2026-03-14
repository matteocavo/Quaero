# Quaero — Architecture

## Overview

This project is a GitHub-first analytics pipeline assistant that transforms raw datasets into clean, analysis-ready tables and suggests meaningful KPIs for dashboards and BI workflows.

The system supports ingestion from multiple data sources and produces curated analytical outputs that are dashboard-ready.

It now supports question-driven analysis, where the pipeline can infer the
metric column, dimension column, and aggregation intent directly from the
dataset plus the user question.

Projects are project-scoped and may contain one dataset or many datasets. The
base architectural contract remains `raw/ -> staging/ -> marts/`. An optional
`integrated/` layer is available for projects that need to combine multiple
datasets into a shared analytical dataset before mart generation.

---

# System Architecture

User Question
      ↓
Deterministic Question Inference
      ↓
Metric / Dimension / Aggregation Resolution
      ↓
Data Ingestion
      ↓
Raw Data Layer
      ↓
Profiling + Cleaning
      ↓
Staging Layer
      ↓
Single-dataset project → Mart Builder
      ↓
KPI Suggestion Engine
      ↓
Dashboard-ready Output

Multi-dataset project
Staging Layer
      ↓
Optional Integrated Dataset Layer
      ↓
Mart Builder
      ↓
KPI Suggestion Engine
      ↓
Dashboard-ready Output

---

# Core Components

## 1. Ingestion Layer

Responsible for retrieving raw data from different sources.

Supported inputs:

- CSV file
- Parquet file
- Direct CSV URL
- Direct Parquet URL
- Config-driven `project_config.json`

Responsibilities:

- detect schema
- store immutable snapshot
- generate ingestion metadata
- support config-driven ingestion of one or more datasets into one project

Output example:
projects/project_name/raw/snapshot_2026_03_13.parquet

For config-driven projects, each source dataset is written as:

`projects/<project_name>/raw/<dataset_name>.parquet`


---

## 2. Profiling Engine

Generates a dataset profile.

Metrics:

- column types
- null percentage
- cardinality
- distribution summary
- potential primary keys

Output:
projects/project_name/metadata/project_profile.json


---

## 3. Cleaning Engine

Standardizes and prepares datasets.

Tasks:

- normalize column names
- enforce data types
- parse dates
- handle missing values
- detect duplicates
- create quality flags

Output layer:
projects/project_name/staging/


---

## 3a. Optional Integrated Dataset Layer

Projects that combine multiple datasets can add an explicit integration layer
between staging and mart generation. Single-dataset projects flow directly from
staging to marts without this step.

Responsibilities:

- align multiple cleaned datasets on a shared time grain
- preserve original cleaned source columns
- add a standardized schema with:
  - `year`
  - `source_metric_value`
  - `dataset_name`
- generate a project-level integrated dataset artifact

Output layer:
projects/project_name/integrated/

Example integrated artifacts:
- `projects/<project_name>/integrated/master_dataset.parquet`
- `projects/<project_name>/integrated/<project_master>.parquet`

The helper column `source_metric_value` is part of the integration contract
only. It standardizes values while datasets are being aligned, but it is not a
recommended mart output metric name.

The integrated dataset becomes the source for downstream use-case marts only
when the project actually combines multiple datasets.


---

## 4. Mart Builder

Creates business-ready tables.

Framework-level mart categories include:
- question-answer marts
- top-entities marts
- distribution marts
- summary statistics marts
- time-trend marts


Tables are optimized for analytics and BI usage.

Framework naming rule:

- generated metric columns must use descriptive semantic names
- preferred form is `<aggregation>_<source_metric_column>`
- examples include `avg_trip_distance`, `total_streams`, and `record_count`
- generic names such as `metric_value`, `value`, or `measure` are not part of
  the mart contract
- the integration-layer helper field `source_metric_value` is allowed only
  inside the optional `integrated/` layer and should not be exposed as a mart
  metric name

Output layer:
projects/project_name/marts/

For projects that use `integrated/`, mart generation runs on top of the
integrated dataset instead of a single cleaned source table.

Framework-level multi-dataset behavior remains generic. Dataset-specific marts
belong to individual use cases built on top of the same raw -> staging ->
integrated -> marts contract.


---

## 5. KPI Suggestion Engine

Uses AI to identify meaningful metrics.

Inputs:

- dataset schema
- sample rows
- business question

Outputs:

Example:
Primary KPI

total_streams

Secondary KPIs

avg_streams_per_track

median_streams_per_track

Suggested Dimensions

release_year

provider

artist


---

## Question-driven Analysis

The CLI entrypoint can now run the pipeline using only:

```bash
python -m app.main <dataset> --question "..." --source-name "..." --project-root .
```

The orchestration layer performs deterministic inference for:

- metric column
- dimension column
- aggregation intent

Optional CLI overrides remain available:

- `--metric-column`
- `--dimension-column`

This keeps the interaction lightweight while preserving explainable,
reviewable inference behavior.

The CLI also supports config-driven project execution:

```bash
python -m app.main --config projects/<project_name>/project_config.json --project-root .
```

Generic project config:

```json
{
  "project": "project_name",
  "question": "What should this project analyze?",
  "datasets": [
    {
      "name": "dataset_a",
      "path": "path/to/dataset_a.csv",
      "time_column": "year"
    }
  ],
  "integrated_output_name": "project_master"
}
```

If the config contains one dataset, the project can stay on the base
`raw/ -> staging/ -> marts/` flow.

If the config combines multiple datasets, the project can add `integrated/`
before mart generation. The current MVP defaults to
`integrated/master_dataset.parquet`, and can also support a project-specific
name such as `integrated/<project_master>.parquet`.

Config-driven mode is still question-driven. The top-level `question` is
propagated into:

- `metadata/final_summary_report.json`
- the generated project `README.md`
- dashboard suggestions metadata

The question guides the primary analytical narrative by influencing:

- primary mart selection
- README explanation
- dashboard suggestion emphasis

Generic marts remain part of the framework output. The question affects
emphasis, not whether the framework produces its standard mart set.

### Public Demo Multi-dataset Use Case: `global_economic_indicators`

The public repository keeps one generic multi-dataset demo project:
`global_economic_indicators`. It demonstrates the reusable architecture without
turning the framework documentation into a bundle of domain-specific projects.

Example public demo config:

```json
{
  "project": "global_economic_indicators",
  "question": "How do global GDP, inflation, and unemployment evolve over time and how are they related?",
  "datasets": [
    {
      "name": "gdp",
      "url": "https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv",
      "time_column": "year"
    },
    {
      "name": "inflation",
      "url": "https://api.worldbank.org/v2/country/all/indicator/FP.CPI.TOTL.ZG?format=json&per_page=20000",
      "time_column": "year"
    }
  ]
}
```

Current public demo marts:

- `mart_gdp_trend`
- `mart_gdp_summary_stats`
- `mart_inflation_trend`
- `mart_inflation_summary_stats`
- `mart_unemployment_trend`
- `mart_unemployment_summary_stats`
- `mart_metric_correlation`

Internal validation projects can still exercise additional domain-specific
specializations, but they are not required to live in the public framework
repository.

---

## 6. Dashboard-ready Analytical Output Layer

Produces analytical marts optimized for downstream dashboard and BI tools.

Deliverables:

- question-answer marts
- top-entities marts
- distribution marts
- summary statistics marts
- time-trend marts

Example output:
projects/project_name/marts/

The `marts/` directory stores the dashboard-ready analytical outputs intended
for tools such as Power BI, Tableau, and Looker Studio. Analytical marts are
the final output layer and can be loaded directly into BI tools without a
separate semantic model export step.

---

# Project-scoped Artifacts

Artifacts are isolated per project so that runs from different datasets do not
overwrite or mix raw snapshots, marts, or metadata. If an artifact
depends on a dataset, mart, metric column, business question, or pipeline run,
it must be stored under:

`projects/{normalized_source_name}/`

Canonical structure:

```text
projects/
  <project_name>/
    raw/
    staging/
    integrated/  # optional, only for projects that combine multiple datasets
    marts/
    metadata/
    README.md
```

Typical project shapes:

- single-dataset project: `raw/`, `staging/`, `marts/`, `metadata/`
- multi-dataset project: `raw/`, `staging/`, optional `integrated/`, `marts/`, `metadata/`

Project-specific artifacts include:

- raw snapshots
- staging datasets
- integrated datasets when a project combines multiple sources
- mart tables
- dashboard-ready analytical marts
- run manifests
- profiling output
- anomaly reports
- analytics metadata
- dashboard suggestions
- semantic metadata
- final summary reports
- dataset coverage metadata when a usable time column exists

The CLI entrypoint resolves `project_root/projects/{normalized_source_name}/`
centrally in `app/pipeline_runner.py` and passes that resolved project path to
all pipeline modules.

The current analytical output layer is BI-tool agnostic and can support Power BI, Tableau, Looker Studio, and any dashboard tool that accepts Parquet-based mart tables.


---

# Storage Strategy

### Local (MVP)

- File storage
- DuckDB / SQLite

### Scalable

- PostgreSQL
- Azure Fabric Lakehouse (OneLake)

---

# Sample Data Guidance

Large real datasets should not be committed to the repository. The
`sample_data/` folder should contain only tiny demo files or lightweight
documentation.

For larger datasets:

- keep the files outside git history when possible
- reference them through external links and local instructions
- run the pipeline against local files or direct file URLs

---

# Repository Structure
```text
quaero/
  app/
    main.py
    pipeline_runner.py
  api/
    pipeline_api.py
  kpi_engine/
    analytics_metadata.py
    anomaly_detection.py
    metric_inference.py
    metrics_definitions.py
    suggest_kpis.py
  pipelines/
    cleaning.py
    dashboard_suggestions.py
    ingestion.py
    integration.py
    mart_builder.py
    profiling.py
  projects/
    <project_name>/
      raw/
      staging/
      integrated/  # optional for multi-dataset projects
      marts/
      metadata/
      README.md
  sample_data/
  tests/
  .github/workflows/
```


---

# Technology Stack

Backend  
Python

Processing  
Pandas / Polars

SQL Layer  
DuckDB / PostgreSQL

Automation  
GitHub Actions

Optional Cloud  
Microsoft Fabric Lakehouse / OneLake

---

# Inference Components

### Deterministic Question Inference
Resolves the primary analysis focus directly from the user question and dataset
schema using deterministic heuristics.

Responsibilities:

- infer metric column
- infer dimension column
- infer aggregation intent
- guide primary mart selection in generated metadata

### KPI Engine
Suggests metrics and dimensions.

### Data Interpretation Assistant
Explains columns and potential business meaning.

---

# Collaboration Model

## ChatGPT

Role:
System architect

Responsibilities:

- architecture design
- pipeline logic
- KPI logic
- prompt engineering

---

## Codex

Role:
Implementation engineer

Tasks:

- create repository scaffold
- implement ingestion modules
- implement cleaning logic
- generate unit tests
- create GitHub workflows
- implement export functions

---

## Claude Code

Role:
Senior code reviewer

Tasks:

- architecture review
- large refactors
- debugging pipelines
- performance review
- reasoning across the full codebase

---

# Future Architecture Extensions

Potential upgrades:

- Dagster orchestration
- dbt transformations
- semantic layer generation
- automated dashboard suggestions
- Fabric Direct Lake integration

