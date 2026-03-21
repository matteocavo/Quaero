# Product Requirements Document (PRD)

## Product Name

Quaero

---

# Problem

Data analysts spend a large amount of time preparing data before analysis.

Typical problems:

- messy CSV exports
- inconsistent schemas
- missing documentation
- manual data cleaning
- lack of reproducible pipelines

As a result:

- analytics work becomes slow
- dashboards are difficult to reproduce
- KPI definitions vary across projects

---

# Goal

Create a system that converts raw datasets into clean analytics-ready datasets, analytical marts, and project-level analysis outputs while assisting analysts in identifying meaningful KPIs.

The current interaction model is question-driven: the user should be able to
run the pipeline with only a dataset, question, source name, and project root,
while the system infers the metric column, dimension column, and aggregation
intent automatically.

The framework also supports config-driven project execution, where a project may
contain one dataset or many datasets under a shared project folder.

In config-driven mode, the top-level project question remains part of the core
interaction model. It guides the primary analytical narrative while the
framework still generates its generic mart outputs.

This interaction must remain available through:

- the CLI
- a lightweight local UI
- an importable Python entrypoint for app integration

Deterministic inference remains the default behavior. An optional LLM fallback
may assist ambiguous inference, but it must remain opt-in, non-breaking,
schema-validated, and secondary to the deterministic path.

---

# Target Users

Primary users:

- Data Analysts
- BI Developers
- Independent consultants
- Open data researchers

---

# Key Value Proposition

The platform helps analysts move from:

Raw messy data

→

Clean structured dataset

→

Suggested KPIs

→

Dashboard-ready analytical marts

---

# Core Features

## 1 Data Ingestion

Supported sources:

- CSV upload
- Parquet upload
- direct CSV URL
- direct Parquet URL
- config-driven `project_config.json`

---

## 2 Dataset Profiling

Automatic generation of:

- schema
- null analysis
- distribution statistics
- potential identifiers

---

## 3 Data Cleaning

Standardization tasks:

- column normalization
- type inference
- date parsing
- duplicate detection
- missing value handling

---

## 4 Pipeline Templates

Different pipeline types based on the business question.

Examples:

Trend Analysis

Benchmark Comparison

Release Impact Analysis

Cohort Analysis

Provider Performance

The framework supports both:

- single-dataset projects using `raw/ -> staging/ -> marts/`
- multi-dataset projects using `raw/ -> staging/ -> integrated/ -> marts/` when integration is required

---

## 5 KPI Suggestion Engine

AI analyzes:

- column names
- dataset schema
- user question

Outputs:

- primary KPI
- secondary KPIs
- suggested dimensions
- suggested filters

---

## 6 Dashboard-ready Analytical Outputs

Outputs ready for dashboards and BI tools.

Deliverables:

- analytical marts
- KPI notes
- profiling and quality metadata
- project README documentation
- question-aware dashboard suggestions
- final summary metadata, including dataset coverage when applicable

Analytical marts are the primary output layer for BI tools.

---

# MVP Scope

First version should include:

- CSV ingestion
- dataset profiling
- cleaning pipeline
- mart builder
- KPI suggestion
- dashboard-ready analytical marts

---

# Question-driven Analysis

Preferred usage:

```bash
python -m app.main <dataset> --question "..." --source-name "..." --project-root .
```

The pipeline performs deterministic semantic inference for:

- metric column
- dimension column
- aggregation intent

Optional manual overrides remain available through:

- `--metric-column`
- `--dimension-column`

Config-driven project usage:

```bash
python -m app.main --config projects/<project_name>/project_config.json --project-root .
```

Config mode supports one or more datasets in the same project.

For config-driven projects, the top-level `question` must be propagated into:

- `final_summary_report.json`
- the generated project README
- dashboard suggestions metadata

The project question should influence:

- primary mart selection
- README narrative emphasis
- dashboard suggestion emphasis

Example `projects/<project_name>/project_config.json`:

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
  ]
}
```

---

# Project-scoped Artifacts

The product isolates generated artifacts per project so that
multiple runs do not mix outputs in shared folders. If an artifact depends on a
dataset, mart, metric column, business question, or pipeline run, it must be
stored under the project folder:

`projects/{normalized_source_name}/`

Canonical structure:

```text
projects/
  <project_name>/
    raw/
    staging/
    integrated/  # optional, only for multi-dataset projects
    marts/
    metadata/
    README.md
```

This keeps raw snapshots, staging data, optional integrated datasets,
analytical marts, and metadata isolated per project. The CLI automatically
creates the project folder before execution.

For config-driven projects, per-dataset coverage metadata should be recorded
when a usable time column exists so BI authors can see each source's minimum
and maximum year without inspecting Parquet files directly.

The analytical outputs are BI-tool agnostic and are intended to support tools such as Power BI, Tableau, and Looker Studio by loading the generated mart tables directly.

---

# Sample Data Guidance

Large real datasets should not be committed to the repository. The
`sample_data/` folder should contain only tiny demo assets or lightweight
documentation.

For larger datasets:

- use local files or direct file URLs
- keep the data out of git history
- share external download links and local setup instructions instead of
  committing the files

---

# Non-Goals (Version 1)

Not included initially:

- enterprise authentication
- multi-tenant SaaS
- full AI autonomous pipelines
- large scale distributed computing

---

# Success Metrics

The product is successful if:

- pipelines are reproducible
- dataset preparation time is reduced
- KPI suggestions are relevant
- output datasets work directly in dashboard and BI tools

---

# Roadmap

## Phase 1 — Local MVP

Features:

- CLI-first interface
- CSV ingestion
- profiling report
- cleaning pipeline
- mart creation
- dashboard-ready analytical marts

---

## Phase 2 — Engineering Improvements

Add:

- config-driven dataset loaders
- GitHub automation
- improved metadata tracking
- better KPI prompt models

---

## Phase 3 — Cloud Integration

Add:

- Azure Fabric Lakehouse
- OneLake integration
- pipeline orchestration
- larger dataset support

---

# Example Use Cases

Single-dataset project:

1 ingestion  
2 profiling  
3 cleaning  
4 mart generation  
5 KPI suggestion  
6 dashboard-ready analytical outputs

Multi-dataset project:

1 config-driven ingestion  
2 per-dataset profiling  
3 per-dataset cleaning  
4 optional integration into a shared project dataset  
5 mart generation from the integrated layer  
6 project README and metadata outputs

Example public demo: `global_economic_indicators`

Datasets:
- World GDP
- World inflation
- World unemployment

User question:
How do global GDP, inflation, and unemployment evolve over time and how are
they related?


Pipeline result:

1 config-driven ingestion  
2 per-dataset profiling  
3 per-dataset cleaning  
4 integration into a shared economic indicators dataset  
5 trend and correlation mart generation  
6 project README and metadata outputs  
7 dashboard-ready marts consumed by BI tools

Output:
- `integrated/master_dataset.parquet`
- `mart_gdp_trend`
- `mart_gdp_summary_stats`
- `mart_inflation_trend`
- `mart_inflation_summary_stats`
- `mart_unemployment_trend`
- `mart_unemployment_summary_stats`
- `mart_metric_correlation`
- `final_summary_report.json` with question, primary mart, and dataset coverage


---

# Why This Project Is Valuable

This project demonstrates skills in:

- analytics engineering
- BI preparation
- data quality management
- AI assisted analysis
- reproducible pipelines


