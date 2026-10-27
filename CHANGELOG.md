# Changelog

All notable changes to Quaero are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project uses [Semantic Versioning](https://semver.org/). Releases before 0.5.0 are
documented in their [GitHub Release notes](https://github.com/matteocavo/Quaero/releases).

## [Unreleased]

Candidate for v0.6.0 - Operations Console. Not released.

### Added
- **Operations console** (`console/`, Streamlit): Project Explorer, Pipeline Operations (start a run,
  bounded auto-refresh, run history with durations and errors), Mart Explorer (paginated preview,
  page-local filter, CSV of the current page), Data Quality & Anomalies, and a Lineage Viewer drawn
  from `lineage.json` (no HTML or CDN script is rendered). It is a client of the API only and keeps
  `app/ui.py` unchanged.
- API read endpoints for the console: `GET /projects/{project}/overview`, `/data-quality`,
  `/anomalies` and `/lineage`, protected by the existing API key. Artifacts are read as the pipeline
  writes them; missing artifacts return `404` (single-dataset runs produce data quality, lineage and
  framed anomalies; multi-dataset runs produce only the raw anomaly report).
- Design for a future human review queue (states, rules, data model, audit trail) in
  `QUAERO_PHASE3_OPERATIONS_CONSOLE.md`; nothing is persisted yet.

### Changed
- `POST /projects/{project}/run` now validates the project identifier (letters, digits, underscores,
  hyphens) and returns `422` otherwise, matching the read endpoints. All existing project names comply.
- API documentation uses `127.0.0.1` as the local bind address instead of `0.0.0.0`.

### Security
- Artifact readers validate identifiers, keep resolved paths inside `metadata/`, cap artifact size at
  10 MB and never include file paths or tracebacks in error responses.

## [0.5.0] - 2026-10-27 - API & Persistence Foundation

Quaero gains an HTTP interface and a durable run history. The analytical pipeline
(`pipelines/`, `kpi_engine/`, `app/`) is unchanged.

### Added
- **FastAPI layer** (`api/`) over the existing pipeline:
  - `GET /health` (public), `GET /projects`, `GET /projects/{project}/summary`,
    `GET /projects/{project}/marts`, `GET /projects/{project}/marts/{mart}` (paginated Parquet preview),
    `POST /projects/{project}/run` (async, `202 Accepted`) and `GET /projects/{project}/runs/{run_id}`.
  - API-key authentication (`X-API-Key`, constant-time comparison, fail-closed `503` when
    `QUAERO_API_KEY` is not set).
  - Path-traversal protection on project and mart identifiers, SSRF protection on `source_url`,
    configurable CORS (`QUAERO_CORS_ORIGINS`) and a catch-all handler that never leaks tracebacks.
  - One active (QUEUED or RUNNING) run per project (`409` otherwise).
- **SQLite persistence layer** (`db/store.py`, standard library only, no new dependency):
  - Project registry, run lifecycle (`QUEUED -> RUNNING -> COMPLETED | FAILED`) and mart registry.
  - WAL journal mode, schema versioning, location via `QUAERO_DB_PATH`.
  - `GET /projects/{project}/runs?limit=` returns the persisted run history.
- 39 new persistence tests and 12 regression tests for the API/persistence layer.
- `.gitignore` entries for runtime artifacts (`quaero.db*`, `runs/`).

### Changed
- Ruff formatting and lint auto-fixes applied to the existing codebase (no functional change).
- JSON run manifests under `runs/` remain the primary record for the API contract; SQLite is a
  secondary, best-effort record, so a database failure never fails a run.

### Fixed
Hardening of the persistence layer found during pre-release review:
- `GET /projects/{project}/runs` returns **HTTP 503** on SQLite errors instead of an empty history
  that looked like a valid result.
- Mart registration records only the Parquet files listed in `result["marts"]` that exist on disk,
  instead of every top-level key of the pipeline result.
- `RunHistoryResponse.total` is the total number of runs for the project, independent of `limit`.
- Run state transitions are atomic (single conditional `UPDATE` with a `rowcount` check), so
  concurrent transitions cannot both succeed.

### Verification
- 284 tests passing, `ruff format --check`, `ruff check` and `compileall` clean, CI green on `main`.

[0.5.0]: https://github.com/matteocavo/Quaero/compare/v0.4.0...v0.5.0
