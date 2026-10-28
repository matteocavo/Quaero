# Changelog

All notable changes to Quaero are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project uses [Semantic Versioning](https://semver.org/). Releases before 0.5.0 are
documented in their [GitHub Release notes](https://github.com/matteocavo/Quaero/releases).

## [Unreleased]

Candidate for v0.7.0 - React web app. Not released.

### Added
- **Web app** (`frontend/`, React 19 + TypeScript + Vite + Tailwind): Get Started workflow, in-browser
  CSV import and profiler, overview dashboard, project explorer, pipeline operations, mart explorer,
  quality and anomalies, and a lineage DAG. **Public Demo** (bundled fixtures, no network requests) is
  the default mode; **Local Operations** talks to the Quaero API through a same-origin dev proxy.
- Dev/preview proxy hardening (`frontend/src/proxyPolicy.ts`): the API key is read from the server's
  `QUAERO_API_KEY` only and added server-side, browser-supplied credentials are dropped, only the 12
  endpoints the app uses are forwarded (fixed methods, size limits), and the server listens on
  `127.0.0.1`.
- GitHub Pages workflow for the static demo (`.github/workflows/web.yml`): builds on pull requests;
  deployment is manual (`workflow_dispatch`) and not active until Pages is enabled.
- Tests: 83 frontend unit/UI/integration tests (Vitest), a bundle secret/host check, and Python
  contract and real-HTTP integration tests that run the frontend's endpoints, including a real
  pipeline run via `POST /projects/{project}/run`, against a live `uvicorn`.
- `frontend/README.md` and a documented dataset-upload gap with a proposal for a safe, separate
  implementation.

### Changed
- `THIRD_PARTY_NOTICES.md` covers the web app's npm dependencies and demo data.
- In Local Operations mode the web app never shows demo data (projects, metrics catalogue, project
  config) and starts with an empty run question.

## [0.6.0] - 2026-10-27 - Operations Console

Quaero gains a local operations console on top of the API. The analytical pipeline
(`pipelines/`, `kpi_engine/`, `app/`) and the persistence layer are unchanged.

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
- `THIRD_PARTY_NOTICES.md`: attribution and upstream terms for the public datasets used by the demo
  projects and loaders (World Bank, DataHub, Berkeley Earth, Our World in Data, NASA, NSIDC),
  Mermaid and the Python dependencies, plus a `## License` section in the README. Quaero itself
  stays under the MIT License.

### Changed
- `POST /projects/{project}/run` now validates the project identifier (letters, digits, underscores,
  hyphens) and returns `422` otherwise, matching the read endpoints. All existing project names comply.
- API documentation uses `127.0.0.1` as the local bind address instead of `0.0.0.0`.
- The README describes the demo data and the CI checks, and CI `compileall` now also covers `db/` and
  `console/`.

### Security
- Artifact readers validate identifiers, keep resolved paths inside `metadata/`, cap artifact size at
  10 MB and never include file paths or tracebacks in error responses.

### Verification
- 468 tests passing (1 skipped: symlink test, covered by an equivalent test),
  `ruff format --check`, `ruff check` and `compileall` clean, CI green on `main`.

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

[Unreleased]: https://github.com/matteocavo/Quaero/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/matteocavo/Quaero/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/matteocavo/Quaero/compare/v0.4.0...v0.5.0
