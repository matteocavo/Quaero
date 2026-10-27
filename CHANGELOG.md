# Changelog

All notable changes to Quaero are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project uses [Semantic Versioning](https://semver.org/). Releases before 0.5.0 are
documented in their [GitHub Release notes](https://github.com/matteocavo/Quaero/releases).

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
