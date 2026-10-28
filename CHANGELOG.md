# Changelog

All notable changes to Quaero are documented in this file.
The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the
project uses [Semantic Versioning](https://semver.org/). Releases before 0.5.0 are
documented in their [GitHub Release notes](https://github.com/matteocavo/Quaero/releases).

## [Unreleased]

Candidate for v0.8.0 - Local Operations & Web App. Not released.

### Added
- **Transparent question interpretation** (`kpi_engine/question_interpreter.py`,
  `POST /uploads/{upload_id}/interpretation`, interpretation panel in the Local Operations wizard): before anything
  runs, Quaero shows how it reads the question - the measure, the grouping, the aggregation, the order, the time
  column, the filters - and whether each part was chosen by the user, read from the question or assumed. The run
  is only offered on an interpretation the person has seen, for exactly what is in the form. Deterministic keyword
  rules in English and Italian; no confidence scores, no model. A question the pipeline cannot answer (median,
  minimum/maximum, percentage, ratio, grouping by month or year when no such column exists, a period or value
  filter, a comparison of two values, two measures at once, why/causal questions, forecasts) is **refused with the
  reason and a suggestion** instead of being answered with something else; a vague question ("What patterns are
  there?") asks for a measure and a grouping. Columns that are identifiers, constant or all empty are not used as
  a measure unless chosen explicitly.
- **Explicit aggregation** (`aggregation`: `average`, `sum` or `count`) on `POST /projects`, `POST
  /projects/{project}/run` and the interpretation endpoint, and an aggregation selector in the wizard. It wins over
  the wording of the question. Only the three aggregations the pipeline really calculates are accepted.
- `interpretation` (status, selections with their origin, issues) recorded in the run results and in
  `final_summary_report.json`, and shown above the results of a run.
- **Local data lifecycle** (`api/services/lifecycle.py`, `GET /storage`, `POST /storage/policy`,
  `POST /storage/cleanup`, `python -m api.lifecycle_cli`, **Local Storage** screen): uploads are temporary until a
  project uses them; saved projects and the uploads they use are never deleted by the cleanup. Policies **Ask on
  exit** (default), **Auto cleanup** (this session's temporary uploads only) and **Keep files**. Every deletion
  needs `confirm=true` and a confirmation dialog; the client sends a scope and an id, never a path. Symlinks and
  Windows junctions are not followed, only `upload_<12hex>.csv|json` regular files are deleted, files in use or a
  failed second file are reported, and use by a project is re-checked right before deleting. After a crash the
  leftovers of the earlier session are listed (launcher message, Local Storage screen) and kept until the user
  confirms. `scripts/start-local.ps1` sets a session id and applies the policy at exit.
- **Suggested analyses** (`frontend/src/analysis/advisor.ts`): deterministic, in-browser rules that find the
  measure, label, grouping and time columns, flag constant, nearly constant, mostly empty and key-like columns, and
  propose analyses with the reason, the exact calculation and the limits; applying one fills the existing query
  builder. No AI or external service, separate from the Python pipeline. The Local Operations wizard uses the same
  rules for its default measure, grouping and time column.

### Changed
- **Dark is the default theme.** Light and System are one click away in the header. Only a choice the user makes is
  stored (`quaero_theme_preference`); nothing is written on the first visit. The old `quaero_theme` key, which the
  previous version wrote for every first-time visitor, is migrated: a stored `dark` becomes an explicit choice, a
  stored `light` is dropped because it cannot be told apart from the old automatic value. The theme is applied
  before the first paint, so there is no flash.
- The measure and grouping of a single-dataset run are no longer guessed by word overlap: they come from the
  interpretation above. The optional LLM fallback is not used on this path (multi-dataset projects are unchanged).

### Fixed
- **Wrong answers to common questions**: "Which product has the *most revenue*" and "most streams" were answered
  with a row count; "how many" was not always a count; "total" was not always a sum; an explicit measure or grouping
  chosen in the form was overridden by the wording of the question. Each is now read as asked, and the explicit
  choice always prevails.
- **`GET /projects/{project}/runs/{run_id}` returned HTTP 500 intermittently on Windows**: the run file was being
  replaced at the moment the poller had it open. Run files are now written with a unique temporary name, replaced
  under a lock with a bounded retry on sharing violations, and read with the same retry.
- The Local Operations wizard and the ready-made patterns chose the first numeric column as the measure even when
  it was constant (for example all zeros).
- The Browser Workspace Overview counted an unreadable value as missing twice.
- The proxy key-leak test and the UI workflow tests no longer fail intermittently on a busy machine (longer
  Testing Library and test timeouts).

### Added (Local Operations workflow)
- **Local Operations CSV-to-Pipeline workflow (`v0.8.0`)**: end-to-end local workflow in the React web
  app (`Import → Configure → Run → Results`) connected to the FastAPI backend and Python pipeline:
  - **CSV Upload API** (`POST /uploads`, `GET /uploads`, `GET /uploads/{upload_id}`): API-key
    authenticated local CSV upload with filename/extension/MIME validation, configurable byte cap
    (`QUAERO_MAX_UPLOAD_BYTES`, default 10 MiB, max 50 MiB), strict RFC 4180 CSV content validation
    (rejects empty, binary, disguised HTML/XML/JSON, header-only, ragged rows, duplicate/blank headers),
    random server-side `upload_<12hex>` identifiers, and storage isolated inside an untracked local
    `uploads/` directory (never publicly served and never trusting client filenames for filesystem paths).
  - **Project Creation API** (`POST /projects`): creates a config-backed project under
    `projects/<project>/project_config.json` from an uploaded `upload_id`, with analytical question,
    optional metric/dimension/time column selection (validated against the uploaded CSV schema) and
    `409 Conflict` protection so existing projects are never overwritten.
  - **Config-driven single-dataset pipeline parity** (`app/pipeline_runner.py`): config-backed
    single-dataset projects now resolve repo-relative `uploads/<upload_id>.csv` paths, support optional
    `time_column` inference, and generate full Data Quality (`dq_score_*.json`), Anomaly Framing v2
    (`anomaly_report_v2_*.json`), Lineage (`lineage.json`, `lineage.html`), SQL exports, Power BI schema
    and `metrics.yml` artifacts matching direct `run_pipeline()` runs.
  - **Local Operations React wizard** (`frontend/src/components/LocalPipelineWizard.tsx`): available in
    Local Operations mode only (`Import & Profile Data` and `Pipeline Operations`), guiding users from
    CSV upload to project creation, pipeline execution, status polling, mart/DQ/anomaly/lineage
    inspection, and JSON/CSV artifact downloads while keeping Public Demo mode 100% browser-only.
  - **Windows local launcher** (`scripts/start-local.ps1`, `npm --prefix frontend run start:local`):
    starts FastAPI and the Vite dev proxy on `127.0.0.1` only, generates an ephemeral session key when
    `QUAERO_API_KEY` is not set, verifies `/health` and proxy readiness, and cleans up child processes on
    exit.
- **Browser Workspace**: a CSV loaded in the browser becomes a temporary context in the global selector,
  separate from the demo projects. The Overview Dashboard and Project Explorer describe it with real
  statistics, schema, quality, filters and the analyses created; Analyze Dataset shares the same state.
  Mart, lineage and pipeline screens say they are unavailable for it instead of showing demo data. It is
  kept in the memory of the tab only (nothing is stored or uploaded; a reload removes it).
- **CSV analysis workspace** in the web app: after importing a CSV in Public Demo mode (or choosing the
  in-browser tab in Local Operations), *Analyze This Dataset* opens a browser-only workspace (preview,
  data quality, filters, a visual query builder with group-by and count/sum/average/min/max, charts, and
  CSV/JSON exports). It runs on the in-memory rows only, with file and row limits and clear messages, and
  makes no network requests.
- **Web app** (`frontend/`, React 19 + TypeScript + Vite + Tailwind): Get Started workflow, in-browser
  CSV import and profiler, overview dashboard, project explorer, pipeline operations, mart explorer,
  quality and anomalies, and a lineage DAG. **Public Demo** (bundled fixtures, no network requests) is
  the default mode; **Local Operations** talks to the Quaero API through a same-origin dev proxy.
- Dev/preview proxy hardening (`frontend/src/proxyPolicy.ts`): the API key is read from the server's
  `QUAERO_API_KEY` only and added server-side, browser-supplied credentials are dropped, only the 16
  endpoints the app uses are forwarded (fixed methods, 8 KiB cap on JSON bodies, 12 MiB cap on
  `POST /uploads`), and the server listens on `127.0.0.1`.
- GitHub Pages workflow for the static demo (`.github/workflows/web.yml`): builds on pull requests;
  deployment is manual (`workflow_dispatch`), allowed only from `main`, and not active until Pages is
  enabled.
- Tests: frontend unit/UI/integration tests (Vitest), a bundle secret/host check, Python contract and
  upload/project/pipeline tests (`tests/test_local_uploads_and_projects.py`,
  `tests/test_frontend_contract.py`, `tests/test_frontend_integration.py`), and Playwright browser E2E
  suites covering both the static Public Demo (`frontend/e2e`) and the full-stack Local Operations
  workflow (`frontend/e2e-local`).
- `frontend/README.md` documenting Public Demo vs Local Operations, the local CSV-to-pipeline workflow,
  upload storage, data removal, and security boundaries.

- Real-browser tests (`frontend/e2e`, Playwright with the installed Chrome) of the static public build
  served under `/Quaero/`: asset loading, console and network cleanliness, every screen, the phone
  layout, CSV import/export and Local Operations being unavailable. They run in the web workflow and can
  target a deployed URL (`E2E_BASE_URL`).

### Fixed
Found by auditing the deployed GitHub Pages site in a real browser:
- **Phone layout**: the sidebar took the whole screen width and squeezed the content to ~30 px. The
  navigation is now a slide-over menu below the `md` breakpoint.
- **Local Operations on a static host**: switching to it from GitHub Pages sent `/api/*` requests to the
  Pages origin (404s) and reported a "backend offline" message. It is now unavailable on any non-loopback
  host or when the build sets `VITE_DEMO_ONLY=true` (the Pages build), with an explanation, and the API
  client refuses to send requests there.
- **CSV import**: a quoted value containing a line break silently split one row into two. The reader
  now follows RFC 4180 (quotes, doubled quotes, line breaks and delimiters in quotes, CRLF, BOM,
  quote-aware delimiter detection) and duplicate or empty header names no longer overwrite columns.
- **CSV export**: values were JSON-escaped (backslash-quoted) and rows with line breaks broke. Exports
  are valid CSV, and text cells starting with `=`, `+`, `@` or a non-numeric `-` are prefixed with an
  apostrophe so spreadsheets do not run them as formulas.
- **Mart explorer (demo)**: marts without a bundled preview showed invented placeholder columns and
  zero rows; they now say that no preview is bundled.

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
