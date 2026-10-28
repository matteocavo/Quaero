# Quaero Web

React 19 + TypeScript + Vite web app for Quaero. It is a client of the Quaero API (see
`../api/README.md`); it does not change the backend.

## Modes: Public Demo vs Local Operations

| Mode | Data & Execution | Network | Persistence | Intended Use |
|---|---|---|---|---|
| **Public Demo** (default) | Bundled demo fixtures + in-browser CSV profiler & Browser Workspace (`src/analysis/`) | **None** (`0` `/api` calls) | Tab memory only (cleared on reload) | Static hosting (GitHub Pages), zero-setup evaluation |
| **Local Operations** | Local FastAPI backend + real Python pipeline (`pipelines/`, `kpi_engine/`, `app/`) | `/api` same-origin loopback proxy only | Local filesystem (`uploads/`, `projects/`, `runs/`) + SQLite (`quaero.db`) | Full local analytics (`Upload CSV → Create Project → Configure Analysis → Run Python Pipeline → Explore Results`) |

Local Operations never falls back to demo data: if the API is unreachable the app shows it as offline,
the project list is empty and views show errors or "not generated".

## Run it locally

### Option 1 — Recommended Windows one-command launcher

From the repository root in PowerShell:

```powershell
.\scripts\start-local.ps1
```

Or via npm from the repository root:

```powershell
npm --prefix frontend run start:local
```

What `scripts/start-local.ps1` does:
- Resolves the local Python interpreter (`.venv\Scripts\python.exe` or `python`) and verifies `uvicorn`, `fastapi`, and `node`/`npm`.
- Uses `$env:QUAERO_API_KEY` from your environment if set, or generates a cryptographically random 256-bit session key in memory for the current terminal session (never written to disk or exposed to browser code).
- Starts FastAPI on `http://127.0.0.1:8000` (loopback only, never LAN) and waits for `/health`.
- Starts the Vite dev server + proxy on `http://127.0.0.1:3000` (loopback only) and verifies readiness.
- Stops both background processes cleanly when you press `Ctrl+C`.

### Option 2 — Manual two-terminal startup

```bash
export QUAERO_API_KEY="your-secret-key"
uvicorn api.main:app --host 127.0.0.1 --port 8000      # from the repository root
cd frontend
npm ci
npm run dev                                            # http://127.0.0.1:3000
```

PowerShell:

```powershell
$env:QUAERO_API_KEY = "your-secret-key"
uvicorn api.main:app --host 127.0.0.1 --port 8000
cd frontend; npm ci; npm run dev
```

| Variable (server side only) | Default | Purpose |
|---|---|---|
| `QUAERO_API_KEY` | none (or ephemeral via `start-local.ps1`) | added by the proxy as `X-API-Key`; never defined for browser code |
| `QUAERO_API_URL` | `http://127.0.0.1:8000` | API the proxy forwards to |
| `QUAERO_MAX_UPLOAD_BYTES` | `10485760` (10 MiB) | maximum CSV upload size accepted by FastAPI (up to 50 MiB) |
| `VITE_BASE_PATH` | `./` | public base path of the build (`/Quaero/` for GitHub Pages) |

Open `http://127.0.0.1:3000`, click the mode pill in the header and choose **Local Operations**.

## Local Operations workflow (`Import → Configure → Run → Results`)

In **Local Operations** mode, opening **Import & Profile Data** (or clicking **+ New CSV Project** in **Pipeline Operations**) launches the Local Python Pipeline Workflow (`src/components/LocalPipelineWizard.tsx`):

1. **Import (Upload CSV to FastAPI)**:
   - Select a `.csv`, `.tsv`, or `.txt` file from disk or reuse a previously uploaded dataset.
   - The file is validated and stored by FastAPI in `<QUAERO_PROJECT_ROOT>/uploads/upload_<12hex>.csv` with schema metadata in `upload_<12hex>.json`.
2. **Configure (Create Local Project)**:
   - Set a project name (normalized to lowercase `snake_case`), analytical question/objective, and optional metric (`numeric` column), dimension, and time column overrides.
   - Clicking **Create Project & Run Pipeline** (or **Create Project Only**) calls `POST /projects`, writing `projects/<project>/project_config.json` and registering the project in SQLite. Existing projects are never overwritten (`409 Conflict`).
3. **Run (Launch Real Python Pipeline)**:
   - Triggers `POST /projects/{project}/run` (`202 Accepted`) and polls `GET /projects/{project}/runs/{run_id}` until completion.
   - Executes the full Python pipeline: ingestion, profiling, per-column Data Quality scoring (`dq_score_*.json`), cleaning log (`cleaning_log_*.json`), semantic inference, Parquet + SQL marts, Anomaly Framing v2 (`anomaly_report_v2_*.json`), column-level Lineage (`lineage.json` + `lineage.html`), Power BI schema (`pbi_schema.json`), and `final_summary_report.json`.
4. **Results (Explore & Download)**:
   - Inspect generated marts in **Mart Explorer** (and export CSV pages), review per-column DQ scores and framed anomalies in **Quality & Anomalies**, inspect the DAG in **Data Lineage DAG**, and download `final_summary_report.json` directly from **Pipeline Operations**.

### Where uploaded files and local projects are stored & how to remove them

All local operations data stays on your machine inside untracked directories:
- **Uploaded CSVs**: `<repo_root>/uploads/<upload_id>.csv` and `<upload_id>.json` (ignored by Git, never served statically).
- **Local projects & artifacts**: `<repo_root>/projects/<project_name>/` (`project_config.json`, `raw/`, `staging/`, `marts/`, `metadata/`, `metrics/`).
- **Run manifests & SQLite**: `<repo_root>/runs/<run_id>.json` and `<repo_root>/quaero.db`.

To delete a local project and its uploaded CSV:

```powershell
Remove-Item -Recurse -Force .\projects\<project_name>
Remove-Item -Force .\uploads\<upload_id>.*
# Optional: clear run manifests and SQLite database
Remove-Item -Recurse -Force .\runs
Remove-Item -Force .\quaero.db*
```

## Public site (GitHub Pages) behaviour

The deployed site is static: no proxy, no API. The Pages build sets `VITE_DEMO_ONLY=true` and
`VITE_BASE_PATH=/Quaero/` (`npm run build:pages`). There Local Operations is **unavailable**: the option
is disabled with an explanation and the app never sends an `/api` request. Local Operations is also
disabled on any host that is not `localhost`, `127.0.0.1` or `[::1]`.

On phones the navigation is a slide-over menu (button in the header).

## Security model & limitations

- The browser calls only `/api/...` on the dev server. The proxy (`vite.config.ts` +
  `src/proxyPolicy.ts`) forwards **only** the 16 endpoints the app uses (`GET /health`, `GET|POST /uploads`,
  `GET /uploads/{id}`, `GET|POST /projects`,
  `GET /projects/{p}/overview|summary|marts|marts/{m}|runs|runs/{id}|data-quality|anomalies|lineage` and
  `POST /projects/{p}/run`), rejects other paths and methods, caps JSON bodies at 8 KiB, upload bodies at
  12 MiB (`POST /uploads`), and queries at 256 characters, and drops `X-API-Key`, `Authorization` and
  `Cookie` sent by the browser.
- The API key comes from the server environment only. The bundle contains no key, no `process.env` and
  no external AI or font service (`npm run check:bundle` enforces it).
- The dev and preview servers listen on `127.0.0.1` only. Do not expose them on a LAN or public interface:
  they attach `QUAERO_API_KEY` to every allowed `/api` request.
- Uploaded CSV files in Local Operations are validated for size, extension, UTF-8 encoding, and RFC 4180
  structure, assigned a random `upload_<12hex>` identifier, and stored inside `<repo_root>/uploads/`
  (never inside `frontend/public/` or any served folder).
- CSV files opened in the in-browser profiler / Browser Workspace are parsed in the browser tab only and
  never uploaded.
- Public Demo makes no requests to any backend.
- **Security limitations**: Local Operations is designed for single-user execution on `127.0.0.1`. There is
  no multi-user authentication, per-user tenant isolation, or automatic upload retention/TTL cleanup; remove
  local files manually from `uploads/` and `projects/` when no longer needed.

## Analyze your own CSV in the browser

Import & Profile Data -> **Analyze This Dataset** opens the *Analyze Dataset* workspace, where an unknown
CSV can be explored without leaving the app:

| Tab | What it does |
|---|---|
| Data Preview | the first rows and each column's detected type |
| Data Quality | the existing profiler checks (missing values, duplicates, constant columns, ...) |
| Explore & Filter | filters by values, numeric/date range, text, missing or present; distributions; matching rows |
| Build Analysis | visual query builder: group by (dates by year/month/day), count / distinct count / sum / average / min / max, sort, top N |
| Results & Charts | the table, bars or a line over time, KPI cards when there is no grouping, warnings |
| Export | aggregated results (CSV), the filtered rows (CSV, as in your file) and a JSON report |

How it works, and what it is not:

- The analysis runs **in your browser tab on the rows you loaded**. Nothing is uploaded and there is no
  network request. With a personal CSV active, no bundled metric or result is shown.
- Questions are **not interpreted**. The optional question note is stored in the report; the analysis is
  whatever you build from the real column names. The shortcut patterns are built from your columns and are
  disabled when the file cannot support them (for example "over time" needs a date column).
- Columns are typed from their values (numeric, categorical, date, boolean). Values that do not fit a
  column's type are treated as missing and counted in the warnings.
- Limits: 10 MB per file, 200,000 rows, 200 columns, 50,000 groups (more is refused with a
  message), a top-N limit of up to 1,000, at most 3 groupings, 6 calculations and 12 filters. The table
  shows the first 200 result rows and the chart the first 30 bars (400 points for a line); the CSV
  export has every group. Over a limit the app says which one and keeps the previous dataset.
- This is **not the Quaero Python pipeline**: no cleaning stages, marts, lineage or anomaly framing, and
  nothing is written to a project. A CSV analyzed here is not visible to the API or to Local Operations.

The code is in `src/analysis/` (pure TypeScript, no React) and `src/views/analysis/`.

### The Browser Workspace

A loaded dataset becomes the **Browser Workspace**: a temporary context that sits next to the demo
projects in the global selector, in its own group ("Browser workspace (this tab only)") and with a different
colour, so it cannot be mistaken for a project. Uploading a CSV selects it automatically.

| Screen | With the Browser Workspace selected |
|---|---|
| Overview Dashboard | rows, columns and types, completeness, Data Quality score, schema, findings, the current filters and the latest analysis, all computed from the CSV |
| Project Explorer | origin, format, dimensions, per-column statistics, and the analyses run in this session (each can be reopened in Build Analysis) |
| Analyze Dataset | the same state as above: filters and analyses are kept while another context is selected |
| Pipeline Operations, Mart Explorer, Quality & Anomalies, Data Lineage DAG | replaced by an explanation: Parquet marts, Python lineage and pipeline execution are not available in the static demo. No demo data is shown instead |

Rules it follows:

- It lives **only in the memory of the tab**. Nothing is uploaded, written to `localStorage`,
  `sessionStorage` or IndexedDB, or sent anywhere. **Reloading the page removes it** (by design).
- Choosing a demo project leaves the workspace but keeps the dataset, its filters and its analyses in memory;
  choosing the workspace again (or opening Analyze Dataset) brings them back. Opening Analyze Dataset while a
  project is selected switches to the workspace; choosing a project while on it moves to that project's Overview.
- Loading another file replaces the dataset and discards its filters, analyses and results. A rejected file
  (empty, header only, too large, not a CSV) changes nothing. *Remove dataset* (with a confirmation) forgets
  everything and returns to the last selected project.
- It invents nothing: no KPI, run, mart or lineage exists for it. The demo projects and every existing screen
  are unchanged when it is not selected.

## Tests

```bash
npm run typecheck
npm run build:pages      # the static public build (base /Quaero/, demo only)
npm run test:e2e         # real Chrome against that build: assets, console, every screen, phone layout,
                         # CSV round trip, no API requests, the CSV analysis workflow
                         # (upload -> filter -> aggregate -> chart -> export) and the Browser Workspace
                         # (selector, Overview, Explorer, demo <-> workspace), on desktop and at 390 px;
                         # needs Chrome installed
npm run test:e2e:local   # full-stack E2E in real Chrome against a live FastAPI + Vite dev proxy in an
                         # isolated temp workspace: uploads an external CSV, creates a project, runs the
                         # Python pipeline, inspects marts/DQ/anomalies/lineage, and downloads exports
npm test                 # proxy policy, API client, CSV profiler and reader/writer, the analysis engine
                         # (filters, aggregations, nulls, limits, exports), the Local Operations wizard,
                         # the upload -> analyze -> export UI workflow with fetch forbidden, and a real
                         # Vite proxy in front of a fake backend
npm run build
QUAERO_API_KEY=any-sentinel-value npm run check:bundle
```

From the repository root, `pytest tests/test_local_uploads_and_projects.py tests/test_frontend_contract.py tests/test_frontend_integration.py`
checks upload/project validation, verifies that TypeScript types match FastAPI schemas, and exercises
every endpoint the app uses against a live `uvicorn`, including a real pipeline run.

## GitHub Pages (static demo only)

`.github/workflows/web.yml` builds the app with `VITE_BASE_PATH=/Quaero/` and publishes only the static
Public Demo to `https://matteocavo.github.io/Quaero/`. It has no secrets and no dependency on FastAPI.
It builds and tests on pull requests and **deploys only when started manually (`workflow_dispatch`)
from `refs/heads/main`**: a manual run from any other branch fails at the first step with an explicit
error, and the deploy job is conditioned on the same ref. For defence in depth you can also restrict the
`github-pages` environment to the `main` branch (Settings -> Environments). Before the first
deployment: Settings -> Pages -> Source = "GitHub Actions", then run the workflow from the Actions tab.

## Known limitations

- **Single-dataset CSV upload in the UI**: `POST /uploads` and `POST /projects` create single-dataset
  projects from local CSV/TSV files. Multi-dataset projects continue to be authored via
  `projects/<project>/project_config.json` or `app/ui.py`.
- **Single-user local security boundary**: Local Operations is designed for loopback (`127.0.0.1`) use on
  a developer workstation. Uploaded CSVs in `uploads/` and generated projects in `projects/` persist on
  disk until manually deleted; there is no automatic TTL cleanup or multi-tenant access control.
- **Ambiguous questions without overrides**: if a business question is too vague for deterministic
  semantic inference and no metric/dimension override is selected, the pipeline fails with an explanation
  in the run history so you can re-run with explicit metric and dimension overrides.
