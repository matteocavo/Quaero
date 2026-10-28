# Quaero API

HTTP layer on top of the Quaero analytics pipeline. Exposes project artifacts and async run triggering over a REST API suitable for Retool dashboards, n8n workflows, or any HTTP client.

---

## Quick start

```bash
# Export required environment variables
export QUAERO_API_KEY="your-secret-key"           # required — fail-closed if unset
export QUAERO_PROJECT_ROOT="/path/to/your/data"   # optional — defaults to repo root
export QUAERO_MAX_UPLOAD_BYTES="10485760"         # optional — max CSV upload size in bytes (default 10 MiB, max 50 MiB)
export QUAERO_CORS_ORIGINS="http://localhost:3000" # optional — comma-separated

# Start the server
uvicorn api.main:app --host 127.0.0.1 --port 8000 --reload
```

**PowerShell (Windows):**

```powershell
# Recommended one-command local stack launcher (FastAPI + React proxy on 127.0.0.1):
.\scripts\start-local.ps1

# Or start FastAPI alone:
$env:QUAERO_API_KEY = "your-secret-key"
$env:QUAERO_PROJECT_ROOT = "C:\path\to\your\data"
uvicorn api.main:app --host 127.0.0.1 --port 8000 --reload
```

---

Bind to `127.0.0.1` (loopback). Use `0.0.0.0` only behind a reverse proxy with TLS and an access policy you control: the API key is a single shared secret.

---

## Authentication

All endpoints except `GET /health` require the `X-API-Key` header.

| Scenario | Status |
|---|---|
| `QUAERO_API_KEY` env var not set | **503** Service Unavailable (fail-closed) |
| Header missing or wrong key | **401** Unauthorized |
| Correct key | Request proceeds |

```bash
curl -H "X-API-Key: your-secret-key" http://localhost:8000/projects
```

---

## Endpoints

### `GET /health`
Public. No authentication. Returns service status.

```json
{ "status": "ok" }
```

---

### `POST /uploads`
Upload a local CSV file (`multipart/form-data`, field name `file`) into the untracked local upload workspace (`<QUAERO_PROJECT_ROOT>/uploads/`). Returns `201 Created` with a safe server-generated `upload_id` and schema/column metadata.

```bash
curl -X POST -H "X-API-Key: your-secret-key" -F "file=@my_dataset.csv" http://127.0.0.1:8000/uploads
```

```json
{
  "upload_id": "upload_a1b2c3d4e5f6",
  "original_filename": "my_dataset.csv",
  "stored_bytes": 1842,
  "row_count": 48,
  "column_count": 5,
  "delimiter": ",",
  "columns": [
    {
      "name": "year",
      "inferred_type": "numeric",
      "non_null_count": 48,
      "null_count": 0,
      "sample_values": ["2021", "2022", "2023"]
    }
  ],
  "created_at": "2026-10-28T10:00:00+00:00"
}
```

**Validation & limits:**
- Allowed extensions: `.csv`, `.tsv`, `.txt` (filename max 255 chars; path separators `/`, `\`, `..`, `:`, and null bytes are rejected).
- Size limit: controlled by `QUAERO_MAX_UPLOAD_BYTES` (default `10485760` bytes / 10 MiB, hard upper bound 50 MiB). Oversized uploads return `413 Content Too Large`.
- Content validation: must be valid UTF-8 RFC 4180 CSV with 2–200 non-empty unique header columns and 1–200,000 data rows. Empty files, binary files, disguised HTML/XML/JSON payloads, header-only files, and ragged rows return `422 Unprocessable Entity`.
- Storage isolation: the original filename is recorded as metadata only and **never** determines a filesystem path. Files are stored as `<QUAERO_PROJECT_ROOT>/uploads/<upload_id>.csv` + `<upload_id>.json` and are never served statically.

---

### `GET /uploads` and `GET /uploads/{upload_id}`
List all stored CSV uploads (newest first) or fetch metadata for a single `upload_id` (`^upload_[0-9a-f]{12}$`).

**Errors:** `404` if `upload_id` does not exist; `422` if `upload_id` does not match the identifier pattern.

---

### `POST /uploads/{upload_id}/interpretation`
Shows how Quaero reads a question for an uploaded dataset, **without creating a project or running anything** (`200 OK`).
It uses the same rules as the pipeline, so what is shown is what a run will do.

```json
{
  "question": "Which product has the most revenue?",
  "metric_column": null,
  "dimension_column": null,
  "aggregation": null,
  "time_column": null
}
```

All fields except `question` are optional explicit choices; an explicit choice always wins over the wording.

```json
{
  "question": "Which product has the most revenue?",
  "language": "en",
  "status": "ready",
  "metric": {"column": "revenue", "origin": "auto_inferred"},
  "dimension": {"column": "product", "origin": "auto_inferred"},
  "aggregation": {"value": "sum", "origin": "auto_inferred"},
  "ordering": {"value": "highest", "origin": "auto_inferred"},
  "time": {"column": null, "origin": null, "grouping": null},
  "filters": {"requested": false},
  "issues": [{"code": "count_read_as_total", "severity": "note", "message": "...", "suggestion": null}],
  "summary": "Total of revenue for each product, highest first.",
  "suggested_questions": []
}
```

- `status`: `ready` (answered as described), `needs_input` (a measure or grouping is missing: say which, or choose
  it) or `unsupported` (the question asks for something the pipeline does not calculate). A run is refused for
  `needs_input` and `unsupported`; the same text is returned as the error of the `FAILED` run.
- `origin` of each part: `user_provided` (explicit choice), `auto_inferred` (read from the question or the data),
  `assumed_default` (nothing in the question; a documented default was used). There are no confidence scores.
- `issues[].severity`: `unsupported` and `needs_input` block the run; `warning` and `note` inform (for example "the
  average is assumed", "the question groups by month but the column you chose is used").
- **Available aggregations:** `average`, `sum`, `count` (with order `highest` or `lowest`), over one measure and one
  grouping. Median, minimum, maximum, percentage, ratio, grouping by month/year when the data has no such column,
  period and value filters, comparisons of two values, two measures at once, causal ("why") questions and forecasts
  are **refused with the reason** and never answered with a different calculation.
- English and Italian keywords are understood. The result is deterministic; no model is used (the optional LLM
  fallback is not used on this path).

**Errors:** `404` if `upload_id` does not exist; `422` if the question is blank or too long.

---

### `POST /projects`
Create a new local Quaero project from an uploaded CSV dataset (`201 Created`). Writes `projects/<project>/project_config.json` and registers the project in SQLite. Existing projects are preserved (`409 Conflict` if the directory already exists).

```json
{
  "project": "logistics_2025",
  "upload_id": "upload_a1b2c3d4e5f6",
  "question": "Which carrier generates the highest average freight cost?",
  "dataset_name": "logistics_2025",
  "metric_column": "freight_cost",
  "dimension_column": "carrier",
  "aggregation": "average",
  "time_column": "ship_year"
}
```

| Field | Required | Constraints |
|---|---|---|
| `project` | Yes | Letters, digits, underscores, hyphens; normalized to lowercase `snake_case` |
| `upload_id` | Yes | Must match an existing `upload_<12hex>` in `uploads/` |
| `question` | Yes | 1–1000 chars, non-blank analytical objective |
| `dataset_name` | No | Optional logical dataset alias (defaults to normalized project name) |
| `metric_column` | No | Optional override; must exist in the uploaded CSV and have `numeric` type |
| `dimension_column` | No | Optional override; must exist in the uploaded CSV and differ from `metric_column` |
| `aggregation` | No | `average`, `sum` or `count`; wins over the wording of the question |
| `time_column` | No | Optional time column present in the uploaded CSV |

**Errors:** `404` if `upload_id` does not exist; `409` if the project already exists; `422` if column names are invalid or `metric_column` is not numeric.

---

### `GET /projects`
List all generated projects under the project root.

```json
{
  "projects": [
    {
      "project": "release_impact",
      "has_summary": true,
      "marts_count": 4
    }
  ]
}
```

---

### `GET /projects/{project}/summary`
Return the final summary report JSON for a project.

```json
{
  "project": "release_impact",
  "summary": { "source": "release_impact", "question": "...", "mart_paths": { ... } }
}
```

**Errors:** `404` if project or summary does not exist.

---

### `GET /projects/{project}/marts`
List all mart Parquet files for a project.

```json
{
  "project": "release_impact",
  "marts": [
    { "name": "mart_time_trend", "path": "projects/release_impact/marts/mart_time_trend.parquet" }
  ]
}
```

---

### `GET /projects/{project}/marts/{mart}`
Return a paginated preview of a mart table.

**Query parameters:**

| Parameter | Default | Constraints |
|---|---|---|
| `page` | `1` | ≥ 1 |
| `page_size` | `100` | 1 – 1000 |

```json
{
  "project": "release_impact",
  "mart": "mart_time_trend",
  "columns": ["year", "streams"],
  "rows": [{ "year": 2020, "streams": 1000000 }],
  "page": 1,
  "page_size": 100,
  "total_rows": 250
}
```

**Errors:** `404` if project or mart does not exist; `422` if page_size > 1000.

---

### `POST /projects/{project}/run`
Trigger an async pipeline run. Returns immediately with `202 Accepted`.

**Request body:**

```json
{
  "question": "Which release years have the strongest streams?",
  "source_url": "https://example.com/data.csv",
  "metric_column": "streams",
  "dimension_column": "release_year",
  "aggregation": "sum"
}
```

| Field | Required | Constraints |
|---|---|---|
| `question` | Yes | 1–1000 chars, non-blank |
| `source_url` | No | Must resolve to a public IP (SSRF protected); omit for config-backed / upload-backed projects |
| `metric_column` | No | Optional manual metric column override |
| `dimension_column` | No | Optional manual dimension column override |
| `aggregation` | No | `average`, `sum` or `count`; wins over the wording of the question |

**Response (`202`):**

```json
{
  "run_id": "a1b2c3d4-...",
  "project": "release_impact",
  "status": "QUEUED"
}
```

**Errors:** `409` if a run is already QUEUED or RUNNING for this project; `422` if the project name contains characters other than letters, digits, underscores and hyphens.

**Security:** `source_url` is validated against an IP blocklist (loopback, RFC-1918 private ranges, link-local) to prevent SSRF attacks.

---

### `GET /projects/{project}/runs/{run_id}`
Poll the status of a pipeline run.

```json
{
  "run_id": "a1b2c3d4-...",
  "project": "release_impact",
  "status": "COMPLETED",
  "error": null,
  "result_keys": ["project_mode", "marts", "..."]
}
```

**Status values:** `QUEUED` → `RUNNING` → `COMPLETED` | `FAILED`

A question that cannot be answered as asked ends as `FAILED` with the explanation in `error` and no result is written. Reading the run file is safe while it is being updated: on Windows the file is replaced atomically with a bounded retry, so polling does not return `500` during a write.

**Errors:** `404` if run_id doesn't exist or belongs to a different project.

---

### `GET /projects/{project}/runs`
Return the persisted run history of a project from the SQLite store, newest first.

**Query parameters:**

| Parameter | Default | Constraints |
|---|---|---|
| `limit` | `50` | 1 - 500 |

```json
{
  "project": "release_impact",
  "runs": [
    {
      "run_id": "a1b2c3d4-...",
      "project": "release_impact",
      "status": "COMPLETED",
      "question": "Which release years have the strongest streams?",
      "source_url": null,
      "created_at": "2026-10-27T10:00:00+00:00",
      "started_at": "2026-10-27T10:00:01+00:00",
      "completed_at": "2026-10-27T10:02:15+00:00",
      "error": null,
      "artifact_path": null
    }
  ],
  "total": 12
}
```

`total` is the total number of runs recorded for the project; `runs` holds at most `limit`
of them, so `len(runs) < total` means the history is truncated.

**Errors:** `503` if the SQLite store cannot be read (an outage is never reported as an empty
history). A project with no runs returns `200` with `"runs": []` and `"total": 0`.

---

### `GET /projects/{project}/overview`
Project facts for the operations console: artifact availability, the latest run and whether the
project can be re-run without a URL.

```json
{
  "project": "release_impact",
  "project_mode": "single_dataset",
  "question": "Which release years generate the strongest average streams?",
  "primary_mart": null,
  "primary_kpi": "avg_streams",
  "marts_count": 4,
  "has_config": false,
  "artifacts": {
    "summary": true, "data_quality": true, "anomalies": true,
    "lineage": true, "metrics_definitions": true
  },
  "last_run": { "run_id": "a1b2c3d4-...", "status": "COMPLETED", "...": "..." },
  "history_available": true
}
```

`project_mode` is `multi_dataset` when the summary says so, `single_dataset` when a summary exists
without that key, and `null` without a readable summary. `history_available` is `false` (and
`last_run` is `null`) when SQLite cannot be read, so an outage is not mistaken for "never run".

**Errors:** `404` if the project does not exist.

---

### `GET /projects/{project}/data-quality`
Data-quality score reports (`metadata/dq_score_*.json`), one entry per report file.

```json
{
  "project": "release_impact",
  "reports": [
    {
      "source": "release_impact",
      "generated_at": "2026-10-27T10:00:00+00:00",
      "overall_score": 93.4,
      "column_count": 3,
      "columns": [
        {
          "column": "streams", "dtype": "int64", "completeness": 100.0, "uniqueness": 87.5,
          "outlier_flag": false, "format_score": 100.0, "dq_score": 93.5
        }
      ]
    }
  ]
}
```

### `GET /projects/{project}/anomalies`
Framed (v2) anomaly report when `anomaly_report_v2_*.json` exists, otherwise the raw (v1) report.
`version` says which one was returned; `business_summary`, `frequency`, `impact` and
`suggested_action` are present for v2 only. If several reports of the same version exist, the most
recently modified is returned.

### `GET /projects/{project}/lineage`
Column-level lineage from `metadata/lineage.json`: `nodes` (`id`, `layer`, `name`, ...) and `edges`
(`from`, `to`, `type`).

**Which artifacts exist depends on how the project was run.** Single-dataset runs write the
data-quality report, the lineage and the framed anomaly report; config-driven multi-dataset runs write
only the raw anomaly report. A missing artifact returns `404` with an explanatory message (a normal
state, not a failure). A present but unreadable or unexpectedly shaped artifact returns `500` with a
generic message, and artifacts larger than 10 MB are refused.

---

## Persistence (SQLite)

Runs, projects and Parquet mart references are also recorded in a SQLite database
(`db/store.py`, standard library only).

| Setting | Behaviour |
|---|---|
| `QUAERO_DB_PATH` | Database file; defaults to `<QUAERO_PROJECT_ROOT>/quaero.db` |
| Initialisation | Idempotent, executed on API startup (WAL journal mode, schema versioning) |
| Run lifecycle | `QUEUED -> RUNNING -> COMPLETED \| FAILED`; every transition is one conditional `UPDATE` |
| Marts | Only Parquet files listed in the pipeline result and present on disk are registered |
| Failure mode | JSON manifests in `runs/` stay the primary record; a SQLite write failure is logged and never fails a run |

---

## Run execution model

Runs are executed via FastAPI `BackgroundTasks` and their state is stored as JSON files in `<project_root>/runs/`. State survives server restarts — a run that was `RUNNING` when the server crashed will appear as `FAILED` on the next read (the background task doesn't resume, and the state file stays as-is until a new run request clears it).

```
runs/
  a1b2c3d4-....json   → { run_id, project, status, question, created_at, ... }
```

---

### Data lifecycle: `GET /storage`, `POST /storage/policy`, `POST /storage/cleanup`

Three kinds of local data are told apart:

| Kind | What it is | Removed by the lifecycle? |
|---|---|---|
| **Temporary upload** | `uploads/upload_<12hex>.csv` + `.json` that no project uses. Each records the `session_id` of the server that created it. | Yes, with confirmation |
| **Saved upload** | An upload that a project's `project_config.json` refers to (by `upload_id` or by dataset path). Computed from the configs every time, never from a stored flag. | **Never** |
| **Project / generated artifacts** | `projects/<name>/`. Projects created from an upload can have their *generated files* cleared (`raw/`, `staging/`, `integrated/`, `marts/`, `metadata/`, `ui_inputs/`); `project_config.json` stays. Projects not created from an upload (the bundled demos, hand-written configs) are protected. | Generated files only, with confirmation |

`GET /storage` returns the space used per category, the session id, the policy and the lists of uploads
(`state`: `current_session`, `orphaned` for an earlier session or an unknown owner, or `saved`) and projects.

`POST /storage/policy` with `{"lifecycle_policy": "ask_on_exit" | "auto_cleanup" | "keep_files"}` (default
`ask_on_exit`) chooses what the launcher does at exit. It is a POST, not a PUT, so the dev proxy keeps allowing
only GET and POST.

`POST /storage/cleanup` with `{"scope", "confirm", "target_id"}`:

| `scope` | `target_id` | Deletes |
|---|---|---|
| `current_session` | none | this server session's temporary uploads |
| `orphaned` | none | temporary uploads left by an earlier session or without a recorded owner |
| `single_upload` | `upload_<12hex>` | one temporary upload (`409` if a project uses it) |
| `project_artifacts` | a project name | generated files of an upload-created project (`409` if preexisting or a run is in progress) |

Rules enforced by the server (tests in `tests/test_lifecycle.py`):

- `confirm` must be `true`; otherwise `422` and nothing is deleted.
- The client never sends a path or a file name: only a scope, an id matching `^upload_[0-9a-f]{12}$`, or the
  name of a project that already exists. Anything else is `404`/`422` and touches nothing.
- Only regular files named exactly `upload_<12hex>.csv|json` directly inside `uploads/` are removed. Symbolic
  links, Windows junctions, directories and every other file name are left alone; an upload that has such a file
  is skipped whole. A linked `uploads/` folder is refused.
- "This session" is decided by the server from the `session_id` stored in the upload metadata (the launcher sets
  `QUAERO_SESSION_ID`). A restart after a crash therefore sees the old files as `orphaned` and never deletes them
  by itself.
- Whether an upload is still used is checked again immediately before each deletion, under the lock that
  project creation also takes.
- A file that cannot be deleted (open in another program on Windows) is reported in `skipped` with its reason;
  the rest continues. The CSV goes first and the metadata (which records the owner) last, and the metadata is
  kept if the CSV could not be deleted, so a leftover is still recognised next time.

**At exit.** `scripts/start-local.ps1` sets a session id, runs `python -m api.lifecycle_cli report` at start (it
prints any orphaned uploads) and `python -m api.lifecycle_cli exit` after stopping the server, which applies the
policy: `ask_on_exit` lists this session's temporary uploads and asks (with no terminal to ask it keeps them),
`auto_cleanup` deletes them, `keep_files` keeps them. Closing the console window with the X may not run the exit
step; nothing is lost, the files are listed at the next start and in the **Local Storage** screen. If you start
`uvicorn` yourself, no exit step runs.

## Local storage & removing local data

| Path under `<QUAERO_PROJECT_ROOT>` | Contents | Git status |
|---|---|---|
| `uploads/<upload_id>.csv` + `.json` | Uploaded CSV files and schema metadata | Untracked (`.gitignore`) |
| `projects/<project>/` | Project config, raw/staging snapshots, Parquet/SQL marts, DQ/anomaly/lineage metadata | Local projects are untracked unless explicitly committed |
| `runs/<run_id>.json` | Async run state manifests | Untracked (`.gitignore`) |
| `quaero.db` | SQLite project, run, and mart registry | Untracked (`.gitignore`) |

To remove a local project and its uploaded dataset:

```powershell
Remove-Item -Recurse -Force .\projects\<project_name>
Remove-Item -Force .\uploads\<upload_id>.*
# Optional: reset local run manifests and SQLite registry
Remove-Item -Recurse -Force .\runs
Remove-Item -Force .\quaero.db*
```

---

## Security model

| Concern | Mitigation |
|---|---|
| API key exposure | `secrets.compare_digest` (timing-safe), fail-closed (503) when env var unset |
| Path traversal | Project, mart, and upload identifiers validated against strict allowlists; `.resolve()` + `is_relative_to()` boundary checks |
| Untrusted upload filenames | Original filename is validated and stored as metadata only; disk path is always `<project_root>/uploads/upload_<12hex>.csv` |
| CSV upload validation | Size cap (`QUAERO_MAX_UPLOAD_BYTES`, default 10 MiB), UTF-8 + RFC 4180 structure check, rejection of empty/binary/HTML/XML/JSON/header-only/ragged files |
| Project overwrite protection | `POST /projects` returns `409 Conflict` if `projects/<project>/` already exists |
| SSRF | `source_url` hostname resolved via DNS; IP checked against RFC-1918, loopback, and link-local blocklist |
| CORS | Origins from `QUAERO_CORS_ORIGINS` env var; `allow_credentials=False` |
| Error leakage | Global exception handler returns `500` without traceback |
| Limitations | Designed for single-user local operation on loopback (`127.0.0.1`) with a single shared secret (`QUAERO_API_KEY`); no multi-tenant isolation or per-user RBAC |

---

## Retool / n8n integration

The API is designed to be consumed by no-code tools:

- **Retool**: add a REST API resource pointing to `http://localhost:8000`, set the `X-API-Key` header in the resource headers. Use `GET /projects/{project}/marts/{mart}?page=1&page_size=500` to feed table components.
- **n8n**: use the HTTP Request node with `X-API-Key` in the header. A `POST /projects/{project}/run` followed by polling `GET /projects/{project}/runs/{run_id}` on a timer implements the async job pattern.

---

## Backward compatibility

The FastAPI layer is purely additive. The existing CLI (`python -m app.main`) and the Streamlit UI (`streamlit run app/ui.py`) are unchanged. No shared state between the HTTP layer and the CLI/Streamlit — they both read from and write to the same filesystem.
