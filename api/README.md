# Quaero API

HTTP layer on top of the Quaero analytics pipeline. Exposes project artifacts and async run triggering over a REST API suitable for Retool dashboards, n8n workflows, or any HTTP client.

---

## Quick start

```bash
# Export required environment variables
export QUAERO_API_KEY="your-secret-key"           # required — fail-closed if unset
export QUAERO_PROJECT_ROOT="/path/to/your/data"   # optional — defaults to repo root
export QUAERO_CORS_ORIGINS="http://localhost:3000" # optional — comma-separated

# Start the server
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

**PowerShell (Windows):**

```powershell
$env:QUAERO_API_KEY = "your-secret-key"
$env:QUAERO_PROJECT_ROOT = "C:\path\to\your\data"
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

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
  "source_url": "https://example.com/data.csv"
}
```

| Field | Required | Constraints |
|---|---|---|
| `question` | Yes | 1–1000 chars, non-blank |
| `source_url` | No | Must resolve to a public IP (SSRF protected) |

**Response (`202`):**

```json
{
  "run_id": "a1b2c3d4-...",
  "project": "release_impact",
  "status": "QUEUED"
}
```

**Errors:** `409` if a run is already QUEUED or RUNNING for this project.

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

## Security model

| Concern | Mitigation |
|---|---|
| API key exposure | `secrets.compare_digest` (timing-safe), fail-closed (503) when env var unset |
| Path traversal | Project and mart names validated against `[a-zA-Z0-9_-]` allowlist; `.resolve()` + `is_relative_to()` boundary check |
| SSRF | `source_url` hostname resolved via DNS; IP checked against RFC-1918, loopback, and link-local blocklist |
| CORS | Origins from `QUAERO_CORS_ORIGINS` env var; `allow_credentials=False` |
| Error leakage | Global exception handler returns `500` without traceback |

---

## Retool / n8n integration

The API is designed to be consumed by no-code tools:

- **Retool**: add a REST API resource pointing to `http://localhost:8000`, set the `X-API-Key` header in the resource headers. Use `GET /projects/{project}/marts/{mart}?page=1&page_size=500` to feed table components.
- **n8n**: use the HTTP Request node with `X-API-Key` in the header. A `POST /projects/{project}/run` followed by polling `GET /projects/{project}/runs/{run_id}` on a timer implements the async job pattern.

---

## Backward compatibility

The FastAPI layer is purely additive. The existing CLI (`python -m app.main`) and the Streamlit UI (`streamlit run app/ui.py`) are unchanged. No shared state between the HTTP layer and the CLI/Streamlit — they both read from and write to the same filesystem.
