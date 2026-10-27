# Quaero Operations Console

A local Streamlit console for operating Quaero. It talks to the Quaero API over HTTP and does not
read artifacts or the SQLite database itself. The pipeline UI in `app/ui.py` is a separate app and is
unchanged.

## Start

```bash
# terminal 1 - the API (loopback only)
export QUAERO_API_KEY="your-secret-key"
uvicorn api.main:app --host 127.0.0.1 --port 8000

# terminal 2 - the console (same key)
export QUAERO_API_KEY="your-secret-key"
streamlit run console/app.py
```

PowerShell:

```powershell
$env:QUAERO_API_KEY = "your-secret-key"
uvicorn api.main:app --host 127.0.0.1 --port 8000
streamlit run console/app.py
```

| Variable | Default | Purpose |
|---|---|---|
| `QUAERO_API_KEY` | none (required) | Sent as `X-API-Key`; read from the environment only, never stored |
| `QUAERO_API_URL` | `http://127.0.0.1:8000` | API base URL; a warning is shown when it is not loopback |

## Screens

| Tab | What it shows |
|---|---|
| Projects | All projects with mode, marts, last run status and artifact availability; details of the selected project |
| Pipeline Operations | Start a run (validated form), status and history with timestamps, durations and errors; optional auto-refresh (3 s, at most 100 refreshes, only while a run is active) |
| Marts | Mart list, metadata (rows, columns), paginated preview, page-local text filter, CSV of the current page |
| Data Quality & Anomalies | Data-quality scores per column and the anomaly report (framed v2 or raw v1) |
| Lineage | Graph drawn from `lineage.json` with edge-type filter, plus node and edge tables |

## Behaviour worth knowing

- **Missing artifacts are normal.** Data quality, lineage and framed anomalies are produced by
  single-dataset runs only; for other projects the console says "not generated for this project".
- **Outages are not hidden.** If the run history store fails, the console says it is unavailable
  instead of showing an empty table.
- **No duplicate jobs.** The Run button is disabled while a run is QUEUED or RUNNING, and the API
  itself answers `409` for a concurrent run.
- **A project without `project_config.json`** needs a source URL to be run.
- **The mart filter only filters the page already loaded** (the API has no server-side filter).
- **No pass/fail threshold** is shown for data quality: the pipeline does not define one.
- **Lineage is data, not HTML.** The pipeline's `lineage.html` (which loads a script from a CDN) is
  not rendered; labels are escaped before they reach the Graphviz renderer.

## Not included yet

Human review queue (approve, correct, reject), server-side mart filtering, metrics definitions and
cleaning-summary screens, run cancellation. The review workflow is designed in
`QUAERO_PHASE3_OPERATIONS_CONSOLE.md`.

## Tests

```bash
pytest tests/test_console_client.py tests/test_console_formatting.py tests/test_console_app.py
```
