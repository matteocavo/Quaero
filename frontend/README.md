# Quaero Web

React 19 + TypeScript + Vite web app for Quaero. It is a client of the Quaero API (see
`../api/README.md`); it does not change the backend.

## Modes

| Mode | Data | Network | Use |
|---|---|---|---|
| **Public Demo** (default) | bundled fixtures and the in-browser CSV profiler | none | static hosting, demos |
| **Local Operations** | your local Quaero API | `/api` same-origin proxy only | operating real projects |

Local Operations never falls back to demo data: if the API is unreachable the app shows it as offline,
the project list is empty and views show errors or "not generated".

## Run it locally

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
| `QUAERO_API_KEY` | none | added by the proxy as `X-API-Key`; never defined for browser code |
| `QUAERO_API_URL` | `http://127.0.0.1:8000` | API the proxy forwards to |
| `VITE_BASE_PATH` | `./` | public base path of the build (`/Quaero/` for GitHub Pages) |

Open the app, click the mode pill in the header and choose **Local Operations**.

## Security model

- The browser calls only `/api/...` on the dev server. The proxy (`vite.config.ts` +
  `src/proxyPolicy.ts`) forwards **only** the endpoints the app uses (`GET /health`, `/projects`,
  `/projects/{p}/overview|summary|marts|marts/{m}|runs|runs/{id}|data-quality|anomalies|lineage` and
  `POST /projects/{p}/run`), rejects other paths and methods, caps run bodies at 8 KiB and queries at
  256 characters, and drops `X-API-Key`, `Authorization` and `Cookie` sent by the browser.
- The API key comes from the server environment only. The bundle contains no key, no `process.env` and
  no external AI or font service (`npm run check:bundle` enforces it).
- The dev and preview servers listen on `127.0.0.1` only. Do not expose them: they add the API key to
  every allowed request.
- CSV files opened in the profiler are parsed in the browser and never uploaded.
- Public Demo makes no requests to any backend.

## Tests

```bash
npm run typecheck
npm test                 # 88 tests: proxy policy, API client, CSV profiler, UI interactions,
                         # and a real Vite proxy in front of a fake backend
npm run build
QUAERO_API_KEY=any-sentinel-value npm run check:bundle
```

From the repository root, `pytest tests/test_frontend_contract.py tests/test_frontend_integration.py`
checks that the TypeScript types match the FastAPI schemas and exercises every endpoint the app uses
against a live `uvicorn`, including a real pipeline run.

## GitHub Pages (static demo only)

`.github/workflows/web.yml` builds the app with `VITE_BASE_PATH=/Quaero/` and publishes only the static
Public Demo to `https://matteocavo.github.io/Quaero/`. It has no secrets and no dependency on FastAPI.
It builds and tests on pull requests and **deploys only when started manually (`workflow_dispatch`)
from `refs/heads/main`**: a manual run from any other branch fails at the first step with an explicit
error, and the deploy job is conditioned on the same ref. For defence in depth you can also restrict the
`github-pages` environment to the `main` branch (Settings -> Environments). Before the first
deployment: Settings -> Pages -> Source = "GitHub Actions", then run the workflow from the Actions tab.

## Known gaps

- **No dataset upload to the pipeline.** The API runs a project from its `project_config.json` or from a
  public `source_url` (SSRF-checked); it has no upload endpoint, so the profiler's CSV stays in the
  browser. A safe, separate implementation would add `POST /uploads` with: API-key auth, a size cap
  (for example 5 MB), extension and content validation (CSV or Parquet only), random server-side file
  names inside a dedicated non-served directory, a per-project quota and a time-to-live, and runs that
  reference an `upload_id` instead of a path or URL. It would need its own tests and review.
- The API does not expose project configuration or the metrics catalogue, so Local Operations shows
  those panels as unavailable.
- Run status is refreshed by reloading the history; there is no polling.
- Vague business questions are rejected by the pipeline (it cannot infer metric and dimension) and the
  run ends `FAILED` with the explanation in the run history.
