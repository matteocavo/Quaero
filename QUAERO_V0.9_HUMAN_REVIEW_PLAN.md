# Quaero v0.9.0 — Human Review & Audit Trail: technical plan

**Status:** APPROVED (D1–D10, see §12) with one mandatory additional constraint (§3.0, acceptance criterion 8).
Nothing in this document is implemented. **Revision note:** the run↔artifact verification required by the approval
found that the association is *not verifiable in v0.8.0*; §3.0 adds a prerequisite (artifact fingerprint recorded at run
completion) and §11 re-cuts the PR split accordingly.
**Baseline:** v0.8.0 (`main`). **Scope:** Local Operations mode only. The browser-only Public Demo is untouched.

Legend used throughout: **[V]** verified in the code at the time of writing · **[P]** proposed · **[A]** needs approval.

---

## 1. Verified current state

| Area | Finding | Evidence |
|---|---|---|
| Human review | Design only. No review tables, endpoints, services or UI exist. The Phase 3 doc says so explicitly and defers it pending a data model and a reviewer-identity decision. | `QUAERO_PHASE3_OPERATIONS_CONSOLE.md` §8 (l.263), §8.1 (l.304), §8.2 (l.322), §8.3 (l.333), §12 |
| Schema versioning | `_SCHEMA_VERSION = 1`. `init_db` inserts the version row only if absent; it never compares versions or migrates. DDL is `CREATE … IF NOT EXISTS`, split on `;`. | `db/store.py:35`, `:111-127` |
| Tables today | `projects`, `runs`, `marts`, `schema_version`. | `db/store.py` |
| Atomic transitions | `transition_run` is one conditional `UPDATE … WHERE status IN (allowed)` with a `rowcount` check → `KeyError` (absent) / `ValueError` (wrong state). | `db/store.py:205` |
| Connections / clock | `get_connection` context manager; `_now()` UTC ISO string. | `db/store.py:133`, `:148` |
| Auth | `require_api_key`: `X-API-Key`, constant-time compare, 503 if no key configured, 401 if wrong. Routers opt in with `dependencies=[Depends(require_api_key)]`. | `api/dependencies.py:25`, `api/routers/reports.py:31` |
| Router registration | Five routers included in `main.py`. CORS allows only GET/POST and headers `X-API-Key`, `Content-Type`. | `api/main.py:113-118` |
| Error mapping | `_http_error`: `FileNotFoundError`/`ValueError` → 404, `PermissionError` → 400, else generic 500. | `api/routers/reports.py:38` |
| Artifact readers | `read_data_quality`, `read_anomalies`, 10 MB cap, resolve + `is_relative_to` guards. | `api/services/reports.py:33`, `:161`, `:181` |
| Review-relevant fields | `AnomalyItem`: `column, method, threshold, anomaly_count, example_values, frequency`, and in v2 `impact` (low/medium/high) and `suggested_action`. `DataQualityColumn`: `column, dtype, completeness, uniqueness, outlier_flag, format_score, dq_score`. | `api/schemas.py:217`, `:245` |
| Artifact scope | Single-dataset runs write DQ + lineage + **v2** anomalies. Multi-dataset runs write only **v1** anomalies, which have **no `impact`**. | `projects/<p>/metadata/` |
| Artifacts are mutable per run | Files are overwritten on each run and carry **no `run_id`**. A review item therefore cannot point at "the artifact of run X" by reference alone. | `projects/<p>/metadata/*`, `api/services/runner.py` |
| Run lifecycle | Runs live in SQLite (`create_run` / `transition_run`) and in `runs/<run_id>.json` state files with a process lock and retry for Windows sharing conflicts. | `api/services/runner.py:216-316`, `tests/test_run_state_reliability.py` |
| Frontend | Local Operations = `mode === 'live'` (default `demo`; GitHub Pages never reaches live). `OperationsView` holds run history, wizard, quality/lineage links. `QuaeroApiClient` + `ApiError`. Python/TS contracts are kept in sync by `tests/_ts_interfaces.py` + `test_frontend_contract.py`. | `frontend/src/AppContext.tsx`, `views/OperationsView.tsx`, `services/apiClient.ts` |
| Local launch | `Start-Quaero.bat` → `scripts/start-local.ps1`: API on 127.0.0.1:8000, Vite proxy on 127.0.0.1:3000 attaches an ephemeral `QUAERO_API_KEY` server-side. | PR #22 |
| Environment | Vitest default pool fails on Node 24.14.0 (environmental). CI on Node 22 passes. | known issue, not in scope |

## 2. Gap analysis

**Reusable as-is:** the conditional-`UPDATE` transition pattern; `require_api_key`; `_http_error`; artifact readers and
size/path guards; `impact` and `outlier_flag` fields; Local Operations mode, `OperationsView`, `QuaeroApiClient`;
the test styles (TestClient, threaded writers, monkeypatched I/O, lock tests).

**Missing:**

1. A real migration step (version compare → apply → stamp). Today a v1 DB can never gain tables except by `IF NOT EXISTS` luck.
2. The three review tables plus append-only enforcement.
3. A **binding between an item and the data it was raised on** (artifacts have no `run_id` and are overwritten).
4. An item-generation mechanism (nothing creates items).
5. The rules configuration (no file, no loader).
6. Review service, router, schemas, TS types, client methods, view, tests.

**Deliberately not missing / not touched:** analytic pipeline, run lifecycle, artifact writers, Public Demo, Streamlit console.

## 3. Proposed architecture [P]

```
 Artifacts (read-only)          SQLite (additive v2)                 API                     UI (live mode only)
 dq_score_*.json    ─┐     ┌─ review_items   (original snapshot)   POST /review/sync        Review Queue
 anomaly_report_v2_* ─┼────►│  review_events  (append-only)   ◄───  GET  /review/items      Item detail
 runs (latest COMPLETED)─┘  └─ review_corrections (overlay)         GET  /review/items/{id}  Decision dialog
                                                                    POST /review/items/{id}/decision  History
                                                                    GET  /review/items/{id}/events
```

Principles:

- **Originals are immutable.** At sync time the exact artifact fragment (one anomaly row / one DQ column row) is
  **snapshotted into `review_items.original_json`**, with the run id and the artifact file's SHA-256. This is the
  answer to the overwritten-artifact gap: the review stays valid and auditable after the next run replaces the file.
- **Audit is append-only.** `review_events` and `review_corrections` get `BEFORE UPDATE` / `BEFORE DELETE` triggers that
  `RAISE(ABORT)`. Only `review_items.status/updated_at` mutate, and only inside the decision transaction.
- **Corrections are overlays.** They never modify artifacts or `original_json`; the API returns an `effective` view
  computed at read time (original + latest non-superseded overlay).
- **Review status ≠ run status.** A run stays `COMPLETED`/`FAILED`; review state lives only on items. No change to `runs`.
- **No new runtime dependency.** Stdlib `json`, `hashlib`, `sqlite3`; existing FastAPI/Pydantic.
- **Layering follows the repo:** `db/store.py` (SQL), `api/services/review.py` (rules, snapshots, overlay), `api/routers/review.py`, `api/schemas.py`.

### 3.0 Run↔artifact binding: verification outcome and design [V][P]

**Verified in v0.8.0 (code read):**
- Artifacts (`dq_score_*`, `anomaly_report_v2_*` under `projects/<p>/metadata/`) are **overwritten in place on every run** and contain **no `run_id`, `generated_at` or hash**.
- `runner.py` calls the pipeline **without passing the `run_id`**; the `runs` row has `started_at`/`completed_at` and an `artifact_path` column, but nothing records *which bytes* a run produced.
- Pipelines can also be run outside the API (CLI), overwriting the same files with no `runs` row at all.
- Therefore "latest COMPLETED run" proves nothing about the files currently on disk. **The binding is not verifiable from the existing data.**

**Design (additive; no pipeline change):**
1. *Fingerprint at completion.* When the runner marks a run `COMPLETED`, it also records, for each review-relevant artifact, `(run_id, artifact, path, sha256, size, mtime_ns, recorded_at)` in a new additive table `run_artifacts` (schema v2, same migration). Hashing is read-only; the pipeline code is not modified.
2. *Verification at sync.* Sync binds items to a run **only if** the current file's SHA-256 equals the fingerprint recorded for the latest COMPLETED run. Otherwise sync **fails explicitly and creates nothing**:
   - no fingerprint for that run (e.g. runs made before v0.9.0) → `409 binding_unverifiable`;
   - hash differs (file overwritten by a newer run, a CLI run, or manual edit) → `409 artifact_changed`;
   - a run is QUEUED/RUNNING for the project → `409 run_active`;
   - artifact missing/unreadable → `409 artifact_unavailable`.
   Error bodies state the cause and the remedy (re-run the project); they contain no paths or SQL.
3. *Immutable snapshots.* `original_json` and `artifact_sha256` are stored at item creation and protected by triggers (no UPDATE/DELETE of identity or snapshot columns). The stored hash is the SHA-256 of the **artifact file bytes** from which the item was derived; `GET /review/items/{id}` re-verifies `original_json` against a stored `snapshot_sha256` (hash of the canonical snapshot) and reports `snapshot_verified: true|false`. A later overwrite of the artifact never alters an acquired item.
4. *Residual window (documented).* Between pipeline completion and fingerprinting, a concurrent external overwrite could be fingerprinted. The runner already serialises runs per project and fingerprints immediately after the pipeline returns and before `COMPLETED`; the CLI-while-API-runs case is out of scope and called out as a limitation.

### 3.1 Item generation
*Binding.* Per §3.0: `run_id` is assigned only after the hash check passes; it is never inferred from "latest COMPLETED" alone.

*Trigger.* Options considered:

| Option | Verdict |
|---|---|
| Lazy creation on `GET` | Rejected: a write on a read; races between pollers. |
| Hook at run completion in `runner.py` | Couples review to the run lifecycle and changes working v0.8.0 behaviour. Rejected for 0.9.0 (revisit later). |
| **Explicit idempotent `POST /review/sync`** | **Proposed.** The UI calls it when the queue opens or on "Refresh". Re-running creates only missing items (unique key), never touches existing ones. |

*Identity:* `(project, run_id, kind, ref)` is unique. `ref` for anomalies = `"<column>|<method>"`; for DQ = `"<column>"`.
Items from an older run keep their status and are shown as **superseded** (derived: `item.run_id != latest completed run`), never deleted or auto-closed.

## 4. SQLite schema and migration strategy [P]

### 4.1 Migration runner

`_SCHEMA_VERSION = 2`. `init_db` changes from "stamp if absent" to:

1. Read `MAX(version)` from `schema_version` (none → fresh DB).
2. Fresh DB: apply base DDL + all migrations, stamp 2.
3. Existing v1 DB: `BEGIN IMMEDIATE`; apply `_MIGRATION_V2` statements; insert version 2 row; `COMMIT`. On any error `ROLLBACK` and raise — the DB remains a valid v1.
4. DB newer than code (version > 2): refuse to start with a clear error (no downgrade).

Properties: purely additive (`CREATE TABLE/INDEX/TRIGGER IF NOT EXISTS`); **no `ALTER`/`UPDATE`/`DELETE` on `projects`, `runs`, `marts`**;
idempotent (running twice is a no-op); historical runs and artifacts unchanged. DDL is run statement-by-statement inside the
explicit transaction (not `executescript`, which commits implicitly).

### 4.2 DDL (v2)

```sql
CREATE TABLE IF NOT EXISTS review_items (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  project         TEXT NOT NULL,
  run_id          TEXT NOT NULL,
  kind            TEXT NOT NULL CHECK (kind IN ('anomaly','dq')),
  ref             TEXT NOT NULL,
  rule_id         TEXT NOT NULL,                 -- 'R1' | 'R2' | 'R3' | 'R4'
  original_json   TEXT NOT NULL,                 -- immutable snapshot of the artifact fragment
  artifact_sha256 TEXT NOT NULL,
  status          TEXT NOT NULL DEFAULT 'REVIEW_REQUIRED'
                  CHECK (status IN ('REVIEW_REQUIRED','ACCEPTED','CORRECTED','REJECTED')),
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL,
  UNIQUE (project, run_id, kind, ref)
);
CREATE INDEX IF NOT EXISTS idx_review_items_project_status ON review_items (project, status);

CREATE TABLE IF NOT EXISTS review_events (       -- APPEND-ONLY
  id                INTEGER PRIMARY KEY AUTOINCREMENT,
  item_id           INTEGER NOT NULL REFERENCES review_items(id),
  from_status       TEXT NOT NULL,
  to_status         TEXT NOT NULL,
  reviewer          TEXT NOT NULL,
  reviewer_source   TEXT NOT NULL DEFAULT 'declared' CHECK (reviewer_source = 'declared'),
  reason            TEXT,
  client_request_id TEXT NOT NULL,                -- idempotency key
  at                TEXT NOT NULL,
  UNIQUE (item_id, client_request_id)
);
CREATE INDEX IF NOT EXISTS idx_review_events_item ON review_events (item_id, id);

CREATE TABLE IF NOT EXISTS review_corrections (  -- APPEND-ONLY
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  item_id      INTEGER NOT NULL REFERENCES review_items(id),
  event_id     INTEGER NOT NULL REFERENCES review_events(id),
  payload_json TEXT NOT NULL,                     -- {"field","from","to","justification"}
  created_at   TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS review_events_no_update BEFORE UPDATE ON review_events
  BEGIN SELECT RAISE(ABORT,'review_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS review_events_no_delete BEFORE DELETE ON review_events
  BEGIN SELECT RAISE(ABORT,'review_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS review_corrections_no_update BEFORE UPDATE ON review_corrections
  BEGIN SELECT RAISE(ABORT,'review_corrections is append-only'); END;
CREATE TRIGGER IF NOT EXISTS review_corrections_no_delete BEFORE DELETE ON review_corrections
  BEGIN SELECT RAISE(ABORT,'review_corrections is append-only'); END;
CREATE TRIGGER IF NOT EXISTS review_items_immutable_identity BEFORE UPDATE OF
  project, run_id, kind, ref, rule_id, original_json, artifact_sha256, created_at ON review_items
  BEGIN SELECT RAISE(ABORT,'review item identity and original are immutable'); END;
```

Notes: additions beyond the Phase 3 sketch are `rule_id`, `original_json`, `artifact_sha256` (needed for the binding and
immutability), `event_id` on corrections, and `client_request_id` (idempotency). A **deliberate consequence:** a
correction that is later superseded (CORRECTED → REJECTED) stays in the table; the overlay resolver ignores overlays whose
item is no longer CORRECTED, but they remain for audit. Foreign keys require `PRAGMA foreign_keys=ON` (verify the current
connection setting during implementation).

## 5. API contracts [P]

All under `/review`, router-level `dependencies=[Depends(require_api_key)]` (401 wrong key, 503 key not configured —
identical to `/reports`). JSON only. Five endpoints; each is justified below.

| # | Method & path | Purpose / why it exists |
|---|---|---|
| 1 | `POST /review/sync` | Idempotent item generation (§3.1). A write, so not a GET. |
| 2 | `GET /review/items` | Queue. Query: `project`, `status`, `kind`, `limit` (1–200, default 50), `offset`. |
| 3 | `GET /review/items/{id}` | Detail: original, effective (overlay applied), current status, rule, latest correction. **The overlay view lives here — no separate endpoint.** |
| 4 | `POST /review/items/{id}/decision` | The only mutation. |
| 5 | `GET /review/items/{id}/events` | Decision history (append-only list, ascending). |

### 5.1 Schemas (Pydantic; mirrored in `types/api.ts` and `_ts_interfaces.py`)

```python
class ReviewSyncRequest(BaseModel):            # extra="forbid"
    project: str                               # existing project-name validator

class ReviewSyncResponse(BaseModel):
    project: str; run_id: str
    created: int; existing: int

class ReviewItem(BaseModel):
    id: int; project: str; run_id: str
    kind: Literal["anomaly","dq"]; ref: str; rule_id: str
    status: Literal["REVIEW_REQUIRED","ACCEPTED","CORRECTED","REJECTED"]
    superseded: bool                           # derived, see §3.1
    created_at: str; updated_at: str

class ReviewItemDetail(ReviewItem):
    original: dict                             # immutable snapshot
    effective: dict                            # original + active overlay
    correction: ReviewCorrection | None
    artifact_sha256: str

class ReviewCorrection(BaseModel):
    field: Literal["impact","outlier_flag"]    # whitelist per kind (§6.2)
    from_value: str | bool
    to_value: str | bool
    justification: str

class ReviewDecisionRequest(BaseModel):        # extra="forbid"
    decision: Literal["ACCEPTED","CORRECTED","REJECTED"]
    reviewer: str                              # 1–80 chars after trim, no control chars
    reason: str | None                         # ≤ 1000 chars
    correction: ReviewCorrection | None
    expected_status: Literal["REVIEW_REQUIRED","CORRECTED"]   # optimistic concurrency
    client_request_id: str                     # UUID, idempotency key

class ReviewEvent(BaseModel):
    id: int; item_id: int; from_status: str; to_status: str
    reviewer: str; reviewer_source: Literal["declared"]
    reason: str | None; at: str
```

Cross-field validation (model validator → **422**): `reason` required (non-blank) for `CORRECTED` and `REJECTED`;
`correction` required **iff** `decision == CORRECTED`; `correction.field` must be allowed for the item's kind and
`to_value` must differ from the original and be in the field's domain (checked in the service → 422).

### 5.2 Decision semantics

One SQLite transaction (`BEGIN IMMEDIATE`):

1. Load item (absent → 404).
2. **Idempotent replay:** if an event exists with `(item_id, client_request_id)` → return 200 with the *original* result; nothing written.
3. If `item.status != expected_status` → **409** `{"code":"stale_status","current":…}`.
4. If `(item.status → decision)` not in the transition table (§6) → **409** `invalid_transition`.
5. `UPDATE review_items SET status=?, updated_at=? WHERE id=? AND status=?` — `rowcount == 0` ⇒ lost race ⇒ 409 (same pattern as `transition_run`).
6. `INSERT` event (+ correction row when CORRECTED). Commit.

Reviewer is taken from the body, trimmed, and stored with `reviewer_source='declared'` set **by the server** (the client cannot send it; the column CHECK pins it).

### 5.3 HTTP codes

`200` reads, sync, decision (including idempotent replay; decision returns `{item, event}`) · `401` bad key · `404` unknown item / project / no completed run ·
`409` stale status, invalid transition, run active during sync · `422` validation (missing reason, bad correction, bad enum, oversize) ·
`503` API key not configured · `500` generic message only (no SQL, no paths), same as the global handler.
No `201`: the resources are derived records, not client-addressable creations — [A] confirm, easy to switch.

### 5.4 Authorization and security

Existing API key only. Single-user local app: no login, RBAC, sessions or tokens in 0.9.0. The reviewer string is
**attribution, not authentication**; the API, schema docs, event rows and UI all say "declared". `project` and ids are validated
(no path construction from user input; ids are integers). Strings are length-capped and rendered as text (React escaping), never HTML.
CORS already permits POST with the needed headers — no change expected.

## 6. Review rules and state machine

### 6.1 Rules [P][A]

Only information the pipeline already produces; **no confidence score**.

| Rule | Condition | Default |
|---|---|---|
| R1 | v2 anomaly with `impact == "high"` | **on** |
| R2 | v2 anomaly with `impact == "medium"` | off |
| R3 | DQ column with `outlier_flag == true` | on [A] (confirm; Phase 3 doc lists it) |
| R4 | DQ `completeness` or `dq_score` below an **owner-defined** threshold | off; **no threshold shipped** |

Config: optional `review_rules.json` in the project root (user-owned, read with stdlib `json`, strict validation, unknown keys rejected).
Absent file ⇒ defaults above. A rule with an undefined threshold is simply inactive. v1 anomaly reports (no `impact`) generate **no** items, and the UI says so.
**R5 (run FAILED, `kind=run`) is excluded** from 0.9.0 because the brief limits rules to anomalies/DQ/thresholds — [A] decide whether to add later.

### 6.2 State machine

```
REVIEW_REQUIRED ──accept──► ACCEPTED        (terminal)
REVIEW_REQUIRED ──correct─► CORRECTED ──accept──► ACCEPTED
REVIEW_REQUIRED ──reject──► REJECTED         CORRECTED ──reject──► REJECTED   (terminal)
```

Reason: optional for ACCEPTED, **mandatory** for CORRECTED and REJECTED. ACCEPTED/REJECTED are terminal (no reopen in 0.9.0 — [A]).
Correctable fields (whitelist): anomaly → `impact` ∈ {low, medium, high}; DQ → `outlier_flag` ∈ {true,false}. Free-form edits are not allowed.
Accepting a CORRECTED item accepts the corrected overlay.

## 7. React UI (Local Operations only) [P]

- Entry: a **Review** tab visible only when `mode === 'live'`; not rendered in Public Demo; no demo-mode code path or data. Nothing in `demo/` or the Pages build changes.
- `ReviewView`: project filter, status filter (default `REVIEW_REQUIRED`), kind filter, paginated table (kind, column/ref, rule, impact/flag, status, run, superseded badge), "Refresh" → `POST /review/sync` then list.
- `ReviewDetail` panel: original vs effective side by side, anomaly fields (method, threshold, count, examples, suggested_action) or DQ metrics, rule that raised it, **History** (events, with "declared reviewer" label).
- `DecisionDialog`: Accept / Correct / Reject. Correct shows field + new value + justification; Reject and Correct require a reason (button disabled, inline message). Confirmation step stating that the decision is permanent and recorded. Reviewer-name input with the helper text "Declared name — not verified"; remembered in `localStorage` as a convenience only (try/catch, never required to render).
- States: loading, empty queue ("No items need review"), v1-only project ("anomaly report has no impact data"), sync error, 409 ("Someone changed this item — reload", auto-refetch), 401/503 reuse existing messages, offline/API-down.
- `client_request_id` generated per dialog open (`crypto.randomUUID()`), reused on retry so a double click or timeout retry is idempotent.
- Types in `types/api.ts`; client methods in `apiClient.ts`; styles reuse existing tokens. Accessible dialog (focus trap, labelled controls).

## 8. Tests and acceptance criteria

**Python (pytest):**

- *Migration* (`tests/test_review_migration.py`): fresh DB → v2; real v1 DB fixture → v2 with `runs`/`projects`/`marts` row-for-row identical; rerun is a no-op; failure mid-migration rolls back to a valid v1; DB newer than code refused.
- *Store/state* : every legal transition; every illegal one raises; terminal states; reason rules; overlay resolution incl. superseded correction.
- *Concurrency* (style of `test_run_state_reliability.py`): N threads deciding the same item → exactly one success, rest 409, exactly one event; concurrent syncs create each item once; reader threads during decisions never see torn state.
- *Idempotency*: replay with same `client_request_id` → identical 200, one event; different key after terminal → 409.
- *Immutability/append-only*: direct `UPDATE`/`DELETE` on events/corrections and identity columns of items raises; artifact files byte-identical (hash) before/after sync and decisions; `original_json` unchanged after a new run overwrites the artifact.
- *API* (`tests/test_review_api.py`, TestClient): 401/503, 404, 409, 422 matrix (missing reason, bad field, extra keys, blank reviewer, oversize), `reviewer_source` cannot be supplied, error bodies contain no paths/SQL.
- *Binding* (`tests/test_review_binding.py`): runner records fingerprints on COMPLETED (and none on FAILED); sync succeeds when hash matches; overwrite after run → `artifact_changed` and zero items; pre-v0.9 run → `binding_unverifiable`; run active → `run_active`; missing file → `artifact_unavailable`; snapshot stays verifiable after artifact overwrite; tampered `original_json` → `snapshot_verified=false`.
- *Rules*: R1/R3 default, R2/R4 off, config on/off, undefined threshold inactive, v1 report → zero items, malformed config → clear 4xx/startup error.
- *Regression*: full existing suite unchanged; `test_frontend_contract.py` covers new schemas; `init_db` on v1 DB used by existing tests still works.

**Frontend (Vitest + Testing Library):** queue render/filters/empty/error; dialog validation (reason mandatory); 409 handling; idempotency key stable across retry; reviewer label says "declared"; Review tab absent in demo mode. **E2E (Playwright, Local Operations):** run a project → Review → sync → open item → Reject with reason → status and history update; second decision attempt blocked; Public Demo has no Review entry.

**Acceptance criteria:** (1) v0.8.0 DBs upgrade in place with zero change to existing rows; (2) every decision yields exactly one immutable event; (3) no artifact byte changes; (4) concurrent/duplicate decisions never double-apply; (5) reviewer never shown as authenticated; (6) Public Demo and Pages bundle unchanged; (7) CI green on Node 22, no new dependency; **(8) sync fails explicitly (409, no item created) whenever the run↔artifact association cannot be verified by SHA-256 (missing fingerprint, changed artifact, run active, artifact unavailable); acquired snapshots are immutable and verifiable via SHA-256 even after the artifact is overwritten.**

## 9. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Artifact overwritten → item loses meaning | Snapshot + sha256 + run id at sync (§3.1); superseded badge. |
| Migration corrupts an existing DB | Additive-only, single transaction, tested against a real v1 fixture; recommend (not force) backing up `quaero.db` in release notes. |
| Run↔artifact binding unverifiable / artifact overwritten | Fingerprint at completion (`run_artifacts`) + SHA-256 check at sync; explicit 409 (`binding_unverifiable`, `artifact_changed`, `run_active`, `artifact_unavailable`); nothing created on failure (§3.0). |
| Runs predating v0.9.0 have no fingerprint | Sync refuses (`binding_unverifiable`) with the remedy "re-run the project"; no backfill guessing. |
| Run in flight during sync → wrong binding | 409 `run_active`. |
| Declared reviewer mistaken for identity | `reviewer_source` pinned by CHECK, "declared/unverified" in API docs, event rows and UI; no auth wording anywhere. |
| Double submit / retries | `client_request_id` + unique index + `expected_status`. |
| Windows/OneDrive file locking | Review state is in SQLite (not JSON files), so the run-file retry hazards do not apply; keep short transactions. |
| Queue noise (too many items) | Defaults only R1/R3; R2/R4 opt-in; filters + pagination. |
| Config typos silently disable rules | Strict schema, unknown keys rejected, effective rules surfaced in sync response [P]. |
| Scope creep to auth/RBAC | Explicit non-goal; revisit in a later minor with its own plan. |
| Node 24 Vitest failure confuses results | Documented environmental issue; validate on Node 22 / CI. |

## 10. Files expected to change (on implementation, after approval)

New: `api/routers/review.py`, `api/services/review.py`, `tests/test_review_migration.py`, `tests/test_review_store.py`,
`tests/test_review_concurrency.py`, `tests/test_review_api.py`, `frontend/src/views/ReviewView.tsx` (+ detail/dialog components),
their Vitest tests, `frontend/e2e/review.spec.ts`, `review_rules.example.json`.
Modified: `db/store.py` (migration runner, review store functions), `api/schemas.py`, `api/main.py` (one `include_router`),
`frontend/src/types/api.ts`, `frontend/src/services/apiClient.ts`, navigation component(s) for the Review tab, `tests/_ts_interfaces.py`,
`CHANGELOG.md`, version strings (single source). Docs: README/ARCHITECTURE sections only at release time.
Also modified (prerequisite, §3.0): `api/services/runner.py` — one additive call after the pipeline returns, recording fingerprints; failure to fingerprint must not fail the run but leaves the run unverifiable. New table `run_artifacts`; `tests/test_review_binding.py`.
**Not touched:** pipelines, the existing `runs` columns/behaviour, Public Demo/GitHub Pages, Streamlit, historical docs, Notion.

## 11. Complexity estimate

Medium–large, roughly 5–7 focused working days: migration runner + schema + store ≈ 1; service/rules/router ≈ 1.5;
frontend view + dialog ≈ 1.5–2; tests (concurrency, migration, E2E) ≈ 1.5–2; docs/release ≈ 0.5. The migration runner and the
binding/snapshot are the highest-risk pieces. Approved split: three separate PRs, one `v0.9.0` tag after integration and validation. **PR 1:** migration (review tables + `run_artifacts`) + runner fingerprinting + store + binding/migration tests. **PR 2:** service + rules + API + tests. **PR 3:** UI + E2E + docs/version. The runner change is confined to PR 1 so PRs 2–3 do not alter run execution.

## 12. Approved decisions

| # | Decision | Approved |
|---|---|---|
| D1 | Generation trigger | Explicit idempotent `POST /review/sync` (no runner hook) |
| D2 | R3 (DQ `outlier_flag=true`) | On by default. R5/run-level items excluded |
| D3 | R4 owner thresholds | No default thresholds shipped |
| D4 | Terminal states | `ACCEPTED` and `REJECTED` are terminal |
| D5 | Correctable fields | Only `impact` and `outlier_flag`, via overlay |
| D6 | Decision status code | HTTP 200 on success |
| D7 | Superseded items | Kept forever, clearly identified |
| D8 | `reason` for ACCEPTED | Optional (mandatory for CORRECTED and REJECTED) |
| D9 | Rules file | `review_rules.json` in the project root |
| D10 | Release | Three separate PRs; single `v0.9.0` tag after integration and validation |

**Mandatory additional constraint (approval):** the run↔artifact association must be reliable before implementation;
"latest COMPLETED run" is insufficient if artifacts can be overwritten; if it is not verifiable, sync fails explicitly;
acquired snapshots stay immutable and verifiable via SHA-256. → §3.0 and acceptance criterion 8.

**Out of scope for v0.9.0 (noted, separate follow-up needing its own plan):** deleting browser workspaces and uploading
multiple datasets (facts and dimensions) from the project selector.
