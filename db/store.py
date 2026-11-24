"""SQLite persistence layer for Quaero 2.0.

Provides an idempotent-init, thread-safe store for:
  - project registry
  - run lifecycle (QUEUED → RUNNING → COMPLETED | FAILED)
  - mart metadata references
  - diagnostic timestamps

Design decisions
----------------
* SQLite with WAL journal mode: supports concurrent readers + one writer,
  suitable for the single-run-per-project model enforced by the API layer.
* No ORM / Alembic: schema is declared as plain DDL strings; migrations
  are additive-only and versioned via a user_version PRAGMA.
* Parquet artifacts are NOT duplicated; only paths are stored as metadata.
* QUAERO_DB_PATH env var controls the database file location.
  Defaults to <project_root>/quaero.db.
* All writes use explicit transactions to guarantee atomicity.
"""

from __future__ import annotations

import os
import sqlite3
import threading
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Schema version — bump when DDL changes (additive changes only)
# ---------------------------------------------------------------------------
_SCHEMA_VERSION = 2

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
_DDL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS schema_version (
    version     INTEGER NOT NULL,
    applied_at  TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
    name        TEXT PRIMARY KEY,
    root_path   TEXT NOT NULL,
    registered_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS runs (
    run_id       TEXT PRIMARY KEY,
    project_name TEXT NOT NULL REFERENCES projects(name),
    status       TEXT NOT NULL CHECK(status IN ('QUEUED','RUNNING','COMPLETED','FAILED')),
    question     TEXT,
    source_url   TEXT,
    created_at   TEXT NOT NULL,
    started_at   TEXT,
    completed_at TEXT,
    error        TEXT,
    artifact_path TEXT,
    FOREIGN KEY (project_name) REFERENCES projects(name)
);

CREATE TABLE IF NOT EXISTS marts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    project_name TEXT NOT NULL,
    mart_name    TEXT NOT NULL,
    parquet_path TEXT,
    row_count    INTEGER,
    registered_at TEXT NOT NULL,
    UNIQUE(project_name, mart_name),
    FOREIGN KEY (project_name) REFERENCES projects(name)
);

CREATE INDEX IF NOT EXISTS idx_runs_project ON runs(project_name);
CREATE INDEX IF NOT EXISTS idx_runs_status  ON runs(status);
CREATE INDEX IF NOT EXISTS idx_marts_project ON marts(project_name);
"""


# ---------------------------------------------------------------------------
# Migration v1 -> v2 (additive only: new tables, indexes and triggers).
# No ALTER/UPDATE/DELETE on projects, runs or marts. No backfill.
# ---------------------------------------------------------------------------
_MIGRATION_V2 = """
CREATE TABLE IF NOT EXISTS run_artifacts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    project     TEXT NOT NULL,
    run_id      TEXT,
    writer      TEXT NOT NULL CHECK(writer IN ('api','cli')),
    artifact    TEXT NOT NULL,
    path        TEXT NOT NULL,
    sha256      TEXT NOT NULL,
    size        INTEGER NOT NULL,
    recorded_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_run_artifacts_lookup ON run_artifacts(project, artifact, id);

CREATE TABLE IF NOT EXISTS review_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    project         TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    kind            TEXT NOT NULL CHECK(kind IN ('anomaly','dq')),
    ref             TEXT NOT NULL,
    rule_id         TEXT,
    original_json   TEXT NOT NULL,
    artifact_sha256 TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'REVIEW_REQUIRED'
        CHECK(status IN ('REVIEW_REQUIRED','ACCEPTED','CORRECTED','REJECTED')),
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE(project, run_id, kind, ref)
);

CREATE INDEX IF NOT EXISTS idx_review_items_project_status ON review_items(project, status);

CREATE TABLE IF NOT EXISTS review_events (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id           INTEGER NOT NULL REFERENCES review_items(id),
    from_status       TEXT NOT NULL,
    to_status         TEXT NOT NULL,
    reviewer          TEXT NOT NULL,
    reviewer_source   TEXT NOT NULL DEFAULT 'declared' CHECK(reviewer_source = 'declared'),
    reason            TEXT,
    client_request_id TEXT NOT NULL,
    at                TEXT NOT NULL,
    UNIQUE(item_id, client_request_id)
);

CREATE INDEX IF NOT EXISTS idx_review_events_item ON review_events(item_id, id);

CREATE TABLE IF NOT EXISTS review_corrections (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id      INTEGER NOT NULL REFERENCES review_items(id),
    event_id     INTEGER NOT NULL REFERENCES review_events(id),
    payload_json TEXT NOT NULL,
    created_at   TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS review_events_no_update BEFORE UPDATE ON review_events
BEGIN SELECT RAISE(ABORT, 'review_events is append-only'); END;

CREATE TRIGGER IF NOT EXISTS review_events_no_delete BEFORE DELETE ON review_events
BEGIN SELECT RAISE(ABORT, 'review_events is append-only'); END;

CREATE TRIGGER IF NOT EXISTS review_corrections_no_update BEFORE UPDATE ON review_corrections
BEGIN SELECT RAISE(ABORT, 'review_corrections is append-only'); END;

CREATE TRIGGER IF NOT EXISTS review_corrections_no_delete BEFORE DELETE ON review_corrections
BEGIN SELECT RAISE(ABORT, 'review_corrections is append-only'); END;

CREATE TRIGGER IF NOT EXISTS review_items_immutable_identity
BEFORE UPDATE OF project, run_id, kind, ref, rule_id, original_json, artifact_sha256, created_at
ON review_items
BEGIN SELECT RAISE(ABORT, 'review item identity and original are immutable'); END
"""


def _split_statements(script: str) -> list[str]:
    """Split a DDL script on ';' outside CREATE TRIGGER ... BEGIN ... END bodies."""
    statements: list[str] = []
    buf: list[str] = []
    in_trigger_body = False
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        buf.append(line)
        upper = stripped.upper()
        if upper.startswith("BEGIN"):
            in_trigger_body = True
        if in_trigger_body:
            if upper.endswith("END;") or upper == "END":
                in_trigger_body = False
                statements.append(chr(10).join(buf).strip().rstrip(";"))
                buf = []
            continue
        if stripped.endswith(";"):
            statements.append(chr(10).join(buf).strip().rstrip(";"))
            buf = []
    if buf:
        statements.append(chr(10).join(buf).strip().rstrip(";"))
    return [s for s in statements if s]


# ---------------------------------------------------------------------------
# Thread-local connection pool
# ---------------------------------------------------------------------------
_local = threading.local()


def _get_db_path() -> Path:
    env = os.environ.get("QUAERO_DB_PATH", "")
    if env:
        return Path(env)
    project_root = os.environ.get("QUAERO_PROJECT_ROOT", "")
    if project_root:
        return Path(project_root) / "quaero.db"
    # default: two levels up from this file (repo root)
    return Path(__file__).resolve().parent.parent / "quaero.db"


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


class SchemaTooNewError(RuntimeError):
    """The database was written by a newer Quaero than this code understands."""


def _apply_migration_v2(conn: sqlite3.Connection) -> None:
    for stmt in _split_statements(_MIGRATION_V2):
        conn.execute(stmt)


def init_db(db_path: Path | None = None) -> None:
    """Create or upgrade the schema (v1 -> v2) and stamp the version.

    Idempotent and additive. A fresh database gets the full schema; a v1 database
    gets only the v2 additions inside one IMMEDIATE transaction (rolled back whole
    on failure). A database newer than this code is refused, never downgraded.
    """
    path = db_path or _get_db_path()
    conn = _connect(path)
    conn.isolation_level = None  # explicit transaction control
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            for statement in _DDL.split(";"):
                stmt = statement.strip()
                if stmt and not stmt.upper().startswith("PRAGMA"):
                    conn.execute(stmt)
            current = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] or 0
            if current > _SCHEMA_VERSION:
                raise SchemaTooNewError(
                    f"database schema v{current} is newer than this Quaero (v{_SCHEMA_VERSION}); "
                    "upgrade Quaero instead of opening it with an older version"
                )
            if current < 2:
                _apply_migration_v2(conn)
                conn.execute(
                    "INSERT INTO schema_version(version, applied_at) VALUES (?, ?)",
                    (2, _now()),
                )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


@contextmanager
def get_connection(db_path: Path | None = None) -> Generator[sqlite3.Connection, None, None]:
    """Context manager returning a per-call connection (WAL allows concurrent reads)."""
    path = db_path or _get_db_path()
    conn = _connect(path)
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Project registry
# ---------------------------------------------------------------------------


def register_project(name: str, root_path: str, db_path: Path | None = None) -> None:
    """Upsert a project entry."""
    with get_connection(db_path) as conn, conn:
        conn.execute(
            """
                INSERT INTO projects(name, root_path, registered_at)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET root_path=excluded.root_path
                """,
            (name, root_path, _now()),
        )


def list_projects(db_path: Path | None = None) -> list[dict[str, Any]]:
    with get_connection(db_path) as conn:
        rows = conn.execute("SELECT * FROM projects ORDER BY name").fetchall()
        return [dict(r) for r in rows]


def get_project(name: str, db_path: Path | None = None) -> dict[str, Any] | None:
    with get_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM projects WHERE name = ?", (name,)).fetchone()
        return dict(row) if row else None


# ---------------------------------------------------------------------------
# Run lifecycle
# ---------------------------------------------------------------------------


def create_run(
    run_id: str,
    project_name: str,
    question: str | None = None,
    source_url: str | None = None,
    db_path: Path | None = None,
) -> None:
    """Insert a new run in QUEUED state. Raises if run_id already exists."""
    with get_connection(db_path) as conn, conn:
        conn.execute(
            """
                INSERT INTO runs(run_id, project_name, status, question, source_url, created_at)
                VALUES (?, ?, 'QUEUED', ?, ?, ?)
                """,
            (run_id, project_name, question, source_url, _now()),
        )


def transition_run(
    run_id: str,
    new_status: str,
    error: str | None = None,
    artifact_path: str | None = None,
    db_path: Path | None = None,
) -> None:
    """Transition run state with timestamp update.

    Allowed transitions:
      QUEUED → RUNNING
      RUNNING → COMPLETED | FAILED
      QUEUED → FAILED

    Raises KeyError if the run does not exist and ValueError if its current
    status does not allow the transition (including when another caller
    changed the status first).
    """
    allowed: dict[str, list[str]] = {
        "RUNNING": ["QUEUED"],
        "COMPLETED": ["RUNNING"],
        "FAILED": ["QUEUED", "RUNNING"],
    }
    if new_status not in allowed:
        raise ValueError(f"Invalid target status: {new_status!r}")

    ts_col = {
        "RUNNING": "started_at",
        "COMPLETED": "completed_at",
        "FAILED": "completed_at",
    }[new_status]
    sources = allowed[new_status]
    placeholders = ",".join("?" * len(sources))

    # Single conditional UPDATE: the status check and the write are one atomic
    # statement, so concurrent callers cannot both win the same transition.
    with get_connection(db_path) as conn, conn:
        cursor = conn.execute(
            f"""
            UPDATE runs
            SET status = ?,
                {ts_col} = ?,
                error = COALESCE(?, error),
                artifact_path = COALESCE(?, artifact_path)
            WHERE run_id = ? AND status IN ({placeholders})
            """,
            (new_status, _now(), error, artifact_path, run_id, *sources),
        )
        if cursor.rowcount == 0:
            row = conn.execute("SELECT status FROM runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                raise KeyError(f"run_id not found: {run_id!r}")
            raise ValueError(
                f"Cannot transition {row['status']!r} → {new_status!r} for run {run_id!r}"
            )


def get_run(run_id: str, db_path: Path | None = None) -> dict[str, Any] | None:
    with get_connection(db_path) as conn:
        row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        return dict(row) if row else None


def get_active_run(project_name: str, db_path: Path | None = None) -> dict[str, Any] | None:
    """Return the QUEUED or RUNNING run for a project, if any."""
    with get_connection(db_path) as conn:
        row = conn.execute(
            """
            SELECT * FROM runs
            WHERE project_name = ? AND status IN ('QUEUED', 'RUNNING')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (project_name,),
        ).fetchone()
        return dict(row) if row else None


def list_runs(
    project_name: str,
    limit: int = 50,
    db_path: Path | None = None,
) -> list[dict[str, Any]]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            """
            SELECT * FROM runs
            WHERE project_name = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (project_name, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def count_runs(project_name: str, db_path: Path | None = None) -> int:
    """Return the total number of runs recorded for a project."""
    with get_connection(db_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM runs WHERE project_name = ?", (project_name,)
        ).fetchone()
        return int(row["n"])


# ---------------------------------------------------------------------------
# Mart registry
# ---------------------------------------------------------------------------


def upsert_mart(
    project_name: str,
    mart_name: str,
    parquet_path: str | None = None,
    row_count: int | None = None,
    db_path: Path | None = None,
) -> None:
    with get_connection(db_path) as conn, conn:
        conn.execute(
            """
                INSERT INTO marts(project_name, mart_name, parquet_path, row_count, registered_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(project_name, mart_name)
                DO UPDATE SET
                    parquet_path = COALESCE(excluded.parquet_path, parquet_path),
                    row_count    = COALESCE(excluded.row_count, row_count),
                    registered_at = excluded.registered_at
                """,
            (project_name, mart_name, parquet_path, row_count, _now()),
        )


def list_marts_db(project_name: str, db_path: Path | None = None) -> list[dict[str, Any]]:
    with get_connection(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM marts WHERE project_name = ? ORDER BY mart_name",
            (project_name,),
        ).fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Run artifact provenance (v2)
# ---------------------------------------------------------------------------


def register_run_artifacts(
    project: str,
    run_id: str | None,
    writer: str,
    artifacts: list[dict[str, Any]],
    db_path: Path | None = None,
) -> None:
    """Insert all fingerprint rows in ONE transaction (all rows or none).

    Must be called while the project write lease is held (lease -> SQLite order).
    ``run_id`` is None for writes not managed by an API run (CLI / direct calls).
    """
    if not artifacts:
        return
    conn = _connect(db_path or _get_db_path())
    conn.isolation_level = None
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            now = _now()
            for art in artifacts:
                conn.execute(
                    "INSERT INTO run_artifacts"
                    "(project, run_id, writer, artifact, path, sha256, size, recorded_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        project,
                        run_id,
                        writer,
                        art["artifact"],
                        art["path"],
                        art["sha256"],
                        int(art["size"]),
                        now,
                    ),
                )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


def latest_run_artifact(
    project: str, artifact: str, db_path: Path | None = None
) -> dict[str, Any] | None:
    """Most recent registration row for (project, artifact), or None."""
    with get_connection(db_path) as conn:
        row = conn.execute(
            "SELECT * FROM run_artifacts WHERE project = ? AND artifact = ? "
            "ORDER BY id DESC LIMIT 1",
            (project, artifact),
        ).fetchone()
    return dict(row) if row else None
