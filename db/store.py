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
_SCHEMA_VERSION = 1

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


def init_db(db_path: Path | None = None) -> None:
    """Create tables if they don't exist and stamp the schema version.

    Idempotent: safe to call on every startup.
    """
    path = db_path or _get_db_path()
    conn = _connect(path)
    with conn:
        for statement in _DDL.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(stmt)
        row = conn.execute("SELECT version FROM schema_version").fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO schema_version(version, applied_at) VALUES (?, ?)",
                (_SCHEMA_VERSION, _now()),
            )
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
