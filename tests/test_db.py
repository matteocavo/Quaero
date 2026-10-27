"""Tests for the SQLite persistence layer (db.store).

All tests use a per-test temp database so they are fully isolated.
"""

from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from pathlib import Path

import pytest

from db import store

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Return a fresh database path for each test."""
    path = tmp_path / "test_quaero.db"
    store.init_db(path)
    return path


@pytest.fixture
def db_with_project(db_path: Path) -> tuple[Path, str]:
    store.register_project("test_proj", "/some/root/test_proj", db_path)
    return db_path, "test_proj"


# ---------------------------------------------------------------------------
# Database initialization
# ---------------------------------------------------------------------------


class TestDatabaseInit:
    def test_init_creates_tables(self, tmp_path: Path) -> None:
        path = tmp_path / "quaero.db"
        store.init_db(path)
        conn = sqlite3.connect(str(path))
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        conn.close()
        assert "projects" in tables
        assert "runs" in tables
        assert "marts" in tables
        assert "schema_version" in tables

    def test_init_stamps_schema_version(self, tmp_path: Path) -> None:
        path = tmp_path / "quaero.db"
        store.init_db(path)
        conn = sqlite3.connect(str(path))
        row = conn.execute("SELECT version FROM schema_version").fetchone()
        conn.close()
        assert row is not None
        assert row[0] == store._SCHEMA_VERSION

    def test_init_is_idempotent(self, tmp_path: Path) -> None:
        path = tmp_path / "quaero.db"
        store.init_db(path)
        store.init_db(path)  # second call must not raise
        conn = sqlite3.connect(str(path))
        count = conn.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0]
        conn.close()
        assert count == 1  # only one version row

    def test_init_creates_parent_dirs(self, tmp_path: Path) -> None:
        path = tmp_path / "nested" / "deep" / "quaero.db"
        store.init_db(path)
        assert path.exists()

    def test_wal_mode_enabled(self, db_path: Path) -> None:
        conn = sqlite3.connect(str(db_path))
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        conn.close()
        assert mode == "wal"


# ---------------------------------------------------------------------------
# Project registry
# ---------------------------------------------------------------------------


class TestProjectRegistry:
    def test_register_project(self, db_path: Path) -> None:
        store.register_project("proj_a", "/path/to/proj_a", db_path)
        proj = store.get_project("proj_a", db_path)
        assert proj is not None
        assert proj["name"] == "proj_a"
        assert proj["root_path"] == "/path/to/proj_a"

    def test_register_project_upserts(self, db_path: Path) -> None:
        store.register_project("proj_a", "/old/path", db_path)
        store.register_project("proj_a", "/new/path", db_path)
        proj = store.get_project("proj_a", db_path)
        assert proj["root_path"] == "/new/path"

    def test_list_projects(self, db_path: Path) -> None:
        store.register_project("alpha", "/a", db_path)
        store.register_project("beta", "/b", db_path)
        projects = store.list_projects(db_path)
        names = [p["name"] for p in projects]
        assert "alpha" in names
        assert "beta" in names

    def test_get_project_not_found(self, db_path: Path) -> None:
        assert store.get_project("nonexistent", db_path) is None


# ---------------------------------------------------------------------------
# Run creation
# ---------------------------------------------------------------------------


class TestRunCreation:
    def test_create_run(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        run_id = str(uuid.uuid4())
        store.create_run(run_id, project, question="test?", db_path=db_path)
        run = store.get_run(run_id, db_path)
        assert run is not None
        assert run["run_id"] == run_id
        assert run["status"] == "QUEUED"
        assert run["question"] == "test?"

    def test_create_run_sets_created_at(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        run_id = str(uuid.uuid4())
        store.create_run(run_id, project, db_path=db_path)
        run = store.get_run(run_id, db_path)
        assert run["created_at"] is not None
        assert "T" in run["created_at"]  # ISO format

    def test_create_run_duplicate_raises(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        run_id = str(uuid.uuid4())
        store.create_run(run_id, project, db_path=db_path)
        with pytest.raises(sqlite3.IntegrityError):
            store.create_run(run_id, project, db_path=db_path)

    def test_create_run_without_project_raises(self, db_path: Path) -> None:
        """Run creation with unknown project should raise FK violation."""
        run_id = str(uuid.uuid4())
        with pytest.raises(sqlite3.IntegrityError):
            store.create_run(run_id, "no_such_project", db_path=db_path)


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------


class TestStateTransitions:
    def _make_run(self, db_path: Path, project: str = "proj") -> str:
        store.register_project(project, "/p", db_path)
        run_id = str(uuid.uuid4())
        store.create_run(run_id, project, db_path=db_path)
        return run_id

    def test_queued_to_running(self, db_path: Path) -> None:
        run_id = self._make_run(db_path)
        store.transition_run(run_id, "RUNNING", db_path=db_path)
        assert store.get_run(run_id, db_path)["status"] == "RUNNING"

    def test_running_to_completed(self, db_path: Path) -> None:
        run_id = self._make_run(db_path)
        store.transition_run(run_id, "RUNNING", db_path=db_path)
        store.transition_run(run_id, "COMPLETED", db_path=db_path)
        run = store.get_run(run_id, db_path)
        assert run["status"] == "COMPLETED"
        assert run["completed_at"] is not None

    def test_running_to_failed(self, db_path: Path) -> None:
        run_id = self._make_run(db_path)
        store.transition_run(run_id, "RUNNING", db_path=db_path)
        store.transition_run(run_id, "FAILED", error="boom", db_path=db_path)
        run = store.get_run(run_id, db_path)
        assert run["status"] == "FAILED"
        assert run["error"] == "boom"

    def test_queued_to_failed_directly(self, db_path: Path) -> None:
        run_id = self._make_run(db_path)
        store.transition_run(run_id, "FAILED", error="instant fail", db_path=db_path)
        assert store.get_run(run_id, db_path)["status"] == "FAILED"

    def test_invalid_transition_raises(self, db_path: Path) -> None:
        run_id = self._make_run(db_path)
        with pytest.raises(ValueError):
            store.transition_run(run_id, "COMPLETED", db_path=db_path)  # QUEUED → COMPLETED invalid

    def test_invalid_status_raises(self, db_path: Path) -> None:
        run_id = self._make_run(db_path)
        with pytest.raises(ValueError):
            store.transition_run(run_id, "BANANA", db_path=db_path)

    def test_transition_unknown_run_raises(self, db_path: Path) -> None:
        with pytest.raises(KeyError):
            store.transition_run("no-such-id", "RUNNING", db_path=db_path)

    def test_started_at_set_on_running(self, db_path: Path) -> None:
        run_id = self._make_run(db_path)
        store.transition_run(run_id, "RUNNING", db_path=db_path)
        assert store.get_run(run_id, db_path)["started_at"] is not None


# ---------------------------------------------------------------------------
# Run history
# ---------------------------------------------------------------------------


class TestRunHistory:
    def test_list_runs_empty(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        runs = store.list_runs(project, db_path=db_path)
        assert runs == []

    def test_list_runs_returns_all(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        for _ in range(3):
            store.create_run(str(uuid.uuid4()), project, db_path=db_path)
        runs = store.list_runs(project, db_path=db_path)
        assert len(runs) == 3

    def test_list_runs_most_recent_first(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        ids = [str(uuid.uuid4()) for _ in range(3)]
        for rid in ids:
            store.create_run(rid, project, db_path=db_path)
            time.sleep(0.01)  # ensure distinct timestamps
        runs = store.list_runs(project, db_path=db_path)
        assert runs[0]["run_id"] == ids[-1]  # newest first

    def test_list_runs_limit(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        for _ in range(10):
            store.create_run(str(uuid.uuid4()), project, db_path=db_path)
        runs = store.list_runs(project, limit=3, db_path=db_path)
        assert len(runs) == 3

    def test_list_runs_project_isolation(self, db_path: Path) -> None:
        store.register_project("proj_x", "/x", db_path)
        store.register_project("proj_y", "/y", db_path)
        store.create_run(str(uuid.uuid4()), "proj_x", db_path=db_path)
        runs_y = store.list_runs("proj_y", db_path=db_path)
        assert runs_y == []


# ---------------------------------------------------------------------------
# Mart registry
# ---------------------------------------------------------------------------


class TestMartRegistry:
    def test_upsert_mart(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        store.upsert_mart(project, "kpi_summary", "/path/kpi.parquet", 100, db_path)
        marts = store.list_marts_db(project, db_path)
        assert len(marts) == 1
        assert marts[0]["mart_name"] == "kpi_summary"

    def test_upsert_mart_is_idempotent(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        store.upsert_mart(project, "sales", "/p.parquet", 50, db_path)
        store.upsert_mart(project, "sales", "/p.parquet", 75, db_path)
        marts = store.list_marts_db(project, db_path)
        assert len(marts) == 1
        assert marts[0]["row_count"] == 75

    def test_list_marts_multiple(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        store.upsert_mart(project, "b_mart", None, None, db_path)
        store.upsert_mart(project, "a_mart", None, None, db_path)
        marts = store.list_marts_db(project, db_path)
        assert [m["mart_name"] for m in marts] == ["a_mart", "b_mart"]  # sorted

    def test_mart_isolation(self, db_path: Path) -> None:
        store.register_project("pa", "/a", db_path)
        store.register_project("pb", "/b", db_path)
        store.upsert_mart("pa", "mart1", None, None, db_path)
        assert store.list_marts_db("pb", db_path) == []


# ---------------------------------------------------------------------------
# Active run detection
# ---------------------------------------------------------------------------


class TestActiveRun:
    def test_active_run_queued(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        run_id = str(uuid.uuid4())
        store.create_run(run_id, project, db_path=db_path)
        active = store.get_active_run(project, db_path)
        assert active is not None
        assert active["run_id"] == run_id

    def test_active_run_running(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        run_id = str(uuid.uuid4())
        store.create_run(run_id, project, db_path=db_path)
        store.transition_run(run_id, "RUNNING", db_path=db_path)
        assert store.get_active_run(project, db_path) is not None

    def test_no_active_run_when_completed(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        run_id = str(uuid.uuid4())
        store.create_run(run_id, project, db_path=db_path)
        store.transition_run(run_id, "RUNNING", db_path=db_path)
        store.transition_run(run_id, "COMPLETED", db_path=db_path)
        assert store.get_active_run(project, db_path) is None


# ---------------------------------------------------------------------------
# Concurrent access
# ---------------------------------------------------------------------------


class TestConcurrentAccess:
    def test_concurrent_reads(self, db_with_project: tuple[Path, str]) -> None:
        db_path, project = db_with_project
        run_id = str(uuid.uuid4())
        store.create_run(run_id, project, db_path=db_path)

        errors: list[Exception] = []

        def read() -> None:
            try:
                store.get_run(run_id, db_path)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=read) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []

    def test_concurrent_project_registration(self, tmp_path: Path) -> None:
        db_path = tmp_path / "concurrent.db"
        store.init_db(db_path)
        errors: list[Exception] = []

        def reg(i: int) -> None:
            try:
                store.register_project(f"proj_{i}", f"/p/{i}", db_path)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=reg, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        projects = store.list_projects(db_path)
        assert len(projects) == 10


# ---------------------------------------------------------------------------
# Restart recovery
# ---------------------------------------------------------------------------


class TestRestartRecovery:
    def test_run_survives_reconnect(self, db_with_project: tuple[Path, str]) -> None:
        """Data written to SQLite must be readable via a new connection."""
        db_path, project = db_with_project
        run_id = str(uuid.uuid4())
        store.create_run(run_id, project, question="persist me", db_path=db_path)
        store.transition_run(run_id, "RUNNING", db_path=db_path)

        # New connection (simulates restart)
        run = store.get_run(run_id, db_path)
        assert run is not None
        assert run["status"] == "RUNNING"
        assert run["question"] == "persist me"

    def test_project_survives_reconnect(self, db_path: Path) -> None:
        store.register_project("durable", "/dur", db_path)
        proj = store.get_project("durable", db_path)
        assert proj is not None


# ---------------------------------------------------------------------------
# Backward compatibility
# ---------------------------------------------------------------------------


class TestBackwardCompatibility:
    """Verify that the persistence layer doesn't break existing contracts."""

    def test_db_module_imports_cleanly(self) -> None:
        from db import store as s

        assert hasattr(s, "init_db")
        assert hasattr(s, "create_run")
        assert hasattr(s, "get_run")
        assert hasattr(s, "transition_run")
        assert hasattr(s, "list_runs")
        assert hasattr(s, "register_project")
        assert hasattr(s, "list_projects")
        assert hasattr(s, "upsert_mart")
        assert hasattr(s, "list_marts_db")

    def test_runner_create_run_without_db(self, tmp_path: Path) -> None:
        """create_run in runner.py must still work when DB is not configured."""
        import os

        os.environ.pop("QUAERO_DB_PATH", None)
        os.environ["QUAERO_DB_PATH"] = str(tmp_path / "runner_test.db")
        store.init_db(tmp_path / "runner_test.db")

        from api.services.runner import create_run as api_create_run

        run_state = api_create_run(tmp_path, "no_such_project")
        assert "run_id" in run_state
        assert run_state["status"] == "QUEUED"
