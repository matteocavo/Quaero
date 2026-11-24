from __future__ import annotations

import sqlite3

import pytest

from db import store

V2_TABLES = {"review_items", "review_events", "review_corrections", "run_artifacts"}


def _tables(path):
    with sqlite3.connect(path) as c:
        return {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _version(path):
    with sqlite3.connect(path) as c:
        return c.execute("SELECT MAX(version) FROM schema_version").fetchone()[0]


def _make_v1(path):
    """A genuine v1 database: base DDL, version 1, with rows in every legacy table."""
    c = sqlite3.connect(path)
    for stmt in store._DDL.split(";"):
        s = stmt.strip()
        if s and not s.upper().startswith("PRAGMA"):
            c.execute(s)
    c.execute("INSERT INTO schema_version VALUES (1, '2026-01-01T00:00:00')")
    c.execute("INSERT INTO projects VALUES ('p', '/x', '2026-01-01')")
    c.execute(
        "INSERT INTO runs(run_id, project_name, status, created_at) VALUES ('r1','p','COMPLETED','2026-01-01')"
    )
    c.execute(
        "INSERT INTO marts(project_name, mart_name, registered_at) VALUES ('p','m','2026-01-01')"
    )
    c.commit()
    c.close()


def _legacy_rows(path):
    with sqlite3.connect(path) as c:
        return [
            c.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall()
            for t in ("projects", "runs", "marts")
        ]


def test_fresh_db_reaches_v2(tmp_path):
    db = tmp_path / "a.db"
    store.init_db(db)
    assert V2_TABLES <= _tables(db)
    assert _version(db) == 2


def test_v1_fixture_upgrades_without_touching_legacy_rows(tmp_path):
    db = tmp_path / "v1.db"
    _make_v1(db)
    before = _legacy_rows(db)
    store.init_db(db)
    assert _version(db) == 2
    assert V2_TABLES <= _tables(db)
    assert _legacy_rows(db) == before


def test_rerun_is_noop(tmp_path):
    db = tmp_path / "v1.db"
    _make_v1(db)
    store.init_db(db)
    store.init_db(db)
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT COUNT(*) FROM schema_version WHERE version=2").fetchone()[0] == 1


def test_mid_migration_failure_rolls_back_to_valid_v1(tmp_path, monkeypatch):
    db = tmp_path / "v1.db"
    _make_v1(db)
    before = _legacy_rows(db)

    def boom(conn):
        conn.execute("CREATE TABLE review_items(id INTEGER)")
        raise RuntimeError("boom")

    monkeypatch.setattr(store, "_apply_migration_v2", boom)
    with pytest.raises(RuntimeError):
        store.init_db(db)
    assert _version(db) == 1
    assert not (V2_TABLES & _tables(db))
    assert _legacy_rows(db) == before
    monkeypatch.undo()
    store.init_db(db)  # retry succeeds
    assert _version(db) == 2


def test_newer_db_is_refused(tmp_path):
    db = tmp_path / "new.db"
    store.init_db(db)
    with sqlite3.connect(db) as c:
        c.execute("INSERT INTO schema_version VALUES (99, 'x')")
    with pytest.raises(store.SchemaTooNewError):
        store.init_db(db)
    assert _version(db) == 99


# ------------------------------------------------------------------ integrity / append-only
@pytest.fixture
def db(tmp_path):
    path = tmp_path / "r.db"
    store.init_db(path)
    c = sqlite3.connect(path)
    c.execute("PRAGMA foreign_keys=ON")
    c.execute(
        "INSERT INTO review_items(project, run_id, kind, ref, original_json, artifact_sha256, created_at, updated_at)"
        " VALUES ('p','r1','anomaly','a1','{}','h','t','t')"
    )
    c.execute(
        "INSERT INTO review_events(item_id, from_status, to_status, reviewer, client_request_id, at)"
        " VALUES (1,'REVIEW_REQUIRED','ACCEPTED','me','req1','t')"
    )
    c.execute(
        "INSERT INTO review_corrections(item_id, event_id, payload_json, created_at) VALUES (1,1,'{}','t')"
    )
    c.commit()
    yield c
    c.close()


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE review_events SET reason='x'",
        "DELETE FROM review_events",
        "UPDATE review_corrections SET payload_json='y'",
        "DELETE FROM review_corrections",
        "UPDATE review_items SET original_json='z'",
        "UPDATE review_items SET artifact_sha256='z'",
        "UPDATE review_items SET run_id='r2'",
    ],
)
def test_append_only_and_immutable(db, sql):
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(sql)


def test_item_status_still_updatable(db):
    db.execute("UPDATE review_items SET status='ACCEPTED', updated_at='t2' WHERE id=1")


@pytest.mark.parametrize(
    "sql",
    [
        (
            "INSERT INTO review_items(project,run_id,kind,ref,original_json,artifact_sha256,created_at,updated_at)"
            " VALUES ('p','r1','anomaly','a1','{}','h','t','t')"
        ),  # duplicate unique
        (
            "INSERT INTO review_items(project,run_id,kind,ref,original_json,artifact_sha256,created_at,updated_at)"
            " VALUES ('p','r1','bogus','a2','{}','h','t','t')"
        ),  # bad kind
        (
            "INSERT INTO review_events(item_id,from_status,to_status,reviewer,client_request_id,at)"
            " VALUES (1,'a','b','me','req1','t')"
        ),  # duplicate request id
        (
            "INSERT INTO review_events(item_id,from_status,to_status,reviewer,reviewer_source,client_request_id,at)"
            " VALUES (1,'a','b','me','verified','req9','t')"
        ),  # only 'declared'
        (
            "INSERT INTO review_events(item_id,from_status,to_status,reviewer,client_request_id,at)"
            " VALUES (999,'a','b','me','req8','t')"
        ),  # FK
        (
            "INSERT INTO run_artifacts(project,writer,artifact,path,sha256,size,recorded_at)"
            " VALUES ('p','bogus','a','p','h',1,'t')"
        ),
    ],
)
def test_constraints(db, sql):
    with pytest.raises(sqlite3.IntegrityError):
        db.execute(sql)
