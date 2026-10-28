"""Local data lifecycle: classification, confirmed cleanup, and the safety rules around deletion."""

from __future__ import annotations

import io
import json
import os
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from api import lifecycle_cli
from api.dependencies import get_project_root
from api.main import app
from api.services import lifecycle
from db import store

KEY = "lifecycle-test-key"
MINE = "session_current00"
EARLIER = "session_earlier00"
CSV = "region,revenue\nNorth,10\nSouth,20\n"


@pytest.fixture()
def root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("QUAERO_API_KEY", KEY)
    monkeypatch.setenv(lifecycle.SESSION_ENV, MINE)
    db_path = tmp_path / "t.db"
    monkeypatch.setenv("QUAERO_DB_PATH", str(db_path))
    store.init_db(db_path)
    (tmp_path / "uploads").mkdir()
    (tmp_path / "projects").mkdir()
    return tmp_path.resolve()


@pytest.fixture()
def client(root: Path, monkeypatch: pytest.MonkeyPatch):
    # The storage router reads the project root from the environment, like the launcher sets it.
    monkeypatch.setenv("QUAERO_PROJECT_ROOT", str(root))
    app.dependency_overrides[get_project_root] = lambda: (
        root
    )  # the upload/project routers still use injection
    yield TestClient(app, raise_server_exceptions=False)
    app.dependency_overrides.clear()


def auth() -> dict[str, str]:
    return {"X-API-Key": KEY}


def make_upload(root: Path, n: int, session: str | None = MINE, name: str = "sales.csv") -> str:
    upload_id = f"upload_{n:012x}"
    (root / "uploads" / f"{upload_id}.csv").write_text(CSV, encoding="utf-8")
    meta = {
        "upload_id": upload_id,
        "original_filename": name,
        "created_at": f"2026-01-0{n % 9 + 1}T00:00:00+00:00",
    }
    if session:
        meta["session_id"] = session
    (root / "uploads" / f"{upload_id}.json").write_text(json.dumps(meta), encoding="utf-8")
    return upload_id


def make_project(root: Path, name: str, upload_id: str | None, artifacts: bool = True) -> Path:
    project = root / "projects" / name
    project.mkdir(parents=True)
    config: dict = {"project": name, "question": "q", "datasets": [{"name": "d", "path": "x.csv"}]}
    if upload_id:
        config["upload_id"] = upload_id
        config["datasets"][0]["path"] = f"uploads/{upload_id}.csv"
    (project / "project_config.json").write_text(json.dumps(config), encoding="utf-8")
    for sub in ("raw", "staging", "marts", "metadata"):
        (project / sub).mkdir()
        if artifacts:
            (project / sub / f"{sub}_file.bin").write_bytes(b"x" * 100)
    return project


def existing(root: Path) -> set[str]:
    return {p.name for p in (root / "uploads").iterdir()}


def cleanup(client: TestClient, **body):
    return client.post("/storage/cleanup", json=body, headers=auth())


# ------------------------------------------------------------------------------------------ policy


class TestPolicy:
    def test_default_is_ask_on_exit(self, root: Path) -> None:
        assert lifecycle.get_policy(root) == "ask_on_exit"

    @pytest.mark.parametrize("policy", ["ask_on_exit", "auto_cleanup", "keep_files"])
    def test_each_policy_can_be_saved(self, client: TestClient, root: Path, policy: str) -> None:
        resp = client.post("/storage/policy", json={"lifecycle_policy": policy}, headers=auth())
        assert resp.status_code == 200 and resp.json() == {"lifecycle_policy": policy}
        assert lifecycle.get_policy(root) == policy
        assert client.get("/storage", headers=auth()).json()["lifecycle_policy"] == policy

    def test_unknown_policy_is_rejected(self, client: TestClient, root: Path) -> None:
        assert (
            client.post(
                "/storage/policy", json={"lifecycle_policy": "delete_everything"}, headers=auth()
            ).status_code
            == 422
        )
        assert lifecycle.get_policy(root) == "ask_on_exit"

    @pytest.mark.parametrize("content", ["{not json", '{"lifecycle_policy": "rm -rf"}', "[]", ""])
    def test_a_damaged_policy_file_means_the_safe_default(self, root: Path, content: str) -> None:
        (root / "uploads" / "_lifecycle.json").write_text(content, encoding="utf-8")
        assert lifecycle.get_policy(root) == "ask_on_exit"


# ------------------------------------------------------------------------------------ classification


class TestClassification:
    def test_uploads_are_told_apart_by_owner_and_use(self, client: TestClient, root: Path) -> None:
        mine = make_upload(root, 1)
        earlier = make_upload(root, 2, session=EARLIER)
        unknown = make_upload(root, 3, session=None)
        saved = make_upload(root, 4, session=EARLIER)
        make_project(root, "my_proj", saved)

        data = client.get("/storage", headers=auth()).json()
        state = {u["upload_id"]: u["state"] for u in data["uploads"]}
        assert state == {
            mine: "current_session",
            earlier: "orphaned",
            unknown: "orphaned",
            saved: "saved",
        }
        assert data["current_session_uploads"] == 1 and data["orphaned_uploads"] == 2
        assert next(u for u in data["uploads"] if u["upload_id"] == saved)["linked_projects"] == [
            "my_proj"
        ]

    def test_a_hand_written_dataset_path_also_counts_as_use(self, root: Path) -> None:
        upload = make_upload(root, 1)
        make_project(root, "manual", None)
        config = root / "projects" / "manual" / "project_config.json"
        config.write_text(
            json.dumps({"datasets": [{"path": f"uploads/{upload}.csv"}]}), encoding="utf-8"
        )
        assert lifecycle.get_storage_status(root).uploads[0].state == "saved"

    def test_sizes_are_reported_per_category(self, client: TestClient, root: Path) -> None:
        make_upload(root, 1)
        saved = make_upload(root, 2)
        make_project(root, "p", saved)
        totals = client.get("/storage", headers=auth()).json()["totals"]
        csv_and_meta = totals["temporary_uploads_bytes"]
        assert csv_and_meta > len(CSV) and totals["saved_uploads_bytes"] > len(CSV)
        assert totals["artifacts_bytes"] == 400  # 4 folders x 100 bytes
        assert totals["total_bytes"] == sum(v for k, v in totals.items() if k != "total_bytes")

    def test_projects_not_created_from_an_upload_are_marked_preexisting(
        self, client: TestClient, root: Path
    ) -> None:
        make_project(root, "bundled_demo", None)
        make_project(root, "mine", make_upload(root, 1))
        kinds = {
            p["project"]: (p["kind"], p["can_clear_artifacts"])
            for p in client.get("/storage", headers=auth()).json()["projects"]
        }
        assert kinds == {"bundled_demo": ("preexisting", False), "mine": ("upload_project", True)}

    def test_an_incomplete_upload_is_flagged(self, client: TestClient, root: Path) -> None:
        upload = make_upload(root, 1)
        (root / "uploads" / f"{upload}.json").unlink()
        item = client.get("/storage", headers=auth()).json()["uploads"][0]
        assert item["incomplete"] is True and item["state"] == "orphaned"


# ------------------------------------------------------------------------------------------ cleanup


class TestConfirmedCleanup:
    def test_nothing_is_deleted_without_confirmation(self, client: TestClient, root: Path) -> None:
        make_upload(root, 1)
        before = existing(root)
        for confirm in (False,):
            resp = cleanup(client, scope="current_session", confirm=confirm)
            assert resp.status_code == 422 and "confirm" in resp.json()["detail"].lower()
        assert (
            client.post(
                "/storage/cleanup", json={"scope": "current_session"}, headers=auth()
            ).status_code
            == 422
        )
        assert existing(root) == before

    def test_current_session_removes_only_this_sessions_unused_uploads(
        self, client: TestClient, root: Path
    ) -> None:
        mine = make_upload(root, 1)
        earlier = make_upload(root, 2, session=EARLIER)
        saved = make_upload(root, 3)  # created by this session, but a project uses it
        make_project(root, "keep_me", saved)
        stranger = root / "uploads" / "notes.txt"
        stranger.write_text("not ours", encoding="utf-8")
        lookalike = root / "uploads" / "upload_zzzzzzzzzzzz.csv"
        lookalike.write_text("x", encoding="utf-8")

        body = cleanup(client, scope="current_session", confirm=True).json()
        assert body["deleted"] == [mine] and body["skipped"] == []
        assert body["freed_bytes"] > 0
        assert existing(root) == {
            f"{earlier}.csv",
            f"{earlier}.json",
            f"{saved}.csv",
            f"{saved}.json",
            "notes.txt",
            "upload_zzzzzzzzzzzz.csv",
        }
        assert (root / "projects" / "keep_me" / "project_config.json").is_file()

    def test_orphaned_removes_earlier_and_unknown_sessions_but_never_this_one(
        self, client: TestClient, root: Path
    ) -> None:
        mine = make_upload(root, 1)
        earlier = make_upload(root, 2, session=EARLIER)
        unknown = make_upload(root, 3, session=None)
        body = cleanup(client, scope="orphaned", confirm=True).json()
        assert sorted(body["deleted"]) == sorted([earlier, unknown])
        assert existing(root) == {f"{mine}.csv", f"{mine}.json"}

    def test_a_single_upload_of_any_session_can_go_if_unused(
        self, client: TestClient, root: Path
    ) -> None:
        earlier = make_upload(root, 1, session=EARLIER)
        other = make_upload(root, 2)
        assert cleanup(client, scope="single_upload", target_id=earlier, confirm=True).json()[
            "deleted"
        ] == [earlier]
        assert existing(root) == {f"{other}.csv", f"{other}.json"}

    def test_an_upload_used_by_a_project_is_refused(self, client: TestClient, root: Path) -> None:
        saved = make_upload(root, 1)
        make_project(root, "proj", saved)
        resp = cleanup(client, scope="single_upload", target_id=saved, confirm=True)
        assert resp.status_code == 409 and "proj" in resp.json()["detail"]
        assert (root / "uploads" / f"{saved}.csv").is_file()

    def test_an_unknown_upload_is_404(self, client: TestClient, root: Path) -> None:
        assert (
            cleanup(
                client, scope="single_upload", target_id="upload_aaaaaaaaaaaa", confirm=True
            ).status_code
            == 404
        )

    def test_an_upload_created_during_the_run_of_cleanup_is_rechecked(
        self, root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A project that starts using an upload after the first scan must still protect it."""
        upload = make_upload(root, 1)
        real = lifecycle.referenced_uploads
        calls = {"n": 0}

        def late(project_root: Path):
            calls["n"] += 1
            if calls["n"] == 1:
                return {}  # the first look (the scan) sees no project yet
            make_project_once()
            return real(project_root)

        created = {"done": False}

        def make_project_once() -> None:
            if not created["done"]:
                created["done"] = True
                make_project(root, "late_project", upload)

        monkeypatch.setattr(lifecycle, "referenced_uploads", late)
        result = lifecycle.execute_cleanup(root, "current_session", confirm=True)
        assert (
            result.deleted == [] and result.skipped and "late_project" in result.skipped[0].reason
        )
        assert (root / "uploads" / f"{upload}.csv").is_file()


# ------------------------------------------------------------------------------- ids and path traversal


class TestPathTraversal:
    @pytest.mark.parametrize(
        "target",
        [
            "../x",
            "..\\x",
            "upload_../../etc",
            "upload_0123456789ab/../x",
            "upload_0123456789ab.csv",
            "UPLOAD_0123456789AB",
            "upload_0123456789a",
            "upload_0123456789abc",
            "/etc/passwd",
            "C:\\Windows",
            "",
            "upload_%2e%2e%2f",
            "upload_0123456789ab\x00",
        ],
    )
    def test_single_upload_accepts_only_an_upload_id(
        self, client: TestClient, root: Path, target: str
    ) -> None:
        survivor = make_upload(root, 1)
        before = existing(root)
        resp = cleanup(client, scope="single_upload", target_id=target, confirm=True)
        assert resp.status_code in (404, 422)
        assert existing(root) == before and survivor

    @pytest.mark.parametrize(
        "target",
        ["../uploads", "..", ".", "projects", "a/b", "..\\..", "/", "C:\\", "no_such_project"],
    )
    def test_project_artifacts_accepts_only_an_existing_project_name(
        self, client: TestClient, root: Path, target: str
    ) -> None:
        upload = make_upload(root, 1)
        make_project(root, "mine", upload)
        before = sorted(str(p.relative_to(root)) for p in root.rglob("*"))
        resp = cleanup(client, scope="project_artifacts", target_id=target, confirm=True)
        assert resp.status_code in (404, 422)
        assert sorted(str(p.relative_to(root)) for p in root.rglob("*")) == before

    def test_unknown_scope_is_rejected(self, client: TestClient, root: Path) -> None:
        make_upload(root, 1)
        assert cleanup(client, scope="everything", confirm=True).status_code == 422
        assert len(existing(root)) == 2

    def test_files_that_only_look_like_uploads_are_never_touched(self, root: Path) -> None:
        names = [
            "upload_0123456789ab.csv.bak",
            "upload_0123456789ab.csv~",
            "upload_0123456789AB.csv",
            "upload_01.csv",
            "x_upload_0123456789ab.csv",
        ]
        for name in names:
            (root / "uploads" / name).write_text("keep", encoding="utf-8")
        (root / "uploads" / "sub").mkdir()
        (root / "uploads" / "sub" / "upload_0123456789ab.csv").write_text("keep", encoding="utf-8")
        lifecycle.execute_cleanup(root, "orphaned", confirm=True)
        lifecycle.execute_cleanup(root, "current_session", confirm=True)
        assert all((root / "uploads" / n).is_file() for n in names)
        assert (root / "uploads" / "sub" / "upload_0123456789ab.csv").is_file()


class TestProjectArtifacts:
    def test_clearing_keeps_the_configuration_and_the_folders(
        self, client: TestClient, root: Path
    ) -> None:
        upload = make_upload(root, 1)
        project = make_project(root, "mine", upload)
        body = cleanup(client, scope="project_artifacts", target_id="mine", confirm=True).json()
        assert body["freed_bytes"] == 400 and body["skipped"] == []
        assert (project / "project_config.json").is_file()
        assert all(
            (project / s).is_dir() and not any((project / s).iterdir())
            for s in ("raw", "staging", "marts", "metadata")
        )
        assert (
            root / "uploads" / f"{upload}.csv"
        ).is_file()  # the source stays: the project still uses it

    def test_a_preexisting_project_is_never_cleared(self, client: TestClient, root: Path) -> None:
        project = make_project(root, "bundled", None)
        resp = cleanup(client, scope="project_artifacts", target_id="bundled", confirm=True)
        assert resp.status_code == 409
        assert (project / "marts" / "marts_file.bin").is_file()

    def test_a_project_with_a_run_in_progress_is_refused(
        self, client: TestClient, root: Path
    ) -> None:
        project = make_project(root, "busy", make_upload(root, 1))
        (root / "runs").mkdir()
        (root / "runs" / "run_1.json").write_text(
            json.dumps({"run_id": "run_1", "project": "busy", "status": "RUNNING"}),
            encoding="utf-8",
        )
        resp = cleanup(client, scope="project_artifacts", target_id="busy", confirm=True)
        assert resp.status_code == 409 and "run in progress" in resp.json()["detail"]
        assert (project / "raw" / "raw_file.bin").is_file()


# ------------------------------------------------------------------------- symlinks, locks, partial


def _link_dir(link: Path, target: Path) -> str:
    """A directory link that really exists: a symlink, or on Windows without privilege a junction."""
    try:
        link.symlink_to(target, target_is_directory=True)
        return "symlink"
    except (OSError, NotImplementedError):
        if sys.platform != "win32":
            pytest.skip("symbolic links are not available here")
    import subprocess

    done = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(link), str(target)],
        check=False,
        capture_output=True,
        text=True,
    )
    if done.returncode != 0:
        pytest.skip(f"could not create a junction: {done.stderr or done.stdout}")
    return "junction"


def _unlink_dir_link(link: Path) -> None:
    try:
        link.unlink()
    except (OSError, PermissionError):
        os.rmdir(link)  # a junction is removed like an empty directory, without touching its target


class TestLinks:
    def test_a_linked_upload_file_is_skipped_and_its_target_survives(
        self, root: Path, tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        outside = tmp_path_factory.mktemp("outside") / "precious.txt"
        outside.write_text("do not delete", encoding="utf-8")
        link = root / "uploads" / "upload_0000000000aa.csv"
        meta = root / "uploads" / "upload_0000000000aa.json"
        meta.write_text(json.dumps({"session_id": MINE}), encoding="utf-8")
        try:
            link.symlink_to(outside)
        except (OSError, NotImplementedError):
            # No privilege to create one: make the operating system report this file as a symlink instead.
            link.write_text("stand-in", encoding="utf-8")
            real_lstat = Path.lstat

            def lstat(self: Path, *a, **k):
                info = real_lstat(self, *a, **k)
                if self == link:
                    values = list(info)
                    values[0] = (info.st_mode & ~0o170000) | 0o120000  # S_IFLNK
                    return os.stat_result(values)
                return info

            monkeypatch.setattr(Path, "lstat", lstat)

        result = lifecycle.execute_cleanup(root, "current_session", confirm=True)
        assert outside.read_text(encoding="utf-8") == "do not delete"
        assert link.exists() or link.is_symlink()
        assert meta.is_file()  # nothing was done to the upload at all
        assert result.deleted == [] and result.freed_bytes == 0
        assert "link" in result.skipped[0].reason

    def test_a_linked_uploads_folder_is_refused_outright(
        self,
        tmp_path: Path,
        tmp_path_factory: pytest.TempPathFactory,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv(lifecycle.SESSION_ENV, MINE)
        project_root = tmp_path.resolve()
        elsewhere = tmp_path_factory.mktemp("elsewhere")
        victim = elsewhere / "upload_0000000000bb.csv"
        victim.write_text("x", encoding="utf-8")
        (elsewhere / "upload_0000000000bb.json").write_text(
            json.dumps({"session_id": MINE}), encoding="utf-8"
        )
        _link_dir(project_root / "uploads", elsewhere)
        try:
            for scope in ("current_session", "orphaned"):
                with pytest.raises(lifecycle.LifecycleError):
                    lifecycle.execute_cleanup(project_root, scope, confirm=True)
            with pytest.raises(lifecycle.LifecycleError):
                lifecycle.get_storage_status(project_root)
            assert victim.is_file() and (elsewhere / "upload_0000000000bb.json").is_file()
        finally:
            _unlink_dir_link(project_root / "uploads")

    def test_a_linked_folder_inside_a_project_is_not_entered_when_clearing(
        self, root: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        outside_dir = tmp_path_factory.mktemp("outside_dir")
        (outside_dir / "keep.txt").write_text("keep", encoding="utf-8")
        (outside_dir / "nested").mkdir()
        (outside_dir / "nested" / "keep_too.txt").write_text("keep", encoding="utf-8")
        project = make_project(root, "mine", make_upload(root, 1))
        _link_dir(project / "marts" / "linked", outside_dir)
        try:
            result = lifecycle.execute_cleanup(
                root, "project_artifacts", confirm=True, target_id="mine"
            )
            assert (outside_dir / "keep.txt").read_text(encoding="utf-8") == "keep"
            assert (outside_dir / "nested" / "keep_too.txt").read_text(encoding="utf-8") == "keep"
            assert (
                result.freed_bytes == 400
            )  # only the project's own files were counted and removed
            assert lifecycle.get_storage_status(root).projects[0].artifacts_bytes == 0
        finally:
            _unlink_dir_link(project / "marts" / "linked")

    def test_a_linked_project_folder_is_ignored(
        self, root: Path, tmp_path_factory: pytest.TempPathFactory
    ) -> None:
        real = tmp_path_factory.mktemp("real_project")
        (real / "project_config.json").write_text(
            json.dumps({"upload_id": "upload_0000000000cc"}), encoding="utf-8"
        )
        _link_dir(root / "projects" / "linked_proj", real)
        try:
            assert [p.project for p in lifecycle.get_storage_status(root).projects] == []
            with pytest.raises(lifecycle.LifecycleNotFound):
                lifecycle.execute_cleanup(
                    root, "project_artifacts", confirm=True, target_id="linked_proj"
                )
        finally:
            _unlink_dir_link(root / "projects" / "linked_proj")


class TestFilesInUseAndPartialDeletion:
    def test_a_file_that_cannot_be_deleted_is_reported_and_the_rest_proceeds(
        self, root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        locked = make_upload(root, 1)
        free = make_upload(root, 2)
        real_unlink = Path.unlink

        def unlink(self: Path, *a, **k):
            if self.name == f"{locked}.csv":
                raise PermissionError("in use")
            return real_unlink(self, *a, **k)

        monkeypatch.setattr(Path, "unlink", unlink)
        result = lifecycle.execute_cleanup(root, "current_session", confirm=True)
        assert result.deleted == [free]
        assert [s.id for s in result.skipped] == [locked] and "in use" in result.skipped[0].reason
        assert (root / "uploads" / f"{locked}.csv").is_file()

    @pytest.mark.skipif(
        sys.platform != "win32", reason="Windows refuses to delete a file that is open"
    )
    def test_a_file_really_open_in_another_program(self, root: Path) -> None:
        locked = make_upload(root, 1)
        with open(root / "uploads" / f"{locked}.csv", "rb"):
            result = lifecycle.execute_cleanup(root, "current_session", confirm=True)
            assert result.deleted == [] and "in use" in result.skipped[0].reason
            # the metadata (which records the owner) must outlive a CSV that could not be deleted
            assert (root / "uploads" / f"{locked}.json").is_file()
        # once the program lets go, the next attempt finishes the job
        again = lifecycle.execute_cleanup(root, "current_session", confirm=True)
        assert again.deleted == [locked] and not (root / "uploads" / f"{locked}.csv").exists()

    def test_a_partial_deletion_is_visible_and_finished_next_time(
        self, root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        upload = make_upload(root, 1)
        csv_size = (root / "uploads" / f"{upload}.csv").stat().st_size
        real_unlink = Path.unlink
        block = {"on": True}

        def unlink(self: Path, *a, **k):
            if block["on"] and self.name == f"{upload}.json":
                raise PermissionError("in use")
            return real_unlink(self, *a, **k)

        monkeypatch.setattr(Path, "unlink", unlink)
        first = lifecycle.execute_cleanup(root, "current_session", confirm=True)
        assert (
            first.deleted == [] and first.freed_bytes == csv_size
        )  # the CSV went, the metadata did not
        assert "json file in use" in first.skipped[0].reason
        item = lifecycle.get_storage_status(root).uploads[0]
        assert item.incomplete and item.state == "current_session"

        block["on"] = False
        second = lifecycle.execute_cleanup(root, "current_session", confirm=True)
        assert second.deleted == [upload] and existing(root) == set()


# ---------------------------------------------------------------------------------------- API surface


class TestApi:
    def test_every_storage_endpoint_needs_the_key(self, client: TestClient) -> None:
        assert client.get("/storage").status_code == 401
        assert (
            client.post("/storage/policy", json={"lifecycle_policy": "keep_files"}).status_code
            == 401
        )
        assert (
            client.post("/storage/cleanup", json={"scope": "orphaned", "confirm": True}).status_code
            == 401
        )

    def test_a_new_upload_records_the_session_that_made_it(
        self, client: TestClient, root: Path
    ) -> None:
        resp = client.post(
            "/uploads",
            files={"file": ("s.csv", io.BytesIO(CSV.encode()), "text/csv")},
            headers=auth(),
        )
        assert resp.status_code == 201 and resp.json()["session_id"] == MINE
        meta = json.loads(
            (root / "uploads" / f"{resp.json()['upload_id']}.json").read_text(encoding="utf-8")
        )
        assert meta["session_id"] == MINE
        state = client.get("/storage", headers=auth()).json()["uploads"][0]["state"]
        assert state == "current_session"

    def test_a_project_created_from_an_upload_protects_it(
        self, client: TestClient, root: Path
    ) -> None:
        upload = client.post(
            "/uploads",
            files={"file": ("s.csv", io.BytesIO(CSV.encode()), "text/csv")},
            headers=auth(),
        ).json()["upload_id"]
        made = client.post(
            "/projects",
            json={"project": "kept", "upload_id": upload, "question": "Which region?"},
            headers=auth(),
        )
        assert made.status_code == 201, made.text
        assert cleanup(client, scope="current_session", confirm=True).json()["deleted"] == []
        assert (root / "uploads" / f"{upload}.csv").is_file()


# ---------------------------------------------------------------------------- restart and the CLI


class TestRestartAndExit:
    def test_after_a_crash_the_old_sessions_uploads_show_up_as_orphaned(
        self, root: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        upload = make_upload(
            root, 1, session=MINE
        )  # the session that was running when the crash happened
        monkeypatch.setenv(lifecycle.SESSION_ENV, "session_afterrestart")
        item = lifecycle.get_storage_status(root).uploads[0]
        assert item.upload_id == upload and item.state == "orphaned"
        # the normal exit cleanup of the new session does not touch them
        lifecycle.execute_cleanup(root, "current_session", confirm=True)
        assert (root / "uploads" / f"{upload}.csv").is_file()
        # the user can review and delete them explicitly
        assert lifecycle.execute_cleanup(root, "orphaned", confirm=True).deleted == [upload]

    def test_report_lists_leftovers_from_an_earlier_session(self, root: Path) -> None:
        make_upload(root, 1, session=EARLIER)
        lines: list[str] = []
        assert lifecycle_cli.run("report", root, interactive=False, say=lines.append) == 0
        assert len(lines) == 1 and "1 temporary upload(s) from an earlier session" in lines[0]
        assert (root / "uploads" / "upload_000000000001.csv").is_file()

    def test_ask_on_exit_deletes_only_after_a_yes(self, root: Path) -> None:
        upload = make_upload(root, 1)
        make_upload(root, 2, session=EARLIER)
        said: list[str] = []
        lifecycle_cli.run("exit", root, interactive=True, ask=lambda _: "n", say=said.append)
        assert (root / "uploads" / f"{upload}.csv").is_file() and "kept" in said[-1]
        lifecycle_cli.run("exit", root, interactive=True, ask=lambda _: "", say=said.append)
        assert (root / "uploads" / f"{upload}.csv").is_file()
        lifecycle_cli.run("exit", root, interactive=True, ask=lambda _: "yes", say=said.append)
        assert not (root / "uploads" / f"{upload}.csv").exists()
        assert (
            root / "uploads" / "upload_000000000002.csv"
        ).is_file()  # the earlier session's file stays

    def test_ask_on_exit_never_deletes_without_a_terminal(self, root: Path) -> None:
        upload = make_upload(root, 1)

        def refuse(_: str) -> str:
            raise AssertionError("must not prompt")

        said: list[str] = []
        lifecycle_cli.run("exit", root, interactive=False, ask=refuse, say=said.append)
        assert (root / "uploads" / f"{upload}.csv").is_file() and "no terminal" in said[0]

    def test_auto_cleanup_removes_this_sessions_temporary_uploads_without_asking(
        self, root: Path
    ) -> None:
        lifecycle.set_policy(root, "auto_cleanup")
        mine = make_upload(root, 1)
        saved = make_upload(root, 2)
        make_project(root, "keep", saved)
        earlier = make_upload(root, 3, session=EARLIER)

        def refuse(_: str) -> str:
            raise AssertionError("auto cleanup must not prompt")

        lifecycle_cli.run("exit", root, interactive=False, ask=refuse, say=lambda _: None)
        assert not (root / "uploads" / f"{mine}.csv").exists()
        assert (root / "uploads" / f"{saved}.csv").is_file() and (
            root / "uploads" / f"{earlier}.csv"
        ).is_file()

    def test_keep_files_deletes_nothing(self, root: Path) -> None:
        lifecycle.set_policy(root, "keep_files")
        upload = make_upload(root, 1)
        lifecycle_cli.run("exit", root, interactive=True, ask=lambda _: "y", say=lambda _: None)
        assert (root / "uploads" / f"{upload}.csv").is_file()

    def test_the_cli_never_fails_the_launcher(
        self, root: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setenv("QUAERO_PROJECT_ROOT", str(root))
        monkeypatch.setattr(
            lifecycle, "get_storage_status", lambda _: (_ for _ in ()).throw(RuntimeError("boom"))
        )
        assert lifecycle_cli.main(["exit"]) == 0
        assert "boom" in capsys.readouterr().err

    def test_the_module_runs_from_a_shell(self, root: Path) -> None:
        import subprocess

        make_upload(root, 1, session=EARLIER)
        env = {**os.environ, "QUAERO_PROJECT_ROOT": str(root), lifecycle.SESSION_ENV: MINE}
        out = subprocess.run(
            [sys.executable, "-m", "api.lifecycle_cli", "report"],
            check=False,
            capture_output=True,
            text=True,
            env=env,
            stdin=subprocess.DEVNULL,
            timeout=60,
        )
        assert out.returncode == 0 and "earlier session" in out.stdout
