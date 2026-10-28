"""Run state files under concurrent access (the intermittent HTTP 500 on Windows).

While a run executes, a poller reads ``runs/<run_id>.json`` and the runner replaces it atomically. On Windows a
reader can be refused for a few milliseconds (``PermissionError``) at the moment of the replace. That is a
temporary sharing conflict, not an error in the run: it is retried briefly. A refusal that lasts is a real error
and is raised, never retried forever.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path

import pytest

from api.services import runner

RUN_ID = "6f1c2d3e-4a5b-4c6d-8e7f-0a1b2c3d4e5f"


@pytest.fixture()
def root(tmp_path: Path) -> Path:
    return tmp_path


def _state(n: int = 0, status: str = "RUNNING") -> dict:
    return {
        "run_id": RUN_ID,
        "project": "p",
        "status": status,
        "error": None,
        "result_keys": [],
        "n": n,
    }


# ------------------------------------------------------------------------------------ the real race
def test_a_poller_and_the_runner_can_share_a_run_file_without_errors(root: Path) -> None:
    """Hammer one run file with a writer and several readers; nobody may see an error or a torn file."""
    runner._save_run(root, _state())
    stop = threading.Event()
    failures: list[BaseException] = []
    reads = {"count": 0}

    def writer() -> None:
        try:
            for n in range(1, 400):
                runner._save_run(root, _state(n))
        except BaseException as exc:
            failures.append(exc)
        finally:
            stop.set()

    def reader() -> None:
        try:
            while not stop.is_set():
                state = runner._load_run(root, RUN_ID)
                time.sleep(0.0005)  # far more often than a real poller (about twice a second)
                assert (
                    state is not None and state["run_id"] == RUN_ID and state["status"] == "RUNNING"
                )
                reads["count"] += 1
        except BaseException as exc:
            failures.append(exc)

    threads = [threading.Thread(target=writer)] + [
        threading.Thread(target=reader) for _ in range(4)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert failures == [], failures[:3]
    assert reads["count"] > 0
    assert runner._load_run(root, RUN_ID)["n"] == 399  # the last write won


# --------------------------------------------------------------------- deterministic, any platform
def _fail_open_n_times(
    monkeypatch: pytest.MonkeyPatch, target: Path, times: int, error: Exception
) -> dict:
    real_open = Path.open
    calls = {"n": 0}

    def flaky(self: Path, *args, **kwargs):
        if self == target:
            calls["n"] += 1
            if calls["n"] <= times:
                raise error
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", flaky)
    return calls


def test_a_short_sharing_conflict_on_read_is_retried(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner._save_run(root, _state(7))
    calls = _fail_open_n_times(
        monkeypatch, runner._run_path(root, RUN_ID), 2, PermissionError(13, "in use")
    )
    assert runner._load_run(root, RUN_ID)["n"] == 7
    assert calls["n"] == 3  # two refusals, then the read


def test_a_permanent_refusal_is_raised_after_a_few_attempts_not_retried_forever(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner._save_run(root, _state())
    calls = _fail_open_n_times(
        monkeypatch, runner._run_path(root, RUN_ID), 10_000, PermissionError(13, "denied")
    )
    started = time.monotonic()
    with pytest.raises(PermissionError):
        runner._load_run(root, RUN_ID)
    assert calls["n"] == runner._IO_ATTEMPTS
    assert time.monotonic() - started < 2.0  # bounded: well under what a poller would wait for


def test_errors_that_are_not_sharing_conflicts_are_not_retried(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner._save_run(root, _state())
    path = runner._run_path(root, RUN_ID)
    path.write_text("{not json", encoding="utf-8")
    calls = {"n": 0}
    real_open = Path.open

    def counting(self: Path, *args, **kwargs):
        if self == path:
            calls["n"] += 1
        return real_open(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counting)
    with pytest.raises(json.JSONDecodeError):
        runner._load_run(root, RUN_ID)
    assert calls["n"] == 1


def test_a_missing_run_is_none_not_an_error(root: Path) -> None:
    assert runner._load_run(root, "6f1c2d3e-0000-4000-8000-000000000000") is None


@pytest.mark.parametrize(
    "run_id", ["does-not-exist", "../outside", "..\\outside", "a/b", "", "r1.json"]
)
def test_an_id_that_is_not_a_run_id_is_not_found_and_never_becomes_a_path(
    root: Path, run_id: str
) -> None:
    assert runner._load_run(root, run_id) is None
    with pytest.raises(ValueError):
        runner._run_path(root, run_id)


def test_a_short_conflict_while_replacing_is_retried_and_leaves_no_stray_file(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_replace = Path.replace
    calls = {"n": 0}

    def flaky(self: Path, target):
        calls["n"] += 1
        if calls["n"] <= 2:
            raise PermissionError(13, "in use")
        return real_replace(self, target)

    monkeypatch.setattr(Path, "replace", flaky)
    runner._save_run(root, _state(3))
    assert runner._load_run(root, RUN_ID)["n"] == 3
    assert [p.name for p in (root / "runs").iterdir()] == [f"{RUN_ID}.json"]


def test_a_permanent_failure_to_replace_is_raised_and_cleans_up_its_temporary_file(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runner._save_run(root, _state(1))

    def refuse(self: Path, target):
        raise PermissionError(13, "denied")

    monkeypatch.setattr(Path, "replace", refuse)
    with pytest.raises(PermissionError):
        runner._save_run(root, _state(2))
    monkeypatch.undo()
    assert [p.name for p in (root / "runs").iterdir()] == [f"{RUN_ID}.json"]
    assert runner._load_run(root, RUN_ID)["n"] == 1  # the previous state is intact


def test_the_active_run_check_is_not_fooled_by_a_momentary_conflict(
    root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If a read conflict were swallowed, an active run would look absent and a second one could start."""
    runner._save_run(root, _state())
    _fail_open_n_times(
        monkeypatch, runner._run_path(root, RUN_ID), 2, PermissionError(13, "in use")
    )
    active = runner._active_run_for_project(root, "p")
    assert active is not None and active["status"] == "RUNNING"


def test_a_writer_waits_for_another_program_that_has_the_file_open(root: Path) -> None:
    """What the lock cannot see (an antivirus scan, a sync client): on Windows a replace is refused until it lets go."""
    runner._save_run(root, _state(1))
    path = runner._run_path(root, RUN_ID)
    held = path.open("rb")
    timer = threading.Timer(0.15, held.close)
    timer.start()
    try:
        runner._save_run(root, _state(5))  # retried until the other program releases the file
    finally:
        timer.join()
        held.close()
    assert runner._load_run(root, RUN_ID)["n"] == 5
    assert [p.name for p in (root / "runs").iterdir()] == [f"{RUN_ID}.json"]


@pytest.mark.parametrize("operation", ["read", "write"])
def test_readers_and_the_writer_of_this_process_take_turns(root: Path, operation: str) -> None:
    """Deterministic check of the in-process lock: while it is held, a read or a replace must wait."""
    runner._save_run(root, _state(1))
    finished = threading.Event()

    def attempt() -> None:
        if operation == "read":
            runner._load_run(root, RUN_ID)
        else:
            runner._save_run(root, _state(2))
        finished.set()

    with runner._RUN_FILE_LOCK:
        thread = threading.Thread(target=attempt)
        thread.start()
        assert not finished.wait(0.2), f"a {operation} went ahead while the run file lock was held"
    thread.join(timeout=10)
    assert finished.is_set()
