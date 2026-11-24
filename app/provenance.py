"""Cross-entrypoint write lease and artifact provenance (v0.9 PR 1).

Why: the reviewed artifacts (``metadata/dq_score_*`` and ``metadata/anomaly_report_v2_*``) are overwritten in
place by every Quaero entrypoint (API runner, CLI, UI, direct calls). To bind a review to the exact bytes it was
made on, each pipeline write is serialised by an exclusive, non-blocking OS file lock per project (the *lease*)
and the artifacts are fingerprinted (SHA-256) and registered while the lease is still held.

Rules
-----
* Lock order is fixed: lease first, then SQLite. Never take the lease while holding a SQLite write transaction.
* The lease is never waited on: contention fails fast with :class:`ProjectBusyError`.
* The kernel releases the lock if the process dies, so a crash cannot leave a project stuck.
* Registration is best effort and never raises: a completed analytic run stays completed. Without a row the run
  is simply *unverifiable* (it can never look verified by accident).
* Out of scope (documented, tested): a non-Quaero program that edits an artifact *before* it is fingerprinted.
  An edit *after* the fingerprint is detected by the hash.
"""

from __future__ import annotations

import hashlib
import logging
import sys
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from db import store

LOGGER = logging.getLogger(__name__)

LOCK_FILENAME = ".quaero_write.lock"
ARTIFACT_PATTERNS = ("dq_score_*.json", "anomaly_report_v2_*.json")

_READ_ATTEMPTS = 6
_READ_DELAY_SECONDS = 0.05

# Binding outcomes (the API layer in a later PR maps these to HTTP errors).
VERIFIED = "verified"
BINDING_UNVERIFIABLE = "binding_unverifiable"
UNMANAGED_WRITE = "unmanaged_write"
ARTIFACT_CHANGED = "artifact_changed"
ARTIFACT_UNAVAILABLE = "artifact_unavailable"
RUN_ACTIVE = "run_active"
RUN_MISMATCH = "run_mismatch"


class ProjectBusyError(RuntimeError):
    """Another Quaero entrypoint is writing this project right now."""


# ------------------------------------------------------------------------------------------- lease
if sys.platform == "win32":
    import msvcrt

    def _try_lock(handle: Any) -> bool:
        try:
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return True
        except OSError:
            return False

    def _unlock(handle: Any) -> None:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _try_lock(handle: Any) -> bool:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            return False

    def _unlock(handle: Any) -> None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


_held = threading.local()  # lock path -> [handle, depth] for the current thread (re-entrancy)


def _held_map() -> dict[str, list[Any]]:
    if not hasattr(_held, "locks"):
        _held.locks = {}
    return _held.locks


@contextmanager
def project_write_lease(project_dir: str | Path) -> Iterator[None]:
    """Hold the exclusive write lease of a project for the duration of the block.

    Re-entrant for the same thread (the API runner and the pipeline both take it). Raises
    :class:`ProjectBusyError` immediately when another thread or process holds it.
    """
    project_dir = Path(project_dir)
    project_dir.mkdir(parents=True, exist_ok=True)
    lock_path = project_dir / LOCK_FILENAME
    key = str(lock_path.resolve())
    held = _held_map()
    if key in held:
        held[key][1] += 1
        try:
            yield
        finally:
            held[key][1] -= 1
        return

    handle = open(lock_path, "a+b")  # noqa: SIM115 - closed in finally
    try:
        if handle.tell() == 0 and lock_path.stat().st_size == 0:
            handle.write(b"\0")
            handle.flush()
        if not _try_lock(handle):
            raise ProjectBusyError(
                f"project busy: another Quaero run is writing '{project_dir.name}'. "
                "Wait for it to finish and try again."
            )
        held[key] = [handle, 1]
        try:
            yield
        finally:
            del held[key]
            try:
                _unlock(handle)
            except OSError:  # the kernel frees it on close anyway
                LOGGER.debug("lease unlock failed for %s", lock_path, exc_info=True)
    finally:
        handle.close()


def lease_is_free(project_dir: str | Path) -> bool:
    """True if the lease could be taken right now (probe: takes and releases it)."""
    try:
        with project_write_lease(project_dir):
            return True
    except ProjectBusyError:
        return False


# ------------------------------------------------------------------------------------- fingerprints
def read_artifact_bytes_with_retry(
    path: str | Path,
    attempts: int = _READ_ATTEMPTS,
    delay: float = _READ_DELAY_SECONDS,
) -> bytes:
    """Read a file once; retry briefly on Windows sharing violations, then raise the real error."""
    last: OSError | None = None
    for attempt in range(attempts):
        try:
            return Path(path).read_bytes()
        except PermissionError as exc:
            last = exc
            if attempt < attempts - 1:
                time.sleep(delay)
    assert last is not None
    raise last


def _fingerprint(project_dir: Path, path: Path) -> tuple[dict[str, Any], bytes]:
    data = read_artifact_bytes_with_retry(path)
    row = {
        "artifact": path.name,
        "path": path.relative_to(project_dir).as_posix(),
        "sha256": hashlib.sha256(data).hexdigest(),
        "size": len(data),
    }
    return row, data


def collect_artifact_paths(project_dir: Path) -> list[Path]:
    metadata = project_dir / "metadata"
    found: list[Path] = []
    for pattern in ARTIFACT_PATTERNS:
        found.extend(sorted(metadata.glob(pattern)))
    return found


def record_artifacts(project_dir: str | Path, project: str, run_id: str | None) -> bool:
    """Fingerprint and register the reviewed artifacts. Call INSIDE the lease. Never raises.

    ``run_id`` is None for writes not managed by an API run (CLI / direct): they supersede earlier API rows.
    All rows are inserted in one transaction, so a partial registration cannot exist. Returns True on success.
    """
    try:
        project_dir = Path(project_dir)
        rows = [_fingerprint(project_dir, p)[0] for p in collect_artifact_paths(project_dir)]
        store.init_db()
        store.register_run_artifacts(project, run_id, "api" if run_id else "cli", rows)
        return True
    except Exception:
        LOGGER.warning(
            "artifact provenance not recorded for project %s (run stays completed, unverifiable)",
            project,
            exc_info=True,
        )
        return False


# --------------------------------------------------------------------------------------- verification
@dataclass(frozen=True)
class BindingResult:
    outcome: str
    data: bytes | None = None  # the exact bytes that were hashed (parse these, never re-read)
    row: dict[str, Any] | None = None


def check_binding(
    project_dir: str | Path, project: str, artifact: str, run_id: str
) -> BindingResult:
    """Verify that ``artifact`` still holds the bytes registered for ``run_id``.

    Takes the lease (non-blocking) so no writer can interleave; read-then-hash uses a single read and returns
    those same bytes. Outcomes: verified, run_active, binding_unverifiable, unmanaged_write, run_mismatch,
    artifact_unavailable, artifact_changed.
    """
    project_dir = Path(project_dir)
    try:
        with project_write_lease(project_dir):
            try:
                row = store.latest_run_artifact(project, artifact)
            except Exception:  # fail closed: no readable provenance means unverifiable
                LOGGER.warning(
                    "provenance lookup failed for %s/%s", project, artifact, exc_info=True
                )
                return BindingResult(BINDING_UNVERIFIABLE)
            if row is None:
                return BindingResult(BINDING_UNVERIFIABLE)
            if row["run_id"] is None:
                return BindingResult(UNMANAGED_WRITE, row=row)
            if row["run_id"] != run_id:
                return BindingResult(RUN_MISMATCH, row=row)
            try:
                data = read_artifact_bytes_with_retry(project_dir / row["path"])
            except OSError:
                return BindingResult(ARTIFACT_UNAVAILABLE, row=row)
            if hashlib.sha256(data).hexdigest() != row["sha256"]:
                return BindingResult(ARTIFACT_CHANGED, row=row)
            return BindingResult(VERIFIED, data=data, row=row)
    except ProjectBusyError:
        return BindingResult(RUN_ACTIVE)
