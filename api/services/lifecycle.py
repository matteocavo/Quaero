"""Local data lifecycle: what Quaero keeps on disk, what is temporary, and how it is removed.

Three kinds of local data are told apart:

* **Temporary uploads** (``uploads/upload_<12hex>.csv`` and its ``.json``): a CSV the user uploaded that no
  project uses. Each one records the ``session_id`` of the server process that created it.
* **Saved uploads**: an upload that a project's ``project_config.json`` refers to. Never removed here.
* **Projects and generated artifacts**: ``projects/<name>/``. A project created from an upload can have its
  generated artifacts cleared (its configuration stays). Projects that were not created from an upload
  (the bundled demos, hand written configurations) are never touched.

Safety rules (each is covered by tests/test_lifecycle.py):

* The caller never supplies a path or a file name. It supplies a *scope* and, for one upload, an id that must
  match ``upload_<12hex>``; everything else is looked up on disk by the server.
* Only regular files named exactly ``upload_<12hex>.csv|json`` directly inside ``uploads/`` are ever
  deleted. Symbolic links, junctions, directories and any other file name are skipped.
* Ownership is decided by the server: "this session" means the ``session_id`` stored in the upload metadata
  equals the one of the running server. An upload from an earlier session, or without a recorded session, is
  *orphaned*; it is never deleted automatically, only when the user asks and confirms.
* Whether an upload is still used is re-checked immediately before each deletion.
* A file that cannot be removed (for example open in another program on Windows) is reported as skipped and
  does not stop the rest; the result says exactly what was and was not deleted.
* Every permanent deletion needs ``confirm=True``.
"""

from __future__ import annotations

import json
import logging
import os
import re
import stat
import threading
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from api.schemas import (
    CleanupResponse,
    CleanupSkipped,
    StorageProjectItem,
    StorageStatusResponse,
    StorageTotals,
    StorageUploadItem,
)

LOGGER = logging.getLogger(__name__)

SESSION_ENV = "QUAERO_SESSION_ID"
POLICIES = ("ask_on_exit", "auto_cleanup", "keep_files")
DEFAULT_POLICY = "ask_on_exit"

_UPLOAD_ID_RE = re.compile(r"^upload_[0-9a-f]{12}$")
_UPLOAD_FILE_RE = re.compile(r"^(upload_[0-9a-f]{12})\.(csv|json)$")
_SESSION_ID_RE = re.compile(r"^session_[A-Za-z0-9_-]{4,64}$")
_UPLOAD_REF_RE = re.compile(r"upload_[0-9a-f]{12}")
_POLICY_FILE = "_lifecycle.json"
_ARTIFACT_DIRS = ("raw", "staging", "integrated", "marts", "metadata", "ui_inputs")
_MAX_CONFIG_BYTES = 1024 * 1024
_MAX_SCANNED_FILES = 100_000

#: Held while an upload is deleted or a project is created from one, so the two cannot interleave.
LIFECYCLE_LOCK = threading.RLock()

_PROCESS_SESSION_ID = f"session_{uuid.uuid4().hex[:12]}"


class LifecycleError(ValueError):
    """The request is not valid (unknown scope, bad id, missing confirmation...)."""


class LifecycleNotFound(LifecycleError):
    """The named upload or project does not exist."""


class LifecycleProtected(LifecycleError):
    """The target exists but must not be deleted (it is in use, saved or preexisting)."""


def current_session_id() -> str:
    """Session of this server: ``QUAERO_SESSION_ID`` when the launcher sets it, else one per process."""
    env = os.environ.get(SESSION_ENV, "").strip()
    return env if _SESSION_ID_RE.match(env) else _PROCESS_SESSION_ID


# ---------------------------------------------------------------------------------------------- policy


def _uploads_dir(project_root: Path) -> Path:
    root = project_root.resolve()
    link = root / "uploads"
    uploads = link.resolve()
    if not uploads.is_relative_to(root) or (link.exists() and _is_link(link)):
        raise LifecycleError("The uploads directory is a link or is outside the project root.")
    return uploads


def get_policy(project_root: Path) -> str:
    """The saved cleanup policy; any unreadable or unknown value means the safe default."""
    path = _uploads_dir(project_root) / _POLICY_FILE
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 4096:
            return DEFAULT_POLICY
        value = json.loads(path.read_text(encoding="utf-8")).get("lifecycle_policy")
    except (OSError, ValueError, AttributeError):
        return DEFAULT_POLICY
    return value if value in POLICIES else DEFAULT_POLICY


def set_policy(project_root: Path, policy: str) -> str:
    if policy not in POLICIES:
        raise LifecycleError(f"lifecycle_policy must be one of {', '.join(POLICIES)}.")
    uploads = _uploads_dir(project_root)
    uploads.mkdir(parents=True, exist_ok=True)
    path = uploads / _POLICY_FILE
    if path.is_symlink():
        raise LifecycleProtected(
            "The policy file is a symbolic link; refusing to write through it."
        )
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"lifecycle_policy": policy}), encoding="utf-8")
    tmp.replace(path)
    return policy


# ---------------------------------------------------------------------------------------------- inventory


@dataclass
class _UploadFiles:
    upload_id: str
    csv: Path | None = None
    meta: Path | None = None
    meta_data: dict[str, Any] = field(default_factory=dict)
    #: A file with this upload's name exists but is a link or not a regular file: the upload is left alone.
    blocked: bool = False

    @property
    def paths(self) -> list[Path]:
        return [p for p in (self.csv, self.meta) if p is not None]

    @property
    def incomplete(self) -> bool:
        return self.csv is None or self.meta is None


def _is_link(path: Path) -> bool:
    """A symbolic link or a Windows junction. ``Path.is_symlink`` is False for a junction, so look at the tag."""
    try:
        info = path.lstat()
    except OSError:
        return True  # cannot tell: treat as unsafe
    if stat.S_ISLNK(info.st_mode):
        return True
    return getattr(info, "st_reparse_tag", 0) == getattr(
        stat, "IO_REPARSE_TAG_MOUNT_POINT", 0xA0000003
    )


def _walk_files(root: Path):
    """Yield ``(directory, file_name)`` for regular files under *root*, never entering a link."""
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                listing = list(entries)
        except OSError:
            continue
        for entry in listing:
            path = Path(entry.path)
            if _is_link(path):
                continue
            if entry.is_dir(follow_symlinks=False):
                stack.append(path)
            elif entry.is_file(follow_symlinks=False):
                yield current, entry.name


def _is_plain_file(path: Path, parent: Path) -> bool:
    """A regular file directly inside *parent*: not a link, not a directory, not outside."""
    try:
        info = path.lstat()
    except OSError:
        return False
    if not stat.S_ISREG(info.st_mode):  # symlinks, junctions, directories, devices...
        return False
    try:
        return path.resolve().parent == parent.resolve()
    except OSError:
        return False


def _scan_uploads(project_root: Path) -> dict[str, _UploadFiles]:
    uploads = _uploads_dir(project_root)
    found: dict[str, _UploadFiles] = {}
    if not uploads.is_dir():
        return found
    with os.scandir(uploads) as entries:
        for entry in entries:
            match = _UPLOAD_FILE_RE.match(entry.name)
            if not match:
                continue  # any other name, including the policy file, is left alone
            path = Path(entry.path)
            item = found.setdefault(match.group(1), _UploadFiles(match.group(1)))
            if not _is_plain_file(path, uploads):
                LOGGER.warning("Leaving %s alone: not a regular file inside uploads/.", entry.name)
                item.blocked = True
                continue
            if match.group(2) == "csv":
                item.csv = path
            else:
                item.meta = path
    for item in found.values():
        if item.meta is not None:
            try:
                if item.meta.stat().st_size <= _MAX_CONFIG_BYTES:
                    data = json.loads(item.meta.read_text(encoding="utf-8"))
                    item.meta_data = data if isinstance(data, dict) else {}
            except (OSError, ValueError):
                item.meta_data = {}
    return found


def referenced_uploads(project_root: Path) -> dict[str, list[str]]:
    """``upload_id -> projects`` for every upload that a project configuration refers to."""
    refs: dict[str, list[str]] = {}
    projects = project_root.resolve() / "projects"
    if not projects.is_dir():
        return refs
    for config in sorted(projects.glob("*/project_config.json")):
        try:
            if config.is_symlink() or config.stat().st_size > _MAX_CONFIG_BYTES:
                continue
            text = config.read_text(encoding="utf-8")
        except OSError:
            continue
        # A plain text search also catches an id or a dataset path written by hand.
        for upload_id in set(_UPLOAD_REF_RE.findall(text)):
            refs.setdefault(upload_id, []).append(config.parent.name)
    return refs


def _upload_state(item: _UploadFiles, refs: dict[str, list[str]], session: str) -> str:
    if item.upload_id in refs:
        return "saved"
    return "current_session" if item.meta_data.get("session_id") == session else "orphaned"


def _size(paths: list[Path]) -> int:
    total = 0
    for path in paths:
        try:
            total += path.stat().st_size
        except OSError:
            pass
    return total


def _upload_item(item: _UploadFiles, refs: dict[str, list[str]], session: str) -> StorageUploadItem:
    name = item.meta_data.get("original_filename")
    created = item.meta_data.get("created_at")
    return StorageUploadItem(
        upload_id=item.upload_id,
        original_filename=name if isinstance(name, str) else None,
        size_bytes=_size(item.paths),
        created_at=created if isinstance(created, str) else None,
        state=_upload_state(item, refs, session),
        linked_projects=sorted(set(refs.get(item.upload_id, []))),
        incomplete=item.incomplete,
    )


def _walk_size(root: Path) -> tuple[int, int]:
    """Bytes and file count under *root*, never following links, bounded in the number of files."""
    total = count = 0
    for directory, name in _walk_files(root):
        try:
            total += (directory / name).stat().st_size
        except OSError:
            continue
        count += 1
        if count >= _MAX_SCANNED_FILES:
            break
    return total, count


def _project_config(project_dir: Path) -> dict[str, Any] | None:
    config = project_dir / "project_config.json"
    try:
        if config.is_symlink() or not config.is_file() or config.stat().st_size > _MAX_CONFIG_BYTES:
            return None
        data = json.loads(config.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _project_item(project_dir: Path) -> StorageProjectItem | None:
    if _is_link(project_dir) or not project_dir.is_dir():
        return None
    config = _project_config(project_dir)
    upload_id = config.get("upload_id") if config else None
    from_upload = isinstance(upload_id, str) and bool(_UPLOAD_ID_RE.match(upload_id))
    config_bytes = _size([project_dir / "project_config.json"])
    artifacts_bytes = artifact_files = 0
    for name in _ARTIFACT_DIRS:
        sub = project_dir / name
        if sub.is_dir() and not _is_link(sub):
            size, count = _walk_size(sub)
            artifacts_bytes += size
            artifact_files += count
    marts = project_dir / "marts"
    marts_count = 0
    if marts.is_dir() and not _is_link(marts):
        marts_count = sum(1 for p in marts.glob("*.parquet") if p.is_file() and not p.is_symlink())
    return StorageProjectItem(
        project=project_dir.name,
        kind="upload_project" if from_upload else "preexisting",
        upload_id=upload_id if from_upload else None,
        config_bytes=config_bytes,
        artifacts_bytes=artifacts_bytes,
        artifact_files=artifact_files,
        marts_count=marts_count,
        can_clear_artifacts=from_upload and artifact_files > 0,
    )


def get_storage_status(project_root: Path) -> StorageStatusResponse:
    session = current_session_id()
    refs = referenced_uploads(project_root)
    uploads = sorted(
        (_upload_item(i, refs, session) for i in _scan_uploads(project_root).values()),
        key=lambda u: (u.created_at or "", u.upload_id),
        reverse=True,
    )
    projects_root = project_root.resolve() / "projects"
    projects: list[StorageProjectItem] = []
    if projects_root.is_dir():
        for child in sorted(projects_root.iterdir(), key=lambda p: p.name):
            item = _project_item(child)
            if item is not None:
                projects.append(item)

    temporary = [u for u in uploads if u.state != "saved"]
    saved_bytes = sum(u.size_bytes for u in uploads if u.state == "saved")
    totals = StorageTotals(
        temporary_uploads_bytes=sum(u.size_bytes for u in temporary),
        saved_uploads_bytes=saved_bytes,
        project_configs_bytes=sum(p.config_bytes for p in projects),
        artifacts_bytes=sum(p.artifacts_bytes for p in projects),
        total_bytes=0,
    )
    totals.total_bytes = (
        totals.temporary_uploads_bytes
        + totals.saved_uploads_bytes
        + totals.project_configs_bytes
        + totals.artifacts_bytes
    )
    return StorageStatusResponse(
        session_id=session,
        lifecycle_policy=get_policy(project_root),
        totals=totals,
        current_session_uploads=sum(1 for u in temporary if u.state == "current_session"),
        orphaned_uploads=sum(1 for u in temporary if u.state == "orphaned"),
        uploads=uploads,
        projects=projects,
    )


# ---------------------------------------------------------------------------------------------- deletion


def _remove_file(path: Path, parent: Path) -> tuple[int, str | None]:
    """Delete one regular file; ``(bytes_freed, None)`` or ``(0, reason)``. Never raises for I/O errors."""
    if not _is_plain_file(path, parent):
        return 0, "not a regular file inside the expected folder (link or changed on disk)"
    try:
        size = path.stat().st_size
        path.unlink()
        return size, None
    except PermissionError:
        return 0, "in use by another program"
    except FileNotFoundError:
        return 0, None  # already gone
    except OSError as exc:
        return 0, f"could not be deleted ({exc.strerror or exc.__class__.__name__})"


def _delete_upload(
    project_root: Path, upload_id: str, allowed_states: set[str]
) -> tuple[int, list[CleanupSkipped]]:
    """Delete one upload if it is *still* temporary and in an allowed state. Re-checks under the lock."""
    skipped: list[CleanupSkipped] = []
    with LIFECYCLE_LOCK:
        item = _scan_uploads(project_root).get(upload_id)
        if item is None:
            return 0, [CleanupSkipped(id=upload_id, reason="no longer exists")]
        refs = referenced_uploads(project_root)
        state = _upload_state(item, refs, current_session_id())
        if state == "saved":
            used = ", ".join(sorted(set(refs[upload_id])))
            return 0, [
                CleanupSkipped(
                    id=upload_id, reason=f"used by project {used}; saved data is never deleted here"
                )
            ]
        if state not in allowed_states:
            return 0, [
                CleanupSkipped(
                    id=upload_id, reason=f"belongs to scope '{state}', not the one requested"
                )
            ]
        if item.blocked:
            return 0, [
                CleanupSkipped(
                    id=upload_id,
                    reason="has a file that is a link or not a regular file; left untouched",
                )
            ]
        uploads = _uploads_dir(project_root)
        freed = 0
        # CSV first, metadata last, and stop at the first failure: the metadata records which session owns
        # the upload, so it must outlive the CSV. A leftover is then still recognised and finished next time.
        for path in item.paths:
            size, reason = _remove_file(path, uploads)
            freed += size
            if reason:
                skipped.append(
                    CleanupSkipped(id=upload_id, reason=f"{path.suffix[1:]} file {reason}")
                )
                break
        return freed, skipped


def _clear_project_artifacts(
    project_root: Path, project: str
) -> tuple[int, int, list[CleanupSkipped]]:
    projects_root = project_root.resolve() / "projects"
    project_dir = projects_root / project
    # The name must match an existing direct child of projects/; it is never joined into a path otherwise.
    names = {p.name for p in projects_root.iterdir()} if projects_root.is_dir() else set()
    if project not in names:
        raise LifecycleNotFound(f"Project '{project}' does not exist.")
    item = _project_item(project_dir)
    if item is None:
        raise LifecycleNotFound(f"Project '{project}' does not exist.")
    if item.kind != "upload_project":
        raise LifecycleProtected(
            f"Project '{project}' was not created from an upload, so its files are never deleted here."
        )
    from api.services.runner import _active_run_for_project

    if _active_run_for_project(project_root, project) is not None:
        raise LifecycleProtected(
            f"Project '{project}' has a run in progress; wait for it to finish."
        )

    freed = deleted = 0
    skipped: list[CleanupSkipped] = []
    for name in _ARTIFACT_DIRS:
        sub = project_dir / name
        if _is_link(sub) or not sub.is_dir():
            continue
        for directory, filename in list(_walk_files(sub)):
            size, reason = _remove_file(directory / filename, directory)
            if reason:
                skipped.append(CleanupSkipped(id=f"{project}/{name}/{filename}", reason=reason))
            else:
                freed += size
                deleted += 1
    return freed, deleted, skipped


def execute_cleanup(
    project_root: Path, scope: str, confirm: bool, target_id: str | None = None
) -> CleanupResponse:
    """Permanently delete data of one *scope*. ``project_config.json`` and saved uploads are never deleted."""
    if confirm is not True:
        raise LifecycleError("Permanent deletion needs explicit confirmation (confirm=true).")

    deleted: list[str] = []
    skipped: list[CleanupSkipped] = []
    freed = 0

    if scope in ("current_session", "orphaned"):
        wanted = "current_session" if scope == "current_session" else "orphaned"
        session = current_session_id()
        refs = referenced_uploads(project_root)
        for item in sorted(_scan_uploads(project_root).values(), key=lambda i: i.upload_id):
            if _upload_state(item, refs, session) != wanted:
                continue
            size, problems = _delete_upload(project_root, item.upload_id, {wanted})
            freed += size
            skipped.extend(problems)
            if not problems:  # a partly deleted upload is reported as skipped, not as deleted
                deleted.append(item.upload_id)
    elif scope == "single_upload":
        if not target_id or not _UPLOAD_ID_RE.match(target_id):
            raise LifecycleError("target_id must be an upload id such as upload_0123456789ab.")
        found = _scan_uploads(project_root).get(target_id)
        if found is None:
            raise LifecycleNotFound(f"Upload '{target_id}' does not exist.")
        used_by = referenced_uploads(project_root).get(target_id)
        if used_by:
            raise LifecycleProtected(
                f"Upload '{target_id}' is used by project {', '.join(sorted(set(used_by)))}; saved data is never deleted here."
            )
        size, problems = _delete_upload(project_root, target_id, {"current_session", "orphaned"})
        freed += size
        skipped.extend(problems)
        if not problems:
            deleted.append(target_id)
    elif scope == "project_artifacts":
        if not target_id:
            raise LifecycleError("target_id must name a project.")
        size, count, problems = _clear_project_artifacts(project_root, target_id)
        freed += size
        skipped.extend(problems)
        deleted.append(f"{target_id}: {count} generated file{'s' if count != 1 else ''}")
    else:
        raise LifecycleError(
            "scope must be current_session, orphaned, single_upload or project_artifacts."
        )

    parts = [f"Deleted {len(deleted)} item{'s' if len(deleted) != 1 else ''}, freed {freed} bytes."]
    if skipped:
        parts.append(f"{len(skipped)} could not be deleted and were left in place.")
    LOGGER.info("Lifecycle cleanup %s: %s", scope, " ".join(parts))
    return CleanupResponse(
        scope=scope, deleted=deleted, freed_bytes=freed, skipped=skipped, message=" ".join(parts)
    )
