"""Command line side of the local data lifecycle, used by ``scripts/start-local.ps1``.

    python -m api.lifecycle_cli report   # at start: list temporary uploads left by an earlier session
    python -m api.lifecycle_cli exit     # at exit: apply the cleanup policy to this session's uploads

It works on the files directly (no server, no API key), so it still runs when the server has been stopped
or has crashed. It uses the same rules as the API (``api.services.lifecycle``): only temporary uploads of
*this* session are ever removed on exit, and with ``ask_on_exit`` nothing is deleted unless a person
answers yes at an interactive prompt.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from pathlib import Path

from api.dependencies import get_project_root
from api.services import lifecycle


def _mb(size: int) -> str:
    return f"{size / (1024 * 1024):.2f} MB" if size >= 1024 * 1024 else f"{size / 1024:.1f} KB"


def run(
    command: str,
    project_root: Path,
    interactive: bool,
    ask: Callable[[str], str] = input,
    say: Callable[[str], None] = print,
) -> int:
    status = lifecycle.get_storage_status(project_root)
    temporary = [u for u in status.uploads if u.state != "saved"]

    if command == "report":
        left = [u for u in temporary if u.state == "orphaned"]
        if left:
            say(
                f"Quaero: {len(left)} temporary upload(s) from an earlier session are still on disk "
                f"({_mb(sum(u.size_bytes for u in left))}). Open 'Local Storage' in Quaero to review or delete them."
            )
        return 0

    # command == "exit"
    mine = [u for u in temporary if u.state == "current_session"]
    policy = status.lifecycle_policy
    if not mine:
        return 0
    size = sum(u.size_bytes for u in mine)
    if policy == "keep_files":
        say(
            f"Quaero: keeping {len(mine)} temporary upload(s) ({_mb(size)}) as configured (keep_files)."
        )
        return 0
    if policy == "ask_on_exit":
        if not interactive:
            say(
                f"Quaero: {len(mine)} temporary upload(s) ({_mb(size)}) were left in place because there is no "
                "terminal to ask. They will be listed at the next start."
            )
            return 0
        say(f"Quaero: this session left {len(mine)} temporary upload(s) ({_mb(size)}):")
        for u in mine:
            say(
                f"  - {u.upload_id}  {u.original_filename or '(unknown name)'}  {_mb(u.size_bytes)}"
            )
        say("Saved projects and the files they use are not affected.")
        if ask("Delete these temporary uploads permanently? [y/N] ").strip().lower() not in (
            "y",
            "yes",
        ):
            say("Quaero: kept. They will be listed at the next start.")
            return 0
    result = lifecycle.execute_cleanup(project_root, "current_session", confirm=True)
    say(f"Quaero: {result.message}")
    for item in result.skipped:
        say(f"  left in place: {item.id} ({item.reason})")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m api.lifecycle_cli", description=__doc__.split("\n\n")[0]
    )
    parser.add_argument("command", choices=("report", "exit"))
    args = parser.parse_args(argv)
    try:
        return run(
            args.command, get_project_root(), interactive=sys.stdin.isatty() and sys.stdout.isatty()
        )
    except Exception as exc:  # the launcher must never fail because of housekeeping
        print(
            f"Quaero: could not finish the cleanup ({exc}). Open 'Local Storage' to see what is left.",
            file=sys.stderr,
        )
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
