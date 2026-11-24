from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture(autouse=True)
def _isolated_quaero_db(tmp_path, monkeypatch):
    """Never let tests write provenance rows into the real repo-root quaero.db."""
    if "QUAERO_DB_PATH" not in __import__("os").environ:
        monkeypatch.setenv("QUAERO_DB_PATH", str(tmp_path / "isolated_quaero.db"))
