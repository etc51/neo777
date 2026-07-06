"""Runtime metadata helpers."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

UNKNOWN_COMMIT_HASH = "unknown"


def get_runtime_commit_hash() -> str:
    """Return the commit hash that should be stamped on runtime reports."""

    env_value = os.getenv("NEO_TRADER_COMMIT_HASH")
    if env_value is not None and env_value.strip():
        return env_value.strip()

    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=_project_root(),
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return UNKNOWN_COMMIT_HASH

    commit_hash = result.stdout.strip()
    if result.returncode != 0 or not commit_hash:
        return UNKNOWN_COMMIT_HASH
    return commit_hash


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


__all__ = [
    "UNKNOWN_COMMIT_HASH",
    "get_runtime_commit_hash",
]
