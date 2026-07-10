"""Create and rotate transactionally consistent SQLite backups."""

from __future__ import annotations

import argparse
import os
import sqlite3
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path


def create_backup(
    db_path: Path,
    backup_dir: Path,
    *,
    keep: int,
    now: datetime | None = None,
) -> Path:
    if keep < 1:
        raise ValueError("keep must be positive")
    if not db_path.is_file():
        raise FileNotFoundError(db_path)
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = (now or datetime.now(UTC)).astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    destination = backup_dir / f"neo_swarm_scalper-{timestamp}.sqlite"
    temporary = destination.with_suffix(".sqlite.tmp")
    if temporary.exists():
        temporary.unlink()
    try:
        with (
            closing(sqlite3.connect(db_path, timeout=30)) as source,
            closing(sqlite3.connect(temporary)) as target,
        ):
            source.backup(target)
        with closing(sqlite3.connect(temporary, timeout=30)) as check:
            result = check.execute("PRAGMA quick_check").fetchone()
        if result is None or result[0] != "ok":
            raise RuntimeError(f"backup quick_check failed: {result}")
        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
    finally:
        if temporary.exists():
            temporary.unlink()
    backups = sorted(backup_dir.glob("neo_swarm_scalper-*.sqlite"), reverse=True)
    for expired in backups[keep:]:
        expired.unlink()
    return destination


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--backup-dir", required=True, type=Path)
    parser.add_argument("--keep", type=int, default=28)
    args = parser.parse_args(argv)
    backup = create_backup(args.db, args.backup_dir, keep=args.keep)
    print(f"backup={backup} size={backup.stat().st_size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
