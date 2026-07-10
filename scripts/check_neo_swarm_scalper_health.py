"""Exit non-zero when the paper bot heartbeat is missing or stale."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class HealthCheckError(RuntimeError):
    """The runtime cannot be considered healthy."""


def check_health(
    db_path: Path,
    *,
    max_age_seconds: float,
    now: datetime | None = None,
) -> dict[str, Any]:
    if not db_path.is_file():
        raise HealthCheckError(f"database_missing:{db_path}")
    try:
        with sqlite3.connect(db_path, timeout=5) as conn:
            row = conn.execute(
                """
                SELECT heartbeat, status
                FROM system_health
                WHERE component = 'tail_catcher'
                ORDER BY id DESC
                LIMIT 1
                """
            ).fetchone()
    except sqlite3.Error as exc:
        raise HealthCheckError(f"database_error:{exc}") from exc
    if row is None or row[0] is None:
        raise HealthCheckError("heartbeat_missing")
    heartbeat = _timestamp(str(row[0]))
    checked_at = now or datetime.now(UTC)
    if checked_at.tzinfo is None:
        checked_at = checked_at.replace(tzinfo=UTC)
    age_seconds = (checked_at.astimezone(UTC) - heartbeat).total_seconds()
    if age_seconds < -30:
        raise HealthCheckError(f"heartbeat_in_future:{age_seconds:.1f}")
    if age_seconds > max_age_seconds:
        raise HealthCheckError(f"heartbeat_stale:{age_seconds:.1f}")
    return {
        "status": "ok",
        "runtime_status": str(row[1]),
        "heartbeat": heartbeat.isoformat(),
        "age_seconds": round(age_seconds, 3),
        "max_age_seconds": max_age_seconds,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--max-age-seconds", type=float, default=90)
    args = parser.parse_args(argv)
    try:
        payload = check_health(args.db, max_age_seconds=args.max_age_seconds)
    except HealthCheckError as exc:
        print(json.dumps({"status": "error", "reason": str(exc)}))
        return 1
    print(json.dumps(payload, sort_keys=True))
    return 0


def _timestamp(raw: str) -> datetime:
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HealthCheckError(f"heartbeat_invalid:{raw}") from exc
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
