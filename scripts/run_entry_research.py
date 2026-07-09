"""Run offline entry-strategy forward labeling on the neo swarm SQLite DB."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from neo_swarm_scalper.config import DEFAULT_CONFIG_PATH, load_config
from neo_swarm_scalper.entry_research import run_entry_research
from neo_swarm_scalper.storage import SQLiteJournal


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run offline entry research labels")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--db", default=None)
    parser.add_argument("--limit-per-instrument", type=int, default=500)
    parser.add_argument("--since", default=None)
    args = parser.parse_args(argv)
    config = load_config(args.config)
    db_path = Path(args.db) if args.db else config.storage.sqlite_path
    since = None if args.since is None else datetime.fromisoformat(args.since)
    summary = run_entry_research(
        SQLiteJournal(db_path, wal_mode=config.storage.wal_mode),
        limit_per_instrument=args.limit_per_instrument,
        since_utc=since,
    )
    print(f"run_id={summary.run_id}")
    print(f"candidates_scored={summary.candidates_scored}")
    print(f"labels_written={summary.labels_written}")
    print(f"best_entry_type={summary.best_entry_type}")
    print(f"best_stop_ticks={summary.best_stop_ticks}")
    print(f"mfe3_rate={summary.mfe3_rate}")
    print(f"mfe5_rate={summary.mfe5_rate}")
    print(f"mfe10_rate={summary.mfe10_rate}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
