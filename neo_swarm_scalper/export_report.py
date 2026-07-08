"""Export a markdown report from an existing `neo_swarm_scalper` SQLite DB."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

from neo_swarm_scalper.config import DEFAULT_CONFIG_PATH, load_config
from neo_swarm_scalper.reports import write_report
from neo_swarm_scalper.storage import SQLiteJournal


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export neo_swarm_scalper report")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--db", default="data/neo_swarm_scalper.sqlite")
    parser.add_argument("--reports-dir", default=None)
    args = parser.parse_args(argv)
    config = load_config(args.config)
    storage = SQLiteJournal(Path(args.db))
    path = write_report(
        storage=storage,
        config=config,
        runtime_start=datetime.now(UTC),
        reports_dir=args.reports_dir,
        final=True,
    )
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
