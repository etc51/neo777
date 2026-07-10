"""CLI for the read-only Neobitcoin research service."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.neobitcoin_research.config import ResearchConfig  # noqa: E402
from neo_trader.neobitcoin_research.reporting import (  # noqa: E402
    generate_daily_research_report,
)
from neo_trader.neobitcoin_research.runtime import run_research_service  # noqa: E402
from neo_trader.neobitcoin_research.storage import ResearchStorage  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = ResearchConfig.from_env()
    if getattr(args, "token_file", None):
        config = replace(config, token_path=args.token_file)
    config.validate()

    if args.command == "run":
        runtime = asyncio.run(
            run_research_service(
                config,
                duration_seconds=args.duration_seconds,
                max_events=args.max_events,
            )
        )
        print(f"events_seen={runtime.events_seen}")
        print(f"valid_orderbooks={runtime.valid_books}")
        print(f"inconsistent_orderbooks={runtime.inconsistent_books}")
        print(f"max_observed_depth={runtime.depth_observed}")
        print(f"reconnects={runtime.reconnects}")
        return 0
    if args.command == "compact":
        with ResearchStorage(
            config.data_root,
            state_db_path=config.data_root / "state.sqlite",
            fsync=config.wal_fsync,
        ) as storage:
            results = storage.compact_all()
            catalog = storage.refresh_duckdb_catalog()
        print(f"compacted_hours={len(results)}")
        print(f"duckdb_catalog={catalog.catalog_path}")
        return 0
    if args.command == "report":
        report_date = date.fromisoformat(args.date) if args.date else datetime.now(UTC).date()
        with ResearchStorage(
            config.data_root,
            state_db_path=config.data_root / "state.sqlite",
            fsync=config.wal_fsync,
        ) as storage:
            paths = generate_daily_research_report(
                storage,
                report_date,
                reports_dir=config.reports_root,
            )
        print(f"report_json={paths.json_path}")
        print(f"report_markdown={paths.markdown_path}")
        return 0
    parser.error(f"unsupported command: {args.command}")
    return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="run the official T-Invest stream")
    run.add_argument("--duration-seconds", type=float)
    run.add_argument("--max-events", type=int)
    run.add_argument("--token-file", type=Path)
    subparsers.add_parser("compact", help="compact JSONL WAL and refresh DuckDB")
    report = subparsers.add_parser("report", help="write daily Markdown and JSON")
    report.add_argument("--date", help="UTC date in YYYY-MM-DD")
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
