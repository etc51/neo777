"""Smoke test entrypoint for mock-data paper runs."""

from __future__ import annotations

import argparse
from pathlib import Path

from neo_swarm_scalper.config import DEFAULT_CONFIG_PATH, load_config
from neo_swarm_scalper.run import MockNeoMarketDataProvider, run_swarm


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run mock-data smoke test")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--cycles", type=int, default=16)
    parser.add_argument("--db", default="data/neo_swarm_scalper_smoke.sqlite")
    parser.add_argument("--reports-dir", default="reports/neo_swarm_scalper")
    args = parser.parse_args(argv)
    config = load_config(args.config)
    result = run_swarm(
        config,
        max_cycles=args.cycles,
        poll_interval_sec=0,
        provider=MockNeoMarketDataProvider(),
        db_path=Path(args.db),
        reports_dir=Path(args.reports_dir),
        sleep=lambda _: None,
    )
    print(f"smoke_cycles={result.cycles}")
    print(f"smoke_db={result.db_path}")
    print("real_orders_disabled=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
