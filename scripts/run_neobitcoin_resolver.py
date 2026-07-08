"""Run Dual-Bot Neobitcoin Resolver in local paper/live-data mode."""

from __future__ import annotations

import argparse
import sys
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.neobitcoin_resolver.config import load_resolver_config  # noqa: E402
from neo_trader.neobitcoin_resolver.runner import (  # noqa: E402
    run_live_resolver,
    run_mock_smoke,
)
from neo_trader.neobitcoin_resolver.secrets import ensure_token_in_env  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Dual-Bot Neobitcoin Resolver")
    parser.add_argument(
        "--mock-data", action="store_true", help="Run deterministic local smoke data."
    )
    parser.add_argument("--db", type=Path, default=None)
    parser.add_argument("--max-cycles", type=int, default=None)
    parser.add_argument("--poll-interval-seconds", type=float, default=None)
    parser.add_argument(
        "--ensure-token-env",
        action="store_true",
        help="Copy a desktop txt token into local .env without printing it.",
    )
    args = parser.parse_args(argv)

    if args.ensure_token_env:
        ensure_token_in_env(env_path=ROOT / ".env")
    config = load_resolver_config()
    if args.poll_interval_seconds is not None:
        config = replace(
            config,
            poll_interval_seconds=args.poll_interval_seconds,
        )
    if args.mock_data:
        result = run_mock_smoke(config=config, db_path=args.db)
    else:
        result = run_live_resolver(config=config, db_path=args.db, max_cycles=args.max_cycles)
    print(f"cycles={len(result.cycles)}")
    print(f"db={result.db_path}")
    print(f"dashboard={result.dashboard_path}")
    print(f"heartbeat={result.heartbeat_path}")
    print("live_trading=false")
    print("token_masked=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
