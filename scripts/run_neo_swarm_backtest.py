"""Run Neo Universal Swarm offline order-book backtest."""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.neo_universal_swarm.backtest import (  # noqa: E402
    OrderbookBacktestConfig,
    run_orderbook_backtest,
)
from neo_trader.neo_universal_swarm.types import SwarmInstrument  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Replay historical order books through Neo Universal Swarm.",
    )
    parser.add_argument(
        "--source",
        action="append",
        type=Path,
        default=None,
        help="Order-book parquet/jsonl file or directory. Can be passed multiple times.",
    )
    parser.add_argument("--accounts", type=Path, default=Path("configs/accounts.yaml"))
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=Path("data/reports/neo_universal_swarm_backtest"),
    )
    parser.add_argument(
        "--instrument",
        action="append",
        choices=[item.value for item in SwarmInstrument],
    )
    parser.add_argument("--min-required-ev-ticks", default="1")
    parser.add_argument("--slippage-stress-ticks", default="0")
    parser.add_argument("--max-pair-age-seconds", type=float, default=300.0)
    parser.add_argument("--max-snapshots", type=int, default=None)
    args = parser.parse_args(argv)

    instruments = (
        tuple(SwarmInstrument(value) for value in args.instrument)
        if args.instrument
        else tuple(SwarmInstrument)
    )
    result = run_orderbook_backtest(
        OrderbookBacktestConfig(
            accounts_path=args.accounts,
            sources=tuple(args.source or (Path("data/raw"),)),
            reports_dir=args.reports_dir,
            instruments=instruments,
            min_required_ev_ticks=Decimal(args.min_required_ev_ticks),
            slippage_stress_ticks=Decimal(args.slippage_stress_ticks),
            max_pair_age_seconds=args.max_pair_age_seconds,
            max_snapshots=args.max_snapshots,
        )
    )
    metrics = result.metrics
    print(f"commit_hash={result.commit_hash}")
    print(f"snapshots_read={result.snapshots_read}")
    print(f"snapshots_used={result.snapshots_used}")
    print(f"invalid_snapshots={result.invalid_snapshots}")
    print(f"opened_pairs={result.opened_pairs}")
    print(f"closed_pairs={metrics.total_pairs}")
    print(f"profitable_pairs={metrics.profitable_pairs}")
    print(f"losing_pairs={metrics.losing_pairs}")
    print(f"pair_winrate={metrics.pair_winrate}")
    print(f"pair_ev_ticks={metrics.pair_ev_ticks}")
    print(f"profit_factor={metrics.profit_factor}")
    print(f"fakeout_rate={metrics.fakeout_rate}")
    print(f"runner_to_breakeven_rate={metrics.runner_to_breakeven_rate}")
    print(f"max_drawdown={metrics.max_drawdown}")
    for instrument, pnl in metrics.pnl_by_instrument.items():
        print(f"pnl_ticks_{instrument}={pnl}")
    print(f"summary_json={result.artifacts.summary_json_path}")
    print(f"labels_jsonl={result.artifacts.labels_jsonl_path}")
    print(f"labels_csv={result.artifacts.labels_csv_path}")
    print(f"predictions_jsonl={result.artifacts.predictions_jsonl_path}")
    print(f"snapshots_jsonl={result.artifacts.snapshots_jsonl_path}")
    print(f"pair_events_jsonl={result.artifacts.pair_events_jsonl_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
