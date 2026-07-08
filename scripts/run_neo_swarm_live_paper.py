"""Run the Neo Universal Bot Swarm on live T-Bank data in paper mode."""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.neo_universal_swarm.live_paper import (  # noqa: E402
    LivePaperSwarmConfig,
    run_live_paper_swarm,
)
from neo_trader.neo_universal_swarm.types import SwarmInstrument  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run Neo Universal Bot Swarm live-data paper trading"
    )
    parser.add_argument("--accounts", type=Path, default=Path("configs/accounts.yaml"))
    parser.add_argument("--orderbook-depth", type=int, default=10)
    parser.add_argument("--poll-interval-seconds", type=float, default=5.0)
    parser.add_argument("--min-required-ev-ticks", default="1")
    parser.add_argument("--slippage-stress-ticks", default="0")
    parser.add_argument("--max-pair-age-seconds", type=float, default=300.0)
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=Path("data/reports/neo_universal_swarm_live_paper"),
    )
    parser.add_argument(
        "--dashboard-state",
        type=Path,
        default=Path("data/monitoring/neo_universal_swarm_dashboard_state.json"),
    )
    parser.add_argument(
        "--heartbeat",
        type=Path,
        default=Path("data/monitoring/neo_universal_swarm_heartbeat.txt"),
    )
    parser.add_argument(
        "--online-learning-state",
        type=Path,
        default=None,
        help="Online learning JSON state. Defaults to reports-dir/online_learning_state.json.",
    )
    parser.add_argument(
        "--instrument",
        action="append",
        choices=[instrument.value for instrument in SwarmInstrument],
        help="Instrument to poll. Can be passed multiple times. Defaults to all.",
    )
    parser.add_argument("--max-cycles", type=int, default=None)
    args = parser.parse_args(argv)

    instruments = (
        tuple(SwarmInstrument(item) for item in args.instrument)
        if args.instrument
        else tuple(SwarmInstrument)
    )
    cycles = run_live_paper_swarm(
        LivePaperSwarmConfig(
            accounts_path=args.accounts,
            orderbook_depth=args.orderbook_depth,
            poll_interval_seconds=args.poll_interval_seconds,
            min_required_ev_ticks=Decimal(args.min_required_ev_ticks),
            slippage_stress_ticks=Decimal(args.slippage_stress_ticks),
            max_pair_age_seconds=args.max_pair_age_seconds,
            reports_dir=args.reports_dir,
            dashboard_state_path=args.dashboard_state,
            heartbeat_path=args.heartbeat,
            online_learning_state_path=args.online_learning_state,
            instruments=instruments,
            max_cycles=args.max_cycles,
        )
    )
    for cycle in cycles:
        print(
            " ".join(
                [
                    f"cycle={cycle.cycle}",
                    f"status={cycle.status}",
                    f"snapshots={cycle.snapshots}",
                    f"active_pairs={cycle.active_pairs}",
                    f"closed_pairs={cycle.closed_pairs}",
                    f"total_pnl_ticks={cycle.total_pnl_ticks}",
                ]
            )
        )
    return 0 if all(cycle.status == "OK" for cycle in cycles) else 1


if __name__ == "__main__":
    raise SystemExit(main())
