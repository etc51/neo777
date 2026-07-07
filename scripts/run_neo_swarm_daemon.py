"""Run the Neo Universal Bot Swarm paper daemon."""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.neo_universal_swarm.daemon import (  # noqa: E402
    SwarmDaemonConfig,
    run_swarm_daemon,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Neo Universal Bot Swarm paper daemon")
    parser.add_argument("--accounts", type=Path, default=Path("configs/accounts.yaml"))
    parser.add_argument("--target-pairs-per-cycle", type=int, default=200)
    parser.add_argument("--min-required-ev-ticks", default="1")
    parser.add_argument("--slippage-stress-ticks", default="0")
    parser.add_argument("--cycle-interval-seconds", type=float, default=60.0)
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=Path("data/reports/neo_universal_swarm_daemon"),
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
    parser.add_argument("--seed", type=int, default=777)
    parser.add_argument("--include-stress-grid", action="store_true")
    parser.add_argument("--max-cycles", type=int, default=None)
    args = parser.parse_args(argv)

    cycles = run_swarm_daemon(
        SwarmDaemonConfig(
            accounts_path=args.accounts,
            target_pairs_per_cycle=args.target_pairs_per_cycle,
            min_required_ev_ticks=Decimal(args.min_required_ev_ticks),
            slippage_stress_ticks=Decimal(args.slippage_stress_ticks),
            cycle_interval_seconds=args.cycle_interval_seconds,
            reports_dir=args.reports_dir,
            dashboard_state_path=args.dashboard_state,
            heartbeat_path=args.heartbeat,
            seed=args.seed,
            include_stress_grid=args.include_stress_grid,
            max_cycles=args.max_cycles,
        )
    )
    for cycle in cycles:
        print(
            " ".join(
                [
                    f"cycle={cycle.cycle}",
                    f"status={cycle.status}",
                    f"total_pairs={cycle.total_pairs}",
                    f"pair_ev_ticks={cycle.pair_ev_ticks}",
                    f"profit_factor={cycle.profit_factor}",
                ]
            )
        )
    return 0 if all(cycle.status == "OK" for cycle in cycles) else 1


if __name__ == "__main__":
    raise SystemExit(main())
