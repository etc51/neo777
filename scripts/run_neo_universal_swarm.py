"""Run Neo Universal Bot Swarm paper simulation."""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.neo_universal_swarm.paper import run_paper_simulation  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Neo Universal Bot Swarm paper simulation")
    parser.add_argument("--accounts", type=Path, default=Path("configs/accounts.yaml"))
    parser.add_argument("--target-pairs", type=int, default=3200)
    parser.add_argument("--min-required-ev-ticks", default="1")
    parser.add_argument("--slippage-stress-ticks", default="0")
    parser.add_argument("--seed", type=int, default=777)
    parser.add_argument(
        "--reports-dir",
        type=Path,
        default=Path("data/reports/neo_universal_swarm"),
    )
    parser.add_argument(
        "--dashboard-state",
        type=Path,
        default=Path("data/monitoring/neo_universal_swarm_dashboard_state.json"),
    )
    parser.add_argument(
        "--no-stress-grid",
        action="store_true",
        help="Skip extra slippage stress runs.",
    )
    args = parser.parse_args(argv)

    result = run_paper_simulation(
        accounts_path=args.accounts,
        target_pairs=args.target_pairs,
        min_required_ev_ticks=Decimal(args.min_required_ev_ticks),
        slippage_stress_ticks=Decimal(args.slippage_stress_ticks),
        seed=args.seed,
        reports_dir=args.reports_dir,
        dashboard_state_path=args.dashboard_state,
        include_stress_grid=not args.no_stress_grid,
    )
    metrics = result.metrics
    print(f"commit_hash={result.commit_hash}")
    print(f"total_pairs={metrics.total_pairs}")
    print(f"profitable_pairs={metrics.profitable_pairs}")
    print(f"losing_pairs={metrics.losing_pairs}")
    print(f"pair_winrate={metrics.pair_winrate}")
    print(f"pair_ev_ticks={metrics.pair_ev_ticks}")
    print(f"pair_ev_rub={metrics.pair_ev_rub}")
    print(f"profit_factor={metrics.profit_factor}")
    print(f"max_drawdown={metrics.max_drawdown}")
    for instrument, pnl in metrics.pnl_by_instrument.items():
        print(f"pnl_ticks_{instrument}={pnl}")
    for key, stress_metrics in result.stress_metrics.items():
        print(f"{key}_pair_ev_ticks={stress_metrics.pair_ev_ticks}")
        print(f"{key}_profit_factor={stress_metrics.profit_factor}")
    if result.artifacts is not None:
        print(f"summary_json={result.artifacts.summary_json_path}")
        print(f"labels_csv={result.artifacts.labels_csv_path}")
        print(f"dashboard_state={result.artifacts.dashboard_state_path}")
        print(f"events_jsonl={result.artifacts.events_jsonl_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
