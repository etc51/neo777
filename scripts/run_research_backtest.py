"""Run offline research backtest from feature-store parquet."""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.research.backtest_runner import (  # noqa: E402
    ResearchBacktestConfig,
    run_research_backtest,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run NeoIntraday research backtest")
    parser.add_argument("--features", type=Path, default=Path("data/features"))
    parser.add_argument("--reports-dir", type=Path, default=Path("data/reports"))
    parser.add_argument(
        "--active-universe",
        type=Path,
        default=Path("configs/active_universe.yaml"),
    )
    parser.add_argument(
        "--strategy-config",
        type=Path,
        default=Path("configs/strategy.yaml"),
    )
    parser.add_argument("--quantity", default="1")
    args = parser.parse_args(argv)

    result = run_research_backtest(
        features_path=args.features,
        reports_dir=args.reports_dir,
        active_universe_path=args.active_universe,
        strategy_config_path=args.strategy_config,
        config=ResearchBacktestConfig(fixed_quantity=Decimal(args.quantity)),
    )
    print(f"backtest_report_json={result.artifacts.json_path}")
    print(f"backtest_trades_csv={result.artifacts.trades_csv_path}")
    print(f"backtest_summary_html={result.artifacts.summary_html_path}")
    print(f"feature_rows={result.feature_rows}")
    print(f"trades={result.metrics.trades}")
    print(f"total_pnl={result.metrics.total_pnl}")
    print(f"profit_factor={result.metrics.profit_factor}")
    print(f"commit_hash={result.commit_hash}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
