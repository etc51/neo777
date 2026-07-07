"""Validate latest research strategy reports without live execution."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.research.strategy_validation import validate_strategy_reports  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    """Run Strategy Validation Sprint 1 report generation."""

    parser = _build_parser()
    args = parser.parse_args(argv)
    liquidity_report = args.liquidity_report or _latest(
        args.reports_dir,
        "liquidity_report_*.json",
    )
    or_report = args.or_report or _latest(args.reports_dir, "backtest_or_auto_*.json")
    or_trades = args.or_trades or _same_stem(or_report, ".csv")
    simple_report = args.simple_report or _latest(
        args.reports_dir,
        "backtest_simple_book_momentum_*.json",
    )
    simple_trades = args.simple_trades or _same_stem(simple_report, ".csv")

    result = validate_strategy_reports(
        liquidity_report_path=liquidity_report,
        or_report_path=or_report,
        or_trades_csv_path=or_trades,
        simple_report_path=simple_report,
        simple_trades_csv_path=simple_trades,
        reports_dir=args.reports_dir,
        neoassets_config_path=args.neoassets_config,
        active_universe_next_path=args.active_universe_next,
    )
    print(f"strategy_validation_json={result.artifacts.json_path}")
    print(f"strategy_validation_csv={result.artifacts.csv_path}")
    print(f"strategy_validation_html={result.artifacts.html_path}")
    print(f"active_universe_next={result.artifacts.next_universe_path}")
    print(f"or_verdict={result.or_verdict}")
    print(f"simple_verdict={result.simple_verdict}")
    print("best_instruments=" + ",".join(result.best_instruments))
    print("disabled_instruments=" + ",".join(result.disabled_instruments))
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate NeoIntraday research strategies")
    parser.add_argument("--reports-dir", type=Path, default=Path("data/reports"))
    parser.add_argument("--liquidity-report", type=Path)
    parser.add_argument("--or-report", type=Path)
    parser.add_argument("--or-trades", type=Path)
    parser.add_argument("--simple-report", type=Path)
    parser.add_argument("--simple-trades", type=Path)
    parser.add_argument(
        "--neoassets-config",
        type=Path,
        default=Path("configs/neoassets_universe.yaml"),
    )
    parser.add_argument(
        "--active-universe-next",
        type=Path,
        default=Path("configs/active_universe_next.yaml"),
    )
    return parser


def _latest(directory: Path, pattern: str) -> Path:
    matches = sorted(directory.glob(pattern), key=lambda path: path.stat().st_mtime, reverse=True)
    if not matches:
        raise FileNotFoundError(f"no reports found: {directory / pattern}")
    return matches[0]


def _same_stem(path: Path, suffix: str) -> Path:
    candidate = path.with_suffix(suffix)
    if not candidate.exists():
        raise FileNotFoundError(f"missing companion report: {candidate}")
    return candidate


if __name__ == "__main__":
    raise SystemExit(main())
