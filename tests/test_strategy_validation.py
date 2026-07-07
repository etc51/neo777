"""Tests for Strategy Validation Sprint 1 reports."""

from __future__ import annotations

import csv
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import yaml

from neo_trader.research.strategy_validation import validate_strategy_reports


def test_strategy_validation_generates_reports_stress_and_next_universe(
    tmp_path: Path,
) -> None:
    reports = tmp_path / "reports"
    configs = tmp_path / "configs"
    liquidity = reports / "liquidity_report.json"
    or_report = reports / "backtest_or_auto.json"
    or_trades = reports / "backtest_or_auto.csv"
    simple_report = reports / "backtest_simple_book_momentum.json"
    simple_trades = reports / "backtest_simple_book_momentum.csv"
    neoassets = configs / "neoassets_universe.yaml"
    next_universe = configs / "active_universe_next.yaml"
    reports.mkdir()
    configs.mkdir()

    _write_liquidity(liquidity)
    _write_backtest_report(or_report, "opening_range_book_momentum", _or_rows())
    _write_trades(or_trades, _or_rows())
    _write_backtest_report(
        simple_report,
        "simple_book_momentum_research",
        _simple_rows(),
        research_marker="RESEARCH_ONLY_NOT_FOR_LIVE",
    )
    _write_trades(simple_trades, _simple_rows())
    _write_neoassets(neoassets)

    result = validate_strategy_reports(
        liquidity_report_path=liquidity,
        or_report_path=or_report,
        or_trades_csv_path=or_trades,
        simple_report_path=simple_report,
        simple_trades_csv_path=simple_trades,
        reports_dir=reports,
        neoassets_config_path=neoassets,
        active_universe_next_path=next_universe,
    )

    assert result.artifacts.json_path.exists()
    assert result.artifacts.csv_path.exists()
    assert result.artifacts.html_path.exists()
    assert result.artifacts.next_universe_path.exists()

    payload = json.loads(result.artifacts.json_path.read_text(encoding="utf-8"))
    or_payload = payload["strategies"]["opening_range_book_momentum"]
    stress = or_payload["stress_tests"]
    assert "slippage_x2" in stress
    assert "remove_best_instrument" in stress
    assert "only_crypto_neoassets" in stress
    assert Decimal(stress["slippage_x2"]["total_pnl"]) < Decimal(
        stress["slippage_x1"]["total_pnl"]
    )
    assert "reject_reason_distribution" in or_payload
    assert "reason_code_distribution" in or_payload

    next_payload = yaml.safe_load(next_universe.read_text(encoding="utf-8"))
    enabled = [item["ticker"] for item in next_payload["instruments"] if item["enabled"]]
    assert enabled
    assert len(enabled) <= 3
    assert "strategy_validation_" in result.artifacts.json_path.name
    assert "strategy_validation_" in result.artifacts.csv_path.name
    assert "strategy_validation_" in result.artifacts.html_path.name


def test_simple_strategy_verdict_remains_diagnostic_only(tmp_path: Path) -> None:
    reports = tmp_path / "reports"
    configs = tmp_path / "configs"
    reports.mkdir()
    configs.mkdir()
    liquidity = reports / "liquidity_report.json"
    or_report = reports / "backtest_or_auto.json"
    or_trades = reports / "backtest_or_auto.csv"
    simple_report = reports / "backtest_simple_book_momentum.json"
    simple_trades = reports / "backtest_simple_book_momentum.csv"
    neoassets = configs / "neoassets_universe.yaml"

    _write_liquidity(liquidity)
    _write_backtest_report(or_report, "opening_range_book_momentum", _or_rows())
    _write_trades(or_trades, _or_rows())
    _write_backtest_report(
        simple_report,
        "simple_book_momentum_research",
        _simple_rows(),
        research_marker="RESEARCH_ONLY_NOT_FOR_LIVE",
    )
    _write_trades(simple_trades, _simple_rows())
    _write_neoassets(neoassets)

    result = validate_strategy_reports(
        liquidity_report_path=liquidity,
        or_report_path=or_report,
        or_trades_csv_path=or_trades,
        simple_report_path=simple_report,
        simple_trades_csv_path=simple_trades,
        reports_dir=reports,
        neoassets_config_path=neoassets,
        active_universe_next_path=configs / "active_universe_next.yaml",
    )

    assert result.simple_verdict == "diagnostic only"


def _write_liquidity(path: Path) -> None:
    payload = {
        "ranked_universe": [
            {"ticker": "BTCUSDperpA", "instrument_uid": "UID_BTC", "score": "60"},
            {"ticker": "ETHUSDperpA", "instrument_uid": "UID_ETH", "score": "50"},
            {"ticker": "STOCKperpA", "instrument_uid": "UID_STOCK", "score": "40"},
        ],
        "instruments": {},
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_backtest_report(
    path: Path,
    strategy_name: str,
    rows: list[dict[str, str]],
    *,
    research_marker: str | None = None,
) -> None:
    pnls = [Decimal(row["pnl"]) for row in rows]
    payload = {
        "strategy_name": strategy_name,
        "research_mode_marker": research_marker,
        "session": {"timezone": "Europe/Moscow"},
        "metrics": {
            "total_pnl": str(sum(pnls, Decimal("0"))),
            "trades": len(rows),
            "winrate": "0.5",
            "profit_factor": "2",
            "max_drawdown": "1",
            "avg_slippage_bps": "2",
            "missed_fill_rate": "0",
            "pnl_by_instrument": {},
            "pnl_by_hour": {},
            "pnl_by_spread_regime": {},
            "pnl_by_volatility_regime": {},
            "reason_code_distribution": {"TEST_SIGNAL": len(rows)},
            "reject_reason_distribution": {"TEST_REJECT": 3},
        },
        "trades": [],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_trades(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = (
        "instrument",
        "instrument_uid",
        "side",
        "entry_time",
        "exit_time",
        "quantity",
        "entry_price",
        "exit_price",
        "pnl",
        "mae",
        "mfe",
        "entry_slippage_bps",
        "exit_slippage_bps",
        "entry_spread_regime",
        "entry_volatility_regime",
        "entry_reason_codes",
        "exit_reason_codes",
    )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_neoassets(path: Path) -> None:
    path.write_text(
        yaml.safe_dump(
            {
                "instruments": [
                    {
                        "ticker": "BTCUSDperpA",
                        "uid": "UID_BTC",
                        "name": "Neo Bitcoin",
                        "instrument_type": "futures",
                        "class_code": "SPBDMFUT",
                    },
                    {
                        "ticker": "ETHUSDperpA",
                        "uid": "UID_ETH",
                        "name": "Neo Ethereum",
                        "instrument_type": "futures",
                        "class_code": "SPBDMFUT",
                    },
                    {
                        "ticker": "STOCKperpA",
                        "uid": "UID_STOCK",
                        "name": "Neo Stock",
                        "instrument_type": "futures",
                        "class_code": "SPBDMFUT",
                    },
                ]
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def _or_rows() -> list[dict[str, str]]:
    return [
        _row("BTCUSDperpA", "UID_BTC", 0, "5", "100", "101"),
        _row("BTCUSDperpA", "UID_BTC", 1, "-1", "101", "100.5"),
        _row("ETHUSDperpA", "UID_ETH", 2, "2", "50", "51"),
        _row("STOCKperpA", "UID_STOCK", 3, "-3", "20", "19"),
    ]


def _simple_rows() -> list[dict[str, str]]:
    return [
        _row("BTCUSDperpA", "UID_BTC", 0, "7", "100", "102"),
        _row("ETHUSDperpA", "UID_ETH", 1, "-2", "50", "49"),
    ]


def _row(
    instrument: str,
    uid: str,
    minute_offset: int,
    pnl: str,
    entry_price: str,
    exit_price: str,
) -> dict[str, str]:
    entry = datetime(2026, 7, 7, 10, 0, tzinfo=UTC) + timedelta(minutes=minute_offset)
    exit_time = entry + timedelta(minutes=1)
    return {
        "instrument": instrument,
        "instrument_uid": uid,
        "side": "LONG",
        "entry_time": entry.isoformat(),
        "exit_time": exit_time.isoformat(),
        "quantity": "1",
        "entry_price": entry_price,
        "exit_price": exit_price,
        "pnl": pnl,
        "mae": "-1",
        "mfe": "3",
        "entry_slippage_bps": "1",
        "exit_slippage_bps": "1",
        "entry_spread_regime": "normal",
        "entry_volatility_regime": "normal",
        "entry_reason_codes": "TEST_ENTRY",
        "exit_reason_codes": "TEST_EXIT",
    }
