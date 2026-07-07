"""Tests for offline research backtest reports."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq

from neo_trader.research.backtest_runner import (
    ResearchBacktestConfig,
    run_research_backtest,
)


def test_research_backtest_runner_writes_json_csv_and_html(tmp_path: Path) -> None:
    features_path = tmp_path / "features"
    reports_dir = tmp_path / "reports"
    active_universe = _active_universe(tmp_path)
    strategy_config = _strategy_config(tmp_path)
    _write_feature_rows(features_path)

    result = run_research_backtest(
        features_path=features_path,
        reports_dir=reports_dir,
        active_universe_path=active_universe,
        strategy_config_path=strategy_config,
        config=ResearchBacktestConfig(fixed_quantity=1),
    )

    assert result.artifacts.json_path.exists()
    assert result.artifacts.trades_csv_path.exists()
    assert result.artifacts.summary_html_path.exists()
    assert result.metrics.trades >= 1
    assert result.metrics.reason_code_distribution

    payload = json.loads(result.artifacts.json_path.read_text(encoding="utf-8"))
    assert payload["metrics"]["trades"] == result.metrics.trades
    assert "reason_code_distribution" in payload["metrics"]
    assert "pnl" in result.artifacts.trades_csv_path.read_text(encoding="utf-8")
    assert "NeoIntraday research backtest" in result.artifacts.summary_html_path.read_text(
        encoding="utf-8"
    )


def _write_feature_rows(features_path: Path) -> None:
    path = features_path / "date=20260707" / "instrument=SBER" / "features.parquet"
    path.parent.mkdir(parents=True)
    base = datetime(2026, 7, 7, 7, 0, tzinfo=UTC)
    rows = [_feature_row(base + timedelta(minutes=index), mid) for index, mid in enumerate((
        100.0,
        100.0,
        100.0,
        101.0,
        103.0,
    ))]
    pq.write_table(pa.Table.from_pylist(rows), path)


def _feature_row(timestamp: datetime, mid: float) -> dict[str, Any]:
    return {
        "timestamp": timestamp.isoformat(),
        "instrument": "SBER",
        "instrument_uid": "UID1",
        "mid": mid,
        "best_bid": mid - 0.05,
        "best_ask": mid + 0.05,
        "spread_bps": 1.0,
        "depth_top5_bid": 500.0,
        "depth_top5_ask": 300.0,
        "depth_top10_bid": 1000.0,
        "depth_top10_ask": 600.0,
        "imbalance_5": 0.5,
        "imbalance_10": 0.5,
        "weighted_imbalance": 0.5,
        "microprice": mid + 0.08,
        "expected_slippage_bps_buy": 1.0,
        "expected_slippage_bps_sell": 1.0,
        "expected_slippage_bps": 1.0,
        "rv_1m": 2.0,
        "rv_5m": 5.0,
        "volatility_percentile": 50.0,
        "vwap": 99.0,
        "ema_fast": mid + 0.2,
        "ema_slow": mid - 0.2,
        "volatility_regime": "normal",
        "bid_wall_score": 1.0,
        "ask_wall_score": 1.0,
        "usable_row": True,
    }


def _active_universe(tmp_path: Path) -> Path:
    path = tmp_path / "active_universe.yaml"
    path.write_text(
        "\n".join(
            (
                "generated_by: test",
                "instruments:",
                "- ticker: SBER",
                "  uid: UID1",
                "  enabled: true",
                "",
            )
        ),
        encoding="utf-8",
    )
    return path


def _strategy_config(tmp_path: Path) -> Path:
    path = tmp_path / "strategy.yaml"
    path.write_text(
        "\n".join(
            (
                'session_start: "10:00:00"',
                'session_end: "18:45:00"',
                "opening_range_minutes: 5",
                "opening_range_candle_count: 3",
                "entry_window_minutes: 120",
                "force_exit_minutes_before_close: 5",
                'breakout_buffer_bps: "1"',
                'max_spread_bps: "10"',
                "min_volatility_percentile:",
                "max_volatility_percentile:",
                "low_volatility_regimes: []",
                "high_volatility_regimes: []",
                'max_expected_slippage_bps: "15"',
                'min_imbalance: "0.10"',
                'min_weighted_imbalance: "0.10"',
                'min_microprice_edge_bps: "0.10"',
                'max_opposing_wall_score: "3"',
                "require_ofi_confirmation: false",
                'min_ofi_confirmation: "0"',
                'min_confidence: "0.10"',
                "atr_window: 3",
                'stop_atr_multiple: "1"',
                "take_profit_r_multiples:",
                '  - "1"',
                '  - "2"',
                "",
            )
        ),
        encoding="utf-8",
    )
    return path


def _read_rows(path: Path) -> list[dict[str, Any]]:
    return cast(list[dict[str, Any]], pq.read_table(path).to_pylist())
