"""Tests for session-aware offline research backtests."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from neo_trader.research.backtest_runner import (
    DATA_WINDOW_TOO_SHORT,
    RESEARCH_ONLY_NOT_FOR_LIVE,
    ResearchBacktestConfig,
    ResearchStrategyName,
    run_research_backtest,
)


def test_auto_from_data_builds_opening_range_from_first_timestamp(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    _write_feature_rows(paths["features"], mids=(100.0, 100.0, 101.0, 102.0, 103.0))
    research_config = _research_config(tmp_path, opening_range_minutes=2)

    result = run_research_backtest(
        features_path=paths["features"],
        reports_dir=paths["reports"],
        active_universe_path=paths["active_universe"],
        strategy_config_path=paths["strategy_config"],
        research_config_path=research_config,
        config=ResearchBacktestConfig(fixed_quantity=1),
    )

    assert result.session_window.opening_range_start is not None
    assert result.session_window.opening_range_start.isoformat() == "2026-07-07T10:00:00"
    assert result.session_window.opening_range_end is not None
    assert result.session_window.opening_range_end.isoformat() == "2026-07-07T10:02:00"
    assert result.session_window.rows_after_or_ready >= 2


def test_short_recording_not_all_opening_range_not_ready_when_rows_after_or_exist(
    tmp_path: Path,
) -> None:
    paths = _fixture_paths(tmp_path)
    _write_feature_rows(paths["features"], mids=(100.0, 100.0, 101.0, 102.0, 103.0, 104.0))
    research_config = _research_config(tmp_path, opening_range_minutes=2)

    result = run_research_backtest(
        features_path=paths["features"],
        reports_dir=paths["reports"],
        active_universe_path=paths["active_universe"],
        strategy_config_path=paths["strategy_config"],
        research_config_path=research_config,
        config=ResearchBacktestConfig(fixed_quantity=1),
    )

    rejects = result.metrics.reject_reason_distribution
    assert result.session_window.status == "OK"
    assert rejects.get("OPENING_RANGE_NOT_READY", 0) < result.feature_rows


def test_data_window_too_short_is_reported(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    _write_feature_rows(paths["features"], mids=(100.0, 100.1, 100.2))
    research_config = _research_config(
        tmp_path,
        opening_range_minutes=2,
        min_rows_after_opening_range=100,
        allow_short=False,
    )

    result = run_research_backtest(
        features_path=paths["features"],
        reports_dir=paths["reports"],
        active_universe_path=paths["active_universe"],
        strategy_config_path=paths["strategy_config"],
        research_config_path=research_config,
        config=ResearchBacktestConfig(fixed_quantity=1),
    )

    assert result.session_window.status == DATA_WINDOW_TOO_SHORT
    assert result.metrics.reject_reason_distribution[DATA_WINDOW_TOO_SHORT] >= 1


def test_simple_research_strategy_generates_trades_and_marks_reports(tmp_path: Path) -> None:
    paths = _fixture_paths(tmp_path)
    _write_feature_rows(
        paths["features"],
        mids=(100.0, 100.2, 100.4, 100.6, 100.8, 100.7, 100.5),
    )
    research_config = _research_config(tmp_path, opening_range_minutes=2)

    result = run_research_backtest(
        features_path=paths["features"],
        reports_dir=paths["reports"],
        active_universe_path=paths["active_universe"],
        strategy_config_path=paths["strategy_config"],
        research_config_path=research_config,
        strategy_name=ResearchStrategyName.SIMPLE_BOOK_MOMENTUM_RESEARCH,
        config=ResearchBacktestConfig(fixed_quantity=1),
    )

    payload = json.loads(result.artifacts.json_path.read_text(encoding="utf-8"))
    diagnostics = json.loads(result.artifacts.diagnostics_json_path.read_text(encoding="utf-8"))
    html = result.artifacts.summary_html_path.read_text(encoding="utf-8")

    assert result.metrics.trades >= 1
    assert result.research_mode_marker == RESEARCH_ONLY_NOT_FOR_LIVE
    assert payload["strategy_name"] == "simple_book_momentum_research"
    assert payload["research_mode_marker"] == RESEARCH_ONLY_NOT_FOR_LIVE
    assert "reject_reason_distribution" in payload["metrics"]
    assert diagnostics["research_mode_marker"] == RESEARCH_ONLY_NOT_FOR_LIVE
    assert RESEARCH_ONLY_NOT_FOR_LIVE in html


def _fixture_paths(tmp_path: Path) -> dict[str, Path]:
    return {
        "features": tmp_path / "features",
        "reports": tmp_path / "reports",
        "active_universe": _active_universe(tmp_path),
        "strategy_config": _strategy_config(tmp_path),
    }


def _write_feature_rows(features_path: Path, *, mids: tuple[float, ...]) -> None:
    path = features_path / "date=20260707" / "instrument=SBER" / "features.parquet"
    path.parent.mkdir(parents=True)
    base = datetime(2026, 7, 7, 7, 0, tzinfo=UTC)
    rows = [
        _feature_row(base + timedelta(minutes=index), mid, index=index)
        for index, mid in enumerate(mids)
    ]
    pq.write_table(pa.Table.from_pylist(rows), path)


def _feature_row(timestamp: datetime, mid: float, *, index: int) -> dict[str, Any]:
    sell_bias = index >= 5
    imbalance = -0.5 if sell_bias else 0.5
    microprice = mid - 0.08 if sell_bias else mid + 0.08
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
        "imbalance_5": imbalance,
        "imbalance_10": imbalance,
        "weighted_imbalance": imbalance,
        "microprice": microprice,
        "expected_slippage_bps_buy": 1.0,
        "expected_slippage_bps_sell": 1.0,
        "expected_slippage_bps": 1.0,
        "rv_1m": 2.0,
        "rv_5m": 5.0,
        "volatility_percentile": 50.0,
        "vwap": mid - 1.0,
        "ema_fast": mid - 0.2 if sell_bias else mid + 0.2,
        "ema_slow": mid + 0.2 if sell_bias else mid - 0.2,
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
                "opening_range_minutes: 30",
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


def _research_config(
    tmp_path: Path,
    *,
    opening_range_minutes: int,
    min_rows_after_opening_range: int = 1,
    allow_short: bool = True,
) -> Path:
    path = tmp_path / "research.yaml"
    path.write_text(
        "\n".join(
            (
                "research:",
                "  session_profile: auto_from_data",
                f"  opening_range_minutes: {opening_range_minutes}",
                f"  min_rows_after_opening_range: {min_rows_after_opening_range}",
                f"  allow_short_recording_backtest: {str(allow_short).lower()}",
                "",
            )
        ),
        encoding="utf-8",
    )
    return path
