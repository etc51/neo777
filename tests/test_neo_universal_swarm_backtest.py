"""Tests for Neo Universal Swarm offline order-book backtests."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from neo_trader.neo_universal_swarm import (
    OrderbookBacktestConfig,
    SwarmInstrument,
    run_orderbook_backtest,
)


def test_orderbook_backtest_replays_jsonl_and_writes_reports(tmp_path: Path) -> None:
    source = tmp_path / "orderbook_snapshots.jsonl"
    start = datetime(2026, 7, 7, 10, 0, tzinfo=UTC)
    snapshots = (
        _record(start, bid="100", ask="101", bid_qty=220, ask_qty=45),
        _record(start + timedelta(seconds=1), bid="104", ask="105", bid_qty=240, ask_qty=40),
        _record(start + timedelta(seconds=2), bid="109", ask="110", bid_qty=260, ask_qty=35),
        _record(start + timedelta(seconds=3), bid="106", ask="107", bid_qty=150, ask_qty=120),
    )
    source.write_text(
        "\n".join(json.dumps(item, sort_keys=True) for item in snapshots) + "\n",
        encoding="utf-8",
    )

    result = run_orderbook_backtest(
        OrderbookBacktestConfig(
            sources=(source,),
            reports_dir=tmp_path / "reports",
            instruments=(SwarmInstrument.NEOBITOK,),
            max_pair_age_seconds=30,
        )
    )

    assert result.snapshots_used == 4
    assert result.opened_pairs >= 1
    assert result.metrics.total_pairs >= 1
    assert result.artifacts.summary_json_path.exists()
    assert result.artifacts.labels_jsonl_path.read_text(encoding="utf-8").strip()
    assert result.artifacts.predictions_jsonl_path.read_text(encoding="utf-8").strip()
    summary = json.loads(result.artifacts.summary_json_path.read_text(encoding="utf-8"))
    assert summary["runtime_mode"] == "offline-orderbook-backtest"
    assert summary["metrics"]["total_pairs"] == result.metrics.total_pairs


def _record(
    timestamp: datetime,
    *,
    bid: str,
    ask: str,
    bid_qty: int,
    ask_qty: int,
) -> dict[str, object]:
    return {
        "valid": True,
        "recorded_at": timestamp.isoformat(),
        "snapshot": {
            "timestamp": timestamp.isoformat(),
            "instrument": "NEOBITOK",
            "last_price": bid,
            "bid_levels": [
                {"price": bid, "quantity": bid_qty},
                {"price": str(float(bid) - 1), "quantity": 100},
                {"price": str(float(bid) - 2), "quantity": 80},
            ],
            "ask_levels": [
                {"price": ask, "quantity": ask_qty},
                {"price": str(float(ask) + 1), "quantity": 40},
                {"price": str(float(ask) + 2), "quantity": 30},
            ],
        },
    }
