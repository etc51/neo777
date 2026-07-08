from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from neo_swarm_scalper.config import load_config
from neo_swarm_scalper.reports import build_report_payload
from neo_swarm_scalper.run import MockNeoMarketDataProvider, run_swarm
from neo_swarm_scalper.storage import SQLiteJournal


class GraceWideSpreadProvider(MockNeoMarketDataProvider):
    def __init__(self) -> None:
        super().__init__()
        self._per_instrument_counter: dict[str, int] = {}

    def get_orderbook_snapshot(self, instrument_id: str, *, depth: int) -> Mapping[str, Any]:
        count = self._per_instrument_counter.get(instrument_id, 0) + 1
        self._per_instrument_counter[instrument_id] = count
        base, tick = _base_tick(instrument_id)
        if count == 1:
            bid = base - tick
            ask = base + tick
        elif count in {2, 3}:
            bid = base + (tick * Decimal("3"))
            ask = bid + (tick * Decimal("10"))
        else:
            bid = base - (tick * Decimal("4"))
            ask = base - (tick * Decimal("2"))
        return _book(instrument_id, depth, bid, ask)

    def get_recent_trades(self, instrument_id: str) -> Sequence[Mapping[str, Any]]:
        base, _ = _base_tick(instrument_id)
        return [{"price": str(base), "quantity": "1", "side": "BUY"}]


class StableTimeExitProvider(MockNeoMarketDataProvider):
    def __init__(self) -> None:
        super().__init__()
        self._per_instrument_counter: dict[str, int] = {}

    def get_orderbook_snapshot(self, instrument_id: str, *, depth: int) -> Mapping[str, Any]:
        count = self._per_instrument_counter.get(instrument_id, 0) + 1
        self._per_instrument_counter[instrument_id] = count
        base, tick = _base_tick(instrument_id)
        if count == 1:
            bid = base - tick
            ask = base + tick
        else:
            bid = base + (tick * Decimal("3"))
            ask = base + (tick * Decimal("5"))
        return _book(instrument_id, depth, bid, ask)

    def get_recent_trades(self, instrument_id: str) -> Sequence[Mapping[str, Any]]:
        base, _ = _base_tick(instrument_id)
        return [{"price": str(base), "quantity": "1", "side": "BUY"}]


def test_wide_spread_grace_logs_avoided_exit_without_uniform_spread_close(
    tmp_path: Path,
) -> None:
    result = run_swarm(
        load_config(),
        max_cycles=4,
        poll_interval_sec=0,
        provider=GraceWideSpreadProvider(),
        db_path=tmp_path / "spread_grace.sqlite",
        reports_dir=tmp_path / "reports",
        sleep=lambda _: None,
    )
    storage = SQLiteJournal(result.db_path)
    avoided = storage.fetch_all(
        """
        SELECT COUNT(*) AS count
        FROM shadow_trade_events
        WHERE event_type = 'avoided_spread_shock_exit'
        """
    )[0]["count"]
    spread_shock_exits = storage.fetch_all(
        """
        SELECT COUNT(*) AS count
        FROM shadow_trades
        WHERE exit_reason = 'spread_shock'
        """
    )[0]["count"]
    stop_exits = storage.fetch_all(
        """
        SELECT COUNT(*) AS count
        FROM shadow_trades
        WHERE exit_reason IN ('stop_loss', 'protected_stop')
        """
    )[0]["count"]
    open_trades = storage.fetch_all(
        "SELECT COUNT(*) AS count FROM shadow_trades WHERE status = 'OPEN'"
    )[0]["count"]
    assert avoided > 0
    assert spread_shock_exits == 0
    assert stop_exits > 0
    assert open_trades > 0


def test_stop_comparison_invalid_when_all_stop_ticks_share_one_exit_reason(
    tmp_path: Path,
) -> None:
    start = datetime(2026, 7, 8, tzinfo=UTC)
    timestamps = iter(
        [
            start,
            start,
            start + timedelta(seconds=3700),
        ]
    )

    def clock() -> datetime:
        return next(timestamps, start + timedelta(seconds=3700))

    result = run_swarm(
        load_config(),
        max_cycles=2,
        poll_interval_sec=0,
        provider=StableTimeExitProvider(),
        db_path=tmp_path / "time_exit.sqlite",
        reports_dir=tmp_path / "reports",
        clock=clock,
        sleep=lambda _: None,
    )
    storage = SQLiteJournal(result.db_path)
    payload = build_report_payload(
        storage=storage,
        config=load_config(),
        runtime_start=start,
        generated_at=start,
    )
    assert payload["shadow_exit_summary"]["time_exits"] > 0
    assert payload["shadow_stop_comparison"]["status"] == "INVALID"
    assert (
        payload["shadow_stop_comparison"]["reason"]
        == "all stop_ticks closed with one exit_reason"
    )


def _base_tick(instrument_id: str) -> tuple[Decimal, Decimal]:
    is_btc = "4eff" in instrument_id or "BTC" in instrument_id.upper()
    return (Decimal("1000"), Decimal("0.1")) if is_btc else (Decimal("100"), Decimal("0.01"))


def _book(
    instrument_id: str,
    depth: int,
    bid: Decimal,
    ask: Decimal,
) -> dict[str, Any]:
    _, tick = _base_tick(instrument_id)
    mid = (bid + ask) / Decimal("2")
    return {
        "instrumentUid": instrument_id,
        "depth": depth,
        "bids": [
            {"price": str(bid - Decimal(i) * tick), "quantity": 220 + i * 5}
            for i in range(min(depth, 10))
        ],
        "asks": [
            {"price": str(ask + Decimal(i) * tick), "quantity": 40 + i * 5}
            for i in range(min(depth, 10))
        ],
        "lastPrice": str(mid),
    }
