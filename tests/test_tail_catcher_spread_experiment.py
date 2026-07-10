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
        if count <= 4:
            mid = base + (tick * Decimal((count - 1) * 2))
            bid = mid - tick
            ask = mid + tick
        elif count in {5, 6}:
            mid = base - (tick * Decimal(6 if count == 5 else 8))
            bid = mid - (tick * Decimal("70"))
            ask = mid + (tick * Decimal("70"))
        else:
            bid = base - (tick * Decimal("9"))
            ask = base - (tick * Decimal("7"))
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
        mid = base + (tick * Decimal(min(count - 1, 3) * 2))
        bid = mid - tick
        ask = mid + tick
        return _book(instrument_id, depth, bid, ask)

    def get_recent_trades(self, instrument_id: str) -> Sequence[Mapping[str, Any]]:
        base, _ = _base_tick(instrument_id)
        return [{"price": str(base), "quantity": "1", "side": "BUY"}]


class SymmetricSpreadExpansionProvider(MockNeoMarketDataProvider):
    def __init__(self) -> None:
        super().__init__()
        self._per_instrument_counter: dict[str, int] = {}

    def get_orderbook_snapshot(self, instrument_id: str, *, depth: int) -> Mapping[str, Any]:
        count = self._per_instrument_counter.get(instrument_id, 0) + 1
        self._per_instrument_counter[instrument_id] = count
        base, tick = _base_tick(instrument_id)
        mid = base + (tick * Decimal(min(count - 1, 3) * 2))
        if count <= 4:
            bid = mid - tick
            ask = mid + tick
        else:
            bid = mid - (tick * Decimal("70"))
            ask = mid + (tick * Decimal("70"))
        return _book(instrument_id, depth, bid, ask)

    def get_recent_trades(self, instrument_id: str) -> Sequence[Mapping[str, Any]]:
        base, _ = _base_tick(instrument_id)
        return [{"price": str(base), "quantity": "1", "side": "BUY"}]


class ProtectionFloorProvider(MockNeoMarketDataProvider):
    def __init__(self) -> None:
        super().__init__()
        self._per_instrument_counter: dict[str, int] = {}

    def get_orderbook_snapshot(self, instrument_id: str, *, depth: int) -> Mapping[str, Any]:
        count = self._per_instrument_counter.get(instrument_id, 0) + 1
        self._per_instrument_counter[instrument_id] = count
        base, tick = _base_tick(instrument_id)
        path_ticks = (0, 2, 4, 6, 12, 10, 10)
        mid = base + (tick * Decimal(path_ticks[min(count - 1, len(path_ticks) - 1)]))
        return _book(instrument_id, depth, mid - tick, mid + tick)

    def get_recent_trades(self, instrument_id: str) -> Sequence[Mapping[str, Any]]:
        base, _ = _base_tick(instrument_id)
        return [{"price": str(base), "quantity": "1", "side": "BUY"}]


def test_wide_spread_grace_logs_avoided_exit_without_uniform_spread_close(
    tmp_path: Path,
) -> None:
    result = run_swarm(
        load_config(),
        max_cycles=7,
        poll_interval_sec=0,
        provider=GraceWideSpreadProvider(),
        db_path=tmp_path / "spread_grace.sqlite",
        reports_dir=tmp_path / "reports",
        clock=_advancing_clock(datetime(2026, 7, 10, tzinfo=UTC)),
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
            start + timedelta(seconds=1),
            start + timedelta(seconds=2),
            start + timedelta(seconds=3),
            start + timedelta(seconds=4),
            start + timedelta(seconds=130),
        ]
    )

    def clock() -> datetime:
        return next(timestamps, start + timedelta(seconds=131))

    result = run_swarm(
        load_config(),
        max_cycles=6,
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
        payload["shadow_stop_comparison"]["reason"] == "all stop_ticks closed with one exit_reason"
    )


def test_symmetric_spread_expansion_does_not_trigger_stop_or_panic(tmp_path: Path) -> None:
    result = run_swarm(
        load_config(),
        max_cycles=7,
        poll_interval_sec=0,
        provider=SymmetricSpreadExpansionProvider(),
        db_path=tmp_path / "symmetric_spread.sqlite",
        reports_dir=tmp_path / "reports",
        clock=_advancing_clock(datetime(2026, 7, 10, tzinfo=UTC)),
        sleep=lambda _: None,
    )
    storage = SQLiteJournal(result.db_path)
    closed = storage.fetch_all(
        "SELECT COUNT(*) AS count FROM shadow_trades WHERE status = 'CLOSED'"
    )[0]["count"]
    stop_cycles = storage.fetch_all("SELECT MAX(stop_trigger_cycles) AS cycles FROM shadow_trades")[
        0
    ]["cycles"]
    avoided = storage.fetch_all(
        """
        SELECT COUNT(*) AS count
        FROM shadow_trade_events
        WHERE event_type = 'avoided_spread_shock_exit'
        """
    )[0]["count"]
    assert closed == 0
    assert stop_cycles == 0
    assert avoided > 0


def test_protection_floor_has_its_own_exit_reason(tmp_path: Path) -> None:
    result = run_swarm(
        load_config(),
        max_cycles=7,
        poll_interval_sec=0,
        provider=ProtectionFloorProvider(),
        db_path=tmp_path / "protection.sqlite",
        reports_dir=tmp_path / "reports",
        clock=_advancing_clock(datetime(2026, 7, 10, tzinfo=UTC)),
        sleep=lambda _: None,
    )
    storage = SQLiteJournal(result.db_path)
    controls = storage.fetch_all(
        """
        SELECT exit_reason, protected_exit_reason, exit_price, entry_price
        FROM shadow_trades
        WHERE is_control = 1
        """
    )
    assert controls
    assert all(row["exit_reason"] == "protected_exit" for row in controls)
    assert all(row["protected_exit_reason"] == "protection_floor" for row in controls)


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


def _advancing_clock(start: datetime) -> Any:
    current = start

    def clock() -> datetime:
        nonlocal current
        value = current
        current += timedelta(seconds=1)
        return value

    return clock
