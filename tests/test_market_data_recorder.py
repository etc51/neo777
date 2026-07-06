"""Tests for raw market-data recording."""

from __future__ import annotations

import asyncio
import importlib
import json
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

from neo_trader.data.market_data_recorder import (
    MarketDataEventType,
    MarketDataQualityCounters,
    MarketDataRecorder,
    MarketDataSource,
    MarketDataSubscription,
    RawMarketDataEvent,
    build_tbank_market_data_requests,
)


class FakeMarketDataSource:
    def __init__(
        self,
        events: Sequence[RawMarketDataEvent],
        *,
        fail_before_events: bool = False,
    ) -> None:
        self.events = events
        self.fail_before_events = fail_before_events

    async def stream(
        self,
        subscriptions: Sequence[MarketDataSubscription],
    ) -> AsyncIterator[RawMarketDataEvent]:
        assert subscriptions
        if self.fail_before_events:
            raise RuntimeError("temporary stream failure")
        for event in self.events:
            yield event


def test_market_data_recorder_writes_parquet_partitions(tmp_path: Path) -> None:
    received_at = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    events = [
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.ORDERBOOK,
            received_at=received_at,
            payload={"orderbook": {"depth": 10}},
            sequence=1,
        ),
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.TRADES,
            received_at=received_at + timedelta(seconds=1),
            payload={"trade": {"quantity": 2}},
            sequence=2,
        ),
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.CANDLES,
            received_at=received_at + timedelta(seconds=2),
            payload={"candle": {"interval": "SUBSCRIPTION_INTERVAL_ONE_MINUTE"}},
            sequence=3,
        ),
    ]
    subscriptions = [
        MarketDataSubscription.orderbook("UID1"),
        MarketDataSubscription.trades("UID1"),
        MarketDataSubscription.candles("UID1"),
    ]
    recorder = MarketDataRecorder(
        root=tmp_path / "data" / "raw",
        flush_rows=2,
        heartbeat_interval_seconds=60,
    )

    result = asyncio.run(
        recorder.run(
            lambda: FakeMarketDataSource(events),
            subscriptions,
            stop_after_events=3,
            max_reconnects=0,
        )
    )

    assert result.events_recorded == 3
    for event_type in MarketDataEventType:
        path = (
            tmp_path
            / "data"
            / "raw"
            / "date=2026-07-06"
            / "instrument=UID1"
            / f"type={event_type.value}.parquet"
        )
        assert path.exists()
        rows = _read_parquet_rows(path)
        assert len(rows) == 1
        assert rows[0]["event_type"] == event_type.value
        assert json.loads(rows[0]["payload_json"])


def test_quality_counters_track_sequence_and_time_gaps() -> None:
    now = datetime(2026, 7, 6, 12, 0, tzinfo=UTC)
    current = now

    def clock() -> datetime:
        return current

    counters = MarketDataQualityCounters(max_gap_seconds=5, clock=clock)
    counters.observe(
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.TRADES,
            received_at=now,
            payload={},
            sequence=1,
        )
    )
    current = now + timedelta(seconds=10)
    counters.observe(
        RawMarketDataEvent.from_payload(
            instrument_uid="UID1",
            event_type=MarketDataEventType.TRADES,
            received_at=current,
            payload={},
            sequence=3,
        )
    )
    counters.heartbeat()

    snapshot = counters.snapshots()[0]
    assert snapshot.events_total == 2
    assert snapshot.gaps == 2
    assert snapshot.stale_seconds == 0
    assert snapshot.last_heartbeat_at == current


def test_recorder_reconnects_with_exponential_backoff(tmp_path: Path) -> None:
    event = RawMarketDataEvent.from_payload(
        instrument_uid="UID1",
        event_type=MarketDataEventType.ORDERBOOK,
        received_at=datetime(2026, 7, 6, 12, 0, tzinfo=UTC),
        payload={"ok": True},
    )
    sources: list[MarketDataSource] = [
        FakeMarketDataSource([], fail_before_events=True),
        FakeMarketDataSource([], fail_before_events=True),
        FakeMarketDataSource([event]),
    ]
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    recorder = MarketDataRecorder(
        root=tmp_path / "raw",
        heartbeat_interval_seconds=60,
        initial_backoff_seconds=1,
        max_backoff_seconds=4,
        sleep=fake_sleep,
    )

    result = asyncio.run(
        recorder.run(
            lambda: sources.pop(0),
            [MarketDataSubscription.orderbook("UID1")],
            stop_after_events=1,
            max_reconnects=2,
        )
    )

    assert result.events_recorded == 1
    assert result.reconnects == 2
    assert delays == [1, 2]


def test_build_tbank_market_data_subscription_requests() -> None:
    requests = build_tbank_market_data_requests(
        [
            MarketDataSubscription.orderbook("UID1", depth=20),
            MarketDataSubscription.trades("UID1"),
            MarketDataSubscription.candles(
                "UID1",
                candle_interval="SUBSCRIPTION_INTERVAL_FIVE_MINUTES",
                waiting_close=True,
            ),
        ]
    )

    assert requests == [
        {
            "subscribeOrderBookRequest": {
                "subscriptionAction": "SUBSCRIPTION_ACTION_SUBSCRIBE",
                "instruments": [
                    {
                        "instrumentId": "UID1",
                        "depth": 20,
                        "orderBookType": "ORDERBOOK_TYPE_ALL",
                    }
                ],
            }
        },
        {
            "subscribeTradesRequest": {
                "subscriptionAction": "SUBSCRIPTION_ACTION_SUBSCRIBE",
                "instruments": [
                    {
                        "instrumentId": "UID1",
                        "tradeSource": "TRADE_SOURCE_ALL",
                        "withOpenInterest": False,
                    }
                ],
            }
        },
        {
            "subscribeCandlesRequest": {
                "subscriptionAction": "SUBSCRIPTION_ACTION_SUBSCRIBE",
                "instruments": [
                    {
                        "instrumentId": "UID1",
                        "interval": "SUBSCRIPTION_INTERVAL_FIVE_MINUTES",
                        "waitingClose": True,
                    }
                ],
            }
        },
    ]


def _read_parquet_rows(path: Path) -> list[dict[str, Any]]:
    pq = cast(Any, importlib.import_module("pyarrow.parquet"))
    table = pq.read_table(path)
    return cast(list[dict[str, Any]], table.to_pylist())
