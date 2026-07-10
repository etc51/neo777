from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from neo_trader.neobitcoin_research.config import ResearchConfig
from neo_trader.neobitcoin_research.runtime import ResearchRuntime
from neo_trader.neobitcoin_research.storage import ResearchStorage
from neo_trader.neobitcoin_research.tbank import (
    TBankInstrumentMetadata,
    TBankStreamRecord,
)


class _FakeClient:
    def __init__(self, records: tuple[TBankStreamRecord, ...]) -> None:
        self.records = records
        self.closed = False

    def discover_neobitcoin(self) -> TBankInstrumentMetadata:
        return TBankInstrumentMetadata(
            instrument_uid="uid",
            ticker="BTCUSDperpA",
            class_code="SPBDMFUT",
            figi="BTCUSDPERP00",
            position_uid=None,
            name="Neo Bitcoin",
            instrument_type="futures",
            exchange="SPB",
            currency="rub",
            lot=1,
            min_price_increment=Decimal("0.1"),
            api_trade_available=True,
            buy_available=True,
            sell_available=True,
            trading_status="NORMAL_TRADING",
            limit_order_available=True,
            market_order_available=True,
            trading_schedules=(),
        )

    async def stream_market_data(self, _instrument: object):
        for record in self.records:
            yield record

    def close(self) -> None:
        self.closed = True


def _orderbook_record(now: datetime) -> TBankStreamRecord:
    bids = [
        {"price": str(Decimal("100") - Decimal(index) / 10), "quantity": 100 + index}
        for index in range(20)
    ]
    asks = [
        {"price": str(Decimal("100.1") + Decimal(index) / 10), "quantity": 90 + index}
        for index in range(20)
    ]
    return TBankStreamRecord(
        event_type="orderbook",
        subscription_kind="orderbook",
        received_at=now,
        received_monotonic_ns=123,
        exchange_timestamp=now,
        instrument_uid="uid",
        ticker="BTCUSDperpA",
        class_code="SPBDMFUT",
        stream_id="stream",
        subscription_id="subscription",
        subscription_status="SUBSCRIPTION_STATUS_SUCCESS",
        is_consistent=True,
        payload={"orderbook": {"depth": 20, "is_consistent": True, "bids": bids, "asks": asks}},
    )


def test_runtime_writes_raw_features_all_execution_models_and_report(tmp_path: Path) -> None:
    now = datetime(2026, 7, 10, 10, tzinfo=UTC)
    client = _FakeClient((_orderbook_record(now),))
    config = ResearchConfig(
        data_root=tmp_path / "data",
        reports_root=tmp_path / "reports",
        requested_depth=20,
        minimum_usable_depth=20,
        position_sizes_rub=(10_000,),
        latencies_ms=(0,),
        horizons_seconds=(5,),
        wal_fsync=True,
    )
    storage = ResearchStorage(
        config.data_root,
        state_db_path=config.data_root / "state.sqlite",
        fsync=True,
    )
    runtime = ResearchRuntime(config=config, client=client, storage=storage)  # type: ignore[arg-type]
    asyncio.run(runtime.run(max_events=1))

    assert runtime.events_seen == 1
    assert runtime.valid_books == 1
    assert runtime.depth_observed == 20
    assert client.closed
    assert list(storage.iter_raw_events())
    assert list(storage.iter_derived("feature_snapshots"))
    executions = list(storage.iter_derived("execution_simulations"))
    models = {str(record["model"]) for record in executions}
    assert models == {
        "IDEAL_TOP_OF_BOOK",
        "AGGRESSIVE_SWEEP",
        "PASSIVE_OPTIMISTIC",
        "PASSIVE_BASE",
        "PASSIVE_PESSIMISTIC",
    }
    assert list(storage.iter_derived("shadow_predictions"))
    assert list(config.data_root.glob("parquet/**/*.parquet"))
    assert (config.data_root / "research.duckdb").is_file()
    assert list(config.reports_root.glob("*.json"))
    assert list(config.reports_root.glob("*.md"))
    storage.close()
