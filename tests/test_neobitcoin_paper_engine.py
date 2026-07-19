from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from neobitcoin_paper.calendar import SessionCalendar
from neobitcoin_paper.datasets import DatasetStore
from neobitcoin_paper.domain import (
    ExecutionModel,
    ExitPolicy,
    MarketEvent,
    OrderBook,
    PaperIntent,
    Side,
    StrategyStatus,
    StrategyVersion,
    deterministic_id,
)
from neobitcoin_paper.engine import PaperTradingEngine
from neobitcoin_paper.execution import PaperExecutionAdapter
from neobitcoin_paper.ingest import CanonicalMarketEvent, DataQualityGate
from neobitcoin_paper.registry import StrategyRegistry
from neobitcoin_paper.state import PaperStateStore
from neobitcoin_paper.strategies import (
    FeatureSnapshot,
    PaperStrategy,
    StrategyContext,
    StrategyDecision,
    frozen_flow_alignment_v1,
)

UID = "4effa274-4e8f-422c-93ff-04aa34fe8e39"
BASE = datetime(2026, 7, 15, 7, 0, tzinfo=UTC)


class StaticFeatures:
    def update(self, event: MarketEvent, book: OrderBook | None) -> FeatureSnapshot:
        return FeatureSnapshot(
            feature_ts=event.exchange_ts,
            source_event_ids=(event.event_id,),
            ready=True,
            initial_side=Side.BUY,
            mid_price=book.mid_price if book is not None else None,
        )

    def reset_continuity(self) -> None:
        return None


@dataclass
class SignalStrategy:
    specification: StrategyVersion
    quantity: Decimal = Decimal("1")
    latency: timedelta = timedelta(milliseconds=100)
    limit_price: Decimal | None = None
    metadata: Mapping[str, object] | None = None

    def evaluate(self, context: StrategyContext) -> StrategyDecision:
        signal = bool(context.event.values.get("signal"))
        intent = None
        if signal:
            intent = PaperIntent.create(
                strategy_id=self.specification.strategy_id,
                strategy_version=self.specification.version,
                instrument_uid=context.event.instrument_uid,
                decision_ts=context.event.processing_ts,
                eligible_ts=context.event.processing_ts + self.latency,
                side=Side.BUY,
                quantity=self.quantity,
                execution_model=ExecutionModel.AGGRESSIVE,
                reason="test-signal",
                confidence=Decimal("1"),
                decision_event_id=context.event.event_id,
                limit_price=self.limit_price,
                exit_policy=ExitPolicy(
                    fixed_stop_ticks=Decimal("5"),
                    trailing_ticks=Decimal("3"),
                    time_exit_seconds=60,
                ),
                metadata=self.metadata,
            )
        evaluation_id = deterministic_id(
            "test-evaluation", self.specification.key, context.event.event_id
        )
        return StrategyDecision(
            evaluation_id=evaluation_id,
            strategy_id=self.specification.strategy_id,
            version=self.specification.version,
            feature_ts=context.event.exchange_ts,
            side_considered=Side.BUY if signal else None,
            signal=signal,
            reason="SIGNAL" if signal else "NO_SIGNAL",
            conditions={"test": signal},
            thresholds={},
            source_event_ids=(context.event.event_id,),
            intent=intent,
        )


@dataclass
class FailingStrategy:
    specification: StrategyVersion

    def evaluate(self, context: StrategyContext) -> StrategyDecision:
        raise RuntimeError("isolated strategy failure")


def spec(name: str) -> StrategyVersion:
    return StrategyVersion(
        strategy_id=name,
        version="v1",
        status=StrategyStatus.FROZEN_PAPER,
        created_at=BASE,
        activated_at=BASE + timedelta(seconds=2),
        parameters={"name": name},
        code_hash=f"code-{name}",
        config_hash=f"config-{name}",
        feature_schema_version="test-v1",
        execution_model_version="paper-v1",
        risk_model_version="risk-v1",
    )


def registry_for(*specifications: StrategyVersion) -> StrategyRegistry:
    registry = StrategyRegistry()
    for specification in specifications:
        registry = registry.register(
            specification,
            registered_at=BASE + timedelta(seconds=1),
            initial_cash=Decimal("10000"),
        )
    return registry


def canonical(
    event_id: str,
    kind: str,
    at: datetime,
    *,
    payload: dict[str, object] | None = None,
    gap: str = "OK",
    consistent: bool | None = None,
    subscription_status: str | None = None,
) -> CanonicalMarketEvent:
    return CanonicalMarketEvent(
        event_id=event_id,
        event_type=kind,  # type: ignore[arg-type]
        instrument_uid=UID,
        exchange_ts=at - timedelta(milliseconds=10),
        receive_ts=at,
        processing_ts=at + timedelta(milliseconds=1),
        revision=0,
        sequence=0,
        source="test",
        latency_ms=10,
        gap_status=gap,
        reconnect_generation=1,
        collector_instance_id="engine-test",
        payload=payload or {},
        subscription_status=subscription_status,
        is_consistent=consistent,
    )


def orderbook(
    event_id: str,
    at: datetime,
    *,
    signal: bool = False,
    ask_quantity: int = 10,
    ask_price: int = 101,
    bid_price: int = 100,
    gap: str = "OK",
) -> CanonicalMarketEvent:
    return canonical(
        event_id,
        "orderbook",
        at,
        payload={
            "bids": [{"price": bid_price, "quantity": 10}],
            "asks": [{"price": ask_price, "quantity": ask_quantity}],
            "signal": signal,
        },
        gap=gap,
        consistent=gap == "OK",
    )


async def make_ready(
    engine: PaperTradingEngine,
    start: datetime = BASE + timedelta(seconds=10),
) -> None:
    await engine.process_event(canonical(f"reconnect-{start.timestamp()}", "reconnect", start))
    for offset, kind in enumerate(
        ("orderbook", "trade", "last_price", "trading_status", "candle"), start=1
    ):
        await engine.process_event(
            canonical(
                f"ack-{kind}-{start.timestamp()}",
                "subscription_ack",
                start + timedelta(milliseconds=offset),
                payload={"subscription_kind": kind},
                subscription_status="SUBSCRIPTION_STATUS_SUCCESS",
            )
        )
    await engine.process_event(
        canonical(
            f"status-{start.timestamp()}",
            "trading_status",
            start + timedelta(milliseconds=10),
            payload={"tradingStatus": "SECURITY_TRADING_STATUS_NORMAL_TRADING"},
        )
    )
    await engine.process_event(orderbook(f"warm-{start.timestamp()}", start + timedelta(seconds=1)))


def build_engine(
    root: Path,
    state: PaperStateStore,
    registry: StrategyRegistry,
    plugins: Sequence[PaperStrategy],
    *,
    decision_latency: timedelta = timedelta(0),
) -> tuple[PaperTradingEngine, DatasetStore]:
    datasets = DatasetStore(root, "2026-07-15", durable_writes=False)
    engine = PaperTradingEngine(
        state_store=state,
        dataset_store=datasets,
        registry=registry,
        plugins=plugins,
        calendar=SessionCalendar(),
        data_quality_gate=DataQualityGate(
            warmup_events=1,
            stale_after_seconds=60,
            max_latency_ms=3000,
            initial_trading_status="NORMAL_TRADING",
        ),
        feature_engine=StaticFeatures(),
        execution_adapter=PaperExecutionAdapter(decision_latency),
        tick_size=Decimal("1"),
    )
    return engine, datasets


def test_time_exit_waits_for_next_causal_book_and_survives_restart(tmp_path: Path) -> None:
    strategy_spec = spec("CAUSAL_EXIT")
    registry = registry_for(strategy_spec)
    database = tmp_path / "state.sqlite"
    data_root = tmp_path / "data"
    with PaperStateStore(database) as state:
        engine, datasets = build_engine(
            data_root,
            state,
            registry,
            [SignalStrategy(strategy_spec)],
            decision_latency=timedelta(milliseconds=100),
        )
        asyncio.run(make_ready(engine))
        signal_at = BASE + timedelta(seconds=20)
        asyncio.run(
            engine.process_event(
                canonical("exit-signal", "trade", signal_at, payload={"signal": True})
            )
        )
        opened = asyncio.run(
            engine.process_event(orderbook("exit-entry", signal_at + timedelta(seconds=1)))
        )
        assert len(opened.opened_position_ids) == 1
        trigger_at = signal_at + timedelta(seconds=82)
        triggered = asyncio.run(
            engine.process_event(orderbook("exit-trigger", trigger_at))
        )
        assert not triggered.closed_position_ids
        assert len(engine.open_positions) == 1
        datasets.abort()

    with PaperStateStore(database) as recovered_state:
        recovered, datasets = build_engine(
            data_root,
            recovered_state,
            registry,
            [SignalStrategy(strategy_spec)],
            decision_latency=timedelta(milliseconds=100),
        )
        closed = asyncio.run(
            recovered.process_event(
                orderbook("exit-eligible", trigger_at + timedelta(milliseconds=200))
            )
        )
        assert len(closed.closed_position_ids) == 1
        assert not recovered.open_positions
        datasets.abort()


def test_gap_signal_is_rejected_and_strategy_failure_is_isolated(tmp_path: Path) -> None:
    good_spec = spec("GOOD")
    bad_spec = spec("BAD")
    registry = registry_for(good_spec, bad_spec)
    with PaperStateStore(tmp_path / "state.sqlite") as state:
        engine, datasets = build_engine(
            tmp_path / "data",
            state,
            registry,
            [SignalStrategy(good_spec), FailingStrategy(bad_spec)],
        )
        asyncio.run(make_ready(engine))
        result = asyncio.run(
            engine.process_event(
                orderbook(
                    "gap-signal",
                    BASE + timedelta(seconds=20),
                    signal=True,
                    gap="ORDERBOOK_GAP",
                )
            )
        )
        assert len(result.rejected_signal_ids) == 1
        assert len(result.strategy_error_ids) == 1
        assert not engine.pending_intents
        assert not engine.open_positions
        paths = datasets.close()
        assert pq.ParquetFile(paths["raw_orderbook_event_windows.parquet"]).metadata.num_rows == 1


def test_signal_first_later_book_full_partial_no_fill_and_account_isolation(
    tmp_path: Path,
) -> None:
    full_spec, partial_spec, no_spec = spec("FULL"), spec("PARTIAL"), spec("NO_FILL")
    registry = registry_for(full_spec, partial_spec, no_spec)
    plugins = [
        SignalStrategy(full_spec, Decimal("1")),
        SignalStrategy(partial_spec, Decimal("3")),
        SignalStrategy(no_spec, Decimal("1"), limit_price=Decimal("100")),
    ]
    with PaperStateStore(tmp_path / "state.sqlite") as state:
        engine, datasets = build_engine(tmp_path / "data", state, registry, plugins)
        asyncio.run(make_ready(engine))
        signal_at = BASE + timedelta(seconds=20)
        signal_result = asyncio.run(
            engine.process_event(canonical("signal", "trade", signal_at, payload={"signal": True}))
        )
        assert len(signal_result.accepted_signal_ids) == 3
        assert len(engine.pending_intents) == 3
        assert not signal_result.order_ids

        fill_result = asyncio.run(
            engine.process_event(
                orderbook(
                    "fill-book",
                    signal_at + timedelta(seconds=1),
                    ask_quantity=2,
                )
            )
        )
        assert len(fill_result.order_ids) == 3
        assert len(fill_result.opened_position_ids) == 2
        assert len(engine.open_positions) == 2
        accounts_with_positions = [a for a in engine.accounts if a.open_position_ids]
        assert len(accounts_with_positions) == 2
        assert len({a.account_id for a in accounts_with_positions}) == 2

        rows = [
            json.loads(line)
            for line in datasets.active_path("paper_orders")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        assert {row["status"] for row in rows} == {"FULL_FILL", "PARTIAL_FILL", "NO_FILL"}
        datasets.abort()


def test_duplicate_and_pending_intent_then_open_position_survive_restart(tmp_path: Path) -> None:
    strategy_spec = spec("RECOVERY")
    registry = registry_for(strategy_spec)
    database = tmp_path / "state.sqlite"
    data_root = tmp_path / "data"
    signal_event = canonical(
        "recoverable-signal",
        "trade",
        BASE + timedelta(seconds=20),
        payload={"signal": True},
    )
    with PaperStateStore(database) as state:
        engine, datasets = build_engine(data_root, state, registry, [SignalStrategy(strategy_spec)])
        asyncio.run(make_ready(engine))
        asyncio.run(engine.process_event(signal_event))
        assert len(engine.pending_intents) == 1
        # Simulate a crash before the broad engine checkpoint.  The accepted
        # intent has its own atomic durable journal and must still recover.
        state.connection.execute(
            "DELETE FROM checkpoints WHERE worker_id = ?",
            ("paper-trading-engine",),
        )
        datasets.abort()

    with PaperStateStore(database) as recovered_state:
        recovered, datasets = build_engine(
            data_root, recovered_state, registry, [SignalStrategy(strategy_spec)]
        )
        assert len(recovered.pending_intents) == 1
        replay = asyncio.run(recovered.process_event(signal_event))
        assert replay.duplicate
        assert len(recovered.pending_intents) == 1
        asyncio.run(make_ready(recovered, BASE + timedelta(seconds=30)))
        assert not recovered.pending_intents
        assert len(recovered.open_positions) == 1
        datasets.abort()

    with PaperStateStore(database) as position_state:
        restored, datasets = build_engine(
            data_root, position_state, registry, [SignalStrategy(strategy_spec)]
        )
        assert len(restored.open_positions) == 1
        assert restored.accounts[0].open_position_ids == (restored.open_positions[0].position_id,)
        datasets.abort()


def test_no_lookahead_and_recovered_last_book_session_finalization(tmp_path: Path) -> None:
    strategy_spec = spec("CAUSAL")
    registry = registry_for(strategy_spec)
    database = tmp_path / "state.sqlite"
    data_root = tmp_path / "data"
    with PaperStateStore(database) as state:
        engine, datasets = build_engine(
            data_root,
            state,
            registry,
            [SignalStrategy(strategy_spec, latency=timedelta(seconds=1))],
        )
        asyncio.run(make_ready(engine))
        signal_at = BASE + timedelta(seconds=20)
        asyncio.run(
            engine.process_event(
                canonical("slow-signal", "trade", signal_at, payload={"signal": True})
            )
        )
        early = asyncio.run(
            engine.process_event(orderbook("too-early", signal_at + timedelta(milliseconds=500)))
        )
        assert not early.order_ids
        assert len(engine.pending_intents) == 1
        eligible = asyncio.run(
            engine.process_event(orderbook("eligible", signal_at + timedelta(seconds=2)))
        )
        assert len(eligible.opened_position_ids) == 1
        assert engine.state_summary()["open_positions"] == 1
        datasets.abort()

    with PaperStateStore(database) as recovered_state:
        recovered, datasets = build_engine(
            data_root,
            recovered_state,
            registry,
            [SignalStrategy(strategy_spec, latency=timedelta(seconds=1))],
        )
        assert recovered.state_summary()["last_book_event_id"] == "eligible"
        closed = recovered.finalize_session(BASE + timedelta(hours=17, minutes=5), "CLOSED")
        assert len(closed) == 1
        assert not recovered.open_positions
        assert recovered.finalize_session(BASE + timedelta(hours=17, minutes=5), "CLOSED") == ()
        datasets.close()


def test_next_book_entry_spread_boundary_and_timeout_are_one_shot(tmp_path: Path) -> None:
    metadata = {
        "max_entry_wait_seconds": 5,
        "max_entry_spread_ticks": 20,
        "signal_reconnect_generation": 1,
    }

    passing_spec = spec("NEXT_BOOK_PASS")
    with PaperStateStore(tmp_path / "pass.sqlite") as state:
        engine, datasets = build_engine(
            tmp_path / "pass-data",
            state,
            registry_for(passing_spec),
            [
                SignalStrategy(
                    passing_spec,
                    latency=timedelta(0),
                    metadata=metadata,
                )
            ],
        )
        asyncio.run(make_ready(engine))
        signal_at = BASE + timedelta(seconds=20)
        signal = asyncio.run(
            engine.process_event(orderbook("next-book-signal", signal_at, signal=True))
        )
        assert not signal.order_ids
        assert len(engine.pending_intents) == 1
        fill = asyncio.run(
            engine.process_event(
                orderbook("spread-20", signal_at + timedelta(seconds=1), ask_price=120)
            )
        )
        assert len(fill.opened_position_ids) == 1
        datasets.abort()

    rejected_spec = spec("NEXT_BOOK_REJECT")
    with PaperStateStore(tmp_path / "reject.sqlite") as state:
        engine, datasets = build_engine(
            tmp_path / "reject-data",
            state,
            registry_for(rejected_spec),
            [
                SignalStrategy(
                    rejected_spec,
                    latency=timedelta(0),
                    metadata=metadata,
                )
            ],
        )
        asyncio.run(make_ready(engine))
        signal_at = BASE + timedelta(seconds=20)
        asyncio.run(engine.process_event(orderbook("spread-signal", signal_at, signal=True)))
        spread_reject = asyncio.run(
            engine.process_event(
                orderbook("spread-21", signal_at + timedelta(seconds=1), ask_price=121)
            )
        )
        assert len(spread_reject.order_ids) == 1
        assert not spread_reject.opened_position_ids
        assert not engine.pending_intents

        false_after_reject = asyncio.run(
            engine.process_event(
                orderbook("no-retry", signal_at + timedelta(seconds=2), ask_price=101)
            )
        )
        assert not false_after_reject.order_ids
        datasets.abort()

    timeout_spec = spec("NEXT_BOOK_TIMEOUT")
    with PaperStateStore(tmp_path / "timeout.sqlite") as state:
        engine, datasets = build_engine(
            tmp_path / "timeout-data",
            state,
            registry_for(timeout_spec),
            [
                SignalStrategy(
                    timeout_spec,
                    latency=timedelta(0),
                    metadata=metadata,
                )
            ],
        )
        asyncio.run(make_ready(engine))
        signal_at = BASE + timedelta(seconds=20)
        asyncio.run(engine.process_event(orderbook("timeout-signal", signal_at, signal=True)))
        timed_out = asyncio.run(
            engine.process_event(orderbook("after-timeout", signal_at + timedelta(seconds=6)))
        )
        assert len(timed_out.order_ids) == 1
        assert not timed_out.opened_position_ids
        assert not engine.pending_intents
        datasets.abort()


def test_flow_shadow_stop_take_and_stress_outputs_are_materialized(
    tmp_path: Path,
) -> None:
    flow_spec = frozen_flow_alignment_v1(
        "MICRO_FLOW_ALIGNMENT",
        created_at=BASE,
        activated_at=BASE + timedelta(seconds=2),
    )
    with PaperStateStore(tmp_path / "shadow.sqlite") as state:
        engine, datasets = build_engine(
            tmp_path / "shadow-data",
            state,
            registry_for(flow_spec),
            [SignalStrategy(flow_spec, latency=timedelta(0))],
        )
        asyncio.run(make_ready(engine))
        signal_at = BASE + timedelta(seconds=20)
        asyncio.run(
            engine.process_event(
                canonical("shadow-signal", "trade", signal_at, payload={"signal": True})
            )
        )
        asyncio.run(
            engine.process_event(orderbook("shadow-entry", signal_at + timedelta(seconds=1)))
        )
        closed = asyncio.run(
            engine.process_event(
                orderbook(
                    "shadow-exit",
                    signal_at + timedelta(seconds=122),
                    bid_price=301,
                    ask_price=302,
                )
            )
        )
        assert len(closed.closed_position_ids) == 1
        stops = [
            json.loads(line)
            for line in datasets.active_path("shadow_stop_results")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        takes = [
            json.loads(line)
            for line in datasets.active_path("shadow_exit_results")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        trades = [
            json.loads(line)
            for line in datasets.active_path("paper_trades")
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        assert len(stops) == 6 and not any(row["triggered"] for row in stops)
        assert len(takes) == 5
        assert [row["exit_model"] for row in takes if row["triggered"]] == ["TAKE_200_TICKS"]
        assert Decimal(trades[0]["stress_slippage_ticks_each_side"]) == Decimal("1")
        assert Decimal(trades[0]["stress_pnl_ticks"]) == (
            Decimal(trades[0]["raw_pnl_ticks"]) - Decimal("2")
        )
        datasets.abort()
