from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from neo_trader.neobitcoin_research.safety import (
    TBankRpcBlockedError,
    require_allowed_rpc_path,
)
from neo_trader.neobitcoin_research.tbank import TBankStreamRecord
from neobitcoin_paper.calendar import SessionCalendar
from neobitcoin_paper.config import PaperConfig
from neobitcoin_paper.delivery import (
    ArtifactDescriptor,
    CodexSameThreadTransport,
    DeliveryError,
)
from neobitcoin_paper.domain import BookLevel, DataQuality, MarketEvent, OrderBook
from neobitcoin_paper.ingest import CanonicalMarketEvent, DataQualityGate, canonicalize_record
from neobitcoin_paper.observability import HealthRegistry, HealthServer, JsonLogFormatter
from neobitcoin_paper.safety import (
    PAPER_ONLY,
    PaperOnlyViolation,
    require_paper_only,
    scan_runtime_sources,
)
from neobitcoin_paper.strategies import (
    CounterflowFeatureEngine,
    StrategyContext,
    StrongCounterflowAbsorptionStrategy,
    frozen_counterflow_v1,
)


def test_paper_only_is_mandatory_and_not_configurable(tmp_path: Path) -> None:
    assert PAPER_ONLY is True
    with pytest.raises(PaperOnlyViolation):
        require_paper_only({})
    with pytest.raises(PaperOnlyViolation):
        require_paper_only({"PAPER_ONLY": "false"})
    with pytest.raises(PaperOnlyViolation):
        require_paper_only({"PAPER_ONLY": "true", "REAL_ORDERS_ENABLED": "true"})
    config = PaperConfig.from_env(
        {
            "PAPER_ONLY": "true",
            "NEOBITCOIN_PAPER_DATA": str(tmp_path),
            "NEOBITCOIN_PAPER_TOKEN_FILE": str(tmp_path / "secret"),
        }
    )
    config.ensure_directories()
    assert config.public_snapshot()["paper_only"] is True
    assert (tmp_path / "daily_archives").is_dir()


def test_runtime_source_has_no_broker_execution_symbols() -> None:
    package = Path(__file__).parents[1] / "neobitcoin_paper"
    assert scan_runtime_sources(package) == ()


def test_real_order_rpc_is_blocked_locally_even_when_live_flags_are_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    monkeypatch.setenv("REAL_ORDERS_ENABLED", "true")
    service = "Orders" + "Service"
    method = "Post" + "Order"
    forbidden_path = f"/tinkoff.public.invest.api.contract.v1.{service}/{method}"
    network_called = False

    def guarded_transport() -> None:
        nonlocal network_called
        require_allowed_rpc_path(forbidden_path)
        network_called = True

    with pytest.raises(TBankRpcBlockedError):
        guarded_transport()

    assert network_called is False


def _canonical_event(
    event_type: str,
    *,
    payload: dict[str, object] | None = None,
    status: str | None = None,
    consistent: bool | None = None,
    latency_ms: float = 1.0,
) -> CanonicalMarketEvent:
    now = datetime.now(UTC)
    return CanonicalMarketEvent(
        event_id=hashlib.sha256(f"{event_type}:{now.isoformat()}".encode()).hexdigest(),
        event_type=event_type,  # type: ignore[arg-type]
        instrument_uid="4effa274-4e8f-422c-93ff-04aa34fe8e39",
        exchange_ts=now - timedelta(milliseconds=latency_ms),
        receive_ts=now,
        processing_ts=now,
        revision=0,
        sequence=0,
        source="test",
        latency_ms=latency_ms,
        gap_status="OK",
        reconnect_generation=1,
        collector_instance_id="test-instance",
        payload=payload or {},
        subscription_status=status,
        is_consistent=consistent,
    )


def test_data_quality_reconnect_requires_acks_and_warmup() -> None:
    gate = DataQualityGate(warmup_events=2, stale_after_seconds=5, max_latency_ms=3_000)
    gate.on_connect()
    for kind in ("orderbook", "trade", "last_price", "trading_status", "candle"):
        gate.observe(
            _canonical_event(
                "subscription_ack",
                payload={"subscription_kind": kind},
                status="SUBSCRIPTION_STATUS_SUCCESS",
            )
        )
    gate.observe(
        _canonical_event(
            "trading_status",
            payload={"tradingStatus": "SECURITY_TRADING_STATUS_NORMAL_TRADING"},
        )
    )
    book = {"bids": [{"price": 100, "quantity": 5}], "asks": [{"price": 101, "quantity": 5}]}
    first = gate.observe(_canonical_event("orderbook", payload=book, consistent=True))
    assert first.entry_allowed is False
    second = gate.observe(_canonical_event("orderbook", payload=book, consistent=True))
    assert second.entry_allowed is True
    gate.on_disconnect()
    assert gate.snapshot().entry_allowed is False


def test_streaming_candle_uses_transport_latency_not_interval_open_age() -> None:
    received = datetime.now(UTC)
    record = TBankStreamRecord(
        event_type="candle",
        subscription_kind=None,
        received_at=received,
        received_monotonic_ns=1,
        exchange_timestamp=received - timedelta(minutes=15),
        instrument_uid="4effa274-4e8f-422c-93ff-04aa34fe8e39",
        ticker="BTCUSDperpA",
        class_code="SPBDMFUT",
        stream_id="stream-test",
        subscription_id="subscription-test",
        subscription_status=None,
        is_consistent=True,
        payload={"interval": "SUBSCRIPTION_INTERVAL_FIFTEEN_MINUTES"},
    )

    event = canonicalize_record(
        record,
        reconnect_generation=1,
        instance_id="test-instance",
        expected_uid="4effa274-4e8f-422c-93ff-04aa34fe8e39",
    )

    assert event.exchange_ts == record.exchange_timestamp
    assert 0 <= event.latency_ms < 3_000


def test_data_quality_numeric_normal_trading_status_is_open_fail_closed() -> None:
    gate = DataQualityGate(warmup_events=1, stale_after_seconds=5, max_latency_ms=3_000)
    gate.on_connect()
    for kind in ("orderbook", "trade", "last_price", "trading_status", "candle"):
        gate.observe(
            _canonical_event(
                "subscription_ack",
                payload={"subscription_kind": kind},
                status="SUBSCRIPTION_STATUS_SUCCESS",
            )
        )
    gate.observe(_canonical_event("trading_status", payload={"tradingStatus": 5}))
    ready = gate.observe(
        _canonical_event(
            "orderbook",
            payload={
                "bids": [{"price": 100, "quantity": 5}],
                "asks": [{"price": 101, "quantity": 5}],
            },
            consistent=True,
        )
    )

    assert ready.trading_status == "NORMAL_TRADING"
    assert ready.entry_allowed is True

    gate.observe(_canonical_event("trading_status", payload={"tradingStatus": 999}))
    assert gate.snapshot().trading_status == "UNKNOWN"
    assert gate.snapshot().entry_allowed is False


def test_closed_market_is_not_mislabeled_as_orderbook_gap() -> None:
    gate = DataQualityGate(warmup_events=1, stale_after_seconds=5, max_latency_ms=3_000)
    gate.on_connect()
    for kind in ("orderbook", "trade", "last_price", "trading_status", "candle"):
        gate.observe(
            _canonical_event(
                "subscription_ack",
                payload={"subscription_kind": kind},
                status="SUBSCRIPTION_STATUS_SUCCESS",
            )
        )
    closed = gate.observe(
        _canonical_event(
            "trading_status",
            payload={"tradingStatus": "SECURITY_TRADING_STATUS_NOT_AVAILABLE_FOR_TRADING"},
        )
    )
    assert closed.subscription_state == "CLOSED_MARKET"
    assert closed.reason == "CLOSED_MARKET"
    assert closed.gap_active is False
    assert closed.entry_allowed is False


class _FakeChannel:
    last: _FakeChannel | None = None

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False
        _FakeChannel.last = self

    def request(self, method: str, params: dict[str, object]) -> dict[str, object]:
        self.calls.append((method, params))
        if method == "thread/resume":
            return {"thread": {"id": params["threadId"]}}
        if method == "turn/start":
            return {"turn": {"id": "turn-test"}}
        return {}

    def notify(self, method: str, params: dict[str, object]) -> None:
        self.calls.append((method, params))

    def wait_notification(self, method: str, timeout_seconds: float) -> dict[str, object]:
        assert method == "turn/completed"
        assert timeout_seconds > 0
        return {
            "method": method,
            "params": {"turn": {"id": "turn-test", "status": "completed"}},
        }

    def close(self) -> None:
        self.closed = True


def test_delivery_resumes_same_thread_and_verifies_artifact(tmp_path: Path) -> None:
    archive = tmp_path / "archive.tar.zst"
    archive.write_bytes(b"verified-test-archive")
    sha = hashlib.sha256(archive.read_bytes()).hexdigest()
    artifact = ArtifactDescriptor(
        archive_id="TEST-2026-07-15",
        session_date="2026-07-15",
        sha256=sha,
        size_bytes=archive.stat().st_size,
        strategies=1,
        signals=0,
        paper_trades=0,
        pnl_summary="zero",
        restart_gap_summary="none",
        local_path=archive,
    )
    transport = CodexSameThreadTransport(channel_factory=_FakeChannel)  # type: ignore[arg-type]
    ack = transport.deliver(thread_id="thread-existing", artifact=artifact)
    assert ack.thread_id == "thread-existing"
    assert ack.turn_id == "turn-test"
    assert _FakeChannel.last is not None and _FakeChannel.last.closed
    methods = [name for name, _ in _FakeChannel.last.calls]
    assert methods == ["initialize", "initialized", "thread/resume", "turn/start"]

    archive.write_bytes(b"tampered")
    with pytest.raises(DeliveryError):
        artifact.verify_accessible()


def test_local_health_endpoints_and_secret_redaction() -> None:
    registry = HealthRegistry()
    registry.heartbeat("market_stream", healthy=True, ready=True)
    registry.heartbeat("state_store", healthy=True, ready=True)
    server = HealthServer(registry, "127.0.0.1", 0)
    server.start()
    try:
        host, port = server.address
        with urllib.request.urlopen(f"http://{host}:{port}/healthz", timeout=2) as response:
            payload = json.loads(response.read())
        assert payload["status"] == "ok"
        with urllib.request.urlopen(f"http://{host}:{port}/metrics", timeout=2) as response:
            assert b"neobitcoin_paper_health 1" in response.read()
    finally:
        server.close()

    formatter = JsonLogFormatter()
    import logging

    record = logging.LogRecord(
        "test",
        logging.INFO,
        __file__,
        1,
        "authorization=%s",
        ("t." + "A" * 30,),
        None,
    )
    rendered = formatter.format(record)
    assert "A" * 10 not in rendered
    assert "***" in rendered


def test_strategy_sandbox_emits_only_intent_after_point_in_time_features() -> None:
    now = datetime(2026, 7, 15, 9, 0, tzinfo=UTC)
    spec = frozen_counterflow_v1(created_at=now - timedelta(minutes=2), activated_at=now)
    strategy = StrongCounterflowAbsorptionStrategy(spec, latency_ms=100)
    features = CounterflowFeatureEngine(tick_size=Decimal("0.1"))
    book = OrderBook(
        event_id="book-1",
        instrument_uid="4effa274-4e8f-422c-93ff-04aa34fe8e39",
        exchange_ts=now + timedelta(seconds=2),
        receive_ts=now + timedelta(seconds=2, milliseconds=10),
        processing_ts=now + timedelta(seconds=2, milliseconds=20),
        bids=(BookLevel(Decimal("100"), Decimal("100")),),
        asks=(BookLevel(Decimal("100.1"), Decimal("10")),),
        trading_status="NORMAL_TRADING",
    )
    events = (
        MarketEvent(
            "trade-a",
            book.instrument_uid,
            "trade",
            now,
            now,
            now,
            values={"direction": "BUY", "quantity": 1000, "price": 100, "latency_ms": 1},
        ),
        MarketEvent(
            "trade-b",
            book.instrument_uid,
            "trade",
            now + timedelta(seconds=1),
            now + timedelta(seconds=1),
            now + timedelta(seconds=1),
            values={"direction": "SELL", "quantity": 250, "price": 100, "latency_ms": 1},
        ),
        MarketEvent(
            "book-event",
            book.instrument_uid,
            "orderbook",
            now + timedelta(seconds=2),
            now + timedelta(seconds=2, milliseconds=10),
            now + timedelta(seconds=2, milliseconds=20),
            trading_status="NORMAL_TRADING",
            values={"latency_ms": 10},
        ),
    )
    snapshot = None
    for event in events:
        snapshot = features.update(event, book if event.event_type == "orderbook" else None)
    assert snapshot is not None and snapshot.ready
    session = SessionCalendar().resolve(events[-1].processing_ts, "NORMAL_TRADING")
    decision = strategy.evaluate(
        StrategyContext(events[-1], book, snapshot, session, DataQuality.GOOD)
    )
    assert decision.signal is True
    assert decision.intent is not None
    assert decision.intent.eligible_ts > decision.intent.decision_ts
    assert not hasattr(strategy, "client")
    forbidden = "post" + "order"
    with pytest.raises(AttributeError):
        getattr(strategy, forbidden)


def test_flow_snapshot_matches_schema_v41_formulas_and_resets_continuity() -> None:
    now = datetime(2026, 7, 15, 9, 0, tzinfo=UTC)
    uid = "4effa274-4e8f-422c-93ff-04aa34fe8e39"
    features = CounterflowFeatureEngine(tick_size=Decimal("0.1"))
    book = OrderBook(
        event_id="flow-book",
        instrument_uid=uid,
        exchange_ts=now + timedelta(seconds=2),
        receive_ts=now + timedelta(seconds=2, milliseconds=10),
        processing_ts=now + timedelta(seconds=2, milliseconds=20),
        bids=(
            BookLevel(Decimal("100"), Decimal("30")),
            BookLevel(Decimal("99.9"), Decimal("20")),
        ),
        asks=(
            BookLevel(Decimal("100.2"), Decimal("10")),
            BookLevel(Decimal("100.3"), Decimal("40")),
        ),
        trading_status="NORMAL_TRADING",
    )
    for event_id, offset, direction, quantity in (
        ("flow-left-boundary", -3, "BUY", 100),
        ("flow-buy", 0, "BUY", 3),
        ("flow-sell", 1, "SELL", 1),
        ("flow-unknown", 1, "UNKNOWN", 2),
        ("flow-future", 3, "BUY", 100),
    ):
        at = now + timedelta(seconds=offset)
        features.update(
            MarketEvent(
                event_id,
                uid,
                "trade",
                at,
                at,
                at,
                values={"direction": direction, "quantity": quantity, "price": 100},
            ),
            None,
        )
    features.update(
        MarketEvent(
            "flow-late-received",
            uid,
            "trade",
            now + timedelta(seconds=1),
            now + timedelta(seconds=4),
            now + timedelta(seconds=4),
            values={"direction": "BUY", "quantity": 100, "price": 100},
        ),
        None,
    )
    book_event = MarketEvent(
        "flow-book-event",
        uid,
        "orderbook",
        book.exchange_ts,
        book.receive_ts,
        book.processing_ts,
    )
    snapshot = features.update(book_event, book)

    assert snapshot.best_bid == Decimal("100")
    assert snapshot.best_ask == Decimal("100.2")
    assert snapshot.bid_qty_1 == Decimal("30")
    assert snapshot.ask_qty_1 == Decimal("10")
    assert snapshot.mid_price == Decimal("100.1")
    assert snapshot.spread == Decimal("0.2")
    assert snapshot.bid_depth_l5 == snapshot.ask_depth_l5 == Decimal("50")
    assert snapshot.imbalance_l5 == Decimal("0")
    assert snapshot.microprice == Decimal("100.15")
    assert snapshot.microprice_offset == Decimal("0.5")
    assert snapshot.spread_ticks == 2
    assert snapshot.alignment_ready
    assert not snapshot.l5_ready
    flow_5s = snapshot.flow_window(5)
    assert flow_5s is not None
    assert flow_5s.trade_count == 3
    assert flow_5s.known_trade_count == 2
    assert flow_5s.unknown_side_trade_count == 1
    assert flow_5s.buy_volume == Decimal("3")
    assert flow_5s.sell_volume == Decimal("1")
    assert flow_5s.unknown_side_volume == Decimal("2")
    assert flow_5s.trade_flow == Decimal("2")
    assert flow_5s.trade_flow_ratio == Decimal("0.5")
    assert flow_5s.first_event_id == "flow-buy"
    assert flow_5s.last_event_id == "flow-unknown"

    features.reset_continuity()
    reset_snapshot = features.update(book_event, book)
    reset_flow = reset_snapshot.flow_window(5)
    assert reset_flow is not None and reset_flow.trade_count == 0
