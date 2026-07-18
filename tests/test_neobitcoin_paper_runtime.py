from __future__ import annotations

import asyncio
import hashlib
import sqlite3
from collections.abc import AsyncIterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from neobitcoin_paper.config import PaperConfig
from neobitcoin_paper.ingest import CanonicalMarketEvent, InstrumentSnapshot
from neobitcoin_paper.runtime import (
    DeliveryWorker,
    DiskGuard,
    DiskLevel,
    PaperRuntime,
    SystemdNotifier,
    build_daily_archive,
)
from neobitcoin_paper.state import PaperStateStore
from neobitcoin_paper.strategies import frozen_counterflow_v1

UID = "4effa274-4e8f-422c-93ff-04aa34fe8e39"


def _config(root: Path) -> PaperConfig:
    return PaperConfig(
        data_root=root,
        token_file=root / "credentials" / "market-data-token",
        thread_id_file=root / "credentials" / "task-id",
        instrument_uid=UID,
        warmup_events=1,
        disk_warning_free_bytes=300,
        disk_critical_free_bytes=200,
        disk_emergency_free_bytes=100,
    )


def _event(
    event_id: str = "runtime-event", observed_at: datetime | None = None
) -> CanonicalMarketEvent:
    now = observed_at or datetime.now(UTC)
    return CanonicalMarketEvent(
        event_id=event_id,
        event_type="reconnect",
        instrument_uid=UID,
        exchange_ts=now,
        receive_ts=now,
        processing_ts=now,
        revision=0,
        sequence=0,
        source="test-market-data",
        latency_ms=0.0,
        gap_status="ORDERBOOK_GAP",
        reconnect_generation=1,
        collector_instance_id="test-runtime",
        payload={"reason": "test"},
    )


def _closed_status_event(event_id: str, observed_at: datetime) -> CanonicalMarketEvent:
    return CanonicalMarketEvent(
        event_id=event_id,
        event_type="trading_status",
        instrument_uid=UID,
        exchange_ts=observed_at,
        receive_ts=observed_at,
        processing_ts=observed_at,
        revision=0,
        sequence=0,
        source="test-market-data",
        latency_ms=0.0,
        gap_status="OK",
        reconnect_generation=1,
        collector_instance_id="test-runtime",
        payload={
            "trading_status": "SECURITY_TRADING_STATUS_NOT_AVAILABLE_FOR_TRADING"
        },
    )


def _break_status_event(event_id: str, observed_at: datetime) -> CanonicalMarketEvent:
    return CanonicalMarketEvent(
        event_id=event_id,
        event_type="trading_status",
        instrument_uid=UID,
        exchange_ts=observed_at,
        receive_ts=observed_at,
        processing_ts=observed_at,
        revision=0,
        sequence=0,
        source="test-market-data",
        latency_ms=0.0,
        gap_status="OK",
        reconnect_generation=1,
        collector_instance_id="test-runtime",
        payload={"trading_status": "SECURITY_TRADING_STATUS_BREAK_IN_TRADING"},
    )


class _FiniteMarketData:
    def __init__(self, events: tuple[CanonicalMarketEvent, ...]) -> None:
        self.events = events
        self.discoveries = 0

    def discover(self) -> InstrumentSnapshot:
        self.discoveries += 1
        return InstrumentSnapshot(
            checked_at=datetime.now(UTC),
            name="Neo Bitcoin",
            ticker="BTCUSDperpA",
            instrument_uid=UID,
            class_code="SPBDMFUT",
            instrument_type="futures",
            exchange="spb_future",
            lot=1,
            min_price_increment="0.1",
            api_market_data_available=True,
            trading_status="SECURITY_TRADING_STATUS_NORMAL_TRADING",
        )

    async def stream(
        self, stop: asyncio.Event
    ) -> AsyncIterator[CanonicalMarketEvent]:
        for event in self.events:
            if stop.is_set():
                return
            yield event


class _HealthServer:
    def __init__(self) -> None:
        self.started = False
        self.closed = False

    def start(self) -> None:
        self.started = True

    def close(self) -> None:
        self.closed = True


class _Notifier:
    def __init__(self) -> None:
        self.messages: list[str] = []

    def notify(self, message: str) -> bool:
        self.messages.append(message)
        return True


def test_runtime_is_restart_safe_idempotent_and_notifies_only_when_healthy(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path / "paper")
    market = _FiniteMarketData((_event(), _event()))
    notifier = _Notifier()
    servers: list[_HealthServer] = []

    def server_factory(_registry: object, host: str, _port: int) -> _HealthServer:
        assert host == "127.0.0.1"
        server = _HealthServer()
        servers.append(server)
        return server

    for _ in range(2):
        runtime = PaperRuntime(
            config,
            environ={"PAPER_ONLY": "true"},
            market_data=market,
            health_server_factory=server_factory,
            notifier=notifier,
            disk_guard=DiskGuard(config, usage=lambda _path: SimpleNamespace(free=1_000)),
        )
        asyncio.run(runtime.run())

    database = config.data_root / "state" / "paper_state.sqlite3"
    with sqlite3.connect(database) as connection:
        generations = int(
            connection.execute("SELECT count(*) FROM restart_generation").fetchone()[0]
        )
        events = int(
            connection.execute(
                "SELECT count(*) FROM idempotency_keys WHERE scope = 'canonical-market-event'"
            ).fetchone()[0]
        )
        strategies = int(
            connection.execute(
                "SELECT count(*) FROM strategy_registry WHERE enabled = 1"
            ).fetchone()[0]
        )
        accounts = int(connection.execute("SELECT count(*) FROM virtual_accounts").fetchone()[0])
        lifecycle = connection.execute(
            """
            SELECT strategy_id, lifecycle_status, evaluation_cohort, enabled
            FROM strategy_registry ORDER BY strategy_id
            """
        ).fetchall()
        schema_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    assert generations == 2
    assert events == 1
    assert strategies == 1
    assert accounts == 2
    assert schema_version == 3
    assert lifecycle == [
        ("L5_FLOW_ALIGNMENT", "PAUSE_NEW_ENTRIES_OOS_FAILURE", "LIVE_OOS", 0),
        (
            "MICRO_FLOW_ALIGNMENT",
            "FROZEN_PAPER_DEGRADED_CONTINUE_OOS",
            "LIVE_OOS",
            1,
        ),
    ]
    assert market.discoveries == 2
    assert all(server.started and server.closed for server in servers)
    # systemd startup readiness is distinct from functional /readyz state.
    assert notifier.messages.count("READY=1") == 2
    assert notifier.messages.count("WATCHDOG=1") == 4
    assert notifier.messages.count("STOPPING=1") == 2
    snapshot = (config.data_root / "state" / "instrument_snapshot.json").read_text(
        encoding="utf-8"
    )
    assert UID in snapshot
    assert "market-data-token" not in snapshot


def test_existing_strong_counterflow_is_preserved_but_forcibly_disabled(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 16, 9, 0, tzinfo=UTC)
    config = _config(tmp_path / "paper")
    config.ensure_directories()
    specification = frozen_counterflow_v1(
        created_at=now - timedelta(minutes=1),
        activated_at=now + timedelta(seconds=10),
    )
    with PaperStateStore(
        config.data_root / "state" / "paper_state.sqlite3",
        clock=lambda: now,
    ) as state:
        state.register_strategy(
            specification.strategy_id,
            specification.version,
            config=dict(specification.parameters),
            code_hash=specification.code_hash,
            activated_at=specification.activated_at or now,
            enabled=True,
        )
        runtime = PaperRuntime(
            config,
            environ={"PAPER_ONLY": "true"},
            market_data=_FiniteMarketData((_event(),)),
            clock=lambda: now,
        )
        installed = runtime._strategy_specifications(state)
        by_id = {item.strategy_id: (item, enabled) for item, _, enabled in installed}
        strong, strong_enabled = by_id["STRONG_COUNTERFLOW_ABSORPTION"]
        assert strong.status.value == "REJECTED_OOS_AS_FORMALIZED"
        assert strong_enabled is False
        assert by_id["MICRO_FLOW_ALIGNMENT"][1] is True
        assert by_id["L5_FLOW_ALIGNMENT"][1] is False
        row = state.connection.execute(
            """
            SELECT enabled FROM strategy_registry
            WHERE strategy_id = 'STRONG_COUNTERFLOW_ABSORPTION'
            """
        ).fetchone()
        assert row is not None and int(row["enabled"]) == 0


def test_disk_guard_pauses_signals_before_emergency(tmp_path: Path) -> None:
    config = _config(tmp_path)
    warning = DiskGuard(config, usage=lambda _path: SimpleNamespace(free=250)).check()
    critical = DiskGuard(config, usage=lambda _path: SimpleNamespace(free=150)).check()
    emergency = DiskGuard(config, usage=lambda _path: SimpleNamespace(free=50)).check()

    assert warning.level is DiskLevel.WARNING and warning.signals_allowed
    assert critical.level is DiskLevel.CRITICAL and not critical.signals_allowed
    assert emergency.level is DiskLevel.EMERGENCY and not emergency.signals_allowed


def test_live_closed_status_plus_grace_does_not_finalize_before_scheduled_close(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path / "paper")
    closed_at = datetime(2026, 7, 18, 6, 50, tzinfo=UTC)
    market = _FiniteMarketData(
        (
            _closed_status_event("closed-start", closed_at),
            _closed_status_event("closed-grace", closed_at + timedelta(seconds=301)),
        )
    )
    runtime = PaperRuntime(
        config,
        environ={"PAPER_ONLY": "true"},
        market_data=market,
        health_server_factory=lambda *_args: _HealthServer(),
        notifier=_Notifier(),
        disk_guard=DiskGuard(config, usage=lambda _path: SimpleNamespace(free=1_000)),
        clock=lambda: closed_at - timedelta(minutes=1),
    )
    asyncio.run(runtime.run())

    with sqlite3.connect(config.data_root / "state" / "paper_state.sqlite3") as connection:
        row = connection.execute(
            "SELECT status, active, trading_status FROM session_state "
            "WHERE session_date = '2026-07-18'"
        ).fetchone()
    assert row is not None
    assert row[0] == "RUNNING" and row[1] == 1
    assert row[2] == "NOT_AVAILABLE_FOR_TRADING"
    assert list((config.data_root / "active" / "2026-07-18").glob("*.inprogress"))


def test_scheduled_close_plus_grace_finalizes_break_status_for_archive(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path / "paper")
    finalizer_at = datetime(2026, 7, 15, 21, 5, tzinfo=UTC)
    market = _FiniteMarketData(
        (_break_status_event("scheduled-close-heartbeat", finalizer_at),)
    )
    runtime = PaperRuntime(
        config,
        environ={"PAPER_ONLY": "true"},
        market_data=market,
        health_server_factory=lambda *_args: _HealthServer(),
        notifier=_Notifier(),
        disk_guard=DiskGuard(config, usage=lambda _path: SimpleNamespace(free=1_000)),
        clock=lambda: finalizer_at - timedelta(minutes=1),
    )
    asyncio.run(runtime.run())

    with sqlite3.connect(config.data_root / "state" / "paper_state.sqlite3") as connection:
        row = connection.execute(
            "SELECT status, active, trading_status FROM session_state "
            "WHERE session_date = '2026-07-15'"
        ).fetchone()
    assert row == ("FINALIZED", 0, "BREAK_IN_TRADING")


def test_restart_after_preopen_finalizes_prior_durable_session(tmp_path: Path) -> None:
    config = _config(tmp_path / "paper")
    config.ensure_directories()
    state = PaperStateStore(config.data_root / "state" / "paper_state.sqlite3")
    state.set_session_state(
        date(2026, 7, 14),
        status="RUNNING",
        state={"open_positions": 0},
        active=True,
        trading_status="UNKNOWN",
    )
    state.close()

    started_at = datetime(2026, 7, 15, 4, 0, tzinfo=UTC)
    runtime = PaperRuntime(
        config,
        environ={"PAPER_ONLY": "true"},
        market_data=_FiniteMarketData((_event("post-downtime", started_at),)),
        health_server_factory=lambda *_args: _HealthServer(),
        notifier=_Notifier(),
        disk_guard=DiskGuard(config, usage=lambda _path: SimpleNamespace(free=1_000)),
        clock=lambda: started_at,
    )
    asyncio.run(runtime.run())

    with sqlite3.connect(config.data_root / "state" / "paper_state.sqlite3") as connection:
        old = connection.execute(
            "SELECT status, active, trading_status FROM session_state "
            "WHERE session_date = '2026-07-14'"
        ).fetchone()
        current = connection.execute(
            "SELECT status, active FROM session_state WHERE session_date = '2026-07-15'"
        ).fetchone()
    assert old == ("FINALIZED", 0, "UNKNOWN")
    assert current is not None and current[1] == 1


def test_real_archive_refuses_an_active_or_unfinalized_session(tmp_path: Path) -> None:
    config = _config(tmp_path / "paper")
    config.ensure_directories()
    state = PaperStateStore(config.data_root / "state" / "paper_state.sqlite3")
    try:
        with pytest.raises(RuntimeError, match="not finalized"):
            build_daily_archive(config, state, date(2026, 7, 15))
    finally:
        state.close()


def test_systemd_notifier_supports_abstract_watchdog_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[object] = []

    class Socket:
        def __enter__(self) -> Socket:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def connect(self, address: object) -> None:
            calls.append(address)

        def sendall(self, payload: bytes) -> None:
            calls.append(payload)

    monkeypatch.setattr("neobitcoin_paper.runtime.socket.socket", lambda *_args: Socket())
    assert SystemdNotifier("@paper-watchdog").notify("WATCHDOG=1")
    assert calls == [b"\0paper-watchdog", b"WATCHDOG=1"]


def test_zero_trade_test_archive_is_validated_and_recorded(tmp_path: Path) -> None:
    config = _config(tmp_path / "paper")
    config.ensure_directories()
    state = PaperStateStore(config.data_root / "state" / "paper_state.sqlite3")
    try:
        built = build_daily_archive(
            config,
            state,
            date(2026, 7, 15),
            test_archive=True,
        )
        row = state.connection.execute(
            "SELECT status, validation_json FROM archives WHERE archive_id = ?",
            (built.archive_id,),
        ).fetchone()
    finally:
        state.close()

    assert built.archive_id.startswith("TEST-")
    assert built.manifest["archive_type"] == "TEST"
    assert built.validation.passed
    assert row is not None and row["status"] == "VALIDATED"
    assert '"oos_included":false' in str(row["validation_json"])


def test_delivery_outbox_retries_without_affecting_archive_and_is_idempotent(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path / "paper")
    config.ensure_directories()
    artifact = config.data_root / "daily_archives" / "unit.tar.zst"
    artifact.write_bytes(b"validated-paper-archive")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    state = PaperStateStore(config.data_root / "state" / "paper_state.sqlite3")
    state.record_archive(
        "DAILY-2026-07-15-000000000000",
        session_date="2026-07-15",
        path=artifact,
        sha256=digest,
        status="VALIDATED",
        validation={"test_archive": False},
        validated_at=datetime.now(UTC),
    )

    class FlakyTransport:
        def __init__(self) -> None:
            self.calls = 0

        def deliver(self, **_kwargs: object) -> SimpleNamespace:
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("simulated transport failure")
            return SimpleNamespace(turn_id="turn-unit")

    transport = FlakyTransport()
    worker = DeliveryWorker(state, transport=transport, maximum_retry_seconds=30)
    delivery_id = worker.enqueue("DAILY-2026-07-15-000000000000", "task-unit")
    assert worker.enqueue("DAILY-2026-07-15-000000000000", "task-unit") == delivery_id
    now = datetime.now(UTC) + timedelta(seconds=1)
    first = worker.run_due(now=now)
    second = worker.run_due(now=now + timedelta(seconds=10))
    row = state.connection.execute(
        "SELECT status, attempt_count FROM delivery_outbox WHERE delivery_id = ?",
        (delivery_id,),
    ).fetchone()
    archive_status = state.connection.execute(
        "SELECT status FROM archives WHERE archive_id = 'DAILY-2026-07-15-000000000000'"
    ).fetchone()[0]
    state.close()

    assert first[0].status == "FAILED_RETRYABLE"
    assert second[0].status == "ACKNOWLEDGED"
    assert row is not None and tuple(row) == ("ACKNOWLEDGED", 2)
    assert archive_status == "VALIDATED"
