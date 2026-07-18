"""Production runtime, archive and delivery orchestration for PAPER_ONLY service."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import shutil
import signal
import socket
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from neo_trader.neobitcoin_research.tbank import normalize_security_trading_status

from .archive import (
    ArchiveBuildRequest,
    BuiltArchive,
    DailyArchiveBuilder,
    _read_archive_manifest,
)
from .calendar import SessionCalendar
from .config import PaperConfig
from .datasets import DatasetStore
from .delivery import ArtifactDescriptor, CodexSameThreadTransport
from .domain import StrategyStatus, StrategyVersion, trading_status_is_open
from .engine import PaperTradingEngine, strategy_lifecycle
from .execution import PaperExecutionAdapter
from .ingest import (
    CanonicalMarketEvent,
    DataQualityGate,
    InstrumentSnapshot,
    ReadOnlyMarketDataAdapter,
    write_instrument_snapshot,
)
from .observability import HealthRegistry, HealthServer, configure_json_logging
from .registry import StrategyRegistry
from .safety import enforce_startup_boundary
from .state import PaperStateStore
from .strategies import (
    CounterflowFeatureEngine,
    FlowAlignmentStrategy,
    PaperStrategy,
    StrongCounterflowAbsorptionStrategy,
    frozen_counterflow_version,
    frozen_flow_alignment_v1,
)

LOGGER = logging.getLogger("neobitcoin_paper.runtime")

_EXPLICIT_CLOSED_STATUSES = frozenset(
    {"NOT_AVAILABLE_FOR_TRADING", "DEALER_NOT_AVAILABLE_FOR_TRADING", "CLOSED"}
)


class MarketDataSource(Protocol):
    def discover(self) -> InstrumentSnapshot: ...

    def stream(self, stop: asyncio.Event) -> AsyncIterator[CanonicalMarketEvent]: ...


class DeliveryTransport(Protocol):
    def deliver(
        self,
        *,
        thread_id: str,
        artifact: ArtifactDescriptor,
        timeout_seconds: float = 900.0,
    ) -> Any: ...


class Notifier(Protocol):
    def notify(self, message: str) -> bool: ...


class DiskLevel(StrEnum):
    OK = "OK"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    EMERGENCY = "EMERGENCY"


@dataclass(frozen=True, slots=True)
class DiskSnapshot:
    level: DiskLevel
    free_bytes: int
    signals_allowed: bool


class DiskGuard:
    def __init__(
        self,
        config: PaperConfig,
        usage: Callable[[Path], Any] = shutil.disk_usage,
    ) -> None:
        self._config = config
        self._usage = usage

    def check(self) -> DiskSnapshot:
        free = int(self._usage(self._config.data_root).free)
        if free <= self._config.disk_emergency_free_bytes:
            level = DiskLevel.EMERGENCY
        elif free <= self._config.disk_critical_free_bytes:
            level = DiskLevel.CRITICAL
        elif free <= self._config.disk_warning_free_bytes:
            level = DiskLevel.WARNING
        else:
            level = DiskLevel.OK
        return DiskSnapshot(
            level=level,
            free_bytes=free,
            signals_allowed=level in {DiskLevel.OK, DiskLevel.WARNING},
        )


class SystemdNotifier:
    """Minimal sd_notify sender; no shell or external helper is involved."""

    def __init__(self, address: str | None = None) -> None:
        self._address = address if address is not None else os.environ.get("NOTIFY_SOCKET")

    def notify(self, message: str) -> bool:
        if not self._address:
            return False
        address: str | bytes = self._address
        if self._address.startswith("@"):
            address = b"\0" + self._address[1:].encode()
        unix_family = getattr(socket, "AF_UNIX", 1)
        try:
            with socket.socket(unix_family, socket.SOCK_DGRAM) as channel:
                channel.connect(address)
                channel.sendall(message.encode("utf-8"))
        except OSError:
            return False
        return True


@dataclass(frozen=True, slots=True)
class DeliveryRunResult:
    delivery_id: str
    status: str
    attempt_count: int
    turn_run_id: str | None = None
    error_type: str | None = None


class DeliveryWorker:
    def __init__(
        self,
        state: PaperStateStore,
        *,
        transport: DeliveryTransport | None = None,
        maximum_retry_seconds: int = 3600,
    ) -> None:
        self._state = state
        self._transport = transport or CodexSameThreadTransport()
        self._maximum_retry_seconds = maximum_retry_seconds

    def enqueue(self, archive_id: str, thread_id: str) -> str:
        delivery_id = self._state.enqueue_delivery(archive_id, thread_id=thread_id)
        row = self._delivery(delivery_id)
        if row["status"] == "CREATED":
            self._state.transition_delivery(delivery_id, "VALIDATED")
            self._state.transition_delivery(delivery_id, "QUEUED")
        elif row["status"] == "VALIDATED":
            self._state.transition_delivery(delivery_id, "QUEUED")
        return delivery_id

    def run_due(self, *, now: datetime | None = None) -> tuple[DeliveryRunResult, ...]:
        now = now or datetime.now(UTC)
        self._state.reset_interrupted_deliveries(next_retry_at=now)
        results: list[DeliveryRunResult] = []
        for row in self._state.pending_deliveries(due_at=now):
            status = str(row["status"])
            delivery_id = str(row["delivery_id"])
            if status == "CREATED":
                self._state.transition_delivery(delivery_id, "VALIDATED")
                self._state.transition_delivery(delivery_id, "QUEUED")
            elif status == "VALIDATED":
                self._state.transition_delivery(delivery_id, "QUEUED")
            elif status == "DELIVERED":
                turn_id = str(row.get("turn_run_id") or "")
                self._state.transition_delivery(
                    delivery_id, "ACKNOWLEDGED", turn_run_id=turn_id
                )
                results.append(
                    DeliveryRunResult(
                        delivery_id,
                        "ACKNOWLEDGED",
                        int(row["attempt_count"]),
                        turn_run_id=turn_id,
                    )
                )
                continue
            if not self._state.claim_delivery(delivery_id, now=now):
                continue
            claimed = self._delivery(delivery_id)
            attempts = int(claimed["attempt_count"])
            try:
                archive_path = Path(str(claimed["archive_path"]))
                try:
                    manifest = _read_archive_manifest(archive_path)
                except Exception:
                    manifest = {}
                row_counts = manifest.get("row_counts", {})
                counts = row_counts if isinstance(row_counts, Mapping) else {}
                artifact = ArtifactDescriptor(
                    archive_id=str(claimed["archive_id"]),
                    session_date=str(claimed["session_date"]),
                    sha256=str(claimed["archive_sha256"]),
                    size_bytes=archive_path.stat().st_size,
                    strategies=int(str(manifest.get("strategies", 0))),
                    signals=int(str(counts.get("candidate_signals.parquet", 0))),
                    paper_trades=int(str(counts.get("paper_trades.parquet", 0))),
                    pnl_summary="see DAILY_SUMMARY.md",
                    restart_gap_summary="see health/data-quality datasets",
                    local_path=archive_path,
                )
                acknowledgement = self._transport.deliver(
                    thread_id=str(claimed["thread_id"]), artifact=artifact
                )
            except Exception as exc:
                base_delay = min(
                    self._maximum_retry_seconds, max(1, 2 ** min(attempts, 16))
                )
                delay = max(
                    1,
                    min(
                        self._maximum_retry_seconds,
                        round(base_delay * random.uniform(0.8, 1.2)),
                    ),
                )
                self._state.transition_delivery(
                    delivery_id,
                    "FAILED_RETRYABLE",
                    last_error=type(exc).__name__,
                    next_retry_at=now + timedelta(seconds=delay),
                )
                results.append(
                    DeliveryRunResult(
                        delivery_id,
                        "FAILED_RETRYABLE",
                        attempts,
                        error_type=type(exc).__name__,
                    )
                )
                continue
            turn_id = str(getattr(acknowledgement, "turn_id", ""))
            self._state.transition_delivery(
                delivery_id,
                "DELIVERED",
                turn_run_id=turn_id,
            )
            self._state.transition_delivery(delivery_id, "ACKNOWLEDGED", turn_run_id=turn_id)
            results.append(
                DeliveryRunResult(delivery_id, "ACKNOWLEDGED", attempts, turn_run_id=turn_id)
            )
        return tuple(results)

    def _delivery(self, delivery_id: str) -> dict[str, Any]:
        row = self._state.connection.execute(
            "SELECT * FROM delivery_outbox WHERE delivery_id = ?", (delivery_id,)
        ).fetchone()
        if row is None:
            raise KeyError(delivery_id)
        return dict(row)


def build_daily_archive(
    config: PaperConfig,
    state: PaperStateStore,
    session_date: date,
    *,
    test_archive: bool = False,
    dataset_store: DatasetStore | None = None,
    builder: DailyArchiveBuilder | None = None,
    instrument_snapshot: Mapping[str, object] | None = None,
) -> BuiltArchive:
    """Build and record a validated archive, including a zero-trade day."""

    if not test_archive:
        session = state.connection.execute(
            "SELECT status, active FROM session_state WHERE session_date = ?",
            (session_date.isoformat(),),
        ).fetchone()
        if session is None or bool(session["active"]) or str(session["status"]) not in {
            "FINALIZED",
            "FINALIZATION_INCOMPLETE",
        }:
            raise RuntimeError("paper session is not finalized")

    calendar = SessionCalendar()
    window = calendar.window_for(session_date)
    store = dataset_store or DatasetStore(config.data_root, session_date)
    recovery = state.recover()
    strategies = tuple(
        {
            "strategy_id": str(row["strategy_id"]),
            "strategy_version": str(row["strategy_version"]),
            "code_hash": str(row["code_hash"]),
            "config_hash": str(row["config_hash"]),
            "activated_at": str(row["activated_at"]),
            "enabled": bool(row["enabled"]),
            "lifecycle_status": str(row.get("lifecycle_status", "FROZEN_PAPER")),
            "evaluation_cohort": str(row.get("evaluation_cohort", "LIVE_OOS")),
            "lifecycle": row.get("lifecycle", {}),
            "parameters": row.get("config", {}),
        }
        for row in recovery.strategies
    )
    persisted_instrument = config.data_root / "state" / "instrument_snapshot.json"
    if instrument_snapshot is not None:
        instrument = dict(instrument_snapshot)
    elif persisted_instrument.is_file() and not persisted_instrument.is_symlink():
        loaded = json.loads(persisted_instrument.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise RuntimeError("persisted instrument snapshot is invalid")
        instrument = dict(loaded)
    else:
        instrument = {
            "name": config.instrument_name,
            "ticker": config.instrument_ticker,
            "instrument_uid": config.instrument_uid,
            "class_code": config.instrument_class_code,
            "instrument_type": "futures",
            "exchange": "spb_future",
            "lot": 1,
            "min_price_increment": "0.1",
        }
    request = ArchiveBuildRequest(
        data_root=config.data_root,
        session_date=session_date,
        start_utc=window.expected_open,
        end_utc=window.expected_close,
        strategy_registry=strategies,
        session_calendar={
            "version": window.calendar_rule_version,
            "timezone": "Europe/Moscow",
            "expected_open": window.expected_open.isoformat(),
            "expected_close": window.expected_close.isoformat(),
            "live_status_has_priority": True,
        },
        config_snapshot=config.public_snapshot(),
        code_version={"package": "neobitcoin_paper", "schema": "v1"},
        instrument_snapshot=instrument,
        test_archive=test_archive,
    )
    built = (builder or DailyArchiveBuilder()).build(request, dataset_store=store)
    if not built.validation.passed:
        raise RuntimeError("archive builder returned an unvalidated artifact")
    state.record_archive(
        built.archive_id,
        session_date=session_date,
        path=built.archive_path,
        sha256=built.sha256,
        status="VALIDATED",
        size_bytes=built.size_bytes,
        validation={
            **asdict(built.validation),
            "test_archive": test_archive,
            "session_classification": built.manifest.get("session_classification"),
            "investigation_required": built.manifest.get("investigation_required", True),
            "oos_included": bool(built.manifest.get("oos_included", False)),
        },
        validated_at=datetime.now(UTC),
    )
    return built


class PaperRuntime:
    def __init__(
        self,
        config: PaperConfig,
        *,
        environ: Mapping[str, str] | None = None,
        market_data: MarketDataSource | None = None,
        health: HealthRegistry | None = None,
        health_server_factory: Callable[[HealthRegistry, str, int], Any] = HealthServer,
        notifier: Notifier | None = None,
        disk_guard: DiskGuard | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.config = config
        self._environ = dict(os.environ if environ is None else environ)
        self._market = market_data or ReadOnlyMarketDataAdapter(
            config.token_file,
            # The SDK emits subscription-check/ping traffic on a roughly
            # 60-second cadence while the market is closed.  Keep the hard
            # stream silence watchdog above that cadence; component freshness
            # remains separately fail-closed for trading.
            stream_silence_seconds=max(75.0, config.stale_after_seconds * 1.5),
        )
        self.health = health or HealthRegistry()
        self._health_server_factory = health_server_factory
        self._notifier = notifier or SystemdNotifier(self._environ.get("NOTIFY_SOCKET"))
        self._disk_guard = disk_guard or DiskGuard(config)
        self._clock = clock or (lambda: datetime.now(UTC))
        self._calendar = SessionCalendar()
        self._ready_notified = False
        self._engine: PaperTradingEngine | None = None
        self._quality_gate: DataQualityGate | None = None
        self._datasets: DatasetStore | None = None
        self._state: PaperStateStore | None = None
        self._active_session_date: date | None = None
        self._session_finalized = False
        self._last_trading_status = "UNKNOWN"
        self._closed_since: datetime | None = None
        self._tick_size = Decimal("0.1")
        self._disk_blocked = False
        self._stream_connected = False

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        package_root = Path(__file__).resolve().parent
        enforce_startup_boundary(package_root, self._environ)
        configure_json_logging()
        self.config.ensure_directories()
        self._install_signals(stop)
        health_server = self._health_server_factory(
            self.health, self.config.health_host, self.config.health_port
        )
        health_server.start()
        state = PaperStateStore(
            self.config.data_root / "state" / "paper_state.sqlite3",
            restart_metadata={"paper_only": True},
            clock=self._now,
        )
        self._state = state
        try:
            state.quick_check()
            state.reset_interrupted_deliveries()
            # Lifecycle policy must migrate even after the current session has
            # already finalized and no execution engine is built.
            self._strategy_specifications(state)
            self.health.heartbeat("state_store", healthy=True, ready=True)
            instrument = await asyncio.to_thread(self._market.discover)
            self._verify_instrument(instrument)
            self._tick_size = Decimal(str(instrument.min_price_increment))
            write_instrument_snapshot(
                self.config.data_root / "state" / "instrument_snapshot.json", instrument
            )
            self.health.heartbeat("instrument", healthy=True, ready=True)
            self._last_trading_status = _normalize_trading_status(instrument.trading_status)
            started_at = self._now()
            initial_session = self._calendar.resolve(
                started_at, self._last_trading_status
            ).session_date_msk
            recovered = state.recover().active_session
            recovered_date = (
                date.fromisoformat(str(recovered["session_date"]))
                if recovered is not None and bool(recovered.get("active"))
                else None
            )
            if recovered_date is not None and recovered_date > initial_session:
                raise RuntimeError("durable active session is ahead of the Moscow calendar")
            if recovered_date is not None and recovered_date < initial_session:
                self._active_session_date = recovered_date
                self._session_finalized = False
                self._engine = self._build_engine(state, recovered_date)
                recovered_close = self._calendar.window_for(recovered_date).expected_close
                self._finalize_active(
                    recovered_close - timedelta(microseconds=1), "UNKNOWN"
                )
            self._active_session_date = initial_session
            self._session_finalized = self._session_is_finalized(state, initial_session)
            if not self._session_finalized:
                self._engine = self._build_engine(state, initial_session)
            else:
                self._engine = None
                self._quality_gate = self._new_quality_gate()
            if _is_explicitly_closed(self._last_trading_status):
                # A closed/not-tradable status is an entry gate, not a session
                # boundary.  In particular the weekend status observed at
                # process start must not finalize the writer after the normal
                # five-minute archive grace period.
                self._closed_since = started_at
            self.health.heartbeat("writer", healthy=True, ready=True)
            self.health.heartbeat("event_loop", healthy=True, ready=True)
            async for event in self._market.stream(stop):
                if stop.is_set():
                    break
                if event.event_type == "reconnect":
                    # Opening a gRPC iterator is not readiness.  Fresh ACKs,
                    # status and a valid book must converge for this generation.
                    self._stream_connected = False
                elif event.event_type == "disconnect":
                    self._stream_connected = False
                self._observe_live_status(event)
                desired_session = self._calendar.resolve(
                    event.processing_ts, self._last_trading_status
                ).session_date_msk
                if self._active_session_date is None or desired_session > self._active_session_date:
                    self._rollover(state, desired_session)
                disk = self._disk_guard.check()
                self.health.set_gauge("disk_free_bytes", disk.free_bytes)
                self.health.heartbeat(
                    "disk",
                    healthy=disk.level is not DiskLevel.EMERGENCY,
                    ready=disk.signals_allowed,
                    detail=disk.level.value,
                )
                if self._disk_blocked and disk.signals_allowed:
                    raise RuntimeError("disk pressure cleared; safe stream restart required")
                if not disk.signals_allowed:
                    if not self._disk_blocked:
                        state.checkpoint("PASSIVE")
                        if self._quality_gate is not None:
                            self._quality_gate.on_disconnect()
                    self._disk_blocked = True
                result = None
                if not self._session_finalized and disk.signals_allowed:
                    if self._engine is None:
                        raise RuntimeError("active paper session has no execution engine")
                    result = await self._engine.process_event(event)
                    self._update_closed_boundary(event.processing_ts)
                    self._maybe_finalize_active(event.processing_ts)
                elif self._quality_gate is not None and disk.signals_allowed:
                    # Between finalized session close and the next pre-open,
                    # continue supervising the 24/7 stream without writing
                    # historical events into a completed session.
                    if event.event_type == "reconnect":
                        self._quality_gate.on_connect(event.reconnect_generation)
                    elif event.event_type == "disconnect":
                        self._quality_gate.on_disconnect()
                    self._quality_gate.observe(event)
                if self._quality_gate is not None:
                    quality = self._quality_gate.snapshot()
                    self._last_trading_status = _normalize_trading_status(
                        quality.trading_status
                    )
                    self._stream_connected = quality.operational_ready
                    self.health.heartbeat(
                        "subscriptions",
                        healthy=quality.subscription_state not in {"FATAL"},
                        ready=quality.operational_ready,
                        detail=f"{quality.subscription_state}:{quality.reason}",
                    )
                    self.health.heartbeat(
                        "feature_pipeline",
                        healthy=True,
                        ready=(
                            quality.feature_ready
                            or quality.subscription_state == "CLOSED_MARKET"
                        ),
                        detail=quality.reason,
                    )
                self.health.heartbeat(
                    "market_stream",
                    healthy=True,
                    ready=self._stream_connected,
                    detail="connected" if self._stream_connected else "disconnected",
                )
                self.health.heartbeat("event_loop", healthy=True, ready=True)
                self.health.heartbeat("state_store", healthy=True, ready=True)
                self.health.heartbeat(
                    "writer", healthy=True, ready=disk.signals_allowed
                )
                self.health.increment("market_events")
                self.health.increment(
                    "duplicate_events", int(result.duplicate) if result is not None else 0
                )
                self.health.set_gauge(
                    "open_positions",
                    len(self._engine.open_positions) if self._engine is not None else 0,
                )
                self.health.set_gauge(
                    "pending_intents",
                    len(self._engine.pending_intents) if self._engine is not None else 0,
                )
                self._notify_healthy()
            self.health.heartbeat("market_stream", healthy=True, ready=False, detail="stopped")
        finally:
            self._notifier.notify("STOPPING=1")
            if self._datasets is not None:
                self._datasets.abort()
            try:
                state.quick_check()
                state.checkpoint("PASSIVE")
                stamp = self._now().strftime("%Y%m%dT%H%M%SZ")
                backup_dir = self.config.data_root / "state" / "backups"
                state.prune_backups(
                    backup_dir, keep=max(1, self.config.state_backup_keep - 1)
                )
                state.backup(backup_dir / f"paper-{stamp}.sqlite")
                state.prune_backups(backup_dir, keep=self.config.state_backup_keep)
            finally:
                state.close()
                health_server.close()

    def _build_engine(
        self, state: PaperStateStore, session_date: date
    ) -> PaperTradingEngine:
        registry = StrategyRegistry()
        plugins: list[PaperStrategy] = []
        for specification, registered_at, enabled in self._strategy_specifications(state):
            registry = registry.register(
                specification,
                registered_at=registered_at,
                initial_cash=Decimal(str(self.config.initial_balance)),
            )
            if enabled:
                if specification.strategy_id in {
                    "MICRO_FLOW_ALIGNMENT",
                    "L5_FLOW_ALIGNMENT",
                }:
                    plugins.append(FlowAlignmentStrategy(specification))
                elif specification.strategy_id == "STRONG_COUNTERFLOW_ABSORPTION":
                    plugins.append(
                        StrongCounterflowAbsorptionStrategy(
                            specification, latency_ms=self.config.decision_latency_ms
                        )
                    )
                else:
                    raise RuntimeError("enabled strategy has no installed sandboxed plugin")
        self._datasets = DatasetStore(self.config.data_root, session_date)
        gate = self._new_quality_gate()
        self._quality_gate = gate
        return PaperTradingEngine(
            state_store=state,
            dataset_store=self._datasets,
            registry=registry,
            plugins=tuple(plugins),
            calendar=self._calendar,
            data_quality_gate=gate,
            feature_engine=CounterflowFeatureEngine(),
            execution_adapter=PaperExecutionAdapter(
                timedelta(milliseconds=self.config.decision_latency_ms)
            ),
            tick_size=self._tick_size,
        )

    def _new_quality_gate(self) -> DataQualityGate:
        return DataQualityGate(
            warmup_events=self.config.warmup_events,
            stale_after_seconds=self.config.stale_after_seconds,
            max_latency_ms=self.config.excessive_latency_ms,
            initial_trading_status=self._last_trading_status,
        )

    def _strategy_specifications(
        self,
        state: PaperStateStore,
    ) -> tuple[tuple[StrategyVersion, datetime, bool], ...]:
        recovery = state.recover()
        if not recovery.strategies:
            created = self._now()
            activated = created + timedelta(seconds=5)
            return (
                (
                    frozen_flow_alignment_v1(
                        "MICRO_FLOW_ALIGNMENT",
                        created_at=created,
                        activated_at=activated,
                    ),
                    created + timedelta(seconds=1),
                    True,
                ),
                (
                    frozen_flow_alignment_v1(
                        "L5_FLOW_ALIGNMENT",
                        created_at=created,
                        activated_at=activated,
                    ),
                    created + timedelta(seconds=1),
                    False,
                ),
            )
        versions: list[tuple[StrategyVersion, datetime, bool]] = []
        installed: set[str] = set()
        for row in recovery.strategies:
            enabled = bool(row["enabled"])
            activated = datetime.fromisoformat(
                str(row["activated_at"]).replace("Z", "+00:00")
            )
            registered = datetime.fromisoformat(
                str(row["registered_at"]).replace("Z", "+00:00")
            )
            strategy_id = str(row["strategy_id"])
            version = str(row["strategy_version"])
            if strategy_id == "STRONG_COUNTERFLOW_ABSORPTION":
                specification = replace(
                    frozen_counterflow_version(
                        version=version,
                        created_at=registered - timedelta(microseconds=1),
                        activated_at=activated,
                        parameter_overrides=row.get("config", {}),
                        discovery_source="immutable persisted paper-strategy registry",
                    ),
                    status=StrategyStatus.REJECTED_OOS_AS_FORMALIZED,
                )
                enabled = False
                state.set_strategy_enabled(strategy_id, version, False)
            elif strategy_id in {"MICRO_FLOW_ALIGNMENT", "L5_FLOW_ALIGNMENT"}:
                if version != "v1":
                    raise RuntimeError("unsupported immutable flow-alignment version")
                specification = frozen_flow_alignment_v1(
                    strategy_id,
                    created_at=registered - timedelta(microseconds=1),
                    activated_at=activated,
                )
                if str(row.get("evaluation_cohort", "LIVE_OOS")) != "LIVE_OOS":
                    raise RuntimeError("flow-alignment evaluation cohort is not LIVE_OOS")
                enabled = strategy_id == "MICRO_FLOW_ALIGNMENT"
                state.set_strategy_enabled(strategy_id, version, enabled)
                state.set_strategy_lifecycle(
                    strategy_id,
                    version,
                    lifecycle_status=specification.status.value,
                    evaluation_cohort="LIVE_OOS",
                    lifecycle=strategy_lifecycle(specification),
                )
            else:
                if enabled:
                    state.set_strategy_enabled(strategy_id, version, False)
                continue
            if specification.code_hash != str(row["code_hash"]):
                raise RuntimeError("persisted strategy code hash does not match installed plugin")
            versions.append((specification, registered, enabled))
            installed.add(strategy_id)
        created = self._now()
        activated = created + timedelta(seconds=5)
        for strategy_id in ("MICRO_FLOW_ALIGNMENT", "L5_FLOW_ALIGNMENT"):
            if strategy_id in installed:
                continue
            versions.append(
                (
                    frozen_flow_alignment_v1(
                        strategy_id,
                        created_at=created,
                        activated_at=activated,
                    ),
                    created + timedelta(seconds=1),
                    strategy_id == "MICRO_FLOW_ALIGNMENT",
                )
            )
        return tuple(versions)

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise RuntimeError("paper runtime clock must return a timezone-aware datetime")
        return value.astimezone(UTC)

    @staticmethod
    def _session_is_finalized(state: PaperStateStore, session_date: date) -> bool:
        row = state.connection.execute(
            "SELECT status, active FROM session_state WHERE session_date = ?",
            (session_date.isoformat(),),
        ).fetchone()
        return bool(
            row is not None
            and not bool(row["active"])
            and str(row["status"]) in {"FINALIZED", "FINALIZATION_INCOMPLETE"}
        )

    def _observe_live_status(self, event: CanonicalMarketEvent) -> None:
        if event.event_type != "trading_status":
            return
        value: object = event.payload.get("trading_status")
        if isinstance(value, Mapping):
            value = value.get("trading_status") or value.get("tradingStatus")
        if value is None:
            value = event.payload.get("tradingStatus")
        self._last_trading_status = _normalize_trading_status(value)

    def _update_closed_boundary(self, observed_at: datetime) -> None:
        if _is_explicitly_closed(self._last_trading_status):
            if self._closed_since is None:
                self._closed_since = observed_at
        else:
            self._closed_since = None

    def _maybe_finalize_active(self, observed_at: datetime) -> None:
        if self._session_finalized:
            return
        grace = timedelta(seconds=self.config.archive_grace_seconds)
        if self._active_session_date is not None:
            window = self._calendar.window_for(self._active_session_date)
            if (
                observed_at >= window.expected_close + grace
                and not trading_status_is_open(self._last_trading_status)
            ):
                # The exchange may remain in BREAK_IN_TRADING after the
                # scheduled close instead of publishing one of the narrow
                # terminal statuses.  Finalize at the session boundary so the
                # daily archive cannot depend on a particular enum transition.
                self._finalize_active(
                    window.expected_close - timedelta(microseconds=1),
                    self._last_trading_status,
                )
                return
        # Never finalize merely because an explicit closed status persisted.
        # The status can legitimately cover pre-open, breaks, weekends, or an
        # unavailable instrument.  Keep recording status/ping evidence for the
        # full expected session and finalize only at the scheduled boundary.

    def _finalize_active(self, observed_at: datetime, trading_status: str) -> None:
        if self._engine is None or self._session_finalized:
            return
        self._engine.finalize_session(observed_at, trading_status=trading_status)
        if self._datasets is not None:
            # Heavy Parquet materialization belongs to the separate archive worker.
            self._datasets.abort()
        self._datasets = None
        self._session_finalized = True

    def _rollover(self, state: PaperStateStore, desired_session: date) -> None:
        if self._active_session_date is not None and not self._session_finalized:
            window = self._calendar.window_for(self._active_session_date)
            fallback_at = window.expected_close - timedelta(microseconds=1)
            self._finalize_active(fallback_at, "UNKNOWN")
        self._active_session_date = desired_session
        self._session_finalized = self._session_is_finalized(state, desired_session)
        self._closed_since = None
        self._engine = (
            None
            if self._session_finalized
            else self._build_engine(state, desired_session)
        )

    def _verify_instrument(self, snapshot: InstrumentSnapshot) -> None:
        expected = (
            self.config.instrument_uid,
            self.config.instrument_ticker.casefold(),
            self.config.instrument_class_code.casefold(),
            "".join(ch for ch in self.config.instrument_name.casefold() if ch.isalnum()),
        )
        actual = (
            snapshot.instrument_uid,
            snapshot.ticker.casefold(),
            snapshot.class_code.casefold(),
            "".join(ch for ch in snapshot.name.casefold() if ch.isalnum()),
        )
        instrument_type = (snapshot.instrument_type or "").strip().casefold()
        exchange = (snapshot.exchange or "").strip().casefold()
        try:
            tick_size = Decimal(str(snapshot.min_price_increment))
        except Exception as exc:
            raise RuntimeError("instrument tick size is invalid") from exc
        if (
            actual != expected
            or not snapshot.api_market_data_available
            or instrument_type not in {"future", "futures"}
            or "future" not in exchange
            or snapshot.lot != 1
            or tick_size != Decimal("0.1")
        ):
            raise RuntimeError("exact Neobitcoin instrument identity check failed")

    def _notify_healthy(self) -> None:
        snapshot = self.health.snapshot()
        critical = ("event_loop", "state_store", "writer", "market_stream")
        # Closed-market SDK ping cadence is about 60 seconds.  This freshness
        # bound remains below systemd WatchdogSec=180 and above the 75-second
        # internal stream-silence reconnect threshold.
        fresh = all(not self.health.stale_component(name, 120.0) for name in critical)
        if not snapshot.healthy or not fresh:
            return
        # systemd READY means the supervised process completed initialization;
        # functional market readiness remains independently represented by
        # /readyz and never permits entries while subscriptions/features lag.
        if not self._ready_notified:
            self._notifier.notify("READY=1")
            self._ready_notified = True
        self._notifier.notify("WATCHDOG=1")

    @staticmethod
    def _install_signals(stop: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        for item in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(item, stop.set)
            except (NotImplementedError, RuntimeError):
                continue


def validated_archive_row(state: PaperStateStore, archive_id: str) -> dict[str, Any]:
    row = state.connection.execute(
        "SELECT * FROM archives WHERE archive_id = ? AND status = 'VALIDATED'", (archive_id,)
    ).fetchone()
    if row is None:
        raise KeyError(f"validated archive not found: {archive_id}")
    return dict(row)


def allowlisted_archive_path(config: PaperConfig, path: str | Path) -> Path:
    root = (config.data_root / "daily_archives").resolve()
    resolved = Path(path).resolve(strict=True)
    if not resolved.is_file() or not resolved.is_relative_to(root):
        raise PermissionError("archive path is outside the allow-listed directory")
    return resolved


def _normalize_trading_status(value: object) -> str:
    return normalize_security_trading_status(value)


def _is_explicitly_closed(value: object) -> bool:
    return _normalize_trading_status(value) in _EXPLICIT_CLOSED_STATUSES


__all__ = [
    "DeliveryRunResult",
    "DeliveryWorker",
    "DiskGuard",
    "DiskLevel",
    "DiskSnapshot",
    "PaperRuntime",
    "SystemdNotifier",
    "allowlisted_archive_path",
    "build_daily_archive",
    "validated_archive_row",
]
