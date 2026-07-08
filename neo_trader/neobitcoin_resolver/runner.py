"""Runner utilities for local smoke and daemon entrypoints."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from neo_trader.broker.tbank import TBankClient, TBankOrderBookSnapshot
from neo_trader.neo_universal_swarm.instruments import load_swarm_instrument_catalog
from neo_trader.neo_universal_swarm.live_paper import orderbook_snapshot_to_book_snapshot
from neo_trader.neobitcoin_resolver.config import ResolverConfig, load_resolver_config
from neo_trader.neobitcoin_resolver.dashboard import (
    write_dashboard_state,
    write_heartbeat_file,
)
from neo_trader.neobitcoin_resolver.engine import (
    DualBotNeobitcoinResolver,
    ResolverCycleResult,
)
from neo_trader.neobitcoin_resolver.mock import smoke_snapshots
from neo_trader.neobitcoin_resolver.storage import ResolverJournal
from neo_trader.neobitcoin_resolver.types import GateSnapshot

ClockFunc = Callable[[], datetime]
SleepFunc = Callable[[float], None]


class OrderbookProvider(Protocol):
    def get_orderbook_snapshot(
        self,
        instrument_id: str,
        *,
        depth: int = 10,
        order_book_type: str | None = None,
    ) -> TBankOrderBookSnapshot:
        """Return one read-only orderbook snapshot."""


@dataclass(frozen=True)
class ResolverRunResult:
    cycles: tuple[ResolverCycleResult, ...]
    db_path: Path
    dashboard_path: Path
    heartbeat_path: Path


def run_resolver_snapshots(
    snapshots: Iterable[GateSnapshot],
    *,
    config: ResolverConfig | None = None,
    db_path: Path | str | None = None,
) -> ResolverRunResult:
    resolved = config or load_resolver_config()
    journal = ResolverJournal(db_path or resolved.sqlite_path)
    journal.initialize()
    resolver = DualBotNeobitcoinResolver(resolved, journal)
    cycles = tuple(resolver.process_snapshot(snapshot) for snapshot in snapshots)
    dashboard_path = write_dashboard_state(config=resolved, journal=journal)
    heartbeat_path = write_heartbeat_file(config=resolved, journal=journal)
    return ResolverRunResult(
        cycles=cycles,
        db_path=journal.path,
        dashboard_path=dashboard_path,
        heartbeat_path=heartbeat_path,
    )


def run_mock_smoke(
    *,
    config: ResolverConfig | None = None,
    db_path: Path | str | None = None,
) -> ResolverRunResult:
    return run_resolver_snapshots(smoke_snapshots(), config=config, db_path=db_path)


def run_live_resolver(
    *,
    config: ResolverConfig | None = None,
    provider: OrderbookProvider | None = None,
    db_path: Path | str | None = None,
    max_cycles: int | None = None,
    sleep: SleepFunc = time.sleep,
    clock: ClockFunc | None = None,
) -> ResolverRunResult:
    """Run the resolver on read-only T-Bank orderbooks until stopped."""

    resolved = config or load_resolver_config()
    journal = ResolverJournal(db_path or resolved.sqlite_path)
    journal.initialize()
    resolver = DualBotNeobitcoinResolver(resolved, journal)
    metadata = load_swarm_instrument_catalog().get(resolved.instrument)
    client = provider or TBankClient()
    now = clock or (lambda: datetime.now(UTC))
    previous_book = None
    cycles: list[ResolverCycleResult] = []
    cycle = 0

    while max_cycles is None or cycle < max_cycles:
        cycle += 1
        timestamp = now()
        try:
            orderbook = client.get_orderbook_snapshot(
                metadata.uid,
                depth=resolved.orderbook_depth,
            )
            book = orderbook_snapshot_to_book_snapshot(
                orderbook,
                instrument=resolved.instrument,
                timestamp=timestamp,
                previous=previous_book,
            )
            previous_book = book
            result = resolver.process_snapshot(GateSnapshot(book=book))
        except Exception as exc:
            journal.record_heartbeat(
                timestamp=timestamp,
                bot_status="running",
                api_status="error",
                data_freshness="bad",
                last_orderbook_time=resolver.last_orderbook_time,
                last_trade_time=resolver.last_trade_time,
                current_state=f"ERROR:{type(exc).__name__}",
                current_open_pair=None
                if resolver.active_pair is None
                else resolver.active_pair.pair_id,
            )
            result = ResolverCycleResult(
                timestamp=timestamp,
                pair_id=None if resolver.active_pair is None else resolver.active_pair.pair_id,
                state="ERROR",
                blocked_reason=type(exc).__name__,
            )
        cycles.append(result)
        write_dashboard_state(config=resolved, journal=journal)
        write_heartbeat_file(config=resolved, journal=journal)
        if max_cycles is None or cycle < max_cycles:
            sleep(resolved.poll_interval_seconds)

    dashboard_path = write_dashboard_state(config=resolved, journal=journal)
    heartbeat_path = write_heartbeat_file(config=resolved, journal=journal)
    return ResolverRunResult(
        cycles=tuple(cycles),
        db_path=journal.path,
        dashboard_path=dashboard_path,
        heartbeat_path=heartbeat_path,
    )


__all__ = [
    "OrderbookProvider",
    "ResolverRunResult",
    "run_live_resolver",
    "run_mock_smoke",
    "run_resolver_snapshots",
]
