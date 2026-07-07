"""Live-market paper loop for the Neo Universal Bot Swarm.

The loop polls T-Bank read-only order books and simulates hedge-pair lifecycle
locally. It never submits, cancels, or replaces broker orders.
"""

from __future__ import annotations

import json
import os
import time
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from neo_trader.broker.tbank import TBankClient, TBankOrderBookSnapshot
from neo_trader.neo_universal_swarm.bots import CuratorBot
from neo_trader.neo_universal_swarm.config import DEFAULT_ACCOUNTS_CONFIG, load_accounts_config
from neo_trader.neo_universal_swarm.dashboard import build_swarm_dashboard_state
from neo_trader.neo_universal_swarm.instruments import (
    SwarmInstrumentCatalog,
    load_swarm_instrument_catalog,
)
from neo_trader.neo_universal_swarm.model import PairEVModel, PairEVModelConfig
from neo_trader.neo_universal_swarm.simulator import (
    HedgePairSimulationConfig,
    aggregate_pair_metrics,
)
from neo_trader.neo_universal_swarm.types import (
    ActivePair,
    BookSnapshot,
    LegSide,
    PairExitReason,
    PairLabel,
    RejectionReason,
    SwarmInstrument,
    SwarmMetrics,
    as_utc,
)
from neo_trader.runtime import get_runtime_commit_hash

ClockFunc = Callable[[], datetime]
SleepFunc = Callable[[float], None]


class OrderBookProvider(Protocol):
    """Read-only market-data subset used by the live paper loop."""

    def get_orderbook_snapshot(
        self,
        instrument_id: str,
        *,
        depth: int = 10,
        order_book_type: str | None = None,
    ) -> TBankOrderBookSnapshot:
        """Return a current order book snapshot."""


@dataclass(frozen=True)
class LivePaperSwarmConfig:
    """Runtime settings for real-data paper trading."""

    accounts_path: Path = DEFAULT_ACCOUNTS_CONFIG
    orderbook_depth: int = 10
    poll_interval_seconds: float = 5.0
    min_required_ev_ticks: Decimal = Decimal("1")
    slippage_stress_ticks: Decimal = Decimal("0")
    max_pair_age_seconds: float = 300.0
    reports_dir: Path = Path("data/reports/neo_universal_swarm_live_paper")
    dashboard_state_path: Path = Path("data/monitoring/neo_universal_swarm_dashboard_state.json")
    heartbeat_path: Path = Path("data/monitoring/neo_universal_swarm_heartbeat.txt")
    instruments: tuple[SwarmInstrument, ...] = tuple(SwarmInstrument)
    max_cycles: int | None = None

    def __post_init__(self) -> None:
        if self.orderbook_depth <= 0:
            raise ValueError("orderbook_depth must be positive.")
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive.")
        if self.min_required_ev_ticks < 0:
            raise ValueError("min_required_ev_ticks must be non-negative.")
        if self.slippage_stress_ticks < 0:
            raise ValueError("slippage_stress_ticks must be non-negative.")
        if self.max_pair_age_seconds <= 0:
            raise ValueError("max_pair_age_seconds must be positive.")
        if not self.instruments:
            raise ValueError("instruments must not be empty.")
        if self.max_cycles is not None and self.max_cycles <= 0:
            raise ValueError("max_cycles must be positive when set.")


@dataclass(frozen=True)
class LivePaperSwarmCycle:
    """One completed live-paper polling cycle."""

    cycle: int
    started_at: datetime
    finished_at: datetime
    status: str
    snapshots: int
    active_pairs: int
    closed_pairs: int
    total_pnl_ticks: Decimal
    error: str | None = None


@dataclass
class _LivePairState:
    pair: ActivePair
    entry: BookSnapshot
    long_entry: Decimal
    short_entry: Decimal
    stop: Decimal
    slippage: Decimal
    loser_side: LegSide | None = None
    runner_side: LegSide | None = None
    loser_loss_ticks: Decimal = Decimal("0")
    runner_mfe_ticks: Decimal = Decimal("-999999")
    runner_mae_ticks: Decimal = Decimal("999999")
    breakeven_active: bool = False
    max_adverse_excursion: Decimal = Decimal("0")
    max_favorable_excursion: Decimal = Decimal("0")
    latest_snapshot: BookSnapshot | None = None


def run_live_paper_swarm(
    config: LivePaperSwarmConfig | None = None,
    *,
    provider: OrderBookProvider | None = None,
    sleep: SleepFunc = time.sleep,
    clock: ClockFunc | None = None,
) -> tuple[LivePaperSwarmCycle, ...]:
    """Run live-market paper trading until stopped or ``max_cycles`` is reached."""

    resolved_config = config or LivePaperSwarmConfig()
    _assert_safe_environment()
    now = clock or (lambda: datetime.now(UTC))
    accounts = load_accounts_config(resolved_config.accounts_path)
    catalog = load_swarm_instrument_catalog()
    model = PairEVModel(
        PairEVModelConfig(
            min_required_ev_ticks=resolved_config.min_required_ev_ticks,
            slippage_stress_ticks=resolved_config.slippage_stress_ticks,
        )
    )
    curator = CuratorBot(accounts=accounts, model=model)
    client = provider or TBankClient()
    owns_client = provider is None
    latest_snapshots: dict[SwarmInstrument, BookSnapshot] = {}
    live_pairs: dict[str, _LivePairState] = {}
    labels: list[PairLabel] = []
    cycles: list[LivePaperSwarmCycle] = []
    cycle_number = 0

    try:
        while True:
            cycle_number += 1
            started_at = _as_utc(now())
            try:
                snapshots = _poll_orderbooks(
                    client,
                    catalog=catalog,
                    instruments=resolved_config.instruments,
                    previous=latest_snapshots,
                    depth=resolved_config.orderbook_depth,
                    clock=now,
                )
                for snapshot in snapshots:
                    latest_snapshots[snapshot.instrument] = snapshot
                    closed = _update_live_pair_for_snapshot(
                        snapshot,
                        live_pairs=live_pairs,
                        config=resolved_config,
                    )
                    for label in closed:
                        curator.settle_pair(label)
                        labels.append(label)
                        live_pairs.pop(label.pair_id, None)

                curator.release_cooldowns()
                for snapshot in snapshots:
                    if _has_active_pair_for_instrument(live_pairs, snapshot.instrument):
                        continue
                    active_pair = curator.maybe_open_pair(snapshot)
                    if active_pair is not None:
                        live_pairs[active_pair.pair_id] = _new_live_pair_state(
                            active_pair,
                            snapshot,
                            slippage_stress_ticks=resolved_config.slippage_stress_ticks,
                        )

                metrics = _metrics(labels, curator)
                if latest_snapshots:
                    _write_live_dashboard_state(
                        resolved_config.dashboard_state_path,
                        curator=curator,
                        latest_snapshots=tuple(
                            latest_snapshots[instrument]
                            for instrument in sorted(latest_snapshots, key=lambda item: item.value)
                        ),
                        metrics=metrics,
                        updated_at=_as_utc(now()),
                    )
                _write_summary_json(
                    resolved_config.reports_dir / "live_paper_summary.json",
                    cycle=cycle_number,
                    metrics=metrics,
                    active_pairs=live_pairs,
                    latest_snapshots=latest_snapshots,
                )
                cycle = LivePaperSwarmCycle(
                    cycle=cycle_number,
                    started_at=started_at,
                    finished_at=_as_utc(now()),
                    status="OK",
                    snapshots=len(snapshots),
                    active_pairs=len(live_pairs),
                    closed_pairs=len(labels),
                    total_pnl_ticks=curator.total_pnl_ticks,
                )
            except Exception as exc:  # pragma: no cover - service resilience path
                cycle = LivePaperSwarmCycle(
                    cycle=cycle_number,
                    started_at=started_at,
                    finished_at=_as_utc(now()),
                    status="ERROR",
                    snapshots=0,
                    active_pairs=len(live_pairs),
                    closed_pairs=len(labels),
                    total_pnl_ticks=curator.total_pnl_ticks,
                    error=f"{type(exc).__name__}: {exc}",
                )
                _write_error_log(resolved_config.reports_dir, exc)

            cycles.append(cycle)
            _write_heartbeat(resolved_config.heartbeat_path, cycle)
            if (
                resolved_config.max_cycles is not None
                and cycle_number >= resolved_config.max_cycles
            ):
                return tuple(cycles)
            sleep(resolved_config.poll_interval_seconds)
    finally:
        close = getattr(client, "close", None)
        if owns_client and close is not None:
            close()


def orderbook_snapshot_to_book_snapshot(
    orderbook: TBankOrderBookSnapshot,
    *,
    instrument: SwarmInstrument,
    timestamp: datetime,
    previous: BookSnapshot | None = None,
    latency_ms: int = 0,
    tick_size: Decimal = Decimal("1"),
) -> BookSnapshot:
    """Convert a T-Bank order book snapshot into a swarm model snapshot."""

    if not orderbook.bids or not orderbook.asks:
        raise ValueError(f"{instrument.value} order book has no bid/ask levels.")
    event_time = _as_utc(timestamp)
    bids = [(level.price, Decimal(level.quantity)) for level in orderbook.bids]
    asks = [(level.price, Decimal(level.quantity)) for level in orderbook.asks]
    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid_price = (best_bid + best_ask) / Decimal("2")
    previous_mid = previous.mid_price if previous is not None else mid_price
    elapsed_seconds = (
        max((event_time - previous.timestamp).total_seconds(), 1e-9)
        if previous is not None
        else 1.0
    )
    tick_velocity = ((mid_price - previous_mid) / tick_size) / Decimal(str(elapsed_seconds))
    tick_direction = 1 if tick_velocity > 0 else -1 if tick_velocity < 0 else 0
    trade_side = (
        LegSide.LONG
        if tick_direction > 0
        else LegSide.SHORT
        if tick_direction < 0
        else None
    )
    top_bid_quantity = bids[0][1]
    top_ask_quantity = asks[0][1]
    volume_delta = top_bid_quantity - top_ask_quantity
    volatility = min(abs(tick_velocity), Decimal("5"))

    return BookSnapshot.from_levels(
        timestamp=event_time,
        instrument=instrument,
        bids=bids,
        asks=asks,
        tick_size=tick_size,
        last_price=orderbook.last_price or mid_price,
        last_trade_size=Decimal("0"),
        trade_side=trade_side,
        tick_direction=tick_direction,
        tick_velocity=tick_velocity,
        volume_delta_1s=volume_delta,
        volume_delta_5s=volume_delta,
        volume_delta_15s=volume_delta,
        volatility_5s=volatility,
        volatility_15s=volatility,
        volatility_60s=volatility,
        latency_ms=latency_ms,
    )


def _poll_orderbooks(
    provider: OrderBookProvider,
    *,
    catalog: SwarmInstrumentCatalog,
    instruments: Sequence[SwarmInstrument],
    previous: Mapping[SwarmInstrument, BookSnapshot],
    depth: int,
    clock: ClockFunc,
) -> tuple[BookSnapshot, ...]:
    metadata = catalog.by_instrument()
    snapshots: list[BookSnapshot] = []
    for instrument in instruments:
        item = metadata[instrument]
        request_started_at = _as_utc(clock())
        orderbook = provider.get_orderbook_snapshot(item.uid, depth=depth)
        received_at = _as_utc(clock())
        latency_ms = int(max((received_at - request_started_at).total_seconds() * 1000, 0))
        snapshots.append(
            orderbook_snapshot_to_book_snapshot(
                orderbook,
                instrument=instrument,
                timestamp=received_at,
                previous=previous.get(instrument),
                latency_ms=latency_ms,
            )
        )
    return tuple(snapshots)


def _new_live_pair_state(
    pair: ActivePair,
    entry: BookSnapshot,
    *,
    slippage_stress_ticks: Decimal,
) -> _LivePairState:
    slippage = entry.slippage_ticks + slippage_stress_ticks
    return _LivePairState(
        pair=pair,
        entry=entry,
        long_entry=entry.best_ask + (slippage * entry.tick_size),
        short_entry=entry.best_bid - (slippage * entry.tick_size),
        stop=Decimal(pair.stop_loss_ticks),
        slippage=slippage,
    )


def _update_live_pair_for_snapshot(
    snapshot: BookSnapshot,
    *,
    live_pairs: Mapping[str, _LivePairState],
    config: LivePaperSwarmConfig,
) -> tuple[PairLabel, ...]:
    labels: list[PairLabel] = []
    for state in tuple(live_pairs.values()):
        if state.pair.instrument is not snapshot.instrument:
            continue
        label = _update_live_pair(state, snapshot, config=config)
        if label is not None:
            labels.append(label)
    return tuple(labels)


def _update_live_pair(
    state: _LivePairState,
    snapshot: BookSnapshot,
    *,
    config: LivePaperSwarmConfig,
) -> PairLabel | None:
    state.latest_snapshot = snapshot
    risk_reason = _risk_exit_reason(snapshot)
    age_seconds = (_as_utc(snapshot.timestamp) - state.entry.timestamp).total_seconds()
    if age_seconds >= config.max_pair_age_seconds:
        risk_reason = PairExitReason.END_OF_REPLAY

    long_pnl = _long_exit_ticks(state, snapshot)
    short_pnl = _short_exit_ticks(state, snapshot)
    pair_total = long_pnl + short_pnl
    state.max_favorable_excursion = max(state.max_favorable_excursion, pair_total)
    state.max_adverse_excursion = min(state.max_adverse_excursion, pair_total)

    if risk_reason is not None and state.runner_side is None:
        return _label(
            state,
            snapshot,
            loser_side=None,
            loser_loss_ticks=Decimal("0"),
            runner_side=None,
            runner_mfe_ticks=max(pair_total, Decimal("0")),
            runner_mae_ticks=min(pair_total, Decimal("0")),
            runner_exit_ticks=pair_total,
            pair_total_pnl_ticks=pair_total,
            exit_reason=risk_reason,
            fakeout_flag=pair_total < 0,
            runner_success_flag=False,
        )

    if state.runner_side is None:
        long_trigger = _long_trigger_ticks(state, snapshot)
        short_trigger = _short_trigger_ticks(state, snapshot)
        if long_trigger <= -state.stop and short_trigger <= -state.stop:
            return _label(
                state,
                snapshot,
                loser_side=None,
                loser_loss_ticks=abs(pair_total),
                runner_side=None,
                runner_mfe_ticks=Decimal("0"),
                runner_mae_ticks=pair_total,
                runner_exit_ticks=Decimal("0"),
                pair_total_pnl_ticks=pair_total,
                exit_reason=PairExitReason.FAILED_PAIR,
                fakeout_flag=True,
                runner_success_flag=False,
            )
        if short_trigger <= -state.stop:
            state.loser_side = LegSide.SHORT
            state.runner_side = LegSide.LONG
            state.loser_loss_ticks = abs(short_pnl)
            state.runner_mfe_ticks = max(long_pnl, Decimal("0"))
            state.runner_mae_ticks = min(long_pnl, Decimal("0"))
            return None
        if long_trigger <= -state.stop:
            state.loser_side = LegSide.LONG
            state.runner_side = LegSide.SHORT
            state.loser_loss_ticks = abs(long_pnl)
            state.runner_mfe_ticks = max(short_pnl, Decimal("0"))
            state.runner_mae_ticks = min(short_pnl, Decimal("0"))
            return None
        return None

    runner_pnl = _runner_exit_ticks(state, snapshot)
    state.runner_mfe_ticks = max(state.runner_mfe_ticks, runner_pnl)
    state.runner_mae_ticks = min(state.runner_mae_ticks, runner_pnl)
    pair_total = runner_pnl - state.loser_loss_ticks
    state.max_favorable_excursion = max(state.max_favorable_excursion, pair_total)
    state.max_adverse_excursion = min(state.max_adverse_excursion, pair_total)
    if runner_pnl >= Decimal(state.pair.breakeven_ticks):
        state.breakeven_active = True

    exit_reason: PairExitReason | None = risk_reason
    if exit_reason is None and state.breakeven_active and runner_pnl <= 0:
        exit_reason = PairExitReason.BREAKEVEN
        runner_pnl = Decimal("0")
        pair_total = -state.loser_loss_ticks
    if exit_reason is None and state.runner_mfe_ticks - runner_pnl >= Decimal("2"):
        exit_reason = PairExitReason.TRAILING_STOP
    if exit_reason is None and _microstructure_trailing_exit(snapshot, state.runner_side):
        exit_reason = PairExitReason.TRAILING_STOP

    if exit_reason is None:
        return None
    return _label(
        state,
        snapshot,
        loser_side=state.loser_side,
        loser_loss_ticks=state.loser_loss_ticks,
        runner_side=state.runner_side,
        runner_mfe_ticks=max(state.runner_mfe_ticks, Decimal("0")),
        runner_mae_ticks=min(state.runner_mae_ticks, Decimal("0")),
        runner_exit_ticks=runner_pnl,
        pair_total_pnl_ticks=pair_total,
        exit_reason=exit_reason,
        fakeout_flag=(
            pair_total < 0
            and state.runner_mfe_ticks < Decimal(state.pair.breakeven_ticks)
        ),
        runner_success_flag=runner_pnl >= Decimal(state.pair.breakeven_ticks),
    )


def _label(
    state: _LivePairState,
    snapshot: BookSnapshot,
    *,
    loser_side: LegSide | None,
    loser_loss_ticks: Decimal,
    runner_side: LegSide | None,
    runner_mfe_ticks: Decimal,
    runner_mae_ticks: Decimal,
    runner_exit_ticks: Decimal,
    pair_total_pnl_ticks: Decimal,
    exit_reason: PairExitReason,
    fakeout_flag: bool,
    runner_success_flag: bool,
) -> PairLabel:
    return PairLabel(
        pair_id=state.pair.pair_id,
        instrument=state.pair.instrument,
        entry_timestamp=state.entry.timestamp,
        exit_timestamp=snapshot.timestamp,
        stop_loss_ticks=state.pair.stop_loss_ticks,
        loser_side=loser_side,
        loser_loss_ticks=loser_loss_ticks,
        runner_side=runner_side,
        runner_mfe_ticks=runner_mfe_ticks,
        runner_mae_ticks=runner_mae_ticks,
        runner_exit_ticks=runner_exit_ticks,
        pair_total_pnl_ticks=pair_total_pnl_ticks,
        pair_total_pnl_rub=pair_total_pnl_ticks,
        exit_reason=exit_reason,
        max_adverse_excursion=min(state.max_adverse_excursion, pair_total_pnl_ticks),
        max_favorable_excursion=max(state.max_favorable_excursion, pair_total_pnl_ticks),
        fakeout_flag=fakeout_flag,
        runner_success_flag=runner_success_flag,
        entry_spread_cost_ticks=state.entry.spread_ticks,
        avg_slippage_ticks=state.slippage,
        latency_ms=snapshot.latency_ms,
        regime="live_paper",
        long_bot_id=state.pair.long_bot_id,
        short_bot_id=state.pair.short_bot_id,
    )


def _metrics(labels: Sequence[PairLabel], curator: CuratorBot) -> SwarmMetrics:
    return aggregate_pair_metrics(
        labels,
        bot_to_account=curator.bot_to_account,
        rejected_by_model=curator.rejections.get(RejectionReason.MODEL, 0),
        rejected_by_spread=curator.rejections.get(RejectionReason.SPREAD, 0),
        rejected_by_stale_book=curator.rejections.get(RejectionReason.STALE_BOOK, 0),
        rejected_by_chop=curator.rejections.get(RejectionReason.CHOP, 0),
        rejected_by_latency=curator.rejections.get(RejectionReason.LATENCY, 0),
    )


def _has_active_pair_for_instrument(
    live_pairs: Mapping[str, _LivePairState],
    instrument: SwarmInstrument,
) -> bool:
    return any(state.pair.instrument is instrument for state in live_pairs.values())


def _risk_exit_reason(snapshot: BookSnapshot) -> PairExitReason | None:
    cfg = HedgePairSimulationConfig()
    if snapshot.spread_ticks > cfg.max_spread_ticks:
        return PairExitReason.WIDE_SPREAD
    if snapshot.latency_ms > cfg.max_latency_ms:
        return PairExitReason.STALE_BOOK
    if (
        abs(snapshot.imbalance(3)) < Decimal("0.01")
        and abs(snapshot.tick_velocity) < Decimal("0.01")
    ):
        return PairExitReason.CHOP
    return None


def _long_exit_ticks(state: _LivePairState, snapshot: BookSnapshot) -> Decimal:
    exit_price = snapshot.best_bid - (state.slippage * snapshot.tick_size)
    return (exit_price - state.long_entry) / snapshot.tick_size


def _long_trigger_ticks(state: _LivePairState, snapshot: BookSnapshot) -> Decimal:
    return (snapshot.best_bid - state.long_entry) / snapshot.tick_size


def _short_exit_ticks(state: _LivePairState, snapshot: BookSnapshot) -> Decimal:
    exit_price = snapshot.best_ask + (state.slippage * snapshot.tick_size)
    return (state.short_entry - exit_price) / snapshot.tick_size


def _short_trigger_ticks(state: _LivePairState, snapshot: BookSnapshot) -> Decimal:
    return (state.short_entry - snapshot.best_ask) / snapshot.tick_size


def _runner_exit_ticks(state: _LivePairState, snapshot: BookSnapshot) -> Decimal:
    if state.runner_side is LegSide.LONG:
        return _long_exit_ticks(state, snapshot)
    return _short_exit_ticks(state, snapshot)


def _microstructure_trailing_exit(snapshot: BookSnapshot, runner_side: LegSide | None) -> bool:
    if runner_side is LegSide.LONG:
        return snapshot.imbalance(1) < Decimal("-0.35") or snapshot.microprice < snapshot.mid_price
    if runner_side is LegSide.SHORT:
        return snapshot.imbalance(1) > Decimal("0.35") or snapshot.microprice > snapshot.mid_price
    return False


def _write_live_dashboard_state(
    path: Path,
    *,
    curator: CuratorBot,
    latest_snapshots: Sequence[BookSnapshot],
    metrics: SwarmMetrics,
    updated_at: datetime,
) -> Path:
    payload = build_swarm_dashboard_state(
        curator=curator,
        latest_snapshots=latest_snapshots,
        metrics=metrics,
        updated_at=updated_at,
        commit_hash=get_runtime_commit_hash(),
    )
    payload["runtime_mode"] = "live-paper"
    payload["market_data_source"] = "tbank-readonly-rest"
    swarm = payload.get("swarm")
    if isinstance(swarm, dict):
        curator_payload = swarm.get("curator")
        if isinstance(curator_payload, dict):
            curator_payload["status"] = "LIVE_PAPER_COORDINATOR"
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return path


def _write_summary_json(
    path: Path,
    *,
    cycle: int,
    metrics: SwarmMetrics,
    active_pairs: Mapping[str, _LivePairState],
    latest_snapshots: Mapping[SwarmInstrument, BookSnapshot],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "commit_hash": get_runtime_commit_hash(),
        "cycle": cycle,
        "runtime_mode": "live-paper",
        "market_data_source": "tbank-readonly-rest",
        "active_pairs": len(active_pairs),
        "closed_pairs": metrics.total_pairs,
        "total_pnl_ticks": str(sum(metrics.pnl_by_bot.values(), Decimal("0"))),
        "latest_market": {
            instrument.value: {
                "timestamp": snapshot.timestamp.isoformat(),
                "best_bid": str(snapshot.best_bid),
                "best_ask": str(snapshot.best_ask),
                "spread_ticks": str(snapshot.spread_ticks),
                "imbalance_3": str(snapshot.imbalance(3)),
            }
            for instrument, snapshot in latest_snapshots.items()
        },
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _write_heartbeat(path: Path, cycle: LivePaperSwarmCycle) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            [
                "mode=live-paper",
                "market_data_source=tbank-readonly-rest",
                f"status={cycle.status}",
                f"cycle={cycle.cycle}",
                f"started_at={cycle.started_at.isoformat()}",
                f"finished_at={cycle.finished_at.isoformat()}",
                f"snapshots={cycle.snapshots}",
                f"active_pairs={cycle.active_pairs}",
                f"closed_pairs={cycle.closed_pairs}",
                f"total_pnl_ticks={cycle.total_pnl_ticks}",
                f"error={cycle.error or ''}",
            ]
        ),
        encoding="utf-8",
    )


def _write_error_log(reports_dir: Path, exc: Exception) -> None:
    reports_dir.mkdir(parents=True, exist_ok=True)
    error_path = reports_dir / "live_paper_error.log"
    error_path.write_text(
        f"{datetime.now(UTC).isoformat()} {type(exc).__name__}: {exc}\n"
        f"{traceback.format_exc()}",
        encoding="utf-8",
    )


def _assert_safe_environment() -> None:
    live_flags = {
        "LIVE_TRADING_ENABLED": os.environ.get("LIVE_TRADING_ENABLED", "false"),
        "NEO_TRADER_LIVE_TRADING_ENABLED": os.environ.get(
            "NEO_TRADER_LIVE_TRADING_ENABLED",
            "false",
        ),
    }
    enabled = [key for key, value in live_flags.items() if value.strip().lower() == "true"]
    if enabled:
        joined = ", ".join(enabled)
        raise RuntimeError(f"live paper swarm never submits orders; live flags enabled: {joined}")

    trading_mode = os.environ.get("TRADING_MODE", "readonly").strip().lower()
    neo_trading_mode = os.environ.get("NEO_TRADER_TRADING_MODE", "readonly").strip().lower()
    if trading_mode != "readonly" or neo_trading_mode != "readonly":
        raise RuntimeError(
            "live paper swarm requires TRADING_MODE and NEO_TRADER_TRADING_MODE readonly."
        )


def _as_utc(value: datetime) -> datetime:
    return as_utc(value)


__all__ = [
    "LivePaperSwarmConfig",
    "LivePaperSwarmCycle",
    "OrderBookProvider",
    "orderbook_snapshot_to_book_snapshot",
    "run_live_paper_swarm",
]
