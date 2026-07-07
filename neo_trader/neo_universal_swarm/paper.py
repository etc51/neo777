"""Paper simulation runner for the Neo Universal Bot Swarm."""

from __future__ import annotations

import csv
import json
import random
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from neo_trader.neo_universal_swarm.bots import CuratorBot
from neo_trader.neo_universal_swarm.config import (
    DEFAULT_ACCOUNTS_CONFIG,
    SwarmAccountsConfig,
    load_accounts_config,
)
from neo_trader.neo_universal_swarm.dashboard import write_swarm_dashboard_state
from neo_trader.neo_universal_swarm.data import JsonlEventStore, OrderBookTradeCollector
from neo_trader.neo_universal_swarm.model import PairEVModel, PairEVModelConfig
from neo_trader.neo_universal_swarm.simulator import (
    HedgePairSimulationConfig,
    HedgePairSimulator,
    aggregate_pair_metrics,
)
from neo_trader.neo_universal_swarm.types import (
    BookSnapshot,
    LegSide,
    PairLabel,
    RejectionReason,
    SwarmInstrument,
    SwarmMetrics,
)
from neo_trader.runtime import get_runtime_commit_hash


@dataclass(frozen=True)
class SyntheticScenario:
    """One generated pair opportunity and future replay path."""

    entry: BookSnapshot
    future: tuple[BookSnapshot, ...]
    regime: str


@dataclass(frozen=True)
class PaperSimulationArtifacts:
    """Files written by a paper simulation."""

    summary_json_path: Path
    labels_csv_path: Path
    dashboard_state_path: Path
    events_jsonl_path: Path


@dataclass(frozen=True)
class PaperSimulationResult:
    """Paper simulation result and artifacts."""

    metrics: SwarmMetrics
    labels: tuple[PairLabel, ...]
    curator: CuratorBot
    artifacts: PaperSimulationArtifacts | None
    stress_metrics: dict[str, SwarmMetrics]
    commit_hash: str


def run_paper_simulation(
    *,
    accounts_path: Path | str = DEFAULT_ACCOUNTS_CONFIG,
    target_pairs: int = 3200,
    min_required_ev_ticks: Decimal = Decimal("1"),
    slippage_stress_ticks: Decimal = Decimal("0"),
    seed: int = 777,
    reports_dir: Path | str = Path("data/reports/neo_universal_swarm"),
    dashboard_state_path: Path | str = Path(
        "data/monitoring/neo_universal_swarm_dashboard_state.json"
    ),
    write_artifacts: bool = True,
    include_stress_grid: bool = True,
) -> PaperSimulationResult:
    """Run deterministic paper simulation and optionally write reports."""

    if target_pairs <= 0:
        raise ValueError("target_pairs must be positive.")
    accounts = load_accounts_config(accounts_path)
    result = _simulate_once(
        accounts=accounts,
        target_pairs=target_pairs,
        min_required_ev_ticks=min_required_ev_ticks,
        slippage_stress_ticks=slippage_stress_ticks,
        seed=seed,
        event_store_path=Path(reports_dir) / "events.jsonl" if write_artifacts else None,
    )
    stress_metrics: dict[str, SwarmMetrics] = {}
    if include_stress_grid:
        for stress in (Decimal("0"), Decimal("1"), Decimal("2")):
            stress_key = f"slippage_{stress}_ticks"
            if stress == slippage_stress_ticks:
                stress_metrics[stress_key] = result.metrics
            else:
                stress_metrics[stress_key] = _simulate_once(
                    accounts=accounts,
                    target_pairs=target_pairs,
                    min_required_ev_ticks=min_required_ev_ticks,
                    slippage_stress_ticks=stress,
                    seed=seed,
                    event_store_path=None,
                ).metrics

    artifacts: PaperSimulationArtifacts | None = None
    if write_artifacts:
        reports_path = Path(reports_dir)
        reports_path.mkdir(parents=True, exist_ok=True)
        labels_csv_path = reports_path / "pair_labels.csv"
        summary_json_path = reports_path / "summary.json"
        events_jsonl_path = reports_path / "events.jsonl"
        latest_snapshots = _latest_snapshots_by_instrument(
            tuple(scenario.entry for scenario in _scenario_sample(seed=seed))
        )
        dashboard_path = write_swarm_dashboard_state(
            dashboard_state_path,
            curator=result.curator,
            latest_snapshots=latest_snapshots,
            metrics=result.metrics,
            updated_at=datetime.now(UTC),
            commit_hash=result.commit_hash,
        )
        _write_labels_csv(labels_csv_path, result.labels)
        _write_summary_json(
            summary_json_path,
            result=result,
            stress_metrics=stress_metrics,
            min_required_ev_ticks=min_required_ev_ticks,
            slippage_stress_ticks=slippage_stress_ticks,
        )
        artifacts = PaperSimulationArtifacts(
            summary_json_path=summary_json_path,
            labels_csv_path=labels_csv_path,
            dashboard_state_path=dashboard_path,
            events_jsonl_path=events_jsonl_path,
        )
    return PaperSimulationResult(
        metrics=result.metrics,
        labels=result.labels,
        curator=result.curator,
        artifacts=artifacts,
        stress_metrics=stress_metrics,
        commit_hash=result.commit_hash,
    )


def _simulate_once(
    *,
    accounts: SwarmAccountsConfig,
    target_pairs: int,
    min_required_ev_ticks: Decimal,
    slippage_stress_ticks: Decimal,
    seed: int,
    event_store_path: Path | None,
) -> PaperSimulationResult:
    event_store = None if event_store_path is None else JsonlEventStore(event_store_path)
    collector = OrderBookTradeCollector(event_store=event_store)
    model = PairEVModel(
        PairEVModelConfig(
            min_required_ev_ticks=min_required_ev_ticks,
            slippage_stress_ticks=slippage_stress_ticks,
        )
    )
    curator = CuratorBot(accounts=accounts, model=model, event_store=event_store)
    labels: list[PairLabel] = []
    generator = SyntheticScenarioGenerator(seed=seed)
    attempts = 0
    max_attempts = target_pairs * 3
    while len(labels) < target_pairs and attempts < max_attempts:
        attempts += 1
        scenario = generator.next_scenario(index=attempts)
        collector.observe_snapshot(scenario.entry)
        active_pair = curator.maybe_open_pair(scenario.entry)
        if active_pair is None:
            continue
        simulator = HedgePairSimulator(
            HedgePairSimulationConfig(
                stop_loss_ticks=active_pair.stop_loss_ticks,
                breakeven_ticks=active_pair.breakeven_ticks,
                slippage_stress_ticks=slippage_stress_ticks,
            )
        )
        label = simulator.simulate(
            pair_id=active_pair.pair_id,
            entry=scenario.entry,
            future=scenario.future,
            long_bot_id=active_pair.long_bot_id,
            short_bot_id=active_pair.short_bot_id,
            regime=scenario.regime,
        )
        curator.settle_pair(label)
        curator.release_cooldowns()
        labels.append(label)

    metrics = aggregate_pair_metrics(
        labels,
        bot_to_account=curator.bot_to_account,
        rejected_by_model=curator.rejections.get(RejectionReason.MODEL, 0),
        rejected_by_spread=curator.rejections.get(RejectionReason.SPREAD, 0),
        rejected_by_stale_book=curator.rejections.get(RejectionReason.STALE_BOOK, 0),
        rejected_by_chop=curator.rejections.get(RejectionReason.CHOP, 0),
        rejected_by_latency=curator.rejections.get(RejectionReason.LATENCY, 0),
    )
    return PaperSimulationResult(
        metrics=metrics,
        labels=tuple(labels),
        curator=curator,
        artifacts=None,
        stress_metrics={},
        commit_hash=get_runtime_commit_hash(),
    )


class SyntheticScenarioGenerator:
    """Deterministic synthetic replay generator for phase-1 smoke testing."""

    def __init__(self, *, seed: int) -> None:
        self._random = random.Random(seed)
        self._start = datetime(2026, 7, 7, 7, 0, tzinfo=UTC)

    def next_scenario(self, *, index: int) -> SyntheticScenario:
        instrument = (
            SwarmInstrument.NEOBITOK if index % 2 == 0 else SwarmInstrument.NEOEFIR
        )
        regime_roll = self._random.random()
        if regime_roll < 0.68:
            regime = "continuation"
        elif regime_roll < 0.86:
            regime = "fakeout"
        elif regime_roll < 0.94:
            regime = "chop"
        elif regime_roll < 0.98:
            regime = "wide_spread"
        else:
            regime = "latency"
        direction = LegSide.LONG if self._random.random() >= 0.5 else LegSide.SHORT
        base = Decimal("10000") + Decimal(index % 500)
        spread = Decimal("4") if regime == "wide_spread" else Decimal("1")
        timestamp = self._start + timedelta(seconds=index * 3)
        latency_ms = 650 if regime == "latency" else self._random.choice([0, 100, 250, 500])
        entry = _snapshot(
            timestamp=timestamp,
            instrument=instrument,
            base_bid=base,
            spread=spread,
            direction=direction,
            regime=regime,
            tick_velocity=_velocity_for(regime, direction),
            latency_ms=latency_ms,
        )
        future = _future_path(
            entry=entry,
            direction=direction,
            regime=regime,
            spread=spread,
        )
        return SyntheticScenario(entry=entry, future=future, regime=regime)


def _future_path(
    *,
    entry: BookSnapshot,
    direction: LegSide,
    regime: str,
    spread: Decimal,
) -> tuple[BookSnapshot, ...]:
    moves: tuple[int, ...]
    if regime == "fakeout":
        moves = (
            (1, 2, 1, -1, -2, -3)
            if direction is LegSide.LONG
            else (-1, -2, -1, 1, 2, 3)
        )
    elif regime == "chop":
        moves = (0, 1, 0, -1, 0, 1)
    elif regime == "wide_spread":
        moves = (0, 1, 2)
    else:
        moves = (
            (1, 2, 5, 9, 13, 10)
            if direction is LegSide.LONG
            else (-1, -2, -5, -9, -13, -10)
        )
    snapshots: list[BookSnapshot] = []
    for offset, move in enumerate(moves, start=1):
        current_direction = direction
        if regime == "fakeout" and offset >= 4:
            current_direction = LegSide.SHORT if direction is LegSide.LONG else LegSide.LONG
        base_bid = entry.best_bid + Decimal(move)
        snapshots.append(
            _snapshot(
                timestamp=entry.timestamp + timedelta(seconds=offset),
                instrument=entry.instrument,
                base_bid=base_bid,
                spread=spread,
                direction=current_direction,
                regime=regime,
                tick_velocity=Decimal(move) / Decimal(offset),
                latency_ms=entry.latency_ms,
            )
        )
    return tuple(snapshots)


def _snapshot(
    *,
    timestamp: datetime,
    instrument: SwarmInstrument,
    base_bid: Decimal,
    spread: Decimal,
    direction: LegSide,
    regime: str,
    tick_velocity: Decimal,
    latency_ms: int,
) -> BookSnapshot:
    if regime == "chop":
        bid_top = Decimal("105")
        ask_top = Decimal("100")
    elif direction is LegSide.LONG:
        bid_top = Decimal("220")
        ask_top = Decimal("45")
    else:
        bid_top = Decimal("45")
        ask_top = Decimal("220")
    bids = (
        (base_bid, bid_top),
        (base_bid - Decimal("1"), bid_top * Decimal("0.70")),
        (base_bid - Decimal("2"), bid_top * Decimal("0.45")),
        (base_bid - Decimal("3"), bid_top * Decimal("0.30")),
        (base_bid - Decimal("4"), bid_top * Decimal("0.20")),
    )
    asks = (
        (base_bid + spread, ask_top),
        (base_bid + spread + Decimal("1"), ask_top * Decimal("0.70")),
        (base_bid + spread + Decimal("2"), ask_top * Decimal("0.45")),
        (base_bid + spread + Decimal("3"), ask_top * Decimal("0.30")),
        (base_bid + spread + Decimal("4"), ask_top * Decimal("0.20")),
    )
    return BookSnapshot.from_levels(
        timestamp=timestamp,
        instrument=instrument,
        bids=bids,
        asks=asks,
        tick_size=Decimal("1"),
        last_price=base_bid + (spread / Decimal("2")),
        last_trade_size=Decimal("1"),
        trade_side=direction,
        tick_direction=1 if tick_velocity > 0 else -1 if tick_velocity < 0 else 0,
        tick_velocity=tick_velocity,
        volume_delta_1s=bid_top - ask_top,
        volume_delta_5s=(bid_top - ask_top) * Decimal("2"),
        volume_delta_15s=(bid_top - ask_top) * Decimal("3"),
        volatility_5s=Decimal("1.2") if regime != "chop" else Decimal("0.2"),
        volatility_15s=Decimal("1.4") if regime != "chop" else Decimal("0.2"),
        volatility_60s=Decimal("1.8") if regime != "chop" else Decimal("0.4"),
        latency_ms=latency_ms,
    )


def _velocity_for(regime: str, direction: LegSide) -> Decimal:
    if regime == "chop":
        return Decimal("0")
    sign = Decimal("1") if direction is LegSide.LONG else Decimal("-1")
    return sign * Decimal("0.75")


def _scenario_sample(*, seed: int) -> tuple[SyntheticScenario, ...]:
    generator = SyntheticScenarioGenerator(seed=seed)
    return (generator.next_scenario(index=1), generator.next_scenario(index=2))


def _latest_snapshots_by_instrument(snapshots: Sequence[BookSnapshot]) -> tuple[BookSnapshot, ...]:
    latest: dict[SwarmInstrument, BookSnapshot] = {}
    for snapshot in snapshots:
        latest[snapshot.instrument] = snapshot
    return tuple(latest[instrument] for instrument in sorted(latest, key=lambda item: item.value))


def _write_labels_csv(path: Path, labels: Sequence[PairLabel]) -> None:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "pair_id",
                "instrument",
                "entry_timestamp",
                "exit_timestamp",
                "stop_loss_ticks",
                "loser_side",
                "loser_loss_ticks",
                "runner_side",
                "runner_mfe_ticks",
                "runner_mae_ticks",
                "runner_exit_ticks",
                "pair_total_pnl_ticks",
                "pair_total_pnl_rub",
                "exit_reason",
                "fakeout_flag",
                "runner_success_flag",
                "regime",
            ]
        )
        for label in labels:
            writer.writerow(
                [
                    label.pair_id,
                    label.instrument.value,
                    label.entry_timestamp.isoformat(),
                    label.exit_timestamp.isoformat(),
                    label.stop_loss_ticks,
                    None if label.loser_side is None else label.loser_side.value,
                    str(label.loser_loss_ticks),
                    None if label.runner_side is None else label.runner_side.value,
                    str(label.runner_mfe_ticks),
                    str(label.runner_mae_ticks),
                    str(label.runner_exit_ticks),
                    str(label.pair_total_pnl_ticks),
                    str(label.pair_total_pnl_rub),
                    label.exit_reason.value,
                    label.fakeout_flag,
                    label.runner_success_flag,
                    label.regime,
                ]
            )


def _write_summary_json(
    path: Path,
    *,
    result: PaperSimulationResult,
    stress_metrics: dict[str, SwarmMetrics],
    min_required_ev_ticks: Decimal,
    slippage_stress_ticks: Decimal,
) -> None:
    summary = {
        "commit_hash": result.commit_hash,
        "target": {
            "min_required_ev_ticks": str(min_required_ev_ticks),
            "slippage_stress_ticks": str(slippage_stress_ticks),
        },
        "metrics": _metrics_json(result.metrics),
        "ev_by_instrument_ticks": _ev_by_instrument(result.labels),
        "stress_metrics": {key: _metrics_json(metrics) for key, metrics in stress_metrics.items()},
        "readiness": {
            "next_research_phase": _ready_for_next_research_phase(result.metrics, stress_metrics),
            "live_phase": False,
            "live_phase_reason": (
                "live_enabled remains false and requires a separate manual approval."
            ),
        },
    }
    path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _metrics_json(metrics: SwarmMetrics) -> dict[str, object]:
    return {
        "total_pairs": metrics.total_pairs,
        "profitable_pairs": metrics.profitable_pairs,
        "losing_pairs": metrics.losing_pairs,
        "pair_winrate": str(metrics.pair_winrate),
        "pair_ev_ticks": str(metrics.pair_ev_ticks),
        "pair_ev_rub": str(metrics.pair_ev_rub),
        "avg_runner_profit_ticks": str(metrics.avg_runner_profit_ticks),
        "avg_loser_loss_ticks": str(metrics.avg_loser_loss_ticks),
        "avg_spread_cost_ticks": str(metrics.avg_spread_cost_ticks),
        "avg_slippage_ticks": str(metrics.avg_slippage_ticks),
        "fakeout_rate": str(metrics.fakeout_rate),
        "runner_to_breakeven_rate": str(metrics.runner_to_breakeven_rate),
        "runner_trailing_capture_ratio": str(metrics.runner_trailing_capture_ratio),
        "profit_factor": str(metrics.profit_factor),
        "max_drawdown": str(metrics.max_drawdown),
        "pnl_by_bot": {key: str(value) for key, value in metrics.pnl_by_bot.items()},
        "pnl_by_account": {key: str(value) for key, value in metrics.pnl_by_account.items()},
        "pnl_by_instrument": {
            key: str(value) for key, value in metrics.pnl_by_instrument.items()
        },
        "pnl_by_regime": {key: str(value) for key, value in metrics.pnl_by_regime.items()},
        "rejected_by_model": metrics.rejected_by_model,
        "rejected_by_spread": metrics.rejected_by_spread,
        "rejected_by_stale_book": metrics.rejected_by_stale_book,
        "rejected_by_chop": metrics.rejected_by_chop,
        "rejected_by_latency": metrics.rejected_by_latency,
    }


def _ev_by_instrument(labels: Sequence[PairLabel]) -> dict[str, str]:
    totals: dict[str, Decimal] = {}
    counts: dict[str, int] = {}
    for label in labels:
        key = label.instrument.value
        totals[key] = totals.get(key, Decimal("0")) + label.pair_total_pnl_ticks
        counts[key] = counts.get(key, 0) + 1
    return {key: str(totals[key] / Decimal(counts[key])) for key in totals}


def _ready_for_next_research_phase(
    metrics: SwarmMetrics,
    stress_metrics: dict[str, SwarmMetrics],
) -> bool:
    slippage_one = stress_metrics.get("slippage_1_ticks")
    return (
        metrics.total_pairs >= 3000
        and metrics.pair_ev_ticks > 0
        and slippage_one is not None
        and slippage_one.pair_ev_ticks > 0
        and metrics.profit_factor > Decimal("1.10")
        and any(pnl > 0 for pnl in metrics.pnl_by_instrument.values())
    )


__all__ = [
    "PaperSimulationArtifacts",
    "PaperSimulationResult",
    "SyntheticScenario",
    "SyntheticScenarioGenerator",
    "run_paper_simulation",
]
