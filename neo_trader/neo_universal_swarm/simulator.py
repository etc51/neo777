"""Event-driven hedge-pair simulator for paper/research runs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import time
from decimal import Decimal

from neo_trader.neo_universal_swarm.types import (
    DEFAULT_TICK_VALUE_RUB,
    BookSnapshot,
    LegSide,
    PairExitReason,
    PairLabel,
    SwarmMetrics,
    decimal_ratio,
)


@dataclass(frozen=True)
class HedgePairSimulationConfig:
    """Hedge-pair replay parameters."""

    stop_loss_ticks: int = 2
    breakeven_ticks: int = 2
    trailing_retrace_ticks: Decimal = Decimal("2")
    max_spread_ticks: Decimal = Decimal("3")
    max_latency_ms: int = 500
    slippage_stress_ticks: Decimal = Decimal("0")
    tick_value_rub: Decimal = DEFAULT_TICK_VALUE_RUB
    cutoff: time = time(20, 45)

    def __post_init__(self) -> None:
        if self.stop_loss_ticks not in {2, 3}:
            raise ValueError("stop_loss_ticks must be 2 or 3.")
        if self.breakeven_ticks != self.stop_loss_ticks:
            raise ValueError("breakeven_ticks must equal stop_loss_ticks.")
        if self.trailing_retrace_ticks <= 0:
            raise ValueError("trailing_retrace_ticks must be positive.")
        if self.max_spread_ticks <= 0:
            raise ValueError("max_spread_ticks must be positive.")
        if self.max_latency_ms < 0:
            raise ValueError("max_latency_ms must be non-negative.")
        if self.slippage_stress_ticks < 0:
            raise ValueError("slippage_stress_ticks must be non-negative.")
        if self.tick_value_rub <= 0:
            raise ValueError("tick_value_rub must be positive.")


class HedgePairSimulator:
    """Simulate one simultaneous long/short pair from book snapshots."""

    def __init__(self, config: HedgePairSimulationConfig | None = None) -> None:
        self.config = config or HedgePairSimulationConfig()

    def simulate(
        self,
        *,
        pair_id: str,
        entry: BookSnapshot,
        future: Sequence[BookSnapshot],
        long_bot_id: str | None = None,
        short_bot_id: str | None = None,
        regime: str = "unknown",
    ) -> PairLabel:
        """Replay a pair from an entry snapshot through future snapshots."""

        if not future:
            raise ValueError("future snapshots must not be empty.")
        path = tuple(sorted(future, key=lambda snapshot: snapshot.timestamp))
        stop = Decimal(self.config.stop_loss_ticks)
        slippage = entry.slippage_ticks + self.config.slippage_stress_ticks
        tick = entry.tick_size
        long_entry = entry.best_ask + (slippage * tick)
        short_entry = entry.best_bid - (slippage * tick)

        loser_side: LegSide | None = None
        runner_side: LegSide | None = None
        loser_loss_ticks = Decimal("0")
        runner_exit = Decimal("0")
        max_favorable = Decimal("-999999")
        max_adverse = Decimal("999999")
        exit_reason = PairExitReason.END_OF_REPLAY
        exit_snapshot = path[-1]

        for snapshot in path:
            risk_reason = self._risk_exit_reason(snapshot)
            long_pnl = self._long_exit_ticks(long_entry, snapshot, slippage)
            short_pnl = self._short_exit_ticks(short_entry, snapshot, slippage)
            long_trigger_pnl = self._long_trigger_ticks(long_entry, snapshot)
            short_trigger_pnl = self._short_trigger_ticks(short_entry, snapshot)
            total_pnl = long_pnl + short_pnl
            max_favorable = max(max_favorable, total_pnl)
            max_adverse = min(max_adverse, total_pnl)

            if risk_reason is not None:
                exit_snapshot = snapshot
                exit_reason = risk_reason
                runner_exit = total_pnl
                return self._label(
                    pair_id=pair_id,
                    entry=entry,
                    exit_snapshot=exit_snapshot,
                    loser_side=None,
                    loser_loss_ticks=Decimal("0"),
                    runner_side=None,
                    runner_mfe_ticks=max(total_pnl, Decimal("0")),
                    runner_mae_ticks=min(total_pnl, Decimal("0")),
                    runner_exit_ticks=runner_exit,
                    pair_total_pnl_ticks=total_pnl,
                    exit_reason=exit_reason,
                    max_adverse_excursion=max_adverse,
                    max_favorable_excursion=max_favorable,
                    fakeout_flag=total_pnl < 0,
                    runner_success_flag=False,
                    avg_slippage_ticks=slippage,
                    long_bot_id=long_bot_id,
                    short_bot_id=short_bot_id,
                    regime=regime,
                )

            if long_trigger_pnl <= -stop and short_trigger_pnl <= -stop:
                exit_snapshot = snapshot
                exit_reason = PairExitReason.FAILED_PAIR
                pair_total = long_pnl + short_pnl
                return self._label(
                    pair_id=pair_id,
                    entry=entry,
                    exit_snapshot=exit_snapshot,
                    loser_side=None,
                    loser_loss_ticks=abs(pair_total),
                    runner_side=None,
                    runner_mfe_ticks=Decimal("0"),
                    runner_mae_ticks=pair_total,
                    runner_exit_ticks=Decimal("0"),
                    pair_total_pnl_ticks=pair_total,
                    exit_reason=exit_reason,
                    max_adverse_excursion=min(max_adverse, pair_total),
                    max_favorable_excursion=max(max_favorable, pair_total),
                    fakeout_flag=True,
                    runner_success_flag=False,
                    avg_slippage_ticks=slippage,
                    long_bot_id=long_bot_id,
                    short_bot_id=short_bot_id,
                    regime=regime,
                )

            if short_trigger_pnl <= -stop:
                loser_side = LegSide.SHORT
                runner_side = LegSide.LONG
                loser_loss_ticks = abs(short_pnl)
                return self._simulate_runner(
                    pair_id=pair_id,
                    entry=entry,
                    path=path,
                    start_snapshot=snapshot,
                    long_entry=long_entry,
                    short_entry=short_entry,
                    slippage=slippage,
                    loser_side=loser_side,
                    loser_loss_ticks=loser_loss_ticks,
                    runner_side=runner_side,
                    max_adverse=max_adverse,
                    max_favorable=max_favorable,
                    long_bot_id=long_bot_id,
                    short_bot_id=short_bot_id,
                    regime=regime,
                )
            if long_trigger_pnl <= -stop:
                loser_side = LegSide.LONG
                runner_side = LegSide.SHORT
                loser_loss_ticks = abs(long_pnl)
                return self._simulate_runner(
                    pair_id=pair_id,
                    entry=entry,
                    path=path,
                    start_snapshot=snapshot,
                    long_entry=long_entry,
                    short_entry=short_entry,
                    slippage=slippage,
                    loser_side=loser_side,
                    loser_loss_ticks=loser_loss_ticks,
                    runner_side=runner_side,
                    max_adverse=max_adverse,
                    max_favorable=max_favorable,
                    long_bot_id=long_bot_id,
                    short_bot_id=short_bot_id,
                    regime=regime,
                )

        final = path[-1]
        long_pnl = self._long_exit_ticks(long_entry, final, slippage)
        short_pnl = self._short_exit_ticks(short_entry, final, slippage)
        pair_total = long_pnl + short_pnl
        return self._label(
            pair_id=pair_id,
            entry=entry,
            exit_snapshot=final,
            loser_side=None,
            loser_loss_ticks=Decimal("0"),
            runner_side=None,
            runner_mfe_ticks=max(pair_total, Decimal("0")),
            runner_mae_ticks=min(pair_total, Decimal("0")),
            runner_exit_ticks=pair_total,
            pair_total_pnl_ticks=pair_total,
            exit_reason=PairExitReason.END_OF_REPLAY,
            max_adverse_excursion=min(max_adverse, pair_total),
            max_favorable_excursion=max(max_favorable, pair_total),
            fakeout_flag=pair_total < 0,
            runner_success_flag=pair_total > 0,
            avg_slippage_ticks=slippage,
            long_bot_id=long_bot_id,
            short_bot_id=short_bot_id,
            regime=regime,
        )

    def _simulate_runner(
        self,
        *,
        pair_id: str,
        entry: BookSnapshot,
        path: Sequence[BookSnapshot],
        start_snapshot: BookSnapshot,
        long_entry: Decimal,
        short_entry: Decimal,
        slippage: Decimal,
        loser_side: LegSide,
        loser_loss_ticks: Decimal,
        runner_side: LegSide,
        max_adverse: Decimal,
        max_favorable: Decimal,
        long_bot_id: str | None,
        short_bot_id: str | None,
        regime: str,
    ) -> PairLabel:
        start_index = path.index(start_snapshot)
        breakeven_active = False
        runner_mfe = Decimal("-999999")
        runner_mae = Decimal("999999")
        runner_exit = Decimal("0")
        exit_snapshot = path[-1]
        exit_reason = PairExitReason.END_OF_REPLAY

        for snapshot in path[start_index:]:
            risk_reason = self._risk_exit_reason(snapshot)
            runner_pnl = self._runner_exit_ticks(
                runner_side=runner_side,
                long_entry=long_entry,
                short_entry=short_entry,
                snapshot=snapshot,
                slippage=slippage,
            )
            runner_mfe = max(runner_mfe, runner_pnl)
            runner_mae = min(runner_mae, runner_pnl)
            pair_total = runner_pnl - loser_loss_ticks
            max_favorable = max(max_favorable, pair_total)
            max_adverse = min(max_adverse, pair_total)

            if runner_pnl >= Decimal(self.config.breakeven_ticks):
                breakeven_active = True
            if risk_reason is not None:
                runner_exit = runner_pnl
                exit_snapshot = snapshot
                exit_reason = risk_reason
                break
            if snapshot.timestamp.time() >= self.config.cutoff:
                runner_exit = runner_pnl
                exit_snapshot = snapshot
                exit_reason = PairExitReason.CUTOFF
                break
            if breakeven_active and runner_pnl <= 0:
                runner_exit = Decimal("0")
                exit_snapshot = snapshot
                exit_reason = PairExitReason.BREAKEVEN
                break
            if runner_mfe - runner_pnl >= self.config.trailing_retrace_ticks:
                runner_exit = runner_pnl
                exit_snapshot = snapshot
                exit_reason = PairExitReason.TRAILING_STOP
                break
            if self._microstructure_trailing_exit(snapshot, runner_side):
                runner_exit = runner_pnl
                exit_snapshot = snapshot
                exit_reason = PairExitReason.TRAILING_STOP
                break
        else:
            final = path[-1]
            runner_exit = self._runner_exit_ticks(
                runner_side=runner_side,
                long_entry=long_entry,
                short_entry=short_entry,
                snapshot=final,
                slippage=slippage,
            )
            runner_mfe = max(runner_mfe, runner_exit)
            runner_mae = min(runner_mae, runner_exit)
            exit_snapshot = final

        pair_total_pnl = runner_exit - loser_loss_ticks
        return self._label(
            pair_id=pair_id,
            entry=entry,
            exit_snapshot=exit_snapshot,
            loser_side=loser_side,
            loser_loss_ticks=loser_loss_ticks,
            runner_side=runner_side,
            runner_mfe_ticks=max(runner_mfe, Decimal("0")),
            runner_mae_ticks=min(runner_mae, Decimal("0")),
            runner_exit_ticks=runner_exit,
            pair_total_pnl_ticks=pair_total_pnl,
            exit_reason=exit_reason,
            max_adverse_excursion=min(max_adverse, pair_total_pnl),
            max_favorable_excursion=max(max_favorable, pair_total_pnl),
            fakeout_flag=pair_total_pnl < 0 and runner_mfe < Decimal(self.config.breakeven_ticks),
            runner_success_flag=runner_exit >= Decimal(self.config.breakeven_ticks),
            avg_slippage_ticks=slippage,
            long_bot_id=long_bot_id,
            short_bot_id=short_bot_id,
            regime=regime,
        )

    def _label(
        self,
        *,
        pair_id: str,
        entry: BookSnapshot,
        exit_snapshot: BookSnapshot,
        loser_side: LegSide | None,
        loser_loss_ticks: Decimal,
        runner_side: LegSide | None,
        runner_mfe_ticks: Decimal,
        runner_mae_ticks: Decimal,
        runner_exit_ticks: Decimal,
        pair_total_pnl_ticks: Decimal,
        exit_reason: PairExitReason,
        max_adverse_excursion: Decimal,
        max_favorable_excursion: Decimal,
        fakeout_flag: bool,
        runner_success_flag: bool,
        avg_slippage_ticks: Decimal,
        long_bot_id: str | None,
        short_bot_id: str | None,
        regime: str,
    ) -> PairLabel:
        return PairLabel(
            pair_id=pair_id,
            instrument=entry.instrument,
            entry_timestamp=entry.timestamp,
            exit_timestamp=exit_snapshot.timestamp,
            stop_loss_ticks=self.config.stop_loss_ticks,
            loser_side=loser_side,
            loser_loss_ticks=loser_loss_ticks,
            runner_side=runner_side,
            runner_mfe_ticks=runner_mfe_ticks,
            runner_mae_ticks=runner_mae_ticks,
            runner_exit_ticks=runner_exit_ticks,
            pair_total_pnl_ticks=pair_total_pnl_ticks,
            pair_total_pnl_rub=pair_total_pnl_ticks * self.config.tick_value_rub,
            exit_reason=exit_reason,
            max_adverse_excursion=max_adverse_excursion,
            max_favorable_excursion=max_favorable_excursion,
            fakeout_flag=fakeout_flag,
            runner_success_flag=runner_success_flag,
            entry_spread_cost_ticks=entry.spread_ticks,
            avg_slippage_ticks=avg_slippage_ticks,
            latency_ms=entry.latency_ms,
            regime=regime,
            long_bot_id=long_bot_id,
            short_bot_id=short_bot_id,
        )

    def _risk_exit_reason(self, snapshot: BookSnapshot) -> PairExitReason | None:
        if snapshot.spread_ticks > self.config.max_spread_ticks:
            return PairExitReason.WIDE_SPREAD
        if snapshot.latency_ms > self.config.max_latency_ms:
            return PairExitReason.STALE_BOOK
        if (
            abs(snapshot.imbalance(3)) < Decimal("0.01")
            and abs(snapshot.tick_velocity) < Decimal("0.01")
        ):
            return PairExitReason.CHOP
        return None

    def _microstructure_trailing_exit(self, snapshot: BookSnapshot, runner_side: LegSide) -> bool:
        if runner_side is LegSide.LONG:
            return (
                snapshot.imbalance(1) < Decimal("-0.35")
                or snapshot.microprice < snapshot.mid_price
            )
        return snapshot.imbalance(1) > Decimal("0.35") or snapshot.microprice > snapshot.mid_price

    def _long_exit_ticks(
        self,
        long_entry: Decimal,
        snapshot: BookSnapshot,
        slippage: Decimal,
    ) -> Decimal:
        exit_price = snapshot.best_bid - (slippage * snapshot.tick_size)
        return (exit_price - long_entry) / snapshot.tick_size

    def _long_trigger_ticks(self, long_entry: Decimal, snapshot: BookSnapshot) -> Decimal:
        return (snapshot.best_bid - long_entry) / snapshot.tick_size

    def _short_exit_ticks(
        self,
        short_entry: Decimal,
        snapshot: BookSnapshot,
        slippage: Decimal,
    ) -> Decimal:
        exit_price = snapshot.best_ask + (slippage * snapshot.tick_size)
        return (short_entry - exit_price) / snapshot.tick_size

    def _short_trigger_ticks(self, short_entry: Decimal, snapshot: BookSnapshot) -> Decimal:
        return (short_entry - snapshot.best_ask) / snapshot.tick_size

    def _runner_exit_ticks(
        self,
        *,
        runner_side: LegSide,
        long_entry: Decimal,
        short_entry: Decimal,
        snapshot: BookSnapshot,
        slippage: Decimal,
    ) -> Decimal:
        if runner_side is LegSide.LONG:
            return self._long_exit_ticks(long_entry, snapshot, slippage)
        return self._short_exit_ticks(short_entry, snapshot, slippage)


def aggregate_pair_metrics(
    labels: Sequence[PairLabel],
    *,
    bot_to_account: Mapping[str, str] | None = None,
    tick_value_rub: Decimal = DEFAULT_TICK_VALUE_RUB,
    rejected_by_model: int = 0,
    rejected_by_spread: int = 0,
    rejected_by_spread_entry_gate: int = 0,
    rejected_by_stale_book: int = 0,
    rejected_by_chop: int = 0,
    rejected_by_latency: int = 0,
) -> SwarmMetrics:
    """Aggregate requested swarm metrics from pair labels."""

    total_pairs = len(labels)
    profits = [label.pair_total_pnl_ticks for label in labels if label.pair_total_pnl_ticks > 0]
    losses = [label.pair_total_pnl_ticks for label in labels if label.pair_total_pnl_ticks < 0]
    gross_profit = sum(profits, Decimal("0"))
    gross_loss = sum(losses, Decimal("0"))
    runner_mfe_total = sum((label.runner_mfe_ticks for label in labels), Decimal("0"))
    runner_exit_positive_total = sum(
        (max(label.runner_exit_ticks, Decimal("0")) for label in labels),
        Decimal("0"),
    )
    pnl_by_bot = _pnl_by_bot(labels)
    account_map = bot_to_account or {}
    pnl_by_account: dict[str, Decimal] = {}
    for bot_id, pnl in pnl_by_bot.items():
        account_ref = account_map.get(bot_id, bot_id)
        pnl_by_account[account_ref] = pnl_by_account.get(account_ref, Decimal("0")) + pnl

    return SwarmMetrics(
        total_pairs=total_pairs,
        profitable_pairs=len(profits),
        losing_pairs=len(losses),
        pair_winrate=decimal_ratio(Decimal(len(profits)), Decimal(total_pairs)),
        pair_ev_ticks=decimal_ratio(
            sum((label.pair_total_pnl_ticks for label in labels), Decimal("0")),
            Decimal(total_pairs),
        ),
        pair_ev_rub=decimal_ratio(
            sum((label.pair_total_pnl_ticks * tick_value_rub for label in labels), Decimal("0")),
            Decimal(total_pairs),
        ),
        avg_runner_profit_ticks=decimal_ratio(
            sum((label.runner_exit_ticks for label in labels), Decimal("0")),
            Decimal(total_pairs),
        ),
        avg_loser_loss_ticks=decimal_ratio(
            sum((label.loser_loss_ticks for label in labels), Decimal("0")),
            Decimal(total_pairs),
        ),
        avg_spread_cost_ticks=decimal_ratio(
            sum((label.entry_spread_cost_ticks for label in labels), Decimal("0")),
            Decimal(total_pairs),
        ),
        avg_slippage_ticks=decimal_ratio(
            sum((label.avg_slippage_ticks for label in labels), Decimal("0")),
            Decimal(total_pairs),
        ),
        fakeout_rate=decimal_ratio(
            Decimal(sum(1 for label in labels if label.fakeout_flag)),
            Decimal(total_pairs),
        ),
        runner_to_breakeven_rate=decimal_ratio(
            Decimal(sum(1 for label in labels if label.runner_success_flag)),
            Decimal(total_pairs),
        ),
        runner_trailing_capture_ratio=decimal_ratio(runner_exit_positive_total, runner_mfe_total),
        profit_factor=decimal_ratio(gross_profit, abs(gross_loss)),
        max_drawdown=_max_drawdown(labels),
        pnl_by_bot=pnl_by_bot,
        pnl_by_account=pnl_by_account,
        pnl_by_instrument=_group_pnl(labels, key="instrument"),
        pnl_by_regime=_group_pnl(labels, key="regime"),
        rejected_by_model=rejected_by_model,
        rejected_by_spread=rejected_by_spread,
        rejected_by_spread_entry_gate=rejected_by_spread_entry_gate,
        rejected_by_stale_book=rejected_by_stale_book,
        rejected_by_chop=rejected_by_chop,
        rejected_by_latency=rejected_by_latency,
    )


def _pnl_by_bot(labels: Sequence[PairLabel]) -> dict[str, Decimal]:
    pnl_by_bot: dict[str, Decimal] = {}
    for label in labels:
        if label.long_bot_id is None or label.short_bot_id is None:
            continue
        if label.loser_side is LegSide.LONG:
            _add_bot_pnl(pnl_by_bot, label.long_bot_id, -label.loser_loss_ticks)
            _add_bot_pnl(pnl_by_bot, label.short_bot_id, label.runner_exit_ticks)
        elif label.loser_side is LegSide.SHORT:
            _add_bot_pnl(pnl_by_bot, label.short_bot_id, -label.loser_loss_ticks)
            _add_bot_pnl(pnl_by_bot, label.long_bot_id, label.runner_exit_ticks)
        else:
            half = label.pair_total_pnl_ticks / Decimal("2")
            _add_bot_pnl(pnl_by_bot, label.long_bot_id, half)
            _add_bot_pnl(pnl_by_bot, label.short_bot_id, half)
    return pnl_by_bot


def _add_bot_pnl(pnl_by_bot: dict[str, Decimal], bot_id: str, pnl: Decimal) -> None:
    pnl_by_bot[bot_id] = pnl_by_bot.get(bot_id, Decimal("0")) + pnl


def _group_pnl(labels: Sequence[PairLabel], *, key: str) -> dict[str, Decimal]:
    grouped: dict[str, Decimal] = {}
    for label in labels:
        group_key = label.instrument.value if key == "instrument" else label.regime
        grouped[group_key] = grouped.get(group_key, Decimal("0")) + label.pair_total_pnl_ticks
    return grouped


def _max_drawdown(labels: Sequence[PairLabel]) -> Decimal:
    equity = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for label in labels:
        equity += label.pair_total_pnl_ticks
        peak = max(peak, equity)
        max_drawdown = min(max_drawdown, equity - peak)
    return max_drawdown


__all__ = [
    "HedgePairSimulationConfig",
    "HedgePairSimulator",
    "aggregate_pair_metrics",
]
