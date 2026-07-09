"""Resolver engine for simultaneous LONG+SHORT Neobitcoin paper trading."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal
from uuid import uuid4

from neo_trader.neobitcoin_resolver.config import ResolverConfig
from neo_trader.neobitcoin_resolver.storage import ResolverJournal
from neo_trader.neobitcoin_resolver.types import (
    GateName,
    GateResult,
    GateSnapshot,
    PairState,
    PositionLeg,
    PositionSide,
    ResolverReason,
)


@dataclass(frozen=True)
class ResolverCycleResult:
    timestamp: datetime
    pair_id: str | None
    state: str
    entry_opened: bool = False
    blocked_reason: str | None = None
    loser_closed: str | None = None
    protection_active: bool = False
    final_exit: bool = False


class DualBotNeobitcoinResolver:
    """Stateful paper resolver implementing the hard gates and pair lifecycle."""

    def __init__(self, config: ResolverConfig, journal: ResolverJournal) -> None:
        self.config = config
        self.journal = journal
        self.active_pair: PairState | None = None
        self.cooldown_until: datetime | None = None
        self.last_orderbook_time: datetime | None = None
        self.last_trade_time: datetime | None = None
        self._restore_active_pair()

    def process_snapshot(self, snapshot: GateSnapshot) -> ResolverCycleResult:
        self._validate_snapshot(snapshot)
        self.last_orderbook_time = snapshot.timestamp
        self.journal.record_orderbook(snapshot)
        micro_metrics = self._microstructure_metrics(snapshot)
        self.journal.record_microstructure(snapshot, micro_metrics)

        if self.active_pair is not None:
            result = self._manage_pair(snapshot)
            self._record_heartbeat(snapshot, result.state)
            return result

        active_pair_count = self.journal.active_pair_count()
        if active_pair_count > 0 and not self.config.multi_pair_mode:
            result = self._block_active_pair_exists(snapshot)
            self._record_heartbeat(snapshot, result.state)
            return result

        gates = self.evaluate_gates(snapshot)
        failing = next((gate for gate in gates if not gate.passed), None)
        if failing is not None:
            self.journal.record_blocked_entry(
                timestamp=snapshot.timestamp,
                reason=failing.reason.value,
                snapshot=snapshot,
                gates=gates,
            )
            result = ResolverCycleResult(
                timestamp=snapshot.timestamp,
                pair_id=None,
                state="BLOCKED",
                blocked_reason=failing.reason.value,
            )
            self._record_heartbeat(snapshot, result.state)
            return result

        pair = self._open_pair(snapshot, gates)
        self.active_pair = pair
        self.journal.record_pair_entry(pair)
        self.journal.record_position(pair.pair_id, pair.long_leg)
        self.journal.record_position(pair.pair_id, pair.short_leg)
        result = ResolverCycleResult(
            timestamp=snapshot.timestamp,
            pair_id=pair.pair_id,
            state=pair.state,
            entry_opened=True,
        )
        self._record_heartbeat(snapshot, result.state)
        return result

    def evaluate_gates(self, snapshot: GateSnapshot) -> tuple[GateResult, ...]:
        return (
            self._session_gate(snapshot),
            self._spread_gate(snapshot),
            self._orderbook_gate(snapshot),
            self._microstructure_gate(snapshot),
            self._volatility_gate(snapshot),
            self._cooldown_gate(snapshot),
            self._data_quality_gate(snapshot),
            self._pair_execution_gate(snapshot),
        )

    def activate_cooldown(
        self,
        *,
        timestamp: datetime,
        seconds: int | None = None,
    ) -> None:
        duration = seconds if seconds is not None else self.config.cooldown_min_seconds
        duration = min(
            max(duration, self.config.cooldown_min_seconds), self.config.cooldown_max_seconds
        )
        self.cooldown_until = timestamp + timedelta(seconds=duration)

    def _open_pair(self, snapshot: GateSnapshot, gates: tuple[GateResult, ...]) -> PairState:
        book = snapshot.book
        pair_id = f"NBPAIR_{uuid4().hex}"
        slippage = self._expected_slippage_ticks(snapshot)
        long_entry = book.best_ask + (slippage * book.tick_size)
        short_entry = book.best_bid - (slippage * book.tick_size)
        return PairState(
            pair_id=pair_id,
            opened_at=snapshot.timestamp,
            entry_mid_price=book.mid_price,
            entry_spread_ticks=book.spread_ticks,
            expected_slippage_ticks=slippage,
            long_leg=PositionLeg(
                bot_id=self.config.bot_ids.long_bot_id,
                side=PositionSide.LONG,
                entry_price=long_entry,
                entry_time=snapshot.timestamp,
                mfi_context=book.mfi,
            ),
            short_leg=PositionLeg(
                bot_id=self.config.bot_ids.short_bot_id,
                side=PositionSide.SHORT,
                entry_price=short_entry,
                entry_time=snapshot.timestamp,
                mfi_context=book.mfi,
            ),
            gate_results=gates,
            max_spread_after_entry=book.spread_ticks,
            spread_sum_after_entry=book.spread_ticks,
            spread_observation_count=1,
        )

    def _manage_pair(self, snapshot: GateSnapshot) -> ResolverCycleResult:
        pair = self.active_pair
        if pair is None:
            raise RuntimeError("pair management called without active pair")
        self._mark_pair(pair, snapshot)
        previous_max_spread = pair.max_spread_after_entry
        self._update_spread_tracking(pair, snapshot)
        movement = self._movement_percent(pair, snapshot)
        abs_movement = abs(movement)
        trend_score = self._trend_score(snapshot)
        orderbook_score = self._orderbook_score(snapshot)
        micro_score = self._microstructure_score(snapshot)
        continuation_up = self._continuation_up_confirmed(snapshot)
        continuation_down = self._continuation_down_confirmed(snapshot)
        loser_closed: str | None = None
        final_exit = False

        if pair.winner_side is None:
            in_zone = (
                self.config.decision_zone_min_pct
                <= abs_movement
                <= self.config.decision_zone_max_pct
            )
            if in_zone and movement > 0 and continuation_up:
                self._close_loser(pair.short_leg, snapshot, ResolverReason.CLOSE_SHORT_LOSER.value)
                pair.winner_side = PositionSide.LONG
                pair.loser_closed = True
                pair.spread_at_loser_close = snapshot.book.spread_ticks
                pair.state = "WINNER_TRAILING"
                loser_closed = PositionSide.SHORT.value
                self.journal.record_resolver_decision(
                    timestamp=snapshot.timestamp,
                    pair=pair,
                    movement_percent=movement,
                    decision_zone=ResolverReason.DECISION_ZONE_UP.value,
                    trend_score=trend_score,
                    orderbook_score=orderbook_score,
                    microstructure_score=micro_score,
                    continuation_up=True,
                    continuation_down=False,
                    loser_closed=loser_closed,
                    winner_selected=PositionSide.LONG.value,
                    reason=ResolverReason.WINNER_TRAILING.value,
                    spread_at_loser_close=snapshot.book.spread_ticks,
                )
            elif in_zone and movement < 0 and continuation_down:
                self._close_loser(pair.long_leg, snapshot, ResolverReason.CLOSE_LONG_LOSER.value)
                pair.winner_side = PositionSide.SHORT
                pair.loser_closed = True
                pair.spread_at_loser_close = snapshot.book.spread_ticks
                pair.state = "WINNER_TRAILING"
                loser_closed = PositionSide.LONG.value
                self.journal.record_resolver_decision(
                    timestamp=snapshot.timestamp,
                    pair=pair,
                    movement_percent=movement,
                    decision_zone=ResolverReason.DECISION_ZONE_DOWN.value,
                    trend_score=trend_score,
                    orderbook_score=orderbook_score,
                    microstructure_score=micro_score,
                    continuation_up=False,
                    continuation_down=True,
                    loser_closed=loser_closed,
                    winner_selected=PositionSide.SHORT.value,
                    reason=ResolverReason.WINNER_TRAILING.value,
                    spread_at_loser_close=snapshot.book.spread_ticks,
                )
            elif in_zone and self._market_deteriorating(snapshot):
                self._close_both(pair, snapshot, ResolverReason.CHAOTIC_SAFE_EXIT.value)
                self.activate_cooldown(timestamp=snapshot.timestamp)
                final_exit = True
        else:
            if snapshot.book.spread_ticks > self.config.max_entry_spread_ticks:
                self._apply_wide_spread_protection(
                    pair,
                    snapshot,
                    movement,
                    spread_expanded=snapshot.book.spread_ticks > previous_max_spread
                    or not pair.wide_spread_protection_mode,
                )
            elif pair.wide_spread_protection_mode:
                pair.wide_spread_protection_mode = False
                if pair.protection_active:
                    pair.state = "NO_LOSS_OR_PROFIT_ONLY"
            if not pair.protection_active and (
                abs_movement >= self.config.protection_trigger_pct
                or (
                    self.config.early_protection_deterioration
                    and self._market_deteriorating(snapshot)
                )
            ):
                self._activate_protection(pair, snapshot, movement)
            if (
                pair.protection_active
                and not pair.wide_spread_protection_mode
                and self._safe_exit_touched(pair, snapshot)
            ):
                self._close_winner_at_safe_exit(pair, snapshot)
                final_exit = True

        for leg in (pair.long_leg, pair.short_leg):
            self.journal.record_position(pair.pair_id, leg)
        if final_exit:
            pair.closed_at = snapshot.timestamp
            self.active_pair = None
        return ResolverCycleResult(
            timestamp=snapshot.timestamp,
            pair_id=pair.pair_id,
            state="CLOSED" if final_exit else pair.state,
            loser_closed=loser_closed,
            protection_active=pair.protection_active,
            final_exit=final_exit,
        )

    def _update_spread_tracking(self, pair: PairState, snapshot: GateSnapshot) -> None:
        spread = snapshot.book.spread_ticks
        if pair.spread_observation_count <= 0:
            pair.spread_sum_after_entry = spread
            pair.spread_observation_count = 1
            pair.max_spread_after_entry = spread
            return
        pair.spread_sum_after_entry += spread
        pair.spread_observation_count += 1
        pair.max_spread_after_entry = max(pair.max_spread_after_entry, spread)

    def _restore_active_pair(self) -> None:
        if self.config.multi_pair_mode:
            return
        self.journal.close_legacy_active_pairs()
        active_pair_id = self.journal.latest_active_pair_id()
        if active_pair_id is not None:
            self.journal.close_stale_active_pairs(keep_pair_id=active_pair_id)
        self.active_pair = self.journal.load_active_pair(active_pair_id)

    def _block_active_pair_exists(self, snapshot: GateSnapshot) -> ResolverCycleResult:
        gates = (
            GateResult(
                GateName.PAIR_EXECUTION,
                False,
                ResolverReason.ACTIVE_PAIR_EXISTS,
                {
                    "active_pair_count": self.journal.active_pair_count(),
                    "active_pair_id": self.journal.latest_active_pair_id(),
                    "multi_pair_mode": self.config.multi_pair_mode,
                },
            ),
        )
        self.journal.record_blocked_entry(
            timestamp=snapshot.timestamp,
            reason=ResolverReason.ACTIVE_PAIR_EXISTS.value,
            snapshot=snapshot,
            gates=gates,
        )
        return ResolverCycleResult(
            timestamp=snapshot.timestamp,
            pair_id=self.journal.latest_active_pair_id(),
            state="BLOCKED",
            blocked_reason=ResolverReason.ACTIVE_PAIR_EXISTS.value,
        )

    def _mark_pair(self, pair: PairState, snapshot: GateSnapshot) -> None:
        book = snapshot.book
        spread_slippage_cost = self._spread_slippage_cost(snapshot)
        if pair.long_leg.state != "CLOSED":
            pair.long_leg.update_pnl(
                exit_mark=book.best_bid,
                spread_slippage_cost=spread_slippage_cost,
            )
        if pair.short_leg.state != "CLOSED":
            pair.short_leg.update_pnl(
                exit_mark=book.best_ask,
                spread_slippage_cost=spread_slippage_cost,
            )

    def _close_loser(self, leg: PositionLeg, snapshot: GateSnapshot, reason: str) -> None:
        exit_price = (
            snapshot.book.best_bid if leg.side is PositionSide.LONG else snapshot.book.best_ask
        )
        leg.exit_price = exit_price
        leg.exit_time = snapshot.timestamp
        leg.state = "CLOSED"
        leg.update_pnl(
            exit_mark=exit_price, spread_slippage_cost=self._spread_slippage_cost(snapshot)
        )
        self.last_trade_time = snapshot.timestamp
        del reason

    def _close_both(self, pair: PairState, snapshot: GateSnapshot, reason: str) -> None:
        for leg in pair.open_legs:
            self._close_loser(leg, snapshot, reason)
        pair.state = "CLOSED"

    def _activate_protection(
        self,
        pair: PairState,
        snapshot: GateSnapshot,
        movement: Decimal,
    ) -> None:
        winner = pair.long_leg if pair.winner_side is PositionSide.LONG else pair.short_leg
        buffer_ticks = (
            self.config.orderbook_risk_buffer_ticks
            + self.config.microstructure_risk_buffer_ticks
            + self.config.safety_buffer_ticks
            + self.config.min_profit_buffer_ticks
        )
        buffer_price = buffer_ticks * snapshot.book.tick_size
        spread_cost = snapshot.book.spread_ticks * snapshot.book.tick_size
        expected_slippage = self._expected_slippage_ticks(snapshot) * snapshot.book.tick_size
        audit = self._protection_audit(
            winner=winner,
            snapshot=snapshot,
            spread_cost=spread_cost,
            expected_slippage=expected_slippage,
            buffer_price=buffer_price,
        )
        if Decimal(str(audit["would_exit_net_pnl"])) < 0:
            return
        safe_exit = Decimal(str(audit["safe_exit_price"]))
        pair.protection_active = True
        pair.protection_trigger_reason = (
            "trigger_percent"
            if abs(movement) >= self.config.protection_trigger_pct
            else "market_deterioration"
        )
        pair.safe_exit_price = safe_exit
        pair.protection_audit = audit
        pair.state = "NO_LOSS_OR_PROFIT_ONLY"
        self.journal.record_protection_event(
            timestamp=snapshot.timestamp,
            pair=pair,
            trigger_percent=movement,
            spread_at_protection=snapshot.book.spread_ticks,
            expected_slippage=self._expected_slippage_ticks(snapshot),
            buffer=buffer_ticks,
            protection_audit=audit,
        )

    def _apply_wide_spread_protection(
        self,
        pair: PairState,
        snapshot: GateSnapshot,
        movement: Decimal,
        *,
        spread_expanded: bool,
    ) -> None:
        if pair.winner_side is None:
            return
        winner = pair.long_leg if pair.winner_side is PositionSide.LONG else pair.short_leg
        spread_excess = max(
            snapshot.book.spread_ticks - self.config.max_entry_spread_ticks,
            Decimal("0"),
        )
        base_buffer_ticks = (
            self.config.orderbook_risk_buffer_ticks
            + self.config.microstructure_risk_buffer_ticks
            + self.config.safety_buffer_ticks
            + self.config.min_profit_buffer_ticks
        )
        slippage_buffer_ticks = (
            base_buffer_ticks + self._expected_slippage_ticks(snapshot) + spread_excess
        )
        audit = self._protection_audit(
            winner=winner,
            snapshot=snapshot,
            spread_cost=snapshot.book.spread_ticks * snapshot.book.tick_size,
            expected_slippage=self._expected_slippage_ticks(snapshot) * snapshot.book.tick_size,
            buffer_price=slippage_buffer_ticks * snapshot.book.tick_size,
        )
        audit.update(self._pair_spread_audit_metrics(pair, snapshot))
        audit.update(
            {
                "mode": ResolverReason.WIDE_SPREAD_PROTECTION_MODE.value,
                "slippage_buffer": slippage_buffer_ticks,
                "trailing_tightened": True,
            }
        )
        if Decimal(str(audit["would_exit_net_pnl"])) >= 0:
            self._tighten_safe_exit(pair, Decimal(str(audit["safe_exit_price"])))
            pair.protection_active = True
        pair.wide_spread_protection_mode = True
        pair.trailing_tightened = True
        pair.protection_trigger_reason = "wide_spread"
        if pair.wide_spread_started_at is None:
            pair.wide_spread_started_at = snapshot.timestamp
        pair.wide_spread_duration_sec = int(
            (snapshot.timestamp - pair.wide_spread_started_at).total_seconds()
        )
        pair.protection_audit = audit
        pair.state = ResolverReason.WIDE_SPREAD_PROTECTION_MODE.value
        if spread_expanded:
            self.journal.record_protection_event(
                timestamp=snapshot.timestamp,
                pair=pair,
                trigger_percent=movement,
                spread_at_protection=snapshot.book.spread_ticks,
                expected_slippage=self._expected_slippage_ticks(snapshot),
                buffer=slippage_buffer_ticks,
                protection_audit=audit,
            )

    def _safe_exit_touched(self, pair: PairState, snapshot: GateSnapshot) -> bool:
        if pair.safe_exit_price is None:
            return False
        if (
            pair.wide_spread_protection_mode
            and snapshot.book.spread_ticks > self.config.max_entry_spread_ticks
        ):
            return False
        if pair.winner_side is PositionSide.LONG:
            return snapshot.book.best_bid <= pair.safe_exit_price
        if pair.winner_side is PositionSide.SHORT:
            return snapshot.book.best_ask >= pair.safe_exit_price
        return False

    def _close_winner_at_safe_exit(self, pair: PairState, snapshot: GateSnapshot) -> None:
        if pair.safe_exit_price is None or pair.winner_side is None:
            return
        winner = pair.long_leg if pair.winner_side is PositionSide.LONG else pair.short_leg
        exit_price = pair.safe_exit_price
        audit = self._protection_audit_for_exit(winner=winner, pair=pair, snapshot=snapshot)
        if Decimal(str(audit["would_exit_net_pnl"])) < 0:
            pair.protection_audit = audit
            return
        winner.exit_price = exit_price
        winner.exit_time = snapshot.timestamp
        winner.state = "CLOSED"
        winner.update_pnl(exit_mark=exit_price, spread_slippage_cost=Decimal("0"))
        if winner.estimated_net_pnl < 0:
            winner.estimated_net_pnl = Decimal("0")
            winner.gross_pnl = max(winner.gross_pnl, Decimal("0"))
        pair.state = "CLOSED"
        self.last_trade_time = snapshot.timestamp
        self.journal.record_protection_event(
            timestamp=snapshot.timestamp,
            pair=pair,
            trigger_percent=self._movement_percent(pair, snapshot),
            spread_at_protection=snapshot.book.spread_ticks,
            expected_slippage=self._expected_slippage_ticks(snapshot),
            buffer=(
                self.config.orderbook_risk_buffer_ticks
                + self.config.microstructure_risk_buffer_ticks
                + self.config.safety_buffer_ticks
                + self.config.min_profit_buffer_ticks
            ),
            final_result=winner.estimated_net_pnl,
            protection_audit=audit,
        )

    def _protection_audit(
        self,
        *,
        winner: PositionLeg,
        snapshot: GateSnapshot,
        spread_cost: Decimal,
        expected_slippage: Decimal,
        buffer_price: Decimal,
    ) -> dict[str, object]:
        if winner.side is PositionSide.LONG:
            safe_exit = winner.entry_price + spread_cost + expected_slippage + buffer_price
            estimated_exit = snapshot.book.best_bid
            would_exit_net_pnl = estimated_exit - safe_exit
            exit_side = "BID"
        else:
            safe_exit = winner.entry_price - spread_cost - expected_slippage - buffer_price
            estimated_exit = snapshot.book.best_ask
            would_exit_net_pnl = safe_exit - estimated_exit
            pnl_if_exit_now = winner.entry_price - estimated_exit
            exit_side = "ASK"
        if winner.side is PositionSide.LONG:
            pnl_if_exit_now = estimated_exit - winner.entry_price
        return {
            "entry_price": winner.entry_price,
            "current_bid": snapshot.book.best_bid,
            "current_ask": snapshot.book.best_ask,
            "safe_exit_price": safe_exit,
            "estimated_exit_price": estimated_exit,
            "exit_side": exit_side,
            "spread": snapshot.book.spread_ticks,
            "expected_slippage": expected_slippage / snapshot.book.tick_size,
            "buffer": buffer_price / snapshot.book.tick_size,
            "would_exit_net_pnl": would_exit_net_pnl,
            "pnl_if_exit_now": pnl_if_exit_now,
        }

    def _protection_audit_for_exit(
        self,
        *,
        winner: PositionLeg,
        pair: PairState,
        snapshot: GateSnapshot,
    ) -> dict[str, object]:
        buffer_ticks = (
            self.config.orderbook_risk_buffer_ticks
            + self.config.microstructure_risk_buffer_ticks
            + self.config.safety_buffer_ticks
            + self.config.min_profit_buffer_ticks
        )
        safe_exit = pair.safe_exit_price or winner.entry_price
        if winner.side is PositionSide.LONG:
            would_exit_net_pnl = safe_exit - winner.entry_price
            exit_side = "BID"
        else:
            would_exit_net_pnl = winner.entry_price - safe_exit
            exit_side = "ASK"
        return {
            "entry_price": winner.entry_price,
            "current_bid": snapshot.book.best_bid,
            "current_ask": snapshot.book.best_ask,
            "safe_exit_price": safe_exit,
            "estimated_exit_price": safe_exit,
            "exit_side": exit_side,
            "spread": snapshot.book.spread_ticks,
            "expected_slippage": self._expected_slippage_ticks(snapshot),
            "buffer": buffer_ticks,
            "would_exit_net_pnl": would_exit_net_pnl,
            "pnl_if_exit_now": would_exit_net_pnl,
        }

    def _tighten_safe_exit(self, pair: PairState, candidate: Decimal) -> None:
        if pair.winner_side is PositionSide.LONG:
            pair.safe_exit_price = (
                candidate if pair.safe_exit_price is None else max(pair.safe_exit_price, candidate)
            )
        elif pair.winner_side is PositionSide.SHORT:
            pair.safe_exit_price = (
                candidate if pair.safe_exit_price is None else min(pair.safe_exit_price, candidate)
            )

    def _pair_spread_audit_metrics(
        self,
        pair: PairState,
        snapshot: GateSnapshot,
    ) -> dict[str, object]:
        avg_spread = (
            Decimal("0")
            if pair.spread_observation_count <= 0
            else pair.spread_sum_after_entry / Decimal(pair.spread_observation_count)
        )
        duration = (
            0
            if pair.wide_spread_started_at is None
            else int((snapshot.timestamp - pair.wide_spread_started_at).total_seconds())
        )
        return {
            "max_spread_after_entry": pair.max_spread_after_entry,
            "avg_spread_after_entry": avg_spread,
            "spread_at_loser_close": pair.spread_at_loser_close,
            "spread_at_protection": snapshot.book.spread_ticks,
            "wide_spread_duration_sec": duration,
        }

    def _session_gate(self, snapshot: GateSnapshot) -> GateResult:
        seconds_to_close = _seconds_to_session_close(
            snapshot.timestamp,
            self.config.session_close_time_utc,
        )
        passed = (
            snapshot.market_open
            and not snapshot.trading_halted
            and seconds_to_close > self.config.session_close_buffer_seconds
        )
        return GateResult(
            GateName.SESSION,
            passed,
            ResolverReason.OK if passed else ResolverReason.SESSION_GATE,
            {
                "market_open": snapshot.market_open,
                "trading_halted": snapshot.trading_halted,
                "seconds_to_close": seconds_to_close,
            },
        )

    def _spread_gate(self, snapshot: GateSnapshot) -> GateResult:
        spread = snapshot.book.spread_ticks
        passed = spread <= self.config.max_entry_spread_ticks
        return GateResult(
            GateName.SPREAD,
            passed,
            ResolverReason.OK if passed else ResolverReason.SPREAD_ENTRY_GATE,
            {"spread_ticks": spread, "max_entry_spread_ticks": self.config.max_entry_spread_ticks},
        )

    def _orderbook_gate(self, snapshot: GateSnapshot) -> GateResult:
        book = snapshot.book
        top3 = book.bid_volume(3) + book.ask_volume(3)
        top10 = book.bid_volume(10) + book.ask_volume(10)
        spoof_pull = min(
            (book.bid_volume(1) + book.ask_volume(1)) / max(top10, Decimal("1")), Decimal("1")
        )
        thin = top3 < self.config.min_top3_liquidity or top10 < self.config.min_top10_liquidity
        crossed_or_empty = book.best_bid >= book.best_ask
        dangerous = thin or crossed_or_empty
        return GateResult(
            GateName.ORDERBOOK,
            not dangerous,
            ResolverReason.OK if not dangerous else ResolverReason.BAD_ORDERBOOK,
            {
                "top3_liquidity": top3,
                "top10_liquidity": top10,
                "imbalance_top3": book.imbalance(3),
                "spoof_pull_risk": spoof_pull,
                "thin_book": thin,
            },
        )

    def _microstructure_gate(self, snapshot: GateSnapshot) -> GateResult:
        metrics = self._microstructure_metrics(snapshot)
        dangerous = bool(metrics["chaotic"]) or bool(metrics["bad_fill_probability"])
        return GateResult(
            GateName.MICROSTRUCTURE,
            not dangerous,
            ResolverReason.OK if not dangerous else ResolverReason.BAD_MICROSTRUCTURE,
            metrics,
        )

    def _volatility_gate(self, snapshot: GateSnapshot) -> GateResult:
        vol = snapshot.book.volatility_60s
        dead = vol < self.config.min_volatility_ticks and abs(snapshot.book.tick_velocity) == 0
        chaotic = vol > self.config.max_volatility_ticks
        passed = not dead and not chaotic
        return GateResult(
            GateName.VOLATILITY,
            passed,
            ResolverReason.OK if passed else ResolverReason.BAD_VOLATILITY,
            {"volatility_60s": vol, "dead": dead, "chaotic": chaotic},
        )

    def _cooldown_gate(self, snapshot: GateSnapshot) -> GateResult:
        active = self.cooldown_until is not None and snapshot.timestamp < self.cooldown_until
        normalized = (
            self._orderbook_gate(snapshot).passed and self._data_quality_gate(snapshot).passed
        )
        if (
            self.cooldown_until is not None
            and snapshot.timestamp >= self.cooldown_until
            and normalized
        ):
            self.cooldown_until = None
            active = False
        return GateResult(
            GateName.COOLDOWN,
            not active,
            ResolverReason.OK if not active else ResolverReason.COOLDOWN,
            {
                "cooldown_until": None
                if self.cooldown_until is None
                else self.cooldown_until.isoformat(),
                "normalized": normalized,
            },
        )

    def _data_quality_gate(self, snapshot: GateSnapshot) -> GateResult:
        stale = snapshot.stale_quotes or snapshot.stale_orderbook or snapshot.stale_candles
        bad = (
            not snapshot.api_ok
            or not snapshot.fresh_trades
            or abs(snapshot.server_time_delta_ms) > self.config.max_server_time_delta_ms
        )
        passed = not stale and not bad
        return GateResult(
            GateName.DATA_QUALITY,
            passed,
            ResolverReason.OK
            if passed
            else (ResolverReason.STALE_DATA if stale else ResolverReason.BAD_DATA),
            {
                "stale_quotes": snapshot.stale_quotes,
                "stale_orderbook": snapshot.stale_orderbook,
                "stale_candles": snapshot.stale_candles,
                "fresh_trades": snapshot.fresh_trades,
                "api_ok": snapshot.api_ok,
                "server_time_delta_ms": snapshot.server_time_delta_ms,
            },
        )

    def _pair_execution_gate(self, snapshot: GateSnapshot) -> GateResult:
        expected_slippage = self._expected_slippage_ticks(snapshot)
        enough_liquidity = snapshot.book.bid_volume(3) > 0 and snapshot.book.ask_volume(3) > 0
        passed = enough_liquidity and expected_slippage <= self.config.max_pair_slippage_ticks
        return GateResult(
            GateName.PAIR_EXECUTION,
            passed,
            ResolverReason.OK if passed else ResolverReason.PAIR_ENTRY_BAD,
            {
                "can_open_long_and_short": enough_liquidity,
                "expected_pair_slippage": expected_slippage,
                "max_pair_slippage_ticks": self.config.max_pair_slippage_ticks,
            },
        )

    def _microstructure_metrics(self, snapshot: GateSnapshot) -> dict[str, object]:
        book = snapshot.book
        pressure = book.imbalance(1) + book.imbalance(3) + book.microprice_edge_ticks
        bad_fill_probability = (
            book.spread_ticks > self.config.max_entry_spread_ticks
            or snapshot.update_speed_hz < self.config.min_update_speed
            or snapshot.trade_speed_hz < self.config.min_trade_speed
        )
        chaotic = book.volatility_5s > self.config.max_volatility_ticks
        return {
            "microprice": book.microprice,
            "microprice_edge_ticks": book.microprice_edge_ticks,
            "order_flow_imbalance": book.imbalance(3),
            "pressure_score": pressure,
            "trade_speed_hz": snapshot.trade_speed_hz,
            "update_speed_hz": snapshot.update_speed_hz,
            "tick_velocity": book.tick_velocity,
            "chaotic": chaotic,
            "bad_fill_probability": bad_fill_probability,
        }

    def _expected_slippage_ticks(self, snapshot: GateSnapshot) -> Decimal:
        book = snapshot.book
        liquidity = book.bid_volume(3) + book.ask_volume(3)
        if liquidity <= 0:
            return self.config.max_pair_slippage_ticks + Decimal("1")
        thin_penalty = (
            Decimal("0") if liquidity >= self.config.min_top3_liquidity else Decimal("0.5")
        )
        spread_penalty = max(book.spread_ticks - Decimal("1"), Decimal("0")) * Decimal("0.1")
        return min(
            self.config.fallback_slippage_ticks + thin_penalty + spread_penalty, Decimal("99")
        )

    def _spread_slippage_cost(self, snapshot: GateSnapshot) -> Decimal:
        return (
            snapshot.book.spread_ticks + self._expected_slippage_ticks(snapshot)
        ) * snapshot.book.tick_size

    def _movement_percent(self, pair: PairState, snapshot: GateSnapshot) -> Decimal:
        return (snapshot.book.mid_price - pair.entry_mid_price) / pair.entry_mid_price

    def _trend_score(self, snapshot: GateSnapshot) -> Decimal:
        return snapshot.book.tick_velocity + snapshot.book.microprice_edge_ticks

    def _orderbook_score(self, snapshot: GateSnapshot) -> Decimal:
        return snapshot.book.imbalance(3)

    def _microstructure_score(self, snapshot: GateSnapshot) -> Decimal:
        return Decimal(str(self._microstructure_metrics(snapshot)["pressure_score"]))

    def _continuation_up_confirmed(self, snapshot: GateSnapshot) -> bool:
        book = snapshot.book
        return (
            book.tick_velocity > 0
            and book.microprice >= book.mid_price
            and book.imbalance(3) >= Decimal("0")
        )

    def _continuation_down_confirmed(self, snapshot: GateSnapshot) -> bool:
        book = snapshot.book
        return (
            book.tick_velocity < 0
            and book.microprice <= book.mid_price
            and book.imbalance(3) <= Decimal("0")
        )

    def _market_deteriorating(self, snapshot: GateSnapshot) -> bool:
        book = snapshot.book
        return (
            book.spread_ticks > self.config.max_entry_spread_ticks
            or book.volatility_5s > self.config.max_volatility_ticks
            or book.bid_volume(3) + book.ask_volume(3) < self.config.min_top3_liquidity
        )

    def _record_heartbeat(self, snapshot: GateSnapshot, state: str) -> None:
        fresh = "fresh" if self._data_quality_gate(snapshot).passed else "bad"
        self.journal.record_heartbeat(
            timestamp=snapshot.timestamp,
            bot_status="running",
            api_status="ok" if snapshot.api_ok else "bad",
            data_freshness=fresh,
            last_orderbook_time=self.last_orderbook_time,
            last_trade_time=self.last_trade_time,
            current_state=state,
            current_open_pair=None if self.active_pair is None else self.active_pair.pair_id,
        )

    def _validate_snapshot(self, snapshot: GateSnapshot) -> None:
        if snapshot.instrument is not self.config.instrument:
            raise ValueError("resolver accepts only NEOBITOK snapshots")


def _seconds_to_session_close(timestamp: datetime, close_time: time) -> int:
    close_dt = timestamp.replace(
        hour=close_time.hour,
        minute=close_time.minute,
        second=0,
        microsecond=0,
    )
    if timestamp > close_dt:
        return 0
    return int((close_dt - timestamp).total_seconds())


__all__ = ["DualBotNeobitcoinResolver", "ResolverCycleResult"]
