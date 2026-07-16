from __future__ import annotations

import csv
import statistics
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from neobitcoin_paper.calendar import SessionCalendar
from neobitcoin_paper.domain import DataQuality, MarketEvent, Side, StrategyStatus
from neobitcoin_paper.strategies import (
    FeatureSnapshot,
    FlowAlignmentStrategy,
    StrategyContext,
    TradeFlowWindow,
    frozen_flow_alignment_v1,
)

NOW = datetime(2026, 7, 16, 9, 0, tzinfo=UTC)
UID = "4effa274-4e8f-422c-93ff-04aa34fe8e39"


def _event(at: datetime) -> MarketEvent:
    return MarketEvent(
        event_id=f"book-{at.timestamp()}",
        instrument_uid=UID,
        event_type="orderbook",
        exchange_ts=at - timedelta(milliseconds=2),
        receive_ts=at - timedelta(milliseconds=1),
        processing_ts=at,
        trading_status="NORMAL_TRADING",
        reconnect_generation=7,
    )


def _features(
    at: datetime,
    *,
    micro: str = "0.40",
    l5: str = "0.30",
    flow: str = "0.40",
    known: int = 1,
    ready: bool = True,
) -> FeatureSnapshot:
    buy = Decimal("7") if Decimal(flow) >= 0 else Decimal("3")
    sell = Decimal("3") if Decimal(flow) >= 0 else Decimal("7")
    return FeatureSnapshot(
        feature_ts=at,
        source_event_ids=(f"source-{at.timestamp()}",),
        ready=False,
        microprice_offset=Decimal(micro),
        imbalance_l5=Decimal(l5),
        alignment_ready=ready,
        l5_ready=ready,
        spread_ticks=20,
        trade_flow_windows=(
            TradeFlowWindow(
                seconds=5,
                trade_count=known,
                known_trade_count=known,
                unknown_side_trade_count=0,
                buy_volume=buy if known else Decimal("0"),
                sell_volume=sell if known else Decimal("0"),
                unknown_side_volume=Decimal("0"),
                trade_flow=(buy - sell) if known else Decimal("0"),
                first_event_id="trade" if known else None,
                last_event_id="trade" if known else None,
            ),
        ),
    )


def _context(
    strategy: FlowAlignmentStrategy,
    at: datetime,
    features: FeatureSnapshot,
    *,
    pending: bool = False,
    open_position: bool = False,
) -> StrategyContext:
    event = _event(at)
    return StrategyContext(
        event=event,
        book=None,
        features=features,
        session=SessionCalendar().resolve(at, "NORMAL_TRADING"),
        data_quality=DataQuality.GOOD,
        own_state={
            strategy.specification.key: {
                "pending": pending,
                "open": open_position,
            }
        },
    )


def _strategy(strategy_id: str) -> FlowAlignmentStrategy:
    spec = frozen_flow_alignment_v1(
        strategy_id,
        created_at=NOW - timedelta(minutes=2),
        activated_at=NOW - timedelta(minutes=1),
    )
    return FlowAlignmentStrategy(spec)


def test_approved_versions_and_exact_threshold_boundaries() -> None:
    micro = _strategy("MICRO_FLOW_ALIGNMENT")
    l5 = _strategy("L5_FLOW_ALIGNMENT")
    assert micro.specification.status is StrategyStatus.FROZEN_PAPER
    assert l5.specification.status is StrategyStatus.FROZEN_PAPER_SECONDARY
    assert micro.specification.parameters["max_entry_spread_ticks"] == 20
    assert l5.specification.parameters["alignment_long_threshold"] == 0.30

    long_decision = micro.evaluate(_context(micro, NOW, _features(NOW)))
    assert long_decision.signal and long_decision.intent is not None
    assert long_decision.intent.side is Side.BUY
    assert long_decision.intent.metadata["max_entry_wait_seconds"] == 5

    short_at = NOW + timedelta(seconds=1)
    short_decision = l5.evaluate(
        _context(
            l5,
            short_at,
            _features(short_at, micro="-0.40", l5="-0.30", flow="-0.40"),
        )
    )
    assert short_decision.signal and short_decision.intent is not None
    assert short_decision.intent.side is Side.SELL


def test_rising_edge_cooldown_blockers_and_restart_state_are_durable() -> None:
    strategy = _strategy("MICRO_FLOW_ALIGNMENT")
    first = strategy.evaluate(_context(strategy, NOW, _features(NOW)))
    assert first.signal and first.intent is not None
    strategy.on_intent_accepted(first.intent)
    assert not strategy.evaluate(
        _context(strategy, NOW + timedelta(seconds=121), _features(NOW))
    ).signal

    saved = strategy.snapshot_state()
    restored = _strategy("MICRO_FLOW_ALIGNMENT")
    restored.restore_state(saved)
    assert not restored.evaluate(
        _context(restored, NOW + timedelta(seconds=122), _features(NOW))
    ).signal

    false_at = NOW + timedelta(seconds=123)
    assert not restored.evaluate(
        _context(restored, false_at, _features(false_at, micro="0.39"))
    ).signal
    blocked_at = NOW + timedelta(seconds=124)
    assert not restored.evaluate(
        _context(restored, blocked_at, _features(blocked_at), pending=True)
    ).signal
    # The blocked true edge was consumed; staying true cannot produce a delayed entry.
    assert not restored.evaluate(
        _context(restored, NOW + timedelta(seconds=125), _features(blocked_at))
    ).signal

    false_again = NOW + timedelta(seconds=126)
    restored.evaluate(
        _context(restored, false_again, _features(false_again, micro="0.39"))
    )
    fresh_edge = NOW + timedelta(seconds=127)
    assert restored.evaluate(
        _context(restored, fresh_edge, _features(fresh_edge))
    ).signal


def test_unknown_or_empty_trade_flow_is_never_feature_ready() -> None:
    strategy = _strategy("MICRO_FLOW_ALIGNMENT")
    decision = strategy.evaluate(
        _context(strategy, NOW, _features(NOW, known=0))
    )
    assert not decision.signal
    assert decision.conditions["feature_ready"] is False


def test_frozen_control_rows_and_aggregate_reconciliation() -> None:
    root = Path(__file__).parents[1]
    with (root / "tests/fixtures/neobitcoin_2026-07-15_signal_evaluations.csv").open(
        "r", encoding="utf-8", newline=""
    ) as source:
        rows = list(csv.DictReader(source))
    expected = {
        "micro_flow5_agreement_spread_le_20": {
            "signals": 109,
            "long": 51,
            "short": 58,
            "total": Decimal("16414"),
            "median": Decimal("106"),
            "profit_factor": Decimal("1.8754133333333334"),
        },
        "l5_flow5_agreement_spread_le_20": {
            "signals": 88,
            "long": 21,
            "short": 67,
            "total": Decimal("7401"),
            "median": Decimal("100.5"),
            "profit_factor": Decimal("1.4207743476036159"),
        },
    }
    for strategy, target in expected.items():
        selected = [row for row in rows if row["strategy"] == strategy]
        pnl = [Decimal(row["pnl_ticks"]) for row in selected]
        assert len(selected) == target["signals"]
        assert sum(row["side"] == "LONG" for row in selected) == target["long"]
        assert sum(row["side"] == "SHORT" for row in selected) == target["short"]
        assert sum(pnl) == target["total"]
        assert Decimal(str(statistics.median(pnl))) == target["median"]
        profit_factor = sum(value for value in pnl if value > 0) / -sum(
            value for value in pnl if value < 0
        )
        assert abs(profit_factor - target["profit_factor"]) < Decimal("1e-15")
