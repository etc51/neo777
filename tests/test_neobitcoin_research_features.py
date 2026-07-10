from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from neo_trader.neobitcoin_research.experiment import (
    ExperimentRow,
    run_walk_forward_experiment,
)
from neo_trader.neobitcoin_research.features import CausalFeatureEngine, TapeTrade
from neo_trader.neobitcoin_research.models import DirectionalBaseline, ShadowDecision
from neo_trader.neobitcoin_research.validation import (
    TimedSample,
    cluster_events,
    purged_walk_forward_folds,
)


@dataclass(frozen=True)
class _Level:
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True)
class _Book:
    instrument_uid: str
    bids: tuple[_Level, ...]
    asks: tuple[_Level, ...]
    is_consistent: bool = True


def _book(*, consistent: bool = True, shift: Decimal = Decimal(0)) -> _Book:
    bids = tuple(
        _Level(Decimal("100") + shift - Decimal(index) / 10, Decimal(100 + index))
        for index in range(20)
    )
    asks = tuple(
        _Level(Decimal("100.1") + shift + Decimal(index) / 10, Decimal(90 + index))
        for index in range(20)
    )
    return _Book("uid", bids, asks, consistent)


def test_features_reject_inconsistent_book() -> None:
    engine = CausalFeatureEngine(tick_size=Decimal("0.1"))
    now = datetime(2026, 7, 10, tzinfo=UTC)
    assert (
        engine.snapshot(book=_book(consistent=False), timestamp=now, receive_timestamp=now) is None
    )


def test_features_include_depth_trade_flow_and_causal_return() -> None:
    engine = CausalFeatureEngine(tick_size=Decimal("0.1"))
    start = datetime(2026, 7, 10, tzinfo=UTC)
    first = engine.snapshot(book=_book(), timestamp=start, receive_timestamp=start)
    assert first is not None
    engine.add_trade(
        TapeTrade(
            timestamp=start + timedelta(seconds=1),
            price=Decimal("100.1"),
            quantity=Decimal(12),
            side="BUY",
        )
    )
    second = engine.snapshot(
        book=_book(shift=Decimal("0.2")),
        timestamp=start + timedelta(seconds=5),
        receive_timestamp=start + timedelta(seconds=5, milliseconds=12),
    )
    assert second is not None
    assert second["cumulative_bid_depth_20"] == sum(range(100, 120))
    assert second["trades_5s_buy_count"] == 1
    assert second["return_5s"] > 0
    assert second["latency_ms"] == 12
    assert "bid_price_20" in second and "ask_volume_delta_20" in second


def test_trade_classification_is_explicit() -> None:
    engine = CausalFeatureEngine(tick_size=Decimal("0.1"))
    trade = engine.classify_trade(
        timestamp=datetime(2026, 7, 10, tzinfo=UTC),
        price=Decimal("100.1"),
        quantity=Decimal(3),
        best_bid=Decimal(100),
        best_ask=Decimal("100.1"),
    )
    assert trade.side == "BUY"
    assert trade.classification_reason == "at_or_above_ask"


def test_shadow_model_prefers_no_trade_when_spread_not_covered() -> None:
    model = DirectionalBaseline(cooldown_seconds=60)
    prediction = model.predict(
        timestamp=datetime(2026, 7, 10, tzinfo=UTC),
        features={
            "imbalance_5": 0.01,
            "distance_weighted_imbalance": 0.01,
            "multi_level_ofi": 1,
            "cumulative_bid_depth_20": 1000,
            "cumulative_ask_depth_20": 1000,
            "spread_bps": 20,
        },
        position_size_rub=10_000,
    )
    assert prediction.decision is ShadowDecision.NO_TRADE
    assert any("does_not_cover" in reason for reason in prediction.reasons)


def test_purging_embargo_and_event_clustering() -> None:
    start = datetime(2026, 7, 10, tzinfo=UTC)
    samples = tuple(
        TimedSample(
            str(index),
            start + timedelta(minutes=index),
            start + timedelta(minutes=index + 1),
            str(index),
        )
        for index in range(12)
    )
    folds = purged_walk_forward_folds(samples, folds=3, embargo=timedelta(minutes=1))
    for fold in folds:
        assert max(fold.train_indices, default=-1) < min(fold.test_indices)
        test_start = min(samples[index].timestamp for index in fold.test_indices)
        assert all(
            samples[index].horizon_end < test_start - timedelta(minutes=1)
            for index in fold.train_indices
        )
    clusters = cluster_events(
        (start, start + timedelta(seconds=5), start + timedelta(seconds=31)),
        horizon=timedelta(seconds=30),
    )
    assert clusters[0] == clusters[1]
    assert clusters[2] == clusters[1]


def test_walk_forward_experiment_never_trains_on_holdout() -> None:
    start = datetime(2026, 7, 10, tzinfo=UTC)
    rows = tuple(
        ExperimentRow(
            event_id=str(index),
            timestamp=start + timedelta(minutes=index * 20),
            horizon_end=start + timedelta(minutes=index * 20 + 15),
            cluster_id=f"cluster-{index}",
            features={"imbalance_5": (-1.0) ** index * 0.2, "multi_level_ofi": 2 - index},
            aggressive_long_pnl_rub=1.0 if index % 2 == 0 else -1.5,
            aggressive_short_pnl_rub=-1.5 if index % 2 == 0 else 1.0,
        )
        for index in range(24)
    )
    result = run_walk_forward_experiment(
        rows,
        experiment_id="sealed",
        feature_names=("imbalance_5", "multi_level_ofi"),
        folds=3,
        embargo=timedelta(minutes=15),
    )
    assert result.metrics
    assert result.holdout_used_for_training is False
    assert len(result.holdout_fingerprint) == 64
