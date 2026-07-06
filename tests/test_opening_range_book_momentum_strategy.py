"""Tests for opening range book momentum strategy decisions."""

from dataclasses import replace
from datetime import datetime, time, timedelta
from decimal import Decimal

from neo_trader.strategy.opening_range_book_momentum import (
    OpeningRangeBookMomentumConfig,
    OpeningRangeBookMomentumStrategy,
    ReasonCode,
    SignalAction,
)


def test_strategy_emits_buy_on_confirmed_opening_range_breakout() -> None:
    config = _config()
    strategy = OpeningRangeBookMomentumStrategy(config)
    candles = _candles_with_latest(
        close=Decimal("102"),
        high=Decimal("102.2"),
        low=Decimal("101.2"),
    )

    signal = strategy.evaluate(
        rolling_candles=candles,
        latest_orderbook_features=_book_features(direction="buy"),
        current_position={"side": "FLAT", "quantity": "0"},
        current_time=datetime(2026, 7, 6, 10, 35),
    )

    assert signal.action is SignalAction.BUY
    assert signal.reason_codes == (
        ReasonCode.LONG_BREAKOUT,
        ReasonCode.ORDERBOOK_CONFIRMATION,
    )
    assert signal.suggested_stop is not None
    assert signal.suggested_stop < Decimal("102")
    assert len(signal.suggested_take_profits) == 2
    assert signal.suggested_take_profits[0] > Decimal("102")
    assert signal.confidence_score >= config.min_confidence


def test_strategy_emits_sell_on_confirmed_opening_range_breakdown() -> None:
    config = _config()
    strategy = OpeningRangeBookMomentumStrategy(config)
    candles = _candles_with_latest(close=Decimal("98"), high=Decimal("98.8"), low=Decimal("97.8"))

    signal = strategy.evaluate(
        rolling_candles=candles,
        latest_orderbook_features=_book_features(direction="sell"),
        current_position=None,
        current_time=datetime(2026, 7, 6, 10, 35),
    )

    assert signal.action is SignalAction.SELL
    assert signal.reason_codes == (
        ReasonCode.SHORT_BREAKDOWN,
        ReasonCode.ORDERBOOK_CONFIRMATION,
    )
    assert signal.suggested_stop is not None
    assert signal.suggested_stop > Decimal("98")
    assert signal.suggested_take_profits[0] < Decimal("98")
    assert signal.confidence_score >= config.min_confidence


def test_strategy_holds_when_breakout_is_not_confirmed_by_orderbook() -> None:
    strategy = OpeningRangeBookMomentumStrategy(_config())
    candles = _candles_with_latest(
        close=Decimal("102"),
        high=Decimal("102.2"),
        low=Decimal("101.2"),
    )
    weak_features = _book_features(direction="buy") | {
        "imbalance": Decimal("0.01"),
        "weighted_imbalance": Decimal("0.01"),
    }

    signal = strategy.evaluate(
        rolling_candles=candles,
        latest_orderbook_features=weak_features,
        current_position=None,
        current_time=datetime(2026, 7, 6, 10, 35),
    )

    assert signal.action is SignalAction.HOLD
    assert signal.reason_codes == (ReasonCode.ORDERBOOK_REJECTION,)


def test_strategy_holds_without_breakout() -> None:
    strategy = OpeningRangeBookMomentumStrategy(_config())

    signal = strategy.evaluate(
        rolling_candles=_candles_with_latest(
            close=Decimal("100.5"),
            high=Decimal("100.8"),
            low=Decimal("100.1"),
        ),
        latest_orderbook_features=_book_features(direction="buy"),
        current_position=None,
        current_time=datetime(2026, 7, 6, 10, 35),
    )

    assert signal.action is SignalAction.HOLD
    assert signal.reason_codes == (ReasonCode.NO_BREAKOUT,)


def test_strategy_exits_long_position_on_suggested_stop() -> None:
    strategy = OpeningRangeBookMomentumStrategy(_config())

    signal = strategy.evaluate(
        rolling_candles=_candles_with_latest(
            close=Decimal("98.5"),
            high=Decimal("99"),
            low=Decimal("98.2"),
        ),
        latest_orderbook_features=_book_features(direction="sell"),
        current_position={
            "side": "LONG",
            "quantity": "1",
            "avg_entry_price": "102",
        },
        current_time=datetime(2026, 7, 6, 10, 35),
    )

    assert signal.action is SignalAction.EXIT
    assert signal.reason_codes == (ReasonCode.STOP_LOSS,)
    assert signal.suggested_stop is not None
    assert signal.confidence_score == Decimal("1")


def test_strategy_exits_position_near_session_close() -> None:
    strategy = OpeningRangeBookMomentumStrategy(_config())

    signal = strategy.evaluate(
        rolling_candles=_candles_with_latest(
            close=Decimal("101"),
            high=Decimal("101.2"),
            low=Decimal("100.8"),
        ),
        latest_orderbook_features=_book_features(direction="buy"),
        current_position={
            "side": "LONG",
            "quantity": "1",
            "avg_entry_price": "100.5",
        },
        current_time=datetime(2026, 7, 6, 18, 41),
    )

    assert signal.action is SignalAction.EXIT
    assert signal.reason_codes == (ReasonCode.EXIT_TIME,)


def test_strategy_rejects_entry_when_spread_is_too_wide() -> None:
    strategy = OpeningRangeBookMomentumStrategy(_config())
    features = _book_features(direction="buy") | {"spread_bps": Decimal("25")}

    signal = strategy.evaluate(
        rolling_candles=_candles_with_latest(
            close=Decimal("102"),
            high=Decimal("102.2"),
            low=Decimal("101.2"),
        ),
        latest_orderbook_features=features,
        current_position=None,
        current_time=datetime(2026, 7, 6, 10, 35),
    )

    assert signal.action is SignalAction.HOLD
    assert signal.reason_codes == (ReasonCode.SPREAD_TOO_WIDE,)


def test_strategy_rejects_entry_when_volatility_percentile_is_too_low() -> None:
    strategy = OpeningRangeBookMomentumStrategy(_config())

    signal = strategy.evaluate(
        rolling_candles=_candles_with_latest(
            close=Decimal("102"),
            high=Decimal("102.2"),
            low=Decimal("101.2"),
        ),
        latest_orderbook_features=_book_features(direction="buy")
        | {"volatility_percentile": Decimal("5")},
        current_position=None,
        current_time=datetime(2026, 7, 6, 10, 35),
    )

    assert signal.action is SignalAction.HOLD
    assert signal.reason_codes == (ReasonCode.VOLATILITY_TOO_LOW,)


def test_strategy_rejects_entry_when_volatility_regime_is_too_high() -> None:
    strategy = OpeningRangeBookMomentumStrategy(_config())

    signal = strategy.evaluate(
        rolling_candles=_candles_with_latest(
            close=Decimal("102"),
            high=Decimal("102.2"),
            low=Decimal("101.2"),
        ),
        latest_orderbook_features=_book_features(direction="buy")
        | {"volatility_regime": "extreme"},
        current_position=None,
        current_time=datetime(2026, 7, 6, 10, 35),
    )

    assert signal.action is SignalAction.HOLD
    assert signal.reason_codes == (ReasonCode.VOLATILITY_TOO_HIGH,)


def test_strategy_rejects_long_entry_below_vwap() -> None:
    strategy = OpeningRangeBookMomentumStrategy(_config())

    signal = strategy.evaluate(
        rolling_candles=_candles_with_latest(
            close=Decimal("102"),
            high=Decimal("102.2"),
            low=Decimal("101.2"),
        ),
        latest_orderbook_features=_book_features(direction="buy") | {"vwap": Decimal("103")},
        current_position=None,
        current_time=datetime(2026, 7, 6, 10, 35),
    )

    assert signal.action is SignalAction.HOLD
    assert signal.reason_codes == (ReasonCode.PRICE_BELOW_VWAP,)


def test_strategy_rejects_short_entry_above_vwap() -> None:
    strategy = OpeningRangeBookMomentumStrategy(_config())

    signal = strategy.evaluate(
        rolling_candles=_candles_with_latest(
            close=Decimal("98"),
            high=Decimal("98.8"),
            low=Decimal("97.8"),
        ),
        latest_orderbook_features=_book_features(direction="sell") | {"vwap": Decimal("97")},
        current_position=None,
        current_time=datetime(2026, 7, 6, 10, 35),
    )

    assert signal.action is SignalAction.HOLD
    assert signal.reason_codes == (ReasonCode.PRICE_ABOVE_VWAP,)


def test_strategy_rejects_entry_when_ema_trend_disagrees() -> None:
    strategy = OpeningRangeBookMomentumStrategy(_config())

    signal = strategy.evaluate(
        rolling_candles=_candles_with_latest(
            close=Decimal("102"),
            high=Decimal("102.2"),
            low=Decimal("101.2"),
        ),
        latest_orderbook_features=_book_features(direction="buy")
        | {"ema_fast": Decimal("99"), "ema_slow": Decimal("100")},
        current_position=None,
        current_time=datetime(2026, 7, 6, 10, 35),
    )

    assert signal.action is SignalAction.HOLD
    assert signal.reason_codes == (ReasonCode.TREND_FILTER_REJECTED,)


def test_strategy_rejects_entry_when_expected_slippage_is_too_high() -> None:
    strategy = OpeningRangeBookMomentumStrategy(_config())

    signal = strategy.evaluate(
        rolling_candles=_candles_with_latest(
            close=Decimal("102"),
            high=Decimal("102.2"),
            low=Decimal("101.2"),
        ),
        latest_orderbook_features=_book_features(direction="buy")
        | {"expected_slippage_bps": Decimal("99")},
        current_position=None,
        current_time=datetime(2026, 7, 6, 10, 35),
    )

    assert signal.action is SignalAction.HOLD
    assert signal.reason_codes == (ReasonCode.SLIPPAGE_TOO_HIGH,)


def test_strategy_rejects_entry_when_required_ofi_is_not_confirmed() -> None:
    config = replace(
        _config(),
        require_ofi_confirmation=True,
        min_ofi_confirmation=Decimal("0.10"),
    )
    strategy = OpeningRangeBookMomentumStrategy(config)

    signal = strategy.evaluate(
        rolling_candles=_candles_with_latest(
            close=Decimal("102"),
            high=Decimal("102.2"),
            low=Decimal("101.2"),
        ),
        latest_orderbook_features=_book_features(direction="buy") | {"ofi": Decimal("0.01")},
        current_position=None,
        current_time=datetime(2026, 7, 6, 10, 35),
    )

    assert signal.action is SignalAction.HOLD
    assert signal.reason_codes == (ReasonCode.OFI_NOT_CONFIRMED,)


def _config() -> OpeningRangeBookMomentumConfig:
    return OpeningRangeBookMomentumConfig(
        session_start=time(10, 0),
        session_end=time(18, 45),
        opening_range_minutes=30,
        entry_window_minutes=120,
        breakout_buffer_bps=Decimal("1"),
        max_spread_bps=Decimal("10"),
        min_imbalance=Decimal("0.15"),
        min_weighted_imbalance=Decimal("0.10"),
        min_microprice_edge_bps=Decimal("1"),
        min_confidence=Decimal("0.50"),
        atr_window=3,
    )


def _candles_with_latest(
    *,
    close: Decimal,
    high: Decimal,
    low: Decimal,
) -> list[dict[str, Decimal | datetime]]:
    start = datetime(2026, 7, 6, 10, 0)
    candles: list[dict[str, Decimal | datetime]] = [
        {
            "timestamp": start + timedelta(minutes=0),
            "high": Decimal("100"),
            "low": Decimal("99"),
            "close": Decimal("99.5"),
        },
        {
            "timestamp": start + timedelta(minutes=5),
            "high": Decimal("101"),
            "low": Decimal("99.5"),
            "close": Decimal("100.5"),
        },
        {
            "timestamp": start + timedelta(minutes=10),
            "high": Decimal("100.8"),
            "low": Decimal("99.2"),
            "close": Decimal("100"),
        },
        {
            "timestamp": start + timedelta(minutes=20),
            "high": Decimal("100.6"),
            "low": Decimal("99.4"),
            "close": Decimal("100.1"),
        },
        {
            "timestamp": start + timedelta(minutes=35),
            "high": high,
            "low": low,
            "close": close,
        },
    ]
    return candles


def _book_features(*, direction: str) -> dict[str, Decimal]:
    if direction == "buy":
        return {
            "mid_price": Decimal("102"),
            "spread_bps": Decimal("2"),
            "imbalance": Decimal("0.45"),
            "weighted_imbalance": Decimal("0.35"),
            "microprice": Decimal("102.05"),
            "bid_wall_score": Decimal("1.4"),
            "ask_wall_score": Decimal("1.2"),
        }
    return {
        "mid_price": Decimal("98"),
        "spread_bps": Decimal("2"),
        "imbalance": Decimal("-0.45"),
        "weighted_imbalance": Decimal("-0.35"),
        "microprice": Decimal("97.95"),
        "bid_wall_score": Decimal("1.2"),
        "ask_wall_score": Decimal("1.4"),
    }
