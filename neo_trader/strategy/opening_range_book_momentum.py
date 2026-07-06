"""Opening range breakout strategy with order-book momentum confirmation.

The strategy is a pure decision component. It never places, modifies, or
cancels orders.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import Literal, TypeAlias

from neo_trader.features.volatility import atr

NumericInput: TypeAlias = Decimal | float | int | str
CandleInput: TypeAlias = Mapping[str, object] | object
FeatureInput: TypeAlias = Mapping[str, object] | object
PositionInput: TypeAlias = Mapping[str, object] | object | None

BPS_FACTOR = Decimal("10000")


class SignalAction(StrEnum):
    """Supported strategy outputs."""

    BUY = "BUY"
    SELL = "SELL"
    EXIT = "EXIT"
    HOLD = "HOLD"


class PositionSide(StrEnum):
    """Normalized current position side."""

    FLAT = "FLAT"
    LONG = "LONG"
    SHORT = "SHORT"


class ReasonCode(StrEnum):
    """Machine-readable signal reason codes."""

    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    OPENING_RANGE_NOT_READY = "OPENING_RANGE_NOT_READY"
    OUTSIDE_ENTRY_WINDOW = "OUTSIDE_ENTRY_WINDOW"
    SPREAD_TOO_WIDE = "SPREAD_TOO_WIDE"
    LONG_BREAKOUT = "LONG_BREAKOUT"
    SHORT_BREAKDOWN = "SHORT_BREAKDOWN"
    ORDERBOOK_CONFIRMATION = "ORDERBOOK_CONFIRMATION"
    ORDERBOOK_REJECTION = "ORDERBOOK_REJECTION"
    LOW_CONFIDENCE = "LOW_CONFIDENCE"
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"
    EXIT_TIME = "EXIT_TIME"
    OPPOSITE_BREAKOUT = "OPPOSITE_BREAKOUT"
    POSITION_HELD = "POSITION_HELD"
    NO_BREAKOUT = "NO_BREAKOUT"


@dataclass(frozen=True)
class StrategyPosition:
    """Current position snapshot consumed by the strategy."""

    side: PositionSide = PositionSide.FLAT
    quantity: Decimal = Decimal("0")
    avg_entry_price: Decimal | None = None

    @property
    def is_flat(self) -> bool:
        return self.side is PositionSide.FLAT or self.quantity == 0


@dataclass(frozen=True)
class OpeningRangeBookMomentumConfig:
    """Configuration for opening range book momentum decisions."""

    session_start: time = time(10, 0)
    session_end: time = time(18, 45)
    opening_range_minutes: int = 30
    opening_range_candle_count: int | None = None
    entry_window_minutes: int = 120
    force_exit_minutes_before_close: int = 5
    breakout_buffer_bps: Decimal = Decimal("2")
    max_spread_bps: Decimal = Decimal("10")
    min_imbalance: Decimal = Decimal("0.15")
    min_weighted_imbalance: Decimal = Decimal("0.10")
    min_microprice_edge_bps: Decimal = Decimal("1")
    max_opposing_wall_score: Decimal = Decimal("3")
    min_confidence: Decimal = Decimal("0.55")
    atr_window: int = 14
    stop_atr_multiple: Decimal = Decimal("1")
    take_profit_r_multiples: tuple[Decimal, ...] = (
        Decimal("1"),
        Decimal("2"),
    )

    def __post_init__(self) -> None:
        if self.opening_range_minutes <= 0:
            raise ValueError("opening_range_minutes must be positive.")
        if self.opening_range_candle_count is not None and self.opening_range_candle_count <= 0:
            raise ValueError("opening_range_candle_count must be positive when provided.")
        if self.entry_window_minutes <= 0:
            raise ValueError("entry_window_minutes must be positive.")
        if self.force_exit_minutes_before_close < 0:
            raise ValueError("force_exit_minutes_before_close must be non-negative.")
        if self.max_spread_bps <= 0:
            raise ValueError("max_spread_bps must be positive.")
        if self.atr_window <= 0:
            raise ValueError("atr_window must be positive.")
        if self.stop_atr_multiple < 0:
            raise ValueError("stop_atr_multiple must be non-negative.")
        if not self.take_profit_r_multiples:
            raise ValueError("take_profit_r_multiples must not be empty.")
        if any(value <= 0 for value in self.take_profit_r_multiples):
            raise ValueError("take_profit_r_multiples must contain positive values.")


@dataclass(frozen=True)
class Signal:
    """Strategy decision output."""

    action: SignalAction
    reason_codes: tuple[ReasonCode, ...]
    suggested_stop: Decimal | None = None
    suggested_take_profits: tuple[Decimal, ...] = ()
    confidence_score: Decimal = Decimal("0")


@dataclass(frozen=True)
class _OpeningRange:
    high: Decimal
    low: Decimal
    end_time: datetime | None

    @property
    def width(self) -> Decimal:
        return self.high - self.low


@dataclass(frozen=True)
class _BookFeatures:
    mid_price: Decimal
    spread_bps: Decimal
    imbalance: Decimal
    weighted_imbalance: Decimal
    microprice: Decimal
    bid_wall_score: Decimal
    ask_wall_score: Decimal


class OpeningRangeBookMomentumStrategy:
    """Opening range breakout strategy confirmed by order-book momentum."""

    def __init__(self, config: OpeningRangeBookMomentumConfig | None = None) -> None:
        self.config = config or OpeningRangeBookMomentumConfig()

    def evaluate(
        self,
        *,
        rolling_candles: Sequence[CandleInput],
        latest_orderbook_features: FeatureInput,
        current_position: PositionInput,
        current_time: datetime,
        config: OpeningRangeBookMomentumConfig | None = None,
    ) -> Signal:
        """Return BUY, SELL, EXIT, or HOLD without submitting orders."""

        resolved_config = config or self.config
        if len(rolling_candles) < 2:
            return Signal(SignalAction.HOLD, (ReasonCode.INSUFFICIENT_DATA,))

        opening_range = _build_opening_range(
            rolling_candles,
            current_time=current_time,
            config=resolved_config,
        )
        if opening_range is None:
            return Signal(SignalAction.HOLD, (ReasonCode.OPENING_RANGE_NOT_READY,))

        latest_close = _positive_decimal(_field_any(rolling_candles[-1], "close", "c"), "close")
        position = _normalize_position(current_position)
        features = _normalize_book_features(latest_orderbook_features)

        if features.spread_bps > resolved_config.max_spread_bps:
            return self._spread_rejection(position, latest_close, opening_range, resolved_config)

        long_breakout = _is_long_breakout(latest_close, opening_range, resolved_config)
        short_breakdown = _is_short_breakdown(latest_close, opening_range, resolved_config)

        if not position.is_flat:
            return self._evaluate_position(
                position=position,
                latest_close=latest_close,
                opening_range=opening_range,
                features=features,
                rolling_candles=rolling_candles,
                current_time=current_time,
                config=resolved_config,
                long_breakout=long_breakout,
                short_breakdown=short_breakdown,
            )

        if not _inside_entry_window(current_time, opening_range, resolved_config):
            return Signal(SignalAction.HOLD, (ReasonCode.OUTSIDE_ENTRY_WINDOW,))

        if long_breakout and self._book_confirms("BUY", features, resolved_config):
            return self._entry_signal(
                action=SignalAction.BUY,
                base_reason=ReasonCode.LONG_BREAKOUT,
                latest_close=latest_close,
                opening_range=opening_range,
                features=features,
                rolling_candles=rolling_candles,
                config=resolved_config,
            )

        if short_breakdown and self._book_confirms("SELL", features, resolved_config):
            return self._entry_signal(
                action=SignalAction.SELL,
                base_reason=ReasonCode.SHORT_BREAKDOWN,
                latest_close=latest_close,
                opening_range=opening_range,
                features=features,
                rolling_candles=rolling_candles,
                config=resolved_config,
            )

        if long_breakout or short_breakdown:
            return Signal(
                SignalAction.HOLD,
                (ReasonCode.ORDERBOOK_REJECTION,),
                confidence_score=self._confidence(
                    "BUY" if long_breakout else "SELL",
                    latest_close,
                    opening_range,
                    features,
                    resolved_config,
                ),
            )

        return Signal(SignalAction.HOLD, (ReasonCode.NO_BREAKOUT,))

    def __call__(
        self,
        *,
        rolling_candles: Sequence[CandleInput],
        latest_orderbook_features: FeatureInput,
        current_position: PositionInput,
        current_time: datetime,
        config: OpeningRangeBookMomentumConfig | None = None,
    ) -> Signal:
        return self.evaluate(
            rolling_candles=rolling_candles,
            latest_orderbook_features=latest_orderbook_features,
            current_position=current_position,
            current_time=current_time,
            config=config,
        )

    def _entry_signal(
        self,
        *,
        action: Literal[SignalAction.BUY, SignalAction.SELL],
        base_reason: ReasonCode,
        latest_close: Decimal,
        opening_range: _OpeningRange,
        features: _BookFeatures,
        rolling_candles: Sequence[CandleInput],
        config: OpeningRangeBookMomentumConfig,
    ) -> Signal:
        confidence = self._confidence(
            _direction_from_action(action),
            latest_close,
            opening_range,
            features,
            config,
        )
        suggested_stop, suggested_take_profits = _risk_plan(
            action=action,
            entry_price=latest_close,
            opening_range=opening_range,
            rolling_candles=rolling_candles,
            config=config,
        )
        if confidence < config.min_confidence:
            return Signal(
                SignalAction.HOLD,
                (base_reason, ReasonCode.ORDERBOOK_CONFIRMATION, ReasonCode.LOW_CONFIDENCE),
                suggested_stop=suggested_stop,
                suggested_take_profits=suggested_take_profits,
                confidence_score=confidence,
            )
        return Signal(
            action,
            (base_reason, ReasonCode.ORDERBOOK_CONFIRMATION),
            suggested_stop=suggested_stop,
            suggested_take_profits=suggested_take_profits,
            confidence_score=confidence,
        )

    def _evaluate_position(
        self,
        *,
        position: StrategyPosition,
        latest_close: Decimal,
        opening_range: _OpeningRange,
        features: _BookFeatures,
        rolling_candles: Sequence[CandleInput],
        current_time: datetime,
        config: OpeningRangeBookMomentumConfig,
        long_breakout: bool,
        short_breakdown: bool,
    ) -> Signal:
        action = SignalAction.BUY if position.side is PositionSide.LONG else SignalAction.SELL
        entry_price = position.avg_entry_price or latest_close
        suggested_stop, suggested_take_profits = _risk_plan(
            action=action,
            entry_price=entry_price,
            opening_range=opening_range,
            rolling_candles=rolling_candles,
            config=config,
        )

        if _past_forced_exit_time(current_time, config):
            return Signal(
                SignalAction.EXIT,
                (ReasonCode.EXIT_TIME,),
                suggested_stop=suggested_stop,
                suggested_take_profits=suggested_take_profits,
                confidence_score=Decimal("1"),
            )

        if position.side is PositionSide.LONG:
            if suggested_stop is not None and latest_close <= suggested_stop:
                return Signal(
                    SignalAction.EXIT,
                    (ReasonCode.STOP_LOSS,),
                    suggested_stop=suggested_stop,
                    suggested_take_profits=suggested_take_profits,
                    confidence_score=Decimal("1"),
                )
            if suggested_take_profits and latest_close >= suggested_take_profits[0]:
                return Signal(
                    SignalAction.EXIT,
                    (ReasonCode.TAKE_PROFIT,),
                    suggested_stop=suggested_stop,
                    suggested_take_profits=suggested_take_profits,
                    confidence_score=Decimal("1"),
                )
            if short_breakdown and self._book_confirms("SELL", features, config):
                return Signal(
                    SignalAction.EXIT,
                    (ReasonCode.OPPOSITE_BREAKOUT,),
                    suggested_stop=suggested_stop,
                    suggested_take_profits=suggested_take_profits,
                    confidence_score=self._confidence(
                        "SELL",
                        latest_close,
                        opening_range,
                        features,
                        config,
                    ),
                )
        elif position.side is PositionSide.SHORT:
            if suggested_stop is not None and latest_close >= suggested_stop:
                return Signal(
                    SignalAction.EXIT,
                    (ReasonCode.STOP_LOSS,),
                    suggested_stop=suggested_stop,
                    suggested_take_profits=suggested_take_profits,
                    confidence_score=Decimal("1"),
                )
            if suggested_take_profits and latest_close <= suggested_take_profits[0]:
                return Signal(
                    SignalAction.EXIT,
                    (ReasonCode.TAKE_PROFIT,),
                    suggested_stop=suggested_stop,
                    suggested_take_profits=suggested_take_profits,
                    confidence_score=Decimal("1"),
                )
            if long_breakout and self._book_confirms("BUY", features, config):
                return Signal(
                    SignalAction.EXIT,
                    (ReasonCode.OPPOSITE_BREAKOUT,),
                    suggested_stop=suggested_stop,
                    suggested_take_profits=suggested_take_profits,
                    confidence_score=self._confidence(
                        "BUY",
                        latest_close,
                        opening_range,
                        features,
                        config,
                    ),
                )

        return Signal(
            SignalAction.HOLD,
            (ReasonCode.POSITION_HELD,),
            suggested_stop=suggested_stop,
            suggested_take_profits=suggested_take_profits,
            confidence_score=Decimal("0.5"),
        )

    def _spread_rejection(
        self,
        position: StrategyPosition,
        latest_close: Decimal,
        opening_range: _OpeningRange,
        config: OpeningRangeBookMomentumConfig,
    ) -> Signal:
        if position.is_flat:
            return Signal(SignalAction.HOLD, (ReasonCode.SPREAD_TOO_WIDE,))

        action = SignalAction.BUY if position.side is PositionSide.LONG else SignalAction.SELL
        stop, take_profits = _risk_plan(
            action=action,
            entry_price=position.avg_entry_price or latest_close,
            opening_range=opening_range,
            rolling_candles=(),
            config=config,
        )
        return Signal(
            SignalAction.HOLD,
            (ReasonCode.SPREAD_TOO_WIDE, ReasonCode.POSITION_HELD),
            suggested_stop=stop,
            suggested_take_profits=take_profits,
            confidence_score=Decimal("0"),
        )

    def _book_confirms(
        self,
        direction: Literal["BUY", "SELL"],
        features: _BookFeatures,
        config: OpeningRangeBookMomentumConfig,
    ) -> bool:
        if direction == "BUY":
            microprice_edge_bps = _microprice_edge_bps("BUY", features)
            return (
                features.imbalance >= config.min_imbalance
                and features.weighted_imbalance >= config.min_weighted_imbalance
                and microprice_edge_bps >= config.min_microprice_edge_bps
                and features.ask_wall_score <= config.max_opposing_wall_score
            )

        microprice_edge_bps = _microprice_edge_bps("SELL", features)
        return (
            features.imbalance <= -config.min_imbalance
            and features.weighted_imbalance <= -config.min_weighted_imbalance
            and microprice_edge_bps >= config.min_microprice_edge_bps
            and features.bid_wall_score <= config.max_opposing_wall_score
        )

    def _confidence(
        self,
        direction: Literal["BUY", "SELL"],
        latest_close: Decimal,
        opening_range: _OpeningRange,
        features: _BookFeatures,
        config: OpeningRangeBookMomentumConfig,
    ) -> Decimal:
        if opening_range.width <= 0:
            breakout_score = Decimal("0")
        elif direction == "BUY":
            breakout_score = _clamp((latest_close - opening_range.high) / opening_range.width)
        else:
            breakout_score = _clamp((opening_range.low - latest_close) / opening_range.width)

        if direction == "BUY":
            imbalance_score = _clamp(
                features.imbalance / max(config.min_imbalance, Decimal("0.0001"))
            )
            weighted_score = _clamp(
                features.weighted_imbalance / max(config.min_weighted_imbalance, Decimal("0.0001"))
            )
            microprice_edge_bps = _microprice_edge_bps("BUY", features)
        else:
            imbalance_score = _clamp(
                (-features.imbalance) / max(config.min_imbalance, Decimal("0.0001"))
            )
            weighted_score = _clamp(
                (-features.weighted_imbalance)
                / max(config.min_weighted_imbalance, Decimal("0.0001"))
            )
            microprice_edge_bps = _microprice_edge_bps("SELL", features)

        microprice_score = _clamp(
            microprice_edge_bps / max(config.min_microprice_edge_bps, Decimal("0.0001"))
        )
        spread_score = _clamp(Decimal("1") - (features.spread_bps / config.max_spread_bps))

        return _clamp(
            (breakout_score * Decimal("0.30"))
            + (imbalance_score * Decimal("0.25"))
            + (weighted_score * Decimal("0.20"))
            + (microprice_score * Decimal("0.15"))
            + (spread_score * Decimal("0.10"))
        )


def _build_opening_range(
    candles: Sequence[CandleInput],
    *,
    current_time: datetime,
    config: OpeningRangeBookMomentumConfig,
) -> _OpeningRange | None:
    if config.opening_range_candle_count is not None:
        if len(candles) <= config.opening_range_candle_count:
            return None
        count_opening_candles = candles[: config.opening_range_candle_count]
        return _range_from_candles(count_opening_candles, end_time=None)

    session_start = datetime.combine(current_time.date(), config.session_start)
    opening_end = session_start + timedelta(minutes=config.opening_range_minutes)
    if current_time < opening_end:
        return None

    opening_candles: list[CandleInput] = []
    for candle in candles:
        candle_time = _candle_time(candle)
        if candle_time is not None and session_start <= candle_time < opening_end:
            opening_candles.append(candle)

    if not opening_candles:
        return None
    return _range_from_candles(opening_candles, end_time=opening_end)


def _range_from_candles(
    candles: Sequence[CandleInput],
    *,
    end_time: datetime | None,
) -> _OpeningRange:
    highs = [_positive_decimal(_field_any(candle, "high", "h"), "high") for candle in candles]
    lows = [_positive_decimal(_field_any(candle, "low", "l"), "low") for candle in candles]
    return _OpeningRange(high=max(highs), low=min(lows), end_time=end_time)


def _inside_entry_window(
    current_time: datetime,
    opening_range: _OpeningRange,
    config: OpeningRangeBookMomentumConfig,
) -> bool:
    if opening_range.end_time is None:
        return True
    entry_end = opening_range.end_time + timedelta(minutes=config.entry_window_minutes)
    return opening_range.end_time <= current_time <= entry_end


def _past_forced_exit_time(
    current_time: datetime,
    config: OpeningRangeBookMomentumConfig,
) -> bool:
    exit_time = datetime.combine(current_time.date(), config.session_end) - timedelta(
        minutes=config.force_exit_minutes_before_close
    )
    return current_time >= exit_time


def _is_long_breakout(
    latest_close: Decimal,
    opening_range: _OpeningRange,
    config: OpeningRangeBookMomentumConfig,
) -> bool:
    threshold = opening_range.high * (Decimal("1") + (config.breakout_buffer_bps / BPS_FACTOR))
    return latest_close > threshold


def _is_short_breakdown(
    latest_close: Decimal,
    opening_range: _OpeningRange,
    config: OpeningRangeBookMomentumConfig,
) -> bool:
    threshold = opening_range.low * (Decimal("1") - (config.breakout_buffer_bps / BPS_FACTOR))
    return latest_close < threshold


def _risk_plan(
    *,
    action: SignalAction,
    entry_price: Decimal,
    opening_range: _OpeningRange,
    rolling_candles: Sequence[CandleInput],
    config: OpeningRangeBookMomentumConfig,
) -> tuple[Decimal, tuple[Decimal, ...]]:
    if action not in {SignalAction.BUY, SignalAction.SELL}:
        raise ValueError(f"unsupported risk-plan action: {action}.")

    range_width = max(opening_range.width, Decimal("0"))
    volatility_buffer = atr(rolling_candles, window=config.atr_window) * config.stop_atr_multiple
    fallback_buffer = range_width if range_width > 0 else entry_price * Decimal("0.005")
    risk_buffer = max(volatility_buffer, fallback_buffer * Decimal("0.25"))

    if action is SignalAction.BUY:
        stop = min(opening_range.low, entry_price - risk_buffer)
        risk = entry_price - stop
        take_profits = tuple(
            entry_price + (risk * multiple) for multiple in config.take_profit_r_multiples
        )
    else:
        stop = max(opening_range.high, entry_price + risk_buffer)
        risk = stop - entry_price
        take_profits = tuple(
            entry_price - (risk * multiple) for multiple in config.take_profit_r_multiples
        )

    return stop, take_profits


def _normalize_position(position: PositionInput) -> StrategyPosition:
    if position is None:
        return StrategyPosition()
    side_value = _field(position, "side")
    quantity_value = _field(position, "quantity")
    if quantity_value is None:
        quantity_value = _field(position, "qty")
    avg_entry_value = _field(position, "avg_entry_price")
    if avg_entry_value is None:
        avg_entry_value = _field(position, "entry_price")

    quantity = Decimal("0") if quantity_value is None else _to_decimal(quantity_value)
    side = _normalize_position_side(side_value, quantity)
    avg_entry_price = (
        None
        if avg_entry_value is None
        else _positive_decimal(avg_entry_value, "avg_entry_price")
    )
    return StrategyPosition(side=side, quantity=abs(quantity), avg_entry_price=avg_entry_price)


def _normalize_position_side(value: object | None, quantity: Decimal) -> PositionSide:
    if value is None:
        if quantity > 0:
            return PositionSide.LONG
        if quantity < 0:
            return PositionSide.SHORT
        return PositionSide.FLAT

    normalized = str(value).strip().upper()
    if normalized in {"FLAT", "NONE", "0", ""}:
        return PositionSide.FLAT
    if normalized in {"LONG", "BUY"}:
        return PositionSide.LONG
    if normalized in {"SHORT", "SELL"}:
        return PositionSide.SHORT
    raise ValueError(f"unsupported position side: {value!r}.")


def _normalize_book_features(features: FeatureInput) -> _BookFeatures:
    mid = _positive_decimal(_field_any(features, "mid_price", "mid"), "mid_price")
    return _BookFeatures(
        mid_price=mid,
        spread_bps=_non_negative_decimal(
            _field_any(features, "spread_bps", "spread"),
            "spread_bps",
        ),
        imbalance=_to_decimal(_field_any(features, "imbalance")),
        weighted_imbalance=_to_decimal(
            _field_any(features, "weighted_imbalance", "weighted_orderbook_imbalance")
        ),
        microprice=_positive_decimal(_field_any(features, "microprice"), "microprice"),
        bid_wall_score=_non_negative_decimal(
            _field_default(features, Decimal("1"), "bid_wall_score", "bid_wall"),
            "bid_wall_score",
        ),
        ask_wall_score=_non_negative_decimal(
            _field_default(features, Decimal("1"), "ask_wall_score", "ask_wall"),
            "ask_wall_score",
        ),
    )


def _microprice_edge_bps(
    direction: Literal["BUY", "SELL"],
    features: _BookFeatures,
) -> Decimal:
    if direction == "BUY":
        return ((features.microprice - features.mid_price) / features.mid_price) * BPS_FACTOR
    return ((features.mid_price - features.microprice) / features.mid_price) * BPS_FACTOR


def _direction_from_action(action: SignalAction) -> Literal["BUY", "SELL"]:
    if action is SignalAction.BUY:
        return "BUY"
    if action is SignalAction.SELL:
        return "SELL"
    raise ValueError(f"unsupported directional action: {action}.")


def _field_default(value: FeatureInput, default: object, *names: str) -> object:
    try:
        return _field_any(value, *names)
    except ValueError:
        return default


def _field_any(value: Mapping[str, object] | object, *names: str) -> object:
    for name in names:
        field_value = _field(value, name)
        if field_value is not None:
            return field_value
    joined = ", ".join(names)
    raise ValueError(f"missing required field, expected one of: {joined}.")


def _field(value: Mapping[str, object] | object, name: str) -> object | None:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _candle_time(candle: CandleInput) -> datetime | None:
    for name in ("timestamp", "time", "datetime", "ts"):
        value = _field(candle, name)
        if value is None:
            continue
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        raise TypeError(f"unsupported candle time value: {value!r}.")
    return None


def _positive_decimal(value: object, field_name: str) -> Decimal:
    decimal_value = _to_decimal(value)
    if decimal_value <= 0:
        raise ValueError(f"{field_name} must be positive.")
    return decimal_value


def _non_negative_decimal(value: object, field_name: str) -> Decimal:
    decimal_value = _to_decimal(value)
    if decimal_value < 0:
        raise ValueError(f"{field_name} must be non-negative.")
    return decimal_value


def _to_decimal(value: object) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        raise TypeError("boolean values are not valid numeric strategy values.")
    if isinstance(value, int | str):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    raise TypeError(f"unsupported numeric value: {value!r}.")


def _clamp(value: Decimal) -> Decimal:
    if value < 0:
        return Decimal("0")
    if value > 1:
        return Decimal("1")
    return value


__all__ = [
    "OpeningRangeBookMomentumConfig",
    "OpeningRangeBookMomentumStrategy",
    "PositionSide",
    "ReasonCode",
    "Signal",
    "SignalAction",
    "StrategyPosition",
]
