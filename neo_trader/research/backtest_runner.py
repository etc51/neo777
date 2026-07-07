"""Offline research backtest runner over feature-store parquet files."""

from __future__ import annotations

import csv
import html
import importlib
import json
import os
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from pathlib import Path
from typing import Any, Final, Literal, TypeAlias, cast
from uuid import uuid4
from zoneinfo import ZoneInfo

import yaml

from neo_trader.runtime import get_runtime_commit_hash
from neo_trader.strategy.opening_range_book_momentum import (
    OpeningRangeBookMomentumConfig,
    OpeningRangeBookMomentumStrategy,
    PositionSide,
    ReasonCode,
    SignalAction,
    StrategyPosition,
)

JsonMapping: TypeAlias = Mapping[str, Any]
BPS_FACTOR: Final = Decimal("10000")
MOSCOW_TZ: Final = ZoneInfo("Europe/Moscow")
RESEARCH_ONLY_NOT_FOR_LIVE: Final = "RESEARCH_ONLY_NOT_FOR_LIVE"
DATA_WINDOW_TOO_SHORT: Final = "DATA_WINDOW_TOO_SHORT"


class ResearchStrategyName(StrEnum):
    """Offline research strategy modes."""

    OPENING_RANGE_BOOK_MOMENTUM = "opening_range_book_momentum"
    SIMPLE_BOOK_MOMENTUM_RESEARCH = "simple_book_momentum_research"


class ResearchSessionProfile(StrEnum):
    """Research-only session profiles."""

    AUTO_FROM_DATA = "auto_from_data"
    MOEX_EQUITY = "moex_equity"
    US_NEO_STOCK = "us_neo_stock"
    CRYPTO_NEO = "crypto_neo"
    CUSTOM = "custom"


@dataclass(frozen=True)
class ResearchSessionConfig:
    """Session settings for offline research backtests."""

    session_profile: ResearchSessionProfile = ResearchSessionProfile.AUTO_FROM_DATA
    opening_range_minutes: int = 15
    min_rows_after_opening_range: int = 100
    allow_short_recording_backtest: bool = True
    timezone: str | None = None
    session_start: time | None = None
    session_end: time | None = None

    def __post_init__(self) -> None:
        if self.opening_range_minutes <= 0:
            raise ValueError("opening_range_minutes must be positive.")
        if self.min_rows_after_opening_range < 0:
            raise ValueError("min_rows_after_opening_range must be non-negative.")


@dataclass(frozen=True)
class ResearchSessionWindow:
    """Resolved data/session window diagnostics."""

    profile: ResearchSessionProfile
    timezone: str
    first_timestamp: datetime | None
    last_timestamp: datetime | None
    data_duration_minutes: Decimal
    opening_range_start: datetime | None
    opening_range_end: datetime | None
    rows_before_or_ready: int
    rows_after_or_ready: int
    status: str

    @property
    def data_window_too_short(self) -> bool:
        return self.status == DATA_WINDOW_TOO_SHORT


@dataclass(frozen=True)
class ResearchBacktestConfig:
    """Research-only simulation parameters."""

    fixed_quantity: Decimal = Decimal("1")
    max_rolling_candles: int = 2_000
    flatten_at_end: bool = True

    def __post_init__(self) -> None:
        if self.fixed_quantity <= 0:
            raise ValueError("fixed_quantity must be positive.")
        if self.max_rolling_candles <= 0:
            raise ValueError("max_rolling_candles must be positive.")


@dataclass(frozen=True)
class ResearchTrade:
    """One closed research trade."""

    instrument: str
    instrument_uid: str
    side: PositionSide
    entry_time: datetime
    exit_time: datetime
    quantity: Decimal
    entry_price: Decimal
    exit_price: Decimal
    pnl: Decimal
    mae: Decimal
    mfe: Decimal
    entry_slippage_bps: Decimal
    exit_slippage_bps: Decimal
    entry_spread_regime: str
    entry_volatility_regime: str
    entry_reason_codes: tuple[str, ...]
    exit_reason_codes: tuple[str, ...]


@dataclass(frozen=True)
class ResearchBacktestMetrics:
    """Aggregate research metrics required by reports."""

    total_pnl: Decimal
    trades: int
    winrate: Decimal
    profit_factor: Decimal | None
    max_drawdown: Decimal
    avg_slippage_bps: Decimal
    missed_fill_rate: Decimal
    pnl_by_instrument: Mapping[str, Decimal]
    pnl_by_hour: Mapping[str, Decimal]
    pnl_by_spread_regime: Mapping[str, Decimal]
    pnl_by_volatility_regime: Mapping[str, Decimal]
    reason_code_distribution: Mapping[str, int]
    reject_reason_distribution: Mapping[str, int]


@dataclass(frozen=True)
class ResearchBacktestArtifacts:
    """Report files written by the research runner."""

    json_path: Path
    trades_csv_path: Path
    summary_html_path: Path
    diagnostics_json_path: Path


@dataclass(frozen=True)
class ResearchBacktestResult:
    """Research backtest output."""

    metrics: ResearchBacktestMetrics
    trades: tuple[ResearchTrade, ...]
    artifacts: ResearchBacktestArtifacts
    feature_rows: int
    session_window: ResearchSessionWindow
    strategy_name: ResearchStrategyName
    research_mode_marker: str | None = None
    commit_hash: str = field(default_factory=get_runtime_commit_hash)


@dataclass(frozen=True)
class _FeatureEvent:
    timestamp: datetime
    strategy_time: datetime
    instrument: str
    instrument_uid: str
    row: Mapping[str, object]


@dataclass
class _OpenTrade:
    instrument: str
    instrument_uid: str
    side: PositionSide
    entry_time: datetime
    quantity: Decimal
    entry_price: Decimal
    entry_slippage_bps: Decimal
    entry_spread_regime: str
    entry_volatility_regime: str
    entry_reason_codes: tuple[str, ...]
    mae: Decimal = Decimal("0")
    mfe: Decimal = Decimal("0")


@dataclass
class _InstrumentState:
    rolling_candles: list[dict[str, object]] = field(default_factory=list)
    open_trade: _OpenTrade | None = None


def run_research_backtest(
    *,
    features_path: Path | str = Path("data/features"),
    reports_dir: Path | str = Path("data/reports"),
    active_universe_path: Path | str = Path("configs/active_universe.yaml"),
    strategy_config_path: Path | str = Path("configs/strategy.yaml"),
    research_config_path: Path | str = Path("configs/research.yaml"),
    config: ResearchBacktestConfig | None = None,
    strategy_config: OpeningRangeBookMomentumConfig | None = None,
    research_config: ResearchSessionConfig | None = None,
    strategy_name: ResearchStrategyName | str = ResearchStrategyName.OPENING_RANGE_BOOK_MOMENTUM,
) -> ResearchBacktestResult:
    """Run the strategy over feature-store rows and export JSON/CSV/HTML reports."""

    resolved_config = config or ResearchBacktestConfig()
    selected_strategy = _strategy_name(strategy_name)
    resolved_research_config = research_config or load_research_config(Path(research_config_path))
    research_timezone = ZoneInfo(_research_timezone(resolved_research_config))
    active_tickers = _load_active_tickers(Path(active_universe_path))
    events = _read_feature_events(
        Path(features_path),
        active_tickers=active_tickers,
        timezone=research_timezone,
    )
    session_window = _resolve_session_window(events, resolved_research_config)
    resolved_strategy_config = _research_strategy_config(
        strategy_config or load_strategy_config(Path(strategy_config_path)),
        research_config=resolved_research_config,
        session_window=session_window,
    )
    strategy = OpeningRangeBookMomentumStrategy(resolved_strategy_config)

    states: dict[str, _InstrumentState] = {}
    trades: list[ResearchTrade] = []
    slippages: list[Decimal] = []
    reason_counts: Counter[str] = Counter()
    entry_signals = 0
    missed_fills = 0

    if (
        session_window.data_window_too_short
        and not resolved_research_config.allow_short_recording_backtest
    ):
        reason_counts[DATA_WINDOW_TOO_SHORT] = max(len(events), 1)
    else:
        for event in events:
            state = states.setdefault(event.instrument_uid, _InstrumentState())
            if not _bool(event.row.get("usable_row")):
                reason_counts["UNUSABLE_ROW"] += 1
                continue

            mid = _decimal(event.row.get("mid"), "mid")
            _observe_synthetic_candle(state, event.strategy_time, mid)
            state.rolling_candles = _trim_rolling_candles(
                state.rolling_candles,
                max_rolling_candles=resolved_config.max_rolling_candles,
                session_window=session_window,
                preserve_opening_range=(
                    selected_strategy
                    is ResearchStrategyName.OPENING_RANGE_BOOK_MOMENTUM
                ),
            )

            if state.open_trade is not None:
                _observe_excursion(state.open_trade, mid)

            if selected_strategy is ResearchStrategyName.SIMPLE_BOOK_MOMENTUM_RESEARCH:
                action, reason_codes = _simple_research_signal(
                    event=event,
                    open_trade=state.open_trade,
                    config=resolved_strategy_config,
                )
            else:
                try:
                    signal = strategy.evaluate(
                        rolling_candles=state.rolling_candles,
                        latest_orderbook_features=_strategy_features(event.row),
                        current_position=_strategy_position(state.open_trade),
                        current_time=event.strategy_time,
                        config=resolved_strategy_config,
                    )
                except (ArithmeticError, InvalidOperation, TypeError, ValueError) as exc:
                    reason_counts[f"STRATEGY_ERROR:{type(exc).__name__}"] += 1
                    continue
                action = signal.action
                reason_codes = tuple(_reason_code_value(reason) for reason in signal.reason_codes)

            reason_counts.update(reason_codes)

            if action in {SignalAction.BUY, SignalAction.SELL}:
                entry_signals += 1
                if state.open_trade is not None:
                    continue
                open_trade = _entry_trade(
                    event=event,
                    action=action,
                    quantity=resolved_config.fixed_quantity,
                    reason_codes=reason_codes,
                )
                if open_trade is None:
                    missed_fills += 1
                    continue
                slippages.append(open_trade.entry_slippage_bps)
                state.open_trade = _observe_excursion(open_trade, mid)
                continue

            if action is SignalAction.EXIT and state.open_trade is not None:
                closed, exit_slippage = _close_trade(
                    event=event,
                    open_trade=state.open_trade,
                    reason_codes=reason_codes,
                )
                if closed is None:
                    missed_fills += 1
                    continue
                slippages.append(exit_slippage)
                trades.append(closed)
                state.open_trade = None

    if resolved_config.flatten_at_end:
        last_events = {
            event.instrument_uid: event
            for event in events
            if _bool(event.row.get("usable_row"))
        }
        for instrument_uid, state in states.items():
            if state.open_trade is None:
                continue
            last_event = last_events.get(instrument_uid)
            if last_event is None:
                continue
            closed, exit_slippage = _close_trade(
                event=last_event,
                open_trade=state.open_trade,
                reason_codes=("END_OF_DATA",),
            )
            if closed is not None:
                slippages.append(exit_slippage)
                trades.append(closed)
                state.open_trade = None

    metrics = _build_metrics(
        trades=trades,
        slippages=slippages,
        reason_counts=reason_counts,
        entry_signals=entry_signals,
        missed_fills=missed_fills,
        timezone=research_timezone,
    )
    artifacts = _write_reports(
        reports_dir=Path(reports_dir),
        features_path=Path(features_path),
        metrics=metrics,
        trades=trades,
        feature_rows=len(events),
        session_window=session_window,
        research_config=resolved_research_config,
        strategy_name=selected_strategy,
        report_prefix=_report_prefix(selected_strategy, resolved_research_config),
        research_mode_marker=(
            RESEARCH_ONLY_NOT_FOR_LIVE
            if selected_strategy is ResearchStrategyName.SIMPLE_BOOK_MOMENTUM_RESEARCH
            else None
        ),
    )
    return ResearchBacktestResult(
        metrics=metrics,
        trades=tuple(trades),
        artifacts=artifacts,
        feature_rows=len(events),
        session_window=session_window,
        strategy_name=selected_strategy,
        research_mode_marker=(
            RESEARCH_ONLY_NOT_FOR_LIVE
            if selected_strategy is ResearchStrategyName.SIMPLE_BOOK_MOMENTUM_RESEARCH
            else None
        ),
    )


def load_strategy_config(path: Path) -> OpeningRangeBookMomentumConfig:
    """Load strategy YAML without importing runtime risk config."""

    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
    if not isinstance(loaded, Mapping):
        loaded = {}
    raw = cast(Mapping[str, object], loaded)
    defaults = OpeningRangeBookMomentumConfig()
    return OpeningRangeBookMomentumConfig(
        session_start=(
            _parse_time(raw["session_start"])
            if "session_start" in raw
            else defaults.session_start
        ),
        session_end=(
            _parse_time(raw["session_end"])
            if "session_end" in raw
            else defaults.session_end
        ),
        opening_range_minutes=_int_value(
            raw,
            "opening_range_minutes",
            defaults.opening_range_minutes,
        ),
        opening_range_candle_count=_optional_int_value(
            raw,
            "opening_range_candle_count",
            defaults.opening_range_candle_count,
        ),
        entry_window_minutes=_int_value(
            raw,
            "entry_window_minutes",
            defaults.entry_window_minutes,
        ),
        force_exit_minutes_before_close=_int_value(
            raw,
            "force_exit_minutes_before_close",
            defaults.force_exit_minutes_before_close,
        ),
        breakout_buffer_bps=_decimal_value(
            raw,
            "breakout_buffer_bps",
            defaults.breakout_buffer_bps,
        ),
        max_spread_bps=_decimal_value(raw, "max_spread_bps", defaults.max_spread_bps),
        min_volatility_percentile=_optional_decimal_value(
            raw,
            "min_volatility_percentile",
            defaults.min_volatility_percentile,
        ),
        max_volatility_percentile=_optional_decimal_value(
            raw,
            "max_volatility_percentile",
            defaults.max_volatility_percentile,
        ),
        low_volatility_regimes=_string_tuple_value(
            raw,
            "low_volatility_regimes",
            defaults.low_volatility_regimes,
        ),
        high_volatility_regimes=_string_tuple_value(
            raw,
            "high_volatility_regimes",
            defaults.high_volatility_regimes,
        ),
        max_expected_slippage_bps=_optional_decimal_value(
            raw,
            "max_expected_slippage_bps",
            defaults.max_expected_slippage_bps,
        ),
        min_imbalance=_decimal_value(raw, "min_imbalance", defaults.min_imbalance),
        min_weighted_imbalance=_decimal_value(
            raw,
            "min_weighted_imbalance",
            defaults.min_weighted_imbalance,
        ),
        min_microprice_edge_bps=_decimal_value(
            raw,
            "min_microprice_edge_bps",
            defaults.min_microprice_edge_bps,
        ),
        max_opposing_wall_score=_decimal_value(
            raw,
            "max_opposing_wall_score",
            defaults.max_opposing_wall_score,
        ),
        require_ofi_confirmation=_bool(raw["require_ofi_confirmation"])
        if "require_ofi_confirmation" in raw
        else defaults.require_ofi_confirmation,
        min_ofi_confirmation=_decimal_value(
            raw,
            "min_ofi_confirmation",
            defaults.min_ofi_confirmation,
        ),
        min_confidence=_decimal_value(raw, "min_confidence", defaults.min_confidence),
        atr_window=_int_value(raw, "atr_window", defaults.atr_window),
        stop_atr_multiple=_decimal_value(
            raw,
            "stop_atr_multiple",
            defaults.stop_atr_multiple,
        ),
        take_profit_r_multiples=_decimal_tuple_value(
            raw,
            "take_profit_r_multiples",
            defaults.take_profit_r_multiples,
        ),
    )


def load_research_config(path: Path) -> ResearchSessionConfig:
    """Load offline research profile config."""

    loaded = yaml.safe_load(path.read_text(encoding="utf-8")) if path.exists() else {}
    if not isinstance(loaded, Mapping):
        loaded = {}
    root = cast(Mapping[str, object], loaded)
    raw_value = root.get("research", root)
    raw = cast(Mapping[str, object], raw_value) if isinstance(raw_value, Mapping) else {}
    profile = _session_profile(_string(raw.get("session_profile")) or "auto_from_data")
    return ResearchSessionConfig(
        session_profile=profile,
        opening_range_minutes=_int_value(raw, "opening_range_minutes", 15),
        min_rows_after_opening_range=_int_value(raw, "min_rows_after_opening_range", 100),
        allow_short_recording_backtest=_bool(
            raw.get("allow_short_recording_backtest", True)
        ),
        timezone=_optional_string(raw.get("timezone")),
        session_start=(
            _parse_time(raw["session_start"])
            if raw.get("session_start") is not None
            else None
        ),
        session_end=(
            _parse_time(raw["session_end"]) if raw.get("session_end") is not None else None
        ),
    )


def _read_feature_events(
    features_path: Path,
    *,
    active_tickers: set[str] | None,
    timezone: ZoneInfo,
) -> tuple[_FeatureEvent, ...]:
    events: list[_FeatureEvent] = []
    for path in sorted(features_path.rglob("*.parquet")):
        for row in _read_parquet_rows(path):
            event = _feature_event_from_row(row, path, timezone=timezone)
            if event is None:
                continue
            if active_tickers is not None and event.instrument not in active_tickers:
                continue
            events.append(event)
    return tuple(
        sorted(
            events,
            key=lambda item: (item.timestamp, item.instrument, item.instrument_uid),
        )
    )


def _feature_event_from_row(
    row: Mapping[str, object],
    path: Path,
    *,
    timezone: ZoneInfo,
) -> _FeatureEvent | None:
    timestamp = _timestamp(row.get("timestamp"))
    instrument = _string(row.get("instrument") or _path_partition_value(path, "instrument"))
    instrument_uid = _string(row.get("instrument_uid") or instrument)
    if timestamp is None or not instrument:
        return None
    return _FeatureEvent(
        timestamp=timestamp,
        strategy_time=_strategy_time(timestamp, timezone=timezone),
        instrument=instrument,
        instrument_uid=instrument_uid,
        row=row,
    )


def _read_parquet_rows(path: Path) -> tuple[Mapping[str, object], ...]:
    try:
        pq = cast(Any, importlib.import_module("pyarrow.parquet"))
        rows = pq.ParquetFile(path).read().to_pylist()
    except Exception:
        return ()
    return tuple(cast(Mapping[str, object], row) for row in rows if isinstance(row, Mapping))


def _observe_synthetic_candle(
    state: _InstrumentState,
    timestamp: datetime,
    mid: Decimal,
) -> None:
    minute = timestamp.replace(second=0, microsecond=0)
    if state.rolling_candles and state.rolling_candles[-1]["timestamp"] == minute:
        candle = state.rolling_candles[-1]
        high = _decimal(candle["high"], "high")
        low = _decimal(candle["low"], "low")
        candle["high"] = max(high, mid)
        candle["low"] = min(low, mid)
        candle["close"] = mid
        return
    state.rolling_candles.append(_synthetic_candle(minute, mid))


def _synthetic_candle(timestamp: datetime, mid: Decimal) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "open": mid,
        "high": mid,
        "low": mid,
        "close": mid,
    }


def _trim_rolling_candles(
    candles: Sequence[dict[str, object]],
    *,
    max_rolling_candles: int,
    session_window: ResearchSessionWindow,
    preserve_opening_range: bool,
) -> list[dict[str, object]]:
    if len(candles) <= max_rolling_candles:
        return list(candles)
    if not preserve_opening_range or session_window.opening_range_end is None:
        return list(candles[-max_rolling_candles:])

    opening_candles: list[dict[str, object]] = []
    tail_candidates: list[dict[str, object]] = []
    for candle in candles:
        timestamp = candle.get("timestamp")
        if isinstance(timestamp, datetime) and timestamp <= session_window.opening_range_end:
            opening_candles.append(candle)
        else:
            tail_candidates.append(candle)
    tail = tail_candidates[-max_rolling_candles:]
    return opening_candles + tail


def _strategy_features(row: Mapping[str, object]) -> Mapping[str, object]:
    buy_slippage = _optional_decimal(row.get("expected_slippage_bps_buy"))
    sell_slippage = _optional_decimal(row.get("expected_slippage_bps_sell"))
    expected_slippage = _max_decimal(buy_slippage, sell_slippage)
    result: dict[str, object] = {
        "mid": _decimal(row.get("mid"), "mid"),
        "spread_bps": _decimal(row.get("spread_bps"), "spread_bps"),
        "imbalance": _decimal(row.get("imbalance_5"), "imbalance_5"),
        "weighted_imbalance": _decimal(row.get("weighted_imbalance"), "weighted_imbalance"),
        "microprice": _decimal(row.get("microprice"), "microprice"),
        "bid_wall_score": _optional_decimal(row.get("bid_wall_score")) or Decimal("1"),
        "ask_wall_score": _optional_decimal(row.get("ask_wall_score")) or Decimal("1"),
        "volatility_regime": _string(row.get("volatility_regime")) or None,
        "vwap": _optional_decimal(row.get("vwap")),
        "ema_fast": _optional_decimal(row.get("ema_fast")),
        "ema_slow": _optional_decimal(row.get("ema_slow")),
        "expected_slippage_bps": expected_slippage,
    }
    percentile = _optional_decimal(row.get("volatility_percentile"))
    if percentile is not None:
        result["volatility_percentile"] = percentile
    return result


def _entry_trade(
    *,
    event: _FeatureEvent,
    action: SignalAction,
    quantity: Decimal,
    reason_codes: tuple[str, ...],
) -> _OpenTrade | None:
    if action is SignalAction.BUY:
        side = PositionSide.LONG
        fill = _fill_price(event.row, side="buy")
    elif action is SignalAction.SELL:
        side = PositionSide.SHORT
        fill = _fill_price(event.row, side="sell")
    else:
        return None
    if fill is None:
        return None
    price, slippage = fill
    return _OpenTrade(
        instrument=event.instrument,
        instrument_uid=event.instrument_uid,
        side=side,
        entry_time=event.timestamp,
        quantity=quantity,
        entry_price=price,
        entry_slippage_bps=slippage,
        entry_spread_regime=_spread_regime(_decimal(event.row.get("spread_bps"), "spread_bps")),
        entry_volatility_regime=_string(event.row.get("volatility_regime")) or "unknown",
        entry_reason_codes=reason_codes,
    )


def _close_trade(
    *,
    event: _FeatureEvent,
    open_trade: _OpenTrade,
    reason_codes: tuple[str, ...],
) -> tuple[ResearchTrade | None, Decimal]:
    if open_trade.side is PositionSide.LONG:
        fill = _fill_price(event.row, side="sell")
    else:
        fill = _fill_price(event.row, side="buy")
    if fill is None:
        return None, Decimal("0")
    exit_price, exit_slippage = fill
    if open_trade.side is PositionSide.LONG:
        pnl = (exit_price - open_trade.entry_price) * open_trade.quantity
    else:
        pnl = (open_trade.entry_price - exit_price) * open_trade.quantity
    return (
        ResearchTrade(
            instrument=open_trade.instrument,
            instrument_uid=open_trade.instrument_uid,
            side=open_trade.side,
            entry_time=open_trade.entry_time,
            exit_time=event.timestamp,
            quantity=open_trade.quantity,
            entry_price=open_trade.entry_price,
            exit_price=exit_price,
            pnl=pnl,
            mae=open_trade.mae,
            mfe=open_trade.mfe,
            entry_slippage_bps=open_trade.entry_slippage_bps,
            exit_slippage_bps=exit_slippage,
            entry_spread_regime=open_trade.entry_spread_regime,
            entry_volatility_regime=open_trade.entry_volatility_regime,
            entry_reason_codes=open_trade.entry_reason_codes,
            exit_reason_codes=reason_codes,
        ),
        exit_slippage,
    )


def _fill_price(
    row: Mapping[str, object],
    *,
    side: Literal["buy", "sell"],
) -> tuple[Decimal, Decimal] | None:
    mid = _optional_decimal(row.get("mid"))
    slippage = _optional_decimal(row.get(f"expected_slippage_bps_{side}"))
    if mid is None or slippage is None or mid <= 0:
        return None
    if side == "buy":
        price = mid * (Decimal("1") + (slippage / BPS_FACTOR))
    else:
        price = mid * (Decimal("1") - (slippage / BPS_FACTOR))
    if price <= 0:
        return None
    return price, slippage


def _observe_excursion(open_trade: _OpenTrade, mid: Decimal) -> _OpenTrade:
    if open_trade.side is PositionSide.LONG:
        unrealized = (mid - open_trade.entry_price) * open_trade.quantity
    else:
        unrealized = (open_trade.entry_price - mid) * open_trade.quantity
    open_trade.mae = min(open_trade.mae, unrealized)
    open_trade.mfe = max(open_trade.mfe, unrealized)
    return open_trade


def _strategy_position(open_trade: _OpenTrade | None) -> StrategyPosition:
    if open_trade is None:
        return StrategyPosition()
    return StrategyPosition(
        side=open_trade.side,
        quantity=open_trade.quantity,
        avg_entry_price=open_trade.entry_price,
    )


def _simple_research_signal(
    *,
    event: _FeatureEvent,
    open_trade: _OpenTrade | None,
    config: OpeningRangeBookMomentumConfig,
) -> tuple[SignalAction, tuple[str, ...]]:
    direction, reason_codes = _simple_research_direction(event.row, config)
    if open_trade is None:
        if direction == "BUY":
            return SignalAction.BUY, (RESEARCH_ONLY_NOT_FOR_LIVE, "SIMPLE_BOOK_MOMENTUM_BUY")
        if direction == "SELL":
            return SignalAction.SELL, (RESEARCH_ONLY_NOT_FOR_LIVE, "SIMPLE_BOOK_MOMENTUM_SELL")
        return SignalAction.HOLD, reason_codes

    if open_trade.side is PositionSide.LONG and direction == "SELL":
        return SignalAction.EXIT, ("SIMPLE_RESEARCH_OPPOSITE_SIGNAL",)
    if open_trade.side is PositionSide.SHORT and direction == "BUY":
        return SignalAction.EXIT, ("SIMPLE_RESEARCH_OPPOSITE_SIGNAL",)
    return SignalAction.HOLD, ("POSITION_HELD",)


def _simple_research_direction(
    row: Mapping[str, object],
    config: OpeningRangeBookMomentumConfig,
) -> tuple[Literal["BUY", "SELL", "HOLD"], tuple[str, ...]]:
    spread = _optional_decimal(row.get("spread_bps"))
    if spread is None or spread > config.max_spread_bps:
        return "HOLD", ("SPREAD_TOO_WIDE",)

    slippage = _max_decimal(
        _optional_decimal(row.get("expected_slippage_bps_buy")),
        _optional_decimal(row.get("expected_slippage_bps_sell")),
    )
    if (
        slippage is not None
        and config.max_expected_slippage_bps is not None
        and slippage > config.max_expected_slippage_bps
    ):
        return "HOLD", ("SLIPPAGE_TOO_HIGH",)

    rv_5m = _optional_decimal(row.get("rv_5m"))
    regime = _string(row.get("volatility_regime")).strip().lower()
    if rv_5m is None or regime in {"", "unknown"}:
        return "HOLD", ("VOLATILITY_UNUSABLE",)

    mid = _decimal(row.get("mid"), "mid")
    micro = _decimal(row.get("microprice"), "microprice")
    imbalance_5 = _decimal(row.get("imbalance_5"), "imbalance_5")
    imbalance_10 = _decimal(row.get("imbalance_10"), "imbalance_10")
    ema_fast = _optional_decimal(row.get("ema_fast"))
    ema_slow = _optional_decimal(row.get("ema_slow"))
    if ema_fast is None or ema_slow is None:
        return "HOLD", ("TREND_FILTER_REJECTED",)

    buy_edge = ((micro - mid) / mid) * BPS_FACTOR if mid > 0 else Decimal("0")
    sell_edge = ((mid - micro) / mid) * BPS_FACTOR if mid > 0 else Decimal("0")
    if (
        imbalance_5 >= config.min_imbalance
        and imbalance_10 >= config.min_imbalance
        and buy_edge >= config.min_microprice_edge_bps
        and ema_fast >= ema_slow
    ):
        return "BUY", ()
    if (
        imbalance_5 <= -config.min_imbalance
        and imbalance_10 <= -config.min_imbalance
        and sell_edge >= config.min_microprice_edge_bps
        and ema_fast <= ema_slow
    ):
        return "SELL", ()
    return "HOLD", ("SIMPLE_RESEARCH_NO_SIGNAL",)


def _build_metrics(
    *,
    trades: Sequence[ResearchTrade],
    slippages: Sequence[Decimal],
    reason_counts: Mapping[str, int],
    entry_signals: int,
    missed_fills: int,
    timezone: ZoneInfo,
) -> ResearchBacktestMetrics:
    total_pnl = sum((trade.pnl for trade in trades), Decimal("0"))
    winners = [trade for trade in trades if trade.pnl > 0]
    gross_profit = sum((trade.pnl for trade in trades if trade.pnl > 0), Decimal("0"))
    gross_loss = sum((trade.pnl for trade in trades if trade.pnl < 0), Decimal("0"))
    profit_factor = None if gross_loss == 0 else gross_profit / abs(gross_loss)
    avg_slippage = (
        sum(slippages, Decimal("0")) / Decimal(len(slippages)) if slippages else Decimal("0")
    )
    return ResearchBacktestMetrics(
        total_pnl=total_pnl,
        trades=len(trades),
        winrate=Decimal(len(winners)) / Decimal(len(trades)) if trades else Decimal("0"),
        profit_factor=profit_factor,
        max_drawdown=_max_drawdown(trades),
        avg_slippage_bps=avg_slippage,
        missed_fill_rate=(
            Decimal(missed_fills) / Decimal(entry_signals) if entry_signals else Decimal("0")
        ),
        pnl_by_instrument=_pnl_by(trades, lambda trade: trade.instrument),
        pnl_by_hour=_pnl_by(
            trades,
            lambda trade: f"{_strategy_time(trade.entry_time, timezone=timezone).hour:02d}",
        ),
        pnl_by_spread_regime=_pnl_by(trades, lambda trade: trade.entry_spread_regime),
        pnl_by_volatility_regime=_pnl_by(trades, lambda trade: trade.entry_volatility_regime),
        reason_code_distribution=dict(sorted(reason_counts.items())),
        reject_reason_distribution=dict(sorted(reason_counts.items())),
    )


def _pnl_by(
    trades: Sequence[ResearchTrade],
    key_func: Callable[[ResearchTrade], str],
) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}
    for trade in trades:
        key = str(key_func(trade))
        result[key] = result.get(key, Decimal("0")) + trade.pnl
    return result


def _max_drawdown(trades: Sequence[ResearchTrade]) -> Decimal:
    equity = Decimal("0")
    peak = Decimal("0")
    max_drawdown = Decimal("0")
    for trade in sorted(trades, key=lambda item: item.exit_time):
        equity += trade.pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def _write_reports(
    *,
    reports_dir: Path,
    features_path: Path,
    metrics: ResearchBacktestMetrics,
    trades: Sequence[ResearchTrade],
    feature_rows: int,
    session_window: ResearchSessionWindow,
    research_config: ResearchSessionConfig,
    strategy_name: ResearchStrategyName,
    report_prefix: str,
    research_mode_marker: str | None,
) -> ResearchBacktestArtifacts:
    reports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    json_path = reports_dir / f"{report_prefix}_{timestamp}.json"
    trades_csv_path = reports_dir / f"{report_prefix}_{timestamp}.csv"
    summary_html_path = reports_dir / f"{report_prefix}_{timestamp}.html"
    diagnostics_json_path = reports_dir / f"research_diagnostics_{timestamp}.json"
    report_payload = _json_payload(
        features_path=features_path,
        metrics=metrics,
        trades=trades,
        feature_rows=feature_rows,
        session_window=session_window,
        research_config=research_config,
        strategy_name=strategy_name,
        research_mode_marker=research_mode_marker,
    )
    _atomic_write_json(
        json_path,
        report_payload,
    )
    _atomic_write_json(
        diagnostics_json_path,
        {
            "generated_at": datetime.now(UTC).isoformat(),
            "commit_hash": get_runtime_commit_hash(),
            "strategy_name": strategy_name.value,
            "research_mode_marker": research_mode_marker,
            "session": _session_payload(session_window),
            "reject_reason_distribution": dict(metrics.reject_reason_distribution),
        },
    )
    _atomic_write_trades_csv(trades_csv_path, trades)
    _atomic_write_html(
        summary_html_path,
        _html_document(
            metrics=metrics,
            trades=trades,
            session_window=session_window,
            strategy_name=strategy_name,
            research_mode_marker=research_mode_marker,
        ),
    )
    return ResearchBacktestArtifacts(
        json_path=json_path,
        trades_csv_path=trades_csv_path,
        summary_html_path=summary_html_path,
        diagnostics_json_path=diagnostics_json_path,
    )


def _json_payload(
    *,
    features_path: Path,
    metrics: ResearchBacktestMetrics,
    trades: Sequence[ResearchTrade],
    feature_rows: int,
    session_window: ResearchSessionWindow,
    research_config: ResearchSessionConfig,
    strategy_name: ResearchStrategyName,
    research_mode_marker: str | None,
) -> dict[str, object]:
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "commit_hash": get_runtime_commit_hash(),
        "features_path": str(features_path),
        "feature_rows": feature_rows,
        "strategy_name": strategy_name.value,
        "research_mode_marker": research_mode_marker,
        "research_config": {
            "session_profile": research_config.session_profile.value,
            "opening_range_minutes": research_config.opening_range_minutes,
            "min_rows_after_opening_range": research_config.min_rows_after_opening_range,
            "allow_short_recording_backtest": research_config.allow_short_recording_backtest,
            "timezone": _research_timezone(research_config),
            "session_start": (
                None
                if research_config.session_start is None
                else research_config.session_start.isoformat()
            ),
            "session_end": (
                None
                if research_config.session_end is None
                else research_config.session_end.isoformat()
            ),
        },
        "session": _session_payload(session_window),
        "metrics": {
            "total_pnl": str(metrics.total_pnl),
            "trades": metrics.trades,
            "winrate": str(metrics.winrate),
            "profit_factor": None if metrics.profit_factor is None else str(metrics.profit_factor),
            "max_drawdown": str(metrics.max_drawdown),
            "avg_slippage_bps": str(metrics.avg_slippage_bps),
            "missed_fill_rate": str(metrics.missed_fill_rate),
            "pnl_by_instrument": _decimal_mapping(metrics.pnl_by_instrument),
            "pnl_by_hour": _decimal_mapping(metrics.pnl_by_hour),
            "pnl_by_spread_regime": _decimal_mapping(metrics.pnl_by_spread_regime),
            "pnl_by_volatility_regime": _decimal_mapping(metrics.pnl_by_volatility_regime),
            "reason_code_distribution": dict(metrics.reason_code_distribution),
            "reject_reason_distribution": dict(metrics.reject_reason_distribution),
        },
        "trades": [_trade_payload(trade) for trade in trades],
    }


def _session_payload(session_window: ResearchSessionWindow) -> dict[str, object]:
    return {
        "session_profile": session_window.profile.value,
        "timezone": session_window.timezone,
        "first_timestamp": _iso_or_none(session_window.first_timestamp),
        "last_timestamp": _iso_or_none(session_window.last_timestamp),
        "data_duration_minutes": str(session_window.data_duration_minutes),
        "opening_range_start": _iso_or_none(session_window.opening_range_start),
        "opening_range_end": _iso_or_none(session_window.opening_range_end),
        "rows_before_or_ready": session_window.rows_before_or_ready,
        "rows_after_or_ready": session_window.rows_after_or_ready,
        "status": session_window.status,
    }


def _trade_payload(trade: ResearchTrade) -> dict[str, object]:
    return {
        "instrument": trade.instrument,
        "instrument_uid": trade.instrument_uid,
        "side": trade.side.value,
        "entry_time": trade.entry_time.isoformat(),
        "exit_time": trade.exit_time.isoformat(),
        "quantity": str(trade.quantity),
        "entry_price": str(trade.entry_price),
        "exit_price": str(trade.exit_price),
        "pnl": str(trade.pnl),
        "mae": str(trade.mae),
        "mfe": str(trade.mfe),
        "entry_slippage_bps": str(trade.entry_slippage_bps),
        "exit_slippage_bps": str(trade.exit_slippage_bps),
        "entry_spread_regime": trade.entry_spread_regime,
        "entry_volatility_regime": trade.entry_volatility_regime,
        "entry_reason_codes": list(trade.entry_reason_codes),
        "exit_reason_codes": list(trade.exit_reason_codes),
    }


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    _atomic_write_text(
        path,
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
    )


def _atomic_write_trades_csv(path: Path, trades: Sequence[ResearchTrade]) -> None:
    fieldnames = (
        "instrument",
        "instrument_uid",
        "side",
        "entry_time",
        "exit_time",
        "quantity",
        "entry_price",
        "exit_price",
        "pnl",
        "mae",
        "mfe",
        "entry_slippage_bps",
        "exit_slippage_bps",
        "entry_spread_regime",
        "entry_volatility_regime",
        "entry_reason_codes",
        "exit_reason_codes",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with tmp_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            for trade in trades:
                row = _trade_payload(trade)
                row["entry_reason_codes"] = "|".join(trade.entry_reason_codes)
                row["exit_reason_codes"] = "|".join(trade.exit_reason_codes)
                writer.writerow(row)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _atomic_write_html(path: Path, document: str) -> None:
    _atomic_write_text(path, document)


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        tmp_path.write_text(text, encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _html_document(
    *,
    metrics: ResearchBacktestMetrics,
    trades: Sequence[ResearchTrade],
    session_window: ResearchSessionWindow,
    strategy_name: ResearchStrategyName,
    research_mode_marker: str | None,
) -> str:
    metric_rows = (
        ("commit_hash", get_runtime_commit_hash()),
        ("strategy_name", strategy_name.value),
        ("research_mode_marker", research_mode_marker or ""),
        ("session_profile", session_window.profile.value),
        ("session_timezone", session_window.timezone),
        ("first_timestamp", _iso_or_none(session_window.first_timestamp) or ""),
        ("last_timestamp", _iso_or_none(session_window.last_timestamp) or ""),
        ("data_duration_minutes", str(session_window.data_duration_minutes)),
        ("opening_range_start", _iso_or_none(session_window.opening_range_start) or ""),
        ("opening_range_end", _iso_or_none(session_window.opening_range_end) or ""),
        ("rows_before_or_ready", str(session_window.rows_before_or_ready)),
        ("rows_after_or_ready", str(session_window.rows_after_or_ready)),
        ("session_status", session_window.status),
        ("total_pnl", str(metrics.total_pnl)),
        ("trades", str(metrics.trades)),
        ("winrate", str(metrics.winrate)),
        ("profit_factor", "" if metrics.profit_factor is None else str(metrics.profit_factor)),
        ("max_drawdown", str(metrics.max_drawdown)),
        ("avg_slippage_bps", str(metrics.avg_slippage_bps)),
        ("missed_fill_rate", str(metrics.missed_fill_rate)),
    )
    trade_rows = tuple(
        (
            trade.instrument,
            trade.side.value,
            trade.entry_time.isoformat(),
            trade.exit_time.isoformat(),
            str(trade.pnl),
            trade.entry_volatility_regime,
        )
        for trade in trades
    )
    return (
        "<!doctype html><html><head><meta charset=\"utf-8\">"
        "<title>NeoIntraday research backtest</title>"
        "<style>body{font-family:Arial,sans-serif;margin:24px;color:#17202a;}"
        "table{border-collapse:collapse;margin:12px 0 24px;width:100%;}"
        "th,td{border:1px solid #d5d8dc;padding:6px 8px;text-align:left;}"
        "th{background:#f4f6f7;}</style></head><body>"
        "<h1>NeoIntraday research backtest</h1>"
        f"<p>{html.escape(research_mode_marker or '')}</p>"
        "<h2>Metrics</h2>"
        f"{_html_table(('Metric', 'Value'), metric_rows)}"
        "<h2>Trades</h2>"
        f"{_html_table(('Instrument', 'Side', 'Entry', 'Exit', 'PnL', 'Vol Regime'), trade_rows)}"
        "</body></html>\n"
    )


def _html_table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    header_html = "".join(f"<th>{html.escape(item)}</th>" for item in headers)
    rows_html = "".join(
        "<tr>" + "".join(f"<td>{html.escape(value)}</td>" for value in row) + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{header_html}</tr></thead><tbody>{rows_html}</tbody></table>"


def _load_active_tickers(path: Path) -> set[str] | None:
    if not path.exists():
        return None
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, Mapping):
        return None
    instruments = loaded.get("instruments")
    if not isinstance(instruments, Sequence) or isinstance(instruments, str | bytes):
        return None
    tickers = {
        _string(item.get("ticker")).strip()
        for item in instruments
        if isinstance(item, Mapping)
        and item.get("enabled", True) is not False
        and _string(item.get("ticker")).strip()
    }
    return tickers or None


def _strategy_name(value: ResearchStrategyName | str) -> ResearchStrategyName:
    if isinstance(value, ResearchStrategyName):
        return value
    normalized = str(value).strip().lower()
    for item in ResearchStrategyName:
        if normalized == item.value:
            return item
    raise ValueError(f"unsupported research strategy: {value!r}.")


def _session_profile(value: str) -> ResearchSessionProfile:
    normalized = value.strip().lower()
    for item in ResearchSessionProfile:
        if normalized == item.value:
            return item
    raise ValueError(f"unsupported research session profile: {value!r}.")


def _research_timezone(config: ResearchSessionConfig) -> str:
    if config.timezone:
        return config.timezone
    if config.session_profile in {
        ResearchSessionProfile.AUTO_FROM_DATA,
        ResearchSessionProfile.MOEX_EQUITY,
    }:
        return "Europe/Moscow"
    if config.session_profile is ResearchSessionProfile.US_NEO_STOCK:
        return "America/New_York"
    if config.session_profile is ResearchSessionProfile.CRYPTO_NEO:
        return "UTC"
    raise ValueError("custom research session profile requires timezone.")


def _resolve_session_window(
    events: Sequence[_FeatureEvent],
    config: ResearchSessionConfig,
) -> ResearchSessionWindow:
    usable = [event for event in events if _bool(event.row.get("usable_row"))]
    timezone = _research_timezone(config)
    if not usable:
        return ResearchSessionWindow(
            profile=config.session_profile,
            timezone=timezone,
            first_timestamp=None,
            last_timestamp=None,
            data_duration_minutes=Decimal("0"),
            opening_range_start=None,
            opening_range_end=None,
            rows_before_or_ready=0,
            rows_after_or_ready=0,
            status=DATA_WINDOW_TOO_SHORT,
        )

    first_time = usable[0].strategy_time
    last_time = usable[-1].strategy_time
    opening_start, opening_end = _opening_range_window(first_time, config)
    rows_before_or_ready = sum(1 for event in usable if event.strategy_time <= opening_end)
    rows_after_or_ready = sum(1 for event in usable if event.strategy_time > opening_end)
    status = (
        "OK"
        if rows_before_or_ready > 0
        and rows_after_or_ready >= config.min_rows_after_opening_range
        and last_time >= opening_end
        else DATA_WINDOW_TOO_SHORT
    )
    duration = Decimal(str(max((last_time - first_time).total_seconds(), 0.0))) / Decimal("60")
    return ResearchSessionWindow(
        profile=config.session_profile,
        timezone=timezone,
        first_timestamp=usable[0].timestamp,
        last_timestamp=usable[-1].timestamp,
        data_duration_minutes=duration,
        opening_range_start=opening_start,
        opening_range_end=opening_end,
        rows_before_or_ready=rows_before_or_ready,
        rows_after_or_ready=rows_after_or_ready,
        status=status,
    )


def _opening_range_window(
    first_time: datetime,
    config: ResearchSessionConfig,
) -> tuple[datetime, datetime]:
    if config.session_profile is ResearchSessionProfile.AUTO_FROM_DATA:
        start = first_time
    else:
        if config.session_start is None or config.session_end is None:
            raise ValueError(
                "non-auto research session profiles require session_start and session_end."
            )
        start = datetime.combine(first_time.date(), config.session_start)
    return start, start + timedelta(minutes=config.opening_range_minutes)


def _research_strategy_config(
    base: OpeningRangeBookMomentumConfig,
    *,
    research_config: ResearchSessionConfig,
    session_window: ResearchSessionWindow,
) -> OpeningRangeBookMomentumConfig:
    if session_window.opening_range_start is None or session_window.opening_range_end is None:
        return base
    session_start = session_window.opening_range_start.time()
    if research_config.session_profile is ResearchSessionProfile.AUTO_FROM_DATA:
        session_end = (
            session_window.last_timestamp.astimezone(ZoneInfo(session_window.timezone)).time()
            if session_window.last_timestamp is not None
            else base.session_end
        )
        entry_window = max(
            1,
            int(
                max(
                    (
                        session_window.last_timestamp.astimezone(
                            ZoneInfo(session_window.timezone)
                        ).replace(tzinfo=None)
                        - session_window.opening_range_end
                    ).total_seconds()
                    / 60,
                    0,
                )
            )
            + 1,
        ) if session_window.last_timestamp is not None else base.entry_window_minutes
    else:
        if research_config.session_end is None:
            raise ValueError("non-auto research session profiles require session_end.")
        session_end = research_config.session_end
        entry_window = base.entry_window_minutes

    return replace(
        base,
        session_start=session_start,
        session_end=session_end,
        opening_range_minutes=research_config.opening_range_minutes,
        opening_range_candle_count=None,
        entry_window_minutes=entry_window,
    )


def _report_prefix(
    strategy_name: ResearchStrategyName,
    config: ResearchSessionConfig,
) -> str:
    if strategy_name is ResearchStrategyName.SIMPLE_BOOK_MOMENTUM_RESEARCH:
        return "backtest_simple_book_momentum"
    if (
        strategy_name is ResearchStrategyName.OPENING_RANGE_BOOK_MOMENTUM
        and config.session_profile is ResearchSessionProfile.AUTO_FROM_DATA
    ):
        return "backtest_or_auto"
    return "backtest_report"


def _decimal_mapping(values: Mapping[str, Decimal]) -> dict[str, str]:
    return {key: str(value) for key, value in sorted(values.items())}


def _spread_regime(spread_bps: Decimal) -> str:
    if spread_bps <= Decimal("2"):
        return "tight"
    if spread_bps <= Decimal("10"):
        return "normal"
    return "wide"


def _strategy_time(timestamp: datetime, *, timezone: ZoneInfo) -> datetime:
    return _as_utc(timestamp).astimezone(timezone).replace(tzinfo=None)


def _timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str) and value.strip():
        try:
            return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            return None
    return None


def _path_partition_value(path: Path, key: str) -> str:
    prefix = f"{key}="
    for part in path.parts:
        if part.startswith(prefix):
            return part.removeprefix(prefix)
    return ""


def _parse_time(value: object) -> time:
    if isinstance(value, time):
        return value
    return time.fromisoformat(str(value))


def _int_value(raw: Mapping[str, object], key: str, default: int) -> int:
    value = raw.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        raise TypeError(f"{key} must be an integer, not bool.")
    return int(str(value))


def _optional_int_value(raw: Mapping[str, object], key: str, default: int | None) -> int | None:
    value = raw.get(key)
    if value is None:
        return default
    if isinstance(value, bool):
        raise TypeError(f"{key} must be an integer, not bool.")
    return int(str(value))


def _decimal_value(raw: Mapping[str, object], key: str, default: Decimal) -> Decimal:
    value = raw.get(key)
    return default if value is None else _decimal(value, key)


def _optional_decimal_value(
    raw: Mapping[str, object],
    key: str,
    default: Decimal | None,
) -> Decimal | None:
    if key not in raw:
        return default
    value = raw.get(key)
    return None if value is None else _decimal(value, key)


def _string_tuple_value(
    raw: Mapping[str, object],
    key: str,
    default: tuple[str, ...],
) -> tuple[str, ...]:
    return _string_tuple(raw[key], key) if key in raw else default


def _decimal_tuple_value(
    raw: Mapping[str, object],
    key: str,
    default: tuple[Decimal, ...],
) -> tuple[Decimal, ...]:
    if key not in raw:
        return default
    return tuple(_decimal(item, key) for item in _sequence(raw[key]))


def _sequence(value: object) -> Sequence[object]:
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return cast(Sequence[object], value)
    raise TypeError(f"expected sequence, got {value!r}.")


def _string_tuple(value: object, field_name: str) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value) if str(item).strip()) or (
        field_name,
    )


def _max_decimal(first: Decimal | None, second: Decimal | None) -> Decimal | None:
    values = [value for value in (first, second) if value is not None]
    return max(values) if values else None


def _optional_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, Decimal):
            return value if value.is_finite() else None
        if isinstance(value, int | str):
            parsed = Decimal(value)
            return parsed if parsed.is_finite() else None
        if isinstance(value, float):
            parsed = Decimal(str(value))
            return parsed if parsed.is_finite() else None
    except (InvalidOperation, ValueError):
        return None
    return None


def _decimal(value: object, field_name: str) -> Decimal:
    parsed = _optional_decimal(value)
    if parsed is None:
        raise ValueError(f"{field_name} must be a finite decimal.")
    return parsed


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _optional_string(value: object) -> str | None:
    text = _string(value).strip()
    return text or None


def _iso_or_none(value: datetime | None) -> str | None:
    return None if value is None else value.isoformat()


def _reason_code_value(reason: ReasonCode | object) -> str:
    return reason.value if isinstance(reason, ReasonCode) else str(reason)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _string(value: object) -> str:
    return "" if value is None else str(value)


__all__ = [
    "DATA_WINDOW_TOO_SHORT",
    "RESEARCH_ONLY_NOT_FOR_LIVE",
    "ResearchBacktestArtifacts",
    "ResearchBacktestConfig",
    "ResearchBacktestMetrics",
    "ResearchBacktestResult",
    "ResearchSessionConfig",
    "ResearchSessionProfile",
    "ResearchSessionWindow",
    "ResearchStrategyName",
    "ResearchTrade",
    "load_research_config",
    "load_strategy_config",
    "run_research_backtest",
]
