"""Offline research backtest runner over feature-store parquet files."""

from __future__ import annotations

import csv
import html
import importlib
import json
import os
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, time
from decimal import Decimal, InvalidOperation
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


@dataclass(frozen=True)
class ResearchBacktestArtifacts:
    """Report files written by the research runner."""

    json_path: Path
    trades_csv_path: Path
    summary_html_path: Path


@dataclass(frozen=True)
class ResearchBacktestResult:
    """Research backtest output."""

    metrics: ResearchBacktestMetrics
    trades: tuple[ResearchTrade, ...]
    artifacts: ResearchBacktestArtifacts
    feature_rows: int
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
    rolling_candles: list[Mapping[str, object]] = field(default_factory=list)
    open_trade: _OpenTrade | None = None


def run_research_backtest(
    *,
    features_path: Path | str = Path("data/features"),
    reports_dir: Path | str = Path("data/reports"),
    active_universe_path: Path | str = Path("configs/active_universe.yaml"),
    strategy_config_path: Path | str = Path("configs/strategy.yaml"),
    config: ResearchBacktestConfig | None = None,
    strategy_config: OpeningRangeBookMomentumConfig | None = None,
) -> ResearchBacktestResult:
    """Run the strategy over feature-store rows and export JSON/CSV/HTML reports."""

    resolved_config = config or ResearchBacktestConfig()
    resolved_strategy_config = strategy_config or load_strategy_config(Path(strategy_config_path))
    strategy = OpeningRangeBookMomentumStrategy(resolved_strategy_config)
    active_tickers = _load_active_tickers(Path(active_universe_path))
    events = _read_feature_events(Path(features_path), active_tickers=active_tickers)

    states: dict[str, _InstrumentState] = {}
    trades: list[ResearchTrade] = []
    slippages: list[Decimal] = []
    reason_counts: Counter[str] = Counter()
    entry_signals = 0
    missed_fills = 0

    for event in events:
        state = states.setdefault(event.instrument_uid, _InstrumentState())
        if not _bool(event.row.get("usable_row")):
            reason_counts["UNUSABLE_ROW"] += 1
            continue

        mid = _decimal(event.row.get("mid"), "mid")
        candle = _synthetic_candle(event.strategy_time, mid)
        state.rolling_candles.append(candle)
        if len(state.rolling_candles) > resolved_config.max_rolling_candles:
            state.rolling_candles = state.rolling_candles[-resolved_config.max_rolling_candles :]

        if state.open_trade is not None:
            _observe_excursion(state.open_trade, mid)

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

        reason_codes = tuple(_reason_code_value(reason) for reason in signal.reason_codes)
        reason_counts.update(reason_codes)

        if signal.action in {SignalAction.BUY, SignalAction.SELL}:
            entry_signals += 1
            if state.open_trade is not None:
                continue
            open_trade = _entry_trade(
                event=event,
                action=signal.action,
                quantity=resolved_config.fixed_quantity,
                reason_codes=reason_codes,
            )
            if open_trade is None:
                missed_fills += 1
                continue
            slippages.append(open_trade.entry_slippage_bps)
            state.open_trade = _observe_excursion(open_trade, mid)
            continue

        if signal.action is SignalAction.EXIT and state.open_trade is not None:
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
    )
    artifacts = _write_reports(
        reports_dir=Path(reports_dir),
        features_path=Path(features_path),
        metrics=metrics,
        trades=trades,
        feature_rows=len(events),
    )
    return ResearchBacktestResult(
        metrics=metrics,
        trades=tuple(trades),
        artifacts=artifacts,
        feature_rows=len(events),
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


def _read_feature_events(
    features_path: Path,
    *,
    active_tickers: set[str] | None,
) -> tuple[_FeatureEvent, ...]:
    events: list[_FeatureEvent] = []
    for path in sorted(features_path.rglob("*.parquet")):
        for row in _read_parquet_rows(path):
            event = _feature_event_from_row(row, path)
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


def _feature_event_from_row(row: Mapping[str, object], path: Path) -> _FeatureEvent | None:
    timestamp = _timestamp(row.get("timestamp"))
    instrument = _string(row.get("instrument") or _path_partition_value(path, "instrument"))
    instrument_uid = _string(row.get("instrument_uid") or instrument)
    if timestamp is None or not instrument:
        return None
    return _FeatureEvent(
        timestamp=timestamp,
        strategy_time=_strategy_time(timestamp),
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


def _synthetic_candle(timestamp: datetime, mid: Decimal) -> Mapping[str, object]:
    return {
        "timestamp": timestamp,
        "open": mid,
        "high": mid,
        "low": mid,
        "close": mid,
    }


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


def _build_metrics(
    *,
    trades: Sequence[ResearchTrade],
    slippages: Sequence[Decimal],
    reason_counts: Mapping[str, int],
    entry_signals: int,
    missed_fills: int,
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
        pnl_by_hour=_pnl_by(trades, lambda trade: f"{_strategy_time(trade.entry_time).hour:02d}"),
        pnl_by_spread_regime=_pnl_by(trades, lambda trade: trade.entry_spread_regime),
        pnl_by_volatility_regime=_pnl_by(trades, lambda trade: trade.entry_volatility_regime),
        reason_code_distribution=dict(sorted(reason_counts.items())),
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
) -> ResearchBacktestArtifacts:
    reports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    json_path = reports_dir / f"backtest_report_{timestamp}.json"
    trades_csv_path = reports_dir / f"backtest_trades_{timestamp}.csv"
    summary_html_path = reports_dir / f"backtest_summary_{timestamp}.html"
    _atomic_write_json(
        json_path,
        _json_payload(
            features_path=features_path,
            metrics=metrics,
            trades=trades,
            feature_rows=feature_rows,
        ),
    )
    _atomic_write_trades_csv(trades_csv_path, trades)
    _atomic_write_html(summary_html_path, _html_document(metrics=metrics, trades=trades))
    return ResearchBacktestArtifacts(
        json_path=json_path,
        trades_csv_path=trades_csv_path,
        summary_html_path=summary_html_path,
    )


def _json_payload(
    *,
    features_path: Path,
    metrics: ResearchBacktestMetrics,
    trades: Sequence[ResearchTrade],
    feature_rows: int,
) -> dict[str, object]:
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "commit_hash": get_runtime_commit_hash(),
        "features_path": str(features_path),
        "feature_rows": feature_rows,
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
        },
        "trades": [_trade_payload(trade) for trade in trades],
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
) -> str:
    metric_rows = (
        ("commit_hash", get_runtime_commit_hash()),
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


def _decimal_mapping(values: Mapping[str, Decimal]) -> dict[str, str]:
    return {key: str(value) for key, value in sorted(values.items())}


def _spread_regime(spread_bps: Decimal) -> str:
    if spread_bps <= Decimal("2"):
        return "tight"
    if spread_bps <= Decimal("10"):
        return "normal"
    return "wide"


def _strategy_time(timestamp: datetime) -> datetime:
    return _as_utc(timestamp).astimezone(MOSCOW_TZ).replace(tzinfo=None)


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


def _reason_code_value(reason: ReasonCode | object) -> str:
    return reason.value if isinstance(reason, ReasonCode) else str(reason)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _string(value: object) -> str:
    return "" if value is None else str(value)


__all__ = [
    "ResearchBacktestArtifacts",
    "ResearchBacktestConfig",
    "ResearchBacktestMetrics",
    "ResearchBacktestResult",
    "ResearchTrade",
    "load_strategy_config",
    "run_research_backtest",
]
