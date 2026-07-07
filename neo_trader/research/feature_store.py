"""Offline feature-store builder for recorded readonly market data."""

from __future__ import annotations

import hashlib
import importlib
import json
import math
import os
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final, Literal, TypeAlias, cast
from uuid import uuid4

import yaml

from neo_trader.features.orderbook import (
    best_bid_ask,
    book_wall_score,
    depth_sum,
    expected_slippage_bps,
    imbalance,
    microprice,
    mid_price,
    spread_bps,
    weighted_imbalance,
)
from neo_trader.features.volatility import (
    volatility_percentile,
    volatility_regime,
)
from neo_trader.runtime import get_runtime_commit_hash

JsonMapping: TypeAlias = Mapping[str, Any]
BPS_FACTOR: Final = Decimal("10000")

FEATURE_COLUMNS: Final = (
    "timestamp",
    "instrument",
    "mid",
    "best_bid",
    "best_ask",
    "spread_bps",
    "depth_top5_bid",
    "depth_top5_ask",
    "depth_top10_bid",
    "depth_top10_ask",
    "imbalance_5",
    "imbalance_10",
    "weighted_imbalance",
    "microprice",
    "expected_slippage_bps_buy",
    "expected_slippage_bps_sell",
    "rv_1m",
    "rv_5m",
    "vwap",
    "ema_fast",
    "ema_slow",
    "volatility_regime",
    "usable_row",
)

_NUMERIC_FEATURE_COLUMNS: Final = (
    "mid",
    "best_bid",
    "best_ask",
    "spread_bps",
    "depth_top5_bid",
    "depth_top5_ask",
    "depth_top10_bid",
    "depth_top10_ask",
    "imbalance_5",
    "imbalance_10",
    "weighted_imbalance",
    "microprice",
    "expected_slippage_bps_buy",
    "expected_slippage_bps_sell",
    "rv_1m",
    "rv_5m",
    "vwap",
    "ema_fast",
    "ema_slow",
)

_EVENT_PRIORITY: Final = {"candles": 0, "trades": 1, "orderbook": 2}


@dataclass(frozen=True)
class FeatureStoreConfig:
    """Parameters for deterministic offline feature generation."""

    expected_fill_quantity: Decimal = Decimal("10")
    weighted_imbalance_lambda: Decimal = Decimal("0.7")
    wall_distance_bps: Decimal = Decimal("10")
    ema_fast_span: int = 12
    ema_slow_span: int = 26
    volatility_history_min_rows: int = 10

    def __post_init__(self) -> None:
        if self.expected_fill_quantity <= 0:
            raise ValueError("expected_fill_quantity must be positive.")
        if self.weighted_imbalance_lambda < 0:
            raise ValueError("weighted_imbalance_lambda must be non-negative.")
        if self.wall_distance_bps < 0:
            raise ValueError("wall_distance_bps must be non-negative.")
        if self.ema_fast_span <= 0:
            raise ValueError("ema_fast_span must be positive.")
        if self.ema_slow_span <= 0:
            raise ValueError("ema_slow_span must be positive.")
        if self.volatility_history_min_rows < 0:
            raise ValueError("volatility_history_min_rows must be non-negative.")


@dataclass(frozen=True)
class FeatureStoreResult:
    """Feature-store build summary."""

    output_root: Path
    rows_written: int
    files_written: tuple[Path, ...]
    feature_columns: tuple[str, ...] = FEATURE_COLUMNS
    commit_hash: str = field(default_factory=get_runtime_commit_hash)


@dataclass(frozen=True)
class _RawEvent:
    timestamp: datetime
    instrument_uid: str
    event_type: Literal["orderbook", "trades", "candles"]
    payload: JsonMapping
    stable_key: str


@dataclass
class _InstrumentState:
    trade_notional: Decimal = Decimal("0")
    trade_quantity: Decimal = Decimal("0")
    candle_notional: Decimal = Decimal("0")
    candle_volume: Decimal = Decimal("0")
    last_mid: Decimal | None = None
    log_returns: deque[tuple[datetime, float]] = field(default_factory=deque)
    rv5_history: list[Decimal] = field(default_factory=list)
    ema_fast: Decimal | None = None
    ema_slow: Decimal | None = None

    def observe_trade(self, payload: Mapping[str, object]) -> None:
        trade = _payload_view(payload, "trade", "trades")
        price = _decimal_or_none(trade.get("price"))
        quantity = _decimal_or_none(
            _first_present(trade, "quantity", "qty", "volume", default=None)
        )
        if price is None or quantity is None or quantity <= 0:
            return
        self.trade_notional += price * quantity
        self.trade_quantity += quantity

    def observe_candle(self, payload: Mapping[str, object]) -> None:
        candle = _payload_view(payload, "candle", "candles")
        close = _decimal_or_none(candle.get("close"))
        volume = _decimal_or_none(candle.get("volume"))
        if close is None or volume is None or volume <= 0:
            return
        self.candle_notional += close * volume
        self.candle_volume += volume

    def observe_mid(
        self,
        *,
        timestamp: datetime,
        mid: Decimal,
        config: FeatureStoreConfig,
    ) -> tuple[Decimal, Decimal, Decimal, str, Decimal, Decimal, Decimal]:
        cutoff = timestamp - timedelta(minutes=5)
        if self.last_mid is not None and self.last_mid > 0 and mid > 0:
            value = math.log(float(mid / self.last_mid))
            if math.isfinite(value):
                self.log_returns.append((timestamp, value))
        self.last_mid = mid
        while self.log_returns and self.log_returns[0][0] < cutoff:
            self.log_returns.popleft()

        rv_1m = _realized_volatility_bps(self.log_returns, timestamp - timedelta(minutes=1))
        rv_5m = _realized_volatility_bps(self.log_returns, cutoff)
        percentile = self._volatility_percentile(rv_5m, config)
        regime = volatility_regime(percentile)
        self.rv5_history.append(rv_5m)

        self.ema_fast = _ema(self.ema_fast, mid, config.ema_fast_span)
        self.ema_slow = _ema(self.ema_slow, mid, config.ema_slow_span)
        vwap = self.vwap(fallback=mid)
        return (
            rv_1m,
            rv_5m,
            percentile,
            regime,
            vwap,
            self.ema_fast,
            self.ema_slow,
        )

    def vwap(self, *, fallback: Decimal) -> Decimal:
        if self.trade_quantity > 0:
            return self.trade_notional / self.trade_quantity
        if self.candle_volume > 0:
            return self.candle_notional / self.candle_volume
        return fallback

    def _volatility_percentile(
        self,
        value: Decimal,
        config: FeatureStoreConfig,
    ) -> Decimal:
        if len(self.rv5_history) < config.volatility_history_min_rows:
            return Decimal("50")
        return volatility_percentile(value, self.rv5_history)


def build_feature_store(
    *,
    raw_path: Path | str = Path("data/raw"),
    output_path: Path | str = Path("data/features"),
    active_universe_path: Path | str = Path("configs/active_universe.yaml"),
    config: FeatureStoreConfig | None = None,
) -> FeatureStoreResult:
    """Build partitioned parquet features from raw readonly parquet events."""

    resolved_config = config or FeatureStoreConfig()
    output_root = Path(output_path)
    uid_to_ticker = _load_active_universe(Path(active_universe_path))
    allowed_uids = set(uid_to_ticker) if uid_to_ticker else None
    events = _read_raw_events(Path(raw_path), allowed_uids=allowed_uids)
    rows_by_partition: dict[tuple[str, str], list[dict[str, object]]] = {}
    states: dict[str, _InstrumentState] = {}

    for event in events:
        state = states.setdefault(event.instrument_uid, _InstrumentState())
        if event.event_type == "trades":
            state.observe_trade(event.payload)
            continue
        if event.event_type == "candles":
            state.observe_candle(event.payload)
            continue

        ticker = uid_to_ticker.get(event.instrument_uid, event.instrument_uid)
        row = _feature_row(
            event=event,
            ticker=ticker,
            state=state,
            config=resolved_config,
        )
        date_key = event.timestamp.strftime("%Y%m%d")
        rows_by_partition.setdefault((date_key, _safe_partition_value(ticker)), []).append(row)

    files_written: list[Path] = []
    rows_written = 0
    for (date_key, ticker), rows in sorted(rows_by_partition.items()):
        rows.sort(key=lambda item: str(item["timestamp"]))
        path = output_root / f"date={date_key}" / f"instrument={ticker}" / "features.parquet"
        _atomic_write_parquet(path, rows)
        files_written.append(path)
        rows_written += len(rows)

    return FeatureStoreResult(
        output_root=output_root,
        rows_written=rows_written,
        files_written=tuple(files_written),
    )


def _feature_row(
    *,
    event: _RawEvent,
    ticker: str,
    state: _InstrumentState,
    config: FeatureStoreConfig,
) -> dict[str, object]:
    base = _base_row(event, ticker)
    try:
        book = _payload_view(event.payload, "orderbook", "order_book")
        bid, ask = best_bid_ask(book)
        mid = mid_price(book)
        rv_1m, rv_5m, percentile, regime, vwap, ema_fast, ema_slow = state.observe_mid(
            timestamp=event.timestamp,
            mid=mid,
            config=config,
        )
        raw_features: dict[str, Decimal] = {
            "mid": mid,
            "best_bid": bid,
            "best_ask": ask,
            "spread_bps": spread_bps(book),
            "depth_top5_bid": depth_sum(book, "bid", 5),
            "depth_top5_ask": depth_sum(book, "ask", 5),
            "depth_top10_bid": depth_sum(book, "bid", 10),
            "depth_top10_ask": depth_sum(book, "ask", 10),
            "imbalance_5": imbalance(book, 5),
            "imbalance_10": imbalance(book, 10),
            "weighted_imbalance": weighted_imbalance(
                book,
                config.weighted_imbalance_lambda,
            ),
            "microprice": microprice(book),
            "expected_slippage_bps_buy": expected_slippage_bps(
                book,
                "buy",
                config.expected_fill_quantity,
            ),
            "expected_slippage_bps_sell": expected_slippage_bps(
                book,
                "sell",
                config.expected_fill_quantity,
            ),
            "rv_1m": rv_1m,
            "rv_5m": rv_5m,
            "vwap": vwap,
            "ema_fast": ema_fast,
            "ema_slow": ema_slow,
            "volatility_percentile": percentile,
            "bid_wall_score": book_wall_score(book, "bid", config.wall_distance_bps),
            "ask_wall_score": book_wall_score(book, "ask", config.wall_distance_bps),
        }
        row = _sanitized_feature_row(base, raw_features)
        row["volatility_regime"] = regime
        row["usable_row"] = all(row.get(key) is not None for key in _NUMERIC_FEATURE_COLUMNS)
        if row["usable_row"]:
            row["unusable_reason"] = ""
        return row
    except (ArithmeticError, InvalidOperation, TypeError, ValueError) as exc:
        return _unusable_row(base, reason=str(exc))


def _base_row(event: _RawEvent, ticker: str) -> dict[str, object]:
    return {
        "timestamp": event.timestamp.isoformat(),
        "instrument": ticker,
        "instrument_uid": event.instrument_uid,
        "source_event_key": event.stable_key,
    }


def _sanitized_feature_row(
    base: Mapping[str, object],
    raw_features: Mapping[str, Decimal],
) -> dict[str, object]:
    row = dict(base)
    for key in _NUMERIC_FEATURE_COLUMNS:
        row[key] = _finite_float_or_none(raw_features.get(key))
    for key in ("volatility_percentile", "bid_wall_score", "ask_wall_score"):
        row[key] = _finite_float_or_none(raw_features.get(key))
    row["expected_slippage_bps"] = _max_optional(
        row.get("expected_slippage_bps_buy"),
        row.get("expected_slippage_bps_sell"),
    )
    row["unusable_reason"] = (
        "" if all(row.get(key) is not None for key in _NUMERIC_FEATURE_COLUMNS)
        else "non_finite_feature"
    )
    return row


def _unusable_row(base: Mapping[str, object], *, reason: str) -> dict[str, object]:
    row = dict(base)
    for key in _NUMERIC_FEATURE_COLUMNS:
        row[key] = None
    row["volatility_percentile"] = None
    row["volatility_regime"] = "unknown"
    row["bid_wall_score"] = None
    row["ask_wall_score"] = None
    row["expected_slippage_bps"] = None
    row["usable_row"] = False
    row["unusable_reason"] = reason[:240]
    return row


def _read_raw_events(
    raw_path: Path,
    *,
    allowed_uids: set[str] | None,
) -> tuple[_RawEvent, ...]:
    events: list[_RawEvent] = []
    seen: set[str] = set()
    for path in sorted(raw_path.rglob("*.parquet")):
        for row in _read_parquet_rows(path):
            event = _event_from_row(row, path)
            if event is None:
                continue
            if allowed_uids is not None and event.instrument_uid not in allowed_uids:
                continue
            if event.stable_key in seen:
                continue
            seen.add(event.stable_key)
            events.append(event)
    return tuple(
        sorted(
            events,
            key=lambda item: (
                item.timestamp,
                _EVENT_PRIORITY[item.event_type],
                item.instrument_uid,
                item.stable_key,
            ),
        )
    )


def _event_from_row(row: Mapping[str, object], path: Path) -> _RawEvent | None:
    event_type = _event_type(row.get("event_type"), path)
    if event_type is None:
        return None
    instrument_uid = _string(
        row.get("instrument_uid")
        or _path_partition_value(path, "instrument")
    )
    if not instrument_uid:
        return None
    timestamp = _row_timestamp(row)
    payload = _payload(row)
    if timestamp is None or payload is None:
        return None
    stable_key = _stable_event_key(
        timestamp=timestamp,
        instrument_uid=instrument_uid,
        event_type=event_type,
        payload=payload,
        sequence=row.get("sequence"),
    )
    return _RawEvent(
        timestamp=timestamp,
        instrument_uid=instrument_uid,
        event_type=event_type,
        payload=payload,
        stable_key=stable_key,
    )


def _read_parquet_rows(path: Path) -> tuple[Mapping[str, object], ...]:
    try:
        pq = cast(Any, importlib.import_module("pyarrow.parquet"))
        rows = pq.ParquetFile(path).read().to_pylist()
    except Exception:
        return ()
    return tuple(cast(Mapping[str, object], row) for row in rows if isinstance(row, Mapping))


def _payload(row: Mapping[str, object]) -> JsonMapping | None:
    raw = row.get("payload_json")
    if isinstance(raw, str):
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if isinstance(loaded, Mapping):
            return dict(cast(Mapping[str, Any], loaded))
    raw_payload = row.get("payload")
    if isinstance(raw_payload, Mapping):
        return dict(cast(Mapping[str, Any], raw_payload))
    return None


def _payload_view(payload: Mapping[str, object], *nested_keys: str) -> Mapping[str, object]:
    for key in nested_keys:
        value = payload.get(key)
        if isinstance(value, Mapping):
            return cast(Mapping[str, object], value)
    return payload


def _row_timestamp(row: Mapping[str, object]) -> datetime | None:
    for key in ("event_time", "received_at"):
        value = row.get(key)
        if isinstance(value, datetime):
            return _as_utc(value)
        if isinstance(value, str) and value.strip():
            return _parse_datetime(value)
    return None


def _event_type(
    value: object,
    path: Path,
) -> Literal["orderbook", "trades", "candles"] | None:
    normalized = _string(value).strip().lower()
    if not normalized:
        normalized = _path_type(path)
    if normalized in {"orderbook", "trades", "candles"}:
        return cast(Literal["orderbook", "trades", "candles"], normalized)
    return None


def _path_type(path: Path) -> str:
    name = path.name
    if name.startswith("type=") and name.endswith(".parquet"):
        return name.removeprefix("type=").removesuffix(".parquet")
    for part in path.parts:
        if part.startswith("type="):
            return part.removeprefix("type=").removesuffix(".parquet")
    return ""


def _path_partition_value(path: Path, key: str) -> str:
    prefix = f"{key}="
    for part in path.parts:
        if part.startswith(prefix):
            return part.removeprefix(prefix)
    return ""


def _load_active_universe(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, Mapping):
        return {}
    instruments = loaded.get("instruments")
    if not isinstance(instruments, Sequence) or isinstance(instruments, str | bytes):
        return {}
    result: dict[str, str] = {}
    for item in instruments:
        if not isinstance(item, Mapping):
            continue
        enabled = item.get("enabled", True)
        uid = _string(item.get("uid")).strip()
        ticker = _string(item.get("ticker")).strip()
        if uid and ticker and enabled is not False:
            result[uid] = ticker
    return result


def _realized_volatility_bps(
    returns: Sequence[tuple[datetime, float]],
    cutoff: datetime,
) -> Decimal:
    variance_sum = sum(value * value for timestamp, value in returns if timestamp >= cutoff)
    if variance_sum <= 0:
        return Decimal("0")
    return Decimal(str(math.sqrt(variance_sum))) * BPS_FACTOR


def _ema(previous: Decimal | None, current: Decimal, span: int) -> Decimal:
    if previous is None:
        return current
    alpha = Decimal("2") / Decimal(span + 1)
    return (current * alpha) + (previous * (Decimal("1") - alpha))


def _first_present(
    mapping: Mapping[str, object],
    *keys: str,
    default: object,
) -> object:
    for key in keys:
        value = mapping.get(key)
        if value is not None:
            return value
    return default


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, Decimal):
            return value if value.is_finite() else None
        if isinstance(value, int | str):
            parsed = Decimal(value)
            return parsed if parsed.is_finite() else None
        if isinstance(value, float):
            if not math.isfinite(value):
                return None
            parsed = Decimal(str(value))
            return parsed if parsed.is_finite() else None
    except (InvalidOperation, ValueError):
        return None
    return None


def _finite_float_or_none(value: Decimal | None) -> float | None:
    if value is None or not value.is_finite():
        return None
    numeric = float(value)
    if not math.isfinite(numeric):
        return None
    return numeric


def _max_optional(first: object, second: object) -> float | None:
    values = [value for value in (first, second) if isinstance(value, int | float)]
    if not values:
        return None
    return float(max(values))


def _stable_event_key(
    *,
    timestamp: datetime,
    instrument_uid: str,
    event_type: str,
    payload: Mapping[str, object],
    sequence: object,
) -> str:
    raw = json.dumps(
        {
            "timestamp": timestamp.isoformat(),
            "instrument_uid": instrument_uid,
            "event_type": event_type,
            "sequence": None if sequence is None else str(sequence),
            "payload": payload,
        },
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _atomic_write_parquet(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pa = cast(Any, importlib.import_module("pyarrow"))
    pq = cast(Any, importlib.import_module("pyarrow.parquet"))
    table = pa.Table.from_pylist(list(rows))
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        pq.write_table(table, tmp_path)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _parse_datetime(value: str) -> datetime | None:
    try:
        return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _safe_partition_value(value: str) -> str:
    safe = "".join(char if char.isalnum() or char in {"-", "_", "."} else "_" for char in value)
    return safe or "UNKNOWN"


def _string(value: object) -> str:
    return "" if value is None else str(value)


__all__ = [
    "FEATURE_COLUMNS",
    "FeatureStoreConfig",
    "FeatureStoreResult",
    "build_feature_store",
]
