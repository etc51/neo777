"""Extract typed raw datasets for a small, mature Neobitcoin review bundle.

Window selection is deliberately based on receive time.  Backfilled candles
can have old exchange timestamps and must never move the capture window.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Final

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]

UTC_TS: Final = pa.timestamp("us", tz="UTC")
LEVEL: Final = pa.struct([pa.field("price", pa.float64()), pa.field("quantity", pa.float64())])


class ReviewRawError(RuntimeError):
    """The local capture cannot provide a mature, stable review window."""


@dataclass(frozen=True, slots=True)
class ReviewWindow:
    candidate_start: datetime
    candidate_end: datetime
    support_start: datetime
    support_end: datetime


@dataclass(frozen=True, slots=True)
class RawExtractionResult:
    window: ReviewWindow
    paths: Mapping[str, Path]
    rows_by_dataset: Mapping[str, int]
    receive_min: datetime
    receive_max: datetime
    exchange_min: datetime
    exchange_max: datetime


COMMON_FIELDS: Final = (
    pa.field("schema_version", pa.string(), nullable=False),
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("instrument_uid", pa.string(), nullable=False),
    pa.field("instrument_ticker", pa.string(), nullable=False),
    pa.field("exchange_ts", UTC_TS, nullable=False),
    pa.field("receive_ts", UTC_TS, nullable=False),
    pa.field("processing_ts", UTC_TS, nullable=False),
    pa.field("session_id", pa.string(), nullable=False),
    pa.field("collector_instance_id", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=False),
    pa.field("code_commit", pa.string(), nullable=False),
    pa.field("config_hash", pa.string(), nullable=False),
    pa.field("data_quality_flags", pa.list_(pa.string()), nullable=False),
)


def _schema(*fields: pa.Field) -> pa.Schema:
    return pa.schema((*COMMON_FIELDS, *fields), metadata={b"schema_version": b"schema-v3"})


RAW_SCHEMAS: Final[dict[str, pa.Schema]] = {
    "raw_orderbook": _schema(
        pa.field("revision", pa.int64()),
        pa.field("depth", pa.int32(), nullable=False),
        pa.field("best_bid", pa.float64()),
        pa.field("best_ask", pa.float64()),
        pa.field("mid_price", pa.float64()),
        pa.field("spread_price", pa.float64()),
        pa.field("spread_percent", pa.float64()),
        pa.field("spread_ticks", pa.float64()),
        pa.field("trading_status", pa.string()),
        pa.field("feed_latency_ms", pa.float64(), nullable=False),
        pa.field("is_duplicate_snapshot", pa.bool_(), nullable=False),
        pa.field("previous_event_id", pa.string()),
        pa.field("bids", pa.list_(LEVEL), nullable=False),
        pa.field("asks", pa.list_(LEVEL), nullable=False),
    ),
    "raw_trades": _schema(
        pa.field("trade_id", pa.string()),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("quantity", pa.float64(), nullable=False),
        pa.field("direction", pa.string()),
        pa.field("aggressor_side", pa.string()),
        pa.field("feed_latency_ms", pa.float64(), nullable=False),
    ),
    "raw_last_price": _schema(
        pa.field("last_price", pa.float64(), nullable=False),
        pa.field("feed_latency_ms", pa.float64(), nullable=False),
    ),
    "market_status_events": _schema(
        pa.field("status_type", pa.string(), nullable=False),
        pa.field("trading_status", pa.string()),
        pa.field("connection_state", pa.string()),
        pa.field("market_order_available", pa.bool_()),
        pa.field("limit_order_available", pa.bool_()),
    ),
}

for _minutes in (1, 5, 15):
    RAW_SCHEMAS[f"candles_{_minutes}m"] = _schema(
        pa.field("candle_start", UTC_TS, nullable=False),
        pa.field("candle_end", UTC_TS, nullable=False),
        pa.field("open", pa.float64(), nullable=False),
        pa.field("high", pa.float64(), nullable=False),
        pa.field("low", pa.float64(), nullable=False),
        pa.field("close", pa.float64(), nullable=False),
        pa.field("volume", pa.float64(), nullable=False),
        pa.field("is_complete", pa.bool_(), nullable=False),
        pa.field("source_timeframe", pa.string(), nullable=False),
        pa.field("is_backfilled", pa.bool_(), nullable=False),
    )


def extract_review_raw(
    source_root: Path,
    output_dir: Path,
    *,
    now: datetime | None = None,
    candidate_window_minutes: int = 10,
    max_outcome_horizon_minutes: int = 30,
    candle_context_hours: int = 6,
    require_stable_window: bool = True,
    critical_gap_seconds: float = 180.0,
    code_commit: str = "unknown-local",
    config_hash: str = "unknown-local",
) -> RawExtractionResult:
    """Read local JSONL capture and publish one typed Parquet per raw dataset."""

    records = list(_read_jsonl_records(source_root))
    if not records:
        raise ReviewRawError(f"no JSONL records below {source_root}")
    normalized = [_normalize_record(record, code_commit, config_hash) for record in records]
    normalized.sort(key=lambda item: item["receive_ts"])
    window = select_review_window(
        normalized,
        now=now,
        candidate_window_minutes=candidate_window_minutes,
        max_outcome_horizon_minutes=max_outcome_horizon_minutes,
        candle_context_hours=candle_context_hours,
        require_stable_window=require_stable_window,
        critical_gap_seconds=critical_gap_seconds,
    )
    rows = _build_rows(normalized, window)
    data_dir = output_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, Path] = {}
    for dataset, schema in RAW_SCHEMAS.items():
        dataset_rows = sorted(rows[dataset], key=lambda row: (row["receive_ts"], row["event_id"]))
        table = pa.Table.from_pylist(dataset_rows, schema=schema)
        path = data_dir / f"{dataset}.parquet"
        pq.write_table(
            table,
            path,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
            row_group_size=50_000,
        )
        paths[dataset] = path

    included = [row for values in rows.values() for row in values]
    if not included:
        raise ReviewRawError("selected window produced no raw rows")
    return RawExtractionResult(
        window=window,
        paths=paths,
        rows_by_dataset={name: len(values) for name, values in rows.items()},
        receive_min=min(row["receive_ts"] for row in included),
        receive_max=max(row["receive_ts"] for row in included),
        exchange_min=min(row["exchange_ts"] for row in included),
        exchange_max=max(row["exchange_ts"] for row in included),
    )


def select_review_window(
    records: Sequence[Mapping[str, Any]],
    *,
    now: datetime | None = None,
    candidate_window_minutes: int = 10,
    max_outcome_horizon_minutes: int = 30,
    candle_context_hours: int = 6,
    require_stable_window: bool = True,
    critical_gap_seconds: float = 5.0,
) -> ReviewWindow:
    """Return the latest minute-aligned mature window with continuous books."""

    if candidate_window_minutes <= 0 or max_outcome_horizon_minutes <= 0:
        raise ValueError("window and horizon must be positive")
    receive_times = [_as_utc(record["receive_ts"]) for record in records]
    if not receive_times:
        raise ReviewRawError("no receive timestamps")
    clock = _as_utc(now or datetime.now(UTC))
    horizon = timedelta(minutes=max_outcome_horizon_minutes)
    duration = timedelta(minutes=candidate_window_minutes)
    latest_end = min(clock - horizon, max(receive_times) - horizon).replace(second=0, microsecond=0)
    earliest_end = min(receive_times) + duration

    while latest_end >= earliest_end:
        start = latest_end - duration
        support_end = latest_end + horizon
        candidate = [
            record for record in records if start <= _as_utc(record["receive_ts"]) <= latest_end
        ]
        raw_start = start - timedelta(seconds=60)
        support = [
            record
            for record in records
            if raw_start - timedelta(seconds=critical_gap_seconds)
            <= _as_utc(record["receive_ts"])
            <= support_end
        ]
        if _window_is_usable(
            candidate,
            support,
            start,
            latest_end,
            support_end,
            require_stable_window,
            critical_gap_seconds,
        ):
            return ReviewWindow(
                candidate_start=start,
                candidate_end=latest_end,
                support_start=start - timedelta(hours=candle_context_hours),
                support_end=support_end,
            )
        latest_end -= timedelta(minutes=1)
    raise ReviewRawError("no mature stable 10-minute window in local capture")


def _window_is_usable(
    candidate: Sequence[Mapping[str, Any]],
    support: Sequence[Mapping[str, Any]],
    candidate_start: datetime,
    candidate_end: datetime,
    support_end: datetime,
    require_stable: bool,
    gap_seconds: float,
) -> bool:
    books = [record for record in candidate if record.get("event_type") == "orderbook"]
    tolerance = timedelta(seconds=gap_seconds)
    if (
        not books
        or not support
        or max(_as_utc(record["receive_ts"]) for record in support) < support_end - tolerance
    ):
        return False
    raw_start = candidate_start - timedelta(seconds=60)
    required_types = (
        {"orderbook"},
        {"trade", "trades"},
        {"last_price"},
    )
    for event_types in required_types:
        event_times = [
            _as_utc(record["receive_ts"])
            for record in support
            if record.get("event_type") in event_types
        ]
        if (
            not event_times
            or min(event_times) > candidate_start
            or max(event_times) < support_end - tolerance
            or min(event_times) > raw_start + tolerance
        ):
            return False
    if not require_stable:
        return True
    bad_types = {"reconnect", "disconnect", "collector_stop", "critical_gap", "stream_error"}
    if any(str(record.get("event_type")) in bad_types for record in candidate):
        return False
    stream_ids = {
        str(record.get("session_id", "")) for record in candidate if record.get("session_id")
    }
    # The feed is event-driven and identical books may be skipped. Silence is
    # not itself a data gap; explicit quality markers and session transitions
    # above are the reliable discontinuity evidence.
    return len(stream_ids) <= 1


def _build_rows(
    records: Sequence[Mapping[str, Any]], window: ReviewWindow
) -> dict[str, list[dict[str, Any]]]:
    rows: dict[str, list[dict[str, Any]]] = {name: [] for name in RAW_SCHEMAS}
    previous_book_id: str | None = None
    previous_book_hash: str | None = None
    current_status: str | None = None
    for record in records:
        receive_ts = _as_utc(record["receive_ts"])
        event_type = str(record["event_type"])
        payload = _payload(record, event_type)
        common = _common(record)
        if event_type == "trading_status":
            current_status = _optional_text(payload.get("trading_status"))

        if event_type == "candle":
            interval = _interval_minutes(payload.get("interval"))
            if interval not in (1, 5, 15):
                continue
            candle_start = _as_utc(payload.get("time") or record["exchange_ts"])
            candle_end = candle_start + timedelta(minutes=interval)
            if not (window.support_start <= candle_start <= window.support_end):
                continue
            rows[f"candles_{interval}m"].append(
                common
                | {
                    "candle_start": candle_start,
                    "candle_end": candle_end,
                    "open": _quotation(payload.get("open")),
                    "high": _quotation(payload.get("high")),
                    "low": _quotation(payload.get("low")),
                    "close": _quotation(payload.get("close")),
                    "volume": float(payload.get("volume") or 0),
                    # Some stream snapshots report ``is_complete=false``
                    # even for historical candles. Exchange-time closure is
                    # authoritative once the candle end precedes receipt.
                    "is_complete": bool(payload.get("is_complete"))
                    or candle_end <= receive_ts,
                    "source_timeframe": f"{interval}m",
                    "is_backfilled": candle_end < receive_ts - timedelta(minutes=interval),
                }
            )
            continue

        raw_context_seconds = 900 if event_type in {"trade", "trades"} else 60
        if not (
            window.candidate_start - timedelta(seconds=raw_context_seconds + 180)
            <= receive_ts
            <= window.support_end + timedelta(seconds=180)
        ):
            continue
        if event_type == "orderbook":
            bids = _levels(payload.get("bids"))
            asks = _levels(payload.get("asks"))
            signature = hashlib.sha256(
                json.dumps([bids, asks], sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            best_bid = bids[0]["price"] if bids else None
            best_ask = asks[0]["price"] if asks else None
            spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
            mid = (
                (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
            )
            tick = _infer_tick(bids, asks)
            rows["raw_orderbook"].append(
                common
                | {
                    "revision": _optional_int(payload.get("revision")),
                    "depth": int(payload.get("depth") or max(len(bids), len(asks))),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "mid_price": mid,
                    "spread_price": spread,
                    "spread_percent": spread / mid * 100 if spread is not None and mid else None,
                    "spread_ticks": spread / tick if spread is not None and tick else None,
                    "trading_status": current_status,
                    "feed_latency_ms": float(record["feed_latency_ms"]),
                    "is_duplicate_snapshot": signature == previous_book_hash,
                    "previous_event_id": previous_book_id,
                    "bids": bids,
                    "asks": asks,
                }
            )
            previous_book_hash = signature
            previous_book_id = str(record["event_id"])
        elif event_type in {"trade", "trades"}:
            direction = _optional_text(payload.get("direction"))
            rows["raw_trades"].append(
                common
                | {
                    "trade_id": _optional_text(payload.get("trade_id")),
                    "price": _quotation(payload.get("price")),
                    "quantity": float(payload.get("quantity") or 0),
                    "direction": direction,
                    "aggressor_side": _aggressor(direction),
                    "feed_latency_ms": float(record["feed_latency_ms"]),
                }
            )
        elif event_type == "last_price":
            rows["raw_last_price"].append(
                common
                | {
                    "last_price": _quotation(payload.get("price")),
                    "feed_latency_ms": float(record["feed_latency_ms"]),
                }
            )
        elif event_type in {"trading_status", "reconnect", "disconnect", "subscription_ack"}:
            rows["market_status_events"].append(
                common
                | {
                    "status_type": event_type,
                    "trading_status": _optional_text(payload.get("trading_status")),
                    "connection_state": _optional_text(record.get("connection_state")),
                    "market_order_available": payload.get("market_order_available_flag"),
                    "limit_order_available": payload.get("limit_order_available_flag"),
                }
            )
    return rows


def _read_jsonl_records(root: Path) -> Iterable[dict[str, Any]]:
    paths = [root] if root.is_file() else sorted(root.rglob("*.jsonl"))
    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            while line := handle.readline():
                try:
                    value = json.loads(line)
                except json.JSONDecodeError as exc:
                    # A concurrently appended final line is not yet durable.
                    if not line.endswith("\n"):
                        break
                    raise ReviewRawError(f"invalid JSONL in {path}") from exc
                if isinstance(value, dict):
                    yield value


def _normalize_record(
    record: Mapping[str, Any], code_commit: str, config_hash: str
) -> dict[str, Any]:
    receive = _as_utc(record.get("receive_timestamp") or record.get("receive_ts"))
    exchange = _as_utc(record.get("exchange_timestamp") or record.get("exchange_ts") or receive)
    session = str(record.get("stream_id") or record.get("session_id") or "local-session")
    uid = str(record.get("instrument_uid") or "").strip()
    event_id = str(record.get("event_id") or "").strip()
    if not uid or not event_id:
        raise ReviewRawError("raw record has an empty event_id or instrument_uid")
    return dict(record) | {
        "event_id": event_id,
        "instrument_uid": uid,
        "instrument_ticker": str(
            record.get("ticker") or record.get("instrument_ticker") or "NEOBITCOIN"
        ),
        "event_type": str(record.get("event_type") or "unknown"),
        "exchange_ts": exchange,
        "receive_ts": receive,
        "processing_ts": _as_utc(record.get("processing_timestamp") or receive),
        "session_id": session,
        "collector_instance_id": str(record.get("collector_instance_id") or session),
        "source": str(record.get("source") or "local_raw_jsonl"),
        "code_commit": code_commit or "unknown-local",
        "config_hash": config_hash or "unknown-local",
        "feed_latency_ms": float(
            record.get("latency_ms") or max((receive - exchange).total_seconds() * 1000, 0)
        ),
    }


def _common(record: Mapping[str, Any]) -> dict[str, Any]:
    flags: list[str] = []
    if record.get("is_consistent") is False:
        flags.append("INCONSISTENT")
    if record.get("excluded_from_analysis") is True:
        flags.append("EXCLUDED")
    if not record.get("processing_timestamp"):
        flags.append("PROCESSING_TS_FALLBACK_RECEIVE")
    return {
        "schema_version": "schema-v3",
        "event_id": str(record["event_id"]),
        "instrument_uid": str(record["instrument_uid"]),
        "instrument_ticker": str(record["instrument_ticker"]),
        "exchange_ts": _as_utc(record["exchange_ts"]),
        "receive_ts": _as_utc(record["receive_ts"]),
        "processing_ts": _as_utc(record["processing_ts"]),
        "session_id": str(record["session_id"]),
        "collector_instance_id": str(record["collector_instance_id"]),
        "source": str(record["source"]),
        "code_commit": str(record["code_commit"]),
        "config_hash": str(record["config_hash"]),
        "data_quality_flags": flags,
    }


def _payload(record: Mapping[str, Any], event_type: str) -> Mapping[str, Any]:
    payload = record.get("payload")
    if not isinstance(payload, Mapping):
        return {}
    nested = payload.get(event_type)
    if event_type == "trades" and nested is None:
        nested = payload.get("trade")
    return nested if isinstance(nested, Mapping) else payload


def _levels(value: object) -> list[dict[str, float]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    result = []
    for level in value:
        if isinstance(level, Mapping):
            result.append(
                {
                    "price": _quotation(level.get("price")),
                    "quantity": float(level.get("quantity") or 0),
                }
            )
    return result


def _quotation(value: object) -> float:
    if isinstance(value, Mapping):
        return float(value.get("units") or 0) + float(value.get("nano") or 0) / 1_000_000_000
    if value is None:
        return math.nan
    if isinstance(value, (int, float, str)):
        return float(value)
    raise ReviewRawError(f"invalid quotation: {value!r}")


def _infer_tick(
    bids: Sequence[Mapping[str, float]], asks: Sequence[Mapping[str, float]]
) -> float | None:
    prices = sorted({level["price"] for level in (*bids, *asks)})
    deltas = [right - left for left, right in zip(prices, prices[1:], strict=False) if right > left]
    return min(deltas) if deltas else None


def _interval_minutes(value: object) -> int:
    mapping = {1: 1, 2: 5, 3: 15, "1m": 1, "5m": 5, "15m": 15}
    return mapping.get(value, int(value) if isinstance(value, int) else 0)


def _aggressor(direction: str | None) -> str | None:
    return {"1": "BUY", "2": "SELL", "BUY": "BUY", "SELL": "SELL"}.get(direction or "")


def _optional_text(value: object) -> str | None:
    return None if value is None else str(value)


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, str)):
        return int(value)
    raise ReviewRawError(f"invalid integer: {value!r}")


def _as_utc(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ReviewRawError(f"invalid timestamp: {value!r}")
    if parsed.tzinfo is None:
        raise ReviewRawError("timestamp must be timezone-aware")
    return parsed.astimezone(UTC)
