"""Readonly neoasset discovery and liquidity precheck helpers."""

from __future__ import annotations

import csv
import json
import os
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final, TypeAlias, cast
from uuid import uuid4

import yaml

from neo_trader.features.orderbook import depth_sum, expected_slippage_bps, spread_bps
from neo_trader.runtime import get_runtime_commit_hash

JsonMapping: TypeAlias = Mapping[str, Any]
OrderBookProvider: TypeAlias = Callable[["NeoAssetRecord"], Mapping[str, object] | None]

NANO_FACTOR: Final = Decimal("1000000000")
DEFAULT_DISCOVERY_MAX_DEPTH: Final = 8
NEO_MARKERS: Final = ("neo", "нео")
INDICATIVE_MARKERS: Final = ("indicative", "indicatives", "индикатив")


@dataclass(frozen=True)
class LiquidityPrecheckConfig:
    """Thresholds for enabling discovered neoassets for recording."""

    test_quantity: Decimal = Decimal("1")
    max_spread_bps: Decimal = Decimal("100")
    max_slippage_bps: Decimal = Decimal("100")
    max_enabled: int = 10


@dataclass(frozen=True)
class NeoAssetRecord:
    """Discovered neoasset plus readonly market-data precheck metrics."""

    ticker: str
    uid: str
    figi: str | None
    class_code: str
    name: str
    instrument_type: str
    exchange: str
    currency: str
    api_trade_available_flag: bool | None
    lot: int | None
    price_increment: Decimal | None
    enabled: bool = False
    notes: tuple[str, ...] = ()
    market_data_available: bool = False
    spread_bps: Decimal | None = None
    top5_depth: Decimal = Decimal("0")
    top10_depth: Decimal = Decimal("0")
    expected_slippage_bps_buy: Decimal | None = None
    expected_slippage_bps_sell: Decimal | None = None
    score: Decimal = Decimal("0")
    raw: dict[str, object] = field(default_factory=dict)

    def to_universe_dict(self) -> dict[str, object]:
        """Return YAML-safe config row for ``configs/neoassets_universe.yaml``."""

        return {
            "ticker": self.ticker,
            "uid": self.uid,
            "figi": self.figi or "",
            "class_code": self.class_code,
            "name": self.name,
            "instrument_type": self.instrument_type,
            "exchange": self.exchange,
            "currency": self.currency,
            "api_trade_available_flag": self.api_trade_available_flag,
            "lot": self.lot,
            "price_increment": _decimal_to_string(self.price_increment),
            "enabled": self.enabled,
            "notes": list(self.notes),
            "market_data_available": self.market_data_available,
            "spread_bps": _decimal_to_string(self.spread_bps),
            "top5_depth": str(self.top5_depth),
            "top10_depth": str(self.top10_depth),
            "expected_slippage_bps_buy": _decimal_to_string(
                self.expected_slippage_bps_buy
            ),
            "expected_slippage_bps_sell": _decimal_to_string(
                self.expected_slippage_bps_sell
            ),
            "score": str(self.score),
        }

    def to_report_dict(self, *, include_raw: bool = False) -> dict[str, object]:
        """Return JSON-safe discovery report row."""

        payload = self.to_universe_dict()
        if include_raw:
            payload["raw"] = _json_safe(self.raw)
        return payload


@dataclass(frozen=True)
class DiscoveryReportPaths:
    """Generated discovery artifact paths."""

    json_path: Path
    csv_path: Path


def parse_neoasset_candidates(payloads: Mapping[str, object]) -> tuple[NeoAssetRecord, ...]:
    """Parse readonly instrument API payloads into neoasset candidates.

    The API does not always expose a dedicated neoasset flag. Selection therefore
    first prefers explicit ``neo`` metadata and then falls back to indicative
    instrument metadata/endpoints, preserving notes that explain the source.
    """

    by_key: dict[str, NeoAssetRecord] = {}
    for source, payload in payloads.items():
        for raw in _iter_instrument_payloads(payload):
            selected, notes = _selection_notes(raw, source)
            if not selected:
                continue
            record = _candidate_from_raw(raw, source=source, notes=notes)
            key = _record_key(record)
            existing = by_key.get(key)
            by_key[key] = record if existing is None else _merge_records(existing, record)

    return tuple(
        sorted(
            by_key.values(),
            key=lambda item: (item.ticker.upper(), item.class_code.upper(), item.uid),
        )
    )


def discovery_diagnostics(
    payloads: Mapping[str, object],
    *,
    source_errors: Mapping[str, str] | None = None,
) -> dict[str, object]:
    """Build a compact diagnostic summary for discovery reports."""

    all_counts: dict[str, int] = {}
    neo_counts: dict[str, int] = {}
    indicative_counts: dict[str, int] = {}
    for source, payload in payloads.items():
        mappings = tuple(_iter_instrument_payloads(payload))
        all_counts[source] = len(mappings)
        neo_counts[source] = sum(1 for item in mappings if _contains_marker(item, NEO_MARKERS))
        indicative_counts[source] = sum(
            1 for item in mappings if _contains_marker(item, INDICATIVE_MARKERS)
        )

    return {
        "source_counts": all_counts,
        "explicit_neo_counts": neo_counts,
        "indicative_metadata_counts": indicative_counts,
        "source_errors": dict(source_errors or {}),
        "selection_rule": (
            "explicit neo metadata first; indicative endpoint/metadata fallback "
            "when no dedicated neoasset flag is present"
        ),
    }


def apply_liquidity_precheck(
    records: Sequence[NeoAssetRecord],
    orderbook_provider: OrderBookProvider,
    *,
    config: LiquidityPrecheckConfig | None = None,
) -> tuple[NeoAssetRecord, ...]:
    """Fetch readonly order book snapshots and enable only usable records."""

    resolved_config = config or LiquidityPrecheckConfig()
    checked = tuple(
        _precheck_one(record, orderbook_provider, resolved_config) for record in records
    )
    ranked = sorted(
        checked,
        key=lambda item: (item.enabled, item.score, item.ticker.upper()),
        reverse=True,
    )

    if resolved_config.max_enabled <= 0:
        return tuple(replace(item, enabled=False) for item in ranked)

    enabled_seen = 0
    limited: list[NeoAssetRecord] = []
    for item in ranked:
        if item.enabled and enabled_seen < resolved_config.max_enabled:
            enabled_seen += 1
            limited.append(item)
        elif item.enabled:
            limited.append(
                replace(
                    item,
                    enabled=False,
                    notes=_append_note(item.notes, "disabled_by_max_enabled_limit"),
                )
            )
        else:
            limited.append(item)
    return tuple(limited)


def write_neoassets_universe(
    path: Path,
    records: Sequence[NeoAssetRecord],
    *,
    generated_at: datetime | None = None,
) -> Path:
    """Write discovered neoassets to YAML config consumed by the recorder."""

    timestamp = _as_utc(generated_at or datetime.now(UTC))
    payload: dict[str, object] = {
        "generated_by": "neo_trader.research.neoassets",
        "generated_at": timestamp.isoformat(),
        "commit_hash": get_runtime_commit_hash(),
        "instruments": [record.to_universe_dict() for record in records],
    }
    _atomic_write_text(path, yaml.safe_dump(payload, sort_keys=False, allow_unicode=True))
    return path


def write_discovery_reports(
    *,
    reports_dir: Path,
    records: Sequence[NeoAssetRecord],
    diagnostics: Mapping[str, object],
    generated_at: datetime | None = None,
    include_raw: bool = True,
) -> DiscoveryReportPaths:
    """Write JSON and CSV discovery reports."""

    timestamp = _as_utc(generated_at or datetime.now(UTC))
    reports_dir.mkdir(parents=True, exist_ok=True)
    suffix = timestamp.strftime("%Y%m%d_%H%M%S")
    json_path = reports_dir / f"neoassets_discovery_{suffix}.json"
    csv_path = reports_dir / f"neoassets_discovery_{suffix}.csv"

    payload = {
        "generated_at": timestamp.isoformat(),
        "commit_hash": get_runtime_commit_hash(),
        "neoassets_found": len(records),
        "neoassets_enabled": sum(1 for record in records if record.enabled),
        "diagnostics": _json_safe(diagnostics),
        "instruments": [
            record.to_report_dict(include_raw=include_raw) for record in records
        ],
    }
    _atomic_write_text(
        json_path,
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
    )
    _atomic_write_csv(csv_path, [_csv_row(record) for record in records])
    return DiscoveryReportPaths(json_path=json_path, csv_path=csv_path)


def tbank_orderbook_payload_to_book(payload: Mapping[str, object]) -> dict[str, object]:
    """Convert a T-Bank order book response into feature-engine input."""

    return {
        "bids": [_level_to_book_level(level) for level in _mapping_items(payload.get("bids"))],
        "asks": [_level_to_book_level(level) for level in _mapping_items(payload.get("asks"))],
    }


def quotation_to_decimal(value: object) -> Decimal | None:
    """Convert a quotation-like object or scalar to ``Decimal``."""

    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | str):
        try:
            return Decimal(value)
        except InvalidOperation:
            return None
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, Mapping):
        units = _int_or_default(value.get("units"), 0)
        nano = _int_or_default(value.get("nano"), 0)
        return Decimal(units) + (Decimal(nano) / NANO_FACTOR)
    units_attr = getattr(value, "units", None)
    nano_attr = getattr(value, "nano", None)
    if units_attr is not None or nano_attr is not None:
        return Decimal(_int_or_default(units_attr, 0)) + (
            Decimal(_int_or_default(nano_attr, 0)) / NANO_FACTOR
        )
    return None


def _precheck_one(
    record: NeoAssetRecord,
    orderbook_provider: OrderBookProvider,
    config: LiquidityPrecheckConfig,
) -> NeoAssetRecord:
    notes = list(record.notes)
    if not record.uid.strip():
        return replace(
            record,
            enabled=False,
            notes=_append_note(tuple(notes), "missing_uid"),
        )

    try:
        book = orderbook_provider(record)
    except Exception as exc:  # noqa: BLE001
        notes.append(f"market_data_error={type(exc).__name__}")
        return replace(record, enabled=False, notes=tuple(dict.fromkeys(notes)))

    if book is None:
        notes.append("market_data_unavailable")
        return replace(record, enabled=False, notes=tuple(dict.fromkeys(notes)))

    try:
        spread = spread_bps(book)
        top5_depth = depth_sum(book, "bid", 5) + depth_sum(book, "ask", 5)
        top10_depth = depth_sum(book, "bid", 10) + depth_sum(book, "ask", 10)
        buy_slippage = expected_slippage_bps(book, "buy", config.test_quantity)
        sell_slippage = expected_slippage_bps(book, "sell", config.test_quantity)
    except (ValueError, TypeError, InvalidOperation) as exc:
        notes.append(f"invalid_orderbook={type(exc).__name__}")
        return replace(record, enabled=False, notes=tuple(dict.fromkeys(notes)))

    enabled = (
        spread <= config.max_spread_bps
        and buy_slippage <= config.max_slippage_bps
        and sell_slippage <= config.max_slippage_bps
        and top5_depth > 0
        and top10_depth > 0
    )
    if not enabled:
        notes.append("liquidity_threshold_failed")

    return replace(
        record,
        enabled=enabled,
        notes=tuple(dict.fromkeys(notes)),
        market_data_available=True,
        spread_bps=spread,
        top5_depth=top5_depth,
        top10_depth=top10_depth,
        expected_slippage_bps_buy=buy_slippage,
        expected_slippage_bps_sell=sell_slippage,
        score=_liquidity_score(
            market_data_available=True,
            spread=spread,
            top5_depth=top5_depth,
            top10_depth=top10_depth,
            buy_slippage=buy_slippage,
            sell_slippage=sell_slippage,
        ),
    )


def _liquidity_score(
    *,
    market_data_available: bool,
    spread: Decimal | None,
    top5_depth: Decimal,
    top10_depth: Decimal,
    buy_slippage: Decimal | None,
    sell_slippage: Decimal | None,
) -> Decimal:
    if not market_data_available or spread is None:
        return Decimal("0")
    slippage_values = [
        value for value in (buy_slippage, sell_slippage) if value is not None
    ]
    slippage_penalty = max(slippage_values) if slippage_values else Decimal("10000")
    return (top5_depth + top10_depth) / (Decimal("1") + spread + slippage_penalty)


def _candidate_from_raw(
    raw: Mapping[str, object],
    *,
    source: str,
    notes: Sequence[str],
) -> NeoAssetRecord:
    ticker = _text(raw, "ticker", "symbol", "positionTicker") or _text(
        raw,
        "uid",
        "instrumentUid",
        "instrument_uid",
    )
    uid = _text(raw, "uid", "instrumentUid", "instrument_uid", "positionUid") or ""
    figi = _text(raw, "figi")
    exchange = _text(raw, "exchange", "instrumentExchange", "instrument_exchange") or ""
    class_code = _text(raw, "classCode", "class_code") or exchange or "NEO"
    normalized_ticker = (ticker or figi or uid or "UNKNOWN").strip()
    instrument_type = _text(raw, "instrumentType", "instrument_type", "type") or source
    return NeoAssetRecord(
        ticker=normalized_ticker,
        uid=uid,
        figi=figi,
        class_code=class_code,
        name=_text(raw, "name", "assetName", "asset_name") or normalized_ticker,
        instrument_type=instrument_type,
        exchange=exchange,
        currency=_text(raw, "currency", "nominalCurrency") or "",
        api_trade_available_flag=_bool_or_none(
            raw,
            "apiTradeAvailableFlag",
            "api_trade_available_flag",
        ),
        lot=_int_or_none(raw, "lot"),
        price_increment=quotation_to_decimal(
            _first(raw, "minPriceIncrement", "min_price_increment", "priceIncrement")
        ),
        notes=tuple(dict.fromkeys(notes)),
        raw=dict(raw),
    )


def _merge_records(existing: NeoAssetRecord, new: NeoAssetRecord) -> NeoAssetRecord:
    return replace(
        existing,
        uid=existing.uid or new.uid,
        figi=existing.figi or new.figi,
        class_code=existing.class_code or new.class_code,
        name=existing.name or new.name,
        instrument_type=existing.instrument_type or new.instrument_type,
        exchange=existing.exchange or new.exchange,
        currency=existing.currency or new.currency,
        api_trade_available_flag=(
            existing.api_trade_available_flag
            if existing.api_trade_available_flag is not None
            else new.api_trade_available_flag
        ),
        lot=existing.lot if existing.lot is not None else new.lot,
        price_increment=(
            existing.price_increment
            if existing.price_increment is not None
            else new.price_increment
        ),
        notes=tuple(dict.fromkeys((*existing.notes, *new.notes))),
    )


def _selection_notes(raw: Mapping[str, object], source: str) -> tuple[bool, tuple[str, ...]]:
    notes = [f"source={source}"]
    if _contains_marker(raw, NEO_MARKERS):
        notes.append("explicit_neo_metadata")
        return True, tuple(notes)
    if "indicative" in source.lower() or _contains_marker(raw, INDICATIVE_MARKERS):
        notes.append("metadata_discovery_no_explicit_neo_flag")
        return True, tuple(notes)
    return False, tuple(notes)


def _iter_instrument_payloads(value: object) -> Iterator[Mapping[str, object]]:
    for item in _iter_mappings(value):
        if _looks_like_instrument(item):
            yield item


def _iter_mappings(value: object, *, depth: int = 0) -> Iterator[Mapping[str, object]]:
    if depth > DEFAULT_DISCOVERY_MAX_DEPTH:
        return
    if isinstance(value, Mapping):
        mapping = {str(key): item for key, item in value.items()}
        yield mapping
        for nested in mapping.values():
            yield from _iter_mappings(nested, depth=depth + 1)
        return
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for item in value:
            yield from _iter_mappings(item, depth=depth + 1)


def _looks_like_instrument(raw: Mapping[str, object]) -> bool:
    ticker = _text(raw, "ticker", "symbol", "positionTicker")
    uid = _text(raw, "uid", "instrumentUid", "instrument_uid", "positionUid")
    figi = _text(raw, "figi")
    return bool(ticker and (uid or figi))


def _record_key(record: NeoAssetRecord) -> str:
    if record.uid:
        return f"uid:{record.uid}"
    if record.figi:
        return f"figi:{record.figi}"
    return f"ticker:{record.ticker}:{record.class_code}"


def _contains_marker(raw: Mapping[str, object], markers: Sequence[str]) -> bool:
    for text in _walk_text(raw):
        normalized = text.lower()
        if any(marker in normalized for marker in markers):
            return True
    return False


def _walk_text(value: object) -> Iterator[str]:
    if isinstance(value, str):
        yield value
        return
    if isinstance(value, Mapping):
        for key, nested in value.items():
            yield str(key)
            yield from _walk_text(nested)
        return
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        for item in value:
            yield from _walk_text(item)


def _level_to_book_level(level: Mapping[str, object]) -> dict[str, object]:
    price = quotation_to_decimal(level.get("price"))
    quantity = quotation_to_decimal(level.get("quantity"))
    if price is None or quantity is None:
        raise ValueError("order book level must contain price and quantity.")
    return {"price": price, "quantity": quantity}


def _mapping_items(value: object) -> tuple[Mapping[str, object], ...]:
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        return ()
    return tuple(cast(Mapping[str, object], item) for item in value if isinstance(item, Mapping))


def _first(raw: Mapping[str, object], *keys: str) -> object | None:
    for key in keys:
        if key in raw:
            return raw[key]
    return None


def _text(raw: Mapping[str, object], *keys: str) -> str | None:
    value = _first(raw, *keys)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _bool_or_none(raw: Mapping[str, object], *keys: str) -> bool | None:
    value = _first(raw, *keys)
    return value if isinstance(value, bool) else None


def _int_or_none(raw: Mapping[str, object], *keys: str) -> int | None:
    value = _first(raw, *keys)
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value))
    except ValueError:
        return None


def _int_or_default(value: object, default: int) -> int:
    if value is None or isinstance(value, bool):
        return default
    try:
        return int(str(value))
    except ValueError:
        return default


def _append_note(notes: tuple[str, ...], note: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys((*notes, note)))


def _csv_row(record: NeoAssetRecord) -> dict[str, object]:
    return {
        key: value
        for key, value in record.to_universe_dict().items()
        if key != "notes"
    } | {"notes": ";".join(record.notes)}


def _atomic_write_csv(path: Path, rows: Sequence[Mapping[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    fieldnames = (
        "ticker",
        "uid",
        "figi",
        "class_code",
        "name",
        "instrument_type",
        "exchange",
        "currency",
        "api_trade_available_flag",
        "lot",
        "price_increment",
        "enabled",
        "notes",
        "market_data_available",
        "spread_bps",
        "top5_depth",
        "top10_depth",
        "expected_slippage_bps_buy",
        "expected_slippage_bps_sell",
        "score",
    )
    try:
        with tmp_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _atomic_write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        tmp_path.write_text(value, encoding="utf-8")
        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def _decimal_to_string(value: Decimal | None) -> str | None:
    return None if value is None else str(value)


def _json_safe(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return _as_utc(value).isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_json_safe(item) for item in value]
    return value


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = [
    "DiscoveryReportPaths",
    "LiquidityPrecheckConfig",
    "NeoAssetRecord",
    "apply_liquidity_precheck",
    "discovery_diagnostics",
    "parse_neoasset_candidates",
    "quotation_to_decimal",
    "tbank_orderbook_payload_to_book",
    "write_discovery_reports",
    "write_neoassets_universe",
]
