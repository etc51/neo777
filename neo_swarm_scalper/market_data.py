"""Read-only market-data feed for Neobitcoin and Neoether."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

import yaml

from neo_swarm_scalper.config import AUTO_DISCOVER, NeoSwarmScalperConfig
from neo_swarm_scalper.safety import load_token_from_env_or_dotenv, mask_token_like_text
from neo_swarm_scalper.storage import SQLiteJournal
from neo_swarm_scalper.types import BookLevel, InstrumentMetadata, MarketSnapshot, decimal_or_none


class ReadOnlyMarketDataProvider(Protocol):
    def find_instruments(self, query: str) -> Sequence[Mapping[str, Any]]:
        """Find instrument metadata. Must not expose order methods."""

    def get_trading_status(self, instrument_id: str) -> Mapping[str, Any]:
        """Return trading status for a read-only instrument id."""

    def get_orderbook_snapshot(self, instrument_id: str, *, depth: int) -> Mapping[str, Any]:
        """Return a read-only order book snapshot."""

    def get_recent_trades(self, instrument_id: str) -> Sequence[Mapping[str, Any]]:
        """Return recent read-only trades or last-price records when trades are unavailable."""

    def get_candles(
        self,
        instrument_id: str,
        *,
        interval: str,
        from_time: datetime,
        to_time: datetime,
    ) -> Sequence[Mapping[str, Any]]:
        """Return read-only candle payloads for an instrument."""


class TBankReadOnlyProvider:
    """Adapter around the existing read-only REST client."""

    def __init__(self, token: str) -> None:
        from neo_trader.broker.tbank import TBankClient

        self._client = TBankClient(token=token, mode="readonly")

    def find_instruments(self, query: str) -> Sequence[Mapping[str, Any]]:
        result = self._client.get_instruments([query], api_trade_available_only=False)
        return [item.raw for items in result.values() for item in items]

    def get_trading_status(self, instrument_id: str) -> Mapping[str, Any]:
        return self._client.get_trading_status(instrument_id).raw

    def get_orderbook_snapshot(self, instrument_id: str, *, depth: int) -> Mapping[str, Any]:
        book = self._client.get_orderbook_snapshot(instrument_id, depth=depth)
        return {
            "figi": book.figi,
            "instrumentUid": book.instrument_uid,
            "depth": book.depth,
            "bids": [
                {"price": str(level.price), "quantity": level.quantity, **dict(level.raw)}
                for level in book.bids
            ],
            "asks": [
                {"price": str(level.price), "quantity": level.quantity, **dict(level.raw)}
                for level in book.asks
            ],
            "lastPrice": None if book.last_price is None else str(book.last_price),
            "raw": dict(book.raw),
        }

    def get_recent_trades(self, instrument_id: str) -> Sequence[Mapping[str, Any]]:
        return self._client.get_last_prices([instrument_id])

    def get_candles(
        self,
        instrument_id: str,
        *,
        interval: str,
        from_time: datetime,
        to_time: datetime,
    ) -> Sequence[Mapping[str, Any]]:
        return self._client.get_candles(
            instrument_id,
            interval=interval,
            from_time=from_time,
            to_time=to_time,
        )


@dataclass(frozen=True)
class FeedStatus:
    connected: bool
    token_present: bool
    instruments: tuple[InstrumentMetadata, ...]
    last_error: str | None = None


class NeoMarketDataFeed:
    """Discovers instruments and polls read-only market data.

    Real T-Bank token usage is confined to this class and only for market data.
    """

    def __init__(
        self,
        config: NeoSwarmScalperConfig,
        *,
        storage: SQLiteJournal,
        provider: ReadOnlyMarketDataProvider | None = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self._provider = provider
        self._metadata: dict[str, InstrumentMetadata] = {}
        self.status = FeedStatus(connected=False, token_present=False, instruments=())

    @property
    def metadata(self) -> tuple[InstrumentMetadata, ...]:
        return tuple(self._metadata[name] for name in sorted(self._metadata))

    def connect(self) -> FeedStatus:
        token = load_token_from_env_or_dotenv(env_name=self.config.token_env)
        token_present = token is not None
        if self._provider is None and token is not None:
            self._provider = TBankReadOnlyProvider(token)
        if self._provider is None:
            self.status = FeedStatus(
                connected=False,
                token_present=token_present,
                instruments=(),
                last_error=f"{self.config.token_env} is missing",
            )
            self.storage.record_data_quality(
                timestamp_utc=datetime.now(UTC),
                instrument=None,
                issue_type="missing_token",
                severity="warning",
                details=f"{self.config.token_env} is required for live market data",
            )
            return self.status

        discovered: list[InstrumentMetadata] = []
        for instrument in self.config.enabled_instruments:
            metadata = self._discover_one(instrument.name)
            if metadata is not None:
                self._metadata[instrument.name] = metadata
                discovered.append(metadata)
        connected = bool(discovered)
        self.status = FeedStatus(
            connected=connected,
            token_present=token_present,
            instruments=tuple(discovered),
            last_error=None if connected else "no instruments discovered",
        )
        return self.status

    def poll_once(self, *, timestamp_utc: datetime | None = None) -> tuple[MarketSnapshot, ...]:
        if self._provider is None:
            self.connect()
        if self._provider is None:
            return ()
        snapshots: list[MarketSnapshot] = []
        for metadata in self.metadata:
            try:
                event_timestamp = timestamp_utc or datetime.now(UTC)
                raw = self._provider.get_orderbook_snapshot(
                    metadata.instrument_id,
                    depth=self.config.data.orderbook_depth,
                )
                snapshot = _snapshot_from_orderbook(raw, metadata, timestamp_utc=event_timestamp)
                snapshots.append(snapshot)
                self.storage.record_market_snapshot(snapshot)
                if snapshot.orderbook_missing:
                    self.storage.record_data_quality(
                        timestamp_utc=snapshot.timestamp_utc,
                        instrument=metadata.name,
                        issue_type="orderbook_missing",
                        severity="warning",
                        details={"instrument_id": metadata.instrument_id},
                    )
                if metadata.min_price_increment is None:
                    self.storage.record_data_quality(
                        timestamp_utc=snapshot.timestamp_utc,
                        instrument=metadata.name,
                        issue_type="tick_size_unknown",
                        severity="warning",
                        details={"ticker": metadata.ticker},
                    )
                self._record_optional_market_events(metadata, snapshot.timestamp_utc)
            except Exception as exc:  # noqa: BLE001 - feed must keep the other instrument alive.
                fallback = self._fallback_snapshot(metadata, timestamp_utc or datetime.now(UTC))
                if fallback is not None:
                    snapshots.append(fallback)
                    self.storage.record_market_snapshot(fallback)
                    self._record_optional_market_events(metadata, fallback.timestamp_utc)
                self.storage.record_data_quality(
                    timestamp_utc=datetime.now(UTC),
                    instrument=metadata.name,
                    issue_type="market_data_error",
                    severity="error",
                    details=mask_token_like_text(str(exc)),
                )
        return tuple(snapshots)

    def _fallback_snapshot(
        self,
        metadata: InstrumentMetadata,
        timestamp_utc: datetime,
    ) -> MarketSnapshot | None:
        provider = self._provider
        if provider is None:
            return None
        price: Decimal | None = None
        try:
            for raw in provider.get_recent_trades(metadata.instrument_id):
                event = _trade_from_raw(raw, timestamp_utc)
                if event is not None:
                    price = event["price"]
                    break
        except Exception as exc:  # noqa: BLE001 - fallback best effort only.
            self.storage.record_data_quality(
                timestamp_utc=timestamp_utc,
                instrument=metadata.name,
                issue_type="fallback_last_price_unavailable",
                severity="warning",
                details=mask_token_like_text(str(exc)),
            )
        if price is None:
            return None
        return MarketSnapshot(
            timestamp_utc=timestamp_utc,
            instrument=metadata.name,
            metadata=metadata,
            last_price=price,
            bid_levels=(),
            ask_levels=(),
            orderbook_missing=True,
            stale=False,
            raw={"fallback": "last_price_after_orderbook_error"},
        )

    def _record_optional_market_events(
        self,
        metadata: InstrumentMetadata,
        timestamp_utc: datetime,
    ) -> None:
        provider = self._provider
        if provider is None:
            return
        if self.config.data.collect_trades:
            try:
                for raw in provider.get_recent_trades(metadata.instrument_id):
                    event = _trade_from_raw(raw, timestamp_utc)
                    if event is None:
                        continue
                    self.storage.record_market_trade(
                        timestamp_utc=event["timestamp"],
                        instrument=metadata.name,
                        price=event["price"],
                        quantity=event["quantity"],
                        side=event["side"],
                        raw=dict(raw),
                    )
            except Exception as exc:  # noqa: BLE001 - trade collection is optional.
                self.storage.record_data_quality(
                    timestamp_utc=timestamp_utc,
                    instrument=metadata.name,
                    issue_type="trades_unavailable",
                    severity="warning",
                    details=mask_token_like_text(str(exc)),
                )

        for timeframe, enabled, interval in _candle_requests(self.config):
            if not enabled:
                continue
            try:
                candles = provider.get_candles(
                    metadata.instrument_id,
                    interval=interval,
                    from_time=timestamp_utc - timedelta(minutes=20),
                    to_time=timestamp_utc,
                )
                parsed = [
                    candle
                    for raw in candles
                    if (candle := _candle_from_raw(raw, timestamp_utc)) is not None
                ]
                if not parsed:
                    continue
                candle = max(parsed, key=lambda item: item["timestamp"])
                self.storage.record_candle(
                    timestamp_utc=candle["timestamp"],
                    instrument=metadata.name,
                    timeframe=timeframe,
                    open_price=candle["open"],
                    high=candle["high"],
                    low=candle["low"],
                    close=candle["close"],
                    volume=candle["volume"],
                    raw=candle["raw"],
                )
            except Exception as exc:  # noqa: BLE001 - candle collection is optional.
                self.storage.record_data_quality(
                    timestamp_utc=timestamp_utc,
                    instrument=metadata.name,
                    issue_type=f"candles_{timeframe}_unavailable",
                    severity="warning",
                    details=mask_token_like_text(str(exc)),
                )

    def _discover_one(self, instrument_name: str) -> InstrumentMetadata | None:
        configured = next(item for item in self.config.instruments if item.name == instrument_name)
        if configured.ticker != AUTO_DISCOVER and configured.figi != AUTO_DISCOVER:
            metadata = InstrumentMetadata(
                name=configured.name,
                display_name=configured.display_name,
                ticker=configured.ticker,
                figi=configured.figi,
                class_code="" if configured.class_code == AUTO_DISCOVER else configured.class_code,
                min_price_increment=None,
            )
            return _with_status(self._provider, metadata)

        for candidate in _queries_for(instrument_name):
            try:
                for raw in self._provider.find_instruments(candidate):
                    metadata = _metadata_from_raw(raw, configured.name, configured.display_name)
                    if metadata is not None:
                        return _with_status(self._provider, metadata)
            except Exception as exc:  # noqa: BLE001 - fallback to local catalog.
                self.storage.record_data_quality(
                    timestamp_utc=datetime.now(UTC),
                    instrument=instrument_name,
                    issue_type="instrument_discovery_error",
                    severity="warning",
                    details=mask_token_like_text(str(exc)),
                )

        metadata = _metadata_from_local_catalog(instrument_name)
        if metadata is None:
            self.storage.record_data_quality(
                timestamp_utc=datetime.now(UTC),
                instrument=instrument_name,
                issue_type="instrument_not_found",
                severity="error",
                details={"queries": list(_queries_for(instrument_name))},
            )
        return metadata


def _with_status(
    provider: ReadOnlyMarketDataProvider | None,
    metadata: InstrumentMetadata,
) -> InstrumentMetadata:
    if provider is None:
        return metadata
    try:
        status = provider.get_trading_status(metadata.instrument_id)
    except Exception:
        return metadata
    return InstrumentMetadata(
        name=metadata.name,
        display_name=metadata.display_name,
        ticker=metadata.ticker,
        figi=metadata.figi,
        class_code=metadata.class_code,
        lot=metadata.lot,
        min_price_increment=metadata.min_price_increment,
        trading_status=_first_str(status, "tradingStatus", "trading_status"),
        currency=metadata.currency,
        exchange=metadata.exchange,
        instrument_type=metadata.instrument_type,
        uid=metadata.uid,
    )


def _queries_for(instrument_name: str) -> tuple[str, ...]:
    if instrument_name == "neobitcoin":
        return (
            "BTCUSDperpA",
            "NEOBITOK",
            "Neo Bitcoin",
            "Необиткоин",
            "bitcoin",
            "neo bitcoin",
        )
    if instrument_name == "neoether":
        return (
            "ETHUSDperpA",
            "NEOEFIR",
            "Neo Ethereum",
            "Неоэфир",
            "ethereum",
            "neo ethereum",
        )
    return (instrument_name,)


def _metadata_from_raw(
    raw: Mapping[str, Any],
    name: str,
    display_name: str,
) -> InstrumentMetadata | None:
    ticker = _first_str(raw, "ticker")
    figi = _first_str(raw, "figi")
    uid = _first_str(raw, "uid", "instrumentUid", "instrument_uid")
    if ticker is None or (figi is None and uid is None):
        return None
    local = _local_catalog_item_for_ticker(ticker)
    return InstrumentMetadata(
        name=name,
        display_name=display_name,
        ticker=ticker,
        figi=figi or str(local.get("figi", "")),
        class_code=_first_str(raw, "classCode", "class_code")
        or str(local.get("class_code", "")),
        lot=_int_or_default(raw.get("lot", local.get("lot")), 1),
        min_price_increment=_price_increment(raw)
        or decimal_or_none(local.get("price_increment")),
        trading_status=_first_str(raw, "tradingStatus", "trading_status"),
        currency=_first_str(raw, "currency") or _optional_str(local.get("currency")),
        exchange=_first_str(raw, "exchange") or _optional_str(local.get("exchange")),
        instrument_type=_first_str(raw, "instrumentType", "instrument_type")
        or _optional_str(local.get("instrument_type")),
        uid=uid or _optional_str(local.get("uid")),
    )


def _metadata_from_local_catalog(instrument_name: str) -> InstrumentMetadata | None:
    expected = "BTCUSDperpA" if instrument_name == "neobitcoin" else "ETHUSDperpA"
    item = _local_catalog_item_for_ticker(expected)
    if not item:
        return None
    display = "Необиткоин" if instrument_name == "neobitcoin" else "Неоэфир"
    return InstrumentMetadata(
        name=instrument_name,
        display_name=display,
        ticker=str(item.get("ticker", "")),
        figi=str(item.get("figi", "")),
        class_code=str(item.get("class_code", "")),
        lot=_int_or_default(item.get("lot"), 1),
        min_price_increment=decimal_or_none(item.get("price_increment")),
        trading_status=None,
        currency=_optional_str(item.get("currency")),
        exchange=_optional_str(item.get("exchange")),
        instrument_type=_optional_str(item.get("instrument_type")),
        uid=_optional_str(item.get("uid")),
    )


def _local_catalog_item_for_ticker(ticker: str) -> Mapping[str, Any]:
    catalog_path = Path("configs/neoassets_universe.yaml")
    if not catalog_path.exists():
        return {}
    loaded = yaml.safe_load(catalog_path.read_text(encoding="utf-8", errors="ignore"))
    instruments = loaded.get("instruments", []) if isinstance(loaded, Mapping) else []
    for item in instruments:
        if not isinstance(item, Mapping) or item.get("ticker") != ticker:
            continue
        return item
    return {}


def _snapshot_from_orderbook(
    raw: Mapping[str, Any],
    metadata: InstrumentMetadata,
    *,
    timestamp_utc: datetime | None = None,
) -> MarketSnapshot:
    bids = tuple(_levels(raw.get("bids", ()), reverse=True))
    asks = tuple(_levels(raw.get("asks", ()), reverse=False))
    last_price = _price(raw.get("lastPrice", raw.get("last_price")))
    timestamp = timestamp_utc or datetime.now(UTC)
    if not last_price and bids and asks:
        last_price = (bids[0].price + asks[0].price) / Decimal("2")
    return MarketSnapshot(
        timestamp_utc=timestamp,
        instrument=metadata.name,
        metadata=metadata,
        last_price=last_price,
        bid_levels=bids,
        ask_levels=asks,
        exchange_timestamp=_timestamp(raw.get("time", raw.get("exchangeTimestamp"))),
        orderbook_missing=not bids or not asks,
        raw=dict(raw),
    )


def _candle_requests(config: NeoSwarmScalperConfig) -> tuple[tuple[str, bool, str], ...]:
    return (
        ("1m", config.data.collect_candles_1m, "CANDLE_INTERVAL_1_MIN"),
        ("5m", config.data.collect_candles_5m, "CANDLE_INTERVAL_5_MIN"),
        ("15m", config.data.collect_candles_15m, "CANDLE_INTERVAL_15_MIN"),
    )


def _trade_from_raw(
    raw: Mapping[str, Any],
    fallback_timestamp: datetime,
) -> dict[str, Any] | None:
    price = _price(raw.get("price", raw.get("lastPrice", raw.get("last_price"))))
    if price is None:
        return None
    quantity = decimal_or_none(raw.get("quantity", raw.get("qty", raw.get("size", 1))))
    timestamp = _timestamp(
        raw.get(
            "time",
            raw.get("timestamp", raw.get("exchangeTimestamp", raw.get("exchange_timestamp"))),
        )
    )
    return {
        "timestamp": timestamp or fallback_timestamp,
        "price": price,
        "quantity": quantity or Decimal("1"),
        "side": _first_str(raw, "side", "direction", "tradeDirection"),
    }


def _candle_from_raw(
    raw: Mapping[str, Any],
    fallback_timestamp: datetime,
) -> dict[str, Any] | None:
    timestamp = _timestamp(raw.get("time", raw.get("timestamp"))) or fallback_timestamp
    open_price = _price(raw.get("open", raw.get("o")))
    high = _price(raw.get("high", raw.get("h")))
    low = _price(raw.get("low", raw.get("l")))
    close = _price(raw.get("close", raw.get("c")))
    volume = decimal_or_none(raw.get("volume", raw.get("v", 0)))
    if None in {timestamp, open_price, high, low, close, volume}:
        return None
    return {
        "timestamp": timestamp,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "raw": dict(raw),
    }


def _levels(raw_levels: object, *, reverse: bool) -> Iterable[BookLevel]:
    if not isinstance(raw_levels, Sequence) or isinstance(raw_levels, str | bytes | bytearray):
        return ()
    levels: list[BookLevel] = []
    for raw in raw_levels:
        if isinstance(raw, Mapping):
            price = _price(raw.get("price"))
            qty = decimal_or_none(raw.get("quantity", raw.get("qty", raw.get("size"))))
            if price is not None and qty is not None:
                levels.append(BookLevel(price=price, quantity=qty))
        elif isinstance(raw, Sequence) and len(raw) >= 2:
            price = decimal_or_none(raw[0])
            qty = decimal_or_none(raw[1])
            if price is not None and qty is not None:
                levels.append(BookLevel(price=price, quantity=qty))
    return tuple(sorted(levels, key=lambda level: level.price, reverse=reverse))


def _price(value: object) -> Decimal | None:
    if isinstance(value, Mapping):
        units = int(value.get("units", 0))
        nano = int(value.get("nano", 0))
        return Decimal(units) + (Decimal(nano) / Decimal("1000000000"))
    return decimal_or_none(value)


def _price_increment(raw: Mapping[str, Any]) -> Decimal | None:
    for key in ("minPriceIncrement", "min_price_increment", "price_increment"):
        value = raw.get(key)
        if value is not None:
            return _price(value)
    return None


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _first_str(raw: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = raw.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _int_or_default(value: object, default: int) -> int:
    if value is None:
        return default
    try:
        return int(str(value))
    except ValueError:
        return default


__all__ = [
    "FeedStatus",
    "NeoMarketDataFeed",
    "ReadOnlyMarketDataProvider",
    "TBankReadOnlyProvider",
]
