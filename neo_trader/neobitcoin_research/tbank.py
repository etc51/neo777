"""Strictly read-only T-Invest integration for Neobitcoin research.

Only instrument discovery and public market-data RPCs are reachable.  The raw
SDK client and HTTP client remain private implementation details, and every RPC
path is checked against the immutable allowlist immediately before transport.
"""

from __future__ import annotations

import asyncio
import base64
import importlib
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, fields, is_dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from types import ModuleType
from typing import Any, Final, Literal, Protocol, TypeAlias, cast

import httpx

from neo_trader.neobitcoin_research.safety import (
    TBankToken,
    load_tbank_token,
    require_allowed_rpc_path,
)

JsonValue: TypeAlias = str | int | float | bool | None | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject: TypeAlias = dict[str, JsonValue]
AsyncSleep: TypeAlias = Callable[[float], Awaitable[None]]

REST_BASE_URL: Final = "https://invest-public-api.tbank.ru/rest"
APP_NAME: Final = "neo_trader_neobitcoin_research"
RPC_NAMESPACE: Final = "tinkoff.public.invest.api.contract.v1."
FIND_INSTRUMENT_RPC: Final = f"/{RPC_NAMESPACE}InstrumentsService/FindInstrument"
GET_INSTRUMENT_BY_RPC: Final = f"/{RPC_NAMESPACE}InstrumentsService/GetInstrumentBy"
TRADING_SCHEDULES_RPC: Final = f"/{RPC_NAMESPACE}InstrumentsService/TradingSchedules"
GET_TRADING_STATUS_RPC: Final = f"/{RPC_NAMESPACE}MarketDataService/GetTradingStatus"
GET_CANDLES_RPC: Final = f"/{RPC_NAMESPACE}MarketDataService/GetCandles"
MARKET_DATA_STREAM_RPC: Final = f"/{RPC_NAMESPACE}MarketDataStreamService/MarketDataStream"

NEOBITCOIN_QUERY: Final = "Neo Bitcoin"
NEOBITCOIN_TICKER: Final = "BTCUSDperpA"
NEOBITCOIN_CLASS_CODE: Final = "SPBDMFUT"
NEOBITCOIN_NAMES: Final = frozenset({"neobitcoin", "необиткоин"})
ORDERBOOK_DEPTH: Final = 50
CANDLE_INTERVALS: Final = (
    "SUBSCRIPTION_INTERVAL_ONE_MINUTE",
    "SUBSCRIPTION_INTERVAL_FIVE_MINUTES",
    "SUBSCRIPTION_INTERVAL_FIFTEEN_MINUTES",
)


class TBankResearchError(RuntimeError):
    """Base class for safe research-integration failures."""


class TBankTransportError(TBankResearchError):
    """A permitted read-only transport call failed."""


class TBankInstrumentDiscoveryError(TBankResearchError):
    """Neobitcoin could not be matched and verified deterministically."""


class TBankSdkUnavailableError(TBankResearchError):
    """The official T-Invest SDK is unavailable at stream startup."""


class TBankSubscriptionRejectedError(TBankResearchError):
    """T-Invest rejected a required market-data subscription."""


class UnaryTransport(Protocol):
    """Narrow unary transport used for fake-only tests and readonly REST."""

    def call(self, rpc_path: str, payload: Mapping[str, object]) -> object:
        """Call one exact RPC and return its decoded response."""

    def close(self) -> None:
        """Release transport resources."""


class StreamTransport(Protocol):
    """Narrow bidirectional stream transport; no raw SDK client is exposed."""

    def stream(
        self,
        rpc_path: str,
        requests: Sequence[object],
    ) -> AsyncIterator[object]:
        """Yield official SDK response messages."""


@dataclass(frozen=True, slots=True)
class TBankInstrumentMetadata:
    """Verified Neobitcoin metadata keyed by T-Invest instrument UID."""

    instrument_uid: str
    ticker: str
    class_code: str
    figi: str | None
    position_uid: str | None
    name: str
    instrument_type: str | None
    exchange: str | None
    currency: str | None
    lot: int
    min_price_increment: Decimal | None
    api_trade_available: bool
    buy_available: bool
    sell_available: bool
    trading_status: str | None
    limit_order_available: bool
    market_order_available: bool
    trading_schedules: tuple[JsonObject, ...]


StreamEventType: TypeAlias = Literal[
    "subscription_ack",
    "ping",
    "orderbook",
    "trade",
    "last_price",
    "trading_status",
    "candle",
    "open_interest",
    "unknown",
]


@dataclass(frozen=True, slots=True)
class TBankStreamRecord:
    """Lossless JSON-safe stream envelope with local timing and ACK identity."""

    event_type: StreamEventType
    subscription_kind: str | None
    received_at: datetime
    received_monotonic_ns: int
    exchange_timestamp: datetime | None
    instrument_uid: str | None
    ticker: str | None
    class_code: str | None
    stream_id: str | None
    subscription_id: str | None
    subscription_status: str | None
    is_consistent: bool | None
    payload: JsonObject


@dataclass(frozen=True, slots=True)
class _SubscriptionIdentity:
    stream_id: str | None
    subscription_id: str | None
    status: str | None
    ticker: str | None
    class_code: str | None


class _ReadonlyRestTransport:
    """Private HTTP transport limited by the same exact RPC allowlist."""

    def __init__(
        self,
        token: TBankToken,
        *,
        base_url: str = REST_BASE_URL,
        timeout_seconds: float = 15.0,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.__token = token
        self.__base_url = base_url.rstrip("/")
        self.__http_client = http_client or httpx.Client(timeout=timeout_seconds)
        self.__owns_client = http_client is None

    def call(self, rpc_path: str, payload: Mapping[str, object]) -> object:
        canonical = require_allowed_rpc_path(rpc_path)
        try:
            response = self.__http_client.post(
                f"{self.__base_url}{canonical}",
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self.__token.get_secret_value()}",
                    "Content-Type": "application/json",
                    "x-app-name": APP_NAME,
                },
                json=dict(payload),
            )
        except httpx.HTTPError as exc:
            raise TBankTransportError("Readonly T-Invest REST request failed.") from exc
        if response.is_error:
            raise TBankTransportError(
                f"Readonly T-Invest REST returned HTTP {response.status_code}."
            )
        try:
            decoded = response.json()
        except ValueError as exc:
            raise TBankTransportError("Readonly T-Invest REST returned invalid JSON.") from exc
        if not isinstance(decoded, Mapping):
            raise TBankTransportError("Readonly T-Invest REST returned an invalid payload.")
        return dict(decoded)

    def close(self) -> None:
        if self.__owns_client:
            self.__http_client.close()


class _SdkStreamTransport:
    """Private official-SDK stream adapter created lazily at runtime."""

    def __init__(
        self,
        token: TBankToken,
        sdk_module: ModuleType | object | None = None,
        *,
        subscription_check_seconds: float = 60.0,
    ) -> None:
        if subscription_check_seconds <= 0:
            raise ValueError("subscription_check_seconds must be positive.")
        self.__token = token
        self.__sdk_module = sdk_module
        self.__subscription_check_seconds = subscription_check_seconds

    async def stream(
        self,
        rpc_path: str,
        requests: Sequence[object],
    ) -> AsyncIterator[object]:
        require_allowed_rpc_path(rpc_path)
        sdk = cast(Any, self.__sdk_module or _load_official_sdk())
        async_client_type = sdk.AsyncClient
        async with async_client_type(self.__token.get_secret_value()) as client:
            stream_service = client.market_data_stream
            stream_method = stream_service.market_data_stream
            iterator = market_data_request_iterator(
                sdk,
                requests,
                subscription_check_seconds=self.__subscription_check_seconds,
            )
            async for response in stream_method(iterator):
                yield response


class TBankMarketDataDecoder:
    """Stateful decoder that propagates ACK stream/subscription IDs to events."""

    _ACK_SPECS: Final = (
        (
            "subscribe_order_book_response",
            "order_book_subscriptions",
            "orderbook",
        ),
        ("subscribe_trades_response", "trade_subscriptions", "trade"),
        (
            "subscribe_last_price_response",
            "last_price_subscriptions",
            "last_price",
        ),
        ("subscribe_info_response", "info_subscriptions", "trading_status"),
        ("subscribe_candles_response", "candles_subscriptions", "candle"),
    )
    _EVENT_SPECS: Final = (
        ("orderbook", "order_book", "orderbook"),
        ("trade", "trade", "trade"),
        ("last_price", "lastPrice", "last_price"),
        ("trading_status", "tradingStatus", "trading_status"),
        ("candle", "candle", "candle"),
        ("open_interest", "openInterest", "open_interest"),
    )

    def __init__(self) -> None:
        self._subscriptions: dict[tuple[str, str, str], _SubscriptionIdentity] = {}

    def decode(
        self,
        response: object,
        *,
        received_at: datetime | None = None,
        received_monotonic_ns: int | None = None,
    ) -> tuple[TBankStreamRecord, ...]:
        wall_time = _as_utc(received_at or datetime.now(UTC))
        monotonic_ns = received_monotonic_ns or time.monotonic_ns()
        full_payload = to_json_safe_object(response)
        records: list[TBankStreamRecord] = []

        for response_name, subscriptions_name, kind in self._ACK_SPECS:
            ack = _present_field(response, response_name, _snake_to_camel(response_name))
            if ack is None:
                continue
            subscriptions = _sequence_field(
                ack,
                subscriptions_name,
                _snake_to_camel(subscriptions_name),
            )
            tracking_stream = _optional_text(_field(ack, "stream_id", "streamId"))
            if not subscriptions:
                records.append(
                    TBankStreamRecord(
                        event_type="subscription_ack",
                        subscription_kind=kind,
                        received_at=wall_time,
                        received_monotonic_ns=monotonic_ns,
                        exchange_timestamp=None,
                        instrument_uid=None,
                        ticker=None,
                        class_code=None,
                        stream_id=tracking_stream,
                        subscription_id=None,
                        subscription_status=None,
                        is_consistent=None,
                        payload=full_payload,
                    )
                )
                continue
            for subscription in subscriptions:
                subscription_payload = to_json_safe_object(subscription)
                uid = _optional_text(_field(subscription, "instrument_uid", "instrumentUid", "uid"))
                interval = (
                    _enum_text(_field(subscription_payload, "interval"))
                    or _enum_text(_field(subscription, "interval"))
                    or ""
                )
                identity = _SubscriptionIdentity(
                    stream_id=_optional_text(_field(subscription, "stream_id", "streamId"))
                    or tracking_stream,
                    subscription_id=_optional_text(
                        _field(subscription, "subscription_id", "subscriptionId")
                    ),
                    status=(
                        _enum_text(
                            _field(
                                subscription_payload,
                                "subscription_status",
                                "subscriptionStatus",
                            )
                        )
                        or _enum_text(
                            _field(subscription, "subscription_status", "subscriptionStatus")
                        )
                    ),
                    ticker=_optional_text(_field(subscription, "ticker")),
                    class_code=_optional_text(_field(subscription, "class_code", "classCode")),
                )
                if uid:
                    self._subscriptions[(kind, uid, interval)] = identity
                records.append(
                    TBankStreamRecord(
                        event_type="subscription_ack",
                        subscription_kind=kind,
                        received_at=wall_time,
                        received_monotonic_ns=monotonic_ns,
                        exchange_timestamp=None,
                        instrument_uid=uid,
                        ticker=identity.ticker,
                        class_code=identity.class_code,
                        stream_id=identity.stream_id,
                        subscription_id=identity.subscription_id,
                        subscription_status=identity.status,
                        is_consistent=None,
                        payload=full_payload,
                    )
                )

        ping = _present_field(response, "ping")
        if ping is not None:
            records.append(
                TBankStreamRecord(
                    event_type="ping",
                    subscription_kind=None,
                    received_at=wall_time,
                    received_monotonic_ns=monotonic_ns,
                    exchange_timestamp=_event_timestamp(ping),
                    instrument_uid=None,
                    ticker=None,
                    class_code=None,
                    stream_id=_optional_text(_field(ping, "stream_id", "streamId")),
                    subscription_id=None,
                    subscription_status=None,
                    is_consistent=None,
                    payload=full_payload,
                )
            )

        for primary_name, alternate_name, kind in self._EVENT_SPECS:
            event = _present_field(response, primary_name, alternate_name)
            if event is None:
                continue
            uid = _optional_text(
                _field(event, "instrument_uid", "instrumentUid", "instrument_id", "instrumentId")
            )
            interval = _enum_text(_field(event, "interval")) or ""
            event_identity = self._subscription_identity(kind, uid, interval)
            records.append(
                TBankStreamRecord(
                    event_type=cast(StreamEventType, kind),
                    subscription_kind=kind,
                    received_at=wall_time,
                    received_monotonic_ns=monotonic_ns,
                    exchange_timestamp=_event_timestamp(event),
                    instrument_uid=uid,
                    ticker=(
                        _optional_text(_field(event, "ticker"))
                        or (event_identity.ticker if event_identity else None)
                    ),
                    class_code=(
                        _optional_text(_field(event, "class_code", "classCode"))
                        or (event_identity.class_code if event_identity else None)
                    ),
                    stream_id=event_identity.stream_id if event_identity else None,
                    subscription_id=(event_identity.subscription_id if event_identity else None),
                    subscription_status=event_identity.status if event_identity else None,
                    is_consistent=_optional_bool(_field(event, "is_consistent", "isConsistent")),
                    payload=full_payload,
                )
            )

        if not records:
            records.append(
                TBankStreamRecord(
                    event_type="unknown",
                    subscription_kind=None,
                    received_at=wall_time,
                    received_monotonic_ns=monotonic_ns,
                    exchange_timestamp=None,
                    instrument_uid=None,
                    ticker=None,
                    class_code=None,
                    stream_id=None,
                    subscription_id=None,
                    subscription_status=None,
                    is_consistent=None,
                    payload=full_payload,
                )
            )
        return tuple(records)

    def _subscription_identity(
        self,
        kind: str,
        uid: str | None,
        interval: str,
    ) -> _SubscriptionIdentity | None:
        if not uid:
            return None
        identity = self._subscriptions.get((kind, uid, interval))
        if identity is not None:
            return identity
        matches = [
            value
            for (stored_kind, stored_uid, _), value in self._subscriptions.items()
            if stored_kind == kind and stored_uid == uid
        ]
        return matches[0] if len(matches) == 1 else None


class TBankResearchClient:
    """Public narrow facade for verified discovery and market-data streaming."""

    def __init__(
        self,
        token: TBankToken,
        *,
        unary_transport: UnaryTransport | None = None,
        stream_transport: StreamTransport | None = None,
        sdk_module: ModuleType | object | None = None,
    ) -> None:
        self.__token = token
        self.__unary_transport = unary_transport or _ReadonlyRestTransport(token)
        self.__owns_unary_transport = unary_transport is None
        self.__sdk_module = sdk_module
        self.__stream_transport = stream_transport or _SdkStreamTransport(
            token,
            sdk_module=sdk_module,
        )

    @classmethod
    def from_token_file(
        cls,
        path: Path | str | None = None,
        *,
        unary_transport: UnaryTransport | None = None,
        stream_transport: StreamTransport | None = None,
        sdk_module: ModuleType | object | None = None,
    ) -> TBankResearchClient:
        """Construct from one exact token file without environment persistence."""

        return cls(
            load_tbank_token(path),
            unary_transport=unary_transport,
            stream_transport=stream_transport,
            sdk_module=sdk_module,
        )

    def close(self) -> None:
        if self.__owns_unary_transport:
            self.__unary_transport.close()

    def discover_neobitcoin(
        self,
        *,
        as_of: datetime | None = None,
    ) -> TBankInstrumentMetadata:
        """Find and verify one exact Neobitcoin UID using readonly RPCs only."""

        found = self._unary_call(
            FIND_INSTRUMENT_RPC,
            {"query": NEOBITCOIN_QUERY, "apiTradeAvailableFlag": False},
        )
        candidate = _select_exact_neobitcoin(_mapping_items(found, "instruments"))
        uid = _required_text(candidate, "uid", "instrumentUid", "instrument_uid")

        details_response = self._unary_call(
            GET_INSTRUMENT_BY_RPC,
            {"idType": "INSTRUMENT_ID_TYPE_UID", "id": uid},
        )
        details = _mapping_field(details_response, "instrument")
        verified_uid = _required_text(details, "uid", "instrumentUid", "instrument_uid")
        if verified_uid != uid:
            raise TBankInstrumentDiscoveryError("GetInstrumentBy returned a different UID.")

        ticker = _required_text(details, "ticker")
        class_code = _required_text(details, "classCode", "class_code")
        if ticker.casefold() != NEOBITCOIN_TICKER.casefold():
            raise TBankInstrumentDiscoveryError("Verified instrument ticker is not Neobitcoin.")
        if class_code.casefold() != NEOBITCOIN_CLASS_CODE.casefold():
            raise TBankInstrumentDiscoveryError("Verified instrument class code is not Neobitcoin.")

        status = self._unary_call(
            GET_TRADING_STATUS_RPC,
            {"instrumentId": uid},
        )
        exchange = _optional_text(_field(details, "exchange"))
        schedules: tuple[JsonObject, ...] = ()
        if exchange:
            timestamp = _as_utc(as_of or datetime.now(UTC))
            schedules_response = self._unary_call(
                TRADING_SCHEDULES_RPC,
                {
                    "exchange": exchange,
                    "from": timestamp.isoformat().replace("+00:00", "Z"),
                    "to": (timestamp + timedelta(days=2)).isoformat().replace("+00:00", "Z"),
                },
            )
            schedules = tuple(
                to_json_safe_object(item)
                for item in _mapping_items(schedules_response, "exchanges")
            )

        return TBankInstrumentMetadata(
            instrument_uid=uid,
            ticker=ticker,
            class_code=class_code,
            figi=_optional_text(_field(details, "figi")),
            position_uid=_optional_text(_field(details, "positionUid", "position_uid")),
            name=_required_text(details, "name"),
            instrument_type=_optional_text(
                _field(details, "instrumentType", "instrument_type", "instrumentKind")
            ),
            exchange=exchange,
            currency=_optional_text(_field(details, "currency")),
            lot=_positive_int(_field(details, "lot"), default=1),
            min_price_increment=_quotation_decimal(
                _field(details, "minPriceIncrement", "min_price_increment")
            ),
            api_trade_available=_optional_bool(
                _field(details, "apiTradeAvailableFlag", "api_trade_available_flag")
            )
            is True,
            buy_available=_optional_bool(_field(details, "buyAvailableFlag", "buy_available_flag"))
            is True,
            sell_available=_optional_bool(
                _field(details, "sellAvailableFlag", "sell_available_flag")
            )
            is True,
            trading_status=_enum_text(_field(status, "tradingStatus", "trading_status")),
            limit_order_available=_optional_bool(
                _field(status, "limitOrderAvailableFlag", "limit_order_available_flag")
            )
            is True,
            market_order_available=_optional_bool(
                _field(status, "marketOrderAvailableFlag", "market_order_available_flag")
            )
            is True,
            trading_schedules=schedules,
        )

    def get_candles(
        self,
        instrument_uid: str,
        *,
        from_time: datetime,
        to_time: datetime,
        interval: str,
    ) -> tuple[JsonObject, ...]:
        """Return historical candles through the readonly safety allowlist."""

        response = self._unary_call(
            GET_CANDLES_RPC,
            {
                "instrumentId": instrument_uid,
                "from": _as_utc(from_time).isoformat().replace("+00:00", "Z"),
                "to": _as_utc(to_time).isoformat().replace("+00:00", "Z"),
                "interval": interval,
            },
        )
        return tuple(to_json_safe_object(item) for item in _mapping_items(response, "candles"))

    async def stream_market_data(
        self,
        instrument: TBankInstrumentMetadata | str,
    ) -> AsyncIterator[TBankStreamRecord]:
        """Yield decoded readonly stream records for one verified UID."""

        uid = (
            instrument.instrument_uid
            if isinstance(instrument, TBankInstrumentMetadata)
            else instrument
        )
        uid = uid.strip()
        if not uid:
            raise ValueError("instrument UID must not be empty.")
        sdk = self.__sdk_module or _load_official_sdk()
        requests = build_bidirectional_subscriptions(sdk, uid)
        decoder = TBankMarketDataDecoder()
        canonical = require_allowed_rpc_path(MARKET_DATA_STREAM_RPC)
        async for response in self.__stream_transport.stream(canonical, requests):
            for record in decoder.decode(response):
                require_successful_subscription_ack(record)
                yield record

    def _unary_call(self, rpc_path: str, payload: Mapping[str, object]) -> Mapping[str, object]:
        canonical = require_allowed_rpc_path(rpc_path)
        response = self.__unary_transport.call(canonical, payload)
        if isinstance(response, Mapping):
            return cast(Mapping[str, object], response)
        converted = to_json_safe_object(response)
        return cast(Mapping[str, object], converted)


def build_bidirectional_subscriptions(sdk: Any, instrument_uid: str) -> tuple[object, ...]:
    """Build depth-50, trades, last-price, status, and 1m/5m/15m requests."""

    uid = instrument_uid.strip()
    if not uid:
        raise ValueError("instrument UID must not be empty.")
    action = _sdk_enum(sdk, "SubscriptionAction", "SUBSCRIPTION_ACTION_SUBSCRIBE")
    requests: list[object] = []

    orderbook_instrument = _construct_message(
        sdk.OrderBookInstrument,
        (
            {
                "instrument_id": uid,
                "depth": ORDERBOOK_DEPTH,
                "order_book_type": _sdk_enum_or_none(
                    sdk,
                    "OrderBookType",
                    "ORDERBOOK_TYPE_ALL",
                ),
            },
            {"instrument_id": uid, "depth": ORDERBOOK_DEPTH},
        ),
    )
    orderbook_request = _construct_message(
        sdk.SubscribeOrderBookRequest,
        ({"subscription_action": action, "instruments": [orderbook_instrument]},),
    )
    requests.append(sdk.MarketDataRequest(subscribe_order_book_request=orderbook_request))

    trade_instrument = sdk.TradeInstrument(instrument_id=uid)
    trade_request = _construct_message(
        sdk.SubscribeTradesRequest,
        (
            {
                "subscription_action": action,
                "instruments": [trade_instrument],
                "trade_source": _sdk_enum_or_none(
                    sdk,
                    "TradeSourceType",
                    "TRADE_SOURCE_ALL",
                ),
                "with_open_interest": True,
            },
            {"subscription_action": action, "instruments": [trade_instrument]},
        ),
    )
    requests.append(sdk.MarketDataRequest(subscribe_trades_request=trade_request))

    last_price_instrument = sdk.LastPriceInstrument(instrument_id=uid)
    last_price_request = sdk.SubscribeLastPriceRequest(
        subscription_action=action,
        instruments=[last_price_instrument],
    )
    requests.append(sdk.MarketDataRequest(subscribe_last_price_request=last_price_request))

    info_instrument = sdk.InfoInstrument(instrument_id=uid)
    info_request = sdk.SubscribeInfoRequest(
        subscription_action=action,
        instruments=[info_instrument],
    )
    requests.append(sdk.MarketDataRequest(subscribe_info_request=info_request))

    candle_instruments = [
        sdk.CandleInstrument(
            instrument_id=uid,
            interval=_sdk_enum(sdk, "SubscriptionInterval", interval),
        )
        for interval in CANDLE_INTERVALS
    ]
    candles_request = _construct_message(
        sdk.SubscribeCandlesRequest,
        (
            {
                "subscription_action": action,
                "instruments": candle_instruments,
                "waiting_close": False,
            },
            {"subscription_action": action, "instruments": candle_instruments},
        ),
    )
    requests.append(sdk.MarketDataRequest(subscribe_candles_request=candles_request))
    return tuple(requests)


def build_subscription_check_request(sdk: Any) -> object:
    """Build the periodic ``GetMySubscriptions`` stream-control request."""

    message_type = getattr(sdk, "GetMySubscriptions", None)
    if message_type is None:
        message_type = getattr(sdk, "GetMySubscriptionsRequest", None)
    if message_type is None:
        raise TBankResearchError("Official SDK has no GetMySubscriptions message type.")
    request = cast(Callable[..., object], message_type)()
    return sdk.MarketDataRequest(get_my_subscriptions=request)


async def market_data_request_iterator(
    sdk: Any,
    requests: Sequence[object],
    *,
    subscription_check_seconds: float = 60.0,
    sleep: AsyncSleep = asyncio.sleep,
) -> AsyncIterator[object]:
    """Yield initial subscriptions and periodic ``GetMySubscriptions`` checks."""

    if subscription_check_seconds <= 0:
        raise ValueError("subscription_check_seconds must be positive.")
    for request in requests:
        yield request
    subscription_check = build_subscription_check_request(sdk)
    while True:
        await sleep(subscription_check_seconds)
        yield subscription_check


def require_successful_subscription_ack(record: TBankStreamRecord) -> None:
    """Fail the stream immediately when any required subscription is rejected."""

    if record.event_type != "subscription_ack" or record.subscription_status is None:
        return
    if record.subscription_status not in {"SUBSCRIPTION_STATUS_SUCCESS", "1"}:
        raise TBankSubscriptionRejectedError(
            f"T-Invest rejected {record.subscription_kind or 'unknown'} subscription "
            f"with status {record.subscription_status}."
        )


def to_json_safe_object(value: object) -> JsonObject:
    """Convert a complete SDK/protobuf response into a JSON-safe mapping."""

    converted = _to_json_safe(value)
    if not isinstance(converted, dict):
        return {"value": converted}
    return converted


def _load_official_sdk() -> ModuleType:
    try:
        return importlib.import_module("t_tech.invest")
    except ImportError as exc:
        raise TBankSdkUnavailableError(
            "Install t-tech-investments from the official T-Bank package index."
        ) from exc


def _select_exact_neobitcoin(
    candidates: Sequence[Mapping[str, object]],
) -> Mapping[str, object]:
    ranked: list[tuple[int, str, Mapping[str, object]]] = []
    for candidate in candidates:
        uid = _optional_text(_field(candidate, "uid", "instrumentUid", "instrument_uid"))
        if not uid:
            continue
        ticker = (_optional_text(_field(candidate, "ticker")) or "").casefold()
        class_code = (_optional_text(_field(candidate, "classCode", "class_code")) or "").casefold()
        name = _normalized_name(_optional_text(_field(candidate, "name")) or "")
        expected_ticker = NEOBITCOIN_TICKER.casefold()
        expected_class = NEOBITCOIN_CLASS_CODE.casefold()
        if ticker == expected_ticker and class_code == expected_class:
            rank = 0
        elif ticker == expected_ticker and not class_code:
            rank = 1
        elif name in NEOBITCOIN_NAMES and class_code == expected_class:
            rank = 2
        else:
            continue
        ranked.append((rank, uid, candidate))

    if not ranked:
        raise TBankInstrumentDiscoveryError("Exact Neobitcoin instrument was not found.")
    best_rank = min(item[0] for item in ranked)
    best = [item for item in ranked if item[0] == best_rank]
    unique_uids = {item[1] for item in best}
    if len(unique_uids) != 1:
        raise TBankInstrumentDiscoveryError("Exact Neobitcoin match is ambiguous.")
    return best[0][2]


def _mapping_items(payload: Mapping[str, object], *names: str) -> tuple[Mapping[str, object], ...]:
    value = _field(payload, *names)
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        return ()
    return tuple(cast(Mapping[str, object], item) for item in value if isinstance(item, Mapping))


def _mapping_field(payload: Mapping[str, object], *names: str) -> Mapping[str, object]:
    value = _field(payload, *names)
    if not isinstance(value, Mapping):
        raise TBankInstrumentDiscoveryError("T-Invest response has no instrument metadata.")
    return cast(Mapping[str, object], value)


def _present_field(value: object, *names: str) -> object | None:
    if isinstance(value, Mapping):
        return _field(value, *names)
    which_oneof = getattr(value, "WhichOneof", None)
    if callable(which_oneof):
        for group in ("payload", "response"):
            try:
                selected = which_oneof(group)
            except ValueError:
                continue
            if selected is not None:
                return cast(object, getattr(value, selected)) if selected in names else None
    has_field = getattr(value, "HasField", None)
    if callable(has_field):
        for name in names:
            try:
                if has_field(name):
                    return cast(object, getattr(value, name))
            except (ValueError, TypeError):
                continue
    return _field(value, *names)


def _field(value: object, *names: str) -> object | None:
    if isinstance(value, Mapping):
        for name in names:
            if name in value and value[name] is not None:
                return cast(object, value[name])
        return None
    for name in names:
        candidate = getattr(value, name, None)
        if candidate is not None:
            return cast(object, candidate)
    return None


def _sequence_field(value: object, *names: str) -> tuple[object, ...]:
    candidate = _field(value, *names)
    if not isinstance(candidate, Sequence) or isinstance(candidate, str | bytes | bytearray):
        return ()
    return tuple(candidate)


def _required_text(value: object, *names: str) -> str:
    text = _optional_text(_field(value, *names))
    if not text:
        raise TBankInstrumentDiscoveryError(
            f"T-Invest metadata is missing required field {names[0]}."
        )
    return text


def _optional_text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_bool(value: object | None) -> bool | None:
    return value if isinstance(value, bool) else None


def _positive_int(value: object | None, *, default: int) -> int:
    if value is None or isinstance(value, bool):
        return default
    try:
        result = int(str(value))
    except ValueError:
        return default
    return result if result > 0 else default


def _quotation_decimal(value: object | None) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Mapping):
        try:
            units = Decimal(str(value.get("units", 0)))
            nano = Decimal(str(value.get("nano", 0)))
        except (InvalidOperation, ValueError):
            return None
        return units + nano / Decimal("1000000000")
    raw_units = getattr(value, "units", None)
    raw_nano = getattr(value, "nano", None)
    if raw_units is not None or raw_nano is not None:
        try:
            return Decimal(str(raw_units or 0)) + Decimal(str(raw_nano or 0)) / Decimal(
                "1000000000"
            )
        except (InvalidOperation, ValueError):
            return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _event_timestamp(value: object) -> datetime | None:
    for name in (
        "time",
        "order_book_ts",
        "orderBookTs",
        "last_trade_ts",
        "lastTradeTs",
    ):
        parsed = _datetime_value(_field(value, name))
        if parsed is not None:
            return parsed
    return None


def _datetime_value(value: object | None) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return _as_utc(parsed)
    to_datetime = getattr(value, "ToDatetime", None)
    if callable(to_datetime):
        parsed = to_datetime(tzinfo=UTC)
        return _as_utc(parsed) if isinstance(parsed, datetime) else None
    return None


def _enum_text(value: object | None) -> str | None:
    if value is None:
        return None
    name = getattr(value, "name", None)
    if isinstance(name, str):
        return name
    text = str(value).strip()
    return text or None


def _construct_message(
    message_type: object,
    alternatives: Sequence[Mapping[str, object | None]],
) -> object:
    last_error: TypeError | None = None
    for values in alternatives:
        kwargs = {key: value for key, value in values.items() if value is not None}
        try:
            return cast(Callable[..., object], message_type)(**kwargs)
        except TypeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise TBankResearchError("No SDK message constructor alternatives were provided.")


def _sdk_enum(sdk: Any, enum_name: str, member_name: str) -> object:
    enum_type = getattr(sdk, enum_name)
    return getattr(enum_type, member_name)


def _sdk_enum_or_none(sdk: Any, enum_name: str, member_name: str) -> object | None:
    enum_type = getattr(sdk, enum_name, None)
    return None if enum_type is None else getattr(enum_type, member_name, None)


def _to_json_safe(value: object) -> JsonValue:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return _as_utc(value).isoformat()
    if isinstance(value, bytes):
        return base64.b64encode(value).decode("ascii")
    if isinstance(value, Enum):
        return value.name
    if isinstance(value, Mapping):
        return {str(key): _to_json_safe(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_to_json_safe(item) for item in value]
    if hasattr(value, "DESCRIPTOR"):
        try:
            json_format = importlib.import_module("google.protobuf.json_format")
            converted = json_format.MessageToDict(
                value,
                preserving_proto_field_name=True,
                use_integers_for_enums=False,
            )
            return _to_json_safe(converted)
        except (ImportError, TypeError, ValueError):
            pass
    if is_dataclass(value) and not isinstance(value, type):
        return {item.name: _to_json_safe(getattr(value, item.name)) for item in fields(value)}
    attributes = getattr(value, "__dict__", None)
    if isinstance(attributes, Mapping):
        return {
            str(key): _to_json_safe(item)
            for key, item in attributes.items()
            if not str(key).startswith("_")
        }
    return str(value)


def _normalized_name(value: str) -> str:
    return "".join(character for character in value.casefold() if character.isalnum())


def _snake_to_camel(value: str) -> str:
    head, *tail = value.split("_")
    return head + "".join(part[:1].upper() + part[1:] for part in tail)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = [
    "CANDLE_INTERVALS",
    "FIND_INSTRUMENT_RPC",
    "GET_INSTRUMENT_BY_RPC",
    "GET_TRADING_STATUS_RPC",
    "MARKET_DATA_STREAM_RPC",
    "NEOBITCOIN_CLASS_CODE",
    "NEOBITCOIN_TICKER",
    "ORDERBOOK_DEPTH",
    "TRADING_SCHEDULES_RPC",
    "StreamTransport",
    "TBankInstrumentDiscoveryError",
    "TBankInstrumentMetadata",
    "TBankMarketDataDecoder",
    "TBankResearchClient",
    "TBankResearchError",
    "TBankSdkUnavailableError",
    "TBankSubscriptionRejectedError",
    "TBankStreamRecord",
    "TBankTransportError",
    "UnaryTransport",
    "build_bidirectional_subscriptions",
    "build_subscription_check_request",
    "market_data_request_iterator",
    "require_successful_subscription_ack",
    "to_json_safe_object",
]
