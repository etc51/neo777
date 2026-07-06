"""Read-only T-Bank Invest REST client.

The client intentionally exposes market-data and account-read methods only.
It does not implement order placement or cancellation.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum
from typing import Any, Final, Literal, Protocol, TypeAlias, TypeGuard, cast

import httpx
from pydantic import SecretStr

from neo_trader.config import Settings, get_settings

TBankModeValue: TypeAlias = Literal["readonly", "sandbox", "live"]
JsonMapping: TypeAlias = Mapping[str, Any]
SleepFunc: TypeAlias = Callable[[float], None]

PROD_REST_BASE_URL: Final = "https://invest-public-api.tbank.ru/rest"
SANDBOX_REST_BASE_URL: Final = "https://sandbox-invest-public-api.tbank.ru/rest"
APP_NAME: Final = "neo_trader"
NANO_FACTOR: Final = Decimal("1000000000")
TEMPORARY_STATUS_CODES: Final = frozenset({408, 425, 429, 500, 502, 503, 504})

USERS_GET_ACCOUNTS: Final = "tinkoff.public.invest.api.contract.v1.UsersService/GetAccounts"
INSTRUMENTS_FIND: Final = "tinkoff.public.invest.api.contract.v1.InstrumentsService/FindInstrument"
MARKETDATA_GET_TRADING_STATUS: Final = (
    "tinkoff.public.invest.api.contract.v1.MarketDataService/GetTradingStatus"
)
MARKETDATA_GET_TRADING_STATUSES: Final = (
    "tinkoff.public.invest.api.contract.v1.MarketDataService/GetTradingStatuses"
)
MARKETDATA_GET_ORDER_BOOK: Final = (
    "tinkoff.public.invest.api.contract.v1.MarketDataService/GetOrderBook"
)

READONLY_METHODS: Final = frozenset(
    {
        USERS_GET_ACCOUNTS,
        INSTRUMENTS_FIND,
        MARKETDATA_GET_TRADING_STATUS,
        MARKETDATA_GET_TRADING_STATUSES,
        MARKETDATA_GET_ORDER_BOOK,
    }
)

RETRYABLE_EXCEPTIONS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.NetworkError,
    httpx.PoolTimeout,
    httpx.ReadError,
    httpx.ReadTimeout,
    httpx.RemoteProtocolError,
    httpx.WriteError,
    httpx.WriteTimeout,
)


class QuotationObject(Protocol):
    """Minimal protocol matching T-Invest Quotation-like objects."""

    units: int
    nano: int


QuotationInput: TypeAlias = Mapping[str, object] | QuotationObject


class TBankMode(StrEnum):
    """Client connection mode."""

    READONLY = "readonly"
    SANDBOX = "sandbox"
    LIVE = "live"


class TBankClientError(Exception):
    """Base class for T-Bank client failures."""


class TBankConfigurationError(TBankClientError):
    """Invalid local configuration."""


class TBankTransportError(TBankClientError):
    """Network-level or timeout error."""

    def __init__(
        self,
        message: str,
        *,
        temporary: bool,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.temporary = temporary
        self.cause = cause


class TBankAPIError(TBankClientError):
    """T-Bank API returned an error response."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        code: str | None = None,
        tracking_id: str | None = None,
        temporary: bool = False,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.tracking_id = tracking_id
        self.temporary = temporary

    @classmethod
    def from_response(cls, response: httpx.Response, *, temporary: bool) -> TBankAPIError:
        payload = _response_payload(response)
        message = _payload_str(payload, "message") or response.reason_phrase
        code = _payload_str(payload, "code")
        tracking_id = response.headers.get("x-tracking-id")
        return cls(
            message,
            status_code=response.status_code,
            code=code,
            tracking_id=tracking_id,
            temporary=temporary,
        )


class TBankResponseError(TBankClientError):
    """Unexpected or malformed API response."""


@dataclass(frozen=True)
class TBankAccount:
    """Brokerage account returned by UsersService/GetAccounts."""

    id: str
    name: str | None
    type: str | None
    status: str | None
    access_level: str | None
    raw: JsonMapping

    @classmethod
    def from_payload(cls, payload: JsonMapping) -> TBankAccount:
        return cls(
            id=_payload_required_str(payload, "id"),
            name=_payload_str(payload, "name"),
            type=_payload_str(payload, "type"),
            status=_payload_str(payload, "status"),
            access_level=_payload_str(payload, "accessLevel", "access_level"),
            raw=dict(payload),
        )


@dataclass(frozen=True)
class TBankInstrument:
    """Short instrument data returned by InstrumentsService/FindInstrument."""

    requested_ticker: str
    ticker: str
    class_code: str | None
    figi: str | None
    uid: str | None
    name: str | None
    instrument_type: str | None
    api_trade_available_flag: bool
    raw: JsonMapping

    @classmethod
    def from_payload(cls, payload: JsonMapping, *, requested_ticker: str) -> TBankInstrument:
        return cls(
            requested_ticker=requested_ticker,
            ticker=_payload_required_str(payload, "ticker"),
            class_code=_payload_str(payload, "classCode", "class_code"),
            figi=_payload_str(payload, "figi"),
            uid=_payload_str(payload, "uid", "instrumentUid", "instrument_uid"),
            name=_payload_str(payload, "name"),
            instrument_type=_payload_str(payload, "instrumentType", "instrument_type"),
            api_trade_available_flag=_payload_bool(
                payload,
                "apiTradeAvailableFlag",
                "api_trade_available_flag",
            ),
            raw=dict(payload),
        )


@dataclass(frozen=True)
class TBankTradingStatus:
    """MarketDataService trading status for one instrument."""

    figi: str | None
    instrument_uid: str | None
    trading_status: str | None
    limit_order_available_flag: bool
    market_order_available_flag: bool
    api_trade_available_flag: bool
    raw: JsonMapping

    @classmethod
    def from_payload(cls, payload: JsonMapping) -> TBankTradingStatus:
        return cls(
            figi=_payload_str(payload, "figi"),
            instrument_uid=_payload_str(payload, "instrumentUid", "instrument_uid"),
            trading_status=_payload_str(payload, "tradingStatus", "trading_status"),
            limit_order_available_flag=_payload_bool(
                payload,
                "limitOrderAvailableFlag",
                "limit_order_available_flag",
            ),
            market_order_available_flag=_payload_bool(
                payload,
                "marketOrderAvailableFlag",
                "market_order_available_flag",
            ),
            api_trade_available_flag=_payload_bool(
                payload,
                "apiTradeAvailableFlag",
                "api_trade_available_flag",
            ),
            raw=dict(payload),
        )


@dataclass(frozen=True)
class TBankOrderBookLevel:
    """One price level in an order book."""

    price: Decimal
    quantity: int
    raw: JsonMapping

    @classmethod
    def from_payload(cls, payload: JsonMapping) -> TBankOrderBookLevel:
        price_payload = payload.get("price")
        if not _is_quotation_payload(price_payload):
            raise TBankResponseError("Order book level has no valid price quotation.")
        return cls(
            price=quotation_to_decimal(price_payload),
            quantity=_payload_int(payload, "quantity"),
            raw=dict(payload),
        )


@dataclass(frozen=True)
class TBankOrderBookSnapshot:
    """MarketDataService/GetOrderBook snapshot."""

    figi: str | None
    instrument_uid: str | None
    depth: int
    bids: tuple[TBankOrderBookLevel, ...]
    asks: tuple[TBankOrderBookLevel, ...]
    last_price: Decimal | None
    close_price: Decimal | None
    limit_up: Decimal | None
    limit_down: Decimal | None
    raw: JsonMapping

    @classmethod
    def from_payload(cls, payload: JsonMapping) -> TBankOrderBookSnapshot:
        return cls(
            figi=_payload_str(payload, "figi"),
            instrument_uid=_payload_str(payload, "instrumentUid", "instrument_uid"),
            depth=_payload_int(payload, "depth"),
            bids=tuple(
                TBankOrderBookLevel.from_payload(item) for item in _payload_items(payload, "bids")
            ),
            asks=tuple(
                TBankOrderBookLevel.from_payload(item) for item in _payload_items(payload, "asks")
            ),
            last_price=_payload_decimal(payload, "lastPrice", "last_price"),
            close_price=_payload_decimal(payload, "closePrice", "close_price"),
            limit_up=_payload_decimal(payload, "limitUp", "limit_up"),
            limit_down=_payload_decimal(payload, "limitDown", "limit_down"),
            raw=dict(payload),
        )


def quotation_to_decimal(value: QuotationInput) -> Decimal:
    """Convert a T-Invest Quotation-like value to Decimal."""

    if isinstance(value, Mapping):
        units = _int_from_object(value.get("units", 0), field_name="units")
        nano = _int_from_object(value.get("nano", 0), field_name="nano")
    else:
        units = _int_from_object(value.units, field_name="units")
        nano = _int_from_object(value.nano, field_name="nano")

    return Decimal(units) + (Decimal(nano) / NANO_FACTOR)


class TBankClient:
    """Small sync REST client for read-only T-Bank Invest API calls."""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        mode: TBankMode | TBankModeValue | None = None,
        token: SecretStr | str | None = None,
        base_url: str | None = None,
        http_client: httpx.Client | None = None,
        timeout_seconds: float | None = None,
        max_retries: int | None = None,
        backoff_seconds: float | None = None,
        sleep: SleepFunc = time.sleep,
    ) -> None:
        resolved_settings = settings or get_settings()
        self.mode = _coerce_mode(mode or resolved_settings.tbank_mode)
        self.base_url = _resolve_base_url(
            mode=self.mode,
            base_url=base_url or resolved_settings.tbank_base_url,
        )
        self.timeout_seconds: float = timeout_seconds or resolved_settings.tbank_timeout_seconds
        self.max_retries: int = (
            max_retries if max_retries is not None else resolved_settings.tbank_max_retries
        )
        self.backoff_seconds: float = (
            backoff_seconds
            if backoff_seconds is not None
            else resolved_settings.tbank_backoff_seconds
        )
        self._token = _coerce_token(token if token is not None else resolved_settings.tbank_token)
        self._sleep = sleep
        self._client = http_client or httpx.Client(timeout=self.timeout_seconds)
        self._owns_client = http_client is None

    def __enter__(self) -> TBankClient:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the underlying HTTP client when owned by this instance."""

        if self._owns_client:
            self._client.close()

    def get_accounts(self, *, status: str | None = None) -> list[TBankAccount]:
        """Return brokerage accounts visible to the configured token."""

        payload: dict[str, Any] = {}
        if status is not None:
            payload["status"] = status
        response = self._post(USERS_GET_ACCOUNTS, payload)
        return [TBankAccount.from_payload(item) for item in _payload_items(response, "accounts")]

    def get_instruments(
        self,
        tickers: Sequence[str],
        *,
        api_trade_available_only: bool = True,
        instrument_kind: str | None = None,
    ) -> dict[str, list[TBankInstrument]]:
        """Find API-tradable instruments for each ticker."""

        result: dict[str, list[TBankInstrument]] = {}
        for ticker in tickers:
            normalized = _normalize_identifier(ticker, field_name="ticker").upper()
            payload: dict[str, Any] = {
                "query": normalized,
                "apiTradeAvailableFlag": api_trade_available_only,
            }
            if instrument_kind is not None:
                payload["instrumentKind"] = instrument_kind

            response = self._post(INSTRUMENTS_FIND, payload)
            instruments = [
                TBankInstrument.from_payload(item, requested_ticker=normalized)
                for item in _payload_items(response, "instruments")
            ]
            result[normalized] = [
                instrument
                for instrument in instruments
                if instrument.ticker.upper() == normalized
            ]

        return result

    def get_trading_status(self, instrument_id: str) -> TBankTradingStatus:
        """Return current trading status for a FIGI, UID, or ticker_class_code id."""

        response = self._post(
            MARKETDATA_GET_TRADING_STATUS,
            {"instrumentId": _normalize_identifier(instrument_id, field_name="instrument_id")},
        )
        return TBankTradingStatus.from_payload(response)

    def get_trading_statuses(self, instrument_ids: Sequence[str]) -> list[TBankTradingStatus]:
        """Return current trading status for several instrument ids."""

        normalized_ids = [
            _normalize_identifier(instrument_id, field_name="instrument_id")
            for instrument_id in instrument_ids
        ]
        response = self._post(
            MARKETDATA_GET_TRADING_STATUSES,
            {"instrumentId": normalized_ids},
        )
        return [
            TBankTradingStatus.from_payload(item)
            for item in _payload_items(response, "tradingStatuses", "trading_statuses")
        ]

    def get_orderbook_snapshot(
        self,
        instrument_id: str,
        *,
        depth: int = 10,
        order_book_type: str | None = None,
    ) -> TBankOrderBookSnapshot:
        """Return an order book snapshot for a FIGI, UID, or ticker_class_code id."""

        if depth <= 0:
            raise ValueError("depth must be positive.")

        payload: dict[str, Any] = {
            "instrumentId": _normalize_identifier(instrument_id, field_name="instrument_id"),
            "depth": depth,
        }
        if order_book_type is not None:
            payload["orderBookType"] = order_book_type

        response = self._post(MARKETDATA_GET_ORDER_BOOK, payload)
        return TBankOrderBookSnapshot.from_payload(response)

    def _post(self, service_method: str, payload: JsonMapping) -> dict[str, Any]:
        if self.mode is TBankMode.READONLY and service_method not in READONLY_METHODS:
            raise TBankConfigurationError(
                f"Method is not allowed in readonly mode: {service_method}"
            )

        url = f"{self.base_url}/{service_method}"
        last_error: TBankClientError | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self._client.post(url, headers=self._headers(), json=dict(payload))
            except RETRYABLE_EXCEPTIONS as exc:
                last_error = TBankTransportError(
                    "Temporary transport error while calling T-Bank API.",
                    temporary=True,
                    cause=exc,
                )
                if self._should_retry(attempt):
                    self._sleep(self._retry_delay(attempt))
                    continue
                raise last_error from exc

            if response.status_code in TEMPORARY_STATUS_CODES:
                last_error = TBankAPIError.from_response(response, temporary=True)
                if self._should_retry(attempt):
                    self._sleep(self._retry_delay(attempt, response=response))
                    continue
                raise last_error

            if response.is_error:
                raise TBankAPIError.from_response(response, temporary=False)

            return _decode_json_response(response)

        if last_error is not None:
            raise last_error
        raise TBankTransportError("T-Bank API request failed before it was sent.", temporary=True)

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "x-app-name": APP_NAME,
        }

    def _should_retry(self, attempt: int) -> bool:
        return attempt < self.max_retries

    def _retry_delay(self, attempt: int, *, response: httpx.Response | None = None) -> float:
        retry_after = response.headers.get("retry-after") if response is not None else None
        if retry_after is not None:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                pass
        return float(self.backoff_seconds * (2**attempt))


def _coerce_mode(value: TBankMode | TBankModeValue | str) -> TBankMode:
    try:
        return TBankMode(value)
    except ValueError as exc:
        allowed = ", ".join(mode.value for mode in TBankMode)
        raise TBankConfigurationError(
            f"Unsupported T-Bank mode: {value!r}. Allowed: {allowed}."
        ) from exc


def _coerce_token(value: SecretStr | str | None) -> str:
    if value is None:
        raise TBankConfigurationError("NEO_TRADER_TBANK_TOKEN is required for TBankClient.")

    token = value.get_secret_value() if isinstance(value, SecretStr) else value
    token = token.strip()
    if not token:
        raise TBankConfigurationError("NEO_TRADER_TBANK_TOKEN must not be empty.")
    return token


def _resolve_base_url(*, mode: TBankMode, base_url: str | None) -> str:
    if base_url is not None and base_url.strip():
        return base_url.rstrip("/")
    if mode is TBankMode.SANDBOX:
        return SANDBOX_REST_BASE_URL
    return PROD_REST_BASE_URL


def _normalize_identifier(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty.")
    return normalized


def _decode_json_response(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise TBankResponseError("T-Bank API returned non-JSON response.") from exc

    if not isinstance(payload, dict):
        raise TBankResponseError("T-Bank API returned unexpected JSON payload.")
    return cast(dict[str, Any], payload)


def _response_payload(response: httpx.Response) -> JsonMapping:
    try:
        payload = response.json()
    except ValueError:
        return {}

    if not isinstance(payload, Mapping):
        return {}
    return cast(JsonMapping, payload)


def _payload_str(payload: JsonMapping, *names: str) -> str | None:
    for name in names:
        value = payload.get(name)
        if value is not None:
            return str(value)
    return None


def _payload_required_str(payload: JsonMapping, *names: str) -> str:
    value = _payload_str(payload, *names)
    if value is None:
        joined_names = ", ".join(names)
        raise TBankResponseError(f"Missing required response field: {joined_names}.")
    return value


def _payload_bool(payload: JsonMapping, *names: str) -> bool:
    for name in names:
        value = payload.get(name)
        if isinstance(value, bool):
            return value
    return False


def _payload_int(payload: JsonMapping, *names: str) -> int:
    for name in names:
        value = payload.get(name)
        if value is not None:
            return _int_from_object(value, field_name=name)
    return 0


def _payload_items(payload: JsonMapping, *names: str) -> list[JsonMapping]:
    for name in names:
        value = payload.get(name)
        if isinstance(value, list):
            return [cast(JsonMapping, item) for item in value if isinstance(item, Mapping)]
    return []


def _payload_decimal(payload: JsonMapping, *names: str) -> Decimal | None:
    for name in names:
        value = payload.get(name)
        if _is_quotation_payload(value):
            return quotation_to_decimal(value)
    return None


def _is_quotation_payload(value: object) -> TypeGuard[QuotationInput]:
    if isinstance(value, Mapping):
        return "units" in value or "nano" in value
    return hasattr(value, "units") and hasattr(value, "nano")


def _int_from_object(value: object, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer, not bool.")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    raise TypeError(f"{field_name} must be an integer-compatible value.")
