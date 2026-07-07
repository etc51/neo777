"""Readonly market-data recording CLI.

This script intentionally does not import execution/order modules and cannot
submit, cancel, replace, or manage orders.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import math
import os
import sys
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, Protocol, TypeAlias, cast

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.config_loader import (  # noqa: E402
    InstrumentConfig,
    load_instrument_universe_config,
)
from neo_trader.data.market_data_recorder import (  # noqa: E402
    SUBSCRIBE_ACTION,
    MarketDataEventType,
    MarketDataRecorder,
    MarketDataRecorderError,
    MarketDataSubscription,
    RawMarketDataEvent,
)
from neo_trader.data.recording_quality_report import (  # noqa: E402
    build_recording_quality_report,
    write_recording_quality_report,
)
from neo_trader.monitoring.dashboard_state_writer import (  # noqa: E402
    DashboardInstrumentState,
    write_readonly_dashboard_state,
)

SAFE_FALSE_VALUES: Final = {"0", "false", "no", "off"}
T_INVEST_TOKEN_ENVS: Final = ("T_INVEST_TOKEN", "NEO_TRADER_TBANK_TOKEN")
EVENT_TYPES: Final = (
    MarketDataEventType.ORDERBOOK,
    MarketDataEventType.TRADES,
    MarketDataEventType.CANDLES,
)
TBankStreamClientFactory: TypeAlias = Callable[[str], "TBankMarketDataStreamClient"]


class RecorderCliError(Exception):
    """Expected recorder CLI failure with a user-facing message."""


@dataclass(frozen=True)
class ResolvedInstrument:
    """Instrument resolved for recorder subscriptions."""

    instrument_uid: str
    ticker: str
    class_code: str
    configured_uid: str


@dataclass(frozen=True)
class SafetyFlags:
    """Runtime flags that must remain readonly for recorder commands."""

    trading_mode: str
    neo_trader_trading_mode: str
    live_trading_enabled: str
    neo_trader_live_trading_enabled: str

    def to_report_dict(self) -> dict[str, str]:
        return {
            "TRADING_MODE": self.trading_mode,
            "NEO_TRADER_TRADING_MODE": self.neo_trader_trading_mode,
            "LIVE_TRADING_ENABLED": self.live_trading_enabled,
            "NEO_TRADER_LIVE_TRADING_ENABLED": self.neo_trader_live_trading_enabled,
        }


class TBankMarketDataStreamClient(Protocol):
    """Minimal async stream client used by the readonly source."""

    def stream_market_data(
        self,
        subscriptions: Sequence[MarketDataSubscription],
    ) -> AsyncIterator[object]:
        """Yield raw T-Bank SDK stream response objects."""


class MockMarketDataSource:
    """Synthetic finite market-data source for pipeline testing."""

    def __init__(self, *, ticks: int, started_at: datetime) -> None:
        self.ticks = ticks
        self.started_at = _as_utc(started_at)

    async def stream(
        self,
        subscriptions: Sequence[MarketDataSubscription],
    ) -> AsyncIterator[RawMarketDataEvent]:
        sequence = 1
        for tick in range(self.ticks):
            event_time = self.started_at + timedelta(seconds=tick)
            for subscription in subscriptions:
                yield RawMarketDataEvent.from_payload(
                    instrument_uid=subscription.instrument_uid,
                    event_type=subscription.event_type,
                    payload=_mock_payload(subscription, tick),
                    received_at=event_time,
                    event_time=event_time,
                    sequence=sequence,
                )
                sequence += 1


class TBankInvestSdkStreamClient:
    """Thin adapter around ``tinkoff.invest.AsyncClient`` market-data stream."""

    def __init__(
        self,
        *,
        token: str,
        keepalive_seconds: float = 1.0,
        sdk_module: object | None = None,
    ) -> None:
        if keepalive_seconds <= 0:
            raise ValueError("keepalive_seconds must be positive.")
        self._token = token
        self._keepalive_seconds = keepalive_seconds
        self._sdk_module = sdk_module or _load_tbank_sdk_module()

    async def stream_market_data(
        self,
        subscriptions: Sequence[MarketDataSubscription],
    ) -> AsyncIterator[object]:
        sdk = self._sdk_module
        async_client_type = sdk.AsyncClient
        async with async_client_type(self._token) as client:
            market_data_stream = client.market_data_stream
            stream_method = market_data_stream.market_data_stream
            async for response in stream_method(self._request_iterator(sdk, subscriptions)):
                yield response

    async def _request_iterator(
        self,
        sdk: object,
        subscriptions: Sequence[MarketDataSubscription],
    ) -> AsyncIterator[object]:
        for request in _build_sdk_market_data_requests(sdk, subscriptions):
            yield request
        while True:
            await asyncio.sleep(self._keepalive_seconds)


class TBankReadonlyMarketDataSource:
    """Readonly T-Bank market-data source that yields recorder raw events."""

    def __init__(
        self,
        *,
        token: str,
        stream_client: TBankMarketDataStreamClient | None = None,
    ) -> None:
        self._token = token
        self._stream_client = stream_client

    async def stream(
        self,
        subscriptions: Sequence[MarketDataSubscription],
    ) -> AsyncIterator[RawMarketDataEvent]:
        stream_client = self._stream_client or TBankInvestSdkStreamClient(token=self._token)
        async for response in stream_client.stream_market_data(subscriptions):
            for event in tbank_stream_response_to_raw_events(response, subscriptions):
                yield event


def main(argv: Sequence[str] | None = None) -> int:
    """Run the recorder CLI and return a process exit code."""

    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        safety_flags = require_readonly_runtime_flags()
        if args.mode == "mock":
            quality_report_path = asyncio.run(
                _run_mock_mode(
                    duration_seconds=args.duration_seconds,
                    max_events=args.max_events,
                    output_path=args.output,
                    dashboard_state_path=args.dashboard_state,
                    reports_dir=args.report_dir,
                    instruments_config=args.instruments_config,
                    safety_flags=safety_flags,
                )
            )
            print(f"recording_quality_report={quality_report_path}")
            return 0
        if args.mode == "tbank-readonly":
            quality_report_path = asyncio.run(
                _run_tbank_readonly_mode(
                    duration_seconds=args.duration_seconds,
                    max_events=args.max_events,
                    output_path=args.output,
                    dashboard_state_path=args.dashboard_state,
                    reports_dir=args.report_dir,
                    instruments_config=args.instruments_config,
                    safety_flags=safety_flags,
                )
            )
            print(f"recording_quality_report={quality_report_path}")
            return 0
        raise RecorderCliError(f"unsupported recorder mode: {args.mode}")
    except RecorderCliError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except MarketDataRecorderError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 3
    except NotImplementedError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 3


def require_readonly_runtime_flags(env: Mapping[str, str] | None = None) -> SafetyFlags:
    """Fail fast unless all recorder runtime flags are safe."""

    source = os.environ if env is None else env
    flags = SafetyFlags(
        trading_mode=source.get("TRADING_MODE", "readonly"),
        neo_trader_trading_mode=source.get("NEO_TRADER_TRADING_MODE", "readonly"),
        live_trading_enabled=source.get("LIVE_TRADING_ENABLED", "false"),
        neo_trader_live_trading_enabled=source.get("NEO_TRADER_LIVE_TRADING_ENABLED", "false"),
    )
    if flags.trading_mode.lower() != "readonly":
        raise RecorderCliError("TRADING_MODE must be readonly for data recording.")
    if flags.neo_trader_trading_mode.lower() != "readonly":
        raise RecorderCliError("NEO_TRADER_TRADING_MODE must be readonly for data recording.")
    if flags.live_trading_enabled.lower() not in SAFE_FALSE_VALUES:
        raise RecorderCliError("LIVE_TRADING_ENABLED must be false for data recording.")
    if flags.neo_trader_live_trading_enabled.lower() not in SAFE_FALSE_VALUES:
        raise RecorderCliError(
            "NEO_TRADER_LIVE_TRADING_ENABLED must be false for data recording."
        )
    return flags


async def _run_mock_mode(
    *,
    duration_seconds: float,
    max_events: int | None,
    output_path: Path,
    dashboard_state_path: Path,
    reports_dir: Path,
    instruments_config: Path,
    safety_flags: SafetyFlags,
) -> Path:
    if duration_seconds <= 0:
        raise RecorderCliError("--duration-seconds must be positive.")

    instrument_universe = load_instrument_universe_config(instruments_config)
    instruments = _enabled_instruments(instrument_universe.instruments)
    if not instruments:
        raise RecorderCliError("configs/instruments.yaml must contain at least one instrument.")

    ticks = max(1, math.ceil(duration_seconds))
    subscriptions = _subscriptions(instruments)
    stop_after_events = len(subscriptions) * ticks
    if max_events is not None:
        stop_after_events = min(max_events, stop_after_events)
    started_at = datetime.now(UTC)
    recorder = MarketDataRecorder(
        root=output_path,
        flush_rows=1,
        heartbeat_interval_seconds=1,
        initial_backoff_seconds=0,
        max_backoff_seconds=0,
    )
    result = await recorder.run(
        lambda: MockMarketDataSource(ticks=ticks, started_at=started_at),
        subscriptions,
        stop_after_events=stop_after_events,
        max_reconnects=0,
    )
    finished_at = datetime.now(UTC)

    dashboard_instruments = _dashboard_instruments(instruments, updated_at=finished_at)
    write_readonly_dashboard_state(
        dashboard_state_path,
        instruments=dashboard_instruments,
        kill_switch_enabled=False,
        commit_hash=result.commit_hash,
        updated_at=finished_at,
    )
    report = build_recording_quality_report(
        mode="mock",
        started_at=started_at,
        finished_at=finished_at,
        result=result,
        instruments=[instrument.instrument_uid for instrument in instruments],
        output_path=output_path,
        dashboard_state_path=dashboard_state_path,
        safety_flags=safety_flags.to_report_dict(),
    )
    return write_recording_quality_report(report, reports_dir=reports_dir)


async def _run_tbank_readonly_mode(
    *,
    duration_seconds: float,
    max_events: int | None,
    output_path: Path,
    dashboard_state_path: Path,
    reports_dir: Path,
    instruments_config: Path,
    safety_flags: SafetyFlags,
    stream_client_factory: TBankStreamClientFactory | None = None,
) -> Path:
    if duration_seconds <= 0:
        raise RecorderCliError("--duration-seconds must be positive.")
    if max_events is not None and max_events <= 0:
        raise RecorderCliError("--max-events must be positive when provided.")

    token = _tbank_token_from_env()
    instrument_universe = load_instrument_universe_config(instruments_config)
    instruments = _enabled_instruments(instrument_universe.instruments)
    if not instruments:
        raise RecorderCliError("configs/instruments.yaml must contain at least one instrument.")
    _require_configured_uids(instruments)

    subscriptions = _subscriptions(instruments)
    started_at = datetime.now(UTC)
    dashboard_instruments = _dashboard_instruments(instruments, updated_at=started_at)
    write_readonly_dashboard_state(
        dashboard_state_path,
        instruments=dashboard_instruments,
        kill_switch_enabled=False,
        updated_at=started_at,
    )
    recorder = MarketDataRecorder(root=output_path, flush_rows=1)
    result = await recorder.run(
        lambda: TBankReadonlyMarketDataSource(
            token=token,
            stream_client=(
                stream_client_factory(token) if stream_client_factory is not None else None
            ),
        ),
        subscriptions,
        stop_after_events=max_events,
        stop_after_seconds=duration_seconds,
    )
    finished_at = datetime.now(UTC)

    dashboard_instruments = _dashboard_instruments(instruments, updated_at=finished_at)
    write_readonly_dashboard_state(
        dashboard_state_path,
        instruments=dashboard_instruments,
        kill_switch_enabled=False,
        commit_hash=result.commit_hash,
        updated_at=finished_at,
    )
    report = build_recording_quality_report(
        mode="tbank-readonly",
        started_at=started_at,
        finished_at=finished_at,
        result=result,
        instruments=[instrument.instrument_uid for instrument in instruments],
        output_path=output_path,
        dashboard_state_path=dashboard_state_path,
        safety_flags=safety_flags.to_report_dict(),
    )
    return write_recording_quality_report(report, reports_dir=reports_dir)


def _tbank_token_from_env() -> str:
    for env_name in T_INVEST_TOKEN_ENVS:
        token = os.getenv(env_name)
        if token is not None and token.strip():
            return token.strip()
    joined_names = " or ".join(T_INVEST_TOKEN_ENVS)
    raise RecorderCliError(
        f"{joined_names} is required for --mode tbank-readonly. "
        "Set it in the local environment only; do not commit it."
    )


def _require_configured_uids(instruments: Sequence[ResolvedInstrument]) -> None:
    missing_uid = [instrument.ticker for instrument in instruments if not instrument.configured_uid]
    if missing_uid:
        joined = ", ".join(missing_uid)
        raise RecorderCliError(
            "configs/instruments.yaml has enabled instruments without uid: "
            f"{joined}. Fill instruments[].uid before using tbank-readonly."
        )


def _enabled_instruments(configs: Sequence[InstrumentConfig]) -> tuple[ResolvedInstrument, ...]:
    enabled = [instrument for instrument in configs if instrument.enabled]
    selected = enabled or list(configs)
    return tuple(_resolve_instrument(instrument) for instrument in selected)


def _resolve_instrument(instrument: InstrumentConfig) -> ResolvedInstrument:
    fallback_uid = f"MOCK-{instrument.ticker}-{instrument.class_code}"
    return ResolvedInstrument(
        instrument_uid=instrument.uid.strip() or fallback_uid,
        ticker=instrument.ticker,
        class_code=instrument.class_code,
        configured_uid=instrument.uid.strip(),
    )


def _subscriptions(instruments: Sequence[ResolvedInstrument]) -> tuple[MarketDataSubscription, ...]:
    subscriptions: list[MarketDataSubscription] = []
    for instrument in instruments:
        subscriptions.extend(
            (
                MarketDataSubscription.orderbook(instrument.instrument_uid),
                MarketDataSubscription.trades(instrument.instrument_uid),
                MarketDataSubscription.candles(instrument.instrument_uid),
            )
        )
    return tuple(subscriptions)


def _dashboard_instruments(
    instruments: Sequence[ResolvedInstrument],
    *,
    updated_at: datetime,
) -> tuple[DashboardInstrumentState, ...]:
    return tuple(
        DashboardInstrumentState(
            instrument_uid=instrument.instrument_uid,
            ticker=instrument.ticker,
            name=f"{instrument.ticker}.{instrument.class_code}",
            spread_bps=Decimal("2.50"),
            imbalance=Decimal("0.12"),
            volatility_regime="normal",
            last_event_at=updated_at,
            last_price=Decimal("100.10"),
        )
        for instrument in instruments
    )


def _mock_payload(subscription: MarketDataSubscription, tick: int) -> dict[str, object]:
    base_price = Decimal("100") + Decimal(tick) / Decimal("10")
    if subscription.event_type is MarketDataEventType.ORDERBOOK:
        return {
            "bids": [
                {"price": str(base_price - Decimal("0.01")), "quantity": "100"},
                {"price": str(base_price - Decimal("0.02")), "quantity": "80"},
            ],
            "asks": [
                {"price": str(base_price + Decimal("0.01")), "quantity": "90"},
                {"price": str(base_price + Decimal("0.02")), "quantity": "70"},
            ],
            "depth": 2,
        }
    if subscription.event_type is MarketDataEventType.TRADES:
        return {
            "price": str(base_price),
            "quantity": "10",
            "direction": "BUY" if tick % 2 == 0 else "SELL",
        }
    if subscription.event_type is MarketDataEventType.CANDLES:
        return {
            "open": str(base_price - Decimal("0.05")),
            "high": str(base_price + Decimal("0.10")),
            "low": str(base_price - Decimal("0.10")),
            "close": str(base_price),
            "volume": "1000",
        }
    raise RecorderCliError(f"unsupported mock event type: {subscription.event_type}")


def tbank_stream_response_to_raw_events(
    response: object,
    subscriptions: Sequence[MarketDataSubscription],
) -> tuple[RawMarketDataEvent, ...]:
    """Convert one T-Bank stream response into zero or more recorder events."""

    received_at = datetime.now(UTC)
    events: list[RawMarketDataEvent] = []
    orderbook = _field(response, "orderbook", "order_book")
    trade = _field(response, "trade")
    candle = _field(response, "candle")

    if orderbook is not None:
        event_time = _event_time(orderbook, "time")
        payload = _orderbook_payload(orderbook, subscriptions)
        events.append(
            RawMarketDataEvent.from_payload(
                instrument_uid=cast(str, payload["instrument_uid"]),
                event_type=MarketDataEventType.ORDERBOOK,
                payload=payload,
                received_at=received_at,
                event_time=event_time,
            )
        )

    if trade is not None:
        event_time = _event_time(trade, "time")
        payload = _trade_payload(trade, subscriptions)
        events.append(
            RawMarketDataEvent.from_payload(
                instrument_uid=cast(str, payload["instrument_uid"]),
                event_type=MarketDataEventType.TRADES,
                payload=payload,
                received_at=received_at,
                event_time=event_time,
            )
        )

    if candle is not None:
        event_time = _event_time(candle, "time")
        payload = _candle_payload(candle, subscriptions)
        events.append(
            RawMarketDataEvent.from_payload(
                instrument_uid=cast(str, payload["instrument_uid"]),
                event_type=MarketDataEventType.CANDLES,
                payload=payload,
                received_at=received_at,
                event_time=event_time,
            )
        )

    return tuple(events)


def _orderbook_payload(
    orderbook: object,
    subscriptions: Sequence[MarketDataSubscription],
) -> dict[str, object]:
    return {
        "source": "tbank",
        "instrument_uid": _instrument_uid_from_event(orderbook, subscriptions),
        "figi": _optional_string(_field(orderbook, "figi")),
        "depth": _optional_int(_field(orderbook, "depth")),
        "is_consistent": _optional_bool(_field(orderbook, "is_consistent", "isConsistent")),
        "time": _optional_datetime(_event_time(orderbook, "time")),
        "limit_up": _optional_decimal_string(_field(orderbook, "limit_up", "limitUp")),
        "limit_down": _optional_decimal_string(_field(orderbook, "limit_down", "limitDown")),
        "bids": _price_levels(_field(orderbook, "bids")),
        "asks": _price_levels(_field(orderbook, "asks")),
    }


def _trade_payload(
    trade: object,
    subscriptions: Sequence[MarketDataSubscription],
) -> dict[str, object]:
    return {
        "source": "tbank",
        "instrument_uid": _instrument_uid_from_event(trade, subscriptions),
        "figi": _optional_string(_field(trade, "figi")),
        "direction": _enum_or_string(_field(trade, "direction")),
        "price": _optional_decimal_string(_field(trade, "price")),
        "quantity": _optional_int(_field(trade, "quantity")),
        "time": _optional_datetime(_event_time(trade, "time")),
        "trade_source": _enum_or_string(_field(trade, "trade_source", "tradeSource")),
        "open_interest": _optional_int(_field(trade, "open_interest", "openInterest")),
    }


def _candle_payload(
    candle: object,
    subscriptions: Sequence[MarketDataSubscription],
) -> dict[str, object]:
    return {
        "source": "tbank",
        "instrument_uid": _instrument_uid_from_event(candle, subscriptions),
        "figi": _optional_string(_field(candle, "figi")),
        "interval": _enum_or_string(_field(candle, "interval")),
        "open": _optional_decimal_string(_field(candle, "open")),
        "high": _optional_decimal_string(_field(candle, "high")),
        "low": _optional_decimal_string(_field(candle, "low")),
        "close": _optional_decimal_string(_field(candle, "close")),
        "volume": _optional_int(_field(candle, "volume")),
        "time": _optional_datetime(_event_time(candle, "time")),
        "last_trade_ts": _optional_datetime(
            _event_time(candle, "last_trade_ts", "lastTradeTs")
        ),
    }


def _price_levels(levels: object) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for level in _sequence(levels):
        result.append(
            {
                "price": _optional_decimal_string(_field(level, "price")),
                "quantity": _optional_int(_field(level, "quantity")),
            }
        )
    return result


def _instrument_uid_from_event(
    event_payload: object,
    subscriptions: Sequence[MarketDataSubscription],
) -> str:
    direct = _optional_string(
        _field(
            event_payload,
            "instrument_uid",
            "instrumentUid",
            "instrument_id",
            "instrumentId",
            "uid",
        )
    )
    if direct:
        for subscription in subscriptions:
            if direct in {subscription.instrument_uid, subscription.resolved_instrument_id}:
                return subscription.instrument_uid
        return direct

    figi = _optional_string(_field(event_payload, "figi"))
    if figi:
        for subscription in subscriptions:
            if figi in {subscription.instrument_uid, subscription.resolved_instrument_id}:
                return subscription.instrument_uid
        return figi

    unique_uids = {subscription.instrument_uid for subscription in subscriptions}
    if len(unique_uids) == 1:
        return next(iter(unique_uids))
    raise RecorderCliError("T-Bank stream event has no instrument uid.")


def _build_sdk_market_data_requests(
    sdk: object,
    subscriptions: Sequence[MarketDataSubscription],
) -> tuple[object, ...]:
    requests: list[object] = []
    action = _sdk_enum(sdk, "SubscriptionAction", SUBSCRIBE_ACTION)

    orderbook_subscriptions = [
        subscription
        for subscription in subscriptions
        if subscription.event_type is MarketDataEventType.ORDERBOOK
    ]
    trade_subscriptions = [
        subscription
        for subscription in subscriptions
        if subscription.event_type is MarketDataEventType.TRADES
    ]
    candle_subscriptions = [
        subscription
        for subscription in subscriptions
        if subscription.event_type is MarketDataEventType.CANDLES
    ]

    if orderbook_subscriptions:
        requests.append(_sdk_orderbook_request(sdk, action, orderbook_subscriptions))
    if trade_subscriptions:
        requests.append(_sdk_trades_request(sdk, action, trade_subscriptions))
    if candle_subscriptions:
        requests.append(_sdk_candles_request(sdk, action, candle_subscriptions))
    return tuple(requests)


def _sdk_orderbook_request(
    sdk: object,
    action: object,
    subscriptions: Sequence[MarketDataSubscription],
) -> object:
    instrument_type = sdk.OrderBookInstrument
    request_type = sdk.SubscribeOrderBookRequest
    envelope_type = sdk.MarketDataRequest
    instruments = [
        _construct_sdk_message(
            instrument_type,
            (
                {
                    "instrument_id": subscription.resolved_instrument_id,
                    "depth": subscription.depth,
                    "order_book_type": _sdk_enum_or_none(
                        sdk,
                        "OrderBookType",
                        subscription.order_book_type,
                    ),
                },
                {
                    "instrument_id": subscription.resolved_instrument_id,
                    "depth": subscription.depth,
                },
                {"figi": subscription.resolved_instrument_id, "depth": subscription.depth},
            ),
        )
        for subscription in subscriptions
    ]
    request = _construct_sdk_message(
        request_type,
        ({"subscription_action": action, "instruments": instruments},),
    )
    return envelope_type(subscribe_order_book_request=request)


def _sdk_trades_request(
    sdk: object,
    action: object,
    subscriptions: Sequence[MarketDataSubscription],
) -> object:
    instrument_type = sdk.TradeInstrument
    request_type = sdk.SubscribeTradesRequest
    envelope_type = sdk.MarketDataRequest
    instruments = [
        _construct_sdk_message(
            instrument_type,
            (
                {
                    "instrument_id": subscription.resolved_instrument_id,
                    "trade_source": _sdk_enum_or_none(
                        sdk,
                        "TradeSourceType",
                        subscription.trade_source,
                    ),
                    "with_open_interest": subscription.with_open_interest,
                },
                {"instrument_id": subscription.resolved_instrument_id},
                {"figi": subscription.resolved_instrument_id},
            ),
        )
        for subscription in subscriptions
    ]
    request = _construct_sdk_message(
        request_type,
        ({"subscription_action": action, "instruments": instruments},),
    )
    return envelope_type(subscribe_trades_request=request)


def _sdk_candles_request(
    sdk: object,
    action: object,
    subscriptions: Sequence[MarketDataSubscription],
) -> object:
    instrument_type = sdk.CandleInstrument
    request_type = sdk.SubscribeCandlesRequest
    envelope_type = sdk.MarketDataRequest
    instruments = [
        _construct_sdk_message(
            instrument_type,
            (
                {
                    "instrument_id": subscription.resolved_instrument_id,
                    "interval": _sdk_enum(
                        sdk,
                        "SubscriptionInterval",
                        subscription.candle_interval,
                    ),
                },
                {
                    "figi": subscription.resolved_instrument_id,
                    "interval": _sdk_enum(
                        sdk,
                        "SubscriptionInterval",
                        subscription.candle_interval,
                    ),
                },
            ),
        )
        for subscription in subscriptions
    ]
    waiting_close = any(subscription.waiting_close for subscription in subscriptions)
    request = _construct_sdk_message(
        request_type,
        (
            {
                "subscription_action": action,
                "instruments": instruments,
                "waiting_close": waiting_close,
            },
            {"subscription_action": action, "instruments": instruments},
        ),
    )
    return envelope_type(subscribe_candles_request=request)


def _construct_sdk_message(
    message_type: object,
    alternatives: Sequence[Mapping[str, object | None]],
) -> object:
    last_error: TypeError | None = None
    for kwargs in alternatives:
        clean_kwargs = {key: value for key, value in kwargs.items() if value is not None}
        try:
            return cast(Any, message_type)(**clean_kwargs)
        except TypeError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise RecorderCliError("No SDK message constructor alternatives were provided.")


def _sdk_enum(sdk: object, enum_name: str, member_name: str) -> object:
    enum_type = getattr(sdk, enum_name)
    return getattr(enum_type, member_name)


def _sdk_enum_or_none(sdk: object, enum_name: str, member_name: str) -> object | None:
    enum_type = getattr(sdk, enum_name, None)
    if enum_type is None:
        return None
    return getattr(enum_type, member_name, None)


def _load_tbank_sdk_module() -> object:
    for module_name in ("t_tech.invest", "tinkoff.invest"):
        try:
            return importlib.import_module(module_name)
        except ImportError:
            continue
    raise RecorderCliError(
        "tbank-readonly requires the T-Invest Python SDK import path "
        "'t_tech.invest' or 'tinkoff.invest'. Install the SDK locally before real recording."
    )


def _field(value: object, *names: str) -> object | None:
    if isinstance(value, Mapping):
        for name in names:
            if name in value and value[name] is not None:
                return value[name]
    for name in names:
        if hasattr(value, name):
            attr = getattr(value, name)
            if attr is not None:
                return attr
    return None


def _sequence(value: object) -> tuple[object, ...]:
    if value is None or isinstance(value, str | bytes | Mapping):
        return ()
    if isinstance(value, Sequence):
        return tuple(value)
    return ()


def _event_time(value: object, *names: str) -> datetime | None:
    for name in names:
        candidate = _field(value, name)
        parsed = _datetime_from_object(candidate)
        if parsed is not None:
            return parsed
    return None


def _datetime_from_object(value: object | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str):
        return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    to_datetime = getattr(value, "ToDatetime", None)
    if callable(to_datetime):
        parsed = to_datetime()
        if isinstance(parsed, datetime):
            return _as_utc(parsed)
    return None


def _optional_datetime(value: datetime | None) -> str | None:
    return None if value is None else _as_utc(value).isoformat()


def _optional_string(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_bool(value: object | None) -> bool | None:
    if isinstance(value, bool):
        return value
    return None


def _optional_int(value: object | None) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return int(value)
    return None


def _enum_or_string(value: object | None) -> str | None:
    if value is None:
        return None
    name = getattr(value, "name", None)
    if isinstance(name, str):
        return name
    return str(value)


def _optional_decimal_string(value: object | None) -> str | None:
    decimal_value = _decimal_from_quotation_like(value)
    return None if decimal_value is None else str(decimal_value)


def _decimal_from_quotation_like(value: object | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | str):
        return Decimal(value)
    if isinstance(value, float):
        return Decimal(str(value))
    units = _field(value, "units")
    nano = _field(value, "nano")
    if units is None and nano is None:
        return None
    return Decimal(_optional_int(units) or 0) + (
        Decimal(_optional_int(nano) or 0) / Decimal("1000000000")
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Readonly market-data recorder")
    parser.add_argument("--mode", choices=("mock", "tbank-readonly"), required=True)
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after N recorded events; useful for safe short smoke tests.",
    )
    parser.add_argument("--output", type=Path, default=Path("data/raw"))
    parser.add_argument(
        "--dashboard-state",
        type=Path,
        default=Path("data/monitoring/dashboard_state.json"),
    )
    parser.add_argument("--report-dir", type=Path, default=Path("data/reports"))
    parser.add_argument(
        "--instruments-config",
        type=Path,
        default=Path("configs/instruments.yaml"),
    )
    return parser


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


if __name__ == "__main__":
    raise SystemExit(main())
