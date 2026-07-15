"""Fake-only tests for the Neobitcoin research T-Invest safety boundary."""

from __future__ import annotations

import asyncio
import os
import ssl
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from enum import IntEnum
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from neo_trader.neobitcoin_research import tbank as tbank_module
from neo_trader.neobitcoin_research.safety import (
    TBankRpcBlockedError,
    TBankToken,
    load_tbank_token,
    require_allowed_rpc_path,
)
from neo_trader.neobitcoin_research.tbank import (
    CANDLE_INTERVALS,
    FIND_INSTRUMENT_RPC,
    GET_INSTRUMENT_BY_RPC,
    GET_TRADING_STATUS_RPC,
    MARKET_DATA_STREAM_RPC,
    ORDERBOOK_DEPTH,
    TRADING_SCHEDULES_RPC,
    TBankMarketDataDecoder,
    TBankResearchClient,
    TBankSubscriptionRejectedError,
    build_bidirectional_subscriptions,
    build_subscription_check_request,
    market_data_request_iterator,
    normalize_security_trading_status,
    require_successful_subscription_ack,
    to_json_safe_object,
)

FAKE_TOKEN = "t." + "A1_b-" * 8
UID = "4effa274-4e8f-422c-93ff-04aa34fe8e39"


class FakeMessage:
    def __init__(self, **kwargs: object) -> None:
        vars(self).update(kwargs)


class FakeUnaryTransport:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False

    def call(self, rpc_path: str, payload: Mapping[str, object]) -> object:
        self.calls.append((rpc_path, dict(payload)))
        if rpc_path == FIND_INSTRUMENT_RPC:
            return {
                "instruments": [
                    {
                        "uid": "FUZZY-UID",
                        "ticker": "BTCUSDperpAX",
                        "classCode": "SPBDMFUT",
                        "name": "Neo Bitcoin synthetic clone",
                    },
                    {
                        "uid": UID,
                        "ticker": "BTCUSDperpA",
                        "classCode": "SPBDMFUT",
                        "name": "Neo Bitcoin",
                    },
                ]
            }
        if rpc_path == GET_INSTRUMENT_BY_RPC:
            return {
                "instrument": {
                    "uid": UID,
                    "positionUid": "POSITION-UID",
                    "figi": "BTCUSDPERP00",
                    "ticker": "BTCUSDperpA",
                    "classCode": "SPBDMFUT",
                    "name": "Neo Bitcoin",
                    "instrumentType": "futures",
                    "exchange": "spb_future",
                    "currency": "rub",
                    "lot": 1,
                    "minPriceIncrement": {"units": "0", "nano": 100_000_000},
                    "apiTradeAvailableFlag": True,
                    "buyAvailableFlag": True,
                    "sellAvailableFlag": True,
                }
            }
        if rpc_path == GET_TRADING_STATUS_RPC:
            return {
                "instrumentUid": UID,
                "tradingStatus": "SECURITY_TRADING_STATUS_NORMAL_TRADING",
                "limitOrderAvailableFlag": True,
                "marketOrderAvailableFlag": False,
            }
        if rpc_path == TRADING_SCHEDULES_RPC:
            return {
                "exchanges": [
                    {
                        "exchange": "spb_future",
                        "days": [{"date": "2026-07-10T00:00:00Z", "isTradingDay": True}],
                    }
                ]
            }
        raise AssertionError(f"unexpected fake RPC: {rpc_path}")

    def close(self) -> None:
        self.closed = True


class FakeStreamTransport:
    def __init__(self, responses: Sequence[object]) -> None:
        self.responses = tuple(responses)
        self.rpc_path: str | None = None
        self.requests: tuple[object, ...] = ()

    async def stream(
        self,
        rpc_path: str,
        requests: Sequence[object],
    ) -> AsyncIterator[object]:
        self.rpc_path = rpc_path
        self.requests = tuple(requests)
        for response in self.responses:
            yield response


def fake_sdk() -> SimpleNamespace:
    message_names = (
        "MarketDataRequest",
        "OrderBookInstrument",
        "SubscribeOrderBookRequest",
        "TradeInstrument",
        "SubscribeTradesRequest",
        "LastPriceInstrument",
        "SubscribeLastPriceRequest",
        "InfoInstrument",
        "SubscribeInfoRequest",
        "CandleInstrument",
        "SubscribeCandlesRequest",
        "GetMySubscriptions",
    )
    values: dict[str, object] = {name: FakeMessage for name in message_names}
    values.update(
        {
            "SubscriptionAction": SimpleNamespace(
                SUBSCRIPTION_ACTION_SUBSCRIBE="SUBSCRIPTION_ACTION_SUBSCRIBE"
            ),
            "OrderBookType": SimpleNamespace(ORDERBOOK_TYPE_ALL="ORDERBOOK_TYPE_ALL"),
            "TradeSourceType": SimpleNamespace(TRADE_SOURCE_ALL="TRADE_SOURCE_ALL"),
            "SubscriptionInterval": SimpleNamespace(
                **{interval: interval for interval in CANDLE_INTERVALS}
            ),
        }
    )
    return SimpleNamespace(**values)


def test_token_is_read_from_only_the_explicit_file_without_output_or_persistence(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    token_path = tmp_path / "exact-token.txt"
    token_path.write_text(f"TBANK_TOKEN={FAKE_TOKEN}\n", encoding="utf-8")
    unrelated = tmp_path / "another.txt"
    unrelated.write_text("t." + "Z" * 40, encoding="utf-8")
    environment_before = dict(os.environ)

    secret = load_tbank_token(token_path)

    assert secret.get_secret_value() == FAKE_TOKEN
    assert FAKE_TOKEN not in repr(secret)
    assert FAKE_TOKEN not in str(secret)
    assert not (tmp_path / ".env").exists()
    assert dict(os.environ) == environment_before
    captured = capsys.readouterr()
    assert FAKE_TOKEN not in captured.out
    assert FAKE_TOKEN not in captured.err


def test_token_loader_accepts_one_labeled_token(tmp_path: Path) -> None:
    token_path = tmp_path / "labeled.txt"
    token_path.write_text(f"Токен: {FAKE_TOKEN}\n", encoding="utf-8")

    secret = load_tbank_token(token_path)

    assert secret.get_secret_value() == FAKE_TOKEN


def test_default_rest_transport_uses_verified_system_ca(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class FakeHttpClient:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        def close(self) -> None:
            return None

    monkeypatch.setattr(tbank_module.httpx, "Client", FakeHttpClient)

    client = TBankResearchClient(TBankToken(FAKE_TOKEN))
    client.close()

    context = captured["verify"]
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True


def test_numeric_security_status_uses_official_enum_and_unknown_is_closed() -> None:
    class FakeSecurityTradingStatus(IntEnum):
        SECURITY_TRADING_STATUS_NORMAL_TRADING = 5

    sdk = SimpleNamespace(SecurityTradingStatus=FakeSecurityTradingStatus)

    assert normalize_security_trading_status(5, sdk_module=sdk) == "NORMAL_TRADING"
    assert normalize_security_trading_status("5", sdk_module=sdk) == "NORMAL_TRADING"
    assert (
        normalize_security_trading_status(
            FakeSecurityTradingStatus.SECURITY_TRADING_STATUS_NORMAL_TRADING,
            sdk_module=sdk,
        )
        == "NORMAL_TRADING"
    )
    assert normalize_security_trading_status(999, sdk_module=sdk) == "UNKNOWN"
    assert to_json_safe_object(
        {"trading_status": FakeSecurityTradingStatus.SECURITY_TRADING_STATUS_NORMAL_TRADING}
    )["trading_status"] == "SECURITY_TRADING_STATUS_NORMAL_TRADING"


def test_rpc_allowlist_is_unconditional_for_live_flags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRADING_MODE", "live")
    monkeypatch.setenv("LIVE_TRADING_ENABLED", "true")
    assert require_allowed_rpc_path(FIND_INSTRUMENT_RPC) == FIND_INSTRUMENT_RPC

    blocked = (
        "/tinkoff.public.invest.api.contract.v1.OrdersService/PostOrder",
        "/tinkoff.public.invest.api.contract.v1.StopOrdersService/PostStopOrder",
        "/tinkoff.public.invest.api.contract.v1.SandboxService/PostSandboxOrder",
        "/tinkoff.public.invest.api.contract.v1.MarketDataService/UnknownMethod",
    )
    for rpc_path in blocked:
        with pytest.raises(TBankRpcBlockedError):
            require_allowed_rpc_path(rpc_path)


def test_discovery_selects_and_verifies_exact_neobitcoin() -> None:
    unary = FakeUnaryTransport()
    client = TBankResearchClient(TBankToken(FAKE_TOKEN), unary_transport=unary)

    metadata = client.discover_neobitcoin(as_of=datetime(2026, 7, 10, 10, 0, tzinfo=UTC))

    assert metadata.instrument_uid == UID
    assert metadata.ticker == "BTCUSDperpA"
    assert metadata.class_code == "SPBDMFUT"
    assert metadata.figi == "BTCUSDPERP00"
    assert metadata.lot == 1
    assert metadata.min_price_increment == Decimal("0.1")
    assert metadata.currency == "rub"
    assert metadata.api_trade_available is True
    assert metadata.trading_schedules[0]["exchange"] == "spb_future"
    assert [path for path, _ in unary.calls] == [
        FIND_INSTRUMENT_RPC,
        GET_INSTRUMENT_BY_RPC,
        GET_TRADING_STATUS_RPC,
        TRADING_SCHEDULES_RPC,
    ]
    public_names = {name for name in dir(client) if not name.startswith("_")}
    assert "client" not in public_names
    assert "channel" not in public_names
    assert "transport" not in public_names


def test_builds_all_required_bidirectional_subscriptions_and_check() -> None:
    sdk = fake_sdk()

    requests = build_bidirectional_subscriptions(sdk, UID)

    assert len(requests) == 5
    orderbook = requests[0].subscribe_order_book_request
    assert orderbook.instruments[0].instrument_id == UID
    assert orderbook.instruments[0].depth == ORDERBOOK_DEPTH == 50
    assert orderbook.instruments[0].order_book_type == "ORDERBOOK_TYPE_ALL"
    trades = requests[1].subscribe_trades_request
    assert trades.instruments[0].instrument_id == UID
    assert trades.with_open_interest is True
    assert requests[2].subscribe_last_price_request.instruments[0].instrument_id == UID
    assert requests[3].subscribe_info_request.instruments[0].instrument_id == UID
    candles = requests[4].subscribe_candles_request
    assert tuple(item.interval for item in candles.instruments) == CANDLE_INTERVALS

    check = build_subscription_check_request(sdk)
    assert isinstance(check.get_my_subscriptions, FakeMessage)


def test_request_iterator_sends_periodic_subscription_check() -> None:
    sdk = fake_sdk()
    initial = FakeMessage(kind="initial")
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    async def read_two_requests() -> tuple[object, object]:
        iterator = market_data_request_iterator(
            sdk,
            (initial,),
            subscription_check_seconds=17.0,
            sleep=fake_sleep,
        )
        try:
            return await anext(iterator), await anext(iterator)
        finally:
            await iterator.aclose()

    first, second = asyncio.run(read_two_requests())

    assert first is initial
    assert isinstance(second.get_my_subscriptions, FakeMessage)
    assert sleeps == [17.0]


def test_decoder_keeps_full_nested_payload_and_propagates_ack_identity() -> None:
    decoder = TBankMarketDataDecoder()
    received_at = datetime(2026, 7, 10, 10, 0, tzinfo=UTC)
    ack_payload = {
        "subscribe_order_book_response": {
            "tracking_id": "TRACK-1",
            "order_book_subscriptions": [
                {
                    "instrument_uid": UID,
                    "ticker": "BTCUSDperpA",
                    "class_code": "SPBDMFUT",
                    "stream_id": "STREAM-1",
                    "subscription_id": "SUB-BOOK-1",
                    "subscription_status": "SUBSCRIPTION_STATUS_SUCCESS",
                    "depth": 50,
                }
            ],
        }
    }
    ack = decoder.decode(
        ack_payload,
        received_at=received_at,
        received_monotonic_ns=123,
    )[0]
    require_successful_subscription_ack(ack)
    assert ack.event_type == "subscription_ack"
    assert ack.stream_id == "STREAM-1"
    assert ack.subscription_id == "SUB-BOOK-1"
    assert (
        ack.payload["subscribe_order_book_response"] == ack_payload["subscribe_order_book_response"]
    )

    event_payload = {
        "orderbook": {
            "instrument_uid": UID,
            "time": "2026-07-10T10:00:00.100000Z",
            "is_consistent": True,
            "bids": [{"price": {"units": "100", "nano": 0}, "quantity": 5}],
            "asks": [{"price": {"units": "101", "nano": 0}, "quantity": 7}],
            "nested_extra": {"preserved": [1, 2, 3]},
        }
    }
    event = decoder.decode(
        event_payload,
        received_at=received_at,
        received_monotonic_ns=456,
    )[0]
    assert event.event_type == "orderbook"
    assert event.stream_id == "STREAM-1"
    assert event.subscription_id == "SUB-BOOK-1"
    assert event.is_consistent is True
    assert event.received_monotonic_ns == 456
    assert event.payload["orderbook"] == event_payload["orderbook"]

    ping = decoder.decode(
        {"ping": {"time": "2026-07-10T10:00:01Z", "stream_id": "STREAM-1"}},
        received_at=received_at,
        received_monotonic_ns=789,
    )[0]
    assert ping.event_type == "ping"
    assert ping.stream_id == "STREAM-1"


def test_subscription_ack_failure_is_rejected() -> None:
    decoder = TBankMarketDataDecoder()
    rejected = decoder.decode(
        {
            "subscribe_trades_response": {
                "trade_subscriptions": [
                    {
                        "instrument_uid": UID,
                        "stream_id": "STREAM-1",
                        "subscription_id": "SUB-TRADES-1",
                        "subscription_status": "SUBSCRIPTION_STATUS_LIMIT_IS_EXCEEDED",
                    }
                ]
            }
        },
        received_monotonic_ns=1,
    )[0]

    with pytest.raises(TBankSubscriptionRejectedError):
        require_successful_subscription_ack(rejected)


def test_stream_uses_only_fake_sdk_and_fake_channel() -> None:
    sdk = fake_sdk()
    stream = FakeStreamTransport(
        (
            {
                "subscribe_trades_response": {
                    "trade_subscriptions": [
                        {
                            "instrument_uid": UID,
                            "stream_id": "STREAM-X",
                            "subscription_id": "SUB-X",
                            "subscription_status": "SUBSCRIPTION_STATUS_SUCCESS",
                        }
                    ]
                }
            },
            {
                "trade": {
                    "instrument_uid": UID,
                    "time": "2026-07-10T10:00:00Z",
                    "price": {"units": "100", "nano": 0},
                    "quantity": 2,
                    "direction": "TRADE_DIRECTION_BUY",
                }
            },
        )
    )
    client = TBankResearchClient(
        TBankToken(FAKE_TOKEN),
        unary_transport=FakeUnaryTransport(),
        stream_transport=stream,
        sdk_module=sdk,
    )

    async def collect() -> list[Any]:
        return [record async for record in client.stream_market_data(UID)]

    records = asyncio.run(collect())

    assert stream.rpc_path == MARKET_DATA_STREAM_RPC
    assert len(stream.requests) == 5
    assert [record.event_type for record in records] == ["subscription_ack", "trade"]
    assert records[1].stream_id == "STREAM-X"
    assert records[1].subscription_id == "SUB-X"
