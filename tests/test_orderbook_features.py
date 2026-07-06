"""Tests for order book feature calculations."""

from decimal import Decimal

import pytest

from neo_trader.features.orderbook import (
    best_bid_ask,
    book_wall_score,
    depth_sum,
    expected_slippage_bps,
    expected_vwap_to_fill,
    imbalance,
    microprice,
    mid_price,
    spread_bps,
    weighted_imbalance,
)


def test_top_of_book_features() -> None:
    book = {
        "bids": [
            (Decimal("99.5"), Decimal("20")),
            (Decimal("100"), Decimal("10")),
            (Decimal("99"), Decimal("70")),
        ],
        "asks": [
            (Decimal("103"), Decimal("10")),
            (Decimal("101"), Decimal("30")),
            (Decimal("102"), Decimal("10")),
        ],
    }

    assert best_bid_ask(book) == (Decimal("100"), Decimal("101"))
    assert mid_price(book) == Decimal("100.5")
    assert spread_bps(book) == pytest.approx(Decimal("99.50248756218905472636815920"))
    assert microprice(book) == Decimal("100.25")


def test_depth_and_imbalance_features() -> None:
    book = {
        "bids": [(100, 10), (99, 20), (98, 70)],
        "asks": [(101, 30), (102, 10), (103, 10)],
    }

    assert depth_sum(book, "bid", 2) == Decimal("30")
    assert depth_sum(book, "ask", 2) == Decimal("40")
    assert imbalance(book, 2) == Decimal("-0.1428571428571428571428571429")
    assert weighted_imbalance(book, Decimal("0")) == Decimal("0.3333333333333333333333333333")


def test_expected_vwap_and_slippage_for_aggressive_buy_and_sell() -> None:
    book = {
        "bids": [(100, 10), (99, 20), (98, 70)],
        "asks": [(101, 30), (102, 10), (103, 10)],
    }

    buy_vwap = expected_vwap_to_fill(book, "buy", Decimal("35"))
    sell_vwap = expected_vwap_to_fill(book, "sell", Decimal("25"))

    assert buy_vwap == Decimal("101.1428571428571428571428571")
    assert sell_vwap == Decimal("99.4")
    assert expected_slippage_bps(book, "buy", Decimal("35")) == pytest.approx(
        Decimal("63.96588486140724946695025871")
    )
    assert expected_slippage_bps(book, "sell", Decimal("25")) == pytest.approx(
        Decimal("109.4527363184079601990049751")
    )


def test_expected_vwap_requires_enough_depth() -> None:
    book = {
        "bids": [(100, 10)],
        "asks": [(101, 10)],
    }

    with pytest.raises(ValueError, match="not enough book depth"):
        expected_vwap_to_fill(book, "buy", Decimal("11"))


def test_book_wall_score_uses_levels_within_distance_from_best() -> None:
    book = {
        "bids": [(100, 10), (99.5, 20), (99, 70), (95, 1)],
        "asks": [(101, 30), (102, 10), (103, 40)],
    }

    assert book_wall_score(book, "bid", Decimal("120")) == Decimal("2.1")
    assert book_wall_score(book, "ask", Decimal("100")) == Decimal("1.5")


def test_dict_levels_and_quotation_like_prices_are_supported() -> None:
    book = {
        "bids": [
            {"price": {"units": 100, "nano": 500_000_000}, "quantity": "3"},
            {"price": {"units": 100, "nano": 0}, "quantity": "7"},
        ],
        "asks": [
            {"price": {"units": 101, "nano": 250_000_000}, "qty": "2"},
        ],
    }

    assert best_bid_ask(book) == (Decimal("100.5"), Decimal("101.25"))
    assert depth_sum(book, "bids", 2) == Decimal("10")
