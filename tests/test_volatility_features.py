"""Tests for volatility feature calculations."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from neo_trader.features.volatility import (
    atr,
    log_returns,
    realized_volatility,
    volatility_percentile,
    volatility_regime,
)


def test_log_returns_from_synthetic_closes() -> None:
    closes = [Decimal("100"), Decimal("105"), Decimal("102.9")]

    returns = log_returns(closes)

    assert len(returns) == 2
    assert returns[0] == pytest.approx(Decimal("0.04879016416943200306537440422"))
    assert returns[1] == pytest.approx(Decimal("-0.02020270731751944840804530102"))


def test_log_returns_accept_candle_mappings() -> None:
    candles = [
        {"high": "101", "low": "99", "close": "100"},
        {"high": "103", "low": "100", "close": "102"},
    ]

    assert log_returns(candles)[0] == pytest.approx(
        Decimal("0.01980262729617971238944949893")
    )


def test_realized_volatility_uses_tail_window() -> None:
    closes = [Decimal("100"), Decimal("105"), Decimal("102.9"), Decimal("108.045")]

    volatility = realized_volatility(closes, window=2)

    assert volatility == pytest.approx(Decimal("0.05280747582149217183240977270"))


def test_atr_from_synthetic_candles() -> None:
    candles = [
        {"high": "10", "low": "9", "close": "9.5"},
        {"high": "11", "low": "9.25", "close": "10.5"},
        {"high": "12", "low": "10", "close": "11"},
    ]

    assert atr(candles) == Decimal("1.583333333333333333333333333")
    assert atr(candles, window=2) == Decimal("1.875")


def test_atr_accepts_object_candles() -> None:
    candles = [
        SimpleNamespace(high=Decimal("20"), low=Decimal("19"), close=Decimal("19.5")),
        SimpleNamespace(high=Decimal("21"), low=Decimal("19.25"), close=Decimal("20.5")),
    ]

    assert atr(candles) == Decimal("1.375")


def test_volatility_percentile_and_regime() -> None:
    history = [Decimal("0.10"), Decimal("0.20"), Decimal("0.30"), Decimal("0.40")]

    assert volatility_percentile(Decimal("0.30"), history) == Decimal("75.00")
    assert volatility_regime(Decimal("10")) == "low"
    assert volatility_regime(Decimal("50")) == "normal"
    assert volatility_regime(Decimal("90")) == "high"
    assert volatility_regime(Decimal("99")) == "extreme"


def test_validation_errors() -> None:
    with pytest.raises(ValueError, match="price must be positive"):
        log_returns([Decimal("100"), Decimal("0")])

    with pytest.raises(ValueError, match="window must be positive"):
        realized_volatility([Decimal("100"), Decimal("101")], window=0)

    with pytest.raises(ValueError, match="history must not be empty"):
        volatility_percentile(Decimal("1"), [])
