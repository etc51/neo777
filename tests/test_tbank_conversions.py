"""Unit tests for T-Bank value conversions."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from neo_trader.broker.tbank import quotation_to_decimal


@pytest.mark.parametrize(
    ("quotation", "expected"),
    [
        ({"units": 0, "nano": 0}, Decimal("0")),
        ({"units": 12, "nano": 340_000_000}, Decimal("12.34")),
        ({"units": 1, "nano": 1}, Decimal("1.000000001")),
        ({"units": -2, "nano": -500_000_000}, Decimal("-2.5")),
        ({"units": "7", "nano": "125000000"}, Decimal("7.125")),
    ],
)
def test_quotation_to_decimal_from_mapping(
    quotation: dict[str, object],
    expected: Decimal,
) -> None:
    assert quotation_to_decimal(quotation) == expected


def test_quotation_to_decimal_from_object() -> None:
    quotation = SimpleNamespace(units=3, nano=25_000_000)

    assert quotation_to_decimal(quotation) == Decimal("3.025")


def test_quotation_to_decimal_rejects_bool_units() -> None:
    with pytest.raises(TypeError):
        quotation_to_decimal({"units": True, "nano": 0})
