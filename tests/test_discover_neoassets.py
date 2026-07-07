"""Tests for readonly T-Bank neoasset discovery helpers."""

from __future__ import annotations

import ast
from decimal import Decimal
from pathlib import Path

import yaml

from neo_trader.research.neoassets import (
    LiquidityPrecheckConfig,
    NeoAssetRecord,
    apply_liquidity_precheck,
    parse_neoasset_candidates,
    write_neoassets_universe,
)


def test_discovery_parser_finds_explicit_and_indicative_metadata() -> None:
    payloads = {
        "Indicatives": {
            "instruments": [
                {
                    "ticker": "TBIND1",
                    "uid": "UID1",
                    "figi": "FIGI1",
                    "classCode": "SPBX",
                    "name": "T-Bank indicative asset",
                    "instrumentType": "INDICATIVE",
                    "exchange": "spb",
                    "currency": "rub",
                    "apiTradeAvailableFlag": True,
                    "lot": 1,
                    "minPriceIncrement": {"units": 0, "nano": 10000000},
                }
            ]
        },
        "FindInstrument:neo": {
            "instruments": [
                {
                    "ticker": "TBNEO2",
                    "uid": "UID2",
                    "figi": "FIGI2",
                    "classCode": "SPBX",
                    "name": "Neo metadata asset",
                    "instrumentType": "share",
                }
            ]
        },
        "Shares": {
            "instruments": [
                {
                    "ticker": "PLAIN",
                    "uid": "UID3",
                    "figi": "FIGI3",
                    "classCode": "TQBR",
                    "name": "Plain share",
                    "instrumentType": "share",
                }
            ]
        },
    }

    records = parse_neoasset_candidates(payloads)

    assert {record.ticker for record in records} == {"TBIND1", "TBNEO2"}
    first = next(record for record in records if record.ticker == "TBIND1")
    assert first.uid == "UID1"
    assert first.price_increment == Decimal("0.01")
    assert "metadata_discovery_no_explicit_neo_flag" in first.notes
    second = next(record for record in records if record.ticker == "TBNEO2")
    assert "explicit_neo_metadata" in second.notes


def test_liquidity_precheck_enables_usable_orderbook() -> None:
    records = (
        _record("GOOD", "UID1"),
        _record("EMPTY", "UID2"),
    )

    checked = apply_liquidity_precheck(
        records,
        lambda record: _book() if record.uid == "UID1" else None,
        config=LiquidityPrecheckConfig(
            test_quantity=Decimal("1"),
            max_spread_bps=Decimal("50"),
            max_slippage_bps=Decimal("50"),
            max_enabled=10,
        ),
    )

    by_ticker = {record.ticker: record for record in checked}
    assert by_ticker["GOOD"].enabled is True
    assert by_ticker["GOOD"].market_data_available is True
    assert by_ticker["GOOD"].top5_depth == Decimal("55")
    assert by_ticker["GOOD"].top10_depth == Decimal("55")
    assert by_ticker["EMPTY"].enabled is False
    assert "market_data_unavailable" in by_ticker["EMPTY"].notes


def test_universe_config_writer_preserves_required_fields(tmp_path: Path) -> None:
    checked = apply_liquidity_precheck(
        (_record("GOOD", "UID1"),),
        lambda _record: _book(),
        config=LiquidityPrecheckConfig(test_quantity=Decimal("1"), max_enabled=1),
    )
    path = tmp_path / "neoassets_universe.yaml"

    write_neoassets_universe(path, checked)

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    instrument = payload["instruments"][0]
    assert instrument["ticker"] == "GOOD"
    assert instrument["uid"] == "UID1"
    assert instrument["figi"] == "FIGI-GOOD"
    assert instrument["class_code"] == "SPBX"
    assert instrument["name"] == "GOOD neoasset"
    assert instrument["instrument_type"] == "indicative"
    assert instrument["exchange"] == "spb"
    assert instrument["currency"] == "rub"
    assert instrument["api_trade_available_flag"] is True
    assert instrument["lot"] == 1
    assert instrument["price_increment"] == "0.01"
    assert instrument["enabled"] is True
    assert isinstance(instrument["notes"], list)


def test_discovery_tools_do_not_import_execution_modules() -> None:
    paths = (
        Path("scripts/discover_neoassets.py"),
        Path("neo_trader/research/neoassets.py"),
    )
    violations: list[str] = []
    for path in paths:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                violations.extend(
                    alias.name
                    for alias in node.names
                    if alias.name.startswith("neo_trader.execution")
                )
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module.startswith("neo_trader.execution"):
                    violations.append(module)

    assert violations == []


def _record(ticker: str, uid: str) -> NeoAssetRecord:
    return NeoAssetRecord(
        ticker=ticker,
        uid=uid,
        figi=f"FIGI-{ticker}",
        class_code="SPBX",
        name=f"{ticker} neoasset",
        instrument_type="indicative",
        exchange="spb",
        currency="rub",
        api_trade_available_flag=True,
        lot=1,
        price_increment=Decimal("0.01"),
        notes=("test",),
    )


def _book() -> dict[str, object]:
    return {
        "bids": [
            {"price": "99.99", "quantity": "10"},
            {"price": "99.98", "quantity": "9"},
            {"price": "99.97", "quantity": "8"},
        ],
        "asks": [
            {"price": "100.01", "quantity": "10"},
            {"price": "100.02", "quantity": "9"},
            {"price": "100.03", "quantity": "9"},
        ],
    }
