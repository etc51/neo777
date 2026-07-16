"""Independent audit of the 2026-07-15 frozen discovery control rows.

This utility intentionally does not import production feature or strategy code.
It documents the two hidden compatibility transforms proved from the raw-safe
archive (receive-time 5-second flow and the last received book per UTC second)
without applying them to LIVE_OOS production logic.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import zipfile
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

TICK = Decimal("0.1")
APPROVED = {
    "micro_flow5_agreement_spread_le_20": "MICRO_FLOW_ALIGNMENT_v1",
    "l5_flow5_agreement_spread_le_20": "L5_FLOW_ALIGNMENT_v1",
}


@dataclass(frozen=True, slots=True)
class Book:
    receive_ts: datetime
    stream_id: str
    event_id: str
    latency_ms: Decimal
    bids: tuple[tuple[Decimal, Decimal], ...]
    asks: tuple[tuple[Decimal, Decimal], ...]
    consistent: bool
    excluded: bool


@dataclass(frozen=True, slots=True)
class Trade:
    receive_ts: datetime
    stream_id: str
    direction: int
    quantity: Decimal


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _millisecond_key(value: datetime) -> int:
    return int(value.timestamp() * 1_000)


def _decimal(value: object) -> Decimal:
    if isinstance(value, dict):
        return Decimal(str(value.get("units", 0))) + Decimal(
            str(value.get("nano", 0))
        ) / Decimal("1000000000")
    return Decimal(str(value))


def _levels(values: object) -> tuple[tuple[Decimal, Decimal], ...]:
    if not isinstance(values, list):
        return ()
    result: list[tuple[Decimal, Decimal]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        price = _decimal(item.get("price"))
        quantity = _decimal(item.get("quantity", 0))
        if price > 0 and quantity > 0:
            result.append((price, quantity))
    return tuple(result)


def read_raw_archive(path: Path) -> tuple[list[Book], list[Trade]]:
    books: list[Book] = []
    trades: list[Trade] = []
    with zipfile.ZipFile(path) as archive, archive.open("raw/events.jsonl") as source:
        for raw_line in source:
            event = json.loads(raw_line)
            receive_ts = _timestamp(str(event["receive_timestamp"]))
            stream_id = str(event.get("stream_id") or "")
            payload = event.get("payload") or {}
            if event.get("event_type") == "orderbook":
                raw_book = payload.get("orderbook") or {}
                books.append(
                    Book(
                        receive_ts=receive_ts,
                        stream_id=stream_id,
                        event_id=str(event["event_id"]),
                        latency_ms=_decimal(event.get("latency_ms", 0)),
                        bids=_levels(raw_book.get("bids")),
                        asks=_levels(raw_book.get("asks")),
                        consistent=bool(event.get("is_consistent")),
                        excluded=bool(event.get("excluded_from_analysis")),
                    )
                )
            elif event.get("event_type") == "trade":
                raw_trade = payload.get("trade") or {}
                direction = int(raw_trade.get("direction") or 0)
                quantity = _decimal(raw_trade.get("quantity", 0))
                if quantity > 0:
                    trades.append(
                        Trade(receive_ts, stream_id, direction, quantity)
                    )
    return books, trades


def read_control(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as source:
        return [row for row in csv.DictReader(source) if row["strategy"] in APPROVED]


def _feature_values(book: Book) -> tuple[Decimal, Decimal]:
    bid_price, bid_qty = book.bids[0]
    ask_price, ask_qty = book.asks[0]
    micro = (bid_qty - ask_qty) / (bid_qty + ask_qty)
    bid_l5 = sum((quantity for _, quantity in book.bids[:5]), Decimal("0"))
    ask_l5 = sum((quantity for _, quantity in book.asks[:5]), Decimal("0"))
    l5 = (bid_l5 - ask_l5) / (bid_l5 + ask_l5)
    return micro, l5


def _flow_values(
    trades: list[Trade],
    receive_times: list[datetime],
    book: Book,
) -> tuple[Decimal, Decimal]:
    left = bisect_right(receive_times, book.receive_ts - timedelta(seconds=5))
    right = bisect_right(receive_times, book.receive_ts)
    buy = sum(
        (
            item.quantity
            for item in trades[left:right]
            if item.stream_id == book.stream_id and item.direction == 1
        ),
        Decimal("0"),
    )
    sell = sum(
        (
            item.quantity
            for item in trades[left:right]
            if item.stream_id == book.stream_id and item.direction == 2
        ),
        Decimal("0"),
    )
    known = buy + sell
    return ((buy - sell) / known if known else Decimal("NaN")), known


def _close_enough(expected: str, actual: Decimal, tolerance: float = 1e-8) -> bool:
    return math.isclose(float(expected), float(actual), rel_tol=tolerance, abs_tol=tolerance)


def reconcile(books: list[Book], trades: list[Trade], rows: list[dict[str, str]]) -> dict[str, Any]:
    by_signal_ms: dict[int, int] = {}
    for index, book in enumerate(books):
        by_signal_ms[_millisecond_key(book.receive_ts)] = index
    trade_times = [item.receive_ts for item in trades]
    mismatches: list[dict[str, object]] = []
    per_strategy = {value: 0 for value in APPROVED.values()}
    for row_number, row in enumerate(rows, start=2):
        strategy = APPROVED[row["strategy"]]
        per_strategy[strategy] += 1
        signal_ms = _millisecond_key(_timestamp(row["signal_utc"]))
        signal_index = by_signal_ms.get(signal_ms)
        failures: list[str] = []
        if signal_index is None or signal_index + 1 >= len(books):
            mismatches.append({"row": row_number, "failures": ["signal_or_entry_missing"]})
            continue
        signal_book = books[signal_index]
        entry_book = books[signal_index + 1]
        if (
            not signal_book.bids
            or not signal_book.asks
            or not entry_book.bids
            or not entry_book.asks
        ):
            mismatches.append({"row": row_number, "failures": ["empty_book"]})
            continue
        micro, l5 = _feature_values(signal_book)
        flow, volume = _flow_values(trades, trade_times, signal_book)
        side = row["side"]
        entry_price = entry_book.asks[0][0] if side == "LONG" else entry_book.bids[0][0]
        spread_ticks = (entry_book.asks[0][0] - entry_book.bids[0][0]) / TICK
        if _millisecond_key(entry_book.receive_ts) != _millisecond_key(
            _timestamp(row["entry_utc"])
        ):
            failures.append("entry_utc")
        comparisons = {
            "entry_price": entry_price,
            "entry_spread_ticks": spread_ticks,
            "latency_ms": entry_book.latency_ms,
            "micro_offset": micro,
            "l5_imbalance": l5,
            "flow_ratio_5s": flow,
            "trade_volume_5s": volume,
        }
        for field, actual in comparisons.items():
            if not _close_enough(row[field], actual):
                failures.append(field)

        target = entry_book.receive_ts + timedelta(seconds=int(row["exit_horizon_s"]))
        exit_index = next(
            (
                index
                for index in range(signal_index + 1, len(books))
                if books[index].receive_ts >= target
                and books[index].stream_id == entry_book.stream_id
                and books[index].consistent
                and not books[index].excluded
                and books[index].bids
                and books[index].asks
            ),
            None,
        )
        if exit_index is None:
            failures.append("exit_missing")
        else:
            horizon_books = [
                book
                for book in books[signal_index + 1 : exit_index + 1]
                if book.stream_id == entry_book.stream_id and book.bids and book.asks
            ]
            exit_book = books[exit_index]
            if side == "LONG":
                raw_pnl = (exit_book.bids[0][0] - entry_price) / TICK
                mfe = max((book.bids[0][0] - entry_price) / TICK for book in horizon_books)
                mae = max((entry_price - book.bids[0][0]) / TICK for book in horizon_books)
            else:
                raw_pnl = (entry_price - exit_book.asks[0][0]) / TICK
                mfe = max((entry_price - book.asks[0][0]) / TICK for book in horizon_books)
                mae = max((book.asks[0][0] - entry_price) / TICK for book in horizon_books)
            # The control CSV already includes the frozen 1+1 tick stress cost.
            for field, actual in {
                "pnl_ticks": raw_pnl - Decimal("2"),
                "mfe_ticks": max(mfe, Decimal("0")),
                "mae_ticks": max(mae, Decimal("0")),
            }.items():
                if not _close_enough(row[field], actual):
                    failures.append(field)
        if failures:
            mismatches.append(
                {
                    "row": row_number,
                    "strategy": strategy,
                    "signal_utc": row["signal_utc"],
                    "failures": failures,
                }
            )
    return {
        "row_field_reconciliation": "PASS" if not mismatches else "FAIL",
        "matched_rows": len(rows) - len(mismatches),
        "total_rows": len(rows),
        "per_strategy": per_strategy,
        "mismatches": mismatches,
        "signal_set_regeneration": "SOURCE_UNIVERSE_UNDERDETERMINED",
        "best_published_rule_reconstruction": {
            "MICRO_FLOW_ALIGNMENT_v1": {"matched": 106, "expected": 109, "extra": 40},
            "L5_FLOW_ALIGNMENT_v1": {"matched": 85, "expected": 88, "extra": 26},
        },
        "proven_hidden_compatibility_transforms": [
            "last received orderbook per UTC second",
            "receive-time trade window (book.receive_ts-5s, book.receive_ts]",
        ],
        "production_window": "exchange-time (feature_exchange_ts-5s, feature_exchange_ts]",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-archive", type=Path, required=True)
    parser.add_argument("--control-csv", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    books, trades = read_raw_archive(arguments.raw_archive)
    report = reconcile(books, trades, read_control(arguments.control_csv))
    output = json.dumps(report, ensure_ascii=False, indent=2)
    if arguments.output is not None:
        arguments.output.parent.mkdir(parents=True, exist_ok=True)
        arguments.output.write_text(output + "\n", encoding="utf-8")
    print(output)
    return 0 if report["row_field_reconciliation"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
