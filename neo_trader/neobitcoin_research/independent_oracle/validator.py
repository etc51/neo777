"""Independent mathematical oracle for schema-v4 review tables.

This package intentionally reimplements all selection and arithmetic.  It must
never import a materializer, simulator, production point-in-time helper, or a
production formula module.
"""

from __future__ import annotations

import math
import re
from bisect import bisect_right
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Final

import pyarrow as pa  # type: ignore[import-untyped]

from .contracts import PRICE_LEVELS, TRADE_WINDOWS, ContractViolation, validate_data_contracts

EPS: Final = 1e-9
GRID_EPS: Final = Decimal("1e-9")
CANDLE_STALE_MS: Final = {"1m": 120_000.0, "5m": 600_000.0, "15m": 1_800_000.0}
_SECRET = re.compile(
    r"(?i)(?:authorization\s*[:=]|bearer\s+|api[_-]?token\s*[:=]|"
    r"-----BEGIN (?:RSA |EC )?PRIVATE KEY-----|\bt\.[A-Za-z0-9_-]{20,})"
)


@dataclass
class OracleReport:
    status: str = "PASS"
    violations: list[ContractViolation] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    def add(
        self,
        code: str,
        dataset: str,
        detail: str,
        row: int | None = None,
        field_name: str | None = None,
    ) -> None:
        self.violations.append(ContractViolation(code, dataset, detail, row, field_name))
        self.status = "FAIL"

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "violations": [item.__dict__ for item in self.violations],
            "metrics": self.metrics,
        }


def _utc(value: Any) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"timestamp must be timezone-aware: {value!r}")
    return value.astimezone(UTC)


def _seq(row: Mapping[str, Any]) -> int:
    return int(row.get("sequence") or row.get("revision") or 0)


def _key(row: Mapping[str, Any]) -> tuple[datetime, int, datetime, str]:
    return (_utc(row["exchange_ts"]), _seq(row), _utc(row["receive_ts"]), str(row["event_id"]))


def _close(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is right
    return math.isclose(float(left), float(right), rel_tol=0.0, abs_tol=EPS)


def _contains_secret(value: Any) -> bool:
    if isinstance(value, str):
        return bool(_SECRET.search(value))
    if isinstance(value, Mapping):
        return any(_contains_secret(key) or _contains_secret(item) for key, item in value.items())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return any(_contains_secret(item) for item in value)
    return False


def _levels(row: Mapping[str, Any], side: str) -> list[tuple[float, float]]:
    result: list[tuple[float, float]] = []
    for item in row.get(side) or []:
        if isinstance(item, Mapping):
            result.append((float(item["price"]), float(item["quantity"])))
        else:
            result.append((float(item[0]), float(item[1])))
    return result


def _candle_start(row: Mapping[str, Any], interval: str) -> datetime:
    value = row.get("candle_start") or row.get("exchange_ts")
    return _utc(value)


def _candle_end(row: Mapping[str, Any]) -> datetime:
    return _utc(row.get("candle_end") or row["exchange_ts"])


def _revision_key(row: Mapping[str, Any]) -> tuple[int, datetime, str]:
    return (_seq(row), _utc(row["receive_ts"]), str(row["event_id"]))


def _candle_group(row: Mapping[str, Any], interval: str) -> tuple[str, str, datetime, datetime]:
    return (
        str(row.get("instrument_uid") or row.get("instrument_id") or row.get("figi") or ""),
        str(row.get("timeframe") or row.get("interval") or interval),
        _candle_start(row, interval),
        _candle_end(row),
    )


def _completion_reason(source_complete: bool, interval_complete: bool) -> str:
    if source_complete and interval_complete:
        return "source_flag_and_interval_elapsed"
    if source_complete:
        return "source_flag"
    if interval_complete:
        return "interval_elapsed"
    return "not_complete"


class IndependentOracleValidator:
    """Recalculate published values solely from tables supplied by the caller."""

    def __init__(
        self,
        *,
        tick_size: float,
        require_all_contracts: bool = True,
        candle_warmup: int = 2,
        atr_period: int = 2,
        spread_threshold_ticks: int = 3,
        candle_stale_ms: Mapping[str, float] | None = None,
        max_source_age_ms: float = 30_000.0,
    ) -> None:
        if tick_size <= 0 or candle_warmup <= 0 or atr_period <= 0:
            raise ValueError("tick_size, candle_warmup, and atr_period must be positive")
        self.tick_size = float(tick_size)
        self.require_all_contracts = require_all_contracts
        self.candle_warmup = int(candle_warmup)
        self.atr_period = int(atr_period)
        self.spread_threshold_ticks = int(spread_threshold_ticks)
        self.candle_stale_ms = dict(CANDLE_STALE_MS if candle_stale_ms is None else candle_stale_ms)
        self.max_source_age_ms = float(max_source_age_ms)

    def validate(self, tables: Mapping[str, pa.Table]) -> OracleReport:
        report = OracleReport()
        report.violations.extend(
            validate_data_contracts(tables, require_all=self.require_all_contracts)
        )
        if report.violations:
            report.status = "FAIL"
        rows = {name: table.to_pylist() for name, table in tables.items()}
        books = sorted(rows.get("raw_orderbook", []), key=_key)
        trades = sorted(rows.get("raw_trades", []), key=_key)
        self._books(books, report)
        self._features(rows, books, trades, report)
        self._spread_filters(rows, report)
        self._executions(rows, books, trades, report)
        self._outcomes(rows, books, report)
        self._stops(rows, books, report)
        self._exits(rows, books, report)
        self._secrets(rows, report)
        report.metrics["foreign_keys"] = {
            "violations": sum(item.code == "foreign_key" for item in report.violations)
        }
        report.metrics["unexplained_mismatches"] = len(report.violations)
        return report

    def _books(self, books: Sequence[Mapping[str, Any]], report: OracleReport) -> None:
        matched = 0
        for index, row in enumerate(books):
            bids, asks = _levels(row, "bids"), _levels(row, "asks")
            if not bids or not asks:
                report.add(
                    "empty_book_side", "raw_orderbook", "bids and asks must be nonempty", index
                )
                continue
            if any(q < 0 for _, q in bids + asks):
                report.add("negative_book_quantity", "raw_orderbook", "quantity < 0", index)
            if any(left[0] <= right[0] for left, right in zip(bids, bids[1:], strict=False)):
                report.add(
                    "unsorted_bids",
                    "raw_orderbook",
                    "bid prices are not strictly descending",
                    index,
                )
            if any(left[0] >= right[0] for left, right in zip(asks, asks[1:], strict=False)):
                report.add(
                    "unsorted_asks", "raw_orderbook", "ask prices are not strictly ascending", index
                )
            if not _close(row.get("best_bid"), bids[0][0]) or not _close(
                row.get("best_ask"), asks[0][0]
            ):
                report.add(
                    "best_price_mismatch",
                    "raw_orderbook",
                    "best price differs from first level",
                    index,
                )
            if bids[0][0] >= asks[0][0] and "crossed_book" not in set(
                row.get("data_quality_flags") or []
            ):
                report.add(
                    "unflagged_crossed_book",
                    "raw_orderbook",
                    "crossed book lacks quality flag",
                    index,
                )
            spread = asks[0][0] - bids[0][0]
            if row.get("spread_price") is not None and not _close(row["spread_price"], spread):
                report.add("spread_mismatch", "raw_orderbook", "spread != best_ask-best_bid", index)
            matched += 1
        report.metrics["raw_orderbook"] = {"rows": len(books), "structurally_reproduced": matched}

    @staticmethod
    def _available(row: Mapping[str, Any], feature_ts: datetime, processing_ts: datetime) -> bool:
        return (
            _utc(row["exchange_ts"]) <= feature_ts
            and _utc(row["receive_ts"]) <= processing_ts
            and _utc(row["processing_ts"]) <= processing_ts
        )

    def _asof(
        self, events: Sequence[Mapping[str, Any]], feature_ts: datetime, processing_ts: datetime
    ) -> Mapping[str, Any] | None:
        eligible = [row for row in events if self._available(row, feature_ts, processing_ts)]
        return max(eligible, key=_key) if eligible else None

    @staticmethod
    def _canonical_candle_history(
        candles: Sequence[Mapping[str, Any]],
        interval: str,
        feature_ts: datetime,
        processing_ts: datetime,
    ) -> list[Mapping[str, Any]]:
        """Latest available revision per completed interval, with no production dependency."""
        revisions: dict[tuple[str, str, datetime, datetime], Mapping[str, Any]] = {}
        for row in candles:
            if _utc(row["receive_ts"]) > processing_ts:
                continue
            if (
                isinstance(row.get("processing_ts"), datetime)
                and _utc(row["processing_ts"]) > processing_ts
            ):
                continue
            if _candle_end(row) > feature_ts:
                continue
            group = _candle_group(row, interval)
            prior = revisions.get(group)
            if prior is None or _revision_key(row) > _revision_key(prior):
                revisions[group] = row
        return sorted(revisions.values(), key=lambda row: (_candle_end(row), _revision_key(row)))

    def _spread_values(self, bid: float, ask: float) -> dict[str, Any]:
        spread = Decimal(str(ask)) - Decimal(str(bid))
        tick = Decimal(str(self.tick_size))
        decimal_ticks = spread / tick
        integer_ticks = int(decimal_ticks.to_integral_value(rounding=ROUND_HALF_UP))
        error = abs(decimal_ticks - Decimal(integer_ticks))
        return {
            "spread_price": float(spread),
            "tick_size": float(tick),
            "spread_ticks_decimal": float(decimal_ticks),
            "spread_ticks_int": integer_ticks,
            "tick_grid_error": float(error),
            "on_grid": error <= GRID_EPS,
        }

    @staticmethod
    def _same_ofi_stream(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
        instrument_left = left.get("instrument_id") or left.get("figi")
        instrument_right = right.get("instrument_id") or right.get("figi")
        if instrument_left != instrument_right:
            return False
        session_left = left.get("session_id") or left.get("connection_id")
        session_right = right.get("session_id") or right.get("connection_id")
        return session_left == session_right

    @staticmethod
    def _is_ofi_reset(row: Mapping[str, Any]) -> bool:
        flags = {str(value).lower() for value in row.get("data_quality_flags") or []}
        return bool(
            row.get("is_reset")
            or row.get("connection_reset")
            or row.get("reconnect")
            or row.get("reset_reason")
            or flags & {"reset", "reconnect", "connection_reset"}
        )

    def _previous_ofi_book(
        self,
        books: Sequence[Mapping[str, Any]],
        current: Mapping[str, Any],
        processing_ts: datetime,
        status_events: Sequence[Mapping[str, Any]],
    ) -> tuple[Mapping[str, Any] | None, str | None]:
        prior = [
            row
            for row in books
            if _key(row) < _key(current)
            and _utc(row["receive_ts"]) <= processing_ts
        ]
        if self._is_ofi_reset(current):
            return None, str(current.get("reset_reason") or "reset_or_reconnect")
        if not prior:
            return None, "no_previous_snapshot"
        candidate = max(prior, key=_key)
        reset_types = {"reconnect", "disconnect", "collector_stop", "stream_reset"}
        for status in status_events:
            if not (_key(candidate) < _key(status) < _key(current)):
                continue
            value = str(
                status.get("event_type")
                or status.get("trading_status")
                or status.get("status")
                or ""
            ).lower()
            if value in reset_types:
                return None, value
        if not self._same_ofi_stream(candidate, current):
            return None, "session_changed"
        if self._is_ofi_reset(candidate):
            return None, "previous_snapshot_reset"
        return candidate, None

    def _features(
        self,
        rows: Mapping[str, list[dict[str, Any]]],
        books: Sequence[Mapping[str, Any]],
        trades: Sequence[Mapping[str, Any]],
        report: OracleReport,
    ) -> None:
        features = rows.get("feature_snapshots", [])
        depth_match = imbalance_match = flow_match = candle_match = age_match = 0
        candle_value_match = atr_match = classification_match = ofi_match = spread_match = 0
        raw_by_name = {
            name: value
            for name, value in rows.items()
            if name in {"raw_last_price", "candles_1m", "candles_5m", "candles_15m"}
        }
        for index, feature in enumerate(features):
            ts = _utc(feature.get("feature_ts") or feature["exchange_ts"])
            processing = _utc(feature.get("feature_processing_ts") or feature["processing_ts"])
            source = self._asof(books, ts, processing)
            if source is None:
                if feature.get("feature_ready"):
                    report.add(
                        "ready_without_orderbook",
                        "feature_snapshots",
                        "no point-in-time book",
                        index,
                    )
                continue
            if feature.get("orderbook_source_event_id") != source.get("event_id"):
                report.add(
                    "orderbook_lineage_mismatch",
                    "feature_snapshots",
                    "not the latest available book",
                    index,
                )
            bids, asks = _levels(source, "bids"), _levels(source, "asks")
            ready_missing = False
            for level in range(1, 21):
                for side, levels in (("bid", bids), ("ask", asks)):
                    expected_price, expected_quantity = (
                        levels[level - 1] if len(levels) >= level else (None, None)
                    )
                    for kind, expected in (
                        ("price", expected_price),
                        ("quantity", expected_quantity),
                    ):
                        column = f"{side}_{kind}_{level:02d}"
                        if not _close(feature.get(column), expected):
                            report.add(
                                "book_level_mismatch",
                                "feature_snapshots",
                                f"{column}: expected {expected!r}",
                                index,
                                column,
                            )
                        if (
                            feature.get("feature_ready")
                            and expected is not None
                            and feature.get(column) is None
                        ):
                            ready_missing = True
            if ready_missing:
                report.add(
                    "ready_with_missing_required",
                    "feature_snapshots",
                    "20-level feature has nulls",
                    index,
                )
            depth_row_ok = imbalance_row_ok = True
            for n in PRICE_LEVELS:
                bid_depth, ask_depth = sum(q for _, q in bids[:n]), sum(q for _, q in asks[:n])
                denominator = bid_depth + ask_depth
                imbalance = (bid_depth - ask_depth) / denominator if denominator else None
                depth_row_ok &= _close(feature.get(f"bid_depth_{n}"), bid_depth) and _close(
                    feature.get(f"ask_depth_{n}"), ask_depth
                )
                imbalance_row_ok &= _close(feature.get(f"imbalance_{n}"), imbalance)
            if not depth_row_ok:
                report.add(
                    "depth_mismatch", "feature_snapshots", "depth does not sum quantities", index
                )
            else:
                depth_match += 1
            if not imbalance_row_ok:
                report.add(
                    "imbalance_mismatch", "feature_snapshots", "imbalance formula differs", index
                )
            else:
                imbalance_match += 1
            top_bid_q = bids[0][1] if bids else None
            top_ask_q = asks[0][1] if asks else None
            expected_ofi = (
                top_bid_q - top_ask_q
                if top_bid_q is not None and top_ask_q is not None
                else None
            )
            previous_book, reset_reason = self._previous_ofi_book(
                books, source, processing, rows.get("market_status_events", [])
            )
            previous_ofi = None
            if previous_book is not None:
                previous_bids = _levels(previous_book, "bids")
                previous_asks = _levels(previous_book, "asks")
                if previous_bids and previous_asks:
                    previous_ofi = previous_bids[0][1] - previous_asks[0][1]
            expected_delta = (
                expected_ofi - previous_ofi
                if expected_ofi is not None and previous_ofi is not None
                else None
            )
            ofi_expected = {
                "ofi": expected_ofi,
                "ofi_delta": expected_delta,
                "ofi_source_event_id": source.get("event_id"),
                "ofi_previous_event_id": previous_book.get("event_id") if previous_book else None,
                "ofi_continuity_valid": previous_book is not None,
                "ofi_reset_reason": reset_reason,
            }
            ofi_ok = all(
                column not in feature or (
                    _close(feature.get(column), value)
                    if column in {"ofi", "ofi_delta"}
                    else feature.get(column) == value
                )
                for column, value in ofi_expected.items()
            )
            if not ofi_ok:
                report.add(
                    "ofi_continuity_mismatch", "feature_snapshots",
                    "OFI lineage/delta differs from previous valid snapshot", index,
                )
            else:
                ofi_match += 1
            if bids and asks:
                spread_values = self._spread_values(bids[0][0], asks[0][0])
                threshold = int(
                    feature.get("spread_threshold_ticks") or self.spread_threshold_ticks
                )
                expected_gate = spread_values["spread_ticks_int"] <= threshold
                expected_spread_fields = {
                    **spread_values,
                    "spread": spread_values["spread_price"],
                    "spread_threshold_ticks": threshold,
                    "spread_gate_passed": expected_gate,
                }
                spread_ok = True
                for column, value in expected_spread_fields.items():
                    if column == "on_grid" or column not in feature:
                        continue
                    actual = feature.get(column)
                    equal = _close(actual, value) if isinstance(value, float) else actual == value
                    if not equal:
                        spread_ok = False
                        report.add(
                            "spread_ticks_mismatch", "feature_snapshots",
                            f"{column}: expected {value!r}", index, column,
                        )
                flags_set = {str(value) for value in feature.get("data_quality_flags") or []}
                if not spread_values["on_grid"] and not (
                    {"off_tick_grid", "tick_grid_error"} & flags_set
                ):
                    spread_ok = False
                    report.add(
                        "unflagged_tick_grid_error", "feature_snapshots",
                        "price spread is not an integral tick count", index, "tick_grid_error",
                    )
                if spread_ok:
                    spread_match += 1
            flow_row_ok = True
            for seconds in TRADE_WINDOWS:
                lower = ts - timedelta(seconds=seconds)
                window = [item for item in trades if lower < _utc(item["exchange_ts"]) <= ts]
                buy = sum(
                    float(item["quantity"])
                    for item in window
                    if str(item.get("aggressor_side")).upper() == "BUY"
                )
                sell = sum(
                    float(item["quantity"])
                    for item in window
                    if str(item.get("aggressor_side")).upper() == "SELL"
                )
                unknown = sum(
                    float(item["quantity"])
                    for item in window
                    if str(item.get("aggressor_side")).upper() not in {"BUY", "SELL"}
                )
                expected_fields = {
                    f"trade_flow_{seconds}s": buy - sell,
                    f"trade_count_{seconds}s": len(window),
                    f"buy_volume_{seconds}s": buy,
                    f"sell_volume_{seconds}s": sell,
                    f"unknown_side_volume_{seconds}s": unknown,
                }
                flow_row_ok &= all(
                    column not in feature or _close(feature.get(column), value)
                    for column, value in expected_fields.items()
                )
            if not flow_row_ok:
                report.add(
                    "trade_flow_mismatch",
                    "feature_snapshots",
                    "trailing event-time flow differs",
                    index,
                )
            else:
                flow_match += 1
            source_groups = {
                "orderbook": books,
                "last_price": raw_by_name.get("raw_last_price", []),
            }
            candle_histories: dict[str, list[Mapping[str, Any]]] = {}
            expected_atrs: dict[str, float | None] = {}
            expected_candle_stale: dict[str, bool] = {}
            for interval in ("1m", "5m", "15m"):
                candles = self._canonical_candle_history(
                    raw_by_name.get(f"candles_{interval}", []), interval, ts, processing
                )
                candle_histories[interval] = candles
                selected = candles[-1] if candles else None
                expected_id = selected.get("event_id") if selected else None
                if feature.get(f"candle_{interval}_source_event_id") != expected_id:
                    report.add(
                        "candle_source_mismatch",
                        "feature_snapshots",
                        f"{interval} source is not latest completed candle",
                        index,
                    )
                else:
                    candle_match += 1
                source_groups[f"candle_{interval}"] = [selected] if selected else []
                if selected is None:
                    continue
                source_complete = bool(selected.get("is_complete", False))
                canonical_complete = (
                    _candle_end(selected) <= ts and _utc(selected["receive_ts"]) <= processing
                )
                interval_age = (ts - _candle_end(selected)).total_seconds() * 1000
                receive_age = (processing - _utc(selected["receive_ts"])).total_seconds() * 1000
                stale = interval_age >= self.candle_stale_ms[interval]
                expected_candle_stale[interval] = stale
                lineage_expected = {
                    f"candle_{interval}_start": selected.get("candle_start"),
                    f"candle_{interval}_end": _candle_end(selected),
                    f"candle_{interval}_revision": _seq(selected),
                    f"candle_{interval}_source_is_complete": source_complete,
                    f"candle_{interval}_canonical_is_complete": canonical_complete,
                    f"candle_{interval}_completion_reason": _completion_reason(
                        source_complete, _candle_end(selected) <= ts
                    ),
                    f"candle_{interval}_revision_receive_ts": _utc(selected["receive_ts"]),
                    f"candle_{interval}_interval_age_ms": interval_age,
                    f"candle_{interval}_receive_age_ms": receive_age,
                    f"candle_{interval}_stale": stale,
                }
                lineage_ok = True
                for column, value in lineage_expected.items():
                    if column not in feature:
                        continue
                    actual = feature.get(column)
                    equal = _close(actual, value) if isinstance(value, float) else actual == value
                    if not equal:
                        lineage_ok = False
                        report.add(
                            "candle_lineage_mismatch", "feature_snapshots",
                            f"{column}: expected {value!r}", index, column,
                        )
                expected_volume = float(selected["volume"])
                expected_return = None
                if len(candles) >= 2:
                    prior_close = float(candles[-2]["close"])
                    expected_return = (
                        float(selected["close"]) / prior_close - 1 if prior_close else None
                    )
                expected_atr = None
                if len(candles) >= self.atr_period + 1:
                    true_ranges: list[float] = []
                    first_position = len(candles) - self.atr_period
                    for position in range(first_position, len(candles)):
                        item = candles[position]
                        previous_close = float(candles[position - 1]["close"])
                        high, low = float(item["high"]), float(item["low"])
                        true_ranges.append(
                            max(high - low, abs(high - previous_close), abs(low - previous_close))
                        )
                    expected_atr = sum(true_ranges) / len(true_ranges)
                expected_atrs[interval] = expected_atr
                expected_warmup = max(0, self.atr_period + 1 - len(candles))
                warmup_field = f"atr_{interval}_warmup_remaining"
                if warmup_field in feature and feature.get(warmup_field) != expected_warmup:
                    report.add(
                        "candle_atr_warmup_mismatch", "feature_snapshots",
                        f"expected {expected_warmup}", index, warmup_field,
                    )
                values_ok = _close(
                    feature.get(f"candle_volume_{interval}"), expected_volume
                ) and _close(feature.get(f"return_{interval}"), expected_return)
                if not values_ok:
                    report.add(
                        "candle_value_mismatch", "feature_snapshots",
                        f"{interval} volume/return does not reproduce raw revision", index,
                    )
                elif lineage_ok:
                    candle_value_match += 1
                if not _close(feature.get(f"atr_{interval}"), expected_atr):
                    report.add(
                        "candle_atr_mismatch", "feature_snapshots",
                        f"{interval} ATR differs from point-in-time candles", index,
                        f"atr_{interval}",
                    )
                else:
                    atr_match += 1
            last_trade = [item for item in trades]
            source_groups["last_trade"] = last_trade
            for prefix, events in source_groups.items():
                selected = self._asof(events, ts, processing) if events else None
                id_column, age_column = f"{prefix}_source_event_id", f"{prefix}_receive_age_ms"
                if id_column not in feature and age_column not in feature:
                    continue
                expected_id = selected.get("event_id") if selected else None
                expected_age = (
                    (processing - _utc(selected["receive_ts"])).total_seconds() * 1000
                    if selected
                    else None
                )
                if feature.get(id_column) != expected_id or not _close(
                    feature.get(age_column), expected_age
                ):
                    report.add(
                        "source_age_mismatch", "feature_snapshots", prefix, index, age_column
                    )
                else:
                    age_match += 1
            one_minute = candle_histories.get("1m", [])
            if len(one_minute) < self.candle_warmup:
                expected_trend = expected_regime = "insufficient_history"
                expected_trend_score = None
            else:
                first_close = float(one_minute[-self.candle_warmup]["close"])
                last_close = float(one_minute[-1]["close"])
                expected_trend_score = last_close / first_close - 1 if first_close else 0.0
                expected_trend = (
                    "up" if expected_trend_score > 0.001
                    else "down" if expected_trend_score < -0.001
                    else "range"
                )
                atr_1m = expected_atrs.get("1m") or 0.0
                expected_regime = (
                    "high_volatility"
                    if last_close and atr_1m / last_close > 0.005
                    else expected_trend
                )
            classification_ok = (
                feature.get("trend") == expected_trend
                and feature.get("regime") == expected_regime
                and (
                    "trend_score" not in feature
                    or _close(feature.get("trend_score"), expected_trend_score)
                )
            )
            if not classification_ok:
                report.add(
                    "classification_mismatch", "feature_snapshots",
                    f"expected trend={expected_trend}, regime={expected_regime}", index,
                )
            else:
                classification_match += 1

            if feature.get("schema_version") == "schema-v4.1" or any(
                name in feature
                for name in ("readiness_version", "readiness_checks_total", "ofi_continuity_valid")
            ):
                mandatory: list[str] = [
                    "best_bid", "best_ask", "spread_ticks_int", "microprice", "ofi", "ofi_delta",
                    "imbalance_5", "last_price", "last_trade_source_event_id",
                    "last_price_source_event_id",
                ]
                for side in ("bid", "ask"):
                    for level in range(1, 21):
                        mandatory.extend(
                            (f"{side}_price_{level:02d}", f"{side}_quantity_{level:02d}")
                        )
                for seconds in TRADE_WINDOWS:
                    mandatory.extend((f"trade_count_{seconds}s", f"trade_flow_{seconds}s"))
                for interval in ("1m", "5m", "15m"):
                    mandatory.extend(
                        (
                            f"candle_{interval}_source_event_id",
                            f"candle_{interval}_canonical_is_complete",
                            f"return_{interval}",
                            f"candle_volume_{interval}",
                            f"atr_{interval}",
                        )
                    )
                mandatory.extend(("trend", "regime"))
                expected_missing = sorted(name for name in mandatory if feature.get(name) is None)
                stale_intervals = [
                    interval for interval in ("1m", "5m", "15m")
                    if expected_candle_stale.get(interval, True)
                ]
                last_trade_source = self._asof(trades, ts, processing) if trades else None
                last_price_events = raw_by_name.get("raw_last_price", [])
                last_price_source = (
                    self._asof(last_price_events, ts, processing) if last_price_events else None
                )
                source_ages = [
                    (processing - _utc(item["receive_ts"])).total_seconds() * 1000
                    for item in (source, last_trade_source, last_price_source)
                    if item is not None
                ]
                stale_source = any(value > self.max_source_age_ms for value in source_ages)
                expected_flags: set[str] = set()
                if any(quantity < 0 for _, quantity in bids + asks):
                    expected_flags.add("negative_quantity")
                if any(bids[pos][0] <= bids[pos + 1][0] for pos in range(len(bids) - 1)):
                    expected_flags.add("bids_not_descending")
                if any(asks[pos][0] >= asks[pos + 1][0] for pos in range(len(asks) - 1)):
                    expected_flags.add("asks_not_ascending")
                if not bids or not asks:
                    expected_flags.add("empty_book_side")
                elif bids[0][0] >= asks[0][0]:
                    expected_flags.add("crossed_book")
                if stale_source:
                    expected_flags.add("stale_source")
                if stale_intervals:
                    expected_flags.add("stale_candle")
                spread_on_grid = bool(
                    bids
                    and asks
                    and self._spread_values(bids[0][0], asks[0][0])["on_grid"]
                )
                if not spread_on_grid:
                    expected_flags.add("off_tick_grid")
                actual_flags = {str(value) for value in feature.get("data_quality_flags") or []}
                if actual_flags != expected_flags:
                    report.add(
                        "readiness_quality_flags_mismatch", "feature_snapshots",
                        f"expected {sorted(expected_flags)!r}, got {sorted(actual_flags)!r}", index,
                        "data_quality_flags",
                    )
                expected_ready = (
                    not expected_missing and not stale_source and not stale_intervals
                    and not expected_flags and spread_on_grid
                )
                actual_missing = sorted(str(value) for value in feature.get("missing_fields") or [])
                if actual_missing != expected_missing:
                    report.add(
                        "readiness_missing_fields_mismatch", "feature_snapshots",
                        f"expected {expected_missing!r}, got {actual_missing!r}", index,
                        "missing_fields",
                    )
                if bool(feature.get("feature_ready")) != expected_ready:
                    report.add(
                        "feature_readiness_mismatch", "feature_snapshots",
                        f"expected feature_ready={expected_ready}", index, "feature_ready",
                    )
                total = len(mandatory) + 4
                failed_extra = (
                    int(stale_source) + int(bool(stale_intervals))
                    + int(bool(expected_flags)) + int(not spread_on_grid)
                )
                passed = max(0, total - len(expected_missing) - failed_extra)
                if feature.get("readiness_checks_total") != total or feature.get(
                    "readiness_checks_passed"
                ) != passed:
                    report.add(
                        "readiness_check_count_mismatch", "feature_snapshots",
                        f"expected {passed}/{total}", index,
                    )
                if stale_intervals and feature.get("source_status") != "stale_candle":
                    report.add(
                        "stale_candle_status_mismatch", "feature_snapshots",
                        ",".join(stale_intervals), index, "source_status",
                    )
            if feature.get("feature_ready") and (
                feature.get("trend") is None
                or feature.get("regime") in {None, "unknown", "UNKNOWN"}
            ):
                report.add(
                    "ready_without_classification",
                    "feature_snapshots",
                    "trend/regime unavailable without reason",
                    index,
                )
        report.metrics["features"] = {
            "rows": len(features),
            "depth_reproduced": depth_match,
            "imbalance_reproduced": imbalance_match,
            "trade_flow_reproduced": flow_match,
            "candle_sources_reproduced": candle_match,
            "source_ages_reproduced": age_match,
            "candle_values_reproduced": candle_value_match,
            "atr_reproduced": atr_match,
            "classification_reproduced": classification_match,
            "ofi_reproduced": ofi_match,
            "spread_reproduced": spread_match,
        }

    def _spread_filters(
        self, rows: Mapping[str, list[dict[str, Any]]], report: OracleReport
    ) -> None:
        features = {
            str(row.get("feature_snapshot_id")): row
            for row in rows.get("feature_snapshots", [])
        }
        candidates = {
            str(row.get("candidate_id")): row for row in rows.get("candidate_events", [])
        }
        checked = matched = 0
        for index, decision in enumerate(rows.get("filter_decisions", [])):
            if str(decision.get("filter_name") or "").lower() not in {
                "spread", "spread_gate", "max_spread"
            }:
                continue
            checked += 1
            feature_id = decision.get("feature_snapshot_id")
            if feature_id is None:
                candidate = candidates.get(str(decision.get("candidate_id")))
                feature_id = candidate.get("feature_snapshot_id") if candidate else None
            feature = features.get(str(feature_id))
            if feature is None:
                report.add(
                    "spread_filter_lineage_missing", "filter_decisions",
                    "spread filter has no feature snapshot", index,
                )
                continue
            integer_ticks = feature.get("spread_ticks_int")
            threshold = feature.get("spread_threshold_ticks", self.spread_threshold_ticks)
            if integer_ticks is None:
                report.add(
                    "spread_filter_integer_ticks_missing", "filter_decisions",
                    "spread filter cannot use floating ticks", index,
                )
                continue
            expected = int(integer_ticks) <= int(threshold)
            if decision.get("passed") is not expected:
                report.add(
                    "spread_gate_mismatch", "filter_decisions",
                    f"expected {expected} from {integer_ticks} <= {threshold}", index, "passed",
                )
            else:
                matched += 1
        report.metrics["spread_filters"] = {"rows": checked, "reproduced": matched}

    def _executions(
        self,
        rows: Mapping[str, list[dict[str, Any]]],
        books: Sequence[Mapping[str, Any]],
        trades: Sequence[Mapping[str, Any]],
        report: OracleReport,
    ) -> None:
        raw_ids = {str(row["event_id"]) for row in [*books, *trades]}
        books_by_id = {str(row["event_id"]): row for row in books}
        checked = reproduced = 0
        passive_delays: list[float] = []
        passive_sources: set[tuple[str, ...]] = set()
        for index, row in enumerate(rows.get("execution_simulations", [])):
            checked += 1
            model = str(row.get("execution_model") or "")
            requested = float(row.get("requested_quantity") or 0.0)
            filled = float(row.get("filled_quantity") or 0.0)
            remaining = float(row.get("remaining_quantity") or requested - filled)
            expected_status = (
                "FULL" if remaining <= EPS and filled > 0 else "PARTIAL" if filled else "NO_FILL"
            )
            status = {
                "FULL_FILL": "FULL",
                "PARTIAL_FILL": "PARTIAL",
                "NO_FILL": "NO_FILL",
            }.get(str(row.get("fill_status") or ""), str(row.get("fill_status") or ""))
            ok = _close(remaining, requested - filled) and status == expected_status
            lineage = list(row.get("fill_lineage") or [])
            lineage_ids = tuple(str(item.get("source_event_id")) for item in lineage)
            if any(source_id not in raw_ids for source_id in lineage_ids):
                report.add(
                    "fill_source_missing",
                    "execution_simulations",
                    "lineage source is absent",
                    index,
                )
                ok = False
            if lineage and not _close(sum(float(item["quantity"]) for item in lineage), filled):
                report.add(
                    "fill_quantity_mismatch",
                    "execution_simulations",
                    "lineage quantity differs",
                    index,
                )
                ok = False
            if model in {"aggressive", "aggressive_marketable"}:
                source = books_by_id.get(str(row.get("source_orderbook_event_id")))
                if source is None:
                    ok = False
                else:
                    levels = _levels(source, "asks" if row.get("side") == "LONG" else "bids")
                    left, quantity, notional = requested, 0.0, 0.0
                    for price, available in levels:
                        take = min(left, available)
                        quantity += take
                        notional += take * price
                        left -= take
                        if left <= EPS:
                            break
                    vwap = notional / quantity if quantity else None
                    ok &= _close(filled, quantity) and _close(row.get("fill_price"), vwap)
            elif model == "ideal_touch":
                ok &= bool(row.get("is_theoretical")) and expected_status == "FULL"
            elif "passive" in model:
                if row.get("fill_ts") is not None:
                    passive_delays.append(
                        (_utc(row["fill_ts"]) - _utc(row["order_ts"])).total_seconds() * 1000
                    )
                passive_sources.add(lineage_ids)
                ok &= all(
                    source_id in {str(item["event_id"]) for item in trades}
                    for source_id in lineage_ids
                )
            if not ok:
                report.add("fill_mismatch", "execution_simulations", model, index)
            else:
                reproduced += 1
        artificial = (
            len(passive_delays) > 2 and len(set(passive_delays)) == 1 and len(passive_sources) > 1
        )
        if artificial:
            report.add("artificial_passive_delay", "execution_simulations", str(passive_delays[0]))
        report.metrics["executions"] = {
            "rows": checked,
            "reproduced": reproduced,
            "artificial_passive_delay": artificial,
        }

    @staticmethod
    def _quote(row: Mapping[str, Any], side: str) -> float | None:
        value = row.get("best_bid" if side == "LONG" else "best_ask")
        return float(value) if value is not None else None

    def _outcomes(
        self,
        rows: Mapping[str, list[dict[str, Any]]],
        books: Sequence[Mapping[str, Any]],
        report: OracleReport,
    ) -> None:
        by_id = {str(row["event_id"]): row for row in books}
        execution_quantity = {
            str(row.get("simulation_id")): float(row.get("filled_quantity") or 0.0)
            for row in rows.get("execution_simulations", [])
        }
        checked = price_ok = mfe_ok = mae_ok = pnl_ok = 0
        book_times = [_utc(book["exchange_ts"]) for book in books]
        for index, row in enumerate(rows.get("future_outcomes", [])):
            if not row.get("outcome_complete"):
                continue
            checked += 1
            entry_ts, target = _utc(row["entry_ts"]), _utc(row["target_ts"])
            target_pos = bisect_right(book_times, target)
            source = books[target_pos - 1] if target_pos else None
            side, entry = str(row["side"]), float(row["entry_price"])
            executable = self._quote(source, side) if source else None
            if (
                source is None
                or row.get("price_source_event_id") != source.get("event_id")
                or not _close(row.get("executable_exit_price"), executable)
            ):
                report.add(
                    "outcome_price_mismatch",
                    "future_outcomes",
                    "wrong last executable quote",
                    index,
                )
            else:
                price_ok += 1
            entry_pos = bisect_right(book_times, entry_ts)
            path = [
                book for book in books[entry_pos:target_pos] if self._quote(book, side) is not None
            ]
            direction = 1.0 if side == "LONG" else -1.0
            excursions = []
            for book in path:
                quote = self._quote(book, side)
                if quote is not None:
                    excursions.append((direction * (quote - entry) / self.tick_size, book))
            best = max(excursions, key=lambda pair: pair[0]) if excursions else (0.0, None)
            worst = min(excursions, key=lambda pair: pair[0]) if excursions else (0.0, None)
            expected_mfe, expected_mae = max(best[0], 0.0), max(-worst[0], 0.0)
            expected_mfe_id = best[1].get("event_id") if best[1] is not None else None
            expected_mae_id = worst[1].get("event_id") if worst[1] is not None else None
            if _close(row.get("mfe_ticks"), expected_mfe) and (
                "mfe_source_event_id" not in row
                or row.get("mfe_source_event_id") == expected_mfe_id
            ):
                mfe_ok += 1
            else:
                report.add(
                    "mfe_mismatch",
                    "future_outcomes",
                    f"expected {expected_mfe} from {expected_mfe_id}",
                    index,
                )
            if _close(row.get("mae_ticks"), expected_mae) and (
                "mae_source_event_id" not in row
                or row.get("mae_source_event_id") == expected_mae_id
            ):
                mae_ok += 1
            else:
                report.add(
                    "mae_mismatch",
                    "future_outcomes",
                    f"expected {expected_mae} from {expected_mae_id}",
                    index,
                )
            gross = (
                direction
                * ((executable if executable is not None else math.nan) - entry)
                * execution_quantity.get(str(row.get("simulation_id")), 0.0)
            )
            if "gross_pnl" not in row or _close(row.get("gross_pnl"), gross):
                pnl_ok += 1
            else:
                report.add("outcome_pnl_mismatch", "future_outcomes", f"expected {gross}", index)
            for field_name in (
                "price_source_event_id",
                "mfe_source_event_id",
                "mae_source_event_id",
            ):
                if row.get(field_name) is not None and str(row[field_name]) not in by_id:
                    report.add(
                        "source_id_missing", "future_outcomes", field_name, index, field_name
                    )
        report.metrics["outcomes"] = {
            "complete": checked,
            "prices_reproduced": price_ok,
            "pnl_reproduced": pnl_ok,
            "mfe_reproduced": mfe_ok,
            "mae_reproduced": mae_ok,
        }
        fill_times = [
            _utc(row["fill_ts"])
            for row in rows.get("execution_simulations", [])
            if row.get("fill_ts") is not None
        ]
        theoretical = [
            _utc(row["entry_ts"])
            for row in rows.get("future_outcomes", [])
            if row.get("entry_ts") is not None
        ]
        entries = [*fill_times, *theoretical]
        required_end = max(entries) + timedelta(seconds=1800) if entries else None
        deficient: list[str] = []
        for name in ("raw_orderbook", "raw_trades", "raw_last_price"):
            values = rows.get(name, [])
            actual = max((_utc(item["exchange_ts"]) for item in values), default=None)
            if required_end is not None and (actual is None or actual < required_end):
                deficient.append(name)
        if deficient:
            report.add("raw_support_short", "raw", ",".join(deficient))
        report.metrics["raw_support"] = {
            "required_end": required_end.isoformat() if required_end else None,
            "deficient_datasets": deficient,
        }

    def _stops(
        self,
        rows: Mapping[str, list[dict[str, Any]]],
        books: Sequence[Mapping[str, Any]],
        report: OracleReport,
    ) -> None:
        executions = {
            str(row.get("simulation_id")): row for row in rows.get("execution_simulations", [])
        }
        books_by_id = {str(row["event_id"]): row for row in books}
        checked = reproduced = 0
        book_times = [_utc(book["exchange_ts"]) for book in books]
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        for index, row in enumerate(rows.get("shadow_stop_results", [])):
            checked += 1
            grouped.setdefault(str(row.get("simulation_id")), []).append(row)
            execution = executions.get(str(row.get("simulation_id")), {})
            fill_ts = _utc(execution.get("fill_ts") or row["entry_ts"])
            side, level = str(row["side"]), float(row["stop_level"])
            path = books[bisect_right(book_times, fill_ts) :]
            triggers = []
            for book in path:
                quote = self._quote(book, side)
                if quote is not None and (
                    (side == "LONG" and quote <= level) or (side != "LONG" and quote >= level)
                ):
                    triggers.append(book)
            expected = triggers[0] if triggers else None
            actual_hit = bool(row.get("stop_triggered"))
            ok = actual_hit == bool(expected)
            if expected is not None:
                ok &= (
                    row.get("trigger_event_id") == expected.get("event_id")
                    and _utc(row["trigger_ts"]) > fill_ts
                )
            if not ok:
                report.add(
                    "stop_trigger_mismatch",
                    "shadow_stop_results",
                    "not first post-fill crossing",
                    index,
                )
            else:
                reproduced += 1
        unexplained = 0
        for group in grouped.values():
            for pos, left in enumerate(group):
                for right in group[pos + 1 :]:
                    same = (
                        left.get("exit_ts"),
                        left.get("exit_price"),
                        left.get("trigger_event_id"),
                    ) == (
                        right.get("exit_ts"),
                        right.get("exit_price"),
                        right.get("trigger_event_id"),
                    )
                    both_triggered = bool(
                        left.get("stop_triggered", left.get("triggered"))
                    ) and bool(right.get("stop_triggered", right.get("triggered")))
                    if (
                        not same
                        or left.get("stop_level") == right.get("stop_level")
                        or not both_triggered
                    ):
                        continue
                    trigger = books_by_id.get(str(left.get("trigger_event_id")))
                    quote = self._quote(trigger, str(left.get("side"))) if trigger else None
                    long = left.get("side") == "LONG"
                    crosses = quote is not None and all(
                        quote <= float(item["stop_level"])
                        if long
                        else quote >= float(item["stop_level"])
                        for item in (left, right)
                    )
                    explained = (
                        left.get("trigger_event_id") == right.get("trigger_event_id")
                        and bool(left.get("gap_through_stop"))
                        and bool(right.get("gap_through_stop"))
                        and crosses
                    )
                    unexplained += int(not explained)
        if unexplained:
            report.add("unexplained_identical_stops", "shadow_stop_results", str(unexplained))
        report.metrics["stops"] = {
            "rows": checked,
            "reproduced": reproduced,
            "unexplained_identical": unexplained,
        }

    def _exits(
        self,
        rows: Mapping[str, list[dict[str, Any]]],
        books: Sequence[Mapping[str, Any]],
        report: OracleReport,
    ) -> None:
        by_id = {str(row["event_id"]): row for row in books}
        grouped: dict[str, list[Mapping[str, Any]]] = {}
        reproduced = 0
        exits = rows.get("shadow_exit_results", [])
        for index, row in enumerate(exits):
            grouped.setdefault(str(row.get("simulation_id")), []).append(row)
            source = by_id.get(str(row.get("exit_source_event_id")))
            if (
                source is None
                or _utc(row["exit_ts"]) < _utc(row["entry_ts"])
                or _utc(source["exchange_ts"]) != _utc(row["exit_ts"])
            ):
                report.add(
                    "exit_trigger_mismatch",
                    "shadow_exit_results",
                    "missing/invalid exit source",
                    index,
                )
            elif row.get("exit_price") is not None and not _close(
                row["exit_price"], self._quote(source, str(row["side"]))
            ):
                report.add(
                    "exit_price_mismatch", "shadow_exit_results", "not executable side", index
                )
            else:
                reproduced += 1
        identical_models = 0
        for group in grouped.values():
            variants = {str(row.get("exit_variant")) for row in group}
            outcomes = {
                (row.get("exit_ts"), row.get("exit_price"), row.get("exit_reason")) for row in group
            }
            if len(variants) > 1 and len(outcomes) == 1:
                identical_models += 1
        if identical_models:
            report.add("identical_exit_models", "shadow_exit_results", str(identical_models))
        report.metrics["exits"] = {
            "rows": len(exits),
            "reproduced": reproduced,
            "identical_model_groups": identical_models,
        }

    def _secrets(self, rows: Mapping[str, list[dict[str, Any]]], report: OracleReport) -> None:
        hits = 0
        for dataset, values in rows.items():
            for index, row in enumerate(values):
                for name, value in row.items():
                    if _contains_secret(value):
                        hits += 1
                        report.add(
                            "secret_detected", dataset, f"secret-like value in {name}", index, name
                        )
        report.metrics["secrets"] = {"hits": hits}


def validate_independent_oracle(
    tables: Mapping[str, pa.Table],
    *,
    tick_size: float,
    require_all_contracts: bool = True,
    candle_warmup: int = 2,
    atr_period: int = 2,
    spread_threshold_ticks: int = 3,
    candle_stale_ms: Mapping[str, float] | None = None,
    max_source_age_ms: float = 30_000.0,
) -> dict[str, Any]:
    return (
        IndependentOracleValidator(
            tick_size=tick_size,
            require_all_contracts=require_all_contracts,
            candle_warmup=candle_warmup,
            atr_period=atr_period,
            spread_threshold_ticks=spread_threshold_ticks,
            candle_stale_ms=candle_stale_ms,
            max_source_age_ms=max_source_age_ms,
        )
        .validate(tables)
        .as_dict()
    )


__all__ = ["IndependentOracleValidator", "OracleReport", "validate_independent_oracle"]
