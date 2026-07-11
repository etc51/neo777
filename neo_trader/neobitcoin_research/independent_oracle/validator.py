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
from typing import Any, Final

import pyarrow as pa  # type: ignore[import-untyped]

from .contracts import PRICE_LEVELS, TRADE_WINDOWS, ContractViolation, validate_data_contracts

EPS: Final = 1e-9
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


class IndependentOracleValidator:
    """Recalculate published values solely from tables supplied by the caller."""

    def __init__(self, *, tick_size: float, require_all_contracts: bool = True) -> None:
        if tick_size <= 0:
            raise ValueError("tick_size must be positive")
        self.tick_size = float(tick_size)
        self.require_all_contracts = require_all_contracts

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

    def _features(
        self,
        rows: Mapping[str, list[dict[str, Any]]],
        books: Sequence[Mapping[str, Any]],
        trades: Sequence[Mapping[str, Any]],
        report: OracleReport,
    ) -> None:
        features = rows.get("feature_snapshots", [])
        depth_match = imbalance_match = flow_match = candle_match = age_match = 0
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
            for interval in ("1m", "5m", "15m"):
                candles = [
                    item
                    for item in raw_by_name.get(f"candles_{interval}", [])
                    if bool(item.get("is_complete", True))
                    and _utc(item.get("candle_end") or item["exchange_ts"]) <= ts
                ]
                selected = self._asof(candles, ts, processing) if candles else None
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
                source_groups[f"candle_{interval}"] = candles
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
        }

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
    tables: Mapping[str, pa.Table], *, tick_size: float, require_all_contracts: bool = True
) -> dict[str, Any]:
    return (
        IndependentOracleValidator(tick_size=tick_size, require_all_contracts=require_all_contracts)
        .validate(tables)
        .as_dict()
    )


__all__ = ["IndependentOracleValidator", "OracleReport", "validate_independent_oracle"]
