"""Raw-only feature snapshots for schema-v4.1."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any, Final

from .timeline import CanonicalEvent, CanonicalTimeline

LEVELS: Final = (1, 3, 5, 10, 20)
TRADE_WINDOWS: Final = (5, 10, 30, 60, 180, 300, 900)


def _id(prefix: str, *parts: object) -> str:
    body = "|".join(str(part) for part in parts)
    return f"{prefix}_{hashlib.sha256(body.encode()).hexdigest()[:24]}"


def _levels(value: object) -> list[tuple[float, float]]:
    result: list[tuple[float, float]] = []
    if not isinstance(value, Sequence):
        return result
    for level in value:
        if isinstance(level, Mapping):
            result.append((float(level["price"]), float(level["quantity"])))
        elif isinstance(level, Sequence) and len(level) >= 2:
            result.append((float(level[0]), float(level[1])))
    return result


@dataclass(frozen=True, slots=True)
class FeatureConfig:
    tick_size: float
    required_book_levels: int = 20
    candle_warmup: int = 2
    atr_period: int = 2
    atr_required: bool = True
    max_source_age_ms: float = 30_000.0
    candle_1m_max_interval_age_ms: float = 120_000.0
    candle_5m_max_interval_age_ms: float = 600_000.0
    candle_15m_max_interval_age_ms: float = 1_800_000.0
    spread_grid_epsilon: float = 1e-9
    spread_threshold_ticks: int = 3

    def __post_init__(self) -> None:
        if (
            self.tick_size <= 0
            or not 1 <= self.required_book_levels <= 20
            or self.candle_warmup <= 0
            or self.atr_period <= 0
            or self.spread_grid_epsilon < 0
            or self.spread_threshold_ticks < 0
        ):
            raise ValueError("invalid feature configuration")

    def candle_max_age_ms(self, minutes: int) -> float:
        return {
            1: self.candle_1m_max_interval_age_ms,
            5: self.candle_5m_max_interval_age_ms,
            15: self.candle_15m_max_interval_age_ms,
        }[minutes]


class FeatureMaterializer:
    def __init__(
        self, timeline: CanonicalTimeline, config: FeatureConfig, *, materialized_ts: datetime
    ) -> None:
        self.timeline = timeline
        self.config = config
        self.materialized_ts = materialized_ts.astimezone(UTC)

    def materialize(
        self, feature_events: Sequence[CanonicalEvent] | None = None
    ) -> list[dict[str, Any]]:
        books = self.timeline.events("raw_orderbook") if feature_events is None else feature_events
        output: list[dict[str, Any]] = []
        for book in books:
            previous, reset_reason = self._previous_valid_book(book)
            row = self._one(book, previous, reset_reason)
            output.append(row)
        return output

    def _previous_valid_book(
        self, book: CanonicalEvent
    ) -> tuple[tuple[CanonicalEvent, float] | None, str | None]:
        prior: CanonicalEvent | None = None
        for event in self.timeline.events("raw_orderbook"):
            if event.sort_key >= book.sort_key:
                break
            bids = _levels(event.row.get("bids"))
            asks = _levels(event.row.get("asks"))
            if bids and asks:
                prior = event
        if prior is None:
            return None, "no_previous_snapshot"
        # An explicit reconnect/reset between snapshots invalidates continuity,
        # even when a producer accidentally reuses a session identifier.
        reset_types = {"reconnect", "disconnect", "collector_stop", "stream_reset"}
        for status in self.timeline.events("market_status_events"):
            if prior.sort_key < status.sort_key < book.sort_key:
                value = str(
                    status.row.get("event_type")
                    or status.row.get("trading_status")
                    or status.row.get("status")
                    or ""
                ).lower()
                if value in reset_types:
                    return None, value
        bids = _levels(prior.row.get("bids"))
        asks = _levels(prior.row.get("asks"))
        return (prior, bids[0][1] - asks[0][1]), None

    def _one(
        self,
        book: CanonicalEvent,
        previous: tuple[CanonicalEvent, float] | None,
        reset_reason: str | None,
    ) -> dict[str, Any]:
        ts = book.exchange_ts
        cutoff = book.processing_ts
        bids = _levels(book.row.get("bids"))
        asks = _levels(book.row.get("asks"))
        flags: list[str] = []
        if any(q < 0 for _, q in bids + asks):
            flags.append("negative_quantity")
        if any(bids[i][0] <= bids[i + 1][0] for i in range(len(bids) - 1)):
            flags.append("bids_not_descending")
        if any(asks[i][0] >= asks[i + 1][0] for i in range(len(asks) - 1)):
            flags.append("asks_not_ascending")
        if not bids or not asks:
            flags.append("empty_book_side")
        elif bids[0][0] >= asks[0][0]:
            flags.append("crossed_book")
        snapshot_id = _id("feature", book.event_id)
        result: dict[str, Any] = {
            "event_id": snapshot_id,
            "schema_version": "schema-v4.1",
            "feature_snapshot_id": snapshot_id,
            "feature_ts": ts,
            "exchange_ts": ts,
            "receive_ts": book.receive_ts,
            "processing_ts": book.processing_ts,
            "feature_receive_ts": book.receive_ts,
            "feature_processing_ts": cutoff,
            "materialized_ts": self.materialized_ts,
            "orderbook_source_event_id": book.event_id,
            "data_quality_flags": flags,
        }
        for side, values in (("bid", bids), ("ask", asks)):
            for level in range(1, 21):
                price, quantity = values[level - 1] if len(values) >= level else (None, None)
                result[f"{side}_price_{level:02d}"] = price
                result[f"{side}_quantity_{level:02d}"] = quantity
        for level in LEVELS:
            bid_depth = sum(quantity for _, quantity in bids[:level]) if bids else None
            ask_depth = sum(quantity for _, quantity in asks[:level]) if asks else None
            result[f"bid_depth_{level}"] = bid_depth
            result[f"ask_depth_{level}"] = ask_depth
            denominator = None if bid_depth is None or ask_depth is None else bid_depth + ask_depth
            if bid_depth is not None and ask_depth is not None and denominator and denominator > 0:
                result[f"imbalance_{level}"] = (bid_depth - ask_depth) / denominator
            else:
                result[f"imbalance_{level}"] = None
            if denominator == 0:
                flags.append(f"zero_depth_{level}")
        best_bid = bids[0][0] if bids else None
        best_ask = asks[0][0] if asks else None
        result["best_bid"], result["best_ask"] = best_bid, best_ask
        result["spread"] = (
            best_ask - best_bid if best_bid is not None and best_ask is not None else None
        )
        self._spread(result, flags)
        bid_q = bids[0][1] if bids else 0.0
        ask_q = asks[0][1] if asks else 0.0
        top_total = bid_q + ask_q
        result["microprice"] = (
            (best_ask * bid_q + best_bid * ask_q) / top_total
            if top_total and best_bid is not None and best_ask is not None
            else None
        )
        result["ofi"] = bid_q - ask_q if bids and asks else None
        self._ofi(result, book, previous, reset_reason)
        self._trade_flow(result, ts, cutoff)
        sources = self._market_sources(result, ts, cutoff)
        self._candles(result, ts, cutoff, sources)
        self._ages(result, book, ts, sources)
        missing = [key for key in self.required_fields() if result.get(key) is None]
        stale = any(
            result.get(key, 0.0) > self.config.max_source_age_ms
            for key in (
                "orderbook_receive_age_ms",
                "last_trade_receive_age_ms",
                "last_price_receive_age_ms",
            )
            if result.get(key) is not None
        )
        stale_candles = [
            f"{minutes}m"
            for minutes in (1, 5, 15)
            if result.get(f"candle_{minutes}m_stale") is True
        ]
        if stale:
            flags.append("stale_source")
        if stale_candles:
            flags.append("stale_candle")
        if result.get("tick_grid_error") is not None and not result.get("spread_on_tick_grid"):
            flags.append("off_tick_grid")
        result["missing_fields"] = missing
        reasons = list(missing)
        if result.get("ofi_delta") is None:
            reasons.append("ofi_delta_requires_previous_snapshot")
        if stale:
            reasons.append("stale_source")
        if stale_candles:
            reasons.append("stale_candle:" + ",".join(stale_candles))
        if not result.get("spread_on_tick_grid", False):
            reasons.append("spread_off_tick_grid")
        result["missing_reason"] = ",".join(dict.fromkeys(reasons)) or None
        result["warmup_remaining"] = max(
            int(result.get(f"atr_{minutes}m_warmup_remaining") or 0)
            for minutes in (1, 5, 15)
        )
        result["readiness_checks_total"] = len(self.required_fields()) + 4
        failed_extra = int(stale) + int(bool(stale_candles)) + int(bool(flags)) + int(
            not result.get("spread_on_tick_grid", False)
        )
        result["readiness_checks_passed"] = max(
            0, result["readiness_checks_total"] - len(missing) - failed_extra
        )
        result["readiness_version"] = "schema-v4.1"
        result["source_status"] = (
            "READY"
            if not missing and not stale and not stale_candles and not flags
            else "stale_candle"
            if stale_candles
            else "INCOMPLETE"
        )
        result["feature_ready"] = result["source_status"] == "READY"
        return result

    def required_fields(self) -> tuple[str, ...]:
        fields = [
            "best_bid",
            "best_ask",
            "spread_ticks_int",
            "microprice",
            "ofi",
            "ofi_delta",
            "imbalance_5",
            "last_price",
            "last_trade_source_event_id",
            "last_price_source_event_id",
        ]
        for side in ("bid", "ask"):
            for level in range(1, self.config.required_book_levels + 1):
                fields.extend((f"{side}_price_{level:02d}", f"{side}_quantity_{level:02d}"))
        for seconds in TRADE_WINDOWS:
            fields.extend((f"trade_count_{seconds}s", f"trade_flow_{seconds}s"))
        for minutes in (1, 5, 15):
            fields.extend(
                (
                    f"candle_{minutes}m_source_event_id",
                    f"candle_{minutes}m_canonical_is_complete",
                    f"return_{minutes}m",
                    f"candle_volume_{minutes}m",
                )
            )
            if self.config.atr_required:
                fields.append(f"atr_{minutes}m")
        fields.extend(("trend", "regime"))
        return tuple(fields)

    def _trade_flow(self, result: dict[str, Any], ts: datetime, cutoff: datetime) -> None:
        for seconds in TRADE_WINDOWS:
            events = self.timeline.between(
                "raw_trades", ts - timedelta(seconds=seconds), ts, processing_cutoff=cutoff
            )
            buy = sell = unknown = 0.0
            for event in events:
                quantity = float(event.row["quantity"])
                side = str(event.row.get("aggressor_side") or "UNKNOWN").upper()
                if side == "BUY":
                    buy += quantity
                elif side == "SELL":
                    sell += quantity
                else:
                    unknown += quantity
            result[f"trade_count_{seconds}s"] = len(events)
            result[f"buy_volume_{seconds}s"] = buy
            result[f"sell_volume_{seconds}s"] = sell
            result[f"unknown_side_volume_{seconds}s"] = unknown
            result[f"trade_flow_{seconds}s"] = buy - sell
            result[f"trade_window_first_event_id_{seconds}s"] = (
                events[0].event_id if events else None
            )
            result[f"trade_window_last_event_id_{seconds}s"] = (
                events[-1].event_id if events else None
            )

    def _market_sources(
        self, result: dict[str, Any], ts: datetime, cutoff: datetime
    ) -> dict[str, CanonicalEvent | None]:
        sources = {
            "last_trade": self.timeline.latest("raw_trades", ts, processing_cutoff=cutoff),
            "last_price": self.timeline.latest("raw_last_price", ts, processing_cutoff=cutoff),
        }
        for name, event in sources.items():
            result[f"{name}_source_event_id"] = event.event_id if event else None
        if sources["last_price"]:
            result["last_price"] = float(sources["last_price"].row["last_price"])
        return sources

    def _spread(self, result: dict[str, Any], flags: list[str]) -> None:
        """Normalize a quote spread on the instrument grid without float comparisons."""

        spread = result.get("spread")
        result["spread_price"] = spread
        result["tick_size"] = self.config.tick_size
        result["spread_threshold_ticks"] = self.config.spread_threshold_ticks
        if spread is None:
            result.update(
                spread_ticks=None,
                spread_ticks_decimal=None,
                spread_ticks_int=None,
                tick_grid_error=None,
                spread_on_tick_grid=False,
                spread_gate_passed=False,
            )
            return
        try:
            ratio = Decimal(str(spread)) / Decimal(str(self.config.tick_size))
            nearest = ratio.to_integral_value(rounding=ROUND_HALF_EVEN)
        except (InvalidOperation, ZeroDivisionError):
            flags.append("invalid_spread_tick_calculation")
            result.update(
                spread_ticks=None,
                spread_ticks_decimal=None,
                spread_ticks_int=None,
                tick_grid_error=None,
                spread_on_tick_grid=False,
                spread_gate_passed=False,
            )
            return
        error = abs(ratio - nearest)
        on_grid = error <= Decimal(str(self.config.spread_grid_epsilon))
        result["spread_ticks_decimal"] = float(ratio)
        result["spread_ticks"] = float(ratio)  # compatibility alias; never used by the gate
        result["spread_ticks_int"] = int(nearest) if on_grid else None
        result["tick_grid_error"] = float(error)
        result["spread_on_tick_grid"] = on_grid
        result["spread_gate_passed"] = bool(
            on_grid and int(nearest) <= self.config.spread_threshold_ticks
        )

    @staticmethod
    def _ofi(
        result: dict[str, Any],
        book: CanonicalEvent,
        previous: tuple[CanonicalEvent, float] | None,
        reset_reason: str | None,
    ) -> None:
        result["ofi_source_event_id"] = book.event_id
        result["ofi_previous_event_id"] = None
        result["ofi_continuity_valid"] = False
        result["ofi_reset_reason"] = reset_reason or "no_previous_snapshot"
        result["ofi_delta"] = None
        if previous is None or result.get("ofi") is None:
            return
        prior, prior_ofi = previous
        current_session = str(book.row.get("session_id") or "")
        prior_session = str(prior.row.get("session_id") or "")
        if current_session != prior_session:
            result["ofi_reset_reason"] = "session_changed"
            return
        if prior.sort_key >= book.sort_key:
            result["ofi_reset_reason"] = "non_monotonic_snapshot"
            return
        result["ofi_previous_event_id"] = prior.event_id
        result["ofi_continuity_valid"] = True
        result["ofi_reset_reason"] = None
        result["ofi_delta"] = float(result["ofi"]) - prior_ofi

    def _candles(
        self,
        result: dict[str, Any],
        ts: datetime,
        cutoff: datetime,
        sources: dict[str, CanonicalEvent | None],
    ) -> None:
        closes: dict[int, list[CanonicalEvent]] = {}
        for minutes in (1, 5, 15):
            dataset = f"candles_{minutes}m"
            known = [
                event
                for event in self.timeline.events(dataset)
                if event.receive_ts <= cutoff
                and event.processing_ts <= cutoff
                and isinstance(event.row.get("candle_end"), datetime)
                and event.row["candle_end"] <= ts
            ]
            # One point-in-time revision per candle interval. Stable canonical order
            # makes the final assignment the latest revision available at cutoff.
            by_interval: dict[tuple[object, object, object, object], CanonicalEvent] = {}
            for event in known:
                key = (
                    event.row.get("instrument_uid") or event.row.get("instrument_id"),
                    event.row.get("timeframe") or event.row.get("interval") or minutes,
                    event.row.get("candle_start"),
                    event.row.get("candle_end"),
                )
                previous = by_interval.get(key)
                revision_key = (event.sequence, event.receive_ts, event.event_id)
                if previous is None or revision_key > (
                    previous.sequence,
                    previous.receive_ts,
                    previous.event_id,
                ):
                    by_interval[key] = event
            available = sorted(
                by_interval.values(),
                key=lambda item: (item.row["candle_end"], item.sort_key),
            )
            closes[minutes] = available
            source = available[-1] if available else None
            sources[f"candle_{minutes}m"] = source
            result[f"candle_{minutes}m_source_event_id"] = source.event_id if source else None
            result[f"candle_volume_{minutes}m"] = float(source.row["volume"]) if source else None
            result[f"candle_{minutes}m_start"] = (
                source.row.get("candle_start") if source else None
            )
            result[f"candle_{minutes}m_end"] = source.row.get("candle_end") if source else None
            revision = source.sequence if source else None
            result[f"candle_{minutes}m_revision"] = revision
            result[f"candle_{minutes}m_selected_revision"] = revision
            result[f"candle_{minutes}m_revision_receive_ts"] = (
                source.receive_ts if source else None
            )
            source_complete = bool(source.row.get("is_complete")) if source else False
            canonical_complete = bool(
                source
                and source.row["candle_end"] <= ts
                and source.receive_ts <= cutoff
                and source.processing_ts <= cutoff
            )
            result[f"candle_{minutes}m_source_is_complete"] = source_complete
            result[f"candle_{minutes}m_canonical_is_complete"] = canonical_complete
            result[f"candle_{minutes}m_completion_reason"] = (
                "source_flag_and_interval_elapsed"
                if source_complete and canonical_complete
                else "interval_elapsed"
                if canonical_complete
                else "source_flag"
                if source_complete
                else "not_complete"
            )
            # True range needs one preceding close in addition to the ATR sample.
            required = max(2, self.config.atr_period + 1)
            result[f"atr_{minutes}m_warmup_remaining"] = max(0, required - len(available))
            if source and len(available) >= 2:
                prior = float(available[-2].row["close"])
                result[f"return_{minutes}m"] = (
                    float(source.row["close"]) / prior - 1 if prior else None
                )
            else:
                result[f"return_{minutes}m"] = None
            if len(available) >= required:
                sample = available[-self.config.atr_period :]
                positions = {id(event): index for index, event in enumerate(available)}
                true_ranges: list[float] = []
                for item in sample:
                    index = positions[id(item)]
                    high = float(item.row["high"])
                    low = float(item.row["low"])
                    previous_close = float(available[index - 1].row["close"])
                    true_ranges.append(
                        max(high - low, abs(high - previous_close), abs(low - previous_close))
                    )
                result[f"atr_{minutes}m"] = sum(true_ranges) / len(true_ranges)
            else:
                result[f"atr_{minutes}m"] = None
        history = closes[1]
        if len(history) < self.config.candle_warmup:
            result.update(
                trend="insufficient_history", regime="insufficient_history", trend_score=None
            )
        else:
            first, last = (
                float(history[-self.config.candle_warmup].row["close"]),
                float(history[-1].row["close"]),
            )
            score = last / first - 1 if first else 0.0
            threshold = 0.001
            result["trend_score"], result["trend_threshold"] = score, threshold
            result["trend"] = (
                "up" if score > threshold else "down" if score < -threshold else "range"
            )
            atr = result.get("atr_1m") or 0.0
            result["regime_volatility_threshold"] = 0.005
            result["regime"] = "high_volatility" if last and atr / last > 0.005 else result["trend"]

    def _ages(
        self,
        result: dict[str, Any],
        book: CanonicalEvent,
        ts: datetime,
        sources: Mapping[str, CanonicalEvent | None],
    ) -> None:
        result["orderbook_receive_age_ms"] = (
            book.processing_ts - book.receive_ts
        ).total_seconds() * 1000
        result["orderbook_exchange_age_ms"] = (ts - book.exchange_ts).total_seconds() * 1000
        for name, source in sources.items():
            result[f"{name}_receive_age_ms"] = (
                (book.processing_ts - source.receive_ts).total_seconds() * 1000 if source else None
            )
            result[f"{name}_exchange_age_ms"] = (
                (ts - source.exchange_ts).total_seconds() * 1000 if source else None
            )
            if name.startswith("candle_"):
                interval_age = (
                    (ts - source.row["candle_end"]).total_seconds() * 1000
                    if source and isinstance(source.row.get("candle_end"), datetime)
                    else None
                )
                result[f"{name}_interval_age_ms"] = interval_age
                minutes = int(name.removeprefix("candle_").removesuffix("m"))
                result[f"{name}_stale"] = (
                    interval_age is None
                    or interval_age >= self.config.candle_max_age_ms(minutes)
                )
        result["materialization_lag_ms"] = (
            result["materialized_ts"] - book.processing_ts
        ).total_seconds() * 1000
