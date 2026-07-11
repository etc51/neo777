"""Raw-only feature snapshots for schema-v4."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
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
    max_source_age_ms: float = 30_000.0

    def __post_init__(self) -> None:
        if self.tick_size <= 0 or not 1 <= self.required_book_levels <= 20:
            raise ValueError("invalid feature configuration")


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
        previous_ofi: float | None = None
        for book in books:
            row = self._one(book, previous_ofi)
            previous_ofi = row.get("ofi")
            output.append(row)
        return output

    def _one(self, book: CanonicalEvent, previous_ofi: float | None) -> dict[str, Any]:
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
            "schema_version": "schema-v4",
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
        result["spread_ticks"] = (
            result["spread"] / self.config.tick_size if result["spread"] is not None else None
        )
        bid_q = bids[0][1] if bids else 0.0
        ask_q = asks[0][1] if asks else 0.0
        top_total = bid_q + ask_q
        result["microprice"] = (
            (best_ask * bid_q + best_bid * ask_q) / top_total
            if top_total and best_bid is not None and best_ask is not None
            else None
        )
        result["ofi"] = bid_q - ask_q if bids and asks else None
        result["ofi_delta"] = (
            result["ofi"] - previous_ofi
            if result["ofi"] is not None and previous_ofi is not None
            else None
        )
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
        if stale:
            flags.append("stale_source")
        result["missing_fields"] = missing
        result["missing_reason"] = (
            ",".join(missing) if missing else ("stale_source" if stale else None)
        )
        result["source_status"] = (
            "READY" if not missing and not stale and not flags else "INCOMPLETE"
        )
        result["feature_ready"] = result["source_status"] == "READY"
        return result

    def required_fields(self) -> tuple[str, ...]:
        fields = [
            "best_bid",
            "best_ask",
            "last_trade_source_event_id",
            "last_price_source_event_id",
        ]
        for side in ("bid", "ask"):
            for level in range(1, self.config.required_book_levels + 1):
                fields.extend((f"{side}_price_{level:02d}", f"{side}_quantity_{level:02d}"))
        fields.extend(f"candle_{minutes}m_source_event_id" for minutes in (1, 5, 15))
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
            available = [
                event
                for event in self.timeline.events(dataset)
                if event.receive_ts <= cutoff
                and event.processing_ts <= cutoff
                and bool(event.row.get("is_complete"))
                and isinstance(event.row.get("candle_end"), datetime)
                and event.row["candle_end"] <= ts
            ]
            closes[minutes] = available
            source = available[-1] if available else None
            sources[f"candle_{minutes}m"] = source
            result[f"candle_{minutes}m_source_event_id"] = source.event_id if source else None
            result[f"candle_volume_{minutes}m"] = float(source.row["volume"]) if source else None
            if source and len(available) >= 2:
                prior = float(available[-2].row["close"])
                result[f"return_{minutes}m"] = (
                    float(source.row["close"]) / prior - 1 if prior else None
                )
                ranges = [
                    float(item.row["high"]) - float(item.row["low"]) for item in available[-14:]
                ]
                result[f"atr_{minutes}m"] = sum(ranges) / len(ranges)
            else:
                result[f"return_{minutes}m"] = None
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

    @staticmethod
    def _ages(
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
        result["materialization_lag_ms"] = (
            result["materialized_ts"] - book.processing_ts
        ).total_seconds() * 1000
