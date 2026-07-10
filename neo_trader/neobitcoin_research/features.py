"""Causal market microstructure features for Neobitcoin."""

from __future__ import annotations

import statistics
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol


class LevelLike(Protocol):
    @property
    def price(self) -> Decimal: ...

    @property
    def quantity(self) -> Decimal | int: ...


class BookLike(Protocol):
    @property
    def instrument_uid(self) -> str: ...

    @property
    def bids(self) -> Sequence[LevelLike]: ...

    @property
    def asks(self) -> Sequence[LevelLike]: ...

    @property
    def is_consistent(self) -> bool: ...


@dataclass(frozen=True)
class TapeTrade:
    timestamp: datetime
    price: Decimal
    quantity: Decimal
    side: str
    classification_confidence: float = 1.0
    classification_reason: str = "exchange"


@dataclass(frozen=True)
class CandlePoint:
    timestamp: datetime
    interval: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _decimal(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _levels(book: BookLike, side: str) -> list[tuple[Decimal, Decimal]]:
    raw = book.bids if side == "bid" else book.asks
    levels = [(_decimal(level.price), _decimal(level.quantity)) for level in raw]
    levels = [(price, quantity) for price, quantity in levels if price > 0 and quantity > 0]
    levels.sort(key=lambda item: item[0], reverse=side == "bid")
    return levels


class CausalFeatureEngine:
    """Maintain only timestamped past state and emit one causal feature row."""

    WINDOWS = (1, 5, 15, 30, 60)
    DEPTHS = (1, 3, 5, 10, 20)

    def __init__(self, *, tick_size: Decimal, lot_size: int = 1) -> None:
        if tick_size <= 0 or lot_size <= 0:
            raise ValueError("tick_size and lot_size must be positive")
        self.tick_size = tick_size
        self.lot_size = lot_size
        self.trades: deque[TapeTrade] = deque()
        self.mid_history: deque[tuple[datetime, Decimal]] = deque()
        self.candles: deque[CandlePoint] = deque()
        self.previous_book: (
            tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]] | None
        ) = None
        self.last_bid_change: datetime | None = None
        self.last_ask_change: datetime | None = None
        self.previous_ofi = Decimal("0")

    def add_trade(self, trade: TapeTrade) -> None:
        if trade.quantity <= 0 or trade.price <= 0:
            return
        self.trades.append(trade)
        self._trim(_utc(trade.timestamp))

    def classify_trade(
        self,
        *,
        timestamp: datetime,
        price: Decimal,
        quantity: Decimal,
        best_bid: Decimal | None,
        best_ask: Decimal | None,
    ) -> TapeTrade:
        if best_ask is not None and price >= best_ask:
            return TapeTrade(timestamp, price, quantity, "BUY", 0.95, "at_or_above_ask")
        if best_bid is not None and price <= best_bid:
            return TapeTrade(timestamp, price, quantity, "SELL", 0.95, "at_or_below_bid")
        previous = self.trades[-1].price if self.trades else None
        if previous is not None and price != previous:
            side = "BUY" if price > previous else "SELL"
            return TapeTrade(timestamp, price, quantity, side, 0.65, "tick_rule")
        return TapeTrade(timestamp, price, quantity, "UNKNOWN", 0.0, "unclassifiable")

    def add_candle(self, candle: CandlePoint) -> None:
        self.candles.append(candle)
        self._trim(_utc(candle.timestamp))

    def snapshot(
        self,
        *,
        book: BookLike,
        timestamp: datetime,
        receive_timestamp: datetime,
        last_price: Decimal | None = None,
        trading_status: str = "UNKNOWN",
        gap_before: bool = False,
        seconds_after_reconnect: float | None = None,
        required_depth: int = 20,
    ) -> dict[str, object] | None:
        """Return None for inconsistent, crossed, stale, or shallow books."""

        ts = _utc(timestamp)
        received = _utc(receive_timestamp)
        self._trim(ts)
        bids = _levels(book, "bid")
        asks = _levels(book, "ask")
        if not book.is_consistent or not bids or not asks:
            return None
        if bids[0][0] >= asks[0][0] or len(bids) < required_depth or len(asks) < required_depth:
            return None
        latency_ms = 0.0 if received < ts else (received - ts).total_seconds() * 1000

        bid, ask = bids[0][0], asks[0][0]
        bid_qty, ask_qty = bids[0][1], asks[0][1]
        mid = (bid + ask) / 2
        spread = ask - bid
        micro = (ask * bid_qty + bid * ask_qty) / (bid_qty + ask_qty)
        previous = self.previous_book
        now_bid_changed = previous is None or previous[0][0][0] != bid
        now_ask_changed = previous is None or previous[1][0][0] != ask
        if now_bid_changed:
            self.last_bid_change = ts
        if now_ask_changed:
            self.last_ask_change = ts

        row: dict[str, object] = {
            "timestamp": ts.isoformat(),
            "instrument_uid": book.instrument_uid,
            "best_bid": float(bid),
            "best_ask": float(ask),
            "midprice": float(mid),
            "last_price": float(last_price) if last_price is not None else None,
            "spread_absolute": float(spread),
            "spread_ticks": float(spread / self.tick_size),
            "spread_bps": float(spread / mid * Decimal(10_000)),
            "microprice": float(micro),
            "microprice_minus_mid": float(micro - mid),
            "latency_ms": latency_ms,
            "stale_age_seconds": max((received - ts).total_seconds(), 0.0),
            "trading_status": trading_status,
            "gap_before": gap_before,
            "seconds_after_reconnect": seconds_after_reconnect,
            "bid_age_seconds": self._age(ts, self.last_bid_change),
            "ask_age_seconds": self._age(ts, self.last_ask_change),
            "best_bid_changed": now_bid_changed,
            "best_ask_changed": now_ask_changed,
        }
        self._book_features(row, bids, asks, mid, previous)
        self._trade_features(row, ts, mid)
        self._return_features(row, ts, mid)
        self._candle_features(row, ts)

        self.mid_history.append((ts, mid))
        self.previous_book = (bids, asks)
        return row

    def _book_features(
        self,
        row: dict[str, object],
        bids: list[tuple[Decimal, Decimal]],
        asks: list[tuple[Decimal, Decimal]],
        mid: Decimal,
        previous: tuple[list[tuple[Decimal, Decimal]], list[tuple[Decimal, Decimal]]] | None,
    ) -> None:
        prev_bids = dict(previous[0]) if previous else {}
        prev_asks = dict(previous[1]) if previous else {}
        for index in range(20):
            bp, bq = bids[index]
            ap, aq = asks[index]
            level = index + 1
            row[f"bid_price_{level}"] = float(bp)
            row[f"ask_price_{level}"] = float(ap)
            row[f"bid_volume_{level}"] = float(bq)
            row[f"ask_volume_{level}"] = float(aq)
            row[f"bid_distance_{level}"] = float(mid - bp)
            row[f"ask_distance_{level}"] = float(ap - mid)
            row[f"bid_volume_delta_{level}"] = float(bq - prev_bids.get(bp, Decimal(0)))
            row[f"ask_volume_delta_{level}"] = float(aq - prev_asks.get(ap, Decimal(0)))
        for depth in self.DEPTHS:
            bid_depth = sum(quantity for _, quantity in bids[:depth])
            ask_depth = sum(quantity for _, quantity in asks[:depth])
            total = bid_depth + ask_depth
            row[f"cumulative_bid_depth_{depth}"] = float(bid_depth)
            row[f"cumulative_ask_depth_{depth}"] = float(ask_depth)
            row[f"imbalance_{depth}"] = float((bid_depth - ask_depth) / total) if total else 0.0

        weights = [Decimal(1) / Decimal(index + 1) for index in range(20)]
        weighted_bid = sum(bids[index][1] * weights[index] for index in range(20))
        weighted_ask = sum(asks[index][1] * weights[index] for index in range(20))
        weighted_total = weighted_bid + weighted_ask
        row["distance_weighted_imbalance"] = (
            float((weighted_bid - weighted_ask) / weighted_total) if weighted_total else 0.0
        )
        row["bid_slope"] = self._slope(bids[:20])
        row["ask_slope"] = self._slope(asks[:20])
        row["bid_convexity"] = self._convexity(bids[:20])
        row["ask_convexity"] = self._convexity(asks[:20])
        row["depth_asymmetry"] = row["imbalance_20"]
        row["bid_max_gap_ticks"] = self._max_gap(bids[:20])
        row["ask_max_gap_ticks"] = self._max_gap(asks[:20])
        all_qty = [quantity for _, quantity in bids[:20] + asks[:20]]
        row["volume_concentration"] = float(max(all_qty) / sum(all_qty)) if all_qty else 0.0

        if previous:
            old_bid = dict(previous[0])
            old_ask = dict(previous[1])
            added_bid = sum(
                max(quantity - old_bid.get(price, Decimal(0)), Decimal(0))
                for price, quantity in bids
            )
            added_ask = sum(
                max(quantity - old_ask.get(price, Decimal(0)), Decimal(0))
                for price, quantity in asks
            )
            removed_bid = sum(
                max(quantity - dict(bids).get(price, Decimal(0)), Decimal(0))
                for price, quantity in previous[0]
            )
            removed_ask = sum(
                max(quantity - dict(asks).get(price, Decimal(0)), Decimal(0))
                for price, quantity in previous[1]
            )
            row.update(
                new_bid_volume=float(added_bid),
                new_ask_volume=float(added_ask),
                removed_bid_volume=float(removed_bid),
                removed_ask_volume=float(removed_ask),
                multi_level_ofi=float(added_bid - removed_bid - added_ask + removed_ask),
            )
        else:
            row.update(
                new_bid_volume=0.0,
                new_ask_volume=0.0,
                removed_bid_volume=0.0,
                removed_ask_volume=0.0,
                multi_level_ofi=0.0,
            )
        ofi = Decimal(str(row["multi_level_ofi"]))
        row["ofi_change"] = float(ofi - self.previous_ofi)
        row["ofi_acceleration"] = row["ofi_change"]
        row["top_level_pressure"] = float(bids[0][1] - asks[0][1])
        self.previous_ofi = ofi

    def _trade_features(self, row: dict[str, object], ts: datetime, mid: Decimal) -> None:
        for seconds in self.WINDOWS:
            floor = ts - timedelta(seconds=seconds)
            trades = [trade for trade in self.trades if floor <= _utc(trade.timestamp) <= ts]
            buys = [trade for trade in trades if trade.side == "BUY"]
            sells = [trade for trade in trades if trade.side == "SELL"]
            buy_volume = sum((trade.quantity for trade in buys), Decimal(0))
            sell_volume = sum((trade.quantity for trade in sells), Decimal(0))
            total = buy_volume + sell_volume
            quantities = [float(trade.quantity) for trade in trades]
            prefix = f"trades_{seconds}s"
            row[f"{prefix}_buy_count"] = len(buys)
            row[f"{prefix}_sell_count"] = len(sells)
            row[f"{prefix}_buy_volume"] = float(buy_volume)
            row[f"{prefix}_sell_volume"] = float(sell_volume)
            row[f"{prefix}_signed_volume"] = float(buy_volume - sell_volume)
            row[f"{prefix}_imbalance"] = float((buy_volume - sell_volume) / total) if total else 0.0
            row[f"{prefix}_mean_size"] = statistics.fmean(quantities) if quantities else 0.0
            row[f"{prefix}_median_size"] = statistics.median(quantities) if quantities else 0.0
            row[f"{prefix}_max_size"] = max(quantities, default=0.0)
            row[f"{prefix}_large_count"] = sum(size >= 10 for size in quantities)
            row[f"{prefix}_intensity"] = len(trades) / seconds
        row["last_trade_direction"] = self.trades[-1].side if self.trades else "UNKNOWN"
        row["last_trade_classification_confidence"] = (
            self.trades[-1].classification_confidence if self.trades else 0.0
        )
        row["trade_price_divergence"] = float(self.trades[-1].price - mid) if self.trades else 0.0

    def _return_features(self, row: dict[str, object], ts: datetime, mid: Decimal) -> None:
        horizons = {
            "1s": 1,
            "5s": 5,
            "15s": 15,
            "30s": 30,
            "1m": 60,
            "3m": 180,
            "5m": 300,
            "15m": 900,
        }
        returns: list[float] = []
        for label, seconds in horizons.items():
            previous = self._latest_before(ts - timedelta(seconds=seconds))
            value = float(mid / previous - 1) if previous and previous > 0 else 0.0
            row[f"return_{label}"] = value
            if seconds <= 60 and previous:
                returns.append(value)
        row["realized_volatility"] = statistics.pstdev(returns) if len(returns) > 1 else 0.0

    def _candle_features(self, row: dict[str, object], ts: datetime) -> None:
        for interval in ("1m", "5m", "15m"):
            candles = [
                c for c in self.candles if c.interval == interval and _utc(c.timestamp) <= ts
            ]
            recent = candles[-14:]
            true_ranges: list[Decimal] = []
            previous_close: Decimal | None = None
            for candle in recent:
                ranges = [candle.high - candle.low]
                if previous_close is not None:
                    ranges.extend(
                        (abs(candle.high - previous_close), abs(candle.low - previous_close))
                    )
                true_ranges.append(max(ranges))
                previous_close = candle.close
            row[f"atr_{interval}"] = (
                float(sum(true_ranges) / len(true_ranges)) if true_ranges else None
            )
            row[f"candle_volume_{interval}"] = float(candles[-1].volume) if candles else None

    def _latest_before(self, target: datetime) -> Decimal | None:
        result: Decimal | None = None
        for timestamp, mid in self.mid_history:
            if timestamp <= target:
                result = mid
            else:
                break
        return result

    def _trim(self, now: datetime) -> None:
        cutoff = now - timedelta(minutes=20)
        while self.trades and _utc(self.trades[0].timestamp) < cutoff:
            self.trades.popleft()
        while self.mid_history and self.mid_history[0][0] < cutoff:
            self.mid_history.popleft()
        while self.candles and _utc(self.candles[0].timestamp) < now - timedelta(days=2):
            self.candles.popleft()

    @staticmethod
    def _age(now: datetime, changed: datetime | None) -> float | None:
        return None if changed is None else max((now - changed).total_seconds(), 0.0)

    def _max_gap(self, levels: Sequence[tuple[Decimal, Decimal]]) -> float:
        gaps = [
            abs(levels[i][0] - levels[i - 1][0]) / self.tick_size for i in range(1, len(levels))
        ]
        return float(max(gaps, default=Decimal(0)))

    @staticmethod
    def _slope(levels: Sequence[tuple[Decimal, Decimal]]) -> float:
        if len(levels) < 2:
            return 0.0
        x = list(range(len(levels)))
        y = [float(quantity) for _, quantity in levels]
        x_mean, y_mean = statistics.fmean(x), statistics.fmean(y)
        denominator = sum((value - x_mean) ** 2 for value in x)
        return sum((xv - x_mean) * (yv - y_mean) for xv, yv in zip(x, y, strict=True)) / denominator

    @staticmethod
    def _convexity(levels: Sequence[tuple[Decimal, Decimal]]) -> float:
        if len(levels) < 3:
            return 0.0
        quantities = [float(quantity) for _, quantity in levels]
        second = [
            quantities[i + 1] - 2 * quantities[i] + quantities[i - 1]
            for i in range(1, len(quantities) - 1)
        ]
        return statistics.fmean(second) if second else 0.0
