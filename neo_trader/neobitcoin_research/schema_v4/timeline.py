"""Canonical event-time model and production point-in-time selection primitives."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final

RAW_DATASETS: Final = frozenset(
    {
        "raw_orderbook",
        "raw_trades",
        "raw_last_price",
        "candles_1m",
        "candles_5m",
        "candles_15m",
        "market_status_events",
    }
)


class TimelineError(ValueError):
    """A raw event violates the canonical time contract."""


def _utc(value: Any, field: str) -> datetime:
    if not isinstance(value, datetime):
        raise TimelineError(f"{field} must be a typed datetime")
    if value.tzinfo is None:
        raise TimelineError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class CanonicalEvent:
    dataset: str
    event_id: str
    exchange_ts: datetime
    receive_ts: datetime
    processing_ts: datetime
    sequence: int
    row: Mapping[str, Any]

    @property
    def sort_key(self) -> tuple[datetime, int, datetime, str]:
        return self.exchange_ts, self.sequence, self.receive_ts, self.event_id


class CanonicalTimeline:
    """Immutable, stable timeline made exclusively from archived raw rows."""

    def __init__(self, raw: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
        unknown = set(raw) - RAW_DATASETS
        if unknown:
            raise TimelineError(f"non-raw datasets are forbidden: {sorted(unknown)}")
        events: dict[str, tuple[CanonicalEvent, ...]] = {}
        ids: set[str] = set()
        for dataset in RAW_DATASETS:
            parsed: list[CanonicalEvent] = []
            for row in raw.get(dataset, ()):
                event_id = str(row.get("event_id") or "")
                if not event_id or event_id in ids:
                    raise TimelineError(f"missing or duplicate event_id: {event_id!r}")
                ids.add(event_id)
                exchange = _utc(row.get("exchange_ts"), "exchange_ts")
                receive = _utc(row.get("receive_ts"), "receive_ts")
                processing = _utc(row.get("processing_ts"), "processing_ts")
                if receive > processing:
                    raise TimelineError(f"receive_ts after processing_ts for {event_id}")
                sequence = int(row.get("sequence", row.get("revision", 0)) or 0)
                parsed.append(
                    CanonicalEvent(dataset, event_id, exchange, receive, processing, sequence, row)
                )
            events[dataset] = tuple(sorted(parsed, key=lambda item: item.sort_key))
        self._events = events
        self._keys = {
            dataset: tuple(event.sort_key for event in values)
            for dataset, values in events.items()
        }
        self._ids = frozenset(ids)

    @property
    def event_ids(self) -> frozenset[str]:
        return self._ids

    def events(self, dataset: str) -> tuple[CanonicalEvent, ...]:
        if dataset not in RAW_DATASETS:
            raise TimelineError(f"not a raw dataset: {dataset}")
        return self._events[dataset]

    def latest(
        self,
        dataset: str,
        target_ts: datetime,
        *,
        processing_cutoff: datetime | None = None,
        predicate: Any = None,
    ) -> CanonicalEvent | None:
        """Last event at/before target, optionally known by a processing cutoff."""

        target = _utc(target_ts, "target_ts")
        cutoff = _utc(processing_cutoff, "processing_cutoff") if processing_cutoff else None
        values = self.events(dataset)
        keys = self._keys[dataset]
        pos = bisect_right(keys, (target, 2**63 - 1, datetime.max.replace(tzinfo=UTC), "\uffff"))
        for event in reversed(values[:pos]):
            if cutoff is not None and (event.receive_ts > cutoff or event.processing_ts > cutoff):
                continue
            if predicate is None or predicate(event.row):
                return event
        return None

    def between(
        self,
        dataset: str,
        start_exclusive: datetime,
        end_inclusive: datetime,
        *,
        processing_cutoff: datetime | None = None,
    ) -> tuple[CanonicalEvent, ...]:
        start = _utc(start_exclusive, "start_exclusive")
        end = _utc(end_inclusive, "end_inclusive")
        cutoff = _utc(processing_cutoff, "processing_cutoff") if processing_cutoff else None
        values = self.events(dataset)
        keys = self._keys[dataset]
        left = bisect_right(
            keys, (start, 2**63 - 1, datetime.max.replace(tzinfo=UTC), "\uffff")
        )
        right = bisect_right(
            keys, (end, 2**63 - 1, datetime.max.replace(tzinfo=UTC), "\uffff")
        )
        return tuple(
            event
            for event in values[left:right]
            if cutoff is None or (event.receive_ts <= cutoff and event.processing_ts <= cutoff)
        )

    def first_at_or_after(self, dataset: str, target_ts: datetime) -> CanonicalEvent | None:
        """First canonical event at or after the target exchange timestamp."""

        target = _utc(target_ts, "target_ts")
        values = self.events(dataset)
        pos = bisect_left(
            self._keys[dataset],
            (target, -(2**63), datetime.min.replace(tzinfo=UTC), ""),
        )
        return values[pos] if pos < len(values) else None

    def after(
        self, dataset: str, start_exclusive: datetime, end_inclusive: datetime
    ) -> tuple[CanonicalEvent, ...]:
        return self.between(dataset, start_exclusive, end_inclusive)

    @classmethod
    def from_rows(cls, **raw: Iterable[Mapping[str, Any]]) -> CanonicalTimeline:
        return cls({name: tuple(rows) for name, rows in raw.items()})
