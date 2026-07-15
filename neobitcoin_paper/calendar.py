"""Effective-dated Moscow session calendar for the Neobitcoin paper service."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .domain import DomainValidationError, SessionLabel, as_utc, trading_status_is_open

MOSCOW = ZoneInfo("Europe/Moscow")
MOEX_2026_EFFECTIVE_DATE = date(2026, 7, 14)

_BREAK_STATUSES = frozenset({"BREAK_IN_TRADING", "DEALER_BREAK_IN_TRADING", "BREAK"})
_CLOSED_STATUSES = frozenset(
    {
        "NOT_AVAILABLE_FOR_TRADING",
        "DEALER_NOT_AVAILABLE_FOR_TRADING",
        "CLOSED",
    }
)
_CLOSING_STATUSES = frozenset(
    {"CLOSING_PERIOD", "CLOSING_AUCTION", "DARK_POOL_AUCTION", "CLOSING"}
)
_PRE_OPEN_STATUSES = frozenset({"OPENING_PERIOD", "OPENING_AUCTION_PERIOD", "PRE_OPEN"})


class CalendarRuleNotFound(DomainValidationError):
    """No reviewed effective-dated rule covers a requested session date."""


@dataclass(frozen=True, slots=True)
class CalendarRule:
    version: str
    effective_from: date
    weekday_neo_open: time = time(7, 0)
    weekend_neo_open: time = time(10, 0)
    neo_close: time = time(0, 0)
    weekday_pre_open: time = time(6, 50)
    weekend_pre_open: time = time(9, 50)
    main_start: time = time(10, 0)
    main_end: time = time(19, 0)
    evening_end: time = time(23, 50)

    def __post_init__(self) -> None:
        if not self.version.strip():
            raise DomainValidationError("calendar rule version must not be blank")
        if self.weekday_pre_open >= self.weekday_neo_open:
            raise DomainValidationError("weekday pre-open must precede the neo window")
        if self.weekend_pre_open >= self.weekend_neo_open:
            raise DomainValidationError("weekend pre-open must precede the neo window")
        if not self.weekday_neo_open <= self.main_start < self.main_end < self.evening_end:
            raise DomainValidationError("weekday regime boundaries are invalid")


@dataclass(frozen=True, slots=True)
class SessionWindow:
    session_date_msk: date
    expected_open: datetime
    expected_close: datetime
    pre_open: datetime
    is_weekend: bool
    is_holiday: bool
    calendar_rule_version: str


@dataclass(frozen=True, slots=True)
class SessionContext:
    observed_at: datetime
    session_date_msk: date
    session_label: SessionLabel
    trading_status: str
    calendar_rule_version: str
    expected_open: datetime
    expected_close: datetime
    scheduled_open: bool
    entry_allowed: bool


class SessionCalendar:
    """Resolve expected regimes while giving a live status final authority.

    The bundled rule begins on 14 July 2026, the date explicitly reviewed in
    the project specification.  Earlier dates intentionally fail closed rather
    than silently inheriting a schedule that has not been documented.
    """

    def __init__(
        self,
        rules: Iterable[CalendarRule] | None = None,
        *,
        holidays: Iterable[date] = (),
    ) -> None:
        supplied = tuple(rules) if rules is not None else (self.rule_2026_07_14(),)
        if not supplied:
            raise DomainValidationError("at least one calendar rule is required")
        ordered = tuple(sorted(supplied, key=lambda item: item.effective_from))
        dates = [item.effective_from for item in ordered]
        if len(dates) != len(set(dates)):
            raise DomainValidationError("calendar effective dates must be unique")
        self._rules = ordered
        self._holidays = frozenset(holidays)

    @staticmethod
    def rule_2026_07_14() -> CalendarRule:
        return CalendarRule(
            version="neo-moscow-2026-07-14-v1",
            effective_from=MOEX_2026_EFFECTIVE_DATE,
        )

    @property
    def rules(self) -> tuple[CalendarRule, ...]:
        return self._rules

    def rule_for(self, session_date: date) -> CalendarRule:
        matching = tuple(rule for rule in self._rules if rule.effective_from <= session_date)
        if not matching:
            raise CalendarRuleNotFound(
                f"no reviewed calendar rule covers session date {session_date.isoformat()}"
            )
        return matching[-1]

    def window_for(self, session_date: date) -> SessionWindow:
        rule = self.rule_for(session_date)
        is_weekend = session_date.weekday() >= 5
        is_holiday = session_date in self._holidays
        special_day = is_weekend or is_holiday
        open_time = rule.weekend_neo_open if special_day else rule.weekday_neo_open
        pre_open_time = rule.weekend_pre_open if special_day else rule.weekday_pre_open
        local_open = datetime.combine(session_date, open_time, tzinfo=MOSCOW)
        local_pre_open = datetime.combine(session_date, pre_open_time, tzinfo=MOSCOW)
        # A midnight close belongs to the session that opened on the prior date.
        local_close = datetime.combine(
            session_date + timedelta(days=1),
            rule.neo_close,
            tzinfo=MOSCOW,
        )
        return SessionWindow(
            session_date_msk=session_date,
            expected_open=local_open.astimezone(UTC),
            expected_close=local_close.astimezone(UTC),
            pre_open=local_pre_open.astimezone(UTC),
            is_weekend=is_weekend,
            is_holiday=is_holiday,
            calendar_rule_version=rule.version,
        )

    def resolve(self, observed_at: datetime, live_status: str | None = None) -> SessionContext:
        observed_at = as_utc(observed_at, "observed_at")
        local = observed_at.astimezone(MOSCOW)
        today = local.date()

        # During the closed hours after midnight, retain the session date that
        # opened on the previous Moscow calendar day.  Switch at today's
        # effective pre-open boundary.
        today_window = self.window_for(today)
        session_date = today if observed_at >= today_window.pre_open else today - timedelta(days=1)
        window = self.window_for(session_date)
        rule = self.rule_for(session_date)
        scheduled_open = window.expected_open <= observed_at < window.expected_close
        scheduled_label = self._scheduled_label(local, window, rule)

        normalized_status = self._normalize_status(live_status)
        label = self._status_label(normalized_status, scheduled_label)
        # Unknown live state is deliberately fail-closed.  Conversely an
        # explicitly open live state wins over a stale fallback clock.
        entry_allowed = trading_status_is_open(normalized_status)

        return SessionContext(
            observed_at=observed_at,
            session_date_msk=session_date,
            session_label=label,
            trading_status=normalized_status,
            calendar_rule_version=rule.version,
            expected_open=window.expected_open,
            expected_close=window.expected_close,
            scheduled_open=scheduled_open,
            entry_allowed=entry_allowed,
        )

    def finalizer_at(
        self,
        session_date: date,
        grace_period: timedelta = timedelta(minutes=5),
    ) -> datetime:
        if grace_period < timedelta(0):
            raise DomainValidationError("archive grace period must be non-negative")
        return self.window_for(session_date).expected_close + grace_period

    @staticmethod
    def _normalize_status(value: str | None) -> str:
        if value is None or not str(value).strip():
            return "UNKNOWN"
        raw = str(value).strip().upper().rsplit(".", maxsplit=1)[-1]
        prefix = "SECURITY_TRADING_STATUS_"
        return raw[len(prefix) :] if raw.startswith(prefix) else raw

    @staticmethod
    def _status_label(status: str, scheduled: SessionLabel) -> SessionLabel:
        if status in _BREAK_STATUSES:
            return SessionLabel.BREAK
        if status in _CLOSED_STATUSES:
            return SessionLabel.CLOSED
        if status in _CLOSING_STATUSES:
            return SessionLabel.CLOSING
        if status in _PRE_OPEN_STATUSES:
            return SessionLabel.PRE_OPEN
        if status == "DEALER_NORMAL_TRADING":
            return SessionLabel.DEALER_MODE
        if status == "UNKNOWN":
            return scheduled
        return scheduled

    @staticmethod
    def _scheduled_label(
        local: datetime,
        window: SessionWindow,
        rule: CalendarRule,
    ) -> SessionLabel:
        observed_utc = local.astimezone(UTC)
        if window.pre_open <= observed_utc < window.expected_open:
            return SessionLabel.PRE_OPEN
        if not window.expected_open <= observed_utc < window.expected_close:
            return SessionLabel.CLOSED
        if window.is_holiday:
            return SessionLabel.HOLIDAY
        if window.is_weekend:
            return SessionLabel.WEEKEND
        local_time = local.timetz().replace(tzinfo=None)
        if rule.weekday_neo_open <= local_time < rule.main_start:
            return SessionLabel.MORNING
        if rule.main_start <= local_time < rule.main_end:
            return SessionLabel.MAIN
        if rule.main_end <= local_time < rule.evening_end:
            return SessionLabel.EVENING
        return SessionLabel.NEO_LATE_SESSION


__all__ = [
    "CalendarRule",
    "CalendarRuleNotFound",
    "MOEX_2026_EFFECTIVE_DATE",
    "MOSCOW",
    "SessionCalendar",
    "SessionContext",
    "SessionWindow",
]
