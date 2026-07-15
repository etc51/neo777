"""Keep the server collector aligned with the live instrument schedule.

The controller asks the existing read-only T-Invest integration for the
SPB_FUTURE schedule of BTCUSDperpA.  A cached schedule is used when the API is
temporarily unreachable; the published SPB Future hours are the final
fail-open fallback so that market data is missed neither on weekdays nor on
weekends.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.neobitcoin_research.tbank import TBankResearchClient  # noqa: E402

MOSCOW = ZoneInfo("Europe/Moscow")


@dataclass(frozen=True)
class TradingSession:
    session_date: date
    start: datetime
    end: datetime
    is_trading_day: bool
    source: str

    def as_dict(self) -> dict[str, object]:
        return {
            "session_date": self.session_date.isoformat(),
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "is_trading_day": self.is_trading_day,
            "source": self.source,
        }


def fallback_session(session_date: date) -> TradingSession:
    """Return the official ordinary SPB Future schedule.

    Since 6 April 2026 futures trade 07:00-00:00 MSK on weekdays and
    10:00-00:00 MSK on weekends/holidays.  API and cache always take
    precedence because the exchange can announce exceptional non-trading days.
    """

    start_at = time(7, 0) if session_date.weekday() < 5 else time(10, 0)
    start = datetime.combine(session_date, start_at, MOSCOW)
    end = datetime.combine(session_date + timedelta(days=1), time(0, 0), MOSCOW)
    return TradingSession(session_date, start, end, True, "official_fallback")


def sessions_from_api(schedules: object) -> dict[str, TradingSession]:
    sessions: dict[str, TradingSession] = {}
    if not isinstance(schedules, (list, tuple)):
        return sessions
    for exchange in schedules:
        if not isinstance(exchange, dict):
            continue
        days = exchange.get("days")
        if not isinstance(days, list):
            continue
        for day in days:
            if not isinstance(day, dict):
                continue
            start = _timestamp(day.get("startTime") or day.get("start_time"))
            end = _timestamp(day.get("endTime") or day.get("end_time"))
            day_stamp = _timestamp(day.get("date"))
            is_trading = bool(day.get("isTradingDay", day.get("is_trading_day", False)))
            if start is not None:
                session_date = start.astimezone(MOSCOW).date()
            elif day_stamp is not None:
                session_date = day_stamp.date()
            else:
                continue
            if not is_trading or start is None or end is None:
                fallback = fallback_session(session_date)
                sessions[session_date.isoformat()] = TradingSession(
                    session_date,
                    fallback.start,
                    fallback.end,
                    False,
                    "tbank_schedule",
                )
                continue
            sessions[session_date.isoformat()] = TradingSession(
                session_date,
                start.astimezone(MOSCOW),
                end.astimezone(MOSCOW),
                True,
                "tbank_schedule",
            )
    return sessions


def load_cached_sessions(path: Path) -> dict[str, TradingSession]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    result: dict[str, TradingSession] = {}
    raw_sessions = payload.get("sessions", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw_sessions, dict):
        return result
    for key, raw in raw_sessions.items():
        if not isinstance(raw, dict):
            continue
        try:
            session_date = date.fromisoformat(str(raw["session_date"]))
            result[str(key)] = TradingSession(
                session_date=session_date,
                start=datetime.fromisoformat(str(raw["start"])),
                end=datetime.fromisoformat(str(raw["end"])),
                is_trading_day=bool(raw["is_trading_day"]),
                source=str(raw.get("source", "cache")),
            )
        except (KeyError, TypeError, ValueError):
            continue
    return result


def cache_is_fresh(path: Path, now: datetime, *, maximum_age: timedelta) -> bool:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        updated_at = _timestamp(payload.get("updated_at"))
    except (AttributeError, OSError, json.JSONDecodeError):
        return False
    if updated_at is None:
        return False
    age = now.astimezone(UTC) - updated_at.astimezone(UTC)
    return timedelta(0) <= age <= maximum_age


def save_cached_sessions(path: Path, sessions: dict[str, TradingSession]) -> None:
    payload = {
        "updated_at": datetime.now(UTC).isoformat(),
        "sessions": {key: value.as_dict() for key, value in sorted(sessions.items())},
    }
    _atomic_json(path, payload)


def fetch_sessions(token_file: Path, now: datetime) -> dict[str, TradingSession]:
    client = TBankResearchClient.from_token_file(token_file)
    try:
        metadata = client.discover_neobitcoin(as_of=now.astimezone(UTC))
    finally:
        client.close()
    return sessions_from_api(metadata.trading_schedules)


def resolve_session(
    session_date: date,
    *,
    cache_path: Path,
    token_file: Path | None,
    now: datetime,
) -> tuple[TradingSession, str | None]:
    cached = load_cached_sessions(cache_path)
    error: str | None = None
    key = session_date.isoformat()
    needs_refresh = key not in cached or not cache_is_fresh(
        cache_path, now, maximum_age=timedelta(minutes=30)
    )
    if token_file is not None and needs_refresh:
        try:
            fresh = fetch_sessions(token_file, now)
        except Exception as exc:  # fail over without leaking token or response bodies
            error = type(exc).__name__
        else:
            cached.update(fresh)
            save_cached_sessions(cache_path, cached)
    if key in cached:
        session = cached[key]
        if session.source == "tbank_schedule" and error is not None:
            session = TradingSession(
                session.session_date,
                session.start,
                session.end,
                session.is_trading_day,
                "cached_tbank_schedule",
            )
        return session, error
    return fallback_session(session_date), error


def apply_service_state(service: str, should_run: bool) -> str:
    active = (
        subprocess.run(
            ["/usr/bin/systemctl", "is-active", "--quiet", service], check=False
        ).returncode
        == 0
    )
    if should_run and not active:
        subprocess.run(["/usr/bin/systemctl", "start", service], check=True)
        return "started"
    if not should_run and active:
        subprocess.run(["/usr/bin/systemctl", "stop", service], check=True)
        return "stopped"
    return "already_active" if active else "already_inactive"


def should_collect(
    now: datetime,
    session: TradingSession,
    previous_session: TradingSession | None = None,
) -> bool:
    """Include a short connection warm-up and the final exchange timestamp."""

    warm_start = session.start - timedelta(minutes=2)
    current = session.is_trading_day and warm_start <= now < session.end
    previous_tail = bool(
        previous_session is not None
        and previous_session.is_trading_day
        and previous_session.end <= now < previous_session.end + timedelta(minutes=1)
    )
    return current or previous_tail


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service", default="neobitcoin-research.service")
    parser.add_argument(
        "--cache",
        type=Path,
        default=Path("/var/lib/neobitcoin-research/server_control/schedule.json"),
    )
    parser.add_argument(
        "--status-file",
        type=Path,
        default=Path("/var/lib/neobitcoin-research/server_control/status.json"),
    )
    parser.add_argument(
        "--token-file",
        type=Path,
        default=Path(
            os.environ.get(
                "NEOBITCOIN_RESEARCH_TOKEN_FILE",
                "/etc/neobitcoin-research/tbank-token.txt",
            )
        ),
    )
    parser.add_argument("--now", help="ISO timestamp for a dry-run/test")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    now = datetime.fromisoformat(args.now) if args.now else datetime.now(MOSCOW)
    if now.tzinfo is None:
        now = now.replace(tzinfo=MOSCOW)
    now = now.astimezone(MOSCOW)
    session, fetch_error = resolve_session(
        now.date(), cache_path=args.cache, token_file=args.token_file, now=now
    )
    cached = load_cached_sessions(args.cache)
    previous = cached.get((now.date() - timedelta(days=1)).isoformat())
    should_run = should_collect(now, session, previous)
    action = "dry_run"
    if not args.dry_run:
        action = apply_service_state(args.service, should_run)
    status = {
        "checked_at": now.isoformat(),
        "service": args.service,
        "should_run": should_run,
        "action": action,
        "session": session.as_dict(),
        "previous_session": previous.as_dict() if previous is not None else None,
        "schedule_fetch_error": fetch_error,
    }
    _atomic_json(args.status_file, status)
    print(json.dumps(status, ensure_ascii=False, sort_keys=True))
    return 0


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


if __name__ == "__main__":
    raise SystemExit(main())
