"""Create an idempotent, validated whole-session Raw JSONL archive."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import sys
import zipfile
from collections import Counter
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.neobitcoin_research.safety import load_tbank_token  # noqa: E402
from scripts.neobitcoin_server_schedule import (  # noqa: E402
    TradingSession,
    fallback_session,
    load_cached_sessions,
)

MOSCOW = ZoneInfo("Europe/Moscow")
MARKET_EVENT_TYPES = frozenset({"orderbook", "trade", "last_price", "candle", "open_interest"})
RAW_MEMBER = "raw/events.jsonl"


class SessionArchiveError(RuntimeError):
    """Raised when a whole-session archive cannot be safely published."""


def create_session_archive(
    data_root: Path,
    output_dir: Path,
    session: TradingSession,
    *,
    token_file: Path | None = None,
    now: datetime | None = None,
    max_tail_gap_minutes: float = 20.0,
) -> dict[str, object]:
    now = (now or datetime.now(MOSCOW)).astimezone(MOSCOW)
    if now < session.end:
        raise SessionArchiveError("session has not ended")
    if not session.is_trading_day:
        raise SessionArchiveError("calendar marks this as a non-trading day")

    output_dir.mkdir(parents=True, exist_ok=True)
    name = f"neobitcoin_session_{session.session_date.isoformat()}_raw-safe.zip"
    final = output_dir / name
    sidecar = output_dir / f"{name}.sha256"
    latest = output_dir / "latest.json"
    if final.exists():
        manifest = verify_session_archive(final)
        archive_sha = _sha256_file(final)
        _publish_latest(latest, final, sidecar, archive_sha, manifest)
        return {**manifest, "archive": str(final), "archive_sha256": archive_sha, "existing": True}

    partial = output_dir / f".{name}.{os.getpid()}.inprogress"
    secret = _secret_bytes(token_file)
    counts: Counter[str] = Counter()
    duplicate_count = 0
    seen_ids: set[str] = set()
    raw_sha = hashlib.sha256()
    raw_bytes = 0
    first_receive: datetime | None = None
    last_receive: datetime | None = None
    first_market: datetime | None = None
    last_market: datetime | None = None
    source_files = _candidate_raw_files(data_root, session)
    if not source_files:
        raise SessionArchiveError("no Raw JSONL files found for the session")

    capture_end = session.end + timedelta(minutes=5)
    try:
        with zipfile.ZipFile(
            partial,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=6,
            allowZip64=True,
        ) as archive:
            with archive.open(RAW_MEMBER, "w", force_zip64=True) as raw_member:
                for path in source_files:
                    with path.open("rb") as source:
                        for line_number, line in enumerate(source, start=1):
                            if not line.strip():
                                continue
                            try:
                                record = json.loads(line)
                            except json.JSONDecodeError as exc:
                                raise SessionArchiveError(
                                    f"invalid JSONL in {path.name}:{line_number}"
                                ) from exc
                            receive = _record_timestamp(record, "receive_timestamp")
                            if receive is None:
                                raise SessionArchiveError("raw record has no receive_timestamp")
                            receive_msk = receive.astimezone(MOSCOW)
                            if receive_msk < session.start or receive_msk > capture_end:
                                continue
                            if secret and secret in line:
                                raise SessionArchiveError("token material detected in Raw JSONL")
                            raw_member.write(line)
                            raw_sha.update(line)
                            raw_bytes += len(line)
                            first_receive = first_receive or receive
                            last_receive = receive
                            event_type = str(record.get("event_type", "unknown"))
                            counts[event_type] += 1
                            event_id = record.get("event_id")
                            if isinstance(event_id, str):
                                if event_id in seen_ids:
                                    duplicate_count += 1
                                else:
                                    seen_ids.add(event_id)
                            if event_type in MARKET_EVENT_TYPES:
                                exchange = (
                                    _record_timestamp(record, "exchange_timestamp") or receive
                                )
                                if exchange.astimezone(MOSCOW) <= capture_end:
                                    first_market = first_market or exchange
                                    last_market = exchange

            market_rows = sum(counts[item] for item in MARKET_EVENT_TYPES)
            if market_rows == 0 or first_market is None or last_market is None:
                raise SessionArchiveError("no market events found for the session")
            tail_gap = max((session.end.astimezone(UTC) - last_market).total_seconds(), 0.0)
            if tail_gap > max_tail_gap_minutes * 60:
                raise SessionArchiveError(
                    f"last market event is {tail_gap:.0f}s before session end"
                )
            manifest: dict[str, object] = {
                "schema": "neobitcoin-session-raw-safe-v1",
                "validation": "PASS",
                "session_date": session.session_date.isoformat(),
                "session_start": session.start.isoformat(),
                "session_end": session.end.isoformat(),
                "schedule_source": session.source,
                "created_at": datetime.now(UTC).isoformat(),
                "raw_member": RAW_MEMBER,
                "raw_sha256": raw_sha.hexdigest(),
                "raw_bytes": raw_bytes,
                "rows": sum(counts.values()),
                "market_rows": market_rows,
                "duplicate_event_ids": duplicate_count,
                "event_type_counts": dict(sorted(counts.items())),
                "first_receive_timestamp": _iso(first_receive),
                "last_receive_timestamp": _iso(last_receive),
                "first_market_timestamp": _iso(first_market),
                "last_market_timestamp": _iso(last_market),
                "tail_gap_seconds": tail_gap,
                "source_file_count": len(source_files),
                "excluded": [
                    "state.sqlite",
                    "*.parquet.inprogress",
                    "active derived Parquet",
                ],
            }
            archive.writestr(
                "MANIFEST.json",
                json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            )
            archive.writestr(
                "README.md",
                "# Neobitcoin whole-session archive\n\n"
                "Validated Raw JSONL only. SQLite and active/derived Parquet are excluded.\n",
            )
        verified = verify_session_archive(partial)
        archive_sha = _sha256_file(partial)
        os.replace(partial, final)
        _atomic_text(sidecar, f"{archive_sha}  {name}\n")
        _publish_latest(latest, final, sidecar, archive_sha, verified)
        return {**verified, "archive": str(final), "archive_sha256": archive_sha, "existing": False}
    finally:
        with contextlib.suppress(FileNotFoundError):
            partial.unlink()


def verify_session_archive(path: Path) -> dict[str, object]:
    with zipfile.ZipFile(path, "r") as archive:
        names = archive.namelist()
        if names != [RAW_MEMBER, "MANIFEST.json", "README.md"]:
            raise SessionArchiveError(f"unexpected archive members: {names}")
        bad = archive.testzip()
        if bad is not None:
            raise SessionArchiveError(f"ZIP CRC validation failed: {bad}")
        manifest = json.loads(archive.read("MANIFEST.json"))
        digest = hashlib.sha256()
        byte_count = 0
        row_count = 0
        with archive.open(RAW_MEMBER) as raw:
            while chunk := raw.read(1024 * 1024):
                digest.update(chunk)
                byte_count += len(chunk)
                row_count += chunk.count(b"\n")
        if manifest.get("validation") != "PASS":
            raise SessionArchiveError("manifest validation is not PASS")
        if digest.hexdigest() != manifest.get("raw_sha256"):
            raise SessionArchiveError("Raw JSONL SHA-256 mismatch")
        if byte_count != manifest.get("raw_bytes") or row_count != manifest.get("rows"):
            raise SessionArchiveError("Raw JSONL size/row count mismatch")
        return manifest


def session_for_date(session_date: date, cache_path: Path) -> TradingSession:
    cached = load_cached_sessions(cache_path)
    return cached.get(session_date.isoformat(), fallback_session(session_date))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path("/var/lib/neobitcoin-research"))
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/var/lib/neobitcoin-research/session_archives"),
    )
    parser.add_argument(
        "--schedule-cache",
        type=Path,
        default=Path("/var/lib/neobitcoin-research/server_control/schedule.json"),
    )
    parser.add_argument("--session-date", help="YYYY-MM-DD; default is yesterday in Moscow")
    parser.add_argument("--token-file", type=Path)
    parser.add_argument("--verify", type=Path)
    args = parser.parse_args(argv)
    if args.verify:
        manifest = verify_session_archive(args.verify)
        print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
        print("validation=PASS")
        return 0
    today = datetime.now(MOSCOW).date()
    session_date = (
        date.fromisoformat(args.session_date) if args.session_date else today - timedelta(days=1)
    )
    session = session_for_date(session_date, args.schedule_cache)
    result = create_session_archive(
        args.data_root,
        args.output_dir,
        session,
        token_file=args.token_file,
    )
    print(f"archive={result['archive']}")
    print(f"sha256={result['archive_sha256']}")
    print(f"rows={result['rows']}")
    print(f"market_rows={result['market_rows']}")
    print("validation=PASS")
    return 0


def _candidate_raw_files(data_root: Path, session: TradingSession) -> list[Path]:
    roots = [data_root / "raw", data_root / "active" / "raw"]
    utc_dates = {
        session.start.astimezone(UTC).date(),
        session.end.astimezone(UTC).date(),
    }
    result: set[Path] = set()
    for root in roots:
        if not root.is_dir():
            continue
        for utc_date in utc_dates:
            result.update(root.glob(f"*/{utc_date.isoformat()}/*/events.jsonl"))
    return sorted(result)


def _record_timestamp(record: object, key: str) -> datetime | None:
    if not isinstance(record, dict):
        return None
    value = record.get(key)
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def _secret_bytes(token_file: Path | None) -> bytes | None:
    if token_file is None:
        return None
    token = load_tbank_token(token_file).get_secret_value().encode("utf-8")
    return token if len(token) >= 16 else None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _publish_latest(
    latest: Path,
    archive: Path,
    sidecar: Path,
    archive_sha: str,
    manifest: dict[str, object],
) -> None:
    payload = {
        "archive": str(archive),
        "sha256_file": str(sidecar),
        "sha256": archive_sha,
        "session_date": manifest["session_date"],
        "session_start": manifest["session_start"],
        "session_end": manifest["session_end"],
        "rows": manifest["rows"],
        "market_rows": manifest["market_rows"],
        "validation": manifest["validation"],
        "published_at": datetime.now(UTC).isoformat(),
    }
    _atomic_text(latest, json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def _atomic_text(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(f"{path.suffix}.{os.getpid()}.tmp")
    temporary.write_text(body, encoding="utf-8")
    os.replace(temporary, path)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


if __name__ == "__main__":
    raise SystemExit(main())
