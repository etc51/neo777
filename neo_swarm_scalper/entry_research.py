"""Offline forward-outcome labeling for entry-strategy research."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from neo_swarm_scalper.entry_engine import EntryCandidate, EntryTypeEngine
from neo_swarm_scalper.storage import SQLiteJournal
from neo_swarm_scalper.types import BookLevel, InstrumentMetadata, MarketSnapshot, PositionSide

DEFAULT_WINDOWS_SEC = (5, 15, 30, 60, 120)
DEFAULT_STOP_TICKS = (2, 3, 4, 5, 7, 10)


@dataclass(frozen=True)
class EntryResearchSummary:
    run_id: str
    candidates_scored: int
    labels_written: int
    best_entry_type: str | None
    best_stop_ticks: int | None
    mfe3_rate: Decimal
    mfe5_rate: Decimal
    mfe10_rate: Decimal


@dataclass(frozen=True)
class _QuotePoint:
    quote_id: int
    timestamp_utc: datetime
    instrument: str
    bid: Decimal | None
    ask: Decimal | None
    mid: Decimal
    spread_abs: Decimal | None
    spread_bps: Decimal
    tick_size: Decimal
    micro: dict[str, Any]
    volatility: dict[str, Any]


def run_entry_research(
    storage: SQLiteJournal,
    *,
    windows_sec: Sequence[int] = DEFAULT_WINDOWS_SEC,
    stop_ticks: Sequence[int] = DEFAULT_STOP_TICKS,
    limit_per_instrument: int = 500,
    since_utc: datetime | None = None,
) -> EntryResearchSummary:
    """Label historical entry candidates from SQLite without touching live decisions."""

    storage.initialize()
    run_id = f"ENTRY_RESEARCH_{uuid4().hex}"
    points_by_instrument = _load_points(
        storage,
        limit_per_instrument=limit_per_instrument,
        since_utc=since_utc,
    )
    engine = EntryTypeEngine()
    created_at = datetime.now(UTC).isoformat()
    labels_written = 0
    candidates_scored = 0
    with storage.connect() as conn:
        for points in points_by_instrument.values():
            history: list[tuple[datetime, Decimal]] = []
            for index, point in enumerate(points):
                history.append((point.timestamp_utc, point.mid))
                candidates = engine.score_candidates(
                    snapshot=_snapshot_from_point(point),
                    micro=point.micro,
                    volatility=point.volatility,
                    price_history=history,
                    peer_contexts={},
                )
                for candidate in candidates:
                    candidates_scored += 1
                    _insert_strategy_score(conn, run_id, point, candidate, created_at)
                    future = points[index + 1 :]
                    if not future:
                        continue
                    for horizon_sec in windows_sec:
                        horizon_points = _future_window(point, future, horizon_sec)
                        if not horizon_points:
                            continue
                        for stop in stop_ticks:
                            outcome = _forward_outcome(point, horizon_points, candidate.side, stop)
                            _insert_forward_label(
                                conn,
                                run_id,
                                point,
                                candidate,
                                stop,
                                horizon_sec,
                                outcome,
                                created_at,
                            )
                            _insert_research_label(
                                conn,
                                run_id,
                                point,
                                candidate,
                                stop,
                                horizon_sec,
                                outcome,
                                created_at,
                            )
                            labels_written += 1
        conn.commit()
    aggregate = _aggregate_summary(storage, run_id)
    return EntryResearchSummary(
        run_id=run_id,
        candidates_scored=candidates_scored,
        labels_written=labels_written,
        best_entry_type=aggregate["best_entry_type"],
        best_stop_ticks=aggregate["best_stop_ticks"],
        mfe3_rate=aggregate["mfe3_rate"],
        mfe5_rate=aggregate["mfe5_rate"],
        mfe10_rate=aggregate["mfe10_rate"],
    )


def _load_points(
    storage: SQLiteJournal,
    *,
    limit_per_instrument: int,
    since_utc: datetime | None,
) -> dict[str, list[_QuotePoint]]:
    ticks = {
        str(row["name"]): _dec(row["tick_size"], Decimal("0.01"))
        for row in storage.fetch_all("SELECT name, tick_size FROM instruments")
    }
    where = "WHERE rq.mid IS NOT NULL"
    params: list[object] = []
    if since_utc is not None:
        where += " AND rq.timestamp_utc >= ?"
        params.append(since_utc.isoformat())
    rows = storage.fetch_all(
        f"""
        SELECT *
        FROM (
            SELECT
                rq.id AS quote_id,
                rq.timestamp_utc,
                rq.instrument,
                rq.bid,
                rq.ask,
                rq.mid,
                rq.spread_abs,
                rq.spread_bps,
                mf.spread_ticks,
                mf.top_bid_qty,
                mf.top_ask_qty,
                mf.depth_bid_3,
                mf.depth_bid_5,
                mf.depth_bid_10,
                mf.depth_ask_3,
                mf.depth_ask_5,
                mf.depth_ask_10,
                mf.orderbook_imbalance_3,
                mf.orderbook_imbalance_5,
                mf.orderbook_imbalance_10,
                mf.microprice,
                mf.microprice_deviation,
                mf.pressure_score,
                mf.liquidity_score,
                mf.book_slope,
                mf.wall_detect_bid,
                mf.wall_detect_ask,
                mf.spread_expansion_flag,
                mf.spread_compression_flag,
                mf.quote_stability,
                mf.orderbook_flip_flag,
                mf.stale_orderbook_flag,
                mf.thin_book_flag,
                vf.tick_velocity,
                vf.price_velocity,
                vf.acceleration,
                vf.range_position,
                vf.breakout_flag,
                vf.range_flag,
                vf.impulse_score,
                vf.chop_score,
                vf.volatility_regime,
                ROW_NUMBER() OVER (
                    PARTITION BY rq.instrument
                    ORDER BY rq.timestamp_utc DESC, rq.id DESC
                ) AS rn
            FROM raw_quotes rq
            LEFT JOIN microstructure_features mf
              ON mf.instrument = rq.instrument
             AND mf.timestamp_utc = rq.timestamp_utc
            LEFT JOIN volatility_features vf
              ON vf.instrument = rq.instrument
             AND vf.timestamp_utc = rq.timestamp_utc
            {where}
        )
        WHERE rn <= ?
        ORDER BY instrument, timestamp_utc, quote_id
        """,
        (*params, limit_per_instrument),
    )
    result: dict[str, list[_QuotePoint]] = defaultdict(list)
    for row in rows:
        instrument = str(row["instrument"])
        bid = _dec_or_none(row["bid"])
        ask = _dec_or_none(row["ask"])
        mid = _dec(row["mid"])
        tick = ticks.get(instrument, _infer_tick(row, bid, ask))
        result[instrument].append(
            _QuotePoint(
                quote_id=int(row["quote_id"]),
                timestamp_utc=_dt(row["timestamp_utc"]),
                instrument=instrument,
                bid=bid,
                ask=ask,
                mid=mid,
                spread_abs=_dec_or_none(row["spread_abs"]),
                spread_bps=_dec(row["spread_bps"]),
                tick_size=tick,
                micro=_micro_from_row(row, bid, ask, mid, tick),
                volatility=_volatility_from_row(row),
            )
        )
    return dict(result)


def _snapshot_from_point(point: _QuotePoint) -> MarketSnapshot:
    bid = point.bid if point.bid is not None else point.mid - point.tick_size
    ask = point.ask if point.ask is not None else point.mid + point.tick_size
    qty_bid = _dec(point.micro.get("top_bid_qty"), Decimal("100"))
    qty_ask = _dec(point.micro.get("top_ask_qty"), Decimal("100"))
    metadata = InstrumentMetadata(
        name=point.instrument,
        display_name=point.instrument,
        ticker=point.instrument,
        figi=point.instrument,
        class_code="RESEARCH",
        min_price_increment=point.tick_size,
        trading_status="normal_trading",
    )
    return MarketSnapshot(
        timestamp_utc=point.timestamp_utc,
        instrument=point.instrument,
        metadata=metadata,
        last_price=point.mid,
        bid_levels=(BookLevel(bid, qty_bid),),
        ask_levels=(BookLevel(ask, qty_ask),),
        stale=False,
        orderbook_missing=False,
        raw={"source": "entry_research"},
    )


def _future_window(
    point: _QuotePoint,
    future: Sequence[_QuotePoint],
    horizon_sec: int,
) -> list[_QuotePoint]:
    cutoff = point.timestamp_utc.timestamp() + horizon_sec
    return [item for item in future if item.timestamp_utc.timestamp() <= cutoff]


def _forward_outcome(
    entry: _QuotePoint,
    future: Sequence[_QuotePoint],
    side: PositionSide,
    stop_ticks: int,
) -> dict[str, Any]:
    entry_price = _entry_price(entry, side)
    tick = entry.tick_size
    thresholds = {
        "mfe3": Decimal("3"),
        "mfe5": Decimal("5"),
        "mfe10": Decimal("10"),
        "mfe_005pct": (entry_price * Decimal("0.0005")) / tick,
        "mfe_010pct": (entry_price * Decimal("0.0010")) / tick,
        "mfe_015pct": (entry_price * Decimal("0.0015")) / tick,
    }
    hit_times: dict[str, Decimal | None] = {key: None for key in thresholds}
    max_mfe = Decimal("0")
    max_mae = Decimal("0")
    time_to_stop: Decimal | None = None
    first_mfe_time: Decimal | None = None
    stop_hit_before_mfe = False
    for point in future:
        elapsed = Decimal(str((point.timestamp_utc - entry.timestamp_utc).total_seconds()))
        pnl_ticks = _pnl_ticks(side, entry_price, _exit_price(point, side), tick)
        max_mfe = max(max_mfe, pnl_ticks)
        max_mae = min(max_mae, pnl_ticks)
        if time_to_stop is None and pnl_ticks <= -Decimal(stop_ticks):
            time_to_stop = elapsed
            if first_mfe_time is None:
                stop_hit_before_mfe = True
        for name, threshold in thresholds.items():
            if hit_times[name] is None and pnl_ticks >= threshold:
                hit_times[name] = elapsed
                if first_mfe_time is None:
                    first_mfe_time = elapsed
    return {
        "stop_hit_before_mfe": stop_hit_before_mfe,
        "mfe3_hit": hit_times["mfe3"] is not None,
        "mfe5_hit": hit_times["mfe5"] is not None,
        "mfe10_hit": hit_times["mfe10"] is not None,
        "mfe_005pct_hit": hit_times["mfe_005pct"] is not None,
        "mfe_010pct_hit": hit_times["mfe_010pct"] is not None,
        "mfe_015pct_hit": hit_times["mfe_015pct"] is not None,
        "max_mfe_ticks": max_mfe,
        "max_mae_ticks": max_mae,
        "time_to_mfe_sec": first_mfe_time,
        "time_to_stop_sec": time_to_stop,
        "direction_correct": max_mfe > Decimal("0"),
        "label": (
            f"good_{side.value.lower()}"
            if hit_times["mfe3"] is not None and not stop_hit_before_mfe
            else f"bad_{side.value.lower()}"
        ),
    }


def _insert_strategy_score(
    conn: Any,
    run_id: str,
    point: _QuotePoint,
    candidate: EntryCandidate,
    created_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO entry_strategy_scores(
            run_id, timestamp_utc, instrument, entry_type, side, direction_model,
            direction_score, pressure_score, impulse_score, pullback_score,
            book_flip_score, reversal_score, lead_lag_score, expected_mfe_ticks,
            expected_stop_risk_ticks, diagnostics_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            point.timestamp_utc.isoformat(),
            point.instrument,
            candidate.entry_type,
            candidate.side.value,
            candidate.direction_model,
            _num(candidate.direction_score),
            _num(candidate.pressure_score),
            _num(candidate.impulse_score),
            _num(candidate.pullback_score),
            _num(candidate.book_flip_score),
            _num(candidate.reversal_score),
            _num(candidate.lead_lag_score),
            _num(candidate.expected_mfe_ticks),
            _num(candidate.expected_stop_risk_ticks),
            _json(candidate.diagnostics),
            created_at,
        ),
    )


def _insert_forward_label(
    conn: Any,
    run_id: str,
    point: _QuotePoint,
    candidate: EntryCandidate,
    stop_ticks: int,
    horizon_sec: int,
    outcome: dict[str, Any],
    created_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO forward_outcome_labels(
            run_id, source_table, source_id, timestamp_utc, instrument, entry_type,
            side, stop_ticks, horizon_sec, stop_hit_before_mfe, mfe3_hit, mfe5_hit,
            mfe10_hit, mfe_005pct_hit, mfe_010pct_hit, mfe_015pct_hit, max_mfe_ticks,
            max_mae_ticks, time_to_mfe_sec, time_to_stop_sec, direction_correct,
            created_at
        )
        VALUES (?, 'raw_quotes', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        _label_params(run_id, point, candidate, stop_ticks, horizon_sec, outcome, created_at),
    )


def _insert_research_label(
    conn: Any,
    run_id: str,
    point: _QuotePoint,
    candidate: EntryCandidate,
    stop_ticks: int,
    horizon_sec: int,
    outcome: dict[str, Any],
    created_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO entry_research_labels(
            run_id, timestamp_utc, instrument, entry_type, side, stop_ticks, horizon_sec,
            label, direction_correct, stop_hit_before_mfe, mfe3_hit, mfe5_hit, mfe10_hit,
            mfe_005pct_hit, mfe_010pct_hit, mfe_015pct_hit, max_mfe_ticks, max_mae_ticks,
            time_to_mfe_sec, time_to_stop_sec, features_json, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            point.timestamp_utc.isoformat(),
            point.instrument,
            candidate.entry_type,
            candidate.side.value,
            stop_ticks,
            horizon_sec,
            outcome["label"],
            int(bool(outcome["direction_correct"])),
            int(bool(outcome["stop_hit_before_mfe"])),
            int(bool(outcome["mfe3_hit"])),
            int(bool(outcome["mfe5_hit"])),
            int(bool(outcome["mfe10_hit"])),
            int(bool(outcome["mfe_005pct_hit"])),
            int(bool(outcome["mfe_010pct_hit"])),
            int(bool(outcome["mfe_015pct_hit"])),
            _num(outcome["max_mfe_ticks"]),
            _num(outcome["max_mae_ticks"]),
            _num(outcome["time_to_mfe_sec"]),
            _num(outcome["time_to_stop_sec"]),
            _json({"micro": point.micro, "volatility": point.volatility}),
            created_at,
        ),
    )


def _label_params(
    run_id: str,
    point: _QuotePoint,
    candidate: EntryCandidate,
    stop_ticks: int,
    horizon_sec: int,
    outcome: dict[str, Any],
    created_at: str,
) -> tuple[Any, ...]:
    return (
        run_id,
        point.quote_id,
        point.timestamp_utc.isoformat(),
        point.instrument,
        candidate.entry_type,
        candidate.side.value,
        stop_ticks,
        horizon_sec,
        int(bool(outcome["stop_hit_before_mfe"])),
        int(bool(outcome["mfe3_hit"])),
        int(bool(outcome["mfe5_hit"])),
        int(bool(outcome["mfe10_hit"])),
        int(bool(outcome["mfe_005pct_hit"])),
        int(bool(outcome["mfe_010pct_hit"])),
        int(bool(outcome["mfe_015pct_hit"])),
        _num(outcome["max_mfe_ticks"]),
        _num(outcome["max_mae_ticks"]),
        _num(outcome["time_to_mfe_sec"]),
        _num(outcome["time_to_stop_sec"]),
        int(bool(outcome["direction_correct"])),
        created_at,
    )


def _aggregate_summary(storage: SQLiteJournal, run_id: str) -> dict[str, Any]:
    best_entry = storage.fetch_all(
        """
        SELECT entry_type,
               AVG(mfe3_hit) AS mfe3_rate,
               AVG(mfe5_hit) AS mfe5_rate,
               AVG(mfe10_hit) AS mfe10_rate
        FROM entry_research_labels
        WHERE run_id = ?
        GROUP BY entry_type
        ORDER BY mfe3_rate DESC, mfe5_rate DESC, mfe10_rate DESC
        LIMIT 1
        """,
        (run_id,),
    )
    best_stop = storage.fetch_all(
        """
        SELECT stop_ticks, AVG(mfe3_hit) AS mfe3_rate
        FROM entry_research_labels
        WHERE run_id = ?
        GROUP BY stop_ticks
        ORDER BY mfe3_rate DESC, stop_ticks
        LIMIT 1
        """,
        (run_id,),
    )
    if not best_entry:
        return {
            "best_entry_type": None,
            "best_stop_ticks": None,
            "mfe3_rate": Decimal("0"),
            "mfe5_rate": Decimal("0"),
            "mfe10_rate": Decimal("0"),
        }
    row = best_entry[0]
    return {
        "best_entry_type": str(row["entry_type"]),
        "best_stop_ticks": None if not best_stop else int(best_stop[0]["stop_ticks"]),
        "mfe3_rate": _dec(row["mfe3_rate"]),
        "mfe5_rate": _dec(row["mfe5_rate"]),
        "mfe10_rate": _dec(row["mfe10_rate"]),
    }


def _micro_from_row(
    row: Any,
    bid: Decimal | None,
    ask: Decimal | None,
    mid: Decimal,
    tick: Decimal,
) -> dict[str, Any]:
    spread_abs = _dec_or_none(row["spread_abs"])
    spread_ticks = _dec_or_none(row["spread_ticks"])
    if spread_ticks is None and spread_abs is not None and tick:
        spread_ticks = spread_abs / tick
    return {
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread_abs": spread_abs,
        "spread_ticks": spread_ticks,
        "spread_bps": _dec(row["spread_bps"]),
        "top_bid_qty": _dec(row["top_bid_qty"], Decimal("100")),
        "top_ask_qty": _dec(row["top_ask_qty"], Decimal("100")),
        "depth_bid_3": _dec(row["depth_bid_3"], Decimal("100")),
        "depth_bid_5": _dec(row["depth_bid_5"], Decimal("100")),
        "depth_bid_10": _dec(row["depth_bid_10"], Decimal("100")),
        "depth_ask_3": _dec(row["depth_ask_3"], Decimal("100")),
        "depth_ask_5": _dec(row["depth_ask_5"], Decimal("100")),
        "depth_ask_10": _dec(row["depth_ask_10"], Decimal("100")),
        "orderbook_imbalance_3": _dec(row["orderbook_imbalance_3"]),
        "orderbook_imbalance_5": _dec(row["orderbook_imbalance_5"]),
        "orderbook_imbalance_10": _dec(row["orderbook_imbalance_10"]),
        "microprice": _dec(row["microprice"], mid),
        "microprice_deviation": _dec(row["microprice_deviation"]),
        "pressure_score": _dec(row["pressure_score"]),
        "liquidity_score": _dec(row["liquidity_score"], Decimal("200")),
        "book_slope": _dec(row["book_slope"]),
        "wall_detect_bid": bool(row["wall_detect_bid"] or False),
        "wall_detect_ask": bool(row["wall_detect_ask"] or False),
        "spread_expansion_flag": bool(row["spread_expansion_flag"] or False),
        "spread_compression_flag": bool(row["spread_compression_flag"] or False),
        "quote_stability": _dec(row["quote_stability"], Decimal("1")),
        "orderbook_flip_flag": bool(row["orderbook_flip_flag"] or False),
        "stale_orderbook_flag": bool(row["stale_orderbook_flag"] or False),
        "thin_book_flag": bool(row["thin_book_flag"] or False),
    }


def _volatility_from_row(row: Any) -> dict[str, Any]:
    return {
        "tick_velocity": _dec(row["tick_velocity"]),
        "price_velocity": _dec(row["price_velocity"]),
        "acceleration": _dec(row["acceleration"]),
        "range_position": _dec(row["range_position"], Decimal("0.5")),
        "breakout_flag": bool(row["breakout_flag"] or False),
        "range_flag": bool(row["range_flag"] or False),
        "impulse_score": _dec(row["impulse_score"]),
        "chop_score": _dec(row["chop_score"], Decimal("1")),
        "volatility_regime": str(row["volatility_regime"] or "normal"),
    }


def _entry_price(point: _QuotePoint, side: PositionSide) -> Decimal:
    if side is PositionSide.LONG:
        return point.ask if point.ask is not None else point.mid
    return point.bid if point.bid is not None else point.mid


def _exit_price(point: _QuotePoint, side: PositionSide) -> Decimal:
    if side is PositionSide.LONG:
        return point.bid if point.bid is not None else point.mid
    return point.ask if point.ask is not None else point.mid


def _pnl_ticks(
    side: PositionSide,
    entry_price: Decimal,
    exit_price: Decimal,
    tick: Decimal,
) -> Decimal:
    pnl_abs = exit_price - entry_price if side is PositionSide.LONG else entry_price - exit_price
    return pnl_abs / tick if tick else Decimal("0")


def _infer_tick(row: Any, bid: Decimal | None, ask: Decimal | None) -> Decimal:
    spread_abs = _dec_or_none(row["spread_abs"])
    if bid is not None and ask is not None and ask > bid:
        return max((ask - bid) / Decimal("2"), Decimal("0.0001"))
    if spread_abs is not None and spread_abs > 0:
        return max(spread_abs / Decimal("2"), Decimal("0.0001"))
    return Decimal("0.01")


def _dec(value: object, default: Decimal = Decimal("0")) -> Decimal:
    if value is None:
        return default
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _dec_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    return _dec(value)


def _num(value: object) -> float | None:
    if value is None:
        return None
    return float(_dec(value))


def _dt(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _json(value: object) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


__all__ = ["EntryResearchSummary", "run_entry_research"]
