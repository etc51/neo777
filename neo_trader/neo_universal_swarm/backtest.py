"""Offline order-book replay backtest for Neo Universal Swarm."""

from __future__ import annotations

import csv
import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

from neo_trader.neo_universal_swarm.bots import CuratorBot
from neo_trader.neo_universal_swarm.config import DEFAULT_ACCOUNTS_CONFIG, load_accounts_config
from neo_trader.neo_universal_swarm.instruments import load_swarm_instrument_catalog
from neo_trader.neo_universal_swarm.live_paper import (
    LivePaperSwarmConfig,
    _active_pair_payload,
    _has_active_pair_for_instrument,
    _json_safe_mapping,
    _label_payload,
    _LivePairState,
    _new_live_pair_state,
    _predict_for_mode,
    _prediction_payload,
    _snapshot_payload,
    _update_live_pair,
)
from neo_trader.neo_universal_swarm.model import PairEVModel, PairEVModelConfig
from neo_trader.neo_universal_swarm.online_learning import ExperimentalMode, OnlineLearningState
from neo_trader.neo_universal_swarm.simulator import aggregate_pair_metrics
from neo_trader.neo_universal_swarm.types import (
    BookSnapshot,
    PairLabel,
    RejectionReason,
    SwarmInstrument,
    SwarmMetrics,
    as_utc,
)
from neo_trader.runtime import get_runtime_commit_hash


@dataclass(frozen=True)
class OrderbookBacktestConfig:
    """Settings for replaying historical order-book events through the paper swarm."""

    accounts_path: Path = DEFAULT_ACCOUNTS_CONFIG
    sources: tuple[Path, ...] = (Path("data/raw"),)
    reports_dir: Path = Path("data/reports/neo_universal_swarm_backtest")
    instruments: tuple[SwarmInstrument, ...] = tuple(SwarmInstrument)
    min_required_ev_ticks: Decimal = Decimal("1")
    slippage_stress_ticks: Decimal = Decimal("0")
    max_pair_age_seconds: float = 300.0
    max_snapshots: int | None = None

    def __post_init__(self) -> None:
        if not self.sources:
            raise ValueError("sources must not be empty.")
        if not self.instruments:
            raise ValueError("instruments must not be empty.")
        if self.min_required_ev_ticks < 0:
            raise ValueError("min_required_ev_ticks must be non-negative.")
        if self.slippage_stress_ticks < 0:
            raise ValueError("slippage_stress_ticks must be non-negative.")
        if self.max_pair_age_seconds <= 0:
            raise ValueError("max_pair_age_seconds must be positive.")
        if self.max_snapshots is not None and self.max_snapshots <= 0:
            raise ValueError("max_snapshots must be positive when set.")


@dataclass(frozen=True)
class OrderbookBacktestArtifacts:
    """Files produced by an offline order-book backtest."""

    summary_json_path: Path
    labels_jsonl_path: Path
    labels_csv_path: Path
    predictions_jsonl_path: Path
    snapshots_jsonl_path: Path
    pair_events_jsonl_path: Path


@dataclass(frozen=True)
class OrderbookBacktestResult:
    """Completed offline order-book backtest."""

    metrics: SwarmMetrics
    labels: tuple[PairLabel, ...]
    learning_state: OnlineLearningState
    artifacts: OrderbookBacktestArtifacts
    snapshots_read: int
    snapshots_used: int
    invalid_snapshots: int
    opened_pairs: int
    predictions: int
    commit_hash: str


@dataclass(frozen=True)
class _RawOrderBookEvent:
    timestamp: datetime
    received_at: datetime
    instrument: SwarmInstrument
    bids: tuple[Mapping[str, object], ...]
    asks: tuple[Mapping[str, object], ...]
    last_price: Decimal | None
    source: str


def run_orderbook_backtest(
    config: OrderbookBacktestConfig | None = None,
) -> OrderbookBacktestResult:
    """Replay historical order books through the same paper-only swarm lifecycle."""

    resolved_config = config or OrderbookBacktestConfig()
    reports_dir = Path(resolved_config.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    artifacts = OrderbookBacktestArtifacts(
        summary_json_path=reports_dir / "summary.json",
        labels_jsonl_path=reports_dir / "pair_labels.jsonl",
        labels_csv_path=reports_dir / "pair_labels.csv",
        predictions_jsonl_path=reports_dir / "model_predictions.jsonl",
        snapshots_jsonl_path=reports_dir / "orderbook_snapshots.jsonl",
        pair_events_jsonl_path=reports_dir / "pair_events.jsonl",
    )
    _reset_artifacts(artifacts)

    accounts = load_accounts_config(resolved_config.accounts_path)
    base_model_config = PairEVModelConfig(
        min_required_ev_ticks=resolved_config.min_required_ev_ticks,
        slippage_stress_ticks=resolved_config.slippage_stress_ticks,
    )
    curator = CuratorBot(accounts=accounts, model=PairEVModel(base_model_config))
    live_config = LivePaperSwarmConfig(
        accounts_path=resolved_config.accounts_path,
        min_required_ev_ticks=resolved_config.min_required_ev_ticks,
        slippage_stress_ticks=resolved_config.slippage_stress_ticks,
        max_pair_age_seconds=resolved_config.max_pair_age_seconds,
        reports_dir=reports_dir,
        instruments=resolved_config.instruments,
    )
    learning_state = OnlineLearningState()
    latest_snapshots: dict[SwarmInstrument, BookSnapshot] = {}
    live_pairs: dict[str, _LivePairState] = {}
    labels: list[PairLabel] = []
    snapshots_used = 0
    opened_pairs = 0
    predictions = 0

    raw_events, invalid_snapshots = _load_raw_events(resolved_config)
    previous: dict[SwarmInstrument, BookSnapshot] = {}
    for cycle, raw_event in enumerate(raw_events, start=1):
        snapshot = _event_to_snapshot(raw_event, previous=previous.get(raw_event.instrument))
        previous[snapshot.instrument] = snapshot
        latest_snapshots[snapshot.instrument] = snapshot
        snapshots_used += 1
        _append_jsonl(
            artifacts.snapshots_jsonl_path,
            {
                "cycle": cycle,
                "source": raw_event.source,
                "valid": True,
                "snapshot": _snapshot_payload(snapshot),
            },
        )

        for label in _update_labels_for_snapshot(
            snapshot=snapshot,
            live_pairs=live_pairs,
            live_config=live_config,
        ):
            _record_closed_label(
                label=label,
                cycle=cycle,
                learning_state=learning_state,
                curator=curator,
                labels=labels,
                live_pairs=live_pairs,
                artifacts=artifacts,
            )

        curator.release_cooldowns()
        if _has_active_pair_for_instrument(live_pairs, snapshot.instrument):
            active_mode = next(
                state.mode
                for state in live_pairs.values()
                if state.pair.instrument is snapshot.instrument
            )
            prediction = _predict_for_mode(
                snapshot,
                mode=active_mode,
                base_config=base_model_config,
            )
            predictions += 1
            _append_jsonl(
                artifacts.predictions_jsonl_path,
                {
                    "cycle": cycle,
                    "instrument": snapshot.instrument.value,
                    "mode": active_mode.value,
                    "skipped_reason": "ACTIVE_PAIR_EXISTS",
                    "prediction": _prediction_payload(prediction),
                    "snapshot": _snapshot_payload(snapshot),
                },
            )
            continue

        mode = learning_state.choose_mode(snapshot.instrument)
        prediction = _predict_for_mode(snapshot, mode=mode, base_config=base_model_config)
        predictions += 1
        active_pair = curator.maybe_open_pair(snapshot, prediction=prediction)
        skipped_reason = None
        if active_pair is None:
            skipped_reason = (
                (prediction.rejection_reason or RejectionReason.MODEL).value
                if prediction is not None
                else RejectionReason.MODEL.value
            )
        _append_jsonl(
            artifacts.predictions_jsonl_path,
            {
                "cycle": cycle,
                "instrument": snapshot.instrument.value,
                "mode": mode.value,
                "opened_pair_id": None if active_pair is None else active_pair.pair_id,
                "skipped_reason": skipped_reason,
                "prediction": _prediction_payload(prediction),
                "snapshot": _snapshot_payload(snapshot),
            },
        )
        if active_pair is None:
            continue
        state = _new_live_pair_state(
            active_pair,
            mode,
            snapshot,
            slippage_stress_ticks=resolved_config.slippage_stress_ticks,
        )
        live_pairs[active_pair.pair_id] = state
        learning_state.record_assignment(active_pair.long_bot_id, active_pair.short_bot_id)
        opened_pairs += 1
        _append_jsonl(
            artifacts.pair_events_jsonl_path,
            {
                "cycle": cycle,
                "event_type": "PAIR_OPENED",
                "mode": mode.value,
                "pair": _active_pair_payload(active_pair),
                "snapshot": _snapshot_payload(snapshot),
            },
        )

    final_cycle = len(raw_events) + 1
    for label in _force_close_open_pairs(
        live_pairs=live_pairs,
        live_config=live_config,
        latest_snapshots=latest_snapshots,
    ):
        _record_closed_label(
            label=label,
            cycle=final_cycle,
            learning_state=learning_state,
            curator=curator,
            labels=labels,
            live_pairs=live_pairs,
            artifacts=artifacts,
        )

    metrics = aggregate_pair_metrics(
        labels,
        bot_to_account=curator.bot_to_account,
        rejected_by_model=curator.rejections.get(RejectionReason.MODEL, 0),
        rejected_by_spread=curator.rejections.get(RejectionReason.SPREAD, 0),
        rejected_by_spread_entry_gate=curator.rejections.get(
            RejectionReason.SPREAD_ENTRY_GATE,
            0,
        ),
        rejected_by_stale_book=curator.rejections.get(RejectionReason.STALE_BOOK, 0),
        rejected_by_chop=curator.rejections.get(RejectionReason.CHOP, 0),
        rejected_by_latency=curator.rejections.get(RejectionReason.LATENCY, 0),
    )
    result = OrderbookBacktestResult(
        metrics=metrics,
        labels=tuple(labels),
        learning_state=learning_state,
        artifacts=artifacts,
        snapshots_read=len(raw_events) + invalid_snapshots,
        snapshots_used=snapshots_used,
        invalid_snapshots=invalid_snapshots,
        opened_pairs=opened_pairs,
        predictions=predictions,
        commit_hash=get_runtime_commit_hash(),
    )
    _write_summary(result, config=resolved_config)
    return result


def _load_raw_events(config: OrderbookBacktestConfig) -> tuple[tuple[_RawOrderBookEvent, ...], int]:
    catalog = load_swarm_instrument_catalog()
    uid_to_instrument = {
        metadata.uid: instrument for instrument, metadata in catalog.by_instrument().items()
    }
    allowed = set(config.instruments)
    events: list[_RawOrderBookEvent] = []
    invalid = 0
    for source_file in _iter_source_files(config.sources):
        try:
            parsed = tuple(
                _iter_parquet_events(source_file, uid_to_instrument=uid_to_instrument)
                if source_file.suffix.lower() == ".parquet"
                else _iter_jsonl_events(source_file, uid_to_instrument=uid_to_instrument)
            )
        except (OSError, ValueError, json.JSONDecodeError):
            invalid += 1
            continue
        for event in parsed:
            if event.instrument not in allowed:
                continue
            if not event.bids or not event.asks:
                invalid += 1
                continue
            events.append(event)
    events.sort(key=lambda event: (event.timestamp, event.instrument.value, event.source))
    if config.max_snapshots is not None:
        events = events[: config.max_snapshots]
    return tuple(events), invalid


def _iter_source_files(sources: Sequence[Path]) -> Iterator[Path]:
    for source in sources:
        path = Path(source)
        if path.is_file():
            if _is_orderbook_source(path):
                yield path
            continue
        if not path.exists():
            continue
        yield from sorted(path.rglob("type=orderbook.parquet"))
        yield from sorted(path.rglob("orderbook_snapshots.jsonl"))


def _is_orderbook_source(path: Path) -> bool:
    return path.name == "type=orderbook.parquet" or path.name.endswith(".jsonl")


def _iter_parquet_events(
    path: Path,
    *,
    uid_to_instrument: Mapping[str, SwarmInstrument],
) -> Iterator[_RawOrderBookEvent]:
    import pandas as pd  # type: ignore[import-untyped]

    frame = pd.read_parquet(path)
    for row in frame.itertuples(index=False):
        payload = json.loads(str(row.payload_json))
        uid = str(payload.get("instrument_uid") or row.instrument_uid)
        instrument = uid_to_instrument.get(uid)
        if instrument is None:
            continue
        timestamp = _parse_datetime(payload.get("time") or row.event_time)
        received_at = _parse_datetime(row.received_at)
        yield _RawOrderBookEvent(
            timestamp=timestamp,
            received_at=received_at,
            instrument=instrument,
            bids=tuple(payload.get("bids") or ()),
            asks=tuple(payload.get("asks") or ()),
            last_price=_optional_decimal(payload.get("last_price")),
            source=str(path),
        )


def _iter_jsonl_events(
    path: Path,
    *,
    uid_to_instrument: Mapping[str, SwarmInstrument],
) -> Iterator[_RawOrderBookEvent]:
    del uid_to_instrument
    with path.open("r", encoding="utf-8") as file:
        for line in file:
            raw_line = line.strip()
            if not raw_line:
                continue
            record = json.loads(raw_line)
            if not isinstance(record, dict) or record.get("valid", True) is False:
                continue
            snapshot = record.get("snapshot")
            if not isinstance(snapshot, dict):
                continue
            yield _RawOrderBookEvent(
                timestamp=_parse_datetime(snapshot["timestamp"]),
                received_at=_parse_datetime(record.get("recorded_at") or snapshot["timestamp"]),
                instrument=SwarmInstrument(str(snapshot["instrument"])),
                bids=tuple(_levels_from_snapshot(snapshot, "bid_levels")),
                asks=tuple(_levels_from_snapshot(snapshot, "ask_levels")),
                last_price=_optional_decimal(snapshot.get("last_price")),
                source=str(path),
            )


def _event_to_snapshot(
    event: _RawOrderBookEvent,
    *,
    previous: BookSnapshot | None,
) -> BookSnapshot:
    best_bid = Decimal(str(event.bids[0]["price"]))
    best_ask = Decimal(str(event.asks[0]["price"]))
    mid_price = (best_bid + best_ask) / Decimal("2")
    previous_mid = previous.mid_price if previous is not None else mid_price
    elapsed_seconds = (
        max((event.timestamp - previous.timestamp).total_seconds(), 1e-9)
        if previous is not None
        else 1.0
    )
    tick_velocity = (mid_price - previous_mid) / Decimal(str(elapsed_seconds))
    tick_direction = 1 if tick_velocity > 0 else -1 if tick_velocity < 0 else 0
    top_bid_quantity = Decimal(str(event.bids[0]["quantity"]))
    top_ask_quantity = Decimal(str(event.asks[0]["quantity"]))
    latency_ms = int(max((event.received_at - event.timestamp).total_seconds() * 1000, 0))
    return BookSnapshot.from_levels(
        timestamp=event.timestamp,
        instrument=event.instrument,
        bids=event.bids,
        asks=event.asks,
        tick_size=Decimal("1"),
        last_price=event.last_price or mid_price,
        last_trade_size=Decimal("0"),
        trade_side=None if tick_direction == 0 else ("LONG" if tick_direction > 0 else "SHORT"),
        tick_direction=tick_direction,
        tick_velocity=tick_velocity,
        volume_delta_1s=top_bid_quantity - top_ask_quantity,
        volume_delta_5s=top_bid_quantity - top_ask_quantity,
        volume_delta_15s=top_bid_quantity - top_ask_quantity,
        volatility_5s=min(abs(tick_velocity), Decimal("5")),
        volatility_15s=min(abs(tick_velocity), Decimal("5")),
        volatility_60s=min(abs(tick_velocity), Decimal("5")),
        latency_ms=latency_ms,
    )


def _update_labels_for_snapshot(
    *,
    snapshot: BookSnapshot,
    live_pairs: Mapping[str, _LivePairState],
    live_config: LivePaperSwarmConfig,
) -> tuple[PairLabel, ...]:
    labels: list[PairLabel] = []
    for state in tuple(live_pairs.values()):
        if state.pair.instrument is not snapshot.instrument:
            continue
        label = _update_live_pair(state, snapshot, config=live_config)
        if label is not None:
            labels.append(label)
    return tuple(labels)


def _force_close_open_pairs(
    *,
    live_pairs: Mapping[str, _LivePairState],
    live_config: LivePaperSwarmConfig,
    latest_snapshots: Mapping[SwarmInstrument, BookSnapshot],
) -> tuple[PairLabel, ...]:
    labels: list[PairLabel] = []
    close_config = LivePaperSwarmConfig(
        accounts_path=live_config.accounts_path,
        min_required_ev_ticks=live_config.min_required_ev_ticks,
        slippage_stress_ticks=live_config.slippage_stress_ticks,
        max_pair_age_seconds=0.000001,
        reports_dir=live_config.reports_dir,
        instruments=live_config.instruments,
    )
    for state in tuple(live_pairs.values()):
        snapshot = (
            state.latest_snapshot
            or latest_snapshots.get(state.pair.instrument)
            or state.entry
        )
        close_snapshot = replace(
            snapshot,
            timestamp=max(
                snapshot.timestamp,
                state.entry.timestamp + timedelta(microseconds=1),
            ),
        )
        label = _update_live_pair(state, close_snapshot, config=close_config)
        if label is not None:
            labels.append(label)
    return tuple(labels)


def _record_closed_label(
    *,
    label: PairLabel,
    cycle: int,
    learning_state: OnlineLearningState,
    curator: CuratorBot,
    labels: list[PairLabel],
    live_pairs: dict[str, _LivePairState],
    artifacts: OrderbookBacktestArtifacts,
) -> None:
    mode = ExperimentalMode(label.regime)
    learning_state.update_label(
        instrument=label.instrument,
        mode=mode,
        label=label,
    )
    curator.settle_pair(label)
    labels.append(label)
    live_pairs.pop(label.pair_id, None)
    payload = {
        "cycle": cycle,
        "mode": mode.value,
        **_label_payload(label),
    }
    _append_jsonl(artifacts.labels_jsonl_path, payload)
    _append_jsonl(
        artifacts.pair_events_jsonl_path,
        {
            "cycle": cycle,
            "event_type": "PAIR_CLOSED",
            "mode": mode.value,
            "label": _label_payload(label),
        },
    )
    _append_label_csv(artifacts.labels_csv_path, label)


def _reset_artifacts(artifacts: OrderbookBacktestArtifacts) -> None:
    for path in (
        artifacts.summary_json_path,
        artifacts.labels_jsonl_path,
        artifacts.labels_csv_path,
        artifacts.predictions_jsonl_path,
        artifacts.snapshots_jsonl_path,
        artifacts.pair_events_jsonl_path,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
    with artifacts.labels_csv_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                "pair_id",
                "instrument",
                "mode",
                "entry_timestamp",
                "exit_timestamp",
                "pair_pnl_ticks",
                "runner_mfe_ticks",
                "runner_mae_ticks",
                "runner_reached_breakeven",
                "fakeout_flag",
                "loser_side",
                "runner_side",
                "exit_reason",
                "long_bot_id",
                "short_bot_id",
            ]
        )


def _append_label_csv(path: Path, label: PairLabel) -> None:
    with path.open("a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(
            [
                label.pair_id,
                label.instrument.value,
                label.regime,
                label.entry_timestamp.isoformat(),
                label.exit_timestamp.isoformat(),
                label.pair_total_pnl_ticks,
                label.runner_mfe_ticks,
                label.runner_mae_ticks,
                label.runner_success_flag,
                label.fakeout_flag,
                None if label.loser_side is None else label.loser_side.value,
                None if label.runner_side is None else label.runner_side.value,
                label.exit_reason.value,
                label.long_bot_id,
                label.short_bot_id,
            ]
        )


def _write_summary(
    result: OrderbookBacktestResult,
    *,
    config: OrderbookBacktestConfig,
) -> None:
    payload = {
        "backtest_id": uuid4().hex,
        "runtime_mode": "offline-orderbook-backtest",
        "commit_hash": result.commit_hash,
        "sources": [str(source) for source in config.sources],
        "instruments": [instrument.value for instrument in config.instruments],
        "snapshots_read": result.snapshots_read,
        "snapshots_used": result.snapshots_used,
        "invalid_snapshots": result.invalid_snapshots,
        "opened_pairs": result.opened_pairs,
        "closed_pairs": result.metrics.total_pairs,
        "predictions": result.predictions,
        "metrics": _metrics_payload(result.metrics),
        "online_learning": result.learning_state.to_payload(),
        "artifacts": {
            "labels_jsonl": str(result.artifacts.labels_jsonl_path),
            "labels_csv": str(result.artifacts.labels_csv_path),
            "predictions_jsonl": str(result.artifacts.predictions_jsonl_path),
            "snapshots_jsonl": str(result.artifacts.snapshots_jsonl_path),
            "pair_events_jsonl": str(result.artifacts.pair_events_jsonl_path),
        },
    }
    result.artifacts.summary_json_path.write_text(
        json.dumps(_json_safe_mapping(payload), ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _metrics_payload(metrics: SwarmMetrics) -> dict[str, object]:
    return {
        "total_pairs": metrics.total_pairs,
        "profitable_pairs": metrics.profitable_pairs,
        "losing_pairs": metrics.losing_pairs,
        "pair_winrate": metrics.pair_winrate,
        "pair_ev_ticks": metrics.pair_ev_ticks,
        "pair_ev_rub": metrics.pair_ev_rub,
        "avg_runner_profit_ticks": metrics.avg_runner_profit_ticks,
        "avg_loser_loss_ticks": metrics.avg_loser_loss_ticks,
        "avg_spread_cost_ticks": metrics.avg_spread_cost_ticks,
        "avg_slippage_ticks": metrics.avg_slippage_ticks,
        "fakeout_rate": metrics.fakeout_rate,
        "runner_to_breakeven_rate": metrics.runner_to_breakeven_rate,
        "runner_trailing_capture_ratio": metrics.runner_trailing_capture_ratio,
        "profit_factor": metrics.profit_factor,
        "max_drawdown": metrics.max_drawdown,
        "pnl_by_bot": metrics.pnl_by_bot,
        "pnl_by_account": metrics.pnl_by_account,
        "pnl_by_instrument": metrics.pnl_by_instrument,
        "pnl_by_regime": metrics.pnl_by_regime,
        "rejected_by_model": metrics.rejected_by_model,
        "rejected_by_spread": metrics.rejected_by_spread,
        "rejected_by_spread_entry_gate": metrics.rejected_by_spread_entry_gate,
        "rejected_by_stale_book": metrics.rejected_by_stale_book,
        "rejected_by_chop": metrics.rejected_by_chop,
        "rejected_by_latency": metrics.rejected_by_latency,
    }


def _append_jsonl(path: Path, payload: Mapping[str, object]) -> None:
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(_json_safe_mapping(payload), ensure_ascii=False, sort_keys=True))
        file.write("\n")


def _levels_from_snapshot(
    snapshot: Mapping[str, object],
    key: str,
) -> tuple[Mapping[str, object], ...]:
    levels = snapshot.get(key)
    if not isinstance(levels, Sequence):
        return ()
    parsed: list[Mapping[str, object]] = []
    for level in levels:
        if isinstance(level, Mapping):
            parsed.append({"price": level["price"], "quantity": level["quantity"]})
    return tuple(parsed)


def _parse_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return as_utc(value)
    return as_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))


def _optional_decimal(value: object) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value))


__all__ = [
    "OrderbookBacktestArtifacts",
    "OrderbookBacktestConfig",
    "OrderbookBacktestResult",
    "run_orderbook_backtest",
]
