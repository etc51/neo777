"""Instrument metadata for the Neo Universal Bot Swarm."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, TypeAlias

import yaml

from neo_trader.neo_universal_swarm.types import SwarmInstrument

JsonMapping: TypeAlias = Mapping[str, Any]
ROOT: Final = Path(__file__).resolve().parents[2]
CONFIG_DIR: Final = ROOT / "configs"
DEFAULT_SWARM_INSTRUMENTS_CONFIG: Final = CONFIG_DIR / "neo_universal_swarm_instruments.yaml"


@dataclass(frozen=True)
class SwarmInstrumentMetadata:
    """Broker and display identifiers for one swarm instrument."""

    instrument: SwarmInstrument
    tbank_query: str
    ticker: str
    class_code: str
    figi: str
    uid: str
    position_uid: str
    enabled: bool = True


@dataclass(frozen=True)
class SwarmInstrumentCatalog:
    """Configured swarm instrument metadata."""

    instruments: tuple[SwarmInstrumentMetadata, ...]

    def __post_init__(self) -> None:
        configured = {item.instrument for item in self.instruments}
        expected = set(SwarmInstrument)
        missing = expected - configured
        if missing:
            names = ", ".join(sorted(item.value for item in missing))
            raise ValueError(f"missing swarm instrument metadata: {names}")

    def by_instrument(self) -> dict[SwarmInstrument, SwarmInstrumentMetadata]:
        return {item.instrument: item for item in self.instruments}

    def get(self, instrument: SwarmInstrument) -> SwarmInstrumentMetadata:
        return self.by_instrument()[instrument]


def load_swarm_instrument_catalog(
    path: Path | str | None = None,
) -> SwarmInstrumentCatalog:
    """Load ``configs/neo_universal_swarm_instruments.yaml``."""

    raw = _load_yaml_mapping(Path(path) if path is not None else DEFAULT_SWARM_INSTRUMENTS_CONFIG)
    instruments_raw = _mapping(_required(raw, "instruments"), "instruments")
    return SwarmInstrumentCatalog(
        instruments=tuple(
            _instrument_from_mapping(SwarmInstrument(key), _mapping(value, key))
            for key, value in sorted(instruments_raw.items())
        )
    )


def _instrument_from_mapping(
    instrument: SwarmInstrument,
    raw: JsonMapping,
) -> SwarmInstrumentMetadata:
    return SwarmInstrumentMetadata(
        instrument=instrument,
        tbank_query=_string(_required(raw, "tbank_query"), "tbank_query"),
        ticker=_string(_required(raw, "ticker"), "ticker"),
        class_code=_string(_required(raw, "class_code"), "class_code"),
        figi=_string(_required(raw, "figi"), "figi"),
        uid=_string(_required(raw, "uid"), "uid"),
        position_uid=_string(_required(raw, "position_uid"), "position_uid"),
        enabled=_bool(raw.get("enabled", True), "enabled"),
    )


def _load_yaml_mapping(path: Path) -> JsonMapping:
    if not path.exists():
        raise FileNotFoundError(f"swarm instruments config not found: {path}")
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    return _mapping(loaded, str(path))


def _required(raw: JsonMapping, key: str) -> object:
    value = raw.get(key)
    if value is None:
        raise ValueError(f"missing required instrument field: {key}")
    return value


def _mapping(value: object, field_name: str) -> JsonMapping:
    if not isinstance(value, Mapping):
        raise TypeError(f"{field_name} must be a mapping.")
    return value


def _string(value: object, field_name: str) -> str:
    if isinstance(value, str) and value:
        return value
    raise TypeError(f"{field_name} must be a non-empty string.")


def _bool(value: object, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise TypeError(f"{field_name} must be a bool.")


__all__ = [
    "SwarmInstrumentCatalog",
    "SwarmInstrumentMetadata",
    "load_swarm_instrument_catalog",
]
