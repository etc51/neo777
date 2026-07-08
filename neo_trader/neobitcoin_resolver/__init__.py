"""Dual-Bot Neobitcoin Resolver.

Paper-mode resolver for the T-Bank Neobitcoin neoasset.  The package never
submits broker orders; it evaluates live/read-only market data, simulates the
LONG+SHORT hedge pair, and records the full audit trail locally.
"""

from neo_trader.neobitcoin_resolver.config import (
    BotIds,
    ResolverConfig,
    load_resolver_config,
)
from neo_trader.neobitcoin_resolver.engine import (
    DualBotNeobitcoinResolver,
    ResolverCycleResult,
)
from neo_trader.neobitcoin_resolver.storage import ResolverJournal
from neo_trader.neobitcoin_resolver.types import (
    GateName,
    GateResult,
    GateSnapshot,
    PairState,
    PositionLeg,
    PositionSide,
    ResolverReason,
)

__all__ = [
    "BotIds",
    "DualBotNeobitcoinResolver",
    "GateName",
    "GateResult",
    "GateSnapshot",
    "PairState",
    "PositionLeg",
    "PositionSide",
    "ResolverConfig",
    "ResolverCycleResult",
    "ResolverJournal",
    "ResolverReason",
    "load_resolver_config",
]
