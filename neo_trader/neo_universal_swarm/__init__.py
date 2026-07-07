"""Neo Universal Bot Swarm paper/simulation package."""

from neo_trader.neo_universal_swarm.bots import CuratorBot, UniversalAccountBot
from neo_trader.neo_universal_swarm.config import (
    CuratorConfig,
    SwarmAccountsConfig,
    UniversalBotConfig,
    load_accounts_config,
)
from neo_trader.neo_universal_swarm.model import PairEVModel, PairEVModelConfig, PairEVPrediction
from neo_trader.neo_universal_swarm.paper import (
    PaperSimulationArtifacts,
    PaperSimulationResult,
    run_paper_simulation,
)
from neo_trader.neo_universal_swarm.simulator import (
    HedgePairSimulationConfig,
    HedgePairSimulator,
    aggregate_pair_metrics,
)
from neo_trader.neo_universal_swarm.types import (
    AccountBotState,
    ActivePair,
    BookSnapshot,
    LegSide,
    PairExitReason,
    PairLabel,
    PairStatus,
    RejectionReason,
    SwarmInstrument,
    SwarmMetrics,
)

__all__ = [
    "AccountBotState",
    "ActivePair",
    "BookSnapshot",
    "CuratorBot",
    "CuratorConfig",
    "HedgePairSimulationConfig",
    "HedgePairSimulator",
    "LegSide",
    "PairEVModel",
    "PairEVModelConfig",
    "PairEVPrediction",
    "PairExitReason",
    "PairLabel",
    "PairStatus",
    "PaperSimulationArtifacts",
    "PaperSimulationResult",
    "RejectionReason",
    "SwarmAccountsConfig",
    "SwarmInstrument",
    "SwarmMetrics",
    "UniversalAccountBot",
    "UniversalBotConfig",
    "aggregate_pair_metrics",
    "load_accounts_config",
    "run_paper_simulation",
]
