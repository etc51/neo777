"""Neo Universal Bot Swarm paper/simulation package."""

from neo_trader.neo_universal_swarm.backtest import (
    OrderbookBacktestArtifacts,
    OrderbookBacktestConfig,
    OrderbookBacktestResult,
    run_orderbook_backtest,
)
from neo_trader.neo_universal_swarm.bots import CuratorBot, UniversalAccountBot
from neo_trader.neo_universal_swarm.config import (
    AccountKind,
    CuratorConfig,
    SwarmAccountsConfig,
    UniversalBotConfig,
    load_accounts_config,
)
from neo_trader.neo_universal_swarm.daemon import (
    SwarmDaemonConfig,
    SwarmDaemonCycle,
    run_swarm_daemon,
)
from neo_trader.neo_universal_swarm.instruments import (
    SwarmInstrumentCatalog,
    SwarmInstrumentMetadata,
    load_swarm_instrument_catalog,
)
from neo_trader.neo_universal_swarm.live_paper import (
    LivePaperSwarmConfig,
    LivePaperSwarmCycle,
    orderbook_snapshot_to_book_snapshot,
    run_live_paper_swarm,
)
from neo_trader.neo_universal_swarm.model import PairEVModel, PairEVModelConfig, PairEVPrediction
from neo_trader.neo_universal_swarm.online_learning import (
    ExperimentalMode,
    InstrumentLearningState,
    ModePerformance,
    OnlineLearningState,
)
from neo_trader.neo_universal_swarm.paper import (
    PaperSimulationArtifacts,
    PaperSimulationResult,
    run_paper_simulation,
)
from neo_trader.neo_universal_swarm.server_dashboard import (
    load_swarm_state,
    render_swarm_dashboard_html,
    serve_swarm_dashboard,
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
    "AccountKind",
    "ActivePair",
    "BookSnapshot",
    "CuratorBot",
    "CuratorConfig",
    "OrderbookBacktestArtifacts",
    "OrderbookBacktestConfig",
    "OrderbookBacktestResult",
    "HedgePairSimulationConfig",
    "HedgePairSimulator",
    "ExperimentalMode",
    "InstrumentLearningState",
    "LegSide",
    "LivePaperSwarmConfig",
    "LivePaperSwarmCycle",
    "ModePerformance",
    "OnlineLearningState",
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
    "SwarmDaemonConfig",
    "SwarmDaemonCycle",
    "SwarmInstrumentCatalog",
    "SwarmInstrumentMetadata",
    "SwarmInstrument",
    "SwarmMetrics",
    "UniversalAccountBot",
    "UniversalBotConfig",
    "aggregate_pair_metrics",
    "load_accounts_config",
    "load_swarm_instrument_catalog",
    "load_swarm_state",
    "orderbook_snapshot_to_book_snapshot",
    "render_swarm_dashboard_html",
    "run_live_paper_swarm",
    "run_orderbook_backtest",
    "run_paper_simulation",
    "run_swarm_daemon",
    "serve_swarm_dashboard",
]
