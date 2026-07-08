"""Paper/live-data neoasset scalper swarm.

The package is deliberately paper-only: it can read market data, simulate
orders locally, write journals, dashboards and reports, but it has no real
order placement API.
"""

from neo_swarm_scalper.config import NeoSwarmScalperConfig, load_config
from neo_swarm_scalper.curator import NeoSwarmCurator
from neo_swarm_scalper.feature_engine import NeoFeatureEngine
from neo_swarm_scalper.market_data import NeoMarketDataFeed
from neo_swarm_scalper.simulator import PaperExecutionSimulator

__all__ = [
    "NeoFeatureEngine",
    "NeoMarketDataFeed",
    "NeoSwarmCurator",
    "NeoSwarmScalperConfig",
    "PaperExecutionSimulator",
    "load_config",
]
