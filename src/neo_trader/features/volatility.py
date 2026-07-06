"""Compatibility shim for src-layout consumers.

The project currently packages ``neo_trader`` from the repository root. The
implementation lives in ``neo_trader.features.volatility``.
"""

from neo_trader.features.volatility import (
    VolatilityRegime,
    atr,
    log_returns,
    realized_volatility,
    volatility_percentile,
    volatility_regime,
)

__all__ = [
    "VolatilityRegime",
    "atr",
    "log_returns",
    "realized_volatility",
    "volatility_percentile",
    "volatility_regime",
]
