"""Compatibility shim for src-layout consumers.

The project currently packages ``neo_trader`` from the repository root. The
implementation lives in ``neo_trader.features.orderbook``.
"""

from neo_trader.features.orderbook import (
    BookLevel,
    best_bid_ask,
    book_wall_score,
    depth_sum,
    expected_slippage_bps,
    expected_vwap_to_fill,
    imbalance,
    microprice,
    mid_price,
    spread_bps,
    weighted_imbalance,
)

__all__ = [
    "BookLevel",
    "best_bid_ask",
    "book_wall_score",
    "depth_sum",
    "expected_slippage_bps",
    "expected_vwap_to_fill",
    "imbalance",
    "microprice",
    "mid_price",
    "spread_bps",
    "weighted_imbalance",
]
