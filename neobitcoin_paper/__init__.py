"""Independent, fail-closed Neobitcoin paper-trading service.

The package deliberately exposes paper-domain types only.  Broker market-data
access lives behind :mod:`neobitcoin_paper.ingest`; strategy plugins never
receive that adapter or a token.
"""

from __future__ import annotations

from neobitcoin_paper.safety import PAPER_ONLY

__all__ = ["PAPER_ONLY"]
