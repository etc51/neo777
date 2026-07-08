"""Serve the Dual-Bot Neobitcoin Resolver dashboard."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.neobitcoin_resolver.server_dashboard import main  # noqa: E402

if __name__ == "__main__":
    raise SystemExit(main())
