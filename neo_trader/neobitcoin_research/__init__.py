"""Read-only Neobitcoin microstructure research system.

The package deliberately contains no order submission, replacement, or
cancellation capability.  Execution classes simulate fills from recorded
market data only.
"""

from .config import ResearchConfig

__all__ = ["ResearchConfig"]
