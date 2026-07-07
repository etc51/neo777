"""Build offline feature-store parquet from readonly raw market data."""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.research.feature_store import (  # noqa: E402
    FeatureStoreConfig,
    build_feature_store,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build NeoIntraday feature store")
    parser.add_argument("--raw", type=Path, default=Path("data/raw"))
    parser.add_argument("--output", type=Path, default=Path("data/features"))
    parser.add_argument(
        "--active-universe",
        type=Path,
        default=Path("configs/active_universe.yaml"),
    )
    parser.add_argument("--expected-fill-quantity", default="10")
    args = parser.parse_args(argv)

    result = build_feature_store(
        raw_path=args.raw,
        output_path=args.output,
        active_universe_path=args.active_universe,
        config=FeatureStoreConfig(
            expected_fill_quantity=Decimal(args.expected_fill_quantity),
        ),
    )
    print(f"feature_store_path={result.output_root}")
    print(f"feature_rows={result.rows_written}")
    print(f"feature_files={len(result.files_written)}")
    print(f"commit_hash={result.commit_hash}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
