"""Create a validated local Neobitcoin 10-minute review bundle."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.neobitcoin_research.review_bundle import (  # noqa: E402
    create_neobitcoin_review_bundle,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "root",
        nargs="?",
        type=Path,
        default=Path.home() / "Documents" / "neobitcoin_research",
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--candidate-window-minutes", type=int, default=10)
    parser.add_argument("--max-outcome-horizon-minutes", type=int, default=30)
    parser.add_argument("--candle-context-hours", type=int, default=6)
    parser.add_argument("--token-file", type=Path, action="append", default=[])
    args = parser.parse_args(argv)
    token_files = tuple(args.token_file or _configured_token_files())
    result = create_neobitcoin_review_bundle(
        args.root,
        output_dir=args.output_dir,
        candidate_window_minutes=args.candidate_window_minutes,
        max_outcome_horizon_minutes=args.max_outcome_horizon_minutes,
        candle_context_hours=args.candle_context_hours,
        token_files=token_files,
    )
    print(f"archive={result.archive_path}")
    print(f"sha256_file={result.sha256_path}")
    print(f"sha256={result.archive_sha256}")
    print(
        f"candidate_window={result.candidate_window_start.isoformat()}/{result.candidate_window_end.isoformat()}"
    )
    print(
        f"support_window={result.support_data_start.isoformat()}/{result.support_data_end.isoformat()}"
    )
    print(f"files={result.file_count}")
    for dataset, rows in result.rows_by_dataset.items():
        print(f"rows.{dataset}={rows}")
    print("validation=PASS")
    return 0


def _configured_token_files() -> list[Path]:
    configured = os.environ.get("NEOBITCOIN_RESEARCH_TOKEN_FILE", "").strip()
    if configured:
        return [Path(configured)]
    default = Path.home() / "Desktop" / "жрт новый про.txt"
    return [default] if default.is_file() else []


if __name__ == "__main__":
    raise SystemExit(main())
