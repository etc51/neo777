"""Create or verify local Neobitcoin compacted-Parquet archives."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.neobitcoin_research.archive import (  # noqa: E402
    create_verified_archive,
    verify_archive,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create")
    create.add_argument("root", type=Path)
    create.add_argument("--force", action="store_true", help="manual archive below 256 MiB")
    create.add_argument("--schema-version", default="v1")
    create.add_argument("--config-hash", default="unknown")
    create.add_argument("--token-file", type=Path, action="append", default=[])
    verify = subparsers.add_parser("verify")
    verify.add_argument("archive", type=Path)
    verify.add_argument("manifest", type=Path)
    verify.add_argument("--token-file", type=Path, action="append", default=[])
    args = parser.parse_args(argv)

    if args.command == "verify":
        verify_archive(args.archive, args.manifest, token_files=_token_files(args.token_file))
        print("validation=passed")
        return 0

    result = create_verified_archive(
        args.root,
        force=args.force,
        schema_version=args.schema_version,
        code_commit=_git_commit(),
        config_hash=args.config_hash,
        token_files=_token_files(args.token_file),
    )
    print(f"created={str(result.created).lower()}")
    print(f"waiting_bytes={result.waiting_bytes}")
    if result.created:
        print(f"archive={result.archive_path}")
        print(f"manifest_json={result.manifest_json_path}")
        print(f"manifest_markdown={result.manifest_markdown_path}")
        print(f"archive_sha256={result.archive_sha256}")
        print(f"files={result.file_count}")
        print(f"rows={result.row_count}")
    return 0


def _git_commit() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip() if completed.returncode == 0 else "unknown"


def _token_files(explicit: list[Path]) -> list[Path]:
    if explicit:
        return explicit
    configured = os.environ.get("NEOBITCOIN_RESEARCH_TOKEN_FILE", "").strip()
    if configured:
        return [Path(configured)]
    desktop_default = Path.home() / "Desktop" / "жрт новый про.txt"
    return [desktop_default] if desktop_default.is_file() else []


if __name__ == "__main__":
    raise SystemExit(main())
