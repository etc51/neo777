"""Pack immutable local Neobitcoin research files into <=400 MiB ZSTD TARs."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import tarfile
import time
from pathlib import Path

import zstandard

MAX_BYTES = 400 * 1024 * 1024
SAFE_AGE_SECONDS = 180


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    data = root / "data"
    archives = root / "archives"
    archives.mkdir(parents=True, exist_ok=True)
    now = time.time()
    candidates = [
        path
        for folder in (data / "raw", data / "parquet", data / "derived_parquet", data / "reports")
        if folder.exists()
        for path in folder.rglob("*")
        if path.is_file() and now - path.stat().st_mtime >= SAFE_AGE_SECONDS
    ]
    selected: list[Path] = []
    total = 0
    for path in sorted(candidates):
        size = path.stat().st_size
        if selected and total + size > MAX_BYTES:
            break
        selected.append(path)
        total += size
    if not selected:
        print("packed=0")
        return 0
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    target = archives / f"neobitcoin-{stamp}.tar.zst"
    temporary = target.with_suffix(".tar.zst.partial")
    manifest: list[dict[str, object]] = []
    with (
        temporary.open("wb") as raw,
        zstandard.ZstdCompressor(level=3).stream_writer(raw) as compressed,
        tarfile.open(fileobj=compressed, mode="w|") as archive,
    ):
                for path in selected:
                    relative = path.relative_to(data)
                    archive.add(path, arcname=str(relative))
                    with path.open("rb") as source:
                        digest = hashlib.file_digest(source, "sha256").hexdigest()
                    manifest.append(
                        {
                            "path": str(relative),
                            "bytes": path.stat().st_size,
                            "sha256": digest,
                        }
                    )
                body = json.dumps({"files": manifest}, ensure_ascii=False).encode()
                info = tarfile.TarInfo("MANIFEST.json")
                info.size = len(body)
                archive.addfile(info, io.BytesIO(body))
    os.replace(temporary, target)
    for path in selected:
        path.unlink()
    print(f"archive={target}")
    print(f"files={len(selected)}")
    print(f"input_bytes={total}")
    print(f"archive_bytes={target.stat().st_size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
