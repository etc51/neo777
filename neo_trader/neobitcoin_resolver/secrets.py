"""Local secret helpers for T-Bank token setup.

The helpers avoid printing token contents.  They only return paths/status and
write to local env files that are already ignored by git.
"""

from __future__ import annotations

from pathlib import Path


def find_desktop_token_file(home: Path | None = None) -> Path | None:
    root = home or Path.home()
    candidates: list[Path] = []
    for desktop_name in ("Desktop", "Рабочий стол"):
        desktop = root / desktop_name
        if desktop.exists():
            candidates.extend(sorted(desktop.glob("*.txt")))
    for path in candidates:
        token = _read_candidate_token(path)
        if token:
            return path
    return None


def ensure_token_in_env(
    *,
    env_path: Path | str = Path(".env"),
    token_file: Path | None = None,
    home: Path | None = None,
) -> Path:
    source = token_file or find_desktop_token_file(home)
    if source is None:
        raise FileNotFoundError("no desktop txt token file found")
    token = _read_candidate_token(source)
    if not token:
        raise ValueError("desktop token file did not contain a usable token")
    target = Path(env_path)
    existing = target.read_text(encoding="utf-8") if target.exists() else ""
    lines = [line for line in existing.splitlines() if not _is_token_line(line)]
    lines.append(f"TBANK_TOKEN={token}")
    lines.append(f"NEO_TRADER_TBANK_TOKEN={token}")
    target.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return target


def _read_candidate_token(path: Path) -> str | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except UnicodeDecodeError:
        text = path.read_text(encoding="cp1251").strip()
    for line in text.splitlines():
        value = line.strip()
        if not value or "=" in value:
            value = value.split("=", 1)[-1].strip()
        if len(value) >= 20 and " " not in value:
            return value
    return None


def _is_token_line(line: str) -> bool:
    key = line.split("=", 1)[0].strip()
    return key in {"TBANK_TOKEN", "NEO_TRADER_TBANK_TOKEN"}


__all__ = ["ensure_token_in_env", "find_desktop_token_file"]
