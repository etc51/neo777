"""Create local Neo swarm account override from read-only T-Bank accounts."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.broker.tbank import TBankClient  # noqa: E402
from neo_trader.neo_universal_swarm.config import (  # noqa: E402
    DEFAULT_LOCAL_ACCOUNTS_CONFIG,
    AccountKind,
    load_accounts_config,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Write ignored local Neo swarm account override from T-Bank read-only account."
    )
    parser.add_argument("--base", type=Path, default=Path("configs/accounts.yaml"))
    parser.add_argument("--output", type=Path, default=DEFAULT_LOCAL_ACCOUNTS_CONFIG)
    parser.add_argument("--real-bot-id", default="BOT_01")
    parser.add_argument("--select-index", type=int, default=0)
    args = parser.parse_args(argv)

    base = load_accounts_config(args.base)
    accounts = TBankClient().get_accounts()
    if not accounts:
        raise RuntimeError("T-Bank returned no visible accounts for the configured token.")
    if args.select_index < 0 or args.select_index >= len(accounts):
        raise ValueError(f"--select-index must be between 0 and {len(accounts) - 1}.")

    real_ref = accounts[args.select_index].id
    payload = {
        "curator": {
            "bot_id": base.curator.bot_id,
            "trading_enabled": base.curator.trading_enabled,
        },
        "universal_bots": [
            {
                "bot_id": bot.bot_id,
                "account_ref": real_ref if bot.bot_id == args.real_bot_id else bot.account_ref,
                "account_kind": (
                    AccountKind.TBANK_READONLY_DATA.value
                    if bot.bot_id == args.real_bot_id
                    else AccountKind.SIMULATED_PAPER.value
                ),
                "role": bot.role,
                "allowed_instruments": [instrument.value for instrument in bot.allowed_instruments],
                "max_lot": bot.max_lot,
                "paper_enabled": bot.paper_enabled,
                "live_enabled": False,
            }
            for bot in base.universal_bots
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    print(f"local_override={args.output}")
    print(f"real_data_bot={args.real_bot_id}")
    print(f"real_ref_masked={_mask(real_ref)}")
    print(f"simulated_bots={len(base.universal_bots) - 1}")
    return 0


def _mask(value: str) -> str:
    if len(value) <= 8:
        return "***"
    return f"{value[:4]}...{value[-4:]}"


if __name__ == "__main__":
    raise SystemExit(main())
