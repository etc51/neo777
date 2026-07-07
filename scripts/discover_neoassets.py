"""Discover T-Bank neoassets using readonly instruments and market-data APIs."""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Final, cast

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo_trader.research.neoassets import (  # noqa: E402
    LiquidityPrecheckConfig,
    NeoAssetRecord,
    apply_liquidity_precheck,
    discovery_diagnostics,
    parse_neoasset_candidates,
    tbank_orderbook_payload_to_book,
    write_discovery_reports,
    write_neoassets_universe,
)

PROD_REST_BASE_URL: Final = "https://invest-public-api.tbank.ru/rest"
APP_NAME: Final = "neo_trader"
SAFE_FALSE_VALUES: Final = {"0", "false", "no", "off"}
TOKEN_ENV_NAMES: Final = ("T_INVEST_TOKEN", "NEO_TRADER_TBANK_TOKEN")
TEMPORARY_STATUS_CODES: Final = frozenset({408, 425, 429, 500, 502, 503, 504})
INSTRUMENTS_SERVICE: Final = "tinkoff.public.invest.api.contract.v1.InstrumentsService"
MARKETDATA_GET_ORDER_BOOK: Final = (
    "tinkoff.public.invest.api.contract.v1.MarketDataService/GetOrderBook"
)


@dataclass(frozen=True)
class DiscoveryCall:
    """Readonly instruments API call used for metadata discovery."""

    name: str
    method: str
    payload: Mapping[str, object]


DISCOVERY_CALLS: Final = (
    DiscoveryCall("Indicatives", f"{INSTRUMENTS_SERVICE}/Indicatives", {}),
    DiscoveryCall(
        "FindInstrument:neo",
        f"{INSTRUMENTS_SERVICE}/FindInstrument",
        {"query": "neo", "apiTradeAvailableFlag": False},
    ),
    DiscoveryCall(
        "Shares",
        f"{INSTRUMENTS_SERVICE}/Shares",
        {"instrumentStatus": "INSTRUMENT_STATUS_ALL"},
    ),
    DiscoveryCall(
        "Bonds",
        f"{INSTRUMENTS_SERVICE}/Bonds",
        {"instrumentStatus": "INSTRUMENT_STATUS_ALL"},
    ),
    DiscoveryCall(
        "Etfs",
        f"{INSTRUMENTS_SERVICE}/Etfs",
        {"instrumentStatus": "INSTRUMENT_STATUS_ALL"},
    ),
    DiscoveryCall(
        "Currencies",
        f"{INSTRUMENTS_SERVICE}/Currencies",
        {"instrumentStatus": "INSTRUMENT_STATUS_ALL"},
    ),
    DiscoveryCall(
        "Futures",
        f"{INSTRUMENTS_SERVICE}/Futures",
        {"instrumentStatus": "INSTRUMENT_STATUS_ALL"},
    ),
    DiscoveryCall(
        "Options",
        f"{INSTRUMENTS_SERVICE}/Options",
        {"instrumentStatus": "INSTRUMENT_STATUS_ALL"},
    ),
    DiscoveryCall("GetAssets", f"{INSTRUMENTS_SERVICE}/GetAssets", {}),
)


class DiscoveryCliError(Exception):
    """Expected discovery failure with a user-facing message."""


class TBankReadonlyRestClient:
    """Small REST client limited to readonly discovery and market-data endpoints."""

    def __init__(
        self,
        *,
        token: str,
        base_url: str = PROD_REST_BASE_URL,
        timeout_seconds: float = 15.0,
        max_retries: int = 2,
        backoff_seconds: float = 0.5,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._token = token
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._backoff_seconds = backoff_seconds
        self._client = http_client or httpx.Client(timeout=timeout_seconds)
        self._owns_client = http_client is None

    def __enter__(self) -> TBankReadonlyRestClient:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def fetch_discovery_payloads(self) -> tuple[dict[str, object], dict[str, str]]:
        """Call readonly instruments endpoints and collect non-fatal failures."""

        payloads: dict[str, object] = {}
        errors: dict[str, str] = {}
        for call in DISCOVERY_CALLS:
            try:
                payloads[call.name] = self.post(call.method, call.payload)
            except Exception as exc:  # noqa: BLE001
                errors[call.name] = type(exc).__name__
        return payloads, errors

    def orderbook_for(self, record: NeoAssetRecord, *, depth: int) -> Mapping[str, object] | None:
        """Return a feature-engine compatible order book snapshot."""

        if not record.uid:
            return None
        payload = self.post(
            MARKETDATA_GET_ORDER_BOOK,
            {"instrumentId": record.uid, "depth": depth},
        )
        return tbank_orderbook_payload_to_book(payload)

    def post(self, method: str, payload: Mapping[str, object]) -> dict[str, Any]:
        """POST one readonly REST request with temporary-error retries."""

        url = f"{self._base_url}/{method}"
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                response = self._client.post(
                    url,
                    headers=self._headers(),
                    json=dict(payload),
                    timeout=self._timeout_seconds,
                )
            except (
                httpx.ConnectError,
                httpx.ConnectTimeout,
                httpx.NetworkError,
                httpx.PoolTimeout,
                httpx.ReadError,
                httpx.ReadTimeout,
                httpx.RemoteProtocolError,
                httpx.WriteError,
                httpx.WriteTimeout,
            ) as exc:
                last_error = exc
                if attempt < self._max_retries:
                    time.sleep(self._backoff_seconds * (2**attempt))
                    continue
                raise DiscoveryCliError("Temporary T-Bank transport error.") from exc

            if response.status_code in TEMPORARY_STATUS_CODES and attempt < self._max_retries:
                time.sleep(self._retry_delay(attempt, response=response))
                continue
            if response.is_error:
                raise DiscoveryCliError(
                    f"T-Bank readonly API returned HTTP {response.status_code}."
                )
            decoded = _decode_json_response(response)
            return decoded

        if last_error is not None:
            raise DiscoveryCliError("T-Bank readonly API request failed.") from last_error
        raise DiscoveryCliError("T-Bank readonly API request failed before send.")

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "x-app-name": APP_NAME,
        }

    def _retry_delay(self, attempt: int, *, response: httpx.Response) -> float:
        retry_after = response.headers.get("retry-after")
        if retry_after is not None:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                pass
        return self._backoff_seconds * (2**attempt)


def main(argv: Sequence[str] | None = None) -> int:
    """Run readonly neoasset discovery."""

    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        _load_dotenv(args.env_file)
        safety_flags = require_readonly_runtime_flags()
        token = _token_from_env()
        with TBankReadonlyRestClient(
            token=token,
            base_url=args.base_url,
            timeout_seconds=args.timeout_seconds,
            max_retries=args.max_retries,
            backoff_seconds=args.backoff_seconds,
        ) as client:
            payloads, source_errors = client.fetch_discovery_payloads()
            records = parse_neoasset_candidates(payloads)
            checked = apply_liquidity_precheck(
                records,
                lambda record: client.orderbook_for(record, depth=args.depth),
                config=LiquidityPrecheckConfig(
                    test_quantity=Decimal(args.test_quantity),
                    max_spread_bps=Decimal(args.max_spread_bps),
                    max_slippage_bps=Decimal(args.max_slippage_bps),
                    max_enabled=args.max_enabled,
                ),
            )

        diagnostics = discovery_diagnostics(payloads, source_errors=source_errors)
        diagnostics["safe_flags"] = safety_flags
        diagnostics["liquidity_precheck"] = {
            "depth": args.depth,
            "test_quantity": args.test_quantity,
            "max_spread_bps": args.max_spread_bps,
            "max_slippage_bps": args.max_slippage_bps,
            "max_enabled": args.max_enabled,
        }
        write_neoassets_universe(args.output_config, checked)
        report_paths = write_discovery_reports(
            reports_dir=args.reports_dir,
            records=checked,
            diagnostics=diagnostics,
        )
        enabled = [record for record in checked if record.enabled]
        print(f"neoassets_found={len(checked)}")
        print(f"neoassets_enabled={len(enabled)}")
        print(f"neoassets_universe={args.output_config}")
        print(f"neoassets_discovery_json={report_paths.json_path}")
        print(f"neoassets_discovery_csv={report_paths.csv_path}")
        if enabled:
            print("top_neoassets=" + ",".join(record.ticker for record in enabled[:10]))
        return 0
    except DiscoveryCliError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


def require_readonly_runtime_flags(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Fail fast unless discovery runtime flags are readonly and non-live."""

    source = os.environ if env is None else env
    flags = {
        "TRADING_MODE": source.get("TRADING_MODE", "readonly"),
        "NEO_TRADER_TRADING_MODE": source.get("NEO_TRADER_TRADING_MODE", "readonly"),
        "LIVE_TRADING_ENABLED": source.get("LIVE_TRADING_ENABLED", "false"),
        "NEO_TRADER_LIVE_TRADING_ENABLED": source.get(
            "NEO_TRADER_LIVE_TRADING_ENABLED",
            "false",
        ),
    }
    if flags["TRADING_MODE"].lower() != "readonly":
        raise DiscoveryCliError("TRADING_MODE must be readonly.")
    if flags["NEO_TRADER_TRADING_MODE"].lower() != "readonly":
        raise DiscoveryCliError("NEO_TRADER_TRADING_MODE must be readonly.")
    if flags["LIVE_TRADING_ENABLED"].lower() not in SAFE_FALSE_VALUES:
        raise DiscoveryCliError("LIVE_TRADING_ENABLED must be false.")
    if flags["NEO_TRADER_LIVE_TRADING_ENABLED"].lower() not in SAFE_FALSE_VALUES:
        raise DiscoveryCliError("NEO_TRADER_LIVE_TRADING_ENABLED must be false.")
    return flags


def _token_from_env() -> str:
    for name in TOKEN_ENV_NAMES:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value.strip()
    raise DiscoveryCliError("TOKEN_MISSING")


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


def _decode_json_response(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as exc:
        raise DiscoveryCliError("T-Bank readonly API returned non-JSON response.") from exc
    if not isinstance(payload, Mapping):
        raise DiscoveryCliError("T-Bank readonly API returned unexpected JSON payload.")
    return cast(dict[str, Any], payload)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Discover T-Bank neoassets in readonly mode")
    parser.add_argument(
        "--output-config",
        type=Path,
        default=Path("configs/neoassets_universe.yaml"),
    )
    parser.add_argument("--reports-dir", type=Path, default=Path("data/reports"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--base-url", default=PROD_REST_BASE_URL)
    parser.add_argument("--depth", type=int, default=10)
    parser.add_argument("--test-quantity", default="1")
    parser.add_argument("--max-spread-bps", default="100")
    parser.add_argument("--max-slippage-bps", default="100")
    parser.add_argument("--max-enabled", type=int, default=10)
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument("--max-retries", type=int, default=2)
    parser.add_argument("--backoff-seconds", type=float, default=0.5)
    return parser


if __name__ == "__main__":
    raise SystemExit(main())
