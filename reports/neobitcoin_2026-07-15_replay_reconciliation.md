# Neo Bitcoin 2026-07-15 replay reconciliation

## Verdict

- `ROW_FIELD_RECONCILIATION`: **PASS (197/197)**
- `MICRO_FLOW_ALIGNMENT_v1`: **109/109 control rows**
- `L5_FLOW_ALIGNMENT_v1`: **88/88 control rows**
- unexplained row-field mismatches: **0**
- `SIGNAL_SET_REGENERATION`: **SOURCE_UNIVERSE_UNDERDETERMINED**

The raw-safe archive was read independently by
`scripts/neobitcoin_discovery_oracle_compat.py`. The oracle does not import the
production feature or strategy implementation.

## Exact row-field result

For every approved control row the oracle independently matched:

- strategy and signal receive timestamp;
- side;
- immediate next raw orderbook entry timestamp and aggressive top price;
- entry spread and source-book latency;
- microprice offset, L5 imbalance, five-second flow ratio and known volume;
- 120-second time exit;
- stressed PnL (one adverse tick at entry and one at exit);
- raw MFE and MAE.

Machine-readable result:
`reports/neobitcoin_2026-07-15_replay_reconciliation.json`.

## Proven discovery compatibility transforms

The control CSV was built with two transforms not stated as production rules:

1. Its signal universe samples the last received orderbook in each UTC second.
   All 145 unique approved signal timestamps satisfy this receive-time rule.
2. Its five-second flow uses received trades in
   `(book.receive_ts - 5 seconds, book.receive_ts]`, rather than the handoff's
   canonical exchange-time interval.

All 197 control entries use the immediate next raw orderbook (`i + 1`).

## Why signal-set regeneration is not marked PASS

After applying the proven compatibility transforms and every published gate,
the best reconstruction is:

| Strategy | Referenced | Expected | Extra valid candidates |
|---|---:|---:|---:|
| MICRO_FLOW_ALIGNMENT_v1 | 106 | 109 | 40 |
| L5_FLOW_ALIGNMENT_v1 | 85 | 88 | 26 |

The extra candidates satisfy the published thresholds, known-trade, book,
tick-grid, entry-spread, five-second wait, generation and gap gates. On all raw
orderbooks, only 22/109 MICRO and 16/88 L5 control rows are literal rising
edges. Therefore the supplied JSON, Markdown and CSV omit a discovery source
filter. Threshold or cooldown fitting would mutate the frozen strategies and
is explicitly prohibited.

## Production decision

`LIVE_OOS` keeps the canonical causal exchange-time trade window:
`(feature_exchange_ts - 5 seconds, feature_exchange_ts]`, additionally gated by
`trade.receive_ts <= feature_processing_ts`. The receive-time behavior remains
isolated to this `DISCOVERY_ORACLE_COMPAT` audit and is never used by the paper
runtime.
