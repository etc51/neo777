# Paper strategy specification

Specification ID: `STRONG_COUNTERFLOW_ABSORPTION_v1`

Registry key: `STRONG_COUNTERFLOW_ABSORPTION` + `v1`

Intended status: `FROZEN_PAPER`

Feature schema: `schema-v4.1-point-in-time`

Execution model: `paper-execution-v1`
Risk model: `paper-risk-v1`

## Provenance and interpretation

No later approved, versioned registry or strategy report containing different
parameters was found during repository discovery. Therefore v1 uses the control
values in the development task and records its discovery source as:

```text
task specification control values; no later approved registry found
```

This is an honest forward paper candidate, not a reconstructed historical rule.
It is not the primary or preferred strategy. It receives no backfilled OOS
signals, no ranking advantage, and no right to absorb another version's account
or history. If an approved earlier source is later found and differs, it must be
represented as a separately reviewed version; v1 must not be silently edited.

The runtime also pre-registers `v1-shadow-s6-t100` before its first eligible
event. It keeps the same entry rule but uses the task-specified shadow grid's
6-tick fixed stop and 100-tick target. It has its own config hash, immutable
registry row, decision stream, positions, PnL, and virtual account. This is an
OOS risk variant, not evidence of profitability and not a retroactive winner.

## Immutable registry record

The registry freezes, at minimum:

- `strategy_id`, `version`, status, creation/activation/deactivation times;
- discovery source and description;
- canonical parameters and their SHA-256 config hash;
- plugin source and its SHA-256 code hash;
- feature, execution, and risk model versions;
- session filters, warmup, new-entry cutoff, and carry policy.

Registration requires `activated_at` strictly after `registered_at`. Activation
therefore cannot create historical OOS trades. Any change to a condition,
threshold, side, latency, quantity, fill model, stop, target, exit, cost, or carry
rule creates `v2` or another new version. A strategy with an open position cannot
be hot-replaced.

Each version receives its own deterministic virtual account, initially funded by
the configured paper balance. Cash, realized/unrealized PnL, equity, drawdown,
orders, positions, checkpoints, evaluation journal, and error/circuit state are
version-scoped.

## Inputs and sandbox

The plugin receives one immutable `StrategyContext`:

- the current canonical market event;
- the latest causal order book, if one exists;
- a point-in-time `FeatureSnapshot` built only from events available no later
  than the evaluation timestamp;
- `SessionContext` with Moscow label and normalized live trading status;
- data-quality state;
- the version's own state.

It does not receive the T-Bank token or client, account services, order methods,
filesystem access, another strategy's state, or any future event. Its only
actionable output is an immutable `PaperIntent` inside a recorded
`StrategyDecision`. The supervisor runs plugins behind a 250 ms default timeout;
three consecutive failures open that version's circuit for 30 seconds while
other versions continue.

## Point-in-time episode definition

The feature builder keeps only causal anonymous trades in the most recent
five-second window and the latest causal book. A trade contributes only when its
timestamp is not later than the current event. The book is rejected for feature
construction if its exchange timestamp is later than the current event.

The initial wave is the earliest same-side run whose cumulative quantity reaches
`1000`. Its volume-weighted reference price and threshold-crossing time are
recorded. The counter wave consists of opposite-side trades beginning at or
after that threshold time. The feature record includes:

- initial side, volume, and reference price;
- counter volume, ratio to the initial volume, and first-counter delay;
- adverse movement in `0.1` price ticks from the reference to causal mid-price;
- level-5 book imbalance;
- observed latency;
- all source event IDs;
- a deterministic episode ID.

The L5 imbalance is signed: positive supports a buy wave, negative supports a
sell wave. Continuity state is reset after a market-data gap/reconnect; the old
five-second window cannot leak into the new generation.

## Entry conditions

All gates must pass:

| Gate | Frozen v1 rule |
|---|---|
| Feature readiness | initial and counter waves plus a causal book are present |
| Initial wave | cumulative same-side quantity `>= 1000` |
| Counter timing | first opposite trade within `<= 5` seconds of threshold crossing |
| Counter size | counter/initial volume ratio in `[0.20, 1.00]` |
| Price absorption | adverse movement `<= 5` ticks |
| L5 support | buy: imbalance `>= +0.20`; sell: imbalance `<= -0.20` |
| Latency | `<= 3000 ms` |
| Quality | exactly `GOOD`, with reconnect warmup complete |
| Live status | `SessionContext.entry_allowed=true` |
| Session | not `CLOSED`, `CLOSING`, `BREAK`, or `UNKNOWN` |
| Independence | episode ID has not previously produced an entry for v1 |
| Causality | entry reference is a real book received by processing time |

The first failed gate is saved as a deterministic rejection reason such as
`GATE:data_quality`, while the complete conditions and thresholds are retained
in `strategy_evaluations` and `filter_decisions`. Rejections are data, not noise:
they must remain in the daily archive and receive raw event-window coverage.

Allowed session filters are `MORNING`, `MAIN`, `EVENING`,
`NEO_LATE_SESSION`, and `WEEKEND`. Minimum warmup is 20 canonical events. The
new-entry cutoff is 300 seconds before expected close; live closed/closing status
can impose an earlier cutoff.

## Paper intent and execution

A passing v1 decision emits:

- side equal to the initial wave direction;
- virtual quantity `1`;
- aggressive execution;
- eligibility at decision processing time plus 100 ms;
- the decision event ID and episode ID;
- confidence capped at 1 from absolute L5 imbalance.

The intent is not a fill. The central paper execution engine selects the first
real order book received after eligibility. A buy consumes asks and a sell
consumes bids; depth determines VWAP and `FULL_FILL`, `PARTIAL_FILL`, or
`NO_FILL`. Source book ID, spread, depth, slippage, and latency remain
reproducible. Invalid, stale, gapped, crossed, empty, excessively latent, or
closed-status books cannot fill a new entry.

## Frozen exit and shadow evaluation

The actual v1 position policy is intraday:

- fixed stop: 5 ticks;
- fixed take profit: 50 ticks;
- breakeven trigger: 6 ticks;
- trailing distance: 10 ticks;
- time exit: 60 seconds;
- close at session end: enabled;
- position carry policy: `INTRADAY_CLOSE`.

The archive additionally evaluates, without changing the actual trade:

- shadow stops at 5, 6, 8, and 10 ticks;
- shadow targets at 50, 100, 200, and 500 ticks;
- a runner through 60 seconds.

Shadow outcomes are stored separately in `shadow_stop_results` and
`shadow_exit_results`. They must never overwrite actual v1 PnL or become
retroactive optimization of v1.

For long positions, executable exit prices come from causal bids; for short
positions they come from causal asks. A trigger event and a later fill event are
distinct. Exit latency and available depth apply. Gross PnL, spread cost,
slippage, latency cost, holding/carry assumptions, and net PnL are separate.
MFE/MAE are calculated only after entry through the actual available horizon and
are non-negative magnitudes. An exit before entry is invalid.

When a gap prevents honest exit simulation, no price is invented. The position
is restored from SQLite, receives a data-quality marker, and follows the
predeclared conservative recovery/session-end policy once a valid causal book
and trading status return.

## Required evidence

For each evaluation, signal, rejection, order, fill, entry, stop, exit, and
material error, retain source IDs and a raw event window. Default coverage is at
least 60–120 seconds before the candidate through position close plus a useful
post-exit interval, bounded when necessary by the 30-minute review horizon. The
window includes full book, trades, last price, candles, market status, latency,
and gap/reconnect markers.

Daily reporting is per version and includes signals, independent episodes,
rejections, orders, fill statuses, long/short entries/exits, open positions,
gross/net PnL, win/loss statistics, expectancy, profit factor, drawdown, MFE/MAE,
results without the best 1/3/5 trades, and breakdowns by session, hour, and
execution model. One runner cannot justify a profitability claim.

## Adding a new strategy version

1. Write a complete strategy specification and canonical parameter object.
2. Validate schema and compute code/config hashes.
3. Add unit and schema-v4.1 golden replay tests.
4. Prove point-in-time causality and the paper-only dependency boundary.
5. Register a new immutable key with a future activation timestamp and a new
   independent virtual account.
6. Activate only after validation and only when that version has no open
   position requiring the old code.
7. Preserve all old versions and rejected decisions. Never backfill OOS trades.

Status progression is review-driven:
`DISCOVERY` → `FROZEN_PAPER` → `OOS_ACCUMULATION` → optionally
`PAPER_VALIDATED`. `PAUSED`, `REJECTED`, and `ARCHIVED` do not erase history.
