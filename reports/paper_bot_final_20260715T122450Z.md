# Neo Bitcoin PAPER_ONLY bot — final deployment evidence

Evidence cutoff: 2026-07-15 17:48 MSK (14:48 UTC)
Acceptance status: **PASS with the explicitly listed residual risks**

This report contains no credential value, private key, or full Codex task ID.

## Identity and deployment

| Field | Command-backed result |
|---|---|
| Project | `C:\Users\HONOR\Documents\777\111_neobitcoin_edge` |
| Remote | `https://github.com/etc51/neo777.git` |
| Branch | `codex/neobitcoin-edge-research` |
| Deployed runtime commit | `a9e13e5e` |
| Runtime commit push | PASS, remote branch advanced through `a9e13e5e` |
| VPS SSH alias / host | `3pips-vds` / `etc00051.fvds.ru` |
| Deploy/config/state roots | `/opt/neobitcoin-paper`, `/etc/neobitcoin-paper`, `/var/lib/neobitcoin-paper` |
| Immutable release | `/opt/neobitcoin-paper-releases/20260715T144751Z` |
| Release bundle | 688,018 bytes; SHA-256 `cd1078dce886f9418cd33c5f9baa663f1af9a15dc5e8351b25644fd8fafd7af4` |
| Service identity | `neopaper`; archive group `neoarchive` |
| User-owned dirty file | `reports/golden_real_candle_audit.md` preserved and never staged |

## Instrument, SDK, credential boundary, and PAPER_ONLY

| Field | Result |
|---|---|
| Instrument | `Neo Bitcoin` / `BTCUSDperpA` |
| UID | `4effa274-4e8f-422c-93ff-04aa34fe8e39` |
| Class/type/exchange | `SPBDMFUT` / `futures` / `spb_future` |
| Lot / tick | `1` / `0.1` |
| Persisted live check | `2026-07-15T14:47:56.559859+00:00` |
| Live TradingStatus | `SECURITY_TRADING_STATUS_NORMAL_TRADING` |
| Live source | exact-UID T-Invest `MarketDataService/GetTradingStatus` plus read-only `MarketDataStreamService` Info subscription; live status has priority over the fallback calendar |
| Official SDK | `t-tech-investments==1.49.2` |
| Token type/scope | T-Invest API access token; provider scope undeclared/UNKNOWN and therefore treated as potentially full-access; value not disclosed |
| Credential file | root:root `0600`, systemd `LoadCredential`; service user cannot read the source file directly |
| TLS | strict hostname verification and `CERT_REQUIRED`; Ubuntu system CA bundle used for HTTPX and gRPC |
| PAPER_ONLY | environment and CLI confirmation required; conflicting live flag exited non-zero with `PaperOnlyViolation` |
| Trading RPC test | `OrdersService/PostOrder` rejected locally before transport: PASS |
| Source safety scan | zero forbidden live-execution symbols: PASS |
| Secret exposure scan | no credential in process argv, environment, or last 200 service journal records: PASS |

No order RPC was used to probe token scope. The only execution adapter in the
runtime is the internal `PaperExecutionAdapter`.

## Active immutable strategies

Both versions are enabled, activated at `2026-07-15T14:29:42.267819Z`, use the
same immutable code hash `af189a45f8de7bc6c9530ab585c9f23648dfda5af20b0fcea9e942b6d0106807`,
and have independent virtual accounts.

| Strategy ID / version | Effective exits | Config hash |
|---|---|---|
| `STRONG_COUNTERFLOW_ABSORPTION` / `v1` | stop 5 ticks, target 50, breakeven 6, trailing 10, time 60 s, session close | `ddb24a2f1ca718b668af3032ac54f07961f0944c12310b80b5a17b4a0b0a91f1` |
| `STRONG_COUNTERFLOW_ABSORPTION` / `v1-shadow-s6-t100` | same entry logic; stop 6 ticks, target 100, same safety exits | `a2e440da2a525b50849c35d44b7ce3729c210075c91b95a3e6970ac78b27f2e5` |

## Effective-dated session calendar

Rule `neo-moscow-2026-07-14-v1`, effective 2026-07-14, timezone
`Europe/Moscow`:

- weekdays: pre-open 06:50, expected Neo window 07:00–00:00 MSK;
- weekends/holidays: pre-open 09:50, expected Neo window 10:00–00:00 MSK;
- weekday regimes: morning 07:00–10:00, main 10:00–19:00, evening
  19:00–23:50, late 23:50–00:00;
- unknown, stale, closed, closing, or break live status disables new entries;
- archive timer: 00:05 MSK with persistence, jitter, and validated close/grace
  requirements.

## systemd and current runtime

`systemd-analyze verify` passed for all five unit files.

| Unit | State/evidence |
|---|---|
| `neobitcoin-paper.service` | enabled; active/running; PID `2336245`; start `2026-07-15 17:47:55 MSK`; `NRestarts=0` after final atomic release switch |
| `neobitcoin-paper-archive.service` | isolated retrying oneshot |
| `neobitcoin-paper-archive.timer` | enabled/active; next observed trigger `2026-07-16 00:05:10 MSK`; persistent |
| `neobitcoin-paper-delivery.service` | manual production check: `Result=success`, `ExecMainStatus=0`, `IDLE_NO_VALIDATED_ARCHIVE` |
| `neobitcoin-paper-delivery.timer` | enabled/active; one-minute durable retry cadence with jitter |

At the final runtime check, `/healthz` and `/readyz` returned HTTP 200 and
`status=ok`; `state_store`, `instrument`, `writer`, `event_loop`, `disk`, and
`market_stream` were all healthy and ready, disk was `OK`, and the market stream
was `connected`.

## Recovery, reconnect, and safety tests

An isolated real `SIGKILL` was applied only to the paper PID `2280269`.
systemd automatically started PID `2286951`; `/readyz` returned to HTTP 200,
the stream reconnected, and the SQLite restart generation advanced 10 → 11.
Before/after durable invariants remained: two strategies, two accounts, one
active session, one worker checkpoint, zero open orders, zero open positions,
and `PRAGMA quick_check=ok`. The crashed generation remains honestly unclosed;
the new generation is the single running instance. Subsequent atomic deployment
performed a clean state recovery into the current final PID.

The 428-test suite covers deterministic restart with open positions and orders
awaiting fills, duplicate event/replay idempotence, stream disconnect/backfill/ACK and
warmup gates, DNS/API failures with bounded jittered backoff, malformed events,
SQLite locking/transactions, interrupted writers, disk warning/critical/
emergency behavior, archive quarantine/rebuild, Codex outage/retry, duplicate
delivery, independent PnL oracle, and the live-order boundary. All passed.

No full VPS reboot was performed because that would interrupt the pre-existing
collector. Boot enablement, persistent timers, systemd unit verification, a real
paper-process crash, and clean release restarts were used as the isolated reboot
simulation.

## Test suite

| Suite | Result |
|---|---|
| Full local pytest | **428 passed** in 142.86 s |
| Final server paper/deploy suite | **50 passed** in 12.36 s |
| Ruff | PASS |
| strict mypy | PASS, 109 source files (paper package: 19 modules) |
| compileall | PASS |
| Local live read-only stream smoke | PASS, 90 events processed in 25 s; no credential output |
| Source credential scan | PASS, zero actual credential and zero generic token matches |

## Validated TEST archive

| Field | Result |
|---|---|
| Session / ID / type | `2026-07-14` / `TEST-2026-07-14-73ee6a8bb61c` / `TEST` |
| Server path | `/var/lib/neobitcoin-paper/daily_archives/neobitcoin_paper_2026-07-14_20260714T040000Z_20260714T210000Z_schema-v1_TEST.tar.zst` |
| Local accessible path | `C:\Users\HONOR\Documents\777\111_neobitcoin_edge\artifacts\paper_archives\neobitcoin_paper_2026-07-14_20260714T040000Z_20260714T210000Z_schema-v1_TEST.tar.zst` |
| Size | 15,572 bytes compressed; 153,600 bytes zstd payload |
| SHA-256 | `297fa5d9b653dcf00b80359e9b6dd09a15f31a585e98c81be343f458c243318a` |
| Ownership/mode | `neopaper:neoarchive`, `0640` |
| Sidecar/server/local hash | all identical: PASS |
| zstd integrity | PASS |
| PyArrow / DuckDB | PASS / PASS |
| Independent reconciliation | PASS; unexplained discrepancies 0 |
| Validation errors/warnings | 0 / 0 |
| Typed datasets | all 20 required Parquet files present; zero-row TEST day is valid |
| Documents | README, manifests, daily summary, validation report, schema dictionary, config/code/calendar/strategy snapshots, `SHA256SUMS` |
| Keys/FKs/duplicates/timestamps/lineage/PnL/position/equity/secrets | all validator checks PASS |

MCP resource:
`neobitcoin-paper://archive/2026-07-14/TEST-2026-07-14-73ee6a8bb61c/297fa5d9b653dcf00b80359e9b6dd09a15f31a585e98c81be343f458c243318a`.
The TEST artifact is permanently excluded from OOS statistics and the production
delivery outbox.

## Same-task delivery

| Field | Result |
|---|---|
| Read-only MCP | `neobitcoin-paper_archives`, protocol `2025-06-18`; 9/9 tools read-only; latest/verify test PASS |
| Actual TEST delivery | visible in the same Codex task as `item-119`; file exists and local SHA matches |
| Short task ID | `019f6597…9740` |
| Delivery turn/run | `019f6619…d5d9` (current delivery/test turn) |
| Automation | `neobitcoin-paper-daily-delivery`; heartbeat; `ACTIVE` |
| Schedule | daily 00:10 Europe/Moscow in this existing task |
| Transport | local Codex heartbeat + read-only MCP over SSH + SCP + local SHA verification |

The automation excludes TEST archives, validates the newest OOS archive, copies
it to the local accessible artifact directory, and reports into this task only
after server/sidecar/local SHA agreement. Server-side delivery retains a durable
retry outbox and does not block trading.

## Existing collector isolation

The collector was not stopped, restarted, reloaded, edited, or included in any
paper deployment command. Its independent state at the final check was:

- `neobitcoin-research.service`: active/running;
- PID `1828340`;
- `NRestarts=1`;
- active since `2026-07-15 15:39:02 MSK`;
- PID/start/restart count were identical immediately before the first successful
  paper deployment, after fault injection, and after the final release.

The earlier discovery PID `3379395` changed independently before paper
deployment began; the preserved deployment baseline is PID `1828340`.

## Residual risks

1. The provider-declared token scope remains unknown. Runtime exposure is still
   constrained to the immutable read-only RPC allowlist and PAPER_ONLY adapter.
2. A host-wide reboot was intentionally not run to protect the collector;
   boot behavior is supported by enabled units, persistent timers, unit
   verification, real SIGKILL recovery, and clean service restarts.
3. Same-task automated delivery requires the local Codex Desktop session to be
   running and authenticated at the heartbeat time. The VPS retains the archive
   and retry outbox if that external dependency is unavailable.
4. Broker/exchange schedules, CA roots, and instrument metadata may change.
   The system fails closed on unknown live status and requires an effective-dated
   calendar/code update for reviewed changes.

## Verdict

**PASS.** The paper-only runtime is deployed and healthy, live-order access is
blocked, recovery and archive validation passed, the TEST artifact is accessible
and delivered in the same task, daily delivery is active, Git runtime commits
are pushed, and the existing collector remained unchanged during deployment.
