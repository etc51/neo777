# Neo Bitcoin paper market-stream incident — 2026-07-18

## Verdict

The missing paper day had two independent layers. Disk exhaustion caused a
restart storm, but it does not explain the final five-minute archive. The
direct functional cause was premature session finalization: discovery returned
`NOT_AVAILABLE_FOR_TRADING` before the 10:00 MSK weekend open, the runtime
started its generic 300-second closed-status timer, finalized the 18 July
session at 09:55:38 MSK, and permanently respected that terminal state after
the actual open.

## Evidence timeline (MSK; UTC shown where relevant)

| Time | Evidence |
|---|---|
| 2026-07-17 22:23:54 | First cross-system `ENOSPC`, while the collector attempted lock creation. |
| 22:33:59 | rsyslog itself could no longer write. |
| 22:35:59 | First paper SQLite `OperationalError`. |
| 22:35:59–2026-07-18 00:42:39 | Disk-caused paper startup failures every approximately six seconds. |
| 00:42:45 | First successful paper start after disk recovery. |
| 00:45:53–08:22:16 | Intermittent systemd watchdog timeouts continued. |
| 09:16:27–09:16:44 | Orderly host poweroff and boot; no kernel OOM/crash evidence. |
| 09:50:37 | Paper bootstrap REST calls for exact instrument, status, and schedule returned HTTP 200; process announced Ready. |
| 09:50:37–09:55:38 | Only useful paper coverage. Subscriptions never converged: six `SUBSCRIPTIONS_NOT_READY`; reconnect lifecycle was mislabeled as 22 `ORDERBOOK_GAP` events. |
| 09:55:38 / 06:55:38Z | Session became `FINALIZATION_INCOMPLETE`, inactive, although its stored expected start was 07:00:00Z. This is 4m22s before session open. |
| 10:00–00:00 | Scheduled weekend trading interval. The already-finalized paper runtime did not recreate its engine/writer. |
| 09:58–00:01 | Separate research collector ran normally for the scheduled day; bootstrap REST calls returned HTTP 200. |

## Root cause

`PaperRuntime` treated any persistent explicitly closed status as permission to
finalize the active session after `archive_grace_seconds=300`, without requiring
the scheduled close boundary. A pre-open `NOT_AVAILABLE_FOR_TRADING` therefore
terminated the entire day. Subsequent restarts loaded the durable terminal
state and did not reopen that session.

## Secondary causes and misleading health

- `/healthz` and `/readyz` represented a living process and recently handled
  events, not full subscription acknowledgements, useful component ages, or an
  active session writer.
- The synthetic reconnect event was treated as connected before subscription
  ACKs and a valid book.
- Silent gRPC iteration had no bounded receive timeout.
- One global freshness timestamp allowed ping/status traffic to hide a stale
  book or trade stream.
- Disconnect/reconnect was labeled `ORDERBOOK_GAP`; the 22 rows are repeated
  lifecycle symptoms, not 22 independently proven exchange gaps.
- `Restart=always`, `RestartSec=5s`, and unlimited start attempts amplified
  disk exhaustion into 501 starts, 464 failed starts, and 456 OperationalErrors.
- The raw collector installer intentionally left the service disabled and a
  scheduler stopped it outside sessions. That behavior explains an observed
  inactive state, but it was not the July 18 daytime failure: research collected
  the scheduled session successfully.

## Ruled out

The exact UID was used. T-Invest REST, trading schedule, DNS, TLS, host clock,
timezone, and NTP were available at the relevant recovery/startup period. The
SQLite database passes `quick_check`; there is no OOM evidence. Therefore
`NOT_AVAILABLE_FOR_TRADING` was a legitimate pre-open/closed state, not proof of
an incorrect UID or upstream outage.

## Corrective controls

- A closed status now blocks entries but never finalizes before scheduled
  close plus grace.
- Reconnect creates a new generation and requires fresh ACK/book/warm-up
  evidence; older generation events are rejected.
- Silent streams time out and reconnect using a fresh client/iterator.
- Locked/crossed books fail validation.
- Raw collection is kept active 24/7; schedule state remains metadata only.
- New archives contain a hashed coverage report and are classified as
  `VALID_OOS_SESSION`, `VALID_NO_TRADE_SESSION`,
  `VALID_MARKET_CLOSED_SESSION`, or `INVALID_DATA_COVERAGE`.
- Incomplete coverage is excluded from OOS and retained; structural
  SHA/zstd/PyArrow/DuckDB success cannot override missing temporal coverage.

## Current host snapshot captured during investigation

Root filesystem: 28 GiB used of 79 GiB, 48 GiB free. Paper data currently about
352 MiB. RAM: 7.7 GiB total, 5.8 GiB available. Clock synchronized, timezone
Europe/Moscow. Current paper process is active with `NRestarts=0` for its current
invocation. One stale MICRO position from 16 July remains in durable metadata;
there are no open broker orders or pending intents, and no real trading RPC was
called during this investigation.
