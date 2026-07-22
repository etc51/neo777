# Neobitcoin session calendar

Calendar rule version: `neo-moscow-2026-07-14-v1`

Effective from: 2026-07-14

Business timezone: `Europe/Moscow` (MSK, UTC+3)
External sources checked: 2026-07-15

## Authority and fail-closed rule

The expected clock is descriptive. The actual T-Invest trading status is the
final authority for an executable paper entry. The precedence is:

1. live `Info`/trading-status subscription for the exact instrument UID;
2. T-Bank's official crypto Neo-asset schedule;
3. official MOEX derivatives sessions/calendar for regime annotation;
4. the reviewed effective-dated local rule as fallback metadata.

T-Invest's instrument documentation explicitly recommends the status delivered
by the market-data status subscription when determining current availability.
Consequently, an expected open window cannot override `CLOSED`, `BREAK`,
`CLOSING`, a stale status, or `UNKNOWN`. Only an explicitly open normalized live
status sets `entry_allowed=true`. Conversely, an explicit open status is retained
as the availability signal even if local schedule metadata is stale; the
disagreement is recorded as a calendar/data-quality event and requires review.

The bundled rule intentionally refuses dates before 2026-07-14 because no older
reviewed effective-dated rule is embedded. Adding or changing a calendar is a
versioned configuration change.

## Expected T-Bank crypto Neo window

| Moscow day type | Pre-open marker | Expected Neo trading window | Session date |
|---|---:|---:|---|
| Weekday | 06:50 | 07:00–00:00 next calendar day | date on which 07:00 open occurs |
| Weekend or holiday | 09:50 | 10:00–00:00 next calendar day | date on which 10:00 open occurs |

The midnight close belongs to the session that opened on the prior Moscow date.
For example, an observation at 00:02 MSK on 2026-07-16 remains associated with
session date 2026-07-15 until the next day's pre-open boundary.

T-Bank's public Neo-asset guidance, checked 2026-07-15, states that cryptocurrency
Neo assets trade 07:00–00:00 MSK on weekdays and 10:00–00:00 MSK on weekends and
holidays. This is the expected Neobitcoin window; the MOEX regime alone must not
be used to authorize it.

## MOEX regime annotation effective 2026-07-14

For working days, official MOEX materials state:

| MSK | Regime label | Meaning |
|---:|---|---|
| 06:50–07:00 | `PRE_OPEN` | opening auction |
| 07:00–10:00 | `MORNING` | morning additional session |
| 10:00–19:00 | `MAIN` | main session |
| 19:00–23:50 | `EVENING` | evening additional session |
| 23:50–00:00 | `NEO_LATE_SESSION` | Neo expected open after MOEX evening boundary |
| 23:50–00:30 | clearing annotation | current derivatives clearing period; not an entry authorization |

For admitted weekend derivatives, MOEX publishes a 09:50–10:00 opening auction
and 10:00–19:00 weekend additional session. Neobitcoin's expected crypto Neo
window remains 10:00–00:00, subject to its exact live status.

MOEX's press release dated 2026-07-10 makes the weekday 06:50 start and the
07:00/10:00/19:00/23:50 boundaries effective 2026-07-14. Its session page also
publishes 07:00–10:00, 10:00–19:00, 19:00–23:50, and the weekend 10:00–19:00
period. The 23:50–00:30 clearing annotation comes from MOEX's official Unified
Trading Session materials.

## Labels and live-status normalization

The persisted session labels are:

- `PRE_OPEN`, `MORNING`, `MAIN`, `EVENING`, `NEO_LATE_SESSION`;
- `WEEKEND`, `HOLIDAY`;
- `CLOSING`, `CLOSED`, `BREAK`, `DEALER_MODE`, `UNKNOWN`.

The T-Invest prefix `SECURITY_TRADING_STATUS_` is removed before comparison.
Known open statuses include `NORMAL_TRADING`,
`TRADING_AT_CLOSING_AUCTION_PRICE`, `DEALER_NORMAL_TRADING`, and `OPEN`. Known
break, closed, closing, and pre-open values map to the corresponding label. A
dealer open status maps to `DEALER_MODE` while preserving the raw normalized
status.

Each canonical event and paper trade records:

- `exchange_ts`, `receive_ts`, and `processing_ts` in UTC;
- `session_date_msk` and `session_label`;
- normalized `trading_status`;
- `calendar_rule_version`.

No code uses the VPS local timezone to derive a session. The timezone must be the
IANA zone `Europe/Moscow`, not a fixed offset string, even though Moscow is
currently UTC+3.

## Holidays, early close, and status disagreement

Weekend is derived from the Moscow calendar date. Reviewed holiday dates are an
explicit effective-dated input; they are not guessed from weekdays. In the
absence of a reviewed holiday rule or live status, entries fail closed.

An early close or emergency break is governed immediately by live status. New
entries stop; existing virtual positions continue to be accounted for without
invented quotes. A later valid book/status may permit the predeclared conservative
exit policy. The status transition and every period of schedule disagreement are
included in `market_status_events` and `data_quality_events`.

If a live subscription becomes stale, reconnects, loses acknowledgement, or
reports an unknown value, expected wall-clock availability cannot reopen the
gate. A complete post-reconnect warmup is required before `feature_ready=true`.

## Session finalization

Expected crypto Neo close is 00:00 MSK. The fallback finalizer checkpoint is
00:15 MSK on the following calendar day, using the session date on which the
window opened. The default sequence is:

1. pass expected close;
2. observe a closed live status for the exact UID;
3. wait the configured grace period, default five minutes;
4. close active writers;
5. complete results through the last honestly available event;
6. validate and publish the daily archive;
7. queue same-task Codex delivery.

The timer is persistent, so a missed 00:15 trigger is run after boot. Runtime
recovery must also discover an unfinished prior session and finalize it
idempotently. A zero-trade day produces a typed zero archive with health and
quality evidence. No active `.inprogress` file enters the bundle.

## Primary sources

All sources below were checked on 2026-07-15:

- [MOEX: schedule expansion effective 14 July 2026](https://www.moex.com/n101980)
- [MOEX: derivatives trading sessions and calendar](https://www.moex.com/torgovye-sessii-na-srochnom-rynke)
- [MOEX: Unified Trading Session materials, including the 23:50–00:30 clearing period](https://www.moex.com/media/edinaya-torgovaya-sessiya-na-srochnom-rynke-17032025-sajt.pdf)
- [T-Bank: Neo-asset mechanics and trading hours](https://www.tbank.ru/invest/help/brokerage/account/forts/neo/)
- [T-Invest API: instrument availability and trading-status authority](https://russianinvestments.github.io/investAPI/head-instruments/)
- [T-Invest API: market-data and Info subscriptions](https://russianinvestments.github.io/investAPI/marketdata/)

The checked date is not a promise that schedules cannot change. Operators must
review these primary sources and live behavior before adding a new effective
rule.
