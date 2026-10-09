# BUG-0249: a pub/sub cut-off left no counter and no log line (FIXED 2026-10-09)

**Status:** **FIXED 2026-10-09**. Both cut-offs are now counted and logged,
and `PROXYSTATS` and `FLINTINFO` carry pub/sub fields, which `flint-exporter`
turns into series as it does every numeric field. Held by the seat's and the
proxy's overflow unit tests, which assert each cut-off is counted exactly
once, and by `tools/pubsub_drill.sh`, which checks each new field and both
log lines against a slow client, a slow link and a seat restart.
**Severity:** medium. Delivery is at most once by design (ADR-0052 D5), and
cutting off a subscriber 32 MiB behind is Redis's behaviour too. The defect
was that an operator could not tell when messages were lost, how often, or
for whom.

## What was missing

Pub/sub shipped with two cut-offs (ADR-0052 stage 3) and no trace of
either:
- **The seat cut off a proxy's link** 32 MiB behind (`Outbox::push` in
  `flint-server`'s `pubsub.rs`). It shut the socket with no log line and no
  counter. Every subscriber that proxy held on that master lost messages
  until the link was dialed again.
- **The proxy cut off a client** 32 MiB behind, closing it as it closes a
  client that hung up. This is the one a tenant notices: a Celery result
  that never arrives, or an asynq cancellation that is missed.

The only trace was the proxy's `pubsub link to <addr>: <err>; dialing again`
line, which reads a seat's cut-off as `seat closed the link`. `PROXYSTATS`
and `FLINTINFO` had no pub/sub field, so no series could say a subscriber
lost messages. Found by the operations side while writing the release's
checks.

## Fixed

**`PROXYSTATS`** (`flint_proxy_*` series):

| field | kind | meaning |
| --- | --- | --- |
| `pubsub_links` | gauge | links connected to a master now |
| `pubsub_clients` | gauge | client connections holding at least one subscription |
| `pubsub_clients_cut_total` | counter | clients cut off 32 MiB behind |
| `pubsub_link_redials_total` | counter | link dials after a connection failed or was lost, one per attempt |
| `pubsub_messages_total` | counter | messages handed to clients' connections, one per client reached |

**`FLINTINFO`** (`flint_*` series): `pubsub_links_cut_total`, the
subscriber links this seat cut off 32 MiB behind.

**Log lines**, one at each cut-off, at the end that made it:
- seat: `pubsub: cut off the subscriber link from <addr>, 32 MiB behind
  (ADR-0052): its proxy's clients lose this seat's messages until it dials
  again`;
- proxy: `pubsub: client <id> (<name>) of namespace <ns> cut off, 32 MiB
  behind (ADR-0052): it was disconnected, and the messages queued for it
  were dropped`. The id is the one `CLIENT ID` answers.

Each cut-off is counted once: the overflow flag is swapped rather than
stored, so two publishes racing past the limit cannot count it twice.

No alarm is implied. A slow consumer losing messages is the contract; the
counters say how often it happens.
