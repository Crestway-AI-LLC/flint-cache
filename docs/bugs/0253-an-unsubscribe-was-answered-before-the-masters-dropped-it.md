# BUG-0253: an UNSUBSCRIBE was answered before the masters dropped it (FIXED 2026-10-10)

**Status:** **FIXED 2026-10-10**. The proxy answers `UNSUBSCRIBE` and
`PUNSUBSCRIBE` once every reachable master has dropped the subscription, as
it already answered `SUBSCRIBE` once they held it. Held by `pubsub_drill`,
which makes the race certain.
**Severity:** low. No message reached a client that had unsubscribed: the
proxy hands a message only to clients that hold it. What was wrong was the
count `PUBLISH` answers, for a moment after the unsubscribe.

## What happened

Gate b252 failed one step of 176: `client_compat`'s redis-py pub/sub check
in RESP2, at an assertion with no message:

    p.unsubscribe("news"); ... r.publish("news", "after") == 0

It had passed in the gates before it, and passed 60 rounds in a row on an
idle laptop. In Redis, once a client has read its `unsubscribe`, a
`PUBLISH` no longer counts it. Through the proxy it could: the proxy keeps
one link to each master and registers there how many of its clients hold
each channel (ADR-0052 D5). `SUBSCRIBE` waited for every link to register
the new count (`Hub::registered`); `UNSUBSCRIBE` sent its reply and let the
links catch up afterwards. A `PUBLISH` landing on a master before its link
did was counted for the departed client. On a gate box running four drills
at once, a link sometimes did not catch up first.

## Fixed

`Hub::remove` answers the sequence number of its change, and
`Subscriber::unsubscribe` waits for it, as `subscribe` does: at most
`SUBSCRIBE_WAIT`, and not for a master that cannot be reached. A connection
that closes drops its subscriptions without waiting, having no one to answer.

`FLINT_TEST_PUBSUB_SYNC_DELAY_MS`, unset in production, holds each link's
registration that long. `pubsub_drill` runs a proxy with 300 ms of it and
requires `PUBLISH` after `UNSUBSCRIBE` to count 0, and both replies to have
waited, so the check cannot pass vacuously.
