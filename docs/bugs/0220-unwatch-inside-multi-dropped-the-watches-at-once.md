# BUG-0220: UNWATCH inside MULTI dropped the watches at once, so EXEC applied over a write that should have aborted it (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by the corpus case "watch unwatch
(single connection)", which Valkey also passes, run against a seat and
through the proxy.
**Severity:** high for a client that sends it: a lost update. WATCH is the
only isolation a Redis transaction offers against another client.

## What happened

```
WATCH k
MULTI
UNWATCH
SET k mine
            (another client: SET k other)
EXEC
```

Redis 8.2.8 and Valkey 9.1.0 queue the UNWATCH. EXEC checks the watch,
finds `k` changed, and aborts with a null reply; `k` stays `other`. The seat
ran UNWATCH when it arrived, so EXEC found nothing watched and applied:
`k` became `mine`, overwriting the other client's write. Measured
2026-10-07 on a release seat and through a standalone proxy, by the
three-way differential run through flint-proxy.

The proxy had a second form of it. With nothing watched, it answered
UNWATCH itself with `OK`, so EXEC returned one reply fewer than the client
had queued, and a client matching replies to commands by position reads
the wrong one.

## The fix

- **The seat** queues UNWATCH inside a transaction. EXEC checks and clears
  the watches as before, and answers `OK` in the UNWATCH's place.
  `UNWATCH x` is refused for its argument count, as upstream refuses it.
- **The proxy** sends UNWATCH to the transaction's node when one is bound
  (a WATCH binds it), where it is queued. With none bound, nothing is
  watched, so the proxy queues it itself and puts `OK` in its place in
  EXEC's reply.

A proxy rolled before its seats forwards UNWATCH to a seat that still runs
it at once, the old behaviour; the roll order (seats first) avoids that.

## Not changed

HELLO and AUTH inside MULTI are still answered at once by the proxy, where
upstream queues them. No client library sends either inside a transaction,
and AUTH changes the connection's tenant, which a queued reply could not
express. `command-support.md` says so.
