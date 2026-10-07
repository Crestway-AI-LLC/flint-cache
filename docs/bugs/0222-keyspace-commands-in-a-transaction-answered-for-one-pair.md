# BUG-0222: DBSIZE, SCAN, FLUSHDB and FLUSHALL inside a transaction answered for one pair (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by `client_compat_drill`, with
redis-py over two pairs and a placed tenant.
**Severity:** high for FLUSHDB: it cleared part of a tenant's keyspace and
answered OK. redis-py's `pipeline()` is a transaction by default, so
`pipe.flushdb(); pipe.execute()` took this path.

## What happened

Outside a transaction the proxy fans these four out over every pair and
combines the answers. Inside one it queued them, like any command, on the
one node the transaction runs on, which holds only that pair's share of
the keyspace. Measured 2026-10-07 through a proxy over two local seats with
20 keys:

- `MULTI; FLUSHDB; EXEC` answered `[OK]` and left 12 keys.
- `MULTI; DBSIZE; EXEC` answered 8.
- `MULTI; SCAN 0; EXEC` returned cursor 0, the end, with 12 of the keys.

## The fix

Inside a transaction, the proxy refuses the four unless that node holds the
tenant's whole keyspace, which is true of a tenant placed on one pair
(ADR-0053) and of a fleet with one pair:

```
ERR FLUSHDB inside a transaction would answer for one shard of this keyspace; send it outside MULTI
```

The refusal poisons the transaction, as a queue-time error does upstream,
so EXEC answers EXECABORT and applies nothing. Splitting the transaction
over every pair is not done: its commands would no longer apply as one
unit, which is what MULTI promises.

Upstream answers all four inside MULTI, so this is a deliberate difference
for a tenant spread over pairs, recorded in `command-support.md`.
