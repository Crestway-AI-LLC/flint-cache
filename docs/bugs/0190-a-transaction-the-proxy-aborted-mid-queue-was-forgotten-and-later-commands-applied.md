# BUG-0190: a transaction the proxy aborted mid-queue was forgotten, and the commands after it applied (FIXED 2026-09-27)

**Status:** **FIXED 2026-09-27**. Not in v0.1.0-rc.77: it ships with the next
release.
**Severity:** high: a partial transaction, applied silently, on the path every
client library uses.

## What was measured

Found by ADR-0052's stage-2 probe on a gate box: rq 2.8.0's enqueue, one
`MULTI`/`EXEC` over a job's hash, its queue's list and the set of queues,
failed with `EXEC without MULTI`. The same enqueue had failed with
`CROSSSLOT` the run before. Its keys carry no hash tag, and the job's id is
random, so which pair each key lands on varies from run to run.

Reproduced through a local proxy in front of two seats, 2026-09-27:

    MULTI                   -> +OK
    SET k1 1                -> +QUEUED        (binds the transaction to k1's pair)
    SET x2 2                -> -EXECABORT ... belongs to a different shard
    SET k1b 3               -> +OK            (applied, outside any transaction)
    EXEC                    -> -ERR EXEC without MULTI
    GET k1b                 -> "3"

## The mechanism

The proxy binds a transaction to the pair that owns its first key, because
the queue lives on one backend connection (ADR-0012). A command whose key
belongs to another pair cannot be queued there, and the proxy aborts:
`abort_txn` drops the backend connection, which discards the node's queue,
and resets the transaction. The reset was the defect. The client still had
the transaction open, and every client library pipelines the whole
`MULTI`...`EXEC` at once, so the commands behind the failing one arrived at a
proxy that no longer knew of any transaction and ran them as plain commands.
Then `EXEC` found no `MULTI`.

The same path serves any other abort inside an open transaction: a backend
that refuses `MULTI`, and a backend connection lost while queueing.

## The fix

A transaction aborted while the client has it open stays open, doomed, as a
Redis transaction does after a command fails to queue. Until the client ends
it, nothing reaches a backend. Every command answers `QUEUED`; `MULTI` and
`WATCH` are refused as nested; `EXEC` answers `EXECABORT Transaction
discarded because of previous errors.` and `DISCARD` answers `OK`, and either
returns the connection to normal. The failing command itself still answers
the `EXECABORT` that names the reason.

Covered in `client_compat_drill`: redis-py's transaction pipeline over keys
on the two pairs is refused, none of its writes apply, and the connection
works afterwards. The local reproduction above applied nothing with the fix.
