# BUG-0080 — a queued write never bumps the watch table, so EXEC commits when it must abort (OPEN)

**Found** 2026-08-31, while fixing the same defect in ADR-0027's batching path.
Not introduced by that work: this one is in the async write queue (ADR-0005
D4) and predates it.

## What is wrong

`WatchedKv` is what makes WATCH work. It wraps the store and bumps the watch
table on `put`/`delete`, so a write records itself against any WATCH on that
key. Its own doc claims the coverage is total:

> expiry, a transaction's commit and the async queue's commit all reach the
> store through here, and none of them passes the command layer.

**On the rocks path that is not true of the queue.** Its consumer commits with

```rust
rocks.apply_writes(&batching.into_ops())     // write_queue.rs:317
```

which goes to `RocksKv` DIRECTLY, underneath the wrapper. `write_queue.rs`
contains no reference to `WatchTable` at all — it is never given one.

So a write committed by the queue is invisible to a watcher, and an `EXEC`
whose watched key was changed by a queued write **commits instead of
aborting**. That is a silent wrong answer, not an error.

## How it surfaced

The identical hole in ADR-0027's batching was caught by `proxy_conformance`:

```
[transactions] watch unwatch (single connection):
  step 11: `EXEC` expected NilArray, got Array(Some([Simple("OK")]))
```

Only THROUGH THE PROXY. On one connection, WATCH leaves `txn.watches`
non-empty and the write is not deferred; the proxy puts the write on a
different backend connection whose watches are empty, so it is. The corpus
case is explicit that "WATCH tracks the key, not the author".

The queue has the same shape and the same blind spot: whether the write is
deferred depends on the CONNECTION's opt-in scope, while WATCH is a promise
about a KEY, held by whoever is watching it.

## Why it has not been seen

`--async-writes` is opt-in and off by default, and the FLINTNS 'a' flag is set
only for tenants the control plane marks. A deployment has to have turned the
queue on AND have a client using WATCH before the two meet.

## The fix, and the ordering that matters

Give the queue the `WatchTable` and bump every key in the batch at commit, as
`commit_pending` now does for ADR-0027:

```rust
for (k, _) in &ops { watch.bump(k); }
```

**Bump BEFORE the commit.** The failure directions are not symmetric: a
spurious bump can only abort a transaction that might have committed, which
WATCH explicitly permits — it is optimistic. A MISSED bump commits one that
must abort, which nothing permits.

## Also worth doing

`watch.rs`'s doc comment asserts coverage the code does not have, and that
assertion is exactly why this went unnoticed. Either the claim comes out or a
test pins it — a comment that says "everything reaches the store through here"
is worth nothing without something that fails when a path stops doing so.
