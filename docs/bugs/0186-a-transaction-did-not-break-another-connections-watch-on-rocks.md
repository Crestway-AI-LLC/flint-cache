# BUG-0186: on rocks, another connection's transaction did not break a WATCH, so the watcher's EXEC committed over it (FIXED 2026-09-26)

**Status:** **FIXED 2026-09-26**. Not in v0.1.0-rc.77; live since
v0.1.0-rc.78 (rolled 2026-09-30).
**Severity:** high: a silent lost update in the one mechanism whose whole job
is to prevent one, on the engine that ships.

## What was measured

A rocks seat run locally, 2026-09-26, two connections:

    A: SET {w}k 1; WATCH {w}k
    B: MULTI; SET {w}k 2; EXEC            -> [OK]
    A: MULTI; SET {w}k 3; EXEC            -> [OK]     (must be nil)
    A: GET {w}k                           -> "3"      (B's write is gone)

With B's change made by a plain `SET` instead, A's `EXEC` answered nil, as
it must. On the mem engine both cases were right. Found while building
ADR-0051's script commit, which needs the same step.

## The mechanism

`WATCH` compares a per-key version taken at `WATCH` with the version at
`EXEC`. The versions move when `WatchedKv`, the wrapper `main` puts around
the store, sees a put or a delete. A transaction does not write through it:
`exec_transaction` runs its commands against a `BatchingKv` and commits the
buffer with `commit_ops`, which on rocks is `RocksKv::apply_writes` on the
engine underneath the wrapper. So a transaction's writes moved no version,
and any `WATCH` on the keys it wrote stayed intact.

The pipelined-write path and the async write queue commit the same way, and
each bumps the versions by hand (BUG-0080). The transaction path did not.
The comment where the wrapper is built said a transaction's commit was
"covered by construction", which is why nothing checked. On mem,
`commit_ops` replays the buffer through the wrapper, so the versions moved
there and the defect was rocks-only.

## The fix

One helper, `commit_watched`, bumps the version of every key in the buffer
and then commits it. The transaction path and the pipelined path both commit
through it, and so will anything else that commits a buffer. The bump comes
before the commit: a spurious bump can only abort a transaction that `WATCH`
permits to abort, and a missed one commits one it must not. The comment at
the wrapper now names what it does not cover.

Covered by a `flint-server` wire test on a rocks seat built as `main` builds
one (one shared watch table, the wrapped store), which replays the sequence
above. It fails with the bump removed. The existing rocks wire harness gives
each connection its own watch table, so it could not have caught this.
