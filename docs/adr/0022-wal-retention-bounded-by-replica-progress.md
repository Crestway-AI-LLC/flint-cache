# ADR-0022: Bound WAL retention by replica progress, not by time and size

Status: **ACCEPTED and IMPLEMENTED** — parts 1 and 2 in full, part 3's
defaults raised (6 h / 8 GiB) but its `--wal-retain-*` flags not built. Gated
by `tools/wal_headroom_drill.sh` since 2026-08-21. The "today nothing reports
any of this" below was true when written and is not now — `wal_headroom_seq`,
`wal_min_acked_seq` and `writes_shed_headroom` are all in FLINTINFO.

Originally PROPOSED — August 2026. Written after a replica livelocked on the
playground for the second time in three weeks and, on the second occasion,
left a pair with no master for nine hours.

## Context

### What breaks

A replica tails the master's WAL from its own cursor. `Repl::updates_since`
asks RocksDB for everything after `last_applied`; when that sequence lives in
a segment RocksDB has already recycled, the iterator yields nothing.
`crates/flint-storage/src/repl.rs:171` refuses to report that as "no updates"
— correctly, since doing so is how *"a replica ends up frozen at a stale
cursor while the master still counts it as live"* — and raises `WalGap`
instead. `flint-server` escalates it (`main.rs:3664`):

    FATAL: WALGAP full sync required: sequence 90776415 is no longer in the
    WAL (latest is 90776416) — this link can never resume. Marking for
    re-seed and exiting; the next start will full-sync from a checkpoint.

The seat exits, the supervisor restarts it, it full-syncs, tails, falls
behind, and hits the same wall. Observed on the playground across at least
three cycles:

    full sync complete; tailing from seq 85553648
    FATAL: WALGAP ... sequence 90776415 is no longer in the WAL
    full sync complete; tailing from seq 90776422
    FATAL: WALGAP ... No such file: .../node-7002/archive/090999.log
    full sync complete; tailing from seq 91841836

This is a **livelock, not a transient**: the recovery path re-runs the race
that caused the failure. Each cycle re-transfers the whole shard.

### Why it happens

`crates/flint-storage/src/rocks.rs:185`

    opts.set_wal_ttl_seconds(3600);
    opts.set_wal_size_limit_mb(1024);

Retention is **one hour or one gigabyte, whichever comes first**. Neither
term refers to any replica. A budget that cannot see its only consumer will
eventually delete something that consumer still needs, and the probability
rises exactly when the replica is slowest — which is while it is completing
the previous full sync.

### The measurement that rules out the obvious explanation

The instinct is "the replica is too slow" or "the write rate is too high".
Neither survives the evidence:

- The playground soak runs at **~50 ops/s**. A gigabyte of WAL did not
  accumulate from write volume.
- The failing gap was **off by one**: `sequence 90776415 is no longer in the
  WAL (latest is 90776416)`. The replica was a **single sequence** short. A
  slow consumer falls thousands behind; a one-sequence miss is a retention
  window closing on a consumer that was essentially caught up.

What actually consumed the budget was the replica spending most of its life
**full-syncing rather than tailing**. Each re-seed takes a checkpoint at some
sequence and then must catch up from there; while it does, the master keeps
writing, and `--fullsync-rate-bytes` (default 64 MiB/s) throttles how fast
the master *serves* that checkpoint — so the slower the re-seed, the more
likely it loses the race it was meant to escape. The recovery mechanism is
part of the loop.

### Why this ADR rather than a constant

Raising the budget moves the cliff. It does not remove it, and it gives no
signal about where the new cliff is. Two occurrences in three weeks, the
first of which was answered with *detection* (`verify-watch.sh`) rather than
a fix, is the argument for changing the rule.

## Decision

**Retention becomes a function of the slowest live replica's position, and
the master applies backpressure rather than letting a replica die.**

Three parts, in dependency order.

### 1. The master learns its own WAL headroom

`ReplHub` already holds per-replica `(acked_seq, last_ack_ms)` and already
filters to LIVE replicas for RPO purposes (`effective_acked` takes the
freshest, because promotion can only choose among the living). Retention
needs the mirror of that: **`min_acked_live`, the slowest live replica**,
since the WAL must survive until the last consumer has passed it.

Headroom is then `latest_sequence - min_acked_live`, exported so the
condition is visible before it is fatal:

    flint_wal_headroom_seq   latest_sequence - min_acked_live
    flint_wal_archive_bytes  size of the archive directory
    flint_wal_min_acked_seq  the slowest live replica's cursor

Today nothing reports any of this, which is why the first occurrence was
diagnosed as a replica problem.

### 2. Shedding extends to WAL headroom

The product already sheds writes to bound a replication risk: past
`--lag-hard-ms` the master returns `-THROTTLED` rather than let the at-risk
set grow. That is the same shape of problem — an unbounded gap between master
and replica — measured in time rather than in retained WAL.

So: **when a live replica's cursor approaches the oldest retained sequence,
shed writes.** A `-THROTTLED` write is a bounded, visible, recoverable
condition that clients already understand. A WALGAP is an unbounded outage
that costs a full re-seed and, twice now, a pair.

This deliberately does NOT try to make RocksDB retain more. `wal_ttl_seconds`
and `wal_size_limit_mb` are set at open and immutable, and the archive is
deleted by RocksDB on its own schedule; fighting that with
`DisableFileDeletions` would suspend SST cleanup too and trade a replication
bug for a disk-exhaustion bug. Slowing the producer is the mechanism the
system already has and the one that composes with the existing contract.

### 3. Retention becomes configurable, with a default chosen against evidence

`--wal-retain-seconds` and `--wal-retain-mb`, defaulting well above today's
1 h / 1 GiB. The defaults exist to make shedding rare; the shedding exists to
make the defaults safe. Neither alone is sufficient, which is why both are in
this ADR rather than a config change on its own.

## Consequences

- **A lagging replica costs write throughput instead of costing the pair.**
  That is the trade this makes explicit: the alternative is not "no cost",
  it is a re-seed loop and an eventual single copy.
- **A new way to shed writes** — operators will see `-THROTTLED` in a
  situation that previously produced no client-visible error at all. It must
  be distinguishable from lag shedding in metrics and logs, or the first
  incident becomes "why is it throttling, lag looks fine".
- **`min_acked_live` makes a dead replica dangerous in a new way.** A replica
  that is live-but-stuck pins retention. The liveness window
  (`LIVENESS_WINDOW_MS`) already bounds this: a replica that stops acking
  drops out of the live set and stops pinning. Worth stating because the
  failure mode of "retention pinned forever by a zombie" is exactly what a
  naive min-across-all-replicas would produce.
- **Re-seed remains the escape hatch**, but becomes deliberate. Under disk
  pressure the master must be allowed to abandon a replica's position and
  force a full sync — the difference from today is that it happens because a
  bound was reached and reported, not because a timer expired unnoticed.

## What must be proven

No drill exercises this, which is why it recurred silently. The gate:

1. Bring up a pair, make the replica lag deliberately (throttle its link or
   drive the master past its consume rate).
2. Assert the master **sheds** rather than the replica dying — no WALGAP, no
   process exit.
3. Release the throttle and assert the replica converges without a re-seed.
4. Then force the disk-pressure path and assert the re-seed happens **once**
   and converges, rather than looping.

Step 4 is the one that distinguishes this from the current behaviour, which
also "re-seeds" — endlessly.

## Alternatives considered

- **Raise the budget.** Rejected as the whole fix: moves the cliff, reports
  nothing, and the evidence shows volume was never the driver. Kept as part 3
  because a generous budget is what makes shedding rare.
- **Gate RocksDB file deletion on min-acked.** Rejected: `DisableFileDeletions`
  is not WAL-scoped, so it suspends SST cleanup and converts a replication
  failure into a disk failure.
- **Re-seed in place instead of exiting.** Rejected as insufficient: it
  tightens the loop without breaking it, since the race is unchanged. Worth
  doing separately — a process exit per gap is a bad failure mode regardless
  — but it is not this ADR.
- **Let the replica fall back to snapshot-based catch-up.** Deferred: the
  snapshot machinery exists (`--rewind-snaps`) and may be a better re-seed
  source than a live checkpoint, but it changes what a re-seed *is* and
  deserves its own decision.
