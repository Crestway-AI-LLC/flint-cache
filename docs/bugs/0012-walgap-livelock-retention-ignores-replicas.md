# BUG-0012: a lagging replica can never catch up — WAL retention ignores replica progress (FIXED and now GATED)

Status: **FIXED and now GATED** 2026-08-21 · found 2026-08-18 · Severity: high — a pair silently degrades to
one copy on one disk, and the seat churns full syncs until someone notices

## Update 2026-08-21 — the fix had shipped; the gate had not

ADR-0022's three parts are all in the tree, and this doc and the ADR were both
stale about it:

    min_acked_live / wal_headroom_seq / wal_headroom_exhausted   repl_hub.rs
    writes_shed_headroom + "-THROTTLED replica too far behind"   main.rs:2269
    DEFAULT_WAL_TTL_SECONDS   21_600   (6 h, was 1 h)
    DEFAULT_WAL_SIZE_LIMIT_MB  8_192   (8 GiB, was 1 GiB)
    DEFAULT_WAL_HEADROOM_SHED_SEQ 4_000_000 — the gate SHIPS ON

So the remedy existed. What did not exist was this doc's own closing
requirement — *"There is no drill for this, which is why it recurred
silently"* — and that is the actual reason round two happened after round one
was answered with detection. **A fix with no gate is round three waiting.**

`tools/wal_headroom_drill.sh` now holds all of it, as controls rather than
observations: the gate ships on (a threshold of 0 is a protection nobody has,
and that branch is reachable — verified by booting with `--wal-headroom-seq 0`);
ordinary traffic sheds nothing at the shipped threshold; tightened, the same
traffic IS refused, by the headroom message specifically rather than the lag or
deadline gate that would shed the same command for a different cause;
restoring the threshold restores writes; and the replica never reaches WALGAP.

The threshold is RAMPED down until it arms rather than fixed, per BUG-0030 —
on this box nothing armed until 2 sequences, so a fixed 10 or 100 would have
failed to create the condition and reported it as a product defect.

The remaining half is now done too: `--wal-ttl-seconds` and
`--wal-size-limit-mb` reached `flint-server` on 2026-08-22 (BUG-0033), routed
to the same `open_with_retention`, defaults untouched. Verified end to end
rather than by parsing — a replica stopped at seq 235 against a master driven
to 9002 under a 1 MB window reports `WALGAP ... sequence 235 is no longer in
the WAL`, in seconds instead of 8 GiB or six hours.

**This is a recurrence.** `packaging/aws/verify-watch.sh` (private ops repo)
was written because of the first one, and its header records it: *"on
2026-07-31 a playground replica hit the WAL-gap escalation, marked itself for
re-seed and exited exactly as designed, and the pair then served on a single
copy for hours with a live writer, because nothing was checking."* The
response to round one was **detection, not a fix**. Round two happened
2026-08-16..18 on the same playground and this time churned the pair into a
mutual `--replica-of` cycle with no master for nine hours.

## Symptom

From a replica's log, repeating:

    full sync complete; tailing from seq 85553648
    FATAL: WALGAP full sync required: sequence 90776415 is no longer in the WAL
           (latest is 90776416) — this link can never resume. Marking for
           re-seed and exiting; the next start will full-sync from a checkpoint.
    full sync complete; tailing from seq 90776422
    FATAL: WALGAP full sync required: IO error: No such file or directory:
           while stat a file for size: .../node-7002/archive/090999.log
    full sync complete; tailing from seq 91841836

Full-sync → tail → fall behind → the segment is pruned underneath → abort →
the supervisor restarts the seat → full-sync again. Each cycle re-transfers
the whole shard.

## The wrong conclusion to draw

That the replica is slow, or that the machine is undersized, and the fix is
to give it more resources or raise a timeout. **Read the sequence numbers:**

    sequence 90776415 is no longer in the WAL (latest is 90776416)

Off by **one**. The replica was a single sequence short when the segment went
away. A slow replica falls thousands or millions behind; a one-sequence miss
is retention being too tight, not a consumer being too slow. That distinction
is the whole diagnosis and it is free to check.

## Root cause

`crates/flint-storage/src/rocks.rs:185`

    opts.set_wal_ttl_seconds(3600);
    opts.set_wal_size_limit_mb(1024);

Retention is **1 hour or 1024 MB, whichever comes first**, and neither term
refers to replica progress. Any replica that lags — for any reason, including
because it is busy completing the previous full sync — can be cut off by a
budget that knows nothing about it. Re-seeding does not help, because the
re-seed re-runs the same race: it is a livelock, not a transient.

The playground master held 844 MB of data, so the size budget was the binding
term.

## Where the pieces are

| Path | Role |
|---|---|
| `crates/flint-storage/src/rocks.rs:185-186` | the retention settings — the defect |
| `crates/flint-storage/src/repl.rs:173` | emits `sequence {} is no longer in the WAL (latest is {})` |
| `crates/flint-server/src/main.rs:3664` | `FATAL: … Marking for re-seed and exiting` |
| `crates/flint-server/src/main.rs:143,327,3225` | existing WALGAP handling, incl. the promotion-fence interaction |

## Fix direction

Bound retention by the **slowest live replica's acked position**, not by time
or size. RocksDB will not do this natively for the WAL archive, so it needs
the master to track a min-acked sequence across registered replicas and gate
archive deletion on it (`DisableFileDeletions` / `EnableFileDeletions` around
the window is one mechanism), with a bounded disk-pressure escape hatch that
forces a re-seed **deliberately** rather than by accident.

Interaction worth designing against: `--fullsync-rate-bytes` (default
67108864) throttles how fast a master *serves* a full sync, so a throttled
re-seed takes longer and is therefore MORE likely to lose the same race it
was meant to escape.

## The check that should hold it

There is no drill for this, which is why it recurred silently. One is needed:
make a replica lag deliberately (throttle it, or drive the master past the
replica's consume rate), then assert the master does not prune a segment the
replica still needs — and that if it must, the seat re-seeds once and
converges rather than looping. Two occurrences in three weeks is the argument
for a gate, not a note.
