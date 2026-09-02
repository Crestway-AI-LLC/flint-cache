# BUG-0085: a control panicked on the state it exists to establish

Status: FIXED 2026-09-02
Severity: low (test-only — no product path involved), but it red-lit a gate

## What happened

`repl::bug_0050_iterator_shape::a_genuinely_recycled_wal_still_raises_walgap`
failed on a gate box, after 166 sibling tests passed:

    thread '...a_genuinely_recycled_wal_still_raises_walgap' panicked at
    crates/flint-storage/src/repl.rs:1732:14:
    iter: Error { message: "IO error: No such file or directory: While opening
    a file for sequentially reading: /tmp/flint-b50-recyc-13650/archive/000018.log" }

The test opens the store with `open_with_retention(&d, 0, 0)` — retention
**off** — writes past several flushes, and then asserts that a cursor the WAL
can no longer reach is reported as a `WalGap` rather than silently served.

Before that assertion it runs a control, because a test that cannot fail is
worse than one that does not run: it checks that the scan genuinely cannot
reach the stale cursor, and skips loudly if the environment kept everything.
The control was written as

    let reachable = kv.db().get_updates_since(0).expect("iter") ...

**The `Err` it panicked on is the condition under test.** Retention is off, so
the segments behind sequence 0 are deleted, and RocksDB reports that by failing
to open one. That error is *stronger* evidence the scan cannot reach `stale`
than an iterator which opens and yields a newer floor. `.expect("iter")` turned
the control's own success condition into a panic.

## Why it had never fired

It was found deliberately, by running the same 139-step suite on a
**c7i.2xlarge (8 vCPU / 16 GiB)** instead of the usual **c7i.4xlarge (16 vCPU /
32 GiB)**. The smaller box recycles sooner under the same write volume, so the
segment is gone by the time the control looks; on the larger box it is usually
still openable.

So the test was never stable — only lucky in the size of box it habitually ran
on. Nothing about the change under test was involved, and the same commit
passed 139 steps on the 16-vCPU box.

## Fix

Match the three outcomes instead of collapsing two of them:

    let reachable = match kv.db().get_updates_since(0) {
        Ok(it) => it.filter_map(|i| i.ok()).map(|(f, _)| f).next(),
        // A MISSING segment is the recycling this test needs. Any other
        // error is a broken database, and collapsing the two would let a
        // real fault masquerade as the condition under test.
        Err(e) if e.to_string().contains("No such file or directory") => None,
        Err(e) => panic!("the WAL scan failed for a reason that is not recycling: {e}"),
    };

The tri-state matters here for the same reason ADR-0028 gives: "cannot look" is
not "absent". A blanket `Err(_) => None` would fix the red gate and would also
let a genuinely broken database walk straight into the assertion and pass it for
the wrong reason. The unexpected-error arm fails loudly, which is the safe
direction — if the RocksDB message ever drifts, this test goes red rather than
quietly asserting nothing.

The missing-segment arm is exercised on the 8-vCPU box, not locally: on macOS
the segment is still openable, so a local run takes the `Ok` path and reaches
the real assertion.

## What it is worth

The bug is trivial. What it cost to find is the point: a half-size gate box ran
the suite for 1991s against 1955s on the full-size box — **1.8% slower for half
the vCPU** — and the single defect it turned up was in a *unit test*, not in any
of the 21 fleet drills, which was the opposite of what was predicted. Box size
is a cheap axis of variation, and it is not currently varied by anything.
