# BUG-0033: the WAL retention window cannot be bounded (OPEN)

Status: OPEN, found 2026-08-19 while building release acceptance · Severity:
medium — this is not a fault in the running product, it is the reason the
fault that took a production pair single-copy for thirteen minutes has no
automated coverage anywhere

## Symptom

There is no way to ask a node for a small WAL retention window, so no test
can put a replica outside it without writing more than 8 GiB or waiting six
hours. BUG-0031 — a replica whose needed sequence has left the retained WAL
failing to recover unattended — therefore has no drill, no acceptance check,
and no regression test. It was found by a production incident and would be
found the same way again.

> **Correction, same day.** The last sentence was wrong, and wrong in the
> direction that matters: it declared a thing impossible on the strength of
> one blocked route. A regression test exists, landed with the BUG-0031 fix
> in `70f4d99`, and it touches no retention config at all. See the section
> below; what survives of this bug is narrower and is stated there.

## Root cause

Retention is two RocksDB terms, and both are fixed at open:

```rust
// crates/flint-storage/src/rocks.rs
pub const DEFAULT_WAL_TTL_SECONDS: u64 = 21_600;   // 6 h, was 1 h
pub const DEFAULT_WAL_SIZE_LIMIT_MB: u64 = 8_192;  // 8 GiB, was 1 GiB

pub fn open(path: &Path) -> Result<Self, rocksdb::Error> {
    Self::open_with_retention(path, DEFAULT_WAL_TTL_SECONDS, DEFAULT_WAL_SIZE_LIMIT_MB)
}
```

`open_with_retention` exists, is documented as the seam, and is **called by
nothing outside its own tests**. `flint-server` calls plain `open`, and has
no `--wal-ttl-seconds` or `--wal-size-limit-mb` flag to pass anything else.
The values are immutable after open, so `FLINTCONFIG` cannot reach them
either.

The widening in ADR-0022 is what put the boundary out of reach. Moving 1 h /
1 GiB to 6 h / 8 GiB was the right call for the fault it addressed — a
replica died against the old window twice in three weeks — but it also moved
the window from "reachable by a test in about a minute" to "reachable only by
a fleet running for a working day." Nothing recorded that trade at the time,
which is how the coverage quietly went to zero.

## What is NOT the workaround

`--wal-headroom-seq` looks like the knob and is not. Per ADR-0022 it is a
write-shedding threshold whose purpose is to make the master refuse writes
*before* a replica meets a recycled segment — it exists to PREVENT this
condition, not to provoke it. It also compares against the slowest **live**
replica, so while the replica under test is stopped it does nothing at all.

Two artifacts were written against that misreading and are wrong because of
it: `tools/wal_gap_recovery_drill.sh` in the ops repo, which additionally
used an inventory key (`server-args`) that has never existed in any release,
and a first draft of `packaging/aws/acceptance/run.sh`. Neither had been run.
A drill that has never run is a hypothesis.

## Consequence

The incident of 2026-08-19 had three stacked defects. OPS-0012 (the repair
path changed under a roll) now has an acceptance check that observes the
behaviour directly. BUG-0032 (`start` asserts `pair[0]` is a live master) has
one too. BUG-0031 has neither, and cannot until this is fixed.

## What was wrong with the claim above

`a_cursor_the_wal_cannot_reach_is_a_gap_not_silence`, in
`crates/flint-storage/src/repl.rs`, reaches the boundary in milliseconds by
**retiring the segment by hand instead of waiting for retention to expire
it**. A flush makes the segment holding the stranded cursor obsolete,
RocksDB moves it to `archive/`, and `archive/` is precisely where expiry
would have deleted it from — so deleting it directly reaches the same end
state without `wal_ttl_seconds` or `wal_size_limit_mb` being consulted at
all:

```rust
let stranded = master.latest_seq() / 2;
assert!(!master.updates_since(stranded).expect("tail").is_empty(),
        "precondition: this cursor is reachable while its WAL is retained");
master.flush();
/* … a few more writes … */
for entry in std::fs::read_dir(master.path().join("archive"))? { /* remove *.log */ }
assert!(retired > 0, "precondition: a WAL segment was retired");
```

Both preconditions are asserted, which is what makes it a measurement rather
than a hope, and the test now REQUIRES a `WalGap` from admission rather than
accepting either outcome — verified by removing the fix, which reproduces the
playground's exact shape: `Ok([ReplBatch { first_seq: 9 }])` for a cursor
stranded at 4.

The generalisable error is not about WAL. I found one route to the boundary
(shrink the window), found it blocked, and wrote down that the boundary was
unreachable — when what I had actually established was that *my* route was.
"Cannot be tested" is a claim about every possible approach and needs far
more evidence than "the approach I thought of does not work."

## What still holds

At the **drill and acceptance level**, where a real replica tails a real
socket and the harness cannot reach into the master's process the way an
in-process test can. `packaging/aws/acceptance/run.sh` reports WAL-gap
recovery as NOT EXERCISED for that reason.

**Tried, and the answer is: half.** A drill owns the fleet and its paths, so
it can retire segments directly. The flush a drill was assumed to lack turns
out to exist and to be socket-reachable — `FLINTSNAPSHOT <root>` moves
obsolete segments into `archive/`:

    archived segments before: 1
    FLINTSNAPSHOT -> OK snap-1787184220293-seq3202-e0.1
    archived segments after:  2

So `packaging/aws/acceptance/run.sh` now forces a real gap against the
bundle, on a real fleet over real sockets, with both preconditions asserted
(segments retired > 0, master advanced past cursor + 1), and asserts the
replica recovers unattended. That is the operational property the 2026-08-19
incident was about — it needed a hand-run `host-wipe-node` while a pair sat
single-copy for thirteen minutes — and it is no longer uncovered.

What a socket still cannot reach is the NARROW case this bug's sibling
names: admission returning `Ok` for a **non-empty span starting past** the
requested sequence. Retiring whole segments strands a cursor *coarsely*, and
the coarse case was never the broken one — admission comes back empty, gets
`WalGap`, and the re-seed path does the right thing. Verified against a
pre-fix build, which recovered cleanly through both `restart-node` and
`host-mark-reseed` + `start`. The narrow case needs a cursor placed
mid-segment, which `latest_seq() / 2` gives an in-process test and no socket
offers. It belongs in `a_cursor_the_wal_cannot_reach_is_a_gap_not_silence`
and no drill will take it away.

## Fix

Add `--wal-ttl-seconds` and `--wal-size-limit-mb` to `flint-server` and route
them to the `open_with_retention` that already exists. The seam is built; it
simply has no caller.

This is now a convenience rather than the only path — the regression test
above needs none of it. It is still worth having: it makes the boundary
reachable to a drill without reaching into RocksDB's internal directory
layout, which is a private detail a test should not have to depend on.

Then two things become possible:

1. A `flint-storage` unit test for the actual BUG-0031 defect —
   `updates_since_budgeted` returning `Ok` for a non-empty span that starts
   *past* the requested sequence, instead of `WalGap`. This is the cheaper
   and more precise home for it, and it needs no fleet at all.
2. A real gap drill, and an acceptance check to match. Until then
   `packaging/aws/acceptance/run.sh` reports the boundary as **not
   exercised** rather than passing over it, and the n/a line names this bug.

Defaults must not change: the flags are for tests and for operators who have
measured their own fleet, not a new recommended value.
