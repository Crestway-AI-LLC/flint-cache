# BUG-0033: the WAL retention window cannot be bounded, so its boundary cannot be tested (OPEN)

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

## Fix

Add `--wal-ttl-seconds` and `--wal-size-limit-mb` to `flint-server` and route
them to the `open_with_retention` that already exists. The seam is built; it
simply has no caller.

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
