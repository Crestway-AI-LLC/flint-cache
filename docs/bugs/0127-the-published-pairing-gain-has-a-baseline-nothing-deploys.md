# BUG-0127: the published pairing gain has a baseline nothing deploys

**Status:** OPEN, found 2026-09-08 · Severity: medium. Not a wrong number —
a true number about a configuration no deployment runs, published as the gain
an operator should expect.

## The claim, and where it is published

`docs/self-hosting.md` recommends the pairing `FLINT_LEVEL_BASE_MB=64` with
`FLINT_BG_JOBS=4` and prices it with BUG-0013's numbers: **2.6x ingest**,
write amplification **16.0 → 10.2**, resident bytes **+36%**.

BUG-0013 states the baseline for those in one clause: *"At 96 GB on a 2-core
seat, against the **8 MB** / default baseline"*. Both of its sweeps used an
8 MB level base — the table under "Why not a default" says so for the ~760 MB
run and the 96 GB run alike.

## Nothing runs an 8 MB level base

`crates/flint-storage/src/rocks.rs:491` sets `max_bytes_for_level_base` **only**
when `FLINT_LEVEL_BASE_MB` is present:

```rust
if let Some(mb) = env_u64("FLINT_LEVEL_BASE_MB") {
    opts.set_max_bytes_for_level_base(mb * 1024 * 1024);
}
```

Unset, the option is never touched and RocksDB's own default applies: **256 MB**.

Nothing sets it on a real seat. Not the AMI, not `first-boot.sh`, not any
inventory, nothing in ops `packaging/`. The only places `FLINT_LEVEL_BASE_MB=8`
appears anywhere in either repo are four test harnesses:

| file | value |
|---|---|
| `tools/ingest_decay_sweep.sh:354` | `DECAY_LEVEL_BASE_MB:-8` |
| `tools/disk_selffill_drill.sh:175` | `8` |
| `tools/ingest_saturation_drill.sh:67` | `8` |
| `tools/read_under_stall_drill.sh:44` | `8` |

**The baseline exists only inside the drills that measure it.** An operator
reading `self-hosting.md` has a seat at 256 MB, and the pairing would *lower*
that to 64 MB — the opposite direction from the one the surrounding prose
reasons about ("a larger level base means fewer levels to search").

## Why the 8 MB was chosen, because it was not careless

For the ~760 MB sweep it was necessary. A dataset that small against a 256 MB
level base never leaves L0, and BUG-0013 records exactly that failure: *"the
write buffer never leaves L0, so there was no deepening LSM and nothing to
decay"*. `ingest_decay_sweep.sh` now refuses any shape where the dataset is
under 50x the level base, as a preflight. 760 MB / 8 MB is 95x and clears it;
760 MB / 256 MB is 3x and would now be refused outright.

**At 96 GB that constraint does not bind.** 96 GB against a 256 MB base is
375x, comfortably clear of the same floor. So for the run the recommendation
actually rests on, a stock baseline was available and the 8 MB was continuity
with the earlier sweeps rather than a requirement. That is the whole defect:
not the choice, but that the ratio it produced was published without its
baseline attached.

## The one stock-baseline datapoint

`readtail-20260908T225452Z` (ops `packaging/aws/readtail-pairing/`) ran stock —
`max_background_jobs=2`, `max_bytes_for_level_base=268435456`, both read off
the engine's LOG — against the pairing, at 96 GB on a 2-core seat. Its bulk
fill moved **21,705 → 23,753 ops/s, +9.4%**.

That is not 2.6x, and it is **not evidence that 2.6x is wrong**: that run used
1 KiB values where BUG-0013 used 10 KB incompressible ones, and value size
changes compaction behaviour materially. It does not separate "different
baseline" from "different value size". What it does establish is that the
pairing's write gain **against the configuration operators actually run has
never been measured**, and the one adjacent number is an order of magnitude
smaller than the published one.

## What would settle it

One run of the existing `readtail-pairing` harness with 10 KB incompressible
values, which isolates value size against the same stock baseline. If the gain
stays near +9.4%, the published figure needs its baseline stated and its
magnitude re-derived. If it approaches 2.6x, the baseline was immaterial and
the doc needs one clause rather than new numbers.

Until then `self-hosting.md` must state the baseline it is quoting. A ratio
without its denominator is not a measurement an operator can act on.
