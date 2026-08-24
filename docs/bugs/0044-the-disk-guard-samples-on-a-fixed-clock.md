# BUG-0044: the disk guard samples on a fixed clock, so a fast writer outruns it

Status: OPEN, found 2026-08-24 · Severity: medium-high — the guard exists to
stop a seat filling its volume, and at NVMe write rates the volume can cross
the whole remaining headroom between two samples. A FIX ALREADY EXISTS, on an
unmerged branch (see below), which is the unusual part of this report.

## The defect

`crates/flint-server/src/main.rs` starts the disk-guard sampler on a fixed
cadence:

    let every = Duration::from_millis(arg("--disk-sample-ms")... .unwrap_or(2_000));

Two seconds, unconditionally, whatever the remaining headroom is. The guard
refuses writes once free space falls under `min_free_pct` (10) or
`min_free_bytes` (2 GiB), whichever is larger — but it only *learns* the free
space when it samples.

So the question is not "does the guard refuse", it is "how much can land
between two ticks". At 200 MB/s that is 400 MB per interval. On the instance
storage this product is sold on — the r7gd/i4i class, ~1.6 GB/s — it is **~3.2
GB per interval, larger than the entire 2 GiB default floor.** A single
interval can carry the volume from above the threshold to past it.

## Evidence, already gathered

Measured on the `phase1-drills` branch while building `disk_selffill_drill.sh`:
**three runs in five saw the first refusal at 7-10% free against a 10%
threshold.** ~100 MB landed inside one interval there; the mechanism scales
with write rate, and that rig was not on instance storage.

## The fix exists and is not on main

`ec73cd9` — "server: the disk guard sampled on a clock, so a fast filler
outran it" — makes the interval a function of headroom with a floor:

  - `MIN_SAMPLE` 50 ms — never sample faster than this;
  - never slower than the configured cadence;
  - `shed_floor_bytes()` computes the shed line so the interval can be sized
    against how close one sample is allowed to bring us to it.

`crates/flint-server/src/diskguard.rs` is 267 lines on main and 489 on the
branch.

## Why this is still open, which is the part worth reading

`ec73cd9` is on `origin/phase1-drills`, **19 commits ahead of main and 184
behind it**. Nothing on that branch has ever been merged. Four of its commits
were checked by subject against main and none are present:

  - `ec73cd9` server: the disk guard sampled on a clock (this bug)
  - `5554755` diskguard: a reactive control cannot bound its own first tick
  - `014d840` write_deadline: a positive control that fires on a spike is a coin toss
  - `c6f2a4c` m3_exit: carry the evidence out of the row-count mismatch (#174)

plus `66e02f1`, which adds the `FLINT_BG_JOBS` compaction-parallelism knob, and
`4656b30`, the ingest decay sweep tool. The ops `docs/testing-roadmap.md`
describes `FLINT_BG_JOBS` as "shipped (flint `66e02f1`)" — it is not in main,
and the whole 2026-08-17 compaction-parallelism measurement was taken against a
build that main cannot reproduce today.

So this is not one defect. It is a branch of finished, evidenced work that
never landed, and the cost of that is being paid in at least one live product
defect and one measurement that cannot be re-run.

## What landing it needs

Not a merge — 184 commits of drift make that a bad idea. Cherry-pick per
commit, each rebased onto current main and gated, in this order:

  1. `ec73cd9` + `5554755` — the two disk-guard commits, which are the live
     defect and belong together;
  2. `66e02f1` — the `FLINT_BG_JOBS` knob, so the compaction question is
     re-openable at all (opt-in, default-unchanged, so it ships nothing);
  3. `014d840`, `c6f2a4c` — drill correctness, no product change;
  4. `4656b30` + the three sweep fixes — tooling, lowest risk, last.

Each needs `tools/gates.sh` green in both feature configs. `diskguard.rs` has
diverged by 222 lines, so (1) is a real rebase and not a fast-forward.
