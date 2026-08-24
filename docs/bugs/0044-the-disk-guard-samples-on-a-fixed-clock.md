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

## 2026-08-24 — batch 1 landed; the branch is deeper than this file first said

**The live defect is fixed on main.** `ec73cd9` and `5554755` cherry-picked
onto current main (as `f4aaad6`, `de92e2f`), plus the knobs and the drill that
test them (`9b468c6`). Full gate green, 120 steps, both feature configs;
`disk_selffill` passes on Linux in 18 s and on macOS locally, so both the
`mkfs.ext4`/loop and `hdiutil` paths are covered. The guard now refuses at 15%
against a 20% threshold where the fixed cadence let it reach 7-10%.

`disk_selffill` joining CORE immediately tripped
`assert_no_cross_drill_kill_patterns`: the new drill declares 6458, and
`controller_ha`'s cleanup swept `--port 645`, which had been self-contained
until that moment. Scoped to `fleet_kill server`. The check caught a
collision as it was introduced rather than as an intermittent failure later,
which is the entire reason it exists.

Staged behind it and gating: `66e02f1` (`FLINT_BG_JOBS`), `c6f2a4c` (m3_exit
evidence), and `4656b30` + its three fixes (`tools/ingest_decay_sweep.sh`),
which is what makes BUG-0013's compaction question re-runnable at all.

### One commit deliberately NOT taken

`014d840` — "write_deadline: a positive control that fires on a spike is a
coin toss". Main solved the same problem independently and better: it ramps
the load 32 -> 64 -> 128 -> 256 until the control actually arms, and fails
with "the positive control COULD NOT BE ARMED on this machine", distinguishing
a failure to create the condition from evidence about the shed. The branch
version predicts the load's cost instead. Two designs for one assertion is
worse than either; main's stands, and it already cites docs/bugs/0030.

### The audit this file should have opened with

"19 commits ahead" undersold it. Ten commits remain stranded, and several
touch product code, not drills:

| commit | subject |
|---|---|
| `643d12e` | controller: a convergence gate written as an equality never fires |
| `28a0aa8` | server: FLINTSYNC must prove the WAL can reach the cursor before promoting |
| `775911c` | server: a node that cannot serve yet must be visible, not dark |
| `e150289` | wal: make the retention window configurable, and drill the replica |
| `d932c7c` | gates: a blind replace disabled the checks; restore them |
| `9f3b868` | drills: write-path and capacity regimes (PARTIALLY taken — the rocks.rs knobs and `disk_selffill` landed; `ingest_saturation_drill.sh` and the `replica_starvation` rewrite did not) |
| `542da86` | coproc drills: derive the durable-row count instead of asserting a constant |
| `a46228c` | drills: match the process, not a mention of its name |
| `247fefc` | drill: say what the starvation arms measure |
| `f6b4780` | docs: min-replicas-to-write is the operator's to set |

**That list is a starting point, not a verdict.** It was built by matching
commit subjects against main's history, which is a weak test: main may well
have fixed the same defect with different wording, and at least two of these
describe failure classes main has demonstrably addressed since — `a46228c` is
the unanchored-match family, which main has now fixed in several places, and
`643d12e`'s equality-gate shape has a field-notes entry of its own dated
2026-08-16. **Each remaining commit needs its defect checked against main's
CODE before anyone concludes it is missing.**

`643d12e` in particular is a 249-line controller change plus a new
`loaded_promote_drill.sh`. A weak check suggests main still carries the
pre-fix shape, but that is not established, and a change of that size to the
promotion path deserves its own session and its own gate rather than being
carried along on the momentum of a disk-guard fix.

### Audit results so far — the list above was overstated, as warned

Four of the ten checked against main's CODE rather than its commit subjects:

| commit | verdict |
|---|---|
| `643d12e` controller convergence equality | **CONFIRMED LIVE on main** (`flint-controller/src/main.rs:635`), and the gate it feeds is at :800. Filed as **BUG-0045**, severity HIGH — a pair under write load can refuse to promote a healthy replica and stay write-dead. |
| `28a0aa8` FLINTSYNC WAL reachability | **ALREADY FIXED on main** (`flint-server/src/main.rs:3859` carries the exact `updates_since_budgeted(cursor, 1)` guard). Not missing. Do not re-land. |
| `775911c` a syncing node must not be dark | **NOT ESTABLISHED.** `loading:` and `wait_for_ready` are absent from main, but main opens its listener BEFORE it starts tailing, so a node is not dark in the ordinary case. Whether the FULL-SYNC path differs is unchecked. |
| `e150289` configurable WAL retention | **PARTIALLY SUPERSEDED.** The env knobs are absent, but main threads `wal_ttl_seconds`/`wal_size_limit_mb` as parameters through `open_with_wal`, so the window IS configurable by a different route. Only the env-var spelling is missing. |

One confirmed live defect, one already fixed, one unestablished, one
superseded — from four commits whose subjects all read as missing fixes. That
ratio is the argument for checking code over subjects, and it is why the six
unchecked entries above remain claims rather than findings.
