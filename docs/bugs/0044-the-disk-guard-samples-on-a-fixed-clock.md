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
| `775911c` a syncing node must not be dark | **CONFIRMED LIVE, and worse than the commit claims.** The full-sync path DOES differ: `flint-server/src/main.rs:1105` blocks before the bind at :1870. Its consequence is BUG-0046 — a managed controller wipes and restarts the transfer every ~20 s, so a replica that cannot sync inside that window never syncs at all. |
| `e150289` configurable WAL retention | **PARTIALLY SUPERSEDED.** The env knobs are absent, but main threads `wal_ttl_seconds`/`wal_size_limit_mb` as parameters through `open_with_wal`, so the window IS configurable by a different route. Only the env-var spelling is missing. |

One confirmed live defect, one already fixed, one unestablished, one
superseded — from four commits whose subjects all read as missing fixes. That
ratio is the argument for checking code over subjects, and it is why the six
unchecked entries above remain claims rather than findings.

### 2026-08-24 — audit complete: all ten checked against main's CODE

The six remaining entries, checked the same way as the first four. The
subject-matching list was overstated in both directions: it named commits whose
defects main had already fixed, and it undersold one whose consequence is worse
than the commit claims.

| commit | verdict |
|---|---|
| `a46228c` match the process, not a mention of its name | **ONE THIRD LIVE.** Both seat-count fixes are superseded and improved on — main uses `fleet_pids`, scoped to the drill's own fleet rather than merely anchored, so it survives a parallel drills stage where the branch's `pgrep` would not. The third change never landed; see Finding 1. |
| `d932c7c` a blind replace disabled the checks | **PARTLY LIVE.** The `df` splice is absent from main (one `df -h`, correct) and `tenant_remove_drill.sh`'s spliced `fleet_warm` is fixed — `assert_no_continuation_splice` landed under a different name. But `assert_drill_builds_keep_rocks` carried no positive control, and `assert_every_drill_accounted_for` did not exist; see Finding 2. |
| `f6b4780` min-replicas-to-write is the operator's to set | **SUPERSEDED, one stale comment.** `docs/failover.md:72-82` states the trade more fully than the branch, including the freshly-promoted-master case, and `flint-ctl` never passes the flag unless an operator sets `min-replicas`. Only the code comment still recommended 1 on replicated pairs. Fixed. |
| `542da86` derive the durable-row count | **NOT LIVE, still fragile.** Main fixed the breakage by correcting the constant 8 to 15 with the breakdown beside it. The branch DERIVES the total from the ids the drill writes, so a layout change names which part moved. Main's number is right today and goes stale the next time the durable layout changes. Open, low priority. |
| `9f3b868` write-path and capacity regimes | **PARTLY TAKEN, as recorded.** `rocks.rs` knobs and `disk_selffill` landed. `ingest_saturation_drill.sh` and `replica_starvation_drill.sh` are absent from main entirely. A coverage gap, not a live defect. Open. |
| `247fefc` say what the starvation arms measure | **NOT APPLICABLE** — edits a file main does not have. Its content is worth keeping regardless: arm B's "accepts everything" is the RAW SERVER's bound, not a deployed fleet's, because flintctl defaults `--widowed-grace-ms` to 10 s and the drill passes no such flag. Reading it as a shipped exposure is the error #197 corrected. |

Tally across all ten: two confirmed live and now fixed (`643d12e` as BUG-0045,
`775911c` as BUG-0046 plus the `-LOADING` fix), one already fixed on main
before the audit started, one superseded by a different route, one partly live
and now fixed, one not applicable, and three open as coverage or fragility
rather than defects. **One in ten of the "missing fixes" was missing in the way
the subject line implied.** That ratio is the argument for reading code.

#### Finding 1 — rewind_rejoin accused the product of losing writes. FIXED.

`tools/rewind_rejoin_drill.sh` was byte-identical to the branch's pre-fix
state. `kill` signals the load subshell and `wait` returns when that subshell
dies; neither stops the `valkey-cli` it already had in flight. That child is
reparented and its `SET` lands on B after the tip is sampled, so A converges to
the target it was given, B holds one key more, and the drill reports

    keyspaces diverge after the loaded rejoin (772 vs 773) — the attach
    replayed or skipped writes

about a rejoin that did nothing wrong. Every observed failure was off by
exactly one — a straggler's signature, not a replayed span.

**The cost is not a flaky drill. It is a flaky drill that accuses the
replication path of losing acknowledged data**, so it gets investigated through
the attach, the cursor translation and the ack accounting, none of which is
where it lives. Now polls until the tip stops moving. An earlier `BTIP` read
taken while the load was still running, and overwritten before use, is gone
too: a dead assignment that samples a moving quantity reads like a measurement
and invites someone to trust it.

#### Finding 2 — two drills were registered nowhere. FIXED.

Of 114 `tools/*_drill.sh`, 109 were in CORE or CHAOS and three more named in
the exclusions block. `coproc_family` and `proxy_chain` were in none of the
three, referenced only in comments, executed by nothing.

The block they slipped past opens with "An absence with no reason beside it is
indistinguishable from an oversight." It was written to prevent this and could
not, because nothing checked the list was complete.

**The second-order cost was already being paid.** `proxy_chain` reserves
6460-6467, and both `loaded_promote` and `loading_visible` were moved off that
block earlier the same day to avoid colliding with it — during a port-collision
investigation that had no way to know the claimant was dead.

`EXCLUDED` is now a variable and both drills are in it marked UNVERIFIED, which
is the honest status: neither has run since ADR-0010 and whether they pass is
unknown. Running them was attempted and refused — `fleet_guard` found a sibling
project's fleet on the box, correctly. **They still need a run**, after which
they either join CORE or get deleted and free the ports.
`assert_every_drill_accounted_for` closes both directions and positive-controls
its own matcher, because the earlier attempt at this check reported sixteen
registered drills as unlisted, one per line of CORE.

#### Still open from this audit

- ~~`542da86`'s derived row count~~ **CLOSED 2026-08-25.** Both drills now count
  durable keys BY KIND, so no total is predicted and no hash is reimplemented.
  Details below; the specification that preceded it is kept for the reasoning.
  **Do not take the branch's fix as written.** It derives the expected total by
  reimplementing FNV-1a and `INDEX_BUCKETS` in Python, inside the drill. That
  swaps a constant that goes stale for a second implementation of the hash that
  can silently drift from `flint_vec::bucket_of` — two implementations of one
  invariant, which is the thing this repo keeps paying for elsewhere (the
  chaos harness's `port_free` beside flintctl's `wait_port_free` was the same
  shape, fixed at 26d981a by sharing the reasoning and not the code).

  **Assert the STRUCTURE instead, which needs no hash at all.** Durable keys
  are `KEY_PREFIX + kind + NUL + set [+ NUL + id]` (`flint-vec/src/lib.rs`
  `durable_key`), so each kind is countable directly:

  - exactly 1 of kind `s` (set-name index, one per namespace)
  - exactly 1 of kind `c` per set — 2 here
  - exactly 1 of kind `v` per (set, id) — 6 here
  - kind `i`: at least 1 and at most `len(ids)` per set, since distinct
    buckets cannot exceed distinct ids and a collision only reduces the count
  - `DBSIZE == 1 + 2 + 6 + i_count`

  That last equality is what the bare total was really guarding — "no keys of
  an unexpected kind" — and it is the only part that needs to know a total.
  The bucket count becomes an observation rather than a prediction, so a
  layout change names which kind moved instead of printing two numbers, and
  nothing has to know how ids hash.

  Attempted on 2026-08-25 and deferred, not abandoned: getting the `SCAN`
  parsing right needs local iteration (`KEYS` is not implemented, so the dump
  goes through `SCAN`), and the box was contended by a sibling project's
  stress run — `fleet_guard` refused, correctly. Same treatment applies to
  `coproc_vec_tls_drill.sh`, which hardcodes 8 in the same shape.
- `ingest_saturation` and `replica_starvation` — real coverage main lacks.
- ~~`coproc_family` and `proxy_chain` — run them, then register or delete.~~
  **CLOSED 2026-08-24: both PASS, both now in CORE.** Neither was broken; each
  had simply never been run. `coproc_family` passes end to end (flintctl spawns
  the declared co-processor, the proxy routes it over mesh mTLS, vectors persist
  into the tenant namespace, `stop` reaps it). `proxy_chain` passes 16 chains x
  25000 hops with the oracle clean.

  One thing to carry forward about `proxy_chain`: **its fault injection armed
  by a thin margin** — 1 of 6 kills landed mid-walk, the other five skipped
  because the walkers had already finished. That is not a silent hole; the
  binary asserts `overlapped > 0` and fails with an instruction to raise
  `--elements` or `--chains`. Note the direction, because it is the opposite of
  the usual one: the 16-vCPU gate box is the WORST case for arming, since a
  fast walk outruns the kill schedule. A contended CI runner walks slower and
  should land more. If this drill ever fails to arm, the machine got faster or
  quieter, and the fix is the one the message names — not a timeout.

### 2026-08-25 — the durable-row check, closed by counting instead of predicting

Both `coproc_vec` and `coproc_vec_tls` asserted a single durable-key total (15
and 8). They now count by kind and assert the shape:

    s  exactly 1      set-name index, one per namespace
    c  one per set    2 and 1 respectively
    v  one per (set, id)
    i  BOUNDED by the ids, not predicted from them — a hash collision may only
       ever reduce this count, so the assertion is a range
    dbsize == s + c + v + i

That last equality is the only thing a total was ever really guarding: "no key
of a kind nobody expected". Everything else is now an observation.

**The branch's fix was not taken, and the reason generalises.** It re-derived
the total by reimplementing FNV-1a and `INDEX_BUCKETS` in Python inside the
drill. That trades a constant that goes stale for a second copy of the hash
that can drift from `flint_vec::bucket_of` in silence — and a drifting
duplicate is strictly worse than a stale constant, because the constant fails
with a number you can read while the duplicate fails with two numbers and no
indication which is authoritative. Counting kinds needs no hash at all.

Ground truth was read from the running drill rather than derived: keys render
as `\x00vec\x00<kind>\x00<set>[\x00<id>]`, and the live corpus is
`s=1 c=2 v=6 i=6`, dbsize 15. Negative-controlled by asserting 3 configs where
there are 2: the drill exits 1 and prints the full count line plus a key dump,
so the next layout change names which kind moved instead of printing two
totals.
