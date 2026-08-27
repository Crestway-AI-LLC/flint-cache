# BUG-0013: bulk writes stall because compaction is left at RocksDB defaults (OPEN)

Status: OPEN, re-scoped 2026-08-24 (found 2026-08-18) · Severity: medium-high.
Two separable claims, and they now have different answers. The FILL half is
CONFIRMED twice: at RocksDB defaults a bulk load builds an unbounded
compaction backlog and crosses the slowdown trigger, which is what silently
shaped a published benchmark number. The REFILL half this file is NAMED for —
a rewrite over an existing tree costing multiples of the first write — has
failed to reproduce three times, most recently on a run that finished and
proved its writes landed. Nothing here corrupts data.

## Symptom

Filling 100 M x 1 KB keys into a **fresh** engine took ~38 minutes. Re-filling
the **same** keyspace over the resulting ~120 GB LSM, on the same hardware and
build, was still running **85 minutes later** when the test fleet's TTL
terminated it.

Steady-state writes are fine: beyond RAM, SET p50 moved only 0.351 -> 0.375 ms
(+7%) against a RAM-resident dataset. So this is not "writes are slow" — it is
bulk writes specifically.

## The wrong conclusion to draw

That the disk is the limit, or that beyond-RAM writes are inherently
expensive. The +7% steady-state number rules both out: the same engine, the
same disk and the same dataset size handle a sustained write rate without
trouble. What changes under a bulk load is that RocksDB stops accepting
writes as fast as they arrive.

## Root cause (hypothesised, NOT yet confirmed — see below)

`crates/flint-storage/src/rocks.rs` configures **nothing** about compaction.
The whole of it:

    opts.create_if_missing(true);
    opts.set_wal_ttl_seconds(...);  opts.set_wal_size_limit_mb(...);
    opts.set_compaction_filter("flint-meta-expiry", ...);
    opts.set_block_based_table_factory(&table_options());

No `max_background_jobs`, no `write_buffer_size` / `max_write_buffer_number`,
no `level0_slowdown_writes_trigger` / `level0_stop_writes_trigger`, no rate
limiter, no `soft`/`hard_pending_compaction_bytes_limit`.

So RocksDB's defaults apply, and the binding one is **`max_background_jobs =
2`** — two threads for every flush and compaction, on an 8 vCPU box with
NVMe. Under a bulk load L0 accumulates faster than two threads can drain it,
RocksDB applies its write stall, and the ingest rate collapses to whatever
compaction can sustain.

Durability is not what is waiting. A write is durable at the WAL, so the ack
never depends on compaction; it depends on RocksDB's **back-pressure**, which
exists to stop L0 growing without bound and destroying read latency.
Compaction speed is therefore a dial between write throughput and read
amplification — and it is currently set to a conservative default rather than
to the hardware.

## Confirm before tuning

`INFO` already exports **`write_stopped`** and **`delayed_write_rate`** and
nobody has ever read them. Step one is a bulk fill with both sampled, plus
`rocksdb.num-files-at-level0` and the pending-compaction property.

**If `write_stopped` is zero the hypothesis above is wrong** and the cause is
elsewhere — disk throughput, the WAL fsync cadence, or the proxy. Tuning an
LSM from reasoning instead of from its own stall counters is how a write
problem becomes a read problem.

**The counter is running — verified, not assumed.** BUG-0022 claimed this
criterion could never fail to acquit because statistics are disabled in
production. That was wrong: `rocksdb.is-write-stopped` is a DB *property*, not
a statistics *ticker*, and properties are live regardless. Measured on the
production open path, it reads `Ok(Some(0))` rather than `Ok(None)`.

Even so, **check `write_stall_readable:1` before believing any zero here.**
FLINTINFO now publishes it beside the two fields (BUG-0022's fix): 1 means the
pair was measured, 0 means the engine could not answer — the mem engine, or a
future build where these do become statistics-gated. A zero from an instrument
that cannot move is worth nothing, and this criterion's whole weight rests on
one.

## Measured 2026-08-19 — INCONCLUSIVE, and the criterion above cannot be applied as written

Ran the confirming measurement on the local SSD: 3.0 GB in 25k x 4KB batches
into a fresh engine, then a **refill of the same keyspace**, sampling FLINTINFO
every 200 ms throughout.

| phase | first 5 batches | last 5 batches | change | mean |
|---|---|---|---|---|
| fill | 62 929 ops/s | 39 457 ops/s | **-37.3%** | 49 920 |
| refill | 18 233 ops/s | 18 874 ops/s | **+3.5%** | 18 350 |

    write_stall_readable : 1 throughout   (the counters were measurable)
    max write_stopped    : 0
    max delayed_write_rate : 0

**The refill costs 2.7x the fill** — the direction the symptom describes — but it
is FLAT across its own 30 batches, and the engine never applied back-pressure.
At this scale the cost is compaction *work*, not compaction *stalling*. The
stall regime was not reached, so this neither confirms nor refutes the
hypothesis.

### The criterion as written silently acquits

> **If `write_stopped` is zero the hypothesis above is wrong**

That has no clause requiring the run to have ENTERED the regime. Applied to the
numbers above it reads "hypothesis wrong" — from a fill that never stressed
compaction hard enough to stall anything. A zero from an instrument that was
never exercised is not evidence in either direction. BUG-0022 predicted exactly
this ("its three-way criterion collapses to 'hypothesis dead' on every run")
and this is the concrete instance.

Score it three ways, not two: **CONFIRMED** (stall signalled), **FALSIFIED**
(throughput collapsed with no stall signal — cause is elsewhere), or
**INCONCLUSIVE** (throughput never collapsed, so the instrument was never
exercised). Only the middle one can kill the hypothesis.

`write_stall_readable: 1` is what makes even the inconclusive verdict
defensible — before BUG-0022's fix there was no way to tell a measured zero
from an absent one, and this run would have been unreportable.

### The wrong verdict this nearly published

The first automated verdict said **FALSIFIED**, and it was an artifact of the
harness, not a result. The script appended the refill rates to the fill's file
and then compared `rates[:5]` against `rates[-5:]` — so "first" was the fill's
opening and "last" was the refill's close, two different workloads. Of course
it read as a collapse. Within the refill there is none: +3.5%.

Had that shipped, it would have closed a live bug on a comparison across two
phases that were never comparable. The tell was that the refill's own first and
last five were both ~18k, which is only visible if the phases are scored
separately — the same failure as every check today that could not distinguish
two states, this time inside the instrument built to test the hypothesis.

### What would settle it

The original observation was ~120 GB of LSM and a refill still running after 85
minutes. This run is 3 GB — 2.5% of that. The next attempt should scale until
either `write_stopped` goes to 1 or throughput degrades WITHIN a single pass,
and should report the L0 file count so "did not stall" and "did not reach the
trigger" stay distinguishable. `rocksdb.num-files-at-level0` is a live DB
property (BUG-0022 established the property/ticker distinction), so it can be
read without enabling statistics.

## 2026-08-22 — the missing instrument now exists

The section above asks the next run to "report the L0 file count so 'did not
stall' and 'did not reach the trigger' stay distinguishable". It could not:
`rocksdb.num-files-at-level0` was never exported, so every run so far could
only say `write_stopped: 0` — the number that made the 3 GB measurement
unscoreable.

FLINTINFO now carries three more fields, on the same three-way contract the
stall pair uses:

    l0_files                  rocksdb.num-files-at-level0
    pending_compaction_bytes  rocksdb.estimate-pending-compaction-bytes
    compaction_readable       1 = the pair was measured, 0 = engine cannot answer

L0 file count is what the defaults are actually compared against —
`level0_slowdown_writes_trigger` 20 and `level0_stop_writes_trigger` 36 — so a
run can now report "reached 4 of 20" or "reached 19 of 20" where before both
read as `write_stopped: 0`.

**Verified in both directions rather than only the useful one**, because an
instrument nobody has seen move is worth what an absent one is worth:

- rocks engine, ~150 MB written in three batches: `l0_files` 0 -> 1 -> 2 as
  flushes landed. It moves.
- mem engine: `compaction_readable: 0`, not a fake zero. It admits when it
  cannot answer, which is the whole point of BUG-0022's distinction.

This does not confirm or refute the hypothesis — it makes the next attempt
scoreable. The verdict rules stand as written above: CONFIRMED, FALSIFIED, or
INCONCLUSIVE, and only the middle one kills it.

## Then

Raise `max_background_jobs` toward the core count, size the write buffers for
the box, and consider a rate limiter so compaction IO is smoothed rather than
starving foreground reads. Every value justified against a measured stall.

**Re-measure read latency afterwards, and treat that as part of the fix, not
a follow-up.** Turning back-pressure down spends read latency to buy write
throughput; the beyond-RAM GET numbers in
`docs/bench/2026-08-18-beyond-ram-current-build.md` (private repo) are what
must not regress.

Make the knobs configurable as `open_with_retention` already does for WAL
retention, so a co-located marketplace VM and a dedicated node can differ.

## Why it matters beyond ingest time

The last published write number, `51,191 ops/s` for a 32-minute pipelined
ingest (rc.6, July), was measured under exactly this stall. It has since been
removed from the public site, but it was a headline figure for a month. A
number produced by an untuned default is not a property of the product.

## 2026-08-22 — measured at 24 M keys: the SLOWDOWN trigger is reached; the refill half is still not scoreable

Two runs on a 16-vCPU gate box, 1 KB values, 120 x 200 000 keys per pass,
sampling `l0_files` and `pending_compaction_bytes` every 20 rounds.

### Run 1 was invalidated by its own corpus, and said so in its own output

Values were 1024 repeated `x`. `sst_bytes` came back **2.17 GB for 24 GB
logical** — an 11x compression ratio, sitting two lines under the logical size
in the same verdict block. Every number in that run was correct; it measured a
real engine doing real compaction, a tenth the size of the one it claimed.

The corpus the symptom was seen with is specified **incompressible** on
purpose, and that word is the whole reason. The drill now generates from
base64-of-urandom and **asserts the ratio**: past 2x it prints a warning
naming the discrepancy, so this cannot recur silently.

### Run 2, incompressible (ratio 0.8x — the LSM is larger than the data)

| | peak `l0_files` | `pending_compaction_bytes` | `write_stopped` |
|---|---|---|---|
| fresh fill | **22** | climbs monotonically to **55.4 GB** | 0 (readable=1) |
| refill | 19 | 42.6 GB falling to 32.2 GB | 0 (readable=1) |

**The slowdown trigger is reached by an ordinary fill.** `level0_slowdown_writes_trigger`
is 20 and L0 peaked at 22 during the FRESH fill of 24 M keys — so RocksDB was
throttling foreground writes on the shipped defaults, at a scale far below the
100 M that produced the original symptom. Pending compaction debt climbing
monotonically to 55 GB while L0 oscillates is the same picture from the other
side: flushes outrunning compaction, with the backlog never worked off.

`write_stopped: 0` with `stall_readable: 1` is a REAL zero — the hard stop at
36 was never reached. That is precisely the distinction the 2026-08-19 run
could not draw, and the reason the instrument was built.

### The refill half is NOT scoreable, and the fault is the instrument again

Rounds 60, 80 and 100 of the refill report **byte-identical**
`pending_compaction_bytes=38891523380` with `l0_files=6`, across 24 M writes.
Live metrics do not freeze like that. Either compaction had genuinely gone
quiet, or **the writes stopped landing** — and this run cannot tell which,
because the load was piped to `/dev/null` and no delivered-write count was
kept.

That is the same gap closed for BUG-0035 earlier the same day, in a drill that
now reports delivered throughput on every run precisely so a zero cannot be
read as a measurement. It was not carried into this experiment. The lesson did
not travel with the person who learned it.

So: the fresh-fill result stands, the refill comparison does not, and the
refill comparison is the one the original symptom is about.

### What settling it actually needs

An LSM near the original scale. This one is 30 GB of SSTs against the ~120 GB
that stalled, on a 60 GB volume that cannot hold more. The refill's cost is a
function of how much of the LSM a rewrite touches, so a 4x smaller tree is not
a smaller version of the same experiment — it may be below whatever threshold
produces the behaviour at all.

Next run needs: a larger volume, a delivered-write count per pass, and the
server's own log kept rather than dying with the box.

## 2026-08-23 — at 100 M keys, compaction debt grows without bound. Fill measured; refill still not.

A 400 GB box (`FLINT_GATE_VOL_GB`, added for this), 1 KB incompressible values,
500 x 200 000 keys per pass, data on the ROOT volume.

**PASS 1 ran to round 460 of 500 and the box then hit its TTL** — started
22:21:55, terminated 01:22:11, exactly the 180 minutes configured. Not a
crash; the time budget was wrong. Two 100 M-key passes need roughly six hours
including build and bootstrap, and were given three.

### What the fill established

| round | written | `pending_compaction_bytes` | `l0_files` |
|---|---|---|---|
| 120 | ~24 GB | 54 GB | 10 |
| 240 | ~48 GB | 84 GB | 4 |
| 360 | ~72 GB | 120 GB | 3 |
| 460 | ~92 GB | **168 GB** | 14 |

**Compaction debt grows monotonically to 1.8x the logical data written, with no
plateau.** `write_stopped` stayed 0 and `stall_readable` 1 throughout — a real
zero — and `l0_files` oscillated 2 to 18 without reaching the stop trigger of
36. So at the shipped defaults the engine never refuses a write and never
catches up; it absorbs an unbounded backlog instead.

That is the hypothesis stated at the top of this file, measured on an ordinary
fill, four times larger than the previous attempt could reach. It is also why
the earlier 24 M-key run saw debt reach 55 GB and read as a plateau: 55 GB was
simply where that fill stopped.

### What is STILL not measured, and it is the same half as before

The refill. It never ran. Every claim in this file about a rewrite over an
existing LSM being worse than a fresh fill remains untested at any scale that
matters — the 24 M-key attempt saw a *gentler* refill, and this run did not
reach one.

**The fill result does not stand in for it.** Unbounded debt on a fresh fill
says compaction is under-provisioned; it says nothing about whether rewriting
an existing 120 GB tree costs multiples of writing it once, which is the
original 38-minutes-versus-85-and-counting symptom.

### For the next attempt

Two passes at 100 M keys need ~6 h wall clock. Either budget that, or run 250
rounds (50 M keys, ~85 min per pass) and get the contrast at a ~60 GB LSM —
still 2.5x the largest tree this bug has ever been measured against, and it
fits comfortably in four hours.

## 2026-08-23 — the two halves separate: the FILL is confirmed, the REFILL does not reproduce

50 M keys per pass (250 x 200 000), 1 KB incompressible, 400 GB root volume,
data under `$HOME`. Disk precheck reported 391 GB available. The box hit its
TTL again at 270 minutes — PASS 1 complete, PASS 2 at round 220 of 250.

### PASS 1, the fresh fill — CONFIRMED, and fully delivered

    PASS 1 DELIVERED: 50 000 000 writes of 50 000 000 offered
    PASS 1 peak l0_files: 24   (slowdown trigger 20, stop trigger 36)
    pending_compaction_bytes: 18 GB @ round 40 -> 55 @ 120 -> 73 @ 200 -> 85 @ 240

Every offered write landed — no shedding, `write_stopped` 0 with
`stall_readable` 1 throughout. `l0_files` peaked at **24, above the slowdown
trigger of 20**, so RocksDB was throttling foreground writes. Debt reached
1.7x the logical data with no plateau, matching the 100 M run's 1.8x.

**That is the "defaults are under-provisioned" half of this bug, measured
twice at two scales, with full delivery proven rather than assumed.**

### PASS 2, the refill — the opposite of the hypothesis

    refill round  20: pending 44 GB, l0=4
    refill round  40: pending 3.3 GB, l0=0
    refill round  60: pending 162 MB, l0=2
    ...
    refill round 220: pending 0,      l0=3

Debt **collapsed** from the fill's 44 GB backlog to zero within 40 rounds and
stayed there, with `l0_files` at 0-4 against the fill's 2-24. Compaction did
not merely keep up during the rewrite — it drained a backlog it could not
drain while the tree was growing.

That is mechanically unsurprising in hindsight: a refill overwrites existing
keys, so the tree stops growing and compaction merges duplicate versions away.
The work per byte written is lower, not higher.

**The original symptom — 38 minutes to fill, 85+ and still running to refill —
does not reproduce here.** The refill ran 220 rounds in comparable wall clock
to the fill's 250.

### The gap in that conclusion, stated rather than buried

**The refill's delivered count never printed**: the TTL cut the run before its
summary. So "the refill is easy" rests on eleven pending/L0 samples trending to
zero, and NOT on proof that the refill's writes were landing. Debt falling to
zero is also what a refill that stopped writing would look like. `write_stopped`
stayed 0 and no `-QUOTA` appeared, which excludes server-side shedding but not
a client-side stall.

Closing that needs one run that finishes: 250 rounds at the measured **~1.9
rounds/min** is ~4.4 h of drilling plus ~25 min of build, so **TTL 330**. Both
attempts so far underestimated this — 180 for a 6 h job, then 270 for a 4.7 h
one — because the rate was estimated from throughput rather than measured from
rounds.

### What this does to the bug

It splits. The fill half is confirmed and actionable: raise
`max_background_jobs`, size write buffers, consider a rate limiter, every value
justified against these numbers. The refill half — the symptom this file is
named for — has now failed to reproduce at 24 M and at 50 M keys, and the
remaining explanations are the ones this run cannot test: the original was 100
M keys on i4i NVMe instance storage, not 50 M on EBS gp3, and it predates a
quarter's worth of WAL-retention and compaction-adjacent fixes.

## 2026-08-24 — the refill finished, its writes landed, and it still does not stall

The rerun the section above asked for, at the TTL it asked for: 330 minutes,
derived from the **measured** 1.9 rounds/min rather than estimated from
throughput. That estimate is what made the two previous attempts run out of
time. Both passes completed and both printed a delivered count — the one thing
every earlier attempt lacked.

50 M keys per pass (250 x 200 000), 1 KB incompressible values, 400 GB root
volume, data under `$HOME`, `c7i.4xlarge`.

| pass | peak `l0_files` / slowdown trigger | `pending_compaction_bytes` | delivered |
|---|---|---|---|
| fill   | **22** / 20 | climbs monotonically to 83 GB, no plateau | 50 000 000 of 50 000 000 |
| refill | **8** / 20  | oscillates 0-34 GB, repeatedly returns to 0 | 50 000 000 of 50 000 000 |

`write_stopped` 0 with `stall_readable` 1 for the whole run — a real zero from
an instrument known to be readable, not the absence of one. Final SST bytes
59.6 GB.

### What this closes

The previous section named its own gap: the refill's delivered count never
printed, so "the refill is easy" rested on eleven debt samples and not on
proof the writes were landing. **Debt falling to zero is also exactly what a
refill that stopped writing would look like** — the two are indistinguishable
from the pending-bytes trace alone.

`PASS 2 DELIVERED: 50 000 000 of 50 000 000` settles it. The writes landed.
The collapse is compaction draining a backlog it could not drain while the
tree was growing, not a client that stopped offering work.

### The two halves, both measured on one uninterrupted run

**The fill half is confirmed, for the second time.** Peak `l0_files` 22 against
a slowdown trigger of 20 (24 on the 2026-08-23 run), with debt growing
monotonically and never plateauing. At the shipped defaults the engine absorbs
an unbounded compaction backlog rather than refusing a write. Actionable, and
the numbers to justify a tuning change against are here and in the section
above.

**The refill half — the symptom this file is named for — has now failed to
reproduce three times**: at 24 M keys, at 50 M keys, and now at 50 M keys with
delivery proof. Peak `l0_files` 8 against a trigger of 20; it does not come
close. Mechanically unsurprising in hindsight: a refill overwrites existing
keys, so the tree stops growing and compaction merges duplicate versions away.
The work per byte written is lower during a rewrite, not higher.

### What remains, and it is not a measurement this file can take

The original symptom was 38 minutes to fill and 85+ still running to refill.
That was **100 M keys on i4i NVMe instance storage**, not 50 M on EBS gp3, and
it predates a quarter of WAL-retention and compaction-adjacent fixes. The
remaining candidate explanations are therefore the environment and the elapsed
fixes — neither of which a larger run on this hardware would distinguish.

Reproducing it would mean the original shape: 100 M keys on i4i instance
storage, ~6 h of wall clock, and ideally at a commit near the original
sighting. Until someone wants that, the honest state is: **a real defect was
found and confirmed on the fill side, and the defect this file was opened for
is unreproduced on current code.**

## 2026-08-24 — before tuning anything, read this: the obvious remedy is already measured

The confirmed fill half points straight at `max_background_jobs`. **That was
swept on 2026-08-17 and the result inverts the intuition.** On an i4i.2xlarge
with the seat pinned to two cores, five sweeps of ~760 MB of incompressible
10 KB values into an 8 MB level base:

| jobs | mean MB/s (n=5) | CompMergeCPU(s) |
|---|---|---|
| default | 221.9 | 3.85 |
| 2 (explicit) | 215.1 | 3.83 |
| 4 | 198.1 | 5.26 |
| 6 | 199.1 | 4.90 |

Pooled: **218.5 -> 198.6 MB/s, -9.1%**, against a 3.1% error bar. Compaction
CPU is the cleaner signal — **3.84 s -> 5.08 s, +32%, with no overlap.** More
parallel compaction spends more CPU merging and bills it to the cores the write
path needs.

**That second column was labelled W-Amp until 2026-08-26**; see the correction
below. It is compaction merge CPU, which happens to support the same
conclusion — the mechanism here was always contention for cores, and CPU
seconds are if anything the more direct evidence for it than amplification
would have been. The conclusion is unchanged; the label was wrong.

**Both results stand and they do not conflict.** That sweep was a 2-core seat
with an 8 MB level base; this bug is a 16-vCPU box building a 60 GB LSM. On a
small seat extra compaction threads steal the cores the write path needs; on a
big box with a large LSM, two jobs cannot keep up. The knob is a function of
cores and LSM size, and **neither measurement licenses a fleet-wide default on
its own.**

### ~~The instrument is not in main~~ — LANDED 2026-08-26

`FLINT_BG_JOBS`, added by flint `66e02f1` as the opt-in knob for exactly this
question, is on `origin/phase1-drills` and **was never merged**. The ops
testing-roadmap calls it "shipped"; it is not. So the 2026-08-17 numbers were
taken against a build main cannot reproduce, and there is currently no way to
re-open the question without landing that commit first. See BUG-0044, which
covers the branch and a live disk-guard defect stranded on it.

### So the next step here is not a tuning change

It is: land `66e02f1`, then sweep the knob in THIS regime — a large LSM on a
box with cores to spare — and only then argue about a default. Sizing write
buffers and adding a rate limiter are also untested; the 2026-08-17 work swept
background jobs alone.

**2026-08-26 — the first half is done.** `FLINT_BG_JOBS` is on main
(`flint-storage/src/rocks.rs`), landed by BUG-0044's cherry-pick of the
stranded branch, and `tools/ingest_decay_sweep.sh` came with it. The section
above saying the instrument is absent is kept for the record and is no longer
true.

The sweep still could not express this regime, because it hardcoded
`FLINT_LEVEL_BASE_MB=8` — the 2026-08-17 shape — so running it answered the
question that already had an answer. `DECAY_LEVEL_BASE_MB` and
`DECAY_WRITE_BUFFER_MB` now override it, defaulted to the old values so prior
numbers stay comparable, and **the shape is printed with the results**: two
sweeps of this file have already appeared to disagree when they were measuring
different regimes, and only one of them recorded which.

So what remains is the measurement itself: a large LSM on a box with cores to
spare, one sweep of the job count, with the shape recorded beside every column.

## 2026-08-26 — MEASURED AT PRODUCTION SCALE: 96 GB, and the ratio holds

The 6.4 GB result came with an explicit caveat that production is ~96 GB and
that a small-dataset finding can invert — this file's own history is two sweeps
that disagreed because their shapes did. So: the same 2x2, 15x the data, on a
2-core pinned seat.

| level base | bg_jobs | mean MB/s | first | last | decay | stall | phys | W-Amp |
|---|---|---|---|---|---|---|---|---|
| 8 MB | default (2) | 34.2 | 68.5 | 22.9 | 67% | 83.7% | 136 GB | 16.0 |
| 8 MB | 4 | 65.9 | 97.2 | 47.5 | 51% | 49.0% | 157 GB | pending |
| 64 MB | default | 45.2 | 126.1 | 26.8 | 79% | 80.9% | 104 GB | 11.3 |
| **64 MB** | **4** | **89.2** | 179.7 | **60.0** | 67% | **43.4%** | 185 GB | **10.2** |

**Error bar 0.6%**, measured the same way as before — `default` against an
explicit `2`, which is the same engine by two routes. Every gap above is two
orders of magnitude clear of it.

**The recommendation survives the scale-up.** Best against baseline is **2.6x**
at 96 GB and was 2.7x at 6.4 GB. Both knobs are still required together, and
`bg_jobs`'s own contribution GREW with depth: +93% at the 8 MB base here
against +32% at 6.4 GB, which is the direction this file predicted and the
opposite of what the 800 MB run showed.

### Three things the small run understated, and they matter for a default

- **Absolute throughput is far lower.** 89.2 MB/s where 6.4 GB read 224.3. The
  tuning lifts the ceiling by the same ratio; the ceiling is much lower.
- **Stalling does not go away.** 2.0% at 6.4 GB, **43.4%** at 96 GB in the best
  configuration. "Fixed" was the wrong word for what the small run showed;
  "less bad" is the right one. The seat still spends nearly half its time
  waiting on compaction.
- **The fastest configuration has the LARGEST footprint** — 185 GB resident for
  96 GB logical, against 104 GB for 64 MB/default. On a 436 GB i4i.large that
  is ~80 GB of extra resident bytes bought with the throughput, and it is a
  real cost to price rather than an accounting artefact.

### What true amplification says, now that it is measured

The W-Amp column here is the CORRECTED metric (see the correction below; every
earlier figure in this file was compaction CPU). Sampled from the engine's own
stats before each configuration wiped its data directory:

- 8 MB base, default: **16.0**
- 64 MB base, default: **11.3**
- 64 MB base, jobs=4: **10.2**

So the winning configuration runs 2.6x faster at **36% lower write
amplification** than the baseline. That is the claim withdrawn earlier the same
day for resting on a broken metric. It was true; it was not knowable from the
evidence then offered for it.

It also resolves the footprint result rather than contradicting it: the winner
writes LESS in total (10.2 vs 16.0) while leaving MORE resident (185 GB vs
136 GB). Less rewriting, more deferred merging. Only the correct metric
separates those.

### A correction to the 6.4 GB guidance about which columns to trust

That run found `last` varying 16% between identical configurations and
concluded: compare means, `last` and `decay` are the noisy ones. **At 96 GB
that is wrong.** Two runs of the identical configuration, two hours apart:

| | run 1 | run 2 | spread |
|---|---|---|---|
| mean | 34.2 | 34.1 | 0.3% |
| last | 22.9 | 23.0 | **0.4%** |
| decay | 67% | 67% | 0 |
| stall | 83.7% | 83.8% | 0.1% |
| phys | 139548 MB | 139641 MB | 0.07% |

The noisiness was a small-dataset artefact. `last` is one interval's rate, and
at 6.4 GB an interval is ~4 seconds of sample; at 96 GB it is ~300. The rule to
keep is not "distrust `last`" but **"an interval too short to average anything
carries one sample's variance"** — which is the same lesson as the 0.25-second
intervals that invalidated the first attempt, arriving from the other side.

### What this still does not license

- **A defaults change.** A 32 MB write buffer is a per-engine memory
  commitment nobody has priced, and 185 GB resident on a 436 GB seat is a
  capacity decision, not a tuning one. What the numbers support is a proposal.
- **Anything about the refill half of this bug**, which remains untouched.
- **Anything about a fleet.** One seat, one namespace, no replication, no
  concurrent read load.

## 2026-08-26 (later) — CORRECTION: every "W-Amp" in this file is compaction CPU

**`ingest_decay_sweep.sh` was reading the wrong column, and had been for
months.** It took field 17 of RocksDB's `Sum` row. RocksDB prints the Size
column as a value and a unit separated by a SPACE — `5.25 GB` — so awk sees two
fields where the header has one name, and every column at or past Size is
offset by one. Field 17 is **`CompMergeCPU(sec)`**. The real W-Amp is field 13.

So every amplification figure in this file, and in the 2026-08-17 sweep it
cites, is **compaction merge CPU seconds**. The tables above are relabelled;
the numbers themselves are unchanged and still real, they simply measure
something else.

**Why it survived.** The values looked right. 5.0 at 800 MB and 61 at 6.4 GB
are plausible write amplifications AND plausible CPU times, and the two scale
together because both grow with how much compaction ran. It even survived a
deliberate check: 4.99 here against 5.08 from an independent run a week
earlier was cited as evidence the extraction was sound. It was sound — stable,
repeatable, and measuring a quantity nobody had asked for.

Worse, the wrong number got an explanation. This file said W-Amp "grows with
how much compaction actually ran", which is a true sentence about CompMergeCPU
and a strange one about amplification. **A misread number that gets
rationalised is harder to find than one that looks absurd**, because the
rationalisation removes the surprise that would have exposed it.

What exposed it was a 96 GB run reporting **1822**. As amplification that is
175 TB written in 45 minutes, about 65 GB/s — impossible on the hardware, and
therefore impossible to explain away. The instrument only became falsifiable
at a scale where its error stopped being plausible.

**Fixed by locating the column from the HEADER on every read**, not by index,
so a RocksDB version that adds or reorders columns cannot silently repeat this.
Controlled against the real `Sum` row: the new extractor returns 9.1 where the
old one returned 59.60.

**What this does and does not change.** Mean, first, last, decay, stall and
phys were never affected — they come from the drill's own timing and from
`ns_bytes`, not from that table — so the 2.7x throughput finding and the
stall collapse stand. What is now unmeasured is true write amplification at
every scale, including the claim that the winning configuration achieved its
throughput at *lower amplification*. That claim is withdrawn pending a re-run;
what the data supports is lower compaction CPU, which is related and not the
same.

## 2026-08-26 — measured. Both knobs, together, and the first attempt measured nothing

The measurement this file has been asking for since 2026-08-17. Seat pinned to
2 cores on an i4i.2xlarge, 6.4 GB logical (640 k x 10 kB), 8 intervals, shape
recorded beside every column.

| level base | bg_jobs | mean MB/s | last MB/s | decay | CompMergeCPU(s) | stall |
|---|---|---|---|---|---|---|
| 8 MB  | default (2) | 82.4 | 72.3 | 59% | 61.4 | 58.3% |
| 8 MB  | 4 | 108.4 | 92.3 | 49% | 70.6 | 31.1% |
| 64 MB | default (2) | 145.5 | 83.7 | 76% | 38.3 | 38.3% |
| **64 MB** | **4** | **224.3** | **176.0** | 50% | 43.4 | **2.0%** |

**2.7x the mean ingest rate, stalls from 58% to 2%, at LOWER compaction CPU
than the baseline (43.4 s vs 61.4 s).** Physical bytes are ~12.3 GB
in all four runs, so this is not throughput bought with disk.

**Both knobs are needed and they interact.** The hypothesis at the top of this
file names `max_background_jobs = 2` as the binding default, and that is
confirmed — but it is not the whole answer. Raising jobs alone (8 MB row 2)
reaches 108 MB/s and makes compaction CPU WORSE. Raising the level base alone (64 MB
row 1) reaches 145 MB/s and still stalls 38% of the time. Only together do
stalls collapse. The file's own note that `write_buffer_size` was untested was
the more important half.

### The error bar, and which columns can carry a conclusion

Measured rather than assumed, by running `default` against an explicit `2` —
the same engine by two routes, since `rocks.rs` only calls
`set_max_background_jobs` when `FLINT_BG_JOBS` is set, so unset falls through
to RocksDB's own default of 2. **Noise floor: 8.1%.** Every gap claimed above
is 4x that or more; the 2.7x headline is about 20x it.

Two runs of the IDENTICAL config, taken 20 minutes apart, also give the
cross-run spread per column — and it is not uniform:

| metric | run 1 | run 2 | spread |
|---|---|---|---|
| mean | 82.4 | 81.8 | **0.7%** |
| last | 72.3 | 60.4 | **16%** |
| decay | 59% | 67% | **8 points** |

**Compare means. `last` and `decay` are single-interval quantities and are the
noisy ones** — which inverts the intuition that the end of the curve is the
honest part. It also means the decay column, which gives this file its shape,
is the least reliable number in it: an 8-point run-to-run spread makes any
decay comparison under ~16 points unusable on its own.

### The first attempt produced a large, clean, entirely fake win

Run first at the sweep's default 800 MB, the 64 MB shape reported throughput
doubled, W-Amp down a third, stalls 8.7% -> 0.6% and decay 51% -> 20%. All of
it was an artefact: 95 MB per interval against a 64 MB level base and a 32 MB
write buffer never leaves L0, so there was no deepening LSM and nothing to
decay. At the real size the same shape decays WORSE (76% vs 59%), which is the
opposite sign.

The existing guard could not catch it — it asserts the baseline decayed at
least 15%, and this decayed 20%. "The curve is shallow" and "there is no curve"
are indistinguishable from the decay figure alone. `ingest_decay_sweep.sh` now
refuses any shape where the dataset is under 50x the level base, as a preflight
before an instance is spent. Full write-up in the ops field notes, section 1.

### What this does NOT establish

- **Production is ~96 GB, not 6.4 GB.** 15x more data on the same 2-core seat
  means more depth than measured here. The direction should hold; the magnitude
  is unverified.
- **This is not a defaults decision.** A 32 MB write buffer is a per-engine
  memory commitment, and this measured one seat size at one dataset size. What
  the numbers license is a proposal, not a merge.
- The refill half of this bug is untouched by any of it.

### Next

Propose defaults with the memory cost priced, and re-measure at a production
LSM size before changing anything shipped. Note the delivery channel is
`flintctl node-env`, which reached bootstrap and nothing else until 2026-08-26
— it would have dropped exactly these variables on the first `upgrade`.
