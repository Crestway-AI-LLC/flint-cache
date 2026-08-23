# BUG-0013: bulk writes stall because compaction is left at RocksDB defaults (OPEN)

Status: OPEN, found 2026-08-18 · Severity: medium-high — it does not corrupt
anything, it makes large ingests take multiples of the time they should, and
it silently shaped a published benchmark number

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
