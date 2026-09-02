# ADR-0026 — Admission control keyed on the master's own write stall, not on replica lag

**Status:** proposed
**Date:** 2026-08-30
**Amends:** [ADR-0022](0022-wal-retention-bounded-by-replica-progress.md)

## Context

ADR-0022 placed back-pressure on replica progress: the master sheds once it
outruns its slowest live replica, so a replica meets back-pressure before it
meets a deleted WAL segment. That is correct as a *durability* bound and
should stay. What it is not, and what we assumed it was, is the thing that
keeps the write path from degrading.

A five-host fleet was brought up on 2026-08-30 to find where WAL replication
tops out, so ingest could be capped below it. There is no such ceiling at this
scale. Across three windows, 60 pipelined connections driving a pair master
directly with 1 KB values:

| window | shed gate | master accepted | replica applied | ratio | soft-delayed |
|---|---|---|---|---|---|
| A | 32,768 | 74,830 seq/s | 75,041 seq/s | 1.0028 | 0 |
| B | 8,388,608 | 45,966 seq/s | 45,967 seq/s | 1.0000 | 4,728/s |
| C | 8,388,608 | 43,672 seq/s | 43,770 seq/s | 1.0023 | 4,309/s |

The replica ended exactly level (`last_applied` = master `latest_seq` =
45,958,716), and in 58 separate intervals it *drained* a backlog at up to
**4.2×** the master's concurrent rate. Peak lag all day was ~85,000 sequences
against a gate of 8,388,608 — three orders of magnitude of slack.

So a gate sized to the WAL archive, which is what ADR-0022 asks for and what
we built, is correct and **inert**. It cannot bind before something else does.

## What actually degrades

RocksDB's own L0 write stall. In windows B and C, with nothing shedding:
`l0_files` sawtooths 8 → 21, all 60 connections sit pinned in flight,
`writes_delayed_soft` runs at ~4,300–4,700/s, and goodput falls to ~44,000
seq/s. Neither side is CPU-bound at the stall (master 92% idle, drivers 99.7%
idle, two-sample `top`) — during a stall both are *waiting*.

Window A is the accidental control. Its gate was wrong — 32,768, from a 1 GiB
archive the master should never have chosen (BUG-0079's boot race) — and it
shed 125,000 writes/s. That shedding was doing admission control, and the
stream it admitted ran with **`writes_delayed_soft` at zero for the entire
window** and **71% more durable throughput** than the "correct" setting.

The existing deadline shed is the wrong instrument for this. It is keyed on
`write_deadline_ms` (2 s), so it fires long after the collapse: over the run
it shed 17,533 writes while 3,044,426 were absorbed as latency. 99.4% of the
back-pressure arrived as delay, not as a signal a client could act on.

## Decision

Add an admission gate on the master keyed on its **own proximity to a write
stall**, shedding with the existing retryable `-THROTTLED` before RocksDB's
internal delay engages. Signals are already exported in `FLINTINFO`:
`l0_files`, `pending_compaction_bytes`, `delayed_write_rate`,
`write_stopped`.

The point is to convert a latency collapse into a backpressure signal. A
client that receives `-THROTTLED` can retry, shed, or slow down; a client
whose connection is pinned inside a stalled engine can do none of those, and
the engine does less total work while it waits.

ADR-0022's replica-lag gate stays exactly as it is, doing the job it is
actually good at — bounding retention against a replica that has genuinely
fallen behind. It simply stops being the mechanism we expect to limit
throughput.

## Alternatives considered

- **Do nothing; let RocksDB stall.** This is today's behaviour and it costs
  41% of goodput at saturation, plus unbounded per-connection latency. The
  measurement above is the argument against it.
- **Retune RocksDB's `level0_slowdown_writes_trigger` / `stop_writes`.**
  Moves where the stall happens without changing that it is expressed as
  delay rather than as a signal. Worth doing as well, not instead.
- **Rely on the deadline shed.** Already present, already firing, and
  measured at 0.6% of the pressure. It is a last resort, not a governor.
- **Size the replica-lag gate tighter so it binds first.** This is what window
  A did by accident, and it is the wrong reason for the right behaviour: it
  couples the write path's throughput to an archive-retention constant, so
  any change to disk size or value size silently retunes admission. That
  coupling is exactly what BUG-0079 was filed about.

## Open questions

- **The control law.** Window A's 62% shed rate was accidental, not optimal.
  A proportional controller holding `l0_files` just under the slowdown
  trigger is the obvious shape, but the gain and the measurement interval are
  unmeasured, and an oscillating admission gate would be worse than none.

  **AMENDED 2026-09-02: ask this of the LAG CAP first, because that is the
  gate that binds.** This ADR's own amendment demoted the stall gate to a
  backstop "for where replica lag binds". A duration-bounded soak has now
  measured which gate that is. One pair on `i4i.large`, 4 feeders at 48 MB/s
  aggregate against the ~22 MB/s a 2-vCPU seat sustains:

  | counter | after one cycle |
  |---|---|
  | `writes_shed_lag` | **134,091** |
  | `writes_shed_deadline` | 0 |
  | `writes_shed_headroom` | 0 |
  | `writes_delayed_soft` | **443** |

  So the control law that governs real back-pressure today is the lag cap's,
  and it is not a controller at all. It is bang-bang with a constant:

      lag <  soft (500ms)   -> nothing
      soft <= lag < hard    -> sleep(2ms) per write, counted
      lag >= hard (1000ms)  -> shed -THROTTLED

  **The soft band did about 0.3% of the work** — 443 delays against 134,091
  rejections, roughly 300:1. Whatever the soft band is for, at this overload it
  is not what stopped the writes.

  Two candidate causes, and they call for different fixes, so the next
  measurement should separate them rather than assume one:

  1. **The band is too narrow in TIME.** With ~48 MB/s offered against ~22 MB/s
     applied, the backlog grows at roughly 1.2 seconds of lag per second, so
     the 500ms between soft and hard is crossed in well under a second. A band
     the controller barely occupies cannot damp anything. *(Arithmetic from
     measured rates, not itself measured — shown so it can be checked.)*
  2. **The action is too weak, and concurrency-blind.** A fixed 2ms sleep per
     write is a constant, not a function of error. It does not scale with how
     far past `soft` the lag is, nor with how many writers are in flight, so
     its damping effect falls as connection count rises — the opposite of what
     is wanted, since more writers is what produces the overload.

  The proportional shape proposed above for `l0_files` applies at least as well
  here, with the same two unknowns — gain and measurement interval — and the
  same hazard, that an oscillating gate is worse than none. The difference is
  that here there is now a measured operating point to design against instead
  of an accidental 62%.

  **Also settled by the same run:** back-pressure comes from offered RATE, not
  from failure events. 12 cross-host kills over 130,152 writes shed **4**;
  sustained ingestion above replication capacity shed **134,091**. Four orders
  of magnitude apart. A control law tuned on kill-driven shedding would be
  tuned on the wrong signal entirely.

  Caveat, because it bounds what may be designed on this: the 300:1 ratio is
  **one cycle**, not a replicated measurement. The primary claim it sits beside
  — that lag binds and the other gates do not — is categorical and survives a
  single sample; a RATIO is a quantity and does not. Replicate before choosing
  a gain.
- **Whether the same limit belongs on the replica.** It applies the same
  writes with the same engine, so it can stall too; it just never did here,
  because the master stalled first and gave it slack.
- **Asymmetric hardware.** Every number above is from identical instances
  with an otherwise-idle replica. A smaller replica, or one serving reads,
  could make ADR-0022's gate bind first after all — which would make this ADR
  a complement rather than a correction.

## Amendment, 2026-08-30 (same day) — tuning does part of this job, without rejecting anything

A connection sweep and a compaction-parallelism A/B on one fleet changed the
shape of the argument above, and one number in it.

**The operating point is a knee the system will not find on its own.**
Throughput scales linearly with connections to ~12, peaks at ~24–48
(~80–88k seq/s, ~84–93 MB/s at 1,054 B/write), and then goes *backwards* —
46,318 seq/s at 384 connections. A client that offers more load past the knee
gets less work done. That is an argument for admission control that does not
depend on any of the reasoning below it: nothing in the write path steers
toward the maximum, and the client cannot see it.

**But compaction parallelism removes the collapse, without shedding a write.**
The shipped engine runs one `rocksdb:low` thread (RocksDB's default
`max_background_jobs: 2`, `max_subcompactions: 1`). With
`FLINT_BG_JOBS=8 FLINT_SUBCOMPACTIONS=4`:

| conns | 1 thread | 6 threads | ratio |
|---|---|---|---|
| 24 | 77,234 | 76,263 | 0.99 |
| 96 | 78,233 | 79,062 | 1.01 |
| 192 | 37,303 / 23,569 | 75,760 | **2.0–3.2×** |
| 384 | 20,079 | 61,607 | **3.1×** |

Nothing at the operating point; 2–3× past it. The ceiling is unmoved — so
this is not speed, it is overload behaviour. The mechanism is in a counter,
not a rate: at 384 connections the shipped config shed **44,554,807** writes
on the 2-second deadline, the tuned one **347,048**. With a single compaction
thread, overload writes queue behind a stalled engine until they time out.

### What this changes

**The admission gate is a backstop, not the primary defence.** Against L0
collapse, config gets most of it for free and rejects nothing, which is
strictly better than shedding. The gate's remaining job is the region config
cannot reach — where **replica lag** binds. That limit was invisible until
compaction stopped being the constraint, and then appeared immediately:
`writes_shed_lag` at 13,520,819 (C=192) and 38,201,926 (C=384).

So the ordering is: tune compaction for ingest-class hardware first, then let
ADR-0022's lag cap do what it was designed for, and reserve a stall-keyed gate
for what neither covers. That is a narrower mandate than this ADR opened with.

### A correction to the evidence above

The Context section leans on window A — a mis-set gate shedding 125,000
writes/s while delivering 71% more durable throughput than the "correct"
setting. That comparison stands, but the reason is now clearer and less
flattering to it: both windows ran at 60 connections, **past the knee**.
Window A's shedding held the system near its peak by accident; window B's
did not, and sat in the degraded region. The finding is real and the
mechanism is admission control, but it was never evidence that 62% shed is a
good target — it is evidence that *something* has to hold the system at its
knee.

### Still not established

- **What caps ~80k seq/s.** Not compaction (6 threads move it 1%), not
  connection count, not replication below C=192. Unmeasured: the fsync path
  (`wal_fsync_ms` 500), device write bandwidth, WAL append serialization.
- **Whether these knobs should ship as defaults.** Every number here comes
  from a 16-vCPU box with 15 idle cores — the case `rocks.rs` warns makes this
  knob look free while saying nothing about a 2-vCPU seat, where compaction
  threads contend with the serve path. `FLINT_SUBCOMPACTIONS` exists so the
  question can be asked on constrained hardware; it does not answer it.
