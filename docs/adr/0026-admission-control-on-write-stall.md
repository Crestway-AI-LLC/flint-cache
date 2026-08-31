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
- **Whether the same limit belongs on the replica.** It applies the same
  writes with the same engine, so it can stall too; it just never did here,
  because the master stalled first and gave it slack.
- **Asymmetric hardware.** Every number above is from identical instances
  with an otherwise-idle replica. A smaller replica, or one serving reads,
  could make ADR-0022's gate bind first after all — which would make this ADR
  a complement rather than a correction.
