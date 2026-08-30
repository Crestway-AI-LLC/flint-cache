# BUG-0079 — the WAL shed guard fires at twice the distance the archive keeps (OPEN)

**Found** 2026-08-30 by a 2 TB ingest run, and reproduced exactly by a second
one. Both stalled at the same place: **163–165 GB**, ~15 minutes into a
sustained 150–200 MB/s fill of 1 KB values on a two-member pair.

## What happens

The replica dies, and its own log says why:

```
FATAL: WALGAP full sync required: IO error: No such file or directory:
  while stat a file for size: /var/lib/flint/node-7001/archive/002256.log
  — this link can never resume. Marking for re-seed and exiting
```

The master deleted a WAL archive segment the replication link still needed.
The replica marks itself for re-seed and exits — by design (#106). Nothing
restarts it (BUG-0077), the master goes widowed, and after
`--widowed-grace-ms` it sheds every write:

```
pair 0  master  live_replicas 0    writes_shed_widowed: 28,864,516
pair 0  DOWN
d0..d63: THROTTLED no live replica for longer than --widowed-grace-ms
```

The load then makes no progress and the run fails. **Availability, not
durability**: nothing was lost or corrupted, the pair simply stopped
accepting writes.

## The cause is a unit mismatch between two constants

Retention and the guard that protects it are denominated differently, and
nothing reconciles them:

| constant | value | unit |
|---|---|---|
| `DEFAULT_WAL_SIZE_LIMIT_MB` | 8 GiB | **bytes** |
| `DEFAULT_WAL_TTL_SECONDS` | 6 h | **time** |
| `DEFAULT_WAL_HEADROOM_SHED_SEQ` | 4,000,000 | **sequences** |

RocksDB deletes when the byte limit OR the TTL trips, whichever comes first.
The guard sheds writes when the master outruns its slowest live replica by
4,000,000 sequences. Measured at the moment of failure:

```
163 GB over 37,667,156 sequences = 4,327 bytes per sequence
8 GiB archive therefore holds     ~1,985,000 sequences
guard fires at                     4,000,000 sequences  =  2.0x too late
```

**The guard cannot protect the archive at this value size.** Deletion happens
at half the distance where shedding begins, so the replica is cut off while
the guard still reports full headroom — `writes_shed_headroom: 0` and
`writes_shed_lag: 0` for the entire run.

The general form: the guard is only safe while a sequence averages under
**8 GiB / 4,000,000 = 2,147 bytes**. Above that the archive horizon sits
inside the guard, and the margin inverts silently. Ours averaged 4,327 —
1 KB values batched roughly four to a sequence.

The design intent is stated in `rocks.rs` and is exactly right; it is the
calibration that fails:

> Generous on purpose — the point is that ADR-0022's shed gate, not this
> window, is what a lagging replica hits first. A window this size makes
> shedding rare; the shedding is what makes the window safe.

The same file records this shape happening once already: *"raising only the
TTL would have left the 1 GiB limit doing the pruning — which is the term
that actually fired in the incident."* The limit went 1 GiB → 8 GiB. At
200 MB/s that bought about forty seconds.

## What it is not

Each eliminated by measurement, not argument:

- **Not OOM.** 55 GB free on the replica, 123 GB total; no `dmesg` OOM lines.
- **Not disk.** 90 GB used of 3.5 TB on the replica, 286 GB of 3.5 TB on the
  master.
- **Not the lag or headroom gate misbehaving.** Both were at 0 all run —
  which is the bug, not an exoneration.
- **Not a stale serving iterator.** `seq_lag` derives from `effective_acked`,
  the FRESHEST replica's ack; with one replica that is this replica, so the
  metric tracked it honestly. A sample nine minutes before the death showed
  `seq_lag: 2,278`, and the missing segment was ~3,500 files old — the
  replica genuinely fell behind inside that window and crossed the archive
  horizon before the guard's threshold.

## Fix options

1. **Derive the shed threshold from the archive budget instead of a constant.**
   The guard should fire on the same quantity RocksDB prunes on. Either
   denominate it in bytes, or compute the sequence equivalent continuously
   from observed bytes-per-sequence — the value is already measurable
   (`latest_seq` against on-disk growth). This makes the calibration
   self-correcting for any value size, which a constant cannot be.
2. **Pin retention to the slowest live replica.** `repl_hub` already has the
   cursor and the doc says outright that this is "the one WAL retention has to
   respect", including why it must be LIVE replicas only ("a dead seat would
   hold the WAL open forever and turn a replication problem into a disk
   problem"). RocksDB's own TTL/size pruning does not consult it.
3. **Raise `DEFAULT_WAL_SIZE_LIMIT_MB`.** A mitigation, not a fix: its
   adequacy depends on ingest rate and value size, which is the property that
   just failed. It would move the cliff, not remove it.

(1) and (2) are complementary — (2) is correct retention, (1) keeps the write
path from ever needing it. (3) alone repeats the 1 GiB → 8 GiB step that led
here.

## Not covered

- **Whether reads or smaller values reach it.** Everything here is 1 KB
  writes at 150–200 MB/s. The crossover is a function of bytes-per-sequence,
  so a 256-byte workload would sit well inside the guard and never show this.
- **What the right archive budget is.** Any fixed number has the same problem
  in a different place.
- **Whether the supervisor alone would have carried the run.** With
  BUG-0077's fix armed the replica would have restarted and re-seeded within a
  minute; at 163 GB that re-seed is minutes of transfer during which the pair
  is single-copy, and whether the fill survives that is unmeasured.
