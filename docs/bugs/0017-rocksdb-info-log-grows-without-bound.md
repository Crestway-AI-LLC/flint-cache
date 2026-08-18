# BUG-0017: the RocksDB info LOG grows without bound, and three subsystems mistake it for data (OPEN)

Status: OPEN, found 2026-08-18 on the playground · Severity: **high** — it is a
disk-exhaustion path that scales with replication churn rather than with stored
data, and it silently corrupts every measurement taken with `du`

## Symptom

Playground, one pair, rc.51 → rc.52. The master's data directory:

    813 MB  LOG.old.1787029335642650
     71 MB  LOG.old.1787073684621581
    248 KB  *.sst          <- the actual data
     26 MB  archive/       <- retained WAL
     28 KB  *.log          <- live WAL

**883 MB of diagnostic log against 248 KB of data — about 3,600:1.**

Sampling 200,000 lines of the big file, one message is 92% of it:

    184,900   [db/wal_manager.cc] Latest Archived log: N
      1,323   [checkpoint_impl.cc] Hard Linking N.sst

## Root cause

`crates/flint-storage/src/rocks.rs` sets **neither `keep_log_file_num` nor
`max_log_file_size`** — six `opts.set_` calls in total, none about logging. So
RocksDB's defaults stand: the info LOG grows without limit and rotated LOGs are
never pruned.

This is the same gap as BUG-0013, which found compaction left at defaults in the
same file. Two findings from one omission argues the fix is an audit of what
`rocks.rs` should PIN, not a third one-off.

## Why it is not housekeeping

**1. It scales with churn, not with data.** The dominant line is emitted by the
WAL archive manager. The harder a node churns WAL archive, the faster it fills
its disk — and churning WAL archive is exactly what BUG-0012's livelock does.
So a replication failure converts itself into a disk failure. The file spans
`2026/08/11-21:06:56` to a rotation at ~05:02Z on 2026-08-18, the week that
ended in nine hours with no master; steady state after that measured
**~6 MB/hour**, unbounded.

**2. The disk guard sheds writes on it.** The guard samples the data directory
for free space. On a small node it will refuse tenant traffic because of debug
logging, with no relationship to what the tenant stored.

**3. The capacity model and the meter count it.** `capacity 467771486208` and
`FLINTNSBYTES` are read against the same directory. On this box the dataset
reads as 918 MB when it is 248 KB — a 3,600x overstatement that would flow
straight into a bill.

## Fix

- Pin `max_log_file_size` and `keep_log_file_num` to something bounded and
  small in `rocks.rs`. A drill that writes enough to force a rotation and
  asserts old logs are pruned; without the assert this regresses silently,
  because nothing else in the system notices a large file.
- **Separately, and more important: stop measuring datasets with `du`.** The
  guard, the capacity model and the meter should count SST + live WAL, not
  directory size. That is a correctness fix independent of log retention — with
  the logs bounded, a large `archive/` or a stray checkpoint would still
  mislead all three.

## Also worth deciding

Whether the info LOG belongs in the data directory at all. `set_db_log_dir`
would move it out, which fixes the guard and the meter by construction rather
than by remembering to exclude it — at the cost of one more path to manage.

## Reproduced / cleared

Confirmed on the playground and cleared by moving the rotated logs to
`/var/lib/flint/diag/` and gzipping: **883 MB reclaimed from the data dirs, kept
as 88 MB compressed** (they are the only record of the livelock week). Data dirs
went 918 MB → 43 MB and 9.3 MB.

## Related

- BUG-0013 — compaction left at RocksDB defaults, same file, same omission
- BUG-0012 — the livelock whose churn writes this log fastest
- BUG-0016 — where I mistook this log for the dataset and filed a wrong bug
