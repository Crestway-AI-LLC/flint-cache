# BUG-0017: the RocksDB info LOG grows without bound (FIXED)

Status: **FIXED, tested, and CLOSED 2026-09-05** · found 2026-08-18 on the
playground · Severity: **medium** — an unbounded disk consumer that scales with
replication churn rather than with stored data. **Scope corrected 2026-08-18**
— see "Three claims withdrawn"; the last stated gap was checked by mutation on
2026-09-05 and did not exist — see the closing section.

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
`max_log_file_size`**. `open_with_retention` (:207) makes six `opts.set_` calls
and none concerns logging, so RocksDB's defaults stand: the info LOG grows
without limit and rotated LOGs are never pruned. `open_read_only` (:183) passes
a bare `Options::default()` for the same reason.

Those two are the only production open sites; everything from :388 is
`#[cfg(test)]`.

This is the same gap as BUG-0013, which found compaction left at defaults in the
same file. Two findings from one omission argues the fix is deciding what
`rocks.rs` should PIN, not a third one-off.

## Why it is worth fixing rather than sweeping

**It scales with churn, not with data.** The dominant line is emitted by the WAL
archive manager, so the harder a node churns WAL archive the faster it fills its
disk — and churning WAL archive is exactly what BUG-0012's livelock does. A
replication failure thereby converts itself into a disk-capacity failure. The
file spans `2026/08/11-21:06:56` to a rotation at ~05:02Z on 2026-08-18, the week
that ended in nine hours with no master; steady state after that measured
**~6 MB/hour**, unbounded.

That is the whole of the defect: a diagnostic file can consume a volume the data
does not need. It is resource exhaustion, not corrupted measurement.

## Three claims withdrawn

The first version of this bug asserted that three subsystems mistake the log for
data. **All three were wrong**, and none was checked before filing:

1. **"The disk guard sheds writes on it."** `crates/flint-server/src/diskguard.rs`
   thresholds on FREE SPACE — `min_free_pct: 10`, `min_free_bytes: 2 GiB`,
   `u.free_pct()` over `flint_storage::disk::Usage` — not on directory size.
   Free space is the correct sensor for ENOSPC: if any file fills the volume the
   guard *should* shed. The guard was working as designed.
2. **"The meter counts it."** `RocksKv::ns_bytes` (`rocks.rs:84`) sums
   `db.get_approximate_sizes()` over the namespace's `MSZ` CF ranges — RocksDB's
   own SST estimate for those key ranges. It never reads directory size, so
   there is no 3,600x billing overstatement.
3. **"The capacity model counts it."** `capacity 467771486208` is a DECLARED
   inventory value: `"capacity" => inv.capacity_bytes`
   (`flint-ctl/src/main.rs:276`) forwarded as `--node-capacity-bytes` (:2811).
   Nothing measures anything.

The withdrawn fix that followed from them — *"stop measuring datasets with `du`,
and more important than log retention"* — has **no target in the product**. A
grep of `crates/` finds no `du`, no `walkdir`, and no directory-size summation on
any metering path. `du` was the tool I measured with by hand; the product never
used it. The 3,600:1 ratio is real, but it inflates a log file, not a meter.

Filed an hour after BUG-0016 was retracted for the same mistake — asserting a
blast radius without reading the consumers — which is why the fix below is
deliberately narrower than the one first proposed.

## Update 2026-08-22 — the fix had shipped; the assert had not

`bound_info_log` pins exactly what this bug proposed — `max_log_file_size`
64 MiB and `keep_log_file_num` 5, a ~320 MB ceiling — and it is applied at BOTH
production open sites (`open_read_only` and `open_with_retention`). Only the
status line was stale. Third bug today whose remedy was already in the tree.

What was missing is this bug's own second requirement, and its reasoning is
exactly right: *"Without the assert this regresses silently, because nothing
else in the system notices a large file."* No disk guard, meter or alert reads
directory size — that was established here by withdrawing three claims that
said otherwise.

`rocks::info_log_bounds` now covers it. Writing it took three attempts and each
failure is worth more than the test:

1. Churn writes to force a rotation — produced `rotated = 1` however hard it
   worked, so `rotated <= keep` passed while pruning had never once run.
2. Six times the churn — still 1. `LOG.old` is created **per DB open**, not by
   size, so no amount of writing could make a second one.
3. Restructured to six opens — still 1, and this time the assert FAILED, which
   is what taught the semantics: **`keep_log_file_num` bounds all info logs
   INCLUDING the live one.** Six opens at keep=2 leaves `LOG` + one `LOG.old`.

The assert is therefore on the TOTAL count, which carries both halves: six opens
with no pruning would leave six files, so exactly `keep` proves pruning ran and
that enough rotations happened for it to matter. Fewer than `keep` now fails as
"the opens did not rotate and this exercised nothing", rather than passing.

A second test pins the ceiling itself, because 64 MiB x 5 is a decision with an
incident behind it — roughly two days at the measured ~6 MB/hour, deliberately
more than one because that outage ran nine hours before anyone looked.

**What is still not covered, stated rather than implied:** the tests drive
`bound_info_log_with`, not the call sites. Deleting `bound_info_log(&mut opts)`
from `open_with_retention` would leave both green.

> **Withdrawn 2026-09-05.** That paragraph is wrong, and the way it is wrong is
> this bug's own recurring mistake — a claim about consumers made without
> reading them. See the closing section: the deletion it describes fails a test
> that already existed when the paragraph was written.

## Fix

- Pin `max_log_file_size` and `keep_log_file_num` in `open_with_retention`, and
  give `open_read_only` the same options rather than a bare default. Proposed
  **64 MiB x 5 = a 320 MB ceiling**, which at the measured ~6 MB/hour is roughly
  two days of history — deliberately more than one, because the incident that
  motivated this ran nine hours before anyone looked.
- A drill that writes enough to force a rotation and asserts old logs are
  pruned. Without the assert this regresses silently, because nothing else in
  the system notices a large file.

Explicitly NOT changing:

- **The disk guard and the meter.** Both are already correct; editing them would
  be a regression driven by the withdrawn claims.
- **The log level.** Dropping INFO to WARN would cut 92% of the volume, but the
  compaction and flush records are what BUG-0013 and #196 are diagnosed from.
  Bound the size, keep the fidelity.
- **`set_db_log_dir`.** Moving the log off the data dir was floated as fixing
  the guard "by construction". With claims 1-3 withdrawn there is nothing to fix
  by construction, and it would make things worse: on the guarded volume an
  unbounded log is at least SEEN by the free-space check. Off it, it grows
  invisibly until the root fills.

## Reproduced / cleared

Confirmed on the playground and cleared by moving the rotated logs to
`/var/lib/flint/diag/` and gzipping: **883 MB reclaimed from the data dirs, kept
as 88 MB compressed** (they are the only record of the livelock week). Data dirs
went 918 MB → 43 MB and 9.3 MB.

## Closing 2026-09-05 — the last gap was checked, and it was not there

The bug's remaining scope was one sentence: *"the tests drive
`bound_info_log_with`, not the call sites. Deleting `bound_info_log(&mut opts)`
from `open_with_retention` would leave both green."* It was checked by making
the deletion rather than by reading, once per call site.

**`open_with_retention` is covered.** Deleting the line fails
`info_log_is_pruned_across_reopens`:

    info LOG is not pruned: 9 files after 9 opens (bound 5)

That test goes through `RocksKv::open`, which is `open_with_retention` with the
default retention, and it was added in `0f065b8` — **the same commit as the
fix, on 2026-08-18**. The "not covered" paragraph was written on 2026-08-22,
four days later, in the same file, about a sibling test twenty lines away. It
is the mistake BUG-0016 was retracted for and this bug was filed an hour later
to avoid: asserting a blast radius without reading the consumers. Three weeks
of "known gap" that a one-line deletion would have closed on day one.

**`open_read_only` is uncovered, and cannot be covered, because the bound is
inert there.** Deleting the line leaves all 188 flint-storage tests green — so
on the evidence of the suite alone it looks exactly like the gap above. It is
not, and the difference is measurable:

| probe | with the bound | without it |
|---|---|---|
| 9 read-only opens of a fresh DB | 1 `LOG`, no `LOG.old` | 1 `LOG`, no `LOG.old` |
| 1 read-only open over 10 staged rotated logs | 10 files | 10 files |

A read-only open neither rotates the info LOG nor prunes existing ones. There
is no behaviour at that call site to assert, so a test written against it would
pass whether or not the bound is set — the vacuous assert this file already
records three attempts at. The line stays: it is free, it is correct, and it is
right if RocksDB changes. `a_read_only_open_neither_rotates_nor_prunes_the_info_log`
pins the assumption that makes it inert, and says in its failure message that
failing is the good case.

**One real defect fell out of the check.** `info_log_is_pruned_across_reopens`
asserted `logs.len() <= DEFAULT_KEEP_LOG_FILE_NUM + 1`, commented "live LOG plus
at most KEEP retained" — implying a ceiling of 6 x 64 MiB = 384 MB, twenty lines
from `the_ceiling_is_the_one_the_incident_justified`, which asserts
`DEFAULT_MAX_LOG_FILE_SIZE * DEFAULT_KEEP_LOG_FILE_NUM` = 320 MB. The two could
not both be right. Measured at 1/5/6/9/20 opens, the file counts are 1/5/5/5/5:
`keep_log_file_num` bounds ALL info logs including the live one, exactly as
`rotated_logs_are_pruned_to_the_keep_limit` had worked out and documented at
length. The ceiling test was right, the assertion has been tightened to
equality, and the two tests now agree about what the bound means.

## Related

- BUG-0013 — compaction left at RocksDB defaults, same file, same omission
- BUG-0012 — the livelock whose churn writes this log fastest
- BUG-0016 — where I mistook this log for the dataset and filed a wrong bug
