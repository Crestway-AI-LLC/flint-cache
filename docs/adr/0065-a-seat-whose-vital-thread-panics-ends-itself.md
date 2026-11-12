# ADR-0065: A seat whose vital thread panics ends itself, rather than serve on without it

Status: **ACCEPTED 2026-11-02, option A** (Jeff: "go with your
recommendation"); **BUILT 2026-11-02** (As built, below). **Proposed
2026-11-02.**
From the error-handling review's cross-cutting sweep
(`docs/error-handling-review.md`, area N).

## Context

`flint-server` runs its periodic work on long-lived `std` threads, each spawned
once at start and never joined. A panic on one ends that thread. Nothing
restarts it, the process goes on serving, and nothing reports that the work
stopped.

| Thread | `main.rs` | What stops if it dies | What shows it today |
|---|---|---|---|
| Replication, on a replica | `replica::run` | The replica applies nothing more; it falls behind for good | `seq_lag` grows |
| WAL fsync | spawned under `--wal-fsync-ms` | The WAL is no longer synced on its cadence: a host loss loses every write since the last sync | `wal_fsync_total` stops moving |
| Disk guard | `diskguard` loop | No shedding and no reclaim as the volume fills; the disk fields go on reporting the last reading | Nothing: `disk_verdict` stays at its last value |
| Lease watchdog | self-fence on expiry | A master whose lease expires is never fenced to read-only, which is what ADR-0018's safety argument rests on | Nothing |
| Lease renewal | ADR-0018's renewer | Renewals stop; the watchdog fences the master at the TTL, and it stays fenced | `role` and read-only, at the next probe |
| GC | expiry and orphan sweep | Expired keys and orphan rows are no longer reclaimed | `gc_swept_*` stop moving, which they also do when there is nothing to sweep |

The others (the pair-agreement check, the engine-write sampler, automatic WAL
headroom) report on the seat rather than protect it.

**It has happened.** [BUG-0276](../bugs/0276-a-full-log-volume-killed-the-disk-guard-silently.md):
the disk guard panicked on a log line it could not write, and the seat went on
accepting writes at 5% free and reporting 99%. BUG-0276 removed that panic, not
the shape: the next panic on any of these threads does the same. A grep of
their bodies finds no panic site today, which is not a proof that none is
reachable. They decode replies from the control plane and the master, walk the
engine, and do arithmetic on clocks and counters.

The co-processor met the same question this month and answered it (ops
ADR-0017's D2 amendment, option B): a panic that leaves its state in doubt ends
the process, and the supervisor's restart rebuilds it.

## Options

**A. Fail-stop: a vital thread's panic ends the process.** The six threads
above are spawned through one helper, `spawn_vital(name, f)`. It runs `f` under
`catch_unwind` and, on a panic, logs `vital thread <name> panicked: <message>;
exiting`, writes a journal event, and calls `hard_exit` (RocksDB is open). The
seat is then a stopped seat, which every part of the fleet already handles: the
seat supervisor (`flintctl start`, once a minute) starts it again, and a
master's pair fails over by the lease, as for a crashed master. The three
reporting threads are spawned through `spawn_reporting(name, f)`, which logs a
panic and counts it in `FLINTINFO` as `thread_panics_total` without exiting.

**B. Restart in place.** `spawn_vital` runs `f` again after a panic, with a
backoff, and exits only after N panics in a window. A one-off panic then costs
no failover. A panic the input makes every time costs N restarts first. The
thread's state starts again from nothing on each restart, so each would need
reading for whether that is safe: the disk guard's last verdict, the fsync
thread's cadence, the watchdog's deadline.

**C. Count and alert.** Each thread beats a heartbeat that `FLINTINFO`
reports (`<thread>_last_ms`), and the agent alerts on one that is stale. The
seat keeps running without the work until a person acts. That is a page for a
condition the product could end itself.

## Decision: A (recommended, accepted 2026-11-02)

- **A stopped seat is a case the fleet already handles,** a running seat
  missing its fsync or its fence is not. The supervisor, failover and the
  rejoin are drilled; serving without a WAL sync or a lease fence is not a
  state anything checks for.
- **It is what the co-processor does** since ADR-0017's D2 amendment, so the
  two binaries answer the question the same way.
- **The cost:** a master whose vital thread panics fails over, so its pair
  is unavailable for writes for the failover time (`docs/slo.md`: budget
  10 s, measured p50 506 ms at `min-replicas-to-write=0`). A
  panic every time a thread starts would restart the seat every minute. That
  is visible to every check that already watches for a seat going down, and
  each panic is in the log with its location, for a bug report.

## Verification

- A unit test drives `spawn_vital` with a panicking body in a child process:
  the process exits with the fail-stop status, and the line names the thread.
- A test-only `FLINT_TEST_PANIC_THREAD=<name>` panics a named thread once it
  has started, so a drill can show it on a real seat. On a replica's
  replication thread, the seat exits and `flintctl start` brings it back
  caught up. On a master's WAL fsync thread, the seat exits, its pair fails
  over, and no acknowledged write is lost.
- `spawn_reporting`'s panic counts in `thread_panics_total`, and the seat goes
  on serving.

## As built (public `653744b3`, gate a65v PASS 181, 2026-11-02; ships in v0.1.0-rc.87)

- `spawn_vital(name, f)` starts replication (`replication`), the WAL fsync
  (`wal-fsync`), the disk guard (`disk-guard`), the lease watchdog
  (`lease-watchdog`) and renewal (`lease-renewal`), and GC (`gc`). Each runs
  under `catch_unwind` in a thread of that name. On a panic it logs `vital
  thread <name> panicked; exiting with status 70` and calls `hard_exit(70)`.
- `spawn_reporting(name, f)` starts the pair-agreement check
  (`pair-agreement`), the engine-write sampler (`engine-sampler`) and
  automatic WAL headroom (`wal-headroom`). A panic is logged and counted in
  `FLINTINFO`'s `thread_panics_total`, in both builds.
- `hard_exit` is in every build now: the lease and GC threads run in a `mem`
  build too.
- **Not journaled.** A new journal event kind would not decode on a control
  plane still on the previous release during a roll. The log line and the
  seat's exit, which every check that watches seats already sees, are the
  record.
- The test-only `FLINT_TEST_PANIC_THREAD=<name>:<ms>` panics a named thread
  that long after it starts. flintctl lists it among the names a seat reads.
- **Held by** `vital_thread_tests` (the fail-stop, in a child process, with
  its status and line; a reporting panic counted) in both builds, and the
  `vital_thread` drill: a master's `wal-fsync` panic exits 70 and its restart
  has all 50 acknowledged writes; a replica's `replication` panic exits 70
  and its restart catches up; an `engine-sampler` panic is counted while the
  seat serves, and the control seat counts none.
- **Not drilled here:** a master's pair failing over after its exit. A
  stopped master's failover is what `failover` and the chaos drills hold.


## Later: a WAL fsync thread whose syncs the engine refuses (BUG-0285, 2026-11-11)

The same rule covers a vital thread that is alive but cannot do its work.
After one WAL append or sync fails, RocksDB refuses every write until the
database is opened again, and the seat went on as master refusing them all.
The `wal-fsync` thread now also syncs after any refused write, and three
refused syncs in a row end the seat with status 70 and a line naming
BUG-0285, as a panic would. Held by the `engine_write_fault` drill.
