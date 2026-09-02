# BUG-0079 — the WAL shed guard fires at twice the distance the archive keeps (budget FIXED and now OBSERVABLE 2026-09-02; mount ordering still open)

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

## Update 2026-08-30 — the fix was silently inert on half the fleet

Deriving the budget from the volume (the fix for (1) and (3) above) landed and
was verified green, and then a measurement fleet showed it doing nothing on
the node that mattered. Two seats, same AMI, same binary, same 3.5 TB NVMe,
one pair:

| | `wal_headroom_shed_seq` | implied archive |
|---|---|---|
| master `172.31.77.205:7001` | 32,768 | **1 GiB** — the FLOOR |
| replica `172.31.69.191:7002` | 8,388,608 | 256 GiB — as intended |

Each seat says which it chose, in one line nothing reads:

```
wal headroom shed threshold: 32768 sequences (half of a 1024 MB archive ...)
wal headroom shed threshold: 8388608 sequences (half of a 262144 MB archive ...)
```

**It is a boot race, not a constant error.** The budget is derived from
`disk::sample(data_dir)` before the engine has created that directory, so
`statvfs` answers `ENOENT` and the `unwrap_or(0)` behind it turns "I could not
measure" into "a zero-byte volume", which clamps to the smallest archive
allowed. Whether a seat wins the race is decided in under a second:

| | statedir created | seat started | outcome |
|---|---|---|---|
| master | 23:08:54.617 | 23:08:54 | lost — 1 GiB |
| replica | 23:08:55.758 | 23:08:55 | won — 256 GiB |

So a pair can come up with a 256× under-provisioned archive on one member, on
a healthy disk, differently on each boot. This is exactly the starvation
BUG-0079 was filed to remove, reintroduced by its own fix.

`disk.rs` already states the rule that was broken, and has a test asserting
it (`a_path_that_does_not_exist_is_unknown_not_full`): *"Callers must treat
that as 'unknown', never as 'full' — a syscall failing is not evidence about
disk space."* A budget is the one caller where believing a failed measurement
costs a replica its archive.

### What was changed

1. **Create the data directory before measuring it.** The engine creates it
   moments later regardless; doing it first is what makes the measurement
   answer for the volume the data will actually live on, and removes the race
   rather than narrowing it.
2. **`disk::sample_nearest`** walks to the first ancestor that exists, as a
   fallback for when the create itself fails. Explicitly *not* sufficient
   alone — with the instance store not yet mounted the nearest ancestor is the
   small root volume, which lands on the same floor just as quietly.
3. **An unmeasurable volume falls back to `DEFAULT_WAL_SIZE_LIMIT_MB` and
   says so**, instead of silently becoming the floor.
4. The override warning could never fire for `--wal-size-limit-mb`: it
   compared the chosen value against a `derived_mb` re-read *after* the chosen
   value had been stored, so the two were equal by construction. It now
   compares against what the node would have chosen on its own.

### Update 2026-09-02 — the budget is now visible, and a check can see a floor

The item below is closed. `FLINTINFO` gained two fields:

- `wal_archive_mb` — the budget actually in force
- `wal_archive_src` — **how it was chosen**: `measured`, `override`,
  `unmeasured`, or `none`

The second field is what makes the first checkable. `1024` is the correct
answer on a 4 GB dev box and a 256x under-provisioning on an NVMe, and an
operator who pinned it is not a defect at all — so the number alone can carry
no assertion. Provenance separates the three.

**The invariant, and why one process is enough.** When the source is
`measured`, the budget must be the one the volume implies:

    wal_archive_mb == clamp((disk_total_bytes / 4) / MiB, 1024, 262144)

`disk_total_bytes` is already in `FLINTINFO`, sampled by the disk guard from
**the same `dir`** the budget was derived from. In the failure those two
disagree, on one seat, with no fleet required: the bad measurement happens once
at boot, while the guard goes on sampling that directory successfully for the
life of the process. `wal_archive_mb` remembers the failure and
`disk_total_bytes` reports the success, so the seat's own INFO contradicts
itself and anything reading it can say so. That also answers the "no check
asserts the two members of a pair agree" half more cheaply than a pair
comparison would: each member is now answerable on its own, and a fleet check
comparing them is a one-line read of a field that exists.

`tools/wal_budget_drill.sh` asserts it, and is in CORE.

**Both fields are rocks-only**, because `flintinfo` itself is
`#[cfg(feature = "rocks")]` and the mem engine has no WAL archive to describe.
The drill's mem seat therefore checks `wal_archive_src:none` against a build
that HAS the fields — a mem-engine seat in a rocks binary — which is the case
an operator can actually meet. Worth stating because the first version of this
change left the static and its constants ungated: they compiled, the rocks
config was green, and `clippy (mem)` failed on the gate box with `static
WAL_BUDGET_SRC is never used`. Local verification had run one feature config;
the rule is both, and the gate is where that got enforced.

**Verified by mutation, both directions:**

| mutation | result |
|---|---|
| the derived sample's value replaced by `0` — the boot race exactly, a failed measurement believed | FAILS: `disk_total_bytes=999995129856 -> expected 238417 MB, seat reports 1024 MB`, and names 1024 as the FLOOR |
| both fields deleted from `FLINTINFO` | FAILS at the capability assert — `absent is not clean` — rather than comparing two empty strings and reporting PASS |

The second row is the one that had to exist. `field()` prints an empty string
both when a value is empty and when the key is absent, so without a
presence check first, deleting the fields turns every later comparison into
empty-vs-empty and the drill goes green against a build that surfaces nothing.

Three controls sit alongside: a pinned seat must report `override` and is
**not** held to the volume (the drill refuses if the pin happens to equal the
derived value, since the control would then prove nothing); a mem seat must
report `none` rather than the compiled default dressed up as a derivation; and
the volume comparison refuses to run until `disk_total_bytes` is positive —
against a total of `0` the expected budget IS the floor, so a floor-clamped
seat would match and the check would agree with the defect it exists to catch.

### Still open

- **A seat that starts before its instance store is mounted** has a worse
  problem than its WAL budget — it would put data on the root volume. The
  create-then-measure fix makes the budget consistent with wherever the data
  lands; it does not make the mount ordering correct. The new invariant does
  not catch this either, and cannot: a seat on the root volume derives a
  budget that is *correct for the volume it is on*, and INFO agrees with
  itself. What is wrong there is the volume, not the arithmetic.
- **A seat that starts before its instance store is mounted** has a worse
  problem than its WAL budget — it would put data on the root volume. The
  create-then-measure fix makes the budget consistent with wherever the data
  lands; it does not make the mount ordering correct.
