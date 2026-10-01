# BUG-0195: snapshot retention kept every snapshot for a day, so on a churning pair the pinned SSTs filled the disk and every write was refused (FIXED 2026-09-30)

**Status:** **FIXED 2026-09-30**, found the same day by the soak run to verify
BUG-0194 (5 x i4i.large, a data plane built from public `4be5b78`, 240 minutes
of ~20 MB/s through a 20 GB window, a kill every 12 minutes). Held by
`tools/snapshot_pressure_drill.sh`, which fails before the fix and passes
after. Released in the next cut after v0.1.0-rc.78.
**Severity:** high. Every write was refused on a healthy pair that still had a
replica, from a policy meant only to make rejoins faster. Any pair writing
faster than its disk can hold a day of snapshots gets there. The soak reached
it in about 108 minutes on a 468 GB NVMe. Every shipped release has it.

## What was seen

Cycles 1 to 8 passed, oracle green. Cycle 9's verdict:

    iter 1: edge served fewer than 50 writes in 10000ms x2 after the kill (0 since) ...
    writer: no ack yet in this run; since the kill 25713 sent, 0 acked, 0 THROTTLED,
    25713 other errors (last: Ok(Error("QUOTA server is low on disk space; writes
    rejected until space is reclaimed ..."))), 0 failed dials

Both seats had crossed the disk guard's floor:

    capacity reclaim: ENGAGED (free 70121693184 of 467771486208 bytes, target 93554297240)
    disk guard: Ok -> Shed (free 46736224256 of 467771486208 bytes)

and every snapshot line in the captured tails, 98 per seat, said the same:

    snapshot snap-...-e0.16 written to /var/lib/flint/snaps/g0 (pruned 0: fewer snapshots held than the floor keeps)

This is the same cycle and the same panic as the 2026-09-21 soak's cycle 9,
which BUG-0071 recorded as unexplained. Its verdict could not say more; this
one, with public `cefeb70`'s writer report, could.

## Mechanism

`FLINTSNAPSHOT` is a RocksDB checkpoint into the snapshot root, and on the same
filesystem a checkpoint is **hard links** to the live SSTs. It costs nothing
when taken. When compaction then replaces those SSTs, the database unlinks its
names and the snapshot's links keep the files. So on a pair whose data churns,
each snapshot pins the SSTs that existed at its moment, and the pinned total
grows with the volume written, not with the data held.

`prune_snapshots` (BUG-0163) removes a snapshot only when it is beyond the
newest **2,880** (a day at one per 30 s) **and** older than
`max(2 x archive reach, 24 h)`. So nothing is removed on a node's first day,
however full the disk. And retention had no disk-pressure term at all. The disk
guard then did its job, refusing writes at 10% free, to protect snapshots whose
only use is a faster rejoin.

**Measured on the drill, inferred on the soak.** The drill's per-round
breakdown shows the space held only by snapshots growing by exactly one round's
rewrite (~25 MB) per snapshot. On the unfixed build that is 376 MB of a 512 MB
disk at 16 snapshots, writes refused from round 15, and the next `FLINTSNAPSHOT`
failing with `No space left on device`. The soak's disk was not measured: the
harness tore the fleet down on failure. Its other occupants were bounded,
though: the live data (~20 GB) and the WAL archive (a ~111 GB budget, enforced
every 10 minutes, BUG-0093). Snapshot pinning is the only term that grows
without limit with time at a fixed rate, which is also why the failure lands on
the same cycle in both runs.

One more consequence, not reached on the soak because `flint-chaos` refuses an
evictable namespace: capacity reclaim engages at 15% free and **evicts tenant
keys** from evictable namespaces. With dead snapshots holding the disk, it would
have deleted live data to make room for files nothing reads.

## Fix

**Snapshot relief.** On every `FLINTSNAPSHOT`, after the age-based prune, the
server checks free space on the data directory's filesystem. Below the relief
line it releases snapshots oldest first, measuring free space again after each
one. It stops as soon as there is room, and never touches what LATEST names or
the newest four (`SNAP_PRESSURE_KEEP`: a rewind takes the newest snapshot at or
below the promotion fence, which trails the dead master by the replica's lag).

- **The line is ordered against the others by construction**
  (`diskguard::snapshot_relief_below`). It is 250% of the shed floor, clamped
  like reclaim's target and never below where reclaim starts. So with rising
  usage, snapshots are released at 25% free, keys are evicted at 15%, and writes
  shed at 10%. A property test sweeps disk sizes, both thresholds and every free
  level, and asserts relief is engaged wherever eviction would start.
- **Every unknown means keep**, as in `prune_snapshots`: an unreadable sample
  stops it, no configured shed line disables it, and a snapshot root on another
  filesystem is left alone, since releasing it frees nothing where the guard
  measures.
- The release is logged on the snapshot line that already prints every 30 s:
  `released N under disk pressure (free F bytes, relief below R)`.

No wire change and no new `Mutation`; a node without the fix simply keeps its
snapshots, as before.

## Verification

- `tools/snapshot_pressure_drill.sh`: a 512 MB filesystem, a 25 MB live set
  rewritten 30 times with a forced compaction and a snapshot after each, and the
  WAL archive pinned small so it cannot be what fills the disk. **Unfixed:**
  writes refused from round 15, then `FLINTSNAPSHOT` fails with ENOSPC at round
  17. **Fixed:** 0 writes refused in 30 rounds, snapshot-only space capped near
  250-300 MB, 10-13 held, every key reads back its final value byte for byte.
  The first cut of this drill used a 60 s WAL TTL. RocksDB checks it only every
  `ttl/2`, so its own archive filled the disk with snapshots already released;
  the per-round breakdown is what showed that.
- Unit tests (`quarantine_tests`): oldest first and stops once there is room;
  never the newest floor or LATEST, even when LATEST is the oldest; nothing on a
  blind sample or without pressure. `diskguard`: the ordering sweep and the
  no-signal cases. Two mutants checked: dropping the floor at reclaim's start
  fails the sweep (`total=1GB free=80% min_free_bytes=1GB`), and releasing
  newest-first fails the order test.

## Deliberately not changed

- **The 24-hour retention itself.** It is right for a box that does not churn
  (the playground's pair is ~300 KiB), and it now yields to pressure.
- **The snapshot cadence.** Thirty seconds is the controller's schedule, and
  pressure now bounds what it can pin.
- **The WAL archive's 10-minute size enforcement** (BUG-0093, accommodated by
  sizing).
