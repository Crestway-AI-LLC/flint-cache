# BUG-0275: every 30-second rewind snapshot copies a RocksDB MANIFEST that never rolls over, so the snapshot store grows with uptime (OPEN)

**Status:** **OPEN 2026-10-30.** Found on the playground (rc.85) by the
ops session and filed there as ops OPS-0410. It is in public main
(`e6f0ef34`) and every release with rewind snapshots.
**Severity:** medium, for a fleet that runs for months without a restart.
Nothing is lost and no write is refused. The snapshot store and its disk
writes grow with the master's uptime, and past BUG-0195's snapshot relief
the rewind window shrinks and full syncs come more often.
**Area:** `crates/flint-storage/src/rocks.rs` (the options the node opens
RocksDB with, and `checkpoint_to`), and the controller's snapshot cadence
(`--snapshot-interval-ms`, 30,000 by default, `crates/flint-controller`).

## What happens

Each rewind snapshot is a RocksDB checkpoint. A checkpoint hard-links the
SST files, which costs almost nothing, and **copies the MANIFEST**. The
MANIFEST takes a record at every flush and compaction, and rolls over only
at `max_manifest_file_size`, which Flint never sets. RocksDB 10.4.2's
default is 1 GiB (`options.h`), so in practice it rolls only when the node
reopens the DB. So every snapshot copies a file that has grown since the
last restart.

Measured on the playground, 2026-10-30 (ops OPS-0410):

| | |
|---|---|
| snapshots kept in `snaps/g0` | 2,881, 30 s apart, about 24 h |
| the store | 14.0 GB |
| each snapshot's SSTs | about 0.4 MB, hard links |
| its MANIFEST copy | 4,564,790 B (oldest kept) to 5,145,264 B (newest) |
| the master's live MANIFEST | 5,145,428 B, born at the rc.85 restart (2026-10-21 18:00:44Z), so about 579 KB a day |
| the replica's | 20 KB |
| the data | `flint_node_sst_bytes` 0.3-1.0 MB, `flint_tenant_bytes` 0.2-0.3 MB, flat for nine days |

What it costs:
- **Storage:** about 1.67 GB of snapshot store per day of master uptime,
  reset by a restart.
- **Disk writes:** about 14 GB a day on the playground today, for the
  copies alone, rising with uptime.
- **The bound:** BUG-0195's relief releases snapshots at 25% free. On the
  playground's 468 GB volume that is about 207 days of uptime; on a 100 GB
  volume, about 45. Past it, snapshots are released early, the rewind
  window shrinks, and full syncs come more often.

## The fix (to choose when it is built)

- **Cap `max_manifest_file_size`.** RocksDB then rolls the MANIFEST into a
  new one that starts with the DB's current state, so a copy costs that
  state plus at most the cap. The cap must stay well above the size of
  that state on a large DB, or every edit rolls it again.
- **Snapshot less often, or keep fewer.** That bounds the count, not each
  copy, and it gives up rewind resolution, which BUG-0195 and the rewind
  design chose.
- **Both,** with the cap doing the work and the cadence unchanged.

## Acceptance

A test that runs a pair through a few thousand flushes and snapshots, as
30-second snapshots over a long uptime would, and holds the newest
snapshot's MANIFEST, and the snapshot store's total, to a bound that does
not grow with the count. It must fail on today's options.
