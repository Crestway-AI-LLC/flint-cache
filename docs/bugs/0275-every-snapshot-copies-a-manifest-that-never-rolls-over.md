# BUG-0275: every 30-second rewind snapshot copies a RocksDB MANIFEST that never rolls over, so the snapshot store grows with uptime (FIXED)

**Status:** **FIXED 2026-10-30** in `crates/flint-storage/src/rocks.rs`
(`manifest_max_bytes`). **OPEN 2026-10-30** before that: found on the
playground (rc.85) by the ops session and filed there as ops OPS-0410. It
was in public main (`e6f0ef34`) and every release with rewind snapshots.
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
- **Under write load, faster.** The MANIFEST takes a record at each flush
  and compaction. The playground's ~200 B per snapshot is about one flush
  per snapshot on a seat that is barely written. A loaded seat flushes and
  compacts far more often, so its copies grow faster; its rate was not
  measured.

## The fix

**The MANIFEST is capped, and the cap scales with the volume.** Every open
of a seat's DB now sets `max_manifest_file_size`. RocksDB then rolls the
MANIFEST into a new one that starts with the DB's current state once it
passes the cap, so a snapshot copies at most that state plus the cap.

The cap cannot simply be small. A roll writes the whole state into the new
MANIFEST, and a state larger than the cap rolls again at the very next
edit, so every flush and compaction would rewrite it. RocksDB 10.4.2 has no
option that ties the cap to the state, and this one is fixed at open, while
a seat's state grows from nothing after a full sync. So the cap is tied to
what bounds the state, the data volume:

- The state is one record per live SST. Measured, it is at most
  `MANIFEST_STATE_BYTES_PER_SST` (256 B) for an SST whose first and last
  keys are Flint's envelope around a 40-byte key
  (`manifest_state_per_sst_stays_under_the_cap_arithmetic`).
- With RocksDB's 64 MiB SST target, a volume full of SSTs holds about
  1/262,144 of its size in state. The cap is twice that, 1/131,072 of the
  volume, between 1 MiB and RocksDB's own 1 GiB:

  | volume | cap |
  |---|---|
  | 468 GB (i4i.large, the playground) | 3.6 MB |
  | 1.7 TB (i4i.2xlarge) | 13 MB |
  | 3.75 TB (i4i.4xlarge) | 29 MB |

- So the store holds at most 2,880 copies (`SNAP_RETAIN_MIN_COUNT`) of at
  most half the cap of state plus the cap: about 3.3% of the volume, and at
  most two thirds of that on average. On the playground the MANIFEST first
  reaches the cap about six days after a restart, at its rate; from then
  the store averages about 5 GB and peaks at about 10 GB, where it was
  14 GB at nine days and growing.
- A volume that cannot be read gets 64 MiB, which holds the state of an
  8 TB volume.
- `FLINT_MANIFEST_MAX_MB`, by `node-env`, overrides the cap
  (`docs/self-hosting.md`). A seat whose live `MANIFEST-*` stays above its
  cap is rewriting it at every edit, and that is when to set it.

The cadence and the retention are unchanged: the cap does the work, and
rewind keeps the resolution BUG-0195 and the rewind design chose.

## Acceptance

A test that runs a pair through a few thousand flushes and snapshots, as
30-second snapshots over a long uptime would, and holds the newest
snapshot's MANIFEST, and the snapshot store's total, to a bound that does
not grow with the count. It must fail on today's options.

**Met 2026-10-30.**
`bug_0275_snapshots_copy_a_manifest_that_does_not_grow_with_their_count`
opens a DB on a 64 KiB cap and takes 1,200 snapshots, each after a write,
keeping the newest 50 as the node's pruning does:
- every copy is at most the cap plus 16 KiB, and the 50 kept together at
  most 50 times that;
- the live directory holds at most two `MANIFEST-*` files, so a rolled
  MANIFEST is deleted;
- the oldest and the newest kept snapshot each open and read the value of
  their moment.

With the cap taken out, the same test fails: a snapshot copied 187,084 B
of MANIFEST, and the copies were still growing. With it, it passes, in
about 80 s. `manifest_state_per_sst_stays_under_the_cap_arithmetic` holds
the 256 B per SST that the cap's arithmetic assumes, on 200 SSTs.
