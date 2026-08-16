# ADR-0019: Rewind rejoin — promotion fences make failover RTO independent of dataset size

Status: accepted (2026-08-15)

## Context

Soak runs 26 and 27 both breached the published 10 s RTO on the same
arithmetic. On run 27: `PromoteIssued + widowed-grace(10 s) = the start of a
26,231 ms ack gap that ended the instant the killed ex-master finished a
42-file full re-seed`. Writes flowed during the grace, then the widowed gate
refused everything until the transfer completed. The failover itself took
711 ms; the outage was the rejoin.

The cause is a composition of three individually-correct contracts:

1. **The ex-master wipe contract** (#107): a node killed while master may
   hold acked-but-unreplicated writes past the promotion point, so it must
   not tail from its data as-is. The remedy was wipe + checkpoint full sync.
2. **The widowed grace** (#121/#122): a master with no live replica may
   accept writes only for a bounded window — this enforces the published
   RPO age bound. Its design assumption, stated in its own comment, is that
   "normal promotions attach a replacement well inside the grace".
3. **The full-sync rate cap** (#184): a re-seed must not starve the write
   path — which also means the re-seed takes minutes at tens of GB and
   hours at TB scale.

Composed: the replacement CANNOT attach inside the grace, because attaching
requires transferring the whole dataset. Measured RTO = grace + re-seed
remainder, growing linearly with data. At 5 TB the published 10 s claim
would be off by three orders of magnitude.

## Decision

Take the re-seed off the write-availability path. The ex-master's data is
99.9% correct — only its tail past the branch point is poison. Rejoin by
**rewinding to a provably-safe local snapshot** and tailing the difference:

1. **Promotion fence.** `FLINTPROMOTE` (and the spare-restore generation
   bump) durably records `(epoch → latest_seq)` — where the timeline
   branched. System rows: not streamed, but copied by every checkpoint, so
   histories accumulate onto whoever is seeded. For a two-member pair the
   current master therefore always holds a fence row for every promotion at
   which the timeline actually diverged (the survivor executed it; same-node
   demote/re-promote cycles may be missing from copies but nothing diverged
   at them). The safety argument lives on `PROMO_FENCE_KEY_PREFIX`.
2. **Labeled snapshots.** A master's `FLINTSNAPSHOT` id carries its role
   epoch (`snap-<ms>-seq<N>-e<g>.<c>`). Replica snapshots are unlabeled and
   never rewind-eligible — the fence argument only covers epochs the
   labeling node held as master.
3. **Rewind at boot.** `flintctl start` no longer wipes a rejoining seat; it
   writes the `NEEDS_RESEED` marker and passes `--rewind-snaps
   <statedir>/snaps/g<i>`. The marked server asks the new master
   (`FLINTFENCE <epoch>`) for the highest seq it vouches for, restores its
   newest local snapshot at or before that bound (hard links — O(files),
   not O(bytes)), reasserts replica identity under the snapshot's epoch,
   and tails from the snapshot's seq. Catch-up is bounded by the snapshot
   cadence (30 s of writes), independent of dataset size.
4. **Fence enforcement on the wire.** `FLINTSYNC` optionally carries the
   replica's claim epoch; a cursor past the fence for that epoch is refused
   with `-WALGAP`, which the tailer already escalates to a full re-seed. On
   acceptance the master's `FLINTSYNC-OK` names its own epoch and the
   replica adopts it durably, so a legitimately grown cursor is not later
   refused at a fence it has outgrown. A promotion racing the rejoin
   therefore downgrades it to a re-seed, never a divergent copy.
5. **Fallback.** No labeled snapshot, no vouchable fence, or a refusal →
   today's wipe + rate-capped full sync, unchanged.

## Consequences

- RTO decouples from dataset size: post-failover write unavailability is
  bounded by restart + hard-link restore + one snapshot-cadence of catch-up
  (seconds), inside the 10 s widowed grace — which keeps its RPO meaning.
- The exposure window for the old behaviour shrinks to "master died before
  taking any labeled snapshot in its current epoch" (one cadence wide).
- `roll_node` still wipes (#190): upgrades of large pairs remain a planned
  widowed window until it adopts the same contract.
- Two-member pairs only: the fence-completeness induction breaks for wider
  replica sets; revisit before those exist.
- Proven by `rewind_rejoin_drill.sh`: the rewind, the epoch adoption, the
  client- and server-side refusal of a past-the-fence snapshot, and the
  abandoned branch staying dead.
