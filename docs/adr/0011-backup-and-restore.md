# ADR-0011: Backup and restore — per-pair checkpoints, and restore that only ever creates

Status: proposed — August 2026 (revised)

> Numbering: 0005–0009 are private-plane records and 0010 is reserved for the
> co-processor extension model. See [README](README.md) for why the sequence
> is shared across repositories.

## Context

Flint is sold as a **durable** cache, and the durability story currently ends
at the pair. `docs/failover.md` is honest about where that stops:

> Master + replica both crash → controller REFUSES (not converged), pages;
> spare-restore from durable snapshot … up to last snapshot (rare)

Those snapshots are local to the host. A whole-pair loss takes the data with
it, and on `i4i` the instance store is ephemeral — an instance stop, terminate
or host retirement destroys that node's copy outright, so "lost a node" always
means "lost its data".

The arithmetic says the risk that matters is not the one people compute.
Independent double-node failure is around **1 in 5.5 million pair-years**.
Correlated causes — operator error, a bad deploy, both members landing in one
availability zone, a poison record replicated faithfully to both — are three
to five orders of magnitude more likely, and replication protects against
**none** of them. Backup is not insurance against the rare case; it is the
only recovery from the common ones.

There is a second driver, and for the managed service it is the larger one:
**a pooled multi-tenant service cannot restore a tenant it cannot isolate.**
ElastiCache does not have this problem because its tenancy boundary is the
cluster — one customer, one cache, one snapshot. Flint's managed plane pools
many tenants onto shared pairs, which is where its cost advantage comes from,
and that pooling is exactly what makes restore hard. Tenant-scoped restore is
therefore **table stakes for having chosen pooling**, not a differentiator
over anyone.

### What backup means for a CACHE, specifically

Most of a cache's contents are TTL'd. A backup restored a day later contains
keys whose absolute expiry has passed, and the GC will sweep them on sight.
**That is correct and must not be "fixed"** — rewriting expiry on restore
resurrects data the application asked to have forgotten. It does mean the
honest purpose is not point-in-time recovery of business records:

1. **Disaster recovery** — the pair is gone; get something serving without a
   cold-origin stampede across the whole keyspace.
2. **Tenant recovery** — one tenant corrupted or deleted their own data.
3. **Cloning and migration** — seed staging, or move a dataset between
   clusters, including across a format break.

For (1), restoring 60% of a keyspace with 40% expired is a *success*: the
origin absorbs 40% of the load instead of 100%. For (2), which is the managed
service's common case, the user usually wants to *look at* the old data before
committing to it.

## Decision

### D1 — The unit of backup is the pair checkpoint, and that is sufficient

Each pair's master takes a RocksDB checkpoint (`RocksKv::checkpoint_to`,
already used for replica full sync) and uploads it. Pairs do this
**independently and without coordination**.

The binding passes `LOG_SIZE_FOR_FLUSH = 0`, which makes RocksDB flush
memtables before cutting the checkpoint. A checkpoint therefore already
contains everything the node had accepted at that instant — nothing is
stranded in an unarchived WAL, and the restored copy needs no replay.

There is no fleet-wide freeze and no attempt at a global snapshot instant.
The justification is not pragmatism: **Flint's own command surface makes
cross-pair consistency undefined.** No operation a client can issue spans two
pairs — a cross-slot multi-key command is refused with `CROSSSLOT`, and a
transaction is confined to one slot by ADR-0012 D1, which puts it on exactly
one pair. So there is no multi-pair invariant a client could ever have
observed, and none a backup can violate.

**The trigger to revisit is cross-PAIR atomicity, and only that.** This
record first argued the point by citing `MULTI`/`EXEC`/`WATCH` as excluded by
design, which was true when it was written and stopped being true when
ADR-0012 shipped same-slot transactions. The conclusion survived the change —
one slot is one pair — but the premise did not, and a decision resting on a
fact that has quietly become false is worse than one with no reasoning at
all. Cross-slot transactions would fire the trigger; nothing shipped so far
does.

### D2 — A backup is the pairs plus the control plane, tied by one manifest

Restoring keys without topology gives data nothing can route to; restoring
topology without keys gives an empty cluster confidently claiming slots.

| Component | Source | Why |
|---|---|---|
| Per-pair checkpoint | master's `checkpoint_to` | the data |
| CP state | `{statedir}/cp-state` | pairs, slot ranges, tenants, hashed tokens, quotas |
| Backup manifest | generated | what this set contains, and whether it is intact |

The manifest records a backup id, start and finish times, **format version**,
producing release tag, each object's `sha256`, each pair's slot range, and the
epoch observed on each master. Restore verifies every checksum before it
touches anything.

**Certificates are deliberately not backed up.** The internal CA is not data,
and a restored cluster minting its own is a safety property (D4).

### D3 — Restore only ever CREATES. It never overwrites live data

Two targets, and neither destroys anything that is currently serving:

**(a) A fresh cluster.** `flintctl restore` bootstraps from an inventory and
seeds it from a backup set. It refuses to run against anything with existing
CP state, node data directories, or a live seat. This is the self-hoster's DR
path and the migration path.

**(b) A NEW namespace in a live cluster.** The managed service's path, and its
only one. A tenant's data as of backup time is materialised into a new
namespace alongside their live one. Nothing is fenced, nothing is overwritten,
and a bug in restore cannot damage serving data.

**In-place replacement of a live namespace is explicitly rejected**, not
deferred. It would require shedding the tenant's writes for the duration,
atomically swapping interleaved ranges, and keeping an undo — three hazards
bought for an outcome that (b) plus a pointer flip achieves without any of
them (see Follow-ons).

Restoring a *lost pair inside a surviving cluster* remains out of scope and
gets its own record.

### D4 — Whole-cluster restore SCRUBS the system rows; namespace restore never touches them

A checkpoint copies the whole Kv, and the manifest rows live in the Kv
(`crates/flint-storage/src/manifest.rs`): `\x00flint\x00role`,
`\x00flint\x00claim\x00*`, `\x00flint\x00migration\x00*`. A naive
whole-cluster restore therefore produces a node that **durably believes it is
master at epoch (g, c)** — the precise ingredient for the split-brain that
ADR-0004's epoch fencing exists to prevent.

So restore-to-fresh-cluster rewrites them before any node starts: **role**
cleared (the restored master is started by `bootstrap` on a fresh epoch line);
**claims** dropped and re-derived from the restored CP registry, which is the
authority; **migration rows** always dropped, because an in-flight move
references a peer that does not exist in the new cluster and resuming it would
be a controller chasing a ghost.

The second defence is structural: a restored cluster mints its **own internal
CA**, so it cannot dial, be dialled by, or be fenced against the cluster it
was copied from. Isolation is by mTLS rather than by hope.

**Namespace-scoped restore has none of this exposure.** It moves only user
envelope ranges and never reads or writes a system row, so the hazard does not
arise. That makes (b) structurally safer than (a), which is a large part of
why it is the managed service's only mode.

### D5 — Namespace-scoped restore is slot-granular, and places by CURRENT ownership

The envelope is `cf | ns_len(1B) | ns | slot(2B BE) | user_key`. Namespace is
the outermost discriminator and slot is next, so one tenant's data is a
contiguous range and `(ns, slot)` is a prefix scan. `slot_prefix(cf, ns, slot)`
already exists, described in the source as *"the unit of migration copy and
cleanup"* — the copy path is the one slot migration uses, and
`slot_migrate_drill.sh` already gates it.

Restore is therefore: for each `(ns, slot)` the tenant occupies, range-scan it
out of the checkpoint and write it into the destination namespace **on
whichever node owns that slot now**.

**Current ownership, never the backup's topology.** Slots move between pairs
on rebalance and expand. Placing by the topology recorded in the backup would
silently put rows on a node that no longer owns them, and the proxy would
never route a read to them. Slot granularity is not only for tenant
isolation — it is what makes restore correct across topology change.

### D6 — Restored namespaces are temporary by default

A restore doubles a tenant's stored bytes. Restored namespaces therefore
**auto-reap after 7 days** unless the tenant keeps them, and the expiry is
shown at restore time. Both namespaces count against quota and are billed
while they exist; a restore that quietly doubled someone's bill would be a
support incident, not a feature.

### D7 — Fail closed on a format break

The manifest carries the format version. A mismatch with the restoring
binary's makes restore **refuse**, naming the migration runbook — the same
posture `flintctl upgrade --manifest` takes. A format break cannot roll back,
so it cannot be silently rolled forward by a restore either.

### D8 — S3-compatible storage, public mechanism, daily cadence to start

The target is any S3-compatible endpoint, not AWS S3 specifically; on-prem
evaluators have their own object stores and are part of the audience.

Backup and restore ship in **`flintctl`, in this repository**, under ELv2. The
open-core promise is "everything needed to run AND operate Flint yourself",
and an operator who cannot back up cannot operate. Scheduling, retention
policy, the tenant-facing catalog and fleet orchestration live in the managed
plane; the primitive does not.

**Checkpoint cadence is a property of the cluster, not the tenant** —
checkpoints are physical and shared by every tenant on the pair. The managed
service therefore runs a **fixed daily cadence** and sells *retention*, not
frequency. Letting one tenant's purchase change the cadence for everyone
resident on the box is a cost-allocation problem with no clean answer; a
finer tier can come later by placing tenants by cadence.

Daily means **up to 24 hours of loss**, and most TTL'd content in a day-old
restore will have expired. That number belongs on the enable screen, not in a
runbook someone reads during an incident.

Encryption of the backup at rest is delegated to the object store (SSE-KMS or
equivalent) and documented rather than implemented. That is where the data
actually leaves the host, and a bucket policy is stronger and easier to audit
than a key we manage.

## Alternatives considered

**In-place destructive restore.** Rejected — see D3. The outcome is available
non-destructively.

**Fleet-wide write freeze for a global snapshot instant.** Rejected: it makes
backup an availability event for a cache whose pitch is absorbing origin load,
and D1 shows it buys an invariant no client can observe.

**Continuous WAL archiving for point-in-time recovery.** Deferred, not
refused. The tailer already exists (`get_updates_since`, used by replication),
so the mechanism is closer than it looks — but archive volume scales with
*write churn*, not dataset size, and a cache is the worst case for that ratio.
A 100 GB working set rewritten hourly generates ~2.4 TB/day of WAL. PITR is a
premium tier with a short window, once a customer asks and pays.

**Backing up the replica instead of the master.** Rejected: the replica is by
definition behind by the async tail, so every backup would inherit the RPO
window on top of its own age. Checkpoints are hard-linked and cheap; take them
from the node that has the data.

**Logical dump (SCAN + re-SET).** Rejected: `SCAN` has no consistency
guarantee, is O(keyspace) in requests rather than a file copy, and loses
absolute TTLs. It remains the right tool for *selective* export.

## Consequences

- The durability claim gains an answer to "and if the pair is gone?".
- The managed service gains per-tenant recovery, which pooling made mandatory.
- **Restore never repairs; it creates.** Operators expecting in-place
  resurrection will be surprised, and the docs must say so plainly in the DR
  runbook, not only here.
- A tenant who restores has two namespaces and one live application. Until the
  follow-on below ships, using the restored data means copying from it or
  repointing by hand.
- Backups inherit the checkpoint's I/O profile: cheap in space at creation,
  proportional to live SST bytes on upload.
- Restored data with elapsed TTLs is swept on sight. Expected, documented, and
  explicitly not "fixed".

## Follow-ons (named, not scoped here)

- **Activation by pointer flip.** The tenant→namespace mapping lives in the CP
  tenant record and proxies already receive versioned snapshot pushes when it
  changes. "Use this restore" then becomes a CP update — atomic, near-instant,
  no data movement, reversible by flipping back. This is what completes the
  recovery story while keeping every restore non-destructive, and it should
  ship only after restore itself has proven out.
- **PITR** (see Alternatives). **In-place reseed of a lost pair.**
  **Cross-region.**

## Verification

Nothing here is believed until a drill fails without it.

1. **`tools/backup_restore_drill.sh`** — bootstrap, write a known corpus
   across at least two pairs, back up, destroy the cluster, restore into a
   fresh inventory, assert every non-expired key is present with its value and
   remaining TTL.
2. **The scrub is asserted, not assumed.** After a whole-cluster restore, read
   the system rows directly: role cleared, no stale claims, no migration rows.
   A drill checking only user keys would pass with the split-brain hazard
   fully intact — **this is the assertion that must fail if D4 is removed.**
3. **Namespace restore is isolated.** Restore tenant A into a new namespace
   while tenant B serves live traffic on the same pairs; assert B's data and
   throughput are untouched, and that A's live namespace is byte-identical
   before and after.
4. **Placement follows current ownership.** Move a slot between pairs *after*
   taking the backup, then restore; assert the rows land on the new owner and
   are readable through the proxy. This fails if D5 is implemented against the
   backup's topology.
5. **Corruption is caught.** Flip a byte in an uploaded object; restore must
   refuse on the checksum rather than produce a subtly broken namespace.
6. **Format break refuses**, exiting non-zero and naming the runbook.
7. **Isolation is real.** A restored cluster and its source running
   simultaneously must fail to reach each other — assert the mTLS failure, so
   D4's second defence is evidence rather than reasoning.
