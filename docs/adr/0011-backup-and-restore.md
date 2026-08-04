# ADR-0011: Backup and restore — per-pair checkpoints to object storage, restore only into a fresh cluster

Status: proposed — August 2026

> Numbering: 0005–0009 are private-plane records and 0010 is reserved for the
> co-processor extension model. See [README](README.md) for why the sequence
> is shared across repositories.

## Context

Flint is sold as a **durable** cache, and the durability story currently ends
at the pair. `docs/failover.md` is honest about where that stops:

> Master + replica both crash → controller REFUSES (not converged), pages;
> spare-restore from durable snapshot … up to last snapshot (rare)

Those snapshots are local to the host. A whole-pair loss — an AZ event, a
rack, a bad deploy, an operator with `rm -rf` — takes the data with it. Every
serious evaluation reaches this question early, and "we replicate to a second
node in the same failure domain" is not an answer to it.

There is a second reason, less dramatic and more frequently useful: there is
currently **no way to move a dataset**. Not to a bigger cluster, not to a
staging environment, not to a laptop for a support investigation.

### What backup means for a CACHE, specifically

This is worth settling before designing anything, because it decides what
"good" looks like.

Most of a cache's contents are TTL'd. A backup restored an hour later contains
keys whose absolute expiry has already passed, and the GC will sweep them on
sight. **That is correct and it should not be "fixed"** — rewriting expiry
times on restore would resurrect data the application asked to have forgotten,
which is worse than losing it. But it means the honest purpose of this feature
is not point-in-time recovery of business records. It is:

1. **Disaster recovery** — the pair is gone; get *something* serving again
   without a cold origin stampede across the entire keyspace.
2. **Cloning** — seed staging, or a support environment, from production shape.
3. **Migration** — move a dataset between clusters, including across a
   format break, without a full re-warm from origin.

For (1), a backup that restores 60% of a keyspace with 40% expired is a
*success*: the origin absorbs 40% of the load instead of 100%.

## Decision

### D1 — The unit of backup is the pair checkpoint, and that is sufficient

Each pair's master takes a RocksDB checkpoint (`RocksKv::checkpoint_to`,
already used for replica full sync — a hard-linked, internally consistent
copy) and uploads it to object storage. Pairs do this **independently and
without coordination**.

There is no fleet-wide freeze, no two-phase commit, no attempt at a global
snapshot instant. The justification is not pragmatism; it is that **Flint's
own command surface makes cross-pair consistency undefined**. From
`docs/command-support.md`, excluded by design: cross-slot multi-key commands,
and `MULTI`/`EXEC`/`WATCH`. No operation a client can issue spans two pairs,
so there is no multi-pair invariant a client could ever have observed, and
therefore none a backup can violate. Per-pair consistency *is* cluster
consistency for everything that is expressible.

This is a property to defend deliberately: **if cross-slot transactions are
ever added, this decision must be revisited**, because that is the moment the
argument stops holding.

### D2 — A backup is the pairs plus the control plane, tied by one manifest

Restoring keys without topology gives a heap of data nothing can route to;
restoring topology without keys gives an empty cluster confidently claiming
slots. A backup set is therefore:

| Component | Source | Why |
|---|---|---|
| Per-pair checkpoint | master's `checkpoint_to` | the data |
| CP state | `{statedir}/cp-state` | pairs, slot ranges, tenants, hashed tokens, quotas, opt-ins |
| Backup manifest | generated | what this set contains and whether it is intact |

The manifest records, per backup: a backup id, wall-clock start and finish,
the **format version**, the release tag that produced it, each object's path
and `sha256`, each pair's slot range, and the epoch observed on each master at
checkpoint time. Restore verifies every checksum before it touches anything.

**Certificates are deliberately NOT backed up.** The internal CA is not data,
and a restored cluster minting its own CA is a safety property, not an
inconvenience — see D4.

### D3 — Restore targets a FRESH cluster, never a live one

`flintctl restore` bootstraps a new cluster from an inventory and seeds it
from a backup set. It refuses to run against a cluster that has a CP state
file, node data directories, or any live seat.

Reseeding a *lost pair inside a surviving cluster* is a different operation
with different risks (the survivor's epoch line, in-flight migrations,
proxies holding a slot map that predates the loss). It is out of scope here
and should get its own record once this one has run in anger. The DR path in
the meantime is: restore to a fresh cluster, then cut traffic over.

### D4 — Restore SCRUBS the system rows, and this is the correctness core

A checkpoint is a copy of the whole Kv, and the manifest rows live in the Kv
(`crates/flint-storage/src/manifest.rs`): `\x00flint\x00role`,
`\x00flint\x00claim\x00*`, `\x00flint\x00migration\x00*`. A naive restore
therefore produces a node that **durably believes it is master at epoch
(g, c)** — the precise ingredient for the split-brain that ADR-0004's epoch
fencing exists to prevent.

So restore rewrites them before the node is ever started:

- **Role** — cleared. The restored master is started as the pair's master by
  `bootstrap`, at a fresh epoch line, exactly as any new cluster's is.
- **Claims** — dropped and re-derived from the restored CP registry, which is
  the authority on who owns which slots.
- **Migration rows** — dropped, always. An in-flight slot move references a
  peer node that does not exist in the new cluster; resuming it would be a
  controller chasing a ghost. A backup taken mid-migration restores as though
  the migration had not started, which is safe because the move's source data
  is in the checkpoint either way.

The second line of defence is structural: a restored cluster mints its **own
internal CA** (D2), so it physically cannot dial, be dialled by, or be fenced
against the cluster it was copied from. A restore into a network that can
still reach the original is isolated by mTLS rather than by hope.

### D5 — Fail closed on a format break

The backup manifest carries the format version. If it does not match the
restoring binary's, restore **refuses** and names the migration runbook. This
mirrors what `flintctl upgrade --manifest` already does with `format_break`:
a format break cannot roll back, so it cannot be silently rolled *forward* by
a restore either.

### D6 — Object storage is S3-compatible, and the mechanism is public

The target is any S3-compatible endpoint, not AWS S3 specifically — on-prem
evaluators have their own object stores and are the audience this feature is
partly for.

Backup and restore ship in **`flintctl`, in this repository**, under ELv2.
The open-core promise is "everything needed to run AND operate Flint
yourself", and a self-hoster who cannot back up cannot operate. Scheduling,
retention policy and fleet-wide orchestration may live in the managed plane;
the primitive may not.

Encryption of the backup at rest is delegated to the object store (SSE-KMS or
equivalent) and documented rather than implemented. That is where the data
actually leaves the host, and a bucket policy is both stronger and easier to
audit than a key we manage ourselves.

## Alternatives considered

**Fleet-wide write freeze for a global snapshot instant.** Rejected: it makes
backup an availability event for a cache whose whole pitch is absorbing
origin load, and D1 shows it would buy an invariant no client can observe.

**Continuous WAL archiving for point-in-time recovery.** Rejected *for now*,
not on principle. It is the right shape for a database, and PITR over a
keyspace that is mostly TTL'd buys much less than it costs. The checkpoint
format does not preclude adding it later.

**Backing up the replica instead of the master.** Tempting — it moves I/O off
the write path. Rejected because the replica is by definition behind by the
async tail, so every backup would silently inherit the RPO window on top of
its own age. Checkpoints are hard-linked and cheap; take them from the node
that has the data.

**Logical dump (SCAN + re-SET).** Rejected: `SCAN` is a cursor over live data
with no consistency guarantee, it is O(keyspace) in requests rather than a
file copy, and it loses absolute TTLs. It remains the right tool for
*selective* export, which is a different feature.

## Consequences

- The durability claim gains an answer to "and if the pair is gone?" that does
  not end in a shrug.
- Restore is a **cluster-creating** operation. Operators expecting in-place
  resurrection will be surprised; the docs must say so plainly, in the DR
  runbook and not only here.
- Backups inherit the checkpoint's I/O profile: hard links make them cheap in
  space at creation and expensive at upload, proportional to live SST bytes.
  Retention is the operator's cost decision, so retention policy is a
  documented knob, not a default we choose for them.
- Restored data with elapsed TTLs is swept on sight. Expected, documented, and
  explicitly not "fixed".

## Verification

Nothing here is believed until a drill fails without it. Required before this
ADR moves to accepted:

1. **`tools/backup_restore_drill.sh`** — bootstrap a cluster, write a known
   corpus across at least two pairs, back up, destroy the cluster entirely,
   restore into a fresh inventory, and assert every non-expired key is present
   with its value and its remaining TTL intact.
2. **The scrub is asserted, not assumed.** After restore, read the system
   rows directly: role cleared, no stale claims, no migration rows. A drill
   that only checks user keys would pass with the split-brain hazard fully
   intact — this is the assertion that must fail if D4 is removed.
3. **Corruption is caught.** Flip a byte in an uploaded object; restore must
   refuse on the checksum rather than produce a subtly broken cluster.
4. **Format break refuses.** A manifest with a mismatched format version must
   exit non-zero naming the runbook.
5. **Isolation is real.** A restored cluster and its source running
   simultaneously must not be able to reach each other — assert the mTLS
   failure, so the claim in D4 is evidence rather than reasoning.
