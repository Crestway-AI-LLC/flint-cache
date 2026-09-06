# Architecture decision records

Decisions that shaped Flint, with the reasoning and the alternatives that
lost. Format and rationale: [0001](0001-record-architecture-decisions.md).

| ADR | Decision |
|---|---|
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions |
| [0002](0002-encoding-abstraction.md) | Encoding abstraction: one envelope over a swappable `Kv` |
| [0003](0003-rocksdb-baseline.md) | RocksDB as the v0 engine, measured rather than assumed |
| [0004](0004-no-per-group-raft.md) | No per-group Raft: epoch fencing in the node's own manifest |
| [0011](0011-backup-and-restore.md) | Backup and restore: per-pair checkpoints to object storage, restore only into a fresh cluster |
| [0012](0012-same-slot-transactions.md) | Same-slot MULTI / EXEC / WATCH: ship the three guarantees Flint can honestly give, document the fourth's absence |
| [0013](0013-user-driven-gc-primitives.md) | User-driven GC primitives: no eviction, but a loop the operator can close |
| [0014](0014-introspection-status-surface.md) | One status surface: a build stamp on every seat, `status --json` with drift detection, `CPMYSTATUS` for tenants (proposed) |
| [0016](0016-bloom-filter-type.md) | Bloom filters as a native type: RedisBloom's `BF.*` protocol over a blocked filter, one row per block |
| [0018](0018-cp-held-leases.md) | The write lease is held at the control plane, not the controller |
| [0019](0019-rewind-rejoin-promotion-fences.md) | Rewind rejoin: promotion fences make failover RTO independent of dataset size |
| [0020](0020-proxy-backend-multiplexing.md) | Multiplex the proxy's backend hop: decouple send from receive so a pipeline survives it (proposed) |
| [0021](0021-proxy-async-worker-model.md) | Give the proxy bounded worker threads and async IO, so backend connections have few owners (proposed) |
| [0022](0022-wal-retention-bounded-by-replica-progress.md) | WAL retention bounded by replica progress: back-pressure before a replica meets a deleted segment (amended — one sequence is ONE write, measured, so a fixed 16 KiB fired the shed ~16x early at 1 KiB values; the threshold now derives from OBSERVED bytes/sequence, floored so a tight budget cannot disable the gate) |
| [0023](0023-slot-aligned-bulk-eviction.md) | Slot-aligned bulk eviction: drop whole SSTs by slot range, when rewriting the namespace is the wrong price (proposed) |
| [0024](0024-boot-decision-counters-that-outlive-the-process.md) | Boot-decision counters that outlive the process: make a rejoin LOOP visible without an evidence bundle (proposed) |
| [0025](0025-stream-collection-reads-instead-of-materialising-them.md) | Stream collection reads instead of materialising them |
| [0026](0026-admission-control-on-write-stall.md) | Admission control keyed on the master's own write stall, not on replica lag (proposed; amended — compaction tuning removes the collapse without shedding, so the gate is a backstop for where replica lag binds) |
| [0027](0027-shared-stripe-locks-for-pure-writes.md) | Shared-mode stripe locks for pure writes, so a pipeline can commit as one batch (implemented; its deadlock-freedom argument corrected, the write deadline's clock repaired after batching moved the commit outside it, and its ~2.2x demoted to a measurement OWED -- BUG-0078 showed both arms were measured through a ~50ms TCP stall) |
| [0028](0028-a-verdict-must-name-what-it-examined.md) | A verdict must name what it examined, and the naming must be refutable: five checks passed in one day about subjects nobody had asked about, and every one was caught by something that PRINTED what it looked at rather than by the check's own result. **ACCEPTED 2026-09-04** with a fourth obligation added on acceptance -- a failure names only what it OBSERVED, and where it cannot separate two causes it says so rather than choosing the more serious one -- on the evidence of seven verdicts in one day that named a product fault for a harness or timing condition, one of which reddened the v0.1.0-rc.68 release gate |
| [0029](0029-separate-the-read-lane-from-the-write-lane.md) | Separate the read lane from the write lane at the proxy's connection key: a backend connection is a strict FIFO because "position is the whole correspondence" (ADR-0021), so a slow write blocks every read queued behind it -- and ADR-0026 measured what a slow write is, an L0 stall that pins all 60 connections in flight with the master 92% idle. ADR-0021 removed head-of-line blocking behind STRANGERS' requests and could not touch blocking behind your own. Lane joins the connection key rather than opening a second pool, and read-your-own-writes is held by a barrier -- a client with a write in flight reads on the WRITE lane until it acks -- because splitting naively lets a GET overtake the SET in front of it. **PROPOSED 2026-09-06**, and gated on the one number nobody has: no drill measures read latency while writes stall, so the harm is certain in KIND and unmeasured in MAGNITUDE. If read p99 during a stall is within noise of baseline the ADR is withdrawn, which is a real possibility because the near-cache may serve the reads that would otherwise queue |
| [ADR-0022](0022-wal-retention-bounded-by-replica-progress.md) | WAL retention follows the slowest live replica; the master sheds instead of letting it die |

## Why the numbering has a gap

You will find references to **ADR-0005 through ADR-0009** throughout this
codebase — the shared read/write classifier and the async write queue
(0005), token hashing and credential rotation (0006), federation plumbing
(0007), and so on. Those records are not in this repository.

Flint is open-core. This repository is the engine and the operational
tooling: server, proxy, control plane, controller, `flintctl`, and the
conformance, bench and chaos harnesses — everything needed to run and
operate Flint yourself, under the Elastic License 2.0. The **managed
plane** — fleet autonomy, metering and billing, the tenant and operator
consoles, marketplace fulfilment — is operated by Crestway AI LLC and lives
in a private repository, and the ADRs numbered 0005+ mostly decide things in
that plane.

**Each repository numbers its own ADRs.** This one uses `ADR-<n>`; the managed
plane uses `OPS-ADR-<n>`; `flint-kv` is a third product with its own. A
citation that crosses a boundary carries the prefix — `OPS-ADR-0023` — and a
bare `ADR-<n>` means one of ours.

This paragraph used to say the opposite: that the two halves shared one
sequence on purpose, and that numbering per repository "would make the two
halves impossible to discuss together". **They never shared one.** The
managed plane starts at 0005 because the first four were written before the
split, and both sequences then advanced independently — so ten numbers name
two different documents each. The rule described an intention nothing
enforced, and no allocator ever existed to enforce it.

The remedy is a prefix rather than a renumber, decided in the managed plane's
own ADR-0030 on 2026-08-27, for three reasons worth repeating here:

- **This tree already solved the identical problem with a prefix.** Bug
  numbers are `BUG-0057` here and `OPS-0057` there, and nobody has ever been
  confused by them. Solving the same problem a second way means a reader has
  to learn two conventions and remember which artefact uses which.
- **A prefix is self-describing; a range is a lookup.** `OPS-ADR-0032` says
  where it lives. A number allocated out of a reserved range says so only to
  someone who already knows the convention.
- **It survives a third repository**, which turned out not to be
  hypothetical.

**Adoption is incremental, by design.** Existing citations are corrected as
files are touched rather than in a sweep: renaming nothing means no commit
message, field note or bug file is invalidated, and those cannot be rewritten.
So a bare `ADR-<n>` in older comments here may still mean a managed-plane
decision — treat it as provenance, not as a lookup, exactly as this repository
already treats the `#118`-style tracker ids in its comments.

The citations are kept rather than stripped: a comment saying *why* the
classifier must be one shared table is worth more than a comment that has had
its provenance filed off, even when you cannot open the reference.

**And the call site is what you actually need.** Where a managed-plane ADR
decides something visible from here, the code comment at the call site states
the decision itself, so nothing you need in order to read this repository
depends on a document you cannot see. That is the load-bearing rule; the
numbering only decides whether a reader can tell they are being pointed
somewhere they cannot go.

### The ten numbers that name two decisions

Recorded so a bare citation of one can be recognised, not to be memorised:

| number | this repository | the managed plane |
|---|---|---|
| 0016 | bloom-filter-type | agent-learning |
| 0018 | cp-held-leases | earning-unattended-action |
| 0019 | rewind-rejoin-promotion-fences | a-site-operations-journal-in-git |
| 0023 | slot-aligned-bulk-eviction | s3-accelerator-look-aside-library |
| 0024 | boot-decision-counters-that-outlive-the-process | distributing-secrets-the-fleet-consumes |
| 0025 | stream-collection-reads-instead-of-materialising-them | verify-the-recommendation-not-the-execution |
| 0026 | admission-control-on-write-stall | a-second-protocol-for-the-object-cache |
| 0027 | shared-stripe-locks-for-pure-writes | arming-is-a-declaration-not-a-hand-edit |
| 0028 | a-verdict-must-name-what-it-examined | the-shipping-path-is-unexercised-until-you-ship |
| 0029 | separate-the-read-lane-from-the-write-lane | what-the-acting-agent-may-investigate |

Nine of those are latent: each repository's code cites its own. **0023 is
not**, and its citations here are qualified for that reason — `flint-storage`
cites `OPS-ADR-0023 D7` sixteen times, and this repository's ADR-0023 is a
different document that is *also about eviction* and has no D-numbered
decisions at all. A reader who followed the bare number landed somewhere
plausible and wrong, which is worse than landing nowhere (BUG-0101).
