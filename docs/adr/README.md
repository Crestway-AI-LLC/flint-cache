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

They are numbered in one sequence on purpose. A decision does not become a
different decision because of which repository it lands in, and renumbering
per repository would make the two halves impossible to discuss together.
The citations are kept as-is rather than stripped: a comment saying *why*
the classifier must be one shared table is worth more than a comment that
has had its provenance filed off, even when you cannot open the reference.

Where an ADR in that range decides something visible from here, the code
comment at the call site states the decision itself, so nothing you need in
order to read this repository depends on a document you cannot see.
