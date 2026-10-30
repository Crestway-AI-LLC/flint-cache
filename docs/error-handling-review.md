# Error-handling review map

Pinned to [`2c2c6281`](https://github.com/Crestway-AI-LLC/flint-cache/tree/2c2c6281e386a1ab7a9908f516d7ed618db494c9), public main on 2026-10-30. Every line number below refers to that commit, and every code link opens it, so the map stays exact as main moves on. About 84,000 lines of non-test code in 16 crates, split into fourteen areas that one reviewer can take each. Each area lists where its error paths are, the questions a reviewer should answer, and the decisions and past bugs that set the rules it should follow.

Report each finding as a numbered bug in `bugs/`: what happens, how it was found, and an acceptance test that fails today.

## Rules every area is reviewed against

- **No `unwrap()` outside tests.** Clippy's `unwrap_used` is set for the workspace and the gate runs clippy with warnings as errors. `expect()` is allowed: 146 calls in production crates. Each should hold an invariant, not an input.
- **A panic ends one thread and poisons its locks.** No profile sets `panic = "abort"` and no production path catches a panic. A panic kills its thread, and any `std::sync::Mutex` it held is poisoned. flint-server never calls `expect` on a lock and recovers a poisoned one at 15 sites; flint-vec calls `lock().expect` at 32 of its 43 lock sites. BUG-0073 is the precedent: a panicking thread stopped a pair's snapshots silently.
- **Leaving with the database open uses `hard_exit`.** Any exit while RocksDB is open goes through `hard_exit` (flint-server `main.rs:162`), which calls `_exit` so it does not race RocksDB's C++ teardown. Startup exits before the store opens use `process::exit`.
- **Errors speak the client's dialect.** Where a command exists in Valkey, RedisJSON or RediSearch, its refusals use their text and kind. An error reply never contains a raw newline (BUG-0184). Flint's own codes are `LOADING`, `READONLY`, `MOVED`, `TRYAGAIN`, `CROSSSLOT`, `THROTTLED`, `WALGAP`, `INDEXED`, `COPROCUNAVAIL`, `VECFULL` and `QUOTA`; [retry-safety.md](retry-safety.md) says which a client may retry.
- **Unknown is not a value.** A probe that fails is unknown, never full, empty or healthy (`disk::sample`, BUG-0079). A verdict names what it examined (ADR-0028). An unknown flag name is refused at start (`reject_unknown_flags`), and flintctl checks seat environment names (BUG-0099).

## The fourteen areas

| | Area | Priority | Size (lines) | Reviewer |
|---|---|---|---|---|
| A | [Write path and storage engine](#a-write-path-and-storage-engine) | P1 | ≈ 4,300 | Rust and RocksDB or storage engines |
| B | [Replication](#b-replication) | P1 | ≈ 3,000 | distributed systems, replication |
| C | [Failover, roles and fencing](#c-failover-roles-and-fencing) | P1 | ≈ 5,200 | distributed systems, consensus |
| D | [Snapshots, backup and restore](#d-snapshots-backup-and-restore) | P1 | ≈ 2,900 | Rust, storage and operations |
| E | [Client connections and the wire protocol](#e-client-connections-and-the-wire-protocol) | P2 | ≈ 3,300 | network servers, the Redis protocol |
| F | [Proxy: routing, retries and backend failures](#f-proxy-routing-retries-and-backend-failures) | P2 | ≈ 1,900 | network and distributed systems |
| G | [Proxy: client sessions](#g-proxy-client-sessions) | P2 | ≈ 3,400 | network servers, multi-tenant systems |
| H | [Scripts and transactions](#h-scripts-and-transactions) | P2 | ≈ 3,800 | Rust, Lua and Redis semantics |
| I | [Slot migration and rebalancing](#i-slot-migration-and-rebalancing) | P2 | ≈ 2,300 | distributed systems |
| J | [Commands and data types](#j-commands-and-data-types) | P3 | ≈ 17,000; split by family | Redis and Valkey semantics |
| K | [Vector and FT co-processor](#k-vector-and-ft-co-processor) | P3 | ≈ 5,600 of service code, ≈ 9,000 of parsers | Rust concurrency, search |
| L | [Startup, configuration and shutdown](#l-startup-configuration-and-shutdown) | P2 | ≈ 2,500 | Rust, systems |
| M | [Operator tooling and the control-plane service](#m-operator-tooling-and-the-control-plane-service) | P3 | ≈ 15,000 | operations tooling, Rust |
| N | [Cross-cutting sweep](#n-cross-cutting-sweep) | P1 | whole tree | anyone with grep and patience |

**P1**: a missed error can lose or corrupt acknowledged data; **P2**: a missed error can cause an outage, a wrong route or a stuck state; **P3**: a missed error can give a wrong error or mislead an operator. Sizes are non-test lines in the regions listed, rounded. Area J is large enough to split by command family, and area M by flintctl and the control-plane service.

## A. Write path and storage engine

**P1** · ≈ 4,300 lines · reviewer: Rust and RocksDB or storage engines

Everything between a client's write and the bytes on disk: admission, the write queue, the batch that becomes one RocksDB WriteBatch, fsync, the disk guard and eviction. A missed error here can acknowledge a write that was never stored.

**Where to look**

- [`crates/flint-storage/src/lib.rs:86-217`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/lib.rs#L86-L217) the Kv trait: get returns Option, put returns nothing
- [`crates/flint-storage/src/rocks.rs:1089`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/rocks.rs#L1089) impl Kv for RocksKv: get uses .ok(), put and delete discard the result
- [`crates/flint-storage/src/rocks.rs:322-358`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/rocks.rs#L322-L358) apply_writes: the one write call that returns RocksDB's error
- [`crates/flint-storage/src/rocks.rs:1034-1088`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/rocks.rs#L1034-L1088) flush_wal_sync, checkpoint_to, flush_checked
- [`crates/flint-storage/src/batch.rs:36-239`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/batch.rs#L36-L239) BatchingKv: collects puts and deletes into one batch
- [`crates/flint-server/src/main.rs:4229-4285`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L4229-L4285) commit_pending
- [`crates/flint-server/src/main.rs:5008-5226`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L5008-L5226) admit_write_path
- [`crates/flint-server/src/main.rs:5346-5458`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L5346-L5458) commit_ops, apply_inline, locks_every_writer
- [`crates/flint-server/src/write_queue.rs:243-371`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/write_queue.rs#L243-L371) submit and the consumer thread
- [`crates/flint-server/src/write_lock.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/write_lock.rs) stripe locks for pure writes
- [`crates/flint-server/src/diskguard.rs:85-398`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/diskguard.rs#L85-L398) verdict, reclaim_action, shed_floor_bytes, apply
- [`crates/flint-storage/src/admission.rs:309-360`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/admission.rs#L309-L360) admit and release
- [`crates/flint-storage/src/eviction.rs:258-410`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/eviction.rs#L258-L410) should_drop and reclaim
- [`crates/flint-storage/src/disk.rs:53-110`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/disk.rs#L53-L110) sample: None means unknown
- [`crates/flint-storage/src/gc.rs:37-110`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/gc.rs#L37-L110) sweep

**Questions to answer**

- Does every write a client is told succeeded go through `apply_writes`, which returns RocksDB's error? Look for any production path that calls `put` or `delete` on `RocksKv` directly, where `let _ = self.db.put(...)` throws the error away.
- `Kv::get` turns a RocksDB read error into `None`. Can an I/O error be served as a cache miss, or drive a write that assumes the key is absent (`INCR`, `SETNX`, `SET NX`)?
- When `apply_writes` fails (disk full, I/O error, a write stall), what does the client get, is anything partly applied, and do the watch table, the write queue and the replication sequence stay consistent?
- After an fsync error from `flush_wal_sync`, does the node keep serving as if the data were durable?
- When `disk::sample` returns `None`, is that treated as unknown everywhere, and never as full or as empty?
- Can `should_drop` remove a row from a namespace that was never declared evictable, for example when the namespace cannot be parsed from the key?
- Are the shed and refusal replies (`-THROTTLED`, quota, disk) the ones `retry-safety.md` says a client may retry?

**Rules and history:** [ADR-0003](adr/0003-rocksdb-baseline.md), [ADR-0013](adr/0013-user-driven-gc-primitives.md), [ADR-0022](adr/0022-wal-retention-bounded-by-replica-progress.md), [ADR-0023](adr/0023-slot-aligned-bulk-eviction.md), [ADR-0026](adr/0026-admission-control-on-write-stall.md), [ADR-0027](adr/0027-shared-stripe-locks-for-pure-writes.md), [ADR-0029](adr/0029-separate-the-read-lane-from-the-write-lane.md), [BUG-0013](bugs/0013-bulk-writes-stall-on-default-compaction.md), [BUG-0079](bugs/0079-the-wal-shed-guard-fires-after-the-archive-has-already-deleted-it.md), [BUG-0195](bugs/0195-snapshot-retention-filled-the-disk-and-every-write-was-refused.md), [BUG-0237](bugs/0237-a-bloom-batch-hid-a-partial-write-behind-one-error.md), [BUG-0251](bugs/0251-a-store-refused-for-its-size-deleted-its-destination-first.md), [retry-safety.md](retry-safety.md), [space-reclaim.md](space-reclaim.md), [capacity-model.md](capacity-model.md)

## B. Replication

**P1** · ≈ 3,000 lines · reviewer: distributed systems, replication

The master's stream to its replicas, the replica's apply loop, full sync, and the decisions to resume, rewind or re-seed. A wrong call here can hide a gap or let a replica diverge.

**Where to look**

- [`crates/flint-storage/src/repl.rs:178-300`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/repl.rs#L178-L300) rewalk_on_gap, wal_file_vanished, retrying_walk, updates_since_budgeted
- [`crates/flint-storage/src/repl.rs:302-497`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/repl.rs#L302-L497) walk_since_budgeted
- [`crates/flint-storage/src/repl.rs:498-539`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/repl.rs#L498-L539) apply_batch on the replica
- [`crates/flint-storage/src/repl.rs:580-820`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/repl.rs#L580-L820) own_seq_for_upstream, set_last_applied, adopt_timeline
- [`crates/flint-server/src/main.rs:7247-7311`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L7247-L7311) drain_acks
- [`crates/flint-server/src/main.rs:7312-7662`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L7312-L7662) flintsync: the master's side of the stream
- [`crates/flint-server/src/main.rs:7765-7913`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L7765-L7913) flintfullsync
- [`crates/flint-server/src/main.rs:7914-8499`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L7914-L8499) mod replica: the replica's loop (run at 7953)
- [`crates/flint-server/src/main.rs:405-818`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L405-L818) parse_rewind_candidate, quarantine_unresumable, classify_probe, probe_resume
- [`crates/flint-server/src/main.rs:819-1205`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L819-L1205) try_rewind
- [`crates/flint-server/src/main.rs:213-371`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L213-L371) NEEDS_RESEED marker: mark, remember, clear
- [`crates/flint-server/src/repl_hub.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/repl_hub.rs) headroom shed

**Questions to answer**

- For every way a stream can end (socket error, `WALGAP`, `SequenceGap`, a storage error, the master demoted mid-stream), does the replica reconnect, quarantine or full-sync, and is the reason logged and counted?
- Can a transient read error be taken for a WAL gap, or a real gap be taken for no new data?
- If `apply_batch` fails partway, is the cursor left where it was, and is the batch all-or-nothing?
- If a full sync is cut off, can the replica start serving a partial copy?
- Can a write be acknowledged to a client under min-replicas when the replica's ack failed or never came?
- `mark_needs_reseed` is best-effort. If writing the marker fails, what does the next start do with the data directory?

**Rules and history:** [ADR-0019](adr/0019-rewind-rejoin-promotion-fences.md), [ADR-0022](adr/0022-wal-retention-bounded-by-replica-progress.md), [BUG-0070](bugs/0070-the-rejoin-probe-scans-the-wal-so-rto-grows-with-uptime.md), [BUG-0143](bugs/0143-the-wal-gap-quarantine-does-not-always-fire-when-the-span-is-gone.md), [BUG-0162](bugs/0162-a-wal-file-that-vanishes-under-a-walk-is-reported-as-an-unrecoverable-gap.md), [BUG-0196](bugs/0196-a-replica-and-master-that-disagree-about-the-stream-lose-writes-silently.md), [failover.md](failover.md)

## C. Failover, roles and fencing

**P1** · ≈ 5,200 lines · reviewer: distributed systems, consensus

Who is master and how the others are kept from acting as one: role epochs on disk, promote, demote, fence, the controller's decision loop, and the control plane's leases and Raft store. A mistake here gives two masters or none.

**Where to look**

- [`crates/flint-server/src/main.rs:1331-1453`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L1331-L1453) role_transition, commit_role, role_snapshot
- [`crates/flint-server/src/main.rs:6493-6904`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L6493-L6904) flintpromote, flintfollow, flintfence, flintdemote
- [`crates/flint-server/src/main.rs:2025-2110`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L2025-L2110) superseded_cause, cp_lease_renew
- [`crates/flint-server/src/main.rs:4950-4999`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L4950-L4999) check_node_health
- [`crates/flint-storage/src/manifest.rs:213-263`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/manifest.rs#L213-L263) read_role, set_role, force_role
- [`crates/flint-storage/src/manifest.rs:416-485`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/manifest.rs#L416-L485) record_promo_fence, promo_fence_bound
- [`crates/flint-controller/src/main.rs:225-530`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controller/src/main.rs#L225-L530) call, converged, should_promote, epoch checks
- [`crates/flint-controller/src/main.rs:531-835`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controller/src/main.rs#L531-L835) observe, apply_flintinfo
- [`crates/flint-controller/src/main.rs:836-1450`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controller/src/main.rs#L836-L1450) tick: the decision loop
- [`crates/flint-controller/src/main.rs:2123-2140`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controller/src/main.rs#L2123-L2140) fence
- [`crates/flint-controlplane/src/ha.rs:84-576`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controlplane/src/ha.rs#L84-L576) RPC framing, call, serve_rpc, propose
- [`crates/flint-controlplane/src/ha.rs:577-1586`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controlplane/src/ha.rs#L577-L1586) handle_admin
- [`crates/flint-controlplane/src/raft.rs:55-143`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controlplane/src/raft.rs#L55-L143) io, atomic_write, Store::open
- [`crates/flint-controlplane/src/main.rs:185-231`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controlplane/src/main.rs#L185-L231) publish_lease_mirror

**Questions to answer**

- If a role write (`set_role`) fails, can the node act as master in memory while its disk says replica, or the reverse?
- When the controller cannot reach a node, does it tell a timeout, a refusal and an unexpected reply apart, and can it promote while the old master may still accept writes?
- Are stale and equal epochs refused by every role command, including right after a restart?
- If lease renewal fails, does a master stop taking writes in time? Is a control plane that is down treated differently from one that refuses?
- If the Raft store cannot open or `atomic_write` fails, does the control plane refuse to serve rather than serve an empty registry?

**Rules and history:** [ADR-0004](adr/0004-no-per-group-raft.md), [ADR-0018](adr/0018-cp-held-leases.md), [ADR-0019](adr/0019-rewind-rejoin-promotion-fences.md), [ADR-0032](adr/0032-one-implementation-of-every-control-plane-mutation.md), [BUG-0074](bugs/0074-min-replicas-that-cannot-survive-a-failover.md), [BUG-0076](bugs/0076-a-surviving-replica-is-never-repointed-after-a-promotion.md), [BUG-0119](bugs/0119-flintctl-panicked-on-a-master-that-was-merely-mid-promotion.md), [BUG-0151](bugs/0151-a-pair-whose-membership-changed-can-hold-two-lease-rows-and-two-masters.md), [BUG-0174](bugs/0174-reconcile-cold-start-gives-up-while-the-master-is-still-loading.md), [BUG-0201](bugs/0201-the-controller-fails-over-a-master-at-its-connection-cap-though-it-is-serving.md), [BUG-0207](bugs/0207-during-a-roll-the-controller-fails-the-pair-over-mid-handover.md), [failover.md](failover.md)

## D. Snapshots, backup and restore

**P1** · ≈ 2,900 lines · reviewer: Rust, storage and operations

Rewind snapshots on the node, the controller's scheduled snapshots, and flint-backup's sets in S3 and their restore. A missed error here leaves a backup that cannot restore, or a restore that serves half the data.

**Where to look**

- [`crates/flint-server/src/main.rs:7663-7764`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L7663-L7764) flintsnapshot: checkpoint, then LATEST, then prune
- [`crates/flint-server/src/main.rs:559-744`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L559-L744) release_snapshots_under_pressure, same_filesystem, prune_snapshots
- [`crates/flint-server/src/main.rs:6445-6492`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L6445-L6492) flintnsrestore
- [`crates/flint-storage/src/rocks.rs:712-727`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/rocks.rs#L712-L727) open_read_only
- [`crates/flint-controller/src/main.rs:663-700`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controller/src/main.rs#L663-L700) the snapshot in-flight guard
- [`crates/flint-controller/src/main.rs:918-965`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controller/src/main.rs#L918-L965) tick starts the snapshot thread
- [`crates/flint-backup/src/main.rs:36-212`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-backup/src/main.rs#L36-L212) die, stage_set, call
- [`crates/flint-backup/src/main.rs:213-650`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-backup/src/main.rs#L213-L650) verify, restore, restore_ns
- [`crates/flint-backup/src/main.rs:782-1200`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-backup/src/main.rs#L782-L1200) schedule, run, record
- [`crates/flint-backup/src/s3.rs:455-612`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-backup/src/s3.rs#L455-L612) read_response: HTTP errors and bodies
- [`crates/flint-backup/src/lib.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-backup/src/lib.rs) the set format and its integrity check
- [`crates/flint-sched/src/lib.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-sched/src/lib.rs) overlap prevention and drift

**Questions to answer**

- If a checkpoint fails partway, is the partial directory removed, and can `LATEST` ever name it?
- Can a prune error, or pruning under disk pressure, delete a snapshot that a rewind or a rejoining replica still needs?
- Does restore verify every file before the node serves restored data? What happens on an S3 5xx or a truncated body mid-download?
- Can a namespace restore that fails halfway leave the namespace partly restored and serving?
- Does a failed scheduled backup reach an operator, or only a log line?

**Rules and history:** [ADR-0011](adr/0011-backup-and-restore.md), [BUG-0058](bugs/0058-a-tier-that-is-already-down-fails-the-job-at-filesystem-init.md), [BUG-0073](bugs/0073-a-panicking-snapshot-thread-wedges-a-pair-forever.md), [BUG-0163](bugs/0163-rewind-snapshots-are-never-pruned.md), [BUG-0195](bugs/0195-snapshot-retention-filled-the-disk-and-every-write-was-refused.md), [BUG-0260](bugs/0260-a-failed-tenant-wipe-could-not-be-finished.md), [BUG-0275](bugs/0275-every-snapshot-copies-a-manifest-that-never-rolls-over.md), [space-reclaim.md](space-reclaim.md)

## E. Client connections and the wire protocol

**P2** · ≈ 3,300 lines · reviewer: network servers, the Redis protocol

Reading and writing RESP, the per-connection loop, the loading state, connection caps, and TLS. Errors here decide whether a bad client gets a clear refusal or takes resources from good ones.

**Where to look**

- [`crates/flint-resp/src/lib.rs:898-1078`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-resp/src/lib.rs#L898-L1078) decode, decode_at, find_crlf, parse_int
- [`crates/flint-resp/src/lib.rs:601-897`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-resp/src/lib.rs#L601-L897) reply, parse_hello, encode
- [`crates/flint-server/src/main.rs:3772-3906`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L3772-L3906) answer_over_cap, admit_to_reserve, read_one_command, read_first_command
- [`crates/flint-server/src/main.rs:3907-4198`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L3907-L4198) accept_while_loading, serve_loading
- [`crates/flint-server/src/main.rs:4286-4716`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L4286-L4716) serve: the connection loop
- [`crates/flint-server/src/main.rs:5561-5638`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L5561-L5638) read_admission, admit_collection_read
- [`crates/flint-tls/src/lib.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-tls/src/lib.rs) mesh TLS config and streams
- [`crates/flint-tls/src/aio.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-tls/src/aio.rs) async TLS

**Questions to answer**

- On a malformed or oversized frame, is the client told why and disconnected, without allocating the size the frame claims?
- When a client disconnects mid-pipeline or mid-reply, is nothing left locked or queued on its behalf?
- Is output to a slow reader bounded?
- While a node is loading, can any command read partial data?
- Do TLS handshake and certificate failures log the peer and release the worker?

**Rules and history:** [ADR-0029](adr/0029-separate-the-read-lane-from-the-write-lane.md), [BUG-0184](bugs/0184-an-error-reply-could-carry-a-raw-newline-and-hang-the-client.md), [security.md](security.md)

## F. Proxy: routing, retries and backend failures

**P2** · ≈ 1,900 lines · reviewer: network and distributed systems

How the proxy finds the right pair, follows MOVED, rediscovers a master after a failure, retries, and fans out. The main risks are retrying a write that already applied, and answering one client with another's reply.

**Where to look**

- [`crates/flint-proxy/src/main.rs:719-851`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/main.rs#L719-L851) try_reserve_family, coproc_call, route, learn_moved
- [`crates/flint-proxy/src/main.rs:977-1141`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/main.rs#L977-L1141) rediscover_after_failure, still_master, rediscover_for, apply_promote_hint
- [`crates/flint-proxy/src/main.rs:1398-1513`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/main.rs#L1398-L1513) call_raw, discover_master
- [`crates/flint-proxy/src/main.rs:1605-1885`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/main.rs#L1605-L1885) call, lease, call_private, call_slow, route_key, repair_reply
- [`crates/flint-proxy/src/main.rs:1886-2228`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/main.rs#L1886-L2228) forward_collect, forward, refresh_pair_master, fan_out
- [`crates/flint-proxy/src/apool.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/apool.rs) the async backend pool
- [`crates/flint-proxy/src/errors.rs:44-121`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/errors.rs#L44-L121) classify, observe, report

**Questions to answer**

- Which backend errors are retried, how often, and is a non-idempotent write ever retried after it may have applied?
- If a backend connection breaks mid-reply, can the next request on it read the rest of that reply?
- When one pair fails during a fan-out (cross-slot MGET, an FT query), does the client get an error, or a partial answer that looks complete?
- Are broken pooled connections dropped rather than reused?
- Is the resend of a `-INDEXED` refusal to the co-processor limited to writes outside a transaction, and sent once?

**Rules and history:** [ADR-0020](adr/0020-proxy-backend-multiplexing.md), [ADR-0021](adr/0021-proxy-async-worker-model.md), [ADR-0048](adr/0048-cross-slot-mget-through-the-proxy.md), [ADR-0049](adr/0049-cross-slot-rename-and-spring-sessions-login.md), [BUG-0041](bugs/0041-one-client-error-escaped-the-retry-budget-during-a-promotion.md), [BUG-0088](bugs/0088-failover-drill-sheds-on-the-shipped-write-deadline.md), [BUG-0098](bugs/0098-the-proxy-forwards-the-one-reply-that-means-retry-me.md), [BUG-0199](bugs/0199-the-proxy-leaked-every-private-connection-and-the-leak-failed-over-a-serving-master.md), [BUG-0200](bugs/0200-a-family-command-blocked-its-proxy-worker.md), [retry-safety.md](retry-safety.md)

## G. Proxy: client sessions

**P2** · ≈ 3,400 lines · reviewer: network servers, multi-tenant systems

One client's session through the proxy: auth, quotas, MULTI/EXEC pinned to a backend, pub/sub, the near cache, and the control-plane snapshot that names tenants.

**Where to look**

- [`crates/flint-proxy/src/main.rs:1142-1365`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/main.rs#L1142-L1365) apply_snapshot, quota_gate
- [`crates/flint-proxy/src/main.rs:2229-2753`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/main.rs#L2229-L2753) is_internal_only, auth_step
- [`crates/flint-proxy/src/main.rs:2754-3307`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/main.rs#L2754-L3307) serve_client
- [`crates/flint-proxy/src/main.rs:3308-3690`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/main.rs#L3308-L3690) reset, abort_txn, refuse_in_txn, call_pinned, transaction_step
- [`crates/flint-proxy/src/main.rs:3740-3988`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/main.rs#L3740-L3988) family_command, family_grant, FT paging
- [`crates/flint-proxy/src/pubsub.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/pubsub.rs) pub/sub fan-in
- [`crates/flint-proxy/src/cache.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/cache.rs) near cache

**Questions to answer**

- Does an auth failure look the same whether the token is unknown or wrong, and is it counted?
- If a control-plane snapshot fails to apply, does the proxy keep serving the last good one?
- If the pinned backend dies between MULTI and EXEC, does the client get `EXECABORT` rather than a partial apply?
- When a backend is lost, are subscribers told, or do they silently stop receiving?
- If an invalidation cannot be delivered, is the near-cache entry dropped rather than served stale?

**Rules and history:** [ADR-0012](adr/0012-same-slot-transactions.md), [ADR-0030](adr/0030-fleet-growth-and-a-tenants-proxy-subset.md), [ADR-0031](adr/0031-invalidating-a-near-cache-entry-the-proxy-did-not-write.md), [ADR-0053](adr/0053-transactions-across-slots-measured.md), [BUG-0081](bugs/0081-a-benign-read-timeout-rotates-the-proxys-control-plane-seat.md), [BUG-0202](bugs/0202-cpwatch-has-no-keepalive-so-idle-proxies-rotate-and-the-control-plane-leaks-a-thread-per-rotation.md), [BUG-0264](bugs/0264-the-proxy-liveness-check-was-a-refused-operator-command.md), [tenant-guide.md](tenant-guide.md)

## H. Scripts and transactions

**P2** · ≈ 3,800 lines · reviewer: Rust, Lua and Redis semantics

EVAL and EVALSHA in the embedded Lua VM, the key guard that limits what a script may write, and MULTI/EXEC/WATCH. A missed error here applies half a script or half a transaction.

**Where to look**

- [`crates/flint-server/src/script.rs:136-390`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/script.rs#L136-L390) compile, sandbox, build
- [`crates/flint-server/src/script.rs:391-727`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/script.rs#L391-L727) run, give_back, execute
- [`crates/flint-server/src/script.rs:754-956`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/script.rs#L754-L956) redis_call, err_table, to_lua, to_reply
- [`crates/flint-server/src/script.rs:1127-1279`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/script.rs#L1127-L1279) refusal, KeyGuard and refuses_write
- [`crates/flint-server/src/script_libs.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/script_libs.rs) cjson, cmsgpack, struct, bit: decoding script input
- [`crates/flint-server/src/main.rs:4717-4949`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L4717-L4949) transaction_control
- [`crates/flint-server/src/main.rs:5227-5345`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L5227-L5345) exec_transaction, commit_watched
- [`crates/flint-server/src/commands.rs:122-200`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/commands.rs#L122-L200) queue_time_error: what poisons a transaction
- [`crates/flint-commands/src/scripts.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-commands/src/scripts.rs) script key classification

**Questions to answer**

- When a script errors after some writes, are all of its buffered writes discarded? Check the interplay with `pcall` and `redis.error_reply`.
- What limits a runaway script (time, memory), and what happens to its connection and locks when it is stopped?
- Can any path in `script_libs.rs` panic on input a script controls?
- Can one command failing at EXEC leave the rest of the transaction applied?
- If the storage write in `commit_watched` fails, what does the client see, and is WATCH state cleared?

**Rules and history:** [ADR-0012](adr/0012-same-slot-transactions.md), [ADR-0051](adr/0051-rate-limiters-measured-and-running-lua.md), [ADR-0052](adr/0052-job-queues-on-flint-measured.md), [ADR-0053](adr/0053-transactions-across-slots-measured.md), [ADR-0060](adr/0060-redisearch-ft-vector-surface.md), [BUG-0181](bugs/0181-a-cross-slot-mset-in-multi-failed-alone-and-the-rest-applied.md), [BUG-0189](bugs/0189-a-script-longer-than-4-kib-was-refused-as-an-oversized-key.md), [command-support.md](command-support.md)

## I. Slot migration and rebalancing

**P2** · ≈ 2,300 lines · reviewer: distributed systems

Moving slots between pairs: the copy, the phase records, the cut-over, the purge of the source, and recovery after a crash at any step. A mistake here leaves a slot owned by no pair, or by two.

**Where to look**

- [`crates/flint-server/src/migrate.rs:276-674`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/migrate.rs#L276-L674) migrate_in
- [`crates/flint-server/src/migrate.rs:675-868`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/migrate.rs#L675-L868) ask_source_phase, classify_source_phase, set_slot_phase, purge_slot_rows
- [`crates/flint-server/src/migrate.rs:1135-1267`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/migrate.rs#L1135-L1267) call_retrying, call_once_with
- [`crates/flint-storage/src/migration.rs:42`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/migration.rs#L42) migrate_slots
- [`crates/flint-storage/src/manifest.rs:126-212`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/manifest.rs#L126-L212) is_inflight, needs_recovery, slot_ownership
- [`crates/flint-storage/src/manifest.rs:293-415`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/manifest.rs#L293-L415) migration records and their scans
- [`crates/flint-controller/src/main.rs:1767-2122`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controller/src/main.rs#L1767-L2122) has_inflight_migration, execute_move, recover_migrations
- [`crates/flint-balance/src/lib.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-balance/src/lib.rs) the rebalance planner

**Questions to answer**

- If the source, the target or the controller dies at each phase, is recovery idempotent, and can a slot end up owned by no pair or by two?
- Can `purge_slot_rows` run after a copy that did not complete?
- Which errors does `call_retrying` retry, and is a batch that was partly copied safe to send again?
- Malformed migration rows are skipped. Is a skip logged, so a corrupt record is noticed?

**Rules and history:** [ADR-0023](adr/0023-slot-aligned-bulk-eviction.md), [ADR-0030](adr/0030-fleet-growth-and-a-tenants-proxy-subset.md), [architecture.md](architecture.md)

## J. Commands and data types

**P3** · ≈ 17,000; split by family lines · reviewer: Redis and Valkey semantics

The refusals every command gives for bad arguments, wrong types and limits, and the multi-step commands that must validate before they write. The main risk is a wrong answer without an error.

**Where to look**

- [`crates/flint-server/src/commands.rs:346-611`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/commands.rs#L346-L611) Dispatcher
- [`crates/flint-server/src/commands.rs:612`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/commands.rs#L612) dispatch, and the per-command arms after it
- [`crates/flint-server/src/commands/streams.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/commands/streams.rs) streams
- [`crates/flint-server/src/commands/streams/groups.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/commands/streams/groups.rs) consumer groups
- [`crates/flint-server/src/commands/geo.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/commands/geo.rs) GEO
- [`crates/flint-server/src/commands/json.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/commands/json.rs) JSON
- [`crates/flint-server/src/json_path.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/json_path.rs) JSONPath
- [`crates/flint-storage/src/strings.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/strings.rs) strings, also hashes.rs, lists.rs, sets.rs, zsets.rs
- [`crates/flint-storage/src/hll.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/hll.rs) HyperLogLog, also bloom.rs, json.rs, streams.rs
- [`crates/flint-commands/src/lib.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-commands/src/lib.rs) the read/write classifier

**Questions to answer**

- Does each refusal match Valkey's text and kind (`WRONGTYPE` or `ERR`)? Which ones does the conformance oracle not cover?
- Are integer overflow, NaN, infinity and out-of-range arguments refused rather than wrapped or stored?
- Do multi-key and multi-step commands (the STORE variants, RENAME, BF.INSERT) validate everything before their first write?
- Can a command misclassified as a read in `flint-commands` write on a replica?

**Rules and history:** [ADR-0025](adr/0025-stream-collection-reads-instead-of-materialising-them.md), [ADR-0054](adr/0054-jsonpath-multi-match.md), [ADR-0055](adr/0055-the-rest-of-the-json-family.md), [BUG-0103](bugs/0103-the-matrix-promised-every-command-was-gated-and-a-1000x-unit-error-passed.md), [BUG-0185](bugs/0185-expire-refused-redis-7s-nx-xx-gt-lt.md), [BUG-0208](bugs/0208-json-numincrby-stored-a-wrong-integer-without-an-error.md), [BUG-0213](bugs/0213-arguments-redis-refuses-were-accepted.md), [BUG-0225](bugs/0225-small-refusals-and-error-texts-that-differed-from-valkeys.md), [BUG-0230](bugs/0230-sorted-set-refusals-and-a-minus-zero-reply-that-differed-from-upstream.md), [BUG-0236](bugs/0236-json-errors-were-flints-words-not-redisjsons.md), [BUG-0237](bugs/0237-a-bloom-batch-hid-a-partial-write-behind-one-error.md), [BUG-0240](bugs/0240-bloom-argument-errors-were-flints-words-and-order.md), [BUG-0251](bugs/0251-a-store-refused-for-its-size-deleted-its-destination-first.md), [command-support.md](command-support.md)

## K. Vector and FT co-processor

**P3** · ≈ 5,600 of service code, ≈ 9,000 of parsers lines · reviewer: Rust concurrency, search

flint-vec: the VEC and FT families, its channel to the pairs, rebuild after a restart, and persistence; plus the prefix rows the data nodes keep so writes cannot bypass an index.

**Where to look**

- [`crates/flint-vec/src/main.rs:303-598`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-vec/src/main.rs#L303-L598) claim_vec_dir, serve, handle_flintfam, open_channel
- [`crates/flint-vec/src/main.rs:599-1041`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-vec/src/main.rs#L599-L1041) rebuild_chunk, install, perform_persist, run_persist
- [`crates/flint-vec/src/main.rs:1042-1097`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-vec/src/main.rs#L1042-L1097) send_cmd, read_reply
- [`crates/flint-vec/src/ftchan.rs:126-520`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-vec/src/ftchan.rs#L126-L520) call, read_doc, aggregate, search, write
- [`crates/flint-vec/src/ftchan.rs:521-647`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-vec/src/ftchan.rs#L521-L647) fill_chunk, run_fill, lend_to_fill
- [`crates/flint-vec/src/ftstore.rs:835-897`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-vec/src/ftstore.rs#L835-L897) plan_ft_write
- [`crates/flint-vec/src/lib.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-vec/src/lib.rs) the VEC family
- [`crates/flint-server/src/indexed.rs:68-151`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/indexed.rs#L68-L151) load, of, under, refusal: the node's prefix rows

**Questions to answer**

- There are 32 `lock().expect` calls (ftchan.rs 18, main.rs 10, ftstore.rs 4). A panic while one of those locks is held poisons it, and every later call on it panics. Is a panic reachable from tenant input or from a pair's reply?
- If a write reached the pair but its read-back or indexing failed, how long is the index behind, and who notices?
- On rebuild, rows of the wrong length are skipped. Are skips counted and reported?
- If the channel to a pair is lost mid-write, does the client get `COPROCUNAVAIL`, and can its retry apply the write twice?
- Can a failed persist leave the durable index state behind memory with no record of it?

**Rules and history:** [ADR-0059](adr/0059-vector-search-filtered-by-metadata.md), [ADR-0060](adr/0060-redisearch-ft-vector-surface.md), [ADR-0061](adr/0061-vector-range-queries.md), [ADR-0062](adr/0062-geo-fields-in-ft.md), [ADR-0063](adr/0063-full-text-search-in-ft.md), [ADR-0064](adr/0064-ft-aggregate-and-cursors.md), [BUG-0200](bugs/0200-a-family-command-blocked-its-proxy-worker.md), [BUG-0203](bugs/0203-flint-vec-ignored-bind-and-listened-on-every-interface.md), [BUG-0265](bugs/0265-a-rebuild-installed-the-rows-a-tenant-planted-unchecked.md), [BUG-0271](bugs/0271-a-backfill-moved-written-documents-out-of-write-order.md), [vectors.md](vectors.md)

## L. Startup, configuration and shutdown

**P2** · ≈ 2,500 lines · reviewer: Rust, systems

Flags, environment knobs, opening an existing data directory, marker files, and leaving the process. The main risk is starting with a setting the operator did not ask for, without saying so.

**Where to look**

- [`crates/flint-server/src/main.rs:162-212`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L162-L212) hard_exit, reject_unknown_flags, arg
- [`crates/flint-server/src/main.rs:372-404`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L372-L404) internal_call_once
- [`crates/flint-server/src/main.rs:2142-3771`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-server/src/main.rs#L2142-L3771) main: about 1,600 lines of startup
- [`crates/flint-storage/src/rocks.rs:276-280`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-storage/src/rocks.rs#L276-L280) env_u64: an unparsable value is ignored
- [`crates/flint-proxy/src/main.rs:3717-3722`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-proxy/src/main.rs#L3717-L3722) parse_u64_knob: panics on a bad value
- [`crates/flint-vec/src/main.rs:167-302`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-vec/src/main.rs#L167-L302) main
- [`crates/flint-controller/src/main.rs:129-164`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controller/src/main.rs#L129-L164) arg, arg_or
- [`crates/flint-controller/src/main.rs:1499-1723`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controller/src/main.rs#L1499-L1723) main
- [`crates/flint-controlplane/src/main.rs:1753-1862`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controlplane/src/main.rs#L1753-L1862) main

**Questions to answer**

- Does a malformed flag value refuse to start, or fall back to the default without a word? flint-server's main.rs has 47 `.parse().ok()` calls, several on flags, while the proxy's `parse_u64_knob` panics instead. Which behaviour is intended?
- `env_u64` ignores a value it cannot parse (`FLINT_BG_JOBS=four`). Should it refuse?
- Opening a data directory that is corrupt, half written, or from a newer format: does the node refuse with a clear message?
- On SIGTERM with writes in flight, are acknowledged writes durable, and does the process close RocksDB or leave through `hard_exit`?

**Rules and history:** [ADR-0024](adr/0024-boot-decision-counters-that-outlive-the-process.md), [BUG-0089](bugs/0089-a-failed-bring-up-is-not-fatal-and-its-reason-is-discarded.md), [BUG-0099](bugs/0099-node-env-accepts-a-name-no-seat-reads-and-says-nothing.md), [self-hosting.md](self-hosting.md)

## M. Operator tooling and the control-plane service

**P3** · ≈ 15,000 lines · reviewer: operations tooling, Rust

flintctl's lifecycle verbs (bootstrap, roll, upgrade, failover, verify, status), its remote calls, the control-plane service's tenant registry, the fleet journal and the exporter. Errors here mislead operators rather than clients.

**Where to look**

- [`crates/flint-ctl/src/main.rs:647-1366`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-ctl/src/main.rs#L647-L1366) call, call_slow, probe_target, call_seq, call_to, wait_pong
- [`crates/flint-ctl/src/main.rs:2466-2910`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-ctl/src/main.rs#L2466-L2910) spawn, spawn_node, seat_probe, wait_port_free
- [`crates/flint-ctl/src/main.rs:4653-5508`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-ctl/src/main.rs#L4653-L5508) call_cp, start_controller, bootstrap
- [`crates/flint-ctl/src/main.rs:5509-6379`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-ctl/src/main.rs#L5509-L6379) verify, verify_checks
- [`crates/flint-ctl/src/main.rs:7865-9334`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-ctl/src/main.rs#L7865-L9334) roll_gate, roll_node, controlled_failover, failover, roll_abort, upgrade
- [`crates/flint-ctl/src/main.rs:9848-10104`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-ctl/src/main.rs#L9848-L10104) host_spawn_under_systemd, host_command
- [`crates/flint-controlplane/src/main.rs:232-1446`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controlplane/src/main.rs#L232-L1446) handle: every control-plane verb
- [`crates/flint-controlplane/src/state.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-controlplane/src/state.rs) state
- [`crates/flint-journal/src/lib.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-journal/src/lib.rs) the best-effort event emitter
- [`crates/flint-exporter/src/main.rs`](https://github.com/Crestway-AI-LLC/flint-cache/blob/2c2c6281e386a1ab7a9908f516d7ed618db494c9/crates/flint-exporter/src/main.rs) Prometheus exporter

**Questions to answer**

- Does every remote call tell a failed ssh from an answer?
- When a roll or upgrade step fails, does it stop and leave the fleet serving?
- Does every `verify` check fail when it could not examine what it names?
- Are control-plane mutations validated before they are stored, and can a multi-row mutation fail halfway?
- Does the journal count the events it drops? Does the exporter show a failed scrape as missing rather than zero?

**Rules and history:** [ADR-0014](adr/0014-introspection-status-surface.md), [ADR-0028](adr/0028-a-verdict-must-name-what-it-examined.md), [ADR-0032](adr/0032-one-implementation-of-every-control-plane-mutation.md), [BUG-0083](bugs/0083-the-build-read-has-one-attempt-and-every-failure-looks-the-same.md), [BUG-0087](bugs/0087-upgrade-re-rolls-what-is-already-rolled.md), [BUG-0089](bugs/0089-a-failed-bring-up-is-not-fatal-and-its-reason-is-discarded.md), [BUG-0102](bugs/0102-three-more-remote-call-sites-read-a-failed-ssh-as-an-answer.md), [BUG-0119](bugs/0119-flintctl-panicked-on-a-master-that-was-merely-mid-promotion.md), [BUG-0159](bugs/0159-verify-has-no-controller-assertion-so-a-fleet-with-no-failover-passes.md), [BUG-0160](bugs/0160-the-raft-control-plane-accepts-what-single-node-refuses.md), [BUG-0264](bugs/0264-the-proxy-liveness-check-was-a-refused-operator-command.md), [release-checklist.md](release-checklist.md)

## N. Cross-cutting sweep

**P1** · whole tree · reviewer: anyone with grep and patience

The patterns that turn an error into silence, counted per crate. Each one is fine when it is deliberate and says why. The sweep asks that question of every site, starting with the crates that serve data.

| Crate | Lines | `expect` | `let _ =` | `.ok()` | `unwrap_or*` | `panic!` and kin | `eprintln!` |
|---|---|---|---|---|---|---|---|
| flint-server | 24,634 | 18 | 42 | 120 | 152 | 6 | 113 |
| flint-vec | 15,407 | 43 | 11 | 62 | 66 | 6 | 16 |
| flint-storage | 12,104 | 3 | 8 | 51 | 67 | 0 | 3 |
| flint-ctl | 10,635 | 54 | 19 | 63 | 93 | 28 | 155 |
| flint-proxy | 7,306 | 0 | 13 | 42 | 44 | 1 | 6 |
| flint-controlplane | 5,421 | 9 | 9 | 47 | 40 | 4 | 6 |
| flint-backup | 2,334 | 2 | 9 | 16 | 69 | 0 | 3 |
| flint-controller | 2,135 | 4 | 14 | 12 | 16 | 1 | 37 |
| flint-tls | 1,143 | 7 | 3 | 13 | 3 | 0 | 2 |
| flint-resp | 1,078 | 2 | 0 | 2 | 1 | 2 | 0 |
| others (7 crates) | 2,193 | 4 | 6 | 15 | 18 | 0 | 3 |
| **Total** | **84,390** | **146** | **134** | **443** | **569** | **48** | **344** |

Regex counts over non-test code at the pinned commit; test modules, test files, benches and the chaos and conformance harnesses are left out. `panic!` and kin includes `unreachable!`, `todo!` and `unimplemented!`. A count is where to start, not a finding.

**Questions to answer**

- Each `let _ =` on a `Result`: is dropping the error deliberate, and does a comment say why?
- Each `.ok()` or `unwrap_or*` on a fallible call: is the failure logged or counted somewhere, or does it become a default value?
- Each `expect`: is it an invariant the code itself guarantees, or can input, the disk or a peer reach it?
- Each `panic!` and `unreachable!` on a serving path: can a client, a peer or a corrupt row reach it?
- Locks: does each crate either recover poisoned locks or make sure no panic can happen while one is held?
- Is each `eprintln!` on an error path matched by a counter an alert can watch? A log line no one reads is how BUG-0073 and BUG-0163 stayed unseen.

**Commands to start**

```bash
git grep -n 'let _ =' -- 'crates/*/src/*.rs'
git grep -nE '\.ok\(\)' -- 'crates/flint-server/src' 'crates/flint-storage/src'
git grep -n 'lock().expect' -- 'crates/*/src'
git grep -nE 'panic!|unreachable!' -- 'crates/*/src'
```

**Rules and history:** [ADR-0028](adr/0028-a-verdict-must-name-what-it-examined.md), [BUG-0073](bugs/0073-a-panicking-snapshot-thread-wedges-a-pair-forever.md), [BUG-0083](bugs/0083-the-build-read-has-one-attempt-and-every-failure-looks-the-same.md), [BUG-0089](bugs/0089-a-failed-bring-up-is-not-fatal-and-its-reason-is-discarded.md), [BUG-0163](bugs/0163-rewind-snapshots-are-never-pruned.md)

Line ranges run from the first function named to the line before the next region, so a range can include a helper or two around the functions it names.
