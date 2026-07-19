# Flint

**A durable, disk-first cache that speaks Redis.** Flint keeps your working
set on local NVMe (RocksDB, Kvrocks-style encoding) instead of holding it
hostage in RAM — so it survives restarts, replicates with a bounded loss
window, fails over in seconds, and costs storage economics instead of memory
economics. Clients connect with any Redis client; no SDK, no new protocol.

## What's in this repository

Everything needed to run AND operate Flint yourself:

- **`flint-server`** — the data-plane node: RESP2 over TCP, strings / hashes /
  sets / lists / sorted sets with TTLs, an in-memory engine for development
  and the RocksDB engine for production, WAL-based async replication with
  lag-capped back-pressure, checkpoint full sync, epoch-fenced promotion,
  warm restarts.
- **`flint-proxy`** — the scale-out layer: routes keys across shard pairs on
  the Redis Cluster 16,384-slot space, token auth, TLS termination at the
  edge, mutual TLS to backends, admission control, a bounded near-cache,
  per-tenant latency histograms.
- **`flint-controlplane`** — the topology registry: pairs, slot ranges,
  tenants and their (hashed-at-rest) tokens, quotas, opt-ins; pushes
  versioned snapshots to proxies; runs durable single-node or Raft-replicated
  (openraft) for HA; self-service token rotation with dual-version windows.
- **`flint-ctl`** — cluster lifecycle from one inventory file: bootstrap
  (with full mTLS cert minting), expand, add-replica, swap-node, tenant
  management, canary upgrades, rotate-certs / rotate-admin.
- **`flint-controller`** — automatic failover: detect → verify → promote →
  fence, with leases so a partitioned master self-demotes.
- **`flint-storage`** — the engine: envelope encoding, typed stores, GC,
  replication primitives, manifests and epochs, over a swappable `Kv` seam.
- **`flint-conformance`** — the compatibility oracle. Every command lands with
  a corpus case validated against a real Valkey **and** both Flint engines;
  CI gates on it.
- **`flint-bench`**, **`flint-chaos`** — the honesty tools: p99.9-under-
  compaction benchmarks and a random-kill chaos harness with a ledger oracle.
- **`flint-tls`**, **`flint-resp`**, **`flint-slot`**, **`flint-commands`**,
  **`flint-journal`** — TLS with hot-reloading certificates, the RESP codec,
  slot hashing, the shared read/write command classifier, and the typed fleet
  event log.

`tools/` holds the drill scripts — end-to-end proofs (replication parity,
failover RTO, slot migration and cutover, quota enforcement, token and
certificate rotation under live traffic, chaos) that run against real
processes, not mocks.

## Quick start

```sh
cargo build --release --features flint-server/rocks

# One durable node
./target/release/flint-server --port 6400 --engine rocks --data-dir ./data

# Talk to it with any Redis client
valkey-cli -p 6400 SET hello world
valkey-cli -p 6400 GET hello
```

A full cluster — control plane, shard pairs, proxy, automatic failover,
mutual TLS — from one inventory file:

```sh
cat > cluster.flint <<EOF
statedir ./state
bins ./target/release
tls on
cp 127.0.0.1:7500
pair 127.0.0.1:7001,127.0.0.1:7002
proxy 127.0.0.1:7379
controller on
EOF
./target/release/flintctl -f cluster.flint bootstrap
./target/release/flintctl -f cluster.flint tenant add acme <token> acme 1
valkey-cli -p 7379 -a <token> SET hello world
```

The drills in `tools/` are runnable, asserting examples of every topology
and failure mode (`repl_drill.sh`, `failover_drill.sh`, `proxy_drill.sh`,
`slot_migrate_drill.sh`, `tenant_quota_drill.sh`, `token_rotation_drill.sh`,
`cert_reload_fleet_drill.sh`, `chaos_drill.sh`, …).

## Verifying

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
# and the production engine configuration:
cargo clippy --workspace --all-targets --features flint-server/rocks -- -D warnings
cargo test --workspace --features flint-server/rocks
```

Conformance against a local Valkey:

```sh
./target/release/flint-conformance --target 127.0.0.1:6400
```

## License

Elastic License 2.0 (Elastic-2.0). In plain terms: free to use, copy, modify,
and redistribute for your own purposes — personal or commercial, at any
company size, including production use behind your own products. The one
restricted use: you may not offer Flint itself to third parties as a hosted
or managed service. Full terms in [LICENSE](LICENSE).

The managed, serverless Flint — fleet autonomy (metering, billing, automated
incident response) and the tenant/operator consoles — is operated by
**Crestway AI LLC** on top of this stack.

## Contributing

Issues and pull requests are welcome. Contributions require agreeing to the
project CLA (see [CONTRIBUTING.md](CONTRIBUTING.md)) so the project retains
clear licensing. Every change must pass the gates above — including a
conformance corpus entry for any new command, validated against the Valkey
oracle.
