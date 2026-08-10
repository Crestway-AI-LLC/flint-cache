# Flint

**A persistent, multi-threaded, multi-tenant cache that speaks Redis and
never evicts.** Flint keeps your working set on local NVMe (RocksDB,
Kvrocks-style encoding) instead of holding it hostage in RAM — so it
survives restarts, replicates with a bounded loss window, fails over in
seconds, and costs storage economics instead of memory economics. Commands
run concurrently on every core rather than queued behind a single thread,
and every tenant lives in its own isolated keyspace behind its own token
and quota. Clients connect with any Redis client; no SDK, no new protocol.

**Fewer misses beats faster hits.** A RAM cache is faster on a hit and that
is not the number your users feel. When the working set does not fit — and
past a few hundred GB it does not, because the managed options cap out —
what they feel is the evictions: every evicted key is a request that falls
through to your origin, and on a hot key it is every concurrent request at
once, the thundering herd that turns a cache miss into an incident. Holding
the whole set costs a few hundred microseconds on the hit and stops the
seconds at the origin. That is the trade this project exists to make, and
[the measured numbers](docs/architecture.md) are there to show the hit is
fast enough that you stop thinking about it.

## Why a disk-first cache — the value in six claims

1. **No eviction, no miss storms.** Nothing is ever dropped because RAM ran
   low, so a hot key can't be evicted into a thundering herd against your
   backend. The failure mode of a full Flint is a refused *write* with a
   clear error — never a silently missing read.
2. **No warm-up.** A restarted node serves its working set immediately from
   disk; failover hands traffic to a replica that has the data; a fresh
   replica seeds from a checkpoint. "The cache is cold, watch the database"
   is not an operational event here.
3. **Hot keys are handled in the architecture.** The proxy tracks per-key
   heat, absorbs hot reads in a bounded short-TTL near-cache at the edge,
   and offers an opt-in async write queue that soaks up write bursts on the
   hot path — while every write still lands in the WAL.
4. **Large datasets at storage cost, without a latency sacrifice your users
   can feel.** A cache call is a network call, and in practice two of them:
   client → proxy, then proxy → cache node. Each hop costs hundreds of
   microseconds (a cross-AZ round trip is ~500 µs–1 ms), and a RAM cache
   behind a proxy crosses the same wires — so both designs pay that part
   identically. The only difference is the last step inside the node, a RAM
   lookup versus an NVMe read: under 200 µs (0.2 ms) at p50 in our own
   same-box measurements against Valkey.
   **That step is the whole of what Flint trades** — everything else on the
   path is identical. What share of your latency it represents depends on
   your topology, so run it against your own hop times; the more network
   between client and cache, the smaller the medium's share. What does not
   depend on topology: a miss costs a full round trip to your origin, and on
   a hot key, all of them at once. Holding 10× the data at NVMe prices is
   what buys that away.
5. **Built to be operated by an agent.** From the first commit, every drill,
   failure claim, and operational lesson is captured in a form software can
   act on — runnable drills in this repo, journaled actions, allowlisted
   remediations that fail closed and page a human on escalation. The managed
   fleet is run that way; the same evidence discipline is what you get here
   for self-hosting.
6. **Multi-threaded execution and real multi-tenancy.** Each connection is
   served on its own thread against a shared store with no global lock, so
   concurrent commands actually run at the same time and a higher-core box
   is faster rather than merely larger — the reason the excluded commands
   (blocking, whole-keyspace, cross-slot) stay excluded is that they would
   put that bottleneck back. Tenancy is enforced rather than agreed: a
   token maps to an isolated keyspace at the proxy, so clients use ordinary
   key names with nothing to prefix, and a bug in one tenant's key naming
   cannot reach another's data. Each tenant carries an ops/second quota and
   a storage cap; crossing the cap sheds *writes* while reads and deletes
   keep working, so the way out is never blocked by the limit itself.

## What's in this repository

Everything needed to run AND operate Flint yourself:

- **`flint-server`** — the data-plane node: RESP over TCP in both dialects
  (RESP2 and RESP3, negotiated per connection with `HELLO`), strings /
  hashes / sets / lists / sorted sets / JSON documents with TTLs, cursor-
  based keyspace `SCAN`, same-slot transactions (`MULTI`/`EXEC`/`WATCH`,
  with [what they guarantee written down](docs/command-support.md)), an
  in-memory engine for development
  and the RocksDB engine for production, WAL-based async replication with
  lag-capped back-pressure, checkpoint full sync, epoch-fenced promotion,
  warm restarts.
- **`flint-proxy`** — the scale-out layer: routes keys across shard pairs on
  the Redis Cluster 16,384-slot space, token auth, TLS termination at the
  edge, mutual TLS to backends, admission control, a bounded near-cache,
  per-tenant latency histograms.
- **`flint-controlplane`** — the topology registry: pairs, slot ranges,
  tenants and their (hashed-at-rest) tokens, quotas, opt-ins; pushes
  versioned snapshots to proxies; runs persistent single-node or Raft-replicated
  (openraft) for HA; self-service token rotation with dual-version windows.
- **`flint-ctl`** — cluster lifecycle from one inventory file: bootstrap
  (with full mTLS cert minting), expand, add-replica, swap-node, graceful
  failover, decommission-node, tenant management, canary upgrades,
  rotate-certs / rotate-admin, and hot config reload (edit the inventory,
  `reload` pushes persistence/RPO/admission knobs to the running fleet — no
  restart).
- **`flint-controller`** — automatic failover: detect → verify → promote →
  fence, with leases so a partitioned master self-demotes.
- **`flint-storage`** — the engine: envelope encoding, typed stores, GC,
  replication primitives, manifests and epochs, over a swappable `Kv` seam.
- **`flint-conformance`** — the compatibility oracle. Every command lands
  with a corpus case validated against a real Valkey **and** both Flint
  engines, under both protocol dialects; CI gates on it. One honest
  exception: stock Redis and Valkey have no JSON type, so the JSON family
  has no reference in that run — those cases are checked separately against
  the RedisJSON module itself (`tools/redisjson_compare.sh`), and the places
  we deliberately differ are written down in
  [docs/command-support.md](docs/command-support.md).
- **`flint-bench`**, **`flint-chaos`** — the honesty tools: p99.9-under-
  compaction benchmarks and a random-kill chaos harness with a ledger oracle.
- **`flint-balance`** — the slot-rebalancing planner: a policy seam plus the
  size-based policy, so a cluster can be levelled by a rule you can read.
- **`flint-backup`** — backup and restore (ADR-0011): per-pair checkpoint
  sets with a checksummed manifest, to a directory or any S3-compatible
  endpoint (SigV4 spoken directly — no SDK, no new dependencies); restore
  that only ever creates, scrubs the copied cluster identity, and proves
  its own scrub; namespace-scoped restore into a live cluster placed by
  current slot ownership; and a `schedule` mode running backup / verify /
  restore-rehearsal as a policy, whose alertable metric is the age of the
  newest set that actually restored — never the run count. `backup-to` in
  the flintctl inventory runs it as a supervised seat.
- **`flint-sched`** — recurring jobs as a library: serial execution (a job
  cannot overlap itself), completion-based cadence (no catch-up bursts),
  monotonic time, capped backoff.
- **`flint-exporter`** — Prometheus metrics for a self-hosted fleet.
- **`flint-tls`**, **`flint-resp`**, **`flint-slot`**, **`flint-commands`**,
  **`flint-journal`** — TLS with hot-reloading certificates, the RESP codec
  (both dialects), slot hashing, the shared read/write command classifier,
  and the typed fleet event log.

## What "persistent" means here, precisely

Flint is a **persistent cache, not a system of record** — and deliberately
not "durable" in the storage-engineering sense of that word, where it
promises an acknowledged write survives any crash. Flint survives
restarts, fails over in seconds, and it *will* lose a bounded amount of
recently-acknowledged data when a master dies — because replication is
asynchronous and the WAL is fsynced on a bounded cadence.

- Warm restart, and the loss of a **replica**: nothing lost.
- Master failover with the replica caught up: nothing lost. Measured RTO
  **p50 506 ms, worst 586 ms** across 5 hosts on a real network (rc.28).
- Master failover with the replica behind: the un-replicated tail may be lost.
  Bounded by **volume** — at most one lag-cap window's worth is ever at risk —
  **not by age**. Measured deepest loss with a deliberately stalled replica:
  **1757 ms**. In healthy runs it is 0.

If your data lives *only* in Flint and losing the last second of it would
matter, run a database as well. Every number above is measured by a drill in
this repository with the command to reproduce it: **[docs/slo.md](docs/slo.md)**.

## What happens when the disk fills — there is no eviction

Flint never evicts an unexpired key. Coming from Redis you might expect a
`maxmemory-policy`; there deliberately isn't one — an eviction policy is a
way of silently losing data the client believed was stored, and this
project's whole posture is that loss should be bounded, loud, and chosen.
Instead, pressure is handled by refusal, in layers:

- **Per-tenant quotas** shed a tenant's writes with `-QUOTA` at their own
  cap, long before the host is at risk.
- **The host disk guard** fires EARLY (defaults: under 10% free or under
  2 GiB, whichever is stricter) — an LSM needs headroom to compact, and a
  disk allowed to actually fill produces a node that cannot dig itself
  out, because freeing space means deleting, deletes are writes, and
  reclaiming the bytes needs the compaction that has no room to run.
  While shedding: space-growing writes are refused with an error that says
  it is the server and not the tenant's cap, **reads serve normally**, and
  the delete path (`DEL`, `UNLINK`, `EXPIRE`, `FLUSHALL`) keeps working so
  the condition can clear itself. The node reopens on its own once space
  returns, with hysteresis so it does not flap. `tools/disk_pressure_drill.sh`
  proves all of this against a genuinely full filesystem, including that
  nothing already written is lost.
- **Space returns** through TTL expiry (a compaction filter reclaims
  expired rows as compaction rewrites them) and on-demand `FLINTCOMPACT`.

The operational consequence: size for the working set and use TTLs. A
cache that is full of unexpired data does not silently shed someone
else's keys to make room for yours — it tells you, and keeps serving
reads while you decide. If you want an eviction-shaped policy anyway,
build it client-side: [docs/space-reclaim.md](docs/space-reclaim.md) is
the watch → rank → delete → verify loop, on primitives
(`FLINTKEYSIZE`/`FLINTKEYSTAMP`, disk-guard journal events) the server
maintains for exactly that.

[docs/security.md](docs/security.md) is the security posture — mutual TLS
everywhere internally, tokens stored as digests, and an explicit list of what
Flint does *not* do (no encryption at rest, no RBAC, no IAM).

[docs/architecture.md](docs/architecture.md) is the system map — the three
planes, and a normal write and read traced end to end.
[docs/failover.md](docs/failover.md) covers failover — master handoff
(planned and crash/partition) and why split-brain is impossible, plus the
stateless proxy-instance failure case.
[docs/self-hosting.md](docs/self-hosting.md) is the deploy-on-your-own-
hardware guide — sizing, the config file, monitoring, users, and
credential rotation.

`tools/` holds the drill scripts — end-to-end proofs (replication parity,
failover RTO, slot migration and cutover, quota enforcement, token and
certificate rotation under live traffic, chaos) that run against real
processes, not mocks.

## Prerequisites

Only if you are building from source. **Tagged releases publish Linux x86_64
binaries plus a `manifest.json` with the sha256** — for a deployment, download
those from the
[Releases page](https://github.com/Crestway-AI-LLC/flint-cache/releases/latest)
and skip this section entirely ([docs/self-hosting.md](docs/self-hosting.md)).

- **Rust 1.85 or newer** (edition 2024). `rustup` recommended; several distro
  toolchains are older and will refuse to build.
- **A C++ toolchain and libclang** — RocksDB is compiled from source by the
  `rocksdb` crate. Debian/Ubuntu: `build-essential clang libclang-dev`.
  RHEL/Amazon Linux: `gcc-c++ clang clang-devel`. macOS: Xcode command line
  tools.
- **`valkey-cli`** (or `redis-cli`) for the examples below, and for the drills.

The first build compiles RocksDB and takes a while — on the order of ten
minutes on a laptop, and a couple of GB in `target/`.

**Or skip all of it and open the repo in the dev container**
([`.devcontainer/`](.devcontainer/)) — it carries the toolchain, libclang, and
the pinned Valkey the conformance oracle needs, so `tools/gates.sh` runs there
too.

## Quick start

One command brings up a real cluster — control plane, a replicated pair, a
routing proxy and the failover controller — on your laptop:

```sh
tools/quickstart.sh
```

It checks prerequisites and names anything missing, builds if needed, writes a
throwaway inventory, bootstraps, adds a tenant, and proves the cluster serves a
write through the proxy. Then `tools/quickstart.sh down` (keep the data) or
`purge` (delete it). Deliberately a pair rather than a single node, so
`tools/quickstart.sh failover` can SIGKILL the master and let you watch the
controller promote the replica — with a replicated witness key proving the
data came through.

The rest of this section is the same thing by hand, if you would rather see the
pieces.

```sh
cargo build --release --features flint-server/rocks,flint-backup/rocks

# One persistent node
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
disposable on
cp 127.0.0.1:7500
pair 127.0.0.1:7001,127.0.0.1:7002
proxy 127.0.0.1:7379
controller on
EOF
./target/release/flintctl -f cluster.flint bootstrap
./target/release/flintctl -f cluster.flint tenant add acme tok-acme acme 1
valkey-cli -p 7379 -a tok-acme SET hello world
```

**`disposable on` is doing real work there, and you should take it off for
anything you intend to keep.** A build straight from source reports no
release version, carries no manifest and no checksums, so `flintctl` refuses
to *mutate* a fleet with it — bootstrap, upgrade, failover, tenant changes —
unless the inventory admits the fleet is throwaway. Read-only verbs (`status`,
`verify`) are always allowed.

To self-host for real, stamp the build with the version you are deploying and
drop the line:

```sh
FLINT_RELEASE_TAG=v0.1.0 cargo build --release --features flint-server/rocks,flint-backup/rocks
```

The tag is baked in at compile time, so every binary reports it and `verify`
can hold the whole fleet to a single build — which is the property the guard
exists to protect. [docs/self-hosting.md](docs/self-hosting.md) covers this
and the rest of a production install.

The drills in `tools/` are runnable, asserting examples of every topology
and failure mode (`repl_drill.sh`, `failover_drill.sh`, `proxy_drill.sh`,
`slot_migrate_drill.sh`, `tenant_quota_drill.sh`, `token_rotation_drill.sh`,
`cert_reload_fleet_drill.sh`, `chaos_drill.sh`, …).

## Verifying

One command runs the whole gate, keeps every log, and its exit status is the
answer:

```sh
tools/gates.sh
```

Stages run individually too — `tools/gates.sh check`, `conformance`, `drills`,
`chaos`:

| Stage | What it runs |
|---|---|
| `check` | fmt, clippy and tests, in **both** feature configurations |
| `conformance` | the compatibility oracle against Valkey, Flint mem, Flint rocks |
| `drills` | the core drills — real processes, no mocks. The `CORE` list in `tools/gates.sh` is the count; enumerating it here only drifts (it read 20 while the gate ran 39). |
| `chaos` | the two randomized kill-and-verify drills |

Logs land in `$FLINT_GATE_LOGS` (default `/tmp/flint-gates`), one file per
step, kept whether it passed or failed. `conformance` needs a local Valkey to
compare against; `drills` need `valkey-cli`.

That script *is* the release gate — [docs/release-checklist.md](docs/release-checklist.md)
stays as the explanation of why each step exists, because a checklist that has
to be retyped is a checklist with steps missing.

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
oracle (or, for commands Valkey does not have, against the module that
defines them — see [docs/adr/README.md](docs/adr/README.md) for how this
repository relates to the managed plane).
