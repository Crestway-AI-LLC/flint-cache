# Self-hosting Flint

Running the open Flint stack on your own hardware — sizing, configuration,
monitoring, users, and credential rotation. Everything here uses only the
open (ELv2) binaries; the managed autonomy plane (metering, billing,
turnkey dashboards, portals) is Crestway's and is called out where it
would otherwise be assumed.

The one tool you drive is **`flintctl`**: one inventory file describes the
cluster, and `flintctl` makes it so. See its module header for the full
verb and inventory reference; this guide is the operator's version of it.

## Quick start (single box)

```sh
cargo build --release --features flint-server/rocks   # or use a release bundle

cat > cluster.flint <<'EOF'
statedir ./state
bins ./target/release
tls on
client-tls on
cp 127.0.0.1:7500
pair 127.0.0.1:7001,127.0.0.1:7002
proxy 0.0.0.0:7379
controller on
EOF

./target/release/flintctl -f cluster.flint bootstrap
./target/release/flintctl -f cluster.flint tenant add acme <token> acme 1
valkey-cli -p 7379 --tls --cacert state/certs/ca.crt -a <token> SET hello world
```

`bootstrap` mints the internal CA, starts the control plane, registers the
topology, starts the nodes, proxy, and controller, and confirms
supervision. `status` shows roles/lag/liveness; `stop` reaps everything it
started.

## 1. Sizing — how many nodes

A **pair** is the unit of durability: a master + a replica. A **cluster**
is one control plane supervising some pairs behind a proxy tier.

Per-node baseline (i4i.2xlarge-class, ~2 TB local NVMe, the benched
profile): **~100K ops/s at p99 < 1 ms**, **~1.6 TB usable** at the 80%
fill target.

| environment | minimum | notes |
|---|---|---|
| **Dev / test** | **1 machine** | everything co-located; a single-node pair (master only, no replica) is fine — no failover, but full functionality. This is the marketplace single-VM shape. |
| **Minimum production** | **2 storage machines** (a master+replica pair on **separate hosts**) + the control components | separate hosts so one machine's loss never takes both copies. The CP, proxy, and controller are lightweight and may co-locate on the storage hosts or a small third box. Gives real failover. |
| **Recommended production** | the pair on 2 hosts + **≥ 2 proxies** + **3-node Raft control plane** + **1 controller** | 2+ proxies so a proxy loss is invisible (stateless subset failover); 3-node CP for control-plane HA; add pairs to scale capacity. |

**Scaling and the hard limits** (from the capacity model):

- **≤ 32 pairs per controller shard** — a controller probes every node
  each tick; past ~32 pairs a correlated failure can stall detection.
  Beyond that, run another controller shard (a controller with a disjoint
  `--pairs` list) — not a bigger controller.
- **64 pairs** recommended per cluster, **256** the hard ceiling (the
  slot-rebalance granularity floor). Growth beyond a cluster is "run
  another cluster."
- At the i4i baseline that is **~204 TB** addressable per cluster
  (128 pairs × 1.6 TB). Add capacity by adding pairs (`flintctl expand`),
  then clusters — not by fattening nodes (the ~3.4% RAM:NVMe ratio is the
  benched hot-tier budget).

> The full capacity model — the detection-sweep math, expansion triggers,
> and per-tenant sizing — is in [capacity-model.md](capacity-model.md); the
> numbers above are its load-bearing conclusions.

## 2. Configuration files

One file: the **flintctl inventory** (`cluster.flint` above). It is the
config file — edit it and re-run `flintctl`; no rebuild. Keys:

**Topology & security** (required / structural):

```
statedir ./state            # logs, data dirs, certs, CP state, pids
bins ./target/release       # where the flint binaries live
tls on                      # mint an internal CA; every internal hop mutual-TLS
client-tls on               # encrypted front door (edge cert for tenants)
cp 127.0.0.1:7500           # one line = single-node CP; three = Raft HA
pair HOST:P,HOST:P[,HOST:P] # a replica set (master first); repeatable
proxy HOST:P                # a routing proxy; repeatable
controller on               # automatic failover supervision
agent HOST:9464             # (managed plane) metrics/automation add-on
capacity <bytes>            # per-node NVMe budget (fill %/expansion math)
admin-token <tok>           # gate the PROXY*/operator surface
edge-san <ip-or-dns>        # extra SANs on the edge cert (real client addrs)
```

**The `pair` line — one line per pair.** The comma separates the *members
of a single pair*, not pairs from each other: the **first member is the
master**, the rest are replicas (`pair m,r1,r2` is a master with two
replicas). Add a pair by adding another `pair` line. Pairs are numbered by
line order (pair 0, pair 1, …) — that index is the stable identity
`flintctl migrate-slots`, `CPSLOTS`, and `swap-node` refer to. A
single-member `pair HOST:P` is a master with no replica: fine for dev, not
for production (a `min-replicas 1` write would shed with no replica to ack).
`cp` follows the same shape — one line is a single-node control plane, three
lines make a Raft HA set.

A two-pair cluster with a proxy tier and HA control plane:

```
statedir /var/lib/flint
bins /opt/flint/bin
tls on
client-tls on
cp 10.0.1.10:7500           # 3 cp lines -> Raft HA control plane
cp 10.0.1.11:7500
cp 10.0.1.12:7500
pair 10.0.2.10:7001,10.0.2.11:7001   # pair 0: master, replica (separate hosts)
pair 10.0.2.12:7001,10.0.2.13:7001   # pair 1: master, replica
proxy 10.0.1.20:7379         # 2 proxies -> a proxy loss is invisible
proxy 10.0.1.21:7379
controller on
capacity 1717986918400       # ~1.6 TB per-node fill budget
min-replicas 1               # close the widowed-master write hole
```

Don't hand-edit an inventory to *grow* a running cluster and re-bootstrap —
add capacity live with `flintctl expand 10.0.2.14:7001,10.0.2.15:7001`
(same comma syntax; the new pair joins unranged, then rebalancing or
`migrate-slots` drains slots onto it), and keep the inventory in sync so
`reload`/`status`/`stop` still see the whole fleet.

**Operator tunables** (all optional; absent = compiled default). Edit,
then `flintctl reload` to push the **HOT** ones to the running fleet with
no restart, or `stop`/`start` for the restart-only ones:

```
wal-fsync-ms 500      node  HOT   WAL fsync cadence (host-loss RPO bound)
lag-soft-ms 500       node  HOT   soft replication lag cap (delays writes)
lag-hard-ms 1000      node  HOT   hard lag cap (sheds; bounds at-risk VOLUME)
min-replicas 1        node  HOT   min-replicas-to-write safety gate (default 0)
widowed-grace-ms N    node  HOT   max time accepting writes with NO live
                                  replica (flintctl default 10000 on pair
                                  members, off for a peerless node; 0 off).
                                  The lag cap cannot fire without a replica
                                  to measure, so this is the only bound on
                                  that window.
max-conns 10000       node+proxy HOT  connection admission cap
cache-ttl-ms 300      proxy HOT   near-cache TTL default (PROXYCACHE)
cache-max-bytes N     proxy HOT   near-cache byte budget
async-queue-cap 4096  node  restart  async write-queue depth
poll-ms 100           ctlr  restart  failure-probe interval (RTO)
confirm 3             ctlr  restart  consecutive fails before promote
lease-ttl-ms 3000     ctlr  restart  master lease TTL
```

On a replicated pair, set `min-replicas 1` — it closes the widowed-master
write hole ([failover.md](failover.md)).

## 3. Monitoring & Grafana

The open stack exposes its state through two RESP admin commands — this is
the metrics source you build on:

- **`FLINTINFO`** on each node: role, epoch, `seq_lag`, `live_replicas`,
  `lag_ms`, the lag caps, `wal_fsync_ms`, `active_conns`/`max_conns`,
  `cert_days_remaining`, write-stall signals, and more.
- **`PROXYSTATS`** on each proxy: connections, command/read/write totals,
  cache hit/miss/entries/bytes, `moved_learned_total`, quota sheds,
  `cert_days_remaining`. **`PROXYLATENCY`** gives per-lane read/write
  histograms.

The open stack ships a reference exporter, **`flint-exporter`**, that polls
those commands and serves them as Prometheus text on `/metrics`. Point it
at your nodes and proxies (mesh certs for the nodes, the same CA verifies
the proxy edge):

```sh
flint-exporter --port 9100 \
  --node 127.0.0.1:7001 --node 127.0.0.1:7002 \
  --proxy 127.0.0.1:7379 \
  --ca state/certs/ca.crt --cert state/certs/int.crt --key state/certs/int.key
# omit --ca/--cert/--key for a fully plaintext dev fleet;
# add --admin-token <tok> if your proxy admin surface is gated.
```

It emits `flint_up{instance,role}` / `flint_proxy_up{instance}` plus every
numeric `FLINTINFO`/`PROXYSTATS` field as a gauge
(`flint_lag_ms`, `flint_wal_fsync_ms`, `flint_proxy_cache_hits_total`, …).
Point Prometheus at it, and Grafana at Prometheus:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: flint
    static_configs:
      - targets: ['127.0.0.1:9100']   # flint-exporter
```

Then build panels on the fields above — the essential four: **replication
lag** (`lag_ms` / `seq_lag` per pair), **role/liveness** (`role`,
`live_replicas`), **cert expiry** (`cert_days_remaining` — alert well
before zero), and **proxy throughput + cache hit rate**.

> `flint-exporter` is intentionally lean — a starting point to extend. The
> managed Crestway plane ships a fuller metrics agent (insights, capacity
> triggers) and curated Grafana dashboards; the underlying data is the
> same commands above.

Alert on: `seq_lag` climbing without draining, `live_replicas` dropping
below your `min-replicas`, `cert_days_remaining` under ~14, and sustained
`write_stopped`.

## 3b. Disk headroom

Per-tenant quotas bound each namespace. **Nothing bounds the host**, and
the sum of quotas is meant to exceed the disk — that oversubscription is
the whole point of packing tenants. So plan for the node to fill, and know
what it does when it starts to.

Each node samples the filesystem holding its `--data-dir` and, below the
threshold, refuses ordinary writes with `-QUOTA` while continuing to serve
reads and to accept `DEL`/`UNLINK`/`EXPIRE`/`FLUSHALL`. It reopens by
itself once space returns; no operator action is needed either way.

| flag | default | meaning |
|---|---|---|
| `--disk-min-free-pct` | `10` | shed below this share of the filesystem; `0` disables |
| `--disk-min-free-bytes` | `2 GiB` | shed below this many free bytes; `0` disables |
| `--disk-sample-ms` | `2000` | how often the filesystem is measured |

Both thresholds apply and the stricter binds: a percentage alone is
useless on a 16 TB disk (10% is 1.6 TB of headroom nobody wants to hold)
and a byte floor alone is useless on a small one.

**The gate fires early on purpose.** An LSM needs free space to compact —
new SSTs are written before the old ones are dropped — and the cure for a
full disk is a trap without it: freeing space means deleting, a delete is
a write, and reclaiming the bytes needs the compaction that has no room to
run. Stopping at 10% leaves room to dig out; stopping at 0% may not.

Watch `disk_free_bytes`, `disk_free_pct` and `disk_verdict` in `FLINTINFO`
(and the exporter). `disk_unknown_samples` counting up means the sampler
cannot read the filesystem — the node is then flying blind and will NOT
shed, because a failed measurement is not evidence of a full disk.

Known gap: this gates client writes. A **replica** applying its master's
WAL keeps writing regardless, so a replica can still fill. Size replicas
with the same headroom as their master.

## 4. Managing users (tenants)

A tenant is a namespace + a token + a proxy subset + quotas. All via
`flintctl`:

```sh
# create: name, token, namespace, k = how many proxies serve it (subset)
flintctl -f cluster.flint tenant add acme <token> acme 2

# quotas: fleet ops/s and storage bytes (0 = unlimited)
flintctl -f cluster.flint tenant-quota acme 50000 53687091200

# opt-ins (each is the tenant's choice; see the tenant guide)
flintctl -f cluster.flint tenant-reads acme on     # replica reads
flintctl -f cluster.flint tenant-cache acme on     # proxy near-cache
flintctl -f cluster.flint tenant-async acme on     # async write queue
```

**What you hand the tenant** (nothing else): the **endpoint** (`host:port`,
TLS), their **token**, the **CA certificate** (`state/certs/ca.crt`, to
verify TLS), and their **limits**. They connect with any Redis/Valkey
client — the namespace is transparent (their token maps to it at the
proxy; they never type it). Hand them [tenant-guide.md](tenant-guide.md):
a ready-to-send onboarding guide with connect-and-go sample code.

Notes:
- Tokens are stored **hashed** (SHA-256) — you cannot recover a token,
  only rotate it.
- The **subset** (`k`) is a shuffle-shard of the proxy fleet; a tenant is
  served only by its subset, which bounds blast radius. `CPSETSUBSET`
  overrides it (dedicated proxies for a large tenant).
- The proxy-side `PROXYLATENCY`/`PROXYHOTKEYS` commands answer per-tenant
  (scoped by the caller's token), so a tenant sees only its own numbers.

## 5. Rotating credentials & keys

Three independent rotations, all zero-downtime (dual-version windows):

**Tenant tokens.** A tenant self-rotates with `CPMYROTATE <current-token>`
(the control plane mints the successor, returns it once; both old and new
authenticate until the old drains, then it retires automatically). An
operator can force a rotation via the control-plane `CPROTATETOKEN` /
`CPDROPPREV` pair. Swap at leisure inside the window — no client
reconnection is forced.

**Fleet admin token** (gates the operator/PROXY* surface):

```sh
flintctl -f cluster.flint rotate-admin
```

Mints the successor, keeps both valid (proxies hold both digests) until
the old is dropped. Hot — no restart.

**Internal mesh + edge certificates.** Re-sign every leaf (the mesh cert
and the client-facing edge cert) from the existing CA in place:

```sh
flintctl -f cluster.flint rotate-certs
```

Every listener and dialer **hot-reloads** its leaf within a couple of
seconds — no restart, and old + new leaves both verify during the roll
(same CA). Watch `cert_days_remaining` (FLINTINFO/PROXYSTATS) and rotate
well before expiry.

**The CA itself** (a bigger, rarer operation — a new trust root must reach
every component before the old is retired) has its own runbook:
[runbooks/ca-rotation.md](runbooks/ca-rotation.md).

## 6. Production on AWS: load balancing the proxy tier

For a multi-proxy production fleet on AWS, front the proxies with a
**Network Load Balancer (NLB)** — not an ALB. RESP is a raw TCP protocol,
so the L7/HTTP ALB cannot speak it; the NLB is L4.

Run the NLB in **TCP passthrough** (no TLS termination at the LB): the
proxy keeps terminating client TLS with its edge cert and the client keeps
verifying against the fleet CA, so the mTLS-to-edge model stays end to end.
(The NLB *can* terminate with an ACM cert and re-encrypt to the proxy if
you prefer certs in ACM — more moving parts; passthrough is the clean
default.)

What the NLB buys you:

- **Health-based eviction in seconds.** An NLB target health check (a TCP
  connect on the proxy port is sufficient — accepting = healthy) pulls an
  unresponsive proxy out of rotation automatically, no DNS publisher and no
  30-second client-retry window. This is the fast-failover the raw
  per-tenant DNS model lacks.
- **One stable, cross-AZ endpoint** that scales with the target group.
- Negligible latency (L4 passthrough is microseconds against sub-ms reads).

**One endpoint, token-scoped — the default shape.** Auth is by **token**,
not by endpoint: a tenant's token maps to its namespace at whichever proxy
it lands on. So every tenant connects to the *same* NLB endpoint
(`cache.example.com`) and AUTHs with its own token — simpler than
per-tenant DNS, and isolation is still real (stateless proxies, per-tenant
ops/s + storage quotas, admission control). The per-tenant shuffle-shard
**subset** (§4, `CPSETSUBSET` + `CPDNSZONE`) then becomes the
**dedicated-isolation tier** for a whale that needs *physical* proxy
separation — put its dedicated proxies behind their own NLB / target
group — rather than the default routing model. The DNS-subset model still
serves non-AWS deployments and anyone who prefers not to run an LB.

**Spread across AZs.** Register proxies in multiple AZs behind the NLB
(cross-AZ aware), and place each pair's master and replica in *different*
AZs so an AZ loss is survivable — the topology the capacity model assumes.

**Not the entry SKU.** The single-VM marketplace shape runs one proxy on
the instance; adding an NLB there is pure cost. The NLB is the multi-VM
production template (the follow-on to the single-VM CloudFormation stack).

## See also

- [architecture.md](architecture.md) — the three planes; write and read paths.
- [failover.md](failover.md) — master handoff (planned + crash), proxy failure.
- [capacity-model.md](capacity-model.md) — sizing the data plane in depth.
- [tenant-guide.md](tenant-guide.md) — what a tenant receives and how to use it.
- [command-support.md](command-support.md) — the supported command surface.
- [release-checklist.md](release-checklist.md) — gates + how a release
  upgrades a running fleet (`flintctl upgrade`: canary, soak, masters last).
