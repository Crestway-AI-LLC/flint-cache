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

**Operator tunables** (all optional; absent = compiled default). Edit,
then `flintctl reload` to push the **HOT** ones to the running fleet with
no restart, or `stop`/`start` for the restart-only ones:

```
wal-fsync-ms 500      node  HOT   WAL fsync cadence (host-loss RPO bound)
lag-soft-ms 500       node  HOT   soft replication lag cap (delays writes)
lag-hard-ms 1000      node  HOT   hard lag cap (sheds; the RPO bound)
min-replicas 1        node  HOT   min-replicas-to-write safety gate
max-conns 10000       node+proxy HOT  connection admission cap
cache-ttl-ms 300      proxy HOT   near-cache TTL default (PROXYCACHE)
cache-max-bytes N     proxy HOT   near-cache byte budget
async-queue-cap 4096  node  restart  async write-queue depth
poll-ms 200           ctlr  restart  failure-probe interval (RTO)
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

To feed **Prometheus + Grafana**, run a small exporter that polls those
commands on a scrape interval and re-emits them as Prometheus text (they
are already `field:value` lines — a ~40-line script). Point Prometheus at
the exporter, and Grafana at Prometheus:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: flint
    static_configs:
      - targets: ['127.0.0.1:9100']   # your FLINTINFO/PROXYSTATS exporter
```

Then build panels on the fields above — the essential four: **replication
lag** (`lag_ms` / `seq_lag` per pair), **role/liveness** (`role`,
`live_replicas`), **cert expiry** (`cert_days_remaining` — alert well
before zero), and **proxy throughput + cache hit rate**.

> The managed Crestway plane ships a turnkey metrics exporter (the fleet
> agent) and curated Grafana dashboards; on the open stack you point your
> own exporter at the commands above. The data is identical — only the
> packaging differs.

Alert on: `seq_lag` climbing without draining, `live_replicas` dropping
below your `min-replicas`, `cert_days_remaining` under ~14, and sustained
`write_stopped`.

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

## See also

- [architecture.md](architecture.md) — the three planes; write and read paths.
- [failover.md](failover.md) — master handoff (planned + crash), proxy failure.
- [capacity-model.md](capacity-model.md) — sizing the data plane in depth.
- [tenant-guide.md](tenant-guide.md) — what a tenant receives and how to use it.
- [command-support.md](command-support.md) — the supported command surface.
- [release-checklist.md](release-checklist.md) — gates + how a release
  upgrades a running fleet (`flintctl upgrade`: canary, soak, masters last).
