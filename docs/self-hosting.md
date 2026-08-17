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

Build prerequisites (Rust 1.85+, a C++ toolchain and libclang for RocksDB,
`valkey-cli`) are listed in the [README](../README.md#prerequisites).

```sh
# Stamp the build with the version you are deploying. flintctl refuses to
# MUTATE a fleet from an unstamped build — see "Why the tag" below.
FLINT_RELEASE_TAG=v0.1.0 cargo build --release --features flint-server/rocks,flint-backup/rocks

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
./target/release/flintctl -f cluster.flint tenant add acme tok-acme acme 1
valkey-cli -p 7379 --tls --cacert state/certs/ca.crt -a tok-acme SET hello world
```

### Why the tag

A build straight from source reports no version, carries no manifest and no
checksums. `flintctl` therefore refuses the fleet-*changing* verbs from one —
`bootstrap`, `upgrade`, `failover`, tenant edits — while always allowing the
read-only ones (`status`, `verify`). Three ways to satisfy it:

- **Download a release bundle** — the simplest, and no toolchain at all.
  Tagged releases publish Linux x86_64 binaries plus a `manifest.json`
  carrying the version and sha256, on the
  [Releases page](https://github.com/Crestway-AI-LLC/flint-cache/releases/latest).
  Unpack into the directory your inventory's `bins` points at:

  ```sh
  # manifest.json has a CONSTANT name while the bundle name carries the
  # version, so `releases/latest/download/manifest.json` is a stable entry
  # point that never goes stale: it always describes the newest release, and
  # the bundle URL and sha256 come out of it rather than out of this page.
  R=https://github.com/Crestway-AI-LLC/flint-cache/releases
  curl -fLO "$R/latest/download/manifest.json"
  BUNDLE=$(python3 -c 'import json;print(json.load(open("manifest.json"))["bundle"])')
  curl -fLO "$BUNDLE"          # flintctl needs both — the manifest is its
  TAR=$(basename "$BUNDLE")    # format-break guard

  # The sha256 says the bytes match THIS manifest. The signature says they
  # came from us — check it first, and see below on which key to trust.
  # minisign.pub lives in the repository root; the note below is about
  # keeping YOUR copy rather than re-fetching it beside every download.
  curl -fLO https://raw.githubusercontent.com/Crestway-AI-LLC/flint-cache/main/minisign.pub
  curl -fLO "$BUNDLE.minisig"
  minisign -Vm "$TAR" -P "$(tail -1 minisign.pub)"
  python3 -c "import json,hashlib,sys
m=json.load(open('manifest.json'))
d=hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest()
sys.exit(0 if d==m['sha256'] else f\"sha256 mismatch: {d} != {m['sha256']}\")" "$TAR"

  tar xzf "$TAR" -C /opt/flint/bin
  ```

  For a specific version instead of the newest, take its tag from the
  Releases page and use `$R/download/<tag>/manifest.json`.

  Pin `minisign.pub` once, out of band, and check every later release against
  your pinned copy — [release-signing.md](release-signing.md) explains why the
  sha256 in `manifest.json` is not a substitute.

  Built on Amazon Linux 2023, so glibc 2.34 — fine on RHEL 9, AL2023, Ubuntu
  22.04 and newer; older hosts should build from source.
- `FLINT_RELEASE_TAG=<tag>` at **compile** time, as above, when you build it
  yourself. The tag is baked into every binary, so `verify` can hold the whole
  fleet to one build and an `upgrade` can prove the new binaries actually
  took. Use a tag that means something to you (`v0.1.0`, or your own build
  number in the same `v<major>.<minor>.<patch>` shape) — it is a claim about
  *which* build this is, not a claim of blessing from us.
- `disposable on` in the inventory — for a cluster you will throw away. Never
  put it in an inventory whose data you would miss: it is the line that says
  "an unverifiable binary may rewrite this fleet."

`bootstrap` mints the internal CA, starts the control plane, registers the
topology, starts the nodes, proxy, and controller, and confirms
supervision. `status` shows roles/lag/liveness; `stop` reaps everything it
started.

## 1. Sizing — how many nodes

A **pair** is the unit of redundancy: a master + a replica. A **cluster**
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

**Size for the working set: Flint never evicts.** There is no
`maxmemory-policy` equivalent — an unexpired key is never dropped to make
room. When a tenant hits their quota, their writes shed with `-QUOTA`;
when the HOST runs low on disk (under 10% free or 2 GiB, whichever is
stricter — an LSM needs headroom to compact), the node refuses
space-growing writes early, keeps serving reads and the delete path, and
reopens by itself once space returns. So the plan is: provision for the
full working set, use TTLs so space returns on its own, and alert on
`disk_free_pct` / `disk_verdict` in `FLINTINFO` (the exporter surfaces
both) well before the guard fires. The guard's flips are also fleet
journal events (`DiskShed`/`DiskResumed`), so tooling can trigger on the
edge instead of polling; if you run your own space-reclaim daemon, rank
candidates with `FLINTKEYSIZE`/`FLINTKEYSTAMP` (see command-support.md)
and remember the delete path stays open while writes shed.
space-reclaim.md is the full recipe for such a daemon.

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
single-member `pair HOST:P` is a master with no replica: fine for dev, and
in production it means losing that host loses the shard's data (and, if you
have set `min-replicas 1`, that its writes shed with nothing to ack).
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

# Failure domains. Separate HOSTS survive a host failure; separate DOMAINS
# survive losing a zone, a rack or a power feed — which is the likelier
# event and the one that takes both copies at once. Declare them and
# `verify` refuses a pair whose members share one.
#
# All-or-nothing on purpose: zoning some pair hosts and not others is
# REFUSED rather than half-checked, because a partial declaration reads as
# anti-affinity without being one.
zone 10.0.2.10 az-a
zone 10.0.2.11 az-b
zone 10.0.2.12 az-a
zone 10.0.2.13 az-b
proxy 10.0.1.20:7379         # 2 proxies -> a proxy loss is invisible
proxy 10.0.1.21:7379
controller on
capacity 1717986918400       # ~1.6 TB per-node fill budget
```

Note what this example does NOT set: `min-replicas`. Flint ships it at **0**,
the same value Redis ships, and a production fleet is expected to leave it
there — the widowed grace below is the bound that is on by default. Setting
it to 1 is a deliberate trade of availability for durability, and
[failover.md](failover.md) explains what you are trading before you do it.

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
min-replicas 0        node  HOT   shed writes while live replicas < N.
                                  SHIPPED VALUE 0 = off, matching Redis.
                                  0: writes always accepted; a widowed
                                     master's writes are at risk until the
                                     widowed grace below sheds them.
                                  1: writes shed with -THROTTLED whenever no
                                     replica is live — INCLUDING the window
                                     after a failover, until the replacement
                                     finishes its full sync. Availability
                                     traded for durability; see failover.md.
widowed-grace-ms N    node  HOT   max time accepting writes with NO live
                                  replica (flintctl default 10000 on pair
                                  members, off for a peerless node; 0 off).
                                  The lag cap cannot fire without a replica
                                  to measure, so this is the only bound on
                                  that window.
max-conns 10000       node+proxy HOT  connection admission cap
fanout-timeout-ms 60000  proxy restart  read budget for the O(KEYS) admin
                                  class — DBSIZE, FLUSHALL, a SCAN step.
                                  Separate from keyed traffic's 5 s on
                                  purpose: a GET should fail over fast,
                                  while DBSIZE honestly costs more the more
                                  keys a node holds (it walks every metadata
                                  row). Size it to the KEYSPACE — the
                                  default covers roughly 20M keys per node;
                                  past that, `verify --probe` starts
                                  reporting a DBSIZE timeout on a fleet
                                  that is otherwise perfectly healthy.
cache-ttl-ms 300      proxy HOT   near-cache TTL default (PROXYCACHE)
cache-max-bytes N     proxy HOT   near-cache byte budget
async-queue-cap 4096  node  restart  async write-queue depth
poll-ms 100           ctlr  restart  failure-probe interval (RTO)
confirm 3             ctlr  restart  consecutive fails before promote
lease-ttl-ms 5000     node  restart  master lease TTL (self-renewed at the CP)
fullsync-rate-bytes 67108864  node HOT  cap on how fast a node SERVES a full
                                  sync, bytes/sec; 0 = uncapped. Not an
                                  inventory key — it is a server flag
                                  (--fullsync-rate-bytes) with this default,
                                  changeable live with FLINTCONFIG. A
                                  re-seeding replica pulls its checkpoint from
                                  the pair's MASTER, which after a failover was
                                  promoted seconds ago and is carrying the pair
                                  alone; uncapped, that was measured stalling
                                  its write path for 11.9 s. Raise it or set 0
                                  to get redundancy back sooner at the cost of
                                  write latency during recovery; see the
                                  recovery window in docs/slo.md.
write-deadline-ms 2000  node  HOT  refuse a write on ARRIVAL when its estimated
                                  wait (inflight x recent service time) already
                                  exceeds this; 0 = no deadline, unbounded
                                  queueing. Also a server flag
                                  (--write-deadline-ms), changeable live with
                                  FLINTCONFIG. A write that completes after its
                                  caller timed out spends capacity live traffic
                                  needed AND is ambiguous — the caller retried,
                                  so INCR may apply twice. Refusing early is
                                  neither: -THROTTLED means retry with backoff.
                                  Set it below your clients' timeout. Watch
                                  writes_shed_deadline and write_wait_est_ms in
                                  FLINTINFO.
node-ready-s 15       ctl   ctl-only PING budget for a freshly spawned replica.
                                  A wiped node full-syncs its checkpoint
                                  BEFORE it binds its listener, so on a
                                  loaded fleet this must cover the whole
                                  transfer — size it to data-per-node, not
                                  to taste. Too small and rolls/restarts
                                  report a healthy syncing node as dead.
```

On a replicated pair, set `min-replicas 1` — it closes the widowed-master
write hole ([failover.md](failover.md)).

## 2b. Running it as a service, and surviving a reboot

`flintctl` starts processes; it is not itself a daemon. Nothing above keeps
the fleet running across a reboot, so wire that up before you rely on it.

**Install layout.** Put the binaries and the inventory somewhere stable,
outside a build tree:

```
/opt/flint/bin/            flint-server, flint-proxy, flint-controlplane,
                           flint-controller, flintctl
/opt/flint/cluster.flint   the inventory   (bins /opt/flint/bin)
/var/lib/flint/            statedir — certs, CP state, node data, logs, pids
```

**`bootstrap` once, `start` forever after.** This is the distinction that
matters at boot time and it is not symmetric:

- `bootstrap` mints the CA, **registers** the topology with the control plane
  and starts everything. It is a first-time act.
- `start` spawns every process against **existing** state — no cert minting,
  no registry writes. Re-registering would append duplicate topology, so a
  boot path that runs `bootstrap` a second time corrupts the registry.

So the boot unit must choose, and the cheapest honest test is whether the CP
state file already exists:

```ini
# /etc/systemd/system/flint.service
[Unit]
Description=Flint fleet (idempotent boot)
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/opt/flint/boot.sh
ExecStop=/opt/flint/bin/flintctl -f /opt/flint/cluster.flint stop

[Install]
WantedBy=multi-user.target
```

```sh
#!/usr/bin/env bash
# /opt/flint/boot.sh
set -euo pipefail
CTL=/opt/flint/bin/flintctl
INV=/opt/flint/cluster.flint
if [ -e /var/lib/flint/cp-state ] || [ -e /var/lib/flint/cp-state-n1 ]; then
  "$CTL" -f "$INV" start          # already bootstrapped: idempotent boot
else
  "$CTL" -f "$INV" bootstrap
fi
"$CTL" -f "$INV" verify
```

`Type=oneshot` with `RemainAfterExit=yes` is deliberate: the unit's job is to
*bring the fleet up*, not to be its parent. Ending with `verify` means a boot
that half-worked fails the unit instead of looking healthy.

On a multi-host fleet only the orchestrator host needs this unit — `flintctl`
reaches the others over ssh, so that host also needs key-based access to them.
That also means a whole-datacentre restart is a race: the orchestrator can boot
before its peers are accepting ssh, and `start` will fail against whichever host
is not up yet. Handle it in the script: systemd refuses to load a
`Type=oneshot` unit whose `Restart=` is anything but `no`, so the retry has to
live in `boot.sh`, not in the unit:

```sh
for i in $(seq 1 20); do
  "$CTL" -f "$INV" start && break
  echo "peers not ready yet (attempt $i); retrying in 15s" >&2
  sleep 15
done
```

**Verify it for real.** A reboot path that has never been rebooted is a
guess. Reboot the box, then check `systemctl status flint`, `flintctl -f
/opt/flint/cluster.flint status` for the expected roles, and read a key you
wrote before the reboot.

## 3. Monitoring & Grafana

The open stack exposes its state through two RESP admin commands — this is
the metrics source you build on:

- **`FLINTINFO`** on each node: role, epoch, `seq_lag`, `live_replicas`,
  `lag_ms`, the lag caps, `wal_fsync_ms`, `active_conns`/`max_conns`,
  `cert_days_remaining`, write-stall signals, and more.

  Note `loading:` in particular if you write your own health checks. A node
  that is pulling its initial copy from a master binds and answers `PING`
  from the first moment — deliberately, so that nothing mistakes it for a
  dead host — and reports `role:loading`, `loading:1` and `loading_ms`
  until it can serve. **`PING` means alive; `loading:0` means ready.** A
  health check that stops at `PING` will call a node ready while it still
  refuses every data command with `-LOADING`.
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
