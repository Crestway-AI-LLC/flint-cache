// SPDX-License-Identifier: Elastic-2.0
//! flintctl — inventory-driven cluster lifecycle (roadmap M4, operability).
//!
//! One file describes the cluster; flintctl makes it so:
//!
//!   flintctl -f cluster.flint bootstrap    certs -> CP -> registry -> nodes
//!                                          -> proxies -> controller -> agent
//!   flintctl -f cluster.flint status       roles/lag/liveness table
//!   flintctl -f cluster.flint tenant add <name> <token> <ns> [k]
//!   flintctl -f cluster.flint expand <a,b[,c]>      new pair + reroll ctl
//!   flintctl -f cluster.flint swap-node <bad> <new> fresh replica + CPSETPAIR
//!   flintctl -f cluster.flint failover <node>       graceful master handoff
//!   flintctl -f cluster.flint decommission-node <a> drop one member from a pair
//!   flintctl -f cluster.flint stop          kill everything it started
//!
//! Inventory (line-based, # comments):
//!   statedir /tmp/flint-prod      logs/, data dirs, certs, CP state, pids
//!   bins ./target/release         where the flint binaries live
//!   tls on                        mint an internal CA; every hop mutual TLS
//!   cp 127.0.0.1:7500             one line = single-node CP (3 = Raft, later)
//!   pair 127.0.0.1:6400,127.0.0.1:6401
//!   proxy 127.0.0.1:7379
//!   controller on
//!   agent 127.0.0.1:9464          shadow agent; the addr's port = metrics port
//!
//! Operator tunables (all OPTIONAL; absent = the binary's compiled default).
//! Edit the file, then `flintctl reload` to push the HOT knobs to the
//! running fleet (no restart), or `stop`/`start` for the restart-only ones.
//! No rebuild, no redeploy either way.
//!   wal-fsync-ms 500      node  HOT   WAL fsync cadence (host-loss RPO)
//!   lag-soft-ms 500       node  HOT   soft lag cap (delays writes)
//!   lag-hard-ms 1000      node  HOT   hard lag cap (sheds; RPO bound)
//!   min-replicas 1        node  HOT   min-replicas-to-write gate
//!   widowed-grace-ms N    node  HOT   max time accepting writes with NO
//!                                     replica (default 10000 on a pair;
//!                                     0 off). The only bound on the
//!                                     widowed window — the lag cap has
//!                                     no replica to measure against.
//!   max-conns 10000       node  HOT   connection admission cap
//!   async-queue-cap 4096  node  restart  async write-queue depth
//!   cache-ttl-ms 300      proxy HOT   near-cache TTL (via PROXYCACHE)
//!   cache-max-bytes N     proxy HOT   near-cache byte budget
//!   poll-ms 100           ctlr  restart  failure-probe interval (RTO)
//!   confirm 3             ctlr  restart  consecutive fails before promote
//!   lease-ttl-ms 5000     node  restart  master lease TTL (self-renewed at the CP, ADR-0018)
//!
//! Exit status (flintctl is scriptable): 0 done; 1 the fleet refused the
//! command — the CP's or node's OWN error text goes to stderr, never a
//! panic; 3 `upgrade` aborted mid-roll and the fleet wants a look.
//!
//! v1 runs LOCAL processes (spawn-a-fresh-node model); a remote runner slots
//! in behind the same command surface later. Two properties this tool leans
//! on by design: controllers are STATELESS (ADR-0004) so `expand` simply
//! restarts the controller with the new pair list — a non-event; and pair
//! IDs are the stable identity while membership floats, so `swap-node` is
//! CPSETPAIR after the replacement converges.

use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flint_resp::{Decoded, Value, decode, encode};

// ---------- inventory ----------

#[derive(Debug, Clone, Default)]
struct Inventory {
    statedir: String,
    bins: String,
    tls: bool,
    cp: Vec<String>,
    pairs: Vec<Vec<String>>,
    proxies: Vec<String>,
    /// Optional per-proxy ADVERTISE address (public-DNS deployments where
    /// bind != the dialable address). Positional with `proxies`; absent =
    /// advertise the proxy line itself (the historical behavior).
    proxy_advertise: Vec<String>,
    /// Trust bundle for dialing the proxies' EDGE port (agent PROXY*
    /// views). Absent = the internal CA, which signs the default edge
    /// cert; public-cert fleets point it at the system bundle.
    edge_trust: Option<String>,
    /// ADR-0010 co-processors (`coproc <FAMILY.> <host:port>`, repeatable):
    /// a command family and the seat that serves it. flintctl spawns the
    /// seat and hands every proxy the matching `--families` mapping, so a
    /// family is deployed by declaring it rather than by hand-passing flags
    /// to two binaries and hoping they agree.
    ///
    /// The seat runs the binary named by the family (`VEC.` -> `flint-vec`),
    /// which is how one key serves the families that exist today without a
    /// per-family inventory dialect.
    coprocs: Vec<(String, String)>,
    /// coproc: per-namespace index RAM budget before the seat sheds
    /// (`--index-mem-bytes`). Absent = [`DEFAULT_COPROC_INDEX_BYTES`].
    coproc_index_bytes: Option<u64>,
    /// Billing journal path (agent --billing): the append-only per-tenant
    /// usage record a metering reporter aggregates from. Absent = the
    /// agent keeps no billing journal (self-hosters who do not bill).
    billing: Option<String>,
    /// Local retention for the agent's daily billing segments
    /// (agent --billing-retain-days). Absent = the agent's default; the
    /// archive timer must ship a segment before it ages out, so this only
    /// ever needs raising, not lowering.
    billing_retain_days: Option<u64>,
    controller: bool,
    /// Failure domain per HOST (`zone <host> <name>`): an availability zone
    /// on a cloud, a rack or a power domain on your own hardware.
    ///
    /// Declaring them is what lets `verify` assert that a pair's two members
    /// never share one. The inventory already requires separate *hosts*, and
    /// separate hosts in one zone survive a host failure but not the loss of
    /// the zone — which is the far likelier event, and the one that takes
    /// both copies at once. On instance-store families the data goes with
    /// them (docs/slo.md).
    ///
    /// Optional, and deliberately ALL-OR-NOTHING: an inventory that zones
    /// some pair hosts and not others is refused rather than half-checked,
    /// because a partial declaration reads as anti-affinity and is not one.
    zones: std::collections::HashMap<String, String>,
    agent: Option<String>,
    /// Per-node storage capacity in bytes (capacity model, question 2);
    /// passed to the agent so it can compute fill + expansion ETAs.
    capacity_bytes: Option<u64>,
    /// Proxy admin token (`--admin-token` on every proxy; presented before
    /// PROXY* operator commands). None = ungated dev fleet.
    admin_token: Option<String>,
    /// Client-facing TLS at the proxy (ADR-0006 D2 packaging default):
    /// bootstrap mints an EDGE cert (IP/localhost SANs, same CA) and passes
    /// --tls-cert/--tls-key; tenants verify with the cluster CA.
    client_tls: bool,
    /// Extra subjectAltNames for the EDGE cert (`edge-san <ip-or-dns>`,
    /// repeatable): the addresses real clients dial — an instance's
    /// private/public IP, a DNS endpoint. localhost/127.0.0.1 are always
    /// included.
    edge_sans: Vec<String>,
    /// Operator TUNABLES, externalized so they change with a config edit +
    /// restart — no rebuild, no redeploy. All optional; an absent key leaves
    /// the binary's compiled default. flintctl threads each into the
    /// spawned process's flags at bootstrap/start, so `stop` + edit +
    /// `start` applies new values.
    wal_fsync_ms: Option<u64>, // node: WAL fsync cadence (host-loss bound)
    lag_soft_ms: Option<u64>,  // node: replication soft lag cap (delay)
    lag_hard_ms: Option<u64>,  // node: replication hard lag cap (RPO shed)
    min_replicas: Option<u32>, // node: min-replicas-to-write safety gate
    /// node: how long a seat may keep accepting writes with NO live replica
    /// before it sheds. Absent = flintctl's own default for pair members
    /// (DEFAULT_WIDOWED_GRACE_MS); explicit 0 turns it off.
    widowed_grace_ms: Option<u64>,
    max_conns: Option<u64>,       // node + proxy: connection admission cap
    async_queue_cap: Option<u64>, // node: async write-queue depth
    cache_ttl_ms: Option<u64>,    // proxy: near-cache TTL default
    cache_max_bytes: Option<u64>, // proxy: near-cache byte budget default
    /// proxy: read budget for the O(keys) admin class (DBSIZE/FLUSHALL/SCAN
    /// step), which scales with the KEYSPACE — not with keyed-traffic
    /// latency. Default 60_000; raise it above ~20M keys per node.
    fanout_timeout_ms: Option<u64>,
    ctl_poll_ms: Option<u64>, // controller: failure-probe interval (RTO)
    ctl_confirm: Option<u32>, // controller: consecutive fails to promote
    ctl_lease_ttl_ms: Option<u64>, // NODE master lease TTL (ADR-0018: renewed at the CP)
    /// How long a freshly (re)spawned replica may take to answer PING.
    /// A wiped node full-syncs its checkpoint BEFORE binding its listener,
    /// so on a loaded fleet this must cover the whole transfer — the 15 s
    /// default fits drills, not fleets carrying tens of GB per pair.
    node_ready_s: Option<u64>,
    /// SSH login for seats that live on OTHER machines. Absent = every seat
    /// is local, which is the single-host fleet every drill exercises.
    ssh_user: Option<String>,
    /// Optional identity file for those SSH hops (`-i`); absent = the agent
    /// or the default key.
    ssh_key: Option<String>,
    /// Run the remote side under `sudo -n`. Packaged fleets keep the mesh key
    /// root-only, so the login user cannot read `certs/int.key` or write
    /// `/var/lib/flint` without it.
    ssh_sudo: bool,
    /// Which host runs proxies[i]. A proxy BINDS a wildcard (`0.0.0.0:7379`),
    /// so unlike every other seat its address does not name its machine.
    /// Positional with `proxy` lines; absent = local.
    /// This fleet is disposable — a chaos cluster that exists for one run.
    /// Only such a fleet may be mutated by a binary that is not a release
    /// build; see require_release_or_disposable.
    disposable: bool,
    proxy_hosts: Vec<String>,
    /// Which host runs the controller. It has no address of its own — it
    /// dials the nodes rather than serving — so placement must be declared.
    controller_host: Option<String>,
    /// Backup policy (ADR-0011 D8): `backup-to <dir|s3://bucket/prefix>`
    /// enables the flint-backup seat; the rest tune it. Object-store
    /// CREDENTIALS are never inventory keys — the seat reads the standard
    /// AWS environment, which for a local seat is flintctl's own and for a
    /// remote `backup-host` must come from that host (an instance profile
    /// is env-free and therefore does not work yet: the store is env-only).
    backup_to: Option<String>,
    backup_every: Option<String>,
    backup_verify_every: Option<String>,
    backup_rehearse_every: Option<String>,
    backup_keep: Option<u32>,
    /// Which host runs the backup seat. Like the controller it dials
    /// rather than serves, so placement must be declared; absent = local.
    backup_host: Option<String>,
}

fn parse_inventory(path: &str) -> Inventory {
    let raw =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read inventory {path}: {e}"));
    let mut inv = Inventory {
        bins: "./target/release".into(),
        ..Default::default()
    };
    for line in raw.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let (key, val) = line.split_once(' ').unwrap_or((line, ""));
        let val = val.trim();
        match key {
            "statedir" => inv.statedir = val.to_string(),
            "bins" => inv.bins = val.to_string(),
            "tls" => inv.tls = val == "on",
            "cp" => inv.cp.push(val.to_string()),
            "pair" => inv.pairs.push(val.split(',').map(String::from).collect()),
            "proxy" => inv.proxies.push(val.to_string()),
            // Public-DNS deployments: what the proxy REGISTERS (and clients/
            // portals dial) when it differs from the bind address — an EC2
            // instance cannot bind its public IP or DNS name. Positional:
            // the Nth proxy-advertise line pairs with the Nth proxy line.
            "proxy-advertise" => inv.proxy_advertise.push(val.to_string()),
            "edge-trust" => inv.edge_trust = Some(val.to_string()),
            // `coproc <FAMILY.> <host:port>`. The prefix keeps its trailing
            // dot because that is what the proxy matches on and what the
            // ADR calls the family; normalising it here would hide a typo
            // ("VEC" vs "VEC.") until a command silently took the keyed
            // path instead of the family path.
            "coproc-index-bytes" => inv.coproc_index_bytes = val.parse().ok(),
            "coproc" => {
                let mut it = val.split_whitespace();
                match (it.next(), it.next(), it.next()) {
                    (Some(family), Some(addr), None) => {
                        if !family.ends_with('.') {
                            die(&format!(
                                "inventory: coproc family `{family}` must end with a dot \
                                 (the proxy matches `VEC.`, not `VEC`)"
                            ));
                        }
                        if coproc_bin(family).is_none() {
                            die(&format!(
                                "inventory: no co-processor binary is known for family \
                                 `{family}`; this build serves {}",
                                COPROC_FAMILIES
                                    .iter()
                                    .map(|(f, _)| *f)
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            ));
                        }
                        inv.coprocs
                            .push((family.to_ascii_uppercase(), addr.to_string()));
                    }
                    _ => die(&format!(
                        "inventory: `coproc` takes exactly a family and an address, \
                         got `coproc {val}`"
                    )),
                }
            }
            "billing" => inv.billing = Some(val.to_string()),
            "billing-retain-days" => inv.billing_retain_days = val.parse().ok(),
            "controller" => inv.controller = val == "on",
            "agent" => inv.agent = Some(val.to_string()),
            "capacity" => inv.capacity_bytes = val.parse().ok(),
            "admin-token" => inv.admin_token = Some(val.to_string()),
            "client-tls" => inv.client_tls = val == "on",
            "edge-san" => inv.edge_sans.push(val.to_string()),
            // Operator tunables (see the Inventory struct).
            "wal-fsync-ms" => inv.wal_fsync_ms = val.parse().ok(),
            "lag-soft-ms" => inv.lag_soft_ms = val.parse().ok(),
            "lag-hard-ms" => inv.lag_hard_ms = val.parse().ok(),
            "min-replicas" => inv.min_replicas = val.parse().ok(),
            "widowed-grace-ms" => inv.widowed_grace_ms = val.parse().ok(),
            "max-conns" => inv.max_conns = val.parse().ok(),
            "async-queue-cap" => inv.async_queue_cap = val.parse().ok(),
            "cache-ttl-ms" => inv.cache_ttl_ms = val.parse().ok(),
            "cache-max-bytes" => inv.cache_max_bytes = val.parse().ok(),
            "fanout-timeout-ms" => inv.fanout_timeout_ms = val.parse().ok(),
            "poll-ms" => inv.ctl_poll_ms = val.parse().ok(),
            "confirm" => inv.ctl_confirm = val.parse().ok(),
            "lease-ttl-ms" => inv.ctl_lease_ttl_ms = val.parse().ok(),
            "node-ready-s" => inv.node_ready_s = val.parse().ok(),
            // Multi-host placement (see the Inventory struct). Omit them all
            // and the fleet is single-host, byte for byte as before.
            "ssh-user" => inv.ssh_user = Some(val.to_string()),
            "ssh-key" => inv.ssh_key = Some(val.to_string()),
            "ssh-sudo" => inv.ssh_sudo = val == "on",
            "proxy-host" => inv.proxy_hosts.push(val.to_string()),
            // `zone <host> <name>`. Malformed lines DIE rather than being
            // dropped: a silently-ignored zone line would leave verify
            // reporting anti-affinity it never checked.
            "zone" => {
                let mut it = val.split_whitespace();
                match (it.next(), it.next(), it.next()) {
                    (Some(host), Some(name), None) => {
                        inv.zones.insert(host.to_string(), name.to_string());
                    }
                    _ => die(&format!(
                        "inventory: `zone` takes exactly a host and a name, got `zone {val}`"
                    )),
                }
            }
            "controller-host" => inv.controller_host = Some(val.to_string()),
            "backup-to" => inv.backup_to = Some(val.to_string()),
            "backup-every" => inv.backup_every = Some(val.to_string()),
            "backup-verify-every" => inv.backup_verify_every = Some(val.to_string()),
            "backup-rehearse-every" => inv.backup_rehearse_every = Some(val.to_string()),
            "backup-keep" => inv.backup_keep = val.parse().ok(),
            "backup-host" => inv.backup_host = Some(val.to_string()),
            // Declares this fleet THROWAWAY: a non-release flintctl may
            // mutate it. Absent means a real cluster, which is the safe
            // default and the one every existing inventory already has.
            "disposable" => inv.disposable = val == "on",
            other => panic!("inventory: unknown key {other:?}"),
        }
    }
    assert!(
        !inv.statedir.is_empty(),
        "inventory needs `statedir <path>`"
    );
    assert!(
        !inv.cp.is_empty(),
        "inventory needs at least one `cp <addr>`"
    );
    // One cp line = single-node CP; three = a Raft group (cp_seat_args
    // derives node ids, raft ports and peer lists from inventory order).
    // Two is refused: an even group cannot form a majority after one loss,
    // which is the only reason to run more than one seat at all.
    assert!(
        inv.cp.len() == 1 || inv.cp.len() == 3,
        "cp seats must number 1 (single-node) or 3 (raft); {} is neither",
        inv.cp.len()
    );
    assert!(
        !inv.pairs.is_empty(),
        "inventory needs at least one `pair a,b`"
    );
    inv
}

fn port_of(addr: &str) -> u16 {
    addr.rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .unwrap_or_else(|| panic!("bad addr {addr}"))
}

// ---------- RESP admin client (mTLS-aware — valkey-cli can't do the mesh) ----------

fn tls_client(inv: &Inventory) -> Option<Arc<flint_tls::ClientConfig>> {
    if !inv.tls {
        return None;
    }
    let d = &inv.statedir;
    Some(
        flint_tls::client_config(
            &format!("{d}/certs/ca.crt"),
            &format!("{d}/certs/int.crt"),
            &format!("{d}/certs/int.key"),
        )
        .expect("load internal certs (bootstrap mints them)"),
    )
}

fn call(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    args: &[&str],
) -> std::io::Result<Value> {
    call_to(addr, tls, args, Duration::from_millis(1500))
}

/// Like `call` but with a caller-chosen read timeout — for slot migration,
/// which streams a whole slot and can exceed the default.
fn call_slow(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    args: &[&str],
    read_timeout: Duration,
) -> std::io::Result<Value> {
    call_to(addr, tls, args, read_timeout)
}

/// Run a SEQUENCE of commands on ONE connection, returning the last reply.
/// Needed wherever an earlier command sets connection state a later one
/// depends on (FLINTNS pins the namespace; FLUSHALL then wipes THAT ns).
/// EDGE-client TLS for the verify probe: verify the proxy's edge cert, present
/// no client cert (edge auth is tokens, not certs) — the same trust a tenant's
/// own redis client uses.
///
/// Without this the probe dialled the client port in PLAINTEXT, and on any
/// `client-tls on` fleet — which is every real deployment — decoded the
/// server's TLS alert as a RESP frame and reported
/// `UnknownType(21)` five times over. So the half of verify that actually
/// catches a stale routing table was unusable exactly where it matters, and
/// it failed in a way that named neither TLS nor the port.
/// The CA bundle that anything dialling THIS fleet's edge must validate
/// against: `edge-trust` when declared, otherwise the internal CA, which
/// signs the edge cert `bootstrap` mints.
///
/// ONE function on purpose. This derivation had three copies — flintctl's
/// own dials, the `--edge-ca` handed to the agent, and the bring-up failure
/// message — and copies of it are precisely how this fleet accumulated five
/// separate bugs in a single day, each one the rediscovery of the same fact:
///
///     the edge cert is not the internal cert, and every component that
///     dials the edge must be told SEPARATELY which CA to trust
///
/// Every one of those was fixed where it was found and none of the fixes
/// generalised, because there was no single place for the fix to live. A
/// fourth consumer written against a fourth copy is the next instance;
/// against this function it is correct by construction. The diagnostic
/// mattered too: while the copies agreed the failure text was accurate by
/// luck, and a mutation that changed only the dial made it report a trust
/// anchor that was never used.
fn edge_trust_path(inv: &Inventory) -> String {
    inv.edge_trust
        .clone()
        .unwrap_or_else(|| format!("{}/certs/ca.crt", inv.statedir))
}

fn edge_tls_client(inv: &Inventory) -> Option<Arc<flint_tls::ClientConfig>> {
    if !inv.client_tls {
        return None;
    }
    let trust = edge_trust_path(inv);
    Some(
        flint_tls::edge_client_config(&trust)
            .unwrap_or_else(|e| die(&format!("edge trust {trust}: {e}"))),
    )
}

/// Where to dial the proxy as a CLIENT would.
///
/// The inventory's proxy line is a BIND address: `0.0.0.0:7379` is not a
/// hostname, and under TLS its SNI matches nothing in the edge cert. The
/// advertise address is the one published to clients and the one the cert is
/// issued for, so a probe of it tests what tenants actually reach — DNS and
/// certificate included.
fn probe_target(inv: &Inventory, i: usize) -> String {
    proxy_dial(inv, i)
}

/// The address to DIAL for `proxies[i]` — liveness, status, verify, admin.
///
/// A bind address is not a destination. `0.0.0.0:7379` names no machine, so
/// dialling it reaches whatever is on THIS host's port 7379: on a fleet where
/// the proxy shares the orchestrator's box that is the proxy, and the mistake
/// is invisible. Give the proxy its own machine and the same dial reaches
/// nothing, so bootstrap declares a perfectly healthy proxy dead —
///
///     proxy 0.0.0.0:7379 never answered PROXYSTATS within 10s
///
/// — which is exactly how the first 7-host run failed. Every smaller topology
/// hid it, including the 2-host chaos run, because there the proxy and the
/// orchestrator were the same machine.
///
/// Order: the ADVERTISE address when declared (what clients and the registry
/// use, and what the edge cert is issued for), else the declared `proxy-host`
/// with the bind port, else the bind address — which is correct for the
/// single-host fleet every drill exercises.
/// Why a proxy that is holding its port might still read as down.
///
/// The message used to be `did not come up (port busy?)`, which names the
/// one cause that is almost never it. On a client-TLS fleet the usual cause
/// is TRUST: flintctl validates the edge chain against the fleet's internal
/// CA unless the inventory says otherwise, so an edge cert signed by anyone
/// else — which is every deployment holding a public certificate — fails the
/// handshake and reports a healthy proxy as absent, with nothing in the text
/// suggesting a certificate was involved. An operator reads "port busy" and
/// goes to `lsof` while the proxy serves customers beside them.
///
/// So the message NAMES THE TRUST ANCHOR IT ACTUALLY USED. That single line
/// is the whole diagnostic: seeing `.../certs/ca.crt` when the edge holds a
/// public cert ends the investigation immediately.
///
/// Exercised by `tools/edge_ca_trust_drill.sh`.
fn proxy_down_help(inv: &Inventory, proxy: &str, dial: &str) -> String {
    let mut m = format!("proxy {proxy} (dialled at {dial}) never answered PROXYSTATS within 10s");
    if inv.client_tls {
        // The SAME derivation the dial used, not a second copy of it.
        let trust = edge_trust_path(inv);
        m.push_str(&format!(
            "\n  the edge speaks TLS, and flintctl validated its certificate against:\n    {trust}"
        ));
        if inv.edge_trust.is_none() {
            m.push_str(
                "\n  which is this fleet's INTERNAL CA — the inventory declares no `edge-trust`.\
                 \n  If this edge serves a public certificate, name the bundle that signs it:\
                 \n    edge-trust /etc/pki/tls/certs/ca-bundle.crt",
            );
        }
    }
    m.push_str(
        "\n  other causes: the port is held by something else, or the proxy exited at\
         \n  startup — check logs/proxy-*.log under the statedir.",
    );
    m
}

fn proxy_dial(inv: &Inventory, i: usize) -> String {
    if let Some(adv) = inv.proxy_advertise.get(i) {
        return adv.clone();
    }
    let bind = &inv.proxies[i];
    match inv.proxy_hosts.get(i) {
        Some(host) => format!("{host}:{}", port_of(bind)),
        None => bind.clone(),
    }
}

fn call_seq(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    cmds: &[&[&str]],
    read_timeout: Duration,
) -> std::io::Result<Value> {
    call_seq_on(addr, tls, cmds, read_timeout, false)
}

/// `edge = true` dials the client port (server name = the dialled host, edge
/// cert SANs) instead of the mesh (fixed internal SNI, mutual auth).
fn call_seq_on(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    cmds: &[&[&str]],
    read_timeout: Duration,
    edge: bool,
) -> std::io::Result<Value> {
    let mut s = if edge {
        flint_tls::connect_edge(addr, tls)?
    } else {
        flint_tls::connect(addr, tls)?
    };
    s.set_read_timeout(Some(read_timeout))?;
    s.set_write_timeout(Some(Duration::from_millis(1500)))?;
    let mut last = Value::Simple(String::new());
    let mut buf = Vec::new();
    let mut chunk = [0u8; 16384];
    for args in cmds {
        let frame = Value::Array(Some(
            args.iter()
                .map(|a| Value::Bulk(Some(a.as_bytes().to_vec())))
                .collect(),
        ));
        let mut out = Vec::new();
        encode(&frame, &mut out);
        s.write_all(&out)?;
        loop {
            match decode(&buf) {
                Ok(Decoded::Complete(v, used)) => {
                    buf.drain(..used);
                    if let Value::Error(e) = &v {
                        return Err(std::io::Error::other(e.clone()));
                    }
                    last = v;
                    break;
                }
                Ok(Decoded::NeedMore) => {
                    let n = s.read(&mut chunk)?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "closed",
                        ));
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("{e:?}"),
                    ));
                }
            }
        }
    }
    Ok(last)
}

fn call_to(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    args: &[&str],
    read_timeout: Duration,
) -> std::io::Result<Value> {
    let mut s = flint_tls::connect(addr, tls)?;
    s.set_read_timeout(Some(read_timeout))?;
    s.set_write_timeout(Some(Duration::from_millis(1500)))?;
    let frame = Value::Array(Some(
        args.iter()
            .map(|a| Value::Bulk(Some(a.as_bytes().to_vec())))
            .collect(),
    ));
    let mut out = Vec::new();
    encode(&frame, &mut out);
    s.write_all(&out)?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 16384];
    loop {
        match decode(&buf) {
            Ok(Decoded::Complete(v, _)) => return Ok(v),
            Ok(Decoded::NeedMore) => {
                let n = s.read(&mut chunk)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "closed",
                    ));
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{e:?}"),
                ));
            }
        }
    }
}

// ---------- reporting a refused admin call ----------
//
// `call` hands back a server's `-ERR` as `Ok(Value::Error(_))` — a
// SUCCESSFUL `Result` carrying a refusal — which makes both lazy spellings
// misreport it. `panic!("{reply:?}")` buries a perfectly good
// `ERR tenant exists` inside `Ok(Error("ERR tenant exists"))`, prints a
// backtrace hint, and exits with a panic code; `.expect(..)` is worse — it
// treats the refusal as success and carries on. Everything below funnels
// through these so an operator gets the SERVER'S OWN sentence and a status
// a script can branch on. (Exit 1 = the command was refused; `upgrade`
// keeps its own 3 for "aborted mid-roll, fleet needs a look".)

/// End the command with our own message: stderr, clean nonzero exit.
fn die(msg: &str) -> ! {
    eprintln!("flintctl: {msg}");
    std::process::exit(1)
}

/// One line for a failed call: the server's error text when it answered,
/// the transport error when it did not.
fn reply_err(reply: &std::io::Result<Value>) -> String {
    match reply {
        Ok(Value::Error(e)) => e.clone(),
        // Unreachable CP, TLS refusal, timeout, short read.
        Err(e) => e.to_string(),
        Ok(other) => format!("unexpected reply {other:?}"),
    }
}

/// End the command with the SERVER'S message.
fn fail(what: &str, reply: &std::io::Result<Value>) -> ! {
    die(&format!("{what}: {}", reply_err(reply)))
}

/// Run an admin call for its effect; any refusal ends the command.
fn must(what: &str, reply: std::io::Result<Value>) -> Value {
    match reply {
        Ok(v) if !matches!(v, Value::Error(_)) => v,
        other => fail(what, &other),
    }
}

/// The PING budget for a freshly (re)spawned replica. A wiped node
/// full-syncs its checkpoint before binding its listener (see
/// flint-server's startup order), so this must cover the transfer on a
/// loaded fleet — `node-ready-s` in the inventory raises it.
fn node_ready_budget(inv: &Inventory) -> Duration {
    Duration::from_secs(inv.node_ready_s.unwrap_or(15))
}

fn wait_pong(addr: &str, tls: &Option<Arc<flint_tls::ClientConfig>>, budget: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < budget {
        if matches!(call(addr, tls, &["PING"]), Ok(Value::Simple(s)) if s == "PONG") {
            return true;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    false
}

fn info_field(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    field: &str,
) -> Option<String> {
    let Ok(Value::Bulk(Some(raw))) = call(addr, tls, &["FLINTINFO"]) else {
        return None;
    };
    String::from_utf8_lossy(&raw)
        .split(['\r', '\n'])
        .find(|l| l.starts_with(field))
        .map(|l| l.trim_start_matches(field).trim().to_string())
}

/// One `key:` line out of a CPINFO response (ADR-0014 D1). Separate from
/// `info_field` because that one asks FLINTINFO, which only data nodes
/// serve — asking a CP for it is how "the CP has no build" reads as "the
/// CP is down".
fn cpinfo_field(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    field: &str,
) -> Option<String> {
    let Ok(Value::Bulk(Some(raw))) = call(addr, tls, &["CPINFO"]) else {
        return None;
    };
    String::from_utf8_lossy(&raw)
        .split(['\r', '\n'])
        .find(|l| l.starts_with(field))
        .map(|l| l.trim_start_matches(field).trim().to_string())
}

/// The `controller:` rows a CP is repeating back. `None` when the CP could
/// not be reached at all, which is different from an empty list ("reached
/// it, nobody has registered") — the caller renders those differently
/// because only one of them means the controller might be missing.
fn cpinfo_controllers(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
) -> Option<Vec<String>> {
    let Ok(Value::Bulk(Some(raw))) = call(addr, tls, &["CPINFO"]) else {
        return None;
    };
    Some(
        String::from_utf8_lossy(&raw)
            .split(['\r', '\n'])
            .filter(|l| l.starts_with("controller:"))
            .map(|l| l.trim_start_matches("controller:").trim().to_string())
            .collect(),
    )
}

/// How many fleet-journal lines the status document carries. The HEAD, not
/// the journal: this is a snapshot taken beside the fleet's configuration,
/// and a caller who wants history has `CPJOURNALREAD` directly.
const JOURNAL_HEAD_N: usize = 10;

/// The newest fleet-journal lines, as the CP serves them.
///
/// `None` when no CP answered, `Some(vec![])` when one did and the journal is
/// empty — the same distinction `controllers` makes, and for the same reason:
/// during a roll, "nothing could be asked" and "nothing has happened" look
/// identical once collapsed, and only one of them is reassuring.
fn journal_head(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    n: usize,
) -> Option<Vec<String>> {
    let n = n.to_string();
    let Ok(Value::Bulk(Some(raw))) = call(addr, tls, &["CPJOURNALREAD", &n]) else {
        return None;
    };
    Some(
        String::from_utf8_lossy(&raw)
            .split('\n')
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .map(str::to_string)
            .collect(),
    )
}

/// One `key:` line out of a proxy's PROXYSTATS.
///
/// Admin-gating tolerance matches `proxy_up`: a -NOAUTH proxy is up, it just
/// will not tell us its build without the admin token, and reporting `-`
/// there is honest where reporting DOWN would not be.
///
/// THE EDGE DIAL IS THE POINT. This used to open with
///
///     if inv.client_tls { return None; }
///
/// — giving up without attempting the call, because the call it would then
/// make was plaintext (`&None`) against a TLS edge. Correct as far as it
/// went, and it meant `roll_edge`, which treats "no build" as fatal, ABORTED
/// EVERY UPGRADE on a client-TLS fleet: the playground and every production
/// deployment. The rc.47 roll on 2026-08-09 rolled all six seats, then:
///
///     == UPGRADE ABORTED rolling proxy-7379: came up but would not report
///        a build ... this roll cannot be verified
///
/// while the proxy was serving happily and answering PROXYSTATS over its
/// edge with `build:v0.1.0-rc.47`. This is #102 in a second place.
///
/// `call_seq_on(.., edge)` already dials the client port with the edge SNI,
/// and `edge_tls_client` is None on a plaintext fleet, so ONE path now
/// serves both rather than a branch that opts out of asking.
fn proxystats_field(inv: &Inventory, i: usize, field: &str) -> Option<String> {
    let tls = edge_tls_client(inv);
    // PRESENT THE ADMIN TOKEN when the inventory has one. PROXYSTATS is an
    // operator surface: once the CP has pushed the admin digest (ADR-0006
    // D4) the proxy refuses it pre-auth, so an unauthenticated read returns
    // -NOAUTH and this function returned None — "would not report a build".
    //
    // `roll_edge` treats that as fatal, so `upgrade` aborted at the last
    // seat on every admin-token fleet, after rolling all of them. Same
    // shape as the proxy_up regression beside it and found the same way:
    // the drill for one uncovered the other.
    //
    // The token is right there in the inventory that named the proxy, and
    // `verify` already dials this way — [AUTH, cmd] over one connection is
    // exactly what call_seq_on's sequence is for.
    let cmds: Vec<Vec<&str>> = match inv.admin_token.as_deref() {
        Some(tok) => vec![vec!["AUTH", tok], vec!["PROXYSTATS"]],
        None => vec![vec!["PROXYSTATS"]],
    };
    let seq: Vec<&[&str]> = cmds.iter().map(|c| c.as_slice()).collect();
    let Ok(Value::Bulk(Some(raw))) = call_seq_on(
        &proxy_dial(inv, i),
        &tls,
        &seq,
        Duration::from_millis(1500),
        inv.client_tls,
    ) else {
        return None;
    };
    String::from_utf8_lossy(&raw)
        .split(['\r', '\n'])
        .find(|l| l.starts_with(field))
        .map(|l| l.trim_start_matches(field).trim().to_string())
}

// ---------- process runner (local; pidfiles for stop/status) ----------

// flintctl is a short-lived CLI: spawned fleet processes deliberately
// OUTLIVE it (pidfiles are the handle; `stop` reaps by pid). There is no
// parent left to wait() — macOS/launchd reparents them — so the zombie
// lint does not apply to this design.
#[allow(clippy::zombie_processes)]
/// Which MACHINE a fleet seat's process lives on.
///
/// v1 ran every process on the machine flintctl itself runs on, and the
/// primitives quietly assume it: `wait_port_free` proves a port is free by
/// BINDING it, `pids_matching` reads `ps`, a pidfile is a path on one disk.
/// Each is a statement about a single host, and each is silently wrong when
/// aimed at a seat somewhere else — `kill_pidfile` in particular would read a
/// local pidfile, find nothing, and report success.
///
/// So a remote seat does not get a reimplementation of those checks over ssh;
/// it gets the SAME code, executed on the host it is about, by invoking that
/// host's own `flintctl host-*`. One implementation of each invariant, two
/// transports. Reimplementing them as shell one-liners is exactly how rc.15
/// shipped a port check that could never pass on Linux.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Runner {
    Local,
    Ssh {
        target: String,
        key: Option<String>,
        sudo: bool,
    },
}

/// Single-quote for `sh -c`: wrap in quotes, and close/escape/reopen for any
/// embedded quote. Fleet arguments carry statedir paths and tokens; one
/// unquoted space would silently truncate an argument list.
fn sh_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', r"'\''"))
}

impl Runner {
    /// The argv that runs `argv` on this runner's host.
    fn wrap(&self, argv: &[String]) -> Vec<String> {
        match self {
            Runner::Local => argv.to_vec(),
            Runner::Ssh { target, key, sudo } => {
                let mut out: Vec<String> = vec!["ssh".into(), "-o".into(), "BatchMode=yes".into()];
                out.extend([
                    "-o".to_string(),
                    "StrictHostKeyChecking=accept-new".to_string(),
                    "-o".to_string(),
                    "ConnectTimeout=10".to_string(),
                ]);
                if let Some(k) = key {
                    out.extend(["-i".to_string(), k.clone()]);
                }
                out.push(target.clone());
                let remote = argv
                    .iter()
                    .map(|a| sh_quote(a))
                    .collect::<Vec<_>>()
                    .join(" ");
                out.push(if *sudo {
                    format!("sudo -n {remote}")
                } else {
                    remote
                });
                out
            }
        }
    }

    fn output(&self, argv: &[String]) -> std::io::Result<std::process::Output> {
        let full = self.wrap(argv);
        Command::new(&full[0]).args(&full[1..]).output()
    }

    /// Copy a local file to `dst` on this runner's host.
    ///
    /// Staged through /tmp then `install`ed, because the destinations are
    /// root-owned (`/var/lib/flint/certs`, `/opt/flint/bin`) and scp runs as
    /// the login user.
    fn send_file(&self, src: &str, dst: &str, mode: &str) -> Result<(), String> {
        match self {
            Runner::Local => {
                if src == dst {
                    return Ok(());
                }
                if let Some(parent) = std::path::Path::new(dst).parent() {
                    std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {dst}: {e}"))?;
                }
                std::fs::copy(src, dst)
                    .map(|_| ())
                    .map_err(|e| format!("copy {src} -> {dst}: {e}"))
            }
            Runner::Ssh { target, key, .. } => {
                let base = std::path::Path::new(dst)
                    .file_name()
                    .map(|f| f.to_string_lossy().to_string())
                    .unwrap_or_else(|| "flint-push".into());
                let stage = format!("/tmp/flintctl-push-{base}");
                let mut scp: Vec<String> = vec!["scp".into(), "-o".into(), "BatchMode=yes".into()];
                scp.extend([
                    "-o".to_string(),
                    "StrictHostKeyChecking=accept-new".to_string(),
                ]);
                if let Some(k) = key {
                    scp.extend(["-i".to_string(), k.clone()]);
                }
                scp.push(src.to_string());
                scp.push(format!("{target}:{stage}"));
                let out = Command::new(&scp[0])
                    .args(&scp[1..])
                    .output()
                    .map_err(|e| format!("scp {src}: {e}"))?;
                if !out.status.success() {
                    return Err(format!(
                        "scp {src} -> {target}: {}",
                        String::from_utf8_lossy(&out.stderr).trim()
                    ));
                }
                let install = vec![
                    "install".to_string(),
                    "-D".to_string(),
                    "-m".to_string(),
                    mode.to_string(),
                    stage.clone(),
                    dst.to_string(),
                ];
                let out = self
                    .output(&install)
                    .map_err(|e| format!("install {dst}: {e}"))?;
                if !out.status.success() {
                    return Err(format!(
                        "install {dst} on {target}: {}",
                        String::from_utf8_lossy(&out.stderr).trim()
                    ));
                }
                let _ = self.output(&["rm".to_string(), "-f".to_string(), stage]);
                Ok(())
            }
        }
    }

    fn label(&self) -> String {
        match self {
            Runner::Local => "local".into(),
            Runner::Ssh { target, .. } => target.clone(),
        }
    }

    fn is_remote(&self) -> bool {
        matches!(self, Runner::Ssh { .. })
    }
}

/// True when `host` names an address THIS machine holds.
///
/// Binding it is the proof — the same discipline `wait_port_free` rests on.
/// Binding port 0 on an address you own succeeds; on another machine's
/// address it fails with EADDRNOTAVAIL. That beats parsing `ifconfig`, and it
/// needs no dependency.
fn is_local_host(host: &str) -> bool {
    use std::net::{IpAddr, TcpListener, ToSocketAddrs};
    use std::sync::{Mutex, OnceLock};
    if host.is_empty() || host == "0.0.0.0" || host == "::" || host == "localhost" {
        return true;
    }
    static MEMO: OnceLock<Mutex<std::collections::HashMap<String, bool>>> = OnceLock::new();
    let memo = MEMO.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    if let Ok(m) = memo.lock()
        && let Some(hit) = m.get(host)
    {
        return *hit;
    }
    let ips: Vec<IpAddr> = match host.parse::<IpAddr>() {
        Ok(ip) => vec![ip],
        // A DNS name has to be resolved before it can be judged.
        Err(_) => (host, 0u16)
            .to_socket_addrs()
            .map(|it| it.map(|s| s.ip()).collect())
            .unwrap_or_default(),
    };
    let local = ips.iter().any(|ip| TcpListener::bind((*ip, 0)).is_ok());
    if let Ok(mut m) = memo.lock() {
        m.insert(host.to_string(), local);
    }
    local
}

/// The host part of `host:port` (IPv6 literals keep their brackets).
fn host_of(addr: &str) -> &str {
    match addr.rsplit_once(':') {
        Some((h, _)) => h,
        None => addr,
    }
}

fn runner_for_host(inv: &Inventory, host: &str) -> Runner {
    if is_local_host(host) {
        return Runner::Local;
    }
    let user = inv.ssh_user.as_deref().unwrap_or_else(|| {
        die(&format!(
            "inventory places a seat on {host}, which is not this machine, but declares no \
             `ssh-user` — flintctl cannot start or stop a process it has no way to reach"
        ))
    });
    Runner::Ssh {
        target: format!("{user}@{host}"),
        key: inv.ssh_key.clone(),
        sudo: inv.ssh_sudo,
    }
}

/// The runner for the machine serving `addr`.
fn runner_for(inv: &Inventory, addr: &str) -> Runner {
    runner_for_host(inv, host_of(addr))
}

/// Proxies bind a wildcard, so their address does not name their machine —
/// `proxy-host` does, positionally.
/// Command family -> the co-processor binary that serves it (ADR-0010). A
/// table, not a convention, because the mapping is a deployment fact the
/// inventory must be able to REFUSE at parse time: an unknown family would
/// otherwise spawn nothing and leave the proxy routing `VEC.*` into a hole.
const COPROC_FAMILIES: &[(&str, &str)] = &[("VEC.", "flint-vec")];

fn coproc_bin(family: &str) -> Option<&'static str> {
    let f = family.to_ascii_uppercase();
    COPROC_FAMILIES
        .iter()
        .find(|(name, _)| *name == f)
        .map(|(_, bin)| *bin)
}

fn coproc_runner(inv: &Inventory, i: usize) -> Runner {
    runner_for(inv, &inv.coprocs[i].1)
}

/// Seat name (pidfile/log stem): family without its dot, plus the port —
/// `vec-7600`. Two co-processors of DIFFERENT families can share a host, so
/// the family has to be in the name.
fn coproc_seat(family: &str, addr: &str) -> String {
    format!(
        "{}-{}",
        family.trim_end_matches('.').to_ascii_lowercase(),
        port_of(addr)
    )
}

/// A co-processor seat's flags. It presents the SERVER-ONLY `coproc` leaf
/// (ADR-0010 D2): serverAuth without clientAuth, so a compromised
/// co-processor cannot turn around and dial the mesh as a member. That is
/// also why it does not get the `int` pair every other seat uses.
///
/// `--index-mem-bytes` is always passed. The binary's own default is 0 =
/// unlimited, which is right for a laptop and wrong for a fleet: the index
/// is RAM-resident (ADR-0017 v0), so an uncapped one grows until the host
/// OOMs and takes the seat's PROXYCHAN writes with it.
fn coproc_args(inv: &Inventory, i: usize) -> Vec<String> {
    let (_, addr) = &inv.coprocs[i];
    let d = &inv.statedir;
    let bind_host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or("127.0.0.1");
    let mut args = vec![
        "--port".to_string(),
        port_of(addr).to_string(),
        "--bind".to_string(),
        bind_host.to_string(),
        "--index-mem-bytes".to_string(),
        inv.coproc_index_bytes
            .unwrap_or(DEFAULT_COPROC_INDEX_BYTES)
            .to_string(),
    ];
    if inv.tls {
        args.extend([
            "--internal-ca".to_string(),
            format!("{d}/certs/ca.crt"),
            "--internal-cert".to_string(),
            format!("{d}/certs/coproc.crt"),
            "--internal-key".to_string(),
            format!("{d}/certs/coproc.key"),
        ]);
    }
    if inv.client_tls {
        args.push("--client-tls".to_string());
    }
    args
}

/// The `--families` mapping every proxy gets: `VEC.=addr1,addr2;OTHER.=addr3`.
/// Built from the inventory so the proxy's routing table and the seats that
/// were actually spawned cannot disagree.
fn families_arg(inv: &Inventory) -> Option<String> {
    if inv.coprocs.is_empty() {
        return None;
    }
    let mut by_family: Vec<(String, Vec<String>)> = Vec::new();
    for (family, addr) in &inv.coprocs {
        match by_family.iter_mut().find(|(f, _)| f == family) {
            Some((_, addrs)) => addrs.push(addr.clone()),
            None => by_family.push((family.clone(), vec![addr.clone()])),
        }
    }
    Some(
        by_family
            .into_iter()
            .map(|(f, addrs)| format!("{f}={}", addrs.join(",")))
            .collect::<Vec<_>>()
            .join(";"),
    )
}

fn proxy_runner(inv: &Inventory, i: usize) -> Runner {
    match inv.proxy_hosts.get(i) {
        Some(h) => runner_for_host(inv, h),
        None => runner_for(inv, &inv.proxies[i]),
    }
}

/// The controller serves nothing, so it has no address to derive from.
fn controller_runner(inv: &Inventory) -> Runner {
    match &inv.controller_host {
        Some(h) => runner_for_host(inv, h),
        None => Runner::Local,
    }
}

fn backup_runner(inv: &Inventory) -> Runner {
    match &inv.backup_host {
        Some(h) => runner_for_host(inv, h),
        None => Runner::Local,
    }
}

fn agent_runner(inv: &Inventory) -> Runner {
    match &inv.agent {
        Some(a) => runner_for(inv, a),
        None => Runner::Local,
    }
}

/// Every distinct machine this inventory places a seat on.
fn all_runners(inv: &Inventory) -> Vec<Runner> {
    let mut out = vec![Runner::Local];
    let mut push = |r: Runner| {
        if !out.contains(&r) {
            out.push(r);
        }
    };
    // EVERY CP seat, not just the first. all_runners drives push-bins, cert
    // distribution and the orphan sweep, so a host carrying ONLY seat 2 or 3
    // was invisible to all of them: the production rehearsal distributed
    // binaries and certs to two of three remotes, then panicked spawning
    // seat 2 on a host with neither. The seat started (the spawn is by
    // inventory), which is precisely why the omission was silent until the
    // remote flintctl printed its usage banner.
    for seat in &inv.cp {
        push(runner_for(inv, seat));
    }
    for pair in &inv.pairs {
        for node in pair {
            push(runner_for(inv, node));
        }
    }
    for i in 0..inv.proxies.len() {
        push(proxy_runner(inv, i));
    }
    if inv.controller {
        push(controller_runner(inv));
    }
    if inv.agent.is_some() {
        push(agent_runner(inv));
    }
    if inv.backup_to.is_some() {
        push(backup_runner(inv));
    }
    // A co-processor commonly gets its OWN host (ADR-0010 D5 keeps its CPU
    // off the proxy's), so omitting it here would hide that host from
    // push-bins, cert distribution and the orphan sweep — the same silent
    // omission the CP loop above documents, one role later.
    for i in 0..inv.coprocs.len() {
        push(coproc_runner(inv, i));
    }
    out
}

fn spawn_env(
    inv: &Inventory,
    r: &Runner,
    name: &str,
    bin: &str,
    args: &[String],
    envs: &[(String, String)],
) {
    if let Runner::Ssh { .. } = r {
        let mut argv = vec![
            format!("{}/flintctl", inv.bins),
            "host-spawn".into(),
            inv.statedir.clone(),
            inv.bins.clone(),
            name.to_string(),
            bin.to_string(),
        ];
        for (k, v) in envs {
            argv.push("--env".into());
            argv.push(format!("{k}={v}"));
        }
        argv.push("--".into());
        argv.extend(args.iter().cloned());
        let out = r
            .output(&argv)
            .unwrap_or_else(|e| panic!("spawn {name} on {}: {e}", r.label()));
        let text = String::from_utf8_lossy(&out.stdout);
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(
            out.status.success(),
            "spawn {name} on {}: {}",
            r.label(),
            err.trim()
        );
        eprintln!("  started {name} on {} ({})", r.label(), text.trim());
        return;
    }
    local_spawn_env(&inv.statedir, &inv.bins, name, bin, args, envs);
}

/// A seat's log, opened for APPEND, with one generation of rotation.
///
/// It used to be `File::create`, which truncates. Every respawn therefore
/// erased the previous run's output, and a seat that starts, runs briefly
/// and dies destroys its own evidence on the way back up — the more it
/// crash-loops, the less there is to read. That is what docs/bugs/0005-oneshot-kills-its-own-seat.md ran into: a
/// replica exiting silently ~90s after a clean start, investigated against
/// a log that only ever held the newest attempt, which looked like a
/// perfectly healthy boot every time.
///
/// Appending is bounded by one rotation rather than left to grow forever;
/// 64 MiB is far above what a seat writes in normal operation (the
/// playground's busiest node reached ~1 MB in two weeks) and small enough
/// that a crash loop cannot fill a disk. Keeping ONE generation is the
/// trade: enough to hold the run before the one that broke, not so much
/// that the guard stops being a guard.
fn open_seat_log(statedir: &str, name: &str) -> std::fs::File {
    let path = format!("{statedir}/logs/{name}.log");
    const MAX_BYTES: u64 = 64 * 1024 * 1024;
    if std::fs::metadata(&path)
        .map(|m| m.len() > MAX_BYTES)
        .unwrap_or(false)
    {
        let _ = std::fs::rename(&path, format!("{path}.1"));
    }
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("log file")
}

/// Start one seat on THIS machine, recording its pid.
fn local_spawn_env(
    statedir: &str,
    bins: &str,
    name: &str,
    bin: &str,
    args: &[String],
    envs: &[(String, String)],
) {
    let mut log = open_seat_log(statedir, name);
    // A banner, because appending only helps if runs can be told apart.
    let _ = writeln!(
        log,
        "=== flintctl start {name} at_ms={} bin={bin} args={} ===",
        now_ms(),
        args.join(" ")
    );
    let _ = log.flush();
    // stdout as well as stderr, into the same file and in order. It used to
    // go to /dev/null, so anything a seat reported there was gone before
    // anyone could read it.
    let log_out = log.try_clone().expect("clone seat log handle");
    let mut cmd = Command::new(format!("{bins}/{bin}"));
    cmd.args(args);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    // Deliberately never waited on: these are the fleet's long-lived daemons,
    // and flintctl exits while they keep serving. Their lifecycle is the
    // pidfile plus `stop_seat`, not this process's exit status.
    #[allow(clippy::zombie_processes)]
    let child = cmd
        .stdout(log_out)
        .stderr(log)
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {bin} ({name}): {e}"));
    std::fs::write(
        format!("{statedir}/pids/{name}.pid"),
        child.id().to_string(),
    )
    .expect("pidfile");
    eprintln!("  started {name} (pid {})", child.id());
}

fn spawn(inv: &Inventory, r: &Runner, name: &str, bin: &str, args: &[String]) {
    spawn_env(inv, r, name, bin, args, &[]);
}

// wipe_node (the orchestrator-side wrapper) is gone: every rejoin path now
// writes the NEEDS_RESEED marker instead (#187/#190) and lets the server
// choose rewind-or-reseed. `host-wipe-node` and local_wipe_node stay — the
// remote dispatch surface must keep serving an older orchestrator's calls.

/// Remove one seat's data dir. Refuses anything that is not a node dir under
/// the given statedir — this runs as root over ssh, so a malformed name must
/// not be able to name something else.
fn local_wipe_node(statedir: &str, name: &str) -> Result<(), String> {
    if statedir.is_empty() || !name.starts_with("node-") || name.contains('/') {
        return Err(format!("refusing to wipe {statedir:?}/{name:?}"));
    }
    let dir = format!("{statedir}/{name}");
    match std::fs::remove_dir_all(&dir) {
        Ok(()) => Ok(()),
        // Already gone is the desired end state.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(format!("remove {dir}: {e}")),
    }
}

/// The re-seed marker filename, shared by contract with flint-server (its
/// NEEDS_RESEED const): a data dir carrying it is a superseded copy the
/// server must not tail from as-is. Writing the MARKER instead of wiping
/// (#187) is what lets the server choose the cheap continuation — rewind to
/// a local snapshot at or before the promotion fence — and reserve the full
/// re-seed for when no safe snapshot exists.
const NEEDS_RESEED_MARKER: &str = "NEEDS_RESEED";

/// Mark one seat's data dir as needing re-seed-or-rewind before it may tail
/// again. A missing dir is fine — a fresh seat has nothing to mark.
fn mark_reseed_node(inv: &Inventory, r: &Runner, port: u16, why: &str) -> Result<(), String> {
    if r.is_remote() {
        let argv = vec![
            format!("{}/flintctl", inv.bins),
            "host-mark-reseed".into(),
            inv.statedir.clone(),
            format!("node-{port}"),
            why.to_string(),
        ];
        let out = r
            .output(&argv)
            .map_err(|e| format!("mark-reseed node-{port} on {}: {e}", r.label()))?;
        if !out.status.success() {
            return Err(format!(
                "mark-reseed node-{port} on {}: {}",
                r.label(),
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        return Ok(());
    }
    local_mark_reseed(&inv.statedir, &format!("node-{port}"), why)
}

/// Same name discipline as local_wipe_node: this runs as root over ssh, so a
/// malformed name must not be able to write outside the statedir.
fn local_mark_reseed(statedir: &str, name: &str, why: &str) -> Result<(), String> {
    if statedir.is_empty() || !name.starts_with("node-") || name.contains('/') {
        return Err(format!("refusing to mark {statedir:?}/{name:?}"));
    }
    let dir = std::path::Path::new(statedir).join(name);
    if !dir.exists() {
        return Ok(());
    }
    std::fs::write(dir.join(NEEDS_RESEED_MARKER), format!("{why}\n"))
        .map_err(|e| format!("write {}/{NEEDS_RESEED_MARKER}: {e}", dir.display()))
}

fn kill_pidfile(inv: &Inventory, r: &Runner, name: &str) {
    if r.is_remote() {
        let argv = vec![
            format!("{}/flintctl", inv.bins),
            "host-kill-pidfile".into(),
            inv.statedir.clone(),
            name.to_string(),
        ];
        let _ = r.output(&argv);
        return;
    }
    local_kill_pidfile(&inv.statedir, name);
}

fn local_kill_pidfile(dir: &str, name: &str) {
    let path = format!("{dir}/pids/{name}.pid");
    if let Ok(pid) = std::fs::read_to_string(&path) {
        let _ = Command::new("kill").args(["-9", pid.trim()]).status();
        let _ = std::fs::remove_file(&path);
    }
}

/// Pids running `bin` with `ident` as an EXACT argument.
///
/// Exact-token, never substring: `<statedir>/node-700` is a prefix of
/// `<statedir>/node-7001`, and killing the wrong seat mid-upgrade would be
/// considerably worse than the bug this exists to fix.
fn pids_matching(bin: &str, ident: &str) -> Vec<u32> {
    let Ok(out) = Command::new("ps").args(["-eo", "pid=,args="]).output() else {
        return Vec::new();
    };
    pids_in_ps(
        &String::from_utf8_lossy(&out.stdout),
        bin,
        ident,
        Some(std::process::id()),
    )
}

/// Is this seat's process running on its host? Same exact-token discipline as
/// `pids_matching`, but usable through a Runner: the ps listing is taken ON
/// the seat's host and parsed here, so there is one parser and two
/// transports — no new host-* verb for an old remote flintctl to lack.
fn seat_alive(r: &Runner, bin: &str, ident: &str) -> bool {
    if r.is_remote() {
        let Ok(out) = r.output(&["ps".to_string(), "-eo".into(), "pid=,args=".into()]) else {
            return false;
        };
        // No self-pid to exclude: OUR pid numbers a process on the
        // orchestrator, and the listing is another machine's. Passing it
        // would let a remote seat that happens to hold the same number be
        // read as dead, and `start` would spawn a duplicate beside it.
        return !pids_in_ps(&String::from_utf8_lossy(&out.stdout), bin, ident, None).is_empty();
    }
    !pids_matching(bin, ident).is_empty()
}

fn pids_in_ps(ps: &str, bin: &str, ident: &str, exclude: Option<u32>) -> Vec<u32> {
    let mut hits = Vec::new();
    for line in ps.lines() {
        let Some((pid, args)) = line.trim().split_once(char::is_whitespace) else {
            continue;
        };
        let Ok(pid) = pid.trim().parse::<u32>() else {
            continue;
        };
        // NEVER match a flintctl. Over ssh the remote command's own argv is
        //
        //   sudo -n .../flintctl host-stop-seat <statedir> node-7002 \
        //        flint-server /var/lib/flint/node-7002 7002
        //
        // which contains the binary name AND the ident as an exact token, so
        // it satisfies both tests below. Excluding only our own pid is not
        // enough: the `sudo` PARENT matches too, and killing it takes the ssh
        // session down with it — the kill lands, then reports failure with an
        // empty error. Locally this cannot happen, because there the ident is
        // a function argument rather than a command line. A flintctl is never
        // a fleet seat, so skipping them is exact rather than a heuristic.
        if args.contains("flintctl") {
            continue;
        }
        if Some(pid) != exclude && args.contains(bin) && args.split_whitespace().any(|t| t == ident)
        {
            hits.push(pid);
        }
    }
    hits
}

/// Stop one fleet seat and PROVE it stopped before anything replaces it.
///
/// `kill_pidfile` alone is a promise, not a fact. A pidfile records only the
/// most recent start, so a fleet restarted out of band leaves pids that no
/// longer exist — and killing a dead pid succeeds silently at doing nothing.
///
/// That is not hypothetical. On the playground every pidfile for the control
/// plane, both nodes and the proxy was stale, so an upgrade killed nothing,
/// started a SECOND node on a port the original still held, watched the new
/// one fail to bind and exit, and then read the build from the SURVIVOR. It
/// aborted with "reports build 0.0.1" — correct, but for a reason two steps
/// removed from the actual fault. And that abort only happened because
/// `--version-tag` was passed; without it the build check is skipped and the
/// same roll reports success having changed nothing.
///
/// So: kill what the pidfile claims, then kill anything still answering to
/// this seat by argv (the `stop` sweep's logic narrowed to one process), and
/// do not return until nothing matches.
/// Wait until `port` can actually be bound.
///
/// "The process is gone from ps" is a PROXY for the precondition that matters,
/// and not a reliable one: `kill` returns as soon as the signal is queued, not
/// when the target has died and released its listener. Rolling the playground
/// to rc.14 hit exactly that window — the replacement control plane bound
/// microseconds too early, died with `AddrInUse`, and left the fleet with NO
/// control plane. The roll reported the truth ("did not answer after the
/// binary swap") but the damage was done by then.
///
/// So assert the real precondition. Binding both the wildcard and loopback
/// forms covers seats that bind either.
fn wait_port_free(port: u16, budget: Duration) -> Result<(), String> {
    use std::net::TcpListener;
    let deadline = Instant::now() + budget;
    loop {
        // ONE AT A TIME, each dropped before the next. Holding the wildcard
        // while probing loopback makes the prober its own conflict: on Linux
        // 0.0.0.0:P and 127.0.0.1:P overlap, so the second bind fails with
        // AddrInUse and the check can NEVER pass. macOS permits the overlap,
        // which is why every local drill went green and the first Linux roll
        // aborted its canary — the same wrong-platform trap as benchmarking
        // an LSM on a laptop.
        let free = [("0.0.0.0", port), ("127.0.0.1", port)]
            .iter()
            .all(|addr| TcpListener::bind(*addr).is_ok());
        if free {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(format!(
                "port {port} still bound after the process was gone — refusing to start a \
                 replacement that would die with AddrInUse and leave nothing serving"
            ));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn stop_seat(
    inv: &Inventory,
    r: &Runner,
    name: &str,
    bin: &str,
    ident: &str,
    port: Option<u16>,
) -> Result<(), String> {
    if r.is_remote() {
        // The whole point of stop_seat is proving a fact about one machine.
        // Ask that machine.
        let mut argv = vec![
            format!("{}/flintctl", inv.bins),
            "host-stop-seat".into(),
            inv.statedir.clone(),
            name.to_string(),
            bin.to_string(),
            ident.to_string(),
        ];
        if let Some(p) = port {
            argv.push(p.to_string());
        }
        let out = r
            .output(&argv)
            .map_err(|e| format!("stop {name} on {}: {e}", r.label()))?;
        if out.status.success() {
            return Ok(());
        }
        let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
        return Err(format!(
            "{name} on {}: {}",
            r.label(),
            if err.is_empty() {
                // ssh exits 255 when the connection itself failed, and says
                // nothing. An empty message here sent one debugging session
                // looking at the wrong end of the pipe.
                format!(
                    "no output, exit {} (255 = the ssh connection failed, not the command)",
                    out.status.code().unwrap_or(-1)
                )
            } else {
                err
            }
        ));
    }
    local_stop_seat(&inv.statedir, name, bin, ident, port)
}

fn local_stop_seat(
    statedir: &str,
    name: &str,
    bin: &str,
    ident: &str,
    port: Option<u16>,
) -> Result<(), String> {
    local_kill_pidfile(statedir, name);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let alive = pids_matching(bin, ident);
        if alive.is_empty() {
            // Gone from ps is necessary but NOT sufficient — see wait_port_free.
            return match port {
                Some(p) => wait_port_free(p, Duration::from_secs(15)),
                None => Ok(()),
            };
        }
        for pid in &alive {
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
        }
        if Instant::now() > deadline {
            return Err(format!(
                "{name}: {bin} still alive as {alive:?} after kill — refusing to start a \
                 replacement that would fail to bind and leave the old build serving"
            ));
        }
        std::thread::sleep(Duration::from_millis(150));
    }
}

/// Push the widowed grace that a pair's CURRENT size implies to every member
/// of it, live, after a topology change.
///
/// "Widowed" has to mean "lost a peer it is supposed to have", not "is alone
/// right now". Those differ exactly when an operator deliberately shrinks a
/// pair: after `decommission`, the survivor is not widowed, it is standalone
/// by decision — and a grace inherited from when it HAD a peer would shed
/// every write once it expired. The decommission drill caught precisely that,
/// reporting "not writable after decommission" ten seconds later.
///
/// The node cannot make this call: from inside the process, "my peer died"
/// and "my peer was retired on purpose" are the same observation. flintctl
/// owns the inventory, so flintctl owns the reconciliation — and it must do
/// it on every path that changes a pair's size, not only at spawn.
///
/// Hot, via FLINTCONFIG: a topology change must not need a restart to become
/// safe, and a restart to become safe is a restart that gets skipped.
fn reconcile_widowed_grace(
    inv: &Inventory,
    members: &[String],
    tls: &Option<Arc<flint_tls::ClientConfig>>,
) {
    // An explicit inventory value is the operator's, and is applied at spawn
    // for every member already. Only flintctl's OWN default is topology-
    // derived, so only that needs revisiting here.
    if inv.widowed_grace_ms.is_some() {
        return;
    }
    let want = if members.len() > 1 {
        DEFAULT_WIDOWED_GRACE_MS
    } else {
        0
    };
    for node in members {
        match call(
            node,
            tls,
            &["FLINTCONFIG", "widowed-grace-ms", &want.to_string()],
        ) {
            Ok(Value::Simple(_)) => {
                eprintln!(
                    "  {node}: widowed-grace-ms={want} (pair size {})",
                    members.len()
                )
            }
            // Not fatal: the seat may be mid-restart, and the spawn path
            // applies the same value. Say so rather than passing silently —
            // a gate that failed to move is the thing worth seeing.
            other => eprintln!(
                "  {node}: could NOT set widowed-grace-ms={want}: {}",
                reply_err(&other)
            ),
        }
    }
}

/// How long a PAIR MEMBER may keep accepting writes with no live replica
/// before it sheds them, when the inventory does not say.
///
/// 10 s deliberately equals the published RPO envelope: the promise is "at
/// most ten seconds' worth of acknowledged writes at risk", and this is the
/// mechanism that makes it true when there is no replica to measure lag
/// against. Pick a different number by setting `widowed-grace-ms`; it wants
/// to be comfortably longer than a promotion plus a replacement replica's
/// first ack, and shorter than the loss you are willing to publish.
const DEFAULT_WIDOWED_GRACE_MS: u64 = 10_000;

/// Per-namespace index RAM a co-processor may hold before it sheds
/// (`coproc-index-bytes`). 1 GiB is a deliberately SMALL default: the
/// ADR-0017 v0 index is RAM-resident, the seat usually shares a host, and
/// the failure mode of guessing high is an OOM that takes the seat out
/// mid-run, where the failure mode of guessing low is a `-QUOTA` the caller
/// sees immediately. Raise it per fleet once you know the host's headroom.
const DEFAULT_COPROC_INDEX_BYTES: u64 = 1024 * 1024 * 1024;

/// How long a master may serve without a successful `CPLEASE` renewal before
/// it self-fences, when the inventory does not say (ADR-0018).
///
/// 5000, not ADR-0004's 3000: under SELF-renewal the fence's tolerance must
/// cover a routine CP leader election (~2.4 s of the lease path dark, measured
/// by ctl_cpha), which the old controller-pushed renewals never had inside
/// their window. At 3000 a leader kill fenced a healthy fleet about as often
/// as not; at 5000 the tolerance is ~3.3 s with fast retry filling the tail.
/// Still seconds-scale, and the reachable-superseded fence stays ~ttl/3.
///
/// THIS IS THE ONLY PLACE THE NUMBER LIVES. Test rigs used to copy it into
/// their inventories — the AWS chaos/soak fleet at 4000, two drills at 3000 —
/// so when ADR-0018 moved the default the rigs silently stayed behind and
/// every chaos result was measured on a fleet tuned tighter than any
/// customer's (#182). An inventory that omits `lease-ttl-ms` inherits this,
/// so rigs should omit it unless they are deliberately probing another value,
/// and `assert_lease_ttl_single_source` in tools/gates.sh fails the build if
/// one reappears. docs/self-hosting.md's table is checked against it by
/// `the_documented_lease_ttl_is_the_one_we_ship`.
const DEFAULT_LEASE_TTL_MS: u64 = 5_000;

/// The binaries `flintctl` starts — the orphan sweep's allowlist. Every
/// co-processor binary in [`COPROC_FAMILIES`] belongs here too: a seat the
/// sweep cannot see survives `stop` and then holds its port against the
/// next `start`.
const FLEET_BINARIES: [&str; 7] = [
    "flint-server",
    "flint-proxy",
    "flint-controlplane",
    "flint-controller",
    "flint-agent",
    "flint-backup",
    "flint-vec",
];

/// Kill fleet processes belonging to THIS inventory that the pidfiles do
/// not know about.
///
/// Why this is needed: pidfiles record only the most recent start, so a
/// `start` that runs while a fleet is already up orphans the earlier set —
/// `stop` then leaves it running forever. (Observed on the playground: a
/// controller from a previous start survived two upgrade cycles. Harmless
/// there because controllers are epoch-fenced and safe to run concurrently
/// — but an orphaned NODE holding a port or a data dir is not harmless.)
///
/// Scoped twice over, because a stray `kill -9` is worse than a stray
/// process: a match requires the command line to name one of OUR binaries
/// AND to carry this inventory's statedir. A second fleet on the same host
/// (different statedir) is untouched, and so is an editor or tail that
/// merely has the path open.
///
/// Caveat: a fully plaintext fleet (`tls off`, `client-tls off`) can leave
/// the proxy with no statedir-derived path in its arguments; that one is
/// only reachable through its pidfile. Every packaged/production fleet is
/// TLS-on, where all five carry it.
/// Sweep every machine the inventory places a seat on — an orphan on host C
/// is exactly as harmful as one here, and rather harder to notice.
fn sweep_orphans(inv: &Inventory) -> usize {
    let mut killed = 0;
    for r in all_runners(inv) {
        if r.is_remote() {
            let argv = vec![
                format!("{}/flintctl", inv.bins),
                "host-sweep".into(),
                inv.statedir.clone(),
            ];
            if let Ok(out) = r.output(&argv) {
                let n: usize = String::from_utf8_lossy(&out.stdout)
                    .trim()
                    .parse()
                    .unwrap_or(0);
                if n > 0 {
                    eprintln!("  swept {n} orphan(s) on {}", r.label());
                }
                killed += n;
            }
        } else {
            killed += local_sweep_orphans(&inv.statedir);
        }
    }
    killed
}

/// Kill every seat THIS machine's pidfiles name.
fn local_stop_all(statedir: &str) {
    match std::fs::read_dir(format!("{statedir}/pids")) {
        Ok(entries) => {
            for e in entries.flatten() {
                let name = e
                    .file_name()
                    .to_string_lossy()
                    .trim_end_matches(".pid")
                    .to_string();
                local_kill_pidfile(statedir, &name);
                println!("stopped {name}");
                eprintln!("  stopped {name}");
            }
        }
        // Missing pidfiles is exactly when the sweep matters most (the
        // directory was wiped, or a start never recorded), so fall through
        // rather than returning early.
        Err(_) => eprintln!("  no pidfiles — sweeping by process instead"),
    }
}

fn local_sweep_orphans(statedir: &str) -> usize {
    let Ok(out) = Command::new("ps").args(["-eo", "pid=,args="]).output() else {
        return 0;
    };
    let me = std::process::id();
    let mut killed = 0;
    for line in String::from_utf8_lossy(&out.stdout).lines() {
        let Some((pid, args)) = line.trim().split_once(char::is_whitespace) else {
            continue;
        };
        let Ok(pid) = pid.trim().parse::<u32>() else {
            continue;
        };
        // Same exclusion as pids_matching: every remote `flintctl host-*`
        // argv carries the statedir, so a sweep that did not skip flintctl
        // would kill the very command performing the sweep.
        if pid == me || args.contains("flintctl") || !args.contains(statedir) {
            continue;
        }
        if !FLEET_BINARIES.iter().any(|b| args.contains(b)) {
            continue;
        }
        let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
        eprintln!("  swept orphan pid {pid} (not in pidfiles)");
        killed += 1;
    }
    killed
}

fn internal_args(inv: &Inventory) -> Vec<String> {
    if !inv.tls {
        return Vec::new();
    }
    let d = &inv.statedir;
    vec![
        "--internal-ca".into(),
        format!("{d}/certs/ca.crt"),
        "--internal-cert".into(),
        format!("{d}/certs/int.crt"),
        "--internal-key".into(),
        format!("{d}/certs/int.key"),
    ]
}

/// `flintctl reload`: re-read the config file and PUSH the hot-reloadable
/// tunables to the RUNNING fleet — no restart. Node knobs go over the mesh
/// via FLINTCONFIG; proxy near-cache via PROXYCACHE. Restart-only knobs
/// (async-queue-cap, controller timing) are reported as needing stop/start.
/// A node that doesn't answer is warned, not fatal — reload is best-effort
/// convergence, `status` shows the truth.
fn reload(inv: &Inventory) {
    let tls = tls_client(inv);
    let node_kv: [(&str, Option<String>); 6] = [
        ("wal-fsync-ms", inv.wal_fsync_ms.map(|v| v.to_string())),
        ("lag-soft-ms", inv.lag_soft_ms.map(|v| v.to_string())),
        ("lag-hard-ms", inv.lag_hard_ms.map(|v| v.to_string())),
        (
            "min-replicas-to-write",
            inv.min_replicas.map(|v| v.to_string()),
        ),
        // Reload pushes only what the inventory SAYS. flintctl's
        // pair-member default is applied at spawn, not here: a reload
        // that silently re-imposed it would overwrite a value an
        // operator had just set live through FLINTCONFIG.
        (
            "widowed-grace-ms",
            inv.widowed_grace_ms.map(|v| v.to_string()),
        ),
        ("max-conns", inv.max_conns.map(|v| v.to_string())),
    ];
    for pair in &inv.pairs {
        for node in pair {
            for (k, v) in node_kv.iter() {
                if let Some(val) = v {
                    match call(node, &tls, &["FLINTCONFIG", k, val]) {
                        Ok(Value::Simple(_)) => println!("  {node}: {k}={val}"),
                        other => {
                            eprintln!("  {node}: FLINTCONFIG {k} rejected: {}", reply_err(&other))
                        }
                    }
                }
            }
        }
    }
    if inv.cache_ttl_ms.is_some() || inv.cache_max_bytes.is_some() {
        // PROXYCACHE needs both; the compiled defaults fill an unset one.
        let ttl = inv.cache_ttl_ms.unwrap_or(300).to_string();
        let maxb = inv.cache_max_bytes.unwrap_or(256 * 1024 * 1024).to_string();
        for i in 0..inv.proxies.len() {
            let proxy = &proxy_dial(inv, i);
            if let Some(tok) = &inv.admin_token {
                let _ = call(proxy, &tls, &["AUTH", tok]);
            }
            match call(proxy, &tls, &["PROXYCACHE", &ttl, &maxb]) {
                Ok(Value::Simple(_)) => println!("  {proxy}: cache ttl={ttl}ms max={maxb}B"),
                other => eprintln!("  {proxy}: PROXYCACHE rejected: {}", reply_err(&other)),
            }
        }
    }
    let mut restart_only = Vec::new();
    if inv.async_queue_cap.is_some() {
        restart_only.push("async-queue-cap");
    }
    if inv.ctl_poll_ms.is_some() || inv.ctl_confirm.is_some() {
        restart_only.push("controller timing (poll-ms/confirm)");
    }
    if inv.ctl_lease_ttl_ms.is_some() {
        restart_only.push("lease-ttl-ms (node restart — ADR-0018)");
    }
    if !restart_only.is_empty() {
        println!(
            "note: {} not hot-reloadable — apply with stop/start",
            restart_only.join(", ")
        );
    }
    println!("== reload complete (hot knobs pushed to the running fleet)");
}

/// Node (flint-server) operator tunables from the inventory — durability,
/// RPO, and admission knobs. Absent keys leave the binary's default, so
/// this never changes behaviour it wasn't explicitly told to.
///
/// `replicated` says whether this seat lives in a pair that HAS a peer. It
/// decides one thing — the widowed grace — and it has to be decided here
/// rather than in the server, because the server cannot know its own
/// topology: a lone flint-server and a pair member that has just lost its
/// peer look identical from inside the process. flintctl reads the
/// inventory, so it is the only component that can tell them apart.
fn node_tuning_args(inv: &Inventory, replicated: bool) -> Vec<String> {
    let mut a = Vec::new();
    let mut push = |flag: &str, v: String| a.extend([flag.to_string(), v]);
    if let Some(v) = inv.wal_fsync_ms {
        push("--wal-fsync-ms", v.to_string());
    }
    if let Some(v) = inv.lag_soft_ms {
        push("--lag-soft-ms", v.to_string());
    }
    if let Some(v) = inv.lag_hard_ms {
        push("--lag-hard-ms", v.to_string());
    }
    if let Some(v) = inv.min_replicas {
        push("--min-replicas-to-write", v.to_string());
    }
    // The widowed grace, ON BY DEFAULT for a seat that has a peer.
    //
    // This is the one tunable flintctl turns on rather than merely passing
    // through, because leaving it off has no safe reading. With no live
    // replica the lag cap cannot fire — `lag_ms` is None, the write path
    // falls through, and the master accepts writes nothing is copying, with
    // no bound at all. Measured before this existed: a default pair whose
    // replica was frozen took 539 writes in ~4s with zero replicas. Every
    // shipped cluster was in that state, because the only guard against it
    // (min-replicas-to-write) defaults to 0 and nothing set it.
    //
    // 10s matches the published RPO envelope on purpose: the doc promises at
    // most ten seconds' worth of acked writes at risk, and this is what makes
    // that true in the widowed case instead of aspirational. A pair that
    // wants a different trade sets `widowed-grace-ms` explicitly; 0 turns it
    // off and restores the old unbounded behaviour.
    //
    // Not applied to a single-member pair: with no peer ever, the grace would
    // shed every write once it expired, which is a standalone node being
    // punished for a redundancy it was never configured to have.
    match (inv.widowed_grace_ms, replicated) {
        (Some(v), _) => push("--widowed-grace-ms", v.to_string()),
        (None, true) => push("--widowed-grace-ms", DEFAULT_WIDOWED_GRACE_MS.to_string()),
        (None, false) => {}
    }
    // ADR-0018: the write lease is the NODE's to renew (against the CP via
    // its --journal target), not the controller's to push. Same inventory
    // key as always; it now lands here. Only replicated pairs are leased —
    // a standalone node has no successor to be fenced against — and 0
    // disables, matching the server's own default.
    if replicated {
        push(
            "--lease-ttl-ms",
            inv.ctl_lease_ttl_ms
                .unwrap_or(DEFAULT_LEASE_TTL_MS)
                .to_string(),
        );
        // Every CP seat, not just the journal target: a renewer pinned to
        // one seat fences the fleet when exactly that seat dies (the
        // -LEADER redirect can only be followed from a seat that answers).
        push("--lease-cp", inv.cp.join(","));
    }
    if let Some(v) = inv.max_conns {
        push("--max-conns", v.to_string());
    }
    if let Some(v) = inv.async_queue_cap {
        push("--async-queue-cap", v.to_string());
    }
    a
}

/// Proxy operator tunables: near-cache defaults + admission.
fn proxy_tuning_args(inv: &Inventory) -> Vec<String> {
    let mut a = Vec::new();
    let mut push = |flag: &str, v: String| a.extend([flag.to_string(), v]);
    if let Some(v) = inv.cache_ttl_ms {
        push("--cache-ttl-ms", v.to_string());
    }
    if let Some(v) = inv.cache_max_bytes {
        push("--cache-max-bytes", v.to_string());
    }
    if let Some(v) = inv.max_conns {
        push("--max-conns", v.to_string());
    }
    if let Some(v) = inv.fanout_timeout_ms {
        push("--fanout-timeout-ms", v.to_string());
    }
    a
}

/// Wait until the controller journals `Supervised` (auto-failover armed)
/// for every pair, with events newer than `since_ms`. "Operation complete"
/// from flintctl MEANS the fleet is supervised again — a freshly rerolled
/// controller refuses to auto-promote a pair it has not yet observed
/// converged (the degraded-window gate), so returning earlier would leave a
/// window where a master kill goes unanswered.
fn wait_supervised(inv: &Inventory, n_pairs: usize, since_ms: u64) {
    let tls = tls_client(inv);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut armed = std::collections::HashSet::new();
        if let Ok(Value::Bulk(Some(raw))) = call_cp(inv, &tls, &["CPJOURNALREAD", "500"]) {
            for line in String::from_utf8_lossy(&raw).lines() {
                if line.contains("\"kind\":\"Supervised\"")
                    && let Some(at) = line
                        .split("\"at_ms\":")
                        .nth(1)
                        .and_then(|r| r.split(',').next())
                        .and_then(|v| v.trim().parse::<u64>().ok())
                    && at >= since_ms
                    && let Some(subj) = line
                        .split("\"subject\":\"")
                        .nth(1)
                        .and_then(|r| r.split('"').next())
                {
                    armed.insert(subj.to_string());
                }
            }
        }
        if (0..n_pairs).all(|i| armed.contains(&format!("g{i}"))) {
            eprintln!("  supervision armed for {n_pairs} pair(s)");
            return;
        }
        if Instant::now() > deadline {
            eprintln!(
                "  WARNING: supervision not confirmed within 30s (journal may be unavailable); auto-failover may lag"
            );
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------- commands ----------

fn mint_certs(inv: &Inventory) {
    let d = format!("{}/certs", inv.statedir);
    std::fs::create_dir_all(&d).expect("certs dir");
    let sh = |cmd: &str| {
        let ok = Command::new("sh")
            .args(["-c", cmd])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(ok, "cert step failed: {cmd}");
    };
    sh(&format!(
        "openssl req -x509 -newkey rsa:2048 -nodes -keyout {d}/ca.key -out {d}/ca.crt -days 365 \
         -subj /CN=flint-internal-ca -addext basicConstraints=critical,CA:TRUE 2>/dev/null"
    ));
    resign_leaves(&d, &sh, &inv.edge_sans);
    eprintln!("  minted internal CA + component cert + edge cert");
}

/// Copy the minted certs to every OTHER machine in the fleet.
///
/// This is a file copy rather than a PKI exercise because the mesh does not
/// use per-host identities: every component presents the same `int` leaf, and
/// internal dials verify a FIXED name (`flint_tls::INTERNAL_SNI`) instead of
/// the address they connected to. So a node on another continent needs the
/// same three files and nothing else.
///
/// The edge cert is different — clients DO verify the address they dialed —
/// but its SANs come from the inventory's `edge-san` lines, so it too is
/// correct everywhere once minted. It only goes to proxy hosts, which are the
/// only seats that serve it.
fn push_certs(inv: &Inventory) {
    let d = format!("{}/certs", inv.statedir);
    let proxy_hosts: Vec<Runner> = (0..inv.proxies.len())
        .map(|i| proxy_runner(inv, i))
        .collect();
    for r in all_runners(inv) {
        if !r.is_remote() {
            continue;
        }
        for (f, mode) in [("ca.crt", "644"), ("int.crt", "644"), ("int.key", "600")] {
            if let Err(e) = r.send_file(&format!("{d}/{f}"), &format!("{d}/{f}"), mode) {
                die(&format!("distributing {f} to {}: {e}", r.label()));
            }
        }
        if inv.client_tls && proxy_hosts.contains(&r) {
            for (f, mode) in [("edge.crt", "644"), ("edge.key", "600")] {
                if let Err(e) = r.send_file(&format!("{d}/{f}"), &format!("{d}/{f}"), mode) {
                    die(&format!("distributing {f} to {}: {e}", r.label()));
                }
            }
        }
        eprintln!("  certs -> {}", r.label());
    }
}

/// `flintctl push-bins <tarball>`: stage a release bundle into every host's
/// bins dir.
///
/// `upgrade` rolls whatever is staged; it does not fetch. On a single host
/// that staging is a manual `tar x`, and this is the same act for a fleet.
///
/// Note the ordering hazard it inherits: the orchestrator's OWN flintctl is
/// among the binaries replaced, so a release that fixes a bug in the roll path
/// cannot roll itself out with the broken one. Unpack first, then upgrade.
fn push_bins(inv: &Inventory, tarball: &str) {
    assert!(
        std::path::Path::new(tarball).exists(),
        "no such bundle: {tarball}"
    );
    for r in all_runners(inv) {
        let staged = format!(
            "/tmp/{}",
            std::path::Path::new(tarball)
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .unwrap_or_else(|| "flint-bundle.tar.gz".into())
        );
        if r.is_remote()
            && let Err(e) = r.send_file(tarball, &staged, "644")
        {
            die(&format!("staging bundle on {}: {e}", r.label()));
        }
        let src = if r.is_remote() {
            staged
        } else {
            tarball.to_string()
        };
        let argv = vec![
            "tar".to_string(),
            "xzf".to_string(),
            src,
            "-C".to_string(),
            inv.bins.clone(),
        ];
        match r.output(&argv) {
            Ok(out) if out.status.success() => eprintln!("  bins -> {}", r.label()),
            Ok(out) => die(&format!(
                "unpacking on {}: {}",
                r.label(),
                String::from_utf8_lossy(&out.stderr).trim()
            )),
            Err(e) => die(&format!("unpacking on {}: {e}", r.label())),
        }
    }
}

/// (Re-)sign the LEAF certs — the mesh cert (int) and the edge cert — from
/// the existing CA, fresh keypairs each time. Used at bootstrap and by
/// `rotate-certs` (ADR-0006 D4): overwriting int.crt/int.key in place makes
/// each component's hot-reload watcher pick up the new leaf, no restart. The
/// CA is untouched, so old and new leaves both verify during the roll.
fn resign_leaves(d: &str, sh: &dyn Fn(&str), edge_sans: &[String]) {
    sh(&format!(
        "openssl req -newkey rsa:2048 -nodes -keyout {d}/int.key -out {d}/int.csr \
         -subj /CN=flint-internal 2>/dev/null"
    ));
    // -CAserial is EXPLICIT, not left to -CAcreateserial's default. The default
    // is meant to be the CA path with .crt swapped for .srl, but LibreSSL (what
    // /usr/bin/openssl is on macOS) drops the directory and writes `.srl` into
    // the CURRENT DIRECTORY instead. Every bootstrap — every drill, every
    // quickstart — silently littered the repo root with a dotfile, and on a
    // real deployment it lands in whatever directory the operator happened to
    // be standing in. Naming the path removes the implementation difference.
    sh(&format!(
        "printf 'subjectAltName=DNS:flint-internal\\nextendedKeyUsage=serverAuth,clientAuth\\nbasicConstraints=CA:FALSE' > {d}/ext.cnf && \
         openssl x509 -req -in {d}/int.csr -CA {d}/ca.crt -CAkey {d}/ca.key \
         -CAcreateserial -CAserial {d}/ca.srl \
         -out {d}/int.crt -days 365 -extfile {d}/ext.cnf 2>/dev/null"
    ));
    sh(&format!(
        "openssl req -newkey rsa:2048 -nodes -keyout {d}/edge.key -out {d}/edge.csr \
         -subj /CN=flint-edge 2>/dev/null"
    ));
    sh(&format!(
        "printf 'subjectAltName={sans}\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE' > {d}/edge-ext.cnf && \
         openssl x509 -req -in {d}/edge.csr -CA {d}/ca.crt -CAkey {d}/ca.key \
         -CAcreateserial -CAserial {d}/ca.srl \
         -out {d}/edge.crt -days 365 -extfile {d}/edge-ext.cnf 2>/dev/null",
        sans = edge_san_list(edge_sans),
    ));
    // The co-processor leaf (ADR-0010 step 1). SAN flint-internal like the mesh
    // leaf — the proxy verifies it on an internal dial at INTERNAL_SNI — but
    // `serverAuth` ONLY, no `clientAuth`. That absence IS the isolation
    // guarantee: a node's mutual-TLS verifier refuses a serverAuth-only leaf as
    // a client certificate, so a co-processor holding this leaf cannot dial the
    // mesh as a member. Nothing consumes it yet (the PROXYCHAN arm is step 2);
    // it is minted now because "a certificate and a test" is the whole of step
    // 1, and the mint is where the security property is either created or lost.
    sh(&format!(
        "openssl req -newkey rsa:2048 -nodes -keyout {d}/coproc.key -out {d}/coproc.csr \
         -subj /CN=flint-coproc 2>/dev/null"
    ));
    sh(&format!(
        "printf 'subjectAltName=DNS:flint-internal\\nextendedKeyUsage=serverAuth\\nbasicConstraints=CA:FALSE' > {d}/coproc-ext.cnf && \
         openssl x509 -req -in {d}/coproc.csr -CA {d}/ca.crt -CAkey {d}/ca.key \
         -CAcreateserial -CAserial {d}/ca.srl \
         -out {d}/coproc.crt -days 365 -extfile {d}/coproc-ext.cnf 2>/dev/null"
    ));

    // Assert the EKU the mint PRODUCED, not the recipe it was asked to run
    // (ADR-0010 D2: "the mint recipe becomes security-critical"). A co-processor
    // leaf accidentally minted by copying the mesh line above would serve
    // correctly and every server-side handshake would still pass, silently
    // keeping the clientAuth that must never be here. Fail the mint instead —
    // on bootstrap AND on every rotate-certs, since both route through here.
    let coproc = flint_tls::cert_eku(&format!("{d}/coproc.crt"))
        .expect("co-processor leaf unreadable/unparseable immediately after minting it");
    assert!(
        coproc.server_auth && !coproc.client_auth,
        "co-processor leaf minted with the WRONG EKU (server_auth={}, client_auth={}); \
         it must be serverAuth-ONLY. A clientAuth bit here is exactly the hole \
         ADR-0010 D2 exists to close — it would let a co-processor dial the mesh as a \
         member. Fix the coproc-ext.cnf line above; do not relax this assert.",
        coproc.server_auth,
        coproc.client_auth
    );
    // The mirror: the mesh leaf must KEEP clientAuth, or every internal dial
    // stops working. Same helper, opposite verdict — the pair is the point.
    let mesh = flint_tls::cert_eku(&format!("{d}/int.crt"))
        .expect("mesh leaf unreadable/unparseable immediately after minting it");
    assert!(
        mesh.server_auth && mesh.client_auth,
        "mesh leaf must be serverAuth,clientAuth (got server_auth={}, client_auth={}); \
         without clientAuth every internal dial stops working.",
        mesh.server_auth,
        mesh.client_auth
    );
}

/// The edge cert's SAN list: loopback always, plus every `edge-san` entry —
/// IPs as IP: entries, everything else as DNS:.
fn edge_san_list(extra: &[String]) -> String {
    let mut sans = vec!["IP:127.0.0.1".to_string(), "DNS:localhost".to_string()];
    for e in extra {
        let e = e.trim();
        if e.is_empty() {
            continue;
        }
        if e.parse::<std::net::IpAddr>().is_ok() {
            sans.push(format!("IP:{e}"));
        } else {
            sans.push(format!("DNS:{e}"));
        }
    }
    sans.join(",")
}

/// `flintctl rotate-certs`: re-sign the leaf certs from the CA in place; the
/// running components' TLS watchers reload within a poll. No restart, no CA
/// change (CA rotation stays a runbook).
fn rotate_certs(inv: &Inventory) {
    let d = format!("{}/certs", inv.statedir);
    assert!(
        std::path::Path::new(&format!("{d}/ca.crt")).exists(),
        "no CA at {d}/ca.crt — is this a `tls on` cluster?"
    );
    let sh = |cmd: &str| {
        let ok = Command::new("sh")
            .args(["-c", cmd])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(ok, "cert step failed: {cmd}");
    };
    resign_leaves(&d, &sh, &inv.edge_sans);
    println!("re-signed mesh + edge leaf certs from the CA; components hot-reload within ~2s");
}

/// Proxy liveness: does it ANSWER, through its own front door.
///
/// This used to fall back to a bare `TcpStream::connect` on a client-TLS
/// fleet, under the reasoning that "flintctl is not an edge client — real
/// encrypted traffic is verified by the drills". That stopped being true
/// when `verify --probe` learned to speak the edge (#102), and the fallback
/// was measuring the wrong thing in the meantime: a TCP accept proves
/// something holds the port, not that a proxy serves. An edge whose cert had
/// expired, whose TLS handshake failed, or which was wedged short of RESP
/// read as UP.
///
/// That mattered because FIVE of the six callers ask "is it serving":
/// `verify`'s `proxy up` assertion, both `status` renderings, `roll_edge`'s
/// "did not serve after the binary swap", and `start`'s wait-for-ready. On a
/// TLS-edge fleet every one of them was answering the weaker question, so
/// `roll_edge` could accept a proxy that had never served.
///
/// The sixth caller — `start` deciding whether to spawn — is the reason to
/// be careful rather than the reason not to fix it. It is left on this same
/// predicate deliberately: if a proxy holds the port but will not answer,
/// `start` SHOULD notice and say so, which is what it already did on every
/// plaintext fleet. Making the two fleet kinds agree is the point.
///
/// PROXYSTATS answers a Bulk when ungated; once the CP pushes an admin
/// digest (ADR-0006 D4) it answers -NOAUTH pre-auth. EITHER proves the proxy
/// is up and serving — only a failure to get a reply means down.
fn proxy_up(inv: &Inventory, i: usize) -> bool {
    // Resolved here rather than by the caller: passing an address in is what
    // let the BIND address reach the liveness check in the first place.
    let proxy = &proxy_dial(inv, i);
    let tls = edge_tls_client(inv);
    match call_seq_on(
        proxy,
        &tls,
        &[&["PROXYSTATS"]],
        Duration::from_millis(1500),
        inv.client_tls,
    ) {
        Ok(Value::Bulk(_)) => true,
        // -NOAUTH PROVES THE PROXY IS SERVING: only a live proxy that has
        // received the CP's admin digest can refuse this way (ADR-0006 D4).
        //
        // It arrives as Err, not Ok(Value::Error). `call_seq_on` turns any
        // RESP error into an io::Error, so the `Ok(Value::Error(..))` arm
        // this replaces was DEAD CODE from the moment proxy_up moved off
        // `call` — which returned the error as a value. Every fleet with an
        // admin token then read its own proxy as down: bootstrap failed,
        // `status` said DOWN, and `roll_edge` aborted the upgrade. The
        // playground was untouched only because its inventory sets no
        // admin-token, so its proxy answers with a Bulk.
        //
        // Matched on the message because that is the only place the error
        // survives; `is_noauth_error` exists so the shape is testable
        // without a live proxy.
        Err(e) if is_noauth_error(&e.to_string()) => true,
        _ => false,
    }
}

/// Does this transport error carry a RESP `-NOAUTH` reply?
///
/// Separated from [`proxy_up`] purely so it can be tested: the branch it
/// guards was silently unreachable for two releases, and a predicate with
/// no test is how that happened twice.
fn is_noauth_error(msg: &str) -> bool {
    msg.trim_start_matches('-')
        .trim_start()
        .starts_with("NOAUTH")
}

fn start_pair_nodes(inv: &Inventory, pair: &[String], gi: usize) {
    let d = &inv.statedir;
    let cp = &inv.cp[0];
    let tls = tls_client(inv);
    // The fleet's OWN view of who is master outranks inventory order. After a
    // failover the promoted node is the file's pair[1]; starting the dead
    // pair[0] bare would boot an ex-master claiming the lineage — the
    // controller fences it (no split-brain), but the pair then sits degraded
    // forever: a fenced replica that never resyncs, writes THROTTLED below
    // min-replicas. So ask the live members first, and only fall back to
    // inventory order when nobody is up (a cold start of the whole pair).
    let roles: Vec<Option<String>> = pair.iter().map(|a| info_field(a, &tls, "role:")).collect();
    let live_master = pair
        .iter()
        .zip(&roles)
        .find(|(_, r)| r.as_deref() == Some("master"))
        .map(|(a, _)| a.clone());
    for (i, addr) in pair.iter().enumerate() {
        let port = port_of(addr);
        // A live seat keeps its process; respawning it would lose the port
        // race AND overwrite its pidfile with the corpse's pid, so every
        // later stop/kill through that pidfile would aim at nothing.
        if let Some(role) = &roles[i] {
            eprintln!("  node-{port} already up ({role})");
            continue;
        }
        // SERVING is proved by the dial above. NOT serving is not proved by
        // a failed dial — a seat doing wipe + full sync has a live process
        // and an unbound port for as long as the sync takes, and reads
        // exactly like a dead one.
        //
        // Treating that as absent is destructive here rather than merely
        // wasteful: the branch below WIPES the data dir before respawning,
        // so a `start` issued while a node is syncing deletes the sync in
        // progress. Run on an interval it never converges. A supervise timer
        // did exactly that to the playground's replica — four restarts in
        // four minutes, never serving, healthy again the moment one `start`
        // ran alone (docs/bugs/0004-start-replaces-a-starting-seat.md).
        //
        // So: a seat with a process is this seat's, and `start` leaves it
        // be. If it is genuinely wedged rather than starting, that is what
        // `stop` then `start` is for — an operator decision, not one made
        // once a minute by a timer that cannot tell the difference.
        if seat_alive(
            &runner_for(inv, addr),
            "flint-server",
            &format!("{d}/node-{port}"),
        ) {
            eprintln!("  node-{port} STARTING (process up, not serving yet) — left alone");
            continue;
        }
        let replica_of = match &live_master {
            // Dead seat rejoining a live lineage: same contract as roll_node —
            // its replication cursor may be an ex-master's and its suffix may
            // have diverged, so it must not tail from its data as-is. The
            // MARKER (not a wipe, #187) leaves the decision to the server:
            // rewind to a local snapshot the master vouches for (cheap, and
            // what keeps the failover RTO independent of dataset size), or
            // discard and full-sync when no safe snapshot exists — the exact
            // behaviour the wipe used to force every time.
            Some(m) => {
                if let Err(e) = mark_reseed_node(
                    inv,
                    &runner_for(inv, addr),
                    port,
                    &format!("superseded copy rejoining the lineage held by {m}"),
                ) {
                    die(&format!(
                        "marking node-{port} for re-seed before rejoin: {e}"
                    ));
                }
                Some(m.clone())
            }
            None if i > 0 => Some(pair[0].clone()),
            None => None,
        };
        let mut args = vec![
            "--port".to_string(),
            port.to_string(),
            // Reachable from the other host of the pair, not just this one.
            "--bind".into(),
            host_of(addr).to_string(),
            "--engine".into(),
            "rocks".into(),
            "--data-dir".into(),
            format!("{d}/node-{port}"),
            "--journal".into(),
            cp.clone(),
        ];
        if let Some(m) = replica_of {
            args.extend(["--replica-of".into(), m]);
        }
        // Where THIS pair's masters snapshot on this host (the controller's
        // FLINTSNAPSHOT cadence writes {snapshot-root}/g{i} locally on
        // whichever member is master). A marked seat rewinds from here.
        args.extend(["--rewind-snaps".into(), format!("{d}/snaps/g{gi}")]);
        args.extend(internal_args(inv));
        args.extend(node_tuning_args(inv, pair.len() > 1));
        spawn(
            inv,
            &runner_for(inv, addr),
            &format!("node-{port}"),
            "flint-server",
            &args,
        );
        // The master must be up before its replicas dial in.
        if i == 0 && live_master.is_none() {
            std::thread::sleep(Duration::from_millis(700));
        }
    }
    reconcile_cold_start(inv, pair, &tls, live_master.is_none());
}

/// After a COLD start, check that inventory order was actually right.
///
/// The loop above falls back to position when nothing is up: pair[0] is
/// started bare and the rest get `--replica-of pair[0]`. If the pair last
/// failed over, the durable roles are the reverse of that — and a node's
/// manifest outranks the flag, correctly, because a flag must never be able
/// to demote a master holding the newest data. The two safety rules compose
/// into a fleet that replicates nothing:
///
///   pair[0], durable replica, started bare  -> replica of NOBODY
///   pair[1], durable master, given the flag -> "ignoring --replica-of"
///
/// and `status` looks structurally fine — both up, roles coherent, epochs
/// agreed. Only `live_replicas 0` gives it away. Seen on the playground:
/// a `stop`/`start` for an unrelated address change left the pair with no
/// replication and no error, on a fleet that had failed over weeks earlier.
///
/// So once the seats are serving, ask them who is master and repair if the
/// answer is not pair[0]. Costs one extra probe on the common path, where
/// the assumption holds and nothing is rolled.
fn reconcile_cold_start(
    inv: &Inventory,
    pair: &[String],
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    was_cold: bool,
) {
    if !was_cold || pair.len() < 2 {
        return;
    }
    // A member that boots into a full sync is alive and not yet serving, so
    // give the probe the same budget the rest of `start` gives a seat.
    let mut roles: Vec<Option<String>> = Vec::new();
    for _ in 0..40 {
        roles = pair.iter().map(|a| info_field(a, tls, "role:")).collect();
        if roles.iter().all(Option::is_some) {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    let Some(master) = pair
        .iter()
        .zip(&roles)
        .find(|(_, r)| r.as_deref() == Some("master"))
        .map(|(a, _)| a.clone())
    else {
        // No master anywhere is a different failure — the controller's to
        // resolve, and re-seeding blind against no lineage would be worse
        // than leaving it visible.
        eprintln!("  pair has no reachable master after cold start — left for the controller");
        return;
    };
    if master == pair[0] {
        return;
    }
    eprintln!(
        "  durable roles disagree with inventory order: {master} is master, not {}",
        pair[0]
    );
    for addr in pair.iter().filter(|a| **a != master) {
        eprintln!("  re-seeding {addr} as a replica of {master}");
        if let Err(e) = roll_node(inv, addr, &master, &[], &None, true) {
            die(&format!("re-seeding {addr} onto {master}: {e}"));
        }
    }
}

/// Argument builders, one per fleet role.
///
/// These exist so `start` and `upgrade` cannot construct a process
/// differently: an upgrade that respawns the proxy with a subtly different
/// argv is a config change disguised as a version bump, and it would show up
/// as behaviour nobody chose. One builder, two callers.
/// One CP call that FOLLOWS THE LEADER. A single-seat CP never redirects and
/// this is exactly `call`. A Raft follower answers `-LEADER <addr>`; during
/// an election every seat answers "no leader elected yet". Mutating through
/// `call(&inv.cp[0], ...)` therefore breaks the moment seat 1 is not the
/// leader — which after the FIRST leader failover is the steady state, and
/// the failure would read as "the CP rejected the command" rather than as
/// mis-routing. Every flintctl CP call goes through here so that cannot
/// happen; the hop/retry budget is small because elections settle in
/// hundreds of milliseconds.
fn call_cp(
    inv: &Inventory,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    args: &[&str],
) -> std::io::Result<Value> {
    let mut target = inv.cp[0].clone();
    let mut last: std::io::Result<Value> = Err(std::io::Error::other("unreached"));
    for attempt in 0..24 {
        // Patience on EVERY retry path, not only "no leader": right after a
        // leader dies, survivors keep advertising the DEAD seat until the
        // election converges, so a redirect can point at a corpse. The first
        // version slept only on "no leader elected" and burned its whole
        // attempt budget ping-ponging stale redirects inside the election
        // window — found by ctl_cpha_drill killing the leader.
        if attempt > 0 {
            std::thread::sleep(Duration::from_millis(350));
        }
        last = call(&target, tls, args);
        match &last {
            Ok(Value::Error(e)) if e.starts_with("LEADER ") => {
                target = e["LEADER ".len()..].trim().to_string();
            }
            Ok(Value::Error(e)) if e.contains("no leader elected") => {}
            // A dead seat: rotate to the next one rather than giving up —
            // with a single seat this falls through and returns the error.
            Err(_) if inv.cp.len() > 1 => {
                let pos = inv.cp.iter().position(|a| *a == target).unwrap_or(0);
                target = inv.cp[(pos + 1) % inv.cp.len()].clone();
            }
            _ => return last,
        }
    }
    last
}

/// The seat name for CP `i` — `cp` alone, `cp-n1..N` for a Raft group.
///
/// ONE definition, because this is a pidfile name and two spellings of it
/// means one caller stopping a seat the other never started. `roll_edge`
/// used the literal "cp" against a three-seat fleet: `stop_seat` looked for
/// `cp.pid`, found nothing, reported the process already gone, and then
/// failed `wait_port_free` because the real `cp-n1` was alive and holding
/// the port. The abort read "port still bound after the process was gone",
/// which is exactly what it looks like when you kill the wrong thing.
fn cp_seat_name(inv: &Inventory, i: usize) -> String {
    if inv.cp.len() == 1 {
        "cp".to_string()
    } else {
        format!("cp-n{}", i + 1)
    }
}

/// The state dir for CP seat `i`, matching `cp_seat_args`.
fn cp_seat_state(inv: &Inventory, i: usize) -> String {
    let d = &inv.statedir;
    if inv.cp.len() == 1 {
        format!("{d}/cp-state")
    } else {
        format!("{d}/cp-state-n{}", i + 1)
    }
}

/// Arguments for CP seat `i`. One `cp` line in the inventory is the
/// single-node CP, unchanged. THREE lines are a Raft group: node ids are
/// 1-based inventory order, the raft port is the client port + 10 (the
/// controlplane_ha drill's convention, now a flintctl contract — open it in
/// the security group), and each seat gets its own state dir so co-located
/// drill seats cannot share one.
fn cp_seat_args(inv: &Inventory, i: usize) -> Vec<String> {
    let d = &inv.statedir;
    let seat = &inv.cp[i];
    let mut args = vec![
        "--port".to_string(),
        port_of(seat).to_string(),
        // Bind the address the inventory tells everyone else to dial. On a
        // loopback fleet this is 127.0.0.1 and nothing changes; on a fleet
        // with real addresses it is the difference between a control plane
        // and an unreachable process.
        "--bind".into(),
        host_of(seat).to_string(),
        "--state".into(),
        if inv.cp.len() == 1 {
            format!("{d}/cp-state")
        } else {
            format!("{d}/cp-state-n{}", i + 1)
        },
    ];
    if inv.cp.len() > 1 {
        let peers = inv
            .cp
            .iter()
            .enumerate()
            .map(|(j, a)| format!("{}={}:{}", j + 1, host_of(a), port_of(a) + 10))
            .collect::<Vec<_>>()
            .join(",");
        let clients = inv
            .cp
            .iter()
            .enumerate()
            .map(|(j, a)| format!("{}={a}", j + 1))
            .collect::<Vec<_>>()
            .join(",");
        args.extend([
            "--raft".into(),
            "--node-id".into(),
            (i + 1).to_string(),
            "--raft-port".into(),
            (port_of(seat) + 10).to_string(),
            "--peers".into(),
            peers,
            "--client-addrs".into(),
            clients,
        ]);
    }
    args.extend(internal_args(inv));
    if let Some(tok) = &inv.admin_token {
        // ADR-0006 D4: the admin token lives in the CP and is pushed to
        // proxies as a digest; rotate it later with `flintctl rotate-admin`.
        args.extend(["--admin-token".to_string(), tok.clone()]);
    }
    args
}

fn proxy_args(inv: &Inventory, i: usize) -> Vec<String> {
    let proxy = &inv.proxies[i];
    // The inventory addr's HOST is the bind address (0.0.0.0 serves external
    // clients — the marketplace shape; 127.0.0.1 stays the loopback default).
    //
    // The ADVERTISE address is the proxy's IDENTITY, and identity has exactly
    // one definition — proxy_dial. Three places used to compute it separately:
    // here, bootstrap's CPADDPROXY, and verify's declared list. Two of them
    // said "advertise else BIND", which on a fleet where the proxy has its own
    // machine means the useless `0.0.0.0:7379`, and the third said proxy_dial.
    // A 7-host bootstrap then came up completely healthy and failed its own
    // verify: `["0.0.0.0:7379"] registered but not in the inventory` alongside
    // `["172.31.64.235:7379"] declared but never registered` — the same seat,
    // under two names. #103 was this failure in another costume.
    let bind_host = proxy
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or("127.0.0.1");
    let advertise = proxy_dial(inv, i);
    let mut args = vec![
        "--port".to_string(),
        port_of(proxy).to_string(),
        "--bind".to_string(),
        bind_host.to_string(),
        "--control-plane".into(),
        // ALL seats, comma-joined: the proxy rotates to the next on watch
        // failure, so no single CP host's loss can stop snapshot delivery.
        // Each proxy STARTS at a different seat (rotation of the list) so
        // steady-state watch load also spreads.
        {
            let n = inv.cp.len();
            (0..n)
                .map(|k| inv.cp[(i + k) % n].clone())
                .collect::<Vec<_>>()
                .join(",")
        },
        "--advertise".into(),
        advertise,
    ];
    args.extend(internal_args(inv));
    args.extend(proxy_tuning_args(inv));
    if let Some(families) = families_arg(inv) {
        args.extend(["--families".to_string(), families]);
        // The CALLBACK half of ADR-0010 D6. A family command's durable write
        // goes co-processor -> PROXYCHAN -> this proxy's EDGE, so the
        // co-processor has to be handed a reachable edge address; the proxy
        // refuses to mint a channel without one and every VEC.* answers
        // -COPROCUNAVAIL. It cannot be derived from the accept socket (the
        // inbound peer is not an edge), which is why it is a flag — and
        // `advertise` is already exactly this address: the proxy's identity
        // IS its edge.
        args.extend(["--edge-advertise".to_string(), proxy_dial(inv, i)]);
    }
    if inv.client_tls {
        // ADR-0006 D2: the packaged default is an encrypted front door.
        args.extend([
            "--tls-cert".to_string(),
            format!("{}/certs/edge.crt", inv.statedir),
            "--tls-key".to_string(),
            format!("{}/certs/edge.key", inv.statedir),
        ]);
    }
    args
}

fn agent_args(inv: &Inventory) -> Option<Vec<String>> {
    let agent = inv.agent.as_ref()?;
    let d = &inv.statedir;
    let mut args = vec![
        "--control-plane".to_string(),
        inv.cp[0].clone(),
        "--metrics-port".into(),
        port_of(agent).to_string(),
        // The host half of `agent <addr>` used to be parsed and dropped, so
        // an inventory naming a routable address still got a loopback
        // listener and said nothing about it. Honour the whole address: an
        // off-box scraper cannot reach 127.0.0.1, and the operator's fix
        // was to edit the line that already existed.
        "--metrics-bind".into(),
        host_of(agent).to_string(),
        "--journal".into(),
        format!("{d}/shadow.jsonl"),
    ];
    if let Some(cap) = inv.capacity_bytes {
        args.extend(["--node-capacity-bytes".into(), cap.to_string()]);
    }
    if let Some(billing) = &inv.billing {
        args.extend(["--billing".to_string(), billing.clone()]);
        if let Some(days) = inv.billing_retain_days {
            args.extend(["--billing-retain-days".into(), days.to_string()]);
        }
    }
    // Edge trust for the agent's own PROXY* dials — the agent is a SEPARATE
    // consumer of the edge and has to be told, which is the whole hazard
    // `edge_trust_path` exists to contain. (This comment had drifted onto
    // the billing block above it, so the one line documenting the hazard was
    // attached to unrelated code.)
    if inv.client_tls {
        args.extend(["--edge-ca".into(), edge_trust_path(inv)]);
    }
    args.extend(internal_args(inv));
    Some(args)
}

fn controller_args(inv: &Inventory) -> Vec<String> {
    let pairs_spec = inv
        .pairs
        .iter()
        .map(|p| p.join(","))
        .collect::<Vec<_>>()
        .join(";");
    let d = &inv.statedir;
    let mut args = vec![
        "--pairs".to_string(),
        pairs_spec,
        "--id".into(),
        "ctl".into(),
        // RTO timing knobs — inventory-tunable (config edit + restart).
        "--poll-ms".into(),
        inv.ctl_poll_ms.unwrap_or(100).to_string(),
        "--confirm".into(),
        inv.ctl_confirm.unwrap_or(3).to_string(),
        "--journal".into(),
        inv.cp[0].clone(),
        "--snapshot-root".into(),
        format!("{d}/snaps"),
        // Option B: cutovers commit ownership truth to the CP.
        "--commit-cp".into(),
        inv.cp[0].clone(),
    ];
    args.extend(internal_args(inv));
    args
}

/// The backup seat's argv (ADR-0011 D8), None unless `backup-to` is set.
///
/// The pair LIST goes through verbatim — the seat asks each pair which
/// member is master at run time, so a failover between backups needs no
/// reroll. A pair-set change does (see the expand/decommission sites),
/// exactly as the controller does.
fn backup_args(inv: &Inventory) -> Option<Vec<String>> {
    let to = inv.backup_to.as_ref()?;
    let d = &inv.statedir;
    let pairs_spec = inv
        .pairs
        .iter()
        .map(|p| p.join(","))
        .collect::<Vec<_>>()
        .join(";");
    let cp_state = if inv.cp.len() == 1 {
        format!("{d}/cp-state")
    } else {
        // Multi-seat CP: the file is Raft-replicated; seat 1's copy is as
        // legitimate as any, and the manifest records which one was taken.
        format!("{d}/cp-state-n1")
    };
    let mut args = vec![
        "schedule".to_string(),
        "--pairs".into(),
        pairs_spec,
        "--cp-state".into(),
        cp_state,
        "--to".into(),
        to.clone(),
        "--every".into(),
        inv.backup_every.clone().unwrap_or_else(|| "24h".into()),
        // Its OWN snapshot root, never the controller's: FLINTSNAPSHOT
        // repoints <root>/LATEST and spare-restore seeds from whatever
        // LATEST names.
        "--snap-root".into(),
        format!("{d}/backup-snaps"),
        "--status-file".into(),
        format!("{d}/logs/backup-status"),
    ];
    if let Some(v) = &inv.backup_verify_every {
        args.extend(["--verify-every".into(), v.clone()]);
    }
    if let Some(v) = &inv.backup_rehearse_every {
        args.extend(["--rehearse-every".into(), v.clone()]);
    }
    if let Some(k) = inv.backup_keep {
        args.extend(["--keep".into(), k.to_string()]);
    }
    if inv.tls {
        args.extend(["--tls".into(), format!("{d}/certs")]);
    }
    Some(args)
}

fn start_backup(inv: &Inventory) {
    if let Some(args) = backup_args(inv) {
        spawn(inv, &backup_runner(inv), "backup", "flint-backup", &args);
    }
}

fn start_controller(inv: &Inventory) {
    spawn(
        inv,
        &controller_runner(inv),
        "controller",
        "flint-controller",
        &controller_args(inv),
    );
}

/// First-time bring-up: mint certs, spawn, REGISTER the topology.
fn bootstrap(inv: &Inventory) {
    launch(inv, true);
}

/// Reboot path (idempotent boot, marketplace first-boot re-runs): spawn
/// every process against EXISTING state — no cert minting, no registry
/// writes (CPADDPAIR/CPADDPROXY append; re-running them would duplicate
/// topology). The CP state file, node data dirs, and certs carry the
/// fleet's memory.
fn start(inv: &Inventory) {
    launch(inv, false);
}

fn launch(inv: &Inventory, register: bool) {
    let d = &inv.statedir;
    for sub in ["logs", "pids", "snaps"] {
        std::fs::create_dir_all(format!("{d}/{sub}")).expect("statedir");
    }
    // Remote hosts need the same skeleton before anything writes a log or a
    // pidfile into it.
    for r in all_runners(inv) {
        if r.is_remote() {
            let dirs: Vec<String> = ["logs", "pids", "snaps"]
                .iter()
                .map(|sub| format!("{d}/{sub}"))
                .collect();
            let mut argv = vec!["mkdir".to_string(), "-p".to_string()];
            argv.extend(dirs);
            if let Err(e) = r.output(&argv) {
                die(&format!("preparing statedir on {}: {e}", r.label()));
            }
        }
    }
    eprintln!(
        "== {} into {d} (tls {})",
        if register { "bootstrap" } else { "start" },
        if inv.tls { "on" } else { "off" }
    );
    if register && inv.tls {
        mint_certs(inv);
        push_certs(inv);
    }
    let tls = tls_client(inv);

    // 1. Control plane — every seat (one for the single-node CP, three for
    // Raft), then the registry (proxies + pairs).
    // Every role below spawns ONLY if its seat is not already serving. On a
    // partially-live fleet (a start right after a failover, a boot unit
    // re-running) the respawned duplicate loses the port race and dies — but
    // not before overwriting the live seat's pidfile with its own pid, after
    // which every stop/kill through that pidfile aims at a corpse. The
    // controller is worse: it has no port to lose, so the duplicate LIVES,
    // and the pair gets two supervisors.
    for i in 0..inv.cp.len() {
        let seat = &inv.cp[i];
        // Shared with roll_edge. Two spellings of a pidfile name is how the
        // roll came to stop a seat this never started.
        let name = cp_seat_name(inv, i);
        if matches!(call(seat, &tls, &["PING"]), Ok(Value::Simple(s)) if s == "PONG") {
            eprintln!("  {name} already up");
            continue;
        }
        // Same rule as the pair nodes: a failed dial does not mean absent.
        // A Raft seat replaying its log answers nothing until it is ready,
        // and spawning beside it gives the duplicate a lost port race and a
        // clobbered pidfile — after which every stop aims at a corpse.
        if seat_alive(
            &runner_for(inv, seat),
            "flint-controlplane",
            &format!("{d}/cp-state"),
        ) {
            eprintln!("  {name} STARTING (process up, not answering yet) — left alone");
            continue;
        }
        spawn(
            inv,
            &runner_for(inv, seat),
            &name,
            "flint-controlplane",
            &cp_seat_args(inv, i),
        );
    }
    for seat in &inv.cp {
        assert!(
            wait_pong(seat, &tls, Duration::from_secs(10)),
            "control plane seat {seat} up"
        );
    }
    // A Raft group that answers PING has not necessarily ELECTED: prove a
    // leader exists before registering anything, or the registration calls
    // spend their retry budget on the election instead of on real failures.
    if inv.cp.len() > 1 {
        assert!(
            matches!(call_cp(inv, &tls, &["PING"]), Ok(Value::Simple(_))),
            "raft CP elected a leader"
        );
        eprintln!("  cp: {} raft seats up, leader elected", inv.cp.len());
    }
    if register {
        for i in 0..inv.proxies.len() {
            // The registry is what clients and portals dial, so it holds the
            // proxy's identity — same definition as the --advertise it was
            // started with and the address verify expects. One function.
            let adv = proxy_dial(inv, i);
            must("register proxy", call_cp(inv, &tls, &["CPADDPROXY", &adv]));
        }
        // Families go in the REGISTRY, not just on the proxy's command line.
        // The proxy takes its route table from CPSNAPSHOT element 7 whenever
        // the element is present, so a CP-fed proxy REPLACES whatever
        // `--families` gave it — a static flag alone survives exactly until
        // the first snapshot lands, which is why a co-processor spawned by
        // the inventory still answered "unknown command". Registering here
        // also means a proxy added later learns the family without a
        // restart, the same way it learns pairs.
        if let Some(families) = families_arg(inv) {
            for entry in families.split(';') {
                if let Some((prefix, addrs)) = entry.split_once('=') {
                    must(
                        "register family",
                        call_cp(inv, &tls, &["CPFAMILY", prefix, addrs]),
                    );
                }
            }
        }
        // Initial pairs carry the even slot split as EXPLICIT level-1
        // routing state; expansion pairs later join with "-" (no range) so
        // capacity never re-routes unmigrated slots.
        let n = inv.pairs.len();
        for (i, pair) in inv.pairs.iter().enumerate() {
            let start = i * 16384 / n;
            let end = (i + 1) * 16384 / n - 1;
            must(
                "register pair",
                call_cp(
                    inv,
                    &tls,
                    &["CPADDPAIR", &pair.join(","), &format!("{start}-{end}")],
                ),
            );
        }
        eprintln!(
            "  registry: {} proxies, {} pairs",
            inv.proxies.len(),
            inv.pairs.len()
        );
    }

    // 2. Data plane.
    for (gi, pair) in inv.pairs.iter().enumerate() {
        start_pair_nodes(inv, pair, gi);
    }
    for pair in &inv.pairs {
        assert!(
            wait_pong(&pair[0], &tls, Duration::from_secs(10)),
            "master {} up",
            pair[0]
        );
    }

    // 3a. Co-processors, BEFORE the proxies that route to them (ADR-0010).
    // Ordering is not cosmetic: a proxy that admits a family command before
    // the seat is listening answers the tenant with a connection error, and
    // the family path has no retry of its own.
    for i in 0..inv.coprocs.len() {
        let (family, addr) = &inv.coprocs[i];
        let bin = coproc_bin(family).expect("inventory parse rejected unknown families");
        let seat = coproc_seat(family, addr);
        if seat_alive(&coproc_runner(inv, i), bin, &seat) {
            eprintln!("  {seat} already up");
            continue;
        }
        spawn(
            inv,
            &coproc_runner(inv, i),
            &seat,
            bin,
            &coproc_args(inv, i),
        );
    }

    // 3. Routing plane.
    for (i, proxy) in inv.proxies.iter().enumerate() {
        // The inventory addr's HOST is the bind address (0.0.0.0 serves
        // external clients — the marketplace shape; 127.0.0.1 stays the
        // loopback default). The ADVERTISE address is what the proxy
        // reports of itself (subset identity): the proxy-advertise line
        // when declared, else the bind line — must match what bootstrap
        // registered.
        if proxy_up(inv, i) {
            eprintln!("  proxy-{} already up", port_of(proxy));
            continue;
        }
        // And again for the routing plane: PROXYSTATS goes unanswered while
        // the proxy is binding and pulling its first CP snapshot.
        if seat_alive(&proxy_runner(inv, i), "flint-proxy", &proxy_dial(inv, i)) {
            eprintln!(
                "  proxy-{} STARTING (process up, not serving yet) — left alone",
                port_of(proxy)
            );
            continue;
        }
        spawn(
            inv,
            &proxy_runner(inv, i),
            &format!("proxy-{}", port_of(proxy)),
            "flint-proxy",
            &proxy_args(inv, i),
        );
    }

    for (i, proxy) in inv.proxies.iter().enumerate() {
        // Liveness probe = PROXYSTATS (answered pre-auth); a CP-fed proxy
        // replies -NOAUTH to PING until a tenant authenticates. Plaintext:
        // the client port is not part of the internal mesh.
        let deadline = Instant::now() + Duration::from_secs(10);
        let dial = proxy_dial(inv, i);
        loop {
            if proxy_up(inv, i) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "{}",
                proxy_down_help(inv, proxy, &dial)
            );
            std::thread::sleep(Duration::from_millis(150));
        }
    }

    // 4. Supervision + observability.
    let ctl_since = now_ms();
    let mut ctl_started = false;
    if inv.controller {
        if seat_alive(
            &controller_runner(inv),
            "flint-controller",
            &format!("{d}/snaps"),
        ) {
            eprintln!("  controller already up");
        } else {
            start_controller(inv);
            ctl_started = true;
        }
    }
    // Optional fleet-agent add-on: the `agent <addr>` inventory key starts
    // a flint-agent binary if one sits in the bins dir. The agent (fleet
    // metering/insights/automation) is not part of this repository; without
    // the binary the key simply has nothing to start.
    if inv.agent.is_some() {
        if seat_alive(
            &agent_runner(inv),
            "flint-agent",
            &format!("{d}/shadow.jsonl"),
        ) {
            eprintln!("  agent already up");
        } else {
            spawn(
                inv,
                &agent_runner(inv),
                "agent",
                "flint-agent",
                &agent_args(inv).expect("agent args"),
            );
        }
    }

    if inv.backup_to.is_some() {
        if seat_alive(
            &backup_runner(inv),
            "flint-backup",
            &format!("{d}/backup-snaps"),
        ) {
            eprintln!("  backup seat already up");
        } else {
            start_backup(inv);
        }
    }

    // Only a controller started HERE emits fresh Supervised events; waiting
    // on one that was already up would time out into a spurious warning.
    if ctl_started {
        wait_supervised(inv, inv.pairs.len(), ctl_since);
    }
    eprintln!(
        "== {} complete",
        if register { "bootstrap" } else { "start" }
    );
    status(inv);
}

/// Reconcile the cluster's three views of itself and report disagreement.
///
/// A cluster can be wrong in ways no single component notices, because each
/// one is internally consistent. The control plane's registry, a node's own
/// manifest, and the proxy's routing table are three separate beliefs about
/// the same fleet, and the interesting failures are the disagreements —
/// a promotion the proxy never heard about, a pair with two masters, a
/// half-finished canary leaving mixed builds.
///
/// So the proxy's view is probed BEHAVIOURALLY rather than read out of it.
/// What matters is not what it thinks the topology is, but whether the
/// commands that depend on that belief actually work. `DBSIZE` and `SCAN`
/// touch every master; either failing means the proxy is holding a stale
/// map. That is exactly the shape of the bug this command was written for:
/// after a failover, keyed traffic recovered while fan-out stayed pointed
/// at the dead node, and every drill stayed green.
fn verify(inv: &Inventory, args: &[String]) {
    let probe = args
        .iter()
        .position(|a| a == "--probe")
        .and_then(|i| args.get(i + 1))
        .cloned();
    let problems = verify_checks(inv, probe.as_deref(), true);
    println!();
    if problems.is_empty() {
        println!(
            "VERIFY OK: {} pair(s), {} proxy(ies) — all views agree",
            inv.pairs.len(),
            inv.proxies.len()
        );
    } else {
        println!("VERIFY FAILED: {} problem(s)", problems.len());
        for p in &problems {
            println!("  - {p}");
        }
        std::process::exit(1);
    }
}

/// Run the reconciliation after an operation that changed the cluster, and
/// REFUSE to report success if the result does not hold together.
///
/// The whole point of the previous commit's findings is that a check nobody
/// runs is a check that does not exist: both bugs it was written for
/// survived because every drill was green and nothing looked. So expansion,
/// failover and slot moves end here, and a cluster that does not reconcile
/// makes the command that produced it fail.
fn verify_after(inv: &Inventory, op: &str) {
    let problems = verify_checks(inv, None, false);
    if problems.is_empty() {
        println!("verify: {op} left the cluster consistent");
        return;
    }
    eprintln!("verify: {op} left the cluster INCONSISTENT:");
    for p in &problems {
        eprintln!("  - {p}");
    }
    eprintln!(
        "flintctl: run `flintctl -f <inventory> verify` for detail; the operation itself may have\n         partially applied, so inspect before retrying."
    );
    std::process::exit(1);
}

/// The checks themselves. Returns the problems found; prints per-check
/// lines only when `loud`, so the post-step form stays quiet on success.
fn verify_checks(inv: &Inventory, probe: Option<&str>, loud: bool) -> Vec<String> {
    let tls = tls_client(inv);
    let mut problems: Vec<String> = Vec::new();
    let mut note = |ok: bool, label: &str, detail: String| {
        if loud {
            println!(
                "  {} {label}{}",
                if ok { "ok  " } else { "FAIL" },
                if detail.is_empty() {
                    String::new()
                } else {
                    format!("  {detail}")
                }
            );
        }
        if !ok {
            problems.push(format!("{label}: {detail}"));
        }
    };
    let head = |t: &str| {
        if loud {
            println!("{t}");
        }
    };

    head("== control plane");
    for seat in &inv.cp {
        let ok = matches!(call(seat, &tls, &["PING"]), Ok(Value::Simple(s)) if s == "PONG");
        note(ok, "reachable", seat.clone());
    }

    head("== pairs: one master each, coherent epochs, one build");
    let mut builds: BTreeSet<String> = BTreeSet::new();
    for (i, pair) in inv.pairs.iter().enumerate() {
        let mut masters = Vec::new();
        let mut down = Vec::new();
        for addr in pair {
            match info_field(addr, &tls, "role:") {
                Some(role) => {
                    if role == "master" {
                        masters.push(addr.clone());
                    }
                    if let Some(b) = info_field(addr, &tls, "build:") {
                        builds.insert(b);
                    }
                }
                None => down.push(addr.clone()),
            }
        }
        let label = format!("pair {i}");
        match masters.len() {
            1 => note(true, &label, format!("master {}", masters[0])),
            0 => note(false, &label, format!("NO master; down: {down:?}")),
            // The invariant epoch fencing exists to make impossible. If this
            // ever prints, two nodes are accepting writes for the same slots.
            _ => note(
                false,
                &label,
                format!("SPLIT BRAIN: {} masters {masters:?}", masters.len()),
            ),
        }
        // A declared member that is not there is a FAILURE, not a footnote.
        //
        // This line used to read `master X (1 down)` and count as ok, so a
        // pair running on ONE node reported `VERIFY OK — all views agree`.
        // The playground ran that way for five days: its replica hit a WAL
        // gap on 2026-08-01, correctly marked itself for re-seed and exited,
        // and nothing restarted it. The agent recommended AttachReplica 963
        // times into the shadow journal; the watch that a human actually
        // reads said OK the whole time, because a green summary line beats a
        // parenthetical every time.
        //
        // Single-copy is exactly the state this command exists to surface:
        // the inventory declares two members, reality has one, and until it
        // is fixed there is no failover target and one disk holds the only
        // copy. Operations that end in `verify_after` now refuse to report
        // success while a fleet is in it.
        note(
            down.is_empty(),
            &format!("pair {i} fully staffed"),
            if down.is_empty() {
                format!("{} member(s) up", pair.len())
            } else {
                format!("SINGLE-COPY: {down:?} down — no failover target, one copy on one disk")
            },
        );
        // Present is not the same as attached, and the check above only
        // knows about present. BUG-0008: both members of a failed-over pair
        // came up serving, with correct roles and agreed epochs, and
        // replicated nothing — so `fully staffed` said `2 member(s) up` and
        // was telling the truth about the wrong thing. The fleet was
        // single-copy with nobody down.
        //
        // Ask the master how many replicas are actually streaming. That is
        // the number the phrase "no failover target, one copy on one disk"
        // was always about.
        if pair.len() > 1 && masters.len() == 1 {
            let attached: usize = info_field(&masters[0], &tls, "live_replicas:")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let want = pair.len() - 1;
            note(
                attached >= want,
                &format!("pair {i} replicating"),
                if attached >= want {
                    format!("{attached} replica(s) streaming from {}", masters[0])
                } else {
                    format!(
                        "SINGLE-COPY: every member up, but {attached} of {want} streaming \
                         from {} — one disk holds the only copy",
                        masters[0]
                    )
                },
            );
        }
    }
    note(
        builds.len() <= 1,
        "single build across the fleet",
        format!("{builds:?}"),
    );

    // The inventory's declared `capacity` against the disk each node actually
    // has. Nodes already report disk_total_bytes (the headroom guard samples
    // statvfs), so this costs nothing and works on a multi-machine fleet,
    // where a single agent could not stat a remote node's filesystem.
    //
    // OVER-declaring is the failure that matters: the capacity model sizes
    // expansion off this number, so a fleet claiming more disk than it has
    // stays quiet through the pressure that should have triggered
    // ExpandCluster. The playground declared 1.6 TB on a 436 GB disk — an
    // i4i.2xlarge figure left behind on an i4i.large — and nothing noticed
    // for weeks, because a wrong constant looks exactly like a right one.
    //
    // Under-declaring is deliberate headroom, so it is reported, not failed.
    if let Some(declared) = inv.capacity_bytes {
        for pair in &inv.pairs {
            for addr in pair {
                let Some(total) = info_field(addr, &tls, "disk_total_bytes:")
                    .and_then(|v| v.parse::<u64>().ok())
                    .filter(|t| *t > 0)
                else {
                    continue; // node down, or an engine with no disk to report
                };
                let gib = |b: u64| b as f64 / (1024.0 * 1024.0 * 1024.0);
                if declared > total {
                    note(
                        false,
                        "declared capacity fits the disk",
                        format!(
                            "{addr}: inventory says {:.0} GiB, the filesystem is {:.0} GiB — \
                             capacity pressure will fire late or never",
                            gib(declared),
                            gib(total)
                        ),
                    );
                } else if declared * 10 < total * 9 {
                    note(
                        true,
                        "declared capacity fits the disk",
                        format!(
                            "{addr}: {:.0} of {:.0} GiB declared (headroom held back)",
                            gib(declared),
                            gib(total)
                        ),
                    );
                } else {
                    note(
                        true,
                        "declared capacity fits the disk",
                        format!("{addr}: {:.0} GiB", gib(total)),
                    );
                }
            }
        }
    }

    // Anti-affinity. Checked here rather than at bootstrap because every
    // topology-changing verb already ends in verify, so one implementation
    // gates bootstrap, expand, add-replica and swap-node alike.
    head("== failure domains");
    if inv.zones.is_empty() {
        if loud {
            println!(
                "  --   not declared. Add `zone <host> <name>` lines and a pair whose\n\
                 \x20      members share a domain becomes a FAILED verify instead of a\n\
                 \x20      surprise (docs/self-hosting.md)."
            );
        }
    } else {
        for (i, pair) in inv.pairs.iter().enumerate() {
            let placed: Vec<(String, Option<&String>)> = pair
                .iter()
                .map(|a| {
                    let h = host_of(a).to_string();
                    let z = inv.zones.get(&h);
                    (h, z)
                })
                .collect();
            // Fail closed on a partial declaration: knowing SOME of the
            // topology is the state that looks safe and is not.
            let unzoned: Vec<&str> = placed
                .iter()
                .filter(|(_, z)| z.is_none())
                .map(|(h, _)| h.as_str())
                .collect();
            if !unzoned.is_empty() {
                note(
                    false,
                    &format!("pair {i} every member has a zone"),
                    format!(
                        "no `zone` line for {} — partial declaration is refused, \
                         because it reads as anti-affinity without being one",
                        unzoned.join(", ")
                    ),
                );
                continue;
            }
            let zones: Vec<&str> = placed
                .iter()
                .filter_map(|(_, z)| z.map(|s| s.as_str()))
                .collect();
            let distinct = zones.iter().collect::<std::collections::HashSet<_>>().len();
            let shown: Vec<String> = placed
                .iter()
                .map(|(h, z)| format!("{h} ({})", z.map(|s| s.as_str()).unwrap_or("?")))
                .collect();
            note(
                distinct == zones.len(),
                &format!("pair {i} members are in distinct failure domains"),
                shown.join(" + "),
            );
        }
    }

    head("== proxies");
    for (i, p) in inv.proxies.iter().enumerate() {
        note(proxy_up(inv, i), "proxy up", p.clone());
    }

    // The CP's proxy registry must contain exactly the proxies this inventory
    // declares, under the identity they run with (advertise when set).
    //
    // Registrations are append-only, so a fleet re-bootstrapped after its
    // identity changed — bind address one time, DNS name the next — carries
    // BOTH rows forever with nothing marking which is live. Tenant placement
    // shuffle-shards across that list, so a new tenant can be handed a name no
    // running proxy answers to and then fail authentication at the edge with a
    // byte-correct token. That happened on the playground and took a digest
    // comparison to diagnose, because nothing logs it.
    let declared: Vec<String> = (0..inv.proxies.len())
        .map(|i| probe_target(inv, i))
        .collect();
    match call_cp(inv, &tls, &["CPPROXIES"]) {
        Ok(Value::Bulk(Some(raw))) => {
            let listed: Vec<String> = String::from_utf8_lossy(&raw)
                .split(',')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            let strays: Vec<&String> = listed.iter().filter(|r| !declared.contains(r)).collect();
            let missing: Vec<&String> = declared.iter().filter(|d| !listed.contains(d)).collect();
            note(
                strays.is_empty(),
                "registry has no stray proxies",
                if strays.is_empty() {
                    format!("{} registered", listed.len())
                } else {
                    format!(
                        "{strays:?} registered but not in the inventory — tenants placed there \
                         get -WRONGPASS; retire with `flintctl retire-proxy <addr>`"
                    )
                },
            );
            note(
                missing.is_empty(),
                "every declared proxy is registered",
                if missing.is_empty() {
                    String::new()
                } else {
                    format!("{missing:?} declared but never registered")
                },
            );
        }
        other => note(false, "CPPROXIES", format!("{other:?}")),
    }

    // The data-plane probe needs a tenant credential; without one the
    // structural checks above still run, but the checks that actually catch
    // a stale routing table cannot. Say so rather than imply a clean bill.
    match (probe, inv.proxies.first()) {
        (Some(spec), Some(_)) => {
            let (tenant, token) = spec.split_once(':').unwrap_or((spec, ""));
            // Dial the ADVERTISE address over EDGE tls — what a tenant dials,
            // with the trust a tenant uses. Probing the bind address in
            // plaintext tested a path no client takes.
            let target = probe_target(inv, 0);
            let edge = edge_tls_client(inv);
            let proxy = &target;
            head(&format!(
                "== data plane through {proxy} as {tenant} ({})",
                if edge.is_some() {
                    "client-tls"
                } else {
                    "plaintext"
                }
            ));
            let t = Duration::from_secs(10);
            // Each probe is its own authenticated session; call_seq returns
            // the LAST reply, which is the one being asserted on.
            match call_seq_on(proxy, &edge, &[&["AUTH", token], &["PING"]], t, true) {
                Ok(Value::Simple(ref r)) if r == "PONG" => note(true, "auth + ping", String::new()),
                other => note(false, "auth + ping", format!("{other:?}")),
            }
            // DBSIZE fans out to EVERY master: one stale entry fails it.
            //
            // It is also the O(keys) class — the node walks every metadata
            // row — so it gets a budget sized for that, not the 10 s the
            // cheap probes use. The client budget must EXCEED the proxy's
            // own fan-out timeout (60 s by default, `fanout-timeout-ms`):
            // whoever gives up first writes the error, and the proxy's
            // version names the pair that was slow while ours would say
            // only "timeout". Scale run 19 was diagnosable in one pass
            // precisely because the proxy answered first.
            let t_fanout = Duration::from_secs(90);
            match call_seq_on(
                proxy,
                &edge,
                &[&["AUTH", token], &["DBSIZE"]],
                t_fanout,
                true,
            ) {
                Ok(Value::Integer(n)) => note(
                    true,
                    "DBSIZE fan-out reaches every master",
                    format!("{n} keys"),
                ),
                other => note(false, "DBSIZE fan-out", format!("{other:?}")),
            }
            // SCAN only OPENS the cursor: the first batch comes from the
            // first master, so a pair three deep can be dead and this still
            // succeeds — observed exactly that while testing. DBSIZE above
            // is the check that touches every master. Labelled for what it
            // actually proves, because a green line that reads "reaches
            // every master" while one is dead is worse than no check.
            match call_seq_on(proxy, &edge, &[&["AUTH", token], &["SCAN", "0"]], t, true) {
                Ok(Value::Array(Some(_))) => note(true, "SCAN opens a cursor", String::new()),
                other => note(false, "SCAN opens a cursor", format!("{other:?}")),
            }
            // A write read back and cleaned up: the keyed path end to end.
            // A write read back and cleaned up: the keyed path end to end —
            // for EVERY PAIR, not for whichever one a single key happened to
            // hash to (#170).
            //
            // This check used to write exactly one key. One key is one slot on
            // one pair, so a pair that could not take a write at all still
            // passed as long as `__verify__:<pid>` hashed somewhere else — and
            // the DBSIZE fan-out above walks the master LIST without checking
            // role, so it stayed green too. That combination is what made the
            // first 10 TB run so confusing: two checks called the fleet healthy
            // while a quarter of it was refusing keyed writes (#167/#168).
            //
            // So synthesise one key per pair using the SAME slot hash and
            // default-owner function the proxy routes with, and report which
            // pair failed. Attribution is by the DEFAULT slot map: a live
            // migration exception can move a slot off its default owner, so a
            // failure names the pair the key belongs to by default — the
            // failure itself is always real, only the label could be off
            // mid-migration, which the message says.
            let npairs = inv.pairs.len();
            let mut probe: Vec<Option<String>> = vec![None; npairs];
            let mut have = 0usize;
            let mut n: u32 = 0;
            while have < npairs && n < 200_000 {
                let k = format!("__verify__:{}:{n}", std::process::id());
                let slot = flint_slot::slot_for_key(k.as_bytes());
                if let Some(p) = flint_slot::default_pair(slot, &[], npairs)
                    && p < npairs
                    && probe[p].is_none()
                {
                    probe[p] = Some(k);
                    have += 1;
                }
                n += 1;
            }
            let mut bad: Vec<String> = Vec::new();
            for (i, key) in probe.iter().enumerate() {
                let Some(k) = key else {
                    // No key found for this pair: a search failure, not a
                    // fleet failure. Say so rather than quietly skipping it.
                    bad.push(format!("pair {i}: no probe key synthesised"));
                    continue;
                };
                let got = call_seq_on(
                    proxy,
                    &edge,
                    &[&["AUTH", token], &["SET", k, "1"], &["GET", k]],
                    t,
                    true,
                );
                if !matches!(got, Ok(Value::Bulk(Some(ref b))) if b == b"1") {
                    bad.push(format!("pair {i}: {got:?}"));
                }
                let _ = call_seq_on(proxy, &edge, &[&["AUTH", token], &["DEL", k]], t, true);
            }
            note(
                bad.is_empty(),
                &format!("write/read round trip on all {npairs} pair(s)"),
                if bad.is_empty() {
                    String::new()
                } else {
                    format!("{} (pair by default slot map)", bad.join("; "))
                },
            );
            // Inline command support: `redis-cli --pipe` and telnet debugging
            // both depend on it, and it failed silently when absent.
            note(
                probe_inline(proxy, token, &edge),
                "inline command accepted",
                String::new(),
            );
        }
        (None, _) => {
            println!("== data plane  SKIPPED (pass --probe <tenant>:<token> to include it)")
        }
        (_, None) => println!("== data plane  SKIPPED (no proxy in the inventory)"),
    }

    problems
}

/// Send one inline (non-RESP) command and see whether the proxy honours it.
/// Written by hand because every other path here speaks RESP.
fn probe_inline(proxy: &str, token: &str, tls: &Option<Arc<flint_tls::ClientConfig>>) -> bool {
    use std::io::{Read, Write};
    let Ok(mut s) = flint_tls::connect_edge(proxy, tls) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(std::time::Duration::from_secs(3)));
    if s.write_all(format!("AUTH {token}\r\nPING\r\n").as_bytes())
        .is_err()
    {
        return false;
    }
    let mut buf = [0u8; 256];
    let mut seen = Vec::new();
    for _ in 0..3 {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                seen.extend_from_slice(&buf[..n]);
                if seen
                    .windows(7)
                    .any(|w| w == b"+PONG\r\n".get(..7).unwrap_or(w))
                {
                    break;
                }
                if seen.ends_with(b"+PONG\r\n") {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    seen.ends_with(b"+PONG\r\n") || seen.windows(6).any(|w| w == b"+PONG\r")
}

fn status(inv: &Inventory) {
    let tls = tls_client(inv);
    // Every CP seat, not just the first: with a Raft group, "cp[0] answers"
    // hides a dead follower until the NEXT failure makes it a lost quorum.
    // ADR-0014 D1: `status` used to print `cp <addr> up` beside pair nodes
    // reporting `build v0.1.0-rc.33`. Three of five seat kinds carried no
    // stamp, and `upgrade` rolls all five — so the tiers that CHANGE during
    // an upgrade were the tiers nothing could identify.
    let mut controller_rows: Vec<String> = Vec::new();
    for seat in &inv.cp {
        let ok = matches!(call(seat, &tls, &["PING"]), Ok(Value::Simple(s)) if s == "PONG");
        let build = cpinfo_field(seat, &tls, "build:")
            .map(|b| flint_build::display(&b, env!("CARGO_PKG_VERSION")).to_string())
            .unwrap_or_else(|| "-".into());
        println!(
            "cp        {seat}  {:<6} build {build}",
            if ok { "up" } else { "DOWN" }
        );
        // The controller has no listener, so it is not a row we can probe —
        // it is a row the CP repeats back to us. Collected here and printed
        // after the seats it supervises.
        if let Some(lines) = cpinfo_controllers(seat, &tls) {
            controller_rows.extend(lines);
        }
    }
    for (i, pair) in inv.pairs.iter().enumerate() {
        for addr in pair {
            match info_field(addr, &tls, "role:") {
                Some(role) => {
                    let lag = info_field(addr, &tls, "seq_lag:").unwrap_or_default();
                    let live = info_field(addr, &tls, "live_replicas:").unwrap_or_default();
                    let epoch = info_field(addr, &tls, "role_epoch:").unwrap_or_default();
                    let reported = info_field(addr, &tls, "build:").unwrap_or_default();
                    let build = flint_build::display(&reported, env!("CARGO_PKG_VERSION"));
                    println!(
                        "pair {i}    {addr}  {role:<7} epoch {epoch:<7} build {build:<10} seq_lag {lag:<5} live_replicas {live}"
                    );
                }
                None => println!("pair {i}    {addr}  DOWN"),
            }
        }
    }
    for (i, proxy) in inv.proxies.iter().enumerate() {
        // The proxy's client port is plaintext (frontend TLS is separate
        // from the internal mesh): probe it without the mesh cert.
        let up = proxy_up(inv, i);
        let build = proxystats_field(inv, i, "build:")
            .map(|b| flint_build::display(&b, env!("CARGO_PKG_VERSION")).to_string())
            .unwrap_or_else(|| "-".into());
        println!(
            "proxy     {proxy}  {:<6} build {build}",
            if up { "up" } else { "DOWN" }
        );
    }
    // Printed even when empty is NOT an option here: an absent controller
    // row means the CP has heard from nobody, which during a roll is the
    // difference between "supervised" and "not". Say so rather than
    // rendering nothing and letting silence read as fine.
    if controller_rows.is_empty() {
        println!("controller          NONE REPORTING  (no controller has registered with the CP)");
    } else {
        for row in controller_rows {
            println!("controller  {row}");
        }
    }
    // Optional fleet-agent add-on: the `agent <addr>` inventory key starts
    // a flint-agent binary if one sits in the bins dir. The agent (fleet
    // metering/insights/automation) is not part of this repository; without
    // the binary the key simply has nothing to start.
    if let Some(agent) = &inv.agent {
        println!("agent     metrics http://{agent}/metrics");
    }
}

/// Knobs that MUST agree between the members of a pair (ADR-0014 D2).
///
/// Every one of these is read from a node's own arguments, so a roll that
/// updated one member and not the other leaves a pair that behaves one way
/// while its survivor behaves another — visible only during the failover
/// that needed it. Nothing in the fleet compared them before this.
///
/// Deliberately excludes everything that is STATE rather than
/// configuration: role, epoch, seq_lag, live_replicas, sst_bytes, disk_*,
/// and every counter. Those differ between a master and its replica by
/// definition, and listing them here would produce drift on every healthy
/// pair — a red that means nothing is a red nobody reads.
const PAIR_KNOBS: &[&str] = &[
    "lag_soft_ms:",
    "lag_hard_ms:",
    "min_replicas_to_write:",
    "widowed_grace_ms:",
    "fullsync_max:",
    "wal_fsync_ms:",
    "max_conns:",
];

/// Below this, a difference is assumed to be a roll in progress rather
/// than drift. `upgrade` restarts one member at a time and waits for it to
/// converge, so a seat younger than this may legitimately not match its
/// partner yet.
///
/// The ADR names the failure this avoids: a check whose red means "unlucky
/// timing" is a check people re-run instead of read. Suppressing here
/// costs a delayed report; not suppressing costs the report's credibility.
/// Overridable with FLINT_ROLL_GRACE_MS so a drill can exercise both sides
/// of the boundary without a two-minute sleep — the same shrink the
/// rotation drill applies to its drain window.
fn roll_grace_ms() -> u64 {
    std::env::var("FLINT_ROLL_GRACE_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(120_000)
}

fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// One JSON document describing the whole fleet (ADR-0014 D2).
///
/// Rendered from the same dials the human `status` uses, so the two cannot
/// disagree. `--json` rather than a daemon or a port: the AMI already ships
/// flintctl, a new listener would be attack surface for questions flintctl
/// can already ask, and the time-series surface is the exporter.
fn status_json(inv: &Inventory) {
    let tls = tls_client(inv);
    let mut out = String::from("{\n");

    out.push_str("  \"cp\": [\n");
    let mut controllers: Vec<String> = Vec::new();
    // Did any CP actually answer? Not "is one configured" — see below.
    let mut answered = false;
    for (i, seat) in inv.cp.iter().enumerate() {
        let up = matches!(call(seat, &tls, &["PING"]), Ok(Value::Simple(s)) if s == "PONG");
        let build = cpinfo_field(seat, &tls, "build:");
        out.push_str(&format!(
            "    {{\"addr\": {}, \"up\": {}, \"build\": {}}}{}\n",
            json_str(seat),
            up,
            build.map(|b| json_str(&b)).unwrap_or("null".into()),
            if i + 1 < inv.cp.len() { "," } else { "" }
        ));
        if let Some(rows) = cpinfo_controllers(seat, &tls) {
            answered = true;
            controllers.extend(rows);
        }
    }
    out.push_str("  ],\n");

    // Controllers are reported by the CP, not probed: no listener, by
    // design. `null` vs `[]` is load-bearing — null means no CP could be
    // reached to ask, [] means one was and knows of none, and during a
    // roll those mean very different things.
    //
    // The condition used to be `inv.cp.is_empty()`, which is not that
    // distinction: it asks whether a CP was CONFIGURED, not whether one
    // ANSWERED. A fleet with a CP in its inventory that was down reported
    // `controllers: []` — "asked, and there are none" — during exactly the
    // control-plane outage when a reader most needs to know the answer is
    // unavailable rather than empty. Found by the positive control on
    // journal_head: pointing an inventory at a dead CP showed
    // `journal_head: null` beside `controllers: []`, and only one of them
    // could be right.
    if !answered {
        out.push_str("  \"controllers\": null,\n");
    } else {
        out.push_str("  \"controllers\": [");
        out.push_str(
            &controllers
                .iter()
                .map(|r| json_str(r))
                .collect::<Vec<_>>()
                .join(", "),
        );
        out.push_str("],\n");
    }

    let mut drift: Vec<String> = Vec::new();
    let mut suppressed: Vec<String> = Vec::new();
    out.push_str("  \"pairs\": [\n");
    for (i, pair) in inv.pairs.iter().enumerate() {
        out.push_str(&format!("    {{\"pair\": {i}, \"members\": [\n"));
        // Field maps per member, kept for the drift comparison below.
        let mut seen: Vec<(String, Option<std::collections::BTreeMap<String, String>>)> =
            Vec::new();
        for (m, addr) in pair.iter().enumerate() {
            let fields = flintinfo_all(addr, &tls);
            match &fields {
                Some(f) => {
                    let mut kv: Vec<String> = f
                        .iter()
                        .map(|(k, v)| format!("{}: {}", json_str(k), json_str(v)))
                        .collect();
                    kv.sort();
                    out.push_str(&format!(
                        "      {{\"addr\": {}, \"up\": true, {}}}{}\n",
                        json_str(addr),
                        kv.join(", "),
                        if m + 1 < pair.len() { "," } else { "" }
                    ));
                }
                None => out.push_str(&format!(
                    "      {{\"addr\": {}, \"up\": false}}{}\n",
                    json_str(addr),
                    if m + 1 < pair.len() { "," } else { "" }
                )),
            }
            seen.push((addr.clone(), fields));
        }
        out.push_str(&format!(
            "    ]}}{}\n",
            if i + 1 < inv.pairs.len() { "," } else { "" }
        ));

        // THE CHECK NO PER-NODE VIEW CAN DO. Compare every member against
        // the first that answered.
        let live: Vec<_> = seen
            .iter()
            .filter_map(|(a, f)| f.as_ref().map(|f| (a, f)))
            .collect();
        if live.len() > 1 {
            // Why a difference might not be reportable. THREE states, not
            // two: reported, held back because a member is mid-roll, and
            // held back because we cannot tell — a node predating
            // ADR-0014 D2 serves no `uptime_ms` at all.
            //
            // That third case was silently folded into the second. Every
            // fleet older than this change would have had ALL drift
            // suppressed with no trace, and `drift: []` read as "clean"
            // when it meant "not evaluated". Found by running it against
            // the rc.37 playground and noticing `uptime_ms: ['-','-']`
            // rather than by reading the code — the values happened to
            // match, so the empty array looked right.
            let unknown_uptime = live.iter().any(|(_, f)| f.get("uptime_ms").is_none());
            let young = live.iter().any(|(_, f)| {
                f.get("uptime_ms")
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_some_and(|u| u < roll_grace_ms())
            });
            let hold = unknown_uptime || young;
            let (base_addr, base) = live[0];
            for (addr, f) in &live[1..] {
                for knob in PAIR_KNOBS {
                    let k = knob.trim_end_matches(':');
                    let (a, b) = (base.get(k), f.get(k));
                    if a.is_some() && a != b {
                        let note = if unknown_uptime {
                            " (NOT EVALUATED: a member reports no uptime_ms — it predates \
                             ADR-0014 D2, so a roll cannot be told from drift. Roll the \
                             fleet to a build that reports it.)"
                        } else {
                            " (held back: a member has been up less than a roll window)"
                        };
                        let row = format!(
                            "pair {i} {k}: {base_addr}={} {addr}={}{}",
                            a.map(String::as_str).unwrap_or("-"),
                            b.map(String::as_str).unwrap_or("-"),
                            if hold { note } else { "" }
                        );
                        if hold {
                            // Emitted, not discarded. A suppression nobody
                            // can see is indistinguishable from a clean
                            // fleet, which is the whole failure this
                            // report exists to avoid.
                            suppressed.push(row);
                        } else {
                            drift.push(row);
                        }
                    }
                }
            }
        }
    }
    out.push_str("  ],\n");

    out.push_str("  \"proxies\": [\n");
    for (i, proxy) in inv.proxies.iter().enumerate() {
        let build = proxystats_field(inv, i, "build:");
        out.push_str(&format!(
            "    {{\"addr\": {}, \"up\": {}, \"build\": {}}}{}\n",
            json_str(proxy),
            proxy_up(inv, i),
            build.map(|b| json_str(&b)).unwrap_or("null".into()),
            if i + 1 < inv.proxies.len() { "," } else { "" }
        ));
    }
    out.push_str("  ],\n");

    // ADR-0014 D2 named "the fleet-journal head" among what this document
    // carries. It was never built and never recorded as dropped — the
    // as-built note simply stopped mentioning it, so the ADR claimed
    // something the code did not do. Same shape as the signing step that
    // left with release.yml: nothing wrong at any one step, and no single
    // diff that said a capability was going.
    //
    // Lines are emitted as STRINGS, not spliced in as raw JSON, even though
    // each is a JSON object. #116 was two JSON objects on one journal line;
    // splicing would let one malformed line make the whole status document
    // unparseable, which is a bad trade for saving the caller a `fromjson`.
    //
    // First CP that answers wins. They serve the same journal, and asking
    // all of them would concatenate duplicates into a fake history.
    match inv
        .cp
        .iter()
        .find_map(|s| journal_head(s, &tls, JOURNAL_HEAD_N))
    {
        Some(lines) => {
            out.push_str("  \"journal_head\": [");
            out.push_str(
                &lines
                    .iter()
                    .map(|l| json_str(l))
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            out.push_str("],\n");
        }
        // null, not []: no CP could be asked. An empty array here would say
        // the fleet has done nothing, which during a control-plane outage is
        // the most misleading thing this document could report.
        None => out.push_str("  \"journal_head\": null,\n"),
    }

    out.push_str("  \"drift\": [");
    out.push_str(
        &drift
            .iter()
            .map(|d| json_str(d))
            .collect::<Vec<_>>()
            .join(", "),
    );
    // `drift: []` is only trustworthy when this is empty too. A caller
    // gating on drift alone would read "not evaluated" as "clean".
    out.push_str("],\n  \"drift_not_evaluated\": [");
    out.push_str(
        &suppressed
            .iter()
            .map(|d| json_str(d))
            .collect::<Vec<_>>()
            .join(", "),
    );
    out.push_str("]\n}\n");
    print!("{out}");
    // Exit 0 even with drift: this is a REPORT, and ADR-0014 is explicit
    // that D2 does not reconcile. A caller that wants to gate reads the
    // array — which is why it is always present, empty when clean, rather
    // than omitted.
}

/// Every `key: value` line of one node's FLINTINFO. `None` when the node
/// did not answer — distinct from an empty map, which would mean it
/// answered with nothing.
fn flintinfo_all(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
) -> Option<std::collections::BTreeMap<String, String>> {
    let Ok(Value::Bulk(Some(raw))) = call(addr, tls, &["FLINTINFO"]) else {
        return None;
    };
    Some(
        String::from_utf8_lossy(&raw)
            .split(['\r', '\n'])
            .filter_map(|l| l.split_once(':'))
            .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
            .collect(),
    )
}

fn tenant_add(inv: &Inventory, rest: &[String]) {
    let tls = tls_client(inv);
    let mut args = vec!["CPADDTENANT"];
    let strs: Vec<&str> = rest.iter().map(|s| s.as_str()).collect();
    args.extend(strs);
    match call_cp(inv, &tls, &args) {
        Ok(Value::Simple(s)) => println!("{s}"),
        other => fail("tenant add failed", &other),
    }
}

/// Remove a tenant end to end: look up its namespace, revoke the record at
/// the CP (CPDELTENANT — auth dies on the next snapshot push, and the ns's
/// slot-exception rows retire with it), then WIPE the namespace's data on
/// every pair master (FLINTNS + FLUSHALL — namespace-scoped by the tenancy
/// invariant). Data wipe is best-effort per pair and reported; re-running
/// is safe (the wipe is idempotent, the CP delete errors "no such tenant").
/// Retire a proxy registration the inventory no longer declares.
///
/// The counterpart to what `verify` reports. Registrations are append-only,
/// so a re-bootstrapped fleet whose proxy identity changed keeps both rows,
/// and tenant placement can shuffle-shard onto the dead one — a tenant that
/// then answers -WRONGPASS at every edge with a byte-correct token.
///
/// Deliberately NOT automatic. `start` runs on every boot, and silently
/// deleting registry rows on boot is the kind of helpfulness that removes a
/// proxy someone meant to keep. verify names it; the operator retires it.
fn retire_proxy(inv: &Inventory, addr: &str) {
    let tls = tls_client(inv);
    let declared: Vec<String> = (0..inv.proxies.len())
        .map(|i| probe_target(inv, i))
        .collect();
    if declared.iter().any(|d| d == addr) {
        die(&format!(
            "refusing: {addr} IS declared in this inventory — retiring it would strip a live \
             proxy from every tenant subset. Remove the proxy line first if that is the intent."
        ));
    }
    match call_cp(inv, &tls, &["CPDELPROXY", addr]) {
        Ok(Value::Simple(s)) => println!("{s}"),
        other => fail("retire-proxy failed", &other),
    }
}

fn tenant_remove(inv: &Inventory, name: &str) {
    let tls = tls_client(inv);
    // Namespace lookup BEFORE deletion (the record dies with the delete).
    let ns = match call_cp(inv, &tls, &["CPTENANTS"]) {
        Ok(Value::Bulk(Some(raw))) => String::from_utf8_lossy(&raw)
            .lines()
            .find_map(|l| {
                let mut it = l.split_whitespace();
                (it.next() == Some(name)).then(|| it.next().map(String::from))?
            })
            .unwrap_or_else(|| die(&format!("tenant remove failed: no such tenant {name:?}"))),
        other => fail("CPTENANTS failed", &other),
    };
    match call_cp(inv, &tls, &["CPDELTENANT", name]) {
        Ok(Value::Simple(s)) => eprintln!("== {s}"),
        other => fail("tenant remove failed", &other),
    }
    for i in 0..inv.pairs.len() {
        let master = pair_master(inv, &tls, &i.to_string());
        // ONE connection for both: FLINTNS pins this connection's namespace,
        // FLUSHALL then wipes exactly that namespace (a fresh connection
        // would wipe the default ns instead).
        let wiped = call_seq(
            &master,
            &tls,
            &[&["FLINTNS", &ns], &["FLUSHALL"]],
            Duration::from_secs(30),
        )
        .is_ok();
        eprintln!(
            "  pair {i} ({master}): ns {ns} {}",
            if wiped {
                "wiped"
            } else {
                "WIPE FAILED (re-run)"
            }
        );
    }
    eprintln!("== tenant {name} removed (ns {ns})");
}

fn expand(inv: &Inventory, inventory_path: &str, pair_spec: &str) {
    let tls = tls_client(inv);
    let pair: Vec<String> = pair_spec.split(',').map(String::from).collect();
    eprintln!("== expand: new pair {pair_spec}");
    // The new pair's group index: appended after the existing pairs, which
    // is where the controller reroll will number it.
    start_pair_nodes(inv, &pair, inv.pairs.len());
    assert!(
        wait_pong(&pair[0], &tls, Duration::from_secs(10)),
        "new master up"
    );
    // "-": the new pair owns no slots yet — capacity joins without
    // re-routing; migration (controller rebalance) moves slots later.
    must(
        "register new pair",
        call_cp(inv, &tls, &["CPADDPAIR", pair_spec, "-"]),
    );
    // Persist to the inventory, then reroll the controller with the new
    // pair list. Controllers are STATELESS (ADR-0004): restarting one is a
    // non-event — it re-derives everything from the nodes' manifests.
    let mut raw = std::fs::read_to_string(inventory_path).expect("inventory");
    raw.push_str(&format!("pair {pair_spec}\n"));
    std::fs::write(inventory_path, raw).expect("inventory update");
    let mut inv2 = inv.clone();
    inv2.pairs.push(pair);
    if inv.controller {
        let t0 = now_ms();
        kill_pidfile(inv, &controller_runner(inv), "controller");
        start_controller(&inv2);
        wait_supervised(&inv2, inv2.pairs.len(), t0);
        // The backup seat pins the SAME pair list; reroll it with the
        // controller or every later set silently omits the change.
        if inv2.backup_to.is_some() {
            kill_pidfile(inv, &backup_runner(inv), "backup");
            start_backup(&inv2);
        }
        eprintln!(
            "  controller rerolled with {} pairs (stateless: a non-event)",
            inv2.pairs.len()
        );
    }
    eprintln!("== expand complete");
}

/// Add a replica to a pair (ADR-0005 D7 topology knob): spawn a fresh
/// replica of the pair's master, wait for it to converge, then CPSETPAIR to
/// append it to the pair's membership. Pure capacity: it owns nothing new,
/// serves reads for tenants that opted into replica reads. Reroll the
/// controller so it supervises the wider member set.
fn add_replica(inv: &Inventory, inventory_path: &str, pair_ref: &str, new: &str) {
    let tls = tls_client(inv);
    // pair_ref is a pair index or any current member address.
    let pair_idx = match pair_ref.parse::<usize>() {
        Ok(i) if i < inv.pairs.len() => i,
        _ => inv
            .pairs
            .iter()
            .position(|p| p.iter().any(|a| a == pair_ref))
            .unwrap_or_else(|| panic!("{pair_ref} is not a pair index or a known member")),
    };
    let master = inv.pairs[pair_idx]
        .iter()
        .find(|a| info_field(a, &tls, "role:").as_deref() == Some("master"))
        .unwrap_or_else(|| panic!("pair {pair_idx} has no reachable master"))
        .clone();
    eprintln!("== add-replica: {new} -> pair {pair_idx} (master {master})");

    let d = &inv.statedir;
    let port = port_of(new);
    let mut args = vec![
        "--port".to_string(),
        port.to_string(),
        "--bind".into(),
        host_of(new).to_string(),
        "--engine".into(),
        "rocks".into(),
        "--data-dir".into(),
        format!("{d}/node-{port}"),
        "--journal".into(),
        inv.cp[0].clone(),
        "--replica-of".into(),
        master.clone(),
    ];
    args.extend(internal_args(inv));
    // Spawned with --replica-of: a pair member by construction.
    args.extend(node_tuning_args(inv, true));
    spawn(
        inv,
        &runner_for(inv, new),
        &format!("node-{port}"),
        "flint-server",
        &args,
    );
    assert!(
        wait_pong(new, &tls, node_ready_budget(inv)),
        "new replica up"
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if info_field(&master, &tls, "seq_lag:").as_deref() == Some("0") {
            break;
        }
        assert!(Instant::now() < deadline, "new replica never converged");
        std::thread::sleep(Duration::from_millis(300));
    }
    eprintln!("  new replica converged (seq_lag 0)");

    let mut members = inv.pairs[pair_idx].clone();
    members.push(new.to_string());
    must(
        "CPSETPAIR",
        call(
            &inv.cp[0],
            &tls,
            &["CPSETPAIR", &pair_idx.to_string(), &members.join(",")],
        ),
    );

    let raw = std::fs::read_to_string(inventory_path).expect("inventory");
    let updated = raw.replace(
        &format!("pair {}", inv.pairs[pair_idx].join(",")),
        &format!("pair {}", members.join(",")),
    );
    std::fs::write(inventory_path, updated).expect("inventory update");
    // The mirror of decommission's call: a pair that just grew back past one
    // member has a peer again, so the existing node — which may have been
    // spawned alone, with the grace off — must start honouring it.
    reconcile_widowed_grace(inv, &members, &tls);
    if inv.controller {
        let t0 = now_ms();
        let mut inv2 = inv.clone();
        inv2.pairs[pair_idx] = members;
        kill_pidfile(inv, &controller_runner(inv), "controller");
        start_controller(&inv2);
        wait_supervised(&inv2, inv2.pairs.len(), t0);
        // The backup seat pins the SAME pair list; reroll it with the
        // controller or every later set silently omits the change.
        if inv2.backup_to.is_some() {
            kill_pidfile(inv, &backup_runner(inv), "backup");
            start_backup(&inv2);
        }
    }
    eprintln!(
        "== add-replica complete: pair {pair_idx} now has an extra replica for D7 read fan-out"
    );
}

fn swap_node(inv: &Inventory, inventory_path: &str, bad: &str, new: &str) {
    let tls = tls_client(inv);
    let Some(pair_idx) = inv.pairs.iter().position(|p| p.iter().any(|a| a == bad)) else {
        panic!("{bad} is not in any pair in the inventory");
    };
    // Refuse to swap a live master out from under the pair: kill/demote it
    // first (the controller will promote the survivor), then swap the seat.
    if info_field(bad, &tls, "role:").as_deref() == Some("master") {
        panic!("{bad} is currently a live MASTER; fail it over first, then swap");
    }
    let master = inv.pairs[pair_idx]
        .iter()
        .find(|a| info_field(a, &tls, "role:").as_deref() == Some("master"))
        .unwrap_or_else(|| panic!("pair {pair_idx} has no reachable master"))
        .clone();
    eprintln!("== swap: {bad} -> {new} (pair {pair_idx}, master {master})");

    // Fresh replica on the replacement seat (spawn-a-fresh-node model).
    let d = &inv.statedir;
    let port = port_of(new);
    let mut args = vec![
        "--port".to_string(),
        port.to_string(),
        "--bind".into(),
        host_of(new).to_string(),
        "--engine".into(),
        "rocks".into(),
        "--data-dir".into(),
        format!("{d}/node-{port}"),
        "--journal".into(),
        inv.cp[0].clone(),
        "--replica-of".into(),
        master.clone(),
    ];
    args.extend(internal_args(inv));
    // Spawned with --replica-of: a pair member by construction.
    args.extend(node_tuning_args(inv, true));
    spawn(
        inv,
        &runner_for(inv, new),
        &format!("node-{port}"),
        "flint-server",
        &args,
    );
    assert!(
        wait_pong(new, &tls, node_ready_budget(inv)),
        "replacement up"
    );

    // Wait for full convergence before the seat changes hands.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if info_field(&master, &tls, "seq_lag:").as_deref() == Some("0") {
            break;
        }
        assert!(Instant::now() < deadline, "replacement never converged");
        std::thread::sleep(Duration::from_millis(300));
    }
    eprintln!("  replacement converged (seq_lag 0)");

    // The pair id is the stable identity; membership floats.
    let new_members: Vec<String> = inv.pairs[pair_idx]
        .iter()
        .map(|a| if a == bad { new.to_string() } else { a.clone() })
        .collect();
    must(
        "CPSETPAIR",
        call(
            &inv.cp[0],
            &tls,
            &["CPSETPAIR", &pair_idx.to_string(), &new_members.join(",")],
        ),
    );
    kill_pidfile(
        inv,
        &runner_for(inv, bad),
        &format!("node-{}", port_of(bad)),
    );

    // Persist + reroll the controller onto the new membership.
    let raw = std::fs::read_to_string(inventory_path).expect("inventory");
    let updated = raw.replace(
        &format!("pair {}", inv.pairs[pair_idx].join(",")),
        &format!("pair {}", new_members.join(",")),
    );
    std::fs::write(inventory_path, updated).expect("inventory update");
    if inv.controller {
        let t0 = now_ms();
        let mut inv2 = inv.clone();
        inv2.pairs[pair_idx] = new_members;
        kill_pidfile(inv, &controller_runner(inv), "controller");
        start_controller(&inv2);
        wait_supervised(&inv2, inv2.pairs.len(), t0);
        // The backup seat pins the SAME pair list; reroll it with the
        // controller or every later set silently omits the change.
        if inv2.backup_to.is_some() {
            kill_pidfile(inv, &backup_runner(inv), "backup");
            start_backup(&inv2);
        }
    }
    eprintln!(
        "== swap complete: pair {pair_idx} seat replaced, registry + inventory updated (supervision armed)"
    );
}

// ---------- canary upgrade ----------

fn epoch_counter(epoch: &str) -> Option<u32> {
    epoch
        .trim()
        .trim_matches(|c| c == '(' || c == ')')
        .split(',')
        .nth(1)
        .and_then(|c| c.trim().parse().ok())
}

/// Journal gate: no event of a disallowed kind since `since_ms`. The fleet
/// journal is the abort signal — an unexpected role transition mid-roll
/// means the fleet is fighting something else; stop adding variables.
fn journal_clean(inv: &Inventory, since_ms: u64, disallowed: &[&str]) -> Result<(), String> {
    let tls = tls_client(inv);
    let Ok(Value::Bulk(Some(raw))) = call_cp(inv, &tls, &["CPJOURNALREAD", "500"]) else {
        // FAIL CLOSED: an upgrade must not roll blind. If the journal is
        // unreadable we cannot distinguish "quiet fleet" from "on fire".
        return Err("fleet journal unreachable — refusing to roll without the gate".into());
    };
    for line in String::from_utf8_lossy(&raw).lines() {
        let at = line
            .split("\"at_ms\":")
            .nth(1)
            .and_then(|r| r.split(',').next())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0);
        if at < since_ms {
            continue;
        }
        for kind in disallowed {
            if line.contains(&format!("\"kind\":\"{kind}\"")) {
                return Err(format!("unexpected {kind} in the fleet journal: {line}"));
            }
        }
    }
    Ok(())
}

/// Roll one node to the new build: kill, respawn on the SAME data dir (an
/// upgrade is a binary swap + warm restart, never a resync) as a replica of
/// `master`, then wait for the build stamp and full convergence.
fn roll_node(
    inv: &Inventory,
    addr: &str,
    master: &str,
    envs: &[(String, String)],
    expect_build: &Option<String>,
    wipe: bool,
) -> Result<(), String> {
    let tls = tls_client(inv);
    let d = &inv.statedir;
    let port = port_of(addr);
    // Warm restart is only trusted for a seat OBSERVED as a live replica
    // right now: a replica's history is a prefix of the master's by
    // construction, so its binary can be swapped and the tail resumes.
    // Anything else — dead seat, unknown lineage, (ex-)master — resyncs
    // fresh. (Checking after the respawn is a race against the controller
    // fencing a returning zombie; checking BEFORE the kill is not.)
    let wipe = wipe || info_field(addr, &tls, "role:").as_deref() != Some("replica");
    stop_seat(
        inv,
        &runner_for(inv, addr),
        &format!("node-{port}"),
        "flint-server",
        &format!("{d}/node-{port}"),
        Some(port),
    )?;
    if wipe {
        // Ex-masters NEVER warm-rejoin, even durably demoted: their
        // replication cursor is an ex-master's (not a tail position) and
        // their unreplicated suffix may have diverged from the new lineage.
        // The MARKER (not a wipe, #187/#190) hands the server the same
        // contract with a cheap continuation: rewind to a local snapshot the
        // master vouches for, or discard and full-sync when none exists —
        // the exact behaviour the wipe used to force every time. Soak run 29
        // measured the difference this path makes: its chaos driver restarts
        // seats through restart-node -> HERE, so every mid-soak rejoin was
        // still a full re-seed (97 files, 73s of shed writes) while the
        // bring-up's `start` path rewound in under a second.
        std::thread::sleep(Duration::from_millis(300));
        mark_reseed_node(
            inv,
            &runner_for(inv, addr),
            port,
            &format!("superseded copy rejoining the lineage held by {master}"),
        )?;
    }
    let mut args = vec![
        "--port".to_string(),
        port.to_string(),
        "--bind".into(),
        host_of(addr).to_string(),
        "--engine".into(),
        "rocks".into(),
        "--data-dir".into(),
        format!("{d}/node-{port}"),
        "--journal".into(),
        inv.cp[0].clone(),
        "--replica-of".into(),
        master.to_string(),
    ];
    // The pair's snapshot dir on this seat's host, so a marked rejoin can
    // rewind (same plumbing as start_pair_nodes).
    if let Some(gi) = inv.pairs.iter().position(|p| p.iter().any(|a| a == addr)) {
        args.extend(["--rewind-snaps".into(), format!("{d}/snaps/g{gi}")]);
    }
    args.extend(internal_args(inv));
    // Spawned with --replica-of: a pair member by construction.
    args.extend(node_tuning_args(inv, true));
    spawn_env(
        inv,
        &runner_for(inv, addr),
        &format!("node-{port}"),
        "flint-server",
        &args,
        envs,
    );
    if !wait_pong(addr, &tls, node_ready_budget(inv)) {
        return Err(format!("{addr} did not come back after the binary swap"));
    }
    if let Some(want) = expect_build {
        let got = info_field(addr, &tls, "build:").unwrap_or_default();
        if &got != want {
            return Err(format!("{addr} reports build {got:?}, expected {want:?}"));
        }
    }
    // Converged again (warm restart: the tail resumes from its own data).
    // Under LIVE write load seq_lag hovers above zero by design (writes
    // keep arriving between acks), so convergence here is sequence lag zero
    // OR time lag comfortably inside the healthy band. The master-phase
    // DRAIN check stays strictly seq_lag==0 — writes are fenced off there.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let seq0 = info_field(master, &tls, "seq_lag:").as_deref() == Some("0");
        let time_ok = info_field(master, &tls, "lag_ms:")
            .and_then(|v| v.parse::<u64>().ok())
            .is_some_and(|ms| ms < 500);
        if seq0 || time_ok {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(format!("{addr} never reconverged behind {master}"));
        }
        std::thread::sleep(Duration::from_millis(300));
    }
}

/// The loss-critical failover core, shared by `upgrade` and `failover`:
/// fence at an epoch above everything either member has seen, then DEMOTE
/// the old master FIRST (it stops acking writes), DRAIN (its replica
/// applies every acked write), commit the CPFENCE record (ADR-0018), and
/// only THEN PROMOTE the new master.
/// Demote-first is what makes it lossless — a promote-first window lets the
/// old master ack writes the new lineage never contains; the no-master gap
/// between demote and promote is absorbed by the proxy's retry budget
/// (latency, not loss). The drain also guarantees the promotion target is
/// caught up, so a lagging replica never freezes an incomplete dataset.
/// Returns the promoted epoch counter.
fn controlled_failover(
    inv: &Inventory,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    pair: &[String],
    old_master: &str,
    new_master: &str,
) -> u32 {
    let next = pair
        .iter()
        .filter_map(|a| info_field(a, tls, "role_epoch:"))
        .filter_map(|e| epoch_counter(&e))
        .max()
        .unwrap_or(1)
        + 1;
    match call(old_master, tls, &["FLINTDEMOTE", "0", &next.to_string()]) {
        Ok(Value::Simple(_)) => {}
        Ok(Value::Error(e)) if e.starts_with("FENCED") => {}
        other => fail(&format!("demotion of {old_master} failed"), &other),
    }
    let drain_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if info_field(old_master, tls, "seq_lag:").as_deref() == Some("0") {
            break;
        }
        assert!(
            Instant::now() < drain_deadline,
            "replica never drained the demoted master {old_master}'s tail"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    // ADR-0018: the fencing record commits BEFORE the promotion, exactly as
    // the controller does. This is NOT the best-effort hint CPPROMOTED was —
    // it is what makes the promotion stick: the new master's own lease
    // renewals are answered against this record, so an unrecorded promotion
    // gets -SUPERSEDED and re-fences within a renewal interval. CPFENCE also
    // bumps the CP version and wakes watching proxies, subsuming CPPROMOTED's
    // notify role (#91) — which matters MORE on this planned path, where the
    // demoted old master stays up and the only reactive signal a proxy gets
    // is -READONLY on a write it already accepted.
    //
    // Mandatory, deliberately inverting the old "a CP that is down must
    // never make a maintenance failover fail": under CP-held leases a
    // promotion the CP never recorded is not a failover, it is a ~ttl/3
    // write window followed by a fence. Failing HERE is safe — the pair is
    // demoted and drained, nothing is lost, and the controller (or a re-run)
    // completes the handoff once the CP is reachable again.
    match call_cp(inv, tls, &["CPFENCE", new_master]) {
        Ok(Value::Simple(_)) => {}
        other => fail(
            &format!(
                "CPFENCE {new_master} did not commit — refusing to promote without \
                 a fencing record (the pair is demoted and drained; re-run once \
                 the CP is reachable, or the controller will finish the handoff)"
            ),
            &other,
        ),
    }
    match call(
        new_master,
        tls,
        &["FLINTPROMOTE", "0", &(next + 1).to_string()],
    ) {
        Ok(Value::Simple(_)) => {}
        other => fail(
            &format!("promotion of {new_master} at (0,{}) failed", next + 1),
            &other,
        ),
    }
    next + 1
}

/// `flintctl failover <node>`: a graceful, epoch-fenced handoff of a LIVE
/// master to its replica — the same demote→drain→promote core the upgrade
/// uses — after which the ex-master rejoins as a fresh replica of the new
/// master (wipe + checkpoint resync, the demote contract). No build change.
/// The one clean way to hand off a master on demand: run it before taking a
/// master out for maintenance or `decommission-node`.
/// Resolve a pair reference (index or any member address) to its live
/// master address.
fn pair_master(
    inv: &Inventory,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    pair_ref: &str,
) -> String {
    let idx = match pair_ref.parse::<usize>() {
        Ok(i) if i < inv.pairs.len() => i,
        _ => inv
            .pairs
            .iter()
            .position(|p| p.iter().any(|a| a == pair_ref))
            .unwrap_or_else(|| panic!("{pair_ref} is not a pair index or a known member")),
    };
    inv.pairs[idx]
        .iter()
        .find(|a| info_field(a, tls, "role:").as_deref() == Some("master"))
        .cloned()
        .unwrap_or_else(|| panic!("pair {idx} has no reachable master"))
}

/// `flintctl migrate-slots <ns> <lo-hi> <src> <dest>`: move a contiguous
/// slot range of ONE namespace from src's master to dest's master — the
/// same epoch-fenced FLINTMIGRATEIN cutover the auto-rebalancer uses, then
/// committed to the CP (CPSETSLOT) so every proxy (cold-started ones
/// included) routes it from the snapshot, not from -MOVED discovery. A live
/// writer sees a brief -TRYAGAIN / -MOVED bridge per slot, never loss.
/// Operator control for capacity moves the size-policy rebalancer would not
/// make on its own.
fn migrate_slots(inv: &Inventory, ns: &str, range: &str, src: &str, dest: &str) {
    let tls = tls_client(inv);
    let (lo, hi) = range
        .split_once('-')
        .and_then(|(a, b)| Some((a.parse::<u16>().ok()?, b.parse::<u16>().ok()?)))
        .unwrap_or_else(|| panic!("range must be lo-hi, e.g. 8000-8191 (got {range:?})"));
    assert!(lo <= hi, "range lo must be <= hi ({lo}-{hi})");
    assert!(hi < 16384, "slots are 0..16383 (got hi={hi})");
    let src_master = pair_master(inv, &tls, src);
    let dest_master = pair_master(inv, &tls, dest);
    assert!(
        src_master != dest_master,
        "src and dest resolve to the same master ({src_master}); nothing to move"
    );
    eprintln!(
        "== migrate-slots {ns} {lo}-{hi}: {src_master} -> {dest_master} ({} slot(s))",
        hi - lo + 1
    );
    let mut moved = 0u32;
    for slot in lo..=hi {
        match call_slow(
            &dest_master,
            &tls,
            &[
                "FLINTMIGRATEIN",
                &src_master,
                &slot.to_string(),
                &dest_master,
                ns,
            ],
            Duration::from_secs(120),
        ) {
            Ok(Value::Simple(_)) => {
                // Option B: commit ownership truth to the CP so proxies route
                // it from the snapshot. Best-effort — the -MOVED bridge still
                // routes correctly until the row lands, and CPSETSLOT is
                // idempotent to re-set.
                match call(
                    &inv.cp[0],
                    &tls,
                    &["CPSETSLOT", ns, &slot.to_string(), &dest_master],
                ) {
                    Ok(Value::Simple(_)) => {}
                    other => eprintln!(
                        "  warn: CPSETSLOT {ns}/{slot} not committed: {} (the -MOVED bridge still routes)",
                        reply_err(&other)
                    ),
                }
                moved += 1;
            }
            other => fail(
                &format!("migrate {ns}/{slot} {src_master}->{dest_master} failed"),
                &other,
            ),
        }
    }
    eprintln!("== migrate-slots complete: {moved} slot(s) now owned by {dest_master}");
    status(inv);
}

fn failover(inv: &Inventory, node: &str) {
    let tls = tls_client(inv);
    let Some(pair_idx) = inv.pairs.iter().position(|p| p.iter().any(|a| a == node)) else {
        panic!("{node} is not in any pair in the inventory");
    };
    let pair = &inv.pairs[pair_idx];
    if info_field(node, &tls, "role:").as_deref() != Some("master") {
        panic!("{node} is not the live master of pair {pair_idx}; nothing to fail over");
    }
    let new_master = pair
        .iter()
        .find(|a| *a != node && info_field(a, &tls, "role:").as_deref() == Some("replica"))
        .cloned()
        .unwrap_or_else(|| {
            panic!("pair {pair_idx} has no reachable replica to promote; add one first")
        });
    eprintln!("== failover: pair {pair_idx}, {node} -> {new_master}");
    let promoted = controlled_failover(inv, &tls, pair, node, &new_master);
    eprintln!("  {node} demoted + drained; {new_master} promoted at (0,{promoted})");
    // The ex-master rejoins as a fresh replica of the NEW master — no build
    // change (empty envs), wipe = the demote contract.
    if let Err(e) = roll_node(inv, node, &new_master, &[], &None, true) {
        panic!("ex-master {node} failed to rejoin as a replica: {e}");
    }
    eprintln!("== failover complete: {new_master} is master; {node} rejoined as replica");
    status(inv);
}

/// `flintctl decommission-node <addr> [--force] [--drain-ms N]`: drop ONE
/// member from its pair, leaving the pair running on its remaining node(s).
/// The inverse of `add-replica`. Refuses a live master (fail it over
/// first), the pair's last node (that is a whole shard — out of scope), or
/// a removal that would drop the pair below the master's
/// min-replicas-to-write and freeze writes (unless --force). Drains traffic
/// gracefully first: a time-bounded `drain_ms` wait (default 5000) during
/// which the node stays up and serving while proxies route off it, then it
/// is stopped.
fn decommission_node(
    inv: &Inventory,
    inventory_path: &str,
    addr: &str,
    force: bool,
    drain_ms: u64,
) {
    let tls = tls_client(inv);
    let Some(pair_idx) = inv.pairs.iter().position(|p| p.iter().any(|a| a == addr)) else {
        panic!("{addr} is not in any pair in the inventory");
    };
    let members = inv.pairs[pair_idx].clone();
    if members.len() <= 1 {
        panic!(
            "{addr} is the last node in pair {pair_idx} — that is the whole shard, \
             not a single-node decommission (out of scope)"
        );
    }
    if info_field(addr, &tls, "role:").as_deref() == Some("master") {
        panic!(
            "{addr} is the live MASTER of pair {pair_idx}; run \
             `flintctl -f <inv> failover {addr}` first, then decommission it"
        );
    }
    // min-replicas guard: what live replica count remains, vs what the
    // master needs to accept writes?
    let master = members
        .iter()
        .find(|a| info_field(a, &tls, "role:").as_deref() == Some("master"))
        .unwrap_or_else(|| panic!("pair {pair_idx} has no reachable master"));
    let min_repl: u32 = info_field(master, &tls, "min_replicas_to_write:")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let live_replicas: u32 = info_field(master, &tls, "live_replicas:")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let addr_live = info_field(addr, &tls, "role:").as_deref() == Some("replica");
    let after = if addr_live {
        live_replicas.saturating_sub(1)
    } else {
        live_replicas
    };
    if min_repl > 0 && after < min_repl && !force {
        panic!(
            "removing {addr} would leave pair {pair_idx} with {after} live replica(s), \
             below min-replicas-to-write={min_repl}: the master would FREEZE writes. \
             Add a replacement first, or pass --force to proceed anyway."
        );
    }
    eprintln!("== decommission-node: draining {addr} out of pair {pair_idx}");
    let remaining: Vec<String> = members.iter().filter(|a| *a != addr).cloned().collect();
    // GRACEFUL DRAIN, in order: remove the node from the topology FIRST
    // (proxies stop routing replica-reads to it on the next snapshot), let
    // that converge while the node is STILL UP AND SERVING, and only THEN
    // stop it — so no read, not even one the proxy would otherwise absorb
    // and retry on the master, ever hits a dying node. Writes never touch a
    // replica, so they are unaffected throughout. Bounded; the snapshot
    // push is sub-second, so a few seconds covers several cycles.
    must(
        "CPSETPAIR",
        call(
            &inv.cp[0],
            &tls,
            &["CPSETPAIR", &pair_idx.to_string(), &remaining.join(",")],
        ),
    );
    eprintln!("  draining {drain_ms}ms for proxies to route off {addr} (still serving)");
    std::thread::sleep(Duration::from_millis(drain_ms));
    kill_pidfile(
        inv,
        &runner_for(inv, addr),
        &format!("node-{}", port_of(addr)),
    );
    let raw = std::fs::read_to_string(inventory_path).expect("inventory");
    let updated = raw.replace(
        &format!("pair {}", members.join(",")),
        &format!("pair {}", remaining.join(",")),
    );
    std::fs::write(inventory_path, updated).expect("inventory update");
    // The pair just got smaller. If it is down to one node, that node is no
    // longer waiting on a peer and must stop behaving as though it were.
    reconcile_widowed_grace(inv, &remaining, &tls);
    if inv.controller {
        let t0 = now_ms();
        let mut inv2 = inv.clone();
        inv2.pairs[pair_idx] = remaining;
        kill_pidfile(inv, &controller_runner(inv), "controller");
        start_controller(&inv2);
        wait_supervised(&inv2, inv2.pairs.len(), t0);
        // The backup seat pins the SAME pair list; reroll it with the
        // controller or every later set silently omits the change.
        if inv2.backup_to.is_some() {
            kill_pidfile(inv, &backup_runner(inv), "backup");
            start_backup(&inv2);
        }
    }
    eprintln!(
        "== decommission-node complete: {addr} removed; pair {pair_idx} runs on its remaining node(s)"
    );
    status(inv);
}

/// Canary upgrade: one replica first, soak against the fleet journal, then
/// the remaining replicas, then masters LAST via an epoch-fenced controlled
/// failover (promote the already-upgraded replica, demote the old master,
/// warm-restart it on the new build as a replica). Any unexpected journal
/// transition aborts the roll — already-upgraded nodes stay (roll forward).
fn upgrade(inv: &Inventory, version_tag: Option<String>, soak_ms: u64, nodes_only: bool) {
    let tls = tls_client(inv);
    // Kept for binaries built before the tag was compiled in: release builds
    // now bake FLINT_RELEASE_TAG, which OUTRANKS this variable, so on a
    // current bundle the injection is inert and `expect` below compares
    // against what the binary itself says. That matters — while flintctl was
    // the only source of the stamp, setting the variable and then asserting
    // the value it had just set proved nothing about which binary had
    // actually been staged. Rolling ONTO an older build still needs it, so
    // it stays until the fleet's floor is past the change.
    let envs: Vec<(String, String)> = version_tag
        .iter()
        .map(|t| ("FLINT_BUILD_VERSION".to_string(), t.clone()))
        .collect();
    let expect = version_tag.clone();
    // Disallowed while replicas roll: ANY role transition. During the master
    // phase the promote/demote we issue are expected; Detected/SelfFenced/
    // SpareRestored still mean something else broke.
    const REPLICA_PHASE_DISALLOWED: &[&str] = &[
        "Detected",
        "PromoteIssued",
        "Promoted",
        "Demoted",
        "SelfFenced",
        "SpareRestored",
    ];
    const MASTER_PHASE_DISALLOWED: &[&str] = &["Detected", "SelfFenced", "SpareRestored"];

    // Observe roles now: replicas roll first, masters last.
    let mut masters: Vec<String> = Vec::new();
    let mut replicas: Vec<(String, String)> = Vec::new(); // (replica, its master)
    for pair in &inv.pairs {
        let m = pair
            .iter()
            .find(|a| info_field(a, &tls, "role:").as_deref() == Some("master"))
            .unwrap_or_else(|| panic!("pair without a reachable master; heal before upgrading"))
            .clone();
        for a in pair {
            if *a != m {
                replicas.push((a.clone(), m.clone()));
            }
        }
        masters.push(m);
    }
    let (canary, canary_master) = replicas
        .first()
        .cloned()
        .expect("need at least one replica");

    eprintln!(
        "== upgrade: canary {canary} first, soak {soak_ms}ms, then {} more replica(s), masters last",
        replicas.len() - 1
    );
    let t0 = now_ms();
    if let Err(e) = roll_node(inv, &canary, &canary_master, &envs, &expect, false) {
        panic!("CANARY FAILED (fleet untouched beyond the canary): {e}");
    }
    eprintln!("  canary {canary} on new build, reconverged; soaking {soak_ms}ms");
    std::thread::sleep(Duration::from_millis(soak_ms));
    if let Err(e) = journal_clean(inv, t0, REPLICA_PHASE_DISALLOWED) {
        eprintln!("== UPGRADE ABORTED at canary soak: {e}");
        eprintln!("   canary stays on the new build (roll forward after diagnosis)");
        std::process::exit(3);
    }
    eprintln!("  soak clean: no unexpected transitions in the fleet journal");

    for (r, m) in replicas.iter().skip(1) {
        let t = now_ms();
        // Re-resolve the pair's CURRENT master: roles may have moved since
        // upgrade start (that is exactly what the gate aborts on — but a
        // roll step must never sync against a captured-stale master).
        let pair = inv
            .pairs
            .iter()
            .find(|p| p.iter().any(|a| a == r))
            .expect("replica belongs to a pair");
        let m_now = pair
            .iter()
            .find(|a| info_field(a, &tls, "role:").as_deref() == Some("master"))
            .cloned()
            .unwrap_or_else(|| m.clone());
        if let Err(e) = roll_node(inv, r, &m_now, &envs, &expect, false) {
            // Same outcome as a failed journal check below — the roll
            // stopped part-way — so it reports the same way. Panicking here
            // gave the one MORE likely failure the worse exit code.
            eprintln!("== UPGRADE ABORTED at {r}: {e}");
            std::process::exit(3);
        }
        if let Err(e) = journal_clean(inv, t, REPLICA_PHASE_DISALLOWED) {
            eprintln!("== UPGRADE ABORTED after {r}: {e}");
            std::process::exit(3);
        }
        eprintln!("  replica {r} rolled");
    }

    eprintln!("== masters last (fenced controlled failover per pair)");
    for (i, pair) in inv.pairs.iter().enumerate() {
        let t = now_ms();
        let old_master = &masters[i];
        let new_master = pair
            .iter()
            .find(|a| *a != old_master)
            .expect("pair has an upgraded replica")
            .clone();
        // The loss-critical demote->drain->promote core (shared with the
        // `failover` verb).
        let promoted = controlled_failover(inv, &tls, pair, old_master, &new_master);
        eprintln!(
            "  pair {i}: {old_master} demoted + drained; {new_master} promoted at (0,{promoted})"
        );
        if let Err(e) = roll_node(inv, old_master, &new_master, &envs, &expect, true) {
            eprintln!("== UPGRADE ABORTED after pair {i} failover, respawning {old_master}: {e}");
            std::process::exit(3);
        }
        if let Err(e) = journal_clean(inv, t, MASTER_PHASE_DISALLOWED) {
            eprintln!("== UPGRADE ABORTED after pair {i} master roll: {e}");
            std::process::exit(3);
        }
        eprintln!("  pair {i}: old master rolled, tailing the new one warm");
    }
    if nodes_only {
        eprintln!(
            "== upgrade complete (DATA PLANE ONLY, --nodes-only): the proxy, control plane, \
             controller and agent are STILL RUNNING THE OLD BINARY"
        );
        status(inv);
        return;
    }
    roll_edge(inv, &envs, &expect);
    eprintln!("== upgrade complete (whole fleet)");
    status(inv);
}

/// Roll everything that is not a pair node: controller, agent, control
/// plane, proxies.
///
/// These have no role to hand over, so each is a stop-and-respawn rather than
/// a failover. Order is chosen so the client-facing hop moves last, over a
/// fleet that is already new: controller and agent first (nothing depends on
/// them for serving), then the control plane (proxies hold a routing cache
/// and reconnect their watch), then the proxies.
///
/// This phase is the difference between an upgrade and a deploy. Without it
/// `upgrade` rolls the two pair nodes and reports success — which is exactly
/// what would have happened shipping rc.12, whose entire point was two fixes
/// in the PROXY. A release that cannot deliver a proxy fix is not a release.
fn roll_edge(inv: &Inventory, envs: &[(String, String)], expect_build: &Option<String>) {
    let d = &inv.statedir;
    let tls = tls_client(inv);
    let die_on = |seat: &str, e: String| -> ! {
        eprintln!("== UPGRADE ABORTED rolling {seat}: {e}");
        eprintln!("   the data plane is already on the new build (roll forward)");
        std::process::exit(3);
    };
    // ADR-0014 D1. Pair nodes have been build-asserted since #105; these
    // three seats could not be, because they carried no stamp — so this
    // function's own abort message ("the data plane is already on the new
    // build") named the one tier it could see. A half-completed edge roll
    // was indistinguishable from a finished one.
    //
    // Skipped without --version-tag, exactly like the node assertion: there
    // is nothing to compare against, and inventing an expectation would
    // make a source build unrollable.
    let assert_build = |seat: &str, got: Option<String>| {
        let Some(want) = expect_build else { return };
        match got {
            Some(got) if &got == want => eprintln!("  {seat} reports {got}"),
            Some(got) => die_on(
                seat,
                format!(
                    "reports build {got:?}, expected {want:?} — the binary it restarted from is not the one that was staged"
                ),
            ),
            None => die_on(
                seat,
                "came up but would not report a build. Either it predates ADR-0014 D1 \
                 (roll it once more, from a binary that stamps) or it is not answering \
                 the info command at all — both mean this roll cannot be verified"
                    .into(),
            ),
        }
    };

    if inv.controller {
        eprintln!("== controller");
        if let Err(e) = stop_seat(
            inv,
            &controller_runner(inv),
            "controller",
            "flint-controller",
            &format!("{d}/snaps"),
            None,
        ) {
            die_on("controller", e);
        }
        spawn_env(
            inv,
            &controller_runner(inv),
            "controller",
            "flint-controller",
            &controller_args(inv),
            envs,
        );
    }

    if let Some(args) = agent_args(inv) {
        eprintln!("== agent");
        if let Err(e) = stop_seat(
            inv,
            &agent_runner(inv),
            "agent",
            "flint-agent",
            &format!("{d}/shadow.jsonl"),
            inv.agent.as_deref().map(port_of),
        ) {
            die_on("agent", e);
        }
        spawn_env(inv, &agent_runner(inv), "agent", "flint-agent", &args, envs);
    }

    eprintln!("== control plane");
    // EVERY seat, one at a time, under the name bootstrap gave it.
    //
    // This rolled only inv.cp[0] and always called it "cp". On a Raft group
    // that is the wrong name (bootstrap writes cp-n1/n2/n3), so the stop
    // aimed at a pidfile nobody wrote, decided the seat was already gone,
    // and then timed out waiting for a port its still-living process held.
    // Even had it worked, two of three seats would have kept the old binary
    // and the build gate would not have noticed, because it only ever asked
    // cp[0] — a mixed-build control plane reported as a finished roll.
    //
    // Sequential is the point: a Raft group keeps serving while a minority
    // is down, so one seat at a time is what makes this a rolling upgrade
    // instead of a control-plane outage. Each seat must answer AND report
    // the new build before the next one is touched.
    for i in 0..inv.cp.len() {
        let seat = inv.cp[i].clone();
        let name = cp_seat_name(inv, i);
        if let Err(e) = stop_seat(
            inv,
            &runner_for(inv, &seat),
            &name,
            "flint-controlplane",
            &cp_seat_state(inv, i),
            Some(port_of(&seat)),
        ) {
            die_on(&name, e);
        }
        spawn_env(
            inv,
            &runner_for(inv, &seat),
            &name,
            "flint-controlplane",
            &cp_seat_args(inv, i),
            envs,
        );
        if !wait_pong(&seat, &tls, Duration::from_secs(15)) {
            die_on(&name, "did not answer after the binary swap".into());
        }
        assert_build(&name, cpinfo_field(&seat, &tls, "build:"));
    }

    if inv.backup_to.is_some() {
        eprintln!("== backup seat");
        if let Err(e) = stop_seat(
            inv,
            &backup_runner(inv),
            "backup",
            "flint-backup",
            &format!("{d}/backup-snaps"),
            None,
        ) {
            die_on("backup", e);
        }
        if let Some(args) = backup_args(inv) {
            spawn_env(
                inv,
                &backup_runner(inv),
                "backup",
                "flint-backup",
                &args,
                envs,
            );
        }
    }

    eprintln!("== proxies last (clients see one blip, over an already-new fleet)");
    for (i, proxy) in inv.proxies.iter().enumerate() {
        let seat = format!("proxy-{}", port_of(proxy));
        // Identity is the ADVERTISE address: it is what this proxy was
        // started with and is unique per proxy, so the match cannot stray
        // onto a sibling. Same definition as proxy_args uses, or the match
        // would look for an argv that was never written.
        let ident = proxy_dial(inv, i);
        if let Err(e) = stop_seat(
            inv,
            &proxy_runner(inv, i),
            &seat,
            "flint-proxy",
            &ident,
            Some(port_of(proxy)),
        ) {
            die_on(&seat, e);
        }
        spawn_env(
            inv,
            &proxy_runner(inv, i),
            &seat,
            "flint-proxy",
            &proxy_args(inv, i),
            envs,
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        while !proxy_up(inv, i) {
            if Instant::now() > deadline {
                die_on(&seat, "did not serve after the binary swap".into());
            }
            std::thread::sleep(Duration::from_millis(150));
        }
        assert_build(&seat, proxystats_field(inv, i, "build:"));
        eprintln!("  {seat} rolled and serving");
    }

    // The controller last, because it is the only seat whose build arrives
    // by REGISTRATION rather than by asking. It re-registers on startup, so
    // a rolled controller has already reported by the time the proxies are
    // done — but give it a bounded wait rather than assuming that ordering
    // holds on a slower machine.
    //
    // This is the seat the recorded failure happened to: "a controller from
    // a previous start survived two upgrade cycles". Two cycles, because
    // nothing could be asked what build it was. If the CP now reports a
    // controller on the OLD build, that orphan is still running.
    if inv.controller && expect_build.is_some() {
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            // Every CP, not just the first. The controller registry is
            // node-local in both control planes — a controller names ONE
            // --commit-cp — so on a 3-seat Raft fleet asking cp[0] alone
            // reports nothing whenever the controller happens to be pointed
            // at cp[1], and this gate would time out on a healthy fleet.
            // `status --json` already unions across seats; this now matches.
            let rows: Vec<String> = inv
                .cp
                .iter()
                .filter_map(|c| cpinfo_controllers(c, &tls))
                .flatten()
                .collect();
            let want = expect_build.as_deref().unwrap_or_default();
            let live: Vec<&String> = rows.iter().filter(|r| r.contains(" live ")).collect();
            let all_new =
                !live.is_empty() && live.iter().all(|r| r.contains(&format!("build={want}")));
            if all_new {
                eprintln!("  controller reports {want} ({} live)", live.len());
                if live.len() > 1 {
                    eprintln!(
                        "  NOTE {} live controllers registered — expected one. \
                         They are epoch-fenced so the fleet is safe, but a duplicate \
                         supervisor is how an orphan survived two rolls unnoticed.",
                        live.len()
                    );
                }
                break;
            }
            if Instant::now() > deadline {
                for r in &rows {
                    eprintln!("   controller {r}");
                }
                die_on(
                    "controller",
                    format!(
                        "no live controller reported build {want} within 45s. Either the \
                         rolled controller cannot reach the CP to register, or an OLD \
                         controller is still running (see rows above)"
                    ),
                );
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }
}

fn stop(inv: &Inventory) {
    // Each host keeps its OWN pids dir, so "stop the fleet" means asking
    // every machine to stop what it is holding. Reading only the local
    // directory would leave remote seats running while reporting success.
    for r in all_runners(inv) {
        if r.is_remote() {
            let argv = vec![
                format!("{}/flintctl", inv.bins),
                "host-stop-all".into(),
                inv.statedir.clone(),
            ];
            match r.output(&argv) {
                Ok(out) => {
                    for line in String::from_utf8_lossy(&out.stdout).lines() {
                        eprintln!("  [{}] {line}", r.label());
                    }
                }
                Err(e) => eprintln!("  [{}] stop failed: {e}", r.label()),
            }
        } else {
            local_stop_all(&inv.statedir);
        }
    }
    let swept = sweep_orphans(inv);
    if swept > 0 {
        eprintln!("  {swept} orphan(s) swept — a start had run over a live fleet");
    }
}

/// The per-host half of the remote runner.
///
/// These run ON the machine they are about, invoked over ssh by an
/// orchestrating flintctl, and they take their parameters on the command line
/// rather than from an inventory — a data host has no reason to hold the
/// fleet's inventory file, and giving it one would create a second copy of the
/// truth to drift.
///
/// They deliberately call the SAME functions the local path calls. That is the
/// whole design: `wait_port_free` is only meaningful executed on the host
/// whose port it is, and a second implementation over ssh would be free to be
/// subtly different — which is precisely how rc.15 shipped a port check that
/// could never pass on Linux.
fn host_command(cmd: &str, a: &[String]) -> ! {
    let need = |i: usize, what: &str| -> String {
        a.get(i)
            .unwrap_or_else(|| die(&format!("{cmd}: missing {what}")))
            .clone()
    };
    match cmd {
        "host-spawn" => {
            let (statedir, bins, name, bin) = (
                need(0, "statedir"),
                need(1, "bins"),
                need(2, "name"),
                need(3, "bin"),
            );
            let mut envs = Vec::new();
            let mut args = Vec::new();
            let mut i = 4;
            while i < a.len() {
                match a[i].as_str() {
                    "--env" => {
                        if let Some((k, v)) = a.get(i + 1).and_then(|kv| kv.split_once('=')) {
                            envs.push((k.to_string(), v.to_string()));
                        }
                        i += 2;
                    }
                    "--" => {
                        args.extend(a[i + 1..].iter().cloned());
                        break;
                    }
                    _ => i += 1,
                }
            }
            for sub in ["logs", "pids", "snaps"] {
                let _ = std::fs::create_dir_all(format!("{statedir}/{sub}"));
            }
            local_spawn_env(&statedir, &bins, &name, &bin, &args, &envs);
            println!("pid recorded in {statedir}/pids/{name}.pid");
            std::process::exit(0)
        }
        "host-stop-seat" => {
            let port = a.get(4).and_then(|p| p.parse::<u16>().ok());
            match local_stop_seat(
                &need(0, "statedir"),
                &need(1, "name"),
                &need(2, "bin"),
                &need(3, "ident"),
                port,
            ) {
                Ok(()) => std::process::exit(0),
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(1)
                }
            }
        }
        "host-kill-pidfile" => {
            local_kill_pidfile(&need(0, "statedir"), &need(1, "name"));
            std::process::exit(0)
        }
        "host-stop-all" => {
            local_stop_all(&need(0, "statedir"));
            std::process::exit(0)
        }
        "host-mark-reseed" => {
            match local_mark_reseed(&need(0, "statedir"), &need(1, "name"), &need(2, "why")) {
                Ok(()) => std::process::exit(0),
                Err(e) => die(&e),
            }
        }
        "host-wipe-node" => match local_wipe_node(&need(0, "statedir"), &need(1, "name")) {
            Ok(()) => std::process::exit(0),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(1)
            }
        },
        "host-sweep" => {
            println!("{}", local_sweep_orphans(&need(0, "statedir")));
            std::process::exit(0)
        }
        "host-port-free" => {
            let port: u16 = need(0, "port")
                .parse()
                .unwrap_or_else(|_| die("host-port-free: port"));
            let ms: u64 = a.get(1).and_then(|m| m.parse().ok()).unwrap_or(15_000);
            match wait_port_free(port, Duration::from_millis(ms)) {
                Ok(()) => std::process::exit(0),
                Err(e) => {
                    eprintln!("{e}");
                    std::process::exit(1)
                }
            }
        }
        other => die(&format!("unknown host command {other:?}")),
    }
}

/// This flintctl's own build stamp — same definition every Flint binary uses.
fn build_version() -> String {
    flint_build::version(env!("CARGO_PKG_VERSION"))
}

/// Verbs that CHANGE a fleet. A binary that is not a release build may run
/// these only against an inventory that declares itself disposable.
///
/// The read-only verbs are deliberately absent: diagnosing a cluster with
/// whatever flintctl is to hand is exactly when `status` and `verify` matter,
/// and neither can break anything.
const MUTATING: &[&str] = &[
    "bootstrap",
    "start",
    "stop",
    "reload",
    "upgrade",
    "push-bins",
    "kill-node",
    "restart-node",
    "expand",
    "swap-node",
    "add-replica",
    "decommission-node",
    "migrate-slots",
    "failover",
    "retire-proxy",
    "rotate-admin",
    "rotate-certs",
    "proxy-cache",
    "tenant",
    "tenant-reads",
    "tenant-cache",
    "tenant-async",
    "tenant-federate",
    "tenant-quota",
];

/// The guard that makes a fast build channel safe to have at all.
///
/// The release bundle is the artifact contract — manifest sha256, public_sha,
/// format_break, `upgrade --version-tag`, the compiled-in stamp — and all of
/// it exists so that what is running is identifiable. A source-built binary
/// has none of that, which is fine for a cluster that exists for one chaos
/// run and is deleted afterwards, and not fine anywhere else. A convenient
/// path gets reused: without this, the same command that rolls a throwaway
/// fleet rolls the playground, and nothing says otherwise until afterwards.
///
/// Enforced HERE rather than in the script that generates the inventory,
/// because a guard you can step around by invoking the tool directly is a
/// convention, not a guard. Only possible because a binary can now say what
/// it is: before the release tag was compiled in, every build looked alike.
fn require_release_or_disposable(inv: &Inventory, cmd: &str) {
    if !MUTATING.contains(&cmd) {
        return;
    }
    let v = build_version();
    if flint_build::is_release(&v) || inv.disposable {
        return;
    }
    die(&format!(
        "refusing `{cmd}`: this flintctl reports build {v:?}, which is not a release, \
         and the inventory does not declare `disposable on`.\n\
         \n\
         A source or CI-artifact build carries no manifest, no sha256 and no \
         version anyone can check, so it may only mutate a fleet that exists to \
         be thrown away. Read-only commands (status, verify) are always allowed.\n\
         \n\
         Pick the one that matches what you are doing:\n\
         \n\
         * Trying Flint out, or running a fleet you will delete — add \
         `disposable on` to the inventory.\n\
         \n\
         * Self-hosting for real from source — stamp the build with the version \
         you are deploying:\n\
         \n      FLINT_RELEASE_TAG=v0.1.0 cargo build --release --features flint-server/rocks\n\
         \n  The tag is baked in at compile time, so every binary reports it \
         and `verify` can hold the whole fleet to one build.\n\
         \n\
         * Running a published release bundle — deploy it as-is; it carries the \
         tag, the manifest and the sha256s."
    ));
}

/// Deliberately a map, not a manual: every verb, one line each, and a pointer
/// to where the detail lives. The module header above is the full reference.
const USAGE: &str = "\
flintctl — drive a Flint cluster from one inventory file.

    flintctl -f <inventory> <command> [args...]
    flintctl --version | --help

Lifecycle    bootstrap  start  stop  status [--json]  verify [--probe <t>:<tok>]
Topology     expand  add-replica  swap-node  decommission-node  migrate-slots
Failure      failover <node>  kill-node <node>  restart-node <node>
Tenants      tenant add|remove  tenant-quota  tenant-reads  tenant-cache
             tenant-async  tenant-federate
Edge         retire-proxy  proxy-cache
Secrets      rotate-certs  rotate-admin
Releases     push-bins <tarball>  upgrade --version-tag <tag>
Config       reload            (edit the inventory; pushes hot knobs, no restart)

A minimal inventory:

    statedir ./state
    bins ./target/release
    tls on
    disposable on          # required for a source build; drop it for a release
    cp 127.0.0.1:7500
    pair 127.0.0.1:7001,127.0.0.1:7002
    proxy 127.0.0.1:7379
    controller on

Guide: docs/self-hosting.md   Failure model: docs/failover.md";

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    // Ask the binary what it is without starting anything. `--version`/`-V` are
    // aliases because that is what an operator types first, and answering it
    // with `panic!("usage: flintctl -f <inventory> ...")` reads as "this tool
    // is broken" on the very first command of an evaluation.
    if argv
        .iter()
        .any(|a| a == "--build-version" || a == "--version" || a == "-V")
    {
        println!("{}", build_version());
        return;
    }
    if argv.iter().any(|a| a == "--help" || a == "-h") || argv.len() == 1 {
        println!("{USAGE}");
        return;
    }
    // Host-side commands come with no inventory: dispatch before requiring -f.
    if let Some(cmd) = argv.get(1).filter(|c| c.starts_with("host-")) {
        host_command(&cmd.clone(), &argv[2..]);
    }
    let inv_path = argv
        .iter()
        .position(|a| a == "-f")
        .and_then(|i| argv.get(i + 1))
        .unwrap_or_else(|| {
            die("no inventory: flintctl -f <inventory> <command>  (--help for the map)")
        })
        .clone();
    let inv = parse_inventory(&inv_path);
    let cmd_at = argv
        .iter()
        .position(|a| a == "-f")
        .map(|i| i + 2)
        .unwrap_or(1);
    let cmd = argv.get(cmd_at).map(|s| s.as_str()).unwrap_or("status");
    let rest: Vec<String> = argv.iter().skip(cmd_at + 1).cloned().collect();
    require_release_or_disposable(&inv, cmd);

    match cmd {
        "bootstrap" => {
            bootstrap(&inv);
            verify_after(&inv, "bootstrap");
        }
        "start" => start(&inv),
        // Fault injection and its inverse. `kill-node` is the abrupt loss of
        // one seat — no drain, no demote — which is what a chaos run needs
        // and what a real crash looks like. `restart-node` brings the seat
        // back the only way an ex-member safely returns: wiped and re-seeded
        // from whoever is master NOW, which may not be who it was before.
        //
        // They live here rather than in the chaos harness because the
        // harness has no idea which machine a seat is on; flintctl does, and
        // routes both through the same Runner the rest of the fleet uses.
        "kill-node" => {
            let addr = rest.first().expect("usage: kill-node <addr>");
            let port = port_of(addr);
            let d = &inv.statedir;
            match stop_seat(
                &inv,
                &runner_for(&inv, addr),
                &format!("node-{port}"),
                "flint-server",
                &format!("{d}/node-{port}"),
                Some(port),
            ) {
                Ok(()) => println!("killed {addr}"),
                Err(e) => die(&format!("kill-node {addr}: {e}")),
            }
        }
        "restart-node" => {
            let addr = rest.first().expect("usage: restart-node <addr>");
            let tls = tls_client(&inv);
            let master = pair_master(&inv, &tls, addr);
            if &master == addr {
                die(&format!(
                    "{addr} is currently MASTER of its pair — restarting it as a replica of \
                     itself is meaningless; fail it over first"
                ));
            }
            match roll_node(&inv, addr, &master, &[], &None, true) {
                Ok(()) => println!("restarted {addr} as a replica of {master}"),
                Err(e) => die(&format!("restart-node {addr}: {e}")),
            }
        }
        "push-bins" => {
            let tarball = rest.first().expect("usage: push-bins <bundle.tar.gz>");
            push_bins(&inv, tarball);
        }
        "status" => {
            if rest.iter().any(|a| a == "--json") {
                status_json(&inv)
            } else {
                status(&inv)
            }
        }
        "retire-proxy" => {
            let addr = rest.first().expect("usage: retire-proxy <addr>");
            retire_proxy(&inv, addr);
        }
        "verify" => verify(&inv, &rest),
        "tenant" => match rest.first().map(|s| s.as_str()) {
            Some("add") => tenant_add(&inv, &rest[1..]),
            Some("remove") => {
                let name = rest.get(1).expect("usage: tenant remove <name>");
                tenant_remove(&inv, name);
            }
            _ => panic!("usage: tenant add <name> <token> <ns> [k] | tenant remove <name>"),
        },
        "expand" => {
            let spec = rest.first().expect("usage: expand <a,b[,c]>");
            expand(&inv, &inv_path, spec);
            // The inventory on disk grew; re-read it so the check sees the
            // pair that was just added rather than the old shape.
            verify_after(&parse_inventory(&inv_path), "expand");
        }
        "swap-node" => {
            let (bad, new) = (
                rest.first().expect("usage: swap-node <bad> <new>"),
                rest.get(1).expect("usage: swap-node <bad> <new>"),
            );
            swap_node(&inv, &inv_path, bad, new);
            verify_after(&parse_inventory(&inv_path), "swap-node");
        }
        "add-replica" => {
            let (pair, new) = (
                rest.first()
                    .expect("usage: add-replica <pair-idx|member> <new>"),
                rest.get(1)
                    .expect("usage: add-replica <pair-idx|member> <new>"),
            );
            add_replica(&inv, &inv_path, pair, new);
            verify_after(&parse_inventory(&inv_path), "add-replica");
        }
        "reload" => reload(&inv),
        "migrate-slots" => {
            let (ns, range, src, dest) = (
                rest.first()
                    .expect("usage: migrate-slots <ns> <lo-hi> <src> <dest>"),
                rest.get(1)
                    .expect("usage: migrate-slots <ns> <lo-hi> <src> <dest>"),
                rest.get(2)
                    .expect("usage: migrate-slots <ns> <lo-hi> <src> <dest>"),
                rest.get(3)
                    .expect("usage: migrate-slots <ns> <lo-hi> <src> <dest>"),
            );
            migrate_slots(&inv, ns, range, src, dest);
            verify_after(&inv, "migrate-slots");
        }
        "failover" => {
            let node = rest.first().expect("usage: failover <node-addr>");
            failover(&inv, node);
            verify_after(&inv, "failover");
        }
        "decommission-node" => {
            let addr = rest
                .first()
                .expect("usage: decommission-node <node-addr> [--force] [--drain-ms N]");
            let force = rest.iter().any(|a| a == "--force");
            let drain_ms = rest
                .iter()
                .position(|a| a == "--drain-ms")
                .and_then(|i| rest.get(i + 1))
                .and_then(|v| v.parse().ok())
                .unwrap_or(5000);
            decommission_node(&inv, &inv_path, addr, force, drain_ms);
            verify_after(&parse_inventory(&inv_path), "decommission-node");
        }
        "tenant-reads" => {
            let (name, mode) = (
                rest.first().expect("usage: tenant-reads <name> <on|off>"),
                rest.get(1).expect("usage: tenant-reads <name> <on|off>"),
            );
            let tls = tls_client(&inv);
            match call_cp(&inv, &tls, &["CPTENANTREADS", name, mode]) {
                Ok(Value::Simple(s)) => println!("{s}"),
                other => fail("tenant-reads failed", &other),
            }
        }
        // Async write-queue opt-in (ADR-0005 D4, the 'a' flag): the hot-key
        // write mitigation, operator-set here or tenant-set via the portal.
        "tenant-async" => {
            let (name, mode) = (
                rest.first().expect("usage: tenant-async <name> <on|off>"),
                rest.get(1).expect("usage: tenant-async <name> <on|off>"),
            );
            let tls = tls_client(&inv);
            match call_cp(&inv, &tls, &["CPTENANTASYNC", name, mode]) {
                Ok(Value::Simple(s)) => println!("{s}"),
                other => fail("tenant-async failed", &other),
            }
        }
        // Federation flag (ADR-0007, plumbing today): marks the tenant and
        // rides the snapshot; routing semantics arrive with the fleet map.
        "tenant-federate" => {
            let (name, mode) = (
                rest.first()
                    .expect("usage: tenant-federate <name> <on|off>"),
                rest.get(1).expect("usage: tenant-federate <name> <on|off>"),
            );
            let tls = tls_client(&inv);
            match call_cp(&inv, &tls, &["CPTENANTFEDERATE", name, mode]) {
                Ok(Value::Simple(s)) => println!("{s}"),
                other => fail("tenant-federate failed", &other),
            }
        }
        // Tenant quotas (M5): fleet ops/s + storage bytes; 0 = unlimited.
        "tenant-quota" => {
            let (name, ops, bytes) = (
                rest.first()
                    .expect("usage: tenant-quota <name> <ops_per_sec> <max_bytes>"),
                rest.get(1)
                    .expect("usage: tenant-quota <name> <ops_per_sec> <max_bytes>"),
                rest.get(2)
                    .expect("usage: tenant-quota <name> <ops_per_sec> <max_bytes>"),
            );
            let tls = tls_client(&inv);
            match call_cp(&inv, &tls, &["CPTENANTQUOTA", name, ops, bytes]) {
                Ok(Value::Simple(s)) => println!("{s}"),
                other => fail("tenant-quota failed", &other),
            }
        }
        // Push the near-cache knobs (PROXYCACHE <ttl_ms> <max_bytes>) to
        // EVERY proxy in the inventory — the per-proxy runtime setting,
        // fleet-applied. Presents the inventory admin token when the fleet
        // is gated.
        // Rotate the FLEET ADMIN token (ADR-0006 D4): the CP mints the
        // successor, keeps both valid, and the agent retires the old one
        // once it has rolled through the fleet. Prints the new token ONCE.
        // Re-sign the mesh/edge LEAF certs from the CA (ADR-0006 D4);
        // running components hot-reload with no restart. CA rotation is a
        // runbook, not this verb.
        "rotate-certs" => rotate_certs(&inv),
        "rotate-admin" => {
            let tls = tls_client(&inv);
            match call_cp(&inv, &tls, &["CPADMINROTATE"]) {
                Ok(Value::Bulk(Some(t))) => {
                    println!("{}", String::from_utf8_lossy(&t));
                    eprintln!(
                        "  new admin token minted; both old and new work until the agent retires the old one"
                    );
                }
                other => fail("rotate-admin failed", &other),
            }
        }
        "proxy-cache" => {
            let (ttl, maxb) = (
                rest.first()
                    .expect("usage: proxy-cache <ttl_ms> <max_bytes>"),
                rest.get(1)
                    .expect("usage: proxy-cache <ttl_ms> <max_bytes>"),
            );
            let tls = tls_client(&inv);
            for i in 0..inv.proxies.len() {
                let proxy = &proxy_dial(&inv, i);
                if let Some(tok) = &inv.admin_token {
                    match call(proxy, &tls, &["AUTH", tok]) {
                        Ok(Value::Simple(_)) => {}
                        other => fail(
                            &format!("proxy-cache: admin auth to {proxy} failed"),
                            &other,
                        ),
                    }
                }
                match call(proxy, &tls, &["PROXYCACHE", ttl, maxb]) {
                    Ok(Value::Simple(_)) => println!("{proxy}: cache ttl={ttl}ms max={maxb}B"),
                    other => fail(&format!("proxy-cache: {proxy} rejected"), &other),
                }
            }
        }
        // Proxy near-cache consent for a tenant (ADR-0005 D6). The cache's
        // TTL/size knobs are per-proxy runtime settings (PROXYCACHE); this
        // records the tenant's acceptance of TTL-bounded stale reads.
        "tenant-cache" => {
            let (name, mode) = (
                rest.first().expect("usage: tenant-cache <name> <on|off>"),
                rest.get(1).expect("usage: tenant-cache <name> <on|off>"),
            );
            let tls = tls_client(&inv);
            match call_cp(&inv, &tls, &["CPTENANTCACHE", name, mode]) {
                Ok(Value::Simple(s)) => println!("{s}"),
                other => fail("tenant-cache failed", &other),
            }
        }
        "upgrade" => {
            let tag = rest
                .iter()
                .position(|a| a == "--version-tag")
                .and_then(|i| rest.get(i + 1))
                .cloned();
            let soak: u64 = rest
                .iter()
                .position(|a| a == "--soak-ms")
                .and_then(|i| rest.get(i + 1))
                .and_then(|v| v.parse().ok())
                .unwrap_or(3000);
            // Release-manifest guard: the canary path's safety net is that
            // any node can roll back or forward freely, which an on-disk
            // FORMAT BREAK destroys. A manifest declaring one refuses the
            // fast path unless the operator explicitly acknowledges the
            // migration-release runbook.
            if let Some(mf) = rest
                .iter()
                .position(|a| a == "--manifest")
                .and_then(|i| rest.get(i + 1))
            {
                let body = std::fs::read_to_string(mf)
                    .unwrap_or_else(|e| panic!("cannot read manifest {mf}: {e}"));
                let breaks = body.split("\"format_break\"").nth(1).is_some_and(|rest| {
                    rest.trim_start()
                        .trim_start_matches(':')
                        .trim_start()
                        .starts_with("true")
                });
                if breaks && !rest.iter().any(|a| a == "--allow-format-break") {
                    panic!(
                        "REFUSED: manifest {mf} declares format_break=true — this release \
                         cannot roll back and must ship via the migration runbook, not the \
                         canary fast path. Re-run with --allow-format-break only if you are \
                         executing that runbook."
                    );
                }
            }
            let nodes_only = rest.iter().any(|a| a == "--nodes-only");
            upgrade(&inv, tag, soak, nodes_only);
        }
        "stop" => stop(&inv),
        other => {
            panic!(
                "unknown command {other:?} (bootstrap|start|status|reload|tenant|tenant-reads|tenant-cache|tenant-async|tenant-federate|tenant-quota|rotate-admin|rotate-certs|proxy-cache|expand|swap-node|add-replica|migrate-slots|failover|decommission-node|upgrade|stop)"
            )
        }
    }
}

#[cfg(test)]
mod port_free_tests {
    use super::*;
    use std::net::TcpListener;

    /// The case that actually broke: a FREE port must be reported free.
    ///
    /// The first version probed 0.0.0.0:P and 127.0.0.1:P while still holding
    /// the first listener. On Linux those overlap, so the second bind failed
    /// and the check could never pass — every roll would abort its canary
    /// after killing the seat. macOS permits the overlap, so all the shell
    /// drills (which only ever run on the dev laptop) went green.
    ///
    /// This test lives here because `cargo test` runs on LINUX in the release
    /// pipeline. That is the only gate in this repo that sees the platform the
    /// fleet actually runs on.
    #[test]
    fn a_free_port_is_reported_free() {
        let port = {
            let probe = TcpListener::bind(("127.0.0.1", 0)).expect("ephemeral port");
            probe.local_addr().expect("local addr").port()
            // probe dropped here: the port is now genuinely free
        };
        assert!(
            wait_port_free(port, Duration::from_secs(2)).is_ok(),
            "a free port must be reported free — if this fails on Linux and passes on macOS, \
             the check is binding overlapping addresses and conflicting with itself"
        );
    }

    /// And a HELD port must not be, or the check is decorative.
    #[test]
    fn a_held_port_is_not_reported_free() {
        let held = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
        let port = held.local_addr().expect("local addr").port();
        let err = wait_port_free(port, Duration::from_millis(400))
            .expect_err("a port with a live listener must not be reported free");
        assert!(
            err.contains(&port.to_string()),
            "the error should name the port: {err}"
        );
        drop(held);
        assert!(
            wait_port_free(port, Duration::from_secs(2)).is_ok(),
            "once released, the port must be reported free"
        );
    }
}

#[cfg(test)]
mod lease_ttl_single_source_tests {
    use super::*;

    /// The documentation is the last remaining copy of DEFAULT_LEASE_TTL_MS,
    /// so check it mechanically rather than by remembering.
    ///
    /// #182: when ADR-0018 moved the default from 3000 to 5000, three test
    /// rigs kept their own hardcoded values (the AWS chaos/soak fleet at
    /// 4000, two drills at 3000) and nothing noticed for weeks — every chaos
    /// result was quietly measured on a fleet fenced tighter than any
    /// customer's. The rigs no longer carry the number at all; an inventory
    /// that omits `lease-ttl-ms` inherits the constant. These two strings are
    /// what is left, and a stale doc is how the next person re-learns the
    /// wrong number.
    #[test]
    fn the_documented_lease_ttl_is_the_one_we_ship() {
        let want = format!("lease-ttl-ms {DEFAULT_LEASE_TTL_MS}");
        for (what, text) in [
            (
                "docs/self-hosting.md",
                include_str!("../../../docs/self-hosting.md"),
            ),
            ("this crate's usage header", include_str!("main.rs")),
        ] {
            assert!(
                text.contains(&want),
                "{what} does not say `{want}` — DEFAULT_LEASE_TTL_MS moved and \
                 the documentation did not follow it"
            );
        }
    }
}

#[cfg(test)]
mod proxy_liveness_tests {
    use super::*;

    /// The exact string `call_seq_on` produces for a -NOAUTH reply, taken
    /// from a live admin-gated proxy rather than invented: it answers
    /// "NOAUTH admin token required for this command", and call_seq_on
    /// wraps that verbatim into an io::Error.
    ///
    /// This test is the positive control the branch never had. proxy_up
    /// matched `Ok(Value::Error(..))` for two releases while call_seq_on
    /// could only ever return `Err`, so the arm was dead and every
    /// admin-token fleet read its own proxy as down. Nothing failed at
    /// compile time and no test noticed.
    #[test]
    fn a_noauth_refusal_is_recognised_as_a_serving_proxy() {
        assert!(is_noauth_error(
            "NOAUTH admin token required for this command"
        ));
        assert!(is_noauth_error("-NOAUTH admin token required"));
        // Nothing else may pass. A connection refused or a timeout means
        // the proxy is genuinely unreachable, and treating either as "up"
        // would invert the check this protects.
        assert!(!is_noauth_error("Connection refused (os error 61)"));
        assert!(!is_noauth_error("timed out"));
        assert!(!is_noauth_error("WRONGPASS invalid token"));
        assert!(!is_noauth_error(""));
    }
}
