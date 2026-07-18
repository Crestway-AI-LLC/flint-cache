//! flint-proxy (v0): the routing plane's front door (docs/design.md §2.1).
//!
//! Clients get ONE plain-RESP endpoint and need no cluster awareness, ever.
//! The proxy owns what clients must never see:
//!   - slot routing: key -> CRC16 slot -> the master that owns it;
//!   - `-MOVED` absorption: a migrated slot's redirect updates the shared
//!     routing cache and the command is retried at the new owner — the
//!     client sees only the final reply (the -MOVED override cache IS the
//!     level-2 routing table, learned from the nodes' own manifests);
//!   - `-TRYAGAIN` absorption: a write hitting a slot frozen mid-cutover is
//!     retried within a bounded budget (~the design's queue-during-failover
//!     window) instead of surfacing;
//!   - failover chasing: a dead backend triggers master rediscovery on that
//!     pair (FLINTINFO role probe — same discovery the controller uses).
//!
//! Default placement is range-based — pair i owns slots [i*16384/N ..
//! (i+1)*16384/N) — matching the control plane's future range maps; the
//! moved-slot cache overrides per slot. A proxy restart loses only cache:
//! the first request routes to the default owner and relearns from -MOVED.
//!
//! Tenancy: --tenants "token=ns,..." enables token auth. Clients AUTH
//! <token> (or AUTH <user> <token>); the proxy maps the token to the
//! tenant's namespace and pins every backend connection to it with a
//! FLINTNS handshake, so all data commands, DBSIZE, and FLUSHALL are
//! tenant-scoped on the nodes. Pre-auth commands get -NOAUTH; bad tokens
//! get -WRONGPASS. Without --tenants the proxy runs open on the default
//! namespace ("0") — the pre-tenancy behavior, unchanged. The moved-slot
//! cache is keyed (ns, slot): migrations are per-namespace, so tenant A's
//! -MOVED must never reroute tenant B, whose rows did not move.
//!
//! v0 scope, deliberately deferred: TLS, metering, cross-slot
//! scatter-gather (multi-key commands route by FIRST key), hot-key
//! absorption, RESP3, inline commands. FLINT* admin commands are REJECTED
//! at the proxy: the data-plane admin surface is internal, and the proxy is
//! the tenant boundary.
//!
//! Usage: flint-proxy --port 7379 --pairs "m0,r0;m1,r1;..."
//!                    [--tenants "tokenA=nsA,tokenB=nsB"]

mod cache;
mod latency;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use flint_resp::{Decoded, Value, decode, encode};
use flint_slot::slot_for_key;

/// Total retry budget for one client command across MOVED chases, TRYAGAIN
/// waits, and failover rediscovery — the proxy's answer to "latency spike,
/// not errors" during topology changes.
const RETRY_BUDGET: Duration = Duration::from_secs(5);
/// Backend I/O timeout. Generous: a frozen-slot drain or a slow disk read
/// must not be misread as a dead node.
const BACKEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Default cap on concurrent client connections — proxy admission control.
/// Thread-per-connection means each connection costs a thread; without a
/// bound, a connection storm (or a slow/widowed backend holding threads)
/// exhausts the proxy tier. Generous vs any real ops footprint; tunable with
/// `--max-conns`. Beyond it, new connections are shed (not silently dropped)
/// with a -THROTTLED the client already knows to back off on.
const DEFAULT_MAX_CONNS: usize = 1024;

/// Written to a connection shed at the admission cap. Reuses the -THROTTLED
/// contract (client: retry with backoff) the data plane uses for durability
/// shedding, so the client needs no new vocabulary for "busy, back off".
const SHED_FRAME: &[u8] = b"-THROTTLED proxy at connection capacity, retry with backoff\r\n";

/// Decrements the live-connection counter when a worker exits — by any path,
/// including a panic — so a crashing handler can never leak a slot.
struct ConnGuard(Arc<Topology>);
impl Drop for ConnGuard {
    fn drop(&mut self) {
        self.0.stat_active.fetch_sub(1, Ordering::Relaxed);
    }
}

fn arg(name: &str) -> Option<String> {
    std::env::args().skip_while(|a| a != name).nth(1)
}

/// Top-K hot-key sketch (ADR-0005 D5 — OBSERVABILITY ONLY: it informs
/// PROXYHOTKEYS, the exporter, and the agent; it never toggles behavior).
/// Space-saving: a full sketch evicts the minimum-total entry, and the
/// newcomer inherits its count (the classic overestimate bound). Counts
/// decay by half every DECAY_HALF_LIFE so the sketch tracks the RECENT
/// window, not all time. Callers SAMPLE updates (1 in SAMPLE_RATE) so the
/// mutex is off the common path; reported counts are therefore
/// approximate-by-design, which is all a top-K needs.
/// Per hot key: a DECAYED score for top-K ranking ("hot now"), plus a
/// MONOTONIC total for rate — the agent diffs the total across sweeps to
/// report req/s on the dashboard. Score decays; totals never do.
#[derive(Default, Clone, Copy)]
struct HotEntry {
    r_score: u64,
    w_score: u64,
    r_total: u64,
    w_total: u64,
}
/// (ns, key) -> HotEntry.
type HotKeyMap = HashMap<(Vec<u8>, Vec<u8>), HotEntry>;

/// A tenant's grant, decoded from the snapshot's "token=ns#flags[@rate]"
/// entry: opt-ins (D7 replica reads, D6 near-cache), the M5 quota state
/// ('q' = over storage quota; rate = this proxy's ops/s share), and the
/// namespace everything is scoped to.
#[derive(Clone, Default)]
struct TenantGrant {
    ns: Vec<u8>,
    replica_reads: bool,
    local_cache: bool,
    /// Over storage quota: writes shed with -QUOTA, reads served.
    over_quota: bool,
    /// Per-proxy ops/s share (token bucket); 0 = unlimited.
    rate: u64,
}

/// Per-namespace token bucket state (tokens, last refill).
type BucketMap = HashMap<Vec<u8>, (f64, Instant)>;

struct HotKeySketch {
    /// Capped at K entries (space-saving).
    entries: std::sync::Mutex<HotKeyMap>,
    last_decay: std::sync::Mutex<Instant>,
}

const HOTKEY_K: usize = 64;
/// 1-in-N sampling: only every Nth keyed command takes the sketch mutex.
/// The sketch is observability-only, so a sparse sample is plenty to surface
/// a DOMINATING key — sampled counts scale back up by this factor, so the
/// threshold stays in real-hit units. Power of two: the `is_multiple_of`
/// tick stays exact even across the u32 counter's wrap.
const HOTKEY_SAMPLE_RATE: u32 = 16;
const HOTKEY_DECAY_HALF_LIFE: Duration = Duration::from_secs(10);

impl HotKeySketch {
    fn new() -> Self {
        Self {
            entries: std::sync::Mutex::new(HashMap::new()),
            last_decay: std::sync::Mutex::new(Instant::now()),
        }
    }

    fn observe(&self, ns: &[u8], key: &[u8], is_write: bool) {
        self.maybe_decay();
        let Ok(mut map) = self.entries.lock() else {
            return;
        };
        let add = HOTKEY_SAMPLE_RATE as u64;
        let id = (ns.to_vec(), key.to_vec());
        if let Some(e) = map.get_mut(&id) {
            if is_write {
                e.w_score += add;
                e.w_total += add;
            } else {
                e.r_score += add;
                e.r_total += add;
            }
            return;
        }
        // Space-saving eviction: displace the minimum-SCORE entry; the
        // newcomer inherits its score (upper-bounds its true recent count).
        // Totals start fresh — a re-admitted key's rate resets, which the
        // agent's counter-reset handling absorbs.
        let inherited = if map.len() >= HOTKEY_K {
            let min = map
                .iter()
                .min_by_key(|(_, e)| e.r_score + e.w_score)
                .map(|(k, e)| (k.clone(), e.r_score + e.w_score));
            match min {
                Some((k, c)) => {
                    map.remove(&k);
                    c
                }
                None => 0,
            }
        } else {
            0
        };
        let mut e = HotEntry::default();
        if is_write {
            e.w_score = inherited + add;
            e.w_total = add;
        } else {
            e.r_score = inherited + add;
            e.r_total = add;
        }
        map.insert(id, e);
    }

    fn maybe_decay(&self) {
        let Ok(mut last) = self.last_decay.lock() else {
            return;
        };
        if last.elapsed() < HOTKEY_DECAY_HALF_LIFE {
            return;
        }
        *last = Instant::now();
        drop(last);
        if let Ok(mut map) = self.entries.lock() {
            // Halve only the ranking SCORE (recent-window semantics); totals
            // stay monotonic for the rate. Drop a key once its score reaches
            // zero — it went cold, so it stops being reported.
            map.retain(|_, e| {
                e.r_score /= 2;
                e.w_score /= 2;
                e.r_score + e.w_score > 0
            });
        }
    }

    /// Top entries by recent SCORE, optionally filtered to one namespace (the
    /// tenant-scoped view — key names are DATA). Returns the monotonic TOTALS
    /// (the agent computes req/s from their deltas), not the score.
    fn top(&self, ns_filter: Option<&[u8]>, n: usize) -> Vec<(Vec<u8>, Vec<u8>, u64, u64)> {
        let Ok(map) = self.entries.lock() else {
            return Vec::new();
        };
        let mut rows: Vec<_> = map
            .iter()
            .filter(|((ns, _), _)| ns_filter.is_none_or(|f| ns.as_slice() == f))
            .map(|((ns, k), e)| {
                (
                    ns.clone(),
                    k.clone(),
                    e.r_score + e.w_score,
                    e.r_total,
                    e.w_total,
                )
            })
            .collect();
        // Rank by recent score; report totals.
        rows.sort_by_key(|(_, _, score, _, _)| std::cmp::Reverse(*score));
        rows.truncate(n);
        rows.into_iter()
            .map(|(ns, k, _, rt, wt)| (ns, k, rt, wt))
            .collect()
    }
}

/// The routing table proper: pairs and their current masters, replaced
/// atomically when the control plane pushes a new topology.
#[derive(Default)]
struct Routing {
    /// Node addresses of each pair; index = pair id.
    pairs: Vec<Vec<String>>,
    /// Current master address per pair (None until discovered).
    masters: Vec<Option<String>>,
    /// Slot range owned by pairs[i], pushed by the control plane (level-1
    /// routing state). Empty/None entries fall back to count-derived ranges
    /// (static --pairs mode, legacy registries). An expansion pair arrives
    /// with NO range: capacity joins without re-routing unmigrated slots.
    ranges: Vec<Option<(u16, u16)>>,
}

/// Shared cluster view: the routing table (static from --pairs, or pushed
/// by the control plane), the moved-slot override cache, and the tenant
/// table (static from --tenants, or pushed — filtered to THIS proxy's
/// assigned tenants, the sub-group boundary).
struct Topology {
    routing: RwLock<Routing>,
    /// (namespace, slot) -> owner address, learned from -MOVED redirects.
    /// Keyed per namespace because migrations move one tenant's slot rows:
    /// tenant A's redirect must not reroute tenant B. Overrides the range
    /// default; lost entries are relearned from the default owner.
    moved: RwLock<HashMap<(Vec<u8>, u16), String>>,
    /// token -> (namespace, replica_reads opt-in, local_cache opt-in).
    /// replica_reads (ADR-0005 D7) lets this tenant's READS fan across the
    /// owning pair's replicas; local_cache (ADR-0005 D6) lets this proxy
    /// answer the tenant's GETs from its short-TTL local cache.
    tenants: RwLock<HashMap<String, TenantGrant>>,
    /// Round-robin cursor for replica selection (D7).
    replica_rr: AtomicUsize,
    /// token -> successful-AUTH count. During a dual-version rotation the
    /// operator watches the PREVIOUS token's count go flat before dropping
    /// it (CPDROPPREV) — the per-version usage metric.
    auth_counts: RwLock<HashMap<String, u64>>,
    /// Fleet-level ops counters (PROXYSTATS -> the exporter). Relaxed
    /// atomics: monotonic telemetry, not coordination.
    stat_conns_total: std::sync::atomic::AtomicU64,
    stat_shed_total: std::sync::atomic::AtomicU64,
    stat_auth_ok_total: std::sync::atomic::AtomicU64,
    stat_auth_fail_total: std::sync::atomic::AtomicU64,
    stat_commands_total: std::sync::atomic::AtomicU64,
    /// Read/write traffic split (shared classifier, ADR-0005 D1/D5): the
    /// per-plane view Grafana and hot-key analysis start from.
    stat_commands_read_total: std::sync::atomic::AtomicU64,
    stat_commands_write_total: std::sync::atomic::AtomicU64,
    /// Live client connections (the admission-control counter).
    stat_active: AtomicUsize,
    /// Hot-key sketch (D5, observability only).
    hotkeys: HotKeySketch,
    /// Per-tenant latency histograms (M4 client-metrics substitute):
    /// tenant-perceived latency measured one hop from the client.
    latency: latency::LatencyHistograms,
    /// M5 quota state by NAMESPACE (rate share + over-quota verdict),
    /// rebuilt on every snapshot so a mid-connection verdict flip applies
    /// without re-auth. Buckets carry the running token count.
    quota: RwLock<HashMap<Vec<u8>, (u64, bool)>>,
    buckets: std::sync::Mutex<BucketMap>,
    /// Quota shed counters (PROXYSTATS -> exporter/billing).
    stat_quota_throttled_total: std::sync::atomic::AtomicU64,
    stat_quota_write_shed_total: std::sync::atomic::AtomicU64,
    /// Proxy-local read cache (D6): short-TTL, bounded bytes, per-tenant
    /// opt-in. Runtime-configured via PROXYCACHE.
    cache: cache::ProxyCache,
    /// True only for a standalone proxy with no tenants configured: no
    /// auth, default namespace. A control-plane-fed proxy is never open —
    /// before its first snapshot it simply has no tenants yet.
    open_mode: bool,
    /// Client-side mutual-TLS config for dialing backends (the internal hop).
    /// `None` = plaintext backends (default). Set by `--internal-*`; the same
    /// triple the servers use, in the client role.
    backend_tls: Option<Arc<rustls::ClientConfig>>,
    /// This proxy's own mesh leaf cert path (--internal-cert), for the
    /// cert-expiry gauge in PROXYSTATS. None => plaintext mesh.
    cert_path: Option<String>,
    /// Admin-token DIGESTS (ADR-0006 D1/D4): current + optionally previous
    /// during a rotation window. Non-empty => the operator surface
    /// (PROXYSTATS, all-tenant hot-key/latency, PROXYAUTHCOUNT, mutating
    /// PROXYCACHE) requires AUTH <admin-token>. Set from a static
    /// --admin-token (hashed at parse) OR pushed by the CP snapshot, so it
    /// rotates without a proxy restart. Empty => open surface (dev).
    admin_digests: RwLock<Vec<String>>,
}

impl Topology {
    /// The address a command for `slot` in `ns` should go to right now.
    /// Range-based default owner (pair i serves slots [i*N/16384 ..)),
    /// overridden per (ns, slot) by the moved cache.
    fn route(&self, ns: &[u8], slot: u16) -> Option<String> {
        if let Ok(moved) = self.moved.read()
            && let Some(addr) = moved.get(&(ns.to_vec(), slot))
        {
            return Some(addr.clone());
        }
        let routing = self.routing.read().ok()?;
        if routing.pairs.is_empty() {
            return None;
        }
        // Range-owned slot -> that pair. Otherwise (no ranges pushed, or an
        // uncovered slot) fall back to the count-derived split — but only
        // across the RANGED prefix when ranges exist, so an unranged
        // expansion pair never absorbs slots by mere existence.
        let ranged = routing.ranges.iter().filter(|r| r.is_some()).count();
        let pair = routing
            .ranges
            .iter()
            .position(|r| matches!(r, Some((a, b)) if (*a..=*b).contains(&slot)))
            .unwrap_or_else(|| {
                let n = if ranged > 0 {
                    ranged
                } else {
                    routing.pairs.len()
                };
                (slot as usize * n) / 16384
            });
        routing.masters.get(pair)?.clone()
    }

    fn learn_moved(&self, ns: &[u8], slot: u16, addr: &str) {
        if let Ok(mut moved) = self.moved.write() {
            moved.insert((ns.to_vec(), slot), addr.to_string());
        }
    }

    /// The tenant table is keyed by token DIGEST (ADR-0006 D1): hash the
    /// presented token, then look up. A leaked table entry cannot AUTH.
    fn lookup_token(&self, token: &str) -> Option<TenantGrant> {
        let digest = flint_tls::sha256_hex(token.as_bytes());
        self.tenants.read().ok()?.get(&digest).cloned()
    }

    /// A replica of the pair that currently owns `(ns, slot)`, chosen
    /// round-robin (D7). None -> no replica known: the caller falls back to
    /// the master. Follows ownership via the master so a migrated slot reads
    /// from the new owner's replicas, not the old pair's.
    fn route_replica(&self, ns: &[u8], slot: u16) -> Option<String> {
        let master = self.route(ns, slot)?;
        let routing = self.routing.read().ok()?;
        let members = routing.pairs.iter().find(|nodes| nodes.contains(&master))?;
        let replicas: Vec<&String> = members.iter().filter(|n| **n != master).collect();
        if replicas.is_empty() {
            return None;
        }
        let idx = self.replica_rr.fetch_add(1, Ordering::Relaxed) % replicas.len();
        Some(replicas[idx].clone())
    }

    /// Per-token usage counters, keyed by DIGEST (rotation drain checks).
    fn bump_auth(&self, token: &str) {
        let digest = flint_tls::sha256_hex(token.as_bytes());
        if let Ok(mut c) = self.auth_counts.write() {
            *c.entry(digest).or_insert(0) += 1;
        }
    }

    /// Drain-check lookup: accepts the PLAINTEXT token (hashed here) or the
    /// digest directly (what digest-only callers like the rotation loop
    /// hold — plaintext is the tenant's alone).
    fn auth_count(&self, token: &str) -> u64 {
        let counts = match self.auth_counts.read() {
            Ok(c) => c,
            Err(_) => return 0,
        };
        let digest = flint_tls::sha256_hex(token.as_bytes());
        counts
            .get(&digest)
            .or_else(|| counts.get(token))
            .copied()
            .unwrap_or(0)
    }

    /// A backend at `addr` failed: forget cached state that points at it and
    /// rediscover the master of every pair that lists it. (Moved entries are
    /// kept — the new master of that pair holds the moved slots too; only
    /// the address may change, which the next -MOVED corrects.)
    fn rediscover_for(&self, addr: &str) {
        let pairs: Vec<(usize, Vec<String>)> = match self.routing.read() {
            Ok(r) => r
                .pairs
                .iter()
                .enumerate()
                .filter(|(i, nodes)| {
                    nodes.iter().any(|n| n == addr)
                        || r.masters
                            .get(*i)
                            .and_then(|m| m.as_ref())
                            .is_some_and(|m| m == addr)
                })
                .map(|(i, nodes)| (i, nodes.clone()))
                .collect(),
            Err(_) => return,
        };
        for (i, nodes) in pairs {
            let found = discover_master(&nodes, &self.backend_tls);
            if let Ok(mut routing) = self.routing.write()
                && let Some(slot) = routing.masters.get_mut(i)
            {
                *slot = found.clone();
            }
            // Moved entries pointing at the dead address migrate to the
            // pair's new master (slot data survives failover via the
            // pair's replica).
            if let Some(new_master) = found
                && new_master != addr
                && let Ok(mut moved) = self.moved.write()
            {
                for v in moved.values_mut() {
                    if v == addr {
                        *v = new_master.clone();
                    }
                }
            }
        }
    }

    /// Apply a control-plane snapshot: replace the tenant table, and — only
    /// if the pair list actually changed — rebuild the routing table
    /// (discovering masters), preserving failover-chased masters otherwise.
    fn apply_snapshot(&self, pairs_spec: &str, tenants_spec: &str, admin_spec: &str) {
        // "curdigest,prevdigest" (either "-"): the operator-surface gate,
        // pushed so admin-token rotation needs no proxy restart. An EMPTY
        // spec (pre-D4 CP) leaves any static --admin-token digest in place.
        if !admin_spec.is_empty() {
            let digests: Vec<String> = admin_spec
                .split(',')
                .filter(|d| !d.is_empty() && *d != "-")
                .map(String::from)
                .collect();
            if let Ok(mut a) = self.admin_digests.write() {
                *a = digests;
            }
        }
        // Pair entry: "a,b" (unranged) or "a,b|start-end" (range-owned).
        let mut new_pairs: Vec<Vec<String>> = Vec::new();
        let mut new_ranges: Vec<Option<(u16, u16)>> = Vec::new();
        if !pairs_spec.is_empty() {
            for entry in pairs_spec.split(';') {
                let (nodes, range) = match entry.split_once('|') {
                    Some((n, r)) => {
                        let parsed = r.split_once('-').and_then(|(a, b)| {
                            Some((a.parse::<u16>().ok()?, b.parse::<u16>().ok()?))
                        });
                        (n, parsed)
                    }
                    None => (entry, None),
                };
                new_pairs.push(nodes.split(',').map(String::from).collect());
                new_ranges.push(range);
            }
        }
        let rebuild = match self.routing.read() {
            Ok(r) => r.pairs != new_pairs || r.ranges != new_ranges,
            Err(_) => return,
        };
        if rebuild {
            let masters: Vec<Option<String>> = new_pairs
                .iter()
                .map(|n| discover_master(n, &self.backend_tls))
                .collect();
            if let Ok(mut routing) = self.routing.write() {
                routing.pairs = new_pairs;
                routing.masters = masters;
                routing.ranges = new_ranges;
            }
        }
        let new_tenants: HashMap<String, TenantGrant> = tenants_spec
            .split(',')
            .filter_map(|pair| {
                let (token, ns_and_flags) = pair.split_once('=')?;
                // "ns" or "ns#<flags>[@<rate>]": 'r' = replica reads (D7),
                // 'c' = near-cache (D6), 'q' = over storage quota (M5);
                // rate = this proxy's ops/s share. Producer: CP tenant.rs.
                let (ns, flags, rate) = match ns_and_flags.split_once('#') {
                    Some((ns, rest)) => match rest.split_once('@') {
                        Some((flags, rate)) => (ns, flags, rate.parse().unwrap_or(0)),
                        None => (ns, rest, 0),
                    },
                    None => (ns_and_flags, "", 0),
                };
                Some((
                    token.to_string(),
                    TenantGrant {
                        ns: ns.as_bytes().to_vec(),
                        replica_reads: flags.contains('r'),
                        local_cache: flags.contains('c'),
                        over_quota: flags.contains('q'),
                        rate,
                    },
                ))
            })
            .collect();
        let keep: std::collections::HashSet<Vec<u8>> =
            new_tenants.values().map(|g| g.ns.clone()).collect();
        // Namespace-keyed quota view: enforcement consults THIS on every
        // command, so a pushed verdict/rate change applies immediately to
        // live connections (no re-auth).
        if let Ok(mut quota) = self.quota.write() {
            *quota = new_tenants
                .values()
                .map(|g| (g.ns.clone(), (g.rate, g.over_quota)))
                .collect();
        }
        if let Ok(mut tenants) = self.tenants.write() {
            *tenants = new_tenants;
        }
        // A removed tenant's latency lanes and bucket must not live forever.
        self.latency.retain(&keep);
        if let Ok(mut buckets) = self.buckets.lock() {
            buckets.retain(|ns, _| keep.contains(ns));
        }
    }

    /// M5 quota gate for one data command. Order: the storage verdict (a
    /// write against a full tenant is shed with -QUOTA no matter the rate),
    /// then the ops/s token bucket (burst = one second of rate). Returns
    /// the error reply to send, or None to proceed. Tenants with no quota
    /// row (or the static/open modes) pay one read-lock lookup and pass.
    fn quota_gate(&self, ns: &[u8], name: &[u8], is_write: bool) -> Option<Value> {
        let (rate, over) = *self.quota.read().ok()?.get(ns)?;
        // Space-REDUCING writes stay allowed over-quota — the self-clear
        // path is the tenant deleting data, which must never be blocked by
        // the very state it cures.
        let reduces_space = matches!(
            name.to_ascii_uppercase().as_slice(),
            b"DEL" | b"UNLINK" | b"FLUSHALL" | b"EXPIRE" | b"PEXPIRE"
        );
        if over && is_write && !reduces_space {
            self.stat_quota_write_shed_total
                .fetch_add(1, Ordering::Relaxed);
            return Some(Value::Error(
                "QUOTA storage quota exceeded; writes rejected until usage drops (reads still served)"
                    .into(),
            ));
        }
        if rate == 0 {
            return None;
        }
        let mut buckets = self.buckets.lock().ok()?;
        let now = Instant::now();
        let (tokens, last) = buckets.entry(ns.to_vec()).or_insert((rate as f64, now));
        *tokens = (*tokens + last.elapsed().as_secs_f64() * rate as f64).min(rate as f64);
        *last = now;
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            None
        } else {
            self.stat_quota_throttled_total
                .fetch_add(1, Ordering::Relaxed);
            Some(Value::Error(
                "THROTTLED ops/s quota exceeded, retry with backoff".into(),
            ))
        }
    }
}

/// One RESP request/response exchange on a raw connection.
fn call_raw(
    stream: &mut flint_tls::Stream,
    buf: &mut Vec<u8>,
    frame: &[u8],
) -> std::io::Result<Value> {
    stream.write_all(frame)?;
    let mut chunk = [0u8; 64 * 1024];
    loop {
        match decode(buf) {
            Ok(Decoded::Complete(v, used)) => {
                buf.drain(..used);
                return Ok(v);
            }
            Ok(Decoded::NeedMore) => {
                let n = stream.read(&mut chunk)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "backend closed",
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

/// Probe a pair's nodes for the current master (FLINTINFO role) — the same
/// discovery rule the controller uses.
fn discover_master(nodes: &[String], tls: &Option<Arc<rustls::ClientConfig>>) -> Option<String> {
    for addr in nodes {
        let Ok(mut stream) = flint_tls::connect(addr, tls) else {
            continue;
        };
        let _ = stream.set_read_timeout(Some(Duration::from_millis(800)));
        let mut out = Vec::new();
        encode(
            &Value::Array(Some(vec![Value::Bulk(Some(b"FLINTINFO".to_vec()))])),
            &mut out,
        );
        let mut buf = Vec::new();
        if let Ok(Value::Bulk(Some(raw))) = call_raw(&mut stream, &mut buf, &out)
            && String::from_utf8_lossy(&raw)
                .split(['\r', '\n'])
                .any(|l| l == "role:master")
        {
            return Some(addr.clone());
        }
    }
    None
}

/// Per-client-thread cache of backend connections.
struct Backends {
    conns: HashMap<String, (flint_tls::Stream, Vec<u8>)>,
    /// Tenant namespace every connection here is pinned to (FLINTNS
    /// handshake at open). One client = one tenant, so raw frames forward
    /// without per-command rewriting.
    ns: Vec<u8>,
    /// Client-side mutual-TLS for the backend hop (`None` = plaintext).
    tls: Option<Arc<rustls::ClientConfig>>,
}

impl Backends {
    fn new(ns: Vec<u8>, tls: Option<Arc<rustls::ClientConfig>>) -> Self {
        Self {
            conns: HashMap::new(),
            ns,
            tls,
        }
    }

    /// Discard the cached connection to `addr` (stale-routing recovery: the
    /// next call dials whatever the refreshed masters map says).
    fn drop_conn(&mut self, addr: &str) {
        self.conns.remove(addr);
    }

    fn call(&mut self, addr: &str, frame: &[u8]) -> std::io::Result<Value> {
        if !self.conns.contains_key(addr) {
            let mut stream = flint_tls::connect(addr, &self.tls)?;
            stream.set_read_timeout(Some(BACKEND_TIMEOUT))?;
            stream.set_write_timeout(Some(BACKEND_TIMEOUT))?;
            // Pin the connection to the tenant namespace before any data
            // command can travel on it.
            let mut hs = Vec::new();
            encode(
                &Value::Array(Some(vec![
                    Value::Bulk(Some(b"FLINTNS".to_vec())),
                    Value::Bulk(Some(self.ns.clone())),
                ])),
                &mut hs,
            );
            let mut hs_buf = Vec::new();
            match call_raw(&mut stream, &mut hs_buf, &hs)? {
                Value::Simple(_) => {}
                other => {
                    return Err(std::io::Error::other(format!(
                        "namespace handshake rejected: {other:?}"
                    )));
                }
            }
            self.conns.insert(addr.to_string(), (stream, Vec::new()));
        }
        let Some((stream, buf)) = self.conns.get_mut(addr) else {
            return Err(std::io::Error::other("conn cache"));
        };
        let result = call_raw(stream, buf, frame);
        if result.is_err() {
            // A failed connection is never reused: the reply stream may be
            // desynchronized (half-read reply), which would corrupt the next
            // exchange.
            self.conns.remove(addr);
        }
        result
    }
}

/// The key a command routes by (mirrors the server's command_key): args[1]
/// unless the command addresses no key. Multi-key commands route by their
/// FIRST key in v0 (scatter-gather is a follow-on).
fn route_key(args: &[Vec<u8>]) -> Option<&[u8]> {
    let name = args.first()?;
    const NO_KEY: &[&[u8]] = &[
        b"PING",
        b"ECHO",
        b"DBSIZE",
        b"FLUSHALL",
        b"COMMAND",
        b"CLUSTER",
        b"INFO",
        b"SELECT",
        b"QUIT",
        b"HELLO",
    ];
    if NO_KEY.iter().any(|c| name.eq_ignore_ascii_case(c)) {
        return None;
    }
    args.get(1).map(|k| k.as_slice())
}

/// Forward one client command, absorbing -MOVED / -TRYAGAIN / backend death
/// within the retry budget. Returns the reply the CLIENT should see.
fn forward(
    topo: &Topology,
    backends: &mut Backends,
    ns: &[u8],
    args: &[Vec<u8>],
    frame: &[u8],
    read_replica: bool,
) -> Value {
    let slot = route_key(args).map(slot_for_key);
    let deadline = Instant::now() + RETRY_BUDGET;
    // D7: prefer a replica for this read; cleared to fall back to the master
    // if a replica attempt errors, so a dead/lagging replica never fails a
    // read.
    let mut use_replica = read_replica;
    loop {
        // Resolve the target: a D7 read to a replica of the owning pair, else
        // the slot's master; no-key commands go to pair 0's master.
        let target = match slot {
            Some(s) if use_replica => topo.route_replica(ns, s).or_else(|| topo.route(ns, s)),
            Some(s) => topo.route(ns, s),
            None => topo
                .routing
                .read()
                .ok()
                .and_then(|r| r.masters.first().cloned().flatten()),
        };
        let Some(addr) = target else {
            if Instant::now() > deadline {
                return Value::Error("ERR no reachable master for this slot".into());
            }
            // Nothing known yet (e.g. mid-failover): rediscover everything.
            let pairs: Vec<Vec<String>> = topo
                .routing
                .read()
                .map(|r| r.pairs.clone())
                .unwrap_or_default();
            for (i, nodes) in pairs.iter().enumerate() {
                let found = discover_master(nodes, &topo.backend_tls);
                if let Ok(mut routing) = topo.routing.write()
                    && let Some(m) = routing.masters.get_mut(i)
                {
                    *m = found;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
            continue;
        };

        match backends.call(&addr, frame) {
            Ok(Value::Error(e)) if e.starts_with("MOVED ") => {
                // "MOVED <slot> <addr>": learn and chase. The client must
                // never see this — absorbing it is the proxy's reason to
                // exist.
                let mut parts = e.split(' ');
                let (_, s, new_addr) = (parts.next(), parts.next(), parts.next());
                if let (Some(s), Some(new_addr)) = (s, new_addr)
                    && let Ok(s) = s.parse::<u16>()
                {
                    topo.learn_moved(ns, s, new_addr);
                }
                if Instant::now() > deadline {
                    return Value::Error("ERR routing did not settle (moved chase)".into());
                }
            }
            Ok(Value::Error(e)) if e.starts_with("TRYAGAIN") => {
                // Slot frozen mid-cutover: the drain is sub-second; wait it
                // out inside the budget instead of bothering the client.
                if Instant::now() > deadline {
                    return Value::Error(e);
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(Value::Error(e)) if e.starts_with("READONLY") => {
                // A LIVE node refusing writes where we expected a master: a
                // demoted-in-place ex-master (controlled failover demotes
                // first and the process stays up). Connection failure is not
                // the only stale-routing signal — rediscover this pair's
                // master and retry, exactly like the dead-backend path.
                // Without this the proxy wedges: the node answers, so no
                // error path ever fires, and every write bounces -READONLY.
                if Instant::now() > deadline {
                    return Value::Error(e);
                }
                topo.rediscover_for(&addr);
                backends.drop_conn(&addr);
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(reply) => return reply,
            Err(_) => {
                // Backend died (or timed out). If this was a D7 replica read,
                // fall back to the master for the retry — a dead/slow replica
                // must never fail a read. Otherwise rediscover the pair's
                // master (the failover-chasing path).
                if Instant::now() > deadline {
                    return Value::Error(
                        "ERR backend unavailable (failover did not settle)".into(),
                    );
                }
                if use_replica {
                    use_replica = false;
                    backends.drop_conn(&addr);
                } else {
                    topo.rediscover_for(&addr);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// Fan a command out to every pair's master; combine with `combine`.
fn fan_out(
    topo: &Topology,
    backends: &mut Backends,
    frame: &[u8],
    combine: impl Fn(Vec<Value>) -> Value,
) -> Value {
    let masters: Vec<Option<String>> = match topo.routing.read() {
        Ok(r) => r.masters.clone(),
        Err(_) => return Value::Error("ERR topology lock".into()),
    };
    let mut replies = Vec::new();
    for addr in masters {
        let Some(addr) = addr else {
            return Value::Error("ERR a pair has no reachable master".into());
        };
        match backends.call(&addr, frame) {
            Ok(v) => replies.push(v),
            Err(e) => return Value::Error(format!("ERR fan-out to {addr}: {e}")),
        }
    }
    combine(replies)
}

/// Outcome of the per-command auth check.
enum AuthStep {
    /// Answer the client directly (AUTH result, NOAUTH, WRONGPASS).
    Reply(Value),
    /// Authorized: proceed in this namespace.
    Proceed(Vec<u8>),
}

/// Redis-shaped auth gate. AUTH <token> or AUTH <user> <token> (the user is
/// ignored; the token alone identifies the tenant). Before auth, everything
/// except AUTH/QUIT gets -NOAUTH. A successful AUTH fixes the connection's
/// namespace; re-AUTH to a different tenant is rejected (reconnect instead)
/// so the backend-connection namespace pinning can never go stale.
fn auth_step(
    topo: &Topology,
    authed_ns: &mut Option<Vec<u8>>,
    replica_reads: &mut bool,
    local_cache: &mut bool,
    is_admin: &mut bool,
    args: &[Vec<u8>],
) -> AuthStep {
    let name = args.first().map(|n| n.to_ascii_uppercase());
    // Admin gate for the operator surface: locked = a token is configured
    // and this connection has not presented it.
    let admin_locked = topo
        .admin_digests
        .read()
        .map(|d| !d.is_empty())
        .unwrap_or(false)
        && !*is_admin;
    let admin_denied = || {
        AuthStep::Reply(Value::Error(
            "NOAUTH admin token required for this command".into(),
        ))
    };
    // Ops query: per-token AUTH count (drain check during token rotation).
    // Requires knowing the exact token; low-sensitivity, answered pre-auth.
    // (A real deploy gates this behind mTLS/admin.)
    // Ops query: aggregate proxy counters for the exporter (same pre-auth
    // rationale as PROXYAUTHCOUNT below). `active` is filled by the caller
    // holding the admission counter.
    if name.as_deref() == Some(b"PROXYSTATS") {
        if admin_locked {
            return admin_denied();
        }
        let load = |c: &std::sync::atomic::AtomicU64| c.load(Ordering::Relaxed);
        let (cache_ttl, cache_max, cache_hits, cache_misses, cache_entries, cache_bytes) =
            topo.cache.stats();
        let quota_throttled = topo.stat_quota_throttled_total.load(Ordering::Relaxed);
        let quota_write_shed = topo.stat_quota_write_shed_total.load(Ordering::Relaxed);
        let info = format!(
            "active:{}\r\nconns_total:{}\r\nshed_total:{}\r\nauth_ok_total:{}\r\nauth_fail_total:{}\r\ncommands_total:{}\r\ncommands_read_total:{}\r\ncommands_write_total:{}\r\nhotkey_sample_rate:{}\r\ncache_ttl_ms:{cache_ttl}\r\ncache_max_bytes:{cache_max}\r\ncache_hits_total:{cache_hits}\r\ncache_misses_total:{cache_misses}\r\ncache_entries:{cache_entries}\r\ncache_bytes:{cache_bytes}\r\nquota_throttled_total:{quota_throttled}\r\nquota_write_shed_total:{quota_write_shed}\r\ncert_days_remaining:{cdr}\r\n",
            topo.stat_active.load(Ordering::Relaxed),
            load(&topo.stat_conns_total),
            load(&topo.stat_shed_total),
            load(&topo.stat_auth_ok_total),
            load(&topo.stat_auth_fail_total),
            load(&topo.stat_commands_total),
            load(&topo.stat_commands_read_total),
            load(&topo.stat_commands_write_total),
            // The hot-key sketch samples 1-in-N; the reported hotkey rate is
            // already scaled back up by this, so it IS the estimated ops/s.
            // Exposed for transparency about the estimate's granularity.
            HOTKEY_SAMPLE_RATE,
            cdr = topo
                .cert_path
                .as_deref()
                .and_then(flint_tls::cert_days_remaining)
                .map_or_else(|| "none".into(), |d| d.to_string()),
        );
        return AuthStep::Reply(Value::Bulk(Some(info.into_bytes())));
    }
    // Ops/tenant query: per-tenant latency histograms (the client-metrics
    // substitute — measured one hop from the client). Same scoping contract
    // as PROXYHOTKEYS: authed = own namespace only (latency shapes leak
    // workload patterns); pre-auth = the all-tenant operator view.
    if name.as_deref() == Some(b"PROXYLATENCY") {
        let scope = authed_ns.as_deref().filter(|_| !topo.open_mode);
        // The UNSCOPED view aggregates every tenant's latency shapes; with a
        // token configured it is admin-only. A tenant's own view is always
        // available to that tenant.
        if scope.is_none() && admin_locked {
            return admin_denied();
        }
        return AuthStep::Reply(Value::Bulk(Some(topo.latency.report(scope).into_bytes())));
    }
    // Ops knob: the proxy near-cache (D6). No args -> report; two args ->
    // set (ttl_ms, max_bytes) at RUNTIME; ttl 0 disables and clears. Same
    // pre-auth operator surface as the other PROXY* commands (mTLS-gated in
    // a real deploy). Which tenants it applies to is NOT set here — that is
    // the tenant's own CPTENANTCACHE consent.
    if name.as_deref() == Some(b"PROXYCACHE") {
        if admin_locked {
            return admin_denied();
        }
        match args.len() {
            1 => {
                let (ttl, maxb, hits, misses, entries, bytes) = topo.cache.stats();
                return AuthStep::Reply(Value::Bulk(Some(
                    format!(
                        "ttl_ms:{ttl}\r\nmax_bytes:{maxb}\r\nhits_total:{hits}\r\nmisses_total:{misses}\r\nentries:{entries}\r\nbytes:{bytes}\r\n"
                    )
                    .into_bytes(),
                )));
            }
            3 => {
                let (Some(ttl), Some(maxb)) = (
                    std::str::from_utf8(&args[1])
                        .ok()
                        .and_then(|v| v.parse().ok()),
                    std::str::from_utf8(&args[2])
                        .ok()
                        .and_then(|v| v.parse().ok()),
                ) else {
                    return AuthStep::Reply(Value::Error(
                        "ERR PROXYCACHE [<ttl_ms> <max_bytes>]".into(),
                    ));
                };
                topo.cache.configure(ttl, maxb);
                return AuthStep::Reply(Value::Simple("OK".into()));
            }
            _ => {
                return AuthStep::Reply(Value::Error(
                    "ERR PROXYCACHE [<ttl_ms> <max_bytes>]".into(),
                ));
            }
        }
    }
    // Ops/tenant query: top hot keys (D5, observability only). SCOPING IS
    // THE CONTRACT: an AUTHED connection sees only its own namespace (key
    // names are data); the unscoped all-namespace view answers pre-auth —
    // the operator/internal surface, mTLS-gated in a real deploy like the
    // other PROXY* introspection commands.
    if name.as_deref() == Some(b"PROXYHOTKEYS") {
        let scope = authed_ns.as_deref().filter(|_| !topo.open_mode);
        // The UNSCOPED view names every tenant's keys (key names are data);
        // with a token configured it is admin-only.
        if scope.is_none() && admin_locked {
            return admin_denied();
        }
        let rows = topo.hotkeys.top(scope, 16);
        let mut out = String::new();
        for (ns, key, r, w) in rows {
            out.push_str(&format!(
                "{} {} reads:{r} writes:{w}\r\n",
                String::from_utf8_lossy(&ns),
                String::from_utf8_lossy(&key),
            ));
        }
        return AuthStep::Reply(Value::Bulk(Some(out.into_bytes())));
    }
    if name.as_deref() == Some(b"PROXYAUTHCOUNT") {
        if admin_locked {
            return admin_denied();
        }
        let n = args
            .get(1)
            .map(|t| topo.auth_count(&String::from_utf8_lossy(t)))
            .unwrap_or(0);
        return AuthStep::Reply(Value::Integer(n as i64));
    }
    if name.as_deref() == Some(b"AUTH") {
        let token = match args.len() {
            2 => &args[1],
            3 => &args[2],
            _ => {
                return AuthStep::Reply(Value::Error(
                    "ERR wrong number of arguments for 'auth' command".into(),
                ));
            }
        };
        // The admin token opens the operator surface, never a namespace: an
        // admin session has no tenant and cannot run data commands. Match
        // the presented token's DIGEST against current OR previous (the
        // rotation window), so a roll never locks the operator out.
        let presented = flint_tls::sha256_hex(token);
        if topo
            .admin_digests
            .read()
            .map(|d| d.contains(&presented))
            .unwrap_or(false)
        {
            *is_admin = true;
            return AuthStep::Reply(Value::Simple("OK admin".into()));
        }
        if topo.open_mode {
            return AuthStep::Reply(Value::Error(
                "ERR Client sent AUTH, but no tenants are configured".into(),
            ));
        }
        let Some(grant) = topo.lookup_token(&String::from_utf8_lossy(token)) else {
            topo.stat_auth_fail_total.fetch_add(1, Ordering::Relaxed);
            return AuthStep::Reply(Value::Error("WRONGPASS invalid token".into()));
        };
        match authed_ns {
            Some(cur) if *cur != grant.ns => {
                return AuthStep::Reply(Value::Error(
                    "ERR already authenticated as another tenant; reconnect to switch".into(),
                ));
            }
            _ => {}
        }
        topo.bump_auth(&String::from_utf8_lossy(token));
        topo.stat_auth_ok_total.fetch_add(1, Ordering::Relaxed);
        *authed_ns = Some(grant.ns);
        *replica_reads = grant.replica_reads;
        *local_cache = grant.local_cache;
        return AuthStep::Reply(Value::Simple("OK".into()));
    }
    match authed_ns {
        Some(ns) => {
            // QUIT stays answerable regardless.
            AuthStep::Proceed(ns.clone())
        }
        None => {
            if name.as_deref() == Some(b"QUIT") {
                AuthStep::Reply(Value::Simple("OK".into()))
            } else {
                AuthStep::Reply(Value::Error("NOAUTH Authentication required.".into()))
            }
        }
    }
}

fn serve_client<S: Read + Write>(mut stream: S, topo: Arc<Topology>) -> std::io::Result<()> {
    // Open mode (standalone, no tenants configured): the connection starts
    // authorized on the default namespace — the pre-tenancy behavior.
    let mut authed_ns: Option<Vec<u8>> = if topo.open_mode {
        Some(b"0".to_vec())
    } else {
        None
    };
    let mut backends: Option<Backends> = None;
    // Replica-read opt-in for THIS connection's tenant (D7), set at AUTH.
    let mut replica_reads = false;
    // Proxy near-cache opt-in for THIS connection's tenant (D6), set at AUTH.
    let mut local_cache = false;
    // Admin session (AUTH <admin-token>): unlocks the operator surface only.
    let mut is_admin = false;
    // Hot-key sampling tick (D5): every HOTKEY_SAMPLE_RATE-th keyed command
    // on this connection feeds the sketch, keeping the mutex off the
    // common path.
    let mut hotkey_tick: u32 = 0;
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut chunk = [0u8; 16 * 1024];
    let mut out: Vec<u8> = Vec::with_capacity(4 * 1024);
    loop {
        let mut consumed = 0;
        out.clear();
        loop {
            match decode(&buf[consumed..]) {
                Ok(Decoded::Complete(frame, used)) => {
                    let raw = buf[consumed..consumed + used].to_vec();
                    consumed += used;
                    let Some(args) = frame_to_args(frame) else {
                        encode(
                            &Value::Error(
                                "ERR Protocol error: expected array of bulk strings".into(),
                            ),
                            &mut out,
                        );
                        stream.write_all(&out)?;
                        return Ok(());
                    };
                    topo.stat_commands_total.fetch_add(1, Ordering::Relaxed);
                    if let Some(name) = args.first() {
                        let is_write = flint_commands::is_write_command(name);
                        if is_write {
                            topo.stat_commands_write_total
                                .fetch_add(1, Ordering::Relaxed);
                        } else if flint_commands::is_read_command(name) {
                            topo.stat_commands_read_total
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        if is_write || flint_commands::is_read_command(name) {
                            hotkey_tick = hotkey_tick.wrapping_add(1);
                            if hotkey_tick.is_multiple_of(HOTKEY_SAMPLE_RATE)
                                && let Some(ns) = authed_ns.as_deref()
                                && let Some(key) = route_key(&args)
                            {
                                topo.hotkeys.observe(ns, key, is_write);
                            }
                        }
                    }
                    // Tenant-perceived latency starts here: everything the
                    // proxy does for this command — cache lookup, routing,
                    // backend round trip, MOVED/failover retries — is what
                    // the client waits for.
                    let started = Instant::now();
                    let reply = match auth_step(
                        &topo,
                        &mut authed_ns,
                        &mut replica_reads,
                        &mut local_cache,
                        &mut is_admin,
                        &args,
                    ) {
                        AuthStep::Reply(v) => v,
                        AuthStep::Proceed(ns) => data_command(
                            &topo,
                            &mut backends,
                            &ns,
                            &args,
                            &raw,
                            replica_reads,
                            local_cache,
                        ),
                    };
                    // Record into this tenant's read/write histogram — data
                    // commands only (the D1 classifier; AUTH/PROXY* are
                    // neither), and only once a namespace is bound.
                    if let Some(ns) = authed_ns.as_deref()
                        && let Some(name) = args.first()
                    {
                        let is_write = flint_commands::is_write_command(name);
                        if is_write || flint_commands::is_read_command(name) {
                            topo.latency.observe(
                                ns,
                                is_write,
                                started.elapsed().as_micros() as u64,
                            );
                        }
                    }
                    encode(&reply, &mut out);
                }
                Ok(Decoded::NeedMore) => break,
                Err(_) => {
                    encode(&Value::Error("ERR Protocol error".into()), &mut out);
                    stream.write_all(&out)?;
                    return Ok(());
                }
            }
        }
        if consumed > 0 {
            buf.drain(..consumed);
            if !out.is_empty() {
                stream.write_all(&out)?;
            }
        }
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// One authorized tenant command, end to end: the D6 near-cache in front
/// (lookup, fill, write-invalidation), D7 replica routing, and the backend
/// forward. Extracted from `serve_client` so the connection loop stays a
/// decode/auth/encode skeleton.
#[allow(clippy::too_many_arguments)]
fn data_command(
    topo: &Topology,
    backends: &mut Option<Backends>,
    ns: &[u8],
    args: &[Vec<u8>],
    raw: &[u8],
    replica_reads: bool,
    local_cache: bool,
) -> Value {
    // M5 quota gate FIRST: a shed command must not touch the cache, a
    // backend, or the bucket-bypassing fast paths. Only data commands are
    // charged (PING/ECHO/QUIT are free; they cost this proxy nothing).
    let is_write = args
        .first()
        .is_some_and(|n| flint_commands::is_write_command(n));
    let is_data = is_write
        || args
            .first()
            .is_some_and(|n| flint_commands::is_read_command(n));
    if is_data
        && let Some(name) = args.first()
        && let Some(shed) = topo.quota_gate(ns, name, is_write)
    {
        return shed;
    }
    // D6 near-cache: a plain GET for an opted-in tenant may answer from the
    // proxy-local cache (TTL-bounded staleness, the tenant's choice). The
    // hot-key sketch already observed the command in the connection loop,
    // so detection still sees cached reads.
    let cacheable = local_cache
        && topo.cache.enabled()
        && args.len() == 2
        && args[0].eq_ignore_ascii_case(b"GET");
    if let Some(v) = cacheable.then(|| topo.cache.get(ns, &args[1])).flatten() {
        return Value::Bulk(Some(v));
    }
    // D7: route to a replica only if the tenant opted in AND this is a read
    // (writes stay on the master).
    let read_replica = replica_reads
        && args
            .first()
            .is_some_and(|n| flint_commands::is_read_command(n));
    let b = backends.get_or_insert_with(|| Backends::new(ns.to_vec(), topo.backend_tls.clone()));
    let reply = handle(topo, b, ns, args, raw, read_replica);
    if cacheable && let Value::Bulk(Some(v)) = &reply {
        topo.cache.put(ns, &args[1], v);
    }
    // A write through THIS proxy invalidates its local entries —
    // read-your-own-writes through one proxy. (A write through another
    // proxy is seen here only after the TTL: the contract.)
    if local_cache
        && topo.cache.enabled()
        && args
            .first()
            .is_some_and(|n| flint_commands::is_write_command(n))
    {
        match args[0].to_ascii_uppercase().as_slice() {
            b"DEL" | b"UNLINK" => {
                for k in &args[1..] {
                    topo.cache.invalidate(ns, k);
                }
            }
            // MSET k1 v1 k2 v2 ...: EVERY written key (odd indices) must
            // drop, or a cached later key would keep serving its old value
            // through this proxy — the read-your-own-writes contract.
            b"MSET" => {
                for k in args[1..].iter().step_by(2) {
                    topo.cache.invalidate(ns, k);
                }
            }
            b"FLUSHALL" => topo.cache.invalidate_ns(ns),
            _ => {
                if let Some(k) = args.get(1) {
                    topo.cache.invalidate(ns, k);
                }
            }
        }
    }
    reply
}

fn handle(
    topo: &Topology,
    backends: &mut Backends,
    ns: &[u8],
    args: &[Vec<u8>],
    raw: &[u8],
    read_replica: bool,
) -> Value {
    let Some(name) = args.first() else {
        return Value::Error("ERR empty command".into());
    };
    let upper = name.to_ascii_uppercase();
    match upper.as_slice() {
        // Connection-level commands are answered locally: they concern the
        // client<->proxy hop, not any backend.
        b"PING" => match args.len() {
            1 => Value::Simple("PONG".into()),
            2 => Value::Bulk(Some(args[1].clone())),
            _ => Value::Error("ERR wrong number of arguments for 'ping' command".into()),
        },
        b"ECHO" if args.len() == 2 => Value::Bulk(Some(args[1].clone())),
        b"QUIT" => Value::Simple("OK".into()),
        // The data-plane admin surface stays internal; the proxy is the
        // tenant boundary.
        _ if upper.starts_with(b"FLINT") => {
            Value::Error("ERR admin commands are not available through the proxy".into())
        }
        // Group-wide aggregates fan out.
        b"DBSIZE" => fan_out(topo, backends, raw, |replies| {
            let mut total = 0i64;
            for r in replies {
                match r {
                    Value::Integer(n) => total += n,
                    other => return Value::Error(format!("ERR dbsize fan-out: {other:?}")),
                }
            }
            Value::Integer(total)
        }),
        b"FLUSHALL" => fan_out(topo, backends, raw, |replies| {
            for r in replies {
                if !matches!(&r, Value::Simple(s) if s == "OK") {
                    return Value::Error(format!("ERR flushall fan-out: {r:?}"));
                }
            }
            Value::Simple("OK".into())
        }),
        _ => forward(topo, backends, ns, args, raw, read_replica),
    }
}

/// One CPWATCH session: subscribe, apply pushed snapshots, ACK each. The
/// snapshot frame is [SNAPSHOT, version, "a,b;c,d", "tok=ns,..."] — already
/// filtered by the control plane to this proxy's assigned tenants.
fn watch_control_plane(
    cp: &str,
    advertise: &str,
    topo: &Topology,
    last_version: &mut u64,
) -> std::io::Result<()> {
    // Same internal-mesh client credentials as the backend hop: the control
    // plane is part of the mesh, so one --internal-* triple covers both.
    let mut stream = flint_tls::connect(cp, &topo.backend_tls)?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    let mut out = Vec::new();
    encode(
        &Value::Array(Some(vec![
            Value::Bulk(Some(b"CPWATCH".to_vec())),
            Value::Bulk(Some(advertise.as_bytes().to_vec())),
            Value::Bulk(Some(last_version.to_string().into_bytes())),
        ])),
        &mut out,
    );
    stream.write_all(&out)?;
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    loop {
        match decode(&buf) {
            Ok(Decoded::Complete(frame, used)) => {
                buf.drain(..used);
                if let Value::Array(Some(items)) = &frame
                    && items.len() >= 4
                    && matches!(items.first(), Some(Value::Bulk(Some(t))) if t.eq_ignore_ascii_case(b"SNAPSHOT"))
                    && let (
                        Some(Value::Integer(version)),
                        Some(Value::Bulk(Some(pairs))),
                        Some(Value::Bulk(Some(tenants))),
                    ) = (items.get(1), items.get(2), items.get(3))
                {
                    // The admin field (element 4) is present from D4 CPs
                    // only; older frames omit it, leaving the static digest.
                    let admin = match items.get(4) {
                        Some(Value::Bulk(Some(a))) => String::from_utf8_lossy(a).to_string(),
                        _ => String::new(),
                    };
                    topo.apply_snapshot(
                        &String::from_utf8_lossy(pairs),
                        &String::from_utf8_lossy(tenants),
                        &admin,
                    );
                    *last_version = *version as u64;
                    eprintln!("control-plane snapshot v{version} applied");
                    out.clear();
                    encode(
                        &Value::Array(Some(vec![
                            Value::Bulk(Some(b"ACK".to_vec())),
                            Value::Bulk(Some(version.to_string().into_bytes())),
                        ])),
                        &mut out,
                    );
                    stream.write_all(&out)?;
                }
            }
            Ok(Decoded::NeedMore) => {
                let n = stream.read(&mut chunk)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "control plane closed",
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

fn frame_to_args(frame: Value) -> Option<Vec<Vec<u8>>> {
    let Value::Array(Some(items)) = frame else {
        return None;
    };
    let mut args = Vec::with_capacity(items.len());
    for item in items {
        let Value::Bulk(Some(bytes)) = item else {
            return None;
        };
        args.push(bytes);
    }
    Some(args)
}

fn main() -> std::io::Result<()> {
    let port: u16 = arg("--port").and_then(|p| p.parse().ok()).unwrap_or(7379);
    let control_plane = arg("--control-plane");

    // Client-facing TLS termination: both flags together enable it, neither
    // keeps the plaintext listener (byte-identical to pre-TLS). One without
    // the other is a config error, not a silent downgrade.
    let tls: Option<Arc<rustls::ServerConfig>> = match (arg("--tls-cert"), arg("--tls-key")) {
        (Some(cert), Some(key)) => {
            Some(flint_tls::server_only_config(&cert, &key).expect("client-facing TLS config"))
        }
        (None, None) => None,
        _ => panic!("--tls-cert and --tls-key must be provided together"),
    };

    // Standalone config (ignored in control-plane mode, which pushes both).
    let pairs: Vec<Vec<String>> = arg("--pairs")
        .map(|spec| {
            spec.split(';')
                .map(|p| p.split(',').map(String::from).collect())
                .collect()
        })
        .unwrap_or_default();
    let tenants: HashMap<String, TenantGrant> = arg("--tenants")
        .map(|spec| {
            spec.split(',')
                .filter_map(|pair| {
                    let (token, ns) = pair.split_once('=')?;
                    assert!(
                        !ns.is_empty() && ns.len() <= 64 && !ns.contains('\0'),
                        "invalid namespace in --tenants"
                    );
                    // Static mode has no CP flags; opt-ins and quotas
                    // default off/unlimited. Keyed by digest like the CP
                    // push (D1): the process never holds plaintext either.
                    Some((
                        flint_tls::sha256_hex(token.as_bytes()),
                        TenantGrant {
                            ns: ns.as_bytes().to_vec(),
                            ..TenantGrant::default()
                        },
                    ))
                })
                .collect()
        })
        .unwrap_or_default();
    assert!(
        control_plane.is_some() || !pairs.is_empty(),
        "need --pairs \"m0,r0;m1,r1\" or --control-plane <addr>"
    );

    // Internal-mesh mutual TLS for the backend hop: the proxy is the CLIENT
    // (presents its cert, verifies each backend against the shared CA). Same
    // both-or-none-plus-ca gating as the frontend; the triple is shared with
    // the servers, used here in the client role.
    let backend_tls: Option<Arc<rustls::ClientConfig>> = match (
        arg("--internal-ca"),
        arg("--internal-cert"),
        arg("--internal-key"),
    ) {
        (Some(ca), Some(cert), Some(key)) => Some(
            flint_tls::client_config(&ca, &cert, &key)
                .expect("build backend (internal) TLS client config"),
        ),
        (None, None, None) => None,
        _ => panic!("--internal-ca, --internal-cert, --internal-key must be given together"),
    };

    let open_mode = control_plane.is_none() && tenants.is_empty();
    let masters: Vec<Option<String>> = pairs
        .iter()
        .map(|nodes| discover_master(nodes, &backend_tls))
        .collect();
    eprintln!(
        "flint-proxy: {} static pair(s), {} static tenant(s), control-plane {:?}, open={open_mode}",
        pairs.len(),
        tenants.len(),
        control_plane,
    );
    let topo = Arc::new(Topology {
        routing: RwLock::new(Routing {
            pairs,
            masters,
            ranges: Vec::new(),
        }),
        moved: RwLock::new(HashMap::new()),
        quota: RwLock::new(
            tenants
                .values()
                .map(|g| (g.ns.clone(), (g.rate, g.over_quota)))
                .collect(),
        ),
        buckets: std::sync::Mutex::new(HashMap::new()),
        stat_quota_throttled_total: std::sync::atomic::AtomicU64::new(0),
        stat_quota_write_shed_total: std::sync::atomic::AtomicU64::new(0),
        tenants: RwLock::new(tenants),
        auth_counts: RwLock::new(HashMap::new()),
        stat_conns_total: std::sync::atomic::AtomicU64::new(0),
        stat_shed_total: std::sync::atomic::AtomicU64::new(0),
        stat_auth_ok_total: std::sync::atomic::AtomicU64::new(0),
        stat_auth_fail_total: std::sync::atomic::AtomicU64::new(0),
        stat_commands_total: std::sync::atomic::AtomicU64::new(0),
        stat_commands_read_total: std::sync::atomic::AtomicU64::new(0),
        stat_commands_write_total: std::sync::atomic::AtomicU64::new(0),
        stat_active: AtomicUsize::new(0),
        replica_rr: AtomicUsize::new(0),
        hotkeys: HotKeySketch::new(),
        latency: latency::LatencyHistograms::default(),
        // D6 near-cache: OFF unless --cache-ttl-ms is given; both knobs stay
        // runtime-settable via PROXYCACHE. The default budget is deliberately
        // small — this is a hot-spot absorber, not a data tier.
        cache: cache::ProxyCache::new(
            arg("--cache-ttl-ms")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            arg("--cache-max-bytes")
                .and_then(|v| v.parse().ok())
                .unwrap_or(64 * 1024 * 1024),
        ),
        open_mode,
        backend_tls,
        cert_path: arg("--internal-cert"),
        admin_digests: RwLock::new(
            arg("--admin-token")
                .map(|t| vec![flint_tls::sha256_hex(t.as_bytes())])
                .unwrap_or_default(),
        ),
    });

    // Control-plane subscription: CPWATCH pushes filtered snapshots (pairs
    // + THIS proxy's tenants); we apply and ACK. Reconnect with backoff on
    // any error — the last-applied table keeps serving in the meantime
    // (control-plane outage never touches the data path).
    if let Some(cp) = control_plane {
        let advertise = arg("--advertise").expect("--control-plane requires --advertise <addr>");
        let topo = Arc::clone(&topo);
        std::thread::spawn(move || {
            let mut last_version: u64 = 0;
            loop {
                if let Err(e) = watch_control_plane(&cp, &advertise, &topo, &mut last_version) {
                    eprintln!("control-plane watch: {e}; reconnecting");
                }
                std::thread::sleep(Duration::from_millis(1000));
            }
        });
    }

    let max_conns: usize = arg("--max-conns")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_CONNS);

    let listener = TcpListener::bind(("127.0.0.1", port))?;
    eprintln!(
        "flint-proxy listening on 127.0.0.1:{port} ({}, max-conns {max_conns})",
        if tls.is_some() { "TLS" } else { "plaintext" }
    );
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        // Admission control: reserve a slot with fetch_add, roll back and shed
        // if it put us over the cap (no worker thread is spawned for a shed).
        topo.stat_conns_total.fetch_add(1, Ordering::Relaxed);
        if topo.stat_active.fetch_add(1, Ordering::Relaxed) >= max_conns {
            topo.stat_active.fetch_sub(1, Ordering::Relaxed);
            topo.stat_shed_total.fetch_add(1, Ordering::Relaxed);
            // Best-effort -THROTTLED for a plaintext client. Under frontend
            // TLS we skip the handshake (spending it on a shed would defeat
            // the guard) and just close — the client sees a reset and backs
            // off, the same contract.
            if tls.is_none() {
                let _ = stream.write_all(SHED_FRAME);
            }
            continue;
        }
        let topo = Arc::clone(&topo);
        let tls = tls.clone();
        let guard = ConnGuard(Arc::clone(&topo));
        std::thread::spawn(move || {
            let _guard = guard; // decrements on any exit, including panic
            match tls {
                Some(cfg) => {
                    // The handshake runs lazily on the first read/write inside
                    // serve_client (the client sends the first command). A
                    // plaintext client hitting the TLS port fails the handshake
                    // and the connection drops — no RESP is ever processed.
                    let conn = match rustls::ServerConnection::new(cfg) {
                        Ok(c) => c,
                        Err(e) => {
                            eprintln!("tls: connection setup failed: {e}");
                            return;
                        }
                    };
                    let _ = serve_client(rustls::StreamOwned::new(conn, stream), topo);
                }
                None => {
                    let _ = serve_client(stream, topo);
                }
            }
        });
    }
    Ok(())
}
