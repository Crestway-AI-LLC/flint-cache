// SPDX-License-Identifier: Elastic-2.0
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
mod errors;
mod latency;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use flint_resp::{Decoded, Value, decode, encode, encode_proto};
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

/// Per-connection undecoded-input ceiling (mirrors the server's
/// client-query-buffer-limit). With bulk strings capped at the codec's
/// MAX_BULK_LEN, a well-behaved client never accumulates this much pending
/// input; hitting it means a hostile or broken client and the connection is
/// closed. The PROXY is the tenant-facing surface, so it must bound this too
/// — not only the server behind it.
const MAX_QUERY_BUF: usize = 1024 * 1024 * 1024;

/// Longest accepted inline (non-RESP) command line without a newline —
/// same bound the server applies, so a client cannot make the proxy buffer
/// an unbounded "line" that never arrives.
const MAX_INLINE_LEN: usize = 64 * 1024;

/// Flush accumulated pipeline replies once they pass this size, instead of
/// buffering a whole read-batch of replies. Without it, a pipeline of large
/// GETs can demand an arbitrarily large reply buffer on the proxy.
const OUT_FLUSH_THRESHOLD: usize = 1024 * 1024;

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
    /// Federation flag (ADR-0007, plumbing only today): this tenant's slot
    /// space may span member clusters. Routing consequences arrive with the
    /// fleet-map work; until then the flag rides the wire and is stored.
    #[allow(dead_code)]
    federated: bool,
    /// Over storage quota: writes shed with -QUOTA, reads served.
    over_quota: bool,
    /// Async write-queue opt-in (ADR-0005 D4): backend connections for this
    /// tenant are pinned with the async handshake flag, so batchable writes
    /// coalesce through the node's write queue. Applies to client
    /// connections authed after the flag arrives (the pin happens at dial).
    async_writes: bool,
    /// Per-proxy ops/s share (token bucket); 0 = unlimited.
    rate: u64,
}

/// A single-use channel grant (ADR-0010 D2). The proxy mints one when it
/// admits a family command — mapping an unguessable token to the tenant
/// namespace a co-processor may act in, a per-command budget, and a deadline —
/// and `PROXYCHAN <token>` consumes it to open a channel connection pinned to
/// `ns`. The token names a namespace the CO-PROCESSOR never can: `FLINTNS`, like
/// every `FLINT*`, is refused at the edge before auth (the #151 guard), so the
/// grant is the ONLY way a channel's namespace is set.
///
/// `budget` is recorded here and enforced in step 4 (D3, the resource class);
/// step 2 enforces single-use (`used`) and the deadline (refused at open, and
/// the channel is closed once it outlives the deadline).
struct ChannelGrant {
    ns: Vec<u8>,
    #[allow(dead_code)]
    budget: u64,
    deadline: Instant,
    used: bool,
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
/// One cluster's routing state: the level-1 table (pairs/masters/ranges)
/// plus its moved-slot override cache. Today every proxy holds exactly ONE
/// of these; a federated tenant's dedicated proxy group (ADR-0007) will
/// hold one per member cluster, selected per-request by the level-0 map.
struct ClusterView {
    routing: RwLock<Routing>,
    /// (namespace, slot) -> owner address, learned from -MOVED redirects.
    /// Keyed per namespace because migrations move one tenant's slot rows:
    /// tenant A's redirect must not reroute tenant B. Since Option B this
    /// is only the BRIDGE between a cutover's node flip and the CP's
    /// exception commit — entries are pruned as the pushed truth arrives.
    moved: RwLock<HashMap<(Vec<u8>, u16), String>>,
    /// (namespace, slot) -> pair INDEX: the CP-held ownership truth
    /// (Option B), pushed in the snapshot's 6th element. Index, not
    /// address, so a routed slot follows its pair's failovers. A cold
    /// proxy routes fragmented ownership correctly from its first
    /// snapshot — zero -MOVED redirects.
    exceptions: RwLock<HashMap<(Vec<u8>, u16), usize>>,
}

struct Topology {
    /// Member-cluster views, indexed by the level-0 map's cluster index.
    /// Invariant: non-empty; index 0 is the home cluster and the fallback.
    clusters: Vec<ClusterView>,
    /// Level-0: slot range -> cluster index (ADR-0007). A non-federated
    /// proxy holds the single-interval default (everything -> cluster 0),
    /// so today this resolves to 0 unconditionally.
    level0: RwLock<flint_slot::SlotIntervals>,
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
    /// -MOVED redirects LEARNED since start. With Option B this should sit
    /// at ~zero in steady state (the CP push carries ownership truth); a
    /// climbing value means proxies are discovering topology the hard way.
    stat_moved_learned: std::sync::atomic::AtomicU64,
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
    /// Per-tenant error counts by kind. PROXYSTATS' flat counters answer
    /// "are there errors" and never "whose, and which" — the only two
    /// questions an incident actually poses.
    errors: errors::ErrorCounts,
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
    /// Client-side mutual-TLS for dialing backends (the internal hop),
    /// hot-reloading its leaf (ADR-0006 D4) — each dial snapshots the
    /// current config. `None` = plaintext backends (default). Set by
    /// `--internal-*`; the same triple the servers use, in the client role.
    backend_tls: Option<Arc<flint_tls::ReloadableClientConfig>>,
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
    /// Last promotion hint applied ("<addr>|<gen>"). Compared for
    /// INEQUALITY — see apply_promote_hint.
    last_promote_hint: RwLock<String>,
    /// Single-use channel tokens (ADR-0010 D2): token -> grant. Populated by
    /// `PROXYCHANMINT` (step 3 will mint internally when admitting a family
    /// command) and consumed by `PROXYCHAN`. Swept of dead entries on each
    /// mint so it cannot grow without bound.
    channel_tokens: std::sync::Mutex<HashMap<String, ChannelGrant>>,
}

impl Topology {
    /// The address a command for `slot` in `ns` should go to right now.
    /// Range-based default owner (pair i serves slots [i*N/16384 ..)),
    /// overridden per (ns, slot) by the moved cache.
    fn route(&self, ns: &[u8], slot: u16) -> Option<String> {
        let view = self.cluster_for(slot);
        // Precedence: the -MOVED bridge (newest, seconds-lived) -> the
        // CP-pushed exception truth -> the pair's default range.
        if let Ok(moved) = view.moved.read()
            && let Some(addr) = moved.get(&(ns.to_vec(), slot))
        {
            return Some(addr.clone());
        }
        if let Ok(exc) = view.exceptions.read()
            && let Some(&pair) = exc.get(&(ns.to_vec(), slot))
        {
            let routing = view.routing.read().ok()?;
            match routing.masters.get(pair) {
                // Valid pair, master known: the exception routes.
                Some(Some(addr)) => return Some(addr.clone()),
                // Valid pair, master not yet discovered: fail THIS lookup so
                // the caller's rediscovery loop fills it (pre-existing
                // contract for undiscovered masters).
                Some(None) => return None,
                // Stale row referencing a pair that no longer exists:
                // DEGRADE to the default-range path below rather than
                // killing routing for this (ns, slot) outright.
                None => {}
            }
        }
        let routing = view.routing.read().ok()?;
        // The default owner — ONE definition shared with the CP's
        // exception-redundancy check (flint-slot::default_pair), so routing
        // and retirement can never drift.
        let pair = flint_slot::default_pair(slot, &routing.ranges, routing.pairs.len())?;
        routing.masters.get(pair)?.clone()
    }

    fn learn_moved(&self, ns: &[u8], slot: u16, addr: &str) {
        self.stat_moved_learned
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if let Ok(mut moved) = self.cluster_for(slot).moved.write() {
            moved.insert((ns.to_vec(), slot), addr.to_string());
        }
    }

    /// The member cluster owning `slot` per the level-0 map — cluster 0
    /// (the home cluster) for every non-federated proxy and for any
    /// uncovered slot. The federation seam: multi-cluster routing changes
    /// THIS function's inputs, not its callers.
    fn cluster_for(&self, slot: u16) -> &ClusterView {
        let idx = self
            .level0
            .read()
            .ok()
            .and_then(|m| m.lookup(slot))
            .map(|i| i as usize)
            .filter(|i| *i < self.clusters.len())
            .unwrap_or(0);
        &self.clusters[idx]
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
        let routing = self.cluster_for(slot).routing.read().ok()?;
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
        // A dead address could belong to any member cluster: walk them all
        // (one, today).
        for view in &self.clusters {
            let pairs: Vec<(usize, Vec<String>)> = match view.routing.read() {
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
                Err(_) => continue,
            };
            for (i, nodes) in pairs {
                let found = discover_master(&nodes, &self.backend_tls);
                if let Ok(mut routing) = view.routing.write()
                    && let Some(slot) = routing.masters.get_mut(i)
                {
                    *slot = found.clone();
                }
                // Moved entries pointing at the dead address migrate to the
                // pair's new master (slot data survives failover via the
                // pair's replica).
                if let Some(new_master) = found
                    && new_master != addr
                    && let Ok(mut moved) = view.moved.write()
                {
                    for v in moved.values_mut() {
                        if v == addr {
                            *v = new_master.clone();
                        }
                    }
                }
            }
        }
    }

    /// Act on the controller's promotion hint: if it DIFFERS from the last
    /// one seen, re-probe the named node's pair right now.
    ///
    /// Differs, not "is greater". The hint is not persisted across a CP
    /// restart, so a generation can legitimately go backwards; ordering
    /// would then ignore every later hint forever, and the symptom would be
    /// an intermittently slow failover that looks like a network problem.
    /// Re-probing is idempotent and costs one round trip per pair member, so
    /// erring toward an extra probe is the cheap direction.
    ///
    /// The address is a POINTER TO WHAT TO PROBE, never a routing decision:
    /// `rediscover_for` asks the pair who claims to be master and believes
    /// the epoch-fenced answer. A stale, duplicated, or outright wrong hint
    /// therefore cannot misroute a write — the worst case is a wasted probe.
    fn apply_promote_hint(&self, hint: &str) {
        if hint.is_empty() {
            return;
        }
        match self.last_promote_hint.write() {
            Ok(mut last) if *last != hint => *last = hint.to_string(),
            // Unchanged (a re-push of the same view, or a reconnect replay):
            // nothing to do. Poisoned lock: leave the reactive path to it.
            _ => return,
        }
        let Some((addr, _gen)) = hint.split_once('|') else {
            return;
        };
        self.rediscover_for(addr);
    }

    /// Apply a control-plane snapshot: replace the tenant table, and — only
    /// if the pair list actually changed — rebuild the routing table
    /// (discovering masters), preserving failover-chased masters otherwise.
    fn apply_snapshot(
        &self,
        pairs_spec: &str,
        tenants_spec: &str,
        admin_spec: &str,
        exc_spec: &str,
    ) {
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
        // Snapshot pushes feed the HOME cluster's view (index 0). A
        // federated proxy group (ADR-0007) will run one watch loop per
        // member CP, each targeting its own view.
        let view = &self.clusters[0];
        let rebuild = match view.routing.read() {
            Ok(r) => r.pairs != new_pairs || r.ranges != new_ranges,
            Err(_) => return,
        };
        if rebuild {
            let masters: Vec<Option<String>> = new_pairs
                .iter()
                .map(|n| discover_master(n, &self.backend_tls))
                .collect();
            if let Ok(mut routing) = view.routing.write() {
                routing.pairs = new_pairs;
                routing.masters = masters;
                routing.ranges = new_ranges;
            }
        }
        // Option B: install the pushed ownership truth, then retire any
        // -MOVED bridge entries it supersedes — the CP is authoritative
        // from this instant.
        let mut new_exc: HashMap<(Vec<u8>, u16), usize> = HashMap::new();
        for e in exc_spec.split(';').filter(|e| !e.is_empty()) {
            // ns may contain ':': parse from the right. The middle field is
            // a single slot or a consolidated run "lo-hi".
            let mut it = e.rsplitn(3, ':');
            let (Some(pair), Some(mid), Some(ns)) = (it.next(), it.next(), it.next()) else {
                continue;
            };
            let Ok(pair) = pair.parse::<usize>() else {
                continue;
            };
            let (lo, hi) = match mid.split_once('-') {
                Some((a, b)) => match (a.parse::<u16>(), b.parse::<u16>()) {
                    (Ok(a), Ok(b)) if a <= b => (a, b),
                    _ => continue,
                },
                None => match mid.parse::<u16>() {
                    Ok(s) => (s, s),
                    Err(_) => continue,
                },
            };
            for slot in lo..=hi {
                new_exc.insert((ns.as_bytes().to_vec(), slot), pair);
            }
        }
        if let Ok(mut exc) = view.exceptions.write() {
            *exc = new_exc.clone();
        }
        // Retire only bridge entries the truth AGREES with (addr == the
        // exception pair's current master). A DISAGREEING bridge is newer
        // information — a re-migration in flight that the CP has not
        // committed yet — and pruning it would cost one fresh -MOVED per
        // push until the commit lands.
        if let (Ok(mut moved), Ok(routing)) = (view.moved.write(), view.routing.read()) {
            moved.retain(|k, addr| match new_exc.get(k) {
                Some(&pair) => {
                    routing.masters.get(pair).and_then(|m| m.as_deref()) != Some(addr.as_str())
                }
                None => true,
            });
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
                        federated: flags.contains('f'),
                        async_writes: flags.contains('a'),
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
        // A removed tenant's latency lanes, error counts and bucket must
        // not live forever.
        self.latency.retain(&keep);
        self.errors.retain(&keep);
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
        // the very state it cures. Shared with the server's disk gate.
        if over && is_write && !flint_commands::reduces_space(name) {
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
fn discover_master(
    nodes: &[String],
    tls: &Option<Arc<flint_tls::ReloadableClientConfig>>,
) -> Option<String> {
    for addr in nodes {
        let Ok(mut stream) = flint_tls::connect_reloadable(addr, tls) else {
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
    /// Client-side mutual-TLS for the backend hop (`None` = plaintext),
    /// snapshotted from the reloadable handle at each dial.
    tls: Option<Arc<flint_tls::ReloadableClientConfig>>,
    /// Async write-queue opt-in (D4): pin backend conns with the 'a'
    /// handshake flag so the node routes batchable writes via its queue.
    async_writes: bool,
}

impl Backends {
    fn new(
        ns: Vec<u8>,
        tls: Option<Arc<flint_tls::ReloadableClientConfig>>,
        async_writes: bool,
    ) -> Self {
        Self {
            conns: HashMap::new(),
            ns,
            tls,
            async_writes,
        }
    }

    /// Discard the cached connection to `addr` (stale-routing recovery: the
    /// next call dials whatever the refreshed masters map says).
    fn drop_conn(&mut self, addr: &str) {
        self.conns.remove(addr);
    }

    fn call(&mut self, addr: &str, frame: &[u8]) -> std::io::Result<Value> {
        if !self.conns.contains_key(addr) {
            let mut stream = flint_tls::connect_reloadable(addr, &self.tls)?;
            stream.set_read_timeout(Some(BACKEND_TIMEOUT))?;
            stream.set_write_timeout(Some(BACKEND_TIMEOUT))?;
            // Speak RESP3 to the backend ALWAYS, whatever this proxy's
            // clients negotiated.
            //
            // The reason is that a RESP2 reply is ambiguous: a flat array
            // could be a hash, a list, or member/score pairs, and once the
            // types are flattened the proxy cannot put them back. RESP3
            // keeps them (`%`, `~`, `,`), so the proxy decodes a reply that
            // still knows what it means and re-renders it for whichever
            // dialect the client asked for.
            //
            // (This used to add "and connections are shared across clients
            // anyway", which is FALSE and worth correcting rather than
            // deleting: `Backends` lives inside `serve_client`, which is
            // spawned per accepted connection, so the pool is per CLIENT —
            // one connection per backend address, for that client alone.
            // Believing otherwise would rule out transactions entirely,
            // since two tenants' MULTIs would interleave on one socket. The
            // RESP3 argument above stands on its own.)
            let mut hello = Vec::new();
            encode(
                &Value::Array(Some(vec![
                    Value::Bulk(Some(b"HELLO".to_vec())),
                    Value::Bulk(Some(b"3".to_vec())),
                ])),
                &mut hello,
            );
            let mut hello_buf = Vec::new();
            match call_raw(&mut stream, &mut hello_buf, &hello)? {
                Value::Map(_) => {}
                other => {
                    return Err(std::io::Error::other(format!(
                        "backend refused RESP3: {other:?}"
                    )));
                }
            }
            // Pin the connection to the tenant namespace before any data
            // command can travel on it; the 'a' arg opts this connection's
            // batchable writes into the node's async write queue (D4).
            let mut hs_args = vec![
                Value::Bulk(Some(b"FLINTNS".to_vec())),
                Value::Bulk(Some(self.ns.clone())),
            ];
            if self.async_writes {
                hs_args.push(Value::Bulk(Some(b"a".to_vec())));
            }
            let mut hs = Vec::new();
            encode(&Value::Array(Some(hs_args)), &mut hs);
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
            None => topo.clusters[0]
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
            for view in &topo.clusters {
                let pairs: Vec<Vec<String>> = view
                    .routing
                    .read()
                    .map(|r| r.pairs.clone())
                    .unwrap_or_default();
                for (i, nodes) in pairs.iter().enumerate() {
                    let found = discover_master(nodes, &topo.backend_tls);
                    if let Ok(mut routing) = view.routing.write()
                        && let Some(m) = routing.masters.get_mut(i)
                    {
                        *m = found;
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(100));
            continue;
        };

        // Backends answer us in RESP3, so a reply the JSON.TYPE quirk
        // applies to arrives already nested. Peel that layer back off and
        // re-mark it, so THIS client's dialect is what decides whether it
        // goes on again.
        let renest = |v: Value| match v {
            Value::Array(Some(mut items)) if items.len() == 1 => {
                Value::Resp3Nested(Box::new(items.remove(0)))
            }
            other => other,
        };
        let nests = args
            .first()
            .is_some_and(|n| flint_resp::resp3_nests_reply(n));
        // JSON.NUMINCRBY's dialects differ in reply KIND, so the RESP2
        // spelling has to be rebuilt from the RESP3 array we just read
        // (derivable: the array holds the matches, args[2] says which
        // spelling the caller expects).
        let kind_differs = args
            .first()
            .is_some_and(|n| flint_resp::resp3_differs_in_kind(n));
        let jsonpath = args.get(2).is_some_and(|p| p.first() == Some(&b'$'));
        let repair = |v: Value| {
            if nests {
                return renest(v);
            }
            if kind_differs && !matches!(v, Value::Error(_)) {
                return Value::ByProto {
                    resp2: Box::new(flint_resp::json_numincrby_resp2(&v, jsonpath)),
                    resp3: Box::new(v),
                };
            }
            v
        };
        match backends.call(&addr, frame).map(repair) {
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
                if Instant::now() > deadline {
                    return Value::Error(e);
                }
                // A STALE-REPLICA fence (R1): the replica lost live contact
                // with the master and refuses the read. Falling back to the
                // MASTER (which is fresh) is the correct answer, not retrying
                // the same stale replica. A slot-freeze TRYAGAIN (no replica
                // in play) still waits the sub-second drain.
                if use_replica {
                    use_replica = false;
                } else {
                    std::thread::sleep(Duration::from_millis(50));
                }
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
/// Re-resolve one pair's master and write it back into the routing map.
///
/// The keyed path does this inline when a backend call fails; the fan-out
/// path did not, which meant a promotion it had not heard about looked
/// exactly like a permanently dead node. Shared so the two cannot drift
/// again.
fn refresh_pair_master(view: &ClusterView, idx: usize, topo: &Topology) -> Option<String> {
    let nodes = view
        .routing
        .read()
        .ok()
        .and_then(|r| r.pairs.get(idx).cloned())
        .unwrap_or_default();
    let found = discover_master(&nodes, &topo.backend_tls);
    if let Ok(mut routing) = view.routing.write()
        && let Some(m) = routing.masters.get_mut(idx)
    {
        m.clone_from(&found);
    }
    found
}

fn fan_out(
    topo: &Topology,
    backends: &mut Backends,
    frame: &[u8],
    combine: impl Fn(Vec<Value>) -> Value,
) -> Value {
    // Fan across every member cluster's masters (one cluster today; the
    // ADR-0007 O(clusters x pairs) broadcast shape).
    //
    // A pair whose master is unreachable — or not known yet — is REDISCOVERED
    // rather than failed. Without this, one failover left DBSIZE, FLUSHALL
    // and SCAN pointed at the dead node forever: keyed traffic healed itself
    // and these did not, so the proxy went on insisting the cluster was
    // broken long after it had recovered. Found on an 8-pair chaos run.
    let mut replies = Vec::new();
    for view in &topo.clusters {
        let (pair_count, mut masters) = match view.routing.read() {
            Ok(r) => (r.pairs.len(), r.masters.clone()),
            Err(_) => return Value::Error("ERR topology lock".into()),
        };
        masters.resize(pair_count, None);
        for (idx, addr) in masters.into_iter().enumerate() {
            // Try what we believe, then what is actually true.
            let mut attempt = addr;
            let mut refreshed = false;
            loop {
                match attempt {
                    Some(a) => match backends.call(&a, frame) {
                        Ok(v) => {
                            replies.push(v);
                            break;
                        }
                        Err(e) => {
                            if refreshed {
                                return Value::Error(format!("ERR fan-out to {a}: {e}"));
                            }
                            backends.drop_conn(&a);
                            attempt = refresh_pair_master(view, idx, topo);
                            refreshed = true;
                        }
                    },
                    None => {
                        if refreshed {
                            return Value::Error(format!("ERR pair {idx} has no reachable master"));
                        }
                        attempt = refresh_pair_master(view, idx, topo);
                        refreshed = true;
                    }
                }
            }
        }
    }
    combine(replies)
}

/// Commands the proxy must NEVER relay to a backend, in any connection
/// state — authenticated or not, inside a transaction or not.
///
/// `FLINT*` is the node's INTERNAL admin surface. The data port trusts its
/// callers about the namespace (`FLINTNS <ns>` is how the proxy itself pins
/// a backend connection), so relaying one from a tenant hands that tenant
/// the ability to re-point its connection at any namespace it can name.
///
/// Uppercased first: the wire is case-insensitive and `flintns` is the same
/// command.
fn is_internal_only(name: &[u8]) -> bool {
    name.to_ascii_uppercase().starts_with(b"FLINT")
}

/// Outcome of the per-command auth check.
enum AuthStep {
    /// Answer the client directly (AUTH result, NOAUTH, WRONGPASS).
    Reply(Value),
    /// Authorized: proceed in this namespace.
    Proceed(Vec<u8>),
    /// `PROXYCHAN` opened a single-use channel (ADR-0010 D2). The namespace is
    /// already pinned into `authed_ns`; this carries the grant's deadline so
    /// the connection loop closes the channel once it outlives it.
    ChannelOpen(Instant),
}

/// Redis-shaped auth gate. AUTH <token> or AUTH <user> <token> (the user is
/// ignored; the token alone identifies the tenant). Before auth, everything
/// except AUTH/QUIT gets -NOAUTH. A successful AUTH fixes the connection's
/// namespace; re-AUTH to a different tenant is rejected (reconnect instead)
/// so the backend-connection namespace pinning can never go stale.
#[allow(clippy::too_many_arguments)]
fn auth_step(
    topo: &Topology,
    authed_ns: &mut Option<Vec<u8>>,
    replica_reads: &mut bool,
    local_cache: &mut bool,
    async_writes: &mut bool,
    is_admin: &mut bool,
    proto: &mut flint_resp::Proto,
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
    // ADR-0010 D2 step 2. PROXYCHANMINT mints a single-use channel token for
    // (namespace, budget, deadline); PROXYCHAN consumes one to open a channel
    // connection pinned to that namespace. In production step 3 mints
    // internally when it admits a family command — this admin command is the
    // ops/test entry to the same table until then. Both are PROXY*, NOT FLINT*,
    // precisely so they reach this arm: the #151 guard refuses every FLINT*
    // above auth, which is why the opener could not keep its FLINTCHAN name.
    if name.as_deref() == Some(b"PROXYCHANMINT") {
        if admin_locked {
            return admin_denied();
        }
        // PROXYCHANMINT <namespace> <budget> <deadline-ms>
        let Some([ns, budget_a, deadline_a]) = args.get(1..4) else {
            return AuthStep::Reply(Value::Error(
                "ERR usage: PROXYCHANMINT <namespace> <budget> <deadline-ms>".into(),
            ));
        };
        let Some(budget) = std::str::from_utf8(budget_a).ok().and_then(|s| s.parse::<u64>().ok())
        else {
            return AuthStep::Reply(Value::Error("ERR budget must be an integer".into()));
        };
        let Some(deadline_ms) = std::str::from_utf8(deadline_a)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        else {
            return AuthStep::Reply(Value::Error("ERR deadline-ms must be an integer".into()));
        };
        let token = flint_tls::mint_token();
        let deadline = Instant::now() + std::time::Duration::from_millis(deadline_ms);
        let Ok(mut tokens) = topo.channel_tokens.lock() else {
            return AuthStep::Reply(Value::Error("ERR channel token lock".into()));
        };
        // Sweep dead entries first, so the table cannot grow without bound.
        let now = Instant::now();
        tokens.retain(|_, g| !g.used && g.deadline > now);
        tokens.insert(
            token.clone(),
            ChannelGrant {
                ns: ns.clone(),
                budget,
                deadline,
                used: false,
            },
        );
        return AuthStep::Reply(Value::Bulk(Some(token.into_bytes())));
    }
    if name.as_deref() == Some(b"PROXYCHAN") {
        // Opened on a FRESH connection: a channel must never be layerable onto
        // an already-authed tenant session, or a tenant holding a leaked token
        // could re-point itself at the grant's namespace.
        if authed_ns.is_some() || *is_admin {
            return AuthStep::Reply(Value::Error(
                "ERR PROXYCHAN must open a channel on a fresh connection".into(),
            ));
        }
        let Some(tok_arg) = args.get(1) else {
            return AuthStep::Reply(Value::Error("ERR usage: PROXYCHAN <token>".into()));
        };
        let token = String::from_utf8_lossy(tok_arg).into_owned();
        let Ok(mut tokens) = topo.channel_tokens.lock() else {
            return AuthStep::Reply(Value::Error("ERR channel token lock".into()));
        };
        // Decide from an immutable read, THEN mutate — no borrow held across
        // the remove/mark.
        enum V {
            Invalid,
            Used,
            Expired,
            Open(Vec<u8>, Instant),
        }
        let verdict = match tokens.get(&token) {
            None => V::Invalid,
            Some(g) if g.used => V::Used,
            Some(g) if Instant::now() >= g.deadline => V::Expired,
            Some(g) => V::Open(g.ns.clone(), g.deadline),
        };
        return match verdict {
            V::Invalid => AuthStep::Reply(Value::Error("WRONGPASS invalid channel token".into())),
            V::Used => AuthStep::Reply(Value::Error("ERR channel token already used".into())),
            V::Expired => {
                tokens.remove(&token);
                AuthStep::Reply(Value::Error("ERR channel token expired".into()))
            }
            V::Open(ns, deadline) => {
                // Single-use: consume on open, so a second PROXYCHAN finds it used.
                if let Some(g) = tokens.get_mut(&token) {
                    g.used = true;
                }
                *authed_ns = Some(ns);
                AuthStep::ChannelOpen(deadline)
            }
        };
    }
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
        let moved_learned = topo.stat_moved_learned.load(Ordering::Relaxed);
        let quota_write_shed = topo.stat_quota_write_shed_total.load(Ordering::Relaxed);
        // build: FIRST (ADR-0014 D1). The edge is rolled by `flintctl
        // upgrade` like everything else, and until now carried no stamp at
        // all — so a half-completed edge roll looked exactly like a
        // finished one.
        let info = format!(
            "build:{build}\r\nactive:{}\r\nconns_total:{}\r\nshed_total:{}\r\nauth_ok_total:{}\r\nauth_fail_total:{}\r\ncommands_total:{}\r\ncommands_read_total:{}\r\ncommands_write_total:{}\r\nhotkey_sample_rate:{}\r\ncache_ttl_ms:{cache_ttl}\r\ncache_max_bytes:{cache_max}\r\ncache_hits_total:{cache_hits}\r\ncache_misses_total:{cache_misses}\r\ncache_entries:{cache_entries}\r\ncache_bytes:{cache_bytes}\r\nmoved_learned_total:{moved_learned}\r\nquota_throttled_total:{quota_throttled}\r\nquota_write_shed_total:{quota_write_shed}\r\ncert_days_remaining:{cdr}\r\n",
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
            build = build_version(),
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
    // Per-tenant error counts by kind. Same scoping as PROXYLATENCY and
    // for the same reason: an error profile is tenant data — it exposes
    // their client's health, and via QUOTA their commercial limits.
    if name.as_deref() == Some(b"PROXYERRORS") {
        let scope = authed_ns.as_deref().filter(|_| !topo.open_mode);
        if scope.is_none() && admin_locked {
            return admin_denied();
        }
        return AuthStep::Reply(Value::Bulk(Some(topo.errors.report(scope).into_bytes())));
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
    // HELLO, before the auth gate on purpose: redis-py 8 and node-redis
    // carry their credentials INSIDE it (`HELLO 3 AUTH default <token>`)
    // and never send a separate AUTH. Answering -NOAUTH here is what made
    // Flint unreachable from the entire modern Python client ecosystem.
    if name.as_deref() == Some(b"HELLO") {
        let req = match flint_resp::parse_hello(args) {
            Ok(r) => r,
            Err(e) => return AuthStep::Reply(e.reply()),
        };
        if let Some((_user, token)) = req.auth.clone() {
            // The username is ignored, exactly as our AUTH arm ignores it:
            // a Flint token IS the identity. Reuse that arm so inline and
            // standalone credentials can never diverge.
            let auth = vec![b"AUTH".to_vec(), token];
            // A rejected credential must fail the HELLO, not quietly hand
            // back a successful handshake with no namespace bound.
            if let AuthStep::Reply(Value::Error(e)) = auth_step(
                topo,
                authed_ns,
                replica_reads,
                local_cache,
                async_writes,
                is_admin,
                proto,
                &auth,
            ) {
                return AuthStep::Reply(Value::Error(e));
            }
        }
        // The dialect switches only once the handshake is otherwise good,
        // so a failed HELLO leaves the connection exactly as it was.
        if let Some(p) = req.proto {
            *proto = p;
        }
        // The build, not the crate version. This is the ONLY version string
        // a client library ever reads, and it used to be the workspace
        // `0.0.1` on a fleet whose every other surface said v0.1.0-rc.37.
        let build = build_version();
        return AuthStep::Reply(flint_resp::hello_reply(
            *proto,
            flint_build::wire(&build),
            "master",
        ));
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
        *async_writes = grant.async_writes;
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
    // This client's transaction, if any (ADR-0012 D7). Per connection, like
    // the backends themselves — a transaction is connection state on the
    // node, so it can only ever belong to one client.
    let mut txn = ProxyTxn::default();
    // Replica-read opt-in for THIS connection's tenant (D7), set at AUTH.
    let mut replica_reads = false;
    // Proxy near-cache opt-in for THIS connection's tenant (D6), set at AUTH.
    let mut local_cache = false;
    // Async write-queue opt-in for THIS connection's tenant (D4), set at
    // AUTH; carried onto backend pins at dial.
    let mut async_writes = false;
    // Admin session (AUTH <admin-token>): unlocks the operator surface only.
    let mut is_admin = false;
    // Set once `PROXYCHAN` opens a channel (ADR-0010 D2): the grant's deadline.
    // A channel that outlives it is closed on its next command — the token
    // dies with the family command it was minted for. `None` for tenants.
    let mut channel_deadline: Option<Instant> = None;
    // The RESP dialect THIS client negotiated, set by HELLO. Backends
    // always answer us in RESP3 so their replies keep their types; this is
    // what we downgrade to on the way back out.
    let mut proto = flint_resp::Proto::default();
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
            // Inline commands (`SET k v\r\n`), the same shape the server
            // already accepts. The proxy used to answer -ERR Protocol error
            // and CLOSE the connection, which broke two ordinary things:
            // `redis-cli --pipe`, the bulk-import path Redis's own
            // mass-insert docs tell you to use, and plain `nc host port`
            // debugging. The --pipe case failed silently — it reported "All
            // data transferred" while nothing had landed.
            //
            // Rewritten into a RESP frame and pushed through the identical
            // auth, quota and routing path, so inline callers get no special
            // treatment beyond the framing.
            if let Some(&first) = buf[consumed..].first()
                && first != b'*'
            {
                let pending = &buf[consumed..];
                let Some(nl) = pending.iter().position(|&b| b == b'\n') else {
                    if pending.len() > MAX_INLINE_LEN {
                        encode(
                            &Value::Error("ERR Protocol error: too big inline request".into()),
                            &mut out,
                        );
                        stream.write_all(&out)?;
                        return Ok(());
                    }
                    break;
                };
                let line = pending[..nl].strip_suffix(b"\r").unwrap_or(&pending[..nl]);
                let args: Vec<Vec<u8>> = line
                    .split(|&b| b == b' ')
                    .filter(|p| !p.is_empty())
                    .map(|p| p.to_vec())
                    .collect();
                let mut resp = Vec::new();
                if !args.is_empty() {
                    encode(
                        &Value::Array(Some(
                            args.into_iter().map(|a| Value::Bulk(Some(a))).collect(),
                        )),
                        &mut resp,
                    );
                }
                // Splice the RESP form in place of the inline bytes, then
                // fall through: one dispatch path, no duplicated auth,
                // quota, routing or latency accounting. An empty line
                // (`\r\n` alone, which redis-cli sends on a bare Enter)
                // splices to nothing and is simply skipped.
                buf.splice(consumed..consumed + nl + 1, resp);
                continue;
            }
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
                    // A channel (PROXYCHAN) dies with the family command's
                    // deadline: past it the connection is closed on its next
                    // command rather than served (ADR-0010 D2). Checked after
                    // decode and before any auth or forward, so an expired
                    // channel cannot issue even one more command. `None` for
                    // tenants — their path is untouched.
                    if let Some(dl) = channel_deadline
                        && Instant::now() >= dl
                    {
                        encode_proto(
                            &Value::Error("ERR channel deadline exceeded".into()),
                            proto,
                            &mut out,
                        );
                        stream.write_all(&out)?;
                        return Ok(());
                    }
                    // Tenant-perceived latency starts here: everything the
                    // proxy does for this command — cache lookup, routing,
                    // backend round trip, MOVED/failover retries — is what
                    // the client waits for.
                    let started = Instant::now();
                    // REFUSED HERE, BEFORE ANYTHING CAN FORWARD IT.
                    //
                    // The per-command match further down also refuses
                    // `FLINT*`, but only on the path that reaches it. Inside
                    // a MULTI, `transaction_step` relays the raw bytes to the
                    // backend and returns before that match is consulted — so
                    // the refusal held outside a transaction and not inside
                    // one. A tenant could open MULTI, send `FLINTNS <other>`,
                    // and re-point its own backend connection at another
                    // tenant's namespace: full read and write access to
                    // anyone whose namespace name they could guess.
                    //
                    // The property wanted is "these bytes never leave the
                    // proxy", and that is only true if it is checked before
                    // any code that could forward them. Checked pre-auth
                    // deliberately: an unauthenticated connection has no more
                    // business naming a namespace than an authenticated one.
                    let reply = if args.first().is_some_and(|n| is_internal_only(n)) {
                        Value::Error(
                            "ERR admin commands are not available through the proxy".into(),
                        )
                    } else {
                        match auth_step(
                            &topo,
                            &mut authed_ns,
                            &mut replica_reads,
                            &mut local_cache,
                            &mut async_writes,
                            &mut is_admin,
                            &mut proto,
                            &args,
                        ) {
                            AuthStep::Reply(v) => v,
                            AuthStep::ChannelOpen(deadline) => {
                                // PROXYCHAN pinned authed_ns to the grant's
                                // namespace; record the deadline so this
                                // connection is closed once it outlives it.
                                channel_deadline = Some(deadline);
                                Value::Simple("OK".into())
                            }
                            AuthStep::Proceed(ns) => data_command(
                                &topo,
                                &mut backends,
                                &mut txn,
                                &ns,
                                &args,
                                &raw,
                                replica_reads,
                                local_cache,
                                async_writes,
                            ),
                        }
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
                    // Every error a client is told about passes here —
                    // the same funnel the latency histogram uses. Counting
                    // at the ~20 construction sites instead would leave
                    // whichever one is added next uncounted, silently.
                    if let Value::Error(msg) = &reply {
                        topo.errors
                            .observe(authed_ns.as_deref(), errors::classify(msg));
                    }
                    encode_proto(&reply, proto, &mut out);
                    if out.len() >= OUT_FLUSH_THRESHOLD {
                        stream.write_all(&out)?;
                        out.clear();
                    }
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
        if buf.len() > MAX_QUERY_BUF {
            out.clear();
            encode(
                &Value::Error("ERR Protocol error: query buffer limit exceeded".into()),
                &mut out,
            );
            let _ = stream.write_all(&out);
            return Ok(());
        }
    }
}

/// One authorized tenant command, end to end: the D6 near-cache in front
/// (lookup, fill, write-invalidation), D7 replica routing, and the backend
/// forward. Extracted from `serve_client` so the connection loop stays a
/// decode/auth/encode skeleton.
#[allow(clippy::too_many_arguments)]
/// One client's transaction, as the proxy sees it (ADR-0012 D7).
///
/// The proxy's normal instinct is to REPAIR a failed command — chase a
/// MOVED, wait out a TRYAGAIN, re-dial a dead backend, fall back to the
/// master. Every one of those is right for a single command and wrong
/// inside a transaction, because the queue lives on one backend CONNECTION:
/// retrying elsewhere, or on a fresh connection to the same address, lands
/// the command on a node with no MULTI open, which EXECUTES it instead of
/// queueing it. The client would see QUEUED and get a partial apply.
#[derive(Default)]
struct ProxyTxn {
    /// The client has sent MULTI and neither EXEC nor DISCARD yet.
    open: bool,
    /// The backend this transaction is bound to. Set by WATCH or by the
    /// first keyed command — NOT by MULTI, which carries no key and so
    /// cannot name a slot.
    addr: Option<String>,
    /// MULTI has been forwarded to `addr`. Deferred because the proxy
    /// cannot know which backend to open it on until a key appears.
    opened: bool,
}

impl ProxyTxn {
    fn reset(&mut self) {
        self.open = false;
        self.addr = None;
        self.opened = false;
    }
}

/// Abort a transaction and make it unrecoverable.
///
/// Dropping the backend connection is the point, not housekeeping: the
/// node's queue and watches are connection state, so closing the socket is
/// what guarantees a later EXEC cannot apply the half-built transaction.
fn abort_txn(backends: &mut Backends, txn: &mut ProxyTxn, why: &str) -> Value {
    if let Some(addr) = txn.addr.clone() {
        backends.drop_conn(&addr);
    }
    txn.reset();
    Value::Error(format!(
        "EXECABORT Transaction discarded: {why}. Retry the transaction."
    ))
}

/// Send one frame to a pinned backend with NO retry and no rerouting.
fn call_pinned(backends: &mut Backends, addr: &str, frame: &[u8]) -> Result<Value, String> {
    match backends.call(addr, frame) {
        Ok(Value::Error(e))
            if e.starts_with("MOVED ")
                || e.starts_with("TRYAGAIN")
                || e.starts_with("READONLY") =>
        {
            // Each of these means "this is no longer the right node, go
            // again". Outside a transaction the proxy absorbs them; here
            // going again is precisely what it must not do.
            Err(format!("backend no longer owns this transaction ({e})"))
        }
        Ok(v) => Ok(v),
        Err(e) => Err(format!("backend unavailable ({e})")),
    }
}

fn encode_cmd(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    encode(
        &Value::Array(Some(
            parts
                .iter()
                .map(|p| Value::Bulk(Some(p.to_vec())))
                .collect(),
        )),
        &mut out,
    );
    out
}

/// Handle a command that is transaction business. `None` means it is not,
/// and the caller should carry on with normal routing.
fn transaction_step(
    topo: &Topology,
    backends: &mut Backends,
    txn: &mut ProxyTxn,
    ns: &[u8],
    args: &[Vec<u8>],
    raw: &[u8],
) -> Option<Value> {
    let name = args.first()?.to_ascii_uppercase();
    // Where a command must go, if it names a key at all.
    let routed = route_key(args)
        .map(slot_for_key)
        .and_then(|s| topo.route(ns, s));

    match name.as_slice() {
        b"WATCH" => {
            // WATCH precedes MULTI and carries keys, so it is what usually
            // fixes the backend. Binding here matters: the node keeps
            // watches per CONNECTION, so the transaction must later run on
            // the very connection that armed them.
            if txn.open {
                return Some(Value::Error(
                    "ERR Command 'watch' not allowed inside a transaction".into(),
                ));
            }
            let addr = routed?;
            match call_pinned(backends, &addr, raw) {
                Ok(v) => {
                    txn.addr = Some(addr);
                    Some(v)
                }
                Err(why) => Some(abort_txn(backends, txn, &why)),
            }
        }
        b"UNWATCH" => {
            let Some(addr) = txn.addr.clone() else {
                // Nothing was ever watched through this proxy connection.
                return Some(Value::Simple("OK".into()));
            };
            let reply = match call_pinned(backends, &addr, raw) {
                Ok(v) => v,
                Err(why) => return Some(abort_txn(backends, txn, &why)),
            };
            if !txn.open {
                txn.addr = None;
            }
            Some(reply)
        }
        b"MULTI" => {
            if txn.open {
                return Some(Value::Error(
                    "ERR Command 'multi' not allowed inside a transaction".into(),
                ));
            }
            txn.open = true;
            // If WATCH already bound a backend, open the transaction there
            // NOW. Otherwise `opened` would stay false with node-side
            // watches armed, and EXEC/DISCARD — answered locally in that
            // state — would never tell the node to clear them, so the next
            // transaction on this connection would abort against a watch
            // the client had already discarded. Found by the differential
            // probe, which is the only place a stale watch is visible.
            if let Some(addr) = txn.addr.clone() {
                match call_pinned(backends, &addr, &encode_cmd(&[b"MULTI"])) {
                    Ok(Value::Simple(s)) if s == "OK" => txn.opened = true,
                    Ok(other) => {
                        return Some(abort_txn(
                            backends,
                            txn,
                            &format!("backend refused MULTI ({other:?})"),
                        ));
                    }
                    Err(why) => return Some(abort_txn(backends, txn, &why)),
                }
            } else {
                // No key seen yet, so no backend can be chosen: MULTI names
                // none, and opening it on whichever backend the keyless rule
                // picks would queue the data commands somewhere else. The
                // node holds nothing for this transaction until then, which
                // is what makes answering locally safe.
                txn.opened = false;
            }
            Some(Value::Simple("OK".into()))
        }
        b"DISCARD" => {
            if !txn.open {
                return Some(Value::Error("ERR DISCARD without MULTI".into()));
            }
            let pending = txn.opened.then(|| txn.addr.clone()).flatten();
            txn.reset();
            match pending {
                // Nothing reached a backend, so there is nothing to discard
                // there; the queue only ever existed in this proxy's intent.
                None => Some(Value::Simple("OK".into())),
                Some(addr) => Some(match call_pinned(backends, &addr, raw) {
                    Ok(v) => v,
                    // The connection is gone, which discarded it for us.
                    Err(_) => Value::Simple("OK".into()),
                }),
            }
        }
        b"EXEC" => {
            if !txn.open {
                return Some(Value::Error("ERR EXEC without MULTI".into()));
            }
            let pending = txn.opened.then(|| txn.addr.clone()).flatten();
            let mut ended = std::mem::take(txn);
            match pending {
                // MULTI ... EXEC with nothing in between: no backend was
                // ever chosen, and the answer is the empty array.
                None => Some(Value::Array(Some(Vec::new()))),
                Some(addr) => Some(match call_pinned(backends, &addr, raw) {
                    // RESP3 has exactly ONE null, and the backend hop always
                    // speaks it (see the HELLO 3 handshake). So the node's
                    // aborted-EXEC reply — a null ARRAY — reaches us as
                    // Value::Null, and forwarding that verbatim to a RESP2
                    // client emits `$-1`, a null BULK. Redis answers `*-1`.
                    //
                    // Re-typed here rather than anywhere upstream because
                    // this is the only place that knows the reply is EXEC's:
                    // the sole null EXEC can return is the null array, so the
                    // information RESP3 discarded is recoverable exactly here
                    // and nowhere else. Array(None) still encodes to `_` for
                    // a RESP3 client, so this costs that path nothing.
                    Ok(Value::Null) => Value::Array(None),
                    Ok(v) => v,
                    Err(why) => abort_txn(backends, &mut ended, &why),
                }),
            }
        }
        _ if txn.open => {
            // A command to queue. Bind the backend if this is the first one
            // that names a key; otherwise fall back to the keyless rule.
            let addr = match (txn.addr.clone(), routed) {
                (Some(pinned), Some(target)) if pinned != target => {
                    // The transaction is bound to one node and this key
                    // lives on another. Rare — it needs a keyless command
                    // to have bound the transaction first — but executing
                    // it would write rows on a node that does not own the
                    // slot, where nothing will ever read them.
                    return Some(abort_txn(
                        backends,
                        txn,
                        "a key in this transaction belongs to a different shard",
                    ));
                }
                (Some(pinned), _) => pinned,
                (None, Some(target)) => target,
                (None, None) => topo.clusters[0]
                    .routing
                    .read()
                    .ok()
                    .and_then(|r| r.masters.first().cloned().flatten())?,
            };
            txn.addr = Some(addr.clone());
            // Open the transaction on the backend now that one is known.
            if !txn.opened {
                match call_pinned(backends, &addr, &encode_cmd(&[b"MULTI"])) {
                    Ok(Value::Simple(s)) if s == "OK" => txn.opened = true,
                    Ok(other) => {
                        return Some(abort_txn(
                            backends,
                            txn,
                            &format!("backend refused MULTI ({other:?})"),
                        ));
                    }
                    Err(why) => return Some(abort_txn(backends, txn, &why)),
                }
            }
            match call_pinned(backends, &addr, raw) {
                Ok(v) => Some(v),
                Err(why) => Some(abort_txn(backends, txn, &why)),
            }
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn data_command(
    topo: &Topology,
    backends: &mut Option<Backends>,
    txn: &mut ProxyTxn,
    ns: &[u8],
    args: &[Vec<u8>],
    raw: &[u8],
    replica_reads: bool,
    local_cache: bool,
    async_writes: bool,
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
    // Transactions come BEFORE the near-cache and the replica decision, and
    // that order is the point (ADR-0012 D7). A queued command answered from
    // the cache would never reach the node's queue, so EXEC would silently
    // skip it; a read routed to a replica would queue on a different
    // connection from the writes. Both are ruled out by handling the
    // transaction here and returning.
    {
        let b = backends.get_or_insert_with(|| {
            Backends::new(ns.to_vec(), topo.backend_tls.clone(), async_writes)
        });
        if let Some(reply) = transaction_step(topo, b, txn, ns, args, raw) {
            return reply;
        }
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
    let b = backends
        .get_or_insert_with(|| Backends::new(ns.to_vec(), topo.backend_tls.clone(), async_writes));
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
            // COPY src dst [REPLACE]: the DESTINATION is the key that
            // changed. The default arm below drops args[1], which for COPY
            // is the SOURCE — nothing wrote it, while a cached destination
            // would go on serving its pre-copy value through this proxy.
            b"COPY" => {
                if let Some(k) = args.get(2) {
                    topo.cache.invalidate(ns, k);
                }
            }
            // RENAME / RENAMENX: BOTH keys change — the source ceases to
            // exist and the destination takes its value. Dropping only one
            // leaves the other answering from before the rename, so a
            // cached source would resurrect a key that is now gone.
            b"RENAME" | b"RENAMENX" => {
                for k in args[1..].iter().take(2) {
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

/// One in-flight keyspace SCAN session THROUGH this proxy: which master
/// the session is currently draining and that master's own cursor. The
/// proxy iterates masters one at a time (a node's cursor table lives on
/// that node, so a session must not bounce between nodes — SCAN is pinned
/// to masters and never takes the replica-read path). Bounded + TTL'd like
/// the node table; namespace-scoped like the node table.
struct ProxyScanCursor {
    ns: Vec<u8>,
    master_idx: usize,
    master: String,
    node_cursor: u64,
    at: std::time::Instant,
}

const PSCAN_TTL: std::time::Duration = std::time::Duration::from_secs(120);
const PSCAN_CAP: usize = 1024;
static NEXT_PSCAN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn pscan_cursors() -> &'static Mutex<HashMap<u64, ProxyScanCursor>> {
    static TABLE: std::sync::OnceLock<Mutex<HashMap<u64, ProxyScanCursor>>> =
        std::sync::OnceLock::new();
    TABLE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// SCAN through the proxy: drain master 0's keyspace via its own SCAN,
/// then master 1's, … — the client sees ONE cursor stream over the whole
/// group. MATCH/COUNT/TYPE pass through verbatim; only the cursor argument
/// is rewritten per hop. Failover mid-scan invalidates the session (the
/// dead master held its cursor state): the client gets "ERR invalid
/// cursor" and restarts — Redis's weak scan guarantee, stated honestly.
fn scan_forward(topo: &Topology, backends: &mut Backends, ns: &[u8], args: &[Vec<u8>]) -> Value {
    if args.len() < 2 {
        return Value::Error("ERR wrong number of arguments for 'scan' command".into());
    }
    let Some(cursor_in) = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    else {
        return Value::Error("ERR invalid cursor".into());
    };
    // Masters, in pair order (the fan_out collection shape).
    let mut masters: Vec<Option<String>> = Vec::new();
    for view in &topo.clusters {
        match view.routing.read() {
            Ok(r) => masters.extend(r.masters.clone()),
            Err(_) => return Value::Error("ERR topology lock".into()),
        }
    }
    let (master_idx, master, node_cursor) = if cursor_in == 0 {
        match masters.first() {
            Some(Some(m)) => (0usize, m.clone(), 0u64),
            _ => return Value::Error("ERR a pair has no reachable master".into()),
        }
    } else {
        let Ok(mut map) = pscan_cursors().lock() else {
            return Value::Error("ERR cursor table lock".into());
        };
        let state = match map.get(&cursor_in) {
            Some(c) if c.ns == ns => (c.master_idx, c.master.clone(), c.node_cursor),
            _ => return Value::Error("ERR invalid cursor".into()),
        };
        // Topology moved under the session (failover, expansion re-slotting
        // the index): the recorded master no longer sits at the recorded
        // seat. Its node-side cursor died with it — retire the session and
        // tell the client honestly rather than resume against a different
        // node's keyspace.
        if masters.get(state.0).and_then(|m| m.as_ref()) != Some(&state.1) {
            map.remove(&cursor_in);
            return Value::Error("ERR invalid cursor".into());
        }
        state
    };
    // Forward with only the cursor rewritten.
    let mut fwd = args.to_vec();
    fwd[1] = node_cursor.to_string().into_bytes();
    let frame = Value::Array(Some(
        fwd.into_iter().map(|a| Value::Bulk(Some(a))).collect(),
    ));
    let mut out = Vec::new();
    encode(&frame, &mut out);
    let reply = match backends.call(&master, &out) {
        Ok(v) => v,
        Err(e) => return Value::Error(format!("ERR scan forward to {master}: {e}")),
    };
    let Value::Array(Some(mut parts)) = reply else {
        return match reply {
            Value::Error(e) => Value::Error(e),
            other => Value::Error(format!("ERR scan reply shape: {other:?}")),
        };
    };
    if parts.len() != 2 {
        return Value::Error("ERR scan reply shape".into());
    }
    let keys = parts.pop().unwrap_or(Value::Array(Some(Vec::new())));
    let node_next: u64 = match &parts[0] {
        Value::Bulk(Some(c)) => match std::str::from_utf8(c).ok().and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return Value::Error("ERR scan reply cursor".into()),
        },
        _ => return Value::Error("ERR scan reply shape".into()),
    };
    // Next session state: same master continues, or advance to the next
    // pair's master with a fresh node cursor, or the group is exhausted.
    let next_state = if node_next != 0 {
        Some((master_idx, master, node_next))
    } else {
        masters
            .get(master_idx + 1)
            .and_then(|m| m.clone().map(|m| (master_idx + 1, m, 0u64)))
    };
    let Ok(mut map) = pscan_cursors().lock() else {
        return Value::Error("ERR cursor table lock".into());
    };
    let proxy_next = match next_state {
        Some((idx, m, nc)) => {
            let now = std::time::Instant::now();
            map.retain(|_, c| now.duration_since(c.at) < PSCAN_TTL);
            let id = if cursor_in != 0 {
                cursor_in
            } else {
                if map.len() >= PSCAN_CAP
                    && let Some((&oldest, _)) = map.iter().min_by_key(|(_, c)| c.at)
                {
                    map.remove(&oldest);
                }
                NEXT_PSCAN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            };
            map.insert(
                id,
                ProxyScanCursor {
                    ns: ns.to_vec(),
                    master_idx: idx,
                    master: m,
                    node_cursor: nc,
                    at: now,
                },
            );
            id
        }
        None => {
            map.remove(&cursor_in);
            0
        }
    };
    Value::Array(Some(vec![
        Value::Bulk(Some(proxy_next.to_string().into_bytes())),
        keys,
    ]))
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
        // Keyspace iteration: one client cursor stream over every pair's
        // master, in pair order. Never the replica-read path (a node's
        // cursor state lives on the node that minted it).
        b"SCAN" => scan_forward(topo, backends, ns, args),
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
    let mut stream = flint_tls::connect_reloadable(cp, &topo.backend_tls)?;
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
                    // Option B: element 5 = slot-ownership exceptions;
                    // pre-Option-B CPs omit it (empty = none).
                    let exc = match items.get(5) {
                        Some(Value::Bulk(Some(e))) => String::from_utf8_lossy(e).to_string(),
                        _ => String::new(),
                    };
                    // #91: element 6 = the controller's promotion hint
                    // ("<addr>|<gen>"); CPs without it omit it, and a proxy
                    // that never sees one keeps the reactive path unchanged.
                    let promo = match items.get(6) {
                        Some(Value::Bulk(Some(p))) => String::from_utf8_lossy(p).to_string(),
                        _ => String::new(),
                    };
                    topo.apply_snapshot(
                        &String::from_utf8_lossy(pairs),
                        &String::from_utf8_lossy(tenants),
                        &admin,
                        &exc,
                    );
                    topo.apply_promote_hint(&promo);
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

/// The build stamp surfaced in PROXYSTATS (ADR-0014 D1). One definition for
/// every Flint binary; see the flint-build crate for why it is not written
/// out here.
fn build_version() -> String {
    flint_build::version(env!("CARGO_PKG_VERSION"))
}

fn main() -> std::io::Result<()> {
    // Ask the binary what it is, without starting it. Same flag surface as
    // flint-server. Until ADR-0014 D1 the proxy carried no stamp at all, so
    // `flintctl upgrade` rolled the edge with no way to check what landed —
    // a half-completed edge roll was indistinguishable from a finished one.
    if std::env::args().any(|a| a == "--build-version") {
        println!("{}", build_version());
        return Ok(());
    }
    let port: u16 = arg("--port").and_then(|p| p.parse().ok()).unwrap_or(7379);
    // A comma-list is the SEATS of one cluster's Raft CP, tried in order —
    // any seat serves CPWATCH from its local applied registry, so the proxy
    // rotates on failure rather than starving when its pinned seat dies.
    // (Federation, ADR-0007, is a list of CLUSTERS and remains unwired;
    // that will need its own syntax precisely because this one now means
    // seats.)
    let control_planes: Vec<String> = arg("--control-plane")
        .map(|v| v.split(',').map(str::trim).map(String::from).collect())
        .unwrap_or_default();
    let control_plane = control_planes.first().cloned();

    // Client-facing TLS termination: both flags together enable it, neither
    // keeps the plaintext listener (byte-identical to pre-TLS). One without
    // the other is a config error, not a silent downgrade.
    // Hot-reloading (ADR-0006 D4 follow-on): each new client connection
    // snapshots the current edge cert, so rotation needs no restart.
    let tls: Option<Arc<flint_tls::ReloadableServerConfig>> =
        match (arg("--tls-cert"), arg("--tls-key")) {
            (Some(cert), Some(key)) => Some(
                flint_tls::ReloadableServerConfig::watch_edge(&cert, &key)
                    .expect("client-facing TLS config"),
            ),
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
    let backend_tls: Option<Arc<flint_tls::ReloadableClientConfig>> = match (
        arg("--internal-ca"),
        arg("--internal-cert"),
        arg("--internal-key"),
    ) {
        (Some(ca), Some(cert), Some(key)) => Some(
            flint_tls::ReloadableClientConfig::watch(&ca, &cert, &key)
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
        clusters: vec![ClusterView {
            routing: RwLock::new(Routing {
                pairs,
                masters,
                ranges: Vec::new(),
            }),
            moved: RwLock::new(HashMap::new()),
            exceptions: RwLock::new(HashMap::new()),
        }],
        level0: RwLock::new(flint_slot::SlotIntervals::single(0)),
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
        stat_moved_learned: std::sync::atomic::AtomicU64::new(0),
        stat_auth_ok_total: std::sync::atomic::AtomicU64::new(0),
        stat_auth_fail_total: std::sync::atomic::AtomicU64::new(0),
        stat_commands_total: std::sync::atomic::AtomicU64::new(0),
        stat_commands_read_total: std::sync::atomic::AtomicU64::new(0),
        stat_commands_write_total: std::sync::atomic::AtomicU64::new(0),
        stat_active: AtomicUsize::new(0),
        replica_rr: AtomicUsize::new(0),
        hotkeys: HotKeySketch::new(),
        latency: latency::LatencyHistograms::default(),
        errors: errors::ErrorCounts::default(),
        // D6 near-cache defaults: TTL 300 ms, budget 256 MB (Jeff,
        // 2026-07-20). ON by default at the proxy — but caching still
        // requires the tenant's 'c' opt-in per request, so an un-opted
        // fleet caches nothing. Both knobs stay runtime-settable via
        // PROXYCACHE (0 TTL disables and clears); the budget is bounded —
        // this is a hot-spot absorber, not a data tier.
        cache: cache::ProxyCache::new(
            arg("--cache-ttl-ms")
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            arg("--cache-max-bytes")
                .and_then(|v| v.parse().ok())
                .unwrap_or(256 * 1024 * 1024),
        ),
        open_mode,
        backend_tls,
        cert_path: arg("--internal-cert"),
        last_promote_hint: RwLock::new(String::new()),
        channel_tokens: std::sync::Mutex::new(HashMap::new()),
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
    if control_plane.is_some() {
        let advertise = arg("--advertise").expect("--control-plane requires --advertise <addr>");
        let seats = control_planes.clone();
        let topo = Arc::clone(&topo);
        std::thread::spawn(move || {
            let mut last_version: u64 = 0;
            // ROTATE ON FAILURE. With one seat this is the old loop exactly.
            // With three, a watch that dies moves to the next seat — the
            // failure this closes: the drill killed the seat this proxy was
            // pinned to, the quorum stayed healthy, and a new tenant's
            // snapshot was never delivered because the proxy sat reconnecting
            // to a corpse. Version tracking spans seats (any seat's applied
            // registry carries the same versions), so rotation never replays.
            let mut i = 0usize;
            loop {
                let cp = &seats[i % seats.len()];
                if let Err(e) = watch_control_plane(cp, &advertise, &topo, &mut last_version) {
                    eprintln!("control-plane watch ({cp}): {e}; trying next seat");
                    i += 1;
                }
                std::thread::sleep(Duration::from_millis(1000));
            }
        });
    }

    let max_conns: usize = arg("--max-conns")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_CONNS);

    // --bind: the listener address. Loopback by default; 0.0.0.0 for a
    // front door serving external clients (the marketplace single-VM shape).
    let bind = arg("--bind").unwrap_or_else(|| "127.0.0.1".into());
    let listener = TcpListener::bind((bind.as_str(), port))?;
    eprintln!(
        "flint-proxy listening on {bind}:{port} ({}, max-conns {max_conns})",
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
            match tls.as_ref().and_then(|r| r.current()) {
                Some(cfg) => {
                    // The handshake runs lazily on the first read/write inside
                    // serve_client (the client sends the first command). A
                    // plaintext client hitting the TLS port fails the handshake
                    // and the connection drops — no RESP is ever processed.
                    // The config is snapshotted per connection, so a rotated
                    // edge cert applies to the next accept with no restart.
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

#[cfg(test)]
mod route_tests {
    use super::*;

    /// A minimal Topology: one cluster view with the given pairs/masters/
    /// ranges, everything else inert. The routing logic under test reads
    /// only clusters/level0.
    fn topo(pairs: Vec<Vec<String>>, masters: Vec<Option<String>>) -> Topology {
        Topology {
            clusters: vec![ClusterView {
                routing: RwLock::new(Routing {
                    pairs,
                    masters,
                    ranges: Vec::new(),
                }),
                moved: RwLock::new(HashMap::new()),
                exceptions: RwLock::new(HashMap::new()),
            }],
            level0: RwLock::new(flint_slot::SlotIntervals::single(0)),
            tenants: RwLock::new(HashMap::new()),
            replica_rr: AtomicUsize::new(0),
            auth_counts: RwLock::new(HashMap::new()),
            stat_conns_total: std::sync::atomic::AtomicU64::new(0),
            stat_shed_total: std::sync::atomic::AtomicU64::new(0),
            stat_moved_learned: std::sync::atomic::AtomicU64::new(0),
            stat_auth_ok_total: std::sync::atomic::AtomicU64::new(0),
            stat_auth_fail_total: std::sync::atomic::AtomicU64::new(0),
            stat_commands_total: std::sync::atomic::AtomicU64::new(0),
            stat_commands_read_total: std::sync::atomic::AtomicU64::new(0),
            stat_commands_write_total: std::sync::atomic::AtomicU64::new(0),
            stat_active: AtomicUsize::new(0),
            hotkeys: HotKeySketch::new(),
            latency: latency::LatencyHistograms::default(),
            errors: errors::ErrorCounts::default(),
            quota: RwLock::new(HashMap::new()),
            buckets: std::sync::Mutex::new(HashMap::new()),
            stat_quota_throttled_total: std::sync::atomic::AtomicU64::new(0),
            stat_quota_write_shed_total: std::sync::atomic::AtomicU64::new(0),
            cache: cache::ProxyCache::new(0, 0),
            open_mode: true,
            backend_tls: None,
            cert_path: None,
            admin_digests: RwLock::new(Vec::new()),
            last_promote_hint: RwLock::new(String::new()),
            channel_tokens: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn two_pair_topo() -> Topology {
        topo(
            vec![vec!["a:1".into()], vec!["b:1".into()]],
            vec![Some("a:1".into()), Some("b:1".into())],
        )
    }

    /// Pair 0's master is "a:1", which nothing answers on in a unit test, so
    /// a re-probe necessarily resolves to None. That makes masters[0]
    /// flipping Some -> None the OBSERVABLE PROOF that rediscover_for ran —
    /// and re-seeding it before a suppressed hint proves the negative just
    /// as directly. A test that only asserted the stored hint string would
    /// pass even if the re-probe were never wired up at all.
    fn master0(t: &Topology) -> Option<String> {
        t.clusters[0].routing.read().expect("routing lock").masters[0].clone()
    }
    fn seed_master0(t: &Topology) {
        t.clusters[0].routing.write().expect("routing lock").masters[0] = Some("a:1".into());
    }

    #[test]
    fn a_changed_promote_hint_reprobes_the_pair() {
        let t = two_pair_topo();
        assert_eq!(master0(&t), Some("a:1".into()), "precondition");
        t.apply_promote_hint("a:1|1");
        assert_eq!(master0(&t), None, "a new hint must trigger a re-probe");
    }

    #[test]
    fn a_repeated_promote_hint_does_not_reprobe() {
        let t = two_pair_topo();
        t.apply_promote_hint("a:1|1");
        seed_master0(&t);
        t.apply_promote_hint("a:1|1");
        assert_eq!(
            master0(&t),
            Some("a:1".into()),
            "replaying the same hint (a re-push, or a reconnect) must not re-probe"
        );
    }

    /// The CP does not persist the hint, so after a CP restart a generation
    /// legitimately goes BACKWARDS. Comparing for ordering would ignore
    /// every subsequent hint forever, and the symptom would be a failover
    /// that is intermittently slow for reasons that look like the network.
    #[test]
    fn a_lower_generation_after_a_cp_restart_still_reprobes() {
        let t = two_pair_topo();
        t.apply_promote_hint("a:1|7");
        seed_master0(&t);
        t.apply_promote_hint("a:1|1");
        assert_eq!(master0(&t), None, "a reset generation must still re-probe");
    }

    /// A CP that predates this feature omits the field entirely; the proxy
    /// must then behave exactly as it did before — reactive rediscovery only.
    #[test]
    fn an_empty_promote_hint_is_ignored() {
        let t = two_pair_topo();
        t.apply_promote_hint("");
        assert_eq!(
            master0(&t),
            Some("a:1".into()),
            "an absent hint must change nothing"
        );
    }

    /// A hint naming a node this proxy does not serve must not disturb it.
    #[test]
    fn a_hint_for_an_unknown_node_leaves_routing_alone() {
        let t = two_pair_topo();
        t.apply_promote_hint("zzz:9|1");
        assert_eq!(master0(&t), Some("a:1".into()));
        assert_eq!(
            t.clusters[0].routing.read().expect("routing lock").masters[1],
            Some("b:1".into())
        );
    }

    #[test]
    fn exception_overrides_range_default() {
        let t = two_pair_topo();
        // Count-derived default: slot 100 -> pair 0. Exception moves it to 1.
        assert_eq!(t.route(b"acme", 100), Some("a:1".into()));
        if let Ok(mut e) = t.clusters[0].exceptions.write() {
            e.insert((b"acme".to_vec(), 100), 1);
        }
        assert_eq!(t.route(b"acme", 100), Some("b:1".into()));
        // Another tenant's identical slot is untouched (per-ns keying).
        assert_eq!(t.route(b"bravo", 100), Some("a:1".into()));
    }

    #[test]
    fn moved_bridge_outranks_exception() {
        let t = two_pair_topo();
        if let Ok(mut e) = t.clusters[0].exceptions.write() {
            e.insert((b"acme".to_vec(), 100), 1);
        }
        t.learn_moved(b"acme", 100, "c:1");
        assert_eq!(t.route(b"acme", 100), Some("c:1".into()));
        assert_eq!(
            t.stat_moved_learned
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn stale_exception_degrades_to_range_default() {
        // R2: a row referencing a nonexistent pair must NOT kill routing.
        let t = two_pair_topo();
        if let Ok(mut e) = t.clusters[0].exceptions.write() {
            e.insert((b"acme".to_vec(), 100), 7); // no pair 7
        }
        assert_eq!(t.route(b"acme", 100), Some("a:1".into()));
        // A valid pair with an undiscovered master keeps the fail-then-
        // rediscover contract (route None).
        if let Ok(mut r) = t.clusters[0].routing.write() {
            r.masters[1] = None;
        }
        if let Ok(mut e) = t.clusters[0].exceptions.write() {
            e.insert((b"acme".to_vec(), 100), 1);
        }
        assert_eq!(t.route(b"acme", 100), None);
    }

    #[test]
    fn snapshot_parses_exceptions_and_prunes_agreeing_bridges_only() {
        let t = two_pair_topo();
        // Two bridges: one AGREES with the incoming truth (-> b:1, pair 1),
        // one DISAGREES (newer re-migration to c:1).
        t.learn_moved(b"acme", 100, "b:1");
        t.learn_moved(b"acme", 200, "c:1");
        // Pairs spec must match the current table INCLUDING ranges so
        // apply_snapshot does not rebuild (a rebuild would re-discover
        // masters over dead test addresses); ns contains ':' to prove
        // right-anchored parsing.
        if let Ok(mut r) = t.clusters[0].routing.write() {
            r.ranges = vec![None, None];
        }
        t.apply_snapshot("a:1;b:1", "", "", "acme:100:1;acme:200:1;odd:ns:300:0");
        let exc = t.clusters[0].exceptions.read().expect("lock");
        assert_eq!(exc.get(&(b"acme".to_vec(), 100)), Some(&1));
        assert_eq!(exc.get(&(b"odd:ns".to_vec(), 300)), Some(&0));
        drop(exc);
        let moved = t.clusters[0].moved.read().expect("lock");
        assert!(
            !moved.contains_key(&(b"acme".to_vec(), 100)),
            "agreeing bridge should be pruned"
        );
        assert_eq!(
            moved.get(&(b"acme".to_vec(), 200)).map(String::as_str),
            Some("c:1"),
            "disagreeing (newer) bridge must survive the push"
        );
    }
}
