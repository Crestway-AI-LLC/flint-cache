// SPDX-License-Identifier: Elastic-2.0
// Armed BEFORE the hazard it guards (ADR-0021). This crate holds ~62
// `lock()`/`read()`/`write()` guards, and as the async conversion lands, a
// guard still alive across an `.await` would deadlock the worker owning it and
// take every client on that worker with it. That is a mistake review does not
// reliably catch and the compiler otherwise permits, so it is denied from the
// moment async code can exist here rather than after the first one ships.
#![deny(clippy::await_holding_lock)]
#![deny(clippy::await_holding_refcell_ref)]
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

mod apool;
mod cache;
mod errors;
mod latency;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use flint_resp::{Decoded, Value, decode, encode, encode_proto};
use flint_slot::slot_for_key;

/// Total retry budget for one client command across MOVED chases, TRYAGAIN
/// waits, and failover rediscovery — the proxy's answer to "latency spike,
/// not errors" during topology changes.
const RETRY_BUDGET: Duration = Duration::from_secs(5);
/// Backend I/O timeout for KEYED traffic. Generous: a frozen-slot drain or a
/// slow disk read must not be misread as a dead node. Deliberately short all
/// the same — a client waiting on GET wants to fail over, not to wait.
pub(crate) const BACKEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Backend read budget for the O(KEYS) ADMIN class — `DBSIZE`, `FLUSHALL`,
/// `SCAN`'s per-master step. These are not keyed traffic and must not be
/// judged by keyed traffic's clock: DBSIZE walks every metadata row on the
/// node it asks, so its honest cost grows with the keyspace, and `SCAN`'s
/// step and `FLUSHALL`'s chunked delete do the same.
///
/// Scale run 19 (2026-08-14) is why this exists as its own constant: on a
/// 100 GB, 4-pair fleet with ~1.6M keys per pair, DBSIZE needed longer than
/// BACKEND_TIMEOUT, so `flintctl verify --probe` reported FAILED — on a
/// fleet that was healthy by every other check and had never been perturbed.
/// A verify that cries wolf above a few hundred GB is worse than no verify.
///
/// This BUYS HEADROOM, it does not remove the wall: the fix that does is a
/// maintained live-key counter making DBSIZE O(1) (#179). Size this to the
/// keyspace — `fanout-timeout-ms` in the inventory.
const FANOUT_TIMEOUT_DEFAULT: Duration = Duration::from_secs(60);

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

/// Co-processor connection pool (ADR-0010 D6): endpoint -> idle connections,
/// each a stream plus its read buffer (as `Backends` keeps them).
type CoprocPool = HashMap<String, Vec<(flint_tls::Stream, Vec<u8>)>>;

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
    /// address -> when a rediscovery for it last STARTED. Collapses the herd
    /// of callers a single pooled-connection death produces into one re-probe
    /// (see `rediscover_for`).
    /// Per-address single-flight + quiet-period state. Was
    /// `HashMap<String, Instant>`, a leading-edge debounce that bounded probe
    /// STARTS and not probes IN FLIGHT (BUG-0052).
    rediscover_gate: Mutex<HashMap<String, ProbeState>>,
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
    /// Read budget for the O(keys) admin class (`--fanout-timeout-ms`); see
    /// FANOUT_TIMEOUT_DEFAULT and Backends::call_slow.
    fanout_timeout: Duration,
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
    /// Family route table (ADR-0010 D1): command prefix, uppercased (e.g.
    /// `VEC.`) -> co-processor endpoint addresses. Fed by `--families` or
    /// CPSNAPSHOT element 7. Global today; D1's "per-cluster" table is the
    /// ADR-0007 federation shape. A command that matches a prefix here — and
    /// is NOT a known read or write — takes the family path instead of routing
    /// to a backend; anything unregistered routes exactly as before.
    families: RwLock<Vec<(Vec<u8>, Vec<String>)>>,
    /// Co-processor connection pool (ADR-0010 D6): endpoint -> idle connections.
    /// Proxy-GLOBAL and shared across clients — a co-processor connection carries
    /// no per-tenant state (the namespace rides the channel token, not the
    /// socket), so unlike the per-client tenant `Backends` this one pool serves
    /// everyone. Synchronous: one family command per connection, checked out
    /// then returned. Dials via `backend_tls` (mutual TLS to the co-processor's
    /// serverAuth leaf in prod, plaintext when unset). Unbounded here; step 4
    /// (D3) adds the size cap and the shed order.
    coproc_pool: std::sync::Mutex<CoprocPool>,
    /// This proxy's ADVERTISED edge address (`--edge-advertise`), handed to a
    /// co-processor as the `PROXYCHAN` callback (D6) — not derived from the
    /// accept socket, since the inbound peer address is not a reachable edge.
    /// `None` => the forward has no callback to hand out and answers
    /// `-COPROCUNAVAIL`.
    edge_advertise: Option<String>,
    /// In-flight family commands (ADR-0010 D3): incremented while a family
    /// command holds a co-processor connection. Family admission is shed once
    /// this reaches `coproc_max_inflight` — family commands shed FIRST under
    /// pressure, and the data path is never bounded by it.
    coproc_inflight: std::sync::atomic::AtomicUsize,
    /// Cap on concurrent family commands (`--family-max-inflight`). Bounds the
    /// aggregate channel I/O — inflight x per-command budget — so it cannot
    /// exhaust the data path's backend capacity (D3).
    coproc_max_inflight: usize,
    /// Per-family-command channel budget and deadline (`--family-budget`,
    /// `--family-deadline-ms`), fixed into the token at mint time (D3).
    family_budget: u64,
    family_deadline: Duration,
}

impl Topology {
    /// True if `name` matches a registered family prefix (ADR-0010 D1).
    /// Case-insensitive on the name, since the wire is. Callers gate this
    /// behind "not a known read/write" so the resolution order holds:
    /// known write → known read → registered family → unknown.
    fn family_registered(&self, name: &[u8]) -> bool {
        let upper = name.to_ascii_uppercase();
        self.families
            .read()
            .map(|f| f.iter().any(|(p, _)| upper.starts_with(p.as_slice())))
            .unwrap_or(false)
    }

    /// Replace the family route table from CPSNAPSHOT element 7. Only called
    /// when the element is PRESENT — an older CP that omits it leaves the
    /// table (static `--families`, or a prior snapshot) untouched.
    fn apply_families(&self, s: &str) {
        if let Ok(mut f) = self.families.write() {
            *f = parse_families(s);
        }
    }

    /// The co-processor endpoints registered for the family `name` matches, or
    /// empty if none (ADR-0010 D1).
    fn family_endpoints(&self, name: &[u8]) -> Vec<String> {
        let upper = name.to_ascii_uppercase();
        self.families
            .read()
            .ok()
            .and_then(|f| {
                f.iter()
                    .find(|(p, _)| upper.starts_with(p.as_slice()))
                    .map(|(_, addrs)| addrs.clone())
            })
            .unwrap_or_default()
    }

    /// Mint a single-use channel token for (ns, budget, deadline) and record
    /// it (ADR-0010 D2). Shared by `PROXYCHANMINT` and the family forward.
    /// Sweeps dead entries first so the table cannot grow without bound.
    fn mint_channel_token(&self, ns: &[u8], budget: u64, deadline: Instant) -> Option<String> {
        let token = flint_tls::mint_token();
        let mut tokens = self.channel_tokens.lock().ok()?;
        let now = Instant::now();
        tokens.retain(|_, g| !g.used && g.deadline > now);
        tokens.insert(
            token.clone(),
            ChannelGrant {
                ns: ns.to_vec(),
                budget,
                deadline,
                used: false,
            },
        );
        Some(token)
    }

    /// Reserve a family-command in-flight slot (ADR-0010 D3), returning a guard
    /// that releases it on drop, or `None` if the cap is reached — in which case
    /// the family command is shed. Family admission is bounded FIRST under
    /// pressure; the data path is never bounded by this.
    fn try_reserve_family(&self) -> Option<InflightGuard<'_>> {
        self.coproc_inflight
            .fetch_update(
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
                |n| (n < self.coproc_max_inflight).then_some(n + 1),
            )
            .ok()
            .map(|_| InflightGuard(&self.coproc_inflight))
    }

    /// Send one `FLINTFAM` frame to the first reachable co-processor endpoint
    /// and return its reply (ADR-0010 D6). Synchronous and pooled: check out an
    /// idle connection or dial a new one, exchange, return it on success / drop
    /// it on failure. The pool lock is held ONLY across checkout and return,
    /// never across the blocking exchange — D3's "no lock the data path needs
    /// is held across a co-processor call". `deadline` bounds the I/O.
    fn coproc_call(
        &self,
        endpoints: &[String],
        frame: &[u8],
        deadline: Duration,
    ) -> Result<Value, ()> {
        for addr in endpoints {
            // Try a pooled connection first, then a fresh dial. A pooled
            // connection can be stale — the co-processor idled it out — without
            // the endpoint being down, so a single pooled failure must not
            // condemn the endpoint before a fresh dial is tried.
            let pooled = self
                .coproc_pool
                .lock()
                .ok()
                .and_then(|mut p| p.get_mut(addr).and_then(|v| v.pop()));
            for (from_pool, conn) in [(true, pooled), (false, None)] {
                let (mut stream, mut buf) = match conn {
                    Some(c) => c,
                    // No pooled connection: fall through to the fresh-dial pass.
                    None if from_pool => continue,
                    None => match flint_tls::connect_reloadable(addr, &self.backend_tls) {
                        Ok(s) => (s, Vec::new()),
                        Err(_) => break, // endpoint down; next endpoint
                    },
                };
                // A socket we cannot bound is a socket that can hang an
                // in-flight slot forever (a co-processor that accepts but never
                // replies). If the deadline cannot be applied, drop this
                // connection rather than proceed unbounded — the boot check
                // already rejects a zero deadline, so this is the belt to that
                // suspenders.
                if stream.set_read_timeout(Some(deadline)).is_err()
                    || stream.set_write_timeout(Some(deadline)).is_err()
                {
                    continue;
                }
                match call_raw(&mut stream, &mut buf, frame) {
                    Ok(v) => {
                        if let Ok(mut p) = self.coproc_pool.lock() {
                            p.entry(addr.clone()).or_default().push((stream, buf));
                        }
                        return Ok(v);
                    }
                    // A failed exchange may have desynchronised the reply stream:
                    // drop the connection (never return it). A pooled failure
                    // falls to the fresh dial; a fresh-dial failure ends this
                    // endpoint. Same discipline `Backends::call` follows.
                    Err(_) => continue,
                }
            }
        }
        Err(())
    }

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
    /// Rediscover because a REQUEST FAILED — the reactive path, coalesced.
    ///
    /// A pooled connection's death fails EVERY in-flight command on it at once
    /// (up to the in-flight cap), and each of those callers arrives here
    /// independently. Measured on a fleet 2026-08-17: one connection death
    /// became a storm — 198 routing transitions on the pooled arm against 0 on
    /// the serial one — with the master flapping `addr -> none -> addr`
    /// hundreds of times inside milliseconds as concurrent probes raced each
    /// other's writes. Threads that saw `none` slept 100 ms, retried, and
    /// probed again. Throughput fell to 0.12x with a 10 s p99.9.
    ///
    /// Whether a pair's master moved is a per-PAIR fact, not a per-request
    /// one, so N simultaneous failures need exactly one probe.
    ///
    /// Deliberately NOT applied to `rediscover_for` itself: a control-plane
    /// promote hint is authoritative news about a topology that just changed,
    /// and suppressing it would delay real failover. Coalesce the symptom,
    /// never the signal.
    fn rediscover_after_failure(&self, addr: &str) {
        // CLAIM THE PROBE, OR RETURN (BUG-0052).
        {
            let mut gate = match self.rediscover_gate.lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            match gate.get(addr) {
                // Someone is probing this address. Duration is irrelevant:
                // there is nothing a second concurrent probe can learn that
                // the first will not, and two of them race their writes.
                Some(ProbeState::InFlight) => return,
                // Probed recently enough that re-probing is noise.
                Some(ProbeState::Idle(done)) if done.elapsed() < REDISCOVER_DEBOUNCE => {
                    return;
                }
                _ => {}
            }
            gate.insert(addr.to_string(), ProbeState::InFlight);
        }

        // The claim MUST be released on every exit path, or one panicking or
        // early-returning probe wedges this address forever — a failure worse
        // than the flap being fixed, because it is permanent and silent. A
        // guard, so the compiler owns that rather than a reviewer.
        struct Release<'a>(&'a Mutex<HashMap<String, ProbeState>>, &'a str);
        impl Drop for Release<'_> {
            fn drop(&mut self) {
                if let Ok(mut g) = self.0.lock() {
                    g.insert(self.1.to_string(), ProbeState::Idle(Instant::now()));
                }
            }
        }
        let _release = Release(&self.rediscover_gate, addr);

        self.rediscover_for(addr);
    }

    /// True when routing STILL names `addr` as a master — the re-probe found
    /// nothing new, so an immediate retry would hit the same refusal.
    ///
    /// This is the discriminator the two `-READONLY` handlers were missing.
    /// One slept 50ms unconditionally and one never slept, so the cost of a
    /// controlled failover depended on which of them serviced the write
    /// (BUG-0055). Backing off is right when there is nothing else to try and
    /// wrong when rediscovery has already found the new master, and that is a
    /// question about the routing table rather than about which call path we
    /// are on.
    fn still_master(&self, addr: &str) -> bool {
        self.clusters.iter().any(|view| {
            view.routing
                .read()
                .map(|r| r.masters.iter().flatten().any(|m| m == addr))
                .unwrap_or(false)
        })
    }

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
                let mut was: Option<String> = None;
                if let Ok(mut routing) = view.routing.write()
                    && let Some(slot) = routing.masters.get_mut(i)
                {
                    was = slot.clone();
                    *slot = found.clone();
                }
                // Say when routing actually MOVED, and to what.
                //
                // Soak run 26 spent a 43850ms client-visible outage after a
                // promotion that finished in 597ms, and the proxy log had
                // NOTHING to say about it: no line here, none in
                // apply_promote_hint, so "did the edge ever learn about the
                // new master, and when" was unanswerable from the evidence
                // bundle. Three separate hypotheses were raised and killed by
                // code reading because no log could arbitrate them. One line
                // per actual change is cheap — re-probes are frequent, but
                // re-probes that CHANGE the master are exactly as rare as
                // promotions (#187).
                if was != found {
                    eprintln!(
                        "[{}] pair {i} master {} -> {} (re-probe triggered by {addr})",
                        log_ms(),
                        was.as_deref().unwrap_or("none"),
                        found.as_deref().unwrap_or("none")
                    );
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
        // The arrival of the hint is itself evidence, separate from whether
        // the re-probe then moves anything. On soak run 26 the proxy log had
        // zero lines mentioning a promotion across a 25-minute run containing
        // several, which left "the hint never arrived" and "the hint arrived
        // and did nothing" indistinguishable — two very different bugs (#187).
        eprintln!(
            "[{}] promotion hint {hint}: re-probing the pair holding {addr}",
            log_ms()
        );
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
    ///
    /// `rate_exempt` skips ONLY the ops/s bucket, not the storage verdict.
    /// A co-processor channel's writes are rate-exempt (ADR-0010 D1: the family
    /// command paid the ops/s charge once at admission, so re-charging each
    /// channel write would make one `VEC.SET` cost an implementation-defined
    /// number of tokens) — but they still GROW storage, so a channel that
    /// writes to an over-quota tenant must be shed just like a direct write, or
    /// the per-tenant storage cap is unenforceable on the co-processor path.
    fn quota_gate(
        &self,
        ns: &[u8],
        name: &[u8],
        is_write: bool,
        rate_exempt: bool,
    ) -> Option<Value> {
        let (rate, over) = *self.quota.read().ok()?.get(ns)?;
        // Space-REDUCING writes stay allowed over-quota — the self-clear
        // path is the tenant deleting data, which must never be blocked by
        // the very state it cures. Shared with the server's disk gate. This
        // verdict applies to a channel's writes too (see `rate_exempt`).
        if over && is_write && !flint_commands::reduces_space(name) {
            self.stat_quota_write_shed_total
                .fetch_add(1, Ordering::Relaxed);
            return Some(Value::Error(
                "QUOTA storage quota exceeded; writes rejected until usage drops (reads still served)"
                    .into(),
            ));
        }
        if rate_exempt || rate == 0 {
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

/// Read exactly ONE reply off a backend connection, topping up `buf` from the
/// socket until a frame completes.
///
/// Split out of `call_raw` so the dispatcher pool (ADR-0020) can read N replies
/// in a row from one connection without a second implementation of the decode
/// loop. Leaves any bytes beyond this reply in `buf` — which is what makes
/// reading N in order work at all.
pub(crate) fn read_reply(
    stream: &mut flint_tls::Stream,
    buf: &mut Vec<u8>,
) -> std::io::Result<Value> {
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

/// One RESP request/response exchange on a raw connection.
fn call_raw(
    stream: &mut flint_tls::Stream,
    buf: &mut Vec<u8>,
    frame: &[u8],
) -> std::io::Result<Value> {
    stream.write_all(frame)?;
    read_reply(stream, buf)
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
/// This client's view of the backend hop.
///
/// Ordinary keyed traffic goes through the WORKER's connection registry
/// (`apool`), shared with the other clients this worker owns and with nobody
/// else. What lives here is only what genuinely needs a connection to itself:
///
///   - TRANSACTIONS. A node keeps MULTI's queue and WATCH's watches per
///     CONNECTION, so a queued command landing on a shared connection would
///     EXECUTE instead of queueing, and the client would see QUEUED followed
///     by a partial apply. This is the case the old per-client design was
///     defending, and it is still right — it just does not need every GET to
///     pay for it.
///   - The O(keys) admin class (`call_slow`), whose minute-long budget would
///     head-of-line block every ordinary read sharing its connection.
struct Backends {
    /// Connections belonging to THIS client alone, never in the registry.
    private: HashMap<String, Rc<apool::AsyncConn>>,
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
    /// Read budget for the O(keys) admin class only (see call_slow).
    fanout_timeout: Duration,
}

impl Backends {
    fn new(
        ns: Vec<u8>,
        tls: Option<Arc<flint_tls::ReloadableClientConfig>>,
        async_writes: bool,
        fanout_timeout: Duration,
    ) -> Self {
        Self {
            private: HashMap::new(),
            ns,
            tls,
            async_writes,
            fanout_timeout,
        }
    }

    /// A connection is only reusable for the SAME (address, namespace,
    /// async-writes) triple, because `FLINTNS` pins it at open.
    fn key(&self, addr: &str) -> apool::Key {
        apool::Key {
            addr: addr.to_string(),
            ns: self.ns.clone(),
            async_writes: self.async_writes,
        }
    }

    /// Discard connections to `addr` (stale-routing recovery: the next call
    /// dials whatever the refreshed masters map says). Both the shared one and
    /// any private one, because a demoted master is wrong for either.
    fn drop_conn(&mut self, addr: &str) {
        apool::retire(&self.key(addr));
        if let Some(c) = self.private.remove(addr) {
            c.shutdown();
        }
    }

    /// Await one staged reply.
    ///
    /// A timed-out request must NOT simply return. Its reply may still be in
    /// flight, and correlation is by position, so the next caller on that
    /// connection would receive it — one slow command would hand every
    /// subsequent client someone else's data. Retiring the connection is what
    /// makes the timeout safe rather than merely late.
    async fn collect(
        rx: tokio::sync::oneshot::Receiver<std::io::Result<Value>>,
        budget: Duration,
        on_timeout: impl FnOnce(),
    ) -> std::io::Result<Value> {
        match tokio::time::timeout(budget, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err(std::io::Error::other("backend connection closed")),
            Err(_) => {
                on_timeout();
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "backend did not answer within the budget",
                ))
            }
        }
    }

    /// Keyed traffic: on this worker's shared connection.
    async fn call(&mut self, addr: &str, frame: &[u8]) -> std::io::Result<Value> {
        let key = self.key(addr);
        let conn = apool::conn_for(&key, &self.tls).await?;
        let rx = conn.stage(frame)?;
        conn.flush().await?;
        Self::collect(rx, BACKEND_TIMEOUT, || apool::retire(&key)).await
    }

    /// This worker's connection to `addr`, for staging a whole prefetch pass
    /// onto ONE connection. Picking a connection per COMMAND instead scatters
    /// a client's pipeline and destroys the batching (see ADR-0020's
    /// amendment, and the fleet run that measured batch depth 1.04).
    async fn lease(&mut self, addr: &str) -> std::io::Result<Rc<apool::AsyncConn>> {
        apool::conn_for(&self.key(addr), &self.tls).await
    }

    /// One command on a connection belonging to THIS client alone, with an
    /// explicit budget. Shared by transactions and the O(keys) admin class.
    async fn private_call(
        &mut self,
        addr: &str,
        frame: &[u8],
        budget: Duration,
    ) -> std::io::Result<Value> {
        let conn = match self.private.get(addr) {
            Some(c) if !c.is_dead() => c.clone(),
            _ => {
                let c = apool::dial_private(&self.key(addr), &self.tls).await?;
                self.private.insert(addr.to_string(), c.clone());
                c
            }
        };
        let rx = conn.stage(frame)?;
        conn.flush().await?;
        let r = Self::collect(rx, budget, || conn.shutdown()).await;
        if r.is_err() {
            // A failed connection is never reused: the reply stream may be
            // desynchronised, which would corrupt the next exchange.
            if let Some(c) = self.private.remove(addr) {
                c.shutdown();
            }
        }
        r
    }

    /// Keyed traffic on a connection belonging to THIS client alone — where a
    /// shared connection would be wrong rather than merely slower.
    async fn call_private(&mut self, addr: &str, frame: &[u8]) -> std::io::Result<Value> {
        self.private_call(addr, frame, BACKEND_TIMEOUT).await
    }

    /// The O(keys) admin class — `DBSIZE`, `FLUSHALL`, `SCAN`'s per-master
    /// step. Same wire, different clock: only the command's own reply gets
    /// `fanout_timeout`, and it travels privately so a minute-long wait cannot
    /// head-of-line block anyone else's reads.
    async fn call_slow(&mut self, addr: &str, frame: &[u8]) -> std::io::Result<Value> {
        let budget = self.fanout_timeout;
        self.private_call(addr, frame, budget).await
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
/// Put a backend reply into the dialect THIS client is owed.
///
/// Backends answer us in RESP3, so a reply the JSON.TYPE quirk applies to
/// arrives already nested: peel that layer off and re-mark it, so the
/// client's own dialect decides whether it goes back on. JSON.NUMINCRBY's
/// dialects differ in reply KIND, so the RESP2 spelling is rebuilt from the
/// RESP3 array (the array holds the matches; args[2] says which spelling the
/// caller expects).
///
/// A free function rather than a closure inside `forward` because the
/// prefetched path collects its replies elsewhere and must repair them
/// identically.
fn repair_reply(args: &[Vec<u8>], v: Value) -> Value {
    if args
        .first()
        .is_some_and(|n| flint_resp::resp3_nests_reply(n))
    {
        return match v {
            Value::Array(Some(mut items)) if items.len() == 1 => {
                Value::Resp3Nested(Box::new(items.remove(0)))
            }
            other => other,
        };
    }
    if args
        .first()
        .is_some_and(|n| flint_resp::resp3_differs_in_kind(n))
        && !matches!(v, Value::Error(_))
    {
        let jsonpath = args.get(2).is_some_and(|p| p.first() == Some(&b'$'));
        return Value::ByProto {
            resp2: Box::new(flint_resp::json_numincrby_resp2(&v, jsonpath)),
            resp3: Box::new(v),
        };
    }
    v
}

/// Collect a prefetched command's reply.
///
/// The happy path — a plain reply for a command already on the wire — is the
/// entire point of prefetching, and costs nothing beyond the wait. Every
/// other outcome (a MOVED chase, a stale replica, a demoted master, a dead
/// connection) hands the command to `forward`, which owns the retry,
/// rediscovery and failover budget. Retries are rare and already expensive;
/// duplicating that logic here to save one round trip on the unhappy path
/// would be the wrong trade and a second place for failover to be subtly
/// wrong.
async fn forward_collect(
    topo: &Arc<Topology>,
    backends: &mut Backends,
    ns: &[u8],
    args: &[Vec<u8>],
    raw: &[u8],
    rx: tokio::sync::oneshot::Receiver<std::io::Result<Value>>,
    addr: String,
) -> Value {
    // A staged command that never answers must take its connection with it:
    // correlation is by position, so a late reply would be handed to whoever
    // asked next.
    let got = match tokio::time::timeout(BACKEND_TIMEOUT, rx).await {
        Ok(Ok(r)) => r,
        Ok(Err(_)) => Err(std::io::Error::other("backend connection closed")),
        Err(_) => {
            backends.drop_conn(&addr);
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "backend did not answer within the budget",
            ))
        }
    };
    match got.map(|v| repair_reply(args, v)) {
        Ok(Value::Error(e)) if e.starts_with("MOVED ") => {
            let mut parts = e.split(' ');
            let (_, s, new_addr) = (parts.next(), parts.next(), parts.next());
            if let (Some(s), Some(new_addr)) = (s, new_addr)
                && let Ok(s) = s.parse::<u16>()
            {
                topo.learn_moved(ns, s, new_addr);
            }
            forward(topo, backends, ns, args, raw, false).await
        }
        Ok(Value::Error(e)) if e.starts_with("TRYAGAIN") => {
            forward(topo, backends, ns, args, raw, false).await
        }
        Ok(Value::Error(e)) if e.starts_with("READONLY") => {
            // A demoted-in-place ex-master. Same recovery as the ordinary
            // path: rediscover this pair's master, then retry there.
            topo.rediscover_after_failure(&addr);
            backends.drop_conn(&addr);
            // The same rule as forward's handler, so one condition has one
            // retry discipline (BUG-0055). This path never slept; that was
            // right whenever rediscovery moved routing and wrong when it did
            // not, and nothing here distinguished the two.
            if topo.still_master(&addr) {
                std::thread::sleep(Duration::from_millis(50));
            }
            forward(topo, backends, ns, args, raw, false).await
        }
        Ok(reply) => reply,
        Err(_) => {
            topo.rediscover_after_failure(&addr);
            forward(topo, backends, ns, args, raw, false).await
        }
    }
}

async fn forward(
    topo: &Arc<Topology>,
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

        match backends
            .call(&addr, frame)
            .await
            .map(|v| repair_reply(args, v))
        {
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
                topo.rediscover_after_failure(&addr);
                backends.drop_conn(&addr);
                // BACK OFF ONLY WHEN THE RE-PROBE FOUND NOTHING (BUG-0055).
                // This slept unconditionally, 50ms AFTER rediscovery had
                // already found the new master — pure added latency on every
                // controlled failover, and measurably the whole of it: the
                // first write after a demote/promote cost 49-64ms above
                // steady state, of which 50 was this.
                if topo.still_master(&addr) {
                    std::thread::sleep(Duration::from_millis(50));
                }
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
                    topo.rediscover_after_failure(&addr);
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

async fn fan_out(
    topo: &Arc<Topology>,
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
                    // call_slow: everything routed here is the O(keys) admin
                    // class (DBSIZE, FLUSHALL), whose honest cost scales with
                    // the keyspace on the node being asked.
                    Some(a) => match backends.call_slow(&a, frame).await {
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
    /// `PROXYCHAN` opened a single-use channel (ADR-0010 D2/D3). The namespace
    /// is already pinned into `authed_ns`; this carries the grant's deadline
    /// and data-command budget so the connection loop closes the channel once
    /// it outlives either bound.
    ChannelOpen { deadline: Instant, budget: u64 },
}

/// Redis-shaped auth gate. AUTH <token> or AUTH <user> <token> (the user is
/// ignored; the token alone identifies the tenant). Before auth, everything
/// except AUTH/QUIT gets -NOAUTH. A successful AUTH fixes the connection's
/// namespace; re-AUTH to a different tenant is rejected (reconnect instead)
/// so the backend-connection namespace pinning can never go stale.
#[allow(clippy::too_many_arguments)]
fn auth_step(
    topo: &Arc<Topology>,
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
        let Some(budget) = std::str::from_utf8(budget_a)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        else {
            return AuthStep::Reply(Value::Error("ERR budget must be an integer".into()));
        };
        let Some(deadline_ms) = std::str::from_utf8(deadline_a)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        else {
            return AuthStep::Reply(Value::Error("ERR deadline-ms must be an integer".into()));
        };
        let deadline = Instant::now() + std::time::Duration::from_millis(deadline_ms);
        return match topo.mint_channel_token(ns, budget, deadline) {
            Some(token) => AuthStep::Reply(Value::Bulk(Some(token.into_bytes()))),
            None => AuthStep::Reply(Value::Error("ERR channel token lock".into())),
        };
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
            Open(Vec<u8>, Instant, u64),
        }
        let verdict = match tokens.get(&token) {
            None => V::Invalid,
            Some(g) if g.used => V::Used,
            Some(g) if Instant::now() >= g.deadline => V::Expired,
            Some(g) => V::Open(g.ns.clone(), g.deadline, g.budget),
        };
        return match verdict {
            V::Invalid => AuthStep::Reply(Value::Error("WRONGPASS invalid channel token".into())),
            V::Used => AuthStep::Reply(Value::Error("ERR channel token already used".into())),
            V::Expired => {
                tokens.remove(&token);
                AuthStep::Reply(Value::Error("ERR channel token expired".into()))
            }
            V::Open(ns, deadline, budget) => {
                // Single-use: consume on open, so a second PROXYCHAN finds it used.
                if let Some(g) = tokens.get_mut(&token) {
                    g.used = true;
                }
                *authed_ns = Some(ns);
                AuthStep::ChannelOpen { deadline, budget }
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
        // ADR-0020 pool counters. `pool_batch_mean` is the load-bearing one:
        // it says whether multiplexing is actually coalescing anything. The
        // reverted in-buffer batcher (1decd25) shipped no such number, so its
        // null result could not be explained without re-reading the source —
        // which is why the ADR made instrumentation an acceptance gate rather
        // than a follow-up.
        let pool_batches = apool::BATCHES.load(Ordering::Relaxed);
        let pool_commands = apool::COMMANDS.load(Ordering::Relaxed);
        let pool_batch_mean = if pool_batches == 0 {
            0.0
        } else {
            pool_commands as f64 / pool_batches as f64
        };

        // build: FIRST (ADR-0014 D1). The edge is rolled by `flintctl
        // upgrade` like everything else, and until now carried no stamp at
        // all — so a half-completed edge roll looked exactly like a
        // finished one.
        let info = format!(
            "build:{build}\r\nactive:{}\r\nconns_total:{}\r\nshed_total:{}\r\nauth_ok_total:{}\r\nauth_fail_total:{}\r\ncommands_total:{}\r\ncommands_read_total:{}\r\ncommands_write_total:{}\r\nhotkey_sample_rate:{}\r\ncache_ttl_ms:{cache_ttl}\r\ncache_max_bytes:{cache_max}\r\ncache_hits_total:{cache_hits}\r\ncache_misses_total:{cache_misses}\r\ncache_entries:{cache_entries}\r\ncache_bytes:{cache_bytes}\r\nmoved_learned_total:{moved_learned}\r\nquota_throttled_total:{quota_throttled}\r\nquota_write_shed_total:{quota_write_shed}\r\npool_lanes:{pool_lanes}\r\npool_batches_total:{pool_batches}\r\npool_commands_total:{pool_commands}\r\npool_batch_mean:{pool_batch_mean:.2}\r\npool_inflight_max:{pool_inflight_max}\r\npool_dial_failures_total:{pool_dials}\r\ncert_days_remaining:{cdr}\r\n",
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
            pool_lanes = apool::LIVE_CONNS.load(Ordering::Relaxed),
            pool_inflight_max = apool::INFLIGHT_MAX.load(Ordering::Relaxed),
            pool_dials = apool::DIAL_FAILURES.load(Ordering::Relaxed),
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
    // Commands forwarded PER BACKEND (BUG-0040). Ops data, not tenant data —
    // it names nodes rather than namespaces — so it is admin-gated like
    // PROXYCACHE below, not tenant-scoped like PROXYERRORS above.
    //
    // Why a command and not another info-block field: the info block is a
    // fixed set of scalars, and this is one row per backend. Reporting it
    // there would mean encoding a map into a field name.
    //
    // THE DISTINCTION CONSUMERS MUST KEEP. A backend with a zero row is
    // EVIDENCE — this proxy has a counter for it and forwarded nothing. A
    // backend with NO row is IGNORANCE — this proxy has never dialed it, which
    // is also what a proxy that just restarted reports about everything.
    // Collapsing those is the bug BUG-0040 exists to prevent: the host a
    // termination gate is asked about is exactly the one most likely to be
    // missing rather than idle.
    if name.as_deref() == Some(b"PROXYBACKENDS") {
        if admin_locked {
            return admin_denied();
        }
        let mut out = String::new();
        for (addr, n) in apool::per_node_commands() {
            out.push_str(&format!("{addr} {n}\r\n"));
        }
        return AuthStep::Reply(Value::Bulk(Some(out.into_bytes())));
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

async fn serve_client<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    mut stream: S,
    topo: Arc<Topology>,
) -> std::io::Result<()> {
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
    // Remaining data-command budget for a channel (ADR-0010 D3): decremented on
    // each data command it issues; at zero the channel is closed. Bounds a
    // looping co-processor at the mint-time budget instead of a latency graph.
    // `None` for tenants (unbounded, as always).
    let mut channel_budget: Option<u64> = None;
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
    // Every complete command in the current read, decoded once. Reused across
    // reads so a pipelining client does not re-allocate it per batch.
    let mut cmds: Vec<(Vec<Vec<u8>>, Vec<u8>)> = Vec::new();
    loop {
        let mut consumed = 0;
        out.clear();
        // Phase 1 — decode every complete command now in the buffer.
        //
        // Decoding used to be interleaved with execution: decode one, block
        // for its reply, decode the next. That is where a client's pipeline
        // died — the node could not be told about command 2 until command 1
        // had travelled all the way back, so it saw N round trips and never a
        // batch. Separating decode from execution is what lets the prefetch
        // pass below see the whole run at once.
        cmds.clear();
        let mut fatal: Option<&str> = None;
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
                        fatal = Some("ERR Protocol error: too big inline request");
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
                    match frame_to_args(frame) {
                        Some(args) => cmds.push((args, raw)),
                        None => {
                            fatal = Some("ERR Protocol error: expected array of bulk strings");
                            break;
                        }
                    }
                }
                Ok(Decoded::NeedMore) => break,
                Err(_) => {
                    fatal = Some("ERR Protocol error");
                    break;
                }
            }
        }

        // Phase 2 — prefetch: stage the leading run of independent keyed
        // commands so the node receives them as one pipeline instead of
        // learning about each only after answering the last. Any transaction
        // state at all is a barrier: MULTI's queue and WATCH's watches are
        // per-connection state on the node, so ordering is load-bearing while
        // either is armed.
        let plan = match authed_ns.as_deref() {
            Some(ns) => {
                prefetch_run(
                    &topo,
                    &mut backends,
                    &cmds,
                    ns,
                    txn.open || txn.addr.is_some(),
                    replica_reads,
                    local_cache,
                    async_writes,
                    channel_deadline.is_some(),
                )
                .await
            }
            // Unauthenticated: nothing may be staged, and AUTH itself must be
            // executed before anything behind it is even classified.
            None => Vec::new(),
        };
        let mut plan = plan.into_iter();

        // Phase 3 — execute in issue order. Per command this is unchanged;
        // the only difference is that a prefetched command collects a reply
        // already in flight instead of starting its own round trip.
        for (args, raw) in cmds.iter() {
            let prefetch = plan.next().unwrap_or(Prefetch::None);
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
                        && let Some(key) = route_key(args)
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
                stream.write_all(&out).await?;
                return Ok(());
            }
            // Channel data-command budget (ADR-0010 D3): a channel gets
            // a fixed number of data commands, then it is closed. A
            // looping co-processor is cut off at the bound, not found
            // later in a latency graph. Counts data commands only — a
            // PING or HELLO on the channel is free.
            if let Some(budget) = channel_budget.as_mut()
                && args.first().is_some_and(|n| {
                    flint_commands::is_write_command(n) || flint_commands::is_read_command(n)
                })
            {
                if *budget == 0 {
                    encode_proto(
                        &Value::Error("ERR channel budget exhausted".into()),
                        proto,
                        &mut out,
                    );
                    stream.write_all(&out).await?;
                    return Ok(());
                }
                *budget -= 1;
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
                Value::Error("ERR admin commands are not available through the proxy".into())
            } else {
                match auth_step(
                    &topo,
                    &mut authed_ns,
                    &mut replica_reads,
                    &mut local_cache,
                    &mut async_writes,
                    &mut is_admin,
                    &mut proto,
                    args,
                ) {
                    AuthStep::Reply(v) => v,
                    AuthStep::ChannelOpen { deadline, budget } => {
                        // PROXYCHAN pinned authed_ns to the grant's
                        // namespace; record the deadline and budget so
                        // this connection is closed once it outlives
                        // either bound.
                        channel_deadline = Some(deadline);
                        channel_budget = Some(budget);
                        Value::Simple("OK".into())
                    }
                    AuthStep::Proceed(ns) => {
                        data_command(
                            &topo,
                            &mut backends,
                            &mut txn,
                            &ns,
                            args,
                            raw,
                            replica_reads,
                            local_cache,
                            async_writes,
                            channel_deadline.is_some(),
                            prefetch,
                        )
                        .await
                    }
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
                    topo.latency
                        .observe(ns, is_write, started.elapsed().as_micros() as u64);
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
                stream.write_all(&out).await?;
                out.clear();
            }
        }
        // A malformed frame ends the connection — but only AFTER the commands
        // that arrived ahead of it in the same read have been answered, which
        // is what the client is owed and what interleaved decoding did
        // naturally.
        if let Some(msg) = fatal {
            encode(&Value::Error(msg.into()), &mut out);
            stream.write_all(&out).await?;
            return Ok(());
        }
        if consumed > 0 {
            buf.drain(..consumed);
            if !out.is_empty() {
                stream.write_all(&out).await?;
            }
        }
        let n = stream.read(&mut chunk).await?;
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
            let _ = stream.write_all(&out).await;
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
///
/// Deliberately `call_private`: a transaction lives on ONE node connection, so
/// it must never ride the shared pool. Routing it through `call` would hand the
/// queued command to whichever pooled connection was free, where the node —
/// having no MULTI open on it — would execute it immediately.
async fn call_pinned(backends: &mut Backends, addr: &str, frame: &[u8]) -> Result<Value, String> {
    match backends.call_private(addr, frame).await {
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
async fn transaction_step(
    topo: &Arc<Topology>,
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
            match call_pinned(backends, &addr, raw).await {
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
            let reply = match call_pinned(backends, &addr, raw).await {
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
                match call_pinned(backends, &addr, &encode_cmd(&[b"MULTI"])).await {
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
                Some(addr) => Some(match call_pinned(backends, &addr, raw).await {
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
                Some(addr) => Some(match call_pinned(backends, &addr, raw).await {
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
                match call_pinned(backends, &addr, &encode_cmd(&[b"MULTI"])).await {
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
            match call_pinned(backends, &addr, raw).await {
                Ok(v) => Some(v),
                Err(why) => Some(abort_txn(backends, txn, &why)),
            }
        }
        _ => None,
    }
}

/// Parse the family route table wire form: `PREFIX=addr,addr;PREFIX=addr`
/// (`--families` and CPSNAPSHOT element 7 share it). Prefixes are uppercased so
/// matching is case-insensitive; empty entries and empty prefixes are skipped.
/// An empty string yields no families.
fn parse_families(s: &str) -> Vec<(Vec<u8>, Vec<String>)> {
    let mut out = Vec::new();
    for entry in s.split(';').map(str::trim).filter(|e| !e.is_empty()) {
        let Some((prefix, addrs)) = entry.split_once('=') else {
            continue;
        };
        let prefix = prefix.trim().to_ascii_uppercase().into_bytes();
        if prefix.is_empty() {
            continue;
        }
        let addrs: Vec<String> = addrs
            .split(',')
            .map(str::trim)
            .filter(|a| !a.is_empty())
            .map(str::to_string)
            .collect();
        out.push((prefix, addrs));
    }
    out
}

/// Parse an optional non-negative integer proxy knob. Absent → None (the caller
/// applies the default); present-but-unparseable is an operator error caught at
/// boot — the same fail-fast discipline `--tenants` uses. A silently-defaulted
/// knob (`--family-deadline-ms fife` quietly becoming 5 s) is a footgun that
/// only surfaces as mysterious behavior under load, long after the typo.
fn parse_u64_knob(flag: &str, raw: Option<String>) -> Option<u64> {
    raw.map(|s| {
        s.parse::<u64>()
            .unwrap_or_else(|_| panic!("{flag}: expected a non-negative integer, got {s:?}"))
    })
}

/// A family channel's data-command budget and deadline, fixed at token-mint
/// time (ADR-0010 D3). The deadline is enforced by the channel loop (step 2);
/// the budget is recorded now and enforced in step 4. Constants here; step 4
/// makes them configurable and measures them (drills 4 and 6).
const FAMILY_CHANNEL_BUDGET: u64 = 256;
const FAMILY_CHANNEL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

/// Handle a registered family command (ADR-0010 D1/D2/D6). Charge the tenant
/// once, mint a single-use channel token, and forward
/// `FLINTFAM <token> <callback> <ns> <command…>` to a co-processor over the pooled,
/// synchronous hop, relaying its reply. A co-processor's OWN `-ERR` is relayed
/// as-is (its logic said no); a TRANSPORT failure — no endpoint, dial refused,
/// deadline, dropped reply — and a missing callback both become
/// `-COPROCUNAVAIL`, kept distinct from the unknown-command error a non-family
/// command earns from the master (reading one as the other is how an outage is
/// misdiagnosed, D3).
async fn family_command(topo: &Arc<Topology>, ns: &[u8], args: &[Vec<u8>]) -> Value {
    let unavail = || {
        Value::Error("COPROCUNAVAIL no co-processor is available for this command family".into())
    };
    let Some(name) = args.first() else {
        return unavail();
    };
    let endpoints = topo.family_endpoints(name);
    if endpoints.is_empty() {
        return unavail();
    }
    // The co-processor calls back over PROXYCHAN to this address (D6); with no
    // advertised edge there is nowhere to call back.
    let Some(callback) = topo.edge_advertise.clone() else {
        return unavail();
    };
    // Reserve a family-command slot (D3): family commands shed FIRST under
    // pressure — before the tenant is charged or a co-processor is dialed —
    // and the data path is never bounded by this counter. The reservation
    // caps concurrent family commands, which caps the aggregate channel I/O.
    // The guard releases the slot on EVERY return below — shed, error, success.
    let Some(_slot) = topo.try_reserve_family() else {
        return unavail();
    };
    // D1: charge the tenant ONCE, at admission, before the dial. If shed
    // (throttled), the command is not forwarded — nothing was admitted. Not
    // rate-exempt: this IS the single ops/s charge for the whole operation.
    if let Some(shed) = topo.quota_gate(ns, name, false, false) {
        return shed;
    }
    // Mint the single-use channel token the co-processor will PROXYCHAN with.
    let deadline = Instant::now() + topo.family_deadline;
    let Some(token) = topo.mint_channel_token(ns, topo.family_budget, deadline) else {
        return unavail();
    };
    // FLINTFAM <token> <callback> <ns> <original command…>. FLINTFAM is safe as
    // a proxy-only verb because the edge refuses every FLINT* before auth
    // (#151), so a client can never speak it. The namespace rides as a LABEL
    // (ADR-0017 D2): a co-processor needs a stable per-tenant key to select the
    // right in-memory index on a query — the single-use token cannot serve that
    // — but it is NOT a credential. All storage still flows only through the
    // namespace-bound channel token; the label selects an index, nothing more.
    let token_b = token.into_bytes();
    let callback_b = callback.into_bytes();
    let mut parts: Vec<&[u8]> = Vec::with_capacity(args.len() + 4);
    parts.push(b"FLINTFAM");
    parts.push(&token_b);
    parts.push(&callback_b);
    parts.push(ns);
    parts.extend(args.iter().map(|a| a.as_slice()));
    let frame = encode_cmd(&parts);
    match topo.coproc_call(&endpoints, &frame, topo.family_deadline) {
        Ok(v) => v,
        Err(()) => unavail(),
    }
}

/// Releases a family-command in-flight slot on drop, so every return path from
/// `family_command` — shed, error, or success — decrements the counter (D3).
struct InflightGuard<'a>(&'a std::sync::atomic::AtomicUsize);
impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}

#[allow(clippy::too_many_arguments)]
/// What the prefetch pass already settled about one client command.
///
/// A client pipelining N commands used to reach the node as N round trips:
/// `serve_client` decoded one command, blocked for its reply, and only then
/// decoded the next. Measured 2026-08-17, the node's CPU per operation behind
/// the proxy at pipeline depth 16 was its cost at depth 1 — it never saw a
/// pipeline at all — while `pool_batch_mean` sat at 1.0 under every load
/// shape and `pool_inflight_max` tracked the CONNECTION count rather than the
/// pipeline depth. The pool was full-duplex and nothing drove it that way.
///
/// The prefetch pass walks the leading run of independent keyed commands in
/// the read buffer and stages them all before collecting any. Because it runs
/// BEFORE execution, every decision it takes is final: the quota bucket is
/// charged there, the near-cache is consulted there, and `data_command` must
/// not repeat either or the tenant is billed twice for one command.
enum Prefetch {
    /// Not eligible, or the run ended here. `data_command` takes its ordinary
    /// path, unchanged — this is what every command saw before this pass
    /// existed, and what most still see.
    None,
    /// The quota gate already shed it. The bucket was charged once, there.
    Shed(Value),
    /// The near-cache already answered it.
    Cached(Value),
    /// Staged on a pooled connection to this address, awaiting collection.
    InFlight(
        tokio::sync::oneshot::Receiver<std::io::Result<Value>>,
        String,
    ),
}

/// Most commands one prefetch pass will stage before falling back to the
/// serial path for the rest of the read.
///
/// A cap is required, not tuning. Staging is what makes a pooled connection's
/// in-flight depth `connections x pipeline depth` instead of `connections`,
/// and both the writer's staging buffer and the pending FIFO grow with it. It
/// also bounds how long the OLDEST outstanding command waits, which used to be
/// the reader's liveness signal — an unbounded pass could make a healthy
/// backend look stalled.
///
/// 64 covers the pipeline depths clients actually use (redis-benchmark and
/// memtier default well below it) while keeping a pass's staged bytes small.
/// Beyond it the remaining commands are simply served the old way: slower, and
/// still correct.
const MAX_PREFETCH: usize = 64;

/// How long one rediscovery of an address suppresses the next.
///
/// Long enough to absorb the herd a single pooled-connection death produces
/// (every in-flight caller on it arrives at `rediscover_for` at once), short
/// enough that a real promotion is still learned promptly — and the control
/// plane pushes promotion hints anyway, so this is the slow path, not the
/// only one.
const REDISCOVER_DEBOUNCE: Duration = Duration::from_millis(250);

/// What the re-probe gate knows about one address.
///
/// A bare `Instant` could only express "a probe STARTED at t", and the gate
/// stamped it before releasing the lock and probing. So the window bounded how
/// often a probe may BEGIN, not how many may be running — and
/// `discover_master`'s read timeout is 800 ms PER NODE across both pair
/// members, up to 6.4x this window. A caller arriving at 260 ms found the
/// window expired and started a SECOND concurrent probe, each racing a write
/// into `routing.masters`: the `addr -> none -> addr` flap this gate exists to
/// prevent, at reduced rate rather than removed (BUG-0052).
///
/// Two states instead, so "someone is probing right now" is representable:
#[derive(Debug, Clone, Copy)]
enum ProbeState {
    /// A probe is running. Every other caller for this address returns
    /// immediately, however long it takes — that is the single-flight half.
    InFlight,
    /// No probe running; the last one finished at this instant. The quiet
    /// period the 250 ms constant was always sized for.
    Idle(Instant),
}

/// May this command be put on the wire before the ones ahead of it have been
/// answered?
///
/// Conservative by construction: everything that could change how a LATER
/// command is interpreted, or that `handle` answers without ever reaching
/// `forward`, is refused. A false negative costs a round trip; a false
/// positive corrupts a client's session.
fn prefetchable(args: &[Vec<u8>], name: &[u8], replica_reads: bool) -> bool {
    let is_write = flint_commands::is_write_command(name);
    let is_read = flint_commands::is_read_command(name);
    // Families, unknown verbs, and the connection-level commands are all
    // neither, and all take paths of their own.
    if !is_write && !is_read {
        return false;
    }
    let upper = name.to_ascii_uppercase();
    // Commands `handle` answers locally or fans out itself never reach
    // `forward`, so they must not be staged as though they would. SCAN is
    // classified as a read command, so this list is load-bearing rather than
    // defensive.
    if upper.starts_with(b"FLINT")
        || matches!(
            upper.as_slice(),
            b"PING"
                | b"ECHO"
                | b"QUIT"
                | b"SCAN"
                | b"DBSIZE"
                | b"FLUSHALL"
                | b"MULTI"
                | b"EXEC"
                | b"DISCARD"
                | b"WATCH"
                | b"UNWATCH"
        )
    {
        return false;
    }
    // A D7 replica read resolves its target differently and falls back to the
    // master when a replica errors. Left on the ordinary path: the fallback
    // is worth more than the batching.
    if replica_reads && is_read {
        return false;
    }
    // Needs a routable key — no-key commands go to pair 0 by a separate rule.
    route_key(args).is_some()
}

/// Stage the leading run of independent keyed commands, then flush once.
///
/// Only a PREFIX is considered. The first command that is not prefetchable
/// ends the run, because anything that changes connection state (AUTH binding
/// a namespace, MULTI opening a transaction) invalidates every assumption
/// this pass makes about the commands behind it. Real pipelines are
/// homogeneous runs, so a prefix captures essentially all of the win for a
/// fraction of the reasoning.
#[allow(clippy::too_many_arguments)]
async fn prefetch_run(
    topo: &Arc<Topology>,
    backends: &mut Option<Backends>,
    cmds: &[(Vec<Vec<u8>>, Vec<u8>)],
    ns: &[u8],
    txn_open: bool,
    replica_reads: bool,
    local_cache: bool,
    async_writes: bool,
    is_channel: bool,
) -> Vec<Prefetch> {
    let mut plan: Vec<Prefetch> = Vec::with_capacity(cmds.len());
    plan.resize_with(cmds.len(), || Prefetch::None);
    // One command cannot overlap with anything, and a channel's per-command
    // budget and an open transaction both make ordering load-bearing.
    if cmds.len() < 2 || txn_open || is_channel {
        return plan;
    }
    let mut staged = 0usize;
    // One lease per target address, held for the whole pass. Taking a fresh
    // connection per command would advance the lane's round-robin every time
    // and scatter this run across every connection in the lane — which is
    // exactly what happened when the lane widened to 8, and it cost the
    // batching the pass exists to create.
    let mut leases: HashMap<String, Rc<apool::AsyncConn>> = HashMap::new();
    for (i, (args, raw)) in cmds.iter().enumerate() {
        if i >= MAX_PREFETCH {
            break;
        }
        let Some(name) = args.first() else { break };
        if !prefetchable(args, name, replica_reads) {
            break;
        }
        let is_write = flint_commands::is_write_command(name);
        if let Some(shed) = topo.quota_gate(ns, name, is_write, false) {
            plan[i] = Prefetch::Shed(shed);
            continue;
        }
        let cacheable = local_cache
            && topo.cache.enabled()
            && args.len() == 2
            && args[0].eq_ignore_ascii_case(b"GET");
        if let Some(v) = cacheable.then(|| topo.cache.get(ns, &args[1])).flatten() {
            plan[i] = Prefetch::Cached(Value::Bulk(Some(v)));
            continue;
        }
        // Routing not settled (mid-failover): stop here and let `forward`'s
        // rediscovery loop do what it already does well, one command at a
        // time. Staging into an unknown topology would only queue commands
        // for a node we are about to stop believing in.
        let Some(addr) = route_key(args)
            .map(slot_for_key)
            .and_then(|s| topo.route(ns, s))
        else {
            break;
        };
        let b = backends.get_or_insert_with(|| {
            Backends::new(
                ns.to_vec(),
                topo.backend_tls.clone(),
                async_writes,
                topo.fanout_timeout,
            )
        });
        let lease = match leases.get(&addr) {
            Some(l) => l,
            None => match b.lease(&addr).await {
                // Dial failed: leave it to `forward`, which dials and retries
                // with the full failover budget.
                Err(_) => break,
                Ok(l) => leases.entry(addr.clone()).or_insert(l),
            },
        };
        match lease.stage(raw) {
            Ok(t) => {
                plan[i] = Prefetch::InFlight(t, addr);
                staged = i + 1;
            }
            // At the connection's in-flight cap, or the connection died. Stop
            // staging; the rest of the run takes the serial path, which is
            // self-limiting and so is its own backpressure.
            Err(_) => break,
        }
    }
    // ONE flush per connection, not per command: that is the batch. The whole
    // run was staged onto as few connections as it has target addresses, so a
    // 16-command pipeline to one node is one write syscall.
    //
    // A failed flush kills the connection, whose reader then fails every
    // waiter on it, so the error surfaces at collection rather than here.
    let _ = staged;
    for conn in leases.values() {
        let _ = conn.flush().await;
    }
    plan
}

#[allow(clippy::too_many_arguments)]
async fn data_command(
    topo: &Arc<Topology>,
    backends: &mut Option<Backends>,
    txn: &mut ProxyTxn,
    ns: &[u8],
    args: &[Vec<u8>],
    raw: &[u8],
    replica_reads: bool,
    local_cache: bool,
    async_writes: bool,
    is_channel: bool,
    prefetch: Prefetch,
) -> Value {
    // Decisions the prefetch pass already took are final — re-running the
    // quota gate here would charge the tenant's bucket twice for one command.
    let prefetched = match prefetch {
        Prefetch::Shed(v) | Prefetch::Cached(v) => return v,
        Prefetch::InFlight(t, addr) => Some((t, addr)),
        Prefetch::None => None,
    };
    if let Some((ticket, addr)) = prefetched {
        // Already gated, already a cache miss, already routed and on the
        // wire. What remains is the reply and the cache write-back.
        let cacheable = local_cache
            && topo.cache.enabled()
            && args.len() == 2
            && args[0].eq_ignore_ascii_case(b"GET");
        let b = backends.get_or_insert_with(|| {
            Backends::new(
                ns.to_vec(),
                topo.backend_tls.clone(),
                async_writes,
                topo.fanout_timeout,
            )
        });
        let reply = forward_collect(topo, b, ns, args, raw, ticket, addr).await;
        cache_writeback(topo, ns, args, &reply, local_cache, cacheable);
        return reply;
    }
    let is_write = args
        .first()
        .is_some_and(|n| flint_commands::is_write_command(n));
    let is_read = args
        .first()
        .is_some_and(|n| flint_commands::is_read_command(n));
    // ADR-0010 D1 resolution order: known write → known read → registered
    // family → unknown. A command that is neither a known write nor a known
    // read, but matches a registered family prefix, takes the family path;
    // everything else — INCLUDING an unregistered unknown command, which still
    // routes to a master and earns the server's error — is untouched. That
    // equivalence is the whole safety argument for shipping the table without
    // re-validating the command surface, and the drill proves it. Checked
    // before the quota gate: a family command is charged once at admission by
    // the forward slice, not through the read/write bucket (D1).
    if !is_write
        && !is_read
        && let Some(name) = args.first()
        && topo.family_registered(name)
    {
        // A co-processor's CHANNEL must not itself issue a family command. If it
        // did, family_command would reserve a FRESH in-flight slot and mint a
        // NEW channel token with a fresh budget and deadline — so a looping or
        // buggy co-processor could re-enter the family path without bound, which
        // is exactly the runaway the D3 resource class exists to cap. The
        // channel does STORAGE (read/write); families are a client-edge verb.
        // Refuse it flatly: no slot, no token, no dial (D3).
        if is_channel {
            return Value::Error(
                "ERR family commands are not available on a co-processor channel".into(),
            );
        }
        return family_command(topo, ns, args).await;
    }
    // M5 quota gate: a shed command must not touch the cache, a backend, or
    // the bucket-bypassing fast paths. Only data commands are charged
    // (PING/ECHO/QUIT are free; they cost this proxy nothing). A CHANNEL's
    // data commands are RATE-exempt (ADR-0010 D1): the family command was
    // charged the ops/s rate once at admission, and re-charging each channel
    // write would make one VEC.SET cost an implementation-defined number of
    // tokens. They are NOT storage-exempt — a channel that writes to an
    // over-quota tenant is shed just like a direct write (quota_gate's storage
    // verdict), or the per-tenant storage cap leaks through the co-processor.
    let is_data = is_write || is_read;
    if is_data
        && let Some(name) = args.first()
        && let Some(shed) = topo.quota_gate(ns, name, is_write, is_channel)
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
            Backends::new(
                ns.to_vec(),
                topo.backend_tls.clone(),
                async_writes,
                topo.fanout_timeout,
            )
        });
        if let Some(reply) = transaction_step(topo, b, txn, ns, args, raw).await {
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
    let b = backends.get_or_insert_with(|| {
        Backends::new(
            ns.to_vec(),
            topo.backend_tls.clone(),
            async_writes,
            topo.fanout_timeout,
        )
    });
    let reply = handle(topo, b, ns, args, raw, read_replica).await;
    cache_writeback(topo, ns, args, &reply, local_cache, cacheable);
    reply
}

/// Cache write-back for one completed data command: store a fresh GET reply,
/// and drop whatever a write just changed.
///
/// Shared by the ordinary path and the prefetched one, which must not
/// diverge: a prefetched write that skipped invalidation would leave this
/// proxy serving its own stale value, breaking read-your-own-writes for the
/// client that issued it.
fn cache_writeback(
    topo: &Arc<Topology>,
    ns: &[u8],
    args: &[Vec<u8>],
    reply: &Value,
    local_cache: bool,
    cacheable: bool,
) {
    if cacheable && let Value::Bulk(Some(v)) = reply {
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
async fn scan_forward(
    topo: &Arc<Topology>,
    backends: &mut Backends,
    ns: &[u8],
    args: &[Vec<u8>],
) -> Value {
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
    // O(keys) class: a SCAN step is bounded by COUNT in ROWS RETURNED, not in
    // rows EXAMINED — a cursor walking a mostly-expired or heavily-filtered
    // region does arbitrary work for one page. Same budget as DBSIZE.
    let reply = match backends.call_slow(&master, &out).await {
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

async fn handle(
    topo: &Arc<Topology>,
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
        b"SCAN" => scan_forward(topo, backends, ns, args).await,
        // Group-wide aggregates fan out.
        b"DBSIZE" => {
            fan_out(topo, backends, raw, |replies| {
                let mut total = 0i64;
                for r in replies {
                    match r {
                        Value::Integer(n) => total += n,
                        other => return Value::Error(format!("ERR dbsize fan-out: {other:?}")),
                    }
                }
                Value::Integer(total)
            })
            .await
        }
        b"FLUSHALL" => {
            fan_out(topo, backends, raw, |replies| {
                for r in replies {
                    if !matches!(&r, Value::Simple(s) if s == "OK") {
                        return Value::Error(format!("ERR flushall fan-out: {r:?}"));
                    }
                }
                Value::Simple("OK".into())
            })
            .await
        }
        _ => forward(topo, backends, ns, args, raw, read_replica).await,
    }
}

/// One CPWATCH session: subscribe, apply pushed snapshots, ACK each. The
/// snapshot frame is [SNAPSHOT, version, "a,b;c,d", "tok=ns,..."] — already
/// filtered by the control plane to this proxy's assigned tenants.
/// Wall-clock milliseconds, for the routing lines only.
///
/// Soak run 27's proxy log proved the promotion hint arrives and that routing
/// moves — and then could not say HOW LONG the pair sat with no master,
/// because the lines carried no time. That duration is the single number
/// separating "repeated promotions, each with a short no-master window" from
/// "one long window where the freshly promoted node would not claim the role"
/// (#187), and the run had to end without it. Same mistake as #188 one level
/// down: the event was logged, the instant was not. Milliseconds since the
/// epoch so a line can be sliced straight against the fleet journal, which
/// stamps the same way.
fn log_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// How many consecutive 30s idle reads to tolerate before concluding the
/// control-plane seat is gone rather than quiet (BUG-0081).
///
/// Ten is ~5 minutes. An idle fleet must never rotate; a silently partitioned
/// seat must eventually be abandoned. With no keepalive on CPWATCH those two
/// states are indistinguishable on the wire, so this constant is where the
/// trade is made -- and it is a trade, not a measurement.
const MAX_IDLE_READS: u32 = 10;

/// Label a control-plane dial failure with its phase, and say what EAGAIN
/// means here.
///
/// A FREE FUNCTION SO IT CAN BE TESTED. As a closure inside
/// `watch_control_plane` the wording was unreachable from any test, and the
/// claim it makes -- that this errno points at port exhaustion -- is exactly
/// the kind of claim that should not rest on having read it once.
fn connect_err(e: std::io::Error) -> std::io::Error {
    // The errno survives only until the error is rebuilt, so ask first.
    // EAGAIN and EWOULDBLOCK both arrive as WouldBlock.
    let starved = e.kind() == std::io::ErrorKind::WouldBlock;
    let labelled = std::io::Error::new(e.kind(), format!("connect: {e}"));
    if !starved {
        return labelled;
    }
    std::io::Error::new(
        labelled.kind(),
        format!(
            "{labelled} (EAGAIN before any read: the ephemeral port range may be \
             exhausted, or the routing cache full -- check TIME_WAIT volume with \
             `ss -s` on this host)"
        ),
    )
}

fn watch_control_plane(
    cp: &str,
    advertise: &str,
    topo: &Arc<Topology>,
    last_version: &mut u64,
) -> std::io::Result<()> {
    // BUG-0081. SAY WHICH CALL FAILED.
    //
    // connect, write and read all reached the caller as an undifferentiated
    // io::Error, so the log line named a seat and a message and not a PHASE.
    // That is why the filed diagnosis -- "EAGAIN on a watch socket is a read
    // timeout, the CP had nothing to push" -- went unchallenged: it is a
    // plausible reading of the message, and nothing in the output could
    // contradict it.
    //
    // The timings do. The rotation loop sleeps 1s per attempt, an attempt
    // that reaches the read and finds an idle CP costs the 30s set below, and
    // the CP sends nothing while idle -- so an idle seat logs every ~31s, not
    // once a second as observed. The error therefore arose BEFORE the read,
    // which no amount of staring at the message could establish and a phase
    // label states outright.
    //
    // ADR-0028: the verdict must name what it examined.
    let phase = |p: &'static str| {
        move |e: std::io::Error| std::io::Error::new(e.kind(), format!("{p}: {e}"))
    };
    // AND WHAT EAGAIN MEANS HERE, because reading it as a timeout is what sent
    // the first diagnosis of this bug down the wrong path.
    //
    // flint_tls::connect dials through TcpStream::connect_timeout, which sets
    // the socket non-blocking, calls connect(2) and returns anything that is
    // not EINPROGRESS. connect(2) documents EAGAIN for a TCP socket with no
    // bound address when the whole ephemeral range is in use -- that is the
    // errno for port exhaustion, where EADDRNOTAVAIL is the intuitive guess
    // and the wrong one. It fails in ~0s, so the rotation loop's 1s sleep
    // becomes the entire period: once a second, which is the cadence this bug
    // was actually reported at, and which an idle read (30s) cannot produce.
    //
    // BOTH DOCUMENTED CAUSES ARE NAMED rather than chosen between: a full
    // routing cache returns the same errno, and nothing at this call site can
    // tell them apart. Suggesting the wrong one with confidence is the failure
    // mode this whole bug is a record of.
    let mut stream = flint_tls::connect_reloadable(cp, &topo.backend_tls).map_err(connect_err)?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(phase("set_read_timeout"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .map_err(phase("set_write_timeout"))?;
    let mut out = Vec::new();
    encode(
        &Value::Array(Some(vec![
            Value::Bulk(Some(b"CPWATCH".to_vec())),
            Value::Bulk(Some(advertise.as_bytes().to_vec())),
            Value::Bulk(Some(last_version.to_string().into_bytes())),
        ])),
        &mut out,
    );
    stream.write_all(&out).map_err(phase("write CPWATCH"))?;
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    // Consecutive reads that returned nothing because the CP had nothing to
    // push. Reset by any successful read; see the NeedMore arm below.
    let mut idle_reads: u32 = 0;
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
                    // ADR-0010 D1: element 7 = the family route table. Option,
                    // not empty-string: ABSENT (older CP) leaves the table as
                    // it is, PRESENT-but-empty clears it. Everything else in
                    // this frame collapses absent to empty because empty is a
                    // valid steady state for it; for families it is not.
                    let families = match items.get(7) {
                        Some(Value::Bulk(Some(f))) => Some(String::from_utf8_lossy(f).to_string()),
                        _ => None,
                    };
                    topo.apply_snapshot(
                        &String::from_utf8_lossy(pairs),
                        &String::from_utf8_lossy(tenants),
                        &admin,
                        &exc,
                    );
                    topo.apply_promote_hint(&promo);
                    if let Some(f) = &families {
                        topo.apply_families(f);
                    }
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
                    stream.write_all(&out).map_err(phase("write ACK"))?;
                }
            }
            Ok(Decoded::NeedMore) => {
                // BUG-0081, first defect: AN IDLE CONTROL PLANE IS NOT A DEAD ONE.
                //
                // The read timeout above is 30s and deliberate, and the control
                // plane sends NOTHING while idle -- its `watch` pushes only when
                // the version advances past what this proxy has ACKed, waiting on
                // a condvar in 500ms slices with no keepalive on the wire. So on a
                // quiet fleet this read times out every 30s, and propagating that
                // rotated the seat: a fresh CPWATCH and a fresh filtered snapshot
                // every ~31s, forever, for nothing.
                //
                // The rotation exists for a real failure -- a proxy pinned to a
                // killed seat, reconnecting to a corpse while quorum stayed
                // healthy -- and that reasoning is sound. "This seat has nothing
                // to say yet" is not that failure.
                //
                // So keep waiting on the SAME connection. A seat that actually
                // dies gives EOF (handled below) or a hard error (handled here).
                //
                // BOUNDED, because a silently partitioned seat times out forever
                // and would otherwise never be rotated away from. This trades
                // detection latency for not churning, and the trade is only
                // necessary because silence and death look identical on this
                // socket. The real fix is a keepalive on CPWATCH, which is a
                // protocol change and is recorded in the bug rather than smuggled
                // in here.
                let n = match stream.read(&mut chunk) {
                    Ok(n) => {
                        idle_reads = 0;
                        n
                    }
                    Err(e)
                        if matches!(
                            e.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                        ) =>
                    {
                        idle_reads += 1;
                        if idle_reads >= MAX_IDLE_READS {
                            return Err(phase("read")(std::io::Error::new(
                                e.kind(),
                                format!(
                                    "silent for {idle_reads} consecutive reads (~{}s); \
                                     treating the seat as gone",
                                    idle_reads * 30
                                ),
                            )));
                        }
                        continue;
                    }
                    Err(e) => return Err(phase("read")(e)),
                };
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
                    // Validate the namespace to the SERVER's rule ([A-Za-z0-9._-],
                    // 1..=64), not a looser one. A looser check here is a silent
                    // footgun: `token=nsA#flags@rate` (the CP push format, which
                    // static mode does NOT parse — see below) passes a length/NUL
                    // check, so the namespace becomes the literal "nsA#flags@rate";
                    // the tenant then authenticates fine but every backend command
                    // fails the server's FLINTNS handshake and surfaces as the
                    // baffling "backend unavailable (failover did not settle)".
                    // Rejecting it here turns that into a clear boot-time error.
                    let ok_ns =
                        |b: &u8| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.');
                    assert!(
                        (1..=64).contains(&ns.len()) && ns.bytes().all(|b| ok_ns(&b)),
                        "invalid namespace {ns:?} in --tenants: expected 1..=64 chars of \
                         [A-Za-z0-9._-] (static mode takes token=ns only — flags and \
                         quotas come from the control plane, not this argument)"
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
    // Dispatchers per (node, namespace, async-writes) lane. Tunable because
    // the right width depends on how slow the slowest command on that node is:
    // a batch owns its connection for a whole write-then-read cycle, so a wide
    // lane buys isolation from head-of-line blocking at the cost of more
    // sockets. Default deliberately small — the point of ADR-0020 is to STOP
    // opening a connection per client.
    let topo = Arc::new(Topology {
        rediscover_gate: Mutex::new(HashMap::new()),
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
        // Sized to the KEYSPACE, not to taste: DBSIZE walks every metadata
        // row on the node it asks. Default 60s (~20M keys of headroom on
        // NVMe); raise it on fleets bigger than that.
        fanout_timeout: arg("--fanout-timeout-ms")
            .and_then(|v| v.parse().ok())
            .map(Duration::from_millis)
            .unwrap_or(FANOUT_TIMEOUT_DEFAULT),
        cert_path: arg("--internal-cert"),
        last_promote_hint: RwLock::new(String::new()),
        channel_tokens: std::sync::Mutex::new(HashMap::new()),
        families: RwLock::new(parse_families(&arg("--families").unwrap_or_default())),
        coproc_pool: std::sync::Mutex::new(HashMap::new()),
        edge_advertise: arg("--edge-advertise"),
        coproc_inflight: std::sync::atomic::AtomicUsize::new(0),
        coproc_max_inflight: {
            let v =
                parse_u64_knob("--family-max-inflight", arg("--family-max-inflight")).unwrap_or(64);
            assert!(
                v >= 1,
                "--family-max-inflight must be >= 1 (a 0 cap sheds every family command; to disable families, omit --families)"
            );
            v as usize
        },
        family_budget: {
            let v = parse_u64_knob("--family-budget", arg("--family-budget"))
                .unwrap_or(FAMILY_CHANNEL_BUDGET);
            assert!(
                v >= 1,
                "--family-budget must be >= 1 (a 0 budget refuses all channel I/O)"
            );
            v
        },
        family_deadline: {
            let ms = parse_u64_knob("--family-deadline-ms", arg("--family-deadline-ms"))
                .unwrap_or(FAMILY_CHANNEL_DEADLINE.as_millis() as u64);
            assert!(
                (1..=300_000).contains(&ms),
                "--family-deadline-ms must be 1..=300000 (0 disables the co-processor socket read timeout; a huge value risks Instant overflow)"
            );
            Duration::from_millis(ms)
        },
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
    let listener = std::net::TcpListener::bind((bind.as_str(), port))?;

    // ONE runtime per worker, each single-threaded, connections pinned to the
    // worker that accepted them (ADR-0021).
    //
    // Deliberately NOT one multi-threaded runtime. Tokio's work-stealing
    // scheduler may move a task between threads at any await, and `apool`'s
    // connections are `Rc`/`RefCell` owned by exactly one thread — the whole
    // reason they need no locks. A shared runtime would either not compile
    // (futures must be Send) or, with locks bolted back on, reproduce the
    // convoy this change exists to remove. Few owners is the property; a
    // single-threaded runtime per worker is how it is obtained.
    // One runtime per worker. Defaults to what this process may actually use —
    // `available_parallelism` honours the CPU affinity mask and cgroup quotas,
    // so a container limited to 2 CPUs on a 64-core host gets 2, which matters
    // because worker count also sets backend connection and mTLS session
    // count. Overridable so a deployment can pin a shape: the single-VM
    // packaging co-locates the proxy with both nodes and wants fewer, and the
    // chain-walk chaos gate runs 16 deliberately to maximise the number of
    // independent FIFO streams a mis-correlated reply could cross.
    //
    // The fallback, for when the query fails outright, is deliberately HIGH.
    // Measured on an 8-core box, 32 conns, pipeline 16: 1 worker 325k ops/s,
    // 4 (the peak here) 540k, 32 workers 443k. Under-provisioning by 4x costs
    // 40%; over-provisioning by 8x costs 18%. The curve is asymmetric, so when
    // we cannot detect, guessing high is the cheaper error.
    let workers = arg("--workers")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(16)
        })
        .clamp(1, 64);
    eprintln!(
        "flint-proxy listening on {bind}:{port} ({}, max-conns {max_conns}, {workers} workers)",
        if tls.is_some() { "TLS" } else { "plaintext" }
    );

    let mut inboxes = Vec::with_capacity(workers);
    for w in 0..workers {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<std::net::TcpStream>();
        inboxes.push(tx);
        let topo = Arc::clone(&topo);
        let tls = tls.clone();
        std::thread::Builder::new()
            .name(format!("flint-proxy-w{w}"))
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => {
                        eprintln!("worker {w}: runtime: {e}");
                        return;
                    }
                };
                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, async move {
                    while let Some(sock) = rx.recv().await {
                        let topo = Arc::clone(&topo);
                        let tls = tls.clone();
                        // The guard decrements the active count on ANY exit,
                        // including a panic inside the connection task.
                        let guard = ConnGuard(Arc::clone(&topo));
                        if sock.set_nonblocking(true).is_err() {
                            continue;
                        }
                        let Ok(sock) = tokio::net::TcpStream::from_std(sock) else {
                            continue;
                        };
                        let _ = sock.set_nodelay(true);
                        tokio::task::spawn_local(async move {
                            let _guard = guard;
                            // The edge config is snapshotted per connection, so
                            // a rotated cert applies to the next accept with no
                            // restart. A plaintext client hitting the TLS port
                            // fails the handshake and drops: no RESP is ever
                            // processed.
                            let cfg = tls.as_ref().and_then(|r| r.current());
                            match flint_tls::aio::accept(sock, &cfg).await {
                                Ok(s) => {
                                    let _ = serve_client(s, topo).await;
                                }
                                Err(e) => eprintln!("tls: connection setup failed: {e}"),
                            }
                        });
                    }
                });
            })?;
    }

    // The acceptor stays a plain blocking thread: it does one syscall per
    // connection and holds no per-connection state, so it needs none of the
    // machinery above.
    let mut next = 0usize;
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else { continue };
        // Admission control: reserve a slot with fetch_add, roll back and shed
        // if it put us over the cap (nothing is dispatched for a shed).
        topo.stat_conns_total.fetch_add(1, Ordering::Relaxed);
        if topo.stat_active.fetch_add(1, Ordering::Relaxed) >= max_conns {
            topo.stat_active.fetch_sub(1, Ordering::Relaxed);
            topo.stat_shed_total.fetch_add(1, Ordering::Relaxed);
            // Best-effort -THROTTLED for a plaintext client. Under frontend
            // TLS we skip the handshake (spending it on a shed would defeat
            // the guard) and just close — the client sees a reset and backs
            // off, the same contract.
            if tls.is_none() {
                use std::io::Write as _;
                let _ = stream.write_all(SHED_FRAME);
            }
            continue;
        }
        // Round-robin. A connection belongs to its worker for life, which is
        // what makes that worker's backend connections un-shared — and what
        // keeps a connection's failures inside one worker.
        let i = next % inboxes.len();
        next = next.wrapping_add(1);
        if inboxes[i].send(stream).is_err() {
            // Only reachable if that worker's runtime is gone. Roll the
            // admission slot back rather than leaking it.
            topo.stat_active.fetch_sub(1, Ordering::Relaxed);
        }
    }
    Ok(())
}

#[cfg(test)]
mod prefetch_tests {
    use super::*;

    fn may_stage(parts: &[&str], replica_reads: bool) -> bool {
        let args: Vec<Vec<u8>> = parts.iter().map(|p| p.as_bytes().to_vec()).collect();
        prefetchable(&args, &args[0], replica_reads)
    }

    /// The prefetch predicate is a safety boundary, not an optimisation knob:
    /// a false positive puts a command on the wire BEFORE the proxy has
    /// decided it may run, and before the commands ahead of it have been
    /// answered.
    ///
    /// SCAN is the case that forces the explicit list. It is classified as a
    /// READ command, so "is this a read or a write?" alone would happily
    /// stage it — but `handle` intercepts SCAN and drives its own per-master
    /// cursor session, so `forward` never sees it and the staged frame would
    /// be answered by somebody else's reply.
    #[test]
    fn prefetch_refuses_every_command_that_does_not_reach_forward() {
        // The ordinary keyed traffic this whole pass exists for.
        assert!(may_stage(&["GET", "k"], false));
        assert!(may_stage(&["SET", "k", "v"], false));
        assert!(
            may_stage(&["get", "k"], false),
            "command names are case-insensitive on the wire"
        );

        // Answered by `handle` or fanned out by it — never reach `forward`.
        for c in [
            vec!["SCAN", "0"],
            vec!["DBSIZE"],
            vec!["FLUSHALL"],
            vec!["PING"],
            vec!["ECHO", "x"],
            vec!["QUIT"],
        ] {
            assert!(
                !may_stage(&c, false),
                "{c:?} must not be staged: handle answers it without forwarding"
            );
        }

        // Per-connection state on the node — ordering is load-bearing, and a
        // queued command that executed instead of queueing would show the
        // client QUEUED followed by a partial apply.
        for c in [
            vec!["MULTI"],
            vec!["EXEC"],
            vec!["DISCARD"],
            vec!["WATCH", "k"],
            vec!["UNWATCH"],
        ] {
            assert!(
                !may_stage(&c, false),
                "{c:?} must not be staged: it is transaction state"
            );
        }

        // The tenant boundary: FLINT* must never leave the proxy, and the
        // prefetch pass runs BEFORE the check that enforces that.
        assert!(!may_stage(&["FLINTNS", "other"], false));
        assert!(!may_stage(&["flintkeysize", "k"], false));

        // A D7 replica read resolves its target differently and falls back to
        // the master on error; writes are unaffected and still stage.
        assert!(!may_stage(&["GET", "k"], true));
        assert!(
            may_stage(&["SET", "k", "v"], true),
            "a replica-reading tenant's WRITES still go to the master and may stage"
        );

        // No routable key: those go to pair 0 by a separate rule.
        assert!(!may_stage(&["MGET"], false));

        // Unknown verbs earn the node's own error on the ordinary path.
        assert!(!may_stage(&["NOTACOMMAND", "k"], false));
    }
}

#[cfg(test)]
mod route_tests {
    use super::*;

    /// A minimal Topology: one cluster view with the given pairs/masters/
    /// ranges, everything else inert. The routing logic under test reads
    /// only clusters/level0.
    fn topo(pairs: Vec<Vec<String>>, masters: Vec<Option<String>>) -> Topology {
        Topology {
            rediscover_gate: Mutex::new(HashMap::new()),
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
            fanout_timeout: FANOUT_TIMEOUT_DEFAULT,
            cert_path: None,
            admin_digests: RwLock::new(Vec::new()),
            last_promote_hint: RwLock::new(String::new()),
            channel_tokens: std::sync::Mutex::new(HashMap::new()),
            families: RwLock::new(Vec::new()),
            coproc_pool: std::sync::Mutex::new(HashMap::new()),
            edge_advertise: None,
            coproc_inflight: std::sync::atomic::AtomicUsize::new(0),
            coproc_max_inflight: 64,
            family_budget: FAMILY_CHANNEL_BUDGET,
            family_deadline: FAMILY_CHANNEL_DEADLINE,
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
    fn family_table_parses_and_matches_by_prefix() {
        // Wire form -> table. "=skipme" has an empty prefix (dropped); "JSON.="
        // has empty endpoints (KEPT — a registered family with no endpoint is
        // still -COPROCUNAVAIL, not "unregistered and routed to the master").
        let fams = parse_families(" VEC.=a:1, b:2 ; se=c:3 ; =skipme ; JSON.= ");
        let prefixes: Vec<String> = fams
            .iter()
            .map(|(p, _)| String::from_utf8_lossy(p).into_owned())
            .collect();
        assert_eq!(
            prefixes,
            vec!["VEC.", "SE", "JSON."],
            "prefixes uppercased, empty-prefix entry dropped"
        );
        assert_eq!(fams[0].1, vec!["a:1", "b:2"]);
        assert!(fams[2].1.is_empty(), "JSON. registered with no endpoints");

        // family_registered is a pure, case-insensitive prefix match. The
        // caller (data_command) gates it behind !is_write && !is_read, so the
        // fact that "SETEX" matches the SE family here is harmless — the
        // resolution order, not this fn, keeps SETEX a write.
        let t = two_pair_topo();
        t.apply_families("VEC.=a:1;SE=b:2");
        assert!(t.family_registered(b"VEC.SET"));
        assert!(t.family_registered(b"vec.get"), "case-insensitive");
        assert!(
            t.family_registered(b"SETEX"),
            "prefix match, resolution order handles the rest"
        );
        assert!(!t.family_registered(b"MAT.MUL"), "unregistered prefix");
        assert!(!t.family_registered(b"GET"));
        // An emptied table registers nothing (a CP that clears element 7).
        t.apply_families("");
        assert!(!t.family_registered(b"VEC.SET"));
    }

    /// D3: family admission is bounded FIRST under pressure. Reservations up to
    /// the cap succeed; the next is shed (`None`); dropping a guard frees
    /// exactly one slot, so the next reservation succeeds again. This is the
    /// counter that caps aggregate channel I/O — the data path is never bounded
    /// by it, which is why the shed here (a family command) is the correct one.
    #[test]
    fn family_inflight_cap_sheds() {
        let mut t = two_pair_topo();
        t.coproc_max_inflight = 2;

        let g1 = t.try_reserve_family();
        let g2 = t.try_reserve_family();
        assert!(
            g1.is_some() && g2.is_some(),
            "reservations up to the cap succeed"
        );
        assert!(
            t.try_reserve_family().is_none(),
            "at the cap, the next family command is shed"
        );

        drop(g1);
        let g3 = t.try_reserve_family();
        assert!(g3.is_some(), "dropping a guard frees exactly one slot");
        assert!(
            t.try_reserve_family().is_none(),
            "and only one — still at the cap"
        );

        drop(g2);
        drop(g3);
        // Fully drained: the cap is available again, counting up from zero.
        assert!(
            t.try_reserve_family().is_some(),
            "back to zero in-flight after every guard drops"
        );
    }

    /// D1 (corrected): a channel's data commands are exempt from the ops/s
    /// RATE — the family command paid that once at admission — but NOT from the
    /// storage over-quota shed. A co-processor that keeps writing to a tenant
    /// already over its storage cap would otherwise defeat the cap entirely, so
    /// the channel's writes are shed exactly like a direct write. This is the
    /// review gap: `rate_exempt` must not become storage-exempt.
    #[test]
    fn channel_data_is_rate_exempt_but_not_storage_exempt() {
        let t = two_pair_topo();
        let ns = b"nsA".as_slice();

        // over_quota=true, no ops/s limit: only the storage verdict is live.
        t.quota
            .write()
            .expect("quota lock")
            .insert(ns.to_vec(), (0, true));
        // A channel WRITE to an over-quota tenant is still shed with -QUOTA,
        // even though it is rate-exempt (rate_exempt=true).
        match t.quota_gate(ns, b"SET", true, true) {
            Some(Value::Error(e)) => {
                assert!(
                    e.starts_with("QUOTA"),
                    "storage shed must apply to a channel write: {e}"
                )
            }
            other => panic!("expected -QUOTA on an over-quota channel write, got {other:?}"),
        }
        // A channel READ is served — the storage verdict only sheds writes.
        assert!(
            t.quota_gate(ns, b"GET", false, true).is_none(),
            "reads are served over-quota, channel or not"
        );

        // rate=1, not over quota: now the ops/s bucket is the only gate.
        t.quota
            .write()
            .expect("quota lock")
            .insert(ns.to_vec(), (1, false));
        // Direct tenant traffic (rate_exempt=false) spends its one burst token,
        // then the next op in the same instant is THROTTLED.
        assert!(
            t.quota_gate(ns, b"GET", false, false).is_none(),
            "first tenant op passes"
        );
        match t.quota_gate(ns, b"GET", false, false) {
            Some(Value::Error(e)) => {
                assert!(
                    e.starts_with("THROTTLED"),
                    "tenant throttled after burst: {e}"
                )
            }
            other => panic!("expected THROTTLED on the second tenant op, got {other:?}"),
        }
        // A channel (rate_exempt=true) is never throttled by the ops/s bucket,
        // however many times it is called — the D1 rate exemption still holds.
        for _ in 0..1000 {
            assert!(
                t.quota_gate(ns, b"GET", false, true).is_none(),
                "channel data commands are rate-exempt"
            );
        }
    }

    /// D3: a co-processor's CHANNEL must not itself trigger a family command.
    /// A channel issuing `VEC.SET` would re-enter family_command, reserve a
    /// fresh in-flight slot and mint a new token with a fresh budget/deadline —
    /// the unbounded re-entry the resource class exists to cap. It is refused
    /// flatly with no slot taken, while the SAME command on a normal connection
    /// still routes to the family path.
    #[test]
    fn a_channel_cannot_issue_a_family_command() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        tokio::task::LocalSet::new().block_on(&rt, a_channel_cannot_issue_a_family_command_inner());
    }

    async fn a_channel_cannot_issue_a_family_command_inner() {
        let t = Arc::new(two_pair_topo());
        t.families
            .write()
            .expect("families lock")
            .push((b"VEC.".to_vec(), vec!["127.0.0.1:1".into()]));
        let args: Vec<Vec<u8>> = vec![b"VEC.SET".to_vec(), b"k".to_vec(), b"v".to_vec()];
        let mut backends = None;

        // On a CHANNEL: refused flatly, and no in-flight slot is reserved (D3).
        let mut txn_ch = ProxyTxn::default();
        let on_channel = data_command(
            &t,
            &mut backends,
            &mut txn_ch,
            b"nsA",
            &args,
            b"",
            false,
            false,
            false,
            true,
            Prefetch::None,
        )
        .await;
        match on_channel {
            Value::Error(e) => assert!(
                e.contains("not available on a co-processor channel"),
                "a channel's family command must be refused flatly: {e}"
            ),
            other => panic!("expected a flat refusal on a channel, got {other:?}"),
        }
        assert_eq!(
            t.coproc_inflight.load(std::sync::atomic::Ordering::Acquire),
            0,
            "the refused channel command must NOT reserve an in-flight slot"
        );

        // On a NORMAL connection: the SAME command still takes the family path
        // (COPROCUNAVAIL here, since the test topo advertises no callback edge).
        let mut txn = ProxyTxn::default();
        let on_client = data_command(
            &t,
            &mut backends,
            &mut txn,
            b"nsA",
            &args,
            b"",
            false,
            false,
            false,
            false,
            Prefetch::None,
        )
        .await;
        match on_client {
            Value::Error(e) => assert!(
                e.contains("COPROCUNAVAIL"),
                "a normal connection's family command still routes to the family path: {e}"
            ),
            other => panic!("expected COPROCUNAVAIL on a normal connection, got {other:?}"),
        }
    }

    /// The discriminator BUG-0055 turns on: back off only when the re-probe
    /// left routing pointing at the address that just refused us.
    ///
    /// Both directions asserted, because a predicate that always returned
    /// `true` would restore the unconditional sleep and one that always
    /// returned `false` would remove it entirely — and each of those is a
    /// previously-shipped behaviour, so neither would look obviously wrong.
    #[test]
    fn still_master_is_true_only_while_routing_names_the_refusing_seat() {
        let t = two_pair_topo();
        seed_master0(&t);
        assert!(
            t.still_master("a:1"),
            "routing names a:1 as master, so there is nothing new to retry against \
             and the caller must back off"
        );
        // A re-probe that finds no master clears the slot — the failover case.
        t.rediscover_after_failure("a:1");
        assert_eq!(
            master0(&t),
            None,
            "precondition: the probe cleared the slot"
        );
        assert!(
            !t.still_master("a:1"),
            "routing has moved off a:1, so a retry has somewhere else to go and \
             sleeping would only add latency"
        );
        // An address that was never a master anywhere.
        assert!(!t.still_master("z:9"), "an unknown address is not a master");
    }

    /// BUG-0052: while a probe is IN FLIGHT, every other caller for that
    /// address must be absorbed — however long the probe takes.
    ///
    /// The old gate stored only "a probe started at t" and compared against a
    /// 250 ms window, while `discover_master`'s read timeout is 800 ms PER
    /// NODE. So a caller arriving after 250 ms of an 800 ms probe found the
    /// window expired and started a second concurrent probe, each racing a
    /// write into `routing.masters`.
    ///
    /// The existing `simultaneous_request_failures_cause_one_reprobe` cannot
    /// see this: its address resolves to nothing, so every probe finishes in
    /// microseconds and no caller ever arrives during one. This test CONTROLS
    /// the duration — a listener that accepts and never answers, so the probe
    /// blocks on the read timeout — and has a second caller arrive at 300 ms,
    /// deliberately PAST the debounce window and INSIDE the probe.
    #[test]
    fn a_second_caller_during_a_slow_probe_is_absorbed() {
        use std::net::TcpListener;
        use std::sync::mpsc;

        // Accept and stay silent. `discover_master` connects, sends
        // FLINTINFO, and waits out its 800ms read timeout.
        let lp = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = format!(
            "127.0.0.1:{}",
            lp.local_addr().expect("listener addr").port()
        );
        std::thread::spawn(move || {
            let mut held = Vec::new();
            while let Ok((c, _)) = lp.accept() {
                held.push(c); // hold it open; never write
            }
        });

        let t = Arc::new(topo(vec![vec![addr.clone()]], vec![Some(addr.clone())]));

        // Caller one, in flight for ~800ms.
        let (tx, rx) = mpsc::channel();
        let t1 = Arc::clone(&t);
        let a1 = addr.clone();
        let h = std::thread::spawn(move || {
            let start = Instant::now();
            t1.rediscover_after_failure(&a1);
            let _ = tx.send(start.elapsed());
        });

        // Caller two arrives PAST the 250ms window and INSIDE the probe. This
        // is the case the old gate got wrong: it would find the window
        // expired and launch a concurrent probe, taking ~800ms itself.
        std::thread::sleep(Duration::from_millis(300));
        let start = Instant::now();
        t.rediscover_after_failure(&addr);
        let second = start.elapsed();

        let first = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("first probe finished");
        h.join().expect("probe thread");

        // Capability assert: if the probe was not actually slow, the second
        // caller never overlapped it and this proves nothing.
        assert!(
            first >= Duration::from_millis(250),
            "the probe finished in {first:?}, so the second caller did not arrive \
             during it and this test exercised nothing — the silent listener is \
             not holding the connection open"
        );
        assert!(
            second < Duration::from_millis(100),
            "a caller arriving during an in-flight probe took {second:?}: it \
             started its own concurrent probe instead of being absorbed (BUG-0052)"
        );

        // And the quiet period still applies once the probe is done.
        let start = Instant::now();
        t.rediscover_after_failure(&addr);
        assert!(
            start.elapsed() < Duration::from_millis(100),
            "the post-probe debounce did not absorb an immediate retry"
        );
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

    /// The herd a pooled-connection death produces must cost ONE re-probe.
    ///
    /// A pooled connection fails every in-flight command on it at once, and
    /// each caller independently asks for rediscovery. On a fleet that turned
    /// one death into 198 routing transitions, the master flapping
    /// `addr -> none -> addr` inside milliseconds while concurrent probes
    /// raced each other, and throughput collapsed to 0.12x with a 10 s p99.9.
    #[test]
    fn simultaneous_request_failures_cause_one_reprobe() {
        let t = two_pair_topo();
        seed_master0(&t);
        // First failure probes for real: "a:1" does not resolve, so the probe
        // finds no master and clears the slot.
        t.rediscover_after_failure("a:1");
        assert_eq!(
            master0(&t),
            None,
            "the first failure must actually re-probe"
        );
        // The rest of the herd arrives while that probe is still fresh and
        // must be absorbed — if they probed too, they would each race a write
        // into the routing table.
        seed_master0(&t);
        for _ in 0..32 {
            t.rediscover_after_failure("a:1");
        }
        assert_eq!(
            master0(&t),
            Some("a:1".into()),
            "32 further failures within the debounce must not re-probe at all"
        );
    }

    /// Coalescing must never swallow control-plane news. A promote hint says
    /// the topology HAS changed; a request failure only says one caller saw a
    /// symptom. Suppressing the former delays real failover.
    #[test]
    fn a_promote_hint_is_never_debounced() {
        let t = two_pair_topo();
        seed_master0(&t);
        t.rediscover_after_failure("a:1"); // arms the gate
        seed_master0(&t);
        t.apply_promote_hint("a:1|9");
        assert_eq!(
            master0(&t),
            None,
            "a promote hint must re-probe even immediately after a failure-driven one"
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

#[cfg(test)]
mod connect_err_tests {
    use super::connect_err;
    use std::io::{Error, ErrorKind};

    // BUG-0081. The hint must name the phase AND fire only on the errno it
    // is about. A hint that appended itself to every dial failure would be
    // the same defect this bug records -- a message that reads like a
    // diagnosis and is not one.
    #[test]
    fn eagain_is_labelled_and_explained() {
        let out = connect_err(Error::new(
            ErrorKind::WouldBlock,
            "Resource temporarily unavailable",
        ))
        .to_string();
        assert!(out.starts_with("connect: "), "phase must lead: {out}");
        assert!(
            out.contains("ephemeral port range"),
            "EAGAIN must be explained: {out}"
        );
        // Both documented causes, so the reader is not pointed at one.
        assert!(
            out.contains("routing cache"),
            "the other cause must be named: {out}"
        );
    }

    // THE NEGATIVE CONTROL. A refused connection is the ordinary case and
    // must carry the phase and nothing else; without this the assertion
    // above passes for a function that appends the hint unconditionally.
    #[test]
    fn other_errors_get_the_phase_and_no_hint() {
        let out = connect_err(Error::new(
            ErrorKind::ConnectionRefused,
            "Connection refused",
        ))
        .to_string();
        assert_eq!(out, "connect: Connection refused");
        assert!(!out.contains("ephemeral"), "hint must not fire here: {out}");
    }

    // The kind survives, because the caller rotates on any Err and a future
    // reader classifying by kind must still see the original.
    #[test]
    fn the_error_kind_is_preserved() {
        assert_eq!(
            connect_err(Error::new(ErrorKind::WouldBlock, "x")).kind(),
            ErrorKind::WouldBlock
        );
    }
}
