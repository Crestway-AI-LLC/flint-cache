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
    /// token -> namespace.
    tenants: RwLock<HashMap<String, Vec<u8>>>,
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
    /// Live client connections (the admission-control counter).
    stat_active: AtomicUsize,
    /// True only for a standalone proxy with no tenants configured: no
    /// auth, default namespace. A control-plane-fed proxy is never open —
    /// before its first snapshot it simply has no tenants yet.
    open_mode: bool,
    /// Client-side mutual-TLS config for dialing backends (the internal hop).
    /// `None` = plaintext backends (default). Set by `--internal-*`; the same
    /// triple the servers use, in the client role.
    backend_tls: Option<Arc<rustls::ClientConfig>>,
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
                let n = if ranged > 0 { ranged } else { routing.pairs.len() };
                (slot as usize * n) / 16384
            });
        routing.masters.get(pair)?.clone()
    }

    fn learn_moved(&self, ns: &[u8], slot: u16, addr: &str) {
        if let Ok(mut moved) = self.moved.write() {
            moved.insert((ns.to_vec(), slot), addr.to_string());
        }
    }

    fn lookup_token(&self, token: &str) -> Option<Vec<u8>> {
        self.tenants.read().ok()?.get(token).cloned()
    }

    fn bump_auth(&self, token: &str) {
        if let Ok(mut c) = self.auth_counts.write() {
            *c.entry(token.to_string()).or_insert(0) += 1;
        }
    }

    fn auth_count(&self, token: &str) -> u64 {
        self.auth_counts
            .read()
            .ok()
            .and_then(|c| c.get(token).copied())
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
    fn apply_snapshot(&self, pairs_spec: &str, tenants_spec: &str) {
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
        let new_tenants: HashMap<String, Vec<u8>> = tenants_spec
            .split(',')
            .filter_map(|pair| {
                let (token, ns) = pair.split_once('=')?;
                Some((token.to_string(), ns.as_bytes().to_vec()))
            })
            .collect();
        if let Ok(mut tenants) = self.tenants.write() {
            *tenants = new_tenants;
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
    tls: &Option<Arc<rustls::ClientConfig>>,
) -> Option<String> {
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
) -> Value {
    let slot = route_key(args).map(slot_for_key);
    let deadline = Instant::now() + RETRY_BUDGET;
    loop {
        // Resolve the target: keyed commands by slot; no-key commands go to
        // pair 0's master (v0: DBSIZE/FLUSHALL fan out below, before here).
        let target = match slot {
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
                // Backend died (or timed out): rediscover that pair's master
                // and retry — the failover-chasing path.
                if Instant::now() > deadline {
                    return Value::Error(
                        "ERR backend unavailable (failover did not settle)".into(),
                    );
                }
                topo.rediscover_for(&addr);
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
fn auth_step(topo: &Topology, authed_ns: &mut Option<Vec<u8>>, args: &[Vec<u8>]) -> AuthStep {
    let name = args.first().map(|n| n.to_ascii_uppercase());
    // Ops query: per-token AUTH count (drain check during token rotation).
    // Requires knowing the exact token; low-sensitivity, answered pre-auth.
    // (A real deploy gates this behind mTLS/admin.)
    // Ops query: aggregate proxy counters for the exporter (same pre-auth
    // rationale as PROXYAUTHCOUNT below). `active` is filled by the caller
    // holding the admission counter.
    if name.as_deref() == Some(b"PROXYSTATS") {
        let load = |c: &std::sync::atomic::AtomicU64| c.load(Ordering::Relaxed);
        let info = format!(
            "active:{}\r\nconns_total:{}\r\nshed_total:{}\r\nauth_ok_total:{}\r\nauth_fail_total:{}\r\ncommands_total:{}\r\n",
            topo.stat_active.load(Ordering::Relaxed),
            load(&topo.stat_conns_total),
            load(&topo.stat_shed_total),
            load(&topo.stat_auth_ok_total),
            load(&topo.stat_auth_fail_total),
            load(&topo.stat_commands_total),
        );
        return AuthStep::Reply(Value::Bulk(Some(info.into_bytes())));
    }
    if name.as_deref() == Some(b"PROXYAUTHCOUNT") {
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
        if topo.open_mode {
            return AuthStep::Reply(Value::Error(
                "ERR Client sent AUTH, but no tenants are configured".into(),
            ));
        }
        let Some(ns) = topo.lookup_token(&String::from_utf8_lossy(token)) else {
            topo.stat_auth_fail_total.fetch_add(1, Ordering::Relaxed);
            return AuthStep::Reply(Value::Error("WRONGPASS invalid token".into()));
        };
        match authed_ns {
            Some(cur) if *cur != ns => {
                return AuthStep::Reply(Value::Error(
                    "ERR already authenticated as another tenant; reconnect to switch".into(),
                ));
            }
            _ => {}
        }
        topo.bump_auth(&String::from_utf8_lossy(token));
        topo.stat_auth_ok_total.fetch_add(1, Ordering::Relaxed);
        *authed_ns = Some(ns);
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
                    let reply = match auth_step(&topo, &mut authed_ns, &args) {
                        AuthStep::Reply(v) => v,
                        AuthStep::Proceed(ns) => {
                            let b = backends
                                .get_or_insert_with(|| Backends::new(ns.clone(), topo.backend_tls.clone()));
                            handle(&topo, b, &ns, &args, &raw)
                        }
                    };
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

fn handle(
    topo: &Topology,
    backends: &mut Backends,
    ns: &[u8],
    args: &[Vec<u8>],
    raw: &[u8],
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
        _ => forward(topo, backends, ns, args, raw),
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
                if let Value::Array(Some(items)) = frame
                    && let [
                        Value::Bulk(Some(tag)),
                        Value::Integer(version),
                        Value::Bulk(Some(pairs)),
                        Value::Bulk(Some(tenants)),
                    ] = items.as_slice()
                    && tag.eq_ignore_ascii_case(b"SNAPSHOT")
                {
                    topo.apply_snapshot(
                        &String::from_utf8_lossy(pairs),
                        &String::from_utf8_lossy(tenants),
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

/// Build the server-side TLS config for client-facing termination
/// (mTLS block, increment 1 — the externally-visible hop). Clients speak
/// TLS to the proxy; the proxy terminates it and the existing RESP path
/// runs over the encrypted stream. Backend/control-plane hops stay
/// plaintext here — internal mTLS is the next increment.
///
/// `ring` is the crypto provider (pure-Rust build, no C toolchain), pinned
/// explicitly rather than via the process-default so a stray
/// `install_default` elsewhere can never change what this listener uses.
/// No client-auth yet: this increment is server-authenticated TLS
/// (confidentiality + server identity); requiring client certs is the
/// mutual half, layered on once internal hops carry certs too.
fn load_tls_config(cert_path: &str, key_path: &str) -> Arc<rustls::ServerConfig> {
    use std::io::BufReader;
    let cert_file = std::fs::File::open(cert_path)
        .unwrap_or_else(|e| panic!("open --tls-cert {cert_path}: {e}"));
    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(cert_file))
        .collect::<Result<_, _>>()
        .expect("parse certificate chain from --tls-cert");
    assert!(!certs.is_empty(), "--tls-cert {cert_path} has no certificates");
    let key_file = std::fs::File::open(key_path)
        .unwrap_or_else(|e| panic!("open --tls-key {key_path}: {e}"));
    let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))
        .expect("read private key from --tls-key")
        .unwrap_or_else(|| panic!("no private key found in --tls-key {key_path}"));
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("TLS protocol versions")
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("build TLS server config (cert/key mismatch?)");
    Arc::new(config)
}

fn main() -> std::io::Result<()> {
    let port: u16 = arg("--port").and_then(|p| p.parse().ok()).unwrap_or(7379);
    let control_plane = arg("--control-plane");

    // Client-facing TLS termination: both flags together enable it, neither
    // keeps the plaintext listener (byte-identical to pre-TLS). One without
    // the other is a config error, not a silent downgrade.
    let tls: Option<Arc<rustls::ServerConfig>> = match (arg("--tls-cert"), arg("--tls-key")) {
        (Some(cert), Some(key)) => Some(load_tls_config(&cert, &key)),
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
    let tenants: HashMap<String, Vec<u8>> = arg("--tenants")
        .map(|spec| {
            spec.split(',')
                .filter_map(|pair| {
                    let (token, ns) = pair.split_once('=')?;
                    assert!(
                        !ns.is_empty() && ns.len() <= 64 && !ns.contains('\0'),
                        "invalid namespace in --tenants"
                    );
                    Some((token.to_string(), ns.as_bytes().to_vec()))
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
    let backend_tls: Option<Arc<rustls::ClientConfig>> =
        match (arg("--internal-ca"), arg("--internal-cert"), arg("--internal-key")) {
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
        tenants: RwLock::new(tenants),
        auth_counts: RwLock::new(HashMap::new()),
        stat_conns_total: std::sync::atomic::AtomicU64::new(0),
        stat_shed_total: std::sync::atomic::AtomicU64::new(0),
        stat_auth_ok_total: std::sync::atomic::AtomicU64::new(0),
        stat_auth_fail_total: std::sync::atomic::AtomicU64::new(0),
        stat_commands_total: std::sync::atomic::AtomicU64::new(0),
        stat_active: AtomicUsize::new(0),
        open_mode,
        backend_tls,
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
