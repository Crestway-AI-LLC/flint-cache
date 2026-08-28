// SPDX-License-Identifier: Elastic-2.0
//! flint-controlplane (v1): the global control plane's tenant-registry +
//! snapshot-push half (design.md §2.2).
//!
//! Holds DURABLE INTENT only — what cannot be re-derived by observing data
//! nodes: the tenant registry (token, namespace, shuffle-shard proxy
//! subset), the proxy fleet, and group topology. Every mutation bumps a
//! version and persists atomically before it is observable. Never on the
//! data path: proxies serve from their last-pushed table if the control
//! plane is down (its outage pauses onboarding, not traffic).
//!
//! Snapshot push (the xDS pattern over RESP framing): a proxy subscribes
//! with `CPWATCH <its-addr> <last-version>`; the connection is hijacked and
//! the control plane pushes a filtered snapshot — the shared pair topology
//! plus ONLY that proxy's assigned tenants — whenever the version advances,
//! reading an ACK per push. The filtering is the sub-group boundary
//! (design.md §2.1): a proxy never holds tokens it does not serve.
//!
//! Admin API (RESP):
//!   CPADDPROXY <addr>                       register a fleet member
//!   CPDELPROXY <addr>                       retire a registration (and drop
//!                                           it from every tenant subset)
//!   CPADDPAIR <a,b[,c]>                     register a replica set
//!   CPADDTENANT <name> <token> <ns> [k]     add tenant; subset = shuffle
//!                                           shard of k (default 2) proxies
//!   CPSETSUBSET <name> <p1,p2|*|->          override subset: an explicit
//!                                           list, `*` = every registered
//!                                           proxy, `-` = NONE (drain). The
//!                                           reply says which, because `-`
//!                                           reads like "all" and means the
//!                                           opposite: a tenant set to `-`
//!                                           is served nowhere and answers
//!                                           -WRONGPASS at every edge.
//!   CPINFO                                  build, registry version, counts,
//!                                           and the controllers that have
//!                                           registered (ADR-0014 D1)
//!   CPCONTROLLER <host:pid> <build>         a controller announcing itself;
//!                                           it has no listener, so this is
//!                                           the only way its build reaches
//!                                           `status` without ssh
//!   CPMYSTATUS <token>                      TENANT-scoped self-view: quota,
//!                                           usage, flags, own endpoint,
//!                                           service build (ADR-0014 D3)
//!   CPSNAPSHOT <proxy-addr>                 one-shot filtered snapshot
//!
//! v1 scope: single node, durable file state (the serialized form is the
//! future Raft snapshot). openraft ×3 HA is the follow-on; so are deltas
//! (full snapshots are fine at this state size), token rotation, and DNS
//! publication of subsets.
//!
//! Usage: flint-controlplane --port 7500 --state /path/state

mod ha;
mod raft;
mod registry;
mod state;
mod tenant;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use flint_resp::{Decoded, Value, decode, encode};
use state::{State, Tenant, shuffle_shard};

fn arg(name: &str) -> Option<String> {
    std::env::args().skip_while(|a| a != name).nth(1)
}

/// State + a condvar so watch threads sleep until a mutation bumps the
/// version instead of polling.
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    /// Fleet-journal file (append-only JSONL beside --state). Observability,
    /// not intent: outside the durability contract of the registry.
    journal_path: String,
    /// Latest metered resident bytes per tenant NAME (CPTENANTUSAGE, set by
    /// the agent's metering sweep each interval). In-memory ONLY — telemetry
    /// for CPMYUSAGE/console reads, never persisted, never versioned.
    usage: Mutex<std::collections::HashMap<String, u64>>,
    /// The LEASE FAST PATH (ADR-0018): master-of-record mirror + renewal
    /// telemetry, under its OWN lock. CPLEASE touches only this — never
    /// `state` — so a renewal can never queue behind a snapshot being
    /// serialized or a commit being fsynced. A renewal that waits past the
    /// TTL is indistinguishable from a partition to the master holding it,
    /// and CP OVERLOAD must not be able to fence the fleet. Writes (CPFENCE,
    /// adoption) go through `state` first for durability, then here; lock
    /// order is always state -> leases, never both held for a renewal.
    leases: Mutex<LeaseFast>,
}

/// See `Shared::leases`. `entries` mirrors `State::leases`; the ring holds
/// the last 128 renewal latencies (µs) for CPINFO's `lease_p99_us` — the
/// gauge that says whether the isolation above is actually holding.
#[derive(Default)]
struct LeaseFast {
    entries: Vec<(Vec<String>, String, u64)>,
    renewals_total: u64,
    lat_us: Vec<u32>,
    lat_idx: usize,
}

impl LeaseFast {
    fn record_latency(&mut self, us: u32) {
        if self.lat_us.len() < 128 {
            self.lat_us.push(us);
        } else {
            self.lat_us[self.lat_idx % 128] = us;
        }
        self.lat_idx = (self.lat_idx + 1) % 128;
    }
    fn p99_us(&self) -> u32 {
        if self.lat_us.is_empty() {
            return 0;
        }
        let mut v = self.lat_us.clone();
        v.sort_unstable();
        v[(v.len() * 99 / 100).min(v.len() - 1)]
    }
}

fn ok() -> Value {
    Value::Simple("OK".into())
}

fn err(msg: &str) -> Value {
    Value::Error(format!("ERR {msg}"))
}

/// Validate the space-free tokens the line format depends on.
fn clean(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':' | b','))
}

fn handle(shared: &Shared, args: &[Vec<u8>]) -> Value {
    let cmd = args
        .first()
        .map(|c| c.to_ascii_uppercase())
        .unwrap_or_default();
    let text = |i: usize| -> Option<String> {
        args.get(i)
            .and_then(|r| std::str::from_utf8(r).ok())
            .map(String::from)
    };
    match cmd.as_slice() {
        b"PING" => Value::Simple("PONG".into()),
        b"CPADDPROXY" => {
            let Some(addr) = text(1).filter(|a| clean(a)) else {
                return err("CPADDPROXY <addr>");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            if !st.proxies.contains(&addr) {
                st.proxies.push(addr);
                match st.commit() {
                    Ok(_) => {}
                    Err(e) => return err(&format!("persist: {e}")),
                }
                shared.changed.notify_all();
            }
            ok()
        }
        b"CPDELPROXY" => {
            let Some(addr) = text(1) else {
                return err("CPDELPROXY <addr>");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            if !st.proxies.iter().any(|p| p == &addr) {
                return err(&format!("no such proxy {addr}"));
            };
            st.proxies.retain(|p| p != &addr);
            // A retired proxy must not linger in any tenant's subset: leaving
            // it there is the same trap one level down, and the tenant keeps
            // a placement slot pointing at nothing.
            for t in st.tenants.values_mut() {
                t.subset.retain(|p| p != &addr);
            }
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            Value::Simple(format!("OK retired {addr}"))
        }
        b"CPADDPAIR" => {
            let Some(nodes) = text(1).filter(|a| clean(a)) else {
                return err("CPADDPAIR <a,b[,c]> [start-end|-]");
            };
            // Optional slot range: level-1 routing state. "-" (or absent) =
            // unranged; an EXPANSION pair should pass "-" so joining adds
            // capacity without re-routing unmigrated slots.
            let range = text(2).as_deref().and_then(state::parse_range);
            // SORTED, because the dedupe below is vector EQUALITY and the
            // lease lookups are membership CONTAINMENT. Unsorted, `CPADDPAIR
            // a,b` and `CPADDPAIR b,a` are two pairs to the equality check and
            // one pair to every containment check -- which is how a lease row
            // written by one key can be read through another (BUG-0065).
            // Canonicalising here makes the existing `contains` dedupe do what
            // it already reads as doing.
            let mut pair: Vec<String> = nodes.split(',').map(String::from).collect();
            pair.sort();
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            if !st.pairs.contains(&pair) {
                st.pairs.push(pair);
                while st.ranges.len() < st.pairs.len() - 1 {
                    st.ranges.push(None);
                }
                st.ranges.push(range);
                match st.commit() {
                    Ok(_) => {}
                    Err(e) => return err(&format!("persist: {e}")),
                }
                shared.changed.notify_all();
            }
            ok()
        }
        // Replace pair <idx>'s membership (node swap). The pair id is the
        // stable identity (slot ranges are positional); membership floats.
        b"CPSETPAIR" => {
            let (Some(idx), Some(nodes)) = (
                text(1).and_then(|v| v.parse::<usize>().ok()),
                text(2).filter(|a| clean(a)),
            ) else {
                return err("CPSETPAIR <idx> <a,b[,c]>");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(p) = st.pairs.get_mut(idx) else {
                return err("no such pair index");
            };
            *p = nodes.split(',').map(String::from).collect();
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        b"CPADDTENANT" => {
            let (Some(name), Some(token), Some(ns)) = (text(1), text(2), text(3)) else {
                return err("CPADDTENANT <name> <token> <ns> [k]");
            };
            if !clean(&name) || !clean(&token) || !clean(&ns) {
                return err("invalid name/token/ns (space-free, <=128 chars)");
            }
            let k: usize = text(4).and_then(|v| v.parse().ok()).unwrap_or(2);
            // ADR-0006 D1: only the DIGEST is stored/pushed; the plaintext
            // dies with this request.
            let token = flint_tls::sha256_hex(token.as_bytes());
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            if st.tenants.contains_key(&name) {
                return err("tenant exists");
            }
            if st.tenants.values().any(|t| t.token == token) {
                return err("token already in use");
            }
            let subset = shuffle_shard(&name, &st.proxies, k);
            let reply = format!("OK tenant {name} ns {ns} subset [{}]", subset.join(","));
            st.tenants.insert(
                name.clone(),
                Tenant {
                    name,
                    token,
                    ns,
                    subset,
                    prev_token: None,
                    replica_reads: false,
                    local_cache: false,
                    federated: false,
                    async_writes: false,
                    ops_per_sec: 0,
                    max_bytes: 0,
                    over_quota: false,
                },
            );
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            Value::Simple(reply)
        }
        b"CPDELTENANT" => {
            // Remove a tenant: the record (auth revoked on the next snapshot
            // push — proxies drop the grant) and its namespace's slot-map
            // exception rows (a dead ns must not pin slot ownership). DATA
            // is not touched here — the CP holds no data; `flintctl tenant
            // remove` follows with a namespace wipe on the pairs.
            let Some(name) = text(1) else {
                return err("CPDELTENANT <name>");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st.tenants.remove(&name) else {
                return err("no such tenant");
            };
            let ns = t.ns.clone();
            st.exceptions.retain(|(e_ns, _, _, _)| e_ns != &ns);
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            drop(st);
            if let Ok(mut usage) = shared.usage.lock() {
                usage.remove(&name);
            }
            shared.changed.notify_all();
            Value::Simple(format!("OK removed {name} ns {ns}"))
        }
        b"CPSETSUBSET" => {
            let (Some(name), Some(subset)) = (text(1), text(2)) else {
                return err("CPSETSUBSET <name> <p1,p2|*|->");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let all: Vec<String> = st.proxies.clone();
            let Some(t) = st.tenants.get_mut(&name) else {
                return err("no such tenant");
            };
            t.subset = match subset.as_str() {
                "-" => Vec::new(),
                "*" => all,
                list => list.split(',').map(String::from).collect(),
            };
            let placed = t.subset.len();
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            // Say what happened. `+OK` after setting a subset to `-` looks
            // identical to `+OK` after placing the tenant everywhere, and the
            // two differ by "serves nowhere".
            Value::Simple(if placed == 0 {
                "OK subset = NONE — this tenant is DRAINED and will answer -WRONGPASS at every edge"
                    .to_string()
            } else {
                format!("OK subset = {placed} proxy(ies)")
            })
        }
        // Option B: record slot-ownership truth at cutover. Owner may be a
        // pair INDEX or any member address (resolved here) — stored as the
        // index so routing follows that pair's failovers automatically.
        b"CPSETSLOT" => {
            let (Some(ns), Some(slot), Some(owner)) = (
                text(1),
                text(2).and_then(|v| v.parse::<u16>().ok()),
                text(3),
            ) else {
                return err("CPSETSLOT <ns> <slot> <pair-idx|member-addr>");
            };
            // flint_slot::SLOT_COUNT without taking the dep for one constant.
            if slot >= 16384 {
                return err("slot out of range");
            }
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let pair: Option<u16> = owner.parse::<u16>().ok().or_else(|| {
                st.pairs
                    .iter()
                    .position(|p| p.contains(&owner))
                    .map(|i| i as u16)
            });
            let Some(pair) = pair.filter(|p| (*p as usize) < st.pairs.len()) else {
                return err("owner is neither a pair index nor a member address");
            };
            st.set_exception(&ns, slot, pair);
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        // SHARP EDGE (documented, not guarded — the CP cannot see data
        // placement): clearing a row makes routing fall back to the range
        // default. Safe ONLY after the slot's data moved back, or as part
        // of a consolidation that rewrites the pair range to cover the
        // exception. Clearing while data sits on the excepted pair SPLITS
        // ownership: reads answer nil from the wrong pair, writes land
        // there. Operator-invoked only; automation must use the future
        // consolidation op.
        b"CPCLEARSLOT" => {
            let (Some(ns), Some(slot)) = (text(1), text(2).and_then(|v| v.parse::<u16>().ok()))
            else {
                return err("CPCLEARSLOT <ns> <slot>");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            if !st.clear_exception(&ns, slot) {
                return err("no such exception");
            }
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        // The queryable ownership truth: "ns lo hi pair" per RUN.
        b"CPSLOTS" => {
            let Ok(st) = shared.state.lock() else {
                return err("state lock");
            };
            Value::Array(Some(
                st.exceptions
                    .iter()
                    .map(|(ns, lo, hi, pair)| {
                        Value::Bulk(Some(format!("{ns} {lo} {hi} {pair}").into_bytes()))
                    })
                    .collect(),
            ))
        }
        // The consolidation sweep, cron-able: merge adjacent runs and drop
        // rows redundant against the default ranges. Replies with the row
        // count that remains.
        b"CPCONSOLIDATE" => {
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let rows = st.consolidate();
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            Value::Integer(rows as i64)
        }
        // Federation flag (ADR-0007, plumbing): marks the tenant as served
        // by a dedicated multi-cluster proxy group. Rides the snapshot as
        // 'f'; no routing consequence until the fleet-map work lands.
        // Async write-queue opt-in (ADR-0005 D4) — the hot-key write
        // mitigation, operator-set. Pushed to the proxy as the 'a' flag on
        // the next snapshot; applies to client connections authed after it.
        b"CPTENANTASYNC" => {
            let (Some(name), Some(mode)) = (text(1), text(2)) else {
                return err("CPTENANTASYNC <name> <on|off>");
            };
            let on = match mode.as_str() {
                "on" => true,
                "off" => false,
                _ => return err("CPTENANTASYNC <name> <on|off>"),
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st.tenants.get_mut(&name) else {
                return err("no such tenant");
            };
            t.async_writes = on;
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        b"CPTENANTFEDERATE" => {
            let (Some(name), Some(mode)) = (text(1), text(2)) else {
                return err("CPTENANTFEDERATE <name> <on|off>");
            };
            let on = match mode.as_str() {
                "on" => true,
                "off" => false,
                _ => return err("CPTENANTFEDERATE <name> <on|off>"),
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st.tenants.get_mut(&name) else {
                return err("no such tenant");
            };
            t.federated = on;
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        // Replica-read opt-in for a tenant (ADR-0005 D7). Pushed to the
        // proxy on the next snapshot; writes stay on the master regardless.
        b"CPTENANTREADS" => {
            let (Some(name), Some(mode)) = (text(1), text(2)) else {
                return err("CPTENANTREADS <name> <on|off>");
            };
            let on = match mode.as_str() {
                "on" => true,
                "off" => false,
                _ => return err("CPTENANTREADS <name> <on|off>"),
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st.tenants.get_mut(&name) else {
                return err("no such tenant");
            };
            t.replica_reads = on;
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        // Proxy near-cache opt-in for a tenant (ADR-0005 D6): the tenant
        // accepts TTL-bounded stale reads from the proxy's local cache.
        // The cache's TTL/size are the PROXY operator's runtime knobs
        // (PROXYCACHE); this flag is the tenant's consent.
        b"CPTENANTCACHE" => {
            let (Some(name), Some(mode)) = (text(1), text(2)) else {
                return err("CPTENANTCACHE <name> <on|off>");
            };
            let on = match mode.as_str() {
                "on" => true,
                "off" => false,
                _ => return err("CPTENANTCACHE <name> <on|off>"),
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st.tenants.get_mut(&name) else {
                return err("no such tenant");
            };
            t.local_cache = on;
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        // Tenant quotas (M5): fleet ops/s + storage bytes; 0 = unlimited.
        // The rate reaches proxies pre-divided by subset size (tenant.rs);
        // the bytes cap is the metering loop's input, not the proxy's.
        b"CPTENANTQUOTA" => {
            let (Some(name), Some(ops), Some(bytes)) = (
                text(1),
                text(2).and_then(|v| v.parse::<u64>().ok()),
                text(3).and_then(|v| v.parse::<u64>().ok()),
            ) else {
                return err("CPTENANTQUOTA <name> <ops_per_sec> <max_bytes>");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st.tenants.get_mut(&name) else {
                return err("no such tenant");
            };
            t.ops_per_sec = ops;
            t.max_bytes = bytes;
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        // Co-processor command families (ADR-0010 D1): the CP is the fleet's
        // single source for the family route table (proxies otherwise only get
        // it from the static --families flag). Prefix uppercased so state is
        // canonical; endpoints are opaque host:port strings the proxy dials.
        b"CPFAMILY" => {
            let (Some(prefix), Some(addrs)) = (text(1), text(2)) else {
                return err("CPFAMILY <prefix> <host:port[,host:port]>");
            };
            let prefix = prefix.to_ascii_uppercase();
            let endpoints: Vec<String> = addrs
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if prefix.is_empty() || endpoints.is_empty() {
                return err("CPFAMILY <prefix> <host:port[,host:port]>");
            }
            if !crate::tenant::valid_family_prefix(&prefix) {
                return err(
                    "CPFAMILY <prefix> must be printable ASCII without spaces, '=', ';' or ','",
                );
            }
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            st.families.insert(prefix, endpoints);
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        b"CPFAMILYCLEAR" => {
            let Some(prefix) = text(1) else {
                return err("CPFAMILYCLEAR <prefix>");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            st.families.remove(&prefix.to_ascii_uppercase());
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        b"CPFAMILIES" => {
            let Ok(st) = shared.state.lock() else {
                return err("state lock");
            };
            let body = st
                .families
                .iter()
                .map(|(p, a)| format!("{p} {}", a.join(",")))
                .collect::<Vec<_>>()
                .join("\n");
            Value::Bulk(Some(body.into_bytes()))
        }
        // The metering loop's storage verdict (M5): flips the 'q' flag the
        // proxies shed writes on. Operator-invocable too (support cases).
        b"CPTENANTOVERQUOTA" => {
            let (Some(name), Some(mode)) = (text(1), text(2)) else {
                return err("CPTENANTOVERQUOTA <name> <on|off>");
            };
            let on = match mode.as_str() {
                "on" => true,
                "off" => false,
                _ => return err("CPTENANTOVERQUOTA <name> <on|off>"),
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st.tenants.get_mut(&name) else {
                return err("no such tenant");
            };
            t.over_quota = on;
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        b"CPROTATETOKEN" => {
            let (Some(name), Some(new)) = (text(1), text(2)) else {
                return err("CPROTATETOKEN <name> <new-token>");
            };
            if !clean(&new) {
                return err("invalid token");
            }
            let new = flint_tls::sha256_hex(new.as_bytes());
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            if st
                .tenants
                .values()
                .any(|t| t.token == new || t.prev_token.as_deref() == Some(new.as_str()))
            {
                return err("token already in use");
            }
            let Some(t) = st.tenants.get_mut(&name) else {
                return err("no such tenant");
            };
            t.prev_token = Some(std::mem::replace(&mut t.token, new));
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            Value::Simple("OK rotated (both tokens valid until CPDROPPREV)".into())
        }
        b"CPDROPPREV" => {
            let Some(name) = text(1) else {
                return err("CPDROPPREV <name>");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st.tenants.get_mut(&name) else {
                return err("no such tenant");
            };
            t.prev_token = None;
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        // The registered proxy fleet, comma-joined — the exporter's poll list.
        // Agent-set usage gauge (metering sweep): latest resident bytes per
        // tenant. Telemetry, in-memory, unversioned.
        b"CPTENANTUSAGE" => {
            let (Some(name), Some(bytes)) = (text(1), text(2).and_then(|v| v.parse::<u64>().ok()))
            else {
                return err("CPTENANTUSAGE <name> <bytes>");
            };
            if let Ok(mut usage) = shared.usage.lock() {
                usage.insert(name, bytes);
            }
            ok()
        }
        // Tenant-scoped self-service view (console v1): the TOKEN is the
        // credential — a tenant can read exactly its own row, nothing else.
        // Reply: "name ns ops_per_sec max_bytes over_quota resident_bytes".
        b"CPMYUSAGE" => {
            let Some(token) = text(1) else {
                return err("CPMYUSAGE <token>");
            };
            let token = flint_tls::sha256_hex(token.as_bytes());
            let Ok(st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st
                .tenants
                .values()
                .find(|t| t.token == token || t.prev_token.as_deref() == Some(token.as_str()))
            else {
                return Value::Error("WRONGPASS invalid token".into());
            };
            let bytes = shared
                .usage
                .lock()
                .ok()
                .and_then(|u| u.get(&t.name).copied())
                .unwrap_or(0);
            Value::Bulk(Some(
                format!(
                    "{} {} {} {} {} {} {} {} {}\r\n",
                    t.name,
                    t.ns,
                    t.ops_per_sec,
                    t.max_bytes,
                    t.over_quota as u8,
                    bytes,
                    t.replica_reads as u8,
                    t.local_cache as u8,
                    t.async_writes as u8
                )
                .into_bytes(),
            ))
        }
        b"CPMYSTATUS" => {
            // ADR-0014 D3. CPMYCONFIG is a SETTER and CPMYUSAGE returns a
            // bare positional line; a tenant had no way to ask "what is my
            // quota, which flags am I on, what am I connected to". The
            // console's /api/overview answers some of it for the SaaS path
            // only, so self-hosted and marketplace tenants had nothing.
            //
            // Same token-digest lookup as CPMYUSAGE — no new
            // authentication path, and the scoping is therefore the one
            // already in use rather than a second implementation of it.
            let Some(token) = text(1) else {
                return err("CPMYSTATUS <token>");
            };
            let token = flint_tls::sha256_hex(token.as_bytes());
            let Ok(st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st
                .tenants
                .values()
                .find(|t| t.token == token || t.prev_token.as_deref() == Some(token.as_str()))
            else {
                return Value::Error("WRONGPASS invalid token".into());
            };
            let bytes = shared
                .usage
                .lock()
                .ok()
                .and_then(|u| u.get(&t.name).copied())
                .unwrap_or(0);
            // `endpoint` is the tenant's OWN proxy subset — what they
            // already dial, and what CPSNAPSHOT already tells them. Not a
            // topology leak, and the distinction the ADR draws: their
            // endpoint yes, node addresses and pair layout no. Nothing
            // below reads any tenant but this one.
            Value::Bulk(Some(
                format!(
                    "tenant:{}\r\nnamespace:{}\r\nendpoint:{}\r\n\
                     quota_ops_per_sec:{}\r\nquota_max_bytes:{}\r\n\
                     usage_bytes:{}\r\nover_quota:{}\r\n\
                     replica_reads:{}\r\nlocal_cache:{}\r\nasync_writes:{}\r\n\
                     federated:{}\r\nbuild:{}\r\n",
                    t.name,
                    t.ns,
                    if t.subset.is_empty() {
                        "-".to_string()
                    } else {
                        t.subset.join(",")
                    },
                    t.ops_per_sec,
                    t.max_bytes,
                    bytes,
                    t.over_quota as u8,
                    t.replica_reads as u8,
                    t.local_cache as u8,
                    t.async_writes as u8,
                    t.federated as u8,
                    build_version(),
                )
                .into_bytes(),
            ))
        }
        // Tenant SELF-ROTATION (ADR-0006 D3): the CURRENT token is the
        // credential; the CP MINTS the successor (tenants never choose
        // secrets), stores only its digest, and returns the plaintext ONCE
        // — the caller's copy is the only copy. The old token stays valid
        // (dual-version window) until the rotation loop observes it
        // drained and drops it. A rotation already in flight is refused:
        // the window holds exactly two tokens.
        b"CPMYROTATE" => {
            let Some(token) = text(1) else {
                return err("CPMYROTATE <current-token>");
            };
            let digest = flint_tls::sha256_hex(token.as_bytes());
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st.tenants.values_mut().find(|t| t.token == digest) else {
                return Value::Error(
                    "WRONGPASS invalid token (rotation needs the CURRENT token)".into(),
                );
            };
            if t.prev_token.is_some() {
                return err("rotation in progress; previous token not yet drained");
            }
            let new_plain = flint_tls::mint_token();
            let new_digest = flint_tls::sha256_hex(new_plain.as_bytes());
            t.prev_token = Some(std::mem::replace(&mut t.token, new_digest));
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            Value::Bulk(Some(new_plain.into_bytes()))
        }
        // Tenant SELF-SERVICE config (portal): the token is the credential;
        // a tenant may toggle exactly the two consent knobs that are ITS
        // choice by contract (ADR-0005: replica reads, near-cache) — never
        // quotas, never another tenant. Pushed to proxies like any change.
        b"CPMYCONFIG" => {
            let (Some(token), Some(setting), Some(mode)) = (text(1), text(2), text(3)) else {
                return err("CPMYCONFIG <token> <replica-reads|near-cache|async-writes> <on|off>");
            };
            let token = flint_tls::sha256_hex(token.as_bytes());
            let on = match mode.as_str() {
                "on" => true,
                "off" => false,
                _ => {
                    return err(
                        "CPMYCONFIG <token> <replica-reads|near-cache|async-writes> <on|off>",
                    );
                }
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st
                .tenants
                .values_mut()
                .find(|t| t.token == token || t.prev_token.as_deref() == Some(token.as_str()))
            else {
                return Value::Error("WRONGPASS invalid token".into());
            };
            match setting.as_str() {
                "replica-reads" => t.replica_reads = on,
                "near-cache" => t.local_cache = on,
                // The tenant's OWN latency trade (ADR-0005 D4): coalesce
                // its batchable writes through the node queue.
                "async-writes" => t.async_writes = on,
                _ => return err("unknown setting (replica-reads|near-cache|async-writes)"),
            }
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        // Tenant metering view (M5): one "name ns ops_per_sec max_bytes
        // over_quota" line per tenant — the agent's sweep input. Tokens are
        // deliberately NOT included (this surface feeds metering, not auth).
        b"CPTENANTS" => {
            let Ok(st) = shared.state.lock() else {
                return err("state lock");
            };
            let usage = shared.usage.lock().ok();
            let mut out = String::new();
            for t in st.tenants.values() {
                let bytes = usage
                    .as_ref()
                    .and_then(|u| u.get(&t.name).copied())
                    .unwrap_or(0);
                out.push_str(&format!(
                    "{} {} {} {} {} {} {} {} {} {} {}\r\n",
                    t.name,
                    t.ns,
                    t.ops_per_sec,
                    t.max_bytes,
                    t.over_quota as u8,
                    bytes,
                    t.replica_reads as u8,
                    t.local_cache as u8,
                    t.prev_token.as_deref().unwrap_or("-"),
                    t.federated as u8,
                    t.async_writes as u8
                ));
            }
            Value::Bulk(Some(out.into_bytes()))
        }
        // Fleet admin token: current (returned to the mesh-authenticated
        // agent so it can present it to proxies) — this port is the mTLS CP
        // surface, so the caller is already an operator.
        b"CPADMINTOKEN" => {
            let Ok(st) = shared.state.lock() else {
                return err("state lock");
            };
            match &st.admin_token {
                Some(t) => Value::Bulk(Some(t.clone().into_bytes())),
                None => Value::Bulk(None),
            }
        }
        // Rotate the fleet admin token (ADR-0006 D4): mint the successor,
        // current -> previous, return the new plaintext ONCE. Both remain
        // valid (proxies get both digests) until the agent retires prev.
        b"CPADMINROTATE" => {
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            if st.admin_prev.is_some() {
                return err("admin rotation in progress; previous token not yet retired");
            }
            let new_plain = flint_tls::mint_token();
            st.admin_prev = st.admin_token.take();
            st.admin_token = Some(new_plain.clone());
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            Value::Bulk(Some(new_plain.into_bytes()))
        }
        // Retire the previous admin token (the agent calls this once it has
        // adopted current across the fleet — drop-on-adoption).
        b"CPADMINPREV" => {
            let Ok(st) = shared.state.lock() else {
                return err("state lock");
            };
            Value::Integer(st.admin_prev.is_some() as i64)
        }
        b"CPADMINDROPPREV" => {
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            if st.admin_prev.take().is_some() {
                match st.commit() {
                    Ok(_) => {}
                    Err(e) => return err(&format!("persist: {e}")),
                }
                shared.changed.notify_all();
            }
            ok()
        }
        b"CPPROXIES" => {
            let Ok(st) = shared.state.lock() else {
                return err("state lock");
            };
            Value::Bulk(Some(st.proxies.join(",").into_bytes()))
        }
        // DNS subset publication: render the authoritative zone data for the
        // tenant->proxy-subset mapping. Each tenant resolves to ONLY its
        // shuffle-shard subset — DNS is how clients land on their sub-group
        // without a bootstrap service. Output is standard zone A records;
        // pushing them to a provider (Route53 etc.) is an integration on
        // top of this rendering.
        b"CPDNSZONE" => {
            let Some(suffix) = text(1).filter(|z| clean(z)) else {
                return err("CPDNSZONE <zone-suffix>");
            };
            let Ok(st) = shared.state.lock() else {
                return err("state lock");
            };
            let zone = state::dns_zone(
                &suffix,
                st.tenants.values().map(|t| (t.name.as_str(), &t.subset)),
            );
            Value::Bulk(Some(zone.into_bytes()))
        }
        // Fleet journal (flint-journal): typed state-transition events from
        // every component. Append is a single pre-serialized JSON line.
        b"CPJOURNAL" => {
            let Some(line) = text(1) else {
                return err("CPJOURNAL <event-json>");
            };
            match flint_journal::append_line(&shared.journal_path, &line) {
                Ok(()) => ok(),
                Err(e) => err(&format!("journal append: {e}")),
            }
        }
        b"CPJOURNALREAD" => {
            // CPJOURNALREAD <n> [KINDS <kind,kind,...>]
            //
            // ADR-0018 item 1. See the twin arm in ha.rs for the full
            // reasoning; the short version is that filtering happens BEFORE
            // the tail, so a consumer's horizon is sized by the volume of
            // what it reasons about rather than by total fleet chatter, and
            // an unknown kind is an ERROR because for a budget counter an
            // empty result reads as "no actions taken" — full budget, act
            // freely — which removes the guard instead of degrading it.
            let kinds = match flint_journal::parse_kinds_arg(text(2).as_deref(), text(3).as_deref())
            {
                Ok(k) => k,
                Err(e) => return err(&e),
            };
            let n = text(1).and_then(|v| v.parse().ok()).unwrap_or(50);
            let lines = flint_journal::tail_kinds(&shared.journal_path, n, &kinds);
            Value::Bulk(Some(lines.join("\n").into_bytes()))
        }
        b"CPINFO" => {
            let Ok(st) = shared.state.lock() else {
                return err("state lock");
            };
            let cdr = arg("--internal-cert")
                .as_deref()
                .and_then(flint_tls::cert_days_remaining)
                .map_or_else(|| "none".into(), |d: i64| d.to_string());
            // `version:` was NEVER a software version — it is the registry
            // generation counter that drives CPWATCH, sitting in an
            // operator-visible response right next to cert_days_remaining
            // and reading exactly like a build. ADR-0014 D1 renames it to
            // registry_version: and adds the real one.
            //
            // The old key stays for one release as an ALIAS, because
            // CPWATCH clients parse it: the rename is the point, breaking
            // the watch protocol to achieve it is not. Emitting both is
            // also what lets a mixed fleet roll in either order.
            //
            // controller_build is what the CONTROLLER last registered
            // (CPCONTROLLER). It has no listener and must not gain one, so
            // its build is as fresh as the last thing it said — and a
            // controller that has said nothing since a roll is itself the
            // signal, which is why the timestamp travels with it.
            let controllers = st.controller_line();
            // Lease telemetry from the FAST lock, taken after `st` is
            // locked — same state -> leases order every writer uses, so
            // CPINFO cannot deadlock against CPFENCE/adoption.
            let (lr, lp99) = shared
                .leases
                .lock()
                .map(|lf| (lf.renewals_total, lf.p99_us()))
                .unwrap_or((0, 0));
            Value::Bulk(Some(
                format!(
                    "build:{}\r\nregistry_version:{}\r\nversion:{}\r\nproxies:{}\r\npairs:{}\r\ntenants:{}\r\nslot_exceptions:{}\r\nlease_renewals_total:{lr}\r\nlease_p99_us:{lp99}\r\ncert_days_remaining:{cdr}\r\n{controllers}",
                    build_version(),
                    st.version,
                    st.version,
                    st.proxies.len(),
                    st.pairs.len(),
                    st.tenants.len(),
                    st.exceptions.len()
                )
                .into_bytes(),
            ))
        }
        b"CPLEASE" => {
            // `CPLEASE <addr>` — a serving master renewing its own write
            // lease (ADR-0018). THE FAST PATH: touches Shared::leases only,
            // never `state`, so it cannot queue behind a snapshot or a
            // commit — a renewal delayed past the TTL fences a healthy
            // master, so overload isolation here is a safety property, not
            // a performance nicety. Adoption (a pair with no record yet) is
            // the one slow case and takes the state lock exactly once per
            // pair lifetime.
            let Some(addr) = text(1) else {
                return err("CPLEASE <addr>");
            };
            let t0 = std::time::Instant::now();
            {
                let Ok(mut lf) = shared.leases.lock() else {
                    return err("lease lock");
                };
                if let Some(i) = lease_row_index(&lf.entries, &addr) {
                    let master = lf.entries[i].1.clone();
                    lf.renewals_total += 1;
                    let us = t0.elapsed().as_micros().min(u128::from(u32::MAX)) as u32;
                    lf.record_latency(us);
                    return if master == addr {
                        Value::Simple("OK".into())
                    } else {
                        // A promotion is on record over this node. Refusal is
                        // the FAST fence: the caller flips read-only now
                        // instead of waiting out its TTL.
                        Value::Error(format!("SUPERSEDED {master}"))
                    };
                }
            }
            // No record: first touch. Adopt — durably, because a CP restart
            // that forgot a fencing record would let a healed old master
            // adopt itself back while its successor serves. Membership is
            // the guard: an address the registry does not know is refused.
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(members) = st.pairs.iter().find(|p| p.contains(&addr)).cloned() else {
                return err("NOPAIR address is not a member of any registered pair");
            };
            st.leases.push((members.clone(), addr.clone(), 0));
            if let Err(e) = st.commit() {
                st.leases.pop();
                return err(&format!("persist: {e}"));
            }
            drop(st);
            if let Ok(mut lf) = shared.leases.lock() {
                lf.entries.push((members, addr.clone(), 0));
                lf.renewals_total += 1;
            }
            Value::Simple("OK".into())
        }
        b"CPFENCE" => {
            // `CPFENCE <addr>` — commit `addr` as its pair's master-of-record
            // BEFORE it is promoted (ADR-0018). This is the fencing write the
            // old master's next CPLEASE trips over (-SUPERSEDED), and it is
            // durable for the same reason adoption is. Also bumps the version
            // so watching proxies wake and re-probe — subsuming CPPROMOTED's
            // hint role on the promotion path.
            let Some(addr) = text(1) else {
                return err("CPFENCE <addr>");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(members) = st.pairs.iter().find(|p| p.contains(&addr)).cloned() else {
                return err("NOPAIR address is not a member of any registered pair");
            };
            // CONTAINMENT, matching what CPLEASE reads by. This was member
            // EQUALITY while the renewal path finds "the first entry whose
            // members contain the caller" -- so with two rows for one pair the
            // fence updated one and the renewal read the other, and a freshly
            // promoted master was told it had been superseded by the peer it
            // had just replaced (BUG-0065). Symmetric keys mean the write and
            // the read cannot land on different rows, whatever is in the table.
            let g = match lease_row_index(&st.leases, &addr) {
                Some(i) => {
                    st.leases[i].1 = addr.clone();
                    st.leases[i].2 += 1;
                    st.leases[i].2
                }
                None => {
                    st.leases.push((members.clone(), addr.clone(), 1));
                    1
                }
            };
            if let Err(e) = st.commit() {
                return err(&format!("persist: {e}"));
            }
            let next = st.promoted.as_ref().map_or(1, |(_, n)| n + 1);
            st.promoted = Some((addr.clone(), next));
            st.version += 1;
            drop(st);
            if let Ok(mut lf) = shared.leases.lock() {
                // Same containment key as the durable row above and as the
                // renewal read below it -- all three must agree or the mirror
                // drifts from the record it mirrors.
                match lease_row_index(&lf.entries, &addr) {
                    Some(i) => {
                        lf.entries[i].1 = addr.clone();
                        lf.entries[i].2 = g;
                    }
                    None => lf.entries.push((members, addr.clone(), g)),
                }
            }
            shared.changed.notify_all();
            Value::Simple(format!("OK fenced {addr} gen {g}"))
        }
        b"CPPROMOTED" => {
            // The controller reporting a promotion it just completed. This
            // does NOT set routing: it bumps a generation so the next
            // snapshot wakes every watching proxy and names the pair worth
            // re-probing. Authority stays with the epoch-fenced nodes, so a
            // wrong or replayed hint costs one probe and cannot misroute.
            let Some(addr) = text(1) else {
                return err("CPPROMOTED <addr>");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let next = st.promoted.as_ref().map_or(1, |(_, g)| g + 1);
            st.promoted = Some((addr.clone(), next));
            // Bump the version so watch() stops waiting. No commit(): the
            // hint is deliberately not durable (tenant::promote_hint).
            st.version += 1;
            drop(st);
            shared.changed.notify_all();
            Value::Simple(format!("OK promoted {addr} gen {next}"))
        }
        b"CPCONTROLLER" => {
            // `CPCONTROLLER <host:pid> <build>` — the controller telling us
            // what it is (ADR-0014 D1). It has no listener, so registration
            // is the only way its build can reach `status` without ssh.
            //
            // Does NOT bump st.version and does NOT commit: this is
            // observability, not registry state. Waking every watching
            // proxy because a controller said hello would make a heartbeat
            // into fleet-wide work, and persisting it would let a stamp
            // outlive the process it describes.
            let (Some(id), Some(build)) = (text(1), text(2)) else {
                return err("CPCONTROLLER <host:pid> <build>");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            state::record_controller(&mut st.controllers, &id, &build, now);
            Value::Simple(format!("OK {id} {build}"))
        }
        b"CPSNAPSHOT" => {
            let Some(proxy) = text(1) else {
                return err("CPSNAPSHOT <proxy-addr>");
            };
            let Ok(st) = shared.state.lock() else {
                return err("state lock");
            };
            let (v, pairs, tenants, admin, exc, promo) = st.snapshot_for(&proxy);
            let families = crate::tenant::families_spec(&st.families);
            snapshot_frame(v, &pairs, &tenants, &admin, &exc, &promo, &families)
        }
        _ => err("unknown control-plane command"),
    }
}

#[allow(clippy::too_many_arguments)]
fn snapshot_frame(
    version: u64,
    pairs: &str,
    tenants: &str,
    admin: &str,
    exc: &str,
    promo: &str,
    families: &str,
) -> Value {
    Value::Array(Some(vec![
        Value::Bulk(Some(b"SNAPSHOT".to_vec())),
        Value::Integer(version as i64),
        Value::Bulk(Some(pairs.as_bytes().to_vec())),
        Value::Bulk(Some(tenants.as_bytes().to_vec())),
        // ADR-0006 D4: "curdigest,prevdigest" — a 5th element; pre-D4
        // proxies index elements 2/3 and ignore it.
        Value::Bulk(Some(admin.as_bytes().to_vec())),
        // Option B: slot-ownership exceptions ("ns:slot:pair;..."), the
        // 6th element; older proxies ignore it, older CPs omit it.
        Value::Bulk(Some(exc.as_bytes().to_vec())),
        // 7th element (index 6): the promotion hint ("<addr>|<gen>", empty if
        // none). Same compatibility contract as the 6th: older proxies ignore
        // it, older CPs omit it, and a proxy that never sees one simply keeps
        // its pre-existing reactive rediscovery.
        Value::Bulk(Some(promo.as_bytes().to_vec())),
        // 8th element (index 7): the co-processor family route table (ADR-0010
        // D1), `PREFIX=addr;...`. ALWAYS emitted so the CP is authoritative —
        // an empty string CLEARS the proxy's table (how DEL of the last family
        // lands), distinct from an ABSENT element (older CP -> leave it alone).
        // Older proxies index 0..6 and ignore it, so appending is safe.
        Value::Bulk(Some(families.as_bytes().to_vec())),
    ]))
}

/// CPWATCH <proxy-addr> <last-version>: hijack the connection and push a
/// filtered snapshot whenever the version advances past what the proxy has
/// ACKed. Push -> ACK -> wait-for-change -> push...
fn watch(
    mut stream: flint_tls::Stream,
    shared: &Shared,
    proxy: String,
    mut acked: u64,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    // Delta suppression: the last view actually sent. A version bump whose
    // FILTERED view for this proxy is unchanged (someone else's tenant
    // changed) is acknowledged locally without a wire push — with shuffle
    // sharding most mutations touch a small subset of proxies, so most
    // pushes are no-ops. State is tiny, so suppress-identical beats
    // computing diffs.
    // The promotion hint is part of the view ON PURPOSE. A promotion changes
    // nothing else a proxy can see -- same pairs, same tenants -- so leaving
    // it out would let delta suppression swallow exactly the push this
    // feature exists to deliver, and the failure would look like "the hint
    // never arrived" rather than "the hint was discarded here".
    // families (ADR-0010) is GLOBAL, not per-proxy, but it is a pushed field,
    // so it MUST be in this tuple or a family-only change is silently
    // suppressed — the exact trap the promotion hint carries a warning about.
    let mut last_view: Option<(String, String, String, String, String, String)> = None;
    loop {
        // Wait until there is something newer than the proxy has ACKed.
        let (v, pairs, tenants, admin, exc, promo, families) = {
            let Ok(mut st) = shared.state.lock() else {
                return Ok(());
            };
            while st.version <= acked {
                let Ok((guard, _timeout)) =
                    shared.changed.wait_timeout(st, Duration::from_millis(500))
                else {
                    return Ok(());
                };
                st = guard;
            }
            let (v, pairs, tenants, admin, exc, promo) = st.snapshot_for(&proxy);
            let families = crate::tenant::families_spec(&st.families);
            (v, pairs, tenants, admin, exc, promo, families)
        };
        if last_view.as_ref()
            == Some(&(
                pairs.clone(),
                tenants.clone(),
                admin.clone(),
                exc.clone(),
                promo.clone(),
                families.clone(),
            ))
        {
            eprintln!("watch {proxy}: suppressed push at version {v} (view unchanged)");
            acked = v;
            continue;
        }
        let mut out = Vec::new();
        encode(
            &snapshot_frame(v, &pairs, &tenants, &admin, &exc, &promo, &families),
            &mut out,
        );
        stream.write_all(&out)?;
        last_view = Some((pairs, tenants, admin, exc, promo, families));
        // Read the ACK (Array ["ACK", <version>]).
        loop {
            match decode(&buf) {
                Ok(Decoded::Complete(frame, used)) => {
                    buf.drain(..used);
                    if let Value::Array(Some(items)) = frame
                        && let [Value::Bulk(Some(tag)), Value::Bulk(Some(raw))] = items.as_slice()
                        && tag.eq_ignore_ascii_case(b"ACK")
                        && let Some(n) = std::str::from_utf8(raw).ok().and_then(|s| s.parse().ok())
                    {
                        acked = n;
                    }
                    break;
                }
                Ok(Decoded::NeedMore) => {
                    let n = stream.read(&mut chunk)?;
                    if n == 0 {
                        return Ok(());
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                Err(_) => return Ok(()),
            }
        }
    }
}

fn serve(mut stream: flint_tls::Stream, shared: Arc<Shared>) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut chunk = [0u8; 8 * 1024];
    let mut out: Vec<u8> = Vec::with_capacity(4 * 1024);
    loop {
        let mut consumed = 0;
        out.clear();
        loop {
            match decode(&buf[consumed..]) {
                Ok(Decoded::Complete(frame, used)) => {
                    consumed += used;
                    let Some(args) = frame_to_args(frame) else {
                        encode(&err("protocol: expected array of bulk strings"), &mut out);
                        stream.write_all(&out)?;
                        return Ok(());
                    };
                    // CPWATCH hijacks the connection.
                    if args
                        .first()
                        .is_some_and(|c| c.eq_ignore_ascii_case(b"CPWATCH"))
                    {
                        let proxy = args
                            .get(1)
                            .and_then(|r| std::str::from_utf8(r).ok())
                            .unwrap_or("")
                            .to_string();
                        let last: u64 = args
                            .get(2)
                            .and_then(|r| std::str::from_utf8(r).ok())
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        if proxy.is_empty() {
                            encode(&err("CPWATCH <proxy-addr> [last-version]"), &mut out);
                            stream.write_all(&out)?;
                            return Ok(());
                        }
                        buf.drain(..consumed);
                        stream.write_all(&out)?;
                        return watch(stream, &shared, proxy, last);
                    }
                    let reply = handle(&shared, &args);
                    encode(&reply, &mut out);
                }
                Ok(Decoded::NeedMore) => break,
                Err(_) => {
                    encode(&err("protocol error"), &mut out);
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

/// The --internal-* triple in the server role: the config every
/// control-plane listener (single-node client port, HA client port, Raft
/// RPC port) accepts with — peers and proxies must present a CA-signed
/// cert. None = plaintext.
fn internal_server_config() -> Option<Arc<flint_tls::ReloadableServerConfig>> {
    match (
        arg("--internal-ca"),
        arg("--internal-cert"),
        arg("--internal-key"),
    ) {
        (Some(ca), Some(cert), Some(key)) => Some(
            flint_tls::ReloadableServerConfig::watch(&ca, &cert, &key)
                .expect("build internal TLS server config"),
        ),
        (None, None, None) => None,
        _ => panic!("--internal-ca, --internal-cert, --internal-key must be given together"),
    }
}

/// The same triple in the client role — the Raft RPC dialer. Hot-reloading
/// like the listeners: each RPC dial snapshots the current leaf.
fn internal_client_config() -> Option<Arc<flint_tls::ReloadableClientConfig>> {
    match (
        arg("--internal-ca"),
        arg("--internal-cert"),
        arg("--internal-key"),
    ) {
        (Some(ca), Some(cert), Some(key)) => Some(
            flint_tls::ReloadableClientConfig::watch(&ca, &cert, &key)
                .expect("build internal TLS client config"),
        ),
        _ => None,
    }
}

/// The build stamp surfaced in CPINFO (ADR-0014 D1). One definition for
/// every Flint binary; see the flint-build crate for why it is not written
/// out here.
fn build_version() -> String {
    flint_build::version(env!("CARGO_PKG_VERSION"))
}

/// The ONE key that resolves a pair's lease row, used by the renewal read and
/// by both of the fence's writes.
///
/// It exists as a function because it did not, and the three sites drifted:
/// the renewal found "the first row whose members contain the caller" while
/// the fence updated "the row whose member vector EQUALS this pair". With two
/// rows for one pair — which `CPADDPAIR`'s equality dedupe allowed, since
/// `a,b` and `b,a` were two pairs to it and one to every containment check —
/// the fence wrote one row and the renewal read the other. A freshly promoted
/// master was then told it had been superseded by the peer it had just
/// replaced, and fenced itself read-only mid-roll (BUG-0065).
///
/// Returning an INDEX rather than a reference is deliberate: the fence needs a
/// mutable borrow of the same row it just located, and an index survives the
/// borrow ending.
fn lease_row_index(rows: &[(Vec<String>, String, u64)], addr: &str) -> Option<usize> {
    rows.iter()
        .position(|(m, _, _)| m.iter().any(|x| x == addr))
}

fn main() -> std::io::Result<()> {
    // Before --raft dispatch: asking a binary what it is must not depend on
    // which mode it would have started in.
    if std::env::args().any(|a| a == "--build-version") {
        println!("{}", build_version());
        return Ok(());
    }
    let port: u16 = arg("--port").and_then(|p| p.parse().ok()).unwrap_or(7500);
    let path = arg("--state").unwrap_or_else(|| "./flint-cp-state".into());

    // HA mode: --raft with --node-id / --raft-port / --peers / --client-addrs
    // runs a Raft-replicated node (openraft); otherwise the durable
    // single-node path below.
    if std::env::args().any(|a| a == "--raft") {
        return run_raft(port, path);
    }

    let mut state = State::load_or_new(path.clone().into());
    // Seed the fleet admin token on first boot (ADR-0006 D4). Once in
    // state it rotates via CPADMINROTATE; the flag is ignored thereafter.
    if state.admin_token.is_none()
        && let Some(t) = arg("--admin-token")
    {
        state.admin_token = Some(t);
        let _ = state.commit();
    }
    eprintln!(
        "flint-controlplane: state {path} (version {}, {} proxies, {} pairs, {} tenants)",
        state.version,
        state.proxies.len(),
        state.pairs.len(),
        state.tenants.len()
    );
    let lease_seed = LeaseFast {
        entries: state.leases.clone(),
        ..Default::default()
    };
    let shared = Arc::new(Shared {
        state: Mutex::new(state),
        changed: Condvar::new(),
        journal_path: format!("{path}.journal"),
        usage: Mutex::new(std::collections::HashMap::new()),
        leases: Mutex::new(lease_seed),
    });
    let internal_tls = internal_server_config();
    // See the note on flint-server's --bind: loopback stays the default so
    // every existing single-host fleet is untouched, but a control plane that
    // can only be reached from its own machine cannot serve proxies, a
    // controller or an agent living anywhere else.
    let bind = arg("--bind").unwrap_or_else(|| "127.0.0.1".into());
    let listener = TcpListener::bind((bind.as_str(), port))?;
    eprintln!(
        "flint-controlplane listening on {bind}:{port} ({})",
        if internal_tls.is_some() {
            "internal mTLS"
        } else {
            "plaintext"
        }
    );
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let shared = Arc::clone(&shared);
        // Snapshot per accept: a rotated leaf serves the next connection.
        let internal_tls = internal_tls.as_ref().and_then(|r| r.current());
        std::thread::spawn(move || {
            let conn = match flint_tls::accept(stream, &internal_tls) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("internal tls accept: {e}");
                    return;
                }
            };
            let _ = serve(conn, shared);
        });
    }
    Ok(())
}

/// --raft entry: build a tokio runtime and run the Raft node + client
/// server. Bridges our blocking main into openraft's async world.
fn run_raft(port: u16, state_path: String) -> std::io::Result<()> {
    let node_id: u64 = arg("--node-id")
        .and_then(|v| v.parse().ok())
        .expect("--raft requires --node-id <n>");
    let raft_port: u16 = arg("--raft-port")
        .and_then(|v| v.parse().ok())
        .expect("--raft requires --raft-port <port>");
    let peers = arg("--peers").expect("--raft requires --peers \"1=addr,2=addr,...\"");
    let clients =
        arg("--client-addrs").expect("--raft requires --client-addrs \"1=addr,2=addr,...\"");
    // The address peers DIAL this node at, and the interface both Raft
    // listeners bind. --bind is the same flag the single-node path honours;
    // on a loopback fleet it defaults to 127.0.0.1 and nothing changes.
    let bind_host = arg("--bind").unwrap_or_else(|| "127.0.0.1".into());
    let raft_addr = format!("{bind_host}:{raft_port}");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    // One --internal-* triple covers all three surfaces of an HA node: the
    // Raft RPC listener + dialer (peer↔peer) and the client port (proxies
    // and admin).
    let tls_server = internal_server_config();
    let tls_client = internal_client_config();
    rt.block_on(async move {
        let ha = ha::start(
            node_id,
            &raft_addr,
            &bind_host,
            raft_port,
            state_path.into(),
            &peers,
            &clients,
            tls_server.clone(),
            tls_client,
        )
        .await?;
        let ha = std::sync::Arc::new(ha);
        // Bring the cluster up (idempotent; lowest node id initializes).
        {
            let ha = std::sync::Arc::clone(&ha);
            tokio::spawn(async move {
                ha.maybe_initialize().await;
            });
        }
        ha::run_client(ha, port, bind_host, tls_server).await
    })
}

/// Wire-level tests for the `CPJOURNALREAD` kind filter (ADR-0018 item 1).
///
/// These call `handle` directly rather than dialing a control plane. That is
/// the point: the behaviour being protected is a SAFETY guard's input, and a
/// check that only runs against live infrastructure is a check nobody runs.
/// `handle` is synchronous and `Shared` is cheap, so there is no excuse for
/// the command surface to be untested.
#[cfg(test)]
mod cpjournalread_filter_tests {
    use super::*;
    use flint_journal::{Event, EventKind};

    struct Tmp(std::path::PathBuf);
    impl Tmp {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let p = std::env::temp_dir().join(format!(
                "flint-cpjr-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&p).expect("scratch dir for the journal fixture");
            Self(p)
        }
        fn journal(&self) -> String {
            self.0.join("j.jsonl").to_string_lossy().into_owned()
        }
    }
    impl Drop for Tmp {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn shared(journal_path: String) -> Shared {
        Shared {
            state: Mutex::new(State::default()),
            changed: Condvar::new(),
            journal_path,
            usage: Mutex::new(std::collections::HashMap::new()),
            leases: Mutex::new(LeaseFast::default()),
        }
    }

    fn seed(path: &str) {
        let mut body = String::new();
        let mut push = |at: u64, kind: EventKind| {
            let e = Event {
                at_ms: at,
                actor: "test".into(),
                kind,
                subject: format!("s{at}"),
                epoch: None,
                cause: None,
                detail: None,
            };
            body.push_str(&serde_json::to_string(&e).expect("event serializes"));
            body.push('\n');
        };
        push(1, EventKind::ActionExecuted);
        for i in 0..300u64 {
            push(100 + i, EventKind::Detected);
        }
        push(9_000, EventKind::ActionVerified);
        std::fs::write(path, body).expect("write the journal fixture");
    }

    fn call(sh: &Shared, args: &[&str]) -> Value {
        let a: Vec<Vec<u8>> = args.iter().map(|s| s.as_bytes().to_vec()).collect();
        handle(sh, &a)
    }

    fn body(v: &Value) -> String {
        match v {
            Value::Bulk(Some(b)) => String::from_utf8_lossy(b).into_owned(),
            other => panic!("expected bulk, got {other:?}"),
        }
    }

    /// The filter reaches back PAST a flood that would otherwise fill the
    /// window — the 2026-08-17 shape, at the command surface.
    #[test]
    fn the_filter_sees_actions_a_flood_would_have_buried() {
        let t = Tmp::new();
        let p = t.journal();
        seed(&p);
        let sh = shared(p);

        let plain = body(&call(&sh, &["CPJOURNALREAD", "50"]));
        assert!(
            !plain.contains("ActionExecuted"),
            "precondition: the flood must bury it unfiltered, else this proves nothing"
        );

        let filtered = body(&call(
            &sh,
            &[
                "CPJOURNALREAD",
                "50",
                "KINDS",
                "ActionExecuted,ActionVerified",
            ],
        ));
        assert!(
            filtered.contains("ActionExecuted"),
            "the buried action surfaces"
        );
        assert!(filtered.contains("ActionVerified"));
        assert!(!filtered.contains("Detected"), "and the flood is excluded");
        assert_eq!(filtered.lines().count(), 2);
    }

    /// THE ONE THAT PROTECTS THE BUDGET. An unknown kind must be an error at
    /// the wire, because an empty bulk reads to tier2 as "no actions taken"
    /// — full budget — and nothing downstream can tell the two apart.
    #[test]
    fn an_unknown_kind_errors_rather_than_returning_no_rows() {
        let t = Tmp::new();
        let p = t.journal();
        seed(&p);
        let sh = shared(p);
        match call(&sh, &["CPJOURNALREAD", "50", "KINDS", "ActionExecutd"]) {
            Value::Error(e) => assert!(e.contains("ActionExecutd"), "must name it: {e}"),
            other => panic!("a typo returned rows instead of refusing: {other:?}"),
        }
        // ...and the same for a malformed narrow request, which must never
        // widen into an unfiltered read.
        assert!(matches!(
            call(&sh, &["CPJOURNALREAD", "50", "KINDS"]),
            Value::Error(_)
        ));
        assert!(matches!(
            call(&sh, &["CPJOURNALREAD", "50", "SINCE", "123"]),
            Value::Error(_)
        ));
    }

    /// Callers that predate the filter keep byte-identical behaviour.
    #[test]
    fn the_unfiltered_form_is_unchanged() {
        let t = Tmp::new();
        let p = t.journal();
        seed(&p);
        let sh = shared(p.clone());
        let got = body(&call(&sh, &["CPJOURNALREAD", "50"]));
        assert_eq!(got, flint_journal::tail(&p, 50).join("\n"));
        // And the documented default when n is absent.
        let dflt = body(&call(&sh, &["CPJOURNALREAD"]));
        assert_eq!(dflt.lines().count(), 50);
    }
}

#[cfg(test)]
mod lease_row_key_tests {
    use super::lease_row_index;

    fn row(members: &[&str], master: &str, generation: u64) -> (Vec<String>, String, u64) {
        (
            members.iter().map(|s| (*s).to_string()).collect(),
            master.to_string(),
            generation,
        )
    }

    /// Membership, not vector identity — the renewal never knows which order
    /// the pair happened to be registered in.
    #[test]
    fn the_key_is_membership_not_member_order() {
        let rows = vec![row(&["b:2", "a:1"], "b:2", 3)];
        assert_eq!(lease_row_index(&rows, "a:1"), Some(0));
        assert_eq!(lease_row_index(&rows, "b:2"), Some(0));
        assert_eq!(lease_row_index(&rows, "c:3"), None);
    }

    /// BUG-0065. Two rows for ONE pair, which CPADDPAIR's equality dedupe used
    /// to allow: the fence wrote the row matching the member vector and the
    /// renewal read the FIRST row containing the caller. When those differed,
    /// a freshly promoted master was told its own demoted peer had superseded
    /// it. One key means both sides land on the same row by construction --
    /// the duplicate becomes stale rather than contradictory.
    #[test]
    fn a_duplicate_row_cannot_split_the_fence_from_the_renewal() {
        // Row 0 is the stale ordering still naming the OLD master; row 1 is
        // the one an equality-keyed fence would have picked.
        let mut rows = vec![
            row(&["a:1", "b:2"], "b:2", 3),
            row(&["b:2", "a:1"], "b:2", 3),
        ];

        // The fence promotes a:1 and resolves its row through the one key.
        let fenced = lease_row_index(&rows, "a:1").expect("fence finds a row");
        rows[fenced].1 = "a:1".to_string();
        rows[fenced].2 += 1;

        // The renewal from a:1 resolves through the SAME key, so it must see
        // the write that just happened -- not the other row.
        let renewed = lease_row_index(&rows, "a:1").expect("renewal finds a row");
        assert_eq!(renewed, fenced, "fence and renewal split across rows");
        assert_eq!(
            rows[renewed].1, "a:1",
            "the renewing master reads itself as master; anything else is a false SUPERSEDED"
        );
    }

    /// The demoted peer must still be told it lost, or a fence stops fencing.
    /// This is the assertion that would fail if the key were widened into
    /// "always answer OK".
    #[test]
    fn the_demoted_peer_still_reads_as_superseded() {
        let rows = vec![row(&["a:1", "b:2"], "a:1", 4)];
        let i = lease_row_index(&rows, "b:2").expect("row");
        assert_ne!(
            rows[i].1, "b:2",
            "b:2 was demoted and must not read itself as master"
        );
        assert_eq!(rows[i].1, "a:1");
    }

    /// The test above uses one function on both sides, so it cannot fail while
    /// that stays true -- which is exactly the point, and exactly why it proves
    /// nothing on its own. What actually holds BUG-0065 shut is STRUCTURAL:
    /// every site that resolves a lease row goes through `lease_row_index`. An
    /// inline member-vector comparison anywhere is the defect returning, so
    /// assert against the source rather than against behaviour a unit test
    /// cannot reach from here.
    #[test]
    fn no_site_resolves_a_lease_row_by_member_vector_equality() {
        // Only the PRODUCTION half: this test names the forbidden patterns as
        // string literals, so scanning the whole file matches itself and fails
        // for its own text. Found exactly that way on the first run.
        let whole = include_str!("main.rs");
        let src = whole.split("#[cfg(test)]").next().unwrap_or(whole);
        for pat in [
            "st.leases.iter_mut().find(",
            "lf.entries.iter_mut().find(",
            "lf.entries.iter().find(",
            "m == &members",
        ] {
            assert!(
                !src.contains(pat),
                "a lease row is being resolved by `{pat}` instead of lease_row_index(); \
                 that asymmetry between the fence's write key and the renewal's read key \
                 IS BUG-0065"
            );
        }
        // And the one key is actually used by all three sites.
        assert!(
            src.matches("lease_row_index(").count() >= 4,
            "expected the renewal read, both fence writes and the definition to \
             reference lease_row_index; found {}",
            src.matches("lease_row_index(").count()
        );
    }

    /// The root fix: sorting at registration makes `a,b` and `b,a` one vector,
    /// so CPADDPAIR's `contains` dedupe rejects the second and no duplicate is
    /// ever created. Without the sort these compare unequal and both are kept.
    #[test]
    fn canonicalising_registration_collapses_reordered_pairs() {
        let mut ab: Vec<String> = "a:1,b:2".split(',').map(String::from).collect();
        let mut ba: Vec<String> = "b:2,a:1".split(',').map(String::from).collect();
        assert_ne!(ab, ba, "unsorted, these are two different pairs");
        ab.sort();
        ba.sort();
        assert_eq!(ab, ba, "sorted, CPADDPAIR's contains-dedupe sees one pair");
    }
}
