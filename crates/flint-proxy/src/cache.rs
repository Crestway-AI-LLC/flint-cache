// SPDX-License-Identifier: Elastic-2.0
//! Proxy-local read cache (ADR-0005 D6, revised). We cannot control users'
//! clients, so the near-cache lives at the closest point we DO control: this
//! proxy. GET replies for OPTED-IN tenants are kept for a short TTL under a
//! bounded byte budget; a hit answers locally without touching a backend.
//!
//! The consistency contract (why opt-in is mandatory, same principle as D4):
//! stale reads are ALLOWED, bounded by the TTL. A write through THIS proxy
//! invalidates its local entry (read-your-own-writes through one proxy); a
//! write through ANOTHER proxy — or straight to a node — becomes visible here
//! only when the TTL lapses. The TTL is the contract; same-proxy
//! invalidation is a freshness optimization on top.
//!
//! Both knobs are runtime-settable via PROXYCACHE (operator surface):
//! `ttl_ms` (0 = disabled, clears the cache) and `max_bytes`. Eviction is
//! FIFO with generation checks — with one short TTL, insertion order IS
//! expiry order, so FIFO evicts the entries closest to death anyway and
//! stays O(1) without LRU bookkeeping.
//!
//! Fairness: the byte budget is shared, so an unchecked key-spraying tenant
//! would evict every other tenant's entries (a noisy neighbor INSIDE the
//! mitigation). At insert time the writing tenant is capped at
//! `budget / resident-tenant-count`; over its share, its OWN oldest entries
//! evict first. Other tenants' entries are touched only by the global
//! budget (oldest-first, which is the sprayer's own entries anyway).

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

struct Entry {
    val: Vec<u8>,
    expires_at: Instant,
    /// Insert generation: re-inserting a key strands the old FIFO slot;
    /// eviction skips slots whose generation no longer matches.
    generation: u64,
    /// Accounted size (key + value) so the byte budget tracks reality.
    cost: usize,
}

#[derive(Default)]
struct Inner {
    map: HashMap<Vec<u8>, Entry>,
    fifo: VecDeque<(Vec<u8>, u64)>,
    bytes: usize,
    /// Resident bytes per namespace — the fairness accounting.
    ns_bytes: HashMap<Vec<u8>, usize>,
    generation: u64,
}

impl Inner {
    /// The composite key's namespace (length-prefixed by `composite`).
    fn ns_of(key: &[u8]) -> &[u8] {
        let n = u32::from_be_bytes([key[0], key[1], key[2], key[3]]) as usize;
        &key[4..4 + n]
    }

    /// Remove `key` from the map, keeping BOTH byte ledgers consistent.
    /// The fifo slot (if any) goes stale and is skipped by eviction.
    fn remove(&mut self, key: &[u8]) -> bool {
        let Some(e) = self.map.remove(key) else {
            return false;
        };
        self.bytes -= e.cost;
        let ns = Self::ns_of(key).to_vec();
        if let Some(b) = self.ns_bytes.get_mut(&ns) {
            *b -= e.cost;
            if *b == 0 {
                self.ns_bytes.remove(&ns);
            }
        }
        true
    }
}

/// The most a tenant may set its own near-cache TTL to, unless the operator
/// says otherwise (`--cache-ttl-max-ms`).
///
/// 60 s rather than unbounded: the TTL is the tenant's accepted staleness AND
/// its residency in a shared byte budget, so an unbounded value is a way to
/// occupy the cache at everyone else's expense. A minute is far past any
/// repeat-read window a cache is for, so the ceiling binds abuse rather than
/// use.
pub const DEFAULT_TTL_MAX_MS: u64 = 60_000;

pub struct ProxyCache {
    inner: Mutex<Inner>,
    /// 0 = disabled. Runtime-settable (PROXYCACHE).
    ///
    /// The FLEET default and the operator's kill switch: 0 disables the cache
    /// for everyone regardless of any per-namespace value, because an
    /// operator has to be able to turn a shared component off without
    /// negotiating with every tenant on it.
    ttl_ms: AtomicU64,
    /// Per-namespace TTL overrides, set by the tenant itself
    /// (`PROXYCACHE <ttl_ms>` on an authed connection).
    ///
    /// The TTL is the staleness the tenant accepts, so it is theirs to
    /// choose: one workload repeats a key every few seconds and another every
    /// few minutes, and an operator picking one number for both is picking it
    /// wrong for at least one of them.
    ///
    /// BOUNDED BY `ttl_max_ms`, because it is a shared byte budget: a longer
    /// TTL means a tenant's entries sit resident longer, so an unbounded value
    /// is a way to occupy the cache. The ceiling is the operator's.
    ns_ttl_ms: Mutex<std::collections::HashMap<Vec<u8>, u64>>,
    /// The most any tenant may set for itself. Operator-owned
    /// (`--cache-ttl-max-ms`); the tenant's own setting is clamped to it
    /// rather than refused, so a tenant asking for more gets the most it may
    /// have and is told what that is.
    ttl_max_ms: AtomicU64,
    /// Byte budget for keys+values. Runtime-settable (PROXYCACHE).
    max_bytes: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
}

/// Composite cache key: length-prefixed namespace + key, so tenant
/// namespaces can never collide byte-wise ("ab"+"c" vs "a"+"bc").
fn composite(ns: &[u8], key: &[u8]) -> Vec<u8> {
    let mut c = Vec::with_capacity(4 + ns.len() + key.len());
    c.extend_from_slice(&(ns.len() as u32).to_be_bytes());
    c.extend_from_slice(ns);
    c.extend_from_slice(key);
    c
}

impl ProxyCache {
    /// The default ceiling. Test-only: the live proxy calls `with_ceiling` so
    /// `--cache-ttl-max-ms` reaches it, and a second constructor that silently
    /// ignored the operator's ceiling is exactly the kind of thing that gets
    /// called by accident.
    #[cfg(test)]
    pub fn new(ttl_ms: u64, max_bytes: u64) -> Self {
        Self::with_ceiling(ttl_ms, max_bytes, DEFAULT_TTL_MAX_MS)
    }

    pub fn with_ceiling(ttl_ms: u64, max_bytes: u64, ttl_max_ms: u64) -> Self {
        ProxyCache {
            ns_ttl_ms: Mutex::new(std::collections::HashMap::new()),
            ttl_max_ms: AtomicU64::new(ttl_max_ms),
            inner: Mutex::new(Inner::default()),
            ttl_ms: AtomicU64::new(ttl_ms),
            max_bytes: AtomicU64::new(max_bytes),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn enabled(&self) -> bool {
        self.ttl_ms.load(Ordering::Relaxed) > 0
    }

    /// The TTL that applies to `ns`: its own if it set one, else the fleet
    /// default.
    ///
    /// A disabled cache stays disabled for everyone — the operator's 0 wins
    /// over any per-namespace value, which is what makes it a kill switch
    /// rather than a suggestion.
    pub fn ttl_for(&self, ns: &[u8]) -> u64 {
        let global = self.ttl_ms.load(Ordering::Relaxed);
        if global == 0 {
            return 0;
        }
        match self.ns_ttl_ms.lock() {
            Ok(m) => m.get(ns).copied().unwrap_or(global),
            // A poisoned lock must not silently hand back the fleet default:
            // that would quietly widen or narrow a tenant's accepted
            // staleness. The global is the honest fallback and is what they
            // had before they set anything.
            Err(_) => global,
        }
    }

    pub fn ttl_max_ms(&self) -> u64 {
        self.ttl_max_ms.load(Ordering::Relaxed)
    }

    /// A tenant sets its own TTL. Returns what was actually applied, which is
    /// the request clamped to the operator's ceiling — the caller reports it,
    /// so a tenant that asked for more learns the number rather than getting
    /// a silent surprise. 0 turns the cache off for this namespace only.
    pub fn set_ns_ttl(&self, ns: &[u8], want_ms: u64) -> u64 {
        let applied = want_ms.min(self.ttl_max_ms.load(Ordering::Relaxed));
        if let Ok(mut m) = self.ns_ttl_ms.lock() {
            m.insert(ns.to_vec(), applied);
        }
        // Entries cached under the OLD ttl keep their own expiry, which is
        // correct in both directions: shortening cannot retroactively unstale
        // a reply already served, and lengthening must not extend an entry the
        // tenant cached under a tighter promise. The new value governs what is
        // cached from here.
        applied
    }

    /// Runtime reconfiguration (PROXYCACHE <ttl_ms> <max_bytes>).
    /// ttl 0 disables AND clears — a disabled cache must not resurrect
    /// stale entries when re-enabled later.
    pub fn configure(&self, ttl_ms: u64, max_bytes: u64) {
        self.ttl_ms.store(ttl_ms, Ordering::Relaxed);
        self.max_bytes.store(max_bytes, Ordering::Relaxed);
        if let Ok(mut inner) = self.inner.lock() {
            if ttl_ms == 0 {
                *inner = Inner::default();
            } else {
                Self::evict_to_budget(&mut inner, max_bytes as usize);
            }
        }
    }

    pub fn stats(&self) -> (u64, u64, u64, u64, usize, usize) {
        let (entries, bytes) = self
            .inner
            .lock()
            .map(|i| (i.map.len(), i.bytes))
            .unwrap_or((0, 0));
        (
            self.ttl_ms.load(Ordering::Relaxed),
            self.max_bytes.load(Ordering::Relaxed),
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            entries,
            bytes,
        )
    }

    /// A cached value for (ns, key), if present and fresh. Counts hit/miss.
    pub fn get(&self, ns: &[u8], key: &[u8]) -> Option<Vec<u8>> {
        if !self.enabled() {
            return None;
        }
        let c = composite(ns, key);
        let now = Instant::now();
        let mut inner = self.inner.lock().ok()?;
        if let Some(e) = inner.map.get(&c) {
            if e.expires_at > now {
                let val = e.val.clone();
                drop(inner);
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Some(val);
            }
            // Expired: reclaim now rather than waiting for FIFO churn.
            inner.remove(&c);
        }
        drop(inner);
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Cache a GET's bulk reply. Values larger than the whole budget are
    /// skipped (they would evict everything and still not fit).
    pub fn put(&self, ns: &[u8], key: &[u8], val: &[u8]) {
        let ttl = self.ttl_for(ns);
        if ttl == 0 {
            return;
        }
        let budget = self.max_bytes.load(Ordering::Relaxed) as usize;
        let c = composite(ns, key);
        let cost = c.len() + val.len();
        if cost > budget {
            return;
        }
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        inner.generation += 1;
        let generation = inner.generation;
        if let Some(old) = inner.map.insert(
            c.clone(),
            Entry {
                val: val.to_vec(),
                expires_at: Instant::now() + std::time::Duration::from_millis(ttl),
                generation,
                cost,
            },
        ) {
            inner.bytes -= old.cost;
            let old_cost = old.cost;
            if let Some(b) = inner.ns_bytes.get_mut(ns) {
                *b -= old_cost;
            }
        }
        inner.bytes += cost;
        *inner.ns_bytes.entry(ns.to_vec()).or_insert(0) += cost;
        inner.fifo.push_back((c.clone(), generation));
        Self::evict_to_budget(&mut inner, budget);
        Self::enforce_ns_share(&mut inner, ns, budget, &c);
    }

    /// Drop (ns, key) — a write to it went through this proxy.
    pub fn invalidate(&self, ns: &[u8], key: &[u8]) {
        if !self.enabled() {
            return;
        }
        if let Ok(mut inner) = self.inner.lock() {
            inner.remove(&composite(ns, key));
        }
    }

    /// Drop every entry in `ns` (FLUSHALL through this proxy).
    pub fn invalidate_ns(&self, ns: &[u8]) {
        if !self.enabled() {
            return;
        }
        let mut prefix = Vec::with_capacity(4 + ns.len());
        prefix.extend_from_slice(&(ns.len() as u32).to_be_bytes());
        prefix.extend_from_slice(ns);
        if let Ok(mut inner) = self.inner.lock() {
            let doomed: Vec<Vec<u8>> = inner
                .map
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();
            for k in doomed {
                inner.remove(&k);
            }
        }
    }

    fn evict_to_budget(inner: &mut Inner, budget: usize) {
        while inner.bytes > budget {
            let Some((key, generation)) = inner.fifo.pop_front() else {
                break;
            };
            // A stale slot (key re-inserted or already invalidated since):
            // skip; its live incarnation has a later FIFO slot.
            let live = inner
                .map
                .get(&key)
                .is_some_and(|e| e.generation == generation);
            if live {
                inner.remove(&key);
            }
        }
    }

    /// Fairness: cap `ns` at budget / resident-tenant-count by evicting its
    /// OWN oldest entries (never another tenant's), sparing the entry just
    /// inserted (`newest`) so a below-share tenant always keeps its latest.
    fn enforce_ns_share(inner: &mut Inner, ns: &[u8], budget: usize, newest: &[u8]) {
        let residents = inner.ns_bytes.len().max(1);
        let share = budget / residents;
        if inner.ns_bytes.get(ns).copied().unwrap_or(0) <= share {
            return;
        }
        // Walk oldest-first; evict only this namespace's LIVE entries. The
        // fifo slots stay (they turn stale and are skipped later).
        let doomed: Vec<Vec<u8>> = inner
            .fifo
            .iter()
            .filter(|(k, generation)| {
                k.as_slice() != newest
                    && Inner::ns_of(k) == ns
                    && inner
                        .map
                        .get(k)
                        .is_some_and(|e| e.generation == *generation)
            })
            .map(|(k, _)| k.clone())
            .collect();
        for k in doomed {
            if inner.ns_bytes.get(ns).copied().unwrap_or(0) <= share {
                break;
            }
            inner.remove(&k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hit_miss_ttl_and_budget() {
        let c = ProxyCache::new(50, 200);
        // Miss, then populate, then hit.
        assert_eq!(c.get(b"acme", b"k"), None);
        c.put(b"acme", b"k", b"v1");
        assert_eq!(c.get(b"acme", b"k").as_deref(), Some(b"v1".as_slice()));
        // Namespace isolation: same key name, other tenant — a miss.
        assert_eq!(c.get(b"globex", b"k"), None);
        // Invalidation (a write through this proxy).
        c.invalidate(b"acme", b"k");
        assert_eq!(c.get(b"acme", b"k"), None);
        // TTL expiry.
        c.put(b"acme", b"t", b"v");
        std::thread::sleep(std::time::Duration::from_millis(60));
        assert_eq!(c.get(b"acme", b"t"), None);
        // Byte budget: inserts evict oldest-first, bytes never exceed it.
        for i in 0..50u32 {
            c.put(b"acme", format!("key:{i}").as_bytes(), &[b'x'; 20]);
            let (_, _, _, _, _, bytes) = c.stats();
            assert!(bytes <= 200, "budget exceeded: {bytes}");
        }
        // ttl=0 disables and clears.
        c.configure(0, 200);
        let (_, _, _, _, entries, bytes) = c.stats();
        assert_eq!((entries, bytes), (0, 0));
        c.put(b"acme", b"k", b"v");
        assert_eq!(c.get(b"acme", b"k"), None);
    }

    #[test]
    fn spraying_tenant_cannot_evict_others() {
        // Budget 1000, two tenants resident -> each capped at ~500 at
        // insert time. globex parks a few entries; acme sprays far past
        // the whole budget; globex's entries must survive.
        let c = ProxyCache::new(60_000, 1000);
        for i in 0..5u32 {
            c.put(b"globex", format!("g:{i}").as_bytes(), &[b'y'; 20]);
        }
        for i in 0..200u32 {
            c.put(b"acme", format!("a:{i}").as_bytes(), &[b'x'; 30]);
        }
        for i in 0..5u32 {
            assert!(
                c.get(b"globex", format!("g:{i}").as_bytes()).is_some(),
                "acme's spray evicted globex's entry g:{i}"
            );
        }
        let (_, _, _, _, _, bytes) = c.stats();
        assert!(bytes <= 1000, "global budget exceeded: {bytes}");
        // acme is bounded by its share, not the whole budget: its newest
        // entries live, its oldest were self-evicted.
        assert!(c.get(b"acme", b"a:199").is_some());
        assert!(c.get(b"acme", b"a:0").is_none());
    }

    /// A tenant's own TTL governs its namespace and nobody else's.
    #[test]
    fn a_tenants_ttl_applies_only_to_its_namespace() {
        let c = ProxyCache::new(5_000, 1 << 20);
        assert_eq!(
            c.ttl_for(b"acme"),
            5_000,
            "unset namespaces take the default"
        );
        assert_eq!(c.set_ns_ttl(b"acme", 30_000), 30_000);
        assert_eq!(c.ttl_for(b"acme"), 30_000);
        assert_eq!(c.ttl_for(b"globex"), 5_000, "a neighbour is untouched");
    }

    /// The ceiling CLAMPS rather than refuses, and the applied value is what
    /// comes back -- so the caller can tell the tenant the number it actually
    /// has instead of the one it asked for.
    #[test]
    fn a_tenant_cannot_exceed_the_operators_ceiling() {
        let c = ProxyCache::with_ceiling(5_000, 1 << 20, 10_000);
        assert_eq!(c.set_ns_ttl(b"acme", 999_999), 10_000);
        assert_eq!(c.ttl_for(b"acme"), 10_000);
    }

    /// THE OPERATOR'S KILL SWITCH OUTRANKS EVERY TENANT. A shared component
    /// has to be turn-off-able without negotiating with everyone on it.
    #[test]
    fn a_global_zero_disables_the_cache_for_a_tenant_that_set_its_own() {
        let c = ProxyCache::new(5_000, 1 << 20);
        c.set_ns_ttl(b"acme", 30_000);
        assert_eq!(c.ttl_for(b"acme"), 30_000);
        c.configure(0, 1 << 20);
        assert_eq!(c.ttl_for(b"acme"), 0, "the operator's 0 wins");
        c.put(b"acme", b"k", b"v");
        assert!(
            c.get(b"acme", b"k").is_none(),
            "and nothing is cached under it"
        );
    }

    /// A tenant may turn its own caching off without affecting the fleet.
    #[test]
    fn a_tenant_zero_is_its_own_and_not_the_fleets() {
        let c = ProxyCache::new(5_000, 1 << 20);
        assert_eq!(c.set_ns_ttl(b"acme", 0), 0);
        c.put(b"acme", b"k", b"v");
        assert!(c.get(b"acme", b"k").is_none(), "acme caches nothing");
        c.put(b"globex", b"k", b"v");
        assert!(c.get(b"globex", b"k").is_some(), "globex still does");
    }
}
