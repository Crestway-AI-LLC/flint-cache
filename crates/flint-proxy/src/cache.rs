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

pub struct ProxyCache {
    inner: Mutex<Inner>,
    /// 0 = disabled. Runtime-settable (PROXYCACHE).
    ttl_ms: AtomicU64,
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
    pub fn new(ttl_ms: u64, max_bytes: u64) -> Self {
        ProxyCache {
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
        let ttl = self.ttl_ms.load(Ordering::Relaxed);
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
}
