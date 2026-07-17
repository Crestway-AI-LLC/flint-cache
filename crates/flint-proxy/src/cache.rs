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
    generation: u64,
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
            if let Some(e) = inner.map.remove(&c) {
                inner.bytes -= e.cost;
            }
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
        }
        inner.bytes += cost;
        inner.fifo.push_back((c, generation));
        Self::evict_to_budget(&mut inner, budget);
    }

    /// Drop (ns, key) — a write to it went through this proxy.
    pub fn invalidate(&self, ns: &[u8], key: &[u8]) {
        if !self.enabled() {
            return;
        }
        if let Ok(mut inner) = self.inner.lock()
            && let Some(e) = inner.map.remove(&composite(ns, key))
        {
            inner.bytes -= e.cost;
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
                if let Some(e) = inner.map.remove(&k) {
                    inner.bytes -= e.cost;
                }
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
            if live && let Some(e) = inner.map.remove(&key) {
                inner.bytes -= e.cost;
            }
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
}
