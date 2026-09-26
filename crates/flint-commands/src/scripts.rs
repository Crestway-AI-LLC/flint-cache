// SPDX-License-Identifier: Elastic-2.0
//! The texts of Lua scripts a tenant has run or loaded, by SHA1, for
//! `EVALSHA` and `SCRIPT EXISTS` (ADR-0051). One type for both planes: the
//! proxy keeps the texts so an `EVALSHA` can be forwarded as an `EVAL` to
//! whichever pair owns the script's keys, and a seat keeps them for clients
//! that talk to it directly.
//!
//! Bounded three ways: scripts and bytes per namespace, and bytes in all.
//! Past a bound the oldest go, and an `EVALSHA` of one answers `NOSCRIPT`,
//! which every client answers by sending the text again. Per namespace, so
//! one tenant cannot learn what another has loaded.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// Scripts one namespace may hold.
pub const SCRIPTS_PER_NS: usize = 1000;
/// Bytes of script text one namespace may hold.
pub const BYTES_PER_NS: usize = 8 << 20;
/// Bytes of script text held for every namespace together.
pub const BYTES_TOTAL: usize = 128 << 20;

#[derive(Default)]
struct Ns {
    texts: HashMap<String, Vec<u8>>,
    order: VecDeque<String>,
    bytes: usize,
}

impl Ns {
    fn evict_oldest(&mut self) -> usize {
        while let Some(old) = self.order.pop_front() {
            if let Some(t) = self.texts.remove(&old) {
                self.bytes -= t.len();
                return t.len();
            }
        }
        0
    }
}

#[derive(Default)]
struct Inner {
    by_ns: HashMap<Vec<u8>, Ns>,
    bytes: usize,
}

/// A bounded, per-namespace map from SHA1 (lowercase hex) to script text.
pub struct ScriptCache {
    inner: Mutex<Option<Inner>>,
}

impl ScriptCache {
    pub const fn new() -> Self {
        Self {
            inner: Mutex::new(None),
        }
    }

    /// Remember `text` under `sha` for `ns`. The caller computes the SHA1.
    pub fn remember(&self, ns: &[u8], sha: &str, text: &[u8]) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let inner = guard.get_or_insert_with(Inner::default);
        let entry = inner.by_ns.entry(ns.to_vec()).or_default();
        if entry.texts.contains_key(sha) {
            return;
        }
        entry.texts.insert(sha.to_string(), text.to_vec());
        entry.order.push_back(sha.to_string());
        entry.bytes += text.len();
        inner.bytes += text.len();
        // The namespace pays for its own excess first.
        while entry.texts.len() > SCRIPTS_PER_NS || entry.bytes > BYTES_PER_NS {
            let freed = entry.evict_oldest();
            if freed == 0 && entry.texts.len() <= SCRIPTS_PER_NS {
                break;
            }
            inner.bytes -= freed;
        }
        // Then, over the total, whoever holds the most.
        while inner.bytes > BYTES_TOTAL {
            let Some(largest) = inner
                .by_ns
                .iter()
                .max_by_key(|(_, n)| n.bytes)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            let freed = inner.by_ns.get_mut(&largest).map_or(0, Ns::evict_oldest);
            if freed == 0 {
                break;
            }
            inner.bytes -= freed;
        }
    }

    /// The text `ns` has under `sha`.
    pub fn lookup(&self, ns: &[u8], sha: &str) -> Option<Vec<u8>> {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        guard.as_ref()?.by_ns.get(ns)?.texts.get(sha).cloned()
    }

    /// `SCRIPT FLUSH`, for one namespace.
    pub fn flush(&self, ns: &[u8]) {
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(inner) = guard.as_mut()
            && let Some(n) = inner.by_ns.remove(ns)
        {
            inner.bytes -= n.bytes;
        }
    }
}

impl Default for ScriptCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remembers_per_namespace_and_flushes_one() {
        let c = ScriptCache::new();
        c.remember(b"a", "s1", b"return 1");
        c.remember(b"b", "s1", b"return 2");
        assert_eq!(c.lookup(b"a", "s1").as_deref(), Some(&b"return 1"[..]));
        assert_eq!(c.lookup(b"b", "s1").as_deref(), Some(&b"return 2"[..]));
        c.flush(b"a");
        assert_eq!(c.lookup(b"a", "s1"), None);
        assert_eq!(c.lookup(b"b", "s1").as_deref(), Some(&b"return 2"[..]));
    }

    #[test]
    fn a_namespace_past_its_count_loses_its_oldest() {
        let c = ScriptCache::new();
        for i in 0..=SCRIPTS_PER_NS {
            c.remember(b"a", &i.to_string(), b"x");
        }
        assert_eq!(c.lookup(b"a", "0"), None, "the oldest went");
        assert!(c.lookup(b"a", "1").is_some());
        assert!(c.lookup(b"a", &SCRIPTS_PER_NS.to_string()).is_some());
    }

    #[test]
    fn the_total_is_bounded_across_namespaces() {
        let c = ScriptCache::new();
        let big = vec![b'x'; BYTES_PER_NS / 2];
        let namespaces = BYTES_TOTAL / big.len() + 4;
        for n in 0..namespaces {
            c.remember(n.to_string().as_bytes(), "s", &big);
        }
        let guard = c.inner.lock().expect("lock");
        assert!(guard.as_ref().expect("populated").bytes <= BYTES_TOTAL);
    }
}
