// SPDX-License-Identifier: Elastic-2.0
//! Each namespace's FT index prefixes on this node (ADR-0060, amendment 2).
//! A write under one of them is refused unless it comes over a co-processor's
//! channel: the co-processor writes those keys for the tenant and indexes
//! what it wrote, so a write that went around it would be a document the
//! index misses with nothing said.
//!
//! The prefixes are a row of the node's own, `\x00flintidx\x00<ns>`, written
//! only by `FLINTINDEXED` from a channel. It is not under `\x00flint\x00`,
//! whose rows are a node's identity and are dropped from the replication
//! stream: this one is data, and a failover's new master must have it. A slot
//! move carries it, and the co-processor writes it again on every rebuild.
//!
//! The rows are read into memory at start and again on promotion, which is
//! when a replica that has been receiving them starts to take writes.

use flint_storage::Kv;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

/// The rows' key prefix.
pub const ROW_PREFIX: &[u8] = b"\x00flintidx\x00";

/// A namespace's prefixes, shared with each write that checks them.
pub type Prefixes = Arc<[Vec<u8>]>;

/// What this node holds of every namespace's prefixes.
#[derive(Default)]
pub struct Indexed {
    by_ns: RwLock<HashMap<Vec<u8>, Prefixes>>,
    /// Whether any namespace has one, so a node without an FT index pays an
    /// atomic load per write and no lookup.
    any: AtomicBool,
}

/// The row that holds `ns`'s prefixes.
pub fn row_key(ns: &[u8]) -> Vec<u8> {
    let mut k = ROW_PREFIX.to_vec();
    k.extend_from_slice(ns);
    k
}

/// Each prefix as its length (4 bytes, big-endian), then its bytes.
fn encode(prefixes: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for p in prefixes {
        out.extend_from_slice(&(p.len() as u32).to_be_bytes());
        out.extend_from_slice(p);
    }
    out
}

fn decode(mut raw: &[u8]) -> Option<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    while !raw.is_empty() {
        let len = u32::from_be_bytes(raw.get(..4)?.try_into().ok()?) as usize;
        out.push(raw.get(4..4 + len)?.to_vec());
        raw = &raw[4 + len..];
    }
    Some(out)
}

impl Indexed {
    /// Read every namespace's row from `store`, replacing what was held: at
    /// start, and on promotion. A row this binary cannot read guards
    /// everything its namespace writes, since the guard must not open.
    pub fn load(&self, store: &dyn Kv) {
        let mut by_ns = HashMap::new();
        store.for_each_prefix(ROW_PREFIX, &mut |k, v| {
            let ns = k[ROW_PREFIX.len()..].to_vec();
            let prefixes = decode(v).unwrap_or_else(|| {
                eprintln!(
                    "flint-server: indexed-prefix row of ns {:?} is unreadable; guarding every write of it",
                    String::from_utf8_lossy(&ns)
                );
                vec![Vec::new()]
            });
            if !prefixes.is_empty() {
                by_ns.insert(ns, Prefixes::from(prefixes));
            }
            true
        });
        self.any.store(!by_ns.is_empty(), Ordering::Release);
        *self.by_ns.write().unwrap_or_else(|e| e.into_inner()) = by_ns;
    }

    /// `ns`'s prefixes, or `None` when it has none.
    pub fn of(&self, ns: &[u8]) -> Option<Prefixes> {
        if !self.any.load(Ordering::Acquire) {
            return None;
        }
        self.by_ns
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(ns)
            .cloned()
    }

    /// Make `prefixes` `ns`'s, durably and then here; none deletes its row.
    pub fn set(&self, store: &dyn Kv, ns: &[u8], mut prefixes: Vec<Vec<u8>>) {
        prefixes.sort();
        prefixes.dedup();
        let mut by_ns = self.by_ns.write().unwrap_or_else(|e| e.into_inner());
        if prefixes.is_empty() {
            store.delete(&row_key(ns));
            by_ns.remove(ns);
        } else {
            store.put(&row_key(ns), &encode(&prefixes));
            by_ns.insert(ns.to_vec(), Prefixes::from(prefixes));
        }
        self.any.store(!by_ns.is_empty(), Ordering::Release);
    }

    /// `ns`'s row as stored, for a slot move to carry: `None` when it has
    /// none.
    pub fn row(store: &dyn Kv, ns: &[u8]) -> Option<Vec<u8>> {
        store.get(&row_key(ns))
    }

    /// Install a row a slot move carried from its source, as `set` would.
    pub fn install_row(&self, store: &dyn Kv, ns: &[u8], raw: &[u8]) -> bool {
        match decode(raw) {
            Some(prefixes) => {
                self.set(store, ns, prefixes);
                true
            }
            None => false,
        }
    }
}

/// The prefix of `prefixes` that `key` is under, if any.
pub fn under<'a>(prefixes: &'a [Vec<u8>], key: &[u8]) -> Option<&'a [u8]> {
    prefixes
        .iter()
        .find(|p| key.starts_with(p))
        .map(|p| p.as_slice())
}

/// What a write under `prefix` from anything but a channel answers. The
/// prefix is in hex, since a key prefix need not be text, and the proxy
/// reads it back to send that prefix's writes through the co-processor.
pub fn refusal(prefix: &[u8]) -> String {
    let hex: String = prefix.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "INDEXED {hex} a key under an FT index's prefix is written through the co-processor (ADR-0060)"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use flint_storage::MemKv;

    #[test]
    fn a_row_round_trips_and_guards_its_namespace_only() {
        let kv = MemKv::new();
        let ix = Indexed::default();
        assert!(ix.of(b"t").is_none());
        ix.set(
            &kv,
            b"t",
            vec![b"doc:".to_vec(), b"a\x00b".to_vec(), b"doc:".to_vec()],
        );
        let p = ix.of(b"t").expect("prefixes");
        assert_eq!(p.len(), 2, "sorted, without repeats");
        assert_eq!(under(&p, b"doc:1"), Some(&b"doc:"[..]));
        assert_eq!(under(&p, b"other"), None);
        assert!(
            ix.of(b"u").is_none(),
            "another namespace's keys are not guarded"
        );
        // Read back from the store, as at start or on promotion.
        let again = Indexed::default();
        again.load(&kv);
        assert_eq!(again.of(b"t").as_deref(), Some(&p[..]));
        // A slot move carries the row as stored.
        let row = Indexed::row(&kv, b"t").expect("a row");
        let dest = MemKv::new();
        let moved = Indexed::default();
        assert!(moved.install_row(&dest, b"t", &row));
        assert_eq!(moved.of(b"t").as_deref(), Some(&p[..]));
        // None left: the row goes, and the node stops looking.
        ix.set(&kv, b"t", Vec::new());
        assert!(ix.of(b"t").is_none());
        assert!(kv.get(&row_key(b"t")).is_none());
        assert!(refusal(b"doc:").starts_with("INDEXED 646f633a "));
    }

    #[test]
    fn an_unreadable_row_guards_every_write_of_its_namespace() {
        let kv = MemKv::new();
        kv.put(&row_key(b"t"), b"\x00\x00\x00\x09short");
        let ix = Indexed::default();
        ix.load(&kv);
        let p = ix.of(b"t").expect("guarded");
        assert_eq!(under(&p, b"anything"), Some(&b""[..]));
    }
}
