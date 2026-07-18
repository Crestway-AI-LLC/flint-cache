// SPDX-License-Identifier: Elastic-2.0
//! Write-batching KV overlay (ADR-0005 D4). Wraps the real store; buffers
//! writes and overlays them on reads, so the async-write consumer can run a
//! batch of commands through the normal `Dispatcher` — each computing its
//! exact reply against the accumulating state (an INCR burst on one key sees
//! its own prior increments) — then commit the whole buffer as ONE engine
//! WriteBatch.
//!
//! Scope: only the string/counter write commands the queue accepts route
//! through here, and NONE of them scan; `for_each_prefix` therefore delegates
//! to the underlying store (buffered writes are invisible to a prefix scan by
//! design — the queue never enqueues a scanning command).

use std::collections::HashMap;
use std::sync::Mutex;

use crate::Kv;

pub struct BatchingKv<'a> {
    under: &'a dyn Kv,
    // key -> Some(value) (put) | None (delete). Final state per key; the
    // intermediate values were already observed via the read overlay during
    // dispatch, so only the last op per key needs to reach the store.
    buf: Mutex<HashMap<Vec<u8>, Option<Vec<u8>>>>,
}

impl<'a> BatchingKv<'a> {
    pub fn new(under: &'a dyn Kv) -> Self {
        Self {
            under,
            buf: Mutex::new(HashMap::new()),
        }
    }

    /// The buffered mutations, ready for `RocksKv::apply_writes`.
    pub fn into_ops(self) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
        self.buf
            .into_inner()
            .unwrap_or_default()
            .into_iter()
            .collect()
    }
}

impl Kv for BatchingKv<'_> {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if let Ok(buf) = self.buf.lock()
            && let Some(op) = buf.get(key)
        {
            // Overlay hit: Some(v) = written, None = deleted this batch.
            return op.clone();
        }
        self.under.get(key)
    }

    fn put(&self, key: &[u8], value: &[u8]) {
        if let Ok(mut buf) = self.buf.lock() {
            buf.insert(key.to_vec(), Some(value.to_vec()));
        }
    }

    fn delete(&self, key: &[u8]) -> bool {
        let existed = self.get(key).is_some();
        if let Ok(mut buf) = self.buf.lock() {
            buf.insert(key.to_vec(), None);
        }
        existed
    }

    fn for_each_prefix(&self, prefix: &[u8], visit: &mut dyn FnMut(&[u8], &[u8]) -> bool) {
        // The queue never enqueues a scanning command (see module docs), so a
        // batchable write's buffered rows are never scanned. Delegate.
        self.under.for_each_prefix(prefix, visit);
    }

    fn clear(&self) {
        // FLUSHALL is not batchable (never enqueued), so this is unreachable
        // in the queue path; delegate for completeness.
        self.under.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;

    #[test]
    fn overlay_reads_see_buffered_writes() {
        let under = MemKv::new();
        under.put(b"a", b"0");
        let b = BatchingKv::new(&under);
        // Buffered put is visible to a later get (INCR-on-same-key semantics).
        assert_eq!(b.get(b"a").as_deref(), Some(b"0".as_slice()));
        b.put(b"a", b"1");
        assert_eq!(b.get(b"a").as_deref(), Some(b"1".as_slice()));
        b.put(b"a", b"2");
        assert_eq!(b.get(b"a").as_deref(), Some(b"2".as_slice()));
        // Delete hides the underlying value.
        b.put(b"c", b"z");
        assert!(b.delete(b"c"));
        assert_eq!(b.get(b"c"), None);
        // Underlying is untouched until commit.
        assert_eq!(under.get(b"a").as_deref(), Some(b"0".as_slice()));
        // The buffer is the FINAL state per key.
        let ops = b.into_ops();
        let map: std::collections::HashMap<_, _> = ops.into_iter().collect();
        assert_eq!(map.get(b"a".as_slice()), Some(&Some(b"2".to_vec())));
        assert_eq!(map.get(b"c".as_slice()), Some(&None));
    }
}
