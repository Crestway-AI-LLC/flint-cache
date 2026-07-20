// SPDX-License-Identifier: Elastic-2.0
//! Writer-writer exclusion for the master's write paths.
//!
//! Why this exists: connection threads dispatch concurrently against the
//! shared store, and every write is a read-modify-write at some layer —
//! INCR/APPEND/SETNX at the value layer, EVERY complex-type mutation at the
//! envelope/meta layer (LPUSH reads the list head before writing it back).
//! Without exclusion, concurrent writers to the SAME key interleave their
//! read and write halves and lose updates (exposed by async_flag_drill: a
//! 24-connection INCR storm on one key lost ~20% of its increments).
//!
//! The scheme (hierarchical, writer-only):
//!   - a single-key write holds GLOBAL.read() + its key's stripe mutex —
//!     writers to different keys proceed in parallel, same-key writers
//!     serialize;
//!   - a multi-key or keyless write (MSET, multi-key DEL/UNLINK, FLUSHALL)
//!     holds GLOBAL.write() — excluding every other writer without
//!     ordered multi-stripe acquisition;
//!   - the async write-queue consumer holds GLOBAL.write() per batch, so
//!     queued batches and inline writers (a flagged tenant's non-batchable
//!     DEL, another tenant's traffic) can never interleave.
//!
//! READS take no lock: a single Kv op is atomic, and Redis-level
//! read-vs-write tearing on multi-part reads (e.g. HGETALL racing HSET) is
//! a separate, pre-existing caveat — this module fixes lost WRITES.
//! The GC sweeper (flint-storage) also runs unlocked; its deletes are
//! confined to expired/orphaned rows.

use std::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};

const STRIPES: usize = 128;

static GLOBAL: RwLock<()> = RwLock::new(());
static STRIPE: [Mutex<()>; STRIPES] = [const { Mutex::new(()) }; STRIPES];

/// Held for the duration of one write dispatch. Variants exist only to keep
/// their guards alive; nothing reads them.
#[allow(dead_code)]
pub enum WriteGuard {
    Single(RwLockReadGuard<'static, ()>, MutexGuard<'static, ()>),
    All(RwLockWriteGuard<'static, ()>),
}

fn stripe_of(key: &[u8]) -> usize {
    // FNV-1a; only distribution matters here.
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h as usize) % STRIPES
}

/// Exclude other writers of `key` (writers to other keys proceed).
/// Namespace is deliberately not mixed in: the envelope key includes it
/// upstream of hashing collisions anyway, and a cross-namespace collision
/// merely serializes two writers that could have run in parallel.
pub fn lock_key(ns: &[u8], key: &[u8]) -> WriteGuard {
    let mut buf = Vec::with_capacity(ns.len() + 1 + key.len());
    buf.extend_from_slice(ns);
    buf.push(b'|');
    buf.extend_from_slice(key);
    let g = GLOBAL.read().unwrap_or_else(|e| e.into_inner());
    let s = STRIPE[stripe_of(&buf)]
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    WriteGuard::Single(g, s)
}

/// Exclude EVERY writer (multi-key/keyless writes; queue batches).
pub fn lock_all() -> WriteGuard {
    WriteGuard::All(GLOBAL.write().unwrap_or_else(|e| e.into_inner()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// The exact shape of the bug: concurrent read-modify-write on one
    /// cell loses updates without the lock, never with it.
    #[test]
    fn same_key_rmw_is_exact_under_lock() {
        let cell = Arc::new(AtomicU64::new(0));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let cell = Arc::clone(&cell);
                std::thread::spawn(move || {
                    for _ in 0..500 {
                        let _g = lock_key(b"ns", b"counter");
                        // Non-atomic RMW on purpose — the lock is the
                        // only thing making it exact.
                        let v = cell.load(Ordering::Relaxed);
                        std::hint::black_box(&v);
                        cell.store(v + 1, Ordering::Relaxed);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().expect("thread");
        }
        assert_eq!(cell.load(Ordering::Relaxed), 8 * 500);
    }

    #[test]
    fn lock_all_excludes_single_key_writers() {
        let _all = lock_all();
        // A stripe attempt from another thread must not complete while
        // GLOBAL is held for write.
        let tried = Arc::new(AtomicU64::new(0));
        let t = {
            let tried = Arc::clone(&tried);
            std::thread::spawn(move || {
                let _g = lock_key(b"ns", b"k");
                tried.store(1, Ordering::SeqCst);
            })
        };
        std::thread::sleep(std::time::Duration::from_millis(80));
        assert_eq!(tried.load(Ordering::SeqCst), 0, "writer got past lock_all");
        drop(_all);
        t.join().expect("thread");
        assert_eq!(tried.load(Ordering::SeqCst), 1);
    }
}
