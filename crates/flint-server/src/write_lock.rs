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

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

const STRIPES: usize = 128;

static GLOBAL: RwLock<()> = RwLock::new(());
// RwLock, not Mutex (ADR-0027). A read-modify-write needs to exclude every
// other writer of its key, but a PURE write -- one that reads nothing -- has
// no stale read for a concurrent writer to invalidate, so pure writes only
// need to exclude the RMW ones, not each other. Shared mode is what lets a
// batch of plain SETs hold many stripes at once without serialising against
// another connection doing the same: at 256 keys a batch touches ~111 of
// these 128, so exclusive acquisition would BE lock_all, which is measured at
// -28% to -31% (see ADR-0027).
static STRIPE: [RwLock<()>; STRIPES] = [const { RwLock::new(()) }; STRIPES];

/// Held for the duration of one write dispatch. Variants exist only to keep
/// their guards alive; nothing reads them.
#[allow(dead_code)]
pub enum WriteGuard {
    /// A read-modify-write of one key: exclusive on its stripe.
    Single(RwLockReadGuard<'static, ()>, RwLockWriteGuard<'static, ()>),
    /// A PURE write of one key -- reads nothing, so it excludes the RMW
    /// writers of that key but not other pure writes.
    Shared(RwLockReadGuard<'static, ()>, RwLockReadGuard<'static, ()>),
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
        .write()
        .unwrap_or_else(|e| e.into_inner());
    WriteGuard::Single(g, s)
}

/// Exclude the READ-MODIFY-WRITE writers of `key`, but not other pure writes
/// (ADR-0027).
///
/// Sound only for a write that reads NOTHING. Two such writes to one key can
/// race freely: neither holds a stale read, so whichever the engine orders
/// last wins, which is exactly what the exclusive lock would have produced.
/// An INCR or any complex-type mutation on the same key takes `lock_key` and
/// is excluded, so its read-modify-write stays atomic.
///
/// The caller decides, and getting that wrong is a LOST UPDATE rather than a
/// slow path. `write_queue::is_batchable` is not the test -- it admits INCR,
/// DECR, APPEND and SETNX, which are precisely the cases that must not use
/// this.
pub fn lock_key_pure(ns: &[u8], key: &[u8]) -> WriteGuard {
    let mut buf = Vec::with_capacity(ns.len() + 1 + key.len());
    buf.extend_from_slice(ns);
    buf.push(b'|');
    buf.extend_from_slice(key);
    let g = GLOBAL.read().unwrap_or_else(|e| e.into_inner());
    let s = STRIPE[stripe_of(&buf)]
        .read()
        .unwrap_or_else(|e| e.into_inner());
    WriteGuard::Shared(g, s)
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

    /// These tests drive PROCESS-GLOBAL lock state, and cargo runs tests in a
    /// binary concurrently, so two of them overlapping is not a hypothetical:
    /// it deadlocked the suite for 56 minutes on 2026-08-31. One test held
    /// `GLOBAL.write()` while another's spawned thread parked on
    /// `GLOBAL.read()`, and the second test's main thread was waiting to join
    /// it.
    ///
    /// The mechanism is worth naming because it is the one ADR-0027 flagged
    /// as its main risk: **`GLOBAL.read()` blocks behind a PENDING writer**.
    /// That is writer-preferring behaviour, and it is correct -- it is what
    /// stops a stream of readers starving a writer -- but it means a
    /// `lock_all()` holder (MSET, FLUSHALL, multi-key DEL) stalls pure writes
    /// too, not only exclusive ones. Confirmed here by observation rather
    /// than assumed from the docs, which do not specify a policy.
    ///
    /// Poisoning is absorbed: a panicking test must fail on its own assert,
    /// not cascade into every later test failing to acquire this.
    static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    fn serial() -> std::sync::MutexGuard<'static, ()> {
        TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The exact shape of the bug: concurrent read-modify-write on one
    /// cell loses updates without the lock, never with it.
    #[test]
    fn same_key_rmw_is_exact_under_lock() {
        let _serial = serial();
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

    /// The property the whole of ADR-0027 rests on: two PURE writes of the
    /// SAME key do not block each other. If this ever fails, batching a
    /// pipeline degenerates back into the serialisation that made the async
    /// queue 30% slower, and it would fail SILENTLY -- as a throughput
    /// regression, not a wrong answer.
    #[test]
    fn two_pure_writes_of_one_key_run_concurrently() {
        let _serial = serial();
        let inside = Arc::new(AtomicU64::new(0));
        let _held = lock_key_pure(b"ns", b"k");
        inside.fetch_add(1, Ordering::SeqCst);
        let t = {
            let inside = Arc::clone(&inside);
            std::thread::spawn(move || {
                let _g = lock_key_pure(b"ns", b"k");
                inside.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(std::time::Duration::from_millis(80));
                inside.load(Ordering::SeqCst)
            })
        };
        let both = t.join().expect("thread");
        assert_eq!(
            both, 2,
            "a second pure write of the same key must not wait for the first"
        );
    }

    /// And the correctness half: a pure write is still excluded by the
    /// read-modify-write writer of that key, so INCR's read and write halves
    /// cannot be split by a SET landing between them.
    #[test]
    fn a_pure_write_waits_for_the_rmw_writer_of_its_key() {
        let _serial = serial();
        let _rmw = lock_key(b"ns", b"k");
        let tried = Arc::new(AtomicU64::new(0));
        let t = {
            let tried = Arc::clone(&tried);
            std::thread::spawn(move || {
                let _g = lock_key_pure(b"ns", b"k");
                tried.store(1, Ordering::SeqCst);
            })
        };
        std::thread::sleep(std::time::Duration::from_millis(80));
        assert_eq!(
            tried.load(Ordering::SeqCst),
            0,
            "pure write got past an RMW holder of the same key"
        );
        drop(_rmw);
        t.join().expect("thread");
        assert_eq!(tried.load(Ordering::SeqCst), 1);
    }

    /// A pure write on a DIFFERENT key is unaffected by an RMW elsewhere --
    /// the stripe scheme's original point, preserved.
    #[test]
    fn a_pure_write_of_another_key_is_not_blocked_by_an_rmw() {
        let _serial = serial();
        let _rmw = lock_key(b"ns", b"counter");
        // Any key that hashes to a different stripe; assert that it does, so
        // this test cannot pass for the wrong reason if the hash changes.
        let a = {
            let mut b = b"ns".to_vec();
            b.push(b'|');
            b.extend_from_slice(b"counter");
            stripe_of(&b)
        };
        let other = (0..)
            .map(|i| format!("k{i}"))
            .find(|k| {
                let mut b = b"ns".to_vec();
                b.push(b'|');
                b.extend_from_slice(k.as_bytes());
                stripe_of(&b) != a
            })
            .expect("some key lands on another stripe");
        let done = Arc::new(AtomicU64::new(0));
        let t = {
            let done = Arc::clone(&done);
            std::thread::spawn(move || {
                let _g = lock_key_pure(b"ns", other.as_bytes());
                done.store(1, Ordering::SeqCst);
            })
        };
        t.join().expect("thread");
        assert_eq!(done.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn lock_all_excludes_single_key_writers() {
        let _serial = serial();
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
