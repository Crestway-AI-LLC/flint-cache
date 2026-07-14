//! The max-value-size policy (Valkey `proto-max-bulk-len` analog,
//! extended to collections): no single value's payload may grow past the
//! configured cap. On a beyond-RAM engine this is the guard that keeps
//! read-all commands (HGETALL, SMEMBERS, LRANGE 0 -1) serveable — an
//! uncapped collection can exceed physical memory.
//!
//! Contract under test, for every type:
//! - a write that would cross the cap fails with `ValueTooLarge` and
//!   leaves the value byte-for-byte untouched (atomic reject);
//! - deletions return budget, so the same write succeeds afterwards;
//! - replacements are charged by delta, not by sum;
//! - `max = 0` disables the cap.

use flint_storage::MemKv;
use flint_storage::hashes::HashStore;
use flint_storage::lists::ListStore;
use flint_storage::sets::SetStore;
use flint_storage::strings::{SetOptions, StoreError, StringStore, system_clock};
use flint_storage::zsets::ZSetStore;

const CAP: u64 = 100;

#[test]
fn string_set_rejects_past_cap() {
    let kv = MemKv::new();
    let s = StringStore::with_max_value_bytes(&kv, b"t", system_clock, CAP);
    assert_eq!(
        s.set(
            1,
            b"k",
            &vec![b'x'; CAP as usize + 1],
            SetOptions::default()
        ),
        Err(StoreError::ValueTooLarge)
    );
    assert_eq!(s.get(1, b"k"), Ok(None), "rejected SET must not write");
    // Exactly at the cap is allowed.
    assert!(
        s.set(1, b"k", &vec![b'x'; CAP as usize], SetOptions::default())
            .is_ok()
    );
}

#[test]
fn string_append_cannot_build_past_cap_incrementally() {
    let kv = MemKv::new();
    let s = StringStore::with_max_value_bytes(&kv, b"t", system_clock, CAP);
    assert_eq!(s.append(1, b"k", &[b'a'; 60]), Ok(60));
    assert_eq!(
        s.append(1, b"k", &[b'b'; 41]),
        Err(StoreError::ValueTooLarge),
        "60 + 41 crosses the cap"
    );
    assert_eq!(s.strlen(1, b"k"), Ok(60), "rejected APPEND must not write");
    assert_eq!(s.append(1, b"k", &[b'b'; 40]), Ok(100));
}

#[test]
fn hash_rejects_atomically_and_frees_budget_on_hdel() {
    let kv = MemKv::new();
    let h = HashStore::with_max_value_bytes(&kv, b"t", system_clock, CAP);
    // "f1" (2) + 78 value bytes = 80 of 100.
    h.hset(1, b"h", &[(b"f1".to_vec(), vec![b'v'; 78])])
        .expect("hset under cap");
    // "f2" (2) + 30 would land at 112: reject, hash untouched.
    assert_eq!(
        h.hset(1, b"h", &[(b"f2".to_vec(), vec![b'v'; 30])]),
        Err(StoreError::ValueTooLarge)
    );
    assert_eq!(h.hlen(1, b"h"), Ok(1), "rejected HSET must not add fields");
    assert_eq!(h.hget(1, b"h", b"f2"), Ok(None));
    // Deleting f1 returns its 80 bytes; the same HSET now fits.
    assert_eq!(h.hdel(1, b"h", &[b"f1".to_vec()]), Ok(1));
    assert_eq!(h.hset(1, b"h", &[(b"f2".to_vec(), vec![b'v'; 30])]), Ok(1));
}

#[test]
fn hash_replacement_is_charged_by_delta() {
    let kv = MemKv::new();
    let h = HashStore::with_max_value_bytes(&kv, b"t", system_clock, CAP);
    h.hset(1, b"h", &[(b"f".to_vec(), vec![b'v'; 90])])
        .expect("hset");
    // Replacing 90 bytes with 99 is a delta of +9 (1 + 99 = 100): fits.
    assert_eq!(h.hset(1, b"h", &[(b"f".to_vec(), vec![b'v'; 99])]), Ok(0));
    // Replacing with 100 would total 101: reject, old value intact.
    assert_eq!(
        h.hset(1, b"h", &[(b"f".to_vec(), vec![b'w'; 100])]),
        Err(StoreError::ValueTooLarge)
    );
    assert_eq!(h.hget(1, b"h", b"f"), Ok(Some(vec![b'v'; 99])));
}

#[test]
fn hash_duplicate_fields_in_one_call_do_not_double_count() {
    let kv = MemKv::new();
    let h = HashStore::with_max_value_bytes(&kv, b"t", system_clock, CAP);
    // Same field twice: last write wins, charged once (1 + 90 = 91).
    assert_eq!(
        h.hset(
            1,
            b"h",
            &[
                (b"f".to_vec(), vec![b'a'; 90]),
                (b"f".to_vec(), vec![b'b'; 90]),
            ],
        ),
        Ok(1)
    );
    assert_eq!(h.hget(1, b"h", b"f"), Ok(Some(vec![b'b'; 90])));
}

#[test]
fn set_rejects_atomically_and_frees_budget_on_srem() {
    let kv = MemKv::new();
    let s = SetStore::with_max_value_bytes(&kv, b"t", system_clock, CAP);
    let big = vec![b'm'; 80];
    assert_eq!(s.sadd(1, b"s", std::slice::from_ref(&big)), Ok(1));
    // 80 + 30 crosses: reject whole call, set untouched.
    assert_eq!(
        s.sadd(1, b"s", &[vec![b'n'; 30]]),
        Err(StoreError::ValueTooLarge)
    );
    assert_eq!(s.scard(1, b"s"), Ok(1));
    // Re-adding an existing member costs nothing.
    assert_eq!(s.sadd(1, b"s", std::slice::from_ref(&big)), Ok(0));
    assert_eq!(s.srem(1, b"s", std::slice::from_ref(&big)), Ok(1));
    assert_eq!(s.sadd(1, b"s", &[vec![b'n'; 30]]), Ok(1));
}

#[test]
fn zset_score_updates_are_free_and_zrem_frees_budget() {
    let kv = MemKv::new();
    let z = ZSetStore::with_max_value_bytes(&kv, b"t", system_clock, CAP);
    // Member cost = len + 8 for the score: 84 + 8 = 92 of 100.
    let m = vec![b'z'; 84];
    assert_eq!(z.zadd(1, b"z", &[(1.0, m.clone())]), Ok(1));
    // A new member would cross; a score update of the existing one is free.
    assert_eq!(
        z.zadd(1, b"z", &[(2.0, vec![b'y'; 10])]),
        Err(StoreError::ValueTooLarge)
    );
    assert_eq!(z.zadd(1, b"z", &[(5.0, m.clone())]), Ok(0));
    assert_eq!(z.zscore(1, b"z", &m), Ok(Some(5.0)));
    assert_eq!(z.zrem(1, b"z", std::slice::from_ref(&m)), Ok(1));
    assert_eq!(z.zadd(1, b"z", &[(2.0, vec![b'y'; 10])]), Ok(1));
}

#[test]
fn list_rejects_atomically_and_pop_frees_budget() {
    let kv = MemKv::new();
    let l = ListStore::with_max_value_bytes(&kv, b"t", system_clock, CAP);
    assert_eq!(l.push(1, b"l", &[vec![b'a'; 60]], false), Ok(1));
    // 60 + (30 + 30) crosses; the whole multi-value push is rejected.
    assert_eq!(
        l.push(1, b"l", &[vec![b'b'; 30], vec![b'c'; 30]], false),
        Err(StoreError::ValueTooLarge)
    );
    assert_eq!(l.llen(1, b"l"), Ok(1), "rejected push must not append");
    assert_eq!(l.pop(1, b"l", true), Ok(Some(vec![b'a'; 60])));
    assert_eq!(
        l.push(1, b"l", &[vec![b'b'; 30], vec![b'c'; 30]], false),
        Ok(2)
    );
}

#[test]
fn zero_disables_the_cap() {
    let kv = MemKv::new();
    let s = StringStore::with_max_value_bytes(&kv, b"t", system_clock, 0);
    assert!(
        s.set(1, b"k", &vec![b'x'; 2 * 1024 * 1024], SetOptions::default())
            .is_ok(),
        "max = 0 must mean unlimited"
    );
}
