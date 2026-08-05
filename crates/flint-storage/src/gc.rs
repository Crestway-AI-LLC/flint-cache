// SPDX-License-Identifier: Elastic-2.0
//! Garbage collection: expired metadata rows and orphaned subkey rows.
//!
//! O(1) deletes and lazy expiry leave physically-present but unreachable
//! rows behind by design. Production reclamation is the compaction filter
//! (RocksKv installs one for expired metadata); this sweeper is the
//! engine-agnostic implementation used by the mem engine, by tests, and as
//! the v0 orphan reclaimer until the filter grows a metadata-lookup handle.

use crate::Kv;
use crate::encoding::{Cf, ComplexMeta, MetaHeader, envelope, parse_subkey_envelope};

#[derive(Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    pub expired_meta: u64,
    pub orphan_rows: u64,
}

/// A per-key critical section the caller supplies: the returned guard is
/// held while the sweeper RE-VERIFIES and deletes. The server passes its
/// `write_lock::lock_key`; tests pass a no-op or an adversarial closure.
pub type KeyLock<'a> = &'a dyn Fn(&[u8], &[u8]) -> Box<dyn std::any::Any>;

/// One full sweep. Pass 1 drops expired metadata rows; pass 2 drops
/// subkey/zscore rows whose (key, version) no longer matches live metadata —
/// which also reclaims the orphans pass 1 just created.
///
/// Every delete is judge-lock-REJUDGE-delete. The scan's verdict is only a
/// candidate: between reading a row and deleting it, a writer can recreate
/// the key, and pass 2's window is the sharper one — a key recreated after
/// its meta was reclaimed starts again at version 1, the same version the
/// dead rows carry, so "orphan" judged before the recreation and deleted
/// after it would take a LIVE field with it. Holding the caller's per-key
/// lock and re-running the full judgment inside it makes the delete and
/// any concurrent write strictly ordered; whichever loses re-observes.
pub fn sweep(kv: &dyn Kv, now_ms: u64, lock_key: KeyLock) -> SweepReport {
    let mut report = SweepReport::default();

    // Both passes cover whole CFs, so they stream: a materialized scan of
    // ranges this size is exactly the DBSIZE OOM. Deleting mid-scan is
    // inside the `for_each_prefix` contract.
    kv.for_each_prefix(&[Cf::Metadata as u8], &mut |k, row| {
        if MetaHeader::decode(row).is_some_and(|h| h.is_expired(now_ms)) {
            let Some((ns, user_key)) = parse_meta_envelope(k) else {
                return true;
            };
            let _guard = lock_key(ns, user_key);
            // Re-judge under the lock: the row a writer just recreated is
            // not the row the scan condemned.
            let still_expired = kv
                .get(k)
                .and_then(|row| MetaHeader::decode(&row))
                .is_some_and(|h| h.is_expired(now_ms));
            if still_expired && kv.delete(k) {
                report.expired_meta += 1;
            }
        }
        true
    });

    for tag in [Cf::Subkey as u8, Cf::ZScore as u8] {
        kv.for_each_prefix(&[tag], &mut |k, _| {
            let Some((ns, slot, user_key, version)) = parse_subkey_envelope(k) else {
                return true;
            };
            let judge = || {
                let live_version = kv
                    .get(&envelope(Cf::Metadata, ns, slot, user_key))
                    .and_then(|row| {
                        let header = MetaHeader::decode(&row)?;
                        if header.is_expired(now_ms) {
                            return None;
                        }
                        ComplexMeta::decode(&row).map(|m| m.version)
                    });
                live_version != Some(version)
            };
            if judge() {
                let _guard = lock_key(ns, user_key);
                if judge() && kv.delete(k) {
                    report.orphan_rows += 1;
                }
            }
            true
        });
    }
    report
}

/// The metadata envelope's (ns, user_key): `cf | ns_len | ns | slot(2) | key`.
fn parse_meta_envelope(k: &[u8]) -> Option<(&[u8], &[u8])> {
    if k.first() != Some(&(Cf::Metadata as u8)) {
        return None;
    }
    let ns_len = *k.get(1)? as usize;
    let ns = k.get(2..2 + ns_len)?;
    let user_key = k.get(2 + ns_len + 2..)?;
    Some((ns, user_key))
}

/// A no-op lock for callers with no concurrent writers (tests, the mem
/// engine's single-threaded uses).
pub fn unguarded(_: &[u8], _: &[u8]) -> Box<dyn std::any::Any> {
    Box::new(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;
    use crate::hashes::HashStore;
    use crate::keyspace::Keyspace;
    use crate::strings::{SetExpiry, SetOptions, StringStore};
    use crate::zsets::ZSetStore;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NOW: AtomicU64 = AtomicU64::new(1_000_000);
    fn now() -> u64 {
        NOW.load(Ordering::Relaxed)
    }

    #[test]
    fn sweep_reclaims_orphans_and_expired() {
        let kv = MemKv::new();
        let ks = Keyspace::new(&kv, b"t", now);
        let h = HashStore::new(&kv, b"t", now);
        let z = ZSetStore::new(&kv, b"t", now);
        let s = StringStore::new(&kv, b"t", now);

        // Hash with 3 fields, zset with 2 members (2 rows each: member + index).
        h.hset(
            1,
            b"h",
            &[
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ],
        )
        .expect("hset");
        z.zadd(1, b"z", &[(1.0, b"m1".to_vec()), (2.0, b"m2".to_vec())])
            .expect("zadd");
        // Expiring string.
        s.set(
            1,
            b"tmp",
            b"v",
            SetOptions {
                expiry: SetExpiry::AtMs(1_000_100),
                ..Default::default()
            },
        )
        .expect("set");

        // O(1) deletes: meta rows vanish, subkey rows remain as orphans.
        assert!(ks.del(1, b"h"));
        assert!(ks.del(1, b"z"));
        let physical_subkeys =
            kv.count_prefix(&[Cf::Subkey as u8]) + kv.count_prefix(&[Cf::ZScore as u8]);
        assert_eq!(
            physical_subkeys, 7,
            "3 hash fields + 2 members + 2 index rows"
        );

        // Clock passes the string's expiry.
        NOW.store(1_000_200, Ordering::Relaxed);
        let report = sweep(&kv, now(), &unguarded);
        assert_eq!(
            report,
            SweepReport {
                expired_meta: 1,
                orphan_rows: 7
            }
        );
        assert_eq!(kv.count_prefix(&[Cf::Subkey as u8]), 0);
        assert_eq!(kv.count_prefix(&[Cf::ZScore as u8]), 0);
        assert_eq!(kv.count_prefix(&[Cf::Metadata as u8]), 0);

        // Idempotent.
        assert_eq!(sweep(&kv, now(), &unguarded), SweepReport::default());
        NOW.store(1_000_000, Ordering::Relaxed);
    }

    /// The race the lock exists for, made deterministic: the adversarial
    /// lock closure RECREATES the key at the moment the sweeper acquires
    /// the lock — i.e., the concurrent writer wins the race just before
    /// the delete. The re-judgment inside the lock must then spare every
    /// row of the revived key. Without the re-judge, this test deletes a
    /// live field: a key recreated after its meta was reclaimed starts
    /// again at version 1, exactly the version the dead rows carried.
    #[test]
    fn a_key_revived_at_the_lock_is_spared() {
        use std::sync::atomic::AtomicBool;
        let kv = MemKv::new();
        let h = HashStore::new(&kv, b"t", now);
        h.hset(1, b"hk", &[(b"f".to_vec(), b"old".to_vec())])
            .expect("hset");
        // Expire the key so pass 1 condemns its meta and pass 2 its field.
        let meta_key = envelope(Cf::Metadata, b"t", 1, b"hk");
        let mut row = kv.get(&meta_key).expect("meta");
        // Rewrite the header's expiry to the past (flags byte + 8B expiry).
        row[1..9].copy_from_slice(&(now() - 1).to_be_bytes());
        kv.put(&meta_key, &row);

        let revived = AtomicBool::new(false);
        let report = sweep(&kv, now(), &|ns: &[u8], user_key: &[u8]| {
            if ns == b"t" && user_key == b"hk" && !revived.swap(true, Ordering::SeqCst) {
                // The writer lands first: full recreation, version 1 again.
                let h2 = HashStore::new(&kv, b"t", now);
                kv.delete(&envelope(Cf::Metadata, b"t", 1, b"hk"));
                h2.hset(1, b"hk", &[(b"f".to_vec(), b"new".to_vec())])
                    .expect("revive");
            }
            Box::new(())
        });
        // The revived key survives whole: its meta was not deleted (the
        // re-judge saw an unexpired row) and its field was not deleted (the
        // re-judge saw a live matching version).
        let h3 = HashStore::new(&kv, b"t", now);
        assert_eq!(
            h3.hget(1, b"hk", b"f").expect("hget"),
            Some(b"new".to_vec()),
            "the sweeper deleted a field of a key revived before its lock"
        );
        // And the sweep itself reports having reclaimed nothing of it.
        assert_eq!(report.expired_meta, 0, "{report:?}");
    }

    #[test]
    fn sweep_spares_live_data() {
        let kv = MemKv::new();
        let h = HashStore::new(&kv, b"t", now);
        h.hset(1, b"live", &[(b"f".to_vec(), b"v".to_vec())])
            .expect("hset");
        // Recreate: old version orphaned, new version live.
        let ks = Keyspace::new(&kv, b"t", now);
        ks.del(1, b"live");
        h.hset(1, b"live", &[(b"g".to_vec(), b"w".to_vec())])
            .expect("hset");
        let report = sweep(&kv, now(), &unguarded);
        assert_eq!(report.orphan_rows, 1, "only the old-version row goes");
        assert_eq!(h.hget(1, b"live", b"g"), Ok(Some(b"w".to_vec())));
        assert_eq!(h.hlen(1, b"live"), Ok(1));
    }
}
