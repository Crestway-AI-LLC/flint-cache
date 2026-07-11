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

/// One full sweep. Pass 1 drops expired metadata rows; pass 2 drops
/// subkey/zscore rows whose (key, version) no longer matches live metadata —
/// which also reclaims the orphans pass 1 just created.
pub fn sweep(kv: &dyn Kv, now_ms: u64) -> SweepReport {
    let mut report = SweepReport::default();

    for (k, row) in kv.scan_prefix(&[Cf::Metadata as u8]) {
        if MetaHeader::decode(&row).is_some_and(|h| h.is_expired(now_ms)) {
            kv.delete(&k);
            report.expired_meta += 1;
        }
    }

    for tag in [Cf::Subkey as u8, Cf::ZScore as u8] {
        for (k, _) in kv.scan_prefix(&[tag]) {
            let Some((ns, slot, user_key, version)) = parse_subkey_envelope(&k) else {
                continue;
            };
            let live_version = kv
                .get(&envelope(Cf::Metadata, ns, slot, user_key))
                .and_then(|row| {
                    let header = MetaHeader::decode(&row)?;
                    if header.is_expired(now_ms) {
                        return None;
                    }
                    ComplexMeta::decode(&row).map(|m| m.version)
                });
            if live_version != Some(version) {
                kv.delete(&k);
                report.orphan_rows += 1;
            }
        }
    }
    report
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
            kv.scan_prefix(&[Cf::Subkey as u8]).len() + kv.scan_prefix(&[Cf::ZScore as u8]).len();
        assert_eq!(
            physical_subkeys, 7,
            "3 hash fields + 2 members + 2 index rows"
        );

        // Clock passes the string's expiry.
        NOW.store(1_000_200, Ordering::Relaxed);
        let report = sweep(&kv, now());
        assert_eq!(
            report,
            SweepReport {
                expired_meta: 1,
                orphan_rows: 7
            }
        );
        assert_eq!(kv.scan_prefix(&[Cf::Subkey as u8]).len(), 0);
        assert_eq!(kv.scan_prefix(&[Cf::ZScore as u8]).len(), 0);
        assert_eq!(kv.scan_prefix(&[Cf::Metadata as u8]).len(), 0);

        // Idempotent.
        assert_eq!(sweep(&kv, now()), SweepReport::default());
        NOW.store(1_000_000, Ordering::Relaxed);
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
        let report = sweep(&kv, now());
        assert_eq!(report.orphan_rows, 1, "only the old-version row goes");
        assert_eq!(h.hget(1, b"live", b"g"), Ok(Some(b"w".to_vec())));
        assert_eq!(h.hlen(1, b"live"), Ok(1));
    }
}
