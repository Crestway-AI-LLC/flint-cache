// SPDX-License-Identifier: Elastic-2.0
//! Type-agnostic keyspace operations.
//!
//! DEL, EXISTS, TYPE, EXPIRE/TTL/PERSIST work on any value type by parsing
//! only the common `MetaHeader` prefix of the metadata row. Deleting a
//! complex key is O(1): drop the metadata row and its subkey rows become
//! unreachable orphans (version-prefixed), garbage-collected by compaction
//! filters later.

use crate::Kv;
use crate::encoding::{
    Cf, ComplexMeta, MetaHeader, ValueType, VersionGen, envelope, subkey_prefix, zscore_prefix,
};
use crate::strings::Clock;

/// What a RENAME / RENAMENX did. Three outcomes rather than a bool because
/// the two commands report the same three states differently: a missing
/// source is an ERROR for both, while a taken destination is invisible to
/// RENAME (it overwrites) and is the whole answer for RENAMENX.
#[derive(Debug, PartialEq, Eq)]
pub enum RenameOutcome {
    Renamed,
    /// NX only: the destination was already taken, so nothing moved.
    DestinationExists,
    NoSuchKey,
}

/// TTL query result, Redis-shaped.
#[derive(Debug, PartialEq, Eq)]
pub enum Ttl {
    Missing,  // -2
    NoExpiry, // -1
    Ms(u64),  // remaining
}

pub struct Keyspace<'a> {
    kv: &'a dyn Kv,
    ns: Vec<u8>,
    clock: Clock,
}

impl<'a> Keyspace<'a> {
    pub fn new(kv: &'a dyn Kv, ns: &[u8], clock: Clock) -> Self {
        Self {
            kv,
            ns: ns.to_vec(),
            clock,
        }
    }

    fn meta_key(&self, slot: u16, key: &[u8]) -> Vec<u8> {
        envelope(Cf::Metadata, &self.ns, slot, key)
    }

    /// Live (non-expired) header + full row; lazily deletes expired rows.
    pub(crate) fn read_live_row(&self, slot: u16, key: &[u8]) -> Option<(MetaHeader, Vec<u8>)> {
        let mk = self.meta_key(slot, key);
        let row = self.kv.get(&mk)?;
        let header = MetaHeader::decode(&row)?;
        if header.is_expired((self.clock)()) {
            self.kv.delete(&mk);
            return None;
        }
        Some((header, row))
    }

    pub fn exists(&self, slot: u16, key: &[u8]) -> bool {
        self.read_live_row(slot, key).is_some()
    }

    pub fn value_type(&self, slot: u16, key: &[u8]) -> Option<ValueType> {
        self.read_live_row(slot, key)
            .and_then(|(h, _)| h.value_type())
    }

    /// O(1) for every type: only the metadata row is removed.
    pub fn del(&self, slot: u16, key: &[u8]) -> bool {
        let live = self.read_live_row(slot, key).is_some();
        if live {
            self.kv.delete(&self.meta_key(slot, key));
        }
        live
    }

    /// COPY src dst [REPLACE], within one slot. False = nothing copied:
    /// either the source is absent or the destination exists without
    /// REPLACE. The TTL travels with the value, as in Redis.
    ///
    /// SAME SLOT IS THE CALLER'S JOB. Both keys are addressed with the one
    /// `slot` argument because a copy spanning slots is not a copy this node
    /// can perform — it would write the destination into local rows the node
    /// may not own. The command layer refuses that case; this signature
    /// makes it unrepresentable here rather than merely unlikely.
    ///
    /// COST. O(1) for strings and JSON documents, whose metadata row IS the
    /// value. O(size) for collections, which must be re-keyed row by row:
    /// subkey rows embed the user key, so there is no aliasing trick that
    /// would let two keys share one set of rows. Redis's COPY is O(N) for
    /// the same reason.
    pub fn copy(&self, slot: u16, src: &[u8], dst: &[u8], replace: bool) -> bool {
        let Some((header, row)) = self.read_live_row(slot, src) else {
            return false;
        };
        // `exists` is expiry-aware, so a destination that is merely awaiting
        // collection never blocks a copy.
        if self.exists(slot, dst) {
            if !replace {
                return false;
            }
            // O(1), exactly as DEL: the displaced subkey rows become
            // unreachable orphans under a version no live metadata claims,
            // and the sweeper reclaims them.
            self.kv.delete(&self.meta_key(slot, dst));
        }
        let Some(kind) = header.value_type() else {
            return false;
        };
        match kind {
            // One row holds flags, TTL and payload together, so copying the
            // row verbatim copies all three and cannot drift from however
            // the value type encodes itself.
            ValueType::String | ValueType::Json => {
                self.kv.put(&self.meta_key(slot, dst), &row);
                true
            }
            ValueType::Hash | ValueType::Set | ValueType::ZSet | ValueType::List => {
                let Some(meta) = ComplexMeta::decode(&row) else {
                    return false;
                };
                // A fresh version for the destination. The sweeper matches a
                // row's version against the live metadata OF ITS OWN KEY, so
                // reusing the source's number would in fact be safe — this
                // is about keeping "a version identifies one incarnation of
                // one key" true, so nothing downstream can read shared
                // numbering as shared lineage. It costs one increment.
                let version = VersionGen::next((self.clock)());
                // PATCH the row, never rebuild it. A list's metadata carries
                // head/tail counters past the shared fields, and re-encoding
                // a decoded ComplexMeta drops them — the destination then
                // fails to decode as a list at all. Patching in place is
                // type-agnostic by construction.
                let mut dst_row = row.clone();
                ComplexMeta::write_version(&mut dst_row, version);
                self.rekey_rows(
                    &subkey_prefix(&self.ns, slot, src, meta.version),
                    &subkey_prefix(&self.ns, slot, dst, version),
                );
                if kind == ValueType::ZSet {
                    // The score index is a second family of rows under its
                    // own CF. Miss it and the copy has members with no
                    // ordering: ZSCORE would answer and ZRANGE would not.
                    self.rekey_rows(
                        &zscore_prefix(&self.ns, slot, src, meta.version),
                        &zscore_prefix(&self.ns, slot, dst, version),
                    );
                }
                // Metadata LAST. Until it lands the destination does not
                // exist, so a crash mid-copy leaves unreachable rows for the
                // sweeper rather than a key whose contents are half there.
                self.kv.put(&self.meta_key(slot, dst), &dst_row);
                true
            }
        }
    }

    /// RENAME / RENAMENX within one slot. Returns whether the source
    /// existed at all, and — for the NX form — whether the rename happened.
    ///
    /// NOT O(1), unlike upstream, and that is a consequence of the encoding
    /// rather than an oversight: a collection's subkey rows embed the user
    /// key, so there is no pointer to re-aim. Renaming a large collection
    /// costs the same as copying it. Documented in command-support.md so the
    /// difference is not discovered under load.
    ///
    /// Renaming a key onto ITSELF is a success that must do nothing. Routed
    /// through the copy path it would be destructive — the REPLACE step
    /// retires the destination, which here is also the source.
    pub fn rename(&self, slot: u16, src: &[u8], dst: &[u8], nx: bool) -> RenameOutcome {
        if !self.exists(slot, src) {
            return RenameOutcome::NoSuchKey;
        }
        if src == dst {
            // NX asks whether the destination is free; onto itself it never
            // is, which is why the two forms answer differently here.
            return if nx {
                RenameOutcome::DestinationExists
            } else {
                RenameOutcome::Renamed
            };
        }
        if nx && self.exists(slot, dst) {
            return RenameOutcome::DestinationExists;
        }
        if !self.copy(slot, src, dst, true) {
            // Unreachable given the liveness check above; treated as a
            // failure rather than reported as a rename that did not happen.
            return RenameOutcome::NoSuchKey;
        }
        self.del(slot, src);
        RenameOutcome::Renamed
    }

    /// Re-write every row under `from` to the same suffix under `to`.
    ///
    /// Both prefixes end at the version field, so whatever follows — a hash
    /// field, a list index, a score-and-member — is opaque here and copies
    /// across untouched. That is what lets one routine serve every
    /// collection type and both of the zset's row families.
    ///
    /// Streams rather than materializing: a collection can be far larger
    /// than RAM on a disk-first engine, which is the whole premise. Writing
    /// into a different prefix than the one being scanned keeps this clear
    /// of the visit-time mutation caveat in the `Kv` contract.
    fn rekey_rows(&self, from: &[u8], to: &[u8]) {
        self.kv.for_each_prefix(from, &mut |k, v| {
            let mut nk = to.to_vec();
            nk.extend_from_slice(&k[from.len()..]);
            self.kv.put(&nk, v);
            true
        });
    }

    /// EXPIRE/PEXPIRE (absolute). False if the key does not exist.
    /// A past instant deletes immediately (Redis semantics).
    pub fn expire_at(&self, slot: u16, key: &[u8], at_ms: u64) -> bool {
        let Some((_, mut row)) = self.read_live_row(slot, key) else {
            return false;
        };
        if at_ms <= (self.clock)() {
            self.kv.delete(&self.meta_key(slot, key));
            return true;
        }
        MetaHeader::write_expire(&mut row, at_ms);
        self.kv.put(&self.meta_key(slot, key), &row);
        true
    }

    /// EXPIRETIME/PEXPIRETIME: the ABSOLUTE expiry instant in ms.
    /// None = key missing; Some(0) = key exists with no expiry.
    pub fn expire_time_ms(&self, slot: u16, key: &[u8]) -> Option<u64> {
        self.read_live_row(slot, key).map(|(h, _)| h.expire_ms)
    }

    pub fn ttl(&self, slot: u16, key: &[u8]) -> Ttl {
        match self.read_live_row(slot, key) {
            None => Ttl::Missing,
            Some((h, _)) if h.expire_ms == 0 => Ttl::NoExpiry,
            Some((h, _)) => Ttl::Ms(h.expire_ms - (self.clock)()),
        }
    }

    /// PERSIST: true if an expiry existed and was removed.
    pub fn persist(&self, slot: u16, key: &[u8]) -> bool {
        let Some((h, mut row)) = self.read_live_row(slot, key) else {
            return false;
        };
        if h.expire_ms == 0 {
            return false;
        }
        MetaHeader::write_expire(&mut row, 0);
        self.kv.put(&self.meta_key(slot, key), &row);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;
    use crate::hashes::HashStore;
    use crate::strings::{SetOptions, StringStore};
    use std::sync::atomic::{AtomicU64, Ordering};

    macro_rules! test_clock {
        ($static_name:ident, $fn_name:ident, $initial:expr) => {
            static $static_name: AtomicU64 = AtomicU64::new($initial);
            fn $fn_name() -> u64 {
                $static_name.load(Ordering::Relaxed)
            }
        };
    }

    #[test]
    fn generic_ops_work_on_both_layouts() {
        test_clock!(NOW, now, 5_000_000);
        let kv = MemKv::new();
        let ks = Keyspace::new(&kv, b"t", now);
        let strings = StringStore::new(&kv, b"t", now);
        let hashes = HashStore::new(&kv, b"t", now);

        strings
            .set(1, b"s", b"v", SetOptions::default())
            .expect("set");
        hashes
            .hset(1, b"h", &[(b"f".to_vec(), b"v".to_vec())])
            .expect("hset");

        assert_eq!(ks.value_type(1, b"s"), Some(ValueType::String));
        assert_eq!(ks.value_type(1, b"h"), Some(ValueType::Hash));
        assert!(ks.exists(1, b"h"));

        // TTL machinery is layout-independent.
        assert!(ks.expire_at(1, b"h", 5_000_500));
        assert_eq!(ks.ttl(1, b"h"), Ttl::Ms(500));
        assert!(ks.persist(1, b"h"));
        assert_eq!(ks.ttl(1, b"h"), Ttl::NoExpiry);
        // Hash content survived the header rewrites.
        assert_eq!(
            hashes.hget(1, b"h", b"f").expect("hget"),
            Some(b"v".to_vec())
        );

        // O(1) delete works on both.
        assert!(ks.del(1, b"s"));
        assert!(ks.del(1, b"h"));
        assert!(!ks.exists(1, b"h"));
        assert!(!ks.del(1, b"h"));
    }

    #[test]
    fn expiry_lifecycle() {
        test_clock!(NOW, now, 6_000_000);
        let kv = MemKv::new();
        let ks = Keyspace::new(&kv, b"t", now);
        let strings = StringStore::new(&kv, b"t", now);
        strings
            .set(1, b"k", b"v", SetOptions::default())
            .expect("set");
        assert!(ks.expire_at(1, b"k", 6_000_100));
        NOW.store(6_000_101, Ordering::Relaxed);
        assert_eq!(ks.ttl(1, b"k"), Ttl::Missing);
        assert!(!ks.exists(1, b"k"));
        // Past-instant expiry deletes immediately.
        strings
            .set(1, b"k2", b"v", SetOptions::default())
            .expect("set");
        assert!(ks.expire_at(1, b"k2", 6_000_000));
        assert!(!ks.exists(1, b"k2"));
    }
}
