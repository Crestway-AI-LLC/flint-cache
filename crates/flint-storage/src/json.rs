// SPDX-License-Identifier: Elastic-2.0
//! JSON documents: the storage half.
//!
//! A document is ONE metadata row holding its serialized bytes — the same
//! payload-in-metadata shape strings use (`StringMeta::new_typed`), so a
//! large document lives beyond RAM through the LSM exactly like any other
//! value, and TTL/DEL/EXPIRE/TYPE work through the generic keyspace layer
//! with no special cases.
//!
//! This layer stays deliberately dumb: it reads and writes opaque bytes,
//! enforces the value-size cap, and gates on the type tag. All parsing,
//! path resolution, and mutation live in the command layer (flint-server),
//! which is where the JSON dependency belongs — storage never learns what
//! a document means.
//!
//! Why not subkeys (one row per JSON field, like hashes)? Because document
//! reads are overwhelmingly whole-document, and a shredded document turns
//! one point lookup into a prefix scan plus a reassembly. Sub-document
//! writes rewrite the row; that is the honest trade of this v1, and it
//! matches how the value-size cap already bounds a single write.

use crate::Kv;
use crate::encoding::{Cf, MetaHeader, StringMeta, ValueType, envelope};
use crate::strings::{Clock, StoreError};

pub struct JsonStore<'a> {
    kv: &'a dyn Kv,
    ns: Vec<u8>,
    clock: Clock,
    max_value_bytes: u64,
}

impl<'a> JsonStore<'a> {
    pub fn new(kv: &'a dyn Kv, ns: &[u8], clock: Clock) -> Self {
        Self::with_max_value_bytes(kv, ns, clock, crate::DEFAULT_MAX_VALUE_BYTES)
    }

    /// `max_value_bytes` = 0 disables the cap.
    pub fn with_max_value_bytes(kv: &'a dyn Kv, ns: &[u8], clock: Clock, max: u64) -> Self {
        Self {
            kv,
            ns: ns.to_vec(),
            clock,
            max_value_bytes: if max == 0 { u64::MAX } else { max },
        }
    }

    fn meta_key(&self, slot: u16, key: &[u8]) -> Vec<u8> {
        envelope(Cf::Metadata, &self.ns, slot, key)
    }

    /// The live document's bytes, or None if absent/expired. WRONGTYPE if
    /// the key holds a non-JSON type — the same gate every typed store
    /// applies, so `JSON.GET` on a string errors instead of guessing.
    pub fn get(&self, slot: u16, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.read_live(slot, key)?.map(|(doc, _)| doc))
    }

    /// (document bytes, current expiry) — the expiry rides along so a
    /// sub-path write can preserve it (JSON.SET on a path keeps the TTL,
    /// like every other in-place mutation in this engine).
    fn read_live(&self, slot: u16, key: &[u8]) -> Result<Option<(Vec<u8>, u64)>, StoreError> {
        let mk = self.meta_key(slot, key);
        let Some(row) = self.kv.get(&mk) else {
            return Ok(None);
        };
        let Some(header) = MetaHeader::decode(&row) else {
            return Ok(None);
        };
        if header.is_expired((self.clock)()) {
            self.kv.delete(&mk);
            return Ok(None);
        }
        if header.value_type() != Some(ValueType::Json) {
            return Err(StoreError::WrongType);
        }
        let Some(meta) = StringMeta::decode(&row) else {
            return Ok(None);
        };
        Ok(Some((meta.payload, header.expire_ms)))
    }

    /// True when the key holds a live JSON document.
    pub fn exists(&self, slot: u16, key: &[u8]) -> Result<bool, StoreError> {
        Ok(self.read_live(slot, key)?.is_some())
    }

    /// Write a document, preserving whatever expiry the key already had.
    ///
    /// EVERY document write goes through here — whole-document replacement
    /// as much as a sub-path edit — because both are mutations of an
    /// existing key, not fresh keys. Clearing the TTL on a root write would
    /// quietly turn an expiring document into an immortal one, which for a
    /// cache is the expensive direction to be wrong in; RedisJSON preserves
    /// it too. A fresh key has no expiry to keep, so it lands at 0.
    ///
    /// The read this does to recover the expiry also re-applies the type
    /// gate: writing a document over a string errors here as well as in the
    /// command handler. Belt and braces, and it costs a read we already
    /// wanted.
    pub fn set(&self, slot: u16, key: &[u8], doc: &[u8]) -> Result<(), StoreError> {
        let keep = self.read_live(slot, key)?.map(|(_, e)| e).unwrap_or(0);
        self.write(slot, key, doc, keep)
    }

    fn write(&self, slot: u16, key: &[u8], doc: &[u8], expire_ms: u64) -> Result<(), StoreError> {
        if doc.len() as u64 > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        let row = StringMeta::new_typed(ValueType::Json, doc.to_vec(), expire_ms, (self.clock)())
            .encode();
        self.kv.put(&self.meta_key(slot, key), &row);
        Ok(())
    }

    /// Delete the whole document. True when something was removed.
    /// (Path-scoped deletes are a read-modify-write in the command layer.)
    pub fn delete(&self, slot: u16, key: &[u8]) -> Result<bool, StoreError> {
        if self.read_live(slot, key)?.is_none() {
            return Ok(false);
        }
        Ok(self.kv.delete(&self.meta_key(slot, key)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;
    use crate::strings::{StringStore, system_clock};

    fn store(kv: &MemKv) -> JsonStore<'_> {
        JsonStore::new(kv, b"t", system_clock)
    }

    #[test]
    fn set_get_delete_roundtrip() {
        let kv = MemKv::new();
        let j = store(&kv);
        assert_eq!(j.get(1, b"d").expect("get"), None);
        j.set(1, b"d", br#"{"a":1}"#).expect("set");
        assert_eq!(
            j.get(1, b"d").expect("get").as_deref(),
            Some(&br#"{"a":1}"#[..])
        );
        assert!(j.exists(1, b"d").expect("exists"));
        assert!(j.delete(1, b"d").expect("del"));
        assert!(!j.delete(1, b"d").expect("del again"));
        assert_eq!(j.get(1, b"d").expect("get"), None);
    }

    #[test]
    fn wrongtype_both_directions() {
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", system_clock);
        let j = store(&kv);
        s.set(1, b"str", b"v", Default::default()).expect("set str");
        // JSON ops on a string are WRONGTYPE...
        assert_eq!(j.get(1, b"str"), Err(StoreError::WrongType));
        assert_eq!(j.delete(1, b"str"), Err(StoreError::WrongType));
        // ...and string ops on a document are too.
        j.set(1, b"doc", br#"{"a":1}"#).expect("set doc");
        assert_eq!(s.get(1, b"doc"), Err(StoreError::WrongType));
    }

    #[test]
    fn a_document_write_never_clobbers_a_foreign_type() {
        let kv = MemKv::new();
        let s = StringStore::new(&kv, b"t", system_clock);
        let j = store(&kv);
        s.set(1, b"k", b"v", Default::default()).expect("set str");
        // The command layer refuses this first (WRONGTYPE, asserted in the
        // conformance corpus); storage refuses it too, so no path into this
        // crate can silently destroy a string with a document.
        assert_eq!(j.set(1, b"k", b"[1,2]"), Err(StoreError::WrongType));
        assert_eq!(s.get(1, b"k").expect("get").as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn every_write_preserves_expiry_root_replacement_included() {
        let kv = MemKv::new();
        let j = store(&kv);
        // Write with an expiry by hand (the command layer does this through
        // the keyspace layer's EXPIRE), then confirm neither a sub-path
        // rewrite nor a whole-document replacement clears it. The root case
        // is the one that matters: a TTL'd document must not become
        // immortal because someone replaced its contents.
        let far = system_clock() + 60_000;
        let row =
            StringMeta::new_typed(ValueType::Json, br#"{"a":1}"#.to_vec(), far, 5_000).encode();
        kv.put(&envelope(Cf::Metadata, b"t", 1, b"d"), &row);
        for doc in [&br#"{"a":2}"#[..], &br#"[9]"#[..]] {
            j.set(1, b"d", doc).expect("write");
            let raw = kv.get(&envelope(Cf::Metadata, b"t", 1, b"d")).expect("row");
            let h = MetaHeader::decode(&raw).expect("header");
            assert_eq!(h.expire_ms, far, "expiry survived");
            assert_eq!(h.value_type(), Some(ValueType::Json));
        }
    }

    #[test]
    fn expired_document_reads_as_missing() {
        let kv = MemKv::new();
        let j = store(&kv);
        let past = system_clock() - 1;
        let row = StringMeta::new_typed(ValueType::Json, b"{}".to_vec(), past, 5_000).encode();
        kv.put(&envelope(Cf::Metadata, b"t", 1, b"d"), &row);
        assert_eq!(j.get(1, b"d").expect("get"), None);
    }

    #[test]
    fn oversized_document_is_refused_atomically() {
        let kv = MemKv::new();
        let j = JsonStore::with_max_value_bytes(&kv, b"t", system_clock, 16);
        j.set(1, b"d", br#"{"a":1}"#).expect("small ok");
        assert_eq!(j.set(1, b"d", &[b'x'; 32]), Err(StoreError::ValueTooLarge));
        // The store is untouched by the refused write.
        assert_eq!(
            j.get(1, b"d").expect("get").as_deref(),
            Some(&br#"{"a":1}"#[..])
        );
    }
}
