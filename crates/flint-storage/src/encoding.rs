//! Encoding layer v1 (ADR-0002): the key envelope and metadata row layout.
//!
//! The envelope `cf-tag | namespace | slot | user_key` is a SYSTEM
//! INVARIANT owned by this module, not by any encoding variant: slot
//! migration (prefix scan + range delete) and per-slot accounting depend on
//! it. Encoding variants own only what sits inside the metadata value and
//! the subkey rows.
//!
//! v1 metadata value (strings): `flags(1B) | expire_ms(8B BE) | payload…`
//! where flags = type (low 4 bits) + encoding variant (next 2 bits).
//! Complex types will extend this with version/size per the Kvrocks-style
//! design (docs/design.md §2.4); the flags byte already reserves the bits.

/// Column-family tag. v0 runs on a single physical keyspace, so the CF is
/// the first envelope byte; the real engine maps these to RocksDB column
/// families without changing any logic above the `Kv` seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cf {
    Metadata = b'M' as isize,
    Subkey = b'S' as isize,
    ZScore = b'Z' as isize,
}

/// Value type stored under a user key. Lives in the low 4 bits of flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    String = 0,
    Hash = 1,
    Set = 2,
    ZSet = 3,
    List = 4,
}

impl ValueType {
    pub fn from_flags(flags: u8) -> Option<Self> {
        match flags & 0x0F {
            0 => Some(Self::String),
            1 => Some(Self::Hash),
            2 => Some(Self::Set),
            3 => Some(Self::ZSet),
            4 => Some(Self::List),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Hash => "hash",
            Self::Set => "set",
            Self::ZSet => "zset",
            Self::List => "list",
        }
    }
}

/// Encoding variant within a type. Lives in bits 4–5 of flags. v1 is the
/// only variant today; the tag is what makes future variants (e.g. inlined
/// small collections) coexist per key with no format migration.
pub const ENCODING_V1: u8 = 0;

pub fn make_flags(t: ValueType, encoding: u8) -> u8 {
    (t as u8) | (encoding << 4)
}

pub fn encoding_of(flags: u8) -> u8 {
    (flags >> 4) & 0x03
}

/// Builds an envelope key: `cf | ns_len(1B) | ns | slot(2B BE) | user_key`.
///
/// The namespace length prefix keeps distinct namespaces from sharing key
/// prefixes; the big-endian slot keeps one namespace's slots contiguous and
/// ordered, so "all rows of slot N" is a prefix scan and "slots 0–8191" is
/// a bounded range.
pub fn envelope(cf: Cf, ns: &[u8], slot: u16, user_key: &[u8]) -> Vec<u8> {
    debug_assert!(ns.len() <= u8::MAX as usize, "namespace too long");
    let mut key = Vec::with_capacity(1 + 1 + ns.len() + 2 + user_key.len());
    key.push(cf as u8);
    key.push(ns.len() as u8);
    key.extend_from_slice(ns);
    key.extend_from_slice(&slot.to_be_bytes());
    key.extend_from_slice(user_key);
    key
}

/// The prefix covering every row of one slot in one CF — the unit of
/// migration copy and cleanup.
pub fn slot_prefix(cf: Cf, ns: &[u8], slot: u16) -> Vec<u8> {
    envelope(cf, ns, slot, b"")
}

/// A string's metadata row value.
#[derive(Debug, PartialEq, Eq)]
pub struct StringMeta {
    pub flags: u8,
    /// Absolute expiry in unix milliseconds; 0 = never.
    pub expire_ms: u64,
    pub payload: Vec<u8>,
}

impl StringMeta {
    pub fn new(payload: Vec<u8>, expire_ms: u64) -> Self {
        Self {
            flags: make_flags(ValueType::String, ENCODING_V1),
            expire_ms,
            payload,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(9 + self.payload.len());
        out.push(self.flags);
        out.extend_from_slice(&self.expire_ms.to_be_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(row: &[u8]) -> Option<Self> {
        if row.len() < 9 {
            return None;
        }
        Some(Self {
            flags: row[0],
            expire_ms: u64::from_be_bytes(row[1..9].try_into().ok()?),
            payload: row[9..].to_vec(),
        })
    }

    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.expire_ms != 0 && self.expire_ms <= now_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_layout_and_ordering() {
        let k = envelope(Cf::Metadata, b"ns1", 0x0102, b"user");
        assert_eq!(k[0], b'M');
        assert_eq!(k[1], 3);
        assert_eq!(&k[2..5], b"ns1");
        assert_eq!(&k[5..7], &[0x01, 0x02]);
        assert_eq!(&k[7..], b"user");

        // Slots order contiguously under one namespace.
        let a = envelope(Cf::Metadata, b"t", 5, b"zzz");
        let b = envelope(Cf::Metadata, b"t", 6, b"aaa");
        assert!(a < b, "slot 5 rows must sort before slot 6 rows");

        // Length prefix prevents cross-namespace prefix collisions:
        // ns "ab" + key "…" can never share a prefix with ns "a" + key "b…".
        let x = envelope(Cf::Metadata, b"a", 1, b"b-key");
        let y = envelope(Cf::Metadata, b"ab", 1, b"key");
        assert_ne!(x[..4], y[..4]);
    }

    #[test]
    fn slot_prefix_covers_exactly_that_slot() {
        let p = slot_prefix(Cf::Metadata, b"t", 7);
        assert!(envelope(Cf::Metadata, b"t", 7, b"anything").starts_with(&p));
        assert!(!envelope(Cf::Metadata, b"t", 8, b"anything").starts_with(&p));
        assert!(!envelope(Cf::Subkey, b"t", 7, b"anything").starts_with(&p));
    }

    #[test]
    fn string_meta_roundtrip() {
        let m = StringMeta::new(b"hello".to_vec(), 1234567890123);
        let row = m.encode();
        assert_eq!(StringMeta::decode(&row), Some(m));
        // Empty payload is legal (SET k "").
        let empty = StringMeta::new(Vec::new(), 0);
        assert_eq!(StringMeta::decode(&empty.encode()), Some(empty));
        // Truncated rows are rejected.
        assert_eq!(StringMeta::decode(&[0u8; 8]), None);
    }

    #[test]
    fn flags_pack_type_and_encoding() {
        let f = make_flags(ValueType::String, ENCODING_V1);
        assert_eq!(ValueType::from_flags(f), Some(ValueType::String));
        assert_eq!(encoding_of(f), ENCODING_V1);
        let f2 = make_flags(ValueType::Hash, 2);
        assert_eq!(ValueType::from_flags(f2), Some(ValueType::Hash));
        assert_eq!(encoding_of(f2), 2);
    }

    #[test]
    fn expiry_check() {
        let m = StringMeta::new(b"v".to_vec(), 1000);
        assert!(!m.is_expired(999));
        assert!(m.is_expired(1000));
        assert!(m.is_expired(1001));
        let never = StringMeta::new(b"v".to_vec(), 0);
        assert!(!never.is_expired(u64::MAX));
    }
}
