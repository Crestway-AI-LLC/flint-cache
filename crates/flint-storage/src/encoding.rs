// SPDX-License-Identifier: Elastic-2.0
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
    /// A JSON document, stored as its serialized bytes in the metadata row
    /// (strings-shaped: one row, no subkeys) — documents live beyond RAM
    /// like any value. Parsing/manipulation is the command layer's job;
    /// storage sees opaque bytes plus this tag.
    Json = 5,
}

impl ValueType {
    pub fn from_flags(flags: u8) -> Option<Self> {
        match flags & 0x0F {
            0 => Some(Self::String),
            1 => Some(Self::Hash),
            2 => Some(Self::Set),
            3 => Some(Self::ZSet),
            4 => Some(Self::List),
            5 => Some(Self::Json),
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
            Self::Json => "json",
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

/// The header every metadata row starts with, regardless of type:
/// `flags(1B) | expire_ms(8B BE)`. Generic keyspace ops (DEL, EXISTS, TYPE,
/// EXPIRE/TTL/PERSIST) parse only this header and work on any value type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaHeader {
    pub flags: u8,
    pub expire_ms: u64,
}

pub const HEADER_LEN: usize = 9;

impl MetaHeader {
    pub fn decode(row: &[u8]) -> Option<Self> {
        if row.len() < HEADER_LEN {
            return None;
        }
        Some(Self {
            flags: row[0],
            expire_ms: u64::from_be_bytes(row[1..9].try_into().ok()?),
        })
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.push(self.flags);
        out.extend_from_slice(&self.expire_ms.to_be_bytes());
    }

    pub fn write_expire(row: &mut [u8], expire_ms: u64) {
        row[1..9].copy_from_slice(&expire_ms.to_be_bytes());
    }

    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.expire_ms != 0 && self.expire_ms <= now_ms
    }

    pub fn value_type(&self) -> Option<ValueType> {
        ValueType::from_flags(self.flags)
    }
}

/// Metadata row value for complex types (hash/set/zset/list):
/// `header(9B) | version(8B BE) | size(4B BE) | bytes(8B BE)`.
///
/// `bytes` is the cumulative payload size of the collection (fields +
/// values / members / elements), maintained by every mutation. It exists
/// so the max-value-bytes policy can reject a write in O(1) — on a
/// beyond-RAM engine a collection can otherwise grow past physical memory
/// and any read-all command (HGETALL, SMEMBERS, LRANGE) becomes an OOM.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComplexMeta {
    pub header: MetaHeader,
    pub version: u64,
    pub size: u32,
    pub bytes: u64,
}

/// Byte length of the complex-meta payload after the common header.
const COMPLEX_TAIL_LEN: usize = 8 + 4 + 8;

impl ComplexMeta {
    pub fn new(t: ValueType, version: u64) -> Self {
        Self {
            header: MetaHeader {
                flags: make_flags(t, ENCODING_V1),
                expire_ms: 0,
            },
            version,
            size: 0,
            bytes: 0,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + COMPLEX_TAIL_LEN);
        self.header.encode_into(&mut out);
        out.extend_from_slice(&self.version.to_be_bytes());
        out.extend_from_slice(&self.size.to_be_bytes());
        out.extend_from_slice(&self.bytes.to_be_bytes());
        out
    }

    /// Rewrite the version of an ENCODED metadata row, leaving every other
    /// byte alone — including whatever a type appends past the common tail.
    ///
    /// This exists because `decode` then `encode` is NOT a round trip for
    /// every type: a list's metadata carries head/tail counters after the
    /// shared fields, and rebuilding the row from a `ComplexMeta` silently
    /// truncates them, leaving a row that no longer decodes as a list. COPY
    /// found this the honest way, by disagreeing with Valkey.
    pub fn write_version(row: &mut [u8], version: u64) {
        row[HEADER_LEN..HEADER_LEN + 8].copy_from_slice(&version.to_be_bytes());
    }

    pub fn decode(row: &[u8]) -> Option<Self> {
        let header = MetaHeader::decode(row)?;
        if row.len() < HEADER_LEN + COMPLEX_TAIL_LEN {
            return None;
        }
        Some(Self {
            header,
            version: u64::from_be_bytes(row[9..17].try_into().ok()?),
            size: u32::from_be_bytes(row[17..21].try_into().ok()?),
            bytes: u64::from_be_bytes(row[21..29].try_into().ok()?),
        })
    }
}

/// Subkey row key:
/// `S | ns_len | ns | slot(BE) | key_len(2B BE) | user_key | version(8B BE) | field`.
///
/// The key-length framing is required here (unlike the metadata envelope,
/// where user_key is the suffix): without it, `(key="ab", field="c")` and
/// `(key="a", field="bc")` would collide.
pub fn subkey_envelope(
    ns: &[u8],
    slot: u16,
    user_key: &[u8],
    version: u64,
    field: &[u8],
) -> Vec<u8> {
    let mut k = subkey_prefix(ns, slot, user_key, version);
    k.extend_from_slice(field);
    k
}

/// Prefix covering every subkey row of one (key, version) — the unit of a
/// full-collection scan (HGETALL) and of orphan identification.
pub fn subkey_prefix(ns: &[u8], slot: u16, user_key: &[u8], version: u64) -> Vec<u8> {
    // Hard assert, not debug: in release a longer key would silently
    // truncate the length frame (`as u16`) and write a corrupted envelope
    // that misparses in GC and can collide across keys. Dispatch rejects
    // oversized keys before reaching here; this is the last line.
    assert!(
        user_key.len() <= u16::MAX as usize,
        "user key exceeds the envelope's u16 length frame"
    );
    let mut k = Vec::with_capacity(1 + 1 + ns.len() + 2 + 2 + user_key.len() + 8);
    k.push(Cf::Subkey as u8);
    k.push(ns.len() as u8);
    k.extend_from_slice(ns);
    k.extend_from_slice(&slot.to_be_bytes());
    k.extend_from_slice(&(user_key.len() as u16).to_be_bytes());
    k.extend_from_slice(user_key);
    k.extend_from_slice(&version.to_be_bytes());
    k
}

/// Monotonic version numbers: unique per key incarnation, process-wide,
/// seeded from the clock so versions keep increasing across restarts.
pub struct VersionGen;

impl VersionGen {
    pub fn next(now_ms: u64) -> u64 {
        use std::sync::atomic::{AtomicU64, Ordering};
        static LAST: AtomicU64 = AtomicU64::new(0);
        let floor = now_ms << 20;
        let prev = LAST
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |prev| {
                Some(prev.max(floor) + 1)
            })
            .unwrap_or(floor);
        prev.max(floor) + 1
    }
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

    /// Same one-row shape, tagged as another payload-in-metadata type —
    /// JSON documents store their serialized bytes exactly like a string
    /// (one row, no subkeys), so they reuse this layout and differ only in
    /// the type tag. Keeps the encoding layer honest: one payload row
    /// format, several types wearing it.
    pub fn new_typed(t: ValueType, payload: Vec<u8>, expire_ms: u64) -> Self {
        Self {
            flags: make_flags(t, ENCODING_V1),
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

#[cfg(test)]
mod complex_tests {
    use super::*;

    #[test]
    fn complex_meta_roundtrip() {
        let mut m = ComplexMeta::new(ValueType::Hash, 42);
        m.size = 7;
        m.bytes = 123_456_789_012; // past u32: byte totals can exceed 4GB
        m.header.expire_ms = 123456;
        assert_eq!(ComplexMeta::decode(&m.encode()), Some(m));
        assert_eq!(ComplexMeta::decode(&[0u8; 20]), None);
        assert_eq!(ComplexMeta::decode(&[0u8; 28]), None, "truncated tail");
    }

    #[test]
    fn header_is_common_prefix_of_both_layouts() {
        let s = StringMeta::new(b"v".to_vec(), 99);
        let c = ComplexMeta::new(ValueType::Hash, 1);
        let hs = MetaHeader::decode(&s.encode()).expect("string header");
        let hc = MetaHeader::decode(&c.encode()).expect("complex header");
        assert_eq!(hs.expire_ms, 99);
        assert_eq!(hs.value_type(), Some(ValueType::String));
        assert_eq!(hc.value_type(), Some(ValueType::Hash));
    }

    #[test]
    fn subkey_framing_prevents_collisions() {
        // (key="ab", field="c") vs (key="a", field="bc") must differ.
        let a = subkey_envelope(b"t", 1, b"ab", 5, b"c");
        let b = subkey_envelope(b"t", 1, b"a", 5, b"bc");
        assert_ne!(a, b);
        // Same (key, version): field ordering under the prefix.
        let p = subkey_prefix(b"t", 1, b"ab", 5);
        assert!(a.starts_with(&p));
        assert!(!b.starts_with(&p));
        // Different version, same key: disjoint prefixes (orphan isolation).
        let p2 = subkey_prefix(b"t", 1, b"ab", 6);
        assert!(!a.starts_with(&p2));
    }

    /// The last line of defense behind dispatch's key cap: building an
    /// envelope for a key the u16 length frame cannot represent must
    /// panic, never silently truncate (truncation corrupts the row and
    /// can collide across keys).
    #[test]
    #[should_panic(expected = "u16 length frame")]
    fn subkey_prefix_refuses_unframeable_keys() {
        let _ = subkey_prefix(b"t", 1, &vec![b'k'; 65_536], 1);
    }

    #[test]
    #[should_panic(expected = "u16 length frame")]
    fn zscore_prefix_refuses_unframeable_keys() {
        let _ = zscore_prefix(b"t", 1, &vec![b'k'; 65_536], 1);
    }

    #[test]
    fn versions_are_monotonic_and_unique() {
        let a = VersionGen::next(1_000);
        let b = VersionGen::next(1_000);
        let c = VersionGen::next(999);
        assert!(b > a);
        assert!(c > b, "version must grow even if clock goes backward");
    }
}

/// List metadata: `header | version | size | head(8B BE, biased) | tail`.
/// Elements live at indices head..tail (exclusive tail); size = tail - head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListMeta {
    pub base: ComplexMeta,
    pub head: i64,
    pub tail: i64,
}

impl ListMeta {
    pub fn new(version: u64) -> Self {
        Self {
            base: ComplexMeta::new(ValueType::List, version),
            head: 0,
            tail: 0,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.base.encode();
        out.extend_from_slice(&bias_index(self.head).to_be_bytes());
        out.extend_from_slice(&bias_index(self.tail).to_be_bytes());
        out
    }

    pub fn decode(row: &[u8]) -> Option<Self> {
        let base = ComplexMeta::decode(row)?;
        if row.len() < HEADER_LEN + COMPLEX_TAIL_LEN + 16 {
            return None;
        }
        let off = HEADER_LEN + COMPLEX_TAIL_LEN;
        Some(Self {
            base,
            head: unbias_index(u64::from_be_bytes(row[off..off + 8].try_into().ok()?)),
            tail: unbias_index(u64::from_be_bytes(row[off + 8..off + 16].try_into().ok()?)),
        })
    }
}

/// Bias a signed list index so the unsigned BE bytes sort in index order.
pub fn bias_index(i: i64) -> u64 {
    (i as u64) ^ (1u64 << 63)
}

pub fn unbias_index(b: u64) -> i64 {
    (b ^ (1u64 << 63)) as i64
}

/// Order-preserving f64 encoding: the BE bytes of the result sort exactly
/// like the doubles (including negatives). Standard sign-flip trick.
pub fn encode_score(s: f64) -> u64 {
    let bits = s.to_bits();
    if bits & (1u64 << 63) != 0 {
        !bits
    } else {
        bits | (1u64 << 63)
    }
}

pub fn decode_score(e: u64) -> f64 {
    let bits = if e & (1u64 << 63) != 0 {
        e & !(1u64 << 63)
    } else {
        !e
    };
    f64::from_bits(bits)
}

/// Score-index row key in the ZScore CF:
/// `Z | ns_len | ns | slot | key_len | user_key | version | score(8B) | member`.
pub fn zscore_envelope(
    ns: &[u8],
    slot: u16,
    user_key: &[u8],
    version: u64,
    score: f64,
    member: &[u8],
) -> Vec<u8> {
    let mut k = zscore_prefix(ns, slot, user_key, version);
    k.extend_from_slice(&encode_score(score).to_be_bytes());
    k.extend_from_slice(member);
    k
}

pub fn zscore_prefix(ns: &[u8], slot: u16, user_key: &[u8], version: u64) -> Vec<u8> {
    // Same u16 length frame, same hard guard as `subkey_prefix`.
    assert!(
        user_key.len() <= u16::MAX as usize,
        "user key exceeds the envelope's u16 length frame"
    );
    let mut k = Vec::with_capacity(1 + 1 + ns.len() + 2 + 2 + user_key.len() + 8);
    k.push(Cf::ZScore as u8);
    k.push(ns.len() as u8);
    k.extend_from_slice(ns);
    k.extend_from_slice(&slot.to_be_bytes());
    k.extend_from_slice(&(user_key.len() as u16).to_be_bytes());
    k.extend_from_slice(user_key);
    k.extend_from_slice(&version.to_be_bytes());
    k
}

#[cfg(test)]
mod order_tests {
    use super::*;

    #[test]
    fn list_meta_roundtrip_and_bias_order() {
        let mut m = ListMeta::new(9);
        m.head = -3;
        m.tail = 4;
        assert_eq!(ListMeta::decode(&m.encode()), Some(m));
        let idx = [-5i64, -1, 0, 1, 7];
        let enc: Vec<u64> = idx.iter().map(|&i| bias_index(i)).collect();
        let mut sorted = enc.clone();
        sorted.sort();
        assert_eq!(enc, sorted, "biased indices must sort in index order");
        assert_eq!(unbias_index(bias_index(-42)), -42);
    }

    #[test]
    fn score_encoding_orders_like_f64() {
        let scores = [-1000.5, -1.0, -0.0, 0.0, 0.25, 1.0, 99999.0];
        let enc: Vec<u64> = scores.iter().map(|&s| encode_score(s)).collect();
        let mut sorted = enc.clone();
        sorted.sort();
        assert_eq!(enc, sorted, "encoded scores must sort like doubles");
        assert_eq!(decode_score(encode_score(2.5)), 2.5);
        assert_eq!(decode_score(encode_score(-7.25)), -7.25);
    }
}

/// Parse a subkey/zscore row key back into (ns, slot, user_key, version).
/// Returns None on malformed keys (wrong tag, truncated).
pub fn parse_subkey_envelope(k: &[u8]) -> Option<(&[u8], u16, &[u8], u64)> {
    if k.len() < 2 || (k[0] != Cf::Subkey as u8 && k[0] != Cf::ZScore as u8) {
        return None;
    }
    let ns_len = k[1] as usize;
    let mut off = 2;
    let ns = k.get(off..off + ns_len)?;
    off += ns_len;
    let slot = u16::from_be_bytes(k.get(off..off + 2)?.try_into().ok()?);
    off += 2;
    let key_len = u16::from_be_bytes(k.get(off..off + 2)?.try_into().ok()?) as usize;
    off += 2;
    let user_key = k.get(off..off + key_len)?;
    off += key_len;
    let version = u64::from_be_bytes(k.get(off..off + 8)?.try_into().ok()?);
    Some((ns, slot, user_key, version))
}

#[cfg(test)]
mod parse_tests {
    use super::*;

    #[test]
    fn subkey_envelope_roundtrips_through_parse() {
        let k = subkey_envelope(b"ns9", 1234, b"user-key", 77, b"field");
        let (ns, slot, key, version) = parse_subkey_envelope(&k).expect("parse");
        assert_eq!(
            (ns, slot, key, version),
            (&b"ns9"[..], 1234, &b"user-key"[..], 77)
        );
        let z = zscore_envelope(b"n", 7, b"k", 5, 1.5, b"m");
        let (ns, slot, key, version) = parse_subkey_envelope(&z).expect("parse z");
        assert_eq!((ns, slot, key, version), (&b"n"[..], 7, &b"k"[..], 5));
        assert_eq!(parse_subkey_envelope(b"Mxx"), None);
        assert_eq!(parse_subkey_envelope(&k[..6]), None);
    }
}
