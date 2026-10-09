// SPDX-License-Identifier: Elastic-2.0
//! StreamStore: Redis streams (ADR-0052 D6).
//!
//! The metadata row is a [`StreamMeta`]. Each entry is a subkey row whose
//! field is [`STREAM_ENTRY_TAG`] and then its ID in big-endian, so a prefix
//! scan walks the entries in ID order and a range is a scan between two
//! IDs. An entry's value is its fields and values, each length-prefixed.
//!
//! Unlike an emptied list, an emptied stream keeps its key, as Redis's does:
//! its last ID still bounds the next one.

use crate::Kv;
use crate::encoding::{
    Cf, MetaHeader, STREAM_ENTRY_TAG, StreamId, StreamMeta, ValueType, VersionGen, envelope,
    subkey_envelope, subkey_prefix,
};
use crate::strings::{Clock, StoreError};

/// How XADD names its entry's ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdSpec {
    /// `*`: the clock's milliseconds, or the next ID after the last when
    /// the clock is behind it.
    Auto,
    /// `ms-*`: these milliseconds, and the next sequence number in them.
    AutoSeq(u64),
    /// `ms-seq`, or `ms` for `ms-0`.
    Explicit(StreamId),
}

/// What a trim keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrimTo {
    /// `MAXLEN n`: the newest `n` entries.
    MaxLen(u64),
    /// `MINID id`: the entries at or above `id`.
    MinId(StreamId),
}

/// A trim, and how many entries it may remove in one call (`LIMIT`, which
/// Redis takes only with `~`); `None` removes as many as it takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trim {
    pub to: TrimTo,
    pub limit: Option<u64>,
}

/// An entry: its ID and its fields and values, in order, flat.
pub type Entry = (StreamId, Vec<Vec<u8>>);

pub struct StreamStore<'a> {
    kv: &'a dyn Kv,
    ns: Vec<u8>,
    clock: Clock,
    max_value_bytes: u64,
}

impl<'a> StreamStore<'a> {
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

    fn entry_key(&self, slot: u16, key: &[u8], version: u64, id: StreamId) -> Vec<u8> {
        let mut field = Vec::with_capacity(17);
        field.push(STREAM_ENTRY_TAG);
        field.extend_from_slice(&id.to_bytes());
        subkey_envelope(&self.ns, slot, key, version, &field)
    }

    fn entries_prefix(&self, slot: u16, key: &[u8], version: u64) -> Vec<u8> {
        let mut p = subkey_prefix(&self.ns, slot, key, version);
        p.push(STREAM_ENTRY_TAG);
        p
    }

    /// The stream at `key`, if there is one; another type is WRONGTYPE.
    pub fn read_meta(&self, slot: u16, key: &[u8]) -> Result<Option<StreamMeta>, StoreError> {
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
        if header.value_type() != Some(ValueType::Stream) {
            return Err(StoreError::WrongType);
        }
        StreamMeta::decode(&row)
            .ok_or(StoreError::WrongType)
            .map(Some)
    }

    fn write_meta(&self, slot: u16, key: &[u8], meta: &StreamMeta) {
        let mut m = *meta;
        m.base.touch((self.clock)());
        self.kv.put(&self.meta_key(slot, key), &m.encode());
    }

    /// The ID an XADD gets, or why it gets none.
    fn next_id(&self, meta: &StreamMeta, spec: IdSpec) -> Result<StreamId, StoreError> {
        let last = meta.last_id;
        if last == StreamId::MAX {
            return Err(StoreError::StreamExhausted);
        }
        let id = match spec {
            IdSpec::Auto => {
                let now = (self.clock)();
                if now > last.ms {
                    StreamId { ms: now, seq: 0 }
                } else {
                    last.next().ok_or(StoreError::StreamExhausted)?
                }
            }
            IdSpec::AutoSeq(ms) if ms == last.ms => StreamId {
                ms,
                seq: last
                    .seq
                    .checked_add(1)
                    .ok_or(StoreError::StreamIdTooSmall)?,
            },
            IdSpec::AutoSeq(ms) => StreamId { ms, seq: 0 },
            IdSpec::Explicit(id) => id,
        };
        if id <= last {
            return Err(StoreError::StreamIdTooSmall);
        }
        Ok(id)
    }

    /// XADD: the new entry's ID, or `None` for `NOMKSTREAM` on a missing
    /// key. `fields` holds fields and values, flat. The ID is checked, and
    /// the size cap, before anything is written.
    pub fn add(
        &self,
        slot: u16,
        key: &[u8],
        spec: IdSpec,
        fields: &[Vec<u8>],
        nomkstream: bool,
        trim: Option<Trim>,
    ) -> Result<Option<StreamId>, StoreError> {
        let mut meta = match self.read_meta(slot, key)? {
            Some(m) => m,
            None if nomkstream => return Ok(None),
            None => StreamMeta::new(VersionGen::next((self.clock)())),
        };
        let id = self.next_id(&meta, spec)?;
        let added: u64 = fields.iter().map(|f| f.len() as u64).sum();
        if meta.base.bytes + added > self.max_value_bytes {
            return Err(StoreError::ValueTooLarge);
        }
        self.kv.put(
            &self.entry_key(slot, key, meta.base.version, id),
            &encode_fields(fields),
        );
        meta.base.size += 1;
        meta.base.bytes += added;
        meta.last_id = id;
        meta.entries_added += 1;
        if let Some(t) = trim {
            self.trim_meta(slot, key, &mut meta, t);
        }
        self.write_meta(slot, key, &meta);
        Ok(Some(id))
    }

    /// XLEN.
    pub fn len(&self, slot: u16, key: &[u8]) -> Result<u64, StoreError> {
        Ok(self
            .read_meta(slot, key)?
            .map_or(0, |m| u64::from(m.base.size)))
    }

    /// XRANGE (`rev` false) and XREVRANGE: the entries from `lo` to `hi`,
    /// both inclusive, at most `count` of them, from the low end or, with
    /// `rev`, from the high end.
    pub fn range(
        &self,
        slot: u16,
        key: &[u8],
        lo: StreamId,
        hi: StreamId,
        count: Option<usize>,
        rev: bool,
    ) -> Result<Vec<Entry>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(Vec::new());
        };
        Ok(self.scan(slot, key, &meta, lo, hi, count, rev))
    }

    #[allow(clippy::too_many_arguments)]
    fn scan(
        &self,
        slot: u16,
        key: &[u8],
        meta: &StreamMeta,
        lo: StreamId,
        hi: StreamId,
        count: Option<usize>,
        rev: bool,
    ) -> Vec<Entry> {
        let mut out = Vec::new();
        if lo > hi || count == Some(0) {
            return out;
        }
        let version = meta.base.version;
        let prefix = self.entries_prefix(slot, key, version);
        let want = count.unwrap_or(usize::MAX);
        let mut visit = |k: &[u8], v: &[u8]| -> bool {
            let Some(id) = StreamId::from_bytes(&k[prefix.len()..]) else {
                return true;
            };
            if id < lo || id > hi {
                // Past the far bound in the scan's direction: done. Short
                // of the near one: keep going (only at a scan's first row).
                return if rev { id > hi } else { id < lo };
            }
            out.push((id, decode_fields(v)));
            out.len() < want
        };
        if rev {
            let before = hi
                .next()
                .map(|n| self.entry_key(slot, key, version, n))
                .unwrap_or_default();
            self.kv.for_each_before(&prefix, &before, &mut visit);
        } else {
            let after = lo
                .prev()
                .map(|p| self.entry_key(slot, key, version, p))
                .unwrap_or_default();
            self.kv.for_each_from(&prefix, &after, &mut visit);
        }
        out
    }

    /// The newest entry, for XREAD's `+`.
    pub fn last_entry(&self, slot: u16, key: &[u8]) -> Result<Option<Entry>, StoreError> {
        Ok(self
            .range(slot, key, StreamId::MIN, StreamId::MAX, Some(1), true)?
            .pop())
    }

    /// The last ID the stream has given, for XREAD's `$`; `None` when there
    /// is no stream.
    pub fn last_id(&self, slot: u16, key: &[u8]) -> Result<Option<StreamId>, StoreError> {
        Ok(self.read_meta(slot, key)?.map(|m| m.last_id))
    }

    /// XDEL: how many of `ids` were there.
    pub fn del(&self, slot: u16, key: &[u8], ids: &[StreamId]) -> Result<u64, StoreError> {
        let Some(mut meta) = self.read_meta(slot, key)? else {
            return Ok(0);
        };
        let mut gone = 0u64;
        for &id in ids {
            let ek = self.entry_key(slot, key, meta.base.version, id);
            let Some(v) = self.kv.get(&ek) else {
                continue;
            };
            self.kv.delete(&ek);
            meta.base.size -= 1;
            meta.base.bytes = meta.base.bytes.saturating_sub(payload_bytes(&v));
            meta.max_deleted_id = meta.max_deleted_id.max(id);
            gone += 1;
        }
        if gone > 0 {
            self.write_meta(slot, key, &meta);
        }
        Ok(gone)
    }

    /// XTRIM: how many entries went.
    pub fn trim(&self, slot: u16, key: &[u8], trim: Trim) -> Result<u64, StoreError> {
        let Some(mut meta) = self.read_meta(slot, key)? else {
            return Ok(0);
        };
        let gone = self.trim_meta(slot, key, &mut meta, trim);
        if gone > 0 {
            self.write_meta(slot, key, &meta);
        }
        Ok(gone)
    }

    /// Remove the oldest entries `trim` asks for, updating `meta` but not
    /// writing it.
    fn trim_meta(&self, slot: u16, key: &[u8], meta: &mut StreamMeta, trim: Trim) -> u64 {
        let budget = trim.limit.unwrap_or(u64::MAX);
        let over = match trim.to {
            TrimTo::MaxLen(n) => u64::from(meta.base.size).saturating_sub(n),
            TrimTo::MinId(_) => u64::from(meta.base.size),
        };
        let take = over.min(budget);
        if take == 0 {
            return 0;
        }
        let prefix = self.entries_prefix(slot, key, meta.base.version);
        let mut doomed: Vec<(Vec<u8>, u64)> = Vec::new();
        self.kv.for_each_from(&prefix, &[], &mut |k, v| {
            if let TrimTo::MinId(min) = trim.to {
                match StreamId::from_bytes(&k[prefix.len()..]) {
                    Some(id) if id < min => {}
                    _ => return false,
                }
            }
            doomed.push((k.to_vec(), payload_bytes(v)));
            (doomed.len() as u64) < take
        });
        for (k, bytes) in &doomed {
            self.kv.delete(k);
            meta.base.size -= 1;
            meta.base.bytes = meta.base.bytes.saturating_sub(*bytes);
        }
        doomed.len() as u64
    }
}

/// `pairs(4B BE)` then each field and value as `len(4B BE) | bytes`.
fn encode_fields(fields: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + fields.iter().map(|f| 4 + f.len()).sum::<usize>());
    out.extend_from_slice(&((fields.len() / 2) as u32).to_be_bytes());
    for f in fields {
        out.extend_from_slice(&(f.len() as u32).to_be_bytes());
        out.extend_from_slice(f);
    }
    out
}

fn decode_fields(v: &[u8]) -> Vec<Vec<u8>> {
    let Some(pairs) = v
        .get(..4)
        .and_then(|b| b.try_into().ok())
        .map(u32::from_be_bytes)
    else {
        return Vec::new();
    };
    let mut out = Vec::with_capacity(pairs as usize * 2);
    let mut at = 4usize;
    for _ in 0..pairs as usize * 2 {
        let Some(len) = v
            .get(at..at + 4)
            .and_then(|b| b.try_into().ok())
            .map(u32::from_be_bytes)
        else {
            break;
        };
        at += 4;
        let Some(f) = v.get(at..at + len as usize) else {
            break;
        };
        out.push(f.to_vec());
        at += len as usize;
    }
    out
}

/// What an entry's row counts toward the stream's `bytes`: its fields and
/// values, without the framing.
fn payload_bytes(v: &[u8]) -> u64 {
    let pairs = v
        .get(..4)
        .and_then(|b| b.try_into().ok())
        .map_or(0, u32::from_be_bytes);
    (v.len() as u64).saturating_sub(4 + 8 * u64::from(pairs))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NOW: AtomicU64 = AtomicU64::new(1_000);
    fn clock() -> u64 {
        NOW.load(Ordering::Relaxed)
    }

    fn id(ms: u64, seq: u64) -> StreamId {
        StreamId { ms, seq }
    }

    fn f(pairs: &[&str]) -> Vec<Vec<u8>> {
        pairs.iter().map(|p| p.as_bytes().to_vec()).collect()
    }

    /// Valkey 9.1's ID rules, case by case from its replies.
    #[test]
    fn ids_follow_valkey() {
        let kv = MemKv::new();
        let s = StreamStore::new(&kv, b"t", clock);
        let add = |spec| s.add(0, b"s", spec, &f(&["f", "v"]), false, None);
        assert_eq!(add(IdSpec::Explicit(id(1, 1))), Ok(Some(id(1, 1))));
        assert_eq!(
            add(IdSpec::Explicit(id(1, 1))),
            Err(StoreError::StreamIdTooSmall)
        );
        assert_eq!(
            add(IdSpec::Explicit(id(1, 0))),
            Err(StoreError::StreamIdTooSmall)
        );
        assert_eq!(add(IdSpec::AutoSeq(1)), Ok(Some(id(1, 2))));
        assert_eq!(add(IdSpec::Explicit(id(2, 5))), Ok(Some(id(2, 5))));
        assert_eq!(
            add(IdSpec::Explicit(id(2, 0))),
            Err(StoreError::StreamIdTooSmall)
        );
        // The clock is at 1000 ms, past the last ID: the clock's.
        assert_eq!(add(IdSpec::Auto), Ok(Some(id(1_000, 0))));
        assert_eq!(add(IdSpec::Auto), Ok(Some(id(1_000, 1))));
        // An ID ahead of the clock: `*` follows the last ID, not the clock.
        assert_eq!(add(IdSpec::Explicit(id(5_000, 7))), Ok(Some(id(5_000, 7))));
        assert_eq!(add(IdSpec::Auto), Ok(Some(id(5_000, 8))));
        assert_eq!(add(IdSpec::AutoSeq(6_000)), Ok(Some(id(6_000, 0))));
        assert_eq!(
            add(IdSpec::AutoSeq(5_999)),
            Err(StoreError::StreamIdTooSmall)
        );
        assert_eq!(
            add(IdSpec::Explicit(StreamId::MAX)),
            Ok(Some(StreamId::MAX))
        );
        assert_eq!(add(IdSpec::Auto), Err(StoreError::StreamExhausted));
        assert_eq!(s.len(0, b"s"), Ok(9));
        // 0-* on a new stream is 0-1: 0-0 is no entry's ID.
        let t = StreamStore::new(&kv, b"t", clock);
        assert_eq!(
            t.add(0, b"n", IdSpec::AutoSeq(0), &f(&["a", "b"]), false, None),
            Ok(Some(id(0, 1)))
        );
        assert_eq!(
            t.add(0, b"m", IdSpec::Auto, &f(&["a", "b"]), true, None),
            Ok(None),
            "NOMKSTREAM leaves a missing key missing"
        );
        assert_eq!(t.len(0, b"m"), Ok(0));
    }

    #[test]
    fn ranges_deletes_and_trims() {
        let kv = MemKv::new();
        let s = StreamStore::new(&kv, b"t", clock);
        for ms in 1..=10u64 {
            s.add(
                0,
                b"s",
                IdSpec::Explicit(id(ms, 0)),
                &f(&["n", &ms.to_string()]),
                false,
                None,
            )
            .expect("a stream test step");
        }
        let ids = |v: Vec<Entry>| v.into_iter().map(|e| e.0.ms).collect::<Vec<_>>();
        assert_eq!(
            ids(s
                .range(0, b"s", id(3, 0), id(5, 0), None, false)
                .expect("a stream test step")),
            [3, 4, 5]
        );
        assert_eq!(
            ids(s
                .range(0, b"s", id(3, 1), id(5, 0), None, false)
                .expect("a stream test step")),
            [4, 5]
        );
        assert_eq!(
            ids(s
                .range(0, b"s", StreamId::MIN, StreamId::MAX, Some(2), false)
                .expect("a stream test step")),
            [1, 2]
        );
        assert_eq!(
            ids(s
                .range(0, b"s", StreamId::MIN, StreamId::MAX, Some(2), true)
                .expect("a stream test step")),
            [10, 9]
        );
        assert_eq!(
            ids(s
                .range(0, b"s", id(2, 0), id(4, 5), None, true)
                .expect("a stream test step")),
            [4, 3, 2]
        );
        assert!(
            s.range(0, b"s", id(5, 0), id(3, 0), None, false)
                .expect("a stream test step")
                .is_empty()
        );
        assert_eq!(
            s.range(0, b"s", id(7, 0), id(7, 0), None, false)
                .expect("a stream test step"),
            vec![(id(7, 0), f(&["n", "7"]))]
        );
        assert_eq!(s.del(0, b"s", &[id(2, 0), id(99, 0), id(2, 0)]), Ok(1));
        assert_eq!(s.len(0, b"s"), Ok(9));
        assert_eq!(
            s.trim(
                0,
                b"s",
                Trim {
                    to: TrimTo::MaxLen(5),
                    limit: None
                }
            ),
            Ok(4)
        );
        assert_eq!(
            ids(s
                .range(0, b"s", StreamId::MIN, StreamId::MAX, None, false)
                .expect("a stream test step")),
            [6, 7, 8, 9, 10]
        );
        assert_eq!(
            s.trim(
                0,
                b"s",
                Trim {
                    to: TrimTo::MinId(id(8, 0)),
                    limit: Some(1)
                }
            ),
            Ok(1)
        );
        assert_eq!(
            s.trim(
                0,
                b"s",
                Trim {
                    to: TrimTo::MinId(id(8, 0)),
                    limit: None
                }
            ),
            Ok(1)
        );
        assert_eq!(s.len(0, b"s"), Ok(3));
        // Emptied, the key stays, and its last ID still bounds the next.
        assert_eq!(
            s.trim(
                0,
                b"s",
                Trim {
                    to: TrimTo::MaxLen(0),
                    limit: None
                }
            ),
            Ok(3)
        );
        assert_eq!(s.len(0, b"s"), Ok(0));
        assert!(s.read_meta(0, b"s").expect("a stream test step").is_some());
        assert_eq!(
            s.add(
                0,
                b"s",
                IdSpec::Explicit(id(10, 0)),
                &f(&["a", "b"]),
                false,
                None
            ),
            Err(StoreError::StreamIdTooSmall)
        );
        let m = s
            .read_meta(0, b"s")
            .expect("a stream test step")
            .expect("a stream test step");
        assert_eq!(
            (m.last_id, m.max_deleted_id, m.entries_added, m.base.bytes),
            (id(10, 0), id(2, 0), 10, 0)
        );
    }

    #[test]
    fn xadd_trims_and_counts_bytes_and_refuses_other_types() {
        let kv = MemKv::new();
        let s = StreamStore::with_max_value_bytes(&kv, b"t", clock, 10);
        let t = Some(Trim {
            to: TrimTo::MaxLen(2),
            limit: None,
        });
        for ms in 1..=4u64 {
            s.add(
                0,
                b"s",
                IdSpec::Explicit(id(ms, 0)),
                &f(&["a", "b"]),
                false,
                t,
            )
            .expect("a stream test step");
        }
        let m = s
            .read_meta(0, b"s")
            .expect("a stream test step")
            .expect("a stream test step");
        assert_eq!((m.base.size, m.base.bytes), (2, 4));
        assert_eq!(
            s.add(0, b"s", IdSpec::Auto, &f(&["abcd", "efgh"]), false, None),
            Err(StoreError::ValueTooLarge),
            "4 + 8 bytes is past the 10-byte cap"
        );
        assert_eq!(s.len(0, b"s"), Ok(2), "a refused add writes nothing");
        crate::strings::StringStore::new(&kv, b"t", clock)
            .set(0, b"str", b"v", Default::default())
            .expect("a stream test step");
        assert_eq!(s.len(0, b"str"), Err(StoreError::WrongType));
        assert_eq!(
            s.add(0, b"str", IdSpec::Auto, &f(&["a", "b"]), false, None),
            Err(StoreError::WrongType)
        );
    }

    #[test]
    fn fields_round_trip_binary_and_empty() {
        let fields = vec![
            b"".to_vec(),
            b"\x00\r\n".to_vec(),
            b"k".to_vec(),
            vec![0xff; 300],
        ];
        let row = encode_fields(&fields);
        assert_eq!(decode_fields(&row), fields);
        assert_eq!(payload_bytes(&row), 304);
    }
}
