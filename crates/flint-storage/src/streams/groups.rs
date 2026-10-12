// SPDX-License-Identifier: Elastic-2.0
//! Consumer groups (ADR-0057): a row per group, per pending entry and per
//! consumer, under the stream's version beside its entries.
//!
//! | tag | row | field after the tag | value |
//! |---|---|---|---|
//! | 1 | group | name | last delivered ID, entries read, pending, consumers |
//! | 2 | pending entry | group, ID | delivery time, delivery count, consumer |
//! | 3 | consumer | group, name | seen time, active time, pending |
//! | 4 | a consumer's own pending entry | group, consumer, ID | empty |
//!
//! A name that more of the field follows is length-prefixed (4 bytes, big
//! endian), so group `a` never prefixes group `ab`'s rows. A name that ends
//! the field is not, so a stream's groups and a group's consumers list in
//! name order, as Valkey lists them; pending entries list in ID order.
//!
//! These are subkey rows of the stream's version, so every generic path
//! carries or removes them with no code here: DEL, expiry, GC, slot
//! migration, backup, COPY and RENAME. No group operation writes the
//! metadata row, so none is a change WATCH sees, as Valkey signals none;
//! `XGROUP CREATE … MKSTREAM` creates a stream, which is one.

use super::{StreamStore, decode_fields};
use crate::encoding::{
    STREAM_CONSUMER_TAG, STREAM_GROUP_TAG, STREAM_OWNED_TAG, STREAM_PENDING_TAG, StreamId,
    StreamMeta, VersionGen, subkey_envelope, subkey_prefix,
};
use crate::strings::StoreError;
use std::collections::BTreeMap;

/// A group's `entries_read` when it is not known (Valkey's
/// `SCG_INVALID_ENTRIES_READ`).
pub const ENTRIES_READ_UNKNOWN: i64 = -1;

/// A consumer group's row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Group {
    /// The last ID delivered to the group: `>` reads what follows it.
    pub last_id: StreamId,
    /// How many of the stream's entries the group has read, or
    /// [`ENTRIES_READ_UNKNOWN`].
    pub entries_read: i64,
    /// Its pending entries.
    pub pending: u64,
    /// Its consumers.
    pub consumers: u64,
}

impl Group {
    fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(40);
        v.extend_from_slice(&self.last_id.to_bytes());
        v.extend_from_slice(&self.entries_read.to_be_bytes());
        v.extend_from_slice(&self.pending.to_be_bytes());
        v.extend_from_slice(&self.consumers.to_be_bytes());
        v
    }

    fn decode(v: &[u8]) -> Option<Self> {
        Some(Self {
            last_id: StreamId::from_bytes(v.get(..16)?)?,
            entries_read: i64::from_be_bytes(v.get(16..24)?.try_into().ok()?),
            pending: u64::from_be_bytes(v.get(24..32)?.try_into().ok()?),
            consumers: u64::from_be_bytes(v.get(32..40)?.try_into().ok()?),
        })
    }
}

/// A consumer, as XINFO CONSUMERS reports one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Consumer {
    pub name: Vec<u8>,
    /// When it last tried to read, in unix ms.
    pub seen_ms: u64,
    /// When it last was delivered an entry, in unix ms; `None` if never.
    pub active_ms: Option<u64>,
    /// Its pending entries.
    pub pending: u64,
}

impl Consumer {
    fn new(name: &[u8], now: u64) -> Self {
        Self {
            name: name.to_vec(),
            seen_ms: now,
            active_ms: None,
            pending: 0,
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(24);
        v.extend_from_slice(&self.seen_ms.to_be_bytes());
        v.extend_from_slice(&self.active_ms.unwrap_or(u64::MAX).to_be_bytes());
        v.extend_from_slice(&self.pending.to_be_bytes());
        v
    }

    fn decode(name: &[u8], v: &[u8]) -> Option<Self> {
        let active = u64::from_be_bytes(v.get(8..16)?.try_into().ok()?);
        Some(Self {
            name: name.to_vec(),
            seen_ms: u64::from_be_bytes(v.get(..8)?.try_into().ok()?),
            active_ms: (active != u64::MAX).then_some(active),
            pending: u64::from_be_bytes(v.get(16..24)?.try_into().ok()?),
        })
    }
}

/// A pending entry: delivered to `consumer` and not acknowledged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pending {
    pub id: StreamId,
    pub consumer: Vec<u8>,
    /// When it was last delivered, in unix ms.
    pub delivered_ms: u64,
    pub deliveries: u64,
}

impl Pending {
    fn encode(&self) -> Vec<u8> {
        let mut v = Vec::with_capacity(16 + self.consumer.len());
        v.extend_from_slice(&self.delivered_ms.to_be_bytes());
        v.extend_from_slice(&self.deliveries.to_be_bytes());
        v.extend_from_slice(&self.consumer);
        v
    }

    fn decode(id: StreamId, v: &[u8]) -> Option<Self> {
        Some(Self {
            id,
            delivered_ms: u64::from_be_bytes(v.get(..8)?.try_into().ok()?),
            deliveries: u64::from_be_bytes(v.get(8..16)?.try_into().ok()?),
            consumer: v.get(16..)?.to_vec(),
        })
    }
}

/// Where XGROUP CREATE and SETID put a group: after the stream's last ID
/// (`$`), or after this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupStart {
    Last,
    Id(StreamId),
}

/// What XGROUP CREATE did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Created {
    Created,
    /// The group exists: BUSYGROUP.
    Exists,
    /// No stream, and no MKSTREAM to make one.
    NoStream,
}

/// A group operation's outcome: no stream, no such group, or the answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Found<T> {
    NoStream,
    NoGroup,
    Yes(T),
}

/// What XREADGROUP reads: entries never delivered to the group (`>`), or
/// the consumer's own pending entries after an ID (its history).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFrom {
    New,
    After(StreamId),
}

/// An entry XREADGROUP answers: its fields, or `None` for one deleted since
/// it was delivered, which only a history read meets.
pub type GroupEntry = (StreamId, Option<Vec<Vec<u8>>>);

/// XPENDING's summary of a group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSummary {
    pub count: u64,
    /// The smallest and largest pending IDs; `None` when nothing is pending.
    pub bounds: Option<(StreamId, StreamId)>,
    /// Each consumer with pending entries, and how many, in name order.
    pub consumers: Vec<(Vec<u8>, u64)>,
}

/// `tag | len(name) | name`: a name that more of the field follows.
fn named(tag: u8, name: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(5 + name.len() + 16);
    f.push(tag);
    f.extend_from_slice(&(name.len() as u32).to_be_bytes());
    f.extend_from_slice(name);
    f
}

fn group_field(group: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(1 + group.len());
    f.push(STREAM_GROUP_TAG);
    f.extend_from_slice(group);
    f
}

fn pending_field(group: &[u8], id: StreamId) -> Vec<u8> {
    let mut f = named(STREAM_PENDING_TAG, group);
    f.extend_from_slice(&id.to_bytes());
    f
}

fn consumer_field(group: &[u8], consumer: &[u8]) -> Vec<u8> {
    let mut f = named(STREAM_CONSUMER_TAG, group);
    f.extend_from_slice(consumer);
    f
}

/// The field prefix of one consumer's own pending entries.
fn owned_prefix(group: &[u8], consumer: &[u8]) -> Vec<u8> {
    let mut f = named(STREAM_OWNED_TAG, group);
    f.extend_from_slice(&(consumer.len() as u32).to_be_bytes());
    f.extend_from_slice(consumer);
    f
}

fn owned_field(group: &[u8], consumer: &[u8], id: StreamId) -> Vec<u8> {
    let mut f = owned_prefix(group, consumer);
    f.extend_from_slice(&id.to_bytes());
    f
}

/// Whether an entry at or after `start` was deleted (Valkey's
/// `streamRangeHasTombstones`, to the end of the stream). Valkey 9.1 counts
/// a deletion that a trim has since passed too: measured 2026-10-12, a group
/// with ENTRIESREAD 0 at `0-0` over entries 1 to 5, after `XDEL 2-0` and
/// `XTRIM MAXLEN 2`, lags 2, not 5.
fn has_tombstones(meta: &StreamMeta, start: StreamId) -> bool {
    meta.base.size != 0 && meta.max_deleted_id != StreamId::MIN && start <= meta.max_deleted_id
}

/// How many entries precede and include `id` since the stream began, when
/// that can be known (Valkey's `streamEstimateDistanceFromFirstEverEntry`).
fn estimate_entries_read(meta: &StreamMeta, first: StreamId, id: StreamId) -> i64 {
    if meta.entries_added == 0 {
        return 0;
    }
    let added = meta.entries_added as i64;
    let len = i64::from(meta.base.size);
    if len == 0 && id <= meta.last_id {
        return added;
    }
    // A deletion lies between `id` and the end, so how many entries came
    // before `id` cannot be known; the start of the stream is the exception.
    if id != StreamId::MIN && id < meta.max_deleted_id {
        return ENTRIES_READ_UNKNOWN;
    }
    if id == meta.last_id {
        return added;
    }
    if id > meta.last_id {
        return ENTRIES_READ_UNKNOWN;
    }
    if meta.max_deleted_id == StreamId::MIN || meta.max_deleted_id < first {
        if id < first {
            return added - len;
        }
        if id == first {
            return added - len + 1;
        }
    }
    ENTRIES_READ_UNKNOWN
}

/// A group's lag: how many of the stream's entries it has not read, when
/// that can be known (Valkey's `streamReplyWithCGLag`).
pub fn lag(meta: &StreamMeta, first: StreamId, g: &Group) -> Option<i64> {
    let added = meta.entries_added as i64;
    if meta.entries_added == 0 {
        return Some(0);
    }
    if g.entries_read != ENTRIES_READ_UNKNOWN && !has_tombstones(meta, g.last_id) {
        return Some(added - g.entries_read);
    }
    match estimate_entries_read(meta, first, g.last_id) {
        ENTRIES_READ_UNKNOWN => None,
        read => Some(added - read),
    }
}

impl StreamStore<'_> {
    fn row(&self, slot: u16, key: &[u8], meta: &StreamMeta, field: &[u8]) -> Vec<u8> {
        subkey_envelope(&self.ns, slot, key, meta.base.version, field)
    }

    fn rows(&self, slot: u16, key: &[u8], meta: &StreamMeta, field_prefix: &[u8]) -> Vec<u8> {
        let mut p = subkey_prefix(&self.ns, slot, key, meta.base.version);
        p.extend_from_slice(field_prefix);
        p
    }

    fn load_group(&self, slot: u16, key: &[u8], meta: &StreamMeta, name: &[u8]) -> Option<Group> {
        self.kv
            .get(&self.row(slot, key, meta, &group_field(name)))
            .and_then(|v| Group::decode(&v))
    }

    fn save_group(&self, slot: u16, key: &[u8], meta: &StreamMeta, name: &[u8], g: &Group) {
        self.kv
            .put(&self.row(slot, key, meta, &group_field(name)), &g.encode());
    }

    fn load_consumer(
        &self,
        slot: u16,
        key: &[u8],
        meta: &StreamMeta,
        group: &[u8],
        name: &[u8],
    ) -> Option<Consumer> {
        self.kv
            .get(&self.row(slot, key, meta, &consumer_field(group, name)))
            .and_then(|v| Consumer::decode(name, &v))
    }

    fn save_consumer(&self, slot: u16, key: &[u8], meta: &StreamMeta, group: &[u8], c: &Consumer) {
        self.kv.put(
            &self.row(slot, key, meta, &consumer_field(group, &c.name)),
            &c.encode(),
        );
    }

    fn load_pending(
        &self,
        slot: u16,
        key: &[u8],
        meta: &StreamMeta,
        group: &[u8],
        id: StreamId,
    ) -> Option<Pending> {
        self.kv
            .get(&self.row(slot, key, meta, &pending_field(group, id)))
            .and_then(|v| Pending::decode(id, &v))
    }

    /// Delete every row under `field_prefix`, as it is walked.
    fn delete_rows(&self, slot: u16, key: &[u8], meta: &StreamMeta, field_prefix: &[u8]) {
        let prefix = self.rows(slot, key, meta, field_prefix);
        self.kv.for_each_prefix(&prefix, &mut |k, _| {
            self.kv.delete(k);
            true
        });
    }

    /// The first entry's ID, as Valkey keeps it: `0-0` for an empty stream.
    fn first_id(&self, slot: u16, key: &[u8], meta: &StreamMeta) -> StreamId {
        if meta.base.size == 0 {
            return StreamId::MIN;
        }
        let prefix = self.entries_prefix(slot, key, meta.base.version);
        let mut first = StreamId::MIN;
        self.kv.for_each_prefix(&prefix, &mut |k, _| {
            if let Some(id) = StreamId::from_bytes(&k[prefix.len()..]) {
                first = id;
            }
            false
        });
        first
    }

    /// The stream at `key` and its group `name`.
    pub fn group(
        &self,
        slot: u16,
        key: &[u8],
        name: &[u8],
    ) -> Result<Found<(StreamMeta, Group)>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(Found::NoStream);
        };
        Ok(match self.load_group(slot, key, &meta, name) {
            Some(g) => Found::Yes((meta, g)),
            None => Found::NoGroup,
        })
    }

    /// XGROUP CREATE. `mkstream` makes an empty stream for a missing key.
    pub fn group_create(
        &self,
        slot: u16,
        key: &[u8],
        name: &[u8],
        start: GroupStart,
        mkstream: bool,
        entries_read: i64,
    ) -> Result<Created, StoreError> {
        let meta = match self.read_meta(slot, key)? {
            Some(m) => m,
            None if !mkstream => return Ok(Created::NoStream),
            None => {
                let m = StreamMeta::new(VersionGen::next((self.clock)()));
                self.write_meta(slot, key, &m);
                m
            }
        };
        if self.load_group(slot, key, &meta, name).is_some() {
            return Ok(Created::Exists);
        }
        let last_id = match start {
            GroupStart::Last => meta.last_id,
            GroupStart::Id(id) => id,
        };
        let g = Group {
            last_id,
            entries_read,
            pending: 0,
            consumers: 0,
        };
        self.save_group(slot, key, &meta, name, &g);
        Ok(Created::Created)
    }

    /// XGROUP SETID: the group's last delivered ID and its entries read.
    pub fn group_setid(
        &self,
        slot: u16,
        key: &[u8],
        name: &[u8],
        start: GroupStart,
        entries_read: i64,
    ) -> Result<Found<()>, StoreError> {
        let (meta, mut g) = match self.group(slot, key, name)? {
            Found::Yes(found) => found,
            Found::NoStream => return Ok(Found::NoStream),
            Found::NoGroup => return Ok(Found::NoGroup),
        };
        g.last_id = match start {
            GroupStart::Last => meta.last_id,
            GroupStart::Id(id) => id,
        };
        g.entries_read = entries_read;
        self.save_group(slot, key, &meta, name, &g);
        Ok(Found::Yes(()))
    }

    /// XGROUP DESTROY: the group, its pending entries and its consumers.
    /// `Yes(false)` when there was no such group.
    pub fn group_destroy(
        &self,
        slot: u16,
        key: &[u8],
        name: &[u8],
    ) -> Result<Found<bool>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(Found::NoStream);
        };
        if self.load_group(slot, key, &meta, name).is_none() {
            return Ok(Found::Yes(false));
        }
        self.kv
            .delete(&self.row(slot, key, &meta, &group_field(name)));
        for tag in [STREAM_PENDING_TAG, STREAM_CONSUMER_TAG, STREAM_OWNED_TAG] {
            self.delete_rows(slot, key, &meta, &named(tag, name));
        }
        Ok(Found::Yes(true))
    }

    /// XGROUP CREATECONSUMER: `Yes(true)` if the consumer is new.
    pub fn consumer_create(
        &self,
        slot: u16,
        key: &[u8],
        group: &[u8],
        consumer: &[u8],
    ) -> Result<Found<bool>, StoreError> {
        let (meta, mut g) = match self.group(slot, key, group)? {
            Found::Yes(found) => found,
            Found::NoStream => return Ok(Found::NoStream),
            Found::NoGroup => return Ok(Found::NoGroup),
        };
        if self
            .load_consumer(slot, key, &meta, group, consumer)
            .is_some()
        {
            return Ok(Found::Yes(false));
        }
        self.save_consumer(
            slot,
            key,
            &meta,
            group,
            &Consumer::new(consumer, (self.clock)()),
        );
        g.consumers += 1;
        self.save_group(slot, key, &meta, group, &g);
        Ok(Found::Yes(true))
    }

    /// XGROUP DELCONSUMER: the consumer and its pending entries, which
    /// leave the group's too. Answers how many were pending.
    pub fn consumer_delete(
        &self,
        slot: u16,
        key: &[u8],
        group: &[u8],
        consumer: &[u8],
    ) -> Result<Found<u64>, StoreError> {
        let (meta, mut g) = match self.group(slot, key, group)? {
            Found::Yes(found) => found,
            Found::NoStream => return Ok(Found::NoStream),
            Found::NoGroup => return Ok(Found::NoGroup),
        };
        if self
            .load_consumer(slot, key, &meta, group, consumer)
            .is_none()
        {
            return Ok(Found::Yes(0));
        }
        let prefix = self.rows(slot, key, &meta, &owned_prefix(group, consumer));
        let mut gone = 0u64;
        self.kv.for_each_prefix(&prefix, &mut |k, _| {
            if let Some(id) = StreamId::from_bytes(&k[prefix.len()..]) {
                self.kv
                    .delete(&self.row(slot, key, &meta, &pending_field(group, id)));
                gone += 1;
            }
            self.kv.delete(k);
            true
        });
        self.kv
            .delete(&self.row(slot, key, &meta, &consumer_field(group, consumer)));
        g.pending = g.pending.saturating_sub(gone);
        g.consumers = g.consumers.saturating_sub(1);
        self.save_group(slot, key, &meta, group, &g);
        Ok(Found::Yes(gone))
    }

    /// XREADGROUP on one stream, for `consumer` of `group`, which it
    /// creates if it is new. `New` reads the entries after the group's last
    /// delivered ID, and answers `None` when there are none; each becomes
    /// pending for the consumer unless `noack`. `After(id)` reads the
    /// consumer's own pending entries after `id`, counting another delivery
    /// of each that still exists.
    #[allow(clippy::too_many_arguments)]
    pub fn read_group(
        &self,
        slot: u16,
        key: &[u8],
        group: &[u8],
        consumer: &[u8],
        from: ReadFrom,
        count: Option<usize>,
        noack: bool,
    ) -> Result<Found<Option<Vec<GroupEntry>>>, StoreError> {
        let (meta, mut g) = match self.group(slot, key, group)? {
            Found::Yes(found) => found,
            Found::NoStream => return Ok(Found::NoStream),
            Found::NoGroup => return Ok(Found::NoGroup),
        };
        let now = (self.clock)();
        let mut c = match self.load_consumer(slot, key, &meta, group, consumer) {
            Some(c) => c,
            None => {
                g.consumers += 1;
                Consumer::new(consumer, now)
            }
        };
        c.seen_ms = now;
        let served = match from {
            ReadFrom::After(after) => {
                Some(self.read_history(slot, key, &meta, group, &c.name, after, count, now))
            }
            ReadFrom::New => {
                let entries = match g.last_id.next() {
                    Some(lo) => self.scan(slot, key, &meta, lo, StreamId::MAX, count, false),
                    None => Vec::new(),
                };
                if entries.is_empty() {
                    None
                } else {
                    let first = self.first_id(slot, key, &meta);
                    let mut others: BTreeMap<Vec<u8>, Consumer> = BTreeMap::new();
                    for (id, _) in &entries {
                        // A count is carried forward only from a position
                        // the stream still holds with no deletion after it,
                        // and is estimated again otherwise. Measured on
                        // Valkey 9.1, 2026-10-12, over entries 1 to 5: at
                        // `2-0` with ENTRIESREAD 2, after `XTRIM MAXLEN 2`,
                        // reading `4-0` gives 4, not 3; at `1-0` with
                        // ENTRIESREAD 1, after `XDEL 2-0`, reading `3-0`
                        // gives nil, not 2.
                        if g.entries_read != ENTRIES_READ_UNKNOWN
                            && g.last_id >= first
                            && !has_tombstones(&meta, g.last_id)
                        {
                            g.entries_read += 1;
                        } else if meta.entries_added > 0 {
                            g.entries_read = estimate_entries_read(&meta, first, *id);
                        }
                        g.last_id = *id;
                        if !noack {
                            self.deliver(
                                slot,
                                key,
                                &meta,
                                group,
                                &mut g,
                                &mut c,
                                &mut others,
                                *id,
                                now,
                            );
                        }
                    }
                    for other in others.values() {
                        self.save_consumer(slot, key, &meta, group, other);
                    }
                    Some(
                        entries
                            .into_iter()
                            .map(|(id, fields)| (id, Some(fields)))
                            .collect(),
                    )
                }
            }
        };
        self.save_consumer(slot, key, &meta, group, &c);
        self.save_group(slot, key, &meta, group, &g);
        Ok(Found::Yes(served))
    }

    /// Make `id` pending for `c`, taking it from whichever consumer held it
    /// (XGROUP SETID can make a group deliver an entry again).
    #[allow(clippy::too_many_arguments)]
    fn deliver(
        &self,
        slot: u16,
        key: &[u8],
        meta: &StreamMeta,
        group: &[u8],
        g: &mut Group,
        c: &mut Consumer,
        others: &mut BTreeMap<Vec<u8>, Consumer>,
        id: StreamId,
        now: u64,
    ) {
        match self.load_pending(slot, key, meta, group, id) {
            Some(held) if held.consumer == c.name => {}
            Some(held) => {
                self.kv
                    .delete(&self.row(slot, key, meta, &owned_field(group, &held.consumer, id)));
                if !others.contains_key(&held.consumer)
                    && let Some(o) = self.load_consumer(slot, key, meta, group, &held.consumer)
                {
                    others.insert(held.consumer.clone(), o);
                }
                if let Some(o) = others.get_mut(&held.consumer) {
                    o.pending = o.pending.saturating_sub(1);
                }
                c.pending += 1;
            }
            None => {
                g.pending += 1;
                c.pending += 1;
            }
        }
        let p = Pending {
            id,
            consumer: c.name.clone(),
            delivered_ms: now,
            deliveries: 1,
        };
        self.kv.put(
            &self.row(slot, key, meta, &pending_field(group, id)),
            &p.encode(),
        );
        self.kv.put(
            &self.row(slot, key, meta, &owned_field(group, &c.name, id)),
            &[],
        );
        c.active_ms = Some(now);
    }

    /// A consumer's own pending entries after `after`, each delivered again.
    #[allow(clippy::too_many_arguments)]
    fn read_history(
        &self,
        slot: u16,
        key: &[u8],
        meta: &StreamMeta,
        group: &[u8],
        consumer: &[u8],
        after: StreamId,
        count: Option<usize>,
        now: u64,
    ) -> Vec<GroupEntry> {
        let mut out = Vec::new();
        let want = count.unwrap_or(usize::MAX);
        if want == 0 {
            return out;
        }
        let prefix = self.rows(slot, key, meta, &owned_prefix(group, consumer));
        let start = self.row(slot, key, meta, &owned_field(group, consumer, after));
        let mut ids = Vec::new();
        self.kv.for_each_from(&prefix, &start, &mut |k, _| {
            if let Some(id) = StreamId::from_bytes(&k[prefix.len()..]) {
                ids.push(id);
            }
            ids.len() < want
        });
        for id in ids {
            match self
                .kv
                .get(&self.entry_key(slot, key, meta.base.version, id))
            {
                Some(v) => {
                    if let Some(mut p) = self.load_pending(slot, key, meta, group, id) {
                        p.delivered_ms = now;
                        p.deliveries += 1;
                        self.kv.put(
                            &self.row(slot, key, meta, &pending_field(group, id)),
                            &p.encode(),
                        );
                    }
                    out.push((id, Some(decode_fields(&v))));
                }
                None => out.push((id, None)),
            }
        }
        out
    }

    /// XACK: how many of `ids` were pending in the group, which they no
    /// longer are.
    pub fn ack(
        &self,
        slot: u16,
        key: &[u8],
        group: &[u8],
        ids: &[StreamId],
    ) -> Result<Found<u64>, StoreError> {
        let (meta, mut g) = match self.group(slot, key, group)? {
            Found::Yes(found) => found,
            Found::NoStream => return Ok(Found::NoStream),
            Found::NoGroup => return Ok(Found::NoGroup),
        };
        let mut owners: BTreeMap<Vec<u8>, Consumer> = BTreeMap::new();
        let mut acked = 0u64;
        for &id in ids {
            let Some(p) = self.load_pending(slot, key, &meta, group, id) else {
                continue;
            };
            self.kv
                .delete(&self.row(slot, key, &meta, &pending_field(group, id)));
            self.kv
                .delete(&self.row(slot, key, &meta, &owned_field(group, &p.consumer, id)));
            if !owners.contains_key(&p.consumer)
                && let Some(o) = self.load_consumer(slot, key, &meta, group, &p.consumer)
            {
                owners.insert(p.consumer.clone(), o);
            }
            if let Some(o) = owners.get_mut(&p.consumer) {
                o.pending = o.pending.saturating_sub(1);
            }
            g.pending = g.pending.saturating_sub(1);
            acked += 1;
        }
        if acked > 0 {
            for o in owners.values() {
                self.save_consumer(slot, key, &meta, group, o);
            }
            self.save_group(slot, key, &meta, group, &g);
        }
        Ok(Found::Yes(acked))
    }

    /// XPENDING's summary of a group.
    pub fn pending_summary(
        &self,
        slot: u16,
        key: &[u8],
        group: &[u8],
    ) -> Result<Found<PendingSummary>, StoreError> {
        let (meta, g) = match self.group(slot, key, group)? {
            Found::Yes(found) => found,
            Found::NoStream => return Ok(Found::NoStream),
            Found::NoGroup => return Ok(Found::NoGroup),
        };
        if g.pending == 0 {
            return Ok(Found::Yes(PendingSummary {
                count: 0,
                bounds: None,
                consumers: Vec::new(),
            }));
        }
        let prefix = self.rows(slot, key, &meta, &named(STREAM_PENDING_TAG, group));
        let mut first = None;
        self.kv.for_each_prefix(&prefix, &mut |k, _| {
            first = StreamId::from_bytes(&k[prefix.len()..]);
            false
        });
        let mut last = None;
        self.kv.for_each_before(&prefix, &[], &mut |k, _| {
            last = StreamId::from_bytes(&k[prefix.len()..]);
            false
        });
        let consumers = self
            .consumers(slot, key, &meta, group)
            .into_iter()
            .filter(|c| c.pending > 0)
            .map(|c| (c.name, c.pending))
            .collect();
        Ok(Found::Yes(PendingSummary {
            count: g.pending,
            bounds: first.zip(last),
            consumers,
        }))
    }

    /// XPENDING's entries from `start` to `end`, at most `count`, idle at
    /// least `min_idle` ms when it is not 0; only `consumer`'s when named,
    /// and none for a consumer that does not exist. Each comes with its idle
    /// time, in ms.
    #[allow(clippy::too_many_arguments)]
    pub fn pending_range(
        &self,
        slot: u16,
        key: &[u8],
        group: &[u8],
        start: StreamId,
        end: StreamId,
        count: u64,
        consumer: Option<&[u8]>,
        min_idle: i64,
    ) -> Result<Found<Vec<(Pending, u64)>>, StoreError> {
        let (meta, _) = match self.group(slot, key, group)? {
            Found::Yes(found) => found,
            Found::NoStream => return Ok(Found::NoStream),
            Found::NoGroup => return Ok(Found::NoGroup),
        };
        let now = (self.clock)();
        let mut out = Vec::new();
        if count == 0 || start > end {
            return Ok(Found::Yes(out));
        }
        let field_prefix = match consumer {
            Some(name) => {
                if self.load_consumer(slot, key, &meta, group, name).is_none() {
                    return Ok(Found::Yes(out));
                }
                owned_prefix(group, name)
            }
            None => named(STREAM_PENDING_TAG, group),
        };
        let prefix = self.rows(slot, key, &meta, &field_prefix);
        let from = start
            .prev()
            .map(|p| {
                let mut k = prefix.clone();
                k.extend_from_slice(&p.to_bytes());
                k
            })
            .unwrap_or_default();
        // Stops at `count`, so a large PEL costs what is answered.
        self.kv.for_each_from(&prefix, &from, &mut |k, v| {
            let Some(id) = StreamId::from_bytes(&k[prefix.len()..]) else {
                return true;
            };
            if id > end {
                return false;
            }
            let p = match consumer {
                // The group's own row carries what XPENDING answers.
                None => Pending::decode(id, v),
                Some(_) => self.load_pending(slot, key, &meta, group, id),
            };
            let Some(p) = p else {
                return true;
            };
            let idle = now as i64 - p.delivered_ms as i64;
            if min_idle != 0 && idle < min_idle {
                return true;
            }
            out.push((p, idle.max(0) as u64));
            (out.len() as u64) < count
        });
        Ok(Found::Yes(out))
    }

    /// A group's consumers, in name order.
    fn consumers(&self, slot: u16, key: &[u8], meta: &StreamMeta, group: &[u8]) -> Vec<Consumer> {
        let prefix = self.rows(slot, key, meta, &named(STREAM_CONSUMER_TAG, group));
        let mut out = Vec::new();
        self.kv.for_each_prefix(&prefix, &mut |k, v| {
            if let Some(c) = Consumer::decode(&k[prefix.len()..], v) {
                out.push(c);
            }
            true
        });
        out
    }

    /// XINFO CONSUMERS: the group's consumers, in name order.
    pub fn group_consumers(
        &self,
        slot: u16,
        key: &[u8],
        group: &[u8],
    ) -> Result<Found<Vec<Consumer>>, StoreError> {
        Ok(match self.group(slot, key, group)? {
            Found::Yes((meta, _)) => Found::Yes(self.consumers(slot, key, &meta, group)),
            Found::NoStream => Found::NoStream,
            Found::NoGroup => Found::NoGroup,
        })
    }

    /// XINFO GROUPS: each group, in name order, with its lag; `None` when
    /// there is no stream.
    #[allow(clippy::type_complexity)]
    pub fn groups(
        &self,
        slot: u16,
        key: &[u8],
    ) -> Result<Option<Vec<(Vec<u8>, Group, Option<i64>)>>, StoreError> {
        let Some(meta) = self.read_meta(slot, key)? else {
            return Ok(None);
        };
        let prefix = self.rows(slot, key, &meta, &[STREAM_GROUP_TAG]);
        let mut found = Vec::new();
        self.kv.for_each_prefix(&prefix, &mut |k, v| {
            if let Some(g) = Group::decode(v) {
                found.push((k[prefix.len()..].to_vec(), g));
            }
            true
        });
        let first = self.first_id(slot, key, &meta);
        Ok(Some(
            found
                .into_iter()
                .map(|(name, g)| {
                    let l = lag(&meta, first, &g);
                    (name, g, l)
                })
                .collect(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemKv;
    use crate::streams::IdSpec;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NOW: AtomicU64 = AtomicU64::new(1_000);
    fn clock() -> u64 {
        NOW.load(Ordering::Relaxed)
    }

    fn id(ms: u64) -> StreamId {
        StreamId { ms, seq: 0 }
    }

    fn add(st: &StreamStore<'_>, key: &[u8], ms: u64) {
        st.add(
            1,
            key,
            IdSpec::Explicit(id(ms)),
            &[b"f".to_vec(), b"v".to_vec()],
            false,
            None,
        )
        .expect("store");
    }

    fn read(st: &StreamStore<'_>, key: &[u8], consumer: &[u8], from: ReadFrom) -> Vec<StreamId> {
        match st
            .read_group(1, key, b"g", consumer, from, None, false)
            .expect("store")
        {
            Found::Yes(Some(e)) => e.into_iter().map(|(id, _)| id).collect(),
            Found::Yes(None) => Vec::new(),
            other => panic!("{other:?}"),
        }
    }

    /// The group's counters always agree with its rows: pending entries,
    /// each consumer's, and its consumers.
    fn assert_counters(st: &StreamStore<'_>, key: &[u8]) {
        let Found::Yes((meta, g)) = st.group(1, key, b"g").expect("store") else {
            panic!("no group");
        };
        let mut pending = 0;
        st.kv.for_each_prefix(
            &st.rows(1, key, &meta, &named(STREAM_PENDING_TAG, b"g")),
            &mut |_, _| {
                pending += 1;
                true
            },
        );
        assert_eq!(g.pending, pending, "group pending");
        let consumers = st.consumers(1, key, &meta, b"g");
        assert_eq!(g.consumers, consumers.len() as u64, "consumers");
        for c in consumers {
            let mut owned = 0;
            st.kv.for_each_prefix(
                &st.rows(1, key, &meta, &owned_prefix(b"g", &c.name)),
                &mut |_, _| {
                    owned += 1;
                    true
                },
            );
            assert_eq!(c.pending, owned, "{:?} pending", c.name);
        }
    }

    #[test]
    fn deliver_reassign_ack_and_delete_keep_every_counter_true() {
        let kv = MemKv::new();
        let st = StreamStore::new(&kv, b"ns", clock);
        for ms in 1..=4 {
            add(&st, b"s", ms);
        }
        assert_eq!(
            st.group_create(1, b"s", b"g", GroupStart::Id(StreamId::MIN), false, -1)
                .expect("store"),
            Created::Created
        );
        assert_eq!(
            read(&st, b"s", b"a", ReadFrom::New),
            [id(1), id(2), id(3), id(4)]
        );
        assert_counters(&st, b"s");
        // SETID back to the start: `b` takes every entry from `a`.
        st.group_setid(1, b"s", b"g", GroupStart::Id(StreamId::MIN), -1)
            .expect("store");
        assert_eq!(read(&st, b"s", b"b", ReadFrom::New).len(), 4);
        assert_counters(&st, b"s");
        assert_eq!(read(&st, b"s", b"a", ReadFrom::After(StreamId::MIN)), []);
        assert_eq!(
            st.ack(1, b"s", b"g", &[id(2), id(2), id(9)])
                .expect("store"),
            Found::Yes(1)
        );
        assert_counters(&st, b"s");
        assert_eq!(
            st.consumer_delete(1, b"s", b"g", b"b").expect("store"),
            Found::Yes(3)
        );
        assert_counters(&st, b"s");
        let Found::Yes(summary) = st.pending_summary(1, b"s", b"g").expect("store") else {
            panic!();
        };
        assert_eq!(summary.count, 0);
    }

    #[test]
    fn a_history_read_counts_a_delivery_and_shows_a_deleted_entry_as_nil() {
        let kv = MemKv::new();
        let st = StreamStore::new(&kv, b"ns", clock);
        add(&st, b"s", 1);
        add(&st, b"s", 2);
        st.group_create(1, b"s", b"g", GroupStart::Id(StreamId::MIN), false, -1)
            .expect("store");
        read(&st, b"s", b"a", ReadFrom::New);
        st.del(1, b"s", &[id(1)]).expect("store");
        let Found::Yes(Some(history)) = st
            .read_group(
                1,
                b"s",
                b"g",
                b"a",
                ReadFrom::After(StreamId::MIN),
                None,
                false,
            )
            .expect("store")
        else {
            panic!();
        };
        assert_eq!(history[0], (id(1), None));
        assert!(history[1].1.is_some());
        let Found::Yes(rows) = st
            .pending_range(1, b"s", b"g", StreamId::MIN, StreamId::MAX, 10, None, 0)
            .expect("store")
        else {
            panic!();
        };
        let deliveries: Vec<u64> = rows.iter().map(|(p, _)| p.deliveries).collect();
        assert_eq!(deliveries, [1, 2], "a deleted entry is not delivered again");
    }

    #[test]
    fn noack_reads_advance_the_group_and_leave_nothing_pending() {
        let kv = MemKv::new();
        let st = StreamStore::new(&kv, b"ns", clock);
        add(&st, b"s", 1);
        st.group_create(1, b"s", b"g", GroupStart::Id(StreamId::MIN), false, -1)
            .expect("store");
        let Found::Yes(Some(e)) = st
            .read_group(1, b"s", b"g", b"a", ReadFrom::New, None, true)
            .expect("store")
        else {
            panic!();
        };
        assert_eq!(e.len(), 1);
        let Found::Yes((_, g)) = st.group(1, b"s", b"g").expect("store") else {
            panic!();
        };
        assert_eq!((g.last_id, g.pending, g.consumers), (id(1), 0, 1));
        let Found::Yes(c) = st.group_consumers(1, b"s", b"g").expect("store") else {
            panic!();
        };
        assert_eq!(c[0].active_ms, None, "a NOACK read is not activity");
    }

    #[test]
    fn destroy_removes_every_row_of_the_group_and_only_that_group() {
        let kv = MemKv::new();
        let st = StreamStore::new(&kv, b"ns", clock);
        add(&st, b"s", 1);
        for g in [&b"g"[..], b"gg"] {
            st.group_create(1, b"s", g, GroupStart::Id(StreamId::MIN), false, -1)
                .expect("store");
            st.read_group(1, b"s", g, b"a", ReadFrom::New, None, false)
                .expect("store");
        }
        assert_eq!(
            st.group_destroy(1, b"s", b"g").expect("store"),
            Found::Yes(true)
        );
        assert_eq!(
            st.group_destroy(1, b"s", b"g").expect("store"),
            Found::Yes(false)
        );
        let Found::Yes(s) = st.pending_summary(1, b"s", b"gg").expect("store") else {
            panic!();
        };
        assert_eq!(s.count, 1, "the other group keeps its pending entry");
        let meta = st.read_meta(1, b"s").expect("store").expect("store");
        let mut left = 0;
        st.kv.for_each_prefix(
            &subkey_prefix(b"ns", 1, b"s", meta.base.version),
            &mut |_, _| {
                left += 1;
                true
            },
        );
        // The entry, and gg's group, pending, consumer and owned rows.
        assert_eq!(left, 5);
    }

    /// A deletion a trim has passed still makes a group's count unknown
    /// behind it, as in Valkey 9.1 (measured 2026-10-12: entries 1 to 5,
    /// `XDEL 2-0`, `XTRIM MAXLEN 2`).
    #[test]
    fn lag_counts_a_deletion_that_a_trim_has_passed() {
        let kv = MemKv::new();
        let st = StreamStore::new(&kv, b"ns", clock);
        for ms in 1..=5 {
            add(&st, b"d", ms);
        }
        st.del(1, b"d", &[id(2)]).expect("store");
        st.trim(
            1,
            b"d",
            crate::streams::Trim {
                to: crate::streams::TrimTo::MaxLen(2),
                limit: None,
            },
        )
        .expect("store");
        for (name, at, read) in [
            (&b"gb"[..], 0, 0),
            (b"gc", 1, -1),
            (b"gd", 1, 1),
            (b"gj", 2, 2),
            (b"gf", 3, 3),
            (b"gh", 4, 2),
        ] {
            st.group_create(1, b"d", name, GroupStart::Id(id(at)), false, read)
                .expect("store");
        }
        let lags: Vec<Option<i64>> = st
            .groups(1, b"d")
            .expect("store")
            .expect("store")
            .into_iter()
            .map(|(_, _, l)| l)
            .collect();
        // In name order: gb, gc, gd, gf, gh, gj.
        assert_eq!(lags, [Some(2), None, None, Some(2), Some(3), Some(2)]);
    }

    /// After a trim passed a group, its next read estimates its count
    /// rather than adding to one the trim made wrong (Valkey 9.1, measured
    /// 2026-10-12).
    #[test]
    fn a_read_after_a_trim_passed_the_group_estimates_its_count() {
        let kv = MemKv::new();
        let st = StreamStore::new(&kv, b"ns", clock);
        for ms in 1..=5 {
            add(&st, b"p", ms);
        }
        st.group_create(1, b"p", b"g", GroupStart::Id(id(2)), false, 2)
            .expect("store");
        st.trim(
            1,
            b"p",
            crate::streams::Trim {
                to: crate::streams::TrimTo::MaxLen(2),
                limit: None,
            },
        )
        .expect("store");
        st.read_group(1, b"p", b"g", b"c", ReadFrom::New, Some(1), false)
            .expect("store");
        let Found::Yes((_, g)) = st.group(1, b"p", b"g").expect("store") else {
            panic!();
        };
        assert_eq!((g.last_id, g.entries_read), (id(4), 4));
    }

    /// A deletion after a group's position makes its next read's count
    /// unknown (Valkey 9.1, measured 2026-10-12).
    #[test]
    fn a_read_past_a_deletion_loses_the_count() {
        let kv = MemKv::new();
        let st = StreamStore::new(&kv, b"ns", clock);
        for ms in 1..=5 {
            add(&st, b"q", ms);
        }
        st.group_create(1, b"q", b"g", GroupStart::Id(id(1)), false, 1)
            .expect("store");
        st.del(1, b"q", &[id(2)]).expect("store");
        st.read_group(1, b"q", b"g", b"c", ReadFrom::New, Some(1), false)
            .expect("store");
        let Found::Yes((_, g)) = st.group(1, b"q", b"g").expect("store") else {
            panic!();
        };
        assert_eq!((g.last_id, g.entries_read), (id(3), ENTRIES_READ_UNKNOWN));
    }

    #[test]
    fn lag_follows_valkeys_estimate() {
        let kv = MemKv::new();
        let st = StreamStore::new(&kv, b"ns", clock);
        for ms in 1..=3 {
            add(&st, b"t", ms);
        }
        st.group_create(1, b"t", b"g", GroupStart::Id(StreamId::MIN), false, -1)
            .expect("store");
        st.group_create(1, b"t", b"h", GroupStart::Last, false, -1)
            .expect("store");
        let lags = |st: &StreamStore<'_>| -> Vec<Option<i64>> {
            st.groups(1, b"t")
                .expect("store")
                .expect("store")
                .into_iter()
                .map(|(_, _, l)| l)
                .collect()
        };
        assert_eq!(lags(&st), [Some(3), Some(0)]);
        st.trim(
            1,
            b"t",
            crate::streams::Trim {
                to: crate::streams::TrimTo::MaxLen(1),
                limit: None,
            },
        )
        .expect("store");
        // Measured on Valkey 9.1, 2026-10-12.
        assert_eq!(lags(&st), [Some(1), Some(0)]);
        add(&st, b"t", 4);
        st.del(1, b"t", &[id(4)]).expect("store");
        assert_eq!(lags(&st), [None, None]);
    }
}
