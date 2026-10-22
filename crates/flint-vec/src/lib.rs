// SPDX-License-Identifier: Elastic-2.0
//! The vector-memory co-processor (ADR-0017): a flat (exact) approximate-
//! nearest-neighbour index served beside the data plane, reached through the
//! ADR-0010 co-processor mechanism as the `VEC.*` command family.
//!
//! This crate is the co-processor's LOGIC — the flat index, the per-tenant
//! store, and the `VEC.*` command planning — with no networking (the binary in
//! `main.rs` wraps it). A read (`VEC.SEARCH`/`GET`/`INFO`) is answered from the
//! in-memory index. A write plans in TWO phases so the in-memory index is never
//! ahead of the durable copy: [`Store::plan`] validates and returns a
//! [`Plan::Write`] carrying the [`Persist`] to perform over the channel and the
//! [`Apply`] to make to the index; the caller performs the durable write FIRST
//! and only [`Store::commit`]s the index if it lands. That ordering matters
//! because a channel write can be SHED (e.g. `-QUOTA` on a tenant over its
//! storage cap), and a search must never return a vector that isn't durable.
//!
//! v0.1 is flat/exact — the recall oracle. v0.2 replaces [`VectorSet`]'s search
//! with an HNSW graph behind the SAME plan. ADR-0049 adds compressed codes in
//! that graph (`QUANT sq8|bin`) with the best `RERANK` re-scored on full
//! vectors; it adds options to the `VEC.*` surface and a fourth config field
//! that the binary before it reads past, so the vector rows themselves are
//! unchanged. ADR-0059 adds fields read from a vector's `META`, which
//! `VEC.SEARCH ... FILTER` matches before it ranks, in a fifth config field.

pub mod filter;
pub mod ft;
pub mod ftindex;
pub mod ftstore;
pub mod fulltext;
pub mod geo;
mod hnsw;
mod kernel;
mod quant;
mod snowball;
pub mod vecfile;

use filter::{Expr, Field, FieldKind, Vals};
use flint_resp::Value;
use hnsw::Hnsw;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use vecfile::VecFile;

/// Distance metric, fixed per set. Scores are normalised so HIGHER is nearer,
/// so one bounded-heap top-k serves all three without a per-metric direction.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Metric {
    /// Cosine similarity in [-1, 1]; higher = nearer.
    Cosine,
    /// Negated squared L2 distance; higher (closer to 0) = nearer.
    L2,
    /// Inner product; higher = nearer.
    Ip,
}

impl Metric {
    pub fn parse(s: &[u8]) -> Option<Metric> {
        match s.to_ascii_lowercase().as_slice() {
            b"cosine" => Some(Metric::Cosine),
            b"l2" => Some(Metric::L2),
            b"ip" => Some(Metric::Ip),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Metric::Cosine => "cosine",
            Metric::L2 => "l2",
            Metric::Ip => "ip",
        }
    }
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    kernel::dot(a, b)
}

/// A stored vector: the original vector (VEC.GET is lossless), its cached L2
/// norm (cosine only), optional opaque metadata bytes, and the values of its
/// set's fields in them (ADR-0059). Boxed slices, not `Vec`s, so the values
/// cost a flat entry no more than the capacities they replaced.
struct Entry {
    vec: Box<[f32]>,
    norm: f32,
    meta: Option<Box<[u8]>>,
    vals: Option<Vals>,
}

/// Which engine backs a set. Chosen at VEC.CREATE and recorded in the durable
/// config so a rebuild restores the same kind.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum IndexKind {
    /// Exact brute-force scan — the recall oracle. Default.
    Flat,
    /// HNSW graph — approximate, sublinear, the recall/latency `EF` knob.
    Hnsw,
}
impl IndexKind {
    pub fn parse(s: &[u8]) -> Option<IndexKind> {
        match s.to_ascii_lowercase().as_slice() {
            b"flat" => Some(IndexKind::Flat),
            b"hnsw" => Some(IndexKind::Hnsw),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            IndexKind::Flat => "flat",
            IndexKind::Hnsw => "hnsw",
        }
    }
}

/// How an HNSW set holds its vectors in the graph (ADR-0049). Chosen at
/// VEC.CREATE and recorded as the config's FOURTH field, which a binary older
/// than this reads past: it loads the set as plain HNSW from the same full
/// durable rows, costing RAM and nothing else, so a rollback stays safe.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Quant {
    /// Full float32 vectors in the graph: today's HNSW. Default.
    None,
    /// Per-vector 8-bit codes in the graph; the top candidates are re-ranked
    /// against the full vectors.
    Sq8,
    /// One bit a dimension, after a fixed random rotation (ADR-0049 item 2):
    /// no training, a 32nd of the vector.
    Bin,
}
impl Quant {
    pub fn parse(s: &[u8]) -> Option<Quant> {
        match s.to_ascii_lowercase().as_slice() {
            b"none" => Some(Quant::None),
            b"sq8" => Some(Quant::Sq8),
            b"bin" => Some(Quant::Bin),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Quant::None => "none",
            Quant::Sq8 => "sq8",
            Quant::Bin => "bin",
        }
    }

    /// How many candidates a search re-ranks when `VEC.SEARCH` names no
    /// `RERANK`: this many times `k`, the depth past which the code's recall
    /// stopped paying for itself on 999,000 real 1536-d embeddings at the
    /// default `EF` (ADR-0049 verification 2). 1-bit gained 0.025 recall@10
    /// from 4k to 10k and 0.007 from 10k to 20k; `sq8` nothing from 2k to 4k.
    /// An unquantized set re-ranks nothing.
    pub fn rerank_multiple(self) -> usize {
        match self {
            Quant::None => 1,
            Quant::Sq8 => 4,
            Quant::Bin => 10,
        }
    }
}

/// A stored vector and its meta, owned.
type Stored = (Vec<f32>, Option<Vec<u8>>);

/// The engine behind a set. Both hold the vectors (a rebuild restores either);
/// flat scans them exactly, HNSW keeps a navigable graph over them.
enum Index {
    Flat(HashMap<Vec<u8>, Entry>),
    Hnsw(Box<Hnsw>),
}

/// One vector set: fixed dim + metric + index engine. `search` dispatches to the
/// engine; the `VEC.*` surface never sees which — the whole point of v0.2 being
/// a drop-in behind the same commands.
pub struct VectorSet {
    dim: usize,
    metric: Metric,
    index: Index,
    /// Absolute expiry (ms since epoch) for TTL'd ids only — the ephemeral-
    /// vector-memory wedge. Overlaid on either engine: search/get skip an
    /// expired id and the sweep removes it, so no engine needs its own TTL. The
    /// durable row carries the same expiry and the data-plane key its own PX,
    /// so a restart's rebuild and the tenant's metering agree without this map.
    expiry: HashMap<Vec<u8>, u64>,
    /// The fields declared at `VEC.CREATE` (ADR-0059), read from each
    /// vector's `META`; empty for most sets.
    fields: Box<[Field]>,
}

impl VectorSet {
    /// `quant` applies to HNSW only; a flat set always holds full vectors (it
    /// is the exact oracle), and VEC.CREATE refuses QUANT with INDEX flat.
    pub fn new(dim: usize, metric: Metric, kind: IndexKind, quant: Quant) -> Self {
        let index = match kind {
            IndexKind::Flat => Index::Flat(HashMap::new()),
            IndexKind::Hnsw => Index::Hnsw(Box::new(Hnsw::new(metric, quant))),
        };
        VectorSet {
            dim,
            metric,
            index,
            expiry: HashMap::new(),
            fields: Box::new([]),
        }
    }

    /// The same set, indexing `fields` from each vector's `META` (ADR-0059).
    /// Only for a set with nothing in it yet.
    pub fn with_fields(mut self, fields: Vec<Field>) -> Self {
        let fields = fields.into_boxed_slice();
        if let Index::Hnsw(h) = &mut self.index {
            h.set_fields(fields.clone());
        }
        self.fields = fields;
        self
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn dim(&self) -> usize {
        self.dim
    }
    pub fn metric(&self) -> Metric {
        self.metric
    }
    pub fn index_name(&self) -> &'static str {
        match &self.index {
            Index::Flat(_) => "flat",
            Index::Hnsw(_) => "hnsw",
        }
    }
    fn kind(&self) -> IndexKind {
        match &self.index {
            Index::Flat(_) => IndexKind::Flat,
            Index::Hnsw(_) => IndexKind::Hnsw,
        }
    }
    pub fn quant(&self) -> Quant {
        match &self.index {
            Index::Flat(_) => Quant::None,
            Index::Hnsw(h) => h.quant(),
        }
    }
    /// Keep this set's full vectors in `file` (ADR-0049 D2): a quantized HNSW
    /// set with nothing in it yet.
    fn attach_vec_file(&mut self, file: VecFile) {
        if let Index::Hnsw(h) = &mut self.index {
            h.attach_vec_file(file);
        }
    }
    /// Whether the full vectors are in a local file (ADR-0049 D2), which the
    /// D4 meter then does not charge for.
    pub fn vectors_on_disk(&self) -> bool {
        matches!(&self.index, Index::Hnsw(h) if h.vectors_on_disk())
    }
    fn disk_bytes(&self) -> u64 {
        match &self.index {
            Index::Flat(_) => 0,
            Index::Hnsw(h) => h.disk_bytes(),
        }
    }
    /// RAM held for vectors whose file write failed, which the meter charges.
    fn spill_bytes(&self) -> usize {
        match &self.index {
            Index::Flat(_) => 0,
            Index::Hnsw(h) => h.spill_bytes(),
        }
    }
    /// What `id` costs the D4 meter, with no file read. An HNSW set's field
    /// index is charged as a whole, in [`VectorSet::held`].
    fn entry_cost(&self, id: &[u8]) -> Option<usize> {
        let (meta_len, vals) = match &self.index {
            Index::Flat(m) => {
                let e = m.get(id)?;
                (
                    e.meta.as_ref().map_or(0, |x| x.len()),
                    filter::vals_bytes(e.vals.as_deref()),
                )
            }
            Index::Hnsw(h) => (h.meta(id)?.map_or(0, |x| x.len()), 0),
        };
        Some(self.cost(id.len(), meta_len) + vals)
    }
    /// What the set holds for the D4 meter beyond its live entries: deleted
    /// slots, vectors whose file write failed, and an HNSW set's field index.
    fn held(&self) -> usize {
        self.spill_bytes() + self.dead_cost() + self.fields_bytes()
    }
    fn fields_bytes(&self) -> usize {
        match &self.index {
            Index::Flat(_) => 0,
            Index::Hnsw(h) => h.fields_bytes(),
        }
    }
    /// What the fields' values hold, and an HNSW set's field index, for
    /// `VEC.INFO`. A scan of a flat set, which a search scans anyway.
    fn field_bytes(&self) -> usize {
        match &self.index {
            Index::Flat(m) => m
                .values()
                .map(|e| filter::vals_bytes(e.vals.as_deref()))
                .sum(),
            Index::Hnsw(h) => h.fields_bytes(),
        }
    }
    /// The most writing `meta` can add to what the set's fields cost, for the
    /// cap check before the write: what [`VectorSet::entry_cost`] and
    /// [`VectorSet::held`] will charge for it, or more.
    fn fields_charge(&self, meta: Option<&[u8]>) -> usize {
        if self.fields.is_empty() {
            return 0;
        }
        let vals = filter::extract(&self.fields, meta);
        match &self.index {
            Index::Flat(_) => filter::vals_bytes(vals.as_deref()),
            Index::Hnsw(_) => filter::FieldIndex::charge(vals.as_deref()),
        }
    }
    /// What the deleted slots of an HNSW set still cost the D4 meter: each is
    /// held until the set's next insert takes it, or the set is emptied
    /// (BUG-0257). A flat set frees an entry when it is deleted.
    fn dead_cost(&self) -> usize {
        match &self.index {
            Index::Flat(_) => 0,
            Index::Hnsw(h) => {
                let (slots, bytes) = h.dead();
                slots * self.cost(0, 0) + bytes
            }
        }
    }
    /// What the next insert of a new id gives back by taking a deleted slot,
    /// for the cap check before it is written: 0 when it takes none.
    fn reuse_credit(&self) -> usize {
        match &self.index {
            Index::Flat(_) => 0,
            Index::Hnsw(h) => h.next_reuse_bytes().map_or(0, |b| self.cost(0, 0) + b),
        }
    }
    /// What an entry with this id and meta length costs the D4 meter.
    fn cost(&self, id_len: usize, meta_len: usize) -> usize {
        entry_bytes(
            self.dim,
            id_len,
            meta_len,
            self.kind(),
            self.quant(),
            self.vectors_on_disk(),
        )
    }
    pub fn len(&self) -> usize {
        match &self.index {
            Index::Flat(m) => m.len(),
            Index::Hnsw(h) => h.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn contains(&self, id: &[u8]) -> bool {
        match &self.index {
            Index::Flat(m) => m.contains_key(id),
            Index::Hnsw(h) => h.contains(id),
        }
    }

    fn set(&mut self, id: Vec<u8>, vec: Vec<f32>, meta: Option<Vec<u8>>, expires_at: Option<u64>) {
        match expires_at {
            Some(e) => {
                self.expiry.insert(id.clone(), e);
            }
            // An upsert with no TTL clears any prior one — a re-set makes the id
            // permanent again, matching SET-clears-TTL for a native key.
            None => {
                self.expiry.remove(&id);
            }
        }
        match &mut self.index {
            Index::Flat(m) => {
                let norm = dot(&vec, &vec).sqrt();
                let vals = filter::extract(&self.fields, meta.as_deref());
                m.insert(
                    id,
                    Entry {
                        vec: vec.into_boxed_slice(),
                        norm,
                        meta: meta.map(Vec::into_boxed_slice),
                        vals,
                    },
                );
            }
            Index::Hnsw(h) => h.set(id, vec, meta),
        }
    }

    fn del(&mut self, id: &[u8]) -> bool {
        self.expiry.remove(id);
        match &mut self.index {
            Index::Flat(m) => m.remove(id).is_some(),
            Index::Hnsw(h) => h.del(id),
        }
    }

    fn is_expired(&self, id: &[u8], now: u64) -> bool {
        self.expiry.get(id).is_some_and(|&e| e <= now)
    }

    /// The absolute expiry (ms since epoch) for `id`, or None if it has no TTL.
    fn expiry_at(&self, id: &[u8]) -> Option<u64> {
        self.expiry.get(id).copied()
    }

    /// Ids that currently carry a not-yet-expired TTL — for VEC.INFO.
    fn ttl_count(&self, now: u64) -> usize {
        self.expiry.values().filter(|&&e| e > now).count()
    }

    /// How many indexed ids have already expired — how far to over-fetch a
    /// top-k so expired hits cannot starve the k live ones.
    fn expired_count(&self, now: u64) -> usize {
        self.expiry.values().filter(|&&e| e <= now).count()
    }

    /// Ids whose TTL has passed, for the sweep to remove.
    fn expired_ids(&self, now: u64) -> Vec<Vec<u8>> {
        self.expiry
            .iter()
            .filter(|&(_, &e)| e <= now)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// The stored vector + meta for VEC.GET (lossless), or None. Errs only
    /// when a vector file cannot be read.
    fn get(&self, id: &[u8]) -> Result<Option<Stored>, String> {
        Ok(match &self.index {
            Index::Flat(m) => m
                .get(id)
                .map(|e| (e.vec.to_vec(), e.meta.as_deref().map(<[u8]>::to_vec))),
            Index::Hnsw(h) => h
                .get(id)?
                .map(|(v, m)| (v.into_owned(), m.map(|x| x.to_vec()))),
        })
    }

    /// Top-k nearest, nearest first, of the vectors `filter` matches, or of
    /// all of them. `ef` is HNSW's recall/latency knob and `rerank` how many
    /// a quantized set re-scores (ADR-0049); flat ignores both (it is always
    /// exact). Errs only when a vector file cannot be read.
    fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: usize,
        rerank: usize,
        filter: Option<&Expr>,
        scan_cap: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, f32)>, String> {
        match (&self.index, filter) {
            (Index::Flat(m), None) => Ok(flat_search(m, self.metric, query, k, |_| true)),
            (Index::Flat(m), Some(f)) => Ok(flat_search(m, self.metric, query, k, |e| {
                f.matches(e.vals.as_deref())
            })),
            (Index::Hnsw(h), None) => h.knn(query, k, ef, rerank),
            (Index::Hnsw(h), Some(f)) => h.knn_where(query, k, ef, rerank, f, scan_cap),
        }
    }
}

#[derive(PartialEq)]
struct Scored {
    score: f32,
    id: Vec<u8>,
}
impl Eq for Scored {}
impl PartialOrd for Scored {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Scored {
    fn cmp(&self, o: &Self) -> Ordering {
        self.score
            .total_cmp(&o.score)
            .then_with(|| self.id.cmp(&o.id))
    }
}

/// Flat score: HIGHER is nearer. HNSW converts its internal distances into this
/// same convention so a mixed fleet answers VEC.SEARCH identically.
fn flat_score(metric: Metric, query: &[f32], q_norm: f32, e: &Entry) -> f32 {
    match metric {
        Metric::Ip => dot(query, &e.vec),
        Metric::L2 => -query
            .iter()
            .zip(e.vec.iter())
            .map(|(q, x)| {
                let d = q - x;
                d * d
            })
            .sum::<f32>(),
        Metric::Cosine => {
            let denom = q_norm * e.norm;
            if denom == 0.0 {
                0.0
            } else {
                dot(query, &e.vec) / denom
            }
        }
    }
}

/// Exact flat top-k of the entries `admit` takes, nearest first (recall 1.0).
/// O(n log k).
fn flat_search(
    entries: &HashMap<Vec<u8>, Entry>,
    metric: Metric,
    query: &[f32],
    k: usize,
    admit: impl Fn(&Entry) -> bool,
) -> Vec<(Vec<u8>, f32)> {
    if k == 0 {
        return Vec::new();
    }
    let q_norm = if metric == Metric::Cosine {
        dot(query, query).sqrt()
    } else {
        0.0
    };
    let mut heap: BinaryHeap<std::cmp::Reverse<Scored>> = BinaryHeap::with_capacity(k + 1);
    for (id, e) in entries.iter().filter(|(_, e)| admit(e)) {
        let score = flat_score(metric, query, q_norm, e);
        heap.push(std::cmp::Reverse(Scored {
            score,
            id: id.clone(),
        }));
        if heap.len() > k {
            heap.pop();
        }
    }
    let mut out: Vec<(Vec<u8>, f32)> = heap.into_iter().map(|r| (r.0.id, r.0.score)).collect();
    out.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    out
}

/// The durable side of a write: what the co-processor must do over the channel
/// so the vectors survive a crash (ADR-0017 D2). Performed by the networked
/// layer BEFORE the matching [`Apply`] touches the index. Keys live under a
/// reserved co-processor prefix in the tenant namespace (`durable_key`).
#[derive(Debug, PartialEq)]
pub enum Persist {
    /// `ttl_ms` mirrors the row's expiry onto the data-plane key itself, so the
    /// durable store reclaims it on the same clock the index does (metering and
    /// quota fall for free); `None` is a key that never expires.
    Put {
        key: Vec<u8>,
        val: Vec<u8>,
        ttl_ms: Option<u64>,
    },
    Del {
        key: Vec<u8>,
    },
    /// Add `member` to the durable id index at `key`. The index is what makes
    /// recovery O(vectors): without it the only way to find this co-processor's
    /// rows is to walk the tenant's whole keyspace (#194).
    SAdd {
        key: Vec<u8>,
        member: Vec<u8>,
    },
    SRem {
        key: Vec<u8>,
        member: Vec<u8>,
    },
}

/// The in-memory side of a write, applied by [`Store::commit`] only after its
/// [`Persist`] lands. Kept as data (not a closure) so [`Store::plan`] stays
/// immutable and the durable write can happen in between.
#[derive(Debug, PartialEq)]
pub enum Apply {
    CreateSet {
        set: Vec<u8>,
        dim: usize,
        metric: Metric,
        index: IndexKind,
        quant: Quant,
        /// Declared at `VEC.CREATE` (ADR-0059); empty for most sets.
        fields: Vec<Field>,
    },
    Insert {
        set: Vec<u8>,
        id: Vec<u8>,
        vec: Vec<f32>,
        meta: Option<Vec<u8>>,
        /// Absolute expiry in ms since epoch; `None` never expires.
        expires_at: Option<u64>,
    },
    Remove {
        set: Vec<u8>,
        id: Vec<u8>,
    },
}

/// The outcome of planning one command.
pub enum Plan {
    /// Serve now; no durable effect (reads, errors, and a no-op delete).
    Reply(Value),
    /// A validated write: perform `persist` over the channel FIRST; on success
    /// `commit(apply)` and reply `ok`; on a shed/failed write reply the
    /// channel's error and leave the index untouched.
    Write {
        /// EVERY command here must land before the index moves, in order.
        /// Order is chosen for crash safety, not convenience: the index entry
        /// goes down BEFORE the row it points at, so a crash in between leaves
        /// a phantom id (the rebuild GETs nil and skips it) rather than a
        /// durable row no index knows about, which would be silent data loss
        /// that only shows up as a short corpus after the next restart.
        persist: Vec<Persist>,
        apply: Apply,
        ok: Value,
    },
}

/// The reserved key prefix for co-processor-owned rows in a tenant namespace.
/// A leading NUL keeps these out of any ordinary keyspace a client walks, and
/// the `vec` tag namespaces this co-processor from any future one.
pub const KEY_PREFIX: &[u8] = b"\x00vec\x00";

/// Durable key for a set's config (`kind = b'c'`) or a vector (`kind = b'v'`).
/// NUL-separated; set and id are validated NUL-free at admission so the parse
/// on rebuild is unambiguous.
fn durable_key(kind: u8, set: &[u8], id: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(KEY_PREFIX.len() + set.len() + id.len() + 3);
    k.extend_from_slice(KEY_PREFIX);
    k.push(kind);
    k.push(0);
    k.extend_from_slice(set);
    if !id.is_empty() {
        k.push(0);
        k.extend_from_slice(id);
    }
    k
}

/// How many buckets a set's id index is split across.
///
/// MIGRATION: vectors written by a co-processor OLDER than this index have no
/// index entry, so a rebuild will not find them. ADR-0017 v0 calls the index
/// the ephemeral half and the durable rows the authority, and that is still
/// true — the rows are all still there — but the co-processor can no longer
/// ENUMERATE them, because the only mechanism that could was the whole-
/// namespace scan this replaced (#194). Re-SETting the corpus repopulates the
/// index; a one-shot VEC.REINDEX is the obvious tool if that ever needs doing
/// against a corpus too big to resend, and is deliberately not built yet.
///
/// The index exists to make recovery O(vectors); the BUCKETING exists so no
/// single reply or single slot carries the whole corpus. One key per set would
/// put every id of a 1M-vector set in one value on one node — and our SSCAN
/// answers the whole collection in one reply, so recovery would hit the same
/// channel-budget wall the full-namespace SCAN did, just from the other side.
/// 256 keeps a 1M-vector set at ~4k ids per reply and spreads the write across
/// the slot space instead of making one hot key.
pub const INDEX_BUCKETS: u16 = 256;

/// The durable index of SET NAMES (`kind = b's'`). Unbucketed on purpose: a
/// tenant has a handful of vector sets, not millions, so this one is small.
pub fn sets_index_key() -> Vec<u8> {
    durable_key(b's', b"", b"")
}

/// Which bucket an id belongs to. FNV-1a, low byte — cheap, stable across
/// processes and architectures, and it needs no distribution guarantee beyond
/// "spreads", since a lopsided bucket costs a bigger reply and nothing else.
pub fn bucket_of(id: &[u8]) -> u16 {
    let mut h: u32 = 0x811c_9dc5;
    for b in id {
        h ^= *b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    (h % INDEX_BUCKETS as u32) as u16
}

/// The durable id-index key for one bucket of a set (`kind = b'i'`).
pub fn index_key(set: &[u8], bucket: u16) -> Vec<u8> {
    durable_key(b'i', set, format!("{bucket:03}").as_bytes())
}

/// The durable config key for a set (`kind = b'c'`) — public so the rebuild can
/// name it without re-deriving the layout.
pub fn config_key(set: &[u8]) -> Vec<u8> {
    durable_key(b'c', set, b"")
}

/// The durable row key for one vector (`kind = b'v'`).
pub fn vector_key(set: &[u8], id: &[u8]) -> Vec<u8> {
    durable_key(b'v', set, id)
}

/// Serialise a vector row for durability: an optional absolute-ms expiry header
/// (`@<ms>` then a 0x1e record-separator), the comma-joined floats, then the raw
/// metadata after a 0x1f unit-separator when present. The header is
/// backward-compatible — a row without a leading `@` is a no-TTL v0.1 row, and
/// floats never start with `@`. A packed-f32 encoding is a later, wire-
/// compatible swap.
fn encode_vec_row(vec: &[f32], meta: Option<&[u8]>, expires_at: Option<u64>) -> Vec<u8> {
    let mut v = Vec::new();
    if let Some(ms) = expires_at {
        v.push(b'@');
        v.extend_from_slice(ms.to_string().as_bytes());
        v.push(0x1e);
    }
    v.extend_from_slice(floats_to_ascii(vec).as_bytes());
    if let Some(m) = meta {
        v.push(0x1f);
        v.extend_from_slice(m);
    }
    v
}

/// The first byte of a packed vector row (ADR-0050 D5). A text row is ASCII
/// throughout, so none starts with it, and every row says which it is.
const PACKED_ROW: u8 = 0xF1;
/// A packed row's flags: an expiry follows the flags byte, and metadata ends
/// the row. Any other bit is a later format, refused rather than misread.
const PACKED_EXPIRES: u8 = 1;
const PACKED_META: u8 = 2;

/// Serialise a vector row packed (ADR-0050 D5): [`PACKED_ROW`], a flags byte,
/// the expiry as a little-endian u64 when flagged, the dimension count as a
/// little-endian u32, the floats as little-endian f32, then the metadata when
/// flagged. A 1536-d row is 6,150 bytes, where text takes about 19,000.
fn encode_packed_row(vec: &[f32], meta: Option<&[u8]>, expires_at: Option<u64>) -> Vec<u8> {
    let mut flags = 0;
    if expires_at.is_some() {
        flags |= PACKED_EXPIRES;
    }
    if meta.is_some() {
        flags |= PACKED_META;
    }
    let mut v = Vec::with_capacity(14 + vec.len() * 4 + meta.map_or(0, <[u8]>::len));
    v.push(PACKED_ROW);
    v.push(flags);
    if let Some(ms) = expires_at {
        v.extend_from_slice(&ms.to_le_bytes());
    }
    v.extend_from_slice(&(vec.len() as u32).to_le_bytes());
    for f in vec {
        v.extend_from_slice(&f.to_le_bytes());
    }
    if let Some(m) = meta {
        v.extend_from_slice(m);
    }
    v
}

/// Inverse of [`encode_packed_row`]. Every length is checked: a row that is
/// short, long or flagged with a bit this binary does not know is an error,
/// never a vector read from the wrong bytes.
fn decode_packed_row(val: &[u8]) -> Result<DecodedVecRow, String> {
    let flags = *val.get(1).ok_or("truncated packed vector row")?;
    if flags & !(PACKED_EXPIRES | PACKED_META) != 0 {
        return Err(format!("packed vector row has unknown flags {flags:#04x}"));
    }
    let mut rest = &val[2..];
    let expires_at = if flags & PACKED_EXPIRES != 0 {
        let (ms, r) = rest
            .split_first_chunk::<8>()
            .ok_or("truncated packed vector expiry")?;
        rest = r;
        Some(u64::from_le_bytes(*ms))
    } else {
        None
    };
    let (n, rest) = rest
        .split_first_chunk::<4>()
        .ok_or("truncated packed vector length")?;
    let n = u32::from_le_bytes(*n) as usize;
    if n == 0 {
        return Err("empty vector".into());
    }
    let len = n
        .checked_mul(4)
        .filter(|&b| b <= rest.len())
        .ok_or("truncated packed vector")?;
    let (floats, tail) = rest.split_at(len);
    let (words, _) = floats.as_chunks::<4>();
    let vec = words.iter().map(|w| f32::from_le_bytes(*w)).collect();
    let meta = if flags & PACKED_META != 0 {
        Some(tail.to_vec())
    } else if tail.is_empty() {
        None
    } else {
        return Err("packed vector row has bytes after its vector".into());
    };
    Ok((vec, meta, expires_at))
}

fn floats_to_ascii(vec: &[f32]) -> String {
    vec.iter()
        .map(|f| f.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

/// Rough RAM an indexed vector costs — what the D4 cap meters against, since no
/// allocator hands back a live per-key heap figure cheaply. Deliberately a
/// slight OVER-estimate (the guard should trip before a real OOM, not after):
/// the vector (dim×4) + id + meta + a fixed per-engine structural cost, where
/// HNSW's neighbour lists dwarf flat's hashmap slot. `tests/meter.rs` holds it
/// to that, against a counting allocator, for every kind of set (BUG-0198).
fn entry_bytes(
    dim: usize,
    id_len: usize,
    meta_len: usize,
    kind: IndexKind,
    quant: Quant,
    on_disk: bool,
) -> usize {
    // Measured by counting allocations (`tests/meter.rs`, BUG-0198) with the
    // containers just past a doubling, their worst case: beyond the vector,
    // id and meta, a flat entry held 163 B and an HNSW node 284 B. These were
    // 64 and 256, and the meter charged up to a third less than a set held.
    let structural = match kind {
        // Entry { vec, norm, meta, vals }, its key, and its slot in the map.
        IndexKind::Flat => 192,
        // A slot in each of the node arrays (ADR-0049 step 3), its layer-0
        // links among them, a share of the upper layers' map, and the id map.
        // 457 B before step 3 flattened the layout.
        IndexKind::Hnsw => 320,
    };
    // The full vector for the re-rank, when it is in RAM: with no
    // `--vec-dir`, a quantized set costs MORE than a plain one, and this says
    // so. A box of its own, pointer included. In the local file (ADR-0049 D2)
    // it costs no RAM; a vector whose write failed and stayed in RAM is
    // charged separately (`VectorSet::spill_bytes`).
    let full = if on_disk { 0 } else { dim * 4 + 16 };
    let vector = match quant {
        Quant::None => dim * 4,
        // The 8-bit code and its (lo, step), and a fifth of the code again.
        // The code is a box of its own, and at 999,000 real 1536-d vectors
        // the process's RSS grew 280 B a vector past the heap it requested
        // (265 B at 50,000; 15 B at 384-d): what the allocator loses around
        // that many 1,536-byte boxes among the larger buffers each write
        // frees. Charging it keeps the cap a bound on RSS, not just on the
        // heap (ADR-0049 verification 1).
        Quant::Sq8 => dim + 8 + dim / 5 + full,
        // The 1-bit code and its factor, in arrays a fixed width a slot that
        // grow by a quarter: up to a quarter more, spare.
        Quant::Bin => (quant::bin_bytes(dim) + 4) * 5 / 4 + full,
    };
    vector + id_len + meta_len + structural
}

/// Parse a query/stored vector from one bulk argument: comma-separated floats,
/// optionally wrapped in `[ ]`. (`VEC.SET set id "0.1,0.2,0.3"`.) A packed-f32
/// blob is a later addition behind the same argument.
fn parse_vector(bytes: &[u8]) -> Result<Vec<f32>, String> {
    let s = std::str::from_utf8(bytes).map_err(|_| "vector is not valid UTF-8".to_string())?;
    let s = s.trim();
    let s = s.strip_prefix('[').unwrap_or(s);
    let s = s.strip_suffix(']').unwrap_or(s);
    let mut out = Vec::new();
    for part in s.split(',') {
        let p = part.trim();
        if p.is_empty() {
            continue;
        }
        out.push(
            p.parse::<f32>()
                .map_err(|_| format!("not a float: {p:?}"))?,
        );
    }
    if out.is_empty() {
        return Err("empty vector".into());
    }
    // Collected without knowing the count, the Vec grew by doubling: at 1536
    // dimensions it held room for 2048, a third more than the vector, for as
    // long as the set keeps it (BUG-0198).
    out.shrink_to_fit();
    Ok(out)
}

/// Inverse of `durable_key`: split a co-processor key into `(kind, set, id)`,
/// or `None` for any key not under [`KEY_PREFIX`] (a foreign key the rebuild
/// scan skips). `id` is empty for a config key. Used by the cold-start rebuild
/// to reconstruct the index from the durable rows (ADR-0017 D3).
pub fn parse_durable_key(key: &[u8]) -> Option<(u8, Vec<u8>, Vec<u8>)> {
    let rest = key.strip_prefix(KEY_PREFIX)?;
    if rest.len() < 2 || rest[1] != 0 {
        return None; // expected <kind><NUL>...
    }
    let kind = rest[0];
    let after = &rest[2..];
    match after.iter().position(|&b| b == 0) {
        Some(i) => Some((kind, after[..i].to_vec(), after[i + 1..].to_vec())),
        None => Some((kind, after.to_vec(), Vec::new())),
    }
}

/// A decoded durable vector row: `(vector, optional meta, optional absolute
/// expiry in ms since epoch)`. See [`decode_vec_row`].
pub type DecodedVecRow = (Vec<f32>, Option<Vec<u8>>, Option<u64>);

/// Inverse of `encode_vec_row` and `encode_packed_row`: `(vector, optional
/// meta, optional expiry-ms)` from a durable vector value. A row starting with
/// the byte `0xF1` is packed (ADR-0050 D5); any other is text, where a leading
/// `@<ms>` + 0x1e is the optional expiry header and its absence is a no-TTL
/// v0.1 row. Both are read whichever this binary writes, so a binary one
/// release older than the first to write packed rows still reads them.
pub fn decode_vec_row(val: &[u8]) -> Result<DecodedVecRow, String> {
    if val.first() == Some(&PACKED_ROW) {
        return decode_packed_row(val);
    }
    let (expires_at, rest) = if val.first() == Some(&b'@') {
        let i = val
            .iter()
            .position(|&b| b == 0x1e)
            .ok_or("truncated vector expiry header")?;
        let ms = std::str::from_utf8(&val[1..i])
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or("bad vector expiry header")?;
        (Some(ms), &val[i + 1..])
    } else {
        (None, val)
    };
    let (floats, meta) = match rest.iter().position(|&b| b == 0x1f) {
        Some(i) => (&rest[..i], Some(rest[i + 1..].to_vec())),
        None => (rest, None),
    };
    Ok((parse_vector(floats)?, meta, expires_at))
}

/// A decoded config row: `(dim, metric, index kind, quantization, fields)`.
pub type SetConfig = (usize, Metric, IndexKind, Quant, Vec<Field>);

/// Inverse of a config row: `<dim>|<metric>[|<index>[|<quant>[|<fields>]]]`.
/// A 2-field config (written before v0.2 added the index kind) is flat, a
/// 3-field one (before ADR-0049) is unquantized, and a 4-field one (before
/// ADR-0059) declares no fields.
///
/// A code this binary does not know is a later binary's (ADR-0049 item 2 adds
/// codes): the set loads unquantized, from the same full durable rows, as a
/// binary from before ADR-0049 loads a `sq8` one. Refusing the row instead
/// would drop the set from the index on a rollback, and a `VEC.CREATE` while
/// rolled back could then replace its config.
pub fn decode_config(val: &[u8]) -> Option<SetConfig> {
    let s = std::str::from_utf8(val).ok()?;
    let mut it = s.split('|');
    let dim = it.next()?.parse().ok()?;
    let metric = Metric::parse(it.next()?.as_bytes())?;
    let index = match it.next() {
        Some(k) => IndexKind::parse(k.as_bytes())?,
        None => IndexKind::Flat,
    };
    let quant = it
        .next()
        .and_then(|q| Quant::parse(q.as_bytes()))
        .unwrap_or(Quant::None);
    let fields = it.next().map(filter::decode_fields).unwrap_or_default();
    Some((dim, metric, index, quant, fields))
}

/// The config row for a set. A plain set keeps the 3-field form, byte for byte
/// what every earlier binary wrote; only a quantized set adds the fourth, and
/// only a set with fields the fifth (ADR-0059), with an unquantized set's
/// fourth written `none`. Every binary from v0.2 on reads the first four and
/// passes over the fifth, so a set with fields loads, unfiltered, on a
/// release from before them.
fn encode_config(
    dim: usize,
    metric: Metric,
    index: IndexKind,
    quant: Quant,
    fields: &[Field],
) -> Vec<u8> {
    let (m, i) = (metric.as_str(), index.as_str());
    match (quant, fields) {
        (Quant::None, []) => format!("{dim}|{m}|{i}"),
        (q, []) => format!("{dim}|{m}|{i}|{}", q.as_str()),
        (q, f) => format!("{dim}|{m}|{i}|{}|{}", q.as_str(), filter::encode_fields(f)),
    }
    .into_bytes()
}

fn valid_name(b: &[u8]) -> bool {
    !b.is_empty() && !b.contains(&0)
}

fn err(msg: &str) -> Value {
    Value::Error(msg.to_string())
}

/// The cap a write would pass ([`Store::over_cap`]).
enum Cap {
    /// The namespace's (D4), with the bytes it already uses.
    Namespace(usize),
    /// Every namespace's together (ADR-0050 D2).
    Server,
}

/// Why a rebuild left a durable vector row out of the index
/// ([`Store::install_vector`], BUG-0265).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeftOut {
    /// An empty set or id, or one with a NUL in it.
    BadName,
    /// No config row this binary could read names its set.
    NoSet,
    /// Its length is not its set's `DIM`.
    WrongDim,
}

impl LeftOut {
    /// How the rebuild's log line says it.
    pub fn reason(self) -> &'static str {
        match self {
            LeftOut::BadName => "with an empty set or id",
            LeftOut::NoSet => "whose set has no config row",
            LeftOut::WrongDim => "whose length is not their set's DIM",
        }
    }
}

/// The refusal of `FIELD` or `FILTER` by a co-processor without
/// `--vec-filters`.
const FILTERS_OFF: &str = "ERR vector filters (FIELD and FILTER) are off on this server";

/// The operator's view of a [`Store`], from [`Store::stats`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreStats {
    /// Estimated index bytes over every namespace, what the overall cap meters.
    pub index_bytes: usize,
    /// The per-namespace cap and the overall cap; 0 = none.
    pub index_cap: usize,
    pub total_cap: usize,
    /// Namespaces holding any index, their sets, and the vectors in them.
    pub namespaces: usize,
    pub sets: usize,
    pub vectors: usize,
    /// Writes refused by the per-namespace cap and by the overall cap.
    pub refused_ns: u64,
    pub refused_total: u64,
    /// Whether durable rows are written packed (ADR-0050 D5).
    pub packed_rows: bool,
    /// Whether `VEC.CREATE ... FIELD` and `VEC.SEARCH ... FILTER` are served
    /// (ADR-0059, `--vec-filters`).
    pub filters: bool,
    /// Whether `FT.*` is served (ADR-0060, `--ft`), its indexes over every
    /// namespace, and the documents in them.
    pub ft: bool,
    pub ft_indexes: usize,
    pub ft_docs: usize,
}

/// The co-processor's per-tenant, per-set state. One `Store` per running
/// co-processor; tenants are isolated by the namespace component of the key
/// (ADR-0017 D1/D4).
#[derive(Default)]
pub struct Store {
    sets: HashMap<(Vec<u8>, Vec<u8>), VectorSet>,
    /// Per-namespace estimated index bytes — what the D4 cap meters against.
    /// The index lives only in this co-processor's RAM, so one tenant's set
    /// growing without bound would starve every other tenant's search; this is
    /// the accounting that stops it.
    ns_bytes: HashMap<Vec<u8>, usize>,
    /// Per-namespace index-memory cap in bytes; 0 = unlimited (opt-in, set by
    /// the binary from `--index-mem-bytes`).
    index_cap: usize,
    /// Cap on every namespace's index together, in bytes; 0 = unlimited
    /// (ADR-0050 D2, `--index-mem-total-bytes`). The co-processor shares its
    /// host with the cache's own seats, and the per-namespace cap alone lets
    /// enough tenants take all of it.
    total_cap: usize,
    /// Writes refused by each cap, for the operator's `FLINTINFO`.
    refused_ns: AtomicU64,
    refused_total: AtomicU64,
    /// Where a quantized set keeps its full vectors (ADR-0049 D2, the binary's
    /// `--vec-dir`); `None` keeps them in RAM.
    vec_dir: Option<PathBuf>,
    /// Vector files made, which names the next one.
    vec_files: u64,
    /// Write durable vector rows packed rather than as text (ADR-0050 D5).
    /// Off for a bare `Store`; the binary turns it on unless started with
    /// `--write-text-rows`, from the release after the one that first read
    /// packed rows.
    packed_rows: bool,
    /// How many times each namespace has been dropped ([`Store::drop_namespace`]).
    generations: HashMap<Vec<u8>, u64>,
    /// Serve `FIELD` and `FILTER` (ADR-0059). Off for a bare `Store`; the
    /// binary turns it on unless started with `--no-vec-filters`, from the
    /// release after the one that first knew them, which served them only
    /// with `--vec-filters`: one before that answers a `FILTER` as if there
    /// were none. A set that already has fields keeps them either way.
    filters: bool,
    /// How many matches a filtered HNSW search scores one by one before it
    /// walks the graph instead; `None` is the rule in `hnsw::scan_threshold`.
    /// Set only to measure where that rule should be (`bench --filters`).
    scan_cap: Option<usize>,
    /// The `FT.*` indexes (ADR-0060), by namespace and name.
    ft: HashMap<(Vec<u8>, Vec<u8>), ftindex::FtIndex>,
    /// The `FT.*` aliases, by namespace and alias, each to its index's name
    /// (ADR-0060 amendment 3, part 4).
    ft_aliases: HashMap<(Vec<u8>, Vec<u8>), Vec<u8>>,
    /// Serve `FT.*`. Off for a bare `Store`; the binary turns it on with
    /// `--ft`, in the release that brings it.
    ft_on: bool,
}

impl Store {
    pub fn new() -> Self {
        Store::default()
    }

    /// Cap the RAM one tenant's indexes may consume (ADR-0017 D4). 0 =
    /// unlimited. A `VEC.SET` that would push a namespace past this is refused
    /// with `-VECFULL` BEFORE its durable write — so nothing is ever
    /// stored-but-unsearchable. A `VEC.DEL` frees space for the same set's
    /// next writes, and in a flat set at once; an HNSW set's deleted slot is
    /// held, and charged, until one of them takes it or the set is emptied
    /// (BUG-0257).
    pub fn set_index_cap(&mut self, bytes: usize) {
        self.index_cap = bytes;
    }

    /// Cap every namespace's index together (ADR-0050 D2). 0 = unlimited. A
    /// `VEC.SET` that would take the sum past it is refused with `-VECFULL`
    /// before its durable write, as the per-namespace cap refuses.
    pub fn set_index_total_cap(&mut self, bytes: usize) {
        self.total_cap = bytes;
    }

    /// What the operator's `FLINTINFO` reports. Totals over every tenant, so
    /// it is never answered to one.
    pub fn stats(&self) -> StoreStats {
        StoreStats {
            index_bytes: self.ns_bytes.values().sum(),
            index_cap: self.index_cap,
            total_cap: self.total_cap,
            namespaces: self.ns_bytes.values().filter(|&&b| b > 0).count(),
            sets: self.sets.len(),
            vectors: self.sets.values().map(VectorSet::len).sum(),
            refused_ns: self.refused_ns.load(Relaxed),
            refused_total: self.refused_total.load(Relaxed),
            packed_rows: self.packed_rows,
            filters: self.filters,
            ft: self.ft_on,
            ft_indexes: self.ft.len(),
            ft_docs: self.ft.values().map(ftindex::FtIndex::len).sum(),
        }
    }

    /// Serve `VEC.CREATE ... FIELD` and `VEC.SEARCH ... FILTER` (ADR-0059).
    pub fn set_filters(&mut self, on: bool) {
        self.filters = on;
    }

    /// Fix how many matches a filtered HNSW search scores one by one, in place
    /// of the square-root rule, or restore the rule with `None`. For
    /// measurement only: `bench --filters` forces each path in turn to find
    /// where they cost the same (ADR-0059's verification).
    pub fn set_scan_threshold(&mut self, cap: Option<usize>) {
        self.scan_cap = cap;
    }

    /// Write durable vector rows packed (ADR-0050 D5). Rows already written
    /// stay as they are until rewritten; both kinds are always read.
    pub fn set_packed_rows(&mut self, on: bool) {
        self.packed_rows = on;
    }

    /// The durable row for a vector, in the encoding this store writes.
    fn vec_row(&self, vec: &[f32], meta: Option<&[u8]>, expires_at: Option<u64>) -> Vec<u8> {
        if self.packed_rows {
            encode_packed_row(vec, meta, expires_at)
        } else {
            encode_vec_row(vec, meta, expires_at)
        }
    }

    /// Keep each quantized set's full vectors in a file in `dir` (ADR-0049
    /// D2). The binary owns `dir`: it locks it and empties it of vector files
    /// before calling this.
    pub fn set_vec_dir(&mut self, dir: PathBuf) {
        self.vec_dir = Some(dir);
    }

    /// A new set, with a vector file when it is quantized and there is a
    /// `vec_dir`. A file that cannot be created leaves the vectors in RAM,
    /// where the meter charges them, rather than refusing the set.
    fn new_set(
        &mut self,
        dim: usize,
        metric: Metric,
        index: IndexKind,
        quant: Quant,
        fields: Vec<Field>,
    ) -> VectorSet {
        let mut vs = VectorSet::new(dim, metric, index, quant).with_fields(fields);
        if let Some(dir) = &self.vec_dir
            && index == IndexKind::Hnsw
            && quant != Quant::None
        {
            let path = dir.join(format!("{}.{}", self.vec_files, vecfile::EXTENSION));
            self.vec_files += 1;
            if let Ok(f) = VecFile::create(&path, dim) {
                vs.attach_vec_file(f);
            }
        }
        vs
    }

    /// Forget everything held for `ns`: its sets, their vector files and its
    /// index bytes (BUG-0255). A tenant's removal wipes its namespace's durable
    /// rows, and a namespace name can be given to a new tenant, so an index
    /// kept here past the removal served the old tenant's vectors to the new
    /// one. Returns how many sets were dropped. The namespace's next touch
    /// rebuilds from whatever durable rows it has.
    ///
    /// Also bumps `ns`'s [`Store::generation`], so a rebuild that was already
    /// reading rows for it installs none of them.
    pub fn drop_namespace(&mut self, ns: &[u8]) -> usize {
        let before = self.sets.len();
        self.sets.retain(|(n, _), _| n.as_slice() != ns);
        self.ft.retain(|(n, _), _| n.as_slice() != ns);
        self.ft_aliases.retain(|(n, _), _| n.as_slice() != ns);
        self.ns_bytes.remove(ns);
        *self.generations.entry(ns.to_vec()).or_default() += 1;
        before - self.sets.len()
    }

    /// How many times `ns` has been dropped. A rebuild reads it when it starts,
    /// and installs only while it is unchanged.
    pub fn generation(&self, ns: &[u8]) -> u64 {
        self.generations.get(ns).copied().unwrap_or(0)
    }

    /// Start an emptied set again (BUG-0255). A deleted vector's slot waits
    /// for the set's next insert (BUG-0197), so a set emptied by `VEC.DEL` or
    /// by its TTLs kept every slot's memory, and its vector file at its
    /// largest, until the co-processor restarted. With nothing left in it,
    /// the set keeps its configuration and its index and file start again.
    fn reset_set(&mut self, key: &(Vec<u8>, Vec<u8>)) {
        let Some(vs) = self.sets.get(key) else {
            return;
        };
        let (dim, metric, kind, quant) = (vs.dim(), vs.metric(), vs.kind(), vs.quant());
        let fields = vs.fields().to_vec();
        // The old set and its file go before the new file is made.
        self.sets.remove(key);
        let fresh = self.new_set(dim, metric, kind, quant, fields);
        self.sets.insert(key.clone(), fresh);
    }

    /// Estimated index bytes currently attributed to `ns` (for INFO/metrics).
    pub fn ns_mem_bytes(&self, ns: &[u8]) -> usize {
        self.ns_bytes.get(ns).copied().unwrap_or(0)
    }

    /// Number of sets held (across all tenants) — for INFO/metrics.
    pub fn set_count(&self) -> usize {
        self.sets.len()
    }

    /// Plan one `VEC.*` command for tenant `ns` WITHOUT mutating the index.
    /// `args[0]` is the command name. Reads return [`Plan::Reply`]; writes
    /// return [`Plan::Write`] for the caller to persist-then-`commit`.
    pub fn plan(&self, ns: &[u8], args: &[Vec<u8>], now: u64) -> Plan {
        let Some(name) = args.first() else {
            return Plan::Reply(err("ERR empty command"));
        };
        match name.to_ascii_uppercase().as_slice() {
            b"VEC.CREATE" => self.plan_create(ns, args),
            b"VEC.SET" => self.plan_set(ns, args, now),
            b"VEC.GET" => Plan::Reply(self.get(ns, args, now)),
            b"VEC.DEL" => self.plan_del(ns, args),
            b"VEC.SEARCH" => Plan::Reply(self.search(ns, args, now)),
            b"VEC.INFO" => Plan::Reply(self.info(ns, args, now)),
            b"VEC.TTL" => Plan::Reply(self.ttl(ns, args, now)),
            b"VEC.EXPIRE" => self.plan_expire(ns, args, now, 1000),
            b"VEC.PEXPIRE" => self.plan_expire(ns, args, now, 1),
            b"VEC.PERSIST" => self.plan_persist(ns, args, now),
            other => Plan::Reply(err(&format!(
                "ERR unknown VEC command '{}'",
                String::from_utf8_lossy(other)
            ))),
        }
    }

    /// Apply a planned write's index mutation, AFTER its durable write landed.
    pub fn commit(&mut self, ns: &[u8], apply: Apply) {
        match apply {
            Apply::CreateSet {
                set,
                dim,
                metric,
                index,
                quant,
                fields,
            } => {
                let key = (ns.to_vec(), set);
                if !self.sets.contains_key(&key) {
                    let vs = self.new_set(dim, metric, index, quant, fields);
                    self.sets.insert(key, vs);
                }
            }
            Apply::Insert {
                set,
                id,
                vec,
                meta,
                expires_at,
            } => {
                // Charge the byte delta to the namespace (new entry, or the meta
                // change on an upsert), computed BEFORE the set replaces it.
                // A failed vector-file write keeps the vector in RAM, which is
                // charged as the change in `spill_bytes`.
                // Deleted slots are charged until taken again (BUG-0257), so a
                // new id that takes one is charged its difference.
                let mut charge: Option<(usize, usize)> = None;
                if let Some(vs) = self.sets.get_mut(&(ns.to_vec(), set)) {
                    let old = vs.entry_cost(&id).unwrap_or(0) + vs.held();
                    vs.set(id.clone(), vec, meta, expires_at);
                    let new = vs.entry_cost(&id).unwrap_or(0) + vs.held();
                    charge = Some((new, old));
                }
                if let Some((new, old)) = charge {
                    let e = self.ns_bytes.entry(ns.to_vec()).or_default();
                    *e = e.saturating_add(new).saturating_sub(old);
                }
            }
            Apply::Remove { set, id } => {
                // An HNSW entry's slot stays charged as a deleted slot until it
                // is taken again or the set is emptied (BUG-0257); a flat
                // entry is freed now.
                let key = (ns.to_vec(), set);
                let mut freed = 0usize;
                let mut emptied = false;
                if let Some(vs) = self.sets.get_mut(&key) {
                    let before = vs.entry_cost(&id).unwrap_or(0) + vs.held();
                    emptied = vs.del(&id) && vs.is_empty();
                    let after = if emptied { 0 } else { vs.held() };
                    freed = before.saturating_sub(after);
                }
                if emptied {
                    self.reset_set(&key);
                }
                if freed > 0
                    && let Some(e) = self.ns_bytes.get_mut(ns)
                {
                    *e = e.saturating_sub(freed);
                }
            }
        }
    }

    /// Remove every id whose TTL has passed as of `now`, across all tenants, and
    /// credit what that frees to each namespace: a flat set's entries, and an
    /// HNSW set's slots only when it is emptied, since until then they are
    /// held for its next inserts (BUG-0257). Returns how many vectors were
    /// removed, with `FT.*` documents. This is a RAM-reclamation path, not a correctness
    /// one: `get`/`search` already mask an expired id the instant its deadline
    /// passes, and the durable row carries its own PX, so a sweep that runs late
    /// only holds memory longer — it can never serve or resurrect an expired
    /// vector. Called on a timer by the networked layer.
    pub fn sweep_expired(&mut self, now: u64) -> usize {
        let mut swept = 0usize;
        let mut emptied = Vec::new();
        for (key, vs) in self.sets.iter_mut() {
            let ids = vs.expired_ids(now);
            if ids.is_empty() {
                continue;
            }
            let ns = &key.0;
            let mut before = vs.held();
            for id in &ids {
                before += vs.entry_cost(id).unwrap_or(0);
                vs.del(id);
            }
            swept += ids.len();
            // As VEC.DEL: deleted HNSW slots stay charged, unless the set is
            // now empty and starts again (BUG-0257).
            let after = if vs.is_empty() {
                emptied.push(key.clone());
                0
            } else {
                vs.held()
            };
            let freed = before.saturating_sub(after);
            if freed > 0
                && let Some(e) = self.ns_bytes.get_mut(ns)
            {
                *e = e.saturating_sub(freed);
            }
        }
        for key in &emptied {
            self.reset_set(key);
        }
        swept + self.ft_sweep(now)
    }

    /// VEC.CREATE <set> DIM <d> METRIC <cosine|l2|ip> [INDEX flat|hnsw]
    /// [QUANT none|sq8|bin] [FIELD <name> TAG|NUMERIC ...]
    fn plan_create(&self, ns: &[u8], args: &[Vec<u8>]) -> Plan {
        if args.len() < 6 {
            return Plan::Reply(err("ERR VEC.CREATE <set> DIM <d> METRIC <cosine|l2|ip>"));
        }
        let set = &args[1];
        if !valid_name(set) {
            return Plan::Reply(err("ERR invalid set name"));
        }
        if !args[2].eq_ignore_ascii_case(b"DIM") || !args[4].eq_ignore_ascii_case(b"METRIC") {
            return Plan::Reply(err("ERR VEC.CREATE <set> DIM <d> METRIC <cosine|l2|ip>"));
        }
        let dim = match std::str::from_utf8(&args[3])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(d) if d >= 1 => d,
            _ => return Plan::Reply(err("ERR DIM must be a positive integer")),
        };
        let Some(metric) = Metric::parse(&args[5]) else {
            return Plan::Reply(err("ERR METRIC must be one of cosine, l2, ip"));
        };
        // Optional trailing `INDEX flat|hnsw` (default flat, exact, the oracle),
        // `QUANT none|sq8|bin` (ADR-0049; HNSW only) and any number of
        // `FIELD <name> TAG|NUMERIC` (ADR-0059), in any order.
        let mut index = IndexKind::Flat;
        let mut quant = Quant::None;
        let mut fields: Vec<Field> = Vec::new();
        let mut i = 6;
        while i < args.len() {
            let Some(val) = args.get(i + 1) else {
                return Plan::Reply(err("ERR VEC.CREATE option needs a value"));
            };
            match args[i].to_ascii_uppercase().as_slice() {
                b"INDEX" => match IndexKind::parse(val) {
                    Some(k) => index = k,
                    None => return Plan::Reply(err("ERR INDEX must be flat or hnsw")),
                },
                b"QUANT" => match Quant::parse(val) {
                    Some(q) => quant = q,
                    None => return Plan::Reply(err("ERR QUANT must be none, sq8 or bin")),
                },
                b"FIELD" => {
                    if !self.filters {
                        return Plan::Reply(err(FILTERS_OFF));
                    }
                    let Some(kind) = args.get(i + 2) else {
                        return Plan::Reply(err("ERR FIELD <name> TAG|NUMERIC"));
                    };
                    if !filter::valid_name(val) {
                        return Plan::Reply(err(&format!(
                            "ERR FIELD name must be letters, digits and _, at most {} of them",
                            filter::MAX_NAME
                        )));
                    }
                    let Some(kind) = FieldKind::parse(kind) else {
                        return Plan::Reply(err("ERR FIELD type must be TAG or NUMERIC"));
                    };
                    let name = String::from_utf8_lossy(val);
                    if fields.iter().any(|f| *f.name == *name) {
                        return Plan::Reply(err(&format!("ERR FIELD {name} is given twice")));
                    }
                    if fields.len() == filter::MAX_FIELDS {
                        return Plan::Reply(err(&format!(
                            "ERR a set has at most {} FIELDs",
                            filter::MAX_FIELDS
                        )));
                    }
                    fields.push(Field {
                        name: name.into(),
                        kind,
                    });
                    i += 1;
                }
                _ => {
                    return Plan::Reply(err(
                        "ERR unknown VEC.CREATE option (want INDEX, QUANT or FIELD)",
                    ));
                }
            }
            i += 2;
        }
        if quant != Quant::None && index != IndexKind::Hnsw {
            // Flat is the exact recall oracle; a quantized flat would be neither.
            return Plan::Reply(err("ERR QUANT needs INDEX hnsw"));
        }
        if self.sets.contains_key(&(ns.to_vec(), set.to_vec())) {
            return Plan::Reply(err("ERR set already exists"));
        }
        Plan::Write {
            persist: vec![
                // The set NAME first: recovery starts from this index, so a
                // config row it does not list is a set the rebuild cannot find.
                Persist::SAdd {
                    key: sets_index_key(),
                    member: set.to_vec(),
                },
                Persist::Put {
                    key: durable_key(b'c', set, b""),
                    val: encode_config(dim, metric, index, quant, &fields),
                    // A set's config row never expires; TTL is per-vector.
                    ttl_ms: None,
                },
            ],
            apply: Apply::CreateSet {
                set: set.to_vec(),
                dim,
                metric,
                index,
                quant,
                fields,
            },
            ok: Value::Simple("OK".into()),
        }
    }

    /// VEC.SET <set> <id> <vec> [META <json>] [EX <secs> | PX <ms>]
    fn plan_set(&self, ns: &[u8], args: &[Vec<u8>], now: u64) -> Plan {
        if args.len() < 4 {
            return Plan::Reply(err(
                "ERR VEC.SET <set> <id> <vec> [META <json>] [EX secs | PX ms]",
            ));
        }
        let (set, id, raw) = (&args[1], &args[2], &args[3]);
        if !valid_name(set) || !valid_name(id) {
            return Plan::Reply(err("ERR invalid set or id (must be non-empty, no NUL)"));
        }
        // Optional trailing options in any order: META <json>, and at most one of
        // EX <secs> / PX <ms> (the ephemeral-memory TTL). EX/PX mirror SET: a
        // re-SET without one makes the id permanent again (handled in `set`).
        let mut meta: Option<Vec<u8>> = None;
        let mut ttl_ms: Option<u64> = None;
        let mut i = 4;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"META" => {
                    let Some(val) = args.get(i + 1) else {
                        return Plan::Reply(err("ERR META needs a value"));
                    };
                    meta = Some(val.clone());
                    i += 2;
                }
                b"EX" | b"PX" => {
                    if ttl_ms.is_some() {
                        return Plan::Reply(err("ERR EX and PX are mutually exclusive"));
                    }
                    let n = match args
                        .get(i + 1)
                        .and_then(|a| std::str::from_utf8(a).ok())
                        .and_then(|s| s.parse::<u64>().ok())
                    {
                        Some(n) if n > 0 => n,
                        _ => return Plan::Reply(err("ERR EX/PX needs a positive integer")),
                    };
                    ttl_ms = Some(if opt.eq_ignore_ascii_case(b"EX") {
                        n.saturating_mul(1000)
                    } else {
                        n
                    });
                    i += 2;
                }
                _ => {
                    return Plan::Reply(err("ERR unknown VEC.SET option (want META, EX, or PX)"));
                }
            }
        }
        // Absolute deadline off the caller-supplied clock, so plan/commit and the
        // durable row all agree on one instant.
        let expires_at = ttl_ms.map(|t| now.saturating_add(t));
        let Some(vs) = self.sets.get(&(ns.to_vec(), set.to_vec())) else {
            return Plan::Reply(err("ERR no such set (VEC.CREATE it first)"));
        };
        let vec = match parse_vector(raw) {
            Ok(v) => v,
            Err(e) => return Plan::Reply(err(&format!("ERR {e}"))),
        };
        if vec.len() != vs.dim() {
            return Plan::Reply(err(&format!(
                "WRONGDIM set is {}, vector is {}",
                vs.dim(),
                vec.len()
            )));
        }
        // D4: refuse a write that would push this tenant's index past its RAM
        // cap. Because the durable write happens BEFORE the index mutation, a
        // refusal here means the vector is not persisted either — a hard bound,
        // never an evict-and-desync. An UPSERT of an existing id only costs its
        // meta delta, so it is charged that, not a whole new entry.
        let new = vs.cost(id.len(), meta.as_ref().map_or(0, |m| m.len()))
            + vs.fields_charge(meta.as_deref());
        // A new id in an HNSW set takes a deleted slot when there is one,
        // whose charge it gives back (BUG-0257): deleting and writing within
        // one set needs no more room than before.
        let old = vs.entry_cost(id).unwrap_or_else(|| vs.reuse_credit());
        match self.over_cap(ns, new, old) {
            Some(Cap::Namespace(used)) => {
                self.refused_ns.fetch_add(1, Relaxed);
                return Plan::Reply(err(&format!(
                    "VECFULL index memory limit reached ({used} of {} bytes for this namespace)",
                    self.index_cap
                )));
            }
            // The refusal names no numbers, since they would be other tenants'.
            Some(Cap::Server) => {
                self.refused_total.fetch_add(1, Relaxed);
                return Plan::Reply(err(
                    "VECFULL vector index memory is full on this server; delete vectors from the set being written, or retry later",
                ));
            }
            None => {}
        }
        Plan::Write {
            persist: vec![
                // Index BEFORE row (see Plan::Write): a crash here leaves a
                // phantom id the rebuild skips, never a durable vector that
                // nothing points at. SADD is idempotent, so re-SETting an id
                // costs a no-op rather than a duplicate.
                Persist::SAdd {
                    key: index_key(set, bucket_of(id)),
                    member: id.to_vec(),
                },
                Persist::Put {
                    key: durable_key(b'v', set, id),
                    val: self.vec_row(&vec, meta.as_deref(), expires_at),
                    ttl_ms,
                },
            ],
            apply: Apply::Insert {
                set: set.to_vec(),
                id: id.to_vec(),
                vec,
                meta,
                expires_at,
            },
            ok: Value::Simple("OK".into()),
        }
    }

    /// Which cap, if any, a write that adds `new` bytes to `ns` and gives back
    /// `old` would take it past: the namespace's (D4), with what it uses, or
    /// every namespace's together (ADR-0050 D2), so vector indexes cannot take
    /// the RAM this host's seats need. Summed per write rather than kept as a
    /// running total: one add per namespace, and nothing to drift.
    fn over_cap(&self, ns: &[u8], new: usize, old: usize) -> Option<Cap> {
        if self.index_cap > 0 {
            let used = self.ns_bytes.get(ns).copied().unwrap_or(0);
            if used.saturating_add(new).saturating_sub(old) > self.index_cap {
                return Some(Cap::Namespace(used));
            }
        }
        if self.total_cap > 0 {
            let total: usize = self.ns_bytes.values().sum();
            if total.saturating_add(new).saturating_sub(old) > self.total_cap {
                return Some(Cap::Server);
            }
        }
        None
    }

    /// Install one vector a rebuild read from its durable row, or say why it
    /// is left out (BUG-0265). The rows are in the tenant's namespace, where
    /// the tenant can write them too: nothing reserves their prefix from it.
    /// So a rebuild holds each row to the shape `VEC.SET` would have: a set
    /// this binary loaded, a valid id and the set's `DIM`. Installed
    /// unchecked, one row of the wrong length in a `QUANT bin` set panicked
    /// under the store lock every tenant shares.
    ///
    /// The caps are NOT checked here: a rebuild loads every durable row
    /// (ADR-0017 D3), and a cap governs only new writes, so a cap lowered
    /// below what a namespace holds leaves its vectors searchable. That a
    /// planted row can take a namespace past its cap the same way is the half
    /// of BUG-0265 still open.
    pub fn install_vector(
        &mut self,
        ns: &[u8],
        set: Vec<u8>,
        id: Vec<u8>,
        vec: Vec<f32>,
        meta: Option<Vec<u8>>,
        expires_at: Option<u64>,
    ) -> Result<(), LeftOut> {
        self.check_vector(ns, &set, &id, vec.len())?;
        self.commit(
            ns,
            Apply::Insert {
                set,
                id,
                vec,
                meta,
                expires_at,
            },
        );
        Ok(())
    }

    /// What [`Store::install_vector`] checks, without the row: so a caller
    /// can name a row it leaves out without copying every row it installs.
    pub fn check_vector(
        &self,
        ns: &[u8],
        set: &[u8],
        id: &[u8],
        len: usize,
    ) -> Result<(), LeftOut> {
        if !valid_name(set) || !valid_name(id) {
            return Err(LeftOut::BadName);
        }
        let Some(vs) = self.sets.get(&(ns.to_vec(), set.to_vec())) else {
            return Err(LeftOut::NoSet);
        };
        if len != vs.dim() {
            return Err(LeftOut::WrongDim);
        }
        Ok(())
    }

    /// VEC.DEL <set> <id>
    fn plan_del(&self, ns: &[u8], args: &[Vec<u8>]) -> Plan {
        if args.len() < 3 {
            return Plan::Reply(err("ERR VEC.DEL <set> <id>"));
        }
        let (set, id) = (&args[1], &args[2]);
        let Some(vs) = self.sets.get(&(ns.to_vec(), set.to_vec())) else {
            return Plan::Reply(err("ERR no such set"));
        };
        if vs.contains(id) {
            Plan::Write {
                // Row first, then the index entry: a crash between them leaves
                // a phantom id, which the rebuild skips on a nil GET. The other
                // order would leave a row no index lists — harmless but leaked.
                persist: vec![
                    Persist::Del {
                        key: durable_key(b'v', set, id),
                    },
                    Persist::SRem {
                        key: index_key(set, bucket_of(id)),
                        member: id.to_vec(),
                    },
                ],
                apply: Apply::Remove {
                    set: set.to_vec(),
                    id: id.to_vec(),
                },
                ok: Value::Integer(1),
            }
        } else {
            Plan::Reply(Value::Integer(0))
        }
    }

    fn get(&self, ns: &[u8], args: &[Vec<u8>], now: u64) -> Value {
        if args.len() < 3 {
            return err("ERR VEC.GET <set> <id>");
        }
        let Some(vs) = self.sets.get(&(ns.to_vec(), args[1].to_vec())) else {
            return err("ERR no such set");
        };
        // An expired id reads as absent until the sweep removes it — the vector
        // is logically gone the instant its TTL passes, sweep or no sweep.
        if vs.is_expired(&args[2], now) {
            return Value::Null;
        }
        match vs.get(&args[2]) {
            Err(e) => err(&format!("ERR {e}")),
            Ok(None) => Value::Null,
            Ok(Some((vec, meta))) => {
                let mut row = vec![Value::Bulk(Some(floats_to_ascii(&vec).into_bytes()))];
                if let Some(m) = meta {
                    row.push(Value::Bulk(Some(m)));
                }
                Value::Array(Some(row))
            }
        }
    }

    /// VEC.SEARCH <set> <query> <k> [EF <n>] [RERANK <n>] [FILTER <expr>]
    fn search(&self, ns: &[u8], args: &[Vec<u8>], now: u64) -> Value {
        if args.len() < 4 {
            return err("ERR VEC.SEARCH <set> <query> <k> [EF <n>] [RERANK <n>] [FILTER <expr>]");
        }
        let Some(vs) = self.sets.get(&(ns.to_vec(), args[1].to_vec())) else {
            return err("ERR no such set");
        };
        let query = match parse_vector(&args[2]) {
            Ok(v) => v,
            Err(e) => return err(&format!("ERR {e}")),
        };
        if query.len() != vs.dim() {
            return err(&format!(
                "WRONGDIM set is {}, query is {}",
                vs.dim(),
                query.len()
            ));
        }
        let k = match std::str::from_utf8(&args[3])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(k) => k,
            None => return err("ERR k must be a non-negative integer"),
        };
        // Optional `EF <n>` (HNSW's recall/latency knob), `RERANK <n>` (how
        // many candidates a quantized set re-scores, ADR-0049) and `FILTER
        // <expr>` (ADR-0059), in any order. Flat ignores EF and RERANK.
        //
        // An option this binary does not know is refused. The releases before
        // this one passed over it, so a `FILTER` sent to one answered as if
        // there were none: a tenant scoping each user to its own folder would
        // have read every folder. Refusing it here is what lets the next
        // release turn filters on: a rollback from it lands on this one.
        let (mut ef, mut rerank, mut filter_src): (Option<usize>, Option<usize>, Option<&[u8]>) =
            (None, None, None);
        let mut i = 4;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            let name = String::from_utf8_lossy(&opt);
            let Some(val) = args.get(i + 1) else {
                return err(&format!("ERR {name} needs a value"));
            };
            let number = || {
                std::str::from_utf8(val)
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
            };
            match opt.as_slice() {
                b"EF" | b"RERANK" => {
                    let Some(n) = number() else {
                        return err(&format!("ERR {name} must be a non-negative integer"));
                    };
                    let slot = if opt == b"EF" { &mut ef } else { &mut rerank };
                    if slot.replace(n).is_some() {
                        return err(&format!("ERR {name} is given twice"));
                    }
                }
                b"FILTER" => {
                    if !self.filters {
                        return err(FILTERS_OFF);
                    }
                    if filter_src.replace(val).is_some() {
                        return err("ERR FILTER is given twice");
                    }
                }
                _ => return err("ERR unknown VEC.SEARCH option (want EF, RERANK or FILTER)"),
            }
            i += 2;
        }
        let filter = match filter_src.map(|f| filter::parse(f, vs.fields())) {
            None | Some(Ok(Expr::All)) => None,
            Some(Ok(e)) => Some(e),
            Some(Err(e)) => return err(&format!("ERR FILTER {e}")),
        };
        let ef = ef.unwrap_or(hnsw::EF_SEARCH_DEFAULT);
        let rerank = rerank.unwrap_or(k.saturating_mul(vs.quant().rerank_multiple()));
        // Expired-but-unswept ids still sit in the index and can outrank live
        // ones, so over-fetch by the expired count and drop them post-hoc: with
        // at most `expired` stale hits, k+expired candidates always yield k live
        // ones. Engine-agnostic — no flat/HNSW code knows about TTL.
        let expired = vs.expired_count(now);
        let found = match vs.search(
            &query,
            k.saturating_add(expired),
            ef,
            rerank.saturating_add(expired),
            filter.as_ref(),
            self.scan_cap,
        ) {
            Ok(found) => found,
            Err(e) => return err(&format!("ERR {e}")),
        };
        let rows: Vec<Value> = found
            .into_iter()
            .filter(|(id, _)| !vs.is_expired(id, now))
            .take(k)
            .map(|(id, score)| {
                Value::Array(Some(vec![
                    Value::Bulk(Some(id)),
                    Value::Double(score as f64),
                ]))
            })
            .collect();
        Value::Array(Some(rows))
    }

    fn info(&self, ns: &[u8], args: &[Vec<u8>], now: u64) -> Value {
        if args.len() < 2 {
            return err("ERR VEC.INFO <set>");
        }
        let Some(vs) = self.sets.get(&(ns.to_vec(), args[1].to_vec())) else {
            return err("ERR no such set");
        };
        let bulk = |s: &str| Value::Bulk(Some(s.as_bytes().to_vec()));
        Value::Array(Some(vec![
            bulk("dim"),
            Value::Integer(vs.dim() as i64),
            bulk("metric"),
            bulk(vs.metric().as_str()),
            bulk("index"),
            bulk(vs.index_name()),
            bulk("count"),
            Value::Integer(vs.len() as i64),
            // How many of `count` carry a live TTL — the ephemeral-memory feature
            // made observable (D7), so a tenant sees permanent vs expiring at a
            // glance without walking the set.
            bulk("expiring"),
            Value::Integer(vs.ttl_count(now) as i64),
            // Namespace-wide (D4): this tenant's estimated index RAM and its cap
            // (0 = unlimited). Reported here so a tenant can see the headroom.
            bulk("ns_mem_bytes"),
            Value::Integer(self.ns_mem_bytes(ns) as i64),
            bulk("ns_mem_cap"),
            Value::Integer(self.index_cap as i64),
            // Last, so every earlier field keeps its position (ADR-0049).
            bulk("quant"),
            bulk(vs.quant().as_str()),
            // Where the full vectors are (step 2): "disk" is the local file,
            // which costs `disk_bytes` and no RAM; `ram_spill_bytes` is RAM
            // held for vectors whose file write failed.
            bulk("vectors_on"),
            bulk(if vs.vectors_on_disk() { "disk" } else { "ram" }),
            bulk("disk_bytes"),
            Value::Integer(vs.disk_bytes() as i64),
            bulk("ram_spill_bytes"),
            Value::Integer(vs.spill_bytes() as i64),
            // ADR-0059: the declared fields as name, type pairs, and what
            // their values and index hold in RAM, which the meter charges.
            bulk("fields"),
            Value::Array(Some(
                vs.fields()
                    .iter()
                    .flat_map(|f| [bulk(&f.name), bulk(f.kind.as_str())])
                    .collect(),
            )),
            bulk("field_bytes"),
            Value::Integer(vs.field_bytes() as i64),
        ]))
    }

    /// VEC.TTL <set> <id> — remaining life in ms (PTTL semantics): `>= 0` ms when
    /// the id is live and carries a TTL, `-1` when it exists without one, `-2`
    /// when it is absent or already expired.
    fn ttl(&self, ns: &[u8], args: &[Vec<u8>], now: u64) -> Value {
        if args.len() < 3 {
            return err("ERR VEC.TTL <set> <id>");
        }
        let Some(vs) = self.sets.get(&(ns.to_vec(), args[1].to_vec())) else {
            return err("ERR no such set");
        };
        let id = &args[2];
        // Expired-but-unswept, or never present: report absent, matching VEC.GET.
        if vs.is_expired(id, now) || !vs.contains(id) {
            return Value::Integer(-2);
        }
        match vs.expiry_at(id) {
            Some(e) => Value::Integer(e.saturating_sub(now) as i64),
            None => Value::Integer(-1),
        }
    }

    /// VEC.EXPIRE <set> <id> <secs> / VEC.PEXPIRE <set> <id> <ms> (`unit_ms` is
    /// 1000 or 1): (re)set the TTL on an existing live id WITHOUT resending the
    /// vector — the co-processor already holds it, so it re-encodes the durable
    /// row with the new deadline. Redis EXPIRE reply: 1 applied, 0 if the id is
    /// absent or already expired.
    fn plan_expire(&self, ns: &[u8], args: &[Vec<u8>], now: u64, unit_ms: u64) -> Plan {
        if args.len() < 4 {
            return Plan::Reply(err("ERR VEC.EXPIRE <set> <id> <ttl>"));
        }
        let (set, id) = (&args[1], &args[2]);
        let Some(vs) = self.sets.get(&(ns.to_vec(), set.to_vec())) else {
            return Plan::Reply(err("ERR no such set"));
        };
        if vs.is_expired(id, now) {
            return Plan::Reply(Value::Integer(0));
        }
        let (vec, meta) = match vs.get(id) {
            Ok(Some(found)) => found,
            Ok(None) => return Plan::Reply(Value::Integer(0)),
            Err(e) => return Plan::Reply(err(&format!("ERR {e}"))),
        };
        let ttl_ms = match std::str::from_utf8(&args[3])
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        {
            Some(n) if n > 0 => n.saturating_mul(unit_ms),
            _ => return Plan::Reply(err("ERR ttl must be a positive integer")),
        };
        let expires_at = now.saturating_add(ttl_ms);
        Plan::Write {
            // No index op: the id already exists, so its membership is
            // unchanged — only the row's expiry header is being rewritten.
            persist: vec![Persist::Put {
                key: durable_key(b'v', set, id),
                val: self.vec_row(&vec, meta.as_deref(), Some(expires_at)),
                ttl_ms: Some(ttl_ms),
            }],
            apply: Apply::Insert {
                set: set.to_vec(),
                id: id.to_vec(),
                vec,
                meta,
                expires_at: Some(expires_at),
            },
            ok: Value::Integer(1),
        }
    }

    /// VEC.PERSIST <set> <id>: remove the TTL, making the id permanent. Rewrites
    /// the durable row without an expiry header and re-`SET`s the data-plane key
    /// with no PX (so its native TTL clears too). Redis PERSIST reply: 1 if a TTL
    /// was removed, 0 if the id is absent/expired or already permanent.
    fn plan_persist(&self, ns: &[u8], args: &[Vec<u8>], now: u64) -> Plan {
        if args.len() < 3 {
            return Plan::Reply(err("ERR VEC.PERSIST <set> <id>"));
        }
        let (set, id) = (&args[1], &args[2]);
        let Some(vs) = self.sets.get(&(ns.to_vec(), set.to_vec())) else {
            return Plan::Reply(err("ERR no such set"));
        };
        if vs.is_expired(id, now) {
            return Plan::Reply(Value::Integer(0));
        }
        let (vec, meta) = match vs.get(id) {
            Ok(Some(found)) => found,
            Ok(None) => return Plan::Reply(Value::Integer(0)),
            Err(e) => return Plan::Reply(err(&format!("ERR {e}"))),
        };
        if vs.expiry_at(id).is_none() {
            return Plan::Reply(Value::Integer(0)); // nothing to persist
        }
        Plan::Write {
            // No index op: the id already exists, so its membership is
            // unchanged — only the row's expiry header is being rewritten.
            persist: vec![Persist::Put {
                key: durable_key(b'v', set, id),
                val: self.vec_row(&vec, meta.as_deref(), None),
                ttl_ms: None,
            }],
            apply: Apply::Insert {
                set: set.to_vec(),
                id: id.to_vec(),
                vec,
                meta,
                expires_at: None,
            },
            ok: Value::Integer(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }
    fn cmd(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| v(p)).collect()
    }

    /// Drive a command through the two-phase path as the networked co-processor
    /// would with a durable store that never sheds: plan, then (for a write)
    /// commit and return `ok`. `run` uses t=0; `run_at` takes an explicit clock
    /// so the TTL tests can set and cross a deadline.
    fn run(st: &mut Store, ns: &[u8], args: &[Vec<u8>]) -> Value {
        run_at(st, ns, args, 0)
    }
    fn run_at(st: &mut Store, ns: &[u8], args: &[Vec<u8>], now: u64) -> Value {
        match st.plan(ns, args, now) {
            Plan::Reply(v) => v,
            Plan::Write { apply, ok, .. } => {
                st.commit(ns, apply);
                ok
            }
        }
    }
    /// Plan at t=0 for the tests that inspect the returned [`Plan`] directly
    /// (durable rows, error shapes) and do not care about the clock.
    fn plan0(st: &Store, ns: &[u8], args: &[Vec<u8>]) -> Plan {
        st.plan(ns, args, 0)
    }

    /// ADR-0049 at the command surface: QUANT is HNSW-only, rides the config's
    /// fourth field, reads back, shows last in INFO, and a quantized set of
    /// each code answers SEARCH (with RERANK in either order beside EF) like a
    /// plain one.
    #[test]
    fn a_quantized_set_round_trips_through_the_commands() {
        let mut st = Store::new();
        let ns = b"nsQ";
        assert_eq!(
            run(
                &mut st,
                ns,
                &cmd(&[
                    "VEC.CREATE",
                    "f",
                    "DIM",
                    "2",
                    "METRIC",
                    "l2",
                    "QUANT",
                    "sq8"
                ])
            ),
            Value::Error("ERR QUANT needs INDEX hnsw".into()),
            "a quantized flat would be neither exact nor the oracle"
        );
        for (code, quant) in [("sq8", Quant::Sq8), ("bin", Quant::Bin)] {
            let create = cmd(&[
                "VEC.CREATE",
                code,
                "DIM",
                "2",
                "METRIC",
                "l2",
                "QUANT",
                code,
                "INDEX",
                "hnsw",
            ]);
            match plan0(&st, ns, &create) {
                Plan::Write { persist, .. } => {
                    let Some(Persist::Put { val, .. }) = persist.into_iter().nth(1) else {
                        panic!("the config row is CREATE's second step")
                    };
                    assert_eq!(val, format!("2|l2|hnsw|{code}").into_bytes());
                    assert_eq!(
                        decode_config(&val),
                        Some((2, Metric::L2, IndexKind::Hnsw, quant, vec![]))
                    );
                }
                _ => panic!("create should plan a write"),
            }
            assert_eq!(run(&mut st, ns, &create), Value::Simple("OK".into()));
            for (id, x) in [("a", "0,0"), ("b", "5,5"), ("c", "9,9")] {
                assert_eq!(
                    run(&mut st, ns, &cmd(&["VEC.SET", code, id, x])),
                    Value::Simple("OK".into())
                );
            }
            for opts in [
                vec!["RERANK", "3", "EF", "16"],
                vec!["EF", "16", "RERANK", "3"],
                vec![],
            ] {
                let mut c = vec!["VEC.SEARCH", code, "1,1", "1"];
                c.extend(opts.iter());
                let Value::Array(Some(rows)) = run(&mut st, ns, &cmd(&c)) else {
                    panic!("search reply")
                };
                assert_eq!(
                    rows,
                    vec![Value::Array(Some(vec![
                        Value::Bulk(Some(v("a"))),
                        Value::Double(-2.0),
                    ]))],
                    "{code}: nearest is a at -||(1,1)||^2, scored on the full vector (opts {opts:?})"
                );
            }
            let Value::Array(Some(info)) = run(&mut st, ns, &cmd(&["VEC.INFO", code])) else {
                panic!("info reply")
            };
            assert_eq!(info[14], Value::Bulk(Some(v("quant"))));
            assert_eq!(info[15], Value::Bulk(Some(v(code))));
            assert_eq!(
                info[16],
                Value::Bulk(Some(v("vectors_on"))),
                "step 2's fields come after it"
            );
            assert_eq!(
                info[10],
                Value::Bulk(Some(v("ns_mem_bytes"))),
                "earlier fields keep their places"
            );
        }
        assert_eq!(
            run(
                &mut st,
                ns,
                &cmd(&[
                    "VEC.CREATE",
                    "x",
                    "DIM",
                    "2",
                    "METRIC",
                    "l2",
                    "INDEX",
                    "hnsw",
                    "QUANT",
                    "pq8"
                ])
            ),
            Value::Error("ERR QUANT must be none, sq8 or bin".into())
        );
    }

    /// Rollback: a plain set's config is byte-identical to what every earlier
    /// binary wrote, and a quantized set's fourth field is one an earlier binary
    /// reads straight past. The decoder below is the pre-ADR-0049 one, verbatim.
    /// The value after `field` in a VEC.INFO reply.
    fn info_field(v: &Value, field: &str) -> Value {
        let Value::Array(Some(items)) = v else {
            panic!("INFO is an array: {v:?}")
        };
        let at = items
            .iter()
            .position(|x| x == &Value::Bulk(Some(field.as_bytes().to_vec())))
            .unwrap_or_else(|| panic!("INFO has no {field}"));
        items[at + 1].clone()
    }

    /// ADR-0049 step 2: with a vector directory, a quantized set's full
    /// vectors are in a file, the D4 meter stops charging for them, and what a
    /// tenant reads back is unchanged.
    #[test]
    fn a_vec_dir_moves_quantized_vectors_out_of_ram() {
        let dir = std::env::temp_dir().join(format!("flint-vec-store-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let create = |name: &str| {
            cmd(&[
                "VEC.CREATE",
                name,
                "DIM",
                "8",
                "METRIC",
                "l2",
                "INDEX",
                "hnsw",
                "QUANT",
                "sq8",
            ])
        };
        let x = "0.5,1.5,2.5,3.5,4.5,5.5,6.5,7.25";
        let fill = |st: &mut Store, ns: &[u8]| {
            assert_eq!(run(st, ns, &create("q")), Value::Simple("OK".into()));
            for i in 0..10 {
                let set = cmd(&["VEC.SET", "q", &format!("id{i}"), x]);
                assert_eq!(run(st, ns, &set), Value::Simple("OK".into()));
            }
        };
        let mut ram = Store::new();
        fill(&mut ram, b"ns");
        let mut disk = Store::new();
        disk.set_vec_dir(dir.clone());
        fill(&mut disk, b"ns");

        let info = run(&mut disk, b"ns", &cmd(&["VEC.INFO", "q"]));
        assert_eq!(
            info_field(&info, "vectors_on"),
            Value::Bulk(Some(b"disk".to_vec()))
        );
        assert_eq!(info_field(&info, "disk_bytes"), Value::Integer(10 * 8 * 4));
        assert_eq!(info_field(&info, "ram_spill_bytes"), Value::Integer(0));
        let ram_info = run(&mut ram, b"ns", &cmd(&["VEC.INFO", "q"]));
        assert_eq!(
            info_field(&ram_info, "vectors_on"),
            Value::Bulk(Some(b"ram".to_vec()))
        );
        assert_eq!(
            ram.ns_mem_bytes(b"ns") - disk.ns_mem_bytes(b"ns"),
            10 * (8 * 4 + 16),
            "the meter stops charging dim x 4 per vector, and the Vec that held it"
        );
        for st in [&mut ram, &mut disk] {
            let got = run(st, b"ns", &cmd(&["VEC.GET", "q", "id3"]));
            assert_eq!(
                got,
                Value::Array(Some(vec![Value::Bulk(Some(x.as_bytes().to_vec()))]))
            );
            let top = run(st, b"ns", &cmd(&["VEC.SEARCH", "q", x, "1"]));
            let Value::Array(Some(rows)) = top else {
                panic!("rows")
            };
            let Value::Array(Some(row)) = &rows[0] else {
                panic!("row")
            };
            assert_eq!(row[1], Value::Double(0.0), "scored on the exact vector");
        }
        // A plain HNSW set keeps its vectors in the graph, file or no file.
        let plain = cmd(&[
            "VEC.CREATE",
            "p",
            "DIM",
            "2",
            "METRIC",
            "l2",
            "INDEX",
            "hnsw",
        ]);
        assert_eq!(run(&mut disk, b"ns", &plain), Value::Simple("OK".into()));
        let info = run(&mut disk, b"ns", &cmd(&["VEC.INFO", "p"]));
        assert_eq!(
            info_field(&info, "vectors_on"),
            Value::Bulk(Some(b"ram".to_vec()))
        );
        drop(disk);
        assert_eq!(
            std::fs::read_dir(&dir).expect("dir").count(),
            0,
            "a set's file goes with the set"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// BUG-0255: a removed tenant's namespace can be given to a new tenant,
    /// so dropping it must leave nothing the new tenant could read, and must
    /// leave every other namespace alone.
    #[test]
    fn dropping_a_namespace_leaves_nothing_of_it() {
        let dir = std::env::temp_dir().join(format!("flint-vec-drop-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let mut st = Store::new();
        st.set_vec_dir(dir.clone());
        let create = cmd(&[
            "VEC.CREATE",
            "q",
            "DIM",
            "4",
            "METRIC",
            "l2",
            "INDEX",
            "hnsw",
            "QUANT",
            "sq8",
        ]);
        let set = cmd(&["VEC.SET", "q", "secret", "1,2,3,4", "META", "private"]);
        for ns in [b"old".as_slice(), b"other"] {
            assert_eq!(run(&mut st, ns, &create), Value::Simple("OK".into()));
            assert_eq!(run(&mut st, ns, &set), Value::Simple("OK".into()));
        }
        assert_eq!(std::fs::read_dir(&dir).expect("dir").count(), 2);
        let other_bytes = st.ns_mem_bytes(b"other");

        assert_eq!(st.drop_namespace(b"old"), 1);
        assert_eq!(st.generation(b"old"), 1);
        assert_eq!(st.generation(b"other"), 0);
        assert_eq!(st.ns_mem_bytes(b"old"), 0);
        assert!(
            matches!(
                run(&mut st, b"old", &cmd(&["VEC.GET", "q", "secret"])),
                Value::Error(_)
            ),
            "the dropped namespace has no set q"
        );
        assert_eq!(
            std::fs::read_dir(&dir).expect("dir").count(),
            1,
            "the dropped set's vector file is gone"
        );
        // The other tenant keeps its set, its vector, its META and its bytes.
        let got = run(&mut st, b"other", &cmd(&["VEC.GET", "q", "secret"]));
        assert_eq!(
            got,
            Value::Array(Some(vec![
                Value::Bulk(Some(b"1,2,3,4".to_vec())),
                Value::Bulk(Some(b"private".to_vec())),
            ]))
        );
        assert_eq!(st.ns_mem_bytes(b"other"), other_bytes);
        // A new tenant on the dropped namespace starts empty, and can use it.
        assert_eq!(run(&mut st, b"old", &create), Value::Simple("OK".into()));
        assert_eq!(
            info_field(&run(&mut st, b"old", &cmd(&["VEC.INFO", "q"])), "count"),
            Value::Integer(0)
        );
        assert_eq!(st.drop_namespace(b"never-seen"), 0);
        drop(st);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// BUG-0255: a set emptied by VEC.DEL, or by its TTLs, gives back its
    /// slots and its vector file, and keeps working.
    #[test]
    fn an_emptied_set_starts_again() {
        let dir = std::env::temp_dir().join(format!("flint-vec-empty-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let mut st = Store::new();
        st.set_vec_dir(dir.clone());
        let create = cmd(&[
            "VEC.CREATE",
            "q",
            "DIM",
            "4",
            "METRIC",
            "l2",
            "INDEX",
            "hnsw",
            "QUANT",
            "bin",
        ]);
        assert_eq!(run(&mut st, b"ns", &create), Value::Simple("OK".into()));
        let disk =
            |st: &mut Store| info_field(&run(st, b"ns", &cmd(&["VEC.INFO", "q"])), "disk_bytes");
        for i in 0..50 {
            let v = format!("{i},1,2,3");
            let set = cmd(&["VEC.SET", "q", &format!("id{i}"), &v]);
            assert_eq!(run(&mut st, b"ns", &set), Value::Simple("OK".into()));
        }
        assert_eq!(disk(&mut st), Value::Integer(50 * 4 * 4));
        for i in 0..49 {
            let del = cmd(&["VEC.DEL", "q", &format!("id{i}")]);
            assert_eq!(run(&mut st, b"ns", &del), Value::Integer(1));
        }
        assert_eq!(
            disk(&mut st),
            Value::Integer(50 * 4 * 4),
            "a set with a vector left keeps its slots for its next inserts (BUG-0197)"
        );
        assert_eq!(
            run(&mut st, b"ns", &cmd(&["VEC.DEL", "q", "id49"])),
            Value::Integer(1)
        );
        assert_eq!(
            disk(&mut st),
            Value::Integer(0),
            "the emptied set's file starts again"
        );
        assert_eq!(st.ns_mem_bytes(b"ns"), 0);
        assert_eq!(
            std::fs::read_dir(&dir).expect("dir").count(),
            1,
            "one file, the new one: the old one is gone"
        );
        // It keeps its configuration and serves again.
        assert_eq!(
            run(&mut st, b"ns", &cmd(&["VEC.SET", "q", "back", "9,1,2,3"])),
            Value::Simple("OK".into())
        );
        let top = run(&mut st, b"ns", &cmd(&["VEC.SEARCH", "q", "9,1,2,3", "1"]));
        let Value::Array(Some(rows)) = top else {
            panic!("rows")
        };
        let Value::Array(Some(row)) = &rows[0] else {
            panic!("row")
        };
        assert_eq!(row[0], Value::Bulk(Some(b"back".to_vec())));
        assert_eq!(
            info_field(&run(&mut st, b"ns", &cmd(&["VEC.INFO", "q"])), "quant"),
            Value::Bulk(Some(b"bin".to_vec()))
        );

        // The TTL sweep empties it the same way.
        assert_eq!(
            run_at(
                &mut st,
                b"ns",
                &cmd(&["VEC.SET", "q", "back", "9,1,2,3", "PX", "10"]),
                1_000
            ),
            Value::Simple("OK".into())
        );
        assert_eq!(st.sweep_expired(2_000), 1);
        assert_eq!(disk(&mut st), Value::Integer(0));
        assert_eq!(std::fs::read_dir(&dir).expect("dir").count(), 1);
        drop(st);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// BUG-0257: an HNSW slot's memory is held until the set's next insert
    /// takes it, so the meter keeps charging it: deleting all but one vector
    /// gives nothing back, an insert into a deleted slot costs nothing more,
    /// and emptying the set gives everything back.
    #[test]
    fn a_deleted_slot_stays_charged_until_taken_again() {
        let mut st = Store::new();
        let create = cmd(&[
            "VEC.CREATE",
            "q",
            "DIM",
            "8",
            "METRIC",
            "l2",
            "INDEX",
            "hnsw",
            "QUANT",
            "sq8",
        ]);
        assert_eq!(run(&mut st, b"ns", &create), Value::Simple("OK".into()));
        let v = |i: usize| format!("{i},1,2,3,4,5,6,7");
        let set =
            |st: &mut Store, id: &str, i: usize| run(st, b"ns", &cmd(&["VEC.SET", "q", id, &v(i)]));
        for i in 0..50 {
            assert_eq!(
                set(&mut st, &format!("x{i:03}"), i),
                Value::Simple("OK".into())
            );
        }
        let full = st.ns_mem_bytes(b"ns");
        for i in 0..49 {
            let del = cmd(&["VEC.DEL", "q", &format!("x{i:03}")]);
            assert_eq!(run(&mut st, b"ns", &del), Value::Integer(1));
        }
        assert_eq!(
            st.ns_mem_bytes(b"ns"),
            full,
            "49 deleted slots still hold their memory, so they are still charged"
        );
        // New ids of the same length take the deleted slots: no new charge.
        // 48, not 49: a deleted entry point is not taken again until the
        // entry moves, so it may stay a held, charged slot (as it should).
        for i in 0..48 {
            assert_eq!(
                set(&mut st, &format!("y{i:03}"), i),
                Value::Simple("OK".into())
            );
        }
        assert_eq!(st.ns_mem_bytes(b"ns"), full);
        // Emptying the set gives it all back (BUG-0255 starts it again).
        for id in (0..48)
            .map(|i| format!("y{i:03}"))
            .chain(["x049".to_string()])
        {
            assert_eq!(
                run(&mut st, b"ns", &cmd(&["VEC.DEL", "q", &id])),
                Value::Integer(1)
            );
        }
        assert_eq!(st.ns_mem_bytes(b"ns"), 0);

        // The TTL sweep charges as VEC.DEL does.
        for i in 0..10 {
            let px = cmd(&["VEC.SET", "q", &format!("t{i:03}"), &v(i), "PX", "10"]);
            assert_eq!(
                run_at(&mut st, b"ns", &px, 1_000),
                Value::Simple("OK".into())
            );
        }
        assert_eq!(set(&mut st, "keep", 0), Value::Simple("OK".into()));
        let with_ttl = st.ns_mem_bytes(b"ns");
        assert_eq!(st.sweep_expired(2_000), 10);
        assert_eq!(st.ns_mem_bytes(b"ns"), with_ttl, "swept slots stay charged");
        assert_eq!(
            run(&mut st, b"ns", &cmd(&["VEC.DEL", "q", "keep"])),
            Value::Integer(1)
        );
        assert_eq!(st.ns_mem_bytes(b"ns"), 0);
    }

    /// BUG-0257, at the cap: deleting makes room for the same set's writes,
    /// and none for another set's, whose memory the deleted slots still hold.
    #[test]
    fn at_the_cap_a_delete_makes_room_only_in_its_own_set() {
        let mut st = Store::new();
        for set in ["a", "b"] {
            let create = cmd(&[
                "VEC.CREATE",
                set,
                "DIM",
                "8",
                "METRIC",
                "l2",
                "INDEX",
                "hnsw",
                "QUANT",
                "sq8",
            ]);
            assert_eq!(run(&mut st, b"ns", &create), Value::Simple("OK".into()));
        }
        let v = |i: usize| format!("{i},1,2,3,4,5,6,7");
        let write = |st: &mut Store, set: &str, id: String, i: usize| {
            run(st, b"ns", &cmd(&["VEC.SET", set, &id, &v(i)]))
        };
        for i in 0..20 {
            assert_eq!(
                write(&mut st, "a", format!("x{i:03}"), i),
                Value::Simple("OK".into())
            );
        }
        st.set_index_cap(st.ns_mem_bytes(b"ns"));
        let refused = |r: Value| matches!(r, Value::Error(e) if e.starts_with("VECFULL"));
        assert!(refused(write(&mut st, "a", "x999".into(), 1)));
        // Six deleted: at least five are free to take, since a deleted entry
        // point waits until the entry moves.
        for i in 0..6 {
            let del = cmd(&["VEC.DEL", "a", &format!("x{i:03}")]);
            assert_eq!(run(&mut st, b"ns", &del), Value::Integer(1));
        }
        assert!(
            refused(write(&mut st, "b", "y000".into(), 1)),
            "set a's deleted slots hold memory set b would need"
        );
        for i in 0..5 {
            assert_eq!(
                write(&mut st, "a", format!("y{i:03}"), i),
                Value::Simple("OK".into()),
                "a deleted slot makes room for its own set's next write"
            );
        }
        // At most the sixth slot is left, and then set a is full again.
        let more = (0..3)
            .take_while(|&i| !refused(write(&mut st, "a", format!("z{i:03}"), i)))
            .count();
        assert!(more <= 1, "{more} writes past the deleted slots");
        assert!(refused(write(&mut st, "b", "y001".into(), 1)));
    }

    #[test]
    fn a_quantized_config_is_readable_by_the_binary_before_it() {
        fn decode_before_adr_0049(val: &[u8]) -> Option<(usize, Metric, IndexKind)> {
            let s = std::str::from_utf8(val).ok()?;
            let mut it = s.split('|');
            let dim = it.next()?.parse().ok()?;
            let metric = Metric::parse(it.next()?.as_bytes())?;
            let index = match it.next() {
                Some(k) => IndexKind::parse(k.as_bytes())?,
                None => IndexKind::Flat,
            };
            Some((dim, metric, index))
        }
        assert_eq!(
            encode_config(8, Metric::Cosine, IndexKind::Hnsw, Quant::None, &[]),
            b"8|cosine|hnsw".to_vec(),
            "a plain set writes exactly what it always did"
        );
        let quantized = encode_config(8, Metric::Cosine, IndexKind::Hnsw, Quant::Sq8, &[]);
        assert_eq!(
            decode_before_adr_0049(&quantized),
            Some((8, Metric::Cosine, IndexKind::Hnsw)),
            "an older binary loads a quantized set as plain HNSW"
        );
        assert_eq!(
            decode_config(b"8|cosine|hnsw"),
            Some((8, Metric::Cosine, IndexKind::Hnsw, Quant::None, vec![]))
        );
        assert_eq!(
            decode_config(b"8|cosine|hnsw|later|96"),
            Some((8, Metric::Cosine, IndexKind::Hnsw, Quant::None, vec![])),
            "a later binary's code loads unquantized, its further fields read past"
        );
        for q in [Quant::Sq8, Quant::Bin] {
            assert_eq!(
                decode_config(&encode_config(8, Metric::Cosine, IndexKind::Hnsw, q, &[])),
                Some((8, Metric::Cosine, IndexKind::Hnsw, q, vec![]))
            );
        }
        assert_eq!(decode_config(b"8|cosine|bogus|sq8"), None);
    }

    #[test]
    fn create_set_search_end_to_end() {
        let mut st = Store::new();
        let ns = b"nsA";

        assert_eq!(
            run(
                &mut st,
                ns,
                &cmd(&["VEC.CREATE", "docs", "DIM", "2", "METRIC", "l2"])
            ),
            Value::Simple("OK".into())
        );
        // duplicate CREATE errors (and plans no write)
        assert!(matches!(
            plan0(&st, ns, &cmd(&["VEC.CREATE", "docs", "DIM", "2", "METRIC", "l2"])),
            Plan::Reply(Value::Error(e)) if e.contains("already exists")
        ));

        for (id, vec) in [("a", "0,0"), ("b", "1,0"), ("c", "3,4")] {
            assert_eq!(
                run(&mut st, ns, &cmd(&["VEC.SET", "docs", id, vec])),
                Value::Simple("OK".into())
            );
        }

        // wrong dimension: an error, and NOT a Write (nothing to persist/apply)
        assert!(matches!(
            plan0(&st, ns, &cmd(&["VEC.SET", "docs", "d", "1,2,3"])),
            Plan::Reply(Value::Error(e)) if e.starts_with("WRONGDIM")
        ));

        // SEARCH from the origin: a (0), b (1), then c (5)
        match run(&mut st, ns, &cmd(&["VEC.SEARCH", "docs", "0,0", "2"])) {
            Value::Array(Some(rows)) => {
                let ids: Vec<Vec<u8>> = rows
                    .iter()
                    .map(|row| match row {
                        Value::Array(Some(pair)) => match &pair[0] {
                            Value::Bulk(Some(id)) => id.clone(),
                            _ => panic!(),
                        },
                        _ => panic!(),
                    })
                    .collect();
                assert_eq!(ids, vec![v("a"), v("b")], "nearest two, in order");
            }
            other => panic!("expected array, got {other:?}"),
        }
    }

    /// `INDEX hnsw` end-to-end through the same VEC.* surface: the durable config
    /// records the kind (so a rebuild restores HNSW, not the flat default), the
    /// engine dispatches on SET/GET/SEARCH, `EF` is accepted, and INFO reports it.
    #[test]
    fn hnsw_index_end_to_end() {
        let mut st = Store::new();
        let ns = b"nsH";

        // CREATE ... INDEX hnsw: the persisted config value ends with the kind,
        // which is what rebuild reads back to pick the engine.
        match plan0(
            &st,
            ns,
            &cmd(&[
                "VEC.CREATE",
                "d",
                "DIM",
                "2",
                "METRIC",
                "l2",
                "INDEX",
                "hnsw",
            ]),
        ) {
            Plan::Write { persist, .. } => {
                assert!(
                    matches!(persist.first(), Some(Persist::SAdd { .. })),
                    "the index entry must be written BEFORE the row (#194)"
                );
                let Some(Persist::Put { val, .. }) = persist.into_iter().nth(1) else {
                    panic!("CREATE's second durable step must be the config row")
                };
                assert!(
                    val.ends_with(b"|hnsw"),
                    "durable config must record the hnsw kind for rebuild, got {:?}",
                    String::from_utf8_lossy(&val)
                );
            }
            _ => panic!("CREATE should plan a Write"),
        }
        run(
            &mut st,
            ns,
            &cmd(&[
                "VEC.CREATE",
                "d",
                "DIM",
                "2",
                "METRIC",
                "l2",
                "INDEX",
                "hnsw",
            ]),
        );

        // Points spread along the x-axis: nearest-to-origin order is unambiguous,
        // and 24 of them exercises more than the entry node.
        for i in 0..24u32 {
            let (id, vec) = (format!("p{i}"), format!("{i},0"));
            assert_eq!(
                run(&mut st, ns, &cmd(&["VEC.SET", "d", &id, &vec])),
                Value::Simple("OK".into())
            );
        }

        // INFO reports the hnsw engine and the live count.
        match run(&mut st, ns, &cmd(&["VEC.INFO", "d"])) {
            Value::Array(Some(f)) => {
                assert_eq!(f[4], Value::Bulk(Some(b"index".to_vec())));
                assert_eq!(f[5], Value::Bulk(Some(b"hnsw".to_vec())), "INFO index");
                assert_eq!(f[7], Value::Integer(24), "INFO count");
            }
            other => panic!("INFO should be an array, got {other:?}"),
        }

        // GET routes through the HNSW engine and is lossless.
        assert_eq!(
            run(&mut st, ns, &cmd(&["VEC.GET", "d", "p5"])),
            Value::Array(Some(vec![Value::Bulk(Some(b"5,0".to_vec()))])),
        );

        // SEARCH from the origin with an explicit EF: the three nearest are the
        // three smallest x, nearest first (score conversion keeps the ordering).
        match run(
            &mut st,
            ns,
            &cmd(&["VEC.SEARCH", "d", "0,0", "3", "EF", "64"]),
        ) {
            Value::Array(Some(rows)) => {
                let ids: Vec<Vec<u8>> = rows
                    .iter()
                    .map(|row| match row {
                        Value::Array(Some(pair)) => match &pair[0] {
                            Value::Bulk(Some(id)) => id.clone(),
                            _ => panic!("row[0] not a bulk id"),
                        },
                        _ => panic!("search row not a pair"),
                    })
                    .collect();
                assert_eq!(
                    ids,
                    vec![v("p0"), v("p1"), v("p2")],
                    "nearest three in order"
                );
            }
            other => panic!("SEARCH should be an array, got {other:?}"),
        }
    }

    #[test]
    fn a_write_plans_the_durable_side_before_the_index_side() {
        let st = Store::new();
        let mut st = st;
        run(
            &mut st,
            b"n",
            &cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "ip"]),
        );
        // A VEC.SET returns a Write whose Persist::Put targets the vector's
        // durable key and whose Apply::Insert carries the parsed vector — the
        // caller performs the Put, THEN commits the Insert.
        match plan0(&st, b"n", &cmd(&["VEC.SET", "s", "id7", "1,2"])) {
            Plan::Write {
                persist,
                apply: Apply::Insert { id, vec, .. },
                ok,
            } => {
                assert!(
                    matches!(persist.first(), Some(Persist::SAdd { .. })),
                    "the index entry must be written BEFORE the row (#194)"
                );
                let Some(Persist::Put { key, .. }) = persist.into_iter().nth(1) else {
                    panic!("SET's second durable step must be the vector row")
                };
                assert_eq!(key, durable_key(b'v', b"s", b"id7"));
                assert_eq!(id, v("id7"));
                assert_eq!(vec, vec![1.0, 2.0]);
                assert_eq!(ok, Value::Simple("OK".into()));
            }
            _ => panic!("VEC.SET should plan a Write"),
        }
        // Because plan() does not mutate, the index is still empty until commit.
        assert!(matches!(
            plan0(&st, b"n", &cmd(&["VEC.GET", "s", "id7"])),
            Plan::Reply(Value::Null)
        ));
    }

    #[test]
    fn get_and_del_and_meta() {
        let mut st = Store::new();
        let ns = b"nsA";
        run(
            &mut st,
            ns,
            &cmd(&["VEC.CREATE", "s", "DIM", "3", "METRIC", "cosine"]),
        );
        run(
            &mut st,
            ns,
            &cmd(&["VEC.SET", "s", "x", "1,2,3", "META", "{\"t\":1}"]),
        );

        match run(&mut st, ns, &cmd(&["VEC.GET", "s", "x"])) {
            Value::Array(Some(row)) => {
                assert_eq!(row[0], Value::Bulk(Some(v("1,2,3"))));
                assert_eq!(row[1], Value::Bulk(Some(v("{\"t\":1}"))));
            }
            other => panic!("expected array, got {other:?}"),
        }
        assert_eq!(
            run(&mut st, ns, &cmd(&["VEC.GET", "s", "missing"])),
            Value::Null
        );

        // DEL of a present id plans a Write (durable delete); of an absent id
        // is a plain Reply(0) with nothing to persist.
        assert!(matches!(
            plan0(&st, ns, &cmd(&["VEC.DEL", "s", "x"])),
            Plan::Write {
                // Row first, index entry second — the reverse would leak a
                // durable row nothing lists (#194).
                ok: Value::Integer(1),
                ..
            }
        ));
        match plan0(&st, ns, &cmd(&["VEC.DEL", "s", "x"])) {
            Plan::Write { persist, .. } => assert!(
                matches!(persist.first(), Some(Persist::Del { .. }))
                    && matches!(persist.get(1), Some(Persist::SRem { .. })),
                "DEL must drop the row THEN the index entry, got {persist:?}"
            ),
            _ => panic!("DEL of a present id plans a Write"),
        }
        assert_eq!(
            run(&mut st, ns, &cmd(&["VEC.DEL", "s", "x"])),
            Value::Integer(1)
        );
        assert!(matches!(
            plan0(&st, ns, &cmd(&["VEC.DEL", "s", "x"])),
            Plan::Reply(Value::Integer(0))
        ));
    }

    #[test]
    fn tenants_are_isolated_by_namespace() {
        let mut st = Store::new();
        run(
            &mut st,
            b"tenantA",
            &cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "ip"]),
        );
        run(
            &mut st,
            b"tenantA",
            &cmd(&["VEC.SET", "s", "secret", "9,9"]),
        );
        // tenantB has no set named "s" of its own.
        assert!(matches!(
            plan0(&st, b"tenantB", &cmd(&["VEC.SEARCH", "s", "9,9", "1"])),
            Plan::Reply(Value::Error(e)) if e.contains("no such set")
        ));
        run(
            &mut st,
            b"tenantB",
            &cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "ip"]),
        );
        match run(&mut st, b"tenantB", &cmd(&["VEC.SEARCH", "s", "9,9", "5"])) {
            Value::Array(Some(rows)) => assert!(rows.is_empty(), "B's index cannot see A's vector"),
            other => panic!("expected empty array, got {other:?}"),
        }
    }

    /// The index keys must be distinguishable from the rows they index, and
    /// from each other, or a rebuild reading one would load the other.
    #[test]
    fn index_keys_are_distinct_from_rows_and_from_each_other() {
        assert!(sets_index_key().starts_with(KEY_PREFIX));
        assert_eq!(
            parse_durable_key(&sets_index_key()).map(|t| t.0),
            Some(b's')
        );
        assert_eq!(
            parse_durable_key(&index_key(b"docs", 7)).map(|t| t.0),
            Some(b'i')
        );
        assert_eq!(
            parse_durable_key(&vector_key(b"docs", b"7")).map(|t| t.0),
            Some(b'v')
        );
        assert_eq!(
            parse_durable_key(&config_key(b"docs")).map(|t| t.0),
            Some(b'c')
        );
        // Two buckets of the same set are different keys, and bucket 7 of one
        // set is not bucket 7 of another.
        assert_ne!(index_key(b"docs", 7), index_key(b"docs", 8));
        assert_ne!(index_key(b"docs", 7), index_key(b"other", 7));
        // Fixed-width bucket labels: "007" and "070" must not collide with
        // each other or with a set named so that the two run together.
        assert_ne!(index_key(b"a", 7), index_key(b"a", 70));
    }

    /// Bucketing exists to bound each recovery reply, so it has to actually
    /// SPREAD. A hash that piled ids into one bucket would pass every other
    /// test here and reintroduce the single huge reply the buckets replaced.
    #[test]
    fn bucket_of_spreads_ids_across_the_index() {
        let mut counts = std::collections::HashMap::new();
        for i in 0..10_000u32 {
            *counts
                .entry(bucket_of(format!("id{i}").as_bytes()))
                .or_insert(0u32) += 1;
        }
        assert!(
            counts.len() > (INDEX_BUCKETS as usize) * 9 / 10,
            "10k ids should reach nearly every bucket, reached {}",
            counts.len()
        );
        let worst = counts.values().copied().max().unwrap_or(0);
        // Uniform would be ~39 per bucket; 4x that is a generous ceiling and
        // still catches a hash that clumps.
        assert!(
            worst < 160,
            "worst bucket holds {worst} of 10000 ids — the hash is clumping"
        );
        // Stable across calls: a bucket computed at write time must be the one
        // read at recovery time, in a DIFFERENT process.
        assert_eq!(bucket_of(b"id42"), bucket_of(b"id42"));
    }

    #[test]
    fn durable_key_roundtrips_prefix_kind_set_id() {
        let k = durable_key(b'v', b"docs", b"42");
        assert!(k.starts_with(KEY_PREFIX));
        // kind 'v', NUL, set, NUL, id — built explicitly (a `\0` next to digits
        // reads like an octal escape Rust does not have).
        let mut want = vec![b'v', 0];
        want.extend_from_slice(b"docs");
        want.push(0);
        want.extend_from_slice(b"42");
        assert_eq!(&k[KEY_PREFIX.len()..], want.as_slice());
        let cfg = durable_key(b'c', b"docs", b"");
        let mut want_cfg = vec![b'c', 0];
        want_cfg.extend_from_slice(b"docs");
        assert_eq!(&cfg[KEY_PREFIX.len()..], want_cfg.as_slice());
    }

    #[test]
    fn durable_rows_round_trip_for_rebuild() {
        let st = Store::new();
        // CREATE's config Put decodes back to (dim, metric).
        match plan0(
            &st,
            b"n",
            &cmd(&["VEC.CREATE", "docs", "DIM", "4", "METRIC", "cosine"]),
        ) {
            Plan::Write { persist, .. } => {
                assert!(
                    matches!(persist.first(), Some(Persist::SAdd { .. })),
                    "the index entry must be written BEFORE the row (#194)"
                );
                let Some(Persist::Put { key, val, ttl_ms }) = persist.into_iter().nth(1) else {
                    panic!("CREATE's second durable step must be the config row")
                };
                assert_eq!(parse_durable_key(&key), Some((b'c', v("docs"), Vec::new())));
                assert_eq!(ttl_ms, None, "a set's config row never expires");
                assert_eq!(
                    decode_config(&val),
                    Some((4, Metric::Cosine, IndexKind::Flat, Quant::None, vec![]))
                );
            }
            _ => panic!("create should plan a config Put"),
        }
        // SET's vector Put decodes back to (vec, meta).
        let mut st = st;
        run(
            &mut st,
            b"n",
            &cmd(&["VEC.CREATE", "docs", "DIM", "4", "METRIC", "cosine"]),
        );
        match plan0(
            &st,
            b"n",
            &cmd(&["VEC.SET", "docs", "id9", "1,2,3,4", "META", "m!"]),
        ) {
            Plan::Write { persist, .. } => {
                assert!(
                    matches!(persist.first(), Some(Persist::SAdd { .. })),
                    "the index entry must be written BEFORE the row (#194)"
                );
                let Some(Persist::Put { key, val, ttl_ms }) = persist.into_iter().nth(1) else {
                    panic!("SET's second durable step must be the vector row")
                };
                assert_eq!(parse_durable_key(&key), Some((b'v', v("docs"), v("id9"))));
                assert_eq!(ttl_ms, None, "no EX/PX given, so no data-plane TTL");
                let (vec, meta, expires_at) = decode_vec_row(&val).expect("decode vec row");
                assert_eq!(vec, vec![1.0, 2.0, 3.0, 4.0]);
                assert_eq!(meta, Some(v("m!")));
                assert_eq!(expires_at, None, "no EX/PX given, so no row expiry header");
            }
            _ => panic!("set should plan a vector Put"),
        }
        // A foreign (non-prefixed) key is not one of ours — the scan skips it.
        assert_eq!(parse_durable_key(b"regular:key"), None);
    }

    /// ADR-0050 D5: a packed row decodes to exactly what was encoded, with or
    /// without its expiry and metadata, at a third of text's size; and no
    /// text row can be mistaken for one.
    #[test]
    fn packed_rows_round_trip_and_no_text_row_looks_packed() {
        let vecs: [&[f32]; 3] = [
            &[1.0, -2.5, 0.0],
            &[0.1, -0.0, f32::MIN_POSITIVE, 1e-30, -3.4e38],
            &[0.123, 0.456, 0.789],
        ];
        for vec in vecs {
            for meta in [None, Some(&b"m\x00\x1f!"[..]), Some(&b""[..])] {
                for exp in [None, Some(0), Some(1_790_000_000_000)] {
                    let row = encode_packed_row(vec, meta, exp);
                    assert_eq!(row[0], PACKED_ROW);
                    let (got, m, e) = decode_vec_row(&row).expect("decode packed");
                    let bits = |v: &[f32]| v.iter().map(|f| f.to_bits()).collect::<Vec<_>>();
                    assert_eq!(bits(&got), bits(vec), "packed keeps every bit");
                    assert_eq!(m.as_deref(), meta);
                    assert_eq!(e, exp);
                    let text = encode_vec_row(vec, meta, exp);
                    assert!(text[0].is_ascii() && text[0] != PACKED_ROW, "{text:?}");
                    assert_eq!(decode_vec_row(&text).expect("decode text").0, vec);
                }
            }
        }
        let wide: Vec<f32> = (0..1536).map(|i| (i as f32 * 0.37).sin() / 39.2).collect();
        assert_eq!(encode_packed_row(&wide, None, None).len(), 6150);
        assert!(encode_vec_row(&wide, None, None).len() > 2 * 6150);
    }

    /// A packed row that is short, long, empty or flagged by a later format is
    /// an error. Read from the wrong offsets, it would be a wrong vector.
    #[test]
    fn a_damaged_or_later_packed_row_is_refused() {
        let row = encode_packed_row(&[1.0, 2.0], None, Some(7));
        for cut in 1..row.len() {
            assert!(decode_vec_row(&row[..cut]).is_err(), "cut at {cut}");
        }
        let mut long = row.clone();
        long.push(0);
        assert!(
            decode_vec_row(&long).is_err(),
            "a trailing byte with no META flag"
        );
        let mut later = row.clone();
        later[1] |= 4;
        assert!(
            decode_vec_row(&later).is_err(),
            "a flag this binary does not know"
        );
        assert!(
            decode_vec_row(&[PACKED_ROW, 0, 0, 0, 0, 0]).is_err(),
            "no floats"
        );
    }

    /// The store writes packed rows only when told, on every path that writes
    /// a row (SET, EXPIRE, PERSIST), and they carry what the text rows did.
    #[test]
    fn a_store_writes_packed_rows_only_when_told() {
        for packed in [false, true] {
            let mut st = Store::new();
            st.set_packed_rows(packed);
            run(
                &mut st,
                b"n",
                &cmd(&["VEC.CREATE", "s", "DIM", "3", "METRIC", "l2"]),
            );
            let row_of = |plan: Plan| match plan {
                Plan::Write { persist, .. } => persist
                    .into_iter()
                    .find_map(|p| match p {
                        Persist::Put { val, .. } => Some(val),
                        _ => None,
                    })
                    .expect("a vector row"),
                Plan::Reply(r) => panic!("expected a write, got {r:?}"),
            };
            let set = cmd(&["VEC.SET", "s", "a", "1,2,3", "META", "m", "PX", "5000"]);
            let rows = [
                row_of(plan0(&st, b"n", &set)),
                {
                    run(&mut st, b"n", &set);
                    row_of(plan0(&st, b"n", &cmd(&["VEC.EXPIRE", "s", "a", "60"])))
                },
                row_of(plan0(&st, b"n", &cmd(&["VEC.PERSIST", "s", "a"]))),
            ];
            for (row, exp) in rows.iter().zip([Some(5000), Some(60_000), None]) {
                assert_eq!(row[0] == PACKED_ROW, packed, "{row:?}");
                let (vec, meta, e) = decode_vec_row(row).expect("decode");
                assert_eq!(vec, vec![1.0, 2.0, 3.0]);
                assert_eq!(meta, Some(v("m")));
                assert_eq!(e, exp);
            }
        }
    }

    /// ADR-0050 D2: the overall cap refuses a write that would take every
    /// namespace together past it though each is under its own cap. It says
    /// no numbers, leaves reads alone, frees with a delete anywhere, and is
    /// counted apart from the per-namespace cap.
    #[test]
    fn the_overall_cap_bounds_every_namespace_together() {
        let mut st = Store::new();
        for ns in [&b"a"[..], b"b"] {
            run(
                &mut st,
                ns,
                &cmd(&["VEC.CREATE", "s", "DIM", "4", "METRIC", "l2"]),
            );
        }
        let ok = Value::Simple("OK".into());
        assert_eq!(
            run(&mut st, b"a", &cmd(&["VEC.SET", "s", "x0", "1,0,0,0"])),
            ok
        );
        let one = st.stats().index_bytes;
        assert!(one > 0);
        st.set_index_cap(3 * one);
        st.set_index_total_cap(4 * one);
        for id in ["x1", "x2"] {
            assert_eq!(
                run(&mut st, b"a", &cmd(&["VEC.SET", "s", id, "0,1,0,0"])),
                ok
            );
        }
        assert_eq!(
            run(&mut st, b"b", &cmd(&["VEC.SET", "s", "y0", "0,0,1,0"])),
            ok
        );
        // Four of four: b is at a third of its own cap, and still refused.
        match run(&mut st, b"b", &cmd(&["VEC.SET", "s", "y1", "0,0,0,1"])) {
            Value::Error(e) => {
                assert!(e.starts_with("VECFULL"), "{e}");
                assert!(
                    !e.chars().any(|c| c.is_ascii_digit()),
                    "names a number: {e}"
                );
            }
            r => panic!("expected the overall cap to refuse, got {r:?}"),
        }
        // a, past its own cap, is refused by that one, which says its numbers.
        match run(&mut st, b"a", &cmd(&["VEC.SET", "s", "x3", "1,1,0,0"])) {
            Value::Error(e) => assert!(e.contains("for this namespace"), "{e}"),
            r => panic!("expected the namespace cap to refuse, got {r:?}"),
        }
        // Reads are untouched by either.
        assert!(matches!(
            run(&mut st, b"b", &cmd(&["VEC.SEARCH", "s", "0,0,1,0", "1"])),
            Value::Array(Some(ref r)) if !r.is_empty()
        ));
        let full = st.stats();
        assert_eq!((full.refused_total, full.refused_ns), (1, 1));
        assert_eq!(full.index_bytes, 4 * one);
        assert_eq!((full.namespaces, full.sets, full.vectors), (2, 2, 4));
        // A delete in a frees room for b.
        assert_eq!(
            run(&mut st, b"a", &cmd(&["VEC.DEL", "s", "x0"])),
            Value::Integer(1)
        );
        assert_eq!(
            run(&mut st, b"b", &cmd(&["VEC.SET", "s", "y1", "0,0,0,1"])),
            ok
        );
        assert_eq!(st.stats().index_bytes, 4 * one);
    }

    #[test]
    fn invalid_names_and_bad_vectors_are_refused() {
        let mut st = Store::new();
        let ns = b"n";
        run(
            &mut st,
            ns,
            &cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "l2"]),
        );
        // NUL in id
        assert!(matches!(
            plan0(&st, ns, &[v("VEC.SET"), v("s"), vec![0], v("1,2")]),
            Plan::Reply(Value::Error(e)) if e.contains("invalid set or id")
        ));
        // non-numeric vector
        assert!(matches!(
            plan0(&st, ns, &cmd(&["VEC.SET", "s", "id", "1,x"])),
            Plan::Reply(Value::Error(e)) if e.contains("not a float")
        ));
        // unknown VEC command
        assert!(matches!(
            plan0(&st, ns, &cmd(&["VEC.NOPE", "s"])),
            Plan::Reply(Value::Error(e)) if e.contains("unknown VEC command")
        ));
    }

    /// D4: the per-namespace index-memory cap refuses a write over the limit
    /// (before persisting), frees on DEL, doesn't double-charge an upsert, and
    /// is isolated per tenant. INFO surfaces the usage and the cap.
    #[test]
    fn index_memory_cap_bounds_a_namespace() {
        let mut st = Store::new();
        st.set_index_cap(450); // 2 dim-4 flat vectors (209 B each by the estimate)
        let ns = b"nsM";
        run(
            &mut st,
            ns,
            &cmd(&["VEC.CREATE", "s", "DIM", "4", "METRIC", "l2"]),
        );

        // Two fit.
        assert_eq!(
            run(&mut st, ns, &cmd(&["VEC.SET", "s", "a", "1,0,0,0"])),
            Value::Simple("OK".into())
        );
        assert_eq!(
            run(&mut st, ns, &cmd(&["VEC.SET", "s", "b", "0,1,0,0"])),
            Value::Simple("OK".into())
        );
        // The third is refused, and refused as a REPLY — no durable write is
        // planned, so the vector is not stored either.
        assert!(matches!(
            plan0(&st, ns, &cmd(&["VEC.SET", "s", "c", "0,0,1,0"])),
            Plan::Reply(Value::Error(e)) if e.starts_with("VECFULL")
        ));
        // An upsert of an existing id is charged only its meta delta (~0), so it
        // still fits even at the cap.
        assert_eq!(
            run(&mut st, ns, &cmd(&["VEC.SET", "s", "a", "9,9,9,9"])),
            Value::Simple("OK".into())
        );
        // Freeing one makes room for a new id again.
        assert_eq!(
            run(&mut st, ns, &cmd(&["VEC.DEL", "s", "a"])),
            Value::Integer(1)
        );
        assert_eq!(
            run(&mut st, ns, &cmd(&["VEC.SET", "s", "c", "0,0,1,0"])),
            Value::Simple("OK".into())
        );
        // INFO surfaces the cap and non-zero usage for this namespace, plus the
        // expiring count (0 here — none of these ids carry a TTL).
        match run(&mut st, ns, &cmd(&["VEC.INFO", "s"])) {
            Value::Array(Some(f)) => {
                assert_eq!(f[8], Value::Bulk(Some(b"expiring".to_vec())));
                assert_eq!(f[9], Value::Integer(0));
                assert_eq!(f[10], Value::Bulk(Some(b"ns_mem_bytes".to_vec())));
                assert!(
                    matches!(f[11], Value::Integer(n) if n > 0),
                    "ns_mem_bytes > 0"
                );
                assert_eq!(f[12], Value::Bulk(Some(b"ns_mem_cap".to_vec())));
                assert_eq!(f[13], Value::Integer(450));
            }
            other => panic!("INFO should be an array, got {other:?}"),
        }

        // A different tenant has its own budget — isolation, the whole point.
        let ns2 = b"nsN";
        run(
            &mut st,
            ns2,
            &cmd(&["VEC.CREATE", "s", "DIM", "4", "METRIC", "l2"]),
        );
        assert_eq!(
            run(&mut st, ns2, &cmd(&["VEC.SET", "s", "a", "1,2,3,4"])),
            Value::Simple("OK".into())
        );
    }

    #[test]
    fn unlimited_cap_never_refuses() {
        let mut st = Store::new(); // cap 0 = unlimited (default)
        let ns = b"nsU";
        run(
            &mut st,
            ns,
            &cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "l2"]),
        );
        for i in 0..64 {
            let id = format!("v{i}");
            assert_eq!(
                run(&mut st, ns, &cmd(&["VEC.SET", "s", &id, "1,1"])),
                Value::Simple("OK".into())
            );
        }
    }

    /// Plan a read-only command at clock `now` and return its reply.
    fn plan_reply(st: &Store, ns: &[u8], args: &[Vec<u8>], now: u64) -> Value {
        match st.plan(ns, args, now) {
            Plan::Reply(v) => v,
            Plan::Write { .. } => panic!("expected a read"),
        }
    }

    /// The ids of a VEC.SEARCH reply, nearest first.
    fn search_ids(v: &Value) -> Vec<Vec<u8>> {
        match v {
            Value::Array(Some(rows)) => rows
                .iter()
                .map(|row| match row {
                    Value::Array(Some(pair)) => match &pair[0] {
                        Value::Bulk(Some(id)) => id.clone(),
                        _ => panic!("id not a bulk string"),
                    },
                    _ => panic!("row not a pair"),
                })
                .collect(),
            other => panic!("expected an array, got {other:?}"),
        }
    }

    #[test]
    fn ttl_masks_get_and_search_then_sweep_reclaims() {
        let mut st = Store::new();
        let ns = b"nsT";
        run(
            &mut st,
            ns,
            &cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "l2"]),
        );

        // "a" is the nearest to the origin but expires at t=1000; b, c never do.
        assert_eq!(
            run_at(
                &mut st,
                ns,
                &cmd(&["VEC.SET", "s", "a", "0,0", "PX", "1000"]),
                0
            ),
            Value::Simple("OK".into())
        );
        run(&mut st, ns, &cmd(&["VEC.SET", "s", "b", "1,0"]));
        run(&mut st, ns, &cmd(&["VEC.SET", "s", "c", "2,0"]));

        // Before the deadline: GET returns "a" and SEARCH ranks it first.
        assert!(matches!(
            plan_reply(&st, ns, &cmd(&["VEC.GET", "s", "a"]), 500),
            Value::Array(Some(_))
        ));
        assert_eq!(
            search_ids(&plan_reply(
                &st,
                ns,
                &cmd(&["VEC.SEARCH", "s", "0,0", "2"]),
                500
            )),
            vec![v("a"), v("b")]
        );

        // At the deadline (expiry is `<= now`): "a" reads absent, and SEARCH masks
        // it even though it is nearest — the over-fetch still yields k live ids.
        assert_eq!(
            plan_reply(&st, ns, &cmd(&["VEC.GET", "s", "a"]), 1000),
            Value::Null
        );
        assert_eq!(
            search_ids(&plan_reply(
                &st,
                ns,
                &cmd(&["VEC.SEARCH", "s", "0,0", "2"]),
                1000
            )),
            vec![v("b"), v("c")],
            "expired nearest is skipped; the next two live ids are returned"
        );

        // The masked vector is still indexed until a sweep runs; the sweep frees
        // exactly it and credits the bytes back, and re-sweeping is a no-op.
        let before = st.ns_mem_bytes(ns);
        assert_eq!(
            st.sweep_expired(1000),
            1,
            "exactly the expired id reclaimed"
        );
        assert!(
            st.ns_mem_bytes(ns) < before,
            "freed the expired entry's index bytes"
        );
        assert_eq!(st.sweep_expired(2000), 0, "nothing left to reclaim");
    }

    #[test]
    fn ttl_ex_px_encode_into_row_and_key() {
        let mut st = Store::new();
        let ns = b"n";
        run(
            &mut st,
            ns,
            &cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "l2"]),
        );

        // EX is seconds, PX is ms; both land as the same absolute deadline in the
        // row header, the Apply, AND the data-plane key's PX (so metering agrees).
        for (opt, unit, ttl) in [("EX", "5", 5000u64), ("PX", "250", 250u64)] {
            match st.plan(ns, &cmd(&["VEC.SET", "s", "x", "1,1", opt, unit]), 10_000) {
                Plan::Write { persist, apply, .. } => {
                    let Some(Persist::Put { val, ttl_ms, .. }) = persist.into_iter().nth(1) else {
                        panic!("SET's second durable step must be the vector row")
                    };
                    assert_eq!(ttl_ms, Some(ttl), "{opt} sets the data-plane PX");
                    let (_, _, expires_at) = decode_vec_row(&val).expect("decode");
                    assert_eq!(expires_at, Some(10_000 + ttl), "row expiry = now + ttl");
                    match apply {
                        Apply::Insert { expires_at, .. } => {
                            assert_eq!(expires_at, Some(10_000 + ttl))
                        }
                        _ => panic!("a SET is an Insert"),
                    }
                }
                _ => panic!("{opt} SET should plan a write"),
            }
        }

        // EX and PX together, and a non-positive/garbage duration, are refused.
        assert!(matches!(
            plan0(&st, ns, &cmd(&["VEC.SET", "s", "x", "1,1", "EX", "5", "PX", "5"])),
            Plan::Reply(Value::Error(e)) if e.contains("mutually exclusive")
        ));
        assert!(matches!(
            plan0(&st, ns, &cmd(&["VEC.SET", "s", "x", "1,1", "EX", "0"])),
            Plan::Reply(Value::Error(e)) if e.contains("positive")
        ));
    }

    #[test]
    fn ttl_reset_without_option_makes_id_permanent() {
        let mut st = Store::new();
        let ns = b"n";
        run(
            &mut st,
            ns,
            &cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "l2"]),
        );
        // Set with a short TTL, confirm it expires, then re-set with no TTL: the
        // id becomes permanent again (SET-clears-TTL, like a native key).
        run_at(
            &mut st,
            ns,
            &cmd(&["VEC.SET", "s", "y", "3,3", "PX", "100"]),
            0,
        );
        assert_eq!(
            plan_reply(&st, ns, &cmd(&["VEC.GET", "s", "y"]), 200),
            Value::Null,
            "expired at its deadline"
        );
        run_at(&mut st, ns, &cmd(&["VEC.SET", "s", "y", "3,3"]), 200);
        assert!(
            matches!(
                plan_reply(&st, ns, &cmd(&["VEC.GET", "s", "y"]), 10_000),
                Value::Array(Some(_))
            ),
            "a re-set with no EX/PX is permanent"
        );
    }

    #[test]
    fn ttl_restored_from_durable_row_on_rebuild() {
        // The D3 rebuild path: a durable row's expiry header decodes back to an
        // absolute deadline, and committing the Insert re-establishes the TTL, so
        // a restarted co-processor masks the same vectors the crashed one would.
        let mut st = Store::new();
        let ns = b"n";
        st.commit(
            ns,
            Apply::CreateSet {
                set: v("s"),
                dim: 2,
                metric: Metric::L2,
                index: IndexKind::Flat,
                quant: Quant::None,
                fields: Vec::new(),
            },
        );
        let row = encode_vec_row(&[9.0, 9.0], None, Some(500));
        let (vec, meta, expires_at) = decode_vec_row(&row).expect("decode");
        assert_eq!(expires_at, Some(500));
        st.commit(
            ns,
            Apply::Insert {
                set: v("s"),
                id: v("z"),
                vec,
                meta,
                expires_at,
            },
        );
        assert!(matches!(
            plan_reply(&st, ns, &cmd(&["VEC.GET", "s", "z"]), 499),
            Value::Array(Some(_))
        ));
        assert_eq!(
            plan_reply(&st, ns, &cmd(&["VEC.GET", "s", "z"]), 500),
            Value::Null,
            "restored TTL fires on the same deadline"
        );
    }

    #[test]
    fn ttl_introspection_via_vec_ttl_and_info() {
        let mut st = Store::new();
        let ns = b"nsM";
        run(
            &mut st,
            ns,
            &cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "l2"]),
        );

        // Permanent id -> -1; absent id -> -2 (PTTL semantics).
        run_at(&mut st, ns, &cmd(&["VEC.SET", "s", "perm", "1,0"]), 0);
        assert_eq!(
            plan_reply(&st, ns, &cmd(&["VEC.TTL", "s", "perm"]), 0),
            Value::Integer(-1)
        );
        assert_eq!(
            plan_reply(&st, ns, &cmd(&["VEC.TTL", "s", "nope"]), 0),
            Value::Integer(-2)
        );

        // TTL'd id -> remaining ms, counting down with the clock; -2 once expired.
        run_at(
            &mut st,
            ns,
            &cmd(&["VEC.SET", "s", "temp", "0,1", "PX", "1000"]),
            0,
        );
        assert_eq!(
            plan_reply(&st, ns, &cmd(&["VEC.TTL", "s", "temp"]), 200),
            Value::Integer(800)
        );
        assert_eq!(
            plan_reply(&st, ns, &cmd(&["VEC.TTL", "s", "temp"]), 1000),
            Value::Integer(-2)
        );

        // INFO's `expiring` counts only LIVE TTL'd ids ('perm' excluded)...
        match plan_reply(&st, ns, &cmd(&["VEC.INFO", "s"]), 200) {
            Value::Array(Some(f)) => {
                assert_eq!(f[8], Value::Bulk(Some(b"expiring".to_vec())));
                assert_eq!(f[9], Value::Integer(1), "only 'temp' is expiring");
            }
            other => panic!("INFO array, got {other:?}"),
        }
        // ...and drops to 0 once 'temp' lapses — a live count, not the raw map size.
        match plan_reply(&st, ns, &cmd(&["VEC.INFO", "s"]), 2000) {
            Value::Array(Some(f)) => assert_eq!(f[9], Value::Integer(0)),
            other => panic!("INFO array, got {other:?}"),
        }
    }

    #[test]
    fn ttl_expire_and_persist_manage_lifetime_without_resend() {
        let mut st = Store::new();
        let ns = b"nsE";
        run(
            &mut st,
            ns,
            &cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "l2"]),
        );
        run_at(&mut st, ns, &cmd(&["VEC.SET", "s", "x", "1,2"]), 0); // permanent

        // EXPIRE gives a permanent id a TTL (reply 1); TTL reflects it, and the
        // vector is untouched (no resend).
        assert_eq!(
            run_at(&mut st, ns, &cmd(&["VEC.EXPIRE", "s", "x", "5"]), 1_000),
            Value::Integer(1)
        );
        assert_eq!(
            plan_reply(&st, ns, &cmd(&["VEC.TTL", "s", "x"]), 1_000),
            Value::Integer(5000)
        );
        assert_eq!(
            plan_reply(&st, ns, &cmd(&["VEC.GET", "s", "x"]), 1_000),
            Value::Array(Some(vec![Value::Bulk(Some(b"1,2".to_vec()))]))
        );

        // The durable-row rewrite carries the new deadline + data-plane PX, so a
        // rebuild and metering agree without the vector being resent.
        match st.plan(ns, &cmd(&["VEC.EXPIRE", "s", "x", "7"]), 2_000) {
            Plan::Write { persist, .. } => {
                assert_eq!(
                    persist.len(),
                    1,
                    "EXPIRE rewrites a row; membership is unchanged so it needs no index op"
                );
                let Some(Persist::Put { val, ttl_ms, .. }) = persist.into_iter().next() else {
                    panic!("EXPIRE must rewrite the vector row")
                };
                assert_eq!(ttl_ms, Some(7000));
                let (vec, _, expires_at) = decode_vec_row(&val).expect("decode");
                assert_eq!(vec, vec![1.0, 2.0], "stored vector preserved across EXPIRE");
                assert_eq!(expires_at, Some(2_000 + 7000));
            }
            _ => panic!("EXPIRE on a live id plans a write"),
        }

        // PEXPIRE overrides in ms; PERSIST removes the TTL (reply 1 -> then -1).
        assert_eq!(
            run_at(&mut st, ns, &cmd(&["VEC.PEXPIRE", "s", "x", "250"]), 1_000),
            Value::Integer(1)
        );
        assert_eq!(
            plan_reply(&st, ns, &cmd(&["VEC.TTL", "s", "x"]), 1_000),
            Value::Integer(250)
        );
        assert_eq!(
            run_at(&mut st, ns, &cmd(&["VEC.PERSIST", "s", "x"]), 1_000),
            Value::Integer(1)
        );
        assert_eq!(
            plan_reply(&st, ns, &cmd(&["VEC.TTL", "s", "x"]), 1_000),
            Value::Integer(-1)
        );
        // PERSIST with no TTL, and EXPIRE/PERSIST on an absent id, all report 0.
        assert_eq!(
            run_at(&mut st, ns, &cmd(&["VEC.PERSIST", "s", "x"]), 1_000),
            Value::Integer(0)
        );
        assert_eq!(
            run_at(&mut st, ns, &cmd(&["VEC.EXPIRE", "s", "ghost", "5"]), 1_000),
            Value::Integer(0)
        );
        assert_eq!(
            run_at(&mut st, ns, &cmd(&["VEC.PERSIST", "s", "ghost"]), 1_000),
            Value::Integer(0)
        );
    }

    fn is_err(v: &Value, part: &str) -> bool {
        matches!(v, Value::Error(e) if e.contains(part))
    }

    /// ADR-0059 release R: `VEC.SEARCH` refuses what it does not know, where
    /// every release before it passed over it. Without `--vec-filters` that
    /// includes `FILTER`, and `VEC.CREATE` refuses `FIELD`.
    #[test]
    fn search_refuses_an_option_it_does_not_know() {
        let mut st = Store::new();
        let ns = b"n";
        run(
            &mut st,
            ns,
            &cmd(&[
                "VEC.CREATE",
                "s",
                "DIM",
                "2",
                "METRIC",
                "l2",
                "INDEX",
                "hnsw",
            ]),
        );
        run(&mut st, ns, &cmd(&["VEC.SET", "s", "a", "0,0"]));
        let search = |st: &Store, extra: &[&str]| {
            let mut c = cmd(&["VEC.SEARCH", "s", "0,0", "1"]);
            c.extend(cmd(extra));
            plan_reply(st, ns, &c, 0)
        };
        for (extra, part) in [
            (&["FOO", "1"][..], "unknown VEC.SEARCH option"),
            (
                &["EF", "8", "WITHSCORES", "1"][..],
                "unknown VEC.SEARCH option",
            ),
            (&["EF"][..], "EF needs a value"),
            (&["EF", "many"][..], "EF must be a non-negative integer"),
            (
                &["RERANK", "-1"][..],
                "RERANK must be a non-negative integer",
            ),
            (&["EF", "8", "ef", "9"][..], "EF is given twice"),
            (
                &["FILTER", "@t:{x}"][..],
                "vector filters (FIELD and FILTER) are off",
            ),
        ] {
            let got = search(&st, extra);
            assert!(is_err(&got, part), "{extra:?}: {got:?}");
        }
        for extra in [
            &[][..],
            &["EF", "8"],
            &["RERANK", "4", "EF", "8"],
            &["ef", "0"],
        ] {
            assert_eq!(search_ids(&search(&st, extra)), vec![v("a")], "{extra:?}");
        }
        let create = cmd(&[
            "VEC.CREATE",
            "f",
            "DIM",
            "2",
            "METRIC",
            "l2",
            "FIELD",
            "t",
            "TAG",
        ]);
        assert!(is_err(&run(&mut st, ns, &create), "are off"));
        assert!(!st.stats().filters);
    }

    #[test]
    fn create_checks_each_field() {
        let mut st = Store::new();
        st.set_filters(true);
        let ns = b"n";
        let create = |extra: &[&str]| {
            let mut c = cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "l2"]);
            c.extend(cmd(extra));
            c
        };
        for (extra, part) in [
            (&["FIELD", "t"][..], "FIELD <name> TAG|NUMERIC"),
            (&["FIELD", "a-b", "TAG"][..], "letters, digits and _"),
            (&["FIELD", "t", "TEXT"][..], "TAG or NUMERIC"),
            (
                &["FIELD", "t", "TAG", "FIELD", "t", "NUMERIC"][..],
                "given twice",
            ),
            (
                &["FIELD", "t", "TAG", "SORTABLE", "x"][..],
                "unknown VEC.CREATE option",
            ),
        ] {
            let got = run(&mut st, ns, &create(extra));
            assert!(is_err(&got, part), "{extra:?}: {got:?}");
        }
        let mut many = Vec::new();
        let names: Vec<String> = (0..=filter::MAX_FIELDS).map(|i| format!("f{i}")).collect();
        for n in &names {
            many.extend(["FIELD", n.as_str(), "TAG"]);
        }
        assert!(is_err(&run(&mut st, ns, &create(&many)), "at most 16"));
        assert_eq!(st.set_count(), 0, "nothing refused was made");
        // The fields ride the config row's fifth field, after `none`.
        match plan0(
            &st,
            ns,
            &create(&[
                "FIELD", "folder", "tag", "INDEX", "hnsw", "FIELD", "price", "NUMERIC",
            ]),
        ) {
            Plan::Write { persist, .. } => {
                let Some(Persist::Put { val, .. }) = persist.into_iter().nth(1) else {
                    panic!("the config row is CREATE's second step")
                };
                assert_eq!(val, b"2|l2|hnsw|none|folder:tag,price:numeric".to_vec());
            }
            _ => panic!("create should plan a write"),
        }
    }

    /// Every kind of set answers a filtered search with the nearest matches,
    /// and only matches, as an exact search over the matches would.
    #[test]
    fn a_filtered_search_answers_from_every_kind_of_set() {
        for extra in [
            &[][..],
            &["INDEX", "hnsw"],
            &["INDEX", "hnsw", "QUANT", "sq8"],
            &["INDEX", "hnsw", "QUANT", "bin"],
        ] {
            let mut st = Store::new();
            st.set_filters(true);
            let ns = b"n";
            let mut c = cmd(&[
                "VEC.CREATE",
                "s",
                "DIM",
                "2",
                "METRIC",
                "l2",
                "FIELD",
                "folder",
                "TAG",
                "FIELD",
                "price",
                "NUMERIC",
            ]);
            c.extend(cmd(extra));
            assert_eq!(
                run(&mut st, ns, &c),
                Value::Simple("OK".into()),
                "{extra:?}"
            );
            // Ten points along a line: folder `a` for the even ones, price
            // its position, and one with no JSON at all.
            for i in 0..10 {
                let meta = format!(
                    r#"{{"folder":"{}","price":{i}}}"#,
                    if i % 2 == 0 { "a" } else { "b" }
                );
                let set = cmd(&[
                    "VEC.SET",
                    "s",
                    &format!("p{i}"),
                    &format!("{i},0"),
                    "META",
                    &meta,
                ]);
                assert_eq!(run(&mut st, ns, &set), Value::Simple("OK".into()));
            }
            run(
                &mut st,
                ns,
                &cmd(&["VEC.SET", "s", "raw", "0,0", "META", "plain bytes"]),
            );
            let ids = |st: &Store, f: &str, k: &str| {
                search_ids(&plan_reply(
                    st,
                    ns,
                    &cmd(&["VEC.SEARCH", "s", "0,0", k, "FILTER", f]),
                    0,
                ))
            };
            let want = |names: &[&str]| -> Vec<Vec<u8>> { names.iter().map(|n| v(n)).collect() };
            assert_eq!(
                ids(&st, "@folder:{a}", "3"),
                want(&["p0", "p2", "p4"]),
                "{extra:?}"
            );
            assert_eq!(
                ids(&st, "@folder:{b}", "2"),
                want(&["p1", "p3"]),
                "{extra:?}"
            );
            assert_eq!(
                ids(&st, "@price:[(3 6]", "9"),
                want(&["p4", "p5", "p6"]),
                "{extra:?}"
            );
            assert_eq!(
                ids(&st, "@folder:{a} @price:[5 +inf]", "9"),
                want(&["p6", "p8"])
            );
            assert_eq!(
                ids(&st, "(@folder:{b} | @price:[0 0])", "3"),
                want(&["p0", "p1", "p3"])
            );
            assert_eq!(ids(&st, "-@folder:{a|b}", "3"), want(&["raw"]), "{extra:?}");
            assert_eq!(ids(&st, "@folder:{c}", "3"), want(&[]));
            assert_eq!(ids(&st, "*", "1").len(), 1);
            let bad = plan_reply(
                &st,
                ns,
                &cmd(&["VEC.SEARCH", "s", "0,0", "3", "FILTER", "@nope:{a}"]),
                0,
            );
            assert!(is_err(&bad, "ERR FILTER names nope"), "{bad:?}");
            // VEC.INFO says what it declared and what that holds.
            let info = plan_reply(&st, ns, &cmd(&["VEC.INFO", "s"]), 0);
            assert_eq!(
                info_field(&info, "fields"),
                Value::Array(Some(
                    ["folder", "tag", "price", "numeric"]
                        .iter()
                        .map(|x| Value::Bulk(Some(v(x))))
                        .collect()
                ))
            );
            assert!(
                matches!(info_field(&info, "field_bytes"), Value::Integer(n) if n > 0),
                "{extra:?}"
            );
            // Deleting a match takes it out, and an upsert moves it.
            run(&mut st, ns, &cmd(&["VEC.DEL", "s", "p0"]));
            run(
                &mut st,
                ns,
                &cmd(&["VEC.SET", "s", "p2", "2,0", "META", r#"{"folder":"b"}"#]),
            );
            assert_eq!(
                ids(&st, "@folder:{a}", "3"),
                want(&["p4", "p6", "p8"]),
                "{extra:?}"
            );
        }
    }

    #[test]
    fn expired_vectors_stay_out_of_a_filtered_search() {
        let mut st = Store::new();
        st.set_filters(true);
        let ns = b"n";
        run(
            &mut st,
            ns,
            &cmd(&[
                "VEC.CREATE",
                "s",
                "DIM",
                "2",
                "METRIC",
                "l2",
                "INDEX",
                "hnsw",
                "FIELD",
                "t",
                "TAG",
            ]),
        );
        let m = r#"{"t":"x"}"#;
        run_at(
            &mut st,
            ns,
            &cmd(&["VEC.SET", "s", "soon", "0,0", "META", m, "PX", "10"]),
            0,
        );
        run_at(
            &mut st,
            ns,
            &cmd(&["VEC.SET", "s", "kept", "5,0", "META", m]),
            0,
        );
        let q = cmd(&["VEC.SEARCH", "s", "0,0", "1", "FILTER", "@t:{x}"]);
        assert_eq!(search_ids(&plan_reply(&st, ns, &q, 5)), vec![v("soon")]);
        assert_eq!(search_ids(&plan_reply(&st, ns, &q, 20)), vec![v("kept")]);
    }

    /// The fields ride the config row, so a rebuild indexes them again from
    /// the vectors' rows; and a release from before ADR-0059 loads the same
    /// set from the same rows, without them.
    #[test]
    fn a_set_with_fields_rebuilds_and_loads_on_a_release_before_them() {
        fn decode_before_adr_0059(val: &[u8]) -> Option<(usize, Metric, IndexKind, Quant)> {
            // rc.83's decode_config, which reads four fields.
            let s = std::str::from_utf8(val).ok()?;
            let mut it = s.split('|');
            let dim = it.next()?.parse().ok()?;
            let metric = Metric::parse(it.next()?.as_bytes())?;
            let index = match it.next() {
                Some(k) => IndexKind::parse(k.as_bytes())?,
                None => IndexKind::Flat,
            };
            let quant = it
                .next()
                .and_then(|q| Quant::parse(q.as_bytes()))
                .unwrap_or(Quant::None);
            Some((dim, metric, index, quant))
        }
        let fields = vec![Field {
            name: "t".into(),
            kind: FieldKind::Tag,
        }];
        for (index, quant) in [
            (IndexKind::Flat, Quant::None),
            (IndexKind::Hnsw, Quant::None),
            (IndexKind::Hnsw, Quant::Bin),
        ] {
            let row = encode_config(3, Metric::L2, index, quant, &fields);
            assert_eq!(
                decode_before_adr_0059(&row),
                Some((3, Metric::L2, index, quant))
            );
            assert_eq!(
                decode_config(&row),
                Some((3, Metric::L2, index, quant, fields.clone()))
            );
        }
        // A rebuild: the config row's fields, then each vector's row.
        let row = encode_config(2, Metric::L2, IndexKind::Hnsw, Quant::None, &fields);
        let (dim, metric, index, quant, fields) = decode_config(&row).expect("decodes");
        let mut st = Store::new();
        st.set_filters(true);
        let ns = b"n";
        st.commit(
            ns,
            Apply::CreateSet {
                set: v("s"),
                dim,
                metric,
                index,
                quant,
                fields,
            },
        );
        for (id, x, t) in [("a", "0", "x"), ("b", "1", "y"), ("c", "2", "x")] {
            let meta = format!(r#"{{"t":"{t}"}}"#);
            let (vec, meta, expires_at) = decode_vec_row(&encode_packed_row(
                &parse_vector(format!("{x},0").as_bytes()).expect("vec"),
                Some(meta.as_bytes()),
                None,
            ))
            .expect("row");
            st.commit(
                ns,
                Apply::Insert {
                    set: v("s"),
                    id: v(id),
                    vec,
                    meta,
                    expires_at,
                },
            );
        }
        let q = cmd(&["VEC.SEARCH", "s", "0,0", "5", "FILTER", "@t:{x}"]);
        assert_eq!(
            search_ids(&plan_reply(&st, ns, &q, 0)),
            vec![v("a"), v("c")]
        );
        // With the flag off the set keeps its fields, and refuses the filter.
        st.set_filters(false);
        assert!(is_err(&plan_reply(&st, ns, &q, 0), "are off"));
    }

    /// BUG-0265: a rebuild holds each durable row to the shape `VEC.SET`
    /// would have, since the tenant can write those rows too. Unchecked, a
    /// 10-float row in a 3-d `QUANT bin` set panicked. The caps it leaves to
    /// new writes (ADR-0017 D3), which `coproc_vec_drill.sh` holds it to.
    #[test]
    fn a_rebuild_holds_each_row_to_what_vec_set_would() {
        for extra in [
            &[][..],
            &["INDEX", "hnsw"],
            &["INDEX", "hnsw", "QUANT", "sq8"],
            &["INDEX", "hnsw", "QUANT", "bin"],
        ] {
            let mut st = Store::new();
            let ns = b"n";
            let mut c = cmd(&["VEC.CREATE", "s", "DIM", "3", "METRIC", "l2"]);
            c.extend(cmd(extra));
            run(&mut st, ns, &c);
            let put = |st: &mut Store, set: &str, id: &str, vec: Vec<f32>| {
                st.install_vector(ns, v(set), v(id), vec, None, None)
            };
            assert_eq!(put(&mut st, "s", "a", vec![1.0, 0.0, 0.0]), Ok(()));
            assert_eq!(
                put(
                    &mut st,
                    "s",
                    "b",
                    vec![1.0, 0.0, 0.0, 5.0, 6.0, 7.0, 8.0, 9.0, 1.0, 2.0]
                ),
                Err(LeftOut::WrongDim),
                "{extra:?}"
            );
            assert_eq!(put(&mut st, "s", "c", vec![1.0]), Err(LeftOut::WrongDim));
            assert_eq!(
                put(&mut st, "nope", "d", vec![1.0, 0.0, 0.0]),
                Err(LeftOut::NoSet)
            );
            assert_eq!(
                put(&mut st, "s", "", vec![1.0, 0.0, 0.0]),
                Err(LeftOut::BadName)
            );
            assert_eq!(
                search_ids(&plan_reply(
                    &st,
                    ns,
                    &cmd(&["VEC.SEARCH", "s", "1,0,0", "5"]),
                    0
                )),
                vec![v("a")],
                "{extra:?}: only the row that fits is searched"
            );
        }
    }
}
