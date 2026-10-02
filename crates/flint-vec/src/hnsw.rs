// SPDX-License-Identifier: Elastic-2.0
//! An HNSW (Hierarchical Navigable Small World) index — ADR-0017 v0.2. It turns
//! flat's O(N) scan into an O(log N)-ish graph traversal behind the SAME
//! `VectorSet` surface and the SAME durable rows (the graph is derived; on a
//! cold start it rebuilds from the vectors like flat does).
//!
//! Internally distances are computed so SMALLER is nearer (the natural form for
//! a nearest-neighbour graph); the public results convert back to flat's
//! "higher is nearer" score so `VEC.SEARCH` replies are identical in shape and
//! ordering whichever index a set uses.
//!
//! Deletion marks a node: it stays in the graph for ROUTING but never appears
//! in results, until the next insert takes its slot (BUG-0197). A cold-start
//! rebuild re-inserts only live vectors from KV.
//!
//! A node is a SLOT: an index into flat arrays, one a field (ADR-0049 step 3),
//! not a struct with a heap allocation for each of its parts. The layer-0
//! links, which every node has, are a fixed stride of one array; the few nodes
//! on higher layers keep those in a map.

use crate::kernel::{self, CENTROIDS, Sq8};
use crate::quant::{self, BinQuery, Book};
use crate::vecfile::VecFile;
use crate::{Metric, Quant};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;
use std::thread::JoinHandle;

const M: usize = 16; // neighbours per node on upper layers
const M0: usize = 32; // neighbours at layer 0 (2*M — denser base layer)
/// A slot's layer-0 entry: its link count, then room for `M0` links.
const STRIDE0: usize = M0 + 1;
const EF_CONSTRUCTION: usize = 100;
/// Default `ef` for a query when `VEC.SEARCH` gives no `EF` — the recall/latency
/// knob. Higher searches more of the graph: better recall, more work.
pub const EF_SEARCH_DEFAULT: usize = 64;
/// How many of a reused slot's live neighbourhood each of its old neighbours
/// re-selects from (BUG-0197), besides its own links. Measured on 5,000 vectors
/// replaced half at a time for ten rounds: 128 kept recall@10 at a fresh
/// build's, 64 lost up to 0.014 of it on clustered data, and no bound let the
/// selection walk ~1,000 candidates for each neighbour.
const REPAIR_POOL: usize = 4 * M0;
/// Live vectors a `QUANT pq` set holds before it trains its codebook on them
/// and re-encodes (ADR-0049 D1's training threshold). k-means wants many
/// points a centroid, and a subspace has 256; below this a set keeps full
/// vectors in the graph.
pub const PQ_TRAIN_DEFAULT: usize = 10_000;

/// Hashes a slot number for the sets a walk keeps: one multiply. The default
/// SipHash is built to withstand keys an adversary picks, which slot numbers,
/// handed out by this index, are not.
#[derive(Default)]
struct SlotHasher(u64);

impl Hasher for SlotHasher {
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        }
    }
    fn write_u32(&mut self, n: u32) {
        self.0 = (n as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

type SlotSet = HashSet<u32, BuildHasherDefault<SlotHasher>>;

/// Who is walking the graph, which decides what a deleted node is to the walk.
#[derive(Clone, Copy, PartialEq)]
enum Walk {
    /// A search: deleted nodes route but are never returned.
    Query,
    /// Inserting node `.0`: deleted nodes are candidates too, since they still
    /// route, and a layer whose nodes are mostly deleted would otherwise give
    /// the new node no links and the next layer no place to start. The node
    /// itself is hidden: a reused slot can be reached through a link that
    /// pointed at its previous occupant (BUG-0197).
    Insert(u32),
}

impl Walk {
    fn hides(self, n: u32) -> bool {
        self == Walk::Insert(n)
    }
}

/// Make room in `v` for `extra` more by a quarter of what it holds, not by the
/// doubling `Vec` does on its own. These arrays hold every node, and just past
/// a doubling half of one is empty: most of what BUG-0198 measured beyond the
/// vectors themselves.
fn grow<T>(v: &mut Vec<T>, extra: usize) {
    if v.len() + extra > v.capacity() {
        v.reserve_exact((v.capacity() / 4).max(extra));
    }
}

/// Write `vals` as slot `slot` of an array `vals.len()` wide a slot, appending
/// when `slot` is the next one. For the small fixed-width fields only: an array
/// of whole vectors would carry up to a quarter of a vector of spare room each,
/// which at 1536 dimensions is more than the rest of a node.
fn put_slot<T: Copy>(v: &mut Vec<T>, slot: u32, vals: &[T]) {
    let at = slot as usize * vals.len();
    if at == v.len() {
        grow(v, vals.len());
        v.extend_from_slice(vals);
    } else {
        v[at..at + vals.len()].copy_from_slice(vals);
    }
}

/// Set slot `slot` of an array of boxes, appending when it is the next one.
/// What holds a slot's vector or code: one allocation of exactly its size.
fn put_box<B>(v: &mut Vec<B>, slot: u32, b: B) {
    if slot as usize == v.len() {
        grow(v, 1);
        v.push(b);
    } else {
        v[slot as usize] = b;
    }
}

/// How the graph holds its vectors (ADR-0049 D1), one box a slot.
enum Codes {
    /// The full float32 vectors: the index without codes, and its re-rank
    /// source itself.
    F32(Vec<Box<[f32]>>),
    /// Per-vector 8-bit scalar quantization: value `i` of a slot is
    /// `lo + step * byte[i]`, with the slot's `[lo, step]` in `scale`. One byte
    /// a dimension instead of four, and no training: the range is the vector's
    /// own, so an online insert needs nothing learned beforehand.
    Sq8 {
        bytes: Vec<Box<[u8]>>,
        scale: Vec<[f32; 2]>,
    },
    /// One bit a dimension after a fixed rotation, and the factor the
    /// estimate divides by (`quant.rs`), each a fixed width a slot in one
    /// array: a box a slot would cost more than a 192-byte code's spare room.
    Bin {
        bits: Vec<u8>,
        factor: Vec<f32>,
        dim: usize,
    },
    /// Product-quantization codes ([`Pq`]).
    Pq(Box<Pq>),
}

/// Slots whose codes the thread that encodes a PQ set's vectors may leave to
/// the write that installs its codebook: about 20-40 ms of encoding at 1536
/// dimensions, the most an install may hold the store for.
const CATCH_UP: usize = 1024;

/// A PQ set's codes (ADR-0049 item 2), `dim / 16` bytes a slot (`quant.rs`).
///
/// Its codebook is learned from its own vectors once it holds enough
/// ([`PQ_TRAIN_DEFAULT`]), and until it is installed the graph walks on the
/// full vectors, exactly. Learning it took 16 s at 10,000 vectors of 1536
/// dimensions, and a write holds the whole store, every tenant's, while it
/// runs: so a thread trains the codebook and encodes the vectors, which it
/// shares rather than copies, and the set keeps taking writes meanwhile. A
/// write or the binary's sweeper ([`Hnsw::tend`]) collects the result,
/// re-encodes what was written since, and installs it.
///
/// Every estimate goes through PQ's squared distance, whose error scales with
/// the gap between two vectors rather than with either one, as a dot
/// product's does: a cosine set encodes unit vectors, and an inner product
/// comes from the distance and the norms.
struct Pq {
    dim: usize,
    unit: bool,
    /// The codebook, once installed: from then on `codes` holds every slot's
    /// code and the walk uses them.
    book: Option<Arc<Book>>,
    codes: Vec<u8>,
    /// The full vectors until the codebook is installed, by slot.
    raw: Vec<Arc<[f32]>>,
    /// Training or encoding under way, and what it has returned so far.
    pending: Option<Pending>,
}

struct Pending {
    job: Option<JoinHandle<Encoded>>,
    /// The codebook, once a thread has trained it.
    learned: Option<Arc<Book>>,
    /// Which slots' codes in [`Pq::codes`] a thread returned and no write has
    /// replaced since.
    fresh: Vec<bool>,
    /// Slots written since the running job took their vectors: what it
    /// returns for them is stale.
    dirty: Vec<u32>,
}

/// What a PQ thread returns: the codebook it trained or was given, and the
/// codes of the slots it was given, `pq_bytes(dim)` each, in order.
struct Encoded {
    book: Arc<Book>,
    slots: Vec<u32>,
    codes: Vec<u8>,
}

impl Pq {
    fn new(unit: bool) -> Pq {
        Pq {
            dim: 0,
            unit,
            book: None,
            codes: Vec::new(),
            raw: Vec::new(),
            pending: None,
        }
    }

    fn put(&mut self, slot: u32, v: &[f32], norm: f32) {
        self.dim = v.len();
        let width = quant::pq_bytes(v.len());
        if let Some(book) = &self.book {
            let mut code = vec![0u8; width];
            book.encode(&pq_input(v, norm, self.unit), &mut code);
            put_slot(&mut self.codes, slot, &code);
            return;
        }
        put_box(&mut self.raw, slot, Arc::from(v));
        if let Some(p) = &mut self.pending {
            if let Some(f) = p.fresh.get_mut(slot as usize) {
                *f = false;
            }
            p.dirty.push(slot);
        }
    }

    /// Slot `slot`'s full vector, while the walk uses them.
    fn raw(&self, slot: u32) -> Option<&[f32]> {
        self.book.is_none().then(|| &self.raw[slot as usize][..])
    }

    /// RAM held for the set rather than a slot: the codebook once installed;
    /// before, the full vectors and whatever a thread has returned. O(1).
    fn held_bytes(&self) -> usize {
        if let Some(b) = &self.book {
            return b.bytes();
        }
        let arc = std::mem::size_of::<Arc<[f32]>>();
        let mut n = self.raw.len() * (self.dim * 4 + arc) + self.raw.capacity() * arc;
        if let Some(p) = &self.pending {
            n += self.codes.capacity() + p.fresh.capacity() + p.dirty.capacity() * 4;
            n += p.learned.as_ref().map_or(0, |b| b.bytes());
        }
        n
    }

    /// Take a step towards an installed codebook: start the thread when the
    /// set is `ready`, collect a finished one, and install once few enough
    /// slots are left to encode here. With `wait`, block until installed.
    fn step(&mut self, ready: bool, wait: bool) {
        while self.book.is_none() && !self.raw.is_empty() {
            let Some(p) = &mut self.pending else {
                if !ready {
                    return;
                }
                let all: Vec<u32> = (0..self.raw.len() as u32).collect();
                let Some(job) = self.spawn(None, &all) else {
                    return; // no thread to be had: try again on a later step
                };
                self.pending = Some(Pending {
                    job: Some(job),
                    learned: None,
                    fresh: Vec::new(),
                    dirty: Vec::new(),
                });
                continue;
            };
            let Some(job) = p.job.take_if(|j| wait || j.is_finished()) else {
                return;
            };
            let width = quant::pq_bytes(self.dim);
            if let Ok(done) = job.join() {
                self.codes.resize(self.raw.len() * width, 0);
                p.fresh.resize(self.raw.len(), false);
                p.dirty.sort_unstable();
                for (s, code) in done.slots.iter().zip(done.codes.chunks_exact(width)) {
                    if p.dirty.binary_search(s).is_err() {
                        let at = *s as usize * width;
                        self.codes[at..at + width].copy_from_slice(code);
                        p.fresh[*s as usize] = true;
                    }
                }
                p.learned = Some(done.book);
            }
            p.dirty.clear();
            let Some(book) = p.learned.clone() else {
                self.pending = None; // the training thread failed: start over
                continue;
            };
            let todo: Vec<u32> = (0..self.raw.len() as u32)
                .filter(|&s| !p.fresh[s as usize])
                .collect();
            if todo.len() > CATCH_UP {
                let job = self.spawn(Some(book), &todo);
                if let Some(p) = &mut self.pending {
                    p.job = job;
                }
                continue;
            }
            for &s in &todo {
                let v = &self.raw[s as usize];
                let at = s as usize * width;
                book.encode(
                    &pq_input(v, l2norm(v), self.unit),
                    &mut self.codes[at..at + width],
                );
            }
            self.codes.shrink_to_fit();
            self.raw = Vec::new();
            self.pending = None;
            self.book = Some(book);
        }
    }

    /// A thread that encodes `slots`' vectors, training a codebook on them
    /// first when not given one. `None` when no thread can be started.
    fn spawn(&self, book: Option<Arc<Book>>, slots: &[u32]) -> Option<JoinHandle<Encoded>> {
        let vecs: Vec<Arc<[f32]>> = slots
            .iter()
            .map(|&s| self.raw[s as usize].clone())
            .collect();
        let (slots, dim, unit) = (slots.to_vec(), self.dim, self.unit);
        std::thread::Builder::new()
            .name("flint-vec-pq".into())
            .spawn(move || {
                let inputs: Vec<Cow<'_, [f32]>> =
                    vecs.iter().map(|v| pq_input(v, l2norm(v), unit)).collect();
                let book = book.unwrap_or_else(|| {
                    let samples: Vec<&[f32]> = inputs.iter().map(|v| &v[..]).collect();
                    Arc::new(Book::train(&samples, dim))
                });
                let width = quant::pq_bytes(dim);
                let mut codes = vec![0u8; inputs.len() * width];
                for (v, code) in inputs.iter().zip(codes.chunks_exact_mut(width)) {
                    book.encode(v, code);
                }
                Encoded { book, slots, codes }
            })
            .ok()
    }

    fn query(&self, v: &[f32], norm: f32) -> Prep {
        match &self.book {
            Some(b) => Prep::Pq(b.table(&pq_input(v, norm, self.unit))),
            None => Prep::Plain,
        }
    }
}

/// A query, or a node being inserted, prepared once for the many distances a
/// walk takes to codes: a 1-bit code's rotation and table, a PQ code's table.
struct Query<'a> {
    v: &'a [f32],
    norm: f32,
    prep: Prep,
}

enum Prep {
    Plain,
    Bin(BinQuery),
    Pq(Vec<[f32; CENTROIDS]>),
}

impl Codes {
    fn new(quant: Quant, metric: Metric) -> Codes {
        match quant {
            Quant::None => Codes::F32(Vec::new()),
            Quant::Sq8 => Codes::Sq8 {
                bytes: Vec::new(),
                scale: Vec::new(),
            },
            Quant::Bin => Codes::Bin {
                bits: Vec::new(),
                factor: Vec::new(),
                dim: 0,
            },
            Quant::Pq => Codes::Pq(Box::new(Pq::new(metric == Metric::Cosine))),
        }
    }

    fn put(&mut self, slot: u32, v: &[f32], norm: f32) {
        match self {
            Codes::F32(f) => put_box(f, slot, v.into()),
            Codes::Bin { bits, factor, dim } => {
                *dim = v.len();
                let mut code = vec![0u8; quant::bin_bytes(v.len())];
                let f = quant::bin_encode(v, norm, &mut code);
                put_slot(bits, slot, &code);
                put_slot(factor, slot, &[f]);
            }
            Codes::Pq(p) => p.put(slot, v, norm),
            Codes::Sq8 { bytes, scale } => {
                let (lo, hi) = v
                    .iter()
                    .fold((f32::INFINITY, f32::NEG_INFINITY), |(l, h), &x| {
                        (l.min(x), h.max(x))
                    });
                let step = if hi > lo { (hi - lo) / 255.0 } else { 0.0 };
                let code: Box<[u8]> = v
                    .iter()
                    .map(|&x| {
                        if step > 0.0 {
                            ((x - lo) / step).round().clamp(0.0, 255.0) as u8
                        } else {
                            0
                        }
                    })
                    .collect();
                put_box(bytes, slot, code);
                put_slot(
                    scale,
                    slot,
                    &[[if lo.is_finite() { lo } else { 0.0 }, step]],
                );
            }
        }
    }

    /// Slot `slot`'s full vector, when the codes are full vectors.
    fn f32(&self, slot: u32) -> Option<&[f32]> {
        match self {
            Codes::F32(f) => Some(&f[slot as usize]),
            Codes::Pq(p) => p.raw(slot),
            Codes::Sq8 { .. } | Codes::Bin { .. } => None,
        }
    }

    /// RAM the codes hold beyond what each slot is charged: a PQ set's
    /// codebook, or its full vectors before it has one. O(1): the meter asks
    /// on every write.
    fn held_bytes(&self) -> usize {
        match self {
            Codes::Pq(p) => p.held_bytes(),
            _ => 0,
        }
    }

    /// Whether this is a PQ set still without its codebook.
    fn untrained(&self) -> bool {
        matches!(self, Codes::Pq(p) if p.book.is_none())
    }

    /// See [`Pq::step`].
    fn tend(&mut self, ready: bool, wait: bool) {
        if let Codes::Pq(p) = self {
            p.step(ready, wait);
        }
    }

    /// `v`, of norm `norm`, prepared for distances to these codes.
    fn query<'a>(&self, v: &'a [f32], norm: f32) -> Query<'a> {
        let prep = match self {
            Codes::Bin { .. } => Prep::Bin(BinQuery::new(v)),
            Codes::Pq(p) => p.query(v, norm),
            _ => Prep::Plain,
        };
        Query { v, norm, prep }
    }

    /// The distance between slots `a` and `b` (norms `an`, `bn`).
    fn between(&self, metric: Metric, a: u32, an: f32, b: u32, bn: f32) -> f32 {
        match self {
            Codes::F32(f) => dist(metric, &f[a as usize], an, &f[b as usize], bn),
            Codes::Sq8 { bytes, scale } => {
                let (x, y) = (sq8_at(bytes, scale, a), sq8_at(bytes, scale, b));
                dist_from(
                    metric,
                    || kernel::sq8_l2sq_sq8(x, y),
                    || kernel::sq8_dot_sq8(x, y),
                    an,
                    bn,
                )
            }
            Codes::Bin { bits, dim, .. } => {
                let w = quant::bin_bytes(*dim);
                let dot = quant::bin_dot(at(bits, w, a), at(bits, w, b), *dim, an, bn);
                dist_from(metric, || an * an + bn * bn - 2.0 * dot, || dot, an, bn)
            }
            Codes::Pq(p) => match &p.book {
                Some(book) => {
                    let w = quant::pq_bytes(p.dim);
                    pq_dist(
                        metric,
                        book.l2sq(at(&p.codes, w, a), at(&p.codes, w, b)),
                        an,
                        bn,
                    )
                }
                None => dist(metric, &p.raw[a as usize], an, &p.raw[b as usize], bn),
            },
        }
    }

    /// The distance between slot `a` (norm `an`) and a prepared query.
    fn to(&self, metric: Metric, a: u32, an: f32, q: &Query<'_>) -> f32 {
        let qn = q.norm;
        match (self, &q.prep) {
            (Codes::F32(f), _) => dist(metric, &f[a as usize], an, q.v, qn),
            (Codes::Pq(p), Prep::Plain) if p.book.is_none() => {
                dist(metric, &p.raw[a as usize], an, q.v, qn)
            }
            (Codes::Sq8 { bytes, scale }, _) => {
                let c = sq8_at(bytes, scale, a);
                dist_from(
                    metric,
                    || kernel::sq8_l2sq(c, q.v),
                    || kernel::sq8_dot(c, q.v),
                    an,
                    qn,
                )
            }
            (Codes::Bin { bits, factor, dim }, Prep::Bin(bq)) => {
                let code = at(bits, quant::bin_bytes(*dim), a);
                let dot = bq.dot(code, an, factor[a as usize]);
                dist_from(metric, || an * an + qn * qn - 2.0 * dot, || dot, an, qn)
            }
            (Codes::Pq(p), Prep::Pq(lut)) if p.book.is_some() => {
                let l2 = kernel::lut_sum(lut, at(&p.codes, quant::pq_bytes(p.dim), a));
                pq_dist(metric, l2, an, qn)
            }
            // A query prepared before the codes changed (a PQ set installs its
            // codebook only between walks, so none is): prepare it again.
            _ => self.to(metric, a, an, &self.query(q.v, qn)),
        }
    }
}

/// What a PQ code is taken of: the vector, or for a cosine set the unit
/// vector, so the code's squared distances are of directions alone.
fn pq_input(v: &[f32], norm: f32, unit: bool) -> Cow<'_, [f32]> {
    if unit && norm > 0.0 {
        Cow::Owned(v.iter().map(|x| x / norm).collect())
    } else {
        Cow::Borrowed(v)
    }
}

/// The distance from PQ's estimate `l2` of the squared distance between two
/// vectors of norms `an` and `bn` (unit vectors' for cosine): the inner
/// product is `(‖a‖² + ‖b‖² − ‖a − b‖²) / 2`, and two unit vectors' cosine
/// `1 − ‖a − b‖² / 2`.
fn pq_dist(metric: Metric, l2: f32, an: f32, bn: f32) -> f32 {
    dist_from(
        metric,
        || l2,
        || match metric {
            Metric::Cosine => an * bn * (1.0 - l2 / 2.0),
            _ => (an * an + bn * bn - l2) / 2.0,
        },
        an,
        bn,
    )
}

/// Slot `slot`'s entry in an array `width` wide a slot.
fn at(v: &[u8], width: usize, slot: u32) -> &[u8] {
    &v[slot as usize * width..(slot as usize + 1) * width]
}

fn sq8_at<'a>(bytes: &'a [Box<[u8]>], scale: &[[f32; 2]], slot: u32) -> Sq8<'a> {
    let [lo, step] = scale[slot as usize];
    (&bytes[slot as usize], lo, step)
}

pub struct Hnsw {
    metric: Metric,
    quant: Quant,
    /// Values a vector: the set's, taken from its first insert.
    dim: usize,
    // One entry a slot, live or deleted.
    ids: Vec<Box<[u8]>>,
    meta: Vec<Option<Box<[u8]>>>,
    /// The ORIGINAL vector's norm, whatever the code: cosine divides by it.
    norms: Vec<f32>,
    deleted: Vec<bool>,
    /// The highest layer a slot is on.
    levels: Vec<u8>,
    codes: Codes,
    /// Layer 0, [`STRIDE0`] a slot: the link count, then the links.
    links0: Vec<u32>,
    /// Layers 1 and up, for the slots on them (about one in `M`): slot `n`'s
    /// links on layer `l` are `upper[&n][l - 1]`.
    upper: HashMap<u32, Vec<Vec<u32>>>,
    /// The full vectors when the graph holds codes, by slot: the re-rank
    /// reads them, and `get` returns them so VEC.GET stays lossless. Unused
    /// for `Quant::None`, whose codes ARE the full vectors.
    full: Full,
    id_to_idx: HashMap<Box<[u8]>, u32>,
    /// Deleted slots the next insert takes before growing the arrays
    /// (BUG-0197). Without it a tombstone stayed until a restart's rebuild, so
    /// a set whose ids churn (`VEC.DEL`, TTL expiry, an upsert's old node) grew
    /// without bound while the D4 meter credited every delete as freed. Never
    /// holds the entry point: that slot is freed when the entry moves.
    free: Vec<u32>,
    entry: Option<u32>,
    max_level: usize,
    live: usize,
    rng: u64,
    m_l: f64,
    /// Live vectors at which a PQ set trains ([`PQ_TRAIN_DEFAULT`]).
    pq_train: usize,
}

/// A stored vector and its meta, as [`Hnsw::get`] finds them.
pub type Found<'a> = (Cow<'a, [f32]>, Option<&'a [u8]>);

/// Where a quantized graph keeps its full vectors (ADR-0049).
enum Full {
    /// In RAM, one box a slot: a co-processor with no `--vec-dir`.
    Ram(Vec<Box<[f32]>>),
    /// In a local file (D2). `spill` holds, in RAM, the vector of a slot whose
    /// write failed, so the index never lacks a vector it acknowledged; the D4
    /// meter is charged for it ([`Hnsw::spill_bytes`]).
    File {
        file: VecFile,
        spill: HashMap<u32, Vec<f32>>,
    },
}

/// A (distance, node) pair ordered by distance (smaller first via `Reverse`),
/// node index as a stable tiebreak so heaps and sorts are deterministic.
#[derive(Clone, Copy, PartialEq)]
struct DI {
    dist: f32,
    idx: u32,
}
impl Eq for DI {}
impl PartialOrd for DI {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for DI {
    fn cmp(&self, o: &Self) -> Ordering {
        self.dist.total_cmp(&o.dist).then(self.idx.cmp(&o.idx))
    }
}

fn l2norm(v: &[f32]) -> f32 {
    kernel::dot(v, v).sqrt()
}

/// Distance where SMALLER is nearer, from whichever of a squared L2 and a dot
/// product the metric needs; each is computed only when asked for.
fn dist_from(
    metric: Metric,
    l2sq: impl FnOnce() -> f32,
    dot: impl FnOnce() -> f32,
    an: f32,
    bn: f32,
) -> f32 {
    match metric {
        Metric::L2 => l2sq(),
        Metric::Ip => -dot(),
        Metric::Cosine => {
            let den = an * bn;
            if den == 0.0 { 1.0 } else { 1.0 - dot() / den }
        }
    }
}

/// [`dist_from`] over two full vectors.
fn dist(metric: Metric, a: &[f32], an: f32, b: &[f32], bn: f32) -> f32 {
    dist_from(metric, || kernel::l2sq(a, b), || kernel::dot(a, b), an, bn)
}

/// Convert an internal distance back to flat's score (HIGHER is nearer), so a
/// mixed fleet of flat and HNSW sets answers `VEC.SEARCH` identically.
fn dist_to_score(metric: Metric, d: f32) -> f32 {
    match metric {
        Metric::L2 => -d,          // flat returns -||q-v||^2
        Metric::Ip => -d,          // d = -ip
        Metric::Cosine => 1.0 - d, // d = 1 - cos
    }
}

impl Hnsw {
    pub fn new(metric: Metric, quant: Quant) -> Self {
        Hnsw {
            metric,
            quant,
            dim: 0,
            ids: Vec::new(),
            meta: Vec::new(),
            norms: Vec::new(),
            deleted: Vec::new(),
            levels: Vec::new(),
            codes: Codes::new(quant, metric),
            links0: Vec::new(),
            upper: HashMap::new(),
            full: Full::Ram(Vec::new()),
            id_to_idx: HashMap::new(),
            free: Vec::new(),
            entry: None,
            max_level: 0,
            live: 0,
            rng: 0x9e3779b97f4a7c15,
            m_l: 1.0 / (M as f64).ln(),
            pq_train: PQ_TRAIN_DEFAULT,
        }
    }

    /// Train a PQ set's codebook at `n` live vectors instead of
    /// [`PQ_TRAIN_DEFAULT`]; for tests and the bench.
    pub fn set_pq_train(&mut self, n: usize) {
        self.pq_train = n;
    }

    /// RAM held for the set as a whole rather than a slot: a PQ codebook, or a
    /// PQ set's full vectors before it trains. The D4 meter charges it.
    pub fn held_bytes(&self) -> usize {
        self.codes.held_bytes()
    }

    /// Whether this is a PQ set that has not trained its codebook yet.
    pub fn untrained(&self) -> bool {
        self.codes.untrained()
    }

    pub fn len(&self) -> usize {
        self.live
    }
    pub fn quant(&self) -> Quant {
        self.quant
    }
    /// Slots held, live or deleted: what the set costs, as `len` is what it
    /// serves.
    #[cfg(test)]
    fn slots(&self) -> usize {
        self.levels.len()
    }
    pub fn contains(&self, id: &[u8]) -> bool {
        self.id_to_idx.contains_key(id)
    }
    /// The stored vector and meta of `id`. Errs only when the vector file
    /// cannot be read.
    pub fn get(&self, id: &[u8]) -> Result<Option<Found<'_>>, String> {
        let Some(&i) = self.id_to_idx.get(id) else {
            return Ok(None);
        };
        Ok(Some((self.full_vec(i)?, self.meta[i as usize].as_deref())))
    }

    /// The meta of `id`, without reading its vector: what the D4 meter needs.
    pub fn meta(&self, id: &[u8]) -> Option<Option<&[u8]>> {
        let &i = self.id_to_idx.get(id)?;
        Some(self.meta[i as usize].as_deref())
    }

    /// Keep this set's full vectors in `file` rather than RAM (ADR-0049 D2).
    /// Only for a quantized set with nothing in it yet.
    pub fn attach_vec_file(&mut self, file: VecFile) {
        debug_assert!(self.quant != Quant::None && self.levels.is_empty());
        self.full = Full::File {
            file,
            spill: HashMap::new(),
        };
    }

    /// Whether the full vectors are in a file rather than RAM.
    pub fn vectors_on_disk(&self) -> bool {
        matches!(self.full, Full::File { .. })
    }

    /// The vector file's size: slots written, live or not.
    pub fn disk_bytes(&self) -> u64 {
        match &self.full {
            Full::File { file, .. } => file.len_bytes(),
            Full::Ram(_) => 0,
        }
    }

    /// RAM held for vectors whose file write failed.
    pub fn spill_bytes(&self) -> usize {
        match &self.full {
            Full::File { spill, .. } => spill.values().map(|v| v.len() * 4).sum(),
            Full::Ram(_) => 0,
        }
    }

    /// The full-precision vector for node `i`: its code when unquantized, the
    /// side store otherwise.
    fn full_vec(&self, i: u32) -> Result<Cow<'_, [f32]>, String> {
        if let Some(v) = self.codes.f32(i) {
            return Ok(Cow::Borrowed(v));
        }
        match &self.full {
            Full::Ram(f) => Ok(Cow::Borrowed(&f[i as usize])),
            Full::File { file, spill } => match spill.get(&i) {
                Some(v) => Ok(Cow::Borrowed(v)),
                None => file
                    .get(i)
                    .map(Cow::Owned)
                    .map_err(|e| format!("vector file {} slot {i}: {e}", file.path().display())),
            },
        }
    }

    /// The full vectors of several nodes, in order: one batch of file reads,
    /// hinted together ([`VecFile::get_many`]).
    fn full_many(&self, idxs: &[u32]) -> Result<Vec<Cow<'_, [f32]>>, String> {
        let Full::File { file, spill } = &self.full else {
            return idxs.iter().map(|&i| self.full_vec(i)).collect();
        };
        let on_file: Vec<u32> = idxs
            .iter()
            .copied()
            .filter(|i| !spill.contains_key(i))
            .collect();
        let mut read = file
            .get_many(&on_file)
            .map_err(|e| format!("vector file {}: {e}", file.path().display()))?
            .into_iter();
        idxs.iter()
            .map(|i| match spill.get(i) {
                Some(v) => Ok(Cow::Borrowed(v.as_slice())),
                None => read
                    .next()
                    .map(Cow::Owned)
                    .ok_or_else(|| "vector file read came back short".to_string()),
            })
            .collect()
    }

    /// Store slot `slot`'s full vector: a new slot is the next one.
    fn put_full(&mut self, slot: u32, v: Vec<f32>) {
        match &mut self.full {
            Full::Ram(f) => put_box(f, slot, v.into_boxed_slice()),
            Full::File { file, spill } => {
                if file.put(slot, &v).is_ok() {
                    spill.remove(&slot);
                } else {
                    spill.insert(slot, v);
                }
            }
        }
    }

    fn next_f64(&mut self) -> f64 {
        let mut x = self.rng;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng = x;
        ((x >> 11) as f64) / ((1u64 << 53) as f64)
    }
    fn gen_level(&mut self) -> usize {
        let u = self.next_f64().max(1e-12);
        (-(u.ln()) * self.m_l).floor() as usize
    }

    /// The highest layer slot `n` is on.
    fn level(&self, n: u32) -> usize {
        self.levels[n as usize] as usize
    }

    /// Slot `n`'s links on `layer`; none when it is not on that layer.
    fn links(&self, n: u32, layer: usize) -> &[u32] {
        if layer == 0 {
            let at = n as usize * STRIDE0;
            let count = self.links0[at] as usize;
            &self.links0[at + 1..at + 1 + count]
        } else {
            self.upper
                .get(&n)
                .and_then(|u| u.get(layer - 1))
                .map_or(&[], |l| l.as_slice())
        }
    }

    /// Replace slot `n`'s links on `layer`, which it is on. At most `M0` on
    /// layer 0, which is the room a slot has there.
    fn set_links(&mut self, n: u32, layer: usize, l: &[u32]) {
        if layer == 0 {
            debug_assert!(l.len() <= M0);
            let at = n as usize * STRIDE0;
            self.links0[at] = l.len() as u32;
            self.links0[at + 1..at + 1 + l.len()].copy_from_slice(l);
        } else if let Some(mine) = self.upper.get_mut(&n).and_then(|u| u.get_mut(layer - 1)) {
            mine.clear();
            mine.extend_from_slice(l);
        }
    }

    /// Write a node into slot `slot`, a free one or the next: every array, no
    /// links yet.
    fn write_slot(
        &mut self,
        slot: u32,
        id: Box<[u8]>,
        meta: Option<Box<[u8]>>,
        v: &[f32],
        norm: f32,
        level: usize,
    ) {
        // `gen_level` floors its draw at 1e-12, which bounds a level near 9.
        let level8 = level.min(u8::MAX as usize) as u8;
        let s = slot as usize;
        if s == self.levels.len() {
            grow(&mut self.ids, 1);
            self.ids.push(id);
            grow(&mut self.meta, 1);
            self.meta.push(meta);
            grow(&mut self.norms, 1);
            self.norms.push(norm);
            grow(&mut self.deleted, 1);
            self.deleted.push(false);
            grow(&mut self.levels, 1);
            self.levels.push(level8);
            put_slot(&mut self.links0, slot, &[0; STRIDE0]);
        } else {
            self.ids[s] = id;
            self.meta[s] = meta;
            self.norms[s] = norm;
            self.deleted[s] = false;
            self.levels[s] = level8;
            self.links0[s * STRIDE0] = 0;
        }
        if level > 0 {
            self.upper.insert(slot, vec![Vec::new(); level]);
        }
        self.codes.put(slot, v, norm);
    }

    fn dnn(&self, a: u32, b: u32) -> f32 {
        let (an, bn) = (self.norms[a as usize], self.norms[b as usize]);
        self.codes.between(self.metric, a, an, b, bn)
    }
    fn dnq(&self, a: u32, q: &Query<'_>) -> f32 {
        self.codes.to(self.metric, a, self.norms[a as usize], q)
    }

    /// Insert (or upsert) a vector. An upsert deletes the old node and inserts
    /// a fresh one, since HNSW has no cheap in-place move. The insert takes a
    /// deleted slot when there is one (BUG-0197), so a set's slots are bounded
    /// by the most it ever held at once, not by how many writes it has seen.
    ///
    /// A PQ set's codebook goes in here, after the insert ([`Hnsw::tend`]), so
    /// no walk sees the codes change under it.
    pub fn set(&mut self, id: Vec<u8>, vec: Vec<f32>, meta: Option<Vec<u8>>) {
        self.insert(id, vec, meta);
        self.tend();
    }

    /// Move a PQ set towards its codebook ([`Pq`]): start training once it
    /// holds enough vectors, and install what a thread has finished. Every
    /// write calls it, and so does the binary's sweeper, so a set that stops
    /// being written still gets its codes.
    pub fn tend(&mut self) {
        self.codes.tend(self.live >= self.pq_train, false);
    }

    /// As [`Hnsw::tend`], waiting for the thread until the codebook is in:
    /// for tests and the bench.
    pub fn settle(&mut self) {
        self.codes.tend(self.live >= self.pq_train, true);
    }

    fn insert(&mut self, id: Vec<u8>, vec: Vec<f32>, meta: Option<Vec<u8>>) {
        if self.dim == 0 {
            self.dim = vec.len();
        }
        debug_assert_eq!(vec.len(), self.dim, "VEC.SET checks dim before here");
        let norm = l2norm(&vec);
        self.del(&id);
        let level = self.gen_level();
        let idx = match self.free.pop() {
            Some(i) => {
                self.unlink(i);
                i
            }
            None => self.levels.len() as u32,
        };
        let id: Box<[u8]> = id.into_boxed_slice();
        self.write_slot(
            idx,
            id.clone(),
            meta.map(Vec::into_boxed_slice),
            &vec,
            norm,
            level,
        );
        self.id_to_idx.insert(id, idx);
        self.live += 1;
        let q = if self.quant != Quant::None {
            let q = vec.clone();
            self.put_full(idx, vec);
            q
        } else {
            vec
        };

        let Some(mut ep) = self.entry else {
            self.entry = Some(idx);
            self.max_level = level;
            return;
        };
        let q = self.codes.query(&q, norm);

        // Descend the layers ABOVE the new node's top with a greedy ef=1 walk.
        let mut lc = self.max_level;
        while lc > level {
            ep = self.greedy(&q, ep, lc, Walk::Insert(idx));
            lc -= 1;
        }
        // Then connect on each layer from the node's top down to 0, deleted
        // nodes included (see `Walk::Insert`).
        let mut ep_set = vec![ep];
        let top = level.min(self.max_level);
        for lc in (0..=top).rev() {
            let w = self.search_layer(&q, &ep_set, EF_CONSTRUCTION, lc, Walk::Insert(idx));
            let mmax = if lc == 0 { M0 } else { M };
            // The walk ranked the candidates by distance to the NEW node, from
            // its full vector: with codes, a better estimate than one between
            // its code and theirs.
            let selected = self.select_heuristic(&w, mmax);
            self.set_links(idx, lc, &selected);
            for &n in &selected {
                let mut theirs = self.links(n, lc).to_vec();
                theirs.push(idx);
                if theirs.len() > mmax {
                    let ncand: Vec<DI> = theirs
                        .iter()
                        .map(|&x| DI {
                            dist: self.dnn(n, x),
                            idx: x,
                        })
                        .collect();
                    theirs = self.select_heuristic(&ncand, mmax);
                }
                self.set_links(n, lc, &theirs);
            }
            ep_set = w.iter().map(|di| di.idx).collect();
        }
        if level > self.max_level {
            // The old entry, if deleted, was held back from `free`; it is an
            // ordinary deleted slot from here.
            if let Some(old) = self.entry
                && self.deleted[old as usize]
            {
                self.free.push(old);
            }
            self.entry = Some(idx);
            self.max_level = level;
        }
    }

    pub fn del(&mut self, id: &[u8]) -> bool {
        let Some(i) = self.id_to_idx.remove(id) else {
            return false;
        };
        self.deleted[i as usize] = true;
        self.live -= 1;
        // A deleted node routes by its code; its full vector is dead.
        if let Full::File { spill, .. } = &mut self.full {
            spill.remove(&i);
        }
        // A deleted node keeps routing until its slot is taken. The entry
        // point is never taken: every search starts there.
        if self.entry != Some(i) {
            self.free.push(i);
        }
        true
    }

    /// Detach deleted slot `i` from the graph before it is reused. A node that
    /// links to `i` would otherwise point at whatever vector moves in, and a
    /// node reachable only through `i` would be lost. So each of `i`'s old
    /// neighbours that links back re-selects its links from its own (less `i`)
    /// and the nearest [`REPAIR_POOL`] live nodes within two hops of `i`: the
    /// repair hnswlib makes on a replace, narrowed. A link to `i` from outside
    /// its neighbourhood is left; a walk takes it as an ordinary link, or as
    /// stale when the new node does not reach that layer (`steps_onto`).
    fn unlink(&mut self, i: u32) {
        let old: Vec<Vec<u32>> = (0..=self.level(i))
            .map(|layer| self.links(i, layer).to_vec())
            .collect();
        self.links0[i as usize * STRIDE0] = 0;
        self.upper.remove(&i);
        for (layer, around) in old.iter().enumerate() {
            let mmax = if layer == 0 { M0 } else { M };
            let mut seen = SlotSet::default();
            seen.insert(i);
            let mut pool: Vec<u32> = Vec::new();
            for &x in around {
                if seen.insert(x) {
                    pool.push(x);
                }
                pool.extend(
                    self.links(x, layer)
                        .iter()
                        .copied()
                        .filter(|&y| seen.insert(y)),
                );
            }
            // A deleted node in the pool is likely the next slot reused, and a
            // link to it would go stale with it.
            pool.retain(|&x| !self.deleted[x as usize] && self.level(x) >= layer);
            for &n in around {
                if n == i || self.level(n) < layer || !self.links(n, layer).contains(&i) {
                    continue;
                }
                let own: Vec<u32> = self
                    .links(n, layer)
                    .iter()
                    .copied()
                    .filter(|&x| x != i)
                    .collect();
                let mut cand: Vec<DI> = pool
                    .iter()
                    .copied()
                    .filter(|&x| x != n && !own.contains(&x))
                    .map(|x| DI {
                        dist: self.dnn(n, x),
                        idx: x,
                    })
                    .collect();
                if cand.len() > REPAIR_POOL {
                    cand.select_nth_unstable(REPAIR_POOL);
                    cand.truncate(REPAIR_POOL);
                }
                cand.extend(own.iter().map(|&x| DI {
                    dist: self.dnn(n, x),
                    idx: x,
                }));
                let relinked = self.select_heuristic(&cand, mmax);
                self.set_links(n, layer, &relinked);
            }
        }
    }

    /// Whether the walk may step onto `n` at `layer`. Besides [`Walk::hides`],
    /// a link can name a reused slot whose new node does not reach this layer:
    /// [`Hnsw::unlink`] repairs only the old node's own neighbourhood. Such a
    /// link is stale, and the node is not on this layer.
    fn steps_onto(&self, walk: Walk, n: u32, layer: usize) -> bool {
        !walk.hides(n) && self.level(n) >= layer
    }

    /// Greedy ef=1 descent at one layer: hop to the nearest neighbour until no
    /// neighbour is closer to the query.
    fn greedy(&self, q: &Query<'_>, ep: u32, layer: usize, walk: Walk) -> u32 {
        let mut cur = ep;
        let mut cur_d = self.dnq(cur, q);
        loop {
            let mut changed = false;
            for &n in self.links(cur, layer) {
                if !self.steps_onto(walk, n, layer) {
                    continue;
                }
                let d = self.dnq(n, q);
                if d < cur_d {
                    cur_d = d;
                    cur = n;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        cur
    }

    /// Beam search at one layer: return up to `ef` nearest nodes. Deleted
    /// nodes always route (their links are followed); what else they are to
    /// the walk is [`Walk`]'s to say.
    fn search_layer(
        &self,
        q: &Query<'_>,
        ep: &[u32],
        ef: usize,
        layer: usize,
        walk: Walk,
    ) -> Vec<DI> {
        let mut visited = SlotSet::default();
        let mut cands: BinaryHeap<std::cmp::Reverse<DI>> = BinaryHeap::new();
        let mut w: BinaryHeap<DI> = BinaryHeap::new(); // max-heap: worst on top
        for &e in ep {
            if !visited.insert(e) {
                continue;
            }
            let d = self.dnq(e, q);
            cands.push(std::cmp::Reverse(DI { dist: d, idx: e }));
            if !(walk == Walk::Query && self.deleted[e as usize]) {
                w.push(DI { dist: d, idx: e });
            }
        }
        while let Some(std::cmp::Reverse(c)) = cands.pop() {
            let worst = w.peek().map(|x| x.dist).unwrap_or(f32::INFINITY);
            if c.dist > worst && w.len() >= ef {
                break;
            }
            for &n in self.links(c.idx, layer) {
                if !self.steps_onto(walk, n, layer) || !visited.insert(n) {
                    continue;
                }
                let d = self.dnq(n, q);
                let worst = w.peek().map(|x| x.dist).unwrap_or(f32::INFINITY);
                if d < worst || w.len() < ef {
                    cands.push(std::cmp::Reverse(DI { dist: d, idx: n }));
                    if !(walk == Walk::Query && self.deleted[n as usize]) {
                        w.push(DI { dist: d, idx: n });
                        if w.len() > ef {
                            w.pop();
                        }
                    }
                }
            }
        }
        w.into_vec()
    }

    /// HNSW neighbour-selection heuristic (paper Algorithm 4): prefer candidates
    /// closer to the query node than to any already-chosen neighbour, which
    /// spreads links out and lifts recall. Tops up with the nearest remaining if
    /// the heuristic leaves us short, to preserve connectivity.
    fn select_heuristic(&self, cand: &[DI], m: usize) -> Vec<u32> {
        let mut sorted = cand.to_vec();
        sorted.sort();
        let mut r: Vec<u32> = Vec::with_capacity(m);
        let mut discarded: Vec<u32> = Vec::new();
        for c in &sorted {
            if r.len() >= m {
                break;
            }
            let mut keep = true;
            for &s in &r {
                if self.dnn(c.idx, s) < c.dist {
                    keep = false;
                    break;
                }
            }
            if keep {
                r.push(c.idx);
            } else {
                discarded.push(c.idx);
            }
        }
        for d in discarded {
            if r.len() >= m {
                break;
            }
            r.push(d);
        }
        r
    }

    /// k-NN query: greedy descent to layer 0, then a beam search with `ef`, then
    /// the `k` nearest as `(id, score)` with score in flat's convention.
    ///
    /// When the graph holds codes (ADR-0049), the beam ranks by code distance,
    /// so its best `rerank` (at least `k`) are re-scored against the full
    /// vectors and only then cut to `k`. That is what lets an aggressive code
    /// keep recall: the code only has to get the right answers into the top
    /// `rerank`, not into the right order.
    ///
    /// Errs only when the vector file cannot be read: a search answers from
    /// the full vectors or not at all, never from codes alone.
    pub fn knn(
        &self,
        q: &[f32],
        k: usize,
        ef: usize,
        rerank: usize,
    ) -> Result<Vec<(Vec<u8>, f32)>, String> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let Some(mut ep) = self.entry else {
            return Ok(Vec::new());
        };
        let qn = l2norm(q);
        let query = self.codes.query(q, qn);
        for lc in (1..=self.max_level).rev() {
            ep = self.greedy(&query, ep, lc, Walk::Query);
        }
        let depth = if self.quant == Quant::None {
            k
        } else {
            rerank.max(k)
        };
        let mut w = self.search_layer(&query, &[ep], ef.max(depth), 0, Walk::Query);
        w.sort();
        w.truncate(depth);
        if self.quant != Quant::None {
            let idxs: Vec<u32> = w.iter().map(|di| di.idx).collect();
            for (di, v) in w.iter_mut().zip(self.full_many(&idxs)?) {
                di.dist = dist(self.metric, &v, self.norms[di.idx as usize], q, qn);
            }
            w.sort();
        }
        w.truncate(k);
        Ok(w.into_iter()
            .map(|di| {
                (
                    self.ids[di.idx as usize].to_vec(),
                    dist_to_score(self.metric, di.dist),
                )
            })
            .collect())
    }

    /// [`Hnsw::knn`] for a set whose vectors are in RAM, which cannot fail.
    #[cfg(test)]
    fn knn_ok(&self, q: &[f32], k: usize, ef: usize, rerank: usize) -> Vec<(Vec<u8>, f32)> {
        self.knn(q, k, ef, rerank)
            .expect("an in-RAM set reads no file")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Rng(u64);
    impl Rng {
        fn f32(&mut self) -> f32 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            (x >> 40) as f32 / (1u64 << 24) as f32 * 2.0 - 1.0
        }
        fn vec(&mut self, d: usize) -> Vec<f32> {
            (0..d).map(|_| self.f32()).collect()
        }
    }

    /// The proof that matters: HNSW's top-k agrees with the brute-force oracle
    /// on most queries (recall), across all three metrics.
    fn recall_for(metric: Metric) -> f64 {
        let (n, dim, k, ef) = (2000usize, 32usize, 10usize, 64usize);
        let seed = match metric {
            Metric::L2 => 0xF00D1,
            Metric::Cosine => 0xF00D2,
            Metric::Ip => 0xF00D3,
        };
        let mut rng = Rng(seed);
        let mut h = Hnsw::new(metric, Quant::None);
        let mut data: Vec<Vec<f32>> = Vec::new();
        for i in 0..n {
            let v = rng.vec(dim);
            h.set(format!("{i}").into_bytes(), v.clone(), None);
            data.push(v);
        }
        let (mut hit, mut tot) = (0usize, 0usize);
        for _ in 0..100 {
            let q = rng.vec(dim);
            let qn = l2norm(&q);
            // brute-force top-k by the same distance
            let mut bf: Vec<(f32, usize)> = data
                .iter()
                .enumerate()
                .map(|(i, v)| (dist(metric, &q, qn, v, l2norm(v)), i))
                .collect();
            bf.sort_by(|a, b| a.0.total_cmp(&b.0));
            bf.truncate(k);
            let truth: HashSet<Vec<u8>> = bf
                .iter()
                .map(|(_, i)| format!("{i}").into_bytes())
                .collect();
            for (id, _) in h.knn_ok(&q, k, ef, k) {
                if truth.contains(&id) {
                    hit += 1;
                }
            }
            tot += k;
        }
        hit as f64 / tot as f64
    }

    #[test]
    fn hnsw_recall_matches_oracle_l2() {
        let r = recall_for(Metric::L2);
        assert!(r >= 0.90, "L2 recall {r} < 0.90");
    }
    #[test]
    fn hnsw_recall_matches_oracle_cosine() {
        let r = recall_for(Metric::Cosine);
        assert!(r >= 0.90, "cosine recall {r} < 0.90");
    }
    #[test]
    fn hnsw_recall_matches_oracle_ip() {
        // IP is not a true metric; recall is a touch lower but still strong.
        let r = recall_for(Metric::Ip);
        assert!(r >= 0.80, "ip recall {r} < 0.80");
    }

    /// Clustered data, the way real embeddings sit (the bench's lesson: uniform
    /// random data is the ANN worst case and says little about either index).
    fn clustered(rng: &mut Rng, n: usize, dim: usize, centroids: usize) -> Vec<Vec<f32>> {
        let cs: Vec<Vec<f32>> = (0..centroids).map(|_| rng.vec(dim)).collect();
        (0..n)
            .map(|i| {
                let c = &cs[i % centroids];
                c.iter().map(|x| x + 0.1 * rng.f32()).collect()
            })
            .collect()
    }

    fn recall_on(metric: Metric, quant: Quant, rerank: usize) -> f64 {
        recall_at(metric, quant, 32, rerank)
    }

    /// As [`recall_on`], at `dim`. A PQ set trains a quarter of the way in,
    /// so most of its graph is built on codes.
    fn recall_at(metric: Metric, quant: Quant, dim: usize, rerank: usize) -> f64 {
        let (n, k, ef) = (2000usize, 10usize, 64usize);
        let mut rng = Rng(0xADC0_0049);
        let data = clustered(&mut rng, n, dim, 40);
        let mut h = Hnsw::new(metric, quant);
        h.set_pq_train(n / 4);
        for (i, v) in data.iter().enumerate() {
            h.set(format!("{i}").into_bytes(), v.clone(), None);
        }
        h.settle();
        assert!(!h.untrained(), "a PQ set trains at its threshold");
        let (mut hit, mut tot) = (0usize, 0usize);
        for qi in 0..100 {
            let q: Vec<f32> = data[(qi * 29) % n]
                .iter()
                .map(|x| x + 0.05 * rng.f32())
                .collect();
            let qn = l2norm(&q);
            let mut bf: Vec<(f32, usize)> = data
                .iter()
                .enumerate()
                .map(|(i, v)| (dist(metric, &q, qn, v, l2norm(v)), i))
                .collect();
            bf.sort_by(|a, b| a.0.total_cmp(&b.0));
            let truth: HashSet<Vec<u8>> = bf[..k]
                .iter()
                .map(|(_, i)| format!("{i}").into_bytes())
                .collect();
            hit += h
                .knn_ok(&q, k, ef, rerank)
                .iter()
                .filter(|(id, _)| truth.contains(id))
                .count();
            tot += k;
        }
        hit as f64 / tot as f64
    }

    /// BUG-0197: an insert searched each layer for LIVE neighbours only, so
    /// under churn a new node could get no links, and one that reached a new
    /// top level became an entry point nothing led away from. Unfixed, every
    /// one of these searches returned nothing.
    #[test]
    fn a_search_finds_live_vectors_among_deleted_ones() {
        let mut rng = Rng(0x0197_0001);
        let mut h = Hnsw::new(Metric::L2, Quant::None);
        let live: Vec<Vec<f32>> = (0..100).map(|_| rng.vec(16)).collect();
        for (i, v) in live.iter().enumerate() {
            h.set(format!("live{i}").into_bytes(), v.clone(), None);
        }
        for round in 0..10 {
            let ids: Vec<Vec<u8>> = (0..100)
                .map(|i| format!("r{round}-{i}").into_bytes())
                .collect();
            for id in &ids {
                h.set(id.clone(), rng.vec(16), None);
            }
            for id in &ids {
                assert!(h.del(id));
            }
        }
        assert_eq!(h.len(), 100);
        let found = live
            .iter()
            .enumerate()
            .filter(|(i, v)| {
                h.knn_ok(v, 1, EF_SEARCH_DEFAULT, 1)
                    .first()
                    .map(|(id, _)| id.clone())
                    == Some(format!("live{i}").into_bytes())
            })
            .count();
        assert_eq!(found, 100, "live vectors that find themselves after churn");
    }

    /// BUG-0197: a delete's slot is taken by the next insert, so the slots a
    /// set holds are bounded by the most it ever held live, plus the entry
    /// point (never reused, since every search starts there). Unfixed, every
    /// write added a slot and nothing removed one until a restart.
    #[test]
    fn deleted_slots_are_reused_so_churn_does_not_grow_the_set() {
        let mut rng = Rng(0x0197_0002);
        let mut h = Hnsw::new(Metric::L2, Quant::Sq8);
        for _ in 0..1000 {
            h.set(b"a".to_vec(), rng.vec(16), None);
        }
        assert_eq!(h.len(), 1);
        assert!(
            h.slots() <= 2,
            "1,000 upserts of one id hold {} slots",
            h.slots()
        );

        let mut h = Hnsw::new(Metric::L2, Quant::None);
        for i in 0..100 {
            h.set(format!("live{i}").into_bytes(), rng.vec(16), None);
        }
        for round in 0..20 {
            let ids: Vec<Vec<u8>> = (0..100)
                .map(|i| format!("r{round}-{i}").into_bytes())
                .collect();
            for id in &ids {
                h.set(id.clone(), rng.vec(16), None);
            }
            for id in &ids {
                h.del(id);
            }
        }
        assert_eq!(h.len(), 100);
        assert!(
            h.slots() <= 201,
            "peak 200 live, {} slots after 20 rounds",
            h.slots()
        );
    }

    /// Live nodes no walk from the entry can reach on layer 0.
    fn unreachable_live(h: &Hnsw) -> usize {
        let Some(e) = h.entry else { return 0 };
        let mut seen = HashSet::new();
        let mut stack = vec![e];
        while let Some(n) = stack.pop() {
            if !seen.insert(n) {
                continue;
            }
            stack.extend_from_slice(h.links(n, 0));
        }
        (0..h.slots() as u32)
            .filter(|&i| !h.deleted[i as usize] && !seen.contains(&i))
            .count()
    }

    /// BUG-0197: a link from outside a deleted node's neighbourhood survives
    /// the relink, so the insert that reuses the slot can walk onto it. Hidden
    /// from its own walk, the new node links to others; not hidden, it found
    /// itself at distance 0 and linked to itself.
    #[test]
    fn a_reused_slot_never_links_to_itself() {
        let mut rng = Rng(0x0197_0005);
        let mut h = Hnsw::new(Metric::L2, Quant::None);
        for i in 0..200 {
            h.set(format!("{i}").into_bytes(), rng.vec(8), None);
        }
        let entry = h.entry.expect("a set of 200 has an entry point");
        let x = (0..200u32)
            .find(|&i| i != entry)
            .expect("a node besides the entry");
        // A node that does not link to `x`, so the relink leaves its new link.
        let p = (0..200u32)
            .find(|&i| i != x && i != entry && !h.links(i, 0).contains(&x))
            .expect("a node that does not link to x");
        assert!(h.del(format!("{x}").as_bytes()));
        let mut planted = h.links(p, 0).to_vec();
        if planted.len() == M0 {
            planted.pop();
        }
        planted.push(x);
        h.set_links(p, 0, &planted);
        let near_p: Vec<f32> = h
            .codes
            .f32(p)
            .expect("an unquantized set")
            .iter()
            .map(|v| v + 1e-3)
            .collect();
        h.set(b"new".to_vec(), near_p.clone(), None);
        assert_eq!(
            h.id_to_idx[b"new".as_slice()],
            x,
            "the insert reused x's slot"
        );
        for i in 0..h.slots() as u32 {
            for layer in 0..=h.level(i) {
                assert!(
                    !h.links(i, layer).contains(&i),
                    "node {i} links to itself on layer {layer}"
                );
            }
        }
        assert_eq!(
            h.knn_ok(&near_p, 1, EF_SEARCH_DEFAULT, 1)[0].0,
            b"new".to_vec()
        );
    }

    /// BUG-0197: a reused slot's neighbourhood is relinked, so a set that has
    /// replaced its vectors three times over searches like one built fresh
    /// from the vectors it now holds, both against brute force: 0.995 against
    /// 1.000 here. Without the relink it reads 0.960. Across six seeds at 3,000
    /// vectors and ten rounds, it was never more than 0.01 below a fresh build
    /// in 11 of 12 runs (the twelfth 0.025 below); without it, 0.014-0.064.
    #[test]
    fn recall_after_churn_matches_a_fresh_build() {
        let (n, dim, k, ef) = (2500usize, 32usize, 10usize, 40usize);
        let mut rng = Rng(0x0197_0004);
        let cs: Vec<Vec<f32>> = (0..40).map(|_| rng.vec(dim)).collect();
        let point = |rng: &mut Rng, i: usize| -> Vec<f32> {
            cs[i % 40].iter().map(|x| x + 0.1 * rng.f32()).collect()
        };
        let mut h = Hnsw::new(Metric::L2, Quant::None);
        let mut live: Vec<(Vec<u8>, Vec<f32>)> = Vec::new();
        for i in 0..n {
            let v = point(&mut rng, i);
            h.set(format!("{i}").into_bytes(), v.clone(), None);
            live.push((format!("{i}").into_bytes(), v));
        }
        // Six rounds, each deleting half of what is live and inserting as many.
        let mut next = n;
        for round in 0..6 {
            let mut keep = Vec::new();
            for (j, (id, v)) in live.drain(..).enumerate() {
                if (j + round) % 2 == 0 {
                    h.del(&id);
                } else {
                    keep.push((id, v));
                }
            }
            live = keep;
            while live.len() < n {
                let v = point(&mut rng, next);
                h.set(format!("{next}").into_bytes(), v.clone(), None);
                live.push((format!("{next}").into_bytes(), v));
                next += 1;
            }
        }
        assert_eq!(h.len(), n);
        assert_eq!(unreachable_live(&h), 0, "live nodes no search can reach");
        let mut fresh = Hnsw::new(Metric::L2, Quant::None);
        for (id, v) in &live {
            fresh.set(id.clone(), v.clone(), None);
        }
        let mut q_rng = Rng(77);
        let (mut churned, mut built, mut tot) = (0usize, 0usize, 0usize);
        for qi in 0..200 {
            let q: Vec<f32> = live[(qi * 13) % n]
                .1
                .iter()
                .map(|x| x + 0.3 * q_rng.f32())
                .collect();
            let qn = l2norm(&q);
            let mut bf: Vec<(f32, &Vec<u8>)> = live
                .iter()
                .map(|(id, v)| (dist(Metric::L2, &q, qn, v, l2norm(v)), id))
                .collect();
            bf.sort_by(|a, b| a.0.total_cmp(&b.0));
            let truth: HashSet<&Vec<u8>> = bf[..k].iter().map(|(_, id)| *id).collect();
            let hits = |g: &Hnsw| {
                g.knn_ok(&q, k, ef, k)
                    .iter()
                    .filter(|(id, _)| truth.contains(id))
                    .count()
            };
            churned += hits(&h);
            built += hits(&fresh);
            tot += k;
        }
        let (churned, built) = (churned as f64 / tot as f64, built as f64 / tot as f64);
        assert!(
            churned >= built - 0.02,
            "recall@{k} after churn {churned:.3}, more than 0.02 below a fresh build's {built:.3}"
        );
    }

    /// ADR-0049: 8-bit codes plus a re-rank keep recall where the full-vector
    /// graph has it, on every metric, against the same brute-force oracle.
    #[test]
    fn sq8_with_rerank_keeps_the_full_vector_recall() {
        for metric in [Metric::L2, Metric::Cosine, Metric::Ip] {
            let full = recall_on(metric, Quant::None, 10);
            let sq8 = recall_on(metric, Quant::Sq8, 40);
            assert!(
                sq8 >= full - 0.02,
                "{metric:?}: sq8+rerank recall {sq8} fell more than 0.02 below full {full}"
            );
            assert!(sq8 >= 0.85, "{metric:?}: sq8+rerank recall {sq8} < 0.85");
        }
    }

    /// ADR-0049 item 2: the 1-bit and PQ codes, re-ranked deeper than `sq8`
    /// needs, find what the full-vector graph finds, on every metric. At 256
    /// dimensions a 1-bit code is 32 bytes and a PQ code 16.
    #[test]
    fn bin_and_pq_with_rerank_keep_the_full_vector_recall() {
        for metric in [Metric::L2, Metric::Cosine, Metric::Ip] {
            let full = recall_at(metric, Quant::None, 256, 10);
            for quant in [Quant::Bin, Quant::Pq] {
                let got = recall_at(metric, quant, 256, 100);
                eprintln!("{metric:?} {quant:?}: {got:.3} against full {full:.3}");
                assert!(
                    got >= full - 0.03,
                    "{metric:?}: {quant:?}+rerank recall {got} fell more than 0.03 below full {full}"
                );
            }
        }
    }

    /// A PQ set trains on a thread while it keeps taking writes. A slot
    /// rewritten after the thread took its vector must not keep the code of
    /// the old one, and slots added meanwhile, more than an install encodes
    /// itself, must get codes too: every slot's code is its current vector's.
    #[test]
    fn writes_during_pq_training_get_their_own_codes() {
        let mut rng = Rng(0x0049_0002);
        let dim = 40;
        let mut p = Pq::new(false);
        let mut vecs: Vec<Vec<f32>> = (0..300).map(|_| rng.vec(dim)).collect();
        for (s, v) in vecs.iter().enumerate() {
            p.put(s as u32, v, l2norm(v));
        }
        p.step(true, false);
        assert!(p.pending.is_some(), "the thread starts at the threshold");
        for s in [5usize, 7] {
            vecs[s] = rng.vec(dim);
            p.put(s as u32, &vecs[s], l2norm(&vecs[s]));
        }
        for _ in 0..CATCH_UP + 200 {
            let v = rng.vec(dim);
            p.put(vecs.len() as u32, &v, l2norm(&v));
            vecs.push(v);
        }
        p.step(true, true);
        let book = p.book.clone().expect("installed");
        assert!(p.raw.is_empty() && p.pending.is_none());
        let w = quant::pq_bytes(dim);
        assert_eq!(p.codes.len(), vecs.len() * w);
        for (s, v) in vecs.iter().enumerate() {
            let mut want = vec![0u8; w];
            book.encode(v, &mut want);
            assert_eq!(&p.codes[s * w..(s + 1) * w], &want[..], "slot {s}");
        }
    }

    /// A scratch directory for one test's vector file.
    fn scratch(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("flint-vec-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&d).expect("temp dir");
        d
    }

    /// ADR-0049: a quantized set returns the vector exactly as stored, and a
    /// search scores against the full vector, whether the full vectors are in
    /// RAM or in the local file (step 2).
    fn returns_and_scores_the_exact_vector(mut h: Hnsw) {
        let v = vec![0.123_456_7, -9.876_543, 3.25, 1e-7];
        h.set(b"a".to_vec(), v.clone(), Some(b"m".to_vec()));
        h.set(b"b".to_vec(), vec![100.0, 100.0, 100.0, 100.0], None);
        let (got, meta) = h.get(b"a").expect("readable").expect("present");
        assert_eq!(&*got, v.as_slice(), "lossless, bit for bit");
        assert_eq!(meta, Some(&b"m"[..]));
        let top = h.knn(&v, 1, 16, 4).expect("readable");
        assert_eq!(top[0].0, b"a".to_vec());
        assert_eq!(
            top[0].1, 0.0,
            "re-ranked on the full vector: exact match scores 0"
        );
        // An upsert takes the slot it frees, and its vector replaces the old.
        let w = vec![5.0, 5.0, 5.0, 5.5];
        h.set(b"a".to_vec(), w.clone(), None);
        let get = |h: &Hnsw, id: &[u8]| h.get(id).expect("readable").map(|(x, _)| x.to_vec());
        assert_eq!(get(&h, b"a"), Some(w));
        assert_eq!(get(&h, b"b"), Some(vec![100.0; 4]));
    }

    #[test]
    fn a_quantized_set_returns_and_scores_the_exact_vector() {
        returns_and_scores_the_exact_vector(Hnsw::new(Metric::L2, Quant::Sq8));
    }

    #[test]
    fn a_set_with_a_vector_file_returns_and_scores_the_exact_vector() {
        let d = scratch("exact");
        let f = VecFile::create(&d.join("0.vecs"), 4).expect("create");
        let mut h = Hnsw::new(Metric::L2, Quant::Sq8);
        h.attach_vec_file(f);
        assert!(h.vectors_on_disk());
        returns_and_scores_the_exact_vector(h);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// A vector whose file write fails is kept in RAM rather than lost, and
    /// reported, so the index never lacks a vector it acknowledged.
    #[test]
    fn a_failed_file_write_keeps_the_vector_in_ram() {
        let d = scratch("spill");
        let f = VecFile::read_only(&d.join("0.vecs"), 4).expect("create");
        let mut h = Hnsw::new(Metric::L2, Quant::Sq8);
        h.attach_vec_file(f);
        let v = vec![1.5, 2.5, 3.5, 4.5];
        h.set(b"a".to_vec(), v.clone(), None);
        assert_eq!(h.spill_bytes(), 16);
        let (got, _) = h.get(b"a").expect("readable").expect("present");
        assert_eq!(&*got, v.as_slice());
        assert_eq!(h.knn(&v, 1, 16, 4).expect("readable")[0].1, 0.0);
        // Deleted, its vector is dead and the RAM is given back.
        assert!(h.del(b"a"));
        assert_eq!(h.spill_bytes(), 0);
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn upsert_and_delete_are_reflected() {
        let mut h = Hnsw::new(Metric::L2, Quant::None);
        h.set(b"a".to_vec(), vec![0.0, 0.0], None);
        h.set(b"b".to_vec(), vec![1.0, 0.0], None);
        assert_eq!(h.len(), 2);
        // upsert a moves it far away
        h.set(b"a".to_vec(), vec![9.0, 9.0], None);
        assert_eq!(h.len(), 2, "upsert is not a new live node");
        let near_origin = h.knn_ok(&[0.0, 0.0], 1, 32, 1);
        assert_eq!(
            near_origin[0].0,
            b"b".to_vec(),
            "moved 'a' is no longer nearest origin"
        );
        // delete
        assert!(h.del(b"b"));
        assert!(!h.contains(b"b"));
        assert_eq!(h.len(), 1);
        let all = h.knn_ok(&[0.0, 0.0], 5, 32, 5);
        assert!(
            all.iter().all(|(id, _)| id != b"b"),
            "deleted 'b' never in results"
        );
    }
}
