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

use crate::vecfile::VecFile;
use crate::{Metric, Quant};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

const M: usize = 16; // neighbours per node on upper layers
const M0: usize = 32; // neighbours at layer 0 (2*M — denser base layer)
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

/// How a node holds its vector in the graph (ADR-0049 D1).
enum Code {
    /// The full float32 vector: today's index, and the re-rank source itself.
    F32(Vec<f32>),
    /// Per-vector 8-bit scalar quantization: value `i` is `lo + step * bytes[i]`.
    /// One byte a dimension instead of four, and no training: the range is the
    /// vector's own, so an online insert needs nothing learned beforehand.
    Sq8 { bytes: Vec<u8>, lo: f32, step: f32 },
}

impl Code {
    fn encode(quant: Quant, v: Vec<f32>) -> Code {
        match quant {
            Quant::None => Code::F32(v),
            Quant::Sq8 => {
                let (lo, hi) = v
                    .iter()
                    .fold((f32::INFINITY, f32::NEG_INFINITY), |(l, h), &x| {
                        (l.min(x), h.max(x))
                    });
                let step = if hi > lo { (hi - lo) / 255.0 } else { 0.0 };
                let bytes = v
                    .iter()
                    .map(|&x| {
                        if step > 0.0 {
                            ((x - lo) / step).round().clamp(0.0, 255.0) as u8
                        } else {
                            0
                        }
                    })
                    .collect();
                Code::Sq8 {
                    bytes,
                    lo: if lo.is_finite() { lo } else { 0.0 },
                    step,
                }
            }
        }
    }

    /// The vector's values, decoded on the fly: no allocation per distance.
    fn vals(&self) -> Vals<'_> {
        match self {
            Code::F32(v) => Vals::F(v.iter()),
            Code::Sq8 { bytes, lo, step } => Vals::Q(bytes.iter(), *lo, *step),
        }
    }
}

enum Vals<'a> {
    F(std::slice::Iter<'a, f32>),
    Q(std::slice::Iter<'a, u8>, f32, f32),
}

impl Iterator for Vals<'_> {
    type Item = f32;
    fn next(&mut self) -> Option<f32> {
        match self {
            Vals::F(i) => i.next().copied(),
            Vals::Q(i, lo, step) => i.next().map(|&b| *lo + *step * b as f32),
        }
    }
}

struct Node {
    id: Vec<u8>,
    code: Code,
    /// The ORIGINAL vector's norm, whatever the code: cosine divides by it.
    norm: f32,
    meta: Option<Vec<u8>>,
    deleted: bool,
    /// `links[layer]` = neighbour node indices; `len() == level + 1`.
    links: Vec<Vec<u32>>,
}

pub struct Hnsw {
    metric: Metric,
    quant: Quant,
    nodes: Vec<Node>,
    /// The full vectors when the graph holds codes, by slot: the re-rank
    /// reads them, and `get` returns them so VEC.GET stays lossless. Unused
    /// for `Quant::None`, whose codes ARE the full vectors.
    full: Full,
    id_to_idx: HashMap<Vec<u8>, u32>,
    /// Deleted slots the next insert takes before growing `nodes` (BUG-0197).
    /// Without it a tombstone stayed until a restart's rebuild, so a set whose
    /// ids churn (`VEC.DEL`, TTL expiry, an upsert's old node) grew without
    /// bound while the D4 meter credited every delete as freed. Never holds the
    /// entry point: that slot is freed when the entry moves.
    free: Vec<u32>,
    entry: Option<u32>,
    max_level: usize,
    live: usize,
    rng: u64,
    m_l: f64,
}

/// A stored vector and its meta, as [`Hnsw::get`] finds them.
pub type Found<'a> = (Cow<'a, [f32]>, Option<&'a [u8]>);

/// Where a quantized graph keeps its full vectors (ADR-0049).
enum Full {
    /// In RAM, indexed like `nodes`: a co-processor with no `--vec-dir`.
    Ram(Vec<Vec<f32>>),
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

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}
fn l2norm(v: &[f32]) -> f32 {
    dot(v, v).sqrt()
}

/// Distance where SMALLER is nearer.
fn dist(metric: Metric, a: &[f32], an: f32, b: &[f32], bn: f32) -> f32 {
    match metric {
        Metric::L2 => a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum(),
        Metric::Ip => -dot(a, b),
        Metric::Cosine => {
            let den = an * bn;
            if den == 0.0 {
                1.0
            } else {
                1.0 - dot(a, b) / den
            }
        }
    }
}

/// [`dist`] over decoded values, for when either side is a code. The all-float
/// case keeps [`dist`] itself, so an unquantized set pays nothing for this.
fn dist_vals(metric: Metric, a: Vals<'_>, an: f32, b: Vals<'_>, bn: f32) -> f32 {
    match metric {
        Metric::L2 => a.zip(b).map(|(x, y)| (x - y) * (x - y)).sum(),
        Metric::Ip => -a.zip(b).map(|(x, y)| x * y).sum::<f32>(),
        Metric::Cosine => {
            let den = an * bn;
            if den == 0.0 {
                1.0
            } else {
                1.0 - a.zip(b).map(|(x, y)| x * y).sum::<f32>() / den
            }
        }
    }
}

/// Distance between a code and a full vector, SMALLER nearer.
fn dist_code(metric: Metric, c: &Code, cn: f32, q: &[f32], qn: f32) -> f32 {
    match c {
        Code::F32(v) => dist(metric, v, cn, q, qn),
        Code::Sq8 { .. } => dist_vals(metric, c.vals(), cn, Vals::F(q.iter()), qn),
    }
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
            nodes: Vec::new(),
            full: Full::Ram(Vec::new()),
            id_to_idx: HashMap::new(),
            free: Vec::new(),
            entry: None,
            max_level: 0,
            live: 0,
            rng: 0x9e3779b97f4a7c15,
            m_l: 1.0 / (M as f64).ln(),
        }
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
        self.nodes.len()
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
        Ok(Some((
            self.full_vec(i)?,
            self.nodes[i as usize].meta.as_deref(),
        )))
    }

    /// The meta of `id`, without reading its vector: what the D4 meter needs.
    pub fn meta(&self, id: &[u8]) -> Option<Option<&[u8]>> {
        let &i = self.id_to_idx.get(id)?;
        Some(self.nodes[i as usize].meta.as_deref())
    }

    /// Keep this set's full vectors in `file` rather than RAM (ADR-0049 D2).
    /// Only for a quantized set with nothing in it yet.
    pub fn attach_vec_file(&mut self, file: VecFile) {
        debug_assert!(self.quant != Quant::None && self.nodes.is_empty());
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
        if let Code::F32(v) = &self.nodes[i as usize].code {
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
            Full::Ram(f) if (slot as usize) < f.len() => f[slot as usize] = v,
            Full::Ram(f) => f.push(v),
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

    fn dnn(&self, a: u32, b: u32) -> f32 {
        let (na, nb) = (&self.nodes[a as usize], &self.nodes[b as usize]);
        match (&na.code, &nb.code) {
            (Code::F32(x), Code::F32(y)) => dist(self.metric, x, na.norm, y, nb.norm),
            _ => dist_vals(
                self.metric,
                na.code.vals(),
                na.norm,
                nb.code.vals(),
                nb.norm,
            ),
        }
    }
    fn dnq(&self, a: u32, q: &[f32], qn: f32) -> f32 {
        let na = &self.nodes[a as usize];
        dist_code(self.metric, &na.code, na.norm, q, qn)
    }

    /// Insert (or upsert) a vector. An upsert deletes the old node and inserts
    /// a fresh one, since HNSW has no cheap in-place move. The insert takes a
    /// deleted slot when there is one (BUG-0197), so a set's slots are bounded
    /// by the most it ever held at once, not by how many writes it has seen.
    pub fn set(&mut self, id: Vec<u8>, vec: Vec<f32>, meta: Option<Vec<u8>>) {
        let norm = l2norm(&vec);
        self.del(&id);
        let level = self.gen_level();
        let q = vec.clone();
        let node = Node {
            id: id.clone(),
            code: Code::encode(self.quant, vec.clone()),
            norm,
            meta,
            deleted: false,
            links: vec![Vec::new(); level + 1],
        };
        let idx = match self.free.pop() {
            Some(i) => {
                self.unlink(i);
                self.nodes[i as usize] = node;
                i
            }
            None => {
                self.nodes.push(node);
                (self.nodes.len() - 1) as u32
            }
        };
        if self.quant != Quant::None {
            self.put_full(idx, vec);
        }
        self.id_to_idx.insert(id, idx);
        self.live += 1;

        let Some(mut ep) = self.entry else {
            self.entry = Some(idx);
            self.max_level = level;
            return;
        };

        // Descend the layers ABOVE the new node's top with a greedy ef=1 walk.
        let mut lc = self.max_level;
        while lc > level {
            ep = self.greedy(&q, norm, ep, lc, Walk::Insert(idx));
            lc -= 1;
        }
        // Then connect on each layer from the node's top down to 0, deleted
        // nodes included (see `Walk::Insert`).
        let mut ep_set = vec![ep];
        let top = level.min(self.max_level);
        for lc in (0..=top).rev() {
            let w = self.search_layer(&q, norm, &ep_set, EF_CONSTRUCTION, lc, Walk::Insert(idx));
            let mmax = if lc == 0 { M0 } else { M };
            // Candidates ranked by distance to the NEW node (idx).
            let cand: Vec<DI> = w
                .iter()
                .map(|di| DI {
                    dist: self.dnn(idx, di.idx),
                    idx: di.idx,
                })
                .collect();
            let selected = self.select_heuristic(&cand, mmax);
            self.nodes[idx as usize].links[lc] = selected.clone();
            for &n in &selected {
                self.nodes[n as usize].links[lc].push(idx);
                if self.nodes[n as usize].links[lc].len() > mmax {
                    let ncand: Vec<DI> = self.nodes[n as usize].links[lc]
                        .iter()
                        .map(|&x| DI {
                            dist: self.dnn(n, x),
                            idx: x,
                        })
                        .collect();
                    let pruned = self.select_heuristic(&ncand, mmax);
                    self.nodes[n as usize].links[lc] = pruned;
                }
            }
            ep_set = w.iter().map(|di| di.idx).collect();
        }
        if level > self.max_level {
            // The old entry, if deleted, was held back from `free`; it is an
            // ordinary deleted slot from here.
            if let Some(old) = self.entry
                && self.nodes[old as usize].deleted
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
        self.nodes[i as usize].deleted = true;
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
        let old = std::mem::take(&mut self.nodes[i as usize].links);
        for (layer, around) in old.iter().enumerate() {
            let mmax = if layer == 0 { M0 } else { M };
            let mut seen: HashSet<u32> = HashSet::from([i]);
            let mut pool: Vec<u32> = Vec::new();
            for &x in around {
                if seen.insert(x) {
                    pool.push(x);
                }
                if let Some(xl) = self.nodes[x as usize].links.get(layer) {
                    pool.extend(xl.iter().copied().filter(|&y| seen.insert(y)));
                }
            }
            // A deleted node in the pool is likely the next slot reused, and a
            // link to it would go stale with it.
            pool.retain(|&x| {
                let nd = &self.nodes[x as usize];
                !nd.deleted && nd.links.len() > layer
            });
            for &n in around {
                let Some(nl) = self.nodes[n as usize].links.get(layer) else {
                    continue;
                };
                if n == i || !nl.contains(&i) {
                    continue;
                }
                let own: Vec<u32> = nl.iter().copied().filter(|&x| x != i).collect();
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
                self.nodes[n as usize].links[layer] = self.select_heuristic(&cand, mmax);
            }
        }
    }

    /// Whether the walk may step onto `n` at `layer`. Besides [`Walk::hides`],
    /// a link can name a reused slot whose new node does not reach this layer:
    /// [`Hnsw::unlink`] repairs only the old node's own neighbourhood. Such a
    /// link is stale, and the node is not on this layer.
    fn steps_onto(&self, walk: Walk, n: u32, layer: usize) -> bool {
        !walk.hides(n) && self.nodes[n as usize].links.len() > layer
    }

    /// Greedy ef=1 descent at one layer: hop to the nearest neighbour until no
    /// neighbour is closer to the query.
    fn greedy(&self, q: &[f32], qn: f32, ep: u32, layer: usize, walk: Walk) -> u32 {
        let mut cur = ep;
        let mut cur_d = self.dnq(cur, q, qn);
        loop {
            let mut changed = false;
            if let Some(neigh) = self.nodes[cur as usize].links.get(layer) {
                for &n in neigh {
                    if !self.steps_onto(walk, n, layer) {
                        continue;
                    }
                    let d = self.dnq(n, q, qn);
                    if d < cur_d {
                        cur_d = d;
                        cur = n;
                        changed = true;
                    }
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
        q: &[f32],
        qn: f32,
        ep: &[u32],
        ef: usize,
        layer: usize,
        walk: Walk,
    ) -> Vec<DI> {
        let mut visited: HashSet<u32> = HashSet::new();
        let mut cands: BinaryHeap<std::cmp::Reverse<DI>> = BinaryHeap::new();
        let mut w: BinaryHeap<DI> = BinaryHeap::new(); // max-heap: worst on top
        for &e in ep {
            if !visited.insert(e) {
                continue;
            }
            let d = self.dnq(e, q, qn);
            cands.push(std::cmp::Reverse(DI { dist: d, idx: e }));
            if !(walk == Walk::Query && self.nodes[e as usize].deleted) {
                w.push(DI { dist: d, idx: e });
            }
        }
        while let Some(std::cmp::Reverse(c)) = cands.pop() {
            let worst = w.peek().map(|x| x.dist).unwrap_or(f32::INFINITY);
            if c.dist > worst && w.len() >= ef {
                break;
            }
            let neigh = self.nodes[c.idx as usize]
                .links
                .get(layer)
                .cloned()
                .unwrap_or_default();
            for n in neigh {
                if !self.steps_onto(walk, n, layer) || !visited.insert(n) {
                    continue;
                }
                let d = self.dnq(n, q, qn);
                let worst = w.peek().map(|x| x.dist).unwrap_or(f32::INFINITY);
                if d < worst || w.len() < ef {
                    cands.push(std::cmp::Reverse(DI { dist: d, idx: n }));
                    if !(walk == Walk::Query && self.nodes[n as usize].deleted) {
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
        for lc in (1..=self.max_level).rev() {
            ep = self.greedy(q, qn, ep, lc, Walk::Query);
        }
        let depth = if self.quant == Quant::None {
            k
        } else {
            rerank.max(k)
        };
        let mut w = self.search_layer(q, qn, &[ep], ef.max(depth), 0, Walk::Query);
        w.sort();
        w.truncate(depth);
        if self.quant != Quant::None {
            let idxs: Vec<u32> = w.iter().map(|di| di.idx).collect();
            for (di, v) in w.iter_mut().zip(self.full_many(&idxs)?) {
                di.dist = dist(self.metric, &v, self.nodes[di.idx as usize].norm, q, qn);
            }
            w.sort();
        }
        w.truncate(k);
        Ok(w.into_iter()
            .map(|di| {
                (
                    self.nodes[di.idx as usize].id.clone(),
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
        let (n, dim, k, ef) = (2000usize, 32usize, 10usize, 64usize);
        let mut rng = Rng(0xADC0_0049);
        let data = clustered(&mut rng, n, dim, 40);
        let mut h = Hnsw::new(metric, quant);
        for (i, v) in data.iter().enumerate() {
            h.set(format!("{i}").into_bytes(), v.clone(), None);
        }
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
            for &x in &h.nodes[n as usize].links[0] {
                stack.push(x);
            }
        }
        h.nodes
            .iter()
            .enumerate()
            .filter(|(i, nd)| !nd.deleted && !seen.contains(&(*i as u32)))
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
            .find(|&i| i != x && i != entry && !h.nodes[i as usize].links[0].contains(&x))
            .expect("a node that does not link to x");
        assert!(h.del(format!("{x}").as_bytes()));
        h.nodes[p as usize].links[0].push(x);
        let near_p: Vec<f32> = h.nodes[p as usize].code.vals().map(|v| v + 1e-3).collect();
        h.set(b"new".to_vec(), near_p.clone(), None);
        assert_eq!(
            h.id_to_idx[b"new".as_slice()],
            x,
            "the insert reused x's slot"
        );
        for (i, nd) in h.nodes.iter().enumerate() {
            for (layer, l) in nd.links.iter().enumerate() {
                assert!(
                    !l.contains(&(i as u32)),
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
