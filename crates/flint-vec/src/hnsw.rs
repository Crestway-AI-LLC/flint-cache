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
//! Deletion tombstones a node: it stays in the graph for ROUTING but never
//! appears in results, and a cold-start rebuild drops tombstones (only live
//! vectors are re-inserted from KV), which also compacts churn.

use crate::{Metric, Quant};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};

const M: usize = 16; // neighbours per node on upper layers
const M0: usize = 32; // neighbours at layer 0 (2*M — denser base layer)
const EF_CONSTRUCTION: usize = 100;
/// Default `ef` for a query when `VEC.SEARCH` gives no `EF` — the recall/latency
/// knob. Higher searches more of the graph: better recall, more work.
pub const EF_SEARCH_DEFAULT: usize = 64;

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
    /// The full vectors when the graph holds codes, indexed like `nodes`: the
    /// re-rank reads them, and `get` returns them so VEC.GET stays lossless.
    /// Empty for `Quant::None`, whose codes ARE the full vectors. ADR-0049 step
    /// 1 keeps these in RAM; step 2 moves them to a local file.
    full: Vec<Vec<f32>>,
    id_to_idx: HashMap<Vec<u8>, u32>,
    entry: Option<u32>,
    max_level: usize,
    live: usize,
    rng: u64,
    m_l: f64,
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
            full: Vec::new(),
            id_to_idx: HashMap::new(),
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
    pub fn contains(&self, id: &[u8]) -> bool {
        self.id_to_idx
            .get(id)
            .is_some_and(|&i| !self.nodes[i as usize].deleted)
    }
    pub fn get(&self, id: &[u8]) -> Option<(&[f32], Option<&[u8]>)> {
        let &i = self.id_to_idx.get(id)?;
        let n = &self.nodes[i as usize];
        if n.deleted {
            return None;
        }
        Some((self.full_vec(i), n.meta.as_deref()))
    }

    /// The full-precision vector for node `i`: its code when unquantized, the
    /// side store otherwise.
    fn full_vec(&self, i: u32) -> &[f32] {
        match &self.nodes[i as usize].code {
            Code::F32(v) => v,
            Code::Sq8 { .. } => &self.full[i as usize],
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

    /// Insert (or upsert) a vector. An upsert tombstones the old node and adds a
    /// fresh one — HNSW has no cheap in-place move, and a new node keeps the
    /// graph valid; the tombstone is dropped on the next rebuild.
    pub fn set(&mut self, id: Vec<u8>, vec: Vec<f32>, meta: Option<Vec<u8>>) {
        let norm = l2norm(&vec);
        if let Some(&old) = self.id_to_idx.get(&id)
            && !self.nodes[old as usize].deleted
        {
            self.nodes[old as usize].deleted = true;
            self.live -= 1;
        }
        let idx = self.nodes.len() as u32;
        let level = self.gen_level();
        let q = vec.clone();
        if self.quant != Quant::None {
            // Indexed like `nodes`: pushed in the same order, never removed.
            self.full.push(vec.clone());
        }
        self.nodes.push(Node {
            id: id.clone(),
            code: Code::encode(self.quant, vec),
            norm,
            meta,
            deleted: false,
            links: vec![Vec::new(); level + 1],
        });
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
            ep = self.greedy(&q, norm, ep, lc);
            lc -= 1;
        }
        // Then connect on each layer from the node's top down to 0.
        let mut ep_set = vec![ep];
        let top = level.min(self.max_level);
        for lc in (0..=top).rev() {
            let w = self.search_layer(&q, norm, &ep_set, EF_CONSTRUCTION, lc);
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
            self.entry = Some(idx);
            self.max_level = level;
        }
    }

    pub fn del(&mut self, id: &[u8]) -> bool {
        if let Some(&i) = self.id_to_idx.get(id)
            && !self.nodes[i as usize].deleted
        {
            self.nodes[i as usize].deleted = true;
            self.live -= 1;
            return true;
        }
        false
    }

    /// Greedy ef=1 descent at one layer: hop to the nearest neighbour until no
    /// neighbour is closer to the query.
    fn greedy(&self, q: &[f32], qn: f32, ep: u32, layer: usize) -> u32 {
        let mut cur = ep;
        let mut cur_d = self.dnq(cur, q, qn);
        loop {
            let mut changed = false;
            if let Some(neigh) = self.nodes[cur as usize].links.get(layer) {
                for &n in neigh {
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

    /// Beam search at one layer: return up to `ef` nearest LIVE nodes. Deleted
    /// nodes still route (their links are followed) but never enter the result.
    fn search_layer(&self, q: &[f32], qn: f32, ep: &[u32], ef: usize, layer: usize) -> Vec<DI> {
        let mut visited: HashSet<u32> = HashSet::new();
        let mut cands: BinaryHeap<std::cmp::Reverse<DI>> = BinaryHeap::new();
        let mut w: BinaryHeap<DI> = BinaryHeap::new(); // max-heap: worst on top
        for &e in ep {
            if !visited.insert(e) {
                continue;
            }
            let d = self.dnq(e, q, qn);
            cands.push(std::cmp::Reverse(DI { dist: d, idx: e }));
            if !self.nodes[e as usize].deleted {
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
                if !visited.insert(n) {
                    continue;
                }
                let d = self.dnq(n, q, qn);
                let worst = w.peek().map(|x| x.dist).unwrap_or(f32::INFINITY);
                if d < worst || w.len() < ef {
                    cands.push(std::cmp::Reverse(DI { dist: d, idx: n }));
                    if !self.nodes[n as usize].deleted {
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
    pub fn knn(&self, q: &[f32], k: usize, ef: usize, rerank: usize) -> Vec<(Vec<u8>, f32)> {
        if k == 0 {
            return Vec::new();
        }
        let Some(mut ep) = self.entry else {
            return Vec::new();
        };
        let qn = l2norm(q);
        for lc in (1..=self.max_level).rev() {
            ep = self.greedy(q, qn, ep, lc);
        }
        let depth = if self.quant == Quant::None {
            k
        } else {
            rerank.max(k)
        };
        let mut w = self.search_layer(q, qn, &[ep], ef.max(depth), 0);
        w.sort();
        w.truncate(depth);
        if self.quant != Quant::None {
            for di in &mut w {
                let n = &self.nodes[di.idx as usize];
                di.dist = dist(self.metric, self.full_vec(di.idx), n.norm, q, qn);
            }
            w.sort();
        }
        w.truncate(k);
        w.into_iter()
            .map(|di| {
                (
                    self.nodes[di.idx as usize].id.clone(),
                    dist_to_score(self.metric, di.dist),
                )
            })
            .collect()
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
            for (id, _) in h.knn(&q, k, ef, k) {
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
                .knn(&q, k, ef, rerank)
                .iter()
                .filter(|(id, _)| truth.contains(id))
                .count();
            tot += k;
        }
        hit as f64 / tot as f64
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

    /// The codes are lossy; VEC.GET is not. A quantized set returns the vector
    /// exactly as stored, and a search scores against the full vector too.
    #[test]
    fn a_quantized_set_returns_and_scores_the_exact_vector() {
        let mut h = Hnsw::new(Metric::L2, Quant::Sq8);
        let v = vec![0.123_456_7, -9.876_543, 3.25, 1e-7];
        h.set(b"a".to_vec(), v.clone(), Some(b"m".to_vec()));
        h.set(b"b".to_vec(), vec![100.0, 100.0, 100.0, 100.0], None);
        let (got, meta) = h.get(b"a").expect("present");
        assert_eq!(got, v.as_slice(), "lossless, bit for bit");
        assert_eq!(meta, Some(&b"m"[..]));
        let top = h.knn(&v, 1, 16, 4);
        assert_eq!(top[0].0, b"a".to_vec());
        assert_eq!(
            top[0].1, 0.0,
            "re-ranked on the full vector: exact match scores 0"
        );
        // An upsert keeps `full` aligned with the nodes it belongs to.
        let w = vec![5.0, 5.0, 5.0, 5.5];
        h.set(b"a".to_vec(), w.clone(), None);
        assert_eq!(h.get(b"a").map(|(x, _)| x.to_vec()), Some(w));
        assert_eq!(h.get(b"b").map(|(x, _)| x.to_vec()), Some(vec![100.0; 4]));
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
        let near_origin = h.knn(&[0.0, 0.0], 1, 32, 1);
        assert_eq!(
            near_origin[0].0,
            b"b".to_vec(),
            "moved 'a' is no longer nearest origin"
        );
        // delete
        assert!(h.del(b"b"));
        assert!(!h.contains(b"b"));
        assert_eq!(h.len(), 1);
        let all = h.knn(&[0.0, 0.0], 5, 32, 5);
        assert!(
            all.iter().all(|(id, _)| id != b"b"),
            "deleted 'b' never in results"
        );
    }
}
