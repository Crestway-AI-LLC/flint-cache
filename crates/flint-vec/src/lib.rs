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
//! with an HNSW graph behind the SAME plan; v1 makes it disk-resident
//! (DiskANN). None of those changes the `VEC.*` surface or the durable format.

mod hnsw;

use flint_resp::Value;
use hnsw::Hnsw;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

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
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

/// A stored vector: the original vector (VEC.GET is lossless), its cached L2
/// norm (cosine only), and optional opaque metadata bytes.
#[derive(Clone)]
struct Entry {
    vec: Vec<f32>,
    norm: f32,
    meta: Option<Vec<u8>>,
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

/// The engine behind a set. Both hold the vectors (a rebuild restores either);
/// flat scans them exactly, HNSW keeps a navigable graph over them.
enum Index {
    Flat(HashMap<Vec<u8>, Entry>),
    Hnsw(Hnsw),
}

/// One vector set: fixed dim + metric + index engine. `search` dispatches to the
/// engine; the `VEC.*` surface never sees which — the whole point of v0.2 being
/// a drop-in behind the same commands.
pub struct VectorSet {
    dim: usize,
    metric: Metric,
    index: Index,
}

impl VectorSet {
    pub fn new(dim: usize, metric: Metric, kind: IndexKind) -> Self {
        let index = match kind {
            IndexKind::Flat => Index::Flat(HashMap::new()),
            IndexKind::Hnsw => Index::Hnsw(Hnsw::new(metric)),
        };
        VectorSet { dim, metric, index }
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

    fn set(&mut self, id: Vec<u8>, vec: Vec<f32>, meta: Option<Vec<u8>>) {
        match &mut self.index {
            Index::Flat(m) => {
                let norm = dot(&vec, &vec).sqrt();
                m.insert(id, Entry { vec, norm, meta });
            }
            Index::Hnsw(h) => h.set(id, vec, meta),
        }
    }

    fn del(&mut self, id: &[u8]) -> bool {
        match &mut self.index {
            Index::Flat(m) => m.remove(id).is_some(),
            Index::Hnsw(h) => h.del(id),
        }
    }

    /// The stored vector + meta for VEC.GET (lossless), or None.
    fn get(&self, id: &[u8]) -> Option<(Vec<f32>, Option<Vec<u8>>)> {
        match &self.index {
            Index::Flat(m) => m.get(id).map(|e| (e.vec.clone(), e.meta.clone())),
            Index::Hnsw(h) => h.get(id).map(|(v, m)| (v.to_vec(), m.map(|x| x.to_vec()))),
        }
    }

    /// Top-k nearest, nearest first. `ef` is HNSW's recall/latency knob; flat
    /// ignores it (it is always exact).
    fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<(Vec<u8>, f32)> {
        match &self.index {
            Index::Flat(m) => flat_search(m, self.metric, query, k),
            Index::Hnsw(h) => h.knn(query, k, ef),
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
            .zip(&e.vec)
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

/// Exact flat top-k, nearest first (recall 1.0). O(n log k).
fn flat_search(
    entries: &HashMap<Vec<u8>, Entry>,
    metric: Metric,
    query: &[f32],
    k: usize,
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
    for (id, e) in entries {
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
/// reserved co-processor prefix in the tenant namespace ([`durable_key`]).
#[derive(Debug, PartialEq)]
pub enum Persist {
    Put { key: Vec<u8>, val: Vec<u8> },
    Del { key: Vec<u8> },
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
    },
    Insert {
        set: Vec<u8>,
        id: Vec<u8>,
        vec: Vec<f32>,
        meta: Option<Vec<u8>>,
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
        persist: Persist,
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

/// Serialise a vector row for durability: comma-joined floats, then the raw
/// metadata after a unit-separator when present. Chosen for legibility in v0.1;
/// a packed-f32 encoding is a later, wire-compatible swap.
fn encode_vec_row(vec: &[f32], meta: Option<&[u8]>) -> Vec<u8> {
    let mut v = floats_to_ascii(vec).into_bytes();
    if let Some(m) = meta {
        v.push(0x1f);
        v.extend_from_slice(m);
    }
    v
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
/// HNSW's neighbour lists dwarf flat's hashmap slot.
fn entry_bytes(dim: usize, id_len: usize, meta_len: usize, kind: IndexKind) -> usize {
    let structural = match kind {
        // Entry { vec, norm, meta } + a HashMap bucket.
        IndexKind::Flat => 64,
        // Node { vec, norm, meta, deleted, links } + the per-node link Vecs
        // (M0 at layer 0 plus a few at upper layers).
        IndexKind::Hnsw => 256,
    };
    dim * 4 + id_len + meta_len + structural
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
    Ok(out)
}

/// Inverse of [`durable_key`]: split a co-processor key into `(kind, set, id)`,
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

/// Inverse of [`encode_vec_row`]: `(vector, optional meta)` from a durable
/// vector value (comma-floats, then meta after a 0x1f separator).
pub fn decode_vec_row(val: &[u8]) -> Result<(Vec<f32>, Option<Vec<u8>>), String> {
    let (floats, meta) = match val.iter().position(|&b| b == 0x1f) {
        Some(i) => (&val[..i], Some(val[i + 1..].to_vec())),
        None => (val, None),
    };
    Ok((parse_vector(floats)?, meta))
}

/// Inverse of a config row: `<dim>|<metric>[|<index>]`. A 2-field config
/// (written before v0.2 added the index kind) is flat.
pub fn decode_config(val: &[u8]) -> Option<(usize, Metric, IndexKind)> {
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

fn valid_name(b: &[u8]) -> bool {
    !b.is_empty() && !b.contains(&0)
}

fn err(msg: &str) -> Value {
    Value::Error(msg.to_string())
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
}

impl Store {
    pub fn new() -> Self {
        Store::default()
    }

    /// Cap the RAM one tenant's indexes may consume (ADR-0017 D4). 0 =
    /// unlimited. A `VEC.SET` that would push a namespace past this is refused
    /// with `-VECFULL` BEFORE its durable write — so nothing is ever
    /// stored-but-unsearchable; the tenant frees space with `VEC.DEL`.
    pub fn set_index_cap(&mut self, bytes: usize) {
        self.index_cap = bytes;
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
    pub fn plan(&self, ns: &[u8], args: &[Vec<u8>]) -> Plan {
        let Some(name) = args.first() else {
            return Plan::Reply(err("ERR empty command"));
        };
        match name.to_ascii_uppercase().as_slice() {
            b"VEC.CREATE" => self.plan_create(ns, args),
            b"VEC.SET" => self.plan_set(ns, args),
            b"VEC.GET" => Plan::Reply(self.get(ns, args)),
            b"VEC.DEL" => self.plan_del(ns, args),
            b"VEC.SEARCH" => Plan::Reply(self.search(ns, args)),
            b"VEC.INFO" => Plan::Reply(self.info(ns, args)),
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
            } => {
                self.sets
                    .entry((ns.to_vec(), set))
                    .or_insert_with(|| VectorSet::new(dim, metric, index));
            }
            Apply::Insert { set, id, vec, meta } => {
                // Charge the byte delta to the namespace (new entry, or the meta
                // change on an upsert), computed BEFORE the set replaces it.
                let mut charge: Option<(usize, usize)> = None;
                if let Some(vs) = self.sets.get_mut(&(ns.to_vec(), set)) {
                    let (dim, kind) = (vs.dim(), vs.kind());
                    let old = vs.get(&id).map_or(0, |(_, m)| {
                        entry_bytes(dim, id.len(), m.map_or(0, |x| x.len()), kind)
                    });
                    let new =
                        entry_bytes(dim, id.len(), meta.as_ref().map_or(0, |m| m.len()), kind);
                    vs.set(id, vec, meta);
                    charge = Some((new, old));
                }
                if let Some((new, old)) = charge {
                    let e = self.ns_bytes.entry(ns.to_vec()).or_default();
                    *e = e.saturating_add(new).saturating_sub(old);
                }
            }
            Apply::Remove { set, id } => {
                let mut freed = 0usize;
                if let Some(vs) = self.sets.get_mut(&(ns.to_vec(), set)) {
                    let (dim, kind) = (vs.dim(), vs.kind());
                    if let Some((_, m)) = vs.get(&id) {
                        freed = entry_bytes(dim, id.len(), m.map_or(0, |x| x.len()), kind);
                    }
                    vs.del(&id);
                }
                if freed > 0
                    && let Some(e) = self.ns_bytes.get_mut(ns)
                {
                    *e = e.saturating_sub(freed);
                }
            }
        }
    }

    /// VEC.CREATE <set> DIM <d> METRIC <cosine|l2|ip> [INDEX flat]
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
        // Optional trailing `INDEX flat|hnsw`; default flat (exact, the oracle).
        let index = if args.len() >= 8 && args[6].eq_ignore_ascii_case(b"INDEX") {
            match IndexKind::parse(&args[7]) {
                Some(k) => k,
                None => return Plan::Reply(err("ERR INDEX must be flat or hnsw")),
            }
        } else {
            IndexKind::Flat
        };
        if self.sets.contains_key(&(ns.to_vec(), set.to_vec())) {
            return Plan::Reply(err("ERR set already exists"));
        }
        Plan::Write {
            persist: Persist::Put {
                key: durable_key(b'c', set, b""),
                val: format!("{dim}|{}|{}", metric.as_str(), index.as_str()).into_bytes(),
            },
            apply: Apply::CreateSet {
                set: set.to_vec(),
                dim,
                metric,
                index,
            },
            ok: Value::Simple("OK".into()),
        }
    }

    /// VEC.SET <set> <id> <vec> [META <json>]
    fn plan_set(&self, ns: &[u8], args: &[Vec<u8>]) -> Plan {
        if args.len() < 4 {
            return Plan::Reply(err("ERR VEC.SET <set> <id> <vec> [META <json>]"));
        }
        let (set, id, raw) = (&args[1], &args[2], &args[3]);
        if !valid_name(set) || !valid_name(id) {
            return Plan::Reply(err("ERR invalid set or id (must be non-empty, no NUL)"));
        }
        let meta: Option<Vec<u8>> = if args.len() >= 6 && args[4].eq_ignore_ascii_case(b"META") {
            Some(args[5].clone())
        } else {
            None
        };
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
        if self.index_cap > 0 {
            let kind = vs.kind();
            let meta_len = meta.as_ref().map_or(0, |m| m.len());
            let new = entry_bytes(vs.dim(), id.len(), meta_len, kind);
            let old = vs.get(id).map_or(0, |(_, m)| {
                entry_bytes(vs.dim(), id.len(), m.map_or(0, |x| x.len()), kind)
            });
            let used = self.ns_bytes.get(ns).copied().unwrap_or(0);
            if used.saturating_add(new).saturating_sub(old) > self.index_cap {
                return Plan::Reply(err(&format!(
                    "VECFULL index memory limit reached ({used} of {} bytes for this namespace)",
                    self.index_cap
                )));
            }
        }
        Plan::Write {
            persist: Persist::Put {
                key: durable_key(b'v', set, id),
                val: encode_vec_row(&vec, meta.as_deref()),
            },
            apply: Apply::Insert {
                set: set.to_vec(),
                id: id.to_vec(),
                vec,
                meta,
            },
            ok: Value::Simple("OK".into()),
        }
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
                persist: Persist::Del {
                    key: durable_key(b'v', set, id),
                },
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

    fn get(&self, ns: &[u8], args: &[Vec<u8>]) -> Value {
        if args.len() < 3 {
            return err("ERR VEC.GET <set> <id>");
        }
        let Some(vs) = self.sets.get(&(ns.to_vec(), args[1].to_vec())) else {
            return err("ERR no such set");
        };
        match vs.get(&args[2]) {
            None => Value::Null,
            Some((vec, meta)) => {
                let mut row = vec![Value::Bulk(Some(floats_to_ascii(&vec).into_bytes()))];
                if let Some(m) = meta {
                    row.push(Value::Bulk(Some(m)));
                }
                Value::Array(Some(row))
            }
        }
    }

    /// VEC.SEARCH <set> <query> <k> [EF <n>]
    fn search(&self, ns: &[u8], args: &[Vec<u8>]) -> Value {
        if args.len() < 4 {
            return err("ERR VEC.SEARCH <set> <query> <k> [EF <n>]");
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
        // Optional `EF <n>`: HNSW's recall/latency knob. Flat ignores it.
        let ef = if args.len() >= 6 && args[4].eq_ignore_ascii_case(b"EF") {
            std::str::from_utf8(&args[5])
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(hnsw::EF_SEARCH_DEFAULT)
        } else {
            hnsw::EF_SEARCH_DEFAULT
        };
        let rows: Vec<Value> = vs
            .search(&query, k, ef)
            .into_iter()
            .map(|(id, score)| {
                Value::Array(Some(vec![
                    Value::Bulk(Some(id)),
                    Value::Double(score as f64),
                ]))
            })
            .collect();
        Value::Array(Some(rows))
    }

    fn info(&self, ns: &[u8], args: &[Vec<u8>]) -> Value {
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
            // Namespace-wide (D4): this tenant's estimated index RAM and its cap
            // (0 = unlimited). Reported here so a tenant can see the headroom.
            bulk("ns_mem_bytes"),
            Value::Integer(self.ns_mem_bytes(ns) as i64),
            bulk("ns_mem_cap"),
            Value::Integer(self.index_cap as i64),
        ]))
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
    /// commit and return `ok`.
    fn run(st: &mut Store, ns: &[u8], args: &[Vec<u8>]) -> Value {
        match st.plan(ns, args) {
            Plan::Reply(v) => v,
            Plan::Write { apply, ok, .. } => {
                st.commit(ns, apply);
                ok
            }
        }
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
            st.plan(ns, &cmd(&["VEC.CREATE", "docs", "DIM", "2", "METRIC", "l2"])),
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
            st.plan(ns, &cmd(&["VEC.SET", "docs", "d", "1,2,3"])),
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
        match st.plan(
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
            Plan::Write {
                persist: Persist::Put { val, .. },
                ..
            } => assert!(
                val.ends_with(b"|hnsw"),
                "durable config must record the hnsw kind for rebuild, got {:?}",
                String::from_utf8_lossy(&val)
            ),
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
        match st.plan(b"n", &cmd(&["VEC.SET", "s", "id7", "1,2"])) {
            Plan::Write {
                persist: Persist::Put { key, .. },
                apply: Apply::Insert { id, vec, .. },
                ok,
            } => {
                assert_eq!(key, durable_key(b'v', b"s", b"id7"));
                assert_eq!(id, v("id7"));
                assert_eq!(vec, vec![1.0, 2.0]);
                assert_eq!(ok, Value::Simple("OK".into()));
            }
            _ => panic!("VEC.SET should plan a Write"),
        }
        // Because plan() does not mutate, the index is still empty until commit.
        assert!(matches!(
            st.plan(b"n", &cmd(&["VEC.GET", "s", "id7"])),
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
            st.plan(ns, &cmd(&["VEC.DEL", "s", "x"])),
            Plan::Write {
                persist: Persist::Del { .. },
                ok: Value::Integer(1),
                ..
            }
        ));
        assert_eq!(
            run(&mut st, ns, &cmd(&["VEC.DEL", "s", "x"])),
            Value::Integer(1)
        );
        assert!(matches!(
            st.plan(ns, &cmd(&["VEC.DEL", "s", "x"])),
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
            st.plan(b"tenantB", &cmd(&["VEC.SEARCH", "s", "9,9", "1"])),
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
        match st.plan(
            b"n",
            &cmd(&["VEC.CREATE", "docs", "DIM", "4", "METRIC", "cosine"]),
        ) {
            Plan::Write {
                persist: Persist::Put { key, val },
                ..
            } => {
                assert_eq!(parse_durable_key(&key), Some((b'c', v("docs"), Vec::new())));
                assert_eq!(
                    decode_config(&val),
                    Some((4, Metric::Cosine, IndexKind::Flat))
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
        match st.plan(
            b"n",
            &cmd(&["VEC.SET", "docs", "id9", "1,2,3,4", "META", "m!"]),
        ) {
            Plan::Write {
                persist: Persist::Put { key, val },
                ..
            } => {
                assert_eq!(parse_durable_key(&key), Some((b'v', v("docs"), v("id9"))));
                let (vec, meta) = decode_vec_row(&val).expect("decode vec row");
                assert_eq!(vec, vec![1.0, 2.0, 3.0, 4.0]);
                assert_eq!(meta, Some(v("m!")));
            }
            _ => panic!("set should plan a vector Put"),
        }
        // A foreign (non-prefixed) key is not one of ours — the scan skips it.
        assert_eq!(parse_durable_key(b"regular:key"), None);
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
            st.plan(ns, &[v("VEC.SET"), v("s"), vec![0], v("1,2")]),
            Plan::Reply(Value::Error(e)) if e.contains("invalid set or id")
        ));
        // non-numeric vector
        assert!(matches!(
            st.plan(ns, &cmd(&["VEC.SET", "s", "id", "1,x"])),
            Plan::Reply(Value::Error(e)) if e.contains("not a float")
        ));
        // unknown VEC command
        assert!(matches!(
            st.plan(ns, &cmd(&["VEC.NOPE", "s"])),
            Plan::Reply(Value::Error(e)) if e.contains("unknown VEC command")
        ));
    }

    /// D4: the per-namespace index-memory cap refuses a write over the limit
    /// (before persisting), frees on DEL, doesn't double-charge an upsert, and
    /// is isolated per tenant. INFO surfaces the usage and the cap.
    #[test]
    fn index_memory_cap_bounds_a_namespace() {
        let mut st = Store::new();
        st.set_index_cap(200); // ~2 dim-4 flat vectors (81 B each by the estimate)
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
            st.plan(ns, &cmd(&["VEC.SET", "s", "c", "0,0,1,0"])),
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
        // INFO surfaces the cap and non-zero usage for this namespace.
        match run(&mut st, ns, &cmd(&["VEC.INFO", "s"])) {
            Value::Array(Some(f)) => {
                assert_eq!(f[10], Value::Bulk(Some(b"ns_mem_cap".to_vec())));
                assert_eq!(f[11], Value::Integer(200));
                assert!(
                    matches!(f[9], Value::Integer(n) if n > 0),
                    "ns_mem_bytes > 0"
                );
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
}
