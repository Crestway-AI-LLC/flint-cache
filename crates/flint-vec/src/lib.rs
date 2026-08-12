// SPDX-License-Identifier: Elastic-2.0
//! The vector-memory co-processor (ADR-0017): a flat (exact) approximate-
//! nearest-neighbour index served beside the data plane, reached through the
//! ADR-0010 co-processor mechanism as the `VEC.*` command family.
//!
//! This crate is the co-processor's LOGIC — the flat index, the per-tenant
//! store, and the `VEC.*` command dispatch — with no networking. A `VEC.SEARCH`
//! is answered entirely from the in-memory index; a `VEC.SET`/`VEC.DEL`/
//! `VEC.CREATE` additionally emits a [`Persist`] intent that the networked
//! co-processor performs over the channel, so vectors are durable in the
//! tenant's namespace (ADR-0017 D2) and the index rebuilds from them on a cold
//! start (D3). Keeping the logic here, `Value`-typed and unit-tested, means the
//! wire layer is a thin translation and the recall/latency oracle (`flat`) can
//! be validated without a cluster.
//!
//! v0.1 is flat/exact — the recall oracle. v0.2 replaces [`VectorSet`]'s search
//! with an HNSW graph behind the SAME dispatch; v1 makes it disk-resident
//! (DiskANN). None of those changes the `VEC.*` surface or the durable format.

use flint_resp::Value;
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

/// One vector set: fixed dim + metric, an id->Entry map for O(1) upsert/delete,
/// and exact top-k. `search` is the only method v0.2/v1 re-implement.
pub struct VectorSet {
    dim: usize,
    metric: Metric,
    entries: HashMap<Vec<u8>, Entry>,
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

impl VectorSet {
    pub fn new(dim: usize, metric: Metric) -> Self {
        VectorSet {
            dim,
            metric,
            entries: HashMap::new(),
        }
    }

    pub fn dim(&self) -> usize {
        self.dim
    }
    pub fn metric(&self) -> Metric {
        self.metric
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn set(&mut self, id: Vec<u8>, vec: Vec<f32>, meta: Option<Vec<u8>>) {
        let norm = dot(&vec, &vec).sqrt();
        self.entries.insert(id, Entry { vec, norm, meta });
    }

    fn del(&mut self, id: &[u8]) -> bool {
        self.entries.remove(id).is_some()
    }

    fn score(&self, query: &[f32], q_norm: f32, e: &Entry) -> f32 {
        match self.metric {
            Metric::Ip => dot(query, &e.vec),
            Metric::L2 => {
                // -||q - v||^2, without allocating a diff vector.
                -query
                    .iter()
                    .zip(&e.vec)
                    .map(|(q, x)| {
                        let d = q - x;
                        d * d
                    })
                    .sum::<f32>()
            }
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

    /// Exact top-k, nearest first (recall 1.0). O(n log k).
    pub fn search(&self, query: &[f32], k: usize) -> Vec<(Vec<u8>, f32)> {
        if k == 0 {
            return Vec::new();
        }
        let q_norm = if self.metric == Metric::Cosine {
            dot(query, query).sqrt()
        } else {
            0.0
        };
        let mut heap: BinaryHeap<std::cmp::Reverse<Scored>> = BinaryHeap::with_capacity(k + 1);
        for (id, e) in &self.entries {
            let score = self.score(query, q_norm, e);
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
}

/// A durability intent: what the co-processor must do over the channel as a
/// side effect of a command, so the vectors survive a co-processor crash
/// (ADR-0017 D2). The networked layer performs it; the logic here only decides
/// it, which keeps the decision unit-testable. Keys live under a reserved
/// co-processor prefix in the tenant namespace ([`durable_key`]).
#[derive(Debug, PartialEq)]
pub enum Persist {
    None,
    Put { key: Vec<u8>, val: Vec<u8> },
    Del { key: Vec<u8> },
}

/// The reserved key prefix for co-processor-owned rows in a tenant namespace.
/// A leading NUL keeps these out of any ordinary keyspace a client walks, and
/// the `vec` tag namespaces this co-processor from any future one.
const KEY_PREFIX: &[u8] = b"\x00vec\x00";

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

fn valid_name(b: &[u8]) -> bool {
    !b.is_empty() && !b.contains(&0)
}

fn err(msg: &str) -> Value {
    Value::Error(msg.to_string())
}

/// The co-processor's per-tenant, per-set state and the `VEC.*` dispatch. One
/// `Store` per running co-processor; tenants are isolated by the namespace
/// component of the key (ADR-0017 D1/D4).
#[derive(Default)]
pub struct Store {
    sets: HashMap<(Vec<u8>, Vec<u8>), VectorSet>,
}

impl Store {
    pub fn new() -> Self {
        Store::default()
    }

    /// Number of sets held (across all tenants) — for INFO/metrics.
    pub fn set_count(&self) -> usize {
        self.sets.len()
    }

    /// Dispatch one `VEC.*` command for tenant `ns`. Returns the reply and any
    /// durability side effect the caller must perform over the channel.
    /// `args[0]` is the command name (`VEC.SET`, …).
    pub fn dispatch(&mut self, ns: &[u8], args: &[Vec<u8>]) -> (Value, Persist) {
        let Some(name) = args.first() else {
            return (err("ERR empty command"), Persist::None);
        };
        match name.to_ascii_uppercase().as_slice() {
            b"VEC.CREATE" => self.create(ns, args),
            b"VEC.SET" => self.set(ns, args),
            b"VEC.GET" => (self.get(ns, args), Persist::None),
            b"VEC.DEL" => self.delete(ns, args),
            b"VEC.SEARCH" => (self.search(ns, args), Persist::None),
            b"VEC.INFO" => (self.info(ns, args), Persist::None),
            other => (
                err(&format!(
                    "ERR unknown VEC command '{}'",
                    String::from_utf8_lossy(other)
                )),
                Persist::None,
            ),
        }
    }

    /// VEC.CREATE <set> DIM <d> METRIC <cosine|l2|ip> [INDEX flat]
    fn create(&mut self, ns: &[u8], args: &[Vec<u8>]) -> (Value, Persist) {
        // args: 0=cmd 1=set 2="DIM" 3=<d> 4="METRIC" 5=<m> [6="INDEX" 7=flat]
        if args.len() < 6 {
            return (
                err("ERR VEC.CREATE <set> DIM <d> METRIC <cosine|l2|ip>"),
                Persist::None,
            );
        }
        let set = &args[1];
        if !valid_name(set) {
            return (err("ERR invalid set name"), Persist::None);
        }
        if !args[2].eq_ignore_ascii_case(b"DIM") || !args[4].eq_ignore_ascii_case(b"METRIC") {
            return (
                err("ERR VEC.CREATE <set> DIM <d> METRIC <cosine|l2|ip>"),
                Persist::None,
            );
        }
        let dim = match std::str::from_utf8(&args[3])
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
        {
            Some(d) if d >= 1 => d,
            _ => return (err("ERR DIM must be a positive integer"), Persist::None),
        };
        let Some(metric) = Metric::parse(&args[5]) else {
            return (
                err("ERR METRIC must be one of cosine, l2, ip"),
                Persist::None,
            );
        };
        let key = (ns.to_vec(), set.to_vec());
        if self.sets.contains_key(&key) {
            return (err("ERR set already exists"), Persist::None);
        }
        self.sets.insert(key, VectorSet::new(dim, metric));
        let cfg = format!("{dim}|{}", metric.as_str()).into_bytes();
        (
            Value::Simple("OK".into()),
            Persist::Put {
                key: durable_key(b'c', set, b""),
                val: cfg,
            },
        )
    }

    /// VEC.SET <set> <id> <vec> [META <json>]
    fn set(&mut self, ns: &[u8], args: &[Vec<u8>]) -> (Value, Persist) {
        if args.len() < 4 {
            return (
                err("ERR VEC.SET <set> <id> <vec> [META <json>]"),
                Persist::None,
            );
        }
        let (set, id, raw) = (&args[1], &args[2], &args[3]);
        if !valid_name(set) || !valid_name(id) {
            return (
                err("ERR invalid set or id (must be non-empty, no NUL)"),
                Persist::None,
            );
        }
        let meta: Option<Vec<u8>> = if args.len() >= 6 && args[4].eq_ignore_ascii_case(b"META") {
            Some(args[5].clone())
        } else {
            None
        };
        let Some(vs) = self.sets.get_mut(&(ns.to_vec(), set.to_vec())) else {
            return (err("ERR no such set (VEC.CREATE it first)"), Persist::None);
        };
        let vec = match parse_vector(raw) {
            Ok(v) => v,
            Err(e) => return (err(&format!("ERR {e}")), Persist::None),
        };
        if vec.len() != vs.dim() {
            return (
                err(&format!(
                    "WRONGDIM set is {}, vector is {}",
                    vs.dim(),
                    vec.len()
                )),
                Persist::None,
            );
        }
        let dkey = durable_key(b'v', set, id);
        let dval = encode_vec_row(&vec, meta.as_deref());
        vs.set(id.to_vec(), vec, meta);
        (
            Value::Simple("OK".into()),
            Persist::Put {
                key: dkey,
                val: dval,
            },
        )
    }

    fn get(&self, ns: &[u8], args: &[Vec<u8>]) -> Value {
        if args.len() < 3 {
            return err("ERR VEC.GET <set> <id>");
        }
        let Some(vs) = self.sets.get(&(ns.to_vec(), args[1].to_vec())) else {
            return err("ERR no such set");
        };
        match vs.entries.get(&args[2]) {
            None => Value::Null,
            Some(e) => {
                let mut row = vec![Value::Bulk(Some(floats_to_ascii(&e.vec).into_bytes()))];
                if let Some(m) = &e.meta {
                    row.push(Value::Bulk(Some(m.clone())));
                }
                Value::Array(Some(row))
            }
        }
    }

    /// VEC.DEL <set> <id>
    fn delete(&mut self, ns: &[u8], args: &[Vec<u8>]) -> (Value, Persist) {
        if args.len() < 3 {
            return (err("ERR VEC.DEL <set> <id>"), Persist::None);
        }
        let (set, id) = (&args[1], &args[2]);
        let Some(vs) = self.sets.get_mut(&(ns.to_vec(), set.to_vec())) else {
            return (err("ERR no such set"), Persist::None);
        };
        if vs.del(id) {
            (
                Value::Integer(1),
                Persist::Del {
                    key: durable_key(b'v', set, id),
                },
            )
        } else {
            (Value::Integer(0), Persist::None)
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
        // EF (args[4..]) is the HNSW recall/latency knob; flat ignores it, but
        // accepting it keeps the wire stable across the flat->HNSW swap.
        let rows: Vec<Value> = vs
            .search(&query, k)
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
            bulk("flat"),
            bulk("count"),
            Value::Integer(vs.len() as i64),
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

    #[test]
    fn create_set_search_end_to_end() {
        let mut st = Store::new();
        let ns = b"nsA";

        // CREATE
        let (r, p) = st.dispatch(
            ns,
            &cmd(&["VEC.CREATE", "docs", "DIM", "2", "METRIC", "l2"]),
        );
        assert_eq!(r, Value::Simple("OK".into()));
        assert!(
            matches!(p, Persist::Put { .. }),
            "create persists its config"
        );

        // duplicate CREATE errors
        let (r, _) = st.dispatch(
            ns,
            &cmd(&["VEC.CREATE", "docs", "DIM", "2", "METRIC", "l2"]),
        );
        assert!(matches!(r, Value::Error(e) if e.contains("already exists")));

        // SET three vectors, each persisted
        for (id, vec) in [("a", "0,0"), ("b", "1,0"), ("c", "3,4")] {
            let (r, p) = st.dispatch(ns, &cmd(&["VEC.SET", "docs", id, vec]));
            assert_eq!(r, Value::Simple("OK".into()));
            assert!(matches!(p, Persist::Put { .. }));
        }

        // wrong dimension is refused
        let (r, p) = st.dispatch(ns, &cmd(&["VEC.SET", "docs", "d", "1,2,3"]));
        assert!(matches!(r, Value::Error(e) if e.starts_with("WRONGDIM")));
        assert_eq!(p, Persist::None, "a refused write persists nothing");

        // SEARCH from the origin: a (0), b (1), c (5)
        let r = st.search(ns, &cmd(&["VEC.SEARCH", "docs", "0,0", "2"]));
        match r {
            Value::Array(Some(rows)) => {
                assert_eq!(rows.len(), 2, "k=2");
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

    #[test]
    fn get_and_del_and_count() {
        let mut st = Store::new();
        let ns = b"nsA";
        st.dispatch(
            ns,
            &cmd(&["VEC.CREATE", "s", "DIM", "3", "METRIC", "cosine"]),
        );
        st.dispatch(
            ns,
            &cmd(&["VEC.SET", "s", "x", "1,2,3", "META", "{\"t\":1}"]),
        );

        // GET returns the vector and the meta.
        match st.get(ns, &cmd(&["VEC.GET", "s", "x"])) {
            Value::Array(Some(row)) => {
                assert_eq!(row[0], Value::Bulk(Some(v("1,2,3"))));
                assert_eq!(row[1], Value::Bulk(Some(v("{\"t\":1}"))));
            }
            other => panic!("expected array, got {other:?}"),
        }
        assert_eq!(st.get(ns, &cmd(&["VEC.GET", "s", "missing"])), Value::Null);

        // DEL returns 1 then 0, and persists a delete only when it hit.
        let (r, p) = st.delete(ns, &cmd(&["VEC.DEL", "s", "x"]));
        assert_eq!(r, Value::Integer(1));
        assert!(matches!(p, Persist::Del { .. }));
        let (r, p) = st.delete(ns, &cmd(&["VEC.DEL", "s", "x"]));
        assert_eq!(r, Value::Integer(0));
        assert_eq!(p, Persist::None);
    }

    #[test]
    fn tenants_are_isolated_by_namespace() {
        let mut st = Store::new();
        st.dispatch(
            b"tenantA",
            &cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "ip"]),
        );
        st.dispatch(b"tenantA", &cmd(&["VEC.SET", "s", "secret", "9,9"]));
        // tenantB has no set named "s" of its own.
        assert!(matches!(
            st.search(b"tenantB", &cmd(&["VEC.SEARCH", "s", "9,9", "1"])),
            Value::Error(e) if e.contains("no such set")
        ));
        // Same set name under B is a DIFFERENT index and cannot see A's vector.
        st.dispatch(
            b"tenantB",
            &cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "ip"]),
        );
        match st.search(b"tenantB", &cmd(&["VEC.SEARCH", "s", "9,9", "5"])) {
            Value::Array(Some(rows)) => assert!(rows.is_empty(), "B's index is empty"),
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
    fn invalid_names_and_bad_vectors_are_refused() {
        let mut st = Store::new();
        let ns = b"n";
        st.dispatch(ns, &cmd(&["VEC.CREATE", "s", "DIM", "2", "METRIC", "l2"]));
        // NUL in id
        let (r, _) = st.dispatch(ns, &[v("VEC.SET"), v("s"), vec![0], v("1,2")]);
        assert!(matches!(r, Value::Error(e) if e.contains("invalid set or id")));
        // non-numeric vector
        let (r, _) = st.dispatch(ns, &cmd(&["VEC.SET", "s", "id", "1,x"]));
        assert!(matches!(r, Value::Error(e) if e.contains("not a float")));
        // unknown VEC command
        let (r, _) = st.dispatch(ns, &cmd(&["VEC.NOPE", "s"]));
        assert!(matches!(r, Value::Error(e) if e.contains("unknown VEC command")));
    }
}
