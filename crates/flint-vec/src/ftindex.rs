// SPDX-License-Identifier: Elastic-2.0
//! One `FT.*` index's state in the co-processor (ADR-0060): the documents
//! under its prefixes, as the values its attributes index, and for an index
//! with a VECTOR field a vector set over the same keys. The documents
//! themselves stay in the tenant's keys; this holds only what a search needs
//! to choose them, and is rebuilt from those keys.

use crate::filter::{self, Field, FieldKind, Val, Vals};
use crate::ft::{IndexDef, On, Search, VectorField, distance, vector_from_bytes};
use crate::{Quant, VectorSet, hnsw};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;

/// A document as read from its key.
pub enum Source<'a> {
    /// A HASH's fields and values, in `HGETALL`'s order.
    Hash(&'a [(Vec<u8>, Vec<u8>)]),
    /// A JSON document.
    Json(&'a Value),
}

/// What one document holds for the index.
struct Doc {
    /// Its attributes' values, in the definition's order; `None` when it has
    /// none of them.
    vals: Option<Vals>,
    /// Absolute expiry in ms since epoch, from the key's own TTL.
    expires_at: Option<u64>,
    /// The order of its last write among the index's documents. Results
    /// that tie come in this order, as RediSearch's document ids, which it
    /// assigns at each write, order them (amendment 3, part 6).
    seq: u64,
}

/// What a document map entry costs the D4 meter beyond its key and values:
/// the `Doc`, its key's box and its slot in the map.
const DOC_BYTES: usize = 104;

/// How many more neighbours a KNN asks its set for than it answers, so that
/// documents tied at its k-th distance are chosen by write order rather than
/// by however the set found them.
const TIE_SLACK: usize = 16;

pub struct FtIndex {
    def: IndexDef,
    fields: Box<[Field]>,
    docs: HashMap<Vec<u8>, Doc>,
    /// Over the documents that hold a vector, keyed as `docs` is.
    set: Option<VectorSet>,
    /// Documents left out since the index was made, as RediSearch counts
    /// `hash_indexing_failures`: a vector that is not `DIM` float32s.
    failures: u64,
    /// Whether it holds every document under its prefixes: false while
    /// `FT.CREATE`'s backfill or a rebuild is still reading them, when a
    /// search is not answered (a partial answer is a wrong one).
    ready: bool,
    /// The next document write's `seq`.
    next_seq: u64,
}

/// One search's answer before the documents are read: how many matched, and
/// the page asked for, each key with its distance when the search was a KNN.
#[derive(Debug, PartialEq)]
pub struct Hits {
    pub total: usize,
    pub keys: Vec<(Vec<u8>, Option<f32>)>,
}

impl FtIndex {
    pub fn new(def: IndexDef) -> Self {
        let fields: Box<[Field]> = def.fields().into_boxed_slice();
        let set = def.vector.as_ref().map(|v| {
            VectorSet::new(v.dim, v.metric, v.index, Quant::None).with_fields(fields.to_vec())
        });
        FtIndex {
            def,
            fields,
            docs: HashMap::new(),
            set,
            failures: 0,
            ready: false,
            next_seq: 0,
        }
    }

    pub fn ready(&self) -> bool {
        self.ready
    }

    pub fn set_ready(&mut self) {
        self.ready = true;
    }

    pub fn def(&self) -> &IndexDef {
        &self.def
    }

    /// Whether `key` is one of this index's documents: under one of its
    /// prefixes.
    pub fn covers(&self, key: &[u8]) -> bool {
        self.def.prefixes.iter().any(|p| key.starts_with(p))
    }

    pub fn len(&self) -> usize {
        self.docs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    pub fn failures(&self) -> u64 {
        self.failures
    }

    /// Index `key` as `src` now holds it, replacing what it held. A
    /// document whose vector is not `DIM` float32s is left out of the index
    /// entirely, as RediSearch leaves it out, and counted; the error says
    /// why. `src` of the wrong type for the index (a string under an `ON
    /// HASH` prefix) is left out uncounted, as RediSearch skips it.
    pub fn put(
        &mut self,
        key: &[u8],
        src: Source<'_>,
        expires_at: Option<u64>,
    ) -> Result<(), String> {
        let (vals, vector) = match (self.def.on, src) {
            (On::Hash, Source::Hash(fields)) => self.read_hash(fields),
            (On::Json, Source::Json(doc)) => self.read_json(doc),
            _ => {
                self.remove(key);
                return Ok(());
            }
        };
        let vector = match vector {
            Ok(v) => v,
            Err(e) => {
                self.remove(key);
                self.failures += 1;
                return Err(e);
            }
        };
        if let Some(set) = &mut self.set {
            match vector {
                Some(v) => set.set(
                    key.to_vec(),
                    v,
                    meta_of(&self.fields, vals.as_deref()),
                    expires_at,
                ),
                None => {
                    set.del(key);
                }
            }
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.docs.insert(
            key.to_vec(),
            Doc {
                vals,
                expires_at,
                seq,
            },
        );
        Ok(())
    }

    /// Drop `key` from the index; whether it was there.
    pub fn remove(&mut self, key: &[u8]) -> bool {
        if let Some(set) = &mut self.set {
            set.del(key);
        }
        self.docs.remove(key).is_some()
    }

    /// Set or clear `key`'s expiry without re-reading it (`EXPIRE`,
    /// `PERSIST`): whether it is a document.
    pub fn set_expiry(&mut self, key: &[u8], expires_at: Option<u64>) -> bool {
        let Some(doc) = self.docs.get_mut(key) else {
            return false;
        };
        doc.expires_at = expires_at;
        if let Some(set) = &mut self.set
            && set.contains(key)
        {
            match expires_at {
                Some(e) => {
                    set.expiry.insert(key.to_vec(), e);
                }
                None => {
                    set.expiry.remove(key);
                }
            }
        }
        true
    }

    /// Drop every document whose TTL has passed as of `now`; how many.
    pub fn sweep_expired(&mut self, now: u64) -> usize {
        let gone: Vec<Vec<u8>> = self
            .docs
            .iter()
            .filter(|(_, d)| d.expires_at.is_some_and(|e| e <= now))
            .map(|(k, _)| k.clone())
            .collect();
        for k in &gone {
            self.remove(k);
        }
        gone.len()
    }

    /// What `key` costs the D4 meter now: its document's entry and values,
    /// and its entry in the vector set. 0 when it is not a document.
    pub fn charge(&self, key: &[u8]) -> usize {
        let Some(d) = self.docs.get(key) else {
            return 0;
        };
        DOC_BYTES
            + key.len()
            + filter::vals_bytes(d.vals.as_deref())
            + self
                .set
                .as_ref()
                .and_then(|s| s.entry_cost(key))
                .unwrap_or(0)
    }

    /// What the vector set holds beyond its live entries: deleted HNSW
    /// slots, which wait for its next insert (BUG-0257).
    pub fn held(&self) -> usize {
        self.set.as_ref().map_or(0, |s| s.held())
    }

    /// What the whole index costs the D4 meter: every document's
    /// [`charge`](Self::charge), and what the vector set holds.
    pub fn bytes(&self) -> usize {
        self.docs.keys().map(|k| self.charge(k)).sum::<usize>() + self.held()
    }

    /// Every document's key, for a rebuild's or `FT.DROPINDEX DD`'s walk.
    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.docs.keys().map(|k| k.as_slice())
    }

    /// Documents whose TTL has passed as of `now`.
    pub fn expired(&self, now: u64) -> Vec<Vec<u8>> {
        self.docs
            .iter()
            .filter(|(_, d)| d.expires_at.is_some_and(|e| e <= now))
            .map(|(k, _)| k.clone())
            .collect()
    }

    fn live(&self, doc: &Doc, now: u64) -> bool {
        doc.expires_at.is_none_or(|e| e > now)
    }

    /// `key`'s write order, or last for a key the index does not hold.
    fn seq(&self, key: &[u8]) -> u64 {
        self.docs.get(key).map_or(u64::MAX, |d| d.seq)
    }

    /// The documents `s` asks for, as of `now`: the `k` nearest the filter
    /// matches for a KNN, else every match; then sorted, then paged.
    pub fn search(&self, s: &Search, now: u64) -> Result<Hits, String> {
        let mut hits: Vec<(Vec<u8>, Option<f32>)> = match (&s.knn, &self.set, &self.def.vector) {
            (Some(knn), Some(set), Some(v)) => {
                let expired = set.expired_count(now);
                let ef = knn.ef.or(v.ef_runtime).unwrap_or(hnsw::EF_SEARCH_DEFAULT);
                let ask = knn.k.saturating_add(expired).saturating_add(TIE_SLACK);
                let mut near: Vec<(Vec<u8>, Option<f32>)> = set
                    .search(&knn.vector, ask, ef, ask, s.filter.as_ref(), None)?
                    .into_iter()
                    .filter(|(id, _)| !set.is_expired(id, now))
                    .map(|(id, score)| (id, Some(distance(v.metric, score))))
                    .collect();
                // Nearest first, and a tie to the earlier write.
                near.sort_by(|a, b| {
                    a.1.partial_cmp(&b.1)
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| self.seq(&a.0).cmp(&self.seq(&b.0)))
                });
                near.truncate(knn.k);
                near
            }
            (Some(_), _, _) => return Err("this index has no VECTOR field".into()),
            (None, _, _) => {
                let mut keys: Vec<(Vec<u8>, Option<f32>)> = self
                    .docs
                    .iter()
                    .filter(|(_, d)| self.live(d, now))
                    .filter(|(_, d)| {
                        s.filter
                            .as_ref()
                            .is_none_or(|f| f.matches(d.vals.as_deref()))
                    })
                    .map(|(k, _)| (k.clone(), None))
                    .collect();
                // In write order, as RediSearch's document ids order them,
                // so a page is the same page each time it is asked for.
                keys.sort_by_key(|(k, _)| self.seq(k));
                keys
            }
        };
        let total = hits.len();
        if let Some((by, asc)) = &s.sortby {
            let by_distance = s.knn.as_ref().is_some_and(|k| k.alias == *by);
            if by_distance {
                // KNN's answer is nearest first already.
                if !asc {
                    hits.reverse();
                }
            } else if let Some(at) = self.fields.iter().position(|f| f.name == *by) {
                let val = |k: &[u8]| self.docs.get(k).and_then(|d| d.vals.as_ref()?.get(at));
                hits.sort_by(|a, b| {
                    let o = sort_order(val(&a.0), val(&b.0), *asc);
                    o.then_with(|| self.seq(&a.0).cmp(&self.seq(&b.0)))
                });
            }
        }
        let keys = hits.into_iter().skip(s.offset).take(s.num).collect();
        Ok(Hits { total, keys })
    }

    fn read_hash(
        &self,
        fields: &[(Vec<u8>, Vec<u8>)],
    ) -> (Option<Vals>, Result<Option<Vec<f32>>, String>) {
        let get = |name: &str| {
            fields
                .iter()
                .rev()
                .find(|(f, _)| f == name.as_bytes())
                .map(|(_, v)| v.as_slice())
        };
        let vals: Vals = self
            .def
            .attrs
            .iter()
            .map(|a| match get(&a.path) {
                Some(raw) => filter::hash_val(a.field.kind, raw, a.separator),
                None => Val::Missing,
            })
            .collect();
        let vector = match &self.def.vector {
            None => Ok(None),
            Some(v) => match get(&v.path) {
                None => Ok(None),
                Some(raw) => vector_from_bytes(raw, v.dim)
                    .map(Some)
                    .map_err(|e| format!("{} is not indexed: {e}", v.name)),
            },
        };
        (any(vals), vector)
    }

    fn read_json(&self, doc: &Value) -> (Option<Vals>, Result<Option<Vec<f32>>, String>) {
        let vals: Vals = self
            .def
            .attrs
            .iter()
            .map(|a| json_val(a.field.kind, a.separator, json_at(doc, &a.path)))
            .collect();
        let vector = match &self.def.vector {
            None => Ok(None),
            Some(v) => json_vector(v, json_at(doc, &v.path)),
        };
        (any(vals), vector)
    }
}

fn any(vals: Vals) -> Option<Vals> {
    vals.iter().any(|v| *v != Val::Missing).then_some(vals)
}

/// The value at `$.a.b` in `doc`.
fn json_at<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    path.strip_prefix("$.")?
        .split('.')
        .try_fold(doc, |v, m| v.get(m))
}

/// A JSON attribute's value: a TAG from a string or an array of strings, a
/// NUMERIC from a number, a TEXT from a string. Anything else is missing,
/// as RediSearch indexes nothing for it.
fn json_val(kind: FieldKind, separator: u8, v: Option<&Value>) -> Val {
    match (kind, v) {
        (FieldKind::Tag | FieldKind::FoldedTag, Some(Value::String(s))) => {
            json_tags(kind, separator, std::iter::once(s.as_str()))
        }
        (FieldKind::Tag | FieldKind::FoldedTag, Some(Value::Array(a))) => {
            json_tags(kind, separator, a.iter().filter_map(|x| x.as_str()))
        }
        (FieldKind::Numeric, Some(Value::Number(n))) => match n.as_f64() {
            Some(x) => Val::Num(if x == 0.0 { 0.0 } else { x }),
            None => Val::Missing,
        },
        (FieldKind::Text, Some(Value::String(s))) => filter::hash_val(kind, s.as_bytes(), 0),
        _ => Val::Missing,
    }
}

/// A JSON TAG's strings as RediSearch reads them: each whole, unless the
/// field declared a separator, which splits each as a HASH field's is split.
fn json_tags<'a>(kind: FieldKind, separator: u8, strs: impl Iterator<Item = &'a str>) -> Val {
    let mut ts: Vec<Box<[u8]>> = Vec::new();
    for s in strs {
        if separator != 0 {
            if let Val::Tags(t) = filter::hash_val(kind, s.as_bytes(), separator) {
                ts.extend(t.into_vec());
            }
        } else if !s.is_empty() {
            ts.push(match kind {
                FieldKind::FoldedTag => filter::fold(s.as_bytes()),
                _ => s.as_bytes().into(),
            });
        }
    }
    ts.sort();
    ts.dedup();
    if ts.is_empty() {
        Val::Missing
    } else {
        Val::Tags(ts.into_boxed_slice())
    }
}

/// A JSON document's vector: an array of `DIM` numbers.
fn json_vector(v: &VectorField, at: Option<&Value>) -> Result<Option<Vec<f32>>, String> {
    let Some(at) = at else {
        return Ok(None);
    };
    let bad = || {
        format!(
            "{} is not indexed: it is not an array of {} numbers",
            v.name, v.dim
        )
    };
    let Value::Array(xs) = at else {
        return Err(bad());
    };
    if xs.len() != v.dim {
        return Err(bad());
    }
    let out: Option<Vec<f32>> = xs
        .iter()
        .map(|x| x.as_f64().map(|f| f as f32).filter(|f| f.is_finite()))
        .collect();
    out.map(Some).ok_or_else(bad)
}

/// The `META` the vector set indexes `vals` from: a JSON object of the
/// values already split and folded, which the set's extraction reads back
/// as the same values. A tag that is not UTF-8 has no JSON form and is left
/// out of the vector set's copy, so a filtered KNN cannot match on it.
fn meta_of(fields: &[Field], vals: Option<&[Val]>) -> Option<Vec<u8>> {
    let vals = vals?;
    let mut obj = serde_json::Map::new();
    for (f, v) in fields.iter().zip(vals) {
        let j = match v {
            Val::Missing => continue,
            Val::Tags(ts) => Value::Array(
                ts.iter()
                    .filter_map(|t| std::str::from_utf8(t).ok())
                    .map(|s| Value::String(s.into()))
                    .collect(),
            ),
            Val::Num(x) => match serde_json::Number::from_f64(*x) {
                Some(n) => Value::Number(n),
                None => continue,
            },
            Val::Text(ws) => Value::String(
                ws.iter()
                    .map(|w| String::from_utf8_lossy(w))
                    .collect::<Vec<_>>()
                    .join(" "),
            ),
        };
        obj.insert(f.name.to_string(), j);
    }
    Some(Value::Object(obj).to_string().into_bytes())
}

/// SORTBY's order of two values, a missing one last either way.
fn sort_order(a: Option<&Val>, b: Option<&Val>, asc: bool) -> Ordering {
    let key = |v: Option<&Val>| -> Option<SortKey> {
        match v? {
            Val::Num(x) => Some(SortKey::Num(*x)),
            Val::Tags(ts) => ts.first().map(|t| SortKey::Bytes(t.to_vec())),
            Val::Text(_) | Val::Missing => None,
        }
    };
    match (key(a), key(b)) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Greater,
        (Some(_), None) => Ordering::Less,
        (Some(x), Some(y)) => {
            let o = x.cmp(&y);
            if asc { o } else { o.reverse() }
        }
    }
}

enum SortKey {
    Num(f64),
    Bytes(Vec<u8>),
}

impl PartialEq for SortKey {
    fn eq(&self, o: &Self) -> bool {
        self.cmp(o) == Ordering::Equal
    }
}

impl Eq for SortKey {}

impl PartialOrd for SortKey {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

impl Ord for SortKey {
    fn cmp(&self, o: &Self) -> Ordering {
        match (self, o) {
            (SortKey::Num(a), SortKey::Num(b)) => a.total_cmp(b),
            (SortKey::Bytes(a), SortKey::Bytes(b)) => a.cmp(b),
            (SortKey::Num(_), SortKey::Bytes(_)) => Ordering::Less,
            (SortKey::Bytes(_), SortKey::Num(_)) => Ordering::Greater,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ft::{parse_create, parse_search};

    fn a(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.as_bytes().to_vec()).collect()
    }

    fn blob(v: &[f32]) -> Vec<u8> {
        v.iter().flat_map(|x| x.to_le_bytes()).collect()
    }

    fn hash_index() -> FtIndex {
        let (_, def) = parse_create(&a(&[
            "FT.CREATE",
            "idx",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "content",
            "TEXT",
            "user",
            "TAG",
            "price",
            "NUMERIC",
            "embedding",
            "VECTOR",
            "FLAT",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "2",
            "DISTANCE_METRIC",
            "L2",
        ]))
        .expect("parses");
        FtIndex::new(def)
    }

    fn put_hash(ix: &mut FtIndex, key: &str, user: &str, price: &str, v: &[f32]) {
        let fields = vec![
            (b"content".to_vec(), format!("the doc {key}").into_bytes()),
            (b"user".to_vec(), user.as_bytes().to_vec()),
            (b"price".to_vec(), price.as_bytes().to_vec()),
            (b"embedding".to_vec(), blob(v)),
        ];
        ix.put(key.as_bytes(), Source::Hash(&fields), None)
            .expect("indexed");
    }

    fn search(ix: &FtIndex, parts: &[&str], v: Option<&[f32]>) -> Hits {
        let mut args = a(parts);
        if let Some(v) = v {
            args.extend(a(&["PARAMS", "2", "v"]));
            args.push(blob(v));
        }
        let s = parse_search(&args, ix.def()).expect("parses");
        ix.search(&s, 1000).expect("searches")
    }

    fn keys(h: &Hits) -> Vec<String> {
        h.keys
            .iter()
            .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
            .collect()
    }

    /// Amendment 3, part 6: results that tie come in write order, as
    /// RediSearch's document ids order them, and a rewrite moves a document
    /// to the end. LlamaIndex's test embeds every text as one vector and
    /// expects the first document written.
    #[test]
    fn results_that_tie_come_in_write_order() {
        let mut ix = hash_index();
        // Written in an order unlike their keys' order, all at one point.
        for k in ["doc:3", "doc:1", "doc:2"] {
            put_hash(&mut ix, k, "u", "5", &[1.0, 1.0]);
        }
        let knn = |ix: &FtIndex, k: &str| {
            let q = format!("*=>[KNN {k} @embedding $v]");
            keys(&search(ix, &["FT.SEARCH", "idx", &q], Some(&[1.0, 1.0])))
        };
        assert_eq!(
            knn(&ix, "1"),
            ["doc:3"],
            "the k-th place goes to the first write"
        );
        assert_eq!(knn(&ix, "3"), ["doc:3", "doc:1", "doc:2"]);
        let all = |ix: &FtIndex| keys(&search(ix, &["FT.SEARCH", "idx", "*"], None));
        assert_eq!(all(&ix), ["doc:3", "doc:1", "doc:2"], "no order asked for");
        let by_price = |ix: &FtIndex| {
            let args = ["FT.SEARCH", "idx", "*", "SORTBY", "price", "ASC"];
            keys(&search(ix, &args, None))
        };
        assert_eq!(by_price(&ix), ["doc:3", "doc:1", "doc:2"], "a SORTBY tie");
        // A rewrite is a new write.
        put_hash(&mut ix, "doc:3", "u", "5", &[1.0, 1.0]);
        assert_eq!(knn(&ix, "1"), ["doc:1"]);
        assert_eq!(all(&ix), ["doc:1", "doc:2", "doc:3"]);
        // A nearer document comes before a farther one written earlier.
        put_hash(&mut ix, "doc:9", "u", "5", &[1.0, 1.1]);
        put_hash(&mut ix, "doc:0", "u", "5", &[1.0, 1.0]);
        assert_eq!(knn(&ix, "4"), ["doc:1", "doc:2", "doc:3", "doc:0"]);
    }

    #[test]
    fn a_filtered_knn_answers_the_nearest_matches_with_their_distances() {
        let mut ix = hash_index();
        put_hash(&mut ix, "doc:1", "Alice", "5", &[0.0, 0.0]);
        put_hash(&mut ix, "doc:2", "bob", "20", &[1.0, 0.0]);
        put_hash(&mut ix, "doc:3", "alice,carol", "30", &[2.0, 0.0]);
        put_hash(&mut ix, "doc:4", "ALICE", "40", &[3.0, 0.0]);
        let h = search(
            &ix,
            &[
                "FT.SEARCH",
                "idx",
                "(@user:{alice})=>[KNN 2 @embedding $v AS d]",
                "SORTBY",
                "d",
            ],
            Some(&[2.9, 0.0]),
        );
        assert_eq!(keys(&h), ["doc:4", "doc:3"]);
        assert_eq!(h.total, 2);
        let d = h.keys[0].1.expect("a distance");
        assert!((d - 0.01).abs() < 1e-4, "squared L2: {d}");
        // A phrase on the TEXT field, and a numeric range, filter-only.
        let h = search(&ix, &["FT.SEARCH", "idx", "@content:(\"doc doc:2\")"], None);
        assert_eq!(keys(&h), ["doc:2"]);
        let h = search(
            &ix,
            &[
                "FT.SEARCH",
                "idx",
                "@price:[10 30]",
                "SORTBY",
                "price",
                "DESC",
            ],
            None,
        );
        assert_eq!(keys(&h), ["doc:3", "doc:2"]);
        // LIMIT pages; total counts every match.
        let h = search(&ix, &["FT.SEARCH", "idx", "*", "LIMIT", "1", "2"], None);
        assert_eq!(
            (h.total, keys(&h)),
            (4, vec!["doc:2".to_string(), "doc:3".to_string()])
        );
    }

    #[test]
    fn a_rewrite_replaces_a_delete_removes_and_a_bad_vector_is_left_out() {
        let mut ix = hash_index();
        put_hash(&mut ix, "doc:1", "alice", "5", &[0.0, 0.0]);
        put_hash(&mut ix, "doc:1", "bob", "5", &[0.0, 0.0]);
        assert!(
            search(&ix, &["FT.SEARCH", "idx", "@user:{alice}"], None)
                .keys
                .is_empty()
        );
        let bad = vec![(b"embedding".to_vec(), b"abc".to_vec())];
        let e = ix
            .put(b"doc:1", Source::Hash(&bad), None)
            .expect_err("wrong length");
        assert!(e.contains("not indexed"), "{e}");
        assert_eq!((ix.len(), ix.failures()), (0, 1));
        put_hash(&mut ix, "doc:2", "bob", "5", &[0.0, 0.0]);
        assert!(ix.remove(b"doc:2"));
        assert!(ix.is_empty());
        // A document with no vector is indexed for its fields, and not
        // answered by a KNN.
        let novec = vec![(b"user".to_vec(), b"dave".to_vec())];
        ix.put(b"doc:5", Source::Hash(&novec), None)
            .expect("indexed");
        assert_eq!(
            keys(&search(&ix, &["FT.SEARCH", "idx", "@user:{dave}"], None)),
            ["doc:5"]
        );
        assert!(
            search(
                &ix,
                &["FT.SEARCH", "idx", "*=>[KNN 3 @embedding $v]"],
                Some(&[0.0, 0.0])
            )
            .keys
            .is_empty()
        );
    }

    #[test]
    fn an_expired_document_is_not_answered_and_the_sweep_drops_it() {
        let mut ix = hash_index();
        put_hash(&mut ix, "doc:1", "alice", "5", &[0.0, 0.0]);
        put_hash(&mut ix, "doc:2", "alice", "5", &[1.0, 0.0]);
        assert!(ix.set_expiry(b"doc:1", Some(500)));
        assert!(!ix.set_expiry(b"doc:9", Some(500)));
        let h = search(
            &ix,
            &["FT.SEARCH", "idx", "*=>[KNN 2 @embedding $v]"],
            Some(&[0.0, 0.0]),
        );
        assert_eq!(keys(&h), ["doc:2"]);
        assert_eq!(
            keys(&search(&ix, &["FT.SEARCH", "idx", "*"], None)),
            ["doc:2"]
        );
        assert_eq!(ix.sweep_expired(1000), 1);
        assert_eq!(ix.len(), 1);
    }

    #[test]
    fn a_json_index_reads_paths_arrays_and_vectors() {
        let (_, def) = parse_create(&a(&[
            "FT.CREATE",
            "j",
            "ON",
            "JSON",
            "PREFIX",
            "1",
            "j:",
            "SCHEMA",
            "$.meta.tags",
            "AS",
            "tags",
            "TAG",
            "$.n",
            "AS",
            "n",
            "NUMERIC",
            "$.v",
            "AS",
            "v",
            "VECTOR",
            "HNSW",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "2",
            "DISTANCE_METRIC",
            "COSINE",
        ]))
        .expect("parses");
        let mut ix = FtIndex::new(def);
        let d1: Value = serde_json::from_str(r#"{"meta":{"tags":["Red","blue"]},"n":3,"v":[1,0]}"#)
            .expect("json");
        let d2: Value =
            serde_json::from_str(r#"{"meta":{"tags":"green"},"n":4,"v":[0,1]}"#).expect("json");
        ix.put(b"j:1", Source::Json(&d1), None).expect("indexed");
        ix.put(b"j:2", Source::Json(&d2), None).expect("indexed");
        let h = search(
            &ix,
            &["FT.SEARCH", "j", "(@tags:{red})=>[KNN 5 @v $v]"],
            Some(&[1.0, 0.0]),
        );
        assert_eq!(keys(&h), ["j:1"]);
        assert!(
            h.keys[0].1.expect("distance").abs() < 1e-5,
            "cosine distance 0"
        );
        let bad: Value = serde_json::from_str(r#"{"v":[1,"x"]}"#).expect("json");
        assert!(ix.put(b"j:3", Source::Json(&bad), None).is_err());
        // A HASH where the index reads JSON is not a document of it.
        ix.put(b"j:2", Source::Hash(&[]), None).expect("skipped");
        assert_eq!(ix.len(), 1);
    }

    /// RediSearch splits a JSON TAG's strings only on a declared separator,
    /// and then each element of an array too. LangChain declares `|` and
    /// writes `"a|b"`.
    #[test]
    fn a_json_tag_is_split_only_on_a_declared_separator() {
        let index = |sep: Option<&str>| {
            let mut args = vec![
                "FT.CREATE",
                "j",
                "ON",
                "JSON",
                "PREFIX",
                "1",
                "j:",
                "SCHEMA",
                "$.c",
                "AS",
                "c",
                "TAG",
            ];
            if let Some(s) = sep {
                args.extend(["SEPARATOR", s]);
            }
            args.extend([
                "$.v",
                "AS",
                "v",
                "VECTOR",
                "FLAT",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "2",
                "DISTANCE_METRIC",
                "L2",
            ]);
            let (_, def) = parse_create(&a(&args)).expect("parses");
            let mut ix = FtIndex::new(def);
            for (k, d) in [
                (b"j:1", r#"{"c":"a|b","v":[1,0]}"#),
                (b"j:2", r#"{"c":["x|Y","z"],"v":[0,1]}"#),
                (b"j:3", r#"{"c":"a,b","v":[1,1]}"#),
            ] {
                let d: Value = serde_json::from_str(d).expect("json");
                ix.put(k, Source::Json(&d), None).expect("indexed");
            }
            ix
        };
        let hits = |ix: &FtIndex, tag: &str| {
            let q = format!("(@c:{{{tag}}})=>[KNN 5 @v $v]");
            let h = search(ix, &["FT.SEARCH", "j", &q], Some(&[1.0, 0.0]));
            let mut k = keys(&h);
            k.sort();
            k
        };
        let piped = index(Some("|"));
        assert_eq!(piped.def.attrs[0].separator, b'|');
        assert_eq!(hits(&piped, "a"), ["j:1"]);
        assert_eq!(hits(&piped, "y"), ["j:2"]);
        assert_eq!(hits(&piped, "z"), ["j:2"]);
        let whole = index(None);
        assert_eq!(whole.def.attrs[0].separator, 0, "JSON's default is none");
        assert!(hits(&whole, "a").is_empty());
        assert_eq!(hits(&whole, "z"), ["j:2"]);
        // A `|` in a query's braces is an OR, so the whole strings are
        // read here rather than searched for.
        let tags = |sep: u8, d: &str| {
            let d: Value = serde_json::from_str(d).expect("json");
            json_val(FieldKind::FoldedTag, sep, Some(&d))
        };
        let t = |xs: &[&str]| Val::Tags(xs.iter().map(|x| x.as_bytes().into()).collect());
        assert_eq!(tags(0, r#""a|B""#), t(&["a|b"]));
        assert_eq!(tags(0, r#"["x|y", " z "]"#), t(&[" z ", "x|y"]));
        assert_eq!(tags(b'|', r#""a,b""#), t(&["a,b"]));
        assert_eq!(tags(b',', r#"["a, b", "c"]"#), t(&["a", "b", "c"]));
    }
}
