// SPDX-License-Identifier: Elastic-2.0
//! One `FT.*` index's state in the co-processor (ADR-0060): the documents
//! under its prefixes, as the values its attributes index, and for an index
//! with a VECTOR field a vector set over the same keys. The documents
//! themselves stay in the tenant's keys; this holds only what a search needs
//! to choose them, and is rebuilt from those keys.

use crate::filter::{self, Field, FieldKind, Val, Vals};
use crate::ft::{IndexDef, On, Search, VectorField, distance, vector_from_bytes};
use crate::ftquery::{self, Docs};
use crate::fulltext::{self, DocId, StopWords, TextField, TextIndex};
use crate::{Quant, VectorSet, hnsw};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

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
    /// assigns at each write, order them (amendment 3, part 6). Its id in
    /// the text index too (ADR-0063).
    seq: u64,
    /// Its score: `SCORE_FIELD`'s, else the index's `SCORE`, kept as
    /// RediSearch keeps it, a float.
    score: f32,
}

/// What a document costs the D4 meter beyond its key, twice, and values:
/// the `Doc`, its key's box and its slot in the map, and its id's entry in
/// `by_seq`.
const DOC_BYTES: usize = 200;

/// What a TAG value's document count costs the D4 meter beyond its bytes.
const TAG_COUNT_BYTES: usize = 64;

/// A document as read for the index: its values, its vector, its TEXT
/// values by attribute, and its score.
struct Read {
    vals: Option<Vals>,
    vector: Result<Option<Vec<f32>>, String>,
    texts: Vec<(usize, Vec<u8>)>,
    score: f32,
}

/// How many more neighbours a KNN asks its set for than it answers, so that
/// documents tied at its k-th distance are chosen by write order rather than
/// by however the set found them.
const TIE_SLACK: usize = 16;

/// The most documents a vector range query counts (ADR-0061): a range of
/// more reports this many, its pages exact up to it. The page ceiling of
/// ADR-0060 amendment 3, part 1.
const RANGE_CAP: usize = 10_000;

pub struct FtIndex {
    def: IndexDef,
    fields: Box<[Field]>,
    docs: HashMap<Vec<u8>, Doc>,
    /// Over the documents that hold a vector, keyed as `docs` is.
    set: Option<VectorSet>,
    /// Documents left out since the index was made, as RediSearch counts
    /// `hash_indexing_failures`: a vector that is not `DIM` float32s, or a
    /// GEO value that is not a point a geohash holds (ADR-0062).
    failures: u64,
    /// Whether it holds every document under its prefixes: false while
    /// `FT.CREATE`'s backfill or a rebuild is still reading them, when a
    /// search is not answered (a partial answer is a wrong one).
    ready: bool,
    /// The next document write's `seq`.
    next_seq: u64,
    /// The TEXT fields' words (ADR-0063), by document id.
    text: TextIndex,
    /// Each TEXT attribute that is indexed, by its place in the definition.
    text_fields: HashMap<usize, TextField>,
    stop: StopWords,
    /// The TEXT attributes declared `NOSTEM`, as a field mask.
    nostem: u32,
    /// Each document's key, by id: write order.
    by_seq: BTreeMap<DocId, Vec<u8>>,
    /// How many documents hold each TAG value, by attribute: a TAG term's
    /// IDF, as RediSearch scores one.
    tags: Vec<HashMap<Box<[u8]>, u32>>,
    tag_bytes: usize,
}

/// Keys with their distance from a KNN's vector, nearest first.
type Ranked = Vec<(Vec<u8>, Option<f32>)>;

/// One search's answer before the documents are read: how many matched, and
/// the page asked for, each key with its distance when the search was a KNN.
#[derive(Debug, PartialEq)]
pub struct Hits {
    pub total: usize,
    pub keys: Vec<(Vec<u8>, Option<f32>)>,
    /// Each key's score under the search's scorer (ADR-0063), for
    /// `WITHSCORES`; 0 for a KNN's, which is refused it.
    pub scores: Vec<f64>,
}

impl FtIndex {
    pub fn new(def: IndexDef) -> Self {
        let fields: Box<[Field]> = def.fields().into_boxed_slice();
        let set = def.vector.as_ref().map(|v| {
            VectorSet::new(v.dim, v.metric, v.index, Quant::None).with_fields(fields.to_vec())
        });
        let mut text_fields = HashMap::new();
        let mut nostem = 0u32;
        for (at, a) in def.attrs.iter().enumerate() {
            if a.field.kind != FieldKind::Text || a.noindex {
                continue;
            }
            let stem = !a.flags.iter().any(|f| &**f == "NOSTEM");
            if !stem {
                nostem |= 1 << at;
            }
            let weight = a
                .weight
                .as_deref()
                .and_then(|w| w.parse::<f64>().ok())
                .unwrap_or(1.0) as f32;
            text_fields.insert(
                at,
                TextField {
                    bit: at as u32,
                    weight,
                    stem,
                },
            );
        }
        let stop = StopWords::of(def.stopwords.as_deref());
        let tags = vec![HashMap::new(); def.attrs.len()];
        FtIndex {
            def,
            fields,
            docs: HashMap::new(),
            set,
            failures: 0,
            ready: false,
            next_seq: 0,
            text: TextIndex::default(),
            text_fields,
            stop,
            nostem,
            by_seq: BTreeMap::new(),
            tags,
            tag_bytes: 0,
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
        let read = match (self.def.on, src) {
            (On::Hash, Source::Hash(fields)) => self.read_hash(fields),
            (On::Json, Source::Json(doc)) => self.read_json(doc),
            _ => {
                self.remove(key);
                return Ok(());
            }
        };
        let Read {
            vals,
            vector,
            texts,
            score,
        } = read;
        let vector = match vector {
            Ok(v) => v,
            Err(e) => {
                self.remove(key);
                self.failures += 1;
                return Err(e);
            }
        };
        // Its earlier write's words and tags go; the vector set replaces its
        // own entry below.
        self.unindex(key);
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
        let words: Vec<(&TextField, &[u8])> = texts
            .iter()
            .filter_map(|(at, t)| Some((self.text_fields.get(at)?, t.as_slice())))
            .collect();
        self.text.put(seq, fulltext::analyze(&words, &self.stop));
        if let Some(v) = &vals {
            for (f, val) in v.iter().enumerate() {
                if let Val::Tags(ts) = val {
                    for t in ts.iter() {
                        let n = self.tags[f].entry(t.clone()).or_insert_with(|| {
                            self.tag_bytes += TAG_COUNT_BYTES + t.len();
                            0
                        });
                        *n += 1;
                    }
                }
            }
        }
        self.by_seq.insert(seq, key.to_vec());
        self.docs.insert(
            key.to_vec(),
            Doc {
                vals,
                expires_at,
                seq,
                score,
            },
        );
        Ok(())
    }

    /// Take `key`'s document out of everything but the vector set: the
    /// document map, its id, its words and its tags' counts.
    fn unindex(&mut self, key: &[u8]) -> bool {
        let Some(d) = self.docs.remove(key) else {
            return false;
        };
        self.by_seq.remove(&d.seq);
        self.text.remove(d.seq);
        if let Some(v) = &d.vals {
            for (f, val) in v.iter().enumerate() {
                if let Val::Tags(ts) = val {
                    for t in ts.iter() {
                        if let Some(n) = self.tags[f].get_mut(t) {
                            *n -= 1;
                            if *n == 0 {
                                self.tags[f].remove(t);
                                self.tag_bytes -= TAG_COUNT_BYTES + t.len();
                            }
                        }
                    }
                }
            }
        }
        true
    }

    /// Whether `key` is one of the index's documents.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.docs.contains_key(key)
    }

    /// Drop `key` from the index; whether it was there.
    pub fn remove(&mut self, key: &[u8]) -> bool {
        if let Some(set) = &mut self.set {
            set.del(key);
        }
        self.unindex(key)
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
            + 2 * key.len()
            + filter::vals_bytes(d.vals.as_deref())
            + self
                .set
                .as_ref()
                .and_then(|s| s.entry_cost(key))
                .unwrap_or(0)
    }

    /// What the index holds beyond each document's own charge: the vector
    /// set's deleted HNSW slots, which wait for its next insert (BUG-0257);
    /// the TEXT fields' words and postings, whose terms documents share
    /// (ADR-0063); and the TAG values' document counts.
    pub fn held(&self) -> usize {
        self.set.as_ref().map_or(0, |s| s.held()) + self.text.bytes() + self.tag_bytes
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

    /// The documents `s` asks for, as of `now`: the `k` nearest the query's
    /// matches for a KNN; else every match, scored by `s.scorer` and ordered
    /// as RediSearch orders them, highest score first (ADR-0063); then
    /// sorted by `SORTBY` if asked, then paged.
    pub fn search(&self, s: &Search, now: u64) -> Result<Hits, String> {
        let view = View { ix: self, now };
        let matches =
            || ftquery::matches(s.query.as_ref(), &self.text, &view, self.nostem, s.scorer);
        let mut hits: Vec<(Vec<u8>, Option<f32>, f64)> = match (&s.knn, &self.set, &self.def.vector)
        {
            (Some(knn), Some(set), Some(v)) => {
                // A query with a text term is matched first, from the
                // postings; the KNN then searches among its matches.
                let allowed: Option<HashSet<&[u8]>> = s.has_text().then(|| {
                    matches()
                        .iter()
                        .filter_map(|(id, _)| self.by_seq.get(id).map(Vec::as_slice))
                        .collect()
                });
                let expired = set.expired_count(now);
                let ef = knn.ef.or(v.ef_runtime).unwrap_or(hnsw::EF_SEARCH_DEFAULT);
                // The `k` nearest live matches, nearest first, a tie to the
                // earlier write.
                let nearest = |k: usize| -> Result<Ranked, String> {
                    let ask = k.saturating_add(expired).saturating_add(TIE_SLACK);
                    let found = match &allowed {
                        Some(a) => set.search_among(&knn.vector, ask, ef, ask, a)?,
                        None => set.search(&knn.vector, ask, ef, ask, s.filter.as_ref(), None)?,
                    };
                    let mut near: Ranked = found
                        .into_iter()
                        .filter(|(id, _)| !set.is_expired(id, now))
                        .map(|(id, score)| (id, Some(distance(v.metric, score))))
                        .collect();
                    near.sort_by(|a, b| {
                        a.1.partial_cmp(&b.1)
                            .unwrap_or(Ordering::Equal)
                            .then_with(|| self.seq(&a.0).cmp(&self.seq(&b.0)))
                    });
                    near.truncate(k);
                    Ok(near)
                };
                let near = match knn.range {
                    None => nearest(knn.k)?,
                    // ADR-0061 C: a range is a KNN cut at its radius. It is
                    // asked for the page's end, then for twice as many
                    // until its farthest is past the radius, every vector is
                    // in, or the ceiling is reached; the total is what lies
                    // within it.
                    Some(radius) => {
                        let mut k = s.offset.saturating_add(s.num).clamp(1, RANGE_CAP);
                        loop {
                            let mut near = nearest(k)?;
                            let got = near.len();
                            near.retain(|(_, d)| d.is_some_and(|d| d <= radius));
                            if near.len() < got || got < k || k >= RANGE_CAP {
                                break near;
                            }
                            k = k.saturating_mul(2).min(RANGE_CAP);
                        }
                    }
                };
                near.into_iter().map(|(k, d)| (k, d, 0.0)).collect()
            }
            (Some(_), _, _) => return Err("this index has no VECTOR field".into()),
            (None, _, _) => {
                let mut m = matches();
                // Highest score first, a tie to the earlier write, as
                // RediSearch's sorter orders a search with no SORTBY;
                // matches come in write order for a SORTBY to sort.
                if s.sortby.is_none() {
                    m.sort_by(ftquery::by_score);
                }
                m.into_iter()
                    .filter_map(|(id, sc)| Some((self.by_seq.get(&id)?.clone(), None, sc)))
                    .collect()
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
        let page: Vec<(Vec<u8>, Option<f32>, f64)> =
            hits.into_iter().skip(s.offset).take(s.num).collect();
        Ok(Hits {
            total,
            scores: page.iter().map(|(_, _, sc)| *sc).collect(),
            keys: page.into_iter().map(|(k, d, _)| (k, d)).collect(),
        })
    }

    /// The index's score for a document that gives none.
    fn default_score(&self) -> f32 {
        self.def.score as f32
    }

    fn read_hash(&self, fields: &[(Vec<u8>, Vec<u8>)]) -> Read {
        let get = |name: &str| {
            fields
                .iter()
                .rev()
                .find(|(f, _)| f == name.as_bytes())
                .map(|(_, v)| v.as_slice())
        };
        // A GEO value that is not a point leaves the document out, as a bad
        // vector does and as RediSearch's indexer does (ADR-0062).
        let mut bad: Option<String> = None;
        let mut texts = Vec::new();
        let vals: Vals = self
            .def
            .attrs
            .iter()
            .enumerate()
            .map(|(at, a)| match get(&a.path) {
                Some(raw) if a.field.kind == FieldKind::Geo => {
                    geo_or_note(&a.field.name, std::iter::once(raw), &mut bad)
                }
                // Its words go to the text index (ADR-0063).
                Some(raw) if a.field.kind == FieldKind::Text => {
                    texts.push((at, raw.to_vec()));
                    Val::Missing
                }
                Some(raw) => filter::hash_val(a.field.kind, raw, a.separator),
                None => Val::Missing,
            })
            .collect();
        // A score that is not a number is the default, as RediSearch's
        // `SchemaRule_HashScore` takes it.
        let score = self
            .def
            .score_field
            .as_deref()
            .and_then(get)
            .and_then(|v| std::str::from_utf8(v).ok())
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| !v.is_nan())
            .map_or(self.default_score(), |v| v as f32);
        if let Some(e) = bad {
            return Read {
                vals: any(vals),
                vector: Err(e),
                texts,
                score,
            };
        }
        let vector = match &self.def.vector {
            None => Ok(None),
            Some(v) => match get(&v.path) {
                None => Ok(None),
                Some(raw) => vector_from_bytes(raw, v.dim)
                    .map(Some)
                    .map_err(|e| format!("{} is not indexed: {e}", v.name)),
            },
        };
        Read {
            vals: any(vals),
            vector,
            texts,
            score,
        }
    }

    fn read_json(&self, doc: &Value) -> Read {
        let mut bad: Option<String> = None;
        let mut texts = Vec::new();
        let vals: Vals = self
            .def
            .attrs
            .iter()
            .enumerate()
            .map(|(at, a)| match (a.field.kind, json_at(doc, &a.path)) {
                // A string, or an array of them, any of which may match.
                (FieldKind::Geo, Some(Value::String(s))) => {
                    geo_or_note(&a.field.name, std::iter::once(s.as_bytes()), &mut bad)
                }
                (FieldKind::Geo, Some(Value::Array(xs))) => geo_or_note(
                    &a.field.name,
                    xs.iter().filter_map(|x| x.as_str()).map(str::as_bytes),
                    &mut bad,
                ),
                (FieldKind::Text, Some(Value::String(s))) => {
                    texts.push((at, s.as_bytes().to_vec()));
                    Val::Missing
                }
                (kind, v) => json_val(kind, a.separator, v),
            })
            .collect();
        let score = self
            .def
            .score_field
            .as_deref()
            .and_then(|p| json_at(doc, p))
            .and_then(Value::as_f64)
            .map_or(self.default_score(), |v| v as f32);
        if let Some(e) = bad {
            return Read {
                vals: any(vals),
                vector: Err(e),
                texts,
                score,
            };
        }
        let vector = match &self.def.vector {
            None => Ok(None),
            Some(v) => json_vector(v, json_at(doc, &v.path)),
        };
        Read {
            vals: any(vals),
            vector,
            texts,
            score,
        }
    }
}

/// The index's documents, as a search as of `now` reads them.
struct View<'a> {
    ix: &'a FtIndex,
    now: u64,
}

impl View<'_> {
    fn doc(&self, id: DocId) -> Option<&Doc> {
        self.ix.docs.get(self.ix.by_seq.get(&id)?)
    }
}

impl Docs for View<'_> {
    fn count(&self) -> usize {
        self.ix.docs.len()
    }
    fn ids(&self) -> Box<dyn Iterator<Item = DocId> + '_> {
        Box::new(
            self.ix
                .by_seq
                .iter()
                .filter(|(_, k)| {
                    self.ix
                        .docs
                        .get(*k)
                        .is_some_and(|d| self.ix.live(d, self.now))
                })
                .map(|(id, _)| *id),
        )
    }
    fn live(&self, id: DocId) -> bool {
        self.doc(id).is_some_and(|d| self.ix.live(d, self.now))
    }
    fn vals(&self, id: DocId) -> Option<&[Val]> {
        self.doc(id)?.vals.as_deref()
    }
    fn score(&self, id: DocId) -> f32 {
        self.doc(id).map_or(1.0, |d| d.score)
    }
    fn tag_docs(&self, field: usize, tag: &[u8]) -> usize {
        self.ix
            .tags
            .get(field)
            .and_then(|m| m.get(tag))
            .map_or(0, |n| *n as usize)
    }
}

/// A GEO field's points, or [`Val::Missing`] with the first error noted in
/// `bad`, which leaves the document out.
fn geo_or_note<'a>(
    name: &str,
    raws: impl Iterator<Item = &'a [u8]>,
    bad: &mut Option<String>,
) -> Val {
    match filter::geo_val(raws) {
        Ok(v) => v,
        Err(e) => {
            bad.get_or_insert_with(|| format!("{name} is not indexed: {e}"));
            Val::Missing
        }
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
            // The words themselves, as the extraction reads an array back:
            // joined into one string, a word holding an escaped separator
            // would be split again (BUG-0272).
            Val::Text(ws) => Value::Array(
                ws.iter()
                    .map(|w| Value::String(String::from_utf8_lossy(w).into_owned()))
                    .collect(),
            ),
            // The hashes themselves: the set's extraction reads them back.
            Val::Geo(hs) => Value::Array(hs.iter().map(|h| Value::from(*h)).collect()),
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
            Val::Text(_) | Val::Geo(_) | Val::Missing => None,
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

    /// Floats in [-1, 1), from a fixed sequence per `seed`.
    fn floats(seed: u64) -> impl FnMut() -> f32 {
        let mut seed = seed;
        move || {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((seed >> 33) as f32 / (1u64 << 31) as f32) * 2.0 - 1.0
        }
    }

    /// A `kind` (FLAT or HNSW) index over a `t` TAG and a `dim`-d `v` of
    /// `metric`, with `n` vectors from a fixed sequence, each value in
    /// [-scale, scale) and every third vector tagged `a`; and the vectors, the
    /// i-th under `d:<i>`.
    fn range_index(
        kind: &str,
        metric: &str,
        n: usize,
        dim: usize,
        scale: f32,
    ) -> (FtIndex, Vec<Vec<f32>>) {
        let dims = dim.to_string();
        let (_, def) = parse_create(&a(&[
            "FT.CREATE",
            "r",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "d:",
            "SCHEMA",
            "t",
            "TAG",
            "v",
            "VECTOR",
            kind,
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            &dims,
            "DISTANCE_METRIC",
            metric,
        ]))
        .expect("parses");
        let mut ix = FtIndex::new(def);
        let mut rnd = floats(7);
        let mut vecs = Vec::with_capacity(n);
        for i in 0..n {
            let v: Vec<f32> = (0..dim).map(|_| rnd() * scale).collect();
            let t = if i.is_multiple_of(3) { "a" } else { "b" };
            let fields = vec![
                (b"t".to_vec(), t.as_bytes().to_vec()),
                (b"v".to_vec(), blob(&v)),
            ];
            ix.put(format!("d:{i:05}").as_bytes(), Source::Hash(&fields), None)
                .expect("indexed");
            vecs.push(v);
        }
        (ix, vecs)
    }

    /// RediSearch's distance of `v` from `q`, by brute force: squared L2,
    /// `1 - dot`, or `1 - cos`.
    fn measure(metric: &str, q: &[f32], v: &[f32]) -> f32 {
        let dot: f32 = q.iter().zip(v).map(|(a, b)| a * b).sum();
        let norm = |x: &[f32]| x.iter().map(|a| a * a).sum::<f32>().sqrt();
        match metric {
            "L2" => q.iter().zip(v).map(|(a, b)| (a - b) * (a - b)).sum(),
            "IP" => 1.0 - dot,
            _ => 1.0 - dot / (norm(q) * norm(v)),
        }
    }

    /// The index `i` of a `d:<i>` key.
    fn doc_no(k: &[u8]) -> usize {
        std::str::from_utf8(&k[2..])
            .ok()
            .and_then(|i| i.parse().ok())
            .expect("d:<i>")
    }

    /// ADR-0061 C, its verification: on FLAT a range is every document within
    /// the radius by a brute-force measure, nearest first, its total
    /// included, over 10,000 vectors, for each metric, from no document in
    /// range to all of them, with and without a filter, and paged. A document
    /// within 1e-4 of the radius may fall either side, as the two ways of
    /// summing its distance round.
    #[test]
    fn a_vector_range_on_flat_is_every_document_within_its_radius() {
        let q = [0.3f32, -0.2, 0.5];
        for metric in ["L2", "IP", "COSINE"] {
            let (ix, vecs) = range_index("FLAT", metric, RANGE_CAP, 3, 1.0);
            let measured: Vec<f32> = vecs.iter().map(|v| measure(metric, &q, v)).collect();
            for filter in ["", " (@t:{a})"] {
                let matches = |i: usize| filter.is_empty() || i.is_multiple_of(3);
                for radius in [0.0f32, 0.05, 0.3, 1.0, 100.0] {
                    let what = format!("{metric} radius {radius}{filter}");
                    let query =
                        format!("@v:[VECTOR_RANGE {radius} $v]=>{{$YIELD_DISTANCE_AS: d}}{filter}");
                    let got = search(
                        &ix,
                        &[
                            "FT.SEARCH",
                            "r",
                            &query,
                            "SORTBY",
                            "d",
                            "LIMIT",
                            "0",
                            "10000",
                        ],
                        Some(&q),
                    );
                    let mut found = vec![false; vecs.len()];
                    for (k, d) in &got.keys {
                        let i = doc_no(k);
                        let d = d.expect("a range answers its distance");
                        assert!(matches(i), "{what}: d:{i} is not a match");
                        assert!((d - measured[i]).abs() <= 1e-4, "{what}: d:{i} at {d}");
                        assert!(measured[i] <= radius + 1e-4, "{what}: d:{i} is past it");
                        found[i] = true;
                    }
                    for (i, &d) in measured.iter().enumerate() {
                        if matches(i) && d <= radius - 1e-4 {
                            assert!(found[i], "{what}: d:{i} at {d} is missing");
                        }
                    }
                    assert_eq!(got.total, got.keys.len(), "{what}: total");
                    assert!(
                        got.keys.windows(2).all(|w| w[0].1 <= w[1].1),
                        "{what}: nearest first"
                    );
                    // A page smaller than the range still counts all of it,
                    // by doubling, and is the range's own page.
                    let page = search(
                        &ix,
                        &["FT.SEARCH", "r", &query, "SORTBY", "d", "LIMIT", "1", "2"],
                        Some(&q),
                    );
                    assert_eq!(page.total, got.total, "{what}: paged total");
                    let want: Vec<String> = keys(&got).into_iter().skip(1).take(2).collect();
                    assert_eq!(keys(&page), want, "{what}: page");
                }
            }
        }
    }

    /// ADR-0061 C, its verification on HNSW: the recall of a range against
    /// the brute-force answer, at the default EF_RUNTIME and RediSearch's
    /// default page of 10, beside the recall of a KNN of as many as the range
    /// holds, over the same 2,000 vectors. A range is cut from a KNN of at
    /// least that many, so it recalls at least as well. The values are small
    /// enough that `1 - dot` stays positive, as a radius must be.
    #[test]
    fn a_vector_range_on_hnsw_recalls_as_its_knn_does() {
        let (n, dim, scale) = (2000, 16, 0.25);
        for (metric, floor) in [("L2", 0.90), ("COSINE", 0.90), ("IP", 0.80)] {
            let (ix, vecs) = range_index("HNSW", metric, n, dim, scale);
            let mut rnd = floats(11);
            let (mut truths, mut in_range, mut in_knn, mut page_hits, mut page_truths) =
                (0usize, 0usize, 0usize, 0usize, 0usize);
            for _ in 0..20 {
                let q: Vec<f32> = (0..dim).map(|_| rnd() * scale).collect();
                let mut by: Vec<(f32, usize)> = vecs
                    .iter()
                    .enumerate()
                    .map(|(i, v)| (measure(metric, &q, v), i))
                    .collect();
                by.sort_by(|a, b| a.0.total_cmp(&b.0));
                for size in [10, 50, 200] {
                    // Between the size-th nearest and the next, so that no
                    // document sits on it.
                    let radius = (by[size - 1].0 + by[size].0) / 2.0;
                    assert!(radius >= 0.0, "{metric}: radius {radius}");
                    let truth: Vec<usize> = by
                        .iter()
                        .take_while(|(d, _)| *d <= radius)
                        .map(|(_, i)| *i)
                        .collect();
                    let query = format!("@v:[VECTOR_RANGE {radius} $v]=>{{$YIELD_DISTANCE_AS: d}}");
                    let got = search(
                        &ix,
                        &["FT.SEARCH", "r", &query, "SORTBY", "d"],
                        Some(q.as_slice()),
                    );
                    // Every one it holds is within the radius, so its total
                    // is the range's recall.
                    truths += truth.len();
                    in_range += got.total;
                    let top: Vec<usize> = truth.iter().take(10).copied().collect();
                    page_truths += top.len();
                    page_hits += got
                        .keys
                        .iter()
                        .filter(|(k, _)| top.contains(&doc_no(k)))
                        .count();
                    let kq = format!("*=>[KNN {} @v $v AS d]", truth.len());
                    let lim = truth.len().to_string();
                    let knn = search(
                        &ix,
                        &["FT.SEARCH", "r", &kq, "LIMIT", "0", &lim],
                        Some(q.as_slice()),
                    );
                    in_knn += knn
                        .keys
                        .iter()
                        .filter(|(k, _)| truth.contains(&doc_no(k)))
                        .count();
                }
            }
            let (range, knn, page) = (
                in_range as f64 / truths as f64,
                in_knn as f64 / truths as f64,
                page_hits as f64 / page_truths as f64,
            );
            eprintln!("{metric}: range recall {range:.3}, its first page {page:.3}, KNN {knn:.3}");
            assert!(
                range >= floor && page >= floor,
                "{metric}: range {range}, page {page}"
            );
            assert!(
                range + 0.01 >= knn,
                "{metric}: range {range} below its KNN {knn}"
            );
        }
    }

    /// ADR-0061 C: a range of more than 10,000 documents reports 10,000, and
    /// its first page is still the nearest.
    #[test]
    fn a_vector_range_counts_to_its_ceiling() {
        let (ix, _) = range_index("FLAT", "L2", RANGE_CAP + 5, 2, 1.0);
        let q = [0.0f32, 0.0];
        let got = search(
            &ix,
            &[
                "FT.SEARCH",
                "r",
                "@v:[VECTOR_RANGE 100 $v]=>{$YIELD_DISTANCE_AS: d}",
                "SORTBY",
                "d",
                "LIMIT",
                "0",
                "3",
            ],
            Some(&q),
        );
        assert_eq!(got.total, RANGE_CAP);
        let knn = search(
            &ix,
            &["FT.SEARCH", "r", "*=>[KNN 3 @v $v AS d]", "SORTBY", "d"],
            Some(&q),
        );
        assert_eq!(keys(&got), keys(&knn));
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

    /// BUG-0272: a filtered KNN reads a TEXT field's words as a filter-only
    /// search does, a word holding an escaped separator included.
    #[test]
    fn a_filtered_knn_reads_text_words_as_the_index_holds_them() {
        let mut ix = hash_index();
        for (key, content, v) in [
            ("doc:1", "tcp\\/ip stack", [0.0, 0.0]),
            ("doc:2", "tcp ip stack", [1.0, 0.0]),
        ] {
            let fields = vec![
                (b"content".to_vec(), content.as_bytes().to_vec()),
                (b"embedding".to_vec(), blob(&v)),
            ];
            ix.put(key.as_bytes(), Source::Hash(&fields), None)
                .expect("indexed");
        }
        let q = "@content:(\"tcp\\/ip\")";
        assert_eq!(
            keys(&search(&ix, &["FT.SEARCH", "idx", q], None)),
            ["doc:1"]
        );
        let knn = format!("({q})=>[KNN 2 @embedding $v AS d]");
        let h = search(
            &ix,
            &["FT.SEARCH", "idx", &knn, "SORTBY", "d"],
            Some(&[0.0, 0.0]),
        );
        assert_eq!(keys(&h), ["doc:1"]);
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

    /// ADR-0062: a GEO field answers a radius as RediSearch measures it.
    /// Over 10,000 random points around a city, for each unit, alone and
    /// ANDed with a tag, the total equals a brute force of RediSearch's
    /// rule: a point whose hash lies in one of the circle's geohash ranges
    /// and whose cell centre is within the radius. Under a KNN on FLAT, the
    /// nearest are taken from those matches. A document whose value is not
    /// a point is left out and counted.
    #[test]
    fn a_geo_field_answers_a_radius_as_redisearch_measures_it() {
        let (_, def) = parse_create(&a(&[
            "FT.CREATE",
            "g",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "p:",
            "SCHEMA",
            "loc",
            "GEO",
            "t",
            "TAG",
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
        ]))
        .expect("parses");
        let mut ix = FtIndex::new(def);
        let mut rnd = floats(5);
        let mut pts = Vec::new();
        for i in 0..10_000 {
            // About 28 km either way of San Francisco.
            let (lon, lat) = (
                -122.4194 + rnd() as f64 * 0.25,
                37.7749 + rnd() as f64 * 0.25,
            );
            let t = if i % 2 == 0 { "a" } else { "b" };
            let fields = vec![
                (b"loc".to_vec(), format!("{lon},{lat}").into_bytes()),
                (b"t".to_vec(), t.as_bytes().to_vec()),
                (b"v".to_vec(), blob(&[lon as f32, lat as f32])),
            ];
            ix.put(format!("p:{i:05}").as_bytes(), Source::Hash(&fields), None)
                .expect("indexed");
            pts.push((lon, lat, crate::geo::hash(lon, lat).expect("a point")));
        }
        // RediSearch's rule, by brute force.
        let rule = |h: u64, lon: f64, lat: f64, r: f64| {
            crate::geo::ranges(lon, lat, r)
                .iter()
                .any(|&(lo, hi)| (lo..hi).contains(&h))
                && crate::geo::within(h, lon, lat, r)
        };
        let (lon, lat) = (-122.4194, 37.7749);
        for (r, unit, metres) in [
            ("750", "m", 750.0),
            ("0.5", "km", 500.0),
            ("2", "KM", 2000.0),
            ("1.5", "mi", 1.5 * 1609.34),
            ("3000", "ft", 3000.0 * 0.3048),
            ("40", "km", 40000.0),
        ] {
            let want: Vec<usize> = (0..pts.len())
                .filter(|&i| rule(pts[i].2, lon, lat, metres))
                .collect();
            let q = format!("@loc:[{lon} {lat} {r} {unit}]");
            let got = search(&ix, &["FT.SEARCH", "g", &q, "LIMIT", "0", "10000"], None);
            assert_eq!(got.total, want.len(), "{q}");
            let tagged = want.iter().filter(|&&i| i % 2 == 0).count();
            let q2 = format!("(@loc:[{lon} {lat} {r} {unit}] @t:{{a}})");
            let got2 = search(&ix, &["FT.SEARCH", "g", &q2, "LIMIT", "0", "10000"], None);
            assert_eq!(got2.total, tagged, "{q2}");
            // The nearest five matches by the vector, from the matches only.
            let v = [lon as f32 + 0.01, lat as f32 - 0.01];
            let mut near: Vec<(f32, String)> = want
                .iter()
                .map(|&i| {
                    let (dx, dy) = (pts[i].0 as f32 - v[0], pts[i].1 as f32 - v[1]);
                    (dx * dx + dy * dy, format!("p:{i:05}"))
                })
                .collect();
            near.sort_by(|a, b| a.0.total_cmp(&b.0));
            let kq = format!("(@loc:[{lon} {lat} {r} {unit}])=>[KNN 5 @v $v AS d]");
            let got3 = search(&ix, &["FT.SEARCH", "g", &kq, "SORTBY", "d"], Some(&v));
            let wanted: Vec<String> = near.into_iter().take(5).map(|(_, k)| k).collect();
            assert_eq!(keys(&got3), wanted, "{kq}");
        }
        let all = search(
            &ix,
            &[
                "FT.SEARCH",
                "g",
                "@loc:[-122.4194 37.7749 40 km]",
                "LIMIT",
                "0",
                "0",
            ],
            None,
        );
        assert_eq!(all.total, 10_000, "40 km holds them all");
        // Not a point, and past the latitude a geohash holds: left out, counted.
        let before = ix.failures();
        for bad in [&b"nowhere"[..], b"0,86", b"-200,10"] {
            let fields = vec![(b"loc".to_vec(), bad.to_vec())];
            assert!(
                ix.put(b"p:bad", Source::Hash(&fields), None).is_err(),
                "{bad:?}"
            );
        }
        assert_eq!(ix.failures(), before + 3);

        // JSON: a string, or an array of them any of which may match.
        let (_, jdef) = parse_create(&a(&[
            "FT.CREATE",
            "j",
            "ON",
            "JSON",
            "PREFIX",
            "1",
            "j:",
            "SCHEMA",
            "$.loc",
            "AS",
            "loc",
            "GEO",
        ]))
        .expect("parses");
        let mut jx = FtIndex::new(jdef);
        let doc = |v: serde_json::Value| serde_json::json!({ "loc": v });
        jx.put(
            b"j:1",
            Source::Json(&doc("13.361389,38.115556".into())),
            None,
        )
        .expect("indexed");
        jx.put(
            b"j:2",
            Source::Json(&doc(serde_json::json!(["0,0", "15.087269 37.502669"]))),
            None,
        )
        .expect("indexed");
        jx.put(b"j:3", Source::Json(&doc("0,0".into())), None)
            .expect("indexed");
        assert!(
            jx.put(b"j:4", Source::Json(&doc("x".into())), None)
                .is_err()
        );
        let got = search(&jx, &["FT.SEARCH", "j", "@loc:[15 37 200 km]"], None);
        assert_eq!(keys(&got), ["j:1", "j:2"]);
        let got = search(&jx, &["FT.SEARCH", "j", "@loc:[15 37 100 km]"], None);
        assert_eq!(keys(&got), ["j:2"], "only Catania is within 100 km");
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

    fn index_of(schema: &[&str]) -> FtIndex {
        let mut parts = vec!["FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:"];
        parts.extend_from_slice(schema);
        let (_, def) = parse_create(&a(&parts)).expect("parses");
        let mut ix = FtIndex::new(def);
        ix.set_ready();
        ix
    }

    fn put(ix: &mut FtIndex, key: &str, fields: &[(&str, &[u8])]) {
        let fields: Vec<(Vec<u8>, Vec<u8>)> = fields
            .iter()
            .map(|(f, v)| (f.as_bytes().to_vec(), v.to_vec()))
            .collect();
        ix.put(key.as_bytes(), Source::Hash(&fields), None)
            .expect("indexed");
    }

    /// ADR-0063: RediSearch's `testScoreIndex` through `FT.CREATE` and
    /// `FT.SEARCH` as a client sends them: `SCORE_FIELD`, a title of `WEIGHT
    /// 10`, each scorer's top five with `WITHSCORES`, to two decimals.
    #[test]
    fn full_text_answers_redisearch_s_own_test_index() {
        let mut ix = index_of(&[
            "SCORE_FIELD",
            "__score",
            "SCHEMA",
            "title",
            "TEXT",
            "WEIGHT",
            "10",
            "body",
            "TEXT",
        ]);
        for n in 1..25usize {
            let sc = ((35.0 - n as f64) / 35.0).sqrt();
            put(
                &mut ix,
                &format!("doc{n}"),
                &[
                    ("__score", format!("{sc:?}").as_bytes()),
                    ("title", "hello world ".repeat(n).as_bytes()),
                    ("body", "lorem ipsum ".repeat(n).as_bytes()),
                ],
            );
        }
        let top = |scorer: &str| -> Vec<(String, f64)> {
            let h = search(
                &ix,
                &[
                    "FT.SEARCH",
                    "idx",
                    "hello world",
                    "SCORER",
                    scorer,
                    "NOCONTENT",
                    "WITHSCORES",
                    "LIMIT",
                    "0",
                    "5",
                ],
                None,
            );
            assert_eq!(h.total, 24, "{scorer}");
            keys(&h)
                .into_iter()
                .zip(h.scores.iter().map(|s| (s * 100.0).round() / 100.0))
                .collect()
        };
        let want = |xs: &[(usize, f64)]| -> Vec<(String, f64)> {
            xs.iter().map(|(n, s)| (format!("doc{n}"), *s)).collect()
        };
        assert_eq!(
            top("TFIDF"),
            want(&[(1, 1.97), (2, 1.94), (3, 1.91), (4, 1.88), (5, 1.85)])
        );
        assert_eq!(
            top("BM25"),
            want(&[(17, 0.73), (18, 0.73), (16, 0.72), (19, 0.72), (15, 0.72)])
        );
        assert_eq!(
            top("BM25STD"),
            want(&[(1, 0.08), (2, 0.08), (3, 0.08), (4, 0.08), (5, 0.08)])
        );
        assert_eq!(
            top("DISMAX"),
            want(&[
                (24, 480.0),
                (23, 460.0),
                (22, 440.0),
                (21, 420.0),
                (20, 400.0)
            ])
        );
        // The default scorer is BM25STD.
        let h = search(
            &ix,
            &["FT.SEARCH", "idx", "hello world", "WITHSCORES"],
            None,
        );
        assert_eq!(keys(&h)[0], "doc1");
        assert!((h.scores[0] - 0.0845).abs() < 1e-3, "{}", h.scores[0]);
    }

    /// ADR-0063: a TAG match is a word's result, so on an index with TEXT
    /// fields a tag or `*` search is ordered by BM25STD, the shortest
    /// documents first; with no TEXT field every score ties and write order
    /// holds.
    #[test]
    fn a_search_with_no_text_word_is_ordered_by_score() {
        let mut ix = index_of(&["SCHEMA", "t", "TEXT", "c", "TAG"]);
        put(
            &mut ix,
            "doc:long",
            &[("t", b"one two three four"), ("c", b"x")],
        );
        put(&mut ix, "doc:short", &[("t", b"one"), ("c", b"x")]);
        put(&mut ix, "doc:mid", &[("t", b"one two"), ("c", b"x")]);
        let q = |query: &str| keys(&search(&ix, &["FT.SEARCH", "idx", query], None));
        assert_eq!(q("@c:{x}"), ["doc:short", "doc:mid", "doc:long"]);
        assert_eq!(q("*"), ["doc:short", "doc:mid", "doc:long"]);
        let mut plain = index_of(&["SCHEMA", "c", "TAG", "n", "NUMERIC"]);
        for k in ["doc:3", "doc:1", "doc:2"] {
            put(&mut plain, k, &[("c", b"x"), ("n", b"1")]);
        }
        let q = |query: &str| keys(&search(&plain, &["FT.SEARCH", "idx", query], None));
        assert_eq!(q("@c:{x}"), ["doc:3", "doc:1", "doc:2"]);
        assert_eq!(q("@n:[0 5]"), ["doc:3", "doc:1", "doc:2"]);
    }

    /// ADR-0063: text terms beside a KNN filter its candidates, as
    /// langchain-redis's `Text("val") % "ba*"` writes them.
    #[test]
    fn a_knn_searches_among_a_text_term_s_matches() {
        let mut ix = hash_index();
        for (k, c, v) in [
            ("doc:foo", "foo", [0.0, 0.0]),
            ("doc:bar", "bar", [3.0, 0.0]),
            ("doc:baz", "baz", [2.0, 0.0]),
            ("doc:qux", "qux", [0.1, 0.0]),
        ] {
            put(
                &mut ix,
                k,
                &[("content", c.as_bytes()), ("embedding", &blob(&v))],
            );
        }
        let knn = |q: &str| {
            let h = search(
                &ix,
                &["FT.SEARCH", "idx", q, "SORTBY", "d"],
                Some(&[0.0, 0.0]),
            );
            keys(&h)
        };
        assert_eq!(
            knn("(@content:(ba*))=>[KNN 3 @embedding $v AS d]"),
            ["doc:baz", "doc:bar"]
        );
        assert_eq!(
            knn("(-@content:foo)=>[KNN 1 @embedding $v AS d]"),
            ["doc:qux"]
        );
        assert_eq!(
            knn("(@content:(the))=>[KNN 3 @embedding $v AS d]"),
            Vec::<String>::new(),
            "a query of stop words matches nothing"
        );
    }

    /// ADR-0063: rewrites, deletes and expiries keep the postings exactly
    /// what an index built afresh from the documents left holds: every
    /// query's matches and scores agree.
    #[test]
    fn postings_follow_every_rewrite_and_delete() {
        let schema = ["SCHEMA", "t", "TEXT", "u", "TEXT", "NOSTEM", "c", "TAG"];
        let words = [
            "running",
            "runs",
            "engine",
            "engineer",
            "engineering",
            "the",
            "doctor",
            "doctors",
            "a",
            "medical",
            "professional",
            "cancer",
            "cancers",
            "lung",
        ];
        let mut x = 0x5eed_u64;
        let mut next = |n: usize| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x % n as u64) as usize
        };
        let mut live = index_of(&schema);
        let mut docs: std::collections::BTreeMap<String, Vec<(&str, String)>> = Default::default();
        for _ in 0..400 {
            let key = format!("doc:{}", next(40));
            if next(5) == 0 {
                live.remove(key.as_bytes());
                docs.remove(&key);
                continue;
            }
            let mut text = |n: usize| -> String {
                (0..1 + next(n))
                    .map(|_| words[next(words.len())])
                    .collect::<Vec<_>>()
                    .join(" ")
            };
            let d = vec![
                ("t", text(8)),
                ("u", text(4)),
                ("c", ["x", "y", "x,y"][next(3)].to_string()),
            ];
            let f: Vec<(&str, &[u8])> = d.iter().map(|(k, v)| (*k, v.as_bytes())).collect();
            put(&mut live, &key, &f);
            docs.insert(key, d);
        }
        let mut fresh = index_of(&schema);
        for (k, d) in &docs {
            let f: Vec<(&str, &[u8])> = d.iter().map(|(k, v)| (*k, v.as_bytes())).collect();
            put(&mut fresh, k, &f);
        }
        let scored = |ix: &FtIndex, q: &str, scorer: &str| -> Vec<(String, u64)> {
            let h = search(
                ix,
                &[
                    "FT.SEARCH",
                    "idx",
                    q,
                    "SCORER",
                    scorer,
                    "WITHSCORES",
                    "LIMIT",
                    "0",
                    "1000",
                ],
                None,
            );
            let mut v: Vec<(String, u64)> = keys(&h)
                .into_iter()
                .zip(h.scores.iter().map(|s| s.to_bits()))
                .collect();
            v.sort();
            v
        };
        for q in [
            "running",
            "@u:running",
            "engine*",
            "%doctr%",
            "*neer*",
            "cancer lung",
            "cancer | doctor",
            "\"medical professional\"",
            "-engineer",
            "~lung doctor",
            "@c:{x} medical",
            "*",
        ] {
            for scorer in ["BM25STD", "TFIDF", "TFIDF.DOCNORM", "BM25", "DISMAX"] {
                assert_eq!(
                    scored(&live, q, scorer),
                    scored(&fresh, q, scorer),
                    "{q} under {scorer}"
                );
            }
        }
        assert_eq!(live.held(), fresh.held(), "the same words and postings");
    }
}
