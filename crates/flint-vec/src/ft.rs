// SPDX-License-Identifier: Elastic-2.0
//! RediSearch's `FT.*` vector surface (ADR-0060), parsed: what `FT.CREATE`
//! and `FT.SEARCH` say, as an index's definition and a search request. The
//! subset `redisvl`, `langchain-redis` and the LlamaIndex Redis store send
//! (ADR-0059's survey); every other option is refused by name, never passed
//! over, so a search is never answered as if it had asked for less.

use crate::filter::{self, Expr, Field, FieldKind};
use crate::ftquery::{self, QNode, Scorer};
use crate::fulltext::StopWords;
use crate::{IndexKind, Metric};
use serde_json::{Value, json};
use std::borrow::Cow;
use std::collections::HashMap;

/// Where an index's documents are: `HASH` keys or JSON ones.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum On {
    Hash,
    Json,
}

/// An index's vector field.
#[derive(Clone, Debug, PartialEq)]
pub struct VectorField {
    /// What a query names it by (`@name`).
    pub name: Box<str>,
    /// Where it is in a document: a HASH field, or a JSON path.
    pub path: Box<str>,
    pub index: IndexKind,
    pub dim: usize,
    pub metric: Metric,
    /// HNSW's `EF_RUNTIME`, a search's default `EF`, when declared.
    pub ef_runtime: Option<usize>,
    /// The engine hints declared and passed over (`M`, `EF_CONSTRUCTION`,
    /// `EPSILON`, `INITIAL_CAP`, `BLOCK_SIZE`), as given, so `FT.INFO`
    /// answers the schema a client made.
    pub hints: Vec<(Box<str>, Box<str>)>,
}

/// A TAG, NUMERIC or TEXT attribute.
#[derive(Clone, Debug, PartialEq)]
pub struct Attr {
    /// Its name, as a query names it, and its kind.
    pub field: Field,
    /// Where it is in a document: a HASH field, or a JSON path.
    pub path: Box<str>,
    /// A TAG's separator: `,` on a HASH index and none (0) on a JSON one,
    /// unless `SEPARATOR` says otherwise, as in RediSearch.
    pub separator: u8,
    /// `NOINDEX`: returned and sortable, but not filtered on.
    pub noindex: bool,
    /// The flags declared that change no answer here (`SORTABLE`, `UNF`,
    /// `NOSTEM`, `WITHSUFFIXTRIE`), in order, and a TEXT field's `WEIGHT`:
    /// kept so `FT.INFO` answers the schema a client made, which `redisvl`
    /// reads back to rebuild it.
    pub flags: Vec<Box<str>>,
    pub weight: Option<Box<str>>,
}

/// What `FT.CREATE` declared.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexDef {
    pub on: On,
    /// The key prefixes whose keys are the index's documents. Never empty:
    /// an index over every key is not served (`PREFIX` is required).
    pub prefixes: Vec<Vec<u8>>,
    /// At most [`filter::MAX_FIELDS`].
    pub attrs: Vec<Attr>,
    /// At most one; an index with none answers filter-only searches.
    pub vector: Option<VectorField>,
    /// `STOPWORDS` as declared, RediSearch's default list when not: left
    /// out of TEXT fields and of queries (ADR-0063).
    pub stopwords: Option<Vec<Box<str>>>,
    /// `SCORE`: a document's score when it gives none, 1 unless declared.
    pub score: f64,
    /// `SCORE_FIELD`: where a document gives its own score.
    pub score_field: Option<Box<str>>,
}

impl IndexDef {
    /// The attributes as the filter parser sees them, in order.
    pub fn fields(&self) -> Vec<Field> {
        self.attrs.iter().map(|a| a.field.clone()).collect()
    }

    /// The definition as the co-processor's index row holds it. JSON, with
    /// the prefixes as arrays of bytes since a key prefix need not be UTF-8.
    pub fn encode(&self) -> String {
        let attrs: Vec<Value> = self
            .attrs
            .iter()
            .map(|a| {
                json!({
                    "name": &*a.field.name,
                    "kind": kind_name(a.field.kind),
                    "path": &*a.path,
                    "sep": a.separator,
                    "noindex": a.noindex,
                    "flags": a.flags.iter().map(|f| &**f).collect::<Vec<_>>(),
                    "weight": a.weight.as_deref(),
                })
            })
            .collect();
        let vector = self.vector.as_ref().map(|v| {
            json!({
                "name": &*v.name,
                "path": &*v.path,
                "index": v.index.as_str(),
                "dim": v.dim,
                "metric": v.metric.as_str(),
                "ef_runtime": v.ef_runtime,
                "hints": v.hints.iter().map(|(k, x)| [&**k, &**x]).collect::<Vec<_>>(),
            })
        });
        json!({
            "on": match self.on { On::Hash => "hash", On::Json => "json" },
            "prefixes": self.prefixes,
            "attrs": attrs,
            "vector": vector,
            "stopwords": self.stopwords,
            "score": self.score,
            "score_field": self.score_field.as_deref(),
        })
        .to_string()
    }

    /// Inverse of [`encode`](Self::encode). `None` for a row this binary
    /// cannot read, such as a kind a later binary adds: the caller leaves
    /// that index out, out loud, rather than serving it with a field missed.
    pub fn decode(s: &str) -> Option<IndexDef> {
        let v: Value = serde_json::from_str(s).ok()?;
        let on = match v.get("on")?.as_str()? {
            "hash" => On::Hash,
            "json" => On::Json,
            _ => return None,
        };
        let prefixes: Vec<Vec<u8>> = serde_json::from_value(v.get("prefixes")?.clone()).ok()?;
        if prefixes.is_empty() || prefixes.iter().any(|p| p.is_empty()) {
            return None;
        }
        let mut attrs = Vec::new();
        for a in v.get("attrs")?.as_array()? {
            let name = a.get("name")?.as_str()?;
            if !filter::valid_name(name.as_bytes()) {
                return None;
            }
            attrs.push(Attr {
                field: Field {
                    name: name.into(),
                    kind: kind_from_name(a.get("kind")?.as_str()?)?,
                },
                path: a.get("path")?.as_str()?.into(),
                separator: u8::try_from(a.get("sep")?.as_u64()?).ok()?,
                noindex: a.get("noindex")?.as_bool()?,
                flags: strings(a.get("flags"))?,
                weight: match a.get("weight") {
                    None | Some(Value::Null) => None,
                    Some(w) => Some(w.as_str()?.into()),
                },
            });
        }
        let vector = match v.get("vector")? {
            Value::Null => None,
            x => Some(VectorField {
                name: x.get("name")?.as_str()?.into(),
                path: x.get("path")?.as_str()?.into(),
                index: IndexKind::parse(x.get("index")?.as_str()?.as_bytes())?,
                dim: usize::try_from(x.get("dim")?.as_u64()?)
                    .ok()
                    .filter(|d| *d > 0)?,
                metric: Metric::parse(x.get("metric")?.as_str()?.as_bytes())?,
                ef_runtime: match x.get("ef_runtime")? {
                    Value::Null => None,
                    n => Some(usize::try_from(n.as_u64()?).ok()?),
                },
                hints: match x.get("hints") {
                    None => Vec::new(),
                    Some(h) => h
                        .as_array()?
                        .iter()
                        .map(|kv| {
                            let kv = kv.as_array()?;
                            Some((kv.first()?.as_str()?.into(), kv.get(1)?.as_str()?.into()))
                        })
                        .collect::<Option<Vec<_>>>()?,
                },
            }),
        };
        let stopwords = match v.get("stopwords") {
            None | Some(Value::Null) => None,
            Some(sw) => Some(strings(Some(sw))?),
        };
        // A row an older binary wrote has neither: its index dropped both
        // when it was made, and scores with the defaults (ADR-0063).
        let score = match v.get("score") {
            None | Some(Value::Null) => 1.0,
            Some(x) => x.as_f64()?,
        };
        let score_field = match v.get("score_field") {
            None | Some(Value::Null) => None,
            Some(f) => Some(f.as_str()?.into()),
        };
        Some(IndexDef {
            on,
            prefixes,
            attrs,
            vector,
            stopwords,
            score,
            score_field,
        })
    }
}

/// A JSON array of strings; absent is empty.
fn strings(v: Option<&Value>) -> Option<Vec<Box<str>>> {
    match v {
        None => Some(Vec::new()),
        Some(v) => v
            .as_array()?
            .iter()
            .map(|x| x.as_str().map(Into::into))
            .collect(),
    }
}

/// An attribute kind's name in an index row. Not [`FieldKind::as_str`],
/// which names a folded tag and a case-sensitive one alike.
fn kind_name(k: FieldKind) -> &'static str {
    match k {
        FieldKind::Tag => "tag",
        FieldKind::FoldedTag => "foldedtag",
        FieldKind::Numeric => "numeric",
        FieldKind::Text => "text",
        FieldKind::Geo => "geo",
    }
}

fn kind_from_name(s: &str) -> Option<FieldKind> {
    Some(match s {
        "tag" => FieldKind::Tag,
        "foldedtag" => FieldKind::FoldedTag,
        "numeric" => FieldKind::Numeric,
        "text" => FieldKind::Text,
        "geo" => FieldKind::Geo,
        _ => return None,
    })
}

/// A `KNN` clause: the `k` nearest documents to a vector, by the index's
/// vector field.
#[derive(Clone, Debug, PartialEq)]
pub struct Knn {
    pub k: usize,
    pub vector: Vec<f32>,
    /// `EF_RUNTIME` in the clause; the field's own when it gives none.
    pub ef: Option<usize>,
    /// The distance's name in the reply (`AS`), `__<field>_score` when none.
    pub alias: Box<str>,
    /// A vector range query's radius (ADR-0061): every document within it,
    /// as a KNN cut at it, `k` being the page's end. `None` for a KNN.
    pub range: Option<f32>,
}

/// What `FT.SEARCH` asked for.
#[derive(Debug, PartialEq)]
pub struct Search {
    /// The query (ADR-0063); `None` when every word in it is a stop word,
    /// which matches no document.
    pub query: Option<QNode>,
    /// The query as ADR-0059's filter, when it has no text term and is not
    /// `*`: what a filtered KNN hands its vector set.
    pub filter: Option<Expr>,
    pub knn: Option<Knn>,
    pub nocontent: bool,
    /// `RETURN`: `(field, the name to answer it as)`. `None` returns every
    /// field.
    pub returns: Option<Vec<(Box<str>, Box<str>)>>,
    /// `SORTBY`: the KNN distance's name or a NUMERIC, TAG or TEXT attribute's,
    /// and whether ascending.
    pub sortby: Option<(Box<str>, bool)>,
    pub offset: usize,
    pub num: usize,
    /// `SCORER`: `BM25STD` unless named.
    pub scorer: Scorer,
    /// `WITHSCORES`: each result's score after its key.
    pub withscores: bool,
}

impl Search {
    /// Whether the query has a text term, which only the `FT.*` index's
    /// postings can answer: a filtered KNN then searches among its matches.
    pub fn has_text(&self) -> bool {
        self.query.as_ref().is_none_or(|q| q.as_filter().is_none())
    }
}

/// A distance as RediSearch reports one, from this crate's score, where
/// higher is nearer: `1 - cos` for COSINE, `1 - dot` for IP, and the squared
/// distance for L2, as RediSearch's own engine reports them.
pub fn distance(metric: Metric, score: f32) -> f32 {
    match metric {
        // A cosine distance is at least 0; a vector's own, computed, can
        // come out a rounding error below it.
        Metric::Cosine => (1.0 - score).max(0.0),
        Metric::Ip => 1.0 - score,
        Metric::L2 => -score,
    }
}

fn upper(a: &[u8]) -> Vec<u8> {
    a.to_ascii_uppercase()
}

fn text(a: &[u8]) -> Result<&str, String> {
    std::str::from_utf8(a).map_err(|_| "an argument is not UTF-8".to_string())
}

fn number(a: Option<&[u8]>, what: &str) -> Result<usize, String> {
    a.and_then(|a| std::str::from_utf8(a).ok())
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(|| format!("{what} needs a non-negative integer"))
}

fn arg(args: &[Vec<u8>], i: usize) -> Option<&[u8]> {
    args.get(i).map(|a| a.as_slice())
}

/// A JSON path this index can read: `$.a` or `$.a.b`, member names only.
fn valid_json_path(p: &str) -> bool {
    p.strip_prefix("$.").is_some_and(|rest| {
        rest.split('.')
            .all(|m| !m.is_empty() && m.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_'))
    })
}

/// `FT.CREATE <index> [ON HASH|JSON] PREFIX <n> <prefix>... [STOPWORDS <n>
/// <word>...] SCHEMA <field> [AS <name>] <type> [<options>] ...`. Returns the
/// index's name and definition. The error is the text after `ERR `.
pub fn parse_create(args: &[Vec<u8>]) -> Result<(Vec<u8>, IndexDef), String> {
    let index = arg(args, 1)
        .filter(|n| !n.is_empty() && !n.contains(&0))
        .ok_or("FT.CREATE <index> [ON HASH|JSON] PREFIX <n> <prefix>... SCHEMA ...")?
        .to_vec();
    let mut on = On::Hash;
    let mut prefixes: Vec<Vec<u8>> = Vec::new();
    let mut stopwords: Option<Vec<Box<str>>> = None;
    let mut score = 1.0;
    let mut score_field: Option<Box<str>> = None;
    let mut i = 2;
    loop {
        let Some(opt) = arg(args, i) else {
            return Err("FT.CREATE needs a SCHEMA".into());
        };
        match upper(opt).as_slice() {
            b"ON" => {
                on = match arg(args, i + 1).map(upper).as_deref() {
                    Some(b"HASH") => On::Hash,
                    Some(b"JSON") => On::Json,
                    _ => return Err("FT.CREATE ON needs HASH or JSON".into()),
                };
                i += 2;
            }
            b"PREFIX" => {
                let n = number(arg(args, i + 1), "FT.CREATE PREFIX")?;
                if n == 0 || args.len() < i + 2 + n {
                    return Err("FT.CREATE PREFIX <n> needs n prefixes".into());
                }
                for p in &args[i + 2..i + 2 + n] {
                    if p.is_empty() {
                        return Err("FT.CREATE PREFIX needs a non-empty prefix: \
                                    an index over every key is not served"
                            .into());
                    }
                    if !prefixes.contains(p) {
                        prefixes.push(p.clone());
                    }
                }
                i += 2 + n;
            }
            // Left out of TEXT fields and of queries (ADR-0063).
            b"STOPWORDS" => {
                let n = number(arg(args, i + 1), "FT.CREATE STOPWORDS")?;
                if args.len() < i + 2 + n {
                    return Err("FT.CREATE STOPWORDS <n> needs n words".into());
                }
                stopwords = Some(
                    args[i + 2..i + 2 + n]
                        .iter()
                        .map(|w| text(w).map(Into::into))
                        .collect::<Result<Vec<_>, _>>()?,
                );
                i += 2 + n;
            }
            // The documents' default score, which every scorer multiplies
            // by. redis-py's IndexDefinition sends `SCORE 1.0` on every
            // index.
            b"SCORE" => {
                score = arg(args, i + 1)
                    .and_then(|v| std::str::from_utf8(v).ok())
                    .and_then(|v| v.parse::<f64>().ok())
                    .filter(|v| (0.0..=1.0).contains(v))
                    .ok_or("FT.CREATE SCORE needs a number from 0 to 1")?;
                i += 2;
            }
            b"SCORE_FIELD" => {
                let f = arg(args, i + 1).ok_or("an FT.CREATE option needs a value")?;
                score_field = Some(text(f)?.into());
                i += 2;
            }
            // Stemming is English's (ADR-0063, sub-decision 2): an index in
            // another language would be stemmed wrongly, so it is refused.
            b"LANGUAGE" => {
                let l = arg(args, i + 1).ok_or("an FT.CREATE option needs a value")?;
                if !l.eq_ignore_ascii_case(b"english") {
                    return Err(format!(
                        "FT.CREATE LANGUAGE {} is not supported: English only (ADR-0063)",
                        String::from_utf8_lossy(l)
                    ));
                }
                i += 2;
            }
            b"LANGUAGE_FIELD" => {
                return Err(
                    "FT.CREATE LANGUAGE_FIELD is not supported: English only (ADR-0063)".into(),
                );
            }
            // A document's payload, which only `HAMMING` and `WITHPAYLOADS`
            // read, both refused.
            b"PAYLOAD_FIELD" => {
                if arg(args, i + 1).is_none() {
                    return Err("an FT.CREATE option needs a value".into());
                }
                i += 2;
            }
            b"SCHEMA" => {
                i += 1;
                break;
            }
            other => {
                return Err(format!(
                    "FT.CREATE option {} is not supported (ADR-0060)",
                    String::from_utf8_lossy(other)
                ));
            }
        }
    }
    if prefixes.is_empty() {
        return Err("FT.CREATE needs PREFIX: an index over every key is not served".into());
    }
    let mut attrs: Vec<Attr> = Vec::new();
    let mut vector: Option<VectorField> = None;
    while i < args.len() {
        let path = text(&args[i])?.to_string();
        i += 1;
        let mut name = path.clone();
        if arg(args, i).is_some_and(|a| a.eq_ignore_ascii_case(b"AS")) {
            name = text(arg(args, i + 1).ok_or("SCHEMA ... AS needs a name")?)?.to_string();
            i += 2;
        }
        if on == On::Json && !valid_json_path(&path) {
            return Err(format!(
                "SCHEMA field {path} is not a JSON path of member names ($.a or $.a.b)"
            ));
        }
        if !filter::valid_name(name.as_bytes()) {
            return Err(format!(
                "SCHEMA field name {name} must be letters, digits and _, at most {} \
                 (use AS to name it)",
                filter::MAX_NAME
            ));
        }
        if attrs.iter().any(|a| *a.field.name == *name)
            || vector.as_ref().is_some_and(|v| *v.name == *name)
        {
            return Err(format!("SCHEMA field {name} is given twice"));
        }
        let ty = upper(arg(args, i).ok_or_else(|| format!("SCHEMA field {name} needs a type"))?);
        i += 1;
        if ty == b"VECTOR" {
            if vector.is_some() {
                return Err("an index has at most one VECTOR field".into());
            }
            let (v, next) = parse_vector(args, i, &name, &path)?;
            vector = Some(v);
            i = next;
            continue;
        }
        let mut kind = match ty.as_slice() {
            b"TAG" => FieldKind::FoldedTag,
            b"NUMERIC" => FieldKind::Numeric,
            b"TEXT" => FieldKind::Text,
            b"GEO" => FieldKind::Geo,
            b"GEOSHAPE" => return Err("GEOSHAPE fields are not supported (ADR-0062)".into()),
            other => {
                return Err(format!(
                    "SCHEMA type {} is not one of TAG, NUMERIC, TEXT, GEO, VECTOR",
                    String::from_utf8_lossy(other)
                ));
            }
        };
        // RediSearch splits a JSON TAG's strings only on a declared
        // separator (`TAG_FIELD_DEFAULT_JSON_SEP` is none).
        let mut separator = if on == On::Json { 0 } else { b',' };
        let mut noindex = false;
        let (mut flags, mut weight): (Vec<Box<str>>, Option<Box<str>>) = (Vec::new(), None);
        // The field's options, up to the next field: a word that is none of
        // them starts the next field.
        while let Some(o) = arg(args, i) {
            let tag = matches!(kind, FieldKind::FoldedTag | FieldKind::Tag);
            match upper(o).as_slice() {
                b"SORTABLE" => {
                    flags.push("SORTABLE".into());
                    i += 1;
                    if arg(args, i).is_some_and(|a| a.eq_ignore_ascii_case(b"UNF")) {
                        flags.push("UNF".into());
                        i += 1;
                    }
                }
                b"NOINDEX" => {
                    noindex = true;
                    i += 1;
                }
                b"SEPARATOR" if tag => {
                    separator = match arg(args, i + 1) {
                        Some([c]) => *c,
                        _ => return Err("TAG SEPARATOR needs one character".into()),
                    };
                    i += 2;
                }
                b"CASESENSITIVE" if tag => {
                    kind = FieldKind::Tag;
                    i += 1;
                }
                // A word's weight in this field's scores (ADR-0063).
                b"WEIGHT" if kind == FieldKind::Text => {
                    let w = text(arg(args, i + 1).ok_or("TEXT WEIGHT needs a value")?)?;
                    if !w.parse::<f64>().is_ok_and(|x| x.is_finite() && x >= 0.0) {
                        return Err("TEXT WEIGHT needs a number of 0 or more".into());
                    }
                    weight = Some(w.into());
                    i += 2;
                }
                b"NOSTEM" if kind == FieldKind::Text => {
                    flags.push("NOSTEM".into());
                    i += 1;
                }
                b"WITHSUFFIXTRIE" if tag || kind == FieldKind::Text => {
                    flags.push("WITHSUFFIXTRIE".into());
                    i += 1;
                }
                o @ (b"PHONETIC" | b"INDEXMISSING" | b"INDEXEMPTY") => {
                    return Err(format!(
                        "SCHEMA option {} is not supported (ADR-0060)",
                        String::from_utf8_lossy(o)
                    ));
                }
                _ => break,
            }
        }
        if attrs.len() == filter::MAX_FIELDS {
            return Err(format!(
                "an index has at most {} fields besides its VECTOR",
                filter::MAX_FIELDS
            ));
        }
        attrs.push(Attr {
            field: Field {
                name: name.into(),
                kind,
            },
            path: path.into(),
            separator,
            noindex,
            flags,
            weight,
        });
    }
    if attrs.is_empty() && vector.is_none() {
        return Err("FT.CREATE SCHEMA needs at least one field".into());
    }
    Ok((
        index,
        IndexDef {
            on,
            prefixes,
            attrs,
            vector,
            stopwords,
            score,
            score_field,
        },
    ))
}

/// `VECTOR FLAT|HNSW <n> <attr> <value> ...`, from `i`, the algorithm.
fn parse_vector(
    args: &[Vec<u8>],
    i: usize,
    name: &str,
    path: &str,
) -> Result<(VectorField, usize), String> {
    let index = match arg(args, i).map(upper).as_deref() {
        Some(b"FLAT") => IndexKind::Flat,
        Some(b"HNSW") => IndexKind::Hnsw,
        Some(b"SVS-VAMANA") => return Err("VECTOR SVS-VAMANA is not supported (ADR-0060)".into()),
        _ => return Err("VECTOR needs FLAT or HNSW".into()),
    };
    let n = number(arg(args, i + 1), "VECTOR's attribute count")?;
    if n % 2 != 0 || args.len() < i + 2 + n {
        return Err("VECTOR <algorithm> <n> needs n words, in name and value pairs".into());
    }
    let (mut dim, mut metric, mut ty, mut ef_runtime) = (None, None, None, None);
    let mut hints: Vec<(Box<str>, Box<str>)> = Vec::new();
    for kv in args[i + 2..i + 2 + n].chunks(2) {
        let v = kv[1].as_slice();
        match upper(&kv[0]).as_slice() {
            b"TYPE" => ty = Some(upper(v)),
            b"DIM" => dim = Some(number(Some(v), "VECTOR DIM")?),
            b"DISTANCE_METRIC" => {
                metric = Some(match upper(v).as_slice() {
                    b"L2" => Metric::L2,
                    b"IP" => Metric::Ip,
                    b"COSINE" => Metric::Cosine,
                    _ => return Err("VECTOR DISTANCE_METRIC needs L2, IP or COSINE".into()),
                })
            }
            b"EF_RUNTIME" => ef_runtime = Some(number(Some(v), "VECTOR EF_RUNTIME")?),
            // Build-time and capacity hints for RediSearch's own engine. This
            // one's HNSW sets its own (ADR-0017); they change no answer.
            h @ (b"M" | b"EF_CONSTRUCTION" | b"EPSILON" | b"INITIAL_CAP" | b"BLOCK_SIZE") => {
                hints.push((String::from_utf8_lossy(h).into(), text(v)?.into()));
            }
            other => {
                return Err(format!(
                    "VECTOR attribute {} is not supported (ADR-0060)",
                    String::from_utf8_lossy(other)
                ));
            }
        }
    }
    match ty.as_deref() {
        Some(b"FLOAT32") => {}
        Some(other) => {
            return Err(format!(
                "VECTOR TYPE {} is not supported: FLOAT32 only (ADR-0060)",
                String::from_utf8_lossy(other)
            ));
        }
        None => return Err("VECTOR needs TYPE FLOAT32".into()),
    }
    let dim = dim
        .filter(|d| *d > 0)
        .ok_or("VECTOR needs DIM, a positive integer")?;
    let metric = metric.ok_or("VECTOR needs DISTANCE_METRIC")?;
    Ok((
        VectorField {
            name: name.into(),
            path: path.into(),
            index,
            dim,
            metric,
            ef_runtime,
            hints,
        },
        i + 2 + n,
    ))
}

/// A vector as little-endian float32 bytes, `dim` of them: a HASH field's
/// value, or a query's `PARAMS`.
pub fn vector_from_bytes(b: &[u8], dim: usize) -> Result<Vec<f32>, String> {
    if b.len() != dim * 4 {
        return Err(format!(
            "the vector is {} bytes; DIM {dim} FLOAT32 is {}",
            b.len(),
            dim * 4
        ));
    }
    let (words, _) = b.as_chunks::<4>();
    let v: Vec<f32> = words.iter().map(|w| f32::from_le_bytes(*w)).collect();
    if v.iter().any(|x| !x.is_finite()) {
        return Err("the vector holds a value that is not finite".into());
    }
    Ok(v)
}

/// `FT.SEARCH <index> <query> [NOCONTENT] [RETURN <n> <field> [AS <name>]
/// ...] [SORTBY <field> [ASC|DESC]] [LIMIT <offset> <num>] [PARAMS <n>
/// <name> <value> ...] [DIALECT 2]`, against the index `def` the caller has
/// found by name. The query is `*` or a filter, ADR-0059's subset with
/// exact-phrase TEXT, and optionally `=>[KNN <k> @<vector field> $<param>
/// [EF_RUNTIME <n>] [AS <name>]]`. The error is the text after `ERR `.
pub fn parse_search(args: &[Vec<u8>], def: &IndexDef) -> Result<Search, String> {
    let query = arg(args, 2).ok_or("FT.SEARCH <index> <query> [options]")?;
    let mut s = Search {
        query: None,
        filter: None,
        knn: None,
        nocontent: false,
        returns: None,
        sortby: None,
        offset: 0,
        num: 10,
        scorer: Scorer::DEFAULT,
        withscores: false,
    };
    let mut params: HashMap<String, Vec<u8>> = HashMap::new();
    let mut i = 3;
    while let Some(o) = arg(args, i) {
        match upper(o).as_slice() {
            b"NOCONTENT" => {
                s.nocontent = true;
                i += 1;
            }
            b"SCORER" => {
                s.scorer =
                    Scorer::parse(arg(args, i + 1).ok_or("FT.SEARCH SCORER needs a scorer")?)?;
                i += 2;
            }
            b"WITHSCORES" => {
                s.withscores = true;
                i += 1;
            }
            b"RETURN" => {
                let n = number(arg(args, i + 1), "FT.SEARCH RETURN")?;
                let end = i + 2 + n;
                if args.len() < end {
                    return Err("FT.SEARCH RETURN <n> needs n words".into());
                }
                let mut out = Vec::new();
                let mut j = i + 2;
                while j < end {
                    let f = text(&args[j])?;
                    let mut name = f;
                    if j + 2 < end && args[j + 1].eq_ignore_ascii_case(b"AS") {
                        name = text(&args[j + 2])?;
                        j += 2;
                    }
                    out.push((f.into(), name.into()));
                    j += 1;
                }
                s.returns = Some(out);
                i = end;
            }
            b"SORTBY" => {
                let f = text(arg(args, i + 1).ok_or("FT.SEARCH SORTBY needs a field")?)?;
                let mut asc = true;
                i += 2;
                match arg(args, i).map(upper).as_deref() {
                    Some(b"ASC") => i += 1,
                    Some(b"DESC") => {
                        asc = false;
                        i += 1;
                    }
                    _ => {}
                }
                s.sortby = Some((f.into(), asc));
            }
            b"LIMIT" => {
                s.offset = number(arg(args, i + 1), "FT.SEARCH LIMIT")?;
                s.num = number(arg(args, i + 2), "FT.SEARCH LIMIT")?;
                i += 3;
            }
            b"PARAMS" => {
                let n = number(arg(args, i + 1), "FT.SEARCH PARAMS")?;
                if n % 2 != 0 || args.len() < i + 2 + n {
                    return Err(
                        "FT.SEARCH PARAMS <n> needs n words, in name and value pairs".into(),
                    );
                }
                for kv in args[i + 2..i + 2 + n].chunks(2) {
                    params.insert(text(&kv[0])?.to_string(), kv[1].clone());
                }
                i += 2 + n;
            }
            b"DIALECT" => {
                match number(arg(args, i + 1), "FT.SEARCH DIALECT")? {
                    2 => {}
                    d => {
                        return Err(format!(
                            "FT.SEARCH DIALECT {d} is not supported: 2 only (ADR-0060)"
                        ));
                    }
                }
                i += 2;
            }
            other => {
                return Err(format!(
                    "FT.SEARCH option {} is not supported (ADR-0060)",
                    String::from_utf8_lossy(other)
                ));
            }
        }
    }
    let query = text(query)?;
    // ADR-0061: a vector range term comes out of the query before the rest
    // is read as a filter.
    let (rest, range) = take_range(query, def, &params)?;
    let query = rest.as_str();
    // The KNN clause is the last `=>[`: a `=>{...}` is a term's attributes.
    let (filter_part, knn_part) = match query.rfind("=>[") {
        Some(at) => (&query[..at], Some(&query[at + 2..])),
        None => (query, None),
    };
    let fields = def.fields();
    let f = filter_part.trim();
    if let Some(v) = &def.vector
        && f.contains(&format!("@{}:", v.name))
    {
        return Err(format!(
            "the query filters on the VECTOR field {}: only as one \
             @{}:[VECTOR_RANGE <radius> $<param>] term (ADR-0061)",
            v.name, v.name
        ));
    }
    let stop = StopWords::of(def.stopwords.as_deref());
    let mut q =
        ftquery::parse(f.as_bytes(), &fields, &stop).map_err(|e| format!("the query {e}"))?;
    // A term on a NOINDEX field matches no document, as RediSearch answers
    // it: the field is returned and sorted on, never searched.
    let noindex: Vec<usize> = def
        .attrs
        .iter()
        .enumerate()
        .filter(|(_, a)| a.noindex)
        .map(|(at, _)| at)
        .collect();
    if !noindex.is_empty() {
        q = q.map(|q| q.unindexed(&noindex));
    }
    s.filter = q
        .as_ref()
        .and_then(QNode::as_filter)
        .filter(|e| *e != Expr::All);
    s.query = q;
    if let Some(k) = knn_part {
        if range.is_some() {
            return Err("a vector range query cannot also be a KNN (ADR-0061)".into());
        }
        s.knn = Some(parse_knn(k, def, &params)?);
    }
    if range.is_some() {
        s.knn = range;
    }
    if s.withscores && s.knn.is_some() {
        return Err(
            "FT.SEARCH WITHSCORES with a KNN or a vector range is not supported (ADR-0063)".into(),
        );
    }
    if let Some((by, _)) = &s.sortby {
        let by_distance = s.knn.as_ref().is_some_and(|k| k.alias == *by);
        let by_attr = def.attrs.iter().any(|a| {
            *a.field.name == **by
                && matches!(
                    a.field.kind,
                    FieldKind::Numeric | FieldKind::Tag | FieldKind::FoldedTag | FieldKind::Text
                )
        });
        if !by_distance && !by_attr {
            return Err(format!(
                "FT.SEARCH SORTBY {by}: sorts by the KNN distance or a NUMERIC, TAG or TEXT field \
                 only (ADR-0063)"
            ));
        }
    }
    Ok(s)
}

/// The query without its vector range term, `@<field>:[VECTOR_RANGE
/// <radius> $<param>]=>{$YIELD_DISTANCE_AS: <name>; $EPSILON: <e>}`, and
/// the term as a [`Knn`] cut at its radius (ADR-0061). The term may stand
/// alone or be ANDed with a filter, in either order, as redisvl writes it;
/// under `|` or `-`, twice, or beside a KNN it is refused by name.
fn take_range(
    query: &str,
    def: &IndexDef,
    params: &HashMap<String, Vec<u8>>,
) -> Result<(String, Option<Knn>), String> {
    let up = query.to_ascii_uppercase();
    let Some(vr) = up.find("VECTOR_RANGE") else {
        return Ok((query.to_string(), None));
    };
    if up[vr + 1..].contains("VECTOR_RANGE") {
        return Err("a query takes one vector range term (ADR-0061)".into());
    }
    let Some(v) = &def.vector else {
        return Err("this index has no VECTOR field".into());
    };
    let shape = "a vector range term is @<field>:[VECTOR_RANGE <radius> $<param>]";
    let at = query[..vr].rfind('@').ok_or(shape)?;
    let (field, open) = query[at + 1..vr].split_once(':').ok_or(shape)?;
    if open.trim() != "[" {
        return Err(shape.into());
    }
    if field.trim() != &*v.name {
        return Err(format!(
            "VECTOR_RANGE names @{}, which is not this index's VECTOR field",
            field.trim()
        ));
    }
    let close = vr + query[vr..].find(']').ok_or(shape)?;
    let toks: Vec<&str> = query[vr..close].split_ascii_whitespace().collect();
    let [_, radius, vector] = toks.as_slice() else {
        return Err(shape.into());
    };
    let radius: f32 = std::str::from_utf8(&param(radius, params)?)
        .ok()
        .and_then(|r| r.trim().parse().ok())
        .filter(|r: &f32| r.is_finite())
        .ok_or("a vector range's radius is a number")?;
    if radius < 0.0 {
        return Err("a vector range's radius is at least 0".into());
    }
    let blob = match vector.strip_prefix('$') {
        Some(_) => param(vector, params)?,
        None => return Err("VECTOR_RANGE needs a $parameter for the query vector".into()),
    };
    let vector = vector_from_bytes(&blob, v.dim).map_err(|e| format!("the query {e}"))?;
    let mut knn = Knn {
        k: 0,
        vector,
        ef: None,
        alias: format!("__{}_score", v.name).into(),
        range: Some(radius),
    };
    let mut end = close + 1;
    if let Some(attrs) = query[end..].strip_prefix("=>{") {
        let len = attrs
            .find('}')
            .ok_or("a vector range's attributes end with }")?;
        for a in attrs[..len]
            .split(';')
            .map(str::trim)
            .filter(|a| !a.is_empty())
        {
            let (k, val) = a
                .split_once(':')
                .ok_or("a vector range's attribute is $<name>: <value>")?;
            match k.trim().to_ascii_uppercase().as_str() {
                "$YIELD_DISTANCE_AS" => knn.alias = val.trim().into(),
                // HNSW's boundary factor; a range here is cut from a KNN,
                // so it changes no answer. redisvl may send it.
                "$EPSILON" => {
                    val.trim()
                        .parse::<f32>()
                        .map_err(|_| "a vector range's $EPSILON is a number")?;
                }
                other => {
                    return Err(format!(
                        "the vector range attribute {other} is not supported (ADR-0061)"
                    ));
                }
            }
        }
        end += 3 + len + 1;
    }
    // A conjunct of the whole query, so that taking it out leaves the filter
    // it is ANDed with. redisvl wraps the two in one group, which comes off.
    let (mut before, mut after) = (query[..at].trim(), query[end..].trim());
    if let (Some(b), Some(a)) = (before.strip_prefix('('), after.strip_suffix(')'))
        && !breaks_a_conjunction(b)
        && !breaks_a_conjunction(a)
    {
        (before, after) = (b.trim(), a.trim());
    }
    if breaks_a_conjunction(before)
        || breaks_a_conjunction(after)
        || before.ends_with(['-', '~'])
        || after.contains("=>[")
    {
        return Err(
            "a vector range term stands alone or is ANDed with a filter, \
                    not under |, - or ~, nor beside a KNN (ADR-0061)"
                .into(),
        );
    }
    let rest = format!("{before} {after}");
    let rest = rest.trim();
    Ok((
        if rest.is_empty() { "*" } else { rest }.to_string(),
        Some(knn),
    ))
}

/// Whether `s`, the query on one side of a vector range term, keeps the term
/// out of a plain AND: a `|` outside every group, tag set, numeric range and
/// quote, or a group it opens and does not close, or closes and did not open.
fn breaks_a_conjunction(s: &str) -> bool {
    let (mut depth, mut until) = (0i32, None);
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match (until, c) {
            (_, '\\') => {
                chars.next();
            }
            (Some(end), c) if c == end => until = None,
            (Some(_), _) => {}
            (None, '{') => until = Some('}'),
            (None, '[') => until = Some(']'),
            (None, '"') => until = Some('"'),
            (None, '(') => depth += 1,
            (None, ')') => {
                depth -= 1;
                if depth < 0 {
                    return true;
                }
            }
            (None, '|') if depth == 0 => return true,
            _ => {}
        }
    }
    depth != 0 || until.is_some()
}

/// A KNN clause's value: a literal, or a `$parameter` from `PARAMS`.
fn param<'a>(tok: &'a str, params: &'a HashMap<String, Vec<u8>>) -> Result<Cow<'a, [u8]>, String> {
    match tok.strip_prefix('$') {
        Some(p) => params
            .get(p)
            .map(|v| Cow::Borrowed(v.as_slice()))
            .ok_or_else(|| format!("the query names ${p}, which PARAMS does not give")),
        None => Ok(Cow::Borrowed(tok.as_bytes())),
    }
}

fn param_number(
    tok: Option<&&str>,
    params: &HashMap<String, Vec<u8>>,
    what: &str,
) -> Result<usize, String> {
    let tok = tok.ok_or_else(|| format!("{what} needs a value"))?;
    number(Some(&param(tok, params)?), what)
}

/// `[KNN <k> @<field> $<param> [EF_RUNTIME <n>] [AS <name>]]`.
fn parse_knn(
    clause: &str,
    def: &IndexDef,
    params: &HashMap<String, Vec<u8>>,
) -> Result<Knn, String> {
    let inner = clause
        .trim()
        .strip_prefix('[')
        .and_then(|c| c.strip_suffix(']'))
        .ok_or("a vector clause is =>[KNN <k> @<field> $<param> ...]")?;
    let toks: Vec<&str> = inner.split_ascii_whitespace().collect();
    if !toks.first().is_some_and(|t| t.eq_ignore_ascii_case("KNN")) {
        return Err("a vector clause is =>[KNN ...]: RANGE is not supported (ADR-0060)".into());
    }
    let Some(v) = &def.vector else {
        return Err("this index has no VECTOR field".into());
    };
    let k = param_number(toks.get(1), params, "KNN's k")?;
    let field = toks
        .get(2)
        .and_then(|t| t.strip_prefix('@'))
        .ok_or("KNN needs @<vector field>")?;
    if field != &*v.name {
        return Err(format!(
            "KNN names @{field}, which is not this index's VECTOR field"
        ));
    }
    let blob = match toks.get(3) {
        Some(t) if t.starts_with('$') => param(t, params)?,
        _ => return Err("KNN needs a $parameter for the query vector".into()),
    };
    let vector = vector_from_bytes(&blob, v.dim).map_err(|e| format!("the query {e}"))?;
    let mut knn = Knn {
        k,
        vector,
        ef: None,
        alias: format!("__{}_score", v.name).into(),
        range: None,
    };
    let mut j = 4;
    while let Some(t) = toks.get(j) {
        match t.to_ascii_uppercase().as_str() {
            "EF_RUNTIME" => {
                knn.ef = Some(param_number(toks.get(j + 1), params, "EF_RUNTIME")?);
                j += 2;
            }
            "AS" => {
                knn.alias = (*toks.get(j + 1).ok_or("KNN ... AS needs a name")?).into();
                j += 2;
            }
            // RediSearch's own choice of how to run a filtered KNN, which
            // `redisvl` can write. This index plans by the matches' count
            // (ADR-0059 B); the answer is the same either way.
            "HYBRID_POLICY" | "BATCH_SIZE" => j += 2,
            other => return Err(format!("KNN option {other} is not supported (ADR-0060)")),
        }
    }
    Ok(knn)
}

/// `FT.DROPINDEX <index> [DD]`: the index's name, and whether its documents
/// are deleted with it.
pub fn parse_drop(args: &[Vec<u8>]) -> Result<(Vec<u8>, bool), String> {
    match args {
        [_, index] => Ok((index.clone(), false)),
        [_, index, dd] if dd.eq_ignore_ascii_case(b"DD") => Ok((index.clone(), true)),
        _ => Err("FT.DROPINDEX <index> [DD]".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::Val;

    fn a(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.as_bytes().to_vec()).collect()
    }

    fn blob(v: &[f32]) -> Vec<u8> {
        v.iter().flat_map(|x| x.to_le_bytes()).collect()
    }

    /// `parts`, then `PARAMS 2 <name> <the vector's bytes>`.
    fn with_vector(parts: &[&str], name: &str, v: &[f32]) -> Vec<Vec<u8>> {
        let mut args = a(parts);
        args.extend(a(&["PARAMS", "2", name]));
        args.push(blob(v));
        args
    }

    /// As `redisvl` writes a schema with a vector, through redis-py: the
    /// HNSW attributes, a TEXT field's WEIGHT, a TAG's SEPARATOR.
    fn redisvl_hash() -> IndexDef {
        let (name, def) = parse_create(&a(&[
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
            "WEIGHT",
            "1",
            "user",
            "TAG",
            "SEPARATOR",
            ",",
            "price",
            "NUMERIC",
            "SORTABLE",
            "embedding",
            "VECTOR",
            "HNSW",
            "12",
            "TYPE",
            "FLOAT32",
            "DIM",
            "3",
            "DISTANCE_METRIC",
            "COSINE",
            "M",
            "16",
            "EF_CONSTRUCTION",
            "200",
            "EF_RUNTIME",
            "10",
        ]))
        .expect("parses");
        assert_eq!(name, b"idx".to_vec());
        // redis-py's IndexDefinition adds `SCORE 1.0` to every index.
        let (_, with_score) = parse_create(&a(&[
            "FT.CREATE",
            "s",
            "ON",
            "HASH",
            "PREFIX",
            "1",
            "doc:",
            "SCORE",
            "1.0",
            "LANGUAGE",
            "english",
            "SCHEMA",
            "user",
            "TAG",
        ]))
        .expect("SCORE and LANGUAGE are accepted");
        assert_eq!(with_score.attrs.len(), 1);
        def
    }

    #[test]
    fn a_redisvl_schema_parses_and_round_trips_its_row() {
        let def = redisvl_hash();
        assert_eq!(def.on, On::Hash);
        assert_eq!(def.prefixes, vec![b"doc:".to_vec()]);
        let kinds: Vec<(&str, FieldKind)> = def
            .attrs
            .iter()
            .map(|x| (&*x.field.name, x.field.kind))
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("content", FieldKind::Text),
                ("user", FieldKind::FoldedTag),
                ("price", FieldKind::Numeric)
            ]
        );
        let v = def.vector.as_ref().expect("a vector field");
        assert_eq!(
            (v.index, v.dim, v.metric, v.ef_runtime),
            (IndexKind::Hnsw, 3, Metric::Cosine, Some(10))
        );
        assert_eq!(IndexDef::decode(&def.encode()), Some(def));
        // JSON, with paths and names, and a case-sensitive tag.
        let (_, j) = parse_create(&a(&[
            "FT.CREATE",
            "j",
            "ON",
            "JSON",
            "PREFIX",
            "2",
            "a:",
            "b:",
            "SCHEMA",
            "$.user.id",
            "AS",
            "user",
            "TAG",
            "CASESENSITIVE",
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
        ]))
        .expect("parses");
        assert_eq!(j.prefixes.len(), 2);
        assert_eq!(j.attrs[0].field.kind, FieldKind::Tag);
        assert_eq!(&*j.attrs[0].path, "$.user.id");
        assert_eq!(IndexDef::decode(&j.encode()), Some(j));
        // An index with no vector field, as LangChain's chat history makes.
        let (_, h) = parse_create(&a(&[
            "FT.CREATE",
            "h",
            "ON",
            "JSON",
            "PREFIX",
            "1",
            "chat:",
            "SCHEMA",
            "$.session_id",
            "AS",
            "session_id",
            "TAG",
        ]))
        .expect("parses");
        assert!(h.vector.is_none());
        // A row this binary cannot read is left out, not half served.
        let later = h
            .encode()
            .replace("\"kind\":\"foldedtag\"", "\"kind\":\"geoshape\"");
        assert_ne!(later, h.encode());
        assert_eq!(IndexDef::decode(&later), None);
    }

    #[test]
    fn what_create_does_not_serve_is_refused_by_name() {
        let refused = |parts: &[&str]| match parse_create(&a(parts)) {
            Ok(d) => panic!("{parts:?} parsed as {d:?}"),
            Err(e) => e,
        };
        let with = |rest: &[&'static str]| -> Vec<&'static str> {
            ["FT.CREATE", "i", "PREFIX", "1", "p"]
                .iter()
                .chain(rest)
                .copied()
                .collect()
        };
        assert!(refused(&["FT.CREATE", "i", "SCHEMA", "a", "TAG"]).contains("needs PREFIX"));
        assert!(
            refused(&["FT.CREATE", "i", "PREFIX", "1", "", "SCHEMA", "a", "TAG"])
                .contains("non-empty")
        );
        assert!(refused(&with(&["FILTER", "x", "SCHEMA", "a", "TAG"])).contains("FILTER"));
        assert!(refused(&with(&["SCHEMA", "g", "GEOSHAPE"])).contains("GEOSHAPE"));
        assert!(refused(&with(&["SCHEMA", "t", "TEXT", "PHONETIC", "dm:en"])).contains("PHONETIC"));
        assert!(refused(&with(&["SCHEMA", "a", "TAG", "a", "NUMERIC"])).contains("twice"));
        assert!(
            refused(&with(&["SCHEMA", "a", "TAG", "SEPARATOR", "::"])).contains("one character")
        );
        assert!(
            refused(&with(&[
                "SCHEMA",
                "v",
                "VECTOR",
                "HNSW",
                "6",
                "TYPE",
                "FLOAT64",
                "DIM",
                "2",
                "DISTANCE_METRIC",
                "L2",
            ]))
            .contains("FLOAT32 only")
        );
        assert!(
            refused(&with(&["SCHEMA", "v", "VECTOR", "SVS-VAMANA", "0"])).contains("SVS-VAMANA")
        );
        assert!(
            refused(&with(&[
                "SCHEMA",
                "v",
                "VECTOR",
                "FLAT",
                "6",
                "TYPE",
                "FLOAT32",
                "DIM",
                "0",
                "DISTANCE_METRIC",
                "L2",
            ]))
            .contains("DIM")
        );
        assert!(
            refused(&[
                "FT.CREATE",
                "i",
                "ON",
                "JSON",
                "PREFIX",
                "1",
                "p",
                "SCHEMA",
                "$..a",
                "AS",
                "a",
                "TAG",
            ])
            .contains("JSON path")
        );
        assert!(
            refused(&[
                "FT.CREATE",
                "i",
                "ON",
                "JSON",
                "PREFIX",
                "1",
                "p",
                "SCHEMA",
                "$.a",
                "TAG"
            ])
            .contains("letters, digits")
        );
        let names: Vec<String> = (0..=filter::MAX_FIELDS).map(|n| format!("f{n}")).collect();
        let mut many: Vec<&str> = with(&["SCHEMA"]);
        for n in &names {
            many.push(n);
            many.push("TAG");
        }
        assert!(refused(&many).contains("at most"));
    }

    #[test]
    fn a_redisvl_knn_search_parses() {
        let def = redisvl_hash();
        let q = [0.5f32, -0.25, 1.0];
        let mut args = a(&[
            "FT.SEARCH",
            "idx",
            "(@user:{Alice} @price:[(10 +inf])=>[KNN 3 @embedding $vector EF_RUNTIME $EF AS \
             vector_distance]",
            "RETURN",
            "3",
            "content",
            "user",
            "vector_distance",
            "SORTBY",
            "vector_distance",
            "ASC",
            "DIALECT",
            "2",
            "LIMIT",
            "0",
            "3",
            "PARAMS",
            "4",
            "vector",
        ]);
        args.push(blob(&q));
        args.extend(a(&["EF", "40"]));
        let s = parse_search(&args, &def).expect("parses");
        let knn = s.knn.as_ref().expect("a KNN clause");
        assert_eq!(
            (knn.k, knn.ef, &*knn.alias),
            (3, Some(40), "vector_distance")
        );
        assert_eq!(knn.vector, q.to_vec());
        assert_eq!(s.sortby, Some(("vector_distance".into(), true)));
        assert_eq!((s.offset, s.num), (0, 3));
        assert_eq!(s.returns.as_ref().map(|r| r.len()), Some(3));
        // The TAG folds case, as RediSearch's does without CASESENSITIVE.
        let f = s.filter.as_ref().expect("a filter");
        let vals = [
            Val::Missing,
            filter::hash_val(FieldKind::FoldedTag, b"alice", b','),
            Val::Num(11.0),
        ];
        assert!(f.matches(Some(&vals[..])));
        // `*` with KNN: every document, and the distance's default name.
        let s = parse_search(
            &with_vector(
                &[
                    "FT.SEARCH",
                    "idx",
                    "*=>[KNN 2 @embedding $v]",
                    "DIALECT",
                    "2",
                ],
                "v",
                &q,
            ),
            &def,
        )
        .expect("parses");
        assert!(s.filter.is_none());
        assert_eq!(&*s.knn.expect("knn").alias, "__embedding_score");
        // LangChain's default: a phrase on a TEXT field, and RETURN ... AS.
        let s = parse_search(
            &a(&[
                "FT.SEARCH",
                "idx",
                "@content:(\"my index\")",
                "RETURN",
                "3",
                "price",
                "AS",
                "p",
                "SORTBY",
                "price",
                "DESC",
            ]),
            &def,
        )
        .expect("parses");
        // A phrase is full text (ADR-0063): the query has it, and no filter
        // could.
        assert!(matches!(
            s.query.as_ref().map(|q| &q.node),
            Some(ftquery::Node::Phrase { .. })
        ));
        assert!(s.filter.is_none() && s.has_text());
        assert_eq!(s.returns, Some(vec![("price".into(), "p".into())]));
        assert_eq!(s.sortby, Some(("price".into(), false)));
    }

    /// ADR-0061: a vector range term, as redisvl and langchain-redis write
    /// it, parses as a KNN cut at its radius, alone or ANDed with a filter;
    /// the shapes it does not serve are refused by name.
    #[test]
    fn a_vector_range_term_parses_as_a_knn_cut_at_its_radius() {
        let def = redisvl_hash();
        let q = [1.0f32, 0.0, 0.0];
        let range = |query: &str| -> Result<Search, String> {
            let mut args = a(&[
                "FT.SEARCH",
                "idx",
                query,
                "SORTBY",
                "vector_distance",
                "ASC",
                "LIMIT",
                "0",
                "3",
                "DIALECT",
                "2",
                "PARAMS",
                "4",
                "distance_threshold",
                "0.25",
                "vector",
            ]);
            args.push(blob(&q));
            parse_search(&args, &def)
        };
        let s = range("@embedding:[VECTOR_RANGE $distance_threshold $vector]=>{$YIELD_DISTANCE_AS: vector_distance}")
            .expect("redisvl's range alone");
        let knn = s.knn.as_ref().expect("a range is a KNN");
        assert_eq!((knn.range, &*knn.alias), (Some(0.25), "vector_distance"));
        assert_eq!(knn.vector, q.to_vec());
        assert!(s.filter.is_none());
        let s = range(
            "@embedding:[VECTOR_RANGE $distance_threshold $vector]=>{$YIELD_DISTANCE_AS: vector_distance; $EPSILON: 0.01} (@user:{alice})",
        )
        .expect("redisvl's range with a filter and EPSILON");
        assert!(s.filter.is_some());
        // redisvl's own shape with a filter: the two in one group.
        let s = range(
            "(@embedding:[VECTOR_RANGE $distance_threshold $vector]=>{$YIELD_DISTANCE_AS: vector_distance} (@user:{alice}))",
        )
        .expect("redisvl's range and filter in one group");
        assert!(s.filter.is_some());
        let s =
            range("(@user:{alice} @embedding:[VECTOR_RANGE 0.5 $vector]=>{$YIELD_DISTANCE_AS: vector_distance})")
                .expect("filter first");
        assert_eq!(s.knn.as_ref().and_then(|k| k.range), Some(0.5));
        assert!(s.filter.is_some());
        // A `|` inside a tag set or a group is the filter's own.
        for q in [
            "@user:{alice|bob} @embedding:[VECTOR_RANGE 0.5 $vector]=>{$YIELD_DISTANCE_AS: vector_distance}",
            "(@user:{alice} | @user:{bob}) @embedding:[VECTOR_RANGE 0.5 $vector]=>{$YIELD_DISTANCE_AS: vector_distance}",
        ] {
            assert!(range(q).expect(q).filter.is_some(), "{q}");
        }
        let refused = |query: &str| range(query).expect_err(query);
        for q in [
            "@user:{a} | @embedding:[VECTOR_RANGE 0.5 $vector]",
            "@user:{a} @embedding:[VECTOR_RANGE 0.5 $vector] | @user:{b}",
            "(@embedding:[VECTOR_RANGE 0.5 $vector]) | @user:{b}",
            "(@user:{a} @embedding:[VECTOR_RANGE 0.5 $vector]) | @user:{b}",
            "@user:{a} | (@embedding:[VECTOR_RANGE 0.5 $vector])",
            "-@embedding:[VECTOR_RANGE 0.5 $vector]",
            "-(@embedding:[VECTOR_RANGE 0.5 $vector])",
            "~@embedding:[VECTOR_RANGE 0.5 $vector]",
            "@embedding:[VECTOR_RANGE 0.5 $vector]=>[KNN 2 @embedding $vector]",
        ] {
            assert!(refused(q).contains("ADR-0061"), "{q}: {}", refused(q));
        }
        assert!(
            refused("@embedding:[VECTOR_RANGE 0.5 $vector] @embedding:[VECTOR_RANGE 0.6 $vector]")
                .contains("one vector range")
        );
        assert!(
            refused("@embedding:[VECTOR_RANGE 0.5 $vector]=>{$SEARCH_WINDOW_SIZE: 4}")
                .contains("SEARCH_WINDOW_SIZE")
        );
        assert!(refused("@embedding:[VECTOR_RANGE -1 $vector]").contains("at least 0"));
        assert!(refused("@other:[VECTOR_RANGE 0.5 $vector]").contains("not this index's VECTOR"));
        assert!(refused("@embedding:[VECTOR_RANGE 0.5 vector]").contains("$parameter"));
    }

    #[test]
    fn what_search_does_not_serve_is_refused_by_name() {
        let def = redisvl_hash();
        let q = [1.0f32, 0.0, 0.0];
        let refused = |args: Vec<Vec<u8>>| match parse_search(&args, &def) {
            Ok(s) => panic!("{args:?} parsed as {s:?}"),
            Err(e) => e,
        };
        assert!(
            refused(with_vector(
                &["FT.SEARCH", "idx", "*=>[KNN 2 @embedding $v]", "WITHSCORES"],
                "v",
                &q
            ))
            .contains("WITHSCORES")
        );
        assert!(refused(a(&["FT.SEARCH", "idx", "*", "SCORER", "HAMMING"])).contains("HAMMING"));
        assert!(refused(a(&["FT.SEARCH", "idx", "*", "SCORER", "NOPE"])).contains("not a scorer"));
        assert!(
            refused(a(&["FT.SEARCH", "idx", "hello=>{$phonetic: true}"])).contains("$phonetic")
        );
        assert!(refused(a(&["FT.SEARCH", "idx", "*", "VERBATIM"])).contains("VERBATIM"));
        assert!(refused(a(&["FT.SEARCH", "idx", "*", "DIALECT", "1"])).contains("DIALECT 1"));
        assert!(
            refused(with_vector(
                &["FT.SEARCH", "idx", "*=>[RANGE 0.2 @embedding $v]"],
                "v",
                &q
            ))
            .contains("RANGE")
        );
        assert!(
            refused(a(&["FT.SEARCH", "idx", "*=>[KNN 2 @embedding $v]"]))
                .contains("PARAMS does not give")
        );
        assert!(
            refused(with_vector(
                &["FT.SEARCH", "idx", "*=>[KNN 2 @other $v]"],
                "v",
                &q
            ))
            .contains("not this index's VECTOR")
        );
        assert!(
            refused(with_vector(
                &["FT.SEARCH", "idx", "*=>[KNN 2 @embedding $v]"],
                "v",
                &q[..2]
            ))
            .contains("bytes")
        );
        // Full text is served (ADR-0063), scored by BM25STD unless named.
        let s = parse_search(
            &a(&[
                "FT.SEARCH",
                "idx",
                "@content:(hello world)",
                "SCORER",
                "TFIDF",
                "WITHSCORES",
            ]),
            &def,
        )
        .expect("parses");
        assert_eq!((s.scorer, s.withscores), (Scorer::Tfidf, true));
        let s = parse_search(&a(&["FT.SEARCH", "idx", "hello"]), &def).expect("parses");
        assert_eq!(s.scorer, Scorer::Bm25Std);
        assert!(
            refused(a(&["FT.SEARCH", "idx", "*", "SORTBY", "nosuch"])).contains("TAG or TEXT")
        );
        let (_, noidx) = parse_create(&a(&[
            "FT.CREATE",
            "n",
            "PREFIX",
            "1",
            "p",
            "SCHEMA",
            "a",
            "TAG",
            "NOINDEX",
        ]))
        .expect("parses");
        // A term on a NOINDEX field matches nothing, as in RediSearch.
        let s = parse_search(&a(&["FT.SEARCH", "n", "@a:{x}"]), &noidx).expect("parses");
        let vals = [filter::hash_val(FieldKind::FoldedTag, b"x", b',')];
        assert!(!s.filter.expect("a filter").matches(Some(&vals[..])));
        assert_eq!(
            parse_drop(&a(&["FT.DROPINDEX", "i", "dd"])),
            Ok((b"i".to_vec(), true))
        );
        assert!(parse_drop(&a(&["FT.DROPINDEX", "i", "KEEPDOCS"])).is_err());
    }

    /// ADR-0063: a document's score and the index's language.
    #[test]
    fn scores_are_kept_and_languages_other_than_english_refused() {
        let base = ["FT.CREATE", "s", "ON", "HASH", "PREFIX", "1", "doc:"];
        let create = |extra: &[&str]| {
            let mut v: Vec<&str> = base.to_vec();
            v.extend_from_slice(extra);
            v.extend_from_slice(&["SCHEMA", "t", "TEXT", "WEIGHT", "2.5"]);
            parse_create(&a(&v))
        };
        let (_, def) = create(&[
            "SCORE",
            "0.5",
            "SCORE_FIELD",
            "__score",
            "LANGUAGE",
            "English",
        ])
        .expect("parses");
        assert_eq!(
            (def.score, def.score_field.as_deref()),
            (0.5, Some("__score"))
        );
        // Kept through the index row, which an older binary wrote without.
        let back = IndexDef::decode(&def.encode()).expect("decodes");
        assert_eq!(back, def);
        let mut old: Value = serde_json::from_str(&def.encode()).expect("json");
        let o = old.as_object_mut().expect("an object");
        o.remove("score");
        o.remove("score_field");
        let old = IndexDef::decode(&old.to_string()).expect("an older row decodes");
        assert_eq!((old.score, old.score_field), (1.0, None));
        for (extra, says) in [
            (&["LANGUAGE", "german"][..], "English only"),
            (&["LANGUAGE_FIELD", "lang"][..], "LANGUAGE_FIELD"),
            (&["SCORE", "2"][..], "from 0 to 1"),
        ] {
            let e = create(extra).expect_err("refused");
            assert!(e.contains(says), "{extra:?}: {e}");
        }
        let mut v: Vec<&str> = base.to_vec();
        v.extend_from_slice(&["SCHEMA", "t", "TEXT", "WEIGHT", "heavy"]);
        assert!(
            parse_create(&a(&v))
                .expect_err("refused")
                .contains("WEIGHT")
        );
    }

    #[test]
    fn distances_are_reported_as_redisearch_reports_them() {
        assert_eq!(distance(Metric::Cosine, 1.0), 0.0);
        assert_eq!(distance(Metric::Ip, 0.25), 0.75);
        // This crate scores L2 as the negated squared distance.
        assert_eq!(distance(Metric::L2, -4.0), 4.0);
        assert_eq!(
            vector_from_bytes(&blob(&[1.5, -2.0]), 2),
            Ok(vec![1.5, -2.0])
        );
        assert!(vector_from_bytes(&blob(&[f32::NAN, 0.0]), 2).is_err());
    }

    #[test]
    fn hash_values_split_fold_and_tokenize_as_redisearch_does() {
        assert_eq!(
            filter::hash_val(FieldKind::FoldedTag, b" Red, BLUE ,red", b','),
            Val::Tags(Box::new([b"blue"[..].into(), b"red"[..].into()]))
        );
        assert_eq!(
            filter::hash_val(FieldKind::Tag, b"Red|red", b'|'),
            Val::Tags(Box::new([b"Red"[..].into(), b"red"[..].into()]))
        );
        assert_eq!(
            filter::hash_val(FieldKind::Numeric, b" 4.5 ", b','),
            Val::Num(4.5)
        );
        assert_eq!(
            filter::hash_val(FieldKind::Numeric, b"nan", b','),
            Val::Missing
        );
        let want: Vec<Box<[u8]>> = ["hello", "world", "wide", "web"]
            .iter()
            .map(|w| w.as_bytes().into())
            .collect();
        assert_eq!(filter::tokenize(b"Hello, World-wide web!"), want);
        let fields = vec![Field {
            name: "t".into(),
            kind: FieldKind::Text,
        }];
        let doc = filter::hash_val(FieldKind::Text, b"the Quick brown fox", b',');
        let hit = |q: &str| {
            filter::parse(q.as_bytes(), &fields)
                .expect(q)
                .matches(Some(std::slice::from_ref(&doc)))
        };
        assert!(hit("@t:(\"quick brown\")"));
        assert!(hit("@t:\"QUICK\""));
        assert!(!hit("@t:(\"brown quick\")"), "a phrase keeps its order");
        assert!(!hit("@t:(\"quick fox\")"), "and its words together");
    }
}
