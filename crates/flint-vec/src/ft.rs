// SPDX-License-Identifier: Elastic-2.0
//! RediSearch's `FT.*` vector surface (ADR-0060), parsed: what `FT.CREATE`
//! and `FT.SEARCH` say, as an index's definition and a search request. The
//! subset `redisvl`, `langchain-redis` and the LlamaIndex Redis store send
//! (ADR-0059's survey); every other option is refused by name, never passed
//! over, so a search is never answered as if it had asked for less.

use crate::filter::{self, Expr, Field, FieldKind};
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
}

/// A TAG, NUMERIC or TEXT attribute.
#[derive(Clone, Debug, PartialEq)]
pub struct Attr {
    /// Its name, as a query names it, and its kind.
    pub field: Field,
    /// Where it is in a document: a HASH field, or a JSON path.
    pub path: Box<str>,
    /// A HASH TAG's separator (`,` unless `SEPARATOR` says otherwise).
    pub separator: u8,
    /// `NOINDEX`: returned and sortable, but not filtered on.
    pub noindex: bool,
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
            })
        });
        json!({
            "on": match self.on { On::Hash => "hash", On::Json => "json" },
            "prefixes": self.prefixes,
            "attrs": attrs,
            "vector": vector,
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
            }),
        };
        Some(IndexDef {
            on,
            prefixes,
            attrs,
            vector,
        })
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
    }
}

fn kind_from_name(s: &str) -> Option<FieldKind> {
    Some(match s {
        "tag" => FieldKind::Tag,
        "foldedtag" => FieldKind::FoldedTag,
        "numeric" => FieldKind::Numeric,
        "text" => FieldKind::Text,
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
}

/// What `FT.SEARCH` asked for.
#[derive(Debug, PartialEq)]
pub struct Search {
    /// `None` is `*`: every document.
    pub filter: Option<Expr>,
    pub knn: Option<Knn>,
    pub nocontent: bool,
    /// `RETURN`: `(field, the name to answer it as)`. `None` returns every
    /// field.
    pub returns: Option<Vec<(Box<str>, Box<str>)>>,
    /// `SORTBY`: the KNN distance's name or a NUMERIC or TAG attribute's,
    /// and whether ascending.
    pub sortby: Option<(Box<str>, bool)>,
    pub offset: usize,
    pub num: usize,
}

/// A distance as RediSearch reports one, from this crate's score, where
/// higher is nearer: `1 - cos` for COSINE, `1 - dot` for IP, and the squared
/// distance for L2, as RediSearch's own engine reports them.
pub fn distance(metric: Metric, score: f32) -> f32 {
    match metric {
        Metric::Cosine | Metric::Ip => 1.0 - score,
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
            // Stop words shape full-text scoring, which is not served. An
            // exact phrase is matched with every word it has, stop word or
            // not (ADR-0060).
            b"STOPWORDS" => {
                let n = number(arg(args, i + 1), "FT.CREATE STOPWORDS")?;
                if args.len() < i + 2 + n {
                    return Err("FT.CREATE STOPWORDS <n> needs n words".into());
                }
                i += 2 + n;
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
            b"GEO" | b"GEOSHAPE" => return Err("GEO fields are not supported (ADR-0060)".into()),
            other => {
                return Err(format!(
                    "SCHEMA type {} is not one of TAG, NUMERIC, TEXT, VECTOR",
                    String::from_utf8_lossy(other)
                ));
            }
        };
        let (mut separator, mut noindex) = (b',', false);
        // The field's options, up to the next field: a word that is none of
        // them starts the next field.
        while let Some(o) = arg(args, i) {
            let tag = matches!(kind, FieldKind::FoldedTag | FieldKind::Tag);
            match upper(o).as_slice() {
                b"SORTABLE" => {
                    i += 1;
                    if arg(args, i).is_some_and(|a| a.eq_ignore_ascii_case(b"UNF")) {
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
                // Full-text ranking and stemming, which an exact phrase
                // does not use.
                b"WEIGHT" if kind == FieldKind::Text => i += 2,
                b"NOSTEM" if kind == FieldKind::Text => i += 1,
                o @ (b"PHONETIC" | b"WITHSUFFIXTRIE" | b"INDEXMISSING" | b"INDEXEMPTY") => {
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
            b"M" | b"EF_CONSTRUCTION" | b"EPSILON" | b"INITIAL_CAP" | b"BLOCK_SIZE" => {}
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
        filter: None,
        knn: None,
        nocontent: false,
        returns: None,
        sortby: None,
        offset: 0,
        num: 10,
    };
    let mut params: HashMap<String, Vec<u8>> = HashMap::new();
    let mut i = 3;
    while let Some(o) = arg(args, i) {
        match upper(o).as_slice() {
            b"NOCONTENT" => {
                s.nocontent = true;
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
    let (filter_part, knn_part) = match query.rfind("=>") {
        Some(at) => (&query[..at], Some(&query[at + 2..])),
        None => (query, None),
    };
    let fields = def.fields();
    let f = filter_part.trim();
    if f != "*" {
        let e = filter::parse(f.as_bytes(), &fields).map_err(|e| format!("the query {e}"))?;
        for a in def.attrs.iter().filter(|a| a.noindex) {
            if mentions(&e, &fields, &a.field.name) {
                return Err(format!(
                    "the query filters on {}, which is NOINDEX",
                    a.field.name
                ));
            }
        }
        if e != Expr::All {
            s.filter = Some(e);
        }
    }
    if let Some(k) = knn_part {
        s.knn = Some(parse_knn(k, def, &params)?);
    }
    if let Some((by, _)) = &s.sortby {
        let by_distance = s.knn.as_ref().is_some_and(|k| k.alias == *by);
        let by_attr = def.attrs.iter().any(|a| {
            *a.field.name == **by
                && matches!(
                    a.field.kind,
                    FieldKind::Numeric | FieldKind::Tag | FieldKind::FoldedTag
                )
        });
        if !by_distance && !by_attr {
            return Err(format!(
                "FT.SEARCH SORTBY {by}: sorts by the KNN distance or a NUMERIC or TAG field only \
                 (ADR-0060)"
            ));
        }
    }
    Ok(s)
}

/// Whether `e` names the attribute `name`.
fn mentions(e: &Expr, fields: &[Field], name: &str) -> bool {
    let at = fields.iter().position(|f| &*f.name == name);
    match e {
        Expr::All => false,
        Expr::Tag { field, .. } | Expr::Range { field, .. } | Expr::Phrase { field, .. } => {
            Some(*field) == at
        }
        Expr::Not(x) => mentions(x, fields, name),
        Expr::And(xs) | Expr::Or(xs) => xs.iter().any(|x| mentions(x, fields, name)),
    }
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
            .replace("\"kind\":\"foldedtag\"", "\"kind\":\"geo\"");
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
        assert!(refused(&with(&["SCHEMA", "g", "GEO"])).contains("GEO"));
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
        assert!(matches!(s.filter, Some(Expr::Phrase { .. })));
        assert_eq!(s.returns, Some(vec![("price".into(), "p".into())]));
        assert_eq!(s.sortby, Some(("price".into(), false)));
    }

    #[test]
    fn what_search_does_not_serve_is_refused_by_name() {
        let def = redisvl_hash();
        let q = [1.0f32, 0.0, 0.0];
        let refused = |args: Vec<Vec<u8>>| match parse_search(&args, &def) {
            Ok(s) => panic!("{args:?} parsed as {s:?}"),
            Err(e) => e,
        };
        assert!(refused(a(&["FT.SEARCH", "idx", "*", "WITHSCORES"])).contains("WITHSCORES"));
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
        assert!(
            refused(a(&["FT.SEARCH", "idx", "@content:(hello world)"])).contains("exact phrase")
        );
        assert!(refused(a(&["FT.SEARCH", "idx", "hello"])).contains("full-text"));
        assert!(
            refused(a(&["FT.SEARCH", "idx", "*", "SORTBY", "content"])).contains("NUMERIC or TAG")
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
        assert!(
            parse_search(&a(&["FT.SEARCH", "n", "@a:{x}"]), &noidx)
                .expect_err("NOINDEX")
                .contains("NOINDEX")
        );
        assert_eq!(
            parse_drop(&a(&["FT.DROPINDEX", "i", "dd"])),
            Ok((b"i".to_vec(), true))
        );
        assert!(parse_drop(&a(&["FT.DROPINDEX", "i", "KEEPDOCS"])).is_err());
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
