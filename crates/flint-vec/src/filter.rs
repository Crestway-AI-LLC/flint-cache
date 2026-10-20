// SPDX-License-Identifier: Elastic-2.0
//! Vector search filtered by metadata (ADR-0059 option B): the fields a set
//! declares at `VEC.CREATE`, read from each vector's `META` when it is a JSON
//! object, and the `FILTER` expression `VEC.SEARCH` applies BEFORE ranking.
//! Filtering after ranking recalls about `min(1, f x s)` of a filter that
//! matches a fraction `s` of a set, for a search that fetches `f x k`.
//!
//! The expression is a subset of RediSearch's query syntax, the subset
//! `redisvl` writes for tags and numbers:
//! - `@f:{a | b}` matches a TAG field holding `a` or `b`. A backslash makes
//!   the next character part of the tag, and blanks at either end are
//!   dropped.
//! - `@n:[lo hi]` matches a NUMERIC field from `lo` to `hi`. `(` before a
//!   bound excludes it, and `-inf` or `+inf` leaves it open.
//! - `-x` negates, terms side by side are AND, `|` between terms is OR,
//!   parentheses group, and `*` is everything.
//!
//! AND and OR do not mix without parentheses (`a b | c` is refused): the two
//! RediSearch dialects group them differently, and a filter that scopes a
//! tenant's results must not depend on which one a reader assumes. `redisvl`
//! parenthesizes every combination it writes, so it never needs to.
//!
//! Tags match exactly, case included. RediSearch folds a TAG's case unless
//! the field is `CASESENSITIVE`; here `Alice` and `alice` are two values, so
//! one user's scope cannot take in another's.

use crate::hnsw::SlotSet;
use std::collections::{BTreeSet, HashMap};
use std::ops::Bound;

/// How many fields a set may declare.
pub const MAX_FIELDS: usize = 16;
/// The longest field name.
pub const MAX_NAME: usize = 64;
/// How deeply a filter may nest, parentheses and negations together: past
/// this a parse would recurse without bound on a tenant's input.
const MAX_DEPTH: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FieldKind {
    /// A string, or an array of strings: `@f:{...}`.
    Tag,
    /// A number: `@f:[lo hi]`.
    Numeric,
    /// RediSearch's TAG as `FT.CREATE` declares it without `CASESENSITIVE`
    /// (ADR-0060): its values, and a filter's, are compared case-folded.
    /// `VEC.CREATE` declares none: its tags match case and all.
    FoldedTag,
    /// RediSearch's TEXT, matched only as an exact phrase (ADR-0060):
    /// `@f:("a b")` matches a value holding those words in that order, split
    /// as RediSearch splits them and lowercased. `FT.CREATE` only.
    Text,
}

impl FieldKind {
    pub fn parse(s: &[u8]) -> Option<FieldKind> {
        match s.to_ascii_uppercase().as_slice() {
            b"TAG" => Some(FieldKind::Tag),
            b"NUMERIC" => Some(FieldKind::Numeric),
            _ => None,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            FieldKind::Tag | FieldKind::FoldedTag => "tag",
            FieldKind::Numeric => "numeric",
            FieldKind::Text => "text",
        }
    }
}

/// A declared field: its name in `META`'s top-level object, and its kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: Box<str>,
    pub kind: FieldKind,
}

/// A field name: letters, digits and `_`, at most [`MAX_NAME`]. Narrower than
/// a JSON key on purpose: the name is written into the set's config row and
/// read back out of a filter, and neither has to escape anything.
pub fn valid_name(b: &[u8]) -> bool {
    !b.is_empty()
        && b.len() <= MAX_NAME
        && b.iter().all(|c| c.is_ascii_alphanumeric() || *c == b'_')
}

/// The config row's fifth field: `name:kind,name:kind`.
pub fn encode_fields(fields: &[Field]) -> String {
    fields
        .iter()
        .map(|f| format!("{}:{}", f.name, f.kind.as_str()))
        .collect::<Vec<_>>()
        .join(",")
}

/// Inverse of [`encode_fields`]. An entry this binary cannot read, such as a
/// kind a later binary adds, is left out rather than refusing the row: the
/// set still loads, and a filter on that field is refused as unknown, out
/// loud. Refusing the row would drop the whole set from the index.
pub fn decode_fields(s: &str) -> Vec<Field> {
    let mut out: Vec<Field> = Vec::new();
    for part in s.split(',') {
        let Some((name, kind)) = part.split_once(':') else {
            continue;
        };
        let Some(kind) = FieldKind::parse(kind.as_bytes()) else {
            continue;
        };
        if valid_name(name.as_bytes()) && !out.iter().any(|f| &*f.name == name) {
            out.push(Field {
                name: name.into(),
                kind,
            });
        }
    }
    out
}

/// One field's value in one vector's `META`.
#[derive(Debug, PartialEq)]
pub enum Val {
    /// Not there, or not of the field's kind: matches no term on the field,
    /// so its negation matches.
    Missing,
    /// A TAG field's values, sorted and without repeats.
    Tags(Box<[Box<[u8]>]>),
    /// A NUMERIC field's value. Never NaN, since JSON has none, and `-0` is
    /// stored as `0`.
    Num(f64),
    /// A TEXT field's words, in order, as [`tokenize`] splits them.
    Text(Box<[Box<[u8]>]>),
}

/// The bytes RediSearch splits text on by default, besides whitespace.
const SEPARATORS: &[u8] = b",.<>{}[]\"':;!@#$%^&*()-+=~";

/// A TEXT value's words, as RediSearch splits them by default and folded to
/// lower case: what an exact phrase is matched against.
pub fn tokenize(text: &[u8]) -> Vec<Box<[u8]>> {
    text.split(|b| b.is_ascii_whitespace() || SEPARATORS.contains(b))
        .filter(|w| !w.is_empty())
        .map(fold)
        .collect()
}

/// `raw` folded to lower case: Unicode's folding when it is UTF-8, ASCII's
/// otherwise.
pub fn fold(raw: &[u8]) -> Box<[u8]> {
    match std::str::from_utf8(raw) {
        Ok(s) => s.to_lowercase().into_bytes().into_boxed_slice(),
        Err(_) => raw.to_ascii_lowercase().into_boxed_slice(),
    }
}

/// One field's value from a HASH field's bytes (ADR-0060): a TAG field
/// split on `separator` with blanks trimmed, a NUMERIC one read as a number,
/// a TEXT one split into words.
pub fn hash_val(kind: FieldKind, raw: &[u8], separator: u8) -> Val {
    match kind {
        FieldKind::Tag | FieldKind::FoldedTag => {
            let mut ts: Vec<Box<[u8]>> = raw
                .split(|b| *b == separator)
                .map(|t| t.trim_ascii())
                .filter(|t| !t.is_empty())
                .map(|t| {
                    if kind == FieldKind::FoldedTag {
                        fold(t)
                    } else {
                        t.into()
                    }
                })
                .collect();
            ts.sort();
            ts.dedup();
            if ts.is_empty() {
                Val::Missing
            } else {
                Val::Tags(ts.into_boxed_slice())
            }
        }
        FieldKind::Numeric => match std::str::from_utf8(raw)
            .ok()
            .and_then(|s| s.trim().parse::<f64>().ok())
        {
            Some(x) if x.is_nan() => Val::Missing,
            Some(x) => Val::Num(if x == 0.0 { 0.0 } else { x }),
            None => Val::Missing,
        },
        FieldKind::Text => {
            let ws = tokenize(raw);
            if ws.is_empty() {
                Val::Missing
            } else {
                Val::Text(ws.into_boxed_slice())
            }
        }
    }
}

/// One vector's values, in the order its set declares its fields.
pub type Vals = Box<[Val]>;

/// The values of `fields` in `meta`, or `None` when it holds none of them:
/// a set with no fields, `META` that is absent or not a JSON object, or one
/// that has none of the fields. `META` itself stays the bytes it was.
pub fn extract(fields: &[Field], meta: Option<&[u8]>) -> Option<Vals> {
    if fields.is_empty() {
        return None;
    }
    let serde_json::Value::Object(obj) = serde_json::from_slice::<serde_json::Value>(meta?).ok()?
    else {
        return None;
    };
    let mut any = false;
    let vals: Vals = fields
        .iter()
        .map(|f| {
            let v = match (f.kind, obj.get(&*f.name)) {
                (FieldKind::Tag, Some(serde_json::Value::String(s))) => {
                    Val::Tags(Box::new([s.as_bytes().into()]))
                }
                (FieldKind::Tag, Some(serde_json::Value::Array(a))) => {
                    let mut ts: Vec<Box<[u8]>> = a
                        .iter()
                        .filter_map(|x| x.as_str())
                        .map(|s| s.as_bytes().into())
                        .collect();
                    ts.sort();
                    ts.dedup();
                    if ts.is_empty() {
                        Val::Missing
                    } else {
                        Val::Tags(ts.into_boxed_slice())
                    }
                }
                (FieldKind::FoldedTag, Some(serde_json::Value::String(s))) => {
                    hash_val(FieldKind::FoldedTag, s.as_bytes(), 0)
                }
                (FieldKind::FoldedTag, Some(serde_json::Value::Array(a))) => {
                    let mut ts: Vec<Box<[u8]>> = a
                        .iter()
                        .filter_map(|x| x.as_str())
                        .map(|s| fold(s.as_bytes()))
                        .collect();
                    ts.sort();
                    ts.dedup();
                    if ts.is_empty() {
                        Val::Missing
                    } else {
                        Val::Tags(ts.into_boxed_slice())
                    }
                }
                (FieldKind::Text, Some(serde_json::Value::String(s))) => {
                    hash_val(FieldKind::Text, s.as_bytes(), 0)
                }
                (FieldKind::Numeric, Some(serde_json::Value::Number(n))) => match n.as_f64() {
                    // `-0` is stored as `0`, so the two sort as one value.
                    Some(x) => Val::Num(if x == 0.0 { 0.0 } else { x }),
                    None => Val::Missing,
                },
                _ => Val::Missing,
            };
            any |= v != Val::Missing;
            v
        })
        .collect();
    any.then_some(vals)
}

/// The heap `vals` holds, by what it asked the allocator for.
pub fn vals_bytes(vals: Option<&[Val]>) -> usize {
    let Some(v) = vals else {
        return 0;
    };
    let mut b = std::mem::size_of_val(v);
    for x in v {
        if let Val::Tags(ts) | Val::Text(ts) = x {
            b += std::mem::size_of_val(&**ts) + ts.iter().map(|t| t.len()).sum::<usize>();
        }
    }
    b
}

/// A parsed `FILTER`, its fields resolved to their positions in the set.
#[derive(Debug, PartialEq)]
pub enum Expr {
    /// `*`: every vector.
    All,
    /// `@f:{a | b}`: field `field` holds one of `any_of`.
    Tag {
        field: usize,
        any_of: Vec<Box<[u8]>>,
    },
    /// `@f:[lo hi]`, each bound inclusive unless written with `(`.
    Range {
        field: usize,
        lo: f64,
        lo_incl: bool,
        hi: f64,
        hi_incl: bool,
    },
    /// `@f:("a b")`: TEXT field `field` holds `words`, in order, together.
    Phrase {
        field: usize,
        words: Vec<Box<[u8]>>,
    },
    Not(Box<Expr>),
    And(Vec<Expr>),
    Or(Vec<Expr>),
}

impl Expr {
    /// Whether a vector with these values matches.
    pub fn matches(&self, vals: Option<&[Val]>) -> bool {
        let at = |f: usize| vals.and_then(|v| v.get(f));
        match self {
            Expr::All => true,
            Expr::Tag { field, any_of } => match at(*field) {
                Some(Val::Tags(ts)) => ts.iter().any(|t| any_of.contains(t)),
                _ => false,
            },
            Expr::Range {
                field,
                lo,
                lo_incl,
                hi,
                hi_incl,
            } => match at(*field) {
                Some(Val::Num(x)) => {
                    (if *lo_incl { x >= lo } else { x > lo })
                        && (if *hi_incl { x <= hi } else { x < hi })
                }
                _ => false,
            },
            Expr::Phrase { field, words } => match at(*field) {
                Some(Val::Text(ts)) => ts.windows(words.len()).any(|w| w == words.as_slice()),
                _ => false,
            },
            Expr::Not(e) => !e.matches(vals),
            Expr::And(es) => es.iter().all(|e| e.matches(vals)),
            Expr::Or(es) => es.iter().any(|e| e.matches(vals)),
        }
    }
}

/// Parse a `FILTER` against a set's fields. The error is the text after
/// `ERR FILTER `.
pub fn parse(src: &[u8], fields: &[Field]) -> Result<Expr, String> {
    let mut p = Parser {
        s: src,
        i: 0,
        fields,
        depth: 0,
    };
    p.ws();
    if p.i == p.s.len() {
        return Err("is empty".into());
    }
    let e = p.union()?;
    p.ws();
    match p.peek() {
        None => Ok(e),
        Some(b')') => Err("has a ')' with no '(' before it".into()),
        Some(c) => Err(format!("has an unexpected '{}'", c as char)),
    }
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
    fields: &'a [Field],
    depth: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }
    fn ws(&mut self) {
        while self.peek().is_some_and(|c| c.is_ascii_whitespace()) {
            self.i += 1;
        }
    }
    fn deeper(&mut self) -> Result<(), String> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(format!("nests deeper than {MAX_DEPTH}"));
        }
        Ok(())
    }

    /// `inter ( '|' inter )*`
    fn union(&mut self) -> Result<Expr, String> {
        let (first, n) = self.inter()?;
        let mut branches = vec![(first, n)];
        loop {
            self.ws();
            if self.peek() != Some(b'|') {
                break;
            }
            self.i += 1;
            branches.push(self.inter()?);
        }
        if branches.len() == 1 {
            return Ok(branches.pop().map(|(e, _)| e).unwrap_or(Expr::All));
        }
        if branches.iter().any(|(_, n)| *n > 1) {
            return Err("mixes AND and OR without parentheses".into());
        }
        Ok(Expr::Or(branches.into_iter().map(|(e, _)| e).collect()))
    }

    /// `unary unary*`: the expression and how many terms it ANDs.
    fn inter(&mut self) -> Result<(Expr, usize), String> {
        let mut terms = Vec::new();
        loop {
            self.ws();
            match self.peek() {
                None | Some(b'|') | Some(b')') => break,
                _ => terms.push(self.unary()?),
            }
        }
        match terms.len() {
            0 => Err(match self.peek() {
                Some(b'|') => "has a '|' with nothing before it".into(),
                Some(b')') => "has '()' with nothing in it".into(),
                _ => "ends with a '|'".into(),
            }),
            1 => Ok((terms.pop().unwrap_or(Expr::All), 1)),
            n => Ok((Expr::And(terms), n)),
        }
    }

    fn unary(&mut self) -> Result<Expr, String> {
        if self.peek() == Some(b'-') {
            self.i += 1;
            self.deeper()?;
            let e = self.unary()?;
            self.depth -= 1;
            return Ok(Expr::Not(Box::new(e)));
        }
        self.atom()
    }

    fn atom(&mut self) -> Result<Expr, String> {
        match self.peek() {
            Some(b'(') => {
                self.i += 1;
                self.deeper()?;
                let e = self.union()?;
                self.ws();
                if self.peek() != Some(b')') {
                    return Err("has a '(' with no ')' after it".into());
                }
                self.i += 1;
                self.depth -= 1;
                Ok(e)
            }
            Some(b'*') => {
                self.i += 1;
                Ok(Expr::All)
            }
            Some(b'@') => {
                self.i += 1;
                self.field_term()
            }
            Some(b'$') => Err("parameters ($name) are not supported".into()),
            _ => Err(
                "supports only @field terms, '-', '(' and '*': full-text search is not supported"
                    .into(),
            ),
        }
    }

    fn field_term(&mut self) -> Result<Expr, String> {
        let start = self.i;
        while self
            .peek()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == b'_')
        {
            self.i += 1;
        }
        let name = &self.s[start..self.i];
        if name.is_empty() {
            return Err("has an '@' with no field name after it".into());
        }
        let name_s = String::from_utf8_lossy(name);
        let Some(field) = self.fields.iter().position(|f| f.name.as_bytes() == name) else {
            return Err(format!("names {name_s}, which is not a FIELD of this set"));
        };
        if self.peek() != Some(b':') {
            return Err(format!("needs a ':' after @{name_s}"));
        }
        self.i += 1;
        self.ws();
        let kind = self.fields[field].kind;
        let tag = matches!(kind, FieldKind::Tag | FieldKind::FoldedTag);
        match (self.peek(), kind) {
            (Some(b'{'), _) if tag => {
                self.i += 1;
                self.tags(field, kind == FieldKind::FoldedTag)
            }
            (Some(b'['), FieldKind::Numeric) => {
                self.i += 1;
                self.range(field)
            }
            (Some(b'(') | Some(b'"'), FieldKind::Text) => self.phrase(field),
            (Some(b'{'), FieldKind::Numeric) => Err(format!(
                "uses @{name_s}:{{...}}, but {name_s} is NUMERIC: use @{name_s}:[lo hi]"
            )),
            (Some(b'['), _) if tag => Err(format!(
                "uses @{name_s}:[...], but {name_s} is a TAG: use @{name_s}:{{a | b}}"
            )),
            (Some(b'{') | Some(b'['), FieldKind::Text) => Err(format!(
                "uses @{name_s} as a TAG or NUMERIC, but {name_s} is TEXT: use @{name_s}:(\"a phrase\")"
            )),
            (Some(b'(') | Some(b'"'), _) => {
                Err("full-text search is not supported: fields are TAG or NUMERIC".into())
            }
            _ => Err(format!("needs {{...}} or [...] after @{name_s}:")),
        }
    }

    /// At `("...")` or `"..."` after a TEXT field: the phrase, to its closing
    /// quote, and the `)` when it opened with `(`. Only a quoted phrase: an
    /// unquoted `(a b)` is RediSearch's AND of two words, with stemming,
    /// which is full-text search and not served.
    fn phrase(&mut self, field: usize) -> Result<Expr, String> {
        let paren = self.peek() == Some(b'(');
        if paren {
            self.i += 1;
            self.ws();
        }
        if self.peek() != Some(b'"') {
            return Err(
                "supports TEXT only as an exact phrase in quotes: @f:(\"a phrase\")".into(),
            );
        }
        self.i += 1;
        let mut text: Vec<u8> = Vec::new();
        loop {
            let Some(c) = self.peek() else {
                return Err("has a '\"' with no '\"' after it".into());
            };
            self.i += 1;
            match c {
                b'\\' => {
                    let Some(n) = self.peek() else {
                        return Err("ends with a '\\'".into());
                    };
                    self.i += 1;
                    text.push(n);
                }
                b'"' => break,
                c => text.push(c),
            }
        }
        if paren {
            self.ws();
            if self.peek() != Some(b')') {
                return Err("has a '(' with no ')' after it".into());
            }
            self.i += 1;
        }
        let words = tokenize(&text);
        if words.is_empty() {
            return Err("has an empty phrase".into());
        }
        Ok(Expr::Phrase { field, words })
    }

    /// After `{`: tags split by `|`, to the closing `}`, folded to lower case
    /// for a field whose values are (`fold`).
    fn tags(&mut self, field: usize, fold_case: bool) -> Result<Expr, String> {
        let mut any_of: Vec<Box<[u8]>> = Vec::new();
        let mut cur: Vec<u8> = Vec::new();
        // How much of `cur` ends in escaped bytes, which trimming keeps.
        let mut kept = 0usize;
        loop {
            let Some(c) = self.peek() else {
                return Err("has a '{' with no '}' after it".into());
            };
            self.i += 1;
            match c {
                b'\\' => {
                    let Some(n) = self.peek() else {
                        return Err("ends with a '\\'".into());
                    };
                    self.i += 1;
                    cur.push(n);
                    kept = cur.len();
                }
                b'|' | b'}' => {
                    while cur.len() > kept && cur.last().is_some_and(|b| b.is_ascii_whitespace()) {
                        cur.pop();
                    }
                    if cur.is_empty() {
                        return Err("has an empty tag".into());
                    }
                    let t = std::mem::take(&mut cur);
                    any_of.push(if fold_case {
                        fold(&t)
                    } else {
                        t.into_boxed_slice()
                    });
                    kept = 0;
                    if c == b'}' {
                        break;
                    }
                }
                b'*' => return Err("tag wildcards are not supported".into()),
                b'$' => return Err("parameters ($name) are not supported".into()),
                c if c.is_ascii_whitespace() && cur.is_empty() => {}
                c => cur.push(c),
            }
        }
        Ok(Expr::Tag { field, any_of })
    }

    /// After `[`: two bounds and the closing `]`.
    fn range(&mut self, field: usize) -> Result<Expr, String> {
        let mut toks: Vec<&[u8]> = Vec::new();
        loop {
            self.ws();
            match self.peek() {
                None => return Err("has a '[' with no ']' after it".into()),
                Some(b']') => {
                    self.i += 1;
                    break;
                }
                Some(_) => {
                    let start = self.i;
                    while self
                        .peek()
                        .is_some_and(|c| !c.is_ascii_whitespace() && c != b']')
                    {
                        self.i += 1;
                    }
                    toks.push(&self.s[start..self.i]);
                }
            }
        }
        match toks[..] {
            [lo, hi] => {
                let ((lo, lo_incl), (hi, hi_incl)) = (bound(lo)?, bound(hi)?);
                Ok(Expr::Range {
                    field,
                    lo,
                    lo_incl,
                    hi,
                    hi_incl,
                })
            }
            [_, _, _, ..] => {
                Err("has more than two bounds in [...]: geo filters are not supported".into())
            }
            _ => Err("needs two bounds in [lo hi]".into()),
        }
    }
}

/// One range bound: a number, `-inf`, `+inf` or `inf`, after an optional `(`
/// that makes it exclusive.
fn bound(tok: &[u8]) -> Result<(f64, bool), String> {
    let (tok, incl) = match tok.strip_prefix(b"(") {
        Some(t) => (t, false),
        None => (tok, true),
    };
    let s = std::str::from_utf8(tok).map_err(|_| "has a bound that is not a number".to_string())?;
    if s.starts_with('$') {
        return Err("parameters ($name) are not supported".into());
    }
    let x = match s.to_ascii_lowercase().as_str() {
        "-inf" => f64::NEG_INFINITY,
        "+inf" | "inf" => f64::INFINITY,
        l if l.contains("nan") => return Err("has a NaN bound".into()),
        _ => s
            .parse::<f64>()
            .map_err(|_| format!("has a bound that is not a number: {s:?}"))?,
    };
    Ok((x, incl))
}

// What the field index costs the D4 meter (`FieldIndex::bytes`), each for a
// container just past its growth, its worst case, and held to by
// `tests/meter.rs`.
/// A slot's `Option<Box<[Val]>>`.
const SLOT: usize = std::mem::size_of::<Option<Vals>>();
/// A tag value: its key and its set in its field's map (48 B a bucket, at 7/8
/// load and just doubled), and that set's own smallest table.
const TAG_VALUE: usize = 160;
/// A slot in a tag value's set: 4 B and a control byte, at 7/8 load and just
/// doubled.
const TAG_POSTING: usize = 12;
/// A numeric slot: a 16 B `(place, slot)` in B-tree nodes left half full by
/// splits.
const NUM_POSTING: usize = 40;

/// A number's place in an order that sorts as the numbers do, so a numeric
/// field's index is an ordered set of `(place, slot)`.
fn order_key(x: f64) -> u64 {
    let b = x.to_bits();
    if b >> 63 == 1 { !b } else { b | 1 << 63 }
}

/// An HNSW set's field index (ADR-0059 B): each live slot's values, a set of
/// slots for every value of every TAG field, and every NUMERIC field's slots
/// in value order. A search counts its matches from these before it starts,
/// to choose between scoring every match and a traversal that admits only
/// matches. A flat set scans every entry anyway, so it keeps values only.
pub struct FieldIndex {
    fields: Box<[Field]>,
    /// By slot; `None` for a slot whose vector holds no field, or that is
    /// deleted.
    vals: Vec<Option<Vals>>,
    /// By field; empty for a NUMERIC one.
    tags: Vec<HashMap<Box<[u8]>, SlotSet>>,
    /// By field; empty for a TAG one.
    nums: Vec<BTreeSet<(u64, u32)>>,
    /// What [`FieldIndex::bytes`] adds up, kept as values are put and
    /// cleared.
    vals_heap: usize,
    tag_values: usize,
    tag_value_bytes: usize,
    tag_postings: usize,
    num_postings: usize,
}

/// Where a search's candidates come from: a superset of its matches.
enum Source<'a> {
    Tag(&'a SlotSet),
    Range(std::collections::btree_set::Range<'a, (u64, u32)>),
}

impl FieldIndex {
    pub fn new(fields: Box<[Field]>) -> Self {
        let n = fields.len();
        FieldIndex {
            fields,
            vals: Vec::new(),
            tags: (0..n).map(|_| HashMap::new()).collect(),
            nums: (0..n).map(|_| BTreeSet::new()).collect(),
            vals_heap: 0,
            tag_values: 0,
            tag_value_bytes: 0,
            tag_postings: 0,
            num_postings: 0,
        }
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Index slot `slot`'s values from its `meta`. The slot holds nothing:
    /// it is new, or [`FieldIndex::clear`]ed when it was deleted.
    pub fn put(&mut self, slot: u32, meta: Option<&[u8]>) {
        let vals = extract(&self.fields, meta);
        let at = slot as usize;
        if at >= self.vals.len() {
            let extra = at + 1 - self.vals.len();
            crate::hnsw::grow(&mut self.vals, extra);
            self.vals.resize_with(at + 1, || None);
        }
        debug_assert!(
            self.vals[at].is_none(),
            "a slot is cleared before it is put"
        );
        let Some(v) = vals else {
            return;
        };
        for (f, val) in v.iter().enumerate() {
            match val {
                Val::Tags(ts) => {
                    for t in ts.iter() {
                        match self.tags[f].get_mut(t) {
                            Some(set) => {
                                set.insert(slot);
                            }
                            None => {
                                let mut set = SlotSet::default();
                                set.insert(slot);
                                self.tags[f].insert(t.clone(), set);
                                self.tag_values += 1;
                                self.tag_value_bytes += t.len();
                            }
                        }
                        self.tag_postings += 1;
                    }
                }
                Val::Num(x) => {
                    self.nums[f].insert((order_key(*x), slot));
                    self.num_postings += 1;
                }
                // A TEXT value has no index entry: a phrase is checked
                // against each candidate's words.
                Val::Missing | Val::Text(_) => {}
            }
        }
        self.vals_heap += vals_bytes(Some(&v));
        self.vals[at] = Some(v);
    }

    /// Take slot `slot` out of the index, when its vector is deleted.
    pub fn clear(&mut self, slot: u32) {
        let Some(v) = self.vals.get_mut(slot as usize).and_then(Option::take) else {
            return;
        };
        for (f, val) in v.iter().enumerate() {
            match val {
                Val::Tags(ts) => {
                    for t in ts.iter() {
                        if let Some(set) = self.tags[f].get_mut(t) {
                            set.remove(&slot);
                            self.tag_postings -= 1;
                            if set.is_empty() {
                                self.tags[f].remove(t);
                                self.tag_values -= 1;
                                self.tag_value_bytes -= t.len();
                            }
                        }
                    }
                }
                Val::Num(x) => {
                    if self.nums[f].remove(&(order_key(*x), slot)) {
                        self.num_postings -= 1;
                    }
                }
                // A TEXT value has no index entry: a phrase is checked
                // against each candidate's words.
                Val::Missing | Val::Text(_) => {}
            }
        }
        self.vals_heap -= vals_bytes(Some(&v));
    }

    pub fn matches(&self, e: &Expr, slot: u32) -> bool {
        e.matches(self.vals.get(slot as usize).and_then(|v| v.as_deref()))
    }

    /// What the index holds, for the D4 meter: an over-estimate of the heap,
    /// which `tests/meter.rs` holds it to. Each figure is for a container
    /// just past its growth, its worst case.
    pub fn bytes(&self) -> usize {
        // A slot's `Option<Box<[Val]>>`, with a quarter spare (`grow`).
        self.vals.capacity() * SLOT
            + self.vals_heap
            + self.tag_values * TAG_VALUE
            + self.tag_value_bytes
            + self.tag_postings * TAG_POSTING
            + self.num_postings * NUM_POSTING
    }

    /// The most a vector with `vals` can add to [`FieldIndex::bytes`]: its
    /// values, a slot, and a new tag value for each of its tags. For the cap
    /// check before a write, which cannot know which values are new.
    pub fn charge(vals: Option<&[Val]>) -> usize {
        let mut b = SLOT * 2 + vals_bytes(vals);
        for v in vals.unwrap_or_default() {
            match v {
                Val::Tags(ts) => {
                    b += ts
                        .iter()
                        .map(|t| t.len() + TAG_VALUE + TAG_POSTING)
                        .sum::<usize>()
                }
                Val::Num(_) => b += NUM_POSTING,
                // A TEXT value has no index entry: a phrase is checked
                // against each candidate's words.
                Val::Missing | Val::Text(_) => {}
            }
        }
        b
    }

    /// The live slots that match `e`, found from the index rather than by
    /// ranking: every one of them (`true`), or more than `cap` of them
    /// (`false`), at which the search should walk the graph instead.
    /// `deleted` has one entry a slot.
    pub fn matching(&self, e: &Expr, cap: usize, deleted: &[bool]) -> (Vec<u32>, bool) {
        let (out, all, _) = self.matching_checked(e, cap, deleted);
        (out, all)
    }

    /// [`matching`](Self::matching), and how many slots it checked against
    /// `e` one by one: none when the index entries it reads are exactly the
    /// matches (BUG-0267). Each check reads that slot's values, a cache miss
    /// on a large set, so a count of `cap` of them cost a filtered search
    /// with many matches as much again as its walk.
    fn matching_checked(&self, e: &Expr, cap: usize, deleted: &[bool]) -> (Vec<u32>, bool, usize) {
        let mut out = Vec::new();
        let mut checked = 0usize;
        match self.sources(e) {
            None => {
                for (slot, &dead) in deleted.iter().enumerate() {
                    let slot = slot as u32;
                    if dead {
                        continue;
                    }
                    checked += 1;
                    if self.matches(e, slot) {
                        out.push(slot);
                        if out.len() > cap {
                            return (out, false, checked);
                        }
                    }
                }
            }
            Some((sources, exact)) => {
                let several = sources.len() > 1;
                let mut seen = SlotSet::default();
                for src in sources {
                    let slots: Box<dyn Iterator<Item = u32>> = match src {
                        Source::Tag(set) => Box::new(set.iter().copied()),
                        Source::Range(r) => Box::new(r.map(|&(_, s)| s)),
                    };
                    for slot in slots {
                        if several && !seen.insert(slot) {
                            continue;
                        }
                        if deleted[slot as usize] {
                            continue;
                        }
                        if !exact {
                            checked += 1;
                            if !self.matches(e, slot) {
                                continue;
                            }
                        }
                        out.push(slot);
                        if out.len() > cap {
                            return (out, false, checked);
                        }
                    }
                }
            }
        }
        (out, true, checked)
    }

    /// Index entries whose slots include every match of `e`, and whether
    /// they are exactly its matches, so that a slot read from them needs no
    /// check against `e`: a TAG term, a range, or an OR of those. `None`
    /// when only a scan of every slot can say (`*`, a negation, a TEXT
    /// phrase, or an OR with one of them in it).
    fn sources(&self, e: &Expr) -> Option<(Vec<Source<'_>>, bool)> {
        match e {
            Expr::All | Expr::Not(_) | Expr::Phrase { .. } => None,
            Expr::Tag { field, any_of } => Some((
                any_of
                    .iter()
                    .filter_map(|t| self.tags[*field].get(t))
                    .map(Source::Tag)
                    .collect(),
                true,
            )),
            Expr::Range {
                field,
                lo,
                lo_incl,
                hi,
                hi_incl,
            } => {
                // `-0` is stored as `0`, so a bound of `-0` is one of `0`:
                // `(-0` must leave out the zeros.
                let zero = |x: f64| if x == 0.0 { 0.0 } else { x };
                let (lo, hi) = (zero(*lo), zero(*hi));
                // Empty, and a range the B-tree would refuse: its start past
                // its end, as `(5 (5` makes.
                if lo > hi || (lo == hi && !(*lo_incl && *hi_incl)) {
                    return Some((Vec::new(), true));
                }
                let from = if *lo_incl {
                    Bound::Included((order_key(lo), 0))
                } else {
                    Bound::Excluded((order_key(lo), u32::MAX))
                };
                let to = if *hi_incl {
                    Bound::Included((order_key(hi), u32::MAX))
                } else {
                    Bound::Excluded((order_key(hi), 0))
                };
                Some((
                    vec![Source::Range(self.nums[*field].range((from, to)))],
                    true,
                ))
            }
            // Any one term's entries hold every match: the one with the
            // fewest known slots, where a range counts as unknown. The other
            // terms still have to be checked, unless there are none.
            Expr::And(es) => {
                let (srcs, exact) =
                    es.iter()
                        .filter_map(|x| self.sources(x))
                        .min_by_key(|(srcs, _)| {
                            srcs.iter()
                                .map(|s| match s {
                                    Source::Tag(set) => set.len(),
                                    Source::Range(_) => usize::MAX / 2,
                                })
                                .fold(0usize, usize::saturating_add)
                        })?;
                Some((srcs, exact && es.len() == 1))
            }
            Expr::Or(es) => {
                let mut all = Vec::new();
                let mut exact = true;
                for x in es {
                    let (srcs, e) = self.sources(x)?;
                    all.extend(srcs);
                    exact &= e;
                }
                Some((all, exact))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields() -> Vec<Field> {
        vec![
            Field {
                name: "folder".into(),
                kind: FieldKind::Tag,
            },
            Field {
                name: "price".into(),
                kind: FieldKind::Numeric,
            },
        ]
    }

    fn vals(json: &str) -> Option<Vals> {
        extract(&fields(), Some(json.as_bytes()))
    }

    fn hit(filter: &str, json: &str) -> bool {
        parse(filter.as_bytes(), &fields())
            .unwrap_or_else(|e| panic!("{filter}: {e}"))
            .matches(vals(json).as_deref())
    }

    fn refused(filter: &str) -> String {
        match parse(filter.as_bytes(), &fields()) {
            Ok(e) => panic!("{filter} parsed as {e:?}"),
            Err(e) => e,
        }
    }

    #[test]
    fn values_come_from_a_json_object_only() {
        assert_eq!(
            vals(r#"{"folder":"a","price":3}"#).map(|v| v.len()),
            Some(2)
        );
        assert_eq!(vals("not json"), None);
        assert_eq!(vals("[1,2]"), None);
        assert_eq!(vals(r#"{"other":1}"#), None);
        assert_eq!(extract(&fields(), None), None);
        assert_eq!(extract(&[], Some(br#"{"folder":"a"}"#)), None);
        // The wrong kind is missing, not an error.
        assert_eq!(
            vals(r#"{"folder":7,"price":"cheap"}"#),
            None,
            "a number in a TAG and a string in a NUMERIC hold nothing"
        );
        assert_eq!(
            vals(r#"{"folder":["b","a","b",3]}"#).as_deref(),
            Some(
                &[
                    Val::Tags(Box::new([b"a"[..].into(), b"b"[..].into()])),
                    Val::Missing
                ][..]
            )
        );
        assert_eq!(
            vals(r#"{"price":-0.0}"#).as_deref(),
            Some(&[Val::Missing, Val::Num(0.0)][..])
        );
    }

    #[test]
    fn every_operator_matches_as_redisearch_reads_it() {
        let doc = r#"{"folder":["contracts","2026"],"price":42}"#;
        assert!(hit("@folder:{contracts}", doc));
        assert!(hit("@folder:{invoices | contracts}", doc));
        assert!(!hit("@folder:{invoices}", doc));
        assert!(!hit("@folder:{Contracts}", doc), "tags keep their case");
        assert!(hit("@price:[42 42]", doc));
        assert!(hit("@price:[10 50]", doc));
        assert!(!hit("@price:[(42 50]", doc));
        assert!(!hit("@price:[10 (42]", doc));
        assert!(hit("@price:[-inf +inf]", doc));
        assert!(hit("@price:[(41.5 inf]", doc));
        assert!(
            !hit("@price:[50 10]", doc),
            "an empty range matches nothing"
        );
        assert!(hit("@folder:{contracts} @price:[40 45]", doc));
        assert!(!hit("@folder:{contracts} @price:[0 10]", doc));
        assert!(hit("@folder:{x} | @price:[40 45]", doc));
        assert!(hit("(@folder:{x} @price:[0 1]) | @folder:{2026}", doc));
        assert!(hit("-@folder:{invoices}", doc));
        assert!(!hit("-@folder:{contracts}", doc));
        assert!(hit("--@folder:{contracts}", doc));
        assert!(hit("*", doc));
        assert!(hit("(-@price:[0 10])", doc));
        // A missing field matches no term, so its negation matches.
        assert!(!hit("@folder:{contracts}", r#"{"price":1}"#));
        assert!(hit("-@folder:{contracts}", r#"{"price":1}"#));
        assert!(hit("-@price:[0 100]", "not json at all"));
        assert!(!hit("@price:[-inf +inf]", "not json at all"));
    }

    #[test]
    fn redisvl_s_own_renderings_parse() {
        // As `redisvl` 0.28 writes them (query/filter.py): every combination
        // parenthesized, negation as `(-...)`, punctuation escaped.
        let doc = r#"{"folder":["a b","x-y"],"price":5}"#;
        assert!(hit(r"@folder:{a\ b}", doc));
        assert!(hit(r"@folder:{x\-y}", doc));
        assert!(hit(r"(@folder:{a\ b} @price:[(4 +inf])", doc));
        assert!(hit(
            r"((@folder:{zz} | @folder:{x\-y}) @price:[-inf 5])",
            doc
        ));
        assert!(!hit(r"(-@folder:{x\-y})", doc));
        assert!(hit(r"@folder:{a\ b|zz}", doc));
        // Blanks at a tag's ends go; one escaped at its end stays.
        assert!(hit("@folder:{  a b  }", doc));
        assert!(!hit(r"@folder:{a b\ }", doc));
    }

    #[test]
    fn what_this_does_not_serve_is_refused_out_loud() {
        assert!(refused("").contains("empty"));
        assert!(refused("@nope:{a}").contains("not a FIELD"));
        assert!(refused("@price:{a}").contains("NUMERIC"));
        assert!(refused("@folder:[1 2]").contains("TAG"));
        assert!(refused("@folder:{a} @price:[1 2] | @folder:{b}").contains("mixes AND and OR"));
        assert!(refused("@folder:{a} | @folder:{b} @price:[1 2]").contains("mixes AND and OR"));
        assert!(refused("@folder:{a*}").contains("wildcards"));
        assert!(refused("@folder:{$p}").contains("parameters"));
        assert!(refused("@price:[$lo 2]").contains("parameters"));
        assert!(refused("@price:[nan 2]").contains("NaN"));
        assert!(refused("@price:[1 NaN]").contains("NaN"));
        assert!(refused("@price:[1 2 3 km]").contains("geo"));
        assert!(refused("@price:[1]").contains("two bounds"));
        assert!(refused("@price:[one 2]").contains("not a number"));
        assert!(refused("@folder:{}").contains("empty tag"));
        assert!(refused("@folder:{a||b}").contains("empty tag"));
        assert!(refused("@folder:{a").contains("no '}'"));
        assert!(refused("@price:[1 2").contains("no ']'"));
        assert!(refused("(@folder:{a}").contains("no ')'"));
        assert!(refused("@folder:{a})").contains("no '('"));
        assert!(refused("()").contains("nothing in it"));
        assert!(refused("@folder:{a} |").contains("ends with"));
        assert!(refused("| @folder:{a}").contains("nothing before"));
        assert!(refused("hello").contains("full-text"));
        assert!(refused("@folder:(hello)").contains("full-text"));
        assert!(refused("@folder").contains("':'"));
        assert!(refused("@:{a}").contains("no field name"));
        assert!(refused(r"@folder:{a\").contains("'\\'"));
        let deep = format!("{}@folder:{{a}}{}", "(".repeat(40), ")".repeat(40));
        assert!(refused(&deep).contains("nests"));
        assert!(refused(&format!("{}@folder:{{a}}", "-".repeat(40))).contains("nests"));
        let ok = format!("{}@folder:{{a}}{}", "(".repeat(30), ")".repeat(30));
        assert!(parse(ok.as_bytes(), &fields()).is_ok());
    }

    #[test]
    fn the_config_field_round_trips_and_reads_past_what_it_does_not_know() {
        let f = fields();
        assert_eq!(encode_fields(&f), "folder:tag,price:numeric");
        assert_eq!(decode_fields(&encode_fields(&f)), f);
        assert_eq!(
            decode_fields("folder:tag,where:geo,price:numeric,bad name:tag,folder:numeric"),
            f,
            "an unknown kind, a bad name and a repeat are left out"
        );
        assert!(valid_name(b"a_B9"));
        assert!(!valid_name(b"a-b"));
        assert!(!valid_name(b""));
        assert!(!valid_name(&[b'a'; MAX_NAME + 1]));
    }

    /// BUG-0267: a filter the index entries answer exactly is not checked
    /// slot by slot, whether it is under the cap or past it; one they only
    /// bound still is.
    #[test]
    fn exact_entries_are_not_checked_one_by_one() {
        let mut fi = FieldIndex::new(fields().into_boxed_slice());
        let n = 2_000u32;
        for i in 0..n {
            let doc = format!(r#"{{"folder":["f{}","all"],"price":{}}}"#, i % 4, i);
            fi.put(i, Some(doc.as_bytes()));
        }
        let deleted = vec![false; n as usize];
        let run = |f: &str, cap: usize| {
            let e = parse(f.as_bytes(), fi.fields()).expect(f);
            let (mut s, all, checked) = fi.matching_checked(&e, cap, &deleted);
            s.sort();
            (s, all, checked)
        };
        for f in [
            "@folder:{f1}",
            "@folder:{f1|f2}",
            "@folder:{f1} | @price:[0 9]",
            "@price:[100 199]",
            "(@price:[100 199])",
        ] {
            let (_, all, checked) = run(f, 10);
            assert!(!all, "{f}: past the cap");
            assert_eq!(checked, 0, "{f}: past the cap, none checked");
            let (_, all, checked) = run(f, 5_000);
            assert!(all, "{f}: under the cap");
            assert_eq!(checked, 0, "{f}: under the cap, none checked");
        }
        // Each still finds exactly its matches.
        assert_eq!(run("@price:[(10 (13]", 100).0, vec![11, 12]);
        assert_eq!(run("@price:[(10 13]", 100).0, vec![11, 12, 13]);
        assert_eq!(run("@price:[10 (13]", 100).0, vec![10, 11, 12]);
        assert_eq!(run("@price:[(5 (5]", 100).0, Vec::<u32>::new());
        assert_eq!(run("@price:[(5 5]", 100).0, Vec::<u32>::new());
        assert_eq!(run("@price:[5 5]", 100).0, vec![5]);
        assert_eq!(run("@price:[(-0 1]", 100).0, vec![1], "(-0 leaves out 0");
        assert_eq!(run("@price:[-inf (2]", 100).0, vec![0, 1]);
        assert_eq!(
            run("@folder:{f1|all}", 5_000).0.len(),
            n as usize,
            "a slot under two of a TAG's values is counted once"
        );
        // An AND's other terms are checked, on the smallest term's entries.
        let (s, all, checked) = run("@folder:{f1} @price:[0 99]", 5_000);
        assert!(all);
        assert_eq!(s.len(), 25);
        assert_eq!(
            checked, 500,
            "the f1 entries, each checked against the range"
        );
    }

    #[test]
    fn the_index_finds_every_match_or_says_there_are_too_many() {
        let mut fi = FieldIndex::new(fields().into_boxed_slice());
        let docs = [
            r#"{"folder":"a","price":1}"#,
            r#"{"folder":["a","b"],"price":2}"#,
            r#"{"folder":"b","price":3}"#,
            r#"{"price":4}"#,
            "not json",
        ];
        for (i, d) in docs.iter().enumerate() {
            fi.put(i as u32, Some(d.as_bytes()));
        }
        let deleted = vec![false; docs.len()];
        let m = |fi: &FieldIndex, f: &str, cap: usize, deleted: &[bool]| {
            let e = parse(f.as_bytes(), fi.fields()).expect(f);
            let (mut s, complete) = fi.matching(&e, cap, deleted);
            s.sort();
            (s, complete)
        };
        assert_eq!(m(&fi, "@folder:{a}", 10, &deleted), (vec![0, 1], true));
        assert_eq!(m(&fi, "@folder:{a|b}", 10, &deleted), (vec![0, 1, 2], true));
        assert_eq!(
            m(&fi, "@folder:{a} | @folder:{b}", 10, &deleted),
            (vec![0, 1, 2], true)
        );
        assert_eq!(m(&fi, "@price:[2 3]", 10, &deleted), (vec![1, 2], true));
        assert_eq!(m(&fi, "@price:[(1 (4]", 10, &deleted), (vec![1, 2], true));
        assert_eq!(
            m(&fi, "@folder:{a} @price:[2 9]", 10, &deleted),
            (vec![1], true)
        );
        assert_eq!(m(&fi, "-@folder:{a}", 10, &deleted), (vec![2, 3, 4], true));
        assert_eq!(m(&fi, "@folder:{zz}", 10, &deleted), (vec![], true));
        assert_eq!(m(&fi, "*", 10, &deleted), (vec![0, 1, 2, 3, 4], true));
        assert!(!m(&fi, "*", 2, &deleted).1, "past the cap it stops");
        assert!(!m(&fi, "@folder:{a|b}", 2, &deleted).1);
        let mut gone = deleted.clone();
        gone[0] = true;
        fi.clear(0);
        assert_eq!(m(&fi, "@folder:{a}", 10, &gone), (vec![1], true));
        assert_eq!(m(&fi, "-@folder:{b}", 10, &gone), (vec![3, 4], true));
        // Every slot cleared leaves nothing behind but the slots themselves.
        for i in 1..docs.len() as u32 {
            fi.clear(i);
        }
        assert_eq!(
            (
                fi.vals_heap,
                fi.tag_values,
                fi.tag_value_bytes,
                fi.tag_postings,
                fi.num_postings
            ),
            (0, 0, 0, 0, 0)
        );
        assert!(fi.tags.iter().all(HashMap::is_empty) && fi.nums.iter().all(BTreeSet::is_empty));
    }
}
