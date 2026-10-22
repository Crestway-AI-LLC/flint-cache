// SPDX-License-Identifier: Elastic-2.0
//! RediSearch's query language over an `FT.*` index, full text included
//! (ADR-0063): `DIALECT 2` as RediSearch's lexer and parser read it, each
//! term matched against the index's postings, and each match scored as
//! RediSearch's `default.c` scores it. TAG, NUMERIC and GEO terms are
//! ADR-0059's and ADR-0062's, read by `filter.rs`.
//!
//! What follows RediSearch, read from its source (written here from those
//! rules, not from its code):
//! - **Precedence:** AND binds tighter than OR; `-` and `~` take the term
//!   after them; `=>{...}` the term or group before it; `@f:` the term or
//!   group after it.
//! - **A word** becomes the union of itself, `+stem` and its bare stem,
//!   unless every field it searches is `NOSTEM`; a word that is a stop word
//!   is dropped. A quoted phrase is neither.
//! - **Prefix, suffix, contains and fuzzy** terms read at most 200 words
//!   from the index's word list, in byte order, and match a document by the
//!   first of them it holds.
//! - **Each match is a tree** of term results, unions, intersections and
//!   virtual results, which each scorer sums as RediSearch's does.

use crate::filter::{self, Expr, Field, FieldKind, Val};
use crate::fulltext::{DocId, Hit, STEM_PREFIX, StopWords, TextIndex};
use crate::snowball;
use std::collections::BTreeMap;

/// The most words a prefix, suffix, contains or fuzzy term reads
/// (`DEFAULT_MAX_PREFIX_EXPANSIONS`).
pub const MAX_EXPANSIONS: usize = 200;
/// The shortest prefix, suffix or infix that matches anything
/// (`DEFAULT_MIN_TERM_PREFIX`): a shorter one matches nothing.
pub const MIN_AFFIX: usize = 2;
/// Every field: a term with no `@field:` searches every TEXT field.
pub const ALL: u32 = u32::MAX;
/// How deep a query nests: without a bound a parse would recurse without
/// one on a tenant's input.
const MAX_DEPTH: usize = 32;

/// A parsed query: a node, its weight and, for an AND, its proximity.
#[derive(Clone, Debug, PartialEq)]
pub struct QNode {
    pub node: Node,
    /// `$weight`: 1 unless set.
    pub weight: f64,
    /// `$slop`: the most words allowed between an AND's terms.
    pub slop: Option<u32>,
    /// `$inorder`: an AND's terms in the order written.
    pub inorder: bool,
}

impl QNode {
    fn new(node: Node) -> QNode {
        QNode {
            node,
            weight: 1.0,
            slop: None,
            inorder: false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Node {
    /// `*`: every document.
    All,
    /// A TAG, NUMERIC or GEO term, as ADR-0059's filter reads it.
    Leaf(Expr),
    /// A word, lowercased, on the TEXT fields of `mask`.
    Word {
        mask: u32,
        word: Box<[u8]>,
    },
    /// `pat*`.
    Prefix {
        mask: u32,
        pat: Box<[u8]>,
    },
    /// `*pat`.
    Suffix {
        mask: u32,
        pat: Box<[u8]>,
    },
    /// `*pat*`.
    Contains {
        mask: u32,
        pat: Box<[u8]>,
    },
    /// `%word%`, `%%word%%` or `%%%word%%%`: within `dist` edits.
    Fuzzy {
        mask: u32,
        word: Box<[u8]>,
        dist: u8,
    },
    /// `"a phrase"`: its words, in order, nothing between, not stemmed.
    Phrase {
        mask: u32,
        words: Vec<Box<[u8]>>,
    },
    And(Vec<QNode>),
    Or(Vec<QNode>),
    Not(Box<QNode>),
    /// `~x`: matches with or without `x`, which only adds to the score.
    Optional(Box<QNode>),
}

impl QNode {
    /// The query as ADR-0059's filter, when it has no text term: what a
    /// filtered KNN hands its vector set. `None` when it has one.
    pub fn as_filter(&self) -> Option<Expr> {
        Some(match &self.node {
            Node::All => Expr::All,
            Node::Leaf(e) => e.clone(),
            Node::And(xs) => Expr::And(xs.iter().map(QNode::as_filter).collect::<Option<_>>()?),
            Node::Or(xs) => Expr::Or(xs.iter().map(QNode::as_filter).collect::<Option<_>>()?),
            Node::Not(x) => Expr::Not(Box::new(x.as_filter()?)),
            // Matches with or without its term.
            Node::Optional(x) => {
                x.as_filter()?;
                Expr::All
            }
            _ => return None,
        })
    }

    /// Whether any term in it is on a field of `noindex`, replaced by one
    /// that matches nothing: a NOINDEX field is returned and sorted on,
    /// never searched (ADR-0060).
    pub fn unindexed(self, noindex: &[usize]) -> QNode {
        let node = match self.node {
            Node::Leaf(e) if leaf_field(&e).is_some_and(|f| noindex.contains(&f)) => {
                Node::Not(Box::new(QNode::new(Node::All)))
            }
            Node::And(xs) => Node::And(xs.into_iter().map(|x| x.unindexed(noindex)).collect()),
            Node::Or(xs) => Node::Or(xs.into_iter().map(|x| x.unindexed(noindex)).collect()),
            Node::Not(x) => Node::Not(Box::new(x.unindexed(noindex))),
            Node::Optional(x) => Node::Optional(Box::new(x.unindexed(noindex))),
            other => other,
        };
        QNode { node, ..self }
    }
}

fn leaf_field(e: &Expr) -> Option<usize> {
    match e {
        Expr::Tag { field, .. }
        | Expr::Range { field, .. }
        | Expr::Phrase { field, .. }
        | Expr::Geo { field, .. } => Some(*field),
        _ => None,
    }
}

/// Parse a query against an index's fields. `None` when every word in it is
/// a stop word, which RediSearch answers with no documents. The error is the
/// text after `ERR the query `.
pub fn parse(src: &[u8], fields: &[Field], stop: &StopWords) -> Result<Option<QNode>, String> {
    let mut p = Parser {
        s: src,
        i: 0,
        fields,
        stop,
        depth: 0,
    };
    p.skip();
    if p.i == p.s.len() {
        return Err("is empty".into());
    }
    let q = p.union(ALL)?;
    p.skip();
    match p.peek() {
        None => Ok(q),
        Some(b')') => Err("has a ')' with no '(' before it".into()),
        Some(c) => Err(format!("has an unexpected '{}'", c as char)),
    }
}

/// Whether `c` is C's `ispunct`.
fn punct(c: u8) -> bool {
    c.is_ascii_punctuation()
}

/// Whether `c` is Ragel's `space`: C's `isspace`.
fn space(c: u8) -> bool {
    matches!(c, b' ' | b'\t'..=b'\r')
}

/// The bytes RediSearch's lexer makes a token of; any other punctuation,
/// space or control byte outside a token it passes over.
const TOKEN_PUNCT: &[u8] = b"|(){}[]:;-~*%\"'@$\\_";

/// A query word as RediSearch's parser normalizes it (`rm_normalize`): a
/// backslash dropped before punctuation or whitespace, then lowercased.
fn normalize(raw: &[u8]) -> Box<[u8]> {
    filter::normalize_query_word(raw)
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
    fields: &'a [Field],
    stop: &'a StopWords,
    depth: usize,
}

impl Parser<'_> {
    fn peek(&self) -> Option<u8> {
        self.s.get(self.i).copied()
    }
    fn at(&self, k: usize) -> Option<u8> {
        self.s.get(k).copied()
    }

    /// Past what RediSearch's lexer skips: whitespace, control bytes, and
    /// punctuation that starts no token.
    fn skip(&mut self) {
        while let Some(c) = self.peek() {
            let skipped = space(c)
                || c.is_ascii_control()
                || (punct(c) && !TOKEN_PUNCT.contains(&c) && !self.token_at(self.i))
                || (c == b'\\' && !self.at(self.i + 1).is_some_and(|n| punct(n) || space(n)));
            if !skipped {
                break;
            }
            self.i += 1;
        }
    }

    /// Whether a punctuation byte at `k` starts a token all the same: `=>`,
    /// or a number's sign or point.
    fn token_at(&self, k: usize) -> bool {
        match self.at(k) {
            Some(b'=') => self.at(k + 1) == Some(b'>'),
            Some(b'+') | Some(b'.') => self.number_len(k) > 0,
            _ => false,
        }
    }

    fn deeper(&mut self) -> Result<(), String> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(format!("nests deeper than {MAX_DEPTH}"));
        }
        Ok(())
    }

    fn digits(&self, k: usize) -> usize {
        self.s[k.min(self.s.len())..]
            .iter()
            .take_while(|c| c.is_ascii_digit())
            .count()
    }

    /// The length of the number RediSearch's lexer reads at `k`, or 0:
    /// `[+-]? (d+ ('.' d+)? | '.' d+ | d+ '.') ([eE] [+-]? d+)?`.
    fn number_len(&self, k: usize) -> usize {
        let mut j = k;
        if matches!(self.at(j), Some(b'+') | Some(b'-')) {
            j += 1;
        }
        let int = self.digits(j);
        if int > 0 {
            j += int;
            if self.at(j) == Some(b'.') {
                j += 1 + self.digits(j + 1);
            }
        } else if self.at(j) == Some(b'.') && self.digits(j + 1) > 0 {
            j += 1 + self.digits(j + 1);
        } else {
            return 0;
        }
        if matches!(self.at(j), Some(b'e') | Some(b'E')) {
            let mut e = j + 1;
            if matches!(self.at(e), Some(b'+') | Some(b'-')) {
                e += 1;
            }
            let d = self.digits(e);
            if d > 0 {
                j = e + d;
            }
        }
        j - k
    }

    /// The length of the term RediSearch's lexer reads at `k`: bytes that are
    /// not punctuation, control or space, `_`, and escaped characters.
    fn term_len(&self, k: usize) -> usize {
        let mut j = k;
        while let Some(c) = self.at(j) {
            if c >= 0x80 || c.is_ascii_alphanumeric() || c == b'_' {
                j += 1;
            } else if c == b'\\'
                && self
                    .at(j + 1)
                    .is_some_and(|n| punct(n) || space(n) || n == b'\\')
            {
                j += 2;
            } else {
                break;
            }
        }
        j - k
    }

    /// The word token at the cursor, raw: a term or a number, the longer of
    /// the two, a number when they tie, as RediSearch's lexer chooses.
    fn word_token(&mut self) -> Option<&[u8]> {
        let (n, t) = (self.number_len(self.i), self.term_len(self.i));
        let len = n.max(t);
        if len == 0 {
            return None;
        }
        let start = self.i;
        self.i += len;
        Some(&self.s[start..start + len])
    }

    /// `and ( '|' and )*`.
    fn union(&mut self, mask: u32) -> Result<Option<QNode>, String> {
        let mut branches = vec![self.and(mask)?];
        loop {
            self.skip();
            if self.peek() != Some(b'|') {
                break;
            }
            self.i += 1;
            branches.push(self.and(mask)?);
        }
        let mut kept: Vec<QNode> = branches.into_iter().flatten().collect();
        Ok(match kept.len() {
            0 => None,
            1 => kept.pop(),
            _ => Some(QNode::new(Node::Or(kept))),
        })
    }

    /// `unary+`, up to a `|`, a `)` or the end.
    fn and(&mut self, mask: u32) -> Result<Option<QNode>, String> {
        let mut terms = Vec::new();
        let mut seen = 0;
        loop {
            self.skip();
            match self.peek() {
                None | Some(b'|') | Some(b')') => break,
                _ => {
                    seen += 1;
                    if let Some(t) = self.unary(mask)? {
                        terms.push(t);
                    }
                }
            }
        }
        if seen == 0 {
            return Err(match self.peek() {
                Some(b'|') => "has a '|' with nothing before it".into(),
                Some(b')') => "has '()' with nothing in it".into(),
                _ => "ends with a '|'".into(),
            });
        }
        Ok(match terms.len() {
            0 => None,
            1 => terms.pop(),
            _ => Some(QNode::new(Node::And(terms))),
        })
    }

    fn unary(&mut self, mask: u32) -> Result<Option<QNode>, String> {
        match self.peek() {
            Some(b'-') if self.number_len(self.i) == 0 => {
                self.i += 1;
                self.deeper()?;
                self.skip();
                let x = self.unary(mask)?;
                self.depth -= 1;
                Ok(x.map(|x| QNode::new(Node::Not(Box::new(x)))))
            }
            Some(b'~') => {
                self.i += 1;
                self.deeper()?;
                self.skip();
                let x = self.unary(mask)?;
                self.depth -= 1;
                Ok(x.map(|x| QNode::new(Node::Optional(Box::new(x)))))
            }
            _ => self.postfix(mask),
        }
    }

    /// An atom, and the attributes after it: `=>{$weight: w; ...}`.
    fn postfix(&mut self, mask: u32) -> Result<Option<QNode>, String> {
        let mut x = self.atom(mask)?;
        loop {
            let save = self.i;
            self.skip();
            if !self.s[self.i..].starts_with(b"=>") {
                self.i = save;
                break;
            }
            self.i += 2;
            self.skip();
            if self.peek() != Some(b'{') {
                return Err(
                    "has '=>' with no attributes after it: a KNN clause ends the query".into(),
                );
            }
            self.i += 1;
            self.attributes(x.as_mut())?;
        }
        Ok(x)
    }

    /// After `{`: `$name: value` pairs split by `;`, to the closing `}`.
    fn attributes(&mut self, x: Option<&mut QNode>) -> Result<(), String> {
        let close = self.s[self.i..]
            .iter()
            .position(|&c| c == b'}')
            .ok_or("has a '{' with no '}' after it")?;
        let body = std::str::from_utf8(&self.s[self.i..self.i + close])
            .map_err(|_| "has attributes that are not UTF-8".to_string())?;
        self.i += close + 1;
        let mut out = x;
        for a in body.split(';').map(str::trim).filter(|a| !a.is_empty()) {
            let (k, v) = a
                .split_once(':')
                .ok_or_else(|| format!("has an attribute {a} that is not $name: value"))?;
            let (k, v) = (k.trim().to_ascii_lowercase(), v.trim());
            if !matches!(k.as_str(), "$weight" | "$slop" | "$inorder") {
                return Err(format!(
                    "has the attribute {k}, which is not supported (ADR-0063)"
                ));
            }
            // A stop word's attributes go with it.
            let Some(x) = out.as_deref_mut() else {
                continue;
            };
            match k.as_str() {
                "$weight" => {
                    x.weight = v
                        .parse::<f64>()
                        .ok()
                        .filter(|w| w.is_finite() && *w >= 0.0)
                        .ok_or("has a $weight that is not a number of 0 or more")?;
                }
                "$slop" => {
                    let n: i64 = v
                        .parse()
                        .map_err(|_| "has a $slop that is not an integer")?;
                    if n < -1 {
                        return Err("has a $slop below -1".into());
                    }
                    x.slop = u32::try_from(n).ok();
                }
                "$inorder" => {
                    x.inorder = match v.to_ascii_lowercase().as_str() {
                        "true" => true,
                        "false" => false,
                        _ => return Err("has an $inorder that is not true or false".into()),
                    };
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn atom(&mut self, mask: u32) -> Result<Option<QNode>, String> {
        match self.peek() {
            Some(b'(') => {
                self.i += 1;
                self.deeper()?;
                let x = self.union(mask)?;
                self.skip();
                if self.peek() != Some(b')') {
                    return Err("has a '(' with no ')' after it".into());
                }
                self.i += 1;
                self.depth -= 1;
                Ok(x)
            }
            Some(b'@') => {
                self.i += 1;
                self.field_term(mask)
            }
            Some(b'$') => Err(
                "names a parameter ($name) in a text term, which is not supported \
                               (ADR-0063)"
                    .into(),
            ),
            _ => self.text_atom(mask),
        }
    }

    /// A word, a prefix, suffix, contains or fuzzy term, a quoted phrase, or
    /// `*`, on the fields of `mask`.
    fn text_atom(&mut self, mask: u32) -> Result<Option<QNode>, String> {
        let leaf = |n| Ok(Some(QNode::new(n)));
        match self.peek() {
            Some(b'"') | Some(b'\'') => {
                let words = self.phrase()?;
                leaf(Node::Phrase { mask, words })
            }
            Some(b'*') => {
                self.i += 1;
                let Some(raw) = self.word_token() else {
                    return leaf(Node::All);
                };
                let pat = normalize(raw);
                if self.peek() == Some(b'*') {
                    self.i += 1;
                    leaf(Node::Contains { mask, pat })
                } else {
                    leaf(Node::Suffix { mask, pat })
                }
            }
            Some(b'%') => {
                let dist = self.s[self.i..].iter().take_while(|&&c| c == b'%').count();
                if dist > 3 {
                    return Err("has a fuzzy term with more than 3 '%'".into());
                }
                self.i += dist;
                let word = normalize(self.word_token().ok_or("has a '%' with no word after it")?);
                let closing = self.s[self.i..].iter().take_while(|&&c| c == b'%').count();
                if closing < dist {
                    return Err("has a fuzzy term with no closing '%'".into());
                }
                self.i += dist;
                leaf(Node::Fuzzy {
                    mask,
                    word,
                    dist: dist as u8,
                })
            }
            _ => {
                let Some(raw) = self.word_token() else {
                    return Err(match self.peek() {
                        Some(c) => format!("has an unexpected '{}'", c as char),
                        None => "ends early".into(),
                    });
                };
                let word = normalize(raw);
                if self.peek() == Some(b'*') {
                    self.i += 1;
                    return leaf(Node::Prefix { mask, pat: word });
                }
                // A stop word is dropped, as RediSearch's parser drops it.
                if self.stop.contains(&word) {
                    return Ok(None);
                }
                leaf(Node::Word { mask, word })
            }
        }
    }

    /// At `"` or `'`: the phrase's words, cut and normalized as RediSearch's
    /// parser cuts a quoted phrase (BUG-0272). Stop words are kept.
    fn phrase(&mut self) -> Result<Vec<Box<[u8]>>, String> {
        let quote = self.peek().unwrap_or(b'"');
        self.i += 1;
        let start = self.i;
        loop {
            match self.peek() {
                None => return Err("has a quote with no closing quote".into()),
                Some(b'\\') if self.at(self.i + 1).is_some() => self.i += 2,
                Some(c) if c == quote => break,
                Some(_) => self.i += 1,
            }
        }
        let raw = &self.s[start..self.i];
        self.i += 1;
        if raw.is_empty() {
            return Err("has an empty phrase, which is not supported (ADR-0063)".into());
        }
        Ok(filter::phrase_words(raw))
    }

    /// After `@`: a TAG, NUMERIC or GEO term, handed to ADR-0059's parser,
    /// or the TEXT term or group after `@f:` or `@f1|f2:`.
    fn field_term(&mut self, mask: u32) -> Result<Option<QNode>, String> {
        let at = self.i;
        let name = |p: &Parser, k: usize| -> usize {
            p.s[k..]
                .iter()
                .take_while(|c| c.is_ascii_alphanumeric() || **c == b'_')
                .count()
        };
        let first = name(self, at);
        if first == 0 {
            return Err("has an '@' with no field name after it".into());
        }
        let find = |n: &[u8]| self.fields.iter().position(|f| f.name.as_bytes() == n);
        let mut names = vec![(at, first)];
        let mut j = at + first;
        while self.at(j) == Some(b'|') {
            let n = name(self, j + 1);
            if n == 0 {
                break;
            }
            names.push((j + 1, n));
            j += 1 + n;
        }
        let mut bits = 0u32;
        let mut kinds = Vec::new();
        for &(k, n) in &names {
            let f = &self.s[k..k + n];
            let Some(at) = find(f) else {
                return Err(format!(
                    "names {}, which is not a field of this index",
                    String::from_utf8_lossy(f)
                ));
            };
            bits |= 1 << at;
            kinds.push(self.fields[at].kind);
        }
        if names.len() == 1 && kinds[0] != FieldKind::Text {
            let (e, end) = filter::parse_field_term(self.s, at, self.fields)?;
            self.i = end;
            return Ok(Some(QNode::new(Node::Leaf(e))));
        }
        if kinds.iter().any(|k| *k != FieldKind::Text) {
            return Err("names several fields with '|', which must all be TEXT".into());
        }
        self.i = j;
        self.skip();
        if self.peek() != Some(b':') {
            return Err(format!(
                "needs a ':' after @{}",
                String::from_utf8_lossy(&self.s[at..j])
            ));
        }
        self.i += 1;
        self.skip();
        let mask = mask & bits;
        match self.peek() {
            Some(b'(') => self.atom(mask),
            Some(b'-') | Some(b'~') => self.unary(mask),
            Some(b'{') | Some(b'[') => Err(format!(
                "uses @{} as a TAG or NUMERIC, but it is TEXT",
                String::from_utf8_lossy(&self.s[at..j])
            )),
            _ => self.text_atom(mask),
        }
    }
}

/// The scoring function `SCORER` names (`default.c`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scorer {
    Tfidf,
    TfidfDocnorm,
    Bm25,
    Bm25Std,
    Bm25StdTanh,
    Bm25StdNorm,
    Dismax,
    Docscore,
}

impl Scorer {
    /// The default since RediSearch 8.0.
    pub const DEFAULT: Scorer = Scorer::Bm25Std;

    pub fn parse(name: &[u8]) -> Result<Scorer, String> {
        Ok(match name.to_ascii_uppercase().as_slice() {
            b"TFIDF" => Scorer::Tfidf,
            b"TFIDF.DOCNORM" => Scorer::TfidfDocnorm,
            b"BM25" => Scorer::Bm25,
            b"BM25STD" => Scorer::Bm25Std,
            b"BM25STD.TANH" => Scorer::Bm25StdTanh,
            b"BM25STD.NORM" => Scorer::Bm25StdNorm,
            b"DISMAX" => Scorer::Dismax,
            b"DOCSCORE" => Scorer::Docscore,
            b"HAMMING" => {
                return Err("FT.SEARCH SCORER HAMMING is not supported: it scores \
                            payloads (ADR-0063)"
                    .into());
            }
            other => {
                return Err(format!(
                    "FT.SEARCH SCORER {} is not a scorer",
                    String::from_utf8_lossy(other)
                ));
            }
        })
    }
}

/// What a search reads of an index's documents besides their words.
pub trait Docs {
    /// How many documents the index holds: RediSearch's `numDocuments`.
    fn count(&self) -> usize;
    /// Every live document, in id order.
    fn ids(&self) -> Box<dyn Iterator<Item = DocId> + '_>;
    /// Whether document `id` is live.
    fn live(&self, id: DocId) -> bool;
    /// Its values, for TAG, NUMERIC and GEO terms.
    fn vals(&self, id: DocId) -> Option<&[Val]>;
    /// Its score: `SCORE_FIELD`'s, else the index's `SCORE`, 1 unless set.
    fn score(&self, id: DocId) -> f32;
    /// How many documents hold `tag` in TAG field `field`.
    fn tag_docs(&self, field: usize, tag: &[u8]) -> usize;
}

/// `TFIDF`'s IDF: `floor(log2(1 + (N + 1) / n))`, `n` at least 1
/// (`calculate_idf`, which takes the exponent of the sum).
fn idf(n_docs: usize, term_docs: usize) -> f64 {
    let v = 1.0 + (n_docs as f64 + 1.0) / term_docs.max(1) as f64;
    ((v.to_bits() >> 52) & 0x7ff) as f64 - 1023.0
}

/// `BM25STD`'s IDF: `ln(1 + (N - n + 0.5) / (n + 0.5))`, `N` at least `n`
/// (`calculate_idf_bm25`).
fn bm25_idf(n_docs: usize, term_docs: usize) -> f64 {
    let total = n_docs.max(term_docs) as f64;
    let term = term_docs as f64;
    (1.0 + (total - term + 0.5) / (term + 0.5)).ln()
}

/// A query resolved against the index: words expanded and their postings
/// found, each term's IDFs worked out, an AND's terms in the order
/// RediSearch's intersection takes them.
enum Plan<'a> {
    /// Matches no document: a word the index does not hold.
    Nothing,
    All {
        weight: f64,
    },
    Term {
        postings: &'a BTreeMap<DocId, Hit>,
        mask: u32,
        weight: f64,
        idf: f64,
        bm25_idf: f64,
    },
    Tag {
        field: usize,
        tag: Box<[u8]>,
        weight: f64,
        idf: f64,
        bm25_idf: f64,
    },
    /// A NUMERIC or GEO term: a numeric result.
    Numeric(&'a Expr),
    Union {
        kids: Vec<Plan<'a>>,
        weight: f64,
        /// Matched by the first child that matches: a prefix's, suffix's,
        /// contains', fuzzy or tag term's words.
        first: bool,
    },
    Inter {
        kids: Vec<Plan<'a>>,
        weight: f64,
        slop: Option<u32>,
        inorder: bool,
    },
    Not {
        kid: Box<Plan<'a>>,
        weight: f64,
    },
    Optional {
        kid: Box<Plan<'a>>,
        weight: f64,
    },
}

struct Planner<'a> {
    text: &'a TextIndex,
    docs: &'a dyn Docs,
    nostem: u32,
}

impl<'a> Planner<'a> {
    fn term(&self, t: &[u8], mask: u32, weight: f64) -> Plan<'a> {
        let Some(postings) = self.text.postings(t) else {
            return Plan::Nothing;
        };
        let n = self.docs.count();
        Plan::Term {
            postings,
            mask,
            weight,
            idf: idf(n, postings.len()),
            bm25_idf: bm25_idf(n, postings.len()),
        }
    }

    fn union(kids: Vec<Plan<'a>>, weight: f64, first: bool) -> Plan<'a> {
        let kids: Vec<Plan<'a>> = kids
            .into_iter()
            .filter(|k| !matches!(k, Plan::Nothing))
            .collect();
        if kids.is_empty() {
            Plan::Nothing
        } else {
            Plan::Union {
                kids,
                weight,
                first,
            }
        }
    }

    /// The words of `terms` that match, at most [`MAX_EXPANSIONS`], each a
    /// child of a union matched by its first.
    fn expansion(&self, terms: impl Iterator<Item = &'a [u8]>, mask: u32, weight: f64) -> Plan<'a> {
        let kids = terms
            .take(MAX_EXPANSIONS)
            .map(|t| self.term(t, mask, 1.0))
            .collect();
        Planner::union(kids, weight, true)
    }

    fn plan(&self, q: &'a QNode) -> Plan<'a> {
        let w = q.weight;
        match &q.node {
            Node::All => Plan::All { weight: w },
            Node::Leaf(e) => match e {
                Expr::Tag { field, any_of } => {
                    let n = self.docs.count();
                    let tag = |t: &[u8]| {
                        let m = self.docs.tag_docs(*field, t);
                        Plan::Tag {
                            field: *field,
                            tag: t.into(),
                            weight: w,
                            idf: idf(n, m),
                            bm25_idf: bm25_idf(n, m),
                        }
                    };
                    match any_of.as_slice() {
                        [one] => tag(one),
                        many => Plan::Union {
                            kids: many.iter().map(|t| tag(t)).collect(),
                            weight: w,
                            first: true,
                        },
                    }
                }
                _ => Plan::Numeric(e),
            },
            Node::Word { mask, word } => {
                // Expanded unless every field it searches is NOSTEM; every
                // field at all counts as stemmable (`QueryNode_Expand`).
                let expand = *mask == ALL || mask & !self.nostem != 0;
                let stem = std::str::from_utf8(word)
                    .ok()
                    .map(snowball::english)
                    .filter(|_| expand);
                let Some(stem) = stem else {
                    return self.term(word, *mask, w);
                };
                let stemmable = if *mask == ALL {
                    ALL
                } else {
                    mask & !self.nostem
                };
                let mut plus = Vec::with_capacity(stem.len() + 1);
                plus.push(STEM_PREFIX);
                plus.extend_from_slice(stem.as_bytes());
                let mut kids = vec![self.term(word, *mask, w), self.term(&plus, stemmable, 1.0)];
                if stem.as_bytes() != &**word {
                    kids.push(self.term(stem.as_bytes(), stemmable, 1.0));
                }
                Planner::union(kids, 1.0, false)
            }
            Node::Prefix { mask, pat } if pat.len() >= MIN_AFFIX => {
                self.expansion(self.text.with_prefix(pat), *mask, w)
            }
            Node::Suffix { mask, pat } if pat.len() >= MIN_AFFIX => {
                self.expansion(self.text.all_terms().filter(|t| t.ends_with(pat)), *mask, w)
            }
            Node::Contains { mask, pat } if pat.len() >= MIN_AFFIX => self.expansion(
                self.text
                    .all_terms()
                    .filter(|t| t.windows(pat.len()).any(|x| x == &**pat)),
                *mask,
                w,
            ),
            Node::Prefix { .. } | Node::Suffix { .. } | Node::Contains { .. } => Plan::Nothing,
            Node::Fuzzy { mask, word, dist } => self.expansion(
                self.text
                    .all_terms()
                    .filter(|t| within_edits(t, word, usize::from(*dist))),
                *mask,
                w,
            ),
            Node::Phrase { mask, words } => {
                let kids: Vec<Plan<'a>> = words.iter().map(|t| self.term(t, *mask, 1.0)).collect();
                if kids.iter().any(|k| matches!(k, Plan::Nothing)) {
                    return Plan::Nothing;
                }
                Plan::Inter {
                    kids,
                    weight: w,
                    slop: Some(0),
                    inorder: true,
                }
            }
            Node::And(xs) => {
                let mut kids: Vec<Plan<'a>> = xs.iter().map(|x| self.plan(x)).collect();
                if kids.iter().any(|k| matches!(k, Plan::Nothing)) {
                    return Plan::Nothing;
                }
                // Smallest first, as RediSearch's intersection orders its
                // children, unless their order is the point.
                if !q.inorder {
                    kids.sort_by(|a, b| self.estimate(a).total_cmp(&self.estimate(b)));
                }
                Plan::Inter {
                    kids,
                    weight: w,
                    slop: q.slop,
                    inorder: q.inorder,
                }
            }
            Node::Or(xs) => Planner::union(xs.iter().map(|x| self.plan(x)).collect(), w, false),
            Node::Not(x) => Plan::Not {
                kid: Box::new(self.plan(x)),
                weight: w,
            },
            Node::Optional(x) => Plan::Optional {
                kid: Box::new(self.plan(x)),
                weight: w,
            },
        }
    }

    /// How many documents a plan's iterator expects: what an intersection
    /// orders its children by.
    fn estimate(&self, p: &Plan) -> f64 {
        match p {
            Plan::Nothing => 0.0,
            Plan::Term { postings, .. } => postings.len() as f64,
            Plan::Tag { field, tag, .. } => self.docs.tag_docs(*field, tag) as f64,
            Plan::Union { kids, .. } => kids.iter().map(|k| self.estimate(k)).sum(),
            Plan::Inter { kids, .. } => kids
                .iter()
                .map(|k| self.estimate(k))
                .fold(f64::INFINITY, f64::min),
            Plan::All { .. } | Plan::Numeric(_) | Plan::Not { .. } | Plan::Optional { .. } => {
                self.docs.count() as f64
            }
        }
    }
}

/// Whether `a` is within `d` edits of `b`, counted in characters
/// (Levenshtein), or in bytes when either is not UTF-8.
fn within_edits(a: &[u8], b: &[u8], d: usize) -> bool {
    match (std::str::from_utf8(a), std::str::from_utf8(b)) {
        (Ok(x), Ok(y)) => {
            let x: Vec<char> = x.chars().collect();
            let y: Vec<char> = y.chars().collect();
            levenshtein(&x, &y, d)
        }
        _ => levenshtein(a, b, d),
    }
}

fn levenshtein<T: PartialEq>(a: &[T], b: &[T], d: usize) -> bool {
    if a.len().abs_diff(b.len()) > d {
        return false;
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, x) in a.iter().enumerate() {
        let mut cur = vec![i + 1; b.len() + 1];
        for (j, y) in b.iter().enumerate() {
            let sub = prev[j] + usize::from(x != y);
            cur[j + 1] = sub.min(prev[j + 1] + 1).min(cur[j] + 1);
        }
        if cur.iter().min().is_some_and(|&m| m > d) {
            return false;
        }
        prev = cur;
    }
    prev[b.len()] <= d
}

/// One document's match, as RediSearch's iterators yield it.
#[derive(Clone, Debug)]
enum Res<'a> {
    Term {
        freq: u32,
        weight: f64,
        idf: f64,
        bm25_idf: f64,
        offsets: &'a [u32],
    },
    Agg {
        inter: bool,
        kids: Vec<Res<'a>>,
        weight: f64,
    },
    Virtual {
        freq: u32,
        weight: f64,
    },
    /// A NUMERIC or GEO match: frequency 1, weight 1.
    Numeric,
}

impl Res<'_> {
    fn with_weight(mut self, w: f64) -> Self {
        match &mut self {
            Res::Term { weight, .. } | Res::Agg { weight, .. } | Res::Virtual { weight, .. } => {
                *weight = w
            }
            Res::Numeric => {}
        }
        self
    }

    /// Whether RediSearch reads positions from it (`has_offsets`): a term
    /// with any, or a union or intersection whose children are not all
    /// virtual. RediSearch also leaves out one whose children's kinds are
    /// exactly numeric and metric, which no search here makes: metric
    /// results are a KNN's.
    fn has_offsets(&self) -> bool {
        match self {
            Res::Term { offsets, .. } => !offsets.is_empty(),
            Res::Agg { kids, .. } => !kids.iter().all(|k| matches!(k, Res::Virtual { .. })),
            Res::Virtual { .. } | Res::Numeric => false,
        }
    }

    /// Its positions, in order: an aggregate's merged from its children's.
    fn offsets(&self, out: &mut Vec<u32>) {
        match self {
            Res::Term { offsets, .. } => out.extend_from_slice(offsets),
            Res::Agg { kids, .. } => {
                for k in kids {
                    k.offsets(out);
                }
            }
            Res::Virtual { .. } | Res::Numeric => {}
        }
    }

    fn positions(&self) -> Vec<u32> {
        let mut v = Vec::new();
        self.offsets(&mut v);
        v.sort_unstable();
        v
    }
}

/// Whether an intersection's children hold positions within `slop` of each
/// other, in the order given when `inorder` (RediSearch's `is_within_range`
/// rules: the children with positions only, greedily from each position of
/// the first; unordered, the narrowest window across all of them).
fn within_range(kids: &[Res], slop: Option<u32>, inorder: bool) -> bool {
    let streams: Vec<Vec<u32>> = kids
        .iter()
        .filter(|k| k.has_offsets())
        .map(Res::positions)
        .collect();
    if streams.len() <= 1 {
        return true;
    }
    let max = slop.unwrap_or(u32::MAX);
    if inorder {
        let mut at = vec![0usize; streams.len()];
        for &first in &streams[0] {
            let mut span: i64 = 0;
            let mut last = first;
            let mut over = false;
            for i in 1..streams.len() {
                let s = &streams[i];
                while at[i] < s.len() && s[at[i]] < last {
                    at[i] += 1;
                }
                let Some(&pos) = s.get(at[i]) else {
                    return false;
                };
                span += i64::from(pos) - i64::from(last) - 1;
                if span > 0 && span as u64 > u64::from(max) {
                    over = true;
                    break;
                }
                last = pos;
            }
            if !over {
                return true;
            }
        }
        false
    } else {
        let n = streams.len();
        let mut at = vec![0usize; n];
        let mut max_pos = streams.iter().map(|s| s[0]).max().unwrap_or(0);
        loop {
            let (min_i, min_pos) = (0..n)
                .map(|i| (i, streams[i][at[i]]))
                .min_by_key(|&(_, p)| p)
                .unwrap_or((0, 0));
            if min_pos != max_pos {
                let span = i64::from(max_pos) - i64::from(min_pos) - (n as i64 - 1);
                if span < 0 || span as u64 <= u64::from(max) {
                    return true;
                }
            }
            at[min_i] += 1;
            let Some(&next) = streams[min_i].get(at[min_i]) else {
                return false;
            };
            max_pos = max_pos.max(next);
        }
    }
}

/// The slop a `TFIDF` or `BM25` score is divided by (`MinOffsetDelta`): 1
/// unless the match is an aggregate of two or more; else the root of the
/// summed squares of the nearest distance between each child with positions
/// and the next, cut to a whole number, or one less than its children when
/// that sum is 0.
fn slop(r: &Res) -> i32 {
    const EOF: u32 = u32::MAX;
    let Res::Agg { kids, .. } = r else {
        return 1;
    };
    if kids.len() <= 1 {
        return 1;
    }
    let with: Vec<Vec<u32>> = kids
        .iter()
        .filter(|k| k.has_offsets())
        .map(Res::positions)
        .collect();
    let mut dist: i32 = 0;
    for pair in with.windows(2) {
        let (a, b) = (&pair[0], &pair[1]);
        let (mut i, mut j) = (0usize, 0usize);
        let next = |v: &Vec<u32>, k: usize| v.get(k).copied().unwrap_or(EOF);
        let (mut p1, mut p2) = (next(a, 0), next(b, 0));
        let mut cd = p1.abs_diff(p2) as i32;
        while cd > 1 && p1 != EOF && p2 != EOF {
            cd = cd.min(p1.abs_diff(p2) as i32);
            if p2 > p1 {
                i += 1;
                p1 = next(a, i);
            } else {
                j += 1;
                p2 = next(b, j);
            }
        }
        dist = dist.wrapping_add(cd.wrapping_mul(cd));
    }
    if dist != 0 {
        f64::from(dist).sqrt() as i32
    } else {
        kids.len() as i32 - 1
    }
}

/// `plan`'s match in document `id`, or `None`.
fn eval<'a>(p: &Plan<'a>, id: DocId, docs: &dyn Docs) -> Option<Res<'a>> {
    match p {
        Plan::Nothing => None,
        Plan::All { weight } => Some(Res::Virtual {
            freq: 1,
            weight: *weight,
        }),
        Plan::Term {
            postings,
            mask,
            weight,
            idf,
            bm25_idf,
        } => {
            let hit = postings.get(&id).filter(|h| h.fields & mask != 0)?;
            Some(Res::Term {
                freq: hit.freq,
                weight: *weight,
                idf: *idf,
                bm25_idf: *bm25_idf,
                offsets: &hit.offsets,
            })
        }
        Plan::Tag {
            field,
            tag,
            weight,
            idf,
            bm25_idf,
        } => match docs.vals(id)?.get(*field)? {
            Val::Tags(ts) if ts.binary_search(tag).is_ok() => Some(Res::Term {
                freq: 1,
                weight: *weight,
                idf: *idf,
                bm25_idf: *bm25_idf,
                offsets: &[],
            }),
            _ => None,
        },
        Plan::Numeric(e) => e.matches(docs.vals(id)).then_some(Res::Numeric),
        Plan::Union {
            kids,
            weight,
            first,
        } => {
            let mut got = Vec::new();
            for k in kids {
                if let Some(r) = eval(k, id, docs) {
                    got.push(r);
                    if *first {
                        break;
                    }
                }
            }
            (!got.is_empty()).then_some(Res::Agg {
                inter: false,
                kids: got,
                weight: *weight,
            })
        }
        Plan::Inter {
            kids,
            weight,
            slop,
            inorder,
        } => {
            let got: Vec<Res<'a>> = kids
                .iter()
                .map(|k| eval(k, id, docs))
                .collect::<Option<_>>()?;
            if (slop.is_some() || *inorder) && !within_range(&got, *slop, *inorder) {
                return None;
            }
            Some(Res::Agg {
                inter: true,
                kids: got,
                weight: *weight,
            })
        }
        Plan::Not { kid, weight } => match eval(kid, id, docs) {
            Some(_) => None,
            None => Some(Res::Virtual {
                freq: 0,
                weight: *weight,
            }),
        },
        Plan::Optional { kid, weight } => Some(match eval(kid, id, docs) {
            Some(r) => r.with_weight(*weight),
            None => Res::Virtual {
                freq: 1,
                weight: 0.0,
            },
        }),
    }
}

/// The documents a plan can match, in id order, from its postings; `None`
/// when only a look at every document can say.
fn candidates(p: &Plan) -> Option<Vec<DocId>> {
    match p {
        Plan::Nothing => Some(Vec::new()),
        Plan::Term { postings, mask, .. } => Some(
            postings
                .iter()
                .filter(|(_, h)| h.fields & mask != 0)
                .map(|(id, _)| *id)
                .collect(),
        ),
        Plan::Union { kids, .. } => {
            let mut all = Vec::new();
            for k in kids {
                all.extend(candidates(k)?);
            }
            all.sort_unstable();
            all.dedup();
            Some(all)
        }
        Plan::Inter { kids, .. } => {
            let mut best: Option<Vec<DocId>> = None;
            for k in kids {
                if let Some(c) = candidates(k) {
                    best = Some(match best {
                        None => c,
                        Some(b) => b
                            .into_iter()
                            .filter(|x| c.binary_search(x).is_ok())
                            .collect(),
                    });
                }
            }
            best
        }
        _ => None,
    }
}

/// What a score reads of the document and the index.
struct Stats {
    len: u32,
    max_freq: u32,
    score: f64,
    avg_len: f64,
}

/// `BM25STD`'s term score, in RediSearch's types: `b` and `k1` are `f32`,
/// and `b × length` is multiplied in `f32` before it is divided.
fn bm25_std(idf: f64, f: f64, len: u32, avg: f64, weight: f64) -> f64 {
    let (b, k1) = (0.75f32, 1.2f32);
    let blen = f64::from(b * len as f32) / avg;
    weight * idf * f * f64::from(k1 + 1.0) / (f + f64::from(k1) * (f64::from(1.0f32 - b) + blen))
}

fn tfidf_sum(r: &Res) -> f64 {
    match r {
        Res::Term {
            freq, weight, idf, ..
        } => weight * f64::from(*freq) * idf,
        Res::Agg { kids, weight, .. } => weight * kids.iter().map(tfidf_sum).sum::<f64>(),
        Res::Virtual { freq, weight } => weight * f64::from(*freq),
        Res::Numeric => 1.0,
    }
}

fn bm25_sum(r: &Res, s: &Stats) -> f64 {
    let (b, k1) = (0.5f32, 1.2f32);
    let norm = |f: f64| f + f64::from(k1) * (f64::from(1.0f32 - b) + f64::from(b) * s.avg_len);
    match r {
        Res::Term {
            freq, weight, idf, ..
        } => {
            let f = f64::from(*freq);
            weight * idf * f / norm(f)
        }
        Res::Agg { kids, weight, .. } => kids.iter().map(|k| bm25_sum(k, s)).sum::<f64>() * weight,
        Res::Virtual { freq, weight } if *freq != 0 => {
            let f = f64::from(*freq);
            weight * f / norm(f)
        }
        Res::Numeric => 1.0 / norm(1.0),
        Res::Virtual { .. } => 0.0,
    }
}

fn bm25_std_sum(r: &Res, s: &Stats) -> f64 {
    match r {
        Res::Term {
            freq,
            weight,
            bm25_idf,
            ..
        } => bm25_std(*bm25_idf, f64::from(*freq), s.len, s.avg_len, *weight),
        Res::Agg { kids, weight, .. } => {
            kids.iter().map(|k| bm25_std_sum(k, s)).sum::<f64>() * weight
        }
        Res::Virtual { freq, weight } if *freq != 0 && *weight != 0.0 => {
            bm25_std(1.0, 1.0, s.len, s.avg_len, *weight)
        }
        Res::Virtual { .. } | Res::Numeric => 0.0,
    }
}

fn dismax(r: &Res) -> f64 {
    match r {
        Res::Term { freq, weight, .. } | Res::Virtual { freq, weight } => weight * f64::from(*freq),
        Res::Numeric => 1.0,
        Res::Agg {
            inter: true,
            kids,
            weight,
        } => weight * kids.iter().map(dismax).sum::<f64>(),
        Res::Agg {
            inter: false,
            kids,
            weight,
        } => weight * kids.iter().map(dismax).fold(0.0, f64::max),
    }
}

/// `BM25STD.TANH`'s stretch (`DEFAULT_BM25STD_TANH_FACTOR`).
const TANH_FACTOR: f64 = 4.0;

fn score(scorer: Scorer, r: &Res, s: &Stats) -> f64 {
    match scorer {
        Scorer::Tfidf | Scorer::TfidfDocnorm => {
            if s.score == 0.0 {
                return 0.0;
            }
            let norm = if scorer == Scorer::Tfidf {
                s.max_freq
            } else {
                s.len
            };
            if norm == 0 {
                return 0.0;
            }
            s.score * tfidf_sum(r) / f64::from(norm) / f64::from(slop(r))
        }
        Scorer::Bm25 => s.score * bm25_sum(r, s) / f64::from(slop(r)),
        Scorer::Bm25Std | Scorer::Bm25StdNorm => s.score * bm25_std_sum(r, s),
        Scorer::Bm25StdTanh => ((1.0 / TANH_FACTOR) * (s.score * bm25_std_sum(r, s))).tanh(),
        Scorer::Dismax => dismax(r),
        Scorer::Docscore => s.score,
    }
}

/// Every document `q` matches, with its score under `scorer`, in id order.
/// `None` matches nothing, as a query of stop words does.
pub fn matches(
    q: Option<&QNode>,
    text: &TextIndex,
    docs: &dyn Docs,
    nostem: u32,
    scorer: Scorer,
) -> Vec<(DocId, f64)> {
    let Some(q) = q else {
        return Vec::new();
    };
    let planner = Planner { text, docs, nostem };
    let plan = planner.plan(q);
    let n = docs.count();
    let avg_len = if n == 0 {
        0.0
    } else {
        text.total_len() as f64 / n as f64
    };
    let ids: Box<dyn Iterator<Item = DocId>> = match candidates(&plan) {
        Some(c) => Box::new(c.into_iter().filter(|id| docs.live(*id))),
        None => docs.ids(),
    };
    let mut out: Vec<(DocId, f64)> = ids
        .filter_map(|id| {
            let r = eval(&plan, id, docs)?;
            let (len, max_freq) = text.doc(id);
            let s = Stats {
                len,
                max_freq,
                score: f64::from(docs.score(id)),
                avg_len,
            };
            Some((id, score(scorer, &r, &s)))
        })
        .collect();
    // `BM25STD.NORM`: each score over the highest (`RPMaxScoreNormalizer`).
    if scorer == Scorer::Bm25StdNorm {
        let top = out
            .iter()
            .map(|(_, s)| *s)
            .fold(f64::NEG_INFINITY, f64::max);
        if top != 0.0 && top.is_finite() {
            for (_, s) in &mut out {
                *s /= top;
            }
        }
    }
    out
}

/// RediSearch's order without `SORTBY`: highest score first, a tie to the
/// lower document id. A NaN score (an index with no TEXT field, where the
/// average length is 0) ties with every other, as RediSearch compares it;
/// here NaNs are equal to each other and below every number, which is the
/// same order whenever, as there, every score is NaN or none is.
pub fn by_score(a: &(DocId, f64), b: &(DocId, f64)) -> std::cmp::Ordering {
    let key = |x: f64| if x.is_nan() { f64::NEG_INFINITY } else { x };
    key(b.1).total_cmp(&key(a.1)).then(a.0.cmp(&b.0))
}

#[cfg(test)]
mod tests;
