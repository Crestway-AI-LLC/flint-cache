// SPDX-License-Identifier: Elastic-2.0
//! The words of an `FT.*` index's TEXT fields, kept as RediSearch keeps them
//! (ADR-0063): each document's words split as its indexer splits them
//! (BUG-0272), stop words left out, English stems added, and a posting list
//! for each word with the counts RediSearch's scorers read.
//!
//! A document is known here by its id, the order of its last write, which
//! is also how RediSearch numbers documents: a rewrite takes a new id.

use crate::filter;
use crate::snowball;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

/// RediSearch's default stop words (`stopwords.h`).
pub const DEFAULT_STOPWORDS: [&str; 33] = [
    "a", "is", "the", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into",
    "it", "no", "not", "of", "on", "or", "such", "that", "their", "then", "there", "these", "they",
    "this", "to", "was", "will", "with",
];

/// The shortest word, in bytes, whose stem is indexed
/// (`DEFAULT_MIN_STEM_LENGTH`).
const MIN_STEM_LEN: usize = 4;

/// What one occurrence of a stem weighs against its word's
/// (`STEM_TOKEN_FACTOR`), in `f32` as RediSearch multiplies it.
const STEM_FACTOR: f32 = 0.2;

/// What a stem is indexed under: `+` and the stem (`STEM_PREFIX`).
pub const STEM_PREFIX: u8 = b'+';

/// A document's id: the order of its last write.
pub type DocId = u64;

/// One TEXT field, as the index reads it.
#[derive(Clone, Debug, PartialEq)]
pub struct TextField {
    /// Its bit in a field mask: its place among the index's attributes.
    pub bit: u32,
    /// `WEIGHT`, 1 unless declared, as RediSearch keeps it.
    pub weight: f32,
    /// Whether its words are stemmed: not `NOSTEM`.
    pub stem: bool,
}

/// An index's stop words: RediSearch's default list, or the one `STOPWORDS`
/// declared (`STOPWORDS 0` is none), matched without case.
#[derive(Clone, Debug)]
pub struct StopWords(HashSet<Box<[u8]>>);

impl StopWords {
    pub fn of(declared: Option<&[Box<str>]>) -> StopWords {
        let fold = |w: &str| filter::fold_text(w.as_bytes());
        StopWords(match declared {
            None => DEFAULT_STOPWORDS.iter().map(|w| fold(w)).collect(),
            Some(ws) => ws.iter().map(|w| fold(w)).collect(),
        })
    }

    /// Whether `word`, already lowercased, is a stop word.
    pub fn contains(&self, word: &[u8]) -> bool {
        self.0.contains(word)
    }
}

/// `word`'s English stem as RediSearch indexes it, `+stem`, or `None` when
/// it is shorter than [`MIN_STEM_LEN`], not UTF-8, or its own stem.
fn indexed_stem(word: &[u8]) -> Option<Box<[u8]>> {
    if word.len() < MIN_STEM_LEN {
        return None;
    }
    let s = std::str::from_utf8(word).ok()?;
    let stem = snowball::english(s);
    if stem.as_bytes().eq_ignore_ascii_case(word) {
        return None;
    }
    let mut out = Vec::with_capacity(stem.len() + 1);
    out.push(STEM_PREFIX);
    out.extend_from_slice(stem.as_bytes());
    Some(out.into_boxed_slice())
}

/// A word's place in one document: how often it occurs, weighted by its
/// fields' `WEIGHT`s; which fields hold it; and where, counted in words
/// across the document's TEXT fields in schema order.
#[derive(Clone, Debug, PartialEq)]
pub struct Hit {
    pub freq: u32,
    pub fields: u32,
    pub offsets: Box<[u32]>,
}

/// One document's words, ready to index.
#[derive(Debug, Default, PartialEq)]
pub struct DocText {
    /// Each word, and each `+stem`, with its hit, in byte order.
    terms: Vec<(Box<[u8]>, Hit)>,
    /// Its length: each word's occurrences, weighted, stems not counted.
    pub len: u32,
    /// The largest frequency among its terms, stems counted.
    pub max_freq: u32,
}

/// A document's TEXT values, each with its field, read as RediSearch's
/// indexer reads them: words split and normalized (BUG-0272), stop words
/// skipped before they take a position, and each word of 4 bytes or more
/// (`DEFAULT_MIN_STEM_LENGTH`) also counted under its `+stem` at a fifth of
/// its weight (`forward_index.c`).
pub fn analyze(texts: &[(&TextField, &[u8])], stop: &StopWords) -> DocText {
    let mut acc: HashMap<Box<[u8]>, (u32, u32, Vec<u32>)> = HashMap::new();
    let (mut pos, mut len, mut max_freq) = (0u32, 0u32, 0u32);
    for (field, text) in texts {
        for w in filter::tokenize(text) {
            if stop.contains(&w) {
                continue;
            }
            pos += 1;
            let stem = if field.stem { indexed_stem(&w) } else { None };
            for (term, raw) in std::iter::once((w, true)).chain(stem.map(|s| (s, false))) {
                let score = if raw {
                    field.weight
                } else {
                    field.weight * STEM_FACTOR
                };
                let add = (score as u32).max(1);
                let e = acc.entry(term).or_insert((0, 0, Vec::new()));
                e.0 += add;
                e.1 |= 1 << field.bit;
                e.2.push(pos);
                max_freq = max_freq.max(e.0);
                if raw {
                    len += add;
                }
            }
        }
    }
    let mut terms: Vec<(Box<[u8]>, Hit)> = acc
        .into_iter()
        .map(|(t, (freq, fields, offsets))| {
            (
                t,
                Hit {
                    freq,
                    fields,
                    offsets: offsets.into_boxed_slice(),
                },
            )
        })
        .collect();
    terms.sort_by(|a, b| a.0.cmp(&b.0));
    DocText {
        terms,
        len,
        max_freq,
    }
}

/// What a document holds in the index besides its postings.
struct DocStats {
    len: u32,
    max_freq: u32,
    /// Its terms, to take its postings out again.
    terms: Box<[Arc<[u8]>]>,
}

/// What a term's entry costs the D4 meter beyond its bytes: the `Arc`'s
/// counts, its slot in the term map at a node's worst fill, and its posting
/// map's first leaf, which a `BTreeMap` allocates whole for one entry (11
/// slots of a document id and a [`Hit`]: 368 bytes). An over-estimate,
/// which `tests/ft_meter.rs` holds it to.
const TERM_BYTES: usize = 480;
/// What a posting costs beyond its offsets: its slot in the posting map at
/// a node's worst fill, and its offsets' allocation rounded up.
const POSTING_BYTES: usize = 96;
/// What a document's stats cost beyond its term list: its slot in the
/// document map, with a table's spare room.
const DOC_BYTES: usize = 96;

/// The inverted index of one `FT.*` index's TEXT fields.
#[derive(Default)]
pub struct TextIndex {
    /// Each term's postings, by document id: write order.
    terms: BTreeMap<Arc<[u8]>, BTreeMap<DocId, Hit>>,
    docs: HashMap<DocId, DocStats>,
    /// Every document's length, summed: with the index's document count,
    /// the average length `BM25` and `BM25STD` read.
    total_len: u64,
    /// What it costs the D4 meter, kept as it changes.
    bytes: usize,
}

impl TextIndex {
    /// Index `doc` under `id`, which holds nothing yet.
    pub fn put(&mut self, id: DocId, doc: DocText) {
        debug_assert!(!self.docs.contains_key(&id), "a document id is used once");
        if doc.terms.is_empty() {
            return;
        }
        let mut terms = Vec::with_capacity(doc.terms.len());
        for (t, hit) in doc.terms {
            let key: Arc<[u8]> = match self.terms.get_key_value(&*t) {
                Some((k, _)) => k.clone(),
                None => {
                    let k: Arc<[u8]> = t.into();
                    self.bytes += TERM_BYTES + k.len();
                    self.terms.insert(k.clone(), BTreeMap::new());
                    k
                }
            };
            self.bytes += POSTING_BYTES + 4 * hit.offsets.len();
            if let Some(p) = self.terms.get_mut(&*key) {
                p.insert(id, hit);
            }
            terms.push(key);
        }
        self.bytes += DOC_BYTES + std::mem::size_of::<Arc<[u8]>>() * terms.len();
        self.total_len += u64::from(doc.len);
        self.docs.insert(
            id,
            DocStats {
                len: doc.len,
                max_freq: doc.max_freq,
                terms: terms.into_boxed_slice(),
            },
        );
    }

    /// Take document `id` out, when it is rewritten, deleted or expires.
    pub fn remove(&mut self, id: DocId) {
        let Some(d) = self.docs.remove(&id) else {
            return;
        };
        self.total_len -= u64::from(d.len);
        self.bytes -= DOC_BYTES + std::mem::size_of::<Arc<[u8]>>() * d.terms.len();
        for t in d.terms.iter() {
            let Some(p) = self.terms.get_mut(&**t) else {
                continue;
            };
            if let Some(hit) = p.remove(&id) {
                self.bytes -= POSTING_BYTES + 4 * hit.offsets.len();
            }
            if p.is_empty() {
                self.terms.remove(&**t);
                self.bytes -= TERM_BYTES + t.len();
            }
        }
    }

    /// Document `id`'s length and largest term frequency; zeros for a
    /// document with no words.
    pub fn doc(&self, id: DocId) -> (u32, u32) {
        self.docs.get(&id).map_or((0, 0), |d| (d.len, d.max_freq))
    }

    /// Every document's length, summed.
    pub fn total_len(&self) -> u64 {
        self.total_len
    }

    /// `term`'s postings, in document id order.
    pub fn postings(&self, term: &[u8]) -> Option<&BTreeMap<DocId, Hit>> {
        self.terms.get(term)
    }

    /// The terms that start with `prefix`, in byte order.
    pub fn with_prefix<'a>(&'a self, prefix: &'a [u8]) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.terms
            .range::<[u8], _>((
                std::ops::Bound::Included(prefix),
                std::ops::Bound::Unbounded,
            ))
            .map(|(t, _)| &**t)
            .take_while(move |t| t.starts_with(prefix))
    }

    /// Every term, stems included, in byte order.
    pub fn all_terms(&self) -> impl Iterator<Item = &[u8]> {
        self.terms.keys().map(|t| &**t)
    }

    /// What the index costs the D4 meter.
    pub fn bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn field(bit: u32, weight: f32, stem: bool) -> TextField {
        TextField { bit, weight, stem }
    }

    fn terms(d: &DocText) -> Vec<(String, u32, u32, Vec<u32>)> {
        d.terms
            .iter()
            .map(|(t, h)| {
                (
                    String::from_utf8_lossy(t).into_owned(),
                    h.freq,
                    h.fields,
                    h.offsets.to_vec(),
                )
            })
            .collect()
    }

    /// A document's terms, frequencies, lengths and positions, as
    /// RediSearch's forward index counts them.
    #[test]
    fn a_document_is_counted_as_redisearch_counts_it() {
        let stop = StopWords::of(None);
        let (title, body) = (field(0, 10.0, true), field(1, 1.0, true));
        let d = analyze(
            &[
                (&title, b"The hello worlds".as_slice()),
                (&body, b"hello lorem".as_slice()),
            ],
            &stop,
        );
        // "the" is a stop word: it takes no position. "worlds" stems to
        // "world", under "+world" at a fifth of title's weight 10, 2 a time.
        assert_eq!(
            terms(&d),
            vec![
                ("+world".into(), 2, 0b01, vec![2]),
                ("hello".into(), 11, 0b11, vec![1, 3]),
                ("lorem".into(), 1, 0b10, vec![4]),
                ("worlds".into(), 10, 0b01, vec![2]),
            ]
        );
        // Length: 10 + 10 from title, 1 + 1 from body, stems not counted.
        assert_eq!((d.len, d.max_freq), (22, 11));
        // A weight below 1 still counts each occurrence once, and a stem at
        // weight 1 counts once too.
        let light = field(0, 0.5, true);
        let d = analyze(&[(&light, b"running".as_slice())], &stop);
        assert_eq!(
            terms(&d),
            vec![
                ("+run".into(), 1, 1, vec![1]),
                ("running".into(), 1, 1, vec![1])
            ]
        );
        assert_eq!((d.len, d.max_freq), (1, 1));
        // NOSTEM, a short word, and a word that is its own stem add no stem.
        let nostem = field(0, 1.0, false);
        let d = analyze(&[(&nostem, b"running".as_slice())], &stop);
        assert_eq!(terms(&d), vec![("running".into(), 1, 1, vec![1])]);
        let d = analyze(&[(&light, b"ran hello".as_slice())], &stop);
        assert_eq!(
            terms(&d),
            vec![
                ("hello".into(), 1, 1, vec![2]),
                ("ran".into(), 1, 1, vec![1])
            ],
            "'ran' is under 4 bytes, and 'hello' is its own stem"
        );
        let d = analyze(&[(&light, b"runs".as_slice())], &stop);
        assert_eq!(
            terms(&d),
            vec![
                ("+run".into(), 1, 1, vec![1]),
                ("runs".into(), 1, 1, vec![1])
            ],
            "4 bytes is long enough"
        );
    }

    #[test]
    fn stop_words_are_the_default_list_or_the_declared_one() {
        let d = StopWords::of(None);
        assert!(d.contains(b"the") && d.contains(b"with") && !d.contains(b"hello"));
        let none = StopWords::of(Some(&[]));
        assert!(!none.contains(b"the"));
        let own = StopWords::of(Some(&["Foo".into()]));
        assert!(own.contains(b"foo") && !own.contains(b"the"));
    }

    #[test]
    fn postings_come_and_go_with_their_documents() {
        let stop = StopWords::of(None);
        let f = field(0, 1.0, false);
        let mut ix = TextIndex::default();
        ix.put(1, analyze(&[(&f, b"hello world".as_slice())], &stop));
        ix.put(2, analyze(&[(&f, b"hello there friend".as_slice())], &stop));
        assert_eq!(ix.postings(b"hello").map(|p| p.len()), Some(2));
        assert_eq!(ix.total_len(), 4, "'there' is a stop word");
        assert_eq!(
            ix.with_prefix(b"fr").collect::<Vec<_>>(),
            vec![b"friend".as_slice()]
        );
        let before = ix.bytes();
        ix.remove(1);
        assert_eq!(ix.postings(b"hello").map(|p| p.len()), Some(1));
        assert!(ix.postings(b"world").is_none());
        assert_eq!((ix.total_len(), ix.doc(1)), (2, (0, 0)));
        assert!(ix.bytes() < before);
        ix.remove(2);
        assert_eq!((ix.bytes(), ix.all_terms().count()), (0, 0));
    }
}
