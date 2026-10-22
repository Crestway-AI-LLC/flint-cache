// SPDX-License-Identifier: Elastic-2.0
use super::*;
use crate::filter::Vals;
use crate::fulltext::{TextField, analyze};
use std::collections::HashMap;

fn text(name: &str) -> Field {
    Field {
        name: name.into(),
        kind: FieldKind::Text,
    }
}

fn tag(name: &str) -> Field {
    Field {
        name: name.into(),
        kind: FieldKind::FoldedTag,
    }
}

fn q(s: &str, fields: &[Field]) -> Option<QNode> {
    parse(s.as_bytes(), fields, &StopWords::of(None)).expect(s)
}

fn word(mask: u32, w: &str) -> QNode {
    QNode::new(Node::Word {
        mask,
        word: w.as_bytes().into(),
    })
}

/// An index's documents for a test: values, scores, tags counted.
#[derive(Default)]
struct TestDocs {
    vals: BTreeMap<DocId, Vals>,
    scores: HashMap<DocId, f32>,
}

impl Docs for TestDocs {
    fn count(&self) -> usize {
        self.vals.len()
    }
    fn ids(&self) -> Box<dyn Iterator<Item = DocId> + '_> {
        Box::new(self.vals.keys().copied())
    }
    fn live(&self, id: DocId) -> bool {
        self.vals.contains_key(&id)
    }
    fn vals(&self, id: DocId) -> Option<&[Val]> {
        self.vals.get(&id).map(|v| &**v)
    }
    fn score(&self, id: DocId) -> f32 {
        self.scores.get(&id).copied().unwrap_or(1.0)
    }
    fn tag_docs(&self, field: usize, t: &[u8]) -> usize {
        self.vals
            .values()
            .filter(|v| matches!(&v[field], Val::Tags(ts) if ts.iter().any(|x| &**x == t)))
            .count()
    }
}

/// An index of TEXT fields only, each document's values by field, every
/// field stemmed at weight 1 unless `weights` says otherwise.
fn index(docs: &[&[&str]], weights: &[f32]) -> (TextIndex, TestDocs) {
    let stop = StopWords::of(None);
    let mut ix = TextIndex::default();
    let mut td = TestDocs::default();
    for (i, d) in docs.iter().enumerate() {
        let id = i as DocId + 1;
        let fields: Vec<TextField> = (0..d.len())
            .map(|f| TextField {
                bit: f as u32,
                weight: weights.get(f).copied().unwrap_or(1.0),
                stem: true,
            })
            .collect();
        let texts: Vec<(&TextField, &[u8])> = fields
            .iter()
            .zip(d.iter())
            .map(|(f, t)| (f, t.as_bytes()))
            .collect();
        ix.put(id, analyze(&texts, &stop));
        td.vals.insert(
            id,
            d.iter().map(|_| Val::Missing).collect::<Vec<_>>().into(),
        );
    }
    (ix, td)
}

fn run(
    query: &str,
    fields: &[Field],
    ix: &TextIndex,
    docs: &TestDocs,
    scorer: Scorer,
) -> Vec<(DocId, f64)> {
    let qn = q(query, fields);
    let mut m = matches(qn.as_ref(), ix, docs, 0, scorer);
    m.sort_by(by_score);
    m
}

fn ids(m: &[(DocId, f64)]) -> Vec<DocId> {
    m.iter().map(|(id, _)| *id).collect()
}

#[test]
fn the_clients_renderings_parse_as_redisearch_reads_them() {
    let f = [text("job"), text("description"), tag("cs")];
    let (job, desc) = (1u32, 2u32);
    // redisvl's TextQuery: the words as a union on the field, stop words
    // already left out by redisvl; one word weighted.
    assert_eq!(
        q("@description:(medical | professional=>{$weight:2.0})", &f),
        Some(QNode::new(Node::Or(vec![
            word(desc, "medical"),
            QNode {
                weight: 2.0,
                ..word(desc, "professional")
            },
        ])))
    );
    // A field weight on the whole group, ORed with another field's.
    let x = q(
        "(@job:(redis | data) => { $weight: 5.0 } | @description:(redis))",
        &f,
    )
    .expect("a node");
    let Node::Or(xs) = &x.node else {
        panic!("{x:?}");
    };
    assert_eq!(xs[0].weight, 5.0);
    assert_eq!(xs[1], word(desc, "redis"));
    // AND binds tighter than OR, `-` takes the next term, and `@f:` the
    // next term only.
    assert_eq!(
        q("a1 b1 | c1", &f).map(|x| x.node),
        Some(Node::Or(vec![
            QNode::new(Node::And(vec![word(ALL, "a1"), word(ALL, "b1")])),
            word(ALL, "c1"),
        ]))
    );
    assert_eq!(
        q("-a1 b1", &f).map(|x| x.node),
        Some(Node::And(vec![
            QNode::new(Node::Not(Box::new(word(ALL, "a1")))),
            word(ALL, "b1"),
        ]))
    );
    assert_eq!(
        q("@job:a1 b1", &f).map(|x| x.node),
        Some(Node::And(vec![word(job, "a1"), word(ALL, "b1")]))
    );
    // redisvl's Text filters, and the strings its tests write by hand.
    assert_eq!(
        q("@job:enginee*", &f).map(|x| x.node),
        Some(Node::Prefix {
            mask: job,
            pat: b"enginee".as_slice().into()
        })
    );
    assert_eq!(
        q("@job:*engine*", &f).map(|x| x.node),
        Some(Node::Contains {
            mask: job,
            pat: b"engine".as_slice().into()
        })
    );
    assert_eq!(
        q("@job:%%engine%%", &f).map(|x| x.node),
        Some(Node::Fuzzy {
            mask: job,
            word: b"engine".as_slice().into(),
            dist: 2
        })
    );
    assert_eq!(
        q("(-@job:engineer)", &f).map(|x| x.node),
        Some(Node::Not(Box::new(word(job, "engineer"))))
    );
    let multi = q("@job|description:hello", &f).expect("a node");
    assert_eq!(multi, word(job | desc, "hello"));
    // A TAG term goes to ADR-0059's filter, intersected with the text.
    let x = q("(@description:(cancer)) (@cs:{high})", &f).expect("a node");
    let Node::And(xs) = &x.node else {
        panic!("{x:?}");
    };
    assert_eq!(xs[0], word(desc, "cancer"));
    assert!(matches!(
        &xs[1].node,
        Node::Leaf(Expr::Tag { field: 2, .. })
    ));
    // Stop words go; a query of nothing else matches nothing.
    assert_eq!(q("the hello", &f), Some(word(ALL, "hello")));
    assert_eq!(q("the", &f), None);
    // Stray punctuation is passed over, as RediSearch's lexer passes it.
    assert_eq!(
        q("hello, world.", &f).map(|x| x.node),
        Some(Node::And(vec![word(ALL, "hello"), word(ALL, "world")]))
    );
    // A number is one word, its point and sign with it.
    assert_eq!(q("3.14", &f), Some(word(ALL, "3.14")));
    assert_eq!(q("-5", &f), Some(word(ALL, "-5")));
    // An escaped character is part of its word, unescaped.
    assert_eq!(q("what\\'s", &f), Some(word(ALL, "what's")));
    // A phrase keeps its stop words, and is not a word.
    assert_eq!(
        q("@job:\"the engineer\"", &f).map(|x| x.node),
        Some(Node::Phrase {
            mask: job,
            words: vec![b"the".as_slice().into(), b"engineer".as_slice().into()]
        })
    );
    for bad in ["@nosuch:x", "$p", "a=>{$phonetic:true}", "(a", "%%%%a%%%%"] {
        assert!(
            parse(bad.as_bytes(), &f, &StopWords::of(None)).is_err(),
            "{bad}"
        );
    }
}

/// RediSearch's `testScoreIndex` (`tests/pytests/test_scorers.py`): 24
/// documents with a title of WEIGHT 10 and a score of their own; the top
/// five of `hello world` under each scorer, to two decimals, as RediSearch
/// answers them on one shard.
#[test]
fn every_scorer_answers_redisearch_s_own_test_index() {
    let stop = StopWords::of(None);
    let title = TextField {
        bit: 0,
        weight: 10.0,
        stem: true,
    };
    let body = TextField {
        bit: 1,
        weight: 1.0,
        stem: true,
    };
    let mut ix = TextIndex::default();
    let mut docs = TestDocs::default();
    let n_max = 25.0f64;
    for n in 1..25u64 {
        let t = "hello world ".repeat(n as usize);
        let b = "lorem ipsum ".repeat(n as usize);
        ix.put(
            n,
            analyze(&[(&title, t.as_bytes()), (&body, b.as_bytes())], &stop),
        );
        docs.vals.insert(n, vec![Val::Missing, Val::Missing].into());
        // As redis-py writes the float, and RediSearch keeps it: a double
        // parsed, held as a float.
        let sc = ((n_max - n as f64 + 10.0) / (n_max + 10.0)).sqrt();
        let parsed: f64 = format!("{sc:?}").parse().expect("a float");
        docs.scores.insert(n, parsed as f32);
    }
    let fields = [text("title"), text("body")];
    let expected: [(Scorer, [(u64, f64); 5]); 7] = [
        (
            Scorer::Tfidf,
            [(1, 1.97), (2, 1.94), (3, 1.91), (4, 1.88), (5, 1.85)],
        ),
        (
            Scorer::TfidfDocnorm,
            [(1, 0.9), (2, 0.88), (3, 0.87), (4, 0.86), (5, 0.84)],
        ),
        (
            Scorer::Bm25,
            [(17, 0.73), (18, 0.73), (16, 0.72), (19, 0.72), (15, 0.72)],
        ),
        (
            Scorer::Bm25Std,
            [(1, 0.08), (2, 0.08), (3, 0.08), (4, 0.08), (5, 0.08)],
        ),
        (
            Scorer::Bm25StdTanh,
            [(1, 0.02), (2, 0.02), (3, 0.02), (4, 0.02), (5, 0.02)],
        ),
        (
            Scorer::Dismax,
            [
                (24, 480.0),
                (23, 460.0),
                (22, 440.0),
                (21, 420.0),
                (20, 400.0),
            ],
        ),
        (
            Scorer::Docscore,
            [(1, 0.99), (2, 0.97), (3, 0.96), (4, 0.94), (5, 0.93)],
        ),
    ];
    for (scorer, want) in expected {
        let m = run("hello world", &fields, &ix, &docs, scorer);
        assert_eq!(m.len(), 24, "{scorer:?} matches every document");
        let got: Vec<(u64, f64)> = m
            .iter()
            .take(5)
            .map(|(id, s)| (*id, (s * 100.0).round() / 100.0))
            .collect();
        assert_eq!(got, want, "{scorer:?}");
    }
}

#[test]
fn words_match_their_stems_and_stop_words_do_not_count() {
    let (ix, docs) = index(
        &[
            &["a research scientist specializing in cancers"],
            &["a medical professional who treats cancer"],
            &["the engineer"],
        ],
        &[],
    );
    let f = [text("description")];
    // "cancer" finds "cancers" through "+cancer", and "cancers" finds
    // "cancer" through its bare stem.
    assert_eq!(ids(&run("cancer", &f, &ix, &docs, Scorer::Tfidf)).len(), 2);
    assert_eq!(
        ids(&run("@description:cancers", &f, &ix, &docs, Scorer::Tfidf)).len(),
        2
    );
    // A stop word in a phrase matches nothing: the index never holds it.
    assert!(run("\"the engineer\"", &f, &ix, &docs, Scorer::Bm25Std).is_empty());
    assert_eq!(
        ids(&run(
            "\"medical professional\"",
            &f,
            &ix,
            &docs,
            Scorer::Bm25Std
        )),
        [2]
    );
    assert!(run("\"professional medical\"", &f, &ix, &docs, Scorer::Bm25Std).is_empty());
    // A phrase is not stemmed.
    assert!(run("\"cancer\"", &f, &ix, &docs, Scorer::Bm25Std).len() == 1);
}

#[test]
fn affix_and_fuzzy_terms_read_the_word_list() {
    let (ix, docs) = index(
        &[
            &["engineer"],
            &["doctor"],
            &["doctor"],
            &["engineer"],
            &["dermatologist"],
            &["engineering manager"],
        ],
        &[],
    );
    let f = [text("job")];
    let n = |q: &str| run(q, &f, &ix, &docs, Scorer::Bm25Std).len();
    assert_eq!(n("@job:enginee*"), 3);
    assert_eq!(n("@job:(engine*|doctor)"), 5);
    assert_eq!(n("@job:*gineer"), 2, "a suffix");
    assert_eq!(n("@job:*ngine*"), 3, "contains");
    // 'engineer' is 2 edits away, and so is '+engin', the stem of
    // 'engineering': stems are in the word list a fuzzy term reads.
    assert_eq!(n("@job:%%engine%%"), 3);
    assert_eq!(n("@job:%doctr%"), 2);
    assert_eq!(
        n("@job:e*"),
        0,
        "a prefix under 2 characters matches nothing"
    );
    // 'engineer' also finds 'engineering' through their stem, '+engin'.
    assert_eq!(n("@job:engineer"), 3);
    assert_eq!(n("-@job:engineer"), 3);
    assert_eq!(n("~@job:engineer"), 6, "an optional term needs nothing");
}

#[test]
fn slop_and_inorder_hold_an_and_s_words_together() {
    let (ix, docs) = index(&[&["one two three four"], &["four three two one"]], &[]);
    let f = [text("t")];
    let n = |q: &str| ids(&run(q, &f, &ix, &docs, Scorer::Bm25Std));
    assert_eq!(n("(one two)"), [1, 2]);
    assert_eq!(n("(one three)=>{$slop:0}"), Vec::<u64>::new());
    assert_eq!(n("(one three)=>{$slop:1}"), [1, 2]);
    assert_eq!(n("(one three)=>{$slop:1; $inorder:true}"), [1]);
    assert_eq!(n("\"two three\""), [1]);
    assert_eq!(n("\"three two\""), [2]);
}

#[test]
fn ties_and_nan_come_in_document_order() {
    let mut m = vec![(3, f64::NAN), (1, f64::NAN), (2, f64::NAN)];
    m.sort_by(by_score);
    assert_eq!(ids(&m), [1, 2, 3]);
    let mut m = vec![(3, 1.0), (1, 2.0), (2, 2.0)];
    m.sort_by(by_score);
    assert_eq!(ids(&m), [1, 2, 3]);
}

#[test]
fn idfs_are_redisearch_s() {
    // floor(log2(1 + 25 / 24)) = 1; floor(log2(1 + 25 / 1)) = 4.
    assert_eq!(idf(24, 24), 1.0);
    assert_eq!(idf(24, 1), 4.0);
    assert_eq!(idf(24, 0), 4.0, "n is at least 1");
    assert!((bm25_idf(24, 24) - (1.0f64 + 0.5 / 24.5).ln()).abs() < 1e-15);
}
