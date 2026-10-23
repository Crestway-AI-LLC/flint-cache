// SPDX-License-Identifier: Elastic-2.0
use super::*;
use crate::ft::parse_create;

fn a(parts: &[&str]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.as_bytes().to_vec()).collect()
}

fn req(parts: &[&str]) -> Result<Request, String> {
    let mut all = vec!["FT.AGGREGATE", "idx"];
    all.extend_from_slice(parts);
    parse(&a(&all))
}

fn def() -> IndexDef {
    parse_create(&a(&[
        "FT.CREATE",
        "idx",
        "ON",
        "HASH",
        "PREFIX",
        "1",
        "doc:",
        "SCHEMA",
        "title",
        "TEXT",
        "SORTABLE",
        "body",
        "TEXT",
        "n",
        "NUMERIC",
        "SORTABLE",
        "price",
        "NUMERIC",
        "t",
        "TAG",
        "u",
        "TAG",
        "SORTABLE",
        "UNF",
        "loc",
        "GEO",
    ]))
    .expect("parses")
    .1
}

fn plan(parts: &[&str], metric: Option<&str>) -> Result<Plan, String> {
    compile(&req(parts)?, &def(), metric)
}

fn slot(p: &Plan, name: &str) -> usize {
    p.root.iter().position(|k| *k.name == *name).expect(name)
}

/// A root row of `p` for document `key`, with `vals` written where a step
/// would find them: under its sorting vector for a sortable name, as its
/// load otherwise.
fn row(p: &Plan, key: &str, seq: u64, vals: &[(&str, Datum)]) -> Row {
    let mut r = Row::root(p, key.as_bytes(), seq);
    for (name, v) in vals {
        let s = slot(p, name);
        if p.root[s].sv.is_some() {
            r.sv[s] = Some(v.clone());
        } else if p.root[s].load.is_some() {
            r.loads[s] = Some(v.clone());
        } else {
            r.vals[s] = Some(v.clone());
        }
    }
    r
}

fn n(x: f64) -> Datum {
    Datum::Num(x)
}

fn s(x: &str) -> Datum {
    Datum::str(x)
}

/// The reply's rows as `name=value` lists.
fn shown(ran: &Ran) -> Vec<Vec<String>> {
    ran.rows
        .iter()
        .map(|r| {
            ran.names
                .iter()
                .enumerate()
                .filter_map(|(i, name)| {
                    r.get(i)
                        .map(|v| format!("{name}={}", String::from_utf8_lossy(&v.text())))
                })
                .collect()
        })
        .collect()
}

/// The four shapes redisvl 0.28.0 sends, as its builders print them.
#[test]
fn redisvl_s_requests_parse() {
    let bulk = req(&[
        "@color:{red}",
        "SCORER",
        "TFIDF",
        "WITHCURSOR",
        "COUNT",
        "500",
        "MAXIDLE",
        "300000",
        "LOAD",
        "1",
        "__key",
        "DIALECT",
        "2",
    ])
    .expect("bulk paging");
    assert_eq!(
        bulk.cursor,
        Some(CursorOpts {
            count: 500,
            max_idle_ms: 300_000
        })
    );
    assert_eq!(
        bulk.steps,
        [Step::Load(vec![("__key".into(), "__key".into())])]
    );
    let router = req(&[
        "@vector:[VECTOR_RANGE $distance_threshold $vector]=>{$YIELD_DISTANCE_AS: vector_distance}",
        "SCORER",
        "TFIDF",
        "DIALECT",
        "2",
        "GROUPBY",
        "1",
        "@route_name",
        "REDUCE",
        "MIN",
        "1",
        "vector_distance",
        "AS",
        "distance",
        "SORTBY",
        "1",
        "@distance",
        "MAX",
        "1",
        "FILTER",
        "(@route_name == 'greeting' && @distance < 0.3)",
    ])
    .expect("router");
    // The SORTBY joins the stretch after the GROUPBY, before the FILTER: its
    // MAX 1 keeps the nearest route, which the FILTER then tests.
    assert!(matches!(router.steps[0], Step::Group { .. }));
    assert_eq!(
        router.steps[1],
        Step::Arrange(Arrange {
            keys: vec![("distance".into(), true)],
            limit: 1,
            ..Arrange::default()
        })
    );
    assert!(matches!(router.steps[2], Step::Filter(_)));
    let hybrid = req(&[
        "(~@description:(medical | lung) (@credit_score:{high}))=>[KNN 7 @user_embedding $vector AS vector_distance]",
        "SCORER", "BM25STD", "ADDSCORES", "LOAD", "2", "user", "description", "DIALECT", "2",
        "APPLY", "(2 - @vector_distance)/2", "AS", "vector_similarity", "APPLY", "@__score",
        "AS", "text_score", "APPLY", "0.30000000000000004*@text_score + 0.7*@vector_similarity",
        "AS", "hybrid_score", "SORTBY", "2", "@hybrid_score", "DESC", "MAX", "7",
    ])
    .expect("hybrid");
    assert!(hybrid.addscores);
    assert_eq!(hybrid.scorer, Scorer::Bm25Std);
    assert_eq!(hybrid.steps.len(), 5);
    let multi = req(&[
        "@a:[VECTOR_RANGE 1.0 $vector_0]=>{$YIELD_DISTANCE_AS: distance_0}",
        "SCORER",
        "TFIDF",
        "LOAD",
        "2",
        "user",
        "age",
        "DIALECT",
        "2",
        "APPLY",
        "(2 - @distance_0)/2",
        "AS",
        "score_0",
        "SORTBY",
        "2",
        "@score_0",
        "DESC",
        "MAX",
        "5",
    ])
    .expect("multi-vector");
    assert_eq!(multi.steps.len(), 3);
}

/// RediSearch's words for what it refuses, and ADR-0064's for what Flint
/// refuses by name.
#[test]
fn bad_requests_are_refused_in_redisearch_s_words() {
    let refused = |parts: &[&str]| req(parts).expect_err("refused");
    assert_eq!(
        refused(&["*", "SORTBY", "1", "@a", "SORTBY", "1", "@b"]),
        "Multiple SORTBY steps are not allowed. Sort multiple fields in a single step"
    );
    assert_eq!(
        refused(&["*", "SORTBY", "2", "@a", "UP"]),
        "MISSING ASC or DESC after sort field (UP)"
    );
    assert_eq!(
        refused(&[
            "*", "SORTBY", "9", "@a", "@b", "@c", "@d", "@e", "@f", "@g", "@h", "@i"
        ]),
        "Cannot sort by more than 8 fields"
    );
    assert_eq!(
        refused(&["*", "LIMIT", "0"]),
        "LIMIT requires two arguments"
    );
    assert_eq!(
        refused(&["*", "LIMIT", "x", "1"]),
        "LIMIT needs two numeric arguments"
    );
    assert_eq!(
        refused(&["*", "LIMIT", "5", "0"]),
        "The `offset` of the LIMIT must be 0 when `num` is 0"
    );
    assert_eq!(
        refused(&["*", "LIMIT", "0", "10001"]),
        "LIMIT exceeds maximum of 10000"
    );
    assert_eq!(
        refused(&["*", "GROUPBY", "1", "t"]),
        "Bad arguments for GROUPBY: Unknown property `t`. Did you mean `@t`?"
    );
    assert_eq!(
        refused(&["*", "GROUPBY", "1", "@t", "REDUCE", "NOPE", "0"]),
        "No such reducer: NOPE"
    );
    assert!(
        refused(&[
            "*", "GROUPBY", "1", "@t", "REDUCE", "QUANTILE", "2", "@n", "0.5"
        ])
        .contains("ADR-0064")
    );
    assert!(
        refused(&[
            "*",
            "GROUPBY",
            "1",
            "@t",
            "REDUCE",
            "COUNT_DISTINCTISH",
            "1",
            "@n"
        ])
        .contains("ADR-0064")
    );
    assert_eq!(
        refused(&["*", "GROUPBY", "1", "@t", "REDUCE", "COUNT", "1", "@n"]),
        "Count accepts 0 values only"
    );
    assert_eq!(
        refused(&["*", "GROUPBY", "1", "@t", "REDUCE", "COUNT", "3"]),
        "Bad arguments for COUNT: Expected an argument, but none provided"
    );
    assert_eq!(
        refused(&["*", "LOAD", "1", "@a", "SCORER", "BM25"]),
        "Unknown argument `SCORER` at position 6 for <main>"
    );
    assert_eq!(
        refused(&["*", "BOGUS"]),
        "Unknown argument `BOGUS` at position 3 for <main>"
    );
    assert_eq!(
        refused(&["*", "RETURN", "1", "a"]),
        "`RETURN` is not supported on FT.AGGREGATE"
    );
    assert_eq!(
        refused(&["*", "LOAD", "x"]),
        "Bad arguments for LOAD: Expected number of fields or `*`"
    );
    assert_eq!(
        refused(&["*", "LOAD", "2", "@a", "AS"]),
        "LOAD path AS name - must be accompanied with NAME"
    );
    assert_eq!(
        refused(&["*", "LOAD", "3", "@a", "AS", "AS"]),
        "Alias for LOAD cannot be `AS`"
    );
    assert_eq!(
        refused(&["*", "WITHCURSOR", "COUNT", "0"]),
        "Bad arguments for COUNT: Value is outside acceptable bounds"
    );
    assert_eq!(
        refused(&["*", "TIMEOUT", "-1"]),
        "TIMEOUT requires a non negative integer"
    );
    assert!(refused(&["*", "DIALECT", "3"]).contains("2 only"));
    assert!(refused(&["*", "VERBATIM"]).contains("ADR-0064"));
    assert!(refused(&["*", "FORMAT", "EXPAND"]).contains("STRING only"));
    assert!(refused(&["*", "APPLY", "@a-1", "AS", "x"]).starts_with("Syntax error"));
    assert_eq!(
        req(&["*", "WITHCURSOR", "MAXIDLE", "999999"])
            .expect("clamped")
            .cursor,
        Some(CursorOpts {
            count: CURSOR_COUNT,
            max_idle_ms: CURSOR_MAX_IDLE_MS
        })
    );
}

/// Without `AS`, a reducer's output is named as RediSearch names it.
#[test]
fn a_reducer_without_as_gets_redisearch_s_name() {
    let r = req(&[
        "*",
        "GROUPBY",
        "1",
        "@t",
        "REDUCE",
        "COUNT",
        "0",
        "REDUCE",
        "SUM",
        "1",
        "@Price",
        "REDUCE",
        "FIRST_VALUE",
        "4",
        "@title",
        "BY",
        "@n",
        "DESC",
    ])
    .expect("parses");
    let Step::Group { reducers, .. } = &r.steps[0] else {
        panic!("a group");
    };
    let names: Vec<&str> = reducers.iter().map(|r| &*r.alias).collect();
    assert_eq!(
        names,
        [
            "__generated_aliascount",
            "__generated_aliassumprice",
            "__generated_aliasfirst_valuetitle,by,n,desc"
        ]
    );
}

/// What a step may name, as RediSearch resolves it: `APPLY` and `FILTER`
/// a loaded name, an earlier alias or a `SORTABLE` attribute; `GROUPBY`,
/// `SORTBY` and a reducer any attribute, before the first `GROUPBY`.
#[test]
fn names_resolve_as_redisearch_resolves_them() {
    assert_eq!(
        plan(&["*", "APPLY", "@body", "AS", "x"], None).expect_err("not sortable"),
        "Property not loaded nor in pipeline: `body`"
    );
    plan(&["*", "APPLY", "@title", "AS", "x"], None).expect("sortable");
    plan(
        &["*", "LOAD", "1", "@body", "APPLY", "@body", "AS", "x"],
        None,
    )
    .expect("loaded");
    assert_eq!(
        plan(&["*", "SORTBY", "1", "@nope"], None).expect_err("unknown"),
        "Property `nope` not loaded nor in schema"
    );
    plan(&["*", "SORTBY", "1", "@body"], None).expect("any attribute");
    assert_eq!(
        plan(&["*", "GROUPBY", "1", "@nope"], None).expect_err("unknown"),
        "No such property `nope`"
    );
    assert_eq!(
        plan(
            &["*", "GROUPBY", "1", "@t", "REDUCE", "SUM", "1", "@nope"],
            None
        )
        .expect_err("unknown"),
        "Property not loaded nor in pipeline: `nope`"
    );
    assert_eq!(
        plan(&["*", "GROUPBY", "1", "@t", "SORTBY", "1", "@n"], None).expect_err("gone"),
        "Property `n` not loaded nor in schema"
    );
    assert_eq!(
        plan(&["*", "GROUPBY", "1", "@t", "LOAD", "1", "@n"], None).expect_err("after"),
        "LOAD cannot be applied after projectors or reducers"
    );
    assert_eq!(
        plan(
            &["*", "GROUPBY", "1", "@t", "REDUCE", "COUNT", "0", "AS", "t"],
            None
        )
        .expect_err("twice"),
        "Property `t` specified more than once"
    );
}

/// Loads come from the index where it holds the field as the document
/// does; a field outside the schema, a GEO field, or a SORTABLE one the
/// index keeps folded is read. A read no step needs waits for the reply's
/// rows.
#[test]
fn loads_read_only_what_the_index_does_not_hold() {
    let p = plan(
        &[
            "*", "LOAD", "6", "@n", "@body", "@title", "@u", "@loc", "@user", "LOAD", "1", "@__key",
        ],
        None,
    )
    .expect("compiles");
    let load = |name: &str| p.root[slot(&p, name)].load.clone().expect("a load");
    assert_eq!(load("n"), Load::Held(2));
    assert_eq!(load("body"), Load::Held(1));
    assert_eq!(load("u"), Load::Held(5), "SORTABLE UNF: held as written");
    assert_eq!(
        load("title"),
        Load::Read("title".into()),
        "held folded, so read"
    );
    assert_eq!(load("loc"), Load::Read("loc".into()));
    assert_eq!(load("user"), Load::Read("user".into()));
    assert_eq!(load("__key"), Load::Key);
    assert!(!p.early_reads);
    assert_eq!(p.late_reads.len(), 3);
    let p = plan(&["*", "LOAD", "1", "@user", "FILTER", "@user == 'x'"], None).expect("compiles");
    assert!(p.early_reads, "a step reads it");
    let p = plan(&["*", "GROUPBY", "1", "@loc"], None).expect("compiles");
    assert!(p.early_reads, "an implicit load of a read field");
    let p = plan(&["*", "LOAD", "1", "@user", "GROUPBY", "1", "@t"], None).expect("compiles");
    assert!(
        !p.early_reads && p.late_reads.is_empty(),
        "nothing to read after a GROUPBY"
    );
}

/// redisvl's router over three routes' references: the nearest per route,
/// sorted, `MAX 1`, then the threshold.
#[test]
fn the_router_s_pipeline_runs_as_redisearch_s() {
    let parts = [
        "@v:[VECTOR_RANGE $r $vec]=>{$YIELD_DISTANCE_AS: vector_distance}",
        "GROUPBY",
        "1",
        "@route_name",
        "REDUCE",
        "MIN",
        "1",
        "vector_distance",
        "AS",
        "distance",
        "SORTBY",
        "1",
        "@distance",
        "MAX",
        "1",
        "FILTER",
        "(@route_name == 'greeting' && @distance < 0.3) || (@route_name == 'farewell' && @distance < 0.5)",
    ];
    let mut d = def();
    d.attrs.push(
        parse_create(&a(&[
            "FT.CREATE",
            "x",
            "PREFIX",
            "1",
            "r:",
            "SCHEMA",
            "route_name",
            "TAG",
        ]))
        .expect("parses")
        .1
        .attrs
        .remove(0),
    );
    let p = compile(&req(&parts).expect("parses"), &d, Some("vector_distance")).expect("compiles");
    assert_eq!(p.metric, Some(0));
    let rows = |dists: &[(&str, f64)]| -> Vec<Row> {
        dists
            .iter()
            .enumerate()
            .map(|(i, (route, dist))| {
                let mut r = Row::root(&p, format!("r:{i}").as_bytes(), i as u64);
                r.vals[0] = Some(n(*dist));
                let rs = slot(&p, "route_name");
                r.loads[rs] = Some(s(route));
                r
            })
            .collect()
    };
    let ran = run(
        &p,
        rows(&[
            ("greeting", 0.25),
            ("farewell", 0.1),
            ("greeting", 0.2),
            ("farewell", 0.6),
        ]),
        1,
    )
    .expect("runs");
    assert_eq!(shown(&ran), [["route_name=farewell", "distance=0.1"]]);
    // The nearest route is past its threshold: MAX 1 kept only it, and the
    // FILTER drops it, as RediSearch's order of steps does.
    let ran = run(&p, rows(&[("greeting", 0.35), ("farewell", 0.45)]), 1).expect("runs");
    assert!(ran.rows.is_empty());
}

#[test]
fn reducers_compute_as_redisearch_s() {
    let p = plan(
        &[
            "*",
            "GROUPBY",
            "1",
            "@t",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "c",
            "REDUCE",
            "SUM",
            "1",
            "@price",
            "AS",
            "sum",
            "REDUCE",
            "AVG",
            "1",
            "@price",
            "AS",
            "avg",
            "REDUCE",
            "MIN",
            "1",
            "@price",
            "AS",
            "min",
            "REDUCE",
            "MAX",
            "1",
            "@price",
            "AS",
            "max",
            "REDUCE",
            "STDDEV",
            "1",
            "@price",
            "AS",
            "sd",
            "REDUCE",
            "COUNT_DISTINCT",
            "1",
            "@body",
            "AS",
            "cd",
            "REDUCE",
            "TOLIST",
            "1",
            "@body",
            "AS",
            "list",
            "REDUCE",
            "FIRST_VALUE",
            "4",
            "@body",
            "BY",
            "@price",
            "DESC",
            "AS",
            "first",
        ],
        None,
    )
    .expect("compiles");
    let rows = vec![
        row(
            &p,
            "doc:1",
            1,
            &[("t", s("a")), ("price", n(2.0)), ("body", s("x"))],
        ),
        row(
            &p,
            "doc:2",
            2,
            &[("t", s("a")), ("price", n(4.0)), ("body", s("y"))],
        ),
        row(
            &p,
            "doc:3",
            3,
            &[("t", s("a")), ("price", n(9.0)), ("body", s("x"))],
        ),
        row(&p, "doc:4", 4, &[("t", s("b")), ("body", s("z"))]),
        row(&p, "doc:5", 5, &[("price", n(1.0))]),
    ];
    let ran = run(&p, rows, 1).expect("runs");
    assert_eq!(ran.total, 3, "the count is the groups");
    assert_eq!(
        shown(&ran),
        [
            vec![
                "t=a",
                "c=3",
                "sum=15",
                "avg=5",
                "min=2",
                "max=9",
                "sd=3.60555127546",
                "cd=2",
                "list=x,y",
                "first=x"
            ],
            vec![
                "t=b", "c=1", "sum=nan", "avg=nan", "min=inf", "max=-inf", "sd=0", "cd=1",
                "list=z", "first=z"
            ],
            // A missing group value is null, and its group is shown as nil.
            vec![
                "t=", "c=1", "sum=1", "avg=1", "min=1", "max=1", "sd=0", "cd=0", "list=", "first="
            ],
        ]
    );
    assert_eq!(ran.rows[2].vals[0], Some(Datum::Null));
}

/// An array value puts its row in a group for each element.
#[test]
fn an_array_groups_by_each_element() {
    let p = plan(
        &["*", "GROUPBY", "1", "@t", "REDUCE", "COUNT", "0", "AS", "c"],
        None,
    )
    .expect("compiles");
    let rows = vec![
        row(&p, "doc:1", 1, &[("t", Datum::Arr(vec![s("a"), s("b")]))]),
        row(&p, "doc:2", 2, &[("t", s("b"))]),
        row(&p, "doc:3", 3, &[("t", Datum::Arr(vec![]))]),
    ];
    let ran = run(&p, rows, 1).expect("runs");
    assert_eq!(
        shown(&ran),
        [vec!["t=a", "c=1"], vec!["t=b", "c=2"], vec!["t=", "c=1"]]
    );
}

/// `SORTBY`: a missing value last either way, numbers before a string that
/// is not one by its text, a tie by write order in the last key's
/// direction, 10 rows with neither `MAX` nor `LIMIT`.
#[test]
fn sorting_follows_redisearch() {
    let p = plan(
        &["*", "LOAD", "1", "@price", "SORTBY", "2", "@price", "ASC"],
        None,
    )
    .expect("compiles");
    let mk = |vals: &[Option<f64>]| -> Vec<Row> {
        vals.iter()
            .enumerate()
            .map(|(i, v)| {
                let vs: Vec<(&str, Datum)> = v.iter().map(|x| ("price", n(*x))).collect();
                row(&p, &format!("doc:{i}"), i as u64, &vs)
            })
            .collect()
    };
    let keys = |ran: &Ran| -> Vec<String> {
        ran.rows
            .iter()
            .map(|r| String::from_utf8_lossy(&r.doc.as_ref().expect("a doc").0).into_owned())
            .collect()
    };
    let ran = run(&p, mk(&[Some(3.0), None, Some(1.0), Some(3.0)]), 1).expect("runs");
    assert_eq!(keys(&ran), ["doc:2", "doc:0", "doc:3", "doc:1"]);
    let p = plan(
        &["*", "LOAD", "1", "@price", "SORTBY", "2", "@price", "DESC"],
        None,
    )
    .expect("compiles");
    let mk2 = |vals: &[Option<f64>]| -> Vec<Row> {
        vals.iter()
            .enumerate()
            .map(|(i, v)| {
                let vs: Vec<(&str, Datum)> = v.iter().map(|x| ("price", n(*x))).collect();
                row(&p, &format!("doc:{i}"), i as u64, &vs)
            })
            .collect()
    };
    let ran = run(&p, mk2(&[Some(3.0), None, Some(1.0), Some(3.0)]), 1).expect("runs");
    assert_eq!(
        keys(&ran),
        ["doc:3", "doc:0", "doc:2", "doc:1"],
        "missing last, ties DESC"
    );
    let ran = run(&p, mk2(&[Some(1.0); 25]), 1).expect("runs");
    assert_eq!(ran.rows.len(), 10, "no MAX, no LIMIT: 10");
    assert_eq!(ran.total, 25);
    assert_eq!(
        sort_cmp(&n(5.0), &s("abc")),
        Ordering::Less,
        "\"5\" < \"abc\" as text"
    );
    assert_eq!(
        sort_cmp(&n(5.0), &s("10")),
        Ordering::Less,
        "a numeric string as a number"
    );
}

/// `LIMIT 0 0` answers the count alone; a `FILTER` takes its dropped rows
/// off the count; a `LIMIT` with no sort pages in index order.
#[test]
fn the_count_and_the_pages_follow_redisearch() {
    let p = plan(
        &[
            "*",
            "LOAD",
            "1",
            "@price",
            "FILTER",
            "@price > 1",
            "LIMIT",
            "1",
            "2",
        ],
        None,
    )
    .expect("compiles");
    assert!(p.buffered, "a LIMIT pages the whole result");
    let rows: Vec<Row> = (0..6)
        .map(|i| row(&p, &format!("doc:{i}"), i, &[("price", n(i as f64))]))
        .collect();
    let ran = run(&p, rows.clone(), 1).expect("runs");
    assert_eq!(ran.total, 4);
    assert_eq!(shown(&ran), [["price=3"], ["price=4"]]);
    let p0 = plan(&["*", "LIMIT", "0", "0"], None).expect("compiles");
    let ran = run(&p0, vec![Row::root(&p0, b"doc:1", 1)], 1).expect("runs");
    assert_eq!((ran.total, ran.rows.len()), (1, 0));
}

/// The hybrid query's row: the distance, `__score`, the loads, then the
/// aliases, in the order their names were made; a missing value left out.
#[test]
fn a_row_shows_its_names_in_the_order_they_were_made() {
    let p = plan(
        &[
            "*",
            "ADDSCORES",
            "LOAD",
            "2",
            "@body",
            "@price",
            "APPLY",
            "(2 - @d)/2",
            "AS",
            "sim",
            "APPLY",
            "@__score",
            "AS",
            "ts",
            "SORTBY",
            "2",
            "@sim",
            "DESC",
            "MAX",
            "7",
        ],
        Some("d"),
    )
    .expect("compiles");
    let mut r = row(&p, "doc:1", 1, &[("body", s("hello"))]);
    r.vals[p.metric.expect("metric")] = Some(n(0.5));
    r.vals[p.score.expect("score")] = Some(n(0.287_682_072_451_780_9));
    let ran = run(&p, vec![r], 1).expect("runs");
    assert_eq!(
        shown(&ran),
        [[
            "d=0.5",
            "__score=0.287682072452",
            "body=hello",
            "sim=0.75",
            "ts=0.287682072452"
        ]]
    );
    let Value::Array(Some(out)) = reply(ran.total, &ran.names, &ran.rows) else {
        panic!("an array");
    };
    assert_eq!(out[0], Value::Integer(1));
    let Value::Array(Some(fields)) = &out[1] else {
        panic!("a row");
    };
    assert_eq!(fields.len(), 10, "price is missing, so left out");
    assert_eq!(fields[1], Value::Bulk(Some(b"0.5".to_vec())));
}

/// A missing value fails the query, as RediSearch's evaluator does.
#[test]
fn a_missing_value_fails_an_apply() {
    let p = plan(
        &["*", "LOAD", "1", "@price", "APPLY", "@price * 2", "AS", "x"],
        None,
    )
    .expect("compiles");
    let err = run(&p, vec![row(&p, "doc:1", 1, &[])], 1).expect_err("fails");
    assert!(err.starts_with("Could not find the value"), "{err}");
}
