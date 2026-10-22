# ADR-0063: Full-text search in `FT.*`, which redisvl's `TextQuery` and LangChain's chat history send

Status: **ACCEPTED 2026-10-22, option B** (Jeff: "Accept B, build it"), with
sub-decision 1 (i), Snowball v2.1.0's own code, and sub-decision 2,
English only for now; **built 2026-10-22** (As built, below), with
BUG-0272 and BUG-0273, to be gated together. **Proposed 2026-10-22.**
It builds on ADR-0060's `FT.*` surface, ADR-0059's filter, and BUG-0272,
which makes a TEXT value's words RediSearch's (public main after
v0.1.0-rc.85, branch stack-1022).

## Context

Today a TEXT field answers one query, an exact phrase, `@f:("a phrase")`.
Any other text term is refused by name. So are `WITHSCORES` and the
choice of scorer: `SCORER` is accepted and changes nothing.

**What it costs.** The three clients' suites, run against a local fleet on
2026-10-21 (public d5feac5f), with each failing test put down to one cause:

| cause | redisvl | langchain-redis | the same, RESP2 |
|---|---|---|---|
| **`FT.SEARCH ... WITHSCORES`** | **22** | **1** | **6** |
| **a text term other than a phrase** | **8** | **6** | **6** |
| `FT.AGGREGATE` | 8 | 0 | 0 |
| `INDEXEMPTY` / `INDEXMISSING` | 6 | 0 | 0 |
| `SORTBY` on a TEXT field | 2 | 0 | 0 |

Every `WITHSCORES` failure is a full-text search: redisvl's `TextQuery`
sets it on every query. The first two rows are therefore one feature, 30 of
redisvl's 71 failures and errors, and 12 of langchain-redis's 16 under
RESP2. Most of langchain-redis's other RESP3 failures are its own parsing of
`FT.INFO` (ADR-0060's verification).

**What the clients send.**
- **redisvl's `TextQuery`,** which langchain-redis's
  `RedisChatMessageHistory.search_messages` uses:
  - `@f:(w1 | w2 | ...)`: the user's words, lowercased and escaped, with
    redisvl's own English stop words removed unless the caller passes
    `stopwords=None`, as langchain-redis does;
  - a word's weight as `w=>{$weight:2.0}`, and a field's as
    `@f:(...) => { $weight: 5.0 }`;
  - several fields ORed in parentheses, ANDed with the filter;
  - `SCORER <name>`, `BM25STD` by default, and `WITHSCORES`.
- **redisvl's `Text` filter:**
  - `==` is a phrase and `!=` its negation, both served today;
  - `%` writes its pattern as it is: `ba*`, `engine*|doctor`,
    `*powerful engine*` and `%%engine%%` in the suites.
- **Filters the tests write by hand:** `@job:engineer`, `(-@job:engineer)`,
  `@job:enginee*`, `@job:(engine*|doctor)`, `@job:*engine*`.
- **Scorers the tests name:** `BM25STD`, `BM25`, `TFIDF`, `TFIDF.DOCNORM`,
  `DISMAX`, `DOCSCORE`.
- **Index options already accepted:**
  - kept in the definition: `STOPWORDS`, and a TEXT field's `WEIGHT`,
    `NOSTEM` and `WITHSUFFIXTRIE`;
  - read and then dropped: `SCORE`, `SCORE_FIELD`, `LANGUAGE` and
    `LANGUAGE_FIELD`.

**What the tests check:** how many results, which ones, and for weights,
which comes first. None checks a score's value.

### What RediSearch does, from its source

Read 2026-10-22 from RediSearch's default branch (`src/ext/default.c`,
`src/tokenize.c`, `src/stemmer.c`, `src/stopwords.h`,
`src/forward_index.c`, `src/query_parser/v2/`, `src/result_processor.c`,
and the Rust `idf` and `query_eval` crates). Facts a research agent
reported and I did not check again are marked *(agent)*.

- **Words:** split and lowercased as BUG-0272 now does.
- **Stop words:**
  - The default list has 33 English words. `STOPWORDS n ...` replaces it,
    and `STOPWORDS 0` empties it.
  - A stop word is left out of the index, and takes no position.
  - In a query, a bare word that is a stop word is dropped. Inside a quoted
    phrase it is kept, and since the index never holds it, the phrase
    matches nothing. `parser.y`'s phrase rule has no stop word check, and
    RediSearch's `testNoStopwords` shows a kept stop word matching nothing.
- **Stemming** is on unless a field is `NOSTEM`:
  - The stemmer is Snowball's, from its `libstemmer`. RediSearch's
    `deps/snowball` submodule is at 4764395431c8, the commit Snowball tagged
    v2.1.0 (2021-01-21). Snowball's English changed in 2025, so a newer
    Snowball stems some words differently. English unless `LANGUAGE`, a document's
    `LANGUAGE_FIELD`, or the query's `LANGUAGE` says otherwise.
  - At indexing, a word of 4 bytes or more whose stem differs from it is
    also indexed as `+stem`. The stem's frequency is a fifth of the word's
    weight, and at least 1.
  - In a query, a word becomes the union of itself, `+stem`, and the bare
    stem when that differs (`StemmerExpander`). A quoted phrase is not
    expanded, and neither is a word on a `NOSTEM` field.
- **A field's `WEIGHT`** acts only at indexing. Each occurrence of a word
  adds the weight, cut to a whole number and at least 1, to the word's
  frequency in the document, and to the document's length. A word's
  frequencies in a document's fields add into one.
- **The query language, `DIALECT 2`:**
  - A bare word searches every TEXT field; `@f:` and `@f1|f2:` name some.
  - `(a b)` is AND and `a | b` is OR, and AND binds tighter: `a b | c` is
    `(a b) | c`.
  - `-x` is NOT, and `~x` is optional, matching without being needed.
  - `x*` is a prefix of at least 2 characters, read from at most 200 words.
  - `*x` is a suffix and `*x*` a contains. Without `WITHSUFFIXTRIE`
    RediSearch walks every word it holds, so the option only makes them
    faster.
  - `%x%`, `%%x%%` and `%%%x%%%` allow 1, 2 and 3 edits.
  - `"..."` is an exact phrase: its words in order, with nothing between.
  - Attributes `=>{$weight: w; $slop: n; $inorder: true}`.
- **Scorers,** each a sum over the query's tree of matching words. An AND
  or an OR adds its children, and a node's `$weight` multiplies its part:
  - `TFIDF`: a word's weight × its frequency × IDF, where IDF is
    `floor(log2(1 + (N + 1) / n))` for N documents, n of them holding the
    word. The sum is multiplied by the document's score, divided by the
    document's largest word frequency, and divided by the "slop", the
    spread of its words' positions.
  - `TFIDF.DOCNORM`: the same, divided by the document's length instead.
  - `BM25` (the old one): IDF as `TFIDF`'s, with k1 1.2 and b 0.5 over the
    average document length only.
  - `BM25STD`: `ln(1 + (N - n + 0.5) / (n + 0.5))` × f × 2.2 /
    (f + 1.2 × (0.25 + 0.75 × length / average length)), times the
    document's score, with no slop.
  - `BM25STD.TANH` and `BM25STD.NORM` rescale `BM25STD`'s scores.
  - `DISMAX` sums frequencies under an AND, and takes the largest under an
    OR.
  - `DOCSCORE` is the document's score alone.
  - `HAMMING` compares payloads.
- **The default scorer** is `BM25STD` since RediSearch 8.0. Before that it
  was `TFIDF` *(agent, from the 2.10 and 8.0 branches)*.
- **A document's score** is `SCORE` for the index, 1 unless set, or the
  document's `SCORE_FIELD`.
- **`WITHSCORES`:** RESP2 puts each result's score right after its key, as
  a double. RESP3 adds a `score` entry to each result's map.
- **Order:**
  - Without `SORTBY`, results come by score, highest first. A tie goes to
    the lower document id, the earlier write.
  - Under `DIALECT 2`, every search with no `SORTBY` is scored, by the
    default scorer when it names none (`pipeline_construction.c`).
- **The oracle:** RediSearch's `tests/pytests/test_scorers.py`
  `testScoreIndex`. It builds a 24-document index with a field `WEIGHT 10`
  and a `SCORE_FIELD`, then gives the top five keys and their scores, to
  two decimals, for each of `TFIDF`, `TFIDF.DOCNORM`, `BM25`, `BM25STD`,
  `BM25STD.TANH`, `DISMAX` and `DOCSCORE`. It checks them again after a
  reload. Its `EXPLAINSCORE` tests give each word's IDF, frequency and
  lengths for three more documents.

## Options

### A. Match every text term, with no scores

Answer each term above by checking each document's words, as a phrase is
checked today, with stop words and stems. `WITHSCORES` stays refused.

- Serves the 20 failures from text filters (langchain-redis's tests count
  once in each protocol). A filter has no score.
- Leaves the 29 `WITHSCORES` failures, and every `TextQuery` with them,
  so langchain-redis's chat history still cannot search.
- Reads every document under the prefix for every search.

### B. An inverted index scored as RediSearch scores (recommended)

Each TEXT field's words go into an index, as RediSearch keeps them:
- **A posting list for each word, and for each `+stem`:** the documents
  that hold it, with its frequency, its fields and its positions.
- **For each document:** its length and its largest word frequency.
- **For the index:** its number of documents, their total length, and each
  word's number of documents.

A search reads only the postings of its words:
- A prefix is a range of the ordered word list.
- A suffix, a contains, or a fuzzy term walks that list, as RediSearch
  does without a suffix trie.
- A phrase, and an AND with `$slop` or `$inorder`, compare positions.

Each match is scored by RediSearch's own sum over the query's tree, with
its constants, under the scorer asked for. The answer comes highest score
first, unless `SORTBY` says otherwise, with `WITHSCORES` if asked.

- Serves all 49 failures in the table's first two rows, and every query
  the three clients write.
- A search costs what its words' postings cost. A chat history keeps every
  session's messages in one index, and a word's postings stay short where a
  scan would read them all.
- The most code of the `FT.*` ADRs: the postings, kept right through every
  rewrite, delete and expiry; a parser for the text grammar; six scorers;
  and `WITHSCORES` in both protocols.
- Memory: a posting for each distinct word in each document, and a
  position for each occurrence. They are charged to the D4 meter, as the
  rest of the index is.

### C. B, and the rest of RediSearch's text surface

That rest: phonetic matching, synonyms, `HIGHLIGHT` and `SUMMARIZE`,
`FT.SPELLCHECK`, suggestions, `FT.EXPLAIN`, `EXPLAINSCORE`, the query
options `VERBATIM`, `NOSTOPWORDS`, `SLOP`, `INORDER` and `LANGUAGE`, and
Chinese word segmentation.

- None of the three clients sends any of these.
- Each can be added after B without changing it.

## Recommendation

**B.** It is the only option that serves what the frameworks send:
`TextQuery` always asks for scores. It is also the only one whose cost
follows the words searched rather than the size of the index.

### Its scope

- **`FT.CREATE`:**
  - TEXT with `WEIGHT`, `NOSTEM`, `SORTABLE` and `WITHSUFFIXTRIE`, as
    today.
  - `STOPWORDS`, now applied.
  - `SCORE` and `SCORE_FIELD`, now kept in the definition and applied.
  - `LANGUAGE` and `LANGUAGE_FIELD`, under sub-decision 2.
- **Indexing:** BUG-0272's words, then stop words, stems, frequencies and
  lengths, as RediSearch counts each.
- **`FT.SEARCH`:**
  - every term in "The query language" above, with RediSearch's precedence;
  - `$weight` on any term or group, and `$slop` and `$inorder` on an AND;
  - `SCORER` with the six scorers above, `BM25STD.TANH` and `BM25STD.NORM`;
  - `WITHSCORES`, in both protocols.
  - **Ordering:** results by score, then write order. A tie goes to the
    earlier write, as today (ADR-0060 amendment 3, part 6).
  - Text terms beside a KNN or a vector range filter its candidates, as a
    tag does.
- **Refused by name:**
  - `HAMMING`, which needs payloads;
  - `$phonetic`;
  - `WITHSCORES` with a KNN or a vector range, which none of the clients
    sends;
  - everything under C.
- **Phrases follow RediSearch from this release:** a phrase is not
  stemmed, and a phrase with a stop word in it matches nothing. That
  changes what today's phrase answers. FT.* has not shipped in a release,
  so nobody depends on the old answer.
- **A search with no text word is scored too,** as RediSearch scores it.
  ADR-0060 answers such a search in write order. Read from RediSearch's
  source after this ADR was first written (`rqe_iterators`'
  `inverted_index/tag.rs`, `wildcard.rs`, `numeric.rs`):
  - A TAG match is a word's result: frequency 1, and the tag value's own
    IDF.
  - A `*` match is a virtual result of frequency 1, which `BM25STD` scores
    by the document's length.
  - A number or a point scores 0 under `BM25STD`.
  So on an index with TEXT fields, RediSearch orders a tag or `*` search
  by score, the shortest documents first, and not by write order. On an
  index with no TEXT field every length is 0 and the scores tie, so write
  order holds. This is filed as a bug against ADR-0060 and fixed with this
  build.
- **Releases:**
  - The definition row gains `score`, `score_field` and the language keys.
    An older co-processor reads the definition by the keys it knows and
    ignores the rest, so it still loads the index. It then answers TEXT as
    phrases only, and refuses the rest by name.
  - Nothing else that is durable changes: the index is rebuilt from the
    keys. So it needs one release with no new flag. `coproc-ft` already
    keeps `FT.*` off unless the operator turns it on.
  - An index made before this release dropped its `SCORE`, `LANGUAGE`, and
    the rest. It scores with the defaults until it is made again.

### Sub-decisions for Jeff

1. **Where the stemmer comes from.** Exactness needs RediSearch's Snowball,
   v2.1.0.
   - **(i) Snowball's own Rust output (recommended).** Generate it from
     v2.1.0's `english.sbl` with Snowball's compiler, and vendor the
     generated file and its small runtime (BSD-3-Clause).
     - Checked word for word against Snowball's `snowball-data`: each
       English word's expected stem. Its `english/voc.txt` and
       `output.txt` last changed in 2006 (2837d237), so they are v2.1.0's.
     - Needs two downloads, each asked for when the build reaches it:
       Snowball's source at tag v2.1.0, built and run locally to generate
       the file, and the `snowball-data` vocabulary.
   - **(ii) The `rust-stemmers` crate (MIT).** It was generated from an
     older Snowball, so its stems can differ from v2.1.0's. The same
     vocabulary check would find any that do. Needs the crate and the
     vocabulary.
2. **Languages.**
   - **English only, for now (recommended).** `LANGUAGE english`, or no
     `LANGUAGE`, is accepted. Any other language, and `LANGUAGE_FIELD`, are
     refused by name. Every answer is then exact. None of the three clients
     sets a language.
   - **Every language with a Snowball v2.1.0 stemmer.** Each comes from the
     same generated code, checked against the same vocabulary. Chinese is
     refused, since it needs word segmentation.

### What will differ from RediSearch, on purpose

- **Scores after deletes and rewrites.** RediSearch counts a deleted
  document in a word's document count until its garbage collector runs.
  Flint counts only live documents. Scores can therefore differ for a
  while after a delete. With no deletes they agree.
- **A cluster.** A clustered RediSearch scores on each shard with that
  shard's counts, and its own test keeps a second set of expected scores
  for it. Flint keeps one index for each namespace, so it answers as a
  single-shard RediSearch does.

## As built (2026-10-22, for v0.1.0-rc.86)

- **Settled from RediSearch's source while building,** beyond the first
  text:
  - **A TAG match is a word's result** (BUG-0273, under Its scope).
  - **Stems are in the word list** that suffix, contains and fuzzy terms
    read, since RediSearch adds every forward-index term, stems included,
    to its terms trie. So `%%engine%%` also matches `engineering`, through
    its stem `+engin`. A prefix never matches a stem, which starts with `+`.
  - **A word's expansion** is the union of itself, `+stem` always, and the
    bare stem when it differs (`StemmerExpander`). The query side has no
    minimum length.
  - **The other results:**
    - a NUMERIC or GEO match is a numeric result of frequency 1 and
      weight 1;
    - a NOT match is a virtual result of frequency 0;
    - an unmatched optional term is a virtual result of frequency 1 and
      weight 0;
    - a matched optional term takes the optional's weight.
  - **An AND's children** are ordered by their expected match count, unless
    `$inorder`. This decides the slop divisor when three or more of them
    hold positions.
  - **The scorers' constants** are `f32`: `k1` 1.2, `b` 0.75 or 0.5, and
    `b × length` is multiplied in `f32`. A document's score is kept as a
    float.
  - **A HASH document's `SCORE_FIELD`** is a strict number with no range
    check. Anything else gives the index's `SCORE`.
- **Assumed, not settled:** a TAG term with several values, `@t:{a | b}`,
  is scored by the first value a document holds, as a prefix's words are.
  It changes a score only for a document holding two of the values.
- **The code:**
  - `crates/flint-vec/src/snowball/`: the generated stemmer, its runtime,
    and the vocabulary, vendored under Snowball's licence (sub-decision 1).
    `tools/gates.sh`'s licence-header check excludes its two `COPYING`
    files, with the reason.
  - `fulltext.rs`: a document's words, counted as RediSearch's forward
    index counts them, and the posting lists.
  - `ftquery.rs`: the parser, the plan, each document's match, and the
    scorers. It is written from RediSearch's rules, not its code, which is
    under RSAL, SSPL or AGPL.
  - `ftindex.rs`: documents carry their write id, score and TAG counts;
    every search is scored. A KNN with a text term searches among that
    term's matches (`VectorSet::search_among`, `Hnsw::knn_among`).
  - `ft.rs`, `ftstore.rs`, and the proxy's RESP3 shape, for the options and
    the reply.
- **Held by:**
  - `every_english_word_stems_as_snowball_v2_1_0_stems_it`, all 29,417
    words;
  - `every_scorer_answers_redisearch_s_own_test_index` and
    `full_text_answers_redisearch_s_own_test_index`: `testScoreIndex`, in
    the query module and through `FT.CREATE` and `FT.SEARCH`. Every scorer
    gave RediSearch's five keys and scores on the first run;
  - `postings_follow_every_rewrite_and_delete`: 400 random rewrites and
    deletes against an index built afresh from what is left. Twelve queries
    under five scorers give the same matches and the same scores, bit for
    bit;
  - the parser, stem, phrase, slop, affix, fuzzy, KNN and reply-shape
    tests beside them;
  - `tests/ft_meter.rs`;
  - `coproc_ft_drill`'s `text` and `textsame` sections, through the proxy,
    before and after a co-processor restart. They check BM25STD's scores
    against its formula, worked out in the drill's Python with RediSearch's
    `f32` constants.
  - Mutation-checked: the rewrite test fails with a delete that leaves its
    postings, and BUG-0273's test fails with write order.
- **Found while building it:**
  - **BUG-0272** (the words) and **BUG-0273** (the order).
  - **The meter undercharged a new word,** found by `tests/ft_meter.rs`. A
    word's first posting allocates a whole `BTreeMap` leaf, 368 bytes, so a
    document of words new to the index held 12.3 KB and was charged 7.3 KB.
    The charge for a new word is now 480 bytes beyond its bytes; that
    document is charged 14.9 KB.

### The clients' suites, as built (2026-10-22)

Against a local fleet, beside the run after ADR-0062 (its *As built*):

| suite | passed before | passed now | failed now | errors now |
|---|---|---|---|---|
| redisvl | 238 | 263 | 29 | 17 |
| langchain-redis, RESP3 | 150 | 157 | 24 | 23 |
| langchain-redis, RESP2 | 188 | 200 | 3 | 1 |
| LlamaIndex | 19 | 19 | 0 | 0 |

- **No failure left names `WITHSCORES` or a text term,** in any suite.
- **redisvl's 29:**
  - `sentence-transformers` 19, not installed;
  - `FT.AGGREGATE` 8;
  - `INDEXEMPTY` 6;
  - `SORTBY` on a TEXT field 7, up from 2. Its word-weight tests now get
    past their text query and sort by `description`;
  - its upsert script 4;
  - `EXPIRE` inside `MULTI` 2.
- **langchain-redis:** what is left is its own RESP3 parsing, `CROSSSLOT`
  and transactions, as before (ADR-0060). Its chat history's
  `search_messages` passes in both protocols.
- **Not served, and not counted above:** a JSON TEXT field holding an
  array of strings, which RediSearch indexes, is still read as missing.
  None of the three clients writes one.

## Verification

- **Units:**
  - the stemmer, against the whole vocabulary;
  - stop words, and the frequencies and lengths a document gets, with
    `WEIGHT`, stems and `NOSTEM`;
  - the parser, on each form the clients render and on RediSearch's
    precedence;
  - each scorer, against numbers worked by hand from `default.c` for
    terms, ANDs, ORs, phrases, optional terms, tags and `$weight`.
- **The oracle:** `testScoreIndex`, all seven scorers: the same five keys
  in the same order, and the same scores to two decimals. Checked once
  when built and again after a co-processor restart. Then the
  `EXPLAINSCORE` tests' IDF, frequency and length figures, as numbers.
- **Postings against a scan:** in tests, every search over random
  documents and queries is answered twice, from the postings and by
  checking each document's words. Both must give the same matches and the
  same scores, through rewrites, deletes and expiries.
- **The meter:** a test holds the postings' bytes, as `tests/meter.rs`
  holds a vector set's (as built: `tests/ft_meter.rs`).
- **`coproc_ft_drill`:** a `text` section that runs `TextQuery`-shaped
  searches with `WITHSCORES` through the proxy, before and after a
  co-processor restart. Scores must be equal across the restart, and ties
  must come in the rebuild's order.
- **The clients' suites:** the 49 failures in the table's first two rows
  pass. Each test that still fails must name another cause.
- **Cost:** a chat-history-shaped index of 100,000 messages, searched by a
  word and a session tag, measured against today's scan.

## What this does not decide

- Everything under C.
- `FT.AGGREGATE`, next by count, and redisvl's `HybridQuery`, which uses it.
- `INDEXEMPTY` and `INDEXMISSING`.
- `SORTBY` on a TEXT field, which needs RediSearch's sort normalization
  (`UNF`). Seven redisvl tests now wait on it (As built). Proposed as
  amendment 1, below.
- `FT.SEARCH` without a KNN still reads every document for a search with no
  text word, as it does today. Postings for tags and numbers would end
  that, and are a separate question.

## Amendment 1 (PROPOSED 2026-10-22): `SORTBY` on TEXT and TAG fields, by their values

**Why now.** `SORTBY` on a TEXT field is refused by name, and with full text
built it is what stops 7 of redisvl's tests:
- 4 are its word-weight tests, which sort by `description`, a TEXT field
  that is not `SORTABLE`;
- 3 sort by a `SORTABLE` TEXT field, two of them `NOINDEX`, one `UNF`.

Reading how RediSearch sorts also showed that Flint sorts a TAG field
differently from RediSearch (BUG-0274, filed with this).

**What RediSearch does, from its source** (`document.c`, `sorting_vector`,
`value/src/comparison.rs`, `pipeline_construction.c`):
- **A `SORTABLE` TEXT or TAG field** puts its whole value in the
  document's sorting vector, case-folded by ICU's `fold_string` unless the
  field is `UNF`. For a TAG that is the value before it is split: `"b,a"`.
- **A field that is not `SORTABLE`** is loaded from the key when a
  `SORTBY` names it, and sorted as it is written.
- **Strings compare as bytes,** ascending or descending as asked. A
  document without the field comes last either way, and a tie goes to the
  earlier write.

### Options

**A. Keep each TEXT and TAG field's sort value in the index (recommended).**
Each document keeps, for each TEXT and TAG field, its value as written,
folded when the field is `SORTABLE` without `UNF`. Any field sorts, with no
reads at search time.
- **Exact,** for every field, `SORTABLE` or not, as RediSearch sorts it.
- **Memory:** the field's text once more. That is small beside what full
  text already holds for the same text. A 500-byte message has about 60
  distinct words, at least 6 KB of postings and positions. So a TEXT-heavy
  index grows by about a tenth. It is charged to the D4 meter, and
  `tests/ft_meter.rs` holds it.
- **Fixes BUG-0274** with the same values.

**B. Keep only `SORTABLE` fields' values; read the rest at search time.** A
`SORTBY` on a field that is not `SORTABLE` reads that field from every
match's key over the channel, as RediSearch loads it.
- Exact, and no memory for fields that are not `SORTABLE`.
- A search sorted that way costs a read per match, up to the 10,000
  ceiling. The search path also has to read in the middle of a search,
  which it does nowhere today.

**C. Keep only `SORTABLE` fields' values; refuse the rest by name.**
- The least code.
- The 4 word-weight tests still fail. A TAG `SORTBY` that is served today
  on a field that is not `SORTABLE` would become a refusal.

### Sub-decision: how to fold

`SORTABLE` without `UNF` sorts by ICU's full Unicode case folding:
`Straße` folds to `strasse`, and a final `ς` folds to `σ`.
- **(i) A table generated from Unicode's `CaseFolding.txt` (recommended).**
  It holds the full foldings, statuses C and F, and is vendored like
  Snowball. Exact, with no new crate. It needs one download, the file
  from unicode.org, which I would ask for when the build reaches it.
- **(ii) Rust's lower case, character by character.** Exact for ASCII and
  most text, but it differs for about 200 code points: `ß`, the final
  sigma, Cherokee, and others.
- **(iii) The `icu_casemap` crate, which RediSearch uses** (Unicode-3.0, on
  `deny.toml`'s list). Exact, but a large new dependency with its data.

### Scope

- **`SORTBY` on a TEXT field,** `SORTABLE` or not, `NOINDEX` or not,
  ascending or descending, with or without a text query.
- **`SORTBY` on a TAG field,** by the same rule (BUG-0274).
- **A JSON value:** a string sorts as written, or folded. An array sorts by
  its first element, as RediSearch's array comparison does (to be settled
  from its source when built).
- **Unchanged:** NUMERIC and the KNN distance sort as they do now. `SORTBY`
  on a GEO field stays refused.
- **Releases:** no new definition row, and nothing durable, since the
  index is rebuilt from the keys. One release.

### Verification

- Unit tests:
  - redisvl's three orders (`Banana`, `ZEBRA`, `apple` with `UNF`;
    `apple`, `Banana`, `ZEBRA` without it);
  - BUG-0274's acceptance test;
  - missing values last in both directions.
- The fold, if (i): every mapping in the table checked against
  `CaseFolding.txt`'s own lines.
- `coproc_ft_drill`: a `SORTBY` on a TEXT field before and after a
  restart.
- The suites: the 7 redisvl tests pass, and nothing that passed now fails.
