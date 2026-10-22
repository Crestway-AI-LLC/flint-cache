# ADR-0063: Full-text search in `FT.*`, which redisvl's `TextQuery` and LangChain's chat history send

Status: **ACCEPTED 2026-10-22, option B** (Jeff: "Accept B, build it"), with
sub-decision 1 (i), Snowball v2.1.0's own code, and sub-decision 2,
English only for now. BUG-0272 is gated with its build. **Proposed
2026-10-22.**
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
- **A search with no text word** under the default scorer: RediSearch's
  `BM25STD` scores tags and numbers 0, so their results stay in write
  order, as today. Two cases are settled from the source before the build
  claims either:
  - `*`: `BM25STD` scores a wildcard match by the document's length (the
    `Virtual` case in `default.c`). On an index with TEXT fields, that
    may order a `*` search by length rather than by write order.
  - the other scorers: `TFIDF` counts a tag as a word of frequency 1.
  If either shows ADR-0060's write order is wrong, it is filed as a bug and
  fixed with this.
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
- **The meter:** `tests/meter.rs` holds the postings' bytes, as it holds
  the rest.
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
  (`UNF`).
- `FT.SEARCH` without a KNN still reads every document for a search with no
  text word, as it does today. Postings for tags and numbers would end
  that, and are a separate question.
