# ADR-0064: `FT.AGGREGATE` and `FT.CURSOR`, which redisvl's bulk updates, semantic router and hybrid queries send

Status: **PROPOSED 2026-10-23.** It builds on ADR-0060's `FT.*` surface,
ADR-0061's vector ranges, ADR-0063's full text and scorers, and ADR-0063
amendment 1 (accepted 2026-10-22, being built), whose sort values it reads.

## Context

`FT.AGGREGATE` and `FT.CURSOR` are refused by name today (ADR-0060). Of
the three clients, only redisvl sends them. langchain-redis and LlamaIndex's
Redis store do not.

**What it costs.** redisvl's catalogued suites, on public e6f0ef34
(2026-10-22): 8 of the 29 failures are `FT.AGGREGATE`.
- 7 are bulk updates and deletes by filter (`test_bulk_operations.py`),
  which page through the matches with a cursor;
- 1 is a hybrid query (`test_stopwords_integration.py`).

Run on 2026-10-23 against the same build, redisvl's suites outside the
catalogue add more that wait on it, behind other causes:
- **`test_aggregation.py`,** 16 tests (25 with their parameters), every
  one a hybrid or multi-vector aggregate. Each fails first at its fixture:
  an index with three VECTOR fields, one of them `FLOAT64`, which ADR-0060
  refuses ("an index has at most one VECTOR field").
- **`test_semantic_router.py`,** 40 tests. The router matches by
  `FT.AGGREGATE`. Here every test stops first at its vectorizer, which
  needs `sentence-transformers`, not installed on this machine.

**What redisvl sends** (redisvl 0.28.0, printed from its own builders).
redis-py adds `SCORER TFIDF` to every request that names no scorer.
- **Paging through a filter's matches,** for `update_by_filter`,
  `delete_by_filter` and its index migrator:
  - `FT.AGGREGATE idx <filter> SCORER TFIDF WITHCURSOR COUNT 500 MAXIDLE
    300000 LOAD 1 __key DIALECT 2`;
  - then `FT.CURSOR READ idx <id>` until the id is 0, and `FT.CURSOR DEL
    idx <id>` if it stops early.

  It removes repeated keys across pages, because a RediSearch cursor can
  return a document again if it was rewritten while the cursor was open.
  The migrator falls back to `SCAN` on `Cursor not found`.
- **The semantic router,** with each route's reference phrases as documents:
  `FT.AGGREGATE idx '@vector:[VECTOR_RANGE $distance_threshold $vector]=>
  {$YIELD_DISTANCE_AS: vector_distance}' SCORER TFIDF DIALECT 2 GROUPBY 1
  @route_name REDUCE MIN 1 vector_distance AS distance SORTBY 1 @distance
  MAX 1 FILTER "(@route_name == 'greeting' && @distance < 0.3) || ..."`.
  The router may be configured to use `SUM` or `AVG` in place of `MIN`.
- **`AggregateHybridQuery`,** which combines a text score with a vector score:
  `FT.AGGREGATE idx '(~@description:(medical | lung | cancer) (<filter>))=>
  [KNN 7 @user_embedding $vector AS vector_distance]' SCORER BM25STD
  ADDSCORES LOAD 6 user credit_score age job location description DIALECT 2
  APPLY '(2 - @vector_distance)/2' AS vector_similarity APPLY @__score AS
  text_score APPLY '0.30000000000000004*@text_score + 0.7*@vector_similarity'
  AS hybrid_score SORTBY 2 @hybrid_score DESC MAX 7`.
- **`MultiVectorQuery`,** which searches several vector fields at once:
  `FT.AGGREGATE idx '@user_embedding:[VECTOR_RANGE 1.0 $vector_0]=>
  {$YIELD_DISTANCE_AS: distance_0} @image_embedding:[VECTOR_RANGE 0.5
  $vector_1]=>{$YIELD_DISTANCE_AS: distance_1} (@credit_score:{high})'
  SCORER TFIDF LOAD 2 user age DIALECT 2 APPLY '(2 - @distance_0)/2' AS
  score_0 APPLY '(2 - @distance_1)/2' AS score_1 APPLY '@score_0 * 0.3 +
  @score_1 * 0.7' AS combined_score SORTBY 2 @combined_score DESC MAX 5`.

All four use arithmetic, comparisons and `&&`/`||` in their expressions, and
no functions.

**How Flint reads documents today.** `FT.SEARCH` finds its matches in the
co-processor's index. It then reads each result back from the pairs over the
proxy's channel, one command per result, under a budget sized to its `LIMIT`
(ADR-0060 amendment 3, part 1). That fits a page of results. It does not fit
a `GROUPBY` over every match of a large index.

The index already holds each NUMERIC value. With ADR-0063 amendment 1 it
also holds each TEXT and TAG value, in the form RediSearch's sorting vector
holds it: folded for a `SORTABLE` field without `UNF`, as written otherwise.
So a pipeline over schema fields can run on the index's own values, and read
back only the rows that survive it.

**Where a cursor would live.** Every proxy sends an `FT.` command to the
first of the family's co-processors that answers, and `flintctl` lists them
in the same order for every proxy (`families_arg`). A cursor kept in that
co-processor's memory is found by the next `FT.CURSOR READ`. It is lost only
when that co-processor stops, as RediSearch's cursors are lost when Redis
restarts. redisvl's migrator already handles that answer.

### What RediSearch does, from its source

Read on 2026-10-23 from RediSearch's master:
- `aggregate_request.c`, `aggregate_exec.c`, `pipeline_construction.c`;
- the reducer and value crates under `src/redisearch_rs`;
- `aggregate/expr/` and `aggregate/functions/`, `cursor.c`;
- `test_aggregate*.py` and `test_cursors.py`.

**Grammar.**
- Query options come first: `SCORER`, `ADDSCORES`, `VERBATIM` and the
  like.
- Then the steps, in the order written: `LOAD`, `GROUPBY ... REDUCE`,
  `APPLY`, `FILTER`, `SORTBY` and `LIMIT`. Each step sees what the steps
  before it produced.
- `LIMIT`, `SORTBY`, `WITHCURSOR`, `PARAMS`, `DIALECT` and `TIMEOUT` may
  come anywhere.
- `LOAD` comes only before the first `GROUPBY`.
- Only one `SORTBY` is allowed between two `GROUPBY`s, and a `LIMIT` joins
  the `SORTBY` of its stretch.
- `PARAMS` reach the query string only, never an expression.

**What a step can read.**
- **`LOAD`** reads named fields:
  - a NUMERIC schema field comes as a number, so a stored `1.0` reads `1`;
  - every other field comes as written;
  - a missing field is left out of the row.
- **`GROUPBY`, `SORTBY` and a reducer's argument** read any schema field
  without a `LOAD`, before the first `GROUPBY`.
- **`APPLY` and `FILTER`** read only:
  - what was loaded;
  - `SORTABLE` fields;
  - the names that earlier steps made.
- **A `SORTABLE` TEXT or TAG field** reads as its sorting vector holds it,
  folded unless the field is `UNF`.
- **Special names:**
  - `__key` is the document's key;
  - `__score` exists with `ADDSCORES`;
  - a KNN's or a range's distance appears under its own name, with no
    `LOAD`.

**Rows.**
- **Without `SORTBY`,** rows come in the index's own order: document ids,
  which is write order. A KNN comes nearest first.
- **`SORTBY` takes up to 8 keys,** ascending unless `DESC` is given, and
  `MAX n`. With neither `MAX` nor `LIMIT` it keeps only 10 rows.
- **Comparison:**
  - numbers compare as numbers, strings as bytes;
  - a missing value sorts last in either direction;
  - a tie goes to the lower document id.

**`GROUPBY`.**
- **Grouping:**
  - rows group by their typed values, so the number 1 and the string `"1"`
    are different groups;
  - a missing value groups as null, which the reply gives as nil;
  - an array value puts its row in one group per element.
- **A grouped row** holds only its group keys and its reducers' outputs.
- **Reducers:**
  - `COUNT`;
  - `COUNT_DISTINCT`, and `COUNT_DISTINCTISH`, a HyperLogLog estimate;
  - `SUM` and `AVG`, NaN with no numbers;
  - `MIN` and `MAX`, +inf and -inf with none;
  - `STDDEV`, the sample's;
  - `QUANTILE`, a CKMS sketch;
  - `TOLIST`;
  - `FIRST_VALUE ... BY`;
  - `RANDOM_SAMPLE`.
- **Without `AS`,** a reducer's output is named
  `__generated_alias<name><args>`, lowercased.

**Expressions** (`APPLY expr AS name`, `FILTER expr`).
- **Values:** numbers, `'` or `"` strings, `@property`, `NULL`, and
  function calls.
- **Operators,** loosest to tightest: `||`, `&&`, `!`, the comparisons, `+
  -`, `* / %`, `^`.
- **Signs:** a sign is part of a number, so `@a-1` is a syntax error and `@a -
  1` is not.
- **Arithmetic** on something that is not a number fails the whole query.
  `x/0` is ±inf.
- **`==`** compares a number with a numeric string as numbers.
- **A missing value** fails the query ("consider using EXISTS").
- **`FILTER`** keeps a row whose result is a non-zero number or a non-empty
  string.
- **Functions:**
  - 7 for numbers: `log`, `log2`, `exp`, `floor`, `ceil`, `abs`, `sqrt`;
  - 13 for strings, among them `lower`, `upper`, `substr`, `format`,
    `split`, `contains`, `startswith`, `strlen`, `to_number`, `to_str`,
    `exists` and `case`;
  - 11 for dates, among them `timefmt` and `parsetime`;
  - `geodistance`.

**Reply.**
- **RESP2:** `[total, [name, value, ...], ...]`.
- **RESP3:** a map of `attributes`, `format`, `results` (each
  `{extra_attributes, values}`), `total_results` and `warning`.
- **A cursor** wraps either as `[reply, id]`. The id is 0 once it is done.
- **Number rendering:** a number goes out as a string:
  - a whole number as an integer;
  - anything else as C's `%.12g`: `0.1+0.2` is `0.3`, `1/3` is
    `0.333333333333`, NaN is `nan`.
- **Missing and null:** a missing value is left out of its row; a null is
  nil.

**The total.**
- RESP2 writes the total before the rows, as a running count.
- After a `SORTBY` or `GROUPBY`, which read every row first, it counts all
  the matches or groups.
- Otherwise it counts only the rows read so far, often 1.
- `WITHCOUNT` makes it exact.

**Cursors.**
- **Defaults:** `COUNT` is 1000. `MAXIDLE` defaults to 300,000 ms, which
  is also its ceiling, and each read starts the idle time again.
- **Commands:**
  - `FT.CURSOR READ idx id [COUNT n]`; a new `COUNT` sticks for later
    reads;
  - `FT.CURSOR DEL idx id`.
- **Errors:** `Cursor not found, id: N`, and `Cursor does not exist`.
- **Limit:** at most 128 cursors open on an index.
- **Not a snapshot.** A cursor walks document ids on the live index, so:
  - a document written while it is open can appear;
  - a document rewritten can come again, which is why redisvl removes
    repeats;
  - a deleted document is skipped.
- **A pipeline with `SORTBY` or `GROUPBY`** reads every row at its first
  page.

**Errors** are in RediSearch's words with its `SEARCH_` prefixes, for
example:
- ``Property not loaded nor in pipeline: `x` ``;
- `No such reducer: X`;
- `Syntax error at offset N near 'tok'`.

## Options

### A. Cursor paging only

Serve `FT.AGGREGATE <query> [LOAD ...] [LIMIT ...] [WITHCURSOR ...]` and
`FT.CURSOR READ` and `DEL`. Refuse `GROUPBY`, `APPLY`, `FILTER` and
`SORTBY` by name.
- Fixes the 7 bulk-operation tests and the migrator.
- The router and the hybrid and multi-vector queries stay refused.
- Small: no expression language and no reducers.

### B. RediSearch's aggregation pipeline, as one shard answers it, with named exclusions (recommended)

Everything under "What RediSearch does", answered as it answers. The
exclusions are sub-decision 1, `FORMAT EXPAND`, and the clustered
coordinator's behaviour.
- **Exact** for every shape redisvl sends, and for aggregations a user
  writes by hand.
- **The cost** is about ADR-0063's: an expression language, 10 reducers,
  about 30 functions, cursors, and two reply shapes.

### C. B, together with the multi-vector indexes redisvl's hybrid tests build

B, plus two of ADR-0060's limits lifted: several VECTOR fields in one
index, and `FLOAT64` vectors.
- Gets `test_aggregation.py`'s 16 tests past their fixture.
- Changes how an index keeps its vector set, which is a question about rows
  and layout of its own. It is better decided apart, as an amendment to
  ADR-0060.

## Recommendation

**B,** and then C's vector change, proposed on its own as an ADR-0060
amendment.

### How it is served

- **Values from the index.** The query matches as `FT.SEARCH`'s does:
  ADR-0059's filter, ADR-0061's ranges, ADR-0063's text and scorers, and
  KNN. The pipeline then runs on what the index holds for each match:
  - NUMERIC values, as numbers;
  - TEXT and TAG values as amendment 1 keeps them: as written, or folded
    for a `SORTABLE` field without `UNF`;
  - the key, the score and the distance.
- **Reads only where the index cannot answer.** Some values the index does
  not hold in the form `LOAD` returns:
  - a field outside the schema;
  - a GEO or VECTOR field;
  - a JSON document's own values, and `LOAD *`.

  Those are read back from the pairs over the channel, as `FT.SEARCH`
  reads its results. A `LOAD` that no later step reads runs after the
  steps that shrink the rows, so only the rows in the reply are read.
- **Cursors** keep their query and the place they stopped, not a copy of
  their matches. Each read walks the index on from there in write order, as
  RediSearch's cursor walks document ids. A cursor whose pipeline has a
  `SORTBY` or a `GROUPBY` keeps the rows its first page computed, charged to
  the D4 meter.
  - At most 128 cursors are open on an index.
  - A cursor expires after 300 s idle.
  - It is lost when its co-processor stops, and the next read then answers
    `Cursor not found, id: N`.
- **Bounds.** A reply holds at most 10,000 rows, as RediSearch answers when
  configured with `MAXAGGREGATERESULTS 10000`: `LIMIT exceeds maximum of
  10000`. That is the same ceiling as an `FT.SEARCH` page, and a larger
  result is paged by cursor. A `GROUPBY` holds at most 1,000,000 groups, as
  RediSearch's default does.
- **The proxy** already routes both commands to the `FT.` family. It sizes
  their channel budget to the `COUNT`, or to the `LIMIT` or `MAX`, as it
  sizes `FT.SEARCH`'s to its `LIMIT`. It builds the RESP3 map, as it does
  for `FT.SEARCH`.
- **Releases.** Nothing is durable, since cursors live in memory, and there
  is no new flag: `coproc-ft on` covers it. One release.

### Sub-decisions for Jeff

1. **`QUANTILE` and `COUNT_DISTINCTISH`,** RediSearch's two approximate
   reducers:
   - **(i) Refuse both by name (recommended).** None of the three clients
     sends them, and an approximation answers as RediSearch's does only if
     its sketch is ported exactly.
   - **(ii) Compute them exactly.** That differs from RediSearch on large
     inputs, a documented difference.
   - **(iii) Port the CKMS sketch and RediSearch's HyperLogLog** from
     their rules.
2. **`timefmt` and `parsetime`,** which hand their format to C's
   `strftime` and `strptime`:
   - **(i) Serve the C locale's conversion specifiers, and refuse any other
     specifier by name (recommended).**
   - **(ii) Refuse both functions by name.**

   The other date functions are calendar arithmetic, and are in B either
   way.

### What will differ from RediSearch, on purpose

- **The total without `SORTBY`, `GROUPBY` or `WITHCOUNT`.** RediSearch's
  RESP2 reports how many rows its pipeline had read when it wrote the
  number, and that depends on how its loader buffers. Flint reports the
  matches left after `FILTER`, as `WITHCOUNT` does. None of the three
  clients reads it: redisvl's paging and its migrator skip it.
- **A reply of more than 10,000 rows** is refused as over the maximum
  (Bounds, above).
- **`TIMEOUT`** is accepted, and the channel's own deadline applies.
- **A clustered RediSearch** aggregates on its coordinator. Flint answers as
  one shard does, as for ADR-0063's scores.

## Verification

- **Units:**
  - each reducer and function, against the numbers in RediSearch's
    `test_aggregate*.py`: `547758.3 % 5.1` is `3.00000000008`, the default
    aliases, the null group, a missing value, and a `SORTBY` with no `MAX`
    keeping 10 rows;
  - the expression grammar: precedence, `@a-1` a syntax error, `@a - 1`
    not;
  - the number format: `%.12g` checked against C's own `snprintf` over a
    table of values.
- **Cursors:**
  - a document written between reads appears;
  - a document rewritten comes again, and a deleted one is skipped;
  - idle expiry, and the limit of 128.
- **`coproc_ft_drill`:** an `agg` section through the proxy, RESP2 and
  RESP3, with:
  - the router's, hybrid's and paging shapes;
  - a cursor read after a co-processor restart, which answers `Cursor not
    found`.
- **The router with a stand-in vectorizer** of fixed vectors, run against a
  local fleet. Its matches must follow `MIN`, `SUM` and `AVG` as RediSearch
  computes them.
- **The suites:** the 8 catalogued tests pass, and nothing that passed now
  fails.
- **The meter:** a test holds what cursors and groups keep to what they are
  charged.

## What this does not decide

- **Several VECTOR fields in one index, and `FLOAT64` vectors.** These are
  `test_aggregation.py`'s fixture, 16 tests: an ADR-0060 amendment, to be
  proposed next.
- **`FT.HYBRID`**, which redisvl uses against Redis 8.4 or later. Flint
  answers as RediSearch 2.6, so redisvl does not send it.
- **`FT.PROFILE` and `FT.EXPLAIN`.**
- **redisvl's `SQLQuery`.** It needs `sql-redis`, which is not installed
  here, so its translations are measured when it is.
