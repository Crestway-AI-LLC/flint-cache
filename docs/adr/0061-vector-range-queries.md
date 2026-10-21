# ADR-0061: Vector range queries in `FT.SEARCH`, for the frameworks' semantic caches

Status: **ACCEPTED 2026-10-20, option C** (Jeff: "Accept option C");
**built 2026-10-21** (As built, below), stacked on ADR-0060's release N.
**Proposed 2026-10-20:** Jeff chose this as the next `FT.*` gap to design
("Vector range queries", 2026-10-20). It builds on ADR-0060's `FT.*`
surface, release N and amendment 3, which are built on the branch
`adr0060-ft` and ride after v0.1.0-rc.85.

## Context

Flint refuses a vector range query by name:

    the query filters on the VECTOR field embedding: vector range queries
    (VECTOR_RANGE) are not supported (ADR-0060)

**What the clients send.** redisvl's `VectorRangeQuery` builds:

    (<filter> @<field>:[VECTOR_RANGE $distance_threshold $vector]=>{$YIELD_DISTANCE_AS: vector_distance})

- `<filter>` is ADR-0059's filter, or nothing.
- `$EPSILON: <e>` may follow the alias, for an HNSW index.
- `SORTBY vector_distance ASC` is added unless the caller sorts otherwise,
  with `LIMIT 0 <num_results>` and `RETURN` of the fields and the distance.

**Who sends it.**
- **redisvl's `SemanticCache`,** the LLM response cache. It looks a prompt
  up as a range query of radius `distance_threshold` (0.1 by default), with
  `num_results` usually 1. langchain-redis's `RedisSemanticCache` is built
  on it. An LLM cache is the most common reason a framework user puts
  vectors in Redis at all.
- **langchain-redis's vector store:** `similarity_search_with_score` and the
  `distance_threshold` searches. These are 8 of its integration tests,
  which fail on Flint today (ADR-0060, verification).
- **redisvl's `HybridQuery`,** inside `FT.AGGREGATE`. That is not served, and
  stays out of scope here.

**What RediSearch answers.**
- Every document of the index within the radius of the vector, the radius
  measured as its KNN measures: squared L2, `1 - dot`, or `1 - cos`.
- ANDed with a filter, only the matches within it.
- On `FLAT`, exactly those. On `HNSW`, a graph walk that keeps candidates
  within `radius x (1 + EPSILON)`, `EPSILON` 0.01 by default, so it may miss
  some, as its KNN may.
- The total is the number within the radius. `SORTBY` and `LIMIT` page them.

## Options

### A. Score every match

Score every vector, or every match of the filter, against the query vector,
and keep those within the radius.

- Exact on both kinds of index.
- Costs a full scan per query. That is what a `FLAT` KNN costs already, but
  on an `HNSW` set it is the cost the graph exists to avoid: a semantic cache
  of 1,000,000 prompts would scan all of them for every prompt.

### B. A range walk in the HNSW graph, as VecSim does

Descend the graph to the query's neighbourhood, then expand every candidate
within `radius x (1 + EPSILON)`.

- As good as RediSearch's own, by the same rule.
- New graph code in `flint-vec`'s HNSW, beside the KNN walk and ADR-0059's
  filtered walk, with its own recall to measure.

### C. A range as a KNN cut at the radius (recommended)

- **The page:** a KNN of `k` = the page's end (`offset + num`), with
  ADR-0059's filter planned as for any filtered KNN (a scan when few match, a
  walk that admits only matches when many). Of its results, those within the
  radius are the range's nearest, in order.
- **The total:**
  - If the KNN's `k`-th result is past the radius, the results within it are
    all of them, and the total is their count.
  - If not, more may lie within it. The KNN is asked again with `k` doubled,
    until its farthest result is past the radius, or every vector is in, or
    `k` reaches 10,000, the page ceiling of ADR-0060 amendment 3 part 1.
- **Exactness:**
  - Exact on `FLAT`.
  - On `HNSW`, as good as a KNN of that `k`, whose `EF_RUNTIME` is raised to
    `k` as every KNN's is. `$EPSILON` is accepted, since redisvl may send
    it, and does not change the answer.
- **Cost:** a semantic cache's lookup, a radius of 0.1 with `num_results`
  1, is one KNN of `k` = 1, as it costs now. A range that holds many
  documents costs one KNN per doubling, up to the ceiling.
- **The one difference from RediSearch:** a range of more than 10,000
  documents reports a total of 10,000. The page itself is exact up to the
  ceiling. The frameworks read the page, not the total, and redisvl's
  paginator stops at an empty page.

## Recommendation

**C.** It answers the frameworks' queries with the machinery that already
answers their KNNs, filter included, so it is exact where the KNN is exact
and approximate where the KNN is approximate, with no new graph code. If a
tenant's ranges come to hold thousands of documents, B is the follow-up,
under the same syntax.

### Its scope

- **The query:** one `@<vector field>:[VECTOR_RANGE <radius> $<param>]`
  term, alone or ANDed with ADR-0059's filter, in either order.
  - The radius is a number or a parameter, as RediSearch takes either.
  - A range under `|` or `-`, two of them, or one with a `=>[KNN ...]` is
    refused by name, as now.
- **Its attributes:** `=>{$YIELD_DISTANCE_AS: <alias>}`, which `RETURN` and
  `SORTBY` read as a KNN's `AS` alias, and `$EPSILON: <e>`. Any other
  attribute, such as SVS-VAMANA's `$SEARCH_WINDOW_SIZE`, is refused by name.
- **The reply:** as a KNN's, in RESP2 and RESP3. With no `SORTBY`,
  nearest first, a tie in write order (amendment 3, part 6).
- **One release, no flag.** A range query writes nothing and keeps no
  state, so a rollback to a release before it loses only the query, which
  that release refuses by name as today.
- **Not `VEC.SEARCH`.** A radius for `VEC.*` can follow under the same rule
  if a tenant asks.

## As built (2026-10-21, stacked on ADR-0060's release N, after v0.1.0-rc.85)

- **The query.** `take_range` (`flint-vec/src/ft.rs`) takes the range term
  out of the query before the rest is read as ADR-0059's filter.
  - The term must be one conjunct of the whole query, so that taking it out
    leaves exactly the filter it is ANDed with. redisvl's own shape, the term
    and its filter in one group, comes off whole.
  - Refused by name:
    - a `|` beside it outside every group, tag set, numeric range and quote;
    - a group around it that is not the whole query;
    - `-` or `~` before it, or a KNN beside it;
    - two range terms;
    - an attribute other than `$YIELD_DISTANCE_AS` and `$EPSILON`.
  - A `|` inside a tag set (`@user:{a|b}`) or inside a group of the filter
    is the filter's own.
  - A negative radius is refused, as RediSearch refuses it.
- **The answer.** The term becomes a KNN with a radius (`Knn.range`).
  `FtIndex::search` asks for `k` = the page's end, at least 1. It keeps
  the results within the radius, and doubles `k` while every result is
  within it, until fewer come back than were asked for or `k` reaches
  10,000 (`RANGE_CAP`). The sort, `RETURN`, page and reply are a KNN's.
- **Held by:**
  - unit tests:
    - `a_vector_range_term_parses_as_a_knn_cut_at_its_radius`;
    - `a_vector_range_on_flat_is_every_document_within_its_radius`;
    - `a_vector_range_on_hnsw_recalls_as_its_knn_does`;
    - `a_vector_range_counts_to_its_ceiling`;
  - `coproc_ft_drill`'s `range` section, before and after a co-processor
    restart.
- **Measured** (`a_vector_range_on_hnsw_recalls_as_its_knn_does`):
  - 2,000 16-d vectors, 20 queries, ranges of 10, 50 and 200 documents;
  - default `EF_RUNTIME`, RediSearch's default page of 10.

  | metric | range (its total) | its first page | KNN of as many |
  |---|---|---|---|
  | L2 | 1.000 | 1.000 | 0.999 |
  | COSINE | 1.000 | 1.000 | 1.000 |
  | IP | 1.000 | 1.000 | 1.000 |

  At these sizes every range matched its brute-force answer; the KNN of as
  many missed 0.1% on L2. A range is cut from a KNN of at least as many,
  whose `ef` is raised to its `k`. The test holds floors of 0.90 (0.80 on
  IP), and says nothing of sets larger than these.

## Verification

- **Parsing:** redisvl's and langchain-redis's query strings as they send
  them, with and without a filter and `$EPSILON`; each refused shape by name.
- **Exactness on `FLAT`:** against a brute-force answer over 10,000 random
  vectors, per metric, every radius from none in range to all of them,
  total included.
- **`HNSW` against `FLAT`:** the same sets, recall of the range at the
  default `EF_RUNTIME`, reported beside its KNN's at the same `k`.
- **The ceiling:** a range of more than 10,000 documents reports 10,000,
  and its first page is exact.
- **`coproc_ft_drill`:** a range with and without a filter, through the
  proxy, before and after a co-processor restart.
- **The clients' suites:** langchain-redis's 8 range tests, and its and
  redisvl's semantic cache tests where they run without a model download.

## What this does not decide

- `FT.AGGREGATE`, which redisvl's `HybridQuery` needs, and which would
  carry a range through `APPLY` and `GROUPBY`.
- A range walk in the HNSW graph (B), unless a tenant's ranges need it.
- `GEO`, `WITHSCORES` and full text, each its own ADR.
