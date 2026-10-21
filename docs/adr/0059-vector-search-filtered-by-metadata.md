# ADR-0059: Vector search filtered by metadata

Status: **ACCEPTED 2026-10-19, option B, in two releases** (Jeff: "go with
your recommendations on all six"). Jeff also approved the framework survey's
downloads, and the 999,000-vector measurement run in principle, to be asked
for again when B is built so that one run measures A, B and the scan
threshold.

**Release R built 2026-10-19** (`crates/flint-vec/src/filter.rs`): every
`VEC.SEARCH` option this binary does not know is refused, and `FIELD` and
`FILTER` are served behind `--vec-filters` (inventory `vec-filters on`). How
it differs from the text below is under *As built*. It ships in v0.1.0-rc.84.

**Release R+1 built 2026-10-19:** `flint-vec` serves filters unless started
with `--no-vec-filters`, and flintctl passes `--vec-filters` unless the
inventory says `vec-filters off`, so a rollback to rc.84's `flint-vec`,
whose own default is off, keeps them served. It ships in v0.1.0-rc.85.

Proposed 2026-10-17.

## Context

`VEC.SEARCH set query k` answers the `k` nearest vectors in a set, from all
of them. An application cannot ask for the nearest documents *in one
folder*, or the nearest products *in stock and under $50*. Today it has two
ways round that, and both are poor:
- **A set per value** (a set per folder) works for one field with few
  values, and not for two fields or a range.
- **Asking for more than `k` and dropping the misses** in the client. The
  next section shows how much that loses.

Jeff listed vector metadata filters on 2026-10-05 as a candidate next
feature, after JSONPath multi-match (ADR-0054, built). Vector search is on by
default from v0.1.0-rc.83 (ops ADR-0050 steps 5 and 6) and waits to be
announced, and a filter is the first thing a tenant building retrieval on it
will look for.

**What there is to build on:**
- **`META`** is opaque bytes. `VEC.SET`'s usage line says `META <json>`,
  but nothing checks that it is JSON. The co-processor keeps it in RAM with
  the vector, where the D4 meter charges it, and in the vector's durable
  row.
- **Expired vectors are already filtered after the fact.** A search fetches
  `k` plus the number of expired-but-unswept vectors and drops them, which is
  exact only because that number is known.
- **A set's settings** are one durable config row, `dim|metric|index[|quant]`.
  Every binary reads the first four fields and ignores any after them.
- **`VEC.SEARCH` passes over options it does not know.** It looks for `EF`
  and `RERANK` among its trailing arguments and skips the rest (v0.1.0-rc.83,
  "Anything else is passed over, as before"). So a release from before
  filters would answer `VEC.SEARCH ... FILTER <expr>` as if there were no
  filter.

### What filtering after the fact loses

This is arithmetic, not a measurement. Take a filter unrelated to where the
vectors lie, which matches a fraction `s` of the set. The true answer, the
`k` nearest matching vectors, is the first `k` matches in the unfiltered
ranking. A search that fetches `f × k` candidates and drops the misses finds
about `f × k × s` matches. Its recall is therefore about `min(1, f × s)`,
even with an exact index:

| matching fraction `s` | recall, fetching 10 × k | fetching 100 × k |
|---:|---:|---:|
| 50% | 1.0 | 1.0 |
| 10% | 1.0 | 1.0 |
| 1% | about 0.1 | 1.0 |
| 0.1% | about 0.01 | about 0.1 |

Fetching more costs a deeper search and, for `QUANT bin` or `sq8`, a
re-rank of every fetched candidate from the vector file on NVMe. Scoping
to one user's or one folder's vectors, the case in the first section, can
match well under 1% of a set.

## Options

### A. Filter after the fact, in the co-processor

`VEC.SEARCH ... FILTER <expr>` fetches `f × k`, evaluates the filter on each
candidate's `META` and keeps the first `k` that match. It needs no new
memory and no schema, and it is the smallest change. It inherits the table
above. Its only gain over the client doing it is fewer bytes on the wire.

### B. Declared fields, planned by match count (recommended)

- **Fields are declared when the set is made:**
  `VEC.CREATE docs DIM 1536 METRIC cosine INDEX hnsw QUANT bin FIELD folder TAG FIELD price NUMERIC`.
  - A `TAG` field holds a string, or an array of strings. A `NUMERIC` field
    holds a number.
  - Values are read from `META` when it is a JSON object (`{"folder":
    "contracts", "price": 42}`). A vector whose `META` is not JSON, or lacks
    the field, does not match a filter on it. META stays as opaque as it is
    today for everything else.
- **The co-processor keeps a field index per set in RAM:**
  - a posting list of the vectors' slots for each tag value;
  - the vectors' slots sorted by value for each numeric field.

  It is rebuilt from the durable rows on load, as the vector index is, and
  the D4 meter and the overall cap charge it.
- **Each search is planned by how many vectors match.** The field index
  gives an exact count of matches before the search starts.
  - **Few matches** (below a threshold that verification sets): score
    exactly those vectors. For a flat set that is exact. For `QUANT bin` or
    `sq8` it scores the in-RAM codes and re-ranks the best `RERANK` from the
    vector file, as an unfiltered search does.
  - **Many matches:** an HNSW search that walks every node, matching or
    not, so the graph stays connected, but admits only matching nodes to the
    result. Its `EF` grows with how few match, up to a cap.
- **The filter is a subset of the query syntax of RediSearch and Valkey
  Search:** `@folder:{contracts | invoices}`, `@price:[10 50]` with `(` for
  an exclusive bound and `-inf` or `+inf` for an open one, `-` to negate,
  terms side by side for AND, `|` between terms for OR, parentheses, and a
  backslash before a punctuation character in a tag value. That is the
  subset `redisvl` writes for tags and numbers (the survey below), so a
  later `FT.*` surface (option C) can share the parser.
- **`VEC.INFO`** lists the fields and the field index's bytes.

### C. Valkey Search's `FT.*` surface

Valkey Search answers `FT.CREATE idx ON HASH ... SCHEMA vec VECTOR HNSW ...
folder TAG price NUMERIC` and hybrid queries, `FT.SEARCH idx "@folder:{x}
=>[KNN 10 @vec $q]"`. This is what LangChain's and LlamaIndex's Redis vector
stores and `redisvl` send, through RediSearch's API. None of them can reach
`VEC.*` today, filter or not. Not measured here: the survey under
*Verification* would measure it.

It is a different data model:
- an index over `HASH` or JSON keys in the data nodes, not over a
  co-processor set;
- kept current from every write to those keys, which Flint has no feed for
  (no keyspace notifications, ADR-0052);
- a query language much wider than filters.

It is several times B, and it deserves its own ADR, after B and the survey.

## Recommendation

**B.** Of the options that fit the co-processor that already holds the
vectors, it is the only one whose recall does not fall with selectivity. C
would also filter before ranking, but it is a different product surface.

**Two releases, because of what an older release does with `FILTER`.** A
release from before this one skips options it does not know, so after a
rollback a filtered search would silently answer unfiltered. A tenant that
uses a filter to scope results, such as one folder per user inside its
namespace, would then read the other folders' vectors. So:
1. **Release R** refuses any `VEC.SEARCH` option it does not know. It
   serves `FIELD` and `FILTER` only when the co-processor runs with
   `--vec-filters` (inventory `vec-filters on`), which is off by default.
2. **Release R+1** turns it on in `flint-vec`'s own default, with an
   opt-out flag, as ADR-0050 step 6 did for packed rows. `flint-vec`
   ignores flags it does not know, so the opt-out cannot stop an older
   release's co-processor. A rollback from R+1 lands on R, which knows
   filters.

The field declarations go in the set's config row as a fifth field,
`dim|metric|index|quant|fields`, with `quant` written as `none` for an
unquantized set. A release before R reads the first four fields and ignores
the fifth (`decode_config`), so a set made with fields still loads there.
Nothing new is written to the durable vector rows, since `META` is already
there.

## Verification

- **Recall and latency against selectivity.** Recall@10 against an exact
  search over the matching vectors, for A and B, at 50%, 10%, 1% and 0.1%,
  with tags drawn independently of the vectors and with tags that follow
  them. p50 and p99 latency, through the proxy. On the 999,000
  `text-embedding-3-large` vectors that vectors.md's table was measured on,
  with Jeff's approval for the run.
- **The scan threshold.** Where scoring every match stops beating a
  filtered traversal, per quantization, on the same data.
- **RAM.** Field-index bytes per vector per field, charged by the meter.
- **Correctness:**
  - every operator and its edge cases (empty tag, exclusive bounds,
    negation of a missing field, NaN refused);
  - META that is not JSON, and fields that are missing;
  - expired vectors excluded;
  - a rebuild from durable rows answering the same;
  - the differences from RediSearch's syntax listed in command-support.md.
- **Rollback:**
  - R's `flint-vec` refuses `VEC.SEARCH` with an option it does not know,
    and refuses `FIELD` and `FILTER` without `--vec-filters`.
  - A set made with fields on R+1, then R's `flint-vec` started on the same
    rows, answers the same filtered searches.
  - The same set under rc.83's `flint-vec` loads and answers unfiltered
    searches. That is why the flag is needed, and the drill shows it.
- **The framework survey.** Done 2026-10-19, in the next section.

## The framework survey, 2026-10-19

Read, not run: `redisvl` 0.28.0, `langchain-redis` 0.2.6 and
`llama-index-vector-stores-redis` 0.9.0, the newest wheels on PyPI that day.
Both stores build their indexes and queries with `redisvl`
(`langchain-redis` needs `redisvl` 0.12 to below 1.0, the LlamaIndex store
0.4.1 to below 0.5), and read and delete documents through its client with
plain `HASH` and JSON commands in pipelines.

- **They send only RediSearch's API:** `FT.CREATE`, `FT.INFO`, `FT._LIST`,
  `FT.SEARCH`, `FT.AGGREGATE`, `FT.CURSOR` and `FT.DROPINDEX`, over
  documents written as `HASH` (both stores' default) or JSON. None sends
  `VEC.*`, so none can use Flint's vector search today, filtered or not. B
  does not change that, and only C would.
- **The filter dialect is B's subset, and more.** A filtered search is
  `(<filter>)=>[KNN k @vec $vector AS vector_distance]` at `DIALECT 2`, with
  `HYBRID_POLICY` and `EF_RUNTIME` when asked. `redisvl`'s tag and number
  filters write exactly the forms listed under B. It also writes full-text
  (`@f:("phrase")`), geo, and tag wildcards, which B leaves out.
- **LangChain's default needs full text even with no filter.** Its default
  schema declares `_index_name` as a `TEXT` field and adds
  `@_index_name:("<index>")` to every vector query. So C needs at least an
  exact-phrase match on a `TEXT` field to serve LangChain's default.
- **Both stores also search without a vector**, for deletes by document id
  and LangChain's chat history (`FilterQuery`, `CountQuery`).
- **`redisvl` no longer refuses a server without the search module** at
  connect ("Module validation was removed"). It reads `MODULE LIST` only to
  test for SVS-VAMANA. A missing `FT.*` shows up at the first index call.

**What this decides:** C follows B, in its own ADR. B's parser and field
index are what C's filter needs. C's own costs are the `HASH` and JSON data
model and the feed from every write to those keys, which the Options section
lists. A tenant on LangChain or LlamaIndex needs C, and one on the raw
client needs only B.

## As built, release R (2026-10-19)

- **The many-matches walk does not raise `EF`.** It admits only matches to
  the `EF` it keeps and goes on until it holds `EF` of them, so it reaches
  further the fewer match without a separate rule. It also starts from up to
  `EF` matches the count already found, as well as from where the descent
  lands: with the matches lying together, layer 0 did not always link the
  two, and on two clusters of 1,500 vectors, one of them matching, a walk
  from the descent alone returned fewer than `k`.
- **The threshold** between scoring every match and walking is
  `sqrt(32 x EF x n)` matches, about 45,000 of 1,000,000 at the default `EF`:
  where the two cost the same number of distances. That was arithmetic; the
  999,000-vector run (below) kept it.
- **Counting stops at the threshold.** The count reads the smallest TAG
  term's set of slots (or a range's) and stops one past the threshold. It
  checks a slot against the whole filter only when those entries are not
  exactly its matches, which is when an AND has other terms (BUG-0267: it
  had checked every slot, at a cache miss each). A filter with no such term
  (`*`, a negation) is counted by a scan of every slot, which stops there
  too.
- **The field index is HNSW's only.** A flat set scores every vector anyway,
  so it keeps each vector's values and checks them as it scans.
- **The syntax:** blanks at a tag's ends are dropped, and a backslash keeps
  one. `$parameter`, tag wildcards, full-text and geo terms are refused by
  name. AND and OR do not mix without parentheses.
- **Measured in the unit tests, not on real embeddings:** on 3,000 16-d
  vectors, scoring every match recalled 1.0 on full vectors and at least
  0.994 with `sq8` or `bin`. A walk recalled 1.0 on full vectors and `sq8`
  at 1 in 2, 1 in 10 and 1 in 100 matching, with tags drawn apart from the
  vectors or following them. With `bin` it recalled at least as much as an
  unfiltered search of the same set, which a 16-bit code holds to between
  0.30 and 0.82.
- **The meter** charges the field index from counts it keeps: each tag
  value, each slot in a value's set, each numeric slot and each vector's
  values. `tests/meter.rs` holds it above what a set with fields holds, for
  TAG values each vector's own and values all of them share.

## Measured on 999,000 vectors, 2026-10-19

The `text-embedding-3-large` set that vectors.md's table was measured on:
1,536 dimensions, cosine, HNSW at the default `EF` of 64, on an i4i.2xlarge.
The filters (`bench --filters`):
- `rN` is a NUMERIC range that N% of the vectors fall in, with the number
  drawn apart from the vectors;
- `c10` and `c100` are a TAG naming the nearest of 10, or of 100, centroid
  rows, so the matches lie together.

200 queries each, recall@10 against an exact search over the matches. p50
in milliseconds, for the set's own choice (rule), scoring every match (scan)
and walking (walk):

| arm | filter | matches | rule recall | rule | scan | walk |
|---|---|---|---|---|---|---|
| plain | `r50` | 500,005 | 0.975 | 6.1 | 586 | 3.1 |
| plain | `r10` | 100,046 | 0.991 | 15.8 | 116 | 11.1 |
| plain | `c10` | 121,277 | 0.985 | 22.7 | 148 | 14.5 |
| plain | `r1` | 10,249 | 1.000 | 11.5 | 11.6 | 79.2 |
| plain | `c100` | 5,808 | 1.000 | 6.8 | 6.9 | 475 |
| plain | `r0.1` | 1,014 | 1.000 | 0.55 | 0.54 | 534 |
| sq8-disk | `r50` | 500,005 | 0.975 | 5.7 | 642 | 3.3 |
| sq8-disk | `r1` | 10,249 | 1.000 | 11.7 | 11.5 | 79.2 |
| bin-disk | `r50` | 500,005 | 0.983 | 6.9 | 259 | 2.2 |
| bin-disk | `r1` | 10,249 | 1.000 | 4.5 | 4.4 | 48.6 |

- **The rule chose the faster way at every point.** It scanned up to 10,249
  matches, where scanning was 7 to 1,000 times faster than walking. It
  walked from 100,046, where walking was 7 to nearly 200 times faster than
  scanning. The crossover is between the two, and the threshold, about
  45,000, is between them. It stays.
- **Where it walked, it paid for its count on top of the walk:** up to
  twice the walk alone (6.1 ms against 3.1 at `r50`). That is BUG-0267,
  fixed: its count no longer checks entries that are exactly the matches.
- **Recall** was at least 0.975 at every point and every arm, and 1.0
  wherever the rule scanned. A walk with half the vectors matching recalled
  0.975 plain, about what an unfiltered search at the same `EF` recalls on
  this set (0.970, vectors.md).
- **Building** the 999,000 took 97 minutes plain, 211 with `sq8` and its
  full vectors on disk, and 33 with `bin`. The resident memory was 6,821,
  2,223 and 841 bytes a vector; the meter charged 6,705, 2,412 and 806.

## What this does not decide

- `FT.*` or any RediSearch surface (option C).
- Full-text search, geo filters, and sorting results by a field.
- Changing `META`'s type. It stays opaque bytes, and fields are read only
  when it is a JSON object.
