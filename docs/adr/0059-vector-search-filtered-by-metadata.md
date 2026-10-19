# ADR-0059: Vector search filtered by metadata

Status: **ACCEPTED 2026-10-19, option B, in two releases** (Jeff: "go with
your recommendations on all six"). Jeff also approved the framework survey's
downloads, and the 999,000-vector measurement run in principle, to be asked
for again when B is built so that one run measures A, B and the scan
threshold. Nothing is built yet.

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
  an exclusive bound, `-` to negate, terms side by side for AND, and
  parentheses. A later `FT.*` surface (option C) could share the parser.
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
- **The framework survey** (downloads approved 2026-10-19).
  Which clients send filtered vector queries, and in which dialect. That
  means `redisvl`, LangChain's and LlamaIndex's Redis vector stores, and any
  client that has a `VEC.*`-shaped API. This decides whether C follows.

## What this does not decide

- `FT.*` or any RediSearch surface (option C).
- Full-text search, geo filters, and sorting results by a field.
- Changing `META`'s type. It stays opaque bytes, and fields are read only
  when it is a JSON object.
