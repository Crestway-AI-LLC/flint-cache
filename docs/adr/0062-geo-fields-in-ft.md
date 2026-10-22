# ADR-0062: GEO fields in `FT.*`, which redisvl's schemas declare

Status: **ACCEPTED 2026-10-21, option B** (Jeff: "Accept B, build it");
**built 2026-10-21** (As built, below), to land in the same release as
ADR-0060's release N. **Proposed 2026-10-21.**
It builds on ADR-0060's `FT.*` surface and ADR-0059's filter, both on public
main after v0.1.0-rc.85.

## Context

Flint refuses an index with a GEO field by name:

    GEO fields are not supported (ADR-0060)

**What it costs.** The three clients' suites were run against a local fleet
on 2026-10-21 (public 3f7d3581), and each failing test was put down to one
cause:

| cause | redisvl | langchain-redis | the same, RESP2 |
|---|---|---|---|
| **a GEO field** | **48** | **3** | **3** |
| `FT.SEARCH ... WITHSCORES` | 11 | 1 | 6 |
| full-text terms | 2 | 4 | 4 |
| `FT.AGGREGATE` | 8 | 0 | 0 |
| `INDEXEMPTY` / `INDEXMISSING` | 6 | 0 | 0 |

GEO is first by far. redisvl's shared test schema has a `location` GEO
field, so `FT.CREATE` fails in the fixture, and 48 tests error before they
start. Most of them never query the field. A few do, with `GeoRadius`.

**What the clients send.**
- In a schema: `FT.CREATE ... SCHEMA ... location GEO`.
- As a value: `"lon,lat"` in a HASH field, or a JSON string of the same
  form.
- In a query, from redisvl's `Geo("location") == GeoRadius(lon, lat, r,
  unit)`: `@location:[<lon> <lat> <radius> <unit>]`. `!=` writes the same
  term under `-`. The unit is `m`, `km`, `mi` or `ft`.

**What RediSearch answers.** From its source:
- **A value** is `lon` and `lat`, separated by a comma or a space, each
  trimmed and finite. A document whose value does not parse fails to index
  (settled when built, below).
- **It is stored as a 52-bit geohash**, 26 steps for each coordinate. The
  latitude is limited to ±85.05112878 and the longitude to ±180, so a point
  nearer a pole is never matched.
- **A radius query** reads up to nine geohash ranges: the cell at a step
  sized to the radius, and its eight neighbours. It keeps a document whose
  cell centre is within the radius by the haversine distance.
- **Units in metres:** `m` 1, `km` 1000, `ft` 0.3048, `mi` 1609.34.
- **Refusals:** a latitude outside ±90 or a longitude outside ±180 is
  `Invalid GeoFilter lat/lon`. A radius that is not above 0 is `Invalid
  GeoFilter radius`. An unknown unit is `Invalid GeoFilter unit`.

## Options

### A. Accept GEO fields, and refuse a geo query by name

`FT.CREATE` takes a GEO field and keeps its values; a document's value is
returned as it is stored. A `@f:[lon lat r unit]` term is refused by name.

- Smallest. It unblocks the tests that only declare the field.
- The tests that query it still fail, and a framework user who filters by
  place still cannot.

### B. GEO as ADR-0059 indexes numbers, by geohash (recommended)

A GEO field is one more kind in ADR-0059's field index:
- Each document's value is parsed and encoded as RediSearch encodes it.
- It goes into an ordered set of `(geohash, slot)` like a NUMERIC field's.
- A geo term's candidates are RediSearch's nine geohash ranges of that set,
  each kept or not by the haversine distance from its cell's centre.

The planner then treats a geo term as it treats a range. It counts the
candidates to choose between scoring every match and a walk that admits
only matches. ANDed with a KNN or a range query it narrows them, as a tag
does.

- Answers as RediSearch does, edges of the radius included, because the
  stored point and the test are its own.
- About as much code as NUMERIC ranges were, with a field kind, a parser
  and a term. No new graph code, and no change to the KNN or the range query.

### C. Score every document's distance

Keep each value as two floats and test every match.

- Simple, and exact against the floats.
- Slower than B on a large index, since the planner cannot count a geo term
  without scanning. Its edges also differ from RediSearch's, since it does
  not test the geohash cell centre.

## Recommendation

**B.** It answers redisvl's schemas and `GeoRadius` filters as RediSearch
does, through the filter and planner that already answer their tags and
numbers.

### Its scope

- **`FT.CREATE`:** a `GEO` field on `ON HASH` and `ON JSON`, with `AS`
  and `NOINDEX` as other fields take them. `GEOSHAPE` stays refused by name.
- **Values:**
  - `"lon,lat"` or `"lon lat"`;
  - for JSON, a string of that form, or an array of them, any of which may
    match (settled when built, below);
  - a value that does not parse, or a point past the latitude limit, leaves
    the document out of the index and is counted, as RediSearch's indexer
    does (settled when built, below).
- **`FT.SEARCH`:**
  - `@f:[lon lat radius unit]`, alone, negated, or with ADR-0059's other
    terms and a KNN or range query;
  - the four units, in any case;
  - RediSearch's three refusals, in its words.
  - The legacy `GEOFILTER` option is refused by name; none of the three
    clients sends it.
- **`SORTBY` on a GEO field** stays refused by name. RediSearch can sort
  by one, but none of the three clients asks to. `FT.INFO` reports the field
  as `GEO`.
- **`VEC.*` is not changed.** `VEC.CREATE ... FIELD f GEO` can follow under
  the same rule if a tenant asks.
- **Releases:** it should land before the next cut, in the same release as
  ADR-0060's release N.
  - That release's notes already say to drop every FT index before rolling
    back below it, since an older co-processor leaves out an index whose
    definition row it cannot read. So a GEO field adds no new rollback rule.
  - If it misses that cut, a GEO field would need the two-release rule
    instead: refused by an older binary, and allowed only once a release
    knows it.

## As built (2026-10-21, for the release after v0.1.0-rc.85)

- **Settled from RediSearch's source when built:**
  - A document whose GEO value does not parse, or whose point a geohash
    cannot hold (a latitude past ±85.05112878, a longitude past ±180),
    fails to index: "Invalid geo coordinates". RediSearch counts it in
    `hash_indexing_failures`. Here it is left out and counted, as a bad
    vector is.
  - A JSON array of points is taken, and any of them may match.
  - A value is split at its first comma or space before each part is
    trimmed. So `"10.0, 20.0"` is a point, and `" 10.0,20.0"` and
    `"10.0 , 20.0"` are not.
  - A point is within a radius when its distance is at most the radius.
- **The code:**
  - `crates/flint-vec/src/geo.rs`, written from RediSearch's rules:
    - parsing a value;
    - the 52-bit hash, and a cell's centre;
    - haversine on Redis's Earth radius, 6,372,797.560856 m;
    - a radius query's step estimate and its nine cells.
  - In `filter.rs`, three additions:
    - `FieldKind::Geo`;
    - `Val::Geo`, a document's hashes;
    - `Expr::Geo`, whose cells are computed once when the query is parsed.
  - A document scan and the field index both ask the same two things: does
    a cell hold the point's hash, and is its centre within the radius. Both
    therefore answer as RediSearch does, including the cells' own misses
    near the poles with a large radius, which RediSearch's source records.
  - The field index keeps a GEO field's points in the ordered set a NUMERIC
    field uses, keyed by hash. It reads a geo term from its cells, and
    removes a document that appears twice because it has several points.
- **Not shared, a follow-up:** flint-server's GEO commands keep their own
  copy of the same geohash rules (`crates/flint-server/src/commands/geo.rs`).
  Both are held to Redis's own Sicily example. One shared crate would keep
  the two from drifting.
- **Held by:**
  - unit tests:
    - `hashes_centres_and_distances_agree_with_redis_s_own_example`;
    - `a_value_is_lon_and_lat_by_a_comma_or_a_space`;
    - `the_ranges_hold_every_point_within_the_radius`;
    - `a_centre_past_the_square_reads_nothing`;
    - `a_geo_filter_parses_as_redisvl_renders_it_and_refuses_as_redisearch_does`;
    - `a_geo_field_answers_a_radius_as_redisearch_measures_it`, which
      checks 10,000 points at each unit, alone, with a tag and under a KNN;
  - `coproc_ft_drill`'s `geo` and `geosame` sections, before and after a
    co-processor restart.
- **Found while building it: BUG-0271.** `coproc_ft_drill`'s GEO section
  writes documents straight after `FT.CREATE`. That showed an index's
  backfill moving documents written during it out of write order, in its
  own index and in every other index on the prefix. Fixed in the same
  stack, before any release served `FT.*`.

### The clients' suites, as built (2026-10-21)

Against a local fleet, beside the run after ADR-0061 (its *As built*):

| suite | passed before | passed now | failed now | errors now |
|---|---|---|---|---|
| redisvl | 207 | 238 | 54 | 17 |
| langchain-redis, RESP3 | 149 | 150 | 31 | 23 |
| langchain-redis, RESP2 | 187 | 188 | 15 | 1 |
| LlamaIndex | 19 | 19 | 0 | 0 |

- **redisvl:** the 48 tests that errored in the fixture now run, and 31 of
  them pass. No failure left names GEO. Those that still fail name another
  cause from the table in Context: `WITHSCORES` in 22 tests now,
  full-text terms, `FT.AGGREGATE` and `INDEXEMPTY`.
- **langchain-redis:** in each protocol, its three tests that GEO stopped
  now run. `test_similarity_search_with_sort_by` passes.
  `test_similarity_search_with_metadata_filtering`, over HASH and over
  JSON, then fails on a full-text term.

## Verification

- **Parsing:** each value form RediSearch takes and each it does not; each
  geo term redisvl renders, and RediSearch's three refusals.
- **Geohash:** encode and centre-decode against points RediSearch's own
  tests fix, the latitude limit included.
- **Exactness:** over 10,000 random points and many radii, at each unit,
  near the poles and across the antimeridian. The answer, total included,
  must equal a brute force of RediSearch's rule: haversine from the cell
  centre, within its nine ranges.
- **The planner:** a geo term ANDed with a KNN, on FLAT and on HNSW, counts
  and plans as a range does.
- **`coproc_ft_drill`:** an index with a GEO field over HASH and JSON, a
  `GeoRadius` filter through the proxy, before and after a co-processor
  restart.
- **The clients' suites:** the 48 redisvl tests that error today, and
  langchain-redis's 3 in each protocol, run. Each that still fails names
  another cause from the table.

## What this does not decide

- `WITHSCORES`, full-text terms, `FT.AGGREGATE`, `INDEXEMPTY` and
  `INDEXMISSING`. Each is its own question, next by the table above.
- `GEOSHAPE`.
- GEO on `VEC.*`.
