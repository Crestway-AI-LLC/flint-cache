# BUG-0273: a TAG or `*` search over an index with TEXT fields came in write order, where RediSearch orders it by score (FIXED 2026-10-22)

**Status:** **FIXED 2026-10-22**, by ADR-0063's build, before any release
served `FT.*`. ADR-0060's release N, on public main after v0.1.0-rc.85, had
it. **Found by reading RediSearch's source** for ADR-0063, not seen in a
run: its tag iterator (`rqe_iterators/src/inverted_index/tag.rs`), its
wildcard iterator, `default.c`'s scorers, and `pipeline_construction.c`,
which scores every `DIALECT 2` search that has no `SORTBY`.
Held by `a_search_with_no_text_word_is_ordered_by_score` in
`crates/flint-vec/src/ftindex.rs`, which fails with ADR-0060's write order.
**Severity:** low. Which documents a search answered was right; their order
was not, and so which ones a `LIMIT` page held.
**Area:** `crates/flint-vec/src/ftindex.rs`, `FtIndex::search`.

## What happened

ADR-0060 amendment 3, part 6, answered a search with no order asked for in
the order its documents were last written, as RediSearch's document ids
order them. That holds for ties. But RediSearch scores every search that
has no `SORTBY`, with `BM25STD` unless it names another scorer, and sorts
by that score first:
- **A TAG match is a word's result** to its scorers: frequency 1, and the
  tag value's own IDF. `BM25STD` scores it by the document's length against
  the index's average.
- **A `*` match** is a virtual result of frequency 1, which `BM25STD` also
  scores by length.
- **A NUMERIC or GEO match** scores 0.

So on an index with TEXT fields, RediSearch answers `@tag:{x}` and `*` with
the shortest documents first, and Flint answered them in write order. On an
index with no TEXT field every document's length is 0, every score ties,
and write order was right.

## The fix

ADR-0063 scores every search as RediSearch does and orders it highest
score first, a tie to the earlier write. A TAG term's IDF reads a count of
the documents holding each tag value, kept as documents are written.

## Not changed

- Ties still come in write order, and a KNN's results by distance.
- A search with `SORTBY` is sorted by its field, as before.
