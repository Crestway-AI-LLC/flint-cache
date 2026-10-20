# BUG-0267: a filtered search with many matches checked each one it counted, which cost as much again as its walk (FIXED 2026-10-19)

**Status:** **FIXED 2026-10-19.** Found by ADR-0059's 999,000-vector run,
the first measurement of its rule on real embeddings. Held by
`filter::tests::exact_entries_are_not_checked_one_by_one` and, on a
300,000-vector synthetic set, by `bench --filters` before and after (below).
**Severity:** low. Answers were right. A filtered `VEC.SEARCH` whose filter
matched more vectors than the threshold took up to twice the time of the walk
it ended in: 6.1 ms instead of 3.1 ms at p50, with half the set matching.
Filters are served by default from the release after rc.84 (ADR-0059 R+1).
**Area:** `crates/flint-vec/src/filter.rs`, `FieldIndex::matching` and
`sources`.

## What happened

An HNSW set with fields chooses, for each filtered search, between scoring
every match and walking the graph (ADR-0059 B). It counts the matches from
its field index up to the threshold, `sqrt(32 x EF x n)`, which is about
45,000 of 999,000 at the default `EF`. Below it, it scores them. At it, it
walks. The 999,000-vector run (`text-embedding-3-large`, 1,536 dimensions,
2026-10-19) measured each search three ways:
- `rule`: what the set chooses;
- `scan`: scoring every match;
- `walk`: walking.

Past the threshold, the rule paid for its count on top of the walk. p50 in
microseconds:

| arm | filter | matches | rule | walk | scan |
|---|---|---|---|---|---|
| plain | `r50` | 500,005 | 6,139 | 3,054 | 585,801 |
| plain | `r10` | 100,046 | 15,776 | 11,108 | 116,018 |
| plain | `c10` | 121,277 | 22,737 | 14,548 | 148,137 |
| sq8-disk | `r50` | 500,005 | 5,666 | 3,317 | 642,093 |
| sq8-disk | `c10` | 121,277 | 22,626 | 15,436 | 163,221 |
| bin-disk | `r50` | 500,005 | 6,878 | 2,204 | 259,231 |
| bin-disk | `c10` | 121,277 | 17,722 | 9,588 | 70,215 |

Below the threshold the rule matched the scan, as it should. Where it
scanned, it was up to 1,000 times faster than the walk would have been:
- `r1`, 10,249 matches: 11.5 ms, against the walk's 79 ms;
- `r0.1`, 1,014 matches: 0.55 ms, against the walk's 534 ms;
- `c100`, 5,808 matches: 6.8 ms, against the walk's 475 ms.

## Why

The count read the index entries of the filter's smallest term: a TAG
value's set of slots, or a range of the numeric B-tree. It then checked each
slot against the whole filter, which reads that slot's values. On a large
set that read is a cache miss, so counting to 45,000 cost 3 to 8 ms, as much
as the walk itself.

For most filters the check proves nothing. A TAG term's entries, or a
range's, are exactly its matches, and so are an OR's of them. Only an AND's
other terms, and a negation, need a slot read. The range was the one partial
exception: its entries were read inclusive of both bounds, and the check is
what left out an exclusive one's (`@r:[0 (0.5]`). The bench's own range
filters are written that way.

## Fix

- **`sources` says whether its entries are exactly the matches:**
  - a TAG term's entries are;
  - a range's are, since it now reads exclusive bounds as exclusive;
  - an OR's are, when all its parts' are;
  - an AND's are only when it has one term.
- **`matching` checks a slot against the filter only when they are not.**
  It still skips deleted slots, and still counts a slot that appears under
  two of an OR's terms once.
- **A range's bounds:**
  - `-0` is read as `0`, as values are stored, so `(-0` leaves out the
    zeros;
  - a range that is empty with an exclusive bound, such as `(5 (5` or
    `(5 5`, is empty before the B-tree is asked, since the B-tree refuses a
    range whose start is past its end.
- **The threshold stays `sqrt(32 x EF x n)`.** The run put the crossover
  between 10,000 matches, where the scan won by seven times, and 100,000,
  where the walk won by ten. The threshold, 45,000, is between the two.

## Evidence

- `exact_entries_are_not_checked_one_by_one`, on 2,000 slots:
  - none is checked for one TAG value, two of them, an OR of a TAG and a
    range, or a range, either under the cap or past it;
  - an AND of a TAG and a range checks the TAG's 500 entries, and finds
    their 25 matches;
  - exclusive and inclusive bounds, `(5 (5`, `(5 5`, `5 5`, `(-0` and
    `-inf` each find exactly their matches.
- `the_index_finds_every_match_or_says_there_are_too_many`, unchanged,
  passes.
- `bench --filters`: see the next section.
