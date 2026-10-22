# BUG-0271: an `FT.CREATE` backfill moved documents out of write order, in its own index and in every other index on the prefix (FIXED 2026-10-21)

**Status:** **FIXED 2026-10-21**, before any release served `FT.*`. ADR-0060's
release N, on public main after v0.1.0-rc.85, had it. **Found by reading
the code, not seen in a run.** I was tracing an ordering failure in
`coproc_ft_drill`'s new GEO section (ADR-0062). That failure turned out to
be the documented order after a restart (below), and the drill was wrong.
A mutation check confirms the drill never reaches this bug: with the old
call put back, it passes, because its backfills finish before its writes.
Held by `a_backfill_keeps_a_written_document_in_its_place`, a test of the
function the backfill now calls, and by `coproc_ft_drill`'s `fillorder`
section, the second case below, which fails with the old call put back.
No test overlaps a backfill with writes, the first case.
**Severity:** low. Which documents an index answered was right, and so was
what it held of each: the backfill reads each key under the same lock a
write takes. Only the order of ties and of unsorted results was wrong, for
documents written while an index was backfilling.
**Area:** `crates/flint-vec/src/ftchan.rs`, `fill_chunk`;
`crates/flint-vec/src/ftstore.rs`, `Store::ft_reindex`.

## What happened

Results that tie, and results with no order asked for, come in the order
their documents were last written, as RediSearch's document ids order them
(ADR-0060 amendment 3, part 6). Each document carries the sequence number of
the last time it was indexed.

`FT.CREATE` over a prefix answers once the index exists. Its backfill then
reads every key under the prefix, on channels the index's searches lend it.
Meanwhile writes to the prefix are served, and each write indexes its
document.

The backfill put each key it read through `Store::ft_reindex`, which a write
uses. That had two effects:
- **In the index being filled,** a document a write had already indexed was
  indexed again when the backfill reached it, with a new sequence number.
  Documents written during a backfill therefore came in the backfill's
  `SCAN` order instead of write order.
- **In every other index covering the key,** `ft_reindex` put the document
  again too, though those indexes were not backfilling. One index's
  backfill could reorder another, complete index.

It shows in two cases:
- **Writes during a backfill.** An index's backfill is still running while
  documents under its prefix are written: an `FT.CREATE` over many existing
  keys, backfilled over several searches, with writes between them.
- **A second index over a covered prefix.** No concurrent write is needed.
  The new index's backfill re-put every document it read into the index
  that already covered it, so that index's ties came out in the new index's
  `SCAN` order.

## The fix

The backfill now calls `Store::ft_fill_doc`. It puts a key into the index
being filled and no other, and only when that index does not hold the key
yet. A copy a write put there was read under the key's lock, as the
backfill reads it, so it is the key as it is: skipping it loses nothing.
Writes still go through `ft_reindex`, into every index that covers the key.

## Not changed

A co-processor's rebuild after a restart still indexes each key as its scan
finds it, so after a restart results that tie come in scan order. ADR-0060
amendment 3, part 6, decided that, as RediSearch's reload numbers its
documents again; `docs/command-support.md` now says so. A namespace answers
`-LOADING` to writes and searches alike until its rebuild finishes, so no
write lands during a rebuild to be moved. The drill failure that led here
was exactly this: after a restart, Catania came before Palermo.
