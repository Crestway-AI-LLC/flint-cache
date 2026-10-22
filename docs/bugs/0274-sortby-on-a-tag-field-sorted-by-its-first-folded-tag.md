# BUG-0274: `SORTBY` on a TAG field sorted by its first tag, folded, where RediSearch sorts by the field's whole value (OPEN)

**Status:** **OPEN 2026-10-22.** Its fix is ADR-0063 amendment 1's
mechanism, proposed the same day, and waits on that amendment's acceptance.
ADR-0060's release N, on public main after v0.1.0-rc.85, has it, and no
release has served `FT.*` yet. **Found by reading RediSearch's source** for
that amendment, not seen in a run: `document.c`'s tag preprocessor and
`sorting_vector`, and `value/src/comparison.rs`. No client suite's test
reaches it.
**Severity:** low. Which documents a search answers is right; their order
under `SORTBY` on a TAG field is not, whenever the field holds several tags,
or tags that differ only in case where RediSearch keeps the case.
**Area:** `crates/flint-vec/src/ftindex.rs`, `sort_order`.

## What happens

Flint sorts a TAG field by the first of the document's tags, after they are
split at the separator, folded to lower case (unless `CASESENSITIVE`) and
sorted. RediSearch sorts by the field's whole value as the document holds
it:
- **A `SORTABLE` TAG** puts its whole value, `"b,a"` and not `a`, into the
  document's sorting vector, case-folded (ICU's `fold_string`) unless the
  field is `UNF`.
- **A TAG that is not `SORTABLE`** is loaded from the key when a `SORTBY`
  names it, and sorted as it is written: no folding.
- **Strings compare as bytes,** and a document without the field comes last
  either way.

| documents' `t` | RediSearch, `SORTBY t` | Flint |
|---|---|---|
| `b,a` and `a,c` | `a,c`, then `b,a` | a tie on `a`, so write order |
| `Bob` and `alice`, not `SORTABLE` | `Bob` (`B` is 66), then `alice` (`a` is 97) | `alice`, then `bob` |

## The fix (pending)

Keep each TAG field's sort value with its document: the value as written,
case-folded when the field is `SORTABLE` without `UNF`. ADR-0063
amendment 1 proposes the same for TEXT fields, and how to fold.

## Acceptance

A unit test in `ftindex.rs`: the two cases in the table, a `SORTABLE` TAG
folded and a `SORTABLE UNF` TAG kept as written, each ascending and
descending, with a document missing the field last both ways. It must fail
with today's `sort_order`.
