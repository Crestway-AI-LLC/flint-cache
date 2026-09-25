# BUG-0180: an OPEN marker followed by punctuation was not a marker, and the bug-state checks had never seen a fixture (FIXED)

**Status:** FIXED 2026-09-25 in `tools/gates.sh`. Tooling only; nothing
ships.
Found porting ops OPS-0324, where the same checks let a row say OPEN for a
month over a deployed fix.
**Severity:** low. Nothing on this index was misread by the check today. The
defect is in what it could not see.

## Symptom

This repository's two bug-state checks, `assert_bug_index_markers_agree_with_status`
and `assert_bug_titles_agree_with_status`, are shared with the ops repository
by convention. There, on 2026-09-25, OPS-0026's index row still said OPEN over
a Status of *BUILT … and DEPLOYED*, and OPS-0019's row ended
*OPEN; the metric is fixed, the reasoning is not* over a write-up that fixed
the reasoning a month earlier. Both passed.

## Root cause

Two gaps, both present here:

- A row marker counted as OPEN only when its first word *equalled* `OPEN`,
  while closed words matched by prefix. So `(FIXED; …)` was a marker and
  `(OPEN, …)` was not. On a row with no title marker whose evidence has a
  FIXED marker and then an OPEN-with-a-comma, the check took the FIXED.
- The closed vocabulary omitted DONE and BUILT. This index uses neither
  today. The ops index opens 15 Status lines with them.

Neither check had a fixture drill, so their vocabulary was whatever the real
index happened to exercise.

This index reads a marker in the TITLE cell before one elsewhere in the row.
So BUG-0050's row, whose title says FIXED and whose evidence ended in an
OPEN-with-mechanism-confirmed parenthetical, was never misjudged by the
check. It was still misleading to a reader, and is corrected here.

## Fix

- `is_open_word` strips trailing `;,.:` before comparing, as the closed words
  already behaved.
- DONE and BUILT join both closed lists; each copy says the other exists.
- `tools/bug_state_markers_drill.sh` (new, in CORE) lifts both real functions
  and runs them over a fixture tree:
  - six contradiction shapes must be named;
  - five agreeing shapes must not, including a FIXED title over quoted OPEN
    history;
  - the clean tree must report its comparison.
- BUG-0050's row no longer ends in a stale OPEN.

## Verification

- The drill passes. It goes red three ways, each with one change reverted:
  - `is_open_word`: `the index check did not name 0002`;
  - the index check's vocabulary: `… did not name 0001`;
  - the title check's copy: `the title check did not name 0005`.
- The first version of fixture 0002 had no title marker and only the OPEN.
  The unmarked-row rule caught that with or without the change, so the drill
  passed with `is_open_word` reverted. The fixture now carries an earlier
  FIXED in its evidence, the only shape where the change decides the verdict
  on this index.
- `DOCUMENT GATES PASSED` on the real index: 171 of 180 compared, the extra
  one being this write-up. No existing row changed verdict.
