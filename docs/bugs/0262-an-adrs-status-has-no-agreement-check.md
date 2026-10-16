# BUG-0262: an ADR's status has no agreement check, and three said PROPOSED over shipped code

**Status:** **FIXED 2026-10-15.** `assert_adr_index_markers_agree_with_status`
holds every index row's trailing state marker against its write-up's Status
line, with fixtures in `tools/adr_state_markers_drill.sh`, wired into
`document_assertions` and the drill list. **It found a fourth on its first
run:** ADR-0014's row said `(proposed)` over "Accepted and implemented in
full — D1, D2, D3". The past-tense-in-code heuristic below is *not* outstanding
work — it was never proposed, for the reason given.
**Severity:** low in production, real in planning — it mis-routes work.
**Area:** `docs/adr/*.md`, `docs/adr/README.md`, `tools/gates.sh`.

## What was wrong

`tools/gates.sh` carries two checks that hold a bug's state against itself:
`assert_bug_titles_agree_with_status` (H1 vs its own Status line) and
`assert_bug_index_markers_agree_with_status` (the index row vs the file).
`tools/bug_state_markers_drill.sh` runs both against fixtures. **Nothing
equivalent exists for ADRs**, and three had drifted:

| ADR | said | code said |
|---|---|---|
| 0020 | `PROPOSED — August 2026` | `apool.rs`: "the socket explosion ADR-0020 removed", "the batching ADR-0020 wanted". Landed `2a7e6ac5`, `adcc69dc`, `5b3799de` (second implementation; the first is the revert the ADR itself names) |
| 0021 | `PROPOSED — August 2026` | `apool.rs` assumes "ADR-0021's model: a bounded set of workers". Landed `eabf2639`, `36412e2e` (stage 2, `crate::pool` removed), `8cf94e74` |
| 0027 | `proposed` | `main.rs:4302` "ADR-0027 part 2: a run of consecutive pure writes accumulates here"; `:4119` "armed at 128 threads before ADR-0027". Landed `0f7a49e4`, `b210961b` |

ADR-0027 is the clearest case: **its own index row in `docs/adr/README.md`
already said `implemented` while the file said `proposed`.** Two copies of one
fact, contradicting each other, with no check between them — precisely the
shape `assert_bug_index_markers_agree_with_status` was written for, one
directory over.

## Why it costs something

The same cost BUG-0180 records for bugs, and it landed the same way. A session
asked "what core work is unbuilt?", read these statuses, and was about to
report ADR-0020, 0021 and 0027 to Jeff as **proposed designs awaiting his
decision** — six items where the true number was three. The index exists so a
state can be read without opening the file; a wrong entry is worse than none.

`gates.sh` records this happening twice already for bugs: BUG-0014 and
BUG-0050 were each reported to Jeff as open work, read straight off the bug
index. The checks were added for bugs and the ADR tree was left as it was.

## Not done

The check. The two bug functions are lifted out of `gates.sh` by
`bug_state_markers_drill.sh` with `sed`, so the shape to copy already exists.
An ADR's vocabulary is wider than a bug's (`PROPOSED`, `ACCEPTED`, `DECIDED`,
`ACCEPTED and IMPLEMENTED`, `SUPERSEDED`, and lowercase variants of each), so
the narrow rule applies: only the unambiguous contradiction, in both
directions, or it will be switched off within a week.

**A second check was considered and is DECLINED (Jeff, 2026-10-16):** an ADR
whose status is `PROPOSED` while `crates/*/src/**` refers to it in the past
tense ("ADR-NNNN removed", "before ADR-NNNN", "ADR-NNNN stage N"). That is what
actually found 0020 and 0021, and it is still the sharper instrument — but the
past-tense vocabulary is open-ended, and the cheap-looking version of this
check false-positived on its FIRST run while being written: "last state
parenthetical in the row" read ADR-0029's "(proposed the same day)", which is
prose about an earlier state, as the row's claim. A looser matcher over code
comments has more room for that, not less. The row-vs-file check plus the habit
of reading an ADR's last dated section before quoting its decision covers the
same ground at no false-positive cost.
