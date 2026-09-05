# BUG-0091 — four core files carry the ops repo's license header, and it grants nothing (FIXED 2026-09-05)

**Found** 2026-09-04 while adding `tools/collection_read_peak.py`, by making the
same mistake and catching it in review: I copied the header form from
`packaging/aws/gate-box/run.sh`, which lives in the **ops** repo.

Status: **FIXED 2026-09-05** · Severity: low, but legal-adjacent rather than
technical — nothing misbehaved, and the repo said two different things about
the same code.

## What is wrong

This repository is Elastic-2.0, and **220 files under `tools/` and `crates/`**
declare it as `# SPDX-License-Identifier: Elastic-2.0`. Four declare instead:

    # Copyright (c) 2026 Crestway AI LLC. All rights reserved.

| file |
|---|
| `tools/min_replicas_survivable_drill.sh` |
| `tools/kill_release_drill.sh` |
| `tools/induced_ratchet_drill.sh` |
| `tools/kill_order_drill.sh` |

That line is correct **in the ops repo**, which is private and reserves
everything. Here it sits in a public, source-available tree beside 220 files
that grant a license, so a reader who takes per-file headers at face value is
told these four are not covered by the terms the rest of the repo offers.

## Why this is not being fixed in the same breath as finding it

Elastic-2.0 is a **grant**. Rewriting "All rights reserved" into an SPDX
identifier on files in a public repository hands out rights that the current
text withholds, which makes it an outward-facing change and Jeff's call rather
than a cleanup. The mechanical edit is four lines and takes a minute; the
decision it encodes is not mine to make by inference.

The likely history is exactly the mistake this file was found by: the header was
pasted from the sibling repo, where it is right. If that is so, the fix is to
replace all four with the SPDX line and nothing else follows. If any of the four
were deliberately reserved, the opposite is true and the repo needs a note
saying so, because the current arrangement reads as an oversight either way.

## How to check it stays fixed

The repo has no header check — that is why four files drifted. A one-line gate
step comparing the two counts (`SPDX-License-Identifier` against
`All rights reserved` under `tools/` and `crates/`) would refuse the next paste,
and unlike most gate steps it cannot be flaky. Worth adding **with** the fix
rather than instead of it: the count is only meaningful once it is correct.

## One of the four was mine, and it is fixed (2026-09-04)

`tools/induced_ratchet_drill.sh` was written **hours after this bug was filed**,
by pasting the header from the ops repo — the exact mistake described above,
made again by someone who had not read this file. It is now
`SPDX-License-Identifier: Elastic-2.0` like the other 203.

That one needed no decision from Jeff. The reasoning this file gives for
deferring is that rewriting the header *grants rights the current text
withholds*, and that is a real question for code whose reservation might have
been deliberate. For a file authored today, whose header was a copy-paste from
a sibling repo and every one of whose neighbours grants Elastic-2.0, the intent
is not in doubt.

**Three remain**, and for those the deferral above stands unchanged:
`kill_order_drill.sh`, `kill_release_drill.sh`,
`min_replicas_survivable_drill.sh`. They predate this bug, so whether their
reservation was deliberate is exactly the open question, and it is Jeff's.

The header check this file recommends is still not added, deliberately: a count
comparing SPDX against "All rights reserved" would fail on those three, so it
is worth adding **with** the decision and not before. 203 SPDX to 3 reserved,
as of this edit.


## FIXED 2026-09-05

All four now declare `# SPDX-License-Identifier: Elastic-2.0`, and the counts
are 228 against 0. One (`induced_ratchet_drill.sh`) was fixed on 2026-09-04 by
the session that wrote it; the remaining three — `kill_release_drill.sh`,
`min_replicas_survivable_drill.sh`, `kill_order_drill.sh` — on Jeff's
instruction, which is what this file was waiting for. Elastic-2.0 is a grant,
so the change hands out rights the previous text withheld, and that was not a
call to make by inference.

### The check, and the trap it walked into first

`assert_license_headers_are_this_repos` in `tools/gates.sh` refuses any tracked
file under `tools/` or `crates/` carrying the ops repo's form. It asks `git
grep` rather than the filesystem, for the reason `assert_gate_is_executable`
gives: the tracked bytes are what gets published.

**Its first run failed on `gates.sh` itself.** The check quoted the string it
searches for, so it matched its own source. That is the identical trap
`plain_process_exit_stays_out_of_the_running_paths` documents two thousand
lines away in the same file — *"the scan was matching its own assertion
messages, which quote the very string they are about"* — and the identical
remedy applies: the needle is assembled from fragments rather than written.

Verified both ways, because a header check that cannot fire is worse than none:
it passes on the clean tree, and it fires on a planted file carrying the ops
header.