# BUG-0091 — four core files carry the ops repo's license header, and it grants nothing

**Found** 2026-09-04 while adding `tools/collection_read_peak.py`, by making the
same mistake and catching it in review: I copied the header form from
`packaging/aws/gate-box/run.sh`, which lives in the **ops** repo.

Status: OPEN · Severity: low, but legal-adjacent rather than technical — nothing
misbehaves, and the repo says two different things about the same code.

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
