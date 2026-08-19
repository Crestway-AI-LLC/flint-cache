# BUG-0028: half the drills ignore their own build's exit status, so a standalone run tests whatever binary happens to be there (FIXED)

Status: **FIXED 2026-08-19**, found while fixing BUG-0026 · Severity: **medium** —
masked under the gate, live for any drill run by hand, and it is what let
BUG-0026's broken build line survive undetected

## Measured

    drills with a `cargo build` line : 54
      exit status CHECKED            : 28
      exit status IGNORED            : 26

(53/27/26 before merging `origin/main`; the extra guarded drill is
`chaos_edge_tls`, which arrived with that merge. A peer measuring the same
property on their own branch got 54/23/31, and reconciling the three sets of
numbers needed both halves: the drill COUNT differed by the ref, the GUARDED
count differed because their classifier did not credit `set -e`. Attributing
both differences to one cause was wrong in each direction.)

The 27 are not accidental. They share one idiom, verbatim:

    cargo build --release -q -p flint-server ... --features flint-server/rocks \
      || { echo "FAIL: build"; exit 1; }

So the convention exists, is established, and is applied to exactly half the
drills that build. Nothing tells you which half a given drill is in, and the two
are indistinguishable from the outside.

Drills declare `set -u`, not `set -e` (100 of 108), so an unguarded `cargo build`
that fails does not stop the drill. It carries on against whatever is already at
`target/release`.

## Why it is masked, and when it is not

**Under `tools/gates.sh` it cannot bite.** The gate builds the whole workspace
in its own step before any drill runs, so a drill's failed rebuild leaves
correct, current binaries in place and the drill tests the right thing anyway.

**Run standalone it bites silently.** The drill proceeds against whatever is in
`target/release` — a stale binary from an earlier build, or one from the other
feature config. The assertions then pass or fail against code that is not the
code under test, and nothing in the output says which binary it used.

That conditional is the defect: the same drill, same output, means two different
things depending on how it was invoked, and it never says which.

## This is how BUG-0026's broken drill stayed invisible

`tenant_remove_drill.sh` had a `fleet_warm` call spliced into the middle of its
build's backslash continuation, so it ran `cargo build … flint-controlplane
fleet_warm ./target/…` and then tried to execute `-p flint-controller …` as a
command. Both failed. The drill passed for years of runs because it only ever
ran under the gate, where the binaries were already built — and because it never
looked at its build's exit status. Two independent maskings, and the drill's
`fleet_warm`, the entire point of that line, never ran once.

## Fix

Statically checkable, and it belongs beside `assert_drill_builds_keep_rocks` —
same file, same lines, a different property of them. Every drill that invokes
`cargo build` must guard it, by `set -e` or by the established `|| { echo "FAIL:
build"; exit 1; }`.

27 drills already comply, so the assert can go in with 26 mechanical edits and
no design argument. Add it and the fixes together, or the check fails on
arrival.

Worth deciding at the same time whether drills should build at all, given the
gate builds the workspace first. The build lines exist so a drill can be run by
hand; that is exactly the case where the guard matters, which argues for keeping
them and guarding them rather than deleting them.

## Method note

The first measurement printed each drill's FIRST `^set -` line as the evidence
for its verdict rather than the line that actually matched, so a drill with
`set -u` early and `set -e` later displayed as "set-e: set -u" and looked
misclassified. The verdict was right; the evidence display was wrong. It was
caught only because the printed evidence contradicted the printed verdict — had
they happened to agree, the number would have shipped unchecked.

Print what the check matched, not what you expect to be there.

A second flaw, found by a peer re-deriving the census: the rule credited `set -e`
appearing ANYWHERE in the file, when it only guards a build that comes AFTER it.
Measured precisely: ONE drill (`anti_affinity`) does set `-e` below its build,
at :109 against a build at :76 — but that build carries its own inline
`|| { echo "FAIL: build"; exit 1; }`, so its guarded verdict never rested on the
unreachable `set -e`. No drill's verdict depends on a `set -e` that cannot reach
its build, so the count of 26 is unaffected — but the rule was unsound and
happened not to bite.

The first attempt to measure THAT reported 2 offenders, and both were artifacts
of the checking script, not the drills. `re.fullmatch(r'set -[a-z]*e[a-z]*')`
against whole lines does not match `set -euo pipefail` (the trailing word), while
the census extracted `^set -\S+` first and so matched `set -euo`. Two checks of
the same property, two extractions, two answers. `anti_affinity` was flagged for
a `set -e` 30 lines below a build that carries its own inline guard;
`chaos_unreadable` for a later `set -e` when `set -euo pipefail` already preceded
it. Both verdicts were right for reasons the second check could not see. "There are none
today" is not "the check is right", and an assert built on that rule would have
passed a drill that set `-e` on the line after its build. The ordering is part
of the check.

Also worth separating precisely, because collapsing them cost a round trip: two
census figures differed between refs for TWO reasons, and attributing both to
one was wrong in each direction. The drill COUNT (53 vs 54) really was the ref —
an unpushed drill on the other branch. The GUARDED count (27 vs 23) was entirely
the classifier's treatment of `set -e`. "The ref is not the cause" was as
over-strong as "the ref is the cause".

The "inline guard" classifications were then spot-checked against the real files,
because a `&&` inside an argument would satisfy the same test. All sampled ones
are genuine `|| { echo "FAIL: build"; exit 1; }`.

## Related

- BUG-0026 — the broken build line this masking hid, and the dead check that
  hid the other half of it
- `assert_drill_builds_keep_rocks` — already reads these same lines, for
  whether they name rocks

## Fixed

Two asserts in `tools/gates.sh`, beside `assert_drill_builds_keep_rocks`, which
already reads the same lines for a different property.

**`assert_drill_build_is_checked`** — every drill that invokes `cargo build`
must guard it, by a `set -e` that PRECEDES the build or by the established
`|| { echo "FAIL: build"; exit 1; }`. The ordering is part of the check, for the
reason in the method note above.

**`assert_no_continuation_splice`** — catches the shape that made `fleet_warm`
decorative: a command spliced into the middle of a backslash continuation.
Detector and discrimination rule from the Running AI Agent session. The rule is
the whole check: only a BARE trailing backslash with balanced quotes is a
splice, because a continuation legitimately precedes a new command after `||`,
`&&`, `|`, `;`, an opening group, `then`/`do`/`else`, and inside a multi-line
quoted payload.

The 26 unguarded drills were fixed mechanically, appending the existing idiom to
the last physical line of each build statement. All 108 drills still parse.

### Verified, negative arm first

    harness: both functions loaded
    === NEGATIVE: clean tree — both must be silent ===
      build-guard: silent (rc=0)
      splice:      silent (rc=0)
    === POSITIVE: plant an unguarded build ===
      rc=1  GATES FAILED: drill(s) run cargo build without checking it:
              tools/zzunguarded_drill.sh:3
    === POSITIVE: plant a splice ===
      rc=1  GATES FAILED: a command is spliced into a backslash continuation:
              tools/zzsplice2_drill.sh:3

The splice detector was additionally run against the three legitimate shapes it
must not flag — `|| \` before a command, `&& \` before a command, and a
multi-line double-quoted `ssh` payload — and stayed silent on all three. That
arm matters more than the positive one: an earlier, cruder sweep found the same
single real instance without being able to tell it from those three, and would
have false-positived the moment someone wrapped a `||` chain.

The guard itself was controlled too, rather than assumed:

    set -u ; false || { echo "FAIL: build"; exit 1; } ; echo REACHED
      -> rc=1, "FAIL: build", REACHED never printed

## The stronger fix, not taken here

The fleet repo does not have this class at all: 34 drills, **zero** `cargo build`
lines, a single `stage-bins` step, and the drills consume
`./target/release/<bin>`. Controlled with the same scanner against this repo, so
the zero is a result rather than a broken regex.

Centralising the build removes the class instead of policing 54 instances, and
it is available here — the gate already pre-builds, which is exactly what masks
this today, so the drills' own build lines are redundant under the gate and
load-bearing only standalone.

It is not free. The fleet repo pays for it with `stage-bins`, which is its own
machinery and had its own bug: the drills call `./target/release/<bin>` for
public binaries that exist there only as symlinks `stage-bins` creates, and a
fresh CI checkout died with "nothing listening" on every drill until that was
found. Removing 26 unguarded build lines by centralising makes the centraliser
load-bearing for all 54.

The assert is cheaper and shipped now. The fleet shape is the better end state
and should not be lost.
