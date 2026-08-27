# BUG-0056: the release gate runs ~130 drill scripts and checks that none of them parse (OPEN)

Status: OPEN, found 2026-08-26 · Severity: low, but the failure it permits is
expensive to diagnose.

## Symptom

`tools/gates.sh` has no `bash -n`, no `shellcheck`, no `shfmt`. Verified:

    $ grep -nE 'shellcheck|bash -n|shfmt' tools/gates.sh
    $ echo $?
    1

So the gate executes ~130 drill scripts, its own libraries under `tools/lib/`,
and itself, without ever asserting that any of them is syntactically valid. A
syntax error in a rarely-exercised branch of a rarely-run drill surfaces only
when that branch runs — as a confusing runtime failure, at the end of a long
gate, on a box that is about to terminate.

## Why this is worth a check rather than a shrug

The same class of bug cost real money in the sibling ops repo on the same day.
`packaging/aws/gate-box/run.sh` generated a remote script that failed to PARSE,
so nothing ran — including the line that records the exit status — and the
caller rendered the missing status as *"the run may still be going"*. The most
alarming outcome produced the most reassuring message, and the box sat idle for
48 minutes believing it was mid-sweep. Write-up: ops `docs/field-notes.md` §3,
"A script that fails to PARSE reports as 'the run may still be going'".

A parse error is the cheapest possible bug to find and one of the more
expensive to find late. `bash -n` over the whole tools tree is well under a
second.

## The fix, and the trap in it

Add an early `step`-gated stage running `bash -n` over `tools/*.sh`,
`tools/lib/*.sh` and `gates.sh` itself. Early, so it fails in seconds rather
than after a build.

**It must assert the file list is non-empty before certifying.** A glob that
stops matching — a directory rename, a `set -u` interaction, a `cd` that did
not happen — produces "checked nothing, found nothing wrong", which is
indistinguishable from a pass. This repository has the same self-testing shape
in `assert_every_drill_accounted_for`; copy it rather than inventing a second
one.

Control it in both directions before committing: a deliberately broken
temporary script must fail the stage, and removing it must pass. A stage that
has only ever been observed passing is not known to be able to fail.

Open question worth deciding rather than defaulting: `tools/lib/*.sh` are
sourced rather than executed, and `bash -n` on a sourced fragment is still
meaningful, but a file that is only ever sourced into a specific context may
legitimately not stand alone.
