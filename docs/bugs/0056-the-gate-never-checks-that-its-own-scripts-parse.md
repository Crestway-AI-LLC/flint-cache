# BUG-0056: the release gate runs ~130 drill scripts and checks that none of them parse (FIXED; the fix could not see the class that motivated it — EXTENDED 2026-09-05)

Status: FIXED 2026-08-27, EXTENDED 2026-09-05 (the title said OPEN for nine days
after the fix landed) · Severity: low, but the failure it permits is
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

## Fixed 2026-08-27

`assert_scripts_parse` runs `bash -n` over `tools/*.sh`, `tools/lib/*.sh` and
`gates.sh` itself, **first** in the check stage — 137 files, well under a
second, ahead of `fmt` and every other assert, since a script that does not
parse can break the asserts that follow it.

Two guards, both of which the write-up above asked for:

**The empty-glob guard.** `n == 0` fails rather than certifying. A directory
rename or a `cd` that did not happen would otherwise report "checked nothing,
found nothing wrong", which is indistinguishable from a pass — the same trap
`assert_every_drill_accounted_for` names, stated the same way.

**A positive control on the validator itself**, which the write-up did not ask
for and which turned out to be the more interesting half. This check's entire
verdict rests on one external command. If `bash -n` were a no-op here — a
shell without `-n`, a PATH surprise — it would silently certify all 137 files.
So it first feeds `bash -n` a file containing an unterminated `if` and refuses
to report a verdict if that is *accepted*. A check whose validator is never
itself tested is the shape of every bug in field-notes §1.

Three controls run against the finished check: a real syntax error is caught
and the offending file and line named; an empty match refuses to certify; a
clean tree still passes, so it is not a check that always fails.

## Immediate relevance

The same day, a mechanical reorder touched **46** drill scripts at once for
BUG-0061. `bash -n` over all of them was run by hand, because the gate could
not do it. That is precisely the edit class this stage exists for: a large
sed-shaped change where one bad substitution is invisible until the affected
drill happens to run.

## 2026-09-05 — the check could not see the incident it was written for

The stage above parses files. **`bash -n` does not parse heredoc bodies** —
they are data to the enclosing script — so a file whose heredoc payload is
malformed passes cleanly. Verified rather than reasoned:

    $ printf '%s\n' "ssh host bash -s <<'R'" "if true; then" "R" > hd.sh
    $ bash -n hd.sh            # ACCEPTED
    $ sed -n '2p' hd.sh > payload.sh && bash -n payload.sh
    payload.sh: line 2: syntax error: unexpected end of file

That is not a corner case here. It is **exactly the incident quoted at the top
of this file**: ops `packaging/aws/gate-box/run.sh` generated a REMOTE script
that failed to parse, so nothing ran — including the line that records the exit
status — and the caller rendered the silence as "the run may still be going".
The stage as built would have certified that script.

### What the extension does

`_gate_heredoc_payloads` extracts every heredoc whose payload is shell, writes
each to its own file, and parses it. Three decisions worth keeping:

- **Shell by USE, never by a list.** A payload counts as shell when the line
  opening it feeds one (`bash -s`, or a redirect into a path ending `.sh`) or
  when the payload declares itself with a shebang. A list of "heredocs that are
  shell" would be a second declaration to keep in sync with the first
  (BUG-0086).
- **Unquoted heredocs are TEMPLATES.** With `<<EOF` the shell resolves `\$`,
  `` \` ``, `\\` and backslash-newline before the payload runs, so the raw
  text is not the text that executes. Exactly those four escapes are emulated
  and nothing else; `$VAR` is left alone because it parses as a word either
  way. **Found by doing it**: two of the ops repo's 55 payloads reported
  syntax errors that did not exist until this was handled —
  `VERIFY_OUT=\$(cat …)` parses as an assignment followed by a subshell. A
  check with two false failures on day one teaches everyone to ignore it.
- **The extractor gets its own positive control.** An extractor that finds
  NOTHING certifies every payload in the tree by examining none of them, and
  reports a pass. So a broken payload is planted and must be both found and
  rejected before a clean sweep of the real ones is believed. That control
  earned its place immediately: the first version wrote its probe with the
  `'"'"'` idiom *inside double quotes*, where a single quote needs no escaping
  at all, producing the delimiter `"R"` — which never matched the closing `R`,
  so the extractor found nothing. The control caught it; a bare sweep would
  have reported a green tree.

### The control that shows the gap

With the `worker.sh` payload inside `gates.sh` deliberately broken:

    $ bash -n tools/gates.sh          # passes — the file check is blind
    GATES FAILED: these heredoc shell payloads do not parse:
          tools/gates.sh:846

Two mutations run: the payload broken (caught, with its source line), and the
extractor neutered so it matches nothing (caught by the planted control, not by
silence).

### Where the coverage actually matters, and what is left

Core has **one** heredoc shell payload and no `packaging/*.sh`. The ops repo has
**55**, across `upgrade-ops-box.sh`, `deploy-phase1.sh`, `roll-fleet.sh` and
`gate-box/run.sh` itself — which is where the incident happened, and which is a
different repository from the one this bug was filed and fixed in. That half is
tracked as ops OPS-0130; the fix landing only here is the "fixed where found,
not where it lives" shape the ops repo has now hit four times.
