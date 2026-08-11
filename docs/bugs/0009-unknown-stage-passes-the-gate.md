# BUG-0009: an unrecognised stage ran nothing and printed GATES PASSED (RESOLVED)

Status: RESOLVED 2026-08-11 · Found by reading, while adding `bloom` to `CORE` · Severity: high (the release gate reports a false green)

## Symptom

    $ tools/gates.sh --help
    GATES PASSED — logs kept in /tmp/flint-gates
    $ tools/gates.sh drill
    GATES PASSED — logs kept in /tmp/flint-gates

Exit 0 both times, in under a second, having run nothing. `drills` is the
stage and `drill` is the typo; `--help` is what you type before you know the
stage names at all.

## Root cause
Stage selection was two lines:

    want() { case " ${STAGES} " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }
    STAGES="${*:-check conformance drills chaos}"

`STAGES` was whatever was on the command line, unvalidated, and `want` asked
whether a stage name appeared somewhere in that string. An unrecognised
argument is therefore not an error — it is a `STAGES` that no `want` matches.
Every `if want ...` block is skipped, `FAILED` is never appended to, and the
closing `[ -n "$FAILED" ]` reads an empty string as success.

Nothing separated "everything passed" from "nothing ran". The only observable
difference was runtime, and nobody times a gate they expect to be green.

## Why this one is worse than it reads
Every other route into this project points here. `docs/release-checklist.md`
names `tools/gates.sh` as the authority for the pre-tag ritual,
`.github/PULL_REQUEST_TEMPLATE.md` says its exit status is the answer, and
`README.md` documents running the stages individually — which is exactly the
invocation that carries an argument, and so the only one that could hit this.
The last check before a tag was the one that could be silently switched off by
a typo.

It is also the failure class this file was written against, arriving through
the argument channel: the header explains that a list which gets retyped gets
dropped, the `CORE` comment records an audit that found 48 of 82 drills in the
gate and no reason written down for the other 34, and BUG-0002 is `verify`
calling a one-node pair healthy. Same shape each time — a green that means
"this was not tested" and reads as "this passed".

Second defect, same root: `rm -rf "$LOGS"` ran before any argument was
inspected, so a false-green `--help` also deleted the previous run's logs on
its way out. The evidence you would go back to was destroyed by the command
that told you nothing was wrong. There were logs from a 09:07 run sitting in
`/tmp/flint-gates` on the morning this was found.

No release is known to have been cut on a run of nothing. The fix is
preemptive; what makes it urgent is that if it had happened, there would be no
trace of it — a gate that skips everything leaves the same empty evidence as a
gate nobody ran.

## Fix
Arguments are parsed and validated at the top of the script, before the `cd`
and before the log directory is cleared:

- unrecognised stage → the offending argument, the usage block, and exit 2
  (the "refused to run" status the disk check already uses; 1 stays "gates
  failed"). Every argument is validated, so `gates.sh drills drill` refuses
  rather than running a partial gate and reporting on it.
- `-h`/`--help` → the usage block, exit 0. Served deliberately instead of
  falling through the same hole, and printed out of the file's own header
  comment with `awk` rather than retyped, so there is no second copy to drift.
- validation ahead of `rm -rf "$LOGS"`, so refusing to run and asking for help
  both keep the last run's logs.

## The check that holds it
Two, because validation only closes the hole we know about.

The second is `RAN_STEPS`, incremented inside `step()`. Zero steps at the end
prints `GATES DID NOT RUN` and exits 2 instead of `GATES PASSED`, whatever the
reason — so a future refactor cannot reintroduce a green run of nothing by
some other route. The pass line carries the count now (`GATES PASSED — 6
steps`), which makes a gate that quietly shrinks visible in its own output
rather than only in a diff of this file.

`tools/gates_drill.sh`, in `CORE`. It invokes the real script only in ways
that exit during argument handling — `--help`, and stages it must refuse — and
forces the dispatch on a copy for the two states the command line can no
longer produce. Every case asserts the three things that were wrong together:
the exit status, that no line begins `GATES PASSED`, and that the log
directory outlived the run. It needs no build and costs about a second,
because it is a property of the harness rather than of the product.

Shown to go red on each condition it exists for, which is what makes it a
check rather than a habit. Against the pre-fix script (`e9f4117`) it fails on
`--help`; with the `RAN_STEPS` backstop deleted it fails on the forced-
dispatch copy; with a stage dropped from `ALL_STAGES` it fails naming that
stage; with the log clear moved back above validation it fails on the marker
file. It carries a positive control too — the same copy with dispatch forced
to `drills` and `step` stubbed must still print `GATES PASSED — N steps`, with
N matching both the steps it printed and the length of `CORE`. Without it,
"cannot report a pass after running nothing" would also be satisfied by a
gates.sh that is merely broken.

The drill earned its place on its first run, by failing. The refusal message
used the words "GATES PASSED" mid-sentence while explaining the bug, so
anything grepping the output for the verdict matched an explanation of it —
the same collision as the `^error` / `errors: 0` note in the script, which is
already the reason its own failure summary is anchored. The message no longer
spells the verdict, and the drill anchors the pattern at column 0.
