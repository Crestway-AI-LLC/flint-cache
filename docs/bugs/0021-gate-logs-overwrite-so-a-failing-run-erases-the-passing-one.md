# BUG-0021: gate step logs are overwritten, so a failing run destroys the passing run's evidence (OPEN)

Status: OPEN, found 2026-08-18 · Severity: **medium** — it costs exactly the
comparison you need at the moment you need it, and it costs it silently

## Symptom

Two full gates were run an hour apart on the same branch. The first passed
`chaos`; the second failed it on a data-integrity assertion. The obvious first
move is to diff a passing chain traversal against a failing one.

That was impossible. Both runs wrote `/tmp/flint-gates/chaos-chaos.log`, and
the second overwrote the first. The passing run's detail no longer exists.

## Root cause

`gates.sh` keeps one log file per STEP, not per RUN:

    Logs land in $FLINT_GATE_LOGS (default $FLINT_DRILL_ROOT/flint-gates) — one
    file per step, kept whether it passed or failed.

"Kept whether it passed or failed" is true within a run and false across runs.
The header's promise is about pass-vs-fail, and the loss is about
run-vs-run — so the file reads as if it has already addressed this.

## Why it matters more than it looks

The file's own header explains why these logs exist:

> Every claim in this project that "the drills pass" has meant a shell
> one-liner reconstructed from memory, whose failures were summarised to one
> line and whose output was then thrown away — so a flake and a real bug
> looked identical, and telling them apart meant running everything again.

Telling a flake from a real bug is *exactly* a cross-run comparison. The
retention that was built to answer that question does not survive the second
run, which is the first moment the question can be asked.

It also interacts badly with intermittency. For a load- or timing-dependent
failure the natural response is to re-run — and every re-run destroys the
evidence from the run before it, so the harder a failure is to reproduce, the
more certainly its history is erased.

## Fix

Namespace the logs per run — a timestamp or the commit under test:

    $FLINT_GATE_LOGS/<utc-stamp>-<short-sha>/<step>.log

with the last run symlinked for convenience. Retention can be bounded (keep N
runs) without reintroducing the problem, since the point is to survive the
*next* run, not to keep everything forever.

Recording the commit in the path also fixes a second thing quietly: today a
gate log cannot say which tree produced it, and two branches diverging in one
file was already enough today to make identical line numbers disagree.

## Worked around, this once

The failing log was copied out by hand before anything could overwrite it. That
is not a fix; it depends on someone realising the hazard while the file still
exists, which is the same reliance on memory the header set out to remove.

## The CI half is worse: the log is never captured at all

Locally a log survives until the next run. In CI it does not survive the job.
The gate prints the path and stops:

    FAIL  chaos_unreadable       (5s)  /tmp/flint-gates/chaos-chaos_unreadable.log

That file lives on the runner, is never dumped to the job log and is never
uploaded as an artifact, so it dies with the container. **Everything CI can
tell you about a failing drill is the word FAIL and a duration.**

Measured cost, 2026-08-18: reconstructing BUG-0014's history meant reading all
91 `gate` runs on `main` and scoring each by whether `chaos_unreadable`
produced a PASS or FAIL line, because that line is the only surviving evidence.
Four failures were found. **Three of them have no assertion text and never
will** — they are known to be the same drill, not verified to be the same bug.
A durability oracle fired three times in CI and left no record of what it saw.

This also removed the cheapest check on a wrong hypothesis. Two separate
"this was fixed" claims were argued that day from commit dates and run
outcomes alone; had the drill logs been retrievable, comparing a failing
assertion against a passing one would have settled it in minutes.

## Fix, second half

Upload `$FLINT_GATE_LOGS` as a job artifact whenever the gate fails, and dump
the failing steps' logs inline so a red run is legible without downloading
anything. Both halves are one change once the logs are namespaced per run: the
directory that becomes safe to keep locally is the directory to upload.

## Related

- BUG-0009 — also `gates.sh`, also a result that does not mean what it says
- BUG-0014 — the intermittent whose diagnosis this retention loss obstructs;
  three of its four CI firings have no recoverable assertion text because of
  the second half above
