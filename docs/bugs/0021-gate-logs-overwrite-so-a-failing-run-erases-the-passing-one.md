# BUG-0021: gate step logs are overwritten, so a failing run destroys the passing run's evidence (FIXED)

Status: **FIXED 2026-08-18**, found the same day · Severity: **medium** — it
cost exactly the comparison you need at the moment you need it, and it cost it
silently

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

## RETRACTED: "CI never captures the log" — it always did

An earlier revision of this file claimed CI prints the failing drill's log path
and never uploads it, so a durability oracle had fired three times leaving only
the word FAIL. **That was wrong, and it was the most confident paragraph here.**

`gate.yml` has always ended with:

      - name: Keep the logs
        if: always()
        uses: actions/upload-artifact@v4
        with: { name: gate-logs, path: /tmp/flint-gates, retention-days: 14 }

All four of BUG-0014's firings still had unexpired `gate-logs` artifacts,
including the 2026-08-11 one. Downloading them recovered every assertion text
and produced the sharpest narrowing that bug has had — see BUG-0014.

**How the claim was made:** `gh run view --log` returns the JOB log, which
carries only the one-line `FAIL  chaos_unreadable  (5s)  <path>`. One channel
was searched, nothing was found, and the absence was written up as an
impossibility with the word "never" attached. The artifact channel was never
checked. This is the same class as everything else in this file — a check that
could not see the thing reporting as though the thing were not there — and it
was committed while documenting that class.

**The real defects, both much smaller than the retracted claim:**

1. **The failure detail is not inline in the job log.** Reading a red gate
   requires knowing that artifacts exist and downloading one. Dumping the
   failing steps' logs into the job output costs nothing and removes the step
   where someone concludes the evidence is gone.
2. **14-day retention is thin for a rare intermittent.** BUG-0014 fires about
   once every 14 gate runs, so its evidence and its next occurrence are on
   comparable timescales. For the drills that assert durability, a longer
   retention is worth the storage.

Neither justifies the alarm. Recorded at length because the retraction is more
instructive than the finding would have been: the local overwrite problem
below is real and was verified by hand, and its neighbour in the same file was
invented from a single unchecked channel.

## The root cause was worse than this file said

The section above blamed one file per STEP. The line is stronger than that:

    LOGS="${FLINT_GATE_LOGS:-${FLINT_DRILL_ROOT:-/tmp}/flint-gates}"
    rm -rf "$LOGS"; mkdir -p "$LOGS"

Every run **deleted the whole directory**. Not just same-named steps — a
re-run of one stage erased the logs of steps it never touched, so narrowing an
investigation by re-running a subset destroyed evidence the subset had nothing
to do with. That is what it did to BUG-0024 the same day: the local gate log
holding the `slot_cutover` EWOULDBLOCK was gone, replaced by two later passing
runs, and the trigger for that bug is still open partly because of it.

## Fixed

`tools/gates.sh` now writes **one directory per run**:

    $FLINT_GATE_LOGS/<utc-stamp>-<short-sha>[-dirty]/<step>.log

with `$FLINT_GATE_LOGS/latest` pointing at the newest and the **newest 20 runs
retained**. Bounded retention does not reintroduce the bug: the point is to
survive the *next* run, not to keep everything forever.

The id carries the tree as well as the time. `-dirty` is not decoration — a
bare sha on a tree with uncommitted changes names a commit that was never the
thing under test, and two branches differing in one file was already enough
that day to make identical line numbers disagree.

Two runs inside one second collide on the id (`gates_drill.sh` does exactly
that), so a colliding id gets a `.1` suffix rather than letting the second run
write into the first run's directory.

### The failing logs are dumped inline

A red gate now prints the last 200 lines of each failing step's log, marked and
prefixed, after the `GATES FAILED:` line. The full log stays in the run
directory and in the CI artifact; the tail is there so nobody has to know an
artifact exists to read a failure. That extra step is exactly where the
retracted claim below came from.

### CI

`gate.yml` retention goes 14 days -> **90**. BUG-0014 fires about once every 14
gate runs, so its evidence and its next occurrence were on comparable
timescales — the retention was expiring at roughly the rate the bug recurred.

**Anything parsing these artifacts must be updated**: the artifact now holds
`<run-id>/<step>.log`, not `<step>.log` at the top level. The `latest` symlink
is excluded from the upload so the artifact does not carry a second copy of
every log.

### Verified

The assertion is layout-independent and was shown able to fail. Run the gate
once, drop a sentinel beside whatever logs it wrote, run it again:

    NEGATIVE CONTROL (gates.sh committed before the fix):
    before   run1 wrote 100 logs -> after run2 the tree holds 100; SENTINEL DESTROYED
    FIXED:
    after    run1 wrote 100 logs -> after run2 the tree holds 200; SENTINEL SURVIVED

The pre-fix column is the point: without it, "the sentinel survived" is equally
satisfied by a test that could never have destroyed it.

The inline dump was verified by running a forged gate whose single drill emits
a distinctive string and exits 1 — the string appears in the gate's **output**,
not only in the file, and the 200-line cap held at 200. `gates_drill.sh`, which
asserts against `gates.sh` itself including its log retention, passes
unchanged.

## Related

- BUG-0009 — also `gates.sh`, also a result that does not mean what it says
- BUG-0014 — the intermittent this file was written in service of; its four
  CI firings were all recoverable from artifacts, which is what the retraction
  above is about
