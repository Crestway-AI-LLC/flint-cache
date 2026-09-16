# BUG-0157: case G judges `fleet_guard` against an empty box, and the guards that would have noticed cannot fail (FIXED 2026-09-15)

Status: **FIXED 2026-09-15**, found 2026-09-16 by the cache session off a real
gate failure · Severity: **medium** — it reddens a green gate intermittently and
reports the wrong cause when it does, which is more expensive than it sounds: a
suite that fails for a reason nobody can name teaches everyone to re-run rather
than to read.

## What was reported

    FAIL: with no live peer lock, FLINT_DRILL_PARALLEL=1 tolerated a foreign
    seat anyway (exit 0) — that is an amnesty, not a distinction

    GATES FAILED: fleet_guard, 151 steps, 20260916T041253Z

The cache session read the shape correctly: the final arm does
`rm -rf "$PEER.lock"` and asserts exit 1 with **no control that the foreign seat
is still alive**, so *"the guard tolerated a foreign seat"* and *"there was no
foreign seat left to refuse"* are the same exit 0 and the same message.

**One correction to the report, and it is the useful part.** They attributed the
assertion to `078e64e`, this session's BUG-0153 arm. It is from `de09f93e`
(2026-08-23). That moves the search from a new arm to a **clock**.

## The clock

The peer fixture's two seats were spawned with the shared `spawn_argv`, which is
`sleep 60`. Its lock's pid was `sleep 300`. So the lock outlived the seats it
vouched for by four minutes — and case G makes **five** `fleet_guard` calls,
each of which can spend `FLINT_FOREIGN_SETTLE` (15s by default) waiting for a
teardown to clear. On a loaded 4-wide box that is over a minute of budget
against a 60-second fixture.

When the fixture expires mid-case, **every remaining arm reads an empty box as a
verdict about the guard.**

`078e64e` added a fifth guard call and a sleep. That is cumulative pressure on a
pre-existing budget rather than the cause, which fits the reporter's own
timeline: three green gates on trees that already contained it, then a red one
after `e283ac2` registered `port_allocator` in CORE and changed 4-wide batch
composition.

## The second defect, which is why the first was invisible

Four places in this drill prove a fixture exists before asserting anything about
it. All four did:

```sh
ps -eo args= | grep -q -- "<pattern>"
```

**That matches its own grep.** The grep is running while `ps` samples, and its
argv contains the pattern. Measured: a pattern matching nothing real returns
**1** from the pipeline and **0** from a snapshot taken first. So the checks
whose entire job is *"the fixture is there, so what follows means something"*
pass with no fixture at all — a check that cannot fail, four times over, in the
one place that would have caught the clock.

## Reproduced twice, by construction

- **Shrink the fixture** to `sleep 5` and the **first** arm fails, not the last:
  *"a peer seat did not refuse WITHOUT FLINT_DRILL_PARALLEL — the flag would be
  untested"*. So this is not one arm's missing control; every fixture-dependent
  arm in the case has it.
- **Kill the fixture immediately before the final arm** and the reported message
  comes back verbatim — exit 0, *"tolerated a foreign seat anyway"* — with
  nothing foreign on the box at all.

## Fixed

- **`PEER_LIFE_S=$(( 20 * ${FLINT_FOREIGN_SETTLE:-15} ))`**, derived from the
  budget it has to survive rather than picked as a round number, and used for
  the lock's pid *and* both seats so they expire together. Them expiring
  separately is precisely the state that produces a false verdict.
- **`require_peer_seat <what is about to be asserted>`** before each of the four
  dependent arms. A dead fixture now reports a wall-clock expiry, says this is
  not a defect in the guard, and names the knob to raise.
- **`seat_in_ps`** replaces all four pipeline-form guards: snapshot first, then
  a quoted `case` substring test. No self-match, no subprocess, and no
  `printf: write error: Broken pipe` — which the obvious `printf | grep -q`
  rewrite emits on every call, because `grep -q` exits at the first hit and
  closes the pipe under the `printf`.

**Mutation-verified**: the injected kill that produced the false amnesty now
produces the expiry message, and the drill is otherwise clean.

## Corroborated independently, and the discrepancy is the interesting part

The cache session fixed this in parallel, reached the same two findings, and
dropped their version rather than push a competing fix to one arm. Three things
from their account are worth keeping here.

**They measured the pipeline at 2, this file says 1, and both are right.** The
count depends on whether any ANCESTOR's command line also carries the pattern:
run the measurement from a shell whose own argv contains the literal and you get
the grep plus that ancestor. Assembling the pattern at runtime, so no ancestor
can carry it, gives **1 from the pipeline and 0 from the snapshot** — and 1 is
the number that matters, because it is the irreducible self-match that exists in
the drill, where the pattern comes from a variable and the launcher is
`bash tools/fleet_guard_drill.sh`. A measurement of a `ps` match is itself
sensitive to how the measurement was launched, which is a small lesson of the
same family as the bug.

**A mutation test can survive a vacuous check.** They wrote the same
`ps | grep` control into their own fix, mutation-tested it with a two-second
seat, and it PASSED. It came apart only when they instrumented the arm instead
of believing the mutant: a debug line reading `arm G elapsed 59s; server seat
visible: 1` for a seat whose lifetime was two seconds. Stated generally, and it
belongs beside the other verification notes: **a mutation test that a vacuous
check survives looks exactly like a mutation test of a sound check that the
mutation did not reach.** Both are a green run after an injected fault. Telling
them apart needs the check's OWN answer printed, not the arm's verdict.

**The vacuous idiom propagated by looking established.** Their copy came from
arm F, read as the file's house style — which is how a bad shape spreads inside
a file that is otherwise careful, and why all four call sites were converted here
rather than only the one that failed.

## Not established

**Whether the 2026-09-16 failure was an expiry or a real concurrent peer's lock
covering the seat.** The reporter could not tell from the log and neither can I;
the synthetic peer declares `7788|7789`, which are in `DRILL_DEAD_PORTS` so no
real drill should claim them — which argues against a port match but not against
a scope match. That is the point of the change rather than a gap in it: the next
occurrence will say which.
