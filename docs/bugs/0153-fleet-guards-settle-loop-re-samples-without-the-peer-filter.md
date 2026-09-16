# BUG-0153: `fleet_guard`'s settle loop re-samples without the peer filter, so one orphan makes every live peer drill foreign (OPEN)

Status: **OPEN**, found 2026-09-15 · Severity: **medium** — it does not corrupt
anything and it does not reach production; it reddens a green gate
intermittently, which is worse than it sounds, because a suite that fails 2 runs
in 4 for a reason nobody has named teaches everyone to re-run rather than to
read.

## The observation

`fleet_guard_drill` case G failed **2 of 4** drills-stage runs on 2026-09-15,
with:

    FAIL: FLINT_DRILL_PARALLEL=1 still refused a live peer drill (exit 1)

**The output contradicts itself, in the same six lines.** Three lines after
announcing the peer drop, the guard lists the very seats it just dropped:

    (8 seat(s) belong to 3 live peer drill(s) in this suite -- not foreign)
    REFUSING TO RUN: this box already has Flint processes outside /tmp/flint-guard-drill
        pid 999273  ppid 1       …flint-server --port 6846 … --data-dir /tmp/flint-upgr…
        pid 999824  ppid 1       …flint-server --port 6845 … --data-dir /tmp/flint-upgr…
        pid 1005080 ppid 996081  flint-server --data-dir /tmp/flint-guardpeer/d 60
        pid 1005082 ppid 996081  flint-controller --pairs 127.0.0.1:7788,127.0.0.1:7789 --id peerctl 60
        …
        6 of 13 are ORPHANS (ppid 1)

`/tmp/flint-guardpeer/d` and `--id peerctl` on 7788/7789 are **case G's own peer
fixture**, the one the line above says is not foreign. Thirteen seats are listed
where the filtered set held five: all eight peer seats are back.

**Not caused by the change under test.** It failed on a tree whose only edit was
`coproc` inventory parsing, and passed twice on trees that changed more.

## What I filed first, and why it was not the defect

The first reading was *"case G cannot classify an orphan"*. That is wrong, and
usefully so. Peer tolerance is keyed on a **live peer lock** —
`_fleet_live_peer_scopes` requires the lock's pid to still be running with the
recorded start time — and an orphan is by definition a seat whose lock is gone.
Case G's own negative control **demands** that treatment:

> `FAIL: with no live peer lock, FLINT_DRILL_PARALLEL=1 tolerated a foreign seat
> anyway — that is an amnesty, not a distinction`

So the guard calling an orphan foreign is the contract working, not failing.

## The actual defect: two samples, one classification

`fleet_guard` samples the foreign set **twice**, and only the first sample is
filtered:

```sh
foreign="$(_fleet_foreign)"
if [ -n "$foreign" ] && [ "${FLINT_DRILL_PARALLEL:-0}" = "1" ]; then
  peers="$(_fleet_live_peer_scopes)"
  [ -n "$peers" ] && foreign="$(… | _fleet_drop_peer_lines "$peers")"   # filtered
fi
[ -z "$foreign" ] && [ -z "$sibling" ] && return 0
…
while [ -n "$foreign" ] && [ "$_fw" -lt "${FLINT_FOREIGN_SETTLE:-15}" ]; do
  sleep 1
  foreign="$(_fleet_foreign)"                                          # NOT filtered
done
```

The settle loop exists for a good reason — the previous drill's seats can still
be exiting when the next one's guard samples — but it rebuilds the population
from the raw scan and **loses the peer classification the first pass applied**.

So: let one unfiltered seat survive the first pass, and the loop puts every live
peer's seats back into `foreign`. They are live, so they never clear; the guard
spins the whole 15 seconds and then refuses, printing peer seats as foreign.
That is exactly what case G sees — its own `sleep 300` peer fixture, tolerated a
moment earlier, comes back as a foreign fleet the instant an unrelated orphan is
on the box.

**And it changes the verdict, not just the report.** With a live peer plus one
seat that is genuinely on its way out, the first pass drops the peer and leaves
the dying seat; the loop waits for the dying seat, which exits — and then finds
the peers, which do not. A run that should have proceeded refuses.

This is the recurring shape: *a population taken from a convenient place rather
than from the authority*. The authority here is "foreign, minus this suite's
live peers", and only one of the two sample sites computes it.

## The fix, and what it does NOT fix

Resolve the live peers **once** and apply them to **every** sample, so the loop
cannot re-derive a different population. The filter itself is correct;
`_fleet_drop_peer_lines` with an empty list is the identity, so it can be
applied unconditionally and there is no second code path to keep in agreement.

**This does not make case G green.** Six of the thirteen seats in that run were
genuine orphans, and an orphan never clears: the guard refuses, correctly, and
case G's `[ "$RC" = 0 ]` fails whatever the peer filter does. What the fix
removes is the part that is actually wrong — a refusal that names live peers as
foreign, and, in the case where the only unfiltered seat is one that is DYING, a
verdict that flips. That second one is a real change of outcome and not just of
wording: the first pass leaves the dying seat, the loop waits for it, it exits —
and then the loop finds the peers, which do not, so a run that should have
proceeded refuses after the full 15 seconds.

Case G's own flakiness has two causes and this is neither of them; both are
below, and both are somebody's decision rather than a defect I can settle.

## What this bug does NOT settle, deliberately

**Whether drills should leave orphans at all.** `upgrade`'s 6845/6846 outlived
it in both failures, and the guard's own comment says the flint-kv suite did the
same twice in an hour on 2026-08-27. A suite that orphans seats is a separate
question from a guard that misclassifies them once they exist, and the fix above
is right whichever way that one is answered — which is why it is not waiting on
it. An orphan will still, correctly, refuse the run after this fix; it will just
refuse it for itself, naming itself, in under a second.

**And case G's exit-code assertion is global**, which with the orphans above is
the actual cause of the flakiness: `[ "$RC" = 0 ]` can be moved by anything else
on the box. The arm three lines below it already carries the lesson —

> ISOLATE THE PORTS KEY, DO NOT COUNT THE BOX. … an exact global count, which is
> the same unscoped-census mistake this whole change set exists to remove,
> committed inside the test for it.

— applied to the message and not to the exit code above it. The regression arm
this bug adds is deliberately local for that reason: it asserts that the peers
are **not named**, which is true whatever else the box makes the guard decide,
rather than asserting a verdict any stranger's orphan can move.

## Numbering

Filed as 0153 because 0152 was taken. The peer pushed public `291c57b` at
2026-09-16T00:02Z and my claim row is timestamped 00:30; I had picked the number
by reading `docs/bugs/` and `coordination.md`, where it was free, 28 minutes
after it had stopped being free in the place that decides. The reservation is
the pushed file, not the claim row.
