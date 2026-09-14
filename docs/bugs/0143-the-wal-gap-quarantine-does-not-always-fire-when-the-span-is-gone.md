# BUG-0143 — the WAL-gap quarantine does not always fire when the span is gone

**Status:** OPEN — observed 2026-09-14 on the gate box, **2 of 3 consecutive
runs**. Not mine and not investigated beyond what the drill reports; filed so
the next full gate does not read a red `walgap_quarantine` as noise.

## What happened

`walgap_quarantine_drill.sh` FAILED at `no quarantine after the purge`, at
38.8 s and 39.4 s in two runs, having PASSED in a run immediately before them
on the same tree. So it is intermittent, not a regression introduced by the
commits under test.

The drill proves its own precondition before judging, which is why the verdict
is worth something. Round 3 of its own output:

```
round 3: cursor 604 is now UNREACHABLE (WALGAP cursor 604 is no longer
reachable from this WAL (oldest retained batch starts at 1482, past the 605
needed
...
FAIL: no quarantine after the purge.
```

and its own failure text says what that means:

> The precondition was PROVEN above: B answered WALGAP for this cursor before
> A was resumed. So the span really is gone, A really asked for it, and the
> quarantine really did not fire. **This is a finding about the fix, not about
> the drill.**

That sentence was written by whoever built the drill, to be printed in exactly
this situation. It is the drill distinguishing its own flakiness from a product
defect and reporting the second.

## Why it is worth a file rather than a re-run

The quarantine is a **data-safety** mechanism: a replica that asks for a span
the master no longer has must be stopped rather than allowed to continue from
a cursor nothing can satisfy. A quarantine that fires only sometimes is
indistinguishable, in a green run, from one that works.

It is red on `main` for whoever runs a full core gate next, and a red drill
with no file beside it is the thing this repo keeps paying for: the next
person spends the investigation again from zero.

## What is NOT established

- **Why it is intermittent.** Three runs is three points; nothing here
  identifies a race, a timing dependence, or a platform.
- **Whether it reproduces off the gate box.** Both failures and the pass were
  on `c7i.xlarge` in the same suite, run 4-wide.
- **Whether any data is actually at risk.** The drill asserts the quarantine
  did not fire; it does not follow the replica afterwards to see what it then
  served. That is the question a fix would have to answer first.
- **Who owns it.** The drill was last touched for BUG-0082; the quarantine
  path is the subject of BUG-0050, BUG-0062 and BUG-0063, none of which
  records this symptom.

## How it was found

By running the full drill suite for an unrelated change (three new drills,
`expand_fill`/`subset_ratchet`/`near_cache_cross_client`). It failed beside
them, in a lane none of them touch — different ports, different state dirs —
and it had passed in the run where those same drills were present, which is
what rules them out as the cause.
