# BUG-0143 — the WAL-gap quarantine does not always fire when the span is gone

**Status:** FIXED 2026-09-14 — **and the quarantine was never broken.** The
drill was asserting something it had not established. Filed OPEN the same day,
observed 2 of 3 consecutive runs on the gate box; everything below the fix
section is the original report and is left as written.

## The fix: the precondition was about B, not about A

The drill proves that **B would refuse** cursor 604 if asked. It then judges
**A** for not quarantining, in a message that asserts *"A really asked for
it"* — which nothing had checked, and which is often false.

The master does not tear down a stalled replica. Its per-replica send loop
treats a write timeout as backpressure, not as a death (`main.rs`, the 50 ms
`set_write_timeout` and its `WouldBlock | TimedOut` arm): it drains acks and
retries, unbounded. So B holds A's link open for the entire `SIGSTOP`. On
`SIGCONT`, A reads the batches already in flight, applies them **in order**,
and never re-issues `FLINTSYNC` — so it never sees the WALGAP.

**That outcome is correct.** A received every sequence. There is no gap for a
quarantine to protect against, and the archive recycling is irrelevant to a
replica that never has to re-request. The intermittency was whether the stall
happened to break the link — which the drill does not control and never
checked.

**So the drill no longer resumes A. It kills it while it is still stopped.**
A then never drains what is in flight, its persisted cursor stays at the
purged value, and the restart must re-admit from there — the state under test,
reached deterministically rather than by hoping. `SIGKILL` is not blockable
and needs no scheduling, so a stopped process dies without running again.

The failure message now distinguishes the two states before judging: if A did
re-admit and still did not quarantine, that is the original, real assertion
and it stands word for word; if A never re-admitted, the drill says the setup
did not reach the state instead of blaming the fix.

**What this does not change:** nothing in the product. No quarantine code was
touched, because none of it was wrong. The three questions the original report
listed as unestablished are answered by the mechanism above, except the last —
ownership — which is now moot.

---

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
