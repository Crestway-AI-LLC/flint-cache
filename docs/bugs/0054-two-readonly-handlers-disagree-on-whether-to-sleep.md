# BUG-0054: two -READONLY handlers disagree on whether to sleep, and a negative control is pinned to the slower one

Status: OPEN, found 2026-08-26 · Severity: LOW as a product matter, MEDIUM as a
gate matter — it reds main on a timing margin of 2 ms, and the drill it reds is
correct to fail.

## The red

`main` at `d7781ed` failed `gate (drills)` on `promote_notice`. The commit was
`accel: colocate an object's chunks in one slot`, which has nothing to do with
the proxy's promotion path; the other 126 steps passed. What failed was the
drill's **negative control**:

    == A) NEGATIVE CONTROL: failover with NO notice (today's behaviour)
      steady-state write (incl. cli spawn): 62ms — the yardstick
      first write after failover: 90ms
      that is 28ms above steady state
    FAIL: expected the reactive path to cost >=30ms above baseline (the 50ms
          retry sleep), got 28ms.

The drill is behaving well. It refuses to report run B's comparison when it
could not establish that run A is slow, which is the discipline that makes the
rest of the suite worth reading. The defect is that the condition it needs is
not one the run controls.

## Why 28 and not 50

`do_failover` demotes then promotes, so the old master stays UP and answers
`-READONLY` — deliberately, because that is the case with no connection error.
The proxy has **two** handlers for that reply, and they disagree:

    forward_collect (main.rs:1389), handler at :1426
        topo.rediscover_after_failure(&addr);
        backends.drop_conn(&addr);
        forward(...).await          // retries IMMEDIATELY, no sleep

    forward (main.rs:1441), handler at :1526
        topo.rediscover_after_failure(&addr);
        backends.drop_conn(&addr);
        std::thread::sleep(Duration::from_millis(50));   // then loops

Whichever handler services the write decides the measurement. Through
`forward`, the cost is a probe plus 50 ms and the control passes. Through
`forward_collect`, it is a probe plus one round trip — 28 ms on this runner —
and the control fails. The 30 ms floor is justified in the drill's own comment
by "the 50ms retry sleep", a sleep that one of the two paths never executes.

Corroboration that it is the path and not the machine: `promote_notice` passed
twice today on a 16-vCPU c7i.4xlarge (8.1s, 8.0s) against the same code, and
failed on the 4-vCPU GitHub runner at `load 4.50` with four drills in flight.
That is consistent with contention changing which path wins a race, and it is
NOT the same claim as "the box was busy so the number drifted" — the two
outcomes are 28 and ~50, not a continuum.

## The sleep is probably the wrong half to keep

`forward`'s branch sleeps 50 ms **after** rediscovering. If rediscovery found
the new master, there is nothing to wait for; the sleep is pure added latency
on every controlled failover, and `forward_collect` demonstrates that retrying
immediately works. A backoff makes sense when the retry would hammer an
unchanged routing table — but the rediscovery is what makes that not the case.

So the likely correct resolution inverts the drill's assumption: make both
handlers retry immediately, and the reactive path gets FASTER, not slower.
Which means `promote_notice`'s negative control is currently pinned to a
defect. Fix the proxy and the drill reds on every run.

## What must land, and in what order

1. **The drill must stop depending on an ambient race.** Same shape as
   BUG-0049: force the condition instead of hoping for it. The honest form is
   to assert the MECHANISM rather than a wall-clock delta — that the reactive
   path performed a rediscovery and the hinted path did not — because that is
   the thing run B is really about, and it does not move with the runner.
   Lowering the 30 ms floor would be the wrong fix; it weakens a check to match
   a measurement rather than making the measurement mean something.
2. **Then reconcile the two handlers.** One condition, one retry discipline.
   Whichever is chosen, it must be chosen deliberately and the drill must not
   silently encode the answer.

Doing 2 before 1 turns an intermittent red into a permanent one.

## Not yet established

Which handler serviced the failing run — that would need the drill's own log
from the uploaded artifact, or a counter. The two-handler asymmetry is read
from the source and is certain; that it is the cause of THIS red is the leading
explanation and not yet proven. A cheap discriminator: log which handler fired,
and re-run under load.
