# BUG-0055: `forward` sleeps 50 ms after it has already found the new master

Status: FIXED 2026-08-26, pending gate. Both handlers now share one rule:
back off only when the re-probe left routing pointing at the seat that just
refused. · Severity: LOW — 50 ms added to the first write after every
controlled failover, on one of two code paths.

The fix is not "delete the sleep". A backoff earns its place when rediscovery
found nothing and an immediate retry would spin against the same refusal until
`RETRY_BUDGET`. What was wrong is that neither handler asked which case it was
in: one always slept, the other never did. `Topology::still_master` is that
question, and it is about the routing table rather than about which call path
happens to be running.

Unit-tested both directions, with a positive control against each degenerate
predicate — always-true restores the unconditional sleep and always-false
removes it entirely, and both are previously-shipped behaviours, so neither
would look obviously wrong. Both stubs fail the test.

MEASURED on the gate box, 2026-08-26:

    before:  first write after failover 49-64 ms above steady state
    after:   first write after failover  1 ms above steady state

with both mechanism assertions intact — run A still observes a reactive
re-probe triggered by the demoted seat, run B still observes none.

**That last part is the whole reason BUG-0054 had to land first.** Under the
old assertion — `SLOW_DELTA >= 30`, justified by this very sleep — a 1 ms
delta reds `promote_notice` permanently on correct code. The drill was
measuring the defect, so the defect could not be removed while the drill
still measured it. Fixing the check first, in its own change, is what made
this a normal fix rather than one that arrives holding a weakened test.

Split out of BUG-0054. It was that bug's first diagnosis and was refuted as its
cause; the asymmetry itself is real and was never in question.

## The two paths

`flint-proxy` handles a `-READONLY` reply — a demoted-in-place ex-master, which
is what a controlled failover produces — in two places, differently:

    forward_collect (main.rs:1389), handler at :1426
        topo.rediscover_after_failure(&addr);
        backends.drop_conn(&addr);
        forward(...).await                              // retries at once

    forward (main.rs:1441), handler at :1526
        topo.rediscover_after_failure(&addr);
        backends.drop_conn(&addr);
        std::thread::sleep(Duration::from_millis(50));  // then loops

## Why the sleep looks wrong

It sleeps *after* rediscovery. If `rediscover_after_failure` found the new
master — and on a controlled failover it will, the promotion having already
happened — there is nothing left to wait for. A backoff earns its place when
the retry would hammer an unchanged routing table; rediscovery is precisely
what makes that not the case, and `forward_collect` demonstrates the immediate
retry works.

Measured cost, from the six runs in BUG-0054: the first write after a
controlled failover lands 49-64 ms above steady state, of which 50 ms is this
sleep.

## Do not "fix" it without reading BUG-0054 first

`promote_notice` used to assert that the reactive path costs >=30 ms above
baseline. Removing the sleep would have made that drill fail permanently on
correct code — a check cemented to a defect. That assertion is now
mechanism-based (a re-probe triggered by the demoted seat, no clock), so this
is safe to change today. It was not safe yesterday, and that ordering is the
reason this is a separate bug rather than a line deleted in passing.

## What must land with it

One retry discipline for one condition, chosen deliberately. If the sleep goes,
something must still bound a retry storm against a pair that is genuinely
mid-promotion — the deadline at `:1450` (`RETRY_BUDGET`, 5 s) is the existing
backstop and may be enough on its own. A test should assert the chosen
behaviour on BOTH paths, since the bug is that they disagreed and nothing
noticed.
