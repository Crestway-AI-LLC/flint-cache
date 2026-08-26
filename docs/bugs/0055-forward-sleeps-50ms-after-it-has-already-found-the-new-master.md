# BUG-0055: `forward` sleeps 50 ms after it has already found the new master

Status: OPEN, found 2026-08-26 · Severity: LOW — 50 ms added to the first write
after every controlled failover, on one of two code paths.

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
