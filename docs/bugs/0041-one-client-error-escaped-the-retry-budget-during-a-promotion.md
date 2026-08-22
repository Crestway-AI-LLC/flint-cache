# BUG-0041 — one client error escaped the retry budget during a promotion

**Status:** OPEN, low rate, and NOT a flaky test. Seen once on the ops gate,
2026-08-22, run 32547390884.

## The claim being violated

`docs/slo.md`: *"Zero acked-write loss and zero client-visible errors"* through
failover. `tier2_promote_drill.sh` states it as the design's contract: *"the
proxy's retry budget absorbs the promotion window, so the outage is a latency
spike, not failures."*

The drill asserts `ERRS == 0` on a writer running through a real master kill.
It saw 1.

## Everything else was perfect, which is what makes this narrow

From the drill log, the self-heal chain was fast and clean:

    kill                       ~1787367468
    promote executed           1787367473434
    promote VERIFIED           1787367473443   (9 ms)
    attach  executed           1787367474188
    attach  VERIFIED           1787367475097
    re-confirmed over 6 probes
    role=master, live_replicas=1, ex-master revived in place

Both structural asserts passed and membership was unchanged. The agent did its
job. What leaked was one client-visible error at the edge during the ~5s the
pair had no master — the writer issues an INCR every 10 ms for 14 s, so the
retry budget absorbed on the order of 1,400 writes and missed one.

## Why this is not a test to relax

A single error out of ~1,400 is exactly the shape a green streak hides, and
the assert is a product claim rather than a test convenience. Relaxing it to
`ERRS <= 1` would convert a rare contract violation into a permanent blind
spot, and the contract is one customers evaluate: "your cache returns errors
during failover" is a different product from "your cache gets slower during
failover".

Treating it as flake was the first instinct here and it was wrong. The gate
failing intermittently on this drill may mean the product fails intermittently,
which is the more expensive reading and therefore the one worth checking first.

## What is not known, and the instrumentation gap that caused that

The writer counts two different events into one number:

    except Exception:  errors += 1; break        # socket reset or timeout
    elif b.startswith(b"-"): errors += 1         # a "-ERR ..." reply

So it is unknown whether this was a dropped connection or an error reply, and
if a reply, what it said — MOVED, a shed, a "no master for slot". Those have
different causes and different fixes.

**Before hunting the cause, make the next occurrence diagnosable**: record the
error kind and the literal reply bytes into `$D/phase2`, and print them in the
FAIL line. The same lesson as the seat-startup failures — the failure is not
instrumented, and the next run will be just as silent as this one.

Rate is unmeasured. Seen once; the gate has run this drill many times, so it
is rare, and a rate is worth having before anyone concludes it is fixed.
