# BUG-0041 — one client error escaped the retry budget during a promotion

**Status:** OPEN — cause still unknown. The next occurrence is now
DIAGNOSABLE (2026-08-22), which was the blocking gap. Low rate, and NOT a
flaky test. Seen once on the ops gate,
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

## The instrumentation, and the hang it uncovered

`tools/tier2_promote_drill.sh` (ops repo) now records, per error: the KIND
(`reply` / `socket` / `eof`), how far into the run it happened, how many writes
had been acked by then, and the literal bytes. It goes to `$D/phase2-errors`
and the FAIL line prints it. An ABSENT detail file is treated as the finding —
"the writer did not finish" — rather than as nothing to show.

Exercised against a fake edge that replies `-ERR`, then closes:

    reply  at +0.023s  after 2 acked  '-ERR no master for slot 42'
    eof    at +0.061s  after 4 acked  peer closed the connection mid-reply (partial=b'')
    note   writer stopped early: the edge closed the connection

**Writing that control found a latent hang in the writer, pre-existing and in
this bug's own path.** The read loop was:

    while not b.endswith(b"\r\n"): b += s.recv(64)

A clean close returns `b""` forever and raises nothing, so on EOF this spins on
empty reads and never re-tests the deadline. The writer hangs, `wait $WRITER`
never returns, and the drill dies on the gate's timeout **with no error
recorded at all**.

That matters here specifically. "The proxy dropped the connection during the
promotion" is a live candidate for the original failure, and it is the one
cause the writer could not have reported — it would have hung instead, and a
hang does not look like a client-visible error. So the single error seen on
2026-08-22 is not necessarily the only occurrence; a clean-close occurrence
would have presented as a timeout, not as `ERRS=1`.

Note also that a socket error **breaks the loop**, so any error also truncates
the run. "1 error" and "1 error then stopped writing" are different failures
and the acked count alone does not separate them, so the detail file says which
happened.
