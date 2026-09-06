# BUG-0041 — a write in flight at master loss outlives the retry budget

*(title was "one client error escaped the retry budget during a promotion" —
refuted 2026-09-05. Nothing escaped: the budget was SPENT, to the millisecond.
Filename kept so links survive.)*

**Status:** CAUSE CONFIRMED 2026-09-05 — the retry budget is exhausted, not
escaped. The fix is a constant nobody should change alone; see *The decision
this leaves* at the foot. The instrumentation added 2026-08-22 is what answered
it, on the first two occurrences that carried it.

**Seen THREE times, not once.** 2026-08-22 (run 32547390884), 2026-08-25
(32801679641) and 2026-08-29 (33277288038). The "seen once" above was written
before the other two happened and was never revisited; found by classifying all
125 ops-gate failures over 988 runs, 2026-08-02 to 2026-09-05.

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

## The answer (2026-09-05)

Both surviving occurrences recorded the same three facts, and they are the whole
diagnosis:

| run | when | acked so far | the literal reply |
|---|---|---|---|
| 2026-08-29 | `+5.026s` | **0** | `-ERR no reachable master for this slot` |
| 2026-08-25 | `+5.019s` | **0** | `-ERR no reachable master for this slot` |

(2026-08-22's artifact had expired — 14-day retention — so it is unconfirmed,
but it predates the instrumentation and could not have said more anyway.)

**`+5.02s` is `RETRY_BUDGET`.** `flint-proxy/src/main.rs:67` sets it to exactly
5 s, and every error exit in the retry loop is guarded by `Instant::now() >
deadline`. The reply is the string at the `let Some(addr) = target else` branch:
no master was known, rediscovery kept finding none, and the budget ran out 26 ms
late. Nothing escaped the retry; the retry gave up.

**`after 0 acked` is the other half, and it is why exactly one write errors.**
This is the writer's FIRST write. The drill starts it at the kill, so that one
request has the entire masterless window ahead of it. The writer is serial, and
a `-ERR` reply does not break its loop, so the next write is issued at
`+5.03 s` with a FRESH 5 s budget against a window that is nearly over — and
sails through. Exactly one error, always the first, run completes at ~1,400
writes. Every observed occurrence has that shape.

### The margin was never positive

From the 08-29 journal, the agent's own timestamps:

    PromoteReplica recommended   1788041161426
    promote ActionExecuted       1788041166029    (+4,603 ms)

and 08-25: `1787625434460 -> 1787625438750`, **+4,290 ms**. Detection happens
*before* the recommendation, so the masterless window is 4.3–4.6 s **plus** the
sweep interval (400 ms) and the re-confirm streak already inside it (6 probes ×
300 ms = 1.8 s). Against a 5 s budget the margin is a couple of hundred
milliseconds at best, and on two runs out of three it was negative.

So this is not a rare coincidence of two unrelated timings. **The masterless
window and the retry budget are the same size by construction**, and which side
of the line a run lands on is noise.

### What the rate does and does not say

3 failures in ~960 gate runs that reached the drills (988 runs; 25 failed before
the drills; 860 passed). That is **the rate at which this DRILL's first write
lands in the bad window** — not customer exposure. The product statement is
stronger and needs no rate at all:

> A client write in flight when a master dies returns an error whenever the
> masterless window exceeds 5 s, and the measured window is 4.3–4.6 s before
> detection is counted.

`docs/slo.md`'s *"zero client-visible errors"* through failover is therefore
false for a write in flight at the moment of failure, and 0.3 % is how often
this particular harness happens to have one.

### Not the LOADING gap

`-LOADING` is forwarded to clients by the proxy, which recognises only MOVED,
TRYAGAIN and READONLY — a real defect, filed separately. It is **not** this one:
both captured replies say `no reachable master for this slot`, which is the
proxy's own string, generated when it had no target at all.

## The decision this leaves

The invariant is one line, and nothing checks it today:

    RETRY_BUDGET  >  the worst-case masterless window

Three ways to make it true, and picking one is Jeff's call because each spends
something different:

1. **Raise `RETRY_BUDGET`.** Cheapest, and it makes a client wait longer before
   being told the truth — the budget is also the time a genuinely dead cluster
   takes to say so.
2. **Shorten the window.** The 1.8 s re-confirm streak is the largest single
   term and it is a deliberate safety property (ADR-0015); shortening it trades
   a client error for a promotion-on-a-blip risk.
3. **State the SLO as it is.** "A write in flight at master loss may error" is
   a different product claim, and a weaker one.

Whichever moves, the margin should be MEASURED every run rather than discovered
once in 320 — see the drill change landing beside this.
