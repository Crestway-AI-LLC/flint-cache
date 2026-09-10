# BUG-0129: a promotion records its outcome and not its trigger

**Status:** FIXED 2026-09-10. Found 2026-09-09 from a production failover on
the Flint playground; the evidence trail is ops
[OPS-0181](../../flint-cache/docs/bugs).
**Severity:** medium. No outage — the fleet recovered in about forty seconds on
its own. The cost is that the same thing can happen again and be equally
unexplainable, and that a *spurious* promotion is indistinguishable from a
necessary one in every artifact that survives.

The controller promotes on `no_master_streak >= confirm`. The playground runs
`--poll-ms 100 --confirm 3`, so a promotion turns on **three specific reads,
about 300 ms apart**. What it logged was:

    PROMOTED 172.31.64.94:7001 at (0,65)

Which node was polled, what came back, and why: none of it survived.

## Why nothing else could stand in

Everything recoverable after the fact records a **decision** or an **effect**,
never the **observation** that caused it — the controller log's own
`PROMOTED`, the node's `demoted to replica at role epoch (0,66)`, the agent's
`RECOMMEND promote`, and Prometheus showing `flint_node_up = 1` for *both*
nodes across the window.

The ops lane's finest-grained view of a fleet is a Prometheus scrape, which is
tens of seconds wide. **A 300 ms event is invisible to it by construction**,
and scraping fast enough to see one would be absurd. The controller is the only
possible witness, and it did not testify.

The style already existed one line down: the re-point failure logs its cause,
`(Connection reset by peer (os error 104))`. The promotion decision simply
carried none.

## Where the reason was being thrown away

`observe()` bound its probe with `let Ok(Value::Bulk(Some(raw))) = call(…) else`,
which **discards the `io::Error`** and keeps only the booleans derived from it.
A refused connect, a reset mid-reply and an 800 ms read timeout are three
different faults with three different causes, and all three arrived at the
promote decision as `reachable = false`.

It is now a `match` that keeps the cause. Control flow is unchanged: anything
that is not a non-null bulk reply takes exactly the path it always did.

## The fix

`Node` gains `why` — what *this* poll saw, when it saw nothing good:
`FLINTINFO <error>; PING <ok|no>; socket <open|closed>`. Empty on a healthy
poll, and that emptiness carries meaning: a pair with no master where every
member answered is itself the finding.

The pair keeps `no_master_ticks`, filled **only** on the no-master path, so a
healthy fleet allocates nothing. When the streak trips, one line goes out
*before* the promotion:

    [ctl][g0] no master for 3/3 ticks (t1: 127.0.0.1:7002 FLINTINFO Connection
    refused (os error 111); PING no; socket closed, …) — promoting 127.0.0.1:7001

Three decisions in that, each with a reason:

- **Before the outcome, not beside it.** A promotion that *fails* needs its
  evidence at least as much as one that works, and the arms below it return
  early on several paths.
- **Capped at 12 ticks, dropping the second.** The alive-but-slow path holds
  for `slow_promote` of wall-clock and would otherwise grow a string per tick
  for minutes. The **first** tick is kept whatever happens: it is where the
  fault began, and discarding it to make room for the hundredth slow-path tick
  would drop the only one that says what started this.
- **Cost.** Three strings on a path that runs only when a promotion is already
  happening. The RTO-critical work is the `FLINTPROMOTE` call underneath it.

## Verification

Two unit tests and one live assertion, because they cover different halves and
neither is sufficient alone.

`an_unreachable_node_records_which_failure_it_was` observes port 1 — refused
instantly on every platform this runs on, so it exercises the refused-connect
arm at no wall-clock cost — and requires the reason to name the failed call
*and* carry the PING and socket outcomes the promote decision is actually made
on.

`a_healthy_poll_records_no_reason` is its positive control. Without it, a `why`
that was populated unconditionally would satisfy the first assertion while
destroying the distinction the log line depends on.

Neither covers whether the string reaches the log. `tools/controller_drill.sh`
does: it kills a real master, waits for a real controller to promote, and now
requires the evidence line to exist **and to name an observation for the killed
port**. A line reading `no master for 3/3 ticks ()` would reproduce this bug
with extra characters, and a check that only asserted the line's presence would
pass it.

## What this does not explain

**Why a healthy master became unreachable** on 2026-09-09. 7002 was serving and
writing snapshots throughout; something made three consecutive polls fail. This
change does not answer that — it makes the *next* one answerable. A
`refresh-policy-routes@ens5` run landed three seconds before the promotion, but
that unit fires roughly every 110 seconds against five promotions in the
controller's entire log, so the coincidence is recorded only so nobody has to
notice it again.

## Related

- ops OPS-0181 — the evidence trail, and the ask this implements
- ops OPS-0180 — the two spool cases from the same failover, both judged `contradiction`
- ops OPS-0037 — "could not measure" must never be recorded as "measured nothing"; an unexplained promotion is that rule broken on the RTO path
