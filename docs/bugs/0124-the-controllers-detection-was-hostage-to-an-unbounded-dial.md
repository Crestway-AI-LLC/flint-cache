# BUG-0124 — the controller's detection was hostage to an unbounded dial

Status: FIXED 2026-09-08 — every controller dial now takes `CONNECT_BUDGET`
(800 ms) instead of flint-tls's 3 s backstop · Severity: medium — detection
latency is the front half of RTO, and its worst case was 19.8 s, which
overruns even the 10 s exit
Found: 2026-09-08, characterising the residual RTO tail after BUG-0122
Component: `flint-controller` — `internal_connect`, and the `observe()` path
built on it

## What happened

After BUG-0122 removed the harness's dial from the measured RTO, one kill in
391 still breached, and it looked nothing like the previous mode:
`max_connect_ms=0`, kill dispatch 3533 ms against a p50 of 723 ms, client stall
3155 ms.

Joining all 391 master kills to the retained journal:

- detection latency from `kill_ms`: **p50 567 ms, p95 641 ms, worst 3452 ms**
- **Pearson r(detection latency, kill dispatch) = 0.984**, n = 391

The five slowest detections are the five slowest dispatches, in the same order.

## The cause

`observe()` asks a node three questions when it does not answer — FLINTINFO,
then PING, then `socket_open` — and the first two go through `call()`:

```rust
let mut stream = internal_connect(addr)?;              // unbounded: 3 s backstop
stream.set_read_timeout(Some(Duration::from_millis(800)))?;
```

**The reply was bounded at 800 ms and the dial was not.** `internal_connect`
fell through to `flint_tls::CONNECT_BACKSTOP`, which is a ceiling on a
blackholed peer, not a latency budget. So one observation of a node whose SYNs
go unanswered costs `3000 + 3000 + 500` = **6.5 s**, and the promote decision
needs `confirm` of them:

| dial budget | worst `observe()` | worst detection at `confirm 3`, `poll 100ms` |
|---|---|---|
| 3 s backstop (before) | 6500 ms | **19 800 ms** |
| `CONNECT_BUDGET` 800 ms (after) | 2100 ms | **6600 ms** |

**19.8 s overruns the 10 s exit, not merely the 3 s one it replaced.** A
blackholed host could breach the budget whichever number was written down,
which is what makes this worth fixing independently of where the exit sits.

That is also the r = 0.984: a stalled host slows the SSH hop carrying the kill
AND the controller's probes to the same host, so detection and dispatch move
together because both are downstream of one event.

## Fix

`internal_connect` dials through `connect_reloadable_within` with
`CONNECT_BUDGET = 800 ms` — matching the read timeout `call` already sets, and
deliberately **looser** than the 500 ms this same file gives `socket_open` to
decide socket-liveness. A dial budget longer than the liveness threshold the
controller judges by was the inconsistency; the new one cannot be too tight
unless `socket_open` was too tight first.

Both `call` and `call_slow` route through `internal_connect`, so one edit
bounds every controller dial. The arithmetic above is a test, not a comment:
worst-case detection must stay inside the exit budget, and the test also
asserts the backstop still overruns it, so it cannot quietly stop demonstrating
why the constant exists.

This is the third instance of one defect — bounded reply, unbounded dial —
after BUG-0122 (the chaos client) and BUG-0123 (`flint-proxy::discover_master`).

## A wrong write-up, kept because the mistake is the point

**This bug was first filed as "the journal records that the controller
promoted, never why"** — asserting that `Detected` carried no way to tell which
promotion rule fired, and that the alive-but-slow path (held for
`--slow-promote-ms`, default 4000 ms) explained the breach.

Both halves were wrong, and one piece of evidence refutes both. Every one of the
391 `Detected` events, the breaching one included, carries:

    cause = "master unreachable, confirmed across required ticks"

That is the REFUSED path, stated outright, in a field the journal has always
emitted. The slow path never fired. **The analysis script printed `detail` and
never `cause`**, so a correlation and a comment in the source were used to infer
something the data already answered.

The lesson is not the one from BUG-0122 — there the instrument genuinely could
not see the phase. Here it could, and did, and was not read. Before concluding
that evidence is missing, enumerate the fields actually present: `grep -o
'"[a-z_]*":' | sort -u` over the artifact would have ended this in one command.

The consequence reached a decision, which is why it is recorded rather than
quietly corrected: M2's exit was moved from 3 s to 10 s partly on the argument
that `slow_promote` made 3 s unreachable. That argument was unfounded. The exit
remains at 10 s on its own merits — `slo.md` publishes 10 s and the chaos drill
has always judged against it — and the fix above is being made anyway, because
19.8 s overran that too.
