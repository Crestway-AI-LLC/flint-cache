# BUG-0035: the default lag cap sheds under gate load, and two drills misreport it (drills FIXED; the shed itself OPEN)

Status: OPEN 2026-08-20 · Severity: medium — one half is a documented claim
with a counter-example, the other is a drill that reports a verdict for
assertions it never reached

## Symptom

Gate 21 on `flintmigrations-all` at `5e3d81e`: 116 PASS, 1 FAIL.

    FAIL  repl                   (7s)  /tmp/flint-gates/20260820T010414Z-5e3d81e/drill-repl.log
            errors: 20328, replies: 50500
            tools/repl_drill.sh: line 22: 14941 Terminated: 15  "$BIN" --port "$MPORT" ...
            tools/repl_drill.sh: line 22: 14949 Terminated: 15  "$BIN" --port "$RPORT" ...

The drill log is 20328 copies of

    THROTTLED replication lag exceeds limit, retry with backoff

The master shed 40% of the load. No stall was induced, no cap was lowered:
this is the shipped default, `--lag-hard-ms 1000` / `--lag-soft-ms 500`
(`repl_hub.rs:20,37`).

**The commit cannot be the cause.** `7a98b18..5e3d81e` is two docs files and
three shell files (`gates.sh` plus `slot_map_drill.sh` and
`restore_ns_drill.sh`, neither of which `repl` runs). No Rust changed, so the
binary behaves identically to the six preceding gate runs — all of which
recorded `throttled=0, errors: 0` in this drill.

## Half one: a published claim now has a counter-example

`tools/lag_cap_drill.sh`'s header states the reason that drill has to lower
the cap at all:

> Loopback replication acks in ~0.2ms and cross-host was not much slower, so a
> 1000ms cap is simply unreachable under any load the harness generates.

`docs/slo.md`'s RPO table says the same thing as a measurement:

    | stall | lag cap | writes shed -THROTTLED | deepest acked-write loss |
    | none  | 1000 ms | 0                      | 0 ms                     |

That first row is exactly gate 21's configuration — no stall, 1000 ms cap —
and gate 21 shed 20328. The cap is reachable by the harness's own load on
loopback. Whether that is a defect or backpressure working correctly is a
separate question; what is not in doubt is that "unreachable" and "0" are
falsified, and both are load-bearing for how the RPO bound is explained.

## Half two: the drill reports a verdict it never computed

`repl_drill.sh` runs under `set -euo pipefail` and loads through

    { awk ... } | valkey-cli -p "$MPORT" --pipe | tail -1

`valkey-cli --pipe` exits non-zero when it counts errors, `pipefail` promotes
that to the pipeline's status, and `set -e` aborts the drill on the spot. The
EXIT trap then kills both seats, which is the two `Terminated: 15` lines.

So the run ended at the LOAD step. It never reached parity samples, the
full-sync assertion, FLINTINFO roles, idle liveness, READONLY, live tail, or
"replica serves reads with master dead" — the six things the drill exists to
check. `FAIL repl` is indistinguishable from a genuine replication failure and
means only that the master applied backpressure while the drill was writing.

This is the same shape as BUG-0029 and BUG-0030: a step that could not answer
producing output shaped like an answer.

## Reproduction: four attempts, all negative, load measured

Standalone at the same commit, on the same box, immediately after the gate:

| attempt | conditions | idle CPU during load | THROTTLED | result |
|---|---|---|---|---|
| 1 | idle box | — | 0 | PASS |
| 2 | 6 CPU burners on 8 cores | — | 0 | PASS |
| 3 | `restart_drill` then `repl_drill` back to back, as the gate runs them, burners still live | 9.6 / 9.2 / 20.2% | 0 | PASS |
| 4 | two `dd conv=fsync` loops on the same filesystem, burners still live | 2.4 / 6.1 / 14.2% | 0 | PASS |

**Attempts 3 and 4 ran under MORE load than intended**, which is worth stating
plainly because the reason is a mistake of the exact kind this file is about.
Attempt 2's six burners were never killed. The teardown ran

    kill $BURNERS ; echo "burners left: $(jobs -p | wc -l)"

and printed `burners left: 0`, which was read as confirmation. `jobs -p` in a
non-interactive shell had never tracked them, so `BURNERS` was empty, `kill`
received no arguments, and the count of zero meant "this shell knows of no
jobs" — output identical to "all six were killed". They span at ~99% CPU each
for 34 minutes and were found only when the box showed load 9.2 on 8 cores.

So attempts 3 and 4 were run with six spinners plus their own load — 2.4% idle
at the floor — and still did not reach a 1000 ms cap. The mistake made those
two negatives STRONGER, not weaker, and it makes attempt 2 a real negative
rather than the inconclusive one this file first called it.

Verify a kill by asking after the PIDs, not by asking the shell how many jobs
it remembers.

## A hypothesis, tested and dead

The first explanation was that a gate run execs FRESHLY BUILT binaries while
every standalone re-run gets the same binary already assessed and cached, and
that Gatekeeper's first-exec stall (69 min on this box before the Developer
Tools grant) was stalling the replica.

`build.log` from the failing run refutes it:

    Finished `release` profile [optimized] target(s) in 0.30s

Nothing was rebuilt. The commit changed no Rust, so gate 21 ran the SAME
binaries as the six clean runs before it, already assessed. There was no fresh
binary and no first exec. The hypothesis is dead, and it was killed by a log
that was already sitting in the failing run's own directory.

## What is actually left

Not CPU (attempts 2, 3), not disk (4), not the restart-then-repl sequence (3),
not a fresh binary (build.log).

A `flint-kv` `cold-modify` process from another session was on the box two
minutes into gate 21, and another was running an hour later. **That is not
offered as the differentiator**, because the box was not sampled for it during
each of the four attempts — so "present during the gate, absent from the
attempts" is a claim the evidence does not support, and writing it that way
was the first draft of this file. What can be said is narrower: another
session's workload comes and goes on this box unmeasured, and it is the
largest uncontrolled input remaining.

There is precedent for it mattering. Gate 17's ten bootstrap failures were
concurrent peer load and nothing else, and that was confirmed only when the
peer independently reported their own bootstraps failing in the same shape at
the same time — neither session could have concluded it alone.

The way to settle this is to sample the box during the drill rather than
reason about it afterwards: record load average and the non-flint process set
into the drill log at the moment the load phase starts. Then a shed run and a
clean run differ by a recorded fact instead of by a recollection.

If concurrent load IS the trigger, the finding stands and strengthens rather
than dissolving: a busy host is an ordinary production condition, and it
reached a cap two documents describe as unreachable under any load.

## Gate 22 is not more evidence — it is the same mistake, downstream

The re-run at `6fea7c2` came out 114 PASS / 3 FAIL: `edge_roll` (bootstrap),
`json` ("Could not connect to Valkey at 127.0.0.1:7681: Connection refused"),
and `chaos` (a BUG-0023 lost link). `repl` PASSED in that run.

**Those three are the burners, not the product.** The timeline is
unambiguous: the burners started ~18:31 local and were killed at 19:06; gate
22 ran 18:35-19:03, entirely inside that window, at load 9.2 on 8 cores. Both
new drill failures are startup-timing failures — a seat that did not begin
listening inside its wait budget — which is exactly what a saturated box
produces. None of them is filed as a defect.

Gate 21, the run this file is about, ended at ~18:20, ELEVEN MINUTES BEFORE
the first burner existed. Its `repl` failure is not explained by this and
still stands.

The lesson is about attribution, not about load: an intermittent failure
arriving right after a self-inflicted change to the environment is the easiest
kind of evidence to misread in both directions — as a product bug, or as
"just my load" when a real one is hiding underneath. The discriminator was a
timestamp, and it was available the whole time in the gate log directory name.

## The drills are fixed; the shed is not

Not by raising the caps. `-THROTTLED` means the write was NEVER ACKED, so a
key absent because of it is correctly absent, and the fix is for the drills to
say so rather than to stop the master saying it.

`tools/lib/fleet.sh` gained three helpers:

- `fleet_load_resp` pipes the load, PRINTS what was shed, and does not fail on
  shed alone. It does fail when the load delivered nothing at all, because
  "nothing was written" and "everything was refused" must not look alike.
- `fleet_retry_write` retries one write past `-THROTTLED`.
- `fleet_ensure_keys` repairs, one write at a time, exactly the keys a drill
  asserts on. Everything else the load shed stays absent on purpose.

**A wrong fix, measured, before the right one.** The first version replayed the
whole stream until nothing shed. Against a 5 ms cap the attempts shed 19388,
19469, 19667, 19413 and 19337 of 20000 and it gave up: the replay is itself a
firehose and recreates the lag that caused the shed. A retry whose load
profile equals the load that failed is not a retry. Single writes converge
because they let the replica drain between them.

## Verification, both directions, both drills

| drill | condition | shed | result |
|---|---|---|---|
| `repl` | ordinary load | 0 of 50500 | PASS |
| `repl` | master forced to `--lag-hard-ms 5` | **49229 of 50500** | PASS, every assertion reached |
| `controller` | ordinary load | 2428 of 20000 | PASS |

The `controller` row is the one that matters: 2428 writes shed on a quiet box
in an ordinary standalone run, and the OLD drill would have printed
`FAIL: tail lost` for it. That is this product's most serious claim —
acked-write loss across a failover — being asserted from evidence that the
master had openly refused the write. The positive control was not induced; it
arrived on its own, which is also a third independent sighting of the shed.

## What is still open

The drills no longer lie about it, and that is all that changed. The shipped
1000 ms cap is still being reached by ordinary load on loopback, which
`lag_cap_drill.sh`'s header and `slo.md`'s no-stall row both say cannot
happen. Sightings so far: gate 21 `repl` (20328), gate 23 `repl` (19932),
gate 23 `controller` (356), and a standalone `controller` (2428).

`slo.md`'s table needs correcting once the trigger is understood — not before,
because "0 shed with no stall" is currently the only written statement that
this contradicts, and replacing it with a vaguer sentence would lose the
contradiction rather than resolve it.

## Note

`ReplHub::new` stores `lag_hard_ms.max(lag_soft_ms)`, so any fix must move
both caps together — passing only `--lag-hard-ms` leaves the 500 ms default
soft cap in force. `lag_cap_drill.sh`'s header records this trap already.
