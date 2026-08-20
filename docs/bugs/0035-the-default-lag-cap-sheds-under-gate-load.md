# BUG-0035: the default lag cap sheds under gate load, and `repl` reports it as a replication failure (OPEN)

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
| 2 | 6 CPU burners on 8 cores | not measured | 0 | PASS |
| 3 | `restart_drill` then `repl_drill` back to back, as the gate runs them | 9.6 / 9.2 / 20.2% | 0 | PASS |
| 4 | two `dd conv=fsync` loops on the same filesystem | 2.4 / 6.1 / 14.2% | 0 | PASS |

Attempt 4 pegged both CPU and disk and still did not reach a 1000 ms cap.
Attempt 2 is the weakest — the burners' effect during the drill window was not
measured, only their creation — and counts as inconclusive rather than as a
negative.

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
not a fresh binary (build.log). The one variable present during gate 21 and
absent from all four attempts is ANOTHER SESSION'S WORKLOAD: a `flint-kv`
`cold-modify` process was observed on the box two minutes into the gate and
was gone for every attempt afterwards. That is not a controlled input and this
is not a claim that it was the cause — it is the honest remaining difference.
There is precedent: gate 17's ten bootstrap failures were concurrent peer load
and nothing else, confirmed only when the peer independently reported their
own bootstraps failing in the same shape at the same time.

If that is the trigger, the finding stands and strengthens: a busy host is an
ordinary production condition, and it reached a cap two documents describe as
unreachable.

## Why this is not being fixed by raising the drill's caps yet

`--lag-soft-ms` and `--lag-hard-ms` are read by `flint-server`
(`main.rs:1211,1214`), so `repl_drill` could pass caps high enough that a
50k-key firehose never sheds, and its FAIL would then mean what it claims.
That is probably the right fix for half two.

It is deliberately not applied in the same change that discovered half one.
Raising the cap makes the phenomenon unobservable, and the phenomenon is
currently a single observation contradicting two written claims. The order is:
reproduce it, or fail to reproduce it a stated number of times, THEN make the
drill deterministic. Fixing it first would leave the contradiction in
`slo.md` with nothing left that could ever surface it again.

## Note

`ReplHub::new` stores `lag_hard_ms.max(lag_soft_ms)`, so any fix must move
both caps together — passing only `--lag-hard-ms` leaves the 500 ms default
soft cap in force. `lag_cap_drill.sh`'s header records this trap already.
