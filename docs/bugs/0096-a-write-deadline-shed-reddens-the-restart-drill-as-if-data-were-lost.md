# BUG-0096: a write-deadline shed reddens the restart drill as if data were lost (FIXED); and `verify_after` raced the convergence it asserts (FIXED)

Status: **FIXED 2026-09-05** · found by triaging the gate failures BUG-0064
records as going unread · Severity: medium — it cost three gate runs in one
day and its failure text points at durability, which is the wrong half of the
system.

## What was happening

Three of the last 98 `gate` runs on `main` failed in `restart`, all on
2026-09-04, with the same shape:

    FAIL  restart                (0.7s)
            THROTTLED write would wait ~2011ms (inflight 65 x service 30942us),
              past --write-deadline-ms 2000, retry with backoff
            errors: 1, replies: 101000

**One refused write in 101,000.** The other two runs read
`~2004ms (inflight 44 x service 45551us)` and
`~2011ms (inflight 30 x service 67038us)`.

## The three samples say what this is, without needing a reproduction

| run | inflight | service | estimate |
|---|---|---|---|
| `33930982465` | 65 | 30 942 µs | 2011 ms |
| `33914192952` | 44 | 45 551 µs | 2004 ms |
| `33899834240` | 30 | 67 038 µs | 2011 ms |

The factors move by more than 2x and **the product is pinned at the 2000 ms
deadline every time**. That is the estimator crossing its own line as the
runner slows, not a fault with a fixed size. Corroborated from the other end:
the estimator has not changed behaviour since `a88ac47d` (#186, 2026-08-15);
the only later commit touching it, `b439cf0f`, records a high-water mark.

So this is [BUG-0035](0035-the-default-lag-cap-sheds-under-gate-load.md)'s shape
in a different subsystem — a shed working exactly as designed, reddening a
drill that is not testing it.

## Why the drill failed on it, and why that is wrong

`-THROTTLED` is a **refusal, not a loss**: the message says "retry with
backoff" and the write was never acked. The loader is `valkey-cli --pipe`, a
bulk loader with no retry, so one refusal ends the run with `set -o pipefail`.

And the drill's subject is whether data survives `kill -9`. It verifies exactly
two keys, `key:0050000` and `hash:0777` — there is no count assertion — so a
write refused elsewhere cannot affect what it claims. Failing the whole drill
on it tests the deadline estimator by accident, on a shared runner.

## The fix

Sheds are **tolerated, bounded, and counted** — never silently:

    == tolerated 697 retryable shed(s) of 101000 writes (ceiling 50): THROTTLED …

- **Counted**, because a rate is the only thing separating a slow runner from a
  real fault (ADR-0018 D6's rule, applied to a drill).
- **Bounded** at 0.05% of the load. One in 101,000 is weather; 711 is the
  estimator firing systematically, and the refusal says so in those terms —
  "look at `--write-deadline-ms` and `write_service_us`, not at durability" —
  so the next reader is not sent hunting the wrong half.
- **Every error line must be a shed.** Checked as a POSITIVE rule: subtract the
  loader's own notices and the sheds, and require nothing to be left, so an
  unrecognised line fails rather than passes.
- **The load is proved to have run.** `|| true` swallows the loader's exit
  code, so without an explicit check a load that never connected would read as
  zero errors. The reply count must be present AND equal `KEYS + 1000`.

### The enumerate-the-errors version was wrong twice, and a control found it

The first cut listed error forms — `grep -c -iE '^-?ERR|WRONGTYPE|MISCONF'`.
It cannot know every error the server can emit, and worse, case-insensitively
`^-?ERR` matches the loader's own summary line `errors: 222, replies: 20000`.
So the check counted the error COUNT as an error and failed with "1
non-retryable error(s)" on a run whose only faults were sheds. The self-match
is the same trap `assert_license_headers_are_this_repos` records — a check
whose subject includes its own output.

## And the hash sample had no before-guard

Found while reading the same twenty lines. The string sample is verified as
written **before** the kill, because `key:0042000` was once verified without
ever having been written and the file records that "the empty parenthesis was
the only thing separating a reported durability failure from a drill that could
not answer".

`hash:0777` three lines later never got that guard. An HSET that was shed — or
simply never issued — still produced `FAIL: hash lost ()` after the restart: the
same false durability failure, in the same file, for the same reason. It is
guarded now.

## Controls

Six, each run and each caught with its own message:

| mutation | result |
|---|---|
| `--write-deadline-ms 1` (heavy shedding) | `711 writes shed of 101000, past the 50 this drill tolerates` |
| same, with the ceiling raised | `tolerated 697 retryable shed(s)`, drill PASSES |
| an unrecognised line among the errors | `1 line(s) that are neither a THROTTLED shed nor a loader notice` |
| loader output emptied | `printed no reply count -- it did not run` |
| reply count wrong | `replied 5 times, expected 101000` |
| `hash:0777` written with a wrong value | caught **before** the kill, not as `hash lost ()` |

## What this does NOT do

It does not widen a budget to quiet CI, which
[BUG-0064](0064-cold-start-roles-cannot-say-whether-the-replica-was-loading-or-absent.md)
warns against: the deadline itself is untouched, a real durability failure
still fails, and a shed rate that stops being rare still fails.

## Found by

BUG-0064's own note that the gate is red ~9-15% of runs and "nobody is
triaging them — they were simply never opened". Opening them: 8 failures in 98
completed runs (8.2%), of which `restart` x3 were this, `decommission` was
BUG-0064 (predating its 2026-09-04 fix), `tenant_rebalance` was BUG-0094, and
`failover` x2 and `backup_seat` remain unexamined.

## Second defect, found by triaging the same population: `verify_after` races

Gate run `33977289678` failed on a commit whose only change was Markdown, so
the cause was not the commit:

    pair 0  127.0.0.1:7701  master   epoch (0,1)   live_replicas 0
    pair 0  127.0.0.1:7702  loading  epoch         (blank)
    verify: bootstrap left the cluster INCONSISTENT:
      - pair 0 replicating: SINGLE-COPY: every member up, but 0 of 1 streaming

**The replica was still loading when bootstrap's own verify ran.** `launch`
waits for PONG, and since #176 PONG means ALIVE, not SERVING — a node binds and
answers from inside its load. `verify` then correctly calls the pair
single-copy, and `flintctl bootstrap` exits 1 on a healthy cluster.

That is BUG-0064's conflation and `f9c194d`'s race one level down. `f9c194d`
fixed it in `decommission_drill.sh`, and `36435a8` audited **"all 18 drills
that call `verify`"** — but `bootstrap` calls `verify_after` at
`flint-ctl/src/main.rs:6716`, *inside flintctl*, where a drill-level audit
could not reach. So did `expand`, `swap-node`, `add-replica`, `migrate-slots`,
`failover` and `decommission-node`: seven one-shot checks of a CONVERGENCE
property, each run the instant the operation that starts convergence returns.

**It is not only a CI flake.** `bootstrap` is the first command an operator
runs, and on a slow enough machine it fails on a cluster that is fine.

### The fix

`verify_after` now retries for 30 s, in that fix's own words — "retrying
`verify` itself is the right wait because verify IS the assertion, no separate
readiness signal to drift out of agreement with it". Success returns on the
first clean pass, so a converged cluster pays nothing, and a cluster that never
converges still fails.

**It says when it waited.** "left the cluster consistent (after waiting for it
to converge)" rather than the bare line, because a convergence that took time
is not the same event as one already done, and collapsing them hides a cluster
getting slower to converge until the day it stops.

### Controls

| injected | result |
|---|---|
| 3 transient problems | `left the cluster consistent (after waiting for it to converge)`, exit 0 |
| none | `left the cluster consistent`, no wait claimed |
| a problem that never clears | `still INCONSISTENT after 30s of waiting to converge`, exit 1, in 35 s wall clock |

The middle row matters as much as the first: without it, a change that always
claimed to have waited would pass the other two.

## 2026-09-05, later — the fix was a SECOND copy of a rule fleet.sh already had

Triaging the rest of the failure population found two more `failover` runs with
the same signature — `~2002ms (inflight 244 x service 8206us)`, 2 errors of
20,000, and `~2033ms`, 1 error of 20,000. `244 x 8.206ms = 2002ms`: the
estimator sitting exactly on its own line again.

So this was never restart-only. Classifying every drill that loads through
`valkey-cli --pipe`: **26 use it, and exactly 3 are exposed** — the ones with
`pipefail` and no guard on the pipe's exit status: `restart`, `failover`,
`repl`.

**And `repl` was already fixed, years of lessons ago.** Its comment says it
outright:

> Loaded through `fleet_load_resp`, which replays whatever the master sheds.
> The previous form piped once under `set -euo pipefail`; `valkey-cli --pipe`
> exits non-zero when it counts errors, so a single `-THROTTLED` aborted the
> run HERE.

`fleet_load_resp` has been in `tools/lib/fleet.sh` since BUG-0035, doing this
job for the lag cap. A write-deadline shed emits the same `-THROTTLED`, so the
helper covered this case the whole time. **The first fix above open-coded it
into `restart_drill.sh` instead** — a second implementation of one rule, in a
suite whose recurring cost is exactly that.

### What was done about it

The open-coded version was not simply deleted: it had three things the helper
lacked, and those were folded IN, so there is now one implementation strictly
better than either.

| moved into `fleet_load_resp` | why |
|---|---|
| an expected reply count | a short load is not a clean one, and `\|\| true` hides the loader's exit code |
| the positive rule that every error line is a shed | an unrecognised line must fail rather than pass |
| an optional shed ceiling | a rate that stops being small is a finding |

**The ceiling had to become per-caller, and that is the interesting part.**
`repl_drill` drives the lag cap deliberately and has seen 20,328 sheds of
50,500 — 40% is the condition under test there, not a fault. A single global
ceiling would have broken the one drill that already had this right. So
`expected_replies` and `max_shed` default to unchecked; `restart` and
`failover` pass 0.05%, `repl` and `controller` pass neither.

`restart_drill` and `failover_drill` now call the helper and carry none of the
rules themselves.

### Controls, re-run against the consolidated helper

| control | result |
|---|---|
| `restart` with `--write-deadline-ms 1` | `668 writes shed of 101000, past the 50 this drill tolerates` |
| `failover` with `--write-deadline-ms 1` | `130 writes shed of 20000, past the 11` |
| **`repl` with `--write-deadline-ms 1`** | **349 of 50500 shed, noted, drill PASSES** — the ceiling is per-caller and did not break the drill whose subject is shedding |
| load against a port with nothing listening | `delivered nothing at all — this is NOT shedding; it is a dead or unreachable seat` |
| a non-shed line injected into the loader output | `1 line(s) that are neither a THROTTLED shed nor a loader notice` |

The third row is the one that would have been missed by testing only the
drills that were failing.

## The whole failure population, now accounted for

Eight gate failures in 98 completed runs on `main` (8.2%), every one explained:

| drill | runs | cause |
|---|---|---|
| `restart` | 3 | write-deadline shed read as a loss (this bug) |
| `failover` | 2 | the same |
| `backup_seat` | 1 | `verify_after` racing convergence (this bug, second defect) |
| `ctl_error` | 1 (later) | the same |
| `decommission` | 1 | BUG-0064, fixed 2026-09-04 |
| `tenant_rebalance` | 1 | BUG-0094, fixed |

Two live defects behind five sixths of a red rate that BUG-0064 recorded as
"nobody is triaging them — they were simply never opened".
