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
