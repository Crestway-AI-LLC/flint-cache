# BUG-0263: a client_compat dependency fetch discards the reason it failed, and a skip reddened a gate with only a guess as evidence

**Status:** **PARTLY FIXED 2026-10-16** — the asynq site now keeps its reason.
The other eleven fetch sites still discard theirs.
**Severity:** low in the product, real in a verdict — it cost the reading of
one EC2 gate and would cost the next.
**Area:** `tools/client_compat_drill.sh`.

## What happened

The public `drills` gate of `83d4a2fe` (2026-10-16, box
`i-01d58e133ec935dc0`) came back `GATES FAILED: client_compat(skipped)` after
157 steps. The drill's own last line was

    PASS: client compatibility — redis-py, rq, celery, limits, python-redis-lock,
    node-redis, ioredis, node redlock, rate-limit-redis, rate-limiter-flexible,
    bullmq, go-redis, redsync, redis_rate, rails-cache-store, redlock-rb,
    sidekiq, sidekiq server

Eighteen clients passed. One was skipped, and the whole evidence for why was:

    == asynq: SKIP (could not fetch the module; offline?)

**With a question mark.** The fetch ran under `>/dev/null 2>&1`, so the run
held no way to tell offline from a wrong version from a blocked module proxy
from a timeout. A gate verdict rests on it, and `gates.sh` treats a skipped
client as a failure deliberately — correctly, because a silent skip deletes
coverage from a run someone is about to trust.

## What the cause turned out to be, and how much of it had to be inferred

None of it came from the drill's output:

- `asynq@v0.26.0` fetches cleanly from the laptop, pulling four deps — so the
  module and the pin are fine.
- `go-redis`, `redsync` and `redis_rate` all RAN in the same box run, so Go
  worked there and those three modules were available without a fetch.
- asynq entered the drill around 2026-10-09 (rc.82's pub-sub work). The baked
  gate AMI predates it, so it is the one Go module not in the AMI's cache, and
  the box could not fetch it.

Every step of that is reconstruction from *which clients survived*. A reader
should not have to do it, and a reader without the laptop and the release
history cannot.

## Fixed, and not

**Fixed:** the asynq fetch captures its output and the skip prints the last six
lines of it. The behaviour is unchanged — it still skips, and the gate still
reds — but the next run says why instead of guessing.

**Not fixed: eleven more sites with the same shape** — five `npm install` and
six `go get`/`go mod tidy`, all `>/dev/null 2>&1`
(`client_compat_drill.sh:1163, 1374, 1377, 1503, 1554, 1668, 1754-1755,
1825-1826`). They did not bite this run because their packages were cached.
They will bite the first time a new client is added and the AMI predates it,
which is exactly what happened here.

**Also not fixed, and it needs a decision rather than a patch:** whether the
gate box should be able to fetch a module at all. Either the AMI carries every
client's dependencies — which means a re-bake is a prerequisite for adding a
client, and nothing says so today — or the launcher ships them, or the box gets
egress. The cheap diagnostic above is what makes that choice an informed one;
guessing between them was the alternative, and this write-up is what that
guessing looks like.
