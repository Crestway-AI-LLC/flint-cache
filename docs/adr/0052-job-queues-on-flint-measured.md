# ADR-0052: Job queues on Flint, measured

Status: **PROPOSED 2026-09-26, for Jeff's decision.** Nothing in it is built.
The three plain commands the measurement found missing (`LMOVE`, `RPOPLPUSH`,
`HINCRBYFLOAT`) were ordinary gaps and are fixed as BUG-0187.

## Context

ADR-0050 left the queue libraries out: "a different product question, and
not this record's". This record measures them and asks the question.

**Measured 2026-09-26** on a gate box, each library on its defaults, first
against a plain Valkey with `MONITOR` recording every command it sent (script
calls included), then through the proxy on a two-pair fleet at `f2f9fb6`:

| library | on Valkey | through Flint | what stopped it |
|---|---|---|---|
| Sidekiq 8.1.7 (Ruby) | push, fetch | push works; **fetch fails** | `ERR unknown command 'BRPOP'` |
| Celery 5.6.3, Redis broker and result backend (Python) | task result 5 | **the worker fails to start** | it sent `BRPOP` to fetch, and `SUBSCRIBE`, `PSUBSCRIBE` and `PUBLISH` |
| rq 2.8.0 (Python) | enqueue, burst worker, result | **enqueue fails** | `KeyError: 'redis_version'`: it reads the version from `INFO`, which Flint does not report |
| BullMQ 6.3.9 (Node) | add, worker completes | **refuses to start** | "Redis version needs to be greater or equal than 5.0.0 Current: 0.0.1" |
| asynq 0.26 (Go) | enqueue, server processes 1 | enqueue works; **the server processes 0** | its dequeue script writes a key it builds rather than declares (below) |

For contrast, the same run passed Flask-Session 0.8.0, connect-redis 9.0.0
and @keyv/redis 5.1.6 through the proxy: sessions and caches are served.

What each queue sent on Valkey that Flint does not serve, beyond the blocker
in the table:

| library | also needs |
|---|---|
| Sidekiq | nothing else measured: `BRPOP` is its only gap |
| Celery | blocking pop and pub/sub, both above |
| rq | `SUBSCRIBE` (a worker's command channel); `XADD` and `XREVRANGE` (results are kept in a stream) |
| BullMQ | `BZPOPMIN`; `XADD` and `XTRIM` (its event stream); the `cjson` and `cmsgpack` Lua libraries; scripts naming several keys, which Flint serves in one slot, so BullMQ needs its documented cluster prefix (`{bull}`) |
| asynq | `SUBSCRIBE` (task cancellation). asynq retries a failed subscription forever and logs it, so without pub/sub it runs and cannot cancel a task |

`LMOVE`, `RPOPLPUSH` and `HINCRBYFLOAT` were in these lists too; BUG-0187
served them.

asynq's dequeue script is the one that meets ADR-0051's key rule:

    local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
    local key = ARGV[2] .. id
    redis.call("HSET", key, "state", "active")

The task's key is built from an argument and the popped id, so it is not in
`KEYS`, and ADR-0051 refuses any key a script did not declare. It is in the
declared keys' slot: every asynq key carries the queue's hash tag
(`asynq:{default}:...`). asynq supports Redis Cluster
(`RedisClusterClientOpt`), and its scripts run there because the node owns
that slot. A scan of asynq's source finds 26 scripts that build a key from
an argument: 7 on the processing path (dequeue, archive, forwarding, lease
recovery, aggregation and cleanup) and 19 in its inspector, which its CLI and
web UI use.

Why this is a Flint question and not only a compatibility one: a job queue is
the Redis workload where a lost acknowledged write costs the most, because it
is a lost job. Flint's premise is that an acknowledged write survives a
restart. The libraries above are what applications use to put jobs in Redis,
and today none of them runs end to end on Flint.

## Decisions

Six, independent. Each can be accepted alone; the staging at the end orders
them by what each unblocks.

### D1. Report `redis_version` in `INFO`

rq and BullMQ refuse to run without it, whatever else Flint serves. BUG-0176
decided on 2026-09-25 that it stays out, because a version is a claim about
the whole command surface.

**Measured:** Valkey 9.1.0 reports `redis_version:7.2.4` beside
`valkey_version:9.1.0`. It froze the field at the last Redis release it
forked from, so clients that gate features on it keep working. That is the
same problem Flint has, answered by the project with the most reason not to
claim to be Redis.

- **(a) Keep it absent.** rq and BullMQ stay refused.
- **(b) Report `redis_version:7.2.4`, as Valkey does,** beside
  `flint_version`. Clients then take their Redis 7 paths. The one measured so
  far is Rails' cache store sending `EXPIRE ... NX` (served since BUG-0185).
  Every library in this record is re-measured with the field present before
  anything else is built, because what a client does at 7.2.4 is what Flint
  must serve. `command-support.md` states what the field means: the version
  clients gate features on, not a promise of every Redis 7.2 command.

**Recommendation: (b).** The objection in BUG-0176 is right about what a
version claims. But the claim is already made by Valkey, and the cost of
refusing to make it is measured: two queue libraries cannot start.

### D2. Allow a script to write an undeclared key in its declared keys' slot

This amends ADR-0051. Its rule was Redis Cluster's rule stated strictly:
every key in `KEYS`. Redis Cluster itself enforces only the slot, and asynq
relies on that.

The rule is not only a check. It is also what the write lock relies on. A
script declaring one key holds that key's stripe; a second key written under
it would reintroduce BUG-0188 exactly: a writer of the undeclared key on
another stripe runs alongside the script.

- **(a) Keep refusing.** asynq cannot process jobs.
- **(b) Allow it, and re-run under the global lock.** `KeyGuard` already
  sees every row a call touches. On the first row of an undeclared key in
  the declared slot, the attempt is abandoned. Its writes are only in its
  buffer, so nothing has escaped. The script then runs again from the start,
  holding the lock that excludes every writer, with that slot allowed. A
  script that stays inside its `KEYS` pays nothing. One that does not pays
  one wasted attempt and a global lock. For asynq that is one dequeue that
  finds a job, not the empty polls. A key in another slot is still refused,
  and a script declaring no keys still may touch none, since it has no slot
  to be in.
- **(c) Stripe the write lock by slot rather than by key.** Every key of a
  slot then shares a stripe, so a script's stripe already covers any key in
  its slot, and every same-slot two-key write (BUG-0188) needs one stripe
  rather than every writer. The cost is that one hot hash tag serialises
  every write to it, as it does on Redis. This changes ADR-0027's measured
  batching and would need its own record and measurements.

**Recommendation: (b)** now. It is contained, it needs no change to the
lock, and its cost falls only on scripts that use the freedom. (c) is worth a
separate record if the global lock shows up in a profile.

### D3. The `cjson` and `cmsgpack` Lua libraries (and `bit`, `struct`)

Only BullMQ among the measured libraries uses them, and BullMQ also needs D1,
D4 and D6.

- **(a) Not served.**
- **(b) Vendor the C libraries Valkey ships.** Output is identical by
  construction. But these are C parsers of tenant input inside the seat, and
  Redis has patched memory-safety defects in them (CVE-2022-24834 in `cjson`,
  CVE-2024-31449 in `bit`).
- **(c) Implement them in Rust,** differential-tested against Valkey through
  the conformance oracle as ADR-0051's number formatting was. Memory-safe. The
  risk is an output difference, which the oracle is built to find.

**Recommendation: (c), when BullMQ's turn comes** (stage 4 below). Nothing
before BullMQ needs them.

### D4. Blocking pops

`BRPOP`, `BLPOP`, `BLMOVE`, `BRPOPLPUSH`, `BZPOPMIN`, `BZPOPMAX`. Sidekiq's
fetch is `BRPOP queue:default 2`. With several queues the keys carry no hash
tag, so they are in different slots, usually on different pairs. Redis
Cluster would refuse that call; Flint presents one standalone server, so it
must serve it.

- **(a) Not served.** Sidekiq and Celery cannot fetch.
- **(b) The proxy polls.** The proxy runs the non-blocking pop over the keys
  in order until one answers or the timeout passes, backing off between
  rounds and resetting on a hit. It needs no seat change, and it works across
  pairs: each pop is atomic on its own pair, which is all `BRPOP` promises
  per key. Its costs are:
  - latency up to one poll interval;
  - a blocked connection costs one pop per key per interval while idle;
  - no first-come-first-served order among blocked clients, which Redis
    gives.
- **(c) Seats wake waiters.** A seat keeps waiters per key and wakes one on
  a push, and the proxy long-polls each pair. Latency and idle cost fall to
  near zero, and per-key order is possible. But a waiter on keys on two pairs
  can be served by both, and the loser must push its element back, which
  reorders the list.

`BLMOVE` and `BRPOPLPUSH` stay same-slot, like `LMOVE`. Inside `MULTI` or a
script, a blocking pop runs as its non-blocking form, as in Redis.

**Recommendation: (b), measured,** with a latency distribution and the idle
cost per blocked connection. Move to (c) only if those numbers demand it.

### D5. Pub/sub

`SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB`.
Measured need: Celery (it does not start without it), rq (a worker's command
channel) and asynq (cancellation). Other frameworks are known to use pub/sub,
but none was measured here.

- **(a) Not served.**
- **(b) A broker per namespace at the seats.** A channel hashes to a pair as
  a key does. A proxy subscribes at that pair's master for its clients, and a
  `PUBLISH` goes to the channel's pair, which fans out to the subscribed
  proxies, which fan out to their clients.
  - A pattern subscription registers at every pair of the namespace.
  - Delivery is at-most-once, as in Redis. A failover drops the
    subscriptions, and proxies resubscribe; messages published in between
    are lost, as they are when a Redis client reconnects.
  - A tenant's channels are its own.
  - A subscribed connection is long-lived proxy state, like a blocked one.

**Recommendation: (b).** The seat is the one rendezvous every proxy already
reaches. A mesh between proxies would be a second one.

### D6. Streams

rq keeps results in a stream (`XADD`, `XREVRANGE`), and BullMQ its events
(`XADD`, `XTRIM`).

- **(a) Not served.**
- **(b) A stream type.** First `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`,
  `XTRIM`, `XDEL` and non-blocking `XREAD`. Then blocking `XREAD` on D4's
  mechanism. Consumer groups (`XGROUP`, `XREADGROUP`, `XACK`, `XPENDING`,
  `XCLAIM`, `XAUTOCLAIM`) are a second step, when measured demand asks for
  them. Streams are the Redis structure whose value depends most on
  durability, which is Flint's premise.

**Recommendation: (b), the first set,** after D4 and D5.

## The question first: does Flint serve job queues?

`command-support.md` says pub/sub, streams and blocking commands "conflict
with slot-sharded multi-tenancy or reintroduce the single-threaded
bottlenecks Flint exists to avoid". The designs above answer the first half.
Each is sharded like keys (D5, D6) or runs at the proxy without holding a
seat (D4b). None of them holds a seat thread while it waits. Accepting any of
D4 to D6 replaces that paragraph.

**Recommendation: yes.** A queue is where durability matters most, and the
libraries are few and measurable. Stage it, and re-run this record's probe
after each stage, because each fix moves a library to its next blocker.

1. **D1 + D2** (days). Expected, to be measured: asynq processes jobs,
   without cancellation; rq and BullMQ get past their version checks to
   their next blocker.
2. **D4, polling.** Expected: Sidekiq works; Celery can fetch.
3. **D5.** Expected: Celery works; rq's worker commands and asynq's
   cancellation work.
4. **D6 first set, then D3.** Expected: rq keeps results; BullMQ runs.

Each stage is its own gate and its own change to `command-support.md`. A
library joins `client_compat_drill` when it passes, so it cannot regress
silently.

If you would rather Flint not serve queues, accept D1 alone. It costs
nothing to build, and every other library that reads the version benefits.
Then document the queue libraries as unsupported, by name.

## Verification (if accepted)

- The probe that produced this record, re-run at each stage on a gate box:
  Valkey as the control, then through the proxy.
- Conformance cases against the Valkey oracle for each new command family.
  Blocking pops get timing-tolerant cases: a pop that waits, one that times
  out, and one served by a push from another connection.
- D2: a unit test that a script writing an undeclared key in its slot re-runs
  under the global lock. And BUG-0188's race run against a script (a script
  moving a hash into an undeclared key while another connection writes it),
  0 lost out of thousands, as for `RENAME`.
- Each library that passes joins `client_compat_drill`.
