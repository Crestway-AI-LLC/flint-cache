# ADR-0052: Job queues on Flint, measured

Status: **ACCEPTED 2026-09-27** (Jeff: "go with your recommendation on
ADR-0052"): Flint serves job queues, in the four stages below. Stages 1 (D1
and D2), 2 (D4) and 3 (D5) are built; see "As built" at the end. Stage 3,
pub/sub, was stopped on 2026-09-30 and stage 4 held on 2026-10-03; **Jeff
reopened both on 2026-10-08** ("Ok. You can start with bitmaps, Batch A and
then Pub/sub and streams."). Stage 3 is built; stage 4, streams (D6) then
`cjson` and `cmsgpack` (D3), is next. The three plain commands the
measurement found missing (`LMOVE`, `RPOPLPUSH`, `HINCRBYFLOAT`) were
ordinary gaps and are fixed as BUG-0187.

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

## As built

### Stage 1: D1 and D2 (2026-09-27)

- **D1** (`flint-proxy`): the proxy's `INFO` reports
  `redis_version:7.2.4` in its server section, beside `flint_version`.
- **D2** (`flint-server`, `script.rs` and `commands.rs`):
  - **The guard.** `KeyGuard` knows the declared keys' slot and whether its
    caller holds the lock over every writer. A row of a declared key is
    allowed, as before. A row of another key in that slot is allowed under
    the lock over every writer. Without that lock it is
    `Stray::NeedsEveryWriter`. A key in another slot, the keyspace, and any
    key of a script that declared none are refused, as before.
  - **Abandoning a script.** `NeedsEveryWriter` makes the command answer
    `Abandon` instead of a reply. The script stops where it stands, through
    the same path that makes the time limit uncatchable, so `pcall` cannot
    keep it going. Its buffer is dropped, and the dispatcher reports
    `wants_every_writer`. The Lua state stays reusable, because the stop is
    an ordinary Lua error.
  - **The re-run.** `execute` runs the script again from the start under
    `lock_all`. It drops the key's stripe first: `lock_all` waits for every
    reader of the global lock, this thread's included, so assigning the new
    guard over the old one would build the new one first and wait forever.
    A transaction already holds `lock_all`, so a script inside `EXEC` runs
    once.
- **The proxy's near-cache** (`cache.rs`): a script may now write keys the
  proxy never sees named, which would have broken read-your-own-writes
  through one proxy. So a script drops every cached entry of its tenant in
  its slot. The cost is O(1): each entry already carries a monotonic insert
  generation, the slot records the current one as its floor, and `get`
  treats an entry at or below its slot's floor as absent.
- **Found by this stage's probe: BUG-0189.** The seat's key-size cap read a
  script's text as its key, so any script over 4 KiB was refused. Through
  the proxy that included `EVALSHA`. BullMQ's scripts are over 4 KiB.
- **Verified.**
  - Unit tests: abandoning a script, caught or not, with nothing written;
    the same script run under the lock; another slot still refused;
    asynq's dequeue script verbatim.
  - A wire test counting `lock_all`: fails with the re-run's lock removed.
  - Conformance cases on the Valkey oracle: a built key, inside `pcall`,
    inside `MULTI`, asynq's dequeue on asynq's own keys, and a
    5,000-byte script.
  - The concurrency the lock exists for, measured on a release build (mem
    engine): a script renaming a 50-field hash onto an undeclared key while
    another connection wrote that key. With the re-run under the lock:
    0 of 6,000 races lost data. With the lock taken out and the key simply
    allowed: 118 and 111 of 3,000 lost all 50 fields.

**Re-measured** through the proxy on a gate box with the same probe as the
Context: first against this stage before BUG-0189 was fixed, then at
`460454c`, which fixes it, with BullMQ also run on its documented cluster
prefix:

| library | before | after stage 1 |
|---|---|---|
| asynq 0.26 | processes nothing | **works**: enqueues, and the server processes the job. Task cancellation still needs D5 |
| rq 2.8.0 | refused at enqueue (no `redis_version`) | past the version check; refused at enqueue by a **cross-slot transaction** |
| BullMQ 6.3.9 | refuses to start (no `redis_version`) | past the version check; failed `add` on BUG-0189, now fixed. After that, on its default prefix `bull`, `add` is refused with `CROSSSLOT` (`bull:probe:wait` and `bull:probe:paused` are in different slots). On `prefix: '{bull}'`, the setting BullMQ documents for Redis Cluster, it reaches its script and stops at `cmsgpack`: D3, stage 4 |
| Sidekiq 8.1.7 | cannot fetch (`BRPOP`) | unchanged: stage 2 |
| Celery 5.6.3 | worker fails to start | unchanged: stages 2 and 3 |

**rq needs something this record did not plan.** Its enqueue is one
`MULTI`/`EXEC` that writes the job's hash, the queue's list and the set of
queues. Those are three keys without a shared hash tag, so they are in three
slots. ADR-0012 serves transactions in one slot, because a transaction
across pairs would need a commit protocol between them. rq's expected stage
("D6 ... rq keeps results") is therefore not enough. Serving rq means
transactions across slots, at least across slots on one pair. That is a
decision of its own, to be brought back as its own record, measured, rather
than folded into this one.

### Stage 2: D4, blocking pops, by the proxy polling (2026-09-27)

- **At the seat** (`commands.rs`): `BLPOP`, `BRPOP`, `BZPOPMIN`,
  `BZPOPMAX`, `BLMOVE` and `BRPOPLPUSH`, never waiting. Each answers what is
  there now, as Redis does inside `MULTI` or a script, with Valkey's reply
  shapes and its errors for a bad timeout (checked on the raw wire, both
  protocols). The multi-key pops exclude every writer, as a two-key write
  does (BUG-0188).
- **At the proxy** (`blocking_pop`): a round is one attempt per key, in the
  caller's order, each on the pair that owns that key, so the order is a
  priority across pairs, which Sidekiq relies on. The first element wins.
  Between rounds the proxy waits, 1 ms and doubling to 20 ms; at the timeout
  it answers a null array.
  - **Watching the client.** Only the wait watches the client's socket
    (`ClientWatch`), never an attempt in flight, so a pop a seat has made is
    never abandoned half-answered. A client that disconnects while blocked
    takes nothing afterwards.
  - **Pipelined commands** behind a pop are kept and run after it, in order.
  - **Inside `MULTI`**, a pop is queued like any command and does not wait.
    EXEC's reply tells a null array from a null bulk per queued pop, which
    RESP3 alone cannot.
  - **Accounting.** A pop is charged once against quota, never staged by
    the prefetch pass, and kept out of the latency histogram.
- **Measured.**
  - An element pushed to a blocked `BRPOP` was taken 13 ms after the push at
    the median, and 24 ms at most, over 60 pushes (local debug build). In
    the gate box's drill, 13 and 15 ms.
  - An idle blocked client cost about 44 attempts a second per key at its
    seat, counted by a relay in front of the seat: 223 in 5 s on one key,
    660 in 5 s on three.
- **Found by this stage's probe: BUG-0190.** A transaction the proxy
  aborted mid-queue was forgotten, so the commands behind it applied outside
  any transaction. rq's enqueue surfaced it. It now stays open and doomed,
  as a Redis transaction does after a queue-time error.

**Re-measured** on a gate box after this stage:

| library | after stage 2 |
|---|---|
| Sidekiq 8.1.7 | **push and fetch work**: `client_compat_drill` checks strict queue order across pairs, a job pushed while the fetcher waits, and an empty fetch that waits its 2 s. `Sidekiq::Queue#clear` is refused: it is a `MULTI` over `queue:<name>` and the `queues` set, which are in two slots |
| asynq 0.26 | works, as after stage 1 |
| rq 2.8.0 | refused at enqueue by its transaction across slots, now whole: nothing applies (BUG-0190) |
| BullMQ 6.3.9 | on `{bull}`, stops at `cmsgpack` (stage 4), as after stage 1 |
| Celery 5.6.3 | its worker still fails to start, now on a transaction across slots: kombu reads the lengths of a queue's priority lists (`celery`, `celery3`, `celery6`, `celery9`) in one `MULTI`, which is refused as `CROSSSLOT` before any pub/sub is reached |

**Transactions across slots are the next blocker**, and the stages above do
not remove it. rq's enqueue, Celery's queue-length read and Sidekiq's
`Queue#clear` are each one `MULTI` over keys without a shared hash tag. The
libraries cannot be told to add one, and ADR-0012 serves a transaction in one
slot. That is a decision of its own, and comes to Jeff as its own record,
measured, before stages 3 and 4. Pub/sub (stage 3) would not by itself make
Celery work.

### Stage 3: D5, pub/sub, stopped (2026-09-30)

Not built. Jeff stopped it as out of scope: the roadmap lists pub/sub as out
of v0 scope, and the measured need is libraries rather than users. It serves
no stored data, so Flint's durability adds nothing to it, and it bills
nothing under per-GB pricing. Its one argument was removing a migration
blocker for a Python team whose single Redis also carries Celery, and no
such team has asked.

**What it means today:** Celery is not supported (its workers subscribe for
their control channel and its Redis result backend for results). rq's
workers run, without their command channel (shutdown, kill, stop-job), and
asynq runs without cancelling a running task. Those teams keep Celery's
broker on Redis or RabbitMQ.

**Measured before stopping**, against Valkey 9.1, for whoever reopens it:

| library | what it sends |
|---|---|
| Celery 5.6.3 | `PSUBSCRIBE /0.celery.pidbox` (a pattern with no wildcard) and `PUBLISH` to it for control; `SUBSCRIBE celery-task-meta-<id>` per awaited result; the result written as `MULTI`, `SETEX celery-task-meta-<id>`, `PUBLISH celery-task-meta-<id>`, `EXEC`: the key and the channel are one string, so one slot. With kombu's `global_keyprefix '{celery}'` every name carries the tag. All RESP2 |
| rq 2.8.0 | `SUBSCRIBE rq:pubsub:<worker>` per worker, `PUBLISH` to it for a command. RESP2 |
| asynq 0.26 | `SUBSCRIBE asynq:cancel` per server, `PUBLISH` to it to cancel. RESP3 |

The design this record chose held up against them: a channel hashed like a
key keeps Celery's result transaction on one pair. The build had reached a
seat-side broker and a RESP3 push frame; it is not in the repository.

### Stage 3: D5, pub/sub, built (2026-10-08)

**Where it departs from D5(b).** D5(b) put a channel on one pair, as a key.
As built, every pair's master holds every subscription of every proxy, and a
`PUBLISH` runs on one pair. Two things forced it:

- A `PUBLISH` inside a transaction runs on the transaction's pair, which its
  keys choose. Celery gives its result key and channel one name, so they
  share a slot, but nothing else promises that.
- A pattern names no pair, so D5(b) already registered every pattern at
  every pair.

So a subscription change goes to every master, one small frame each, and a
message goes from one. In the measured libraries a subscription changes no
more often than a message is sent: Celery subscribes once per awaited
result, which is published once.

**At the seat** (`flint-server`, `pubsub.rs`):

- **The broker.** One per process, by namespace. `FLINTSUBSCRIBER` turns a
  connection into a proxy's subscriber connection. On it
  `FLINTSUB`/`FLINTPSUB <ns> <name> <delta>` changes how many of that
  proxy's clients hold a channel or pattern, answered with the new count.
  Messages go out as `flintmessage` or `flintpmessage` frames, once per
  proxy.
- **Delivery is immediate.** The connection splits into a reader, its own
  thread, and a writer thread woken by a condition variable. That uses
  `flint-tls`'s duplex split (ADR-0020), extended to accepted streams. The
  parked build polled every 2 ms instead: 2.5 ms from publish to receive,
  where Valkey takes 0.12 ms. Now it is 0.05 ms through a local proxy.
- **A connection 32 MiB behind is cut off**, by shutting its socket from the
  publishing thread, so no lock a stuck writer holds is waited for.
- **A `PUBLISH` inside `MULTI` or a script is held** until the writes around
  it commit, and dropped if they do not.
- **`PUBSUB` answers from the broker**, for the whole namespace.

**At the proxy** (`flint-proxy`, `pubsub.rs`):

- **Links.** One link per master, dialed when a client first subscribes and
  closed 60 s after the last unsubscribes, so a client that subscribes per
  result does not dial every master each time.
- **Counts are reconciled, not relayed.** A link sends the difference
  between the count the proxy holds now and the count it last registered. A
  new connection starts from zero at the seat and registers everything,
  which is the whole of restart and failover.
- **Following the masters.** A supervisor checks the topology every 250 ms
  and moves a link to a new master. A link pings each second and is dialed
  again after 5 s of silence.
- **Confirmations.** A `SUBSCRIBE` is confirmed once every reachable master
  holds it, waiting at most 2 s.
- **Clients.** RESP2 clients get Redis's subscribe mode and RESP3 clients
  get pushes. A client 32 MiB behind is cut off, even while a write to it is
  stalled.

**Found while building: BUG-0244.** The scans' glob read a pattern with an
unterminated `[` unlike Redis. The pub/sub glob, judged against Valkey on
2.4 million random cases, is now the one matcher.

**Verified.**

- **Probe.** The same steps on Valkey 9.1 and through a two-pair Flint
  proxy, three connections, in both protocols: subscribe, pattern and
  publish replies, the RESP2 refusals, every `PUBSUB` form and error, a
  transaction, 51 channels at once, binary names and globs. The one
  difference is the order of a bare `UNSUBSCRIBE`'s confirmations (Valkey
  uses its hash table's).
- **Unit tests.** The seat broker's counts, delivery, overflow and held
  publishes; the glob; the proxy's confirmations, delivery, cut-off and
  RESP2 allow-list; the RESP3 push frame; an accepted TLS stream's duplex
  split.
- **Corpus.** A publisher's replies with no subscriber, on the Valkey
  reference, the seat and the proxy, in both protocols.
- **Drill.** `tools/pubsub_drill.sh` (CORE) runs two pairs and two proxies.
  - Sixteen channels over both pairs: each message reaches each holder once.
  - 200 channels each published to the instant its subscription was
    confirmed, through the other proxy.
  - A transaction publishing on its own pair's channel and on the other's,
    and an aborted one.
  - A script that publishes, and one that publishes and then fails.
  - A client 48 MiB behind is cut off; a closed client's subscriptions
    end.
  - A seat restarted under live subscribers, which were registered again
    0.20 s after it answered (the gate box's release build; 0.47 s for a
    local debug build).
- **Mutants**, each killed by the drill:
  - links not registering again on a new connection;
  - publishes not held in a script;
  - no cut-off while a write is stalled;
  - the seat's writer not woken;
  - a transaction's `PUBLISH` routed by its channel;
  - a confirmation sent before registration. This one passed the first
    drill, which is why the 200 instant publishes are in it.
- **Libraries**, through the proxy on a gate box, on the TLS fleet of
  `tools/client_compat_drill.sh`, which the gate runs:

| library | after stage 3 |
|---|---|
| Celery 5.6.3 | **works.** A task's result comes back, and the worker answers `ping` on its control channel. That holds with kombu's `global_keyprefix: '{celery}'` on a tenant across pairs, and unconfigured on a tenant placed on one pair (ADR-0053). Unconfigured on a tenant across pairs it still stops at a transaction across slots, as ADR-0053 measured |
| asynq 0.26 | **works, cancellation included.** The inspector's `CancelProcessing` cancels a running task's context |
| rq 2.8.0 | enqueue on a placed tenant, as before. Its worker still needs `LMOVE` across slots and streams (stage 4), so its command channel, though served, has no worker to reach yet |
| redis-py 7.0.1 (8.1.0 locally) | its `PubSub` in both protocols, with patterns, `PUBSUB`, a transaction's `PUBLISH`, and one tenant's channels kept from another's |

Each joined `client_compat_drill`, so it cannot regress silently.

### Stage 4: D6 then D3, held (2026-10-03), reopened (2026-10-08)

Jeff held it on 2026-10-03 until a tenant asked for rq or BullMQ, and
reopened it on 2026-10-08, after stage 3. Those are the two libraries it
serves: rq keeps results on a stream, and BullMQ's scripts need `cmsgpack`.
ADR-0053's amendment (`LMOVE` and its relatives across slots, for rq's
worker) comes with it, since rq needs both. Built in three parts: D6's first
set and blocking `XREAD` (below), then D3, then the amendment.

#### D6: streams, first set (2026-10-09)

**The two-release rule decided the rollout.** A stream is a new value type
(7). A release from before it reads a stream key as having no type: `TYPE`
answers `none` while `EXISTS` answers 1, and its typed stores answer
WRONGTYPE. Every release must roll back (Jeff, 2026-09-23), so, as
ADR-0053 did for placed tenants, this release reads streams everywhere and
creates one only when the seat runs with `--streams` (inventory:
`streams on`), off by default. The next release turns it on. An operator
who turns it on now gives up rolling back below this release once a tenant
has made a stream.

- **Storage** (`flint-storage`, `streams.rs`):
  - **The metadata row** is the collection row (version, entries, bytes)
    plus the last ID ever given, the largest deleted ID and the number of
    entries ever added. The last two are what Redis 7 keeps for consumer
    groups' lag. They are written from the first release, so groups need no
    second format change, and no second two-release rollout.
  - **Entries** are subkey rows whose field is `0x00`, then the ID in
    big-endian, so a scan walks them in ID order. Other row kinds a stream
    may need later, a group or a pending entry, take other leading bytes.
  - **Generic machinery works unchanged.** GC, `DEL`, expiry, slot
    migration, backup, eviction, `FLINTKEYSIZE`, `WATCH`, `COPY` and
    `RENAME` handle a stream with no stream code, because it is a collection
    row and subkey rows like the others. `COPY` gained one match arm.
  - **An emptied stream keeps its key**, as Redis's does, because its last
    ID still bounds the next.
- **Seat** (`commands/streams.rs`): `XADD`, `XLEN`, `XRANGE`, `XREVRANGE`,
  `XDEL`, `XTRIM` and `XREAD`, parsed as Valkey parses them, so the same
  wrong command fails with the same error first. `XREAD` answers at once,
  `BLOCK` or not.
- **Proxy:**
  - **`XREAD BLOCK` waits as the blocking pops do.** `$` and `+` are
    resolved to the newest entry's ID before the first attempt, by one
    `XREVRANGE … COUNT 1` per such stream. Re-sent as `$`, each attempt
    would have read only what followed IT, and an entry added between two
    attempts would have been skipped.
  - **XREAD's RESP3 map is rebuilt as RESP2's pairs**, inside `EXEC` too.
- **Differences** (command-support.md lists them):
  - `~` trims exactly, up to `LIMIT` (10,000 by default), where Valkey
    trims whole internal nodes.
  - A multi-stream `XREAD` needs one slot, except a placed tenant's
    (ADR-0053).
  - Consumer groups, `XINFO` and `XSETID` are not yet served.

**Verified** against Valkey 9.1.0 on the laptop:
- **A probe of 88 calls per protocol** (RESP2 and RESP3) differs only where
  documented: `~` trims exactly, and a two-slot `XREAD` is refused.
- **A random differential of 40,000 commands** (four seeds, both protocols)
  found one defect. `XRANGE … COUNT 0` answered the null array before it
  looked the key up, so a key of another type answered nil where Valkey
  answers WRONGTYPE, and a missing key nil where Valkey answers an empty
  array. The key is looked up first now, and the corpus holds both.
- **`XREAD BLOCK` through an authenticated proxy** matches Valkey step for
  step in both protocols: a wake on `$` 0.3 s after the `XADD` that causes
  it, a 0.5 s timeout answering nil, `BLOCK 0`, two streams in one slot,
  `+`, a stream created while it is waited on, WRONGTYPE, and a negative
  timeout.
- **Reading the newest entry costs what it reads.** On 200,000 entries,
  `XREVRANGE s + - COUNT 1` takes 0.25 ms plain, 0.29 ms in `MULTI` and
  0.33 ms in a script (debug build, in memory; Valkey 0.12, 0.35 and
  0.13 ms). In `MULTI` and scripts it took 68 ms until BUG-0248.
- **The corpus** passes on a seat (206 cases), through the proxy (203) and
  on Valkey (151 it shares), in both protocols. Redis clients' blocking
  `XREAD` and BullMQ run in `client_compat_drill`.

#### D3: the script libraries (2026-10-09)

As D3 recommended (c): `cjson`, `cmsgpack`, `bit` and `struct` in Rust
(`script_libs.rs`), behind the same read-only wrapping as the standard
libraries.

- **Each function is Rust behind a small C function**, which is what a
  script calls. Only a C function can do what the libraries' C does here:
  - **Name the script's line.** The first build raised errors from a Lua
    wrapper with `error(message, 2)`. A script that tail-calls the function
    (`return cjson.decode(s)`) loses its own frame to the wrapper, so its
    line was gone. The C function raises after `luaL_where(L, 1)` as
    `luaL_error` does, and argument errors go through `luaL_argerror`, which
    names the function as the script called it (`'?'` under `pcall`).
  - **Take and give thousands of values.** Rust holds an mlua reference for
    each Lua string or table in hand. mlua has about 7,996 of them and
    panics past that, where Valkey answers. So the C function packs the
    arguments into one table, and the many results of `cmsgpack.unpack` and
    `struct.unpack` come back the same way. `redis.call` and `redis.pcall`
    had the same limit, and now take their arguments packed (BUG-0246).
- **Nesting is walked on stacks of their own**, not by recursion. The first
  build overflowed a connection thread's stack, and crashed the seat, at
  1,000 levels of `cjson`; Valkey decodes MessagePack 3,999 deep. The slots
  Valkey's C would hold on the Lua stack, whose limit is 8,000, are counted,
  so a call fails at the depth and count Valkey's does, with its message.

- **What they match, from Valkey 9.1, function by function:**
  - `cjson`: `%.14g` numbers, `\/` and `\u00XX` escapes, sparse-array and
    nesting refusals, lua-cjson's tokenizer errors with their character
    positions, `strtod`'s acceptance of hex and `inf`, and `cjson.null`.
  - `cmsgpack`: the narrowest integer encoding, float32 when exact, the
    16-level nesting cutoff, and `unpack_one`/`unpack_limit`'s offsets.
  - `bit`: LuaBitOp's rounding by its own trick.
  - `struct`: lua-struct's sizes, alignment and errors.
- **One deliberate difference:** `cjson`'s settings answer their defaults
  and refuse changes, which would outlive the script on a shared state.

**Verified** against Valkey 9.1.0, call by call: 128 ordinary calls across
the four libraries, then their edges. Bisecting Valkey found where each runs
out: `cjson` at 1,001 levels, `cmsgpack` at 4,000 levels and 8,000 values,
`cmsgpack.pack` at 4,001 arguments, and `struct.unpack` at 7,998 results.
Around 80 more calls covered errors from tail calls, `pcall` and method
calls, LuaBitOp's argument order, lua-struct's nil marker, and nil and NaN
map keys. Flint now agrees on all of them but two, both documented:
- **an object's key order** (Lua 5.1's, as in Redis 8.2);
- **listing a library table** (BUG-0247).

The corpus holds 33 of those edges, and they pass on Valkey too.

The first build failed three ways, each fixed before the push:
- errors lost the script's line from a tail call;
- 1,000-deep `cjson` crashed the seat;
- `cmsgpack.pack` of 9,000 keys hit mlua's reference cap.

Thirteen mutants, one per property (the error's level, the slot count, the
5.1 stack check, LuaBitOp's order, the nil marker, the bare nil-key error,
the argument and result limits, the overlay's merge and seeks, and
`COUNT 0`'s order), were each killed by the check meant for it.

#### ADR-0053's amendment (2026-10-09)

On a placed tenant, `LMOVE`, `RPOPLPUSH`, `BLMOVE` and `BRPOPLPUSH` may span
slots: the seat reads and writes each key under its own slot when the
connection carries `FLINTWHOLE`. That was the last thing rq's worker
needed. In `client_compat_drill`, rq 2.8.0's burst worker on a placed tenant
finishes every job and its results read back. The design and the
measurements are in ADR-0053, under its amendment.
