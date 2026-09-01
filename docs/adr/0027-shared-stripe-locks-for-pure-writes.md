# ADR-0027 — Shared-mode stripe locks for pure writes, so a pipeline can commit as one batch

**Status:** proposed
**Date:** 2026-08-31
**Relates to:** [ADR-0005 D4](0005) (async write queue), [ADR-0012 D6](0012-same-slot-transactions.md) (no serial isolation for readers), BUG-0078 (the edge caps ingest)

## Context

A pair master tops out at **~80,000 seq/s (~84 MB/s)** on `i4i.4xlarge` with
1 KB values, driven directly over internal mTLS. Measured 2026-08-30/31, and
the ceiling is not where any of us guessed:

| candidate | verdict | evidence |
|---|---|---|
| fsync | **no** | a background tick at 2/s cannot gate 80,000 writes/s |
| DB mutex | **no** | `rocksdb.db.mutex.wait.micros` COUNT 0 |
| compaction threads | **no** | 6 threads vs 1 moves the operating point 1% |
| connection count | **no** | throughput peaks at 24 connections and *declines* after |
| replication | **no** | the lag cap only binds past 192 connections |

What is left is per-write overhead, and the engine's own counters name it:

```
rocksdb.number.keys.written   5,137,320
rocksdb.write.wal             5,137,324    one WAL append PER KEY
rocksdb.write.self            2,510,360
rocksdb.write.other           2,626,968    group commit collapsing only ~2
rocksdb.db.write.micros       P50 13us  P95 125us  P99 217us
```

A `perf` profile of the master under load agrees: **`WriteThread::AwaitState`
is the top symbol at 7.83%**, and the scheduler churn around it
(`__schedule`, `finish_task_switch`, `_raw_spin_unlock_irqrestore`,
`__sched_yield`) brings threads-waiting-on-the-write-group to **~21% of
samples**. The actual work — memtable insert, CRC — is a few percent each.

RocksDB's answer to this is `WriteBatch`, and Flint already has the plumbing:
`RocksKv::apply_writes` commits a set of mutations as one grouped WAL append,
and `BatchingKv` buffers a command run while overlaying reads so each command
still computes its exact reply. Transactions (ADR-0012) and the async queue
(ADR-0005 D4) both use it.

**So why is a connection's pipeline not committed that way?** Because of the
write lock, and this is the crux of the whole decision.

`write_lock.rs` exists for lost updates: "every write is a read-modify-write at
some layer" — INCR/APPEND/SETNX at the value layer, every complex-type
mutation at the meta layer. Its scheme is 128 hash stripes: a single-key write
takes `GLOBAL.read()` plus its stripe `Mutex`; a multi-key or keyless write
takes `GLOBAL.write()`.

A batch spans many keys, so it needs many stripes — and that is where it dies:

| batch size | stripes held, of 128 |
|---|---|
| 2 keys | 2 (2%) |
| 32 keys | 28 (22%) |
| **256 keys** | **111 (87%)** |

At any batch size worth having, ordered multi-stripe acquisition **is** global
exclusion. That is why the async queue's consumer takes `lock_all()` per batch
— an honest approximation of what it would get anyway.

### The measured consequence

Taking `lock_all()` per batch replaces 24 parallel writers with one, and it
costs far more than batching saves:

| connections | async off | async on | delta | queue depth |
|---|---|---|---|---|
| 24 | 62,880 | 44,486 | **-29%** | 2-14 |
| 192 | 75,953 | 52,011 | **-31%** | max 114 |
| 384 | 46,111 | 33,188 | **-28%** | max 269 |

The obvious rescue — "the batches were too small" — was tested and **refuted**:
at 384 connections the queue reached 269 deep and the penalty was identical.
Batch size was never the variable. It also made overload *worse*, shedding
37.7M and 40.5M writes on the 2 s deadline against 22.4M and 30.0M with it
off, because the queue hop adds latency. It did fix the engine-level symptoms
(`writes_delayed_soft` 1.0-1.8M -> **0**, L0 4-8 vs 6-21), which is worth
remembering: it smooths the path while narrowing it.

### The premise turned out to be partly false

"Every write is a read-modify-write" was true of plain `SET` only by accident.
`SET` read the old header and used it for exactly two things — NX/XX (does the
key exist) and KEEPTTL (what was its expiry) — so an unconditional `SET k v`
paid a full point lookup and dropped the answer. Fixed in `d3ef6d7`; the read
is now taken only when something consumes it. Measured at the storage layer:
**~1.9x on overwrites** (where the read hit) and **~1.15x on fresh inserts**
(where the bloom filter rejected it cheaply, n=2, spread +7% to +23%).

The percentage is the smaller half. **An unconditional `SET` is now a pure
write** — it reads nothing, so there is no stale read for a concurrent writer
to invalidate, and nothing to lose an update against.

## Decision

**Make the stripe lock shared/exclusive rather than exclusive-only, and commit
a pipeline's contiguous pure writes as one `WriteBatch`.**

```
STRIPE: [Mutex<()>; 128]          ->  [RwLock<()>; 128]

pure write (plain SET)      GLOBAL.read() + STRIPE[h].read()    SHARED
RMW write (INCR, LPUSH, …)  GLOBAL.read() + STRIPE[h].write()   EXCLUSIVE
multi-key / keyless         GLOBAL.write()                       unchanged
reads                       no lock                              unchanged
```

Correctness, pair by pair:

| pair, same key | outcome |
|---|---|
| SET vs SET | both shared, concurrent. Neither reads, so there is no stale read to lose; RocksDB's write order picks the winner, exactly as the mutex would have |
| SET vs INCR | INCR takes exclusive -> mutually excluded; the read-modify-write stays atomic |
| INCR vs INCR | both exclusive -> serialised, unchanged |
| SET vs LPUSH | LPUSH is RMW at the meta layer -> exclusive -> excluded |

**Shared locks do not block each other**, so two connections each committing a
256-key batch of plain SETs proceed in parallel. That is the property
`lock_all()` destroyed and the reason the async queue lost 30%.

The cost is favourable: ~111 uncontended shared acquisitions (one atomic each)
per 256-key batch, against ~255 write-group joins saved at a 13 us P50.

It is deadlock-free without ordering gymnastics: batches take only shared
locks so they never block each other, and an RMW holds nothing while waiting
for its single stripe, so no cycle can form. Ascending-index acquisition is
still specified, cheaply, so the property does not rest on that argument
surviving future edits.

A useful side effect independent of batching: SET-vs-SET on a colliding stripe
stops serialising. With 24 connections over 128 stripes that happens often.

## Alternatives considered

- **`lock_all()` per batch (status quo for the async queue).** Measured at
  -28% to -31% across three concurrency levels, and the "batches were too
  small" rescue was refuted at depth 269. Rejected on evidence.
- **Raise the stripe count.** Fails the birthday bound: with 24 connections
  holding 256 keys each, ~6,144 keys are live at once, so keeping collisions
  under 1% needs of order 10^9 stripes. Not viable.
- **Shard the committer** (partition the keyspace, one writer per shard, so
  concurrent batches are disjoint by construction). Sound, and genuinely
  lock-free, but it fans one pipeline into S engine writes, needs fan-out/join
  plumbing and per-shard ordering, and relaxes cross-key order within a
  connection. Strictly more machinery than this ADR for the same goal; worth
  revisiting if shared stripes prove insufficient.
- **RocksDB `TransactionDB` / `OptimisticTransactionDB`.** The engine ships a
  real key-level lock manager and conflict detection; we open plain `DB` and
  built 128 hash stripes over it. This is the principled long-term answer for
  the RMW commands and should be evaluated on its own, but it is a far larger
  change than making one lock shared.
- **Merge operators for the RMW commands.** Pushes INCR-style read-modify-write
  *into* the engine, removing the application read and therefore the lock, for
  counters specifically. Complementary, not a substitute — it does nothing for
  complex-type meta mutations.

## Risks and open questions

- **Writer preference is the one that can silently undo this.** If the
  platform's `RwLock` is writer-preferring, a single waiting INCR blocks
  arriving SET batches on that stripe and reintroduces the serialisation this
  ADR removes. `std::sync::RwLock` does not guarantee a policy. The choice
  must be pinned deliberately — and the failure is quiet, so it needs a test
  that would catch it rather than a comment saying it should not happen.
- **The predicate must be strict.** `write_queue::is_batchable` is the wrong
  test: it admits `INCR`, `DECR`, `APPEND` and `SETNX`, which are exactly the
  RMW cases. A new, narrower "pure write" predicate is needed — `SET` with no
  NX/XX/KEEPTTL/GET — and getting it wrong is a lost-update bug, not a
  performance regression.
- **Failure semantics change.** Today each `put` in a pipeline lands
  independently; batched, a commit failure fails the whole run and the replies
  already computed must be rewritten as errors.
- **Cross-key ordering within a pipeline relaxes.** `SET a; SET b` may land as
  b-then-a. ADR-0012 D6 already declines serial isolation for readers, so this
  is not a new promise being broken, but it should be stated rather than
  discovered.
- **Replication accounting shifts.** One batch is one WAL sequence, so
  bytes-per-sequence rises sharply. The WAL headroom guard is denominated in
  SEQUENCES and assumes ~16 KiB each (BUG-0079); a 256-key batch of 1 KB
  values makes a sequence ~256 KB. That guard needs re-deriving, and BUG-0079
  is precisely the bug where a unit mismatch between a guard and the thing it
  guarded went unnoticed.
- **Unquantified.** The 1.9x/1.15x above is the dead-read removal alone,
  single-threaded at the storage layer. What batching itself is worth on the
  server path — TLS, RESP parsing, replication and the write group all
  unchanged — has not been measured. The profile says ~21% of samples are
  write-group waiting, which bounds the prize but does not predict it.

## Scope note

All of this is **server-side**, and BUG-0078 (OPEN) says that is not the
ceiling a real client meets: the same loader through the proxy edge got 690
writes/s per connection against 5,030 direct — **7.3x worse, at 0.08 of a
core** — because the edge punishes round-trips. Everything here was measured
with the proxy deliberately out of the path. Raising the server's ceiling is
worth doing and does not, on its own, make a proxied client faster.

## Amendment, 2026-08-31 — implemented, and one argument above is wrong

### The deadlock-freedom argument was incomplete, and the gap was real

This ADR said:

> It is deadlock-free without ordering gymnastics: batches take only shared
> locks so they never block each other, and an RMW holds nothing while waiting
> for its single stripe, so no cycle can form.

True for DISTINCT readers. It misses a reader **re-entering** a lock it already
holds. The first implementation took `GLOBAL.read()` once per key, so a batch
holding it from its first key asked for it again on its second -- and with a
`lock_all()` pending, the writer-preferring `RwLock` blocked that second
acquisition against a writer waiting for the batch to finish. The batch could
not finish; finishing is what releases the guard.

It wedged the test suite for 25 minutes, and it would have reached a fleet as
an unexplained stall: every `MSET`, `FLUSHALL` and multi-key `DEL` takes
`lock_all()`, and the failure produces no error, no shed counter and nothing in
`FLINTINFO` -- just connections that stop.

The risk section named writer preference as the main hazard and asked for a
test. It got one, twice: the suite deadlocked before the test existed, and then
the same property produced the product bug. **A hazard listed in an ADR is not
a hazard mitigated.**

Corrected design, which is what shipped:

- **one `GLOBAL.read()` per BATCH**, taken at construction, never per key;
- **each stripe at most once**, tracked in a `held` set -- re-entering one
  stripe reproduces the same deadlock a level down;
- **`serve` owns the locking**, so `execute` takes no guard for a batched
  write. That removed the guard-sink machinery this ADR originally implied.

A regression test pins it: a 120-key batch against a thread continuously
holding `lock_all()`, with a bounded wait, because the bug was an INDEFINITE
block and any finite budget separates that from slow. Verified in both
directions -- reintroducing the per-key acquisition makes it fail with its own
message.

### Measured

The "Unquantified" note above is settled for the server path. Eight concurrent
connections, depth-256 pipelines, through a real server on loopback:

| value size | `origin/main` | with batching | ratio |
|---|---|---|---|
| 32 B | 96,479 / 92,954 | 298,370 / 338,087 | **~3.4x** |
| **1 KB** | 83,871 / 82,752 | 198,640 / 169,931 | **~2.2x** |

Quote the 1 KB row: value size changes the RATIO, not just the rate, because
the smaller the value the larger a share per-write overhead is.

The number worth keeping is the control. **Un-batched, the laptop tops out at
~83,000 sets/s and the 5-host fleet topped out at ~80,000 seq/s** -- loopback
plaintext against mTLS across a real network, same wall. That is independent
evidence the ceiling is write-path bound rather than network bound, which is
what the profile claimed and what makes a local measurement admissible here at
all.

Biases, in both directions: loopback strips TLS and the network from the
denominator, so a fleet will show less; and 8 connections cannot contend 128
stripes the way a fleet does, so this mostly measures the batching and
understates the shared-lock half. **A fleet A/B is still owed.**

### Scope, unchanged

Still server-side. BUG-0078 has the edge at 690 writes/s per connection against
5,030 direct, so a proxied client does not see any of this.

### What it broke: the write deadline's own clock

Staging a write is not serving it, and the admission gate did not know the
difference.

The deadline (#186) refuses on arrival when `inflight x service` already
exceeds it, and both terms come from the `WriteInFlight` guard `execute` holds
for the length of its call. Under batching that call only STAGES the write:
the commit happens later, in `commit_pending`, outside the guard. So the clock
stopped before the work it was meant to measure, and a staged write stopped
counting as in flight while it waited for the flush. Both terms under-read,
and the gate under-fired.

Same binary, same box, same 128x100 load, differing only in whether the write
is batchable:

| write | batched | wall clock | `write_service_us` |
|---|---|---|---|
| plain `SET` | yes | 0.32s | **7** |
| `SET .. EX` | no | 0.33s | 15 |
| plain `SET` | yes | 0.32s | **7** |
| `SET .. EX` | no | 0.34s | 11 |

Identical real cost, half the measured cost. The missing half is the commit.

**It was caught by a positive control, not by a failing assertion.** The
`write_deadline` drill went red saying it could not ARM — 256 concurrent 4 KiB
writes all served inside 1ms — which reads like a fast box and was not. On one
box, clean `main` armed at 128 threads while this branch could not arm through
256; with the guard corrected it arms at 128 again. Nothing asserted a wrong
value anywhere; the shed simply stopped being reachable, and only a control
that has to be MADE to fire could notice that.

The first remedy tried was the wrong one, and is worth recording as such:
raising the ladder to 256 threads x 256 KiB moved `write_service_us` to 49 and
still shed nothing, because the term that was wrong was never the load. A
drill that had been "recalibrated" that way would have gone green over a
weakened gate.

The guard now lives in `PendingBatch` and drops after `commit_ops`, beside the
stripe guards and for the same reason: the write is in flight until it
commits.

### Correction 2026-08-31: the control above was measuring a TCP timer

The "Measured" section leans on a coincidence — the laptop topping out at
~83,000 sets/s and the 5-host fleet at ~80,000 seq/s, "loopback plaintext
against mTLS across a real network, same wall" — and reads it as independent
evidence that the ceiling is write-path bound.

**Read it the other way.** Two environments that share no hardware, no
transport and no network hitting the same number is the signature of a
limiter that belongs to neither. BUG-0078 found it: the node never set
`TCP_NODELAY`, so any pipeline over ~16 KiB paid a delayed ACK per round
trip — a flat ~50 ms, identical on a laptop and on a fleet, because a timer
does not care what it is running on. These measurements used depth-256
pipelines of 1 KiB values, which is squarely past that threshold.

With the one-line fix, one connection direct to a node goes from 5,081 to
472,978 writes/s at that depth. So the ceiling those numbers describe was the
timer, and the ratios in the table above compare two arms that were both
sitting on it.

**What survives.** The batching ratios are still meaningful as ratios: both
arms paid the same stall, so the comparison is not meaningless — but it was
taken in a regime where per-round-trip latency dominated, and the honest
position is that **the magnitude is unverified at the real ceiling**. The
"~2.2x on 1 KB pipelined writes" in the index should be treated as a
measurement owed, not a result. The fleet A/B this ADR already says is owed
is now owed for a second reason.

The deadlock-freedom argument, the lock protocol and the correctness
reasoning are untouched by this: it is only the performance claim that was
measured through a stall.
