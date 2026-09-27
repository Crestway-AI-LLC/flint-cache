# ADR-0053: Transactions across slots, measured

Status: **PROPOSED 2026-09-27, for Jeff's decision.** Nothing is built.

## Context

ADR-0012 serves a transaction in one slot. ADR-0052's stages 1 and 2 got the
job-queue libraries past their first blockers, and three of them then stopped
at that rule: rq's enqueue, Celery's worker, and Sidekiq's `Queue#clear`.

**Measured 2026-09-27** on a gate box with the ADR-0052 probe. Each library
ran against a plain Valkey with `MONITOR` recording every `MULTI`...`EXEC` it
sent, and then through the proxy on a two-pair fleet at `4b27ff8`:

| library | its transactions (distinct shapes) | slots per transaction | through Flint |
|---|---|---|---|
| rq 2.8.0 | 9, all writes: enqueue (the job's hash, the queue's list), worker registration and heartbeat, job start, and job completion (worker, job, results stream, finished set, wip set, execution records) | 1 to 9 | refused at enqueue |
| Celery 5.6.3, unconfigured | 6: queue lengths (`LLEN` of `celery`, `celery3`, `celery6`, `celery9`, read-only), the unacked index and hash, deleting a worker's reply queues, a result's `SETEX` with its `PUBLISH` | 1 to 4 | the worker fails on the queue-length read |
| Celery 5.6.3, kombu `global_keyprefix: '{celery}'` | the same 6 | **1** | past every transaction; stops at `PSUBSCRIBE`/`SUBSCRIBE` (ADR-0052 stage 3) |
| Sidekiq 8.1.7 | `Queue#clear`: `UNLINK queue:<name>`, `SREM queues` | 2 | refused (`client_compat_drill`) |

Push and fetch work for Sidekiq (ADR-0052 stage 2). Its server, run for real,
was not measured: the probe could not start it. What the server itself sends
in transactions, its heartbeat included, is part of this record's
verification.

Two of the libraries can be configured into one slot. BullMQ documents a
hash-tag `prefix` for Redis Cluster (ADR-0052), and kombu's
`global_keyprefix` does the same for Celery (measured above). rq and Sidekiq
cannot. rq's key names are class constants, and Sidekiq dropped namespace
support in 7.0. Both are run against one Redis node, where every key is on
that node.

**What the one-slot rule protects.** A transaction lives on one node's
connection and runs under that node's lock over every writer, committed as
one batch (ADR-0012 D2, D3). That is atomic on one seat whatever slots the
keys are in. What cannot be atomic is a transaction across **pairs**: there
is no single step spanning two seats. So the rule ADR-0012 needed was one
seat, and "one slot" was the simplest way to guarantee it.

Every transaction measured above queues single-key commands. Each command
already finds its own key's slot when it runs, so a transaction across slots
on one seat needs no change to any command, only to the queue-time check.
Multi-key commands are different: `RENAME`, for one, keeps both keys under
the source key's slot. Letting those span slots would mean re-plumbing each
of them, and nothing here needs it.

## Options

**A. Keep one slot.** Document Celery's `global_keyprefix` and BullMQ's
`prefix`. rq, and Sidekiq's `Queue#clear` and whatever else of Sidekiq needs
a transaction, stay unsupported.

**B. Split read-only transactions per pair, as `MGET` is split (ADR-0048).**
This covers Celery's queue-length read and gives up its snapshot across
slots. But Celery's writes span slots too (the unacked index and hash), and
rq's are all writes. B unblocks no library, so it is listed only to be
rejected.

**C. One seat, any slots.** A seat serves a transaction whose keys span
slots it holds. The proxy already binds a transaction to one pair, and
refuses one that reaches a second pair, whole (BUG-0190). Atomicity is
exactly today's: one lock over every writer, one batch.
- **Cost:** days.
- **Where it falls short:** on a fleet of one pair, every measured
  transaction works. On a fleet of several pairs, a transaction works only
  when its keys happen to share a pair. rq's enqueue names a random job id,
  so on two pairs it would fail about half the time. That is worse than a
  clear refusal, so C should not ship alone.

**D. C, and a tenant placed on one pair, opt-in.** A tenant can be given a
home pair. The proxy then routes every slot of that tenant there, so every
transaction it sends is on one seat, and C serves it. A seat knows which
namespaces live wholly on it, and serves transactions across slots for those
only. It stays strict for every other tenant, and for a client connected to
the seat directly. The same freedom extends to scripts, so BullMQ could run
on its default prefix.
- **What it costs the tenant:** its throughput and its size are one pair's,
  as they are on one Redis.
- **What it costs Flint:** a field on the tenant, a routing rule in the
  proxy, the namespace list pushed to seats, and moving a tenant between
  pairs as one unit, which the balancer must honour. About a week.

**E. Atomic transactions across pairs.** A two-phase commit between seats:
durable prepare records, in-doubt transactions resolved across a failover,
and lock ordering across seats. Weeks, and every transaction across pairs
pays a round trip. No measured library needs it if D exists.

## Recommendation

**D.** It serves these libraries the way they are built to run, on one
node's worth of keys, while every other tenant keeps its data spread. It
changes no command's semantics. A transaction remains atomic on one seat,
and one that would reach a second pair is still refused whole. Keep A's
documentation too: a tenant spread across pairs can still run Celery and
BullMQ with their prefixes. Reject B, and leave E until a measured need
survives D.

This comes before ADR-0052's stage 3: without it, pub/sub would not make
Celery work unconfigured.

## Verification (if accepted)

- `client_compat_drill` on a one-pair tenant: rq enqueues and a worker
  completes its job; Sidekiq's `Queue#clear` and a real Sidekiq server,
  whose heartbeat registers the process; Celery unconfigured, once stage 3
  lands; BullMQ on its default prefix, once stage 4 lands.
- A two-pair drill. A one-pair tenant's transaction across slots commits
  whole, including when a writer of one of its keys races it. A spread
  tenant's transaction across pairs is refused whole. A seat refuses a
  transaction across slots for a namespace that is not wholly on it.
- Moving a one-pair tenant to another pair, under load, with its
  transactions still atomic.
- The ADR-0052 probe re-run, as after every stage.
