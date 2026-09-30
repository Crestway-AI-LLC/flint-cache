# ADR-0053: Transactions across slots, measured

Status: **ACCEPTED 2026-09-27** (Jeff: "go with your recommendation on
ADR-0053"): option D. Built for transactions and scripts; see "As built" at
the end, which also measures a need the Context missed (rq's worker). The
amendment it proposed for that need was **accepted 2026-09-30** (Jeff: "go
with your recommendations"), to be built with ADR-0052's stage 4.

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

## As built (2026-09-27)

Option D, for transactions and scripts. Not in rc.77: ships with the next
release.

**A tenant is placed when it is created**, with
`flintctl tenant add-on-pair <name> <token> <ns> <pair> [k]` (at the control
plane, `CPADDTENANTONPAIR`). `<pair>` is the pair's index in the inventory,
or one of its members. The namespace must be the tenant's own: the control
plane refuses to place a tenant in a namespace a spread tenant uses, a spread
tenant in a placed one, and two placed tenants in one namespace. A placed
tenant stays where it was created (see "Not built").

**Two releases to switch it on.** A release from before this one routes a
placed tenant by the slot table, across every pair, where its keys are not:
rolled back to, it would serve that tenant empty. So this release reads
placement everywhere, and creates a placed tenant only when the control plane
runs with `--placed-tenants` (inventory: `placed-tenants on`), which is off
by default. The next release turns it on, and a rollback from there lands on
this release, which reads it. An operator who turns it on now accepts that
this fleet does not roll back below this release while it holds a placed
tenant.

- **Control plane.** The tenant record has an optional `pair`. A record
  without it is spread, and a release before this one ignores the field, so
  the change adds no `Mutation` variant. `CPPLACED` lists each placed
  namespace and its pair. The snapshot proxies read carries `P<n>` in the
  tenant's flags.
  - **Found on the way: BUG-0191.** A Raft control-plane node whose state
    file would not parse started with an empty store, so it forgot its vote
    and could vote twice. An unknown `Mutation` variant from a newer release
    was one way to reach it. It now refuses to start, and says why.
- **Proxy.** Every key of a placed tenant routes to its pair, and so does
  every keyless command. A slot in migration, and an exception row, would
  still win, as for any tenant; a placed tenant has neither, since nothing
  moves it. A pair index with no pair behind it is refused rather than
  routed by the slot table. On each seat connection it opens for a placed
  tenant, the proxy sends `FLINTWHOLE` after `FLINTNS`.
- **Seat.** `FLINTWHOLE` marks the connection's namespace as whole on this
  seat, and `FLINTNS` clears the mark. On such a connection a transaction
  and a script may span slots. A transaction still runs under the lock over
  every writer and commits as one batch (ADR-0012 D2, D3). A script that
  reaches an undeclared key runs again under that lock, as in its own slot
  (ADR-0052 D2). Every other connection keeps the one-slot rule, and so does
  a client that dials a seat directly. A tenant cannot send `FLINTWHOLE`:
  the proxy refuses every `FLINT*` command from a client.
  - **Where D differs from the proposal.** The seat is not given a list of
    the namespaces that live wholly on it; the proxy marks the connection.
    The seat trusts its internal peer for this as it already does for
    `FLINTNS`, and nothing new is pushed to seats.
- **Multi-key commands keep their one-slot rule** on a placed tenant too:
  `RENAME`, `LMOVE`, `SMOVE`, `MSET`, `SUNIONSTORE` and the rest. Each
  stores its keys under one slot.
- **Controller.** A placed tenant's keys count toward its pair's load, since
  they fill it as any keys do, and a pair full of one must not be sent more.
  None of its slots is ever chosen to move. The controller asks the control
  plane `CPPLACED` every cycle. When a planned move's source has nothing
  movable, it tries the plan's next move, so a pair that one placed tenant
  keeps heavy holds up no other move. If it cannot ask, it moves nothing
  that cycle. A controller without `--commit-cp` has no control plane to ask
  and treats every tenant as spread; `flintctl` always passes it.
- **Operator.** `flintctl migrate-slots` refuses a placed namespace.

**Measured** on a gate box, in `client_compat_drill` and
`placed_tenant_rebalance_drill`, both in the gate:

- **Transactions and scripts.** A placed tenant's transaction over three
  slots, which a spread tenant's two pairs would refuse, commits whole. WATCH
  across slots aborts on another connection's write to any watched key. A
  script whose `KEYS` span slots runs. `RENAME` across slots is still
  refused. A spread tenant's transaction across pairs is still refused, and
  the two tenants' keys stay apart.
- **rq 2.8.0.** Enqueue works on a placed tenant: five jobs, each queued, in
  order. So does `Queue.empty`, whose script reaches every job's key.
- **Sidekiq 8.1.7, its server run for real** on a placed tenant.
  `Queue#clear` empties the queue. The server runs a job, and its heartbeat
  registers the process (`Sidekiq::ProcessSet`). Its log shows two gaps,
  neither of them this record's, logged as BUG-0192. The metrics flush on
  each heartbeat sends `BITFIELD`, which Flint does not serve, so it fails
  every beat, and the job and the heartbeat are unaffected. INFO reports no
  `maxmemory_policy`, so the server warns that Redis will evict its data,
  which Flint does only for a namespace that opts in.
- **The balancer.** A placed tenant (8,000 keys) and a spread one (4,000
  keys in four slots) both started on pair 0, with rebalancing armed. The
  controller moved every key of the spread tenant to pair 1, in one move,
  and none of the placed tenant's. Both read back whole through the proxy,
  and the placed tenant's transaction across slots still committed.
- **Not measured:** the queue probe re-run, with Celery (pub/sub, stage 3)
  and BullMQ on its default prefix (`cmsgpack`, stage 4). Each stops at its
  stage first.

**rq's worker needs two things more, measured.** It dequeues with
`LMOVE rq:queue:<name> rq:queue:<name>:intermediate`, and those two keys are
in different slots (6541 and 3513 for the queue `compat`), so the seat
refuses the `LMOVE` as `CROSSSLOT`. It then reads a job's result from a
stream (`XREVRANGE`), which is stage 4. The Context above said nothing
measured needed a multi-key command across slots. rq's worker does, and so
this record's claim that D serves rq is not yet true.

**Amendment, ACCEPTED 2026-09-30** (Jeff: "go with your recommendations"),
not yet built: on a placed tenant, let
`LMOVE`, `RPOPLPUSH`, `BLMOVE` and `BRPOPLPUSH` span slots. Each is one pop
and one push, and each already excludes every writer while it runs
(BUG-0188), so the change is to take the destination's own slot; days, not
a re-plumbing. The other multi-key commands keep one slot until something
measured needs them. It is built alongside ADR-0052's stage 4, since rq's
worker needs both before it can finish a job.

**Not built.** Moving a placed tenant to another pair, whole. It stays on the
pair it was created on, and that pair's size and throughput are its limit.
The verification item "moving a one-pair tenant to another pair, under load"
waits with it.
