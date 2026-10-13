# ADR-0057: Stream consumer groups, with pending entries that survive a failover

Status: **ACCEPTED 2026-10-12** (Jeff: "go with your recommendations on
ADR-0057 and ADR-0058"). Each decision stands as recommended below:
- D2's storage (b), with the per-consumer index;
- D4's blocking at the proxy, with `FLINTXREADGROUP` so that a miss writes
  nothing;
- D5's two releases;
- D6's three parts, each with its own gate.

Proposed 2026-10-10. Jeff approved writing it that day as the next large
item, rolled out in two releases as ADR-0056 was.

## Context

Streams shipped on 2026-10-09 (ADR-0052 D6) without consumer groups.
`XGROUP`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XAUTOCLAIM`, `XINFO`
and `XSETID` answer `ERR unknown command`.

Groups are the part of streams most applications use. A group hands each
entry to one of its consumers and keeps it **pending** until that consumer
acknowledges it. If the consumer dies, another one claims the entry. The
pending entries list (PEL) is the at-least-once record of a queue. These are
known to use groups, from their documentation; none is measured here:

- Dapr's Redis pub/sub component;
- Watermill's `redisstream`;
- Spring Data Redis's `StreamMessageListenerContainer`;
- Redpanda Connect's `redis_streams` input.

**Why durability matters here.** Redis replicates asynchronously, so after a
failover it can lose the most recent `XREADGROUP` and `XACK` effects:
entries already delivered are delivered again, and acknowledged ones become
pending again. The application sees duplicates. Flint already keeps every
acknowledged `XADD`. With groups on the same write path, an acknowledged
`XACK` stays acknowledged, and a delivered entry stays pending under the
consumer that received it.

**What a seat already has.** D6 was built with groups in mind.

- The metadata row records the last ID, the largest deleted ID and the
  number of entries ever added. These are Redis 7's inputs to a group's lag,
  so groups need no change to the metadata format.
- Entry rows lead with tag byte 0. D6 reserved other leading bytes for the
  rows groups need.

**Valkey 9.1 and Redis 8.2 differ.** Redis 8.2 adds `XACKDEL`, `XDELEX`, and
`KEEPREF`, `DELREF` and `ACKED` on `XADD` and `XTRIM`. Valkey 9.1 has none
of them (checked in both local binaries, 2026-10-10). Flint follows Valkey
9.1, as it does for the rest of streams.

**Observed on Valkey 9.1** (2026-10-10, on the laptop). The design must
reproduce each of these:

- An `XREADGROUP` that delivers nothing still creates its consumer and
  stamps its seen time.
- Reading history (any ID but `>`) answers at once, even with `BLOCK 2000`.
  An entry deleted since its delivery comes back as its ID with nil fields.
- A blocked `XREADGROUP` keeps the seen time of the call. After a 1 s block
  that timed out, `XINFO CONSUMERS` showed the consumer idle for 1,066 ms.
- A blocked `XREADGROUP` whose group is destroyed, or whose key is deleted,
  answers the same `NOGROUP` error that a new call gets.

## Decisions

### D1. The commands: Valkey 9.1's set

- `XGROUP`:
  - `CREATE` (with `MKSTREAM` and `ENTRIESREAD`);
  - `SETID` (with `ENTRIESREAD`);
  - `DESTROY`, `CREATECONSUMER`, `DELCONSUMER` and `HELP`.
- `XREADGROUP`, with `COUNT`, `BLOCK` and `NOACK`.
- `XACK`.
- `XPENDING`: the summary form, and the extended form with `IDLE` and a
  consumer.
- `XCLAIM`, with `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`, `JUSTID` and
  `LASTID`.
- `XAUTOCLAIM`, with `COUNT` and `JUSTID`. The deleted IDs are its third
  element.
- `XINFO`: `STREAM` (with `FULL` and `COUNT`), `GROUPS`, `CONSUMERS` and
  `HELP`.
- `XSETID`, with `ENTRIESADDED` and `MAXDELETEDID`. Its fields are already
  stored.

Each is parsed as Valkey parses it, so a wrong command fails with the same
error, checked in the same order, as D6's commands do.

**Not served:** Redis 8.2's `XACKDEL` and `XDELEX`, and `KEEPREF`, `DELREF`
and `ACKED` on `XADD` and `XTRIM`.

### D2. Storage: a row per group, per pending entry and per consumer

- **(a) One row per group, holding its PEL and its consumers.** Every
  `XREADGROUP` and `XACK` would rewrite the whole PEL. That is ADR-0056's
  problem again: a write that costs the size of the structure. Rejected.
- **(b) A row per item, under the stream's version, beside the entries,**
  each kind led by its own tag byte:

  | tag | row | field after the tag | value |
  |---|---|---|---|
  | 0 | entry (exists) | ID | fields and values |
  | 1 | group | group name | last delivered ID, entries read, pending count |
  | 2 | pending entry | group, ID | consumer, delivery time, delivery count |
  | 3 | consumer | group, consumer name | seen time, active time, pending count |
  | 4 | a consumer's pending entry | group, consumer, ID | empty |

  - **Names are length-prefixed** in tags 2 to 4, so group `a` never
    prefixes the rows of group `ab`.
  - **Tag 2 rows sort in ID order within a group.** `XPENDING`'s range,
    `XAUTOCLAIM`'s scan, and the summary's smallest and largest IDs are
    bounded scans.
  - **Tag 4 is the per-consumer index** that Redis keeps as each consumer's
    own PEL. A consumer's history read, and `XPENDING` filtered to one
    consumer, read only that consumer's entries.
- **(c) As (b), without tag 4,** filtering the group's PEL by consumer
  instead. That saves one row per pending entry. But a consumer's history
  read (`XREADGROUP … 0`, the first thing a restarted consumer does) would
  walk every other consumer's pending entries. Rejected.

**Recommendation: (b).**

- **Rows written per call:**
  - an `XREADGROUP` that delivers n entries writes 2n + 2 rows (n pending
    rows, n index rows, the group and the consumer);
  - an `XACK` of n entries deletes 2n rows and rewrites the group and each
    consumer it touches.

  D7 measures what that costs.
- **Generic machinery carries groups unchanged.** Every new row is a subkey
  row under the stream's version, as entries are. So these carry or remove
  groups with no new code: `DEL`, expiry, GC, slot migration, backup,
  `COPY` and `RENAME`. A `COPY` of a stream copies its groups, as Redis's
  does.
- **The metadata row does not change.** Group state never goes into the
  metadata row's tail. `StreamMeta::decode` reads 40 bytes there and
  `encode` writes 40, so a release that did not know about an added field
  would drop it the next time an `XADD` rewrote the row.
- **Accounting:**
  - `XLEN` counts entries only, as now.
  - The metadata row's `bytes` stays the entries' bytes. The per-key cap
    (512 MiB) and read admission (BUG-0060) bound it, and a growing PEL
    must not make `XADD` fail. Redis does not limit the size of a PEL.
  - Tenant storage metering reads RocksDB's bytes per namespace, so it
    counts group rows with no change.
  - `FLINTKEYSIZE` reports a stream's entries only, as documented.
  - `XINFO STREAM FULL COUNT 0` is the one command that reads a whole PEL.
    Its admission adds each group's pending count, times the size of a
    pending row, to the entries' bytes.

### D3. Semantics

- **Durability.** `XREADGROUP` is a write. As with every write, its reply
  goes out after its rows are on disk, and a failover keeps them
  (failover.md). So after a failover:
  - an acknowledged `XACK` stays acknowledged;
  - an entry that `XREADGROUP` delivered stays pending under its consumer,
    so `>` does not deliver it again.

  This is the gain over Redis, and D7 drills it.
- **An ambiguous failure is still the client's problem** (retry-safety.md).
  If the reply to an `XREADGROUP` is lost on a dropped connection, its
  entries stay pending under that consumer. The standard recovery still
  works: read history with `0`, or `XAUTOCLAIM`. retry-safety.md gains a
  row for each command:
  - `XACK` is safe to retry (it names what to change);
  - a retried `XGROUP CREATE` answers `BUSYGROUP`;
  - `XREADGROUP … >` is unsafe (a retry delivers the next entries);
  - `XCLAIM` and `XAUTOCLAIM` are unsafe (a retry changes delivery counts
    and owners again).
- **Times** are the master's wall clock in milliseconds, as Redis's are.
  After a failover, idle times are measured on the new master's clock.
- **Lag and entries-read** follow Redis 7's estimate from the metadata's
  entries added and largest deleted ID. Each is nil where Redis's is.
- **A multi-stream `XREADGROUP` needs one slot,** except for a placed
  tenant (ADR-0053), as `XREAD` does.
- **In `MULTI` and scripts,** `XREADGROUP` answers without waiting, as
  `XREAD` does.

### D4. A blocked `XREADGROUP` waits at the proxy, as `XREAD` does

- **(a) The proxy polls** (ADR-0052 D4's (b)). Each attempt is a
  non-blocking `XREADGROUP` on the master, which delivers atomically.
  - Two consumers of one group, both blocked on one stream, never both get
    the same entry.
  - The wait survives a failover, because the loop re-sends to the new
    master. A Redis client blocked on a master that dies does not get that.
- **(b) Seats wake their waiters** (ADR-0052 D4's (c)).

**Recommendation: (a),** with one change from `XREAD`: **a miss must not
write.**

- **The first attempt is the client's own command.** It creates the
  consumer and stamps its seen time, as Valkey's does.
- **Later attempts go as `FLINTXREADGROUP`,** an internal form. The seat
  answers it as it answers `XREADGROUP`, but writes nothing unless it
  delivers.
- **Without that form, an idle blocked consumer would cost a write every
  20 ms:** each one a WAL record, shipped to the replica. `XINFO CONSUMERS`
  would also show the consumer as idle for about 0 ms, where Valkey shows
  the time since the call (measured above).

The rest follows from Valkey:

- **Only `>` waits.** An `XREADGROUP` that names any other ID answers at
  once.
- **A group destroyed, or a key deleted, while a client waits:** the next
  attempt answers `NOGROUP`, the error Valkey gives a blocked client.

Two costs carry over from `XREAD`:

- **No first-come order among blocked consumers.** Valkey serves the
  longest-waiting client first.
- **Latency of up to one poll interval** (20 ms at most).

Move to (b) only if D7's measurement demands it, as for the blocking pops.

### D5. The rollout: two releases, as ADR-0056

**Why two releases, when the rows are safe for the release before.** rc.82
(due 2026-10-12), the first release with streams, scans only tag-0 rows. It would leave a
group's rows untouched and keep serving the entries. But it does not know
`XREADGROUP` or `XACK`, so after a rollback to it every consumer would fail
until the roll forward. Rollback must work (Jeff, 2026-09-23). For a queue,
"work" means the consumers keep consuming.

- **Release R** serves every group command on groups that exist. `XGROUP
  CREATE` (and `MKSTREAM` through it) creates a group only on a seat
  started with `--stream-groups` (inventory: `stream-groups on`), off by
  default. Otherwise it answers `ERR consumer groups are not enabled here
  yet: this deployment creates none (ADR-0057)`, the same shape as streams'
  refusal. Ops declares the inventory key, as it did for `streams on`.
- **R+1 turns creation on,** once R is the oldest release anywhere. A
  rollback from R+1 to R keeps every group working. It only stops new
  groups being created.
- **Groups need streams.** A seat creates a group only on a stream, and
  creates a stream only with `streams on` (ADR-0052 D6), which is due to
  become the default in the release after rc.82. That release is the
  earliest R can be.
- **Turning groups on sooner has a price.** An operator who does so gives up
  rolling back below R once a tenant has a group. Below R, the entries stay
  readable and the groups survive untouched, but no consumer can read or
  acknowledge until the roll forward.

### D6. Build order, each part with its own gate

1. Storage and the core loop: `XGROUP`, non-blocking `XREADGROUP`, `XACK`,
   `XPENDING`, and `XINFO GROUPS` and `CONSUMERS`.
2. `XCLAIM`, `XAUTOCLAIM`, `XINFO STREAM` (with `FULL`), `XSETID`, and a
   blocking `XREADGROUP` at the proxy (`FLINTXREADGROUP`).
3. D7's drills, and the documentation: command-support.md, retry-safety.md
   and self-hosting.md (the new flag).

About five to eight working days in all.

### D7. Verification (if accepted)

- **Differential against Valkey 9.1:**
  - a probe of every command, in RESP2 and RESP3;
  - a random differential over the group commands, with several seeds and
    both protocols.

  Where replies carry times, the times are pinned with `XCLAIM`'s `TIME`
  and `IDLE`, and idle fields are compared within a tolerance.
- **A failover drill.** Producers run `XADD`, and consumers run
  `XREADGROUP` and `XACK`, while the master is killed mid-run. Afterwards:
  - every acknowledged `XADD` is either acknowledged or pending;
  - no acknowledged `XACK` is pending again;
  - `>` does not deliver again anything that `XREADGROUP` already delivered
    to a client.
- **A rollback drill.** Roll back from R+1 to R with groups and live
  consumers, and the consumers keep consuming. Roll back from R to rc.82,
  and the entries stay readable and the groups intact. The roll forward
  restores the groups.
- **A blocked consumer through an authenticated proxy:**
  - a wake on `XADD`, a timeout, and `BLOCK 0`;
  - the group destroyed, and the key deleted, while it waits;
  - a failover while it waits.

  `XINFO CONSUMERS`' idle time grows during the block. The master's WAL
  sequence does not move while a consumer waits, which shows that misses
  write nothing.
- **Clients:**
  - redis-py's group API, in `client_compat_drill`;
  - one framework from Dapr, Watermill, Spring Data Redis and Redpanda
    Connect. Each is a download that needs Jeff's approval.
- **Cost:** `XREADGROUP COUNT 10` and an `XACK` per entry, with p50, p99 and
  WAL bytes per entry, against Valkey on the same laptop.

## What this does not decide

- **Redis 8.2's additions** (`XACKDEL`, `XDELEX`, `KEEPREF`, `DELREF`,
  `ACKED`). Revisit them if Valkey adopts them or a tenant needs them.
- **Seat-side wake-ups,** and first-come order among blocked consumers
  (D4's (b)).
- **Which release is R.** That follows from D5 and the release calendar.

## As built

### Part 1: storage and the core loop (2026-10-12)

- **Storage** (`flint-storage`, `streams/groups.rs`): D2's four row kinds.
  - The group row holds the last delivered ID, entries read, and its pending
    and consumer counts.
  - A pending row holds the delivery time, the delivery count and the
    consumer.
  - A consumer row holds the seen time, the active time and a pending count.
  - A consumer's own pending row is empty.
  - The metadata row is never written for a group.
- **Seat:** `XGROUP` (every subcommand), `XREADGROUP`, `XACK`, `XPENDING`,
  and `XINFO GROUPS`, `CONSUMERS` and `HELP`. A seat answers `XREADGROUP`
  at once, `BLOCK` or not. `XREAD` and `XREADGROUP` share one option parser,
  as in Valkey, so `XREAD` now answers Valkey's own errors for `GROUP`,
  `NOACK` and `>`.
- **The flag:** `--stream-groups` (inventory: `stream-groups on`).
  - It refuses only the creation of a new group. An `XGROUP CREATE` of an
    existing group answers `BUSYGROUP` with the flag off too, so consumers
    that create their group when they start keep working after D5's rollback.
  - `MKSTREAM`'s stream also waits for `--streams`, as XADD's does.
- **Proxy:**
  - It routes by the keys after `STREAMS` and after a subcommand.
  - It puts back the null arrays that RESP2 has where RESP3 has its one null:
    an entry deleted since delivery, in a history read, and XPENDING's
    summary with nothing pending.
  - It rebuilds XREADGROUP's map as RESP2's pairs, inside `EXEC` too.
- **Quota gates:** `XACK` counts as reducing space, so a queue over its
  quota can still drain.

**Valkey 9.1's lag rules are newer than Redis 7.0's.** The random
differential found this, and each rule below was then measured on its own,
over entries 1 to 5:

1. **A deletion makes a group's entries-read unknown even after a trim has
   passed it.** After `XDEL 2-0` and `XTRIM MAXLEN 2`, a group at `0-0` with
   ENTRIESREAD 0 lags 2, not 5.
2. **The count of an ID behind a deletion cannot be estimated, except at
   the start of the stream.** A group at `1-0` lags nil.
3. **A read carries the count forward only from a position the stream
   still holds, with no deletion after it. Otherwise the count is
   estimated again.**
   - At `2-0` with ENTRIESREAD 2, after `XTRIM MAXLEN 2`, reading `4-0`
     gives 4, not 3.
   - At `1-0` with ENTRIESREAD 1, after `XDEL 2-0`, reading `3-0` gives
     nil.

**Verified** against Valkey 9.1 on the laptop:
- **A scenario of 231 commands, and one of 60 on the order of errors,** are
  identical at a seat in both protocols.
- **The corpus gained four cases,** generated from Valkey's replies. They
  pass at a seat, and through the proxy as a tenant in both protocols
  (`proxy_conformance_drill`: 218 cases direct, 215 through the proxy).
- **A random differential** of 100,000 commands at a seat (10 seeds) and
  36,000 through an authenticated proxy (6 seeds), in both protocols, finds
  no difference. It compares every reply with time fields masked.
- **A master and its replica** hold identical groups, pending entries and
  consumers.
- **Ten mutants failed their tests:** nine in the storage, the seat and the
  key parser, and one that dropped the proxy's RESP2 repair.

### Part 2: claiming, `XSETID`, `XINFO STREAM`, and the wait (2026-10-12)

- **Seat:**
  - `XCLAIM`, with `IDLE`, `TIME`, `RETRYCOUNT`, `FORCE`, `JUSTID` and
    `LASTID`.
  - `XAUTOCLAIM`, with `COUNT` and `JUSTID`. It answers the IDs of entries
    gone from the stream, and drops their pending rows. As in Valkey, one
    call looks at no more than ten times `COUNT` pending entries, and
    answers a cursor to resume from.
  - `XSETID`, with `ENTRIESADDED` and `MAXDELETEDID`.
  - `XINFO STREAM`, with `FULL` and `COUNT`.
- **`XINFO STREAM`'s `radix-tree-keys` and `radix-tree-nodes`** count rows,
  the stream's length. Flint has no radix tree to describe
  (command-support.md).
- **D4, the wait:**
  - The proxy sends a blocked `XREADGROUP` once as the client sent it. That
    first attempt makes the consumer and stamps when it was seen, as Valkey
    does once for a blocked client.
  - Then the proxy polls with `FLINTXREADGROUP`, on the same backoff as
    `XREAD` (1 ms, rising to 20 ms). A poll that finds nothing writes
    nothing: no WAL record, no replication, no consumer.
  - A tenant cannot send `FLINTXREADGROUP`, because the proxy refuses every
    `FLINT*` command.
  - A blocked `XREADGROUP` is left out of staging and of the latency
    histogram, as a blocked `XREAD` is.
- **Destroying the group or deleting the stream during the wait** answers
  `NOGROUP`, with the same text as Valkey 9.1 (measured, both cases).

**Verified** against Valkey 9.1 on the laptop:
- **A scenario of 123 commands** is identical at a seat in both protocols.
- **The corpus gained three cases.**
  - Two were generated from Valkey's replies.
  - One is seat-only: `FLINTXREADGROUP` makes no consumer on a miss, and
    delivers on a hit.
  - The corpus passes at a seat, and through the proxy as a tenant in both
    protocols (`proxy_conformance_drill`: 221 cases direct, 217 through the
    proxy).
- **The random differential gained part 2's commands.** It runs 60,000
  commands at a seat and 24,000 through an authenticated proxy, and finds
  no difference.
- **The wait, through the proxy, in both protocols:**
  - an entry wakes the waiter;
  - the master's WAL does not move while a consumer waits;
  - `XINFO CONSUMERS` shows the waiter idle for at least a second;
  - `BLOCK` times out with a null;
  - the two `NOGROUP` cases above;
  - a history read answers at once;
  - one entry goes to exactly one of two waiters.
- **Two mutants failed:**
  - a proxy that polled with `XREADGROUP` itself (the WAL check caught it);
  - a poll that wrote on a miss (the seat-only case caught it).

### Part 3: D7's drills (2026-10-12)

- **`stream_groups_failover_drill`** (in the gate): a producer and a consumer
  run through the proxy across a planned failover and a -9 kill of a master
  whose replica has caught up. Those are the two shapes in which failover.md
  promises that no acknowledged write is lost. An unplanned kill mid-stream
  can lose the async tail, for a group's rows as for any key, within the RPO.
  - Every acknowledged `XADD` is in the stream and was delivered. Each is
    acknowledged or still pending.
  - No acknowledged `XACK` is pending again.
  - `>` delivered nothing twice.
  - A consumer blocked with `BLOCK 0` across the planned failover woke for
    the entry added after it.
  - Locally: 2,000 to 2,400 entries a run over four runs, all delivered
    and acknowledged, with no client error. A seat whose `XACK` forgot nothing failed
    invariant 4.
- **`stream_groups_rollback_drill`** (by hand, with
  `FLINT_PREV_SERVER`): this build stores a stream with two groups, pending
  entries and a consumer.
  - rc.82, built from its tag, opens the same data. The 10 entries read back
    and no group row appears as an entry. `XREADGROUP` is refused and `XADD`
    works.
  - This build opens it again, and the pending entries and both groups are
    as they were. `>` delivers rc.82's entry to each group.
  - It passes against rc.82. Given this build as the previous release, it
    refuses to run.
  - It is out of the gate because it needs a second build (gates.sh,
    `EXCLUDED`).
- **`client_compat_drill`** gains redis-py's group API in both protocols:
  `XGROUP`, `XREADGROUP`, `XACK`, `XPENDING`, `XCLAIM`, `XAUTOCLAIM` and
  `XINFO`. It also gains a blocking `XREADGROUP` that:
  - wakes for an entry;
  - shows its consumer idle for a second while it waits;
  - times out;
  - ends with `NOGROUP` when its group is destroyed.

  The same checks pass against Valkey 9.1.
- **Cost:** one run on the laptop, 20,000 entries of 64 bytes, one client,
  direct to each server. The rocks seat is against Valkey 9.1 with
  `appendonly yes`, `appendfsync everysec`:

  | | Flint | Valkey |
  |---|---|---|
  | `XREADGROUP COUNT 10`, p50 / p99 | 166 / 305 µs | 111 / 194 µs |
  | `XACK` of one entry, p50 / p99 | 49 / 110 µs | 28 / 70 µs |
  | log bytes per entry read and acknowledged | 429 | 246 |

  A read writes the group's row, the consumer's row, and a pending row and
  an owned-index row per entry. An `XACK` deletes those two and rewrites
  the group's and the consumer's counts.

**A framework's consumer (2026-10-12): Redpanda Connect 4.113.0**, approved
by Jeff and downloaded with its published sha256 (laptop only, deleted
after). Its `redis_streams` input is go-redis 9.18.0 speaking RESP3. It
sends `HELLO 3`, `CLIENT SETINFO`, `CLIENT SETNAME`, `XGROUP CREATE … 0
MKSTREAM`, `XREADGROUP … COUNT 10 BLOCK 500`, first from `0` (its own
pending entries) and then from `>`, and batched `XACK`s. Its output adds
with `XADD`.

The same scenario ran against Valkey 9.1 and against a local fleet through
the proxy, and the two summaries were identical:
- **200 entries** were read once each and acknowledged. The group's
  pending count was 0, `entries-read` 200 and lag 0.
- **A consumer killed with `kill -9`** before committing left at least
  what it had output pending. Restarted under the same name, it got those
  entries back. The union was all 100, and nothing was left pending.

Flint only, the case this ADR is for: a consumer was killed with 50
entries pending (40 output, 10 in flight). Then `flintctl failover` moved
the pair's master, and the group still had 50 pending. Restarted, the
consumer received all 40 again, and the union was 100. The group ended
with pending 0, `entries-read` 500 and lag 0. The client logged no error
besides the "context canceled" of a blocking read stopped at shutdown,
which Valkey's runs log too.

Dapr, Watermill and Spring Data Redis were not run. One framework was
what this verification asked for.
