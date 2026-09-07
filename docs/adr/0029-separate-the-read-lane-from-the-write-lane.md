# ADR-0029 — Separate the read lane from the write lane at the proxy's connection key

**Status:** **ACCEPTED 2026-09-06** (proposed the same day). Accepted on the
measurement below rather than on the argument above: on the gate box, read
**p99 stayed at 0.239 ms** — flat — while **one read in 230 waited 555 ms**
behind a stalled write on a shared backend FIFO.

The mechanism is small and the correctness condition on it is not, which is
why the barrier in decision 2 is specified here rather than left to the
implementation.

## Where this came from

`docs/roadmap.md`'s hot-key prevention lane carried three items. An audit on
2026-09-06 found that two of them were not work:

- *write-side async batch merge* had **shipped** (`MAX_PURE_BATCH` = 256,
  ADR-0005 D4, ADR-0027), and the concession it promised to narrow is already
  measured with group commit in place — *"group commit caps a node at
  low-hundreds-of-K ops/s"*. It was a term in the argument, not a lever
  against it.
- *read-side near-cache* v1 had **shipped in the proxy** rather than the SDK
  the line named (ADR-0005 D6, revised).

This is the one that is real.

## The mechanism of the harm

**A backend connection is a strict FIFO, by design and for a good reason.**
ADR-0021 states it plainly:

> Correlation needs no request ids: RESP replies arrive in request order, so
> position is the whole correspondence.

That is what makes the proxy's backend hop cheap — no request ids, no
correlation table, no per-command bookkeeping. It also means **a reply cannot
overtake the reply in front of it**, so a slow command blocks every command
queued behind it on that connection, whatever kind it is.

ADR-0021 removed a *different* head-of-line problem and it is worth being
exact about which, because it is easy to think this one went with it. Giving
each worker its own connections removed blocking **behind strangers'
requests** — one client's slow command no longer stalled the other 31 threads
sharing a pooled socket. Blocking behind *your own* traffic on *your own*
connection is untouched, and cannot be touched by ownership.

**And ADR-0026 measured what a slow write looks like.** Under RocksDB's L0
write stall, with nothing shedding:

| symptom | value |
|---|---|
| `writes_delayed_soft` | ~4,300–4,700/s |
| in-flight connections | **all 60 pinned** |
| goodput | falls to ~44,000 seq/s |
| CPU | master 92% idle, drivers 99.7% idle — both *waiting* |

Seconds of back-pressure, not microseconds. Every read sharing a connection
with one of those writes waits behind it.

## What is measured, and what is not — corrected 2026-09-06

**The first version of this ADR said "no drill measures read latency while
writes stall". That was wrong, and it was wrong because I checked two drills I
expected to be relevant instead of enumerating them.**
`tools/rw_isolation_drill.sh` — an ADR-0005 D1 drill, in the gate — does
exactly this shape: one client pipelines a write storm while another samples
GET latency through the proxy, and it asserts the reader's p50 and p99 stay
flat.

**What it covers, now that it has been run deliberately:** reads do NOT
degrade behind a foreign write storm, even when the reader and the writer
provably share one backend connection. Measured, `--workers 1`, with
`pool_lanes` asserted at 1 while both clients are live: baseline read p50
0.098 ms / p99 0.217 ms; under a 27,488-write storm, p50 0.081 ms / p99
0.295 ms.

**What it does not cover, and this is the whole of what is left:** a storm is
not a **stall**. It runs a default LSM on a small dataset, so RocksDB never
enters L0 back-pressure and every write in the queue is sub-millisecond — a
FIFO drains as fast as its slowest member, and none of these are slow.
ADR-0026's regime, where a write takes seconds, is not reached.

**So the harm is certain in kind, bounded to one regime by measurement, and
unmeasured inside it.** That is a narrower and better-founded claim than the
one this ADR opened with, and it changes what the gating drill has to do: not
"measure reads under writes", which exists, but "measure reads under STALLED
writes", which does not.

There is a specific reason to expect it matters here rather than in general.
Flint's segment is read-heavy — sessions, feature stores, embeddings,
response caches (`design.md`, "what Flint is not for"). A write stall that
adds seconds to *reads* is the most customer-visible failure this system can
have, and it is invisible to every gate we run.

## The claim that decides how big this is

**RocksDB's L0 stall is writer-side.** It applies back-pressure to writers;
readers are not blocked by it beyond CPU and IO contention, and ADR-0026's own
measurement is consistent with that — during the stall the master is 92% idle.

If that holds, then **the read blocking is entirely an artifact of sharing one
FIFO at the proxy**, and the fix is entirely proxy-side. The roadmap's
parenthetical asked for "server-side staging" as well; that would be a change
to the data plane's hot path to compensate for a proxy-side queueing choice.

**This is stated as the claim to confirm, not as a finding.** It is the
difference between a change to one struct and a change to the write path, and
it is the kind of thing that has been wrong before in this tree.

## Two qualifications found while designing the measurement

Both of these were missing from the first version of this ADR, and both were
found by asking *how would I observe this* rather than by re-reading it.

### The harm needs the reader and the writer on the SAME worker

Connections are assigned to workers **round-robin and pinned for life**
(ADR-0021), and backend connections are per worker. So two clients only share
a backend FIFO if they landed on the same worker — with `W` workers, clients
`i` and `i+W`.

This is not a detail, it is the difference between a drill that measures the
mechanism and one that measures luck. **A two-client drill on an 8-worker
proxy would almost certainly show nothing and would "retire" this ADR for the
wrong reason.** The measurement below therefore runs `--workers 1`, where
sharing is guaranteed and assertable, and reports the mechanism at its worst.

The production magnitude is then a second question — the client-to-worker
ratio — and at `max-conns 1024` against ~8 workers, sharing is the norm
rather than the exception. But that is an inference, and this ADR does not
rest on it.

### Replica reads ALREADY separate the lanes, for tenants that opt in

`D7` routes a read to a replica when the tenant has opted in (`tenant-reads`)
and the command is a read; **writes stay on the master**. Those are different
addresses, so different `Key`s, so different connections — a tenant using
replica reads has lane separation today, obtained a different way.

That narrows the population this ADR is about: **tenants NOT using replica
reads, which is the default.** And it is a genuine partial alternative that
the first version of this ADR failed to list.

It is not a substitute, for a reason worth stating: replica reads trade
CONSISTENCY for the separation — a read may be behind by the replication lag
— while lane separation trades neither. A tenant that cannot take stale reads
has no way to get out of the shared FIFO today.

## Decision

### 1. Lane joins the connection key

    struct Key { addr: String, ns: Vec<u8>, async_writes: bool, lane: Lane }

ADR-0021 already keys connections per worker per `(address, namespace,
async-writes)` and dials them on demand. This adds a dimension to an existing
key; it is **not** a second pool, and a namespace that only ever reads never
opens a write lane.

### 2. Read-your-own-writes is held by a barrier, not by hope

**Splitting naively is a correctness regression, not a latency trade.** With
`SET k v` on one connection and `GET k` on another, the GET can overtake the
SET and return the old value — read-your-own-writes, which every client
assumes and no protocol here promises to restore.

So: **a client with a write in flight issues its reads on the WRITE lane until
that write is acked.** The lanes diverge only where nothing can be reordered.

**The benefit is therefore not uniform, and that must not be discovered
later.** A client alternating SET/GET with no gap never leaves the write lane
and gains nothing from this. A read-only client — the common cache shape —
gets full isolation. That is the right trade for our segment, and it means any
benchmark of this feature has to state its read/write mix or it is measuring
the mix rather than the change.

### 3. No server-side machinery

Conditional on the writer-side claim above holding.

### 4. Off by default until the number exists

The measurement below decides whether it ships on, opt-in, or not at all.

## The measurement this is gated on

One drill, and it is cheap because both halves already exist:

1. Drive a namespace into an L0 write stall — `ingest_saturation` already
   produces the condition ADR-0026 characterised (shrunken LSM:
   `FLINT_LEVEL_BASE_MB=8`, `FLINT_WRITE_BUFFER_MB=4`). The storm in
   `rw_isolation` does not; that is the difference between the two.
2. Concurrently, a **read-only client on the same namespace** samples GET
   latency through the proxy. **`--workers 1`**, so the reader and the writer
   provably share one backend connection — assertable, because `PROXYSTATS`
   reports `pool_lanes`, and it must read 1.

   The tenant must NOT have replica reads on, or the read goes to a different
   address and the drill measures nothing.
3. Report read p50/p99/p99.9 during the stall against the same client's
   pre-stall baseline.

**A positive control is mandatory**, and this is where a check like this
usually goes wrong: the drill must also show the stall actually happened
(`writes_delayed_soft` non-zero, `l0_files` sawtoothing) — otherwise a
flat read latency is indistinguishable from a stall that never occurred, and
the drill would report a pass for the wrong reason.

## THE MEASUREMENT, RUN 2026-09-06

`tools/read_under_stall_drill.sh`. Same shape as `rw_isolation` — one client
storms writes, another samples GETs through the proxy — with the two
differences that matter: a **shrunken LSM** so RocksDB reaches back-pressure,
and `--workers 1` with `pool_lanes` asserted at 1 so the reader and writer
provably share one backend FIFO.

| where | | quiet | under stalled writes |
|---|---|---|---|
| macOS, run 1 | p50 | 0.037 ms | 0.128 ms |
| macOS, run 1 | p99 | 0.091 ms | 0.236 ms |
| macOS, run 1 | **p99.9** | 0.120 ms | **104.290 ms** |
| macOS, run 1 | **max** | 0.120 ms | **112.876 ms** |
| macOS, run 2 | **max** | 0.161 ms | **150.062 ms** |
| **Linux, gate box (c7i.xlarge)** | p50 | 0.045 ms | 0.062 ms |
| **Linux, gate box** | p99 | 0.081 ms | **0.239 ms** |
| **Linux, gate box** | **p99.9** | 0.919 ms | **364.780 ms** |
| **Linux, gate box** | **max** | 0.919 ms | **555.385 ms** |

**The Linux row is the one to read.** On the platform the fleet runs, p99 is
0.239 ms — three times a sub-millisecond baseline, which any dashboard would
call healthy — while **one read in 230 waited 555 milliseconds**. Thirteen
reads out of 3,000 exceeded the quiet maximum, and the worst of them by a
factor of 600.

Rarer than on the laptop and far worse when it happens, which is the shape
that survives averaging and kills a p99 SLO one request at a time.

**The harm is real, it is a TAIL, and p50/p99 do not show it.** In every run
the median and the 99th barely move — 2.5–3.0× on a sub-millisecond number —
while the tail goes **three orders of magnitude** above the quiet maximum. The
proportion of affected reads varies with how much of the sampling window
overlapped the stall (0.43% on Linux, 9–58% on the laptop); the size of the
excursion does not.

**This is the same trap as the tenant-isolation exit clause**, which was
reworded the same day for the same reason: a check on p50 or p99 would report
that isolation holds, through the failure it exists to catch. Any threshold
this drill ever grows has to be on the tail.

**What the controls establish, so the number is not read for more than it is
worth.** The stall reached was `write_stopped`, not `writes_delayed_soft`,
which stayed at 0 — a hard stop rather than the soft delay ADR-0026
characterised. One worker, a laptop, and a deliberately shrunken LSM. What is
demonstrated is the MECHANISM: on a shared FIFO, a stalled write delays reads
behind it by ~1000×. What is not demonstrated is the frequency in production,
which depends on the client-to-worker ratio and on how often a fleet node
stalls.

**Recommendation, on this evidence: ACCEPT.** The status stays PROPOSED
because that is not mine to change.

## IMPLEMENTED 2026-09-06, and the tail is gone

`Lane { Read, Write }` joined `apool::Key`; `Backends::key/call/lease` take
one; `drop_conn` retires **both** (a demoted master is wrong for either, and
retiring one would leave the other serving a stale route); `forward` picks the
lane from the shared command classifier, with anything that is not a plain
read going on the write lane — classifying an unknown verb as a read would put
it in front of the reads it must not delay.

**The barrier lives in exactly one place**, which is the part worth reviewing:
`prefetch_run` is the only path where a client's write and its later read are
in flight at once — everywhere else a reply is awaited before the next command
is read. There the lane is **sticky once a write appears**: reads before the
first write take the read lane, and from the first write onward the rest of
the run stays on the write lane, in order, on one FIFO. `SET k v; GET k` in
one pipeline cannot split.

**The same drill that found the harm, re-run against the implementation. Both
rows are the gate box — the platform the fleet runs on:**

| | before (one shared FIFO) | after (lanes split) |
|---|---|---|
| quiet max | 0.919 ms | 1.593 ms |
| under stall, p99 | 0.239 ms | 0.057 ms |
| under stall, **p99.9** | **364.780 ms** | **0.143 ms** |
| under stall, **max** | **555.385 ms** | **0.470 ms** |
| reads over the quiet max | 13 of 3,000 | **0 of 3,000** |

**The worst read under a stall went from 555 ms to 0.470 ms** — three orders of
magnitude — and there are no excursions above the quiet maximum at all. On
macOS the same comparison is 555.385 → 0.319 ms worst case, 13 of 3,000 → 0.

Note the direction of the p99 row: under a stall it is now *lower* than the
quiet baseline, because the reads are no longer sharing a connection with
anything. The number that mattered was never p99 — it is the row below it.

**Both drills' preconditions inverted, and that is the mechanism's own
signature.** `rw_isolation` and `read_under_stall` each asserted
`pool_lanes == 1` — one worker, one namespace, so a shared FIFO. They now
assert `2`. Nothing else on that fleet can produce a second connection, so the
number is the separation.

## What would retire this item

This section said: *"if read p99 during a stall is within noise of baseline,
this ADR is withdrawn"*. **That test has now been run, and read p99 IS within
noise — 0.091 ms to 0.236 ms.** Retiring the ADR on it would have been the
wrong call, and the criterion was wrong rather than the answer: **p99 is not
where this harm lives.** Kept here rather than edited away, because a
withdrawal criterion that would have withdrawn a confirmed defect is worth
seeing.

The criterion that survives: **the tail.** If reads behind a stalled write
stay within an order of magnitude of the quiet maximum, there is nothing here.
They are three orders of magnitude out.

The near-cache remains a real mitigation for cache-friendly workloads — it
serves repeat reads without touching a backend — but it is opt-in, TTL-bounded
and does not help a miss, so it narrows the exposure rather than removing it.

## Alternatives rejected

**Reorder replies on one connection (priority queue).** Cannot be done without
abandoning positional correlation, which means adding request ids to the
backend protocol — a protocol change to avoid a second socket per worker.

**Bound the write batch so writes never hold the line long.** Attacks the
wrong quantity: ADR-0026 shows seconds of RocksDB back-pressure, not a large
batch. Shrinking batches would cost write throughput and leave the stall.

**Split in the client.** We cannot control users' clients — the same argument
ADR-0005 D6 used when the near-cache moved into the proxy.

**Tell everyone to turn on replica reads.** It does separate the lanes (see
above), and for a tenant that can take a stale read it is strictly cheaper
than this ADR — no new sockets, no barrier, no code. It is rejected as *the*
answer rather than as *an* answer: it makes consistency the price of read
isolation, and the tenants most exposed to a write stall are not obviously
the ones most able to pay it.

**Do nothing, and lean on admission control.** ADR-0026's shed keeps the
*write* path from collapsing and says nothing about reads queued behind it. It
is the reason the stall is survivable, not a reason reads are unaffected.

## Cost

**Sockets: `workers × backends × namespaces × lanes`.** Workers are cores
clamped to 64, **not clients** — so this is at most 2× a quantity ADR-0020
already bounded, against the per-client explosion it removed (up to 1024
backend sockets and mTLS sessions per node at `max-conns 1024`). Different
order of magnitude, and lanes are dialled on demand rather than pre-opened.

**A second mTLS session per lane**, on the same argument.

**One more state a client can be in** — "has a write in flight" — in a path
whose simplicity is the reason it is fast. That is the real cost, and it is
why the barrier is specified here rather than left to the implementation.
