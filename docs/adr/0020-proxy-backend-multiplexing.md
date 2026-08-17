# ADR-0020: Multiplex the proxy's backend hop — decouple send from receive so a pipeline survives it

Status: PROPOSED — August 2026. Written after a measured failure: the obvious
fix was built, benchmarked, and reverted (`1decd25`, reverted in `1c45e72`),
and the reason it failed is the thing this ADR changes.

> Numbering continues the shared sequence (see the ADR README). The proxy lives
> in the public repo, so this ADR does too; 0020 is the next free number.

## Context

### What we measured

Off-box, same AZ, 20 M × 1 KB, 32 connections (`docs/bench/` in the private
repo, 2026-08-16):

| scenario | Flint p50 | ElastiCache p50 | gap |
|---|---|---|---|
| hot GET | 0.303 ms | 0.119 ms | 2.55× |
| GET pipelined ×16 | 4.735 ms | 0.415 ms | **11.4×** |

The unpipelined gap is a per-command CPU story and is **not** what this ADR
addresses. The pipelined gap is the anomaly: pipelining exists to amortise
per-request overhead, and per-command cost `(pipe×16 avg)/16` amortised

- **1.13–1.59×** for Flint,
- **4.57–4.72×** for ElastiCache.

Ours barely amortised at all.

### The fix that did not work, and why

`1decd25` gathered a run of pipelined reads out of one client's read buffer and
issued them as a single backend exchange. Measured A/B on one fleet, both
proxies built from source on the box, one commit apart, alternating legs:

```
settled store (reps 2-3)      old 38a1bb6        new 1decd25
  throughput                    142,560 ops/s      147,522 ops/s   1.035x
  p50                             3.599 ms           3.343 ms      1.077x
  p99.9                           7.567 ms          13.215 ms      0.57x  <- WORSE
```

Two things went wrong, and the second one is this ADR's subject.

**It was speculative work in front of every command.** `try_pipelined_reads`
ran before the main `decode`, paying a full decode, `frame_to_args`, two
uppercase allocations, `family_registered` (a lock) and `topo.route` (two more
locks plus two `ns.to_vec()`) — then returning `None` and letting the ordinary
path repeat all of it. Its own comment claimed it had "done nothing at all";
side-effect free is not free. On a box already at 0 % idle that doubled
per-command proxy CPU, which is the p99.9 regression.

**It batched at a layer where there was nothing to batch.** This is the
important one:

```rust
fn serve_client(...) {
    let mut backends: Option<Backends> = None;   // a LOCAL, per client connection
```

Every client connection owns its own `Backends`, hence its own sockets, and
`call_raw` writes one frame and blocks for its reply. So a backend connection
carries **at most one in-flight request**. A pipelining client keeps N requests
outstanding but runs a sliding window after warmup — one reply in, one request
out — so each connection has ~1 command buffered at any instant. The commands
were there; they were spread across the other 31 client connections and their
31 separate sockets, where no per-connection batcher could reach them.

### What the prior art does

**Redis** (`src/networking.c`) — `processInputBuffer` loops over every complete
command in `c->querybuf`, replies accumulate in `c->buf` then the `c->reply`
list, and the socket write happens once per event-loop iteration via
`handleClientsWithPendingWrites`: *"we just flag the client and put it into a
list of clients that have something to write to the socket."*

**`flint-server` and this proxy's client-facing side already have that shape.**
Redis is a server with no backend hop, so it has nothing to say about the leg
we got wrong — which is why reading only Redis would have missed this.

**twemproxy** is the relevant reference, and its README states the mechanism
outright:

> "twemproxy would try to batch these requests and send them as a single
> message onto the server connection"
> "**pipelining is the reason why twemproxy ends up doing better in terms of
> throughput**"

with `server_connections` defaulting to **1** — *"By default, we open at most 1
server connection."* It batches **across client connections onto a shared
backend connection**. That is the pool we do not have.

### The cost we are also paying

- 32 client connections open **32 backend sockets and 32 mTLS sessions** to one
  node, where twemproxy would use one.
- Sampled during the bench: `flint-proxy` at **~2.1 of 8 cores (~30 % of the
  box)**, 0.0 % idle, **0.0 % iowait** with the corpus page-cached — CPU-bound,
  disk not in the loop.
- Of a 303 µs client-observed GET, ~104 µs is wire and ~170 µs is proxy and
  server framing; the storage engine is ~23–32 µs.

## Decision

**Make a backend connection a shared, multiplexed resource: a small pool per
`(node address, namespace)`, with send decoupled from receive, and replies
correlated by position.**

Concretely:

1. **A pool, not a per-client socket.** `Backends` stops being a local in
   `serve_client` and becomes a process-wide pool keyed by `(addr, ns)`.
   Connections are pinned to a namespace by the `FLINTNS` handshake at open, so
   the namespace must be part of the key or the handshake must be re-run.

2. **Send decoupled from receive.** Each pooled connection gets a writer and a
   reader half plus an **in-order pending queue**. A client thread encodes its
   frame, pushes `(reply-slot)` onto that connection's queue, writes the frame,
   and waits on its slot. A reader dispatches each decoded reply to the head of
   the queue.

3. **No request IDs.** RESP replies arrive on a connection in request order, so
   position is the correlation. This is the same reason twemproxy needs none,
   and the same invariant `call_raw_batch` relied on — it was correct there and
   is correct here.

4. **Batching becomes emergent.** With many clients sharing few connections,
   several requests are in flight naturally; the writer may coalesce whatever is
   queued into one `write`. No speculative look-ahead in the read loop, which is
   exactly the mistake being reverted.

## What must not break

- **Transactions keep a private connection.** `MULTI`/`WATCH` is connection
  state on the node; a queued command that lands on a different connection
  executes instead of queueing, and the client sees `QUEUED` then a partial
  apply. `ProxyTxn` already pins an address — it must become an explicit
  **checkout** of a dedicated connection for the transaction's lifetime, and
  `abort_txn`'s "drop the socket" guarantee must survive pooling.
- **The O(keys) admin class stays off the shared pool.** `call_slow`
  (`DBSIZE`, `FLUSHALL`, `SCAN`'s per-master step) runs on a 60 s budget; on a
  shared connection it would head-of-line block every ordinary GET behind it.
- **Co-processor channels** (ADR-0010) carry a per-command deadline and budget
  and are connection-scoped; they check out a dedicated connection too.
- **The async write queue** (D4) pins backend connections with an `'a'`
  handshake flag, so pooling must key on that flag or keep those connections
  separate.
- **Failure semantics widen.** A broken connection currently fails one request;
  pooled it fails every in-flight request on that connection. All of them must
  fail deterministically and the connection must be dropped — the reasoning in
  `call_raw_batch`'s doc comment (a half-read reply desynchronises the stream)
  applies with more force.

## Consequences

**Expected gains.** Pipelining amortises the internal hop for real; backend
sockets and mTLS sessions drop from per-client to pool-size, cutting CPU on
both proxy and node — which matters because the proxy is ~30 % of the box.

**New failure mode: head-of-line blocking.** One slow command delays everything
behind it on that connection. Mitigations: pool size > 1, the admin class
excluded above, and a per-connection in-flight cap. This is a real trade and
should be measured, not assumed away.

**Concurrency model change in the most safety-critical component.** The proxy
is the tenant boundary. This touches how every request reaches a node.

## Alternatives considered

1. **In-buffer batching** — built as `1decd25`, measured, reverted. Wrong
   layer: it can only see one client's buffer, and that buffer holds ~1 command.
2. **Full async runtime (tokio) for the proxy.** The largest change and the
   most disruption to a component with working failover, tenancy and TLS paths.
   Multiplexing gets most of the benefit inside the existing thread model; async
   remains available later if profiles justify it.
3. **Do nothing.** The 11.4× pipelined gap stands. Defensible only if no
   customer pipelines — and pipelining is standard practice for bulk reads, so
   this is a bet against normal client behaviour.

## Verification

The harness already exists and is debugged
(`packaging/aws/pipeline-bench/run.sh`, private repo): one fleet, one node, one
corpus, both proxies built from source one commit apart, alternating legs, with
an md5 control proving the two arms differ and a probe that asserts ops
**actually served**.

Gates for accepting the change:

1. **Pipelined p50 improves materially** — the amortisation ratio moves off
   1.1–1.6× toward the 4.6× a RAM cache achieves.
2. **No p99.9 regression.** The reverted attempt failed here; head-of-line
   blocking makes it the risk to watch.
3. **Instrumentation ships with it**, not after: in-flight depth per connection,
   pool utilisation, head-of-line wait, and coalesced-write sizes. The reverted
   change shipped no counters, so its null result could not be explained without
   re-reading the code — that is not repeatable.
4. **The existing proxy drills pass unchanged.** `proxy_drill`,
   `proxy_cache_drill`, `proxy_tls_drill`, `proxy_chaos_drill`,
   `proxy_backpressure_drill`, `admin_gated_proxy_drill`,
   `proxy_registry_drill` are the regression suite for this refactor, exactly as
   the 19 core drills were for the remote runner.
5. **Discard the first leg after any fill.** The A/B that produced the numbers
   above was nearly misread because the control arm ran straight after the
   20 M-key load and paid compaction debt, scoring the tax as the treatment's
   win. Three reps per arm minimum, first leg quarantined.

## What this ADR does not claim

It does not address the **unpipelined** 2.55× gap. That is per-command CPU in
the proxy and server path — RESP decode and encode twice over, tenant auth,
routing, near-cache lookup, mTLS on the internal hop — and multiplexing does not
remove any of it. On a RAM-resident corpus the storage engine is roughly a tenth
of a request; the rest is what we built around it, and that is a separate piece
of work.
