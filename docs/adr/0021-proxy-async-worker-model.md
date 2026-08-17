# ADR-0021: Give the proxy bounded worker threads and async IO

Status: PROPOSED — August 2026. Written after ADR-0020's pooled backend
multiplexing was measured on a fleet and found to collapse under
concurrency. This ADR changes the concurrency model that made the collapse
structural.

## Context

### What ADR-0020 built, and what it measured

ADR-0020 replaced per-client backend sockets with a shared, full-duplex pool,
then added command staging so a client's pipeline reaches the node as a
pipeline. Measured on a fleet (2 x `i4i.2xlarge`, 20 M x 1 KB, pipeline 16,
memtier), staged against its parent:

| client conns | old ops/s | new ops/s | new p99.9 |
|---|---|---|---|
| 4 | 60,352 | 121,785 | 0.99 ms |
| 8 | 92,008 | 186,735 | 1.27 ms |
| 16 | 133,353 | 206,624 | 8.45 ms |
| 32 | 134,847 | 47,780 | 2,162 ms |
| 64 | 105,792 | 12,266 | 10,027 ms |

The mechanism works — 2x with *better* tails — up to about 16 client
connections, then collapses. Clients saw `-ERR no reachable master` against a
node whose own logs were clean.

### The scaling variable is threads-per-pooled-connection

The proxy is **thread-per-client-connection**, so client connections *are* OS
threads. Those threads share a small pool of backend connections. Everything
degrades in the ratio `proxy threads / pooled connections`: 1 at 8 clients,
4 at 32, 8 at 64 — which is exactly where the numbers fall over.

Three effects compound in that ratio:

1. **Lock convoy.** Staging takes four mutexes per command — lane map, slot,
   `order`, `pending` — and the TLS `append` (encryption, real CPU) happens
   *inside* the `order` lock. A 16-command pass performs ~64 lock operations.
   Before staging, threads were blocked on a backend round trip most of the
   time and rarely met; staging made them sprint through those cycles
   back-to-back. The contention window and the arrival rate both grew.
2. **Failure amplification.** One connection death fails every in-flight
   caller at once, and each independently re-probed the topology: 198 routing
   transitions on the pooled arm against 0 on the serial one, master flapping
   `addr -> none -> addr` inside milliseconds as concurrent probes raced.
   Threads that saw `none` slept 100 ms and retried. Slow -> timeout -> mass
   failure -> storm -> slower.
3. **Head-of-line blocking.** A caller can wait behind every other request
   queued on the connection it happens to share.

(2) is coalesced and (1) is reduced by per-pass leasing in `f103d92`. Neither
removes the cause: many OS threads contending for few shared connections.

### Why the obvious fix is the wrong one

Envoy's Redis client does not have these problems, and it is tempting to copy
its **thread-local connection pool**. That would be a mistake here.

Envoy has a *fixed, small* number of worker threads, each running an event
loop that multiplexes thousands of downstream clients. "One upstream
connection per worker" therefore means ~8 connections total. Thread-local is
merely how Envoy reaches **few owners**, given its concurrency model.

In a thread-per-connection proxy, thread-local means one backend connection
*per client* — precisely the design ADR-0020 removed (at `max-conns 1024`,
up to 1024 backend sockets and mTLS sessions per node). Copying the mechanism
reproduces the problem it was meant to cure.

The property worth having is **few owners of each backend connection**. Our
model cannot provide it, because our owner count is our client count.

## Decision

**Run the proxy on a bounded pool of worker threads doing non-blocking IO,
with each worker owning its backend connections outright.**

- Worker threads ~= available cores, not ~= clients.
- Each worker multiplexes many client connections.
- Backend connections are **per worker, per (address, namespace,
  async-writes)** and are never shared between workers. No `order` lock, no
  `pending` mutex, no lane map on the hot path — an owner needs no locks
  against itself.
- Reply correlation stays FIFO by position, as today and as in Envoy and
  twemproxy. No request ids.
- Staging (ADR-0020's amendment) is kept: a client's pipeline is submitted
  before any reply is collected. Uncontended, it is pure win — that is what
  the 4- and 8-connection rows above measure.

The runtime is **tokio**, already a workspace dependency: `flint-controlplane`
runs `tokio` with `tokio-rustls` 0.26 on the same rustls 0.23 / `ring`
provider this proxy uses. The async path, the TLS story and the house pattern
(`runtime::Builder::new_multi_thread`, not `#[tokio::main]`) already exist
in-repo, which is most of what makes this affordable.

## What must not break

The proxy is the tenant boundary. The following are properties, not details:

- **Tenancy.** `FLINTNS` pinning, per-connection `authed_ns`, the refusal of
  `FLINT*` above auth. A connection may never see another tenant's data.
- **Transactions.** `MULTI`/`WATCH` is per-connection state on the node, so a
  transaction still needs a connection nobody else uses, for its lifetime.
- **Failover.** `-MOVED` chasing, `-TRYAGAIN`, `-READONLY` demotion recovery,
  rediscovery, and the promote-hint path — including that promote hints are
  never debounced while failure-driven rediscovery is coalesced.
- **The O(keys) admin class** stays off any shared connection.
- **Co-processor channels**, their deadlines and budgets.
- **mTLS on the internal hop** and client-facing TLS termination.

## Risks

**Locks held across await points.** The proxy has ~62 `lock()`/`read()`/
`write()` sites. Holding a `std::sync::Mutex` guard across an `.await` risks
deadlock and destroys the concurrency this ADR is buying. This is the
single largest hazard and it is **mechanically checkable**: deny
`clippy::await_holding_lock` in the proxy crate so every site is caught by the
gate rather than by review. Guards must be scoped to end before any await.

**Blocking work on the runtime.** `discover_master` dials, `call_slow` waits
up to 60 s, cert reloads touch the filesystem. Each must become async or move
to `spawn_blocking`; a blocking call on a worker stalls every client that
worker owns — a far worse failure than today's, where it stalls one.

**This is the largest change to the most safety-critical component.**
ADR-0020 rejected exactly this work as "the largest change and the most
disruption to a component with working failover, tenancy and TLS paths." That
judgment was right then; what changed is the evidence that the cheaper option
does not scale.

## Staging

Each stage lands independently and leaves the tree green.

1. **Async primitives in `flint-tls`.** `AsyncStream`, async connect/accept,
   reloadable-config equivalents, mirroring today's sync surface. Testable
   alone; the proxy does not change. The duplex-split test (`tests/duplex.rs`)
   has an async counterpart, since full-duplex operation is the property the
   pool depends on.
2. **The edge on tokio.** Accept loop, TLS termination, client read/decode and
   reply write become async tasks on a bounded runtime. The backend hop stays
   synchronous behind `spawn_blocking` — correct, temporarily slower, and it
   keeps the tree shippable while the surface converts.
3. **The backend hop async, owned per worker.** Delete the shared pool's
   locks. `forward`, `handle`, `data_command`, `forward_collect`, `fan_out`,
   `scan_forward`, `transaction_step`, `call_pinned`, `discover_master` become
   async. `thread::sleep` (5 sites) becomes `tokio::time::sleep`.
4. **Remove the scaffold** from stage 2 and re-measure.

## Verification

The drill suite is the regression gate and it is unusually good here: seven
proxy drills (`proxy_drill`, `proxy_cache_drill`, `proxy_tls_drill`,
`proxy_chaos_drill`, `proxy_backpressure_drill`, `admin_gated_proxy_drill`,
`proxy_registry_drill`) plus the core suite. They must pass unchanged at every
stage — unchanged being the point, since they encode the properties above.

Performance acceptance, learned from ADR-0020's measurement failures:

1. **The concurrency sweep must not regress at ANY point**, 4 through 64
   client connections. A good headline at one load shape is what hid this
   collapse for two runs.
2. **`pool_batch_mean` snapshotted per concurrency point**, not cumulatively —
   the fleet figure was an average over every leg and therefore
   uninterpretable.
3. **Both driver shapes.** `valkey-benchmark` bursts a whole pipeline into one
   write; `memtier` slides a window. They exercise the proxy's read buffer
   completely differently, and a local 2.4x measured with the bursting driver
   did not survive contact with the sliding one. Report which was used.
4. **Single-thread `-c 1 -P 16` before and after**, as the control that
   isolates the mechanism from all contention.
5. **The A/B harness must prove what it measured**: the binary serving each
   port is verified via `/proc/<pid>/exe`, after a run reported a confident
   1.27x for a build that had died on `AddrInUse` and never served a request.

## Alternatives considered

1. **Exclusive checkout of a pooled connection for the staging window.** One
   acquire/release per pass instead of ~64 lock operations, and each pass's
   frames land contiguously. Cheap, no threading change, and it reduces the
   convoy — but 4 threads still queue per connection at a 4:1 ratio, and the
   blast radius is untouched. A good mitigation, not a fix. Worth taking if
   this ADR is deferred.
2. **Shard connections by thread id (`thread_id % N`).** Sounds Envoy-shaped
   and is not: with 32 threads and 8 connections it is still 4 threads per
   connection. It changes who contends, not how much.
3. **Thread-local backend connections in the current model.** Reproduces the
   per-client socket explosion ADR-0020 removed. Discussed above.
4. **Do nothing; cap concurrency.** Defensible only if no tenant exceeds ~16
   connections through one proxy, which is not a bet worth making.

## What this ADR does not claim

It does not claim the per-command CPU cost changes. Measured 2026-08-17 on one
box, `flint-server` is within 1.18x of Valkey per operation when pipelined and
2.8x better unpipelined; the gap to a RAM cache was always the proxy hop, and
this ADR addresses how that hop scales, not what one command costs.
