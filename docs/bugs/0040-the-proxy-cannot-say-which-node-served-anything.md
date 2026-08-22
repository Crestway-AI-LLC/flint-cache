# BUG-0040 — the proxy cannot say which node served anything

**Status:** FIXED 2026-08-21, both halves, landed but NOT YET DEPLOYED —
it reaches the fleet on the next release. Found while designing a
pre-termination safety gate that turned out to be unbuildable without it.

    proxy  8cf94e7  per-backend counters + PROXYBACKENDS
    agent  ce48e96  polls it, exports flint_proxy_pool_node_commands_total{proxy,node}

The gate itself is still unbuilt and is tracked in the ops repo's
`docs/actions.md`; this bug was only ever about the missing signal.

## What is missing

The proxy counts commands globally and nothing else:

    crates/flint-proxy/src/main.rs:1807
    let pool_commands = apool::COMMANDS.load(Ordering::Relaxed);

One `AtomicU64` for the whole pool. Both exported series carry `proxy` and no
backend identity:

    flint_proxy_commands_total{fleet,instance,job,proxy}       = 586964
    flint_proxy_pool_commands_total{fleet,instance,job,proxy}  = 522678

Nodes do not fill the gap from their side. A node exports `live_replicas`,
`seq_lag`, `sst_bytes`, `cert_days_remaining` — no request counter of any
kind. So **there is no signal anywhere in the system that answers "did traffic
reach node X".**

## Why it surfaced

A retired host is kept 72h before termination (`docs/actions.md` in the ops
repo). The obvious final gate is "no user traffic in the last 12 hours", and
it cannot be built.

Worse, the naive version is actively dangerous. A dead box exports nothing, so
its counters are ABSENT, and absent reads as zero. The host most likely to
have no data for reasons other than no traffic — exporter down, scrape target
dropped, series gone stale — is precisely the host the gate exists to protect.
**The check would pass automatically for every candidate**, which is the
check-that-cannot-fail pattern arriving inside a safety gate.

The fix has to measure from the PROXY, which is alive and scraped, rather than
from the box about to be killed. A live exporter reporting zero is evidence; a
dead box's silence is not.

## The change

Label the pool counter by backend:

    flint_proxy_pool_commands_total{proxy="…", node="172.31.64.94:7001"}

The proxy already has the backend address at dispatch — ADR-0021 gave each
worker its own per-backend connections — so this is one counter becoming a map
keyed by the address the worker already holds. Cardinality is bounded by the
node count, which is the fleet size.

## Worth more than the gate that prompted it

"Which node is actually serving the traffic" is unanswerable today. That
blocks more than termination:

- **Rebalancing** decides by `sst_bytes`, i.e. by what is STORED, with no view
  of what is SERVED. A pair can be balanced by size and lopsided by load.
- **Hot-key and hot-slot work** measures at the proxy in aggregate; it cannot
  say which backend absorbed the storm.
- **Any per-node capacity claim** — "this node handles N ops/s" — is currently
  an inference from a fleet-wide number divided by a node count.

## What must NOT be built on it naively

When the label exists, the gate stays three-valued or it reintroduces the same
defect:

| reading | verdict |
|---|---|
| series PRESENT for the full window and flat | no traffic — safe to terminate |
| series present and non-zero | traffic seen — refuse |
| series absent, gappy, or shorter than the window | **cannot tell — refuse** |

The third row is the whole point. "We watched for 12 hours and saw nothing"
and "we have no data for 12 hours" must never collapse into one answer, and
the second one is what a missing exporter produces.


## How it was fixed, and the two things not to undo

**A registry keyed by address, not a field on the connection.** Connections
live in per-worker thread-locals, so nothing outside a worker can enumerate
them and the admin command runs on another thread. Keying by address also
makes a counter outlive connection churn — a backend that drops and redials
keeps one running total. A per-connection counter would zero itself on every
reconnect, and reconnects cluster exactly when a node is unhealthy, so it
would reset precisely when the number is wanted.

**The lock is taken once per connection**, in `AsyncConn::new`, beside a dial
that already does I/O. The command path holds an `Arc` resolved at that moment
and does the same relaxed `fetch_add` as before. A mutex on the command path
would rebuild the convoy ADR-0021 removed.

**A new metric name, not a label on the old one.**
`flint_proxy_pool_commands_total` already exists unlabelled from the
info-block passthrough. Publishing an unlabelled fleet total and per-node
totals under one name makes `sum()` count every command twice, once globally
and once per node, and whoever writes that query has no reason to suspect it.
`flint_proxy_pool_node_commands_total{proxy,node}` keeps both summable and
leaves existing dashboards untouched.

The property the whole bug turns on is pinned by a test rather than left to a
comment: a dialed backend with no traffic reports **0**, a never-dialed
backend is **absent**. Evidence and ignorance stay distinguishable.