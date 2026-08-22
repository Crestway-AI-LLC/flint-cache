# BUG-0040 — the proxy cannot say which node served anything

**Status:** OPEN. Found 2026-08-21 while designing a pre-termination safety
gate that turned out to be unbuildable without it.

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
