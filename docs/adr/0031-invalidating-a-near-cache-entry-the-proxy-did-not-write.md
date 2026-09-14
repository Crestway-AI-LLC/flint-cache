# ADR-0031 — invalidating a near-cache entry this proxy did not write

**Status:** **DECIDED 2026-09-14 (Jeff). Option B taken; A and A′ NOT built.**
The ceiling drops 60 s -> **30 s**, the 5 s default is unchanged, and
cross-proxy invalidation is deferred without a date.

**His reasoning, which is the part worth keeping**: turning the near-cache on
is an *informed opt-in to staleness* — D6 makes it opt-in per tenant for
exactly that reason — so at the 5 s default the window is inside what a
tenant accepted when it asked for a cache. 60 s was not: a minute of a stale
entitlement or session is a bug report, and the ceiling exists to bind abuse
rather than to license that.

**A CORRECTION TO THIS FILE'S OWN COST ESTIMATE.** Fact 1 below says peer
discovery needs a snapshot wire change. **It does not.** `CPSUBSETS` already
returns `tenant -> proxies` and the proxy already polls the control plane, so
discovery is one extra call per poll cycle and a cached map — an hour, no
wire change, no rolling-upgrade hazard.

**The real cost is the transport, and it is not the invalidation.** A proxy
binds exactly ONE listener (`main.rs:4370`), the client edge, and it holds
`admin_digests` — hashes, not plaintext — so it cannot authenticate to a
peer even on the port it already has. Proxy-to-proxy therefore needs a new
internal mTLS listener: a port, an inventory key, packaging, security-group
rules and drills. That is a new operational surface, larger than the feature it
would carry. Whoever revisits this should start there, not in `cache.rs`.

**Scope:** what invalidates a proxy's near-cache entry when the write did not
go through that proxy. Not whether the near-cache should exist (ADR-0005 D6),
and not the TTL's default or ceiling.

## v1 is not broken, and that matters for how v2 is framed

`flint-proxy/src/cache.rs` states its contract plainly:

> stale reads are ALLOWED, bounded by the TTL. A write through THIS proxy
> invalidates its local entry (read-your-own-writes through one proxy); a
> write through ANOTHER proxy — or straight to a node — becomes visible here
> only when the TTL lapses. **The TTL is the contract**; same-proxy
> invalidation is a freshness optimization on top.

So v2 is not a bug fix. It is **tightening a contract that was deliberately
loose**, and it has to justify itself against the contract rather than against
an expectation the contract never made.

## Why now — the trigger arrived from the other direction

The 2026-09-06 audit recommended NOT scheduling v2, because the window it
closes was 300 ms: `--cache-ttl-ms`'s default at the time. The recorded trigger
for revisiting was *"a tenant wanting a materially longer TTL"*.

**The default is now 5 s and a tenant may set its own up to a 60 s ceiling.**
That is 16x to 200x the window the recommendation rested on, and it is the
trigger arriving from the operator side instead of the tenant side.

## The exposure, stated precisely

A tenant is served by a shuffle shard of `k` proxies, default 2. A client holds
one connection to one of them, and a write through *that* proxy invalidates its
own entry — **so read-your-own-writes holds for the client that wrote.**

The exposure is **cross-client**: A writes through proxy 1, B reads through
proxy 2 and sees the old value until the TTL lapses. At 5 s that is a
noticeable window for a session or an entitlement; at 60 s it is a bug report.

**No drill covers it.** Two proxies, two clients, one write and one read is a
small drill and should exist whichever option is taken.

## Three facts the design starts from, read from the code

**1. The proxy does not know its peers — at all.** Nothing in `flint-proxy`
carries a proxy list or a tenant's subset; `CPSNAPSHOT <proxy-addr>` is what a
proxy fetches for *itself*. So cross-proxy invalidation is **not a proxy-local
change**: the control plane has to tell a proxy who else serves each tenant,
and that is a snapshot wire change with the gate cost that implies.

**2. A tenant cannot write straight to a node.** The module names that path and
it is real, but it is not tenant-reachable: nodes speak internal mesh mTLS and
namespacing happens at the proxy. It is an operator path — `flintctl`,
migrations, drills — and it stays TTL-bounded under every option below. Written
down rather than quietly dropped from the scope.

**3. Replica reads are a separate opt-in, and they are where a naive v2 fails.**
`main.rs` keys tokens to `(namespace, replica_reads, local_cache)` — three
independent per-tenant flags. With replica reads on, a peer that receives an
invalidation can immediately refill from a **replica that has not applied the
write yet**, and re-cache the stale value with a *fresh* TTL.

That is **worse than v1**, and the asymmetry is the point: v1's staleness
expires, while a re-cached stale entry renews itself. A v2 that ignores this
makes the combination it was built to help worse than leaving it alone.

## Options

**A — subset in the snapshot, then proxy-to-proxy invalidate.** Fan-out is
`k-1`, one message at the default. Fails open: a lost invalidation degrades to
the TTL, which is exactly v1's contract, so the failure mode is *no worse than
today*.

**A′ — the same, carrying the write's sequence.** The peer suppresses caching
that key until it can read at or past that sequence. Closes fact 3 at the cost
of carrying a number and comparing it.

**B — build nothing; lower the ceiling.** The exposure grew because the ceiling
went to 60 s. Lowering it shrinks the window proportionally, costs nothing, and
ships today. Not a fix, and it is the honest alternative that any "should we
build this" answer has to beat.

## Recommendation

**A′, rather than A then A′ later.**

Carrying the sequence is not much harder than carrying the key, and building A
first means shipping an interval in which `local_cache + replica_reads` — a
combination the control plane allows today, silently — is *worse* than not
having built anything. An interval like that is how a fix acquires a reputation.

**B is worth taking anyway and immediately**, whatever is decided about A′: the
ceiling is a number, and 60 s is a long time to serve a stale entitlement. It is
not a substitute, because lowering a bound is not the same as invalidating.

**TAKEN, and only B.** `DEFAULT_TTL_MAX_MS` is 30 s. The constant carries the
reason in its own doc comment, because the number is a decision rather than a
tuning parameter: **this bound IS the cross-client staleness window**, and a
later reader moving it should know that is what they are moving. A test now
asserts the DEFAULT ceiling clamps a tenant asking for an hour — previously
only an explicitly-passed ceiling was covered, so a change to the constant
failed nothing.

## Consequences

- A snapshot wire change gets the **bare full gate**, by this repo's own rule:
  the consumers of a shared shape are not enumerable with confidence.
- Fan-out is per write, per peer. At `k = 2` that is one message; a whale
  widened by `CPSETSUBSET` pays proportionally, which is the correct direction.
- A lost invalidation is not an error and must not be retried into one. It
  degrades to the TTL, and the TTL is still the contract.
- The operator direct-to-node path stays TTL-bounded, and is now written down.

## What this does not decide

The TTL default and ceiling (option B is a recommendation, not a decision
taken here); whether the near-cache should ever be on by default — it should
not, for the reason D4 and D6 both give; and whether
`local_cache + replica_reads` should be refusable at `CPADDTENANT`, which
becomes moot under A′ and urgent under A.
