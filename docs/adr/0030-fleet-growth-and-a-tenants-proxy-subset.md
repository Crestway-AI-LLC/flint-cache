# ADR-0030 — fleet growth and a tenant's proxy subset

**Status:** **PROPOSED 2026-09-14.** The roadmap records "proxy scale-out for a
live tenant" as blocked on this decision. Written because the blocker is real;
the recommendation at the end is mine and is the part to argue with.

**Scope:** what a change in proxy fleet membership does to `tenant.subset`.
Not the saturation *signal* — that is the sibling question this shares with
throughput-triggered pair expansion, and it is not decided here.

## The mechanics, read rather than recalled

A tenant's proxies are a shuffle shard of the fleet, `k` wide (default 2):

    registry.rs:226  shuffle_shard(name, fleet, k)

It sorts the fleet, seeds from `fnv1a(tenant_name)`, and walks. Deterministic
given `(name, fleet, k)` — and note the second argument. **The result depends
on the whole fleet list**, so recomputing after the fleet grows does not merely
add; it can move a tenant off a proxy it is using.

The subset is computed once, at `CPADDTENANT`, and **stored** (`t.subset`).
Exactly three mutations write it afterwards:

| mutation | what it does to subsets |
|---|---|
| `AddTenant` | computes it, once |
| `DelProxy` | removes the retired proxy **from every tenant** |
| `SetSubset` | an operator sets it by hand (`CPSETSUBSET`) |

`AddProxy` is not in that table. It appends to `self.proxies` and touches no
tenant, which is the fact the roadmap records: a newly registered proxy serves
zero existing tenants.

## The asymmetry nobody chose

**`DelProxy` shrinks a subset and nothing ever re-widens it.** The removal is
deliberate and its comment is right — a retired proxy left in a subset is a
placement slot pointing at nothing. But there is no compensating pick, and no
automatic path back. So:

- a tenant at `k = 2` that loses one proxy runs at `k = 1`, permanently, until
  a human runs `CPSETSUBSET`;
- proxy churn is a **one-way ratchet**, and its terminus is an empty subset,
  which the control plane itself describes as *"this tenant is DRAINED and will
  answer -WRONGPASS"* — an outage for that tenant, reached by attrition.

This is the part that changes the decision. The question is usually framed as
*should fleet growth reach tenant subsets?*, and the answer is already yes:
fleet **shrinkage** reaches them today. What is actually on the table is that
membership changes propagate in one direction, and it is the direction that
degrades.

I have not found a drill covering the ratchet, and would expect one to be part
of whatever is chosen: retire a proxy under a tenant, then assert the tenant is
back to `k` rather than merely still serving.

## The candidates

**A — re-shard on fleet growth.** Recompute the same `k` over the larger fleet.
New proxies pick up existing tenants; isolation is preserved because `k` is
unchanged. Cost: live tenants' subsets move, and a moved subset moves
connections.

**B — demand-driven widen.** Raise `k` for a tenant whose own proxies are
saturated. Cost: it spends isolation for headroom, and it needs the per-tenant
edge saturation signal that does not exist — the same gap blocking
throughput-triggered pair expansion.

**Not a candidate:** adding every new proxy to every tenant. That erases the
isolation shuffle-sharding exists for, and it is worth writing down so it is
not rediscovered as an obvious shortcut.

## Recommendation

**Take A, and take it for the ratchet rather than for scale-out.**

1. **Re-shard is the only one of the two that fixes the degradation**, and the
   degradation is live today while scale-out is a want. B raises `k` for a
   saturated tenant and leaves a churned tenant at `k = 1` forever.
2. **B cannot be built yet and A can.** B needs a per-tenant edge saturation
   definition; that is the same design question blocking the pair path, and
   pairing two unbuilt things behind one missing signal is how neither gets
   done. A needs no new signal: fleet membership already changes.
3. **A is the smaller behavioural claim.** `k` does not move, so the isolation
   property the design rests on is untouched; what moves is which `k` proxies,
   which is a thing `DelProxy` and `CPSETSUBSET` already do.

**The cost is real and is the thing to argue about:** re-sharding moves live
connections. Mitigations, none of them free: recompute only for tenants below
`k` (repairs the ratchet, gives new proxies almost nothing, and is the
conservative first step); or recompute for all and accept the churn on a fleet
event that is already operator-initiated.

**If only one thing is taken from this, take the first mitigation.** Recompute
for tenants *below* `k` on any membership change. It is small, it needs no new
signal, it closes a path to a tenant-visible outage, and it leaves the
scale-out question exactly where it is rather than pretending to answer it.

## Consequences

- B is not rejected; it is **deferred behind its signal**, with the note that
  the signal is shared with the pair path and should be defined once.
- Whatever is taken needs the ratchet drill named above, because a subset that
  is merely *serving* is not a subset that is *whole*, and only the second is
  what `k` promises.
- `CPSETSUBSET` stays the manual override and stays the documented escape for
  whale isolation. Nothing here changes what an operator can set by hand.
