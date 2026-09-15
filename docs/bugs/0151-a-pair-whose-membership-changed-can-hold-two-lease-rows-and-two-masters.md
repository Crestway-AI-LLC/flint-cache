# BUG-0151 — a pair whose membership changed can hold two lease rows, and answer OK to both masters

**Status:** OPEN — found 2026-09-15 while fixing BUG-0150, by a test that
failed for the right reason and then turned out to be asserting a property the
product does not have on **either** control plane. Severity: high if reached —
this is the fence not fencing.

## What is established, and how

A lease row is `(pair members, master-of-record, generation)` and is resolved
by MEMBERSHIP CONTAINMENT: the first row whose members include the address.
`CPSETPAIR` replaces a pair's member vector and **nothing migrates the lease
rows** — in `registry.rs` `leases` appears only in `Fence` and `LeaseAdopt`, and
in `main.rs` the `CPSETPAIR` arm touches `st.pairs` alone.

So a new member cannot be found by containment, because it was not in the pair
when the row was written. Observed directly, applying the mutations to
`RegistryState`:

```
AddPair [a:1, b:2]; LeaseAdopt a:1   -> [ ([a:1,b:2], a:1, 0) ]
SetPair 0 -> [a:1, c:3]              (b:2 replaced; the row still says [a:1,b:2])
Fence c:3                            -> [ ([a:1,b:2], a:1, 0),
                                          ([a:1,c:3], c:3, 1) ]
```

Two rows for one pair. Reading `CPLEASE` — which returns `OK` if the FIRST row
containing the caller names the caller as master, and `SUPERSEDED` otherwise:

- `a:1` renews → finds row 0 → master is `a:1` → **OK**
- `c:3` renews → finds row 1 → master is `c:3` → **OK**

**Both are told they are master.** That is the state ADR-0018's fencing record
exists to make impossible.

## What is NOT established

That the production flow reaches it. The sequence needs a `CPSETPAIR` that
replaces a member, then a `CPFENCE` of the NEW member, then the OLD master
alive and renewing. `flintctl swap-node` does issue `CPSETPAIR` after a
replacement converges, and promotion issues `CPFENCE`, so "replace a failed
replica, then lose the master" is an ordinary sequence that composes them — but
that is reading the code, not a run. **Nobody has seen this happen.** It has
not been reproduced on a fleet, no drill covers it, and it is filed here at the
strength it actually has.

`three_member_repoint_drill` exercises repointing and passes, which is evidence
the common path is fine, not evidence this path is unreachable — the drill
would have to fence a member added after the row was written, with the old
master still renewing, to say anything about it.

## Which path can reach it, from the ops side (added 2026-09-15)

Read from the code, not run — the same standing as the section above, and
offered because it **rules two paths out** rather than because it reproduces
anything.

**The operator path cannot reach it.** `flintctl`'s failover core demotes the
old master FIRST, then drains, then commits `CPFENCE`, then promotes — its own
comment says demote-first is what makes it lossless. A demoted master is not a
renewing one, so `failover` and `upgrade` compose `CPSETPAIR` and `CPFENCE`
without ever holding the third condition open.

**A killed master cannot reach it either**, which is why the existing coverage
misses. The ops drill `tools/flintctl_drill.sh` already runs two thirds of this
sequence on every ops gate: `swap-node 9601 → 6914` at :77 issues the
`CPSETPAIR`, and the controller promotes `6914` at :91. Between them, :84 is
`kill -9` on the old master. A dead master renews nothing, so the drill
composes the two operations the bug needs and then removes the condition that
makes them dangerous. Deleting that `kill` does not fix it: the promotion in
that drill happens *because* the master died.

**What is left is the controller's automatic path against a master that is
unreachable but alive.** `flint-controller` cannot demote first — it cannot
reach the node — so it commits `CPFENCE` and promotes, and demotes the old
master as a zombie only when it reappears (`main.rs:19`, `:50`). If that master
is partitioned from the controller but still reaching the control plane, it is
alive and renewing across the fence, which is exactly the state this file
describes.

So the drill that would settle it is a **partition**, not a kill: sever
controller→master while leaving master→CP intact, after a `swap-node`. Every
drill that touches this area kills or SIGSTOPs, and both of those stop the
renewal that the bug requires.

None of this is a sighting. It narrows where to point one.

## Why it is not fixed here

The fix is a design choice and it should be made deliberately:

- **Resolve rows by pair INDEX** rather than by membership. Routing already
  follows pair index through failovers, so this is the shape the rest of the
  system uses — but it changes the durable row's key and needs a migration.
- **Migrate the row in `SetPair`**, rewriting its member vector. Smaller, and
  it keeps containment working, but it puts a lease write inside a pair
  mutation on both paths.
- **Refuse the fence** when the addr is in no existing row but is in a pair
  whose row exists under different membership, and make the operator repair it.
  Safest, worst to be woken by.

Whichever it is, it lands in `registry::apply` and in `main.rs` — BUG-0146's
two copies again — and it wants a drill that reaches the state before a fix
goes near it, because the thing to verify is that two masters cannot both be
told OK, not that a unit test agrees with itself.

**Both control planes are equally affected.** This is not a raft/single-node
divergence; it is a property of the containment key, which is the right key for
what BUG-0065 was about and silent about membership that changes underneath it.
