# BUG-0151 — a pair whose membership changed can hold two lease rows, and answer OK to both masters

**Status:** **FIXED 2026-09-15** — and **REACHED THROUGH THE PRODUCT** before
it was fixed. Filed the same day with the reachability explicitly unestablished;
the drill written next settled it in the worse direction. This was the fence
not fencing, on an ordinary operator sequence.

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

*Established later the same day, in the worse direction — see "Reached,
then closed" below. This section is kept as written: it is the record of what
was known at filing, and the reason the drill was written before the fix.*

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

> **RETRACTED the same day, by the author of this section.** The reasoning
> below rules two paths out on the grounds that they never hold "the old master
> alive and renewing" open. That condition is how the damage is *observed*, not
> what *creates* it: the `RegistryState` trace above shows two rows appearing
> from `SetPair` + `Fence` alone, with no renewal involved and no seats in the
> picture at all. So demote-first does not prevent the state — it only means
> nobody is asking yet — and neither does a `kill -9`, since the stale row
> outlives the process and anything that later holds that address is told OK.
>
> I took the three conditions from this file's own "what is NOT established"
> and reasoned about how to satisfy the third, without checking whether it was
> a creation condition or an observation one. **Both paths below do reach the
> defective state.** The section is kept rather than deleted because the
> mechanism it describes — `flintctl` demoting before it fences, the controller
> being unable to — is accurate and worth knowing; only the "cannot reach it"
> conclusion is wrong.

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

*Added after the above, then corrected: **I endorsed this narrowing and was
wrong to.** My note said the answer stands — not the operator path, not a killed
master, but the controller against a master it cannot reach — and the section's
own author retracted it within the hour for a reason I should have caught while
agreeing with it. "The old master alive and renewing" is how the damage is
OBSERVED, not what creates it. The trace two sections above shows two rows
appearing from `SetPair` + `Fence` alone, with no renewal and no seats in the
picture; and a kill does not help either, because the stale row is DURABLE
state that outlives the process, so whatever later holds that address is told
OK.*

*What my drill actually shows is narrower than I claimed for it: asking
`CPLEASE` for the displaced master is a way to OBSERVE the two rows, not the
thing that produces them. So no deployment path is ruled out, and a fleet
reproduction does not need a partition — it needs a repoint, a fence, and
anything at all that later asks.*

*The mechanism the section describes is accurate and worth keeping: `flintctl`
demotes before it fences and the controller cannot. Only the conclusion drawn
from it was wrong, in their write-up and then again in mine.*

## Why it was not fixed at filing

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

## Reached, then closed

`tools/lease_after_repoint_drill.sh` runs the sequence against a real control
plane. Before the fix:

```
== control: the peer reads as superseded, so a refusal is observable here
  127.0.0.1:6432 -> SUPERSEDED 127.0.0.1:6431
== repoint: :6432 is replaced by :6433, then :6433 is promoted and fenced
  OK fenced 127.0.0.1:6433 gen 1
== the question: how many addresses does the CP call master?
  127.0.0.1:6431 -> OK
  127.0.0.1:6433 -> OK
FAIL: 2 addresses hold the write lease for one pair at the same time.
```

After it, the displaced incumbent reads `SUPERSEDED 127.0.0.1:6433` and the
fenced member holds the lease. **Red before, green after, on the product** —
which is the strongest control this drill could have, and the reason it was
written before the fix rather than alongside it.

**The section above overstated nothing and understated the risk.** It said the
composition "is reading the code, not a run". The run agreed with the reading.

### Why no drill had ever been here

Nothing in the suite exercised `CPSETPAIR` or `swap-node` at all — the grep
that established that is two lines and was worth more than any amount of
reasoning about whether the path was reachable. A verb with no drill is not a
verb that works; it is a verb nobody has asked.

The drill is CP-level on purpose. No servers are started: `CPADDPAIR` registers
addresses rather than processes, and the whole question is the control plane's
own bookkeeping. Starting three nodes would have added a failover's worth of
timing to something deterministic.

### The fix

`tenant::repoint_lease_row` moves a pair's row onto its new membership, located
by any member it had BEFORE the change — the only handle that still works at
that moment. Called from `registry.rs`'s `Mutation::SetPair` and from
`main.rs`'s `CPSETPAIR`, which needs it **twice**: `st.leases` is the durable
record and `lf.entries` is the fast mirror `CPLEASE` actually reads, so
migrating only the first would have left the single-node path answering out of
the row the repoint had just made stale. That mirror is exactly the kind of
second copy BUG-0146 is about, and it is why the drill was run against the
product rather than trusted to a unit test.

The option chosen was the second of the three this file listed. Pair-index keys
remain the cleaner end state and still want the durable-format migration they
always did; nothing here forecloses them.

**A second gap the drill exposed on the way:** `CPADDPAIR` sorts a pair's
members (BUG-0065's root fix, so `a,b` and `b,a` are one pair to the `contains`
dedupe) and `CPSETPAIR` did not. A repoint could therefore write an unsorted
vector that a later `CPADDPAIR` of the same members would not match,
registering a duplicate pair — BUG-0065's root returning through a verb its fix
never touched. Both paths now sort, in the handler rather than in `apply()`, so
already-committed log entries replay unchanged.

### On changing `apply()`

Repointing inside `Mutation::SetPair` changes what the raft state machine does
with a log entry, so two nodes at different versions replaying the same entry
would diverge. ADR-0030's `DelProxy` refill set that precedent four days
earlier and the same reasoning applies: it is a correctness fix, the divergence
window is a rolling upgrade, and leaving the bug in place to preserve
bit-compatibility with a wrong answer is the worse trade. Worth stating rather
than discovering.

Unit tests cover the raft path, which the drill cannot reach — it drives a
single-node control plane, and that is the other implementation. Removing the
`repoint_lease_row` call from `apply` fails
`a_repoint_moves_the_lease_row_so_the_next_fence_finds_it`; the companion
control, that a pair which never held a lease gains no row from a repoint,
stays green under that mutation, as it should.
