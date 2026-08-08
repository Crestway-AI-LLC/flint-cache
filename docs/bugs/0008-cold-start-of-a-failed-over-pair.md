# BUG-0008: cold start of a failed-over pair replicated nothing (RESOLVED)

Status: RESOLVED 2026-08-08 · Severity: high (silent single-copy fleet)

## Symptom
On the playground, `flintctl stop` then `flintctl start` — run for an
unrelated address change — left the pair with `live_replicas 0` on both
members. Nothing errored. `status` looked structurally fine: both nodes
up, roles coherent (7001 replica, 7002 master), epochs agreed, builds
matching. The only signal was the replica count.

The pair had failed over weeks earlier, which is what made it eligible.

## Root cause
Two correct rules composing into a broken fleet.

`start_pair_nodes` asks the live members who is master, and falls back to
inventory ORDER when nobody answers — pair[0] bare, the rest with
`--replica-of pair[0]`. On a cold start there is genuinely nobody to ask,
so the fallback is reasonable on its face.

`flint-server` refuses to demote itself on a flag: `manifest role is
master (durable); ignoring --replica-of`. That rule is right and must
stay — a flag must never be able to demote a master holding the newest
data.

After a failover the durable roles are the reverse of inventory order, so
the two rules produce:

    pair[0], durable replica, started bare  -> replica of NOBODY
    pair[1], durable master, given the flag -> "ignoring --replica-of"

Neither node objects, because neither is doing anything wrong. The fleet
serves reads and writes from one copy indefinitely.

## Fix
`reconcile_cold_start` runs after the cold-start spawn loop: once the
seats are serving, ask them who is master, and if it is not pair[0],
re-seed every other member onto the real master via `roll_node`. Costs
one extra probe on the common path, where the assumption holds and
nothing is rolled.

Deliberately NOT fixed by making the flag authoritative, which would
delete the durable-role protection that is the more important of the two
rules.

If no member reports master, the pair is left alone with a diagnostic —
re-seeding blind against no lineage would be worse than leaving it
visible for the controller.

## Why nothing caught it
Every existing drill either bootstraps fresh (inventory order is correct
by construction) or fails over on a fleet that stays up (`live_master` is
found, so the fallback never runs). The bug needs both — a failover AND a
full stop — and no drill combined them.

`tools/cold_start_roles_drill.sh` now does, and is in the CORE gate list.
Confirmed to fail on the unfixed binary before being trusted: exit 1 with
`live_replicas 0`, exit 0 after.

## The second half: `verify` did not catch it either
BUG-0002 taught `verify` that a declared member which is not reachable is
a failed check. That fix counts members that are **present**. Here every
member was present, so `pair 0 fully staffed` printed `2 member(s) up`
and was telling the truth about the wrong thing — the fleet was
single-copy with nobody down, and `verify` passed.

So `verify` now also asks the master how many replicas are actually
streaming, and fails the pair when that is short of `members - 1`:

    SINGLE-COPY: every member up, but 0 of 1 streaming from <master>
    — one disk holds the only copy

That is the number the original phrase "no failover target, one copy on
one disk" was always about. Present is not attached.

Safe for the operations that end in `verify_after`: `add-replica` already
blocks on `seq_lag: 0`, and `roll_node` waits for the rejoin, so neither
reaches verify mid-sync.

## Related
BUG-0002, directly — this is the same failure surviving in the half of
the state space that fix did not cover.
