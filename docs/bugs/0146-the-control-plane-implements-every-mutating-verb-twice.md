# BUG-0146 — the control plane implements every mutating verb twice

**Status:** OPEN — found 2026-09-14 by a fix that landed in one of the two and
was reported green by six unit tests.

## What happened

ADR-0030's refill went into `registry::RegistryState::apply`'s
`Mutation::DelProxy`. Six unit tests passed, four of them dying to a mutation
of the new code. `subset_ratchet_drill` then failed on the gate box:

```
FAIL: the tenant is at 1 after a retirement, not back at 2 — [127.0.0.1:7565]
```

**The fix was real and the control plane did nothing**, because `CPDELPROXY`
is implemented **twice**:

| path | where | how |
|---|---|---|
| raft | `ha.rs:581` | proposes `Mutation::DelProxy` → `registry::apply` |
| single-node | `main.rs:171` | mutates `state::State` inline |

The drill starts its control plane without `--raft`, so it exercised the half
that had not been fixed. **The tests exercised the TYPE; the drill exercised
the PRODUCT.**

## It is not one verb

Every mutating verb has both forms — `CPADDPROXY`, `CPADDTENANT`,
`CPSETSUBSET`, `CPDELTENANT` and the rest each appear once in `main.rs` and
once in `ha.rs`. And the placement function itself is duplicated:
`shuffle_shard` exists identically at `state.rs:173` and `registry.rs:226`.

So the control plane has **two implementations of its placement logic and two
of every mutation**, kept in step by hand. The `Tenant` struct is shared, which
is what makes the duplication survivable and also what makes it invisible: the
data agrees, so nothing reconciles the behaviour.

This is the inverse of the rule `flintctl`'s remote runner was designed around
— *one implementation of each invariant and two transports* — which exists
precisely because a check that behaves differently on the machine where it
matters is the rc.15 bug class.

## What was done here, and what was not

**Done:** the refill is now a single function, `tenant::refill_after_retire`,
called from both paths, with `shuffle_shard` passed in as an argument so the
refill is single even while its input is not. A test asserts the two shuffles
place a retired tenant identically — the cheapest available guard against the
copies diverging.

**Not done:** unifying the two paths. That is a real change to the control
plane's structure, it touches every mutating verb, and it deserves its own
design and its own gate rather than being smuggled in behind a subset fix.

## The part worth acting on first

**A unit test against `RegistryState` proves nothing about a single-node
fleet**, and single-node is what most drills and every small deployment run.
Until the paths are unified, a change to a mutation needs either both arms
edited or a drill that runs the path the tests do not.
