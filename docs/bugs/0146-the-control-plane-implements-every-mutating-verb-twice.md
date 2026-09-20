# BUG-0146 — the control plane implements every mutating verb twice

**Status:** OPEN — found 2026-09-14 by a fix that landed in one of the two and
was reported green by six unit tests. **The design this file asked for exists:
[ADR-0032](../adr/0032-one-implementation-of-every-control-plane-mutation.md),
ACCEPTED 2026-09-15 (Jeff) — candidate A, single-node runs the same state
machine, staged so the durable-format change moves on its own.** This stays
OPEN because the work is not done, not because the decision is outstanding.

Three more instances landed between the filing and the decision, and they are
the reason the ADR was taken rather than deferred again:

- **BUG-0148** — `CPMYSTATUS` dispatched only by `main.rs`; eighteen days.
- **BUG-0150** — BUG-0065's fix *and the structural guard holding it shut* both
  only in `main.rs`, with the guard reading `include_str!("main.rs")` while its
  own forbidden literal sat in `registry.rs`, twice.
- **BUG-0152** — the single-node line format carried ten of `Tenant`'s twelve
  fields. **Direction reversed**: here the Raft path is correct and single-node
  is wrong, which is why "check the other path" is not a rule that can be
  learned from the previous two.

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

## Where this actually stands, measured 2026-09-19

ADR-0032 reports steps 1-4 done, and this file still said "the work is not
done". Both can be true, so here is what was checked rather than recalled:

**Single now:**

- **The meaning of every mutation.** `main.rs`'s single-node arms call
  `st.apply_mutation(registry::Mutation::…)` — the same state machine `ha.rs`
  proposes into. The original defect (a fix landing in `Mutation::DelProxy`
  while the single-node arm did nothing) cannot recur in `apply`, because
  there is one `apply`.
- **The verb tables**, guarded by `assert_cp_verbs_agree_across_paths`
  (`tools/gates.sh:2013`), 44 and 44 today. It catches an arm somebody forgot
  to ADD — BUG-0148's shape — and says in its own source that it cannot catch
  one somebody forgot to UPDATE.
- **The refusals**, split out as BUG-0160 and fixed there.
- **Membership canonicalisation**, as of
  [BUG-0169](0169-pair-membership-is-canonicalised-in-four-hand-written-places.md):
  BUG-0065's rule was a `.sort()` written out at four sites, two of which had
  already been the site of a miss (BUG-0150, BUG-0151). Now one function.

**Still two, and not yet judged:** each dispatcher owns its own parse, its own
liveness check (`state.lock()` against `leader_view().await`) and its own reply
string. Some of that is inherent — one is a local mutex and the other is a
Raft proposal, which is what candidate A chose. What has NOT been established
is that none of it still carries a *semantic* rule the other must mirror by
hand. BUG-0169 found one such rule by reading two verbs; there are forty-four.

**So this stays OPEN, and the reason is now specific rather than general.**
Closing it needs a per-verb audit asking one question of each arm: does it do
anything between parse and `apply`/`propose` that the other arm must do
identically? That is the remaining work, and it is a reading task with a
decidable answer, not a design question.

## The part worth acting on first

**A unit test against `RegistryState` proves nothing about a single-node
fleet**, and single-node is what most drills and every small deployment run.
Until the paths are unified, a change to a mutation needs either both arms
edited or a drill that runs the path the tests do not.
