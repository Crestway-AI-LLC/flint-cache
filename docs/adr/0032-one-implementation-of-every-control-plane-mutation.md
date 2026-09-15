# ADR-0032 — one implementation of every control-plane mutation

**Status:** **PROPOSED 2026-09-15** — design only, no code. Written because
BUG-0146 asked for one, and because the duplication it describes produced
**four** defects in eight days, three of them in the last twenty-four hours.

**Scope:** the two implementations of every mutating control-plane verb, and
the two durable formats underneath them. Not the Raft protocol, not whether
single-node remains a supported deployment — that last one is a product
question this ADR asks rather than answers, and the answer changes which
candidate wins.

## The mechanics, read rather than recalled

`--raft` is decided in `main()` and enters `run_raft` → `ha::run_client`, which
never reaches `main.rs`'s dispatch. From there the two paths share the `Tenant`
struct and nothing else:

| | single-node | raft |
|---|---|---|
| dispatch | `main.rs`, 43 arms | `ha.rs`, 43 arms |
| state type | `state::State` | `registry::RegistryState` |
| how a verb mutates | inline, then `st.commit()` | builds a `Mutation`, `ha.propose`, `registry::apply` |
| durable format | **hand-written** `serialize()` and parser | serde `Serialize`/`Deserialize` |
| placement | `shuffle_shard` at `state.rs:173` | `shuffle_shard` at `registry.rs:226`, identical |

The two state types carry **eleven identical fields** — version, proxies,
pairs, ranges, tenants, exceptions, leases, promoted, families and the admin
token pair. `State` has one the other does not: `controllers`.

There is also a **third** copy of the lease rows on the single-node path:
`Shared::leases` / `LeaseFast.entries`, the fast mirror `CPLEASE` reads on its
hot path rather than taking the state lock.

**The shared `Tenant` struct is what makes the duplication survivable and also
what makes it invisible.** The data agrees, so nothing ever reconciles the
behaviour.

## What it has cost, measured rather than feared

| | what happened |
|---|---|
| ADR-0030 | the `DelProxy` refill landed in `registry.rs`. Six unit tests green, four dying to a mutation of the new code, and **the product did nothing** — the drill runs single-node. Found on the gate box, not by the tests |
| BUG-0148 | `CPMYSTATUS` dispatched only by `main.rs`, so a tenant on a Raft control plane got *unknown command* for the one verb ADR-0014 gives them. Eighteen days |
| BUG-0150 | BUG-0065's fix **and the structural test that holds it shut** both landed only in `main.rs`. The test reads `include_str!("main.rs")`, so its own forbidden literal sat in `registry.rs`, twice, while it passed |
| BUG-0151 | the fix needed **three** call sites, because of the fast mirror. Two would have left the single-node plane answering out of the stale row |

Four in eight days, one of them the split-brain guard. This is not a latent
risk being insured against; it is a rate.

**A fifth, found while writing this, and deliberately not filed as a bug.**
`CPCONTROLLER` is dispatched by both, but `RegistryState` has no `controllers`
field: the Raft path keeps announcements in `Ha::controllers`, a node-local
`Mutex`. So a controller that announces to node 1 is invisible to `status`
asked of node 2. That may well be right — ephemeral liveness data does not
obviously belong in a replicated log — but it is a behavioural difference
nobody chose in writing, and the unification has to decide it rather than
inherit it.

## What the guards can and cannot do

Two structural checks landed this week and both earn their place:
`assert_cp_verbs_agree_across_paths` compares the dispatch tables, and the
lease-key test now reads all four control-plane sources rather than one.

Neither closes this. A verb table catches **the arm somebody forgot to add**.
Nothing textual catches **the arm somebody forgot to update**, which is
ADR-0030's shape and BUG-0150's, and which is the expensive half. A guard that
compares two implementations to each other is also only ever as good as the
question it asks; the one BUG-0150 broke had been asking a true question about
the wrong file for eighteen days, confidently.

## The candidates

**A — single-node runs the same state machine.** `main.rs`'s dispatch builds a
`Mutation` and applies it to a `RegistryState` directly, persisting after each
apply; Raft's only difference is that `propose` goes through the log first.
`state::State`'s mutation paths are deleted, and with them one `shuffle_shard`.

**B — extract the mutation bodies into a shared module both call**, keeping
both state types and both formats.

**C — keep both, keep adding guards.** The status quo plus vigilance.

**D — delete single-node.** Raft always, including for drills.

## Recommendation: A, staged

**Over B**, because B leaves two state types, therefore two durable formats and
two sets of invariants, and closes only the half of the problem that is code.
Every defect above was behaviour drift, and B would have prevented ADR-0030's
and BUG-0150's while leaving BUG-0151's third copy exactly where it is.

**Over C**, because C is what we have been doing. It caught BUG-0148 and by
construction cannot catch the other three.

**Over D**, because single-node is what most drills and every small deployment
run, and removing it is a product decision with a blast radius far larger than
this refactor. If the answer to the open question below is "development
convenience", D becomes the cheaper answer and most of this work disappears —
which is why the question is asked before the work starts, not after.

Staged, because the durable format is the real cost and it should move on its
own:

1. **`RegistryState` becomes the single state type.** `State` keeps only
   loading: read the hand-written format, produce a `RegistryState`. Decide
   `controllers` explicitly here.
2. **Persist `RegistryState` on the single-node path**, writing serde and
   reading either — one release of tolerant reading before the old writer goes.
3. **Dispatch builds `Mutation`.** The two dispatch functions stay, because the
   transports genuinely differ; what stops being duplicated is what each verb
   MEANS. The verb-parity guard already asserts the tables match.
4. **Delete the duplicate `shuffle_shard`**, and let the fast mirror be derived
   at one place from the applied state rather than maintained beside it.

## Consequences

**`apply()` becomes the only place a mutation means anything**, which is what
makes a single unit test authoritative about the product — the thing ADR-0030
discovered it was not.

**Mixed-version replay is already a live consideration and gets no worse.**
ADR-0030's refill and BUG-0151's repoint both changed what `apply` does with a
committed log entry; nodes at different versions replaying one can diverge. The
answer has twice been that a correctness fix is worth a rolling-upgrade window.
This ADR does not change that trade, it concentrates it.

**The single-node durable format changes**, and that is the one irreversible
part. It wants its own release and its own gate, with a tolerant reader shipped
first — which is why it is step 1 and 2 rather than folded into the rest.

**The drills keep pointing where they point.** `subset_ratchet` found ADR-0030's
miss and `lease_after_repoint` found BUG-0151's, both against the single-node
plane. Under this change they exercise the same code the raft path runs, which
is the point — but the Raft-specific drills (`controlplane_ha`, `ctl_cpha`,
`cpha_roll`) stay, because the transport is what they are about.

## The open question, for Jeff

**Is single-node a supported deployment, or a development convenience?**
Everything above assumes the former. If it is the latter, candidate D is
cheaper than A and this ADR becomes a deletion plan instead of a refactor.
