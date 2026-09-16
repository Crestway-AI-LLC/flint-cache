# ADR-0032 — one implementation of every control-plane mutation

**Status:** **ACCEPTED 2026-09-15 (Jeff)** — candidate **A**, single-node runs
the same state machine. Written because BUG-0146 asked for a design, and
because the duplication it describes produced **five** defects in eight days,
four of them in the last twenty-four hours.

**One premise below was wrong, and is corrected here rather than quietly
edited.** This ADR asked whether single-node is a supported deployment or a
development convenience, and said "single-node is what most drills and every
small deployment run". Jeff, 2026-09-15: **production control planes are three
nodes at minimum.** So the single-node control plane is drills and development
— supported, and not a production topology.

**That strengthens A rather than weakening it.** If production never runs the
single-node control plane, then every drill running against it exercises code
production does not run — ADR-0030's failure as a standing condition rather
than an accident. Unifying is what makes the drill suite mean something about
the shipped control plane.

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

**Over D** — decided, see the status block. Single-node turned out to be
drills and development rather than a deployment topology, which makes D cheaper
than this ADR first allowed. It was still not taken: the drills that found
ADR-0030's miss, BUG-0151 and BUG-0152 are all cheap deterministic single-CP
runs, and requiring three Raft nodes in each would cost more than the
duplication does.

Staged, because the durable format is the real cost and it should move on its
own:

1. **`RegistryState` becomes the single state type.** `State` keeps only
   loading: read the hand-written format, produce a `RegistryState`. Decide
   `controllers` explicitly here.
2. **Persist `RegistryState` on the single-node path**, writing serde and
   reading either — one release of tolerant reading before the old writer goes.
   **The READING half landed 2026-09-16** — see "Step 2's reader, shipped
   alone" below. The writer is unchanged and a test holds it that way.
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

## Step 2's reader, shipped alone (2026-09-16)

`State::load_or_new` now reads **either** format: serde JSON if the file begins
with `{`, the hand-written line format otherwise. **The writer is untouched.**

**Taken out of order on purpose.** The staging above puts the step-1 refactor
first, and this went ahead of it because a release was being cut the same day.
Step 1 is a refactor with no format consequence and can land any time; the
reader is the piece with a deadline, because *"one release of tolerant reading
before the old writer goes"* is only satisfied if the reader is in a release
that has actually shipped. Missing that cut would have pushed step 2 a full
release cycle, so the cheap half went in the cut and the expensive half waits.

**The direction that needs the lead time is backward.** A new binary reading an
old file has always worked. The case that bites is an OLD binary handed a file a
NEWER one wrote — a rollback, a half-finished roll, a drill fixture kept across
a version bump. That is why reader and writer cannot ship together.

Four things it decides, each with a test that was driven red:

- **The discriminator is the leading `{`**, which cannot collide: the line
  format's first line is always `version <n>` and every keyword it accepts is
  lowercase ASCII.
- **A damaged registry refuses to start rather than loading empty.** Falling
  through to the line parser was the tempting default and is the one direction
  that destroys data: that parser ignores unknown keywords, so a corrupt file
  reads as no pairs and no tenants, and the next `commit()` writes that back
  over the real registry.
- **`RegistryState` is destructured exhaustively.** A twelfth field added later
  is a compile error at the conversion rather than a field silently dropped on
  every single-node load — which is BUG-0152 exactly.
- **`controllers` stays absent from both formats**, answering ahead of time the
  question step 1 was told to decide explicitly. It is a live report from
  processes that may not be running, so a cold control plane knowing nothing is
  correct; it relearns within one heartbeat.

`ranges` is padded to the pair count on absorb, because the line loader always
writes one entry per pair while `RegistryState::ranges` is `#[serde(default)]`
and a pre-ranges snapshot arrives empty.

**The release boundary has its own test.** `the_writer_has_not_moved_yet` loads
JSON, commits, and asserts the file on disk is still the line format. It fails
the moment step 2's writer lands, which is the reminder that the writer must not
share a release with the reader.

**The drills keep pointing where they point.** `subset_ratchet` found ADR-0030's
miss and `lease_after_repoint` found BUG-0151's, both against the single-node
plane. Under this change they exercise the same code the raft path runs, which
is the point — but the Raft-specific drills (`controlplane_ha`, `ctl_cpha`,
`cpha_roll`) stay, because the transport is what they are about.

## The question this ADR asked, and its answer

**Was single-node a supported deployment, or a development convenience?**
Answered 2026-09-15: supported, but not a production topology — production is
three nodes at minimum. A was taken anyway, for the reason in the status block:
the value of unifying goes UP when the un-unified path is the one every drill
runs and no fleet does.

## A fifth instance, found in this ADR's first hour

BUG-0152: the single-node line format carried ten of `Tenant`'s twelve fields,
so `CPTENANTASYNC on` and `CPTENANTFEDERATE on` reverted on a control-plane
restart. **The direction is reversed** from BUG-0148 and BUG-0150 — here the
Raft path is correct, because serde carries every field it is given, and the
single-node path is wrong.

That is worth more than another tally mark. Two implementations do not drift in
a direction that can be predicted and watched; they drift. A reviewer who had
learned from BUG-0148 and BUG-0150 to check the Raft path would have looked
straight past this one.
