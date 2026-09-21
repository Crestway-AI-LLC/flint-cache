# BUG-0146 — the control plane implements every mutating verb twice

**Status:** OPEN — found 2026-09-14 by a fix that landed in one of the two and
was reported green by six unit tests. **The design this file asked for exists:
[ADR-0032](../adr/0032-one-implementation-of-every-control-plane-mutation.md),
ACCEPTED 2026-09-15 (Jeff) — candidate A, single-node runs the same state
machine, staged so the durable-format change moves on its own.** This stays
OPEN because the work is not done, not because the decision is outstanding.
**CORRECTED 2026-09-21: that sentence is stale and outlived its truth.** All
four ADR-0032 steps are done, and *Where that leaves this file* below says so:
the stated work is complete and what keeps this open is a suspicion, so
closing it **is** a record-keeping decision. The stale sentence had a measured
cost — it was read in isolation on 2026-09-21 and used to tell Jeff the close
was not his call, contradicting a correct earlier statement. A status block is
the first thing anybody reads and the last thing anybody updates.

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

### That audit, first pass (2026-09-19)

Run over all **42 verbs both dispatchers serve**, comparing each arm's
*argument-transformation profile* — the set of rules it applies to an argument
before the mutation is built: canonicalise, uppercase, split, trim, drop-empty,
hash, mint, shuffle, on/off decode, range-bound, prefix-validate, lease-row
key. Deliberately NOT control flow, because `state.lock()` against
`leader_view().await` is the transport and is different by design.

**40 of 42 profiles are identical. Both differences are explained and neither
is a defect:**

- **`CPFENCE` replies differently.** Single-node answers `OK fenced <addr> gen
  <g>`, reading the generation back out of the lease row; Raft answers `OK
  fenced <addr>`. Nothing consumes it: the controller's only test of that
  reply is `matches!(reply, Ok(Value::Simple(_)))`, so the generation is
  diagnostic. Recorded rather than fixed.
- **`CPSNAPSHOT` was an instrument artifact.** It is the last arm in
  `main.rs`, so the extractor ran past it into the trailing function
  definitions.

### What this pass does NOT establish, which is the important half

It compares the PRESENCE of a transform in each arm. It does not check that
two arms apply the same transform **to the same argument in the same way** —
both could call `to_ascii_uppercase` on different variables and read as
identical. So it rules out the coarse shape (a rule in one arm and absent from
the other, which is BUG-0150's shape and was BUG-0169's) and says nothing about
the fine one.

**The first version of the instrument produced a false negative**, which is why
that limit is stated rather than assumed. It stripped lines beginning `let
Some(` as parse boilerplate — and `CPSETSLOT`'s pair-index range check is
written exactly that way, so the audit reported the two arms as differing when
they do not. A tool built to find a rule present in one arm and missing from
the other was hiding rules. Checked by reading both arms, not by trusting it.

So: the coarse class is closed on this evidence and the fine class is untested.
A second pass would have to compare arms by reading, and 42 is a small enough
number that this is a bounded job rather than an open-ended one.

### Correction to the paragraph above (2026-09-20)

**It ran over 42 of 43 verbs, not all of them.** The extractor required an arm
to be written `b"CPX" => {`, and `ha.rs`'s `CPCONSOLIDATE` is written
`=> match ha.propose(...)` — an expression, not a block. That arm was invisible
to the pass, AND its text was swallowed by the arm above it, which then
reported a mutation the previous verb does not construct. A coverage claim of
"all 42 verbs both dispatchers serve" was therefore true of a set the tool had
defined for itself.

Re-run with every arm form matched, the final arm bounded at the match's
catch-all, and comments stripped: **43 of 43 common, and 0 constructing a
different set of mutations.** `CPCONSOLIDATE` checked by hand — both count rows
after applying, and `propose` returns after the entry is APPLIED, which
`leader_view`'s own comment states.

### The second pass (2026-09-20), and what it found

Comparing the construction EXPRESSIONS argument by argument across all 43:
seven differ textually and all seven are equivalent — variable naming
(`addr` vs `a`), field-init shorthand (`name: name` vs `name`), and
inline-versus-prebound reads of the same field (`st.admin_token` bound first
vs `reg.admin_token` inline). Verified by reading each, not by normalising
them away.

**So the mutation-construction path is clean, and the fine class turned out to
live somewhere the question was not pointed.** The arms also perform SIDE
EFFECTS on state that is not registry state and that no mutation reconciles —
and `CPDELTENANT` clears the tenant's `usage` row on the single-node path and
not on the Raft one. Filed and fixed as
[BUG-0171](0171-cpdeltenant-clears-the-usage-row-on-one-control-plane-and-not-the-other.md),
direction single-node-correct/Raft-wrong.

### The third pass (2026-09-20): the other non-registry state

The paragraph this replaces said the lease mirror, the controller registry and
the journal had the same exposure as the `usage` map and no check. Measured,
they do not — and the reasons differ, which is why "same exposure" was the
wrong shorthand:

| state | single-node | raft | verdict |
|---|---|---|---|
| `usage` | `insert`, `remove` | `insert`, `remove` | symmetric since BUG-0171, and guarded |
| `controllers` | `record_controller` into `st.controllers` | `record_controller` into `ha.controllers` | same writer, same renderer (`controller_line` is a one-line wrapper around `render_controllers`), `#[serde(skip)]` on both so neither persists |
| journal | `append_line`, `parse_kinds_arg`, `tail_kinds` | the same three | symmetric |
| leases | fast mirror + a `pop()` compensation | registry state | **different by design, not drift** |

**The lease difference is the one worth stating.** ADR-0018 gives the
single-node plane a mirror under its own lock so `CPLEASE` never queues behind
a snapshot being serialised; on Raft the rows ARE registry state. So there is
no shared structure to compare, and the `leases.pop()` after a failed commit —
which ADR-0032 already flagged as "not a verb" — has no Raft analogue because
a failed *propose* applies nothing to compensate for. An audit keyed on the
field name would have reported an asymmetry; the asymmetry is the design.

### Where that leaves this file

Everything it asked for has been done. The meaning of every mutation is
single, the verb tables are guarded, the refusals were unified as BUG-0160,
the constructions agree across all 43 arms, and the side effects beside them
have been measured — one defect found and fixed, the rest clean with reasons.

**What is NOT established is that no further class exists**, and three passes
each found one outside the previous one's scope, so that is a live caution
rather than a formality. The honest statement: this bug's stated work is
complete, and keeping it open now records a suspicion rather than a task.
Closing it is a record-keeping decision — worth taking deliberately, because
the classes below were each invisible until someone pointed a different
question at the same two files:

1. the apply path (ADR-0032)
2. the verb tables (`assert_cp_verbs_agree_across_paths`)
3. the refusals (BUG-0160)
4. the mutation constructions (pass 2)
5. the side effects beside them (pass 3, BUG-0171)
6. **the reply vocabulary (pass 4, 2026-09-21,
   [BUG-0173](0173-only-one-control-plane-has-a-reply-vocabulary.md))** — and
   it found a live divergence, so the caution above was not a formality.

### The fourth pass (2026-09-21): what each arm SAYS, not what it computes

The three passes before this compared what the arms *do*. None compared what
they *reply*. `main.rs` has `ok()` and `err()`, one definition each, and
`err()` prepends `ERR `. **`ha.rs` has neither** — 77 `Value::Error` and 20
`Value::Simple("OK")` sites written out by hand. The `ERR ` rule is therefore
automatic on one path and copied on the other, which is this file's whole
subject.

It had already diverged: both single-node `NOPAIR` sites said
`err("NOPAIR …")`, giving `-ERR NOPAIR …`, while Raft gave `-NOPAIR …`. Raft
is right — `NOPAIR` is a code like the `LEADER`/`SUPERSEDED`/`WRONGPASS`
written bare beside it. Fixed, with a guard whose code set is derived from
`ha.rs` rather than guessed from syntax, because a first attempt that flagged
any all-caps first word fired on forty correct usage messages.

**So a fourth question found a fourth class, which is the argument this file
has been making since its second pass.** What that means for closing it is
still a record-keeping decision and still Jeff's; the difference is that "no
further class exists" is now disproven once more rather than merely unproven.

## The part worth acting on first

**A unit test against `RegistryState` proves nothing about a single-node
fleet**, and single-node is what most drills and every small deployment run.
Until the paths are unified, a change to a mutation needs either both arms
edited or a drill that runs the path the tests do not.
