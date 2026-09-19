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
   `controllers` explicitly here. **DONE 2026-09-16** — see "Step 1" below.
2. **Persist `RegistryState` on the single-node path**, writing serde and
   reading either — one release of tolerant reading before the old writer goes.
   **The READING half landed 2026-09-16** — see "Step 2's reader, shipped
   alone" below. **The WRITER is written and gated but NOT pushed** (2026-09-18)
   — see "Step 2's writer" below for the two conditions it is waiting on.
3. **Dispatch builds `Mutation`.** The two dispatch functions stay, because the
   transports genuinely differ; what stops being duplicated is what each verb
   MEANS. The verb-parity guard already asserts the tables match.
   **DONE 2026-09-17** — see "Step 3" below. It left the verbs' REFUSALS in two
   places, and eleven of them disagree (BUG-0160).
4. **Delete the duplicate `shuffle_shard`**, and let the fast mirror be derived
   at one place from the applied state rather than maintained beside it.
   **First half done 2026-09-16** with step 1, because unifying the types made
   it a re-export. **Second half done 2026-09-17** — see "Step 4" below.

## Consequences

**`apply()` becomes the only place a mutation means anything**, which is what
makes a single unit test authoritative about the product — the thing ADR-0030
discovered it was not. (Step 3 split it: the meaning is `apply_mutation`, and
`apply` is that plus the Raft path's version bump. A unit test of an arm is
authoritative about what a verb DOES on both paths, and says nothing about what
either path refuses.)

**Mixed-version replay is already a live consideration and gets no worse.**
ADR-0030's refill and BUG-0151's repoint both changed what `apply` does with a
committed log entry; nodes at different versions replaying one can diverge. The
answer has twice been that a correctness fix is worth a rolling-upgrade window.
This ADR does not change that trade, it concentrates it.

**The single-node durable format changes**, and that is the one irreversible
part. It wants its own release and its own gate, with a tolerant reader shipped
first — which is why it is step 1 and 2 rather than folded into the rest.

## Step 1, done (2026-09-16)

Staged inside the step, because the type swap is 159 call sites in `main.rs`
alone and the duplication it removes is worth removing before that lands rather
than inside it.

**Done: one renderer.** `State::snapshot_for` and `RegistryState::snapshot_for`
were separate assemblies of the same six elements, from the same helpers, in the
same order — identical except that one factored the admin digest into
`admin_digests()` and the other inlined the closure. A third copy of the same
defect class the ADR was written for, found while reading for step 1 rather than
by a check.

Both now fill one `tenant::SnapshotSource` and call one
`tenant::snapshot_tuple`. `admin_digests` moved to `tenant.rs` with them.

- **A struct, not nine positional arguments**, and that is not style. The two
  longest arguments are `admin_token` and `admin_prev`, both
  `&Option<String>`, adjacent, and swapping them is silent: the snapshot still
  renders, the digests are still digests, and a proxy accepts the PREVIOUS
  admin token as current for the whole rotation window. Named fields make that
  a compile error instead of a security regression nothing observes.
- **`both_state_types_render_the_same_snapshot`** is what stops them becoming
  two again, and it compares the tuple WHOLE. Element order is a wire contract
  with every proxy, so two control planes that agree on the contents and
  disagree on where element 4 sits is the expensive version of this failure —
  and a per-field assertion written in the same order as the bug would not see
  it. Red on an admin-field swap and on a stale version, each killing only that
  test.
- **What that test cannot do**, stated so nobody reads it as more: it catches
  DIVERGENCE, not a change to the shared renderer. Both sides move together
  now, which is the point; the proxy's parser and the conformance suite are what
  hold the format itself.
- A delegating `State::admin_digests` wrapper was left behind for one minute
  and clippy's `-D warnings` called it dead, correctly — its only caller was the
  assembly that moved.

**Done: the type swap.** There is now **one state struct in the crate**.
`RegistryState` is it; `State` is `pub use crate::registry::RegistryState as
State`, so the call sites that are about the single-node plane still read
naturally while the type a mutation means something to is the same on both
paths.

It was far smaller than "159 sites" suggested, and the measurement is worth
keeping because it is what made the change safe to attempt: of those, 41 are
`state.lock()` on the mutex and untouched, ~80 are **field accesses whose names
were already identical on both structs**, and only 32 were method calls — 27 of
them `commit()`.

- **`controllers` moved onto `RegistryState` with `#[serde(skip)]`, beside
  `promoted`**, which is the question this step was told to decide explicitly.
  Same category: a live report from processes that may not be running, never
  Rafted, because committing a heartbeat would wake every watching proxy to say
  a controller said hello.
- **`path` moved with it**, also `#[serde(skip)]`, and that is the one piece
  worth arguing. It keeps `commit()` a method on the one state type, so its 27
  call sites did not each have to learn where the file lives. Under Raft it is
  `None` by construction and `commit()` is a no-op, because durability there is
  the log's job. The alternative — a free function taking the path — would have
  been purer and would have made this a 27-site rewrite instead of a
  re-declaration.
- **`state.rs` keeps only the hand-written line format**: `load_or_new`,
  `serialize`, `commit`. That is a property of the single-node *deployment*, not
  of the state, and step 2 deletes it.
- **`absorb_registry` is gone** — with one type there is nothing to convert.
  The tolerant reader now parses straight into the state and restores the two
  things serde cannot carry: `path`, whose absence would make `commit()` write
  nowhere and still return `Ok`, and the `ranges` padding.
- **The duplicate `shuffle_shard` went too** (step 4's first half). The two
  copies were byte-identical but for one comment, and were reached by different
  callers — `main.rs` used `state`'s, `ha.rs` used `registry`'s — so the two
  planes placed tenants through two functions that merely happened to agree.
  Clippy then named `fnv1a` dead, the private seed whose only caller was the
  copy that went.

**And `both_state_types_render_the_same_snapshot` was DELETED rather than
carried forward green.** It compared the two renderers and was the guard that
stopped them diverging again; with one type it would compare a value with
itself. It cannot fail, and it cannot fail for the best possible reason — the
duplication is gone by construction — but a test that reads as coverage and is
none is the failure class this work exists to remove. The comment where it stood
says so. What replaced it is the type system: two renderers again requires
somebody to reintroduce a second struct, which nobody does by accident.

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

## Step 3, done (2026-09-17)

**Every mutation in `main.rs` now goes through `RegistryState::apply_mutation`**:
27 dispatch arms and the first-boot admin-token seed, 28 sites. None of them
writes a registry field directly any more.

**`apply` was split first, because the two paths disagree about who owns the
version.** Under Raft the log's ordering defines it, so `apply` bumps. On the
single-node path persistence defines it, so `commit()` bumps. Calling `apply`
from single-node dispatch would have bumped every verb twice. So
`apply_mutation` is the match and nothing else: no version, no persistence, no
wakeup. `apply` is the version bump plus that call, and it is still the Raft
path's only entry.

**The rule each arm followed: dispatch keeps the DECISION, the mutation takes
the CHANGE.** What to refuse, and whether anything happens at all, stays in the
handler. What happening means is the mutation's. Most arms converted
mechanically. These did not, and each is why the rule is worded that way:

- **`CPDELPROXY` is where ADR-0030 lived.** The retire-and-refill had two
  copies, and the first version of that fix landed in only one of them. It now
  has one copy.
- **`CPSETPAIR` reads `old` without mutating.** The handler used
  `mem::replace` to capture the old membership. Doing that and then applying
  `SetPair` would repoint the lease row twice, the second time from the new
  membership to itself.
- **`CPCLEARSLOT` asks before it applies.** The refusal (`no such exception`)
  used to come from `clear_slot_owner`'s return value. A mutation cannot hand
  one back, because a log entry cannot mean different things depending on what
  it found. So coverage became `tenant::covers_slot`, and `clear_slot_owner`
  stopped returning anything. Both use one private `run_covers`, so the question
  the handler asks and the rows the mutation removes cannot disagree.
- **`CPADDPROXY`, `CPADDPAIR` and `CPADMINDROPPREV` keep their condition.**
  Applying unconditionally would be correct, because every mutation is total.
  It would also commit, bump the version and wake every watch loop for a no-op.
- **`CPMYROTATE` and `CPMYCONFIG` resolve the tenant by MAP KEY**, since that is
  what the mutation looks it up by. They used to edit the entry the token lookup
  matched.

**Two single-node behaviours changed**, and each has a test at the command
surface in `main.rs` (`step3_dispatch_tests`):

- **A lost lease-adoption race answers from the winning row.** Two first
  touches for one pair can both miss the fast mirror, which is read under a
  different lock. The loser then took the state lock and pushed a SECOND durable
  row behind the winner's. `LeaseAdopt` refuses to. So a straight conversion
  would have committed and mirrored a row the mutation never wrote, and the
  mirror would have drifted from its record. The loser now gets `OK` or
  `SUPERSEDED <master>` from the existing row, which is what the fast path would
  have said a moment later. Found by making dispatch agree with the mutation,
  not observed in a run.
- **`CPFENCE` bumps the version once.** Single-node bumped it twice:
  `commit()`, and then an explicit increment to publish a promotion hint that was
  set after the commit. `Fence` sets the hint before the commit, so that second
  bump had nothing left to publish. Raft has always bumped once for this verb.
  Both callers, the controller and flintctl, check only for a `+` reply, and the
  bump's job is waking `CPWATCH`.

**`CPPROMOTED` is the one verb whose bump dispatch still owns.** It never
commits, because the hint is deliberately not durable, so no `commit()` exists
to bump for it.

**Deleted: `set_exception`, `clear_exception` and `consolidate`.** Step 1 put
them on the unified type so the single-node dispatch compiled unchanged. After
step 3 their only caller was a test, and a second route to one change is what
this step removes. The test now drives `apply_mutation`.

**What stays in dispatch, on purpose:**

- **The `Shared::leases` fast mirror** (`CPSETPAIR`, `CPFENCE`, `CPLEASE`). A
  cache is not state, so `apply_mutation` must not know about it. Deriving it
  from applied state is step 4's second half.
- **`CPLEASE`'s `leases.pop()` after a failed commit.** It undoes a change that
  persistence refused. It is not a verb.
- **`CPCONTROLLER` and `CPTENANTUSAGE`**, which were never registry state.

**What step 3 did not unify: refusals.** Comparing the two dispatchers arm by
arm for this step found that on Raft, eleven verbs skip the refusal single-node
makes. `CPTENANTQUOTA`, `CPSETSUBSET`, `CPDELPROXY` and eight more propose
unconditionally, commit a no-op, and reply `OK` for a name that does not exist.
The one drill asserting those refusals runs one control-plane seat. Filed as
**BUG-0160** rather than fixed here, because it is a change to what the
production control plane answers and deserves its own gate — and fixed there
the same day, with the refusals reading the leader's registry rather than
whichever seat answered.

## Step 4, done (2026-09-17)

**The fast mirror is now a copy of the applied state, taken in one place.**
`publish_lease_mirror(shared, &st)` assigns `lf.entries = st.leases.clone()`,
and the three arms that touch a lease row — `CPLEASE`'s adoption, `CPFENCE`,
`CPSETPAIR` — call it instead of each patching the cache in its own shape: a
repoint here, a push there, a generation written by hand in the third.

**Why a copy rather than three correct patches.** The patches WERE correct,
individually, and that is the point: BUG-0151 was one of them agreeing with the
durable row until a repoint made it disagree, and nothing could report the
drift because each arm was the only place that knew what it had written. A copy
cannot disagree with what it copies.

**Taken under the state lock**, in the documented order (state → leases). Two
mutations therefore cannot interleave and leave the mirror describing neither:
whoever holds `state` decides what the mirror says next. The alternative —
publishing after dropping the state lock, as the old patches did — needs a
version guard to stop an older copy landing last, and a guard is a second thing
to get right.

**The renewal path is untouched**, which is the property that matters:
`CPLEASE` still takes only the lease lock on its hot path, and a renewal
delayed past its TTL still fences a healthy master. This copies a handful of
rows while holding a lock the renewal path never wants.

**What the test asserts is the invariant, not the spelling.**
`every_lease_verb_leaves_the_mirror_equal_to_the_record` runs an adoption, a
fence and a repoint, and after each one compares `lf.entries` with `st.leases`
whole. A test written as "the repoint moved the cached row too" would pass
against three patches that agree today and say nothing about the next arm
somebody adds.

**Three bindings went with the patches**: `old` in `CPSETPAIR` and `members` in
the two lease arms existed to feed the hand-written cache updates. They are now
the checks they always were — an index in range, an address that belongs to a
registered pair — with nothing read out of them.

## Step 2's writer, written and held (2026-09-18)

`commit()` encodes the registry with serde instead of the hand-written line
format. The line PARSER stays — every state file written before the upgrade is
in that format, and it migrates on the first commit after, not at load.

**Held, not pushed, on two conditions that are not the code's to satisfy:**

1. **Jeff's word.** The ADR is accepted and the release condition below is met,
   but the format change is the irreversible part of this work and the peer
   relayed a go-ahead for the *type swap*, which is a different step. An
   inherited reading of somebody else's sentence is not an approval for this
   one.
2. **The rollback FLOOR, which is a tag rule and not a file on the box.**
   Corrected here: I first wrote that `/opt/flint/bin.rc72` is what the runbook
   rolls back to. It is not. `roll-fleet.sh` keeps no copy of the outgoing
   `/opt/flint/bin`, the `bin.rc*` directories are residue from hand-rolls, and
   the runbook's rollback section exists precisely to stop somebody assuming
   one is there — rollback is **re-rolling the previous tag**.

   So the condition is about which tag: after this writer ships, never re-roll
   below the release that first carried the tolerant reader (rc.73). **A
   pre-rc.73 binary does not refuse the file — it empties it.** That parser's
   `match` ends in `_ => {}`, so every line of a JSON state file is ignored,
   the load succeeds with no proxies, no pairs and no tenants, and the next
   `commit()` writes that back over the real registry. Tracked as **OPS-0264**,
   which puts the floor in the runbook's rollback section and the release
   checklist.

**OPS-0259's staging window is fine for this**, which is worth saying because
it was the obvious worry: a seat that dies mid-stage comes back on the new
binary, so an rc.74 CP would write JSON while the rest of the fleet is rc.73 —
and rc.73 reads JSON. The window bites on the rollback path, not the roll.

**What the change is, in full:** `serialize()` is gone and `encode()` is
`serde_json::to_string_pretty`. Pretty on purpose: the line format's one real
virtue was that an operator could `cat` it mid-incident, the registry is small,
and compact JSON would save bytes nobody is short of.

**A guarantee that was quietly format-shaped, found by moving the writer.**
ADR-0006 D1 says the control plane holds token DIGESTS, never plaintext — and
that migration lived inside the LINE parser as a closure. Under the new writer
a state file is JSON, and the JSON path had no such rule: `digestify` is now a
module-level function both readers call, so the guarantee is a property of
`load_or_new` rather than of whichever format the file happened to be in. The
admin token is deliberately exempt: it is plaintext by design, because the
agent retrieves it and the proxies get only its digest.

**The test that held the boundary shut is now the test that it opened
deliberately.** `the_writer_has_not_moved_yet` became
`the_writer_has_moved_and_an_old_file_migrates_on_first_commit`: write the OLD
format (with the real former writer, kept as a test fixture so the sample
cannot drift from what the reader must accept), load, commit, and assert the
file is JSON and that every field survived. Plus a direct control that a
plaintext token is digested on load *from either format*, so the new invariant
is not held up only by a round-trip test that happened to notice.

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
