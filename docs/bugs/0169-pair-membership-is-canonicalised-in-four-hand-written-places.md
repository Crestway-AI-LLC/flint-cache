# BUG-0169 — pair membership is canonicalised in four hand-written places (FIXED 2026-09-19)

**Status:** FIXED 2026-09-19 · Severity: **no live defect — an invariant with
four copies, two of which have already been the site of a miss.**

Found reading [BUG-0146](0146-the-control-plane-implements-every-mutating-verb-twice.md),
which is still OPEN with *"Not done: unifying the two paths"* while
[ADR-0032](../adr/0032-one-implementation-of-every-control-plane-mutation.md)
reports steps 1 through 4 all done.

## What the ADR did deliver, measured rather than assumed

- `main.rs`'s single-node arms call `st.apply_mutation(registry::Mutation::…)`
  — the same state machine `ha.rs` proposes into. The *meaning* of a mutation
  is single.
- `assert_cp_verbs_agree_across_paths` (`tools/gates.sh:2013`) guards the two
  dispatchers' verb TABLES. 44 and 44 today.
- Refusals were split out as BUG-0160 and fixed there.

That check says plainly what it cannot do: *"This catches the arm somebody
forgot to ADD. Nothing here catches the arm somebody forgot to UPDATE."*

## The residual, and it is BUG-0146's class exactly

BUG-0065's root fix is that pair membership must be **canonical**. The dedupe
deciding whether a registration is new is vector EQUALITY; every lease lookup
is membership CONTAINMENT. Unsorted, `CPADDPAIR a,b` and `CPADDPAIR b,a` are
TWO pairs to the dedupe and ONE pair to every containment check — which is how
a lease row written under one key gets read through another.

That canonicalisation was a `.sort()` written out at **four sites**:

| file | verb | line |
|---|---|---|
| `main.rs` | `CPADDPAIR` | 241 |
| `main.rs` | `CPSETPAIR` | 281 |
| `ha.rs` | `CPADDPAIR` | 633 |
| `ha.rs` | `CPSETPAIR` | 1113 |

Those are exactly the four places that construct `Mutation::AddPair` or
`Mutation::SetPair` — verified by grepping every construction, not by reading
around. Each carried its own paragraph re-deriving the paragraph above.

**All four agreed. Nothing made them.** And two of the four have already been
the site of a miss: **BUG-0150** was `ha.rs`'s `CPADDPAIR` lacking the sort
that `main.rs`'s had, and **BUG-0151** was the `CPSETPAIR` path. Both were
found by drills, because a unit test against `RegistryState` proves nothing
about a single-node fleet — which is BUG-0146's own headline.

## Why it is not in `apply()`, and why that stays true

Canonicalising inside the state machine would change how **already-committed
log entries replay**. The comments at all four sites said so and they were
right. So the invariant belongs to whoever BUILDS the mutation — which is what
made it four copies rather than one.

The fix keeps that property and removes the copies: `state::canonical_members`
is one function in the handler layer, called from all four sites.
`apply()` is untouched. It follows the pattern BUG-0146 itself established with
`tenant::refill_after_retire` — *"the refill is one function called from both
paths"*.

`clean()` moved with it. It was byte-identical private copies in `main.rs` and
`ha.rs`; `canonical_members` needs it from both, so the move is forced by the
fix rather than scope added to it.

## Deduplication, not a behaviour change

`canonical_members` rejects **exactly** what the four sites rejected. `a,,b`
still parses to an empty member and is still accepted. That may well be worth
refusing, and refusing it is not this change — a widening smuggled in behind a
deduplication is a change to what the control plane answers that nobody
reviewed. A test pins the current answer so that a later widening has to be a
decision.

## The guard, and both directions

`membership_is_canonicalised_in_one_place_only`, in the style of the
lease-row guard beside it:

- **the COUNT** — each dispatcher must reference `canonical_members` at least
  twice, one per arm. This is BUG-0150's shape: an arm that stopped
  canonicalising.
- **the ABSENCE** — neither dispatcher may contain `.sort()`. This is how the
  copies appeared in the first place, and a count alone reads it as healthy.
  Production halves only, both currently zero.

Plus a control asserting the forbidden pattern can actually match a sort,
because a pattern that matches nothing certifies both files by reading neither.

**`canonicalising_registration_collapses_reordered_pairs` now calls the
function.** It used to sort two vectors itself and assert that sorting works —
true of `sort`, and silent about the control plane. That is the BUG-0146 shape
in a test: six unit tests passing against the type while the product did
nothing.

## Four mutations, all killed — and two instrument bugs before them

Killed: an arm that stops canonicalising; a dispatcher that starts sorting by
hand again; `canonical_members` that stops sorting; `canonical_members` that
quietly widens validation.

**The guard was wrong twice first, and both are worth keeping.**

1. Counting the raw source, the "arm stops canonicalising" mutant **passed**.
   A comment I had written two edits earlier — on the line the call used to
   occupy, naming the function to explain why the sort was gone — held the
   count at the threshold. The guard read the *explanation* of the thing
   instead of the thing. `assert_cp_verbs_agree_across_paths` names this trap
   in its own source and I walked into it anyway.
2. Requiring `canonical_members(` with a paren then failed on the **clean
   tree**: the call is `and_then(canonical_members)`, a function reference with
   no paren of its own. A pattern narrow enough to exclude prose was narrow
   enough to exclude the real call.

It now strips comment lines and counts the name in what remains. Both arms read
the stripped code, so a commented-out `.sort()` does not trip the absence check
either.

## What this does NOT close

**BUG-0146 is not closed by this**, and saying so would need an audit this did
not do. What is established: the mutation *meaning* is single, verb tables are
guarded, refusals were unified, and membership canonicalisation is now one
function. What is NOT established is the same claim for every other verb — each
dispatcher still owns its own parse, its own liveness check (`state.lock()` vs
`leader_view()`) and its own reply, and whether any of those still carry a
semantic rule the other must mirror by hand has not been checked arm by arm.
That audit is what a close would require.
