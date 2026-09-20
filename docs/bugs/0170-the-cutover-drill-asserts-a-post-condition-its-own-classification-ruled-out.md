# BUG-0170 — the cutover drill asserts a post-condition its own classification ruled out (FIXED 2026-09-19)

**Status:** FIXED 2026-09-19 · Severity: **it reddened main on a commit that
changed one markdown file, and its failure text names the product.**

## What happened

The `gate` CI leg failed on public `b5223eb`. That commit is **markdown only**
— `git diff --name-only efd3054 b5223eb` is one `.md` file, 41 added lines,
zero code — and the nine gate runs before it were green. The drill failed code
byte-identical to code it had just passed. A re-run of the same sha passed.

The failure:

    [killed in phase pull] after restart: source=[] dest=[] -> NEVER STARTED (source still owns and serves the slot)
    FAIL: move not resolved after recovery (dest write='OK' source read='val-000000')

Those two lines are adjacent, and they contradict each other.

## The mechanism

`slot_cutover_recovery_drill.sh` kills both nodes at an observed phase,
restarts them, and classifies the interrupted state from the durable manifests.
**BUG-0132 already taught it that empty manifests on both nodes are
ambiguous**, so it asks the source who owns the slot and reports four outcomes:
`INTERRUPTED mid-move`, `completed pre-kill`, `NEVER STARTED`, and
`indeterminate`. Its own comment calls two of them *"opposite facts"* that the
previous version *"rendered identically"*.

**It then ran the recovery controller and asserted the same post-condition for
all four**: the dest accepts a write AND the source answers `-MOVED`.

In the NEVER STARTED case there is nothing durable to reconcile from, so the
recovery controller will never move the slot and the source correctly keeps
serving it. The assertion cannot hold. The drill waited 30 seconds for it, then
printed `move not resolved after recovery` — which reads as a product defect
and is a correct system.

**This is BUG-0132's class one layer up.** That fix made the OBSERVATION
three-state and left the ASSERTION one-state.

## What it is not

**Not a split.** The `dest write='OK'` in the failure text is not evidence of
two owners: these are two standalone servers rather than a cluster with a
shared slot map, so a dest holding no migration record has no reason to refuse
a write. A split is two nodes each answering as OWNER.

**Not data loss.** The move did not happen. The source holds everything it was
seeded with, and an operator would re-issue the move.

## The fix

Each outcome gets the post-condition that belongs to it.

- `indeterminate` now fails **explicitly**, saying ownership could not be
  established, instead of falling into an assertion it cannot satisfy and
  blaming the product.
- `NEVER STARTED` asserts the **safe** outcome: the source serves reads and
  accepts writes for the slot, every sampled key is still on it, and the arm
  reports that it did **not** exercise recovery.
- It still **runs the recovery controller** first. Skipping it would leave the
  interesting question unasked; running it asserts the stronger property, that
  recovery does not FABRICATE a move from an empty manifest.

**The dest's holdings are reported, not asserted**, and that restraint is the
point. `wait_for_phase pull` waits for the dest to report an `importing` record
AND to hold rows, so this branch is reached only when that record is gone after
the restart. Whether the ROWS also went is a question nothing here has
observed. Asserting "the dest is empty" would have been a guess, and a wrong
guess turns one misreporting drill into another.

## A green run that exercised no recovery now fails

Each `never-started` arm is a legitimate pass, so a run made **entirely** of
them would be a green verdict over a recovery path that never ran — the same
shape as a check that matches no files. The raced arms now record their
classification and the drill fails if none of them reached recovery.

## And the branch is constructed, not raced

`assert_never_started` runs on the raced path roughly one run in ten, so
shipping it on that strength would mean shipping a branch that had never
executed — the shape of defect this whole bug is about. The drill already makes
this argument for the frozen window: *"constructing a state beats racing for it
whenever the state can be constructed."* So a deterministic arm builds the
state directly — two nodes, the corpus on the source, no migration ever issued
— and asserts its precondition (no records anywhere) before trusting anything
below it.

Verified: the drill passes with the raced arms landing on `INTERRUPTED` and
`completed`, and the constructed arm exercising every line of the new branch. A
mutation deleting one sampled key from the source is caught (`MISSING
key075000`, `FAIL: 1 keys lost`, exit 1). That mutation also exposed a
diagnostic of mine that lied about its own fixture — the `WHERE` line reported
`seeded 150000` in an arm that seeded 2000 — now taken from the arm's own count.

## The question this leaves open, deliberately unanswered here

`wait_for_phase pull` returns only once the dest **reports an `importing`
record** and holds rows. So at kill time that record existed and was
observable, and after the restart it was gone from both nodes. **A migration
record that `FLINTMIGRATIONS` reports is therefore not durable at that point**,
and a whole-cluster kill there loses the fact that a move was in flight — along
with whatever rows had already been copied, which may or may not survive on the
dest with nothing to explain them.

Nothing is claimed about whether that is acceptable. It is safe in the sense
this drill measures — no split, no loss, the move simply did not happen — and
whether a move should be durably recorded before its copy phase becomes
observable is a design question for whoever owns the cutover path. Raised here
because this drill is the only thing that has ever looked.
