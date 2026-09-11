# BUG-0132: slot recovery can hand ownership to a node that does not have the data (OPEN)

Status: **OPEN**, found 2026-09-10 · Severity: **high, pending confirmation of
the mechanism** — no bytes are known destroyed, but after an interrupted slot
cutover a client can be redirected to a node that returns nothing for keys it
durably acked. The write is on disk and unreachable, which is
indistinguishable from loss at the client.

**This is the bug keeping core `main` red**, and it is not BUG-0131's chaos
break (fixed at `bbca61f`) nor the docs commits it was first attributed to.

## What CI shows

`slot_cutover_recovery` has failed **2 of the last 6 gate runs on `main`**, at
24.1s and 24.5s — near-identical durations, which is a timeout shape rather
than random noise. It failed on `e47bf84` (a documentation-only commit, so it
cannot have been caused by that change) and again on `bbca61f`.

The failing arm, from run 34564798603:

```
[delay 0.5] after restart: source=[] dest=[] -> completed pre-kill (recovery is a no-op)
  MISSING key000001 on dest
  MISSING key075000 on dest
  MISSING key149999 on dest
  FAIL: 3 keys lost after recovery
```

## Why this is not "a flaky drill"

**The failure lands AFTER the drill's `RESOLVED` check passed.** That check is
not a formality — it loops for up to 30s requiring BOTH:

- `dest` accepts a write for the slot, and
- `source` answers `-MOVED <slot> <dest>` for a read.

So recovery did not fail to act. It completed, it moved ownership to `dest`,
and it made `source` redirect the whole slot there. Only then were three of
four sampled keys absent on `dest`.

**The data was on `source`.** The drill seeds all 150,000 keys to `SPORT`
before the cutover begins:

```
awk ... | valkey-cli -p $SPORT --pipe
```

So the bytes are on source's disk. After recovery, source `-MOVED`s the slot
to dest and dest has nothing. **Stranded, not destroyed** — and from a client's
position the difference is invisible: it follows the redirect and gets nothing
for a write that was acked.

## The question the mechanism turns on

At `delay 0.5` the drill reports `source=[] dest=[]` — **no migration records
on either node** — and the first version of this file asked what made
*recovery* conclude that dest owned the slot.

**That was the wrong question.** Reading `recover_migrations` answers it: with
no records anywhere it returns before touching either node, so recovery was not
involved. The flip was already committed by the cutover itself. The corrected
account is under "What recovery actually did at `delay 0.5`" below; the
question that remains is **why the ownership flip is durable when the
destination's copy of the data is not**.

Kept rather than deleted because the wrong question was load-bearing for a
while, and because one of its two branches survives in a different place: a
`migrating` record CAN drive recovery to hand the slot to a dest that never
took the data. That is the second hazard below, and it is not what CI hit.

**Do not tune the drill's delays.** Moving the timing moves which phase the
kill lands in, and the phase is the finding.

## The drill cannot tell "not started" from "completed", and says so out loud

Line 59:

```bash
if [ -n "$SM" ] || [ -n "$DM" ]; then PHASE="INTERRUPTED mid-move"; else PHASE="completed pre-kill (recovery is a no-op)"; fi
```

No migration records on either side is rendered as **completed pre-kill**. It
equally means **never started**. There is no third branch, and the verdict text
then asserts one of the two readings as fact.

This is the same rule as ops OPS-0037 and BUG-0131: *"could not measure" must
never be recorded as "measured nothing"*, and *absence and not-yet must not
render alike*. Here it matters twice over, because the classification is what a
reader uses to decide whether the subsequent `MISSING` lines are alarming.

**FIXED in this commit** — the line now asks the source who owns the slot
rather than inferring it from an absence, and distinguishes *completed pre-kill*
(source already `-MOVED`) from *NEVER STARTED* (source still serves it) from
*indeterminate*. That is what established the corrected account below: on the
failing CI run the source had already flipped, so the cutover completed and the
data did not follow.

## RETRACTED: "it also fails locally on different arms"

An earlier version of this file reported three local failures — `delay 0.3`
resolving BACKWARD, and the deterministic `half-done-flip` arm — and built a
design question on them about whether recovery must always roll forward.

**All of it was a missing binary.** The drill had no `cargo build` line and ran
`./target/release/flint-controller` directly; this worktree had only
`flint-server`. The recovery controller never started in any local run, and
`$FLINT_DRILL_ROOT/flint-rec.log` said so plainly:

```
tools/slot_cutover_recovery_drill.sh: line 63: ./target/release/flint-controller: No such file or directory
```

So `delay 0.3` was simply the un-recovered interrupted state — dest holds an
`importing` record and redirects to source, source still serves the key —
which is what "nothing recovered it" looks like, not a rollback. **With the
controller built, the drill passes completely here: all three timed arms and
the half-done-flip arm.** The CI failure does not reproduce on this machine.

The lesson is the one already in the ops KB from the other direction: a drill
that runs a binary it does not build reports the *product's* failure text for
an *environment* fault, and the real cause sits in a log nobody opens. Fixed
below.

## What recovery actually did at `delay 0.5`, which is sharper than the first reading

`recover_migrations` (`crates/flint-controller/src/main.rs`) opens by
collecting `importing`, `migrating` and `aborted` records from every
`--recover-nodes` target, and then:

```rust
if importing.is_empty() && migrating.is_empty() && aborted.is_empty() {
    return;
}
```

The failing CI run reports `source=[] dest=[]`. **So recovery did nothing at
all** — it returned before touching either node.

But the drill's `RESOLVED` gate requires `source` to answer
`-MOVED <slot> <dest>`, and a source only reaches that state through
`FLINTSLOTMOVED`, which is the last step of a *successful* cutover. Nothing
else wrote it, because recovery returned early.

**So the flip committed before the kill, and the destination still did not
have the keys.** That reframes the bug: it is not "recovery hands a slot to the
wrong node" but **the ownership flip is not ordered after the durability of the
data it transfers**. The kill lands after the flip is durable and before dest's
copy is.

**This is a hypothesis from reading the code and one CI log, not a
measurement.** It has not been reproduced, and nobody has yet seen a failing
run's data directories.

### A second hazard, in the same function, that this failure does NOT exercise

When a `migrating` record IS present and the dest has no `aborted` marker, the
flip is completed on one condition:

```rust
if reachable(dest) { ... FLINTSLOTMOVED ... }
```

**Reachable, not "has the data".** BUG-0025 already hardened the neighbouring
inference — its comment is explicit that *"the dest has no Importing record,
therefore it owns the slot"* was wrong because an abort writes the same absence,
and records the cost: *"an acked write on the source, absent on the dest, gone
from both within seconds with this loop logging success."* The `aborted` marker
fixes the case where a dest SAYS it gave up. A dest killed before it could
write that marker is indistinguishable from a healthy one, and this branch will
hand it the slot.

Not exercised by the CI failure above, which never reaches recovery at all.
Recorded because it is the same defect one branch over, and whoever fixes the
ordering should decide whether "reachable" is the right predicate here too.

## What the drill now does, so the next failure is answerable

The CI failure could not be diagnosed from two occurrences, and three of the
reasons were in the drill rather than the product:

- **It builds what it runs.** `cargo build` for `flint-server` and
  `flint-controller`, plus an `-x` check on each, so a missing binary fails
  saying so instead of failing in the product's words.
- **It keeps the evidence.** Every failure path used to `rm -rf` both data
  directories — destroying the one artifact that answers stranded-versus-lost,
  on exactly the runs where the question arises. Failures now print where the
  directories are and leave them.
- **It says WHERE the bytes are.** On a missing key it reports
  `source DBSIZE` and `dest DBSIZE` against the seeded count. Deliberately NOT
  a per-key `GET` on the source: past the split-ownership assertion the source
  answers `-MOVED` for every key in the slot, so that branch could never return
  a value and would be dead code reporting "lost" in all cases. `DBSIZE`
  answers through the redirect because it counts what the node HOLDS. Verified
  by forcing the path with a key that was never written: a healthy run reports
  `dest DBSIZE=150000`.
- **The phase classification has a third state.** `source=[] dest=[]` now asks
  the source who owns the slot instead of inferring it, and distinguishes
  *completed pre-kill* (source already `-MOVED`) from *NEVER STARTED* (source
  still serves) from *indeterminate*. Those were rendered identically, and the
  verdict text then asserted one of them.
- **Mode fixed**, 100644 → 100755, so it runs directly like its peers.

## What is NOT established

- **Whether any byte was lost.** Still unanswered, but no longer
  unanswerable: the drill now preserves both data directories on failure and
  prints both DBSIZE counts. The next occurrence settles it.
- **Whether this is new.** No bisect has been run. It failed on a
  documentation-only commit, so it predates today's changes, but how far back
  is unknown.
- **Whether the local failures and the CI failure share a cause.** Different
  arms, different platforms, same drill.

- **Whether the CI failure reproduces anywhere else.** It does not reproduce
  on this Mac at any of the three delays. The drill is timing-sensitive by
  construction, so that is weak evidence about the Linux runner rather than
  about the product.

## How it was found

Watching CI after landing an unrelated fix, rather than assuming the peer's
chaos fix would turn `main` green. It did turn chaos green; `main` stayed red
for a different reason, in a different job, that had already fired once and
been attributed to the wrong commit.
