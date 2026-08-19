# BUG-0025: migration recovery completes a flip onto a destination that never imported, destroying acked writes (FIXED)

Status: **FIXED 2026-08-19** with an `Aborted` phase · found 2026-08-18 while acting on BUG-0024's fix list ·
Severity: **high but LATENT** — measured acked-write loss with the recovery
controller reporting success, but no deployed controller enables the reconcile
today (see "Why this is not on fire"). It is primed to fire the moment
recovery is wired in, which is precisely what BUG-0024 recommended doing.

## What happens

`recover_migrations` (`crates/flint-controller/src/main.rs:1378`) resolves an
interrupted slot move by reading both participants' durable records. Its rule
for a frozen source:

> a source Migrating S to dest D whose Importing is already gone (D owns it —
> the flip half-completed) -> COMPLETE (tell O to answer -MOVED)

The parenthetical is an **inference**, not an observation, and it is wrong for
at least two of the three ways that absence arises.

Measured. Source frozen to a destination that holds nothing:

    seeded 20000 keys in slot 13624 on the SOURCE
      source DBSIZE=20000   dest DBSIZE=0
      source FLINTMIGRATIONS = [13624 migrating 127.0.0.1:6585 0]
      dest   FLINTMIGRATIONS = []

    [RBREC] recovery: completing flip of slot 13624 — 127.0.0.1:6584 -> Moved to 127.0.0.1:6585

    AFTER 6s of recovery:
      source GET   -> MOVED 13624 127.0.0.1:6585
      source DBSIZE=0   dest DBSIZE=0

20 000 rows purged in under six seconds, the controller logging success, and
every client now redirected to a node that has nothing.

## The realistic shape: an acked write, and the drill still passes

The extreme case above needs a destination with no data. The ordinary case
needs only a **tail** — the writes a real cutover drains under the freeze.
This is `slot_cutover_recovery_drill.sh`'s own `test_half_done_flip` setup
with **one key added**:

    BEFORE:  source DBSIZE=2001 ({mover}:late = ACKED-BEFORE-FREEZE)  dest DBSIZE=2000
    AFTER:   source DBSIZE=0                                          dest DBSIZE=2000
             dest {mover}:late -> ''

`{mover}:late` was acked by the source before the freeze. After recovery it
exists on neither node. **And the drill's assertion passes** — it checks only
that the source answers `-MOVED`:

    if echo "$(valkey-cli -p $SPORT GET "{mover}:key000000" 2>&1)" | grep -qE "MOVED $SLOT $DADDR"; then RESOLVED=1

It never reads a key from the destination. `run_once` does check dest keys,
but that path kills both nodes, which **preserves** the dest's Importing
record on disk — so it always takes the resume branch and never reaches this
one. The branch with the data assertion cannot enter the dangerous state, and
the branch that enters it has no data assertion.

## Root cause: three states, one manifest

The destination's `Importing` record is absent in all of these:

| how it got there | does the dest hold the slot? |
|---|---|
| pre-flip `clear_migration` at cutover step 5 | **yes** — the flip really is half-done |
| `rollback()` after the source was frozen | partially — it has the bulk pull, not the tail |
| data-ship-only `FLINTMIGRATEIN` (no self-addr, never writes Importing) | yes, but it was never a cutover |

`rollback()` and the pre-flip clear are literally the same call —
`manifest::clear_migration(kv, ns, slot)` — so no amount of care reading the
manifests can separate them. There are **eight** `rollback()` sites in
`migrate.rs`, and several are reachable *after* the freeze succeeds: the
source returning an error mid-drain, the source closing the connection, a read
error, and the 120-second drain deadline. None is exotic.

The comment directly above `recover_migrations` cites ADR-0004 — "every
structural state is observable from manifests". This is a state that is not.
The ADR's premise is sound; this function quietly stopped satisfying it by
folding an abort and a completion onto the same bytes.

## Why this is not on fire

`controller_args` (`crates/flint-ctl/src/main.rs:2831`) never passes
`--recover-nodes`, and `recover_migrations` is gated on
`!cfg.recover_nodes.is_empty()`. Only `slot_cutover_recovery_drill.sh` passes
it. **So no fleet `flintctl` starts runs this reconcile at all** — not the
playground, not any release.

That is the mitigation and it is also its own finding: the recovery this drill
proves works is not enabled anywhere real. A drill that green-lights a
capability the product never turns on is a claim about code, not about the
system.

## Reachability narrowed by BUG-0024's fix (2026-08-19), not removed

The dangerous state is "source `Migrating`, destination holds no `Importing`".
Since `35aea11` the destination unfreezes the source it froze, via
`FLINTSLOTABORT`, as part of `rollback()`. That closes the ordinary route into
this state. What remains:

| route | status after 35aea11 |
|---|---|
| `rollback()` after the freeze | now calls `FLINTSLOTABORT`; reaches this state only if that call **fails** (logged, not silent) |
| crash mid-cutover | never reached it — the destination's `Importing` is durable, so a crash preserves it and the reconcile takes the **resume** branch |
| data-ship-only `FLINTMIGRATEIN` (no self-addr) | **unchanged** — it never writes `Importing` at all, so a source frozen against such a destination is indistinguishable from a half-done flip |

The third row is the one to keep in view: it is how
`slot_cutover_recovery_drill.sh` constructs its half-done-flip case, so the
state is not hypothetical and is reachable by an operator sequence with no
failure involved.

**This does not close the bug.** The defect is that the reconcile decides an
unrecoverable action — the source purges every row — from an inference, and the
inference is still there. A narrower entrance to a trap is still a trap, and
the entrance that remains is the one a drill walks through deliberately.

## Fix

1. **Observe, do not infer.** Before completing a flip, ask the destination
   whether it actually owns and holds the slot. The reconcile already dials
   both nodes; a presence check is one more round trip on a path that runs
   every 2 s only when something is in flight.
2. **Or make the states distinguishable.** If `rollback()` left an explicit
   `Aborted` marker instead of an absence, "aborted" and "flip half-done"
   would be different bytes and the inference would become a read. This is the
   more faithful fix to ADR-0004 and it also makes the state visible to
   `FLINTMIGRATIONS`.
3. **Never disown on a bare inference.** `flintslotmoved` purges every row of
   the slot. That is the correct behaviour for a real handoff and unrecoverable
   for a mistaken one, so the bar for reaching it should be an observation.
4. **The drill's half-done-flip branch must assert the destination serves the
   data**, not merely that the source redirects. As written it would pass
   unchanged against both measurements above.

Until one of these lands, **do not wire `--recover-nodes` into
`controller_args`.** BUG-0024's fix list recommended making the freeze
rollback reach the source; doing that by enabling this reconcile would turn a
stranded slot into a destroyed one.

## Related

- BUG-0024 — the cutover timeout that reaches this state; its item 4 is
  corrected there
- ADR-0004 — the observability premise this function no longer satisfies

## Fixed 2026-08-19 — the destination records WHY it stopped

The inference was unfixable while an abort and a completed flip wrote the same
bytes. They no longer do. `MigrationPhase::Aborted` (4) is a destination saying
"I started an import and gave up", which is a different fact from having no
record — and it is the fact the reconcile needed.

**Where it is written.** `rollback()` sets `Aborted` *before* it does anything
else, so dying anywhere below leaves a recoverable fact rather than a lie. If it
then unfreezes the source successfully, it clears the marker — the source is
released and nothing is owed. If the unfreeze **fails**, the marker stays: it is
now the only durable evidence that a source is frozen on this destination's
account. `FLINTSLOTABORT` on a destination mid-import records the abort too,
instead of deleting the record and dropping the node back to its base claim
where it would serve a partial copy as though it were whole.

**Where it is read.** `FLINTMIGRATIONS` reports it — via a new
`needs_recovery()` rather than `is_inflight()`, because nothing is in flight for
an aborted import and it is still precisely the row the controller must act on.
`recover_migrations` checks for it before completing a flip, and unfreezes the
source instead, clearing both markers. A stale marker whose source is no longer
frozen is swept on a later pass so it cannot redirect a destination's clients
forever.

**Where it changes serving.** `slot_ownership` and `check_slot_gate` treat
Aborted like Importing: redirect to `peer`, which for an abort is the source
that never stopped owning the slot.

### Verified — a differential, with the negative control first

Constructed deterministically. Three attempts to hit the window by killing the
source mid-cutover were all VOID — the freeze-to-flip gap is one loopback round
trip and the cutover completed first every time, twice leaving the source
`Moved` and once leaving 75863 orphan rows from an interrupted purge. The
recovery drill makes the same call for the same reason.

Same source state, same controller, one bit of difference:

| | ARM A — no marker | ARM B — marker |
|---|---|---|
| controller says | `completing flip of slot 13624` | `dest ABORTED slot 13624 — unfreezing instead of completing the flip` |
| source DBSIZE | 20001 -> **0** | 20001 -> **20001** |
| `{mover}:late` (acked before the freeze) | gone — `MOVED` | **`ACKED`** |
| source write afterwards | `MOVED` | `OK` — unfrozen |

The marker also survives a destination restart, which it must: it is read after
a crash, which is when it matters.

### What ARM A means, and what is left

ARM A is a **hand-built inconsistent state**, not a live path: a source frozen
against a destination that never imported anything. After this fix every
mechanically reachable route either records the abort or leaves a destination
that legitimately holds the data —

- `rollback()` after a freeze -> writes `Aborted`
- `FLINTSLOTABORT` on an importing destination -> writes `Aborted`
- crash mid-cutover -> the destination's `Importing` is durable, so recovery
  takes the **resume** branch and never reaches this one
- data-ship-only `FLINTMIGRATEIN` then a manual freeze -> no record, but the
  destination **does** hold the slot, so completing the flip is correct (this
  is the recovery drill's half-done-flip case)

What remains is that the reconcile still completes a flip on the *absence* of a
marker rather than on positive evidence that the destination holds the slot. The
absence is now only produced by states where completing is right, so the trap is
closed — but the stronger form, where the destructive action requires the
destination to affirm it owns the data, is still the better design and is worth
doing if this path ever grows another writer. Filed as the follow-up rather than
built here, because it needs a read-only ownership query that does not exist.

**`--recover-nodes` is still not wired into `controller_args`,** and the
`gates.sh` guard still enforces that. Enabling recovery is now safe from this
defect, but that is a separate decision with its own testing, and the guard
should be removed by whoever makes it deliberately.
