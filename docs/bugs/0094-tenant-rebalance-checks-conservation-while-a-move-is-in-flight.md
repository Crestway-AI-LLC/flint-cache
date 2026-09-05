# BUG-0094 — `tenant_rebalance` asserts conservation while a slot move is still in flight

**Status:** FIXED 2026-09-04 · **Severity:** medium — an intermittent gate
failure that accuses the product of duplicating a tenant's keys

**Symptom.** Under the gate, `tenant_rebalance_drill.sh` intermittently fails

    FAIL: alpha lost/gained keys: 15000

against a seeded 12000. It is the only one of that batch of gate failures
that reads as a *correctness* claim rather than a timing one, which is what
made it worth chasing first: 15000 says the group invented 3000 rows.

## It is the ruler, not the product

`rebalance_execute_drill.sh` — this drill's direct sibling, same controller,
same move mechanism — **already found this and already says so**, in a comment
written when it hit the identical failure at three pairs:

> A slot move copies to the destination and only then drops the source, so
> mid-flight the same keys are counted on BOTH pairs and every DBSIZE is
> inflated. […] It is a race in the ruler, not a defect in the product: the
> drill was asking "are the sizes balanced yet" when it meant "has the
> migration finished".

The fix never reached the sibling. This is the pattern `tools/gates.sh`
already names one level down — "The project had already found this twice, in
controlplane_drill.sh and lease_drill.sh, and fixed it in those two files" —
a fix applied where the bug was found rather than where the bug lives.

## Why this drill's numbers are so tidy

`3000` is not a coincidence and neither is 15000. The drill seeds four tags,
alpha 3000 rows each, beta 1000 each, all on g0 — 16000 rows. The controller
runs `--max-slots-per-cycle 3` and moves three alpha units. Its settle
predicate is

    MEAN>0 && MAX*100 <= MEAN*125 && grep -q "rebalance EXECUTE"

Both halves are satisfiable before the plan finishes.

* `rebalance EXECUTE` is printed by `execute_move` **before the first unit
  moves**, and the units are then migrated **serially**. It announces a plan;
  it says nothing about completion.
* Fills after **two** of the three units are g0=10000, g1=6000 — mean 8000,
  max 10000 — and `10000*100 <= 8000*125` is `1000000 <= 1000000`. It holds
  **exactly**, on the boundary.

So the loop can break with the third unit still copying. `DBSIZE` counts
residency, the proxy SUMS the masters, and the conservation assert then reads
12000 + 3000 = **15000**.

## Positive control

The window is sub-second on an idle laptop, which is why six consecutive
local runs passed and said nothing. Paced with `--migrate-rate-bytes 20000`
it is ~10 s wide and the failure is deterministic. Sampling both predicates
every 0.3 s through one rebalance:

| samples | fills | old predicate | `DBSIZE alpha` | in flight |
|---|---|---|---|---|
| 79–109 | g0=10000 g1=6000 | **satisfied** | **15000** | slot 4616 `importing` |
| 110 | g0=7000 g1=9000 | satisfied | 12000 | none |

**31 of the 32 samples that satisfied the old predicate read 15000.** The new
predicate was satisfied at exactly one sample, 110, reading 12000.

The same run also shows the two rulers disagreeing about the same rows: at
the first import, `FLINTSLOTSTATS` reported g1=0 while `DBSIZE` already
counted the destination's copy. Fills are ownership; DBSIZE is residency.

## Asking the nodes is not sufficient

The obvious signal — poll `FLINTMIGRATIONS` until no node reports a move —
is **not** sound, and the reason is worth keeping:

* the destination clears its `Importing` record **before** the flip
  (`migrate.rs`, "Step 5: flip dest-first, then source");
* the source sets `Moved` **before** purging, deliberately, so a crash
  mid-purge leaves invisible orphans rather than an unrecorded handoff;
* `Moved` is hidden from the bare `FLINTMIGRATIONS`, which filters to
  `needs_recovery()`.

So for the whole length of the source's purge — which scales with the slot,
as `migrate.rs` notes when it gives that call a 60 s budget — **both nodes
report nothing in flight while both still hold the rows.** Same shape as
BUG-0025: an absence that two different states produce.

The sound signal is the controller's per-unit line. `MIGRATEIN-OK` is logged
only after the source has replied, and the source does not reply until the
purge is done.

## The fix

Settle on *units*, not on the plan announcement:

* every unit named in a `units [...]` line has a matching `MIGRATEIN-OK`, and
* neither node reports an `importing`/`migrating` row, and
* the fills are within the deadband.

An abandoned move (`move failed`, or an `aborted` record) fails immediately
rather than being waited out for 90 s and then reported as "did not
converge" — a cause the check never established (ADR-0028 obligation 4).

Deliberately **not** the sibling's approach of folding `SUM == TOTAL` into the
settle condition. That works, but it makes the wait and the assertion the same
predicate, so the conservation check afterwards can no longer fail. Keying the
wait on migration completion instead leaves conservation independently
falsifiable, which is the only reason to assert it. (The sibling is not
actually blind — its *timeout* branch still distinguishes a persistent
conservation failure from the mid-move window, with good diagnostics.)

The drill now prints what the wait observed:

    settle: units announced=3 resolved=3; polls that saw work in flight: 0

`0` means every move finished inside one poll and the wait proved nothing *on
this host* — which is not the same as the wait being unnecessary, and is
exactly the distinction OPS-0037 exists for.

## A second instance, in the sibling's own guard

`rebalance_execute_drill.sh` guards its accusatory message with

    [ "$MOVES" -lt "$EXECS" ]

`MOVES` counts completed **units**; `EXECS` counts plan **announcements**. It
runs `--max-slots-per-cycle 2`, so one announcement can carry two units and
the guard only holds while the in-flight unit is the first of the first cycle:

| state | MOVES | EXECS | guard | result |
|---|---|---|---|---|
| unit 1 in flight | 0 | 1 | true | correct |
| unit 1 done, unit 2 in flight | 1 | 1 | **false** | "duplicated rows" |
| 2 cycles, 3 done, 1 in flight | 3 | 2 | **false** | "duplicated rows" |

Confirmed on a synthetic log: `MOVES=1 EXECS=1 ANN=2` — the old guard falls
through to the accusation the comment above it exists to prevent. Fixed to
compare against announced units.

## What is NOT claimed

That the transient inflation is a product defect. A copy-then-purge migration
has a window where two nodes hold a slot, and `DBSIZE` summing masters will
see it; the sibling's verdict — a race in the ruler — stands. What is new here
is that the window is *not observable from the nodes* for its last stretch,
so an operator asking `FLINTMIGRATIONS` "is anything moving?" during a
purge is told no while the sum is still inflated.
