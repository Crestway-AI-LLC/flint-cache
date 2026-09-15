# BUG-0142 — the capacity loop's last step is unarmed on every fleet flintctl builds

**Status:** **FIXED 2026-09-14** — Jeff's call: arm it, **default off**, add the
inventory key. Found while building `expand_fill_drill.sh`, the drill for the
seam between `expand` and rebalancing.

**A CORRECTION TO THIS FILE, found while fixing it.** The text below says the
controller "plans rebalances and only logs them". It does **not plan at all**:
the loop is `if cfg.rebalance_deadband > 0.0`, and `flintctl` passed no
deadband either, so no plan was ever produced for `--rebalance-execute` to
withhold. The bug was one knob larger than it was written up as.

**The fix is two inventory keys**, `rebalance-deadband` and
`rebalance-execute`, both absent by default so every inventory in the field
produces exactly the argv it produced before. `rebalance-execute on` without a
positive deadband is **refused**: it arms a loop that never produces a move,
and the failure mode is silence — the fleet looks armed, the operator sees
nothing happen, and no line anywhere says why. A deadband alone plans and logs,
which is the honest first step for an operator who wants to see what the
rebalancer would do. `capacity-model.md` now documents both and says they are
off until asked.

## What is promised

`docs/capacity-model.md` states the loop to operators:

> **70% fill ⇒ expand ⇒ controller drains the pressured pair**

and names the mechanism one paragraph down — the controller plans moves every
cycle and, *"with `--rebalance-execute` — ships them a few slots per cycle"*.
The ops roadmap's elasticity lane says the same thing in the same words: the
expansion pair joins unranged and *"rebalancing drains the pressured pair,
clearing the standing condition."*

## What happens

`--rebalance-execute` is read as `std::env::args().any(|a| a == "--rebalance-execute")`
(`flint-controller/src/main.rs:1505`) — argv only, so no environment variable
arms it. And `flint-ctl`'s `controller_args` (`main.rs:3756`) never passes it.
There is no inventory key for it either.

Searched repo-wide, the flag appears in exactly four places:

| site | what it is |
|---|---|
| `tools/rebalance_execute_drill.sh` | a drill, arms it by hand |
| `tools/tenant_rebalance_drill.sh` | a drill, arms it by hand |
| `flint-controller/src/main.rs` | the flag's own definition and doc |
| `docs/capacity-model.md` | the sentence promising it to operators |

**So every controller `flintctl` starts plans rebalances and only logs them.**
That is every fleet the documented path produces — `flintctl bootstrap`, the
AMI's first-boot, and `packaging/aws/chaos-cluster`.

## Why it matters, precisely

An operator who follows the recommendation gets the expansion and nothing
after it: a new pair holding nothing, a pressured pair still pressured, and a
`CapacityPressure` condition that **never clears** because the thing that would
clear it was never armed. The agent will keep recommending `ExpandCluster`, and
each expansion adds an idle pair.

The two halves either side of the gap are both proven — `rebalance_execute_drill`
tests the rebalancer with the flag on, and `migrate_slots_drill` tests `expand`
followed by an EXPLICIT migrate. Neither runs the documented automatic path, so
nothing was red.

**This compounds with a separately recorded absence.** The ops agent's own
`--traffic-rebalance` was measured absent on both production boxes on
2026-09-11 (1,022 chars of running agent args, no rebalance flag of any kind,
with a positive control proving the grep could hit). So neither rebalancer is
armed anywhere in production, by two independent routes.

## Not fixed here, and why

Arming it is a **decision**, not a repair. A controller that executes
rebalances by default moves tenant data without being asked, on a policy
(`--balance-policy size`) chosen for it. That is exactly the class of change
this repo does not make in passing. What is needed:

1. an inventory key (`rebalance-execute`, with the deadband and
   `max-slots-per-cycle` beside it), so a fleet can be armed declaratively;
2. a decision on the **default** — off preserves today's behaviour and leaves
   the documentation wrong; on makes the documentation true and moves data on
   fleets that never asked;
3. whichever is chosen, `capacity-model.md` and the roadmap's elasticity lane
   have to agree with it. Today they describe a fleet nobody runs.

`tools/expand_fill_drill.sh` lands with this file and covers the mechanism —
a pair joining EMPTY, filled hands-free, keys conserved. It arms the controller
itself, so it passes today and would not have caught the wiring gap; the gap is
in what `flintctl` passes, and that belongs to whichever fix item 1 above takes.
