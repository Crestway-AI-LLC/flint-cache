# BUG-0046: managed redundancy repair wipes the replica it is waiting for

Status: OPEN, found 2026-08-24 · Severity: HIGH — on a managed pair
(`--manage-slots`), a replica whose full sync takes longer than ~20 seconds can
never come up. The controller wipes and restarts the transfer every ~20 s
forever, making no progress. At fleet dataset sizes this is not a risk, it is
the guaranteed outcome.

## The loop

Four facts on main, each individually reasonable:

1. **A fresh replica is TCP-dark for its whole full sync.**
   `flint-server/src/main.rs:1105` blocks in `replica::full_sync_download` in a
   retry loop; the listener does not bind until `main.rs:1870`. Until the
   transfer completes the node is, at the TCP layer, indistinguishable from a
   dead one.

2. **The controller repairs redundancy by wiping.**
   `flint-controller/src/main.rs:809` — a non-master slot unreachable for
   `confirm` ticks is "respawned as a FRESH replica", and `spawn_slot` opens
   with `std::fs::remove_dir_all(&slot.dir)`. The partial transfer is
   discarded, not resumed.

3. **The cooldown that is supposed to cover the sync is hardcoded to 20 s.**
   `main.rs:843`: `self.slot_cooldown[i] = Instant::now() + Duration::from_secs(20)`,
   commented "a cooldown after a respawn avoids thrash while it syncs."

4. **Re-confirmation after the cooldown takes 0.3 s.** Defaults are
   `--poll-ms 100` and `--confirm 3`.

So the cycle is **~20.3 seconds**: wipe, spawn, sync starts, dark, 20 s
cooldown, three poll ticks, wipe again. Any dataset that cannot full-sync in
20 seconds never finishes one. The transfer restarts from zero every cycle, so
there is no accumulation and no convergence — more data makes it worse, never
better.

The cost model's anchor node carries ~96 GB per node. A full sync there is
minutes to tens of minutes. This defect is therefore reachable by every
managed pair at production size and unreachable in a drill, which is why the
suite is green.

## Scope, stated so it is not overclaimed

  - **Managed controllers only** (`self.managed`, i.e. `--manage-slots`). An
    unmanaged controller does not run redundancy repair and cannot enter this
    loop.
  - `socket_alive` does not rescue it. That distinction ("alive but slow")
    exists on the no-master path, not this one, and a full-syncing node has no
    listener at all — it is not slow, it is absent.
  - The related `#139` symptom ("four restarts in four minutes") predates
    these defaults; the shape is the same, the period is not.

## What main already does, and why it is not enough

flintctl knows about the dark window and compensates — `node_ready_budget`
(`flint-ctl/src/main.rs:678`) waits up to `node-ready-s` before concluding a
seat is absent, and its own comment concedes the default: *"the 15 s default
fits drills, not fleets carrying tens of GB per pair."* So the orchestrator
waits and the controller does not. One of the two components that acts on a
dark node was taught to wait; the other still wipes.

## The fix is a readiness signal, not a bigger constant

Raising 20 s to 300 s moves the threshold and keeps the defect: the budget is
still a guess about transfer time, which depends on dataset size and link
speed. The node has to be able to SAY it is loading.

flint `775911c` — "server: a node that cannot serve yet must be visible, not
dark" — is stranded on `origin/phase1-drills` (see BUG-0044) and adds exactly
that: bind the listener before the sync, report `loading:1` in FLINTINFO, and
`wait_for_ready(port, budget)` so callers distinguish "not up" from "not up
YET". Redundancy repair can then skip a slot that is loading rather than
wiping it.

That commit was marked NOT ESTABLISHED when BUG-0044 audited the branch,
because main opens its listener before TAILING and the full-sync path had not
been checked. It has now been checked. This is its consequence.

## Same family as the rest of 2026-08-24

A fixed 20 s cooldown, a fixed 2000 ms disk-guard sample (BUG-0044), fixed
300/150/500 ms roll sleeps (the canary flake), a fixed 15 s node-ready budget.
Every one is a duration chosen on a machine where the thing it waits for is
instant, and every one fails on the machine where it is not.
