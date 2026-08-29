# BUG-0069 — eviction is per-tenant policy with only a per-seat switch (FIXED 2026-08-28)

**Found** 2026-08-28, answering whether eviction could be rolled out to the
playground. The evictor is finished; the way to turn it on is not.

## The gap

A namespace is declared evictable in exactly two places, both per seat:

- `--evictable-ns <list>` on the node at start
- `FLINTCONFIG evictable-ns` at runtime

`flintctl` has **no** reference to it, nothing in the fleet repo's `packaging/`
sets it, and there is no inventory key. (A comment in
`flint-chaos/src/cluster.rs` lists "an inventory line" among the ways a
namespace might be declared — that is the refusal check being deliberately
agnostic about how the flag arrives, not a path that exists. Worth saying,
because grepping for it reads as though one does.)

So the policy is **per tenant** — the flag's value is a namespace list — while
the switch is **per seat**. Nothing composes those.

## Why that is worse than merely inconvenient

Both members of a pair must be given the same list, and the code already says
what happens when they are not (`flint-server/src/main.rs:923`):

> Per-seat config lets the two members of a pair silently disagree, and a pair
> where one side reclaims while the other fills to `-QUOTA` is divergent POLICY
> rather than divergent decisions — strictly worse, and nothing else would
> surface it.

`EVICTABLE_AGREE` exists precisely because this is reachable, and FLINTINFO
reports `evictable_ns_agree` as 1 / 0 / -1. But it is a **detector, not a
guard**: it tells an operator afterwards, and there is no deploy step for it to
refuse at, because there is no deploy step at all. Turning eviction on across a
fleet today is per-seat `FLINTCONFIG` calls with correctness resting on
remembering to make all of them.

## The work

1. `flintctl` passes the namespace list through to the seats it spawns.
2. An inventory key, so the list is fleet state rather than an argument someone
   typed twice.
3. The fleet repo's render-inventory carries it.
4. A deploy-time agreement check — the one thing `evictable_ns_agree` cannot do
   from where it sits.

## The open question, which is why this is not purely mechanical

**What should a mismatch do at deploy: refuse, or warn?** Refusing is
consistent with how this tree treats a configuration that could put a wrong
number in a durability claim (`refuse_if_evictable`, the disk guard's host
asserts). But eviction is opt-in and a half-applied rollout is a state an
operator may be moving THROUGH deliberately — a refusal mid-roll could strand a
fleet with one seat converted. Worth deciding before building, not after.

## Two things it is not

- **Not ADR-0023.** Slot-aligned bulk eviction is PROPOSED and unbuilt; this is
  about turning on the per-key evictor that already shipped (2026-08-26).
- **Not a reason to delay the playground on its own** — but note chaos
  **refuses** to run against an evictable namespace by design, since against
  one it can only report phantom loss or wave real loss through as licensed. A
  playground that doubles as a chaos or verification target must not mark the
  namespaces it tests.


## FIXED 2026-08-28

All four items landed, plus the guard the bug argued for.

1. `evictable-ns` is an inventory key, threaded into every spawned seat.
2. `reload` pushes it to a running fleet alongside the other hot node knobs --
   turning a namespace evictable is a decision made on a live fleet, and the
   alternative was a per-seat FLINTCONFIG to every member with correctness
   resting on remembering them all.
3. `verify` REFUSES a pair whose members disagree, naming the pair and both
   seats, and `evictable_ns` joins `PAIR_KNOBS` so drift AFTER deploy is
   reported too.

## The open question, answered

**Refuse, with an explicit override.** Decided by Jeff 2026-08-28.

The refusal is the default because divergent eviction policy is the one shape
nothing else surfaces at deploy time. Two things keep it from causing the
failure it prevents:

- A member inside the roll window is HELD BACK rather than refused, exactly as
  `PAIR_KNOBS` drift is. `roll` converges the two sides one at a time and
  passes through a legitimate disagreement on the way; refusing there would
  fail the very roll that fixes it and strand a half-converted fleet. An
  operation's own `verify_after` tolerates it for the same reason.
- `--allow-evictable-mismatch` carries a deliberate half-applied rollout, and
  SAYS so in the output rather than passing silently -- an operator who forgets
  the flag is set must still be able to see that a mismatch is being carried.

The roll-grace half turned out to matter more than the flag: it handles the
"an operator may be moving through this state deliberately" case structurally,
where the flag handles it by permission.

## Held by

`tools/evictable_agree_drill.sh` (gate step `evictable_agree`), with a positive
control that the inventory key actually reached BOTH seats -- without it every
assertion compares two defaults and keeps passing against a fleet where the key
does nothing. Mutation-checked both ways, failing DIFFERENT arms: disarming the
refusal fails the REFUSED arm, removing the roll-grace hold fails the HELD BACK
arm.

Gate: 133 steps, 0 failures. The step count rising from 132 is itself the
control that the new drill was registered rather than silently absent.
