# BUG-0155: `ppid 1` is the normal state of a healthy seat, so the orphan discriminator discriminates nothing (OPEN)

Status: **OPEN**, found 2026-09-15 · Severity: **medium** — one half is a
message that asserts something false to an operator; the other half is a
decision about whether another project's live fleet is allowed to share the box,
and it is the half BUG-0063 was written to get right.

## The premise, in `tools/lib/fleet.sh`

> "A sibling project's fleet refuses on sight" is the contract … But the
> contract rests on a presumption: that a named binary is a fleet SOMEONE IS
> RUNNING. **An orphan is the case where that presumption is false** … So
> orphanhood, NOT idleness, is the discriminator.

The reasoning is right. The test for it is `ppid != 1`, and that is where it
comes apart: **`ppid 1` is what a healthy daemon looks like.**

## Two independent reasons the premise is false

**Ours.** `local_spawn_env` starts a seat and never waits on it, and says so:

> Deliberately never waited on: these are the fleet's long-lived daemons, and
> flintctl exits while they keep serving.

`flintctl` exits within milliseconds of the spawn, so the kernel reparents every
seat to init. **Every seat a `bootstrap`, a `start` or a `host-spawn` brings up
is at `ppid 1` while perfectly healthy**, from the moment it starts.

**Theirs.** `flint-kv-ops/packaging/aws/ami/flint-kv.service` is `Type=simple`
with `ExecStart=/opt/flint-kv/bin/flint-kv-server`. Under systemd the service's
parent IS pid 1, by definition. So a real flint-kv fleet in the field — the
thing `_fleet_sibling_named_live` exists to refuse on sight — is at `ppid 1`
always, not occasionally.

## What it costs, in the two places it is read

**A decision.** `_fleet_sibling_named_live() { _fleet_sibling_named | awk '$2 != 1' }`
selects the siblings that refuse on sight; one at `ppid 1` falls through to the
activity check its build artifacts get, *"which proceeds only if it is genuinely
not contending"*. A live but QUIET flint-kv fleet therefore proceeds — which is
exactly the outcome the sibling contract forbids, in the words of the comment
two lines above the filter: *"a sleeping fake fleet is still a fleet, which is
what fleet_guard_drill asserts and what measuring contention for named binaries
would have destroyed."* The filter destroys it by another road.

**A statement to an operator.** The refusal prints *"ALL n ARE ORPHANS (ppid 1):
nobody is driving them, so [they] will never clear on their own"*. On
2026-09-15 it said that about six of thirteen seats, while in the same listing
three seats of the *same fleet* had a live parent — the split was not who is
driving the fleet, it was which process happened to spawn each seat. Advice to
stop waiting, given about seats whose drill is alive and about to clean them up.

## The drill's fixtures differ in exactly the property under test

`fleet_guard_drill` has both arms, and only the artificial one is parented:

- **case H**, the orphan: `orphan_as() { bash -c "( exec -a $1 sleep 60 & )"; }`
  — an intermediate shell that exits, leaving `ppid 1`. It even asserts the
  parent is 1 before proceeding, so the fixture is honest about what it is.
- **case I**, *"a LIVE sibling fleet must still refuse on sight"*: `spawn_as
  flint-kv-server`, backgrounded by the drill, so its parent is the drill.

Case I is a real positive control for the code as written. It is not a control
for the *situation* it names, because a live flint-kv fleet on a Linux box does
not look like that — it looks like case H. **The contract is enforced against a
shape a real fleet may never have**, which is the fixture end of the
checks-that-cannot-fail family: the assertion is fine, the world it asserts
against was built to match the implementation.

## What I am NOT claiming

- **Not that a flint-kv fleet has ever been let through.** Nothing measured
  that; what is measured is the service type, the spawn model and the two
  fixtures. On a *dev* box a flint-kv drill starts its own seats and those may
  well be parented — case H's branch may simply never have been reached by a
  real fleet either way.
- **Not that the fix is to delete the ppid test.** BUG-0063 is real: the flint-kv
  suite left seats behind twice in one hour on 2026-08-27 and blocked every
  drill on the box until a human killed them. Something has to tell a corpse
  from a fleet. `ppid` was a cheap answer and it is the wrong one; what replaces
  it — a liveness probe, a lock file with a recorded start time as
  `_fleet_live_peer_scopes` already uses for peers, or asking the owning suite —
  is a design call that spans two products.

## Found by

Withdrawing the same inference from my own [BUG-0153](0153-fleet-guards-settle-loop-re-samples-without-the-peer-filter.md)
write-up, where I had read `ppid 1` as "the drill that started these has exited"
and built a paragraph on it. It had not exited; its controller, control plane
and proxy were in the same listing at a live ppid. The inference was wrong in my
write-up for the same reason it is wrong in the code.
