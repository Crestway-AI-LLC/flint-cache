# BUG-0155: `ppid 1` is the normal state of a healthy seat, so the orphan discriminator discriminates nothing (FIXED 2026-09-16)

Status: **FIXED 2026-09-16**, found 2026-09-15 · Severity: **medium**. Both
halves, by two sessions: the message by the cache session, the decision here
after Jeff made the call it was waiting on — see "The decision half, closed" at
the end.

The two halves were always separable, and they turned out to want opposite
treatments. One is a message that asserts something false to an operator, and
it could be replaced with a record we already hold. The other is a decision
about whether another project's live fleet is allowed to share the box — the
half BUG-0063 was written to get right — and every candidate fix for it spans
two products.

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

## Measured 2026-09-16

The write-up below originally said the pass-through was inferred and not
measured. It is measured now, and it holds.

**The filter is blind to a real fleet's shape.** A named sibling started the
way a systemd `Type=simple` unit and a `flintctl`-spawned seat are both
started — parent exits, kernel reparents to init — was put on the box and
counted through each detector:

| read by | sees it |
|---|---|
| `_fleet_sibling_named` | 1 |
| `_fleet_sibling_named_live` | **0** |
| `_fleet_sibling` | 1 |

So it is a named sibling fleet by every test except the one that decides
whether to refuse on sight, and it falls through to the activity check exactly
as predicted.

**The false statement reproduces through the product.** A new arm J in
`fleet_guard_drill` puts an out-of-scope seat at `ppid 1` under a scope whose
`fleet_init` lock is *held by a process that is still running* — nothing about
it is abandoned — and runs the guard. Against the library as it stood:

```
    pid 36108   ppid 1       flint-server --data-dir .../flint-guardj0155/d
  ALL 1 ARE ORPHANS (ppid 1): nobody is driving them, so
  waiting will not clear this. Someone has to remove them.
```

That is the defect in one line: the seat's owner was alive, recorded, and
readable from disk, and the guard told the operator to go kill it.

## The message half: fixed by reading the record instead of guessing

`ppid` was standing in for a question the suite already answers elsewhere.
`_fleet_live_peer_scopes` proves a drill is live from its `fleet_init` lock —
pid **and** process start time, so a reused pid cannot launder a dead run into
a live one — and `_fleet_drop_peer_lines` already matches seats to a scope by
path boundary and by declared ports. The refusal now uses both:

```
  1 of 2 are held by a LIVE drill in this suite: its
  fleet_init lock is taken and the owner is still running, so they
  WILL clear when it finishes. Owner scope(s): .../flint-d0155driven
  1 of 2 have NO live fleet_init lock, so nothing in this
  suite is driving them. That is as far as this box can say -- they
  may be a leak, or a fleet started outside the drills. ...
```

Three properties of that replacement are deliberate:

- **It is read whatever `FLINT_DRILL_PARALLEL` says.** The flag governs the
  *decision* to tolerate a peer; this is the *diagnosis*. Withholding a fact we
  hold would not have made the refusal safer, only unattributable.
- **It proves one direction and claims only that one.** A live lock proves an
  owner. Its absence proves nothing — a seat from `flintctl start` outside any
  drill never had a lock — so those are reported as UNATTRIBUTED, not as
  abandoned. The old message's certainty was the bug; replacing it with a
  different certainty would have been the same bug.
- **It names only the owners that matched.** A live lock whose drill owns
  nothing in the list is not why we are refusing, and printing it would send
  someone to wait on the wrong run.

The five-hour incident that motivated the ppid test in the first place is
better served by this, not worse: those `/tmp/flint-m3-67*` processes had no
live lock over their scope, which is a record rather than an inference.

On the sibling side there is no lock to read — `fleet_init`'s locks are this
suite's and another project does not take them — so the refusal stops answering
the question. It says the two places that do know instead. The branch it
replaces also asserted the seats were "burning CPU" in a case where the refusal
had come from the foreign list and nothing had been measured.

## What I am NOT claiming

- **Not that a flint-kv fleet has ever been let through** on this box.
  Measured above is that it *would* be — the filter cannot see the shape a real
  one has. What has actually happened on a given day is not in evidence, and on
  a *dev* box a flint-kv drill starts its own seats and those may well be
  parented, so case H's branch may never have been reached by a real fleet
  either way.
- **Not that the fix is to delete the ppid test.** BUG-0063 is real: the flint-kv
  suite left seats behind twice in one hour on 2026-08-27 and blocked every
  drill on the box until a human killed them. Something has to tell a corpse
  from a fleet. `ppid` was a cheap answer and it is the wrong one; what replaces
  it — a liveness probe, a lock file with a recorded start time as
  `_fleet_live_peer_scopes` already uses for peers, or asking the owning suite —
  is a design call that spans two products.

## Where this stands (as of the message fix — superseded below)

**Jeff's call, 2026-09-16, was "fix 0155".** What follows was the state before
that, kept because its reasoning about the two asymmetric failures is what the
fix had to respect. The decision half is closed in the section after it.

**Fixed:** both refusal messages. Neither now states an answer it does not
have. `_fleet_foreign`'s header says the ppid is raw data and not a verdict,
and the comment above `_fleet_sibling_named_live` says what the filter does
rather than what it was hoped to do. Arm J of `fleet_guard_drill` is the
regression: it was red against the library as it stood and green after.

**Open, and it is a decision rather than an oversight:** `_fleet_sibling_named_live`
still filters on `$2 != 1`, so a live but quiet sibling fleet still falls
through. The behaviour is **unchanged on purpose**. The two failures are not
symmetric — permissive costs contention and a flaky-looking drill, strict costs
a wedged box and a hand-kill — and the candidates all reach into flint-kv:

1. **Refuse on sight again, ppid removed.** Restores the contract exactly.
   Costs BUG-0063 back: a leaked sibling seat blocks every drill until someone
   removes it. `FLINT_DRILL_FORCE=1` is the escape hatch and it is a blunt one.
2. **A cross-project run lock.** flint-kv's suite takes a lock in a shared
   location with pid and start time, and this guard reads it exactly as it now
   reads its own. Sound in the direction that matters and it is the same
   mechanism already proven here — but it is a change to flint-kv's suite, and
   it does nothing for a fleet started by systemd rather than by a drill.
3. **Ask the seat.** A liveness probe tells a serving process from a serving
   process; a leaked seat answers `PING` too. It does not discriminate and is
   listed only so it stops being re-proposed.

My recommendation is (1) plus (2) — refuse on sight, and give flint-kv a lock
so the common case stops needing a human — but the cost lands on another
session's box and on another product's suite, so it is Jeff's call.

**Also duplicated:** the ops repo carries its own implementation of the same
message, with the same false claim stated more strongly (*"Kill them and
re-run; there is no peer to be polite to"*). Tracked separately; a fix that
landed in one of two implementations is BUG-0150's shape and this is not going
to repeat it.

## Found by

Withdrawing the same inference from my own [BUG-0153](0153-fleet-guards-settle-loop-re-samples-without-the-peer-filter.md)
write-up, where I had read `ppid 1` as "the drill that started these has exited"
and built a paragraph on it. It had not exited; its controller, control plane
and proxy were in the same listing at a live ppid. The inference was wrong in my
write-up for the same reason it is wrong in the code.

## The decision half, closed 2026-09-16 — with two claims withdrawn

**"It spans two products" was wrong, and that is what kept it open.** The three
candidates listed above were *a liveness probe, a lock with a recorded start
time as `_fleet_live_peer_scopes` already uses for peers, or asking the owning
suite*. The second is **already on disk in the other product**: flint-kv's
`drill_lib.sh` takes `$TMPDIR/flint-kv-drill.lock`, a directory holding the
runner's `pid`, and tests it with `kill -0`. Its own comment calls it *"where a
live one holds its claim"*. The answer was being published all along; nothing
over there has to change. Found by reading the sibling's harness rather than
reasoning from its unit file.

**And the decision half was LATENT, not live.** This file argued a real flint-kv
fleet under systemd sits at `ppid 1` and would be let through. The service type
is right; the inference was too fast. `flint-kv.service` runs on **KV fleet
nodes**, and nothing that calls `fleet_guard` runs there — so a supervised
sibling and these drills cannot currently share a box, and the ppid test has not
been wrong in production. The gap that IS reachable is narrower: a sibling run
**still in progress** whose seat has been reparented.

### What the code does

`_fleet_sibling_named_live` is now *parented **OR** its suite holds its lock*.
Strictly more seats than `ppid != 1` alone, and it loses nothing BUG-0063
depends on: with no run in progress an orphan still falls through to the
activity check, so a corpse is still stepped over without a human. That
preserves the asymmetry the section above insists on — permissive costs
contention, strict costs a wedged box and a hand-kill — because the only seats
it newly refuses are ones a running suite has claimed.

`_fleet_sibling_suite_running` reads the **holder**, not the file.
`drill_lib.sh` reclaims its own lock when the owner is gone for the reason it
records — a lock whose owner is gone *"is not a lock, it is litter"*, and it
blocked all seventeen of their drills until a human removed it. Reading
existence rather than liveness here would import that failure.

The path is `FLINT_SIBLING_LOCK`, defaulted to the real one so nothing changes
in production and overridable so the drill uses a fixture. Creating or removing
the real path would corrupt a genuine flint-kv run sharing the box — a drill
doing the exact damage this guard exists to prevent.

### Drill: case H2

*"a sibling suite that says it is MID-RUN makes its orphan driven again"* —
spawn an orphan, confirm it in `ps`, then ask `_fleet_sibling_named_live`
directly: with no lock it must say no, with a lock naming a live pid it must say
yes, and with a lock naming a reaped pid it must say no again. That third check
is not decoration: without it the lock would be a blanket amnesty in the other
direction and litter from a crashed run would block the box forever.

Asserted on the **predicate** rather than on a `fleet_guard` verdict, which
anything else on the box can move — [BUG-0157](0157-case-g-judges-the-guard-against-an-empty-box.md)'s
lesson applied while writing instead of after a red gate.

**Mutation-verified**: with `_fleet_sibling_named_live` reverted to ppid alone,
H2 fails on the live-holder arm with exactly the diagnostic it carries, and the
cache session's arm J still passes on the merged tree.

### Still true afterwards

A supervised sibling fleet with **no** drill lock still reads as a corpse. Not
reachable today; if it becomes so, the answer is service-cgroup membership
(`/proc/PID/cgroup` names the unit), which is Linux-only and wants the care this
file gives every check that exists on one of its two platforms.
