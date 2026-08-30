# BUG-0077 — a seat that exits for a re-seed is never restarted on a fleet (FIXED 2026-08-30)

**Found** 2026-08-30 by the three-member fleet chaos re-run that confirmed
BUG-0076's fix. The fix worked and immediately exposed the next link.

## What happens

A replica re-pointed at a new master presents its cursor, and the master's
attach guard refuses it when that cursor sits past the promotion fence:

```
7002| FLINTFOLLOW: re-pointing replication at 172.31.75.24:7003 (epoch (0,6))
7002| FATAL: WALGAP cursor 162080 is past the promotion fence 162052 for epoch
      (0,5): that span was never on this timeline — this link can never
      resume. Marking for re-seed and exiting.
```

**The refusal is correct.** That copy had applied 28 batches beyond the branch
point — writes the surviving lineage never had — so it cannot continue and a
re-seed is the only sound remedy. The seat does what it is designed to do:
write `NEEDS_RESEED` and exit, on the reasoning (`replica::run`) that an
in-process re-seed would tear the DB handle out from under live readers, and
that *"exiting is also the honest signal — under systemd (`Restart=on-failure`)
the next start re-seeds unattended"*.

**Nothing restarts it here.** Fleet seats are started by `flintctl` through
`host-spawn` as plain processes; there is no systemd unit behind them, so the
"next start" never comes. The pair runs a member short until an operator
notices, and `verify` ends the run with:

```
FAIL pair 0 fully staffed  SINGLE-COPY: ["172.31.77.217:7002"] down — no
     failover target, one copy on one disk
```

## Why it only shows up now

Before BUG-0076 the re-point never happened, so this path was never taken: the
stranded replica retried a dead address forever instead. The same fleet run
before the fix contains ZERO of these FATAL exits, and the one after contains
them. Trading a silent spin for a loud exit is the right trade — the exit is
visible to `verify` and self-heals under a supervisor — but on an unsupervised
fleet both end the same way, with the pair a member short.

The chaos oracle passed both times (16 kills, 848,566 writes, zero corruption,
zero time-travel, zero cross-key). This is availability, not durability.

## Evidence the fix underneath it works

The same run took **5 master kills, against 1 before the fix**. That is not a
sampling difference: the harness declines a master kill unless every replica
is live and converged, and a permanently stranded member can never satisfy it.
Five accepted master kills means survivors were re-attaching and the pair was
returning to full strength between kills.

## The supervisor already exists, and this is the second time it was needed

Filed as "nothing restarts it". That was wrong in an instructive way:
`packaging/aws/supervise.sh` and `flint-supervise.timer` exist for EXACTLY
this, and their header records the first occurrence —

> The gap this closes: a seat can exit CORRECTLY and never come back. On
> 2026-08-01 the playground's replica hit a WAL gap, marked itself for
> re-seed and exited — the designed behaviour — and nothing ran the next
> start. **It sat single-copy for five days.** `flint-first-boot.service`
> covers a REBOOT; nothing covered a seat dying under a box that stays up.

So the mechanism was built, after a five-day outage, and this run reproduced
the same outage anyway. The reason is the chain that installs it:

- `flint-ami.pkr.hcl` enables `flint-nvme`, `flint-first-boot`, `prometheus`
  and `grafana-server`. **It does not enable `flint-supervise.timer`.**
- `first-boot.sh` is what installs the unit files AND enables the timer.
- `chaos-cluster/up.sh` **disables `flint-first-boot`** — correctly, or six
  hosts would each bootstrap their own single-host cluster (the trap that
  planning for the multi-host work called out by name).

Avoiding six single-host clusters therefore also removes the supervisor,
because both hang off the same unit. Nothing says so anywhere, and the fleet
that most needs supervision — the one built to have its seats killed — is the
only topology that silently has none.

And it is not only the harness. `docs/self-hosting.md` documented surviving a
REBOOT and never mentioned this case at all, so an operator following it got
the five-day failure mode with no warning.

## Fixed

**The chaos fleet** now runs `flintctl start` once, from the orchestrator,
between the kills and the post-chaos verify — which is what
`flint-supervise.timer` does every minute on a supervised box. `start` skips
seats already serving, so it is a no-op on a healthy fleet. Deliberately NOT
in the kill loop: a supervisor racing chaos would restart seats the run
intends to be dead and quietly change the fault model.

**The docs** now say it. `self-hosting.md` §2b gains the distinction its own
reboot unit does not cover — a seat exiting while the box stays up — with the
unit, the timer, the `KillMode=process` trap that cost a day on the
playground, and the advice to alert on `verify`'s SINGLE-COPY rather than
trust a restarter nothing watches.

## What is deliberately NOT changed

Enabling `flint-supervise.timer` on every AMI host. On a multi-host fleet each
seat host has no inventory — it lives on the orchestrator — so a per-host
timer would log "no inventory" every minute and supervise nothing. The unit is
right; where it runs is the part that does not generalise from one box to a
fleet.

## Fix options considered

1. **The controller restarts a seat that exited.** It already supervises the
   pair and already knows the member is unreachable while its socket is dead.
   It would need the seat's spawn arguments, which today only `flintctl` has.
2. **`flintctl` gains a supervise loop** for disposable/fleet seats. Matches
   where spawning already lives, but adds a daemon to a tool that is currently
   a one-shot command.
3. **Fleet seats run under systemd**, like the AMI's single-host path already
   assumes. Closest to the design's stated intent — the exit path was WRITTEN
   for `Restart=on-failure` — and it makes the chaos fleet resemble a real
   deployment more, not less.
4. **In-process re-seed**, so the seat never exits. Rejected once already for
   a good reason (the DB handle is shared with the serving path); nothing has
   changed that.

Option 3 was the honest one and is what shipped, in the narrow form above: the
behaviour is not a bug in the seat, it is a supervisor that exists, is enabled
by a unit this fleet must disable for an unrelated reason, and was never
written down for anyone self-hosting.

## Not covered

- Whether the playground's timer is actually running TODAY. The unit is
  enabled by a first-boot that ran months ago; nothing re-asserts it, and
  `smoke-ami.sh` checks it on a fresh image rather than on the live box.
- How often a re-pointed replica is genuinely past the fence. Once in this
  run, out of five promotions — but the run is far too small to call that a
  rate, and it depends on how far a replica lags at the moment of promotion.
