# BUG-0060: thirty drills kill the supervised seat before its supervisor

Status: OPEN, found 2026-08-27 · Severity: MEDIUM as a gate matter — it is a
latent race in 30 drills that currently loses only because a teardown bug
happens to make it too fast to lose.

## How it surfaced

BUG-0051's fix makes `fleet_kill` wait for the pids it killed to actually be
gone, which the surrounding comment has always said it does. The gate then
reported two drills leaking seats:

    GATES FAILED: controller_multipair(leaked) snapshot_restore(leaked)

Ten gate runs earlier the same day were clean; the leaks appear only on the run
carrying that change. Both drills passed their assertions and took 3.1s and
1.1s longer than the run before.

## The mechanism

Both cleanups kill in this order:

    cleanup() { fleet_kill server; fleet_kill controller; ... }

**The controller's job is to notice a dead node and respawn it.** Killing the
nodes while their supervisor is still running is a race with the supervisor,
and the drills have been winning it for the wrong reason: `fleet_kill` returned
within milliseconds of `kill -9`, so the controller was dead before it could
react. Make `fleet_kill` wait for the deaths — the correct behaviour — and the
window opens to the length of the wait. What leaks is the node the controller
put back.

So this is not a regression BUG-0051 introduced. It is a defect BUG-0051 makes
reachable, and the fix for one cannot land without the other.

## It is 30 drills, not 2

Every drill whose cleanup kills `server` before `controller`:

    admin_gated_proxy  anti_affinity  attached_chaos  backup_seat  build_stamp
    cert_reload_fleet  cert_rotate  client_compat  cold_start_roles
    config_drift  config_file  control_tls  controller_ha  controller_multipair
    decommission  edge_ca_trust  edge_roll  json  loaded_promote  m3_exit
    migrate_slots  proxy_conformance  proxy  proxy_registry  roll_shed  scan
    stop_sweep  tenant_rebalance  tenant_remove  upgrade

Two lost the race on one run. The other 28 are the same shape and differ only
in how long their controller takes to notice.

## The fix, and why it is the drills rather than fleet_kill

Kill the supervisor first. `fleet_kill` cannot reorder this for them — callers
invoke it once per seat kind, so the ordering lives in each drill.

Nor should it be fixed by making `fleet_kill` return early again: a cleanup
that kills managed nodes while their manager runs is wrong independently of
timing, and it would come back the moment anything else slowed teardown down.

## What must land with it

A check that fails on the wrong order. The gate's existing leak detector is
that check — it caught this — but it only fires when the race is actually lost,
which is why 28 of the 30 look fine today. A source assertion that no cleanup
kills `server` before `controller` would cover all 30 deterministically, and
belongs with the reorder.

## Ordering

1. This bug (reorder + the source assertion).
2. Then BUG-0051's `fleet_kill` wait, which is already written and verified by
   `tools/kill_release_drill.sh` and is blocked only by this.

Doing 2 first turns an intermittent leak into a reliable one across 30 drills.
