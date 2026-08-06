# BUG-0005: a systemd oneshot killed the daemons it spawned (RESOLVED)

Status: RESOLVED 2026-08-06 · Severity: high (supervision that destroys what it restarts) · Cost: most of a day of misdiagnosis

## Symptom
A supervise timer running `flintctl start` every minute restarted a killed
replica and the replica died again within ~74 seconds, repeatedly. The seat
logged a clean start every time — full sync complete, listening,
replicating from the master — and then nothing. No panic, no error, no
shutdown line.

## What it was diagnosed as, wrongly
In order: a WAL-gap re-seed loop; an OOM kill; controller fencing; a
supervisor stampede (BUG-0004). Every hypothesis was about the node. All
were wrong.

## Root cause
`flint-supervise.service` is `Type=oneshot`, and systemd's default
`KillMode=control-group` kills everything remaining in a unit's cgroup when
the unit DEACTIVATES. The seats `flintctl start` spawns inherit that
cgroup. So the instant ExecStart returned, systemd killed the flint-server
it had just started.

## Why it survived four investigations
Every manual `flintctl start` survived indefinitely — because an ssh
session puts the process in `user.slice/.../session-N.scope`, not a service
cgroup. The bug could not occur in the one place it was being observed.
`cat /proc/<pid>/cgroup` is the check that distinguishes them; the
controlled experiment is timer-off versus timer-on (300s+ alive vs dead in
74s).

Finding it also required fixing the logging first: seat logs were opened
with `File::create`, so each respawn erased the previous run's output and
every investigation opened a file describing a healthy boot. Appending
showed three runs each ending mid-sentence, which is what says SIGNALLED
rather than FAILING.

## Fix
`KillMode=process` on the unit. `flint-first-boot.service` carried the
identical hazard and worked only because `RemainAfterExit=yes` keeps its
unit active so the cgroup is never cleaned — meaning
`systemctl restart flint-first-boot` would have taken the whole fleet down.
It carries `KillMode=process` now too.

**General rule: any unit that starts long-lived processes it does not
itself supervise needs `KillMode=process`, and "it works today" may only
mean its cgroup has never been cleaned.**

## The check that holds it
The AMI smoke test asserts `KillMode=process` on both units, that the
supervise timer has an actual next elapse, and then kills a seat on a fresh
instance and requires it to be serving 150 seconds later — two ticks, so a
seat restarted and re-killed still reads as failed.
