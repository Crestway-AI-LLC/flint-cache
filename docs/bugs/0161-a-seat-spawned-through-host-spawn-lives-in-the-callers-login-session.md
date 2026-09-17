# BUG-0161: a seat spawned through `host-spawn` lives in the caller's login session (FIXED)

Status: **FIXED 2026-09-17**, found in the 24-hour ops-agent review Jeff ordered
· Severity: **high** — no data was lost, and nothing had died yet. But every
seat the remote runner starts sits one `loginctl terminate-session` away from
dying, and the ops agent's own repair path is how it got there.

## What was on the box

At 16:58:57Z the ops agent on the primary ops box saw the playground's replica
gone: `pair 0: live_replicas:0 (single-copy exposure)`, `:7001=unreadable`. That
condition was real. At 16:59:07Z it repaired it, and the playground's journal
recorded the exact commands, each from an ssh login:

    sudo[3252132]: ec2-user : ... COMMAND=/opt/flint/bin/flintctl host-stop-seat /var/lib/flint node-7001 ...
    sudo[3252192]: ec2-user : ... COMMAND=/opt/flint/bin/flintctl host-spawn /var/lib/flint /opt/flint/bin node-7001 flint-server -- ...

The replica came back and rewound cleanly. It came back **here**:

    3252222  flint-server  0::/user.slice/user-1000.slice/session-8534.scope
    330088   flint-server  0::/system.slice/flint-ctl-330075.service      <- the master, rolled via ctl.sh

    $ loginctl show-session 8534
    Remote=yes RemoteHost=172.31.14.109 State=closing

`172.31.14.109` is the primary ops box. The session had already ended. The
seat survived only because `KillUserProcesses` defaults to `no`. One
`loginctl terminate-session 8534`, or a host hardened with
`KillUserProcesses=yes`, would have ended it. It was the only seat on the box
in that state. Jeff moved it with `ctl.sh restart-node` (warm rejoin,
verified), and every seat is now under `system.slice`.

## Why: the fix existed, on the other path

This hazard was found and fixed on **2026-08-09**, and `ctl.sh`'s header
records it: all six playground seats were then in session scopes of logins
that had ended days earlier. The fix hands the whole `flintctl` command to
systemd, `systemd-run --property=Type=oneshot --property=KillMode=process`, so
seats land in `system.slice`.

That fix is in `ctl.sh`, the wrapper an operator runs by hand. The remote runner
does not go through `ctl.sh`: it runs `sudo -n flintctl host-spawn` on the
target over ssh. `flintctl` itself contains **zero** occurrences of
`systemd-run` or `cgroup`. So the fix reached the call site that prompted it
and not the one the ops agent uses — the same shape as OPS-0108 and OPS-0095.

## The fix

**It lives on the target, in `host-spawn`,** because every remote spawn passes
through it whoever called it: the agent, an operator's ssh, a multi-host drill.
`host-spawn` reads its own cgroup, and when it is inside `/user.slice` it
re-runs itself under a transient systemd unit with `ctl.sh`'s flags,
unchanged. Those flags have placed every playground seat since 2026-08-09,
including all six on the rc.73 roll. Proven beats clever.
`--wait --pipe` hands the orchestrator the same stdout and exit status it had,
and `--quiet` keeps systemd-run's own status lines out of them.

**The decision is a pure function, `session_escape`,** with no I/O, so every
branch is tested on any machine. Re-running needs three things: the EFFECTIVE
uid is 0, systemd is booted (`/run/systemd/system`), and `systemd-run` is on
PATH. The uid is the second field of `/proc/self/status`'s `Uid:` line, not
the first. Under `sudo -n` the real uid is the ssh user's, and reading it would
have called every remote spawn unprivileged, so none would ever escape. That
case has its own test.

**When it cannot leave, it spawns anyway and says so,** naming the scope and the
remedy: `[node-7001] WILL DIE WITH ITS LOGIN SESSION: <scope>: not root ...
(inventory: ssh-sudo on)`. Refusing would break every unprivileged caller, and
the public drills run `host-spawn` unprivileged. A fleet without `ssh-sudo on`
gets this warning, not protection, and that is a real limit.

**The re-run is marked with an argument, `--escaped`, not an environment
variable,** because `local_spawn_env` does not clear the environment, so the seat
would inherit the marker. A re-run still in a user slice warns rather than
re-running, which would recurse with each level holding a unit open under
`--wait`. An older target flintctl skips the flag, because the parser already
skips unknown flags before `--`.

## Found while fixing it: the warning would have reached nobody

The orchestrator's `spawn_env` printed the remote's **stdout** on success and
discarded **stderr**. So the new warning would have been printed on the target
and dropped by the caller: a check whose output no one sees. It also meant
something already shipped was being swallowed on every remote spawn:
`lock_seat`'s `seat lock unavailable ... PROCEEDING UNLOCKED` (BUG-0144), which
says a spawn ran without the duplicate-seat guard. The orchestrator now prints
the host's non-empty stderr beneath its `started` line, as `    [<host>] ...`.
Four spaces and a bracket, so it cannot match `supervise.sh`'s `^  started `
count, the one script that parses this output. `supervise.sh` runs the local
runner anyway, which never takes this branch.

## Verification

- **Ten unit tests** in `session_escape_tests`: the playground's verbatim
  cgroup re-runs; the post-`ctl.sh` cgroup, also verbatim, spawns in place
  (this is what ends a re-run); no cgroups spawns; unprivileged, no systemd, or
  no `systemd-run` each warn with their reason; a re-run still in a session
  does not recurse; a v1 host reads the `name=systemd` hierarchy; malformed
  lines are skipped; under-sudo uid is the effective one; and the marker is
  inserted where the parser reads flags, never after `--`.
- **`host_verbs_drill`** gains an arm. It runs a real `host-spawn --escaped` and
  asserts two things only a real spawn can show. First, the marker is absent
  from the seat's own argv. Second, it was actually parsed: unprivileged inside
  a user slice, a re-run warns *"even after re-running under systemd"*, and an
  unparsed marker would warn *"not root"*. Anywhere else the arm prints the
  cgroup and uid it saw instead of passing silently.
- **A defect in that arm, found before it ran.** Its first draft read the
  cgroup with `CG=$(grep -m1 '^0::' /proc/self/cgroup | cut ...)` inside a
  drill running `set -euo pipefail`. With no `/proc`, as on macOS, grep's
  failure became the assignment's and `set -e` ended the drill. That was proven
  on this Mac: old form exit 2, new form (`|| true`) exit 0 with an empty
  cgroup.

## Not claimed

That the re-run places a seat under `system.slice` on the playground. That is
`systemd-run`'s behaviour with flags proven there since 2026-08-09, not
something this change has yet demonstrated on that box. The next remote spawn
there is the first observation of it, and its cgroup is the thing to read.

Also recorded, and not fixed here: `sudo` already journals `host-spawn`'s full
argv, `--env` values included. Those values are storage tuning and
`FLINT_BUILD_VERSION` today. But `node-env` is a generic inventory key, and
anything put in it is on disk in the journal on every remote spawn. That is
the credential concern `ctl.sh` raises about `--probe`, already true for this
path, and it predates this fix.
