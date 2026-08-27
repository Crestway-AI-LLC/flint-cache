# BUG-0051: `fleet_kill`'s wait-for-death loop reads a list that nothing fills

Status: FIXED 2026-08-26. The append is in `fleet_kill`, the stray one is
gone from `fleet_signal`, and `tools/kill_release_drill.sh` holds it there. · Severity: MEDIUM — the product is fine. This
is the teardown half of #176, it is twenty-five lines of comment explaining a
race and the code that answers it, and it has never executed once.

## What happened

`fleet_kill` (`tools/lib/fleet.sh:974`) selects our seats, re-verifies each pid
against an exact basename allowlist, `kill -9`s it, and then — per a long
comment about two earlier attempts that polled ports and were both wrong —
waits for each killed pid to actually be gone before returning:

    local pid args _killed_pids=""
    ...
    if [ -n "${_killed_pids# }" ]; then
      for _pid in $_killed_pids; do
        while kill -0 "$_pid" 2>/dev/null; do ...

Nothing ever appends to `_killed_pids`. The kill loop signals the pid and moves
on. The only append in the tree is at `tools/lib/fleet.sh:931`, inside
`fleet_signal` — a different function, which sends SIGSTOP/SIGCONT, does not
kill anything, and has no wait block to feed.

The two halves are swapped: the function that needs the list does not build it,
and the function that builds it has no use for it.

## Proven by running it, not by reading it

A pid whose `argv[0]` basename is exactly `flint-server`, so the allowlist
accepts it, with `_fleet_ours` stubbed to return it and `bash -x` recording
what ran:

    'kill -9 2121' in trace: 1     <- positive control: the kill path WAS reached
    'kill -0' in trace:      0     <- the wait loop never executed

The positive control matters here. The first version of this probe copied
`/bin/sleep` to a file named `flint-server`; the copy lost its signature, died
on launch, and the probe's "victim is dead" check passed for a reason that had
nothing to do with `fleet_kill` — a check that would have confirmed the claim
whether or not the claim was true.

## Consequence

Every caller falls back to the fixed `sleep` that the comment says stopped
being safe. Before #176 a restarted node did not bind until its initial full
sync finished, so a `sleep 0.4` had seconds of slack. #176 binds within
milliseconds of exec, and twenty-one drills follow this call with a respawn on
the same ports. The documented symptom is `promote_notice`'s "nothing listening
on 127.0.0.1:6911 after 30s": the replacement lost the race to the socket, took
EADDRINUSE, exited, and the drill then measured a promotion into a node that
was not there — 5064 ms against a 19 ms steady state.

That flake is live. It reads as a product failure and is a harness race.

## Two smaller defects in the same swap

- `fleet_signal`'s append sits **before** the `case` filter, so it records pids
  it then `continue`s past without signalling.
- `_killed_pids` is not declared `local` in `fleet_signal`, so it leaks into
  whatever called it.

Neither has visible effect today, because `fleet_kill`'s own `local
_killed_pids=""` shadows the leaked value — so even the accidental cross-talk
cannot make the loop fire.

## Why it stayed invisible

A wait that never happens is indistinguishable from a wait that finished
immediately. There is no output either way, the guard `[ -n "${_killed_pids# }" ]`
fails silently, and the failure it was written to prevent is intermittent. The
comment above it reads as evidence the problem was handled.

## The fix

Append inside `fleet_kill`, after the allowlist `case` accepts the pid, and
delete the stray append from `fleet_signal`.

## What must land with it

Not the one-liner on its own. This is a bug whose entire shape is "code that
never ran", so the fix needs a check that fails without it: kill a seat and
respawn on the same port immediately, asserting the bind succeeds — with the
release **observed**, not slept past. Landing the append alone would replace
dead code with unwitnessed code, which is the same defect one step later.


## The drill, and the six attempts its control took

The bug doc said the fix must not land without a check that fails against the
unfixed code, because "code that never ran" replaced by "code nothing
witnesses" is the same defect one step later. That check now exists and is
verified both ways:

    unfixed fleet_kill : FAIL: fleet_kill returned while pid 2507 was still alive
    fixed              : PASS — pid gone on return, replacement bound 6955

It asserts the POSTCONDITION with no sleep in between — when `fleet_kill`
returns, the pid is gone — then respawns on the same port immediately and
requires the replacement to bind and to be a DIFFERENT pid. That second half is
the consequence the race actually produces: the replacement takes EADDRINUSE,
exits, and surfaces later as "nothing listening" nowhere near the cause.

The postcondition discriminates only because a real seat takes time to die,
which was measured before the drill was written: a `flint-server` with rocks
open is still present for 29-83 poll iterations after `kill -9`, five runs of
five. Had that been consistently zero the assertion would have passed either
way.

**Six attempts at the control, and the first five failed for reasons that had
nothing to do with the assertion:** a copied `/bin/sleep` that lost its
signature and died before the kill path ran; a port already declared by
`backup_schedule`; `FLINT_CORE_ORDER` narrowing CORE so gates.sh refused
because every other drill became unclassified; the same again; and
`fleet_wait_listen 127.0.0.1 $P 30` when the function takes ports only, which
produced `nothing listening on 127.0.0.1:127.0.0.1`.

Every one of those exited non-zero. A control checked by exit status would have
been recorded as working five times over. **A control that fails is not a
control that worked** — the only thing that separated them was reading which
message came back.
