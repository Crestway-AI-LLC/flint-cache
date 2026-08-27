# BUG-0063: the sibling guard cannot see an orphan, so it tells you to wait forever (OPEN)

**Found** 2026-08-27, blocking `tools/walgap_quarantine_drill.sh`. A
`flint-kv-server` sat at **ppid 1, 0.0% CPU, for 44 minutes** and every drill
on this box refused with:

```
REFUSING TO RUN: another Flint-family project has a fleet up on this box
  ... Wait for that run to finish, or stop it from ITS project.
```

There was no run to finish and no parent to finish it. The advice was for
something that could never happen.

## The same lesson, learned on one branch and not the other

`fleet_guard` has two refusal branches, and the FOREIGN one already knows this:

```
if [ "$_live" = "0" ]; then
  echo "  ALL $_orph ARE ORPHANS (ppid 1): nobody is driving them, so"
  echo "  waiting will not clear this. Someone has to remove them."
```

That was written after 2026-08-20, when a drill was refused for five hours
against orphaned writer subshells and two sessions both read the refusal as
"a peer is working, wait". The SIBLING branch never got it — so the identical
failure recurred in the one place the first fix could not reach. Same family
as OPS-0056: a lesson applied to one of two paths, with nothing comparing them.

## Why it cannot simply be copied across

The obvious patch is wrong, and worth recording because it looks right:

```sh
_sorph=$(printf '%s\n' "$sibling" | awk '$2 == 1' | wc -l)   # WRONG
```

`_fleet_foreign` emits `ps -eo pid=,ppid=,args=`, so `$2` is the ppid and its
orphan test is sound. **`_fleet_sibling` emits `ps -eo pid=,args=`** — `$2` is
the first word of the COMMAND. The copied test would compare a path against
`1`, always find zero orphans, and print nothing, while looking like a working
check. A positional field read from the wrong producer, which is BUG-0013's
W-Amp shape exactly.

So the fix has to widen `_fleet_sibling` to carry ppid, and that changes a
format three other call sites consume (`cut -c1-90` at the refusal, `awk
'{print $1}'` in `_fleet_sibling_sample`, and the settle loop). Not a one-line
change, which is why this is filed rather than patched under an unrelated task.

## The narrower cause, and the better fix

The guard is not simply blind here — it deliberately treats these differently:

```sh
if [ -n "$sibling" ] && [ -z "$foreign" ] && [ -z "$(_fleet_sibling_named)" ]; then
  _fleet_sibling_settle && return 0
```

An UNNAMED sibling (a cargo test binary like `ttl-ccbacfe2d0cd3f35`) is
activity-sampled and waited out. A NAMED one (`flint-<project>-<component>`)
is presumed to be a live fleet and refused outright. That presumption is
reasonable and it is what fails: a named *server* binary is exactly what a
crashed harness leaves behind, and orphaned is precisely the state in which it
is NOT a live fleet.

`_fleet_sibling_activity` already samples CPU, so the machinery to tell an idle
orphan from a working fleet exists and is simply not consulted on this path.
Extending the settle to a named sibling that is BOTH ppid 1 and idle would have
cleared this without weakening the guard: the thing it exists to prevent is
CONTENTION, and a process at 0.0% CPU contends for nothing.

**Do not "fix" this by widening `FLINT_DRILL_FORCE`.** The guard was right to
refuse a box it could not reason about; it was the reasoning that was missing.

## Impact

Every drill in this repo is blocked on a box carrying an orphaned sibling
binary, with a message that sends the operator to wait. Recovery requires
someone to notice the ppid and remove it by hand — which is what the foreign
branch was changed to make unnecessary.
