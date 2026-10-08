# BUG-0227: `flintctl start` died on a fleet with a running co-processor (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by `edge_roll_drill`, which now runs
`start` twice with a co-processor up and requires both to exit 0, find it,
and leave its process alone; with the old probe it fails as the playground
did. And by a unit test that the token `start` probes for is one
`coproc_args` puts in the argv.
**Severity:** high on any fleet that runs a co-processor. flint-supervise
runs `start` every minute to respawn dead seats; it failed every minute, and
the seats `start` reaches after the co-processors, the proxies and the
controller, were left with nothing to respawn them.

## What happened

ops ADR-0050 step 4 added a co-processor to the playground on v0.1.0-rc.81,
2026-10-08 02:04Z. From 02:05 flint-supervise failed every minute; `verify`
paged "FLINT UNITS FAILED … flint-supervise.service" at 02:09 and page-watch
escalated at 02:14. Found by the Running Cache Agent; Jeff backed step 4 out
until the release carrying this fix.

`start` asks `seat_alive` whether each seat's process is running, and
`pids_in_ps` answers by an exact whitespace token of the process's argv: a
node is found by its data directory, which its argv carries. The
co-processor block asked for its seat name, `vec-7420`, which flint-vec's
argv carries only inside `--vec-dir /var/lib/flint/vec-7420`. A running
co-processor therefore read as dead, `start` went to spawn one, found the
live pid in the pidfile, and died with BUG-0144's refusal:

```
flintctl: refusing to start vec-7420: pid 1132829 is ALREADY running it.
```

`upgrade` was never affected: it stops a co-processor by its vec-dir path.
No drill ran `start` with a co-processor already up. The same refusal
appeared in a drill run for BUG-0226 the same evening and was read as
BUG-0138's guard against differing arguments.

## The fix

`coproc_vec_dir` is the one spelling of a co-processor's directory: its
`--vec-dir`, and the token `start` and `upgrade` find it by.
