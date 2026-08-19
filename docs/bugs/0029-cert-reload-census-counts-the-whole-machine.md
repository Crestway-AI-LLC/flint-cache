# BUG-0029: cert_reload_fleet's pid census counted the whole machine (FIXED)

Status: FIXED 2026-08-19 · Severity: high — it converted an unrelated
process's lifecycle into a product claim about hot-reload

## Symptom

Full gate at `95c250f`:

    == every listener presents the NEW serial, same pids
      after:    cp=8E23C2C82EC6E6E1 node=8E23C2C82EC6E6E1 edge=8E23C2C82EC6E6E2
    FAIL: pids changed — something restarted
    tools/cert_reload_fleet_drill.sh: line 89: /tmp/flint-certreload/writer.out: No such file or directory

Read as written, that says cert rotation restarted a component instead of
hot-reloading it — a durability-adjacent regression in the thing the drill
exists to protect. **It did not happen.** The three serials advanced on all
three listeners, which is the assertion that actually tested the product, and
it passed.

## Cause

`tools/cert_reload_fleet_drill.sh:75`:

    pids() { pgrep -f "flint-(server|proxy|controlplane|controller)" | sort | tr '\n' ' '; }

Unscoped. `PIDS1` and `PIDS2` describe the **box**, not this drill's fleet, and
line 105 turns any difference between them into "something restarted". The
drill declares `fleet_init $FLINT_DRILL_ROOT/flint-certreload 7061 7062 7795
7998` and then measures without reference to either.

Two distinct populations leak in, and the second is the nastier one:

1. **Another suite's seats.** A concurrent session's fleet
   (`flint-proxy :9614`, control plane `:7598`) was up when `PIDS1` was taken
   and gone when `PIDS2` was taken. That delta alone fails the drill.

2. **Processes that are not Flint at all.** `pgrep -f` matches the whole
   command line, so anything whose arguments merely *contain* the string
   counts. On this box that includes the **agent sessions themselves**, which
   carry `--add-dir .../crates/flint-proxy`:

       $ pgrep -f "flint-(server|proxy|controlplane|controller)"
       44735  ./target/release/flint-controller ...
       44907  .../target/release/flint-server ...
       45590  .../target/release/flint-server ...
       64161  /Applications/Claude.app/.../disclaimer ...      <- not Flint
       64162  .../claude.app/Contents/MacOS/claude ...          <- not Flint

   So **opening or closing an editor between the two samples turns this drill
   red**, and the failure it prints is about certificate hot-reload.

## Why it hid

Four other drills failed in the same gate on the same foreign fleet, but they
failed by **refusing** — `fleet_guard` names the foreign processes and declines
to run. A refusal is self-describing. This one *ran*, sampled a population it
never owned, and reported the difference as a product defect. From the outside
it did not look like the other four, which is exactly why batch-attributing all
five to "the peer's fleet" would have filed the wrong conclusion — and why the
fifth is the one worth keeping.

**A refusal names the foreign fleet; an unscoped census launders it into a
product claim.**

## The second line is not a second symptom

`line 89: .../writer.out: No such file or directory` is the writer subshell's
own redirect. The writer loops for 8 s and writes only at the end; the parent
reaches the assertion at ~4.5 s, exits 1, and its cleanup removes `$D`. The
orphaned writer then has no directory to write into. It will accompany **every**
failure on lines 102-105 and says nothing about anyone deleting state.

## Fix

    pids() { _fleet_ours "server proxy controlplane controller" | sort | tr '\n' ' '; }

`_fleet_ours` matches on the executable's **basename** and then requires the
scope dir or a declared port, so neither population can enter the set: a
process named `claude` fails the basename test, and a foreign fleet fails the
ownership test. Same component list, same intent, scoped.

## Scope of the class

Both repos were swept for unscoped process matching in drills. Every other
`pgrep`/`pkill` is anchored — by `$STATE`, by `--id A/B/C`, by `--node-id
$LEADER`, by `--port $P1`. This was the only bare one in the public repo; the
fleet repo has none.

This is the **eighth** unanchored-match defect found on 2026-08-19 and the
first that fabricates a claim rather than merely miscounting. The others were
`pgrep -f` patterns that matched the command doing the searching; this one
matches bystanders. Same root: `-f` searches a string that appears in more
places than the author pictured.

## Related

- **The scope-dir gap, found alongside it.** `assert_no_port_overlap` checks
  declared ports; nothing checked scope dirs, and scope is the *other* half of
  `_fleet_ours`'s ownership test. `coproc_cred` and `coproc_family` both
  claimed `$FLINT_DRILL_ROOT/flint-coproc` with disjoint ports, so each could
  select the other's seats and each `rm -rf`s that dir at start — benign only
  because the gate runs drills sequentially. Fixed by giving `coproc_family`
  its own dir and adding `assert_no_scope_overlap`, which was verified with a
  negative control (silent on the clean tree) before the positive one (fires
  and names both drills when the collision is reintroduced).
- BUG-0027 — leak attribution; the guard that correctly refuses rather than kills
- BUG-0020 — drills whose ports are invisible to the declaration parser
