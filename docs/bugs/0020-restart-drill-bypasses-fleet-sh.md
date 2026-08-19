# BUG-0020: `restart_drill.sh` bypasses `fleet.sh`, so it declares no ports and cleans up unscoped (FIXED)

Status: **FIXED 2026-08-18**, found the same day when a full gate reported
`restart(leaked)` · Severity: **medium** — one drill sat outside both the
port-overlap preflight and the scoped-cleanup contract that every other drill
is held to

## Symptom

A full gate run at `c1371d9`:

    PASS  restart                (3s)
          LEAKED: restart left 1 Flint process(es) running
    ...
    GATES FAILED: restart(leaked) chaos

The drill's own assertions passed. The leak was invisible to it and visible
only to the gate's global check — which is the shape of a cleanup that does not
own what it is cleaning.

## Root cause

`tools/restart_drill.sh` (78 lines) never sources `fleet.sh`. It calls neither
`fleet_init` nor `fleet_guard` nor `fleet_kill`. Its entire teardown is:

    11: PORT="${2:-6410}"
    15: cleanup() { pkill -f "flint-server --port $PORT" 2>/dev/null || true; rm -rf "$DIR"; }
    63: pkill -9 -f "flint-server --port $PORT"

Only two drills in the suite bypass `fleet_init`: this one and
`gates_drill.sh`.

## Two independent consequences

**1. It is invisible to the port-overlap preflight.** `assert_no_port_overlap`
greps `^fleet_init` across `tools/*_drill.sh`, so a drill that never calls it
declares nothing and can collide with any declared block without the guard
noticing. That is the guard BUG-0003 added and BUG-0010 re-anchored, with a
drill sitting inside its blind spot. **Not firing today** — 6410 is unclaimed
by any declared block — so this is structural, not active.

**2. Its cleanup is unscoped, which is the likely leak mechanism.** Every other
drill gets `fleet_kill`, scoped to the ports it declared and aware of what it
owns. This one gets a bare `pkill -f` on an EXIT trap. If the trap does not
fire, or the pattern does not match the spawned argv exactly, the seat
survives and nothing in the drill notices.

`pkill -f` is also the matcher BUG-0010 had to anchor elsewhere, because it
matches any command line containing the string — including our own tooling's
argv.

## What this did NOT cause

The same gate run failed `chaos` on a data-integrity assertion, and the obvious
theory is that the leaked seat interfered. **It did not**, and the reasoning is
worth keeping so nobody re-derives it:

- `chaos_drill.sh:7-8` declares 6330-6337 and calls `fleet_guard`.
- restart's seat was on **6410**, outside that block.
- `fleet_guard` is scoped to the caller's own `fleet_init` ports, so it
  correctly did not refuse, and chaos ran clean into its iterations.

The gate's leak check is **global**; `fleet_guard` is **per-drill**. They
answer different questions and both answered correctly — this is not a
disagreement between two checks, and should not be filed as one.

## Fix

Bring `restart_drill.sh` under `fleet.sh`: `fleet_init` with 6410 declared,
`fleet_guard` at the top, `fleet_kill` for teardown. That closes both
consequences at once — the port becomes visible to the preflight and the
cleanup becomes scoped and owned.

Then consider whether `assert_no_port_overlap` should **fail** on a drill that
declares nothing, rather than silently skipping it. A preflight that cannot see
a participant is the same defect one level up: a check that passes because it
had nothing to check.

## Related

- BUG-0003, BUG-0010 — the port-overlap guard this sits outside of
- BUG-0019 — also a drill whose failure mode is invisible where it matters

## Fixed

`tools/restart_drill.sh` is now under `fleet.sh` like the other 100 drills:

    fleet_init $FLINT_DRILL_ROOT/flint-restart- 6410
    fleet_guard
    fleet_kill server

The port is a **literal on the `fleet_init` line**, not `$2`. That detail is
the whole first consequence: `assert_no_port_overlap` builds its map by
parsing those lines, so a port reaching the drill through an argument declares
nothing no matter how correct the rest of the drill is. The `[PORT]` argument
is therefore gone; nothing passed it (gates.sh runs every drill bare).

The mid-run `pkill -9 -f "flint-server --port $PORT"` became a **checked**
scoped signal:

    fleet_signal_port "$PORT" -9 \
      || { echo "FAIL: no seat of ours on 6410 to kill — ..."; exit 1; }

`fleet_signal_port` returns non-zero when it signalled nothing, and that
distinction is the drill: a kill that matched no process leaves the original
server up, so the restart timed afterwards is a server that never went down
and the survival check reads memory that was never reloaded from disk. The
old `pkill` had no such signal.

The restart-timing loop also gained a 60 s deadline. It polls at 20 ms because
it *is* the measurement (`fleet_wait_ping`'s 200 ms would be too coarse), but
it was unbounded — and `gates.sh` puts no timeout around a step, so a server
that never came back would have hung the entire gate instead of failing one
drill.

### The preflight blind spot itself is closed

The write-up asked whether `assert_no_port_overlap` should fail on a drill
that declares nothing. The answer turned out not to be a blanket rule:
`gates_drill.sh` exercises `gates.sh` itself and starts no seats, so it has
nothing to declare and a blanket rule would need an allowlist. The invariant
that actually holds is narrower — **a drill that starts something must say
what it owns** — and `gates.sh` now asserts exactly that
(`assert_spawning_drills_declare_ports`). Today the suite is clean under it:
one drill lacks `fleet_init`, and it spawns nothing.

### A third defect, found while verifying

Running the fixed drill at `KEYS=20000` failed with:

    FAIL: string lost ()

Nothing was lost. `key:0042000` was hardcoded and is never written when
`KEYS <= 42000`, so the drill reported a durability failure about a key that
had never existed — a check that could not answer, printing something
indistinguishable from an answer. The empty `()` was the only tell. The gate
always passes the 100000 default, so it never fired there. The sample index is
now derived from `KEYS`, and the drill asserts the key is present *before* the
kill, so the survival check can never be vacuous.

### Verified

- clean tree passes the new preflight; a synthetic drill that spawns without
  `fleet_init` makes it FAIL and sets `FAILED` (both controls run)
- drill PASSES at `KEYS=20000` and `KEYS=100000`, restart-to-PONG 59 ms / 121 ms
- the gate's own leak predicate,
  `pgrep -f 'target/release/flint-(server|proxy|controlplane|controller|agent)'`,
  reports **0** after every run
- forced-failure control: with the seat killed out from under it, the drill
  stops at the new guard and **exits 1** rather than timing a restart that
  never happened
