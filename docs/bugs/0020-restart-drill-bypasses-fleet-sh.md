# BUG-0020: `restart_drill.sh` bypasses `fleet.sh`, so it declares no ports and cleans up unscoped (OPEN)

Status: OPEN, found 2026-08-18 when a full gate reported `restart(leaked)` ·
Severity: **medium** — one drill sits outside both the port-overlap preflight
and the scoped-cleanup contract that every other drill is held to

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
