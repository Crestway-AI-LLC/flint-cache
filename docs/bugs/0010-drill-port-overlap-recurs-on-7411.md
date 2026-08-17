# BUG-0010: the port-overlap guard is back in the red — 7411 claimed twice (OPEN)

Status: OPEN, found 2026-08-16 · Severity: medium, but corrosive — `gates.sh`
now exits FAILED on every run, which trains readers to ignore a red gate

This is **BUG-0003 recurring**. That write-up fixed the overlaps that existed
in August 2026 and added the preflight that catches new ones. The preflight is
working exactly as designed; what it is reporting is a fresh overlap
introduced since.

## Symptom

Any invocation of the check stage:

    $ tools/gates.sh check
    FAIL  two or more drills declare the same port(s):
            7411: tools/build_stamp_drill.sh tools/coproc_family_drill.sh
            fleet_guard reads the other drill's seats as this drill's own.
            Give each drill a disjoint block.
    PASS  fmt                    (2s)
    PASS  clippy (mem)           (2s)
    PASS  clippy (rocks)         (4s)
    PASS  test (mem)             (99s)
    PASS  test (rocks)           (46s)
    PASS  licences               (1s)

    GATES FAILED: port-overlap

Every real stage passes. The only failure is the preflight, so the gate's exit
status has been decoupled from whether the code is good.

## Root cause

`tools/build_stamp_drill.sh` and `tools/coproc_family_drill.sh` both declare
port **7411**. Per BUG-0003, `tools/lib/fleet.sh` decides seat ownership by
scope directory OR declared port, so a port claimed by two drills makes each
treat the other's processes as its own — `fleet_guard` waves them through and
`fleet_kill` sends them `-9`.

`coproc_family_drill.sh` is the newer of the two, arriving with the
co-processor work (`73b7eb6`, `d72378d`, `38a1bb6`).

## Confirmed not to be a side effect of anything in flight

Reproduced on 2026-08-16 with a working tree whose only modification was
`crates/flint-proxy/src/main.rs`; both drill files were committed and
unmodified. `git grep 7411 tools/` shows exactly the two claimants.

## Fix

Give one of the two a disjoint block. `coproc_family_drill.sh` is the natural
one to move, being newer, unless its port numbers are referenced from
elsewhere (check `docs/` and any fixture that hardcodes them). Then confirm:

    tools/gates.sh check      # must reach GATES PASSED

## Why it matters more than a port number suggests

The gate is the project's authority on release readiness — `gates.sh` exists
precisely because "the drills pass" used to mean a shell one-liner
reconstructed from memory. A gate that always exits FAILED for a known-benign
reason is worse than a gate that fails loudly for a real one, because the
habit it teaches is to skim past the red line. BUG-0009 in this same directory
is the mirror image: a gate that printed PASSED having run nothing.

## Related

- `0003-drill-port-overlap.md` — the original, and the mechanism in full
- `0009-unknown-stage-passes-the-gate.md` — the other way this gate has lied
