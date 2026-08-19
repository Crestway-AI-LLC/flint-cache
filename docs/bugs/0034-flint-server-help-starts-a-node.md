# BUG-0034: `flint-server --help` starts a node on the default port (FIXED, one half open)

Status: `--help` FIXED 2026-08-19 · the unknown-argument half is OPEN ·
Severity: medium — an operator asking for usage gets a running node instead,
and one stray copy refused 64 drills in a single gate

## Symptom

    $ ./target/release/flint-server --help
    flint-server listening on 127.0.0.1:6380 (plaintext)
    ← never exits

No usage, no error, no exit. The flag is ignored and the binary does what it
does with no arguments: binds the DEFAULT port and serves.

## How it surfaced, which is the interesting part

A full gate at `6f2f47d` reported **66 failures**, 64 of them at 0 s. The
summary named `upgrade(leaked)` and the gate's own leak check said:

    LEAKED: upgrade left 2 Flint process(es) running

Every drill after it refused:

    REFUSING TO RUN: this box already has Flint processes outside /tmp/flint-affinity
        13437 ./target/release/flint-server --help

The "leaked seat" was a `--help` invocation holding `127.0.0.1:6380`. Because
6380 is the DEFAULT port, no drill declares it, so it fell outside every
drill's scope and `fleet_guard` refused — correctly, by BUG-0027's contract.
One process that was never a seat cost 64 drills.

**The leak attribution was also wrong**: `upgrade` PASSED and did not start it.
The check names the drill that was running when the process was first seen,
which is not the same as the drill that started it — worth remembering before
chasing a drill's cleanup on that evidence alone. The `--help` process was
started by something outside both repos; nothing in `tools/` invokes it.

## Fix

`--help` / `-h` are now handled beside `--build-version`, before the listener:
print usage, exit 0.

`--build-version` was already handled and exits cleanly, so `fleet_warm` —
which calls `"$bin" --build-version` on every drill startup — was never
affected. That was checked rather than assumed.

## The half that is NOT fixed

**An unrecognised argument is silently ignored.** This binary has no argument
parser; it scans `env::args()` once per flag it cares about. So:

    flint-server --prot 7001      # typo
    → listening on 127.0.0.1:6380

A mistyped flag starts a node on the default port and reports success. That is
the same defect class as the `--help` case and strictly more dangerous, because
the operator believes they specified a port.

Not fixed here because rejecting unknown arguments requires enumerating the
full accepted set, and doing that from a grep risks refusing a flag some caller
depends on — a worse outage than the one being fixed. It needs the accepted
set derived deliberately, ideally by moving to a real parser.

## Verification

- `flint-server --help` prints usage, exits 0, and starts no listener (checked
  with `lsof` on 6380 immediately after)
- `--build-version` still prints the stamp and exits — the path this shares

## Related

- BUG-0027 — the leak check and `fleet_guard`; both behaved correctly here.
  The refusals were right; the thing they refused should not have existed.
