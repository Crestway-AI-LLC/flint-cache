# BUG-0034: `flint-server` ignores unrecognised arguments and starts a node (help/version FIXED; rejection ATTEMPTED AND REVERTED)

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

## Rejection was implemented, hung two gates, and was reverted the same hour

**Do not attempt this again from a grep.** The general fix — refuse any
unrecognised argument — was written, verified against every flag `flintctl`
passes when spawning a node, and it still broke the suite. `slot_map_drill.sh`
starts a seat like this:

    $B --port 7101 --engine rocks --data-dir "$D/a" --advertise 127.0.0.1:7101

`--advertise` is a PROXY flag. `flint-server` has never read it — `arg("--advertise")`
appears zero times — so passing it was always a silent no-op, and rejection
turned that no-op into `exit 2`. The drill then waited forever for a seat that
would never bind: `slot_map` normally finishes in **6 s** and hung for **4:45**
before the gate was killed. It hung two runs before the cause was found, and
the first was misread as contention because three earlier reds that day
genuinely were.

**Why the enumeration failed even though it was checked.** The accepted set was
derived correctly from the CALLEE — `arg()` is the only way a value reaches
this binary, so the 32 call sites really are exhaustive. The unchecked half was
the CALLER: flintctl was verified, the drills were not, and the drills pass
flags that flint-server has always ignored. An argument list has two ends and
enumerating one of them proves nothing about the other.

That is the concrete reason the original write-up deferred this, and the
deferral was right. Reinstating it needs the caller side enumerated too, which
means every direct `flint-server` invocation in `tools/` — and ideally the
spurious flags removed there first, so rejection cannot break anything by
construction.

### What IS fixed

`--help`, `-h` print usage and exit 0. `--version` and `-V` now alias
`--build-version` and print the stamp and exit 0.

Those cover both incidents this defect actually caused: the stray `--help` node
on port 6380 that put 64 drills into `fleet_guard` refusals, and `--version`
hanging a box with a 30-minute TTL during release acceptance. Narrow, and safe
because they add early exits rather than removing a fall-through.

### What is still open, and is still a real hazard

    flint-server --prot 7001    # typo
    → listening on 127.0.0.1:6380

A mistyped flag still starts a node on the default port and reports success.
That is worse than the two fixed cases, because the operator believes they
specified a port.

## What the half-fix looked like before this

`--help` was the symptom; **ignoring unknown input was the defect**. A peer
session hit the same bug from a different direction while building release
acceptance:

    flint-server --version   → starts a node, never exits

That hung a box with a 30-minute TTL. A "report the build" check reached for
the flag every operator reaches for, and got a running server. (`flintctl
--version` is the one that answers, and since `FLINT_RELEASE_TAG` is baked at
compile time it is also the check that catches a bundle built from the wrong
commit.)

Two incidents from one defect settled that adding flags to an early-exit list
one at a time is not a fix. Unrecognised arguments are now REFUSED with exit 2.

**The enumeration is not a guess, which is what deferring it was waiting on.**
`arg()` is the only way a value reaches this program and `env::args()` is read
in exactly three places — that helper, the `--build-version` check, and the
`--help` check. So the accepted set is the `arg()` call sites: 32 value flags
plus three bare ones. Every flag flintctl passes when spawning a node — port,
bind, engine, data-dir, journal, replica-of, rewind-snaps, the three
`internal-*` and the five tuning flags — is in it, checked before the change
rather than after.

A value flag with nothing after it is also refused, rather than silently
defaulting.

Measured:

| argument | exit | |
|---|---|---|
| `--version` | **2** | `unrecognised argument '--version'` |
| `--prot 7001` (typo) | **2** | `unrecognised argument '--prot'` |
| `wat` | **2** | `unrecognised argument 'wat'` |
| `--port` (no value) | **2** | `--port expects a value and got none` |
| `--help` | 0 | usage |
| `--build-version` | 0 | the stamp |
| `--port 6390 --engine mem --bind 127.0.0.1` | — | still starts and serves |

## What the half-fix looked like before this

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
