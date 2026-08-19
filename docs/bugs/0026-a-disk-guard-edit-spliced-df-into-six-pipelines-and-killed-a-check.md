# BUG-0026: a disk-guard edit spliced `df -h` into six pipelines, so the gate reported disk usage as the reason for every failure (FIXED)

Status: **FIXED 2026-08-19**, found 2026-08-18 · Severity: **high** — the gate's
primary failure explanation was replaced by a healthy-looking disk table for
every red run since 2026-08-?? (`c539a0d`), and one preflight was left unable
to fail at all

## Symptom

A real failing gate, captured while verifying BUG-0021's inline dump:

    FAIL  synthfail              (1s)  .../drill-synthfail.log
            Filesystem      Size    Used   Avail Capacity iused ifree %iused  Mounted on
            /dev/disk7s1   931Gi    75Gi   856Gi     9%    216k  9.0G    0%   /Volumes/FlintDev
          LEAKED: synthfail left 1 Flint process(es) running
            Filesystem      Size    Used   Avail Capacity iused ifree %iused  Mounted on
            /dev/disk7s1   931Gi    75Gi   856Gi     9%    216k  9.0G    0%   /Volumes/FlintDev

The drill's log contained `FAIL: synthetic failure to prove the dump`. None of
it reached the summary. The `LEAKED` line, which is supposed to name the
processes, printed the same table.

The comment sitting directly above the broken line says:

> A summary that can print a HEALTHY line as the reason for a failure is worse
> than printing nothing, because it is read as the answer.

That is an exact description of what the line beneath it was doing.

## Root cause

`c539a0d` ("drills: one configurable scratch root, and a disk guard that
measures it (#180)") added a legitimate standalone disk display:

    df -h "$_GATE_DISK_TARGET" | sed 's/^/  /'

and, by what the spacing shows was a mechanical substitution over `| sed`,
pasted the same text into **six other pipelines** where it consumes nothing and
discards stdin. Every one carries the tell-tale doubled space:

| line | what it should print | what it printed |
|---|---|---|
| 249 | the step's SKIP lines | disk usage |
| 274 | **the reason a step FAILED** | disk usage |
| 296 | the leaked processes (`ps` output) | disk usage |
| 390 | `assert_no_default_ports`' offending lines | disk usage |
| 599 | the conformance `overall:` results | disk usage |
| 633 | the drill file `assert_drill_builds_keep_rocks` reads | disk usage |

Before `c539a0d` each was a plain `... | sed 's/^/   /'`. `git show
c539a0d^:tools/gates.sh` has them intact, which is where the fix came from —
this was a restoration, not a redesign.

## The worst of the six: a check that could not fail

Line 633 feeds `df` output into the variable `assert_drill_builds_keep_rocks`
greps, so `grep 'cargo build.*-p flint-server'` never matched anything. The
check has been incapable of failing since `c539a0d`.

Demonstrated rather than argued. Planting a drill that does exactly what the
check forbids, and running the SHIPPED function under bash:

    === POSITIVE CONTROL: plant a drill doing `cargo build --release -p flint-server` ===
      -> returned 0 with no output = the check found NOTHING

After the fix, the same plant fires, and the clean tree is silent.

It guards a real cascade: a drill that rebuilds `flint-server` without
`flint-server/rocks` downgrades `./target/release/flint-server` to a mem-only
binary, and every later `--engine rocks` drill then reports "nothing listening".

## What the revived check immediately found

Its first clean-tree run was **not** silent. It flagged
`tools/tenant_remove_drill.sh`, where `35569c8` had spliced a `fleet_warm` call
into the middle of a backslash continuation:

    cargo build --release -q -p flint-server -p flint-proxy -p flint-controlplane \
    fleet_warm ./target/release/flint-server ./target/release/flint-proxy ...
      -p flint-controller -p flint-ctl --features flint-server/rocks

So the drill ran `cargo build ... flint-controlplane fleet_warm ./target/...`
(cargo, given `fleet_warm` as an argument) and then tried to execute
`-p flint-controller -p flint-ctl --features flint-server/rocks` as a command.
The drill passed anyway, because the gate builds the binaries in its own step
first and the drill never checks its build's exit status — so `fleet_warm`, the
whole point of the line, never ran either.

A sweep for the same shape across `tools/*.sh` and `tools/lib/*.sh` found this
one instance and no others.

## Fix

Remove the six mid-pipeline splices, keeping the standalone disk display at
line 362. Repair the `tenant_remove_drill.sh` continuation so the build
completes and `fleet_warm` runs after it.

Verified, both directions:

- `assert_drill_builds_keep_rocks`: clean tree silent; a planted violation
  fires and names only the planted file
- a failing step now prints `FAIL: the reason this step failed` where the disk
  table used to be, and the run contains **zero** stray `Filesystem` tables
- `tenant_remove` drill passes with the repaired build line

## Why it survived this long

Nothing here fires on a green run. Five of the six sites only produce output
when something fails, and the sixth is a check whose whole job is to stay
silent. So the gate looked correct for as long as it was passing, and the one
run that could have exposed it — a red gate — printed a plausible-looking table
instead of an error, which reads as a formatting quirk rather than as lost
evidence.

It compounded BUG-0021 exactly: that bug destroyed the failing run's *log*,
this one destroyed the failing run's *summary*. Between them a red gate carried
neither the reason nor the evidence, which is why BUG-0024 was filed with an
error string and nothing else.

## Related

- BUG-0021 — the other half of an unreadable red gate; its inline dump is what
  surfaced this
- BUG-0009 — also `gates.sh`, also a result that does not mean what it says
- BUG-0022 — same shape one layer down: an instrument that cannot answer,
  reporting the answer the reader expects
