# BUG-0019: `FLINT_DRILL_ROOT` on a mounted volume breaks `disk_pressure`, and the error blames the image (OPEN)

Status: OPEN, found 2026-08-18 running the gate for the BUG-0017 fix ·
Severity: **medium** — the gate reports a red that is not the product's, and its
message points away from the cause

## Symptom

    FAIL  disk_pressure          (2s)
    == a 512 MB filesystem to run out of
    FAIL: could not attach the disk image

`GATES FAILED: disk_pressure`, exit 1, against 115 other steps passing. Nothing
had started yet — no seat, no server, 2 seconds in.

The run used `FLINT_DRILL_ROOT=/Volumes/FlintDev/drillscratch`, an external APFS
volume, to keep drill I/O off the boot disk.

## Root cause

`tools/disk_pressure_drill.sh` builds its own filesystem to exhaust:

    IMG=$FLINT_DRILL_ROOT/flint-diskpressure.dmg
    MNT=$FLINT_DRILL_ROOT/flint-diskpressure-mnt
    hdiutil create -size 512m -fs HFS+ ... "$IMG"
    hdiutil attach "$IMG" -mountpoint "$MNT" -quiet || fail "could not attach the disk image"

The mountpoint is *inside* `FLINT_DRILL_ROOT`. When that root is itself a
mounted volume, macOS refuses to attach an image beneath it. `create` succeeds,
`attach` fails, and the cleanup trap deletes the image — so the evidence removes
itself and the next reader finds no `.dmg` to inspect.

Isolated, running the drill's own two commands at each root:

    /Volumes/FlintDev/drillscratch     ATTACH FAILED
    /tmp                               ATTACH OK

Re-running the drill alone with the default root: **PASS** — writes shed with a
QUOTA error, reads and the delete path keep working, nothing written is lost,
the node reopens once space returns. The product was never implicated.

## Why this is worth a fix rather than a habit

**#180 made this root configurable *specifically* so the gate could run off a
different volume.** That task is satisfied for every drill except this one,
which cannot work off a mounted volume by construction — it mounts something
there. So the knob has a silent exception, and the exception is invisible until
it fires.

**The message points at the wrong thing.** "could not attach the disk image"
sends the reader to `hdiutil`, to the image, to the 512 MB size, to permissions
— everywhere except the environment variable that actually decided it. The
drill's header already carries a long note about a *different* attach failure
(a stale mount surviving cleanup), which is the first thing a reader will check
and rule out, as happened here.

**A gate that goes red for a reason unrelated to what it tests is the failure
this repo's whole verification discipline exists to prevent.** It is the same
shape as BUG-0009 (an unknown stage passing the gate) inverted: there a green
meant nothing, here a red means nothing.

## A lead on the "intermittent" history

`disk_pressure` has a recorded history of failing and then not reproducing —
filed as flakiness and instrumented for next time. **"Intermittent" plus "fails
only under a non-default root" is the shape of a config-dependent failure
misfiled as flakiness.** If those earlier runs used a non-default
`FLINT_DRILL_ROOT`, it was never intermittent at all.

Not verified — the earlier runs' roots were not recorded, which is itself part
of the problem. Stated as a lead so it is checked rather than rediscovered.

## Fix

Any of these closes it; the first is the smallest:

- **Validate the root up front**, in `gates.sh` or in the drill's preflight:
  if `FLINT_DRILL_ROOT` resolves inside a mount that is not the boot volume,
  fail immediately naming the variable, or skip this one drill with that reason
  stated. A skip that says why is honest; a red that misdirects is not.
- **Decouple the image from the root** — place the `.dmg` and its mountpoint on
  the boot volume unconditionally, since what the drill needs is *a small
  filesystem*, not one on the drill root. The rest of its scratch can stay
  wherever the root points.
- **Record the root in the gate log header** regardless, so a future
  "intermittent" failure can be correlated with it instead of guessed at.

## Related

- #180 — made the drill scratch root configurable; this is its unhandled case
- BUG-0009 — the other direction: a gate result that does not mean what it says
- BUG-0010 — also a drill diagnosing on the wrong signal (`pgrep` matching
  session argv rather than seats)
