# BUG-0019: `FLINT_DRILL_ROOT` on a mounted volume breaks `disk_pressure`, and the error blames the image (FIXED)

Status: **FIXED** 2026-08-18, found the same day running the gate for the
BUG-0017 fix · Severity was medium — the gate reported a red that was not the
product's, and its message pointed away from the cause

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

## Fix — applied

`tools/disk_pressure_drill.sh` now places the image and its mountpoint on the
volume that accepts an attach, and leaves everything else on the drill root.

The choice was between refusing and removing the exception. Refusing is honest
but leaves #180 with a permanent carve-out; removing it is possible because what
this drill needs is *a* small filesystem, not one on the drill root. So the
scope, lock and seat log still follow `FLINT_DRILL_ROOT` and only the ~512 MB
image relocates — which is the price of the drill running at all.

The predicate compares **devices**, not mount points, against the fallback:
on APFS the sealed system volume and the data volume have different mount
points, so testing against `/` would have relocated even the default root.
Darwin only — the Linux loop-mount path has no such restriction and is
untouched. The header now prints the image root, so a future "intermittent"
failure can be correlated with it instead of guessed at.

**Both controls run, which is what makes this a fix rather than a hope:**

- *Positive* — the exact configuration that failed
  (`FLINT_DRILL_ROOT=/Volumes/FlintDev/drillscratch`) now prints
  `image + mountpoint go to ... instead (BUG-0019)` and reaches
  `PASS: disk pressure`.
- *Negative* — at the default root the relocation does **not** fire
  (`image root /tmp`) and the drill passes unchanged, so the fix cannot be
  passing by relocating unconditionally.

## Related

- #180 — made the drill scratch root configurable; this is its unhandled case
- BUG-0009 — the other direction: a gate result that does not mean what it says
- BUG-0010 — also a drill diagnosing on the wrong signal (`pgrep` matching
  session argv rather than seats)
