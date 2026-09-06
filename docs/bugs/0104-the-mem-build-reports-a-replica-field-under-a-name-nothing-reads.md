# BUG-0104 — the mem build reports its replica field under a name nothing reads (FIXED 2026-09-05)

**Status: FIXED 2026-09-05**, and held by the `flint` family's `FLINTINFO`
case in the conformance corpus. Found 2026-09-05 while writing that case,
because the field it asserted was present on one engine and absent on the
other · Severity: low, and latent — production runs rocks, where the field
was always correct. It was one dev cluster away from presenting as a broken
controller.

## Symptom

`FLINTINFO` against the two builds:

    rocks:  role:master loading:0 role_epoch:none ... live_replicas:0 ...
    mem:    role:master loading:0 live_replica:0

Different name, and different units: the rocks build emits a **count**
(`hub.live_replica_count(now)`), the mem build emitted a **boolean**
(`hub.has_live_replica(now) as u8`).

## Root cause

Two `FLINTINFO` renderers, one per build. The mem-only one
(`crates/flint-server/src/main.rs:5885`) had carried the singular spelling
for as long as it has existed. Nothing read it: the string `live_replica`
without a trailing `s` appeared in exactly one place in the repository — the
line that produced it.

Every consumer reads the plural. `flint-controller` parses
`"live_replicas" => node.live_replicas = v.parse().unwrap_or(0)`, so against
a mem seat it would find no such field and keep the default 0 — and
`promotable()` requires `live_replicas >= 1`. A controller fronting mem seats
would therefore have found **no promotable node, ever**, while every seat was
healthy and every log was quiet.

## Why it never fired

Production is rocks, and the one drill combination that could have reached it
does not exist: of the twelve tools that pass `--engine mem`, only
`gates.sh` also starts a controller, and there the mem seat is the
conformance target rather than a controller-fronted node. So this is written
up as a latent defect with a named blast radius, not as an incident.

## Fix

The mem renderer emits `live_replicas` with the same count the rocks
renderer uses. There is no compatibility cost, because there was no consumer
of the old name to break.

## The check, and what nearly hid it

The corpus case asserts `live_replicas:` by name against both engines, and it
was confirmed to fire: restoring the old spelling fails the case at the step
that asserts it.

Getting there took one wrong reading first. The mem run initially passed
while the binary in `target/release` had been built with `--features rocks`;
a rocks-featured binary started with `--engine mem` still takes the *rocks*
`FLINTINFO` renderer, so the run printed eighty fields and proved nothing
about the code that had just been changed. The mem-only path is only reached
by the mem-only build, so verifying it means building without the feature and
keeping that binary separate. Same family as BUG-0028 (every drill shares
`./target/release`) — the binary on disk is not necessarily the one the
source says.
