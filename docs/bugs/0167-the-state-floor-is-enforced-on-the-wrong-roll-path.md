# BUG-0167: the control-plane state floor is enforced on the roll path nobody is told to use, and not on the one the release notes give them (OPEN)

Status: **OPEN**, found 2026-09-18 while writing OPS-0265 · Severity: **low
until a fleet rolls back two steps, high the once it does** — the outcome is a
control plane that comes up empty and overwrites the real registry, not an
error.

## What is enforced, and where

ADR-0032 step 2's writer makes the single-node control plane persist its
registry as JSON. A binary older than **v0.1.0-rc.73** has only the line
parser, and that parser does not fail on JSON: its outer `match` ends in
`_ => {}`, so every line falls through, the load succeeds with no proxies, no
pairs and no tenants, and the next `commit()` writes that back over the real
registry. Measured on v0.1.0-rc.72.

**OPS-0265** put a guard in the ops repo's `packaging/aws/roll-fleet.sh`: below
the floor it reads the control plane's state file and refuses on JSON, or on
not being able to tell. That is real and it covers the playground, which is
rolled that way.

## What is not enforced

**`flintctl upgrade` has no such check, and it is the documented path.** Every
release's notes say, verbatim:

> unpack into the inventory's bins dir, then `flintctl -f <inventory> upgrade
> --manifest manifest.json --version-tag <tag>`

`packaging/aws/ctl.sh` takes the same route on the box
(`ctl.sh upgrade --version-tag v0.1.0-rc.38 --soak-ms 5000`).

Measured, not assumed:

- `upgrade` rolls the control plane — `crates/flint-ctl/src/main.rs` spawns
  `flint-controlplane` in that path, and `inv.cp` is one of the five seat kinds
  it rolls.
- It is explicitly built to go backwards. Its own comment on the
  `FLINT_BUILD_VERSION` injection says *"Rolling ONTO an older build still
  needs it, so it stays until the fleet's floor is past the change."*
- Nothing in that path reads the state file or compares a version to a floor.

So after the first release carrying the writer, `push-bins <rc.72 bundle>`
followed by `upgrade --version-tag v0.1.0-rc.72` puts a pre-reader control
plane in front of a JSON state file with nothing objecting.

## Why `format_break` does not already cover this

It is the obvious candidate and it does not fit. `upgrade --manifest` reads
`format_break` out of the manifest and refuses unless `--allow-format-break` is
passed, saying the release *"cannot roll back and must ship via the migration
runbook"*. Two reasons that is the wrong instrument here:

1. **It gates rolling FORWARD onto the release, not backward off it.** The
   hazard is the rollback.
2. **It is binary and this constraint is not.** Rolling back from the writer's
   release to rc.73 is SAFE — rc.73 carries the tolerant reader, which is the
   entire point of splitting ADR-0032 across two releases. Only going below
   rc.73 destroys anything. `format_break` can only say "irreversible", so
   declaring it would force `--allow-format-break` on ordinary rolls and train
   operators to pass it, which is worse than not having it.

## Candidate fixes, none chosen

1. **Port the OPS-0265 check into `upgrade`.** flintctl already runs where the
   file is, so it needs no ssh: read the CP's `--state` file, and refuse to
   roll the CP onto a target below the floor when it is JSON. Smallest change,
   and it puts the check on the path operators are actually told to use.
   Against: a floor constant in flintctl is a third copy of "rc.73" (the
   runbook and roll-fleet.sh have the other two), and a constant that must be
   updated by hand is the thing every stale-number bug here starts as.
2. **Make the state file say what it is** and have the roller refuse a target
   that cannot read that format. Removes the version constant entirely and
   generalises to the next format change. This is NEW DESIGN and needs a
   decision, not a patch.
3. **Extend `format_break` into a floor** — a manifest field naming the oldest
   release that can read what this one writes, rather than a boolean. Honest
   about the real shape, and touches the release pipeline.

(2) is the one that stops this recurring; (1) is what closes the hole this
week. They are not exclusive.

## What is NOT claimed

**Not that the ops guard is wrong or wasted.** It covers the playground, which
is the fleet that exists, and it is where a rollback is actually performed
today.

**Not that this has ever fired.** The hazard needs a deliberate rollback of
more than one release, and after OPS-0264 the runbook says not to do that. What
is established is that nothing would stop it.
