# BUG-0167: the control-plane state floor is enforced on the roll path nobody is told to use, and not on the one the release notes give them (FIXED 2026-09-19)

Status: **FIXED 2026-09-19**, found 2026-09-18 while writing OPS-0265 · Severity: **low
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

## The negative claim, with its own reproduction

"Nothing in that path checks the format" is the dangerous kind of claim: a
negative nobody disproves. So it is reproducible here rather than asserted.

    git show origin/main:crates/flint-ctl/src/main.rs > /tmp/ctlmain.rs
    grep -nE "rc\.73|STATE_FLOOR|state_format|ALLOW_CP_FORMAT|cp-state|format_break" /tmp/ctlmain.rs

Twenty hits, and every one of them is something else:

- **`cp-state` (13 hits) is about PATH SPELLING, not content** — a lone seat
  uses `<statedir>/cp-state` and a Raft group `<statedir>/cp-state-n<i>`, which
  is what `cp_seat_state` exists to get right. None of them opens the file.
- **`format_break` (3) is the manifest refusal** at 9096/9104 plus one doc
  comment.
- **`rc.73` (1)** is a comment about where seats were put during that roll.
- **`STATE_FLOOR`, `state_format`, `ALLOW_CP_FORMAT`: zero.**

Independently re-derived by the peer session against the same file, which is
why it is written down once here instead of twice in two transcripts.

**One thing that grep makes easier rather than harder.** `cp_seat_state`
already computes the control plane's state path, correctly for both the lone
and Raft spellings, and `upgrade` already runs where that file is. So fix (1)
below needs no new path logic and no ssh — it needs a read, a first byte, and a
version comparison.

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

## Fixed 2026-09-19

`flintctl upgrade` now refuses to put a control plane in front of a state file
it may not be able to read, before a seat is touched and before the roll record
exists — a refusal has to leave the fleet and the journal exactly as they were.

**The cost sits on the dangerous path only.** A fleet whose state file is still
the line format reads one byte and returns. The check engages only once a
writer-carrying build has committed, and only for a single-node control plane,
because `raft.rs` never references `state::State`.

### Which candidate, and a correction to this file's own reasoning

The filing called (2) — make the state file self-describing so the roller needs
no version constant — the one that stops this recurring. **That was wrong about
this hazard, and the correction is the useful part.** The thing being guarded
against is an OLD binary. Nothing added to the state file changes what rc.72
does with it; that binary will never look. A self-describing file only moves
where the REFUSER learns the requirement, and every file already migrated by
rc.74 lacks the field anyway.

Generally: **a compatibility floor about older binaries cannot be derived from
those binaries. It has to be asserted from outside them.**

### What can be derived, and now is

The version comparison was the whole check for about an hour, and running the
drills killed it. A control plane built from source reports the crate version
`0.0.1`, which cannot be ordered against `v0.1.0-rc.73` at all — so it read as
"cannot tell", and the guard refused. **Every developer build and every drill
would have been blocked**; `upgrade_drill.sh` failed on exactly that, which is
the whole reason the drills get run before the push.

So the binary answers for itself. `flint-controlplane --state-formats` prints
`line json`, and the guard asks that FIRST:

- it answers and lists `json` → proceed, whatever its version says;
- it answers and does not → refuse, on its own word, whatever its version says;
- it does not answer → fall back to the version against the floor.

That ordering is what makes the constant shrink rather than spread. The
fallback governs only binaries that predate the flag — which are exactly the
ones whose release tags parse — and **the next format change needs no new
constant anywhere**, which is what candidate (2) was reaching for and could not
get by itself.

### The refusal, and the way out

`FLINT_ROLL_ALLOW_CP_FORMAT=1` overrides it and prints what is being accepted,
the same name and meaning as the `roll-fleet.sh` guard, so an operator learns
one thing rather than two. The refusal names the file, what the staged binary
said about itself, the floor, and the fact that the older binary does not
reject the file but empties it.

### Tested

Thirteen unit tests over the decision matrix and its two inputs, and the
decision is split out from the action precisely so the matrix can be exercised
— the action ends in `process::exit`, and a decision nobody can run is how a
guard ends up refusing the wrong half. Seven mutations, each killed by the test
written for it: the comparison made lexical, an unorderable build treated as
safe, an older build allowed through, a missing build version treated as safe,
a missing state file read as not-JSON, the floor moved, and the guard made
over-broad so a line-format file is refused too.

Six drills that drive `upgrade` were run before the push — `upgrade`,
`cpha_roll`, `edge_roll`, `roll_shed`, `promote_notice`, `build_read_failure`,
`admin_gated_proxy` — which is the check that found the over-refusal above.

## What is still open

**The three copies of the floor.** The runbook, `roll-fleet.sh` and
`CP_STATE_FLOOR` all name rc.73. They cannot be collapsed for the reason above,
so the ops gate asserts they AGREE instead.

## Candidate fixes as filed, kept for the reasoning

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
