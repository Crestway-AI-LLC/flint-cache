# BUG-0086: the port check compares declarations to each other, and never to use

Status: the collision it missed is FIXED; the check gap is CLOSED 2026-09-02
Severity: low today (the gate runs drills sequentially), and it is a check that
cannot see the thing it exists to prevent

## What was found

`cp_watch_idle_drill.sh` declared ports 6521-6523. `controller_multipair_drill.sh`
runs three pairs, the third of which is `(6520 6521)`, and names both in its
`--manage-pairs` string — so 6521 is a live seat there. Its `fleet_init`
declared `6500 6501 6510 6511 6520`. **Not 6521.**

Two drills therefore shared a port, and nothing said so, because
`assert_no_port_overlap` reads this and only this:

    grep -h '^fleet_init' tools/*_drill.sh | awk '{for (i=3; i<=NF; i++) print $i}' | sort -n | uniq -d

It compares **declarations against each other**. A port a drill uses without
declaring is invisible to it — and an undeclared port is exactly the case where
the declaration is wrong, which is the case the check exists to catch.

`assert_no_duplicate_drill_ports` is the second port check and reads the same
source: `drill_declared_ports` in `tools/lib/drill-ports.sh`. Two checks, one
question. And `assert_spawning_drills_declare_ports` asks only whether a drill
that spawns Flint processes has a `fleet_init` line **at all** — not whether the
line is complete, which is the failure here.

**The file already knows how to do this properly, one dimension over.**
`assert_no_used_path_overlap` scans drills for `$FLINT_DRILL_ROOT/<name>`
assignments — what they *use* — rather than for what they declare, and it even
carries an ARMED guard that fails when the pattern matches nothing, "the exact
way a duplicate-port check in this suite passed vacuously for as long as it
existed". Paths got the treatment. Ports never did.

`fleet.sh` is what makes the declaration load-bearing: `_fleet_ours` takes a pid
if the `ps` line carries the scope dir *or* a declared port. Here the scope dir
(`flint-mp-`) still claimed the seat, which is why nothing has broken; drop that
second half of the disjunction and an undeclared port is a seat the drill cannot
recognise as its own.

## Fixed

`controller_multipair_drill.sh` now declares 6521, and `cp_watch_idle_drill.sh`
moved to a free block, 6603-6607. Both were needed: declaring 6521 without
moving would have made `assert_no_port_overlap` red, correctly.

## The check gap, and why it is not closed here

The check that would have caught this asks whether every port a drill *uses*
appears in its `fleet_init`. Measured across 129 drills, scanning only the
unambiguous `127.0.0.1:PORT` form, six use a port they never declare:

| drill | declared | undeclared |
|---|---|---|
| `backup_drill.sh` | 6950-6954 | 6999 |
| `build_stamp_drill.sh` | 7411-7414 | 7001, 7500 |
| `config_drift_drill.sh` | 7421-7424 | 7999 |
| `controller_ha_drill.sh` | 6450-6452 | 64512 |
| `fleet_guard_drill.sh` | 6378-6385, 6999 | 7788, 7789 |
| `txn_failure_drill.sh` | 6960-6962 | 6999 |

Six of 129 looked like a backfill. It is not one — see the correction below,
which reads all six. And **the scan that produced this table would not have
caught the bug this file is about** either: `controller_multipair` names
6521 in a bash array and in a `6521:$DIR` string, never as `127.0.0.1:6521`, so
a `127.0.0.1:` scan misses it. A looser scan — every 4-digit literal — catches
it and also catches ports named in comments and in deliberately-unused
positions, several of which appear above: `6999` looks like a port chosen
*because* nothing listens on it.

So the honest state is: the check is worth having, the naive version has false
positives that need judging one at a time, and shipping it with an exclusion
list would reproduce the shape of the defect — a list that says "these are
fine" is a second declaration to keep in sync with use.

Left open with the measurement recorded, so the next attempt starts from six
named cases rather than from scratch.

## CORRECTED: all six candidates are right as they stand

The table above was written from a scan, not from reading the six drills. Read
individually, **every one of them is correct**, and the check that would have
flagged them would have been wrong six times out of six:

| drill | port | what it actually is |
|---|---|---|
| `backup_drill.sh` | 6999 | `FLINTSLOTFREEZE "$FSLOT" 127.0.0.1:6999` — a freeze *destination*. Nothing binds it. |
| `txn_failure_drill.sh` | 6999 | the same call, same reason |
| `build_stamp_drill.sh` | 7001, 7500 | inside a **comment** showing example output |
| `controller_ha_drill.sh` | 64512 | inside a garbled interleaved log sample in a comment; not a port at all |
| `config_drift_drill.sh` | 7999 | a CP address that **must be unreachable**: *"with no CP reachable it MUST read null … prove the null branch is reachable by taking the CP away"*. Declaring it would reserve a port whose whole job is to refuse. |
| `fleet_guard_drill.sh` | 7788, 7789 | a **simulated peer drill's** ports, written into `$PEER.lock/ports` so the guard has someone else's lock to find. Declaring them would claim ownership of the very thing being pretended to belong to another drill. |

Zero true positives. So the finding is not "six rows to backfill" — it is that
**a port literal is not evidence of ownership**, and the four things a port can
be in a drill are not distinguishable by scanning for it:

- a port the drill **binds** — must be declared;
- a destination that must be **unreachable** — must NOT be declared, since
  declaring reserves it;
- a port standing in for **another** drill — must not be declared;
- a number in a comment.

## Why the path check ported and this one does not

`assert_no_used_path_overlap` works because `NAME=$FLINT_DRILL_ROOT/<x>` is
*always* ownership: there is no idiom in this suite for naming a directory you
require to be absent, or one you are pretending belongs to someone else. Ports
have both idioms, in active use, as positive controls. The asymmetry is in the
subject, not in the effort spent.

## What is actually constructible

A narrower check: scan the **arguments that bind**, not port literals.
`controller_multipair`'s 6521 appears in `--manage-pairs`, which is a binding
argument, and none of the six false positives appear in one — `fleet_guard`'s
7788/7789 are in `--pairs`, which is a *connect* argument, and that distinction
is the whole of it.

That is a rule about two flags rather than a general property, and it would
have caught this bug. Whether it generalises past the flags that exist today is
unproven, and claiming otherwise is what the first version of this section did.

## BUILT 2026-09-02

`assert_no_duplicate_drill_ports` already carried the "every port used in code
must be declared" half; it scanned `--port N`, `-p N` and `127.0.0.1:N`. Two
binding forms it could not see are now scanned with it — one check, one
question, one failure message, rather than the second check this file warns
about.

**The supervising flags.** `--manage-pairs` / `--manage-slots` take `PORT:DIR`
specs. `build_pairs` turns them into `Pair::managed`, and a managed pair is
supervised: `spawn_slot` runs `flint-server --port <port>` and respawns it. So
those ports are bound by the drill's own process tree. `--pairs` and `--nodes`
take the same shape and build `Pair::decision`, which only dials seats someone
else runs — they stay unscanned, which is precisely why `fleet_guard`'s
fake-peer `7788`/`7789` do not trip this. The bind/connect distinction the
section above proposed survived contact with the source, and it is a property
of the flag rather than of the spelling.

**The chaos base.** `--port-base N` claims `N .. N+SPAN-1` — master, replica,
proxy, and the replacement-replica pool. Only `N` is written in the drill, so
before this a drill declaring just its base read as fully declared while
binding seven more. `SPAN` is READ from `crates/flint-chaos/src/cluster.rs`,
never restated: that file calls it "a contract with the drills, so it lives
here rather than being spelled out in seven shell scripts", and copying the 8
into the gate would make the gate the eighth place to keep in sync — this
file's own defect, one dimension over.

**No exclusion list was needed**, which was the objection that kept this
unbuilt. Not because the false positives were argued away but because none of
them appear in a binding argument: the freeze destinations and the unreachable
CP are `127.0.0.1:PORT` operands, the fake peer's are in `--pairs`, and two
were comments. `DRILL_DEAD_PORTS` already existed in `tools/lib/drill-ports.sh`
for an independent reason — the allocator must never hand those out — so
nothing new had to be declared to make the check quiet.

### Verified by mutation, five ways

A check that passes on a clean tree has demonstrated nothing, which is the
whole subject of this file. Each mutation was confirmed to have changed bytes
before its result was read:

| mutation | result |
|---|---|
| 6521 removed from `controller_multipair`'s `fleet_init` — **this bug, exactly** | FAILS, naming 6521 |
| 6500 removed — the FIRST token of the spec | FAILS, naming 6500 |
| 6870 removed — first token, other drill | FAILS, naming 6870 |
| `chaos_drill` declares `6330 6331 6332` instead of the full block | FAILS, naming 6333-6337 |
| `SPAN` renamed to `PORT_SPAN` in the crate | FAILS, saying it cannot read the constant |

The first-token cases are in the table because the first version of the scan
dropped every one of them and still reported the tree clean. It used BSD
`sed`'s `\?`, which is a GNU extension: the substitution silently did nothing,
the extraction returned the flag name instead of the ports, and the comparison
passed. On the Linux gate box the same line would have worked — a check that
behaves differently on the machine where it matters is the rc.15 bug class, and
it appeared here while building the check meant to prevent its cousin. The
shipped version does the work in `grep -oE` and `seq`, which agree on both.

The last row is the tri-state one. If the constant cannot be read, the two
available ways to be silent are both wrong: defaulting to 8 keeps asserting
against a number the source may have changed, and skipping the expansion passes
every chaos drill without examining it. It fails instead.

## The shape

Not "nobody thought to write it" — that was the first draft of this section and
it is wrong. The check exists, for paths, in the same file, with the anti-vacuum
guard and the comment explaining why the guard is there. What did not happen is
carrying it across to the dimension it was originally about.

That makes this the same shape as `assert_no_scope_overlap`, which exists
because `assert_no_port_overlap` "was read as covering ownership; it never
looked at $2" — one level down again: it was read as covering ports, and it
only ever looked at what the drills *said* about ports. Three of these landed in
two days, and in every one the correct instinct had already been written down
somewhere adjacent.
