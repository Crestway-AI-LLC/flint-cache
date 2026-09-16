# BUG-0154: `next-free-ports.sh` suggests ports the gate's OTHER collision check rejects (OPEN)

Status: **OPEN**, found 2026-09-15 · Severity: **low** — it costs a gate cycle,
not correctness. It is filed because the helper's own file says, in its opening
comment, that this must not happen.

## The contract, written down and then half-kept

`tools/lib/drill-ports.sh` opens with:

> Two things need this answer and must never disagree: the gate's
> `assert_no_duplicate_drill_ports`, which refuses a collision, and
> `tools/next-free-ports.sh`, which suggests where to put a new drill. **A
> helper that suggested a port the gate then rejected would be worse than no
> helper**, so they read the same function rather than each carrying a copy.

And `next-free-ports.sh` repeats it at the point of decision:

> SELF-CHECK: prove the answer against the same data the gate will use, rather
> than trusting the loop above.

The gate has **two** port-collision checks. The allocator consults one.

## What happened

Adding `spawn_duplicate_drill.sh` on 2026-09-15, `tools/next-free-ports.sh 2`
answered `6442 6443`. The gate then refused the tree:

    GATES FAILED: a drill's kill pattern reaches another drill's ports:
        controller: pattern '--port 644' also matches port 6442, declared by spawn_duplicate

`controller_drill.sh` carries `pkill -9 -f "flint-server --port 644"`, a
truncated port, and `assert_no_cross_drill_kill_patterns` refuses any declared
port that such a pattern is a prefix of. That check is right — under a parallel
gate the controller drill would SIGKILL the new drill's seat — and the allocator
knows nothing about it.

Truncated patterns in `tools/` today are `640 644 648 65 653 657 658 66 670 673
675 67`, so the allocator can hand out a doomed port anywhere in 640x, 644x,
648x, 65xx, 66xx or 67xx. Its default base is 6300, which walks straight into
that band: the first free run it found was inside it.

## A second copy, already drifted

`assert_no_cross_drill_kill_patterns` does not use `drill_declared_ports`
either. It rebuilds the port map inline, and the two differ already:

- the shared helper anchors on `^[[:space:]]*fleet_init`, after the
  BUG-noted case where an unanchored match read `assert_no_default_ports`' own
  regex as a declaration of 7001;
- the inline copy is unanchored, and does not scan `gates.sh`, whose
  conformance stage claims ports exactly as a drill does — which is the reason
  the shared helper includes it.

So the kill-pattern check is asking its question against a port map that is both
looser and smaller than the one the duplicate check uses.

## Fix

One authority for each of the two populations, used by both consumers:

- add `drill_kill_prefixes [dir]` to `tools/lib/drill-ports.sh`, emitting
  `<prefix> <drill>` with the same extraction the gate does today (non-comment
  lines only — two drills quote the pattern they used to have, inside the
  comment explaining why it was wrong);
- have `assert_no_cross_drill_kill_patterns` take both its populations from the
  library: `drill_declared_ports` for the map and `drill_kill_prefixes` for the
  patterns;
- have `next-free-ports.sh` skip any candidate a prefix from ANOTHER drill would
  match, inside the self-check that already claims to prove the answer against
  the gate's data.

**And give that check a coverage refusal.** Its siblings in `gates.sh` all
refuse when they examined nothing — *"a glob that stops matching certifies every
file by reading none of them"* — and this one does not. A pattern extractor that
silently stopped matching would leave the check green forever, which is the
failure class this repo files most often.

## Not established

Whether any of the twelve truncated patterns should simply be widened to full
ports instead, which would shrink the excluded band to nothing. That is a change
to twelve drills' teardown and belongs to whoever owns them; the allocator has
to cope with the tree as it is either way.
