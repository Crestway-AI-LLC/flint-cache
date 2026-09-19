# BUG-0163: rewind snapshots are never pruned (FIXED 2026-09-19)

Status: **FIXED 2026-09-19.** Filed OPEN 2026-09-17 with the rule proposed and
deliberately unimplemented; Jeff took the decision on 2026-09-19 and it is
implemented below. · Severity:
**medium, and certain.** Nothing is broken today and about 100 days of headroom
remain. The growth is unbounded and nothing anywhere deletes a snapshot, so the
end state is not in doubt — only the date.

## Measured on the playground, 2026-09-17

| | |
|---|---|
| entries in `/var/lib/flint/snaps/g0` | **158,443** |
| of those, quarantined `unresumable-*` | **86,193** (54%) |
| live rewind candidates `snap-*` | 72,249 |
| bytes | **138 GB**, of 142 GB used on the volume — **97%** |
| oldest entry | **2026-07-24 19:34** |
| growth | one snapshot per 30s, about 2.5 GB/day |
| headroom | 294 GB free; the disk guard's 10% floor is about 100 days out |

The instance was launched at 2026-07-24T19:33. **The oldest snapshot is from the
hour the box was born**, which is the whole finding: nothing has ever removed
one. A search across `crates/` for any `remove_file` or `remove_dir_all` touching
snapshots returns nothing, and neither does one across the ops packaging.

Inodes are not the constraint: 1,399,601 used of 228,515,584. Bytes are.

One operational consequence already: the directory can no longer be globbed.
`ls -1d snap-*` in it fails with `Argument list too long`, so any cleanup has to
walk with `find`, not a shell glob.

## The obvious fix is wrong, which is why this is open

More than half the pile is `unresumable-*`, renamed by `quarantine_unresumable`
onto snapshots "that can never again be resumed from". Deleting those looks like
74 GB of free win and it would remove recovery options. `try_rewind` carries them
ON PURPOSE:

    // The third element is the cursor a quarantined snapshot was disqualified
    // against, or None for an ordinary candidate. Quarantined ones are carried
    // here rather than filtered out so the fence can RECONSIDER them; the
    // condition that permits it is applied below, not here.

BUG-0071 sets the condition: a quarantined snapshot is re-admitted when the
master's fence for its epoch now sits BELOW the cursor it was disqualified
against, and is then used if its own seq is at or below that fence. Both halves
depend on what the master answers at the time, so no snapshot can be declared
permanently useless by reading its name.

## What a safe rule would rest on, if we want one

A rewind is only worth anything if the node can then TAIL FORWARD from the
restored snapshot. That tail comes from the master's WAL archive, which is
pruned on a TTL — measured at about 12 hours on this box (`oldest 43190s`). A
snapshot older than the archive's reach therefore cannot produce a successful
rewind whatever its epoch: restoring to it lands directly in the gap that
BUG-0162 is about.

That gives a retention rule derived from a real quantity rather than picked:
**keep snapshots covering the WAL archive's reach, plus a margin, and read the
reach at runtime rather than hardcoding it.** Twelve hours at one per 30s is
about 1,440 snapshots; even a generous margin leaves a ~98% reduction against
the 158,443 held.

It was not implemented when this was filed. It deletes recovery state
irreversibly on the strength of a rule evaluated at runtime, and a mistake in it
removes options during exactly the incident the snapshots exist for. That is a
decision to take deliberately, not inside a bug fix, and the ~100 days of
headroom meant nothing was forced.

## Implemented 2026-09-19, on Jeff's decision

`prune_snapshots` in `crates/flint-server/src/main.rs`, called from
`FLINTSNAPSHOT` after the `LATEST` repoint — so a fresh snapshot exists and
`LATEST` already names it before anything old is removed.

**Cutoff: `max(2 x observed reach, 24h)`.** The reach comes from
`Store::archive_span()`, which reads THIS node's archive; the tail a rewind
needs comes from the master's. Same TTL policy, so it is a fair proxy — the
factor of two is what pays for it being only a proxy.

**It deletes by AGE, never by NAME, and that is the whole reason it is safe.**
The section above is the argument against the tempting fix, and it still holds:
nothing here reads `unresumable-` and concludes anything. A quarantined snapshot
inside the reach survives; one past it does not, on the same test as any other.

**Every unknown keeps.** Unreadable archive directory, archive with no segments,
no readable mtime, unreadable snapshot root, an entry whose own mtime will not
read — each ends in "prune nothing" or "skip this entry". The asymmetry is
priced: keeping too much costs disk, with ~100 days of headroom measured;
deleting too much costs recovery options during the incident these exist for.

**Two floors, because the age rule alone is not enough.** A 24h minimum, so a
short archive — a fresh box, an idle one, a just-rotated directory — cannot
authorise a deep prune. And a count floor of 2,880 kept regardless of age,
which covers what the age rule cannot: a clock stepping FORWARD makes every
entry look ancient at once, and age alone would empty the directory in one pass.

**Nine tests, four mutations, each killed by exactly one.** Removing the
unreadable-archive guard, the `LATEST` exemption, the by-age (not by-name)
match, or the count floor each fails precisely its own test and leaves the other
sixteen in the module green.

Expected effect on the playground: 158,443 entries against a 24h cutoff at one
per 30s leaves order 2,880 — the ~98% reduction this section predicted.

## Checked and ruled out: this is not a failover-latency problem

`try_rewind`'s own header records a decision window of 7.6s within a 10.9s
budget-breaching outage, and names snapshot enumeration and a FLINTFENCE round
trip per distinct epoch among its four costs. That invites the conclusion that
158,443 snapshots lengthen failover. Measured, it does not:

- **18 distinct epochs**, 0.39 through 0.71 — so at most 18 round trips, not one
  per snapshot;
- enumerating the whole directory takes **0.110s** warm;
- candidates are sorted by sequence descending and the loop stops at the first
  one that clears the fence, so a healthy rewind makes about one round trip.

The 7.6s window is real and belongs to something else. It is recorded here only
so the next reader does not re-derive the same wrong link.
