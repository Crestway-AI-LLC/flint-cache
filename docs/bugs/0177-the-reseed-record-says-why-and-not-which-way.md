# BUG-0177: the re-seed record says why a copy was marked, and not which way its rejoin went (FIXED)

**Status:** FIXED 2026-09-24 in `flint-server`. It ships with the next release.
The ops side is the operations agent reading the new field (ops OPS-0319).
Found by the operations agent's `recent_reseed` insight firing the morning
after a routine roll, for a seat that had not re-seeded.
**Severity:** low for the server, since nothing it does changed. It matters
because this is the only field that can separate the cheap rejoin from the
expensive one, BUG-0071's full re-seed with the write gates held shut, and it
could not.

## Symptom

The playground's node 7002, over the internal mesh, the day after the rc.77
roll:

    last_reseed_reason:superseded copy rejoining the lineage held by 172.31.64.94:7001
    last_reseed_at_ms:1790191771720

Its own log at that instant:

    rewound to /var/lib/flint/snaps/g0/snap-1790191742174-seq280599726-e0.75 (seq 280599726 <= fence 280600723, epoch (0,75)): tailing incrementally instead of a full re-seed

That was a sub-second local rewind. The operations agent reads only the two
fields, so it raised the same insight it raises for a full re-seed, and kept
it raised for a day.

## Root cause

`flintctl` marks every ex-master before it rejoins a live lineage, correctly.
The marker tells the server that this copy cannot be trusted blindly. The
server records the marker's reason at startup (BUG-0082) and then decides
between four outcomes:
- verify the copy as-is (warm);
- rewind to a local snapshot;
- discard and full-sync;
- or, with no upstream at all, simply clear the marker.

It reported the reason and not the decision. A promotion also clears a left-over marker,
so a freshly promoted master reported a "re-seed reason" too, having re-seeded
nothing.

## Fix

FLINTINFO gains `last_reseed_path`, set where each decision is made:

| value | meaning |
|---|---|
| `warm` | the marked copy was verified against the master as-is; nothing moved |
| `rewound` | restored from a local snapshot the master vouched for |
| `full_sync` | discarded and re-seeded from a checkpoint over the wire |
| `lineage` | no upstream to rejoin (a start with no `--replica-of`, or a promotion); the marker was only cleared |

The field follows BUG-0082's rules. It is absent until the rejoin decides, so
a field present means it happened. It is reset whenever a new reason is
recorded, because a new marker is a new episode and the last one's path must
not be read beside it. A promotion records `lineage` only when it actually
cleared a marker (`clear_needs_reseed` now says whether it did); otherwise it
would relabel an earlier episode it never saw.

It is additive. An older agent ignores an unknown field, and a newer agent
reading an older server sees no path and keeps the old behaviour.

## Verification

Unit tests in the `reseed` test module:
- the path is absent until the rejoin decides;
- each of the four values is reported on its own line after the reason;
- a new reason forgets the previous episode's path;
- `clear_needs_reseed` returns false when there was nothing to clear.

The rejoin block is unreachable from a unit test, so its wiring is asserted
from the source, BUG-0082's weak instrument. The source is searched in
production code only, cut at the first test module, because
`include_str!` carries the test's own literals. The checks are:
- `warm` is recorded after `warm = true`;
- `rewound` is recorded after `try_rewind`;
- `full_sync` is recorded inside `if !rewound`, before the copy is discarded;
- `lineage` appears exactly twice, the promotion's guarded by the cleared
  result.

## Related

- [BUG-0082](0082-the-rejoin-adopts-a-cursor-the-archive-can-no-longer-serve.md)
  added the reason, and this adds what it could not say.
- [BUG-0071](0071-a-full-re-seed-on-rejoin-holds-writes-shut-for-90s.md) is
  the cost `full_sync` identifies.
- Ops [OPS-0319](../../../flint-cache/docs/bugs) is the agent's side.
