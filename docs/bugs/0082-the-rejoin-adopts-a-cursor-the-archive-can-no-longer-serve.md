# BUG-0082 — the rejoin adopts a cursor the archive can no longer serve (OPEN)

**Found** 2026-09-01 on the playground, by reading the operations agent's own
repair history rather than by a load run. The agent has executed **16
`AttachReplica` repairs in three weeks**, every one of them a
`flintctl restart-node`, and this is what it was repairing.

Status: OPEN · Severity: medium-high — a pair drops to one copy on every
rejoin that takes this path, and it does not self-heal

## What happens

A seat is demoted, restarts as a replica, rewinds past the promotion fence,
adopts the master's timeline, and then dies before serving anything:

```
demoted to replica at role epoch (0,59) (fenced; wipe + --replica-of to resync)
=== flintctl start node-7001 at_ms=1788226503028 ... ===
marked copy refused by 172.31.64.94:7002 (refused: WALGAP promotion fence history
  for epoch (0,59) is incomplete on this node: cannot vouch for cursor 153926303);
  trying a rewind
rewind: candidate snap-1788226491307-seq168379074-e0.58 clears the fence for epoch (0,58)
rewound to ... : tailing incrementally instead of a full re-seed
replicating from 172.31.64.94:7002 starting at seq 168379074 (epoch (0,58))
adopted the master's translated cursor 168623419: tailing its sequence space now
adopted the master's role epoch (0,60): this copy is on its timeline now
FATAL: WALGAP full sync required: IO error: No such file or directory: while stat
  a file for size: /var/lib/flint/node-7002/archive/140130.log — this link can
  never resume. Marking for re-seed and exiting; the next start will full-sync
  from a checkpoint.
```

It then disqualifies every local snapshot it might have rewound to:

```
quarantine: 4957 snapshot(s) at or below seq 170369842 disqualified;
            the next start has no rewind candidate to lose this race with
```

## This is NOT BUG-0079, and the difference is the whole finding

The FATAL line is character-for-character the one in BUG-0079, so the
temptation is to file it there. The cause cannot be the same:

| | BUG-0079 | this |
|---|---|---|
| load | 2 TB ingest, 150–200 MB/s | playground soak, **50–116 ops/s** |
| when | ~15 min into a sustained fill, 163–165 GB in | **8–14 log lines after start-up** |
| archive at the time | 8 GiB limit reached in minutes | **278 MB, holding a full 12 h** |

**The sharp discriminator is not the workload — it is WHICH RETENTION TERM
PRUNED.** RocksDB applies whichever of TTL or size trips first
(`rocks.rs:347`), and `rocks.rs:348` records which one fired in 0079:

> raising only the TTL would have left the 1 GiB limit doing the pruning —
> which is the term that actually fired in the incident.

0079 is the SIZE budget going under ingest. Here the size bound is ~30x away —
278 MB against 8 GiB — and only the TTL is in play. Same FATAL string, a
different pruner. That is two bugs, and the throughput figures above are
corroboration rather than the argument.

(Credit where due: this discriminator, and the correction in the next section,
came from the session that owns ADR-0022.)

**Measured across every occurrence, which is what settles it.** 27 FATALs on
this pair (18 on `node-7001`, 9 on `node-7002`), each naming a different
missing segment, and **every single one lands 8–14 lines after a
`flintctl start`.** Not one occurred during steady operation. Four of the six
`adopted the master's translated cursor` lines in the log are immediately
followed by the FATAL.

So the segment is not being consumed away by writes. The rejoin is asking for
one that is already outside the window, at start-up, deterministically.

## The likely cause is the translation, not retention

An earlier draft of this file said `min_acked_live` "pins retention to the
slowest live replica". **That is wrong and the correction matters.** Retention
is `open_with_retention(path, wal_ttl, wal_mb)` — a TTL and a byte budget
derived from the volume — and **no replica cursor reaches it at any point**.
`min_acked_live` feeds only the SHED GATE, which protects the archive
indirectly: a master that outruns its replica meets backpressure instead of
deleting the segment out from under it (`rocks.rs:343`).

With that corrected, the arithmetic points one way. For retention to be the
cause, the archive would have to stop holding a segment the replica needs
**within seconds** of it going down. Neither term does that here: the TTL is
12 hours against a seconds-long outage, and the size bound is ~30x away. A
correctly-translated cursor, after a seat has been gone for seconds, should sit
seconds deep inside a twelve-hour window. This one is asking for something
outside it.

**So the likelier reading is that the cursor is wrong, not that the window is
too small.**

There IS a real hole in the neighbourhood and it deserves its own note rather
than being folded in here: `min_acked_live` filters to replicas seen within
`LIVENESS_WINDOW_MS`, so a seat that has EXITED stops feeding even the shed
gate, and `--widowed-grace-ms` governs writes rather than retention. BUG-0012
("WAL retention ignores replica progress", FIXED 2026-08-21) closed the
*lagging live* case; the *absent* one is open. It is just not what produced the
27 failures below.

## What is NOT established, and the measurement that would settle it

Untraced: why the cursor adopted after a fence rewind (`168623419` here) lands
outside a window holding twelve hours of a 50–116 ops/s workload.

**Log the AGE of the adopted cursor and the AGE of the oldest retained
segment** at the moment of the FATAL — not the distance between them. Distance
in sequences cannot separate the two candidates: under TTL pruning a distance
can look entirely plausible while the age is absurd. If a seconds-long outage
adopts a cursor that is hours old, translation is settled on the spot and no
retention change is needed.

That is a two-line change at the FATAL site and it decides which half of the
system is at fault, so it is worth doing before anything is fixed.

## Why it has been invisible

Three things hide it, and all three were true on the playground today:

1. **It self-heals on the next start** — "the next start will full-sync from a
   checkpoint" — so a fleet with a working supervisor shows a brief blip.
2. **Nothing restarts it** (BUG-0077, FIXED 2026-08-30 by
   `flint-supervise.service`). On the playground that unit had been dead for
   19 hours on a mode bit, so nothing did, and the pair stayed single-copy
   until the operations agent restarted the seat at 15:36Z.
3. **The agent repairs it silently.** 16 completed `AttachReplica` repairs, 15
   of them reporting "success signals read healthy". The repair works, so the
   underlying fault never surfaced as an incident anybody read.

The third is the one worth keeping: a repair that works is a fault that stops
being reported. The operations agent's intent journal was the only record that
this had happened sixteen times.

## Reproducing

Any demotion that takes the fence-rewind path, on a pair whose archive does not
reach back to the translated cursor. On the playground it recurs on its own
roughly twice a week. A deterministic version is worth building before a fix is
attempted, since the FATAL is shared with BUG-0079 and a fix for either could
look like a fix for both.

## One margin that moved today, from the ADR-0022 owner

`c70b9d1` re-derives the shed threshold from an OBSERVED bytes-per-sequence
rather than a fixed 16 KiB that had never been measured. It does not touch the
rejoin path or the pruner, so it neither causes nor fixes this — but it makes
the shed gate fire **later** than it used to, ~16x later at 1 KiB records,
because it now implements the documented half-the-budget intent instead of a
16x-tight accident. Anyone reasoning about margins near this path should know
the old incidental conservatism is gone.
