# BUG-0070: the rejoin probe scans the WAL, so failover time grows with a master's uptime (OPEN)

**Found** 2026-08-28, decomposing a `min-replicas=1` failover with the sub-phase
instrumentation added the same day. Severity: high — it makes RTO a function of
how long a master has been up, which is the one variable an operator does not
associate with failover.

## Measured

One 60-minute soak, 5 hosts, `min-replicas=1`, one kill per cycle. Every column
is from the fleet journal, not inferred:

| total | restart | decision window | fence | restore | probe | unacct | cursor |
|---|---|---|---|---|---|---|---|
| 1,050 ms | 876 ms | 120 ms | 2 | 25 | **35** | 58 | 149,795 |
| 5,694 ms | 827 ms | 2,779 ms | 3 | 209 | **2,354** | 213 | 2,832,158 |
| 7,200 ms | 900 ms | 4,652 ms | 3 | 269 | **4,077** | 303 | 7,774,768 |
| 7,950 ms | 851 ms | 5,468 ms | 2 | 208 | **4,985** | 273 | 10,329,665 |
| 7,472 ms | 794 ms | 5,081 ms | 2 | 189 | **4,686** | 204 | 15,186,665 |

**Refined by the fifth point: probe PLATEAUS, it does not grow without bound.**
4,985 ms at cursor 10.3M, then 4,686 ms at 15.2M. The scan is over the RETAINED
WAL, and retention is capped (`--wal-ttl-seconds`, `--wal-size-limit-mb`), so
the cost rises until retention saturates and then holds at whatever that window
costs to walk — about 4.7-5.0 s on this fleet. "Grows with uptime" was the right
direction and the wrong asymptote; the honest statement is that RTO is
proportional to the RETENTION WINDOW, reached after enough uptime to fill it.
That is worse than it sounds, not better: it means the penalty is a permanent
property of a tuned fleet rather than something a restart clears.

**Only `probe` moves at all.** `fence` is flat at 2-3 ms across all four. `restore` and
the unaccounted remainder flatten after the first (small-dataset) cycle.
`restart` is a constant to three digits. Probe tracks the cursor at roughly
**500 ms per million sequences**, and by cycle 4 is 91% of the decision window
and 63% of the whole outage.

Note what is NOT correlated: the fence gap. It was 8,988 sequences on the run's
worst outage and shrank across the earlier soak's cycles while decision time
doubled. The cost is not proportional to how far behind the replica is.

## Mechanism

    replica restarts
      -> probe_resume(cursor)                       "can I resume from here?"
      -> FLINTSYNC <cursor> <gen> <counter>          the REAL tailer handshake
      -> master: updates_since_budgeted(cursor, 1)
      -> RocksDB get_updates_since(cursor)           walks WAL files to position

`updates_since_budgeted` takes a 1-byte budget, and the comment above it is
careful about that: "a budget of 1 byte materializes at most one batch". That
bounds what comes BACK. It says nothing about what it costs to FIND the cursor,
and that is the whole expense: RocksDB positions by walking WAL files.

So a yes/no question is answered by a scan whose length is the retained WAL.

## Why "expected" is the wrong frame

That WAL grows is expected. That FAILOVER TIME grows with it is not, and the
two are only connected by this implementation choice.

The perverse part: the cost rises the more caught-up the replica is. Position
is reached by walking from the WAL's floor, so a cursor near the tip skips
nearly the whole file set, while a badly-lagged replica's cursor is found
sooner. The healthiest replica pays the most.

And it puts two goods in opposition that need not be. WAL retention is what
makes a rejoin cheap — tail the difference instead of re-seeding the dataset —
but retention is exactly what makes the probe slow. Today you buy rejoin
cheapness with failover slowness.

## Prevention, not mitigation

The question is not how to make the scan faster. It is why a resumability check
touches the WAL at all.

**Corrected 2026-08-28, before implementing: the fields I named are not WAL
bounds.** `wal_min_acked_seq` is `hub.min_acked_live(now)` — the minimum acked
sequence across LIVE REPLICAS — and `wal_headroom_seq` is likewise
replica-derived. Neither describes what the WAL still retains, and the master
does not publish its floor at all today. I proposed that fix from the fields'
NAMES without reading what feeds them, which is the same error as reading a
metric's label for its meaning.

**What survives, and is better.** The cost is asymmetric in a way the fix can
exploit: `get_updates_since(seq)` walks WAL files to find the one holding `seq`,
so a HIGH cursor skips nearly every file while `get_updates_since(0)` matches
the first file immediately. **The floor is cheap to obtain; only the high cursor
is expensive.** So the master can answer a resumability question with two cheap
values — the floor (first batch of a scan from 0) and `latest_sequence_number()`
(already O(1), already used) — and the check becomes `floor <= cursor <=
latest`, with no positioning at the cursor at all.

That asymmetry is reasoned from the API's contract, not measured. The fix's own
prediction below is the test.

Three things make that safe rather than a shortcut:

1. **The real sync re-checks.** `try_rewind`'s own comment already says so:
   "The master re-checks the same fence at FLINTSYNC, so a stale answer here
   downgrades to a re-seed rather than a divergent copy." An optimistic probe
   that is occasionally wrong lands in the fallback that exists today.
2. **A bounds check cannot be optimistic in the dangerous direction.** The
   retained floor is monotonically increasing, so a cursor that compares as
   in-range may have been recycled a moment later — which the re-check catches
   — while a cursor that compares as out-of-range genuinely is.
3. **It removes the reason the probe exists in its current form.** The probe was
   added so a transient `EAGAIN` would not be read as a master's refusal
   (`e54fe7d`, which cut a 95 s outage to single digits). That fix needed a
   verdict, not a stream. Reusing the full tailer handshake to obtain a verdict
   is what dragged the WAL in.

Alternatives considered and rejected as mitigation rather than prevention:
shorter WAL retention (buys RTO by making rejoins re-seed — trades the same two
goods, just the other way); snapshotting more often (does not help, since the
scan is from the floor); caching the position on the master (keeps the scan,
adds invalidation).

## Not yet done

Nothing is changed. The measurement stands on four points from one soak with a
mechanism identified in source; a fifth cycle was still running when this was
filed. What would confirm it beyond doubt is the fix itself: if the probe
becomes a bounds check, `probe` should collapse to single-digit milliseconds and
stop tracking the cursor, and total outage should fall to roughly
restart + restore + unaccounted, or about 1.3 s at these sizes.
