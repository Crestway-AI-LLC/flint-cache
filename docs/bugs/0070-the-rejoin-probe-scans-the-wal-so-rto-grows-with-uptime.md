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

## How it actually landed, and where the plan above was wrong

Two corrections, both found by building it.

**The upper bound is not a bound.** The section above prescribes
`floor <= cursor <= latest`. The second half is wrong: when `cursor > latest`,
`updates_since_budgeted` returns an **empty `Ok`**, not a `WalGap` — a replica
that is level with or ahead of the master is not a retention failure, and
refusing it there would invent a re-seed the old code never performed. So the
check answers `Some(true)` for `latest <= cursor` and only ever refuses on the
FLOOR. Caught in review by the peer session before it shipped.

Ahead-of-tip is a real state, not a curiosity: a demoted master rejoins holding
writes the survivor never acked. It must re-seed — but that verdict belongs to
the **promotion fence**, which already runs earlier in the same handler, not to
a retention check that cannot see lineage.

**A separate command was the wrong shape.** The first implementation added a
`FLINTWALRANGE` command so the probe could ask for bounds without a handshake.
`tools/failover_bystander_drill.sh` failed it immediately:
`FATAL: WALGAP cursor 2 is past the promotion fence 0`. The new command
answered the retention question **while skipping the fence check** that the
FLINTSYNC handler performs first, so a superseded copy could be told it was
resumable. That implementation was reverted in full.

What shipped instead keeps `FLINTSYNC` as the probe — fence check, ordering and
all — and makes only its retention step cheap, replacing the
`updates_since_budgeted(cursor, 1)` positioning call with
`RocksKv::cursor_within_wal_bounds`. When that cannot answer from the WAL's
bounds (an empty WAL yields no floor) it returns `None` and the handler falls
through to the original positioning call, so the expensive path remains as the
fallback rather than the default.

The general lesson: *"make the probe cheap"* was treated as licence to replace
the probe rather than to make one step of it cheaper. The fence was invisible in
the cost profile precisely because it is cheap — 2–3 ms — which is what made it
easy to design past.

**Guarded by a differential test.** `cursor_within_wal_bounds` is deliberately a
second implementation of a question the positioning call already answers, so
`bounds_check_agrees_with_the_positioning_call` sweeps a spread of cursors
across a staged retention gap and asserts the two agree on every one. It also
asserts it actually reached both verdicts — the first two versions of that test
passed while never once producing `Some(false)`, because deleting archived
segments under an open DB does not move the floor, and an empty WAL answers
`None`.

## What this does NOT fix

Measured in the verification soak, 2026-08-28, cycle 3: a rejoin that needs a
**full re-seed** cost **94.2 s** of write blackout at `min-replicas-to-write=1`
(`full sync: received 103 files`, ~8.2 M sequences). The probe's WAL scan is a
~5 s term; the re-seed is a ~90 s one. Cheapening the probe does not touch it.
Filed separately as BUG-0071.

## CORRECTION 2026-08-28: the mechanism above is wrong

Everything above blames `updates_since_budgeted` → `get_updates_since` walking
WAL files to position at the cursor. **That is not where the time goes**, and
the "fix" built on it (`cursor_within_wal_bounds`) was measured on a fleet to
change failover time by nothing. It has been reverted.

### What the measurement said

Local, release, 1 KiB values — the two calls the theory accused:

| sequences | archived WAL files | `get_updates_since(0)` | `get_updates_since(cursor)` |
|---|---|---|---|
| 200 K | 4 | 0.2 ms | 11.5 ms |
| 1.2 M | 24 | 2.8 ms | 11.0 ms |
| 2.4 M | 48 | 10.5 ms | 11.9 ms |

Milliseconds, and flat. Idle or under a concurrent writer, 1-byte or 1 KiB
values, the answer did not change. Seek position was never the cost.

### Where it actually goes

`RocksKv::own_seq_for_upstream`, which translates the replica's UPSTREAM cursor
into this node's own sequence space. It calls `get_updates_since(0)` and then
**iterates every batch and every operation** from sequence 0 until it finds the
batch whose cursor row reaches the requested position. It runs in the FLINTSYNC
handler on the REWIND path — the path every soak cycle takes — and it runs
BEFORE the retention check that was optimised.

| sequences | walk |
|---|---|
| 200 K | 62 ms |
| 600 K | 235 ms |
| 1.2 M | 531 ms |
| 2.4 M | 1,157 ms |

Linear, ~0.48 µs per retained sequence. Extrapolated to the fleet's 15.04 M
cursor: **~7.2 s predicted against 6,902 ms measured** — 63% of a 10,969 ms
failover blackout.

### The line that pointed at it

In the same rejoin, against the same master, at the same instant:
`fence=2ms probe=6902ms`. Both are round trips to that master. Connection setup,
TLS handshake and master-side saturation would cost FLINTFENCE exactly what they
cost FLINTSYNC, so the difference had to be inside the FLINTSYNC handler — which
is what made a WAL-scan story unnecessary and a code read sufficient.

### What shipped

A sparse index over the upstream→own mapping (`\x00flint\x00upidx\x00`, one
entry per 10 k upstream sequences), used as a HINT for a start position. The
walk is unchanged and still reads the real cursor rows.

Measured at 3.6 M sequences: **201.9 ms indexed against 2,689 ms unindexed**,
same answer. It is a large improvement and NOT a constant one: the residual is
RocksDB's own seek to the starting position, which no stride removes. Making
translation genuinely O(1) means not touching the WAL at all — an exact
per-batch mapping read as a point lookup — which is a bigger change and is not
what this is.

### Why the hint cannot corrupt a replica

`get_updates_since` SKIPS the batch at its starting position (see
`flint-storage::repl`'s header). A hint landing exactly on the answer's batch
therefore steps over it, and the scan returns the NEXT qualifying batch — a
LATER position, from which a replica would resume past data it never applied.
A failed lookup was never the danger; a **successful wrong one** was, and a
plain "fall back if nothing is found" guard does not catch it.

So the hinted walk verifies its own start: the first cursor-bearing batch it
sees must still be BELOW the target. Cursor rows are monotone, so that proves
no earlier batch could qualify — which is exactly the claim "the first match
found is THE first match". If it does not hold, the walk restarts from 0.
`a_hint_never_changes_the_answer` plants hints that are too early, one before
the target, exactly on it, past the answer, and beyond the WAL entirely, and
asserts the answer is identical every time. It caught the bug above.

### The lesson, which is the same one twice

The Prevention section above said of its own asymmetry: *"reasoned from the
API's contract, not measured. The fix's own prediction below is the test."* The
prediction failed and the fix shipped anyway, because the gate proves
correctness and nothing proved EFFECT. Then the first local measurement missed
the real cost too — it timed iterator construction and the first item, which is
neither of the things the code executes. Measuring the part you have a
hypothesis about is not measuring.
