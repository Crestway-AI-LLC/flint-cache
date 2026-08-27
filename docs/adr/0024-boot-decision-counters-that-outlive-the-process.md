# ADR-0024: boot-decision counters that outlive the process (PROPOSED, not implemented)

Status: PROPOSED, 2026-08-27. Nothing here is built. Raised by BUG-0062, whose
diagnosis needed an evidence bundle and `grep` to establish a fact that should
have been a number on a dashboard.

## The problem

A replica's rejoin is a sequence of DECISIONS taken at boot: warm-rejoin, rewind
to a local snapshot, refuse past the fence, full re-seed. Every one of them is
recorded as a log line and nothing else.

That is survivable for a single boot and useless for a LOOP, which is the
failure mode that actually hurts. BUG-0062's livelock was established by pulling
an evidence bundle off five hosts and counting lines:

```
1  rewound to .../snap-…-seq130042-e0.4
1  rewound to .../snap-…-seq196517-e0.6
2  rewound to .../snap-…-seq207196-e0.7      <- the same snapshot, twice, fatal both times
```

"Twice" was the whole finding. Nothing exported it, nothing alerted on it, and
the operator-visible symptom — `flintctl: never reconverged behind` — pointed at
lag, which was fine.

**A gauge would not have helped either**, which is the part that makes this an
ADR rather than a ticket. On `WalPurged` the seat marks itself and EXITS
(`hard_exit(3)`, deliberately — the DB handle is shared with the serving path).
Any in-memory counter dies with the process, and the next boot starts at zero.
The quantity worth seeing — *how many times has this node re-seeded in a row* —
is precisely the one that cannot live in memory.

## What to record

Four counters, monotonic per data directory:

| counter | answers |
|---|---|
| `rewind_attempts_total` | is the rewind path being taken at all? |
| `rewind_walgap_total` | how often does the attach lose the retention race? |
| `reseeds_total` | **is this node looping?** — the one that matters |
| `snapshots_quarantined_total` | is BUG-0062's remedy firing, and how often? |

`reseeds_total` climbing on one seat while its pair is healthy IS the livelock,
visible without opening a log. `snapshots_quarantined_total` would have turned
"the fix has never fired in production" — a claim that took a purpose-built
drill to settle — into a number anyone could read.

## Where they live, which is the actual decision

They must survive `hard_exit` and a full re-seed of the data they sit beside.
Three candidates, none free:

1. **A file in the data dir.** Simplest, and wrong by default: a full re-seed
   deletes the directory, resetting exactly the counter whose *whole purpose*
   is to count re-seeds. It would have to live one level up, beside the dir
   rather than in it, and then it is not obviously owned by anything.
2. **A system row inside RocksDB.** Owned, transactional, and unavailable at the
   moments that matter — the store is not open yet during the boot decision, and
   is being discarded during a re-seed.
3. **The agent, from log lines.** No storage question at all, and it inverts the
   dependency: a seat's own recovery history becomes something only an external
   process knows, and only while that process is running and shipping.

(1)-beside-the-dir is the likely answer, but "what owns a file that outlives the
data directory it describes" is a real question about ownership and cleanup, not
a detail — an orphaned counter file after a seat is decommissioned is the same
class of litter this repo has filed twice already.

## Why not just alert on the log line

Because the log line is what we already had. `FATAL: … can never resume` was
printed, correctly, both times — and the run still reported `never reconverged`.
A log line is evidence only to someone who already suspects where to look; the
point of a counter is to remove that precondition.

## Not doing this yet

BUG-0062's structural fix (quarantine) removes the livelock, so the urgency is
gone and the design question can be answered properly. What remains true is that
if it recurs in another form, the diagnosis is another evidence bundle and
another `grep`.
