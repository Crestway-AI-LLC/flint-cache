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

## Amendment, 2026-10-16 — the ownership question, answered (PROPOSED)

"Not doing this yet" above rests on urgency, not on the design, and the design
had one open question: *what owns a file that outlives the data directory it
describes.* An existing mechanism answers it, so the question was larger in the
abstract than in this tree.

**Where.** `{statedir}/boot-counters/node-{port}.json` — in the statedir, a
sibling of the data dir rather than inside it, so the re-seed that deletes
`{statedir}/node-{port}/` leaves the counter that exists to count re-seeds.
That is option (1) above, "beside the dir".

**Who owns it: the inventory.** `sweep_orphans(inv: &Inventory)`
(`crates/flint-ctl/src/main.rs:3121`) already derives what is live from the
inventory and already removes statedir litter; the counters path joins what it
sweeps. `decommission_node` (`:8360`) removes it with the seat's other state.
So the orphan is prevented by a mechanism that exists and is already exercised,
not by a new rule someone has to remember — which is what made the question
feel open. The litter this repo has filed twice came from paths nothing owned;
this one is owned by the same thing that owns the data dir.

**Absence must not read as zero.** A missing or unparseable file reports
`unknown`, never `0`. A seat that has never written one and a seat that has
re-seeded nothing are different facts, and a counter whose whole purpose is to
show a LOOP is the last place to render them identically.

**Written whole, not appended.** One JSON object, four `u64`s, rewritten and
renamed over. A torn write loses counts and cannot corrupt a reader; an append
log would need parsing at exactly the moment the seat is least healthy.

**Exported through `FLINTINFO`**, so the agent and the dashboard read a number
instead of grepping five hosts — which is the entire point of the ADR.

**What it deliberately does not do:** survive a host rebuild. Wipe the
statedir and the history is gone, correctly — a rebuilt host has no rejoin
history to report.

**Status of this amendment: PROPOSED.** The design question is answered; whether
to build it now is Jeff's, and the ADR's own answer above is "the urgency is
gone".
