# BUG-0082 — the rejoin adopts a cursor the archive can no longer serve (OPEN; the refusal now names the archive's AGE, 2026-09-02)

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

### DONE 2026-09-02 — and the site was not the one this section named

The refusal now carries what the archive actually holds:

    WALGAP full sync required: <io error> (archive holds 47 segment(s),
      newest 3s old, oldest 43201s old)

**The FATAL site could not answer this, and that is the part worth recording.**
The FATAL prints on the REPLICA; the missing segment is on the MASTER
(`/var/lib/flint/node-7002/archive/140130.log` in the transcript above is the
master's path). A replica cannot stat an archive it does not own, however
carefully it logs — so the age has to be attached where the refusal is
produced, in `flintsync`'s `ReplError::WalGap` arm, and it then rides to the
replica inside the error string the FATAL already prints verbatim. Two lines,
in the other process.

`RocksKv::archive_span()` walks `<data-dir>/archive` and reports segment count
with the oldest and newest mtimes as ages. **A directory that cannot be read
returns `None`, not an empty span**, and the refusal then says "archive state
could not be read on the master" rather than "archive holds NO segments" — a
refusal must not assert a retention fact it failed to look up, which is the
same rule `disk.rs` states for a failed `statvfs` and BUG-0079 broke.

**How to read the result when this next recurs.** The archive is the master's
and the outage is knowable from the logs:

| oldest retained segment | reading |
|---|---|
| hours old, and the cursor is below it | the cursor is hours old after a seconds-long outage → **translation**, and no retention change would help |
| seconds old | the archive really did prune inside the outage → **retention** |

That is the discrimination this section asked for, and it is now in the one
line an operator already sees.

Five unit tests, including both halves of the unknown/empty distinction: a
missing directory is `None`, a readable empty one is `Some(segments: 0)`.
Non-`.log` files are excluded, since `LOCK` and `OPTIONS-*` live in that
directory too and counting them would inflate a number whose only job is to be
believed.

### There are TWO refusals, and the unit tests could not have told me

The first version wired the age into `flintsync`'s `ReplError::WalGap` arm —
the one whose text appears in the transcript at the top of this file — and the
five unit tests passed. An assertion added to `walgap_quarantine_drill.sh`,
the one drill in the suite that provokes a real WALGAP, failed immediately:

    WALGAP cursor 1256 is no longer reachable from this WAL
      (oldest retained batch starts at 1728, past the 1257 needed ...):
      full sync required

A different message, from a different site. `flintsync` refuses on **retention
admission** before it streams anything (`updates_since_budgeted(cursor, 1)`),
and that is the refusal a REJOIN meets first; the arm I had edited is reached
only once streaming is under way. Both are real, both end at a replica, and
only one was carrying the age.

**And the admission refusal was the more important of the two**, because it
already reports the oldest retained batch AS A SEQUENCE — the exact quantity
this file argues cannot discriminate. It looked informative and was not.

Both sites now carry the span. The lesson is the cheap one: unit tests on
`archive_span` prove the function works and say nothing about whether anything
calls it, and the wiring assertion cost one line in a drill that already
produced the condition.

### The discrimination, demonstrated

`walgap_quarantine` runs its master with `--wal-ttl-seconds 1
--wal-size-limit-mb 1`, so it IS the short-retention arm, and the refusal now
says so in the same breath as the refusal:

    full sync required (archive holds 2 segment(s), newest 1s old, oldest 1s old)

A one-second-old archive is retention, unambiguously. The playground's is
twelve hours, and a cursor below it after a seconds-long outage is the other
arm. Same message, and now they read differently.

**Not a fix, and not a reproduction.** It decides which half to fix on the next
occurrence — roughly twice a week on the playground — without waiting to build
the deterministic reproducer this file still wants.

### 2026-09-05 — the diagnostic is DEPLOYED, so the next occurrence is readable

Checked, because "it decides on the next occurrence" is only true once the
thing doing the deciding is running on the fleet that produces them:

| fact | how it was established |
|---|---|
| `3120a3a` (this diagnostic, 2026-09-02) is in **v0.1.0-rc.69** and in no earlier tag | `git merge-base --is-ancestor` against rc.65/66/67/69 |
| the playground fleet was rolled to **rc.69 on 2026-09-04** | `docs/playground-runbook.md`, the rollback section, checked on the box after that roll |

So every occurrence from 2026-09-04 onward carries the span, and every one
before it does not. At roughly twice a week, the deciding evidence should
appear within days of that date rather than needing to be waited for
indefinitely.

**Where it will be.** Seats log to `{statedir}/logs/{name}.log`
(`flint-ctl/src/main.rs:1438`) and the playground's statedir is
`/var/lib/flint`, so:

    grep -h "archive holds" /var/lib/flint/logs/node-*.log

Read the result with the table above: an **oldest segment hours old with the
cursor below it** settles translation and no retention change would help; an
**oldest segment seconds old** settles retention.

**Nothing here has read a playground log.** This section establishes only that
the instrument is in place and where to point it — the discrimination itself is
still unmade.

### And the third reason it is invisible is untouched by any of this

The list below says the agent repairs this silently — 16 completed
`AttachReplica` repairs, 15 reporting "success signals read healthy" — so the
underlying fault never surfaced as an incident anybody read. **That is exactly
as true of the new line as of the old one.** The span now exists in a seat log
on a box, twice a week, and nothing routes it anywhere a human looks; the
repair still closes cleanly and the case still reads as handled.

So the realistic failure mode is no longer "we lack the evidence" but "the
evidence accumulates unread", which is a worse place to be, because it looks
like progress. Whoever picks this up should either read the log on purpose
(the grep above) or make the refusal reach the report — the second being the
only version that survives nobody remembering.

#### What "reach the report" would take, and the subtlety that shapes it

Traced rather than guessed, because the obvious design does not work.

The operations agent builds its whole world from `FLINTINFO` and other
protocol calls (`flint-agent/src/world.rs`) — **it never reads a seat log, and
could not**: it dials addresses, and the log is a file on a box. So the conduit
has to be a `FLINTINFO` field, which the agent already parses and the report
already renders.

The reason string is already persisted. `mark_needs_reseed(dir, why)` writes
`cannot resume this tail: {why}` into the `NEEDS_RESEED` marker, and `{why}` is
the same text that carries the archive span. Nothing needs capturing that is
not already on disk.

**The subtlety is lifetime, and it runs the wrong way.** Two things conspire:

- the seat **exits** immediately after writing the marker (`hard_exit(3)`, at
  `main.rs:6435`), so at the moment the evidence is produced there is no
  process left to answer `FLINTINFO`;
- the next start full-syncs and calls `clear_needs_reseed`, which **deletes the
  marker** — "this copy is authoritative again".

So recovery erases the evidence, and the only window in which a live seat holds
it is between start-up and the clear. A field reporting the *current* marker
would be empty every time anyone looked.

The shape that works is *last* reseed reason, not *current*: read the marker
before clearing it, keep the string and its timestamp in memory, and expose
them as a `FLINTINFO` field that outlives the recovery which destroyed the
file. Small, and additive — but it IS a product surface, so it is left as a
design here rather than added in passing.

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

## The reason now outlives the recovery that destroys it (2026-09-05)

Implements the design traced in `fcd9715`, which established that the conduit
has to be a FLINTINFO field — the operations agent builds its world from
protocol calls and never reads a seat log, because it dials addresses and the
log is a file on a box it cannot see. Sixteen `AttachReplica` repairs were
opaque for exactly that reason.

**The lifetime runs the wrong way, and that is the whole design.** The reason is
durable for precisely as long as it is useless: `mark_needs_reseed` writes it
and the seat calls `hard_exit(3)` immediately, so nothing is alive to answer
FLINTINFO; the next start reads it, wipes or clears, and full-syncs. A field
reporting the CURRENT marker would be empty every single time anyone looked,
because recovery is what deletes the answer. So `LAST_RESEED` holds the last
reason in memory, past the recovery, and FLINTINFO renders
`last_reseed_reason` and `last_reseed_at_ms`.

**Captured at BOTH destroyers, and the second is the one that matters.**
`clear_needs_reseed` is the obvious site; the wipe path is the one a "cannot
resume this tail" reseed actually takes, and it calls `remove_dir_all` without
ever reaching that function. Capturing only in the clear would have remembered
every reason except the one this bug is about.

**Absent, not a sentinel.** A seat that has never been told to reseed emits no
line at all. OPS-0134 is what a well-meaning placeholder costs — `-99999` for
"no certificate configured" reached the agent as a cert expiring 273 years ago
and produced `RotateCerts` on a healthy fleet — and for a PROSE field the
tempting placeholder is `none`, which is a string a reader can quote back as
the reason. `last_reseed_at_ms` is omitted on its own when the marker's mtime
was unreadable, so a consumer never sees a fabricated instant beside a real
reason.

Seven tests, four mutations checked. Two things the mutation run caught, both
in the tests rather than the code:

- `the_reason_survives_a_directory_wipe` calls the helper directly, so deleting
  the production call site left it green. The wipe is unreachable from a unit
  test, so the ordering is now asserted against the source — a weak
  instrument, used deliberately and labelled, rather than a hole nobody
  recorded.
- `a_reason_cannot_inject_a_field` asserted `!contains("role:master")` and
  failed against a WORKING guard: the CRLF is replaced with a space, so the
  text correctly survives inside the reason. The property is line structure,
  not substring.

**Next, and deliberately not here:** the agent parses FLINTINFO by named field
(`world::info_field`), so surfacing this in the report is an ops-repo change
and lands after this one. Until it does, the field is readable by hand —
`FLINTINFO` on a seat now answers "why did you reseed", which it never could.

## "So read it" — read, 2026-09-05: there has been nothing to read

`5d222d3` left the instruction to go and read the deployed diagnostic. Done,
from the archived fleet journal (`playground/fleet/cp-state.journal`, 14,584
rows) rather than from a box.

**The diagnostic has had nothing to discriminate, because the failure has not
recurred.** Since the journal's first row on 2026-09-01:

| | count |
|---|---|
| `RejoinStarted` | 7 |
| of those, reaching `RejoinDecided` (the warm path) | 5 |
| `AttachReplica` repairs | **0** |

The two rejoins with no `RejoinDecided` were PROMOTED mid-rejoin (09-02 15:55
and 09-04 21:12, each a demote/promote pair inside a roll), so they stopped
being rejoins rather than failing as ones. **Every rejoin that completed as a
rejoin decided warm.** None took the rewind that this bug is about.

**That is a positive control, and it is why the zero means something.** A quiet
fleet would show zero attaches for the boring reason. This one demoted,
promoted, rolled all four seats to rc.69, and rejoined seven times in five
days; the operation the bug lives in happened repeatedly and did not produce
it.

### What this does NOT establish, and a hypothesis the dates refute

The reported rate was 16 repairs in three weeks (~0.76/day), so five quiet days
is unlikely under it — but the journal spans six days, so **there is no
before/after inside one series**, and everything before 09-01 is a different
source read a different way.

The tempting explanation was ADR-0035's warm-rejoin probe: verify the copy
before discarding it, which avoids precisely the rewind that adopts an
unservable cursor, and whose own comment describes this failure ("rewound one
15 minutes into the past and the catch-up span was already gone from the
master's WAL"). **It does not fit.** That probe landed in `f9782c4` on
**2026-08-16**, inside the three-week window that produced the 16 repairs. It
cannot on its own explain a quiet period that began afterwards.

So: no cause is claimed. The measurement is that the symptom has stopped for
five days across seven rejoins, and that the instrument added on 09-02 has not
yet had an occurrence to speak about. **Left OPEN for that reason** — a
diagnostic that has never fired has not been shown to work, and a bug whose
symptom is absent is not a bug that has been explained.

## 2026-09-06 — "nothing to read" was guaranteed, and the journal says more than the instrumentation could

The section above records reading for the reseed reason on 2026-09-05 and
finding nothing. That reading could not have found anything, and the reason
is not that the condition stayed quiet.

**The playground does not have the instrumentation.** It runs
`v0.1.0-rc.69`, tagged `0cece54` on 2026-09-04. `last_reseed_reason` and
`last_reseed_at_ms` landed in `088bc4e` on 2026-09-05, which is **not an
ancestor of that tag**. A binary cannot emit a field whose code did not exist
when it was built, so the absence said nothing either way. Confirmed against
the box as well as against git: no `reseed` string in the exporter's metrics,
and the deployed `flintctl` has no verb that would surface it.

This is the same shape as the mistake that produced BUG-0109's wrong severity
earlier the same day — a fact established about the SOURCE, reported as a
fact about the FLEET. The fix ships whenever rc.70 is cut and deployed; until
then this bug's instrumentation is not in service and should not be cited as
evidence of anything.

### What the fleet journal does say, which needed no new build

`/var/lib/flint/cp-state.journal`, read today. 9 `RejoinStarted`, 7
`RejoinDecided`, 2 `Promoted`, 2 `Demoted`, 0 `AttachReplica`. The last four
rejoin decisions:

| when (UTC) | epoch | path taken |
|---|---|---|
| 2026-09-04 21:12 | (0,62) | **rewind** — "rewound to seq 184548113 (fence 184548882): tailing incrementally" |
| 2026-09-05 17:14 | (0,64) | warm rejoin at seq 189145310 |
| 2026-09-06 07:15 | (0,64) | warm rejoin at seq 190890687 |
| 2026-09-06 13:56 | (0,64) | warm rejoin at seq 191721781 |

Read carefully, because the rewind is easy to misread as a recurrence. **It is
not.** This bug's failure is rewind → adopt the master's translated cursor →
`FATAL: WALGAP full sync required`. The 09-04 rewind ended in "tailing
incrementally", which is the rewind SUCCEEDING. No fatal follows it in the
journal, and the pair is `live_replicas 1` right now.

So on the evidence available without the instrumentation: the failing path has
not been observed since this bug was filed, the recent rejoins take the warm
path ADR-0035 added, and the epoch has advanced 59 → 64 without the pair
dropping to one copy and staying there.

That is **not** grounds to close it. It is one fleet, the failure was always
intermittent, and the mechanism described above is unchanged in the code. What
it changes is the next step: the question is no longer "read the reseed
reason", it is **get rc.70 onto the playground and then read it** — and until
that happens, a quiet journal is the only evidence there is.
