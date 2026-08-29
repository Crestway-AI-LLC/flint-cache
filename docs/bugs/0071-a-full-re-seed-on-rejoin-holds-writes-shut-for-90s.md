# BUG-0071: a full re-seed on rejoin holds writes shut for ~90 s at min-replicas=1 (OPEN)

**Found** 2026-08-28, in the soak run to verify BUG-0070's probe fix. Severity:
high — it is a 9x overrun of the published 10 s RTO budget, and unlike BUG-0070
it is not a slow check that can be made fast: it is the dataset moving over the
wire while the write path is held shut.

## Measured

One cycle of a 5-host `min-replicas-to-write=1` soak, from the fleet journal
(`/tmp/flint-scale-evidence-20260828-143346`), relative to the kill:

| at | event |
|---|---|
| +520 ms | master unreachable, confirmed across required ticks |
| +527 ms | promotion applied, epoch-fenced |
| +765 ms | `WriteQuorumLost` — live replicas 0, min-replicas-to-write 1 |
| +1,510 ms | `RejoinStarted` on the surviving seat |
| +23.3 / +53.3 / +83.4 s | three scheduled snapshots, **all at `seq8175258`** |
| +94,191 ms | `WriteQuorumRestored` — live replicas 1 |

**Total blackout 94,204 ms** against a 10,000 ms budget. Detection and promotion
were healthy — 527 ms, well inside budget. Everything after is the rejoin.

The three snapshots pin the cost independently of any timer: their sequence
number is **identical across 60 seconds**, so the master accepted no writes at
all in that window. This was not a slow write path; it was a closed one.

The seat log gives the cause:

```
rewind: cannot resume from the restored copy against …:7002
  (refused: cursor 8191139 is outside the master's retained WAL [3065673, 8175258]); full re-seed
NEEDS_RESEED present: this copy cannot be continued
  (superseded copy rejoining the lineage held by …:7002) — discarding it and re-seeding
full sync: received 103 files
```

## The re-seed was correct

Cursor **8,191,139** against a master tip of **8,175,258**: the rejoining node
held **15,881 sequences the survivor never had**. It was the old master, and its
tail was writes that died with it. Continuing that copy would be time travel.
The oracle recorded zero corruption and zero acked-write loss for the cycle —
the safety property held. What it cost was availability.

## Two things this is NOT

**Not an artifact of the reverted `FLINTWALRANGE` build** this soak was running.
That build refused on retention wording; the shipped code refuses the same
cursor at the **promotion fence** instead. Both end in a full re-seed, because
supersession is the real condition and neither gate invents it. If anything the
shipped path is marginally faster here: a refusal naming the promotion fence
skips the doomed `try_rewind` attempt (`main.rs:1533`) that this log shows
running and failing first.

**Not BUG-0070.** That is a ~5 s WAL scan inside the probe. This is a ~90 s
transfer after the probe has already answered. Fixing one does not touch the
other, and this one is the larger term whenever a re-seed is required.

## Why it becomes a write outage

At `min-replicas-to-write=1` a freshly promoted master has zero live replicas
until the dead seat rejoins, so the entire re-seed is spent refusing writes.
Clients saw fast refusals, not stalls — the worst single write held was 481 ms
while the gap between consecutive acks was 94 s, which is the signature of
`-THROTTLED` rather than a stalled commit. At `min-replicas-to-write=0` the
master accepts writes immediately and this cycle would have shown ~1 s; that is
the documented durability trade, not a fix.

## Prevention, not mitigation

The re-seed shipped **103 files / ~8.2 M sequences** to replace a copy that
differed from the master by **15,881 sequences** — five orders of magnitude more
than the divergence. The rewind path exists precisely to avoid that
(`--rewind-snaps`, restore from a local snapshot before the branch point, hard
links rather than the wire) and it RAN and FAILED here, which is the thing worth
understanding: three snapshots at `seq8175258` were taken DURING the outage, but
the branch point was ~8.19 M, so every snapshot available to rewind to was on
the wrong side of it.

Directions, none yet chosen:

1. **Rewind to the fence, not to a snapshot.** The divergent suffix is bounded
   and known — the fence names it. Truncating a superseded copy back to the
   fence is a local operation on 15,881 sequences.
2. **Take a snapshot at promotion.** A snapshot pinned at the fence gives the
   rewind path a target on the correct side by construction.
3. **Let the re-seed not hold the write path.** The re-seeding replica is not a
   durable copy yet either way; counting it as live only at completion is what
   makes the whole transfer a write outage.

Direction 1 is the smallest and attacks the actual asymmetry. Untested.

## Open

Whether a re-seed is the COMMON failover path or a minority one is not
established: this soak's other cycles tailed incrementally
(`tailing incrementally instead of a full re-seed`, epochs 0.5 / 0.7 / 0.8), so
the distribution is bimodal and its shape is unmeasured. Cycle 3 is one sample.
That number decides whether the published RTO should quote the re-seed case.

## The ratio, measured 2026-08-29 — and why it is still not the answer

The Open section above says the incremental/re-seed split "decides whether the
published RTO should quote the re-seed case". Across seven soaks the always-run
boot-decision collection holds **33 rejoin decisions**:

| path | count | share |
|---|---|---|
| rewound and tailed incrementally | 28 | 85% |
| full re-seed | 5 | 15% |

**Do not publish that 15%.** Two things make it the wrong number:

**1. All five re-seeds are a different case than this bug.** Every one gives the
same reason — `rewind: no snapshot dir at /var/lib/flint/snaps/g0` — and all
five come from one early soak. That is the COLD START: a fleet whose first
rejoin happens before any snapshot exists, so there is nothing to rewind to.
It is bounded by the snapshot cadence and disappears once a fleet has run for
one interval. This bug is about a SUPERSEDED copy, where snapshots exist and
every one of them is on the wrong side of the branch point. That case appears
**zero** times in the collected set.

**2. The collection cannot see the cycles that matter.** In
`scale-cluster/run.sh` every failure branch ends in `die`, and
`collect_boot_decisions` / `collect_rejoin_events` are called after them, on the
path where a cycle "already earned its verdict". So a cycle that FAILS never
reaches the always-run collection — and a 94.2 s re-seed is exactly a cycle that
fails. The evidence is not lost (capture_evidence pulls journals and seat logs
on failure), but it lands in a bundle rather than the aggregate, so any count
over the always-run files omits failures **silently**. Filed as OPS-0082.

The one observation of this bug's actual case — soak 2026-08-28 cycle 3 — is
visible only in `/tmp/flint-scale-evidence-20260828-143346`, and is absent from
that same soak's boot-decisions file, which records cycles 1 and 2 and stops.

So the honest state is unchanged from when this was filed: **n=1 for the
superseded-copy re-seed**, and the 85/15 split above describes cold starts, not
this. What the measurement did establish is that a healthy steady-state rejoin
tails incrementally — 28 for 28 once a fleet has snapshots — which means the
exposure is narrow and real rather than routine, and that a fix should be judged
on the tail it removes, not on a frequency nobody has measured.

## Input from the Flint KV session, 2026-08-29 — and what it changes here

Flint KV solved the "what counts as live during a rebuild" question first, so it
was asked directly. Three answers, one of which changes what to build.

**The design holds.** Accept-but-don't-count has not been argued against there;
the two tests that pin it still stand
(`a_rebuilding_replica_takes_writes_but_does_not_acknowledge_them`,
`an_accepting_rebuilding_replica_is_counted_apart_from_a_real_one`). So
direction 3 above is not a guess — it is a shipped position elsewhere in the
same codebase family.

**But the counting rule is the easy half.** Their warning, quoted because it is
the part worth acting on: *"What decides whether the cluster converges is the
repair job's cadence, and an adaptive backoff built on 'asking when there is
nothing to do is waste' is exactly wrong for that job: the cost of asking is one
status call per replica, and the cost of asking late is a log sitting one
replica short, so the next disk to fail takes it below quorum."* Their
`ReplicaRepairJob` runs almost flat against every other job's backoff — 200 ms
after work, 500 ms after idle, a 2 s ceiling — because *"backing off to ten
seconds meant a wipe every few seconds outran repair and the cluster degraded
until it could not start a node."* They asked what our re-seed path backs off
to before we change what counts as live.

**Checked, and we are in the clear on that axis.** The replica tailer reconnects
on a FLAT one second — `flint-server/src/main.rs:5149`, *"replication link lost
({e}); reconnecting in 1s"* followed by `sleep(Duration::from_secs(1))`. No
escalation, no idle-based widening. The only escalating backoff on this path is
`probe_resume`'s `200 ms x attempt`, bounded at five attempts, and it has never
fired in any measured soak (zero `probe attempt` lines across every run).

So for us the cadence is already the shape their experience says it must be, and
the counting rule really is the substantive change rather than half of one. That
removes the risk they flagged; it does not remove the work.

### A second-order finding worth carrying

The question we sent about a cold or idle tree pinning their release point found
a live bug — not the mechanism we guessed, but next to it. A tree that never
writes holds nothing; a tree that wrote once and went idle WOULD pin the log,
and that case was already known and mitigated by a periodic flush job. The
mitigation was **disabled**: the job iterated a map holding followed shards
beside owned ones, called a helper that refuses writes on a follower, and let
`?` end the whole pass at the first one — so every tree ordered behind it never
reached its clock trigger. Their file is `core/docs/bugs/0011-*`.

The transferable part is not about trees. **A mitigation that exists but never
runs reads exactly like a mitigation that works** — from the code, from the
design doc, and from any test that asserts it is wired rather than that it
fires. Worth holding against our own periodic paths: the snapshot timer this bug
depends on is one of them, and "three snapshots were taken during the outage"
was evidence it ran only because the outage happened to record them.

## Narrowed 2026-08-29: the fence accepted what the tip refused

Re-reading the evidence bundle rather than the summary turns "the rewind path
ran and failed" into something specific. Every `rewind:` line in
`/tmp/flint-scale-evidence-20260828-143346`:

```
rewind: no snapshot dir at /var/lib/flint/snaps/g0; full re-seed          (x5)
rewind: cannot resume from the restored copy against …:7002
        (refused: cursor 8191139 is outside the master's retained WAL
         [3065673, 8175258]); full re-seed                                (x1)
```

**There is no `snapshot N is past the fence; trying older` line for the failing
case.** So the candidate was not rejected by the fence — it passed, was
restored, and was then refused by the master for sitting past its tip
(8,191,139 against 8,175,258). Two checks that exist to answer the same
question disagreed, and the cheaper one said yes.

The five "no snapshot dir" lines are a different condition entirely — the cold
start, before any snapshot exists — which is also every re-seed in the collected
boot-decision set. They are not this.

### What has been excluded

- **Not a name/content mismatch.** The snapshot filename records
  `kv.latest_seq()` (`main.rs:4893`) and `try_rewind` probes with
  `kv.latest_seq()` of the restored copy (`main.rs:574`). Same quantity, so a
  candidate cannot pass the fence under one number and probe under another.
- **Not naive cross-space comparison.** `FenceBound`'s own documentation states
  the hazard and the rule: *"each promotion switches to the promoted node's own
  sequence space, so seqs from different fence rows are not comparable numbers;
  only the row whose epoch immediately supersedes `since` is in the asker's
  space."* The design already accounts for it.
- **Not a wedged snapshot timer.** That was a real defect on this path and is
  now BUG-0073, but it produces "no snapshot dir" / stale candidates, not a
  candidate that passes the fence and fails the tip.

### The open question, stated so it can be tested

Why did `promo_fence_bound` return a bound at or above 8,191,139 for the
ex-master's epoch, when the survivor's tip was 8,175,258 and the ex-master held
15,881 sequences the survivor never received?

Either the bound was `Bound(n)` with `n >= 8,191,139` — in which case the fence
row for the superseding epoch does not name the branch point in the asker's
space, despite the rule above — or it was `Unfenced` and `try_rewind` treats
that arm more permissively than the FLINTSYNC handler does, which refuses it.
Those are distinguishable and neither is established.

### The experiment

Same shape as the flush bug the Flint KV session found: construct the condition
and observe, rather than reason about it.

Extend `tools/rewind_rejoin_drill.sh` with an arm that makes the two sequence
spaces genuinely diverge — let a master accept writes its replica never
receives, promote the replica, then restart the ex-master with `--rewind-snaps`
pointing at snapshots it took while it was master — and assert that the fence
REJECTS its own snapshot rather than accepting a copy the master will then
refuse. Log `promo_fence_bound`'s arm and value at the decision, because the
distinguishing evidence is which arm was taken and no current line reports it.

**Deliberately not fixed yet.** Three plausible fixes follow from three
different answers, and today has twice produced a shipped fix built on a
mechanism that turned out to be wrong — one of them gated 133/133 green while
moving the number it targeted by nothing. The instrument comes first.

## CORRECTION 2026-08-29: this bug's one observation is from the reverted build

The section "Two things this is NOT" says: *"NOT an artifact of the reverted
`FLINTWALRANGE` build this soak was running. That build refused on retention
wording; the shipped code refuses the same cursor at the promotion fence
instead. Both end in a full re-seed."*

**That is wrong.** It assumed the shipped code's fence would refuse a cursor
that `try_rewind`'s fence had just accepted. Following the actual code says it
would not, and the two fences are the same query.

### Why the fence accepts it

The ordinary promotion records `record_promo_fence(kv, epoch, kv.last_applied())`
(`main.rs:4179`) — the **upstream** cursor, i.e. a number in the OLD master's
sequence space, which is what `FenceBound`'s rule requires ("only the row whose
epoch immediately supersedes `since` is in the asker's space"). So when
`try_rewind` compares its snapshot's seq (8,191,139, the ex-master's own space)
against that bound, it is comparing like with like, and the candidate passes
**correctly** — the survivor really had received those sequences.

The `Unfenced` arm is also excluded: `try_rewind` maps it to `None` and
`continue`, skipping the candidate. It is not permissive.

### What the shipped handler then does, and the reverted one did not

Shipped order in `flintsync`: promotion fence (4658) → **`own_seq_for_upstream`
translation** (4681) → retention (4721). The translation exists precisely because
the cursor arriving is in the upstream space while the WAL is in this node's.
A candidate that clears the fence is translated and then checked for retention
as a LOCAL sequence.

`FLINTWALRANGE` answered the retention question without that path — which is the
same defect that made `failover_bystander` fail it and got it reverted. So it
compared an upstream-space cursor against the master's local WAL range and
refused it. The observed message is its wording, and no shipped message matches:
the shipped refusals are "is past the promotion fence", "is no longer reachable
from this WAL", "cannot map upstream cursor … into this WAL", and "promotion
fence history … is incomplete".

So the 94.2 s re-seed was produced by code that is no longer in the tree.

### What this changes

**Severity on current code is unknown, not established.** On the shipped path
this cursor plausibly RESUMES: same fence, then a translation that maps it below
the master's tip, then a retention check it passes. The remaining way to reach a
re-seed here is the translation itself failing — "cannot map upstream cursor"
when the mapping is no longer retained — which is a different cause, a different
message, and one the BUG-0070 sparse index now makes cheap to evaluate rather
than a full WAL walk.

The other five re-seeds in the evidence are "no snapshot dir": the cold start.
Real, bounded by snapshot cadence, and not this.

**So this bug currently rests on zero observations against shipping code.** It
stays open because a 94 s write outage is worth proving absent rather than
assumed absent, and because the "does a re-seeding replica hold the write path
shut" question is real regardless of how often the re-seed is reached.

### The correction I keep having to make

This is the third mechanism error today, and all three have the same shape: I
stated what the code would do without running it. The first shipped a fix that
gated 133/133 green and moved its target number by nothing. The second measured
the wrong call exhaustively. This one defended a claim about the shipped path
while the only evidence came from a build that had been reverted hours earlier.
The experiment below must therefore run on current code, and its first job is to
establish whether this bug reproduces at all.

## CAUSE FOUND 2026-08-29, by experiment on current code

Three runs against `main`, each building the condition rather than reasoning
about it. The harness is in the session scratchpad; arm D of
`rewind_rejoin_drill.sh` is the part worth keeping.

**1. It reproduces.** A master that accepts 400 writes its replica never
receives, then dies; the survivor is promoted; the ex-master rejoins marked,
with `--rewind-snaps`:

```
rewind: snapshot …-seq702-e0.1 is past the fence (702 > 302); trying older
rewind: no snapshot at or before the fence; full re-seed
full sync: received 7 files
```

**2. The mechanism is not broken.** Same run with one snapshot taken BEFORE the
divergence:

```
rewind: snapshot …-seq702-e0.1 is past the fence (702 > 302); trying older
rewind: candidate …-seq302-e0.1 clears the fence for epoch (0,1) (302 <= 302)
rewound to …-seq302-e0.1: tailing incrementally instead of a full re-seed
```
and on the master: `rewind attach: upstream cursor 302 (epoch (0,1)) maps to
local seq 604`. Fence, fallback-to-older, translation and resume all work.

So the fence is correct, the translation is correct, and the earlier
fence/tip-disagreement hypothesis was an artifact of the reverted build. **The
whole cause is whether a candidate at or below the branch point still exists.**

**3. What removes it: `quarantine_unresumable`.** When a tailer hits a WAL gap
it cannot resume, it renames EVERY snapshot with `seq <= last_applied` so the
name no longer parses as a candidate (`main.rs:5143`, BUG-0062's livelock fix —
without it the next boot picks the same losing snapshot and fails identically).

Re-running experiment 2 with exactly that rename applied:

```
   quarantined: snap-…-seq302-e0.1
rewind: snapshot …-seq702-e0.1 is past the fence (702 > 302); trying older
rewind: no snapshot at or before the fence; full re-seed
```

### The chain, stated once

1. A tailer hits an unresumable WAL gap at cursor C → every snapshot `<= C` is
   permanently disqualified.
2. Later, a promotion records a fence at or below C (the survivor's
   `last_applied`, in the ex-master's space).
3. `try_rewind` sees only snapshots ABOVE the fence, refuses each correctly, and
   runs out of candidates.
4. Full re-seed. At `min-replicas-to-write=1` the transfer IS the write outage:
   94.2 s.

### Why the quarantine is too wide

It is scoped to one cursor and unbounded in time. The condition that justified
it — *this* master's WAL no longer reaching seq S — is a fact about one master's
retention at one moment. After a promotion the tail is served by a DIFFERENT
node with a different WAL, and a snapshot disqualified for the old master may be
exactly the candidate the new fence needs. The rename keeps the bytes
(`unresumable-` prefix) but discards the only thing that would let a later
decision reconsider them: which cursor they failed for, and against whom.

**Not proposing the fix in this file yet.** The obvious shape — record the
failing cursor in the quarantined name so a later, lower fence can re-admit it —
is one line of reasoning away from being wrong in the way three things were
wrong today. It now has an instrument: experiment 3 fails, experiment 2 passes,
and a fix has to flip the first without breaking the second or reopening
BUG-0062's livelock.

### Kept

`rewind_rejoin_drill.sh` arm D: a genuinely superseded ex-master, with a valid
candidate, must still rewind. Arms A and C both let the replica catch up before
the kill, so nothing exercised divergence until now. It asserts the divergence
was staged (or it would pass vacuously), that the post-divergence snapshot is
REFUSED, that some candidate is reported as clearing the fence, and that no full
transfer happens. Mutation-checked by making the fence never reject: the arm
fails.

`try_rewind` now also logs the ACCEPTED candidate and the bound it cleared.
Before this, only rejections named a bound — which is why the production
evidence could not distinguish "accepted" from "never asked".

## Fix options, and why the obvious one is wrong

Reading the quarantine's call path to design the fix turned up the constraint
that rules out the first idea, so it is recorded before anything is built.

### Why it disqualifies EVERYTHING at or below the cursor

The narrow fix — quarantine only the candidate that was actually restored and
refused — is enough to break BUG-0062's livelock, which was "the next boot picks
the same losing snapshot and fails identically". It is not enough for what the
breadth is actually buying.

The failure path is: restore → probe refused → quarantine → `mark_needs_reseed`
→ `hard_exit(3)`. The next boot reads the marker, and because the refusal is a
purged WAL rather than a promotion fence, it takes the rewind path AGAIN. With
only the tried candidate removed it picks the next-older one, fails identically,
and exits. **N candidates means N failed boots before the re-seed.** Disqualifying
the whole range reaches the re-seed in ONE extra boot.

So the breadth is not carelessness; it is recovery time for the WAL-purge case.
A fix that narrows it trades this bug's 94.2 s for a slower purge recovery, which
is not obviously a better trade and is certainly not a free one.

### What the fix has to satisfy

1. Experiment 3 must flip: a quarantined-then-promoted node rewinds.
2. Experiment 2 must keep passing: an ordinary superseded rejoin still rewinds.
3. BUG-0062's livelock must stay closed: no infinite retry of a losing snapshot.
4. The purge case must still reach a re-seed in about one boot, not N.

(4) is the one the narrow fix fails, and it was invisible until the call path
was read end to end.

### The shape that can satisfy all four

Keep the breadth, make it CONDITIONAL rather than permanent: record what the
snapshots were disqualified AGAINST, and let a later decision re-admit them when
that condition no longer applies. The quarantine's premise is "this master's WAL
can no longer reach these sequences" — a fact about one master at one moment,
which a promotion invalidates by definition.

Not built yet. The discriminator has to be something available where the
quarantine happens and meaningful where enumeration happens, and getting that
wrong is how the last three mechanisms went wrong. The instrument now exists to
answer it rather than argue it: experiments 2 and 3, plus a new arm asserting a
same-lineage retry does NOT re-admit — which is requirement 3, and the one a
careless re-admission rule would break.
