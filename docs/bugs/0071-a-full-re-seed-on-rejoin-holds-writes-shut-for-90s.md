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
