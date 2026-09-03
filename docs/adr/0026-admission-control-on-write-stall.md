# ADR-0026 — Admission control keyed on the master's own write stall, not on replica lag

**Status:** proposed
**Date:** 2026-08-30
**Amends:** [ADR-0022](0022-wal-retention-bounded-by-replica-progress.md)

## Context

ADR-0022 placed back-pressure on replica progress: the master sheds once it
outruns its slowest live replica, so a replica meets back-pressure before it
meets a deleted WAL segment. That is correct as a *durability* bound and
should stay. What it is not, and what we assumed it was, is the thing that
keeps the write path from degrading.

A five-host fleet was brought up on 2026-08-30 to find where WAL replication
tops out, so ingest could be capped below it. There is no such ceiling at this
scale. Across three windows, 60 pipelined connections driving a pair master
directly with 1 KB values:

| window | shed gate | master accepted | replica applied | ratio | soft-delayed |
|---|---|---|---|---|---|
| A | 32,768 | 74,830 seq/s | 75,041 seq/s | 1.0028 | 0 |
| B | 8,388,608 | 45,966 seq/s | 45,967 seq/s | 1.0000 | 4,728/s |
| C | 8,388,608 | 43,672 seq/s | 43,770 seq/s | 1.0023 | 4,309/s |

The replica ended exactly level (`last_applied` = master `latest_seq` =
45,958,716), and in 58 separate intervals it *drained* a backlog at up to
**4.2×** the master's concurrent rate. Peak lag all day was ~85,000 sequences
against a gate of 8,388,608 — three orders of magnitude of slack.

So a gate sized to the WAL archive, which is what ADR-0022 asks for and what
we built, is correct and **inert**. It cannot bind before something else does.

## What actually degrades

RocksDB's own L0 write stall. In windows B and C, with nothing shedding:
`l0_files` sawtooths 8 → 21, all 60 connections sit pinned in flight,
`writes_delayed_soft` runs at ~4,300–4,700/s, and goodput falls to ~44,000
seq/s. Neither side is CPU-bound at the stall (master 92% idle, drivers 99.7%
idle, two-sample `top`) — during a stall both are *waiting*.

Window A is the accidental control. Its gate was wrong — 32,768, from a 1 GiB
archive the master should never have chosen (BUG-0079's boot race) — and it
shed 125,000 writes/s. That shedding was doing admission control, and the
stream it admitted ran with **`writes_delayed_soft` at zero for the entire
window** and **71% more durable throughput** than the "correct" setting.

The existing deadline shed is the wrong instrument for this. It is keyed on
`write_deadline_ms` (2 s), so it fires long after the collapse: over the run
it shed 17,533 writes while 3,044,426 were absorbed as latency. 99.4% of the
back-pressure arrived as delay, not as a signal a client could act on.

## Decision

Add an admission gate on the master keyed on its **own proximity to a write
stall**, shedding with the existing retryable `-THROTTLED` before RocksDB's
internal delay engages. Signals are already exported in `FLINTINFO`:
`l0_files`, `pending_compaction_bytes`, `delayed_write_rate`,
`write_stopped`.

The point is to convert a latency collapse into a backpressure signal. A
client that receives `-THROTTLED` can retry, shed, or slow down; a client
whose connection is pinned inside a stalled engine can do none of those, and
the engine does less total work while it waits.

ADR-0022's replica-lag gate stays exactly as it is, doing the job it is
actually good at — bounding retention against a replica that has genuinely
fallen behind. It simply stops being the mechanism we expect to limit
throughput.

## Alternatives considered

- **Do nothing; let RocksDB stall.** This is today's behaviour and it costs
  41% of goodput at saturation, plus unbounded per-connection latency. The
  measurement above is the argument against it.
- **Retune RocksDB's `level0_slowdown_writes_trigger` / `stop_writes`.**
  Moves where the stall happens without changing that it is expressed as
  delay rather than as a signal. Worth doing as well, not instead.
- **Rely on the deadline shed.** Already present, already firing, and
  measured at 0.6% of the pressure. It is a last resort, not a governor.
- **Size the replica-lag gate tighter so it binds first.** This is what window
  A did by accident, and it is the wrong reason for the right behaviour: it
  couples the write path's throughput to an archive-retention constant, so
  any change to disk size or value size silently retunes admission. That
  coupling is exactly what BUG-0079 was filed about.

## Open questions

- **The control law.** Window A's 62% shed rate was accidental, not optimal.
  A proportional controller holding `l0_files` just under the slowdown
  trigger is the obvious shape, but the gain and the measurement interval are
  unmeasured, and an oscillating admission gate would be worse than none.

  **AMENDED 2026-09-02: ask this of the LAG CAP first, because that is the
  gate that binds.** This ADR's own amendment demoted the stall gate to a
  backstop "for where replica lag binds". A duration-bounded soak has now
  measured which gate that is. One pair on `i4i.large`, 4 feeders at 48 MB/s
  aggregate against the ~22 MB/s a 2-vCPU seat sustains:

  | counter | after one cycle |
  |---|---|
  | `writes_shed_lag` | **134,091** |
  | `writes_shed_deadline` | 0 |
  | `writes_shed_headroom` | 0 |
  | `writes_delayed_soft` | **443** |

  So the control law that governs real back-pressure today is the lag cap's,
  and it is not a controller at all. It is bang-bang with a constant:

      lag <  soft (500ms)   -> nothing
      soft <= lag < hard    -> sleep(2ms) per write, counted
      lag >= hard (1000ms)  -> shed -THROTTLED

  **The soft band did about 0.3% of the work** — 443 delays against 134,091
  rejections, roughly 300:1. Whatever the soft band is for, at this overload it
  is not what stopped the writes.

  Two candidate causes, and they call for different fixes, so the next
  measurement should separate them rather than assume one:

  1. **The band is too narrow in TIME.** With ~48 MB/s offered against ~22 MB/s
     applied, the backlog grows at roughly 1.2 seconds of lag per second, so
     the 500ms between soft and hard is crossed in well under a second. A band
     the controller barely occupies cannot damp anything. *(Arithmetic from
     measured rates, not itself measured — shown so it can be checked.)*
  2. **The action is too weak, and concurrency-blind.** A fixed 2ms sleep per
     write is a constant, not a function of error. It does not scale with how
     far past `soft` the lag is, nor with how many writers are in flight, so
     its damping effect falls as connection count rises — the opposite of what
     is wanted, since more writers is what produces the overload.

  The proportional shape proposed above for `l0_files` applies at least as well
  here, with the same two unknowns — gain and measurement interval — and the
  same hazard, that an oscillating gate is worse than none. The difference is
  that here there is now a measured operating point to design against instead
  of an accidental 62%.

  **Also settled by the same run:** back-pressure comes from offered RATE, not
  from failure events. 12 cross-host kills over 130,152 writes shed **4**;
  sustained ingestion above replication capacity shed **134,091**. Four orders
  of magnitude apart. A control law tuned on kill-driven shedding would be
  tuned on the wrong signal entirely.

  Caveat, because it bounds what may be designed on this: the 300:1 ratio is
  **one cycle**, not a replicated measurement. The primary claim it sits beside
  — that lag binds and the other gates do not — is categorical and survives a
  single sample; a RATIO is a quantity and does not. Replicate before choosing
  a gain.
- **Whether the same limit belongs on the replica.** It applies the same
  writes with the same engine, so it can stall too; it just never did here,
  because the master stalled first and gave it slack.
- **Asymmetric hardware.** Every number above is from identical instances
  with an otherwise-idle replica. A smaller replica, or one serving reads,
  could make ADR-0022's gate bind first after all — which would make this ADR
  a complement rather than a correction.

## Amendment, 2026-08-30 (same day) — tuning does part of this job, without rejecting anything

A connection sweep and a compaction-parallelism A/B on one fleet changed the
shape of the argument above, and one number in it.

**The operating point is a knee the system will not find on its own.**
Throughput scales linearly with connections to ~12, peaks at ~24–48
(~80–88k seq/s, ~84–93 MB/s at 1,054 B/write), and then goes *backwards* —
46,318 seq/s at 384 connections. A client that offers more load past the knee
gets less work done. That is an argument for admission control that does not
depend on any of the reasoning below it: nothing in the write path steers
toward the maximum, and the client cannot see it.

**But compaction parallelism removes the collapse, without shedding a write.**
The shipped engine runs one `rocksdb:low` thread (RocksDB's default
`max_background_jobs: 2`, `max_subcompactions: 1`). With
`FLINT_BG_JOBS=8 FLINT_SUBCOMPACTIONS=4`:

| conns | 1 thread | 6 threads | ratio |
|---|---|---|---|
| 24 | 77,234 | 76,263 | 0.99 |
| 96 | 78,233 | 79,062 | 1.01 |
| 192 | 37,303 / 23,569 | 75,760 | **2.0–3.2×** |
| 384 | 20,079 | 61,607 | **3.1×** |

Nothing at the operating point; 2–3× past it. The ceiling is unmoved — so
this is not speed, it is overload behaviour. The mechanism is in a counter,
not a rate: at 384 connections the shipped config shed **44,554,807** writes
on the 2-second deadline, the tuned one **347,048**. With a single compaction
thread, overload writes queue behind a stalled engine until they time out.

### What this changes

**The admission gate is a backstop, not the primary defence.** Against L0
collapse, config gets most of it for free and rejects nothing, which is
strictly better than shedding. The gate's remaining job is the region config
cannot reach — where **replica lag** binds. That limit was invisible until
compaction stopped being the constraint, and then appeared immediately:
`writes_shed_lag` at 13,520,819 (C=192) and 38,201,926 (C=384).

So the ordering is: tune compaction for ingest-class hardware first, then let
ADR-0022's lag cap do what it was designed for, and reserve a stall-keyed gate
for what neither covers. That is a narrower mandate than this ADR opened with.

### A correction to the evidence above

The Context section leans on window A — a mis-set gate shedding 125,000
writes/s while delivering 71% more durable throughput than the "correct"
setting. That comparison stands, but the reason is now clearer and less
flattering to it: both windows ran at 60 connections, **past the knee**.
Window A's shedding held the system near its peak by accident; window B's
did not, and sat in the degraded region. The finding is real and the
mechanism is admission control, but it was never evidence that 62% shed is a
good target — it is evidence that *something* has to hold the system at its
knee.

### Still not established

- **What caps ~80k seq/s.** Not compaction (6 threads move it 1%), not
  connection count, not replication below C=192. Unmeasured: the fsync path
  (`wal_fsync_ms` 500), device write bandwidth, WAL append serialization.

  **Two of those three are now measured and eliminated** — see the 2026-09-03
  run at the bottom of this file. The fsync path moves throughput 0.55% across
  500 / 50 / 0 ms, and the write path asks the device for ~7% of what fio gets
  out of it. **WAL append serialization is the only one still unmeasured.**
  That run did not sample the L0 stall counters, so whether its seat reached a
  knee at all is also still open — see the limitation recorded with it.

  Left in place rather than rewritten, because the list is the record of what
  was open on 2026-08-30 and the value of these blocks is being able to see
  what a question looked like before it was answered. But a reader scanning
  headings would otherwise take "Unmeasured: the fsync path, device write
  bandwidth" at face value, which is the same staleness two bug headers carried
  until today.

### ANSWERED 2026-09-02 — should these knobs ship as defaults? No.

The second open question was whether numbers from a 16-vCPU box with 15 idle
cores say anything about a constrained seat. Measured on `i4i.large` (2 vCPU),
order-balanced, both arms verified from RocksDB's own LOG with 43 compactions
confirmed so the mechanism actually ran: **`FLINT_SUBCOMPACTIONS=4` is worth
+1.71%**, against within-arm spreads of 0.72% and 0.45% — where the same knob
is worth +9.8% on 16 vCPU and +14–43% at the batched rate. An order of
magnitude smaller.

`self-hosting.md` already carried the companion result for the other knob, and
it points the same way harder: `FLINT_BG_JOBS` raised on a 2-vCPU seat made
ingest **9.1% slower, with write amplification up 32%**, because compaction
threads take the cores the write path needs.

So the default stays 1, and `rocks.rs`'s warning is now measured rather than
warned about: fifteen idle cores make a compaction knob look free while saying
nothing about a seat where compaction contends with the serve path.

### The first question is LOCAL, not a fleet question

Worth stating because it changes what it costs to answer. Everything the fleet
run established points away from the fleet:

- the replica ended exactly level, peak lag ~85,000 sequences against a gate of
  8,388,608 — three orders of magnitude of slack, and in 58 intervals it
  *drained* backlog at up to 4.2x the master's rate. Replication is not the cap;
- the master sat **92% idle** at the stall. Not CPU;
- six compaction threads move the number by 1%. Not compaction parallelism.

What remains — the fsync path, device write bandwidth, WAL append
serialisation — are all properties of ONE machine. So the experiment is one
`i4i.4xlarge` driven to the knee, not five hosts, and three readings
discriminate the three candidates:

1. **Write amplification**, from the engine's own counters (WAL bytes, flush
   write bytes, compaction write bytes). At the knee the logical rate is
   84–93 MB/s at 1,054 B/write; multiply by the measured amplification to get
   what the device is actually being asked for.
2. **The device ceiling**, measured on the same box with `fio` rather than
   quoted from a spec sheet, and compared against (1).
3. **`wal_fsync_ms`**, currently 500. If the fsync path is the cap, moving it
   moves the ceiling; if it does not, that candidate is eliminated for the cost
   of one arm.

**And a process note that cost this write-up something.** The 2026-08-30 run
recorded rates but not the engine's byte counters, and its boxes are long gone,
so the amplification cannot be recovered from anything on disk — the question
has to be re-run to be answered at all. A run that establishes a rate should
keep the counters that would explain it, because the explaining question is
always asked later and always by someone who cannot go back.

### RUN 2026-09-03 — the fsync path is eliminated; the device is not close

One `i4i.4xlarge` seat, one `c7i.4xlarge` loader, build `d565ebf`, 1024-byte
values, four order-balanced 120 s legs.
`packaging/aws/writepath-cap/run.sh` in the ops repo.

**Reading 3 first, because it is the decisive one.**

| leg | `wal-fsync-ms` | write ops/s | logical MB/s | fsync/s | expected/s |
|---|---|---|---|---|---|
| 1 | 500 | 138,137 | 141.5 | 1.98 | 2.00 |
| 2 | 50 | 138,580 | 141.9 | 19.34 | 20.00 |
| 3 | **0** | 138,469 | 141.8 | **0.00** | 0 |
| 4 | 500 | 138,904 | 142.2 | 1.98 | 2.00 |

**Total spread 0.55%**, across a tenfold change in cadence and then no fsync at
all. **The fsync path is not the cap.**

The null is not vacuous, and that is the whole reason the arms are built this
way. Each one was verified twice before its number was taken: `FLINTCONFIG` was
read back so the value is the engine's rather than ours, and `wal_fsync_total`
had to climb at a rate that TRACKS the cadence — 1.98/s at 500 ms, 19.34/s at
50 ms, exactly zero at 0. A knob that was accepted and ignored would produce
these same four identical throughputs and would mean nothing.

The arms also needed no restart. `WAL_FSYNC_MS` is an atomic the fsync tick
re-reads every iteration, so all four legs ran against one continuously-growing
LSM and no arm can be attributed to a restart or a re-warmed cache.

**Readings 1 and 2: the device is at single-digit percent.**

| | MB/s |
|---|---|
| fio, 1 MiB sequential, qd32, same mount, before Flint wrote anything | **2,334** |
| fio, 4 KiB with `fdatasync` every write | 34.2 |
| best logical write rate observed | 142.2 |
| × 1.18 measured amplification = asked of the device | **~167** |

**About 7% of what fio got out of the same device.** Device write bandwidth is
not the constraint, and it is not marginal — it is more than an order of
magnitude away.

The 34.2 MB/s figure is worth keeping beside it: a WAL that synced on every
write could not reach even the 84–93 MB/s this ADR is about, let alone 142.
The bounded cadence is what makes the observed rate reachable at all, and
leg 3 shows that removing the remaining 2/s buys nothing.

**Amplification, and a correction to how it was first computed.** Measured
1.18x — WAL 66.82 GB (1.00x, the WAL is uncompressed and tracks ingest
exactly) plus 11.71 GB of flush and compaction, over 66.82 GB ingested. The
first version of the instrument reported **2.18x**, having added a flush term
equal to `ingest` on the reasoning that the memtable reaches L0 once. Wrong
twice: the memtable DEDUPES — 39M writes over a 20M key space, finishing at
338.92 MB on disk — and the Sum row's `Write(GB)` already contains the L0
flush output, so the term was double-counted as well as invented. It inflated
the answer in the direction that made the conclusion look weaker, which is
exactly why it would have survived unexamined.

**Two caveats, stated because they bound what this establishes.**

- The payload is memtier's default, which compresses heavily. Flush and
  compaction bytes here are compressed while the WAL is not, so 1.18x is a
  LOWER bound on amplification for real data, and the device share is a lower
  bound too. That is the safe direction for "the device is not the cap" and
  the wrong direction for quoting 1.18x as an amplification figure.
- RocksDB's own Sum-row W-Amp reads **10.1** against this 1.18x, and the gap is
  expected rather than a fault: its denominator is bytes entering L0, ours is
  bytes the client sent, and those differ by exactly the dedup factor. Both are
  printed by the harness so the gap is visible instead of one being quoted.

**What is left.** WAL append serialisation is the only surviving candidate;
nothing here eliminates it.

**And a limitation of this run that has to be stated before its headline number
is used.** This seat sustained 138k ops/s at 142 MB/s where the fleet's knee
was ~80–88k seq/s at 84–93 MB/s. The units DO line up — 80,000 × 1,054 B =
84.3 MB/s, so that figure is one sequence per ~1 KB write, unbatched, exactly
like memtier's 138,904 × 1,024 B = 142.2 MB/s. (Worth saying because elsewhere
in this project a seq/s comparison IS invalid: batching packs ~15 keys into a
sequence, and write-path-next item 3 had to count keys for that reason.)

**But comparable units are not a comparable quantity.** This ADR's own finding
is that what degrades is RocksDB's L0 write stall — `l0_files` sawtoothing
8 → 21 with `writes_delayed_soft` at 4,300–4,700/s, goodput falling from the
knee to ~44k seq/s. **The run above never sampled any of that.** It measured
throughput and fsync and nothing else, so it cannot say whether 138k was a
higher knee or simply a rate below this seat's knee — an unstalled ceiling and
a knee are different quantities, and only one of them is what ~80k is.

So the honest claim is narrower than "the fleet's number is not a property of
this hardware": **on this seat, at this offered load, neither the fsync path
nor device bandwidth was the limit.** Whether the seat was at its knee is
unmeasured, and until it is, 138k against 80k is not a ratio anyone should
quote.

The harness now samples `l0_files`, `write_stall_readable` and
`writes_delayed_soft` DURING each leg — after the load stops the sawtooth
drains and every leg reads quiet — and refuses to let the rates be compared to
a knee unless at least one leg actually stalled. That makes the re-run answer
the question this one raised.

### RE-RUN 2026-09-03 — at the knee this time, and the answer is no

Same two boxes, build `319b250`, load pipelined at depth 32 rather than depth 1.
**Every leg stalled** (`write_stall_readable=1`, `l0_files` peaking 12–20), so
these are knee numbers and comparable in kind to the fleet's.

| leg | `wal-fsync-ms` | client ops/s | engine seq/s | logical MB/s | fsync/s | peak l0 | stalled |
|---|---|---|---|---|---|---|---|
| 1 | 500 | 495,033 | 495,035 | 506.9 | 2.01 | 18 | **1** |
| 2 | 50 | 467,475 | 467,462 | 478.7 | 17.35 | 20 | **1** |
| 3 | 0 | 467,886 | 467,873 | 479.1 | 0.00 | 20 | **1** |
| 4 | 500 | 466,226 | 466,214 | 477.4 | 2.02 | 12 | **1** |

**Depth was the whole of the previous run's shortfall.** 138,904 → 495,033
ops/s on identical hardware, purely from pipelining. The first run was
round-trip-bound and never reached the engine at all, exactly as write-path-
next item 1 found for the un-pipelined path.

**The fsync null survives at the knee, and now has a scale to be judged
against.** Legs 2–4 span 466,226–467,886 — **0.35%** — across 50 ms, no fsync,
and 500 ms. Leg 1 sits 6% above leg 4 *at the same setting*, which is the
order effect: leg 1 met a shallower tree. So the tree-depth drift is 6% and the
knob's effect is under 0.35%, an order of magnitude smaller. Order-balancing
was not a formality here; forward-only would have read leg 1's 495k as a
500 ms advantage.

**The proxy is not in the way at this depth.** Client ops/s and engine
`latest_seq` agree to within 20 ops in every leg, so nothing between the loader
and the engine is dropping or throttling, and the client's count can be
trusted. Worth checking rather than assuming: item 1 measured the proxied and
direct paths differing ~6x, and the fleet's knee was taken driving the master
directly.

**Device: 23%.** 506.9 MB/s logical × 1.05 measured amplification = 530.4 MB/s
against fio's 2,335.3 MB/s on the same mount. Higher than the first run's 7%
because the rate more than tripled, and still not the constraint.

**`writes_delayed_soft` was 0 in every leg, and that is not a null.** The fleet
run had it at 4,300–4,700/s during its stall. It is the soft *replication-lag*
band, and this seat has no replica, so there is nothing for it to measure.
**The two runs' `delayed_soft` columns are not comparable** — which also means
the fleet's soft-delay pressure cannot be reproduced without a replica.

### What this settles

**~80k seq/s is not a ceiling of the write path on this hardware.** A single
i4i.4xlarge seat, driven into a genuine L0 write stall, sustained **~467–495k
seq/s** — five to six times the fleet's knee, while stalling. So whatever binds
at ~80k binds around five times earlier than the machine does, and it is not
the fsync path (0.35%), not device bandwidth (23%), and not the WAL append path
that both of those would have had to pass through.

**What differs between the two, and therefore what to look at next:** this seat
had **no replica**. The fleet's pair had one, and `writes_delayed_soft` — the
one counter that fired heavily there and is structurally silent here — is keyed
on replication lag. ADR-0022's lag cap and its soft band are the remaining
structural difference, and write-path-next item 9 independently found
`writes_shed_lag` to be "the gate that binds, and it binds alone" at a
different operating point.

That is a hypothesis, not a measurement: the two runs also differ in hardware
and payload. The experiment that would settle it is this same harness with a
replica attached, which is one more box and the same half hour.

### REPLICA RUN 2026-09-03 — it is the lag cap, and the account closes exactly

Same harness, same seat type, one `i4i.4xlarge` replica attached as the pair's
second member (flintctl's remote runner, first use of `ssh-user`/`ssh-key`/
`ssh-sudo` anywhere in packaging). Both members ran the same source build.
`live_replicas=1` was asserted before any leg.

| leg | `wal-fsync-ms` | client ops/s | engine seq/s | peak l0 | stalled | `writes_shed_lag` | `delayed_soft` |
|---|---|---|---|---|---|---|---|
| 1 | 500 | 213,754 | **105,192** | 8 | 1 | 13,025,416 | 208,098 |
| 2 | 50 | 204,699 | **102,456** | 3 | 1 | 12,267,827 | 208,925 |
| 3 | 0 | 203,150 | **102,730** | 4 | 1 | 12,049,591 | 208,093 |
| 4 | 500 | 198,635 | **102,274** | 9 | 1 | 11,572,256 | 204,630 |

**Attaching one replica took the committed rate from ~470k to ~102k seq/s — a
4.6x collapse on identical hardware.** And ~102–105k is the same
neighbourhood as the fleet's ~80–88k knee, where the solo seat's ~470k was
five times away from it.

**The account closes to 0.03%.** In the solo run client ops/s and engine
`latest_seq` agreed to within 20 ops; here they diverge by half, and the
divergence is exactly the shedding:

| leg | engine committed/s | `writes_shed_lag`/s | sum | client offered/s | delta |
|---|---|---|---|---|---|
| 1 | 105,192 | 108,545 | 213,737 | 213,754 | −17 |
| 2 | 102,456 | 102,232 | 204,688 | 204,699 | −11 |
| 3 | 102,730 | 100,413 | 203,143 | 203,150 | −7 |
| 4 | 102,274 | 96,435 | 198,709 | 198,635 | +74 |

Roughly **half of every offered write is refused by the lag cap**, and the
three counters account for each other with nothing left over. That is what
makes this a measurement rather than a coincidence of magnitudes.

**This does not contradict the five-host run above; it completes it.** That run
eliminated the WAL-headroom gate — peak lag ~85,000 against a gate of
8,388,608, "correct and inert" — and it was right. `writes_shed_lag` is a
different gate, keyed on `lag_hard_ms` (1,000 ms), and it is the one that
binds. write-path-next item 9 found the same thing independently at a
completely different operating point: *"`writes_shed_lag` is the gate that
binds, and it binds alone."* Here the headroom gate was 2,147,483,648 and again
never fired.

**The fsync null holds a third time, now with replication in the path.** Legs
2–4 span 102,274–102,730, **0.45%**, across 50 ms / no fsync / 500 ms. Leg 1
sits 2.8% above leg 4 at the same setting — tree depth again. Device: 11% of
fio's 2,336 MB/s.

### So what caps the write path

Not the machine. A solo seat stalls at ~470k seq/s; the same seat in a pair
commits ~102k and refuses the rest. **The ceiling is the rate the replica can
apply, enforced by the lag cap** — and that is a property of the topology and
of `lag_hard_ms`, not of the fsync path, the device, or WAL append.

**What this run does NOT separate**, and it is the obvious next arm: whether
~102k is the replica's genuine apply ceiling or simply where a 1,000 ms
`lag_hard_ms` chooses to shed. Both fit this data identically. Varying
`lag_hard_ms` on the same harness distinguishes them — if the committed rate
tracks the threshold, the cap is the tuning; if it does not move, the replica
really is saturated.

**And one instrument fault to fix before that arm.** `lag_ms` read 0 in every
leg because it is sampled after the leg, when the replica has caught up — the
same mistake the stall counters had before they were moved inside the leg. The
shed counters are cumulative and unaffected, but the lag column is currently
worthless and should be sampled during the load.

**A caveat on the counters.** RocksDB's cumulative dump runs every 30 s
(`FLINT_STATS_DUMP_SEC=30`), so the amplification terms are read from a
snapshot up to 30 s behind the end of the run. Every term is equally stale, so
the ratio holds; the absolute byte totals are a slight under-count.
