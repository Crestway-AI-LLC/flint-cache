# BUG-0038: the replication shipper runs at a quarter of its documented rate, and the lag caps were set against the documented one (FIXED)

Status: FIXED 2026-08-20 · Severity: medium — nothing is lost or corrupted;
what is wrong is that a shipped default (`--lag-soft-ms 500`) sits BELOW the
system's ordinary operating point, so the brake is engaged during healthy
traffic and BUG-0035's margin is thin by construction

## The claim in the code

`main.rs:3321`, on the FLINTSYNC push loop:

> Throughput ceiling is REPL_TAIL_BUDGET_BYTES per ~20ms cycle (~200MB/s),
> far above a link.

## Measured

Instrumented per-stage, local pair on loopback, one sustained 400k-write
pipe of ~530-byte values, 222.9 MiB shipped, two runs:

| stage | run 1 | run 2 | share |
|---|---|---|---|
| socket write | 44.4 ms | 42.2 ms | **52%** |
| `drain_acks` | 23.7 ms | 18.6 ms | **25%** |
| `updates_since_budgeted` | 13.7 ms | 12.8 ms | 17% |
| encode | 5.0 ms | 4.8 ms | 6% |
| **total per 4.4 MiB cycle** | **88.2 ms** | **79.7 ms** | |
| **ship rate** | **50 MiB/s** | **53 MiB/s** | |

Not ~20 ms and not ~200 MB/s. **80-88 ms and ~50 MiB/s**, four times slower
than the comment, on loopback where there is no link to be "far above".

## Why it produces the 631 ms steady state

The loop is strictly serial: drain acks -> sample -> materialize -> encode ->
write, then round again. Roughly **77% of each cycle is spent blocked on the
socket or draining acks rather than producing anything**, and the two halves
never overlap — while the master fetches and encodes (~18 ms) the replica has
only the socket buffer to chew on, and while the master writes (~42 ms) it is
not preparing the next batch.

A sustained writer outruns 50 MiB/s, so the queue builds until backpressure.
The arithmetic closes: 631 ms / 80 ms ≈ 8 cycles ≈ 35 MiB ≈ 40,000 writes at
530 bytes, against the `lag_max_gap = 40161` measured independently in
BUG-0035. The steady-state lag is not a mystery about replication being slow;
it is the cycle time times the queue depth.

## What this does to the caps

`--lag-soft-ms` ships at **500 ms** and the ordinary peak is **631 ms**. The
soft band is therefore entered during healthy traffic as a matter of course —
217 and 423 delayed writes in two clean no-stall runs — and `--lag-hard-ms
1000` sits at about 1.6x the natural operating point. That is the whole of
BUG-0035's thin margin, and it is a consequence of this, not of anything the
gate box does.

## The constraint any fix has to respect

The single-threaded duplex is deliberate, and the reason is in the same
comment: a rustls session is one stateful object and cannot be `try_clone`d,
so the dedicated ACK-reader thread the old design had was removed to let the
stream be TLS. **A fix may not simply put the reader back on another thread.**

What is available without touching that:

- the ~18 ms of fetch+encode is pure CPU and could be prepared for cycle N+1
  while cycle N is writing, since only the DATA moves off the critical path,
  not the socket;
- `drain_acks` reads in 4 KiB chunks and calls `record_ack` — one `replicas`
  mutex acquisition — once per WAL batch, about 7870 per cycle, purely to
  compute a maximum;
- the 42 ms socket write is the replica CONSUMING, and is the real floor.

So the honest ceiling for a fix that keeps the TLS constraint is roughly 80 ms
-> ~50 ms, i.e. ~85 MiB/s and a steady state near 380 ms. Better, not
transformative, and worth saying before anyone budgets for more.

## An attempted fix that failed, recorded so it is not retried blind

Coalescing the acks — accumulate the maximum while decoding, call
`record_ack` once per drain, and read in 64 KiB chunks instead of 4 KiB —
looked like ~20 ms of free win. It is not viable as written: the run that
should have shipped 222.9 MiB shipped 57.9 MiB, `drain_acks` went UP to
76 ms, and the loading client returned no result at all, which is a
correctness regression and not merely a slower one. Reverted. The cause was
not established; the interaction to look at first is the drain's exit
condition, since the coalesced form only records on the WouldBlock path and a
continuously-fed drain may not reach it.

The baseline was re-run twice afterwards to confirm the harness was not the
variable: both completed the full 400k load and reproduced 222.9 MiB at
79.7-88.2 ms per cycle.

## Fixed: 615 ms -> 115 ms, by acking once per read group instead of per batch

**The attribution above is wrong and is corrected here rather than edited out.**
This file said the loop was copy-bound and serial and proposed overlapping the
master's ~18 ms of fetch+encode. That would have bought almost nothing.

Instrumenting *inside* `drain_acks` — counting reads and frames and timing the
reads themselves — settles it:

    drain_acks total          23.7 ms
      BLOCKED inside read()   21.4 ms   <- 90%
      CPU                      2.2 ms
      reads per cycle            179
      ACK frames per cycle      3626

`drain_acks` was not computing, it was **waiting**. Add the 42 ms socket write,
also waiting, and **~63 of every 80 ms was the master blocked on the replica**.
The master's own work was ~20 ms. The loop was never the bottleneck; the
replica's apply path was, and every master-side optimisation was aimed at 2 ms.

### The fix

The replica sent one ACK **per applied WAL batch** — a 30-byte `write()` each,
3626 per shipped cycle. Replication is ordered, so every ACK but the highest
is the same fact retold. It now accumulates `pending_ack` and flushes one frame
when the loop is about to block on `read()`.

    ACK frames per cycle   3626 -> 38
    reads per cycle         179 -> 36

Measured interleaved, four rounds each, quiet box:

| arm | load s | `lag_ms_max` | `lag_max_gap` | `writes_delayed_soft` |
|---|---|---|---|---|
| main | 5.74 | 622 | 39473 | 266 |
| fixed | 5.43 | **120** | 10178 | **0** |
| main | 5.40 | 627 | 22429 | 227 |
| fixed | 5.40 | **109** | 7798 | **0** |
| main | 5.72 | 585 | 33928 | 240 |
| fixed | 5.60 | **105** | 7725 | **0** |
| main | 5.82 | 608 | 31666 | 250 |
| fixed | 5.38 | **190** | 14070 | **0** |

**615 ms -> 115 ms of steady-state lag, and `writes_delayed_soft` is zero in
every run** — ordinary traffic stops entering the soft band at all. Load time
is unchanged, so this is not a throughput win: the replica spends its CPU
applying instead of on 3626 syscalls, and the replica was the constraint.

Margin to the shipped 1000 ms hard cap goes from ~370 ms to ~885 ms, which is
the whole of BUG-0035's thin margin.

### Two hypotheses that died, so they are not retried

**The mutex.** `record_ack` took the `replicas` mutex once per frame, ~7870
per cycle. Coalescing that on the MASTER side — accumulate the max, record
once — ran clean three times and changed nothing measurable. It was chasing
2.2 ms of CPU. Not shipped: a change that buys nothing should not ship.

**The memmove.** `ack_buf.drain(..used)` per frame looked O(N^2). It is not:
the old code read in 4 KiB chunks and drained after each frame, so the buffer
never exceeded ~4 KB and the total movement is ~1.6 ms per cycle. A cursor
version measured *worse*, which was contention, not the change.

### The measurement trap, which caught me three times in one session

I first concluded the replica coalescing "buys zero latency" — drain time was
23.7 ms before and 23.8 ms after. That measurement was taken at load average
14-25 on 8 cores. On a quiet box the same change is a 5.3x win.

Every wrong turn here has the same shape: a latency measurement taken while a
peer's benchmark had the box at 7x oversubscription. What worked:

- **counts instead of times** where possible — ACK frames per cycle is
  load-insensitive, and 3626 -> 38 was true at load 59;
- **interleaved A/B** (A B A B) rather than AAA then BBB, so drift hits both
  arms;
- **checking `uptime` before believing any number**, and checking that the two
  binaries under test actually differ by sha before trusting an A/B at all.
