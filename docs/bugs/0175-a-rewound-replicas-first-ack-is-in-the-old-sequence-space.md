# BUG-0175: a rewound replica's first ACK is in the old sequence space, so the master reads the whole offset as lag (FIXED 2026-09-22)

**Status:** **FIXED 2026-09-22**, found 2026-09-21 reviewing the day's ops
agent run (ops OPS-0295). The fix has two replica halves and a master guard;
`tools/rewind_ack_space_drill.sh` holds all three, and a mutant of each fails
it (below). Released in the next cut after v0.1.0-rc.76.
**Severity:** medium. It is not seen to lose data, and the pair converges
within about one heartbeat. But the same untranslated number feeds the
WAL-headroom **write-shed gate**. On a rejoin whose offset exceeds the
threshold in force, the master would refuse writes with `THROTTLED` for that
window. The offset reached **21,384,102** on the playground, and the boot-default
threshold is **3,568,800**.

## What was seen

At the rc.76 playground roll (2026-09-21T19:50:43Z) the ops agent's
`flint_node_seq_lag` peaked at **3,042,520**. The previous 186 hourly sweeps,
back to 09-11, never saw it above 1,000. At promotion the new master logged:

    rewind attach: upstream cursor 271627120 (epoch (0,73)) maps to local seq 274668873

The offset between the two nodes' sequence spaces is 274,668,873 −
271,627,120 = **3,041,753**. The spike is that offset plus about 770
sequences, roughly six seconds of the playground's ~130 writes/s. The
replica logged `rewound to ... snap-...-seq271627120-e0.73 ... tailing
incrementally instead of a full re-seed`, then `adopted the master's
translated cursor 274668873`. So no three-million-entry catch-up happened.
The number is a cursor from the wrong space.

## Mechanism

**Corrected while fixing it.** The OPEN write-up named only path 2 below, the
heartbeat, and called the whole thing timing-dependent. Path 1 fires on
**every** rewind attach.

1. **The accept handler acked the wrong number, every time.** On
   `FLINTSYNC-OK` the replica adopted the translated cursor and then sent
   `ACK cursor`. But `cursor` was the local captured **before** the
   handshake: the number it had asked with, in the old master's space.
2. **The heartbeat could beat the accept.** The loop sends
   `ACK kv.last_applied()` every 500 ms "whatever else happens", checked
   first on every iteration, and its clock starts when `FLINTSYNC` is sent.
   Until the OK is decoded, `last_applied` is still the old-space cursor. A
   translation plus retention probe slower than 500 ms put it on the wire
   first.
3. The master's `drain_acks` passes every `ACK` to `ReplHub::record_ack`
   unchanged. The master translated the cursor it SERVES (`own_seq_for_upstream`)
   but not the acks it RECEIVES.
4. That untranslated ack then feeds two consumers:
   - `seq_lag` (`latest − effective_acked`) reads the offset as lag, which is
     what the agent saw;
   - `wal_headroom_exhausted` (`latest − min_acked_live ≥
     wal_headroom_shed_seq`) sheds writes if the offset exceeds the threshold.
5. The next ACK is in the new space. `record_ack` keeps the per-replica
   maximum, so the window closes at the replica's next ack: milliseconds
   under load, up to a heartbeat when idle. That is why the agent sampled it
   once in eighteen rejoins even though path 1 fires on all of them.

## How big the offset gets

Every `rewind attach` line in both playground node logs since 08-18:

| rejoin | offset (sequences) |
|---|---|
| 2026-08-18 17:21Z | 21,384,102 |
| 2026-08-23 15:17Z | 10,324,198 |
| 2026-09-14 15:47Z | 8,117,949 |
| 2026-08-27 02:41Z | 7,750,140 |
| 2026-08-24 16:29Z | 6,516,713 |
| 2026-09-16 21:26Z | 6,473,305 |
| 2026-09-11 00:08Z | 5,956,450 |
| 2026-08-21 23:08Z | 5,240,324 |
| 2026-09-02 15:55Z | 4,525,452 |
| 2026-08-20 01:51Z | 4,168,551 |
| 2026-08-27 17:55Z | 4,063,053 |
| **2026-09-19 14:58Z (rc.74 roll)** | **3,545,382** |
| **2026-09-21 19:51Z (rc.76 roll)** | **3,041,753** |
| 2026-08-21 05:08Z | 2,759,709 |
| 2026-08-20 19:41Z | 2,721,112 |
| 2026-09-04 21:12Z | 2,102,571 |
| 2026-09-01 01:34Z | 244,345 |
| 2026-08-20 21:58Z | 1 |

## Why the threshold in force matters, and what is not known

The shed threshold is derived at boot from the archive budget, assuming 16 KiB
per sequence: **3,568,800** on the playground. A background thread re-derives
it every 30 s from observed bytes per sequence. A roll promotes a master
seconds after restarting it, so the replica attaches while the boot default is
still in force.

- **09-19:** the rc.74 roll's offset cleared the threshold by 23,418 sequences.
- **Today:** the offset was 85% of it.

Eleven of the eighteen offsets above exceed it. Whether any of those rejoins
actually shed writes cannot be recovered:

- the node does not log sheds, only counts them in `writes_shed_headroom`;
- that counter dies with the process;
- the ops agent exports `writes_shed_lag` but not `writes_shed_headroom`.

The rc.74 roll not showing a spike is sampling, not absence. The window is
about one heartbeat, and the agent happened to catch today's.

## Fix

- **Replica, path 1.** The accept handler acks `kv.last_applied()`. After
  the adoption that is the translated cursor, and on a keepalive it is also
  fresher than the connect-time `cursor`.
- **Replica, path 2.** No heartbeat until the replica has been accepted. An
  `accepted` flag is set by the `FLINTSYNC-OK` handler.
- **Master guard.** `drain_acks` ignores any ACK below the cursor this stream
  was served from, and counts it in a new FLINTINFO field,
  `acks_below_cursor`. A replica acks its applied position, which starts at
  the served cursor, so nothing legitimate is ever below it. This covers a
  replica on an older build, for example during a rollback. The counter is
  also the drill's deterministic witness.
- **Test knob.** `FLINT_TEST_DELAY_SYNC_OK_MS` holds the master's OK so path 2
  is exercised every run. It is listed in flintctl's `SEAT_ENV_NAMES` beside
  BUG-0174's knob.

## Verified

`tools/rewind_ack_space_drill.sh`, registered in `DRILLS`, builds a rewind
attach under live load.

- **The fixture.** The offset is measured at the snapshot, not the tip. The
  first cut measured the tip, 2,002, and its own fixture check caught the
  attach at 302. The shed threshold is half that offset, and the drill checks
  the logged attach offset exceeds it. Local run: offset 6,002 against a
  threshold of 3,001.
- **Arm 1.** Master holds its OK 1.5 s. Required: zero acks below the served
  cursor, zero sheds, convergence.
- **Arm 2.** A raw client speaks the pre-fix handshake: the old epoch, then an
  ACK of the old-space cursor. Required: it is counted and nothing is shed.
  Sent pipelined, that ACK was consumed by the connection's command reader and
  never reached the drain, so arm 2 passed or failed on how the bytes split.
  Mutant M3b exposed this, and the client now pauses 300 ms, as a real pre-fix
  heartbeat would.

The fixed build passed three consecutive runs. Each mutant was recompiled
(checked for the `Compiling` line) and fails the drill:

| mutant | fails on |
|---|---|
| M1: heartbeat not gated on `accepted` | arm 1, acked below the served cursor |
| M2: accept handler acks `cursor` | arm 1, acked below the served cursor |
| M3: master guard removed | arm 2, old-space ack not counted |
| M3b: counted but still recorded | arm 2, **B shed 120 writes** |

M3b is the severity, measured. A recorded old-space ack makes the master
refuse live writes, 120 of them in a 2 s liveness window at the drill's paced
load. That is the playground's exposure on any rejoin whose offset exceeded
the threshold in force.

## Not in this fix

- **The ops agent.** It exports neither `writes_shed_headroom` nor the
  in-force threshold, and its sweep divides by a compiled 4,000,000. That is
  ops OPS-0295.
- **`acks_below_cursor`** is new here, and nothing watches it yet.
