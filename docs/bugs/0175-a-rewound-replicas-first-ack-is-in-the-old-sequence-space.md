# BUG-0175: a rewound replica's first ACK is in the old sequence space, so the master reads the whole offset as lag (OPEN)

**Status:** **OPEN**, found 2026-09-21 reviewing the day's ops agent run
(ops OPS-0295). The root cause is measured. The fix is not written yet.
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

1. The replica's stream loop sends `ACK kv.last_applied()` every 500 ms
   "whatever else happens" (`main.rs`, the heartbeat above the frame decode).
   It is checked **first** on every iteration.
2. After a rewind, `kv.last_applied()` is the snapshot's cursor, in the
   **old master's** space. It is adopted into the new master's space only
   when the loop decodes `FLINTSYNC-OK <translated cursor>`. That decode comes
   after the heartbeat check, and the rewind has taken far more than 500 ms,
   so the first heartbeat fires before the OK line is read.
3. The master's `drain_acks` passes every `ACK` to `ReplHub::record_ack`
   unchanged. The master translated the cursor it SERVES (`own_seq_for_upstream`)
   but not the acks it RECEIVES.
4. That untranslated ack then feeds two consumers:
   - `seq_lag` (`latest − effective_acked`) reads the offset as lag, which is
     what the agent saw;
   - `wal_headroom_exhausted` (`latest − min_acked_live ≥
     wal_headroom_shed_seq`) sheds writes if the offset exceeds the threshold.
5. The next ACK is in the new space. `record_ack` keeps the per-replica
   maximum, so the window closes at the replica's next ack.

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

## Fix (not yet written)

The ack must not leave the replica in a space the master does not use.
Either:
- the replica sends no ACK until the handshake's `FLINTSYNC-OK` has been
  processed, or
- the master ignores, or translates, acks below the cursor it served on a
  rewind attach.

The first is the smaller change. A drill needs an offset **above** the shed
threshold. That makes the shed visible in `writes_shed_headroom`, and a
drill that runs only with offsets under the threshold would pass whether or
not the fix is present.
