# BUG-0194: a rejoin the master translated DOWN kept its old cursor and skipped the difference, and the full re-seed it fell back to could not checkpoint past half the RAM (FIXED 2026-09-30)

**Status:** **FIXED 2026-09-30**, found the same day by the rc.78 soak (5 x
i4i.large, one pair, driven from the internal disk; evidence
`~/soak-0930/flint-scale-evidence-20260930-141744`). Two defects, one per
half of the failure, each with a drill that fails before its fix:
`tools/rewind_lower_space_drill.sh` and `tools/fullsync_ckpt_dir_drill.sh`.
Released in the next cut after v0.1.0-rc.78.
**Severity:** high. The first defect loses replicated writes on the rejoined
copy **silently**: measured 900 of 7,500 keys missing, nothing logged, and the
copy's cursor above the master's tip, so lag read zero. The copy serves
replica reads without those keys, and a later failover to it loses them for
good. When the loss happened to be loud instead, it sent the seat to a full
re-seed, and the second defect made that re-seed impossible for any pair
holding more than half a node's RAM on the AMI.

## What was seen

Cycle 5 of the soak failed: `master kill DOWNGRADED — no live replica within
8s`, then `restart-node 172.31.65.67:7002 did not come back`. Both seats had
met the same thing on their last rejoin. On 7002:

    replicating from 172.31.78.27:7001 starting at seq 33507110 (epoch (0,10))
    adopted the master's role epoch (0,11): this copy is on its timeline now
    replication link lost (apply: SequenceGap { expected: 33507111, got: 33507097 }); reconnecting in 1s
    replicating from 172.31.78.27:7001 starting at seq 33507110 (epoch (0,11))
    FATAL: WALGAP cursor 33507110 is no longer reachable from this WAL (oldest retained batch starts at 33507112, past the 33507111 needed ...

and its master had logged `rewind attach: upstream cursor 33507110 (epoch
(0,10)) maps to local seq 20533089`. The copy then quarantined all 49 of its
snapshots and went to a full re-seed. The master logged `full sync starting
(1/2 slots in use)` 38 times and `full sync served` never. The replica logged
`full sync not ready (peer closed connection without sending TLS
close_notify)` 37 times. 7001 had done the same earlier (`28145241 -> 28136266`,
`SequenceGap { expected: 28145242, got: 28145227 }`, 26 snapshots
quarantined), and got out only by rewinding to an older snapshot.

The claim row (ops `docs/coordination.md`, 2026-09-30T21:21) read this as a
cursor landing inside the master's apply batch. That was the symptom. The
cursor was in the wrong sequence space.

## Mechanism

### 1. The adoption moved only forward

A rewound copy presents a cursor in its old master's sequence space. The new
master translates it into its own (`own_seq_for_upstream`), streams from the
translated number, and returns it in `FLINTSYNC-OK`. The replica adopted it
only if it was **higher** than the cursor it had asked with. That holds while a
master's own space runs ahead of the stream it applied (each applied batch adds
a cursor row). It fails once the master is itself a copy that rewound to an old
snapshot and adopted a far higher cursor: its own space starts low and stays
behind. Replaying applied batches costs it one sequence per upstream sequence,
so only its own later writes ever close the gap. The soak produces that shape
routinely. Both seats had it.

So the copy kept its old-space number while the master streamed from a lower
one. `apply_batch` drops every batch ending at or below the cursor as already
applied, which is the module's documented idempotence. Then one of two things:

- **Silent.** If a batch starts exactly at the old cursor + 1, it applies, and
  the whole offset is skipped without a word. The drill measured this on the
  unfixed build: B served 6,600 keys against A's 7,500, with `last_applied`
  12906 above A's tip of 8410.
- **Loud, as on the soak.** A batch straddles the old cursor, and the strict
  contiguity check fails (`SequenceGap`, `got` below `expected`). The epoch had
  already been adopted in a separate write, so the reconnect presented an
  old-space cursor **under the master's epoch**. The master read that number in
  its own space, where it sat inside a batch, and refused it as a WALGAP.
  `quarantine_unresumable` then disqualified every snapshot at or below it.

Two more defects sat in the same adoption:

- **It snapped in the wrong WAL.** `set_last_applied` moves a cursor to the end
  of the batch containing it in THIS node's WAL. That is right for its other
  callers (a checkpoint's own latest, a rewound snapshot's own position). For a
  translated cursor, which numbers the MASTER's WAL, it could step over master
  sequences the copy never applied.
- **Epoch and cursor were two writes.** The epoch names the space the cursor is
  in. A crash between the writes leaves a pair that the next attach reads in
  the wrong space.

### 2. The full re-seed checkpointed in temp_dir()

`flintfullsync` made its checkpoint in `std::env::temp_dir()`. On the database's
own filesystem a RocksDB checkpoint is hard links. Anywhere else, RocksDB copies
every file. On the AMI (AL2023), `/tmp` is a tmpfs capped at half the RAM
([AWS](https://docs.aws.amazon.com/linux/al2023/ug/filesystem-slash-tmp.html);
ops `packaging/aws/gate-box/run.sh` measured it on its own boxes). That is
8 GiB on an i4i.large, while the soak's pair held about 20 GB. Each attempt
copied into memory until the tmpfs filled. Then `checkpoint_to(..)?` returned
the error, and the connection closed with nothing logged on either side. The
replica's retry loop reads an EOF as "master not ready" and asked again.

What is measured and what is inferred. Measured: 38 attempts, 0 served, 37
EOF retries, each attempt's slot released before the next arrived (`1/2 slots
in use` every time). The only early exit between "starting" and "served" is
that `?`. The 37 retries fell within about 380 s of the seat's last start, so
roughly 10 s each, which fits a copy of several GiB before failing rather than
an instant refusal. **Not measured:** the error text. RocksDB's `LOG` records
`Snapshot failed -- ...`, but the harness captured only its stall lines, and the
seats were torn down. The fix makes the next occurrence say what it was.

## Fix

1. **The handshake adopts the translated cursor exactly, in either direction,
   with the epoch, in one write.** New `RocksKv::adopt_timeline(cursor, role)`
   puts the cursor row (not snapped) and the role row in one `WriteBatch`. The
   tailer calls it on the first `FLINTSYNC-OK` of a connection. The idle
   keepalive's `FLINTSYNC-OK <cursor>` no longer moves the cursor: every batch
   before it has already brought the copy to that position, so equal is a
   no-op, and ahead could only mean batches it never applied.
   `manifest::role_row` now owns the role row's encoding.
2. **A full sync checkpoints inside the data directory** (`<data>/flint-fullsync/`),
   so it is hard links and needs no space. On failure the master logs `full sync
   FAILED: could not checkpoint into ...` and sends `-ERR full sync could not
   checkpoint on the master: <reason>`. The replica keeps retrying, now naming the
   cause. A boot removes any checkpoint a crash left there, since its links pin
   SSTs compaction has replaced.

No wire change and no new `Mutation`. A mixed pair works either way round: an
old master's `FLINTSYNC-OK` has the same shape, and an old replica reads the new
`-ERR` through its existing error path.

## Verification

- `tools/rewind_lower_space_drill.sh` builds the soak's shape in two
  promotions. A rejoins B from a snapshot taken once B leads A's space by about
  6,000 sequences. Then B snapshots, and B rejoins the promoted A, whose
  attach translates `12906 -> 7208`. The drill checks the attach really went
  down before it believes anything. **Unfixed:** `FAIL: B did not adopt the
  translated cursor 7208`; with that check removed, `keyspaces diverge (A: 7500
  ..., B: 6600 ...)`. **Fixed:** PASS, with every key and value equal (SHA-256
  over all keys and values).
- `tools/fullsync_ckpt_dir_drill.sh` runs the master with `TMPDIR` pointing at
  a directory that does not exist. **Unfixed:** the soak's signature exactly
  (`full sync starting (1/2 slots in use)` repeated, never `served`; the
  replica at `retry 60` of "not ready"). **Fixed:** PASS on all three arms: it
  seeds with nothing left behind; a blocked checkpoint is logged on the master
  and named in the replica's retry, and it seeds once cleared; a leftover
  checkpoint is swept at boot.
- A unit test, `adopt_timeline_is_exact_in_either_direction_and_carries_the_epoch`,
  also shows the contrast: `set_last_applied(5)` inside an 8-op local batch
  gives 8.

## Deliberately not changed

- **`apply_batch`'s silent skip.** It is how a wrong-space cursor lost data
  without a trace. With exact adoption, a correct stream never delivers a
  batch ending at or below the cursor, so the skip could become a detected
  error. But it is the M0 idempotence contract (ADR-0003, this module's
  header), and changing it is its own decision.
- **The false WALGAP on an interior cursor while later batches exist.**
  BUG-0050's `batch_covering` fallback runs only when the iterator is empty.
  Here that refusal was the one thing that turned a silent loss into a loud
  re-seed, so "fixing" it alone would have made the soak lose data quietly.
  With cursors adopted exactly, no legitimate attach presents an interior
  cursor.
