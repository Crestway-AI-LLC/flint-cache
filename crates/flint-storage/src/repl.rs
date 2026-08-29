// SPDX-License-Identifier: Elastic-2.0
//! Replication primitives: WAL tailing on the master, atomic apply on the
//! replica.
//!
//! CORRECTNESS-CRITICAL. The contract (established by the M0 audit,
//! docs/adr/0003): `rust-rocksdb`'s WAL iterator skips the batch at its
//! starting position, so the tailer is *sequence-idempotent* — the replica
//! requests from `last_applied` (not `last_applied + 1`) and drops every op
//! with seq <= last_applied. That is correct whether the iterator skips the
//! first batch (current behavior) or returns it (fixed upstream), and it is
//! also what makes crash-resume safe.
//!
//! Atomicity: the replica applies each batch's ops together with the
//! last-applied marker row in ONE WriteBatch. A crash between batches
//! resumes from the marker; a crash mid-write applies nothing. There is no
//! state where data is applied but the cursor is not (or vice versa).

use rocksdb::{WriteBatch, WriteBatchIterator};

use crate::rocks::RocksKv;

/// Marker row holding the replica's last applied master sequence number.
/// Lives outside the user envelope space (user rows start with 'M'/'S'/'Z').
pub const REPL_STATE_KEY: &[u8] = b"\x00flint\x00last_applied";

/// Sparse index over the UPSTREAM -> OWN sequence mapping (BUG-0070).
///
/// `own_seq_for_upstream` used to walk the WAL from sequence 0, iterating
/// every batch and every op, to find the batch whose cursor row reaches a
/// requested upstream position. Measured at ~0.48 us per retained sequence:
/// 77 ms at 200 k, 1,157 ms at 2.4 M, and 6,902 ms on a fleet whose cursor
/// had reached 15.0 M -- 63% of a 10,969 ms failover blackout, and a cost
/// that grows with the master's UPTIME rather than with anything the
/// operator can see.
///
/// Entries are HINTS ONLY: a lookup picks the newest entry at or before the
/// requested position and starts the ordinary walk there. The walk is
/// unchanged and still reads the real cursor rows, so a stale, missing or
/// too-early entry costs time and never correctness -- and a walk that finds
/// nothing retries from 0. Nothing here is load-bearing for a result.
///
/// The `\x00flint\x00` prefix keeps these rows OUT of the replication stream
/// (see `SYSTEM_PREFIX`), which is required rather than tidy: own-sequence
/// numbers are node-local, so an entry copied to another node would name a
/// position that means something different there.
const REPL_UPIDX_PREFIX: &[u8] = b"\x00flint\x00upidx\x00";

/// One index entry per this many upstream sequences.
///
/// Chosen by measurement, not by feel. The lookup has two costs: the walk
/// after the hint (which the stride bounds) and RocksDB's own seek to the
/// starting position (which it does not). At a 100 k stride the walk was
/// ~134 ms of a 394 ms lookup at 3.6 M sequences; the rest is seek, and no
/// stride removes it. 10 k trades a 10x bigger index -- still only ~16 bytes
/// per 10 k sequences, ~24 KB at the 15 M cursor that produced the 6.9 s
/// failover -- for a walk small enough to disappear next to the seek.
const REPL_UPIDX_STRIDE: u64 = 10_000;

/// Key for an upstream position. Big-endian so lexicographic order over the
/// keyspace IS numeric order, which is what makes the reverse seek below find
/// the newest entry at or before a position.
fn upidx_key(upstream_seq: u64) -> Vec<u8> {
    let mut k = REPL_UPIDX_PREFIX.to_vec();
    k.extend_from_slice(&upstream_seq.to_be_bytes());
    k
}

/// Result of one bounded walk in [`RocksKv::own_seq_for_upstream`].
enum ScanOutcome {
    /// The first batch whose cursor row reaches the requested position.
    Found(u64),
    /// The walk began past the answer, so a match here would not be the
    /// FIRST match. Caller must restart from the beginning.
    StartedTooLate,
    /// Ran off the end without a match.
    Exhausted,
}

/// One logical WAL operation (default column family only — the v0 engine
/// keeps its whole keyspace there by design).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// One master WriteBatch worth of ops (after floor-trimming and
/// system-row filtering). `first_seq..=last_seq` is the master sequence
/// span this batch covers — ops may be fewer than the span (filtered
/// system rows still consume sequence numbers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplBatch {
    pub first_seq: u64,
    pub last_seq: u64,
    pub ops: Vec<ReplOp>,
}

#[derive(Debug)]
pub enum ReplError {
    /// The WAL no longer reaches back to the requested sequence (purged).
    /// The replica must full-sync from a checkpoint.
    WalGap(String),
    /// A batch does not start where the cursor ends: frames were lost or
    /// reordered. The replica must drop the link and re-request from its
    /// durable cursor.
    SequenceGap {
        expected: u64,
        got: u64,
    },
    Storage(String),
}

/// Node-local system rows (manifest role/claims, the repl cursor itself)
/// must NOT replicate: they are this node's identity, not data. They still
/// consume sequence numbers, so filtering keeps the span but drops the op.
const SYSTEM_PREFIX: &[u8] = b"\x00flint\x00";

struct OpCollector {
    /// Sequence of the op about to be visited (pre-incremented).
    seq: u64,
    /// Ops with seq <= floor are already applied and dropped.
    floor: u64,
    ops: Vec<ReplOp>,
}

impl WriteBatchIterator for OpCollector {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.seq += 1;
        if self.seq > self.floor && !key.starts_with(SYSTEM_PREFIX) {
            self.ops.push(ReplOp::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            });
        }
    }

    fn delete(&mut self, key: &[u8]) {
        self.seq += 1;
        if self.seq > self.floor && !key.starts_with(SYSTEM_PREFIX) {
            self.ops.push(ReplOp::Delete { key: key.to_vec() });
        }
    }
}

/// Byte budget per `updates_since_budgeted` poll on serving loops. Small
/// enough that a poll's working set (the batches plus their encoded frames)
/// stays in the tens of MB; the caller's poll loop supplies continuation.
pub const REPL_TAIL_BUDGET_BYTES: usize = 4 * 1024 * 1024;

impl RocksKv {
    /// Master side: every op after `last_applied`, grouped per WAL batch.
    /// Sequence-idempotent per the module contract.
    ///
    /// UNBUDGETED: materializes the whole tail, which for a laggard near
    /// the WAL retention limit is gigabytes. Serving loops that poll must
    /// use `updates_since_budgeted`; this form is for tests and callers
    /// that know the tail is small.
    pub fn updates_since(&self, last_applied: u64) -> Result<Vec<ReplBatch>, ReplError> {
        self.updates_since_budgeted(last_applied, usize::MAX)
    }

    /// Like `updates_since`, but stops after the first batch that brings
    /// the accumulated op payload (key + value bytes) to `max_bytes`.
    /// Batches are never split, so the result is always applied/shipped
    /// whole; the caller resumes from the last batch's `last_seq` on its
    /// next poll and converges without any extra signal.
    pub fn updates_since_budgeted(
        &self,
        last_applied: u64,
        max_bytes: usize,
    ) -> Result<Vec<ReplBatch>, ReplError> {
        if self.db().latest_sequence_number() <= last_applied {
            return Ok(Vec::new());
        }
        let iter = self
            .db()
            .get_updates_since(last_applied)
            .map_err(|e| ReplError::WalGap(e.to_string()))?;
        let mut out = Vec::new();
        let mut bytes = 0usize;
        // The RAW start of the first batch we are handed, before the clamp
        // below hides it. A gap has TWO shapes and the empty-iterator check
        // at the end only sees one of them (docs/bugs/0031).
        let mut raw_first: Option<u64> = None;
        for item in iter {
            let (first_seq, batch) = item.map_err(|e| ReplError::Storage(e.to_string()))?;
            raw_first.get_or_insert(first_seq);
            let mut collector = OpCollector {
                seq: first_seq - 1,
                floor: last_applied,
                ops: Vec::new(),
            };
            batch.iterate(&mut collector);
            let last_seq = collector.seq;
            if last_seq <= last_applied {
                continue; // entire batch already applied
            }
            // The span starts after the floor even when the batch began
            // below it (partial batch at the cursor boundary). A batch whose
            // ops were ALL filtered still ships (empty) so the cursor
            // advances and contiguity holds.
            bytes += collector
                .ops
                .iter()
                .map(|op| match op {
                    ReplOp::Put { key, value } => key.len() + value.len(),
                    ReplOp::Delete { key } => key.len(),
                })
                .sum::<usize>();
            out.push(ReplBatch {
                first_seq: first_seq.max(last_applied + 1),
                last_seq,
                ops: collector.ops,
            });
            if bytes >= max_bytes {
                break;
            }
        }
        // We know there are newer sequences (checked above), so producing no
        // batch at all means the WAL cannot reach back to `last_applied`:
        // RocksDB hands out an iterator that simply yields nothing when the
        // requested sequence lives in a segment that has been recycled.
        //
        // Reporting that as "no updates" is how a replica ends up frozen at a
        // stale cursor while the master still counts it as live — seq_lag
        // climbing, live_replicas 1, and not one error on either side. It is
        // the same condition as the explicit gap below, so it gets the same
        // answer: the replica must full-sync.
        if out.is_empty() {
            // AN EMPTY ITERATOR HAS TWO CAUSES AND THIS USED TO ASSUME ONE.
            //
            // `get_updates_since(seq)` yields only batches whose first_seq is
            // GREATER than seq — measured, not assumed: against a single batch
            // spanning 2..=9, cursor 1 yields it and cursors 2, 3, 8 and 9
            // yield nothing. So a cursor resting anywhere INSIDE a batch's
            // span, rather than on its end, gets an empty iterator while the
            // data sits in the live WAL one segment away.
            //
            // Reading that as "the WAL was recycled" sent a healthy replica to
            // a full re-seed 18 times on the playground (BUG-0050), always
            // "one sequence short", at any depth, and unaffected by the two
            // retention raises that were tried as fixes.
            //
            // So ASK, rather than infer. The scan is the same full-WAL walk
            // `own_seq_for_upstream` already accepts as "bounded by retention
            // and paid once", and here it is paid only on a path that was
            // previously an unconditional re-seed — strictly cheaper than
            // what it replaces.
            if let Some(covering) = self.batch_covering(last_applied)? {
                // The WAL DOES reach back. Serve the covering batch clamped to
                // the cursor: its remaining ops may be empty, and that is the
                // point — the batch still ships so the cursor ADVANCES to a
                // real batch end and the link unfreezes. Returning Ok(vec![])
                // here instead would fix the false re-seed and leave the
                // replica pinned at an interior sequence forever, which is the
                // frozen-cursor failure the comment above exists to prevent.
                return Ok(vec![covering]);
            }
            return Err(ReplError::WalGap(format!(
                "sequence {} is no longer in the WAL (latest is {})",
                last_applied + 1,
                self.db().latest_sequence_number()
            )));
        }
        // THE OTHER SHAPE OF THE SAME GAP (docs/bugs/0031). The check above
        // catches a cursor so old the WAL yields NOTHING. It cannot catch a
        // cursor whose successor was recycled while LATER batches survive:
        // the iterator then hands back a batch that starts PAST the sequence
        // we need, `out` is non-empty, and this returned Ok.
        //
        // That Ok is what the admission term in FLINTSYNC calls to decide
        // whether a marked replica may warm-rejoin (BUG-0015). So the master
        // said "your copy is fine", the replica CLEARED its NEEDS_RESEED
        // marker on the strength of it, attached, and only then did the
        // stream's contiguity check see the hole and exit — re-marking on the
        // way out. The next start repeated it: a livelock that ran for 13
        // minutes on the playground and needed a data-dir wipe by hand.
        //
        // The two checks were asking different questions. "Are there batches
        // after my cursor" is not "does the span START where I need it", and
        // only the second one is admission. `first_seq.max(last_applied + 1)`
        // above deliberately clamps a batch that BEGINS below the floor —
        // legitimate at the cursor boundary — which is why the raw value has
        // to be captured before the clamp rather than read back from `out`.
        if let Some(first) = raw_first
            && first > last_applied + 1
        {
            return Err(ReplError::WalGap(format!(
                "oldest retained batch starts at {first}, past the {} needed                  (latest is {})",
                last_applied + 1,
                self.db().latest_sequence_number()
            )));
        }
        Ok(out)
    }

    /// Replica side: apply one batch atomically together with the cursor.
    ///
    /// Idempotence is enforced here, not assumed: physical replay is only
    /// safe when monotonic (re-applying an OLD batch would regress
    /// overwritten keys and the cursor with them). Stale batches are
    /// no-ops; a batch that does not start exactly at cursor+1 is a
    /// SequenceGap — the replica must re-request from its durable cursor.
    pub fn apply_batch(&self, batch: &ReplBatch) -> Result<(), ReplError> {
        let cursor = self.last_applied();
        if batch.last_seq <= cursor {
            return Ok(()); // already applied: idempotent no-op
        }
        if batch.first_seq != cursor + 1 {
            return Err(ReplError::SequenceGap {
                expected: cursor + 1,
                got: batch.first_seq,
            });
        }
        let mut wb = WriteBatch::default();
        for op in &batch.ops {
            match op {
                ReplOp::Put { key, value } => wb.put(key, value),
                ReplOp::Delete { key } => wb.delete(key),
            }
        }
        wb.put(REPL_STATE_KEY, batch.last_seq.to_be_bytes());
        self.db()
            .write(wb)
            .map_err(|e| ReplError::Storage(e.to_string()))?;
        // Index AFTER the commit, because the value being recorded is the own
        // sequence the batch just landed at -- which does not exist until it
        // has. A separate put costs one more sequence; the walk counts ops
        // generically, so that is invisible to it.
        //
        // Best-effort on purpose: a failed index write leaves a gap that the
        // next stride fills, and a lookup that lands before its target simply
        // walks further. Failing the APPLY over a hint would trade a durable
        // write path for a performance aid.
        if batch.last_seq / REPL_UPIDX_STRIDE != cursor / REPL_UPIDX_STRIDE {
            let own = self.db().latest_sequence_number();
            let _ = self.db().put(upidx_key(batch.last_seq), own.to_be_bytes());
        }
        Ok(())
    }

    /// The engine's newest sequence number (master-side lag reference).
    pub fn latest_seq(&self) -> u64 {
        self.db().latest_sequence_number()
    }

    /// Map a position in this node's UPSTREAM stream to this node's OWN
    /// sequence space.
    ///
    /// Sequence numbers are node-local: a replica applies each upstream
    /// batch together with its cursor row in one WriteBatch, so its own
    /// numbers run ahead of the upstream position it tracks. A cursor from
    /// the old master's space therefore does not index this node's WAL —
    /// the mistake soak run 30 measured: a rewound ex-master presented its
    /// snapshot's (old-space) seq, the freshly promoted master served its
    /// own WAL from that number, and the stream landed off-position (a
    /// SequenceGap when the strict check caught it; silent identical-value
    /// replays when it did not).
    ///
    /// The mapping is already durable in this node's WAL: every apply batch
    /// ends with the cursor-row put whose VALUE is the upstream seq it
    /// reached. Scan the retained WAL from its start until the FIRST apply
    /// batch that reached `upstream_seq`, and return that batch's own-space
    /// last seq — the position a tailer of THIS node resumes from. A scan
    /// that runs out of WAL or past the applies without finding the cursor
    /// row is a WalGap: the caller full-syncs, the safe default.
    ///
    /// From the START, not from `upstream_seq`. The first version began at
    /// `get_updates_since(upstream_seq - 1)` on the assumption that
    /// own-space always runs AHEAD of upstream-space (every apply batch
    /// adds a cursor row). That holds only for a node whose whole history
    /// is applies of that one stream. A node that REWOUND to its own
    /// older-space snapshot tails a lineage whose numbers dwarf its own —
    /// soak run 35 cycle 4: own tip 2.6M, upstream cursor 6.08M — so the
    /// "optimized" start pointed past the end of this WAL, RocksDB said
    /// "not yet written", and a perfectly mappable cursor was refused into
    /// a 49-second full re-seed with the widow gate holding every write.
    /// And when own-space has grown past the number again (a later
    /// mastership), the same start lands BEYOND the mapping row and either
    /// misses it or finds a later epoch's row — a silent overshoot. The
    /// full scan is bounded by WAL retention and paid once per rewind
    /// attach; correctness is worth it.
    pub fn own_seq_for_upstream(&self, upstream_seq: u64) -> Result<u64, ReplError> {
        struct CursorFind {
            seq: u64,
            reached: Option<u64>,
        }
        impl WriteBatchIterator for CursorFind {
            fn put(&mut self, key: &[u8], value: &[u8]) {
                self.seq += 1;
                if key == REPL_STATE_KEY {
                    self.reached = value.try_into().ok().map(u64::from_be_bytes);
                }
            }
            fn delete(&mut self, _key: &[u8]) {
                self.seq += 1;
            }
        }
        // The walk, startable from an arbitrary own sequence.
        //
        // `verify_start` is what makes an untrusted hint safe. `get_updates_since`
        // SKIPS the batch at its starting position (this module's header), so a
        // hint landing ON the answer's batch steps over it and the scan happily
        // returns the NEXT qualifying batch -- a LATER position, from which a
        // replica would resume past data it never applied. A failed scan was
        // never the danger; a successful wrong one was.
        //
        // So: the first cursor-bearing batch we see must still be BELOW the
        // target. Cursor rows are monotone, so that proves we started early
        // enough that no earlier batch could qualify, which is exactly the
        // claim "the first match found is THE first match". If it does not
        // hold we started too late and say so, and the caller walks from 0.
        let scan = |from: u64, verify_start: bool| -> Result<ScanOutcome, ReplError> {
            let iter = self
                .db()
                .get_updates_since(from)
                .map_err(|e| ReplError::WalGap(e.to_string()))?;
            let mut checked = !verify_start;
            for item in iter {
                let (first_seq, batch) = item.map_err(|e| ReplError::Storage(e.to_string()))?;
                let mut find = CursorFind {
                    seq: first_seq - 1,
                    reached: None,
                };
                batch.iterate(&mut find);
                let Some(reached) = find.reached else {
                    continue; // no cursor row: says nothing either way
                };
                if !checked {
                    checked = true;
                    if reached >= upstream_seq {
                        return Ok(ScanOutcome::StartedTooLate);
                    }
                }
                if reached >= upstream_seq {
                    return Ok(ScanOutcome::Found(find.seq));
                }
            }
            Ok(ScanOutcome::Exhausted)
        };

        // BUG-0070: start at the newest index entry STRICTLY BELOW the target
        // rather than at sequence 0, bounding the walk to about one stride.
        // Strictly below, because an entry landing exactly on the target names
        // the batch the iterator will skip.
        let hint = self.upidx_hint(upstream_seq);
        if hint > 0
            && let ScanOutcome::Found(found) = scan(hint, true)?
        {
            return Ok(found);
        }
        match scan(0, false)? {
            ScanOutcome::Found(found) => Ok(found),
            _ => Err(ReplError::WalGap(format!(
                "no apply batch reaching upstream seq {upstream_seq} is retained in this WAL"
            ))),
        }
    }

    /// Newest indexed OWN sequence for an upstream position STRICTLY BELOW
    /// `upstream_seq`, or 0 when the index has nothing to offer (an un-indexed
    /// span, a fleet older than the index, a node that was never a replica).
    /// Zero means "walk from the beginning" -- the behaviour before the index.
    fn upidx_hint(&self, upstream_seq: u64) -> u64 {
        let Some(below) = upstream_seq.checked_sub(1) else {
            return 0;
        };
        let key = upidx_key(below);
        let mut it = self.db().iterator(rocksdb::IteratorMode::From(
            &key,
            rocksdb::Direction::Reverse,
        ));
        match it.next() {
            // Reverse-from lands on the greatest key <= `key`, and big-endian
            // encoding makes that the greatest upstream position <= it.
            Some(Ok((k, v))) if k.starts_with(REPL_UPIDX_PREFIX) && v.len() == 8 => {
                u64::from_be_bytes(v.as_ref().try_into().unwrap_or([0u8; 8]))
            }
            _ => 0,
        }
    }

    /// The retained batch whose span COVERS `cursor`, clamped to it, or None
    /// if no retained batch does.
    ///
    /// Only ever called on the empty-iterator path, where the alternative was
    /// an unconditional full re-seed. A hit means the cursor is interior to a
    /// batch rather than past the end of the WAL — the data is present and the
    /// replica must not be re-seeded. A miss means the WAL genuinely cannot
    /// reach back, and the caller raises WalGap exactly as before.
    ///
    /// Returns the batch with `first_seq` clamped to `cursor + 1` and only the
    /// ops above the cursor, so applying it is idempotent with whatever the
    /// replica already has and leaves it on a real batch end.
    fn batch_covering(&self, cursor: u64) -> Result<Option<ReplBatch>, ReplError> {
        let iter = self
            .db()
            .get_updates_since(0)
            .map_err(|e| ReplError::WalGap(e.to_string()))?;
        for item in iter {
            let (first_seq, batch) = item.map_err(|e| ReplError::Storage(e.to_string()))?;
            if first_seq > cursor {
                // Batches are handed out in order; past the cursor means no
                // earlier batch can cover it.
                break;
            }
            let mut collector = OpCollector {
                seq: first_seq - 1,
                floor: cursor,
                ops: Vec::new(),
            };
            batch.iterate(&mut collector);
            if collector.seq > cursor {
                return Ok(Some(ReplBatch {
                    first_seq: first_seq.max(cursor + 1),
                    last_seq: collector.seq,
                    ops: collector.ops,
                }));
            }
        }
        Ok(None)
    }

    /// Set the replica cursor directly (after a checkpoint full sync, the
    /// copied DB's own latest sequence IS the master cursor).
    ///
    /// SNAPPED TO A BATCH END, because an interior cursor is a link that
    /// cannot advance (BUG-0050). `get_updates_since(seq)` yields only batches
    /// whose first_seq exceeds `seq`, so a cursor resting inside a batch's
    /// span is offered nothing and reads as a recycled WAL. A cursor obtained
    /// by APPLYING a batch is always that batch's end and is safe; the four
    /// call sites here set one from elsewhere — a checkpoint's own latest, a
    /// translated cursor adopted from a master — and nothing about those
    /// guarantees a boundary.
    ///
    /// This is the cause-side half of the fix. `updates_since` also RECOVERS
    /// from an interior cursor now, so neither half is load-bearing alone:
    /// this one stops them being written, that one survives any that already
    /// exist on disk or arrive from an older peer.
    ///
    /// Snapping FORWARD to the batch end, never backward. The ops between the
    /// requested seq and the batch end were all part of one atomic write, so a
    /// replica that has the batch has all of them; advancing the cursor skips
    /// nothing. Retreating would replay ops the replica already holds, which
    /// is safe for puts and wrong for a delete followed by a put of the same
    /// key.
    pub fn set_last_applied(&self, seq: u64) -> Result<(), ReplError> {
        let snapped = self.snap_to_batch_end(seq).unwrap_or(seq);
        self.db()
            .put(REPL_STATE_KEY, snapped.to_be_bytes())
            .map_err(|e| ReplError::Storage(e.to_string()))
    }

    /// The end of the retained batch containing `seq`, or None when no
    /// retained batch covers it (already a boundary, or past the WAL's reach).
    /// Best-effort: a None leaves the caller's value untouched, because a
    /// cursor this cannot classify is exactly the case `updates_since` now
    /// handles on the read side.
    fn snap_to_batch_end(&self, seq: u64) -> Option<u64> {
        let iter = self.db().get_updates_since(0).ok()?;
        for item in iter {
            let (first_seq, batch) = item.ok()?;
            if first_seq > seq {
                break;
            }
            let mut find = OpCollector {
                seq: first_seq - 1,
                floor: u64::MAX, // count only; collect nothing
                ops: Vec::new(),
            };
            batch.iterate(&mut find);
            if find.seq > seq {
                return Some(find.seq);
            }
        }
        None
    }

    /// Replica cursor, surviving restarts. 0 = nothing applied.
    pub fn last_applied(&self) -> u64 {
        self.db()
            .get(REPL_STATE_KEY)
            .ok()
            .flatten()
            .and_then(|v| v.try_into().ok().map(u64::from_be_bytes))
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Kv;
    use crate::strings::{SetOptions, StringStore, system_clock};

    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new(tag: &str) -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let p = std::env::temp_dir().join(format!(
                "flint-repl-{tag}-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&p);
            Self(p)
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The index is a HINT. This pins the property everything else rests on:
    /// whatever the hint says -- right, stale, absent, or actively wrong --
    /// the answer must be the one the plain walk gives. Adversarial rather
    /// than happy-path on purpose: a hint that merely works when correct is
    /// not the claim being made.
    #[test]
    fn a_hint_never_changes_the_answer() {
        let d = TempDir::new("upidxhint");
        let kv = RocksKv::open(&d.0).expect("open");
        // A handful of applied batches, so real cursor rows exist to be found.
        let mut upstream = 0u64;
        for b in 0..8u64 {
            let ops: Vec<ReplOp> = (0..5u32)
                .map(|i| ReplOp::Put {
                    key: format!("k{b}-{i}").into_bytes(),
                    value: b"v".to_vec(),
                })
                .collect();
            let batch = ReplBatch {
                first_seq: upstream + 1,
                last_seq: upstream + ops.len() as u64,
                ops,
            };
            upstream = batch.last_seq;
            kv.apply_batch(&batch).expect("apply");
        }
        let target = 20u64;
        // Truth, with an empty index (no entry was written: the stride is far
        // above anything this fixture reaches, which the assert below pins).
        assert_eq!(kv.upidx_hint(target), 0, "fixture unexpectedly indexed");
        let truth = kv.own_seq_for_upstream(target).expect("baseline");

        // Now poison the index in every direction that matters.
        let latest = kv.db().latest_sequence_number();
        for (label, at, own) in [
            ("far too early", 1u64, 1u64),
            ("one before the target", target - 1, 2),
            ("exactly the target", target, truth),
            ("past the answer", target, latest),
            ("beyond the WAL entirely", target, latest + 10_000),
        ] {
            kv.db()
                .put(upidx_key(at), own.to_be_bytes())
                .expect("plant");
            assert_eq!(
                kv.own_seq_for_upstream(target).expect("with hint"),
                truth,
                "hint '{label}' (upstream {at} -> own {own}) changed the answer"
            );
        }
    }

    /// POSITIVE CONTROL on the writer: crossing a stride must actually record
    /// an entry. Without this the hint test above passes vacuously forever on
    /// an index nothing ever writes -- which is precisely how the retention
    /// "fix" this replaced went unnoticed.
    ///
    /// The stride is crossed by moving the UPSTREAM cursor, not by writing
    /// 100 k rows: the boundary is a property of the upstream sequence, so a
    /// six-op batch landing across it exercises the same branch in
    /// milliseconds.
    #[test]
    fn crossing_a_stride_records_an_index_entry() {
        let d = TempDir::new("upidxwrite");
        let kv = RocksKv::open(&d.0).expect("open");
        kv.set_last_applied(REPL_UPIDX_STRIDE - 1).expect("seed");
        assert_eq!(
            kv.upidx_hint(REPL_UPIDX_STRIDE + 5),
            0,
            "nothing should be indexed before the crossing"
        );
        let ops: Vec<ReplOp> = (0..6u32)
            .map(|i| ReplOp::Put {
                key: format!("s{i}").into_bytes(),
                value: b"v".to_vec(),
            })
            .collect();
        let batch = ReplBatch {
            first_seq: REPL_UPIDX_STRIDE,
            last_seq: REPL_UPIDX_STRIDE + 5,
            ops,
        };
        kv.apply_batch(&batch).expect("apply");
        // Queried from ABOVE the entry: `upidx_hint` selects strictly below
        // its argument, because an entry landing exactly on the target names
        // the batch `get_updates_since` will skip.
        let hint = kv.upidx_hint(REPL_UPIDX_STRIDE + 100);
        assert!(hint > 0, "crossing a stride recorded no index entry");
        assert_eq!(
            kv.upidx_hint(REPL_UPIDX_STRIDE + 5),
            0,
            "an entry AT the target must not be offered as a hint"
        );
        // And it must point at a real position in THIS node's space.
        assert!(
            hint <= kv.db().latest_sequence_number(),
            "index entry {hint} is past the latest sequence"
        );
    }

    /// Index rows must never reach another node: own-sequence numbers are
    /// node-local, so a copied entry names a position that means something
    /// else there. That exclusion is the `SYSTEM_PREFIX` filter the streaming
    /// path already applies and `system_rows_do_not_replicate_but_advance_the_cursor`
    /// already proves; what is asserted here is that these keys are INSIDE it.
    #[test]
    fn index_rows_sit_under_the_non_replicating_prefix() {
        assert!(
            upidx_key(0).starts_with(SYSTEM_PREFIX),
            "index keys must carry the system prefix or they will replicate"
        );
        assert!(upidx_key(u64::MAX).starts_with(SYSTEM_PREFIX));
        // Big-endian ordering is what makes the reverse seek mean "newest at
        // or before". A little-endian key would still look fine in isolation.
        assert!(upidx_key(1) < upidx_key(2));
        assert!(upidx_key(255) < upidx_key(256));
        assert!(upidx_key(u64::MAX - 1) < upidx_key(u64::MAX));
    }

    /// BUG-0070 COST PROBE. The shipped fix assumed an asymmetry it never
    /// measured: that `get_updates_since(0)` matches the first WAL file
    /// immediately while a HIGH cursor walks to position, so a bounds check
    /// would be cheap where positioning was not. The fleet says otherwise --
    /// probe cost still scaled with WAL size after the fix (3,073 ms at cursor
    /// 2.95 M, against 2,354 ms at 2.8 M before it).
    ///
    /// This asks the question locally, where nothing else is moving, and
    /// separates the two candidate costs:
    ///   * CONSTRUCT  -- building the iterator (enumerating/opening WAL files)
    ///   * FIRST      -- advancing to the first batch (seeking to a position)
    ///
    /// If construct dominates and is equal for both start positions, the
    /// asymmetry does not exist and no bounds check phrased in terms of this
    /// API can be cheap. (It did, and it does not -- see the correction in
    /// docs/bugs/0070. The cost is the WALK, which is neither of these, and
    /// the third arm below is what finally measured it.)
    ///
    /// Prints, asserts nothing about the answer. It DOES assert the fixture
    /// reached a production-like shape (many WAL files), because a single-file
    /// WAL would answer a different question cheaply and look like good news.
    #[test]
    #[ignore = "measurement: writes millions of sequences; run with --release --ignored"]
    fn bug_0070_probe_wal_scan_cost_by_start_position() {
        use std::time::Instant;
        let d = TempDir::new("walcost");
        // Long TTL / big cap: nothing must be pruned mid-run, so the only
        // variable is how much WAL is retained.
        let kv = RocksKv::open_with_retention(&d.0, 24 * 3600, 64 * 1024).expect("open");
        let st = StringStore::new(&kv, b"ns", system_clock);

        let count_wal = || {
            let live = std::fs::read_dir(&d.0)
                .map(|r| {
                    r.filter_map(Result::ok)
                        .filter(|e| e.file_name().to_string_lossy().ends_with(".log"))
                        .count()
                })
                .unwrap_or(0);
            let arch = std::fs::read_dir(d.0.join("archive"))
                .map(|r| r.filter_map(Result::ok).count())
                .unwrap_or(0);
            (live, arch)
        };

        eprintln!(
            "{:>10} {:>8} {:>7} {:>12} {:>12} {:>12} {:>12} {:>12}",
            "seqs", "wal", "arch", "cons(0)ms", "first(0)ms", "cons(hi)ms", "first(hi)ms", "WALKms"
        );
        // VALUE SIZE MATTERS and defaulted wrong the first time: at 1 byte a
        // 3 M-sequence WAL is 60 small files, while the fleet writes 1 KiB and
        // retains gigabytes. WAL retention is a TTL/SIZE budget, so the file
        // count and bytes -- not the sequence count -- are what a scan meets.
        let vsize: usize = std::env::var("FLINT_PROBE_VALUE_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1);
        let val = vec![b'x'; vsize];
        let targets: Vec<u64> = std::env::var("FLINT_PROBE_TARGETS")
            .ok()
            .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
            .unwrap_or_else(|| vec![100_000, 500_000, 1_000_000, 2_000_000, 3_000_000]);
        eprintln!("  value_bytes={vsize}");
        let mut written = 0u64;
        for target in targets {
            while written < target {
                for _ in 0..1_000 {
                    st.set(
                        1,
                        format!("k{written}").as_bytes(),
                        &val,
                        SetOptions::default(),
                    )
                    .expect("set");
                    written += 1;
                }
                // Flush periodically so the WAL becomes MANY files, which is
                // the production shape. One giant live file would make the
                // enumeration hypothesis untestable.
                if written.is_multiple_of(50_000) {
                    kv.flush();
                }
            }
            let latest = kv.db().latest_sequence_number();
            let hi = latest.saturating_sub(1);

            let t = Instant::now();
            let mut it0 = kv.db().get_updates_since(0).expect("iter0");
            let c0 = t.elapsed();
            let t = Instant::now();
            let _ = it0.next();
            let f0 = t.elapsed();
            drop(it0);

            let t = Instant::now();
            let mut ith = kv.db().get_updates_since(hi).expect("iterhi");
            let ch = t.elapsed();
            let t = Instant::now();
            let _ = ith.next();
            let fh = t.elapsed();
            drop(ith);

            // The WALK, not the construct: this is what the FLINTSYNC handler
            // actually pays on the rewind path, and what arms 1-2 could not see.
            let t = Instant::now();
            let _ = kv.own_seq_for_upstream(hi);
            let walk = t.elapsed().as_secs_f64() * 1000.0;

            let (live, arch) = count_wal();
            eprintln!(
                "{:>10} {:>8} {:>7} {:>12.1} {:>12.1} {:>12.1} {:>12.1} {:>12.1}",
                latest,
                live,
                arch,
                c0.as_secs_f64() * 1000.0,
                f0.as_secs_f64() * 1000.0,
                ch.as_secs_f64() * 1000.0,
                fh.as_secs_f64() * 1000.0,
                walk
            );
        }
        // THIRD ARM, and the one that matters: `own_seq_for_upstream` does not
        // stop at the first batch -- it ITERATES every batch and every op from
        // sequence 0 until it finds the cursor row. Construction being cheap
        // (arms 1-2) says nothing about that walk, which is the mistake this
        // probe was written to correct.
        //
        // This fixture is a standalone master with no REPL_STATE_KEY rows, so
        // the lookup MISSES and walks the whole WAL. That is the right shape:
        // on the fleet the translated cursor sits near the tip (15,037,880 of
        // ~15.1 M), so the real scan is nearly the full walk too.
        {
            let latest = kv.db().latest_sequence_number();
            let t = Instant::now();
            let r = kv.own_seq_for_upstream(latest.saturating_sub(1));
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            eprintln!(
                "own_seq_for_upstream(latest-1) over {latest} seqs: {ms:.1}ms  (result: {})",
                match &r {
                    Ok(v) => format!("Ok({v})"),
                    Err(e) => format!("{e:?}"),
                }
            );
        }

        // SECOND ARM: the same two calls while a writer hammers the DB. The
        // first arm measured an IDLE database; the fleet's master is ingesting
        // continuously and refusing writes at the moment the probe runs, and
        // that difference is the only one the first arm cannot see.
        {
            use std::sync::Arc;
            use std::sync::atomic::{AtomicBool, Ordering};
            let stop = Arc::new(AtomicBool::new(false));
            let kv2 = Arc::new(kv);
            let w_stop = stop.clone();
            let w_kv = kv2.clone();
            let writer = std::thread::spawn(move || {
                let ws = StringStore::new(&*w_kv, b"ns", system_clock);
                let mut i = 10_000_000u64;
                while !w_stop.load(Ordering::Relaxed) {
                    for _ in 0..200 {
                        let _ = ws.set(1, format!("h{i}").as_bytes(), b"v", SetOptions::default());
                        i += 1;
                    }
                }
                i
            });
            // Let the writer get going so the measurement lands under load.
            for _ in 0..40 {
                std::thread::yield_now();
            }
            let mut worst0 = 0.0f64;
            let mut worsth = 0.0f64;
            for _ in 0..20 {
                let latest = kv2.db().latest_sequence_number();
                let hi = latest.saturating_sub(1);
                let t = Instant::now();
                let mut it0 = kv2.db().get_updates_since(0).expect("iter0");
                let _ = it0.next();
                let e0 = t.elapsed().as_secs_f64() * 1000.0;
                drop(it0);
                let t = Instant::now();
                let mut ith = kv2.db().get_updates_since(hi).expect("iterhi");
                let _ = ith.next();
                let eh = t.elapsed().as_secs_f64() * 1000.0;
                drop(ith);
                worst0 = worst0.max(e0);
                worsth = worsth.max(eh);
            }
            stop.store(true, Ordering::Relaxed);
            let ended = writer.join().expect("writer");
            eprintln!(
                "UNDER LOAD (writer active, {} writes issued): worst from(0)={worst0:.1}ms  worst from(hi)={worsth:.1}ms",
                ended - 10_000_000
            );
            let (l2, a2) = count_wal();
            eprintln!("  wal files after load arm: live={l2} archive={a2}");
        }

        // FOURTH ARM -- the fix, measured the same way the bug was. A
        // replica-shaped DB (real apply batches, so real cursor rows and real
        // index entries), translated near the tip, with the index present and
        // then with it deleted. The answers must be IDENTICAL: this is the
        // differential check at realistic scale, not just in the unit tests.
        {
            let d2 = TempDir::new("upidxbench");
            let kv2 = RocksKv::open(&d2.0).expect("open2");
            let mut upstream = 0u64;
            let goal: u64 = std::env::var("FLINT_PROBE_APPLY_SEQS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2_400_000);
            while upstream < goal {
                // Same value size as the arms above: the walk's cost is
                // dominated by WAL BYTES read, not by op count, so a fixture
                // with tiny values understates the gain by ~40x.
                let ops: Vec<ReplOp> = (0..1_000u32)
                    .map(|i| ReplOp::Put {
                        key: format!("a{upstream}-{i}").into_bytes(),
                        value: val.clone(),
                    })
                    .collect();
                let b = ReplBatch {
                    first_seq: upstream + 1,
                    last_seq: upstream + ops.len() as u64,
                    ops,
                };
                upstream = b.last_seq;
                kv2.apply_batch(&b).expect("apply");
            }
            let target = upstream.saturating_sub(5_000);

            let t = Instant::now();
            let with_idx = kv2.own_seq_for_upstream(target);
            let ms_with = t.elapsed().as_secs_f64() * 1000.0;

            // Strip the index and ask again: this is the pre-fix path.
            let mut keys = Vec::new();
            let it = kv2.db().iterator(rocksdb::IteratorMode::From(
                REPL_UPIDX_PREFIX,
                rocksdb::Direction::Forward,
            ));
            for row in it.flatten() {
                if !row.0.starts_with(REPL_UPIDX_PREFIX) {
                    break;
                }
                keys.push(row.0.to_vec());
            }
            let planted = keys.len();
            for k in keys {
                kv2.db().delete(k).expect("del");
            }
            assert!(
                planted > 0,
                "no index entries were written: this arm would compare the \
                 full walk against itself and report a speedup of 1x"
            );

            let t = Instant::now();
            let without = kv2.own_seq_for_upstream(target);
            let ms_without = t.elapsed().as_secs_f64() * 1000.0;

            assert_eq!(
                format!("{with_idx:?}"),
                format!("{without:?}"),
                "the index CHANGED the answer at {upstream} sequences"
            );
            eprintln!(
                "own_seq_for_upstream near tip of {upstream} upstream seqs \
                 ({planted} index entries): indexed={ms_with:.1}ms  \
                 unindexed={ms_without:.1}ms  speedup={:.1}x",
                if ms_with > 0.0 {
                    ms_without / ms_with
                } else {
                    0.0
                }
            );
        }

        let (live, arch) = count_wal();
        assert!(
            live + arch >= 8,
            "fixture never produced a multi-file WAL (live={live} archive={arch}): \
             this run measured a shape production does not have"
        );
    }

    /// BUG-0050 PROBE. Is a cursor that has fallen out of the LIVE WAL, but
    /// is still inside the ARCHIVED window that `wal_ttl_seconds` retains,
    /// reachable by the tail path?
    ///
    /// Measured on the playground 2026-08-26: the master held 735 archived
    /// segments spanning 6 h 1 m, and the admission check reported a
    /// reachable span of ~357 sequences — about ten seconds at that fleet's
    /// write rate. Six hours kept, ten seconds served. This asks the question
    /// locally, where nothing else is moving.
    ///
    /// It is deliberately not an assertion about which answer is CORRECT: it
    /// prints and asserts only that the probe actually exercised the path
    /// (segments were archived, and the cursor really did leave the live
    /// WAL). What the tail does with such a cursor is the finding.
    #[test]
    fn bug_0050_is_an_archived_cursor_reachable() {
        let md = TempDir::new("archivereach");
        let master = RocksKv::open(&md.0).expect("open");
        let s = StringStore::new(&master, b"ns", system_clock);

        for i in 0..200u32 {
            s.set(1, format!("a{i}").as_bytes(), b"v", SetOptions::default())
                .expect("set");
        }
        let early = master.db().latest_sequence_number();

        // Flush so the memtable leaves and the segment holding `early` can be
        // archived rather than needed for recovery.
        for round in 0..4 {
            master.flush();
            for i in 0..200u32 {
                s.set(
                    1,
                    format!("b{round}-{i}").as_bytes(),
                    b"v",
                    SetOptions::default(),
                )
                .expect("set");
            }
        }
        master.flush();

        let archived = std::fs::read_dir(md.0.join("archive"))
            .map(|d| d.filter_map(Result::ok).count())
            .unwrap_or(0);
        let latest = master.db().latest_sequence_number();

        let outcome = match master.updates_since(early) {
            Ok(b) => format!("SERVED {} batch(es)", b.len()),
            Err(e) => format!("REFUSED: {e:?}"),
        };
        eprintln!(
            "BUG-0050 probe A (no checkpoint): archived={archived} early={early} latest={latest} -> {outcome}"
        );

        // PROBE B. The playground refused a cursor at snapshot_seq - 1 while
        // its oldest reachable batch sat at snapshot_seq + 1, and it takes a
        // checkpoint every ~30 s. So: does taking one move the floor?
        let ckpt = TempDir::new("ckpt");
        std::fs::create_dir_all(&ckpt.0).expect("parent");
        let mid = master.db().latest_sequence_number();
        master
            .checkpoint_to(&ckpt.0.join("snap"))
            .expect("checkpoint");
        for i in 0..200u32 {
            s.set(1, format!("d{i}").as_bytes(), b"v", SetOptions::default())
                .expect("set");
        }
        let after = master.db().latest_sequence_number();
        let out_b = match master.updates_since(early) {
            Ok(b) => format!("SERVED {} batch(es)", b.len()),
            Err(e) => format!("REFUSED: {e:?}"),
        };
        let out_c = match master.updates_since(mid.saturating_sub(1)) {
            Ok(b) => format!("SERVED {} batch(es)", b.len()),
            Err(e) => format!("REFUSED: {e:?}"),
        };
        eprintln!(
            "BUG-0050 probe B (after checkpoint at {mid}, latest {after}): \
             pre-checkpoint cursor {early} -> {out_b} | just-before-checkpoint cursor -> {out_c}"
        );

        // PROBE C. THE ACTUAL GEOMETRY OF THE FAILURE, which neither A nor B
        // reproduces: the cursor is at latest-1 — essentially caught up — and
        // the WAL rolls underneath it. On the playground that returned
        // "sequence N is no longer in the WAL (latest is N+1)", which is the
        // one shape no retention depth can explain.
        for i in 0..200u32 {
            s.set(1, format!("e{i}").as_bytes(), b"v", SetOptions::default())
                .expect("set");
        }
        let tail_cursor = master.db().latest_sequence_number() - 1;
        master.flush(); // roll: the segment holding tail_cursor leaves the live WAL
        let out_d = match master.updates_since(tail_cursor) {
            Ok(b) => format!("SERVED {} batch(es)", b.len()),
            Err(e) => format!("REFUSED: {e:?}"),
        };
        // And again with a write after the roll, so latest moves past the roll
        // point exactly as it does on a live master.
        s.set(1, b"post-roll", b"v", SetOptions::default())
            .expect("set");
        let out_e = match master.updates_since(tail_cursor) {
            Ok(b) => format!("SERVED {} batch(es)", b.len()),
            Err(e) => format!("REFUSED: {e:?}"),
        };
        eprintln!(
            "BUG-0050 probe C (cursor at latest-1 = {tail_cursor}, then roll): \
             immediately -> {out_d} | after one more write -> {out_e}"
        );

        // PROBE D. DOES A SEQUENCE EXIST THAT BEGINS NO BATCH?
        //
        // The admission check refuses when `raw_first > last_applied + 1`. On
        // the playground last_applied was 131869492 and the first offered
        // batch began at >= 131869494 — so nothing began at 131869493. RocksDB
        // advances the sequence PER KEY, so a multi-key write consumes several
        // sequences while beginning exactly one batch. Every interior sequence
        // of such a batch is a number no batch starts at.
        //
        // If a cursor can come to rest on one of those, the check reports a
        // gap over data that is present — which fits every fact: one sequence
        // short, at any depth, unaffected by retention, and rare.
        let multi: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..8u32)
            .map(|i| (format!("Mmulti{i}").into_bytes(), Some(b"v".to_vec())))
            .collect();
        let before_multi = master.db().latest_sequence_number();
        master.apply_writes(&multi).expect("multi-key write");
        let after_multi = master.db().latest_sequence_number();
        let consumed = after_multi - before_multi;

        let batches = master
            .updates_since(before_multi)
            .expect("tail after multi");
        let spans: Vec<String> = batches
            .iter()
            .map(|b| format!("{}..={}", b.first_seq, b.last_seq))
            .collect();
        // An INTERIOR sequence of the multi-key batch: no batch starts here.
        let interior = before_multi + 1;
        let out_f = match master.updates_since(interior) {
            Ok(b) => format!("SERVED {} batch(es)", b.len()),
            Err(e) => format!("REFUSED: {e:?}"),
        };
        // THE CONTRAST that makes it airtight: the same WAL, the same instant,
        // asked from the sequence just BEFORE the batch begins.
        let out_g = match master.updates_since(before_multi) {
            Ok(b) => format!("SERVED {} batch(es)", b.len()),
            Err(e) => format!("REFUSED: {e:?}"),
        };
        eprintln!(
            "BUG-0050 probe D: an 8-key write consumed {consumed} sequence(s) \
             ({before_multi} -> {after_multi}) and produced batches [{}]",
            spans.join(", ")
        );
        eprintln!("                  cursor at batch START-1 ({before_multi}) -> {out_g}");
        eprintln!("                  cursor at INTERIOR      ({interior}) -> {out_f}");

        // ARMED. If nothing was archived, or the cursor never left the live
        // WAL, the probe answered an easier question than the one asked and
        // its result means nothing either way.
        assert!(
            archived > 0,
            "nothing was archived, so this never exercised the archive path — \
             the probe is inconclusive, not reassuring"
        );
        assert!(latest > early, "the cursor did not fall behind at all");
    }

    fn scan_all(kv: &RocksKv) -> Vec<(Vec<u8>, Vec<u8>)> {
        // User rows only (skip the repl marker).
        let mut rows = kv.scan_prefix(b"M");
        rows.extend(kv.scan_prefix(b"S"));
        rows.extend(kv.scan_prefix(b"Z"));
        rows
    }

    /// Pull everything new from master and apply to replica; returns batches applied.
    fn catch_up(master: &RocksKv, replica: &RocksKv) -> usize {
        let batches = master.updates_since(replica.last_applied()).expect("tail");
        let n = batches.len();
        for b in &batches {
            replica.apply_batch(b).expect("apply");
        }
        n
    }

    #[test]
    fn full_parity_roundtrip_and_incremental_catchup() {
        let (md, rd) = (TempDir::new("master"), TempDir::new("replica"));
        let master = RocksKv::open(&md.0).expect("open master");
        let replica = RocksKv::open(&rd.0).expect("open replica");
        let s = StringStore::new(&master, b"0", system_clock);

        s.set(1, b"k1", b"v1", SetOptions::default()).expect("set");
        s.set(2, b"k2", b"v2", SetOptions::default()).expect("set");
        assert!(catch_up(&master, &replica) > 0);
        assert_eq!(
            scan_all(&master),
            scan_all(&replica),
            "byte-for-byte parity"
        );
        let cursor = replica.last_applied();
        assert!(cursor > 0);

        // Incremental: new writes only.
        s.set(1, b"k3", b"v3", SetOptions::default()).expect("set");
        s.set(1, b"k1", b"v1-updated", SetOptions::default())
            .expect("set");
        catch_up(&master, &replica);
        assert_eq!(scan_all(&master), scan_all(&replica));
        assert!(replica.last_applied() > cursor);

        // Reads through the normal store work on the replica.
        let rs = StringStore::new(&replica, b"0", system_clock);
        assert_eq!(rs.get(1, b"k1"), Ok(Some(b"v1-updated".to_vec())));
    }

    /// The budget must partition a large tail into resumable slices — each
    /// poll bounded, batches never split, and the plain poll-loop converges
    /// to full parity with no continuation signal beyond the cursor.
    #[test]
    fn budgeted_tail_is_bounded_per_poll_and_converges() {
        let (md, rd) = (TempDir::new("m-budget"), TempDir::new("r-budget"));
        let master = RocksKv::open(&md.0).expect("open");
        let replica = RocksKv::open(&rd.0).expect("open");
        let s = StringStore::new(&master, b"0", system_clock);
        let value = vec![b'x'; 1_000];
        for i in 0..200u32 {
            s.set(
                1,
                format!("k{i:04}").as_bytes(),
                &value,
                SetOptions::default(),
            )
            .expect("set");
        }

        const BUDGET: usize = 8 * 1_000; // ~8 ops per poll against ~200 total
        let mut polls = 0;
        loop {
            let batches = master
                .updates_since_budgeted(replica.last_applied(), BUDGET)
                .expect("tail");
            if batches.is_empty() {
                break;
            }
            polls += 1;
            let bytes: usize = batches
                .iter()
                .flat_map(|b| &b.ops)
                .map(|op| match op {
                    ReplOp::Put { key, value } => key.len() + value.len(),
                    ReplOp::Delete { key } => key.len(),
                })
                .sum();
            // One whole batch may overshoot the budget, never more.
            assert!(
                bytes < BUDGET + 2_000,
                "poll materialized {bytes} bytes against a {BUDGET} budget"
            );
            for b in &batches {
                replica.apply_batch(b).expect("apply");
            }
        }
        assert!(polls > 5, "budget did not slice the tail (polls={polls})");
        assert_eq!(scan_all(&master), scan_all(&replica), "converged to parity");
        assert_eq!(replica.last_applied(), master.latest_seq());
    }

    #[test]
    fn rerequest_from_cursor_is_idempotent() {
        let (md, rd) = (TempDir::new("m2"), TempDir::new("r2"));
        let master = RocksKv::open(&md.0).expect("open");
        let replica = RocksKv::open(&rd.0).expect("open");
        let s = StringStore::new(&master, b"0", system_clock);
        s.set(1, b"a", b"1", SetOptions::default()).expect("set");
        s.set(1, b"b", b"2", SetOptions::default()).expect("set");
        catch_up(&master, &replica);
        let cursor = replica.last_applied();

        // Re-request from the same cursor: nothing new may be returned
        // (the batch containing `cursor` must be deduped, not re-applied).
        let again = master.updates_since(cursor).expect("tail");
        assert!(
            again.is_empty(),
            "re-request from cursor must yield nothing, got {again:?}"
        );

        // And even a manual re-apply of an old batch is harmless *iff*
        // ops are idempotent puts/deletes — verify state unchanged.
        let before = scan_all(&replica);
        let from_zero = master.updates_since(0).expect("tail");
        for b in &from_zero {
            replica.apply_batch(b).expect("apply");
        }
        assert_eq!(scan_all(&replica), before, "replay is a no-op for state");
    }

    #[test]
    fn deletes_and_multi_op_batches_replicate() {
        let (md, rd) = (TempDir::new("m3"), TempDir::new("r3"));
        let master = RocksKv::open(&md.0).expect("open");
        let replica = RocksKv::open(&rd.0).expect("open");
        let h = crate::hashes::HashStore::new(&master, b"0", system_clock);
        // HSET writes meta + subkeys (multi-op batches through the Kv, one
        // WAL batch per put in v0 — the tailer must preserve order).
        h.hset(
            1,
            b"h",
            &[
                (b"f1".to_vec(), b"v1".to_vec()),
                (b"f2".to_vec(), b"v2".to_vec()),
            ],
        )
        .expect("hset");
        h.hdel(1, b"h", &[b"f1".to_vec()]).expect("hdel");
        catch_up(&master, &replica);
        assert_eq!(scan_all(&master), scan_all(&replica));
        let rh = crate::hashes::HashStore::new(&replica, b"0", system_clock);
        assert_eq!(rh.hget(1, b"h", b"f1"), Ok(None));
        assert_eq!(rh.hget(1, b"h", b"f2"), Ok(Some(b"v2".to_vec())));
    }

    #[test]
    fn cursor_survives_replica_restart() {
        let (md, rd) = (TempDir::new("m4"), TempDir::new("r4"));
        let master = RocksKv::open(&md.0).expect("open");
        let s = StringStore::new(&master, b"0", system_clock);
        s.set(1, b"k", b"v", SetOptions::default()).expect("set");
        let cursor = {
            let replica = RocksKv::open(&rd.0).expect("open replica");
            catch_up(&master, &replica);
            replica.last_applied()
        }; // replica dropped = closed
        let reopened = RocksKv::open(&rd.0).expect("reopen replica");
        assert_eq!(reopened.last_applied(), cursor, "cursor is durable");
        // And catch-up from the durable cursor finds nothing new.
        assert_eq!(catch_up(&master, &reopened), 0);
    }

    #[test]
    fn stale_batch_replay_is_a_guarded_noop() {
        let (md, rd) = (TempDir::new("m5"), TempDir::new("r5"));
        let master = RocksKv::open(&md.0).expect("open");
        let replica = RocksKv::open(&rd.0).expect("open");
        let s = StringStore::new(&master, b"0", system_clock);
        s.set(1, b"k", b"v1", SetOptions::default()).expect("set");
        let old_batches = master.updates_since(0).expect("tail");
        s.set(1, b"k", b"v2", SetOptions::default()).expect("set");
        catch_up(&master, &replica);
        let cursor = replica.last_applied();
        let rs = StringStore::new(&replica, b"0", system_clock);
        assert_eq!(rs.get(1, b"k"), Ok(Some(b"v2".to_vec())));
        // Replaying the OLD batch (k=v1) must not regress state or cursor.
        for b in &old_batches {
            replica.apply_batch(b).expect("stale apply is a no-op");
        }
        assert_eq!(rs.get(1, b"k"), Ok(Some(b"v2".to_vec())), "no regression");
        assert_eq!(replica.last_applied(), cursor, "cursor did not move");
    }

    #[test]
    fn sequence_gaps_are_rejected() {
        let (md, rd) = (TempDir::new("m6"), TempDir::new("r6"));
        let master = RocksKv::open(&md.0).expect("open");
        let replica = RocksKv::open(&rd.0).expect("open");
        let s = StringStore::new(&master, b"0", system_clock);
        s.set(1, b"a", b"1", SetOptions::default()).expect("set");
        s.set(1, b"b", b"2", SetOptions::default()).expect("set");
        s.set(1, b"c", b"3", SetOptions::default()).expect("set");
        let batches = master.updates_since(0).expect("tail");
        assert!(batches.len() >= 3);
        replica.apply_batch(&batches[0]).expect("first");
        // Skipping a batch must be detected, not silently absorbed.
        let err = replica.apply_batch(&batches[2]).expect_err("gap");
        assert!(matches!(err, ReplError::SequenceGap { .. }), "{err:?}");
        // Applying in order proceeds normally.
        replica.apply_batch(&batches[1]).expect("second");
        replica.apply_batch(&batches[2]).expect("third");
    }

    /// A cursor the WAL can no longer reach must be an ERROR, not silence.
    ///
    /// RocksDB answers `get_updates_since` for a recycled sequence with an
    /// iterator that yields nothing, which used to be reported as "no
    /// updates" — indistinguishable from a caught-up replica. On the
    /// playground that left a node frozen 20k sequences behind while the
    /// master still counted it live, seq_lag climbing, nothing logged.
    #[test]
    fn a_cursor_the_wal_cannot_reach_is_a_gap_not_silence() {
        let md = TempDir::new("m-walgap");
        let master = RocksKv::open(&md.0).expect("open");
        let s = StringStore::new(&master, b"0", system_clock);
        for i in 0..8 {
            s.set(
                1,
                format!("early{i}").as_bytes(),
                b"v",
                SetOptions::default(),
            )
            .expect("set");
        }
        // A cursor in the MIDDLE of what the first segment holds: the writes
        // just after it are the ones about to become unreachable.
        let stranded = master.latest_seq() / 2;
        assert!(
            !master.updates_since(stranded).expect("tail").is_empty(),
            "precondition: this cursor is reachable while its WAL is retained"
        );

        // Retire the segment holding `stranded`: a flush makes it obsolete
        // and RocksDB moves it to archive/, which is exactly where retention
        // expiry deletes it from. Deleting it by hand is the same end state
        // without waiting out the TTL.
        master.flush();
        for i in 0..4 {
            s.set(
                1,
                format!("late{i}").as_bytes(),
                b"v",
                SetOptions::default(),
            )
            .expect("set");
        }
        let archive = master.path().join("archive");
        let mut retired = 0;
        for entry in std::fs::read_dir(&archive).expect("archive dir") {
            let p = entry.expect("entry").path();
            if p.extension().is_some_and(|e| e == "log") {
                std::fs::remove_file(&p).expect("retire segment");
                retired += 1;
            }
        }
        assert!(retired > 0, "precondition: a WAL segment was retired");

        // The master HAS newer data, so "nothing to do" here is a lie — and
        // it is the specific lie that freezes a replica while the master goes
        // on counting it live. RocksDB answers a recycled sequence either by
        // yielding nothing or by starting at the oldest segment it kept, so
        // both shapes are legal; what is NOT legal is silence.
        // BOTH shapes must be a GAP HERE, not just the empty one. This used
        // to accept a non-empty span that started past the cursor and leave it
        // to "the replica's own contiguity check" — which does catch it, but
        // only after the replica has been ADMITTED, has cleared its
        // NEEDS_RESEED marker on the strength of that admission, and has
        // attached. It then exits and re-marks, and the next start is admitted
        // again: the 13-minute livelock in docs/bugs/0031.
        //
        // Accepting either answer made this test unable to fail for the shape
        // that actually shipped. A test that permits both branches of a
        // question is not testing the question.
        match master.updates_since(stranded) {
            Err(ReplError::WalGap(_)) => {}
            other => panic!(
                "a cursor outside the retained WAL must be a WalGap at ADMISSION, \
                 whether the WAL yields nothing or yields a span starting past it — \
                 deferring the second shape to the stream is bug 0031: {other:?}"
            ),
        }

        // And the caught-up case must stay quiet: a cursor at or past the
        // latest sequence is not a gap, it is a replica with nothing to do.
        assert!(
            master
                .updates_since(master.latest_seq())
                .expect("caught up is not a gap")
                .is_empty()
        );
    }

    #[test]
    fn system_rows_do_not_replicate_but_advance_the_cursor() {
        use crate::manifest::{self, Epoch, Role, RoleClaim};
        let (md, rd) = (TempDir::new("m7"), TempDir::new("r7"));
        let master = RocksKv::open(&md.0).expect("open");
        let replica = RocksKv::open(&rd.0).expect("open");
        // Master writes its own manifest (role) — a system row.
        manifest::set_role(
            &master,
            RoleClaim {
                role: Role::Master,
                epoch: Epoch {
                    generation: 0,
                    counter: 1,
                },
            },
        )
        .expect("role");
        let s = StringStore::new(&master, b"0", system_clock);
        s.set(1, b"k", b"v", SetOptions::default()).expect("set");
        // Replica has its own identity that must survive tailing.
        manifest::set_role(
            &replica,
            RoleClaim {
                role: Role::Replica,
                epoch: Epoch {
                    generation: 0,
                    counter: 1,
                },
            },
        )
        .expect("replica role");
        catch_up(&master, &replica);
        // Data replicated; the master's role row did NOT overwrite ours.
        let rs = StringStore::new(&replica, b"0", system_clock);
        assert_eq!(rs.get(1, b"k"), Ok(Some(b"v".to_vec())));
        assert_eq!(
            manifest::read_role(&replica).expect("role").role,
            Role::Replica,
            "master identity must not leak through the stream"
        );
        // Cursor covers the filtered rows too (span preserved).
        assert_eq!(replica.last_applied(), master.latest_seq());
        assert!(
            master
                .updates_since(replica.last_applied())
                .expect("tail")
                .is_empty()
        );
    }

    /// Soak run 35 cycle 4: a node that rewound to its own older-space
    /// snapshot tails a lineage whose numbers dwarf its own — its cursor
    /// rows carry values in the MILLIONS while its RocksDB seqs are in the
    /// tens. `own_seq_for_upstream` must still find the mapping row; the
    /// original scan started at `get_updates_since(upstream_seq - 1)`,
    /// which on this shape points past the end of the WAL entirely, and a
    /// perfectly mappable cursor was refused into a 49-second full re-seed.
    #[test]
    fn own_seq_maps_an_upstream_space_far_ahead_of_our_own() {
        let d = TempDir::new("map-behind");
        let kv = RocksKv::open(&d.0).expect("open");
        // This node's cursor sits at 6M in its upstream's space; its own
        // seqs start near zero.
        kv.set_last_applied(6_000_000).expect("cursor");
        let b1 = ReplBatch {
            first_seq: 6_000_001,
            last_seq: 6_000_002,
            ops: vec![
                ReplOp::Put {
                    key: b"Ma".to_vec(),
                    value: b"1".to_vec(),
                },
                ReplOp::Put {
                    key: b"Mb".to_vec(),
                    value: b"2".to_vec(),
                },
            ],
        };
        kv.apply_batch(&b1).expect("apply b1");
        let b2 = ReplBatch {
            first_seq: 6_000_003,
            last_seq: 6_000_004,
            ops: vec![ReplOp::Put {
                key: b"Mc".to_vec(),
                value: b"3".to_vec(),
            }],
        };
        kv.apply_batch(&b2).expect("apply b2");

        // Map the position b1 reached. The answer is b1's OWN last seq —
        // a single-digit number — and serving from it must hand b2 next.
        let own = kv
            .own_seq_for_upstream(6_000_002)
            .expect("a retained mapping row must be found regardless of the spaces' offset");
        assert!(
            own < kv.latest_seq(),
            "own-space position must be a local seq, got {own} (latest {})",
            kv.latest_seq()
        );
        let next = kv.updates_since(own).expect("serve from mapped position");
        assert!(
            next.iter().flat_map(|b| &b.ops).any(|op| matches!(
                op,
                ReplOp::Put { key, .. } if key == b"Mc"
            )),
            "serving from the mapped position must deliver exactly the ops after it"
        );
        assert!(
            !next.iter().flat_map(|b| &b.ops).any(|op| matches!(
                op,
                ReplOp::Put { key, .. } if key == b"Ma"
            )),
            "ops at or before the mapped position must not replay"
        );
    }
}

#[cfg(test)]
mod bug_0050_iterator_shape {
    use super::*;
    use crate::strings::{SetOptions, StringStore, system_clock};

    /// BUG-0050, cause side: writing an interior cursor must SNAP it forward
    /// to the batch end, so a link can never be pinned at a position
    /// `get_updates_since` will not serve.
    #[test]
    fn setting_an_interior_cursor_snaps_it_to_the_batch_end() {
        let d = std::env::temp_dir().join(format!("flint-b50-snap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        let kv = RocksKv::open(&d).expect("open");
        let s = StringStore::new(&kv, b"ns", system_clock);
        s.set(1, b"seed", b"v", SetOptions::default()).expect("set");

        let before = kv.db().latest_sequence_number();
        let ops: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..8u32)
            .map(|i| (format!("Msnap{i}").into_bytes(), Some(b"v".to_vec())))
            .collect();
        kv.apply_writes(&ops).expect("multi");
        let end = kv.db().latest_sequence_number();
        assert!(
            end - before >= 3,
            "fixture batch too short to have an interior"
        );

        let interior = before + 1;
        kv.set_last_applied(interior).expect("set");
        assert_eq!(
            kv.last_applied(),
            end,
            "an interior cursor must be stored as the batch end, not as given"
        );

        // A cursor already ON a boundary must be left exactly as it is —
        // snapping must not move a correct value.
        kv.set_last_applied(before).expect("set");
        assert_eq!(
            kv.last_applied(),
            before,
            "a boundary cursor was moved; snapping must be a no-op there"
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    /// BUG-0050 REGRESSION: a cursor interior to a batch must be SERVED.
    #[test]
    fn an_interior_cursor_is_served_not_refused() {
        let d = std::env::temp_dir().join(format!("flint-b50-reg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        let kv = RocksKv::open(&d).expect("open");
        let s = StringStore::new(&kv, b"ns", system_clock);
        s.set(1, b"seed", b"v", SetOptions::default()).expect("set");

        let before = kv.db().latest_sequence_number();
        let ops: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..8u32)
            .map(|i| (format!("Mreg{i}").into_bytes(), Some(b"v".to_vec())))
            .collect();
        kv.apply_writes(&ops).expect("multi");
        let after = kv.db().latest_sequence_number();

        // CONTROL FIRST. If the batch did not span several sequences there is
        // no interior position and this test proves nothing.
        assert!(
            after - before >= 3,
            "the fixture batch spans {} sequence(s); an interior cursor needs \
             several, so this test would pass without exercising anything",
            after - before
        );

        let interior = before + 1;
        let served = kv
            .updates_since(interior)
            .unwrap_or_else(|e| panic!("interior cursor {interior} refused: {e:?} — BUG-0050"));
        assert!(!served.is_empty(), "served nothing for an interior cursor");
        // And it must leave the replica on a REAL batch end, or the link is
        // merely frozen instead of re-seeding.
        assert_eq!(
            served.last().expect("batch").last_seq,
            after,
            "an interior cursor must be advanced to the batch end, or it stays interior forever"
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    /// AND THE PROTECTION MUST SURVIVE. A cursor the WAL genuinely cannot
    /// reach back to still has to raise WalGap — that is the condition the
    /// empty-iterator check exists for, and the fix above must not swallow it.
    #[test]
    fn a_genuinely_recycled_wal_still_raises_walgap() {
        let d = std::env::temp_dir().join(format!("flint-b50-recyc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        // Retention OFF: no archive, so flushed segments are deleted rather
        // than kept — which is what "recycled" means here.
        let kv = RocksKv::open_with_retention(&d, 0, 0).expect("open");
        let s = StringStore::new(&kv, b"ns", system_clock);
        for i in 0..50u32 {
            s.set(1, format!("old{i}").as_bytes(), b"v", SetOptions::default())
                .expect("set");
        }
        let stale = kv.db().latest_sequence_number();
        for round in 0..6 {
            kv.flush();
            for i in 0..50u32 {
                s.set(
                    1,
                    format!("n{round}-{i}").as_bytes(),
                    b"v",
                    SetOptions::default(),
                )
                .expect("set");
            }
        }
        kv.flush();

        // CONTROL: the scan must actually be unable to reach `stale`, or this
        // asserts nothing. If retention kept everything, skip rather than pass
        // — a test that cannot fail is worse than one that does not run.
        let reachable = kv
            .db()
            .get_updates_since(0)
            .expect("iter")
            .filter_map(|i| i.ok())
            .map(|(f, _)| f)
            .next();
        if reachable.is_some_and(|f| f <= stale) {
            eprintln!(
                "  SKIP: the WAL still reaches back to {stale} (oldest {reachable:?}); \
                 this environment does not recycle, so the protection is untested here"
            );
            let _ = std::fs::remove_dir_all(&d);
            return;
        }
        let err = kv
            .updates_since(stale)
            .expect_err("a cursor the WAL cannot reach must still be a gap");
        assert!(
            matches!(err, ReplError::WalGap(_)),
            "expected WalGap for an unreachable cursor, got {err:?}"
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    /// DOES THE COLLECTOR'S SEQUENCE ACCOUNTING MATCH ROCKSDB'S?
    ///
    /// OpCollector implements only put/delete. If a batch carries ops it does
    /// not see — a non-default column family, a range delete — its `seq`
    /// undercounts, and the `last_seq` a replica records is SHORT of the real
    /// batch end. That is a cursor landing interior by construction, which is
    /// BUG-0050's precondition. Compares what the collector reports against
    /// what the DB's own sequence did.
    #[test]
    fn collector_sequence_matches_the_dbs_own_advance() {
        let d = std::env::temp_dir().join(format!("flint-b50-acct-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        let kv = RocksKv::open(&d).expect("open");
        let s = StringStore::new(&kv, b"ns", system_clock);
        s.set(1, b"seed", b"v", SetOptions::default()).expect("set");

        for (label, n) in [("1-key", 1usize), ("8-key", 8), ("64-key", 64)] {
            let before = kv.db().latest_sequence_number();
            let ops: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..n)
                .map(|i| (format!("M{label}{i}").into_bytes(), Some(b"v".to_vec())))
                .collect();
            kv.apply_writes(&ops).expect("write");
            let after = kv.db().latest_sequence_number();
            let reported = kv
                .updates_since(before)
                .expect("tail")
                .last()
                .map(|b| b.last_seq)
                .unwrap_or(0);
            eprintln!(
                "  {label}: db {before} -> {after} (advance {}), collector last_seq {reported}{}",
                after - before,
                if reported == after {
                    "  MATCH"
                } else {
                    "  *** SHORT ***"
                }
            );
        }
        let _ = std::fs::remove_dir_all(&d);
    }

    /// What does the iterator actually HAND BACK for an interior cursor?
    /// The fix for BUG-0050 differs completely depending on the answer, so
    /// this is measured rather than reasoned about.
    #[test]
    fn what_the_iterator_yields_for_an_interior_cursor() {
        let d = std::env::temp_dir().join(format!("flint-b50-shape-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        let kv = RocksKv::open(&d).expect("open");
        let s = StringStore::new(&kv, b"ns", system_clock);
        s.set(1, b"seed", b"v", SetOptions::default()).expect("set");

        let before = kv.db().latest_sequence_number();
        let ops: Vec<(Vec<u8>, Option<Vec<u8>>)> = (0..8u32)
            .map(|i| (format!("Mk{i}").into_bytes(), Some(b"v".to_vec())))
            .collect();
        kv.apply_writes(&ops).expect("multi");
        let after = kv.db().latest_sequence_number();

        for cursor in [before, before + 1, before + 2, after - 1, after] {
            let it = kv.db().get_updates_since(cursor).expect("iter");
            let spans: Vec<String> = it
                .filter_map(|i| i.ok())
                .map(|(first, b)| format!("first={first} count={}", b.len()))
                .collect();
            eprintln!(
                "  cursor {cursor}: iterator yielded {} item(s) [{}]",
                spans.len(),
                spans.join("; ")
            );
        }
        eprintln!("  (batch occupies {}..={})", before + 1, after);
        let _ = std::fs::remove_dir_all(&d);
    }
}
