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
            .map_err(|e| ReplError::Storage(e.to_string()))
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
        let iter = self
            .db()
            .get_updates_since(0)
            .map_err(|e| ReplError::WalGap(e.to_string()))?;
        for item in iter {
            let (first_seq, batch) = item.map_err(|e| ReplError::Storage(e.to_string()))?;
            let mut find = CursorFind {
                seq: first_seq - 1,
                reached: None,
            };
            batch.iterate(&mut find);
            if let Some(reached) = find.reached
                && reached >= upstream_seq
            {
                return Ok(find.seq);
            }
        }
        Err(ReplError::WalGap(format!(
            "no apply batch reaching upstream seq {upstream_seq} is retained in this WAL"
        )))
    }

    /// Set the replica cursor directly (after a checkpoint full sync, the
    /// copied DB's own latest sequence IS the master cursor).
    pub fn set_last_applied(&self, seq: u64) -> Result<(), ReplError> {
        self.db()
            .put(REPL_STATE_KEY, seq.to_be_bytes())
            .map_err(|e| ReplError::Storage(e.to_string()))
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
