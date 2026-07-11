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

/// One master WriteBatch worth of ops. `last_seq` is the sequence number of
/// the final op in the batch — the replica's cursor after applying it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplBatch {
    pub last_seq: u64,
    pub ops: Vec<ReplOp>,
}

#[derive(Debug)]
pub enum ReplError {
    /// The WAL no longer reaches back to the requested sequence (purged).
    /// The replica must full-sync from a checkpoint.
    WalGap(String),
    Storage(String),
}

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
        if self.seq > self.floor {
            self.ops.push(ReplOp::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            });
        }
    }

    fn delete(&mut self, key: &[u8]) {
        self.seq += 1;
        if self.seq > self.floor {
            self.ops.push(ReplOp::Delete { key: key.to_vec() });
        }
    }
}

impl RocksKv {
    /// Master side: every op after `last_applied`, grouped per WAL batch.
    /// Sequence-idempotent per the module contract.
    pub fn updates_since(&self, last_applied: u64) -> Result<Vec<ReplBatch>, ReplError> {
        if self.db().latest_sequence_number() <= last_applied {
            return Ok(Vec::new());
        }
        let iter = self
            .db()
            .get_updates_since(last_applied)
            .map_err(|e| ReplError::WalGap(e.to_string()))?;
        let mut out = Vec::new();
        for item in iter {
            let (first_seq, batch) = item.map_err(|e| ReplError::Storage(e.to_string()))?;
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
            out.push(ReplBatch {
                last_seq,
                ops: collector.ops,
            });
        }
        Ok(out)
    }

    /// Replica side: apply one batch atomically together with the cursor.
    pub fn apply_batch(&self, batch: &ReplBatch) -> Result<(), ReplError> {
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
}
