//! Opt-in async write queue (ADR-0005 D4). For an opted-in namespace, a
//! batchable string/counter write enqueues instead of applying inline; the
//! connection thread blocks on its reply (ack-after-apply), and a single
//! consumer drains the queue in batches — running each command through the
//! normal Dispatcher over a BatchingKv, then committing the whole batch as
//! ONE engine WriteBatch (RocksDB group commit). Reads and non-batchable
//! writes bypass this entirely.
//!
//! The trade, per the ADR: a write pays queue-wait + batch latency (order
//! 2-3x) in exchange for many connections' writes collapsing into one engine
//! write — the counter/telemetry shape. Per-connection program order is free:
//! a connection blocks on each write's ack before its next command, so
//! `SET k; GET k` on one connection stays ordered; the batching is ACROSS
//! connections.
//!
//! The queue is a rocks-only feature (the batch commit needs
//! `RocksKv::apply_writes`). The `mem` build compiles the types so the
//! `Option<&WriteQueue>` plumbing threads uniformly through `serve`/`execute`,
//! but never constructs one — hence the module-wide dead-code allow.
#![allow(dead_code)]

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "rocks")]
use std::sync::mpsc::Receiver;
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};

use flint_resp::Value;
#[cfg(feature = "rocks")]
use flint_storage::Kv;
#[cfg(feature = "rocks")]
use flint_storage::batch::BatchingKv;
#[cfg(feature = "rocks")]
use flint_storage::rocks::RocksKv;

#[cfg(feature = "rocks")]
use crate::commands::{Dispatcher, Limits};

/// Max commands merged into one engine write. Bigger = better amortization,
/// worse tail latency for the last op in a batch; 256 is a balanced default.
const BATCH_MAX: usize = 256;
/// Default bounded queue depth. Full -> the writer gets -THROTTLED (the
/// existing back-off contract), never an unbounded backlog. Overridable with
/// `--async-queue-cap` (lets operators tune the shed point, and lets the
/// drill force the -THROTTLED path deterministically).
pub const DEFAULT_QUEUE_CAP: usize = 4096;

/// Which namespaces route writes through the queue.
pub enum AsyncScope {
    All,
    Only(HashSet<Vec<u8>>),
}

impl AsyncScope {
    /// Parse `--async-writes all` or `--async-writes ns1,ns2`.
    pub fn parse(spec: &str) -> Self {
        if spec.trim() == "all" {
            AsyncScope::All
        } else {
            AsyncScope::Only(
                spec.split(',')
                    .map(|s| s.trim().as_bytes().to_vec())
                    .collect(),
            )
        }
    }

    fn wants(&self, ns: &[u8]) -> bool {
        match self {
            AsyncScope::All => true,
            AsyncScope::Only(set) => set.contains(ns),
        }
    }
}

/// A batchable write awaiting its turn: the command, the namespace it runs
/// in, and the one-shot reply channel the connection thread blocks on.
struct WriteJob {
    ns: Vec<u8>,
    args: Vec<Vec<u8>>,
    reply: SyncSender<Value>,
}

pub struct WriteQueue {
    scope: AsyncScope,
    tx: SyncSender<WriteJob>,
    depth: Arc<AtomicUsize>,
}

/// String/counter writes only — pure get+put+delete, so the BatchingKv never
/// needs a scan overlay (module docs in flint-storage::batch). Everything
/// else (complex types, DEL, FLUSHALL, unknown) bypasses the queue and
/// applies inline, even in an opted-in namespace.
pub fn is_batchable(name: &[u8]) -> bool {
    matches!(
        name.to_ascii_uppercase().as_slice(),
        b"SET" | b"SETNX" | b"SETEX" | b"INCR" | b"DECR" | b"INCRBY" | b"DECRBY" | b"APPEND"
    )
}

impl WriteQueue {
    /// Spawn the consumer and return the handle. `store` is the real store
    /// (dispatched through the BatchingKv); `rocks` commits the batch.
    #[cfg(feature = "rocks")]
    pub fn start(
        scope: AsyncScope,
        cap: usize,
        store: Arc<dyn Kv>,
        rocks: Arc<RocksKv>,
        clock: flint_storage::strings::Clock,
        limits: Limits,
    ) -> Arc<Self> {
        let (tx, rx) = sync_channel::<WriteJob>(cap.max(1));
        let depth = Arc::new(AtomicUsize::new(0));
        let q = Arc::new(WriteQueue {
            scope,
            tx,
            depth: Arc::clone(&depth),
        });
        std::thread::spawn(move || consumer(rx, store, rocks, clock, limits, depth));
        q
    }

    pub fn wants(&self, ns: &[u8]) -> bool {
        self.scope.wants(ns)
    }

    pub fn depth(&self) -> usize {
        self.depth.load(Ordering::Relaxed)
    }

    /// Enqueue a batchable write and block until the consumer applies it and
    /// sends the reply (ack-after-apply). Full queue -> -THROTTLED.
    pub fn submit(&self, ns: Vec<u8>, args: Vec<Vec<u8>>) -> Value {
        let (rtx, rrx) = sync_channel::<Value>(1);
        self.depth.fetch_add(1, Ordering::Relaxed);
        match self.tx.try_send(WriteJob {
            ns,
            args,
            reply: rtx,
        }) {
            Ok(()) => rrx
                .recv()
                .unwrap_or_else(|_| Value::Error("ERR async write consumer stopped".into())),
            Err(TrySendError::Full(_)) => {
                self.depth.fetch_sub(1, Ordering::Relaxed);
                Value::Error("THROTTLED async write queue full, retry with backoff".into())
            }
            Err(TrySendError::Disconnected(_)) => {
                self.depth.fetch_sub(1, Ordering::Relaxed);
                Value::Error("ERR async write consumer stopped".into())
            }
        }
    }
}

#[cfg(feature = "rocks")]
fn consumer(
    rx: Receiver<WriteJob>,
    store: Arc<dyn Kv>,
    rocks: Arc<RocksKv>,
    clock: flint_storage::strings::Clock,
    limits: Limits,
    depth: Arc<AtomicUsize>,
) {
    while let Ok(first) = rx.recv() {
        // Drain up to BATCH_MAX already-queued jobs (non-blocking) so one
        // engine write serves the whole burst.
        let mut jobs = vec![first];
        while jobs.len() < BATCH_MAX {
            match rx.try_recv() {
                Ok(j) => jobs.push(j),
                Err(_) => break,
            }
        }
        depth.fetch_sub(jobs.len(), Ordering::Relaxed);

        // Run every command against ONE BatchingKv: each computes its exact
        // reply over the accumulating state (an INCR burst on one key sees
        // its own prior increments).
        let batching = BatchingKv::new(store.as_ref());
        let mut pending: Vec<(Value, SyncSender<Value>)> = Vec::with_capacity(jobs.len());
        for job in jobs {
            let reply =
                Dispatcher::with_limits(&batching, clock, limits, &job.ns).dispatch(&job.args);
            pending.push((reply, job.reply));
        }
        // Commit the whole batch as ONE engine write, THEN release the acks —
        // durability is amortized across the batch, never weakened.
        if let Err(e) = rocks.apply_writes(&batching.into_ops()) {
            let msg = Value::Error(format!("ERR batch write failed: {e}"));
            for (_, tx) in pending {
                let _ = tx.send(msg.clone());
            }
            continue;
        }
        for (reply, tx) in pending {
            let _ = tx.send(reply);
        }
    }
}
