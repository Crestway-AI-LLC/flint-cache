// SPDX-License-Identifier: Elastic-2.0
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
use std::sync::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "rocks")]
use std::sync::mpsc::Receiver;
use std::sync::mpsc::{RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::time::Duration;

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
/// A queued write that has not been acked within this window means the
/// single consumer is wedged (e.g. a RocksDB write stall mid-commit). Rather
/// than block the connection thread forever, the writer is shed -THROTTLED
/// (the queue's own bounded contract) so the client backs off instead of
/// hanging. Generous vs any healthy batch commit.
const SUBMIT_TIMEOUT: Duration = Duration::from_secs(5);
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
    /// Parse `--async-writes all` or `--async-writes ns1,ns2`. `off`/`none`
    /// yields the empty set, which is the handshake-only state the node boots
    /// into when the flag is absent — so FLINTCONFIG can turn the node-level
    /// scope back off without a restart. Spelling those two out matters: a
    /// bare `parse("off")` would otherwise route a namespace literally named
    /// `off` and silently leave every other namespace synchronous.
    pub fn parse(spec: &str) -> Self {
        let t = spec.trim();
        if t == "all" {
            AsyncScope::All
        } else if t == "off" || t == "none" || t.is_empty() {
            AsyncScope::Only(HashSet::new())
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

    /// What FLINTCONFIG / INFO report back. `off` and a namespace list are
    /// distinguishable, so an operator can tell "nothing is routed" from
    /// "these three are".
    pub fn describe(&self) -> String {
        match self {
            AsyncScope::All => "all".to_string(),
            AsyncScope::Only(set) if set.is_empty() => "off".to_string(),
            AsyncScope::Only(set) => {
                let mut names: Vec<String> = set
                    .iter()
                    .map(|n| String::from_utf8_lossy(n).into_owned())
                    .collect();
                names.sort();
                names.join(",")
            }
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
    /// Which namespaces route through the queue. Behind a lock rather than a
    /// plain field so FLINTCONFIG can widen or disable it live; read once per
    /// batchable write, which is negligible beside the channel send and the
    /// RocksDB commit that follow it.
    scope: RwLock<AsyncScope>,
    tx: SyncSender<WriteJob>,
    depth: Arc<AtomicUsize>,
    /// The channel's own capacity. FIXED: `sync_channel` cannot be resized,
    /// so this is the allocation bound for the life of the process and the
    /// ceiling every runtime cap is clamped to.
    hard_cap: usize,
    /// Runtime admission bound (FLINTCONFIG `async-queue-cap`). Lowering it
    /// sheds earlier than the channel would; it can never raise capacity above
    /// `hard_cap`, and `set_soft_cap` REFUSES rather than silently clamping,
    /// because a knob that accepts a value it cannot honour is worse than one
    /// that rejects it.
    soft_cap: AtomicUsize,
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
        let hard_cap = cap.max(1);
        let (tx, rx) = sync_channel::<WriteJob>(hard_cap);
        let depth = Arc::new(AtomicUsize::new(0));
        let q = Arc::new(WriteQueue {
            scope: RwLock::new(scope),
            tx,
            depth: Arc::clone(&depth),
            hard_cap,
            soft_cap: AtomicUsize::new(hard_cap),
        });
        std::thread::spawn(move || consumer(rx, store, rocks, clock, limits, depth));
        q
    }

    pub fn wants(&self, ns: &[u8]) -> bool {
        match self.scope.read() {
            Ok(g) => g.wants(ns),
            // A poisoned lock means a panic while the scope was held. Fail
            // CLOSED to the synchronous path: it is always correct, just
            // slower, whereas guessing "route it" would change durability
            // semantics off the back of an unrelated panic.
            Err(_) => false,
        }
    }

    /// Swap the node-level scope live (FLINTCONFIG `async-writes`). In-flight
    /// jobs already in the channel are unaffected; the change applies to the
    /// next write admitted.
    pub fn set_scope(&self, scope: AsyncScope) -> Result<(), String> {
        match self.scope.write() {
            Ok(mut g) => {
                *g = scope;
                Ok(())
            }
            Err(_) => Err("ERR async-writes scope lock poisoned".to_string()),
        }
    }

    pub fn scope_desc(&self) -> String {
        match self.scope.read() {
            Ok(g) => g.describe(),
            Err(_) => "unknown (lock poisoned)".to_string(),
        }
    }

    pub fn hard_cap(&self) -> usize {
        self.hard_cap
    }

    pub fn soft_cap(&self) -> usize {
        self.soft_cap.load(Ordering::Relaxed)
    }

    /// Lower (or restore) the admission bound. Refuses anything above the
    /// channel's fixed capacity instead of clamping, so `FLINTCONFIG
    /// async-queue-cap 100000` is an error the operator sees rather than a
    /// value that reads back as 4096.
    pub fn set_soft_cap(&self, n: usize) -> Result<(), String> {
        if n == 0 {
            return Err("ERR async-queue-cap must be >= 1".to_string());
        }
        if n > self.hard_cap {
            return Err(format!(
                "ERR async-queue-cap {n} exceeds the channel capacity {} fixed at startup \
                 (--async-queue-cap); a larger queue needs a restart",
                self.hard_cap
            ));
        }
        self.soft_cap.store(n, Ordering::Relaxed);
        Ok(())
    }

    pub fn depth(&self) -> usize {
        self.depth.load(Ordering::Relaxed)
    }

    /// Enqueue a batchable write and block until the consumer applies it and
    /// sends the reply (ack-after-apply). Full queue -> -THROTTLED.
    pub fn submit(&self, ns: Vec<u8>, args: Vec<Vec<u8>>) -> Value {
        let (rtx, rrx) = sync_channel::<Value>(1);
        // Runtime admission bound, checked before the channel's own. Sheds
        // with the same -THROTTLED the full channel gives, so a client cannot
        // tell which bound it hit and does not need to.
        if self.depth.load(Ordering::Relaxed) >= self.soft_cap.load(Ordering::Relaxed) {
            return Value::Error("THROTTLED async write queue full, retry with backoff".into());
        }
        self.depth.fetch_add(1, Ordering::Relaxed);
        match self.tx.try_send(WriteJob {
            ns,
            args,
            reply: rtx,
        }) {
            Ok(()) => match rrx.recv_timeout(SUBMIT_TIMEOUT) {
                Ok(reply) => reply,
                // The consumer is wedged: don't hang the connection forever.
                // The write's own slot is left counted-in-flight (the batch
                // may still commit it); the CLIENT is told to back off.
                Err(RecvTimeoutError::Timeout) => {
                    Value::Error("THROTTLED async write queue stalled, retry with backoff".into())
                }
                Err(RecvTimeoutError::Disconnected) => {
                    Value::Error("ERR async write consumer stopped".into())
                }
            },
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

        // Exclude every inline writer for the batch's dispatch + commit
        // (write_lock.rs): a flagged tenant's non-batchable DEL, or another
        // tenant's inline traffic, must not interleave with the batch's
        // read-modify-write halves.
        let _all = crate::write_lock::lock_all();
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

#[cfg(test)]
mod scope_tests {
    use super::*;

    /// `off`/`none` must yield the EMPTY set, not a namespace named "off".
    /// Without this, `FLINTCONFIG async-writes off` would route a namespace
    /// literally called `off` and leave everything else synchronous — the
    /// knob would report success and do the opposite of what was asked.
    #[test]
    fn off_and_none_disable_rather_than_naming_a_namespace() {
        for spec in ["off", "none", "", "  off  "] {
            match AsyncScope::parse(spec) {
                AsyncScope::Only(set) => assert!(
                    set.is_empty(),
                    "parse({spec:?}) kept {} entries; `off` must disable, not name a namespace",
                    set.len()
                ),
                AsyncScope::All => panic!("parse({spec:?}) enabled ALL namespaces"),
            }
        }
        assert!(!AsyncScope::parse("off").wants(b"off"));
        assert!(AsyncScope::parse("all").wants(b"anything"));
        assert!(AsyncScope::parse("a,b").wants(b"a"));
        assert!(!AsyncScope::parse("a,b").wants(b"c"));
    }

    /// The reported scope has to distinguish "nothing is routed" from "these
    /// are", or an operator reading FLINTCONFIG cannot tell a disabled queue
    /// from a populated one.
    #[test]
    fn describe_distinguishes_off_from_a_namespace_list() {
        assert_eq!(AsyncScope::parse("all").describe(), "all");
        assert_eq!(AsyncScope::parse("off").describe(), "off");
        assert_eq!(AsyncScope::parse("b,a").describe(), "a,b");
    }
}
