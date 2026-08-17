// SPDX-License-Identifier: Elastic-2.0
//! Shared, full-duplex backend connections (ADR-0020, second implementation).
//!
//! # Why the first implementation was replaced
//!
//! The first pool put a dispatcher thread in front of each connection:
//! receive a job, drain the queue, write the batch, then **block reading
//! every reply before writing anything more**. That last step is the defect.
//! While the dispatcher read batch K's replies the connection carried no new
//! requests, the node finished the batch and idled, and commands piled up in
//! a channel — a pipeline bubble every cycle. It also cost two thread
//! handoffs, a frame copy and a channel allocation per command, and replies
//! that had already arrived sat undelivered until the whole batch decoded,
//! which showed up directly as a doubled p99.9.
//!
//! Envoy's Redis client (`client_impl.cc`, reviewed 2026-08-17) does none of
//! that. `makeRequestInternal` appends the request to `pending_requests_`,
//! encodes into `encoder_buffer_`, and **returns** — nothing ever stops
//! writing to wait for a reply. `onRespValue` completes
//! `pending_requests_.front()` as each reply arrives. On close it drains and
//! fails everything pending. The connection is full-duplex; correlation is
//! FIFO; flushing is a buffer, not a stop-and-wait.
//!
//! # This implementation
//!
//! The same shape, adapted to blocking threads:
//!
//! - **Callers never block the connection.** `submit` pushes a reply slot
//!   onto the pending FIFO and appends the frame to the connection's staging
//!   buffer under one short ordering lock (memory only), then flushes outside
//!   it and parks on its own slot.
//! - **One reader thread per connection** decodes replies continuously and
//!   completes the FIFO front, exactly `onRespValue`. It never takes the
//!   ordering lock, so it can never stall a writer, and vice versa.
//! - **Coalescing is flush-combining, not a timer.** Whoever flushes writes
//!   everything staged so far — several threads' frames in one syscall under
//!   contention, immediate write when idle. Envoy's `buffer_flush_timeout`
//!   (default 3 ms) is deliberately refused: against a 0.3 ms p50 a flush
//!   timer costs more than it recovers, and with the buffer unset Envoy
//!   itself "flushes on every data reception".
//! - **FIFO order is the wire order** because slot-push and frame-append
//!   happen under one lock, and the flush path preserves append order.
//!   RESP guarantees reply order per connection, so position is the whole
//!   correlation — no request ids, same as Envoy and twemproxy.
//! - **A connection failure fails everyone on it, promptly.** The reader
//!   drains the FIFO with the error (Envoy's `while (!pending_requests_
//!   .empty())`), and the connection is torn down rather than reused — a
//!   half-read stream would hand the next command somebody else's reply.
//!
//! In-flight depth needs no explicit cap: `submit` is synchronous per
//! caller, so pending can never exceed the number of live client commands,
//! which the proxy's connection cap already bounds.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flint_resp::{Decoded, Value, decode};

use crate::{BACKEND_TIMEOUT, dial_backend};

/// Connections per (address, namespace, async-writes) lane. More than one
/// spreads slow-command exposure across sockets without recreating the
/// per-client socket explosion this pool replaces.
pub(crate) const DEFAULT_LANE_WIDTH: usize = 2;

/// The reader's socket poll. Short, so liveness checks run even when the
/// backend is silent; each wakeup is two syscalls on an idle connection.
const READ_POLL: Duration = Duration::from_secs(1);

/// How long a submitter waits on its slot. Above BACKEND_TIMEOUT so the
/// reader's oldest-pending liveness check fires first and produces an error
/// that can say why.
fn submit_budget() -> Duration {
    BACKEND_TIMEOUT + Duration::from_secs(5)
}

#[derive(Default)]
pub(crate) struct PoolStats {
    /// Non-empty socket flushes (one write syscall each).
    pub batches: AtomicU64,
    /// Commands submitted. commands/batches is the mean coalescing — the
    /// number that says whether sharing is doing anything at all.
    pub commands: AtomicU64,
    /// Highest simultaneous in-flight depth seen on any one connection: the
    /// direct witness of full-duplex operation. The dispatcher design could
    /// never exceed its own batch size here.
    pub inflight_max: AtomicU64,
    /// Live lanes (distinct addr/ns/async-writes triples).
    pub lanes: AtomicUsize,
    /// Dial failures.
    pub dial_failures: AtomicU64,
}

struct Pending {
    at: Instant,
    tx: SyncSender<std::io::Result<Value>>,
}

#[derive(PartialEq, Eq, Hash, Clone)]
struct Key {
    addr: String,
    ns: Vec<u8>,
    async_writes: bool,
}

struct Conn {
    /// Held across {push slot, append frame}: what makes FIFO order equal
    /// wire order. Never held across socket IO.
    order: Mutex<()>,
    w: flint_tls::DuplexWriter,
    pending: Arc<Mutex<VecDeque<Pending>>>,
    dead: Arc<AtomicBool>,
}

impl Conn {
    fn dial(
        key: &Key,
        tls: &Option<Arc<flint_tls::ReloadableClientConfig>>,
        stats: &Arc<PoolStats>,
    ) -> std::io::Result<Arc<Conn>> {
        let stream = dial_backend(&key.addr, &key.ns, key.async_writes, tls)?;
        let (rd, w) = stream.into_duplex()?;
        rd.set_read_timeout(Some(READ_POLL))?;
        let pending: Arc<Mutex<VecDeque<Pending>>> = Arc::new(Mutex::new(VecDeque::new()));
        let dead = Arc::new(AtomicBool::new(false));
        let conn = Arc::new(Conn {
            order: Mutex::new(()),
            w: w.clone(),
            pending: pending.clone(),
            dead: dead.clone(),
        });
        std::thread::spawn(move || reader_loop(rd, w, pending, dead));
        let _ = stats; // lanes gauge is maintained by the pool map
        Ok(conn)
    }

    /// One command: stage, flush, wait on our own slot. The connection is
    /// never ours — between our flush and our reply, any number of other
    /// commands travel on it.
    fn submit(&self, frame: &[u8], stats: &PoolStats) -> std::io::Result<Value> {
        if self.dead.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("backend connection is down"));
        }
        let (tx, rx) = sync_channel(1);
        {
            let _g = self
                .order
                .lock()
                .map_err(|_| std::io::Error::other("order lock poisoned"))?;
            {
                let mut q = self
                    .pending
                    .lock()
                    .map_err(|_| std::io::Error::other("pending poisoned"))?;
                q.push_back(Pending {
                    at: Instant::now(),
                    tx,
                });
                stats
                    .inflight_max
                    .fetch_max(q.len() as u64, Ordering::Relaxed);
            }
            // Memory-only: encrypt/stage. An error here poisons the stream
            // state, so the whole connection dies rather than desyncs.
            if let Err(e) = self.w.append(frame) {
                self.dead.store(true, Ordering::Relaxed);
                self.w.shutdown();
                return Err(e);
            }
        }
        // Socket write OUTSIDE the ordering lock: whoever gets the socket
        // first carries every staged frame (flush-combining).
        match self.w.flush() {
            Ok(0) => {}
            Ok(_) => {
                stats.batches.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                self.dead.store(true, Ordering::Relaxed);
                self.w.shutdown();
                return Err(e);
            }
        }
        stats.commands.fetch_add(1, Ordering::Relaxed);
        match rx.recv_timeout(submit_budget()) {
            Ok(r) => r,
            Err(_) => {
                // The reader's liveness check should have fired first; if we
                // got here the connection is not answering and not dying.
                self.dead.store(true, Ordering::Relaxed);
                self.w.shutdown();
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "backend did not answer within the submit budget",
                ))
            }
        }
    }
}

/// The read half of every pooled connection: decode replies continuously,
/// complete the FIFO front — `onRespValue`, as a thread.
fn reader_loop(
    mut rd: flint_tls::DuplexReader,
    w: flint_tls::DuplexWriter,
    pending: Arc<Mutex<VecDeque<Pending>>>,
    dead: Arc<AtomicBool>,
) {
    let mut acc: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut chunk = [0u8; 64 * 1024];
    let die = |why: String| {
        dead.store(true, Ordering::Relaxed);
        w.shutdown();
        // Envoy's close path: drain and fail every outstanding request. A
        // pooled failure takes down N callers where per-client took one, so
        // each must be TOLD rather than left to its timeout.
        if let Ok(mut q) = pending.lock() {
            let mut n = 0;
            while let Some(p) = q.pop_front() {
                let _ = p.tx.send(Err(std::io::Error::other(format!(
                    "{why} ({n} earlier replies delivered)"
                ))));
                n += 1;
            }
        }
    };
    loop {
        loop {
            match decode(&acc) {
                Ok(Decoded::Complete(v, used)) => {
                    acc.drain(..used);
                    let slot = pending.lock().ok().and_then(|mut q| q.pop_front());
                    match slot {
                        Some(p) => {
                            let _ = p.tx.send(Ok(v));
                        }
                        None => {
                            // A reply with no outstanding request: the stream
                            // is desynchronized and nothing on it can be
                            // trusted again.
                            return die("backend sent an unsolicited reply".into());
                        }
                    }
                }
                Ok(Decoded::NeedMore) => break,
                Err(e) => return die(format!("backend protocol error: {e:?}")),
            }
        }
        match rd.read(&mut chunk) {
            Ok(0) => return die("backend closed".into()),
            Ok(n) => acc.extend_from_slice(&chunk[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                // Idle poll tick: the liveness check. A backend that accepted
                // commands and answers nothing for a whole BACKEND_TIMEOUT is
                // dead for our purposes, however open its socket is.
                let stalled = pending
                    .lock()
                    .ok()
                    .and_then(|q| q.front().map(|p| p.at.elapsed() > BACKEND_TIMEOUT))
                    .unwrap_or(false);
                if stalled {
                    return die("backend stopped answering (oldest command over budget)".into());
                }
            }
            Err(e) => return die(format!("backend read failed: {e}")),
        }
    }
}

struct Lane {
    slots: Vec<Mutex<Option<Arc<Conn>>>>,
    rr: AtomicUsize,
}

pub(crate) struct BackendPool {
    lanes: Mutex<HashMap<Key, Arc<Lane>>>,
    tls: Option<Arc<flint_tls::ReloadableClientConfig>>,
    width: usize,
    pub stats: Arc<PoolStats>,
}

impl BackendPool {
    pub(crate) fn new(
        tls: Option<Arc<flint_tls::ReloadableClientConfig>>,
        width: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            lanes: Mutex::new(HashMap::new()),
            tls,
            width: width.max(1),
            stats: Arc::new(PoolStats::default()),
        })
    }

    /// Send one frame and wait for its reply, sharing the connection with
    /// every other client bound for the same node and namespace.
    pub(crate) fn call(
        &self,
        addr: &str,
        ns: &[u8],
        async_writes: bool,
        frame: &[u8],
    ) -> std::io::Result<Value> {
        let key = Key {
            addr: addr.to_string(),
            ns: ns.to_vec(),
            async_writes,
        };
        let lane = self.lane(&key)?;
        let i = lane.rr.fetch_add(1, Ordering::Relaxed) % lane.slots.len();
        let conn = {
            let mut slot = lane.slots[i]
                .lock()
                .map_err(|_| std::io::Error::other("lane slot poisoned"))?;
            match slot.as_ref() {
                Some(c) if !c.dead.load(Ordering::Relaxed) => c.clone(),
                _ => {
                    // Redial under the slot lock: a herd arriving at a dead
                    // connection produces one dial, not one per caller.
                    let c = Conn::dial(&key, &self.tls, &self.stats).inspect_err(|_| {
                        self.stats.dial_failures.fetch_add(1, Ordering::Relaxed);
                    })?;
                    *slot = Some(c.clone());
                    c
                }
            }
        };
        conn.submit(frame, &self.stats)
    }

    fn lane(&self, key: &Key) -> std::io::Result<Arc<Lane>> {
        let mut lanes = self
            .lanes
            .lock()
            .map_err(|_| std::io::Error::other("pool poisoned"))?;
        if let Some(l) = lanes.get(key) {
            return Ok(l.clone());
        }
        let lane = Arc::new(Lane {
            slots: (0..self.width).map(|_| Mutex::new(None)).collect(),
            rr: AtomicUsize::new(0),
        });
        lanes.insert(key.clone(), lane.clone());
        self.stats.lanes.store(lanes.len(), Ordering::Relaxed);
        Ok(lane)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flint_resp::encode;
    use std::io::{Read, Write};
    use std::net::TcpListener;

    /// A backend that answers the dial handshake immediately, then WITHHOLDS
    /// every data reply until it has received `n` complete data commands —
    /// after which it echoes each command's key back, in order.
    ///
    /// This is the discriminating test between the two pool designs. The
    /// dispatcher wrote a batch and then blocked reading its replies, so
    /// with staggered submitters its first batch was one command — which
    /// this backend never answers alone, and the whole pool deadlocked onto
    /// its timeout. A full-duplex pool keeps writing while blocked reading,
    /// so all `n` commands arrive and everything completes.
    fn withholding_backend(n: usize) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            let Ok((mut sock, _)) = listener.accept() else {
                return;
            };
            let mut buf: Vec<u8> = Vec::new();
            let mut chunk = [0u8; 16 * 1024];
            let mut keys: Vec<Vec<u8>> = Vec::new();
            let mut replied = false;
            loop {
                let r = match sock.read(&mut chunk) {
                    Ok(0) | Err(_) => return,
                    Ok(r) => r,
                };
                buf.extend_from_slice(&chunk[..r]);
                let mut out = Vec::new();
                while let Ok(Decoded::Complete(v, used)) = decode(&buf) {
                    buf.drain(..used);
                    let args: Vec<Vec<u8>> = match v {
                        Value::Array(Some(items)) => items
                            .into_iter()
                            .filter_map(|i| match i {
                                Value::Bulk(Some(b)) => Some(b),
                                _ => None,
                            })
                            .collect(),
                        _ => Vec::new(),
                    };
                    let name = args
                        .first()
                        .cloned()
                        .unwrap_or_default()
                        .to_ascii_uppercase();
                    if name == b"HELLO" {
                        // Raw RESP3 map: `encode` is RESP2 and would flatten
                        // it to the array the dial then rejects.
                        out.extend_from_slice(b"%0\r\n");
                    } else if name == b"FLINTNS" {
                        encode(&Value::Simple("OK".into()), &mut out);
                    } else {
                        keys.push(args.get(1).cloned().unwrap_or_default());
                    }
                }
                if !replied && keys.len() >= n {
                    replied = true;
                    for k in &keys {
                        encode(&Value::Bulk(Some(k.clone())), &mut out);
                    }
                }
                if !out.is_empty() && sock.write_all(&out).is_err() {
                    return;
                }
            }
        });
        addr
    }

    fn get_frame(key: &str) -> Vec<u8> {
        let mut v = Vec::new();
        encode(
            &Value::Array(Some(vec![
                Value::Bulk(Some(b"GET".to_vec())),
                Value::Bulk(Some(key.as_bytes().to_vec())),
            ])),
            &mut v,
        );
        v
    }

    #[test]
    fn all_commands_reach_a_withholding_backend_before_any_reply() {
        const N: usize = 8;
        let addr = withholding_backend(N);
        let p = BackendPool::new(None, 1); // width 1: ONE shared connection

        let mut handles = Vec::new();
        for i in 0..N {
            let p = p.clone();
            let a = addr.to_string();
            handles.push(std::thread::spawn(move || {
                // Staggered on purpose: the dispatcher design would write
                // command 0 alone and then block reading a reply that only
                // arrives after commands 1..N — which it would never send.
                std::thread::sleep(Duration::from_millis(30 * i as u64));
                let key = format!("k{i}");
                (key.clone(), p.call(&a, b"0", false, &get_frame(&key)))
            }));
        }
        for h in handles {
            let (key, got) = h.join().expect("join");
            match got {
                Ok(Value::Bulk(Some(b))) => assert_eq!(
                    b,
                    key.as_bytes(),
                    "a caller received another caller's reply — FIFO order \
                     and wire order disagreed"
                ),
                other => panic!(
                    "{key}: {other:?} — with a withholding backend this \
                     means the pool stopped writing to wait for replies"
                ),
            }
        }

        assert_eq!(p.stats.commands.load(Ordering::Relaxed), N as u64);
        assert_eq!(
            p.stats.inflight_max.load(Ordering::Relaxed),
            N as u64,
            "all {N} commands must have been in flight on one connection at \
             once — anything less means the connection was not full-duplex"
        );
    }

    #[test]
    fn a_dead_backend_fails_its_callers_rather_than_hanging_them() {
        let p = BackendPool::new(None, 1);
        let got = p.call("127.0.0.1:1", b"0", false, &get_frame("k"));
        assert!(got.is_err(), "a dial failure must surface to the caller");
        assert!(p.stats.dial_failures.load(Ordering::Relaxed) >= 1);
    }
}
