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
//! In-flight depth IS capped, by the caller. This module originally argued a
//! cap was unnecessary because `submit` is synchronous per caller, so pending
//! could not exceed the live client-connection count. That stopped being true
//! when `stage` was split out so one caller could hold a whole pipeline in
//! flight: depth became connections x pipeline depth. Two bounds replace the
//! lost invariant — `serve_client` limits one prefetch pass (`MAX_PREFETCH`),
//! and, because several callers share a connection, `MAX_INFLIGHT` limits what
//! any ONE connection may carry. The per-connection bound is the load-bearing
//! one: a per-pass limit alone still let 8 clients stack 255 commands onto a
//! single socket and drive the node out of its lease.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flint_resp::{Decoded, Value, decode};

use crate::{BACKEND_TIMEOUT, dial_backend};

/// Connections per (address, namespace, async-writes) lane. More than one
/// spreads slow-command exposure across sockets without recreating the
/// per-client socket explosion this pool replaces.
///
/// Raised from 2 to 8 on 2026-08-17, when staging made queue depth per
/// connection matter. Measured on one fleet, 8 client connections, 1 KB GETs:
///
/// | | width 2 | width 8 |
/// |---|---|---|
/// | pipeline 16 | 228k ops/s, p99 2.27 ms | 268k ops/s, p99 0.86 ms |
/// | pipeline 64 | 196k ops/s, p99 104.70 ms | 289k ops/s, p99 11.63 ms |
///
/// The p99 column is the reason. Head-of-line blocking was ADR-0020's named
/// risk, and a narrow lane is what realises it: every client sharing a socket
/// waits behind whatever is queued ahead of it, so a 32-deep queue on two
/// sockets turns one slow command into a hundred-millisecond tail. Eight
/// sockets per node is still far below the per-client connection count this
/// pool replaced.
pub(crate) const DEFAULT_LANE_WIDTH: usize = 8;

/// The reader's socket poll. Short, so liveness checks run even when the
/// backend is silent; each wakeup is two syscalls on an idle connection.
const READ_POLL: Duration = Duration::from_secs(1);

/// Most commands one pooled connection may have outstanding at once.
///
/// `submit` is self-limiting: one command per caller thread, so depth could
/// never exceed the live client-connection count. `stage` deliberately breaks
/// that — a caller puts its whole pipeline in flight — and several callers
/// share a connection, so depth becomes callers x pipeline depth.
///
/// Measured 2026-08-17: 8 client connections at pipeline depth 64 put **255**
/// commands on ONE socket. That saturated the node hard enough to starve its
/// lease renewal — it self-fenced to read-only ("partitioned from
/// controllers") three times, the proxy lost its master, and throughput
/// collapsed to zero for seconds at a stretch. The same load bounded is
/// stable, and one connection at depth 64 serves 215k ops/s through the
/// proxy.
///
/// So this is a safety bound on how deep a client may drive a shared backend
/// queue, not a tuning knob. Staging stops at the cap and the remaining
/// commands take the ordinary serial path, which is self-limiting and
/// therefore its own backpressure.
const MAX_INFLIGHT: usize = 32;

/// How long a submitter waits on its slot. Above BACKEND_TIMEOUT so the
/// reader's no-progress liveness check fires first and produces an error that
/// can say why.
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
    /// Diagnostics only. A pooled connection's death is a fleet-visible event
    /// — it fails every caller on it at once — so it must be attributable to
    /// an address in a log, not just to whoever happened to receive the error.
    addr: String,
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
            addr: key.addr.clone(),
        });
        let addr = key.addr.clone();
        std::thread::spawn(move || reader_loop(rd, w, pending, dead, addr));
        let _ = stats; // lanes gauge is maintained by the pool map
        Ok(conn)
    }

    /// Stage one command — push its reply slot and append its frame — and
    /// **return**. No socket IO, no waiting: this is `makeRequestInternal`.
    ///
    /// Splitting this out of `submit` is what lets ONE caller hold N commands
    /// in flight. `submit` (stage, flush, park) can only ever have one
    /// outstanding command per thread, so a client pipelining 16 GETs reached
    /// the node as 16 separate round trips and the node saw no pipeline at
    /// all — measured 2026-08-17, and the reason `pool_batch_mean` sat at 1.0
    /// no matter how the load was shaped.
    ///
    /// `cap` bounds how many commands may be outstanding here (see
    /// `MAX_INFLIGHT`). `None` is the self-limiting serial path, which adds at
    /// most one command per caller thread and so needs no bound.
    fn stage(
        &self,
        frame: &[u8],
        stats: &PoolStats,
        cap: Option<usize>,
    ) -> std::io::Result<Receiver<std::io::Result<Value>>> {
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
                // Checked under the same lock as the push, so the bound holds
                // against concurrent stagers rather than merely usually.
                if let Some(cap) = cap
                    && q.len() >= cap
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "backend connection at in-flight capacity",
                    ));
                }
                q.push_back(Pending { tx });
                stats
                    .inflight_max
                    .fetch_max(q.len() as u64, Ordering::Relaxed);
            }
            // Memory-only: encrypt/stage. An error here poisons the stream
            // state, so the whole connection dies rather than desyncs.
            if let Err(e) = self.w.append(frame) {
                self.dead.store(true, Ordering::Relaxed);
                self.w.shutdown();
                eprintln!("pool: backend {} staging failed: {e}", self.addr);
                return Err(e);
            }
        }
        stats.commands.fetch_add(1, Ordering::Relaxed);
        Ok(rx)
    }

    /// Push everything staged onto the socket, OUTSIDE the ordering lock:
    /// whoever gets there first carries every staged frame (flush-combining).
    ///
    /// Idempotent and nearly free when there is nothing to write — another
    /// thread's flush may already have carried our frames, which is precisely
    /// the coalescing working. So a caller that staged N commands can simply
    /// flush once per ticket without tracking which connections it touched.
    fn flush_staged(&self, stats: &PoolStats) -> std::io::Result<()> {
        match self.w.flush() {
            Ok(0) => Ok(()),
            Ok(_) => {
                stats.batches.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.dead.store(true, Ordering::Relaxed);
                self.w.shutdown();
                eprintln!("pool: backend {} flush failed: {e}", self.addr);
                Err(e)
            }
        }
    }

    /// Park on one staged command's slot until the reader completes it.
    fn wait_slot(&self, rx: &Receiver<std::io::Result<Value>>) -> std::io::Result<Value> {
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

    /// One command: stage, flush, wait on our own slot. The connection is
    /// never ours — between our flush and our reply, any number of other
    /// commands travel on it.
    fn submit(&self, frame: &[u8], stats: &PoolStats) -> std::io::Result<Value> {
        let rx = self.stage(frame, stats, None)?;
        self.flush_staged(stats)?;
        self.wait_slot(&rx)
    }
}

/// ONE connection, held for the duration of one prefetch pass.
///
/// The connection is chosen once, when the lease is taken, and every command
/// in the pass is staged onto it — that is what makes a pass a batch. Staging
/// through `call`/`conn_for` per command instead advances the lane's
/// round-robin on every command and scatters a 16-command run across all 8
/// lane connections, two apiece.
///
/// That is not hypothetical, it shipped: raising the lane width from 2 to 8 on
/// local throughput and p99 evidence silently disabled batching, and the fleet
/// measured `pool_batch_mean` 1.04 against 4.77-4.96 locally at width 2. The
/// change had stopped doing the one thing it exists to do, while looking
/// faster on the metrics being watched.
///
/// Round-robin still applies BETWEEN passes, so concurrent clients spread
/// across the lane; it just no longer runs inside one client's pipeline.
pub(crate) struct PoolLease {
    conn: Arc<Conn>,
    stats: Arc<PoolStats>,
}

impl PoolLease {
    /// Stage one command of this pass. `WouldBlock` means the connection is at
    /// its in-flight cap: the caller should stop staging and let the rest of
    /// the run take the ordinary serial path.
    pub(crate) fn stage(&self, frame: &[u8]) -> std::io::Result<Ticket> {
        let rx = self.conn.stage(frame, &self.stats, Some(MAX_INFLIGHT))?;
        Ok(Ticket {
            conn: self.conn.clone(),
            rx,
            stats: self.stats.clone(),
        })
    }
}

/// A command staged on a pooled connection but not yet answered.
///
/// The caller holds one per in-flight command, flushes, and then collects in
/// issue order. Holding several at once is the whole point: that is what puts
/// a client's pipeline onto the wire as a pipeline instead of N round trips.
pub(crate) struct Ticket {
    conn: Arc<Conn>,
    rx: Receiver<std::io::Result<Value>>,
    stats: Arc<PoolStats>,
}

impl Ticket {
    /// Push this ticket's frame — and anything else staged on its connection
    /// — onto the wire. Safe to call once per ticket even when several
    /// tickets share a connection; the later calls find nothing staged.
    pub(crate) fn flush(&self) -> std::io::Result<()> {
        self.conn.flush_staged(&self.stats)
    }

    /// Block for this command's reply.
    pub(crate) fn wait(self) -> std::io::Result<Value> {
        self.conn.wait_slot(&self.rx)
    }
}

/// The read half of every pooled connection: decode replies continuously,
/// complete the FIFO front — `onRespValue`, as a thread.
fn reader_loop(
    mut rd: flint_tls::DuplexReader,
    w: flint_tls::DuplexWriter,
    pending: Arc<Mutex<VecDeque<Pending>>>,
    dead: Arc<AtomicBool>,
    addr: String,
) {
    let mut acc: Vec<u8> = Vec::with_capacity(64 * 1024);
    let mut chunk = [0u8; 64 * 1024];
    // When this connection last DELIVERED something. The liveness check below
    // is written against progress rather than against the age of the oldest
    // outstanding command — see the check for why the difference matters.
    let mut last_reply = Instant::now();
    let die = |why: String| {
        dead.store(true, Ordering::Relaxed);
        w.shutdown();
        // Envoy's close path: drain and fail every outstanding request. A
        // pooled failure takes down N callers where per-client took one, so
        // each must be TOLD rather than left to its timeout.
        let mut n = 0;
        if let Ok(mut q) = pending.lock() {
            while let Some(p) = q.pop_front() {
                let _ = p.tx.send(Err(std::io::Error::other(format!(
                    "{why} ({n} earlier replies delivered)"
                ))));
                n += 1;
            }
        }
        // LOG IT. A pooled connection's death fails every caller on it at
        // once, and each of those callers independently asks the topology to
        // rediscover — so one death here becomes N re-probes and a routing
        // flap. On a fleet that presented as a throughput collapse with
        // multi-second tails, and the trigger could not be identified from
        // the proxy log at all, because this reason string only ever reached
        // the callers. `failed=` is the amplification factor, measured.
        eprintln!("pool: backend {addr} connection died: {why} (failed={n} in flight)");
    };
    loop {
        loop {
            match decode(&acc) {
                Ok(Decoded::Complete(v, used)) => {
                    acc.drain(..used);
                    last_reply = Instant::now();
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
                // Idle poll tick: the liveness check. "Dead" means the backend
                // has DELIVERED NOTHING for a whole BACKEND_TIMEOUT while we
                // are waiting on it — not that the oldest outstanding command
                // is old.
                //
                // Those were the same statement while a caller could only ever
                // have one command outstanding. Once a caller stages its whole
                // pipeline the oldest command is legitimately old for as long
                // as the batch takes to drain, so measuring its age shoots a
                // healthy connection under precisely the load this pool exists
                // to carry — and takes every other caller on it down too.
                let waiting = pending.lock().map(|q| !q.is_empty()).unwrap_or(false);
                if waiting && last_reply.elapsed() > BACKEND_TIMEOUT {
                    return die("backend stopped answering (no reply for the whole budget)".into());
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
        self.conn_for(addr, ns, async_writes)?
            .submit(frame, &self.stats)
    }

    /// Stage a command and return its ticket WITHOUT flushing or waiting.
    ///
    /// The caller is expected to stage a whole run, flush, and only then
    /// collect — see `Ticket`. Nothing here blocks, so a single client thread
    /// can put its entire pipeline in flight before reading the first reply.
    pub(crate) fn lease(
        &self,
        addr: &str,
        ns: &[u8],
        async_writes: bool,
    ) -> std::io::Result<PoolLease> {
        Ok(PoolLease {
            conn: self.conn_for(addr, ns, async_writes)?,
            stats: self.stats.clone(),
        })
    }

    /// Pick this lane's next connection, dialing if the slot is empty or its
    /// occupant is dead. One dial implementation for both call shapes.
    fn conn_for(&self, addr: &str, ns: &[u8], async_writes: bool) -> std::io::Result<Arc<Conn>> {
        let key = Key {
            addr: addr.to_string(),
            ns: ns.to_vec(),
            async_writes,
        };
        let lane = self.lane(&key)?;
        let i = lane.rr.fetch_add(1, Ordering::Relaxed) % lane.slots.len();
        let mut slot = lane.slots[i]
            .lock()
            .map_err(|_| std::io::Error::other("lane slot poisoned"))?;
        match slot.as_ref() {
            Some(c) if !c.dead.load(Ordering::Relaxed) => Ok(c.clone()),
            _ => {
                // Redial under the slot lock: a herd arriving at a dead
                // connection produces one dial, not one per caller.
                let c = Conn::dial(&key, &self.tls, &self.stats).inspect_err(|_| {
                    self.stats.dial_failures.fetch_add(1, Ordering::Relaxed);
                })?;
                *slot = Some(c.clone());
                Ok(c)
            }
        }
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

    /// ONE caller's pipeline must reach the backend as a pipeline.
    ///
    /// This is the property the pool was missing for a month. The test below
    /// proves N THREADS can be in flight together, and it passed the whole
    /// time — but every caller used `submit`, which stages one frame and
    /// parks, so a single client pipelining N commands still produced N round
    /// trips. Measured on a fleet and a laptop alike: `pool_batch_mean` stuck
    /// at 1.0, `pool_inflight_max` tracking the CONNECTION count, and the node
    /// paying its unpipelined per-command cost at every pipeline depth.
    ///
    /// The withholding backend answers nothing until all N commands arrive,
    /// so a caller that parks after each `stage` cannot finish: command 0's
    /// reply only exists once command N-1 has been written.
    #[test]
    fn one_caller_can_hold_its_whole_pipeline_in_flight() {
        const N: usize = 16;
        let addr = withholding_backend(N);
        // Lane width 8 ON PURPOSE. The bug this pins shipped: `conn_for`
        // round-robins per CALL, so staging command-by-command through the
        // pool spread one client's run across all 8 connections, two apiece,
        // and `pool_batch_mean` fell from 4.9 to 1.04 on a fleet. A run must
        // land on ONE connection whatever the lane width, so a width of 1
        // could not have caught it — and the backend here accepts a single
        // connection, so a scattering implementation cannot even complete.
        let p = BackendPool::new(None, 8);
        let a = addr.to_string();

        // Stage the whole run first — no waiting — exactly as the prefetch
        // pass in serve_client does.
        // ONE lease for the whole run — the connection is chosen once, which
        // is what makes the run a batch rather than N scattered commands.
        let lease = p.lease(&a, b"0", false).expect("lease");
        let mut tickets = Vec::new();
        for i in 0..N {
            let key = format!("k{i}");
            let t = lease.stage(&get_frame(&key)).expect("stage");
            tickets.push((key, t));
        }
        for (_, t) in &tickets {
            t.flush().expect("flush");
        }
        assert_eq!(
            p.stats.inflight_max.load(Ordering::Relaxed),
            N as u64,
            "one caller must be able to hold all {N} commands in flight; \
             a lower depth means it parked between commands and the client's \
             pipeline was serialised into round trips"
        );

        // Only now collect, in issue order.
        for (key, t) in tickets {
            match t.wait() {
                Ok(Value::Bulk(Some(b))) => assert_eq!(
                    b,
                    key.as_bytes(),
                    "reply for {key} went to the wrong caller — FIFO order and \
                     wire order disagreed"
                ),
                other => panic!(
                    "{key}: {other:?} — with a withholding backend this means \
                     not every staged command reached the node"
                ),
            }
        }
        assert_eq!(p.stats.commands.load(Ordering::Relaxed), N as u64);
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
