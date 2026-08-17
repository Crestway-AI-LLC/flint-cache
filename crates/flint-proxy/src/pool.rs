// SPDX-License-Identifier: Elastic-2.0
//! Shared, multiplexed backend connections (ADR-0020).
//!
//! # Why this exists
//!
//! Every client connection used to own its own backend sockets — `Backends`
//! was a local in `serve_client` — and `call_raw` wrote one frame then blocked
//! for its reply. A backend connection therefore carried **at most one
//! in-flight request**, so a client that pipelined got no pipelining on the
//! internal hop: measured off-box, pipelining amortised 1.1-1.6x for Flint
//! against 4.6x for a managed Valkey.
//!
//! An earlier attempt batched inside ONE client's read buffer and was reverted
//! (`1decd25` / `1c45e72`): a pipelining client runs a sliding window after
//! warmup, so each connection has ~1 command buffered at any instant. The
//! commands existed — spread across the other client connections and their own
//! sockets, where no per-connection batcher could reach them.
//!
//! # The shape, and what the prior art says about it
//!
//! Many client threads submit to FEW connections, each owned outright by one
//! dispatcher thread. The dispatcher takes whatever is queued, writes it as one
//! batch, and reads the replies back in order.
//!
//! - **twemproxy** batches across client connections onto a shared server
//!   connection (`server_connections` defaults to 1) and says plainly that
//!   "pipelining is the reason why twemproxy ends up doing better in terms of
//!   throughput".
//! - **Envoy's Redis proxy** documents the identical motivation — its buffer
//!   "makes it possible for multiple clients to send requests to Envoy and have
//!   them batched" — and offers `max_buffer_size_before_flush` with a
//!   `buffer_flush_timeout` defaulting to 3 ms.
//!
//! **We take the sharing and refuse the waiting.** Envoy's timer trades latency
//! for bigger batches, which suits a millisecond-scale service; our GET p50 is
//! ~0.3 ms end to end, so a 3 ms flush window would cost an order of magnitude
//! more than it could ever recover. This dispatcher therefore never sleeps to
//! accumulate: it drains what is *already* queued and writes immediately —
//! Envoy's behaviour with the buffer unset, where it "flushes on every data
//! reception". The batching is emergent from many clients sharing a connection,
//! not from delaying any of them.
//!
//! # Ordering
//!
//! No request ids. RESP replies arrive on a connection in request order, so
//! position is the correlation — the same invariant twemproxy and Envoy rely
//! on. It holds here because a pooled connection has exactly ONE writer (its
//! dispatcher), which is what makes "write order == reply order" true.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, sync_channel};
use std::time::{Duration, Instant};

use flint_resp::Value;

use crate::{BACKEND_TIMEOUT, dial_backend, read_reply};

/// Dispatchers per (address, namespace, async-writes) lane.
///
/// More than one because a batch occupies its connection for its whole
/// write-then-read cycle, so a slow command head-of-line blocks everything
/// behind it on that connection (ADR-0020's named trade). A small lane spreads
/// that risk without recreating the per-client socket explosion this replaces.
pub(crate) const DEFAULT_LANE_WIDTH: usize = 2;

/// Queue depth per dispatcher before submitters block.
///
/// Bounded on purpose: an unbounded queue in front of a wedged backend turns a
/// dead node into unbounded memory growth and hides the stall from the client,
/// which then waits far past the point it would have given up.
const QUEUE_DEPTH: usize = 1024;

/// Most commands written in one batch. Bounds the head-of-line exposure of the
/// first request in a batch and the size of one write.
const MAX_BATCH: usize = 128;

/// How long a submitter waits for its reply before giving up on the lane. Kept
/// above `BACKEND_TIMEOUT` so the dispatcher's own socket timeout fires first
/// and produces a real error rather than this bail-out, which cannot say why.
fn submit_budget() -> Duration {
    BACKEND_TIMEOUT + Duration::from_secs(5)
}

#[derive(Default)]
pub(crate) struct PoolStats {
    /// Batches written to a backend (one write syscall each).
    pub batches: AtomicU64,
    /// Commands carried by those batches. commands/batches is the mean batch
    /// size — the number that says whether multiplexing is doing anything, and
    /// exactly what the reverted attempt could not report about itself.
    pub commands: AtomicU64,
    /// Largest batch seen.
    pub batch_max: AtomicU64,
    /// Total microseconds commands spent queued before being written.
    pub queue_wait_us: AtomicU64,
    /// Live dispatcher lanes (distinct addr/ns/async-writes triples).
    pub lanes: AtomicUsize,
    /// Dial failures — a lane that cannot reach its backend.
    pub dial_failures: AtomicU64,
}

struct Job {
    frame: Vec<u8>,
    queued_at: Instant,
    reply: SyncSender<std::io::Result<Value>>,
}

#[derive(PartialEq, Eq, Hash, Clone)]
struct Key {
    addr: String,
    ns: Vec<u8>,
    async_writes: bool,
}

struct Lane {
    txs: Vec<SyncSender<Job>>,
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

    /// Send one frame and wait for its reply, sharing a connection with every
    /// other client bound for the same node and namespace.
    pub(crate) fn call(
        &self,
        addr: &str,
        ns: &[u8],
        async_writes: bool,
        frame: &[u8],
    ) -> std::io::Result<Value> {
        let lane = self.lane(addr, ns, async_writes)?;
        // Round-robin rather than least-loaded: picking the shortest queue
        // would need every submitter to read every dispatcher's depth on the
        // hot path, and the depths are stale the instant they are read.
        let i = lane.rr.fetch_add(1, Ordering::Relaxed) % lane.txs.len();
        let (tx, rx) = sync_channel(1);
        let job = Job {
            frame: frame.to_vec(),
            queued_at: Instant::now(),
            reply: tx,
        };
        lane.txs[i]
            .send(job)
            .map_err(|_| std::io::Error::other("backend lane closed"))?;
        match rx.recv_timeout(submit_budget()) {
            Ok(v) => v,
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "backend lane did not answer",
            )),
        }
    }

    fn lane(&self, addr: &str, ns: &[u8], async_writes: bool) -> std::io::Result<Arc<Lane>> {
        let key = Key {
            addr: addr.to_string(),
            ns: ns.to_vec(),
            async_writes,
        };
        let mut lanes = self
            .lanes
            .lock()
            .map_err(|_| std::io::Error::other("pool poisoned"))?;
        if let Some(l) = lanes.get(&key) {
            return Ok(l.clone());
        }
        let mut txs = Vec::with_capacity(self.width);
        for _ in 0..self.width {
            let (tx, rx) = sync_channel::<Job>(QUEUE_DEPTH);
            let k = key.clone();
            let tls = self.tls.clone();
            let stats = self.stats.clone();
            std::thread::spawn(move || dispatch(k, rx, tls, stats));
            txs.push(tx);
        }
        let lane = Arc::new(Lane {
            txs,
            rr: AtomicUsize::new(0),
        });
        lanes.insert(key, lane.clone());
        self.stats.lanes.store(lanes.len(), Ordering::Relaxed);
        Ok(lane)
    }
}

/// One dispatcher: owns a backend connection outright for the process's life.
///
/// Sole ownership is the point. `flint_tls::Stream` wraps a `rustls`
/// `StreamOwned`, whose reads and writes both need `&mut` on one object, so a
/// reader thread and a writer thread cannot share a TLS connection without
/// taking a lock across blocking IO. Giving one thread the whole connection
/// sidesteps that and makes reply ordering trivially true.
fn dispatch(
    key: Key,
    rx: Receiver<Job>,
    tls: Option<Arc<flint_tls::ReloadableClientConfig>>,
    stats: Arc<PoolStats>,
) {
    let mut conn: Option<(flint_tls::Stream, Vec<u8>)> = None;
    loop {
        // Block for the first job; everything after it is taken WITHOUT
        // waiting. This is the whole batching policy — see the module header
        // on why there is deliberately no flush timer.
        let Ok(first) = rx.recv() else {
            return; // pool dropped: no more submitters
        };
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH {
            match rx.try_recv() {
                Ok(j) => batch.push(j),
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        }

        stats.batches.fetch_add(1, Ordering::Relaxed);
        stats
            .commands
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        stats
            .batch_max
            .fetch_max(batch.len() as u64, Ordering::Relaxed);
        let waited: u64 = batch
            .iter()
            .map(|j| j.queued_at.elapsed().as_micros() as u64)
            .sum();
        stats.queue_wait_us.fetch_add(waited, Ordering::Relaxed);

        if conn.is_none() {
            match dial_backend(&key.addr, &key.ns, key.async_writes, &tls) {
                Ok(s) => conn = Some((s, Vec::new())),
                Err(e) => {
                    stats.dial_failures.fetch_add(1, Ordering::Relaxed);
                    fail_all(batch, &e, 0);
                    continue;
                }
            }
        }
        let Some((stream, buf)) = conn.as_mut() else {
            continue;
        };

        let mut wire = Vec::with_capacity(batch.iter().map(|j| j.frame.len()).sum());
        for j in &batch {
            wire.extend_from_slice(&j.frame);
        }
        if let Err(e) = std::io::Write::write_all(stream, &wire) {
            conn = None;
            fail_all(batch, &e, 0);
            continue;
        }

        // Read every reply BEFORE handing any back. A short read leaves the
        // socket holding replies that the next batch would mistake for its
        // own, so the connection is dropped whole rather than reused.
        let mut replies = Vec::with_capacity(batch.len());
        let mut failure: Option<std::io::Error> = None;
        for _ in 0..batch.len() {
            match read_reply(stream, buf) {
                Ok(v) => replies.push(v),
                Err(e) => {
                    failure = Some(e);
                    break;
                }
            }
        }
        match failure {
            None => {
                for (j, v) in batch.into_iter().zip(replies) {
                    let _ = j.reply.send(Ok(v));
                }
            }
            Some(e) => {
                let done = replies.len();
                let mut it = batch.into_iter();
                for v in replies {
                    if let Some(j) = it.next() {
                        let _ = j.reply.send(Ok(v));
                    }
                }
                fail_all(it.collect(), &e, done);
                conn = None;
            }
        }
    }
}

/// Fail every job that will not get an answer. A pooled connection failure
/// takes down N requests where the per-client design took down one, so each
/// caller must be told rather than left on its timeout.
fn fail_all(jobs: Vec<Job>, cause: &std::io::Error, already_served: usize) {
    for j in jobs {
        let _ = j.reply.send(Err(std::io::Error::new(
            cause.kind(),
            format!("backend batch failed after {already_served} replies: {cause}"),
        )));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flint_resp::{Decoded, decode, encode};
    use std::io::{Read, Write};
    use std::net::TcpListener;

    /// A backend that completes the dial handshake and then echoes each
    /// command's key back, recording how many complete commands arrived in
    /// each read.
    ///
    /// `stall` makes the FIRST reply slow. That is what turns this from a
    /// hopeful test into a deterministic one: while the dispatcher is blocked
    /// reading, every other submitter piles into the queue, so the next batch
    /// is necessarily larger than one. Without it the test would pass or fail
    /// on thread scheduling.
    fn echo_backend(seen: Arc<Mutex<Vec<usize>>>, stall: Duration) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        std::thread::spawn(move || {
            let Ok((mut sock, _)) = listener.accept() else {
                return;
            };
            let mut buf: Vec<u8> = Vec::new();
            let mut chunk = [0u8; 16 * 1024];
            let mut handshakes = 0;
            let mut stalled = false;
            loop {
                let n = match sock.read(&mut chunk) {
                    Ok(0) | Err(_) => return,
                    Ok(n) => n,
                };
                buf.extend_from_slice(&chunk[..n]);
                let mut out = Vec::new();
                let mut in_this_read = 0usize;
                while let Ok(Decoded::Complete(v, used)) = decode(&buf) {
                    buf.drain(..used);
                    let args: Vec<Vec<u8>> = match v {
                        flint_resp::Value::Array(Some(items)) => items
                            .into_iter()
                            .filter_map(|i| match i {
                                flint_resp::Value::Bulk(Some(b)) => Some(b),
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
                        handshakes += 1;
                        // Raw RESP3 map frame. `encode` is RESP2 and flattens
                        // a map to an array, which is precisely the ambiguity
                        // the backend handshake speaks RESP3 to avoid — so
                        // encoding one here would make the test fail for the
                        // reason the production code exists to prevent.
                        out.extend_from_slice(b"%0\r\n");
                    } else if name == b"FLINTNS" {
                        handshakes += 1;
                        encode(&flint_resp::Value::Simple("OK".into()), &mut out);
                    } else {
                        // Echo the KEY so a caller can prove it got its own
                        // reply and not its neighbour's.
                        in_this_read += 1;
                        let key = args.get(1).cloned().unwrap_or_default();
                        encode(&flint_resp::Value::Bulk(Some(key)), &mut out);
                    }
                }
                if in_this_read > 0 {
                    seen.lock().expect("seen").push(in_this_read);
                    if !stalled {
                        stalled = true;
                        std::thread::sleep(stall);
                    }
                }
                let _ = handshakes;
                if sock.write_all(&out).is_err() {
                    return;
                }
            }
        });
        addr
    }

    fn get_frame(key: &str) -> Vec<u8> {
        let mut v = Vec::new();
        encode(
            &flint_resp::Value::Array(Some(vec![
                flint_resp::Value::Bulk(Some(b"GET".to_vec())),
                flint_resp::Value::Bulk(Some(key.as_bytes().to_vec())),
            ])),
            &mut v,
        );
        v
    }

    #[test]
    fn concurrent_clients_share_a_connection_and_get_their_own_replies() {
        const CLIENTS: usize = 24;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let addr = echo_backend(seen.clone(), Duration::from_millis(80));
        // Width 1: every client converges on ONE connection, which is the
        // configuration under test — the whole claim is that sharing is what
        // creates a batch.
        let p = BackendPool::new(None, 1);

        let mut handles = Vec::new();
        for i in 0..CLIENTS {
            let p = p.clone();
            let a = addr.to_string();
            handles.push(std::thread::spawn(move || {
                let key = format!("k{i}");
                let got = p.call(&a, b"0", false, &get_frame(&key));
                (key, got)
            }));
        }
        for h in handles {
            let (key, got) = h.join().expect("join");
            match got {
                Ok(flint_resp::Value::Bulk(Some(b))) => assert_eq!(
                    b,
                    key.as_bytes(),
                    "a caller received another caller's reply — pooled replies \
                     are correlated by POSITION, so this means write order and \
                     queue order disagreed"
                ),
                other => panic!("{key}: unexpected reply {other:?}"),
            }
        }

        let sizes = seen.lock().expect("seen").clone();
        let max = sizes.iter().copied().max().unwrap_or(0);
        assert!(
            max > 1,
            "no batch ever formed (sizes {sizes:?}) — clients sharing one \
             connection must coalesce, or this pool is just the old \
             one-request-at-a-time path with extra threads"
        );
        assert_eq!(
            p.stats.commands.load(Ordering::Relaxed),
            CLIENTS as u64,
            "every command must be accounted for exactly once"
        );
        assert!(p.stats.batches.load(Ordering::Relaxed) < CLIENTS as u64);
    }

    #[test]
    fn a_dead_backend_fails_its_callers_rather_than_hanging_them() {
        // Nothing listening: the dial fails, and the submitter must be told.
        // Silence here would strand a client thread for the whole submit
        // budget with no explanation.
        let p = BackendPool::new(None, 1);
        let got = p.call("127.0.0.1:1", b"0", false, &get_frame("k"));
        assert!(got.is_err(), "a dial failure must surface to the caller");
        assert!(p.stats.dial_failures.load(Ordering::Relaxed) >= 1);
    }
}
