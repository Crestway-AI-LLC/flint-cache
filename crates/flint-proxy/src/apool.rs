// SPDX-License-Identifier: Elastic-2.0
//! Backend connections owned by ONE worker (ADR-0021).
//!
//! # What this replaces and why
//!
//! [`crate::pool`] shares each backend connection between every proxy thread
//! routing to that node. Measured on a fleet 2026-08-17, that is where the
//! design falls over — everything degrades in the ratio
//! `proxy threads / pooled connections`, which is 1 at 8 clients and 8 at 64:
//!
//! | client conns | shared pool | + staging | p99.9 |
//! |---|---|---|---|
//! | 8 | 92,008 | 186,735 | 1.27 ms |
//! | 32 | 134,847 | 47,780 | 2,162 ms |
//! | 64 | 105,792 | 12,266 | 10,027 ms |
//!
//! Three effects compound in that ratio: a lock convoy (four mutexes per
//! command, with TLS encryption inside one of them), failure amplification
//! (one death fails every in-flight caller, each of which then re-probed the
//! topology — 198 routing transitions against 0 on the serial arm), and
//! head-of-line blocking behind strangers' requests.
//!
//! # The fix is ownership, not tuning
//!
//! Envoy's Redis client avoids all three because its upstream connection is
//! owned by one worker and shared with nobody. It is tempting to call that
//! "thread-local" and copy it, but Envoy reaches few owners by having FEW
//! THREADS, each multiplexing many clients; copying thread-locality into a
//! thread-per-client proxy yields one backend connection per client, which is
//! the socket explosion ADR-0020 removed.
//!
//! So this module assumes ADR-0021's model: a bounded set of workers, each a
//! single-threaded runtime owning its own connections. Two consequences:
//!
//! - **State needs no synchronisation.** `Rc`/`RefCell`/`Cell` express the
//!   real invariant — one accessor at a time — with none of the convoy. Every
//!   borrow ends before an await, enforced by
//!   `deny(clippy::await_holding_refcell_ref)` at the crate root.
//! - **Writes still need mutual exclusion**, because several client tasks on
//!   the SAME worker may flush concurrently and a half-written frame would
//!   desynchronise the stream. That is a `tokio::sync::Mutex` — async-aware,
//!   so holding it across the write is correct rather than a hazard — and it
//!   is contended only by that one worker's tasks, never by 32 OS threads.
//!
//! Correlation needs no request ids: RESP replies arrive in request order, so
//! position is the whole correspondence. True of the shared pool, of
//! twemproxy, and of Envoy.

// TRANSITIONAL. This module is complete and tested but not yet reachable from
// `serve_client`, which is still blocking — the edge conversion is the other
// half of ADR-0021 stage 2. Remove this the moment the edge calls in; a stale
// allow(dead_code) is how an unused module survives long enough to rot.
#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

use flint_resp::{Decoded, Value, decode};
use flint_tls::aio::AsyncStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt, WriteHalf};
use tokio::sync::oneshot;

/// Replies outstanding on one connection, in request order.
type Pending = Rc<RefCell<VecDeque<oneshot::Sender<std::io::Result<Value>>>>>;

pub(crate) struct AsyncConn {
    /// Serialises writers on this worker. Async-aware on purpose: it is held
    /// across the socket write, which is exactly what a std mutex must never
    /// do and what this one is for.
    w: tokio::sync::Mutex<WriteHalf<AsyncStream>>,
    /// Frames staged but not yet written. A whole client pipeline is staged
    /// and written once — the batching ADR-0020 wanted, here costing no
    /// coordination beyond one worker-local borrow.
    staged: RefCell<Vec<u8>>,
    pending: Pending,
    dead: Rc<Cell<bool>>,
}

impl AsyncConn {
    /// Split the stream, keep the write half, and hand the read half to a
    /// local task that completes the pending FIFO — Envoy's `onRespValue`.
    pub(crate) fn new(stream: AsyncStream, addr: String) -> Rc<Self> {
        let (mut rd, wr) = tokio::io::split(stream);
        let pending: Pending = Rc::new(RefCell::new(VecDeque::new()));
        let dead = Rc::new(Cell::new(false));
        let conn = Rc::new(AsyncConn {
            w: tokio::sync::Mutex::new(wr),
            staged: RefCell::new(Vec::with_capacity(16 * 1024)),
            pending: pending.clone(),
            dead: dead.clone(),
        });

        tokio::task::spawn_local(async move {
            let mut acc: Vec<u8> = Vec::with_capacity(64 * 1024);
            let mut chunk = [0u8; 64 * 1024];
            let why = 'read: loop {
                // Drain every complete reply already buffered before reading
                // again — a pipelined batch arrives in one or two reads.
                loop {
                    let step = decode(&acc);
                    match step {
                        Ok(Decoded::Complete(v, used)) => {
                            acc.drain(..used);
                            let slot = pending.borrow_mut().pop_front();
                            match slot {
                                Some(tx) => {
                                    let _ = tx.send(Ok(v));
                                }
                                None => {
                                    break 'read "backend sent an unsolicited reply".to_string();
                                }
                            }
                        }
                        Ok(Decoded::NeedMore) => break,
                        Err(e) => break 'read format!("backend protocol error: {e:?}"),
                    }
                }
                match rd.read(&mut chunk).await {
                    Ok(0) => break "backend closed".to_string(),
                    Ok(n) => acc.extend_from_slice(&chunk[..n]),
                    Err(e) => break format!("backend read failed: {e}"),
                }
            };
            // Drain and fail everything pending, as Envoy's close path does.
            // A shared connection's failure takes down N callers where a
            // private socket took one, so each must be TOLD rather than left
            // to a timeout.
            dead.set(true);
            let mut n = 0;
            loop {
                let slot = pending.borrow_mut().pop_front();
                let Some(tx) = slot else { break };
                let _ = tx.send(Err(std::io::Error::other(format!(
                    "{why} ({n} earlier replies delivered)"
                ))));
                n += 1;
            }
            // The trigger, not just the storm. The shared pool logged neither,
            // so a fleet collapse could be observed but never explained.
            eprintln!("apool: backend {addr} died: {why} (failed={n} in flight)");
        });
        conn
    }

    /// Stage one command and return immediately — Envoy's
    /// `makeRequestInternal`. Nothing awaits, so one caller can put a whole
    /// pipeline in flight before collecting any of it.
    pub(crate) fn stage(
        &self,
        frame: &[u8],
    ) -> std::io::Result<oneshot::Receiver<std::io::Result<Value>>> {
        if self.dead.get() {
            return Err(std::io::Error::other("backend connection is down"));
        }
        let (tx, rx) = oneshot::channel();
        // Slot pushed and frame appended together, so FIFO order IS wire
        // order. Both borrows end here; neither can reach an await.
        self.pending.borrow_mut().push_back(tx);
        self.staged.borrow_mut().extend_from_slice(frame);
        Ok(rx)
    }

    /// Write everything staged. Whoever gets the write lock first carries
    /// every frame staged so far, so concurrent stagers on this worker
    /// coalesce into one syscall rather than queueing behind each other.
    pub(crate) async fn flush(&self) -> std::io::Result<()> {
        let buf = {
            let mut s = self.staged.borrow_mut();
            if s.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *s)
        };
        let mut w = self.w.lock().await;
        w.write_all(&buf).await
    }

    pub(crate) fn is_dead(&self) -> bool {
        self.dead.get()
    }

    pub(crate) fn inflight(&self) -> usize {
        self.pending.borrow().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flint_resp::encode;
    use tokio::net::TcpListener;

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

    /// A backend that answers NOTHING until it has received `n` complete
    /// commands, then echoes each key back in order.
    ///
    /// This is the discriminating fixture. An implementation that parks after
    /// each command cannot finish: command 0's reply only exists once command
    /// n-1 has been written. It is exactly the shape the proxy's first pooled
    /// backend failed, and the shape a client's pipeline actually has.
    async fn withholding_backend(n: usize) -> String {
        let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = l.local_addr().expect("addr").to_string();
        tokio::spawn(async move {
            let (mut sock, _) = l.accept().await.expect("accept");
            let mut buf: Vec<u8> = Vec::new();
            let mut chunk = [0u8; 16 * 1024];
            let mut keys: Vec<Vec<u8>> = Vec::new();
            while keys.len() < n {
                let r = match sock.read(&mut chunk).await {
                    Ok(0) | Err(_) => return,
                    Ok(r) => r,
                };
                buf.extend_from_slice(&chunk[..r]);
                while let Ok(Decoded::Complete(v, used)) = decode(&buf) {
                    buf.drain(..used);
                    if let Value::Array(Some(items)) = v
                        && let Some(Value::Bulk(Some(k))) = items.get(1)
                    {
                        keys.push(k.clone());
                    }
                }
            }
            let mut out = Vec::new();
            for k in &keys {
                encode(&Value::Bulk(Some(k.clone())), &mut out);
            }
            let _ = sock.write_all(&out).await;
        });
        addr
    }

    fn run<F: std::future::Future<Output = ()> + 'static>(f: F) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        tokio::task::LocalSet::new().block_on(&rt, f);
    }

    #[test]
    fn one_caller_holds_its_whole_pipeline_in_flight() {
        const N: usize = 16;
        run(async {
            let addr = withholding_backend(N).await;
            let s = flint_tls::aio::connect(&addr, &None).await.expect("dial");
            let conn = AsyncConn::new(s, addr);

            let mut rxs = Vec::new();
            for i in 0..N {
                rxs.push((
                    format!("k{i}"),
                    conn.stage(&get_frame(&format!("k{i}"))).expect("stage"),
                ));
            }
            assert_eq!(
                conn.inflight(),
                N,
                "one caller must hold all {N} commands in flight; a lower depth \
                 means it parked between commands and the pipeline was \
                 serialised into round trips"
            );
            conn.flush().await.expect("flush");

            for (key, rx) in rxs {
                match rx.await.expect("reply") {
                    Ok(Value::Bulk(Some(b))) => assert_eq!(
                        b,
                        key.as_bytes(),
                        "reply for {key} went to the wrong caller — FIFO order \
                         and wire order disagreed"
                    ),
                    other => panic!("{key}: {other:?} — not every staged command reached the node"),
                }
            }
        });
    }

    #[test]
    fn a_dead_backend_fails_every_caller_rather_than_hanging_them() {
        run(async {
            // Accepts, then drops the connection without replying.
            let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = l.local_addr().expect("addr").to_string();
            tokio::spawn(async move {
                let _ = l.accept().await;
            });
            let s = flint_tls::aio::connect(&addr, &None).await.expect("dial");
            let conn = AsyncConn::new(s, addr);

            let rxs: Vec<_> = (0..8)
                .map(|i| conn.stage(&get_frame(&format!("k{i}"))).expect("stage"))
                .collect();
            let _ = conn.flush().await;
            for rx in rxs {
                // Every caller is TOLD. Pooling widens a failure from one
                // request to all of them, so silence here would become a
                // timeout for each and a stall for the worker.
                assert!(
                    rx.await.expect("caller must be woken").is_err(),
                    "a dead backend must fail its callers, not hang them"
                );
            }
            assert!(conn.is_dead(), "the connection must be marked dead");
        });
    }
}
