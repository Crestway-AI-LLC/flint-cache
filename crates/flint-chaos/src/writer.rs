// SPDX-License-Identifier: Elastic-2.0
//! A workload that keeps writing THROUGH a master kill.
//!
//! Why this exists: the KV chaos loop used to write for a spell, stop, wait
//! for `seq_lag == 0`, and only then kill. A converged master has no
//! unreplicated suffix, so no acked write CAN be lost — "acked keys
//! regressed: 0" across every run was a property of the harness, not
//! evidence about the engine. Removing the convergence wait helped, but with
//! the writer still parked at the moment of the kill the suffix stayed empty
//! by timing instead of by construction: the run reported `deepest acked-write
//! loss: 0ms`, which is the harness saying it never put anything at risk.
//!
//! So the writer runs on its own thread and does not stop for the kill. That
//! is the only arrangement in which the RPO bound — acked writes older than
//! the lag cap must survive, newer ones may not — has anything to test.
//!
//! It finds the master ITSELF rather than sharing the cluster handle, because
//! `Target` is not `Sync` (the local `Cluster` owns child processes; the
//! attached one used `Cell` counters). Everything the writer needs is a list
//! of candidate endpoints and the TLS config, both cheap to share. For an
//! attached fleet, or a controlled local pair, that list is FIXED — roles
//! float between the same addresses — so the writer rediscovers the master
//! exactly the way a real client does. For the harness-driven local mode,
//! where each replacement replica gets a new port, the main thread republishes
//! the list after a kill; that mode does not report RTO anyway, precisely
//! because the harness is the one doing the promoting.
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flint_resp::Value;

use crate::cluster::Client;
use crate::oracle::{KeyLedger, value_for};

/// Wall clock in ms. The RPO bound is a claim about TIME — "acked longer ago
/// than the cap must have replicated" — so the ledger needs a real clock, not
/// the monotonic one used for pacing.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub struct Shared {
    /// Candidate master addresses. Replaced by the main thread only in the
    /// mode where ports move; otherwise written once.
    pub endpoints: Mutex<Vec<String>>,
    pub tls: Option<Arc<flint_tls::ClientConfig>>,
    pub ledger: Mutex<HashMap<String, KeyLedger>>,
    pub seq: AtomicU64,
    pub throttled: AtomicU64,
    pub stop: AtomicBool,
    /// Park the writer WITHOUT stopping it. Needed because the controller
    /// arms auto-failover only after observing the pair converged, and under
    /// a continuous hammer that observation never happens — the first run of
    /// this writer proved it: the kill landed, no promotion came, and the
    /// run died on "controller did not promote within 20s". Same regime the
    /// hotkey drill already handles the same way: park briefly so the
    /// controller arms, resume, THEN kill — writes are in flight at the kill,
    /// which is the entire point.
    pub pause: AtomicBool,
    /// Set by the main thread immediately before a kill; the writer reports
    /// the first ack after it. 0 means no kill is being timed.
    pub kill_ms: AtomicU64,
    /// Written once per kill by the writer: ms from the kill to the first
    /// write the CLIENT got an ack for. That is the published RTO definition
    /// — the pair accepting writes again — measured by the thing actually
    /// trying to write, which is a more honest vantage than a probe issued
    /// after the recovery is already known to have happened.
    pub recovered_ms: AtomicU64,
    /// The measurement opens only after the writer has SEEN the outage. The
    /// clock is armed just before the SIGKILL lands, and without this gate an
    /// ack from the not-yet-dead master closed the window at "RTO 1ms" — a
    /// number that measured the gap between two lines of harness code. A
    /// kill -9 guarantees the next call on that connection fails, so the
    /// gate always opens.
    pub outage_seen: AtomicBool,
    /// Wall clock of the most recent ack, and the largest gap between two
    /// consecutive acks since the kill was armed.
    ///
    /// Through the proxy edge a client does NOT see an outage: a probe held
    /// across a master kill got 120 replies and 120 of them were +OK. The
    /// proxy chases the promotion and retries underneath, so "time from the
    /// error to the first success" — the direct-path RTO — measures an event
    /// the client never experiences, and waiting for it hangs forever (it
    /// did). What a client actually feels is a STALL: one write takes as long
    /// as the failover instead of failing. So the client path reports the
    /// widest inter-ack gap spanning the kill, which is the same quantity the
    /// SLO cares about seen from where the customer sits.
    pub last_ack_ms: AtomicU64,
    pub max_stall_ms: AtomicU64,
    /// Acks observed strictly after the kill instant — proof that service
    /// continued, and how the client path knows the window has closed.
    pub acks_after_kill: AtomicU64,
    pub key_count: u64,
    /// Client-path mode: dial the PROXY EDGE with a tenant credential rather
    /// than the pair's master directly.
    ///
    /// The #99 plan specified this ("the workload runs from the orchestrator
    /// through the proxy edge") and it was never built, so client-visible
    /// failover — the proxy noticing a promotion and chasing it, the retry
    /// semantics a real client sees — was covered only by the LOCAL
    /// proxy_chaos drill and never once multi-host.
    ///
    /// Direct-to-master stays the default: it isolates the ENGINE's failover
    /// from the proxy's rediscovery, and when both run you can tell which
    /// layer a regression came from.
    pub edge: Option<Edge>,
    /// Every key this writer touches carries this hash tag, pinning it to one
    /// pair even though the proxy does the routing. Empty in direct mode.
    pub tag: String,
}

#[derive(Clone)]
pub struct Edge {
    pub addr: String,
    pub tenant: String,
    pub token: String,
}

impl Shared {
    pub fn new(
        endpoints: Vec<String>,
        tls: Option<Arc<flint_tls::ClientConfig>>,
        key_count: u64,
    ) -> Self {
        Self {
            endpoints: Mutex::new(endpoints),
            tls,
            ledger: Mutex::new(HashMap::new()),
            seq: AtomicU64::new(0),
            throttled: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            pause: AtomicBool::new(false),
            kill_ms: AtomicU64::new(0),
            recovered_ms: AtomicU64::new(0),
            outage_seen: AtomicBool::new(false),
            last_ack_ms: AtomicU64::new(0),
            max_stall_ms: AtomicU64::new(0),
            acks_after_kill: AtomicU64::new(0),
            key_count,
            edge: None,
            tag: String::new(),
        }
    }

    pub fn with_edge(mut self, edge: Option<Edge>, tag: String) -> Self {
        self.edge = edge;
        self.tag = tag;
        self
    }

    fn key(&self, n: u64) -> String {
        if self.tag.is_empty() {
            format!("key{n}")
        } else {
            format!("{{{}}}key{n}", self.tag)
        }
    }

    pub fn set_endpoints(&self, eps: Vec<String>) {
        *self.endpoints.lock().unwrap_or_else(|e| e.into_inner()) = eps;
    }

    /// A connected, authenticated client for this writer's traffic: the edge
    /// when configured, else whichever endpoint answers as master.
    pub fn connect(&self) -> Option<Client> {
        let Some(e) = &self.edge else {
            return self.connect_master();
        };
        // The edge is a FIXED address that outlives every failover — that is
        // the point of it. The proxy speaks plaintext to clients (frontend
        // TLS is a separate concern from the internal mesh), so no config.
        let mut c = Client::connect_addr(&e.addr, &None).ok()?;
        match c.call(&[b"AUTH", e.token.as_bytes()]) {
            Ok(Value::Simple(_)) => Some(c),
            // A CP-fed proxy answers -NOAUTH until a tenant authenticates and
            // may still be loading its snapshot right after a restart; either
            // way the caller retries.
            _ => None,
        }
    }

    /// Dial whichever endpoint currently answers as master.
    fn connect_master(&self) -> Option<Client> {
        let eps = self
            .endpoints
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        for ep in eps {
            let Ok(mut c) = Client::connect_addr(&ep, &self.tls) else {
                continue;
            };
            if let Ok(Value::Bulk(Some(raw))) = c.call(&[b"FLINTINFO"]) {
                let info = String::from_utf8_lossy(&raw);
                if info
                    .lines()
                    .any(|l| l.trim() == "role:master" || l.trim() == "role: master")
                {
                    return Some(c);
                }
            }
        }
        None
    }
}

/// Write continuously until told to stop. Runs on its own thread.
///
/// Every ack is stamped with the wall clock at the moment the reply landed,
/// which is what lets the oracle separate "lost, but acked inside the cap's
/// window" (the async contract) from "lost, though acked long enough ago that
/// replication must have carried it" (a breach of the published bound).
pub fn run(shared: &Shared, seed: u64) {
    use rand::{Rng, SeedableRng, rngs::SmallRng};
    let mut rng = SmallRng::seed_from_u64(seed);
    let mut client = shared.connect();

    while !shared.stop.load(Ordering::SeqCst) {
        if shared.pause.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(5));
            continue;
        }
        let Some(c) = client.as_mut() else {
            std::thread::sleep(Duration::from_millis(10));
            client = shared.connect();
            continue;
        };
        let key = shared.key(rng.random_range(0..shared.key_count));
        let seq = shared.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let value = value_for(&key, seq);

        // Recorded as ATTEMPTED before the call: a write that is acked but
        // whose ack never reaches us must still be a legal value to observe
        // later, or the final walk would call the engine's correct answer a
        // phantom.
        {
            let mut led = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
            let entry = led.entry(key.clone()).or_default();
            entry.written.push(seq);
            entry.last_written = seq;
        }

        match c.call(&[b"SET", key.as_bytes(), value.as_bytes()]) {
            Ok(Value::Simple(s)) if s == "OK" => {
                let at = now_ms();
                {
                    let mut led = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                    led.entry(key).or_default().record_ack(seq, at);
                }
                let kill = shared.kill_ms.load(Ordering::SeqCst);
                // Stall accounting, for the client path. Measured on every
                // ack so the window spanning the kill is caught wherever it
                // falls.
                let prev = shared.last_ack_ms.swap(at, Ordering::SeqCst);
                if kill != 0 && prev != 0 {
                    let gap = at.saturating_sub(prev);
                    shared.max_stall_ms.fetch_max(gap, Ordering::SeqCst);
                    if at > kill {
                        shared.acks_after_kill.fetch_add(1, Ordering::SeqCst);
                    }
                }
                // First ack AFTER the observed outage closes the RTO
                // measurement; acks before the outage are the old master
                // still draining and say nothing about recovery.
                if kill != 0
                    && shared.outage_seen.load(Ordering::SeqCst)
                    && shared.recovered_ms.load(Ordering::SeqCst) == 0
                {
                    shared
                        .recovered_ms
                        .store(at.saturating_sub(kill).max(1), Ordering::SeqCst);
                }
            }
            Ok(Value::Error(e)) if e.starts_with("THROTTLED") => {
                // The lag/quorum gate shedding: never acked, so the ledger
                // does not count it and no false loss is reported. The client
                // contract is retry-with-backoff.
                shared.throttled.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(20));
            }
            _ => {
                // Dead or demoted master: rediscover, exactly as a client
                // would. This is the blackout the RTO measurement is timing.
                if shared.kill_ms.load(Ordering::SeqCst) != 0 {
                    shared.outage_seen.store(true, Ordering::SeqCst);
                }
                client = shared.connect();
                if client.is_none() {
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }
}
