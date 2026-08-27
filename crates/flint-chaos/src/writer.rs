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

/// Wall clock in MICROSECONDS, for the one question milliseconds cannot
/// answer: which master served a write.
///
/// The ledger classifies an ack by comparing its SEND stamp against the
/// instant the old master died. At millisecond resolution those two can land
/// in the same tick, and then the ordering that decides "old master's last
/// words" versus "the new master's write" is simply not recorded — the
/// comparison picks a side anyway and reports the result as a fact. BUG-0014
/// turned on exactly that: a whole durability verdict rested on ONE entry
/// whose send stamp equalled the death stamp.
///
/// Microseconds do not make ties impossible, they make them rare enough to be
/// reportable rather than routine — which is why the tie still has an explicit
/// AMBIGUOUS outcome at the comparison site instead of a silent default.
/// Kept separate from `now_ms` rather than replacing it: the RPO bound is a
/// claim about milliseconds and its arithmetic reads better in them.
pub fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
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
    /// WHEN the worst stall ended (the ack that closed it). The magnitude
    /// alone is unattributable: on the edge path this gap is the number
    /// gated against the RTO budget, and it is the worst gap ANYWHERE in
    /// the post-kill window — a promotion blackout and a RocksDB write
    /// stall on the new master produce the same figure. Only the instant
    /// says which, by placing it against the fleet journal.
    pub max_stall_at_ms: AtomicU64,
    /// The longest a SINGLE request was held before getting any answer at all
    /// — an ack, a `-THROTTLED`, or a dropped connection. Same post-kill
    /// window as `max_stall_ms`, and deliberately its complement.
    ///
    /// `max_stall_ms` is the gap between consecutive ACKS, so it cannot tell
    /// "one write hung for 9 s" apart from "writes were refused promptly for
    /// 9 s": in both cases no ack lands for 9 s. Those are opposite outcomes.
    /// The first is the failure #186 exists to remove; the second is #186
    /// WORKING — the client got an immediate, unambiguous retry signal and
    /// spent none of its own budget waiting.
    ///
    /// So this is the number the write deadline actually bounds, and reading
    /// the two together is what says which happened. Without it a run where
    /// fast-fail worked perfectly would report the same headline figure as the
    /// stall it replaced.
    pub max_hold_ms: AtomicU64,
    /// When the longest hold ended, for the same reason `max_stall_at_ms`
    /// exists: the magnitude alone cannot be placed against the journal.
    pub max_hold_at_ms: AtomicU64,
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
    /// A per-RUN prefix on every key, so one run's keys cannot collide with
    /// another's on a cluster both use.
    ///
    /// OPS-0058. The soak runs a fresh flint-chaos per cycle against the SAME
    /// cluster with no flush, and each process restarts `seq` at zero. With
    /// ~800 writes a cycle over the same key names, successive cycles' seq
    /// ranges overlap almost entirely, so a value left by cycle 3 is
    /// byte-indistinguishable from one cycle 4 could have written: same key,
    /// same owner stamp, a seq in the same range. The final walk then reads a
    /// previous cycle's value at a key this cycle's ledger owns and calls it
    /// `PHANTOM ... never written`.
    ///
    /// That made the strongest assertion in the harness unattributable — it
    /// could not separate its own residue from a genuinely lost write, which
    /// is the one thing it exists to detect. A distinct keyspace per run
    /// removes the residue, so a surviving PHANTOM means what it says.
    ///
    /// IT MUST STAY OUTSIDE THE BRACES. Only the text inside `{}` picks the
    /// slot (`cluster::pair_tag` searches for a tag whose CRC16 lands in the
    /// pair's range), so a nonce placed inside would rehash the key onto an
    /// arbitrary pair and the verdict after a kill would judge the wrong
    /// nodes. `key_nonce_does_not_change_the_hash_tag` holds that.
    pub run_nonce: String,
}

#[derive(Clone)]
pub struct Edge {
    pub addr: String,
    pub tenant: String,
    pub token: String,
    /// The tenant's view of the proxy's edge certificate. None = plaintext.
    ///
    /// Separate from `Shared::tls`, which is the internal MESH config: they
    /// are different trust roots and different server-name rules, and the
    /// reason this field exists is that chaos used to hardcode plaintext here
    /// (see `connect`). Not `Option<Arc<..>>` collapsed into the mesh one on
    /// purpose — a fleet can run a TLS mesh behind a plaintext edge, or the
    /// reverse, and both are postures a customer actually deploys.
    pub tls: Option<Arc<flint_tls::ClientConfig>>,
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
            max_stall_at_ms: AtomicU64::new(0),
            max_hold_ms: AtomicU64::new(0),
            max_hold_at_ms: AtomicU64::new(0),
            acks_after_kill: AtomicU64::new(0),
            key_count,
            edge: None,
            tag: String::new(),
            run_nonce: String::new(),
        }
    }

    pub fn with_edge(mut self, edge: Option<Edge>, tag: String) -> Self {
        self.edge = edge;
        self.tag = tag;
        self
    }

    /// See `run_nonce`. Derived from the run seed by the caller, so `--seed N`
    /// replays the same keyspace as well as the same sequence.
    pub fn with_run_nonce(mut self, nonce: String) -> Self {
        self.run_nonce = nonce;
        self
    }

    fn key(&self, n: u64) -> String {
        if self.tag.is_empty() {
            format!("{}key{n}", self.run_nonce)
        } else {
            // Nonce AFTER the closing brace on purpose — see `run_nonce`.
            format!("{{{}}}{}key{n}", self.tag, self.run_nonce)
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
        // the point of it.
        //
        // THIS USED TO PASS `&None` UNCONDITIONALLY, with a comment saying
        // frontend TLS was "a separate concern from the internal mesh". It is
        // a separate concern, and it is also the concern: every release note
        // claiming the fleet is chaos-tested was describing a plaintext edge,
        // while the posture a customer deploys is a TLS one.
        //
        // AND IT FAILED BY MISATTRIBUTION, which is worse than failing
        // silently. The TCP connect succeeds, the proxy waits for a handshake
        // that never comes, AUTH times out, and `connect` returns None
        // forever — until the post-kill stall detector trips and panics with
        // "the proxy never recovered". So the run does end, pointing at a
        // perfectly healthy proxy. Measured, not assumed: that is verbatim
        // what chaos_edge_tls_drill.sh's negative control produces
        // (ADR-0018 item 9, #20).
        let mut c = Client::connect_edge_addr(&e.addr, &e.tls).ok()?;
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
/// Fold one request's held time into `max_hold_ms`. Post-kill only, so it
/// covers exactly the window `max_stall_ms` covers and the two can be read
/// side by side.
fn record_hold(shared: &Shared, sent: u64, answered: u64) {
    if shared.kill_ms.load(Ordering::SeqCst) == 0 {
        return;
    }
    let held = answered.saturating_sub(sent);
    let mut cur = shared.max_hold_ms.load(Ordering::SeqCst);
    while held > cur {
        match shared
            .max_hold_ms
            .compare_exchange(cur, held, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => {
                shared.max_hold_at_ms.store(answered, Ordering::SeqCst);
                break;
            }
            Err(actual) => cur = actual,
        }
    }
}

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

        // Stamped BEFORE the call. A request already in flight when a master
        // dies can still be acked by it, and the reply is read after the kill
        // instant — so the ack time alone cannot say which master served it.
        // See KeyLedger::acked_at.
        let sent_us = now_us();
        let reply = c.call(&[b"SET", key.as_bytes(), value.as_bytes()]);
        // Recorded before the match, so a reconnect in the error arm is not
        // counted as time the server held the request. Every answer closes a
        // hold, including a refusal — that is the point of the measure.
        // record_hold measures a DURATION in ms; the send stamp is µs.
        record_hold(shared, sent_us / 1_000, now_ms());
        match reply {
            Ok(Value::Simple(s)) if s == "OK" => {
                let at = now_ms();
                {
                    let mut led = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                    led.entry(key).or_default().record_ack(seq, sent_us, at);
                }
                let kill = shared.kill_ms.load(Ordering::SeqCst);
                // Stall accounting, for the client path. Measured on every
                // ack so the window spanning the kill is caught wherever it
                // falls.
                let prev = shared.last_ack_ms.swap(at, Ordering::SeqCst);
                if kill != 0 && prev != 0 {
                    let gap = at.saturating_sub(prev);
                    // CAS rather than fetch_max, so the INSTANT can be stored
                    // by whoever actually raised the maximum. Two writers can
                    // still interleave between the swap and the store, which
                    // for a diagnostic timestamp is acceptable: it can only
                    // name a different ack of the same magnitude, never a
                    // different window.
                    let mut cur = shared.max_stall_ms.load(Ordering::SeqCst);
                    while gap > cur {
                        match shared.max_stall_ms.compare_exchange(
                            cur,
                            gap,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => {
                                shared.max_stall_at_ms.store(at, Ordering::SeqCst);
                                break;
                            }
                            Err(actual) => cur = actual,
                        }
                    }
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

#[cfg(test)]
mod clock_resolution {
    use super::{now_ms, now_us};

    /// A POSITIVE CONTROL on the fix itself. Switching the send stamp to
    /// microseconds only narrows the ambiguous boundary if the platform clock
    /// actually resolves below a millisecond. If it did not — if
    /// `SystemTime::now` advanced in 1ms steps — every sample would be an
    /// exact multiple of 1000, ties would stay exactly as common as before,
    /// and the whole change would be a no-op that reads like a fix.
    ///
    /// One sample landing on a multiple of 1000 is ordinary (1 in 1000). All
    /// 200 doing so is a coarse clock.
    #[test]
    fn the_microsecond_clock_actually_resolves_below_a_millisecond() {
        let samples: Vec<u64> = (0..200).map(|_| now_us()).collect();
        assert!(
            samples.iter().any(|us| us % 1_000 != 0),
            "every one of 200 samples was a whole millisecond: this clock has \
             no sub-ms resolution, so microsecond stamps do not narrow the \
             BUG-0014 boundary at all"
        );
    }

    /// The two clocks must describe the same instant in different units. A
    /// `now_us` built on `as_millis` — or on the wrong epoch — would pass the
    /// resolution test above while making every send stamp 1000x wrong
    /// against the death stamp.
    #[test]
    fn the_two_clocks_agree_once_the_units_are_reconciled() {
        let us = now_us();
        let ms = now_ms();
        let drift = (us / 1_000).abs_diff(ms);
        assert!(
            drift <= 50,
            "now_us()/1000 = {} but now_ms() = {ms} (drift {drift}ms): the two \
             clocks are not reading the same wall clock in compatible units",
            us / 1_000
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::pair_tag;

    fn shared(tag: &str, nonce: &str) -> Shared {
        Shared::new(Vec::new(), None, 8)
            .with_edge(None, tag.to_string())
            .with_run_nonce(nonce.to_string())
    }

    /// THE property the nonce's placement rests on. Only the text inside `{}`
    /// selects a slot, so prefixing outside the braces must leave routing
    /// identical — otherwise a run's keys scatter off their pair and the
    /// verdict after a kill judges the wrong nodes.
    ///
    /// Asserted with `flint_slot::slot_for_key`, the function the proxy
    /// actually routes with, rather than a re-derivation of the hash-tag rule
    /// here: a test that reimplements the rule can be wrong in the same way as
    /// the code and agree with it.
    #[test]
    fn key_nonce_does_not_change_the_hash_tag() {
        for tag in ["p0x0", "p1x3", "p2x17"] {
            let plain = shared(tag, "");
            let noncy = shared(tag, "rdeadbeef-");
            for n in [0u64, 1, 37, 299] {
                let a = plain.key(n);
                let b = noncy.key(n);
                assert_ne!(a, b, "the nonce must actually change the key name");
                assert_eq!(
                    flint_slot::slot_for_key(a.as_bytes()),
                    flint_slot::slot_for_key(b.as_bytes()),
                    "nonce moved {a} -> {b} to a different slot"
                );
                assert_eq!(
                    flint_slot::hash_tag(b.as_bytes()),
                    tag.as_bytes(),
                    "the hash tag of {b} is no longer the pair tag"
                );
            }
        }
    }

    /// The negative control for the test above: prove it can fail. A nonce
    /// placed INSIDE the braces is the mistake being guarded against, and it
    /// must show up as a different slot — otherwise the assertion above passes
    /// for a reason unrelated to placement.
    #[test]
    fn a_nonce_inside_the_braces_would_move_the_slot() {
        let outside = format!("{{{}}}{}key37", "p0x0", "rdeadbeef-");
        let inside = format!("{{{}{}}}key37", "p0x0", "rdeadbeef-");
        assert_ne!(
            flint_slot::slot_for_key(outside.as_bytes()),
            flint_slot::slot_for_key(inside.as_bytes()),
            "if these matched, the placement test could not detect the bug it exists for"
        );
    }

    /// A tagged key must still land in its own pair's slot range, which is the
    /// reason `pair_tag` searches for a tag at all.
    #[test]
    fn key_nonce_keeps_each_pair_in_its_own_slot_range() {
        let pair_count = 3;
        for i in 0..pair_count {
            let tag = pair_tag(i, pair_count);
            let lo = (i * 16384 / pair_count) as u16;
            let hi = ((i + 1) * 16384 / pair_count - 1) as u16;
            let sh = shared(&tag, "r1234abcd-");
            for n in [0u64, 5, 299] {
                let slot = flint_slot::slot_for_key(sh.key(n).as_bytes());
                assert!(
                    slot >= lo && slot <= hi,
                    "pair {i}: key {} landed in slot {slot}, outside {lo}..={hi}",
                    sh.key(n)
                );
            }
        }
    }

    /// OPS-0058 itself: two runs sharing a cluster must not share key names,
    /// or one run's residue is indistinguishable from the other's write.
    #[test]
    fn different_runs_get_disjoint_keyspaces() {
        let a = shared("p0x0", "r1111-");
        let b = shared("p0x0", "r2222-");
        let ka: std::collections::HashSet<String> = (0..300).map(|n| a.key(n)).collect();
        let kb: std::collections::HashSet<String> = (0..300).map(|n| b.key(n)).collect();
        assert_eq!(ka.len(), 300, "keys within a run must stay distinct");
        assert!(
            ka.is_disjoint(&kb),
            "two runs shared {} key name(s)",
            ka.intersection(&kb).count()
        );
    }

    /// `--seed N` must replay the keyspace, not just the write sequence.
    #[test]
    fn the_same_nonce_reproduces_the_same_keyspace() {
        let a = shared("p0x0", "rcafe-");
        let b = shared("p0x0", "rcafe-");
        for n in [0u64, 42, 299] {
            assert_eq!(a.key(n), b.key(n));
        }
    }

    /// Direct mode has no tag, and must still be namespaced — the local chaos
    /// drills reuse one data dir across runs for the same reason the soak
    /// reuses one cluster.
    #[test]
    fn direct_mode_also_carries_the_nonce() {
        let sh = shared("", "rfeed-");
        assert_eq!(sh.key(7), "rfeed-key7");
        assert_eq!(
            shared("", "").key(7),
            "key7",
            "empty nonce stays backward-compatible"
        );
    }
}
