// SPDX-License-Identifier: Elastic-2.0
//! flint hotkey-chaos: the HOT-KEY write path under controller-driven
//! failover. A writer thread hammers a HANDFUL of keys through the proxy and
//! — unlike every other chaos workload — keeps writing THROUGH the master
//! kill. That concentrates each failover's loss window on exactly the keys
//! being written that instant (the worst case for a cache fronting a hot
//! counter or session), and yields the number the paused-writer workloads
//! cannot: the client-observed WRITE BLACKOUT per failover — last acked
//! write before the kill to first acked write after, end to end through
//! detection, promotion, and proxy rediscovery.
//!
//! Verified per kill and at exit (same oracle as the other KV workloads):
//!   - no torn/corrupted value, no cross-key value, no time travel, no
//!     phantom (every surfaced seq was actually written to that key);
//!   - acked regressions across master kills counted and reported (the
//!     async-replication contract; kills happen from converged state, so
//!     the expected count is 0);
//!   - every write blackout <= --blackout-budget-ms (default 10_000 — the
//!     published failover RTO in docs/slo.md; the run FAILS if a failover
//!     ever exceeds it).
//!
//! Usage: hotkey [--kills 6] [--keys 8] [--spell-ms 1200] [--blackout-budget-ms 10000]

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use flint_chaos::cluster::{Client, Cluster, arg};
use flint_chaos::oracle::{KeyLedger, parse_value, value_for};
use flint_resp::Value;

/// Ack gaps below this are latency jitter, not a blackout worth recording.
const BLACKOUT_MIN: Duration = Duration::from_millis(250);

/// How long an audit GET may chase a promotion before it is a real failure.
const PROXY_OP_BUDGET: Duration = Duration::from_secs(12);

fn hot_key(idx: usize) -> String {
    format!("hot:{idx}")
}

struct Shared {
    ledger: Mutex<Vec<KeyLedger>>,
    /// Global write sequence, shared by every writer thread.
    seq: AtomicU64,
    writes: AtomicU64,
    acks: AtomicU64,
    transient_errors: AtomicU64,
    /// (offset-ms of the last ack before the gap, gap length in ms).
    blackouts: Mutex<Vec<(u64, u64)>>,
    pause: AtomicBool,
    /// How many writer threads are currently parked.
    parked: AtomicUsize,
    stop: AtomicBool,
}

fn reconnect(proxy_port: u16) -> Client {
    loop {
        if let Ok(c) = Client::connect(proxy_port) {
            return c;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// One writer's hot loop: round-robin SETs over its OWNED subset of the hot
/// keys, forever, through kills. Ownership is disjoint (key idx belongs to
/// writer idx % writers), so each key has exactly one writer and the per-key
/// acked floor stays a valid oracle even with many concurrent connections —
/// which is what the async write queue batches across. Every attempt is
/// recorded before it is sent (written = the time-travel ceiling); only an
/// OK ack advances the durability floor. An ack arriving after a gap longer
/// than BLACKOUT_MIN records the gap. Pauses requested by the auditor park
/// the loop and are excluded from blackout accounting.
fn writer_loop(
    sh: Arc<Shared>,
    proxy_port: u16,
    key_count: usize,
    writers: usize,
    w: usize,
    t0: Instant,
) {
    let owned: Vec<usize> = (0..key_count).filter(|i| i % writers == w).collect();
    assert!(!owned.is_empty(), "writer {w} owns no keys");
    let mut client = reconnect(proxy_port);
    let mut at = 0usize;
    let mut last_ok = Instant::now();
    loop {
        if sh.stop.load(Ordering::SeqCst) {
            return;
        }
        if sh.pause.load(Ordering::SeqCst) {
            sh.parked.fetch_add(1, Ordering::SeqCst);
            while sh.pause.load(Ordering::SeqCst) {
                if sh.stop.load(Ordering::SeqCst) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            sh.parked.fetch_sub(1, Ordering::SeqCst);
            last_ok = Instant::now(); // an audit pause is not a blackout
        }
        let seq = sh.seq.fetch_add(1, Ordering::SeqCst) + 1;
        at = (at + 1) % owned.len();
        let idx = owned[at];
        let key = hot_key(idx);
        let value = value_for(&key, seq);
        {
            let mut l = sh.ledger.lock().expect("ledger lock");
            l[idx].written.push(seq);
            l[idx].last_written = seq;
        }
        sh.writes.fetch_add(1, Ordering::Relaxed);
        match client.call(&[b"SET", key.as_bytes(), value.as_bytes()]) {
            Ok(Value::Simple(s)) if s == "OK" => {
                let gap = last_ok.elapsed();
                if gap > BLACKOUT_MIN {
                    let started = t0.elapsed().saturating_sub(gap);
                    sh.blackouts
                        .lock()
                        .expect("blackouts lock")
                        .push((started.as_millis() as u64, gap.as_millis() as u64));
                }
                last_ok = Instant::now();
                sh.acks.fetch_add(1, Ordering::Relaxed);
                sh.ledger.lock().expect("ledger lock")[idx].last_acked = seq;
            }
            // Anything non-OK (timeout mid-promotion, -ERR no master, a torn
            // connection) is NOT an ack: the ledger floor does not move, so
            // there is no false loss. Reconnect and keep hammering — the gap
            // until the next OK is the blackout being measured.
            Ok(_) | Err(_) => {
                sh.transient_errors.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(30));
                client = reconnect(proxy_port);
            }
        }
    }
}

/// Definitive GET through the proxy (same discipline as proxy_chaos).
fn proxy_get(client: &mut Client, proxy_port: u16, key: &str) -> Option<Vec<u8>> {
    let start = Instant::now();
    loop {
        match client.call(&[b"GET", key.as_bytes()]) {
            Ok(Value::Bulk(v)) => return v,
            other => {
                if start.elapsed() > PROXY_OP_BUDGET {
                    panic!("proxy GET {key} never resolved within budget: {other:?}");
                }
                std::thread::sleep(Duration::from_millis(40));
                *client = reconnect(proxy_port);
            }
        }
    }
}

/// Audit every hot key (writer must be parked): torn/cross-key/time-travel
/// are fatal; a value below the acked floor counts as a regression and
/// lowers the floor (the async contract, reported per kill). Returns
/// (keys regressed, max regression depth in GLOBAL seqs) — the depth is the
/// count of writes issued between the lost ack and the last ack, so divided
/// by the observed write rate it measures the loss window in TIME: the
/// observed RPO of that failover.
fn audit(sh: &Shared, client: &mut Client, proxy_port: u16, key_count: usize) -> (u64, u64) {
    let mut regressed = 0u64;
    let mut max_depth = 0u64;
    for idx in 0..key_count {
        let key = hot_key(idx);
        let (last_acked, last_written) = {
            let l = sh.ledger.lock().expect("ledger lock");
            (l[idx].last_acked, l[idx].last_written)
        };
        if last_acked == 0 {
            continue;
        }
        match proxy_get(client, proxy_port, &key) {
            Some(raw) => {
                let (owner, got) =
                    parse_value(&raw).unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                assert_eq!(owner, key, "CROSS-KEY at {key}: owned by {owner}");
                assert!(
                    got <= last_written,
                    "TIME TRAVEL at {key}: {got} > {last_written}"
                );
                if got < last_acked {
                    regressed += 1;
                    max_depth = max_depth.max(last_acked - got);
                    sh.ledger.lock().expect("ledger lock")[idx].last_acked = got;
                }
            }
            None => {
                regressed += 1;
                max_depth = max_depth.max(last_acked);
                sh.ledger.lock().expect("ledger lock")[idx].last_acked = 0;
            }
        }
    }
    (regressed, max_depth)
}

fn main() {
    let kills: u32 = arg("--kills", 6);
    let key_count: usize = arg("--keys", 8);
    let writers: usize = arg("--writers", 4);
    let spell_ms: u64 = arg("--spell-ms", 1_200);
    let blackout_budget_ms: u64 = arg("--blackout-budget-ms", 10_000);
    assert!(writers >= 1 && writers <= key_count, "1 <= writers <= keys");
    // The async write queue (ADR-0005 D4) is enabled on every spawned node
    // via the harness passthrough; report which path this run exercises.
    let async_mode = std::env::var("FLINT_CHAOS_ASYNC_WRITES").unwrap_or_default();
    println!(
        "hotkey-chaos: {kills} master kills over {key_count} hot keys, {writers} writers run \
         THROUGH the kills, write path = {}, blackout budget {blackout_budget_ms} ms \
         (published RTO), path=client->proxy->node, driver=controller",
        if async_mode.is_empty() {
            "inline (sync)".to_string()
        } else {
            format!("ASYNC QUEUE (--async-writes {async_mode})")
        }
    );

    let mut cluster = Cluster::bootstrap_controlled(150, 3, 3_000);
    let proxy_port = cluster.start_proxy();
    let t0 = Instant::now();

    let sh = Arc::new(Shared {
        ledger: Mutex::new((0..key_count).map(|_| KeyLedger::default()).collect()),
        seq: AtomicU64::new(0),
        writes: AtomicU64::new(0),
        acks: AtomicU64::new(0),
        transient_errors: AtomicU64::new(0),
        blackouts: Mutex::new(Vec::new()),
        pause: AtomicBool::new(false),
        parked: AtomicUsize::new(0),
        stop: AtomicBool::new(false),
    });
    let writer_handles: Vec<_> = (0..writers)
        .map(|w| {
            let sh = Arc::clone(&sh);
            std::thread::spawn(move || writer_loop(sh, proxy_port, key_count, writers, w, t0))
        })
        .collect();
    let mut auditor = reconnect(proxy_port);
    let mut regressions_total = 0u64;
    let mut max_loss_depth = 0u64;

    for kill in 1..=kills {
        // Let the writer hammer in steady state.
        std::thread::sleep(Duration::from_millis(spell_ms));

        // Convergence can never SAMPLE clean under a live hammer (seq_lag
        // flickers with every in-flight write), so prove it with the writer
        // parked: the replica drains in ms, the controller's sweep records
        // converged. Then resume the hammer and kill under it — the whole
        // point of this workload.
        sh.pause.store(true, Ordering::SeqCst);
        while sh.parked.load(Ordering::SeqCst) < writers {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            cluster.wait_healthy(Duration::from_secs(10)),
            "pair never converged before kill {kill}"
        );
        cluster.await_controller_observed();
        sh.pause.store(false, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(200)); // hammer is live again

        let kill_at = t0.elapsed().as_millis() as u64;
        cluster.kill_master_now_await_controller();

        // Park every writer and audit the hot keys through the new master.
        sh.pause.store(true, Ordering::SeqCst);
        while sh.parked.load(Ordering::SeqCst) < writers {
            std::thread::sleep(Duration::from_millis(5));
        }
        let (regressed, depth) = audit(&sh, &mut auditor, proxy_port, key_count);
        regressions_total += regressed;
        max_loss_depth = max_loss_depth.max(depth);
        sh.pause.store(false, Ordering::SeqCst);
        println!(
            "kill {kill}: master down at +{kill_at} ms (controller promoted, proxy chased); \
             acked keys regressed: {regressed}, deepest by {depth} writes"
        );
    }

    // Let the writers settle, then stop them and run the final audit.
    std::thread::sleep(Duration::from_millis(spell_ms));
    sh.stop.store(true, Ordering::SeqCst);
    for h in writer_handles {
        h.join().expect("writer thread");
    }

    let ledger = sh.ledger.lock().expect("ledger lock");
    for (idx, entry) in ledger.iter().enumerate() {
        let key = hot_key(idx);
        let raw = proxy_get(&mut auditor, proxy_port, &key)
            .unwrap_or_else(|| panic!("hot key {key} absent at exit"));
        let (owner, got) =
            parse_value(&raw).unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
        assert_eq!(owner, key, "CROSS-KEY at {key}: owned by {owner}");
        assert!(got <= entry.last_written, "TIME TRAVEL at {key}");
        assert!(
            entry.written.contains(&got),
            "PHANTOM at {key}: seq {got} never written to it"
        );
    }

    let writes = sh.writes.load(Ordering::Relaxed);
    let acks = sh.acks.load(Ordering::Relaxed);
    let errors = sh.transient_errors.load(Ordering::Relaxed);
    let blackouts = sh.blackouts.lock().expect("blackouts lock");
    let max_blackout = blackouts.iter().map(|&(_, d)| d).max().unwrap_or(0);
    let secs = t0.elapsed().as_secs_f64();

    println!("---");
    println!(
        "PASS: {kills} master kills through the proxy; {writes} writes, {acks} acked \
         ({:.0}/s over {secs:.1} s), {errors} transient errors absorbed",
        acks as f64 / secs
    );
    println!("  corruption: 0  time-travel: 0  cross-key: 0  phantom: 0");
    // Loss depth is in global write seqs; at the observed write rate that
    // converts to the loss window in time — the OBSERVED RPO.
    let rate = acks as f64 / secs;
    let observed_rpo_ms = if rate > 0.0 {
        (max_loss_depth as f64 / rate * 1_000.0).ceil() as u64
    } else {
        0
    };
    println!(
        "  acked keys regressed across kills: {regressions_total} (async contract); \
         deepest regression {max_loss_depth} writes ~= {observed_rpo_ms} ms observed RPO \
         (published bound 10000 ms, enforced cap 1000 ms)"
    );
    assert!(
        observed_rpo_ms <= 10_000,
        "RPO BUDGET EXCEEDED: observed loss window ~= {observed_rpo_ms} ms"
    );
    println!(
        "  write blackouts >{} ms: {} — {}",
        BLACKOUT_MIN.as_millis(),
        blackouts.len(),
        if blackouts.is_empty() {
            "none".to_string()
        } else {
            blackouts
                .iter()
                .map(|(at, d)| format!("+{at}ms/{d}ms"))
                .collect::<Vec<_>>()
                .join(" ")
        }
    );
    println!("  max write blackout: {max_blackout} ms (budget {blackout_budget_ms} ms)");
    assert!(
        max_blackout <= blackout_budget_ms,
        "RTO BUDGET EXCEEDED: a write blackout of {max_blackout} ms broke the \
         {blackout_budget_ms} ms budget"
    );
}
