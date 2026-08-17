// SPDX-License-Identifier: Elastic-2.0
//! Concurrent chain traversal THROUGH the proxy, under failover (ADR-0021).
//!
//! `chain.rs` walks one linked list straight at a node and is the storage
//! engine's oracle. This walks MANY independent chains concurrently through
//! the proxy edge, and it exists for a different failure.
//!
//! The async proxy correlates backend replies **by position**: a connection's
//! replies arrive in request order, so the Nth reply belongs to the Nth
//! request. There are no request ids — the same invariant twemproxy and Envoy
//! rely on. Every worker owns its own backend connections, and many client
//! connections share each one. If that correlation is ever wrong — an off-by-
//! one in the pending FIFO, a partially written frame, a timed-out request
//! whose late reply is handed to the next caller, a connection retired while
//! commands are in flight — then one client receives another client's value.
//!
//! Throughput tests cannot see that. Ledger oracles catch corruption of a
//! value but not a correctly-formed value delivered to the wrong asker. A
//! chain walk catches it on the first hop: walker W reads `c{W}:{i}` and the
//! value must be exactly `c{W}:{i+1}`. A value from another chain is a
//! mis-correlated reply, named as such, at the exact hop it happened.
//!
//! Chains are built and fully replicated BEFORE any kill, and each walker
//! reads only its own chain, so a correct system must always complete every
//! walk with exactly N hops. Reads that fail because their node was just
//! killed are retried at the SAME position — the walk never advances on an
//! unconfirmed read — but a read that SUCCEEDS and returns the wrong pointer
//! is a hard failure with no tolerance.
//!
//! Usage: proxy_chain [--chains 16] [--elements 2000] [--kills 6] [--workers 16]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use flint_chaos::cluster::{Client, Cluster, arg};
use flint_resp::Value;

const END: &[u8] = b"END";

fn key(chain: u64, i: u64) -> String {
    format!("c{chain}:{i:06}")
}

/// One GET through the proxy, retrying transport failures and absent values
/// at the SAME position.
///
/// A killed node, a promotion in flight, or a proxy chasing a new master all
/// present as an error or a momentary miss. None of those is a verdict about
/// the data, so they are retried. What is NOT retried is a successful read of
/// the WRONG value — that is the bug this binary exists to find, and retrying
/// it would hide it.
fn get_retry(client: &mut Client, proxy_port: u16, k: &str, budget: Duration) -> Vec<u8> {
    let deadline = Instant::now() + budget;
    let mut attempt = 0u32;
    loop {
        match client.call(&[b"GET", k.as_bytes()]) {
            Ok(Value::Bulk(Some(v))) => return v,
            Ok(_) | Err(_) => {
                assert!(
                    Instant::now() < deadline,
                    "chain read of {k} never succeeded within {budget:?} — \
                     the walk cannot advance on an unconfirmed read"
                );
                std::thread::sleep(Duration::from_millis(20 + 10 * u64::from(attempt.min(10))));
                attempt += 1;
                if let Ok(fresh) = Client::connect(proxy_port) {
                    *client = fresh;
                }
            }
        }
    }
}

/// Walk one chain end to end, asserting every pointer.
fn walk(proxy_port: u16, chain: u64, n: u64, hops: &AtomicU64) {
    let mut client = Client::connect(proxy_port).expect("walker connect");
    let mut i = 1u64;
    let mut seen = 0u64;
    loop {
        let k = key(chain, i);
        let got = get_retry(&mut client, proxy_port, &k, Duration::from_secs(30));
        seen += 1;
        hops.fetch_add(1, Ordering::Relaxed);

        if got == END {
            assert_eq!(
                i, n,
                "chain {chain}: END at element {i}, expected it at {n} — \
                 the chain ended early or late"
            );
            assert_eq!(seen, n, "chain {chain}: walked {seen} hops, expected {n}");
            return;
        }

        let want = key(chain, i + 1);
        if got != want.as_bytes() {
            let got_s = String::from_utf8_lossy(&got).to_string();
            // Name the failure precisely: a pointer into ANOTHER chain is a
            // mis-correlated reply (this client got that client's value),
            // which is a different bug from a corrupted pointer within our
            // own chain.
            let other_chain = got_s.starts_with('c') && !got_s.starts_with(&format!("c{chain}:"));
            assert!(
                !other_chain,
                "chain {chain} hop {i}: CROSS-CHAIN POINTER — read {k} and got \
                 {got_s:?}, which belongs to another walker. A reply was \
                 delivered to the wrong caller: backend correlation is by \
                 position and it is WRONG."
            );
            panic!(
                "chain {chain} hop {i}: WRONG POINTER — read {k}, expected \
                 {want:?}, got {got_s:?}"
            );
        }
        i += 1;
        assert!(
            i <= n,
            "chain {chain}: walked past the tail without seeing END — cycle or \
             misdirected pointer"
        );
    }
}

fn main() {
    let port_base: u16 = arg("--port-base", 6460u16);
    let chains: u64 = arg("--chains", 16);
    let n: u64 = arg("--elements", 2000);
    let kills: u32 = arg("--kills", 6);
    let workers: usize = arg("--workers", 16);

    println!(
        "proxy-chain: {chains} concurrent chains x {n} elements, {kills} kills, \
         proxy workers={workers}, path=client->proxy->node"
    );

    // A real controller promotes; the proxy must chase on its own.
    let mut cluster = Cluster::bootstrap_controlled_at(port_base, 150, 3);
    let proxy_port = cluster.start_proxy_with_workers(Some(workers));

    // --- Build: every chain, through the proxy, pipelined.
    let build_start = Instant::now();
    let mut w = Client::connect(proxy_port).expect("build connect");
    for c in 0..chains {
        let mut batch: Vec<Vec<Vec<u8>>> = Vec::with_capacity(500);
        for i in 1..=n {
            let next = if i == n {
                END.to_vec()
            } else {
                key(c, i + 1).into_bytes()
            };
            batch.push(vec![b"SET".to_vec(), key(c, i).into_bytes(), next]);
            if batch.len() == 500 {
                for attempt in 0..5u32 {
                    if w.pipeline(&batch).is_ok() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(100 << attempt));
                    w = Client::connect(proxy_port).expect("rebuild connect");
                    assert!(attempt < 4, "chain build failed after 5 attempts");
                }
                batch.clear();
            }
        }
        if !batch.is_empty() {
            w.pipeline(&batch).expect("final build batch");
        }
    }
    println!(
        "built {} links in {:.1}s",
        chains * n,
        build_start.elapsed().as_secs_f64()
    );
    assert!(
        cluster.wait_healthy(Duration::from_secs(60)),
        "replica must catch up before traversal — a kill during the walk must \
         never be able to lose a link that was never replicated"
    );

    // --- Traverse: every chain at once, while the master is killed under them.
    let hops = AtomicU64::new(0);
    let mid_walk_kills = AtomicU64::new(0);
    let walk_start = Instant::now();
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..chains)
            .map(|c| {
                let hops = &hops;
                s.spawn(move || walk(proxy_port, c, n, hops))
            })
            .collect();

        // Kills run on this thread while the walkers are mid-chain.
        //
        // Whether a kill actually LANDED during a traversal is counted, not
        // assumed: a run whose walkers finish before the first kill proves
        // nothing about failover and would pass silently. The assertion after
        // the walk is what stops that being mistaken for a green test.
        let total = chains * n;
        for k in 1..=kills {
            // Fire on PROGRESS, not on a timer. A fixed sleep races the walk:
            // on a quiet box the walkers finish first and every kill lands
            // after the fact, which passes while testing nothing. Waiting for
            // the walk to reach fraction k/(kills+1) puts each kill inside a
            // traversal by construction.
            let target = total * u64::from(k) / u64::from(kills + 1);
            let wait_until = Instant::now() + Duration::from_secs(60);
            while hops.load(Ordering::Relaxed) < target && Instant::now() < wait_until {
                std::thread::sleep(Duration::from_millis(5));
            }
            let before = hops.load(Ordering::Relaxed);
            if before >= total {
                println!("  kill {k}/{kills}: skipped — walkers already finished");
                continue;
            }
            if cluster.wait_healthy(Duration::from_secs(10)) {
                cluster.kill_master_await_controller();
                mid_walk_kills.fetch_add(1, Ordering::Relaxed);
                println!(
                    "  kill {k}/{kills}: master killed mid-walk at {before}/{total} hops, \
                     controller promoting"
                );
            } else {
                println!("  kill {k}/{kills}: skipped — pair not healthy yet");
            }
        }

        for h in handles {
            h.join().expect("a walker failed — see the assertion above");
        }
    });

    // THE CONTROL. Walking 320k pointers correctly proves nothing about
    // failover if every kill landed after the walkers had already finished —
    // and a fast walk on a quiet box makes that the DEFAULT outcome, not an
    // edge case. Fail loudly rather than report a green run that tested the
    // happy path only.
    let overlapped = mid_walk_kills.load(Ordering::Relaxed);
    assert!(
        overlapped > 0,
        "no kill landed while the walkers were traversing ({} of {} kills were \
         mid-walk): this run exercised the chain oracle but NOT failover. \
         Raise --elements or --chains so the walk outlives the kill schedule.",
        overlapped,
        kills
    );
    let (mk, rk) = (cluster.master_kills, cluster.replica_kills);
    println!(
        "PASS: {chains} chains x {n} hops through the proxy ({} total) in {:.1}s, \
         {mk} master kills / {rk} replica kills ({overlapped} landed mid-walk), \
         {workers} proxy workers",
        hops.load(Ordering::Relaxed),
        walk_start.elapsed().as_secs_f64()
    );
    println!(
        "      every pointer matched its own chain: no reply was delivered to \
         the wrong caller across {} concurrent client connections sharing this \
         proxy's per-worker backend connections",
        chains
    );
}
