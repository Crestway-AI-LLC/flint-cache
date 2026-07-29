// SPDX-License-Identifier: Elastic-2.0
//! flint proxy-chaos: the SAME ledger oracle as `flint-chaos`, but the whole
//! workload flows through the proxy — client→proxy→node — while a real
//! controller drives failover. This is the production path the direct-to-node
//! chaos test deliberately bypasses (there the client chases masters itself).
//!
//! What only this variant exercises:
//!   - the proxy chasing masters under REPEATED controller-driven failover
//!     (its FLINTINFO rediscovery vs the controller's detect→promote latency);
//!   - the proxy's retry budget masking the promotion window end-to-end, so
//!     the client sees latency, not errors, across a kill;
//!   - the ledger oracle certifying the FULL path — no acked write lost or
//!     corrupted client→proxy→node, not merely client→node.
//!
//! The pair runs on FIXED ports (controlled mode); roles float between them
//! and the proxy is configured with both, so the client only ever knows the
//! proxy port. Master kills wait for the controller to promote; the proxy
//! then rediscovers on its own.
//!
//! Usage: proxy_chaos [--iterations 12] [--keys 400] [--mode mixed|replica|master] [--seed N]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use flint_chaos::cluster::{Client, Cluster, arg};
use flint_chaos::oracle::{KeyLedger, parse_value, value_for};
use flint_resp::Value;
use rand::{Rng, SeedableRng, rngs::SmallRng};

/// How long to keep retrying one operation through the proxy before treating
/// it as a real failure. A failover chase (dead-backend detect + rediscover +
/// retry) can exceed a single client read timeout, so transient errors here
/// are the proxy absorbing a promotion, not loss — generous but bounded.
const PROXY_OP_BUDGET: Duration = Duration::from_secs(12);

/// Reconnect to the proxy (its client port is stable across node failovers).
fn reconnect(proxy_port: u16) -> Client {
    loop {
        if let Ok(c) = Client::connect(proxy_port) {
            return c;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A definitive GET through the proxy: retries transient errors/timeouts
/// (the proxy chasing a promotion) within the budget, reconnecting as needed.
/// Returns Some(bytes) for a value, None for a genuine absent key (the proxy
/// returns Bulk(None), distinct from a no-master error). Panics only if no
/// definitive answer arrives within the budget — that IS a real failure.
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

/// Fan-out commands (DBSIZE, SCAN, FLUSHALL) address every pair's master
/// directly rather than routing by slot, so they are the one path a stale
/// master table breaks *permanently*: keyed traffic re-resolves on the next
/// MOVED, but a fan-out has nothing to correct it. That is exactly how the
/// shipped bug behaved — keyed reads healed within seconds while SCAN and
/// DBSIZE stayed pointed at a node that had been dead since the promotion.
///
/// Must be called BEFORE any keyed read following the promotion. The keyed
/// path repairs the shared master table as a side effect of re-resolving,
/// so a fan-out checked afterwards passes even against the buggy proxy —
/// verified by reverting the fix. Order is what makes this a regression.
///
/// `floor` is the minimum DBSIZE that would still be honest: 0 before the
/// keyed sweep (any definitive answer proves rediscovery), and the number of
/// keys just read back afterwards (a fan-out that skipped the promoted pair
/// would return a short count rather than an error).
fn assert_fanout_healthy(client: &mut Client, proxy_port: u16, floor: u64, iteration: u32) {
    let start = Instant::now();
    let mut last;
    loop {
        let dbsize = match client.call(&[b"DBSIZE"]) {
            Ok(Value::Integer(n)) if n as u64 >= floor => n as u64,
            other => {
                last = format!("DBSIZE -> {other:?}");
                if start.elapsed() > PROXY_OP_BUDGET {
                    panic!(
                        "iter {iteration}: fan-out still broken {:?} after promotion ({last}); \
                         keyed traffic recovered, so this is a stale master table, not a dead cluster",
                        start.elapsed()
                    );
                }
                std::thread::sleep(Duration::from_millis(40));
                *client = reconnect(proxy_port);
                continue;
            }
        };
        // SCAN opens a cursor over the same fan-out set; a stale entry there
        // errors rather than under-reporting, so a clean first page is the
        // signal. Full enumeration is the scan drill's job, not this one.
        match client.call(&[b"SCAN", b"0", b"COUNT", b"10"]) {
            Ok(Value::Array(Some(items))) if items.len() == 2 => {
                println!(
                    "  iter {iteration}: fan-out healthy after promotion (DBSIZE {dbsize} >= {floor}, SCAN cursor opened)"
                );
                return;
            }
            other => {
                last = format!("SCAN -> {other:?}");
                if start.elapsed() > PROXY_OP_BUDGET {
                    panic!("iter {iteration}: fan-out still broken after promotion ({last})");
                }
                std::thread::sleep(Duration::from_millis(40));
                *client = reconnect(proxy_port);
            }
        }
    }
}

fn main() {
    let iterations: u32 = arg("--iterations", 12);
    let key_count: u64 = arg("--keys", 400);
    let mode: String = arg("--mode", "mixed".to_string());
    println!(
        "proxy-chaos: {iterations} kills, {key_count} keys, mode={mode}, driver=controller, path=client->proxy->node"
    );

    // Always controller-driven: a real controller promotes, the proxy routes.
    let mut cluster = Cluster::bootstrap_controlled(150, 3, 3_000);
    let proxy_port = cluster.start_proxy();
    let mut ledger: HashMap<String, KeyLedger> = HashMap::new();
    let mut rng = SmallRng::seed_from_u64(arg("--seed", 42));
    let mut seq = 0u64;
    let mut acked_lost_total = 0u64;
    let mut writer = reconnect(proxy_port);

    for iteration in 1..=iterations {
        // Write for a random spell — all through the proxy.
        let spell = Duration::from_millis(rng.random_range(400..900));
        let start = Instant::now();
        while start.elapsed() < spell {
            seq += 1;
            let key = format!("key{}", rng.random_range(0..key_count));
            let value = value_for(&key, seq);
            let entry = ledger.entry(key.clone()).or_default();
            entry.written.push(seq);
            entry.last_written = seq;
            match writer.call(&[b"SET", key.as_bytes(), value.as_bytes()]) {
                Ok(Value::Simple(s)) if s == "OK" => entry.last_acked = seq,
                // Any non-OK through the proxy (a chase mid-promotion, a
                // transient -ERR no master) is NOT an ack — the ledger does
                // not count it, so there is no false loss. Reconnect + retry.
                Ok(_) | Err(_) => {
                    std::thread::sleep(Duration::from_millis(30));
                    writer = reconnect(proxy_port);
                }
            }
        }

        let want_master = match mode.as_str() {
            "replica" => false,
            "master" => true,
            _ => rng.random_bool(0.5),
        };
        if want_master && cluster.wait_healthy(Duration::from_secs(8)) {
            // Controller promotes; the proxy must chase on its own.
            cluster.kill_master_await_controller();
            // FIRST command after the promotion, deliberately: see the note on
            // assert_fanout_healthy. Anything keyed here would mask the bug.
            assert_fanout_healthy(&mut writer, proxy_port, 0, iteration);
            let mut lost_here = 0u64;
            let mut readable = 0u64;
            for (key, entry) in ledger.iter_mut() {
                if entry.last_acked == 0 {
                    continue;
                }
                match proxy_get(&mut writer, proxy_port, key) {
                    Some(raw) => {
                        readable += 1;
                        let (owner, got) = parse_value(&raw)
                            .unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                        assert_eq!(&owner, key, "CROSS-KEY at {key}: owned by {owner}");
                        if got < entry.last_acked {
                            lost_here += 1;
                            entry.last_acked = got;
                            entry.last_written = entry.last_written.max(got);
                        }
                    }
                    None => {
                        lost_here += 1;
                        entry.last_acked = 0;
                    }
                }
            }
            acked_lost_total += lost_here;
            println!(
                "iter {iteration}: killed MASTER (controller-promoted, proxy-chased); acked keys regressed: {lost_here}"
            );
            // Now that the keyed sweep has established a floor, re-check the
            // count itself: the pre-sweep call proved the fan-out rediscovers,
            // this one proves it did not quietly drop a pair from the sum.
            assert_fanout_healthy(&mut writer, proxy_port, readable, iteration);
        } else {
            cluster.kill_replica_fixed();
            for (key, entry) in &ledger {
                if entry.last_acked == 0 {
                    continue;
                }
                match proxy_get(&mut writer, proxy_port, key) {
                    Some(raw) => {
                        let (owner, got) = parse_value(&raw)
                            .unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                        assert_eq!(&owner, key, "CROSS-KEY at {key}: owned by {owner}");
                        assert!(
                            got >= entry.last_acked,
                            "iter {iteration}: REPLICA kill lost acked write at {key}: {got} < {}",
                            entry.last_acked
                        );
                    }
                    None => {
                        panic!(
                            "iter {iteration}: REPLICA kill lost acked key {key} (absent via proxy)"
                        )
                    }
                }
            }
            println!("iter {iteration}: killed REPLICA; zero acked loss verified through proxy");
        }
    }

    // Final full-keyspace walk, through the proxy.
    let (mut present, mut missing) = (0u64, 0u64);
    for (key, entry) in &ledger {
        match proxy_get(&mut writer, proxy_port, key) {
            Some(raw) => {
                let (owner, got) =
                    parse_value(&raw).unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                assert_eq!(&owner, key, "CROSS-KEY at {key}: owned by {owner}");
                assert!(
                    entry.written.contains(&got),
                    "PHANTOM at {key}: seq {got} never written"
                );
                assert!(got <= entry.last_written, "TIME TRAVEL at {key}");
                present += 1;
            }
            None => missing += 1,
        }
    }

    let (mk, rk) = (cluster.master_kills, cluster.replica_kills);
    println!("---");
    println!(
        "PASS: {iterations} kills ({mk} master, {rk} replica) through the proxy, {seq} writes"
    );
    println!("  corruption: 0  time-travel: 0  cross-key: 0  (full path client->proxy->node)");
    println!(
        "  acked keys regressed across master kills: {acked_lost_total} (async contract; replica kills: zero)"
    );
    println!("  final walk: {present} present, {missing} missing-or-regressed");
}
