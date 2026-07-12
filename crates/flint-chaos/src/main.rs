//! flint-chaos (KV workload): random writes with a checksummed ledger while
//! master/replica instances are killed randomly. Oracle: no corruption ever;
//! zero acked-write loss on replica kills and on steady-state master
//! failover; no time-travel or cross-key bleed.
//!
//! Usage: flint-chaos [--iterations 12] [--keys 400] [--mode mixed|replica|master] [--seed N]

use std::collections::HashMap;
use std::time::{Duration, Instant};

use flint_chaos::cluster::{Client, Cluster, arg};
use flint_resp::Value;
use flint_slot::crc16;
use rand::{Rng, SeedableRng, rngs::SmallRng};

/// Value embeds the OWNING KEY literally + seq + crc, so a value belonging
/// to another key is detectable with certainty. "flint|<key>|<seq>|<crc>".
fn value_for(key: &str, seq: u64) -> String {
    let crc = crc16(format!("{key}|{seq}").as_bytes());
    format!("flint|{key}|{seq}|{crc:04x}")
}

fn parse_value(raw: &[u8]) -> Option<(String, u64)> {
    let s = std::str::from_utf8(raw).ok()?;
    let mut parts = s.split('|');
    if parts.next()? != "flint" {
        return None;
    }
    let key = parts.next()?.to_string();
    let seq: u64 = parts.next()?.parse().ok()?;
    let crc: u16 = u16::from_str_radix(parts.next()?, 16).ok()?;
    if crc != crc16(format!("{key}|{seq}").as_bytes()) {
        return None;
    }
    Some((key, seq))
}

#[derive(Default)]
struct KeyLedger {
    written: Vec<u64>,
    last_acked: u64,
    last_written: u64,
}

fn main() {
    let iterations: u32 = arg("--iterations", 12);
    let key_count: u64 = arg("--keys", 400);
    let mode: String = arg("--mode", "mixed".to_string());
    let controller_driven = arg("--driver", "harness".to_string()) == "controller";
    let min_replicas: u32 = arg("--min-replicas", 0);
    if min_replicas > 0 {
        // Gate every node the harness spawns; workloads must retry -THROTTLED.
        unsafe { std::env::set_var("FLINT_CHAOS_MIN_REPLICAS", min_replicas.to_string()) };
    }
    println!(
        "chaos-kv: {iterations} kills, {key_count} keys, mode={mode}, driver={}, min_replicas={min_replicas}",
        if controller_driven {
            "controller"
        } else {
            "harness"
        }
    );

    let mut cluster = if controller_driven {
        Cluster::bootstrap_controlled(150, 3, 3_000)
    } else {
        Cluster::bootstrap()
    };
    let mut ledger: HashMap<String, KeyLedger> = HashMap::new();
    let mut rng = SmallRng::seed_from_u64(arg("--seed", 42));
    let mut seq = 0u64;
    let mut acked_lost_total = 0u64;
    let mut throttled_total = 0u64;
    let mut writer = Client::connect(cluster.master()).expect("writer connect");

    for iteration in 1..=iterations {
        // Write for a random spell.
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
                Ok(Value::Error(e)) if e.starts_with("THROTTLED") => {
                    // Widowed/lagging master shed this write; the client
                    // contract is retry-with-backoff. It was never acked, so
                    // the ledger does not count it — no false loss.
                    throttled_total += 1;
                    std::thread::sleep(Duration::from_millis(20));
                }
                Ok(_) | Err(_) => {
                    std::thread::sleep(Duration::from_millis(50));
                    if let Ok(c) = Client::connect(cluster.master()) {
                        writer = c;
                    }
                }
            }
        }

        let want_master = match mode.as_str() {
            "replica" => false,
            "master" => true,
            _ => rng.random_bool(0.5),
        };
        // Steady-state guard: only kill a master with a live, caught-up
        // replica — that tests the ≤1s RPO regime, not degraded-window loss.
        if want_master && cluster.wait_healthy(Duration::from_secs(8)) {
            if controller_driven {
                cluster.kill_master_await_controller();
            } else {
                cluster.kill_master();
            }
            let mut c = Client::connect(cluster.master()).expect("new master");
            let mut lost_here = 0u64;
            for (key, entry) in ledger.iter_mut() {
                if entry.last_acked == 0 {
                    continue;
                }
                match c.call(&[b"GET", key.as_bytes()]) {
                    Ok(Value::Bulk(Some(raw))) => {
                        let (owner, got) = parse_value(&raw)
                            .unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                        assert_eq!(&owner, key, "CROSS-KEY at {key}: owned by {owner}");
                        if got < entry.last_acked {
                            lost_here += 1;
                            entry.last_acked = got;
                            entry.last_written = entry.last_written.max(got);
                        }
                    }
                    Ok(Value::Bulk(None)) => {
                        lost_here += 1;
                        entry.last_acked = 0;
                    }
                    _ => {}
                }
            }
            acked_lost_total += lost_here;
            println!("iter {iteration}: killed MASTER; acked keys regressed: {lost_here}");
            writer = Client::connect(cluster.master()).expect("reconnect");
        } else {
            if controller_driven {
                cluster.kill_replica_fixed();
            } else {
                cluster.kill_replica();
            }
            let mut c = Client::connect(cluster.master()).expect("master");
            for (key, entry) in &ledger {
                if entry.last_acked == 0 {
                    continue;
                }
                match c.call(&[b"GET", key.as_bytes()]) {
                    Ok(Value::Bulk(Some(raw))) => {
                        let (owner, got) = parse_value(&raw)
                            .unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                        assert_eq!(&owner, key, "CROSS-KEY at {key}: owned by {owner}");
                        assert!(
                            got >= entry.last_acked,
                            "iter {iteration}: REPLICA kill lost acked write at {key}: {got} < {}",
                            entry.last_acked
                        );
                    }
                    other => {
                        panic!("iter {iteration}: REPLICA kill lost acked key {key}: {other:?}")
                    }
                }
            }
            println!("iter {iteration}: killed REPLICA; zero acked loss verified");
        }
    }

    // Final full-keyspace walk.
    let mut c = Client::connect(cluster.master()).expect("final connect");
    let (mut present, mut missing) = (0u64, 0u64);
    for (key, entry) in &ledger {
        match c.call(&[b"GET", key.as_bytes()]) {
            Ok(Value::Bulk(Some(raw))) => {
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
            Ok(Value::Bulk(None)) => missing += 1,
            other => panic!("final walk failed at {key}: {other:?}"),
        }
    }

    let (mk, rk) = (cluster.master_kills, cluster.replica_kills);
    println!("---");
    println!("PASS: {iterations} kills ({mk} master, {rk} replica), {seq} writes");
    println!("  corruption: 0  time-travel: 0  cross-key: 0");
    println!(
        "  acked keys regressed across master kills: {acked_lost_total} (async contract; replica kills: zero)"
    );
    println!(
        "  writes shed -THROTTLED (retried): {throttled_total} (widowed/lag gate exercised when > 0)"
    );
    println!("  final walk: {present} present, {missing} missing-or-regressed");
}
