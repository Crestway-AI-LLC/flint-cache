// SPDX-License-Identifier: Elastic-2.0
//! flint-chaos (KV workload): random writes with a checksummed ledger while
//! master/replica instances are killed randomly. Oracle: no corruption ever;
//! zero acked-write loss on replica kills and on steady-state master
//! failover; no time-travel or cross-key bleed.
//!
//! Usage: flint-chaos [--iterations 12] [--keys 400] [--mode mixed|replica|master] [--seed N]
//!
//! `--inventory <path>` attaches to a REAL flintctl-managed fleet instead of
//! spawning a local pair, so the same oracle runs against seats that may be
//! on different machines. Faults go through `flintctl kill-node` /
//! `restart-node`, which know where each seat lives.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use flint_chaos::cluster::{Attached, Cluster, Target, arg};

/// Wall clock in ms. The RPO bound is a statement about TIME — "acked longer
/// ago than the cap must have replicated" — so the ledger needs a real clock,
/// not the monotonic Instant used for pacing.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
use flint_chaos::oracle::{KeyLedger, parse_value, value_for};
use flint_resp::Value;
use rand::{Rng, SeedableRng, rngs::SmallRng};

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
    let inventory: String = arg("--inventory", String::new());
    // The server's own default (repl_hub::DEFAULT_LAG_HARD_MS). Writes acked
    // longer ago than this MUST have replicated, because past it the master
    // sheds instead of acking — so losing one is a breach of the published
    // bound, not the async contract.
    let lag_hard_ms: u64 = arg("--lag-hard-ms", 1_000);
    // Slack for measurement, not for the engine: the master samples lag
    // slightly before it decides to ack, and our ack clock is taken after the
    // reply arrives. Small next to the cap it qualifies.
    let rpo_margin_ms: u64 = arg("--rpo-margin-ms", 500);
    // docs/slo.md publishes RTO <= 10 s.
    let rto_budget_ms: u64 = arg("--rto-budget-ms", 10_000);
    println!(
        "chaos-kv: {iterations} kills, {key_count} keys, mode={mode}, driver={}, min_replicas={min_replicas}",
        if !inventory.is_empty() {
            // An attached fleet HAS a controller; promotion was never ours
            // to make, so saying "harness" here would misreport what was
            // actually under test.
            "fleet-controller"
        } else if controller_driven {
            "controller"
        } else {
            "harness"
        }
    );

    let mut cluster = if inventory.is_empty() {
        Target::Local {
            cluster: if controller_driven {
                Cluster::bootstrap_controlled(150, 3, 3_000)
            } else {
                Cluster::bootstrap()
            },
            controller_driven,
        }
    } else {
        // A real fleet already has a controller; promotion is its job.
        Target::Attached(Attached::open(&inventory, arg("--pair", 0)))
    };
    let mut ledger: HashMap<String, KeyLedger> = HashMap::new();
    let mut rng = SmallRng::seed_from_u64(arg("--seed", 42));
    let mut seq = 0u64;
    let mut acked_lost_total = 0u64;
    let mut throttled_total = 0u64;
    let mut rtos: Vec<u64> = Vec::new();
    let mut deepest_loss_ms: u64 = 0;
    let mut writer = cluster.master_client().expect("writer connect");

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
                Ok(Value::Simple(s)) if s == "OK" => entry.record_ack(seq, now_ms()),
                Ok(Value::Error(e)) if e.starts_with("THROTTLED") => {
                    // Widowed/lagging master shed this write; the client
                    // contract is retry-with-backoff. It was never acked, so
                    // the ledger does not count it — no false loss.
                    throttled_total += 1;
                    std::thread::sleep(Duration::from_millis(20));
                }
                Ok(_) | Err(_) => {
                    std::thread::sleep(Duration::from_millis(50));
                    if let Ok(c) = cluster.master_client() {
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
        // Kill a master that HAS a live replica but is NOT required to be
        // caught up, with writes landing right until the kill. That is the
        // regime the RPO number describes; requiring seq_lag==0 first (the
        // old guard) left nothing unreplicated to lose, so the oracle's
        // verdict was a property of this harness.
        if want_master && cluster.wait_replica_live(Duration::from_secs(8)) {
            // Everything acked before this instant, minus the cap's window,
            // must survive. Taken BEFORE the kill: kill_master() blocks
            // through the promotion, so a timestamp after it would already
            // include the recovery it is meant to measure.
            let kill_ms = now_ms();
            let harness_promoted = cluster.promotion_is_harness();
            cluster.kill_master();
            // RTO: master death -> the pair accepting writes again, which is
            // the published definition. Measured only when something the
            // product ships did the promoting.
            let mut c = cluster.master_client().expect("new master");
            let probe = format!("rto-probe-{iteration}");
            let mut writable_ms = None;
            let deadline = Instant::now() + Duration::from_millis(rto_budget_ms.max(1) * 2);
            while Instant::now() < deadline {
                if let Ok(Value::Simple(ok)) = c.call(&[b"SET", probe.as_bytes(), b"1"])
                    && ok == "OK"
                {
                    writable_ms = Some(now_ms().saturating_sub(kill_ms));
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
                if let Ok(fresh) = cluster.master_client() {
                    c = fresh;
                }
            }
            let rto = writable_ms.unwrap_or_else(|| {
                panic!("iter {iteration}: pair never accepted writes within {rto_budget_ms}ms x2")
            });
            if !harness_promoted {
                rtos.push(rto);
                assert!(
                    rto <= rto_budget_ms,
                    "iter {iteration}: RTO {rto}ms exceeds the published budget {rto_budget_ms}ms \
                     (docs/slo.md) — kill to writable"
                );
            }

            // Acked writes older than the cap's window are guaranteed
            // replicated, so the survivor must hold them. Newer ones may be
            // gone: that IS the async contract, and counting them as failures
            // would make the drill demand something never promised.
            let must_have_replicated_by = kill_ms.saturating_sub(lag_hard_ms + rpo_margin_ms);
            let mut lost_here = 0u64;
            for (key, entry) in ledger.iter_mut() {
                if entry.last_acked == 0 {
                    continue;
                }
                let got = match c.call(&[b"GET", key.as_bytes()]) {
                    Ok(Value::Bulk(Some(raw))) => {
                        let (owner, seq_got) = parse_value(&raw)
                            .unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                        assert_eq!(&owner, key, "CROSS-KEY at {key}: owned by {owner}");
                        seq_got
                    }
                    Ok(Value::Bulk(None)) => 0,
                    _ => continue,
                };
                let breaches = entry.breaches(got, must_have_replicated_by);
                assert!(
                    breaches.is_empty(),
                    "iter {iteration}: RPO BREACH at {key}: survivor holds seq {got}, but \
                     seq {} was acked {}ms before the kill — beyond the {lag_hard_ms}ms cap \
                     (+{rpo_margin_ms}ms margin) that promises replication carried it",
                    breaches[0].0,
                    kill_ms.saturating_sub(breaches[0].1)
                );
                if let Some(newest) = entry.newest_lost_ack_ms(got) {
                    deepest_loss_ms = deepest_loss_ms.max(kill_ms.saturating_sub(newest));
                }
                if got < entry.last_acked {
                    lost_here += 1;
                    entry.last_acked = got;
                    entry.last_written = entry.last_written.max(got);
                    entry.acked_at.retain(|&(seq, _)| seq <= got);
                }
            }
            acked_lost_total += lost_here;
            println!(
                "iter {iteration}: killed MASTER (writer live); RTO {rto}ms{}; acked keys \
                 regressed: {lost_here} (all within the {lag_hard_ms}ms cap)",
                if harness_promoted {
                    " harness-promoted, not RTO"
                } else {
                    ""
                }
            );
            writer = cluster.master_client().expect("reconnect");
        } else {
            cluster.kill_replica();
            let mut c = cluster.master_client().expect("master");
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
    let mut c = cluster.master_client().expect("final connect");
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

    let (mk, rk) = cluster.kills();
    println!("---");
    println!("PASS: {iterations} kills ({mk} master, {rk} replica), {seq} writes");
    println!("  corruption: 0  time-travel: 0  cross-key: 0");
    println!(
        "  acked keys regressed across master kills: {acked_lost_total} (async contract; replica kills: zero)"
    );
    println!(
        "  writes shed -THROTTLED (retried): {throttled_total} (widowed/lag gate exercised when > 0)"
    );
    if rtos.is_empty() {
        println!("  RTO: not measured (promotions were harness-issued, not the product's)");
    } else {
        let mut sorted = rtos.clone();
        sorted.sort_unstable();
        let p50 = sorted[sorted.len() / 2];
        let worst = *sorted.last().expect("non-empty");
        println!(
            "  RTO kill->writable over {} promotion(s): p50 {p50}ms, worst {worst}ms (budget {rto_budget_ms}ms, docs/slo.md)",
            sorted.len()
        );
    }
    println!(
        "  deepest acked-write loss: {deepest_loss_ms}ms before the kill (cap {lag_hard_ms}ms + {rpo_margin_ms}ms margin; anything older would have failed the run)"
    );
    println!("  final walk: {present} present, {missing} missing-or-regressed");
}
