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

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use flint_chaos::cluster::{Attached, Cluster, Target, arg};

/// Wall clock in ms. The RPO bound is a statement about TIME — "acked longer
/// ago than the cap must have replicated" — so the ledger needs a real clock,
/// not the monotonic Instant used for pacing.
use flint_chaos::oracle::parse_value;
use flint_chaos::writer::{self, Edge, Shared, now_ms};
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
    // A "randomized" gate that runs ONE path per topology explores nothing
    // after its first run. run.sh pinned --seed 22 and the attached drill
    // --seed 7, forever (#118 item 5). The seed now defaults to the clock and
    // is PRINTED, so every run is a fresh draw and any failure is replayable
    // by pasting the number back as --seed.
    let seed: u64 = arg(
        "--seed",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(42),
    );
    // The server's own default (repl_hub::DEFAULT_LAG_HARD_MS). Writes acked
    // longer ago than this MUST have replicated, because past it the master
    // sheds instead of acking — so losing one is a breach of the published
    // bound, not the async contract.
    let lag_hard_ms: u64 = arg("--lag-hard-ms", 1_000);
    // The oracle's bound and the server's enforced cap must be the SAME
    // number, or the check compares against a cap nobody applied.
    if lag_hard_ms != 1_000 {
        unsafe { std::env::set_var("FLINT_CHAOS_LAG_HARD_MS", lag_hard_ms.to_string()) };
    }
    // Freeze the replica for this long before each master kill, so the master
    // acks writes the replica has not taken. Without it the unreplicated
    // suffix is empty on loopback and the RPO bound has nothing to measure —
    // every run to date reported a loss depth of 0ms.
    let stall_replica_ms: u64 = arg("--stall-replica-ms", 0);
    // Slack for measurement, not for the engine: the master samples lag
    // slightly before it decides to ack, and our ack clock is taken after the
    // reply arrives. Small next to the cap it qualifies.
    let rpo_margin_ms: u64 = arg("--rpo-margin-ms", 500);
    // docs/slo.md publishes RTO <= 10 s.
    let rto_budget_ms: u64 = arg("--rto-budget-ms", 10_000);
    // Client-path mode: drive the workload through the proxy edge instead of
    // dialling each pair's master (#118 item 3, and what the #99 plan asked
    // for). Needs a tenant credential because a CP-fed proxy is gated.
    let edge_addr: String = arg("--edge", String::new());
    let edge_auth: String = arg("--auth", String::new());
    println!(
        "chaos-kv: {iterations} kills, {key_count} keys, mode={mode}, driver={}, min_replicas={min_replicas}, seed={seed} (replay with --seed {seed})",
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

    // One Target PER PAIR. The 7-host runs reported "16 cross-host kills on
    // 7 hosts" while every kill landed on pair 0 — Attached::open took a
    // single pair index defaulting to 0 and run.sh never passed another, so
    // pair 1 was scenery (#118 item 2). Each pair also gets its own writer
    // and ledger: a hammer on pair 0 while pair 1's master dies would test
    // nothing about pair 1's loss window. --pair N still pins one pair for
    // debugging.
    let mut targets: Vec<Target> = if inventory.is_empty() {
        vec![Target::Local {
            cluster: if controller_driven {
                Cluster::bootstrap_controlled(150, 3, 3_000)
            } else {
                Cluster::bootstrap()
            },
            controller_driven,
        }]
    } else {
        // A real fleet already has a controller; promotion is its job.
        let pin: i64 = arg("--pair", -1);
        if pin >= 0 {
            vec![Target::Attached(Attached::open(&inventory, pin as usize))]
        } else {
            (0..Attached::pair_count(&inventory))
                .map(|i| Target::Attached(Attached::open(&inventory, i)))
                .collect()
        }
    };
    println!("  pairs under test: {}", targets.len());
    // The workload runs on its own thread and does not stop for kills —
    // that is the entire point (#118 item 1): with the writer parked at the
    // moment a master dies, the unreplicated suffix is empty and the RPO
    // bound has nothing to test. See writer.rs for the full argument.
    let writer_seed: u64 = seed;
    let edge = if edge_addr.is_empty() {
        None
    } else {
        let (tenant, token) = edge_auth
            .split_once(':')
            .unwrap_or_else(|| panic!("--edge needs --auth <tenant>:<token>"));
        println!("  client path: proxy edge {edge_addr} as tenant {tenant}");
        Some(Edge {
            addr: edge_addr.clone(),
            tenant: tenant.to_string(),
            token: token.to_string(),
        })
    };
    let pair_count = targets.len();
    let shareds: Vec<std::sync::Arc<Shared>> = (0..pair_count)
        .map(|i| {
            let t = &targets[i];
            // Through the edge the PROXY routes, so each writer pins its keys
            // behind a hash tag landing in its own pair's slots — otherwise
            // writer 1's keys scatter onto pair 0 and its verdict after a
            // pair-1 kill would judge the wrong nodes.
            let tag = if edge.is_some() {
                flint_chaos::cluster::pair_tag(i, pair_count)
            } else {
                String::new()
            };
            std::sync::Arc::new(
                Shared::new(t.endpoints(), t.tls(), key_count).with_edge(edge.clone(), tag),
            )
        })
        .collect();
    let writer_handles: Vec<_> = shareds
        .iter()
        .enumerate()
        .map(|(i, sh)| {
            let sh = sh.clone();
            // Distinct stream per pair, derived from the run seed so a replay
            // reproduces every writer, not just the first.
            std::thread::spawn(move || writer::run(&sh, writer_seed.wrapping_add(i as u64)))
        })
        .collect();

    let mut rng = SmallRng::seed_from_u64(seed);
    let mut acked_lost_total = 0u64;
    let mut rtos: Vec<u64> = Vec::new();
    let mut deepest_loss_ms: u64 = 0;

    for iteration in 1..=iterations {
        // Let the writer run for a spell BETWEEN kills; it keeps writing
        // through what follows.
        std::thread::sleep(Duration::from_millis(rng.random_range(400..900)));

        // Which pair takes this kill. Every pair's writer keeps hammering
        // regardless; only the chosen pair's ledger is judged afterwards.
        let pair_idx = rng.random_range(0..targets.len());
        let cluster = &mut targets[pair_idx];
        let shared = &shareds[pair_idx];

        let want_master = match mode.as_str() {
            "replica" => false,
            "master" => true,
            _ => rng.random_bool(0.5),
        };
        // Kill a master that HAS a live replica but is NOT required to be
        // caught up, with writes in flight AT the kill. That is the regime
        // the RPO number describes; requiring seq_lag==0 first (the old
        // guard) left nothing unreplicated to lose, so the oracle's verdict
        // was a property of this harness.
        if want_master && cluster.wait_replica_live(Duration::from_secs(8)) {
            let harness_promoted = cluster.promotion_is_harness();
            // ARM, RESUME, THEN KILL. The controller arms auto-failover only
            // after observing the pair converged, and under a live hammer it
            // never does — the first run of this loop killed an unarmed pair
            // and died on "controller did not promote within 20s". So: park
            // the writer until convergence has been visible long enough for
            // the controller's confirm*poll window (the hotkey drill's
            // precedent), resume the hammer, give it a beat so the replica is
            // genuinely behind again, and only then kill. Writes are in
            // flight AT the kill, which is the point of this whole change.
            shared.pause.store(true, Ordering::SeqCst);
            assert!(
                cluster.wait_healthy(Duration::from_secs(10)),
                "iter {iteration}: pair never converged while the writer was parked"
            );
            std::thread::sleep(Duration::from_millis(1_500)); // controller confirm*poll
            shared.pause.store(false, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(300)); // hammer re-established

            // Deliberately push the replica behind, so the master is acking
            // writes that have not replicated when it dies. Kept under the
            // 2s liveness window so the pair still looks failover-worthy —
            // this is the bounded-loss regime, not the widowed one.
            if stall_replica_ms > 0 && cluster.stall_replica(true) {
                std::thread::sleep(Duration::from_millis(stall_replica_ms));
                // Unfreeze BEFORE the kill, not after. The survivor has to be
                // running to be promoted and to answer the oracle, but it is
                // still carrying the whole backlog, so the master dies with
                // acked writes the replica has not taken. Resuming afterwards
                // instead meant sending FLINTPROMOTE to a stopped process.
                cluster.stall_replica(false);
            }
            // Arm the writer's RTO clock, then kill. The kill blocks through
            // the promotion, so any timestamp taken after it would include
            // the recovery it is meant to measure. The writer closes the
            // measurement with its FIRST POST-KILL ACK — the vantage of the
            // thing actually trying to write.
            shared.recovered_ms.store(0, Ordering::SeqCst);
            shared.outage_seen.store(false, Ordering::SeqCst);
            shared.max_stall_ms.store(0, Ordering::SeqCst);
            shared.acks_after_kill.store(0, Ordering::SeqCst);
            let kill_ms = now_ms();
            shared.kill_ms.store(kill_ms, Ordering::SeqCst);
            cluster.kill_master_hot();
            // Harness-mode replacement replicas get fresh ports; republish
            // so the writer can find the pair again. (Attached and
            // controlled-local endpoints are fixed; this is a no-op there.)
            shared.set_endpoints(cluster.endpoints());

            let deadline = Instant::now() + Duration::from_millis(rto_budget_ms.max(1) * 2);
            // Two different questions, because the two paths answer different
            // ones. DIRECT: how long from the error to the first success —
            // the client saw the outage. CLIENT PATH: how long was the worst
            // stall — the client saw no error at all, just one slow write,
            // because the proxy chased the promotion underneath. Waiting for
            // a direct-path outage on the client path hangs forever; the
            // first run of edge mode did exactly that.
            let rto = if shared.edge.is_some() {
                // Enough acks after the kill that the failover window is
                // certainly behind us, then take the worst gap.
                loop {
                    if shared.acks_after_kill.load(Ordering::SeqCst) >= 50 {
                        break shared.max_stall_ms.load(Ordering::SeqCst).max(1);
                    }
                    assert!(
                        Instant::now() < deadline,
                        "iter {iteration}: edge served fewer than 50 writes in \
                         {rto_budget_ms}ms x2 after the kill — the proxy never recovered"
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
            } else {
                loop {
                    let r = shared.recovered_ms.load(Ordering::SeqCst);
                    if r != 0 {
                        break r;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "iter {iteration}: writer saw no ack within {rto_budget_ms}ms x2 of the kill"
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
            };
            shared.kill_ms.store(0, Ordering::SeqCst);
            if !harness_promoted {
                rtos.push(rto);
                assert!(
                    rto <= rto_budget_ms,
                    "iter {iteration}: {rto}ms exceeds the published budget {rto_budget_ms}ms \
                     (docs/slo.md) — kill to first post-kill ack"
                );
            }

            // Oracle pass over a SNAPSHOT of the ledger. The writer keeps
            // going, so keys examined here can gain acks afterwards — but a
            // new ack cannot un-lose an old one, and each key's verdict uses
            // only what was recorded at snapshot time.
            struct KeySnap {
                key: String,
                acked_at: Vec<(u64, u64)>,
                last_acked: u64,
            }
            let snapshot: Vec<KeySnap> = {
                let led = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                led.iter()
                    .filter(|(_, e)| e.last_acked != 0)
                    .map(|(k, e)| KeySnap {
                        key: k.clone(),
                        acked_at: e.acked_at.clone(),
                        last_acked: e.last_acked,
                    })
                    .collect()
            };
            let must_have_replicated_by = kill_ms.saturating_sub(lag_hard_ms + rpo_margin_ms);
            // Read back the way the CLIENT would: through the edge when
            // that is the path under test, so a proxy that has not chased
            // the promotion shows up as the data loss it would be for a
            // real client rather than being bypassed.
            let mut c = shared
                .connect()
                .or_else(|| cluster.master_client().ok())
                .expect("oracle connect");
            let mut lost_here = 0u64;
            for KeySnap {
                key,
                acked_at,
                last_acked,
            } in &snapshot
            {
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
                // Acked before the cap's window? Then replication carried it,
                // or the bound is broken. Acked inside the window? Losing it
                // is the async contract — track the depth, not a failure.
                for &(seq, at) in acked_at {
                    // The writer may have re-acked this key AFTER the kill;
                    // those acks belong to the new master and say nothing
                    // about what the old one lost.
                    if at >= kill_ms {
                        continue;
                    }
                    assert!(
                        seq <= got || at > must_have_replicated_by,
                        "iter {iteration}: RPO BREACH at {key}: survivor holds seq {got}, but \
                         seq {seq} was acked {}ms before the kill — beyond the {lag_hard_ms}ms \
                         cap (+{rpo_margin_ms}ms margin) that promises replication carried it",
                        kill_ms.saturating_sub(at)
                    );
                    if seq > got {
                        deepest_loss_ms = deepest_loss_ms.max(kill_ms.saturating_sub(at));
                    }
                }
                if got < *last_acked {
                    lost_here += 1;
                }
                // Retire what this failover lost, ALWAYS. Entries acked
                // before the kill and above what the survivor holds are gone;
                // leaving them in the ledger makes the NEXT kill re-judge
                // them, and their age keeps growing — which is how a run
                // reported "acked 7628ms before the kill" against a 1000ms
                // cap on a fleet that had lost nothing of the sort.
                //
                // The previous version only pruned when the key's last_acked
                // still matched the snapshot, i.e. when the writer had not
                // acked anything new mid-pass. Against a concurrent writer
                // that guard fails constantly, so the pruning silently did
                // not happen. Post-kill acks are kept: they belong to the new
                // master and are not this failover's business.
                let mut led = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(entry) = led.get_mut(key) {
                    entry.acked_at.retain(|&(s, at)| s <= got || at >= kill_ms);
                    entry.last_acked = entry.acked_at.iter().map(|&(s, _)| s).max().unwrap_or(0);
                }
            }
            acked_lost_total += lost_here;
            println!(
                "iter {iteration}: pair {pair_idx}: killed MASTER (writes in flight); {} {rto}ms{}; acked keys \
                 regressed: {lost_here} (all within the {lag_hard_ms}ms cap)",
                if shared.edge.is_some() {
                    "client stall"
                } else {
                    "RTO"
                },
                if harness_promoted {
                    " harness-promoted, not RTO"
                } else {
                    ""
                }
            );
        } else {
            cluster.kill_replica();
            shared.set_endpoints(cluster.endpoints());
            // A replica kill must not disturb the write path at all: every
            // ack recorded BEFORE the kill still stands on the master.
            let snapshot: Vec<(String, u64)> = {
                let led = shared.ledger.lock().unwrap_or_else(|e| e.into_inner());
                led.iter()
                    .filter(|(_, e)| e.last_acked != 0)
                    .map(|(k, e)| (k.clone(), e.last_acked))
                    .collect()
            };
            let mut c = shared
                .connect()
                .or_else(|| cluster.master_client().ok())
                .expect("master");
            for (key, last_acked) in &snapshot {
                match c.call(&[b"GET", key.as_bytes()]) {
                    Ok(Value::Bulk(Some(raw))) => {
                        let (owner, got) = parse_value(&raw)
                            .unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                        assert_eq!(&owner, key, "CROSS-KEY at {key}: owned by {owner}");
                        assert!(
                            got >= *last_acked,
                            "iter {iteration}: REPLICA kill lost acked write at {key}: {got} < {last_acked}"
                        );
                    }
                    other => {
                        panic!("iter {iteration}: REPLICA kill lost acked key {key}: {other:?}")
                    }
                }
            }
            println!("iter {iteration}: pair {pair_idx}: killed REPLICA; zero acked loss verified");
        }
    }

    // Stop every writer before the final walk, or the walk races live SETs
    // and "TIME TRAVEL" would fire on a value newer than the snapshot
    // ceiling.
    for sh in &shareds {
        sh.stop.store(true, Ordering::SeqCst);
    }
    for h in writer_handles {
        h.join().expect("writer thread");
    }
    let seq: u64 = shareds.iter().map(|s| s.seq.load(Ordering::SeqCst)).sum();
    let throttled_total: u64 = shareds
        .iter()
        .map(|s| s.throttled.load(Ordering::SeqCst))
        .sum();

    // Final full-keyspace walk, per pair against that pair's own ledger.
    let (mut present, mut missing) = (0u64, 0u64);
    for (t, sh) in targets.iter_mut().zip(&shareds) {
        let ledger = std::mem::take(&mut *sh.ledger.lock().unwrap_or_else(|e| e.into_inner()));
        let mut c = sh
            .connect()
            .or_else(|| t.master_client().ok())
            .expect("final connect");
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
    }

    let (mk, rk) = targets.iter().fold((0u32, 0u32), |(m, r), t| {
        let (tm, tr) = t.kills();
        (m + tm, r + tr)
    });
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
        let label = if edge.is_some() {
            // Named for what it is. Through the edge the client never saw a
            // failure, so calling this RTO would overstate the outage and
            // understate the product: the proxy absorbed it.
            "client-visible stall (no errors seen; the proxy chased the promotion)"
        } else {
            "RTO kill->writable"
        };
        println!(
            "  {label} over {} promotion(s): p50 {p50}ms, worst {worst}ms (budget {rto_budget_ms}ms, docs/slo.md)",
            sorted.len()
        );
    }
    println!(
        "  deepest acked-write loss: {deepest_loss_ms}ms before the kill (cap {lag_hard_ms}ms + {rpo_margin_ms}ms margin; anything older would have failed the run)"
    );
    // Say plainly when a run proved nothing about the bound. A zero here is
    // not a pass — it means no acked write was ever at risk, so the RPO
    // check had nothing to judge.
    if deepest_loss_ms == 0 {
        println!(
            "  NOTE: loss depth 0 means replication kept up throughout — the RPO bound was \
             not exercised by this run (try --stall-replica-ms)"
        );
    }
    if throttled_total == 0 {
        println!(
            "  NOTE: nothing was shed — the lag cap never bit, so the mechanism the bound \
             RESTS on is unproven by this run (try a smaller --lag-hard-ms)"
        );
    }
    println!("  final walk: {present} present, {missing} missing-or-regressed");
}
