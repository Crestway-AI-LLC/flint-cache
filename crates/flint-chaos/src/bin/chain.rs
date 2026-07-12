//! Chain-traversal chaos: build a linked list of N elements
//! (key0000001 -> key0000002 -> ... -> key{N} -> "END"), then walk it
//! pointer-by-pointer while master/replica instances are killed randomly.
//!
//! Why this is a strong oracle: traversal is a chain of DEPENDENT reads.
//! The chain is deterministic, so at every hop the pointer must equal the
//! expected next key. Any lost link (nil), misdirected pointer (cross-key /
//! corruption), cycle, or premature/late END surfaces at the exact element
//! where it happens. Because the chain is fully built and replicated before
//! any kill, and master kills fire only in the healthy regime (replica live
//! and caught up), a correct system must ALWAYS complete the walk with
//! exactly N hops — zero tolerance.
//!
//! Reads must also survive failover: a GET that fails because its server was
//! just killed is retried against the (possibly promoted) new master, at the
//! same position — the walk never advances on an unconfirmed read.
//!
//! Usage: chain [--elements 1000000] [--kills 12] [--seed 42]

use std::time::Duration;

use flint_chaos::cluster::{Client, Cluster, arg, dbsize};
use flint_resp::Value;
use rand::{Rng, SeedableRng, rngs::SmallRng};

fn key(i: u64) -> String {
    format!("key{i:07}")
}

const END: &[u8] = b"END";

fn main() {
    let n: u64 = arg("--elements", 1_000_000);
    let kills: u32 = arg("--kills", 12);
    let controller_driven = arg("--driver", "harness".to_string()) == "controller";
    let mut rng = SmallRng::seed_from_u64(arg("--seed", 42));
    println!(
        "chaos-chain: {n} elements, {kills} kills, driver={}",
        if controller_driven {
            "controller"
        } else {
            "harness"
        }
    );

    let mut cluster = if controller_driven {
        // A real flint-controller makes every failover decision; the harness
        // only kills nodes and re-attaches replacements on fixed ports.
        Cluster::bootstrap_controlled(150, 3, 3_000)
    } else {
        Cluster::bootstrap()
    };

    // --- Build phase: pipelined SETs, key{i} -> key{i+1}, key{N} -> END.
    let build_start = std::time::Instant::now();
    let mut w = Client::connect(cluster.master()).expect("build connect");
    let mut batch: Vec<Vec<Vec<u8>>> = Vec::with_capacity(1000);
    for i in 1..=n {
        let next = if i == n {
            END.to_vec()
        } else {
            key(i + 1).into_bytes()
        };
        batch.push(vec![b"SET".to_vec(), key(i).into_bytes(), next]);
        if batch.len() == 1000 {
            w.pipeline(&batch).expect("pipeline build");
            batch.clear();
        }
    }
    if !batch.is_empty() {
        w.pipeline(&batch).expect("pipeline build tail");
    }
    println!(
        "built {n} links in {:.1}s; waiting for replica to catch up",
        build_start.elapsed().as_secs_f64()
    );
    assert!(
        cluster.wait_healthy(Duration::from_secs(60)),
        "replica must fully catch up before traversal"
    );
    eprintln!(
        "  [integrity] post-build: master={:?} replica={:?}",
        dbsize(cluster.master()),
        dbsize(cluster.replica())
    );

    // --- Traverse phase: follow pointers, killing at random intervals.
    // Schedule kills at distinct random hop positions across the walk.
    let mut kill_at: Vec<u64> = (0..kills).map(|_| rng.random_range(1..n)).collect();
    kill_at.sort_unstable();
    kill_at.dedup();
    let mut kill_idx = 0;

    let mut c = Client::connect(cluster.master()).expect("traverse connect");
    let mut hops: u64 = 0;
    let mut expect = 1u64; // we should be standing on key{expect}
    let mut cur = key(1);

    loop {
        // Fire a scheduled kill when we reach its hop position.
        if kill_idx < kill_at.len() && hops == kill_at[kill_idx] {
            let kill_master = rng.random_bool(0.5);
            if kill_master && cluster.wait_healthy(Duration::from_secs(8)) {
                if controller_driven {
                    // Kill and WAIT for the controller to promote — the
                    // harness does not decide the failover.
                    cluster.kill_master_await_controller();
                    eprintln!(
                        "  hop {hops}: killed MASTER, CONTROLLER promoted; new master :{}",
                        cluster.master()
                    );
                } else {
                    cluster.kill_master();
                    eprintln!(
                        "  hop {hops}: killed MASTER, promoted; new master :{}",
                        cluster.master()
                    );
                }
            } else if controller_driven {
                cluster.kill_replica_fixed();
                eprintln!("  hop {hops}: killed REPLICA");
            } else {
                cluster.kill_replica();
                eprintln!("  hop {hops}: killed REPLICA");
            }
            // Integrity probe: once the replacement reports healthy, its
            // key count must match the master's exactly (no writes are in
            // flight during traversal). Catches truncation at the attach
            // where it happens instead of thousands of hops later.
            if cluster.wait_healthy(Duration::from_secs(30)) {
                let (m, r) = (dbsize(cluster.master()), dbsize(cluster.replica()));
                eprintln!("  [integrity] master={m:?} replica={r:?}");
                assert_eq!(
                    m,
                    r,
                    "TRUNCATED SEED at hop {hops}: master :{} has {m:?} keys, replica :{} has {r:?}",
                    cluster.master(),
                    cluster.replica()
                );
            } else {
                eprintln!("  [integrity] replacement never became healthy");
            }
            c = Client::connect(cluster.master()).expect("reconnect after kill");
            kill_idx += 1;
        }

        // Read current element with bounded retry. A GET that errors
        // (server killed under us) OR returns nil during a failover window
        // is retried against the current master for up to ~3s. Only a key
        // that stays nil after the cluster settles is a truly lost link —
        // this separates a transient blip (correct, tolerable) from real
        // data loss (a bug). The walk never advances on an unconfirmed read.
        let deadline = std::time::Instant::now() + Duration::from_secs(3);
        let mut retries = 0u32;
        let val = loop {
            match c.call(&[b"GET", cur.as_bytes()]) {
                Ok(Value::Bulk(Some(v))) => break v,
                Ok(Value::Bulk(None)) => {
                    // Nil: could be a transient window right at a promotion.
                    // Reconnect to the (possibly new) master and retry.
                    if std::time::Instant::now() >= deadline {
                        panic!(
                            "BROKEN CHAIN at {cur} (hop {hops}): still nil after {retries} retries over 3s on master :{} — a truly lost link",
                            cluster.master()
                        );
                    }
                    retries += 1;
                    std::thread::sleep(Duration::from_millis(50));
                    if let Ok(nc) = Client::connect(cluster.master()) {
                        c = nc;
                    }
                }
                Ok(other) => panic!("unexpected reply at {cur}: {other:?}"),
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(100));
                    if let Ok(nc) = Client::connect(cluster.master()) {
                        c = nc;
                    }
                }
            }
        };
        if retries > 0 {
            eprintln!(
                "  hop {hops}: {cur} resolved after {retries} nil-retries (transient failover window)"
            );
        }

        hops += 1;
        if val == END {
            assert_eq!(
                expect, n,
                "PREMATURE END at {cur}: reached END after {hops} hops, expected {n}"
            );
            break;
        }

        // The pointer must be exactly the deterministic next key.
        let want_next = key(expect + 1);
        let got_next = String::from_utf8_lossy(&val);
        assert_eq!(
            got_next, want_next,
            "MISDIRECTED POINTER at {cur} (hop {hops}): points to {got_next}, expected {want_next}"
        );
        cur = want_next;
        expect += 1;

        assert!(
            hops <= n,
            "CYCLE or overlong chain: {hops} hops exceeds {n} elements"
        );
    }

    assert_eq!(hops, n, "chain length {hops} != {n} elements");
    let (mk, rk) = (cluster.master_kills, cluster.replica_kills);
    println!("---");
    println!("PASS: walked {hops} links end-to-end through {mk} master + {rk} replica kills");
    println!("  every pointer correct, no lost link, no cycle, exactly one END at key{n:07}");
}
