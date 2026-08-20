// SPDX-License-Identifier: Elastic-2.0
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

/// What a direct GET on one member found.
///
/// Three-way on purpose. "Could not ask" and "asked, and it is not there"
/// are the two states this probe exists to tell apart, and an `Option` that
/// folded a transport error into `None` would report a dead node as a
/// missing key — which is the very confusion the panic below is trying to
/// resolve. BUG-0022 is the same mistake one layer down.
enum Probe {
    Present(usize),
    Absent,
    Unreachable(String),
}

impl std::fmt::Display for Probe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Probe::Present(n) => write!(f, "PRESENT ({n} bytes)"),
            Probe::Absent => write!(f, "ABSENT"),
            Probe::Unreachable(e) => write!(f, "UNREACHABLE ({e})"),
        }
    }
}

/// Ask one member directly for one key, bypassing whatever the walk resolved.
fn probe_key(port: u16, k: &str) -> Probe {
    let mut c = match Client::connect(port) {
        Ok(c) => c,
        Err(e) => return Probe::Unreachable(format!("connect: {e}")),
    };
    match c.call(&[b"GET", k.as_bytes()]) {
        Ok(Value::Bulk(Some(v))) => Probe::Present(v.len()),
        Ok(Value::Bulk(None)) => Probe::Absent,
        Ok(other) => Probe::Unreachable(format!("unexpected reply: {other:?}")),
        Err(e) => Probe::Unreachable(format!("call: {e}")),
    }
}

/// One FLINTINFO field, or None if the node cannot be read.
///
/// Added for BUG-0023: the build is a firehose of pipelined SETs, and until
/// `writes_shed_lag` existed the only way to know whether the master had
/// REFUSED any of them was to infer it from a key going missing thousands of
/// hops later. A counter read straight off the master replaces that whole
/// chain of reasoning with one number.
fn info_field(port: u16, name: &str) -> Option<String> {
    let info = match Client::connect(port).and_then(|mut c| c.call(&[b"FLINTINFO"])) {
        Ok(Value::Bulk(Some(v))) => String::from_utf8_lossy(&v).into_owned(),
        _ => return None,
    };
    info.split(['\r', '\n'])
        .find(|l| l.starts_with(name))
        .map(|l| l.trim_start_matches(name).to_string())
}

/// One coherent FLINTINFO snapshot per node, rendered as a single line.
///
/// Deliberately ONE round trip rather than a field-at-a-time helper called
/// six times: this fires while a promotion may still be settling, so six
/// separate reads would be six different instants, and the resulting line
/// could describe a state the node was never in.
fn node_line(port: u16) -> String {
    let info = match Client::connect(port).and_then(|mut c| c.call(&[b"FLINTINFO"])) {
        Ok(Value::Bulk(Some(v))) => String::from_utf8_lossy(&v).into_owned(),
        Ok(other) => return format!(":{port} FLINTINFO unreadable (unexpected reply: {other:?})"),
        Err(e) => return format!(":{port} FLINTINFO unreadable ({e})"),
    };
    let f = |name: &str| -> String {
        info.split(['\r', '\n'])
            .find(|l| l.starts_with(name))
            .map(|l| l.trim_start_matches(name).to_string())
            .unwrap_or_else(|| "<absent>".into())
    };
    format!(
        ":{port} role={} epoch={} latest_seq={} last_applied={} acked_seq={} seq_lag={} live_replicas={} dbsize={}",
        f("role:"),
        f("role_epoch:"),
        f("latest_seq:"),
        f("last_applied:"),
        f("acked_seq:"),
        f("seq_lag:"),
        f("live_replicas:"),
        dbsize(port)
            .map(|n| n.to_string())
            .unwrap_or_else(|| "<unreadable>".into()),
    )
}

/// Prove the lost-link probe works, on a healthy cluster, before any kill.
///
/// The probe it checks fires on maybe 7% of gate runs (BUG-0014's measured
/// rate for its sibling) and has never reproduced standalone. An instrument
/// that rare cannot be left unexercised: a renamed FLINTINFO field or a
/// wrong port would turn the one dump that matters into a page of
/// `<absent>`, and nobody would learn that until the next firing — weeks
/// later, from the log that was supposed to answer the question.
///
/// Negative control FIRST: `key0000000` is never written (the chain starts
/// at 1), so a probe that reports it PRESENT is hallucinating and every
/// later ABSENT is meaningless. Then the positive control: `key0000001` is
/// always written, so a probe that reports it ABSENT cannot see keys at all
/// and would blame the product for its own blindness.
fn verify_probe(master: u16, replica: u16) {
    let never = key(0);
    match probe_key(master, &never) {
        Probe::Absent => {}
        other => panic!(
            "probe self-check: {never} was never written, but the probe reports {other} on :{master} — the probe cannot be trusted to tell ABSENT from PRESENT, so this run's diagnostics are void"
        ),
    }
    let always = key(1);
    match probe_key(master, &always) {
        Probe::Present(_) => {}
        other => panic!(
            "probe self-check: {always} was just built and verified replicated, but the probe reports {other} on :{master} — the probe cannot see keys that exist"
        ),
    }
    for (label, port) in [("master", master), ("replica", replica)] {
        let line = node_line(port);
        if line.contains("<absent>") || line.contains("unreadable") {
            panic!(
                "probe self-check: FLINTINFO on the {label} did not yield the expected fields — a field was renamed and the lost-link dump would print nothing usable: {line}"
            );
        }
    }
    eprintln!(
        "  [probe] self-check ok: ABSENT/PRESENT discriminated, FLINTINFO fields resolve on both members"
    );
}

/// The dump BUG-0023 asks for, printed at the instant of a lost link.
///
/// The discriminating line is the direct GET on each member. It separates
/// "never replicated" (absent everywhere) from "lost by the promoted node"
/// (present on the other member, absent on the master) from "the walk was
/// reading the wrong node" (present on the master the instant we ask it
/// ourselves) — three causes that the bare panic text cannot tell apart,
/// and which no amount of re-running distinguishes because this has never
/// reproduced outside a full gate.
fn diagnose_lost_link(cur: &str, master: u16, replica: u16, hops: u64, retries: u32) {
    eprintln!("  [lost-link] key={cur} hop={hops} retries={retries}");
    eprintln!("  [lost-link] master  {}", node_line(master));
    eprintln!("  [lost-link] replica {}", node_line(replica));
    let on_master = probe_key(master, cur);
    let on_replica = probe_key(replica, cur);
    eprintln!("  [lost-link] direct GET on master  :{master} -> {on_master}");
    eprintln!("  [lost-link] direct GET on replica :{replica} -> {on_replica}");
    let verdict = match (&on_master, &on_replica) {
        (Probe::Present(_), _) => {
            "READ PATH — the key IS on the master when asked directly, so the walk's reads went elsewhere"
        }
        (Probe::Absent, Probe::Present(_)) => {
            "PROMOTION LOSS — replicated to the other member but missing on the promoted master"
        }
        (Probe::Absent, Probe::Absent) => {
            "NEVER LANDED — absent on both members, so it was lost at or before the build/replication"
        }
        _ => "UNDETERMINED — a member could not be asked; see the UNREACHABLE line above",
    };
    eprintln!("  [lost-link] verdict: {verdict}");
}

/// Send one build batch, retrying transport errors instead of dying on them.
///
/// EAGAIN here is the client's 1500ms read timeout firing while a loaded box
/// stalls the server — a retryable condition, not a verdict about the data
/// (a run died on exactly this at "pipeline build: WouldBlock"). The batch is
/// all SETs of deterministic values, so resending it is idempotent. A timeout
/// mid-batch leaves unread replies on the old stream, so every retry starts
/// from a fresh connection rather than a desynced one.
fn pipeline_retry(w: &mut Client, master: u16, batch: &[Vec<Vec<u8>>]) {
    let mut last: Option<std::io::Error> = None;
    for attempt in 0..5u32 {
        match w.pipeline(batch) {
            Ok(()) => return,
            Err(e) => {
                last = Some(e);
                std::thread::sleep(Duration::from_millis(100 << attempt));
                if let Ok(fresh) = Client::connect(master) {
                    *w = fresh;
                }
            }
        }
    }
    panic!("pipeline build failed after 5 attempts: {last:?}");
}

fn main() {
    // Which 8-port block this run owns (see cluster::SPAN). The driving
    // drill declares the same block to fleet_init, so every port bound here
    // is a port some drill is accountable for.
    let port_base: u16 = arg("--port-base", 6460u16);
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
        Cluster::bootstrap_controlled_at(port_base, 150, 3)
    } else {
        Cluster::bootstrap_at(port_base)
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
            pipeline_retry(&mut w, cluster.master(), &batch);
            batch.clear();
        }
    }
    if !batch.is_empty() {
        pipeline_retry(&mut w, cluster.master(), &batch);
    }
    println!(
        "built {n} links in {:.1}s; waiting for replica to catch up",
        build_start.elapsed().as_secs_f64()
    );
    // WAS THE BUILD REFUSED, AND HOW CLOSE DID IT COME? Measured, not inferred.
    //
    // `pipeline` now fails loudly on a refusal, so shed>0 here should be
    // unreachable — printing it anyway is the check on that claim. `lag_ms_max`
    // is the more useful number day to day: it is the PEAK, where FLINTINFO's
    // `lag_ms` is instantaneous and a spike that would shed thousands is
    // invisible to any poll that straddles it. A build that ran at 900ms of a
    // 1000ms cap passed, and should not be reported the same way as one that
    // ran at 40ms.
    {
        let m = cluster.master();
        let shed = info_field(m, "writes_shed_lag:").unwrap_or_else(|| "<unreadable>".into());
        let peak = info_field(m, "lag_ms_max:").unwrap_or_else(|| "<unreadable>".into());
        let soft = info_field(m, "lag_soft_ms:").unwrap_or_else(|| "?".into());
        let hard = info_field(m, "lag_hard_ms:").unwrap_or_else(|| "?".into());
        eprintln!(
            "  [build] writes_shed_lag={shed} lag_ms_max={peak} (caps soft={soft} hard={hard})"
        );
    }
    assert!(
        cluster.wait_healthy(Duration::from_secs(60)),
        "replica must fully catch up before traversal"
    );
    eprintln!(
        "  [integrity] post-build: master={:?} replica={:?}",
        dbsize(cluster.master()),
        dbsize(cluster.replica())
    );
    verify_probe(cluster.master(), cluster.replica());

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
                        diagnose_lost_link(
                            &cur,
                            cluster.master(),
                            cluster.replica(),
                            hops,
                            retries,
                        );
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
