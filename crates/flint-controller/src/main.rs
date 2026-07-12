//! flint-controller (v0): the meta trio's DECISION LOGIC, single-node.
//!
//! Watches one master/replica pair and, on master death, automatically:
//!   1. confirms death with repeated probes (not one flaky poll),
//!   2. verifies the survivor is SAFE to promote — the pair must have been
//!      sequence-converged (seq_lag == 0) within a recency window; a replica
//!      that was badly behind is a degraded-window failure, NOT an
//!      automatic promotion (that path needs the S3/spare seed, deferred),
//!   3. promotes the survivor with a read-then-bumped epoch (FLINTPROMOTE),
//!   4. fences the old master if it returns (FLINTDEMOTE at a higher epoch).
//!
//! What this is NOT yet (both follow-on):
//!   - HA: this is ONE controller. A partitioned/crashed controller stops
//!     making decisions. The trio makes the DECIDER a 3-node Raft quorum so
//!     it survives its own failures and can't be split by a partition.
//!   - Push leases: detection here is poll-based. The trio adds a lease the
//!     master renews, so a partitioned master self-fences on TTL expiry even
//!     without the controller reaching it.
//!
//! Why a single controller is not reckless in the meantime: every action it
//! takes is epoch-fenced by the data nodes' manifests. A wrong or duplicate
//! decision is rejected with -FENCED; the blast radius of a bad controller
//! is bounded by machinery that already exists and is chaos-tested.
//!
//! Usage:
//!   flint-controller --master 127.0.0.1:6460 --replica 127.0.0.1:6470 \
//!     [--poll-ms 200] [--confirm 3] [--max-stale-ms 5000]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use flint_resp::{Decoded, Value, decode, encode};

fn arg(name: &str) -> Option<String> {
    std::env::args().skip_while(|a| a != name).nth(1)
}

fn arg_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    arg(name).and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// One-shot RESP call: connect, send, read one reply. Cheap and stateless —
/// the controller polls, it does not hold long connections.
fn call(addr: &str, args: &[&[u8]]) -> std::io::Result<Value> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_millis(800)))?;
    stream.set_write_timeout(Some(Duration::from_millis(800)))?;
    let frame = Value::Array(Some(
        args.iter().map(|a| Value::Bulk(Some(a.to_vec()))).collect(),
    ));
    let mut out = Vec::new();
    encode(&frame, &mut out);
    stream.write_all(&out)?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match decode(&buf) {
            Ok(Decoded::Complete(v, _)) => return Ok(v),
            Ok(Decoded::NeedMore) => {
                let n = stream.read(&mut chunk)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "closed",
                    ));
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("{e:?}"),
                ));
            }
        }
    }
}

#[derive(Debug, Default)]
struct Info {
    role: String,
    role_epoch_counter: u32,
    live_replicas: u32,
    seq_lag: Option<u64>, // None = no live replica / not reported
}

fn flintinfo(addr: &str) -> Option<Info> {
    let Value::Bulk(Some(raw)) = call(addr, &[b"FLINTINFO"]).ok()? else {
        return None;
    };
    let text = String::from_utf8_lossy(&raw);
    let mut info = Info::default();
    for line in text.split(['\r', '\n']).filter(|l| !l.is_empty()) {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        match k {
            "role" => info.role = v.to_string(),
            "live_replicas" => info.live_replicas = v.parse().unwrap_or(0),
            "seq_lag" => info.seq_lag = v.parse().ok(),
            "role_epoch" => {
                // (0,2) -> 2
                info.role_epoch_counter = v
                    .trim_matches(|c| c == '(' || c == ')')
                    .split(',')
                    .nth(1)
                    .and_then(|c| c.parse().ok())
                    .unwrap_or(0);
            }
            _ => {}
        }
    }
    Some(info)
}

fn reachable(addr: &str) -> bool {
    matches!(call(addr, &[b"PING"]), Ok(Value::Simple(s)) if s == "PONG")
}

fn main() {
    let master = arg("--master").expect("--master HOST:PORT required");
    let replica = arg("--replica").expect("--replica HOST:PORT required");
    let poll = Duration::from_millis(arg_or("--poll-ms", 200));
    let confirm: u32 = arg_or("--confirm", 3);
    let max_stale = Duration::from_millis(arg_or("--max-stale-ms", 5_000));

    eprintln!(
        "flint-controller: watching master={master} replica={replica} \
         (poll={poll:?} confirm={confirm} max-stale={max_stale:?})"
    );
    eprintln!(
        "NOTE: single-node decider; epoch fencing on the data nodes bounds any wrong decision."
    );

    // Current roles as the controller understands them. On a master kill,
    // the survivor is promoted and the roles swap; the old master, if it
    // returns, is fenced.
    let mut master = master;
    let mut replica = replica;
    let mut miss = 0u32;
    // Last time the pair was sequence-converged — the safety gate for
    // automatic promotion.
    let mut last_converged = Instant::now();
    let mut converged_ever = false;

    loop {
        std::thread::sleep(poll);

        // Track convergence health from the master's view.
        if let Some(m) = flintinfo(&master) {
            if m.live_replicas >= 1 && m.seq_lag == Some(0) {
                last_converged = Instant::now();
                converged_ever = true;
            }
            miss = 0;
            // Zombie check: if the node we believe is the REPLICA now claims
            // master (a returned old master, or a mis-start), fence it.
            if let Some(r) = flintinfo(&replica)
                && r.role == "master"
            {
                fence_zombie(&master, &replica, &r);
            }
            continue;
        }

        // Master did not answer FLINTINFO this tick.
        miss += 1;
        if miss < confirm {
            continue;
        }
        // Confirmed unreachable across `confirm` consecutive probes.
        if reachable(&master) {
            // It answered a bare PING — treat as a transient FLINTINFO blip.
            miss = 0;
            continue;
        }

        eprintln!("master {master} unreachable for {confirm} probes; evaluating failover");

        // Safety gate: only auto-promote a survivor that was recently
        // sequence-converged. Otherwise this is a degraded-window failure
        // (the survivor may be far behind) — refuse and page. The
        // spare-seed/S3 recovery path is deferred.
        if !converged_ever || last_converged.elapsed() > max_stale {
            eprintln!(
                "REFUSING auto-promotion: pair not converged within {max_stale:?} \
                 (degraded window — needs spare/S3 recovery, not implemented). PAGE."
            );
            // Keep watching; if the master returns, resume.
            std::thread::sleep(Duration::from_secs(1));
            miss = 0;
            continue;
        }

        if !reachable(&replica) {
            eprintln!("survivor {replica} also unreachable — cannot fail over. PAGE.");
            std::thread::sleep(Duration::from_secs(1));
            miss = 0;
            continue;
        }

        // Read-then-bump the epoch from the survivor's durable role.
        let cur = flintinfo(&replica)
            .map(|i| i.role_epoch_counter)
            .unwrap_or(1);
        let next = cur + 1;
        match call(
            &replica,
            &[b"FLINTPROMOTE", b"0", next.to_string().as_bytes()],
        ) {
            Ok(Value::Simple(s)) => {
                eprintln!("PROMOTED {replica} to master at (0,{next}): {s}");
                // Roles swap: the survivor is the new master; the old master
                // is now the node to fence if it returns.
                std::mem::swap(&mut master, &mut replica);
                miss = 0;
                converged_ever = false; // new pair has no replica yet
            }
            other => {
                eprintln!("promotion of {replica} FAILED: {other:?} — retrying next tick");
            }
        }
    }
}

/// Fence a node that wrongly believes it is master (returned zombie) with a
/// strictly higher epoch than the true master's.
fn fence_zombie(true_master: &str, zombie: &str, zombie_info: &Info) {
    let true_epoch = flintinfo(true_master)
        .map(|i| i.role_epoch_counter)
        .unwrap_or(zombie_info.role_epoch_counter);
    let next = true_epoch.max(zombie_info.role_epoch_counter) + 1;
    match call(zombie, &[b"FLINTDEMOTE", b"0", next.to_string().as_bytes()]) {
        Ok(Value::Simple(s)) => {
            eprintln!("FENCED zombie {zombie} at (0,{next}): {s}")
        }
        other => eprintln!("fencing {zombie} FAILED: {other:?}"),
    }
}
