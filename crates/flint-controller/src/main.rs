//! flint-controller (v0): the meta trio's DECISION LOGIC.
//!
//! Discovery-based and STATELESS about roles: every tick it re-observes all
//! nodes of a pair and derives the current truth (who is master, at what
//! epoch, is the replica converged) rather than tracking role state
//! internally. That has two payoffs:
//!   - Correctness is a pure function of observed reality, so a controller
//!     that missed an event simply rediscovers it next tick.
//!   - Multiple controllers can run concurrently for HA: they all observe
//!     the same reality and reach the same conclusions, and every action is
//!     epoch-fenced by the data nodes' manifests — a duplicate or racing
//!     decision is rejected with -FENCED, never corrupts. Redundant work is
//!     the only cost, which a future Raft coordinator removes.
//!
//! Per tick:
//!   - legitimate master = reachable node claiming `role:master` with the
//!     highest epoch;
//!   - any OTHER node claiming master is a zombie → FLINTDEMOTE it;
//!   - if no master is reachable but a survivor exists and the pair was
//!     sequence-converged (seq_lag==0) recently → FLINTPROMOTE the survivor;
//!   - a survivor that was NOT recently converged is a degraded-window
//!     failure → refuse and page (spare/S3 recovery is deferred).
//!
//! Push leases: the controller renews the master's lease each tick
//! (FLINTLEASE); a master partitioned from every controller self-fences on
//! TTL expiry on its own.
//!
//! Managed mode (--manage-slots PORT:DIR,...): the controller also SUPERVISES
//! nodes — bootstraps the pair and respawns a dead non-master slot as a
//! fresh replica of the current master (spawn-a-fresh-node spare model),
//! owning the whole failover cycle. --nodes (addresses only) is
//! decision-only, for when node lifecycle is external.
//!
//! Usage:
//!   flint-controller --nodes 127.0.0.1:6460,127.0.0.1:6470 [--id A]
//!   flint-controller --manage-slots 6460:/data/a,6470:/data/b [--id A]
//!   common: [--poll-ms 200] [--confirm 3] [--max-stale-ms 5000] [--lease-ttl-ms 3000]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::Command;
use std::time::{Duration, Instant};

use flint_resp::{Decoded, Value, decode, encode};

/// A node the controller supervises (managed mode): its port and data dir.
struct Slot {
    port: u16,
    dir: String,
}

fn server_bin() -> String {
    arg("--server-bin").unwrap_or_else(|| {
        format!(
            "{}/../../target/release/flint-server",
            env!("CARGO_MANIFEST_DIR")
        )
    })
}

fn kill_port(port: u16) {
    let _ = Command::new("pkill")
        .args(["-9", "-f", &format!("flint-server --port {port}")])
        .status();
}

/// Launch a flint-server for `slot`. `master` = None spawns a fresh master
/// (bootstrap only); Some(addr) spawns a fresh replica of that master. The
/// data dir is wiped first — "spawn a fresh node" always seeds from scratch
/// (full sync), never resurrects stale bytes. Returns the child so the
/// supervisor can reap it on the next respawn (no defunct accumulation).
fn spawn_slot(bin: &str, slot: &Slot, master: Option<&str>) -> std::process::Child {
    kill_port(slot.port);
    let _ = std::fs::remove_dir_all(&slot.dir);
    let mut cmd = Command::new(bin);
    cmd.args([
        "--port",
        &slot.port.to_string(),
        "--engine",
        "rocks",
        "--data-dir",
        &slot.dir,
    ]);
    if let Some(m) = master {
        cmd.args(["--replica-of", m]);
    }
    cmd.stderr(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("spawn flint-server")
}

fn arg(name: &str) -> Option<String> {
    std::env::args().skip_while(|a| a != name).nth(1)
}

fn arg_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    arg(name).and_then(|v| v.parse().ok()).unwrap_or(default)
}

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

#[derive(Debug, Clone)]
struct Node {
    addr: String,
    reachable: bool,
    role: String, // "master" | "replica" | "" (unknown/unreachable)
    epoch: u32,
    live_replicas: u32,
    seq_lag: Option<u64>,
}

fn observe(addr: &str) -> Node {
    let mut node = Node {
        addr: addr.to_string(),
        reachable: false,
        role: String::new(),
        epoch: 0,
        live_replicas: 0,
        seq_lag: None,
    };
    let Ok(Value::Bulk(Some(raw))) = call(addr, &[b"FLINTINFO"]) else {
        // Distinguish "down" from "up but FLINTINFO hiccup" with a PING.
        node.reachable = matches!(call(addr, &[b"PING"]), Ok(Value::Simple(s)) if s == "PONG");
        return node;
    };
    node.reachable = true;
    for line in String::from_utf8_lossy(&raw)
        .split(['\r', '\n'])
        .filter(|l| !l.is_empty())
    {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        match k {
            "role" => node.role = v.to_string(),
            "live_replicas" => node.live_replicas = v.parse().unwrap_or(0),
            "seq_lag" => node.seq_lag = v.parse().ok(),
            "role_epoch" => {
                node.epoch = v
                    .trim_matches(|c| c == '(' || c == ')')
                    .split(',')
                    .nth(1)
                    .and_then(|c| c.parse().ok())
                    .unwrap_or(0);
            }
            _ => {}
        }
    }
    node
}

fn main() {
    let poll = Duration::from_millis(arg_or("--poll-ms", 200));
    let confirm: u32 = arg_or("--confirm", 3);
    let max_stale = Duration::from_millis(arg_or("--max-stale-ms", 5_000));
    // Lease TTL handed to the master on each renewal. Generous vs the poll
    // interval so transient controller unavailability never trips a healthy
    // master; a master self-fences only after this long with NO controller
    // (of any in the HA set) reaching it. 0 disables lease renewal.
    let lease_ttl: u64 = arg_or("--lease-ttl-ms", 3_000);
    let id = arg("--id").unwrap_or_else(|| "ctl".into());

    // Managed mode: --manage-slots PORT:DIR,PORT:DIR — the controller
    // SUPERVISES these nodes (bootstraps them, and respawns a dead
    // non-master slot as a fresh replica of the current master), owning the
    // full failover cycle. Otherwise --nodes gives addresses only and node
    // lifecycle is external (decision-only mode).
    let slots: Vec<Slot> = arg("--manage-slots")
        .map(|s| {
            s.split(',')
                .map(|pair| {
                    let (p, d) = pair.split_once(':').expect("PORT:DIR");
                    Slot {
                        port: p.parse().expect("port"),
                        dir: d.to_string(),
                    }
                })
                .collect()
        })
        .unwrap_or_default();
    let managed = !slots.is_empty();
    let bin = server_bin();
    let nodes: Vec<String> = if managed {
        slots
            .iter()
            .map(|s| format!("127.0.0.1:{}", s.port))
            .collect()
    } else {
        arg("--nodes")
            .expect("--nodes a,b[,c] or --manage-slots PORT:DIR,... required")
            .split(',')
            .map(|s| s.to_string())
            .collect()
    };

    eprintln!(
        "[{id}] flint-controller: nodes={nodes:?} managed={managed} poll={poll:?} confirm={confirm} max-stale={max_stale:?}"
    );

    let mut last_converged = Instant::now();
    let mut converged_ever = false;
    let mut no_master_streak = 0u32;
    // Per-slot dead-streak and respawn cooldown (managed mode).
    let mut slot_miss = vec![0u32; slots.len()];
    let mut slot_cooldown = vec![Instant::now(); slots.len()];
    // Child handles of supervised nodes, reaped on respawn so a long-running
    // supervisor never accumulates defunct processes.
    let mut slot_child: Vec<Option<std::process::Child>> = slots.iter().map(|_| None).collect();
    let reap =
        |i: usize, child: std::process::Child, handles: &mut Vec<Option<std::process::Child>>| {
            if let Some(mut old) = handles[i].replace(child) {
                let _ = old.kill();
                let _ = old.wait();
            }
        };

    loop {
        std::thread::sleep(poll);
        let states: Vec<Node> = nodes.iter().map(|a| observe(a)).collect();

        // Managed bootstrap: nothing is up → launch slot0 as master, the
        // rest as fresh replicas of it.
        if managed && states.iter().all(|n| !n.reachable) {
            eprintln!("[{id}] bootstrapping managed pair");
            let c0 = spawn_slot(&bin, &slots[0], None);
            reap(0, c0, &mut slot_child);
            let master_addr = nodes[0].clone();
            std::thread::sleep(Duration::from_millis(600));
            for (i, slot) in slots.iter().enumerate().skip(1) {
                let c = spawn_slot(&bin, slot, Some(&master_addr));
                reap(i, c, &mut slot_child);
            }
            std::thread::sleep(Duration::from_millis(400));
            continue;
        }

        let masters: Vec<&Node> = states
            .iter()
            .filter(|n| n.reachable && n.role == "master")
            .collect();
        let max_epoch = states.iter().map(|n| n.epoch).max().unwrap_or(0);

        if let Some(legit) = masters.iter().max_by_key(|n| n.epoch).copied() {
            no_master_streak = 0;
            // Renew the legitimate master's lease (idempotent across the HA
            // set; only extends life, never un-fences).
            if lease_ttl > 0 {
                let _ = call(
                    &legit.addr,
                    &[b"FLINTLEASE", lease_ttl.to_string().as_bytes()],
                );
            }
            // Any other reachable master-claimer is a zombie: fence it.
            for m in &masters {
                if m.addr != legit.addr {
                    let next = max_epoch + 1;
                    fence(&id, &m.addr, next);
                }
            }
            // Track convergence of the legitimate master's pair.
            if legit.live_replicas >= 1 && legit.seq_lag == Some(0) {
                last_converged = Instant::now();
                converged_ever = true;
            }

            // Redundancy repair (managed mode): a non-master slot that is
            // dead for `confirm` ticks is respawned as a FRESH replica of
            // the current master — the spawn-a-fresh-node spare model. A
            // cooldown after a respawn avoids thrashing while it full-syncs.
            if managed {
                for (i, slot) in slots.iter().enumerate() {
                    if format!("127.0.0.1:{}", slot.port) == legit.addr {
                        slot_miss[i] = 0;
                        continue;
                    }
                    let reachable = states
                        .iter()
                        .find(|n| n.addr == format!("127.0.0.1:{}", slot.port))
                        .is_some_and(|n| n.reachable);
                    if reachable || Instant::now() < slot_cooldown[i] {
                        if reachable {
                            slot_miss[i] = 0;
                        }
                        continue;
                    }
                    slot_miss[i] += 1;
                    if slot_miss[i] >= confirm {
                        eprintln!(
                            "[{id}] slot :{} dead — respawning as fresh replica of {}",
                            slot.port, legit.addr
                        );
                        let c = spawn_slot(&bin, slot, Some(&legit.addr));
                        reap(i, c, &mut slot_child);
                        slot_miss[i] = 0;
                        slot_cooldown[i] = Instant::now() + Duration::from_secs(20);
                    }
                }
            }
            continue;
        }

        // No reachable master. Confirm across `confirm` ticks before acting.
        no_master_streak += 1;
        if no_master_streak < confirm {
            continue;
        }

        // Degraded-window gate: only auto-promote a recently-converged pair.
        if !converged_ever || last_converged.elapsed() > max_stale {
            eprintln!(
                "[{id}] no master and pair not converged within {max_stale:?} — REFUSING (degraded window; needs spare/S3). PAGE."
            );
            no_master_streak = 0;
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }

        // The survivor is the reachable node with the highest epoch.
        let Some(survivor) = states
            .iter()
            .filter(|n| n.reachable)
            .max_by_key(|n| n.epoch)
        else {
            eprintln!("[{id}] no reachable node in the pair — cannot fail over. PAGE.");
            no_master_streak = 0;
            std::thread::sleep(Duration::from_secs(1));
            continue;
        };

        let next = max_epoch + 1;
        match call(
            &survivor.addr,
            &[b"FLINTPROMOTE", b"0", next.to_string().as_bytes()],
        ) {
            Ok(Value::Simple(s)) => {
                eprintln!("[{id}] PROMOTED {} at (0,{next}): {s}", survivor.addr);
                converged_ever = false; // new master has no replica yet
                no_master_streak = 0;
            }
            // -FENCED here means another controller already promoted at this
            // or a higher epoch: the desired outcome exists, so it's fine.
            Ok(Value::Error(e)) if e.starts_with("FENCED") => {
                eprintln!("[{id}] promotion fenced (another controller won): {e}");
                no_master_streak = 0;
            }
            other => eprintln!("[{id}] promotion of {} failed: {other:?}", survivor.addr),
        }
    }
}

fn fence(id: &str, zombie: &str, epoch: u32) {
    match call(
        zombie,
        &[b"FLINTDEMOTE", b"0", epoch.to_string().as_bytes()],
    ) {
        Ok(Value::Simple(s)) => eprintln!("[{id}] FENCED {zombie} at (0,{epoch}): {s}"),
        Ok(Value::Error(e)) if e.starts_with("FENCED") => {
            eprintln!("[{id}] fence of {zombie} already done by a peer: {e}")
        }
        other => eprintln!("[{id}] fence of {zombie} failed: {other:?}"),
    }
}
