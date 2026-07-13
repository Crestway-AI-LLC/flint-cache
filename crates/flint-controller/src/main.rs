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
//! Multi-pair: one controller drives N pairs (a group), each with its own
//! failover state, ticked independently every sweep. Pairs are separated by
//! ';'. Equal-epoch ties in master selection break to the lowest address so
//! concurrent controllers in the HA set agree (ADR-0004).
//!
//! Usage:
//!   flint-controller --nodes 127.0.0.1:6460,127.0.0.1:6470 [--id A]
//!   flint-controller --pairs "a1,b1;a2,b2" [--id A]
//!   flint-controller --manage-slots 6460:/data/a,6470:/data/b [--id A]
//!   flint-controller --manage-pairs "6500:/d/a,6501:/d/b;6510:/d/c,6511:/d/d"
//!   common: [--poll-ms 200] [--confirm 3] [--max-stale-ms 5000] [--lease-ttl-ms 3000]

mod planner;

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
fn spawn_slot(
    bin: &str,
    slot: &Slot,
    master: Option<&str>,
    min_replicas: u32,
) -> std::process::Child {
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
    // Every managed node carries the write-quorum gate: roles float, so a
    // replica promoted after a kill must also shed writes while widowed.
    if min_replicas > 0 {
        cmd.args(["--min-replicas-to-write", &min_replicas.to_string()]);
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

/// Read-only knobs shared by every pair this controller drives.
struct Config {
    poll: Duration,
    confirm: u32,
    max_stale: Duration,
    lease_ttl: u64,
    min_replicas: u32,
    /// Positive enables the rebalance planner (dry-run: logs the plan). The
    /// value is the deadband fraction — imbalance below it is left alone.
    rebalance_deadband: f64,
    /// Nodes to run MIGRATION RECOVERY over (separate from failover, since
    /// these are independent masters, not a pair). On restart the controller
    /// observes their in-flight migration records and resumes or rolls back.
    recover_nodes: Vec<String>,
    bin: String,
    id: String,
}

/// One replica set the controller drives, with its own failover state. The
/// controller ticks each pair independently every sweep; a group is just N
/// pairs. Decision-only pairs have empty `slots` (external node lifecycle);
/// managed pairs are supervised (bootstrap + spare respawn).
struct Pair {
    label: String,
    nodes: Vec<String>,
    slots: Vec<Slot>,
    managed: bool,
    last_converged: Instant,
    converged_ever: bool,
    no_master_streak: u32,
    slot_miss: Vec<u32>,
    slot_cooldown: Vec<Instant>,
    slot_child: Vec<Option<std::process::Child>>,
    last_page: Option<Instant>,
}

impl Pair {
    fn new(label: String, nodes: Vec<String>, slots: Vec<Slot>, managed: bool) -> Self {
        let n = slots.len();
        Self {
            label,
            nodes,
            managed,
            last_converged: Instant::now(),
            converged_ever: false,
            no_master_streak: 0,
            slot_miss: vec![0; n],
            slot_cooldown: vec![Instant::now(); n],
            slot_child: (0..n).map(|_| None).collect(),
            last_page: None,
            slots,
        }
    }

    fn decision(label: String, nodes: Vec<String>) -> Self {
        Self::new(label, nodes, Vec::new(), false)
    }

    fn managed(label: String, slots: Vec<Slot>) -> Self {
        let nodes = slots
            .iter()
            .map(|s| format!("127.0.0.1:{}", s.port))
            .collect();
        Self::new(label, nodes, slots, true)
    }

    /// Reap the previous child in slot `i` before recording the new one, so a
    /// long-running supervisor never accumulates defunct processes.
    fn reap(&mut self, i: usize, child: std::process::Child) {
        if let Some(mut old) = self.slot_child[i].replace(child) {
            let _ = old.kill();
            let _ = old.wait();
        }
    }

    /// Observe this pair's load for the rebalance planner: the reachable
    /// master's key count (DBSIZE). None if no master answers.
    fn observe_fill(&self) -> Option<planner::PairLoad> {
        for addr in &self.nodes {
            let n = observe(addr);
            if n.reachable
                && n.role == "master"
                && let Ok(Value::Integer(fill)) = call(addr, &[b"DBSIZE"])
            {
                return Some(planner::PairLoad {
                    label: self.label.clone(),
                    fill: fill.max(0) as u64,
                });
            }
        }
        None
    }

    /// Page at most every 2s per pair, so a persistently degraded pair does
    /// not spam the log or (via a blocking sleep) starve the other pairs.
    fn page(&mut self, msg: std::fmt::Arguments) {
        if self
            .last_page
            .is_none_or(|t| t.elapsed() > Duration::from_secs(2))
        {
            eprintln!("{msg}");
            self.last_page = Some(Instant::now());
        }
    }

    /// One observe→decide pass for this pair.
    fn tick(&mut self, cfg: &Config) {
        let states: Vec<Node> = self.nodes.iter().map(|a| observe(a)).collect();

        // Managed bootstrap: nothing up → launch slot0 as master, rest as
        // fresh replicas of it.
        if self.managed && states.iter().all(|n| !n.reachable) {
            eprintln!("[{}][{}] bootstrapping managed pair", cfg.id, self.label);
            let c0 = spawn_slot(&cfg.bin, &self.slots[0], None, cfg.min_replicas);
            self.reap(0, c0);
            let master_addr = self.nodes[0].clone();
            std::thread::sleep(Duration::from_millis(600));
            for i in 1..self.slots.len() {
                let c = spawn_slot(
                    &cfg.bin,
                    &self.slots[i],
                    Some(&master_addr),
                    cfg.min_replicas,
                );
                self.reap(i, c);
            }
            std::thread::sleep(Duration::from_millis(400));
            return;
        }

        let masters: Vec<&Node> = states
            .iter()
            .filter(|n| n.reachable && n.role == "master")
            .collect();
        let max_epoch = states.iter().map(|n| n.epoch).max().unwrap_or(0);

        // Legitimate master = reachable master with the highest epoch; ties
        // broken by LOWEST address so every controller in the HA set picks
        // the same winner (ADR-0004 tie-break, applied identically here and
        // in the fence rule below).
        let legit = masters
            .iter()
            .max_by_key(|n| (n.epoch, std::cmp::Reverse(n.addr.as_str())))
            .copied();

        if let Some(legit) = legit {
            let legit_addr = legit.addr.clone();
            let legit_converged = legit.live_replicas >= 1 && legit.seq_lag == Some(0);
            self.no_master_streak = 0;
            // Renew the legit master's lease (idempotent across the HA set;
            // only extends life, never un-fences).
            if cfg.lease_ttl > 0 {
                let _ = call(
                    &legit_addr,
                    &[b"FLINTLEASE", cfg.lease_ttl.to_string().as_bytes()],
                );
            }
            // Any other reachable master-claimer is a zombie: fence it.
            for m in &masters {
                if m.addr != legit_addr {
                    fence(&cfg.id, &m.addr, max_epoch + 1);
                }
            }
            if legit_converged {
                self.last_converged = Instant::now();
                self.converged_ever = true;
            }

            // Redundancy repair (managed): a non-master slot dead for
            // `confirm` ticks is respawned as a FRESH replica of the current
            // master. A cooldown after a respawn avoids thrash while it syncs.
            if self.managed {
                for i in 0..self.slots.len() {
                    let slot_addr = format!("127.0.0.1:{}", self.slots[i].port);
                    if slot_addr == legit_addr {
                        self.slot_miss[i] = 0;
                        continue;
                    }
                    let reachable = states
                        .iter()
                        .find(|n| n.addr == slot_addr)
                        .is_some_and(|n| n.reachable);
                    if reachable || Instant::now() < self.slot_cooldown[i] {
                        if reachable {
                            self.slot_miss[i] = 0;
                        }
                        continue;
                    }
                    self.slot_miss[i] += 1;
                    if self.slot_miss[i] >= cfg.confirm {
                        eprintln!(
                            "[{}][{}] slot :{} dead — respawning as fresh replica of {legit_addr}",
                            cfg.id, self.label, self.slots[i].port
                        );
                        let c = spawn_slot(
                            &cfg.bin,
                            &self.slots[i],
                            Some(&legit_addr),
                            cfg.min_replicas,
                        );
                        self.reap(i, c);
                        self.slot_miss[i] = 0;
                        self.slot_cooldown[i] = Instant::now() + Duration::from_secs(20);
                    }
                }
            }
            return;
        }

        // No reachable master. Confirm across `confirm` ticks before acting.
        self.no_master_streak += 1;
        if self.no_master_streak < cfg.confirm {
            return;
        }

        // Degraded-window gate: only auto-promote a recently-converged pair.
        if !self.converged_ever || self.last_converged.elapsed() > cfg.max_stale {
            let (id, label, ms) = (cfg.id.clone(), self.label.clone(), cfg.max_stale);
            self.page(format_args!(
                "[{id}][{label}] no master and pair not converged within {ms:?} — REFUSING (degraded window; needs spare/S3). PAGE."
            ));
            self.no_master_streak = 0;
            return;
        }

        // Survivor = reachable node with the highest epoch; ties by lowest
        // address (deterministic across the HA set).
        let Some(survivor) = states
            .iter()
            .filter(|n| n.reachable)
            .max_by_key(|n| (n.epoch, std::cmp::Reverse(n.addr.as_str())))
        else {
            let (id, label) = (cfg.id.clone(), self.label.clone());
            self.page(format_args!(
                "[{id}][{label}] no reachable node in the pair — cannot fail over. PAGE."
            ));
            self.no_master_streak = 0;
            return;
        };

        let next = max_epoch + 1;
        match call(
            &survivor.addr,
            &[b"FLINTPROMOTE", b"0", next.to_string().as_bytes()],
        ) {
            Ok(Value::Simple(s)) => {
                eprintln!(
                    "[{}][{}] PROMOTED {} at (0,{next}): {s}",
                    cfg.id, self.label, survivor.addr
                );
                self.converged_ever = false; // new master has no replica yet
                self.no_master_streak = 0;
            }
            // -FENCED here means another controller already promoted at this
            // or a higher epoch: the desired outcome exists, so it's fine.
            Ok(Value::Error(e)) if e.starts_with("FENCED") => {
                eprintln!(
                    "[{}][{}] promotion fenced (another controller won): {e}",
                    cfg.id, self.label
                );
                self.no_master_streak = 0;
            }
            other => eprintln!(
                "[{}][{}] promotion of {} failed: {other:?}",
                cfg.id, self.label, survivor.addr
            ),
        }
    }
}

/// Parse "PORT:DIR,PORT:DIR" into a pair's slots.
fn parse_slots(spec: &str) -> Vec<Slot> {
    spec.split(',')
        .map(|pair| {
            let (p, d) = pair.split_once(':').expect("PORT:DIR");
            Slot {
                port: p.parse().expect("port"),
                dir: d.to_string(),
            }
        })
        .collect()
}

/// Build the pairs from CLI. Single-pair flags (--nodes / --manage-slots) are
/// preserved; multi-pair flags (--pairs / --manage-pairs) separate pairs with
/// ';'. A group is expressed as several pairs to one controller.
fn build_pairs() -> Vec<Pair> {
    if let Some(spec) = arg("--manage-pairs") {
        return spec
            .split(';')
            .enumerate()
            .map(|(i, p)| Pair::managed(format!("g{i}"), parse_slots(p)))
            .collect();
    }
    if let Some(spec) = arg("--manage-slots") {
        return vec![Pair::managed("g0".into(), parse_slots(&spec))];
    }
    if let Some(spec) = arg("--pairs") {
        return spec
            .split(';')
            .enumerate()
            .map(|(i, p)| Pair::decision(format!("g{i}"), p.split(',').map(String::from).collect()))
            .collect();
    }
    if let Some(spec) = arg("--nodes") {
        return vec![Pair::decision(
            "g0".into(),
            spec.split(',').map(String::from).collect(),
        )];
    }
    Vec::new()
}

fn main() {
    let cfg = Config {
        poll: Duration::from_millis(arg_or("--poll-ms", 200)),
        confirm: arg_or("--confirm", 3),
        max_stale: Duration::from_millis(arg_or("--max-stale-ms", 5_000)),
        // Lease TTL handed to each master per renewal. Generous vs the poll
        // interval so transient controller unavailability never trips a
        // healthy master; a master self-fences only after this long with NO
        // controller (of any in the HA set) reaching it. 0 disables leases.
        lease_ttl: arg_or("--lease-ttl-ms", 3_000),
        // Passed to every managed node so a promoted-then-widowed master
        // sheds writes (Redis min-replicas-to-write). 0 = disabled.
        min_replicas: arg("--min-replicas-to-write")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        rebalance_deadband: arg("--rebalance-deadband")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0),
        recover_nodes: arg("--recover-nodes")
            .map(|s| s.split(',').map(String::from).collect())
            .unwrap_or_default(),
        bin: server_bin(),
        id: arg("--id").unwrap_or_else(|| "ctl".into()),
    };

    let mut pairs = build_pairs();
    assert!(
        !pairs.is_empty() || !cfg.recover_nodes.is_empty(),
        "need --nodes/--pairs, --manage-slots/--manage-pairs, or --recover-nodes"
    );

    eprintln!(
        "[{}] flint-controller: {} pair(s) recover-nodes={} poll={:?} confirm={}",
        cfg.id,
        pairs.len(),
        cfg.recover_nodes.len(),
        cfg.poll,
        cfg.confirm,
    );

    let mut last_rebalance = Instant::now();
    let mut last_recover = Instant::now();
    loop {
        std::thread::sleep(cfg.poll);
        for pair in pairs.iter_mut() {
            pair.tick(&cfg);
        }

        // Migration recovery: reconcile in-flight moves left by an
        // interruption (a restart mid-cutover). Cheap when nothing is in
        // flight (one FLINTMIGRATIONS per node returning empty).
        if !cfg.recover_nodes.is_empty() && last_recover.elapsed() > Duration::from_secs(2) {
            last_recover = Instant::now();
            recover_migrations(&cfg);
        }

        // Rebalance planning (dry-run): every 5s, observe each pair's fill
        // and log the deterministic-hysteresis plan. Execution via
        // FLINTMIGRATEIN, gated by the safety rules, is a follow-on.
        if cfg.rebalance_deadband > 0.0 && last_rebalance.elapsed() > Duration::from_secs(5) {
            last_rebalance = Instant::now();
            let loads: Vec<planner::PairLoad> =
                pairs.iter().filter_map(|p| p.observe_fill()).collect();
            if loads.len() >= 2 {
                let fills: Vec<(String, u64)> =
                    loads.iter().map(|l| (l.label.clone(), l.fill)).collect();
                let plan = planner::plan_moves(&loads, cfg.rebalance_deadband);
                if plan.is_empty() {
                    eprintln!(
                        "[{}] rebalance: balanced within deadband {:.2} — fills={fills:?}",
                        cfg.id, cfg.rebalance_deadband
                    );
                } else {
                    for m in &plan {
                        eprintln!(
                            "[{}] rebalance PLAN (dry-run): move ~{} load from {} to {} — fills={fills:?}",
                            cfg.id, m.approx, m.from, m.to
                        );
                    }
                }
            }
        }
    }
}

fn reachable(addr: &str) -> bool {
    matches!(call(addr, &[b"PING"]), Ok(Value::Simple(s)) if s == "PONG")
}

/// Like `call` but with a long read timeout, for driving a blocking
/// FLINTMIGRATEIN (which streams a whole slot) during recovery.
fn call_slow(addr: &str, args: &[&[u8]], timeout: Duration) -> std::io::Result<Value> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(timeout))?;
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

/// One in-flight migration record observed on a node.
struct InFlight {
    node: String,
    slot: u16,
    peer: String,
}

/// Reconcile migrations interrupted by a restart. Observing the two
/// participants' durable records is enough to decide (ADR-0004: every
/// structural state is observable from manifests):
///   - a destination still Importing S from source O  -> RESUME (re-drive the
///     move; if O is gone, roll back so the dest stops redirecting to it);
///   - a source Migrating S to dest D whose Importing is already gone (D owns
///     it — the flip half-completed) -> COMPLETE (tell O to answer -MOVED);
///     if D is gone entirely, unfreeze O so it serves the slot again.
fn recover_migrations(cfg: &Config) {
    let mut importing: Vec<InFlight> = Vec::new();
    let mut migrating: Vec<InFlight> = Vec::new();
    for node in &cfg.recover_nodes {
        let Ok(Value::Array(Some(rows))) = call(node, &[b"FLINTMIGRATIONS"]) else {
            continue;
        };
        for row in rows {
            let Value::Bulk(Some(b)) = row else { continue };
            let line = String::from_utf8_lossy(&b);
            let parts: Vec<&str> = line.split(' ').collect();
            let [slot, phase, peer] = parts.as_slice() else {
                continue;
            };
            let Ok(slot) = slot.parse::<u16>() else {
                continue;
            };
            let rec = InFlight {
                node: node.clone(),
                slot,
                peer: peer.to_string(),
            };
            match *phase {
                "importing" => importing.push(rec),
                "migrating" => migrating.push(rec),
                _ => {}
            }
        }
    }
    if importing.is_empty() && migrating.is_empty() {
        return;
    }

    for rec in &importing {
        let (dest, src) = (&rec.node, &rec.peer);
        if reachable(src) {
            eprintln!(
                "[{}] recovery: resuming import of slot {} into {dest} from {src}",
                cfg.id, rec.slot
            );
            let r = call_slow(
                dest,
                &[
                    b"FLINTMIGRATEIN",
                    src.as_bytes(),
                    rec.slot.to_string().as_bytes(),
                    dest.as_bytes(),
                ],
                Duration::from_secs(120),
            );
            eprintln!(
                "[{}] recovery: resume of slot {} -> {r:?}",
                cfg.id, rec.slot
            );
        } else {
            eprintln!(
                "[{}] recovery: source {src} unreachable — aborting import of slot {} at {dest}",
                cfg.id, rec.slot
            );
            let _ = call(dest, &[b"FLINTSLOTABORT", rec.slot.to_string().as_bytes()]);
        }
    }

    for rec in &migrating {
        let (src, dest) = (&rec.node, &rec.peer);
        // If the dest is still importing this slot, the resume loop owns it.
        if importing
            .iter()
            .any(|i| &i.node == dest && i.slot == rec.slot)
        {
            continue;
        }
        if reachable(dest) {
            eprintln!(
                "[{}] recovery: completing flip of slot {} — {src} -> Moved to {dest}",
                cfg.id, rec.slot
            );
            let _ = call(
                src,
                &[
                    b"FLINTSLOTMOVED",
                    rec.slot.to_string().as_bytes(),
                    dest.as_bytes(),
                ],
            );
        } else {
            eprintln!(
                "[{}] recovery: dest {dest} gone — unfreezing slot {} on {src}",
                cfg.id, rec.slot
            );
            let _ = call(src, &[b"FLINTSLOTABORT", rec.slot.to_string().as_bytes()]);
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
