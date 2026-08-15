// SPDX-License-Identifier: Elastic-2.0
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
//! (CPLEASE, self-renewed at the CP per ADR-0018); a master that cannot renew self-fences on
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
//!   common: [--poll-ms 100] [--confirm 3] [--max-stale-ms 5000]
//!           [--slow-promote-ms 4000]  (patience for a listening-but-slow master)
//!
//! No lease flags: ADR-0018 moved the write lease to the CP. Masters renew
//! their own lease (CPLEASE) against the CP they already journal to; this
//! process only commits the fencing record (CPFENCE) before it promotes.

// The rebalance planner + balance-policy seam lives in the shared
// `flint-balance` library so the managed plane reuses the same
// metric-agnostic planner (fed a different load metric). Aliased to
// `planner` to keep call sites unchanged.
use flint_balance as planner;

use std::io::{Read, Write};
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
    restore_from: Option<&str>,
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
    if let Some(root) = restore_from {
        cmd.args(["--restore-from", root]);
    }
    // Spawned nodes report their own transitions to the same journal.
    if let Some(Some(j)) = JOURNAL_TARGET.get() {
        cmd.args(["--journal", j]);
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

/// Internal-mesh mutual-TLS client config (--internal-* triple), set once at
/// startup; every controller→node dial goes through internal_connect, so the
/// control loop speaks mutual TLS to an mTLS data plane and plaintext
/// otherwise — one dial path.
static INTERNAL_CLIENT: std::sync::OnceLock<
    Option<std::sync::Arc<flint_tls::ReloadableClientConfig>>,
> = std::sync::OnceLock::new();

fn internal_connect(addr: &str) -> std::io::Result<flint_tls::Stream> {
    flint_tls::connect_reloadable(addr, INTERNAL_CLIENT.get().unwrap_or(&None))
}

/// Fleet-journal target (--journal <cp-addr>). Best-effort, detached: the
/// control loop's decisions never wait on the journal.
static JOURNAL_TARGET: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();

fn journal_event(
    actor: &str,
    kind: flint_journal::EventKind,
    subject: &str,
    epoch: Option<String>,
    cause: &str,
) {
    let Some(Some(target)) = JOURNAL_TARGET.get().cloned() else {
        return;
    };
    flint_journal::emit_detached(
        target,
        INTERNAL_CLIENT
            .get()
            .cloned()
            .unwrap_or(None)
            .map(|r| r.current()),
        flint_journal::Event {
            at_ms: flint_journal::now_ms(),
            actor: format!("controller:{actor}"),
            kind,
            subject: subject.to_string(),
            epoch,
            cause: Some(cause.to_string()),
            detail: None,
        },
    );
}

fn call(addr: &str, args: &[&[u8]]) -> std::io::Result<Value> {
    let mut stream = internal_connect(addr)?;
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
    /// The process still accepts TCP connections even if the app did not
    /// answer FLINTINFO/PING — i.e. ALIVE but slow, not dead. Only meaningful
    /// when `reachable` is false; true whenever `reachable` is true.
    socket_alive: bool,
    role: String, // "master" | "replica" | "" (unknown/unreachable)
    epoch: u32,
    live_replicas: u32,
    seq_lag: Option<u64>,
}

/// Does a process still accept TCP connections on this address? A plain,
/// non-TLS connect answers "is the port open" at the KERNEL level: a
/// CPU-starved master whose app cannot answer FLINTINFO still reads as alive
/// here, because the kernel completes the handshake without the process being
/// scheduled. A dead process refuses (or resets) the connection; a partitioned
/// one times out. Either non-alive case falls through to the fast promote.
fn socket_open(addr: &str) -> bool {
    use std::net::ToSocketAddrs;
    let Ok(mut addrs) = addr.to_socket_addrs() else {
        return false;
    };
    addrs.any(|sa| std::net::TcpStream::connect_timeout(&sa, Duration::from_millis(500)).is_ok())
}

/// Should the pair promote now? A master that REFUSES the connection is a dead
/// process — promote after `confirm` ticks (fast, RTO-critical, the killed-
/// master path every chaos drill exercises; a refused connect returns instantly
/// so the streak accrues at the poll rate). A master whose socket still accepts
/// connections but whose app times out is ALIVE but slow (CPU starvation, a
/// compaction burst); promoting away from it just flaps, and on a uniformly-
/// loaded fleet the target is equally slow. Hold it for `slow_promote` of
/// WALL-CLOCK — not ticks: observe() blocks on the timeouts of an unresponsive
/// node, so ticks no longer run at the poll rate and a tick count would mis-time
/// the escape by an order of magnitude. `slow_elapsed` is how long the pair has
/// been in the alive-but-slow state.
fn should_promote(
    slow: bool,
    no_master_streak: u32,
    confirm: u32,
    slow_elapsed: Option<Duration>,
    slow_promote: Duration,
) -> bool {
    if slow {
        slow_elapsed.is_some_and(|e| e >= slow_promote)
    } else {
        no_master_streak >= confirm
    }
}

/// The pair's own master, self-fenced but still serving its replica (#168).
///
/// flint-server flips a master to read-only when its lease goes unrenewed, and
/// FLINTINFO renders `role:` FROM that read-only flag — so anything that
/// starves renewals past the TTL (under ADR-0018, losing the CP; before it,
/// a controller stall) leaves a pair with no master-CLAIMER even
/// though nothing died and nothing diverged. That state is self-perpetuating:
/// `last_converged` only advances while a legitimate master exists, so once
/// every master fences, the degraded-window gate refuses forever and the pair
/// never takes another write without an operator. Measured on a healthy
/// 2-pair fleet: a 10s controller stall took it to zero masters, and it was
/// still at zero 45s after the controller came back.
///
/// So recognise the state instead of paging on it. A reachable node at the
/// top epoch reporting `live_replicas >= 1` with `seq_lag == 0` has a replica
/// ATTACHED AND CAUGHT UP — and a replica still following this node cannot
/// itself have been promoted, so there is no successor to split-brain
/// against. Promoting it is the same master resuming, not a degraded-window
/// gamble, and the epoch bump fences any stale lineage exactly as an ordinary
/// promotion does.
///
/// Deliberately strict, because this bypasses a data-safety gate: the node
/// must hold the TOP epoch among reachable nodes (never an ex-master that a
/// successor has already superseded) and its replica must be exactly caught
/// up (`seq_lag == 0`, not merely draining), so "in sync" is observed, not
/// assumed.
fn insync_lineage_holder(states: &[Node]) -> Option<&Node> {
    let top = states
        .iter()
        .filter(|n| n.reachable)
        .map(|n| n.epoch)
        .max()?;
    states
        .iter()
        .find(|n| n.reachable && n.epoch == top && n.live_replicas >= 1 && n.seq_lag == Some(0))
}

/// The member this controller last OBSERVED holding the pair's full lineage,
/// still reachable but no longer claiming to be master (#171).
///
/// [`insync_lineage_holder`] asks "is someone RIGHT NOW provably holding the
/// full lineage", and to answer yes it needs a live replica: `live_replicas >=
/// 1 && seq_lag == Some(0)`. That question is UNANSWERABLE for a pair whose
/// master died or self-fenced and whose partner has not re-attached — FLINTINFO
/// renders `seq_lag` as the string "none" whenever no live replica is attached,
/// so BOTH members report `live_replicas 0, seq_lag none` and nothing can
/// satisfy it. The gate then refuses a pair that is sitting there intact, on
/// every tick, forever. Measured on the 5 TB scale run: two pairs of four
/// permanently write-dead with every node alive and holding its data.
///
/// So REMEMBER rather than re-derive. `remembered` is the member this
/// controller last watched hold the whole lineage — a legitimate master with a
/// caught-up replica, or the survivor it promoted itself. If that member is
/// still reachable AND at the top epoch, promoting it cannot lose an
/// acknowledged write: it had everything at the last observation, and this
/// branch only runs when the pair has NO master, so nothing can have advanced
/// past it since.
///
/// This is strictly MORE information than the live predicate, not weaker
/// fencing. A survivor that was never observed in sync — the genuine degraded
/// window, where the pair's data may only exist on a node we cannot reach —
/// matches nothing here and still pages.
fn remembered_lineage_holder<'a>(
    states: &'a [Node],
    remembered: Option<&(String, u32)>,
) -> Option<&'a Node> {
    let (addr, epoch) = remembered?;
    let top = states
        .iter()
        .filter(|n| n.reachable)
        .map(|n| n.epoch)
        .max()?;
    states
        .iter()
        .find(|n| n.reachable && n.epoch == top && n.epoch >= *epoch && &n.addr == addr)
}

/// ADR-0018 moved lease renewal OUT of the controller entirely. The renewer
/// that lived here (#168's decoupling) kept the fence anchored to this one
/// process: five scale runs in a row turned some flavour of controller
/// silence into a fleet-wide self-fence of healthy masters (#168, #171,
/// #172). Masters now renew their own lease against the control plane
/// (CPLEASE), and this controller's contribution to fencing is exactly one
/// write: CPFENCE, committed durably BEFORE any FLINTPROMOTE, which the old
/// master's next renewal trips over. Controller death now costs failover
/// capability while it lasts — and nothing else.
fn observe(addr: &str) -> Node {
    let mut node = Node {
        addr: addr.to_string(),
        reachable: false,
        socket_alive: false,
        role: String::new(),
        epoch: 0,
        live_replicas: 0,
        seq_lag: None,
    };
    let Ok(Value::Bulk(Some(raw))) = call(addr, &[b"FLINTINFO"]) else {
        // Distinguish "down" from "up but FLINTINFO hiccup" with a PING.
        node.reachable = matches!(call(addr, &[b"PING"]), Ok(Value::Simple(s)) if s == "PONG");
        // App unresponsive: is the process DEAD (connection refused) or ALIVE
        // but slow (socket still accepts)? The promote decision needs this to
        // avoid flapping a starved-but-listening master to death.
        node.socket_alive = node.reachable || socket_open(addr);
        return node;
    };
    node.reachable = true;
    node.socket_alive = true;
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
    /// Patience for a master that still accepts TCP connections but whose app
    /// is unresponsive (slow, not dead) before promoting anyway. Far longer
    /// than `poll * confirm` so a compaction burst or a CPU-starved node does
    /// not flap. See `required_no_master_ticks`.
    slow_promote: Duration,
    max_stale: Duration,
    min_replicas: u32,
    /// Positive enables the rebalance planner. The value is the deadband
    /// fraction — imbalance below it is left alone. Without
    /// --rebalance-execute the plan is only logged (dry run).
    rebalance_deadband: f64,
    /// Execute planned moves (FLINTMIGRATEIN cutovers) instead of logging.
    rebalance_execute: bool,
    /// Executor pacing: at most this many slots ship per rebalance cycle;
    /// convergence happens over several observe→plan→move cycles.
    max_slots_per_cycle: usize,
    /// Control plane to COMMIT slot-ownership truth to after each
    /// successful cutover (Option B: CPSETSLOT <ns> <slot> <new-owner>).
    /// None = no CP (static drills): proxies learn via -MOVED as before.
    commit_cp: Option<String>,
    /// Nodes to run MIGRATION RECOVERY over (separate from failover, since
    /// these are independent masters, not a pair). On restart the controller
    /// observes their in-flight migration records and resumes or rolls back.
    recover_nodes: Vec<String>,
    /// Snapshot root directory (any mounted path; S3 via mount/sync on the
    /// same layout). Enables BOTH the schedule (periodic FLINTSNAPSHOT on
    /// each managed master into <root>/<pair-label>/) and disaster restore
    /// (whole-pair loss -> spawn a spare seeded from LATEST, which asserts
    /// mastership in a bumped generation).
    snapshot_root: Option<String>,
    snapshot_interval: Duration,
    /// How the rebalancer scores pair load. Open stack: `size` (balance by
    /// data). The private plane ships a `traffic` policy (balance by
    /// ops/second) behind the same `BalancePolicy` seam. Selected by
    /// --balance-policy; unknown names fail fast at startup.
    balance_policy: Box<dyn planner::BalancePolicy>,
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
    /// When the pair first had no legit master but a node still ACCEPTING TCP
    /// (alive-but-slow). Wall-clock, not ticks: observe() blocks on an
    /// unresponsive node's probe timeouts, so the loop is observe-bound rather
    /// than poll-bound, and a tick count would badly mis-time the escalation.
    slow_since: Option<Instant>,
    /// One "detected" journal per outage, not one per tick.
    outage_announced: bool,
    slot_miss: Vec<u32>,
    slot_cooldown: Vec<Instant>,
    slot_child: Vec<Option<std::process::Child>>,
    last_page: Option<Instant>,
    /// Consecutive ticks with ZERO reachable nodes; bootstrap/restore only
    /// past `confirm` — a transient double-blip must not wipe live nodes.
    dark_streak: u32,
    last_snapshot: Instant,
    /// (addr, epoch) of the member last OBSERVED holding the full lineage: a
    /// legitimate master with a caught-up replica, or the survivor this
    /// controller promoted. Read only by the degraded-window gate, to recover a
    /// pair whose lineage holder is alive but has lost its replica (#171) —
    /// the case `insync_lineage_holder` structurally cannot answer.
    last_insync: Option<(String, u32)>,
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
            slow_since: None,
            outage_announced: false,
            slot_miss: vec![0; n],
            slot_cooldown: vec![Instant::now(); n],
            slot_child: (0..n).map(|_| None).collect(),
            last_page: None,
            dark_streak: 0,
            last_snapshot: Instant::now(),
            last_insync: None,
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
    /// master's TOTAL key count across ALL namespaces (summed from
    /// FLINTSLOTSTATS — DBSIZE is namespace-scoped since tenancy, so it
    /// would read 0 on a node whose data lives in tenant namespaces), plus
    /// that master's address (the endpoint the executor ships from/to).
    fn observe_fill(
        &self,
        policy: &dyn planner::BalancePolicy,
    ) -> Option<(planner::PairLoad, String)> {
        for addr in &self.nodes {
            let n = observe(addr);
            if n.reachable && n.role == "master" {
                // The policy maps the master's per-slot stats to the single
                // load number the planner balances on. `size` (open default)
                // sums key counts; other policies weigh the same stats
                // differently without changing the planner.
                let fill = policy.pair_load(&slot_stats(addr));
                return Some((
                    planner::PairLoad {
                        label: self.label.clone(),
                        fill,
                    },
                    addr.clone(),
                ));
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

        // Managed bootstrap / disaster restore: nothing reachable for
        // `confirm` ticks → launch slot0 as master (seeded from the latest
        // snapshot when one exists — whole-pair loss), rest as fresh
        // replicas of it. Confirm-gated: a transient blip where both nodes
        // miss one poll must not wipe live processes.
        if self.managed && states.iter().all(|n| !n.reachable) {
            self.dark_streak += 1;
            if self.dark_streak < cfg.confirm {
                return;
            }
            self.dark_streak = 0;
            let snap_dir = cfg
                .snapshot_root
                .as_ref()
                .map(|r| format!("{r}/{}", self.label));
            let restore = snap_dir
                .as_deref()
                .filter(|d| std::path::Path::new(d).join("LATEST").exists());
            if let Some(root) = restore {
                eprintln!(
                    "[{}][{}] WHOLE PAIR DARK — restoring spare from snapshot root {root}",
                    cfg.id, self.label
                );
            } else {
                eprintln!("[{}][{}] bootstrapping managed pair", cfg.id, self.label);
            }
            let c0 = spawn_slot(&cfg.bin, &self.slots[0], None, cfg.min_replicas, restore);
            self.reap(0, c0);
            let master_addr = self.nodes[0].clone();
            std::thread::sleep(Duration::from_millis(600));
            for i in 1..self.slots.len() {
                let c = spawn_slot(
                    &cfg.bin,
                    &self.slots[i],
                    Some(&master_addr),
                    cfg.min_replicas,
                    None,
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
            self.slow_since = None;
            self.outage_announced = false;
            // Snapshot schedule (Tier-0, design.md §2.9): a periodic durable
            // checkpoint of each managed master into <root>/<pair-label>/.
            // This is what makes whole-pair loss survivable (spare restore
            // above) instead of a total-loss page.
            if let Some(root) = &cfg.snapshot_root
                && self.last_snapshot.elapsed() >= cfg.snapshot_interval
            {
                let dir = format!("{root}/{}", self.label);
                match call_slow(
                    &legit_addr,
                    &[b"FLINTSNAPSHOT", dir.as_bytes()],
                    Duration::from_secs(30),
                ) {
                    Ok(Value::Simple(reply)) => {
                        self.last_snapshot = Instant::now();
                        journal_event(
                            &cfg.id,
                            flint_journal::EventKind::SnapshotTaken,
                            &legit_addr,
                            None,
                            &format!("scheduled snapshot into {dir}: {reply}"),
                        );
                    }
                    other => eprintln!(
                        "[{}][{}] snapshot on {legit_addr} failed: {other:?}",
                        cfg.id, self.label
                    ),
                }
            }
            // Any other reachable master-claimer is a zombie: fence it.
            for m in &masters {
                if m.addr != legit_addr {
                    fence(&cfg.id, &m.addr, max_epoch + 1);
                }
            }
            if legit_converged {
                if !self.converged_ever {
                    // First convergence this process-life: auto-failover is
                    // now armed for this pair. Journaled so orchestration
                    // (flintctl) can wait for supervision instead of hoping.
                    journal_event(
                        &cfg.id,
                        flint_journal::EventKind::Supervised,
                        &self.label,
                        None,
                        "pair observed converged; auto-failover armed",
                    );
                }
                self.last_converged = Instant::now();
                self.converged_ever = true;
                // Remember WHO held it. The degraded-window gate needs this
                // later, when this same node may be alive but replica-less and
                // therefore unable to prove the same thing about itself (#171).
                self.last_insync = Some((legit_addr.clone(), legit.epoch));
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
                            None,
                        );
                        self.reap(i, c);
                        self.slot_miss[i] = 0;
                        self.slot_cooldown[i] = Instant::now() + Duration::from_secs(20);
                    }
                }
            }
            return;
        }

        // No reachable master. A master that still ACCEPTS connections but whose
        // app is unresponsive is alive-but-slow, not dead: promoting away from it
        // just flaps (and on a uniformly-loaded fleet the target is equally slow).
        // Require far more confirmation for that case than for a dead process.
        self.no_master_streak += 1;
        let slow = states.iter().any(|n| !n.reachable && n.socket_alive);
        if slow {
            self.slow_since.get_or_insert_with(Instant::now);
        } else {
            self.slow_since = None;
        }
        let slow_elapsed = self.slow_since.map(|t| t.elapsed());
        if !should_promote(
            slow,
            self.no_master_streak,
            cfg.confirm,
            slow_elapsed,
            cfg.slow_promote,
        ) {
            return;
        }
        if !self.outage_announced {
            self.outage_announced = true;
            journal_event(
                &cfg.id,
                flint_journal::EventKind::Detected,
                &self.label,
                None,
                if slow {
                    "master unresponsive but still listening — slow past patience window"
                } else {
                    "master unreachable, confirmed across required ticks"
                },
            );
        }

        // Degraded-window gate: only auto-promote a recently-converged pair —
        // UNLESS the pair is holding its own self-fenced master with a
        // caught-up replica (#168). That pair is provably in sync, and
        // refusing it is precisely what turns a few seconds of controller
        // unavailability into a permanent, fleet-wide write outage: no master
        // means no convergence, and no convergence means this gate refuses
        // forever.
        if !self.converged_ever || self.last_converged.elapsed() > cfg.max_stale {
            match insync_lineage_holder(&states) {
                Some(h) => {
                    eprintln!(
                        "[{}][{}] no master-claimer, but {} holds the top epoch with a caught-up replica — self-fenced, recovering it (#168)",
                        cfg.id, self.label, h.addr
                    );
                    journal_event(
                        &cfg.id,
                        flint_journal::EventKind::Detected,
                        &self.label,
                        None,
                        "self-fenced master still serving a caught-up replica: recovering, not a degraded window",
                    );
                }
                // Nobody can prove it RIGHT NOW. Fall back to what this
                // controller already watched happen (#171): a replica-less
                // lineage holder cannot satisfy the live predicate however
                // intact it is, and refusing it turns a pair that is sitting
                // there whole into a permanent write outage.
                None => match remembered_lineage_holder(&states, self.last_insync.as_ref()) {
                    Some(h) => {
                        eprintln!(
                            "[{}][{}] no master-claimer and no PROVABLE in-sync node, but {} is the member last observed holding the lineage and still holds the top epoch — recovering it (#171)",
                            cfg.id, self.label, h.addr
                        );
                        journal_event(
                            &cfg.id,
                            flint_journal::EventKind::Detected,
                            &self.label,
                            None,
                            "lineage holder alive but replica-less: recovering on the last observation, not a degraded window",
                        );
                    }
                    None => {
                        let (id, label, ms) = (cfg.id.clone(), self.label.clone(), cfg.max_stale);
                        self.page(format_args!(
                            "[{id}][{label}] no master and pair not converged within {ms:?}, and no member was ever observed holding the lineage — REFUSING (degraded window; needs spare/S3). PAGE."
                        ));
                        self.no_master_streak = 0;
                        return;
                    }
                },
            }
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
        // THE FENCING RECORD, BEFORE THE PROMOTION (ADR-0018). CPFENCE
        // durably names the survivor as its pair's master-of-record; the old
        // master's next CPLEASE renewal trips over it (-SUPERSEDED) and
        // fences within a renewal interval, even if this controller never
        // reaches it. A promotion whose record cannot commit does NOT happen:
        // promoting without it would reopen the split-brain window the lease
        // exists to close. This inverts ADR-0004's "failover must not depend
        // on the CP" — deliberately, and argued in the ADR: the CP has a
        // quorum under it; this process, as five scale runs proved, does not.
        if let Some(cp) = &cfg.commit_cp {
            let mut fenced = false;
            for _ in 0..3 {
                // An HA control plane serves lease writes only at its
                // LEADER; a follower answers `-LEADER <addr>`. Follow one
                // hop per attempt, fresh each time (leadership moves).
                let mut reply = call(cp, &[b"CPFENCE", survivor.addr.as_bytes()]);
                if let Ok(Value::Error(e)) = &reply
                    && let Some(leader) = e.strip_prefix("LEADER ")
                {
                    reply = call(leader.trim(), &[b"CPFENCE", survivor.addr.as_bytes()]);
                }
                if matches!(reply, Ok(Value::Simple(_))) {
                    fenced = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            if !fenced {
                let (id, label) = (cfg.id.clone(), self.label.clone());
                self.page(format_args!(
                    "[{id}][{label}] cannot commit the fencing record (CPFENCE {} at {cp}) — REFUSING to promote without it. PAGE.",
                    survivor.addr
                ));
                self.no_master_streak = 0;
                return;
            }
        }
        match call(
            &survivor.addr,
            &[b"FLINTPROMOTE", b"0", next.to_string().as_bytes()],
        ) {
            Ok(Value::Simple(s)) => {
                eprintln!(
                    "[{}][{}] PROMOTED {} at (0,{next}): {s}",
                    cfg.id, self.label, survivor.addr
                );
                journal_event(
                    &cfg.id,
                    flint_journal::EventKind::PromoteIssued,
                    &survivor.addr,
                    Some(format!("(0,{next})")),
                    "epoch-fenced promotion of the freshest survivor",
                );
                // The proxies were already told: CPFENCE (above) bumps the
                // CP's version and wakes every proxy parked in CPWATCH,
                // subsuming the CPPROMOTED hint this arm used to send after
                // the promote (#91 → ADR-0018). The push now precedes the
                // promotion instead of trailing it, and it is durable.
                self.converged_ever = false; // new master has no replica yet
                // ...but it IS the lineage holder as of right now, and it will
                // not be able to demonstrate that until a replica re-attaches.
                // Without this the pair is unrecoverable for exactly that
                // window if the fresh master then fences (#171).
                self.last_insync = Some((survivor.addr.clone(), next));
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

/// The build stamp this controller registers with the CP (ADR-0014 D1).
/// One definition for every Flint binary; see the flint-build crate.
fn build_version() -> String {
    flint_build::version(env!("CARGO_PKG_VERSION"))
}

fn main() {
    // The controller has NO LISTENER and must not gain one — a supervisor
    // that accepts connections is a supervisor that can be asked to do
    // things, and its safety today rests on being unreachable. So this flag
    // is the only way to interrogate the file directly, and the running
    // process reports itself by REGISTERING with the CP instead (below).
    if std::env::args().any(|a| a == "--build-version") {
        println!("{}", build_version());
        return;
    }
    // Internal-mesh mutual TLS toward the nodes (the controller is a client
    // everywhere — it has no listener). Same both-or-none-plus-ca gating as
    // the other components.
    match (
        arg("--internal-ca"),
        arg("--internal-cert"),
        arg("--internal-key"),
    ) {
        (Some(ca), Some(cert), Some(key)) => {
            let _ = INTERNAL_CLIENT.set(Some(
                flint_tls::ReloadableClientConfig::watch(&ca, &cert, &key)
                    .expect("build internal TLS client config"),
            ));
        }
        (None, None, None) => {
            let _ = INTERNAL_CLIENT.set(None);
        }
        _ => panic!("--internal-ca, --internal-cert, --internal-key must be given together"),
    }
    let _ = JOURNAL_TARGET.set(arg("--journal"));
    // Resolve the rebalance policy up front so an unknown name is a startup
    // error, not a silent fallback to the wrong metric.
    let policy_name = arg("--balance-policy").unwrap_or_else(|| "size".into());
    let balance_policy = planner::policy_by_name(&policy_name).unwrap_or_else(|| {
        eprintln!("unknown --balance-policy '{policy_name}' (open stack ships: size)");
        std::process::exit(2);
    });

    let cfg = Config {
        // 100ms, halved from 200 on 2026-08-02. Detection is poll x confirm
        // and it dominates the client-visible failover stall — measured at
        // ~70% of it. On 7 EC2 hosts through the proxy edge, 30 kills per
        // setting: 200:3 gave p50 644ms / worst 753ms over 15 promotions,
        // 100:2 gave p50 317ms / worst 322ms over 18. This lands between
        // them at ~430ms (loopback 100:3 measured 433 and 467ms p50).
        //
        // CONFIRM STAYS AT 3, deliberately. confirm is the tolerance for a
        // transient miss — a dropped probe, a GC pause, a busy host — and
        // 100:2 was never tested where that tolerance can fail: every
        // "zero spurious promotions" number came from an idle loopback
        // soak. Halving the interval keeps all three misses required; it
        // only shortens the window they must occur in.
        poll: Duration::from_millis(arg_or("--poll-ms", 100)),
        confirm: arg_or("--confirm", 3),
        // A dead master (connection refused) still promotes on poll*confirm.
        // This is only the tolerance for a master that is LISTENING but slow —
        // 4s rides out compaction bursts and CPU starvation without flapping,
        // while still failing over a genuinely hung-but-listening process.
        slow_promote: Duration::from_millis(arg_or("--slow-promote-ms", 4_000)),
        max_stale: Duration::from_millis(arg_or("--max-stale-ms", 5_000)),
        // Lease TTL handed to each master per renewal. Generous vs the poll
        // interval so transient controller unavailability never trips a
        // healthy master; a master self-fences only after this long with NO
        // controller (of any in the HA set) reaching it. 0 disables leases.

        // How stale the decision loop's published view may get before the
        // renewer stops renewing. Generous on purpose: it must comfortably
        // cover a SLOW sweep (the thing decoupling exists to survive) while
        // still bounding a decision loop that has died outright, so the lease
        // keeps its partition guarantee. 30s is ~10x the default lease and
        // ~2x the worst observed sweep on the 4-pair scale fleet.
        // Passed to every managed node so a promoted-then-widowed master
        // sheds writes (Redis min-replicas-to-write). 0 = disabled.
        min_replicas: arg("--min-replicas-to-write")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        rebalance_deadband: arg("--rebalance-deadband")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0),
        rebalance_execute: std::env::args().any(|a| a == "--rebalance-execute"),
        max_slots_per_cycle: arg_or("--max-slots-per-cycle", 4),
        commit_cp: arg("--commit-cp"),
        recover_nodes: arg("--recover-nodes")
            .map(|s| s.split(',').map(String::from).collect())
            .unwrap_or_default(),
        snapshot_root: arg("--snapshot-root"),
        snapshot_interval: Duration::from_millis(arg_or("--snapshot-interval-ms", 30_000)),
        balance_policy,
        bin: server_bin(),
        id: arg("--id").unwrap_or_else(|| "ctl".into()),
    };

    let mut pairs = build_pairs();
    assert!(
        !pairs.is_empty() || !cfg.recover_nodes.is_empty(),
        "need --nodes/--pairs, --manage-slots/--manage-pairs, or --recover-nodes"
    );

    eprintln!(
        "[{}] flint-controller: {} pair(s) recover-nodes={} poll={:?} confirm={} balance-policy={}",
        cfg.id,
        pairs.len(),
        cfg.recover_nodes.len(),
        cfg.poll,
        cfg.confirm,
        cfg.balance_policy.name(),
    );

    // ADR-0014 D1: say what we are, since nobody can ask. Once at startup
    // and every REGISTER_EVERY thereafter — the CP's staleness window is
    // three of these, so one missed report is a hiccup and three is a
    // process that is gone.
    //
    // Registering repeatedly rather than once is what makes an ORPHANED
    // controller visible: the recorded failure is one surviving two upgrade
    // cycles unnoticed, and a single startup announcement would have gone
    // just as unnoticed the moment the CP restarted.
    const REGISTER_EVERY: Duration = Duration::from_secs(30);
    let identity = format!(
        "{}:{}",
        std::process::Command::new("hostname")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| "unknown".into()),
        std::process::id()
    );
    let register = |cfg: &Config| {
        // Best effort, exactly like CPPROMOTED: supervision must never
        // depend on the CP being up, and a controller that cannot announce
        // itself is still a controller that must keep supervising.
        if let Some(cp) = &cfg.commit_cp {
            let _ = call(
                cp,
                &[
                    b"CPCONTROLLER",
                    identity.as_bytes(),
                    build_version().as_bytes(),
                ],
            );
        }
    };
    register(&cfg);

    let mut last_rebalance = Instant::now();
    let mut last_recover = Instant::now();
    let mut last_register = Instant::now();
    loop {
        std::thread::sleep(cfg.poll);
        if last_register.elapsed() > REGISTER_EVERY {
            last_register = Instant::now();
            register(&cfg);
        }
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

        // Rebalance: every 5s, observe each pair's fill and derive the
        // deterministic-hysteresis plan. Dry-run logs it; with
        // --rebalance-execute, the FIRST move of the plan is executed this
        // cycle (bounded slots, serial cutovers) and the next cycle re-plans
        // from fresh fills — convergence by repeated small steps, with the
        // deadband stopping the loop at balance.
        if cfg.rebalance_deadband > 0.0 && last_rebalance.elapsed() > Duration::from_secs(5) {
            last_rebalance = Instant::now();
            let observed: Vec<(planner::PairLoad, String)> = pairs
                .iter()
                .filter_map(|p| p.observe_fill(&*cfg.balance_policy))
                .collect();
            if observed.len() >= 2 {
                let loads: Vec<planner::PairLoad> =
                    observed.iter().map(|(l, _)| l.clone()).collect();
                let masters: std::collections::HashMap<String, String> = observed
                    .iter()
                    .map(|(l, m)| (l.label.clone(), m.clone()))
                    .collect();
                let fills: Vec<(String, u64)> =
                    loads.iter().map(|l| (l.label.clone(), l.fill)).collect();
                let plan = planner::plan_moves(&loads, cfg.rebalance_deadband);
                match plan.first() {
                    None => eprintln!(
                        "[{}] rebalance: balanced within deadband {:.2} — fills={fills:?}",
                        cfg.id, cfg.rebalance_deadband
                    ),
                    Some(_) if !cfg.rebalance_execute => {
                        for m in &plan {
                            eprintln!(
                                "[{}] rebalance PLAN (dry-run): move ~{} load from {} to {} — fills={fills:?}",
                                cfg.id, m.approx, m.from, m.to
                            );
                        }
                    }
                    Some(m) => execute_move(&cfg, m, &masters, &fills),
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
    let mut stream = internal_connect(addr)?;
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

/// True if `addr` reports any in-flight migration record. Errors count as
/// busy — when in doubt, don't start another move.
fn has_inflight_migration(addr: &str) -> bool {
    match call(addr, &[b"FLINTMIGRATIONS"]) {
        Ok(Value::Array(Some(rows))) => !rows.is_empty(),
        _ => true,
    }
}

/// Per-(namespace, slot) key counts from a master (FLINTSLOTSTATS:
/// "slot count ns" bulks) — the migration unit is (ns, slot).
fn slot_stats(addr: &str) -> Vec<((String, u16), u64)> {
    let Ok(Value::Array(Some(rows))) = call(addr, &[b"FLINTSLOTSTATS"]) else {
        return Vec::new();
    };
    rows.into_iter()
        .filter_map(|r| {
            let Value::Bulk(Some(b)) = r else {
                return None;
            };
            let line = String::from_utf8_lossy(&b).into_owned();
            let mut parts = line.split(' ');
            let slot: u16 = parts.next()?.parse().ok()?;
            let n: u64 = parts.next()?.parse().ok()?;
            let ns = parts.next()?.to_string();
            Some(((ns, slot), n))
        })
        .collect()
}

/// Execute ONE planned pair-level move: pick the donor slots (deterministic,
/// bounded by max_slots_per_cycle) and drive serial FLINTMIGRATEIN cutovers
/// on the destination master. Gates: skip the cycle if either side already
/// has an in-flight migration (recovery or a concurrent controller owns it —
/// the fenced records make duplicates collapse, but not-starting is cheaper
/// than being fenced). Serial one-move-per-cycle IS the pacing: the next
/// cycle re-observes fills and re-plans, and the deadband ends the loop.
fn execute_move(
    cfg: &Config,
    m: &planner::Move,
    masters: &std::collections::HashMap<String, String>,
    fills: &[(String, u64)],
) {
    let (Some(src), Some(dst)) = (masters.get(&m.from), masters.get(&m.to)) else {
        return;
    };
    if has_inflight_migration(src) || has_inflight_migration(dst) {
        eprintln!(
            "[{}] rebalance: move {}->{} deferred — a migration is already in flight",
            cfg.id, m.from, m.to
        );
        return;
    }
    let stats = slot_stats(src);
    let units = planner::select_units(&stats, m.approx, cfg.max_slots_per_cycle);
    if units.is_empty() {
        return;
    }
    eprintln!(
        "[{}] rebalance EXECUTE: ~{} load {}({src}) -> {}({dst}), units {units:?} — fills={fills:?}",
        cfg.id, m.approx, m.from, m.to
    );
    for (ns, slot) in units {
        match call_slow(
            dst,
            &[
                b"FLINTMIGRATEIN",
                src.as_bytes(),
                slot.to_string().as_bytes(),
                dst.as_bytes(),
                ns.as_bytes(),
            ],
            Duration::from_secs(120),
        ) {
            Ok(Value::Simple(s)) => {
                eprintln!(
                    "[{}] rebalance: {ns}/{slot} {}->{}: {s}",
                    cfg.id, m.from, m.to
                );
                // Option B: the cutover is durable on the nodes; now commit
                // the ownership truth to the CP so every proxy (including
                // cold-started ones) routes it from the snapshot, not from
                // -MOVED discovery. Best-effort: on failure the -MOVED
                // bridge still routes correctly and the next cycle retries
                // nothing (the CP row is idempotent to re-set).
                // Resolution note: the CP matches `dst` against its
                // REGISTERED pair members, so pairs must be registered by
                // the nodes' advertise addresses (flintctl does). A form
                // mismatch (hostname vs IP) fails the commit — logged
                // below; the -MOVED bridge still routes until fixed.
                if let Some(cp) = &cfg.commit_cp {
                    let r = call(
                        cp,
                        &[
                            b"CPSETSLOT",
                            ns.as_bytes(),
                            slot.to_string().as_bytes(),
                            dst.as_bytes(),
                        ],
                    );
                    eprintln!(
                        "[{}] rebalance: CPSETSLOT {ns}/{slot} -> {dst}: {r:?}",
                        cfg.id
                    );
                }
            }
            other => {
                // Stop the cycle; recovery reconciles any half-done state and
                // the next cycle re-plans from observed reality.
                eprintln!(
                    "[{}] rebalance: {ns}/{slot} move failed ({other:?}) — yielding to recovery",
                    cfg.id
                );
                return;
            }
        }
    }
}

/// One in-flight migration record observed on a node.
struct InFlight {
    node: String,
    slot: u16,
    peer: String,
    ns: String,
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
            let [slot, phase, peer, ns] = parts.as_slice() else {
                continue;
            };
            let Ok(slot) = slot.parse::<u16>() else {
                continue;
            };
            let rec = InFlight {
                node: node.clone(),
                slot,
                peer: peer.to_string(),
                ns: ns.to_string(),
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
                    rec.ns.as_bytes(),
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
            let _ = call(
                dest,
                &[
                    b"FLINTSLOTABORT",
                    rec.slot.to_string().as_bytes(),
                    rec.ns.as_bytes(),
                ],
            );
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
                    rec.ns.as_bytes(),
                ],
            );
        } else {
            eprintln!(
                "[{}] recovery: dest {dest} gone — unfreezing slot {} on {src}",
                cfg.id, rec.slot
            );
            let _ = call(
                src,
                &[
                    b"FLINTSLOTABORT",
                    rec.slot.to_string().as_bytes(),
                    rec.ns.as_bytes(),
                ],
            );
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A node as `observe` would report it.
    fn node(addr: &str, epoch: u32, live_replicas: u32, seq_lag: Option<u64>) -> Node {
        Node {
            addr: addr.into(),
            reachable: true,
            socket_alive: true,
            // Every node in this state reports role:replica — that IS the bug
            // signature (#168): FLINTINFO renders role from the read-only flag,
            // so a self-fenced master calls itself a replica.
            role: "replica".into(),
            epoch,
            live_replicas,
            seq_lag,
        }
    }

    #[test]
    fn self_fenced_master_with_caught_up_replica_is_recoverable() {
        // The exact fleet state a controller stall leaves behind: nobody claims
        // master, but one node is still streaming to a caught-up replica.
        let states = vec![node("a:1", 3, 1, Some(0)), node("b:2", 3, 0, None)];
        assert_eq!(
            insync_lineage_holder(&states).map(|n| n.addr.as_str()),
            Some("a:1")
        );
    }

    /// #171: the lineage holder is alive but replica-less, so it reports
    /// live_replicas 0 / seq_lag none and cannot prove anything about itself.
    /// The controller watched it hold the lineage; that observation is what
    /// makes promoting it safe.
    #[test]
    fn replica_less_lineage_holder_is_recovered_from_memory() {
        let states = vec![node("a:1", 3, 0, None), node("b:2", 2, 0, None)];
        // The live predicate cannot help here — that is the bug.
        assert!(insync_lineage_holder(&states).is_none());
        let remembered = ("a:1".to_string(), 3u32);
        assert_eq!(
            remembered_lineage_holder(&states, Some(&remembered)).map(|n| n.addr.as_str()),
            Some("a:1")
        );
    }

    /// The genuine degraded window must STILL refuse. A survivor this
    /// controller never observed holding the lineage may be missing writes that
    /// only exist on a node it cannot reach, and guessing there is the one
    /// thing the gate exists to prevent.
    #[test]
    fn never_observed_survivor_still_refuses() {
        let states = vec![node("a:1", 3, 0, None), node("b:2", 2, 0, None)];
        assert!(remembered_lineage_holder(&states, None).is_none());
        let someone_else = ("c:3".to_string(), 3u32);
        assert!(remembered_lineage_holder(&states, Some(&someone_else)).is_none());
    }

    /// Being remembered is not enough: a remembered member that is NOT at the
    /// top reachable epoch has been overtaken, and promoting it would discard
    /// the newer lineage sitting right there.
    #[test]
    fn remembered_but_overtaken_does_not_qualify() {
        let states = vec![node("a:1", 2, 0, None), node("b:2", 5, 0, None)];
        let remembered = ("a:1".to_string(), 2u32);
        assert!(remembered_lineage_holder(&states, Some(&remembered)).is_none());
    }

    /// An unreachable remembered member cannot be promoted, however perfect its
    /// record — this is the host-loss case, and it must page.
    #[test]
    fn remembered_but_unreachable_does_not_qualify() {
        let mut states = vec![node("a:1", 3, 0, None), node("b:2", 3, 0, None)];
        states[0].reachable = false;
        let remembered = ("a:1".to_string(), 3u32);
        assert!(remembered_lineage_holder(&states, Some(&remembered)).is_none());
    }

    #[test]
    fn genuinely_degraded_pair_is_not_recoverable() {
        // No live replica anywhere: this IS a degraded window (the survivor
        // cannot prove it holds the data), so the gate must still refuse and
        // page rather than gamble.
        let states = vec![node("a:1", 3, 0, None), node("b:2", 3, 0, None)];
        assert!(insync_lineage_holder(&states).is_none());
    }

    #[test]
    fn lagging_replica_does_not_qualify() {
        // Attached but NOT caught up: "in sync" must be observed, not assumed.
        let states = vec![node("a:1", 3, 1, Some(42))];
        assert!(insync_lineage_holder(&states).is_none());
    }

    #[test]
    fn superseded_ex_master_never_qualifies() {
        // A stale ex-master at a LOWER epoch with a lingering follower must not
        // be resurrected — a successor already holds the lineage.
        let states = vec![node("old:1", 2, 1, Some(0)), node("new:2", 5, 0, None)];
        assert!(insync_lineage_holder(&states).is_none());
    }

    #[test]
    fn unreachable_node_never_qualifies() {
        let mut down = node("a:1", 9, 1, Some(0));
        down.reachable = false;
        let states = vec![down, node("b:2", 3, 0, None)];
        assert!(insync_lineage_holder(&states).is_none());
    }

    #[test]
    fn dead_master_promotes_on_confirm_ticks() {
        // A refused connection (dead) escalates on the streak alone — fast,
        // RTO-critical, the killed-master path every chaos drill exercises.
        assert!(!should_promote(
            false,
            2,
            3,
            None,
            Duration::from_millis(4000)
        ));
        assert!(should_promote(
            false,
            3,
            3,
            None,
            Duration::from_millis(4000)
        ));
    }

    #[test]
    fn slow_master_waits_for_wall_clock_not_ticks() {
        // A listening-but-slow master ignores the streak entirely: it escalates
        // only once it has been slow for slow_promote of WALL-CLOCK, however many
        // (slow, timeout-bound) ticks that took. This is the whole fix — a
        // starved master is held, not flapped, and the knob means real time.
        let sp = Duration::from_millis(4000);
        assert!(!should_promote(
            true,
            999,
            3,
            Some(Duration::from_millis(3999)),
            sp
        ));
        assert!(should_promote(
            true,
            1,
            3,
            Some(Duration::from_millis(4000)),
            sp
        ));
    }

    #[test]
    fn slow_master_with_no_elapsed_never_promotes() {
        // Just entered the slow state (slow_since not yet set): never promote on
        // the same tick.
        assert!(!should_promote(
            true,
            100,
            3,
            None,
            Duration::from_millis(4000)
        ));
    }

    #[test]
    fn socket_open_true_for_a_listener_false_for_a_closed_port() {
        // The slow-vs-dead primitive: a bound listener answers "alive" at the
        // kernel level even without accept()ing (a SIGSTOPped master is exactly
        // this); a port with nothing on it is refused.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let addr = listener.local_addr().expect("local addr").to_string();
        assert!(
            socket_open(&addr),
            "a bound listener must read as socket-alive"
        );

        // Bind then drop to get an address nothing is listening on.
        let closed = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
            l.local_addr().expect("local addr").to_string()
        };
        assert!(!socket_open(&closed), "a closed port must read as dead");
    }
}
