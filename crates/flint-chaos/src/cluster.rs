// SPDX-License-Identifier: Elastic-2.0
//! Shared chaos infrastructure: a RESP client, a process fleet that cleans
//! up on panic, and a `Cluster` that plays the meta trio's role manually —
//! detecting kills, promoting survivors via epoch-fenced FLINTPROMOTE, and
//! attaching fresh full-sync-seeded replacements.
//!
//! This is the ONE implementation of the failover mechanics; every chaos
//! workload drives it, so the subtle kill/promote/replace logic can never
//! drift between tests.

use std::io::{Read, Write};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use flint_resp::{Decoded, Value, decode, encode};

/// A fleet this harness did NOT spawn: real seats, on real machines, managed
/// by flintctl.
///
/// The harness cannot start or kill those processes itself — it has no idea
/// which machine any of them is on, and reimplementing that knowledge here
/// would be a second copy of it to drift. So faults go out through
/// `flintctl kill-node` / `restart-node`, which already route to the owning
/// host, and this type only decides WHICH seat and WHEN.
///
/// Everything downstream — the ledger, the value checksums, the acked-write
/// accounting — is untouched. That is the whole point: the oracle that has
/// been catching corruption on one host is exactly the oracle we want to
/// point at seven.
pub struct Attached {
    /// `flintctl -f <inventory>`, the control surface for every fault.
    inventory: String,
    ctl: String,
    /// The pair under test, as declared. Roles float between these two.
    members: Vec<String>,
    pub tls: Option<std::sync::Arc<flint_tls::ClientConfig>>,
    master_kills: std::cell::Cell<u32>,
    replica_kills: std::cell::Cell<u32>,
}

impl Attached {
    /// Read ONLY the pair lines and the cert dir from the inventory.
    ///
    /// Deliberately not a second full inventory parser: flintctl owns that,
    /// and duplicating it here would be one more thing to drift. `pair` and
    /// `statedir` are the two keys this harness needs, and both have been
    /// stable since the format existed.
    /// How many pairs the inventory declares — so the harness can open one
    /// Attached per pair instead of silently testing only the first. The
    /// 7-host runs advertised "16 cross-host kills on 7 hosts" while every
    /// kill landed on pair 0 and pair 1 was scenery (#118 item 2).
    pub fn pair_count(inventory: &str) -> usize {
        let raw = std::fs::read_to_string(inventory)
            .unwrap_or_else(|e| panic!("read inventory {inventory}: {e}"));
        raw.lines()
            .filter_map(|l| l.split('#').next())
            .filter(|l| l.trim_start().starts_with("pair "))
            .count()
    }

    pub fn open(inventory: &str, pair_index: usize) -> Self {
        let raw = std::fs::read_to_string(inventory)
            .unwrap_or_else(|e| panic!("read inventory {inventory}: {e}"));
        let mut pairs: Vec<Vec<String>> = Vec::new();
        let mut statedir = String::new();
        let mut tls_on = false;
        for line in raw.lines() {
            let line = line.split('#').next().unwrap_or("").trim();
            let Some((k, v)) = line.split_once(' ') else {
                continue;
            };
            match k {
                "pair" => pairs.push(v.trim().split(',').map(String::from).collect()),
                "statedir" => statedir = v.trim().to_string(),
                "tls" => tls_on = v.trim() == "on",
                _ => {}
            }
        }
        assert!(
            pair_index < pairs.len(),
            "inventory declares {} pair(s); --pair {pair_index} is out of range",
            pairs.len()
        );
        let tls = if tls_on {
            let d = format!("{statedir}/certs");
            Some(
                flint_tls::client_config(
                    &format!("{d}/ca.crt"),
                    &format!("{d}/int.crt"),
                    &format!("{d}/int.key"),
                )
                .expect("mesh certs (run where the fleet's certs live)"),
            )
        } else {
            None
        };
        Self {
            inventory: inventory.to_string(),
            ctl: std::env::var("FLINTCTL_BIN").unwrap_or_else(|_| {
                format!(
                    "{}/../../target/release/flintctl",
                    env!("CARGO_MANIFEST_DIR")
                )
            }),
            members: pairs[pair_index].clone(),
            tls,
            master_kills: std::cell::Cell::new(0),
            replica_kills: std::cell::Cell::new(0),
        }
    }

    fn ctl(&self, args: &[&str]) -> Result<String, String> {
        let out = Command::new(&self.ctl)
            .args(["-f", &self.inventory])
            .args(args)
            .output()
            .map_err(|e| format!("flintctl {args:?}: {e}"))?;
        if out.status.success() {
            Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
        } else {
            Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
        }
    }

    pub fn tls(&self) -> &Option<std::sync::Arc<flint_tls::ClientConfig>> {
        &self.tls
    }

    /// Everything an operator would ask for when a promotion does not happen,
    /// gathered at the moment it does not happen.
    ///
    /// THE COST OF NOT HAVING THIS. The 5 TB scale run (#171) died on
    /// "controller did not promote within 30s" at iteration 11 of 12, and the
    /// panic carried nothing but that sentence. The fleet tore down seconds
    /// later — teardown is unconditional, and correctly so — taking the
    /// controller's log with it. An intermittent failure that destroys its own
    /// evidence costs one whole multi-host run per attempt, and the local
    /// repro (tools/failover_churn_drill.sh) does NOT reproduce it, so there
    /// is no cheaper way to see the state that matters.
    ///
    /// Deliberately best-effort and infallible: this runs on a path that is
    /// already failing, and a diagnostic that can itself fail would replace
    /// the real error with its own.
    fn failure_context(&self, dead: &str, survivor: &str) -> String {
        let mut s = String::new();
        s.push_str(&format!(
            "\n  pair members: {:?}\n  killed: {dead}\n  awaited promotion of: {survivor}\n",
            self.members
        ));
        match self.ctl(&["status"]) {
            Ok(o) => s.push_str(&format!("  --- flintctl status ---\n{o}\n")),
            Err(e) => s.push_str(&format!("  --- flintctl status FAILED: {e}\n")),
        }
        // Both seats' raw view. The survivor's own numbers are the crux: a
        // lone survivor reports live_replicas 0 and seq_lag none, and whether
        // the controller can still promote from that is the open question.
        for m in &self.members {
            let info = self.info(m);
            if info.is_empty() {
                s.push_str(&format!("  {m}: UNREACHABLE\n"));
                continue;
            }
            let f = |k: &str| info.get(k).cloned().unwrap_or_else(|| "-".into());
            s.push_str(&format!(
                "  {m}: role={} epoch={} latest_seq={} acked_seq={} seq_lag={} live_replicas={} write_stopped={}\n",
                f("role"), f("role_epoch"), f("latest_seq"), f("acked_seq"),
                f("seq_lag"), f("live_replicas"), f("write_stopped"),
            ));
        }
        s
    }

    fn role_of(&self, addr: &str) -> Option<String> {
        let mut c = Client::connect_addr(addr, &self.tls).ok()?;
        let Ok(Value::Bulk(Some(raw))) = c.call(&[b"FLINTINFO"]) else {
            return None;
        };
        String::from_utf8_lossy(&raw)
            .lines()
            .find_map(|l| l.strip_prefix("role:").map(|v| v.trim().to_string()))
    }

    /// Whoever is master RIGHT NOW — asked, never assumed. After a failover
    /// the roles have swapped, and a harness that remembered the old answer
    /// would write to a node that now rejects writes.
    pub fn master(&self) -> String {
        for _ in 0..100 {
            for m in &self.members {
                if self.role_of(m).as_deref() == Some("master") {
                    return m.clone();
                }
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        panic!("pair {:?} has no reachable master", self.members);
    }

    pub fn replica(&self) -> Option<String> {
        let master = self.master();
        self.members.iter().find(|m| **m != master).cloned()
    }

    /// Steady state: a live replica, fully converged in sequence. The same
    /// gate the local harness applies, read from the master's own view.
    pub fn wait_healthy(&self, budget: Duration) -> bool {
        self.wait_replica(budget, true)
    }

    /// A live replica exists — but say NOTHING about how far behind it is.
    ///
    /// This is the precondition for a master kill that actually tests the
    /// RPO claim. Requiring seq_lag==0 as well (what wait_healthy does)
    /// guarantees the dying master has no unreplicated suffix, so no acked
    /// write CAN be lost and the oracle's "zero loss" result is a statement
    /// about the harness rather than the engine. A live replica is still
    /// required: with none, loss is unbounded by design (the widowed-master
    /// case), which is a different scenario and not this one.
    pub fn wait_replica_live(&self, budget: Duration) -> bool {
        self.wait_replica(budget, false)
    }

    fn wait_replica(&self, budget: Duration, need_converged: bool) -> bool {
        let start = Instant::now();
        while start.elapsed() < budget {
            let m = self.master();
            let info = self.info(&m);
            let live = info.get("live_replicas").is_some_and(|v| v.trim() != "0");
            let converged = !need_converged || self.replica_holds_the_lineage(&m, &info);
            if live && converged {
                return true;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        false
    }

    /// Converged means the REPLICA says it took the data — not that the
    /// master says it is caught up.
    ///
    /// Both halves of this used to come from the master alone:
    /// `live_replicas != 0` and `seq_lag == 0`. Soak run 22 showed why that
    /// is the wrong vantage. Iteration 3 killed pair 3's replica; iteration
    /// 8 killed the same pair's MASTER. The replacement replica was still
    /// re-seeding, but the master's numbers satisfied the gate, so the kill
    /// went ahead — and the controller then, correctly, refused to promote a
    /// survivor it had never observed in-sync:
    ///
    ///   [ctl][g3] no master and pair not converged within 5s, and no member
    ///   was ever observed holding the lineage — REFUSING (degraded window)
    ///
    /// The run died on a promotion timeout for a kill it should never have
    /// made, wearing the #171 signature of a bug that is actually fixed.
    /// Promoting that member would have frozen an incomplete dataset, so the
    /// controller was right and the harness was wrong.
    ///
    /// The controller's predicate is replica-side, so this one has to be too.
    /// A member reporting `acked_seq:none` holds nothing, however the master
    /// scores its lag, and `fullsync_active` on the master says a re-seed is
    /// in flight even when the ack counter has started moving.
    fn replica_holds_the_lineage(
        &self,
        master: &str,
        master_info: &std::collections::HashMap<String, String>,
    ) -> bool {
        let lag_zero = master_info
            .get("seq_lag")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .is_some_and(|l| l == 0);
        let no_reseed = master_info
            .get("fullsync_active")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .is_none_or(|n| n == 0);
        if !lag_zero || !no_reseed {
            return false;
        }
        let Some(replica) = self.members.iter().find(|m| m.as_str() != master) else {
            return false;
        };
        // Asked of the replica itself, and it must be `last_applied`.
        //
        // The first cut of this asked the replica for `acked_seq` and never
        // converged, failing iteration 1 of a healthy local pair. Measured on
        // a live pair, both sides answer:
        //
        //   master   role:master  latest_seq:5   last_applied:0  acked_seq:5     seq_lag:0
        //   replica  role:replica latest_seq:10  last_applied:5  acked_seq:none  seq_lag:none
        //
        // `acked_seq`, `seq_lag` and `live_replicas` are MASTER-side metrics —
        // a replica reports none/0 for them however healthy it is — so that
        // gate could not pass, ever. `latest_seq` is a RocksDB WAL position
        // private to each instance (10 vs 5 here), so it is not comparable
        // across nodes either. `last_applied` is the one field that means
        // "how far into the MASTER's stream this member has got", which is
        // the lineage question.
        self.info(replica)
            .get("last_applied")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .is_some_and(|applied| applied > 0)
    }

    fn info(&self, addr: &str) -> std::collections::HashMap<String, String> {
        let mut out = std::collections::HashMap::new();
        if let Ok(mut c) = Client::connect_addr(addr, &self.tls)
            && let Ok(Value::Bulk(Some(raw))) = c.call(&[b"FLINTINFO"])
        {
            for line in String::from_utf8_lossy(&raw).lines() {
                if let Some((k, v)) = line.split_once(':') {
                    out.insert(k.to_string(), v.trim().to_string());
                }
            }
        }
        out
    }

    /// Kill a seat and, if it was the master, wait for the fleet's own
    /// controller to promote the survivor. The harness never promotes here:
    /// on a real fleet that decision belongs to flint-controller, and taking
    /// it away would test the harness instead of the product.
    pub fn kill(&self, addr: &str) -> Result<(), String> {
        let was_master = self.role_of(addr).as_deref() == Some("master");
        if was_master {
            self.master_kills.set(self.master_kills.get() + 1);
        } else {
            self.replica_kills.set(self.replica_kills.get() + 1);
        }
        let survivor = self.members.iter().find(|m| *m != addr).cloned();
        self.ctl(&["kill-node", addr])?;
        if was_master && let Some(s) = survivor {
            let deadline = Instant::now() + Duration::from_secs(30);
            while Instant::now() < deadline {
                if self.role_of(&s).as_deref() == Some("master") {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            return Err(format!(
                "controller did not promote {s} within 30s (#171){}",
                self.failure_context(addr, &s)
            ));
        }
        Ok(())
    }

    /// Bring a killed seat back. Always wiped and re-seeded from the CURRENT
    /// master, which after a failover is its old peer.
    pub fn restart(&self, addr: &str) -> Result<(), String> {
        self.ctl(&["restart-node", addr]).map(|_| ())
    }

    pub fn members(&self) -> &[String] {
        &self.members
    }

    pub fn kills(&self) -> (u32, u32) {
        (self.master_kills.get(), self.replica_kills.get())
    }
}

/// The fleet under test, however it got there.
///
/// One workload and one oracle drive both: a harness-spawned local pair, and
/// a real flintctl-managed fleet whose seats may be on different machines.
/// The alternative — a second copy of the write loop for the attached case —
/// would mean the corruption checks that matter most ran against a DIFFERENT
/// implementation on the topology they matter most on.
pub enum Target {
    Local {
        cluster: Cluster,
        controller_driven: bool,
    },
    Attached(Attached),
}

impl Target {
    /// A client on whoever is master now.
    pub fn master_client(&self) -> std::io::Result<Client> {
        match self {
            Target::Local { cluster, .. } => Client::connect(cluster.master()),
            Target::Attached(a) => Client::connect_addr(&a.master(), a.tls()),
        }
    }

    pub fn wait_healthy(&self, budget: Duration) -> bool {
        match self {
            Target::Local { cluster, .. } => cluster.wait_healthy(budget),
            Target::Attached(a) => a.wait_healthy(budget),
        }
    }

    /// See Attached::wait_replica_live — a live replica, not a converged one.
    pub fn wait_replica_live(&self, budget: Duration) -> bool {
        match self {
            Target::Local { cluster, .. } => cluster.wait_replica_live(budget),
            Target::Attached(a) => a.wait_replica_live(budget),
        }
    }

    /// Candidate master endpoints for a client-side writer: the fixed pair
    /// addresses (attached), or the pair's CURRENT ports (local, where a
    /// harness-mode replacement replica gets a fresh port — the caller
    /// republishes after each kill).
    pub fn endpoints(&self) -> Vec<String> {
        match self {
            Target::Local { cluster, .. } => vec![
                format!("127.0.0.1:{}", cluster.master()),
                format!("127.0.0.1:{}", cluster.replica()),
            ],
            Target::Attached(a) => a.members().to_vec(),
        }
    }

    /// The mesh client config an attached fleet's writer needs; None locally.
    pub fn tls(&self) -> Option<std::sync::Arc<flint_tls::ClientConfig>> {
        match self {
            Target::Local { .. } => None,
            Target::Attached(a) => a.tls.clone(),
        }
    }

    /// Does the HARNESS issue the promotion, rather than a controller?
    ///
    /// Recovery wall-clock is only an RTO measurement when something the
    /// product ships does the promoting. In harness-driven local mode the
    /// harness sends FLINTPROMOTE itself, so the number would describe this
    /// binary, and reporting it as RTO would be a lie with a decimal point.
    pub fn promotion_is_harness(&self) -> bool {
        match self {
            Target::Local {
                controller_driven, ..
            } => !*controller_driven,
            Target::Attached(_) => false,
        }
    }

    /// Freeze this pair's replica so the master's acks outrun replication.
    /// Returns false when the target cannot be frozen (attached fleets: the
    /// seat may be on another host, and reaching it would need the ssh path).
    pub fn stall_replica(&self, on: bool) -> bool {
        match self {
            Target::Local { cluster, .. } => {
                signal_by_port(cluster.replica(), if on { "-STOP" } else { "-CONT" });
                true
            }
            Target::Attached(_) => false,
        }
    }

    /// Kill the master WITHOUT any convergence pre-wait, for workloads that
    /// keep writing through the kill. The caller is responsible for having
    /// parked the writer long enough for the controller to arm (see
    /// writer::Shared::pause); the standard kill_master's own wait can never
    /// observe seq_lag==0 under a live hammer and would burn its whole
    /// timeout before killing anyway.
    /// Returns the wall clock stamped right after the SIGKILL landed — the
    /// earliest instant at which no write can have been served by the dead
    /// master. Acks SENT at or after this are provably the new master's;
    /// anything earlier may be the old master's last words.
    pub fn kill_master_hot(&mut self) -> u64 {
        match self {
            Target::Local {
                cluster,
                controller_driven,
            } => {
                if *controller_driven {
                    cluster.kill_master_now_await_controller();
                } else {
                    cluster.kill_master();
                }
                cluster.last_kill_dead_ms
            }
            Target::Attached(a) => {
                let dead = a.master();
                a.kill(&dead).unwrap_or_else(|e| panic!("kill master: {e}"));
                let dead_ms = crate::writer::now_ms();
                a.restart(&dead)
                    .unwrap_or_else(|e| panic!("restart {dead}: {e}"));
                dead_ms
            }
        }
    }

    pub fn kill_master(&mut self) {
        match self {
            Target::Local {
                cluster,
                controller_driven,
            } => {
                if *controller_driven {
                    cluster.kill_master_await_controller();
                } else {
                    cluster.kill_master();
                }
            }
            Target::Attached(a) => {
                let dead = a.master();
                a.kill(&dead).unwrap_or_else(|e| panic!("kill master: {e}"));
                // Bring the dead seat back as a replica of the survivor that
                // was just promoted. Same fixed address, fresh data.
                a.restart(&dead)
                    .unwrap_or_else(|e| panic!("restart {dead}: {e}"));
            }
        }
    }

    pub fn kill_replica(&mut self) {
        match self {
            Target::Local {
                cluster,
                controller_driven,
            } => {
                if *controller_driven {
                    cluster.kill_replica_fixed();
                } else {
                    cluster.kill_replica();
                }
            }
            Target::Attached(a) => {
                let Some(r) = a.replica() else {
                    panic!("pair has no replica to kill");
                };
                a.kill(&r).unwrap_or_else(|e| panic!("kill replica: {e}"));
                a.restart(&r).unwrap_or_else(|e| panic!("restart {r}: {e}"));
            }
        }
    }

    pub fn kills(&self) -> (u32, u32) {
        match self {
            Target::Local { cluster, .. } => (cluster.master_kills, cluster.replica_kills),
            Target::Attached(a) => a.kills(),
        }
    }
}

/// A hash tag whose slot lands in `pair_idx`'s share of an EVEN split.
///
/// Going through the proxy edge means the PROXY decides which pair a key
/// lands on, which would dissolve the per-pair ledgers: a writer for pair 1
/// would scatter keys onto pair 0 and its verdict after a pair-1 kill would
/// be judging the wrong nodes. Pinning every key of writer i behind a tag
/// that routes to pair i keeps each ledger about exactly one pair, while the
/// traffic still crosses the real client path.
///
/// Assumes the bootstrap split (`i*16384/n ..= (i+1)*16384/n - 1`), which is
/// what `flintctl bootstrap` registers and what a throwaway chaos fleet has.
/// A fleet with migrated slots would need the CP's map instead; asserting
/// that here would mean a second slot-map parser, so the caller is expected
/// not to point edge mode at a rebalanced cluster.
pub fn pair_tag(pair_idx: usize, pair_count: usize) -> String {
    let lo = (pair_idx * 16384 / pair_count) as u16;
    let hi = ((pair_idx + 1) * 16384 / pair_count - 1) as u16;
    for n in 0..100_000u32 {
        let tag = format!("p{pair_idx}x{n}");
        let slot = flint_slot::crc16(tag.as_bytes()) % 16384;
        if slot >= lo && slot <= hi {
            return tag;
        }
    }
    panic!("no hash tag found for pair {pair_idx} of {pair_count} (slots {lo}..={hi})");
}

pub fn arg<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::args()
        .skip_while(|a| a != name)
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

pub struct Client {
    stream: flint_tls::Stream,
    buf: Vec<u8>,
}

impl Client {
    pub fn connect(port: u16) -> std::io::Result<Self> {
        Self::connect_addr(&format!("127.0.0.1:{port}"), &None)
    }

    /// Dial any address, optionally over the internal mesh.
    ///
    /// A harness-spawned fleet is plaintext loopback; a real one is mTLS on
    /// real addresses. Same client either way — only the config differs.
    pub fn connect_addr(
        addr: &str,
        tls: &Option<std::sync::Arc<flint_tls::ClientConfig>>,
    ) -> std::io::Result<Self> {
        let stream = flint_tls::connect(addr, tls)?;
        stream.set_read_timeout(Some(Duration::from_millis(1500)))?;
        Ok(Self {
            stream,
            buf: Vec::new(),
        })
    }

    pub fn call(&mut self, args: &[&[u8]]) -> std::io::Result<Value> {
        let frame = Value::Array(Some(
            args.iter().map(|a| Value::Bulk(Some(a.to_vec()))).collect(),
        ));
        let mut out = Vec::new();
        encode(&frame, &mut out);
        self.stream.write_all(&out)?;
        self.read_one()
    }

    /// Pipelined write: send every command, then read exactly one reply per
    /// command. Used to build large datasets fast (traversal stays serial).
    pub fn pipeline(&mut self, cmds: &[Vec<Vec<u8>>]) -> std::io::Result<()> {
        let mut out = Vec::new();
        for cmd in cmds {
            let frame = Value::Array(Some(
                cmd.iter().map(|a| Value::Bulk(Some(a.clone()))).collect(),
            ));
            encode(&frame, &mut out);
        }
        self.stream.write_all(&out)?;
        for _ in 0..cmds.len() {
            self.read_one()?;
        }
        Ok(())
    }

    fn read_one(&mut self) -> std::io::Result<Value> {
        let mut chunk = [0u8; 64 * 1024];
        loop {
            match decode(&self.buf) {
                Ok(Decoded::Complete(value, used)) => {
                    self.buf.drain(..used);
                    return Ok(value);
                }
                Ok(Decoded::NeedMore) => {
                    let n = self.stream.read(&mut chunk)?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "closed",
                        ));
                    }
                    self.buf.extend_from_slice(&chunk[..n]);
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
}

fn server_bin() -> String {
    std::env::var("FLINT_SERVER_BIN").unwrap_or_else(|_| {
        format!(
            "{}/../../target/release/flint-server",
            env!("CARGO_MANIFEST_DIR")
        )
    })
}

/// Kills tracked servers on drop — a panicking assertion can never leave
/// zombies holding ports (which once fooled a debug session into a false
/// cross-key report).
pub struct Fleet(std::rc::Rc<std::cell::RefCell<Vec<Child>>>);
impl Fleet {
    fn new() -> Self {
        Self(std::rc::Rc::new(std::cell::RefCell::new(Vec::new())))
    }
    fn track(&self, c: Child) {
        self.0.borrow_mut().push(c);
    }
}
impl Drop for Fleet {
    fn drop(&mut self) {
        for c in self.0.borrow_mut().iter_mut() {
            let _ = c.kill();
            let _ = c.wait();
        }
        // ...and take the data with them. Killing the processes but leaving
        // their RocksDB directories behind is what let 9,795 of them and 13 GB
        // accumulate under $TMPDIR on one laptop: enough to cross
        // flint-server's own disk headroom guard, at which point every node in
        // every drill answered `-QUOTA server is low on disk space` and three
        // unrelated drills went red at once. repl_drill reported
        // `errors: 50500, replies: 50500`, which reads exactly like a
        // catastrophic replication regression and is nothing of the sort.
        //
        // A harness that fails for a reason it invented, and reports it as a
        // product failure, costs more than the disk does.
        //
        // Runs on the panic path too — the oracle asserts, and an assert
        // unwinds — which matters because a FAILING run is the one that
        // leaked before. FLINT_CHAOS_KEEP=1 preserves the state for a
        // post-mortem, which is the only reason to want it.
        if std::env::var("FLINT_CHAOS_KEEP").is_ok_and(|v| v != "0") {
            eprintln!(
                "  (FLINT_CHAOS_KEEP: leaving {}* in {})",
                our_prefix(),
                std::env::temp_dir().display()
            );
            return;
        }
        // On the way out of a FAILING run, keep the node logs and drop only
        // the data. Those two differ in both size and worth: a pair's RocksDB
        // directories are hundreds of megabytes and reconstructible from the
        // seed, while the logs are a few kilobytes and are the only record of
        // what the server thought it was doing when the oracle fired. Keeping
        // the small useful thing and discarding the large reproducible one is
        // the whole trade.
        //
        // `thread::panicking()` is what makes the distinction possible at all
        // — it is true here precisely when an assert is unwinding through us,
        // which is exactly the run whose evidence is worth saving.
        let failing = std::thread::panicking();
        remove_run_files(&our_prefix(), !failing);
        if failing {
            eprintln!(
                "  node logs kept for the post-mortem: {}/{}*.log",
                std::env::temp_dir().display(),
                our_prefix()
            );
        }
    }
}

/// The name prefix every node of THIS process owns — both its data directory
/// and the `<dir>.log` beside it.
fn our_prefix() -> String {
    format!("flint-chaos-{}-", std::process::id())
}

/// Remove this run's files. `logs_too` is false when a panic is unwinding.
///
/// Handles directories AND plain files deliberately. The first cut of this
/// called only `remove_dir_all`, which silently does nothing to a regular
/// file, so the data directories vanished while 4,742 `.log` files stayed —
/// a cleanup that reported success and left most of the mess. Same failure
/// shape as every other half-check this harness has grown out of.
fn remove_run_files(prefix: &str, logs_too: bool) {
    let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) else {
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if !name.starts_with(prefix) {
            continue;
        }
        if name.ends_with(".log") {
            if logs_too {
                let _ = std::fs::remove_file(e.path());
            }
        } else {
            let _ = std::fs::remove_dir_all(e.path());
        }
    }
}

/// Sweep directories left by chaos runs whose process is gone.
///
/// Drop covers this run; it cannot cover the ones already on the box, and it
/// cannot cover a run killed with SIGKILL or ended by `process::exit`, which
/// skip destructors entirely. So each run also clears the corpses of dead
/// ones. Ownership is proven by the pid embedded in the name: `kill(pid, 0)`
/// failing with ESRCH means no such process, so nothing can still be using
/// that directory. A live pid is left strictly alone — two concurrent chaos
/// runs must not delete each other's data, which is the same scoping rule
/// tools/lib/fleet.sh applies to processes.
pub fn sweep_stale_dirs() {
    let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) else {
        return;
    };
    let me = std::process::id();
    let (mut swept, mut bytes) = (0u32, 0u64);
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        let Some(rest) = name.strip_prefix("flint-chaos-") else {
            continue;
        };
        let Some(pid) = rest.split('-').next().and_then(|p| p.parse::<u32>().ok()) else {
            continue;
        };
        if pid == me || pid_is_alive(pid) {
            continue;
        }
        let path = e.path();
        let removed = if path.is_dir() {
            bytes += dir_bytes(&path);
            std::fs::remove_dir_all(&path).is_ok()
        } else {
            bytes += path.metadata().map(|m| m.len()).unwrap_or(0);
            std::fs::remove_file(&path).is_ok()
        };
        if removed {
            swept += 1;
        }
    }
    if swept > 0 {
        eprintln!(
            "  swept {swept} stale chaos dir(s), {} MB reclaimed",
            bytes / 1_048_576
        );
    }
}

fn pid_is_alive(pid: u32) -> bool {
    // Reject 0 BEFORE asking the kernel. `kill(0, sig)` does not address
    // process 0 — it addresses the caller's whole process group — so it
    // returns success and would report a directory named `flint-chaos-0-*`
    // as owned by something alive, forever, un-sweepable. No real process is
    // pid 0, so any such directory is garbage by definition.
    if pid == 0 {
        return false;
    }
    // Signal 0 performs the permission and existence checks without sending
    // anything. Cheaper and far more precise than parsing `ps`.
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

fn dir_bytes(p: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(p) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| match e.metadata() {
            Ok(m) if m.is_dir() => dir_bytes(&e.path()),
            Ok(m) => m.len(),
            Err(_) => 0,
        })
        .sum()
}

/// Execute the server binary ONCE, print-and-exit, before any spawn that is
/// timed. Costs milliseconds when warm.
///
/// The first exec of a freshly linked binary pays for dynamic linking,
/// signature validation and a cold page cache, and on this machine that was
/// measured at TWENTY SECONDS — against `bootstrap_at`'s 15s PING budget. The
/// failure is maximally misleading: the node log is EMPTY (the process has not
/// reached its first print), no port is bound, and the run dies at `master up`
/// as though the server were broken. Three chaos runs were spent on it, and
/// the same shape once cost an afternoon elsewhere in this suite.
///
/// gates.sh already warms flint-server centrally for exactly this reason, but
/// 49 drills build their own binaries and any of them run standalone skips
/// that. Warming HERE covers every spawn path this harness has — bootstrap,
/// promote-replace, respawn — from one place, and covers the soak too.
fn warm_server_bin() {
    static WARM: std::sync::Once = std::sync::Once::new();
    WARM.call_once(|| {
        // print-and-exit, so this can never bind a port or leave a seat.
        // ALL the binaries this harness spawns, not just the server: gate
        // run 3 lost its chaos leg to a freshly re-linked flint-controller
        // spending its first-exec loader stall inside the 20s promotion
        // window — the same failure this warm-up already prevented for
        // flint-server, one binary over.
        for bin in [server_bin(), controller_bin(), proxy_bin()] {
            let _ = Command::new(bin).arg("--build-version").output();
        }
    });
}

fn spawn_node(port: u16, dir: &str, replica_of: Option<u16>) -> Child {
    warm_server_bin();
    let mut cmd = Command::new(server_bin());
    cmd.args([
        "--port",
        &port.to_string(),
        "--engine",
        "rocks",
        "--data-dir",
        dir,
    ]);
    if let Some(master) = replica_of {
        cmd.args(["--replica-of", &format!("127.0.0.1:{master}")]);
    }
    // Optional write-quorum gate for every node the harness spawns (set by
    // workloads exercising min-replicas-to-write under failover). Env-based
    // so it reaches all spawn sites — bootstrap, promote-replace, respawn.
    if let Ok(n) = std::env::var("FLINT_CHAOS_MIN_REPLICAS")
        && n != "0"
    {
        cmd.args(["--min-replicas-to-write", &n]);
    }
    // The lag cap the ORACLE asserts against must be the cap the SERVER
    // actually enforces, or the check is measuring one number against a
    // different one. Env-based for the same reason as the quorum gate: it has
    // to reach every spawn site, including promote-replace and respawn.
    if let Ok(ms) = std::env::var("FLINT_CHAOS_LAG_HARD_MS")
        && !ms.is_empty()
    {
        // BOTH caps, because ReplHub::new clamps hard up to soft
        // (`lag_hard_ms.max(lag_soft_ms)`) to keep hard >= soft. Passing only
        // --lag-hard-ms 5 therefore yields a 500ms cap — the default soft —
        // and a run that looks like it tested aggressive shedding while
        // testing nothing of the sort. Two experiments here read zero before
        // the clamp was found.
        let soft = ms
            .parse::<u64>()
            .map(|v| v.saturating_sub(1).max(1))
            .unwrap_or(1);
        cmd.args(["--lag-soft-ms", &soft.to_string(), "--lag-hard-ms", &ms]);
    }
    // Optional async write queue (ADR-0005 D4) for every spawned node, so
    // the queue survives promote-replace and respawn. The open-mode proxy
    // pins namespace "0", so `FLINT_CHAOS_ASYNC_WRITES=0` opts the whole
    // chaos path in.
    if let Ok(spec) = std::env::var("FLINT_CHAOS_ASYNC_WRITES")
        && !spec.is_empty()
    {
        cmd.args(["--async-writes", &spec]);
    }
    let log = std::fs::File::create(format!("{dir}.log")).expect("node log");
    cmd.stderr(log)
        .stdout(std::process::Stdio::null())
        .spawn()
        .expect("spawn flint-server")
}

/// SIGSTOP / SIGCONT a node by port, to make replication fall behind ON
/// PURPOSE.
///
/// Every chaos run ever recorded reported `deepest acked-write loss: 0ms`
/// and `writes shed -THROTTLED: 0` — including a 7-host run over a real
/// network. Those zeros are not evidence of correctness; they mean the
/// harness never created the condition. Loopback replication acks in ~0.2ms,
/// so the unreplicated suffix closes inside the measurement's resolution and
/// lag never approaches the 1000ms cap.
///
/// Freezing the replica for a chosen interval produces exactly the regime the
/// RPO claim describes: a master acking writes its replica has not taken yet.
/// Under the liveness window (2s) a short freeze leaves the replica still
/// counted live, so the kill gate still sees a pair worth failing over.
fn signal_by_port(port: u16, sig: &str) {
    let _ = Command::new("pkill")
        .args([sig, "-f", &format!("flint-server --port {port}")])
        .status();
}

fn kill_by_port(port: u16) {
    let _ = Command::new("pkill")
        .args(["-9", "-f", &format!("flint-server --port {port}")])
        .status();
}

fn fresh_dir(n: u32) -> String {
    let dir = std::env::temp_dir().join(format!("flint-chaos-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir.display().to_string()
}

pub fn wait_for_pong(port: u16, budget: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < budget {
        if let Ok(mut c) = Client::connect(port)
            && matches!(c.call(&[b"PING"]), Ok(Value::Simple(s)) if s == "PONG")
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(30));
    }
    false
}

pub fn dbsize(port: u16) -> Option<i64> {
    let mut c = Client::connect(port).ok()?;
    match c.call(&[b"DBSIZE"]).ok()? {
        Value::Integer(n) => Some(n),
        _ => None,
    }
}

fn flintinfo_field(port: u16, field: &str) -> Option<String> {
    let mut c = Client::connect(port).ok()?;
    let Value::Bulk(Some(info)) = c.call(&[b"FLINTINFO"]).ok()? else {
        return None;
    };
    String::from_utf8_lossy(&info)
        .split(['\r', '\n'])
        .find(|f| f.starts_with(field))
        .map(|f| f.trim_start_matches(field).to_string())
}

fn controller_bin() -> String {
    std::env::var("FLINT_CONTROLLER_BIN").unwrap_or_else(|_| {
        format!(
            "{}/../../target/release/flint-controller",
            env!("CARGO_MANIFEST_DIR")
        )
    })
}

fn proxy_bin() -> String {
    std::env::var("FLINT_PROXY_BIN").unwrap_or_else(|_| {
        format!(
            "{}/../../target/release/flint-proxy",
            env!("CARGO_MANIFEST_DIR")
        )
    })
}

/// Poll until `port` reports the given role, or the budget elapses.
fn wait_until_role(port: u16, role: &str, budget: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < budget {
        if flintinfo_field(port, "role:").is_some_and(|v| v.trim() == role) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

/// How many ports one chaos cluster may bind, counting from its base. A
/// caller declares `base .. base+SPAN-1` to `fleet_init` and this crate must
/// stay inside that: base+0 master, base+1 replica, base+2 proxy, and
/// base+FIRST_POOL upward for replacement replicas.
///
/// The number is a contract with the drills, so it lives here rather than
/// being spelled out in seven shell scripts. Every port below is derived
/// from `base` by construction — the pool wraps within `POOL` slots rather
/// than climbing — so staying inside the block is a property of the
/// arithmetic, not of anyone remembering.
pub const SPAN: u16 = 8;
const FIRST_POOL: u16 = 3;
const POOL: u16 = SPAN - FIRST_POOL;

/// Can this address be bound right now? Same bind-to-prove-it discipline the
/// rest of the harness uses: a process holding the port is a fact about the
/// kernel, not something to infer from a pidfile or a `ps` line.
fn port_free(port: u16) -> bool {
    std::net::TcpListener::bind(("127.0.0.1", port)).is_ok()
}

/// A master + one replica, managed like the trio will: promote-on-master-
/// kill, replace-on-any-kill. Topology mechanics only — each workload owns
/// its own data oracle.
///
/// Two modes:
///   - default (`bootstrap`): the harness IS the trio — it promotes on a
///     master kill and attaches replacements on fresh ports.
///   - controlled (`bootstrap_controlled`): a real flint-controller process
///     makes the failover decisions; the harness only kills nodes and
///     re-attaches replacements. Ports are FIXED (a replacement reuses the
///     dead node's port with a fresh data dir) so the controller watches
///     stable addresses while roles float between them.
pub struct Cluster {
    fleet: Fleet,
    /// The first port of this cluster's declared block.
    base: u16,
    master_port: u16,
    replica_port: u16,
    next_id: u32,
    controlled: bool,
    pub master_kills: u32,
    pub replica_kills: u32,
    /// base + 2; only bound when start_proxy is called.
    pub proxy_port: u16,
    /// Wall clock stamped immediately AFTER the most recent master kill's
    /// SIGKILL was delivered. The ledger needs this boundary, not the one
    /// armed before the kill: between arming and pkill actually landing the
    /// old master is alive and acking, and an ack from that gap judged as
    /// "the new master's" leaves the ledger claiming a value the survivor
    /// never had.
    pub last_kill_dead_ms: u64,
}

impl Cluster {
    /// Ports come from a BASE rather than three constants.
    ///
    /// They used to be hardcoded 6460/6470/7690, which made them invisible
    /// to the drill-port guards: a drill driving chaos binds them without
    /// declaring them, so nothing could see that tenant_quota's control
    /// plane also sits on 7690, or that two chaos-driven drills necessarily
    /// share a cluster's worth of ports. A base makes each caller's block
    /// declarable, which is what lets the guard do its job.
    pub fn bootstrap_at(base: u16) -> Self {
        let fleet = Fleet::new();
        let master_port = base;
        let replica_port = base + 1;
        fleet.track(spawn_node(master_port, &fresh_dir(0), None));
        // Same 15s budget replacement spawns get: a loaded box (another
        // drill building its dataset, RocksDB opening) can hold a fresh
        // node past 5s before it answers PING, and bootstrap dying on that
        // is a flake, not a finding.
        assert!(
            wait_for_pong(master_port, Duration::from_secs(15)),
            "master up"
        );
        fleet.track(spawn_node(replica_port, &fresh_dir(1), Some(master_port)));
        assert!(
            wait_for_pong(replica_port, Duration::from_secs(15)),
            "replica up"
        );
        Self {
            fleet,
            base,
            master_port,
            replica_port,
            proxy_port: base + 2,
            // 0 and 1 are the two seats bootstrap just made; `next_id` also
            // names data directories, which must stay unique for the life of
            // the run even though the PORT it maps to cycles.
            next_id: 2,
            controlled: false,
            master_kills: 0,
            replica_kills: 0,
            last_kill_dead_ms: 0,
        }
    }

    // There is deliberately NO `bootstrap()` / `bootstrap_controlled()`
    // defaulting to 6460. Those wrappers existed for one commit and did the
    // damage the port base was added to undo: `chain`, `hotkey` and
    // `proxy_chaos` all kept them, so three of this crate's four binaries
    // went on binding the hardcoded block while the drills driving them
    // declared a different one. A default is an invitation to stay
    // invisible; every caller now has to say which block it owns.

    /// Fixed ports; the controller watches both forever.
    ///
    /// No lease here: ADR-0018 moved the write lease to the CP, and these
    /// local chaos fleets run without one (--nodes mode). The controller is
    /// detection+promotion only; lease fencing is exercised by the drills
    /// that stand up a real CP (lease/stall/bystander).
    pub fn bootstrap_controlled_at(base: u16, poll_ms: u64, confirm: u32) -> Self {
        let mut c = Self::bootstrap_at(base);
        c.controlled = true;
        let nodes = format!("127.0.0.1:{},127.0.0.1:{}", c.master_port, c.replica_port);
        let child = Command::new(controller_bin())
            .args([
                "--nodes",
                &nodes,
                "--id",
                "chaos-ctl",
                "--poll-ms",
                &poll_ms.to_string(),
                "--confirm",
                &confirm.to_string(),
            ])
            .stderr(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("spawn flint-controller");
        c.fleet.track(child);
        c
    }

    pub fn master(&self) -> u16 {
        self.master_port
    }

    pub fn replica(&self) -> u16 {
        self.replica_port
    }

    /// Start a proxy in front of this pair and return its client port. Only
    /// valid in controlled mode: the proxy is configured with the pair's two
    /// FIXED ports and chases whichever is master itself (its own FLINTINFO
    /// rediscovery), exactly as it would in production behind a controller —
    /// so a workload connects to the proxy and never learns a node port.
    /// Both nodes' ordering is stable across failovers because roles float
    /// between the same two ports.
    pub fn start_proxy(&mut self) -> u16 {
        self.start_proxy_with_workers(None)
    }

    /// Start the edge with an explicit worker count.
    ///
    /// Worker count is the variable that matters for the async proxy
    /// (ADR-0021): backend connections are owned per worker, so N workers on
    /// one node means N independent FIFO streams whose replies must never be
    /// crossed. Pinning it makes that testable instead of dependent on
    /// whatever core count the runner happens to have.
    pub fn start_proxy_with_workers(&mut self, workers: Option<usize>) -> u16 {
        assert!(
            self.controlled,
            "start_proxy needs bootstrap_controlled (fixed ports)"
        );
        let proxy_port = self.proxy_port;
        let pairs = format!(
            "127.0.0.1:{},127.0.0.1:{}",
            self.master_port, self.replica_port
        );
        let log = std::fs::File::create(format!(
            "{}/flint-chaos-proxy.log",
            std::env::temp_dir().display()
        ))
        .expect("proxy log");
        let mut argv = vec![
            "--port".to_string(),
            proxy_port.to_string(),
            "--pairs".to_string(),
            pairs,
        ];
        if let Some(w) = workers {
            argv.push("--workers".to_string());
            argv.push(w.to_string());
        }
        let child = Command::new(proxy_bin())
            .args(&argv)
            .stderr(log)
            .stdout(std::process::Stdio::null())
            .spawn()
            .expect("spawn flint-proxy");
        self.fleet.track(child);
        assert!(
            wait_for_pong(proxy_port, Duration::from_secs(10)),
            "proxy up on :{proxy_port}"
        );
        proxy_port
    }

    /// True once the replica is live AND has fully converged in sequence
    /// (seq_lag == 0) AND is within the time-lag cap. Sequence convergence
    /// is the promotion-readiness signal: a replica draining a backlog can
    /// show time-lag ~0 (no recent writes) while still missing tens of
    /// thousands of keys — promoting it then freezes an incomplete dataset
    /// (BUG-0001). Requiring seq_lag == 0 is what makes failover safe.
    pub fn wait_healthy(&self, budget: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < budget {
            let live = flintinfo_field(self.master_port, "live_replicas:")
                .is_some_and(|v| v.trim() != "0");
            let converged = flintinfo_field(self.master_port, "seq_lag:")
                .and_then(|v| v.trim().parse::<u64>().ok())
                .is_some_and(|lag| lag == 0);
            let in_time = flintinfo_field(self.master_port, "lag_ms:")
                .and_then(|v| v.trim().parse::<u64>().ok())
                .is_some_and(|lag| lag < 400);
            if live && converged && in_time {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        false
    }

    /// A live replica exists; its lag is deliberately NOT constrained.
    ///
    /// The counterpart of wait_healthy for the RPO regime: see
    /// Attached::wait_replica_live. Killing only converged masters proves
    /// nothing about the loss window, because there is nothing to lose.
    pub fn wait_replica_live(&self, budget: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < budget {
            if flintinfo_field(self.master_port, "live_replicas:").is_some_and(|v| v.trim() != "0")
            {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        false
    }

    /// A port for a replacement replica, from THIS cluster's block.
    ///
    /// This was `6470 + next_id`: hardcoded, base-independent, and unbounded.
    /// Giving the cluster a `--port-base` moved the two seats that exist
    /// before the first kill and nothing else, so every replacement after
    /// that walked up from 6470 no matter what the caller asked for — a
    /// 12-second run was measured binding 6472, which `reseed_drill`
    /// declares. The seats a chaos run spends most of its life on were
    /// exactly the ones still outside every drill's declared block, which is
    /// the whole defect the base was supposed to fix.
    ///
    /// So the pool is base-relative AND bounded, because a drill can only
    /// declare a bounded set. It cycles rather than climbing: only two nodes
    /// are alive at a time, so `POOL` slots give that many kills of
    /// separation before an address is reused. A slot still held by a
    /// process that has not finished dying is skipped, not waited on — this
    /// runs after `kill_by_port` has already sent -9.
    fn next_replica_port(&mut self) -> u16 {
        for _ in 0..POOL {
            let p = self.base + FIRST_POOL + (self.next_id % POOL as u32) as u16;
            self.next_id += 1;
            if p != self.master_port && port_free(p) {
                return p;
            }
        }
        panic!(
            "no free replacement port in {}..{} — every slot in this cluster's \
             block is still held. Widen SPAN or find what is not dying.",
            self.base + FIRST_POOL,
            self.base + SPAN - 1
        );
    }

    /// Kill the master, promote the replica (read-then-bump epoch, exactly
    /// as the trio will), attach a fresh replacement replica. Returns the
    /// new master port. Caller should `wait_healthy` first for steady state.
    pub fn kill_master(&mut self) -> u16 {
        self.master_kills += 1;
        // Read the survivor's epoch before the kill (it is still reachable).
        let current: u32 = flintinfo_field(self.replica_port, "role_epoch:")
            .and_then(|v| {
                v.trim()
                    .trim_matches(|c| c == '(' || c == ')')
                    .split(',')
                    .nth(1)
                    .and_then(|c| c.parse().ok())
            })
            .unwrap_or(1);
        kill_by_port(self.master_port);
        self.last_kill_dead_ms = crate::writer::now_ms();
        let next = current + 1;
        let mut c = Client::connect(self.replica_port).expect("survivor connect");
        match c.call(&[b"FLINTPROMOTE", b"0", next.to_string().as_bytes()]) {
            Ok(Value::Simple(_)) => {}
            other => panic!("promotion to (0,{next}) failed (epoch was {current}): {other:?}"),
        }
        // Promoted survivor is the new master; attach a fresh replica.
        self.master_port = self.replica_port;
        self.replica_port = self.next_replica_port();
        let dir = fresh_dir(self.next_id);
        self.fleet
            .track(spawn_node(self.replica_port, &dir, Some(self.master_port)));
        assert!(
            wait_for_pong(self.replica_port, Duration::from_secs(15)),
            "replacement replica up"
        );
        self.master_port
    }

    /// Wait for the pair to be *sustainably* converged before a controlled
    /// master kill. The controller resets its converged flag on each
    /// promotion and re-promotes only a survivor it has independently
    /// re-observed at seq_lag==0; wait_healthy proves the master's view, but
    /// killing the instant it flips can outrun the controller's poll. Holding
    /// that view stable for a window comfortably beyond confirm*poll
    /// guarantees the controller's next sweep records it. (This is the
    /// steady-state regime under test, not the degraded window.)
    pub fn await_controller_observed(&self) {
        let need = Duration::from_millis(1_300);
        let deadline = Instant::now() + Duration::from_secs(12);
        let mut converged_since: Option<Instant> = None;
        while Instant::now() < deadline {
            let converged = flintinfo_field(self.master_port, "seq_lag:")
                .and_then(|v| v.trim().parse::<u64>().ok())
                .is_some_and(|l| l == 0)
                && flintinfo_field(self.master_port, "live_replicas:")
                    .is_some_and(|v| v.trim() != "0");
            match (converged, converged_since) {
                (true, None) => converged_since = Some(Instant::now()),
                (true, Some(t)) if t.elapsed() >= need => return,
                (false, _) => converged_since = None,
                _ => {}
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Controlled mode: kill the master, then WAIT for the external
    /// controller to promote the survivor (the harness does not promote).
    /// Re-attach a fresh replacement replica on the dead node's fixed port.
    pub fn kill_master_await_controller(&mut self) -> u16 {
        self.await_controller_observed();
        self.kill_master_now_await_controller()
    }

    /// Controlled mode, hot-writer variant: kill IMMEDIATELY, without the
    /// sustained-convergence wait. For workloads that keep writing through
    /// the kill (the wait can never observe seq_lag==0 under a live hammer):
    /// the caller parks its writer, proves convergence itself (wait_healthy +
    /// await_controller_observed), resumes the writer, then calls this.
    pub fn kill_master_now_await_controller(&mut self) -> u16 {
        assert!(self.controlled, "use bootstrap_controlled");
        self.master_kills += 1;
        let dead = self.master_port;
        let survivor = self.replica_port;
        kill_by_port(dead);
        self.last_kill_dead_ms = crate::writer::now_ms();
        assert!(
            wait_until_role(survivor, "master", Duration::from_secs(20)),
            "controller did not promote survivor :{survivor} within 20s"
        );
        // Bring the dead port back as a fresh replica of the new master.
        self.fleet
            .track(spawn_node(dead, &fresh_dir(self.next_id), Some(survivor)));
        self.next_id += 1;
        self.master_port = survivor;
        self.replica_port = dead;
        assert!(
            wait_for_pong(dead, Duration::from_secs(15)),
            "replacement replica up on :{dead}"
        );
        self.master_port
    }

    /// Controlled mode: kill the replica, re-attach a fresh one on the same
    /// fixed port.
    pub fn kill_replica_fixed(&mut self) {
        assert!(self.controlled, "use bootstrap_controlled");
        self.replica_kills += 1;
        let dead = self.replica_port;
        kill_by_port(dead);
        self.fleet.track(spawn_node(
            dead,
            &fresh_dir(self.next_id),
            Some(self.master_port),
        ));
        self.next_id += 1;
        assert!(
            wait_for_pong(dead, Duration::from_secs(15)),
            "replacement replica up on :{dead}"
        );
    }

    /// Kill the replica and attach a fresh replacement.
    pub fn kill_replica(&mut self) {
        self.replica_kills += 1;
        kill_by_port(self.replica_port);
        self.replica_port = self.next_replica_port();
        let dir = fresh_dir(self.next_id);
        self.fleet
            .track(spawn_node(self.replica_port, &dir, Some(self.master_port)));
        assert!(
            wait_for_pong(self.replica_port, Duration::from_secs(15)),
            "replacement replica up"
        );
    }
}

#[cfg(test)]
mod sweep_tests {
    use super::*;

    /// The sweep must never touch a LIVE run's directory.
    ///
    /// Two chaos runs on one box is normal — the gate runs `chaos` and
    /// `proxy_chaos` back to back, and a developer may have one going. A sweep
    /// that deleted by name alone would pull the RocksDB directory out from
    /// under a running node, and the victim would report corruption: a data
    /// bug that never happened, in the one harness whose entire job is to
    /// tell real corruption from noise. Ownership is decided by whether the
    /// pid in the name is still alive, exactly as fleet.sh scopes processes.
    #[test]
    fn sweep_spares_a_living_runs_directory() {
        let tmp = std::env::temp_dir();
        // Our own pid: alive by construction, and the sweep skips it twice
        // over (pid == me, and pid_is_alive).
        let mine = tmp.join(format!("flint-chaos-{}-sweeptest", std::process::id()));
        // A pid that cannot be running: pid 0 is never a normal process, and
        // kill(0, 0) addresses the process GROUP rather than a process, so
        // the sweep must decide this one on the ESRCH path, not by accident.
        let dead = tmp.join("flint-chaos-4294967294-sweeptest");
        std::fs::create_dir_all(&mine).expect("make live-run dir");
        std::fs::create_dir_all(&dead).expect("make dead-run dir");
        std::fs::write(mine.join("CURRENT"), b"live").expect("seed live-run data");

        sweep_stale_dirs();

        assert!(mine.exists(), "sweep deleted a LIVE run's data directory");
        assert!(!dead.exists(), "sweep left a dead run's directory behind");
        let _ = std::fs::remove_dir_all(&mine);
    }

    /// A FAILING run keeps its logs and drops its data.
    ///
    /// This is the asymmetry the cleanup exists for. The run that fails is
    /// the one whose evidence matters, and it is also the one that used to
    /// leak, because the oracle asserts and an assert skipped the tidy-up
    /// path entirely. Data directories are hundreds of megabytes and fully
    /// reproducible from the printed seed; logs are kilobytes and are the
    /// only record of what the server believed at the moment it broke.
    #[test]
    fn a_failing_run_keeps_logs_and_drops_data() {
        let tmp = std::env::temp_dir();
        let prefix = format!("flint-chaos-panictest{}-", std::process::id());
        let data = tmp.join(format!("{prefix}0"));
        let log = tmp.join(format!("{prefix}0.log"));
        std::fs::create_dir_all(&data).expect("make data dir");
        std::fs::write(data.join("CURRENT"), b"sst").expect("seed data dir");
        std::fs::write(&log, b"what the server thought").expect("seed node log");

        // logs_too=false is exactly what Drop passes while panicking.
        remove_run_files(&prefix, false);
        assert!(!data.exists(), "a failing run must still drop its data");
        assert!(log.exists(), "a failing run must KEEP its node logs");

        // And the passing path takes both.
        remove_run_files(&prefix, true);
        assert!(!log.exists(), "a passing run must remove its logs too");
    }

    /// The first cut called only `remove_dir_all`, which returns an error on
    /// a regular file and was ignored — so 4,742 `.log` files survived a
    /// cleanup that reported success. Pin the file half specifically.
    #[test]
    fn cleanup_removes_log_files_not_only_directories() {
        let tmp = std::env::temp_dir();
        let prefix = format!("flint-chaos-filetest{}-", std::process::id());
        let log = tmp.join(format!("{prefix}7.log"));
        std::fs::write(&log, b"x").expect("seed log file");
        remove_run_files(&prefix, true);
        assert!(
            !log.exists(),
            "cleanup skipped a plain file — remove_dir_all does nothing to one"
        );
    }

    #[test]
    fn pid_zero_is_not_reported_as_a_live_process() {
        // kill(0, 0) returns success — it signals the caller's process GROUP,
        // not a process numbered 0 — so a check that just forwards to the
        // kernel would call pid 0 alive forever and leak any directory named
        // after it. The guard is an explicit early return, and this is what
        // holds it in place.
        assert!(
            !pid_is_alive(0),
            "pid 0 reported alive: kill(0,0) signals the process GROUP and \
             always succeeds, so a directory named for it would never be swept"
        );
        // The live case still has to work, or the guard above could be
        // "return false" and this file would still be green.
        assert!(
            pid_is_alive(std::process::id()),
            "our own pid must read as alive"
        );
    }
}
