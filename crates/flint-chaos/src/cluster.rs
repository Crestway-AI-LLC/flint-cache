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
    tls: Option<std::sync::Arc<flint_tls::ClientConfig>>,
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
            let converged = !need_converged
                || info
                    .get("seq_lag")
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .is_some_and(|l| l == 0);
            if live && converged {
                return true;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        false
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
            return Err(format!("controller did not promote {s} within 30s"));
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
    }
}

fn spawn_node(port: u16, dir: &str, replica_of: Option<u16>) -> Child {
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
    master_port: u16,
    replica_port: u16,
    next_id: u32,
    controlled: bool,
    pub master_kills: u32,
    pub replica_kills: u32,
}

impl Cluster {
    pub fn bootstrap() -> Self {
        let fleet = Fleet::new();
        let master_port = 6460;
        let replica_port = 6470;
        fleet.track(spawn_node(master_port, &fresh_dir(0), None));
        assert!(
            wait_for_pong(master_port, Duration::from_secs(5)),
            "master up"
        );
        fleet.track(spawn_node(replica_port, &fresh_dir(1), Some(master_port)));
        assert!(
            wait_for_pong(replica_port, Duration::from_secs(5)),
            "replica up"
        );
        Self {
            fleet,
            master_port,
            replica_port,
            next_id: 2,
            controlled: false,
            master_kills: 0,
            replica_kills: 0,
        }
    }

    /// Bootstrap with a real flint-controller making failover decisions.
    /// Fixed ports; the controller watches both forever.
    pub fn bootstrap_controlled(poll_ms: u64, confirm: u32, lease_ttl_ms: u64) -> Self {
        let mut c = Self::bootstrap();
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
                "--lease-ttl-ms",
                &lease_ttl_ms.to_string(),
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
        assert!(
            self.controlled,
            "start_proxy needs bootstrap_controlled (fixed ports)"
        );
        let proxy_port = 7690;
        let pairs = format!(
            "127.0.0.1:{},127.0.0.1:{}",
            self.master_port, self.replica_port
        );
        let log = std::fs::File::create(format!(
            "{}/flint-chaos-proxy.log",
            std::env::temp_dir().display()
        ))
        .expect("proxy log");
        let child = Command::new(proxy_bin())
            .args(["--port", &proxy_port.to_string(), "--pairs", &pairs])
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

    fn next_replica_port(&mut self) -> u16 {
        let p = 6470 + self.next_id as u16;
        self.next_id += 1;
        p
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
