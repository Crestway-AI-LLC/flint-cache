//! Shared chaos infrastructure: a RESP client, a process fleet that cleans
//! up on panic, and a `Cluster` that plays the meta trio's role manually —
//! detecting kills, promoting survivors via epoch-fenced FLINTPROMOTE, and
//! attaching fresh full-sync-seeded replacements.
//!
//! This is the ONE implementation of the failover mechanics; every chaos
//! workload drives it, so the subtle kill/promote/replace logic can never
//! drift between tests.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use flint_resp::{Decoded, Value, decode, encode};

pub fn arg<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::args()
        .skip_while(|a| a != name)
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

pub struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    pub fn connect(port: u16) -> std::io::Result<Self> {
        let stream = TcpStream::connect(("127.0.0.1", port))?;
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
        let pairs = format!("127.0.0.1:{},127.0.0.1:{}", self.master_port, self.replica_port);
        let log = std::fs::File::create(format!("{}/flint-chaos-proxy.log", std::env::temp_dir().display()))
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
    fn await_controller_observed(&self) {
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
        assert!(self.controlled, "use bootstrap_controlled");
        self.await_controller_observed();
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
