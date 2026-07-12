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

/// A master + one replica, managed like the trio will: promote-on-master-
/// kill, replace-on-any-kill. Topology mechanics only — each workload owns
/// its own data oracle.
pub struct Cluster {
    fleet: Fleet,
    master_port: u16,
    replica_port: u16,
    next_id: u32,
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
            master_kills: 0,
            replica_kills: 0,
        }
    }

    pub fn master(&self) -> u16 {
        self.master_port
    }

    pub fn replica(&self) -> u16 {
        self.replica_port
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
