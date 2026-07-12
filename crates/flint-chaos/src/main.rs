//! flint-chaos: randomly kill master or replica under continuous load and
//! verify the durability contract with a client-side ledger.
//!
//! The harness plays the meta trio's role manually: it detects its own
//! kills, promotes the survivor via the same epoch-fenced FLINTPROMOTE the
//! trio will use, and attaches a fresh replacement replica (full-sync
//! seeded). Old victims are never restarted in place — master rejoin/demote
//! is a trio-era feature; replacements use fresh dirs (the spare pattern).
//!
//! Oracle (per docs/design.md):
//!   1. No corruption ever: every value read back must be a value the
//!      writer wrote for that key (values embed key, seq, crc).
//!   2. Replica kills: zero acked-write loss.
//!   3. Master kills: acked loss allowed but bounded and MEASURED —
//!      reported per kill like the product's incident accounting.
//!   4. No time travel: final seq per key <= last written seq.
//!
//! Usage: flint-chaos [--iterations 12] [--keys 400] [--mode mixed|replica|master]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

use flint_resp::{Decoded, Value, decode, encode};
use flint_slot::crc16;
use rand::{Rng, SeedableRng, rngs::SmallRng};

const MPORT: u16 = 6460;
const RPORT_BASE: u16 = 6470;

fn arg<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::args()
        .skip_while(|a| a != name)
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    fn connect(port: u16) -> std::io::Result<Self> {
        let stream = TcpStream::connect(("127.0.0.1", port))?;
        stream.set_read_timeout(Some(Duration::from_millis(1500)))?;
        Ok(Self {
            stream,
            buf: Vec::new(),
        })
    }

    fn call(&mut self, args: &[&[u8]]) -> std::io::Result<Value> {
        let frame = Value::Array(Some(
            args.iter().map(|a| Value::Bulk(Some(a.to_vec()))).collect(),
        ));
        let mut out = Vec::new();
        encode(&frame, &mut out);
        self.stream.write_all(&out)?;
        let mut chunk = [0u8; 16 * 1024];
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

/// Kills a set of spawned servers on drop — so a panicking assertion can
/// never leave zombies holding ports (which is exactly what fooled an
/// earlier debugging session into a false cross-key report).
struct Fleet(std::rc::Rc<std::cell::RefCell<Vec<Child>>>);
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
    // The victim is tracked by the Fleet for cleanup, but must die NOW.
    let _ = Command::new("pkill")
        .args(["-9", "-f", &format!("flint-server --port {port}")])
        .status();
}

fn fresh_dir(tag: &str, n: u32) -> String {
    let dir = std::env::temp_dir().join(format!("flint-chaos-{tag}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir.display().to_string()
}

/// Wait until `port` (a master) reports a live replica with lag under the
/// cap — i.e. the pair is in the healthy regime where the ≤1s RPO contract
/// applies. Returns false if it never gets there.
fn wait_for_live_replica(port: u16, budget: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < budget {
        if let Ok(mut c) = Client::connect(port)
            && let Ok(Value::Bulk(Some(info))) = c.call(&[b"FLINTINFO"])
        {
            let info = String::from_utf8_lossy(&info);
            let live = info.contains("live_replicas:1") || info.contains("live_replicas:2");
            let caught_up = info
                .split(['\r', '\n'])
                .find(|f| f.starts_with("lag_ms:"))
                .and_then(|f| f.trim_start_matches("lag_ms:").parse::<u64>().ok())
                .is_some_and(|lag| lag < 400);
            if live && caught_up {
                return true;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn wait_for_pong(port: u16, budget: Duration) -> bool {
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

/// Read the survivor's current role epoch counter (generation is 0 in v0)
/// so promotion bumps from the actual durable value, exactly as the trio
/// will: read-then-increment, never a guessed global counter.
fn role_epoch_counter(port: u16) -> Option<u32> {
    let mut c = Client::connect(port).ok()?;
    let Value::Bulk(Some(info)) = c.call(&[b"FLINTINFO"]).ok()? else {
        return None;
    };
    let info = String::from_utf8_lossy(&info);
    let field = info
        .split(['\r', '\n'])
        .find(|f| f.starts_with("role_epoch:"))?;
    // role_epoch:(0,2)
    let inner = field
        .trim_start_matches("role_epoch:")
        .trim_matches(|c| c == '(' || c == ')');
    inner.split(',').nth(1)?.parse().ok()
}

/// Value embeds the OWNING KEY literally plus the seq and a crc — so a
/// value that belongs to another key is detectable with certainty, not by
/// checksum luck. Layout: "flint|<key>|<seq>|<crc16(key|seq)>".
fn value_for(key: &str, seq: u64) -> String {
    let crc = crc16(format!("{key}|{seq}").as_bytes());
    format!("flint|{key}|{seq}|{crc:04x}")
}

/// Returns the (owning_key, seq) the value actually encodes, or None if the
/// bytes are not a well-formed flint value (torn/garbage). Cross-key is
/// then `owning_key != expected` at the call site — unambiguous.
fn parse_value(raw: &[u8]) -> Option<(String, u64)> {
    let s = std::str::from_utf8(raw).ok()?;
    let mut parts = s.split('|');
    if parts.next()? != "flint" {
        return None;
    }
    let key = parts.next()?.to_string();
    let seq: u64 = parts.next()?.parse().ok()?;
    let crc: u16 = u16::from_str_radix(parts.next()?, 16).ok()?;
    if crc != crc16(format!("{key}|{seq}").as_bytes()) {
        return None;
    }
    Some((key, seq))
}

#[derive(Default, Clone)]
struct KeyLedger {
    written: Vec<u64>,
    last_acked: u64,
    last_written: u64,
}

fn main() {
    let iterations: u32 = arg("--iterations", 12);
    let key_count: u64 = arg("--keys", 400);
    let mode: String = arg("--mode", "mixed".to_string());

    println!("chaos: {iterations} kills, {key_count} keys, mode={mode}");

    // Topology: one master, one replica; the harness is the trio.
    let fleet = Fleet::new();
    let mut next_dir = 0u32;
    let mut master_port = MPORT;
    let mut replica_port = RPORT_BASE;
    let master = spawn_node(master_port, &fresh_dir("m", next_dir), None);
    fleet.track(master);
    next_dir += 1;
    assert!(
        wait_for_pong(master_port, Duration::from_secs(5)),
        "master up"
    );
    let replica = spawn_node(replica_port, &fresh_dir("r", next_dir), Some(master_port));
    fleet.track(replica);
    next_dir += 1;
    assert!(
        wait_for_pong(replica_port, Duration::from_secs(5)),
        "replica up"
    );

    let mut ledger: HashMap<String, KeyLedger> = HashMap::new();
    // seq -> key it was actually written to (diagnostic for unknown-seq).
    let mut seq_owner: HashMap<u64, String> = HashMap::new();
    let mut rng = SmallRng::seed_from_u64(arg("--seed", 42));
    let mut seq = 0u64;
    let mut epoch_counter = 1u32;
    let mut master_kills = 0u32;
    let mut replica_kills = 0u32;
    let mut acked_lost_total = 0u64;
    let mut writer = Client::connect(master_port).expect("writer connect");

    for iteration in 1..=iterations {
        // Write continuously for a random spell.
        let spell = Duration::from_millis(rng.random_range(400..900));
        let start = Instant::now();
        while start.elapsed() < spell {
            seq += 1;
            let key = format!("key{}", rng.random_range(0..key_count));
            let value = value_for(&key, seq);
            seq_owner.insert(seq, key.clone());
            let entry = ledger.entry(key.clone()).or_default();
            entry.written.push(seq);
            entry.last_written = seq;
            match writer.call(&[b"SET", key.as_bytes(), value.as_bytes()]) {
                Ok(Value::Simple(s)) if s == "OK" => entry.last_acked = seq,
                Ok(Value::Error(e)) if e.starts_with("THROTTLED") => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Ok(_) | Err(_) => {
                    // Unacked: allowed to be lost. Reconnect lazily.
                    std::thread::sleep(Duration::from_millis(50));
                    if let Ok(c) = Client::connect(master_port) {
                        writer = c;
                    }
                }
            }
        }

        let kill_master = match mode.as_str() {
            "replica" => false,
            "master" => true,
            _ => rng.random_bool(0.5),
        };

        // Diagnostic: what did the master see about its replica right
        // before we kill something?
        if let Ok(mut c) = Client::connect(master_port)
            && let Ok(Value::Bulk(Some(info))) = c.call(&[b"FLINTINFO"])
        {
            let info = String::from_utf8_lossy(&info).replace('\r', " ");
            let live = info
                .split_whitespace()
                .find(|f| f.starts_with("live_replicas:"))
                .unwrap_or("?");
            let lag = info
                .split_whitespace()
                .find(|f| f.starts_with("lag_ms:"))
                .unwrap_or("?");
            eprintln!("  [pre-kill iter {iteration}] master :{master_port} {live} {lag}");
        }

        // Steady-state guard: only kill a master once its replica is live
        // and caught up (healthy regime, ≤1s RPO). Otherwise this round
        // becomes a replica kill — the degraded-window case is tested
        // separately and deliberately, not by accident.
        let kill_master = kill_master && wait_for_live_replica(master_port, Duration::from_secs(8));

        if kill_master {
            master_kills += 1;
            // Read the survivor's current epoch BEFORE killing the master
            // (the survivor is the replica, still reachable).
            let current = role_epoch_counter(replica_port).unwrap_or(1);
            kill_by_port(master_port);
            // Promote at current+1 — read-then-bump, the trio's move.
            let next = current + 1;
            epoch_counter = epoch_counter.max(next);
            let mut c = Client::connect(replica_port).expect("survivor connect");
            let ep = next.to_string();
            match c.call(&[b"FLINTPROMOTE", b"0", ep.as_bytes()]) {
                Ok(Value::Simple(_)) => {}
                other => panic!(
                    "iteration {iteration}: promotion to (0,{next}) failed (survivor epoch was {current}): {other:?}"
                ),
            }
            // Per-kill acked-loss accounting (the product's incident
            // report in miniature): keys whose final value regressed below
            // the last ACKED write. Survivor state becomes the new truth.
            let mut lost_here = 0u64;
            for (key, entry) in ledger.iter_mut() {
                if entry.last_acked == 0 {
                    continue;
                }
                match c.call(&[b"GET", key.as_bytes()]) {
                    Ok(Value::Bulk(Some(raw))) => {
                        let (owner, got) = parse_value(&raw)
                            .unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                        assert_eq!(&owner, key, "CROSS-KEY at {key}: value owned by {owner}");
                        if got < entry.last_acked {
                            lost_here += 1;
                            // Accept the survivor's state as the new truth.
                            entry.last_acked = got;
                            entry.last_written = entry.last_written.max(got);
                        }
                    }
                    Ok(Value::Bulk(None)) => {
                        lost_here += 1;
                        entry.last_acked = 0;
                    }
                    _ => {}
                }
            }
            acked_lost_total += lost_here;
            println!(
                "iter {iteration}: killed MASTER, promoted :{replica_port} at (0,{next}); acked keys regressed: {lost_here}"
            );
            // Survivor is the new master; attach a fresh replacement replica.
            master_port = replica_port;
            replica_port = RPORT_BASE + iteration as u16;
            fleet.track(spawn_node(
                replica_port,
                &fresh_dir("r", next_dir),
                Some(master_port),
            ));
            next_dir += 1;
            writer = Client::connect(master_port).expect("reconnect to new master");
        } else {
            replica_kills += 1;
            kill_by_port(replica_port);
            // Replica death must lose NOTHING: verify every acked key now.
            let mut c = Client::connect(master_port).expect("master connect");
            for (key, entry) in &ledger {
                if entry.last_acked == 0 {
                    continue;
                }
                match c.call(&[b"GET", key.as_bytes()]) {
                    Ok(Value::Bulk(Some(raw))) => {
                        let (owner, got) = parse_value(&raw)
                            .unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                        assert_eq!(&owner, key, "CROSS-KEY at {key}: value owned by {owner}");
                        assert!(
                            got >= entry.last_acked,
                            "iter {iteration}: REPLICA kill lost acked write at {key}: {got} < {}",
                            entry.last_acked
                        );
                    }
                    other => {
                        panic!("iter {iteration}: REPLICA kill lost acked key {key}: {other:?}")
                    }
                }
            }
            println!("iter {iteration}: killed REPLICA; zero acked loss verified");
            replica_port = RPORT_BASE + iteration as u16;
            fleet.track(spawn_node(
                replica_port,
                &fresh_dir("r", next_dir),
                Some(master_port),
            ));
            next_dir += 1;
        }
        assert!(
            wait_for_pong(replica_port, Duration::from_secs(10)),
            "replacement replica up"
        );
    }

    // Final walk: full-keyspace verification on the current master.
    let mut c = Client::connect(master_port).expect("final connect");
    let (mut present, mut missing) = (0u64, 0u64);
    for (key, entry) in &ledger {
        match c.call(&[b"GET", key.as_bytes()]) {
            Ok(Value::Bulk(Some(raw))) => {
                let (owner, got) =
                    parse_value(&raw).unwrap_or_else(|| panic!("TORN VALUE at {key}: {raw:?}"));
                assert_eq!(&owner, key, "CROSS-KEY at {key}: value owned by {owner}");
                assert!(
                    entry.written.contains(&got),
                    "PHANTOM at {key}: seq {got} never written to it"
                );
                assert!(got <= entry.last_written, "TIME TRAVEL at {key}");
                present += 1;
            }
            Ok(Value::Bulk(None)) if entry.last_acked == 0 => missing += 1, // never acked
            Ok(Value::Bulk(None)) => missing += 1, // regressed to deleted-by-loss: already counted
            other => panic!("final walk failed at {key}: {other:?}"),
        }
    }
    // Fleet Drop kills every spawned server.

    println!("---");
    println!(
        "PASS: {iterations} kills ({master_kills} master, {replica_kills} replica), {} writes",
        seq
    );
    println!("  corruption: 0 (checksums verified on every read)  time-travel: 0");
    println!(
        "  acked keys regressed across master kills: {acked_lost_total} (bounded by the async contract; replica kills: zero tolerance held)"
    );
    println!("  final walk: {present} present, {missing} missing-or-regressed");
}
