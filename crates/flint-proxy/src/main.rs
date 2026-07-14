//! flint-proxy (v0): the routing plane's front door (docs/design.md §2.1).
//!
//! Clients get ONE plain-RESP endpoint and need no cluster awareness, ever.
//! The proxy owns what clients must never see:
//!   - slot routing: key -> CRC16 slot -> the master that owns it;
//!   - `-MOVED` absorption: a migrated slot's redirect updates the shared
//!     routing cache and the command is retried at the new owner — the
//!     client sees only the final reply (the -MOVED override cache IS the
//!     level-2 routing table, learned from the nodes' own manifests);
//!   - `-TRYAGAIN` absorption: a write hitting a slot frozen mid-cutover is
//!     retried within a bounded budget (~the design's queue-during-failover
//!     window) instead of surfacing;
//!   - failover chasing: a dead backend triggers master rediscovery on that
//!     pair (FLINTINFO role probe — same discovery the controller uses).
//!
//! Default placement is range-based — pair i owns slots [i*16384/N ..
//! (i+1)*16384/N) — matching the control plane's future range maps; the
//! moved-slot cache overrides per slot. A proxy restart loses only cache:
//! the first request routes to the default owner and relearns from -MOVED.
//!
//! v0 scope, deliberately deferred: TLS, tenant auth/namespaces, metering,
//! cross-slot scatter-gather (multi-key commands route by FIRST key),
//! hot-key absorption, RESP3, inline commands. FLINT* admin commands are
//! REJECTED at the proxy: the data-plane admin surface is internal, and the
//! proxy is the tenant boundary.
//!
//! Usage: flint-proxy --port 7379 --pairs "m0,r0;m1,r1;..."

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use flint_resp::{Decoded, Value, decode, encode};
use flint_slot::slot_for_key;

/// Total retry budget for one client command across MOVED chases, TRYAGAIN
/// waits, and failover rediscovery — the proxy's answer to "latency spike,
/// not errors" during topology changes.
const RETRY_BUDGET: Duration = Duration::from_secs(5);
/// Backend I/O timeout. Generous: a frozen-slot drain or a slow disk read
/// must not be misread as a dead node.
const BACKEND_TIMEOUT: Duration = Duration::from_secs(5);

fn arg(name: &str) -> Option<String> {
    std::env::args().skip_while(|a| a != name).nth(1)
}

/// Shared cluster view: the pair list (static in v0), each pair's current
/// master (rediscovered on failure), and the moved-slot override cache.
struct Topology {
    /// Node addresses of each pair; index = pair id.
    pairs: Vec<Vec<String>>,
    /// Current master address per pair (None until discovered).
    masters: RwLock<Vec<Option<String>>>,
    /// Slot -> owner address, learned from -MOVED redirects. Overrides the
    /// range default; entries are re-learned if they go stale (another
    /// -MOVED) or lost on restart (relearned from the default owner).
    moved: RwLock<HashMap<u16, String>>,
}

impl Topology {
    /// Range-based default owner: pair i serves slots [i*N/16384 ..).
    fn default_pair(&self, slot: u16) -> usize {
        (slot as usize * self.pairs.len()) / 16384
    }

    /// The address a command for `slot` should go to right now.
    fn route(&self, slot: u16) -> Option<String> {
        if let Ok(moved) = self.moved.read()
            && let Some(addr) = moved.get(&slot)
        {
            return Some(addr.clone());
        }
        let pair = self.default_pair(slot);
        self.masters.read().ok()?.get(pair)?.clone()
    }

    fn learn_moved(&self, slot: u16, addr: &str) {
        if let Ok(mut moved) = self.moved.write() {
            moved.insert(slot, addr.to_string());
        }
    }

    /// A backend at `addr` failed: forget cached state that points at it and
    /// rediscover the master of every pair that lists it. (Moved entries are
    /// kept — the new master of that pair holds the moved slots too; only
    /// the address may change, which the next -MOVED corrects.)
    fn rediscover_for(&self, addr: &str) {
        for (i, nodes) in self.pairs.iter().enumerate() {
            let involves = nodes.iter().any(|n| n == addr)
                || self
                    .masters
                    .read()
                    .ok()
                    .and_then(|m| m.get(i).cloned().flatten())
                    .is_some_and(|m| m == addr);
            if involves {
                let found = discover_master(nodes);
                if let Ok(mut masters) = self.masters.write()
                    && let Some(slot) = masters.get_mut(i)
                {
                    *slot = found.clone();
                }
                // Moved entries pointing at the dead address migrate to the
                // pair's new master (slot data survives failover via the
                // pair's replica).
                if let Some(new_master) = found
                    && new_master != addr
                    && let Ok(mut moved) = self.moved.write()
                {
                    for v in moved.values_mut() {
                        if v == addr {
                            *v = new_master.clone();
                        }
                    }
                }
            }
        }
    }
}

/// One RESP request/response exchange on a raw connection.
fn call_raw(stream: &mut TcpStream, buf: &mut Vec<u8>, frame: &[u8]) -> std::io::Result<Value> {
    stream.write_all(frame)?;
    let mut chunk = [0u8; 64 * 1024];
    loop {
        match decode(buf) {
            Ok(Decoded::Complete(v, used)) => {
                buf.drain(..used);
                return Ok(v);
            }
            Ok(Decoded::NeedMore) => {
                let n = stream.read(&mut chunk)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "backend closed",
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

/// Probe a pair's nodes for the current master (FLINTINFO role) — the same
/// discovery rule the controller uses.
fn discover_master(nodes: &[String]) -> Option<String> {
    for addr in nodes {
        let Ok(mut stream) = TcpStream::connect(addr) else {
            continue;
        };
        let _ = stream.set_read_timeout(Some(Duration::from_millis(800)));
        let mut out = Vec::new();
        encode(
            &Value::Array(Some(vec![Value::Bulk(Some(b"FLINTINFO".to_vec()))])),
            &mut out,
        );
        let mut buf = Vec::new();
        if let Ok(Value::Bulk(Some(raw))) = call_raw(&mut stream, &mut buf, &out)
            && String::from_utf8_lossy(&raw)
                .split(['\r', '\n'])
                .any(|l| l == "role:master")
        {
            return Some(addr.clone());
        }
    }
    None
}

/// Per-client-thread cache of backend connections.
struct Backends {
    conns: HashMap<String, (TcpStream, Vec<u8>)>,
}

impl Backends {
    fn new() -> Self {
        Self {
            conns: HashMap::new(),
        }
    }

    fn call(&mut self, addr: &str, frame: &[u8]) -> std::io::Result<Value> {
        if !self.conns.contains_key(addr) {
            let stream = TcpStream::connect(addr)?;
            stream.set_read_timeout(Some(BACKEND_TIMEOUT))?;
            stream.set_write_timeout(Some(BACKEND_TIMEOUT))?;
            self.conns.insert(addr.to_string(), (stream, Vec::new()));
        }
        let Some((stream, buf)) = self.conns.get_mut(addr) else {
            return Err(std::io::Error::other("conn cache"));
        };
        let result = call_raw(stream, buf, frame);
        if result.is_err() {
            // A failed connection is never reused: the reply stream may be
            // desynchronized (half-read reply), which would corrupt the next
            // exchange.
            self.conns.remove(addr);
        }
        result
    }
}

/// The key a command routes by (mirrors the server's command_key): args[1]
/// unless the command addresses no key. Multi-key commands route by their
/// FIRST key in v0 (scatter-gather is a follow-on).
fn route_key(args: &[Vec<u8>]) -> Option<&[u8]> {
    let name = args.first()?;
    const NO_KEY: &[&[u8]] = &[
        b"PING",
        b"ECHO",
        b"DBSIZE",
        b"FLUSHALL",
        b"COMMAND",
        b"CLUSTER",
        b"INFO",
        b"SELECT",
        b"QUIT",
        b"HELLO",
    ];
    if NO_KEY.iter().any(|c| name.eq_ignore_ascii_case(c)) {
        return None;
    }
    args.get(1).map(|k| k.as_slice())
}

/// Forward one client command, absorbing -MOVED / -TRYAGAIN / backend death
/// within the retry budget. Returns the reply the CLIENT should see.
fn forward(topo: &Topology, backends: &mut Backends, args: &[Vec<u8>], frame: &[u8]) -> Value {
    let slot = route_key(args).map(slot_for_key);
    let deadline = Instant::now() + RETRY_BUDGET;
    loop {
        // Resolve the target: keyed commands by slot; no-key commands go to
        // pair 0's master (v0: DBSIZE/FLUSHALL fan out below, before here).
        let target = match slot {
            Some(s) => topo.route(s),
            None => topo
                .masters
                .read()
                .ok()
                .and_then(|m| m.first().cloned().flatten()),
        };
        let Some(addr) = target else {
            if Instant::now() > deadline {
                return Value::Error("ERR no reachable master for this slot".into());
            }
            // Nothing known yet (e.g. mid-failover): rediscover everything.
            for nodes in &topo.pairs {
                let found = discover_master(nodes);
                if let Ok(mut masters) = topo.masters.write() {
                    for (i, p) in topo.pairs.iter().enumerate() {
                        if std::ptr::eq(p, nodes)
                            && let Some(m) = masters.get_mut(i)
                        {
                            *m = found.clone();
                        }
                    }
                }
            }
            std::thread::sleep(Duration::from_millis(100));
            continue;
        };

        match backends.call(&addr, frame) {
            Ok(Value::Error(e)) if e.starts_with("MOVED ") => {
                // "MOVED <slot> <addr>": learn and chase. The client must
                // never see this — absorbing it is the proxy's reason to
                // exist.
                let mut parts = e.split(' ');
                let (_, s, new_addr) = (parts.next(), parts.next(), parts.next());
                if let (Some(s), Some(new_addr)) = (s, new_addr)
                    && let Ok(s) = s.parse::<u16>()
                {
                    topo.learn_moved(s, new_addr);
                }
                if Instant::now() > deadline {
                    return Value::Error("ERR routing did not settle (moved chase)".into());
                }
            }
            Ok(Value::Error(e)) if e.starts_with("TRYAGAIN") => {
                // Slot frozen mid-cutover: the drain is sub-second; wait it
                // out inside the budget instead of bothering the client.
                if Instant::now() > deadline {
                    return Value::Error(e);
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Ok(reply) => return reply,
            Err(_) => {
                // Backend died (or timed out): rediscover that pair's master
                // and retry — the failover-chasing path.
                if Instant::now() > deadline {
                    return Value::Error(
                        "ERR backend unavailable (failover did not settle)".into(),
                    );
                }
                topo.rediscover_for(&addr);
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// Fan a command out to every pair's master; combine with `combine`.
fn fan_out(
    topo: &Topology,
    backends: &mut Backends,
    frame: &[u8],
    combine: impl Fn(Vec<Value>) -> Value,
) -> Value {
    let mut replies = Vec::new();
    for i in 0..topo.pairs.len() {
        let addr = {
            let Ok(masters) = topo.masters.read() else {
                return Value::Error("ERR topology lock".into());
            };
            masters.get(i).cloned().flatten()
        };
        let Some(addr) = addr else {
            return Value::Error("ERR a pair has no reachable master".into());
        };
        match backends.call(&addr, frame) {
            Ok(v) => replies.push(v),
            Err(e) => return Value::Error(format!("ERR fan-out to {addr}: {e}")),
        }
    }
    combine(replies)
}

fn serve_client(mut stream: TcpStream, topo: Arc<Topology>) -> std::io::Result<()> {
    let mut backends = Backends::new();
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut chunk = [0u8; 16 * 1024];
    let mut out: Vec<u8> = Vec::with_capacity(4 * 1024);
    loop {
        let mut consumed = 0;
        out.clear();
        loop {
            match decode(&buf[consumed..]) {
                Ok(Decoded::Complete(frame, used)) => {
                    let raw = buf[consumed..consumed + used].to_vec();
                    consumed += used;
                    let Some(args) = frame_to_args(frame) else {
                        encode(
                            &Value::Error(
                                "ERR Protocol error: expected array of bulk strings".into(),
                            ),
                            &mut out,
                        );
                        stream.write_all(&out)?;
                        return Ok(());
                    };
                    let reply = handle(&topo, &mut backends, &args, &raw);
                    encode(&reply, &mut out);
                }
                Ok(Decoded::NeedMore) => break,
                Err(_) => {
                    encode(&Value::Error("ERR Protocol error".into()), &mut out);
                    stream.write_all(&out)?;
                    return Ok(());
                }
            }
        }
        if consumed > 0 {
            buf.drain(..consumed);
            if !out.is_empty() {
                stream.write_all(&out)?;
            }
        }
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn handle(topo: &Topology, backends: &mut Backends, args: &[Vec<u8>], raw: &[u8]) -> Value {
    let Some(name) = args.first() else {
        return Value::Error("ERR empty command".into());
    };
    let upper = name.to_ascii_uppercase();
    match upper.as_slice() {
        // Connection-level commands are answered locally: they concern the
        // client<->proxy hop, not any backend.
        b"PING" => match args.len() {
            1 => Value::Simple("PONG".into()),
            2 => Value::Bulk(Some(args[1].clone())),
            _ => Value::Error("ERR wrong number of arguments for 'ping' command".into()),
        },
        b"ECHO" if args.len() == 2 => Value::Bulk(Some(args[1].clone())),
        b"QUIT" => Value::Simple("OK".into()),
        // The data-plane admin surface stays internal; the proxy is the
        // tenant boundary.
        _ if upper.starts_with(b"FLINT") => {
            Value::Error("ERR admin commands are not available through the proxy".into())
        }
        // Group-wide aggregates fan out.
        b"DBSIZE" => fan_out(topo, backends, raw, |replies| {
            let mut total = 0i64;
            for r in replies {
                match r {
                    Value::Integer(n) => total += n,
                    other => return Value::Error(format!("ERR dbsize fan-out: {other:?}")),
                }
            }
            Value::Integer(total)
        }),
        b"FLUSHALL" => fan_out(topo, backends, raw, |replies| {
            for r in replies {
                if !matches!(&r, Value::Simple(s) if s == "OK") {
                    return Value::Error(format!("ERR flushall fan-out: {r:?}"));
                }
            }
            Value::Simple("OK".into())
        }),
        _ => forward(topo, backends, args, raw),
    }
}

fn frame_to_args(frame: Value) -> Option<Vec<Vec<u8>>> {
    let Value::Array(Some(items)) = frame else {
        return None;
    };
    let mut args = Vec::with_capacity(items.len());
    for item in items {
        let Value::Bulk(Some(bytes)) = item else {
            return None;
        };
        args.push(bytes);
    }
    Some(args)
}

fn main() -> std::io::Result<()> {
    let port: u16 = arg("--port").and_then(|p| p.parse().ok()).unwrap_or(7379);
    let pairs: Vec<Vec<String>> = arg("--pairs")
        .expect("--pairs \"m0,r0;m1,r1\" required")
        .split(';')
        .map(|p| p.split(',').map(String::from).collect())
        .collect();
    assert!(!pairs.is_empty(), "need at least one pair");

    let masters: Vec<Option<String>> = pairs.iter().map(|nodes| discover_master(nodes)).collect();
    eprintln!(
        "flint-proxy: {} pair(s), masters {:?}",
        pairs.len(),
        masters
    );
    let topo = Arc::new(Topology {
        pairs,
        masters: RwLock::new(masters),
        moved: RwLock::new(HashMap::new()),
    });

    let listener = TcpListener::bind(("127.0.0.1", port))?;
    eprintln!("flint-proxy listening on 127.0.0.1:{port}");
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let topo = Arc::clone(&topo);
        std::thread::spawn(move || {
            let _ = serve_client(stream, topo);
        });
    }
    Ok(())
}
