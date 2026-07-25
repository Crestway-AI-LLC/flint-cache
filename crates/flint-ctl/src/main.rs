// SPDX-License-Identifier: Elastic-2.0
//! flintctl — inventory-driven cluster lifecycle (roadmap M4, operability).
//!
//! One file describes the cluster; flintctl makes it so:
//!
//!   flintctl -f cluster.flint bootstrap    certs -> CP -> registry -> nodes
//!                                          -> proxies -> controller -> agent
//!   flintctl -f cluster.flint status       roles/lag/liveness table
//!   flintctl -f cluster.flint tenant add <name> <token> <ns> [k]
//!   flintctl -f cluster.flint expand <a,b[,c]>      new pair + reroll ctl
//!   flintctl -f cluster.flint swap-node <bad> <new> fresh replica + CPSETPAIR
//!   flintctl -f cluster.flint failover <node>       graceful master handoff
//!   flintctl -f cluster.flint decommission-node <a> drop one member from a pair
//!   flintctl -f cluster.flint stop          kill everything it started
//!
//! Inventory (line-based, # comments):
//!   statedir /tmp/flint-prod      logs/, data dirs, certs, CP state, pids
//!   bins ./target/release         where the flint binaries live
//!   tls on                        mint an internal CA; every hop mutual TLS
//!   cp 127.0.0.1:7500             one line = single-node CP (3 = Raft, later)
//!   pair 127.0.0.1:6400,127.0.0.1:6401
//!   proxy 127.0.0.1:7379
//!   controller on
//!   agent 127.0.0.1:9464          shadow agent; the addr's port = metrics port
//!
//! Operator tunables (all OPTIONAL; absent = the binary's compiled default).
//! Edit the file, then `flintctl reload` to push the HOT knobs to the
//! running fleet (no restart), or `stop`/`start` for the restart-only ones.
//! No rebuild, no redeploy either way.
//!   wal-fsync-ms 500      node  HOT   WAL fsync cadence (host-loss RPO)
//!   lag-soft-ms 500       node  HOT   soft lag cap (delays writes)
//!   lag-hard-ms 1000      node  HOT   hard lag cap (sheds; RPO bound)
//!   min-replicas 1        node  HOT   min-replicas-to-write gate
//!   max-conns 10000       node  HOT   connection admission cap
//!   async-queue-cap 4096  node  restart  async write-queue depth
//!   cache-ttl-ms 300      proxy HOT   near-cache TTL (via PROXYCACHE)
//!   cache-max-bytes N     proxy HOT   near-cache byte budget
//!   poll-ms 200           ctlr  restart  failure-probe interval (RTO)
//!   confirm 3             ctlr  restart  consecutive fails before promote
//!   lease-ttl-ms 3000     ctlr  restart  master lease TTL
//!
//! v1 runs LOCAL processes (spawn-a-fresh-node model); a remote runner slots
//! in behind the same command surface later. Two properties this tool leans
//! on by design: controllers are STATELESS (ADR-0004) so `expand` simply
//! restarts the controller with the new pair list — a non-event; and pair
//! IDs are the stable identity while membership floats, so `swap-node` is
//! CPSETPAIR after the replacement converges.

use std::io::{Read, Write};
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flint_resp::{Decoded, Value, decode, encode};

// ---------- inventory ----------

#[derive(Debug, Clone, Default)]
struct Inventory {
    statedir: String,
    bins: String,
    tls: bool,
    cp: Vec<String>,
    pairs: Vec<Vec<String>>,
    proxies: Vec<String>,
    controller: bool,
    agent: Option<String>,
    /// Per-node storage capacity in bytes (capacity model, question 2);
    /// passed to the agent so it can compute fill + expansion ETAs.
    capacity_bytes: Option<u64>,
    /// Proxy admin token (`--admin-token` on every proxy; presented before
    /// PROXY* operator commands). None = ungated dev fleet.
    admin_token: Option<String>,
    /// Client-facing TLS at the proxy (ADR-0006 D2 packaging default):
    /// bootstrap mints an EDGE cert (IP/localhost SANs, same CA) and passes
    /// --tls-cert/--tls-key; tenants verify with the cluster CA.
    client_tls: bool,
    /// Extra subjectAltNames for the EDGE cert (`edge-san <ip-or-dns>`,
    /// repeatable): the addresses real clients dial — an instance's
    /// private/public IP, a DNS endpoint. localhost/127.0.0.1 are always
    /// included.
    edge_sans: Vec<String>,
    /// Operator TUNABLES, externalized so they change with a config edit +
    /// restart — no rebuild, no redeploy. All optional; an absent key leaves
    /// the binary's compiled default. flintctl threads each into the
    /// spawned process's flags at bootstrap/start, so `stop` + edit +
    /// `start` applies new values.
    wal_fsync_ms: Option<u64>, // node: WAL fsync cadence (host-loss bound)
    lag_soft_ms: Option<u64>,      // node: replication soft lag cap (delay)
    lag_hard_ms: Option<u64>,      // node: replication hard lag cap (RPO shed)
    min_replicas: Option<u32>,     // node: min-replicas-to-write safety gate
    max_conns: Option<u64>,        // node + proxy: connection admission cap
    async_queue_cap: Option<u64>,  // node: async write-queue depth
    cache_ttl_ms: Option<u64>,     // proxy: near-cache TTL default
    cache_max_bytes: Option<u64>,  // proxy: near-cache byte budget default
    ctl_poll_ms: Option<u64>,      // controller: failure-probe interval (RTO)
    ctl_confirm: Option<u32>,      // controller: consecutive fails to promote
    ctl_lease_ttl_ms: Option<u64>, // controller: master lease TTL
}

fn parse_inventory(path: &str) -> Inventory {
    let raw =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read inventory {path}: {e}"));
    let mut inv = Inventory {
        bins: "./target/release".into(),
        ..Default::default()
    };
    for line in raw.lines() {
        let line = line.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let (key, val) = line.split_once(' ').unwrap_or((line, ""));
        let val = val.trim();
        match key {
            "statedir" => inv.statedir = val.to_string(),
            "bins" => inv.bins = val.to_string(),
            "tls" => inv.tls = val == "on",
            "cp" => inv.cp.push(val.to_string()),
            "pair" => inv.pairs.push(val.split(',').map(String::from).collect()),
            "proxy" => inv.proxies.push(val.to_string()),
            "controller" => inv.controller = val == "on",
            "agent" => inv.agent = Some(val.to_string()),
            "capacity" => inv.capacity_bytes = val.parse().ok(),
            "admin-token" => inv.admin_token = Some(val.to_string()),
            "client-tls" => inv.client_tls = val == "on",
            "edge-san" => inv.edge_sans.push(val.to_string()),
            // Operator tunables (see the Inventory struct).
            "wal-fsync-ms" => inv.wal_fsync_ms = val.parse().ok(),
            "lag-soft-ms" => inv.lag_soft_ms = val.parse().ok(),
            "lag-hard-ms" => inv.lag_hard_ms = val.parse().ok(),
            "min-replicas" => inv.min_replicas = val.parse().ok(),
            "max-conns" => inv.max_conns = val.parse().ok(),
            "async-queue-cap" => inv.async_queue_cap = val.parse().ok(),
            "cache-ttl-ms" => inv.cache_ttl_ms = val.parse().ok(),
            "cache-max-bytes" => inv.cache_max_bytes = val.parse().ok(),
            "poll-ms" => inv.ctl_poll_ms = val.parse().ok(),
            "confirm" => inv.ctl_confirm = val.parse().ok(),
            "lease-ttl-ms" => inv.ctl_lease_ttl_ms = val.parse().ok(),
            other => panic!("inventory: unknown key {other:?}"),
        }
    }
    assert!(
        !inv.statedir.is_empty(),
        "inventory needs `statedir <path>`"
    );
    assert!(
        !inv.cp.is_empty(),
        "inventory needs at least one `cp <addr>`"
    );
    assert_eq!(
        inv.cp.len(),
        1,
        "multi-node CP bootstrap is the HA follow-on"
    );
    assert!(
        !inv.pairs.is_empty(),
        "inventory needs at least one `pair a,b`"
    );
    inv
}

fn port_of(addr: &str) -> u16 {
    addr.rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .unwrap_or_else(|| panic!("bad addr {addr}"))
}

// ---------- RESP admin client (mTLS-aware — valkey-cli can't do the mesh) ----------

fn tls_client(inv: &Inventory) -> Option<Arc<flint_tls::ClientConfig>> {
    if !inv.tls {
        return None;
    }
    let d = &inv.statedir;
    Some(
        flint_tls::client_config(
            &format!("{d}/certs/ca.crt"),
            &format!("{d}/certs/int.crt"),
            &format!("{d}/certs/int.key"),
        )
        .expect("load internal certs (bootstrap mints them)"),
    )
}

fn call(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    args: &[&str],
) -> std::io::Result<Value> {
    call_to(addr, tls, args, Duration::from_millis(1500))
}

/// Like `call` but with a caller-chosen read timeout — for slot migration,
/// which streams a whole slot and can exceed the default.
fn call_slow(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    args: &[&str],
    read_timeout: Duration,
) -> std::io::Result<Value> {
    call_to(addr, tls, args, read_timeout)
}

/// Run a SEQUENCE of commands on ONE connection, returning the last reply.
/// Needed wherever an earlier command sets connection state a later one
/// depends on (FLINTNS pins the namespace; FLUSHALL then wipes THAT ns).
fn call_seq(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    cmds: &[&[&str]],
    read_timeout: Duration,
) -> std::io::Result<Value> {
    let mut s = flint_tls::connect(addr, tls)?;
    s.set_read_timeout(Some(read_timeout))?;
    s.set_write_timeout(Some(Duration::from_millis(1500)))?;
    let mut last = Value::Simple(String::new());
    let mut buf = Vec::new();
    let mut chunk = [0u8; 16384];
    for args in cmds {
        let frame = Value::Array(Some(
            args.iter()
                .map(|a| Value::Bulk(Some(a.as_bytes().to_vec())))
                .collect(),
        ));
        let mut out = Vec::new();
        encode(&frame, &mut out);
        s.write_all(&out)?;
        loop {
            match decode(&buf) {
                Ok(Decoded::Complete(v, used)) => {
                    buf.drain(..used);
                    if let Value::Error(e) = &v {
                        return Err(std::io::Error::other(e.clone()));
                    }
                    last = v;
                    break;
                }
                Ok(Decoded::NeedMore) => {
                    let n = s.read(&mut chunk)?;
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
    Ok(last)
}

fn call_to(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    args: &[&str],
    read_timeout: Duration,
) -> std::io::Result<Value> {
    let mut s = flint_tls::connect(addr, tls)?;
    s.set_read_timeout(Some(read_timeout))?;
    s.set_write_timeout(Some(Duration::from_millis(1500)))?;
    let frame = Value::Array(Some(
        args.iter()
            .map(|a| Value::Bulk(Some(a.as_bytes().to_vec())))
            .collect(),
    ));
    let mut out = Vec::new();
    encode(&frame, &mut out);
    s.write_all(&out)?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 16384];
    loop {
        match decode(&buf) {
            Ok(Decoded::Complete(v, _)) => return Ok(v),
            Ok(Decoded::NeedMore) => {
                let n = s.read(&mut chunk)?;
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

fn wait_pong(addr: &str, tls: &Option<Arc<flint_tls::ClientConfig>>, budget: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < budget {
        if matches!(call(addr, tls, &["PING"]), Ok(Value::Simple(s)) if s == "PONG") {
            return true;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    false
}

fn info_field(
    addr: &str,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    field: &str,
) -> Option<String> {
    let Ok(Value::Bulk(Some(raw))) = call(addr, tls, &["FLINTINFO"]) else {
        return None;
    };
    String::from_utf8_lossy(&raw)
        .split(['\r', '\n'])
        .find(|l| l.starts_with(field))
        .map(|l| l.trim_start_matches(field).trim().to_string())
}

// ---------- process runner (local; pidfiles for stop/status) ----------

// flintctl is a short-lived CLI: spawned fleet processes deliberately
// OUTLIVE it (pidfiles are the handle; `stop` reaps by pid). There is no
// parent left to wait() — macOS/launchd reparents them — so the zombie
// lint does not apply to this design.
#[allow(clippy::zombie_processes)]
fn spawn_env(inv: &Inventory, name: &str, bin: &str, args: &[String], envs: &[(String, String)]) {
    let d = &inv.statedir;
    let log = std::fs::File::create(format!("{d}/logs/{name}.log")).expect("log file");
    let mut cmd = Command::new(format!("{}/{bin}", inv.bins));
    cmd.args(args);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    let child = cmd
        .stdout(std::process::Stdio::null())
        .stderr(log)
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {bin} ({name}): {e}"));
    std::fs::write(format!("{d}/pids/{name}.pid"), child.id().to_string()).expect("pidfile");
    eprintln!("  started {name} (pid {})", child.id());
}

fn spawn(inv: &Inventory, name: &str, bin: &str, args: &[String]) {
    spawn_env(inv, name, bin, args, &[]);
}

fn kill_pidfile(dir: &str, name: &str) {
    let path = format!("{dir}/pids/{name}.pid");
    if let Ok(pid) = std::fs::read_to_string(&path) {
        let _ = Command::new("kill").args(["-9", pid.trim()]).status();
        let _ = std::fs::remove_file(&path);
    }
}

fn internal_args(inv: &Inventory) -> Vec<String> {
    if !inv.tls {
        return Vec::new();
    }
    let d = &inv.statedir;
    vec![
        "--internal-ca".into(),
        format!("{d}/certs/ca.crt"),
        "--internal-cert".into(),
        format!("{d}/certs/int.crt"),
        "--internal-key".into(),
        format!("{d}/certs/int.key"),
    ]
}

/// `flintctl reload`: re-read the config file and PUSH the hot-reloadable
/// tunables to the RUNNING fleet — no restart. Node knobs go over the mesh
/// via FLINTCONFIG; proxy near-cache via PROXYCACHE. Restart-only knobs
/// (async-queue-cap, controller timing) are reported as needing stop/start.
/// A node that doesn't answer is warned, not fatal — reload is best-effort
/// convergence, `status` shows the truth.
fn reload(inv: &Inventory) {
    let tls = tls_client(inv);
    let node_kv: [(&str, Option<String>); 5] = [
        ("wal-fsync-ms", inv.wal_fsync_ms.map(|v| v.to_string())),
        ("lag-soft-ms", inv.lag_soft_ms.map(|v| v.to_string())),
        ("lag-hard-ms", inv.lag_hard_ms.map(|v| v.to_string())),
        (
            "min-replicas-to-write",
            inv.min_replicas.map(|v| v.to_string()),
        ),
        ("max-conns", inv.max_conns.map(|v| v.to_string())),
    ];
    for pair in &inv.pairs {
        for node in pair {
            for (k, v) in node_kv.iter() {
                if let Some(val) = v {
                    match call(node, &tls, &["FLINTCONFIG", k, val]) {
                        Ok(Value::Simple(_)) => println!("  {node}: {k}={val}"),
                        other => eprintln!("  {node}: FLINTCONFIG {k} rejected: {other:?}"),
                    }
                }
            }
        }
    }
    if inv.cache_ttl_ms.is_some() || inv.cache_max_bytes.is_some() {
        // PROXYCACHE needs both; the compiled defaults fill an unset one.
        let ttl = inv.cache_ttl_ms.unwrap_or(300).to_string();
        let maxb = inv.cache_max_bytes.unwrap_or(256 * 1024 * 1024).to_string();
        for proxy in &inv.proxies {
            if let Some(tok) = &inv.admin_token {
                let _ = call(proxy, &tls, &["AUTH", tok]);
            }
            match call(proxy, &tls, &["PROXYCACHE", &ttl, &maxb]) {
                Ok(Value::Simple(_)) => println!("  {proxy}: cache ttl={ttl}ms max={maxb}B"),
                other => eprintln!("  {proxy}: PROXYCACHE rejected: {other:?}"),
            }
        }
    }
    let mut restart_only = Vec::new();
    if inv.async_queue_cap.is_some() {
        restart_only.push("async-queue-cap");
    }
    if inv.ctl_poll_ms.is_some() || inv.ctl_confirm.is_some() || inv.ctl_lease_ttl_ms.is_some() {
        restart_only.push("controller timing (poll-ms/confirm/lease-ttl-ms)");
    }
    if !restart_only.is_empty() {
        println!(
            "note: {} not hot-reloadable — apply with stop/start",
            restart_only.join(", ")
        );
    }
    println!("== reload complete (hot knobs pushed to the running fleet)");
}

/// Node (flint-server) operator tunables from the inventory — durability,
/// RPO, and admission knobs. Absent keys leave the binary's default, so
/// this never changes behaviour it wasn't explicitly told to.
fn node_tuning_args(inv: &Inventory) -> Vec<String> {
    let mut a = Vec::new();
    let mut push = |flag: &str, v: String| a.extend([flag.to_string(), v]);
    if let Some(v) = inv.wal_fsync_ms {
        push("--wal-fsync-ms", v.to_string());
    }
    if let Some(v) = inv.lag_soft_ms {
        push("--lag-soft-ms", v.to_string());
    }
    if let Some(v) = inv.lag_hard_ms {
        push("--lag-hard-ms", v.to_string());
    }
    if let Some(v) = inv.min_replicas {
        push("--min-replicas-to-write", v.to_string());
    }
    if let Some(v) = inv.max_conns {
        push("--max-conns", v.to_string());
    }
    if let Some(v) = inv.async_queue_cap {
        push("--async-queue-cap", v.to_string());
    }
    a
}

/// Proxy operator tunables: near-cache defaults + admission.
fn proxy_tuning_args(inv: &Inventory) -> Vec<String> {
    let mut a = Vec::new();
    let mut push = |flag: &str, v: String| a.extend([flag.to_string(), v]);
    if let Some(v) = inv.cache_ttl_ms {
        push("--cache-ttl-ms", v.to_string());
    }
    if let Some(v) = inv.cache_max_bytes {
        push("--cache-max-bytes", v.to_string());
    }
    if let Some(v) = inv.max_conns {
        push("--max-conns", v.to_string());
    }
    a
}

/// Wait until the controller journals `Supervised` (auto-failover armed)
/// for every pair, with events newer than `since_ms`. "Operation complete"
/// from flintctl MEANS the fleet is supervised again — a freshly rerolled
/// controller refuses to auto-promote a pair it has not yet observed
/// converged (the degraded-window gate), so returning earlier would leave a
/// window where a master kill goes unanswered.
fn wait_supervised(inv: &Inventory, n_pairs: usize, since_ms: u64) {
    let tls = tls_client(inv);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let mut armed = std::collections::HashSet::new();
        if let Ok(Value::Bulk(Some(raw))) = call(&inv.cp[0], &tls, &["CPJOURNALREAD", "500"]) {
            for line in String::from_utf8_lossy(&raw).lines() {
                if line.contains("\"kind\":\"Supervised\"")
                    && let Some(at) = line
                        .split("\"at_ms\":")
                        .nth(1)
                        .and_then(|r| r.split(',').next())
                        .and_then(|v| v.trim().parse::<u64>().ok())
                    && at >= since_ms
                    && let Some(subj) = line
                        .split("\"subject\":\"")
                        .nth(1)
                        .and_then(|r| r.split('"').next())
                {
                    armed.insert(subj.to_string());
                }
            }
        }
        if (0..n_pairs).all(|i| armed.contains(&format!("g{i}"))) {
            eprintln!("  supervision armed for {n_pairs} pair(s)");
            return;
        }
        if Instant::now() > deadline {
            eprintln!(
                "  WARNING: supervision not confirmed within 30s (journal may be unavailable); auto-failover may lag"
            );
            return;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------- commands ----------

fn mint_certs(inv: &Inventory) {
    let d = format!("{}/certs", inv.statedir);
    std::fs::create_dir_all(&d).expect("certs dir");
    let sh = |cmd: &str| {
        let ok = Command::new("sh")
            .args(["-c", cmd])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(ok, "cert step failed: {cmd}");
    };
    sh(&format!(
        "openssl req -x509 -newkey rsa:2048 -nodes -keyout {d}/ca.key -out {d}/ca.crt -days 365 \
         -subj /CN=flint-internal-ca -addext basicConstraints=critical,CA:TRUE 2>/dev/null"
    ));
    resign_leaves(&d, &sh, &inv.edge_sans);
    eprintln!("  minted internal CA + component cert + edge cert");
}

/// (Re-)sign the LEAF certs — the mesh cert (int) and the edge cert — from
/// the existing CA, fresh keypairs each time. Used at bootstrap and by
/// `rotate-certs` (ADR-0006 D4): overwriting int.crt/int.key in place makes
/// each component's hot-reload watcher pick up the new leaf, no restart. The
/// CA is untouched, so old and new leaves both verify during the roll.
fn resign_leaves(d: &str, sh: &dyn Fn(&str), edge_sans: &[String]) {
    sh(&format!(
        "openssl req -newkey rsa:2048 -nodes -keyout {d}/int.key -out {d}/int.csr \
         -subj /CN=flint-internal 2>/dev/null"
    ));
    sh(&format!(
        "printf 'subjectAltName=DNS:flint-internal\\nextendedKeyUsage=serverAuth,clientAuth\\nbasicConstraints=CA:FALSE' > {d}/ext.cnf && \
         openssl x509 -req -in {d}/int.csr -CA {d}/ca.crt -CAkey {d}/ca.key -CAcreateserial \
         -out {d}/int.crt -days 365 -extfile {d}/ext.cnf 2>/dev/null"
    ));
    sh(&format!(
        "openssl req -newkey rsa:2048 -nodes -keyout {d}/edge.key -out {d}/edge.csr \
         -subj /CN=flint-edge 2>/dev/null"
    ));
    sh(&format!(
        "printf 'subjectAltName={sans}\nextendedKeyUsage=serverAuth\nbasicConstraints=CA:FALSE' > {d}/edge-ext.cnf && \
         openssl x509 -req -in {d}/edge.csr -CA {d}/ca.crt -CAkey {d}/ca.key -CAcreateserial \
         -out {d}/edge.crt -days 365 -extfile {d}/edge-ext.cnf 2>/dev/null",
        sans = edge_san_list(edge_sans),
    ));
}

/// The edge cert's SAN list: loopback always, plus every `edge-san` entry —
/// IPs as IP: entries, everything else as DNS:.
fn edge_san_list(extra: &[String]) -> String {
    let mut sans = vec!["IP:127.0.0.1".to_string(), "DNS:localhost".to_string()];
    for e in extra {
        let e = e.trim();
        if e.is_empty() {
            continue;
        }
        if e.parse::<std::net::IpAddr>().is_ok() {
            sans.push(format!("IP:{e}"));
        } else {
            sans.push(format!("DNS:{e}"));
        }
    }
    sans.join(",")
}

/// `flintctl rotate-certs`: re-sign the leaf certs from the CA in place; the
/// running components' TLS watchers reload within a poll. No restart, no CA
/// change (CA rotation stays a runbook).
fn rotate_certs(inv: &Inventory) {
    let d = format!("{}/certs", inv.statedir);
    assert!(
        std::path::Path::new(&format!("{d}/ca.crt")).exists(),
        "no CA at {d}/ca.crt — is this a `tls on` cluster?"
    );
    let sh = |cmd: &str| {
        let ok = Command::new("sh")
            .args(["-c", cmd])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        assert!(ok, "cert step failed: {cmd}");
    };
    resign_leaves(&d, &sh, &inv.edge_sans);
    println!("re-signed mesh + edge leaf certs from the CA; components hot-reload within ~2s");
}

/// Proxy liveness that respects the front door: plaintext fleets answer
/// PROXYSTATS; a client-TLS fleet is probed at the TCP layer (flintctl is
/// not an edge client — real encrypted traffic is verified by the drills).
fn proxy_up(inv: &Inventory, proxy: &str) -> bool {
    if inv.client_tls {
        return std::net::TcpStream::connect(proxy).is_ok();
    }
    // PROXYSTATS answers a Bulk when ungated; once the CP pushes an admin
    // digest (ADR-0006 D4) it answers -NOAUTH pre-auth. EITHER proves the
    // proxy is up and serving — only connect failure means down.
    match call(proxy, &None, &["PROXYSTATS"]) {
        Ok(Value::Bulk(_)) => true,
        Ok(Value::Error(e)) => e.starts_with("NOAUTH"),
        _ => false,
    }
}

fn start_pair_nodes(inv: &Inventory, pair: &[String]) {
    let d = &inv.statedir;
    let cp = &inv.cp[0];
    for (i, addr) in pair.iter().enumerate() {
        let port = port_of(addr);
        let mut args = vec![
            "--port".to_string(),
            port.to_string(),
            "--engine".into(),
            "rocks".into(),
            "--data-dir".into(),
            format!("{d}/node-{port}"),
            "--journal".into(),
            cp.clone(),
        ];
        if i > 0 {
            args.extend(["--replica-of".into(), pair[0].clone()]);
        }
        args.extend(internal_args(inv));
        args.extend(node_tuning_args(inv));
        spawn(inv, &format!("node-{port}"), "flint-server", &args);
        // The master must be up before its replicas dial in.
        if i == 0 {
            std::thread::sleep(Duration::from_millis(700));
        }
    }
}

fn start_controller(inv: &Inventory) {
    let pairs_spec = inv
        .pairs
        .iter()
        .map(|p| p.join(","))
        .collect::<Vec<_>>()
        .join(";");
    let d = &inv.statedir;
    let mut args = vec![
        "--pairs".to_string(),
        pairs_spec,
        "--id".into(),
        "ctl".into(),
        // RTO timing knobs — inventory-tunable (config edit + restart).
        "--poll-ms".into(),
        inv.ctl_poll_ms.unwrap_or(200).to_string(),
        "--confirm".into(),
        inv.ctl_confirm.unwrap_or(3).to_string(),
        "--journal".into(),
        inv.cp[0].clone(),
        "--snapshot-root".into(),
        format!("{d}/snaps"),
        // Option B: cutovers commit ownership truth to the CP.
        "--commit-cp".into(),
        inv.cp[0].clone(),
    ];
    if let Some(v) = inv.ctl_lease_ttl_ms {
        args.extend(["--lease-ttl-ms".into(), v.to_string()]);
    }
    args.extend(internal_args(inv));
    spawn(inv, "controller", "flint-controller", &args);
}

/// First-time bring-up: mint certs, spawn, REGISTER the topology.
fn bootstrap(inv: &Inventory) {
    launch(inv, true);
}

/// Reboot path (idempotent boot, marketplace first-boot re-runs): spawn
/// every process against EXISTING state — no cert minting, no registry
/// writes (CPADDPAIR/CPADDPROXY append; re-running them would duplicate
/// topology). The CP state file, node data dirs, and certs carry the
/// fleet's memory.
fn start(inv: &Inventory) {
    launch(inv, false);
}

fn launch(inv: &Inventory, register: bool) {
    let d = &inv.statedir;
    for sub in ["logs", "pids", "snaps"] {
        std::fs::create_dir_all(format!("{d}/{sub}")).expect("statedir");
    }
    eprintln!(
        "== {} into {d} (tls {})",
        if register { "bootstrap" } else { "start" },
        if inv.tls { "on" } else { "off" }
    );
    if register && inv.tls {
        mint_certs(inv);
    }
    let tls = tls_client(inv);

    // 1. Control plane, then the registry (proxies + pairs).
    let cp = &inv.cp[0];
    let mut cp_args = vec![
        "--port".to_string(),
        port_of(cp).to_string(),
        "--state".into(),
        format!("{d}/cp-state"),
    ];
    cp_args.extend(internal_args(inv));
    if let Some(tok) = &inv.admin_token {
        // ADR-0006 D4: the admin token lives in the CP and is pushed to
        // proxies as a digest; rotate it later with `flintctl rotate-admin`.
        cp_args.extend(["--admin-token".to_string(), tok.clone()]);
    }
    spawn(inv, "cp", "flint-controlplane", &cp_args);
    assert!(
        wait_pong(cp, &tls, Duration::from_secs(10)),
        "control plane up"
    );
    if register {
        for proxy in &inv.proxies {
            call(cp, &tls, &["CPADDPROXY", proxy]).expect("register proxy");
        }
        // Initial pairs carry the even slot split as EXPLICIT level-1
        // routing state; expansion pairs later join with "-" (no range) so
        // capacity never re-routes unmigrated slots.
        let n = inv.pairs.len();
        for (i, pair) in inv.pairs.iter().enumerate() {
            let start = i * 16384 / n;
            let end = (i + 1) * 16384 / n - 1;
            call(
                cp,
                &tls,
                &["CPADDPAIR", &pair.join(","), &format!("{start}-{end}")],
            )
            .expect("register pair");
        }
        eprintln!(
            "  registry: {} proxies, {} pairs",
            inv.proxies.len(),
            inv.pairs.len()
        );
    }

    // 2. Data plane.
    for pair in &inv.pairs {
        start_pair_nodes(inv, pair);
    }
    for pair in &inv.pairs {
        assert!(
            wait_pong(&pair[0], &tls, Duration::from_secs(10)),
            "master {} up",
            pair[0]
        );
    }

    // 3. Routing plane.
    for proxy in &inv.proxies {
        // The inventory addr's HOST is the bind address (0.0.0.0 serves
        // external clients — the marketplace shape; 127.0.0.1 stays the
        // loopback default).
        let bind_host = proxy
            .rsplit_once(':')
            .map(|(h, _)| h)
            .unwrap_or("127.0.0.1");
        let mut args = vec![
            "--port".to_string(),
            port_of(proxy).to_string(),
            "--bind".to_string(),
            bind_host.to_string(),
            "--control-plane".into(),
            cp.clone(),
            "--advertise".into(),
            proxy.clone(),
        ];
        args.extend(internal_args(inv));
        args.extend(proxy_tuning_args(inv));
        if inv.client_tls {
            // ADR-0006 D2: the packaged default is an encrypted front door.
            args.extend([
                "--tls-cert".to_string(),
                format!("{}/certs/edge.crt", inv.statedir),
                "--tls-key".to_string(),
                format!("{}/certs/edge.key", inv.statedir),
            ]);
        }
        spawn(
            inv,
            &format!("proxy-{}", port_of(proxy)),
            "flint-proxy",
            &args,
        );
    }

    for proxy in &inv.proxies {
        // Liveness probe = PROXYSTATS (answered pre-auth); a CP-fed proxy
        // replies -NOAUTH to PING until a tenant authenticates. Plaintext:
        // the client port is not part of the internal mesh.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if proxy_up(inv, proxy) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "proxy {proxy} did not come up (port busy?)"
            );
            std::thread::sleep(Duration::from_millis(150));
        }
    }

    // 4. Supervision + observability.
    let ctl_since = now_ms();
    if inv.controller {
        start_controller(inv);
    }
    // Optional fleet-agent add-on: the `agent <addr>` inventory key starts
    // a flint-agent binary if one sits in the bins dir. The agent (fleet
    // metering/insights/automation) is not part of this repository; without
    // the binary the key simply has nothing to start.
    if let Some(agent) = &inv.agent {
        let mut args = vec![
            "--control-plane".to_string(),
            cp.clone(),
            "--metrics-port".into(),
            port_of(agent).to_string(),
            "--journal".into(),
            format!("{d}/shadow.jsonl"),
        ];
        if let Some(cap) = inv.capacity_bytes {
            args.extend(["--node-capacity-bytes".into(), cap.to_string()]);
        }
        args.extend(internal_args(inv));
        spawn(inv, "agent", "flint-agent", &args);
    }
    if inv.controller {
        wait_supervised(inv, inv.pairs.len(), ctl_since);
    }
    eprintln!(
        "== {} complete",
        if register { "bootstrap" } else { "start" }
    );
    status(inv);
}

fn status(inv: &Inventory) {
    let tls = tls_client(inv);
    let cp = &inv.cp[0];
    let cp_ok = matches!(call(cp, &tls, &["PING"]), Ok(Value::Simple(s)) if s == "PONG");
    println!("cp        {cp}  {}", if cp_ok { "up" } else { "DOWN" });
    for (i, pair) in inv.pairs.iter().enumerate() {
        for addr in pair {
            match info_field(addr, &tls, "role:") {
                Some(role) => {
                    let lag = info_field(addr, &tls, "seq_lag:").unwrap_or_default();
                    let live = info_field(addr, &tls, "live_replicas:").unwrap_or_default();
                    let epoch = info_field(addr, &tls, "role_epoch:").unwrap_or_default();
                    let build = info_field(addr, &tls, "build:").unwrap_or_default();
                    println!(
                        "pair {i}    {addr}  {role:<7} epoch {epoch:<7} build {build:<10} seq_lag {lag:<5} live_replicas {live}"
                    );
                }
                None => println!("pair {i}    {addr}  DOWN"),
            }
        }
    }
    for proxy in &inv.proxies {
        // The proxy's client port is plaintext (frontend TLS is separate
        // from the internal mesh): probe it without the mesh cert.
        let up = proxy_up(inv, proxy);
        println!("proxy     {proxy}  {}", if up { "up" } else { "DOWN" });
    }
    // Optional fleet-agent add-on: the `agent <addr>` inventory key starts
    // a flint-agent binary if one sits in the bins dir. The agent (fleet
    // metering/insights/automation) is not part of this repository; without
    // the binary the key simply has nothing to start.
    if let Some(agent) = &inv.agent {
        println!("agent     metrics http://{agent}/metrics");
    }
}

fn tenant_add(inv: &Inventory, rest: &[String]) {
    let tls = tls_client(inv);
    let mut args = vec!["CPADDTENANT"];
    let strs: Vec<&str> = rest.iter().map(|s| s.as_str()).collect();
    args.extend(strs);
    match call(&inv.cp[0], &tls, &args) {
        Ok(Value::Simple(s)) => println!("{s}"),
        other => panic!("tenant add failed: {other:?}"),
    }
}

/// Remove a tenant end to end: look up its namespace, revoke the record at
/// the CP (CPDELTENANT — auth dies on the next snapshot push, and the ns's
/// slot-exception rows retire with it), then WIPE the namespace's data on
/// every pair master (FLINTNS + FLUSHALL — namespace-scoped by the tenancy
/// invariant). Data wipe is best-effort per pair and reported; re-running
/// is safe (the wipe is idempotent, the CP delete errors "no such tenant").
fn tenant_remove(inv: &Inventory, name: &str) {
    let tls = tls_client(inv);
    // Namespace lookup BEFORE deletion (the record dies with the delete).
    let ns = match call(&inv.cp[0], &tls, &["CPTENANTS"]) {
        Ok(Value::Bulk(Some(raw))) => String::from_utf8_lossy(&raw)
            .lines()
            .find_map(|l| {
                let mut it = l.split_whitespace();
                (it.next() == Some(name)).then(|| it.next().map(String::from))?
            })
            .unwrap_or_else(|| panic!("no such tenant {name:?}")),
        other => panic!("CPTENANTS failed: {other:?}"),
    };
    match call(&inv.cp[0], &tls, &["CPDELTENANT", name]) {
        Ok(Value::Simple(s)) => eprintln!("== {s}"),
        other => panic!("tenant remove failed: {other:?}"),
    }
    for i in 0..inv.pairs.len() {
        let master = pair_master(inv, &tls, &i.to_string());
        // ONE connection for both: FLINTNS pins this connection's namespace,
        // FLUSHALL then wipes exactly that namespace (a fresh connection
        // would wipe the default ns instead).
        let wiped = call_seq(
            &master,
            &tls,
            &[&["FLINTNS", &ns], &["FLUSHALL"]],
            Duration::from_secs(30),
        )
        .is_ok();
        eprintln!(
            "  pair {i} ({master}): ns {ns} {}",
            if wiped {
                "wiped"
            } else {
                "WIPE FAILED (re-run)"
            }
        );
    }
    eprintln!("== tenant {name} removed (ns {ns})");
}

fn expand(inv: &Inventory, inventory_path: &str, pair_spec: &str) {
    let tls = tls_client(inv);
    let pair: Vec<String> = pair_spec.split(',').map(String::from).collect();
    eprintln!("== expand: new pair {pair_spec}");
    start_pair_nodes(inv, &pair);
    assert!(
        wait_pong(&pair[0], &tls, Duration::from_secs(10)),
        "new master up"
    );
    // "-": the new pair owns no slots yet — capacity joins without
    // re-routing; migration (controller rebalance) moves slots later.
    call(&inv.cp[0], &tls, &["CPADDPAIR", pair_spec, "-"]).expect("register new pair");
    // Persist to the inventory, then reroll the controller with the new
    // pair list. Controllers are STATELESS (ADR-0004): restarting one is a
    // non-event — it re-derives everything from the nodes' manifests.
    let mut raw = std::fs::read_to_string(inventory_path).expect("inventory");
    raw.push_str(&format!("pair {pair_spec}\n"));
    std::fs::write(inventory_path, raw).expect("inventory update");
    let mut inv2 = inv.clone();
    inv2.pairs.push(pair);
    if inv.controller {
        let t0 = now_ms();
        kill_pidfile(&inv.statedir, "controller");
        start_controller(&inv2);
        wait_supervised(&inv2, inv2.pairs.len(), t0);
        eprintln!(
            "  controller rerolled with {} pairs (stateless: a non-event)",
            inv2.pairs.len()
        );
    }
    eprintln!("== expand complete");
}

/// Add a replica to a pair (ADR-0005 D7 topology knob): spawn a fresh
/// replica of the pair's master, wait for it to converge, then CPSETPAIR to
/// append it to the pair's membership. Pure capacity: it owns nothing new,
/// serves reads for tenants that opted into replica reads. Reroll the
/// controller so it supervises the wider member set.
fn add_replica(inv: &Inventory, inventory_path: &str, pair_ref: &str, new: &str) {
    let tls = tls_client(inv);
    // pair_ref is a pair index or any current member address.
    let pair_idx = match pair_ref.parse::<usize>() {
        Ok(i) if i < inv.pairs.len() => i,
        _ => inv
            .pairs
            .iter()
            .position(|p| p.iter().any(|a| a == pair_ref))
            .unwrap_or_else(|| panic!("{pair_ref} is not a pair index or a known member")),
    };
    let master = inv.pairs[pair_idx]
        .iter()
        .find(|a| info_field(a, &tls, "role:").as_deref() == Some("master"))
        .unwrap_or_else(|| panic!("pair {pair_idx} has no reachable master"))
        .clone();
    eprintln!("== add-replica: {new} -> pair {pair_idx} (master {master})");

    let d = &inv.statedir;
    let port = port_of(new);
    let mut args = vec![
        "--port".to_string(),
        port.to_string(),
        "--engine".into(),
        "rocks".into(),
        "--data-dir".into(),
        format!("{d}/node-{port}"),
        "--journal".into(),
        inv.cp[0].clone(),
        "--replica-of".into(),
        master.clone(),
    ];
    args.extend(internal_args(inv));
    args.extend(node_tuning_args(inv));
    spawn(inv, &format!("node-{port}"), "flint-server", &args);
    assert!(
        wait_pong(new, &tls, Duration::from_secs(15)),
        "new replica up"
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if info_field(&master, &tls, "seq_lag:").as_deref() == Some("0") {
            break;
        }
        assert!(Instant::now() < deadline, "new replica never converged");
        std::thread::sleep(Duration::from_millis(300));
    }
    eprintln!("  new replica converged (seq_lag 0)");

    let mut members = inv.pairs[pair_idx].clone();
    members.push(new.to_string());
    call(
        &inv.cp[0],
        &tls,
        &["CPSETPAIR", &pair_idx.to_string(), &members.join(",")],
    )
    .expect("CPSETPAIR");

    let raw = std::fs::read_to_string(inventory_path).expect("inventory");
    let updated = raw.replace(
        &format!("pair {}", inv.pairs[pair_idx].join(",")),
        &format!("pair {}", members.join(",")),
    );
    std::fs::write(inventory_path, updated).expect("inventory update");
    if inv.controller {
        let t0 = now_ms();
        let mut inv2 = inv.clone();
        inv2.pairs[pair_idx] = members;
        kill_pidfile(d, "controller");
        start_controller(&inv2);
        wait_supervised(&inv2, inv2.pairs.len(), t0);
    }
    eprintln!(
        "== add-replica complete: pair {pair_idx} now has an extra replica for D7 read fan-out"
    );
}

fn swap_node(inv: &Inventory, inventory_path: &str, bad: &str, new: &str) {
    let tls = tls_client(inv);
    let Some(pair_idx) = inv.pairs.iter().position(|p| p.iter().any(|a| a == bad)) else {
        panic!("{bad} is not in any pair in the inventory");
    };
    // Refuse to swap a live master out from under the pair: kill/demote it
    // first (the controller will promote the survivor), then swap the seat.
    if info_field(bad, &tls, "role:").as_deref() == Some("master") {
        panic!("{bad} is currently a live MASTER; fail it over first, then swap");
    }
    let master = inv.pairs[pair_idx]
        .iter()
        .find(|a| info_field(a, &tls, "role:").as_deref() == Some("master"))
        .unwrap_or_else(|| panic!("pair {pair_idx} has no reachable master"))
        .clone();
    eprintln!("== swap: {bad} -> {new} (pair {pair_idx}, master {master})");

    // Fresh replica on the replacement seat (spawn-a-fresh-node model).
    let d = &inv.statedir;
    let port = port_of(new);
    let mut args = vec![
        "--port".to_string(),
        port.to_string(),
        "--engine".into(),
        "rocks".into(),
        "--data-dir".into(),
        format!("{d}/node-{port}"),
        "--journal".into(),
        inv.cp[0].clone(),
        "--replica-of".into(),
        master.clone(),
    ];
    args.extend(internal_args(inv));
    args.extend(node_tuning_args(inv));
    spawn(inv, &format!("node-{port}"), "flint-server", &args);
    assert!(
        wait_pong(new, &tls, Duration::from_secs(15)),
        "replacement up"
    );

    // Wait for full convergence before the seat changes hands.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if info_field(&master, &tls, "seq_lag:").as_deref() == Some("0") {
            break;
        }
        assert!(Instant::now() < deadline, "replacement never converged");
        std::thread::sleep(Duration::from_millis(300));
    }
    eprintln!("  replacement converged (seq_lag 0)");

    // The pair id is the stable identity; membership floats.
    let new_members: Vec<String> = inv.pairs[pair_idx]
        .iter()
        .map(|a| if a == bad { new.to_string() } else { a.clone() })
        .collect();
    call(
        &inv.cp[0],
        &tls,
        &["CPSETPAIR", &pair_idx.to_string(), &new_members.join(",")],
    )
    .expect("CPSETPAIR");
    kill_pidfile(d, &format!("node-{}", port_of(bad)));

    // Persist + reroll the controller onto the new membership.
    let raw = std::fs::read_to_string(inventory_path).expect("inventory");
    let updated = raw.replace(
        &format!("pair {}", inv.pairs[pair_idx].join(",")),
        &format!("pair {}", new_members.join(",")),
    );
    std::fs::write(inventory_path, updated).expect("inventory update");
    if inv.controller {
        let t0 = now_ms();
        let mut inv2 = inv.clone();
        inv2.pairs[pair_idx] = new_members;
        kill_pidfile(d, "controller");
        start_controller(&inv2);
        wait_supervised(&inv2, inv2.pairs.len(), t0);
    }
    eprintln!(
        "== swap complete: pair {pair_idx} seat replaced, registry + inventory updated (supervision armed)"
    );
}

// ---------- canary upgrade ----------

fn epoch_counter(epoch: &str) -> Option<u32> {
    epoch
        .trim()
        .trim_matches(|c| c == '(' || c == ')')
        .split(',')
        .nth(1)
        .and_then(|c| c.trim().parse().ok())
}

/// Journal gate: no event of a disallowed kind since `since_ms`. The fleet
/// journal is the abort signal — an unexpected role transition mid-roll
/// means the fleet is fighting something else; stop adding variables.
fn journal_clean(inv: &Inventory, since_ms: u64, disallowed: &[&str]) -> Result<(), String> {
    let tls = tls_client(inv);
    let Ok(Value::Bulk(Some(raw))) = call(&inv.cp[0], &tls, &["CPJOURNALREAD", "500"]) else {
        // FAIL CLOSED: an upgrade must not roll blind. If the journal is
        // unreadable we cannot distinguish "quiet fleet" from "on fire".
        return Err("fleet journal unreachable — refusing to roll without the gate".into());
    };
    for line in String::from_utf8_lossy(&raw).lines() {
        let at = line
            .split("\"at_ms\":")
            .nth(1)
            .and_then(|r| r.split(',').next())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0);
        if at < since_ms {
            continue;
        }
        for kind in disallowed {
            if line.contains(&format!("\"kind\":\"{kind}\"")) {
                return Err(format!("unexpected {kind} in the fleet journal: {line}"));
            }
        }
    }
    Ok(())
}

/// Roll one node to the new build: kill, respawn on the SAME data dir (an
/// upgrade is a binary swap + warm restart, never a resync) as a replica of
/// `master`, then wait for the build stamp and full convergence.
fn roll_node(
    inv: &Inventory,
    addr: &str,
    master: &str,
    envs: &[(String, String)],
    expect_build: &Option<String>,
    wipe: bool,
) -> Result<(), String> {
    let tls = tls_client(inv);
    let d = &inv.statedir;
    let port = port_of(addr);
    // Warm restart is only trusted for a seat OBSERVED as a live replica
    // right now: a replica's history is a prefix of the master's by
    // construction, so its binary can be swapped and the tail resumes.
    // Anything else — dead seat, unknown lineage, (ex-)master — resyncs
    // fresh. (Checking after the respawn is a race against the controller
    // fencing a returning zombie; checking BEFORE the kill is not.)
    let wipe = wipe || info_field(addr, &tls, "role:").as_deref() != Some("replica");
    kill_pidfile(d, &format!("node-{port}"));
    if wipe {
        // Ex-masters NEVER warm-rejoin, even durably demoted: their
        // replication cursor is an ex-master's (not a tail position) and
        // their unreplicated suffix may have diverged from the new lineage.
        // The demote contract is wipe + checkpoint resync; flintctl knows it
        // just demoted this seat, so it applies the contract itself.
        std::thread::sleep(Duration::from_millis(300));
        let _ = std::fs::remove_dir_all(format!("{d}/node-{port}"));
    }
    let mut args = vec![
        "--port".to_string(),
        port.to_string(),
        "--engine".into(),
        "rocks".into(),
        "--data-dir".into(),
        format!("{d}/node-{port}"),
        "--journal".into(),
        inv.cp[0].clone(),
        "--replica-of".into(),
        master.to_string(),
    ];
    args.extend(internal_args(inv));
    args.extend(node_tuning_args(inv));
    spawn_env(inv, &format!("node-{port}"), "flint-server", &args, envs);
    if !wait_pong(addr, &tls, Duration::from_secs(15)) {
        return Err(format!("{addr} did not come back after the binary swap"));
    }
    if let Some(want) = expect_build {
        let got = info_field(addr, &tls, "build:").unwrap_or_default();
        if &got != want {
            return Err(format!("{addr} reports build {got:?}, expected {want:?}"));
        }
    }
    // Converged again (warm restart: the tail resumes from its own data).
    // Under LIVE write load seq_lag hovers above zero by design (writes
    // keep arriving between acks), so convergence here is sequence lag zero
    // OR time lag comfortably inside the healthy band. The master-phase
    // DRAIN check stays strictly seq_lag==0 — writes are fenced off there.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let seq0 = info_field(master, &tls, "seq_lag:").as_deref() == Some("0");
        let time_ok = info_field(master, &tls, "lag_ms:")
            .and_then(|v| v.parse::<u64>().ok())
            .is_some_and(|ms| ms < 500);
        if seq0 || time_ok {
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(format!("{addr} never reconverged behind {master}"));
        }
        std::thread::sleep(Duration::from_millis(300));
    }
}

/// The loss-critical failover core, shared by `upgrade` and `failover`:
/// fence at an epoch above everything either member has seen, then DEMOTE
/// the old master FIRST (it stops acking writes), DRAIN (its replica
/// applies every acked write), and only THEN PROMOTE the new master.
/// Demote-first is what makes it lossless — a promote-first window lets the
/// old master ack writes the new lineage never contains; the no-master gap
/// between demote and promote is absorbed by the proxy's retry budget
/// (latency, not loss). The drain also guarantees the promotion target is
/// caught up, so a lagging replica never freezes an incomplete dataset.
/// Returns the promoted epoch counter.
fn controlled_failover(
    inv: &Inventory,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    pair: &[String],
    old_master: &str,
    new_master: &str,
) -> u32 {
    let _ = inv;
    let next = pair
        .iter()
        .filter_map(|a| info_field(a, tls, "role_epoch:"))
        .filter_map(|e| epoch_counter(&e))
        .max()
        .unwrap_or(1)
        + 1;
    match call(old_master, tls, &["FLINTDEMOTE", "0", &next.to_string()]) {
        Ok(Value::Simple(_)) => {}
        Ok(Value::Error(e)) if e.starts_with("FENCED") => {}
        other => panic!("demotion of {old_master} failed: {other:?}"),
    }
    let drain_deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if info_field(old_master, tls, "seq_lag:").as_deref() == Some("0") {
            break;
        }
        assert!(
            Instant::now() < drain_deadline,
            "replica never drained the demoted master {old_master}'s tail"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    match call(
        new_master,
        tls,
        &["FLINTPROMOTE", "0", &(next + 1).to_string()],
    ) {
        Ok(Value::Simple(_)) => {}
        other => panic!(
            "promotion of {new_master} at (0,{}) failed: {other:?}",
            next + 1
        ),
    }
    next + 1
}

/// `flintctl failover <node>`: a graceful, epoch-fenced handoff of a LIVE
/// master to its replica — the same demote→drain→promote core the upgrade
/// uses — after which the ex-master rejoins as a fresh replica of the new
/// master (wipe + checkpoint resync, the demote contract). No build change.
/// The one clean way to hand off a master on demand: run it before taking a
/// master out for maintenance or `decommission-node`.
/// Resolve a pair reference (index or any member address) to its live
/// master address.
fn pair_master(
    inv: &Inventory,
    tls: &Option<Arc<flint_tls::ClientConfig>>,
    pair_ref: &str,
) -> String {
    let idx = match pair_ref.parse::<usize>() {
        Ok(i) if i < inv.pairs.len() => i,
        _ => inv
            .pairs
            .iter()
            .position(|p| p.iter().any(|a| a == pair_ref))
            .unwrap_or_else(|| panic!("{pair_ref} is not a pair index or a known member")),
    };
    inv.pairs[idx]
        .iter()
        .find(|a| info_field(a, tls, "role:").as_deref() == Some("master"))
        .cloned()
        .unwrap_or_else(|| panic!("pair {idx} has no reachable master"))
}

/// `flintctl migrate-slots <ns> <lo-hi> <src> <dest>`: move a contiguous
/// slot range of ONE namespace from src's master to dest's master — the
/// same epoch-fenced FLINTMIGRATEIN cutover the auto-rebalancer uses, then
/// committed to the CP (CPSETSLOT) so every proxy (cold-started ones
/// included) routes it from the snapshot, not from -MOVED discovery. A live
/// writer sees a brief -TRYAGAIN / -MOVED bridge per slot, never loss.
/// Operator control for capacity moves the size-policy rebalancer would not
/// make on its own.
fn migrate_slots(inv: &Inventory, ns: &str, range: &str, src: &str, dest: &str) {
    let tls = tls_client(inv);
    let (lo, hi) = range
        .split_once('-')
        .and_then(|(a, b)| Some((a.parse::<u16>().ok()?, b.parse::<u16>().ok()?)))
        .unwrap_or_else(|| panic!("range must be lo-hi, e.g. 8000-8191 (got {range:?})"));
    assert!(lo <= hi, "range lo must be <= hi ({lo}-{hi})");
    assert!(hi < 16384, "slots are 0..16383 (got hi={hi})");
    let src_master = pair_master(inv, &tls, src);
    let dest_master = pair_master(inv, &tls, dest);
    assert!(
        src_master != dest_master,
        "src and dest resolve to the same master ({src_master}); nothing to move"
    );
    eprintln!(
        "== migrate-slots {ns} {lo}-{hi}: {src_master} -> {dest_master} ({} slot(s))",
        hi - lo + 1
    );
    let mut moved = 0u32;
    for slot in lo..=hi {
        match call_slow(
            &dest_master,
            &tls,
            &[
                "FLINTMIGRATEIN",
                &src_master,
                &slot.to_string(),
                &dest_master,
                ns,
            ],
            Duration::from_secs(120),
        ) {
            Ok(Value::Simple(_)) => {
                // Option B: commit ownership truth to the CP so proxies route
                // it from the snapshot. Best-effort — the -MOVED bridge still
                // routes correctly until the row lands, and CPSETSLOT is
                // idempotent to re-set.
                match call(
                    &inv.cp[0],
                    &tls,
                    &["CPSETSLOT", ns, &slot.to_string(), &dest_master],
                ) {
                    Ok(Value::Simple(_)) => {}
                    other => eprintln!(
                        "  warn: CPSETSLOT {ns}/{slot} not committed: {other:?} (the -MOVED bridge still routes)"
                    ),
                }
                moved += 1;
            }
            other => panic!("migrate {ns}/{slot} {src_master}->{dest_master} failed: {other:?}"),
        }
    }
    eprintln!("== migrate-slots complete: {moved} slot(s) now owned by {dest_master}");
    status(inv);
}

fn failover(inv: &Inventory, node: &str) {
    let tls = tls_client(inv);
    let Some(pair_idx) = inv.pairs.iter().position(|p| p.iter().any(|a| a == node)) else {
        panic!("{node} is not in any pair in the inventory");
    };
    let pair = &inv.pairs[pair_idx];
    if info_field(node, &tls, "role:").as_deref() != Some("master") {
        panic!("{node} is not the live master of pair {pair_idx}; nothing to fail over");
    }
    let new_master = pair
        .iter()
        .find(|a| *a != node && info_field(a, &tls, "role:").as_deref() == Some("replica"))
        .cloned()
        .unwrap_or_else(|| {
            panic!("pair {pair_idx} has no reachable replica to promote; add one first")
        });
    eprintln!("== failover: pair {pair_idx}, {node} -> {new_master}");
    let promoted = controlled_failover(inv, &tls, pair, node, &new_master);
    eprintln!("  {node} demoted + drained; {new_master} promoted at (0,{promoted})");
    // The ex-master rejoins as a fresh replica of the NEW master — no build
    // change (empty envs), wipe = the demote contract.
    if let Err(e) = roll_node(inv, node, &new_master, &[], &None, true) {
        panic!("ex-master {node} failed to rejoin as a replica: {e}");
    }
    eprintln!("== failover complete: {new_master} is master; {node} rejoined as replica");
    status(inv);
}

/// `flintctl decommission-node <addr> [--force] [--drain-ms N]`: drop ONE
/// member from its pair, leaving the pair running on its remaining node(s).
/// The inverse of `add-replica`. Refuses a live master (fail it over
/// first), the pair's last node (that is a whole shard — out of scope), or
/// a removal that would drop the pair below the master's
/// min-replicas-to-write and freeze writes (unless --force). Drains traffic
/// gracefully first: a time-bounded `drain_ms` wait (default 5000) during
/// which the node stays up and serving while proxies route off it, then it
/// is stopped.
fn decommission_node(
    inv: &Inventory,
    inventory_path: &str,
    addr: &str,
    force: bool,
    drain_ms: u64,
) {
    let tls = tls_client(inv);
    let Some(pair_idx) = inv.pairs.iter().position(|p| p.iter().any(|a| a == addr)) else {
        panic!("{addr} is not in any pair in the inventory");
    };
    let members = inv.pairs[pair_idx].clone();
    if members.len() <= 1 {
        panic!(
            "{addr} is the last node in pair {pair_idx} — that is the whole shard, \
             not a single-node decommission (out of scope)"
        );
    }
    if info_field(addr, &tls, "role:").as_deref() == Some("master") {
        panic!(
            "{addr} is the live MASTER of pair {pair_idx}; run \
             `flintctl -f <inv> failover {addr}` first, then decommission it"
        );
    }
    // min-replicas guard: what live replica count remains, vs what the
    // master needs to accept writes?
    let master = members
        .iter()
        .find(|a| info_field(a, &tls, "role:").as_deref() == Some("master"))
        .unwrap_or_else(|| panic!("pair {pair_idx} has no reachable master"));
    let min_repl: u32 = info_field(master, &tls, "min_replicas_to_write:")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let live_replicas: u32 = info_field(master, &tls, "live_replicas:")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let addr_live = info_field(addr, &tls, "role:").as_deref() == Some("replica");
    let after = if addr_live {
        live_replicas.saturating_sub(1)
    } else {
        live_replicas
    };
    if min_repl > 0 && after < min_repl && !force {
        panic!(
            "removing {addr} would leave pair {pair_idx} with {after} live replica(s), \
             below min-replicas-to-write={min_repl}: the master would FREEZE writes. \
             Add a replacement first, or pass --force to proceed anyway."
        );
    }
    eprintln!("== decommission-node: draining {addr} out of pair {pair_idx}");
    let remaining: Vec<String> = members.iter().filter(|a| *a != addr).cloned().collect();
    // GRACEFUL DRAIN, in order: remove the node from the topology FIRST
    // (proxies stop routing replica-reads to it on the next snapshot), let
    // that converge while the node is STILL UP AND SERVING, and only THEN
    // stop it — so no read, not even one the proxy would otherwise absorb
    // and retry on the master, ever hits a dying node. Writes never touch a
    // replica, so they are unaffected throughout. Bounded; the snapshot
    // push is sub-second, so a few seconds covers several cycles.
    call(
        &inv.cp[0],
        &tls,
        &["CPSETPAIR", &pair_idx.to_string(), &remaining.join(",")],
    )
    .expect("CPSETPAIR");
    eprintln!("  draining {drain_ms}ms for proxies to route off {addr} (still serving)");
    std::thread::sleep(Duration::from_millis(drain_ms));
    kill_pidfile(&inv.statedir, &format!("node-{}", port_of(addr)));
    let raw = std::fs::read_to_string(inventory_path).expect("inventory");
    let updated = raw.replace(
        &format!("pair {}", members.join(",")),
        &format!("pair {}", remaining.join(",")),
    );
    std::fs::write(inventory_path, updated).expect("inventory update");
    if inv.controller {
        let t0 = now_ms();
        let mut inv2 = inv.clone();
        inv2.pairs[pair_idx] = remaining;
        kill_pidfile(&inv.statedir, "controller");
        start_controller(&inv2);
        wait_supervised(&inv2, inv2.pairs.len(), t0);
    }
    eprintln!(
        "== decommission-node complete: {addr} removed; pair {pair_idx} runs on its remaining node(s)"
    );
    status(inv);
}

/// Canary upgrade: one replica first, soak against the fleet journal, then
/// the remaining replicas, then masters LAST via an epoch-fenced controlled
/// failover (promote the already-upgraded replica, demote the old master,
/// warm-restart it on the new build as a replica). Any unexpected journal
/// transition aborts the roll — already-upgraded nodes stay (roll forward).
fn upgrade(inv: &Inventory, version_tag: Option<String>, soak_ms: u64) {
    let tls = tls_client(inv);
    let envs: Vec<(String, String)> = version_tag
        .iter()
        .map(|t| ("FLINT_BUILD_VERSION".to_string(), t.clone()))
        .collect();
    let expect = version_tag.clone();
    // Disallowed while replicas roll: ANY role transition. During the master
    // phase the promote/demote we issue are expected; Detected/SelfFenced/
    // SpareRestored still mean something else broke.
    const REPLICA_PHASE_DISALLOWED: &[&str] = &[
        "Detected",
        "PromoteIssued",
        "Promoted",
        "Demoted",
        "SelfFenced",
        "SpareRestored",
    ];
    const MASTER_PHASE_DISALLOWED: &[&str] = &["Detected", "SelfFenced", "SpareRestored"];

    // Observe roles now: replicas roll first, masters last.
    let mut masters: Vec<String> = Vec::new();
    let mut replicas: Vec<(String, String)> = Vec::new(); // (replica, its master)
    for pair in &inv.pairs {
        let m = pair
            .iter()
            .find(|a| info_field(a, &tls, "role:").as_deref() == Some("master"))
            .unwrap_or_else(|| panic!("pair without a reachable master; heal before upgrading"))
            .clone();
        for a in pair {
            if *a != m {
                replicas.push((a.clone(), m.clone()));
            }
        }
        masters.push(m);
    }
    let (canary, canary_master) = replicas
        .first()
        .cloned()
        .expect("need at least one replica");

    eprintln!(
        "== upgrade: canary {canary} first, soak {soak_ms}ms, then {} more replica(s), masters last",
        replicas.len() - 1
    );
    let t0 = now_ms();
    if let Err(e) = roll_node(inv, &canary, &canary_master, &envs, &expect, false) {
        panic!("CANARY FAILED (fleet untouched beyond the canary): {e}");
    }
    eprintln!("  canary {canary} on new build, reconverged; soaking {soak_ms}ms");
    std::thread::sleep(Duration::from_millis(soak_ms));
    if let Err(e) = journal_clean(inv, t0, REPLICA_PHASE_DISALLOWED) {
        eprintln!("== UPGRADE ABORTED at canary soak: {e}");
        eprintln!("   canary stays on the new build (roll forward after diagnosis)");
        std::process::exit(3);
    }
    eprintln!("  soak clean: no unexpected transitions in the fleet journal");

    for (r, m) in replicas.iter().skip(1) {
        let t = now_ms();
        // Re-resolve the pair's CURRENT master: roles may have moved since
        // upgrade start (that is exactly what the gate aborts on — but a
        // roll step must never sync against a captured-stale master).
        let pair = inv
            .pairs
            .iter()
            .find(|p| p.iter().any(|a| a == r))
            .expect("replica belongs to a pair");
        let m_now = pair
            .iter()
            .find(|a| info_field(a, &tls, "role:").as_deref() == Some("master"))
            .cloned()
            .unwrap_or_else(|| m.clone());
        if let Err(e) = roll_node(inv, r, &m_now, &envs, &expect, false) {
            panic!("replica roll failed at {r}: {e}");
        }
        if let Err(e) = journal_clean(inv, t, REPLICA_PHASE_DISALLOWED) {
            eprintln!("== UPGRADE ABORTED after {r}: {e}");
            std::process::exit(3);
        }
        eprintln!("  replica {r} rolled");
    }

    eprintln!("== masters last (fenced controlled failover per pair)");
    for (i, pair) in inv.pairs.iter().enumerate() {
        let t = now_ms();
        let old_master = &masters[i];
        let new_master = pair
            .iter()
            .find(|a| *a != old_master)
            .expect("pair has an upgraded replica")
            .clone();
        // The loss-critical demote->drain->promote core (shared with the
        // `failover` verb).
        let promoted = controlled_failover(inv, &tls, pair, old_master, &new_master);
        eprintln!(
            "  pair {i}: {old_master} demoted + drained; {new_master} promoted at (0,{promoted})"
        );
        if let Err(e) = roll_node(inv, old_master, &new_master, &envs, &expect, true) {
            panic!("old master respawn failed at {old_master}: {e}");
        }
        if let Err(e) = journal_clean(inv, t, MASTER_PHASE_DISALLOWED) {
            eprintln!("== UPGRADE ABORTED after pair {i} master roll: {e}");
            std::process::exit(3);
        }
        eprintln!("  pair {i}: old master rolled, tailing the new one warm");
    }
    eprintln!(
        "== upgrade complete (data plane); proxies/CP/controller/agent roll is the --fleet follow-on"
    );
    status(inv);
}

fn stop(inv: &Inventory) {
    let d = &inv.statedir;
    let Ok(entries) = std::fs::read_dir(format!("{d}/pids")) else {
        eprintln!("nothing to stop (no pidfiles)");
        return;
    };
    for e in entries.flatten() {
        let name = e
            .file_name()
            .to_string_lossy()
            .trim_end_matches(".pid")
            .to_string();
        kill_pidfile(d, &name);
        eprintln!("  stopped {name}");
    }
}

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    let inv_path = argv
        .iter()
        .position(|a| a == "-f")
        .and_then(|i| argv.get(i + 1))
        .unwrap_or_else(|| panic!("usage: flintctl -f <inventory> <command> [...]"))
        .clone();
    let inv = parse_inventory(&inv_path);
    let cmd_at = argv
        .iter()
        .position(|a| a == "-f")
        .map(|i| i + 2)
        .unwrap_or(1);
    let cmd = argv.get(cmd_at).map(|s| s.as_str()).unwrap_or("status");
    let rest: Vec<String> = argv.iter().skip(cmd_at + 1).cloned().collect();

    match cmd {
        "bootstrap" => bootstrap(&inv),
        "start" => start(&inv),
        "status" => status(&inv),
        "tenant" => match rest.first().map(|s| s.as_str()) {
            Some("add") => tenant_add(&inv, &rest[1..]),
            Some("remove") => {
                let name = rest.get(1).expect("usage: tenant remove <name>");
                tenant_remove(&inv, name);
            }
            _ => panic!("usage: tenant add <name> <token> <ns> [k] | tenant remove <name>"),
        },
        "expand" => {
            let spec = rest.first().expect("usage: expand <a,b[,c]>");
            expand(&inv, &inv_path, spec);
        }
        "swap-node" => {
            let (bad, new) = (
                rest.first().expect("usage: swap-node <bad> <new>"),
                rest.get(1).expect("usage: swap-node <bad> <new>"),
            );
            swap_node(&inv, &inv_path, bad, new);
        }
        "add-replica" => {
            let (pair, new) = (
                rest.first()
                    .expect("usage: add-replica <pair-idx|member> <new>"),
                rest.get(1)
                    .expect("usage: add-replica <pair-idx|member> <new>"),
            );
            add_replica(&inv, &inv_path, pair, new);
        }
        "reload" => reload(&inv),
        "migrate-slots" => {
            let (ns, range, src, dest) = (
                rest.first()
                    .expect("usage: migrate-slots <ns> <lo-hi> <src> <dest>"),
                rest.get(1)
                    .expect("usage: migrate-slots <ns> <lo-hi> <src> <dest>"),
                rest.get(2)
                    .expect("usage: migrate-slots <ns> <lo-hi> <src> <dest>"),
                rest.get(3)
                    .expect("usage: migrate-slots <ns> <lo-hi> <src> <dest>"),
            );
            migrate_slots(&inv, ns, range, src, dest);
        }
        "failover" => {
            let node = rest.first().expect("usage: failover <node-addr>");
            failover(&inv, node);
        }
        "decommission-node" => {
            let addr = rest
                .first()
                .expect("usage: decommission-node <node-addr> [--force] [--drain-ms N]");
            let force = rest.iter().any(|a| a == "--force");
            let drain_ms = rest
                .iter()
                .position(|a| a == "--drain-ms")
                .and_then(|i| rest.get(i + 1))
                .and_then(|v| v.parse().ok())
                .unwrap_or(5000);
            decommission_node(&inv, &inv_path, addr, force, drain_ms);
        }
        "tenant-reads" => {
            let (name, mode) = (
                rest.first().expect("usage: tenant-reads <name> <on|off>"),
                rest.get(1).expect("usage: tenant-reads <name> <on|off>"),
            );
            let tls = tls_client(&inv);
            match call(&inv.cp[0], &tls, &["CPTENANTREADS", name, mode]) {
                Ok(Value::Simple(s)) => println!("{s}"),
                other => panic!("tenant-reads failed: {other:?}"),
            }
        }
        // Async write-queue opt-in (ADR-0005 D4, the 'a' flag): the hot-key
        // write mitigation, operator-set here or tenant-set via the portal.
        "tenant-async" => {
            let (name, mode) = (
                rest.first().expect("usage: tenant-async <name> <on|off>"),
                rest.get(1).expect("usage: tenant-async <name> <on|off>"),
            );
            let tls = tls_client(&inv);
            match call(&inv.cp[0], &tls, &["CPTENANTASYNC", name, mode]) {
                Ok(Value::Simple(s)) => println!("{s}"),
                other => panic!("tenant-async failed: {other:?}"),
            }
        }
        // Federation flag (ADR-0007, plumbing today): marks the tenant and
        // rides the snapshot; routing semantics arrive with the fleet map.
        "tenant-federate" => {
            let (name, mode) = (
                rest.first()
                    .expect("usage: tenant-federate <name> <on|off>"),
                rest.get(1).expect("usage: tenant-federate <name> <on|off>"),
            );
            let tls = tls_client(&inv);
            match call(&inv.cp[0], &tls, &["CPTENANTFEDERATE", name, mode]) {
                Ok(Value::Simple(s)) => println!("{s}"),
                other => panic!("tenant-federate failed: {other:?}"),
            }
        }
        // Tenant quotas (M5): fleet ops/s + storage bytes; 0 = unlimited.
        "tenant-quota" => {
            let (name, ops, bytes) = (
                rest.first()
                    .expect("usage: tenant-quota <name> <ops_per_sec> <max_bytes>"),
                rest.get(1)
                    .expect("usage: tenant-quota <name> <ops_per_sec> <max_bytes>"),
                rest.get(2)
                    .expect("usage: tenant-quota <name> <ops_per_sec> <max_bytes>"),
            );
            let tls = tls_client(&inv);
            match call(&inv.cp[0], &tls, &["CPTENANTQUOTA", name, ops, bytes]) {
                Ok(Value::Simple(s)) => println!("{s}"),
                other => panic!("tenant-quota failed: {other:?}"),
            }
        }
        // Push the near-cache knobs (PROXYCACHE <ttl_ms> <max_bytes>) to
        // EVERY proxy in the inventory — the per-proxy runtime setting,
        // fleet-applied. Presents the inventory admin token when the fleet
        // is gated.
        // Rotate the FLEET ADMIN token (ADR-0006 D4): the CP mints the
        // successor, keeps both valid, and the agent retires the old one
        // once it has rolled through the fleet. Prints the new token ONCE.
        // Re-sign the mesh/edge LEAF certs from the CA (ADR-0006 D4);
        // running components hot-reload with no restart. CA rotation is a
        // runbook, not this verb.
        "rotate-certs" => rotate_certs(&inv),
        "rotate-admin" => {
            let tls = tls_client(&inv);
            match call(&inv.cp[0], &tls, &["CPADMINROTATE"]) {
                Ok(Value::Bulk(Some(t))) => {
                    println!("{}", String::from_utf8_lossy(&t));
                    eprintln!(
                        "  new admin token minted; both old and new work until the agent retires the old one"
                    );
                }
                other => panic!("rotate-admin failed: {other:?}"),
            }
        }
        "proxy-cache" => {
            let (ttl, maxb) = (
                rest.first()
                    .expect("usage: proxy-cache <ttl_ms> <max_bytes>"),
                rest.get(1)
                    .expect("usage: proxy-cache <ttl_ms> <max_bytes>"),
            );
            let tls = tls_client(&inv);
            for proxy in &inv.proxies {
                if let Some(tok) = &inv.admin_token {
                    match call(proxy, &tls, &["AUTH", tok]) {
                        Ok(Value::Simple(_)) => {}
                        other => panic!("proxy-cache: admin auth to {proxy} failed: {other:?}"),
                    }
                }
                match call(proxy, &tls, &["PROXYCACHE", ttl, maxb]) {
                    Ok(Value::Simple(_)) => println!("{proxy}: cache ttl={ttl}ms max={maxb}B"),
                    other => panic!("proxy-cache: {proxy} rejected: {other:?}"),
                }
            }
        }
        // Proxy near-cache consent for a tenant (ADR-0005 D6). The cache's
        // TTL/size knobs are per-proxy runtime settings (PROXYCACHE); this
        // records the tenant's acceptance of TTL-bounded stale reads.
        "tenant-cache" => {
            let (name, mode) = (
                rest.first().expect("usage: tenant-cache <name> <on|off>"),
                rest.get(1).expect("usage: tenant-cache <name> <on|off>"),
            );
            let tls = tls_client(&inv);
            match call(&inv.cp[0], &tls, &["CPTENANTCACHE", name, mode]) {
                Ok(Value::Simple(s)) => println!("{s}"),
                other => panic!("tenant-cache failed: {other:?}"),
            }
        }
        "upgrade" => {
            let tag = rest
                .iter()
                .position(|a| a == "--version-tag")
                .and_then(|i| rest.get(i + 1))
                .cloned();
            let soak: u64 = rest
                .iter()
                .position(|a| a == "--soak-ms")
                .and_then(|i| rest.get(i + 1))
                .and_then(|v| v.parse().ok())
                .unwrap_or(3000);
            // Release-manifest guard: the canary path's safety net is that
            // any node can roll back or forward freely, which an on-disk
            // FORMAT BREAK destroys. A manifest declaring one refuses the
            // fast path unless the operator explicitly acknowledges the
            // migration-release runbook.
            if let Some(mf) = rest
                .iter()
                .position(|a| a == "--manifest")
                .and_then(|i| rest.get(i + 1))
            {
                let body = std::fs::read_to_string(mf)
                    .unwrap_or_else(|e| panic!("cannot read manifest {mf}: {e}"));
                let breaks = body.split("\"format_break\"").nth(1).is_some_and(|rest| {
                    rest.trim_start()
                        .trim_start_matches(':')
                        .trim_start()
                        .starts_with("true")
                });
                if breaks && !rest.iter().any(|a| a == "--allow-format-break") {
                    panic!(
                        "REFUSED: manifest {mf} declares format_break=true — this release \
                         cannot roll back and must ship via the migration runbook, not the \
                         canary fast path. Re-run with --allow-format-break only if you are \
                         executing that runbook."
                    );
                }
            }
            upgrade(&inv, tag, soak);
        }
        "stop" => stop(&inv),
        other => {
            panic!(
                "unknown command {other:?} (bootstrap|start|status|reload|tenant|tenant-reads|tenant-cache|tenant-async|tenant-federate|tenant-quota|rotate-admin|rotate-certs|proxy-cache|expand|swap-node|add-replica|migrate-slots|failover|decommission-node|upgrade|stop)"
            )
        }
    }
}
