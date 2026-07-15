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
}

fn parse_inventory(path: &str) -> Inventory {
    let raw = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("read inventory {path}: {e}"));
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
            other => panic!("inventory: unknown key {other:?}"),
        }
    }
    assert!(!inv.statedir.is_empty(), "inventory needs `statedir <path>`");
    assert!(!inv.cp.is_empty(), "inventory needs at least one `cp <addr>`");
    assert_eq!(inv.cp.len(), 1, "multi-node CP bootstrap is the HA follow-on");
    assert!(!inv.pairs.is_empty(), "inventory needs at least one `pair a,b`");
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
    let mut s = flint_tls::connect(addr, tls)?;
    s.set_read_timeout(Some(Duration::from_millis(1500)))?;
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

fn info_field(addr: &str, tls: &Option<Arc<flint_tls::ClientConfig>>, field: &str) -> Option<String> {
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
fn spawn(inv: &Inventory, name: &str, bin: &str, args: &[String]) {
    let d = &inv.statedir;
    let log = std::fs::File::create(format!("{d}/logs/{name}.log")).expect("log file");
    let child = Command::new(format!("{}/{bin}", inv.bins))
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(log)
        .spawn()
        .unwrap_or_else(|e| panic!("spawn {bin} ({name}): {e}"));
    std::fs::write(format!("{d}/pids/{name}.pid"), child.id().to_string()).expect("pidfile");
    eprintln!("  started {name} (pid {})", child.id());
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
            eprintln!("  WARNING: supervision not confirmed within 30s (journal may be unavailable); auto-failover may lag");
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
    sh(&format!(
        "openssl req -newkey rsa:2048 -nodes -keyout {d}/int.key -out {d}/int.csr \
         -subj /CN=flint-internal 2>/dev/null"
    ));
    sh(&format!(
        "printf 'subjectAltName=DNS:flint-internal\\nextendedKeyUsage=serverAuth,clientAuth\\nbasicConstraints=CA:FALSE' > {d}/ext.cnf && \
         openssl x509 -req -in {d}/int.csr -CA {d}/ca.crt -CAkey {d}/ca.key -CAcreateserial \
         -out {d}/int.crt -days 365 -extfile {d}/ext.cnf 2>/dev/null"
    ));
    eprintln!("  minted internal CA + component cert");
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
        "--poll-ms".into(),
        "200".into(),
        "--confirm".into(),
        "3".into(),
        "--journal".into(),
        inv.cp[0].clone(),
        "--snapshot-root".into(),
        format!("{d}/snaps"),
    ];
    args.extend(internal_args(inv));
    spawn(inv, "controller", "flint-controller", &args);
}

fn bootstrap(inv: &Inventory) {
    let d = &inv.statedir;
    for sub in ["logs", "pids", "snaps"] {
        std::fs::create_dir_all(format!("{d}/{sub}")).expect("statedir");
    }
    eprintln!("== bootstrap into {d} (tls {})", if inv.tls { "on" } else { "off" });
    if inv.tls {
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
    spawn(inv, "cp", "flint-controlplane", &cp_args);
    assert!(wait_pong(cp, &tls, Duration::from_secs(10)), "control plane up");
    for proxy in &inv.proxies {
        call(cp, &tls, &["CPADDPROXY", proxy]).expect("register proxy");
    }
    // Initial pairs carry the even slot split as EXPLICIT level-1 routing
    // state; expansion pairs later join with "-" (no range) so capacity
    // never re-routes unmigrated slots.
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
    eprintln!("  registry: {} proxies, {} pairs", inv.proxies.len(), inv.pairs.len());

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
        let mut args = vec![
            "--port".to_string(),
            port_of(proxy).to_string(),
            "--control-plane".into(),
            cp.clone(),
            "--advertise".into(),
            proxy.clone(),
        ];
        args.extend(internal_args(inv));
        spawn(inv, &format!("proxy-{}", port_of(proxy)), "flint-proxy", &args);
    }

    // 4. Supervision + observability.
    let ctl_since = now_ms();
    if inv.controller {
        start_controller(inv);
    }
    if let Some(agent) = &inv.agent {
        let mut args = vec![
            "--control-plane".to_string(),
            cp.clone(),
            "--metrics-port".into(),
            port_of(agent).to_string(),
            "--journal".into(),
            format!("{d}/shadow.jsonl"),
        ];
        args.extend(internal_args(inv));
        spawn(inv, "agent", "flint-agent", &args);
    }
    if inv.controller {
        wait_supervised(inv, inv.pairs.len(), ctl_since);
    }
    eprintln!("== bootstrap complete");
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
                    println!(
                        "pair {i}    {addr}  {role:<7} epoch {epoch:<7} seq_lag {lag:<5} live_replicas {live}"
                    );
                }
                None => println!("pair {i}    {addr}  DOWN"),
            }
        }
    }
    for proxy in &inv.proxies {
        // The proxy's client port is plaintext (frontend TLS is separate
        // from the internal mesh): probe it without the mesh cert.
        let up = matches!(call(proxy, &None, &["PROXYSTATS"]), Ok(Value::Bulk(_)));
        println!("proxy     {proxy}  {}", if up { "up" } else { "DOWN" });
    }
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

fn expand(inv: &Inventory, inventory_path: &str, pair_spec: &str) {
    let tls = tls_client(inv);
    let pair: Vec<String> = pair_spec.split(',').map(String::from).collect();
    eprintln!("== expand: new pair {pair_spec}");
    start_pair_nodes(inv, &pair);
    assert!(wait_pong(&pair[0], &tls, Duration::from_secs(10)), "new master up");
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
        eprintln!("  controller rerolled with {} pairs (stateless: a non-event)", inv2.pairs.len());
    }
    eprintln!("== expand complete");
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
    spawn(inv, &format!("node-{port}"), "flint-server", &args);
    assert!(wait_pong(new, &tls, Duration::from_secs(15)), "replacement up");

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
    eprintln!("== swap complete: pair {pair_idx} seat replaced, registry + inventory updated (supervision armed)");
}

fn stop(inv: &Inventory) {
    let d = &inv.statedir;
    let Ok(entries) = std::fs::read_dir(format!("{d}/pids")) else {
        eprintln!("nothing to stop (no pidfiles)");
        return;
    };
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().trim_end_matches(".pid").to_string();
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
    let cmd_at = argv.iter().position(|a| a == "-f").map(|i| i + 2).unwrap_or(1);
    let cmd = argv.get(cmd_at).map(|s| s.as_str()).unwrap_or("status");
    let rest: Vec<String> = argv.iter().skip(cmd_at + 1).cloned().collect();

    match cmd {
        "bootstrap" => bootstrap(&inv),
        "status" => status(&inv),
        "tenant" => {
            assert!(rest.first().map(|s| s.as_str()) == Some("add"), "usage: tenant add <name> <token> <ns> [k]");
            tenant_add(&inv, &rest[1..]);
        }
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
        "stop" => stop(&inv),
        other => panic!("unknown command {other:?} (bootstrap|status|tenant|expand|swap-node|stop)"),
    }
}
