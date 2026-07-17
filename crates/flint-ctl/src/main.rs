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
    /// Per-node storage capacity in bytes (capacity model, question 2);
    /// passed to the agent so it can compute fill + expansion ETAs.
    capacity_bytes: Option<u64>,
    /// Proxy admin token (`--admin-token` on every proxy; presented before
    /// PROXY* operator commands). None = ungated dev fleet.
    admin_token: Option<String>,
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
    eprintln!(
        "== bootstrap into {d} (tls {})",
        if inv.tls { "on" } else { "off" }
    );
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
    assert!(
        wait_pong(cp, &tls, Duration::from_secs(10)),
        "control plane up"
    );
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
    eprintln!(
        "  registry: {} proxies, {} pairs",
        inv.proxies.len(),
        inv.pairs.len()
    );

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
            if matches!(call(proxy, &None, &["PROXYSTATS"]), Ok(Value::Bulk(_))) {
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
        // Fencing epoch: above everything either member has seen.
        let next = pair
            .iter()
            .filter_map(|a| info_field(a, &tls, "role_epoch:"))
            .filter_map(|e| epoch_counter(&e))
            .max()
            .unwrap_or(1)
            + 1;
        // ORDER IS LOSS-CRITICAL: demote FIRST (the old master stops
        // acking writes), DRAIN (its replica applies every acked write),
        // THEN promote. Promote-first opens a window where the proxy still
        // routes to the old master, which acks writes the new lineage will
        // never contain. The no-master gap between demote and promote is
        // absorbed by the proxy's retry budget: latency, not loss.
        match call(old_master, &tls, &["FLINTDEMOTE", "0", &next.to_string()]) {
            Ok(Value::Simple(_)) => {}
            Ok(Value::Error(e)) if e.starts_with("FENCED") => {}
            other => panic!("demotion of {old_master} failed: {other:?}"),
        }
        let drain_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if info_field(old_master, &tls, "seq_lag:").as_deref() == Some("0") {
                break;
            }
            assert!(
                Instant::now() < drain_deadline,
                "pair {i}: replica never drained the demoted master's tail"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        match call(
            &new_master,
            &tls,
            &["FLINTPROMOTE", "0", &(next + 1).to_string()],
        ) {
            Ok(Value::Simple(_)) => {}
            other => panic!(
                "promotion of {new_master} at (0,{})failed: {other:?}",
                next + 1
            ),
        }
        eprintln!(
            "  pair {i}: {old_master} demoted + drained; {new_master} promoted at (0,{})",
            next + 1
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
        "status" => status(&inv),
        "tenant" => {
            assert!(
                rest.first().map(|s| s.as_str()) == Some("add"),
                "usage: tenant add <name> <token> <ns> [k]"
            );
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
        "add-replica" => {
            let (pair, new) = (
                rest.first()
                    .expect("usage: add-replica <pair-idx|member> <new>"),
                rest.get(1)
                    .expect("usage: add-replica <pair-idx|member> <new>"),
            );
            add_replica(&inv, &inv_path, pair, new);
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
            upgrade(&inv, tag, soak);
        }
        "stop" => stop(&inv),
        other => {
            panic!(
                "unknown command {other:?} (bootstrap|status|tenant|tenant-reads|tenant-cache|tenant-quota|proxy-cache|expand|swap-node|add-replica|upgrade|stop)"
            )
        }
    }
}
