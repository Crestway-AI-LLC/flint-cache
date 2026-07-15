//! flint-controlplane (v1): the global control plane's tenant-registry +
//! snapshot-push half (design.md §2.2).
//!
//! Holds DURABLE INTENT only — what cannot be re-derived by observing data
//! nodes: the tenant registry (token, namespace, shuffle-shard proxy
//! subset), the proxy fleet, and group topology. Every mutation bumps a
//! version and persists atomically before it is observable. Never on the
//! data path: proxies serve from their last-pushed table if the control
//! plane is down (its outage pauses onboarding, not traffic).
//!
//! Snapshot push (the xDS pattern over RESP framing): a proxy subscribes
//! with `CPWATCH <its-addr> <last-version>`; the connection is hijacked and
//! the control plane pushes a filtered snapshot — the shared pair topology
//! plus ONLY that proxy's assigned tenants — whenever the version advances,
//! reading an ACK per push. The filtering is the sub-group boundary
//! (design.md §2.1): a proxy never holds tokens it does not serve.
//!
//! Admin API (RESP):
//!   CPADDPROXY <addr>                       register a fleet member
//!   CPADDPAIR <a,b[,c]>                     register a replica set
//!   CPADDTENANT <name> <token> <ns> [k]     add tenant; subset = shuffle
//!                                           shard of k (default 2) proxies
//!   CPSETSUBSET <name> <p1,p2|->            override subset (whale
//!                                           isolation / drain)
//!   CPINFO                                  version + counts
//!   CPSNAPSHOT <proxy-addr>                 one-shot filtered snapshot
//!
//! v1 scope: single node, durable file state (the serialized form is the
//! future Raft snapshot). openraft ×3 HA is the follow-on; so are deltas
//! (full snapshots are fine at this state size), token rotation, and DNS
//! publication of subsets.
//!
//! Usage: flint-controlplane --port 7500 --state /path/state

mod ha;
mod raft;
mod registry;
mod state;

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use flint_resp::{Decoded, Value, decode, encode};
use state::{State, Tenant, shuffle_shard};

fn arg(name: &str) -> Option<String> {
    std::env::args().skip_while(|a| a != name).nth(1)
}

/// State + a condvar so watch threads sleep until a mutation bumps the
/// version instead of polling.
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
    /// Fleet-journal file (append-only JSONL beside --state). Observability,
    /// not intent: outside the durability contract of the registry.
    journal_path: String,
}

fn ok() -> Value {
    Value::Simple("OK".into())
}

fn err(msg: &str) -> Value {
    Value::Error(format!("ERR {msg}"))
}

/// Validate the space-free tokens the line format depends on.
fn clean(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':' | b','))
}

fn handle(shared: &Shared, args: &[Vec<u8>]) -> Value {
    let cmd = args
        .first()
        .map(|c| c.to_ascii_uppercase())
        .unwrap_or_default();
    let text = |i: usize| -> Option<String> {
        args.get(i)
            .and_then(|r| std::str::from_utf8(r).ok())
            .map(String::from)
    };
    match cmd.as_slice() {
        b"PING" => Value::Simple("PONG".into()),
        b"CPADDPROXY" => {
            let Some(addr) = text(1).filter(|a| clean(a)) else {
                return err("CPADDPROXY <addr>");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            if !st.proxies.contains(&addr) {
                st.proxies.push(addr);
                match st.commit() {
                    Ok(_) => {}
                    Err(e) => return err(&format!("persist: {e}")),
                }
                shared.changed.notify_all();
            }
            ok()
        }
        b"CPADDPAIR" => {
            let Some(nodes) = text(1).filter(|a| clean(a)) else {
                return err("CPADDPAIR <a,b[,c]>");
            };
            let pair: Vec<String> = nodes.split(',').map(String::from).collect();
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            if !st.pairs.contains(&pair) {
                st.pairs.push(pair);
                match st.commit() {
                    Ok(_) => {}
                    Err(e) => return err(&format!("persist: {e}")),
                }
                shared.changed.notify_all();
            }
            ok()
        }
        b"CPADDTENANT" => {
            let (Some(name), Some(token), Some(ns)) = (text(1), text(2), text(3)) else {
                return err("CPADDTENANT <name> <token> <ns> [k]");
            };
            if !clean(&name) || !clean(&token) || !clean(&ns) {
                return err("invalid name/token/ns (space-free, <=128 chars)");
            }
            let k: usize = text(4).and_then(|v| v.parse().ok()).unwrap_or(2);
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            if st.tenants.contains_key(&name) {
                return err("tenant exists");
            }
            if st.tenants.values().any(|t| t.token == token) {
                return err("token already in use");
            }
            let subset = shuffle_shard(&name, &st.proxies, k);
            let reply = format!("OK tenant {name} ns {ns} subset [{}]", subset.join(","));
            st.tenants.insert(
                name.clone(),
                Tenant {
                    name,
                    token,
                    ns,
                    subset,
                    prev_token: None,
                },
            );
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            Value::Simple(reply)
        }
        b"CPSETSUBSET" => {
            let (Some(name), Some(subset)) = (text(1), text(2)) else {
                return err("CPSETSUBSET <name> <p1,p2|->");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st.tenants.get_mut(&name) else {
                return err("no such tenant");
            };
            t.subset = if subset == "-" {
                Vec::new()
            } else {
                subset.split(',').map(String::from).collect()
            };
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        b"CPROTATETOKEN" => {
            let (Some(name), Some(new)) = (text(1), text(2)) else {
                return err("CPROTATETOKEN <name> <new-token>");
            };
            if !clean(&new) {
                return err("invalid token");
            }
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            if st
                .tenants
                .values()
                .any(|t| t.token == new || t.prev_token.as_deref() == Some(new.as_str()))
            {
                return err("token already in use");
            }
            let Some(t) = st.tenants.get_mut(&name) else {
                return err("no such tenant");
            };
            t.prev_token = Some(std::mem::replace(&mut t.token, new));
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            Value::Simple("OK rotated (both tokens valid until CPDROPPREV)".into())
        }
        b"CPDROPPREV" => {
            let Some(name) = text(1) else {
                return err("CPDROPPREV <name>");
            };
            let Ok(mut st) = shared.state.lock() else {
                return err("state lock");
            };
            let Some(t) = st.tenants.get_mut(&name) else {
                return err("no such tenant");
            };
            t.prev_token = None;
            match st.commit() {
                Ok(_) => {}
                Err(e) => return err(&format!("persist: {e}")),
            }
            shared.changed.notify_all();
            ok()
        }
        // Fleet journal (flint-journal): typed state-transition events from
        // every component. Append is a single pre-serialized JSON line.
        b"CPJOURNAL" => {
            let Some(line) = text(1) else {
                return err("CPJOURNAL <event-json>");
            };
            match flint_journal::append_line(&shared.journal_path, &line) {
                Ok(()) => ok(),
                Err(e) => err(&format!("journal append: {e}")),
            }
        }
        b"CPJOURNALREAD" => {
            let n = text(1).and_then(|v| v.parse().ok()).unwrap_or(50);
            let lines = flint_journal::tail(&shared.journal_path, n);
            Value::Bulk(Some(lines.join("\n").into_bytes()))
        }
        b"CPINFO" => {
            let Ok(st) = shared.state.lock() else {
                return err("state lock");
            };
            Value::Bulk(Some(
                format!(
                    "version:{}\r\nproxies:{}\r\npairs:{}\r\ntenants:{}\r\n",
                    st.version,
                    st.proxies.len(),
                    st.pairs.len(),
                    st.tenants.len()
                )
                .into_bytes(),
            ))
        }
        b"CPSNAPSHOT" => {
            let Some(proxy) = text(1) else {
                return err("CPSNAPSHOT <proxy-addr>");
            };
            let Ok(st) = shared.state.lock() else {
                return err("state lock");
            };
            let (v, pairs, tenants) = st.snapshot_for(&proxy);
            snapshot_frame(v, &pairs, &tenants)
        }
        _ => err("unknown control-plane command"),
    }
}

fn snapshot_frame(version: u64, pairs: &str, tenants: &str) -> Value {
    Value::Array(Some(vec![
        Value::Bulk(Some(b"SNAPSHOT".to_vec())),
        Value::Integer(version as i64),
        Value::Bulk(Some(pairs.as_bytes().to_vec())),
        Value::Bulk(Some(tenants.as_bytes().to_vec())),
    ]))
}

/// CPWATCH <proxy-addr> <last-version>: hijack the connection and push a
/// filtered snapshot whenever the version advances past what the proxy has
/// ACKed. Push -> ACK -> wait-for-change -> push...
fn watch(
    mut stream: flint_tls::Stream,
    shared: &Shared,
    proxy: String,
    mut acked: u64,
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        // Wait until there is something newer than the proxy has ACKed.
        let (v, pairs, tenants) = {
            let Ok(mut st) = shared.state.lock() else {
                return Ok(());
            };
            while st.version <= acked {
                let Ok((guard, _timeout)) =
                    shared.changed.wait_timeout(st, Duration::from_millis(500))
                else {
                    return Ok(());
                };
                st = guard;
            }
            st.snapshot_for(&proxy)
        };
        let mut out = Vec::new();
        encode(&snapshot_frame(v, &pairs, &tenants), &mut out);
        stream.write_all(&out)?;
        // Read the ACK (Array ["ACK", <version>]).
        loop {
            match decode(&buf) {
                Ok(Decoded::Complete(frame, used)) => {
                    buf.drain(..used);
                    if let Value::Array(Some(items)) = frame
                        && let [Value::Bulk(Some(tag)), Value::Bulk(Some(raw))] = items.as_slice()
                        && tag.eq_ignore_ascii_case(b"ACK")
                        && let Some(n) = std::str::from_utf8(raw).ok().and_then(|s| s.parse().ok())
                    {
                        acked = n;
                    }
                    break;
                }
                Ok(Decoded::NeedMore) => {
                    let n = stream.read(&mut chunk)?;
                    if n == 0 {
                        return Ok(());
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                Err(_) => return Ok(()),
            }
        }
    }
}

fn serve(mut stream: flint_tls::Stream, shared: Arc<Shared>) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(8 * 1024);
    let mut chunk = [0u8; 8 * 1024];
    let mut out: Vec<u8> = Vec::with_capacity(4 * 1024);
    loop {
        let mut consumed = 0;
        out.clear();
        loop {
            match decode(&buf[consumed..]) {
                Ok(Decoded::Complete(frame, used)) => {
                    consumed += used;
                    let Some(args) = frame_to_args(frame) else {
                        encode(&err("protocol: expected array of bulk strings"), &mut out);
                        stream.write_all(&out)?;
                        return Ok(());
                    };
                    // CPWATCH hijacks the connection.
                    if args
                        .first()
                        .is_some_and(|c| c.eq_ignore_ascii_case(b"CPWATCH"))
                    {
                        let proxy = args
                            .get(1)
                            .and_then(|r| std::str::from_utf8(r).ok())
                            .unwrap_or("")
                            .to_string();
                        let last: u64 = args
                            .get(2)
                            .and_then(|r| std::str::from_utf8(r).ok())
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(0);
                        if proxy.is_empty() {
                            encode(&err("CPWATCH <proxy-addr> [last-version]"), &mut out);
                            stream.write_all(&out)?;
                            return Ok(());
                        }
                        buf.drain(..consumed);
                        stream.write_all(&out)?;
                        return watch(stream, &shared, proxy, last);
                    }
                    let reply = handle(&shared, &args);
                    encode(&reply, &mut out);
                }
                Ok(Decoded::NeedMore) => break,
                Err(_) => {
                    encode(&err("protocol error"), &mut out);
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

/// The --internal-* triple in the server role: the config every
/// control-plane listener (single-node client port, HA client port, Raft
/// RPC port) accepts with — peers and proxies must present a CA-signed
/// cert. None = plaintext.
fn internal_server_config() -> Option<Arc<flint_tls::ServerConfig>> {
    match (
        arg("--internal-ca"),
        arg("--internal-cert"),
        arg("--internal-key"),
    ) {
        (Some(ca), Some(cert), Some(key)) => Some(
            flint_tls::server_config(&ca, &cert, &key)
                .expect("build internal TLS server config"),
        ),
        (None, None, None) => None,
        _ => panic!("--internal-ca, --internal-cert, --internal-key must be given together"),
    }
}

/// The same triple in the client role — the Raft RPC dialer.
fn internal_client_config() -> Option<Arc<flint_tls::ClientConfig>> {
    match (
        arg("--internal-ca"),
        arg("--internal-cert"),
        arg("--internal-key"),
    ) {
        (Some(ca), Some(cert), Some(key)) => Some(
            flint_tls::client_config(&ca, &cert, &key)
                .expect("build internal TLS client config"),
        ),
        _ => None,
    }
}

fn main() -> std::io::Result<()> {
    let port: u16 = arg("--port").and_then(|p| p.parse().ok()).unwrap_or(7500);
    let path = arg("--state").unwrap_or_else(|| "./flint-cp-state".into());

    // HA mode: --raft with --node-id / --raft-port / --peers / --client-addrs
    // runs a Raft-replicated node (openraft); otherwise the durable
    // single-node path below.
    if std::env::args().any(|a| a == "--raft") {
        return run_raft(port, path);
    }

    let state = State::load_or_new(path.clone().into());
    eprintln!(
        "flint-controlplane: state {path} (version {}, {} proxies, {} pairs, {} tenants)",
        state.version,
        state.proxies.len(),
        state.pairs.len(),
        state.tenants.len()
    );
    let shared = Arc::new(Shared {
        state: Mutex::new(state),
        changed: Condvar::new(),
        journal_path: format!("{path}.journal"),
    });
    let internal_tls = internal_server_config();
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    eprintln!(
        "flint-controlplane listening on 127.0.0.1:{port} ({})",
        if internal_tls.is_some() { "internal mTLS" } else { "plaintext" }
    );
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let shared = Arc::clone(&shared);
        let internal_tls = internal_tls.clone();
        std::thread::spawn(move || {
            let conn = match flint_tls::accept(stream, &internal_tls) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("internal tls accept: {e}");
                    return;
                }
            };
            let _ = serve(conn, shared);
        });
    }
    Ok(())
}

/// --raft entry: build a tokio runtime and run the Raft node + client
/// server. Bridges our blocking main into openraft's async world.
fn run_raft(port: u16, state_path: String) -> std::io::Result<()> {
    let node_id: u64 = arg("--node-id")
        .and_then(|v| v.parse().ok())
        .expect("--raft requires --node-id <n>");
    let raft_port: u16 = arg("--raft-port")
        .and_then(|v| v.parse().ok())
        .expect("--raft requires --raft-port <port>");
    let peers = arg("--peers").expect("--raft requires --peers \"1=addr,2=addr,...\"");
    let clients =
        arg("--client-addrs").expect("--raft requires --client-addrs \"1=addr,2=addr,...\"");
    let raft_addr = format!("127.0.0.1:{raft_port}");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    // One --internal-* triple covers all three surfaces of an HA node: the
    // Raft RPC listener + dialer (peer↔peer) and the client port (proxies
    // and admin).
    let tls_server = internal_server_config();
    let tls_client = internal_client_config();
    rt.block_on(async move {
        let ha = ha::start(
            node_id,
            &raft_addr,
            raft_port,
            state_path.into(),
            &peers,
            &clients,
            tls_server.clone(),
            tls_client,
        )
        .await?;
        let ha = std::sync::Arc::new(ha);
        // Bring the cluster up (idempotent; lowest node id initializes).
        {
            let ha = std::sync::Arc::clone(&ha);
            tokio::spawn(async move {
                ha.maybe_initialize().await;
            });
        }
        ha::run_client(ha, port, tls_server).await
    })
}
