// SPDX-License-Identifier: Elastic-2.0
//! HA runtime: a Raft node (openraft) + JSON-framed RPC network between
//! control-plane nodes + the client-facing RESP server, all on tokio.
//! Entered only in --raft mode; the blocking single-node path is untouched.
//!
//! Writes (admin mutations) go to the leader — a follower replies with a
//! redirect carrying the leader's client address. Reads and the CPWATCH
//! snapshot push are served from ANY node's local applied registry (the
//! state machine is replicated, and config push is versioned/eventual), so
//! proxies can subscribe to any CP node.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::Adaptor;
use openraft::{BasicNode, Config, Raft};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::raft::{NodeId, Request, Store, TypeConfig};
use crate::registry::Mutation;

pub type CpRaft = Raft<TypeConfig>;

/// RPC envelope over the inter-node link: length-prefixed JSON, tag selects
/// the Raft handler on the far side.
#[derive(Serialize, Deserialize)]
enum Rpc {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<NodeId>),
    Snapshot(InstallSnapshotRequest<TypeConfig>),
}

async fn read_framed<S: AsyncRead + Unpin>(s: &mut S) -> std::io::Result<Vec<u8>> {
    let len = s.read_u32().await? as usize;
    let mut buf = vec![0u8; len];
    s.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn write_framed<S: AsyncWrite + Unpin>(s: &mut S, bytes: &[u8]) -> std::io::Result<()> {
    s.write_u32(bytes.len() as u32).await?;
    s.write_all(bytes).await?;
    s.flush().await
}

// --- Network: dial a peer, ship one RPC, read its Result ---

#[derive(Clone)]
pub struct Network {
    /// Internal-mesh mutual-TLS dial config, hot-reloading its leaf
    /// (ADR-0006 D4 follow-on); each dial builds a fresh connector from the
    /// current snapshot. None = plaintext.
    pub tls: Option<Arc<flint_tls::ReloadableClientConfig>>,
}

pub struct Conn {
    target: NodeId,
    addr: String,
    tls: Option<Arc<flint_tls::ReloadableClientConfig>>,
}

/// Dial `addr` (optionally through mutual TLS) and run one framed
/// request/response exchange. The TLS config is snapshotted per dial, so a
/// rotated leaf applies to the next RPC with no restart.
async fn exchange(
    addr: &str,
    tls: &Option<Arc<flint_tls::ReloadableClientConfig>>,
    body: &[u8],
) -> std::io::Result<Vec<u8>> {
    let mut tcp = TcpStream::connect(addr).await?;
    match tls.as_ref().map(|r| TlsConnector::from(r.current())) {
        None => {
            write_framed(&mut tcp, body).await?;
            read_framed(&mut tcp).await
        }
        Some(c) => {
            let name =
                tokio_rustls::rustls::pki_types::ServerName::try_from(flint_tls::INTERNAL_SNI)
                    .expect("internal SNI");
            let mut t = c.connect(name, tcp).await?;
            write_framed(&mut t, body).await?;
            read_framed(&mut t).await
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Conn;
    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Conn {
        Conn {
            target,
            addr: node.addr.clone(),
            tls: self.tls.clone(),
        }
    }
}

impl Conn {
    async fn call<Resp: for<'de> Deserialize<'de>>(
        &self,
        rpc: &Rpc,
    ) -> Result<
        Resp,
        openraft::error::RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        let body = serde_json::to_vec(rpc).map_err(|e| unreachable(&self.addr, e))?;
        let resp = exchange(&self.addr, &self.tls, &body)
            .await
            .map_err(|e| unreachable(&self.addr, e))?;
        // Wire form: Result<Resp, RaftError<NodeId>>.
        let parsed: Result<Resp, openraft::error::RaftError<NodeId>> =
            serde_json::from_slice(&resp).map_err(|e| unreachable(&self.addr, e))?;
        parsed.map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

fn unreachable(
    addr: &str,
    e: impl std::error::Error + 'static,
) -> openraft::error::RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>> {
    let _ = addr;
    openraft::error::RPCError::Unreachable(Unreachable::new(&e))
}

impl RaftNetwork<TypeConfig> for Conn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<
        AppendEntriesResponse<NodeId>,
        openraft::error::RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        self.call(&Rpc::Append(rpc)).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _o: RPCOption,
    ) -> Result<
        VoteResponse<NodeId>,
        openraft::error::RPCError<NodeId, BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        self.call(&Rpc::Vote(rpc)).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _o: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        openraft::error::RPCError<
            NodeId,
            BasicNode,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        let body = serde_json::to_vec(&Rpc::Snapshot(rpc))
            .map_err(|e| openraft::error::RPCError::Unreachable(Unreachable::new(&e)))?;
        let resp = exchange(&self.addr, &self.tls, &body)
            .await
            .map_err(|e| openraft::error::RPCError::Unreachable(Unreachable::new(&e)))?;
        let parsed: Result<
            InstallSnapshotResponse<NodeId>,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        > = serde_json::from_slice(&resp)
            .map_err(|e| openraft::error::RPCError::Unreachable(Unreachable::new(&e)))?;
        parsed.map_err(|e| openraft::error::RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

// --- RPC server: dispatch inbound frames to the local Raft ---

async fn serve_rpc(
    raft: CpRaft,
    listener: TcpListener,
    tls: Option<Arc<flint_tls::ReloadableServerConfig>>,
) {
    loop {
        let Ok((sock, _)) = listener.accept().await else {
            continue;
        };
        let raft = raft.clone();
        // Snapshot per accept: a rotated leaf serves the next connection.
        let acceptor = tls
            .as_ref()
            .and_then(|r| r.current())
            .map(TlsAcceptor::from);
        tokio::spawn(async move {
            match acceptor {
                None => rpc_conn(raft, sock).await,
                Some(a) => {
                    // Mutual TLS: a peer without a CA-signed cert fails here.
                    if let Ok(t) = a.accept(sock).await {
                        rpc_conn(raft, t).await;
                    }
                }
            }
        });
    }
}

async fn rpc_conn<S: AsyncRead + AsyncWrite + Unpin>(raft: CpRaft, mut s: S) {
    while let Ok(body) = read_framed(&mut s).await {
        let Ok(rpc) = serde_json::from_slice::<Rpc>(&body) else {
            break;
        };
        let out = match rpc {
            Rpc::Append(r) => serde_json::to_vec(&raft.append_entries(r).await),
            Rpc::Vote(r) => serde_json::to_vec(&raft.vote(r).await),
            Rpc::Snapshot(r) => serde_json::to_vec(&raft.install_snapshot(r).await),
        };
        let Ok(out) = out else { break };
        if write_framed(&mut s, &out).await.is_err() {
            break;
        }
    }
}

/// Parse `--peers 1=addr,2=addr,3=addr`.
fn parse_peers(spec: &str) -> BTreeMap<NodeId, BasicNode> {
    spec.split(',')
        .filter_map(|kv| {
            let (id, addr) = kv.split_once('=')?;
            Some((id.parse().ok()?, BasicNode::new(addr)))
        })
        .collect()
}

pub struct Ha {
    pub raft: CpRaft,
    pub store: Arc<Store>,
    pub node_id: NodeId,
    pub members: BTreeMap<NodeId, BasicNode>,
    /// node -> client (RESP) address, for leader-redirect replies.
    pub client_addrs: BTreeMap<NodeId, String>,
    /// Node-local fleet-journal file (observability, not Raft state; each
    /// HA node journals the events IT receives — emitters talk to one CP).
    pub journal_path: String,
    /// Latest metered resident bytes per tenant name (CPTENANTUSAGE) —
    /// node-local telemetry for CPMYUSAGE, never Rafted.
    pub usage: std::sync::Mutex<std::collections::HashMap<String, u64>>,
}

/// Build the Raft node and start the RPC server. Returns the handle the
/// client server proposes/reads through.
#[allow(clippy::too_many_arguments)]
pub async fn start(
    node_id: NodeId,
    raft_addr: &str,
    raft_port: u16,
    state_path: std::path::PathBuf,
    peers_spec: &str,
    client_spec: &str,
    tls_server: Option<Arc<flint_tls::ReloadableServerConfig>>,
    tls_client: Option<Arc<flint_tls::ReloadableClientConfig>>,
) -> std::io::Result<Ha> {
    let config = Arc::new(
        Config {
            heartbeat_interval: 250,
            election_timeout_min: 750,
            election_timeout_max: 1500,
            ..Default::default()
        }
        .validate()
        .expect("raft config"),
    );
    let journal_path = format!("{}.journal", state_path.display());
    let store = Store::open(state_path);
    let (log_store, state_machine) = Adaptor::new(store.clone());
    let network = Network { tls: tls_client };
    let raft = Raft::new(node_id, config, network, log_store, state_machine)
        .await
        .expect("raft new");

    let listener = TcpListener::bind(("127.0.0.1", raft_port)).await?;
    {
        let raft = raft.clone();
        tokio::spawn(serve_rpc(raft, listener, tls_server));
    }
    let _ = raft_addr;

    let members = parse_peers(peers_spec);
    let client_addrs = client_spec
        .split(',')
        .filter_map(|kv| {
            let (id, addr) = kv.split_once('=')?;
            Some((id.parse::<NodeId>().ok()?, addr.to_string()))
        })
        .collect();

    Ok(Ha {
        raft,
        store,
        node_id,
        members,
        client_addrs,
        journal_path,
        usage: std::sync::Mutex::new(std::collections::HashMap::new()),
    })
}

impl Ha {
    /// Bring the cluster up (idempotent): one node initializes the full
    /// membership; others no-op if already initialized.
    pub async fn maybe_initialize(&self) {
        // Only attempt from the lowest node id, to avoid racing initializes.
        if self.members.keys().next() == Some(&self.node_id) {
            std::thread::sleep(Duration::from_millis(300));
            let _ = self.raft.initialize(self.members.clone()).await;
        }
    }

    /// Propose a mutation to the leader. On this node being a follower,
    /// returns Err(leader_client_addr) so the caller can redirect.
    pub async fn propose(&self, m: Mutation) -> Result<u64, Option<String>> {
        match self.raft.client_write(m as Request).await {
            Ok(r) => Ok(r.data.version),
            Err(e) => {
                let leader = e
                    .forward_to_leader()
                    .and_then(|f| f.leader_id)
                    .and_then(|id| self.client_addrs.get(&id).cloned());
                Err(leader)
            }
        }
    }
}

// --- Client-facing RESP server (admin + reads + CPWATCH) in raft mode ---

use flint_resp::{Decoded, Value, decode, encode};

fn args_of(frame: Value) -> Option<Vec<Vec<u8>>> {
    let Value::Array(Some(items)) = frame else {
        return None;
    };
    let mut out = Vec::with_capacity(items.len());
    for i in items {
        let Value::Bulk(Some(b)) = i else { return None };
        out.push(b);
    }
    Some(out)
}

fn snapshot_frame(v: u64, pairs: &str, tenants: &str, admin: &str) -> Value {
    Value::Array(Some(vec![
        Value::Bulk(Some(b"SNAPSHOT".to_vec())),
        Value::Integer(v as i64),
        Value::Bulk(Some(pairs.as_bytes().to_vec())),
        Value::Bulk(Some(tenants.as_bytes().to_vec())),
        Value::Bulk(Some(admin.as_bytes().to_vec())),
    ]))
}

/// Run the client RESP server: admin mutations propose through Raft (leader
/// only; a follower redirects), reads and CPWATCH serve from the local
/// applied registry.
pub async fn run_client(
    ha: Arc<Ha>,
    port: u16,
    tls_server: Option<Arc<flint_tls::ReloadableServerConfig>>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    eprintln!(
        "flint-controlplane[raft node {}] client on :{port} ({})",
        ha.node_id,
        if tls_server.is_some() {
            "internal mTLS"
        } else {
            "plaintext"
        }
    );
    loop {
        let Ok((sock, _)) = listener.accept().await else {
            continue;
        };
        let ha = Arc::clone(&ha);
        // Snapshot per accept (rotated leaf serves the next connection).
        let acceptor = tls_server
            .as_ref()
            .and_then(|r| r.current())
            .map(TlsAcceptor::from);
        tokio::spawn(async move {
            match acceptor {
                None => {
                    let _ = client_conn(sock, ha).await;
                }
                Some(a) => {
                    // Mutual TLS: proxies/admin present the mesh cert.
                    if let Ok(t) = a.accept(sock).await {
                        let _ = client_conn(t, ha).await;
                    }
                }
            }
        });
    }
}

async fn client_conn<S: AsyncRead + AsyncWrite + Unpin>(
    mut sock: S,
    ha: Arc<Ha>,
) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 16 * 1024];
    loop {
        // Try to parse a full command from the buffer.
        match decode(&buf) {
            Ok(Decoded::Complete(frame, used)) => {
                let raw = buf[..used].to_vec();
                buf.drain(..used);
                let Some(args) = args_of(frame) else {
                    let mut o = Vec::new();
                    encode(&Value::Error("ERR protocol".into()), &mut o);
                    sock.write_all(&o).await?;
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
                    let last = args
                        .get(2)
                        .and_then(|r| std::str::from_utf8(r).ok())
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0u64);
                    return watch_loop(sock, ha, proxy, last, buf).await;
                }
                let reply = handle_admin(&ha, &args).await;
                let mut o = Vec::new();
                encode(&reply, &mut o);
                sock.write_all(&o).await?;
                let _ = raw;
            }
            Ok(Decoded::NeedMore) => {
                let n = sock.read(&mut chunk).await?;
                if n == 0 {
                    return Ok(());
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => {
                let mut o = Vec::new();
                encode(&Value::Error("ERR protocol".into()), &mut o);
                sock.write_all(&o).await?;
                return Ok(());
            }
        }
    }
}

fn clean(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':' | b','))
}

async fn handle_admin(ha: &Ha, args: &[Vec<u8>]) -> Value {
    let cmd = args
        .first()
        .map(|c| c.to_ascii_uppercase())
        .unwrap_or_default();
    let text = |i: usize| -> Option<String> {
        args.get(i)
            .and_then(|r| std::str::from_utf8(r).ok())
            .map(String::from)
    };
    let redirect = |leader: Option<String>| match leader {
        Some(a) => Value::Error(format!("LEADER {a}")),
        None => Value::Error("ERR no leader elected yet, retry".into()),
    };
    match cmd.as_slice() {
        b"PING" => Value::Simple("PONG".into()),
        b"CPADDPROXY" => {
            let Some(a) = text(1).filter(|a| clean(a)) else {
                return Value::Error("ERR CPADDPROXY <addr>".into());
            };
            match ha.propose(Mutation::AddProxy(a)).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPADDPAIR" => {
            let Some(nodes) = text(1).filter(|a| clean(a)) else {
                return Value::Error("ERR CPADDPAIR <a,b[,c]>".into());
            };
            let range = text(2).as_deref().and_then(crate::state::parse_range);
            let pair = nodes.split(',').map(String::from).collect();
            match ha.propose(Mutation::AddPair { nodes: pair, range }).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPADDTENANT" => {
            let (Some(name), Some(token), Some(ns)) = (text(1), text(2), text(3)) else {
                return Value::Error("ERR CPADDTENANT <name> <token> <ns> [k]".into());
            };
            if !clean(&name) || !clean(&token) || !clean(&ns) {
                return Value::Error("ERR invalid name/token/ns".into());
            }
            let k: usize = text(4).and_then(|v| v.parse().ok()).unwrap_or(2);
            // ADR-0006 D1: hash BEFORE proposing — the Raft log, snapshots,
            // and every follower store only the digest.
            let token = flint_tls::sha256_hex(token.as_bytes());
            // Compute the subset from the current fleet (deterministic).
            let reg = ha.store.registry().await;
            if reg.tenants.contains_key(&name) {
                return Value::Error("ERR tenant exists".into());
            }
            if reg.tenants.values().any(|t| t.token == token) {
                return Value::Error("ERR token already in use".into());
            }
            let subset = crate::registry::shuffle_shard(&name, &reg.proxies, k);
            let reply = format!("OK tenant {name} ns {ns} subset [{}]", subset.join(","));
            match ha
                .propose(Mutation::AddTenant {
                    name,
                    token,
                    ns,
                    subset,
                })
                .await
            {
                Ok(_) => Value::Simple(reply),
                Err(l) => redirect(l),
            }
        }
        b"CPSETSUBSET" => {
            let (Some(name), Some(subset)) = (text(1), text(2)) else {
                return Value::Error("ERR CPSETSUBSET <name> <p1,p2|->".into());
            };
            let subset = if subset == "-" {
                Vec::new()
            } else {
                subset.split(',').map(String::from).collect()
            };
            match ha.propose(Mutation::SetSubset { name, subset }).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPROTATETOKEN" => {
            let (Some(name), Some(new)) = (text(1), text(2)) else {
                return Value::Error("ERR CPROTATETOKEN <name> <new-token>".into());
            };
            if !clean(&new) {
                return Value::Error("ERR invalid token".into());
            }
            let new = flint_tls::sha256_hex(new.as_bytes());
            let reg = ha.store.registry().await;
            if !reg.tenants.contains_key(&name) {
                return Value::Error("ERR no such tenant".into());
            }
            if reg
                .tenants
                .values()
                .any(|t| t.token == new || t.prev_token.as_deref() == Some(new.as_str()))
            {
                return Value::Error("ERR token already in use".into());
            }
            match ha.propose(Mutation::RotateToken { name, new }).await {
                Ok(_) => Value::Simple("OK rotated (both tokens valid until CPDROPPREV)".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPDROPPREV" => {
            let Some(name) = text(1) else {
                return Value::Error("ERR CPDROPPREV <name>".into());
            };
            match ha.propose(Mutation::DropPrev { name }).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPTENANTREADS" => {
            let (Some(name), Some(mode)) = (text(1), text(2)) else {
                return Value::Error("ERR CPTENANTREADS <name> <on|off>".into());
            };
            let on = match mode.as_str() {
                "on" => true,
                "off" => false,
                _ => return Value::Error("ERR CPTENANTREADS <name> <on|off>".into()),
            };
            match ha.propose(Mutation::SetReplicaReads { name, on }).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPTENANTFEDERATE" => {
            let (Some(name), Some(mode)) = (text(1), text(2)) else {
                return Value::Error("ERR CPTENANTFEDERATE <name> <on|off>".into());
            };
            let on = match mode.as_str() {
                "on" => true,
                "off" => false,
                _ => return Value::Error("ERR CPTENANTFEDERATE <name> <on|off>".into()),
            };
            match ha.propose(Mutation::SetFederated { name, on }).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPTENANTCACHE" => {
            let (Some(name), Some(mode)) = (text(1), text(2)) else {
                return Value::Error("ERR CPTENANTCACHE <name> <on|off>".into());
            };
            let on = match mode.as_str() {
                "on" => true,
                "off" => false,
                _ => return Value::Error("ERR CPTENANTCACHE <name> <on|off>".into()),
            };
            match ha.propose(Mutation::SetLocalCache { name, on }).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPTENANTQUOTA" => {
            let (Some(name), Some(ops), Some(bytes)) = (
                text(1),
                text(2).and_then(|v| v.parse::<u64>().ok()),
                text(3).and_then(|v| v.parse::<u64>().ok()),
            ) else {
                return Value::Error("ERR CPTENANTQUOTA <name> <ops_per_sec> <max_bytes>".into());
            };
            match ha
                .propose(Mutation::SetQuota {
                    name,
                    ops_per_sec: ops,
                    max_bytes: bytes,
                })
                .await
            {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPTENANTOVERQUOTA" => {
            let (Some(name), Some(mode)) = (text(1), text(2)) else {
                return Value::Error("ERR CPTENANTOVERQUOTA <name> <on|off>".into());
            };
            let on = match mode.as_str() {
                "on" => true,
                "off" => false,
                _ => return Value::Error("ERR CPTENANTOVERQUOTA <name> <on|off>".into()),
            };
            match ha.propose(Mutation::SetOverQuota { name, on }).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPSETPAIR" => {
            let (Some(idx), Some(nodes)) = (
                text(1).and_then(|v| v.parse::<usize>().ok()),
                text(2).filter(|a| clean(a)),
            ) else {
                return Value::Error("ERR CPSETPAIR <idx> <a,b[,c]>".into());
            };
            let nodes: Vec<String> = nodes.split(',').map(String::from).collect();
            match ha.propose(Mutation::SetPair { idx, nodes }).await {
                Ok(v) => Value::Simple(format!("OK version {v}")),
                Err(redir) => redirect(redir),
            }
        }
        b"CPTENANTUSAGE" => {
            let (Some(name), Some(bytes)) = (text(1), text(2).and_then(|v| v.parse::<u64>().ok()))
            else {
                return Value::Error("ERR CPTENANTUSAGE <name> <bytes>".into());
            };
            if let Ok(mut usage) = ha.usage.lock() {
                usage.insert(name, bytes);
            }
            Value::Simple("OK".into())
        }
        b"CPMYUSAGE" => {
            let Some(token) = text(1) else {
                return Value::Error("ERR CPMYUSAGE <token>".into());
            };
            let token = flint_tls::sha256_hex(token.as_bytes());
            let reg = ha.store.registry().await;
            let Some(t) = reg
                .tenants
                .values()
                .find(|t| t.token == token || t.prev_token.as_deref() == Some(token.as_str()))
            else {
                return Value::Error("WRONGPASS invalid token".into());
            };
            let bytes = ha
                .usage
                .lock()
                .ok()
                .and_then(|u| u.get(&t.name).copied())
                .unwrap_or(0);
            Value::Bulk(Some(
                format!(
                    "{} {} {} {} {} {} {} {}\r\n",
                    t.name,
                    t.ns,
                    t.ops_per_sec,
                    t.max_bytes,
                    t.over_quota as u8,
                    bytes,
                    t.replica_reads as u8,
                    t.local_cache as u8
                )
                .into_bytes(),
            ))
        }
        b"CPMYROTATE" => {
            let Some(token) = text(1) else {
                return Value::Error("ERR CPMYROTATE <current-token>".into());
            };
            let digest = flint_tls::sha256_hex(token.as_bytes());
            let name = {
                let reg = ha.store.registry().await;
                let Some(t) = reg.tenants.values().find(|t| t.token == digest) else {
                    return Value::Error(
                        "WRONGPASS invalid token (rotation needs the CURRENT token)".into(),
                    );
                };
                if t.prev_token.is_some() {
                    return Value::Error(
                        "ERR rotation in progress; previous token not yet drained".into(),
                    );
                }
                t.name.clone()
            };
            // Mint OUTSIDE the Raft log; propose only the digest — the log
            // and every follower never see the plaintext (the D1 property).
            let new_plain = flint_tls::mint_token();
            let new_digest = flint_tls::sha256_hex(new_plain.as_bytes());
            match ha
                .propose(Mutation::RotateToken {
                    name,
                    new: new_digest,
                })
                .await
            {
                Ok(_) => Value::Bulk(Some(new_plain.into_bytes())),
                Err(l) => redirect(l),
            }
        }
        b"CPMYCONFIG" => {
            let (Some(token), Some(setting), Some(mode)) = (text(1), text(2), text(3)) else {
                return Value::Error(
                    "ERR CPMYCONFIG <token> <replica-reads|near-cache> <on|off>".into(),
                );
            };
            let token = flint_tls::sha256_hex(token.as_bytes());
            let on = match mode.as_str() {
                "on" => true,
                "off" => false,
                _ => {
                    return Value::Error(
                        "ERR CPMYCONFIG <token> <replica-reads|near-cache> <on|off>".into(),
                    );
                }
            };
            let name =
                {
                    let reg = ha.store.registry().await;
                    match reg.tenants.values().find(|t| {
                        t.token == token || t.prev_token.as_deref() == Some(token.as_str())
                    }) {
                        Some(t) => t.name.clone(),
                        None => return Value::Error("WRONGPASS invalid token".into()),
                    }
                };
            let mutation = match setting.as_str() {
                "replica-reads" => Mutation::SetReplicaReads { name, on },
                "near-cache" => Mutation::SetLocalCache { name, on },
                _ => return Value::Error("ERR unknown setting (replica-reads|near-cache)".into()),
            };
            match ha.propose(mutation).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPTENANTS" => {
            let reg = ha.store.registry().await;
            let usage = ha.usage.lock().ok();
            let mut out = String::new();
            for t in reg.tenants.values() {
                let bytes = usage
                    .as_ref()
                    .and_then(|u| u.get(&t.name).copied())
                    .unwrap_or(0);
                out.push_str(&format!(
                    "{} {} {} {} {} {} {} {} {}\r\n",
                    t.name,
                    t.ns,
                    t.ops_per_sec,
                    t.max_bytes,
                    t.over_quota as u8,
                    bytes,
                    t.replica_reads as u8,
                    t.local_cache as u8,
                    t.prev_token.as_deref().unwrap_or("-")
                ));
            }
            Value::Bulk(Some(out.into_bytes()))
        }
        b"CPADMINTOKEN" => {
            let reg = ha.store.registry().await;
            match reg.admin_token {
                Some(t) => Value::Bulk(Some(t.into_bytes())),
                None => Value::Bulk(None),
            }
        }
        b"CPADMINROTATE" => {
            let reg = ha.store.registry().await;
            if reg.admin_prev.is_some() {
                return Value::Error(
                    "ERR admin rotation in progress; previous token not yet retired".into(),
                );
            }
            let new_plain = flint_tls::mint_token();
            match ha
                .propose(Mutation::SetAdmin {
                    token: Some(new_plain.clone()),
                    prev: reg.admin_token,
                })
                .await
            {
                Ok(_) => Value::Bulk(Some(new_plain.into_bytes())),
                Err(l) => redirect(l),
            }
        }
        b"CPADMINPREV" => {
            let reg = ha.store.registry().await;
            Value::Integer(reg.admin_prev.is_some() as i64)
        }
        b"CPADMINDROPPREV" => {
            let reg = ha.store.registry().await;
            match ha
                .propose(Mutation::SetAdmin {
                    token: reg.admin_token,
                    prev: None,
                })
                .await
            {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPPROXIES" => {
            let reg = ha.store.registry().await;
            Value::Bulk(Some(reg.proxies.join(",").into_bytes()))
        }
        b"CPDNSZONE" => {
            let Some(suffix) = text(1) else {
                return Value::Error("ERR CPDNSZONE <zone-suffix>".into());
            };
            let reg = ha.store.registry().await;
            let zone = crate::state::dns_zone(
                &suffix,
                reg.tenants.values().map(|t| (t.name.as_str(), &t.subset)),
            );
            Value::Bulk(Some(zone.into_bytes()))
        }
        // Fleet journal: node-local append (observability, not Raft intent).
        b"CPJOURNAL" => {
            let Some(line) = text(1) else {
                return Value::Error("ERR CPJOURNAL <event-json>".into());
            };
            match flint_journal::append_line(&ha.journal_path, &line) {
                Ok(()) => Value::Simple("OK".into()),
                Err(e) => Value::Error(format!("ERR journal append: {e}")),
            }
        }
        b"CPJOURNALREAD" => {
            let n = text(1).and_then(|v| v.parse().ok()).unwrap_or(50);
            let lines = flint_journal::tail(&ha.journal_path, n);
            Value::Bulk(Some(lines.join("\n").into_bytes()))
        }
        b"CPINFO" => {
            let reg = ha.store.registry().await;
            let leader = ha.raft.current_leader().await;
            let cdr = std::env::args()
                .skip_while(|a| a != "--internal-cert")
                .nth(1)
                .as_deref()
                .and_then(flint_tls::cert_days_remaining)
                .map_or_else(|| "none".into(), |d: i64| d.to_string());
            Value::Bulk(Some(
                format!(
                    "version:{}\r\nproxies:{}\r\npairs:{}\r\ntenants:{}\r\nnode:{}\r\nleader:{}\r\ncert_days_remaining:{cdr}\r\n",
                    reg.version,
                    reg.proxies.len(),
                    reg.pairs.len(),
                    reg.tenants.len(),
                    ha.node_id,
                    leader.map(|l| l.to_string()).unwrap_or_else(|| "none".into()),
                )
                .into_bytes(),
            ))
        }
        b"CPSNAPSHOT" => {
            let Some(proxy) = text(1) else {
                return Value::Error("ERR CPSNAPSHOT <proxy-addr>".into());
            };
            let reg = ha.store.registry().await;
            let (v, pairs, tenants, admin) = reg.snapshot_for(&proxy);
            snapshot_frame(v, &pairs, &tenants, &admin)
        }
        _ => Value::Error("ERR unknown control-plane command".into()),
    }
}

async fn watch_loop<S: AsyncRead + AsyncWrite + Unpin>(
    mut sock: S,
    ha: Arc<Ha>,
    proxy: String,
    mut acked: u64,
    mut buf: Vec<u8>,
) -> std::io::Result<()> {
    let mut chunk = [0u8; 8192];
    // Delta suppression: a version bump whose filtered view is unchanged is
    // acknowledged locally, not pushed (see the single-node watch()).
    let mut last_view: Option<(String, String, String)> = None;
    loop {
        let reg = ha.store.registry().await;
        if reg.version > acked {
            let (v, pairs, tenants, admin) = reg.snapshot_for(&proxy);
            if last_view.as_ref() == Some(&(pairs.clone(), tenants.clone(), admin.clone())) {
                eprintln!("watch {proxy}: suppressed push at version {v} (view unchanged)");
                acked = v;
                continue;
            }
            last_view = Some((pairs.clone(), tenants.clone(), admin.clone()));
            let mut o = Vec::new();
            encode(&snapshot_frame(v, &pairs, &tenants, &admin), &mut o);
            sock.write_all(&o).await?;
            // Read the ACK.
            loop {
                match decode(&buf) {
                    Ok(Decoded::Complete(frame, used)) => {
                        buf.drain(..used);
                        if let Value::Array(Some(items)) = frame
                            && let [Value::Bulk(Some(tag)), Value::Bulk(Some(raw))] =
                                items.as_slice()
                            && tag.eq_ignore_ascii_case(b"ACK")
                            && let Some(n) =
                                std::str::from_utf8(raw).ok().and_then(|s| s.parse().ok())
                        {
                            acked = n;
                        }
                        break;
                    }
                    Ok(Decoded::NeedMore) => {
                        let n = sock.read(&mut chunk).await?;
                        if n == 0 {
                            return Ok(());
                        }
                        buf.extend_from_slice(&chunk[..n]);
                    }
                    Err(_) => return Ok(()),
                }
            }
        } else {
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }
}
