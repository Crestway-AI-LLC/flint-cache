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
    /// Controllers that have announced themselves (CPCONTROLLER), node-local
    /// and never Rafted for the same reason as `usage`: a heartbeat is
    /// observability, and committing one would wake every watching proxy
    /// because a controller said hello.
    ///
    /// Node-local means each CP seat knows only the controllers that talked
    /// to IT — a controller names one `--commit-cp`. `flintctl status` already
    /// unions the rows across every CP in the inventory, so the fleet-wide
    /// answer is assembled by the reader rather than by consensus.
    pub controllers: std::sync::Mutex<crate::state::Controllers>,
    /// CPLEASE telemetry (ADR-0018) — node-local like `usage`: the leader
    /// serves renewals, so its numbers are the fleet's. (total, 128-ring of
    /// latencies in us, ring index.)
    pub lease_meter: std::sync::Mutex<(u64, Vec<u32>, usize)>,
}

/// Build the Raft node and start the RPC server. Returns the handle the
/// client server proposes/reads through.
#[allow(clippy::too_many_arguments)]
pub async fn start(
    node_id: NodeId,
    raft_addr: &str,
    bind_host: &str,
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

    // BIND THE ADDRESS THIS NODE IS DIALED AT, not loopback. Both Raft
    // listeners hardcoded 127.0.0.1, so an HA control plane was reachable
    // only from its own host — peers could never connect and no proxy or
    // flintctl on another machine could reach the client port. Invisible on
    // loopback (where 127.0.0.1 IS the right answer), which is why
    // controlplane_ha_drill and ctl_cpha_drill both pass; found by the
    // production rehearsal, where three seats on three machines started
    // cleanly and then nothing could talk to them.
    let listener = TcpListener::bind((bind_host, raft_port)).await?;
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
        controllers: std::sync::Mutex::new(crate::state::Controllers::new()),
        lease_meter: std::sync::Mutex::new((0, Vec::new(), 0)),
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

#[allow(clippy::too_many_arguments)]
fn snapshot_frame(
    v: u64,
    pairs: &str,
    tenants: &str,
    admin: &str,
    exc: &str,
    promo: &str,
    families: &str,
) -> Value {
    Value::Array(Some(vec![
        Value::Bulk(Some(b"SNAPSHOT".to_vec())),
        Value::Integer(v as i64),
        Value::Bulk(Some(pairs.as_bytes().to_vec())),
        Value::Bulk(Some(tenants.as_bytes().to_vec())),
        Value::Bulk(Some(admin.as_bytes().to_vec())),
        Value::Bulk(Some(exc.as_bytes().to_vec())),
        // 7th element (index 6): the promotion hint ("<addr>|<gen>", empty if
        // none). Same compatibility contract as the 6th: older proxies ignore
        // it, older CPs omit it, and a proxy that never sees one simply keeps
        // its pre-existing reactive rediscovery.
        Value::Bulk(Some(promo.as_bytes().to_vec())),
        // 8th element (index 7): the co-processor family route table (ADR-0010
        // D1), `PREFIX=addr;...`. ALWAYS emitted so the CP is authoritative —
        // an empty string CLEARS the proxy's table (that is how DEL of the last
        // family lands), which the proxy distinguishes from an ABSENT element
        // (older CP -> leave the table alone). Older proxies index 0..6 and
        // ignore this, so appending it is backward-safe.
        Value::Bulk(Some(families.as_bytes().to_vec())),
    ]))
}

/// Run the client RESP server: admin mutations propose through Raft (leader
/// only; a follower redirects), reads and CPWATCH serve from the local
/// applied registry.
pub async fn run_client(
    ha: Arc<Ha>,
    port: u16,
    bind_host: String,
    tls_server: Option<Arc<flint_tls::ReloadableServerConfig>>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind((bind_host.as_str(), port)).await?;
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
        b"CPDELPROXY" => {
            let Some(addr) = text(1) else {
                return Value::Error("ERR CPDELPROXY <addr>".into());
            };
            match ha.propose(Mutation::DelProxy(addr.clone())).await {
                Ok(_) => Value::Simple(format!("OK retired {addr}")),
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
        b"CPDELTENANT" => {
            let Some(name) = text(1) else {
                return Value::Error("ERR CPDELTENANT <name>".into());
            };
            let reg = ha.store.registry().await;
            let Some(t) = reg.tenants.get(&name) else {
                return Value::Error("ERR no such tenant".into());
            };
            let reply = format!("OK removed {name} ns {}", t.ns);
            match ha.propose(Mutation::DelTenant { name }).await {
                Ok(_) => Value::Simple(reply),
                Err(l) => redirect(l),
            }
        }
        b"CPSETSUBSET" => {
            let (Some(name), Some(subset)) = (text(1), text(2)) else {
                return Value::Error("ERR CPSETSUBSET <name> <p1,p2|*|->".into());
            };
            // `*` = every registered proxy, `-` = NONE (drain). `-` reads
            // like "all" and means the opposite; see CPSETSUBSET's docs.
            let subset: Vec<String> = match subset.as_str() {
                "-" => Vec::new(),
                "*" => ha.store.registry().await.proxies.clone(),
                list => list.split(',').map(String::from).collect(),
            };
            let placed = subset.len();
            match ha.propose(Mutation::SetSubset { name, subset }).await {
                Ok(_) if placed == 0 => Value::Simple(
                    "OK subset = NONE — this tenant is DRAINED and will answer -WRONGPASS at every edge"
                        .into(),
                ),
                Ok(_) => Value::Simple(format!("OK subset = {placed} proxy(ies)")),
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
        b"CPLEASE" => {
            // `CPLEASE <addr>` — a serving master renewing its own write
            // lease (ADR-0018). LEADER-ONLY, unlike the other local reads
            // here: a follower's applied state can be arbitrarily stale
            // behind a partition, and a stale +OK would keep extending a
            // superseded master's deadline — split-brain with no bound.
            // The node's renewer follows one LEADER hop per attempt.
            let Some(addr) = text(1) else {
                return Value::Error("ERR CPLEASE <addr>".into());
            };
            let leader = ha.raft.current_leader().await;
            if leader != Some(ha.node_id) {
                return redirect(leader.and_then(|id| ha.client_addrs.get(&id).cloned()));
            }
            let t0 = std::time::Instant::now();
            let reg = ha.store.registry().await;
            if let Some((_, master, _)) = reg.leases.iter().find(|(m, _, _)| m.contains(&addr)) {
                let master = master.clone();
                drop(reg);
                if let Ok(mut m) = ha.lease_meter.lock() {
                    m.0 += 1;
                    let us = t0.elapsed().as_micros().min(u128::from(u32::MAX)) as u32;
                    if m.1.len() < 128 {
                        m.1.push(us);
                    } else {
                        let i = m.2;
                        m.1[i] = us;
                    }
                    m.2 = (m.2 + 1) % 128;
                }
                return if master == addr {
                    Value::Simple("OK".into())
                } else {
                    Value::Error(format!("SUPERSEDED {master}"))
                };
            }
            let member = reg.pairs.iter().any(|p| p.contains(&addr));
            drop(reg);
            if !member {
                return Value::Error(
                    "NOPAIR address is not a member of any registered pair".into(),
                );
            }
            // First touch: adopt, durably — through Raft for the same
            // reason the record itself is Rafted.
            match ha.propose(Mutation::LeaseAdopt { addr }).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPFENCE" => {
            // `CPFENCE <addr>` — commit `addr` as its pair's master-of-record
            // BEFORE it is promoted (ADR-0018). Raft-committed; the version
            // bump wakes watching proxies (subsuming CPPROMOTED's hint role
            // on the promotion path).
            let Some(addr) = text(1) else {
                return Value::Error(ERR_CPFENCE_ARITY.into());
            };
            let reg = ha.store.registry().await;
            let member = reg.pairs.iter().any(|p| p.contains(&addr));
            drop(reg);
            if !member {
                return Value::Error(
                    "NOPAIR address is not a member of any registered pair".into(),
                );
            }
            match ha.propose(Mutation::Fence { addr: addr.clone() }).await {
                Ok(_) => Value::Simple(format!("OK fenced {addr}")),
                Err(l) => redirect(l),
            }
        }
        b"CPPROMOTED" => {
            // See the single-node CP: a wakeup hint, not routing authority.
            // Proposed through Raft like every other registry mutation so
            // followers serving CPWATCH render the same hint.
            let Some(addr) = text(1) else {
                return Value::Error("ERR CPPROMOTED <addr>".into());
            };
            match ha.propose(Mutation::Promoted { addr }).await {
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
        b"CPSETSLOT" => {
            let (Some(ns), Some(slot), Some(owner)) = (
                text(1),
                text(2).and_then(|v| v.parse::<u16>().ok()),
                text(3),
            ) else {
                return Value::Error("ERR CPSETSLOT <ns> <slot> <pair-idx|member-addr>".into());
            };
            if slot >= 16384 {
                return Value::Error("ERR slot out of range".into());
            }
            let pair: Option<u16> = {
                let reg = ha.store.registry().await;
                owner
                    .parse::<u16>()
                    .ok()
                    .or_else(|| {
                        reg.pairs
                            .iter()
                            .position(|p| p.contains(&owner))
                            .map(|i| i as u16)
                    })
                    .filter(|p| (*p as usize) < reg.pairs.len())
            };
            let Some(pair) = pair else {
                return Value::Error(
                    "ERR owner is neither a pair index nor a member address".into(),
                );
            };
            match ha.propose(Mutation::SetSlotOwner { ns, slot, pair }).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPCLEARSLOT" => {
            let (Some(ns), Some(slot)) = (text(1), text(2).and_then(|v| v.parse::<u16>().ok()))
            else {
                return Value::Error("ERR CPCLEARSLOT <ns> <slot>".into());
            };
            match ha.propose(Mutation::ClearSlotOwner { ns, slot }).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPSLOTS" => {
            let reg = ha.store.registry().await;
            Value::Array(Some(
                reg.exceptions
                    .iter()
                    .map(|(ns, lo, hi, pair)| {
                        Value::Bulk(Some(format!("{ns} {lo} {hi} {pair}").into_bytes()))
                    })
                    .collect(),
            ))
        }
        b"CPCONSOLIDATE" => match ha.propose(Mutation::ConsolidateSlots).await {
            Ok(_) => {
                let reg = ha.store.registry().await;
                Value::Integer(reg.exceptions.len() as i64)
            }
            Err(l) => redirect(l),
        },
        b"CPTENANTASYNC" => {
            let (Some(name), Some(mode)) = (text(1), text(2)) else {
                return Value::Error("ERR CPTENANTASYNC <name> <on|off>".into());
            };
            let on = match mode.as_str() {
                "on" => true,
                "off" => false,
                _ => return Value::Error("ERR CPTENANTASYNC <name> <on|off>".into()),
            };
            match ha.propose(Mutation::SetAsyncWrites { name, on }).await {
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
        b"CPFAMILY" => {
            // CPFAMILY <prefix> <host:port[,host:port...]> — register/replace a
            // co-processor family (ADR-0010 D1). Prefix uppercased so the CP
            // state is canonical (the proxy uppercases too); endpoints are
            // opaque host:port strings the proxy dials.
            let (Some(prefix), Some(addrs)) = (text(1), text(2)) else {
                return Value::Error("ERR CPFAMILY <prefix> <host:port[,host:port]>".into());
            };
            let prefix = prefix.to_ascii_uppercase();
            let endpoints: Vec<String> = addrs
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if prefix.is_empty() || endpoints.is_empty() {
                return Value::Error("ERR CPFAMILY <prefix> <host:port[,host:port]>".into());
            }
            if !crate::tenant::valid_family_prefix(&prefix) {
                return Value::Error(
                    "ERR CPFAMILY <prefix> must be printable ASCII without spaces, '=', ';' or ','"
                        .into(),
                );
            }
            match ha.propose(Mutation::SetFamily { prefix, endpoints }).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPFAMILYCLEAR" => {
            let Some(prefix) = text(1) else {
                return Value::Error("ERR CPFAMILYCLEAR <prefix>".into());
            };
            let prefix = prefix.to_ascii_uppercase();
            match ha.propose(Mutation::ClearFamily { prefix }).await {
                Ok(_) => Value::Simple("OK".into()),
                Err(l) => redirect(l),
            }
        }
        b"CPFAMILIES" => {
            // Read the registered family table (ordered, one per line):
            // "<prefix> <addr,addr>". The observability + drill read path.
            let reg = ha.store.registry().await;
            let body = reg
                .families
                .iter()
                .map(|(p, a)| format!("{p} {}", a.join(",")))
                .collect::<Vec<_>>()
                .join("\n");
            Value::Bulk(Some(body.into_bytes()))
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
                    "{} {} {} {} {} {} {} {} {}\r\n",
                    t.name,
                    t.ns,
                    t.ops_per_sec,
                    t.max_bytes,
                    t.over_quota as u8,
                    bytes,
                    t.replica_reads as u8,
                    t.local_cache as u8,
                    t.async_writes as u8
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
                    "ERR CPMYCONFIG <token> <replica-reads|near-cache|async-writes> <on|off>"
                        .into(),
                );
            };
            let token = flint_tls::sha256_hex(token.as_bytes());
            let on = match mode.as_str() {
                "on" => true,
                "off" => false,
                _ => {
                    return Value::Error(
                        "ERR CPMYCONFIG <token> <replica-reads|near-cache|async-writes> <on|off>"
                            .into(),
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
                "async-writes" => Mutation::SetAsyncWrites { name, on },
                _ => {
                    return Value::Error(
                        "ERR unknown setting (replica-reads|near-cache|async-writes)".into(),
                    );
                }
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
                    "{} {} {} {} {} {} {} {} {} {} {}\r\n",
                    t.name,
                    t.ns,
                    t.ops_per_sec,
                    t.max_bytes,
                    t.over_quota as u8,
                    bytes,
                    t.replica_reads as u8,
                    t.local_cache as u8,
                    t.prev_token.as_deref().unwrap_or("-"),
                    t.federated as u8,
                    t.async_writes as u8
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
            // Parity with the single-node CPINFO is not cosmetic. `flintctl
            // upgrade` calls assert_build("control plane", cpinfo_field(…,
            // "build:")) and DIES when that is absent, so a CPINFO without
            // `build:` made every Raft fleet unrollable — the seats swapped,
            // then the roll aborted claiming the control plane would not
            // report a build. ADR-0014 D1 had landed on one of the two
            // control planes and nothing noticed, because no drill has ever
            // upgraded a multi-seat CP.
            //
            // `registry_version:` is the same rename D1 made on the other
            // path: `version:` was never a software version, it is the
            // registry generation that drives CPWATCH. Both are emitted —
            // the alias because CPWATCH clients parse it.
            let controllers = ha
                .controllers
                .lock()
                .map(|c| crate::state::render_controllers(&c))
                .unwrap_or_default();
            let (lr, lp99) = ha
                .lease_meter
                .lock()
                .map(|m| {
                    let p99 = if m.1.is_empty() {
                        0
                    } else {
                        let mut v = m.1.clone();
                        v.sort_unstable();
                        v[(v.len() * 99 / 100).min(v.len() - 1)]
                    };
                    (m.0, p99)
                })
                .unwrap_or((0, 0));
            Value::Bulk(Some(
                format!(
                    "build:{}\r\nregistry_version:{}\r\nversion:{}\r\nproxies:{}\r\npairs:{}\r\ntenants:{}\r\nslot_exceptions:{}\r\nlease_renewals_total:{lr}\r\nlease_p99_us:{lp99}\r\nnode:{}\r\nleader:{}\r\ncert_days_remaining:{cdr}\r\n{controllers}",
                    crate::build_version(),
                    reg.version,
                    reg.version,
                    reg.proxies.len(),
                    reg.pairs.len(),
                    reg.tenants.len(),
                    reg.exceptions.len(),
                    ha.node_id,
                    leader.map(|l| l.to_string()).unwrap_or_else(|| "none".into()),
                )
                .into_bytes(),
            ))
        }
        b"CPCONTROLLER" => {
            // The HA counterpart of main.rs's handler. A controller has no
            // listener and must not gain one, so announcing itself is the
            // only way its build reaches `status` without ssh — and that has
            // to work whichever control plane it is pointed at.
            let (Some(id), Some(build)) = (text(1), text(2)) else {
                return Value::Error("ERR CPCONTROLLER <host:pid> <build>".into());
            };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            match ha.controllers.lock() {
                Ok(mut c) => {
                    crate::state::record_controller(&mut c, &id, &build, now);
                    Value::Simple(format!("OK {id} {build}"))
                }
                Err(_) => Value::Error("ERR controller registry lock".into()),
            }
        }
        b"CPSNAPSHOT" => {
            let Some(proxy) = text(1) else {
                return Value::Error("ERR CPSNAPSHOT <proxy-addr>".into());
            };
            let reg = ha.store.registry().await;
            let (v, pairs, tenants, admin, exc, promo) = reg.snapshot_for(&proxy);
            let families = reg.families_spec();
            snapshot_frame(v, &pairs, &tenants, &admin, &exc, &promo, &families)
        }
        _ => Value::Error(ERR_UNKNOWN_CP_COMMAND.into()),
    }
}

/// Two error strings that are a CROSS-CRATE CONTRACT, not just wording.
///
/// `flintctl upgrade` decides whether a control plane predates `CPFENCE` by
/// reading which of these it gets back from a bare `CPFENCE` probe (see
/// flint-ctl's `cp_lacks_cpfence`). Capability-sniffing rather than a
/// hardcoded "CPFENCE landed in rc.N" gate, which is the right instinct —
/// but it means editing either string here silently reclassifies every CP.
///
/// The dangerous direction is not the obvious one. A CP wrongly read as OLD
/// gets its control plane rolled on EVERY upgrade of EVERY healthy fleet,
/// because the probe fires before the pairs are touched.
///
/// flint-ctl cannot import these — it does not depend on this crate, and
/// adding the dependency would drag the whole Raft stack into the operator
/// tool. So they are pinned by a test HERE instead, in the crate where the
/// edit would be made, so a reworded message breaks a test rather than a
/// roll. Change either string and you must change flint-ctl to match.
///
/// That test is `cp_error_contract_tests`, AT THE END OF THIS FILE rather
/// than here beside the constants it guards. Not preference: clippy's
/// `items after a test module` fires on any item declared after a
/// `#[cfg(test)] mod`, and there is real code below this point. Tidying the
/// test back up next to these will not compile under `-D warnings` — noted
/// because that is the obvious thing to want, and the lint does not explain
/// itself.
pub const ERR_UNKNOWN_CP_COMMAND: &str = "ERR unknown control-plane command";
pub const ERR_CPFENCE_ARITY: &str = "ERR CPFENCE <addr>";

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
    // The tuple must cover EVERY pushed field — omitting one (as the
    // exception spec once was) makes changes to it silently suppressible.
    // See the single-node watch(): the promotion hint is part of the view or
    // suppression eats the one push that carries it.
    // families is GLOBAL (not per-proxy), but it IS a pushed field, so it must
    // be in this tuple or a family-only change is silently suppressed — the
    // exact trap the promotion hint hit (registry.rs promote tests).
    let mut last_view: Option<(String, String, String, String, String, String)> = None;
    loop {
        let reg = ha.store.registry().await;
        if reg.version > acked {
            let (v, pairs, tenants, admin, exc, promo) = reg.snapshot_for(&proxy);
            let families = reg.families_spec();
            if last_view.as_ref()
                == Some(&(
                    pairs.clone(),
                    tenants.clone(),
                    admin.clone(),
                    exc.clone(),
                    promo.clone(),
                    families.clone(),
                ))
            {
                eprintln!("watch {proxy}: suppressed push at version {v} (view unchanged)");
                acked = v;
                continue;
            }
            last_view = Some((
                pairs.clone(),
                tenants.clone(),
                admin.clone(),
                exc.clone(),
                promo.clone(),
                families.clone(),
            ));
            let mut o = Vec::new();
            encode(
                &snapshot_frame(v, &pairs, &tenants, &admin, &exc, &promo, &families),
                &mut o,
            );
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

#[cfg(test)]
mod cp_error_contract_tests {
    use super::*;

    /// Pinned literally, on purpose. This test has no logic to get wrong —
    /// its whole job is to fail when someone rewords the string, so that the
    /// person doing it is told flint-ctl matches on it.
    #[test]
    fn the_strings_flintctl_classifies_on_are_unchanged() {
        assert_eq!(
            ERR_UNKNOWN_CP_COMMAND, "ERR unknown control-plane command",
            "flintctl's upgrade probe reads this to mean the CP predates CPFENCE \
             and rolls the control plane first; see cp_lacks_cpfence"
        );
        assert_eq!(
            ERR_CPFENCE_ARITY, "ERR CPFENCE <addr>",
            "flintctl's upgrade probe reads this to mean the CP DOES know CPFENCE. \
             If this ever starts matching the unknown-command text, every healthy \
             fleet gets its control plane rolled on every upgrade"
        );
    }

    /// The two must stay distinguishable by the substring flint-ctl uses.
    /// A future rewording that made the arity error also contain "unknown
    /// control-plane command" would pass the equality checks above while
    /// inverting the classification.
    #[test]
    fn the_arity_error_is_not_mistakable_for_the_unknown_command_error() {
        assert!(!ERR_CPFENCE_ARITY.contains("unknown control-plane command"));
    }
}
