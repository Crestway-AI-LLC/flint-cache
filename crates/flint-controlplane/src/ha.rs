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
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

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

async fn read_framed(s: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let len = s.read_u32().await? as usize;
    let mut buf = vec![0u8; len];
    s.read_exact(&mut buf).await?;
    Ok(buf)
}

async fn write_framed(s: &mut TcpStream, bytes: &[u8]) -> std::io::Result<()> {
    s.write_u32(bytes.len() as u32).await?;
    s.write_all(bytes).await?;
    s.flush().await
}

// --- Network: dial a peer, ship one RPC, read its Result ---

#[derive(Clone)]
pub struct Network;

pub struct Conn {
    target: NodeId,
    addr: String,
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = Conn;
    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Conn {
        Conn {
            target,
            addr: node.addr.clone(),
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
        let mut s = TcpStream::connect(&self.addr)
            .await
            .map_err(|e| unreachable(&self.addr, e))?;
        write_framed(&mut s, &body)
            .await
            .map_err(|e| unreachable(&self.addr, e))?;
        let resp = read_framed(&mut s)
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
        let mut s = TcpStream::connect(&self.addr)
            .await
            .map_err(|e| openraft::error::RPCError::Unreachable(Unreachable::new(&e)))?;
        write_framed(&mut s, &body)
            .await
            .map_err(|e| openraft::error::RPCError::Unreachable(Unreachable::new(&e)))?;
        let resp = read_framed(&mut s)
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

async fn serve_rpc(raft: CpRaft, listener: TcpListener) {
    loop {
        let Ok((mut s, _)) = listener.accept().await else {
            continue;
        };
        let raft = raft.clone();
        tokio::spawn(async move {
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
        });
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
}

/// Build the Raft node and start the RPC server. Returns the handle the
/// client server proposes/reads through.
pub async fn start(
    node_id: NodeId,
    raft_addr: &str,
    raft_port: u16,
    state_path: std::path::PathBuf,
    peers_spec: &str,
    client_spec: &str,
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
    let store = Store::open(state_path);
    let (log_store, state_machine) = Adaptor::new(store.clone());
    let raft = Raft::new(node_id, config, Network, log_store, state_machine)
        .await
        .expect("raft new");

    let listener = TcpListener::bind(("127.0.0.1", raft_port)).await?;
    {
        let raft = raft.clone();
        tokio::spawn(serve_rpc(raft, listener));
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

fn snapshot_frame(v: u64, pairs: &str, tenants: &str) -> Value {
    Value::Array(Some(vec![
        Value::Bulk(Some(b"SNAPSHOT".to_vec())),
        Value::Integer(v as i64),
        Value::Bulk(Some(pairs.as_bytes().to_vec())),
        Value::Bulk(Some(tenants.as_bytes().to_vec())),
    ]))
}

/// Run the client RESP server: admin mutations propose through Raft (leader
/// only; a follower redirects), reads and CPWATCH serve from the local
/// applied registry.
pub async fn run_client(ha: Arc<Ha>, port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", port)).await?;
    eprintln!(
        "flint-controlplane[raft node {}] client on :{port}",
        ha.node_id
    );
    loop {
        let Ok((sock, _)) = listener.accept().await else {
            continue;
        };
        let ha = Arc::clone(&ha);
        tokio::spawn(async move {
            let _ = client_conn(sock, ha).await;
        });
    }
}

async fn client_conn(mut sock: TcpStream, ha: Arc<Ha>) -> std::io::Result<()> {
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
            let pair = nodes.split(',').map(String::from).collect();
            match ha.propose(Mutation::AddPair(pair)).await {
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
        b"CPINFO" => {
            let reg = ha.store.registry().await;
            let leader = ha.raft.current_leader().await;
            Value::Bulk(Some(
                format!(
                    "version:{}\r\nproxies:{}\r\npairs:{}\r\ntenants:{}\r\nnode:{}\r\nleader:{}\r\n",
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
            let (v, pairs, tenants) = reg.snapshot_for(&proxy);
            snapshot_frame(v, &pairs, &tenants)
        }
        _ => Value::Error("ERR unknown control-plane command".into()),
    }
}

async fn watch_loop(
    mut sock: TcpStream,
    ha: Arc<Ha>,
    proxy: String,
    mut acked: u64,
    mut buf: Vec<u8>,
) -> std::io::Result<()> {
    let mut chunk = [0u8; 8192];
    loop {
        let reg = ha.store.registry().await;
        if reg.version > acked {
            let (v, pairs, tenants) = reg.snapshot_for(&proxy);
            let mut o = Vec::new();
            encode(&snapshot_frame(v, &pairs, &tenants), &mut o);
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
