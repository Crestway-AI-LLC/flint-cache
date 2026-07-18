//! Internal-mesh mutual TLS (docs/design.md — the mTLS block, internal hops).
//!
//! Every internal hop (proxy↔backend, node↔node replication, proxy↔control-
//! plane, inter-node Raft) is the same shape: a Rust TCP server accepting
//! connections and Rust TCP clients dialing out, all members of one cluster.
//! The trust model is membership, not per-node identity: a shared internal
//! CA signs the cert every component presents, and each side verifies the
//! other's cert chains to that CA. A peer that can't present a CA-signed cert
//! is not in the cluster and the handshake fails.
//!
//! Both roles use the SAME cert/key/ca triple (`--internal-*`): on a listener
//! it's the server config (which *requires* a client cert — the mutual half);
//! on a dialer it's the client config (which presents that cert and verifies
//! the server). One internal cert with both server+client usage covers both.
//!
//! Server identity is a fixed name ([`INTERNAL_SNI`]) rather than per-host,
//! so a dialer verifies "this is a cluster node" independent of which IP it
//! reached — internal addresses are ephemeral; cluster membership is not.
//!
//! `ring` is the crypto provider (pure-Rust build), pinned explicitly so a
//! stray `install_default` elsewhere can never change what these configs use.

use std::io::{self, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConnection, RootCertStore, ServerConnection};

// Re-exported so callers name the config types without depending on rustls
// directly — this crate owns the internal-TLS surface.
pub use rustls::{ClientConfig, ServerConfig};

/// The fixed server name every internal cert carries as a SAN, and every
/// internal dialer verifies against — decouples the cert from the (ephemeral)
/// IP a peer happens to be reached at.
pub const INTERNAL_SNI: &str = "flint-internal";

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn load_certs(path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let f = std::fs::File::open(path)
        .map_err(|e| io::Error::new(e.kind(), format!("open cert {path}: {e}")))?;
    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(f))
        .collect::<Result<_, _>>()
        .map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("parse cert {path}: {e}"),
            )
        })?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no certificates in {path}"),
        ));
    }
    Ok(certs)
}

fn load_key(path: &str) -> io::Result<PrivateKeyDer<'static>> {
    let f = std::fs::File::open(path)
        .map_err(|e| io::Error::new(e.kind(), format!("open key {path}: {e}")))?;
    rustls_pemfile::private_key(&mut BufReader::new(f))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("parse key {path}: {e}")))?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("no private key in {path}"),
            )
        })
}

fn root_store(ca_path: &str) -> io::Result<RootCertStore> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(ca_path)? {
        roots
            .add(cert)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("add CA root: {e}")))?;
    }
    Ok(roots)
}

/// Server config for an internal listener: presents `cert`/`key` and
/// *requires* every client to present a cert chaining to `ca` (mutual auth —
/// a client with no/invalid cert fails the handshake).
pub fn server_config(ca: &str, cert: &str, key: &str) -> io::Result<Arc<ServerConfig>> {
    let roots = Arc::new(root_store(ca)?);
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(roots, provider())
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("client verifier: {e}")))?;
    let cfg = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| io::Error::other(format!("tls versions: {e}")))?
        .with_client_cert_verifier(verifier)
        .with_single_cert(load_certs(cert)?, load_key(key)?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("server cert/key: {e}")))?;
    Ok(Arc::new(cfg))
}

/// Client config for an internal dialer: presents `cert`/`key` (the mutual
/// half — the server verifies it) and verifies the server's cert chains to
/// `ca`.
/// Server-authenticated-only TLS (NO client certs) — the EDGE surface:
/// the proxy's client-facing listener and the portals' HTTPS. Distinct from
/// `server_config` (the mutual-TLS internal mesh) on purpose: edge clients
/// are browsers and redis clients that authenticate with tokens, not certs.
pub fn server_only_config(cert: &str, key: &str) -> io::Result<Arc<ServerConfig>> {
    let certs = load_certs(cert)?;
    let key = load_key(key)?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| io::Error::other(format!("tls versions: {e}")))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map(Arc::new)
        .map_err(|e| io::Error::other(format!("cert/key: {e}")))
}

/// Mint a fresh credential: 32 bytes from the system CSPRNG, lowercase
/// hex (64 chars, space-free — RESP- and env-var-friendly). Server-side
/// minting only (ADR-0006 D3): tenants never choose secrets.
pub fn mint_token() -> String {
    use ring::rand::SecureRandom;
    let rng = ring::rand::SystemRandom::new();
    let mut bytes = [0u8; 32];
    rng.fill(&mut bytes).expect("system CSPRNG");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// SHA-256 of `data`, lowercase hex — the token-at-rest digest (ADR-0006
/// D1). The registry stores and pushes DIGESTS; verifiers hash the
/// presented token and compare. A stolen digest cannot authenticate (it
/// would be hashed again), so digests are non-secret and plain equality
/// comparison is safe.
pub fn sha256_hex(data: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, data);
    digest.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

/// EDGE-client TLS: verify the server against `ca`, present no client cert
/// (edge auth is tokens, not certs), and use the dialed host as the server
/// name — how a tenant's own redis client or the console verifies the
/// proxy's edge cert (IP/localhost SANs).
pub fn edge_client_config(ca: &str) -> io::Result<Arc<ClientConfig>> {
    let cfg = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| io::Error::other(format!("tls versions: {e}")))?
        .with_root_certificates(root_store(ca)?)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

/// Connect to an EDGE TLS listener: server name = the host part of `addr`
/// (matching the edge cert's IP/DNS SANs), unlike the mesh's fixed SNI.
pub fn connect_edge(addr: &str, cfg: &Option<Arc<ClientConfig>>) -> io::Result<Stream> {
    let tcp = TcpStream::connect(addr)?;
    match cfg {
        None => Ok(Stream::Plain(tcp)),
        Some(cfg) => {
            let host = addr.split(':').next().unwrap_or(addr).to_string();
            let name = ServerName::try_from(host)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("sni: {e}")))?;
            let conn = ClientConnection::new(cfg.clone(), name)
                .map_err(|e| io::Error::other(format!("tls connect: {e}")))?;
            Ok(Stream::ClientTls(Box::new(rustls::StreamOwned::new(
                conn, tcp,
            ))))
        }
    }
}

pub fn client_config(ca: &str, cert: &str, key: &str) -> io::Result<Arc<ClientConfig>> {
    let cfg = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| io::Error::other(format!("tls versions: {e}")))?
        .with_root_certificates(root_store(ca)?)
        .with_client_auth_cert(load_certs(cert)?, load_key(key)?)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("client cert/key: {e}")))?;
    Ok(Arc::new(cfg))
}

/// A connection that is either plaintext or TLS. Both variants are `Read +
/// Write`, so call sites work over the encrypted or plain stream unchanged.
/// Boxed TLS state keeps the enum small (the rustls session is large).
pub enum Stream {
    Plain(TcpStream),
    ServerTls(Box<rustls::StreamOwned<ServerConnection, TcpStream>>),
    ClientTls(Box<rustls::StreamOwned<ClientConnection, TcpStream>>),
}

impl Read for Stream {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(s) => s.read(b),
            Stream::ServerTls(s) => s.read(b),
            Stream::ClientTls(s) => s.read(b),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        match self {
            Stream::Plain(s) => s.write(b),
            Stream::ServerTls(s) => s.write(b),
            Stream::ClientTls(s) => s.write(b),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Stream::Plain(s) => s.flush(),
            Stream::ServerTls(s) => s.flush(),
            Stream::ClientTls(s) => s.flush(),
        }
    }
}

impl Stream {
    /// The underlying `TcpStream`, but only for a plaintext connection. TLS
    /// returns `None`: a rustls session is a single stateful object and can't
    /// be `try_clone`d, so the duplex-push replication paths (which clone the
    /// socket) can't run over it yet — the caller turns `None` into a clean
    /// "not supported over TLS" error. That hop is a later increment.
    pub fn into_plain(self) -> Option<TcpStream> {
        match self {
            Stream::Plain(s) => Some(s),
            _ => None,
        }
    }

    pub fn is_tls(&self) -> bool {
        !matches!(self, Stream::Plain(_))
    }

    /// The underlying socket (TLS wraps a `TcpStream`), for socket-level knobs
    /// like timeouts that apply beneath the TLS layer.
    fn sock(&self) -> &TcpStream {
        match self {
            Stream::Plain(s) => s,
            Stream::ServerTls(s) => &s.sock,
            Stream::ClientTls(s) => &s.sock,
        }
    }

    pub fn set_read_timeout(&self, d: Option<std::time::Duration>) -> io::Result<()> {
        self.sock().set_read_timeout(d)
    }

    pub fn set_write_timeout(&self, d: Option<std::time::Duration>) -> io::Result<()> {
        self.sock().set_write_timeout(d)
    }
}

/// Accept side: wrap an already-accepted `TcpStream` as plaintext, or drive a
/// server-side TLS handshake when `cfg` is set. The handshake runs lazily on
/// the first read/write by the caller; a client that fails mutual auth errors
/// there and the connection drops.
pub fn accept(tcp: TcpStream, cfg: &Option<Arc<ServerConfig>>) -> io::Result<Stream> {
    match cfg {
        None => Ok(Stream::Plain(tcp)),
        Some(cfg) => {
            let conn = ServerConnection::new(cfg.clone())
                .map_err(|e| io::Error::other(format!("tls accept: {e}")))?;
            Ok(Stream::ServerTls(Box::new(rustls::StreamOwned::new(
                conn, tcp,
            ))))
        }
    }
}

/// Dial side: connect to `addr` plaintext, or over client-side TLS when `cfg`
/// is set (presenting our cert, verifying the server against the internal CA
/// at [`INTERNAL_SNI`]).
pub fn connect(addr: &str, cfg: &Option<Arc<ClientConfig>>) -> io::Result<Stream> {
    let tcp = TcpStream::connect(addr)?;
    match cfg {
        None => Ok(Stream::Plain(tcp)),
        Some(cfg) => {
            let name = ServerName::try_from(INTERNAL_SNI)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("sni: {e}")))?;
            let conn = ClientConnection::new(cfg.clone(), name)
                .map_err(|e| io::Error::other(format!("tls connect: {e}")))?;
            Ok(Stream::ClientTls(Box::new(rustls::StreamOwned::new(
                conn, tcp,
            ))))
        }
    }
}

/// The shared hot-reload engine (ADR-0006 D4): build once (hard error),
/// then poll the watched files' mtimes every 2s and swap in a rebuilt
/// config on change. A later bad reload is logged and the previous good
/// config is KEPT — rotation must never take a healthy listener/dialer
/// down.
fn watch_config<T: Send + Sync + 'static>(
    build: impl Fn() -> io::Result<Arc<T>> + Send + 'static,
    watched: Vec<String>,
) -> io::Result<Arc<std::sync::RwLock<Arc<T>>>> {
    let cell = Arc::new(std::sync::RwLock::new(build()?));
    let handle = Arc::clone(&cell);
    std::thread::spawn(move || {
        let mtime = |p: &String| std::fs::metadata(p).and_then(|m| m.modified()).ok();
        let mut last: Vec<_> = watched.iter().map(mtime).collect();
        loop {
            std::thread::sleep(std::time::Duration::from_secs(2));
            let now: Vec<_> = watched.iter().map(mtime).collect();
            if now != last && now.iter().all(|m| m.is_some()) {
                match build() {
                    Ok(fresh) => {
                        if let Ok(mut g) = handle.write() {
                            *g = fresh;
                        }
                        eprintln!("tls: hot-reloaded leaf certificate");
                        last = now;
                    }
                    Err(e) => eprintln!("tls: reload skipped (keeping current): {e}"),
                }
            }
        }
    });
    Ok(cell)
}

/// A server TLS config that HOT-RELOADS its leaf cert/key from disk
/// (ADR-0006 D4): a new connection picks up a freshly re-minted leaf with
/// NO process restart; existing connections keep their session (same CA —
/// both leaves verify). Rotating the leaf is then `flintctl rotate-certs`
/// (re-sign from the CA) + one poll; the CA itself stays a runbook (rare,
/// cross-signed). Covers both listener flavors: the mutual-TLS mesh
/// (`watch`) and the edge/server-only surface (`watch_edge`).
pub struct ReloadableServerConfig {
    current: Arc<std::sync::RwLock<Arc<ServerConfig>>>,
}

impl ReloadableServerConfig {
    /// Mesh listener: build from the internal triple and watch `cert`/`key`.
    /// Errors only on the INITIAL build.
    pub fn watch(ca: &str, cert: &str, key: &str) -> io::Result<Arc<Self>> {
        let (ca, cert, key) = (ca.to_string(), cert.to_string(), key.to_string());
        let watched = vec![cert.clone(), key.clone()];
        let current = watch_config(move || server_config(&ca, &cert, &key), watched)?;
        Ok(Arc::new(ReloadableServerConfig { current }))
    }

    /// Edge listener (proxy client-facing port, portal HTTPS): server-only
    /// TLS, no client certs — same reload discipline.
    pub fn watch_edge(cert: &str, key: &str) -> io::Result<Arc<Self>> {
        let (cert, key) = (cert.to_string(), key.to_string());
        let watched = vec![cert.clone(), key.clone()];
        let current = watch_config(move || server_only_config(&cert, &key), watched)?;
        Ok(Arc::new(ReloadableServerConfig { current }))
    }

    /// The current config, ready for `accept`. Read once per new connection.
    pub fn current(&self) -> Option<Arc<ServerConfig>> {
        self.current.read().ok().map(|g| g.clone())
    }
}

/// The dial-side counterpart: a mesh CLIENT config that hot-reloads its
/// leaf. Every long-lived dialer (proxy→backend/CP, node→node, controller,
/// agent, portals' internal dials) snapshots `current()` per dial, so the
/// first dial after a rotation already presents the new leaf.
pub struct ReloadableClientConfig {
    current: Arc<std::sync::RwLock<Arc<ClientConfig>>>,
}

impl ReloadableClientConfig {
    /// Build from the mesh triple and watch `cert`/`key`. Errors only on
    /// the INITIAL build.
    pub fn watch(ca: &str, cert: &str, key: &str) -> io::Result<Arc<Self>> {
        let (ca, cert, key) = (ca.to_string(), cert.to_string(), key.to_string());
        let watched = vec![cert.clone(), key.clone()];
        let current = watch_config(move || client_config(&ca, &cert, &key), watched)?;
        Ok(Arc::new(ReloadableClientConfig { current }))
    }

    /// The current config: snapshot once per dial.
    pub fn current(&self) -> Arc<ClientConfig> {
        self.current
            .read()
            .map(|g| g.clone())
            .unwrap_or_else(|e| e.into_inner().clone())
    }
}

/// `connect`, but resolving a reloadable dial config at THIS dial — the
/// drop-in for call sites that used to hold a load-once `ClientConfig`.
pub fn connect_reloadable(
    addr: &str,
    cfg: &Option<Arc<ReloadableClientConfig>>,
) -> io::Result<Stream> {
    let snap = cfg.as_ref().map(|r| r.current());
    connect(addr, &snap)
}

/// Days until the leaf certificate at `path` expires — the cert-hygiene
/// signal (ADR-0006 D4 pairs with this: the metric tells you WHEN to
/// `flintctl rotate-certs`). Negative once expired; None if the file is
/// unreadable or unparseable. Each component computes this for its OWN leaf
/// and reports it through the introspection command it already answers.
pub fn cert_days_remaining(path: &str) -> Option<i64> {
    let pem = std::fs::read(path).ok()?;
    // First certificate in the file is the leaf.
    let (_, der) = x509_parser::pem::parse_x509_pem(&pem).ok()?;
    let cert = der.parse_x509().ok()?;
    let not_after = cert.validity().not_after.timestamp();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs() as i64;
    Some((not_after - now) / 86_400)
}
