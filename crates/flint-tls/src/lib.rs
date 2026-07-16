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
