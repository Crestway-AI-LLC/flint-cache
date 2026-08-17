// SPDX-License-Identifier: Elastic-2.0
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

use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
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

// PEM parsing comes from rustls-pki-types, which rustls already re-exports.
// It used to come from rustls-pemfile, archived upstream in August 2025
// (RUSTSEC-2025-0134) with exactly this migration as the recommendation.
// The error text is preserved verbatim: these strings are what an operator
// sees when a cert path is wrong at boot, and they are worth more than the
// two lines saved by letting the library phrase it.
fn load_certs(path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let certs: Vec<_> = CertificateDer::pem_file_iter(path)
        .map_err(|e| pem_err(path, "cert", e))?
        .collect::<Result<_, _>>()
        .map_err(|e| pem_err(path, "cert", e))?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no certificates in {path}"),
        ));
    }
    Ok(certs)
}

fn load_key(path: &str) -> io::Result<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_file(path).map_err(|e| match e {
        // The library folds "file had no key section" into its error enum;
        // keep it distinguishable from a malformed one, because the fix is
        // different (wrong file vs corrupt file).
        rustls::pki_types::pem::Error::NoItemsFound => io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no private key in {path}"),
        ),
        e => pem_err(path, "key", e),
    })
}

/// Map a PEM error to the same shape the loader has always produced: an
/// `Io` error keeps its original kind (so "not found" still reads as not
/// found), anything else is invalid data.
fn pem_err(path: &str, what: &str, e: rustls::pki_types::pem::Error) -> io::Error {
    match e {
        rustls::pki_types::pem::Error::Io(io_err) => {
            io::Error::new(io_err.kind(), format!("open {what} {path}: {io_err}"))
        }
        other => io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parse {what} {path}: {other:?}"),
        ),
    }
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

/// SHA-256 of everything `r` yields, lowercase hex, in bounded memory.
///
/// The one-shot form above takes a slice, which is right for a token and
/// wrong for a backup object: an SST is tens of megabytes by default and
/// unbounded under a custom compaction setting, so hashing one by reading it
/// whole would make integrity checking cost as much memory as the largest
/// file anybody ever produced. Backup checksums every object twice — once on
/// the way out, once to verify — so this is the hot path for both.
pub fn sha256_stream_hex(r: &mut impl io::Read) -> io::Result<String> {
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA256);
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = r.read(&mut buf)?;
        if n == 0 {
            break;
        }
        ctx.update(&buf[..n]);
    }
    Ok(ctx
        .finish()
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
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

    /// Read WITHOUT first flushing pending TLS write data.
    ///
    /// `rustls::StreamOwned::read` (the `Read` impl above) completes prior
    /// io before reading: pending write bytes are flushed FIRST, and when
    /// the send buffer is full that flush is a `WouldBlock` — the read
    /// returns having consumed nothing. On a full-duplex hop (FLINTSYNC
    /// batches one way, ACKs the other) that couples the directions: the
    /// serving side cannot drain ACKs while its own send is jammed, the
    /// replica is blocked writing the very ACK the drain would consume,
    /// and the pair deadlocks with both send buffers full. Soak run 32
    /// died exactly there — kernel stacks showed one thread per side in
    /// `sk_stream_wait_memory`, the server cycling on its 50ms write
    /// timeout and its "drain" never consuming a byte. The volume needed
    /// to fill both buffers is why no loopback drill ever saw it.
    ///
    /// This path flushes only opportunistically (a jammed flush is skipped,
    /// not fatal) and then reads regardless, so a jammed send direction can
    /// never gate the receive direction. `WouldBlock` out of this call
    /// means "no inbound data this tick", exactly like a plain read.
    pub fn drain_read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        fn tls_read<C, D>(conn: &mut C, sock: &mut TcpStream, buf: &mut [u8]) -> io::Result<usize>
        where
            C: std::ops::DerefMut<Target = rustls::ConnectionCommon<D>>,
        {
            loop {
                match io::Read::read(&mut conn.reader(), buf) {
                    Ok(n) => return Ok(n),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {} // nothing buffered
                    Err(e) => return Err(e),
                }
                if conn.wants_write() {
                    match conn.write_tls(sock) {
                        Ok(_) => {}
                        Err(e)
                            if e.kind() == io::ErrorKind::WouldBlock
                                || e.kind() == io::ErrorKind::TimedOut => {}
                        Err(e) => return Err(e),
                    }
                }
                match conn.read_tls(sock) {
                    Ok(0) => return Ok(0), // peer closed
                    Ok(_) => {
                        conn.process_new_packets()
                            .map_err(|e| io::Error::other(format!("tls: {e}")))?;
                    }
                    // Includes WouldBlock/TimedOut from the socket read
                    // timeout: surfaced to the caller as "no data".
                    Err(e) => return Err(e),
                }
            }
        }
        match self {
            Stream::Plain(s) => io::Read::read(s, buf),
            Stream::ServerTls(s) => tls_read(&mut s.conn, &mut s.sock, buf),
            Stream::ClientTls(s) => tls_read(&mut s.conn, &mut s.sock, buf),
        }
    }

    /// Split into independently-usable read and write halves (ADR-0020).
    ///
    /// A `rustls::StreamOwned` cannot be split by cloning — reads and writes
    /// both need `&mut` on the one connection object — which is what forced
    /// the proxy's first pool into a half-duplex write-batch-then-read-batch
    /// cycle. The split here takes the other route: the TLS **state machine**
    /// goes behind a mutex with short, memory-only critical sections, and all
    /// **socket IO happens outside that lock** (the reader owns a cloned fd
    /// for reads; writes drain a shared ciphertext buffer under a separate
    /// socket-write lock). That keeps the two directions genuinely
    /// independent: a send jammed on a slow peer never stops the reader from
    /// draining inbound data — the drain_read lesson, made structural.
    ///
    /// Client-role streams only. The one caller is the proxy's backend pool;
    /// a `ServerTls` stream has no business being pooled, and refusing it
    /// here is clearer than a runtime deadlock later.
    pub fn into_duplex(self) -> io::Result<(DuplexReader, DuplexWriter)> {
        match self {
            Stream::Plain(s) => {
                let rsock = s.try_clone()?;
                Ok((
                    DuplexReader {
                        sock: rsock,
                        tls: None,
                    },
                    DuplexWriter {
                        out: Arc::new(std::sync::Mutex::new(Vec::new())),
                        tls: None,
                        sock: Arc::new(std::sync::Mutex::new(s)),
                    },
                ))
            }
            Stream::ClientTls(s) => {
                let owned = *s;
                let rsock = owned.sock.try_clone()?;
                let state = Arc::new(std::sync::Mutex::new(TlsHalf {
                    conn: owned.conn,
                    out: Vec::new(),
                }));
                Ok((
                    DuplexReader {
                        sock: rsock,
                        tls: Some(state.clone()),
                    },
                    DuplexWriter {
                        out: Arc::new(std::sync::Mutex::new(Vec::new())),
                        tls: Some(state),
                        sock: Arc::new(std::sync::Mutex::new(owned.sock)),
                    },
                ))
            }
            Stream::ServerTls(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "into_duplex is for client-role (dial-side) streams",
            )),
        }
    }
}

/// The shared TLS state machine behind a split stream: the `rustls`
/// connection plus the ciphertext it has produced but nobody has flushed.
/// Every lock hold is memory-only — encrypt, decrypt, buffer — never a
/// blocking socket call, which is the whole deadlock-freedom argument.
struct TlsHalf {
    conn: ClientConnection,
    out: Vec<u8>,
}

/// The read half. Owns its socket clone outright, so a blocking (or
/// timeout-bounded) read never holds any lock another thread wants.
pub struct DuplexReader {
    sock: TcpStream,
    tls: Option<Arc<std::sync::Mutex<TlsHalf>>>,
}

impl DuplexReader {
    /// Socket read timeout. The pool's reader thread sets a short poll here
    /// so it can run liveness checks between reads; `WouldBlock`/`TimedOut`
    /// surface to the caller as exactly that.
    pub fn set_read_timeout(&self, d: Option<std::time::Duration>) -> io::Result<()> {
        self.sock.set_read_timeout(d)
    }

    fn lock_tls(
        s: &Arc<std::sync::Mutex<TlsHalf>>,
    ) -> io::Result<std::sync::MutexGuard<'_, TlsHalf>> {
        s.lock().map_err(|_| io::Error::other("tls state poisoned"))
    }

    /// Read decrypted bytes. Plaintext sockets read straight through; TLS
    /// alternates socket reads (no lock) with state-machine work (lock, but
    /// memory only).
    pub fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let Some(state) = self.tls.clone() else {
            return io::Read::read(&mut self.sock, buf);
        };
        let mut chunk = [0u8; 64 * 1024];
        loop {
            {
                let mut st = Self::lock_tls(&state)?;
                match io::Read::read(&mut st.conn.reader(), buf) {
                    Ok(n) => return Ok(n),
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e),
                }
            }
            // Socket read OUTSIDE the lock: this is what lets the reader keep
            // draining while a writer sits blocked in write_all on a jammed
            // send — the two directions never share a wait.
            let n = io::Read::read(&mut self.sock, &mut chunk)?;
            if n == 0 {
                return Ok(0);
            }
            let mut st = Self::lock_tls(&state)?;
            let mut rd = &chunk[..n];
            while !rd.is_empty() {
                let k = st.conn.read_tls(&mut rd)?;
                st.conn
                    .process_new_packets()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
                if k == 0 {
                    break;
                }
            }
            // Post-handshake protocol bytes the peer may prompt (ticket acks,
            // key-update replies) are ENCRYPTED here but flushed by the next
            // writer flush — the reader never touches the socket-write path,
            // so ciphertext ordering has a single owner.
            while st.conn.wants_write() {
                let TlsHalf { conn, out } = &mut *st;
                conn.write_tls(out)?;
            }
        }
    }
}

/// The write half. `append` is memory-only; `flush` swaps the accumulated
/// bytes out under the socket-write lock and writes them in one call.
///
/// Sharing model (why this is `Clone` and every method takes `&self`): many
/// client threads append concurrently and each then calls `flush`. Whoever
/// holds the socket lock writes EVERYTHING accumulated so far — so under
/// contention one syscall carries several threads' frames, and under no
/// contention each frame goes out immediately. That is Envoy's
/// `encoder_buffer_` coalescing with the flush timer replaced by the callers
/// themselves: batching when busy, zero added latency when idle.
#[derive(Clone)]
pub struct DuplexWriter {
    /// Plaintext staging (plain connections only).
    out: Arc<std::sync::Mutex<Vec<u8>>>,
    /// TLS state shared with the read half (TLS connections only).
    tls: Option<Arc<std::sync::Mutex<TlsHalf>>>,
    /// The socket, locked only for writes. Swap-then-write under this lock
    /// keeps byte order equal to append order.
    sock: Arc<std::sync::Mutex<TcpStream>>,
}

impl DuplexWriter {
    /// Stage one frame: encrypt (TLS) or buffer (plain). Never touches the
    /// socket, so callers can hold their own ordering locks across it.
    pub fn append(&self, frame: &[u8]) -> io::Result<()> {
        match &self.tls {
            Some(state) => {
                let mut st = DuplexReader::lock_tls(state)?;
                io::Write::write_all(&mut st.conn.writer(), frame)?;
                while st.conn.wants_write() {
                    let TlsHalf { conn, out } = &mut *st;
                    conn.write_tls(out)?;
                }
                Ok(())
            }
            None => {
                self.out
                    .lock()
                    .map_err(|_| io::Error::other("out buffer poisoned"))?
                    .extend_from_slice(frame);
                Ok(())
            }
        }
    }

    /// Write everything staged so far. Returns the bytes this call wrote —
    /// zero when a concurrent flusher already carried them.
    pub fn flush(&self) -> io::Result<usize> {
        let mut sock = self
            .sock
            .lock()
            .map_err(|_| io::Error::other("socket lock poisoned"))?;
        // Swap under the socket lock, AFTER acquiring it: two flushers can
        // never reorder each other's bytes because the swap and the write are
        // one critical section.
        let bytes = match &self.tls {
            Some(state) => {
                let mut st = DuplexReader::lock_tls(state)?;
                std::mem::take(&mut st.out)
            }
            None => std::mem::take(
                &mut *self
                    .out
                    .lock()
                    .map_err(|_| io::Error::other("out buffer poisoned"))?,
            ),
        };
        if bytes.is_empty() {
            return Ok(0);
        }
        io::Write::write_all(&mut *sock, &bytes)?;
        Ok(bytes.len())
    }

    /// Tear the connection down from the write side: wakes a reader blocked
    /// in a socket read so a poisoned connection fails everyone promptly
    /// instead of leaking a parked thread.
    pub fn shutdown(&self) {
        if let Ok(sock) = self.sock.lock() {
            let _ = sock.shutdown(std::net::Shutdown::Both);
        }
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
    // Bounded connect. A bare TcpStream::connect has no timeout of its own,
    // so a blackholed peer (host down harder than a RST — partition, SG
    // change, hung NIC) parks the CALLER for the kernel's SYN-retry budget,
    // minutes, regardless of any read timeout set after. Every internal
    // dialer (controller sweep, server lease renewals, proxy back-ends)
    // comes through here; none of them can afford an unbounded wait.
    let sockaddr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("no addr: {addr}")))?;
    let tcp = TcpStream::connect_timeout(&sockaddr, std::time::Duration::from_secs(3))?;
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

/// The Extended Key Usage a leaf carries — read from the first certificate in
/// `path`. `None` if the file is unreadable, unparseable, or carries no EKU
/// extension at all.
///
/// Exists so a MINT can refuse to ship a co-processor leaf that is wrong,
/// rather than waiting for a drill to catch it. A co-processor leaf must be
/// `serverAuth` and NOT `clientAuth`: that absence is the whole isolation
/// argument (a node's mutual-TLS verifier refuses a `serverAuth`-only leaf as
/// a client certificate, so the co-processor cannot dial the mesh). A leaf
/// minted by copying the mesh line — `serverAuth,clientAuth`, the obvious
/// thing to do — would keep serving correctly and silently retain the
/// clientAuth that must not be there, and every server-side handshake test
/// would still pass. rustls enforces this bit on the wire; this reads the same
/// bit off the cert so the mint can assert it the moment it is produced.
pub struct CertEku {
    pub server_auth: bool,
    pub client_auth: bool,
}

pub fn cert_eku(path: &str) -> Option<CertEku> {
    cert_eku_from_pem(&std::fs::read(path).ok()?)
}

/// The file-free core of [`cert_eku`], split out so the EKU reader can be
/// unit-tested against embedded fixtures without touching the filesystem.
fn cert_eku_from_pem(pem: &[u8]) -> Option<CertEku> {
    // First certificate in the file is the leaf.
    let (_, der) = x509_parser::pem::parse_x509_pem(pem).ok()?;
    let cert = der.parse_x509().ok()?;
    let ext = cert.extended_key_usage().ok()??;
    Some(CertEku {
        server_auth: ext.value.server_auth,
        client_auth: ext.value.client_auth,
    })
}

#[cfg(test)]
mod tests {
    use super::cert_eku_from_pem;

    // Two self-signed leaves (P-256) that differ in exactly ONE thing: the EKU.
    // If `cert_eku` could not tell them apart — e.g. a bug that always reported
    // client_auth=false — the mint-time assert in flint-ctl's `resign_leaves`
    // would be vacuous, and a co-processor leaf minted by copying the mesh line
    // (ADR-0010 D2's exact failure) would ship unnoticed. This is that assert's
    // positive control: prove the reader actually discriminates, in BOTH
    // directions, so a "passing" assert is known to be checking something.
    const SERVER_ONLY: &str = "-----BEGIN CERTIFICATE-----
MIICGjCCAcCgAwIBAgIJAKz1YYmNVE8zMAoGCCqGSM49BAMCMAwxCjAIBgNVBAMM
AXQwHhcNMjYwODExMjE1ODE5WhcNMzYwODA4MjE1ODE5WjAMMQowCAYDVQQDDAF0
MIIBSzCCAQMGByqGSM49AgEwgfcCAQEwLAYHKoZIzj0BAQIhAP////8AAAABAAAA
AAAAAAAAAAAA////////////////MFsEIP////8AAAABAAAAAAAAAAAAAAAA////
///////////8BCBaxjXYqjqT57PrvVV2mIa8ZR0GsMxTsPY7zjw+J9JgSwMVAMSd
NgiG5wSTamZ44ROdJreBn36QBEEEaxfR8uEsQkf4vOblY6RA8ncDfYEt6zOg9KE5
RdiYwpZP40Li/hp/m47n60p8D54WK84zV2sxXs7LtkBoN79R9QIhAP////8AAAAA
//////////+85vqtpxeehPO5ysL8YyVRAgEBA0IABAL1u0pCNXEA554ZoH24q5N4
R851I/1BaC+bonFk2/T7FkasaTyuywqIuena8Yr18Yj7OBeYUCIH++ewQYczHjqj
FzAVMBMGA1UdJQQMMAoGCCsGAQUFBwMBMAoGCCqGSM49BAMCA0gAMEUCIH349r1G
RAL+o0xF4qapYlxXw08qybwMASFPnQPZ23pGAiEAhrWYikuqE1+8rKOyFrz9df1O
MlkuqcVf24zbBv9criw=
-----END CERTIFICATE-----
";
    const DUAL: &str = "-----BEGIN CERTIFICATE-----
MIICJTCCAcqgAwIBAgIJAK7fHzNG9fUhMAoGCCqGSM49BAMCMAwxCjAIBgNVBAMM
AXQwHhcNMjYwODExMjE1ODE5WhcNMzYwODA4MjE1ODE5WjAMMQowCAYDVQQDDAF0
MIIBSzCCAQMGByqGSM49AgEwgfcCAQEwLAYHKoZIzj0BAQIhAP////8AAAABAAAA
AAAAAAAAAAAA////////////////MFsEIP////8AAAABAAAAAAAAAAAAAAAA////
///////////8BCBaxjXYqjqT57PrvVV2mIa8ZR0GsMxTsPY7zjw+J9JgSwMVAMSd
NgiG5wSTamZ44ROdJreBn36QBEEEaxfR8uEsQkf4vOblY6RA8ncDfYEt6zOg9KE5
RdiYwpZP40Li/hp/m47n60p8D54WK84zV2sxXs7LtkBoN79R9QIhAP////8AAAAA
//////////+85vqtpxeehPO5ysL8YyVRAgEBA0IABNWpcDwSPjSh++CpLujR5of6
0Z8cmM5JcXPjI/3C2eM4yY4Zu5q/5XsablfXPkn+ZrLvDV2MDLZ0Zx/SZwo2+Uaj
ITAfMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjAKBggqhkjOPQQDAgNJ
ADBGAiEAkKTfR8gfMzWaDyPP1ncKWfIgu4F18JG7wWlDqr3yvDECIQCrEJV+9xmk
+J+nrpwyN2JwnpqBiopfGtB9+4yT1LUJyg==
-----END CERTIFICATE-----
";

    #[test]
    fn cert_eku_distinguishes_server_only_from_dual() {
        let s = cert_eku_from_pem(SERVER_ONLY.as_bytes()).expect("server-only leaf should parse");
        assert!(s.server_auth, "server-only leaf must report serverAuth");
        assert!(
            !s.client_auth,
            "server-only leaf must NOT report clientAuth — that absence is the isolation bit"
        );

        let d = cert_eku_from_pem(DUAL.as_bytes()).expect("dual leaf should parse");
        assert!(d.server_auth, "dual leaf must report serverAuth");
        assert!(
            d.client_auth,
            "dual leaf must report clientAuth — proves the reader is not stuck at false"
        );
    }
}
