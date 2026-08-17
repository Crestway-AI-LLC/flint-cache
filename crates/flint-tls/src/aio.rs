// SPDX-License-Identifier: Elastic-2.0
//! Async counterparts of this crate's stream helpers (ADR-0021 stage 1).
//!
//! Same certificates, same CA, same fixed [`INTERNAL_SNI`] server identity,
//! same bounded-dial rule — only the IO model differs. A mesh that spoke
//! different TLS depending on which side happened to be async would be a
//! second security surface, so everything here routes through the SAME config
//! builders the blocking API uses.
//!
//! # Why there is no `into_duplex` here
//!
//! The blocking API grows a [`crate::DuplexReader`]/[`crate::DuplexWriter`]
//! pair because a `rustls::StreamOwned` cannot be cloned: reading and writing
//! both need `&mut` on one object, so the proxy's connection pool had to put
//! the TLS state machine behind a mutex and keep all socket IO outside it.
//! That machinery exists to buy full-duplex operation on blocking threads.
//!
//! `tokio::io::split` provides the same property natively, so reproducing the
//! mutex would be carrying a workaround across a boundary that removed the
//! problem. What DOES carry across is the property itself — a writer must keep
//! writing while a reader is already blocked — and `tests/aio_duplex.rs` pins
//! it here exactly as `tests/duplex.rs` pins it for the blocking split.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls::{ClientConfig, ServerConfig};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::{INTERNAL_SNI, ReloadableClientConfig};

/// How long a dial may take before it is a failure rather than a wait.
///
/// Mirrors the blocking `connect`, and for the same reason: a bare connect has
/// no timeout of its own, so a blackholed peer — a partition, a security-group
/// change, a hung NIC, anything harder than a RST — parks the caller for the
/// kernel's SYN-retry budget, which is minutes. On the async side that would
/// pin a task rather than a thread, but every client that task owns waits with
/// it, so the bound matters more here, not less.
const DIAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// A plaintext or TLS stream, whichever the config asked for.
///
/// The `Option<Arc<Config>>` convention is the blocking API's: `None` means
/// plaintext, so a caller that has not been given credentials cannot
/// accidentally negotiate an unverified session — it gets no TLS at all, which
/// is loud, rather than weak TLS, which is quiet.
pub enum AsyncStream {
    Plain(TcpStream),
    ServerTls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
    ClientTls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for AsyncStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            AsyncStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            AsyncStream::ServerTls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
            AsyncStream::ClientTls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AsyncStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        b: &[u8],
    ) -> Poll<io::Result<usize>> {
        match &mut *self {
            AsyncStream::Plain(s) => Pin::new(s).poll_write(cx, b),
            AsyncStream::ServerTls(s) => Pin::new(s.as_mut()).poll_write(cx, b),
            AsyncStream::ClientTls(s) => Pin::new(s.as_mut()).poll_write(cx, b),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            AsyncStream::Plain(s) => Pin::new(s).poll_flush(cx),
            AsyncStream::ServerTls(s) => Pin::new(s.as_mut()).poll_flush(cx),
            AsyncStream::ClientTls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            AsyncStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            AsyncStream::ServerTls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
            AsyncStream::ClientTls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Accept side: wrap an accepted connection, terminating TLS when configured.
pub async fn accept(tcp: TcpStream, cfg: &Option<Arc<ServerConfig>>) -> io::Result<AsyncStream> {
    match cfg {
        None => Ok(AsyncStream::Plain(tcp)),
        Some(cfg) => {
            let tls = TlsAcceptor::from(cfg.clone()).accept(tcp).await?;
            Ok(AsyncStream::ServerTls(Box::new(tls)))
        }
    }
}

/// Dial side: connect to `addr` plaintext, or over client-side TLS when `cfg`
/// is set — presenting our cert and verifying the peer against the internal CA
/// at [`INTERNAL_SNI`], which is a FIXED name rather than the dialed address,
/// so one leaf serves every internal hop.
pub async fn connect(addr: &str, cfg: &Option<Arc<ClientConfig>>) -> io::Result<AsyncStream> {
    let tcp = tokio::time::timeout(DIAL_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| {
            io::Error::new(io::ErrorKind::TimedOut, format!("dial timed out: {addr}"))
        })??;
    // Nagle off: a request is one small write and its latency is the point.
    let _ = tcp.set_nodelay(true);
    match cfg {
        None => Ok(AsyncStream::Plain(tcp)),
        Some(cfg) => {
            let name = rustls::pki_types::ServerName::try_from(INTERNAL_SNI)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("sni: {e}")))?;
            let tls = TlsConnector::from(cfg.clone()).connect(name, tcp).await?;
            Ok(AsyncStream::ClientTls(Box::new(tls)))
        }
    }
}

/// [`connect`], resolving a reloadable dial config at THIS dial.
///
/// Snapshotting per dial is what makes certificate rotation take effect
/// without a restart, and it is why callers must not cache a `ClientConfig`.
pub async fn connect_reloadable(
    addr: &str,
    cfg: &Option<Arc<ReloadableClientConfig>>,
) -> io::Result<AsyncStream> {
    let snap = cfg.as_ref().map(|r| r.current());
    connect(addr, &snap).await
}
