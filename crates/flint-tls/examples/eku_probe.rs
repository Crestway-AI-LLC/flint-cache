// SPDX-License-Identifier: Elastic-2.0
//! What does the internal mesh actually refuse? Four dialers, one node-shaped
//! listener, measured rather than reasoned about.
//!
//! The mesh trusts MEMBERSHIP, not per-node identity: one shared leaf under
//! one internal CA (see this crate's header). A reasonable reading of that is
//! "any leaf signed by the internal CA gets in", which would make the CA the
//! only thing standing between a component and every tenant's keys. This
//! probe checks whether that reading is right, because the answer decides
//! what a component holding a *server-only* leaf — the proxy's edge cert is
//! already minted that way — is able to do on the internal mesh.
//!
//! Cases, with a control at each end so the run cannot pass by being blind:
//!   1. the mesh leaf (serverAuth,clientAuth)  — CONTROL, must be admitted
//!   2. no client credential at all
//!   3. a serverAuth-ONLY leaf from the SAME CA, offered as a client cert
//!   4. a plaintext dialer                     — CONTROL, must be refused
//!
//! Both certs must be minted with `flintctl`'s own openssl recipe, and the
//! EKUs asserted before the run — a probe that "proves" a server-only leaf is
//! refused while having accidentally minted a leaf with no EKU at all is
//! proving nothing.
//!
//! Run: cargo run -p flint-tls --example eku_probe -- <certs-dir>
//! where <certs-dir> holds ca.crt, int.{crt,key} and coproc.{crt,key}.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

fn dir() -> String {
    std::env::args()
        .nth(1)
        .expect("usage: eku_probe <certs-dir>")
}

/// Serve one connection with the mesh (mutual-TLS) server config, and report
/// whether the handshake completed.
///
/// **The handshake is LAZY.** `accept()` returning Ok proves nothing — rustls
/// does not exchange a byte until the first read or write. An earlier version
/// of this probe reported "HANDSHAKE OK" for a PLAINTEXT dialer, because it
/// took Ok from `accept` and swallowed the read error with `unwrap_or(0)`.
/// The only evidence a handshake happened is a successful read of the payload
/// the client actually sent, so that is what is reported here, errors and all.
fn serve_once(
    listener: TcpListener,
    cfg: Arc<rustls::ServerConfig>,
) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let Ok((tcp, _)) = listener.accept() else {
            return "accept failed".into();
        };
        let _ = tcp.set_read_timeout(Some(Duration::from_secs(3)));
        match flint_tls::accept(tcp, &Some(cfg)) {
            Ok(mut s) => {
                let mut b = [0u8; 16];
                match s.read(&mut b) {
                    Ok(0) => "REFUSED (peer closed, no payload)".to_string(),
                    Ok(n) => format!("ADMITTED, read {n} bytes: {:?}", &b[..n]),
                    Err(e) => format!("REFUSED at handshake: {e}"),
                }
            }
            Err(e) => format!("REFUSED at accept: {e}"),
        }
    })
}

/// Dial, then force the lazy handshake to actually run and report what
/// happened. A write alone can be buffered, so this writes AND reads back.
fn dial(addr: &str, cfg: Option<Arc<rustls::ClientConfig>>) -> String {
    let mut s = match flint_tls::connect(addr, &cfg) {
        Ok(s) => s,
        Err(e) => return format!("REFUSED at connect: {e}"),
    };
    if let Err(e) = s.write_all(b"hello") {
        return format!("REFUSED at write: {e}");
    }
    if let Err(e) = s.flush() {
        return format!("REFUSED at flush: {e}");
    }
    // The server never replies, so the expected good outcome is a clean EOF
    // or a timeout — NOT a TLS error. Anything that errors here means the
    // handshake was rejected.
    let mut b = [0u8; 1];
    match s.read(&mut b) {
        Ok(_) => "HANDSHAKE COMPLETED".to_string(),
        Err(e)
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut =>
        {
            "HANDSHAKE COMPLETED (no reply, as expected)".to_string()
        }
        Err(e) => format!("REFUSED: {e}"),
    }
}

fn port_of(l: &TcpListener) -> u16 {
    l.local_addr().expect("addr").port()
}

fn main() {
    let d = dir();
    let ca = format!("{d}/ca.crt");

    // Every case dials the SAME mesh server config — the one a node uses.
    let mk_server = || {
        flint_tls::server_config(&ca, &format!("{d}/int.crt"), &format!("{d}/int.key"))
            .expect("server_config")
    };

    println!("case                                    result");
    println!("----------------------------------------------------------------");

    let case = |name: &str, make: &dyn Fn(&str) -> String| {
        let l = TcpListener::bind("127.0.0.1:0").expect("bind");
        let p = port_of(&l);
        let h = serve_once(l, mk_server());
        let client = make(&format!("127.0.0.1:{p}"));
        let server = h.join().unwrap_or_else(|_| "server panicked".into());
        println!("{name:<38} client: {client}");
        println!("{:<38} server: {server}", "");
    };

    // 1. POSITIVE CONTROL. The real mesh leaf (serverAuth,clientAuth) must
    //    get in. Without this, "everything was refused" would prove only
    //    that the harness cannot connect at all.
    case("mesh leaf as client (CONTROL: must pass)", &|addr| {
        let cfg = flint_tls::client_config(&ca, &format!("{d}/int.crt"), &format!("{d}/int.key"))
            .expect("client_config");
        dial(addr, Some(cfg))
    });

    // 2. No client credential at all. `server_config` builds its verifier
    //    with `.build()` rather than `allow_unauthenticated()`, so this
    //    should die in the handshake, before any command exists.
    case("NO client credential", &|addr| {
        let cfg = flint_tls::edge_client_config(&ca).expect("edge_client_config");
        dial(addr, Some(cfg))
    });

    // 3. THE QUESTION: a serverAuth-ONLY leaf offered as a client
    //    certificate. Same CA, same SAN as the mesh leaf — only the EKU
    //    differs, so this isolates the EKU as the variable.
    case(
        "serverAuth-ONLY leaf as client",
        &|addr| match flint_tls::client_config(
            &ca,
            &format!("{d}/coproc.crt"),
            &format!("{d}/coproc.key"),
        ) {
            Err(e) => format!("REFUSED building client_config: {e}"),
            Ok(cfg) => dial(addr, Some(cfg)),
        },
    );

    // 4. NEGATIVE CONTROL. A plaintext dialer must be refused. If this ever
    //    reports ADMITTED, the harness is not measuring TLS at all — which
    //    is exactly what the first version of this probe did.
    case("plaintext dialer (CONTROL: must fail)", &|addr| {
        match TcpStream::connect(addr) {
            Err(e) => format!("tcp refused: {e}"),
            Ok(mut s) => {
                let _ = s.set_read_timeout(Some(Duration::from_secs(3)));
                if let Err(e) = s.write_all(b"PING\r\n") {
                    return format!("write failed: {e}");
                }
                // Bytes coming back here are the server's TLS ALERT record,
                // which a plaintext reader sees as raw bytes. That is a
                // refusal, not an admission — 0x15 is the TLS alert content
                // type. The server side of this case is the authoritative
                // half; this is only here to show the connection was made.
                let mut b = [0u8; 1];
                match s.read(&mut b) {
                    Ok(0) => "refused (server closed)".to_string(),
                    Ok(_) if b[0] == 0x15 => "refused (TLS alert record)".to_string(),
                    Ok(_) => format!("refused (server sent 0x{:02x})", b[0]),
                    Err(e) => format!("refused: {e}"),
                }
            }
        }
    });
}
