// SPDX-License-Identifier: Elastic-2.0
//! Regression test for the full-duplex TLS deadlock (soak run 32).
//!
//! `rustls::StreamOwned::read` flushes pending TLS write data before it
//! reads. On a full-duplex hop that couples the two directions: once a
//! side's send jams (peer not reading), its reads jam too — and if the peer
//! is itself blocked writing, neither side ever reads again. Kernel stacks
//! on the wedged fleet showed exactly one thread per side parked in
//! `sk_stream_wait_memory`. `Stream::drain_read` exists to break that
//! coupling: it must return inbound bytes even while this side's own send
//! is jammed mid-record.
//!
//! The test builds a real mTLS loopback pair, genuinely fills BOTH socket
//! directions until the kernel refuses more, proves the coupled jam exists
//! (the plain read returns WouldBlock despite inbound data waiting — the
//! positive control), and then proves drain_read reads through it.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

fn mint(dir: &std::path::Path) -> (String, String, String) {
    use rcgen::{
        BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, SanType,
    };
    let ca_key = KeyPair::generate().unwrap();
    let mut ca_params = CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let leaf_key = KeyPair::generate().unwrap();
    let mut leaf = CertificateParams::new(Vec::new()).unwrap();
    leaf.subject_alt_names = vec![SanType::DnsName("flint-internal".try_into().unwrap())];
    leaf.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ServerAuth,
        ExtendedKeyUsagePurpose::ClientAuth,
    ];
    let leaf_cert = leaf.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

    let ca_path = dir.join("ca.crt");
    let cert_path = dir.join("leaf.crt");
    let key_path = dir.join("leaf.key");
    std::fs::write(&ca_path, ca_cert.pem()).unwrap();
    std::fs::write(&cert_path, leaf_cert.pem()).unwrap();
    std::fs::write(&key_path, leaf_key.serialize_pem()).unwrap();
    (
        ca_path.to_str().unwrap().into(),
        cert_path.to_str().unwrap().into(),
        key_path.to_str().unwrap().into(),
    )
}

fn would_block(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

/// Write chunks until the kernel refuses more (send buffer + peer recv
/// buffer full). Returns bytes accepted. Panics if the environment never
/// blocks — a test that cannot create the jam must fail loudly, not pass
/// vacuously.
fn fill_until_jammed(s: &mut flint_tls::Stream, label: &str) -> usize {
    let chunk = [0x5au8; 64 * 1024];
    let mut sent = 0usize;
    // 256 MB cap: any sane loopback buffer pair is orders of magnitude less.
    while sent < 256 * 1024 * 1024 {
        match s.write(&chunk) {
            Ok(n) => sent += n,
            Err(e) if would_block(&e) => return sent,
            Err(e) => panic!("{label}: unexpected write error while filling: {e}"),
        }
    }
    panic!("{label}: wrote 256MB without blocking — cannot create the jam here");
}

#[test]
fn drain_read_reads_through_a_jammed_send() {
    let dir = std::env::temp_dir().join(format!("flint-tls-drain-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (ca, cert, key) = mint(&dir);

    let scfg = flint_tls::server_config(&ca, &cert, &key).unwrap();
    let ccfg = flint_tls::client_config(&ca, &cert, &key).unwrap();

    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let accept_thread = std::thread::spawn(move || l.accept().unwrap().0);
    let mut client = flint_tls::connect(&addr, &Some(ccfg)).unwrap();
    let server_tcp = accept_thread.join().unwrap();
    let mut server = flint_tls::accept(server_tcp, &Some(scfg)).unwrap();

    for s in [&client, &server] {
        s.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
        s.set_write_timeout(Some(Duration::from_millis(50)))
            .unwrap();
    }

    // Drive the lazy handshake single-threaded: alternate attempts until one
    // application byte crosses each way.
    let mut buf = [0u8; 4096];
    let mut hello_done = (false, false);
    for _ in 0..200 {
        if !hello_done.0 {
            match client.write(b"c") {
                Ok(_) => hello_done.0 = true,
                Err(e) if would_block(&e) => {}
                Err(e) => panic!("client hello: {e}"),
            }
        }
        match server.read(&mut buf) {
            Ok(n) if n > 0 => {
                hello_done.1 = true;
            }
            Ok(_) => {}
            Err(e) if would_block(&e) => {}
            Err(e) => panic!("server hello read: {e}"),
        }
        if hello_done.0 && hello_done.1 {
            break;
        }
    }
    assert!(
        hello_done.0 && hello_done.1,
        "handshake never completed over the timeout pump"
    );

    // The run-32 shape. Client first: its bytes land in the server's recv
    // queue (the "ACKs waiting to be drained"), and its own send jams (the
    // replica blocked writing). Then the server fills its send direction —
    // leaving it with pending TLS write data against a full socket.
    let from_client = fill_until_jammed(&mut client, "client");
    let from_server = fill_until_jammed(&mut server, "server");
    assert!(from_client > 0 && from_server > 0);

    // POSITIVE CONTROL: the plain read is blind here. It flushes the jammed
    // send first, gets WouldBlock, and returns without consuming the client
    // bytes that are provably waiting. Without this assert, a rustls change
    // that made read skip the flush would leave drain_read untested below.
    match server.read(&mut buf) {
        Err(e) if would_block(&e) => {}
        Ok(n) => panic!(
            "plain read returned {n} bytes through a jammed send — the coupled-jam \
             condition this test exists to create is absent, so the drain_read \
             assertion below would prove nothing"
        ),
        Err(e) => panic!("plain read: unexpected error {e}"),
    }

    // THE FIX: drain_read returns those bytes despite the jammed send.
    let n = server
        .drain_read(&mut buf)
        .expect("drain_read must read through a jammed send");
    assert!(n > 0, "drain_read returned no data");

    // And it keeps draining: consume a healthy amount to prove it is a real
    // read path, not a one-shot fluke of buffered plaintext.
    let mut total = n;
    while total < 128 * 1024 {
        match server.drain_read(&mut buf) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if would_block(&e) => break,
            Err(e) => panic!("drain_read follow-up: {e}"),
        }
    }
    assert!(
        total >= 64 * 1024,
        "drain_read stalled after {total} bytes with at least {from_client} inbound"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
