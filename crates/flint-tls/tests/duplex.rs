// SPDX-License-Identifier: Elastic-2.0
//! The duplex split must be genuinely full-duplex (ADR-0020).
//!
//! The proxy's first backend pool was half-duplex in usage: write a batch,
//! then block reading its replies, writing nothing meanwhile. Envoy's Redis
//! client never does that — requests keep flowing out while replies flow in.
//! This test pins the property that makes the reimplementation possible: on
//! ONE mTLS connection, the write half keeps writing while the read half is
//! already blocked reading, against a server that answers NOTHING until it
//! has received every frame.
//!
//! A half-duplex implementation cannot pass: after frame 1 it would wait for
//! a reply the server will never send until frames 2..K arrive.
#![allow(clippy::unwrap_used)]

use std::io::{Read, Write};
use std::net::TcpListener;
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

const K: usize = 32;
const FRAME: usize = 128;

#[test]
fn writes_continue_while_a_read_is_blocked_over_mtls() {
    let dir = std::env::temp_dir().join(format!("flint-tls-duplex-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (ca, cert, key) = mint(&dir);

    let scfg = flint_tls::server_config(&ca, &cert, &key).unwrap();
    let ccfg = flint_tls::client_config(&ca, &cert, &key).unwrap();

    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = l.local_addr().unwrap().to_string();

    // Server: read until ALL K frames have arrived, replying to none of them
    // — then echo everything back in one burst. The withholding is the test:
    // any implementation that stops writing to wait for a reply starves it.
    let server = std::thread::spawn(move || {
        let tcp = l.accept().unwrap().0;
        let mut s = flint_tls::accept(tcp, &Some(scfg)).unwrap();
        let mut got = Vec::new();
        let mut chunk = [0u8; 16 * 1024];
        while got.len() < K * FRAME {
            match s.read(&mut chunk) {
                Ok(0) => panic!("client closed early with {} bytes", got.len()),
                Ok(n) => got.extend_from_slice(&chunk[..n]),
                Err(e) => panic!("server read: {e}"),
            }
        }
        s.write_all(&got).unwrap();
        s.flush().unwrap();
        got
    });

    let stream = flint_tls::connect(&addr, &Some(ccfg)).unwrap();
    // Force the handshake to complete before splitting (connect is lazy):
    // one write makes rustls run it. The server's byte count includes it.
    // Simpler: handshake happens on the first append+flush below — rustls
    // buffers application data until the handshake finishes, so no special
    // casing is needed. Split immediately.
    let (mut rd, wr) = stream.into_duplex().unwrap();
    rd.set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();

    // Reader thread parks FIRST, before anything is written — the strongest
    // form of the property: reads already in progress, writes still flowing.
    let reader = std::thread::spawn(move || {
        let mut got = Vec::new();
        let mut chunk = [0u8; 16 * 1024];
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while got.len() < K * FRAME {
            assert!(
                std::time::Instant::now() < deadline,
                "reader starved: {} of {} bytes — the split is not full-duplex",
                got.len(),
                K * FRAME
            );
            match rd.read(&mut chunk) {
                Ok(0) => panic!("server closed early"),
                Ok(n) => got.extend_from_slice(&chunk[..n]),
                Err(e)
                    if matches!(
                        e.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(e) => panic!("reader: {e}"),
            }
        }
        got
    });

    // Writer: K distinct frames, appended and flushed one at a time, spaced
    // out so the reader is demonstrably blocked between them.
    let mut sent = Vec::new();
    for i in 0..K {
        let frame = vec![i as u8; FRAME];
        wr.append(&frame).unwrap();
        let _ = wr.flush().unwrap();
        sent.extend_from_slice(&frame);
        std::thread::sleep(Duration::from_millis(5));
    }

    let echoed = reader.join().unwrap();
    let server_got = server.join().unwrap();
    assert_eq!(
        server_got, sent,
        "server saw different bytes than were sent"
    );
    assert_eq!(echoed, sent, "echo mismatch through the duplex split");
}
