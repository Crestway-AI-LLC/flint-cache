// SPDX-License-Identifier: Elastic-2.0
//! The async split must be genuinely full-duplex (ADR-0021 stage 1).
//!
//! This is `tests/duplex.rs`'s property, carried across the IO models. The
//! blocking side needed a hand-built split — a `rustls::StreamOwned` cannot be
//! cloned, so the TLS state machine sits behind a mutex with all socket IO
//! outside it — and the async side gets the same thing from
//! `tokio::io::split`. The MACHINERY is different and should be; the property
//! is not, and it is the property the proxy's connection pool depends on.
//!
//! Why it is worth a test rather than an assumption: the proxy's first pooled
//! backend implementation wrote a batch and then blocked reading every reply
//! before writing anything more. It looked correct, it passed everything, and
//! it put a pipeline bubble in every cycle. A server that withholds all
//! replies until every frame has arrived cannot be satisfied by such an
//! implementation — it deadlocks by construction, which is the point.
#![cfg(feature = "aio")]
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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

#[tokio::test]
async fn writes_continue_while_a_read_is_blocked_over_mtls() {
    let dir = std::env::temp_dir().join(format!("flint-tls-aio-duplex-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (ca, cert, key) = mint(&dir);

    let scfg = flint_tls::server_config(&ca, &cert, &key).unwrap();
    let ccfg = flint_tls::client_config(&ca, &cert, &key).unwrap();

    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();

    // Server: read until ALL K frames have arrived, replying to none of them,
    // then echo everything back at once. The withholding IS the test.
    let server = tokio::spawn(async move {
        let (tcp, _) = l.accept().await.unwrap();
        let mut s = flint_tls::aio::accept(tcp, &Some(scfg)).await.unwrap();
        let mut got = vec![0u8; K * FRAME];
        s.read_exact(&mut got).await.unwrap();
        s.write_all(&got).await.unwrap();
        s.flush().await.unwrap();
        got
    });

    let stream = flint_tls::aio::connect(&addr, &Some(ccfg)).await.unwrap();
    let (mut rd, mut wr) = tokio::io::split(stream);

    // The reader is spawned FIRST and blocks immediately — the strongest form
    // of the property: reads already outstanding while writes keep flowing.
    let reader = tokio::spawn(async move {
        let mut got = vec![0u8; K * FRAME];
        rd.read_exact(&mut got).await.unwrap();
        got
    });

    // K frames, written one at a time and spaced, so the reader is
    // demonstrably parked between them.
    let mut sent = Vec::new();
    for i in 0..K {
        let frame = vec![i as u8; FRAME];
        wr.write_all(&frame).await.unwrap();
        wr.flush().await.unwrap();
        sent.extend_from_slice(&frame);
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // A half-duplex implementation starves here rather than failing an
    // assertion, so bound it: the deadline is the diagnostic.
    let echoed = tokio::time::timeout(Duration::from_secs(20), reader)
        .await
        .expect("reader starved — the split is not full-duplex")
        .unwrap();
    let server_got = tokio::time::timeout(Duration::from_secs(20), server)
        .await
        .expect("server never received all frames — writes stopped to wait for replies")
        .unwrap();

    assert_eq!(
        server_got, sent,
        "server saw different bytes than were sent"
    );
    assert_eq!(echoed, sent, "echo mismatch through the async split");
}
