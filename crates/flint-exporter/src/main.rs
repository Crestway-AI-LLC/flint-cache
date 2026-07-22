// SPDX-License-Identifier: Elastic-2.0
//! flint-exporter — the reference Prometheus exporter for a self-hosted
//! open Flint fleet. It polls `FLINTINFO` on each node and `PROXYSTATS` on
//! each proxy over the internal mesh (mutual TLS), and re-emits every
//! numeric field as a Prometheus gauge on `/metrics`. Point Prometheus at
//! it; point Grafana at Prometheus (see docs/self-hosting.md §3).
//!
//! It is deliberately small and dependency-light — a starting point you can
//! extend, not a product. The managed Crestway plane ships a turnkey
//! exporter and curated dashboards; this gives the open stack the same data.
//!
//! Usage:
//!   flint-exporter --port 9100 \
//!     --node 127.0.0.1:7001 --node 127.0.0.1:7002 \
//!     --proxy 127.0.0.1:7379 \
//!     [--ca certs/ca.crt --cert certs/int.crt --key certs/int.key] \
//!     [--admin-token <tok>]        # if the proxy admin surface is gated
//!
//! Omit --ca/--cert/--key for a plaintext (dev) fleet.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use flint_resp::{Decoded, Value, decode, encode};
use flint_tls::ClientConfig;

fn multi(flag: &str) -> Vec<String> {
    let a: Vec<String> = std::env::args().collect();
    a.iter()
        .enumerate()
        .filter(|(_, v)| v.as_str() == flag)
        .filter_map(|(i, _)| a.get(i + 1).cloned())
        .collect()
}
fn one(flag: &str) -> Option<String> {
    let a: Vec<String> = std::env::args().collect();
    a.iter()
        .position(|v| v == flag)
        .and_then(|i| a.get(i + 1).cloned())
}

/// One RESP round trip to `addr` over the (optionally TLS) mesh, with an
/// optional AUTH first on the same connection (for a gated proxy). Returns
/// the bulk-string body of the reply, or None on any failure.
fn scrape(
    addr: &str,
    tls: &Option<Arc<ClientConfig>>,
    cmd: &[u8],
    admin: &Option<String>,
    edge: bool,
) -> Option<String> {
    // Nodes answer on the internal mesh (fixed SNI); a proxy's PROXYSTATS
    // is on its client-facing EDGE port (SNI = the addr host). A fully
    // plaintext dev fleet uses neither (tls = None).
    let mut s = if edge {
        flint_tls::connect_edge(addr, tls).ok()?
    } else {
        flint_tls::connect(addr, tls).ok()?
    };
    s.set_read_timeout(Some(Duration::from_millis(1500))).ok()?;
    s.set_write_timeout(Some(Duration::from_millis(1500)))
        .ok()?;
    let send = |s: &mut flint_tls::Stream, args: &[&[u8]]| -> Option<Value> {
        let frame = Value::Array(Some(
            args.iter().map(|a| Value::Bulk(Some(a.to_vec()))).collect(),
        ));
        let mut out = Vec::new();
        encode(&frame, &mut out);
        s.write_all(&out).ok()?;
        let mut buf = Vec::new();
        let mut chunk = [0u8; 16 * 1024];
        loop {
            match decode(&buf) {
                Ok(Decoded::Complete(v, _)) => return Some(v),
                Ok(Decoded::NeedMore) => {
                    let n = s.read(&mut chunk).ok()?;
                    if n == 0 {
                        return None;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                Err(_) => return None,
            }
        }
    };
    if let Some(tok) = admin {
        // Best-effort: an ungated fleet answers OK/ERR either way.
        let _ = send(&mut s, &[b"AUTH", tok.as_bytes()]);
    }
    match send(&mut s, &[cmd])? {
        Value::Bulk(Some(b)) => Some(String::from_utf8_lossy(&b).into_owned()),
        _ => None,
    }
}

/// Turn a `field:value\r\n` body into Prometheus lines under `prefix`.
/// Numeric fields become gauges `{prefix}{field}{instance} value`; a
/// `role` field becomes a label on `{prefix}up`; non-numeric fields are
/// skipped. `up` is 1 when the body parsed, 0 when the target was dark.
fn emit(prefix: &str, instance: &str, body: Option<&str>, out: &mut String) {
    let Some(body) = body else {
        out.push_str(&format!("{prefix}up{{instance=\"{instance}\"}} 0\n"));
        return;
    };
    let mut role = String::new();
    let mut fields = String::new();
    for line in body.split(['\r', '\n']).filter(|l| !l.is_empty()) {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k == "role" {
            role = v.to_string();
            continue;
        }
        if let Ok(n) = v.parse::<f64>() {
            let key = k.replace('-', "_");
            fields.push_str(&format!("{prefix}{key}{{instance=\"{instance}\"}} {n}\n"));
        }
    }
    if role.is_empty() {
        out.push_str(&format!("{prefix}up{{instance=\"{instance}\"}} 1\n"));
    } else {
        out.push_str(&format!(
            "{prefix}up{{instance=\"{instance}\",role=\"{role}\"}} 1\n"
        ));
    }
    out.push_str(&fields);
}

fn render(
    nodes: &[String],
    proxies: &[String],
    tls: &Option<Arc<ClientConfig>>,
    admin: &Option<String>,
) -> String {
    let mut out = String::new();
    out.push_str("# flint-exporter: FLINTINFO/PROXYSTATS scraped live\n");
    for n in nodes {
        emit(
            "flint_",
            n,
            scrape(n, tls, b"FLINTINFO", &None, false).as_deref(),
            &mut out,
        );
    }
    for p in proxies {
        emit(
            "flint_proxy_",
            p,
            scrape(p, tls, b"PROXYSTATS", admin, true).as_deref(),
            &mut out,
        );
    }
    out
}

fn main() {
    let port: u16 = one("--port").and_then(|v| v.parse().ok()).unwrap_or(9100);
    let nodes = multi("--node");
    let proxies = multi("--proxy");
    let admin = one("--admin-token");
    if nodes.is_empty() && proxies.is_empty() {
        eprintln!(
            "usage: flint-exporter --port N --node H:P... --proxy H:P... [--ca --cert --key] [--admin-token T]"
        );
        std::process::exit(2);
    }
    let tls = match (one("--ca"), one("--cert"), one("--key")) {
        (Some(ca), Some(cert), Some(key)) => Some(
            flint_tls::client_config(&ca, &cert, &key)
                .expect("load mesh certs (--ca/--cert/--key)"),
        ),
        _ => None,
    };
    let listener = TcpListener::bind(("0.0.0.0", port)).expect("bind metrics port");
    eprintln!(
        "flint-exporter on :{port} ({} node(s), {} proxy(ies), tls={})",
        nodes.len(),
        proxies.len(),
        tls.is_some()
    );
    for stream in listener.incoming() {
        let Ok(mut s) = stream else { continue };
        let mut buf = [0u8; 1024];
        let _ = s.read(&mut buf); // consume the request line (any path -> metrics)
        let body = render(&nodes, &proxies, &tls, &admin);
        let resp = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let _ = s.write_all(resp.as_bytes());
    }
}
