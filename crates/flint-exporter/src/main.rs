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

/// Escape a label VALUE for the Prometheus text format: backslash, double
/// quote and newline, in that order.
///
/// These values arrive from a scraped seat, not from our own config, so
/// the exporter does not get to assume they are well-formed. One stray
/// quote in a build stamp would not corrupt that one label — it would
/// truncate or invalidate the whole exposition, and every metric on the
/// dashboard would go blank at once, which reads as a fleet outage.
fn escape_label(v: &str) -> String {
    v.replace('\\', r"\\")
        .replace('"', "\\\"")
        .replace('\n', r"\n")
}

/// Turn a `field:value\r\n` body into Prometheus lines under `prefix`.
/// Numeric fields become gauges `{prefix}{field}{instance} value`; a
/// `role` field becomes a label on `{prefix}up`; a `build` field becomes
/// `{prefix}build_info`; other non-numeric fields are skipped. `up` is 1
/// when the body parsed, 0 when the target was dark.
fn emit(prefix: &str, instance: &str, body: Option<&str>, out: &mut String) {
    let Some(body) = body else {
        out.push_str(&format!("{prefix}up{{instance=\"{instance}\"}} 0\n"));
        return;
    };
    let mut role = String::new();
    let mut build = String::new();
    let mut fields = String::new();
    for line in body.split(['\r', '\n']).filter(|l| !l.is_empty()) {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k == "role" {
            role = v.to_string();
            continue;
        }
        if k == "build" {
            build = v.to_string();
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
            "{prefix}up{{instance=\"{instance}\",role=\"{}\"}} 1\n",
            escape_label(&role)
        ));
    }
    // The build stamp (ADR-0014 D1) as its own always-1 gauge with the
    // version in a label — the standard `_build_info` shape.
    //
    // Deliberately NOT another label on `up`. A label is part of a series'
    // identity, so putting the version there would end one series and
    // start another on every roll: `up` would go stale mid-upgrade, any
    // alert watching it would fire on a healthy seat, and the before/after
    // of a canary would be two unrelated lines rather than one. Keeping
    // the version on its own metric is what makes "which build is this
    // seat on" answerable without disturbing anything watching liveness.
    if !build.is_empty() {
        out.push_str(&format!(
            "{prefix}build_info{{instance=\"{instance}\",version=\"{}\"}} 1\n",
            escape_label(&build)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact FLINTINFO prefix a node answers with, so the test breaks
    /// if the field is renamed or dropped upstream rather than quietly
    /// continuing to assert a string this file made up.
    const NODE_BODY: &str =
        "role:master\r\nrole_epoch:3\r\nbuild:v0.1.0-rc.50\r\nsst_bytes:4096\r\n";

    #[test]
    fn build_stamp_becomes_its_own_gauge() {
        let mut out = String::new();
        emit("flint_", "10.0.0.1:7001", Some(NODE_BODY), &mut out);
        assert!(
            out.contains(
                "flint_build_info{instance=\"10.0.0.1:7001\",version=\"v0.1.0-rc.50\"} 1\n"
            ),
            "no build_info line in:\n{out}"
        );
        // The version must NOT ride on `up`: that would change the series
        // identity of the liveness metric on every roll.
        assert!(
            out.contains("flint_up{instance=\"10.0.0.1:7001\",role=\"master\"} 1\n"),
            "up lost its shape:\n{out}"
        );
        assert!(
            !out.contains("flint_up{")
                || !out
                    .lines()
                    .any(|l| l.starts_with("flint_up{") && l.contains("version="))
        );
        // And the numeric fields still come through unchanged.
        assert!(out.contains("flint_sst_bytes{instance=\"10.0.0.1:7001\"} 4096\n"));
        // `build` must not also be emitted as a bare (unparseable) gauge.
        assert!(
            !out.contains("flint_build{"),
            "build leaked as a gauge:\n{out}"
        );
    }

    #[test]
    fn proxy_build_stamp_is_prefixed_separately() {
        let mut out = String::new();
        emit(
            "flint_proxy_",
            "10.0.0.9:7500",
            Some("build:v0.1.0-rc.50\r\nactive:7\r\n"),
            &mut out,
        );
        assert!(out.contains(
            "flint_proxy_build_info{instance=\"10.0.0.9:7500\",version=\"v0.1.0-rc.50\"} 1\n"
        ));
    }

    /// A dark target reports `up 0` and NOTHING else — in particular no
    /// stale build_info. A version left behind by a seat that is gone
    /// would answer "which build is running here" with a build that is
    /// not running anywhere.
    #[test]
    fn a_dark_target_carries_no_version() {
        let mut out = String::new();
        emit("flint_", "10.0.0.1:7001", None, &mut out);
        assert_eq!(out, "flint_up{instance=\"10.0.0.1:7001\"} 0\n");
    }

    /// A seat with no build stamp (a pre-ADR-0014 binary) must emit no
    /// build_info at all, rather than one with an empty version that would
    /// plot as a real series named "".
    #[test]
    fn a_missing_stamp_emits_no_series() {
        let mut out = String::new();
        emit(
            "flint_",
            "i",
            Some("role:replica\r\nsst_bytes:1\r\n"),
            &mut out,
        );
        assert!(!out.contains("build_info"), "{out}");
    }

    /// Label values come from the scraped seat, so they are escaped. An
    /// unescaped quote here would not corrupt one label — it would break
    /// the whole exposition and blank every panel at once.
    #[test]
    fn label_values_are_escaped() {
        let mut out = String::new();
        emit("flint_", "i", Some("build:v1\"; evil=\"x\r\n"), &mut out);
        assert!(
            out.contains(r#"version="v1\"; evil=\"x""#),
            "quote not escaped:\n{out}"
        );
    }
}
