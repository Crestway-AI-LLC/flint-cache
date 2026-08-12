// SPDX-License-Identifier: Elastic-2.0
//! The vector-memory co-processor binary (ADR-0017). It accepts `FLINTFAM`
//! frames from the proxy, serves `VEC.*` against a shared [`Store`], and for a
//! write performs the durable side over a `PROXYCHAN` channel to the proxy edge
//! BEFORE committing the index change — so the in-memory index is never ahead
//! of the durable copy (ADR-0017 D2).
//!
//! v0.1 is PLAINTEXT, exactly like the ADR-0010 drill stand-ins the proxy
//! already dials over plain TCP when it runs without `--internal-cert`. Mesh
//! TLS (a serverAuth-only leaf to accept the proxy's dial, ADR-0010 D5) is a
//! follow-on. Cold-start rebuild-from-namespace with a LOADING state (D3) is
//! the next increment; until it lands a fresh co-processor starts empty — the
//! vectors are durable in KV, just not yet reloaded into the index.

use flint_resp::{Decoded, Value, decode, encode};
use flint_vec::{Persist, Plan, Store};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let port: u16 = arg(&args, "--port")
        .and_then(|s| s.parse().ok())
        .unwrap_or(6700);
    let bind = format!("0.0.0.0:{port}");
    let store = Arc::new(Mutex::new(Store::new()));
    let listener = TcpListener::bind(&bind).unwrap_or_else(|e| panic!("bind {bind}: {e}"));
    eprintln!("flint-vec co-processor on {bind} (plaintext, v0.1 flat/exact)");
    for conn in listener.incoming().flatten() {
        let store = store.clone();
        std::thread::spawn(move || serve(conn, store));
    }
}

fn arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

/// One FLINTFAM connection from the proxy (its co-proc pool keeps the
/// connection warm and sends one command at a time over it).
fn serve(mut stream: TcpStream, store: Arc<Mutex<Store>>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(60)));
    let mut buf = Vec::new();
    let mut chunk = [0u8; 16 * 1024];
    loop {
        let frame = match decode(&buf) {
            Ok(Decoded::Complete(v, used)) => {
                buf.drain(..used);
                v
            }
            Ok(Decoded::NeedMore) => match stream.read(&mut chunk) {
                Ok(0) | Err(_) => return, // peer closed / timeout
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    continue;
                }
            },
            Err(_) => return, // protocol error: drop the connection
        };
        let reply = handle_flintfam(&frame, &store);
        let mut out = Vec::new();
        encode(&reply, &mut out);
        if stream.write_all(&out).is_err() {
            return;
        }
    }
}

/// Parse `FLINTFAM <token> <callback> <ns> <cmd...>`, plan the command, and for
/// a write perform the durable side over the channel before committing.
fn handle_flintfam(frame: &Value, store: &Arc<Mutex<Store>>) -> Value {
    let Value::Array(Some(parts)) = frame else {
        return err("ERR expected FLINTFAM array");
    };
    let bulks: Option<Vec<&[u8]>> = parts
        .iter()
        .map(|p| match p {
            Value::Bulk(Some(b)) => Some(b.as_slice()),
            _ => None,
        })
        .collect();
    let Some(bulks) = bulks else {
        return err("ERR FLINTFAM parts must be bulk strings");
    };
    if bulks.len() < 5 || !bulks[0].eq_ignore_ascii_case(b"FLINTFAM") {
        return err("ERR FLINTFAM <token> <callback> <ns> <cmd...>");
    }
    let (token, callback, ns) = (bulks[1], bulks[2], bulks[3]);
    let cmd_args: Vec<Vec<u8>> = bulks[4..].iter().map(|b| b.to_vec()).collect();

    let plan = store.lock().expect("store lock").plan(ns, &cmd_args);
    match plan {
        Plan::Reply(v) => v,
        Plan::Write { persist, apply, ok } => match perform_persist(callback, token, &persist) {
            Ok(()) => {
                store.lock().expect("store lock").commit(ns, apply);
                ok
            }
            // A shed or failed durable write (e.g. -QUOTA on an over-quota
            // tenant) is relayed; the index is left untouched.
            Err(e) => e,
        },
    }
}

/// Open a single-use `PROXYCHAN` channel to the proxy edge and perform one
/// durable data command. `Ok(())` on a non-error reply; `Err(reply)` carries
/// the channel's error for the co-processor to relay to the client.
fn perform_persist(callback: &[u8], token: &[u8], persist: &Persist) -> Result<(), Value> {
    let addr = std::str::from_utf8(callback).map_err(|_| err("ERR bad callback address"))?;
    let mut ch = TcpStream::connect(addr)
        .map_err(|e| err(&format!("COPROCUNAVAIL channel dial failed: {e}")))?;
    let _ = ch.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = ch.set_write_timeout(Some(Duration::from_secs(5)));

    send_cmd(&mut ch, &[b"PROXYCHAN", token])
        .map_err(|e| err(&format!("COPROCUNAVAIL channel open failed: {e}")))?;
    if let Value::Error(e) =
        read_reply(&mut ch).map_err(|e| err(&format!("COPROCUNAVAIL channel open failed: {e}")))?
    {
        return Err(err(&format!("COPROCUNAVAIL channel refused: {e}")));
    }

    let dcmd: Vec<&[u8]> = match persist {
        Persist::Put { key, val } => vec![b"SET", key, val],
        Persist::Del { key } => vec![b"DEL", key],
    };
    send_cmd(&mut ch, &dcmd)
        .map_err(|e| err(&format!("COPROCUNAVAIL channel write failed: {e}")))?;
    match read_reply(&mut ch)
        .map_err(|e| err(&format!("COPROCUNAVAIL channel write failed: {e}")))?
    {
        // Relay the channel's own error verbatim — a -QUOTA (over storage quota)
        // is a truer, more actionable answer than a blanket COPROCUNAVAIL.
        r @ Value::Error(_) => Err(r),
        _ => Ok(()),
    }
}

fn send_cmd(ch: &mut TcpStream, parts: &[&[u8]]) -> std::io::Result<()> {
    let arr = Value::Array(Some(
        parts
            .iter()
            .map(|p| Value::Bulk(Some(p.to_vec())))
            .collect(),
    ));
    let mut out = Vec::new();
    encode(&arr, &mut out);
    ch.write_all(&out)
}

fn read_reply(ch: &mut TcpStream) -> std::io::Result<Value> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match decode(&buf) {
            Ok(Decoded::Complete(v, _)) => return Ok(v),
            Ok(Decoded::NeedMore) => {
                let n = ch.read(&mut chunk)?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "channel closed",
                    ));
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "bad channel reply",
                ));
            }
        }
    }
}

fn err(msg: &str) -> Value {
    Value::Error(msg.to_string())
}
