//! flint-server: the data-plane node binary.
//!
//! v0: a blocking TCP server (thread per connection) speaking RESP2 over
//! `MemKv`. Deliberately simple — it exists so the conformance oracle has a
//! target from day one. The real engine (RocksDB-backed, thread-per-core)
//! replaces the internals without changing the wire behavior; the async
//! runtime choice is a recorded-later ADR.

mod commands;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use flint_resp::{Decoded, Value, decode, encode};
use flint_storage::{Kv, MemKv};

use crate::commands::Dispatcher;

fn arg(name: &str) -> Option<String> {
    std::env::args().skip_while(|a| a != name).nth(1)
}

fn main() -> std::io::Result<()> {
    let port = arg("--port")
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(6380);
    let engine = arg("--engine").unwrap_or_else(|| "mem".into());
    let store: Arc<dyn Kv> = match engine.as_str() {
        "mem" => Arc::new(MemKv::new()),
        #[cfg(feature = "rocks")]
        "rocks" => {
            let dir = arg("--data-dir").unwrap_or_else(|| "./flint-data".into());
            let kv = flint_storage::rocks::RocksKv::open(std::path::Path::new(&dir))
                .map_err(|e| std::io::Error::other(format!("rocksdb open: {e}")))?;
            eprintln!("engine=rocks data-dir={dir}");
            Arc::new(kv)
        }
        other => {
            eprintln!(
                "unknown --engine '{other}' (built-in: mem{})",
                if cfg!(feature = "rocks") {
                    ", rocks"
                } else {
                    "; build with --features rocks for rocks"
                }
            );
            std::process::exit(2);
        }
    };
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    eprintln!("flint-server listening on 127.0.0.1:{port}");
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let store: Arc<dyn Kv> = Arc::clone(&store);
        std::thread::spawn(move || {
            let _ = serve(stream, store.as_ref());
        });
    }
    Ok(())
}

fn serve(mut stream: TcpStream, store: &dyn Kv) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut chunk = [0u8; 16 * 1024];
    let mut out: Vec<u8> = Vec::with_capacity(4 * 1024);
    loop {
        // Drain every complete frame already buffered (pipelining).
        let mut consumed = 0;
        out.clear();
        loop {
            let pending = &buf[consumed..];
            let Some(&first) = pending.first() else { break };
            // Inline commands (redis-cli --pipe handshakes, telnet): any
            // line not starting with a RESP array marker, split on spaces.
            if first != b'*' {
                let Some(nl) = pending.iter().position(|&b| b == b'\n') else {
                    break; // incomplete inline line
                };
                let line = &pending[..nl];
                let line = line.strip_suffix(b"\r").unwrap_or(line);
                consumed += nl + 1;
                let args: Vec<Vec<u8>> = line
                    .split(|&b| b == b' ')
                    .filter(|part| !part.is_empty())
                    .map(|part| part.to_vec())
                    .collect();
                if args.is_empty() {
                    continue; // empty inline line: no reply, like Redis
                }
                let reply =
                    Dispatcher::new(store, flint_storage::strings::system_clock).dispatch(&args);
                encode(&reply, &mut out);
                continue;
            }
            match decode(pending) {
                Ok(Decoded::Complete(frame, used)) => {
                    consumed += used;
                    let reply = handle_frame(store, frame);
                    encode(&reply, &mut out);
                }
                Ok(Decoded::NeedMore) => break,
                Err(_) => {
                    // Protocol error: report and drop the connection, like Redis.
                    encode(&Value::Error("ERR Protocol error".into()), &mut out);
                    stream.write_all(&out)?;
                    return Ok(());
                }
            }
        }
        if consumed > 0 {
            buf.drain(..consumed);
            stream.write_all(&out)?;
        }
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(()); // client closed
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn handle_frame(store: &dyn Kv, frame: Value) -> Value {
    // Commands arrive as arrays of bulk strings.
    let Value::Array(Some(items)) = frame else {
        return Value::Error("ERR Protocol error: expected array".into());
    };
    let mut args = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::Bulk(Some(bytes)) => args.push(bytes),
            _ => return Value::Error("ERR Protocol error: expected bulk string".into()),
        }
    }
    Dispatcher::new(store, flint_storage::strings::system_clock).dispatch(&args)
}
