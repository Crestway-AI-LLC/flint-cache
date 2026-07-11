//! flint-server: the data-plane node binary.
//!
//! v0: blocking TCP, thread per connection, RESP2 (+ inline commands).
//! Engines: `--engine mem` (default) or `--engine rocks --data-dir DIR`
//! (build with `--features rocks`).
//!
//! Replication (rocks only): a master serves `FLINTSYNC <seq>` by turning
//! the connection into a push stream of WAL batches; `--replica-of HOST:PORT`
//! starts a replica that tails the master, applies batches atomically, and
//! rejects mutating commands with -READONLY.

mod commands;
mod repl_hub;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

use flint_resp::{Decoded, Value, decode, encode};
use flint_storage::{Kv, MemKv};

use crate::commands::Dispatcher;
use crate::repl_hub::ReplHub;

#[cfg(feature = "rocks")]
use flint_storage::rocks::RocksKv;

#[cfg(not(feature = "rocks"))]
type RocksHandle = ();
#[cfg(feature = "rocks")]
type RocksHandle = Arc<RocksKv>;

fn arg(name: &str) -> Option<String> {
    std::env::args().skip_while(|a| a != name).nth(1)
}

fn main() -> std::io::Result<()> {
    let port = arg("--port")
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(6380);
    let engine = arg("--engine").unwrap_or_else(|| "mem".into());
    let replica_of = arg("--replica-of");
    let read_only = replica_of.is_some();

    #[allow(unused_mut)]
    let mut rocks: Option<RocksHandle> = None;
    let store: Arc<dyn Kv> = match engine.as_str() {
        "mem" => {
            if replica_of.is_some() {
                eprintln!("--replica-of requires --engine rocks");
                std::process::exit(2);
            }
            Arc::new(MemKv::new())
        }
        #[cfg(feature = "rocks")]
        "rocks" => {
            let dir = arg("--data-dir").unwrap_or_else(|| "./flint-data".into());
            let fresh = !std::path::Path::new(&dir).join("CURRENT").exists();
            // A fresh replica seeds from a checkpoint (the spare-seeding
            // path), then tails the WAL from the copied DB's own sequence.
            if fresh && let Some(target) = &replica_of {
                replica::full_sync_download(target, std::path::Path::new(&dir))?;
            }
            let kv = RocksKv::open(std::path::Path::new(&dir))
                .map_err(|e| std::io::Error::other(format!("rocksdb open: {e}")))?;
            if fresh && replica_of.is_some() && kv.last_applied() == 0 {
                let cursor = kv.latest_seq();
                kv.set_last_applied(cursor)
                    .map_err(|e| std::io::Error::other(format!("cursor init: {e:?}")))?;
                eprintln!("full sync complete; tailing from seq {cursor}");
            }
            let kv = Arc::new(kv);
            rocks = Some(Arc::clone(&kv));
            eprintln!("engine=rocks data-dir={dir}");
            kv
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

    #[cfg(feature = "rocks")]
    if let (Some(target), Some(kv)) = (replica_of.clone(), rocks.clone()) {
        eprintln!("replica-of={target} (writes rejected with -READONLY)");
        std::thread::spawn(move || replica::run(&target, &kv));
    }

    let hub = Arc::new(ReplHub::new());
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    eprintln!("flint-server listening on 127.0.0.1:{port}");
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        let store: Arc<dyn Kv> = Arc::clone(&store);
        // Option<Arc<RocksKv>> with the feature; Option<()> (Copy) without.
        #[allow(clippy::clone_on_copy)]
        let rocks = rocks.clone();
        let hub = Arc::clone(&hub);
        std::thread::spawn(move || {
            let _ = serve(stream, store.as_ref(), read_only, rocks, &hub);
        });
    }
    Ok(())
}

fn serve(
    mut stream: TcpStream,
    store: &dyn Kv,
    read_only: bool,
    rocks: Option<RocksHandle>,
    hub: &Arc<ReplHub>,
) -> std::io::Result<()> {
    let mut buf: Vec<u8> = Vec::with_capacity(16 * 1024);
    let mut chunk = [0u8; 16 * 1024];
    let mut out: Vec<u8> = Vec::with_capacity(4 * 1024);
    loop {
        let mut consumed = 0;
        out.clear();
        loop {
            let pending = &buf[consumed..];
            let Some(&first) = pending.first() else { break };
            // Inline commands (redis-cli --pipe handshakes, telnet): any
            // line not starting with a RESP array marker, split on spaces.
            if first != b'*' {
                let Some(nl) = pending.iter().position(|&b| b == b'\n') else {
                    break;
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
                    continue;
                }
                let reply = execute(store, read_only, &rocks, hub, &args);
                encode(&reply, &mut out);
                continue;
            }
            match decode(pending) {
                Ok(Decoded::Complete(frame, used)) => {
                    consumed += used;
                    let Some(args) = frame_to_args(frame) else {
                        encode(
                            &Value::Error(
                                "ERR Protocol error: expected array of bulk strings".into(),
                            ),
                            &mut out,
                        );
                        stream.write_all(&out)?;
                        return Ok(());
                    };
                    // FLINTSYNC/FLINTFULLSYNC hijack the connection.
                    if args
                        .first()
                        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTSYNC"))
                    {
                        buf.drain(..consumed);
                        stream.write_all(&out)?;
                        return flintsync(stream, rocks, hub, &args);
                    }
                    if args
                        .first()
                        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTFULLSYNC"))
                    {
                        buf.drain(..consumed);
                        stream.write_all(&out)?;
                        return flintfullsync(stream, rocks);
                    }
                    let reply = execute(store, read_only, &rocks, hub, &args);
                    encode(&reply, &mut out);
                }
                Ok(Decoded::NeedMore) => break,
                Err(_) => {
                    encode(&Value::Error("ERR Protocol error".into()), &mut out);
                    stream.write_all(&out)?;
                    return Ok(());
                }
            }
        }
        if consumed > 0 {
            buf.drain(..consumed);
            if !out.is_empty() {
                stream.write_all(&out)?;
            }
        }
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

fn execute(
    store: &dyn Kv,
    read_only: bool,
    rocks: &Option<RocksHandle>,
    hub: &Arc<ReplHub>,
    args: &[Vec<u8>],
) -> Value {
    let is_write = args
        .first()
        .is_some_and(|name| commands::is_write_command(name));
    if read_only && is_write {
        return Value::Error("READONLY You can't write against a read only replica.".into());
    }
    if args
        .first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"FLINTINFO"))
    {
        return flintinfo(read_only, rocks, hub);
    }
    // Lag-cap backpressure: the write path enforces the RPO bound.
    if is_write && !read_only {
        let now = flint_storage::strings::system_clock();
        match hub.lag_ms(now) {
            Some(lag) if lag >= repl_hub::LAG_HARD_MS => {
                return Value::Error(
                    "THROTTLED replication lag exceeds limit, retry with backoff".into(),
                );
            }
            Some(lag) if lag >= repl_hub::LAG_SOFT_MS => {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            _ => {}
        }
    }
    Dispatcher::new(store, flint_storage::strings::system_clock).dispatch(args)
}

#[cfg(feature = "rocks")]
fn flintinfo(read_only: bool, rocks: &Option<RocksHandle>, hub: &Arc<ReplHub>) -> Value {
    let now = flint_storage::strings::system_clock();
    let latest = rocks.as_ref().map(|kv| kv.latest_seq()).unwrap_or(0);
    let last_applied = rocks.as_ref().map(|kv| kv.last_applied()).unwrap_or(0);
    let info = format!(
        "role:{}\r\nlatest_seq:{latest}\r\nlast_applied:{last_applied}\r\nacked_seq:{}\r\nlive_replica:{}\r\nlag_ms:{}\r\n",
        if read_only { "replica" } else { "master" },
        hub.acked(),
        hub.has_live_replica(now) as u8,
        hub.lag_ms(now)
            .map_or_else(|| "none".into(), |l| l.to_string()),
    );
    Value::Bulk(Some(info.into_bytes()))
}

#[cfg(not(feature = "rocks"))]
fn flintinfo(read_only: bool, _rocks: &Option<RocksHandle>, hub: &Arc<ReplHub>) -> Value {
    let now = flint_storage::strings::system_clock();
    let info = format!(
        "role:{}\r\nlive_replica:{}\r\n",
        if read_only { "replica" } else { "master" },
        hub.has_live_replica(now) as u8,
    );
    Value::Bulk(Some(info.into_bytes()))
}

fn frame_to_args(frame: Value) -> Option<Vec<Vec<u8>>> {
    let Value::Array(Some(items)) = frame else {
        return None;
    };
    let mut args = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Value::Bulk(Some(bytes)) => args.push(bytes),
            _ => return None,
        }
    }
    Some(args)
}

/// Master side of replication: stream WAL batches from the requested
/// sequence until the replica disconnects.
#[cfg(feature = "rocks")]
fn flintsync(
    mut stream: TcpStream,
    rocks: Option<RocksHandle>,
    hub: &Arc<ReplHub>,
    args: &[Vec<u8>],
) -> std::io::Result<()> {
    use flint_storage::repl::{ReplError, ReplOp};

    let Some(kv) = rocks else {
        let mut out = Vec::new();
        encode(
            &Value::Error("ERR FLINTSYNC requires the rocks engine".into()),
            &mut out,
        );
        return stream.write_all(&out);
    };
    let mut cursor: u64 = args
        .get(1)
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut out = Vec::new();
    encode(&Value::Simple(format!("FLINTSYNC-OK {cursor}")), &mut out);
    stream.write_all(&out)?;
    eprintln!("replica connected, streaming from seq {cursor}");
    // ACK reader: the replica confirms applied sequences on the same socket.
    {
        let reader = stream.try_clone()?;
        let hub = Arc::clone(hub);
        std::thread::spawn(move || ack_reader(reader, &hub));
    }
    loop {
        hub.record_sample(kv.latest_seq(), flint_storage::strings::system_clock());
        match kv.updates_since(cursor) {
            Ok(batches) if !batches.is_empty() => {
                out.clear();
                for batch in &batches {
                    let ops: Vec<Value> = batch
                        .ops
                        .iter()
                        .map(|op| match op {
                            ReplOp::Put { key, value } => Value::Array(Some(vec![
                                Value::Bulk(Some(b"P".to_vec())),
                                Value::Bulk(Some(key.clone())),
                                Value::Bulk(Some(value.clone())),
                            ])),
                            ReplOp::Delete { key } => Value::Array(Some(vec![
                                Value::Bulk(Some(b"D".to_vec())),
                                Value::Bulk(Some(key.clone())),
                            ])),
                        })
                        .collect();
                    let frame = Value::Array(Some(vec![
                        Value::Integer(batch.last_seq as i64),
                        Value::Array(Some(ops)),
                    ]));
                    encode(&frame, &mut out);
                    cursor = batch.last_seq;
                }
                stream.write_all(&out)?;
            }
            Ok(_) => std::thread::sleep(std::time::Duration::from_millis(20)),
            Err(ReplError::WalGap(e)) => {
                out.clear();
                encode(
                    &Value::Error(format!("WALGAP full sync required: {e}")),
                    &mut out,
                );
                stream.write_all(&out)?;
                return Ok(());
            }
            Err(ReplError::Storage(e)) => {
                eprintln!("replication stream error: {e}");
                return Ok(());
            }
        }
    }
}

#[cfg(feature = "rocks")]
fn ack_reader(mut stream: TcpStream, hub: &Arc<ReplHub>) {
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        match decode(&buf) {
            Ok(Decoded::Complete(frame, used)) => {
                buf.drain(..used);
                if let Value::Array(Some(items)) = frame
                    && let [Value::Bulk(Some(tag)), Value::Bulk(Some(raw))] = items.as_slice()
                    && tag.eq_ignore_ascii_case(b"ACK")
                    && let Some(seq) = std::str::from_utf8(raw).ok().and_then(|s| s.parse().ok())
                {
                    hub.record_ack(seq, flint_storage::strings::system_clock());
                }
            }
            Ok(Decoded::NeedMore) => {
                let Ok(n) = stream.read(&mut chunk) else {
                    return;
                };
                if n == 0 {
                    return;
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(_) => return,
        }
    }
}

/// Master side of a checkpoint full sync: stream every file of a fresh
/// checkpoint, then FULLSYNC-END. v0 buffers each file in memory (fine at
/// drill scale; chunked streaming is a later refinement).
#[cfg(feature = "rocks")]
fn flintfullsync(mut stream: TcpStream, rocks: Option<RocksHandle>) -> std::io::Result<()> {
    let mut out = Vec::new();
    let Some(kv) = rocks else {
        encode(
            &Value::Error("ERR FLINTFULLSYNC requires the rocks engine".into()),
            &mut out,
        );
        return stream.write_all(&out);
    };
    let ckpt = std::env::temp_dir().join(format!(
        "flint-fullsync-{}-{}",
        std::process::id(),
        flint_storage::strings::system_clock()
    ));
    kv.checkpoint_to(&ckpt)
        .map_err(|e| std::io::Error::other(format!("checkpoint: {e}")))?;
    let result = (|| -> std::io::Result<()> {
        for entry in std::fs::read_dir(&ckpt)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            let bytes = std::fs::read(entry.path())?;
            out.clear();
            encode(
                &Value::Array(Some(vec![
                    Value::Bulk(Some(b"F".to_vec())),
                    Value::Bulk(Some(name.into_bytes())),
                    Value::Bulk(Some(bytes)),
                ])),
                &mut out,
            );
            stream.write_all(&out)?;
        }
        out.clear();
        encode(&Value::Simple("FULLSYNC-END".into()), &mut out);
        stream.write_all(&out)
    })();
    let _ = std::fs::remove_dir_all(&ckpt);
    eprintln!("full sync served");
    result
}

#[cfg(not(feature = "rocks"))]
fn flintfullsync(mut stream: TcpStream, _rocks: Option<RocksHandle>) -> std::io::Result<()> {
    let mut out = Vec::new();
    encode(
        &Value::Error("ERR FLINTFULLSYNC requires a build with --features rocks".into()),
        &mut out,
    );
    stream.write_all(&out)
}

#[cfg(not(feature = "rocks"))]
fn flintsync(
    mut stream: TcpStream,
    _rocks: Option<RocksHandle>,
    _hub: &Arc<ReplHub>,
    _args: &[Vec<u8>],
) -> std::io::Result<()> {
    let mut out = Vec::new();
    encode(
        &Value::Error("ERR FLINTSYNC requires a build with --features rocks".into()),
        &mut out,
    );
    stream.write_all(&out)
}

/// Replica side: connect, request the tail from our durable cursor, apply
/// batches atomically; reconnect with backoff on any error.
#[cfg(feature = "rocks")]
mod replica {
    use super::*;
    use flint_storage::repl::{ReplBatch, ReplOp};

    pub fn run(target: &str, kv: &Arc<RocksKv>) {
        loop {
            if let Err(e) = tail_once(target, kv) {
                eprintln!("replication link lost ({e}); reconnecting in 1s");
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }

    /// Download a checkpoint into `dir` before the DB is first opened.
    pub fn full_sync_download(target: &str, dir: &std::path::Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        let mut stream = TcpStream::connect(target)?;
        let mut out = Vec::new();
        encode(
            &Value::Array(Some(vec![Value::Bulk(Some(b"FLINTFULLSYNC".to_vec()))])),
            &mut out,
        );
        stream.write_all(&out)?;
        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 256 * 1024];
        let mut files = 0u32;
        loop {
            match decode(&buf) {
                Ok(Decoded::Complete(frame, used)) => {
                    buf.drain(..used);
                    match frame {
                        Value::Simple(s) if s == "FULLSYNC-END" => {
                            eprintln!("full sync: received {files} files");
                            return Ok(());
                        }
                        Value::Error(e) => {
                            return Err(std::io::Error::other(format!("master error: {e}")));
                        }
                        Value::Array(Some(items)) => {
                            let [
                                Value::Bulk(Some(tag)),
                                Value::Bulk(Some(name)),
                                Value::Bulk(Some(bytes)),
                            ] = items.as_slice()
                            else {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "malformed full-sync frame",
                                ));
                            };
                            if tag != b"F" {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "unexpected full-sync tag",
                                ));
                            }
                            let fname = String::from_utf8_lossy(name);
                            // Checkpoint dirs are flat; reject path tricks.
                            if fname.contains('/') || fname.contains("..") {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "unsafe file name in full sync",
                                ));
                            }
                            std::fs::write(dir.join(fname.as_ref()), bytes)?;
                            files += 1;
                        }
                        _ => {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "malformed full-sync frame",
                            ));
                        }
                    }
                }
                Ok(Decoded::NeedMore) => {
                    let n = stream.read(&mut chunk)?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "master closed during full sync",
                        ));
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("full-sync protocol error: {e:?}"),
                    ));
                }
            }
        }
    }

    fn tail_once(target: &str, kv: &Arc<RocksKv>) -> std::io::Result<()> {
        let mut stream = TcpStream::connect(target)?;
        let cursor = kv.last_applied();
        let mut out = Vec::new();
        encode(
            &Value::Array(Some(vec![
                Value::Bulk(Some(b"FLINTSYNC".to_vec())),
                Value::Bulk(Some(cursor.to_string().into_bytes())),
            ])),
            &mut out,
        );
        stream.write_all(&out)?;
        eprintln!("replicating from {target} starting at seq {cursor}");

        let mut buf: Vec<u8> = Vec::new();
        let mut chunk = [0u8; 64 * 1024];
        loop {
            match decode(&buf) {
                Ok(Decoded::Complete(frame, used)) => {
                    buf.drain(..used);
                    match frame {
                        Value::Simple(s) if s.starts_with("FLINTSYNC-OK") => {
                            out.clear();
                            encode(
                                &Value::Array(Some(vec![
                                    Value::Bulk(Some(b"ACK".to_vec())),
                                    Value::Bulk(Some(cursor.to_string().into_bytes())),
                                ])),
                                &mut out,
                            );
                            stream.write_all(&out)?;
                        }
                        Value::Error(e) => {
                            return Err(std::io::Error::other(format!("master error: {e}")));
                        }
                        other => {
                            let batch = parse_batch(other).ok_or_else(|| {
                                std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    "malformed replication frame",
                                )
                            })?;
                            kv.apply_batch(&batch)
                                .map_err(|e| std::io::Error::other(format!("apply: {e:?}")))?;
                            out.clear();
                            encode(
                                &Value::Array(Some(vec![
                                    Value::Bulk(Some(b"ACK".to_vec())),
                                    Value::Bulk(Some(batch.last_seq.to_string().into_bytes())),
                                ])),
                                &mut out,
                            );
                            stream.write_all(&out)?;
                        }
                    }
                }
                Ok(Decoded::NeedMore) => {
                    let n = stream.read(&mut chunk)?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "master closed",
                        ));
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("replication protocol error: {e:?}"),
                    ));
                }
            }
        }
    }

    fn parse_batch(frame: Value) -> Option<ReplBatch> {
        let Value::Array(Some(items)) = frame else {
            return None;
        };
        let [Value::Integer(last_seq), Value::Array(Some(raw_ops))] = items.as_slice() else {
            return None;
        };
        let mut ops = Vec::with_capacity(raw_ops.len());
        for raw in raw_ops {
            let Value::Array(Some(parts)) = raw else {
                return None;
            };
            match parts.as_slice() {
                [
                    Value::Bulk(Some(tag)),
                    Value::Bulk(Some(key)),
                    Value::Bulk(Some(value)),
                ] if tag == b"P" => {
                    ops.push(ReplOp::Put {
                        key: key.clone(),
                        value: value.clone(),
                    });
                }
                [Value::Bulk(Some(tag)), Value::Bulk(Some(key))] if tag == b"D" => {
                    ops.push(ReplOp::Delete { key: key.clone() });
                }
                _ => return None,
            }
        }
        Some(ReplBatch {
            last_seq: *last_seq as u64,
            ops,
        })
    }
}
