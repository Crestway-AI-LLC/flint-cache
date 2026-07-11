//! flint-conformance: the compatibility oracle.
//!
//! Runs a table-driven corpus of Redis-semantics cases against any RESP2
//! endpoint and reports pass rates per command family. The same corpus runs
//! against a reference server (valkey/redis) to validate the oracle itself,
//! and against flint-server to measure conformance. Nonzero exit on any
//! failure, so CI can gate on it.
//!
//! Usage: `flint-conformance --target 127.0.0.1:6380`

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::ExitCode;

use flint_resp::{Decoded, Value, decode, encode};

/// What a step's reply must look like.
#[derive(Debug, Clone)]
enum Expect {
    Ok,                 // +OK
    Pong,               // +PONG
    Nil,                // $-1
    Int(i64),           // :n
    Str(&'static [u8]), // $len\r\n<bytes>
    Bytes(Vec<u8>),     // like Str, for computed payloads
    AnyError,           // -...
}

struct Case {
    family: &'static str,
    name: &'static str,
    steps: Vec<(Vec<Vec<u8>>, Expect)>,
}

fn cmd(parts: &[&[u8]]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.to_vec()).collect()
}

fn corpus() -> Vec<Case> {
    let big = vec![0xABu8; 1024];
    vec![
        Case {
            family: "connection",
            name: "ping and echo",
            steps: vec![
                (cmd(&[b"PING"]), Expect::Pong),
                (cmd(&[b"PING", b"hello"]), Expect::Str(b"hello")),
                (cmd(&[b"ECHO", b"abc"]), Expect::Str(b"abc")),
                (cmd(&[b"ECHO"]), Expect::AnyError),
            ],
        },
        Case {
            family: "strings",
            name: "set then get",
            steps: vec![
                (cmd(&[b"SET", b"k1", b"v1"]), Expect::Ok),
                (cmd(&[b"GET", b"k1"]), Expect::Str(b"v1")),
            ],
        },
        Case {
            family: "strings",
            name: "get missing is nil",
            steps: vec![(cmd(&[b"GET", b"missing"]), Expect::Nil)],
        },
        Case {
            family: "strings",
            name: "set overwrites",
            steps: vec![
                (cmd(&[b"SET", b"k2", b"a"]), Expect::Ok),
                (cmd(&[b"SET", b"k2", b"b"]), Expect::Ok),
                (cmd(&[b"GET", b"k2"]), Expect::Str(b"b")),
            ],
        },
        Case {
            family: "strings",
            name: "set nx",
            steps: vec![
                (cmd(&[b"SET", b"k3", b"a", b"NX"]), Expect::Ok),
                (cmd(&[b"SET", b"k3", b"b", b"NX"]), Expect::Nil),
                (cmd(&[b"GET", b"k3"]), Expect::Str(b"a")),
                (cmd(&[b"SET", b"k3", b"c", b"nx"]), Expect::Nil),
            ],
        },
        Case {
            family: "strings",
            name: "set xx",
            steps: vec![
                (cmd(&[b"SET", b"k4", b"a", b"XX"]), Expect::Nil),
                (cmd(&[b"GET", b"k4"]), Expect::Nil),
                (cmd(&[b"SET", b"k4", b"a"]), Expect::Ok),
                (cmd(&[b"SET", b"k4", b"b", b"XX"]), Expect::Ok),
                (cmd(&[b"GET", b"k4"]), Expect::Str(b"b")),
            ],
        },
        Case {
            family: "strings",
            name: "set nx xx together is an error",
            steps: vec![(cmd(&[b"SET", b"k5", b"v", b"NX", b"XX"]), Expect::AnyError)],
        },
        Case {
            family: "strings",
            name: "empty value roundtrips",
            steps: vec![
                (cmd(&[b"SET", b"k6", b""]), Expect::Ok),
                (cmd(&[b"GET", b"k6"]), Expect::Str(b"")),
            ],
        },
        Case {
            family: "strings",
            name: "binary value roundtrips",
            steps: vec![
                (cmd(&[b"SET", b"k7", b"\x00\xff\r\n\x00"]), Expect::Ok),
                (cmd(&[b"GET", b"k7"]), Expect::Str(b"\x00\xff\r\n\x00")),
            ],
        },
        Case {
            family: "strings",
            name: "1kb value roundtrips",
            steps: vec![
                (cmd(&[b"SET", b"k8", &big]), Expect::Ok),
                (cmd(&[b"GET", b"k8"]), Expect::Bytes(big.clone())),
            ],
        },
        Case {
            family: "strings",
            name: "binary-safe keys",
            steps: vec![
                (cmd(&[b"SET", b"k\x00\x01", b"v"]), Expect::Ok),
                (cmd(&[b"GET", b"k\x00\x01"]), Expect::Str(b"v")),
                (cmd(&[b"GET", b"k"]), Expect::Nil),
            ],
        },
        Case {
            family: "keyspace",
            name: "del returns removal count",
            steps: vec![
                (cmd(&[b"SET", b"d1", b"x"]), Expect::Ok),
                (cmd(&[b"SET", b"d2", b"y"]), Expect::Ok),
                (cmd(&[b"DEL", b"d1", b"d2", b"d3"]), Expect::Int(2)),
                (cmd(&[b"GET", b"d1"]), Expect::Nil),
            ],
        },
        Case {
            family: "keyspace",
            name: "del counts a key once",
            steps: vec![
                (cmd(&[b"SET", b"d4", b"x"]), Expect::Ok),
                (cmd(&[b"DEL", b"d4", b"d4"]), Expect::Int(1)),
            ],
        },
        Case {
            family: "keyspace",
            name: "exists counts duplicates",
            steps: vec![
                (cmd(&[b"SET", b"e1", b"x"]), Expect::Ok),
                (cmd(&[b"EXISTS", b"e1", b"e1", b"nope"]), Expect::Int(2)),
                (cmd(&[b"EXISTS", b"nope"]), Expect::Int(0)),
            ],
        },
        Case {
            family: "protocol",
            name: "arity errors",
            steps: vec![
                (cmd(&[b"GET"]), Expect::AnyError),
                (cmd(&[b"SET", b"only-key"]), Expect::AnyError),
                (cmd(&[b"DEL"]), Expect::AnyError),
            ],
        },
        Case {
            family: "protocol",
            name: "unknown command errors",
            steps: vec![(cmd(&[b"FLINTNOSUCH", b"x"]), Expect::AnyError)],
        },
        Case {
            family: "protocol",
            name: "command name is case-insensitive",
            steps: vec![
                (cmd(&[b"set", b"c1", b"v"]), Expect::Ok),
                (cmd(&[b"gEt", b"c1"]), Expect::Str(b"v")),
            ],
        },
    ]
}

struct Client {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Client {
    fn connect(target: &str) -> std::io::Result<Self> {
        Ok(Self {
            stream: TcpStream::connect(target)?,
            buf: Vec::new(),
        })
    }

    fn call(&mut self, args: &[Vec<u8>]) -> std::io::Result<Value> {
        let frame = Value::Array(Some(
            args.iter().map(|a| Value::Bulk(Some(a.clone()))).collect(),
        ));
        let mut out = Vec::new();
        encode(&frame, &mut out);
        self.stream.write_all(&out)?;
        let mut chunk = [0u8; 16 * 1024];
        loop {
            match decode(&self.buf) {
                Ok(Decoded::Complete(value, used)) => {
                    self.buf.drain(..used);
                    return Ok(value);
                }
                Ok(Decoded::NeedMore) => {
                    let n = self.stream.read(&mut chunk)?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "server closed connection mid-reply",
                        ));
                    }
                    self.buf.extend_from_slice(&chunk[..n]);
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("protocol error from server: {e:?}"),
                    ));
                }
            }
        }
    }
}

fn matches(expect: &Expect, got: &Value) -> bool {
    match expect {
        Expect::Ok => *got == Value::Simple("OK".into()),
        Expect::Pong => *got == Value::Simple("PONG".into()),
        Expect::Nil => *got == Value::Bulk(None),
        Expect::Int(i) => *got == Value::Integer(*i),
        Expect::Str(s) => *got == Value::Bulk(Some(s.to_vec())),
        Expect::Bytes(b) => *got == Value::Bulk(Some(b.clone())),
        Expect::AnyError => matches!(got, Value::Error(_)),
    }
}

fn render(args: &[Vec<u8>]) -> String {
    args.iter()
        .map(|a| String::from_utf8_lossy(a).into_owned())
        .collect::<Vec<_>>()
        .join(" ")
}

fn main() -> ExitCode {
    let target = std::env::args()
        .skip_while(|a| a != "--target")
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6380".into());

    let mut per_family: BTreeMap<&str, (u32, u32)> = BTreeMap::new();
    let mut failures: Vec<String> = Vec::new();

    for case in corpus() {
        let entry = per_family.entry(case.family).or_insert((0, 0));
        entry.1 += 1;
        let result = run_case(&target, &case);
        match result {
            Ok(None) => entry.0 += 1,
            Ok(Some(failure)) => {
                failures.push(format!("[{}] {}: {failure}", case.family, case.name))
            }
            Err(e) => failures.push(format!("[{}] {}: io error: {e}", case.family, case.name)),
        }
    }

    println!("target: {target}");
    let (mut pass, mut total) = (0, 0);
    for (family, (p, t)) in &per_family {
        println!("  {family:<12} {p}/{t}");
        pass += p;
        total += t;
    }
    println!(
        "overall: {pass}/{total} ({:.1}%)",
        100.0 * pass as f64 / total as f64
    );
    if failures.is_empty() {
        ExitCode::SUCCESS
    } else {
        println!("\nfailures:");
        for f in &failures {
            println!("  {f}");
        }
        ExitCode::FAILURE
    }
}

/// Runs one case on a fresh connection with a clean keyspace.
/// Ok(None) = pass; Ok(Some(msg)) = semantic failure; Err = transport failure.
fn run_case(target: &str, case: &Case) -> std::io::Result<Option<String>> {
    let mut client = Client::connect(target)?;
    let flushed = client.call(&cmd(&[b"FLUSHALL"]))?;
    if flushed != Value::Simple("OK".into()) {
        return Ok(Some(format!("FLUSHALL failed: {flushed:?}")));
    }
    for (step_no, (args, expect)) in case.steps.iter().enumerate() {
        let got = client.call(args)?;
        if !matches(expect, &got) {
            return Ok(Some(format!(
                "step {}: `{}` expected {:?}, got {:?}",
                step_no + 1,
                render(args),
                expect,
                got
            )));
        }
    }
    Ok(None)
}
