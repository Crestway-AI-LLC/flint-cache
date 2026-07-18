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
    Ok,                   // +OK
    Pong,                 // +PONG
    Nil,                  // $-1
    Int(i64),             // :n
    IntRange(i64, i64),   // :n where lo <= n <= hi (TTL imprecision)
    Simple(&'static str), // +text
    Str(&'static [u8]),   // $len\r\n<bytes>
    Bytes(Vec<u8>),       // like Str, for computed payloads
    AnyError,             // -...
    /// Exact array, in order (HMGET etc. where order is defined).
    Arr(Vec<Expect>),
    /// Flat field/value reply compared as an unordered map (HGETALL —
    /// Redis hash iteration order is unspecified).
    UnorderedPairs(Vec<(&'static [u8], &'static [u8])>),
    /// Array of bulk strings compared as an unordered set (SMEMBERS).
    UnorderedStrs(Vec<&'static [u8]>),
}

struct Case {
    family: &'static str,
    name: &'static str,
    /// (command, expected reply, delay after step in ms)
    steps: Vec<(Vec<Vec<u8>>, Expect, u64)>,
}

/// Step with no delay.
fn s(parts: &[&[u8]], expect: Expect) -> (Vec<Vec<u8>>, Expect, u64) {
    (cmd(parts), expect, 0)
}

/// Step followed by a real-time delay (used only where semantics require
/// actual expiration; kept rare to avoid slow, flaky runs).
fn sd(parts: &[&[u8]], expect: Expect, delay_ms: u64) -> (Vec<Vec<u8>>, Expect, u64) {
    (cmd(parts), expect, delay_ms)
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
                s(&[b"PING"], Expect::Pong),
                s(&[b"PING", b"hello"], Expect::Str(b"hello")),
                s(&[b"ECHO", b"abc"], Expect::Str(b"abc")),
                s(&[b"ECHO"], Expect::AnyError),
            ],
        },
        Case {
            family: "strings",
            name: "set then get",
            steps: vec![
                s(&[b"SET", b"k1", b"v1"], Expect::Ok),
                s(&[b"GET", b"k1"], Expect::Str(b"v1")),
            ],
        },
        Case {
            family: "strings",
            name: "get missing is nil",
            steps: vec![s(&[b"GET", b"missing"], Expect::Nil)],
        },
        Case {
            family: "strings",
            name: "set overwrites",
            steps: vec![
                s(&[b"SET", b"k2", b"a"], Expect::Ok),
                s(&[b"SET", b"k2", b"b"], Expect::Ok),
                s(&[b"GET", b"k2"], Expect::Str(b"b")),
            ],
        },
        Case {
            family: "strings",
            name: "set nx",
            steps: vec![
                s(&[b"SET", b"k3", b"a", b"NX"], Expect::Ok),
                s(&[b"SET", b"k3", b"b", b"NX"], Expect::Nil),
                s(&[b"GET", b"k3"], Expect::Str(b"a")),
                s(&[b"SET", b"k3", b"c", b"nx"], Expect::Nil),
            ],
        },
        Case {
            family: "strings",
            name: "set xx",
            steps: vec![
                s(&[b"SET", b"k4", b"a", b"XX"], Expect::Nil),
                s(&[b"GET", b"k4"], Expect::Nil),
                s(&[b"SET", b"k4", b"a"], Expect::Ok),
                s(&[b"SET", b"k4", b"b", b"XX"], Expect::Ok),
                s(&[b"GET", b"k4"], Expect::Str(b"b")),
            ],
        },
        Case {
            family: "strings",
            name: "set nx xx together is an error",
            steps: vec![s(&[b"SET", b"k5", b"v", b"NX", b"XX"], Expect::AnyError)],
        },
        Case {
            family: "strings",
            name: "empty value roundtrips",
            steps: vec![
                s(&[b"SET", b"k6", b""], Expect::Ok),
                s(&[b"GET", b"k6"], Expect::Str(b"")),
            ],
        },
        Case {
            family: "strings",
            name: "binary value roundtrips",
            steps: vec![
                s(&[b"SET", b"k7", b"\x00\xff\r\n\x00"], Expect::Ok),
                s(&[b"GET", b"k7"], Expect::Str(b"\x00\xff\r\n\x00")),
            ],
        },
        Case {
            family: "strings",
            name: "1kb value roundtrips",
            steps: vec![
                s(&[b"SET", b"k8", &big], Expect::Ok),
                s(&[b"GET", b"k8"], Expect::Bytes(big.clone())),
            ],
        },
        Case {
            family: "strings",
            name: "binary-safe keys",
            steps: vec![
                s(&[b"SET", b"k\x00\x01", b"v"], Expect::Ok),
                s(&[b"GET", b"k\x00\x01"], Expect::Str(b"v")),
                s(&[b"GET", b"k"], Expect::Nil),
            ],
        },
        Case {
            family: "strings",
            name: "getrange windows",
            steps: vec![
                s(&[b"SET", b"gr1", b"Hello World"], Expect::Ok),
                s(&[b"GETRANGE", b"gr1", b"0", b"4"], Expect::Str(b"Hello")),
                s(&[b"GETRANGE", b"gr1", b"-5", b"-1"], Expect::Str(b"World")),
                s(
                    &[b"GETRANGE", b"gr1", b"0", b"-1"],
                    Expect::Str(b"Hello World"),
                ),
                s(&[b"GETRANGE", b"gr1", b"9", b"2"], Expect::Str(b"")),
                s(&[b"GETRANGE", b"gr1", b"50", b"60"], Expect::Str(b"")),
                s(&[b"GETRANGE", b"nosuchg", b"0", b"-1"], Expect::Str(b"")),
            ],
        },
        Case {
            family: "strings",
            name: "setrange pad overwrite ttl",
            steps: vec![
                // Missing key + offset: zero-padded creation.
                s(&[b"SETRANGE", b"sr1", b"5", b"World"], Expect::Int(10)),
                s(&[b"GET", b"sr1"], Expect::Str(b"\0\0\0\0\0World")),
                s(&[b"SET", b"sr2", b"Hello World"], Expect::Ok),
                s(&[b"SETRANGE", b"sr2", b"6", b"Redis"], Expect::Int(11)),
                s(&[b"GET", b"sr2"], Expect::Str(b"Hello Redis")),
                // Empty patch never creates the key.
                s(&[b"SETRANGE", b"srn", b"0", b""], Expect::Int(0)),
                s(&[b"EXISTS", b"srn"], Expect::Int(0)),
                // TTL survives the in-place mutation.
                s(&[b"SETEX", b"srt", b"100", b"hello"], Expect::Ok),
                s(&[b"SETRANGE", b"srt", b"0", b"H"], Expect::Int(5)),
                s(&[b"TTL", b"srt"], Expect::IntRange(95, 100)),
                s(&[b"GET", b"srt"], Expect::Str(b"Hello")),
                s(&[b"SETRANGE", b"sr2", b"-1", b"x"], Expect::AnyError),
            ],
        },
        Case {
            family: "keyspace",
            name: "del returns removal count",
            steps: vec![
                s(&[b"SET", b"d1", b"x"], Expect::Ok),
                s(&[b"SET", b"d2", b"y"], Expect::Ok),
                s(&[b"DEL", b"d1", b"d2", b"d3"], Expect::Int(2)),
                s(&[b"GET", b"d1"], Expect::Nil),
            ],
        },
        Case {
            family: "keyspace",
            name: "del counts a key once",
            steps: vec![
                s(&[b"SET", b"d4", b"x"], Expect::Ok),
                s(&[b"DEL", b"d4", b"d4"], Expect::Int(1)),
            ],
        },
        Case {
            family: "keyspace",
            name: "exists counts duplicates",
            steps: vec![
                s(&[b"SET", b"e1", b"x"], Expect::Ok),
                s(&[b"EXISTS", b"e1", b"e1", b"nope"], Expect::Int(2)),
                s(&[b"EXISTS", b"nope"], Expect::Int(0)),
            ],
        },
        Case {
            family: "protocol",
            name: "arity errors",
            steps: vec![
                s(&[b"GET"], Expect::AnyError),
                s(&[b"SET", b"only-key"], Expect::AnyError),
                s(&[b"DEL"], Expect::AnyError),
            ],
        },
        Case {
            family: "protocol",
            name: "unknown command errors",
            steps: vec![s(&[b"FLINTNOSUCH", b"x"], Expect::AnyError)],
        },
        Case {
            family: "protocol",
            name: "command name is case-insensitive",
            steps: vec![
                s(&[b"set", b"c1", b"v"], Expect::Ok),
                s(&[b"gEt", b"c1"], Expect::Str(b"v")),
            ],
        },
        Case {
            family: "ttl",
            name: "expire ttl persist lifecycle",
            steps: vec![
                s(&[b"SET", b"t1", b"v"], Expect::Ok),
                s(&[b"TTL", b"t1"], Expect::Int(-1)),
                s(&[b"EXPIRE", b"t1", b"100"], Expect::Int(1)),
                s(&[b"TTL", b"t1"], Expect::IntRange(95, 100)),
                s(&[b"PTTL", b"t1"], Expect::IntRange(95_000, 100_000)),
                s(&[b"PERSIST", b"t1"], Expect::Int(1)),
                s(&[b"TTL", b"t1"], Expect::Int(-1)),
                s(&[b"PERSIST", b"t1"], Expect::Int(0)),
            ],
        },
        Case {
            family: "ttl",
            name: "missing keys",
            steps: vec![
                s(&[b"TTL", b"nope"], Expect::Int(-2)),
                s(&[b"PTTL", b"nope"], Expect::Int(-2)),
                s(&[b"EXPIRE", b"nope", b"10"], Expect::Int(0)),
                s(&[b"PERSIST", b"nope"], Expect::Int(0)),
            ],
        },
        Case {
            family: "ttl",
            name: "set with ex and px",
            steps: vec![
                s(&[b"SET", b"t2", b"v", b"EX", b"100"], Expect::Ok),
                s(&[b"TTL", b"t2"], Expect::IntRange(95, 100)),
                s(&[b"SET", b"t3", b"v", b"PX", b"100000"], Expect::Ok),
                s(&[b"TTL", b"t3"], Expect::IntRange(95, 100)),
                s(&[b"SET", b"t4", b"v", b"EX", b"0"], Expect::AnyError),
                s(&[b"SET", b"t4", b"v", b"EX", b"abc"], Expect::AnyError),
            ],
        },
        Case {
            family: "ttl",
            name: "plain set clears ttl, keepttl keeps it",
            steps: vec![
                s(&[b"SET", b"t5", b"v", b"EX", b"100"], Expect::Ok),
                s(&[b"SET", b"t5", b"v2"], Expect::Ok),
                s(&[b"TTL", b"t5"], Expect::Int(-1)),
                s(&[b"SET", b"t5", b"v3", b"EX", b"100"], Expect::Ok),
                s(&[b"SET", b"t5", b"v4", b"KEEPTTL"], Expect::Ok),
                s(&[b"TTL", b"t5"], Expect::IntRange(1, 100)),
                s(&[b"GET", b"t5"], Expect::Str(b"v4")),
            ],
        },
        Case {
            family: "ttl",
            name: "keys really expire",
            steps: vec![
                sd(&[b"SET", b"t6", b"v", b"PX", b"60"], Expect::Ok, 140),
                s(&[b"GET", b"t6"], Expect::Nil),
                s(&[b"TTL", b"t6"], Expect::Int(-2)),
                s(&[b"EXISTS", b"t6"], Expect::Int(0)),
            ],
        },
        Case {
            family: "ttl",
            name: "setex and setnx",
            steps: vec![
                s(&[b"SETEX", b"t7", b"100", b"v"], Expect::Ok),
                s(&[b"TTL", b"t7"], Expect::IntRange(95, 100)),
                s(&[b"SETEX", b"t8", b"0", b"v"], Expect::AnyError),
                s(&[b"SETNX", b"t9", b"a"], Expect::Int(1)),
                s(&[b"SETNX", b"t9", b"b"], Expect::Int(0)),
                s(&[b"GET", b"t9"], Expect::Str(b"a")),
            ],
        },
        Case {
            family: "strings",
            name: "incr decr family",
            steps: vec![
                s(&[b"INCR", b"c1"], Expect::Int(1)),
                s(&[b"INCR", b"c1"], Expect::Int(2)),
                s(&[b"INCRBY", b"c1", b"10"], Expect::Int(12)),
                s(&[b"DECR", b"c1"], Expect::Int(11)),
                s(&[b"DECRBY", b"c1", b"5"], Expect::Int(6)),
                s(&[b"INCRBY", b"c1", b"-2"], Expect::Int(4)),
            ],
        },
        Case {
            family: "strings",
            name: "incr on non-integer errors",
            steps: vec![
                s(&[b"SET", b"c2", b"abc"], Expect::Ok),
                s(&[b"INCR", b"c2"], Expect::AnyError),
                s(&[b"SET", b"c3", b"9223372036854775807"], Expect::Ok),
                s(&[b"INCR", b"c3"], Expect::AnyError),
                s(&[b"INCRBY", b"c4", b"notanum"], Expect::AnyError),
            ],
        },
        Case {
            family: "strings",
            name: "incr preserves ttl",
            steps: vec![
                s(&[b"SET", b"c5", b"5", b"EX", b"100"], Expect::Ok),
                s(&[b"INCR", b"c5"], Expect::Int(6)),
                s(&[b"TTL", b"c5"], Expect::IntRange(1, 100)),
            ],
        },
        Case {
            family: "strings",
            name: "append and strlen",
            steps: vec![
                s(&[b"APPEND", b"a1", b"he"], Expect::Int(2)),
                s(&[b"APPEND", b"a1", b"llo"], Expect::Int(5)),
                s(&[b"GET", b"a1"], Expect::Str(b"hello")),
                s(&[b"STRLEN", b"a1"], Expect::Int(5)),
                s(&[b"STRLEN", b"missing"], Expect::Int(0)),
            ],
        },
        Case {
            family: "keyspace",
            name: "type command",
            steps: vec![
                s(&[b"SET", b"y1", b"v"], Expect::Ok),
                s(&[b"TYPE", b"y1"], Expect::Simple("string")),
                s(&[b"TYPE", b"missing"], Expect::Simple("none")),
            ],
        },
        Case {
            family: "keyspace",
            name: "del removes ttl state too",
            steps: vec![
                s(&[b"SET", b"y2", b"v", b"EX", b"100"], Expect::Ok),
                s(&[b"DEL", b"y2"], Expect::Int(1)),
                s(&[b"SET", b"y2", b"v2"], Expect::Ok),
                s(&[b"TTL", b"y2"], Expect::Int(-1)),
            ],
        },
        Case {
            family: "ttl",
            name: "expire with past time deletes",
            steps: vec![
                s(&[b"SET", b"y3", b"v"], Expect::Ok),
                s(&[b"EXPIRE", b"y3", b"-1"], Expect::Int(1)),
                s(&[b"EXISTS", b"y3"], Expect::Int(0)),
                s(&[b"GET", b"y3"], Expect::Nil),
            ],
        },
        Case {
            family: "hashes",
            name: "hset counts new fields, hget reads",
            steps: vec![
                s(&[b"HSET", b"h1", b"a", b"1", b"b", b"2"], Expect::Int(2)),
                s(&[b"HSET", b"h1", b"a", b"9", b"c", b"3"], Expect::Int(1)),
                s(&[b"HGET", b"h1", b"a"], Expect::Str(b"9")),
                s(&[b"HGET", b"h1", b"nope"], Expect::Nil),
                s(&[b"HGET", b"nosuch", b"f"], Expect::Nil),
                s(&[b"HLEN", b"h1"], Expect::Int(3)),
                s(&[b"HEXISTS", b"h1", b"b"], Expect::Int(1)),
                s(&[b"HEXISTS", b"h1", b"zz"], Expect::Int(0)),
            ],
        },
        Case {
            family: "hashes",
            name: "hdel to empty removes the key",
            steps: vec![
                s(&[b"HSET", b"h2", b"a", b"1", b"b", b"2"], Expect::Int(2)),
                s(&[b"HDEL", b"h2", b"a", b"nope"], Expect::Int(1)),
                s(&[b"HDEL", b"h2", b"b"], Expect::Int(1)),
                s(&[b"EXISTS", b"h2"], Expect::Int(0)),
                s(&[b"TYPE", b"h2"], Expect::Simple("none")),
                s(&[b"HDEL", b"nosuch", b"f"], Expect::Int(0)),
            ],
        },
        Case {
            family: "hashes",
            name: "hgetall hmget hkeys hvals",
            steps: vec![
                s(&[b"HSET", b"h3", b"x", b"10", b"y", b"20"], Expect::Int(2)),
                s(
                    &[b"HGETALL", b"h3"],
                    Expect::UnorderedPairs(vec![(b"x", b"10"), (b"y", b"20")]),
                ),
                s(&[b"HGETALL", b"nosuch"], Expect::UnorderedPairs(vec![])),
                s(
                    &[b"HMGET", b"h3", b"y", b"zz", b"x"],
                    Expect::Arr(vec![Expect::Str(b"20"), Expect::Nil, Expect::Str(b"10")]),
                ),
                s(
                    &[b"HMGET", b"nosuch", b"a", b"b"],
                    Expect::Arr(vec![Expect::Nil, Expect::Nil]),
                ),
            ],
        },
        Case {
            family: "hashes",
            name: "hash respects ttl machinery",
            steps: vec![
                s(&[b"HSET", b"h4", b"f", b"v"], Expect::Int(1)),
                s(&[b"EXPIRE", b"h4", b"100"], Expect::Int(1)),
                s(&[b"TTL", b"h4"], Expect::IntRange(95, 100)),
                s(&[b"PERSIST", b"h4"], Expect::Int(1)),
                s(&[b"HGET", b"h4", b"f"], Expect::Str(b"v")),
                s(&[b"DEL", b"h4"], Expect::Int(1)),
                s(&[b"HGETALL", b"h4"], Expect::UnorderedPairs(vec![])),
            ],
        },
        Case {
            family: "hashes",
            name: "recreate after del is a fresh hash",
            steps: vec![
                s(&[b"HSET", b"h5", b"old", b"x"], Expect::Int(1)),
                s(&[b"DEL", b"h5"], Expect::Int(1)),
                s(&[b"HSET", b"h5", b"new", b"y"], Expect::Int(1)),
                s(&[b"HGET", b"h5", b"old"], Expect::Nil),
                s(&[b"HLEN", b"h5"], Expect::Int(1)),
            ],
        },
        Case {
            family: "protocol",
            name: "wrongtype in both directions",
            steps: vec![
                s(&[b"SET", b"wt-s", b"v"], Expect::Ok),
                s(&[b"HSET", b"wt-h", b"f", b"v"], Expect::Int(1)),
                s(&[b"HGET", b"wt-s", b"f"], Expect::AnyError),
                s(&[b"HSET", b"wt-s", b"f", b"v"], Expect::AnyError),
                s(&[b"GET", b"wt-h"], Expect::AnyError),
                s(&[b"INCR", b"wt-h"], Expect::AnyError),
                s(&[b"APPEND", b"wt-h", b"x"], Expect::AnyError),
                s(&[b"STRLEN", b"wt-h"], Expect::AnyError),
                s(&[b"SET", b"wt-h", b"overwritten"], Expect::Ok),
                s(&[b"GET", b"wt-h"], Expect::Str(b"overwritten")),
            ],
        },
        Case {
            family: "sets",
            name: "sadd srem sismember smembers scard",
            steps: vec![
                s(&[b"SADD", b"s1", b"a", b"b", b"a"], Expect::Int(2)),
                s(&[b"SADD", b"s1", b"b", b"c"], Expect::Int(1)),
                s(&[b"SCARD", b"s1"], Expect::Int(3)),
                s(&[b"SISMEMBER", b"s1", b"a"], Expect::Int(1)),
                s(&[b"SISMEMBER", b"s1", b"zz"], Expect::Int(0)),
                s(
                    &[b"SMEMBERS", b"s1"],
                    Expect::UnorderedStrs(vec![b"a", b"b", b"c"]),
                ),
                s(&[b"SMEMBERS", b"nosuch"], Expect::UnorderedStrs(vec![])),
                s(&[b"SREM", b"s1", b"a", b"zz"], Expect::Int(1)),
                s(&[b"SREM", b"s1", b"b", b"c"], Expect::Int(2)),
                s(&[b"EXISTS", b"s1"], Expect::Int(0)),
                s(&[b"TYPE", b"s1"], Expect::Simple("none")),
            ],
        },
        Case {
            family: "sets",
            name: "spop srandmember deterministic shapes",
            steps: vec![
                // A one-member set pins the random pick.
                s(&[b"SADD", b"sp1", b"a"], Expect::Int(1)),
                s(&[b"SRANDMEMBER", b"sp1"], Expect::Str(b"a")),
                s(
                    &[b"SRANDMEMBER", b"sp1", b"5"],
                    Expect::Arr(vec![Expect::Str(b"a")]),
                ),
                s(
                    &[b"SRANDMEMBER", b"sp1", b"-3"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"a"),
                        Expect::Str(b"a"),
                    ]),
                ),
                s(&[b"SRANDMEMBER", b"sp1", b"0"], Expect::Arr(vec![])),
                s(&[b"SRANDMEMBER", b"nosuchs"], Expect::Nil),
                s(&[b"SRANDMEMBER", b"nosuchs", b"3"], Expect::Arr(vec![])),
                s(&[b"SPOP", b"sp1"], Expect::Str(b"a")),
                s(&[b"EXISTS", b"sp1"], Expect::Int(0)),
                s(&[b"SPOP", b"nosuchs"], Expect::Nil),
                s(&[b"SPOP", b"nosuchs", b"2"], Expect::Arr(vec![])),
                // Over-count pops everything — compare unordered.
                s(&[b"SADD", b"sp2", b"a", b"b", b"c"], Expect::Int(3)),
                s(
                    &[b"SPOP", b"sp2", b"5"],
                    Expect::UnorderedStrs(vec![b"a", b"b", b"c"]),
                ),
                s(&[b"EXISTS", b"sp2"], Expect::Int(0)),
                s(&[b"SADD", b"sp3", b"x"], Expect::Int(1)),
                s(&[b"SPOP", b"sp3", b"-1"], Expect::AnyError),
            ],
        },
        Case {
            family: "strings",
            name: "incrbyfloat human formatting",
            steps: vec![
                // Dyadic increments are exact in binary floating point, so
                // the human formatting is deterministic cross-platform.
                s(&[b"INCRBYFLOAT", b"fb1", b"10.5"], Expect::Str(b"10.5")),
                s(&[b"INCRBYFLOAT", b"fb1", b"0.25"], Expect::Str(b"10.75")),
                s(&[b"INCRBYFLOAT", b"fb1", b"-0.75"], Expect::Str(b"10")),
                s(&[b"GET", b"fb1"], Expect::Str(b"10")),
                // Exponent-form stored values parse; output is human form.
                s(&[b"SET", b"fb2", b"3.0e3"], Expect::Ok),
                s(&[b"INCRBYFLOAT", b"fb2", b"200"], Expect::Str(b"3200")),
                s(&[b"SET", b"fb3", b"hello"], Expect::Ok),
                s(&[b"INCRBYFLOAT", b"fb3", b"1"], Expect::AnyError),
                s(&[b"INCRBYFLOAT", b"fb4", b"notafloat"], Expect::AnyError),
                s(&[b"SET", b"fb5", b"inf"], Expect::Ok),
                s(&[b"INCRBYFLOAT", b"fb5", b"1"], Expect::AnyError),
            ],
        },
        Case {
            family: "hashes",
            name: "hscan single shot with match and novalues",
            steps: vec![
                s(
                    &[b"HSET", b"hs1", b"f1", b"v1", b"f2", b"v2", b"g1", b"v3"],
                    Expect::Int(3),
                ),
                s(
                    &[b"HSCAN", b"hs1", b"0"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedPairs(vec![
                            (b"f1", b"v1"),
                            (b"f2", b"v2"),
                            (b"g1", b"v3"),
                        ]),
                    ]),
                ),
                s(
                    &[b"HSCAN", b"hs1", b"0", b"MATCH", b"f*"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedPairs(vec![(b"f1", b"v1"), (b"f2", b"v2")]),
                    ]),
                ),
                s(
                    &[b"HSCAN", b"hs1", b"0", b"COUNT", b"100", b"NOVALUES"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"f1", b"f2", b"g1"]),
                    ]),
                ),
                s(
                    &[b"HSCAN", b"nosuchh", b"0"],
                    Expect::Arr(vec![Expect::Str(b"0"), Expect::Arr(vec![])]),
                ),
                s(&[b"HSCAN", b"hs1", b"notanumber"], Expect::AnyError),
                s(&[b"HSCAN", b"hs1", b"0", b"MATCH"], Expect::AnyError),
            ],
        },
        Case {
            family: "sets",
            name: "sscan single shot with match",
            steps: vec![
                s(&[b"SADD", b"ss1", b"a", b"ab", b"b"], Expect::Int(3)),
                s(
                    &[b"SSCAN", b"ss1", b"0"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"a", b"ab", b"b"]),
                    ]),
                ),
                s(
                    &[b"SSCAN", b"ss1", b"0", b"MATCH", b"a*"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"a", b"ab"]),
                    ]),
                ),
                s(
                    &[b"SSCAN", b"ss1", b"0", b"MATCH", b"?"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"a", b"b"]),
                    ]),
                ),
                s(
                    &[b"SSCAN", b"nosuchs2", b"0"],
                    Expect::Arr(vec![Expect::Str(b"0"), Expect::Arr(vec![])]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "zscan single shot with match",
            steps: vec![
                s(&[b"ZADD", b"zs1", b"1", b"a", b"2", b"b"], Expect::Int(2)),
                s(
                    &[b"ZSCAN", b"zs1", b"0"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedPairs(vec![(b"a", b"1"), (b"b", b"2")]),
                    ]),
                ),
                s(
                    &[b"ZSCAN", b"zs1", b"0", b"MATCH", b"b*"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedPairs(vec![(b"b", b"2")]),
                    ]),
                ),
                s(
                    &[b"ZSCAN", b"nosuchz", b"0"],
                    Expect::Arr(vec![Expect::Str(b"0"), Expect::Arr(vec![])]),
                ),
            ],
        },
        Case {
            family: "lists",
            name: "push pop order",
            steps: vec![
                s(&[b"RPUSH", b"l1", b"a", b"b"], Expect::Int(2)),
                s(&[b"LPUSH", b"l1", b"c"], Expect::Int(3)),
                s(&[b"TYPE", b"l1"], Expect::Simple("list")),
                s(
                    &[b"LRANGE", b"l1", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"c"),
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                    ]),
                ),
                s(&[b"LPOP", b"l1"], Expect::Str(b"c")),
                s(&[b"RPOP", b"l1"], Expect::Str(b"b")),
                s(&[b"LLEN", b"l1"], Expect::Int(1)),
                s(&[b"LPOP", b"l1"], Expect::Str(b"a")),
                s(&[b"EXISTS", b"l1"], Expect::Int(0)),
                s(&[b"LPOP", b"l1"], Expect::Nil),
            ],
        },
        Case {
            family: "lists",
            name: "lpush multi reverses",
            steps: vec![
                s(&[b"LPUSH", b"l2", b"a", b"b", b"c"], Expect::Int(3)),
                s(
                    &[b"LRANGE", b"l2", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"c"),
                        Expect::Str(b"b"),
                        Expect::Str(b"a"),
                    ]),
                ),
            ],
        },
        Case {
            family: "lists",
            name: "lrange negative indices and clamping",
            steps: vec![
                s(&[b"RPUSH", b"l3", b"a", b"b", b"c", b"d"], Expect::Int(4)),
                s(
                    &[b"LRANGE", b"l3", b"-2", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"c"), Expect::Str(b"d")]),
                ),
                s(
                    &[b"LRANGE", b"l3", b"0", b"99"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                        Expect::Str(b"d"),
                    ]),
                ),
                s(&[b"LRANGE", b"l3", b"3", b"1"], Expect::Arr(vec![])),
                s(&[b"LRANGE", b"nosuch", b"0", b"-1"], Expect::Arr(vec![])),
            ],
        },
        Case {
            family: "lists",
            name: "lset overwrite and range errors",
            steps: vec![
                s(&[b"RPUSH", b"l4", b"a", b"b", b"c"], Expect::Int(3)),
                s(&[b"LSET", b"l4", b"1", b"B"], Expect::Ok),
                s(&[b"LSET", b"l4", b"-1", b"C"], Expect::Ok),
                s(
                    &[b"LRANGE", b"l4", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"B"),
                        Expect::Str(b"C"),
                    ]),
                ),
                s(&[b"LSET", b"l4", b"3", b"x"], Expect::AnyError),
                s(&[b"LSET", b"l4", b"-4", b"x"], Expect::AnyError),
                s(&[b"LSET", b"nosuch", b"0", b"x"], Expect::AnyError),
            ],
        },
        Case {
            family: "lists",
            name: "ltrim keep window and empty deletes",
            steps: vec![
                s(
                    &[b"RPUSH", b"l5", b"a", b"b", b"c", b"d", b"e"],
                    Expect::Int(5),
                ),
                s(&[b"LTRIM", b"l5", b"1", b"-2"], Expect::Ok),
                s(
                    &[b"LRANGE", b"l5", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                        Expect::Str(b"d"),
                    ]),
                ),
                // Pushes after a trim behave normally.
                s(&[b"LPUSH", b"l5", b"z"], Expect::Int(4)),
                s(
                    &[b"LRANGE", b"l5", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"z"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                        Expect::Str(b"d"),
                    ]),
                ),
                // Inverted keep-range empties (and thus deletes) the key.
                s(&[b"LTRIM", b"l5", b"5", b"1"], Expect::Ok),
                s(&[b"EXISTS", b"l5"], Expect::Int(0)),
                // Missing key is still +OK.
                s(&[b"LTRIM", b"nosuch", b"0", b"-1"], Expect::Ok),
            ],
        },
        Case {
            family: "lists",
            name: "lpos rank count maxlen",
            steps: vec![
                s(
                    &[b"RPUSH", b"l6", b"a", b"b", b"c", b"a", b"b", b"c", b"a"],
                    Expect::Int(7),
                ),
                s(&[b"LPOS", b"l6", b"a"], Expect::Int(0)),
                s(&[b"LPOS", b"l6", b"a", b"RANK", b"2"], Expect::Int(3)),
                s(&[b"LPOS", b"l6", b"a", b"RANK", b"-1"], Expect::Int(6)),
                s(&[b"LPOS", b"l6", b"missing"], Expect::Nil),
                s(
                    &[b"LPOS", b"l6", b"a", b"COUNT", b"0"],
                    Expect::Arr(vec![Expect::Int(0), Expect::Int(3), Expect::Int(6)]),
                ),
                s(
                    &[b"LPOS", b"l6", b"a", b"RANK", b"-1", b"COUNT", b"2"],
                    Expect::Arr(vec![Expect::Int(6), Expect::Int(3)]),
                ),
                s(
                    &[b"LPOS", b"l6", b"a", b"COUNT", b"0", b"MAXLEN", b"2"],
                    Expect::Arr(vec![Expect::Int(0)]),
                ),
                s(
                    &[b"LPOS", b"l6", b"missing", b"COUNT", b"0"],
                    Expect::Arr(vec![]),
                ),
                s(&[b"LPOS", b"l6", b"a", b"RANK", b"0"], Expect::AnyError),
                s(&[b"LPOS", b"l6", b"a", b"COUNT", b"-1"], Expect::AnyError),
            ],
        },
        Case {
            family: "lists",
            name: "lrem counts and directions",
            steps: vec![
                s(
                    &[b"RPUSH", b"l7", b"a", b"b", b"a", b"c", b"a"],
                    Expect::Int(5),
                ),
                s(&[b"LREM", b"l7", b"1", b"a"], Expect::Int(1)),
                s(
                    &[b"LRANGE", b"l7", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"b"),
                        Expect::Str(b"a"),
                        Expect::Str(b"c"),
                        Expect::Str(b"a"),
                    ]),
                ),
                s(&[b"LREM", b"l7", b"-1", b"a"], Expect::Int(1)),
                s(
                    &[b"LRANGE", b"l7", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"b"),
                        Expect::Str(b"a"),
                        Expect::Str(b"c"),
                    ]),
                ),
                s(&[b"LREM", b"l7", b"0", b"a"], Expect::Int(1)),
                s(
                    &[b"LRANGE", b"l7", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"c")]),
                ),
                s(&[b"LREM", b"l7", b"0", b"zz"], Expect::Int(0)),
                s(&[b"LREM", b"nosuchl", b"0", b"x"], Expect::Int(0)),
            ],
        },
        Case {
            family: "lists",
            name: "linsert before after and misses",
            steps: vec![
                s(&[b"RPUSH", b"l8", b"a", b"c"], Expect::Int(2)),
                s(&[b"LINSERT", b"l8", b"BEFORE", b"c", b"b"], Expect::Int(3)),
                s(&[b"LINSERT", b"l8", b"AFTER", b"c", b"d"], Expect::Int(4)),
                s(
                    &[b"LRANGE", b"l8", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                        Expect::Str(b"d"),
                    ]),
                ),
                s(
                    &[b"LINSERT", b"l8", b"BEFORE", b"zz", b"x"],
                    Expect::Int(-1),
                ),
                s(
                    &[b"LINSERT", b"nosuchl", b"BEFORE", b"a", b"b"],
                    Expect::Int(0),
                ),
                s(
                    &[b"LINSERT", b"l8", b"SIDEWAYS", b"a", b"b"],
                    Expect::AnyError,
                ),
                // Ends still behave after interior rewrites.
                s(&[b"RPUSH", b"l8", b"e"], Expect::Int(5)),
                s(&[b"LPOP", b"l8"], Expect::Str(b"a")),
                s(
                    &[b"LRANGE", b"l8", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                        Expect::Str(b"d"),
                        Expect::Str(b"e"),
                    ]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "zadd zscore zrange ordering",
            steps: vec![
                s(
                    &[b"ZADD", b"z1", b"2", b"b", b"1", b"a", b"3", b"c"],
                    Expect::Int(3),
                ),
                s(&[b"ZSCORE", b"z1", b"b"], Expect::Str(b"2")),
                s(&[b"ZSCORE", b"z1", b"missing"], Expect::Nil),
                s(&[b"ZCARD", b"z1"], Expect::Int(3)),
                s(
                    &[b"ZRANGE", b"z1", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                    ]),
                ),
                s(
                    &[b"ZRANGE", b"z1", b"0", b"1", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"1"),
                        Expect::Str(b"b"),
                        Expect::Str(b"2"),
                    ]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "score update reorders without double count",
            steps: vec![
                s(&[b"ZADD", b"z2", b"1", b"a", b"2", b"b"], Expect::Int(2)),
                s(&[b"ZADD", b"z2", b"5", b"a"], Expect::Int(0)),
                s(&[b"ZCARD", b"z2"], Expect::Int(2)),
                s(
                    &[b"ZRANGE", b"z2", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"a")]),
                ),
                s(&[b"ZSCORE", b"z2", b"a"], Expect::Str(b"5")),
            ],
        },
        Case {
            family: "zsets",
            name: "zrem to empty removes key; decimal scores",
            steps: vec![
                s(&[b"ZADD", b"z3", b"1.5", b"a"], Expect::Int(1)),
                s(&[b"ZSCORE", b"z3", b"a"], Expect::Str(b"1.5")),
                s(&[b"ZREM", b"z3", b"a", b"zz"], Expect::Int(1)),
                s(&[b"EXISTS", b"z3"], Expect::Int(0)),
                s(&[b"ZADD", b"z3", b"nope", b"a"], Expect::AnyError),
            ],
        },
        Case {
            family: "protocol",
            name: "wrongtype across new families",
            steps: vec![
                s(&[b"SET", b"wt2-s", b"v"], Expect::Ok),
                s(&[b"SADD", b"wt2-s", b"m"], Expect::AnyError),
                s(&[b"LPUSH", b"wt2-s", b"m"], Expect::AnyError),
                s(&[b"ZADD", b"wt2-s", b"1", b"m"], Expect::AnyError),
                s(&[b"RPUSH", b"wt2-l", b"x"], Expect::Int(1)),
                s(&[b"GET", b"wt2-l"], Expect::AnyError),
                s(&[b"SMEMBERS", b"wt2-l"], Expect::AnyError),
                s(&[b"HGET", b"wt2-l", b"f"], Expect::AnyError),
                s(&[b"DEL", b"wt2-l"], Expect::Int(1)),
            ],
        },
        Case {
            family: "strings",
            name: "mset mget with missing and wrongtype",
            steps: vec![
                s(&[b"MSET", b"m1", b"a", b"m2", b"b"], Expect::Ok),
                s(&[b"RPUSH", b"m3", b"x"], Expect::Int(1)),
                s(
                    &[b"MGET", b"m1", b"nosuch", b"m2", b"m3"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Nil,
                        Expect::Str(b"b"),
                        Expect::Nil,
                    ]),
                ),
                s(&[b"MSET", b"m4"], Expect::AnyError),
            ],
        },
        Case {
            family: "hashes",
            name: "hincrby",
            steps: vec![
                s(&[b"HINCRBY", b"hi", b"f", b"5"], Expect::Int(5)),
                s(&[b"HINCRBY", b"hi", b"f", b"-2"], Expect::Int(3)),
                s(&[b"HGET", b"hi", b"f"], Expect::Str(b"3")),
                s(&[b"HSET", b"hi", b"txt", b"abc"], Expect::Int(1)),
                s(&[b"HINCRBY", b"hi", b"txt", b"1"], Expect::AnyError),
            ],
        },
        Case {
            family: "zsets",
            name: "zincrby creates and reorders",
            steps: vec![
                s(&[b"ZADD", b"zi", b"5", b"a"], Expect::Int(1)),
                s(&[b"ZINCRBY", b"zi", b"3", b"a"], Expect::Str(b"8")),
                s(&[b"ZINCRBY", b"zi", b"2", b"new"], Expect::Str(b"2")),
                s(&[b"ZCARD", b"zi"], Expect::Int(2)),
                s(
                    &[b"ZRANGE", b"zi", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"new"), Expect::Str(b"a")]),
                ),
            ],
        },
        Case {
            family: "lists",
            name: "lindex",
            steps: vec![
                s(&[b"RPUSH", b"li", b"a", b"b", b"c"], Expect::Int(3)),
                s(&[b"LINDEX", b"li", b"0"], Expect::Str(b"a")),
                s(&[b"LINDEX", b"li", b"-1"], Expect::Str(b"c")),
                s(&[b"LINDEX", b"li", b"99"], Expect::Nil),
                s(&[b"LINDEX", b"li", b"-99"], Expect::Nil),
                s(&[b"LINDEX", b"nosuch", b"0"], Expect::Nil),
            ],
        },
        Case {
            family: "keyspace",
            name: "dbsize counts live keys",
            steps: vec![
                s(&[b"DBSIZE"], Expect::Int(0)),
                s(&[b"SET", b"d1", b"v"], Expect::Ok),
                s(&[b"SET", b"d2", b"v"], Expect::Ok),
                s(&[b"HSET", b"d3", b"f", b"v"], Expect::Int(1)),
                s(&[b"DBSIZE"], Expect::Int(3)),
                s(&[b"DEL", b"d1"], Expect::Int(1)),
                s(&[b"DBSIZE"], Expect::Int(2)),
            ],
        },
        // --- Tier-1 command coverage (validated vs Valkey) ---
        Case {
            family: "strings",
            name: "getdel returns and removes",
            steps: vec![
                s(&[b"SET", b"gd1", b"v"], Expect::Ok),
                s(&[b"GETDEL", b"gd1"], Expect::Str(b"v")),
                s(&[b"GET", b"gd1"], Expect::Nil),
                s(&[b"GETDEL", b"gd_missing"], Expect::Nil),
            ],
        },
        Case {
            family: "strings",
            name: "getset returns old",
            steps: vec![
                s(&[b"GETSET", b"gs1", b"a"], Expect::Nil),
                s(&[b"GETSET", b"gs1", b"b"], Expect::Str(b"a")),
                s(&[b"GET", b"gs1"], Expect::Str(b"b")),
            ],
        },
        Case {
            family: "strings",
            name: "set with get option",
            steps: vec![
                s(&[b"SET", b"sg1", b"a", b"GET"], Expect::Nil),
                s(&[b"SET", b"sg1", b"b", b"GET"], Expect::Str(b"a")),
                s(&[b"GET", b"sg1"], Expect::Str(b"b")),
                // NX + GET: write rejected, old value still returned.
                s(&[b"SET", b"sg1", b"c", b"NX", b"GET"], Expect::Str(b"b")),
                s(&[b"GET", b"sg1"], Expect::Str(b"b")),
            ],
        },
        Case {
            family: "keyspace",
            name: "expireat and expiretime",
            steps: vec![
                s(&[b"SET", b"ea1", b"v"], Expect::Ok),
                // Far-future absolute second; TTL reads back ~that window.
                s(&[b"EXPIREAT", b"ea1", b"9999999999"], Expect::Int(1)),
                s(&[b"EXPIRETIME", b"ea1"], Expect::Int(9999999999)),
                // A past instant deletes.
                s(&[b"SET", b"ea2", b"v"], Expect::Ok),
                s(&[b"EXPIREAT", b"ea2", b"1"], Expect::Int(1)),
                s(&[b"GET", b"ea2"], Expect::Nil),
                // No-expiry / missing sentinels.
                s(&[b"SET", b"ea3", b"v"], Expect::Ok),
                s(&[b"EXPIRETIME", b"ea3"], Expect::Int(-1)),
                s(&[b"EXPIRETIME", b"ea_missing"], Expect::Int(-2)),
            ],
        },
        Case {
            family: "keyspace",
            name: "unlink removes like del",
            steps: vec![
                s(&[b"SET", b"ul1", b"v"], Expect::Ok),
                s(&[b"SET", b"ul2", b"v"], Expect::Ok),
                s(&[b"UNLINK", b"ul1", b"ul2", b"ul_missing"], Expect::Int(2)),
                s(&[b"GET", b"ul1"], Expect::Nil),
            ],
        },
        Case {
            family: "hashes",
            name: "hsetnx and hstrlen",
            steps: vec![
                s(&[b"HSETNX", b"hx1", b"f", b"hello"], Expect::Int(1)),
                s(&[b"HSETNX", b"hx1", b"f", b"other"], Expect::Int(0)),
                s(&[b"HGET", b"hx1", b"f"], Expect::Str(b"hello")),
                s(&[b"HSTRLEN", b"hx1", b"f"], Expect::Int(5)),
                s(&[b"HSTRLEN", b"hx1", b"nofield"], Expect::Int(0)),
            ],
        },
        Case {
            family: "sets",
            name: "smismember batch membership",
            steps: vec![
                s(&[b"SADD", b"sm1", b"a", b"b"], Expect::Int(2)),
                s(
                    &[b"SMISMEMBER", b"sm1", b"a", b"x", b"b"],
                    Expect::Arr(vec![Expect::Int(1), Expect::Int(0), Expect::Int(1)]),
                ),
            ],
        },
        // --- Tier-2: sorted-set range family (validated vs Valkey) ---
        Case {
            family: "zsets",
            name: "zrangebyscore inclusive and exclusive",
            steps: vec![
                s(
                    &[b"ZADD", b"zr", b"1", b"a", b"2", b"b", b"3", b"c"],
                    Expect::Int(3),
                ),
                s(
                    &[b"ZRANGEBYSCORE", b"zr", b"1", b"2"],
                    Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"b")]),
                ),
                s(
                    &[b"ZRANGEBYSCORE", b"zr", b"(1", b"3"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"c")]),
                ),
                s(
                    &[b"ZRANGEBYSCORE", b"zr", b"-inf", b"+inf"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                    ]),
                ),
                s(
                    &[b"ZRANGEBYSCORE", b"zr", b"-inf", b"+inf", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"1"),
                        Expect::Str(b"b"),
                        Expect::Str(b"2"),
                        Expect::Str(b"c"),
                        Expect::Str(b"3"),
                    ]),
                ),
                s(
                    &[
                        b"ZRANGEBYSCORE",
                        b"zr",
                        b"-inf",
                        b"+inf",
                        b"LIMIT",
                        b"1",
                        b"1",
                    ],
                    Expect::Arr(vec![Expect::Str(b"b")]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "zrevrange and zrevrangebyscore",
            steps: vec![
                s(
                    &[b"ZADD", b"zv", b"1", b"a", b"2", b"b", b"3", b"c"],
                    Expect::Int(3),
                ),
                s(
                    &[b"ZREVRANGE", b"zv", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"c"),
                        Expect::Str(b"b"),
                        Expect::Str(b"a"),
                    ]),
                ),
                s(
                    &[b"ZREVRANGEBYSCORE", b"zv", b"3", b"2"],
                    Expect::Arr(vec![Expect::Str(b"c"), Expect::Str(b"b")]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "zrank zrevrank zcount zmscore",
            steps: vec![
                s(
                    &[b"ZADD", b"zk", b"10", b"a", b"20", b"b", b"30", b"c"],
                    Expect::Int(3),
                ),
                s(&[b"ZRANK", b"zk", b"a"], Expect::Int(0)),
                s(&[b"ZRANK", b"zk", b"c"], Expect::Int(2)),
                s(&[b"ZRANK", b"zk", b"missing"], Expect::Nil),
                s(&[b"ZREVRANK", b"zk", b"a"], Expect::Int(2)),
                s(&[b"ZCOUNT", b"zk", b"15", b"30"], Expect::Int(2)),
                s(&[b"ZCOUNT", b"zk", b"(10", b"(30"], Expect::Int(1)),
                s(
                    &[b"ZMSCORE", b"zk", b"a", b"nope", b"c"],
                    Expect::Arr(vec![Expect::Str(b"10"), Expect::Nil, Expect::Str(b"30")]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "zpopmin zpopmax",
            steps: vec![
                s(
                    &[b"ZADD", b"zp", b"1", b"a", b"2", b"b", b"3", b"c"],
                    Expect::Int(3),
                ),
                s(
                    &[b"ZPOPMIN", b"zp"],
                    Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"1")]),
                ),
                s(
                    &[b"ZPOPMAX", b"zp", b"2"],
                    Expect::Arr(vec![
                        Expect::Str(b"c"),
                        Expect::Str(b"3"),
                        Expect::Str(b"b"),
                        Expect::Str(b"2"),
                    ]),
                ),
                s(&[b"ZCARD", b"zp"], Expect::Int(0)),
            ],
        },
        Case {
            family: "zsets",
            name: "zremrangebyscore and byrank",
            steps: vec![
                s(
                    &[
                        b"ZADD", b"zd", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d",
                    ],
                    Expect::Int(4),
                ),
                s(&[b"ZREMRANGEBYSCORE", b"zd", b"2", b"3"], Expect::Int(2)),
                s(
                    &[b"ZRANGE", b"zd", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"d")]),
                ),
                s(&[b"ZREMRANGEBYRANK", b"zd", b"0", b"0"], Expect::Int(1)),
                s(
                    &[b"ZRANGE", b"zd", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"d")]),
                ),
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
        Expect::IntRange(lo, hi) => matches!(got, Value::Integer(n) if n >= lo && n <= hi),
        Expect::Simple(t) => *got == Value::Simple((*t).into()),
        Expect::Str(s) => *got == Value::Bulk(Some(s.to_vec())),
        Expect::Bytes(b) => *got == Value::Bulk(Some(b.clone())),
        Expect::AnyError => matches!(got, Value::Error(_)),
        Expect::Arr(items) => match got {
            Value::Array(Some(vals)) if vals.len() == items.len() => {
                items.iter().zip(vals).all(|(e, v)| matches(e, v))
            }
            _ => false,
        },
        Expect::UnorderedStrs(items) => match got {
            Value::Array(Some(vals)) if vals.len() == items.len() => {
                let mut got_s: Vec<Vec<u8>> = Vec::new();
                for v in vals {
                    match v {
                        Value::Bulk(Some(b)) => got_s.push(b.clone()),
                        _ => return false,
                    }
                }
                got_s.sort();
                let mut want: Vec<Vec<u8>> = items.iter().map(|i| i.to_vec()).collect();
                want.sort();
                got_s == want
            }
            _ => false,
        },
        Expect::UnorderedPairs(pairs) => match got {
            Value::Array(Some(vals)) if vals.len() == pairs.len() * 2 => {
                let mut got_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                for chunk in vals.chunks(2) {
                    match (&chunk[0], &chunk[1]) {
                        (Value::Bulk(Some(f)), Value::Bulk(Some(v))) => {
                            got_pairs.push((f.clone(), v.clone()));
                        }
                        _ => return false,
                    }
                }
                got_pairs.sort();
                let mut want: Vec<(Vec<u8>, Vec<u8>)> = pairs
                    .iter()
                    .map(|(f, v)| (f.to_vec(), v.to_vec()))
                    .collect();
                want.sort();
                got_pairs == want
            }
            _ => false,
        },
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
    for (step_no, (args, expect, delay_ms)) in case.steps.iter().enumerate() {
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
        if *delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(*delay_ms));
        }
    }
    Ok(None)
}
