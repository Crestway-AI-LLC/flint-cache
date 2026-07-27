// SPDX-License-Identifier: Elastic-2.0
//! RESP wire protocol: encoding and incremental decoding, RESP2 and RESP3.
//!
//! The decoder is incremental: it consumes from a byte slice and reports
//! `NeedMore` on partial frames, so the server can read from sockets
//! without framing assumptions.
//!
//! ## Why both protocols, and how they coexist
//!
//! RESP3 is not optional in practice: redis-py 8 defaults to it and carries
//! credentials inside `HELLO 3 AUTH ...`, so a server that cannot answer
//! HELLO 3 is unreachable from the whole Python/AI client ecosystem.
//!
//! The two protocols differ only in how a handful of replies are TYPED, not
//! in what they mean — a hash is a map either way, RESP2 just flattens it.
//! So [`Value`] carries the meaning ([`Value::Map`], [`Value::Set`],
//! [`Value::Double`]) and the ENCODER renders it for the protocol the
//! connection negotiated. Command handlers say "this is a map" exactly
//! once and never branch on protocol; [`encode_proto`] does the rest, and
//! its RESP2 rendering is byte-identical to what those handlers used to
//! emit by hand.

/// Which RESP dialect a connection speaks. Connections start at
/// [`Proto::Resp2`] and move to [`Proto::Resp3`] only via `HELLO 3`, so
/// every existing client and every internal hop is unaffected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Proto {
    #[default]
    Resp2,
    Resp3,
}

impl Proto {
    /// The wire version number, as `HELLO` reports it.
    pub fn version(self) -> i64 {
        match self {
            Proto::Resp2 => 2,
            Proto::Resp3 => 3,
        }
    }

    /// Parse a `HELLO` protover argument. `None` for anything Redis would
    /// answer `-NOPROTO` to.
    pub fn from_version(v: i64) -> Option<Self> {
        match v {
            2 => Some(Proto::Resp2),
            3 => Some(Proto::Resp3),
            _ => None,
        }
    }
}

/// A reply value, carrying its MEANING rather than a wire shape.
///
/// `Eq` is deliberately absent: [`Value::Double`] holds an `f64`. Nothing
/// uses `Value` as a hash key, and `==` still works through `PartialEq`.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// `+OK\r\n`
    Simple(String),
    /// `-ERR message\r\n`
    Error(String),
    /// `:42\r\n`
    Integer(i64),
    /// `$5\r\nhello\r\n`; `None` is the null bulk string `$-1\r\n`.
    Bulk(Option<Vec<u8>>),
    /// `*2\r\n...`; `None` is the null array `*-1\r\n`.
    Array(Option<Vec<Value>>),
    /// The null reply. RESP2 spells it `$-1`, RESP3 spells it `_`.
    ///
    /// `Bulk(None)` and `Array(None)` encode identically — under RESP3
    /// there is exactly ONE null and `$-1` is not it. That is not a
    /// stylistic point: a RESP3 parser handed `$-1` sits waiting for a
    /// payload that never comes, so a plain `GET` of a missing key hangs
    /// the client until its socket timeout. This variant exists so
    /// handlers can say "nothing is here" outright.
    Null,
    /// A score or other real number. RESP2 renders it as a bulk string
    /// (Redis's own formatting: integral values print without a decimal
    /// point); RESP3 renders it as `,`.
    Double(f64),
    /// A field/value mapping — hashes, `HELLO`. RESP2 flattens it to an
    /// array of `2n` elements; RESP3 sends `%n`.
    Map(Vec<(Value, Value)>),
    /// An unordered collection. RESP2 sends `*n`; RESP3 sends `~n`.
    Set(Vec<Value>),
    /// A reply RESP3 nests one array level deeper than RESP2 does.
    ///
    /// This exists for exactly one command, `JSON.TYPE`, and it is
    /// bug-compatibility rather than protocol: RedisJSON wraps that reply
    /// in an extra array under RESP3, and redis-py's JSON client unwraps
    /// one level to compensate. Match the quirk and
    /// `r.json().type(key, "$.p")` answers `["array"]` like it does against
    /// the real module; skip it and the same call answers `"array"`, which
    /// is the kind of difference that breaks user code far from here.
    Resp3Nested(Box<Value>),
    /// Member/score pairs — `ZRANGE … WITHSCORES`, `ZPOPMIN key count`.
    ///
    /// This one is a STRUCTURAL difference, not just a type tag: RESP2
    /// flattens to `[m, s, m, s, …]` with string scores, while RESP3 nests
    /// to `[[m, ,s], [m, ,s], …]`. Only a dedicated variant can render
    /// both, which is why it exists alongside `Map`.
    ScorePairs(Vec<(Vec<u8>, f64)>),
}

/// Redis-compatible score formatting: integral values print without a
/// decimal point, everything else uses shortest-roundtrip. This is the
/// RESP2 spelling of [`Value::Double`], and matches what the sorted-set
/// commands emitted before RESP3 existed here — the conformance corpus
/// pins it against a real Valkey.
pub fn fmt_double(s: f64) -> Vec<u8> {
    if s.fract() == 0.0 && s.is_finite() && s.abs() < 1e17 {
        format!("{}", s as i64).into_bytes()
    } else {
        format!("{s}").into_bytes()
    }
}

/// True for commands whose RESP3 reply carries [`Value::Resp3Nested`]'s
/// extra array layer.
///
/// The proxy needs this as well as the server: it reads backend replies in
/// RESP3, so it receives the already-nested frame and has to peel that
/// layer back off before deciding what its OWN client should see.
pub fn resp3_nests_reply(command: &[u8]) -> bool {
    command.eq_ignore_ascii_case(b"JSON.TYPE")
}

/// A parsed `HELLO [protover [AUTH user pass] [SETNAME name]]`.
///
/// The AUTH clause is the load-bearing part. redis-py 8 does not send a
/// separate `AUTH` command when it wants RESP3 — it folds the credentials
/// into HELLO — so a server that parses HELLO but ignores `AUTH` here
/// rejects the entire modern Python client ecosystem with `-NOAUTH`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HelloRequest {
    /// The protocol asked for; `None` for a bare `HELLO`, which Redis
    /// treats as "tell me about yourself" and leaves the dialect alone.
    pub proto: Option<Proto>,
    /// Credentials carried inline: `(username, password)`.
    pub auth: Option<(Vec<u8>, Vec<u8>)>,
}

/// Why a `HELLO` could not be honored. Both map to specific errors Redis
/// clients recognize, so they stay distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HelloError {
    /// A protover we do not speak. Redis answers `-NOPROTO`.
    NoProto,
    /// Malformed arguments.
    Syntax,
}

impl HelloError {
    pub fn reply(self) -> Value {
        match self {
            HelloError::NoProto => Value::Error(
                "NOPROTO unsupported protocol version, supported versions are 2 and 3".into(),
            ),
            HelloError::Syntax => Value::Error("ERR syntax error in HELLO".into()),
        }
    }
}

/// Parse `HELLO`'s arguments (`args[0]` is the command name).
pub fn parse_hello(args: &[Vec<u8>]) -> Result<HelloRequest, HelloError> {
    let mut req = HelloRequest::default();
    if args.len() < 2 {
        return Ok(req);
    }
    let ver = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or(HelloError::NoProto)?;
    req.proto = Some(Proto::from_version(ver).ok_or(HelloError::NoProto)?);
    let mut i = 2;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"AUTH" if i + 2 < args.len() => {
                req.auth = Some((args[i + 1].clone(), args[i + 2].clone()));
                i += 3;
            }
            // SETNAME is accepted and ignored: it names the connection for
            // an operator's benefit, and refusing it would fail clients
            // that always send it.
            b"SETNAME" if i + 1 < args.len() => i += 2,
            _ => return Err(HelloError::Syntax),
        }
    }
    Ok(req)
}

/// The `HELLO` reply: the same seven fields Redis reports, as a map — so
/// it flattens for RESP2 and stays a map for RESP3, automatically.
pub fn hello_reply(proto: Proto, version: &str, role: &str) -> Value {
    let s = |v: &str| Value::Bulk(Some(v.as_bytes().to_vec()));
    Value::Map(vec![
        (s("server"), s("flint")),
        (s("version"), s(version)),
        (s("proto"), Value::Integer(proto.version())),
        (s("id"), Value::Integer(0)),
        (s("mode"), s("standalone")),
        (s("role"), s(role)),
        (s("modules"), Value::Array(Some(vec![]))),
    ])
}

/// Decoding outcome for a single frame. Not `Eq`, for the same reason
/// [`Value`] is not.
#[derive(Debug, PartialEq)]
pub enum Decoded {
    /// A complete value and the number of input bytes it consumed.
    Complete(Value, usize),
    /// The input ends mid-frame; read more bytes and retry.
    NeedMore,
}

/// Errors for malformed frames (protocol violations, not partial input).
#[derive(Debug, PartialEq, Eq)]
pub enum ProtocolError {
    UnknownType(u8),
    BadInteger,
    BadLength,
    MissingCrlf,
    /// Nesting deeper than the decoder permits.
    TooDeep,
}

const MAX_DEPTH: usize = 32;

/// Largest accepted bulk-string payload (Redis `proto-max-bulk-len`).
/// Declared lengths are rejected at header-parse time, BEFORE any payload
/// arrives — otherwise a 5-byte `$4294967296\r\n` header commits the
/// server to buffering 4GB from that connection.
pub const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Largest accepted array element count (Redis caps multibulk at 1M).
pub const MAX_ARRAY_LEN: usize = 1024 * 1024;

/// Encode for RESP2 — the default for every internal hop (proxy→backend
/// admin calls, controller, control plane), which never negotiates HELLO.
pub fn encode(value: &Value, out: &mut Vec<u8>) {
    encode_proto(value, Proto::Resp2, out);
}

/// Encode for the protocol this connection negotiated.
///
/// The RESP2 arm is a DOWNGRADE, and it is the interesting one: it must
/// reproduce exactly what Redis sends to a RESP2 client, because that is
/// what the conformance corpus pins against a real Valkey. A map flattens,
/// a set is a plain array, a double becomes a bulk string, and member/score
/// pairs interleave instead of nesting.
pub fn encode_proto(value: &Value, proto: Proto, out: &mut Vec<u8>) {
    let resp3 = proto == Proto::Resp3;
    match value {
        Value::Null => {
            out.extend_from_slice(if resp3 { b"_\r\n" } else { b"$-1\r\n" });
        }
        Value::Double(d) => {
            if resp3 {
                out.push(b',');
                // RESP3 spells the infinities out; finite values use the
                // same shortest-roundtrip text as the RESP2 bulk.
                if d.is_infinite() {
                    out.extend_from_slice(if *d > 0.0 { b"inf" } else { b"-inf" });
                } else {
                    out.extend_from_slice(&fmt_double(*d));
                }
                out.extend_from_slice(b"\r\n");
            } else {
                encode_proto(&Value::Bulk(Some(fmt_double(*d))), proto, out);
            }
        }
        Value::Map(pairs) => {
            if resp3 {
                out.push(b'%');
                out.extend_from_slice(pairs.len().to_string().as_bytes());
            } else {
                out.push(b'*');
                out.extend_from_slice((pairs.len() * 2).to_string().as_bytes());
            }
            out.extend_from_slice(b"\r\n");
            for (k, v) in pairs {
                encode_proto(k, proto, out);
                encode_proto(v, proto, out);
            }
        }
        Value::Set(items) => {
            out.push(if resp3 { b'~' } else { b'*' });
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_proto(item, proto, out);
            }
        }
        Value::Resp3Nested(inner) => {
            if resp3 {
                out.extend_from_slice(b"*1\r\n");
            }
            encode_proto(inner, proto, out);
        }
        Value::ScorePairs(pairs) => {
            out.push(b'*');
            let n = if resp3 { pairs.len() } else { pairs.len() * 2 };
            out.extend_from_slice(n.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for (member, score) in pairs {
                if resp3 {
                    out.extend_from_slice(b"*2\r\n");
                }
                encode_proto(&Value::Bulk(Some(member.clone())), proto, out);
                encode_proto(&Value::Double(*score), proto, out);
            }
        }
        Value::Simple(s) => {
            out.push(b'+');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Error(s) => {
            out.push(b'-');
            out.extend_from_slice(s.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Integer(i) => {
            out.push(b':');
            out.extend_from_slice(i.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        // Both spellings of "absent" collapse to RESP3's single null.
        Value::Bulk(None) | Value::Array(None) if resp3 => out.extend_from_slice(b"_\r\n"),
        Value::Bulk(None) => out.extend_from_slice(b"$-1\r\n"),
        Value::Bulk(Some(data)) => {
            out.push(b'$');
            out.extend_from_slice(data.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(data);
            out.extend_from_slice(b"\r\n");
        }
        Value::Array(None) => out.extend_from_slice(b"*-1\r\n"),
        Value::Array(Some(items)) => {
            out.push(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_proto(item, proto, out);
            }
        }
    }
}

/// Decode one frame from the front of `input`.
pub fn decode(input: &[u8]) -> Result<Decoded, ProtocolError> {
    decode_at(input, 0)
}

fn decode_at(input: &[u8], depth: usize) -> Result<Decoded, ProtocolError> {
    if depth > MAX_DEPTH {
        return Err(ProtocolError::TooDeep);
    }
    let Some(&type_byte) = input.first() else {
        return Ok(Decoded::NeedMore);
    };
    match type_byte {
        b'+' | b'-' | b':' => {
            let Some(line_end) = find_crlf(&input[1..]) else {
                return Ok(Decoded::NeedMore);
            };
            let line = &input[1..1 + line_end];
            let consumed = 1 + line_end + 2;
            let value = match type_byte {
                b'+' => Value::Simple(to_string(line)?),
                b'-' => Value::Error(to_string(line)?),
                _ => Value::Integer(parse_int(line)?),
            };
            Ok(Decoded::Complete(value, consumed))
        }
        b'$' => {
            let Some(line_end) = find_crlf(&input[1..]) else {
                return Ok(Decoded::NeedMore);
            };
            let len = parse_int(&input[1..1 + line_end])?;
            let header = 1 + line_end + 2;
            if len == -1 {
                return Ok(Decoded::Complete(Value::Bulk(None), header));
            }
            let len = usize::try_from(len).map_err(|_| ProtocolError::BadLength)?;
            if len > MAX_BULK_LEN {
                return Err(ProtocolError::BadLength);
            }
            let total = header + len + 2;
            if input.len() < total {
                return Ok(Decoded::NeedMore);
            }
            if &input[header + len..total] != b"\r\n" {
                return Err(ProtocolError::MissingCrlf);
            }
            let data = input[header..header + len].to_vec();
            Ok(Decoded::Complete(Value::Bulk(Some(data)), total))
        }
        // Aggregates. `*` and `~` carry one element per declared item; `%`
        // carries two (a field and a value), which is the only structural
        // difference between them on the wire.
        b'*' | b'~' | b'%' => {
            let Some(line_end) = find_crlf(&input[1..]) else {
                return Ok(Decoded::NeedMore);
            };
            let len = parse_int(&input[1..1 + line_end])?;
            let mut offset = 1 + line_end + 2;
            if len == -1 {
                return Ok(Decoded::Complete(Value::Array(None), offset));
            }
            let len = usize::try_from(len).map_err(|_| ProtocolError::BadLength)?;
            if len > MAX_ARRAY_LEN {
                return Err(ProtocolError::BadLength);
            }
            let per = if type_byte == b'%' { 2 } else { 1 };
            let mut items = Vec::with_capacity((len * per).min(1024));
            for _ in 0..len * per {
                match decode_at(&input[offset..], depth + 1)? {
                    Decoded::Complete(value, used) => {
                        items.push(value);
                        offset += used;
                    }
                    Decoded::NeedMore => return Ok(Decoded::NeedMore),
                }
            }
            let value = match type_byte {
                b'~' => Value::Set(items),
                b'%' => Value::Map(
                    items
                        .chunks_exact(2)
                        .map(|p| (p[0].clone(), p[1].clone()))
                        .collect(),
                ),
                _ => Value::Array(Some(items)),
            };
            Ok(Decoded::Complete(value, offset))
        }
        // RESP3 scalars.
        b'_' => match find_crlf(&input[1..]) {
            // `_\r\n` carries no payload, so the CRLF must sit immediately
            // after the type byte; anything else is a framing error.
            Some(0) => Ok(Decoded::Complete(Value::Null, 3)),
            Some(_) => Err(ProtocolError::MissingCrlf),
            None => Ok(Decoded::NeedMore),
        },
        b',' => {
            let Some(line_end) = find_crlf(&input[1..]) else {
                return Ok(Decoded::NeedMore);
            };
            let line = &input[1..1 + line_end];
            let text = std::str::from_utf8(line).map_err(|_| ProtocolError::BadInteger)?;
            let d = match text {
                "inf" => f64::INFINITY,
                "-inf" => f64::NEG_INFINITY,
                other => other.parse().map_err(|_| ProtocolError::BadInteger)?,
            };
            Ok(Decoded::Complete(Value::Double(d), 1 + line_end + 2))
        }
        // `#t` / `#f`. We never emit booleans, but a RESP3 peer may, and
        // silently failing to parse one would desynchronize the stream.
        b'#' => {
            let Some(line_end) = find_crlf(&input[1..]) else {
                return Ok(Decoded::NeedMore);
            };
            let v = match &input[1..1 + line_end] {
                b"t" => 1,
                b"f" => 0,
                _ => return Err(ProtocolError::BadInteger),
            };
            Ok(Decoded::Complete(Value::Integer(v), 1 + line_end + 2))
        }
        other => Err(ProtocolError::UnknownType(other)),
    }
}

fn find_crlf(input: &[u8]) -> Option<usize> {
    input.windows(2).position(|w| w == b"\r\n")
}

fn to_string(line: &[u8]) -> Result<String, ProtocolError> {
    String::from_utf8(line.to_vec()).map_err(|_| ProtocolError::BadInteger)
}

fn parse_int(line: &[u8]) -> Result<i64, ProtocolError> {
    let s = std::str::from_utf8(line).map_err(|_| ProtocolError::BadInteger)?;
    s.parse().map_err(|_| ProtocolError::BadInteger)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(value: Value) {
        let mut buf = Vec::new();
        encode(&value, &mut buf);
        match decode(&buf) {
            Ok(Decoded::Complete(decoded, consumed)) => {
                assert_eq!(decoded, value);
                assert_eq!(consumed, buf.len());
            }
            other => panic!("roundtrip failed: {other:?}"),
        }
    }

    #[test]
    fn roundtrips() {
        roundtrip(Value::Simple("OK".into()));
        roundtrip(Value::Error("ERR unknown command".into()));
        roundtrip(Value::Integer(-42));
        roundtrip(Value::Bulk(None));
        roundtrip(Value::Bulk(Some(b"hello\r\nworld".to_vec())));
        roundtrip(Value::Array(None));
        roundtrip(Value::Array(Some(vec![
            Value::Bulk(Some(b"SET".to_vec())),
            Value::Bulk(Some(b"key".to_vec())),
            Value::Bulk(Some(b"value".to_vec())),
            Value::Array(Some(vec![Value::Integer(1)])),
        ])));
    }

    #[test]
    fn partial_frames_need_more() {
        let mut buf = Vec::new();
        encode(
            &Value::Array(Some(vec![
                Value::Bulk(Some(b"GET".to_vec())),
                Value::Bulk(Some(b"k".to_vec())),
            ])),
            &mut buf,
        );
        for cut in 0..buf.len() {
            assert_eq!(
                decode(&buf[..cut]),
                Ok(Decoded::NeedMore),
                "prefix of {cut} bytes should be incomplete"
            );
        }
    }

    #[test]
    fn pipelined_frames_report_exact_consumption() {
        let mut buf = Vec::new();
        encode(&Value::Simple("OK".into()), &mut buf);
        let first_len = buf.len();
        encode(&Value::Integer(7), &mut buf);
        let Ok(Decoded::Complete(v, used)) = decode(&buf) else {
            panic!("first frame should decode");
        };
        assert_eq!(v, Value::Simple("OK".into()));
        assert_eq!(used, first_len);
        let Ok(Decoded::Complete(v2, _)) = decode(&buf[used..]) else {
            panic!("second frame should decode");
        };
        assert_eq!(v2, Value::Integer(7));
    }

    #[test]
    fn malformed_input_errors() {
        assert_eq!(decode(b"?bogus\r\n"), Err(ProtocolError::UnknownType(b'?')));
        assert_eq!(decode(b":notanum\r\n"), Err(ProtocolError::BadInteger));
        assert_eq!(decode(b"$-2\r\n"), Err(ProtocolError::BadLength));
        assert_eq!(decode(b"$3\r\nabcXY"), Err(ProtocolError::MissingCrlf));
    }

    /// The caps must fire on the HEADER alone: waiting for payload bytes
    /// would defeat their purpose (bounding what a connection can make the
    /// server buffer).
    #[test]
    fn oversized_declared_lengths_are_rejected_from_the_header() {
        // 4GB bulk declaration, zero payload bytes sent.
        assert_eq!(decode(b"$4294967296\r\n"), Err(ProtocolError::BadLength));
        // One past the cap fails; the cap itself parses (NeedMore: header
        // accepted, awaiting payload).
        let over = format!("${}\r\n", MAX_BULK_LEN + 1);
        assert_eq!(decode(over.as_bytes()), Err(ProtocolError::BadLength));
        let at = format!("${MAX_BULK_LEN}\r\n");
        assert_eq!(decode(at.as_bytes()), Ok(Decoded::NeedMore));
        // Same for array element counts.
        let over = format!("*{}\r\n", MAX_ARRAY_LEN + 1);
        assert_eq!(decode(over.as_bytes()), Err(ProtocolError::BadLength));
        let at = format!("*{MAX_ARRAY_LEN}\r\n");
        assert_eq!(decode(at.as_bytes()), Ok(Decoded::NeedMore));
    }

    fn enc(v: &Value, p: Proto) -> Vec<u8> {
        let mut b = Vec::new();
        encode_proto(v, p, &mut b);
        b
    }

    /// Every expectation here was captured off the wire from a real Redis
    /// 8.2 answering the same command to a RESP2 and a RESP3 client. That
    /// is the whole point of the exercise: guessing these shapes is how you
    /// ship a client-visible protocol bug.
    #[test]
    fn resp3_shapes_match_real_redis_and_resp2_downgrades_cleanly() {
        // null: every spelling of absent is `_` under RESP3. Sending
        // `$-1` there leaves a RESP3 parser blocked on a payload that
        // never arrives — a hung client, not a wrong value.
        assert_eq!(enc(&Value::Null, Proto::Resp2), b"$-1\r\n");
        assert_eq!(enc(&Value::Null, Proto::Resp3), b"_\r\n");
        assert_eq!(enc(&Value::Bulk(None), Proto::Resp2), b"$-1\r\n");
        assert_eq!(enc(&Value::Bulk(None), Proto::Resp3), b"_\r\n");
        assert_eq!(enc(&Value::Array(None), Proto::Resp2), b"*-1\r\n");
        assert_eq!(enc(&Value::Array(None), Proto::Resp3), b"_\r\n");
        // Nulls nested inside aggregates too — MGET and HMGET are full of
        // them, and one `$-1` in a 100-element array hangs just as hard.
        assert_eq!(
            enc(
                &Value::Array(Some(vec![
                    Value::Bulk(Some(b"v".to_vec())),
                    Value::Bulk(None)
                ])),
                Proto::Resp3
            ),
            b"*2\r\n$1\r\nv\r\n_\r\n"
        );
        // doubles: integral scores print without a decimal point in BOTH.
        assert_eq!(enc(&Value::Double(1.0), Proto::Resp2), b"$1\r\n1\r\n");
        assert_eq!(enc(&Value::Double(1.0), Proto::Resp3), b",1\r\n");
        assert_eq!(enc(&Value::Double(2.5), Proto::Resp2), b"$3\r\n2.5\r\n");
        assert_eq!(enc(&Value::Double(2.5), Proto::Resp3), b",2.5\r\n");
        assert_eq!(
            enc(&Value::Double(f64::INFINITY), Proto::Resp3),
            b",inf\r\n"
        );
        // HGETALL
        let m = Value::Map(vec![(
            Value::Bulk(Some(b"f1".to_vec())),
            Value::Bulk(Some(b"v1".to_vec())),
        )]);
        assert_eq!(enc(&m, Proto::Resp2), b"*2\r\n$2\r\nf1\r\n$2\r\nv1\r\n");
        assert_eq!(enc(&m, Proto::Resp3), b"%1\r\n$2\r\nf1\r\n$2\r\nv1\r\n");
        // empty hash: *0 vs %0
        assert_eq!(enc(&Value::Map(vec![]), Proto::Resp2), b"*0\r\n");
        assert_eq!(enc(&Value::Map(vec![]), Proto::Resp3), b"%0\r\n");
        // SMEMBERS
        let s = Value::Set(vec![Value::Bulk(Some(b"a".to_vec()))]);
        assert_eq!(enc(&s, Proto::Resp2), b"*1\r\n$1\r\na\r\n");
        assert_eq!(enc(&s, Proto::Resp3), b"~1\r\n$1\r\na\r\n");
        assert_eq!(enc(&Value::Set(vec![]), Proto::Resp3), b"~0\r\n");
        // ZRANGE … WITHSCORES: flat in RESP2, nested pairs in RESP3.
        let z = Value::ScorePairs(vec![(b"a".to_vec(), 1.0), (b"b".to_vec(), 2.0)]);
        assert_eq!(
            enc(&z, Proto::Resp2),
            b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n"
        );
        assert_eq!(
            enc(&z, Proto::Resp3),
            b"*2\r\n*2\r\n$1\r\na\r\n,1\r\n*2\r\n$1\r\nb\r\n,2\r\n"
        );
        // An empty score list is *0 in both — no nesting to speak of.
        assert_eq!(enc(&Value::ScorePairs(vec![]), Proto::Resp2), b"*0\r\n");
        assert_eq!(enc(&Value::ScorePairs(vec![]), Proto::Resp3), b"*0\r\n");
    }

    /// The proxy reads backend replies with this decoder, so every RESP3
    /// type a backend can emit has to survive the round trip with its
    /// meaning intact — otherwise the downgrade at the client edge would
    /// re-render it as the wrong shape.
    #[test]
    fn resp3_frames_decode_back_to_their_meaning() {
        for v in [
            Value::Null,
            Value::Double(1.0),
            Value::Double(-2.5),
            Value::Map(vec![(Value::Bulk(Some(b"k".to_vec())), Value::Integer(3))]),
            Value::Set(vec![Value::Bulk(Some(b"a".to_vec())), Value::Integer(2)]),
        ] {
            let buf = enc(&v, Proto::Resp3);
            assert_eq!(
                decode(&buf),
                Ok(Decoded::Complete(v.clone(), buf.len())),
                "{v:?} did not survive a RESP3 round trip"
            );
        }
        // ScorePairs is the one variant that does NOT round-trip to itself:
        // on the wire it is an array of pairs, and that is what comes back.
        // The proxy re-renders it from that array, which is equivalent.
        let buf = enc(&Value::ScorePairs(vec![(b"a".to_vec(), 1.0)]), Proto::Resp3);
        assert_eq!(
            decode(&buf),
            Ok(Decoded::Complete(
                Value::Array(Some(vec![Value::Array(Some(vec![
                    Value::Bulk(Some(b"a".to_vec())),
                    Value::Double(1.0),
                ]))])),
                buf.len()
            ))
        );
        // Booleans: we never send them, but must not choke on one.
        assert_eq!(
            decode(b"#t\r\n"),
            Ok(Decoded::Complete(Value::Integer(1), 4))
        );
        assert_eq!(
            decode(b"#f\r\n"),
            Ok(Decoded::Complete(Value::Integer(0), 4))
        );
        // A truncated null is NeedMore, not a silent accept.
        assert_eq!(decode(b"_"), Ok(Decoded::NeedMore));
    }

    /// The exact frame redis-py 8 opens every connection with, and the
    /// variants other clients send. Getting this wrong is not a degraded
    /// experience — it is "cannot connect".
    #[test]
    fn hello_parses_the_frames_real_clients_send() {
        let a = |parts: &[&str]| -> Vec<Vec<u8>> {
            parts.iter().map(|p| p.as_bytes().to_vec()).collect()
        };
        // redis-py 8's default: credentials folded into HELLO.
        assert_eq!(
            parse_hello(&a(&["HELLO", "3", "AUTH", "default", "tok"])),
            Ok(HelloRequest {
                proto: Some(Proto::Resp3),
                auth: Some((b"default".to_vec(), b"tok".to_vec())),
            })
        );
        // Bare HELLO asks about the server without changing the dialect.
        assert_eq!(parse_hello(&a(&["HELLO"])), Ok(HelloRequest::default()));
        // Explicit RESP2, and SETNAME accepted-and-ignored.
        assert_eq!(
            parse_hello(&a(&["HELLO", "2"])).expect("hello 2").proto,
            Some(Proto::Resp2)
        );
        assert_eq!(
            parse_hello(&a(&["HELLO", "3", "SETNAME", "app"]))
                .expect("setname")
                .proto,
            Some(Proto::Resp3)
        );
        assert_eq!(
            parse_hello(&a(&["HELLO", "3", "AUTH", "u", "p", "SETNAME", "app"]))
                .expect("both")
                .auth,
            Some((b"u".to_vec(), b"p".to_vec()))
        );
        // Versions we do not speak, and junk, stay distinguishable.
        assert_eq!(parse_hello(&a(&["HELLO", "4"])), Err(HelloError::NoProto));
        assert_eq!(parse_hello(&a(&["HELLO", "x"])), Err(HelloError::NoProto));
        assert_eq!(
            parse_hello(&a(&["HELLO", "3", "BOGUS"])),
            Err(HelloError::Syntax)
        );
        // A truncated AUTH clause is a syntax error, never a silent
        // "authenticated with an empty password".
        assert_eq!(
            parse_hello(&a(&["HELLO", "3", "AUTH", "u"])),
            Err(HelloError::Syntax)
        );
    }

    #[test]
    fn hello_reply_flattens_for_resp2_and_stays_a_map_for_resp3() {
        let r = hello_reply(Proto::Resp3, "0.0.1", "master");
        assert!(enc(&r, Proto::Resp3).starts_with(b"%7\r\n"));
        assert!(enc(&r, Proto::Resp2).starts_with(b"*14\r\n"));
        // The reported proto must be the one actually in force, or clients
        // that do check it will disconnect.
        assert!(
            enc(&r, Proto::Resp3)
                .windows(9)
                .any(|w| w == b"proto\r\n:3")
        );
        let r2 = hello_reply(Proto::Resp2, "0.0.1", "master");
        assert!(
            enc(&r2, Proto::Resp2)
                .windows(9)
                .any(|w| w == b"proto\r\n:2")
        );
    }

    #[test]
    fn deep_nesting_is_rejected() {
        let mut buf = Vec::new();
        for _ in 0..64 {
            buf.extend_from_slice(b"*1\r\n");
        }
        buf.extend_from_slice(b":1\r\n");
        assert_eq!(decode(&buf), Err(ProtocolError::TooDeep));
    }
}
