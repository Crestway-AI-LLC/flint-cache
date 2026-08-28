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
    /// Two genuinely different replies, one per dialect — the escape hatch
    /// for when the protocols disagree about the reply's KIND, not merely
    /// its shape, and no amount of re-rendering can bridge them.
    ///
    /// Exactly one command needs it: `JSON.NUMINCRBY`. RESP2 answers a
    /// JSON string (`[6]`), RESP3 answers a typed RESP array (`*1 :6`), and
    /// for a legacy path that matches nothing RESP2 answers an ERROR where
    /// RESP3 answers an empty array. An encoder cannot turn a string into
    /// an array into an error, so both are carried and the encoder picks.
    ///
    /// Reach for this last. Every other difference between the dialects is
    /// a rendering of the same meaning, and [`Value::Map`], [`Value::Set`]
    /// and friends say that far better than a pair of pre-baked replies.
    ByProto {
        resp2: Box<Value>,
        resp3: Box<Value>,
    },
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

/// True for the command whose two dialects disagree in reply KIND, so the
/// proxy knows to rebuild the RESP2 spelling from the RESP3 one it read.
pub fn resp3_differs_in_kind(command: &[u8]) -> bool {
    command.eq_ignore_ascii_case(b"JSON.NUMINCRBY")
}

/// Rebuild `JSON.NUMINCRBY`'s RESP2 reply from its RESP3 array.
///
/// The proxy reads backends in RESP3, so this is the direction it needs:
/// `*1 :6` becomes the JSON text `[6]` for a `$` caller, or the bare `6`
/// for a legacy one. Every input is derivable — the array holds the
/// matches, and the path the caller wrote says which spelling they expect.
pub fn json_numincrby_resp2(resp3_reply: &Value, jsonpath: bool) -> Value {
    let Value::Array(Some(items)) = resp3_reply else {
        // An error (or anything unexpected) passes straight through: it is
        // already the same in both dialects.
        return resp3_reply.clone();
    };
    let render = |v: &Value| -> Vec<u8> {
        match v {
            Value::Integer(i) => i.to_string().into_bytes(),
            Value::Double(d) => fmt_double(*d),
            _ => b"null".to_vec(),
        }
    };
    if jsonpath {
        let mut out = vec![b'['];
        for (i, item) in items.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            out.extend_from_slice(&render(item));
        }
        out.push(b']');
        return Value::Bulk(Some(out));
    }
    // Legacy: the value itself. No match, or a match that is not a number,
    // is the error RESP2 callers get — the one place the dialects disagree
    // about the KIND of the reply rather than its shape.
    match items.first() {
        Some(Value::Integer(_) | Value::Double(_)) => Value::Bulk(Some(render(&items[0]))),
        _ => Value::Error("ERR Path does not exist or does not contains a number".into()),
    }
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
    let resp3_sel = proto == Proto::Resp3;
    match value {
        Value::Null => {
            out.extend_from_slice(if resp3_sel { b"_\r\n" } else { b"$-1\r\n" });
        }
        Value::Double(d) => {
            if resp3_sel {
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
            if resp3_sel {
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
            out.push(if resp3_sel { b'~' } else { b'*' });
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_proto(item, proto, out);
            }
        }
        Value::ByProto { resp2, resp3 } => {
            encode_proto(if resp3_sel { resp3 } else { resp2 }, proto, out);
        }
        Value::Resp3Nested(inner) => {
            if resp3_sel {
                out.extend_from_slice(b"*1\r\n");
            }
            encode_proto(inner, proto, out);
        }
        Value::ScorePairs(pairs) => {
            out.push(b'*');
            let n = if resp3_sel {
                pairs.len()
            } else {
                pairs.len() * 2
            };
            out.extend_from_slice(n.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for (member, score) in pairs {
                if resp3_sel {
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
        Value::Bulk(None) | Value::Array(None) if resp3_sel => out.extend_from_slice(b"_\r\n"),
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

/// Encode `value` into `out`, draining `out` through `sink` whenever it grows
/// past `threshold`.
///
/// Byte-for-byte identical to [`encode_proto`]; the only thing that changes is
/// WHEN those bytes leave the buffer. For a collection reply that is the
/// difference between the out-buffer holding the whole dataset and holding one
/// flush window: `HGETALL` on a 205 MB hash cost +557 MB of peak RSS because
/// the collection existed twice, once as the reply value and once serialized
/// into this buffer (ADR-0025).
///
/// The bound this gives is `threshold + the largest single element`, not
/// `threshold` — a drain happens BETWEEN elements, and one 512 MB bulk still
/// lands in the buffer whole. Saying otherwise would overstate it: this caps
/// the number of elements resident, not the size of one.
///
/// It also does not bound the reply VALUE, which the caller has already
/// materialized before calling this. Removing that second copy needs the store
/// and the encoder fused, which is a larger change than this one.
pub fn encode_proto_flushing(
    value: &Value,
    proto: Proto,
    out: &mut Vec<u8>,
    threshold: usize,
    sink: &mut dyn FnMut(&mut Vec<u8>) -> std::io::Result<()>,
) -> std::io::Result<()> {
    match value {
        // Recurses, so a nested array drains too. Everything else is a single
        // value with nothing to interleave, and goes out through the ordinary
        // encoder unchanged.
        Value::Array(Some(items)) => {
            out.push(b'*');
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_proto_flushing(item, proto, out, threshold, sink)?;
                if out.len() >= threshold {
                    sink(out)?;
                }
            }
        }
        _ => encode_proto(value, proto, out),
    }
    Ok(())
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
                // Consumed two at a time rather than cloned. `items` holds
                // exactly `2 * len` elements by construction above, so the
                // pairing is total and no element can be dropped; taking them
                // by value also removes two clones per field from a path the
                // proxy runs on every reply it forwards.
                b'%' => {
                    let mut pairs = Vec::with_capacity(items.len() / 2);
                    let mut rest = items.into_iter();
                    while let (Some(k), Some(v)) = (rest.next(), rest.next()) {
                        pairs.push((k, v));
                    }
                    Value::Map(pairs)
                }
                // An array whose every element is a [member, double] pair is
                // a SCORED result, and the decoder says so. This is not a
                // heuristic: `Value::Double` can only have come from an
                // RESP3 frame (`,` does not exist in RESP2), and in this
                // command surface bulk+double pairs occur only as scores.
                // Without the canonicalization, a proxy that decodes a
                // backend's RESP3 ZRANGE..WITHSCORES gets a generic nested
                // array and faithfully re-encodes the NESTING to an RESP2
                // client — which is how every pre-RESP3 client library
                // received corrupt WITHSCORES replies through the edge while
                // conformance, which dials the node, stayed green.
                _ if !items.is_empty()
                    && items.iter().all(|i| {
                        matches!(
                            i,
                            Value::Array(Some(p))
                                if matches!(p.as_slice(), [Value::Bulk(Some(_)), Value::Double(_)])
                        )
                    }) =>
                {
                    Value::ScorePairs(
                        items
                            .into_iter()
                            .map(|i| match i {
                                Value::Array(Some(p)) => match (&p[0], &p[1]) {
                                    (Value::Bulk(Some(m)), Value::Double(s)) => (m.clone(), *s),
                                    _ => unreachable!("checked by the guard"),
                                },
                                _ => unreachable!("checked by the guard"),
                            })
                            .collect(),
                    )
                }
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
    #[test]
    fn a_decoded_resp3_scored_reply_flattens_for_a_resp2_client() {
        // The proxy's whole downgrade path in one assertion: decode the
        // backend's RESP3 nested pairs, re-encode for RESP2, and the client
        // must see the flat interleave — NOT the nesting. This was client
        // bug zero of the edge: every pre-RESP3 library got nested arrays
        // through the proxy while the node answered flat.
        let resp3_frame = b"*2\r\n*2\r\n$1\r\na\r\n,1\r\n*2\r\n$1\r\nb\r\n,2.5\r\n";
        let Ok(Decoded::Complete(v, used)) = decode(resp3_frame) else {
            panic!("frame did not decode");
        };
        assert_eq!(used, resp3_frame.len());
        assert!(
            matches!(&v, Value::ScorePairs(p) if p.len() == 2),
            "decode must canonicalize bulk+double pairs to ScorePairs, got {v:?}"
        );
        let mut resp2 = Vec::new();
        encode_proto(&v, Proto::Resp2, &mut resp2);
        assert_eq!(
            resp2, b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$3\r\n2.5\r\n",
            "RESP2 client must get the flat interleave"
        );
        // And the RESP3 re-encode is byte-identical to what arrived: the
        // canonicalization must be invisible to an RESP3 client.
        let mut resp3 = Vec::new();
        encode_proto(&v, Proto::Resp3, &mut resp3);
        assert_eq!(resp3, resp3_frame);
    }

    #[test]
    fn ordinary_nested_arrays_are_not_flattened() {
        // The negative control: nesting without doubles (EXEC replies,
        // SCAN cursors) must survive untouched — the canonicalization keys
        // on Double, which only an RESP3 frame can carry.
        let frame = b"*2\r\n*2\r\n$1\r\na\r\n$1\r\n1\r\n*2\r\n$1\r\nb\r\n$3\r\n2.5\r\n";
        let Ok(Decoded::Complete(v, _)) = decode(frame) else {
            panic!("frame did not decode");
        };
        assert!(
            matches!(&v, Value::Array(Some(items)) if matches!(items[0], Value::Array(_))),
            "bulk-only nesting must stay nested, got {v:?}"
        );
    }

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
        // ScorePairs DOES round-trip to itself — the decoder canonicalizes
        // an array of bulk+double pairs back to the variant. This assertion
        // used to pin the opposite ("the proxy re-renders it from that
        // array, which is equivalent"), and that claim was the bug: for an
        // RESP2 client the re-render kept the RESP3 NESTING, so every
        // pre-RESP3 library got corrupt WITHSCORES replies through the
        // proxy while the node answered flat. Meaning must survive the
        // decode, or the downgrade has nothing to downgrade from.
        let buf = enc(&Value::ScorePairs(vec![(b"a".to_vec(), 1.0)]), Proto::Resp3);
        assert_eq!(
            decode(&buf),
            Ok(Decoded::Complete(
                Value::ScorePairs(vec![(b"a".to_vec(), 1.0)]),
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

#[cfg(test)]
mod flushing_encoder_tests {
    use super::*;

    /// Drain into one contiguous transcript, exactly as a socket would see it.
    fn transcript(v: &Value, proto: Proto, threshold: usize) -> (Vec<u8>, usize, usize) {
        let mut wire = Vec::new();
        let mut flushes = 0usize;
        let mut peak = 0usize;
        let mut out = Vec::new();
        {
            let mut sink = |o: &mut Vec<u8>| -> std::io::Result<()> {
                flushes += 1;
                wire.extend_from_slice(o);
                o.clear();
                Ok(())
            };
            // peak is sampled by the caller below via the returned buffer, so
            // track it inside the loop instead: encode one element at a time.
            encode_proto_flushing(v, proto, &mut out, threshold, &mut sink)
                .expect("sink never fails");
        }
        peak = peak.max(out.len());
        wire.extend_from_slice(&out);
        (wire, flushes, peak)
    }

    fn big_array(n: usize, elem: usize) -> Value {
        Value::Array(Some(
            (0..n)
                .map(|i| Value::Bulk(Some(vec![b'a' + (i % 26) as u8; elem])))
                .collect(),
        ))
    }

    /// THE delivery control. A streaming encoder that truncates or reorders
    /// produces a wonderful memory number and a broken client, so the bytes
    /// must be indistinguishable from the non-streaming encoder's.
    #[test]
    fn the_wire_bytes_are_identical_to_the_non_streaming_encoder() {
        for proto in [Proto::Resp2, Proto::Resp3] {
            let v = big_array(500, 4096);
            let mut want = Vec::new();
            encode_proto(&v, proto, &mut want);
            let (got, flushes, _) = transcript(&v, proto, 64 * 1024);
            assert_eq!(got, want, "streamed bytes differ from encode_proto");
            assert!(flushes > 0, "nothing was flushed; the test proves nothing");
        }
    }

    /// The array header must state the true element count. This is the failure
    /// the ADR names explicitly: a header that disagrees with the body hangs
    /// or desyncs the client rather than erroring.
    #[test]
    fn the_header_count_matches_the_elements_delivered() {
        let (wire, _, _) = transcript(&big_array(300, 1024), Proto::Resp2, 8 * 1024);
        let header_end = wire
            .windows(2)
            .position(|w| w == b"\r\n")
            .expect("array header has a CRLF");
        let n: usize = std::str::from_utf8(&wire[1..header_end])
            .expect("header is ascii")
            .parse()
            .expect("header is a count");
        assert_eq!(n, 300, "header count");
        assert_eq!(wire[0], b'*');
        // Count delivered bulks by decoding the whole transcript back.
        match decode(&wire).expect("transcript is a decodable frame") {
            Decoded::Complete(Value::Array(Some(items)), used) => {
                assert_eq!(items.len(), 300, "delivered element count");
                assert_eq!(used, wire.len(), "trailing bytes after the array");
            }
            other => panic!("transcript did not decode to a complete array: {other:?}"),
        }
    }

    /// The point of the change: the buffer stops growing with the collection.
    #[test]
    fn the_buffer_does_not_grow_with_the_collection() {
        let threshold = 16 * 1024;
        let mut peaks = Vec::new();
        for n in [50usize, 500, 5000] {
            let v = big_array(n, 1024);
            let mut out = Vec::new();
            let mut peak = 0usize;
            let mut sink = |o: &mut Vec<u8>| -> std::io::Result<()> {
                o.clear();
                Ok(())
            };
            // Encode element-wise so the peak is observed, mirroring what the
            // connection sees between drains.
            if let Value::Array(Some(items)) = &v {
                out.push(b'*');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for item in items {
                    encode_proto_flushing(item, Proto::Resp2, &mut out, threshold, &mut sink)
                        .expect("sink never fails");
                    peak = peak.max(out.len());
                    if out.len() >= threshold {
                        sink(&mut out).expect("sink never fails");
                    }
                }
            }
            peaks.push(peak);
        }
        // 100x the elements must not mean 100x the buffer. Bound is
        // threshold + one element, so assert against that rather than a
        // fixed number that would drift with the fixture.
        let bound = threshold + 1024 + 64;
        for (i, p) in peaks.iter().enumerate() {
            assert!(
                *p <= bound,
                "peak {p} exceeds threshold+element bound {bound} at case {i}"
            );
        }
        assert!(
            peaks[2] <= peaks[0] * 2,
            "buffer scaled with the collection: {peaks:?}"
        );
    }

    /// A reply below the threshold must behave exactly as before — no flush,
    /// so nothing about small replies changes.
    #[test]
    fn a_small_reply_never_flushes() {
        let (wire, flushes, _) = transcript(&big_array(3, 16), Proto::Resp2, 1024 * 1024);
        assert_eq!(flushes, 0, "a small reply should not reach the sink");
        let mut want = Vec::new();
        encode_proto(&big_array(3, 16), Proto::Resp2, &mut want);
        assert_eq!(wire, want);
    }

    /// Nested arrays drain too, rather than silently buffering whole.
    #[test]
    fn nested_arrays_also_drain() {
        let inner = big_array(200, 1024);
        let v = Value::Array(Some(vec![inner.clone(), inner]));
        let mut want = Vec::new();
        encode_proto(&v, Proto::Resp2, &mut want);
        let (got, flushes, _) = transcript(&v, Proto::Resp2, 8 * 1024);
        assert_eq!(got, want);
        assert!(flushes > 1, "nested elements did not drain: {flushes}");
    }
}
