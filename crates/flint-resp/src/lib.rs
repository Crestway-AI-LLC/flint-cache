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
    /// A yes/no answer. RESP3 sends `#t`/`#f`; RESP2 has no boolean and
    /// sends `:1`/`:0`, as Redis downgrades one. RedisBloom answers
    /// `BF.ADD`, `BF.EXISTS` and their multi forms this way under RESP3
    /// (BUG-0239), and a RESP3 client library hands the caller `True`
    /// rather than `1`.
    Boolean(bool),
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
    /// A coordinate, as `addReplyHumanLongDouble` spells one for GEOPOS and
    /// a geo search's WITHCOORD ([`fmt_human_double`]): a bulk string in
    /// RESP2 and a `,` frame in RESP3. Valkey 9.1 spells coordinates so;
    /// Redis 8.2 spells them as [`Value::Double`] does, and where the two
    /// differ Flint answers as Valkey. Decoded, the frame is a
    /// [`Value::Double`] again, so the proxy restores this by command
    /// ([`geo_reply`]).
    HumanDouble(f64),
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
    /// Two commands need it: `JSON.NUMINCRBY` and its twin
    /// `JSON.NUMMULTBY` (ADR-0055). RESP2 answers a
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
    /// Out-of-band data the server sends unasked: a pub/sub message
    /// (ADR-0052 D5). RESP3 sends `>n`, which a client tells apart from a
    /// reply; RESP2 has no such type and sends the same elements as `*n`,
    /// which is what Redis does for a subscribed RESP2 client.
    Push(Vec<Value>),
}

/// A double as Redis spells one (`d2string`): the RESP2 spelling of
/// [`Value::Double`] and the text of a RESP3 `,` frame. The conformance
/// corpus pins it against a real Valkey.
///
/// An integral value within ±2^62 prints as an integer. Anything else is the
/// shortest digits that round-trip, laid out as Redis's `fpconv_dtoa` lays
/// them out: in full while the exponent is small, and as `1.5e+300` or
/// `1e-7` past that. This spelled every value out in full until BUG-0214,
/// so `1e20` was 21 digits and `5e-324` was 326 characters. Redis's digits
/// come from Grisu2, which does not always pick the shortest or the nearest
/// last digit: 13 of 4,999 random doubles, all with 16 or 17 significant
/// digits, read back differently here, and each spelling named the same
/// double.
pub fn fmt_double(s: f64) -> Vec<u8> {
    if s.is_nan() {
        return b"nan".to_vec();
    }
    if s.is_infinite() {
        return if s > 0.0 {
            b"inf".to_vec()
        } else {
            b"-inf".to_vec()
        };
    }
    // Redis's d2string spells the sign of a zero (BUG-0230).
    if s == 0.0 {
        return if s.is_sign_negative() {
            b"-0".to_vec()
        } else {
            b"0".to_vec()
        };
    }
    const HALF: f64 = (i64::MAX / 2) as f64;
    if s.fract() == 0.0 && (-HALF..=HALF).contains(&s) {
        return (s as i64).to_string().into_bytes();
    }
    // `{:e}` gives the shortest round-trip digits: "-1.23456789e-4".
    let sci = format!("{s:e}");
    let (mantissa, exp10) = sci.split_once('e').expect("{:e} has an exponent");
    let exp10: i32 = exp10.parse().expect("{:e} exponent is an integer");
    let digits: Vec<u8> = mantissa.bytes().filter(u8::is_ascii_digit).collect();
    let n = digits.len() as i32;
    // The value is `digits * 10^k`, and `exp` is the scientific exponent's
    // magnitude: fpconv's `emit_digits`, case for case.
    let k = exp10 - (n - 1);
    let exp = exp10.abs();
    let mut out = Vec::with_capacity(32);
    if s < 0.0 {
        out.push(b'-');
    }
    if k >= 0 && exp < n + 7 {
        out.extend_from_slice(&digits);
        out.resize(out.len() + k as usize, b'0');
    } else if k < 0 && (k > -7 || exp < 4) {
        let point = n + k;
        if point <= 0 {
            out.extend_from_slice(b"0.");
            out.resize(out.len() + (-point) as usize, b'0');
            out.extend_from_slice(&digits);
        } else {
            out.extend_from_slice(&digits[..point as usize]);
            out.push(b'.');
            out.extend_from_slice(&digits[point as usize..]);
        }
    } else {
        out.push(digits[0]);
        if n > 1 {
            out.push(b'.');
            out.extend_from_slice(&digits[1..]);
        }
        out.push(b'e');
        out.push(if exp10 < 0 { b'-' } else { b'+' });
        out.extend_from_slice(exp.to_string().as_bytes());
    }
    out
}

/// True when this command's null reply is a null ARRAY, `*-1` under RESP2,
/// rather than a null bulk. RESP3 has one null, `_`, and the proxy reads
/// seats in RESP3, so it needs this to give a RESP2 client the null Redis
/// sends:
/// - BLPOP, BRPOP, BZPOPMIN and BZPOPMAX, in any form, and LMPOP, ZMPOP,
///   BLMPOP and BZMPOP;
/// - LPOP and RPOP with a count, and ZRANK and ZREVRANK with WITHSCORE
///   (BUG-0215).
pub fn null_is_array(args: &[Vec<u8>]) -> bool {
    let Some(name) = args.first() else {
        return false;
    };
    let is = |n: &[u8]| name.eq_ignore_ascii_case(n);
    is(b"BLPOP")
        || is(b"BRPOP")
        || is(b"BZPOPMIN")
        || is(b"BZPOPMAX")
        || is(b"LMPOP")
        || is(b"ZMPOP")
        || is(b"BLMPOP")
        || is(b"BZMPOP")
        || (is(b"LPOP") || is(b"RPOP")) && args.len() == 3
        || (is(b"ZRANK") || is(b"ZREVRANK")) && args.len() == 4
        // Streams (ADR-0052 D6): an XREAD that finds nothing, and an XRANGE
        // asked for `COUNT 0`.
        || is(b"XREAD")
        || is(b"XRANGE")
        || is(b"XREVRANGE")
}

/// XREAD's reply, as RESP2 spells it: a list of `[key, entries]` pairs,
/// where RESP3 answers a map (ADR-0052 D6). Anything else is unchanged.
pub fn xread_resp2(v: &Value) -> Value {
    match v {
        Value::Map(pairs) => Value::Array(Some(
            pairs
                .iter()
                .map(|(k, e)| Value::Array(Some(vec![k.clone(), e.clone()])))
                .collect(),
        )),
        other => other.clone(),
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

/// True for the commands whose two dialects disagree in reply KIND, so the
/// proxy knows to rebuild the RESP2 spelling from the RESP3 one it read:
/// `JSON.NUMINCRBY`, and `JSON.NUMMULTBY`, which RedisJSON answers the same
/// way (ADR-0055).
pub fn resp3_differs_in_kind(command: &[u8]) -> bool {
    command.eq_ignore_ascii_case(b"JSON.NUMINCRBY")
        || command.eq_ignore_ascii_case(b"JSON.NUMMULTBY")
}

/// A double as upstream's `ld2string` spells it in its human mode, which is
/// how GEOPOS and WITHCOORD spell a coordinate: `%.17Lf`, its trailing
/// zeros and then a bare trailing point dropped, and a `-0` left as `0`.
/// `long double` is `double` on arm64, where Valkey 9.1 is checked. The
/// same spelling as `flint_storage::strings::fmt_float_human`, which
/// INCRBYFLOAT's stored text uses.
pub fn fmt_human_double(d: f64) -> Vec<u8> {
    let mut s = format!("{d:.17}");
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s == "-0" {
        s.remove(0);
    }
    s.into_bytes()
}

/// The geo commands whose replies carry coordinates: GEOPOS, and a search
/// asked for WITHCOORD. Their doubles are all coordinates.
pub fn geo_coordinates(args: &[Vec<u8>]) -> bool {
    args.first().is_some_and(|n| {
        matches!(
            n.to_ascii_uppercase().as_slice(),
            b"GEOPOS"
                | b"GEORADIUS"
                | b"GEORADIUS_RO"
                | b"GEORADIUSBYMEMBER"
                | b"GEORADIUSBYMEMBER_RO"
                | b"GEOSEARCH"
        )
    })
}

/// A [`geo_coordinates`] reply read back from RESP3, as its own: its
/// doubles spelled as coordinates again ([`Value::HumanDouble`]), and a
/// null inside it, which only GEOPOS has, for a missing member, a null
/// ARRAY again, which RESP2 spells `*-1`. The 17 decimals name the double
/// they were printed from exactly enough that printing it again gives them
/// back.
pub fn geo_reply(v: &Value) -> Value {
    fn inner(v: &Value) -> Value {
        match v {
            Value::Double(d) => Value::HumanDouble(*d),
            Value::Null | Value::Bulk(None) => Value::Array(None),
            Value::Array(Some(items)) => Value::Array(Some(items.iter().map(inner).collect())),
            other => other.clone(),
        }
    }
    match v {
        Value::Array(Some(items)) => Value::Array(Some(items.iter().map(inner).collect())),
        other => other.clone(),
    }
}

/// A finite double as the seat's JSON text spells it, by the same code: the
/// seat writes NUMINCRBY's RESP2 text with serde_json, and the proxy
/// rebuilds that text from the seat's RESP3 reply, where a double is only a
/// number. Spelled the Redis way (`fmt_double`), `3.0` came back as `3`
/// (BUG-0235). serde_json's formatter is not Rust's `{:e}`: where two
/// shortest spellings tie, they pick differently, so only the seat's own
/// library matches it everywhere.
pub fn fmt_json_double(d: f64) -> Vec<u8> {
    match serde_json::Number::from_f64(d) {
        Some(n) => n.to_string().into_bytes(),
        // Not finite: the seat refuses such a result, so none arrives here.
        None => fmt_double(d),
    }
}

/// True for `BF.INFO key field`, whose two dialects differ in shape in a way
/// no generic downgrade recovers: RedisBloom answers a one-pair map under
/// RESP3 (`%1 +Capacity :100`) and a one-element array holding only the
/// value under RESP2 (`*1 :100`), where flattening the map would give two
/// elements (BUG-0239).
pub fn bf_info_field(args: &[Vec<u8>]) -> bool {
    args.len() == 3 && args[0].eq_ignore_ascii_case(b"BF.INFO")
}

/// True for `HRANDFIELD key count WITHVALUES`, whose RESP3 reply nests each
/// field with its value (`*2 *2 $f $v ...`) where RESP2 interleaves them.
/// Unlike a sorted set's pairs, which carry a double and decode back to
/// [`Value::ScorePairs`], two strings in an array say nothing about being a
/// pair, so the command has to.
pub fn hrandfield_withvalues(args: &[Vec<u8>]) -> bool {
    args.len() == 4
        && args[0].eq_ignore_ascii_case(b"HRANDFIELD")
        && args[3].eq_ignore_ascii_case(b"WITHVALUES")
}

/// ZMPOP and BZMPOP, whose `[member, score]` pairs stay nested in RESP2 as
/// well. The decoder reads RESP3 bulk+double pairs as a scored result
/// ([`Value::ScorePairs`]), which RESP2 renders interleaved, so the command
/// says these are pairs.
pub fn zmpop_reply(args: &[Vec<u8>]) -> bool {
    args.first()
        .is_some_and(|n| n.eq_ignore_ascii_case(b"ZMPOP") || n.eq_ignore_ascii_case(b"BZMPOP"))
}

/// A ZMPOP reply, `[key, pairs]`, with its pairs as nested arrays again,
/// which encode as pairs in both protocols. Anything else is unchanged.
pub fn zmpop_nested(v: &Value) -> Value {
    match v {
        Value::Array(Some(items)) => match items.as_slice() {
            [key, Value::ScorePairs(pairs)] => Value::Array(Some(vec![
                key.clone(),
                Value::Array(Some(
                    pairs
                        .iter()
                        .map(|(m, s)| {
                            Value::Array(Some(vec![
                                Value::Bulk(Some(m.clone())),
                                Value::Double(*s),
                            ]))
                        })
                        .collect(),
                )),
            ])),
            _ => v.clone(),
        },
        _ => v.clone(),
    }
}

/// The RESP2 spelling of a RESP3 array of pairs: interleaved. Anything else
/// (an error) passes through.
pub fn flatten_pairs(resp3_reply: &Value) -> Value {
    match resp3_reply {
        Value::Array(Some(items)) => Value::Array(Some(
            items
                .iter()
                .flat_map(|pair| match pair {
                    Value::Array(Some(p)) => p.clone(),
                    other => vec![other.clone()],
                })
                .collect(),
        )),
        other => other.clone(),
    }
}

/// Rebuild a one-field `BF.INFO`'s RESP2 reply from its RESP3 map: the value
/// alone, in a one-element array. Anything else (an error) passes through.
pub fn bf_info_field_resp2(resp3_reply: &Value) -> Value {
    match resp3_reply {
        Value::Map(pairs) if pairs.len() == 1 => Value::Array(Some(vec![pairs[0].1.clone()])),
        other => other.clone(),
    }
}

/// Rebuild `JSON.NUMINCRBY`'s (or `JSON.NUMMULTBY`'s) RESP2 reply from its
/// RESP3 array.
///
/// The proxy reads backends in RESP3, so this is the direction it needs:
/// `*1 :6` becomes the JSON text `[6]` for a `$` caller, or the bare `6`
/// for a legacy one. Every input is derivable — the array holds the
/// matches, and the path the caller wrote (`None` for none, the legacy
/// root) says which spelling they expect and names itself in the legacy
/// refusal (BUG-0236).
pub fn json_numincrby_resp2(resp3_reply: &Value, path: Option<&[u8]>) -> Value {
    let path = String::from_utf8_lossy(path.unwrap_or(b"."));
    let jsonpath = path.starts_with('$');
    let Value::Array(Some(items)) = resp3_reply else {
        // An error (or anything unexpected) passes straight through: it is
        // already the same in both dialects.
        return resp3_reply.clone();
    };
    // JSON text, as the seat writes it: a double keeps its `.0` (BUG-0235).
    let render = |v: &Value| -> Vec<u8> {
        match v {
            Value::Integer(i) => i.to_string().into_bytes(),
            Value::Double(d) => fmt_json_double(*d),
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
        _ => Value::Error(format!(
            "ERR Path '{}' does not exist or does not contains a number",
            json_fixed_path(&path)
        )),
    }
}

/// A JSON path as most of RedisJSON's errors name it: a `$` path as
/// written, a legacy one rewritten the module's way, `.` to `$`, `.a` to
/// `$.a`, and anything else behind `$.`, so `a` is `$.a` and `[0]` is
/// `$.[0]` (BUG-0236). Here rather than in the server so the proxy, which
/// rebuilds one of those errors, spells it the same way.
pub fn json_fixed_path(path: &str) -> String {
    if path.starts_with('$') {
        path.to_string()
    } else if path == "." {
        "$".to_string()
    } else if path.starts_with('.') {
        format!("${path}")
    } else {
        format!("$.{path}")
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
    /// `SETNAME`'s connection name. The proxy keeps it for `CLIENT GETNAME`
    /// (BUG-0183); a seat has no use for it.
    pub setname: Option<Vec<u8>>,
}

/// Why a `HELLO` could not be honored, in Redis's and Valkey's words
/// (BUG-0225): clients recognize `-NOPROTO`, so the cases stay distinct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HelloError {
    /// An integer protover we do not speak. Redis answers `-NOPROTO`.
    NoProto,
    /// A protover that is not an integer.
    NotInteger,
    /// An option HELLO does not take, or one short of its arguments: the
    /// option as the client spelled it.
    Syntax(String),
}

impl HelloError {
    pub fn reply(self) -> Value {
        match self {
            HelloError::NoProto => Value::Error("NOPROTO unsupported protocol version".into()),
            HelloError::NotInteger => {
                Value::Error("ERR Protocol version is not an integer or out of range".into())
            }
            HelloError::Syntax(opt) => {
                Value::Error(format!("ERR Syntax error in HELLO option '{opt}'"))
            }
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
        .ok_or(HelloError::NotInteger)?;
    req.proto = Some(Proto::from_version(ver).ok_or(HelloError::NoProto)?);
    let mut i = 2;
    while i < args.len() {
        let opt = args[i].to_ascii_uppercase();
        match opt.as_slice() {
            b"AUTH" if i + 2 < args.len() => {
                req.auth = Some((args[i + 1].clone(), args[i + 2].clone()));
                i += 3;
            }
            // SETNAME names the connection for an operator's benefit.
            // Refusing it would fail clients that always send it.
            b"SETNAME" if i + 1 < args.len() => {
                req.setname = Some(args[i + 1].clone());
                i += 2;
            }
            _ => {
                return Err(HelloError::Syntax(
                    String::from_utf8_lossy(&args[i]).into_owned(),
                ));
            }
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

/// The text of a simple string or error: ONE line on the wire (BUG-0184).
///
/// An error can carry a client's own bytes: the unknown-command reply
/// echoes the arguments, and a Lua script sent with `EVAL` is usually many
/// lines. A raw LF inside the line is not a terminator to every parser:
/// redis-py's reads to the first LF, finds no CR before it, and waits for more
/// bytes that never come, so django-redis's `incr` hung until its socket
/// timeout instead of failing at once. Upstream replaces CR and LF in an
/// error with spaces before sending it; so does this, for simple strings too.
fn push_line(out: &mut Vec<u8>, s: &str) {
    if s.bytes().any(|b| b == b'\r' || b == b'\n') {
        out.extend(
            s.bytes()
                .map(|b| if b == b'\r' || b == b'\n' { b' ' } else { b }),
        );
    } else {
        out.extend_from_slice(s.as_bytes());
    }
}

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
                // The same text as the RESP2 bulk, the infinities included.
                out.extend_from_slice(&fmt_double(*d));
                out.extend_from_slice(b"\r\n");
            } else {
                encode_proto(&Value::Bulk(Some(fmt_double(*d))), proto, out);
            }
        }
        Value::HumanDouble(d) => {
            if resp3_sel {
                out.push(b',');
                out.extend_from_slice(&fmt_human_double(*d));
                out.extend_from_slice(b"\r\n");
            } else {
                encode_proto(&Value::Bulk(Some(fmt_human_double(*d))), proto, out);
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
        Value::Push(items) => {
            out.push(if resp3_sel { b'>' } else { b'*' });
            out.extend_from_slice(items.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for item in items {
                encode_proto(item, proto, out);
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
            push_line(out, s);
            out.extend_from_slice(b"\r\n");
        }
        Value::Error(s) => {
            out.push(b'-');
            push_line(out, s);
            out.extend_from_slice(b"\r\n");
        }
        Value::Integer(i) => {
            out.push(b':');
            out.extend_from_slice(i.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Value::Boolean(b) => out.extend_from_slice(match (resp3_sel, b) {
            (true, true) => b"#t\r\n",
            (true, false) => b"#f\r\n",
            (false, true) => b":1\r\n",
            (false, false) => b":0\r\n",
        }),
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
        // Aggregates. `*`, `~` and `>` carry one element per declared item;
        // `%` carries two (a field and a value), which is the only structural
        // difference between them on the wire.
        b'*' | b'~' | b'%' | b'>' => {
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
                b'>' => Value::Push(items),
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
        // `#t` / `#f`. Decoded as a boolean, not an integer, so the proxy
        // reading a seat in RESP3 can still send a RESP3 client `#t`
        // (BUG-0239): meaning must survive the decode for the re-encode
        // to pick the client's spelling.
        b'#' => {
            let Some(line_end) = find_crlf(&input[1..]) else {
                return Ok(Decoded::NeedMore);
            };
            let v = match &input[1..1 + line_end] {
                b"t" => true,
                b"f" => false,
                _ => return Err(ProtocolError::BadInteger),
            };
            Ok(Decoded::Complete(Value::Boolean(v), 1 + line_end + 2))
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
    /// ZMPOP's pairs decode from RESP3 as a scored result; rebuilt, they are
    /// nested again, which RESP2 keeps, and its null is an array.
    #[test]
    fn zmpop_pairs_stay_nested_and_its_null_is_an_array() {
        let decoded = Value::Array(Some(vec![
            Value::Bulk(Some(b"z".to_vec())),
            Value::ScorePairs(vec![(b"m".to_vec(), 1.5)]),
        ]));
        let args = [b"BZMPOP".to_vec()];
        assert!(zmpop_reply(&args) && !zmpop_reply(&[b"ZPOPMIN".to_vec()]));
        let mut out = Vec::new();
        encode_proto(&zmpop_nested(&decoded), Proto::Resp2, &mut out);
        assert_eq!(
            out,
            b"*2\r\n$1\r\nz\r\n*1\r\n*2\r\n$1\r\nm\r\n$3\r\n1.5\r\n"
        );
        for name in ["LMPOP", "zmpop", "BLMPOP", "BZMPOP"] {
            assert!(null_is_array(&[name.as_bytes().to_vec()]), "{name}");
        }
    }

    /// A coordinate read back from RESP3 is spelled with 17 decimals again,
    /// in both protocols, and GEOPOS's missing member is a null array again.
    #[test]
    fn a_geo_reply_keeps_its_coordinates_and_null_arrays_through_resp3() {
        let seat = b"*2\r\n*2\r\n,13.36138933897018433\r\n,38.11555639549629859\r\n_\r\n";
        let Ok(Decoded::Complete(v, _)) = decode(seat) else {
            panic!("frame did not decode");
        };
        assert!(geo_coordinates(&[b"geopos".to_vec()]) && !geo_coordinates(&[b"GEODIST".to_vec()]));
        let v = geo_reply(&v);
        let mut out = Vec::new();
        encode_proto(&v, Proto::Resp2, &mut out);
        assert_eq!(
            out,
            b"*2\r\n*2\r\n$20\r\n13.36138933897018433\r\n$20\r\n38.11555639549629859\r\n*-1\r\n"
        );
        out.clear();
        encode_proto(&v, Proto::Resp3, &mut out);
        assert_eq!(out, seat);
        assert_eq!(fmt_human_double(-0.0), b"0");
        assert_eq!(fmt_human_double(180.0), b"180");
        assert_eq!(
            fmt_human_double(-85.051_127_512_639_42),
            b"-85.05112751263942528"
        );
    }

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

    /// ADR-0052 D5: a pub/sub message is `>` to a RESP3 client and a plain
    /// array to a RESP2 one, and the decoder reads `>` back as a push.
    #[test]
    fn a_push_is_its_own_type_in_resp3_and_an_array_in_resp2() {
        let m = Value::Push(vec![
            Value::Bulk(Some(b"message".to_vec())),
            Value::Bulk(Some(b"ch".to_vec())),
            Value::Bulk(Some(b"hi".to_vec())),
        ]);
        let three = enc(&m, Proto::Resp3);
        assert_eq!(three, b">3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n");
        assert_eq!(
            enc(&m, Proto::Resp2),
            b"*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n$2\r\nhi\r\n"
        );
        assert_eq!(
            decode(&three).expect("decode"),
            Decoded::Complete(m, three.len())
        );
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
        // BUG-0214: past a small exponent, Redis switches to `1e+20`.
        assert_eq!(enc(&Value::Double(1e20), Proto::Resp2), b"$5\r\n1e+20\r\n");
        assert_eq!(enc(&Value::Double(1e-7), Proto::Resp3), b",1e-7\r\n");
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
        // Booleans decode as themselves (BUG-0239), and each dialect
        // spells one its own way: RESP2 has none and sends an integer.
        assert_eq!(
            decode(b"#t\r\n"),
            Ok(Decoded::Complete(Value::Boolean(true), 4))
        );
        assert_eq!(
            decode(b"#f\r\n"),
            Ok(Decoded::Complete(Value::Boolean(false), 4))
        );
        assert_eq!(enc(&Value::Boolean(true), Proto::Resp3), b"#t\r\n");
        assert_eq!(enc(&Value::Boolean(false), Proto::Resp3), b"#f\r\n");
        assert_eq!(enc(&Value::Boolean(true), Proto::Resp2), b":1\r\n");
        assert_eq!(enc(&Value::Boolean(false), Proto::Resp2), b":0\r\n");
        // A truncated null is NeedMore, not a silent accept.
        assert_eq!(decode(b"_"), Ok(Decoded::NeedMore));
    }

    /// BUG-0184: an error or simple string carrying CR or LF (an echoed
    /// multi-line Lua script) goes out as ONE line, with each replaced by a
    /// space as upstream does, in both protocols.
    #[test]
    fn an_error_is_one_line_whatever_it_echoes() {
        for proto in [Proto::Resp2, Proto::Resp3] {
            let mut out = Vec::new();
            encode_proto(
                &Value::Error(
                    "ERR unknown command 'EVAL', with args beginning with: '\n  local x\r\n' "
                        .into(),
                ),
                proto,
                &mut out,
            );
            assert_eq!(
                out,
                b"-ERR unknown command 'EVAL', with args beginning with: '   local x  ' \r\n"
                    .to_vec()
            );
            let mut ok = Vec::new();
            encode_proto(&Value::Simple("a\nb".into()), proto, &mut ok);
            assert_eq!(ok, b"+a b\r\n".to_vec());
        }
        // The common case is untouched.
        let mut out = Vec::new();
        encode(&Value::Error("ERR plain".into()), &mut out);
        assert_eq!(out, b"-ERR plain\r\n".to_vec());
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
                setname: None,
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
        // BUG-0183: the name is kept, for the proxy's CLIENT GETNAME.
        assert_eq!(
            parse_hello(&a(&["HELLO", "3", "SETNAME", "app"]))
                .expect("setname")
                .setname,
            Some(b"app".to_vec())
        );
        assert_eq!(
            parse_hello(&a(&["HELLO", "3", "AUTH", "u", "p", "SETNAME", "app"]))
                .expect("both")
                .auth,
            Some((b"u".to_vec(), b"p".to_vec()))
        );
        // Versions we do not speak, and junk, stay distinguishable.
        assert_eq!(parse_hello(&a(&["HELLO", "4"])), Err(HelloError::NoProto));
        assert_eq!(
            parse_hello(&a(&["HELLO", "x"])),
            Err(HelloError::NotInteger)
        );
        assert_eq!(
            parse_hello(&a(&["HELLO", "3", "bogus"])),
            Err(HelloError::Syntax("bogus".into()))
        );
        // Redis's and Valkey's words (BUG-0225).
        assert_eq!(
            HelloError::Syntax("bogus".into()).reply(),
            Value::Error("ERR Syntax error in HELLO option 'bogus'".into())
        );
        assert_eq!(
            HelloError::NotInteger.reply(),
            Value::Error("ERR Protocol version is not an integer or out of range".into())
        );
        // A truncated AUTH clause is a syntax error, never a silent
        // "authenticated with an empty password".
        assert_eq!(
            parse_hello(&a(&["HELLO", "3", "AUTH", "u"])),
            Err(HelloError::Syntax("AUTH".into()))
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

    #[test]
    fn doubles_are_spelled_as_redis_spells_them() {
        // BUG-0214. Each pair is what Redis 8.2.8 and Valkey 9.1.0 answered
        // to ZSCORE, measured 2026-10-07.
        let cases: &[(f64, &str)] = &[
            (0.0, "0"),
            (3.0, "3"),
            (-17.0, "-17"),
            (0.1, "0.1"),
            (2.5, "2.5"),
            (1234567.125, "1234567.125"),
            (0.0001, "0.0001"),
            (1e-5, "0.00001"),
            (1e-6, "0.000001"),
            (1e-7, "1e-7"),
            (0.000123456789, "1.23456789e-4"),
            (-2.5e-9, "-2.5e-9"),
            (5e-324, "5e-324"),
            (1e15, "1000000000000000"),
            (123456789012345678.0, "123456789012345680"),
            (4611686018427387904.0, "4611686018427387904"),
            (9.3e18, "9.3e+18"),
            (1e20, "1e+20"),
            (1e22, "1e+22"),
            (1234567890123456789012.0, "1234567890123456800000"),
            (1.5e300, "1.5e+300"),
            (f64::MAX, "1.7976931348623157e+308"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            // BUG-0230: d2string spells the sign of a zero.
            (-0.0, "-0"),
        ];
        for &(d, want) in cases {
            assert_eq!(fmt_double(d), want.as_bytes(), "{d:e}");
        }
    }

    #[test]
    fn fmt_json_double_spells_a_double_as_the_seats_json_does() {
        // BUG-0235. Each spelling is the seat's own (serde_json), read back
        // from JSON.GET on 2026-10-08.
        let cases: &[(f64, &str)] = &[
            (3.0, "3.0"),
            (-0.0, "-0.0"),
            (0.0, "0.0"),
            (1.5, "1.5"),
            (100.0, "100.0"),
            (1e15, "1000000000000000.0"),
            (1e16, "1e+16"),
            (1e17, "1e+17"),
            (1e21, "1e+21"),
            (1e300, "1e+300"),
            (1.7976931348623157e308, "1.7976931348623157e+308"),
            (1.2345678901234566e16, "1.2345678901234566e+16"),
            (123456789.123, "123456789.123"),
            (0.1, "0.1"),
            (0.0001, "0.0001"),
            (0.00001, "0.00001"),
            (0.000123, "0.000123"),
            (1e-7, "1e-7"),
            (1.5e-7, "1.5e-7"),
            (-2.5e-9, "-2.5e-9"),
            (5e-324, "5e-324"),
            // The double nearest this is ...099.25, which ...099.2 and ...099.3
            // name equally well: serde_json and RedisJSON write `.2`, Rust's
            // `{:e}` writes `.3`.
            (900_719_925_474_099.2, "900719925474099.2"),
        ];
        for &(d, want) in cases {
            assert_eq!(fmt_json_double(d), want.as_bytes(), "{d:e}");
        }
        let reply = Value::Array(Some(vec![
            Value::Double(3.0),
            Value::Integer(4),
            Value::Null,
        ]));
        assert_eq!(
            json_numincrby_resp2(&reply, Some(b"$.a")),
            Value::Bulk(Some(b"[3.0,4,null]".to_vec()))
        );
        assert_eq!(
            json_numincrby_resp2(&Value::Array(Some(vec![Value::Double(6.0)])), Some(b".a")),
            Value::Bulk(Some(b"6.0".to_vec()))
        );
        // BUG-0236: the legacy refusal names the path as RedisJSON rewrites it.
        assert_eq!(
            json_numincrby_resp2(&Value::Array(Some(vec![])), Some(b"a")),
            Value::Error("ERR Path '$.a' does not exist or does not contains a number".into())
        );
        assert_eq!(json_fixed_path("[0]"), "$.[0]");
    }
}
