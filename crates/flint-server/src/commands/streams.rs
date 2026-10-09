// SPDX-License-Identifier: Elastic-2.0
//! Streams (ADR-0052 D6): XADD, XLEN, XRANGE, XREVRANGE, XDEL, XTRIM and
//! XREAD, with Valkey 9.1's replies and errors, in its order of checking.
//!
//! A seat answers XREAD without waiting, `BLOCK` or not, as Redis does
//! inside `MULTI`; a client of the proxy waits there (ADR-0052 D4).
//!
//! Creating a stream needs the seat's `--streams` (`Limits::streams`): a
//! release from before streams reads a stream key as no type at all, so
//! the first release that serves them creates them only when an operator
//! says so, and the next turns it on. Every stream command reads an
//! existing stream either way.

use super::*;
use flint_storage::encoding::StreamId;
use flint_storage::streams::{Entry, IdSpec, Trim, TrimTo};

const INVALID_ID: &str = "ERR Invalid stream ID specified as stream command argument";

/// What `~` trims at most per call when no `LIMIT` is given: Valkey's
/// `100 * stream-node-max-entries`, at its default of 100.
const APPROX_TRIM_LIMIT: u64 = 10_000;

/// Redis's `string2ull` for the forms clients send: decimal digits, with
/// leading zeros and a leading `+` allowed.
fn parse_u64(s: &[u8]) -> Option<u64> {
    let digits = s.strip_prefix(b"+").unwrap_or(s);
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(digits).ok()?.parse().ok()
}

/// An ID argument. `missing_seq` stands in for an absent `-seq`; `strict`
/// refuses `-` and `+`. With `star`, `ms-*` parses, as `(ms, None)`.
fn parse_id(
    arg: &[u8],
    missing_seq: u64,
    strict: bool,
    star: bool,
) -> Result<(u64, Option<u64>), Value> {
    let invalid = || err(INVALID_ID);
    if arg.len() > 127 {
        return Err(invalid());
    }
    match arg {
        b"-" | b"+" if strict => return Err(invalid()),
        b"-" => return Ok((0, Some(0))),
        b"+" => return Ok((u64::MAX, Some(u64::MAX))),
        _ => {}
    }
    let (ms, seq) = match arg.iter().position(|&b| b == b'-') {
        Some(dash) => (&arg[..dash], Some(&arg[dash + 1..])),
        None => (arg, None),
    };
    let ms = parse_u64(ms).ok_or_else(invalid)?;
    let seq = match seq {
        None => Some(missing_seq),
        Some(b"*") if star => None,
        Some(s) => Some(parse_u64(s).ok_or_else(invalid)?),
    };
    Ok((ms, seq))
}

fn full_id(arg: &[u8], missing_seq: u64, strict: bool) -> Result<StreamId, Value> {
    let (ms, seq) = parse_id(arg, missing_seq, strict, false)?;
    Ok(StreamId {
        ms,
        seq: seq.unwrap_or(missing_seq),
    })
}

/// An interval bound of XRANGE: `(` makes it exclusive.
fn interval_id(arg: &[u8], missing_seq: u64) -> Result<(StreamId, bool), Value> {
    match arg.strip_prefix(b"(") {
        Some(rest) => Ok((full_id(rest, missing_seq, true)?, true)),
        None => Ok((full_id(arg, missing_seq, false)?, false)),
    }
}

/// XADD's and XTRIM's options, read as Valkey reads them
/// (`streamParseAddOrTrimArgsOrReply`).
struct AddArgs {
    nomkstream: bool,
    trim: Option<Trim>,
    /// XADD's ID, and where its fields start.
    id: Option<(IdSpec, usize)>,
}

fn parse_add_or_trim(args: &[Vec<u8>], xadd: bool) -> Result<AddArgs, Value> {
    let mut out = AddArgs {
        nomkstream: false,
        trim: None,
        id: None,
    };
    let mut approx = false;
    let mut limit: Option<i64> = None;
    let mut i = 2;
    while i < args.len() {
        let more = args.len() - 1 - i;
        let opt = &args[i];
        if xadd && opt.as_slice() == b"*" {
            out.id = Some((IdSpec::Auto, i + 1));
            break;
        } else if (opt.eq_ignore_ascii_case(b"MAXLEN") || opt.eq_ignore_ascii_case(b"MINID"))
            && more > 0
        {
            if out.trim.is_some() {
                return Err(err(
                    "ERR syntax error, MAXLEN and MINID options at the same time are not compatible",
                ));
            }
            approx = false;
            if more >= 2 && args[i + 1].as_slice() == b"~" {
                approx = true;
                i += 1;
            } else if more >= 2 && args[i + 1].as_slice() == b"=" {
                i += 1;
            }
            let to = if opt.eq_ignore_ascii_case(b"MAXLEN") {
                let n = parse_i64(&args[i + 1]).map_err(|_| err(NOT_AN_INTEGER))?;
                if n < 0 {
                    return Err(err("ERR The MAXLEN argument must be >= 0."));
                }
                TrimTo::MaxLen(n as u64)
            } else {
                TrimTo::MinId(full_id(&args[i + 1], 0, true)?)
            };
            out.trim = Some(Trim { to, limit: None });
            i += 2;
            continue;
        } else if opt.eq_ignore_ascii_case(b"LIMIT") && more > 0 {
            let n = parse_i64(&args[i + 1]).map_err(|_| err(NOT_AN_INTEGER))?;
            if n < 0 {
                return Err(err("ERR The LIMIT argument must be >= 0."));
            }
            limit = Some(n);
            i += 2;
            continue;
        } else if xadd && opt.eq_ignore_ascii_case(b"NOMKSTREAM") {
            out.nomkstream = true;
        } else if xadd {
            let (ms, seq) = parse_id(opt, 0, true, true)?;
            let spec = match seq {
                None => IdSpec::AutoSeq(ms),
                Some(seq) => IdSpec::Explicit(StreamId { ms, seq }),
            };
            out.id = Some((spec, i + 1));
            break;
        } else {
            return Err(err("ERR syntax error"));
        }
        i += 1;
    }
    if limit.is_some_and(|n| n != 0) && out.trim.is_none() {
        return Err(err(
            "ERR syntax error, LIMIT cannot be used without specifying a trimming strategy",
        ));
    }
    if !xadd && out.trim.is_none() {
        return Err(err(
            "ERR syntax error, XTRIM must be called with a trimming strategy",
        ));
    }
    if let Some(t) = out.trim.as_mut() {
        t.limit = match (approx, limit) {
            // `~` trims exactly here, up to LIMIT, where Valkey trims whole
            // internal nodes and so keeps up to a node's worth more.
            (true, None) => Some(APPROX_TRIM_LIMIT),
            (true, Some(0)) => None,
            (true, Some(n)) => Some(n as u64),
            (false, None) => None,
            (false, Some(_)) => {
                return Err(err(
                    "ERR syntax error, LIMIT cannot be used without the special ~ option",
                ));
            }
        };
    }
    Ok(out)
}

/// `[id, [field, value, ...]]`, in both protocols.
fn entry_value((id, fields): Entry) -> Value {
    Value::Array(Some(vec![
        Value::Bulk(Some(id.to_string().into_bytes())),
        Value::Array(Some(
            fields.into_iter().map(|f| Value::Bulk(Some(f))).collect(),
        )),
    ]))
}

fn entries_value(entries: Vec<Entry>) -> Value {
    Value::Array(Some(entries.into_iter().map(entry_value).collect()))
}

impl Dispatcher<'_> {
    /// `XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT n]]
    /// <*|id> field value [field value ...]`.
    pub(super) fn cmd_xadd(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 5 {
            return arity_err("xadd");
        }
        let parsed = match parse_add_or_trim(args, true) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let Some((spec, at)) = parsed.id else {
            return arity_err("xadd");
        };
        let fields = &args[at.min(args.len())..];
        if fields.len() < 2 || fields.len() % 2 == 1 {
            return arity_err("xadd");
        }
        if spec == IdSpec::Explicit(StreamId::MIN) {
            return err("ERR The ID specified in XADD must be greater than 0-0");
        }
        let key = &args[1];
        let slot = slot_for_key(key);
        if !self.limits.streams {
            // Appending to a stream that exists is safe on any release
            // that has one; creating the first is what waits for the flag.
            match self.streams.read_meta(slot, key) {
                Ok(Some(_)) => {}
                Ok(None) if parsed.nomkstream => return Value::Bulk(None),
                Ok(None) => {
                    return err(
                        "ERR streams are not enabled here yet: this deployment creates none \
                         (ADR-0052)",
                    );
                }
                Err(e) => return store_err(e),
            }
        }
        match self
            .streams
            .add(slot, key, spec, fields, parsed.nomkstream, parsed.trim)
        {
            Ok(Some(id)) => Value::Bulk(Some(id.to_string().into_bytes())),
            Ok(None) => Value::Bulk(None),
            Err(e) => store_err(e),
        }
    }

    /// `XTRIM key MAXLEN|MINID [=|~] threshold [LIMIT n]`.
    pub(super) fn cmd_xtrim(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 4 {
            return arity_err("xtrim");
        }
        let parsed = match parse_add_or_trim(args, false) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let Some(trim) = parsed.trim else {
            return err("ERR syntax error");
        };
        reply(
            self.streams.trim(slot_for_key(&args[1]), &args[1], trim),
            |n| Value::Integer(n as i64),
        )
    }

    /// `XRANGE key start end [COUNT n]`, and XREVRANGE with `end` first.
    pub(super) fn cmd_xrange(&self, args: &[Vec<u8>], rev: bool) -> Value {
        let name = if rev { "xrevrange" } else { "xrange" };
        if args.len() < 4 {
            return arity_err(name);
        }
        let (lo_arg, hi_arg) = if rev {
            (&args[3], &args[2])
        } else {
            (&args[2], &args[3])
        };
        let (mut lo, lo_ex) = match interval_id(lo_arg, 0) {
            Ok(b) => b,
            Err(e) => return e,
        };
        if lo_ex {
            match lo.next() {
                Some(n) => lo = n,
                None => return err("ERR invalid start ID for the interval"),
            }
        }
        let (mut hi, hi_ex) = match interval_id(hi_arg, u64::MAX) {
            Ok(b) => b,
            Err(e) => return e,
        };
        if hi_ex {
            match hi.prev() {
                Some(p) => hi = p,
                None => return err("ERR invalid end ID for the interval"),
            }
        }
        let mut count: Option<usize> = None;
        let mut j = 4;
        while j < args.len() {
            if args[j].eq_ignore_ascii_case(b"COUNT") && j + 1 < args.len() {
                let Ok(n) = parse_i64(&args[j + 1]) else {
                    return err(NOT_AN_INTEGER);
                };
                count = Some(n.max(0) as usize);
                j += 2;
            } else {
                return err("ERR syntax error");
            }
        }
        if count == Some(0) {
            // Valkey looks the key up before it reads COUNT 0: a missing key
            // answers an empty array and another type WRONGTYPE; only a
            // stream, an empty one too, answers the null array.
            return reply(
                self.streams.read_meta(slot_for_key(&args[1]), &args[1]),
                |m| match m {
                    Some(_) => Value::Array(None),
                    None => Value::Array(Some(Vec::new())),
                },
            );
        }
        reply(
            self.streams
                .range(slot_for_key(&args[1]), &args[1], lo, hi, count, rev),
            entries_value,
        )
    }

    /// `XDEL key id [id ...]`.
    pub(super) fn cmd_xdel(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 3 {
            return arity_err("xdel");
        }
        let mut ids = Vec::with_capacity(args.len() - 2);
        for a in &args[2..] {
            match full_id(a, 0, true) {
                Ok(id) => ids.push(id),
                Err(e) => return e,
            }
        }
        reply(
            self.streams.del(slot_for_key(&args[1]), &args[1], &ids),
            |n| Value::Integer(n as i64),
        )
    }

    /// `XREAD [COUNT n] [BLOCK ms] STREAMS key [key ...] id [id ...]`,
    /// answered now: a seat never waits (the proxy does).
    pub(super) fn cmd_xread(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 4 {
            return arity_err("xread");
        }
        let mut count: Option<usize> = None;
        let mut streams_at = None;
        let mut i = 1;
        while i < args.len() {
            let more = args.len() - 1 - i;
            let o = &args[i];
            if o.eq_ignore_ascii_case(b"BLOCK") && more > 0 {
                match parse_i64(&args[i + 1]) {
                    Ok(ms) if ms < 0 => return err("ERR timeout is negative"),
                    Ok(_) => {}
                    Err(_) => return err("ERR timeout is not an integer or out of range"),
                }
                i += 2;
            } else if o.eq_ignore_ascii_case(b"COUNT") && more > 0 {
                let Ok(n) = parse_i64(&args[i + 1]) else {
                    return err(NOT_AN_INTEGER);
                };
                count = (n > 0).then_some(n as usize);
                i += 2;
            } else if o.eq_ignore_ascii_case(b"STREAMS") && more > 0 {
                if more % 2 == 1 {
                    return err(
                        "ERR Unbalanced 'xread' list of streams: for each stream key an ID or \
                         '$' must be specified.",
                    );
                }
                streams_at = Some(i + 1);
                break;
            } else {
                return err("ERR syntax error");
            }
        }
        let Some(at) = streams_at else {
            return err("ERR syntax error");
        };
        let n = (args.len() - at) / 2;
        let (keys, ids) = args[at..].split_at(n);
        if !self.whole
            && let Some(refusal) = Self::crossslot(&keys[0], &keys[1..])
        {
            return refusal;
        }
        // Each key's starting point, as Valkey resolves it: a key of
        // another type refuses the whole read, and `$` and `+` read the
        // stream as it stands now.
        let mut after = Vec::with_capacity(n);
        for (key, id) in keys.iter().zip(ids) {
            let slot = slot_for_key(key);
            let meta = match self.streams.read_meta(slot, key) {
                Ok(m) => m,
                Err(e) => return store_err(e),
            };
            let from = match id.as_slice() {
                b"$" => meta.map_or(StreamId::MIN, |m| m.last_id),
                b"+" => meta.map_or(StreamId::MIN, |m| m.last_id.prev().unwrap_or(StreamId::MIN)),
                _ => match full_id(id, 0, true) {
                    Ok(id) => id,
                    Err(e) => return e,
                },
            };
            after.push(from);
        }
        let mut found = Vec::new();
        for (key, from) in keys.iter().zip(after) {
            let Some(lo) = from.next() else {
                continue;
            };
            match self
                .streams
                .range(slot_for_key(key), key, lo, StreamId::MAX, count, false)
            {
                Ok(e) if e.is_empty() => {}
                Ok(e) => found.push((key.clone(), entries_value(e))),
                Err(e) => return store_err(e),
            }
        }
        if found.is_empty() {
            return Value::Array(None);
        }
        // RESP3 answers a map of key to entries, RESP2 a list of pairs.
        Value::ByProto {
            resp2: Box::new(Value::Array(Some(
                found
                    .iter()
                    .map(|(k, e)| Value::Array(Some(vec![Value::Bulk(Some(k.clone())), e.clone()])))
                    .collect(),
            ))),
            resp3: Box::new(Value::Map(
                found
                    .into_iter()
                    .map(|(k, e)| (Value::Bulk(Some(k)), e))
                    .collect(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_parse_as_valkey_reads_them() {
        assert_eq!(parse_id(b"1-2", 0, true, false), Ok((1, Some(2))));
        assert_eq!(parse_id(b"7", 0, true, false), Ok((7, Some(0))));
        assert_eq!(
            parse_id(b"7", u64::MAX, false, false),
            Ok((7, Some(u64::MAX)))
        );
        assert_eq!(parse_id(b"5-*", 0, true, true), Ok((5, None)));
        assert!(parse_id(b"5-*", 0, true, false).is_err());
        assert!(parse_id(b"-", 0, true, false).is_err());
        assert_eq!(parse_id(b"-", 0, false, false), Ok((0, Some(0))));
        assert_eq!(
            parse_id(b"+", 0, false, false),
            Ok((u64::MAX, Some(u64::MAX)))
        );
        assert_eq!(
            parse_id(b"18446744073709551615-18446744073709551615", 0, true, false),
            Ok((u64::MAX, Some(u64::MAX)))
        );
        for bad in [
            &b"abc"[..],
            b"1-x",
            b"",
            b"1-",
            b"-1",
            b"1-2-3",
            b"18446744073709551616",
        ] {
            assert!(parse_id(bad, 0, true, false).is_err(), "{bad:?}");
        }
        assert_eq!(parse_id(b"007-01", 0, true, false), Ok((7, Some(1))));
    }
}
