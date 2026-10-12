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

/// Consumer groups (ADR-0057).
mod groups;

const INVALID_ID: &str = "ERR Invalid stream ID specified as stream command argument";

/// What `~` trims at most per call when no `LIMIT` is given: Valkey's
/// `100 * stream-node-max-entries`, at its default of 100.
const APPROX_TRIM_LIMIT: u64 = 10_000;

/// An entry's ID as stored: what read admission adds to each entry's
/// payload (BUG-0256), since a reply carries the ID of every entry.
const STORED_ID_BYTES: u64 = 16;

/// A pending entry's stored fields (its ID, delivery time and delivery
/// count), for read admission (BUG-0256). The consumer's name is not
/// counted: that would mean reading every pending row to price the read.
const PENDING_ROW_BYTES: u64 = 32;

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

/// XREAD's and XREADGROUP's options, as Valkey's `xreadCommand` reads them.
struct ReadArgs<'a> {
    /// `None` reads everything (`COUNT` 0 or less too).
    count: Option<usize>,
    /// XREADGROUP's group and consumer.
    group: Option<(&'a [u8], &'a [u8])>,
    noack: bool,
    keys: &'a [Vec<u8>],
    ids: &'a [Vec<u8>],
}

/// Read XREAD's or XREADGROUP's options up to `STREAMS`, with Valkey's
/// errors in its order. A seat answers at once, so `BLOCK` is checked and
/// dropped; the proxy does the waiting.
fn parse_read(args: &[Vec<u8>], xreadgroup: bool) -> Result<ReadArgs<'_>, Value> {
    let mut count = None;
    let mut group = None;
    let mut noack = false;
    let mut streams_at = None;
    let mut i = 1;
    while i < args.len() {
        let more = args.len() - 1 - i;
        let o = &args[i];
        if o.eq_ignore_ascii_case(b"BLOCK") && more > 0 {
            match parse_i64(&args[i + 1]) {
                Ok(ms) if ms < 0 => return Err(err("ERR timeout is negative")),
                Ok(_) => {}
                Err(_) => return Err(err("ERR timeout is not an integer or out of range")),
            }
            i += 2;
        } else if o.eq_ignore_ascii_case(b"COUNT") && more > 0 {
            let Ok(n) = parse_i64(&args[i + 1]) else {
                return Err(err(NOT_AN_INTEGER));
            };
            count = (n > 0).then_some(n as usize);
            i += 2;
        } else if o.eq_ignore_ascii_case(b"STREAMS") && more > 0 {
            if more % 2 == 1 {
                let (name, symbol) = if xreadgroup {
                    ("xreadgroup", '>')
                } else {
                    ("xread", '$')
                };
                return Err(err(&format!(
                    "ERR Unbalanced '{name}' list of streams: for each stream key an ID or \
                     '{symbol}' must be specified."
                )));
            }
            streams_at = Some(i + 1);
            break;
        } else if o.eq_ignore_ascii_case(b"GROUP") && more >= 2 {
            if !xreadgroup {
                return Err(err(
                    "ERR The GROUP option is only supported by XREADGROUP. You called XREAD instead.",
                ));
            }
            group = Some((args[i + 1].as_slice(), args[i + 2].as_slice()));
            i += 3;
        } else if o.eq_ignore_ascii_case(b"NOACK") {
            if !xreadgroup {
                return Err(err(
                    "ERR The NOACK option is only supported by XREADGROUP. You called XREAD instead.",
                ));
            }
            noack = true;
            i += 1;
        } else {
            return Err(err("ERR syntax error"));
        }
    }
    let Some(at) = streams_at else {
        return Err(err("ERR syntax error"));
    };
    if xreadgroup && group.is_none() {
        return Err(err("ERR Missing GROUP option for XREADGROUP"));
    }
    let (keys, ids) = args[at..].split_at((args.len() - at) / 2);
    Ok(ReadArgs {
        count,
        group,
        noack,
        keys,
        ids,
    })
}

/// XREAD's and XREADGROUP's reply: the null array when no stream had
/// anything, else a map of key to entries in RESP3 and a list of pairs in
/// RESP2.
fn per_stream(found: Vec<(Vec<u8>, Value)>) -> Value {
    if found.is_empty() {
        return Value::Array(None);
    }
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
    /// What a stream read can build, for read admission (BUG-0060,
    /// BUG-0256), or `None` when it is not one this prices or will refuse
    /// anyway. Entries are costed at the stream's mean stored size plus
    /// their IDs, on how many the read can return: its `COUNT` when it
    /// gives one, the whole stream when it does not. An ID range without
    /// `COUNT` is charged the whole stream, as a score range without
    /// `LIMIT` is charged the whole zset.
    pub(in crate::commands) fn stream_read_bytes(
        &self,
        name_upper: &[u8],
        args: &[Vec<u8>],
    ) -> Option<u64> {
        match name_upper {
            b"XRANGE" | b"XREVRANGE" => {
                let key = args.get(1)?;
                // The last COUNT given wins, as in the command; 0 or less
                // reads nothing.
                let mut count = None;
                let mut j = 4;
                while j < args.len() {
                    if !args[j].eq_ignore_ascii_case(b"COUNT") {
                        return None;
                    }
                    count = Some(parse_i64(args.get(j + 1)?).ok()?.max(0) as u64);
                    j += 2;
                }
                self.entries_bytes(key, count)
            }
            b"XREAD" | b"XREADGROUP" | b"FLINTXREADGROUP" => {
                let r = parse_read(args, name_upper != b"XREAD").ok()?;
                let mut total = 0u64;
                for (key, id) in r.keys.iter().zip(r.ids) {
                    let entries = match id.as_slice() {
                        // XREAD's `$` reads only what arrives later, which
                        // a seat never waits for; `+` reads the last entry.
                        b"$" if r.group.is_none() => Some(0),
                        b"+" if r.group.is_none() => Some(1),
                        _ => r.count.map(|c| c as u64),
                    };
                    total = total.saturating_add(self.entries_bytes(key, entries)?);
                }
                Some(total)
            }
            // XINFO STREAM key FULL [COUNT n]: the first n entries (10 when
            // no COUNT is given, all of them for 0), and each group's first
            // n pending entries twice: in its own list and its consumers'.
            b"XINFO" => {
                if !args.get(1)?.eq_ignore_ascii_case(b"STREAM")
                    || !args.get(3)?.eq_ignore_ascii_case(b"FULL")
                {
                    return None;
                }
                let key = args.get(2)?;
                let count = match args.get(4) {
                    None => 10,
                    Some(w) if w.eq_ignore_ascii_case(b"COUNT") => {
                        parse_i64(args.get(5)?).ok().filter(|n| *n >= 0)? as u64
                    }
                    Some(_) => return None,
                };
                let limit = (count > 0).then_some(count);
                let groups = self.streams.groups(slot_for_key(key), key).ok()??;
                let pending: u64 = groups
                    .iter()
                    .map(|(_, g, _)| limit.map_or(g.pending, |n| n.min(g.pending)))
                    .fold(0, u64::saturating_add);
                let entries = self.entries_bytes(key, limit)?;
                Some(entries.saturating_add(pending.saturating_mul(2 * PENDING_ROW_BYTES)))
            }
            // The extended form, `XPENDING key group [IDLE ms] start end
            // count [consumer]`: up to `count` of the group's pending entries.
            b"XPENDING" => {
                let count_at = if args.get(3)?.eq_ignore_ascii_case(b"IDLE") {
                    7
                } else {
                    5
                };
                let count = parse_i64(args.get(count_at)?).ok()?.max(0) as u64;
                let pending = self.group_pending(args.get(1)?, args.get(2)?)?;
                Some(count.min(pending).saturating_mul(PENDING_ROW_BYTES))
            }
            // `XAUTOCLAIM key group consumer min-idle start [COUNT n]
            // [JUSTID]`: up to n of the group's pending entries, as entries,
            // or as IDs with JUSTID.
            b"XAUTOCLAIM" => {
                let mut count = 100u64;
                let mut justid = false;
                let mut j = 6;
                while j < args.len() {
                    if args[j].eq_ignore_ascii_case(b"COUNT") {
                        count = parse_i64(args.get(j + 1)?).ok().filter(|n| *n > 0)? as u64;
                        j += 2;
                    } else if args[j].eq_ignore_ascii_case(b"JUSTID") {
                        justid = true;
                        j += 1;
                    } else {
                        return None;
                    }
                }
                let key = args.get(1)?;
                let rows = count.min(self.group_pending(key, args.get(2)?)?);
                if justid {
                    Some(rows.saturating_mul(STORED_ID_BYTES))
                } else {
                    self.entries_bytes(key, Some(rows))
                }
            }
            _ => None,
        }
    }

    /// `entries` of the stream at `key` (all of them when `None`) at its
    /// mean stored size, each with its ID: 0 for a missing or empty stream,
    /// `None` for another type, whose read answers WRONGTYPE.
    fn entries_bytes(&self, key: &[u8], entries: Option<u64>) -> Option<u64> {
        let Some(meta) = self.streams.read_meta(slot_for_key(key), key).ok()? else {
            return Some(0);
        };
        let size = u64::from(meta.base.size);
        if size == 0 {
            return Some(0);
        }
        let n = entries.map_or(size, |e| e.min(size));
        Some(n.saturating_mul(meta.base.bytes / size + STORED_ID_BYTES))
    }

    /// How many entries group `group` of the stream at `key` has pending,
    /// or `None` when there is no such group (the command says so).
    fn group_pending(&self, key: &[u8], group: &[u8]) -> Option<u64> {
        match self.streams.group(slot_for_key(key), key, group).ok()? {
            flint_storage::streams::Found::Yes((_, g)) => Some(g.pending),
            _ => None,
        }
    }

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
        let ReadArgs {
            count, keys, ids, ..
        } = match parse_read(args, false) {
            Ok(r) => r,
            Err(e) => return e,
        };
        if !self.whole
            && let Some(refusal) = Self::crossslot(&keys[0], &keys[1..])
        {
            return refusal;
        }
        // Each key's starting point, as Valkey resolves it: a key of
        // another type refuses the whole read, and `$` and `+` read the
        // stream as it stands now.
        let mut after = Vec::with_capacity(keys.len());
        for (key, id) in keys.iter().zip(ids) {
            let slot = slot_for_key(key);
            let meta = match self.streams.read_meta(slot, key) {
                Ok(m) => m,
                Err(e) => return store_err(e),
            };
            let from = match id.as_slice() {
                b"$" => meta.map_or(StreamId::MIN, |m| m.last_id),
                b"+" => meta.map_or(StreamId::MIN, |m| m.last_id.prev().unwrap_or(StreamId::MIN)),
                b">" => {
                    return err(
                        "ERR The > ID can be specified only when calling XREADGROUP using the \
                         GROUP <group> <consumer> option.",
                    );
                }
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
        per_stream(found)
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
