// SPDX-License-Identifier: Elastic-2.0
//! Consumer groups (ADR-0057): XGROUP, XREADGROUP, XACK, XPENDING and XINFO
//! GROUPS and CONSUMERS, with Valkey 9.1's replies and errors, in its order
//! of checking.
//!
//! Creating a group needs the seat's `--stream-groups`
//! (`Limits::stream_groups`). Every other group command works on groups
//! that exist either way, and so does an `XGROUP CREATE` of a group that
//! exists, which answers BUSYGROUP: consumers that create their group when
//! they start, and ignore BUSYGROUP, keep working after a rollback to the
//! release before the one that turns creation on (ADR-0057 D5).

use super::*;
use flint_storage::streams::{
    AutoClaimed, ClaimOpts, Claimed, Created, ENTRIES_READ_UNKNOWN, Found, GroupEntry, GroupFull,
    GroupStart, Pending, PendingSummary, ReadFrom, SetIdRefusal, StreamInfo,
};

const NEEDS_KEY: &str = "ERR The XGROUP subcommand requires the key to exist. Note that for \
     CREATE you may want to use the MKSTREAM option to create an empty stream automatically.";

const GROUPS_OFF: &str =
    "ERR consumer groups are not enabled here yet: this deployment creates none (ADR-0057)";

const XGROUP_HELP: &[&str] = &[
    "XGROUP <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "CREATE <key> <groupname> <id|$> [option]",
    "    Create a new consumer group. Options are:",
    "    * MKSTREAM",
    "      Create the empty stream if it does not exist.",
    "    * ENTRIESREAD entries_read",
    "      Set the group's entries_read counter (internal use).",
    "CREATECONSUMER <key> <groupname> <consumer>",
    "    Create a new consumer in the specified group.",
    "DELCONSUMER <key> <groupname> <consumer>",
    "    Remove the specified consumer.",
    "DESTROY <key> <groupname>",
    "    Remove the specified group.",
    "SETID <key> <groupname> <id|$> [ENTRIESREAD entries_read]",
    "    Set the current group ID and entries_read counter.",
    "HELP",
    "    Print this help.",
];

const XINFO_HELP: &[&str] = &[
    "XINFO <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
    "CONSUMERS <key> <groupname>",
    "    Show consumers of <groupname>.",
    "GROUPS <key>",
    "    Show the stream consumer groups.",
    "STREAM <key> [FULL [COUNT <count>]",
    "    Show information about the stream.",
    "HELP",
    "    Print this help.",
];

fn help(lines: &[&str]) -> Value {
    Value::Array(Some(
        lines.iter().map(|l| Value::Simple((*l).into())).collect(),
    ))
}

fn text(b: &[u8]) -> std::borrow::Cow<'_, str> {
    String::from_utf8_lossy(b)
}

/// XGROUP's, and XINFO CONSUMERS', answer to a missing group.
fn no_group(group: &[u8], key: &[u8]) -> Value {
    err(&format!(
        "NOGROUP No such consumer group '{}' for key name '{}'",
        text(group),
        text(key)
    ))
}

fn bulk(b: impl Into<Vec<u8>>) -> Value {
    Value::Bulk(Some(b.into()))
}

fn id_value(id: StreamId) -> Value {
    bulk(id.to_string())
}

/// A reply of `name value` pairs: a map in RESP3, a flat list in RESP2.
fn pairs(fields: Vec<(&str, Value)>) -> Value {
    Value::ByProto {
        resp2: Box::new(Value::Array(Some(
            fields
                .iter()
                .flat_map(|(k, v)| [bulk(*k), v.clone()])
                .collect(),
        ))),
        resp3: Box::new(Value::Map(
            fields.into_iter().map(|(k, v)| (bulk(k), v)).collect(),
        )),
    }
}

/// XREADGROUP's entries for one stream: a deleted entry is its ID and nil.
fn group_entries_value(entries: Vec<GroupEntry>) -> Value {
    Value::Array(Some(
        entries
            .into_iter()
            .map(|(id, fields)| match fields {
                Some(f) => entry_value((id, f)),
                None => Value::Array(Some(vec![id_value(id), Value::Array(None)])),
            })
            .collect(),
    ))
}

/// An ENTRIESREAD value: -1 (unknown) or more.
fn entries_read_arg(arg: &[u8]) -> Result<i64, Value> {
    let Ok(n) = parse_i64(arg) else {
        return Err(err(NOT_AN_INTEGER));
    };
    if n < ENTRIES_READ_UNKNOWN {
        return Err(err("ERR value for ENTRIESREAD must be positive or -1"));
    }
    Ok(n)
}

impl Dispatcher<'_> {
    /// `XGROUP CREATE | SETID | DESTROY | CREATECONSUMER | DELCONSUMER |
    /// HELP`.
    pub(in crate::commands) fn cmd_xgroup(&self, args: &[Vec<u8>]) -> Value {
        let Some(sub) = args.get(1) else {
            return arity_err("xgroup");
        };
        let upper = sub.to_ascii_uppercase();
        let (name, arity_ok) = match upper.as_slice() {
            b"CREATE" => ("create", args.len() >= 5),
            b"SETID" => ("setid", args.len() >= 5),
            b"DESTROY" => ("destroy", args.len() == 4),
            b"CREATECONSUMER" => ("createconsumer", args.len() == 5),
            b"DELCONSUMER" => ("delconsumer", args.len() == 5),
            b"HELP" => ("help", args.len() == 2),
            _ => {
                return err(&format!(
                    "ERR unknown subcommand '{}'. Try XGROUP HELP.",
                    text(sub)
                ));
            }
        };
        if !arity_ok {
            return arity_err(&format!("xgroup|{name}"));
        }
        if name == "help" {
            return help(XGROUP_HELP);
        }
        let syntax = || {
            err(&format!(
                "ERR unknown subcommand or wrong number of arguments for '{}'. Try XGROUP HELP.",
                text(sub)
            ))
        };
        // CREATE's and SETID's options come first, before anything is
        // looked up.
        let mut mkstream = false;
        let mut entries_read = ENTRIES_READ_UNKNOWN;
        if name == "create" || name == "setid" {
            let mut i = 5;
            while i < args.len() {
                if name == "create" && args[i].eq_ignore_ascii_case(b"MKSTREAM") {
                    mkstream = true;
                    i += 1;
                } else if args[i].eq_ignore_ascii_case(b"ENTRIESREAD") && i + 1 < args.len() {
                    entries_read = match entries_read_arg(&args[i + 1]) {
                        Ok(n) => n,
                        Err(e) => return e,
                    };
                    i += 2;
                } else {
                    return syntax();
                }
            }
        }
        let (key, group) = (&args[2], &args[3]);
        let slot = slot_for_key(key);
        let meta = match self.streams.read_meta(slot, key) {
            Ok(m) => m,
            Err(e) => return store_err(e),
        };
        if meta.is_none() && !(name == "create" && mkstream) {
            return err(NEEDS_KEY);
        }
        if matches!(name, "setid" | "createconsumer" | "delconsumer") {
            match self.streams.group(slot, key, group) {
                Ok(Found::Yes(_)) => {}
                Ok(_) => return no_group(group, key),
                Err(e) => return store_err(e),
            }
        }
        // Each subcommand's own count of arguments is checked last, as
        // Valkey's dispatch does.
        match name {
            "create" if args.len() <= 8 => {
                let start = if args[4].as_slice() == b"$" {
                    GroupStart::Last
                } else {
                    match full_id(&args[4], 0, true) {
                        Ok(id) => GroupStart::Id(id),
                        Err(e) => return e,
                    }
                };
                // A group that exists answers BUSYGROUP whatever the flag:
                // only a creation waits for it.
                let exists = meta.is_some()
                    && matches!(self.streams.group(slot, key, group), Ok(Found::Yes(_)));
                if !exists {
                    if !self.limits.stream_groups {
                        return err(GROUPS_OFF);
                    }
                    if meta.is_none() && !self.limits.streams {
                        return err(
                            "ERR streams are not enabled here yet: this deployment creates none \
                             (ADR-0052)",
                        );
                    }
                }
                match self
                    .streams
                    .group_create(slot, key, group, start, mkstream, entries_read)
                {
                    Ok(Created::Created) => Value::Simple("OK".into()),
                    Ok(Created::Exists) => err("BUSYGROUP Consumer Group name already exists"),
                    Ok(Created::NoStream) => err(NEEDS_KEY),
                    Err(e) => store_err(e),
                }
            }
            "setid" if args.len() == 5 || args.len() == 7 => {
                let start = if args[4].as_slice() == b"$" {
                    GroupStart::Last
                } else {
                    match full_id(&args[4], 0, false) {
                        Ok(id) => GroupStart::Id(id),
                        Err(e) => return e,
                    }
                };
                match self
                    .streams
                    .group_setid(slot, key, group, start, entries_read)
                {
                    Ok(Found::Yes(())) => Value::Simple("OK".into()),
                    Ok(_) => no_group(group, key),
                    Err(e) => store_err(e),
                }
            }
            "destroy" => match self.streams.group_destroy(slot, key, group) {
                Ok(Found::Yes(gone)) => Value::Integer(gone as i64),
                Ok(_) => err(NEEDS_KEY),
                Err(e) => store_err(e),
            },
            "createconsumer" => match self.streams.consumer_create(slot, key, group, &args[4]) {
                Ok(Found::Yes(made)) => Value::Integer(made as i64),
                Ok(_) => no_group(group, key),
                Err(e) => store_err(e),
            },
            "delconsumer" => match self.streams.consumer_delete(slot, key, group, &args[4]) {
                Ok(Found::Yes(pending)) => Value::Integer(pending as i64),
                Ok(_) => no_group(group, key),
                Err(e) => store_err(e),
            },
            _ => syntax(),
        }
    }

    /// `XREADGROUP GROUP group consumer [COUNT n] [BLOCK ms] [NOACK]
    /// STREAMS key [key ...] id [id ...]`, answered now: a seat never waits
    /// (the proxy does). `poll` is `FLINTXREADGROUP`, the proxy's repeated
    /// attempt for a blocked client, which writes nothing when it finds
    /// nothing (ADR-0057 D4).
    pub(in crate::commands) fn cmd_xreadgroup(&self, args: &[Vec<u8>], poll: bool) -> Value {
        if args.len() < 7 {
            return arity_err("xreadgroup");
        }
        let ReadArgs {
            count,
            group,
            noack,
            keys,
            ids,
        } = match parse_read(args, true) {
            Ok(r) => r,
            Err(e) => return e,
        };
        let Some((group, consumer)) = group else {
            return err("ERR Missing GROUP option for XREADGROUP");
        };
        if !self.whole
            && let Some(refusal) = Self::crossslot(&keys[0], &keys[1..])
        {
            return refusal;
        }
        // Every stream and its group must exist, and every ID parse, before
        // anything is read: Valkey checks them all first, key by key.
        let mut plan = Vec::with_capacity(keys.len());
        for (key, id) in keys.iter().zip(ids) {
            match self.streams.group(slot_for_key(key), key, group) {
                Ok(Found::Yes(_)) => {}
                Ok(_) => {
                    return err(&format!(
                        "NOGROUP No such key '{}' or consumer group '{}' in XREADGROUP with \
                         GROUP option",
                        text(key),
                        text(group)
                    ));
                }
                Err(e) => return store_err(e),
            }
            let from = match id.as_slice() {
                b">" => ReadFrom::New,
                b"$" | b"+" => {
                    let id = text(id);
                    return err(&format!(
                        "ERR The {id} ID is meaningless in the context of XREADGROUP: you want \
                         to read the history of this consumer by specifying a proper ID, or use \
                         the > ID to get new messages. The {id} ID would just return an empty \
                         result set."
                    ));
                }
                _ => match full_id(id, 0, true) {
                    Ok(id) => ReadFrom::After(id),
                    Err(e) => return e,
                },
            };
            plan.push(from);
        }
        let mut found = Vec::new();
        for (key, from) in keys.iter().zip(plan) {
            match self.streams.read_group(
                slot_for_key(key),
                key,
                group,
                consumer,
                from,
                count,
                noack,
                poll,
            ) {
                Ok(Found::Yes(Some(entries))) => {
                    found.push((key.clone(), group_entries_value(entries)));
                }
                Ok(_) => {}
                Err(e) => return store_err(e),
            }
        }
        per_stream(found)
    }

    /// `XACK key group id [id ...]`: how many were pending.
    pub(in crate::commands) fn cmd_xack(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 4 {
            return arity_err("xack");
        }
        let (key, group) = (&args[1], &args[2]);
        let slot = slot_for_key(key);
        // No stream or no group acknowledges nothing, before the IDs are
        // read; an ID that does not parse then refuses them all.
        match self.streams.group(slot, key, group) {
            Ok(Found::Yes(_)) => {}
            Ok(_) => return Value::Integer(0),
            Err(e) => return store_err(e),
        }
        let mut ids = Vec::with_capacity(args.len() - 3);
        for a in &args[3..] {
            match full_id(a, 0, true) {
                Ok(id) => ids.push(id),
                Err(e) => return e,
            }
        }
        match self.streams.ack(slot, key, group, &ids) {
            Ok(Found::Yes(n)) => Value::Integer(n as i64),
            Ok(_) => Value::Integer(0),
            Err(e) => store_err(e),
        }
    }

    /// `XPENDING key group [[IDLE min-idle] start end count [consumer]]`.
    pub(in crate::commands) fn cmd_xpending(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 3 {
            return arity_err("xpending");
        }
        let summary = args.len() == 3;
        if !summary && !(6..=9).contains(&args.len()) {
            return err("ERR syntax error");
        }
        let mut range = None;
        if !summary {
            let mut at = 3;
            let mut min_idle = 0;
            if args[3].eq_ignore_ascii_case(b"IDLE") {
                let Ok(n) = parse_i64(&args[4]) else {
                    return err(NOT_AN_INTEGER);
                };
                min_idle = n;
                if args.len() < 8 {
                    return err("ERR syntax error");
                }
                at += 2;
            }
            let Ok(count) = parse_i64(&args[at + 2]) else {
                return err(NOT_AN_INTEGER);
            };
            let (mut start, start_ex) = match interval_id(&args[at], 0) {
                Ok(b) => b,
                Err(e) => return e,
            };
            if start_ex {
                match start.next() {
                    Some(n) => start = n,
                    None => return err("ERR invalid start ID for the interval"),
                }
            }
            let (mut end, end_ex) = match interval_id(&args[at + 1], u64::MAX) {
                Ok(b) => b,
                Err(e) => return e,
            };
            if end_ex {
                match end.prev() {
                    Some(p) => end = p,
                    None => return err("ERR invalid end ID for the interval"),
                }
            }
            let consumer = args.get(at + 3).map(|c| c.as_slice());
            range = Some((start, end, count.max(0) as u64, consumer, min_idle));
        }
        let (key, group) = (&args[1], &args[2]);
        let slot = slot_for_key(key);
        let no_group = || {
            err(&format!(
                "NOGROUP No such key '{}' or consumer group '{}'",
                text(key),
                text(group)
            ))
        };
        let Some((start, end, count, consumer, min_idle)) = range else {
            return match self.streams.pending_summary(slot, key, group) {
                Ok(Found::Yes(s)) => summary_value(s),
                Ok(_) => no_group(),
                Err(e) => store_err(e),
            };
        };
        match self
            .streams
            .pending_range(slot, key, group, start, end, count, consumer, min_idle)
        {
            Ok(Found::Yes(rows)) => Value::Array(Some(
                rows.into_iter()
                    .map(|(p, idle)| {
                        Value::Array(Some(vec![
                            id_value(p.id),
                            bulk(p.consumer),
                            Value::Integer(idle as i64),
                            Value::Integer(p.deliveries as i64),
                        ]))
                    })
                    .collect(),
            )),
            Ok(_) => no_group(),
            Err(e) => store_err(e),
        }
    }

    /// `XINFO GROUPS key | CONSUMERS key group | HELP`. `STREAM` is part 2
    /// of ADR-0057.
    pub(in crate::commands) fn cmd_xinfo(&self, args: &[Vec<u8>]) -> Value {
        let Some(sub) = args.get(1) else {
            return arity_err("xinfo");
        };
        let upper = sub.to_ascii_uppercase();
        let (name, arity_ok) = match upper.as_slice() {
            b"GROUPS" => ("groups", args.len() == 3),
            b"CONSUMERS" => ("consumers", args.len() == 4),
            b"STREAM" => ("stream", args.len() >= 3),
            b"HELP" => ("help", args.len() == 2),
            _ => {
                return err(&format!(
                    "ERR unknown subcommand '{}'. Try XINFO HELP.",
                    text(sub)
                ));
            }
        };
        if !arity_ok {
            return arity_err(&format!("xinfo|{name}"));
        }
        if name == "help" {
            return help(XINFO_HELP);
        }
        let key = &args[2];
        let slot = slot_for_key(key);
        if name == "stream" {
            return self.xinfo_stream(args, sub);
        }
        match name {
            "groups" => match self.streams.groups(slot, key) {
                Ok(Some(groups)) => Value::Array(Some(
                    groups
                        .into_iter()
                        .map(|(name, g, lag)| {
                            pairs(vec![
                                ("name", bulk(name)),
                                ("consumers", Value::Integer(g.consumers as i64)),
                                ("pending", Value::Integer(g.pending as i64)),
                                ("last-delivered-id", id_value(g.last_id)),
                                (
                                    "entries-read",
                                    if g.entries_read == ENTRIES_READ_UNKNOWN {
                                        Value::Null
                                    } else {
                                        Value::Integer(g.entries_read)
                                    },
                                ),
                                ("lag", lag.map_or(Value::Null, Value::Integer)),
                            ])
                        })
                        .collect(),
                )),
                Ok(None) => err("ERR no such key"),
                Err(e) => store_err(e),
            },
            _ => {
                let group = &args[3];
                let now = (self.clock)();
                match self.streams.group_consumers(slot, key, group) {
                    Ok(Found::Yes(consumers)) => Value::Array(Some(
                        consumers
                            .into_iter()
                            .map(|c| {
                                let idle = (now as i64 - c.seen_ms as i64).max(0);
                                let inactive = c.active_ms.map_or(-1, |a| now as i64 - a as i64);
                                pairs(vec![
                                    ("name", bulk(c.name)),
                                    ("pending", Value::Integer(c.pending as i64)),
                                    ("idle", Value::Integer(idle)),
                                    ("inactive", Value::Integer(inactive)),
                                ])
                            })
                            .collect(),
                    )),
                    Ok(Found::NoStream) => err("ERR no such key"),
                    Ok(Found::NoGroup) => no_group(group, key),
                    Err(e) => store_err(e),
                }
            }
        }
    }

    /// `XINFO STREAM key [FULL [COUNT n]]`. The key is looked up before the
    /// options are read, as Valkey does for every XINFO subcommand.
    fn xinfo_stream(&self, args: &[Vec<u8>], sub: &[u8]) -> Value {
        let key = &args[2];
        let slot = slot_for_key(key);
        match self.streams.read_meta(slot, key) {
            Ok(Some(_)) => {}
            Ok(None) => return err("ERR no such key"),
            Err(e) => return store_err(e),
        }
        let syntax = || {
            err(&format!(
                "ERR unknown subcommand or wrong number of arguments for '{}'. Try XINFO HELP.",
                text(sub)
            ))
        };
        let opts = &args[3..];
        let full = !opts.is_empty();
        let mut count: i64 = 10;
        if full {
            if !(opts.len() == 1 || opts.len() == 3) || !opts[0].eq_ignore_ascii_case(b"FULL") {
                return syntax();
            }
            if opts.len() == 3 {
                if !opts[1].eq_ignore_ascii_case(b"COUNT") {
                    return syntax();
                }
                let Ok(n) = parse_i64(&opts[2]) else {
                    return err(NOT_AN_INTEGER);
                };
                count = n.max(0);
            }
        }
        if !full {
            return match self.streams.stream_info(slot, key) {
                Ok(Some(info)) => {
                    let mut fields = stream_head(&info);
                    fields.push(("groups", Value::Integer(info.groups as i64)));
                    fields.push((
                        "first-entry",
                        info.first.clone().map_or(Value::Null, entry_value),
                    ));
                    fields.push((
                        "last-entry",
                        info.last.clone().map_or(Value::Null, entry_value),
                    ));
                    pairs(fields)
                }
                Ok(None) => err("ERR no such key"),
                Err(e) => store_err(e),
            };
        }
        match self.streams.stream_info_full(slot, key, count as usize) {
            Ok(Some((info, entries, groups))) => {
                let mut fields = stream_head(&info);
                fields.push(("entries", entries_value(entries)));
                fields.push((
                    "groups",
                    Value::Array(Some(groups.into_iter().map(group_full_value).collect())),
                ));
                pairs(fields)
            }
            Ok(None) => err("ERR no such key"),
            Err(e) => store_err(e),
        }
    }

    /// `XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms]
    /// [TIME ms] [RETRYCOUNT n] [FORCE] [JUSTID] [LASTID id]`, read as
    /// Valkey reads it: the group first, then the idle time, then IDs until
    /// one does not parse, then options.
    pub(in crate::commands) fn cmd_xclaim(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 6 {
            return arity_err("xclaim");
        }
        let (key, group, consumer) = (&args[1], &args[2], &args[3]);
        let slot = slot_for_key(key);
        match self.streams.group(slot, key, group) {
            Ok(Found::Yes(_)) => {}
            Ok(_) => return no_key_or_group(key, group),
            Err(e) => return store_err(e),
        }
        let Ok(min_idle) = parse_i64(&args[4]) else {
            return err("ERR Invalid min-idle-time argument for XCLAIM");
        };
        let mut ids = Vec::new();
        let mut j = 5;
        while let Some(Ok(id)) = args.get(j).map(|a| full_id(a, 0, true)) {
            ids.push(id);
            j += 1;
        }
        let now = (self.clock)() as i64;
        let mut opts = ClaimOpts::default();
        let mut delivered = None;
        let mut retry = -1;
        while j < args.len() {
            let more = args.len() - 1 - j;
            let opt = &args[j];
            let number = |a: &[u8], what: &str| {
                parse_i64(a)
                    .map_err(|_| err(&format!("ERR Invalid {what} option argument for XCLAIM")))
            };
            if opt.eq_ignore_ascii_case(b"FORCE") {
                opts.force = true;
            } else if opt.eq_ignore_ascii_case(b"JUSTID") {
                opts.just_id = true;
            } else if opt.eq_ignore_ascii_case(b"IDLE") && more > 0 {
                j += 1;
                match number(&args[j], "IDLE") {
                    Ok(idle) => delivered = Some(now.saturating_sub(idle)),
                    Err(e) => return e,
                }
            } else if opt.eq_ignore_ascii_case(b"TIME") && more > 0 {
                j += 1;
                match number(&args[j], "TIME") {
                    Ok(at) => delivered = Some(at),
                    Err(e) => return e,
                }
            } else if opt.eq_ignore_ascii_case(b"RETRYCOUNT") && more > 0 {
                j += 1;
                match number(&args[j], "RETRYCOUNT") {
                    Ok(n) => retry = n,
                    Err(e) => return e,
                }
            } else if opt.eq_ignore_ascii_case(b"LASTID") && more > 0 {
                j += 1;
                match full_id(&args[j], 0, true) {
                    Ok(id) => opts.last_id = Some(id),
                    Err(e) => return e,
                }
            } else {
                return err(&format!("ERR Unrecognized XCLAIM option '{}'", text(opt)));
            }
            j += 1;
        }
        // A delivery time in the future or before 1970 is now, as Valkey
        // takes a client's clock to be a little off rather than refuse it.
        opts.delivered_ms = delivered.map(|d| if d < 0 || d > now { now } else { d } as u64);
        opts.retry_count = (retry >= 0).then_some(retry as u64);
        match self.streams.claim(
            slot,
            key,
            group,
            consumer,
            &ids,
            min_idle.max(0) as u64,
            opts,
        ) {
            Ok(Found::Yes(claimed)) => claimed_value(claimed),
            Ok(_) => no_key_or_group(key, group),
            Err(e) => store_err(e),
        }
    }

    /// `XAUTOCLAIM key group consumer min-idle-time start [COUNT n]
    /// [JUSTID]`, read as Valkey reads it: every argument before the key.
    pub(in crate::commands) fn cmd_xautoclaim(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 6 {
            return arity_err("xautoclaim");
        }
        let Ok(min_idle) = parse_i64(&args[4]) else {
            return err("ERR Invalid min-idle-time argument for XAUTOCLAIM");
        };
        let (mut start, start_ex) = match args[5].strip_prefix(b"(") {
            Some(rest) => match full_id(rest, 0, true) {
                Ok(id) => (id, true),
                Err(e) => return e,
            },
            None => match full_id(&args[5], 0, true) {
                Ok(id) => (id, false),
                Err(e) => return e,
            },
        };
        if start_ex {
            match start.next() {
                Some(n) => start = n,
                None => return err("ERR invalid start ID for the interval"),
            }
        }
        let mut count: u64 = 100;
        let mut just_id = false;
        let mut j = 6;
        while j < args.len() {
            let more = args.len() - 1 - j;
            if args[j].eq_ignore_ascii_case(b"COUNT") && more > 0 {
                match parse_i64(&args[j + 1]) {
                    Ok(n) if (1..=i64::MAX / 16).contains(&n) => count = n as u64,
                    _ => return err("ERR COUNT must be > 0"),
                }
                j += 1;
            } else if args[j].eq_ignore_ascii_case(b"JUSTID") {
                just_id = true;
            } else {
                return err("ERR syntax error");
            }
            j += 1;
        }
        let (key, group, consumer) = (&args[1], &args[2], &args[3]);
        match self.streams.autoclaim(
            slot_for_key(key),
            key,
            group,
            consumer,
            min_idle.max(0) as u64,
            start,
            count,
            just_id,
        ) {
            Ok(Found::Yes(AutoClaimed {
                next,
                claimed,
                deleted,
            })) => Value::Array(Some(vec![
                id_value(next),
                claimed_value(claimed),
                Value::Array(Some(deleted.into_iter().map(id_value).collect())),
            ])),
            Ok(_) => no_key_or_group(key, group),
            Err(e) => store_err(e),
        }
    }

    /// `XSETID key last-id [ENTRIESADDED n] [MAXDELETEDID id]`: the options
    /// are read before the key, as Valkey reads them. It writes the
    /// metadata row, so WATCH sees it, as Valkey signals it.
    pub(in crate::commands) fn cmd_xsetid(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 3 {
            return arity_err("xsetid");
        }
        let id = match full_id(&args[2], 0, true) {
            Ok(id) => id,
            Err(e) => return e,
        };
        let mut entries_added = None;
        let mut max_deleted = None;
        let mut i = 3;
        while i < args.len() {
            let more = args.len() - 1 - i;
            if args[i].eq_ignore_ascii_case(b"ENTRIESADDED") && more > 0 {
                let Ok(n) = parse_i64(&args[i + 1]) else {
                    return err(NOT_AN_INTEGER);
                };
                if n < 0 {
                    return err("ERR entries_added must be positive");
                }
                entries_added = Some(n as u64);
            } else if args[i].eq_ignore_ascii_case(b"MAXDELETEDID") && more > 0 {
                let d = match full_id(&args[i + 1], 0, true) {
                    Ok(d) => d,
                    Err(e) => return e,
                };
                if id < d {
                    return err(
                        "ERR The ID specified in XSETID is smaller than the provided \
                         max_deleted_entry_id",
                    );
                }
                max_deleted = Some(d);
            } else {
                return err("ERR syntax error");
            }
            i += 2;
        }
        let key = &args[1];
        match self
            .streams
            .set_last_id(slot_for_key(key), key, id, entries_added, max_deleted)
        {
            Ok(Some(Ok(()))) => Value::Simple("OK".into()),
            Ok(Some(Err(SetIdRefusal::BelowMaxDeleted))) => {
                err("ERR The ID specified in XSETID is smaller than current max_deleted_entry_id")
            }
            Ok(Some(Err(SetIdRefusal::BelowTop))) => {
                err("ERR The ID specified in XSETID is smaller than the target stream top item")
            }
            Ok(Some(Err(SetIdRefusal::AddedBelowLength))) => err(
                "ERR The entries_added specified in XSETID is smaller than the target stream \
                 length",
            ),
            Ok(None) => err("ERR no such key"),
            Err(e) => store_err(e),
        }
    }
}

/// XPENDING's summary: the count, the smallest and largest IDs, and each
/// consumer with pending entries and how many (a bulk string, as Valkey
/// spells it).
fn summary_value(s: PendingSummary) -> Value {
    let Some((first, last)) = s.bounds else {
        return Value::Array(Some(vec![
            Value::Integer(0),
            Value::Bulk(None),
            Value::Bulk(None),
            Value::Array(None),
        ]));
    };
    Value::Array(Some(vec![
        Value::Integer(s.count as i64),
        id_value(first),
        id_value(last),
        Value::Array(Some(
            s.consumers
                .into_iter()
                .map(|(name, n)| Value::Array(Some(vec![bulk(name), bulk(n.to_string())])))
                .collect(),
        )),
    ]))
}

/// XCLAIM's, XAUTOCLAIM's and XPENDING's range answer to a missing key or
/// group.
fn no_key_or_group(key: &[u8], group: &[u8]) -> Value {
    err(&format!(
        "NOGROUP No such key '{}' or consumer group '{}'",
        text(key),
        text(group)
    ))
}

/// What XCLAIM and XAUTOCLAIM answer: entries, or IDs under JUSTID.
fn claimed_value(claimed: Vec<Claimed>) -> Value {
    Value::Array(Some(
        claimed
            .into_iter()
            .map(|(id, fields)| match fields {
                Some(f) => entry_value((id, f)),
                None => id_value(id),
            })
            .collect(),
    ))
}

/// XINFO STREAM's fields before its entries.
///
/// `radix-tree-keys` and `radix-tree-nodes` describe Valkey's memory: its
/// entries packed a hundred to a node, and the tree of those nodes. Flint
/// keeps one row per entry and no such tree, so both count its rows. A
/// client that reads them as Valkey's memory reads the wrong number either
/// way; one that only parses the reply finds both fields where it expects
/// them.
fn stream_head(info: &StreamInfo) -> Vec<(&'static str, Value)> {
    let rows = Value::Integer(i64::from(info.meta.base.size));
    vec![
        ("length", Value::Integer(i64::from(info.meta.base.size))),
        ("radix-tree-keys", rows.clone()),
        ("radix-tree-nodes", rows),
        ("last-generated-id", id_value(info.meta.last_id)),
        ("max-deleted-entry-id", id_value(info.meta.max_deleted_id)),
        (
            "entries-added",
            Value::Integer(info.meta.entries_added as i64),
        ),
        ("recorded-first-entry-id", id_value(info.first_id)),
    ]
}

/// A group in XINFO STREAM FULL.
fn group_full_value(g: GroupFull) -> Value {
    let pending = Value::Array(Some(
        g.pending
            .into_iter()
            .map(|p: Pending| {
                Value::Array(Some(vec![
                    id_value(p.id),
                    bulk(p.consumer),
                    Value::Integer(p.delivered_ms as i64),
                    Value::Integer(p.deliveries as i64),
                ]))
            })
            .collect(),
    ));
    let consumers = Value::Array(Some(
        g.consumers
            .into_iter()
            .map(|(c, own)| {
                pairs(vec![
                    ("name", bulk(c.name)),
                    ("seen-time", Value::Integer(c.seen_ms as i64)),
                    (
                        "active-time",
                        Value::Integer(c.active_ms.map_or(-1, |a| a as i64)),
                    ),
                    ("pel-count", Value::Integer(c.pending as i64)),
                    (
                        "pending",
                        Value::Array(Some(
                            own.into_iter()
                                .map(|p| {
                                    Value::Array(Some(vec![
                                        id_value(p.id),
                                        Value::Integer(p.delivered_ms as i64),
                                        Value::Integer(p.deliveries as i64),
                                    ]))
                                })
                                .collect(),
                        )),
                    ),
                ])
            })
            .collect(),
    ));
    pairs(vec![
        ("name", bulk(g.name)),
        ("last-delivered-id", id_value(g.group.last_id)),
        (
            "entries-read",
            if g.group.entries_read == ENTRIES_READ_UNKNOWN {
                Value::Null
            } else {
                Value::Integer(g.group.entries_read)
            },
        ),
        ("lag", g.lag.map_or(Value::Null, Value::Integer)),
        ("pel-count", Value::Integer(g.group.pending as i64)),
        ("pending", pending),
        ("consumers", consumers),
    ])
}
