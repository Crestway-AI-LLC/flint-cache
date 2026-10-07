// SPDX-License-Identifier: Elastic-2.0
//! Command dispatch: `Vec<arg-bytes>` in, RESP `Value` out.
//!
//! Commands route through the encoding layer with the slot computed per
//! key — the same data path the distributed system will use. Generic
//! keyspace commands (DEL/EXISTS/TYPE/EXPIRE/TTL/PERSIST) are
//! type-agnostic; typed commands return WRONGTYPE per Redis. The
//! conformance oracle is the referee for every reply shape.

use flint_resp::Value;
use flint_slot::slot_for_key;
use flint_storage::Kv;
use flint_storage::bloom::BloomStore;
use flint_storage::hashes::HashStore;
use flint_storage::json::JsonStore;
use flint_storage::keyspace::{Keyspace, RenameOutcome, Ttl};
use flint_storage::lists::{ListStore, LsetOutcome};
use flint_storage::sets::SetStore;
use flint_storage::strings::{
    BitfieldKind, BitfieldOp, BitfieldOverflow, Clock, SetExpiry, SetOptions, SetOutcome,
    StoreError, StringStore, parse_redis_i64,
};
use flint_storage::zsets::{LexBound, ScoreBound, ZSetStore, ZaddFlags, ZsetRows};

/// The JSON commands ADR-0055 added.
mod json;

/// True for commands that mutate the keyspace (rejected on replicas).
/// Delegates to the SHARED classifier (flint-commands, ADR-0005 D1): the
/// server's -READONLY gate and slot gate must classify identically to the
/// proxy's traffic split and future replica-read routing — one table, no
/// drift.
pub fn is_write_command(name: &[u8]) -> bool {
    flint_commands::is_write_command(name)
}

/// The single key a command addresses (its slot-determining key), or None
/// for commands that don't target one key. v0 commands all place their key at
/// `args[1]`; FLINT* admin/replication commands are intercepted before this, so
/// only the no-key data/util commands need excluding. Used to check per-slot
/// ownership and answer -MOVED after a migration (rocks builds only).
#[cfg_attr(not(feature = "rocks"), allow(dead_code))]
pub fn command_key(args: &[Vec<u8>]) -> Option<&[u8]> {
    let name = args.first()?;
    const NO_KEY: &[&[u8]] = &[
        b"PING",
        b"ECHO",
        b"DBSIZE",
        b"FLUSHALL",
        b"FLUSHDB",
        b"COMMAND",
        b"CLUSTER",
        b"INFO",
        b"SELECT",
        b"QUIT",
        b"HELLO",
        b"SCRIPT",
        b"KEYS",
        b"TIME",
    ];
    if NO_KEY.iter().any(|c| name.eq_ignore_ascii_case(c)) {
        return None;
    }
    // A script's key is KEYS[1], not the script (ADR-0050, ADR-0051): it is
    // what the write lock, the slot owner and a transaction's slot are taken
    // from. Every key a script declares shares its slot, or it is refused.
    if name.eq_ignore_ascii_case(b"EVAL") || name.eq_ignore_ascii_case(b"EVALSHA") {
        return flint_commands::eval_keys(args)
            .and_then(|k| k.first())
            .map(|k| k.as_slice());
    }
    if let Some(key) = flint_commands::json_debug_key(args) {
        return key;
    }
    args.get(1).map(|k| k.as_slice())
}

/// The default namespace: unauthenticated/direct connections and every
/// pre-tenancy tool operate here. Tenant connections select their own via
/// FLINTNS (set by the proxy after token auth).
pub const DEFAULT_NS: &[u8] = b"0";

/// The error a command would fail with at QUEUE time inside MULTI, or None
/// if it would be accepted (ADR-0012 D1).
///
/// Redis distinguishes two kinds of failure inside a transaction, and the
/// distinction is not cosmetic. An unknown command or a wrong argument
/// count is caught when the command is queued and POISONS the transaction,
/// so EXEC applies nothing. A runtime failure — WRONGTYPE, a bad float —
/// is discovered only at execution and appears as one element of EXEC's
/// reply while every other command still applies. Collapsing the first into
/// the second would partially apply a transaction the client was told would
/// abort, which is the failure worth preventing.
///
/// WHY THIS PROBE-DISPATCHES RATHER THAN CONSULTING AN ARITY TABLE. Arity
/// depends only on the command name and the argument COUNT — never on
/// stored data — and every dispatch arm validates it before touching the
/// store. So the verdict against a throwaway empty store is exactly the
/// verdict against the real one, and reusing the dispatcher means there is
/// no second table of ~90 arities to drift out of step the first time an
/// arm changes. The probe cannot affect anything: its store is discarded.
///
/// A command's own CROSSSLOT refusal is the same kind of verdict (BUG-0181).
/// Whether MSET, MGET, the set operations, ZUNIONSTORE, COPY or RENAME span
/// slots depends on their keys alone, and every arm checks it before reading
/// the store. It used to be left to EXEC, where it is a runtime error: the
/// transaction was queued on the command's FIRST key, the command failed as
/// one element of EXEC's reply, and everything queued around it applied,
/// although `command-support.md` promises a cross-slot key poisons the
/// transaction. `DEL`, `UNLINK` and `EXISTS` check no slot of their own; the
/// queue step walks their keys itself (BUG-0179).
pub fn queue_time_error(args: &[Vec<u8>], whole: bool) -> Option<Value> {
    // A script is checked, never run: probing it would run tenant code at
    // queue time. Its arity and its keys' slots are what a queued command's
    // verdict rests on, as for any other (ADR-0051). A placed tenant's
    // script may span slots (`whole`, ADR-0053); every other command keeps
    // its own slot rule, which its storage needs.
    if let Some(name) = args.first()
        && (name.eq_ignore_ascii_case(b"EVAL") || name.eq_ignore_ascii_case(b"EVALSHA"))
    {
        if args.len() < 3 {
            return Some(arity_err(
                &String::from_utf8_lossy(name).to_ascii_lowercase(),
            ));
        }
        let keys = flint_commands::eval_keys(args)?;
        if whole {
            return None;
        }
        return Dispatcher::crossslot(keys.first()?, &keys[1..]);
    }
    let probe = flint_storage::MemKv::new();
    let reply = Dispatcher::new(&probe, crate::commands::probe_clock).dispatch(args);
    match &reply {
        // Both texts are produced in THIS file — `arity_err` and the
        // dispatcher's unknown-command arm — so matching them is matching
        // our own output, not parsing someone else's.
        Value::Error(e)
            if e.starts_with("ERR unknown command")
                || e.starts_with("ERR wrong number of arguments")
                || e.starts_with("CROSSSLOT") =>
        {
            Some(reply)
        }
        _ => None,
    }
}

/// A fixed clock for the queue-time probe. The probe never reads or writes
/// anything that outlives it, and pinning time keeps it from depending on
/// the wall clock at all.
fn probe_clock() -> u64 {
    0
}

/// Upstream's unknown-command reply, byte for byte — name in the case the
/// client SENT it, then each argument quoted and space-separated with a
/// trailing space. Captured off the wire from a live server rather than
/// recalled, because the punctuation is not what one would guess.
///
/// Worth matching rather than approximating: this is the reply a client
/// sees when it uses a command Flint has not implemented, so it is the
/// error most likely to be read by a human comparing the two systems, and
/// inside MULTI it is what poisons a transaction.
///
/// Arguments are truncated so an unknown command carrying a large payload
/// cannot turn a typo into a multi-megabyte error string.
fn unknown_command(name: &[u8], rest: &[Vec<u8>]) -> Value {
    const MAX_ARG: usize = 128;
    const MAX_ARGS: usize = 20;
    let mut msg = format!(
        "ERR unknown command '{}', with args beginning with: ",
        String::from_utf8_lossy(name)
    );
    for arg in rest.iter().take(MAX_ARGS) {
        let shown = &arg[..arg.len().min(MAX_ARG)];
        msg.push_str(&format!("'{}' ", String::from_utf8_lossy(shown)));
    }
    Value::Error(msg)
}

/// Wire-facing policy limits, plumbed from the CLI.
#[derive(Clone, Copy)]
pub struct Limits {
    /// Cap on any single value's total payload; 0 disables.
    pub max_value_bytes: u64,
    /// Cap on user-key length. Always clamped to the envelope's
    /// structural ceiling (`flint_storage::MAX_KEY_BYTES`); 0 means
    /// "ceiling only".
    pub max_key_bytes: u64,
    /// A Lua script's time and memory (ADR-0051).
    pub script: crate::script::ScriptLimits,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_value_bytes: flint_storage::DEFAULT_MAX_VALUE_BYTES,
            max_key_bytes: flint_storage::DEFAULT_MAX_KEY_BYTES,
            script: crate::script::ScriptLimits::default(),
        }
    }
}

impl Limits {
    fn effective_max_key(&self) -> u64 {
        if self.max_key_bytes == 0 {
            flint_storage::MAX_KEY_BYTES
        } else {
            self.max_key_bytes.min(flint_storage::MAX_KEY_BYTES)
        }
    }
}

pub struct Dispatcher<'a> {
    keyspace: Keyspace<'a>,
    strings: StringStore<'a>,
    hashes: HashStore<'a>,
    sets: SetStore<'a>,
    lists: ListStore<'a>,
    zsets: ZSetStore<'a>,
    json: JsonStore<'a>,
    bloom: BloomStore<'a>,
    kv: &'a dyn Kv,
    clock: Clock,
    limits: Limits,
    ns: Vec<u8>,
    /// Whether the caller holds the write lock over every writer, so a
    /// script may touch keys in its slot that it did not declare (ADR-0052).
    every_writer: bool,
    /// Set when a script was abandoned because it reached such a key while
    /// the caller held only its declared keys' locks.
    wants_every_writer: std::cell::Cell<bool>,
    /// The connection's namespace lives wholly on this seat's pair
    /// (`FLINTWHOLE`, ADR-0053): a script may name and touch keys in any
    /// slot, not only its KEYS' one.
    whole: bool,
}

impl<'a> Dispatcher<'a> {
    /// Default policy limits + default namespace. The server binary always
    /// goes through `with_limits`; this is the test-and-embedding
    /// convenience.
    #[allow(dead_code)]
    pub fn new(kv: &'a dyn Kv, clock: Clock) -> Self {
        Self::with_limits(kv, clock, Limits::default(), DEFAULT_NS)
    }

    /// Namespace-scoped dispatcher: every data command, DBSIZE, and
    /// FLUSHALL operate on `ns` only — the tenant-isolation boundary.
    pub fn with_limits(kv: &'a dyn Kv, clock: Clock, limits: Limits, ns: &[u8]) -> Self {
        let max = limits.max_value_bytes;
        Self {
            keyspace: Keyspace::new(kv, ns, clock),
            strings: StringStore::with_max_value_bytes(kv, ns, clock, max),
            hashes: HashStore::with_max_value_bytes(kv, ns, clock, max),
            sets: SetStore::with_max_value_bytes(kv, ns, clock, max),
            lists: ListStore::with_max_value_bytes(kv, ns, clock, max),
            zsets: ZSetStore::with_max_value_bytes(kv, ns, clock, max),
            json: JsonStore::with_max_value_bytes(kv, ns, clock, max),
            bloom: BloomStore::with_max_value_bytes(kv, ns, clock, max),
            kv,
            clock,
            limits,
            ns: ns.to_vec(),
            every_writer: false,
            wants_every_writer: std::cell::Cell::new(false),
            whole: false,
        }
    }

    /// Tell the dispatcher that its connection is a placed tenant's
    /// (`FLINTWHOLE`, ADR-0053).
    pub fn whole(mut self, yes: bool) -> Self {
        self.whole = yes;
        self
    }

    /// Tell the dispatcher that its caller holds the lock over every writer
    /// (`write_lock::lock_all`), as a transaction always does.
    pub fn holding_every_writer(mut self, yes: bool) -> Self {
        self.every_writer = yes;
        self
    }

    /// True when a script this dispatcher ran must run again holding the
    /// lock over every writer: it reached a key outside its `KEYS`, in their
    /// slot (ADR-0052). Its reply was an error and nothing it wrote reached
    /// the store; the caller discards that reply and runs the command again
    /// under `lock_all`, with `holding_every_writer(true)`.
    pub fn wants_every_writer(&self) -> bool {
        self.wants_every_writer.get()
    }

    /// `cf | ns_len | ns` — the prefix bounding this namespace's rows in one
    /// CF. DBSIZE and FLUSHALL must scan/delete inside it, never CF-wide:
    /// other tenants' rows share the physical keyspace.
    fn ns_prefix(&self, cf: flint_storage::encoding::Cf) -> Vec<u8> {
        let mut p = Vec::with_capacity(2 + self.ns.len());
        p.push(cf as u8);
        p.push(self.ns.len() as u8);
        p.extend_from_slice(&self.ns);
        p
    }

    /// True when any key argument of this command exceeds the key cap.
    /// Key positions mirror `command_key`: v0 commands take their key at
    /// `args[1]`; DEL/EXISTS are all-keys; MSET keys sit at odd indices.
    /// Enforced for reads and writes alike — an oversized key must never
    /// reach the envelope builders (their length frame is 2 bytes).
    fn has_oversized_key(&self, name_upper: &[u8], args: &[Vec<u8>]) -> bool {
        let max = self.limits.effective_max_key() as usize;
        match name_upper {
            b"PING" | b"ECHO" | b"DBSIZE" | b"FLUSHALL" | b"FLUSHDB" | b"COMMAND" | b"CLUSTER"
            | b"INFO" | b"SELECT" | b"QUIT" | b"HELLO" | b"SCAN" => false,
            b"DEL" | b"EXISTS" => args[1..].iter().any(|k| k.len() > max),
            b"MSET" => args[1..].iter().step_by(2).any(|k| k.len() > max),
            // ADR-0055: JSON.MGET's last argument is its path, JSON.MSET's
            // keys lead each triple, and JSON.DEBUG's key follows its
            // subcommand.
            b"JSON.MGET" => args[1..args.len().saturating_sub(1).max(1)]
                .iter()
                .any(|k| k.len() > max),
            b"JSON.MSET" => args[1..].iter().step_by(3).any(|k| k.len() > max),
            b"JSON.DEBUG" => flint_commands::json_debug_key(args)
                .flatten()
                .is_some_and(|k| k.len() > max),
            // BUG-0189: `args[1]` is the script's text (or its SHA1), not a
            // key, and a text past the cap was refused as one: BullMQ's
            // scripts are larger than 4 KiB. The declared keys are checked
            // here; a key a script builds is checked when its call dispatches.
            b"EVAL" | b"EVALSHA" => {
                flint_commands::eval_keys(args).is_some_and(|ks| ks.iter().any(|k| k.len() > max))
            }
            b"SCRIPT" => false,
            _ => args.get(1).is_some_and(|k| k.len() > max),
        }
    }

    /// The collection this command will materialise, in bytes, from ONE
    /// metadata read -- the input to BUG-0060's admission. `None` means "not a
    /// whole-collection read", which is the answer for most commands and for a
    /// key that does not exist.
    ///
    /// Membership is decided by whether the command materialises the WHOLE
    /// collection, not by whether it looks like a range:
    ///
    /// - `ZRANGE` and friends are IN, costed on what they can return: a rank
    ///   window its length, a LIMIT its count, and a score or lex range
    ///   without one the whole set. They were charged the whole set while
    ///   `ZSetStore` built the entire ordered set and sliced it; since
    ///   BUG-0216 a read stops where it is told to. ZPOPMIN and ZPOPMAX are
    ///   IN for their count, the same way.
    /// - `LRANGE` is IN, but costed against the REQUESTED SLICE rather than the
    ///   key: `ListStore::lrange` reads only the ranks asked for, so charging
    ///   it the whole list would refuse `LRANGE key 0 0` on a large one. It was
    ///   left out entirely at first for that reason, and that left
    ///   `LRANGE key 0 -1` unbounded — the exact read this bug is about.
    ///   `range_bytes` normalises the bounds the way `lrange` does and
    ///   estimates from the mean element size.
    ///
    /// The multiplier this feeds was measured on HASHES. Sets and zsets store
    /// their members as keys with empty values, so their per-item overhead
    /// differs and k may too; it has not been measured for them. Applying the
    /// hash figure is an approximation, and named as one.
    pub fn collection_read_bytes(&self, name_upper: &[u8], args: &[Vec<u8>]) -> Option<u64> {
        let key = args.get(1)?;
        let slot = slot_for_key(key);
        let bytes = match name_upper {
            b"HGETALL" | b"HKEYS" | b"HVALS" => self.hashes.stored_bytes(slot, key),
            b"SMEMBERS" => self.sets.stored_bytes(slot, key),
            b"ZRANGE" | b"ZREVRANGE" | b"ZRANGEBYSCORE" | b"ZREVRANGEBYSCORE" | b"ZRANGEBYLEX"
            | b"ZREVRANGEBYLEX" => {
                self.zsets
                    .read_bytes(slot, key, zset_read_rows(name_upper, args))
            }
            // The whole set is built (`smembers`) and, for a negative count,
            // `|count|` members besides (BUG-0218).
            b"SRANDMEMBER" => {
                let set = self.sets.stored_bytes(slot, key).ok().flatten()?;
                let extra = match args.get(2).map(|c| parse_i64(c)) {
                    Some(Ok(n)) if n < 0 => self.srandmember_reply_bytes(slot, key, n).ok()?,
                    _ => 0,
                };
                Ok(Some(set.saturating_add(extra)))
            }
            b"ZPOPMIN" | b"ZPOPMAX" => {
                let count = args.get(2).map_or(Some(1), |c| parse_i64(c).ok())?;
                self.zsets
                    .read_bytes(slot, key, ZsetRows::AtMost(count.max(0) as u64))
            }
            // Costed on the slice, not the key. Bounds that do not parse are
            // NOT admitted as zero: the command's own error answers that, and
            // sizing a request that will never run would bound nothing.
            b"LRANGE" => {
                let (Ok(start), Ok(stop)) = (parse_i64(args.get(2)?), parse_i64(args.get(3)?))
                else {
                    return None;
                };
                self.lists.range_bytes(slot, key, start, stop)
            }
            // A pop with a count materialises the slice it takes, costed as
            // LRANGE costs one (BUG-0215).
            b"LPOP" | b"RPOP" => {
                let n = parse_i64(args.get(2)?).ok().filter(|n| *n > 0)?;
                let (start, stop) = if name_upper == b"LPOP" {
                    (0, n - 1)
                } else {
                    (-n, -1)
                };
                self.lists.range_bytes(slot, key, start, stop)
            }
            _ => return None,
        };
        // A metadata read that ERRORS (wrong type, say) is not a collection
        // read this can size, and the command's own error path will say so.
        bytes.ok().flatten()
    }

    pub fn dispatch(&self, args: &[Vec<u8>]) -> Value {
        let Some(name) = args.first() else {
            return err("ERR empty command");
        };
        let name_upper = name.to_ascii_uppercase();
        if self.has_oversized_key(&name_upper, args) {
            return err("ERR key exceeds maximum allowed size (max-key-bytes)");
        }
        match name_upper.as_slice() {
            // connection
            b"PING" => match args.len() {
                1 => Value::Simple("PONG".into()),
                2 => Value::Bulk(Some(args[1].clone())),
                _ => arity_err("ping"),
            },
            b"ECHO" => exact(args, 2, "echo", |a| Value::Bulk(Some(a[1].clone()))),

            // strings
            b"SET" => self.cmd_set(args),
            b"SETNX" => exact(args, 3, "setnx", |a| {
                let opts = SetOptions {
                    nx: true,
                    ..Default::default()
                };
                match self.strings.set(slot_for_key(&a[1]), &a[1], &a[2], opts) {
                    Ok(SetOutcome::Done) => Value::Integer(1),
                    Ok(SetOutcome::Unchanged) => Value::Integer(0),
                    Err(e) => store_err(e),
                }
            }),
            b"SETEX" => exact(args, 4, "setex", |a| {
                match string_expiry(b"EX", &a[2], (self.clock)(), "setex") {
                    Ok(at) => {
                        let opts = SetOptions {
                            expiry: SetExpiry::AtMs(at),
                            ..Default::default()
                        };
                        match self.strings.set(slot_for_key(&a[1]), &a[1], &a[3], opts) {
                            Ok(_) => Value::Simple("OK".into()),
                            Err(e) => store_err(e),
                        }
                    }
                    Err(e) => e,
                }
            }),
            b"GET" => exact(args, 2, "get", |a| {
                reply(self.strings.get(slot_for_key(&a[1]), &a[1]), Value::Bulk)
            }),
            b"GETDEL" => exact(args, 2, "getdel", |a| {
                reply(
                    self.strings.get_del(slot_for_key(&a[1]), &a[1]),
                    Value::Bulk,
                )
            }),
            b"GETEX" => self.cmd_getex(args),
            b"GETSET" => exact(args, 3, "getset", |a| {
                // Set new, return old (nil if absent; WRONGTYPE if non-string).
                let slot = slot_for_key(&a[1]);
                let old = self.strings.get(slot, &a[1]);
                if let Err(e) = old {
                    return store_err(e);
                }
                match self.strings.set(slot, &a[1], &a[2], SetOptions::default()) {
                    Ok(_) => Value::Bulk(old.ok().flatten()),
                    Err(e) => store_err(e),
                }
            }),
            b"INCR" => exact(args, 2, "incr", |a| {
                reply(
                    self.strings.incr_by(slot_for_key(&a[1]), &a[1], 1),
                    Value::Integer,
                )
            }),
            b"DECR" => exact(args, 2, "decr", |a| {
                reply(
                    self.strings.incr_by(slot_for_key(&a[1]), &a[1], -1),
                    Value::Integer,
                )
            }),
            b"INCRBY" => self.cmd_incr_delta(args, "incrby", 1),
            b"DECRBY" => self.cmd_incr_delta(args, "decrby", -1),
            b"INCRBYFLOAT" => exact(args, 3, "incrbyfloat", |a| match parse_f64(&a[2]) {
                Ok(delta) => reply(
                    self.strings
                        .incr_by_float(slot_for_key(&a[1]), &a[1], delta),
                    |repr| Value::Bulk(Some(repr)),
                ),
                // Redis checks the key's type before it reads the increment,
                // so another type answers WRONGTYPE (BUG-0219).
                Err(_) => match self.strings.strlen(slot_for_key(&a[1]), &a[1]) {
                    Err(e) => store_err(e),
                    Ok(_) => err("ERR value is not a valid float"),
                },
            }),
            b"APPEND" => exact(args, 3, "append", |a| {
                reply(
                    self.strings.append(slot_for_key(&a[1]), &a[1], &a[2]),
                    |n| Value::Integer(n as i64),
                )
            }),
            b"STRLEN" => exact(args, 2, "strlen", |a| {
                reply(self.strings.strlen(slot_for_key(&a[1]), &a[1]), |n| {
                    Value::Integer(n as i64)
                })
            }),
            b"GETRANGE" => exact(args, 4, "getrange", |a| {
                match (parse_i64(&a[2]), parse_i64(&a[3])) {
                    (Ok(start), Ok(end)) => reply(
                        self.strings
                            .getrange(slot_for_key(&a[1]), &a[1], start, end),
                        |v| Value::Bulk(Some(v)),
                    ),
                    _ => err("ERR value is not an integer or out of range"),
                }
            }),
            b"BITFIELD" => self.cmd_bitfield(args, "bitfield", false),
            b"BITFIELD_RO" => self.cmd_bitfield(args, "bitfield_ro", true),
            b"SETRANGE" => exact(args, 4, "setrange", |a| match parse_i64(&a[2]) {
                Ok(off) if off >= 0 => reply(
                    self.strings
                        .setrange(slot_for_key(&a[1]), &a[1], off as u64, &a[3]),
                    |n| Value::Integer(n as i64),
                ),
                Ok(_) => err("ERR offset is out of range"),
                Err(_) => err("ERR value is not an integer or out of range"),
            }),

            b"MSET" => {
                if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                    return arity_err("mset");
                }
                // CROSSSLOT, for the same reason the set ops refuse it
                // (BUG-0053). MSET is the worse half: a key this node does
                // not own is WRITTEN here anyway, so the value lands on a
                // node that will never be asked for it and the caller is
                // told OK. Nothing upstream catches this — flint-proxy's
                // route_key derives one slot from args[1] and ships the
                // whole command there.
                let keys: Vec<Vec<u8>> = args[1..].chunks(2).map(|c| c[0].clone()).collect();
                if let Some(e) = Self::crossslot(&keys[0], &keys[1..]) {
                    return e;
                }
                for chunk in args[1..].chunks(2) {
                    if let Err(e) = self.strings.set(
                        slot_for_key(&chunk[0]),
                        &chunk[0],
                        &chunk[1],
                        SetOptions::default(),
                    ) {
                        return store_err(e);
                    }
                }
                Value::Simple("OK".into())
            }
            b"MGET" => {
                if args.len() < 2 {
                    return arity_err("mget");
                }
                // CROSSSLOT (BUG-0053). Without this a key belonging to
                // another pair reads as a MISS — nil in its correct slot,
                // indistinguishable from a key that genuinely is not there.
                // That is the same "plausible-looking answer that is
                // silently incorrect" the set ops refuse, and MGET reached
                // neither this helper nor the inline copy.
                if let Some(e) = Self::crossslot(&args[1], &args[2..]) {
                    return e;
                }
                // Redis MGET yields nil (not an error) for wrong-type keys.
                Value::Array(Some(
                    args[1..]
                        .iter()
                        .map(|k| Value::Bulk(self.strings.get(slot_for_key(k), k).unwrap_or(None)))
                        .collect(),
                ))
            }

            // hashes
            b"HSET" => self.cmd_hset(args, "hset"),
            // HMSET is HSET answering +OK (BUG-0182). Deprecated upstream since
            // Redis 4.0 and still what Spring Session and ASP.NET Core's
            // IDistributedCache write every entry with.
            b"EVAL" | b"EVALSHA" => self.cmd_eval(args),
            b"TIME" => exact(args, 1, "time", |_| {
                let us = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros())
                    .unwrap_or(0);
                Value::Array(Some(vec![
                    Value::Bulk(Some((us / 1_000_000).to_string().into_bytes())),
                    Value::Bulk(Some((us % 1_000_000).to_string().into_bytes())),
                ]))
            }),
            b"SCRIPT" => self.cmd_script(args),
            b"HMSET" => match self.cmd_hset(args, "hmset") {
                Value::Integer(_) => Value::Simple("OK".into()),
                other => other,
            },
            b"HSETNX" => exact(args, 4, "hsetnx", |a| {
                reply(
                    self.hashes.hsetnx(slot_for_key(&a[1]), &a[1], &a[2], &a[3]),
                    |set| Value::Integer(set as i64),
                )
            }),
            b"HSTRLEN" => exact(args, 3, "hstrlen", |a| {
                reply(
                    self.hashes.hstrlen(slot_for_key(&a[1]), &a[1], &a[2]),
                    |n| Value::Integer(n as i64),
                )
            }),
            b"HGET" => exact(args, 3, "hget", |a| {
                reply(
                    self.hashes.hget(slot_for_key(&a[1]), &a[1], &a[2]),
                    Value::Bulk,
                )
            }),
            b"HDEL" => {
                if args.len() < 3 {
                    return arity_err("hdel");
                }
                reply(
                    self.hashes
                        .hdel(slot_for_key(&args[1]), &args[1], &args[2..]),
                    |n| Value::Integer(n as i64),
                )
            }
            b"HINCRBY" => exact(args, 4, "hincrby", |a| match parse_i64(&a[3]) {
                Ok(delta) => match self
                    .hashes
                    .hincr_by(slot_for_key(&a[1]), &a[1], &a[2], delta)
                {
                    Ok(n) => Value::Integer(n),
                    // Valkey names the hash value here, as HINCRBYFLOAT does.
                    Err(StoreError::NotInteger) => err("ERR hash value is not an integer"),
                    Err(e) => store_err(e),
                },
                Err(_) => err("ERR value is not an integer or out of range"),
            }),
            b"HINCRBYFLOAT" => exact(args, 4, "hincrbyfloat", |a| match parse_f64(&a[3]) {
                // Valkey refuses an infinite increment up front here, where
                // INCRBYFLOAT lets the sum refuse it.
                Ok(delta) if delta.is_infinite() => err("ERR value is NaN or Infinity"),
                Ok(delta) => {
                    match self
                        .hashes
                        .hincr_by_float(slot_for_key(&a[1]), &a[1], &a[2], delta)
                    {
                        Ok(repr) => Value::Bulk(Some(repr)),
                        // Valkey names the hash value here, where INCRBYFLOAT
                        // says "value".
                        Err(StoreError::NotFloat) => err("ERR hash value is not a float"),
                        Err(e) => store_err(e),
                    }
                }
                Err(_) => err("ERR value is not a valid float"),
            }),
            b"HLEN" => exact(args, 2, "hlen", |a| {
                reply(self.hashes.hlen(slot_for_key(&a[1]), &a[1]), |n| {
                    Value::Integer(n as i64)
                })
            }),
            b"HEXISTS" => exact(args, 3, "hexists", |a| {
                reply(
                    self.hashes.hexists(slot_for_key(&a[1]), &a[1], &a[2]),
                    |b| Value::Integer(b as i64),
                )
            }),
            b"HMGET" => {
                if args.len() < 3 {
                    return arity_err("hmget");
                }
                reply(
                    self.hashes
                        .hmget(slot_for_key(&args[1]), &args[1], &args[2..]),
                    |vals| Value::Array(Some(vals.into_iter().map(Value::Bulk).collect())),
                )
            }
            b"HGETALL" => exact(args, 2, "hgetall", |a| {
                reply(self.hashes.hgetall(slot_for_key(&a[1]), &a[1]), |pairs| {
                    // A hash IS a map; RESP2 just has no way to say so.
                    Value::Map(
                        pairs
                            .into_iter()
                            .map(|(f, v)| (Value::Bulk(Some(f)), Value::Bulk(Some(v))))
                            .collect(),
                    )
                })
            }),
            b"HKEYS" => exact(args, 2, "hkeys", |a| {
                reply(self.hashes.hgetall(slot_for_key(&a[1]), &a[1]), |pairs| {
                    Value::Array(Some(
                        pairs
                            .into_iter()
                            .map(|(f, _)| Value::Bulk(Some(f)))
                            .collect(),
                    ))
                })
            }),
            b"HVALS" => exact(args, 2, "hvals", |a| {
                reply(self.hashes.hgetall(slot_for_key(&a[1]), &a[1]), |pairs| {
                    Value::Array(Some(
                        pairs
                            .into_iter()
                            .map(|(_, v)| Value::Bulk(Some(v)))
                            .collect(),
                    ))
                })
            }),

            // sets
            b"SADD" => {
                if args.len() < 3 {
                    return arity_err("sadd");
                }
                reply(
                    self.sets.sadd(slot_for_key(&args[1]), &args[1], &args[2..]),
                    |n| Value::Integer(n as i64),
                )
            }
            b"SREM" => {
                if args.len() < 3 {
                    return arity_err("srem");
                }
                reply(
                    self.sets.srem(slot_for_key(&args[1]), &args[1], &args[2..]),
                    |n| Value::Integer(n as i64),
                )
            }
            b"SISMEMBER" => exact(args, 3, "sismember", |a| {
                reply(
                    self.sets.sismember(slot_for_key(&a[1]), &a[1], &a[2]),
                    |b| Value::Integer(b as i64),
                )
            }),
            b"SMISMEMBER" => {
                if args.len() < 3 {
                    return arity_err("smismember");
                }
                let slot = slot_for_key(&args[1]);
                match self.sets.smismember(slot, &args[1], &args[2..]) {
                    Ok(flags) => Value::Array(Some(
                        flags
                            .into_iter()
                            .map(|b| Value::Integer(b as i64))
                            .collect(),
                    )),
                    Err(e) => store_err(e),
                }
            }
            b"SMEMBERS" => exact(args, 2, "smembers", |a| {
                reply(self.sets.smembers(slot_for_key(&a[1]), &a[1]), |ms| {
                    Value::Set(ms.into_iter().map(|m| Value::Bulk(Some(m))).collect())
                })
            }),
            b"SCARD" => exact(args, 2, "scard", |a| {
                reply(self.sets.scard(slot_for_key(&a[1]), &a[1]), |n| {
                    Value::Integer(n as i64)
                })
            }),
            // SINTER / SUNION / SDIFF: multi-key, and therefore same-slot
            // only. The node refuses a cross-slot request rather than
            // answering it, because it would answer WRONGLY: a key this node
            // does not own reads as an empty set, and an intersection against
            // a phantom empty set is a plausible-looking answer that is
            // silently incorrect.
            //
            // NOTHING UPSTREAM CATCHES THIS. The proxy routes multi-key
            // commands by their FIRST key and never inspects the rest
            // (flint-proxy/src/main.rs, v0-scope note), so a cross-slot
            // request arrives here intact whether the client dialled a node
            // directly or came through the edge. This check is the only
            // enforcement in the system, not a second line.
            //
            // It is also what makes the MIGRATION gate correct for multi-key
            // commands: check_slot_gate derives the slot from command_key,
            // which is args[1] alone. Gating on one key is sound only once
            // every key is known to share its slot. Without this refusal, a
            // key in a handed-off slot would read as locally empty during a
            // migration and no -MOVED would ever be emitted.
            //
            // See the cross-slot tests at the foot of this file before
            // weakening any of it.
            b"SINTERSTORE" => {
                self.cmd_sstore(args, "sinterstore", flint_storage::sets::SetOp::Inter)
            }
            b"SUNIONSTORE" => {
                self.cmd_sstore(args, "sunionstore", flint_storage::sets::SetOp::Union)
            }
            b"SDIFFSTORE" => self.cmd_sstore(args, "sdiffstore", flint_storage::sets::SetOp::Diff),
            b"SINTER" | b"SUNION" | b"SDIFF" => {
                if args.len() < 2 {
                    return Value::Error(format!(
                        "ERR wrong number of arguments for '{}' command",
                        String::from_utf8_lossy(&args[0]).to_lowercase()
                    ));
                }
                let keys = &args[1..];
                let slot = slot_for_key(&keys[0]);
                if let Some(bad) = keys.iter().find(|k| slot_for_key(k) != slot) {
                    return Value::Error(format!(
                        "CROSSSLOT Keys in request don't hash to the same slot ({} is slot {}, \
                         {} is slot {}) — use a hash tag such as {{tag}}key to colocate them",
                        String::from_utf8_lossy(&keys[0]),
                        slot,
                        String::from_utf8_lossy(bad),
                        slot_for_key(bad)
                    ));
                }
                let op = match args[0].to_ascii_uppercase().as_slice() {
                    b"SINTER" => flint_storage::sets::SetOp::Inter,
                    b"SUNION" => flint_storage::sets::SetOp::Union,
                    _ => flint_storage::sets::SetOp::Diff,
                };
                reply(self.sets.sop(slot, op, keys), |ms| {
                    Value::Set(ms.into_iter().map(|m| Value::Bulk(Some(m))).collect())
                })
            }
            b"SPOP" => self.cmd_spop(args),
            b"SRANDMEMBER" => self.cmd_srandmember(args),
            b"HSCAN" => self.cmd_scan_typed(args, ScanKind::Hash),
            b"SSCAN" => self.cmd_scan_typed(args, ScanKind::Set),
            b"ZSCAN" => self.cmd_scan_typed(args, ScanKind::ZSet),

            // lists
            b"LPUSH" | b"RPUSH" => {
                if args.len() < 3 {
                    return arity_err(if name.eq_ignore_ascii_case(b"LPUSH") {
                        "lpush"
                    } else {
                        "rpush"
                    });
                }
                let left = name.eq_ignore_ascii_case(b"LPUSH");
                reply(
                    self.lists
                        .push(slot_for_key(&args[1]), &args[1], &args[2..], left),
                    |n| Value::Integer(n as i64),
                )
            }
            b"LMOVE" => {
                if args.len() != 5 {
                    return arity_err("lmove");
                }
                let side = |a: &[u8]| {
                    if a.eq_ignore_ascii_case(b"LEFT") {
                        Some(true)
                    } else if a.eq_ignore_ascii_case(b"RIGHT") {
                        Some(false)
                    } else {
                        None
                    }
                };
                match (side(&args[3]), side(&args[4])) {
                    (Some(from_left), Some(to_left)) => {
                        self.cmd_lmove(&args[1], &args[2], from_left, to_left)
                    }
                    _ => err("ERR syntax error"),
                }
            }
            b"RPOPLPUSH" => exact(args, 3, "rpoplpush", |a| {
                self.cmd_lmove(&a[1], &a[2], false, true)
            }),
            // ADR-0052 D4: the blocking pops, never waiting. See `cmd_bpop`.
            b"BLMOVE" => {
                if args.len() != 6 {
                    return arity_err("blmove");
                }
                let side = |a: &[u8]| {
                    if a.eq_ignore_ascii_case(b"LEFT") {
                        Some(true)
                    } else if a.eq_ignore_ascii_case(b"RIGHT") {
                        Some(false)
                    } else {
                        None
                    }
                };
                let (Some(from_left), Some(to_left)) = (side(&args[3]), side(&args[4])) else {
                    return err("ERR syntax error");
                };
                match parse_block_timeout(&args[5]) {
                    Ok(_) => self.cmd_lmove(&args[1], &args[2], from_left, to_left),
                    Err(e) => e,
                }
            }
            b"BRPOPLPUSH" => exact(args, 4, "brpoplpush", |a| {
                match parse_block_timeout(&a[3]) {
                    Ok(_) => self.cmd_lmove(&a[1], &a[2], false, true),
                    Err(e) => e,
                }
            }),
            b"BLPOP" => self.cmd_bpop(args, "blpop", false, false),
            b"BRPOP" => self.cmd_bpop(args, "brpop", false, true),
            b"BZPOPMIN" => self.cmd_bpop(args, "bzpopmin", true, false),
            b"BZPOPMAX" => self.cmd_bpop(args, "bzpopmax", true, true),
            // `LPOP key [count]` (BUG-0215: the count, Redis 6.2's, was an
            // arity error, and RPOP's error named LPOP).
            b"LPOP" | b"RPOP" => {
                let left = name.eq_ignore_ascii_case(b"LPOP");
                match args {
                    [_, key] => reply(self.lists.pop(slot_for_key(key), key, left), Value::Bulk),
                    [_, key, count] => match parse_i64(count) {
                        Ok(n) if n >= 0 => reply(
                            self.lists.pop_n(slot_for_key(key), key, n as u64, left),
                            |popped| {
                                Value::Array(popped.map(|elems| {
                                    elems.into_iter().map(|e| Value::Bulk(Some(e))).collect()
                                }))
                            },
                        ),
                        _ => err("ERR value is out of range, must be positive"),
                    },
                    _ => arity_err(if left { "lpop" } else { "rpop" }),
                }
            }
            b"LINDEX" => exact(args, 3, "lindex", |a| match parse_i64(&a[2]) {
                Ok(rank) => reply(
                    self.lists.lindex(slot_for_key(&a[1]), &a[1], rank),
                    Value::Bulk,
                ),
                // Redis reads the key before the index: a missing key is
                // nil and another type WRONGTYPE, whatever the index says
                // (BUG-0219).
                Err(_) => match self.lists.llen(slot_for_key(&a[1]), &a[1]) {
                    Ok(0) => Value::Bulk(None),
                    Ok(_) => err("ERR value is not an integer or out of range"),
                    Err(e) => store_err(e),
                },
            }),
            b"LLEN" => exact(args, 2, "llen", |a| {
                reply(self.lists.llen(slot_for_key(&a[1]), &a[1]), |n| {
                    Value::Integer(n as i64)
                })
            }),
            b"LRANGE" => exact(args, 4, "lrange", |a| {
                match (parse_i64(&a[2]), parse_i64(&a[3])) {
                    (Ok(start), Ok(stop)) => reply(
                        self.lists.lrange(slot_for_key(&a[1]), &a[1], start, stop),
                        |vs| {
                            Value::Array(Some(
                                vs.into_iter().map(|v| Value::Bulk(Some(v))).collect(),
                            ))
                        },
                    ),
                    _ => err("ERR value is not an integer or out of range"),
                }
            }),
            b"LSET" => exact(args, 4, "lset", |a| match parse_i64(&a[2]) {
                Ok(rank) => match self.lists.lset(slot_for_key(&a[1]), &a[1], rank, &a[3]) {
                    Ok(LsetOutcome::Set) => Value::Simple("OK".into()),
                    Ok(LsetOutcome::NoKey) => err("ERR no such key"),
                    Ok(LsetOutcome::OutOfRange) => err("ERR index out of range"),
                    Err(e) => store_err(e),
                },
                // As LINDEX: the key first, so `no such key` or WRONGTYPE
                // before the index (BUG-0219).
                Err(_) => match self.lists.llen(slot_for_key(&a[1]), &a[1]) {
                    Ok(0) => err("ERR no such key"),
                    Ok(_) => err("ERR value is not an integer or out of range"),
                    Err(e) => store_err(e),
                },
            }),
            b"LTRIM" => exact(args, 4, "ltrim", |a| {
                match (parse_i64(&a[2]), parse_i64(&a[3])) {
                    (Ok(start), Ok(stop)) => reply(
                        self.lists.ltrim(slot_for_key(&a[1]), &a[1], start, stop),
                        |()| Value::Simple("OK".into()),
                    ),
                    _ => err("ERR value is not an integer or out of range"),
                }
            }),
            b"LPOS" => self.cmd_lpos(args),
            b"LREM" => exact(args, 4, "lrem", |a| match parse_i64(&a[2]) {
                // Redis's bound is symmetric, so i64::MIN is out of it. It
                // removed matches from the tail here (BUG-0219).
                Ok(i64::MIN) => err(OUT_OF_SYMMETRIC_RANGE),
                Ok(count) => reply(
                    self.lists.lrem(slot_for_key(&a[1]), &a[1], count, &a[3]),
                    |n| Value::Integer(n as i64),
                ),
                Err(_) => err("ERR value is not an integer or out of range"),
            }),
            b"LINSERT" => exact(args, 5, "linsert", |a| {
                let before = match a[2].to_ascii_uppercase().as_slice() {
                    b"BEFORE" => true,
                    b"AFTER" => false,
                    _ => return err("ERR syntax error"),
                };
                reply(
                    self.lists
                        .linsert(slot_for_key(&a[1]), &a[1], before, &a[3], &a[4]),
                    Value::Integer,
                )
            }),

            // zsets
            b"ZADD" => self.cmd_zadd(args),
            b"ZSCORE" => exact(args, 3, "zscore", |a| {
                reply(self.zsets.zscore(slot_for_key(&a[1]), &a[1], &a[2]), |s| {
                    s.map(Value::Double).unwrap_or(Value::Null)
                })
            }),
            b"ZINCRBY" => exact(args, 4, "zincrby", |a| match parse_f64(&a[2]) {
                Ok(delta) => reply(
                    self.zsets
                        .zincr_by(slot_for_key(&a[1]), &a[1], delta, &a[3]),
                    Value::Double,
                ),
                Err(_) => err("ERR value is not a valid float"),
            }),
            b"ZREM" => {
                if args.len() < 3 {
                    return arity_err("zrem");
                }
                reply(
                    self.zsets
                        .zrem(slot_for_key(&args[1]), &args[1], &args[2..]),
                    |n| Value::Integer(n as i64),
                )
            }
            b"ZCARD" => exact(args, 2, "zcard", |a| {
                reply(self.zsets.zcard(slot_for_key(&a[1]), &a[1]), |n| {
                    Value::Integer(n as i64)
                })
            }),
            b"ZRANGE" => self.cmd_zrange(args),
            b"ZREVRANGE" => self.cmd_zrange_idx(args, "zrevrange", true),
            b"ZRANGEBYSCORE" => self.cmd_zrangebyscore(args, "zrangebyscore", false),
            b"ZREVRANGEBYSCORE" => self.cmd_zrangebyscore(args, "zrevrangebyscore", true),
            b"ZRANGEBYLEX" => self.cmd_zrangebylex(args, "zrangebylex", false),
            b"ZREVRANGEBYLEX" => self.cmd_zrangebylex(args, "zrevrangebylex", true),
            b"ZRANK" => self.cmd_zrank(args, "zrank", false),
            b"ZREVRANK" => self.cmd_zrank(args, "zrevrank", true),
            b"ZCOUNT" => self.cmd_zcount(args),
            b"ZMSCORE" => self.cmd_zmscore(args),
            b"ZPOPMIN" => self.cmd_zpop(args, "zpopmin", false),
            b"ZPOPMAX" => self.cmd_zpop(args, "zpopmax", true),
            b"ZLEXCOUNT" => self.cmd_zlexrange(args, "zlexcount", false),
            b"ZREMRANGEBYLEX" => self.cmd_zlexrange(args, "zremrangebylex", true),
            b"ZUNIONSTORE" => self.cmd_zstore(args, "zunionstore", false),
            b"ZINTERSTORE" => self.cmd_zstore(args, "zinterstore", true),
            b"ZREMRANGEBYSCORE" => self.cmd_zremrangebyscore(args),
            b"ZREMRANGEBYRANK" => self.cmd_zremrangebyrank(args),

            // keyspace (type-agnostic)
            b"DEL" | b"UNLINK" => {
                // UNLINK is Redis's async unlink; our DEL is already O(1)
                // (version bump), so they are identical here.
                multi_key(args, "del", |k| self.keyspace.del(slot_for_key(k), k))
            }
            b"EXISTS" => multi_key(args, "exists", |k| self.keyspace.exists(slot_for_key(k), k)),
            b"TYPE" => exact(args, 2, "type", |a| {
                match self.keyspace.value_type(slot_for_key(&a[1]), &a[1]) {
                    Some(t) => Value::Simple(t.name().into()),
                    None => Value::Simple("none".into()),
                }
            }),
            // SELECT: docs/command-support.md has listed this as supported
            // since the connection family was written, and it answered
            // "unknown command" until now — found by probing rather than by
            // reading, which is the only way a claim like this ever surfaces.
            //
            // Index 0 is the only database a namespace has, so that is the
            // only index accepted. The error texts are Valkey's own, checked
            // against a live server: a non-integer and an out-of-range index
            // fail differently there, and a client that distinguishes them
            // should not have to care which server it is talking to.
            b"SELECT" => exact(args, 2, "select", |a| match parse_i64(&a[1]) {
                Ok(0) => Value::Simple("OK".into()),
                Ok(_) => err("ERR DB index is out of range"),
                Err(_) => err("ERR value is not an integer or out of range"),
            }),
            b"COPY" => self.cmd_copy(args),
            b"RENAME" => self.cmd_rename(args, "rename", false),
            b"RENAMENX" => self.cmd_rename(args, "renamenx", true),
            b"EXPIRE" => self.cmd_expire(args, "expire", 1000),
            b"PEXPIRE" => self.cmd_expire(args, "pexpire", 1),
            b"EXPIREAT" => self.cmd_expire_at(args, "expireat", 1000),
            b"PEXPIREAT" => self.cmd_expire_at(args, "pexpireat", 1),
            b"TTL" => self.cmd_ttl(args, "ttl", 1000),
            b"PTTL" => self.cmd_ttl(args, "pttl", 1),
            b"EXPIRETIME" => self.cmd_expire_time(args, "expiretime", 1000),
            b"PEXPIRETIME" => self.cmd_expire_time(args, "pexpiretime", 1),
            b"PERSIST" => exact(args, 2, "persist", |a| {
                Value::Integer(self.keyspace.persist(slot_for_key(&a[1]), &a[1]) as i64)
            }),

            // ADR-0013: the ranking primitives for user-driven GC. Both are
            // O(1) reads of the metadata row; nil for a missing/expired key.
            b"FLINTKEYSIZE" => exact(args, 2, "flintkeysize", |a| {
                match self.keyspace.key_stat(slot_for_key(&a[1]), &a[1]) {
                    Some(st) => Value::Integer(st.size_bytes as i64),
                    None => Value::Bulk(None),
                }
            }),
            b"FLINTKEYSTAMP" => exact(args, 2, "flintkeystamp", |a| {
                match self.keyspace.key_stat(slot_for_key(&a[1]), &a[1]) {
                    // [written_ms, created_ms]; 0 = unknown (a pre-stamp row,
                    // or a type with no version to derive creation from).
                    Some(st) => Value::Array(Some(vec![
                        Value::Integer(st.written_ms as i64),
                        Value::Integer(st.created_ms as i64),
                    ])),
                    None => Value::Bulk(None),
                }
            }),

            // JSON documents
            b"JSON.SET" => self.cmd_json_set(args),
            b"JSON.GET" => self.cmd_json_get(args),
            b"JSON.DEL" | b"JSON.FORGET" => self.cmd_json_del(args),
            b"JSON.TYPE" => self.cmd_json_type(args),
            b"JSON.NUMINCRBY" => self.cmd_json_numop(args, false),
            b"JSON.NUMMULTBY" => self.cmd_json_numop(args, true),
            b"JSON.ARRAPPEND" => self.cmd_json_arrappend(args),
            b"JSON.ARRLEN" => self.cmd_json_arrlen(args),
            // ADR-0055, in commands/json.rs.
            b"JSON.MGET" => self.cmd_json_mget(args),
            b"JSON.MSET" => self.cmd_json_mset(args),
            b"JSON.MERGE" => self.cmd_json_merge(args),
            b"JSON.STRLEN" => self.cmd_json_strlen(args),
            b"JSON.STRAPPEND" => self.cmd_json_strappend(args),
            b"JSON.OBJLEN" => self.cmd_json_objlen(args),
            b"JSON.OBJKEYS" => self.cmd_json_objkeys(args),
            b"JSON.TOGGLE" => self.cmd_json_toggle(args),
            b"JSON.ARRINDEX" => self.cmd_json_arrindex(args),
            b"JSON.ARRINSERT" => self.cmd_json_arrinsert(args),
            b"JSON.ARRPOP" => self.cmd_json_arrpop(args),
            b"JSON.ARRTRIM" => self.cmd_json_arrtrim(args),
            b"JSON.CLEAR" => self.cmd_json_clear(args),
            b"JSON.RESP" => self.cmd_json_resp(args),
            b"JSON.DEBUG" => self.cmd_json_debug(args),
            b"BF.RESERVE" => self.cmd_bf_reserve(args),
            b"BF.ADD" => exact(args, 3, "bf.add", |a| {
                reply(self.bloom.add(slot_for_key(&a[1]), &a[1], &a[2]), |b| {
                    Value::Integer(b as i64)
                })
            }),
            b"BF.EXISTS" => exact(args, 3, "bf.exists", |a| {
                reply(self.bloom.exists(slot_for_key(&a[1]), &a[1], &a[2]), |b| {
                    Value::Integer(b as i64)
                })
            }),
            b"BF.MADD" | b"BF.MEXISTS" => self.cmd_bf_multi(name, args),
            b"BF.CARD" => exact(args, 2, "bf.card", |a| {
                reply(self.bloom.card(slot_for_key(&a[1]), &a[1]), |n| {
                    Value::Integer(n as i64)
                })
            }),
            b"BF.INFO" => self.cmd_bf_info(args),
            b"BF.INSERT" => self.cmd_bf_insert(args),
            // ADR-0016 D7.2: our block layout is not RedisBloom's, so a
            // dump would be a blob that looks portable and is accepted by
            // nothing. Refusing is the honest failure; the alternative is
            // discovered at the far end of somebody's migration.
            b"BF.SCANDUMP" | b"BF.LOADCHUNK" => {
                err("ERR BF.SCANDUMP/BF.LOADCHUNK are not supported: \
                 Flint's filter layout differs from RedisBloom's, so the \
                 chunk format is not interchangeable")
            }

            // keyspace iteration
            b"SCAN" => self.cmd_scan(args),

            // admin
            b"DBSIZE" => {
                // O(n) streaming scan of metadata rows, skipping expired
                // ones. MUST stay on `for_each_prefix`: a materialized
                // scan of this CF is O(dataset) memory and OOM-killed the
                // server at 100M keys. Becomes a maintained counter with
                // per-slot accounting later. Doubles as the full-sync
                // integrity probe.
                let now = (self.clock)();
                let mut live: i64 = 0;
                self.kv.for_each_prefix(
                    &self.ns_prefix(flint_storage::encoding::Cf::Metadata),
                    &mut |_, row| {
                        if flint_storage::encoding::MetaHeader::decode(row)
                            .is_some_and(|h| !h.is_expired(now))
                        {
                            live += 1;
                        }
                        true
                    },
                );
                Value::Integer(live)
            }
            // FLUSHDB IS FLUSHALL (BUG-0178). A tenant has one database --
            // SELECT accepts 0 only -- so "this database" and "every
            // database" are the same set. It was an unknown command, which
            // made Django's cache.clear() raise and Rails' RedisCacheStore
            // #clear silently do nothing (its error handler swallows it).
            b"FLUSHALL" | b"FLUSHDB" => {
                // ASYNC or SYNC, or nothing. Anything else flushed too until
                // BUG-0213, where Redis refuses it and flushes nothing.
                if !flint_commands::flush_args_ok(args) {
                    return err("ERR syntax error");
                }
                // Namespace-scoped: a tenant flushing its cache must never
                // touch another tenant's rows (kv.clear() would). Chunked
                // collect-then-delete keeps memory bounded on huge tenants.
                use flint_storage::encoding::Cf;
                for cf in [Cf::Metadata, Cf::Subkey, Cf::ZScore] {
                    let prefix = self.ns_prefix(cf);
                    loop {
                        let mut batch: Vec<Vec<u8>> = Vec::new();
                        self.kv.for_each_prefix(&prefix, &mut |k, _| {
                            batch.push(k.to_vec());
                            batch.len() < 10_000
                        });
                        if batch.is_empty() {
                            break;
                        }
                        let done = batch.len() < 10_000;
                        for k in &batch {
                            self.kv.delete(k);
                        }
                        if done {
                            break;
                        }
                    }
                }
                Value::Simple("OK".into())
            }
            b"COMMAND" => Value::Array(Some(vec![])),
            other => unknown_command(other, &args[1..]),
        }
    }

    /// GETEX key [EX s | PX ms | EXAT ts | PXAT ts | PERSIST].
    ///
    /// The default is KEEP, not CLEAR — the opposite of SET. A bare GETEX is
    /// a plain GET and must leave the TTL alone; reusing SetOptions::default
    /// here would silently make every GETEX a PERSIST.
    fn cmd_getex(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 2 {
            return arity_err("getex");
        }
        // Redis takes one kind of expiry option at most. Repeating one is
        // allowed and the last wins; a second kind is a syntax error, so a
        // client cannot half-apply a contradiction like "EX 60 PERSIST".
        let mut persist = false;
        let mut timed: Option<(Vec<u8>, &[u8])> = None;
        let mut i = 2;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"PERSIST" if timed.is_none() => persist = true,
                b"EX" | b"PX" | b"EXAT" | b"PXAT"
                    if !persist
                        && i + 1 < args.len()
                        && timed.as_ref().is_none_or(|(o, _)| *o == opt) =>
                {
                    timed = Some((opt, &args[i + 1]));
                    i += 1;
                }
                _ => return err("ERR syntax error"),
            }
            i += 1;
        }
        // The time is judged before the key is read, as Valkey judges it.
        // Redis 8.2 reads the key first, so a missing key answers nil there
        // whatever the time says.
        let expiry = match timed {
            None if persist => SetExpiry::Clear,
            None => SetExpiry::Keep,
            Some((opt, raw)) => match string_expiry(&opt, raw, (self.clock)(), "getex") {
                Ok(at) => SetExpiry::AtMs(at),
                Err(e) => return e,
            },
        };
        reply(
            self.strings.getex(slot_for_key(&args[1]), &args[1], expiry),
            Value::Bulk,
        )
    }

    fn cmd_set(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 3 {
            return arity_err("set");
        }
        let (key, value) = (&args[1], &args[2]);
        let mut opts = SetOptions::default();
        // SET ... GET: return the OLD value (nil if absent; WRONGTYPE if the
        // key held a non-string). NX+GET/XX+GET are valid in modern Redis.
        let mut want_get = false;
        // One kind of expiry option at most, as GETEX (BUG-0213): EX, PX,
        // EXAT, PXAT or KEEPTTL, repeated or not. A second kind, or NX with
        // XX, is a syntax error where this kept the last. The time is judged
        // after every option parses, as Redis judges it.
        let mut keep_ttl = false;
        let mut timed: Option<(Vec<u8>, &[u8])> = None;
        let mut i = 3;
        while i < args.len() {
            let opt = args[i].to_ascii_uppercase();
            match opt.as_slice() {
                b"NX" if !opts.xx => opts.nx = true,
                b"XX" if !opts.nx => opts.xx = true,
                b"GET" => want_get = true,
                b"KEEPTTL" if timed.is_none() => keep_ttl = true,
                b"EX" | b"PX" | b"EXAT" | b"PXAT"
                    if !keep_ttl
                        && i + 1 < args.len()
                        && timed.as_ref().is_none_or(|(o, _)| *o == opt) =>
                {
                    timed = Some((opt, &args[i + 1]));
                    i += 1;
                }
                _ => return err("ERR syntax error"),
            }
            i += 1;
        }
        if keep_ttl {
            opts.expiry = SetExpiry::Keep;
        }
        if let Some((opt, raw)) = timed {
            match string_expiry(&opt, raw, (self.clock)(), "set") {
                Ok(at) => opts.expiry = SetExpiry::AtMs(at),
                Err(e) => return e,
            }
        }
        let slot = slot_for_key(key);
        // With GET we must read the old value first (and surface WRONGTYPE).
        let old = if want_get {
            match self.strings.get(slot, key) {
                Ok(v) => Some(v),
                Err(e) => return store_err(e),
            }
        } else {
            None
        };
        match self.strings.set(slot, key, value, opts) {
            Ok(SetOutcome::Done) => match old {
                Some(v) => Value::Bulk(v),
                None => Value::Simple("OK".into()),
            },
            // NX/XX rejected the write: GET still returns the old value.
            Ok(SetOutcome::Unchanged) => match old {
                Some(v) => Value::Bulk(v),
                None => Value::Bulk(None),
            },
            Err(e) => store_err(e),
        }
    }

    /// `BITFIELD key [GET type offset] [OVERFLOW WRAP|SAT|FAIL]
    /// [SET type offset value] [INCRBY type offset increment] ...` and
    /// `BITFIELD_RO key [GET type offset] ...` (BUG-0192). Every operation is
    /// parsed before any runs, with Valkey's errors in Valkey's order; the
    /// storage layer runs them (`StringStore::bitfield`).
    fn cmd_bitfield(&self, args: &[Vec<u8>], name: &str, read_only: bool) -> Value {
        const BAD_TYPE: &str = "ERR Invalid bitfield type. Use something like i16 u8. \
                                Note that u64 is not supported but i64 is.";
        const BAD_OFFSET: &str = "ERR bit offset is not an integer or out of range";
        // Valkey bounds an offset by proto-max-bulk-len, 512 MiB; the value
        // cap of this seat then bounds what a write may grow the string to.
        const MAX_OFFSET_BYTES: i64 = 512 * 1024 * 1024;
        if args.len() < 2 {
            return arity_err(name);
        }
        let field_type = |raw: &[u8]| -> Option<(bool, u32)> {
            let (&first, digits) = raw.split_first()?;
            let signed = match first {
                b'i' => true,
                b'u' => false,
                _ => return None,
            };
            let bits = u32::try_from(parse_i64(digits).ok()?).ok()?;
            let max = if signed { 64 } else { 63 };
            (1..=max).contains(&bits).then_some((signed, bits))
        };
        let field_offset = |raw: &[u8], bits: u32| -> Option<u64> {
            let (hash, digits) = match raw.split_first() {
                Some((b'#', rest)) => (true, rest),
                _ => (false, raw),
            };
            let n = parse_i64(digits).ok()?;
            let n = if hash {
                n.checked_mul(i64::from(bits))?
            } else {
                n
            };
            (n >= 0 && (n >> 3) < MAX_OFFSET_BYTES).then_some(n as u64)
        };
        let mut ops = Vec::new();
        let mut overflow = BitfieldOverflow::Wrap;
        let mut i = 2;
        while i < args.len() {
            let rest = args.len() - i - 1;
            let verb = args[i].to_ascii_uppercase();
            let takes_value = match verb.as_slice() {
                b"GET" if rest >= 2 => false,
                b"SET" | b"INCRBY" if rest >= 3 => true,
                b"OVERFLOW" if rest >= 1 => {
                    overflow = match args[i + 1].to_ascii_uppercase().as_slice() {
                        b"WRAP" => BitfieldOverflow::Wrap,
                        b"SAT" => BitfieldOverflow::Sat,
                        b"FAIL" => BitfieldOverflow::Fail,
                        _ => return err("ERR Invalid OVERFLOW type specified"),
                    };
                    i += 2;
                    continue;
                }
                _ => return err("ERR syntax error"),
            };
            let Some((signed, bits)) = field_type(&args[i + 1]) else {
                return err(BAD_TYPE);
            };
            let Some(offset) = field_offset(&args[i + 2], bits) else {
                return err(BAD_OFFSET);
            };
            let kind = if takes_value {
                let Ok(v) = parse_i64(&args[i + 3]) else {
                    return err("ERR value is not an integer or out of range");
                };
                if verb.as_slice() == b"SET" {
                    BitfieldKind::Set(v, overflow)
                } else {
                    BitfieldKind::IncrBy(v, overflow)
                }
            } else {
                BitfieldKind::Get
            };
            ops.push(BitfieldOp {
                signed,
                bits,
                offset,
                kind,
            });
            i += if takes_value { 4 } else { 3 };
        }
        if read_only && ops.iter().any(|op| op.kind != BitfieldKind::Get) {
            return err("ERR BITFIELD_RO only supports the GET subcommand");
        }
        match self
            .strings
            .bitfield(slot_for_key(&args[1]), &args[1], &ops)
        {
            Ok(replies) => Value::Array(Some(
                replies
                    .into_iter()
                    .map(|r| r.map_or(Value::Bulk(None), Value::Integer))
                    .collect(),
            )),
            Err(e) => store_err(e),
        }
    }

    /// LMOVE / RPOPLPUSH (BUG-0187): pop from one end of `src`, push onto
    /// one end of `dst`, answer the element. Both keys in one slot. Valkey's
    /// order of checks: a missing source answers nil whatever `dst` is, then
    /// the source's type, then the destination's, all before anything moves.
    /// The destination is a second key, so `main` locks every writer for it,
    /// as for any multi-key write (BUG-0188).
    fn cmd_lmove(&self, src: &[u8], dst: &[u8], from_left: bool, to_left: bool) -> Value {
        if let Some(refusal) = Self::crossslot(src, std::slice::from_ref(&dst.to_vec())) {
            return refusal;
        }
        let slot = slot_for_key(src);
        match self.lists.llen(slot, src) {
            Ok(0) => return Value::Bulk(None),
            Ok(_) => {}
            Err(e) => return store_err(e),
        }
        if src != dst
            && let Err(e) = self.lists.llen(slot, dst)
        {
            return store_err(e);
        }
        let v = match self.lists.pop(slot, src, from_left) {
            Ok(Some(v)) => v,
            Ok(None) => return Value::Bulk(None),
            Err(e) => return store_err(e),
        };
        match self
            .lists
            .push(slot, dst, std::slice::from_ref(&v), to_left)
        {
            Ok(_) => Value::Bulk(Some(v)),
            Err(e) => {
                // The one refusal left is the value cap on `dst`: put the
                // element back where it came from, so a refusal moves nothing.
                let _ = self
                    .lists
                    .push(slot, src, std::slice::from_ref(&v), from_left);
                store_err(e)
            }
        }
    }

    /// `BLPOP` / `BRPOP` / `BZPOPMIN` / `BZPOPMAX key [key ...] timeout`,
    /// without waiting (ADR-0052 D4): the first key in order that holds an
    /// element answers `[key, element]` (`[key, member, score]` for a sorted
    /// set), and none answers a null array. That is what Redis does inside
    /// `MULTI` or a script, where a command may not block.
    ///
    /// A seat never waits. Through the proxy a client does: the proxy runs
    /// this form one key at a time, in the caller's order, until one answers
    /// or the timeout passes. `end` is the right end of a list, or the
    /// highest score.
    fn cmd_bpop(&self, args: &[Vec<u8>], name: &str, zset: bool, end: bool) -> Value {
        if args.len() < 3 {
            return arity_err(name);
        }
        if let Err(e) = parse_block_timeout(&args[args.len() - 1]) {
            return e;
        }
        let keys = &args[1..args.len() - 1];
        if let Some(refusal) = Self::crossslot(&keys[0], &keys[1..]) {
            return refusal;
        }
        for k in keys {
            let slot = slot_for_key(k);
            if zset {
                match self.zsets.zpop(slot, k, 1, end) {
                    Ok(popped) => {
                        if let Some((member, score)) = popped.into_iter().next() {
                            return Value::Array(Some(vec![
                                Value::Bulk(Some(k.clone())),
                                Value::Bulk(Some(member)),
                                Value::Double(score),
                            ]));
                        }
                    }
                    Err(e) => return store_err(e),
                }
            } else {
                match self.lists.pop(slot, k, !end) {
                    Ok(Some(v)) => {
                        return Value::Array(Some(vec![
                            Value::Bulk(Some(k.clone())),
                            Value::Bulk(Some(v)),
                        ]));
                    }
                    Ok(None) => {}
                    Err(e) => return store_err(e),
                }
            }
        }
        Value::Array(None)
    }

    fn cmd_hset(&self, args: &[Vec<u8>], name: &str) -> Value {
        if args.len() < 4 || !args.len().is_multiple_of(2) {
            return arity_err(name);
        }
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = args[2..]
            .chunks(2)
            .map(|c| (c[0].clone(), c[1].clone()))
            .collect();
        reply(
            self.hashes.hset(slot_for_key(&args[1]), &args[1], &pairs),
            |n| Value::Integer(n as i64),
        )
    }

    /// `EVAL script numkeys key.. arg..` and `EVALSHA sha1 numkeys ...`
    /// (ADR-0051): the script runs in the sandboxed Lua of `crate::script`.
    ///
    /// Its writes buffer on a `BatchingKv` over this dispatcher's store and
    /// reach it only if the script ends normally, so a script that fails, or
    /// runs past its limits, leaves nothing behind. Each `redis.call` runs on
    /// its own overlay of that buffer behind a `KeyGuard`: a command that
    /// reaches a key outside the declared keys' slot is refused and its
    /// overlay discarded, so even a `pcall`ed refusal changes nothing.
    ///
    /// Atomicity against other writers is the caller's lock (`main` locks
    /// the declared keys, as it does any write's); crash atomicity is the
    /// caller's commit of this dispatcher's own store, which `main` makes a
    /// `BatchingKv` committed as one batch. A key in the slot but not
    /// declared is not under that lock, so reaching one without the lock
    /// over every writer abandons the script (`wants_every_writer`).
    fn cmd_eval(&self, args: &[Vec<u8>]) -> Value {
        let is_sha = args[0].eq_ignore_ascii_case(b"EVALSHA");
        let name = if is_sha { "evalsha" } else { "eval" };
        if args.len() < 3 {
            return arity_err(name);
        }
        let Ok(n) = parse_i64(&args[2]) else {
            return err("ERR value is not an integer or out of range");
        };
        if n < 0 {
            return err("ERR Number of keys can't be negative");
        }
        if n as usize > args.len() - 3 {
            return err("ERR Number of keys can't be greater than number of args");
        }
        let (keys, argv) = args[3..].split_at(n as usize);
        let text = if is_sha {
            let sha = String::from_utf8_lossy(&args[1]).to_ascii_lowercase();
            match crate::script::lookup(&self.ns, &sha) {
                Some(t) => t,
                None => return err("NOSCRIPT No matching script."),
            }
        } else {
            args[1].clone()
        };
        if !self.whole
            && let Some(first) = keys.first()
            && let Some(refusal) = Self::crossslot(first, &keys[1..])
        {
            return refusal;
        }
        let declared: std::collections::HashSet<Vec<u8>> = keys.iter().cloned().collect();
        let buffer = flint_storage::batch::BatchingKv::new(self.kv);
        let needs: std::cell::RefCell<Option<crate::script::Stray>> = Default::default();
        let call = |cmd: &[Vec<u8>]| -> Result<Value, crate::script::Abandon> {
            let overlay = flint_storage::batch::BatchingKv::new(&buffer);
            let (reply, strayed) = {
                let guard =
                    crate::script::KeyGuard::new(&overlay, &self.ns, &declared, self.every_writer)
                        .whole(self.whole);
                let reply = Dispatcher::with_limits(&guard, self.clock, self.limits, &self.ns)
                    .dispatch(cmd);
                (reply, guard.strayed())
            };
            match strayed {
                Some(stray @ crate::script::Stray::NeedsEveryWriter(_)) => {
                    *needs.borrow_mut() = Some(stray);
                    return Err(crate::script::Abandon);
                }
                Some(stray) => return Ok(stray.refusal()),
                None => {}
            }
            for (k, v) in overlay.into_ops() {
                match v {
                    Some(v) => buffer.put(&k, &v),
                    None => {
                        buffer.delete(&k);
                    }
                }
            }
            Ok(reply)
        };
        let sha = if is_sha {
            String::from_utf8_lossy(&args[1]).to_ascii_lowercase()
        } else {
            flint_tls::sha1_hex(&text)
        };
        let out = crate::script::run(&self.ns, &sha, &text, keys, argv, self.limits.script, &call);
        if out.compiled && !is_sha {
            crate::script::remember(&self.ns, &sha, &text);
        }
        if out.abandoned {
            self.wants_every_writer.set(true);
            return match needs.into_inner() {
                Some(stray) => stray.refusal(),
                None => out.reply,
            };
        }
        if out.commit {
            for (k, v) in buffer.into_ops() {
                match v {
                    Some(v) => self.kv.put(&k, &v),
                    None => {
                        self.kv.delete(&k);
                    }
                }
            }
        }
        out.reply
    }

    /// `SCRIPT LOAD | EXISTS | FLUSH | KILL`, over this namespace's scripts.
    fn cmd_script(&self, args: &[Vec<u8>]) -> Value {
        let Some(sub) = args.get(1) else {
            return arity_err("script");
        };
        match sub.to_ascii_uppercase().as_slice() {
            b"LOAD" if args.len() == 3 => match crate::script::compile_check(&args[2]) {
                Ok(()) => {
                    let sha = flint_tls::sha1_hex(&args[2]);
                    crate::script::remember(&self.ns, &sha, &args[2]);
                    Value::Bulk(Some(sha.into_bytes()))
                }
                Err(e) => e,
            },
            b"EXISTS" if args.len() >= 3 => Value::Array(Some(
                args[2..]
                    .iter()
                    .map(|s| {
                        let sha = String::from_utf8_lossy(s).to_ascii_lowercase();
                        Value::Integer(crate::script::lookup(&self.ns, &sha).is_some() as i64)
                    })
                    .collect(),
            )),
            b"FLUSH"
                if args.len() == 2
                    || (args.len() == 3
                        && (args[2].eq_ignore_ascii_case(b"ASYNC")
                            || args[2].eq_ignore_ascii_case(b"SYNC"))) =>
            {
                crate::script::flush(&self.ns);
                Value::Simple("OK".into())
            }
            // A script is stopped by its time limit, not by SCRIPT KILL, and
            // none outlives the command that runs it.
            b"KILL" if args.len() == 2 => err("NOTBUSY No scripts in execution right now."),
            b"LOAD" | b"EXISTS" | b"FLUSH" | b"KILL" => arity_err("script"),
            _ => err(&format!(
                "ERR unknown subcommand '{}'. Try SCRIPT HELP.",
                String::from_utf8_lossy(sub)
            )),
        }
    }

    /// `ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]`,
    /// parsed as Redis's `zaddGenericCommand` parses it (BUG-0215: every
    /// flag was read as a score). The flags lead, in any order, and the
    /// first word that is not one starts the pairs.
    fn cmd_zadd(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 4 {
            return arity_err("zadd");
        }
        let mut flags = ZaddFlags::default();
        let mut ch = false;
        let mut i = 2;
        while let Some(opt) = args.get(i) {
            match opt.to_ascii_uppercase().as_slice() {
                b"NX" => flags.nx = true,
                b"XX" => flags.xx = true,
                b"GT" => flags.gt = true,
                b"LT" => flags.lt = true,
                b"CH" => ch = true,
                b"INCR" => flags.incr = true,
                _ => break,
            }
            i += 1;
        }
        let rest = &args[i..];
        if rest.is_empty() || !rest.len().is_multiple_of(2) {
            return err("ERR syntax error");
        }
        if flags.nx && flags.xx {
            return err("ERR XX and NX options at the same time are not compatible");
        }
        if (flags.gt || flags.lt) && flags.nx || flags.gt && flags.lt {
            return err("ERR GT, LT, and/or NX options at the same time are not compatible");
        }
        if flags.incr && rest.len() > 2 {
            return err("ERR INCR option supports a single increment-element pair");
        }
        let mut pairs = Vec::with_capacity(rest.len() / 2);
        for chunk in rest.chunks(2) {
            let Ok(score) = parse_f64(&chunk[0]) else {
                return err("ERR value is not a valid float");
            };
            pairs.push((score, chunk[1].clone()));
        }
        match self
            .zsets
            .zadd_with(slot_for_key(&args[1]), &args[1], &pairs, flags)
        {
            // INCR answers the member's new score, or nil when the flags
            // left it alone, as ZINCRBY would have answered.
            Ok(done) if flags.incr => done.score.map_or(Value::Null, Value::Double),
            Ok(done) if ch => Value::Integer((done.added + done.updated) as i64),
            Ok(done) => Value::Integer(done.added as i64),
            Err(e) => store_err(e),
        }
    }

    /// `ZRANGE key start stop [BYSCORE | BYLEX] [REV] [LIMIT offset count]
    /// [WITHSCORES]`, Redis 6.2's form, parsed as Redis's
    /// `zrangeGenericCommand` parses it (BUG-0215: only WITHSCORES was
    /// taken). The options parse first, then the range in the kind they
    /// chose. REV with BYSCORE or BYLEX takes the bounds as (max, min), as
    /// ZREVRANGEBYSCORE does.
    fn cmd_zrange(&self, args: &[Vec<u8>]) -> Value {
        #[derive(PartialEq)]
        enum By {
            Rank,
            Score,
            Lex,
        }
        if args.len() < 4 {
            return arity_err("zrange");
        }
        let (mut by, mut rev, mut withscores, mut limit) = (None, false, false, None);
        let mut i = 4;
        while i < args.len() {
            let after = args.len() - i - 1;
            match args[i].to_ascii_uppercase().as_slice() {
                b"WITHSCORES" => withscores = true,
                b"LIMIT" if after >= 2 => {
                    let (Ok(offset), Ok(count)) =
                        (parse_i64(&args[i + 1]), parse_i64(&args[i + 2]))
                    else {
                        return err("ERR value is not an integer or out of range");
                    };
                    limit = Some((offset, count));
                    i += 2;
                }
                b"REV" if !rev => rev = true,
                b"BYSCORE" if by.is_none() => by = Some(By::Score),
                b"BYLEX" if by.is_none() => by = Some(By::Lex),
                _ => return err("ERR syntax error"),
            }
            i += 1;
        }
        let by = by.unwrap_or(By::Rank);
        // Redis tells LIMIT from its absence by a count other than -1, so a
        // rank range takes `LIMIT <offset> -1` and ignores it, offset and
        // all.
        if limit.is_some_and(|(_, count)| count != -1) && by == By::Rank {
            return err(
                "ERR syntax error, LIMIT is only supported in combination with either BYSCORE \
                 or BYLEX",
            );
        }
        if withscores && by == By::Lex {
            return err("ERR syntax error, WITHSCORES not supported in combination with BYLEX");
        }
        let (slot, key) = (slot_for_key(&args[1]), &args[1]);
        let (lo, hi) = if rev && by != By::Rank {
            (&args[3], &args[2])
        } else {
            (&args[2], &args[3])
        };
        let (offset, count) = limit.unwrap_or((0, -1));
        match by {
            By::Rank => match (parse_i64(lo), parse_i64(hi)) {
                (Ok(start), Ok(stop)) => reply(
                    self.zsets.zrange_rev(slot, key, start, stop, rev),
                    |ranked| Self::zrows(ranked, withscores),
                ),
                _ => err("ERR value is not an integer or out of range"),
            },
            By::Score => {
                let (Some(min), Some(max)) = (ScoreBound::parse(lo), ScoreBound::parse(hi)) else {
                    return err("ERR min or max is not a float");
                };
                reply(
                    self.zsets
                        .zrange_by_score(slot, key, min, max, rev, offset, count),
                    |r| Self::zrows(r, withscores),
                )
            }
            By::Lex => {
                let (Some(min), Some(max)) = (LexBound::parse(lo), LexBound::parse(hi)) else {
                    return err("ERR min or max not valid string range item");
                };
                reply(
                    self.zsets
                        .zrange_by_lex(slot, key, &min, &max, rev, offset, count),
                    |ms| Value::Array(Some(ms.into_iter().map(|m| Value::Bulk(Some(m))).collect())),
                )
            }
        }
    }

    /// The member[/score] rows every ZRANGE-family command replies with.
    ///
    /// WITHSCORES is not a flag on an array of strings: RESP2 interleaves
    /// members and scores, RESP3 nests each as its own pair with a real
    /// double. Saying `ScorePairs` here states which of those we mean once,
    /// and lets the encoder render whichever the connection asked for.
    fn zrows(ranked: Vec<(Vec<u8>, f64)>, withscores: bool) -> Value {
        if withscores {
            return Value::ScorePairs(ranked);
        }
        Value::Array(Some(
            ranked
                .into_iter()
                .map(|(member, _)| Value::Bulk(Some(member)))
                .collect(),
        ))
    }

    fn cmd_zrange_idx(&self, args: &[Vec<u8>], name: &str, rev: bool) -> Value {
        let withscores = match args.len() {
            4 => false,
            5 if args[4].eq_ignore_ascii_case(b"WITHSCORES") => true,
            5 => return err("ERR syntax error"),
            _ => return arity_err(name),
        };
        match (parse_i64(&args[2]), parse_i64(&args[3])) {
            (Ok(start), Ok(stop)) => reply(
                self.zsets
                    .zrange_rev(slot_for_key(&args[1]), &args[1], start, stop, rev),
                |r| Self::zrows(r, withscores),
            ),
            _ => err("ERR value is not an integer or out of range"),
        }
    }

    /// ZRANGEBYSCORE / ZREVRANGEBYSCORE key min max `[WITHSCORES]`
    /// [LIMIT offset count]. The reversed form takes (max, min).
    fn cmd_zrangebyscore(&self, args: &[Vec<u8>], name: &str, rev: bool) -> Value {
        if args.len() < 4 {
            return arity_err(name);
        }
        let (lo_raw, hi_raw) = if rev {
            (&args[3], &args[2])
        } else {
            (&args[2], &args[3])
        };
        let (Some(min), Some(max)) = (ScoreBound::parse(lo_raw), ScoreBound::parse(hi_raw)) else {
            return err("ERR min or max is not a float");
        };
        let mut withscores = false;
        let (mut offset, mut count) = (0i64, -1i64);
        let mut i = 4;
        while i < args.len() {
            match args[i].to_ascii_uppercase().as_slice() {
                b"WITHSCORES" => withscores = true,
                b"LIMIT" => {
                    let (Some(o), Some(c)) = (args.get(i + 1), args.get(i + 2)) else {
                        return err("ERR syntax error");
                    };
                    let (Ok(o), Ok(c)) = (parse_i64(o), parse_i64(c)) else {
                        return err("ERR value is not an integer or out of range");
                    };
                    offset = o;
                    count = c;
                    i += 2;
                }
                _ => return err("ERR syntax error"),
            }
            i += 1;
        }
        reply(
            self.zsets.zrange_by_score(
                slot_for_key(&args[1]),
                &args[1],
                min,
                max,
                rev,
                offset,
                count,
            ),
            |r| Self::zrows(r, withscores),
        )
    }

    /// ZRANGEBYLEX / ZREVRANGEBYLEX key min max [LIMIT offset count].
    /// The reversed form takes (max, min), as the score forms do.
    ///
    /// No WITHSCORES here: the lex family exists for sets whose scores are
    /// all equal, so a score column would be a constant. Redis does not
    /// accept it either.
    fn cmd_zrangebylex(&self, args: &[Vec<u8>], name: &str, rev: bool) -> Value {
        if args.len() < 4 {
            return arity_err(name);
        }
        let (lo_raw, hi_raw) = if rev {
            (&args[3], &args[2])
        } else {
            (&args[2], &args[3])
        };
        let (Some(min), Some(max)) = (LexBound::parse(lo_raw), LexBound::parse(hi_raw)) else {
            return err("ERR min or max not valid string range item");
        };
        let (mut offset, mut count) = (0i64, -1i64);
        let mut i = 4;
        while i < args.len() {
            match args[i].to_ascii_uppercase().as_slice() {
                b"LIMIT" => {
                    let (Some(o), Some(c)) = (args.get(i + 1), args.get(i + 2)) else {
                        return err("ERR syntax error");
                    };
                    let (Ok(o), Ok(c)) = (parse_i64(o), parse_i64(c)) else {
                        return err("ERR value is not an integer or out of range");
                    };
                    offset = o;
                    count = c;
                    i += 2;
                }
                // Not a generic syntax error: WITHSCORES is the option a
                // client reaches for by habit after the score forms, and
                // upstream spells out why it cannot apply here. Worth
                // copying verbatim — the generic message would leave the
                // caller re-reading their own argument list.
                b"WITHSCORES" => {
                    return err(
                        "ERR syntax error, WITHSCORES not supported in combination with BYLEX",
                    );
                }
                _ => return err("ERR syntax error"),
            }
            i += 1;
        }
        reply(
            self.zsets.zrange_by_lex(
                slot_for_key(&args[1]),
                &args[1],
                &min,
                &max,
                rev,
                offset,
                count,
            ),
            |ms| Value::Array(Some(ms.into_iter().map(|m| Value::Bulk(Some(m))).collect())),
        )
    }

    /// COPY source destination `[REPLACE]`. Same slot only, for the reason
    /// the set operations are: the destination is written into this node's
    /// local rows, so a destination in a slot this node does not own would
    /// be stored where nothing will ever read it and COPY would return 1
    /// having created nothing anybody can find. A wrong answer, not a slow
    /// one — refuse it.
    ///
    /// DB is accepted only as `DB 0`. A namespace has exactly one logical
    /// database, so index 0 names the one the client is already in and the
    /// option is a no-op worth tolerating — clients emit it. Any other index
    /// names a database that does not exist here, and Valkey's own answer
    /// for an index it cannot reach is "DB index is out of range", so that
    /// is the answer. Silently copying into db 0 instead would be the real
    /// hazard: the caller believes the data went somewhere else.
    /// The CROSSSLOT refusal every multi-key command here shares. `first`
    /// is the key whose slot the request is judged against — the
    /// destination for the writing forms, since that is also what the proxy
    /// routes by.
    fn crossslot(first: &[u8], others: &[Vec<u8>]) -> Option<Value> {
        let slot = slot_for_key(first);
        let bad = others.iter().find(|k| slot_for_key(k) != slot)?;
        Some(Value::Error(format!(
            "CROSSSLOT Keys in request don't hash to the same slot ({} is slot {}, \
             {} is slot {}) — use a hash tag such as {{tag}}key to colocate them",
            String::from_utf8_lossy(first),
            slot,
            String::from_utf8_lossy(bad),
            slot_for_key(bad)
        )))
    }

    /// RENAME / RENAMENX key newkey. Same slot, and here that is not merely
    /// a correctness rule but the only thing that makes the command cheap
    /// enough to offer: across slots it would be a cross-node move.
    fn cmd_rename(&self, args: &[Vec<u8>], name: &str, nx: bool) -> Value {
        if args.len() != 3 {
            return arity_err(name);
        }
        let (src, dst) = (&args[1], &args[2]);
        if let Some(e) = Self::crossslot(src, &args[2..3]) {
            return e;
        }
        match self.keyspace.rename(slot_for_key(src), src, dst, nx) {
            RenameOutcome::NoSuchKey => err("ERR no such key"),
            // RENAMENX answers 0/1; RENAME cannot reach DestinationExists
            // unless it was asked to rename a key onto itself, which is a
            // no-op success upstream.
            RenameOutcome::DestinationExists if nx => Value::Integer(0),
            RenameOutcome::DestinationExists => Value::Simple("OK".into()),
            RenameOutcome::Renamed if nx => Value::Integer(1),
            RenameOutcome::Renamed => Value::Simple("OK".into()),
        }
    }

    /// SINTERSTORE / SUNIONSTORE / SDIFFSTORE dst key [key ...].
    ///
    /// No numkeys here, unlike the sorted-set forms — the destination is
    /// `args[1]` and everything after it is a source.
    ///
    /// AND UNLIKE THEM, A SORTED SET IS NOT A LEGAL INPUT: ZUNIONSTORE
    /// accepts a plain set at score 1, but the set commands answer
    /// WRONGTYPE for a zset. The asymmetry is upstream's, verified against a
    /// live server rather than assumed symmetric, and `smembers` already
    /// enforces it by type-checking what it reads.
    fn cmd_sstore(&self, args: &[Vec<u8>], name: &str, op: flint_storage::sets::SetOp) -> Value {
        if args.len() < 3 {
            return arity_err(name);
        }
        let dst = &args[1];
        let keys = &args[2..];
        if let Some(e) = Self::crossslot(dst, keys) {
            return e;
        }
        let slot = slot_for_key(dst);
        // Read every source before touching the destination: the
        // destination is allowed to be one of them.
        let members = match self.sets.sop(slot, op, keys) {
            Ok(m) => m,
            Err(e) => return store_err(e),
        };
        reply(self.sets.sreplace(slot, dst, &members), |n| {
            Value::Integer(n as i64)
        })
    }

    /// ZLEXCOUNT / ZREMRANGEBYLEX key min max.
    fn cmd_zlexrange(&self, args: &[Vec<u8>], name: &str, remove: bool) -> Value {
        if args.len() != 4 {
            return arity_err(name);
        }
        let (Some(min), Some(max)) = (LexBound::parse(&args[2]), LexBound::parse(&args[3])) else {
            return err("ERR min or max not valid string range item");
        };
        let slot = slot_for_key(&args[1]);
        let outcome = if remove {
            self.zsets.zremrangebylex(slot, &args[1], &min, &max)
        } else {
            self.zsets.zlexcount(slot, &args[1], &min, &max)
        };
        reply(outcome, |n| Value::Integer(n as i64))
    }

    fn cmd_copy(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 3 {
            return arity_err("copy");
        }
        let mut replace = false;
        let mut i = 3;
        while i < args.len() {
            match args[i].to_ascii_uppercase().as_slice() {
                b"REPLACE" => replace = true,
                b"DB" => {
                    let Some(raw) = args.get(i + 1) else {
                        return err("ERR syntax error");
                    };
                    let Ok(n) = parse_i64(raw) else {
                        return err("ERR value is not an integer or out of range");
                    };
                    if n != 0 {
                        return err("ERR DB index is out of range");
                    }
                    i += 1;
                }
                _ => return err("ERR syntax error"),
            }
            i += 1;
        }
        let (src, dst) = (&args[1], &args[2]);
        let slot = slot_for_key(src);
        if slot_for_key(dst) != slot {
            return Value::Error(format!(
                "CROSSSLOT Keys in request don't hash to the same slot ({} is slot {}, \
                 {} is slot {}) — use a hash tag such as {{tag}}key to colocate them",
                String::from_utf8_lossy(src),
                slot,
                String::from_utf8_lossy(dst),
                slot_for_key(dst)
            ));
        }
        // Copying a key onto itself is an ERROR upstream, not a quiet 0 —
        // verified against Valkey rather than inferred, because the two are
        // easy to confuse and only one of them tells the caller they wrote a
        // command that cannot mean anything.
        //
        // The store must not see this case at all. With REPLACE it would
        // delete the destination's metadata first, which here IS the
        // source's, and for a collection it would then re-key every row to a
        // fresh version under the same name: the key survives, but its whole
        // contents are duplicated on disk until the sweeper catches up.
        if src == dst {
            return err("ERR source and destination objects are the same");
        }
        Value::Integer(self.keyspace.copy(slot, src, dst, replace) as i64)
    }

    /// One input to ZUNIONSTORE / ZINTERSTORE, read as (member, score).
    ///
    /// A plain SET is a legal input and contributes score 1 per member —
    /// which is why this dispatches on the stored type rather than simply
    /// asking the zset store. Anything else is WRONGTYPE; a missing key is
    /// the empty input, not an error.
    fn zstore_source(&self, slot: u16, key: &[u8]) -> Result<Vec<(Vec<u8>, f64)>, StoreError> {
        use flint_storage::encoding::ValueType as VT;
        match self.keyspace.value_type(slot, key) {
            None => Ok(Vec::new()),
            Some(VT::ZSet) => self.zsets.zrange(slot, key, 0, -1),
            Some(VT::Set) => Ok(self
                .sets
                .smembers(slot, key)?
                .into_iter()
                .map(|m| (m, 1.0))
                .collect()),
            Some(_) => Err(StoreError::WrongType),
        }
    }

    /// ZUNIONSTORE / ZINTERSTORE dst numkeys key [key ...]
    /// [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX].
    ///
    /// Same slot as ever, and here it covers the DESTINATION too: this
    /// writes, so a destination in a slot the node does not own would be
    /// stored where nothing can read it while the reply claimed a
    /// cardinality. The proxy routes by the first key, which for these is
    /// the destination — correct precisely because every key shares its slot.
    ///
    /// TWO PLACES A NaN CAN APPEAR, both of which upstream turns into 0 and
    /// neither of which is guesswork — they were confirmed against a live
    /// server: `0 * inf` when a weight zeroes an infinite score, and
    /// `+inf + -inf` when SUM meets both infinities. Left alone, a NaN score
    /// would encode and then order unpredictably against every other member.
    fn cmd_zstore(&self, args: &[Vec<u8>], name: &str, inter: bool) -> Value {
        use std::collections::HashMap;
        use std::collections::hash_map::Entry;

        if args.len() < 4 {
            return arity_err(name);
        }
        let Ok(declared) = parse_i64(&args[2]) else {
            return err("ERR value is not an integer or out of range");
        };
        if declared <= 0 {
            return err(&format!(
                "ERR at least 1 input key is needed for '{name}' command"
            ));
        }
        // Compare against what is actually there before widening: a huge
        // declared count must not become an in-bounds index by wrapping.
        let numkeys = declared as usize;
        if numkeys > args.len() - 3 {
            return err("ERR syntax error");
        }
        let keys = &args[3..3 + numkeys];

        let mut weights = vec![1.0f64; numkeys];
        let mut aggregate = b"SUM".to_vec();
        let mut i = 3 + numkeys;
        while i < args.len() {
            match args[i].to_ascii_uppercase().as_slice() {
                b"WEIGHTS" => {
                    // Exactly one weight per key. A short list is a syntax
                    // error rather than a padded-with-ones convenience: the
                    // caller has miscounted, and quietly filling in 1.0 would
                    // produce a plausible wrong answer.
                    if args.len() - i - 1 < numkeys {
                        return err("ERR syntax error");
                    }
                    for (n, w) in weights.iter_mut().enumerate() {
                        let Ok(v) = parse_f64(&args[i + 1 + n]) else {
                            return err("ERR weight value is not a float");
                        };
                        *w = v;
                    }
                    i += numkeys;
                }
                b"AGGREGATE" => {
                    let Some(kind) = args.get(i + 1) else {
                        return err("ERR syntax error");
                    };
                    aggregate = kind.to_ascii_uppercase();
                    if !matches!(aggregate.as_slice(), b"SUM" | b"MIN" | b"MAX") {
                        return err("ERR syntax error");
                    }
                    i += 1;
                }
                _ => return err("ERR syntax error"),
            }
            i += 1;
        }

        let dst = &args[1];
        let slot = slot_for_key(dst);
        if let Some(bad) = keys.iter().find(|k| slot_for_key(k) != slot) {
            return Value::Error(format!(
                "CROSSSLOT Keys in request don't hash to the same slot ({} is slot {}, \
                 {} is slot {}) — use a hash tag such as {{tag}}key to colocate them",
                String::from_utf8_lossy(dst),
                slot,
                String::from_utf8_lossy(bad),
                slot_for_key(bad)
            ));
        }

        let combine = |a: f64, b: f64| -> f64 {
            let v = match aggregate.as_slice() {
                b"MIN" => a.min(b),
                b"MAX" => a.max(b),
                _ => a + b,
            };
            if v.is_nan() { 0.0 } else { v }
        };

        // EVERY source is read before anything is written, because the
        // destination is allowed to be one of them: ZUNIONSTORE k 2 k other
        // is legal and must fold k's own contents in before k is replaced.
        let mut acc: HashMap<Vec<u8>, f64> = HashMap::new();
        for (n, key) in keys.iter().enumerate() {
            let members = match self.zstore_source(slot, key) {
                Ok(m) => m,
                Err(e) => return store_err(e),
            };
            let weighted = members.into_iter().map(|(m, s)| {
                let v = s * weights[n];
                (m, if v.is_nan() { 0.0 } else { v })
            });
            if n == 0 {
                acc = weighted.collect();
                continue;
            }
            if inter {
                // Intersection keeps only what survived every earlier input,
                // so it is rebuilt each round rather than pruned in place.
                let mut next = HashMap::with_capacity(acc.len());
                for (m, v) in weighted {
                    if let Some(prev) = acc.get(&m) {
                        next.insert(m, combine(*prev, v));
                    }
                }
                acc = next;
                if acc.is_empty() {
                    break;
                }
            } else {
                for (m, v) in weighted {
                    match acc.entry(m) {
                        Entry::Occupied(mut e) => {
                            let merged = combine(*e.get(), v);
                            e.insert(merged);
                        }
                        // A member absent from the accumulator so far takes
                        // its own weighted score: there is nothing to
                        // aggregate it against yet.
                        Entry::Vacant(e) => {
                            e.insert(v);
                        }
                    }
                }
            }
        }

        let pairs: Vec<(f64, Vec<u8>)> = acc.into_iter().map(|(m, s)| (s, m)).collect();
        reply(self.zsets.zreplace(slot, dst, &pairs), |n| {
            Value::Integer(n as i64)
        })
    }

    /// `ZRANK key member [WITHSCORE]` and ZREVRANK. WITHSCORE (Redis 7.2)
    /// answers `[rank, score]`, and a missing member a null array
    /// (BUG-0215: it was an arity error).
    fn cmd_zrank(&self, args: &[Vec<u8>], name: &str, rev: bool) -> Value {
        let withscore = match args.len() {
            3 => false,
            4 if args[3].eq_ignore_ascii_case(b"WITHSCORE") => true,
            4 => return err("ERR syntax error"),
            _ => return arity_err(name),
        };
        let (slot, key, member) = (slot_for_key(&args[1]), &args[1], &args[2]);
        let rank = match self.zsets.zrank(slot, key, member, rev) {
            Ok(Some(rank)) => rank as i64,
            Ok(None) if withscore => return Value::Array(None),
            Ok(None) => return Value::Bulk(None),
            Err(e) => return store_err(e),
        };
        if !withscore {
            return Value::Integer(rank);
        }
        match self.zsets.zscore(slot, key, member) {
            Ok(Some(score)) => Value::Array(Some(vec![Value::Integer(rank), Value::Double(score)])),
            Ok(None) => Value::Array(None),
            Err(e) => store_err(e),
        }
    }

    fn cmd_zcount(&self, args: &[Vec<u8>]) -> Value {
        exact(args, 4, "zcount", |a| {
            let (Some(min), Some(max)) = (ScoreBound::parse(&a[2]), ScoreBound::parse(&a[3]))
            else {
                return err("ERR min or max is not a float");
            };
            reply(
                self.zsets.zcount(slot_for_key(&a[1]), &a[1], min, max),
                |n| Value::Integer(n as i64),
            )
        })
    }

    fn cmd_zmscore(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 3 {
            return arity_err("zmscore");
        }
        reply(
            self.zsets
                .zmscore(slot_for_key(&args[1]), &args[1], &args[2..]),
            |scores| {
                Value::Array(Some(
                    scores
                        .into_iter()
                        .map(|s| s.map(Value::Double).unwrap_or(Value::Null))
                        .collect(),
                ))
            },
        )
    }

    /// ZPOPMIN/ZPOPMAX key `[count]`.
    ///
    /// Whether a COUNT was written changes the reply's shape, not just its
    /// length: without one the reply is a single flat `[member, score]`,
    /// with one it is a list of pairs — and under RESP3 those are visibly
    /// different frames (`*2` of member+double vs `*n` of `*2`s). So the
    /// presence of the argument has to survive to the reply, which is why
    /// it is tracked separately from the count itself.
    fn cmd_zpop(&self, args: &[Vec<u8>], name: &str, max_end: bool) -> Value {
        let (count, counted) = match args.len() {
            2 => (1usize, false),
            3 => match parse_i64(&args[2]) {
                Ok(n) if n >= 0 => (n as usize, true),
                Ok(_) => return err("ERR value is out of range, must be positive"),
                Err(_) => return err("ERR value is not an integer or out of range"),
            },
            _ => return arity_err(name),
        };
        reply(
            self.zsets
                .zpop(slot_for_key(&args[1]), &args[1], count, max_end),
            |r| match counted {
                true => Value::ScorePairs(r),
                // The bare form flattens the single row it popped (and is
                // simply empty when the key was).
                false => Value::Array(Some(
                    r.into_iter()
                        .flat_map(|(m, sc)| [Value::Bulk(Some(m)), Value::Double(sc)])
                        .collect(),
                )),
            },
        )
    }

    fn cmd_zremrangebyscore(&self, args: &[Vec<u8>]) -> Value {
        exact(args, 4, "zremrangebyscore", |a| {
            let (Some(min), Some(max)) = (ScoreBound::parse(&a[2]), ScoreBound::parse(&a[3]))
            else {
                return err("ERR min or max is not a float");
            };
            reply(
                self.zsets
                    .zremrangebyscore(slot_for_key(&a[1]), &a[1], min, max),
                |n| Value::Integer(n as i64),
            )
        })
    }

    fn cmd_zremrangebyrank(&self, args: &[Vec<u8>]) -> Value {
        exact(args, 4, "zremrangebyrank", |a| {
            match (parse_i64(&a[2]), parse_i64(&a[3])) {
                (Ok(start), Ok(stop)) => reply(
                    self.zsets
                        .zremrangebyrank(slot_for_key(&a[1]), &a[1], start, stop),
                    |n| Value::Integer(n as i64),
                ),
                _ => err("ERR value is not an integer or out of range"),
            }
        })
    }

    /// The JSON family's shared preamble: parse the path argument (absent
    /// = the legacy root, like Redis) and load the live document. Returns
    /// the parsed path plus the parsed document, or the error reply to send.
    fn json_open(
        &self,
        key: &[u8],
        path_arg: Option<&Vec<u8>>,
    ) -> Result<(crate::json_path::Path, Option<serde_json::Value>), Value> {
        let raw = path_arg.map(|p| String::from_utf8_lossy(p).to_string());
        // No path argument means the LEGACY root, not `$`: `JSON.GET key`
        // must answer the document, not a container holding it.
        let path = match crate::json_path::parse(raw.as_deref().unwrap_or(".")) {
            Ok(p) => p,
            Err(crate::json_path::PathError::Unsupported) => {
                return Err(err(UNSUPPORTED_PATH));
            }
            Err(crate::json_path::PathError::Malformed) => {
                return Err(err("ERR malformed JSON path"));
            }
        };
        let bytes = match self.json.get(slot_for_key(key), key) {
            Ok(b) => b,
            Err(e) => return Err(store_err(e)),
        };
        let doc = match bytes {
            None => None,
            Some(b) => match serde_json::from_slice(&b) {
                Ok(v) => Some(v),
                // A row that fails to parse is corruption, not a user error.
                Err(_) => return Err(err("ERR stored document is not valid JSON")),
            },
        };
        Ok((path, doc))
    }

    /// Serialize a JSON value into a bulk reply.
    fn json_bulk(v: &serde_json::Value) -> Value {
        match serde_json::to_vec(v) {
            Ok(b) => Value::Bulk(Some(b)),
            Err(_) => err("ERR could not serialize value"),
        }
    }

    /// Shape a value-returning JSON reply for the caller's dialect.
    ///
    /// The commands whose reply IS a JSON document (GET, NUMINCRBY) carry
    /// their matches inside the serialized JSON — `[1]`, `[null]`, `[]` —
    /// while the ones that reply in RESP terms (TYPE, ARRLEN, ARRAPPEND) use
    /// a RESP array instead; `json_resp_matches` below is that variant.
    /// Both are RedisJSON's shapes, verified against the module.
    ///
    /// `found` is a definite path's one match, `Some(None)` means
    /// "the path matched, but the value is not what this command operates
    /// on" — a non-array for ARRLEN, a non-number for NUMINCRBY. Legacy
    /// callers get an error there; JSONPath callers get a null element,
    /// because in a multi-match world one bad match must not fail the rest.
    fn json_doc_matches(
        path: &crate::json_path::Path,
        found: Option<Option<serde_json::Value>>,
        legacy_err: &str,
    ) -> Value {
        match (path.is_jsonpath(), found) {
            (true, Some(Some(v))) => Self::json_bulk(&serde_json::json!([v])),
            (true, Some(None)) => Self::json_bulk(&serde_json::json!([serde_json::Value::Null])),
            (true, None) => Self::json_bulk(&serde_json::json!([])),
            (false, Some(Some(v))) => Self::json_bulk(&v),
            (false, _) => err(legacy_err),
        }
    }

    /// The RESP-array counterpart of [`Self::json_doc_matches`], for the
    /// commands that answer in RESP types rather than serialized JSON.
    fn json_resp_matches(
        path: &crate::json_path::Path,
        found: Option<Option<Value>>,
        legacy_err: &str,
    ) -> Value {
        match (path.is_jsonpath(), found) {
            (true, Some(Some(v))) => Value::Array(Some(vec![v])),
            (true, Some(None)) => Value::Array(Some(vec![Value::Bulk(None)])),
            (true, None) => Value::Array(Some(Vec::new())),
            (false, Some(Some(v))) => v,
            (false, _) => err(legacy_err),
        }
    }

    /// ADR-0054: the values an indefinite path selects, in document order,
    /// duplicates kept (a read of `$[0,0]` answers the element twice).
    fn json_selected<'d>(
        doc: &'d serde_json::Value,
        path: &crate::json_path::Path,
    ) -> Vec<&'d serde_json::Value> {
        crate::json_path::select(doc, path)
            .into_iter()
            .filter_map(|loc| crate::json_path::get(doc, &crate::json_path::Path::internal(loc)))
            .collect()
    }

    /// ADR-0054: the distinct locations JSON.SET and JSON.DEL act on, first
    /// occurrence first: `$[0,0]` deletes element 0 once, as RedisJSON
    /// does. (NUMINCRBY and ARRAPPEND act once per occurrence, as it does.)
    fn json_targets(locs: Vec<Vec<crate::json_path::Step>>) -> Vec<Vec<crate::json_path::Step>> {
        let mut seen = std::collections::HashSet::new();
        locs.into_iter()
            .filter(|l| seen.insert(l.clone()))
            .collect()
    }

    /// Persist a mutated document, preserving any TTL (a sub-document write
    /// is an in-place mutation, not a fresh key).
    fn json_save(&self, key: &[u8], doc: &serde_json::Value) -> Option<Value> {
        let Ok(bytes) = serde_json::to_vec(doc) else {
            return Some(err("ERR could not serialize document"));
        };
        match self.json.set(slot_for_key(key), key, &bytes) {
            Ok(()) => None,
            Err(e) => Some(store_err(e)),
        }
    }

    /// JSON.SET key path value [NX|XX] — writes a document or a path within
    /// one. Root path on a missing key creates the document; a sub-path
    /// requires the key AND the path's parent to exist (no silent creation
    /// of intermediate levels).
    fn cmd_json_set(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 4 || args.len() > 5 {
            return arity_err("json.set");
        }
        let (nx, xx) = match args.get(4).map(|f| f.to_ascii_uppercase()) {
            None => (false, false),
            Some(f) if f == b"NX" => (true, false),
            Some(f) if f == b"XX" => (false, true),
            Some(_) => return err("ERR syntax error"),
        };
        let Ok(value): Result<serde_json::Value, _> = serde_json::from_slice(&args[3]) else {
            return err("ERR value is not valid JSON");
        };
        let (path, mut doc) = match self.json_open(&args[1], Some(&args[2])) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        match Self::json_set_in(&mut doc, &path, value, nx, xx) {
            Err(reply) => reply,
            Ok(false) => Value::Bulk(None),
            Ok(true) => {
                let doc = doc.as_ref().expect("a write leaves a document");
                // Every document write, the root's included, is an in-place
                // mutation of the key, so the TTL survives: RedisJSON's
                // behavior, and the safe direction for a cache, where
                // clearing it would quietly make an expiring document
                // immortal.
                match self.json_save(&args[1], doc) {
                    Some(e) => e,
                    None => Value::Simple("OK".into()),
                }
            }
        }
    }

    /// JSON.SET's rules applied to a document in hand (`None` for a missing
    /// key), shared with JSON.MSET. `Ok(true)` when it wrote, `Ok(false)`
    /// when NX or XX declined (nil), or the error to answer.
    pub(super) fn json_set_in(
        doc: &mut Option<serde_json::Value>,
        path: &crate::json_path::Path,
        value: serde_json::Value,
        nx: bool,
        xx: bool,
    ) -> Result<bool, Value> {
        // Whole-document write: the key itself is the NX/XX subject.
        if path.is_root() {
            if (nx && doc.is_some()) || (xx && doc.is_none()) {
                return Ok(false);
            }
            *doc = Some(value);
            return Ok(true);
        }
        let Some(doc) = doc else {
            return Err(err("ERR new objects must be created at the root"));
        };
        // ADR-0054, an indefinite path: each location it selects is
        // replaced, and nothing is created. RedisJSON adds a value only on
        // a path that names one location, so an indefinite path matching
        // nothing is refused there, and so is NX, which only ever adds; XX
        // matching nothing is nil. Verified against RedisJSON v8.2.8.
        if path.selectors().is_some() {
            const NOT_ADDED: &str = "ERR a multi-match path replaces existing values and adds none";
            if nx {
                return Err(err(NOT_ADDED));
            }
            let locs = Self::json_targets(crate::json_path::select(doc, path));
            if locs.is_empty() {
                return if xx { Ok(false) } else { Err(err(NOT_ADDED)) };
            }
            // Innermost first: a replaced ancestor would otherwise leave a
            // descendant's location pointing into the new value. The final
            // document is the same either way.
            for loc in locs.into_iter().rev() {
                let at = crate::json_path::Path::internal(loc);
                if let Some(slot) = crate::json_path::get_mut(doc, &at) {
                    *slot = value.clone();
                }
            }
            return Ok(true);
        }
        // NX/XX on a sub-path test the PATH's existence.
        let exists = crate::json_path::get(doc, path).is_some();
        if (nx && exists) || (xx && !exists) {
            return Ok(false);
        }
        match crate::json_path::set(doc, path, value) {
            crate::json_path::SetOutcome::Set | crate::json_path::SetOutcome::Created => Ok(true),
            crate::json_path::SetOutcome::MissingParent => Err(err(
                "ERR path parent does not exist (intermediate levels are not created)",
            )),
            crate::json_path::SetOutcome::ShapeMismatch => Err(err(
                "ERR path does not fit the document's shape at that position",
            )),
        }
    }

    /// JSON.DEL key `[path]` — root path deletes the key; a sub-path removes
    /// that member/element, and every location a multi-match path selects.
    /// Returns the number of locations deleted.
    fn cmd_json_del(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 2 || args.len() > 3 {
            return arity_err("json.del");
        }
        let (path, doc) = match self.json_open(&args[1], args.get(2)) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let key = &args[1];
        let Some(mut doc) = doc else {
            return Value::Integer(0);
        };
        if path.is_root() {
            return match self.json.delete(slot_for_key(key), key) {
                Ok(true) => Value::Integer(1),
                Ok(false) => Value::Integer(0),
                Err(e) => store_err(e),
            };
        }
        // ADR-0054: every selected location, counted. A location beneath
        // another selected one goes with it and is not counted twice, and
        // removal runs last-first so no removal shifts an index still to
        // come. The root is never a member's removal (`$` deletes the key).
        if path.selectors().is_some() {
            let mut locs = Self::json_targets(crate::json_path::select(&doc, &path));
            locs.retain(|l| !l.is_empty());
            let all = locs.clone();
            locs.retain(|l| !all.iter().any(|a| a.len() < l.len() && l.starts_with(a)));
            locs.sort();
            let mut removed = 0i64;
            for loc in locs.into_iter().rev() {
                if crate::json_path::remove(&mut doc, &crate::json_path::Path::internal(loc)) {
                    removed += 1;
                }
            }
            if removed == 0 {
                return Value::Integer(0);
            }
            return match self.json_save_or_drop(key, &doc) {
                Some(e) => e,
                None => Value::Integer(removed),
            };
        }
        if !crate::json_path::remove(&mut doc, &path) {
            return Value::Integer(0);
        }
        match self.json_save_or_drop(key, &doc) {
            Some(e) => e,
            None => Value::Integer(1),
        }
    }

    /// Persist a document a JSON.DEL removed from, or delete the key when
    /// the removal left it an empty object or array. BUG-0209: RedisJSON
    /// deletes the key there (its testDelCommand asserts it), and Flint
    /// kept `{}`, so EXISTS and a later sub-path JSON.SET disagreed.
    fn json_save_or_drop(&self, key: &[u8], doc: &serde_json::Value) -> Option<Value> {
        let emptied = match doc {
            serde_json::Value::Object(m) => m.is_empty(),
            serde_json::Value::Array(a) => a.is_empty(),
            _ => false,
        };
        if !emptied {
            return self.json_save(key, doc);
        }
        self.json
            .delete(slot_for_key(key), key)
            .err()
            .map(store_err)
    }

    /// JSON.TYPE key `[path]` — Redis's type vocabulary for the value at the
    /// path. Nil when the KEY is absent (either dialect).
    fn cmd_json_type(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 2 || args.len() > 3 {
            return arity_err("json.type");
        }
        let (path, doc) = match self.json_open(&args[1], args.get(2)) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        // A missing key's nil takes the RESP3 nesting too (BUG-0210):
        // RedisJSON answers `[null]` there.
        let Some(doc) = doc else {
            return Value::Resp3Nested(Box::new(Value::Bulk(None)));
        };
        // RedisJSON nests this reply one level deeper under RESP3 and
        // redis-py unwraps to match; `Resp3Nested` carries that intent so
        // the RESP2 spelling stays exactly as it was. See
        // `flint_resp::resp3_nests_reply`.
        let nest = |v: Value| Value::Resp3Nested(Box::new(v));
        if path.selectors().is_some() {
            let names = Self::json_selected(&doc, &path)
                .into_iter()
                .map(|v| Value::Bulk(Some(crate::json_path::type_name(v).into())))
                .collect();
            return nest(Value::Array(Some(names)));
        }
        let Some(v) = crate::json_path::get(&doc, &path) else {
            // JSON.TYPE is the one command whose legacy dialect answers NIL
            // rather than an error for a path that matches nothing — asking
            // what type something is and being told "nothing" is an answer,
            // not a failure. Verified against RedisJSON, which is otherwise
            // error-on-no-match for the legacy dialect.
            return nest(match path.is_jsonpath() {
                true => Value::Array(Some(Vec::new())),
                false => Value::Bulk(None),
            });
        };
        let name = Value::Bulk(Some(crate::json_path::type_name(v).into()));
        nest(Self::json_resp_matches(
            &path,
            Some(Some(name)),
            PATH_MISSING,
        ))
    }

    /// BF.RESERVE key error_rate capacity `[EXPANSION` `n]` `[NONSCALING]`
    ///
    /// Note the argument order — error rate BEFORE capacity, which is
    /// RedisBloom's and reads backwards to most people. Kept because the
    /// point of this family is that existing clients work unchanged.
    fn cmd_bf_reserve(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 4 {
            return arity_err("bf.reserve");
        }
        let Ok(error) = parse_f64(&args[2]) else {
            return err("ERR bad error rate");
        };
        let Some(capacity) = parse_u64(&args[3]) else {
            return err("ERR bad capacity");
        };
        let mut expansion = flint_storage::bloom::DEFAULT_EXPANSION;
        let mut i = 4;
        while i < args.len() {
            match args[i].to_ascii_uppercase().as_slice() {
                b"NONSCALING" => {
                    expansion = 0;
                    i += 1;
                }
                b"EXPANSION" => {
                    let Some(n) = args.get(i + 1).and_then(|v| parse_u64(v)) else {
                        return err("ERR bad expansion");
                    };
                    if n == 0 || n > u8::MAX as u64 {
                        return err("ERR bad expansion");
                    }
                    expansion = n as u8;
                    i += 2;
                }
                _ => return err("ERR syntax error"),
            }
        }
        match self
            .bloom
            .reserve(slot_for_key(&args[1]), &args[1], capacity, error, expansion)
        {
            Ok(()) => Value::Simple("OK".into()),
            Err(e) => store_err(e),
        }
    }

    /// BF.MADD key item [item ...] and BF.MEXISTS key item [item ...] —
    /// one reply element per item, in request order.
    fn cmd_bf_multi(&self, name: &[u8], args: &[Vec<u8>]) -> Value {
        if args.len() < 3 {
            return arity_err(if name == b"BF.MADD" {
                "bf.madd"
            } else {
                "bf.mexists"
            });
        }
        let slot = slot_for_key(&args[1]);
        let items = &args[2..];
        let out = if name == b"BF.MADD" {
            self.bloom.madd(slot, &args[1], items)
        } else {
            self.bloom.mexists(slot, &args[1], items)
        };
        match out {
            Ok(v) => Value::Array(Some(
                v.into_iter().map(|b| Value::Integer(b as i64)).collect(),
            )),
            Err(e) => store_err(e),
        }
    }

    /// BF.INFO key [CAPACITY|SIZE|FILTERS|ITEMS|EXPANSION]
    fn cmd_bf_info(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 2 || args.len() > 3 {
            return arity_err("bf.info");
        }
        let info = match self.bloom.info(slot_for_key(&args[1]), &args[1]) {
            Ok(Some(i)) => i,
            Ok(None) => return err("ERR not found"),
            Err(e) => return store_err(e),
        };
        // Expansion 0 means NONSCALING, which RedisBloom reports as a nil
        // rather than a zero — the filter has no growth factor at all.
        let expansion = match info.expansion {
            0 => Value::Bulk(None),
            n => Value::Integer(n as i64),
        };
        if let Some(field) = args.get(2) {
            // ONE-ELEMENT ARRAY, not a bare value. RedisBloom answers
            // `*1\r\n:5000\r\n` to `BF.INFO k CAPACITY`, and a client that
            // indexes [0] — which is what its own libraries do — gets an
            // error against a bare integer instead of a number.
            //
            // Verified on the wire against RedisBloom 2.8.16, not inferred:
            // the nil for a NONSCALING filter is wrapped too (`*1\r\n$-1`),
            // while a bad section name stays a BARE error.
            let one = match field.to_ascii_uppercase().as_slice() {
                b"CAPACITY" => Value::Integer(info.capacity as i64),
                b"SIZE" => Value::Integer(info.size_bytes as i64),
                b"FILTERS" => Value::Integer(info.filters as i64),
                b"ITEMS" => Value::Integer(info.items as i64),
                b"EXPANSION" => expansion,
                // RedisBloom's exact text, which carries no `ERR` code —
                // the first word is the code, as in every RESP error.
                _ => return err("Invalid information value"),
            };
            return Value::Array(Some(vec![one]));
        }
        // SIMPLE strings for the field names, matching RedisBloom on the
        // wire (`+Capacity`, not `$8\r\nCapacity`). Most clients coerce
        // both to a string, so this is not the load-bearing half — but the
        // whole claim of this family is that the bytes match, and a
        // difference nobody can name is the kind that surfaces in one
        // unlucky client a year from now.
        Value::Array(Some(vec![
            Value::Simple("Capacity".into()),
            Value::Integer(info.capacity as i64),
            Value::Simple("Size".into()),
            Value::Integer(info.size_bytes as i64),
            Value::Simple("Number of filters".into()),
            Value::Integer(info.filters as i64),
            Value::Simple("Number of items inserted".into()),
            Value::Integer(info.items as i64),
            Value::Simple("Expansion rate".into()),
            expansion,
        ]))
    }

    /// BF.INSERT key `[CAPACITY` `n]` `[ERROR` `e]` `[EXPANSION` `n]` `[NOCREATE]`
    /// `[NONSCALING]` ITEMS item `[item` `...]`
    ///
    /// Reserve-if-absent and add, in one round trip. The options bind only
    /// when the filter is CREATED here; against an existing filter they are
    /// ignored, exactly as RedisBloom does, because its parameters were
    /// fixed when it was made.
    fn cmd_bf_insert(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 4 {
            return arity_err("bf.insert");
        }
        let mut capacity = flint_storage::bloom::DEFAULT_CAPACITY;
        let mut error = flint_storage::bloom::DEFAULT_ERROR;
        let mut expansion = flint_storage::bloom::DEFAULT_EXPANSION;
        let mut nocreate = false;
        let mut items: Option<&[Vec<u8>]> = None;

        let mut i = 2;
        while i < args.len() {
            match args[i].to_ascii_uppercase().as_slice() {
                b"CAPACITY" => match args.get(i + 1).and_then(|v| parse_u64(v)) {
                    Some(n) => {
                        capacity = n;
                        i += 2;
                    }
                    None => return err("ERR bad capacity"),
                },
                b"ERROR" => match args.get(i + 1).and_then(|v| parse_f64(v).ok()) {
                    Some(e) => {
                        error = e;
                        i += 2;
                    }
                    None => return err("ERR bad error rate"),
                },
                b"EXPANSION" => match args.get(i + 1).and_then(|v| parse_u64(v)) {
                    Some(n) if n > 0 && n <= u8::MAX as u64 => {
                        expansion = n as u8;
                        i += 2;
                    }
                    _ => return err("ERR bad expansion"),
                },
                b"NOCREATE" => {
                    nocreate = true;
                    i += 1;
                }
                b"NONSCALING" => {
                    expansion = 0;
                    i += 1;
                }
                b"ITEMS" => {
                    items = Some(&args[i + 1..]);
                    break;
                }
                _ => return err("ERR syntax error"),
            }
        }
        let Some(items) = items.filter(|i| !i.is_empty()) else {
            return err("ERR syntax error");
        };

        let slot = slot_for_key(&args[1]);
        let exists = match self.bloom.info(slot, &args[1]) {
            Ok(v) => v.is_some(),
            Err(e) => return store_err(e),
        };
        if !exists {
            if nocreate {
                return err("ERR not found");
            }
            if let Err(e) = self
                .bloom
                .reserve(slot, &args[1], capacity, error, expansion)
            {
                return store_err(e);
            }
        }
        match self.bloom.madd(slot, &args[1], items) {
            Ok(v) => Value::Array(Some(
                v.into_iter().map(|b| Value::Integer(b as i64)).collect(),
            )),
            Err(e) => store_err(e),
        }
    }

    /// JSON.NUMINCRBY key path number — atomically add to a number at the
    /// path, replying with the new value.
    fn cmd_json_numop(&self, args: &[Vec<u8>], mult: bool) -> Value {
        if args.len() != 4 {
            return arity_err(if mult {
                "json.nummultby"
            } else {
                "json.numincrby"
            });
        }
        let Some(by) = std::str::from_utf8(&args[3])
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
        else {
            return err("ERR value is not a number");
        };
        // The increment as an exact integer, when it is written as one.
        // `2.0` is a float, so an integer incremented by it becomes one,
        // as in RedisJSON.
        let by = (
            by,
            std::str::from_utf8(&args[3])
                .ok()
                .and_then(|s| s.parse::<i64>().ok()),
        );
        let (path, doc) = match self.json_open(&args[1], Some(&args[2])) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let Some(mut doc) = doc else {
            return err(NO_SUCH_KEY);
        };
        // NUMINCRBY is the ONE command whose two dialects disagree about
        // the KIND of reply, not just its shape: RESP2 answers JSON text
        // (`[6]`), RESP3 answers a typed RESP array (`*1 :6`). So each
        // outcome is built for both and `ByProto` carries the pair.
        let numeric = |v: Option<&serde_json::Value>| -> Value {
            match v {
                Some(n) if n.is_i64() || n.is_u64() => Value::Integer(n.as_i64().unwrap_or(0)),
                Some(n) => Value::Double(n.as_f64().unwrap_or(0.0)),
                None => Value::Null,
            }
        };
        let paired = |resp2: Value, matches: Vec<Value>| Value::ByProto {
            resp2: Box::new(resp2),
            resp3: Box::new(Value::Array(Some(matches))),
        };
        // ADR-0054: each selected number incremented, a null for each
        // selected non-number. A location a union names twice is
        // incremented twice, as RedisJSON does. A refused result fails the
        // whole command, and nothing is saved.
        if path.selectors().is_some() {
            let mut as_json = Vec::new();
            let mut as_resp = Vec::new();
            for loc in crate::json_path::select(&doc, &path) {
                let at = crate::json_path::Path::internal(loc);
                let Some(slot) = crate::json_path::get_mut(&mut doc, &at) else {
                    continue;
                };
                if !slot.is_number() {
                    as_json.push(serde_json::Value::Null);
                    as_resp.push(Value::Null);
                    continue;
                }
                if let Err(e) = Self::json_numop(slot, by, mult) {
                    return e;
                }
                as_resp.push(numeric(Some(slot)));
                as_json.push(slot.clone());
            }
            if as_json.iter().any(|v| !v.is_null())
                && let Some(e) = self.json_save(&args[1], &doc)
            {
                return e;
            }
            return paired(Self::json_bulk(&serde_json::Value::Array(as_json)), as_resp);
        }
        let Some(slot) = crate::json_path::get_mut(&mut doc, &path) else {
            return paired(
                Self::json_doc_matches(&path, None, PATH_MISSING),
                Vec::new(),
            );
        };
        if !slot.is_number() {
            return paired(
                Self::json_doc_matches(&path, Some(None), "ERR path does not hold a number"),
                vec![Value::Null],
            );
        }
        if let Err(e) = Self::json_numop(slot, by, mult) {
            return e;
        }
        let out = slot.clone();
        match self.json_save(&args[1], &doc) {
            Some(e) => e,
            None => paired(
                Self::json_doc_matches(&path, Some(Some(out.clone())), PATH_MISSING),
                vec![numeric(Some(&out))],
            ),
        }
    }

    /// Add `by` to the number in `slot`, or multiply by it (ADR-0055's
    /// NUMMULTBY), with `by` as f64 and as an exact integer when written as
    /// one. An integer and an integer make an integer, in i64 arithmetic:
    /// BUG-0208 worked in f64 and cast back, so `+0` changed an integer
    /// above 2^53 and overflow saturated. Overflow is refused, as Redis's
    /// INCRBY refuses it (RedisJSON wraps); so is a result that is not
    /// finite. A refusal leaves `slot` as it was.
    fn json_numop(
        slot: &mut serde_json::Value,
        by: (f64, Option<i64>),
        mult: bool,
    ) -> Result<(), Value> {
        if let (Some(cur), Some(n)) = (slot.as_i64(), by.1) {
            let next = if mult {
                cur.checked_mul(n)
            } else {
                cur.checked_add(n)
            };
            let Some(next) = next else {
                return Err(err(if mult {
                    "ERR multiplication would overflow"
                } else {
                    "ERR increment or decrement would overflow"
                }));
            };
            *slot = serde_json::Value::from(next);
            return Ok(());
        }
        let cur = slot.as_f64().unwrap_or(0.0);
        let next = if mult { cur * by.0 } else { cur + by.0 };
        if !next.is_finite() {
            return Err(err("ERR result is not a finite number"));
        }
        *slot = match serde_json::Number::from_f64(next) {
            Some(n) => serde_json::Value::Number(n),
            None => return Err(err("ERR result is not representable")),
        };
        Ok(())
    }

    /// JSON.ARRAPPEND key path value [value ...] — append to an array,
    /// replying with the new length.
    fn cmd_json_arrappend(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 4 {
            return arity_err("json.arrappend");
        }
        let mut values = Vec::with_capacity(args.len() - 3);
        for raw in &args[3..] {
            match serde_json::from_slice(raw) {
                Ok(v) => values.push(v),
                Err(_) => return err("ERR value is not valid JSON"),
            }
        }
        let (path, doc) = match self.json_open(&args[1], Some(&args[2])) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let Some(mut doc) = doc else {
            return err(NO_SUCH_KEY);
        };
        // ADR-0054: each selected array extended, its new length answered;
        // nil for each selected non-array. As with NUMINCRBY, a location a
        // union names twice is extended twice, as RedisJSON does.
        if path.selectors().is_some() {
            let mut out = Vec::new();
            let mut changed = false;
            for loc in crate::json_path::select(&doc, &path) {
                let at = crate::json_path::Path::internal(loc);
                match crate::json_path::get_mut(&mut doc, &at) {
                    Some(serde_json::Value::Array(arr)) => {
                        arr.extend(values.iter().cloned());
                        out.push(Value::Integer(arr.len() as i64));
                        changed = true;
                    }
                    _ => out.push(Value::Bulk(None)),
                }
            }
            if changed && let Some(e) = self.json_save(&args[1], &doc) {
                return e;
            }
            return Value::Array(Some(out));
        }
        let Some(target) = crate::json_path::get_mut(&mut doc, &path) else {
            return Self::json_resp_matches(&path, None, PATH_MISSING);
        };
        let serde_json::Value::Array(arr) = target else {
            return Self::json_resp_matches(&path, Some(None), NOT_AN_ARRAY);
        };
        arr.extend(values);
        let len = arr.len() as i64;
        match self.json_save(&args[1], &doc) {
            Some(e) => e,
            None => Self::json_resp_matches(&path, Some(Some(Value::Integer(len))), NOT_AN_ARRAY),
        }
    }

    /// JSON.ARRLEN key `[path]` — length of the array at the path.
    fn cmd_json_arrlen(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 2 || args.len() > 3 {
            return arity_err("json.arrlen");
        }
        let (path, doc) = match self.json_open(&args[1], args.get(2)) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        // A missing key is nil under the legacy dialect and an error under
        // `$`, as RedisJSON answers (BUG-0210).
        let Some(doc) = doc else {
            return if path.is_jsonpath() {
                err(NO_SUCH_KEY)
            } else {
                Value::Bulk(None)
            };
        };
        if path.selectors().is_some() {
            let lens = Self::json_selected(&doc, &path)
                .into_iter()
                .map(|v| match v {
                    serde_json::Value::Array(a) => Value::Integer(a.len() as i64),
                    _ => Value::Bulk(None),
                })
                .collect();
            return Value::Array(Some(lens));
        }
        // The two failure shapes carry different legacy messages, so they
        // are separate calls rather than one folded expression.
        let Some(v) = crate::json_path::get(&doc, &path) else {
            return Self::json_resp_matches(&path, None, PATH_MISSING);
        };
        match v {
            serde_json::Value::Array(a) => Self::json_resp_matches(
                &path,
                Some(Some(Value::Integer(a.len() as i64))),
                NOT_AN_ARRAY,
            ),
            _ => Self::json_resp_matches(&path, Some(None), NOT_AN_ARRAY),
        }
    }

    /// SCAN cursor [MATCH pat] [COUNT n] [TYPE t] — incremental keyspace
    /// iteration over THIS namespace's metadata rows, in (slot, key) order.
    ///
    /// Cursor model: Redis clients (redis-py, go-redis) parse the cursor
    /// with int(), so it MUST be numeric — a position-encoded string cursor
    /// breaks them. A numeric cursor cannot losslessly encode an arbitrary
    /// resume key, so cursors are SERVER-SIDE: a bounded, TTL'd table maps
    /// id -> (ns, resume envelope key). Each batch re-seeks fresh
    /// (`for_each_from`), so the scan holds no iterator open and tolerates
    /// concurrent writes with Redis's weak guarantee (keys present the
    /// whole scan are returned; concurrent adds/removes may or may not
    /// be). An expired or unknown cursor answers "ERR invalid cursor" —
    /// honest truncation, never a silent partial enumeration.
    ///
    /// COUNT bounds rows EXAMINED per batch (default 10, like Redis);
    /// MATCH globs the user key; TYPE filters on the header's value type.
    /// Expired-but-unswept rows are skipped, mirroring DBSIZE.
    fn cmd_scan(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 2 {
            return arity_err("scan");
        }
        let Some(cursor_in) = std::str::from_utf8(&args[1])
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
        else {
            return err("ERR invalid cursor");
        };
        let mut pattern: Option<&[u8]> = None;
        let mut count: usize = 10;
        let mut type_filter: Option<flint_storage::encoding::ValueType> = None;
        let mut i = 2;
        while i < args.len() {
            match args[i].to_ascii_uppercase().as_slice() {
                b"MATCH" => match args.get(i + 1) {
                    Some(p) => {
                        pattern = Some(p);
                        i += 2;
                    }
                    None => return err("ERR syntax error"),
                },
                b"COUNT" => match args.get(i + 1).and_then(|c| parse_i64(c).ok()) {
                    Some(n) if n >= 1 => {
                        count = (n as usize).min(10_000);
                        i += 2;
                    }
                    _ => return err("ERR syntax error"),
                },
                b"TYPE" => match args.get(i + 1).map(|t| t.to_ascii_lowercase()) {
                    Some(t) => {
                        use flint_storage::encoding::ValueType as VT;
                        type_filter = Some(match t.as_slice() {
                            b"string" => VT::String,
                            b"hash" => VT::Hash,
                            b"set" => VT::Set,
                            b"zset" => VT::ZSet,
                            b"list" => VT::List,
                            // An unknown type matches nothing (Redis answers
                            // empty batches, not an error).
                            _ => {
                                return Value::Array(Some(vec![
                                    Value::Bulk(Some(b"0".to_vec())),
                                    Value::Array(Some(Vec::new())),
                                ]));
                            }
                        });
                        i += 2;
                    }
                    None => return err("ERR syntax error"),
                },
                _ => return err("ERR syntax error"),
            }
        }

        // Resolve the resume position. Cursor 0 = a fresh scan; otherwise
        // the table row must exist AND belong to this namespace (a cursor
        // is not transferable across tenants).
        let resume: Vec<u8> = if cursor_in == 0 {
            Vec::new()
        } else {
            match scan_cursors().lock() {
                Ok(map) => match map.get(&cursor_in) {
                    Some(c) if c.ns == self.ns => c.resume.clone(),
                    _ => return err("ERR invalid cursor"),
                },
                Err(_) => return err("ERR cursor table lock"),
            }
        };

        let prefix = self.ns_prefix(flint_storage::encoding::Cf::Metadata);
        let now = (self.clock)();
        let mut keys: Vec<Value> = Vec::new();
        let mut examined = 0usize;
        let mut last: Vec<u8> = Vec::new();
        let mut more = false;
        self.kv.for_each_from(&prefix, &resume, &mut |k, row| {
            if examined == count {
                // One row PAST the budget proves the keyspace continues.
                more = true;
                return false;
            }
            examined += 1;
            last = k.to_vec();
            let Some(h) = flint_storage::encoding::MetaHeader::decode(row) else {
                return true;
            };
            if h.is_expired(now) {
                return true;
            }
            if let Some(want) = type_filter
                && flint_storage::encoding::ValueType::from_flags(h.flags) != Some(want)
            {
                return true;
            }
            // user key = envelope minus (prefix + 2 slot bytes).
            let user = &k[prefix.len() + 2..];
            if pattern.is_none_or(|p| glob_match(p, user)) {
                keys.push(Value::Bulk(Some(user.to_vec())));
            }
            true
        });

        let next = if more {
            let Ok(mut map) = scan_cursors().lock() else {
                return err("ERR cursor table lock");
            };
            // TTL sweep + capacity bound: an abandoned scan must not leak.
            let now_t = std::time::Instant::now();
            map.retain(|_, c| now_t.duration_since(c.at) < SCAN_CURSOR_TTL);
            let id = if cursor_in != 0 && map.contains_key(&cursor_in) {
                cursor_in // continue the same session in place
            } else {
                if map.len() >= SCAN_CURSOR_CAP
                    && let Some((&oldest, _)) = map.iter().min_by_key(|(_, c)| c.at)
                {
                    map.remove(&oldest);
                }
                NEXT_SCAN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            };
            map.insert(
                id,
                ScanCursor {
                    ns: self.ns.clone(),
                    resume: last,
                    at: now_t,
                },
            );
            id
        } else {
            // Scan complete: retire the session row.
            if cursor_in != 0
                && let Ok(mut map) = scan_cursors().lock()
            {
                map.remove(&cursor_in);
            }
            0
        };

        Value::Array(Some(vec![
            Value::Bulk(Some(next.to_string().into_bytes())),
            Value::Array(Some(keys)),
        ]))
    }

    /// HSCAN/SSCAN/ZSCAN key cursor `[MATCH` `pat]` `[COUNT` `n]` `[NOVALUES]`.
    /// Our collections materialize from ONE prefix scan, so every scan is a
    /// single-shot iteration: ignore the cursor's value, return the whole
    /// (filtered) collection, answer cursor "0" — exactly Redis's behavior
    /// for listpack/intset-encoded keys, and a valid SCAN contract (each
    /// element returned once, iteration terminates). COUNT is a hint and is
    /// validated then ignored; NOVALUES is HSCAN-only.
    fn cmd_scan_typed(&self, args: &[Vec<u8>], kind: ScanKind) -> Value {
        let name = match kind {
            ScanKind::Hash => "hscan",
            ScanKind::Set => "sscan",
            ScanKind::ZSet => "zscan",
        };
        if args.len() < 3 {
            return arity_err(name);
        }
        if std::str::from_utf8(&args[2])
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .is_none()
        {
            return err("ERR invalid cursor");
        }
        let mut pattern: Option<&[u8]> = None;
        let mut novalues = false;
        let mut i = 3;
        while i < args.len() {
            match args[i].to_ascii_uppercase().as_slice() {
                b"MATCH" => match args.get(i + 1) {
                    Some(p) => {
                        pattern = Some(p);
                        i += 2;
                    }
                    None => return err("ERR syntax error"),
                },
                b"COUNT" => match args.get(i + 1).and_then(|c| parse_i64(c).ok()) {
                    Some(n) if n >= 1 => i += 2,
                    _ => return err("ERR syntax error"),
                },
                b"NOVALUES" if matches!(kind, ScanKind::Hash) => {
                    novalues = true;
                    i += 1;
                }
                _ => return err("ERR syntax error"),
            }
        }
        let keep = |s: &[u8]| pattern.is_none_or(|p| glob_match(p, s));
        let slot = slot_for_key(&args[1]);
        let items = match kind {
            ScanKind::Hash => match self.hashes.hgetall(slot, &args[1]) {
                Ok(pairs) => pairs
                    .into_iter()
                    .filter(|(f, _)| keep(f))
                    .flat_map(|(f, v)| {
                        if novalues {
                            vec![Value::Bulk(Some(f))]
                        } else {
                            vec![Value::Bulk(Some(f)), Value::Bulk(Some(v))]
                        }
                    })
                    .collect(),
                Err(e) => return store_err(e),
            },
            ScanKind::Set => match self.sets.smembers(slot, &args[1]) {
                Ok(ms) => ms
                    .into_iter()
                    .filter(|m| keep(m))
                    .map(|m| Value::Bulk(Some(m)))
                    .collect(),
                Err(e) => return store_err(e),
            },
            ScanKind::ZSet => match self.zsets.zrange(slot, &args[1], 0, -1) {
                Ok(rows) => rows
                    .into_iter()
                    .filter(|(m, _)| keep(m))
                    .flat_map(|(m, sc)| {
                        vec![
                            Value::Bulk(Some(m)),
                            Value::Bulk(Some(flint_resp::fmt_double(sc))),
                        ]
                    })
                    .collect(),
                Err(e) => return store_err(e),
            },
        };
        Value::Array(Some(vec![
            Value::Bulk(Some(b"0".to_vec())),
            Value::Array(Some(items)),
        ]))
    }

    /// SPOP key `[count]`. Without count: single bulk (or nil). With count:
    /// an array — count 0 is the empty array, negative is an error.
    fn cmd_spop(&self, args: &[Vec<u8>]) -> Value {
        match args.len() {
            2 => match self.sets.spop(slot_for_key(&args[1]), &args[1], 1) {
                Ok(mut popped) => Value::Bulk(popped.pop()),
                Err(e) => store_err(e),
            },
            3 => match parse_i64(&args[2]) {
                Ok(n) if n >= 0 => reply(
                    self.sets.spop(slot_for_key(&args[1]), &args[1], n as u64),
                    |ms| Value::Set(ms.into_iter().map(|m| Value::Bulk(Some(m))).collect()),
                ),
                _ => err("ERR value is out of range, must be positive"),
            },
            _ => arity_err("spop"),
        }
    }

    /// SRANDMEMBER key `[count]`. Without count: single bulk (or nil). With
    /// count: array — positive is distinct-clamped, negative repeats.
    /// `SRANDMEMBER key [count]`. A negative count repeats members, so its
    /// reply is `|count|` members however small the set. That reply was
    /// built in one allocation, and `SRANDMEMBER k -2000000000000` on a
    /// three-member set took the seat down (BUG-0218). It is refused past
    /// the seat's reply ceiling now, and BUG-0060's admission sizes it
    /// (`collection_read_bytes`).
    fn cmd_srandmember(&self, args: &[Vec<u8>]) -> Value {
        match args.len() {
            2 => match self.sets.srandmember(slot_for_key(&args[1]), &args[1], 1) {
                Ok(mut picks) => Value::Bulk(picks.pop()),
                Err(e) => store_err(e),
            },
            3 => {
                let n = match parse_i64(&args[2]) {
                    // Redis's bound is symmetric, so i64::MIN is out of it.
                    Ok(i64::MIN) => return err(OUT_OF_SYMMETRIC_RANGE),
                    Ok(n) => n,
                    Err(_) => return err("ERR value is not an integer or out of range"),
                };
                let slot = slot_for_key(&args[1]);
                if n < 0 {
                    match self.srandmember_reply_bytes(slot, &args[1], n) {
                        Ok(bytes) if bytes > self.reply_ceiling() => {
                            return err(&format!(
                                "ERR SRANDMEMBER with count {n} would build a reply of about \
                                 {bytes} bytes, past this server's limit of {} \
                                 (max-value-bytes); ask for fewer",
                                self.reply_ceiling()
                            ));
                        }
                        Ok(_) => {}
                        Err(e) => return store_err(e),
                    }
                }
                reply(self.sets.srandmember(slot, &args[1], n), |ms| {
                    Value::Array(Some(ms.into_iter().map(|m| Value::Bulk(Some(m))).collect()))
                })
            }
            _ => arity_err("srandmember"),
        }
    }

    /// Bytes `SRANDMEMBER key count` builds for a negative `count`: each of
    /// `|count|` members at the set's mean size, plus what a reply element
    /// costs in memory (`REPLY_ELEMENT_BYTES`). 0 for a missing key.
    fn srandmember_reply_bytes(&self, slot: u16, key: &[u8], n: i64) -> Result<u64, StoreError> {
        let Some(bytes) = self.sets.stored_bytes(slot, key)? else {
            return Ok(0);
        };
        let mean = bytes / self.sets.scard(slot, key)?.max(1);
        Ok(n.unsigned_abs().saturating_mul(mean + REPLY_ELEMENT_BYTES))
    }

    /// The most a single reply may build: the seat's max-value-bytes, the
    /// largest collection it accepts, or its default when that is off.
    fn reply_ceiling(&self) -> u64 {
        match self.limits.max_value_bytes {
            0 => flint_storage::DEFAULT_MAX_VALUE_BYTES,
            max => max,
        }
    }

    /// LPOS key element [RANK rank] [COUNT num] [MAXLEN len]. Without COUNT
    /// the reply is a single index (or nil); with COUNT it is an array —
    /// COUNT 0 means every match.
    fn cmd_lpos(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 3 {
            return arity_err("lpos");
        }
        let mut rank: i64 = 1;
        let mut count: Option<u64> = None;
        let mut maxlen: u64 = 0;
        let mut i = 3;
        while i < args.len() {
            let Some(val) = args.get(i + 1) else {
                return err("ERR syntax error");
            };
            match args[i].to_ascii_uppercase().as_slice() {
                b"RANK" => match parse_i64(val) {
                    Ok(0) => {
                        return err(
                            "ERR RANK can't be zero: use 1 to start from the first match, 2 \
                             from the second ... or use negative to start from the end of the \
                             list",
                        );
                    }
                    // Redis's bound is symmetric, so i64::MIN is out of it.
                    Ok(i64::MIN) => return err(OUT_OF_SYMMETRIC_RANGE),
                    Ok(r) => rank = r,
                    Err(_) => return err("ERR value is not an integer or out of range"),
                },
                b"COUNT" => match parse_i64(val) {
                    Ok(c) if c >= 0 => count = Some(c as u64),
                    _ => return err("ERR COUNT can't be negative"),
                },
                b"MAXLEN" => match parse_i64(val) {
                    Ok(m) if m >= 0 => maxlen = m as u64,
                    _ => return err("ERR MAXLEN can't be negative"),
                },
                _ => return err("ERR syntax error"),
            }
            i += 2;
        }
        // No COUNT still needs just one hit; COUNT 0 lifts the cap.
        let cap = count.unwrap_or(1);
        match self.lists.lpos(
            slot_for_key(&args[1]),
            &args[1],
            &args[2],
            rank,
            cap,
            maxlen,
        ) {
            Ok(hits) => {
                if count.is_none() {
                    match hits.first() {
                        Some(&idx) => Value::Integer(idx),
                        None => Value::Bulk(None),
                    }
                } else {
                    Value::Array(Some(hits.into_iter().map(Value::Integer).collect()))
                }
            }
            Err(e) => store_err(e),
        }
    }

    fn cmd_expire(&self, args: &[Vec<u8>], name: &str, unit_ms: u64) -> Value {
        if args.len() < 3 {
            return arity_err(name);
        }
        let cond = match ExpireCond::parse(&args[3..]) {
            Ok(c) => c,
            Err(e) => return e,
        };
        match parse_i64(&args[2]) {
            Ok(n) => {
                let now = (self.clock)() as i64;
                // An instant Redis cannot represent is refused (BUG-0213).
                // It was clamped here, so `EXPIRE k -9223372036854775808`
                // deleted the key where Redis answers an error.
                let Some(when) = n
                    .checked_mul(unit_ms as i64)
                    .and_then(|ms| ms.checked_add(now))
                else {
                    return err(&format!("ERR invalid expire time in '{name}' command"));
                };
                // Already in the past: delete-on-touch semantics.
                let at = if when <= now { 1 } else { when as u64 };
                self.expire_if(&args[1], cond, when, at)
            }
            Err(_) => err("ERR value is not an integer or out of range"),
        }
    }

    /// EXPIREAT/PEXPIREAT: the argument is an ABSOLUTE instant (s or ms).
    fn cmd_expire_at(&self, args: &[Vec<u8>], name: &str, unit_ms: u64) -> Value {
        if args.len() < 3 {
            return arity_err(name);
        }
        let cond = match ExpireCond::parse(&args[3..]) {
            Ok(c) => c,
            Err(e) => return e,
        };
        match parse_i64(&args[2]) {
            Ok(n) => match n.checked_mul(unit_ms as i64) {
                Some(when) => self.expire_if(&args[1], cond, when, when.max(1) as u64),
                None => err(&format!("ERR invalid expire time in '{name}' command")),
            },
            Err(_) => err("ERR value is not an integer or out of range"),
        }
    }

    /// Set `key` to expire at `at` if `cond` holds, comparing the requested
    /// instant `when` (which may be in the past) against the key's current
    /// expiry, as Redis does. 1 if the expiry was set (or the key deleted by
    /// a past one), 0 if the key is missing or the condition failed. The read
    /// and the write are one step: EXPIRE is not a pure write, so it holds
    /// the key's exclusive lock.
    fn expire_if(&self, key: &[u8], cond: ExpireCond, when: i64, at: u64) -> Value {
        let slot = slot_for_key(key);
        if cond != ExpireCond::default() {
            match self.keyspace.expire_time_ms(slot, key) {
                None => return Value::Integer(0),
                Some(current) if !cond.holds(current, when) => return Value::Integer(0),
                Some(_) => {}
            }
        }
        Value::Integer(self.keyspace.expire_at(slot, key, at) as i64)
    }

    /// EXPIRETIME/PEXPIRETIME: the ABSOLUTE expiry (s or ms); -1 no expiry,
    /// -2 missing key.
    fn cmd_expire_time(&self, args: &[Vec<u8>], name: &str, unit_ms: u64) -> Value {
        exact(args, 2, name, |a| {
            match self.keyspace.expire_time_ms(slot_for_key(&a[1]), &a[1]) {
                None => Value::Integer(-2),
                Some(0) => Value::Integer(-1),
                Some(ms) => Value::Integer((ms / unit_ms) as i64),
            }
        })
    }

    fn cmd_ttl(&self, args: &[Vec<u8>], name: &str, unit_ms: u64) -> Value {
        exact(args, 2, name, |a| {
            match self.keyspace.ttl(slot_for_key(&a[1]), &a[1]) {
                Ttl::Missing => Value::Integer(-2),
                Ttl::NoExpiry => Value::Integer(-1),
                // Redis rounds to the nearest second; this rounded up
                // (BUG-0214). PTTL's unit of 1 leaves milliseconds exact.
                Ttl::Ms(ms) => Value::Integer((ms.saturating_add(unit_ms / 2) / unit_ms) as i64),
            }
        })
    }

    fn cmd_incr_delta(&self, args: &[Vec<u8>], name: &str, sign: i64) -> Value {
        exact(args, 3, name, |a| match parse_i64(&a[2]) {
            // DECRBY negates its argument, and i64::MIN has no negation. It
            // saturated to i64::MAX here, one short (BUG-0213).
            Ok(delta) => match delta.checked_mul(sign) {
                Some(delta) => reply(
                    self.strings.incr_by(slot_for_key(&a[1]), &a[1], delta),
                    Value::Integer,
                ),
                None => err("ERR decrement would overflow"),
            },
            Err(_) => err("ERR value is not an integer or out of range"),
        })
    }
}

fn reply<T>(r: Result<T, StoreError>, f: impl FnOnce(T) -> Value) -> Value {
    match r {
        Ok(v) => f(v),
        Err(e) => store_err(e),
    }
}

/// Which collection a typed scan walks.
enum ScanKind {
    Hash,
    Set,
    ZSet,
}

/// One in-flight keyspace SCAN session: which namespace it belongs to and
/// the envelope key to resume STRICTLY AFTER. See `cmd_scan` for why the
/// table is server-side (numeric-cursor client compatibility).
struct ScanCursor {
    ns: Vec<u8>,
    resume: Vec<u8>,
    at: std::time::Instant,
}

/// Bounded + TTL'd: an abandoned scan costs one row for two minutes, and
/// the table never exceeds SCAN_CURSOR_CAP rows (oldest evicted).
const SCAN_CURSOR_TTL: std::time::Duration = std::time::Duration::from_secs(120);
const SCAN_CURSOR_CAP: usize = 1024;
static NEXT_SCAN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn scan_cursors() -> &'static std::sync::Mutex<std::collections::HashMap<u64, ScanCursor>> {
    static TABLE: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<u64, ScanCursor>>,
    > = std::sync::OnceLock::new();
    TABLE.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Redis stringmatchlen-style glob over bytes: `*`, `?`, `[set]`/`[^set]`
/// with `a-z` ranges, and `\` escapes. Iterative with single-star
/// backtracking (globs have no nested quantifiers, so one backtrack point
/// suffices).
fn glob_match(pat: &[u8], s: &[u8]) -> bool {
    let (mut p, mut i) = (0usize, 0usize);
    let (mut star_p, mut star_i) = (usize::MAX, 0usize);
    while i < s.len() {
        let advanced = if p < pat.len() {
            match pat[p] {
                b'*' => {
                    star_p = p;
                    star_i = i;
                    p += 1;
                    continue;
                }
                b'?' => {
                    p += 1;
                    i += 1;
                    true
                }
                b'[' => match class_match(pat, p, s[i]) {
                    Some((true, next_p)) => {
                        p = next_p;
                        i += 1;
                        true
                    }
                    _ => false,
                },
                b'\\' if p + 1 < pat.len() => {
                    if pat[p + 1] == s[i] {
                        p += 2;
                        i += 1;
                        true
                    } else {
                        false
                    }
                }
                c => {
                    if c == s[i] {
                        p += 1;
                        i += 1;
                        true
                    } else {
                        false
                    }
                }
            }
        } else {
            false
        };
        if !advanced {
            if star_p == usize::MAX {
                return false;
            }
            // Backtrack: let the last '*' swallow one more input byte.
            star_i += 1;
            i = star_i;
            p = star_p + 1;
        }
    }
    while p < pat.len() && pat[p] == b'*' {
        p += 1;
    }
    p == pat.len()
}

/// `[...]` class at `pat[open]` (which is '['): does `c` match, and where
/// does the class end? None on an unterminated class (treated as no match,
/// mirroring Redis's lenient parser).
fn class_match(pat: &[u8], open: usize, c: u8) -> Option<(bool, usize)> {
    let mut p = open + 1;
    let negate = pat.get(p) == Some(&b'^');
    if negate {
        p += 1;
    }
    let mut hit = false;
    let mut first = true;
    while p < pat.len() {
        match pat[p] {
            b']' if !first => return Some((hit != negate, p + 1)),
            b'\\' if p + 1 < pat.len() => {
                if pat[p + 1] == c {
                    hit = true;
                }
                p += 2;
            }
            lo if p + 2 < pat.len() && pat[p + 1] == b'-' && pat[p + 2] != b']' => {
                let hi = pat[p + 2];
                if (lo.min(hi)..=lo.max(hi)).contains(&c) {
                    hit = true;
                }
                p += 3;
            }
            ch => {
                if ch == c {
                    hit = true;
                }
                p += 1;
            }
        }
        first = false;
    }
    None
}

fn store_err(e: StoreError) -> Value {
    match e {
        StoreError::NotInteger => err("ERR value is not an integer or out of range"),
        StoreError::Overflow => err("ERR increment or decrement would overflow"),
        StoreError::NanScore => err("ERR resulting score is not a number (NaN)"),
        StoreError::NotFloat => err("ERR value is not a valid float"),
        StoreError::NanOrInfinity => err("ERR increment would produce NaN or Infinity"),
        StoreError::WrongType => {
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        StoreError::ValueTooLarge => {
            err("ERR value exceeds maximum allowed size (max-value-bytes)")
        }
        StoreError::KeyExists => err("ERR item exists"),
        StoreError::BadParameter => err("ERR bad capacity or error rate"),
        StoreError::FilterFull => err("ERR non scaling filter is full"),
    }
}

fn exact(args: &[Vec<u8>], n: usize, name: &str, f: impl FnOnce(&[Vec<u8>]) -> Value) -> Value {
    if args.len() == n {
        f(args)
    } else {
        arity_err(name)
    }
}

fn multi_key(args: &[Vec<u8>], name: &str, mut f: impl FnMut(&[u8]) -> bool) -> Value {
    if args.len() < 2 {
        return arity_err(name);
    }
    Value::Integer(args[1..].iter().filter(|k| f(k)).count() as i64)
}

fn parse_f64(raw: &[u8]) -> Result<f64, ()> {
    let s = std::str::from_utf8(raw).map_err(|_| ())?;
    let v: f64 = s.parse().map_err(|_| ())?;
    // Rust rounds a spelling past f64's range to infinity ("1e400") or to
    // zero ("1e-400") where strtod reports ERANGE, which Valkey answers as
    // not a float. Only a spelled-out infinity or zero may parse as one.
    let unsigned = s.trim_start_matches(['+', '-']).to_ascii_lowercase();
    let spelled_inf = unsigned == "inf" || unsigned == "infinity";
    let spelled_zero = || {
        let mantissa = unsigned.split('e').next().unwrap_or_default();
        !mantissa.bytes().any(|b| matches!(b, b'1'..=b'9'))
    };
    if v.is_nan() || (v.is_infinite() && !spelled_inf) || (v == 0.0 && !spelled_zero()) {
        Err(())
    } else {
        Ok(v)
    }
}

/// How many rows a ZRANGE-family read can return, for BUG-0060's
/// admission (BUG-0216). A LIMIT with a count of -1 is no LIMIT, as Redis
/// reads it, and a rank range ignores one; arguments that do not parse are
/// left to the command's own error, sized as the whole set.
fn zset_read_rows(name_upper: &[u8], args: &[Vec<u8>]) -> ZsetRows {
    let opts = args.get(4..).unwrap_or_default();
    let has = |word: &[u8]| opts.iter().any(|a| a.eq_ignore_ascii_case(word));
    let by_rank = matches!(name_upper, b"ZREVRANGE")
        || name_upper == b"ZRANGE" && !has(b"BYSCORE") && !has(b"BYLEX");
    if by_rank {
        return match (
            args.get(2).map(|a| parse_i64(a)),
            args.get(3).map(|a| parse_i64(a)),
        ) {
            (Some(Ok(start)), Some(Ok(stop))) => ZsetRows::Ranks(start, stop),
            _ => ZsetRows::All,
        };
    }
    let limit = opts
        .iter()
        .position(|a| a.eq_ignore_ascii_case(b"LIMIT"))
        .and_then(|i| opts.get(i + 2))
        .and_then(|c| parse_i64(c).ok())
        .filter(|c| *c >= 0);
    limit.map_or(ZsetRows::All, |c| ZsetRows::AtMost(c as u64))
}

/// The instant an `EX`, `PX`, `EXAT` or `PXAT` argument of SET, SETEX or
/// GETEX names, in unix ms, or Redis's refusal (BUG-0213): a count of zero
/// or less, absolute or not, or one whose milliseconds, or whose sum with
/// now, overflows a signed 64-bit count. Seconds were clamped here, and
/// SETEX's multiplication wrapped in a release build.
///
/// Redis tests the sum after a signed addition, which C leaves undefined
/// on overflow, so the answer depends on the compiler: Valkey 9.1.0 built
/// on the gate box (Linux) refuses `PX 9223372036854775807`, and macOS
/// builds of Redis 8.2.8 and Valkey 9.1.0 answer OK. This refuses, as the
/// source means to.
fn string_expiry(opt: &[u8], raw: &[u8], now: u64, cmd: &str) -> Result<u64, Value> {
    let Ok(n) = parse_i64(raw) else {
        return Err(err("ERR value is not an integer or out of range"));
    };
    let invalid = || err(&format!("ERR invalid expire time in '{cmd}' command"));
    let unit = if matches!(opt, b"EX" | b"EXAT") {
        1000
    } else {
        1
    };
    let ms = (n > 0)
        .then(|| n.checked_mul(unit))
        .flatten()
        .ok_or_else(invalid)?;
    let at = if matches!(opt, b"EX" | b"PX") {
        i64::try_from(now)
            .ok()
            .and_then(|now| ms.checked_add(now))
            .ok_or_else(invalid)?
    } else {
        ms
    };
    Ok(at as u64)
}

/// A blocking command's timeout, in seconds, checked as Valkey checks it:
/// not a float (NaN, or a spelling past a double's range), negative, or so
/// large that its milliseconds overflow a signed 64-bit count.
fn parse_block_timeout(raw: &[u8]) -> Result<f64, Value> {
    let Ok(secs) = parse_f64(raw) else {
        return Err(err("ERR timeout is not a float or out of range"));
    };
    if secs < 0.0 {
        return Err(err("ERR timeout is negative"));
    }
    if secs * 1000.0 > i64::MAX as f64 {
        return Err(err("ERR timeout is out of range"));
    }
    Ok(secs)
}

/// An integer argument, read as Redis reads one (BUG-0213): `01` and `+1`
/// are not integers.
fn parse_i64(raw: &[u8]) -> Result<i64, ()> {
    parse_redis_i64(raw).ok_or(())
}

/// A non-negative count argument. `Option` rather than `Result<_, ()>`
/// because every caller wants to substitute its own error string.
fn parse_u64(raw: &[u8]) -> Option<u64> {
    std::str::from_utf8(raw).ok()?.parse().ok()
}

/// Redis's refusal of i64::MIN where its bound is symmetric
/// (`getRangeLongFromObjectOrReply(-LONG_MAX, LONG_MAX)`): LPOS's RANK, LREM's
/// count, SRANDMEMBER's count.
const OUT_OF_SYMMETRIC_RANGE: &str =
    "ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807";

/// What one reply element costs in memory beyond its bytes: the `Value`
/// and its `Vec`'s header, rounded up.
const REPLY_ELEMENT_BYTES: u64 = 64;

/// The legacy-dialect JSON errors. Only ever reached from a non-`$` path:
/// a JSONPath caller gets an empty or null-holding container instead, which
/// is the whole point of the two dialects.
const PATH_MISSING: &str = "ERR Path does not exist";
const UNSUPPORTED_PATH: &str = "ERR path contains an unsupported construct (multi-match \
     outside the $ dialect, a regex or multi-match operand in a filter, a \
     negative slice step, or a slice in a union)";
const NOT_AN_ARRAY: &str = "ERR path does not hold an array";
const NO_SUCH_KEY: &str = "ERR could not perform this operation on a key that doesn't exist";

fn err(msg: &str) -> Value {
    Value::Error(msg.into())
}

/// The conditions Redis 7 added to EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT:
/// set the expiry only if the key has none (NX), has one (XX), or the new
/// one is later (GT) or earlier (LT) than the current. A key with no expiry
/// counts as expiring never, so GT never applies to it and LT always does.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ExpireCond {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
}

impl ExpireCond {
    /// The options after the time, with upstream's errors and precedence:
    /// an unknown option first, then the incompatible pairs.
    fn parse(opts: &[Vec<u8>]) -> Result<Self, Value> {
        let mut c = Self::default();
        for o in opts {
            match o.to_ascii_uppercase().as_slice() {
                b"NX" => c.nx = true,
                b"XX" => c.xx = true,
                b"GT" => c.gt = true,
                b"LT" => c.lt = true,
                _ => {
                    return Err(err(&format!(
                        "ERR Unsupported option {}",
                        String::from_utf8_lossy(o)
                    )));
                }
            }
        }
        if c.nx && (c.xx || c.gt || c.lt) {
            return Err(err(
                "ERR NX and XX, GT or LT options at the same time are not compatible",
            ));
        }
        if c.gt && c.lt {
            return Err(err(
                "ERR GT and LT options at the same time are not compatible",
            ));
        }
        Ok(c)
    }

    /// Whether the new expiry `when` (absolute ms) may replace `current`
    /// (absolute ms; 0 is no expiry, as the keyspace stores it).
    fn holds(self, current: u64, when: i64) -> bool {
        let persistent = current == 0;
        let current = i64::try_from(current).unwrap_or(i64::MAX);
        !(self.nx && !persistent
            || self.xx && persistent
            || self.gt && (persistent || when <= current)
            || self.lt && !persistent && when >= current)
    }
}

fn arity_err(cmd: &str) -> Value {
    err(&format!(
        "ERR wrong number of arguments for '{cmd}' command"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flint_storage::MemKv;
    use flint_storage::strings::system_clock;

    use flint_resp::Proto;

    fn call(kv: &MemKv, parts: &[&[u8]]) -> Value {
        let d = Dispatcher::new(kv, system_clock);
        d.dispatch(&parts.iter().map(|p| p.to_vec()).collect::<Vec<_>>())
    }

    /// BUG-0181: every multi-key command that refuses cross-slot keys does
    /// so at QUEUE time, so the refusal poisons the transaction instead of
    /// failing alone at EXEC. `a` is slot 15495, `b` 3300.
    #[test]
    fn a_commands_own_crossslot_is_a_queue_time_error() {
        let q = |parts: &[&str]| {
            queue_time_error(
                &parts
                    .iter()
                    .map(|p| p.as_bytes().to_vec())
                    .collect::<Vec<_>>(),
                false,
            )
        };
        for c in [
            &["MSET", "a", "1", "b", "2"][..],
            &["MGET", "a", "b"],
            &["JSON.MGET", "a", "b", "$"],
            &["JSON.MSET", "a", "$", "1", "b", "$", "2"],
            &["SINTER", "a", "b"],
            &["SUNIONSTORE", "a", "b"],
            &["ZUNIONSTORE", "a", "1", "b"],
            &["COPY", "a", "b"],
            &["RENAME", "a", "b"],
        ] {
            assert!(
                matches!(q(c), Some(Value::Error(ref e)) if e.starts_with("CROSSSLOT")),
                "{c:?} spans slots and must be refused when queued, got {:?}",
                q(c)
            );
        }
        // Colocated, the same commands queue.
        assert_eq!(q(&["MSET", "{u}a", "1", "{u}b", "2"]), None);
        assert_eq!(q(&["MGET", "{u}a", "{u}b"]), None);
        assert_eq!(q(&["JSON.MGET", "{u}a", "{u}b", "$"]), None);
        assert_eq!(q(&["JSON.MSET", "{u}a", "$", "1", "{u}b", "$", "2"]), None);
        // JSON.MGET's path is not a key: `$` alone must not read as a
        // second slot.
        assert_eq!(q(&["JSON.MGET", "a", "$"]), None);
        assert_eq!(q(&["RENAME", "{u}a", "{u}b"]), None);
        // DEL, UNLINK and EXISTS check no slot of their own: the queue step
        // walks their keys (BUG-0179), not this probe.
        assert_eq!(q(&["DEL", "a", "b"]), None);
    }

    /// The bytes a client on `proto` actually receives. Comparing wire
    /// output is the honest assertion for replies whose two dialects differ
    /// — the carrier (`ByProto`, `Resp3Nested`) is an implementation
    /// detail, but the bytes are the contract.
    fn wire(v: &Value, proto: Proto) -> Vec<u8> {
        let mut out = Vec::new();
        flint_resp::encode_proto(v, proto, &mut out);
        out
    }

    /// Drive a full SCAN to completion, returning every key seen. Panics on
    /// a non-conforming reply shape or a scan that fails to terminate.
    fn scan_all(kv: &MemKv, extra: &[&[u8]]) -> Vec<Vec<u8>> {
        let mut cursor = b"0".to_vec();
        let mut out = Vec::new();
        for _ in 0..10_000 {
            let mut args: Vec<&[u8]> = vec![b"SCAN", &cursor];
            args.extend_from_slice(extra);
            let Value::Array(Some(reply)) = call(kv, &args) else {
                panic!("SCAN reply shape");
            };
            let Value::Bulk(Some(next)) = &reply[0] else {
                panic!("cursor shape");
            };
            let Value::Array(Some(keys)) = &reply[1] else {
                panic!("keys shape");
            };
            for k in keys {
                let Value::Bulk(Some(k)) = k else {
                    panic!("key shape");
                };
                out.push(k.clone());
            }
            if next.as_slice() == b"0" {
                return out;
            }
            cursor = next.clone();
        }
        panic!("scan did not terminate");
    }

    #[test]
    fn scan_pages_through_the_whole_keyspace_exactly_once() {
        let s = MemKv::new();
        for i in 0..137 {
            call(&s, &[b"SET", format!("k:{i:03}").as_bytes(), b"v"]);
        }
        // Default COUNT (10) forces many batches; no key lost, none doubled.
        let mut got = scan_all(&s, &[]);
        got.sort();
        let mut want: Vec<Vec<u8>> = (0..137).map(|i| format!("k:{i:03}").into_bytes()).collect();
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn scan_match_and_count_and_type() {
        let s = MemKv::new();
        for i in 0..30 {
            call(&s, &[b"SET", format!("s:{i}").as_bytes(), b"v"]);
        }
        call(&s, &[b"HSET", b"h:1", b"f", b"v"]);
        call(&s, &[b"LPUSH", b"l:1", b"v"]);
        // MATCH filters without breaking pagination.
        let got = scan_all(&s, &[b"MATCH", b"s:1*", b"COUNT", b"7"]);
        assert_eq!(got.len(), 11, "s:1 and s:10..19");
        // TYPE filter: only the hash.
        let got = scan_all(&s, &[b"TYPE", b"hash"]);
        assert_eq!(got, vec![b"h:1".to_vec()]);
        // Unknown TYPE matches nothing, errors nothing.
        assert!(scan_all(&s, &[b"TYPE", b"stream"]).is_empty());
    }

    /// The RESP surface of the Bloom family, in the shapes a RedisBloom
    /// client already expects (ADR-0016). Reply TYPES are the contract
    /// here as much as the values: an integer where a client parses an
    /// integer, an array of integers for the multi forms.
    #[test]
    fn bloom_speaks_redisbloom() {
        let s = MemKv::new();

        // BF.ADD auto-creates. 1 = newly added, 0 = already present.
        assert_eq!(call(&s, &[b"BF.ADD", b"bf", b"a"]), Value::Integer(1));
        assert_eq!(call(&s, &[b"BF.ADD", b"bf", b"a"]), Value::Integer(0));
        assert_eq!(call(&s, &[b"BF.EXISTS", b"bf", b"a"]), Value::Integer(1));
        assert_eq!(call(&s, &[b"BF.EXISTS", b"bf", b"nope"]), Value::Integer(0));
        assert_eq!(call(&s, &[b"BF.CARD", b"bf"]), Value::Integer(1));
        assert_eq!(call(&s, &[b"TYPE", b"bf"]), Value::Simple("bloom".into()));

        assert_eq!(
            call(&s, &[b"BF.MADD", b"bf", b"a", b"b", b"c"]),
            Value::Array(Some(vec![
                Value::Integer(0),
                Value::Integer(1),
                Value::Integer(1)
            ]))
        );
        assert_eq!(
            call(&s, &[b"BF.MEXISTS", b"bf", b"b", b"zzz"]),
            Value::Array(Some(vec![Value::Integer(1), Value::Integer(0)]))
        );

        // A missing key is not an error for EXISTS/CARD, and is for INFO —
        // matching RedisBloom, where INFO is the one that must find a
        // filter to describe.
        assert_eq!(call(&s, &[b"BF.EXISTS", b"gone", b"a"]), Value::Integer(0));
        assert_eq!(call(&s, &[b"BF.CARD", b"gone"]), Value::Integer(0));
        assert!(matches!(call(&s, &[b"BF.INFO", b"gone"]), Value::Error(_)));

        // BF.RESERVE takes ERROR RATE FIRST, then capacity.
        assert_eq!(
            call(&s, &[b"BF.RESERVE", b"r", b"0.001", b"5000"]),
            Value::Simple("OK".into())
        );
        assert!(matches!(
            call(&s, &[b"BF.RESERVE", b"r", b"0.001", b"5000"]),
            Value::Error(e) if e.contains("exists")
        ));
        // A single-field BF.INFO is a ONE-ELEMENT ARRAY. Verified on the
        // wire against RedisBloom 2.8.16, which answers `*1\r\n:5000\r\n`
        // — its clients index [0], so a bare integer breaks them.
        assert_eq!(
            call(&s, &[b"BF.INFO", b"r", b"CAPACITY"]),
            Value::Array(Some(vec![Value::Integer(5000)]))
        );
        assert_eq!(
            call(&s, &[b"BF.INFO", b"r", b"ITEMS"]),
            Value::Array(Some(vec![Value::Integer(0)]))
        );
        // An unknown section is a BARE error, NOT a wrapped one — also
        // checked on the wire, because "everything is wrapped" would have
        // been the natural guess and is wrong.
        assert!(matches!(
            call(&s, &[b"BF.INFO", b"r", b"NOSUCH"]),
            Value::Error(_)
        ));

        // NONSCALING reports a nil expansion rate, not a zero — and the
        // nil is wrapped like any other field.
        call(&s, &[b"BF.RESERVE", b"n", b"0.01", b"100", b"NONSCALING"]);
        assert_eq!(
            call(&s, &[b"BF.INFO", b"n", b"EXPANSION"]),
            Value::Array(Some(vec![Value::Bulk(None)]))
        );
        call(
            &s,
            &[b"BF.RESERVE", b"e", b"0.01", b"100", b"EXPANSION", b"4"],
        );
        assert_eq!(
            call(&s, &[b"BF.INFO", b"e", b"EXPANSION"]),
            Value::Array(Some(vec![Value::Integer(4)]))
        );

        // BF.INSERT reserves and adds in one trip; NOCREATE refuses to.
        assert_eq!(
            call(
                &s,
                &[
                    b"BF.INSERT",
                    b"i",
                    b"CAPACITY",
                    b"1000",
                    b"ITEMS",
                    b"x",
                    b"y"
                ]
            ),
            Value::Array(Some(vec![Value::Integer(1), Value::Integer(1)]))
        );
        assert_eq!(call(&s, &[b"BF.EXISTS", b"i", b"x"]), Value::Integer(1));
        assert!(matches!(
            call(&s, &[b"BF.INSERT", b"absent", b"NOCREATE", b"ITEMS", b"x"]),
            Value::Error(e) if e.contains("not found")
        ));

        // The full BF.INFO reply is the five documented name/value pairs.
        let Value::Array(Some(rows)) = call(&s, &[b"BF.INFO", b"bf"]) else {
            panic!("BF.INFO should reply an array");
        };
        assert_eq!(rows.len(), 10);
        // SIMPLE strings for the names, as RedisBloom sends them (`+Capacity`).
        assert_eq!(rows[0], Value::Simple("Capacity".into()));
        assert_eq!(rows[6], Value::Simple("Number of items inserted".into()));
        assert_eq!(rows[7], Value::Integer(3));

        // Wrong type both ways, and the dump commands refuse rather than
        // emitting a chunk format that is not interchangeable (D7.2).
        call(&s, &[b"SET", b"str", b"v"]);
        assert!(matches!(
            call(&s, &[b"BF.ADD", b"str", b"x"]),
            Value::Error(e) if e.starts_with("WRONGTYPE")
        ));
        assert!(matches!(
            call(&s, &[b"GET", b"bf"]),
            Value::Error(e) if e.starts_with("WRONGTYPE")
        ));
        assert!(matches!(
            call(&s, &[b"BF.SCANDUMP", b"bf", b"0"]),
            Value::Error(e) if e.contains("not supported")
        ));

        // Arity and syntax are refused, not guessed at.
        assert!(matches!(call(&s, &[b"BF.ADD", b"bf"]), Value::Error(_)));
        assert!(matches!(
            call(&s, &[b"BF.RESERVE", b"q", b"0.01"]),
            Value::Error(_)
        ));
        assert!(matches!(
            call(&s, &[b"BF.RESERVE", b"q", b"nope", b"100"]),
            Value::Error(e) if e.contains("error rate")
        ));
        assert!(matches!(
            call(&s, &[b"BF.RESERVE", b"q", b"0.01", b"100", b"WAT"]),
            Value::Error(e) if e.contains("syntax")
        ));
    }

    /// The classifier is what keeps a write off a replica, so the family's
    /// entries are asserted here rather than assumed.
    #[test]
    fn bloom_commands_classify() {
        for w in [
            b"BF.ADD".as_slice(),
            b"BF.MADD",
            b"BF.RESERVE",
            b"BF.INSERT",
        ] {
            assert!(flint_commands::is_write_command(w), "{w:?} must be a write");
            assert!(!flint_commands::is_read_command(w));
        }
        for r in [
            b"BF.EXISTS".as_slice(),
            b"BF.MEXISTS",
            b"BF.CARD",
            b"BF.INFO",
        ] {
            assert!(flint_commands::is_read_command(r), "{r:?} must be a read");
            assert!(!flint_commands::is_write_command(r));
        }
        // A Bloom filter never shrinks, so nothing here frees space; DEL
        // is the only way out and is already in that set.
        assert!(!flint_commands::reduces_space(b"BF.ADD"));
    }

    #[test]
    fn scan_skips_expired_and_rejects_bad_cursors() {
        let s = MemKv::new();
        call(&s, &[b"SET", b"live", b"v"]);
        call(&s, &[b"SET", b"dead", b"v", b"PX", b"1"]);
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert_eq!(scan_all(&s, &[]), vec![b"live".to_vec()]);
        // A cursor that was never issued is an honest error, not a silent
        // restart or truncation.
        assert!(matches!(
            call(&s, &[b"SCAN", b"999999999"]),
            Value::Error(e) if e.contains("invalid cursor")
        ));
        assert!(matches!(
            call(&s, &[b"SCAN", b"not-a-number"]),
            Value::Error(e) if e.contains("invalid cursor")
        ));
    }

    #[test]
    fn scan_cursor_is_namespace_scoped() {
        let s = MemKv::new();
        // Tenant A seeds enough keys to leave a live cursor mid-scan.
        let a = Dispatcher::with_limits(&s, system_clock, Limits::default(), b"tenant-a");
        for i in 0..40 {
            a.dispatch(&[
                b"SET".to_vec(),
                format!("a:{i}").into_bytes(),
                b"v".to_vec(),
            ]);
        }
        let Value::Array(Some(reply)) = a.dispatch(&[
            b"SCAN".to_vec(),
            b"0".to_vec(),
            b"COUNT".to_vec(),
            b"5".to_vec(),
        ]) else {
            panic!("scan shape");
        };
        let Value::Bulk(Some(cursor)) = &reply[0] else {
            panic!("cursor shape");
        };
        assert_ne!(cursor.as_slice(), b"0", "mid-scan cursor expected");
        // Tenant B presenting A's cursor is rejected — cursors are not
        // transferable across namespaces.
        let b = Dispatcher::with_limits(&s, system_clock, Limits::default(), b"tenant-b");
        assert!(matches!(
            b.dispatch(&[b"SCAN".to_vec(), cursor.clone()]),
            Value::Error(e) if e.contains("invalid cursor")
        ));
        // And A can continue the same cursor unharmed.
        assert!(matches!(
            a.dispatch(&[b"SCAN".to_vec(), cursor.clone()]),
            Value::Array(Some(_))
        ));
    }

    #[test]
    fn flushall_is_namespace_scoped() {
        // FLUSHALL is a fan-out command: the proxy sends it to every master
        // on a backend connection pinned to ONE tenant's namespace
        // (Backends::call). Its blast radius is therefore a SERVER promise —
        // a tenant flushing its own keyspace must never touch another
        // tenant's rows, where a naive kv.clear() would wipe the shared
        // store. The FLINTNS-escape fix (proxy #151) stops a tenant naming
        // another namespace; this guards the other half: that a legitimately
        // scoped FLUSHALL stays scoped. Seeds string + hash + zset so a
        // regression in any one of the three CFs the handler clears
        // (Metadata, Subkey, ZScore) is caught, not just the string case.
        let s = MemKv::new();
        let a = Dispatcher::with_limits(&s, system_clock, Limits::default(), b"tenant-a");
        let b = Dispatcher::with_limits(&s, system_clock, Limits::default(), b"tenant-b");
        for d in [&a, &b] {
            d.dispatch(&[b"SET".to_vec(), b"str".to_vec(), b"v".to_vec()]);
            d.dispatch(&[
                b"HSET".to_vec(),
                b"hash".to_vec(),
                b"f".to_vec(),
                b"v".to_vec(),
            ]);
            d.dispatch(&[
                b"ZADD".to_vec(),
                b"zset".to_vec(),
                b"1".to_vec(),
                b"m".to_vec(),
            ]);
        }
        // Control: B holds its own data before A's flush.
        assert_eq!(
            b.dispatch(&[b"GET".to_vec(), b"str".to_vec()]),
            Value::Bulk(Some(b"v".to_vec()))
        );

        assert_eq!(
            a.dispatch(&[b"FLUSHALL".to_vec()]),
            Value::Simple("OK".into())
        );

        // A's keyspace is empty across all three types.
        assert_eq!(
            a.dispatch(&[b"GET".to_vec(), b"str".to_vec()]),
            Value::Bulk(None)
        );
        assert_eq!(
            a.dispatch(&[b"HGET".to_vec(), b"hash".to_vec(), b"f".to_vec()]),
            Value::Bulk(None)
        );
        assert_eq!(
            a.dispatch(&[b"ZSCORE".to_vec(), b"zset".to_vec(), b"m".to_vec()]),
            Value::Null
        );
        assert_eq!(a.dispatch(&[b"DBSIZE".to_vec()]), Value::Integer(0));

        // B is UNTOUCHED across all three types.
        assert_eq!(
            b.dispatch(&[b"GET".to_vec(), b"str".to_vec()]),
            Value::Bulk(Some(b"v".to_vec()))
        );
        assert_eq!(
            b.dispatch(&[b"HGET".to_vec(), b"hash".to_vec(), b"f".to_vec()]),
            Value::Bulk(Some(b"v".to_vec()))
        );
        assert!(matches!(
            b.dispatch(&[b"ZSCORE".to_vec(), b"zset".to_vec(), b"m".to_vec()]),
            Value::Double(_)
        ));
    }

    #[test]
    fn dbsize_is_namespace_scoped() {
        // The other fan-out command. A tenant must count only its own keys,
        // never the shared backend's cross-tenant total.
        let s = MemKv::new();
        let a = Dispatcher::with_limits(&s, system_clock, Limits::default(), b"tenant-a");
        let b = Dispatcher::with_limits(&s, system_clock, Limits::default(), b"tenant-b");
        for i in 0..3 {
            a.dispatch(&[b"SET".to_vec(), format!("a{i}").into_bytes(), b"v".to_vec()]);
        }
        for i in 0..7 {
            b.dispatch(&[b"SET".to_vec(), format!("b{i}").into_bytes(), b"v".to_vec()]);
        }
        // Each sees only its own count, not the 10 rows in the shared store.
        assert_eq!(a.dispatch(&[b"DBSIZE".to_vec()]), Value::Integer(3));
        assert_eq!(b.dispatch(&[b"DBSIZE".to_vec()]), Value::Integer(7));
    }

    #[test]
    fn json_document_lifecycle_and_paths() {
        let s = MemKv::new();
        // Root write creates the document; TYPE and GET see it.
        assert_eq!(
            call(&s, &[b"JSON.SET", b"d", b"$", br#"{"a":1,"t":["x"]}"#]),
            Value::Simple("OK".into())
        );
        assert_eq!(call(&s, &[b"TYPE", b"d"]), Value::Simple("json".into()));
        // `$` paths reply in containers; the legacy spellings reply bare.
        // Both arrive wrapped in `Resp3Nested`, which adds a level under
        // RESP3 only — matching how RedisJSON answers JSON.TYPE there.
        assert_eq!(
            call(&s, &[b"JSON.TYPE", b"d", b"$.a"]),
            Value::Resp3Nested(Box::new(Value::Array(Some(vec![Value::Bulk(Some(
                b"integer".to_vec()
            ))]))))
        );
        assert_eq!(
            call(&s, &[b"JSON.TYPE", b"d", b".a"]),
            Value::Resp3Nested(Box::new(Value::Bulk(Some(b"integer".to_vec()))))
        );
        assert_eq!(
            call(&s, &[b"JSON.GET", b"d", b"$.a"]),
            Value::Bulk(Some(b"[1]".to_vec()))
        );
        assert_eq!(
            call(&s, &[b"JSON.GET", b"d", b".a"]),
            Value::Bulk(Some(b"1".to_vec()))
        );
        // Sub-path write, then read back through the whole document.
        assert_eq!(
            call(&s, &[b"JSON.SET", b"d", b"$.a", b"42"]),
            Value::Simple("OK".into())
        );
        assert_eq!(
            call(&s, &[b"JSON.GET", b"d", b"$.a"]),
            Value::Bulk(Some(b"[42]".to_vec()))
        );
        // Array ops.
        assert_eq!(
            call(&s, &[b"JSON.ARRAPPEND", b"d", b"$.t", br#""y""#, br#""z""#]),
            Value::Array(Some(vec![Value::Integer(3)]))
        );
        assert_eq!(
            call(&s, &[b"JSON.ARRLEN", b"d", b"$.t"]),
            Value::Array(Some(vec![Value::Integer(3)]))
        );
        assert_eq!(call(&s, &[b"JSON.ARRLEN", b"d", b".t"]), Value::Integer(3));
        assert_eq!(
            call(&s, &[b"JSON.GET", b"d", b"$.t[-1]"]),
            Value::Bulk(Some(br#"["z"]"#.to_vec()))
        );
        // Numeric increment keeps integers integral. NUMINCRBY is the one
        // command whose two dialects differ in reply KIND, so assert what
        // each protocol actually puts on the wire rather than the carrier.
        assert_eq!(
            wire(
                &call(&s, &[b"JSON.NUMINCRBY", b"d", b"$.a", b"8"]),
                Proto::Resp2
            ),
            b"$4\r\n[50]\r\n"
        );
        assert_eq!(
            wire(
                &call(&s, &[b"JSON.NUMINCRBY", b"d", b"$.a", b"0"]),
                Proto::Resp3
            ),
            b"*1\r\n:50\r\n"
        );
        assert_eq!(
            wire(
                &call(&s, &[b"JSON.NUMINCRBY", b"d", b".a", b"0"]),
                Proto::Resp2
            ),
            b"$2\r\n50\r\n"
        );
        // Path delete removes just that member; the document survives. A
        // path matching nothing is an empty container, not nil.
        assert_eq!(call(&s, &[b"JSON.DEL", b"d", b"$.a"]), Value::Integer(1));
        assert_eq!(
            call(&s, &[b"JSON.GET", b"d", b"$.a"]),
            Value::Bulk(Some(b"[]".to_vec()))
        );
        assert_eq!(
            call(&s, &[b"JSON.ARRLEN", b"d", b"$.t"]),
            Value::Array(Some(vec![Value::Integer(3)]))
        );
        // Root delete removes the key.
        assert_eq!(call(&s, &[b"JSON.DEL", b"d"]), Value::Integer(1));
        assert_eq!(call(&s, &[b"EXISTS", b"d"]), Value::Integer(0));
    }

    #[test]
    fn json_errors_are_specific_and_safe() {
        let s = MemKv::new();
        call(
            &s,
            &[b"JSON.SET", b"d", b"$", br#"{"o":{"n":1},"s":"str"}"#],
        );
        // Unsupported path constructs name themselves: multi-match in the
        // legacy dialect (ADR-0054), and a regex filter in either.
        for path in [&b"..n"[..], b".o[*]", br#"$.o[?(@.n =~ "1")]"#] {
            assert!(
                matches!(call(&s, &[b"JSON.GET", b"d", path]), Value::Error(e) if e.contains("unsupported")),
                "{}",
                String::from_utf8_lossy(path)
            );
        }
        // Intermediates are never created silently.
        assert!(matches!(
            call(&s, &[b"JSON.SET", b"d", b"$.x.y", b"1"]),
            Value::Error(e) if e.contains("parent")
        ));
        assert_eq!(
            call(&s, &[b"JSON.GET", b"d", b"$.x"]),
            Value::Bulk(Some(b"[]".to_vec()))
        );
        // Type mismatches at the path: a null element under `$`, an error
        // under the legacy dialect. Same condition, two contracts.
        assert_eq!(
            wire(
                &call(&s, &[b"JSON.NUMINCRBY", b"d", b"$.s", b"1"]),
                Proto::Resp2
            ),
            b"$6\r\n[null]\r\n"
        );
        assert_eq!(
            wire(
                &call(&s, &[b"JSON.NUMINCRBY", b"d", b"$.s", b"1"]),
                Proto::Resp3
            ),
            b"*1\r\n_\r\n"
        );
        // The legacy dialect errors under RESP2 — and under RESP3 answers a
        // one-element array holding null, because there the reply KIND
        // itself differs. RedisJSON does exactly this; assert the bytes.
        assert!(
            String::from_utf8_lossy(&wire(
                &call(&s, &[b"JSON.NUMINCRBY", b"d", b".s", b"1"]),
                Proto::Resp2
            ))
            .contains("number")
        );
        assert_eq!(
            wire(
                &call(&s, &[b"JSON.NUMINCRBY", b"d", b".s", b"1"]),
                Proto::Resp3
            ),
            b"*1\r\n_\r\n"
        );
        assert_eq!(
            call(&s, &[b"JSON.ARRAPPEND", b"d", b"$.o", b"1"]),
            Value::Array(Some(vec![Value::Bulk(None)]))
        );
        assert!(matches!(
            call(&s, &[b"JSON.ARRAPPEND", b"d", b".o", b"1"]),
            Value::Error(e) if e.contains("array")
        ));
        // Invalid JSON input is rejected before it can be stored.
        assert!(matches!(
            call(&s, &[b"JSON.SET", b"d2", b"$", b"{not json"]),
            Value::Error(e) if e.contains("valid JSON")
        ));
        assert_eq!(call(&s, &[b"EXISTS", b"d2"]), Value::Integer(0));
        // WRONGTYPE both directions against a string.
        call(&s, &[b"SET", b"str", b"v"]);
        assert!(
            matches!(call(&s, &[b"JSON.GET", b"str"]), Value::Error(e) if e.starts_with("WRONGTYPE"))
        );
        assert!(matches!(call(&s, &[b"GET", b"d"]), Value::Error(e) if e.starts_with("WRONGTYPE")));
    }

    /// ADR-0054: an indefinite `$` path reads every match and writes every
    /// match, in document order. Each expectation is RedisJSON v8.2.8's
    /// answer to the same command.
    #[test]
    fn json_multimatch_reads_and_writes_every_match() {
        let s = MemKv::new();
        let get = |path: &[u8]| call(&s, &[b"JSON.GET", b"d", path]);
        let json = |text: &str| Value::Bulk(Some(text.as_bytes().to_vec()));
        call(
            &s,
            &[
                b"JSON.SET",
                b"d",
                b"$",
                br#"{"a":{"n":1,"m":"x"},"b":{"n":2},"c":[{"n":3},{"k":4}],"l":[1,2]}"#,
            ],
        );
        // Reads: recursive descent, wildcard, union, slice, filter.
        assert_eq!(get(b"$..n"), json("[1,2,3]"));
        assert_eq!(get(b"$.c[*].n"), json("[3]"));
        assert_eq!(get(b"$.l[1,0,1]"), json("[2,1,2]"));
        assert_eq!(get(b"$.l[0:1]"), json("[1]"));
        assert_eq!(get(br#"$.c[?(@.n > 2)]"#), json(r#"[{"n":3}]"#));
        assert_eq!(get(b"$.nothing[*]"), json("[]"));
        assert_eq!(
            call(&s, &[b"JSON.TYPE", b"d", b"$.*"]),
            Value::Resp3Nested(Box::new(Value::Array(Some(
                ["object", "object", "array", "array"]
                    .iter()
                    .map(|t| Value::Bulk(Some(t.as_bytes().to_vec())))
                    .collect()
            ))))
        );
        assert_eq!(
            call(&s, &[b"JSON.ARRLEN", b"d", b"$.*"]),
            Value::Array(Some(vec![
                Value::Bulk(None),
                Value::Bulk(None),
                Value::Integer(2),
                Value::Integer(2),
            ]))
        );
        // Writes reach every match; a non-number or non-array answers nil.
        assert_eq!(
            call(&s, &[b"JSON.ARRAPPEND", b"d", b"$[\"b\",\"l\"]", b"9"]),
            Value::Array(Some(vec![Value::Bulk(None), Value::Integer(3)]))
        );
        assert_eq!(
            wire(
                &call(&s, &[b"JSON.NUMINCRBY", b"d", b"$..n", b"10"]),
                Proto::Resp2
            ),
            b"$10\r\n[11,12,13]\r\n"
        );
        assert_eq!(
            wire(
                &call(&s, &[b"JSON.NUMINCRBY", b"d", b"$.a.*", b"1"]),
                Proto::Resp3
            ),
            b"*2\r\n:12\r\n_\r\n"
        );
        // A location a union names twice is incremented twice.
        assert_eq!(
            wire(
                &call(&s, &[b"JSON.NUMINCRBY", b"d", b"$.l[0,0]", b"1"]),
                Proto::Resp2
            ),
            b"$5\r\n[2,3]\r\n"
        );
        assert_eq!(get(b"$.l"), json("[[3,2,9]]"));
        // A refusal at any match refuses the whole command, and nothing is
        // saved: the matches before it stay as they were.
        call(
            &s,
            &[b"JSON.SET", b"d", b"$.c[0].n", b"9223372036854775807"],
        );
        assert!(matches!(
            call(&s, &[b"JSON.NUMINCRBY", b"d", b"$..n", b"1"]),
            Value::Error(e) if e.contains("overflow")
        ));
        assert_eq!(get(b"$..n"), json("[12,12,9223372036854775807]"));
        // SET replaces every match, nested ones included, and adds none:
        // matching nothing is refused, NX always is, XX answers nil.
        call(&s, &[b"JSON.SET", b"d", b"$.a.in", br#"{"n":5}"#]);
        assert_eq!(
            call(&s, &[b"JSON.SET", b"d", b"$..n", b"7"]),
            Value::Simple("OK".into())
        );
        assert_eq!(get(b"$..n"), json("[7,7,7,7]"));
        for set in [
            &[&b"JSON.SET"[..], b"d", b"$.*.z", b"true"][..],
            &[b"JSON.SET", b"d", b"$.*.z", b"true", b"NX"],
            &[b"JSON.SET", b"d", b"$..n", b"0", b"NX"],
        ] {
            assert!(
                matches!(call(&s, set), Value::Error(e) if e.contains("adds none")),
                "{set:?}"
            );
        }
        assert_eq!(
            call(&s, &[b"JSON.SET", b"d", b"$.*.z", b"true", b"XX"]),
            Value::Bulk(None)
        );
        assert_eq!(
            call(&s, &[b"JSON.SET", b"d", b"$..n", b"8", b"XX"]),
            Value::Simple("OK".into())
        );
        assert_eq!(get(b"$..z"), json("[]"));
        // DEL counts each removed location once; one inside another removed
        // one goes with it, uncounted. Index order cannot shift a later
        // removal.
        assert_eq!(
            call(&s, &[b"JSON.DEL", b"d", b"$.l[0,2]"]),
            Value::Integer(2)
        );
        assert_eq!(get(b"$.l"), json("[[2]]"));
        assert_eq!(
            call(&s, &[b"JSON.DEL", b"d", b"$.l[0,0]"]),
            Value::Integer(1)
        );
        assert_eq!(get(b"$.l"), json("[[]]"));
        assert_eq!(call(&s, &[b"JSON.DEL", b"d", b"$..n"]), Value::Integer(4));
        assert_eq!(get(b"$..n"), json("[]"));
        // Emptying the document deletes the key (BUG-0209).
        assert_eq!(call(&s, &[b"JSON.DEL", b"d", b"$..*"]), Value::Integer(4));
        assert_eq!(call(&s, &[b"EXISTS", b"d"]), Value::Integer(0));
    }

    /// ADR-0055: a multi-match edit that moves array elements applies at the
    /// last location first, so nested matches all land where they were
    /// named; a location a union names twice is edited twice, in order.
    #[test]
    fn json_multimatch_edits_land_on_nested_and_repeated_locations() {
        let s = MemKv::new();
        let get = |k: &[u8]| call(&s, &[b"JSON.GET", k]);
        let json = |text: &str| Value::Bulk(Some(text.as_bytes().to_vec()));
        let nested = br#"{"a":[{"a":[1]},{"a":[2,3]}]}"#;
        call(&s, &[b"JSON.SET", b"n", b"$", nested]);
        // Edited in document order, the insert into the outer array would
        // move the inner ones before they were reached.
        assert_eq!(
            call(&s, &[b"JSON.ARRINSERT", b"n", b"$..a", b"0", br#""x""#]),
            Value::Array(Some(vec![
                Value::Integer(3),
                Value::Integer(2),
                Value::Integer(3)
            ]))
        );
        assert_eq!(
            get(b"n"),
            json(r#"{"a":["x",{"a":["x",1]},{"a":["x",2,3]}]}"#)
        );
        call(&s, &[b"JSON.SET", b"n", b"$", nested]);
        assert_eq!(
            call(&s, &[b"JSON.ARRPOP", b"n", b"$..a", b"0"]),
            Value::Array(Some(vec![
                Value::Bulk(Some(br#"{"a":[]}"#.to_vec())),
                Value::Bulk(Some(b"1".to_vec())),
                Value::Bulk(Some(b"2".to_vec())),
            ]))
        );
        assert_eq!(get(b"n"), json(r#"{"a":[{"a":[3]}]}"#));
        // CLEAR counts a match inside another cleared one once.
        call(&s, &[b"JSON.SET", b"n", b"$", nested]);
        assert_eq!(call(&s, &[b"JSON.CLEAR", b"n", b"$..a"]), Value::Integer(1));
        assert_eq!(get(b"n"), json(r#"{"a":[]}"#));
        // A union naming one location twice toggles it twice.
        call(&s, &[b"JSON.SET", b"b", b"$", b"[true,false]"]);
        assert_eq!(
            call(&s, &[b"JSON.TOGGLE", b"b", b"$[0,0,1]"]),
            Value::Array(Some(vec![
                Value::Integer(0),
                Value::Integer(1),
                Value::Integer(1)
            ]))
        );
        assert_eq!(get(b"b"), json("[true,true]"));
    }

    /// ADR-0055: JSON.MSET checks every triple against the documents as they
    /// were, then applies them in order, storing nothing unless all apply.
    #[test]
    fn json_mset_is_all_or_nothing() {
        let s = MemKv::new();
        let get = |k: &[u8]| call(&s, &[b"JSON.GET", k]);
        let json = |text: &str| Value::Bulk(Some(text.as_bytes().to_vec()));
        call(&s, &[b"JSON.SET", b"{t}a", b"$", br#"{"x":1}"#]);
        // A key the command creates is still missing to a later triple.
        assert!(matches!(
            call(
                &s,
                &[b"JSON.MSET", b"{t}c", b"$", b"{}", b"{t}c", b"$.y", b"1"]
            ),
            Value::Error(_)
        ));
        assert_eq!(call(&s, &[b"EXISTS", b"{t}c"]), Value::Integer(0));
        // A triple that cannot apply to what an earlier one wrote refuses
        // the command, and the earlier triple is not stored either.
        assert!(matches!(
            call(
                &s,
                &[b"JSON.MSET", b"{t}a", b"$", b"5", b"{t}a", b"$.x", b"2"]
            ),
            Value::Error(_)
        ));
        assert_eq!(get(b"{t}a"), json(r#"{"x":1}"#));
        // So does a missing intermediate, where RedisJSON writes the rest.
        assert!(matches!(
            call(
                &s,
                &[b"JSON.MSET", b"{t}a", b"$.x", b"3", b"{t}a", b"$.q.r", b"1"]
            ),
            Value::Error(_)
        ));
        assert_eq!(get(b"{t}a"), json(r#"{"x":1}"#));
        assert_eq!(
            call(
                &s,
                &[
                    b"JSON.MSET",
                    b"{t}a",
                    b"$.x",
                    b"8",
                    b"{t}b",
                    b"$",
                    b"[]",
                    b"{t}a",
                    b"$.x",
                    b"9"
                ]
            ),
            Value::Simple("OK".into())
        );
        assert_eq!(get(b"{t}a"), json(r#"{"x":9}"#));
        assert_eq!(get(b"{t}b"), json("[]"));
        assert!(matches!(
            call(&s, &[b"JSON.MSET", b"a", b"$", b"1", b"b", b"$", b"2"]),
            Value::Error(e) if e.starts_with("CROSSSLOT")
        ));
    }

    /// BUG-0208: an integer is incremented in integer arithmetic. The old
    /// f64 round trip stored each of these wrong, without an error.
    #[test]
    fn json_numincrby_keeps_integers_exact() {
        let s = MemKv::new();
        let incr = |start: &str, by: &str| {
            call(
                &s,
                &[
                    b"JSON.SET",
                    b"p",
                    b"$",
                    format!(r#"{{"a":{start}}}"#).as_bytes(),
                ],
            );
            let reply = call(&s, &[b"JSON.NUMINCRBY", b"p", b".a", by.as_bytes()]);
            let stored = call(&s, &[b"JSON.GET", b"p", b".a"]);
            (reply, stored)
        };
        let json = |text: &str| Value::Bulk(Some(text.as_bytes().to_vec()));
        // Above 2^53, +0 and +1 are exact.
        assert_eq!(incr("9007199254740993", "0").1, json("9007199254740993"));
        assert_eq!(incr("9007199254740993", "1").1, json("9007199254740994"));
        assert_eq!(incr("-9007199254740993", "-1").1, json("-9007199254740994"));
        // Overflow is refused and the integer is kept.
        let (reply, stored) = incr("9223372036854775807", "1");
        assert!(matches!(reply, Value::Error(e) if e.contains("overflow")));
        assert_eq!(stored, json("9223372036854775807"));
        // A whole increment beyond i64 is a float, not i64::MAX.
        assert_eq!(incr("1", "1e19").1, json("1e+19"));
        // An increment written as a float makes the integer a float, as in
        // RedisJSON, whether or not it is whole.
        assert_eq!(incr("1", "2.0").1, json("3.0"));
        assert_eq!(incr("-5", "2.5").1, json("-2.5"));
        // The `$` dialect takes the same path.
        call(&s, &[b"JSON.SET", b"p", b"$", br#"{"a":9007199254740993}"#]);
        assert_eq!(
            wire(
                &call(&s, &[b"JSON.NUMINCRBY", b"p", b"$.a", b"0"]),
                Proto::Resp2
            ),
            b"$18\r\n[9007199254740993]\r\n"
        );
    }

    /// BUG-0209: a JSON.DEL that leaves an empty object or array deletes
    /// the key, as RedisJSON does; one that leaves anything else keeps it.
    #[test]
    fn json_del_that_empties_the_document_deletes_the_key() {
        let s = MemKv::new();
        for (doc, path, removed, left) in [
            (&br#"{"a":1}"#[..], &b"$.a"[..], 1, 0),
            (br#"{"a":1}"#, b".a", 1, 0),
            (b"[1]", b"$[0]", 1, 0),
            (br#"{"a":1,"b":2}"#, b"$.*", 2, 0),
            // An emptied member is not an emptied document.
            (br#"{"a":[1]}"#, b"$.a[0]", 1, 1),
            (br#"{"a":1,"b":2}"#, b"$.a", 1, 1),
            // Nothing removed, nothing deleted.
            (b"{}", b"$.a", 0, 1),
        ] {
            call(&s, &[b"JSON.SET", b"d", b"$", doc]);
            let what = String::from_utf8_lossy(path);
            assert_eq!(
                call(&s, &[b"JSON.DEL", b"d", path]),
                Value::Integer(removed),
                "{what}"
            );
            assert_eq!(call(&s, &[b"EXISTS", b"d"]), Value::Integer(left), "{what}");
        }
    }

    #[test]
    fn json_set_nx_xx_and_ttl_preservation() {
        let s = MemKv::new();
        // NX creates, then refuses.
        assert_eq!(
            call(&s, &[b"JSON.SET", b"d", b"$", br#"{"a":1}"#, b"NX"]),
            Value::Simple("OK".into())
        );
        assert_eq!(
            call(&s, &[b"JSON.SET", b"d", b"$", br#"{"a":2}"#, b"NX"]),
            Value::Bulk(None)
        );
        // XX on a missing path refuses; on an existing path it writes.
        assert_eq!(
            call(&s, &[b"JSON.SET", b"d", b"$.new", b"1", b"XX"]),
            Value::Bulk(None)
        );
        assert_eq!(
            call(&s, &[b"JSON.SET", b"d", b"$.a", b"9", b"XX"]),
            Value::Simple("OK".into())
        );
        assert_eq!(
            call(&s, &[b"JSON.GET", b"d", b"$.a"]),
            Value::Bulk(Some(b"[9]".to_vec()))
        );
        // EVERY document write is an in-place mutation of an existing key,
        // so the TTL survives — the root replacement included. Clearing it
        // there would quietly make an expiring document immortal.
        assert_eq!(call(&s, &[b"EXPIRE", b"d", b"100"]), Value::Integer(1));
        call(&s, &[b"JSON.SET", b"d", b"$.a", b"10"]);
        assert!(
            matches!(call(&s, &[b"TTL", b"d"]), Value::Integer(t) if t > 0),
            "sub-path write kept the TTL"
        );
        call(&s, &[b"JSON.SET", b"d", b"$", br#"{"a":1}"#]);
        assert!(
            matches!(call(&s, &[b"TTL", b"d"]), Value::Integer(t) if t > 0),
            "root replacement kept the TTL too"
        );
        // A genuinely fresh key has no expiry to keep.
        call(&s, &[b"JSON.DEL", b"d"]);
        call(&s, &[b"JSON.SET", b"d", b"$", br#"{"a":1}"#]);
        assert_eq!(call(&s, &[b"TTL", b"d"]), Value::Integer(-1));
    }

    #[test]
    fn hash_commands_roundtrip() {
        let s = MemKv::new();
        assert_eq!(
            call(&s, &[b"HSET", b"h", b"a", b"1", b"b", b"2"]),
            Value::Integer(2)
        );
        assert_eq!(
            call(&s, &[b"HGET", b"h", b"a"]),
            Value::Bulk(Some(b"1".to_vec()))
        );
        assert_eq!(call(&s, &[b"HLEN", b"h"]), Value::Integer(2));
        assert_eq!(call(&s, &[b"HDEL", b"h", b"a"]), Value::Integer(1));
        assert_eq!(call(&s, &[b"TYPE", b"h"]), Value::Simple("hash".into()));
        assert_eq!(call(&s, &[b"HDEL", b"h", b"b"]), Value::Integer(1));
        assert_eq!(call(&s, &[b"TYPE", b"h"]), Value::Simple("none".into()));
    }

    #[test]
    fn wrongtype_both_directions() {
        let s = MemKv::new();
        call(&s, &[b"SET", b"str", b"v"]);
        call(&s, &[b"HSET", b"h", b"f", b"v"]);
        assert!(
            matches!(call(&s, &[b"HGET", b"str", b"f"]), Value::Error(e) if e.starts_with("WRONGTYPE"))
        );
        assert!(matches!(call(&s, &[b"GET", b"h"]), Value::Error(e) if e.starts_with("WRONGTYPE")));
        assert!(
            matches!(call(&s, &[b"INCR", b"h"]), Value::Error(e) if e.starts_with("WRONGTYPE"))
        );
        // DEL/EXISTS/EXPIRE are type-agnostic.
        assert_eq!(call(&s, &[b"EXISTS", b"h", b"str"]), Value::Integer(2));
        assert_eq!(call(&s, &[b"EXPIRE", b"h", b"100"]), Value::Integer(1));
        assert_eq!(call(&s, &[b"DEL", b"h", b"str"]), Value::Integer(2));
    }

    /// ADR-0050: the recognised texts, exactly as the libraries send
    /// them. Each must hash to its table entry, or recognition is silently off.
    const DJANGO_INCR_CHECKED: &str = "\n                    local exists = redis.call('EXISTS', KEYS[1])\n                    if (exists == 1) then\n                        return redis.call('INCRBY', KEYS[1], ARGV[1])\n                    else return false end\n                    ";
    const DJANGO_INCR: &str =
        "\n                    return redis.call('INCRBY', KEYS[1], ARGV[1])\n                    ";
    const LOCK_RELEASE: &str = "\n        local token = redis.call('get', KEYS[1])\n        if not token or token ~= ARGV[1] then\n            return 0\n        end\n        redis.call('del', KEYS[1])\n        return 1\n    ";
    const LOCK_EXTEND: &str = "\n        local token = redis.call('get', KEYS[1])\n        if not token or token ~= ARGV[1] then\n            return 0\n        end\n        local expiration = redis.call('pttl', KEYS[1])\n        if not expiration then\n            expiration = 0\n        end\n        if expiration < 0 then\n            return 0\n        end\n\n        local newttl = ARGV[2]\n        if ARGV[3] == \"0\" then\n            newttl = ARGV[2] + expiration\n        end\n        redis.call('pexpire', KEYS[1], newttl)\n        return 1\n    ";
    const LOCK_REACQUIRE: &str = "\n        local token = redis.call('get', KEYS[1])\n        if not token or token ~= ARGV[1] then\n            return 0\n        end\n        redis.call('pexpire', KEYS[1], ARGV[2])\n        return 1\n    ";
    // The amendment's, as node redlock 4.2.0, redsync v4.13.0 and Ruby
    // redlock 2.1.0 sent them, and python-redis-lock 4.0.1's two it refuses.
    const REDLOCK_ACQUIRE: &str = "\n\t-- Return 0 if an entry already exists.\n\tfor i, key in ipairs(KEYS) do\n\t\tif redis.call(\"exists\", key) == 1 then\n\t\t\treturn 0\n\t\tend\n\tend\n\n\t-- Create an entry for each provided key.\n\tfor i, key in ipairs(KEYS) do\n\t\tredis.call(\"set\", key, ARGV[1], \"PX\", ARGV[2])\n\tend\n\n\t-- Return the number of entries added.\n\treturn #KEYS\n";
    const REDLOCK_EXTEND: &str = "\n\t-- Return 0 if an entry exists with a *different* lock value.\n\tfor i, key in ipairs(KEYS) do\n\t\tif redis.call(\"get\", key) ~= ARGV[1] then\n\t\t\treturn 0\n\t\tend\n\tend\n\n\t-- Update the entry for each provided key.\n\tfor i, key in ipairs(KEYS) do\n\t\tredis.call(\"set\", key, ARGV[1], \"PX\", ARGV[2])\n\tend\n\n\t-- Return the number of entries updated.\n\treturn #KEYS\n";
    const REDLOCK_RELEASE: &str = "\n\tlocal count = 0\n\tfor i, key in ipairs(KEYS) do\n\t\t-- Only remove entries for *this* lock value.\n\t\tif redis.call(\"get\", key) == ARGV[1] then\n\t\t\tredis.pcall(\"del\", key)\n\t\t\tcount = count + 1\n\t\tend\n\tend\n\n\t-- Return the number of entries removed.\n\treturn count\n";
    const REDSYNC_EXTEND: &str = "\n\tif redis.call(\"GET\", KEYS[1]) == ARGV[1] then\n\t\treturn redis.call(\"PEXPIRE\", KEYS[1], ARGV[2])\n\telse\n\t\treturn 0\n\tend\n";
    const REDSYNC_RELEASE: &str = "\n\tlocal val = redis.call(\"GET\", KEYS[1])\n\tif val == ARGV[1] then\n\t\treturn redis.call(\"DEL\", KEYS[1])\n\telseif val == false then\n\t\treturn -1\n\telse\n\t\treturn 0\n\tend\n";
    const REDLOCK_RB_LOCK: &str = "      if (redis.call(\"exists\", KEYS[1]) == 0 and ARGV[3] == \"yes\") or redis.call(\"get\", KEYS[1]) == ARGV[1] then\n        return redis.call(\"set\", KEYS[1], ARGV[1], \"PX\", ARGV[2])\n      end\n";
    const REDLOCK_RB_UNLOCK: &str = "      if redis.call(\"get\",KEYS[1]) == ARGV[1] then\n        return redis.call(\"del\",KEYS[1])\n      else\n        return 0\n      end\n";
    const REDLOCK_RB_INFO: &str =
        "      return { redis.call(\"get\", KEYS[1]), redis.call(\"pttl\", KEYS[1]) }\n";
    const PY_REDIS_LOCK_EXTEND: &str = "\n    if redis.call(\"get\", KEYS[1]) ~= ARGV[1] then\n        return 1\n    elseif redis.call(\"ttl\", KEYS[1]) < 0 then\n        return 2\n    else\n        redis.call(\"expire\", KEYS[1], ARGV[2])\n        return 0\n    end\n";
    const PY_REDIS_LOCK_RELEASE: &str = "\n    if redis.call(\"get\", KEYS[1]) ~= ARGV[1] then\n        return 1\n    else\n        redis.call(\"del\", KEYS[2])\n        redis.call(\"lpush\", KEYS[2], 1)\n        redis.call(\"pexpire\", KEYS[2], ARGV[2])\n        redis.call(\"del\", KEYS[1])\n        return 0\n    end\n";
    // Read from the published packages: node redlock 5.0.0-beta.2, and
    // redsync's release before v4.12.0 and its `WithSetNXOnExtend` extend.
    const REDLOCK5_ACQUIRE: &str = "\n  -- Return 0 if an entry already exists.\n  for i, key in ipairs(KEYS) do\n    if redis.call(\"exists\", key) == 1 then\n      return 0\n    end\n  end\n\n  -- Create an entry for each provided key.\n  for i, key in ipairs(KEYS) do\n    redis.call(\"set\", key, ARGV[1], \"PX\", ARGV[2])\n  end\n\n  -- Return the number of entries added.\n  return #KEYS\n";
    const REDLOCK5_EXTEND: &str = "\n  -- Return 0 if an entry exists with a *different* lock value.\n  for i, key in ipairs(KEYS) do\n    if redis.call(\"get\", key) ~= ARGV[1] then\n      return 0\n    end\n  end\n\n  -- Update the entry for each provided key.\n  for i, key in ipairs(KEYS) do\n    redis.call(\"set\", key, ARGV[1], \"PX\", ARGV[2])\n  end\n\n  -- Return the number of entries updated.\n  return #KEYS\n";
    const REDLOCK5_RELEASE: &str = "\n  local count = 0\n  for i, key in ipairs(KEYS) do\n    -- Only remove entries for *this* lock value.\n    if redis.call(\"get\", key) == ARGV[1] then\n      redis.pcall(\"del\", key)\n      count = count + 1\n    end\n  end\n\n  -- Return the number of entries removed.\n  return count\n";
    const REDSYNC_RELEASE_BEFORE_4_12: &str = "\n\tif redis.call(\"GET\", KEYS[1]) == ARGV[1] then\n\t\treturn redis.call(\"DEL\", KEYS[1])\n\telse\n\t\treturn 0\n\tend\n";
    const REDSYNC_EXTEND_SETNX: &str = "\n\tif redis.call(\"GET\", KEYS[1]) == ARGV[1] then\n\t\treturn redis.call(\"PEXPIRE\", KEYS[1], ARGV[2])\n\telseif redis.call(\"SET\", KEYS[1], ARGV[1], \"PX\", ARGV[2], \"NX\") then\n\t\treturn 1\n\telse\n\t\treturn 0\n\tend\n";
    // node rate-limit-redis 6.0.1, as sent (the source strips each line's
    // indent), and 5.0.0's increment, which writes twice and is refused.
    const RATE_LIMIT_REDIS_INCR: &str = "local windowMs = tonumber(ARGV[1])\nlocal timeToExpire = redis.call(\"PTTL\", KEYS[1])\nif timeToExpire <= 0 then\nredis.call(\"SET\", KEYS[1], 1, \"PX\", windowMs)\nreturn { 1, windowMs }\nend\nlocal totalHits = redis.call(\"INCR\", KEYS[1])        \nreturn { totalHits, timeToExpire }";
    const RATE_LIMIT_REDIS_GET: &str = "local totalHits = redis.call(\"GET\", KEYS[1])\nlocal timeToExpire = redis.call(\"PTTL\", KEYS[1])\nreturn { totalHits, timeToExpire }";
    const RATE_LIMIT_REDIS_5_INCR: &str = "local windowMs = tonumber(ARGV[2])\nlocal resetOnChange = ARGV[1] == \"1\"\nlocal timeToExpire = redis.call(\"PTTL\", KEYS[1])\nif timeToExpire <= 0 then\nredis.call(\"SET\", KEYS[1], 1, \"PX\", windowMs)\nreturn { 1, windowMs }\nend\nlocal totalHits = redis.call(\"INCR\", KEYS[1])\nif resetOnChange then\nredis.call(\"PEXPIRE\", KEYS[1], windowMs)\ntimeToExpire = windowMs\nend\nreturn { totalHits, timeToExpire }";

    fn ev(s: &MemKv, parts: &[&str]) -> Value {
        call(s, &parts.iter().map(|p| p.as_bytes()).collect::<Vec<_>>())
    }

    /// Every library script this file exercises compiles in the sandbox,
    /// the two-write and two-key ones that ADR-0050's table could not run
    /// included: `SCRIPT LOAD` answers each text's own SHA1.
    #[test]
    fn every_library_script_compiles() {
        let s = MemKv::new();
        for text in [
            LOCK_RELEASE,
            LOCK_EXTEND,
            LOCK_REACQUIRE,
            DJANGO_INCR_CHECKED,
            DJANGO_INCR,
            REDLOCK_ACQUIRE,
            REDLOCK_EXTEND,
            REDLOCK_RELEASE,
            REDSYNC_EXTEND,
            REDSYNC_RELEASE,
            REDLOCK_RB_LOCK,
            REDLOCK_RB_UNLOCK,
            REDLOCK_RB_INFO,
            REDLOCK5_ACQUIRE,
            REDLOCK5_EXTEND,
            REDLOCK5_RELEASE,
            REDSYNC_RELEASE_BEFORE_4_12,
            REDSYNC_EXTEND_SETNX,
            RATE_LIMIT_REDIS_INCR,
            RATE_LIMIT_REDIS_GET,
            RATE_LIMIT_REDIS_5_INCR,
            PY_REDIS_LOCK_EXTEND,
            PY_REDIS_LOCK_RELEASE,
        ] {
            assert_eq!(
                ev(&s, &["SCRIPT", "LOAD", text]),
                Value::Bulk(Some(flint_tls::sha1_hex(text.as_bytes()).into_bytes()))
            );
        }
    }

    fn pttl(s: &MemKv, k: &str) -> i64 {
        let Value::Integer(t) = ev(s, &["PTTL", k]) else {
            panic!("PTTL {k}")
        };
        t
    }

    /// node redlock 4.2.0 and 5.0.0-beta: acquire only when absent, extend
    /// and release only with the token, each answering 1 or 0.
    #[test]
    fn node_redlock_scripts_do_what_the_lua_does() {
        for texts in [
            (REDLOCK_ACQUIRE, REDLOCK_EXTEND, REDLOCK_RELEASE),
            (REDLOCK5_ACQUIRE, REDLOCK5_EXTEND, REDLOCK5_RELEASE),
        ] {
            node_redlock_life(texts);
        }
    }

    fn node_redlock_life((acquire, extend, release): (&str, &str, &str)) {
        let s = MemKv::new();
        assert_eq!(
            ev(&s, &["EVAL", acquire, "1", "r", "tok", "10000"]),
            Value::Integer(1)
        );
        assert!((9_000..=10_000).contains(&pttl(&s, "r")));
        assert_eq!(
            ev(&s, &["EVAL", acquire, "1", "r", "other", "10000"]),
            Value::Integer(0),
            "held"
        );
        assert_eq!(
            ev(&s, &["EVAL", extend, "1", "r", "other", "30000"]),
            Value::Integer(0)
        );
        assert_eq!(
            ev(&s, &["EVAL", extend, "1", "r", "tok", "30000"]),
            Value::Integer(1)
        );
        assert!(pttl(&s, "r") > 20_000, "extended");
        assert_eq!(
            ev(&s, &["EVAL", extend, "1", "gone", "tok", "30000"]),
            Value::Integer(0),
            "a missing lock is not extended"
        );
        assert_eq!(ev(&s, &["EXISTS", "gone"]), Value::Integer(0));
        assert_eq!(
            ev(&s, &["EVAL", release, "1", "r", "other"]),
            Value::Integer(0)
        );
        assert_eq!(
            ev(&s, &["EVAL", release, "1", "r", "tok"]),
            Value::Integer(1)
        );
        assert_eq!(ev(&s, &["EXISTS", "r"]), Value::Integer(0));
        // A bad TTL is SET's error, as `redis.call` raises it.
        assert!(matches!(
            ev(&s, &["EVAL", acquire, "1", "r", "tok", "soon"]),
            Value::Error(_)
        ));
        assert_eq!(ev(&s, &["EXISTS", "r"]), Value::Integer(0));
    }

    /// redsync v4.13.0: extend answers PEXPIRE's reply, release DEL's, and a
    /// release of a lock that is gone answers -1.
    #[test]
    fn redsync_scripts_do_what_the_lua_does() {
        let s = MemKv::new();
        ev(&s, &["SET", "m", "tok", "PX", "8000"]);
        assert_eq!(
            ev(&s, &["EVAL", REDSYNC_EXTEND, "1", "m", "other", "30000"]),
            Value::Integer(0)
        );
        assert_eq!(
            ev(&s, &["EVAL", REDSYNC_EXTEND, "1", "m", "tok", "30000"]),
            Value::Integer(1)
        );
        assert!(pttl(&s, "m") > 20_000);
        assert_eq!(
            ev(&s, &["EVAL", REDSYNC_RELEASE, "1", "m", "other"]),
            Value::Integer(0)
        );
        assert_eq!(
            ev(&s, &["EVAL", REDSYNC_RELEASE, "1", "m", "tok"]),
            Value::Integer(1)
        );
        assert_eq!(
            ev(&s, &["EVAL", REDSYNC_RELEASE, "1", "m", "tok"]),
            Value::Integer(-1),
            "gone"
        );
        ev(&s, &["HSET", "h", "f", "v"]);
        assert!(
            matches!(ev(&s, &["EVAL", REDSYNC_RELEASE, "1", "h", "tok"]), Value::Error(e) if e.starts_with("WRONGTYPE"))
        );
        // Before v4.12.0 a release of a lock that is gone answered 0.
        ev(&s, &["SET", "o", "tok"]);
        assert_eq!(
            ev(
                &s,
                &["EVAL", REDSYNC_RELEASE_BEFORE_4_12, "1", "o", "other"]
            ),
            Value::Integer(0)
        );
        assert_eq!(
            ev(&s, &["EVAL", REDSYNC_RELEASE_BEFORE_4_12, "1", "o", "tok"]),
            Value::Integer(1)
        );
        assert_eq!(
            ev(&s, &["EVAL", REDSYNC_RELEASE_BEFORE_4_12, "1", "o", "tok"]),
            Value::Integer(0)
        );
    }

    /// redsync's `WithSetNXOnExtend`: an extend of a lock that expired takes
    /// it again, and one held by another answers 0 and leaves it alone.
    #[test]
    fn redsync_setnx_extend_does_what_the_lua_does() {
        let s = MemKv::new();
        assert_eq!(
            ev(
                &s,
                &["EVAL", REDSYNC_EXTEND_SETNX, "1", "x", "tok", "30000"]
            ),
            Value::Integer(1),
            "gone: taken again"
        );
        assert_eq!(ev(&s, &["GET", "x"]), Value::Bulk(Some(b"tok".to_vec())));
        assert!(pttl(&s, "x") > 20_000);
        assert_eq!(
            ev(
                &s,
                &["EVAL", REDSYNC_EXTEND_SETNX, "1", "x", "tok", "50000"]
            ),
            Value::Integer(1),
            "held: extended"
        );
        assert!(pttl(&s, "x") > 40_000);
        assert_eq!(
            ev(
                &s,
                &["EVAL", REDSYNC_EXTEND_SETNX, "1", "x", "other", "90000"]
            ),
            Value::Integer(0),
            "held by another"
        );
        assert_eq!(ev(&s, &["GET", "x"]), Value::Bulk(Some(b"tok".to_vec())));
        assert!(pttl(&s, "x") <= 50_000);
    }

    /// Ruby redlock: lock takes an absent key only when `ARGV[3]` is "yes",
    /// re-takes its own, answers OK or nil; unlock answers DEL's reply; info
    /// answers `[value, pttl]`, with a nil value for a missing key.
    #[test]
    fn ruby_redlock_scripts_do_what_the_lua_does() {
        let s = MemKv::new();
        assert_eq!(
            ev(
                &s,
                &["EVAL", REDLOCK_RB_LOCK, "1", "q", "tok", "10000", "no"]
            ),
            Value::Bulk(None),
            "absent, but new locks not allowed"
        );
        assert_eq!(ev(&s, &["EXISTS", "q"]), Value::Integer(0));
        assert_eq!(
            ev(
                &s,
                &["EVAL", REDLOCK_RB_LOCK, "1", "q", "tok", "10000", "yes"]
            ),
            Value::Simple("OK".into())
        );
        assert_eq!(
            ev(
                &s,
                &["EVAL", REDLOCK_RB_LOCK, "1", "q", "other", "10000", "yes"]
            ),
            Value::Bulk(None),
            "held by another"
        );
        assert_eq!(
            ev(
                &s,
                &["EVAL", REDLOCK_RB_LOCK, "1", "q", "tok", "30000", "no"]
            ),
            Value::Simple("OK".into()),
            "our own, extended"
        );
        assert!(pttl(&s, "q") > 20_000);
        let Value::Array(Some(info)) = ev(&s, &["EVAL", REDLOCK_RB_INFO, "1", "q"]) else {
            panic!("info")
        };
        assert_eq!(info[0], Value::Bulk(Some(b"tok".to_vec())));
        assert!(matches!(info[1], Value::Integer(t) if t > 20_000));
        assert_eq!(
            ev(&s, &["EVAL", REDLOCK_RB_UNLOCK, "1", "q", "other"]),
            Value::Integer(0)
        );
        assert_eq!(
            ev(&s, &["EVAL", REDLOCK_RB_UNLOCK, "1", "q", "tok"]),
            Value::Integer(1)
        );
        assert_eq!(
            ev(&s, &["EVAL", REDLOCK_RB_INFO, "1", "q"]),
            Value::Array(Some(vec![Value::Bulk(None), Value::Integer(-2)]))
        );
        // The GET runs only when the first clause is false: an absent key
        // with "yes" never reaches it, a held hash does and raises.
        ev(&s, &["HSET", "h", "f", "v"]);
        assert!(
            matches!(ev(&s, &["EVAL", REDLOCK_RB_LOCK, "1", "h", "tok", "10000", "yes"]), Value::Error(e) if e.starts_with("WRONGTYPE"))
        );
    }

    /// rate-limit-redis 6.x: a new window is SET with the window as its TTL
    /// and answers `[1, window]`; a live one counts up and answers the TTL
    /// left; the get answers `[hits, pttl]`, a nil count for no window.
    #[test]
    fn rate_limit_redis_scripts_do_what_the_lua_does() {
        let s = MemKv::new();
        let hit = |s: &MemKv| ev(s, &["EVAL", RATE_LIMIT_REDIS_INCR, "1", "rl:ip", "60000"]);
        assert_eq!(
            hit(&s),
            Value::Array(Some(vec![Value::Integer(1), Value::Integer(60000)]))
        );
        assert!((59_000..=60_000).contains(&pttl(&s, "rl:ip")));
        let Value::Array(Some(second)) = hit(&s) else {
            panic!("second hit")
        };
        assert_eq!(second[0], Value::Integer(2));
        assert!(matches!(second[1], Value::Integer(t) if (59_000..=60_000).contains(&t)));
        let Value::Array(Some(got)) = ev(&s, &["EVAL", RATE_LIMIT_REDIS_GET, "1", "rl:ip"]) else {
            panic!("get")
        };
        assert_eq!(got[0], Value::Bulk(Some(b"2".to_vec())));
        assert_eq!(
            ev(&s, &["EVAL", RATE_LIMIT_REDIS_GET, "1", "rl:none"]),
            Value::Array(Some(vec![Value::Bulk(None), Value::Integer(-2)]))
        );
        // A key with no TTL is a new window, as `PTTL <= 0` makes it.
        ev(&s, &["SET", "rl:bare", "7"]);
        assert_eq!(
            ev(&s, &["EVAL", RATE_LIMIT_REDIS_INCR, "1", "rl:bare", "1000"]),
            Value::Array(Some(vec![Value::Integer(1), Value::Integer(1000)]))
        );
        // The window is read only when a window is opened, as in the Lua:
        // a live window counts whatever it is sent.
        let Value::Array(Some(live)) =
            ev(&s, &["EVAL", RATE_LIMIT_REDIS_INCR, "1", "rl:ip", "soon"])
        else {
            panic!("live")
        };
        assert_eq!(live[0], Value::Integer(3));
        assert!(matches!(
            ev(&s, &["EVAL", RATE_LIMIT_REDIS_INCR, "1", "rl:new", "1.5"]),
            Value::Error(_)
        ));
        assert_eq!(ev(&s, &["EXISTS", "rl:new"]), Value::Integer(0));
    }

    /// Redis 7's EXPIRE conditions (BUG-0185), on all four commands: a key
    /// with no expiry counts as never expiring, a missing key answers 0, and
    /// the options are checked before the number, with upstream's errors.
    /// BUG-0218: a negative count past the seat's reply ceiling is refused
    /// with a reason and builds nothing. It built `|count|` members in one
    /// allocation, and two trillion of them took the seat down.
    #[test]
    fn srandmember_refuses_a_reply_past_the_seats_limit() {
        let s = MemKv::new();
        assert_eq!(ev(&s, &["SADD", "s", "a", "b", "c"]), Value::Integer(3));
        let Value::Error(e) = ev(&s, &["SRANDMEMBER", "s", "-2000000000000"]) else {
            panic!("a two-trillion-member reply was not refused")
        };
        assert!(e.contains("past this server's limit"), "{e}");
        assert_eq!(
            ev(&s, &["SRANDMEMBER", "s", "-9223372036854775808"]),
            Value::Error(OUT_OF_SYMMETRIC_RANGE.into())
        );
        let Value::Array(Some(picks)) = ev(&s, &["SRANDMEMBER", "s", "-5"]) else {
            panic!("a small negative count must still answer")
        };
        assert_eq!(picks.len(), 5, "repeats members, as Redis does");
        assert_eq!(
            ev(&s, &["SRANDMEMBER", "nosuch", "-2000000000000"]),
            Value::Array(Some(vec![]))
        );
        let d = Dispatcher::new(&s, system_clock);
        let args: Vec<Vec<u8>> = ["SRANDMEMBER", "s", "-1000"]
            .iter()
            .map(|a| a.as_bytes().to_vec())
            .collect();
        // The set (3 members of 1 byte plus their accounting) and 1,000 reply
        // elements are what admission is asked to hold.
        let sized = d
            .collection_read_bytes(b"SRANDMEMBER", &args)
            .expect("sized");
        assert!(sized >= 1000 * REPLY_ELEMENT_BYTES, "{sized}");
    }

    /// BUG-0216: BUG-0060's admission charges a sorted-set read what it can
    /// return. 100 members of 4 bytes cost 12 each with their scores.
    #[test]
    fn a_sorted_set_read_is_sized_by_what_it_returns() {
        let s = MemKv::new();
        let mut zadd = vec!["ZADD".to_string(), "z".to_string()];
        for i in 0..100 {
            zadd.push(i.to_string());
            zadd.push(format!("m{i:03}"));
        }
        let zadd: Vec<&str> = zadd.iter().map(String::as_str).collect();
        assert_eq!(ev(&s, &zadd), Value::Integer(100));
        let d = Dispatcher::new(&s, system_clock);
        let size = |parts: &[&str]| {
            let args: Vec<Vec<u8>> = parts.iter().map(|p| p.as_bytes().to_vec()).collect();
            d.collection_read_bytes(&args[0].to_ascii_uppercase(), &args)
        };
        assert_eq!(size(&["ZRANGE", "z", "0", "0"]), Some(12));
        assert_eq!(size(&["ZRANGE", "z", "0", "-1"]), Some(1200));
        assert_eq!(size(&["ZREVRANGE", "z", "0", "9", "WITHSCORES"]), Some(120));
        assert_eq!(
            size(&["ZRANGE", "z", "0", "-1", "LIMIT", "0", "-1"]),
            Some(1200)
        );
        assert_eq!(
            size(&["ZRANGE", "z", "-inf", "+inf", "BYSCORE"]),
            Some(1200)
        );
        assert_eq!(
            size(&["ZRANGE", "z", "-inf", "+inf", "BYSCORE", "LIMIT", "0", "2"]),
            Some(24)
        );
        assert_eq!(
            size(&["ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "5", "1"]),
            Some(12)
        );
        assert_eq!(size(&["ZRANGEBYLEX", "z", "-", "+"]), Some(1200));
        assert_eq!(size(&["ZPOPMIN", "z"]), Some(12));
        assert_eq!(size(&["ZPOPMAX", "z", "500"]), Some(1200));
        assert_eq!(size(&["ZRANGE", "nosuch", "0", "-1"]), None);
    }

    /// BUG-0213. Not a corpus case: the reference's answer depends on how
    /// it was compiled (`string_expiry`), so only Flint's is pinned here.
    #[test]
    fn a_relative_expiry_whose_sum_with_now_overflows_is_refused() {
        let s = MemKv::new();
        ev(&s, &["SET", "k", "v"]);
        for set in [
            ["SET", "k", "w", "PX", "9223372036854775807"],
            ["SET", "k", "w", "EX", "9223372036854775"],
        ] {
            assert_eq!(
                ev(&s, &set),
                Value::Error("ERR invalid expire time in 'set' command".into()),
                "{set:?}"
            );
        }
        assert_eq!(
            ev(&s, &["GETEX", "k", "PX", "9223372036854775807"]),
            Value::Error("ERR invalid expire time in 'getex' command".into())
        );
        assert_eq!(ev(&s, &["GET", "k"]), Value::Bulk(Some(b"v".to_vec())));
        assert_eq!(ev(&s, &["TTL", "k"]), Value::Integer(-1));
        // Absolute instants are not summed: PXAT takes the largest there is.
        assert_eq!(
            ev(&s, &["SET", "k", "w", "PXAT", "9223372036854775807"]),
            Value::Simple("OK".into())
        );
    }

    #[test]
    fn expire_conditions_do_what_redis_7_does() {
        let s = MemKv::new();
        let int = Value::Integer;
        let ttl = |s: &MemKv, k: &str| {
            let Value::Integer(t) = ev(s, &["TTL", k]) else {
                panic!("TTL {k}")
            };
            t
        };
        ev(&s, &["SET", "k", "v"]);
        assert_eq!(ev(&s, &["EXPIRE", "k", "100", "XX"]), int(0), "no TTL: XX");
        assert_eq!(
            ev(&s, &["EXPIRE", "k", "100", "GT"]),
            int(0),
            "no TTL is never: GT"
        );
        assert_eq!(ttl(&s, "k"), -1);
        assert_eq!(ev(&s, &["EXPIRE", "k", "100", "NX"]), int(1));
        assert_eq!(
            ev(&s, &["EXPIRE", "k", "200", "NX"]),
            int(0),
            "has a TTL: NX"
        );
        assert_eq!(ev(&s, &["EXPIRE", "k", "50", "GT"]), int(0));
        assert!((99..=100).contains(&ttl(&s, "k")));
        assert_eq!(ev(&s, &["EXPIRE", "k", "200", "gt"]), int(1), "any case");
        assert!((199..=200).contains(&ttl(&s, "k")));
        assert_eq!(ev(&s, &["EXPIRE", "k", "300", "LT"]), int(0));
        assert_eq!(ev(&s, &["PEXPIRE", "k", "50000", "XX", "LT"]), int(1));
        assert!((49..=50).contains(&ttl(&s, "k")));
        // LT applies to a key with no expiry.
        ev(&s, &["SET", "p", "v"]);
        assert_eq!(ev(&s, &["EXPIRE", "p", "100", "LT"]), int(1));
        // The absolute forms carry them too.
        ev(&s, &["SET", "a", "v"]);
        assert_eq!(ev(&s, &["EXPIREAT", "a", "9999999999", "NX"]), int(1));
        assert_eq!(ev(&s, &["EXPIREAT", "a", "9999999998", "GT"]), int(0));
        assert_eq!(ev(&s, &["PEXPIREAT", "a", "9999999999500", "GT"]), int(1));
        assert_eq!(ev(&s, &["PEXPIRETIME", "a"]), int(9_999_999_999_500));
        assert_eq!(ev(&s, &["EXPIRE", "nokey", "10", "NX"]), int(0), "missing");
        // A past expiry that meets its condition deletes, as one without does.
        ev(&s, &["SET", "d", "v"]);
        assert_eq!(ev(&s, &["EXPIRE", "d", "-1", "LT"]), int(1));
        assert_eq!(ev(&s, &["EXISTS", "d"]), int(0));
        let refused = |parts: &[&str], why: &str| {
            assert!(
                matches!(ev(&s, parts), Value::Error(ref e) if e.contains(why)),
                "{parts:?}: {:?}",
                ev(&s, parts)
            );
        };
        refused(&["EXPIRE", "k", "10", "NX", "XX"], "NX and XX, GT or LT");
        refused(&["EXPIRE", "k", "10", "NX", "GT"], "NX and XX, GT or LT");
        refused(&["EXPIRE", "k", "10", "GT", "LT"], "GT and LT options");
        refused(&["EXPIRE", "k", "10", "BOGUS"], "Unsupported option BOGUS");
        refused(
            &["EXPIRE", "k", "soon", "BOGUS"],
            "Unsupported option BOGUS",
        );
        refused(&["EXPIRE", "k", "soon", "NX"], "not an integer");
        refused(&["EXPIRE", "k"], "wrong number");
        refused(&["EXPIREAT", "k"], "wrong number");
        assert!((49..=50).contains(&ttl(&s, "k")), "no refusal touched it");
    }

    /// redis-py's Lock, end to end: release only with the token, extend by
    /// adding and by replacing, reacquire, and each refused without the token.
    #[test]
    fn lock_scripts_do_what_the_lua_does() {
        let s = MemKv::new();
        assert_eq!(
            ev(&s, &["SET", "lk", "tok", "PX", "10000"]),
            Value::Simple("OK".into())
        );
        assert_eq!(
            ev(&s, &["EVAL", LOCK_RELEASE, "1", "lk", "other"]),
            Value::Integer(0)
        );
        assert_eq!(
            ev(&s, &["EVAL", LOCK_EXTEND, "1", "lk", "tok", "5000", "0"]),
            Value::Integer(1)
        );
        let Value::Integer(t) = ev(&s, &["PTTL", "lk"]) else {
            panic!()
        };
        assert!(t > 10_000 && t <= 15_000, "added: {t}");
        assert_eq!(
            ev(&s, &["EVAL", LOCK_EXTEND, "1", "lk", "tok", "3000", "1"]),
            Value::Integer(1)
        );
        let Value::Integer(t) = ev(&s, &["PTTL", "lk"]) else {
            panic!()
        };
        assert!(t > 2_000 && t <= 3_000, "replaced: {t}");
        assert_eq!(
            ev(&s, &["EVAL", LOCK_REACQUIRE, "1", "lk", "tok", "8000"]),
            Value::Integer(1)
        );
        assert_eq!(
            ev(&s, &["EVAL", LOCK_REACQUIRE, "1", "lk", "other", "8000"]),
            Value::Integer(0)
        );
        // EVALSHA of a loaded script, its SHA in any case.
        ev(&s, &["SCRIPT", "LOAD", LOCK_RELEASE]);
        assert_eq!(
            ev(
                &s,
                &[
                    "EVALSHA",
                    "C3F8721CBB97F72BC19E972846BD7AAF91901658",
                    "1",
                    "lk",
                    "tok"
                ]
            ),
            Value::Integer(1),
            "EVALSHA, any case"
        );
        assert_eq!(ev(&s, &["EXISTS", "lk"]), Value::Integer(0));
        assert_eq!(
            ev(&s, &["EVAL", LOCK_RELEASE, "1", "lk", "tok"]),
            Value::Integer(0),
            "already gone"
        );
        // A lock with no TTL cannot be extended (the script's `expiration < 0`).
        ev(&s, &["SET", "lk2", "tok"]);
        assert_eq!(
            ev(&s, &["EVAL", LOCK_EXTEND, "1", "lk2", "tok", "5000", "0"]),
            Value::Integer(0)
        );
    }

    #[test]
    fn django_incr_scripts_do_what_the_lua_does() {
        let s = MemKv::new();
        assert_eq!(
            ev(&s, &["EVAL", DJANGO_INCR_CHECKED, "1", "n", "1"]),
            Value::Bulk(None),
            "missing: nil"
        );
        ev(&s, &["SET", "n", "5"]);
        assert_eq!(
            ev(&s, &["EVAL", DJANGO_INCR_CHECKED, "1", "n", "2"]),
            Value::Integer(7)
        );
        assert_eq!(
            ev(&s, &["EVAL", DJANGO_INCR, "1", "m", "3"]),
            Value::Integer(3)
        );
        ev(&s, &["SET", "t", "x"]);
        assert!(matches!(
            ev(&s, &["EVAL", DJANGO_INCR_CHECKED, "1", "t", "1"]),
            Value::Error(_)
        ));
    }

    /// A dispatcher in its own namespace, with its own script limits: the
    /// script cache is per namespace and process-wide, and a test that
    /// flushes it must not flush another test's.
    fn ev_in(s: &MemKv, ns: &str, limits: Limits, parts: &[&str]) -> Value {
        let args: Vec<Vec<u8>> = parts.iter().map(|p| p.as_bytes().to_vec()).collect();
        Dispatcher::with_limits(
            s,
            flint_storage::strings::system_clock,
            limits,
            ns.as_bytes(),
        )
        .dispatch(&args)
    }

    /// python-redis-lock's scripts name two keys, the lock and its signal
    /// list. With a hash tag in the lock's name they share a slot and run
    /// whole, release's four writes included; without one they are two slots
    /// and refused, as any cross-slot script is.
    #[test]
    fn python_redis_lock_scripts_run_when_their_keys_share_a_slot() {
        let s = MemKv::new();
        assert!(matches!(
            ev(&s, &["EVAL", PY_REDIS_LOCK_RELEASE, "2", "lock:x", "lock-signal:x", "tok", "100"]),
            Value::Error(e) if e.starts_with("CROSSSLOT")
        ));
        ev(&s, &["SET", "lock:{x}", "tok", "EX", "10"]);
        assert_eq!(
            ev(
                &s,
                &[
                    "EVAL",
                    PY_REDIS_LOCK_EXTEND,
                    "2",
                    "lock:{x}",
                    "lock-signal:{x}",
                    "tok",
                    "60"
                ]
            ),
            Value::Integer(0)
        );
        assert!(pttl(&s, "lock:{x}") > 50_000);
        assert_eq!(
            ev(
                &s,
                &[
                    "EVAL",
                    PY_REDIS_LOCK_RELEASE,
                    "2",
                    "lock:{x}",
                    "lock-signal:{x}",
                    "tok",
                    "100"
                ]
            ),
            Value::Integer(0)
        );
        assert_eq!(ev(&s, &["EXISTS", "lock:{x}"]), Value::Integer(0));
        assert_eq!(ev(&s, &["LLEN", "lock-signal:{x}"]), Value::Integer(1));
        assert!(pttl(&s, "lock-signal:{x}") > 0);
    }

    /// `SCRIPT` and `EVALSHA` over a namespace's cache, and upstream's
    /// numkeys errors.
    #[test]
    fn script_commands_and_malformed_calls() {
        let s = MemKv::new();
        let ev = |parts: &[&str]| ev_in(&s, "script-cmds", Limits::default(), parts);
        assert_eq!(ev(&["EVAL", "return 1", "0"]), Value::Integer(1));
        assert_eq!(
            ev(&["EVALSHA", "0123456789012345678901234567890123456789", "0"]),
            Value::Error("NOSCRIPT No matching script.".into())
        );
        let sha = "e0e1f9fabfc9d4800c877a703b823ac0578ff8db";
        assert_eq!(
            ev(&["SCRIPT", "LOAD", "return 1"]),
            Value::Bulk(Some(sha.into()))
        );
        assert_eq!(
            ev(&["SCRIPT", "EXISTS", sha, "abc"]),
            Value::Array(Some(vec![Value::Integer(1), Value::Integer(0)]))
        );
        assert_eq!(ev(&["EVALSHA", sha, "0"]), Value::Integer(1));
        // EVAL caches what it runs, as Redis does.
        ev(&["EVAL", "return 2", "0"]);
        assert_eq!(
            ev(&["EVALSHA", &flint_tls::sha1_hex(b"return 2"), "0"]),
            Value::Integer(2)
        );
        // Another namespace sees none of it.
        assert_eq!(
            ev_in(
                &s,
                "script-other",
                Limits::default(),
                &["SCRIPT", "EXISTS", sha]
            ),
            Value::Array(Some(vec![Value::Integer(0)]))
        );
        assert_eq!(
            ev(&["SCRIPT", "FLUSH", "ASYNC"]),
            Value::Simple("OK".into())
        );
        assert_eq!(
            ev(&["SCRIPT", "EXISTS", sha]),
            Value::Array(Some(vec![Value::Integer(0)]))
        );
        assert!(
            matches!(ev(&["SCRIPT", "LOAD", "return ("]), Value::Error(e)
            if e == "ERR Error compiling script (new function): user_script:1: unexpected symbol near '<eof>'")
        );
        assert_eq!(
            ev(&["SCRIPT", "KILL"]),
            Value::Error("NOTBUSY No scripts in execution right now.".into())
        );
        assert!(
            matches!(ev(&["SCRIPT", "BOGUS"]), Value::Error(e) if e.contains("unknown subcommand"))
        );
        assert!(
            matches!(ev(&["EVAL", LOCK_RELEASE, "-1"]), Value::Error(e) if e.contains("negative"))
        );
        assert!(
            matches!(ev(&["EVAL", LOCK_RELEASE, "3", "a"]), Value::Error(e) if e.contains("greater than"))
        );
        assert!(
            matches!(ev(&["EVAL", LOCK_RELEASE, "x"]), Value::Error(e) if e.contains("not an integer"))
        );
        assert!(
            matches!(ev(&["EVAL", LOCK_RELEASE]), Value::Error(e) if e.contains("wrong number"))
        );
    }

    /// ADR-0051's sandbox: past the time limit a script is stopped wherever
    /// it hides (a `pcall`, an `xpcall`, a coroutine the script made), past
    /// the memory limit likewise, and in every case nothing it wrote is kept.
    #[test]
    fn a_script_is_stopped_at_its_limits_and_keeps_nothing() {
        let s = MemKv::new();
        let limits = Limits {
            script: crate::script::ScriptLimits {
                time: std::time::Duration::from_millis(20),
                ..Default::default()
            },
            ..Limits::default()
        };
        let ev = |parts: &[&str]| ev_in(&s, "script-limits", limits, parts);
        for body in [
            "while true do end",
            "while true do pcall(function() while true do end end) end",
            "while true do xpcall(function() while true do end end, function(e) return e end) end",
            "coroutine.resume(coroutine.create(function() while true do end end))",
            "coroutine.wrap(function() while true do end end)()",
        ] {
            let text = format!("redis.call('set', KEYS[1], 'x') {body}");
            let started = std::time::Instant::now();
            let reply = ev(&["EVAL", &text, "1", "{l}k"]);
            assert!(
                matches!(&reply, Value::Error(e) if e.contains("time limit of 20 ms")),
                "{body}: {reply:?}"
            );
            assert!(
                started.elapsed() < std::time::Duration::from_secs(2),
                "{body}"
            );
            assert_eq!(
                ev(&["EXISTS", "{l}k"]),
                Value::Integer(0),
                "{body} kept a write"
            );
        }
        let reply = ev(&[
            "EVAL",
            "redis.call('set', KEYS[1], 'x') return #string.rep('x', 2^30)",
            "1",
            "{l}m",
        ]);
        assert!(
            matches!(&reply, Value::Error(e) if e.contains("memory limit")),
            "{reply:?}"
        );
        assert_eq!(ev(&["EXISTS", "{l}m"]), Value::Integer(0));
    }

    /// An uncaught error discards the script's writes; an error REPLY is a
    /// value, and they are kept (ADR-0051: Redis keeps them in both cases).
    #[test]
    fn a_failed_script_keeps_nothing_and_an_error_reply_keeps_all() {
        let s = MemKv::new();
        assert!(matches!(
            ev(&s, &["EVAL", "redis.call('set', KEYS[1], 'x') error('then failed')", "1", "rb"]),
            Value::Error(e) if e == "ERR user_script:1: then failed script: on @user_script:1."
        ));
        assert_eq!(ev(&s, &["EXISTS", "rb"]), Value::Integer(0));
        assert_eq!(
            ev(
                &s,
                &[
                    "EVAL",
                    "redis.call('set', KEYS[1], 'x') return redis.error_reply('MY no')",
                    "1",
                    "ok"
                ]
            ),
            Value::Error("MY no".into())
        );
        assert_eq!(ev(&s, &["GET", "ok"]), Value::Bulk(Some(b"x".to_vec())));
    }

    /// A script touches only keys in its declared keys' slot. A command
    /// that reaches past it is refused and has no effect, even caught.
    #[test]
    fn a_script_touches_only_keys_in_the_slot_it_declares() {
        assert_ne!(slot_for_key(b"{t}a"), slot_for_key(b"other"));
        assert_ne!(slot_for_key(b"{t}r"), slot_for_key(b"zz"));
        let s = MemKv::new();
        assert!(matches!(
            ev(&s, &["EVAL", "return 1", "2", "a", "b"]),
            Value::Error(e) if e.starts_with("CROSSSLOT")
        ));
        assert_eq!(
            ev(&s, &["EVAL", "return #KEYS", "2", "{t}a", "{t}b"]),
            Value::Integer(2)
        );
        assert!(matches!(
            ev(&s, &["EVAL", "return redis.call('get', 'other')", "1", "{t}a"]),
            Value::Error(e) if e.contains("access key 'other'") && e.contains("not in the slot")
                && e.ends_with("script: on @user_script:1.")
        ));
        // Caught, the refused DEL still did nothing: the declared key it
        // deleted before reaching the other one stays. (DEL takes keys in
        // any slots; RENAME would refuse `zz` itself, as CROSSSLOT.)
        ev(&s, &["SET", "zz", "z"]);
        assert!(matches!(
            ev(&s, &["EVAL", "redis.call('set', KEYS[1], 'v') return redis.pcall('del', KEYS[1], 'zz')", "1", "{t}r"]),
            Value::Error(e) if e.contains("access key 'zz'")
        ));
        assert_eq!(ev(&s, &["GET", "{t}r"]), Value::Bulk(Some(b"v".to_vec())));
        assert_eq!(ev(&s, &["GET", "zz"]), Value::Bulk(Some(b"z".to_vec())));
        // A script that declares no keys has no slot, and may touch none.
        assert!(matches!(
            ev(&s, &["EVAL", "return redis.call('get', '{t}a')", "0"]),
            Value::Error(e) if e.contains("access key '{t}a'")
        ));
        for cmd in ["dbsize", "flushall", "scan"] {
            let text = format!(
                "return redis.call('{cmd}'{})",
                if cmd == "scan" { ", '0'" } else { "" }
            );
            assert!(
                matches!(ev(&s, &["EVAL", &text, "1", "{t}a"]), Value::Error(e) if e.contains("whole keyspace")),
                "{cmd}"
            );
        }
        assert!(matches!(
            ev(&s, &["EVAL", "return redis.call('eval', 'return 1', '0')", "0"]),
            Value::Error(e) if e.starts_with("ERR This command is not allowed from script")
        ));
    }

    /// ADR-0052 D2: a key in the declared keys' slot that they do not
    /// include. Holding only the declared keys' locks, the script is
    /// abandoned where it stands, even inside `pcall`, with nothing written,
    /// and the dispatcher asks for the lock over every writer. Holding that
    /// lock, the same script runs.
    #[test]
    fn an_undeclared_key_in_the_slot_runs_only_holding_every_writer() {
        assert_eq!(slot_for_key(b"{t}k"), slot_for_key(b"{t}u"));
        let s = MemKv::new();
        let parts = |v: &[&str]| v.iter().map(|p| p.as_bytes().to_vec()).collect::<Vec<_>>();
        let plain = "redis.call('set', KEYS[1], 'a') redis.call('set', '{t}u', 'b') return 1";
        let caught = "redis.call('set', KEYS[1], 'a') \
                      local ok = redis.pcall('set', '{t}u', 'b') return 'caught'";
        for text in [plain, caught] {
            let d = Dispatcher::new(&s, system_clock);
            let reply = d.dispatch(&parts(&["EVAL", text, "1", "{t}k"]));
            assert!(d.wants_every_writer(), "{text}: {reply:?}");
            assert!(
                matches!(&reply, Value::Error(e) if e.contains("'{t}u'") && e.contains("every writer")),
                "{text}: {reply:?}"
            );
            assert_eq!(
                ev(&s, &["EXISTS", "{t}k", "{t}u"]),
                Value::Integer(0),
                "{text}"
            );
        }
        let d = Dispatcher::new(&s, system_clock).holding_every_writer(true);
        assert_eq!(
            d.dispatch(&parts(&["EVAL", plain, "1", "{t}k"])),
            Value::Integer(1)
        );
        assert!(!d.wants_every_writer());
        assert_eq!(ev(&s, &["GET", "{t}u"]), Value::Bulk(Some(b"b".to_vec())));
        // Holding every writer widens the slot, not the keyspace.
        let d = Dispatcher::new(&s, system_clock).holding_every_writer(true);
        let reply = d.dispatch(&parts(&[
            "EVAL",
            "return redis.call('get', 'other')",
            "1",
            "{t}k",
        ]));
        assert!(
            matches!(&reply, Value::Error(e) if e.contains("not in the slot")),
            "{reply:?}"
        );
        assert!(!d.wants_every_writer());
        // A script that stays inside its KEYS never asks.
        let d = Dispatcher::new(&s, system_clock);
        assert_eq!(
            d.dispatch(&parts(&[
                "EVAL",
                "return redis.call('incr', KEYS[1] .. '')",
                "1",
                "{t}n"
            ])),
            Value::Integer(1)
        );
        assert!(!d.wants_every_writer());
    }

    /// BUG-0189: the key cap reads a script's KEYS, not its text. BullMQ's
    /// scripts run past 4 KiB and were refused as an oversized key.
    #[test]
    fn a_long_script_is_not_an_oversized_key_but_its_keys_are_checked() {
        let s = MemKv::new();
        let max = flint_storage::DEFAULT_MAX_KEY_BYTES as usize;
        let text = format!(
            "return redis.call('incr', KEYS[1]) -- {}",
            "x".repeat(max + 1)
        );
        assert_eq!(ev(&s, &["EVAL", &text, "1", "{t}n"]), Value::Integer(1));
        let long_key = "k".repeat(max + 1);
        assert!(matches!(
            ev(&s, &["EVAL", "return 1", "1", &long_key]),
            Value::Error(e) if e.contains("max-key-bytes")
        ));
        // A key the script builds past the cap is refused at its call.
        assert!(matches!(
            ev(&s, &["EVAL", "return redis.call('get', KEYS[1] .. string.rep('k', ARGV[1]))", "1", "{t}n", &max.to_string()]),
            Value::Error(e) if e.contains("max-key-bytes")
        ));
    }

    /// ADR-0053: a placed tenant's script (`whole`) may declare and touch
    /// keys in any slot. Declared keys in two slots are refused without it;
    /// with it, an undeclared key in another slot abandons the script to run
    /// again holding every writer, as one in the same slot does, and under
    /// that lock it runs. A script declaring no keys still touches none.
    #[test]
    fn a_placed_tenants_script_may_span_slots() {
        assert_ne!(slot_for_key(b"{a}k"), slot_for_key(b"{b}k"));
        let s = MemKv::new();
        let parts = |v: &[&str]| v.iter().map(|p| p.as_bytes().to_vec()).collect::<Vec<_>>();
        let two = "redis.call('set', KEYS[1], 'x') redis.call('set', KEYS[2], 'y') return 1";
        let spread = Dispatcher::new(&s, system_clock).holding_every_writer(true);
        assert!(matches!(
            spread.dispatch(&parts(&["EVAL", two, "2", "{a}k", "{b}k"])),
            Value::Error(e) if e.starts_with("CROSSSLOT")
        ));
        let placed = Dispatcher::new(&s, system_clock)
            .holding_every_writer(true)
            .whole(true);
        assert_eq!(
            placed.dispatch(&parts(&["EVAL", two, "2", "{a}k", "{b}k"])),
            Value::Integer(1)
        );
        assert_eq!(ev(&s, &["GET", "{a}k"]), Value::Bulk(Some(b"x".to_vec())));
        assert_eq!(ev(&s, &["GET", "{b}k"]), Value::Bulk(Some(b"y".to_vec())));
        let reach = "redis.call('set', '{b}u', 'z') return 2";
        let striped = Dispatcher::new(&s, system_clock).whole(true);
        assert!(matches!(
            striped.dispatch(&parts(&["EVAL", reach, "1", "{a}k"])),
            Value::Error(_)
        ));
        assert!(
            striped.wants_every_writer(),
            "another slot without every writer re-runs"
        );
        assert_eq!(ev(&s, &["EXISTS", "{b}u"]), Value::Integer(0));
        let all = Dispatcher::new(&s, system_clock)
            .holding_every_writer(true)
            .whole(true);
        assert_eq!(
            all.dispatch(&parts(&["EVAL", reach, "1", "{a}k"])),
            Value::Integer(2)
        );
        let keyless = Dispatcher::new(&s, system_clock)
            .holding_every_writer(true)
            .whole(true);
        assert!(matches!(
            keyless.dispatch(&parts(&["EVAL", "return redis.call('get', '{a}k')", "0"])),
            Value::Error(e) if e.contains("access key '{a}k'")
        ));
    }

    /// ADR-0053 at queue time: a placed tenant's script may name keys in
    /// several slots, and every other command keeps its own slot rule, which
    /// its storage needs (`RENAME` keeps both keys under the source's slot).
    #[test]
    fn a_placed_tenant_relaxes_only_the_scripts_queue_time_slot_rule() {
        let q = |parts: &[&str], whole: bool| {
            queue_time_error(
                &parts
                    .iter()
                    .map(|p| p.as_bytes().to_vec())
                    .collect::<Vec<_>>(),
                whole,
            )
        };
        let script = ["EVAL", "return 1", "2", "{a}k", "{b}k"];
        assert!(
            q(&script, false)
                .is_some_and(|e| matches!(e, Value::Error(t) if t.starts_with("CROSSSLOT")))
        );
        assert!(q(&script, true).is_none());
        for c in [
            &["MSET", "{a}k", "1", "{b}k", "2"][..],
            &["RENAME", "{a}k", "{b}k"],
        ] {
            assert!(
                q(c, true)
                    .is_some_and(|e| matches!(e, Value::Error(t) if t.starts_with("CROSSSLOT"))),
                "{c:?}"
            );
        }
    }

    /// asynq 0.26's dequeue script, as measured in ADR-0052: it pops a task
    /// id and then writes the task's hash under a key built from ARGV and
    /// that id. Run holding every writer, as `main` re-runs it.
    #[test]
    fn asynqs_dequeue_script_runs_holding_every_writer() {
        const DEQUEUE: &str = r#"
if redis.call("EXISTS", KEYS[2]) == 0 then
	local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[3])
	if id then
		local key = ARGV[2] .. id
		redis.call("HSET", key, "state", "active")
		redis.call("HDEL", key, "pending_since")
		redis.call("ZADD", KEYS[4], ARGV[1], id)
		return redis.call("HGET", key, "msg")
	end
end
return nil"#;
        let s = MemKv::new();
        ev(&s, &["RPUSH", "asynq:{q}:pending", "id1"]);
        ev(
            &s,
            &[
                "HSET",
                "asynq:{q}:t:id1",
                "msg",
                "hello",
                "state",
                "pending",
                "pending_since",
                "1",
            ],
        );
        let args: Vec<Vec<u8>> = [
            "EVAL",
            DEQUEUE,
            "4",
            "asynq:{q}:pending",
            "asynq:{q}:paused",
            "asynq:{q}:active",
            "asynq:{q}:lease",
            "1000",
            "asynq:{q}:t:",
        ]
        .iter()
        .map(|p| p.as_bytes().to_vec())
        .collect();
        let d = Dispatcher::new(&s, system_clock);
        assert!(matches!(d.dispatch(&args), Value::Error(_)));
        assert!(d.wants_every_writer());
        assert_eq!(
            ev(&s, &["LLEN", "asynq:{q}:pending"]),
            Value::Integer(1),
            "abandoned: nothing moved"
        );
        let d = Dispatcher::new(&s, system_clock).holding_every_writer(true);
        assert_eq!(d.dispatch(&args), Value::Bulk(Some(b"hello".to_vec())));
        assert_eq!(
            ev(&s, &["HGET", "asynq:{q}:t:id1", "state"]),
            Value::Bulk(Some(b"active".to_vec()))
        );
        assert_eq!(
            ev(&s, &["HEXISTS", "asynq:{q}:t:id1", "pending_since"]),
            Value::Integer(0)
        );
        assert_eq!(
            ev(&s, &["LRANGE", "asynq:{q}:active", "0", "-1"]),
            Value::Array(Some(vec![Value::Bulk(Some(b"id1".to_vec()))]))
        );
        assert_eq!(
            ev(&s, &["ZSCORE", "asynq:{q}:lease", "id1"]),
            Value::Double(1000.0)
        );
    }

    /// The sandbox: text only, no loaders, read-only globals.
    #[test]
    fn the_sandbox_loads_text_only_and_protects_its_globals() {
        let s = MemKv::new();
        assert!(matches!(
            ev(&s, &["EVAL", "\x1bLua\x51\x00", "0"]),
            Value::Error(e) if e.contains("attempt to load a binary chunk")
        ));
        for name in [
            "load",
            "loadstring",
            "dofile",
            "loadfile",
            "require",
            "os",
            "io",
            "debug",
            "package",
            "setfenv",
            "getfenv",
            "print",
        ] {
            assert!(
                matches!(ev(&s, &["EVAL", &format!("return type({name})"), "0"]), Value::Error(e)
                    if e.contains(&format!("nonexistent global variable '{name}'"))),
                "{name}"
            );
        }
        assert!(matches!(
            ev(&s, &["EVAL", "x = 1", "0"]),
            Value::Error(e) if e == "ERR user_script:1: Attempt to modify a readonly table script: on @user_script:1."
        ));
        assert!(matches!(
            ev(&s, &["EVAL", "setmetatable(_G, nil)", "0"]),
            Value::Error(_)
        ));
        assert_eq!(
            ev(&s, &["EVAL", "return redis.sha1hex('abc')", "0"]),
            Value::Bulk(Some(b"a9993e364706816aba3e25717850c26c9cd0d89d".to_vec()))
        );
    }

    /// A Lua number handed to `redis.call` is spelled as Valkey 9.1 spells
    /// it (measured): integers within half of i64's range, else fpconv.
    #[test]
    fn a_lua_number_argument_is_spelled_as_valkey_spells_it() {
        for (n, want) in [
            (0.1, "0.1"),
            (60000.0, "60000"),
            (1e17, "100000000000000000"),
            (4.5e18, "4500000000000000000"),
            (5e18, "5e+18"),
            (1e21, "1e+21"),
            (1e-7, "1e-7"),
            (1.0 / 3.0, "0.3333333333333333"),
            (123456789.12345679, "1.2345678912345679e+8"),
            (-2.5e300, "-2.5e+300"),
            (-0.0, "0"),
            (1.5, "1.5"),
            (0.000123, "0.000123"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
        ] {
            assert_eq!(
                String::from_utf8(crate::script::lua_number_arg(n)).expect("ascii"),
                want,
                "{n}"
            );
        }
    }

    /// The key a script routes, locks and is owned by is `KEYS[1]`.
    #[test]
    fn a_scripts_key_is_keys_1() {
        let a = |p: &[&str]| p.iter().map(|x| x.as_bytes().to_vec()).collect::<Vec<_>>();
        assert_eq!(
            command_key(&a(&["EVAL", "return 1", "1", "k", "v"])),
            Some(&b"k"[..])
        );
        assert_eq!(
            command_key(&a(&["evalsha", "abc", "2", "k1", "k2"])),
            Some(&b"k1"[..])
        );
        assert_eq!(command_key(&a(&["EVAL", "return 1", "0"])), None);
        assert_eq!(command_key(&a(&["SCRIPT", "LOAD", "x"])), None);
        assert_eq!(command_key(&a(&["KEYS", "*"])), None);
    }

    /// BUG-0182: HMSET is HSET answering +OK, and its errors name it.
    #[test]
    fn hmset_writes_like_hset_and_answers_ok() {
        let s = MemKv::new();
        assert_eq!(
            call(&s, &[b"HMSET", b"h", b"a", b"1", b"b", b"2"]),
            Value::Simple("OK".into())
        );
        assert_eq!(
            call(&s, &[b"HGET", b"h", b"b"]),
            Value::Bulk(Some(b"2".to_vec()))
        );
        assert_eq!(
            call(&s, &[b"HMSET", b"h", b"a"]),
            Value::Error("ERR wrong number of arguments for 'hmset' command".into())
        );
    }

    #[test]
    fn hset_arity_must_be_even_pairs() {
        let s = MemKv::new();
        assert!(matches!(call(&s, &[b"HSET", b"h", b"f"]), Value::Error(_)));
        assert!(matches!(
            call(&s, &[b"HSET", b"h", b"f", b"v", b"g"]),
            Value::Error(_)
        ));
    }

    /// DBSIZE's contract from the 100M-key OOM (docs/bench, 2026-07-13
    /// EC2 run): it must stream the metadata CF via `for_each_prefix`,
    /// never materialize it with `scan_prefix`. The spy store forwards
    /// everything to a real MemKv but fails the test if the materializing
    /// path is hit.
    struct NoMaterializeKv(MemKv);

    impl Kv for NoMaterializeKv {
        fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
            self.0.get(key)
        }
        fn put(&self, key: &[u8], value: &[u8]) {
            self.0.put(key, value)
        }
        fn delete(&self, key: &[u8]) -> bool {
            self.0.delete(key)
        }
        fn for_each_prefix(&self, prefix: &[u8], visit: &mut dyn FnMut(&[u8], &[u8]) -> bool) {
            self.0.for_each_prefix(prefix, visit)
        }
        fn scan_prefix(&self, _prefix: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
            panic!("DBSIZE must stream via for_each_prefix, not materialize via scan_prefix")
        }
        fn clear(&self) {
            self.0.clear()
        }
    }

    #[test]
    fn dbsize_streams_and_skips_expired() {
        let s = NoMaterializeKv(MemKv::new());
        let d = Dispatcher::new(&s, system_clock);
        let call =
            |parts: &[&[u8]]| d.dispatch(&parts.iter().map(|p| p.to_vec()).collect::<Vec<_>>());
        assert_eq!(call(&[b"DBSIZE"]), Value::Integer(0));
        assert_eq!(call(&[b"SET", b"k1", b"v"]), Value::Simple("OK".into()));
        assert_eq!(call(&[b"SET", b"k2", b"v"]), Value::Simple("OK".into()));
        // Already expired at write time: physically present, not counted.
        assert_eq!(
            call(&[b"SET", b"dead", b"v", b"PXAT", b"1"]),
            Value::Simple("OK".into())
        );
        assert_eq!(call(&[b"DBSIZE"]), Value::Integer(2));
    }

    /// The max-value-bytes policy surfaces on the wire with one stable
    /// error string, for strings and collections alike.
    #[test]
    fn max_value_bytes_policy_rejects_on_the_wire() {
        let s = MemKv::new();
        let d = Dispatcher::with_limits(
            &s,
            system_clock,
            Limits {
                max_value_bytes: 16,
                ..Default::default()
            },
            DEFAULT_NS,
        );
        let call =
            |parts: &[&[u8]]| d.dispatch(&parts.iter().map(|p| p.to_vec()).collect::<Vec<_>>());
        let too_large =
            Value::Error("ERR value exceeds maximum allowed size (max-value-bytes)".into());
        assert_eq!(call(&[b"SET", b"k", &[b'x'; 17]]), too_large);
        assert_eq!(call(&[b"SET", b"k", b"small"]), Value::Simple("OK".into()));
        assert_eq!(call(&[b"APPEND", b"k", &[b'y'; 12]]), too_large);
        assert_eq!(
            call(&[b"HSET", b"h", b"field", b"0123456789abcdef"]),
            too_large
        );
        assert_eq!(call(&[b"RPUSH", b"l", &[b'e'; 17]]), too_large);
        assert_eq!(call(&[b"SADD", b"s", &[b'm'; 17]]), too_large);
        assert_eq!(call(&[b"ZADD", b"z", b"1", &[b'q'; 9]]), too_large);
    }

    /// The key cap: the structural 64KB ceiling is always on (the subkey
    /// envelope frames key length as u16 — an oversized key would corrupt
    /// it), and --max-key-bytes can only lower it. Reads and writes,
    /// single- and multi-key commands alike.
    #[test]
    fn key_size_ceiling_is_always_enforced() {
        let s = MemKv::new();
        // Policy OFF (0 = ceiling only), so what this asserts really is the
        // STRUCTURAL limit and not the 4 KiB default sitting in front of it.
        let d = Dispatcher::with_limits(
            &s,
            system_clock,
            Limits {
                max_key_bytes: 0,
                ..Default::default()
            },
            DEFAULT_NS,
        );
        let call =
            |parts: &[&[u8]]| d.dispatch(&parts.iter().map(|p| p.to_vec()).collect::<Vec<_>>());
        let too_long = Value::Error("ERR key exceeds maximum allowed size (max-key-bytes)".into());
        let over = vec![b'k'; 65_536];
        let at = vec![b'k'; 65_535];
        // Writes: complex types would hit the envelope; strings stay
        // consistent with them.
        assert_eq!(call(&[b"HSET", &over, b"f", b"v"]), too_long);
        assert_eq!(call(&[b"SET", &over, b"v"]), too_long);
        // Reads too — an oversized key must never reach a prefix builder.
        assert_eq!(call(&[b"HGETALL", &over]), too_long);
        // Multi-key shapes: the oversized key is not at args[1].
        assert_eq!(call(&[b"DEL", b"ok", &over]), too_long);
        assert_eq!(call(&[b"MSET", b"ok", b"v", &over, b"v"]), too_long);
        // At the ceiling everything works.
        assert_eq!(call(&[b"HSET", &at, b"f", b"v"]), Value::Integer(1));
        assert_eq!(call(&[b"DEL", &at]), Value::Integer(1));
        assert_eq!(call(&[b"ZADD", &at, b"1", b"m"]), Value::Integer(1));
    }

    /// The shipped default is 4 KiB — ElastiCache Serverless's key ceiling
    /// — so a key that works on the service people are migrating from works
    /// here, and one that does not is refused at both ends rather than
    /// discovered in production.
    #[test]
    fn default_key_cap_matches_the_managed_service_ceiling() {
        assert_eq!(flint_storage::DEFAULT_MAX_KEY_BYTES, 4096);
        let s = MemKv::new();
        let d = Dispatcher::new(&s, system_clock);
        let call =
            |parts: &[&[u8]]| d.dispatch(&parts.iter().map(|p| p.to_vec()).collect::<Vec<_>>());
        let at = vec![b'k'; 4096];
        let over = vec![b'k'; 4097];
        assert_eq!(call(&[b"SET", &at, b"v"]), Value::Simple("OK".into()));
        assert_eq!(
            call(&[b"SET", &over, b"v"]),
            Value::Error("ERR key exceeds maximum allowed size (max-key-bytes)".into())
        );
        // Refused on the way in: nothing was written under the long key.
        assert_eq!(call(&[b"EXISTS", &at]), Value::Integer(1));
    }

    #[test]
    fn max_key_bytes_can_lower_but_not_raise_the_ceiling() {
        let s = MemKv::new();
        let d = Dispatcher::with_limits(
            &s,
            system_clock,
            Limits {
                max_key_bytes: 8,
                ..Default::default()
            },
            DEFAULT_NS,
        );
        let call =
            |parts: &[&[u8]]| d.dispatch(&parts.iter().map(|p| p.to_vec()).collect::<Vec<_>>());
        let too_long = Value::Error("ERR key exceeds maximum allowed size (max-key-bytes)".into());
        assert_eq!(call(&[b"SET", b"ninechars", b"v"]), too_long);
        assert_eq!(
            call(&[b"SET", b"eightchr", b"v"]),
            Value::Simple("OK".into())
        );

        // A configured value above the ceiling clamps back down to it.
        let raised = Dispatcher::with_limits(
            &s,
            system_clock,
            Limits {
                max_key_bytes: u64::MAX,
                ..Default::default()
            },
            DEFAULT_NS,
        );
        let over = vec![b'k'; 65_536];
        assert_eq!(
            raised.dispatch(&[b"HSET".to_vec(), over, b"f".to_vec(), b"v".to_vec()]),
            too_long
        );
    }

    #[test]
    fn string_commands_still_work() {
        let s = MemKv::new();
        assert_eq!(call(&s, &[b"SET", b"k", b"v"]), Value::Simple("OK".into()));
        assert_eq!(call(&s, &[b"GET", b"k"]), Value::Bulk(Some(b"v".to_vec())));
        assert_eq!(call(&s, &[b"INCR", b"c"]), Value::Integer(1));
        assert_eq!(call(&s, &[b"TTL", b"k"]), Value::Integer(-1));
        assert_eq!(call(&s, &[b"EXPIRE", b"k", b"100"]), Value::Integer(1));
        assert_eq!(call(&s, &[b"PERSIST", b"k"]), Value::Integer(1));
    }

    /// Sorted members of a set reply, so assertions do not depend on the
    /// hash order the store happens to produce.
    fn members(v: Value) -> Vec<Vec<u8>> {
        let Value::Set(ms) = v else {
            panic!("expected a set reply, got {v:?}");
        };
        let mut out: Vec<Vec<u8>> = ms
            .into_iter()
            .map(|m| match m {
                Value::Bulk(Some(b)) => b,
                other => panic!("expected a bulk member, got {other:?}"),
            })
            .collect();
        out.sort();
        out
    }

    /// A cross-slot set operation must be REFUSED with an error, never
    /// answered.
    ///
    /// The failure guarded here is not a crash, which is why it needs a
    /// test at all: it is a *plausible* answer. A key whose slot this node
    /// does not own reads as an empty set, so an unchecked cross-slot
    /// SINTER returns the empty set — a well-formed reply that is silently
    /// wrong, and that no client can distinguish from a real empty
    /// intersection.
    ///
    /// This assertion cannot live in the conformance suite: every case
    /// there is compared against real Valkey, and standalone Valkey has no
    /// slots, so it answers cross-slot set operations happily. The refusal
    /// is a deliberate divergence from standalone semantics toward cluster
    /// semantics, so it has to be asserted here.
    /// BUG-0053: MGET and MSET reached neither cross-slot guard, so a key on
    /// another pair came back as a nil IN ITS CORRECT POSITION — a miss the
    /// caller cannot distinguish from absence — and MSET wrote the value onto
    /// a node that does not own the slot.
    ///
    /// The proxy cannot catch this: `route_key` derives one slot from args[1]
    /// and ships the whole command there, which is a documented v0 deferral.
    /// So this refusal is the only enforcement, exactly as it is for the set
    /// ops.
    #[test]
    fn cross_slot_mget_and_mset_are_refused_rather_than_answered_wrongly() {
        // Capability assert, same reason as the set-op test above: if these
        // keys stop hashing apart this test becomes a tautology, and it must
        // fail here naming why rather than pass vacuously.
        assert_ne!(
            slot_for_key(b"alpha"),
            slot_for_key(b"beta"),
            "stale corpus: these keys no longer land in different slots, \
             so this test would pass without exercising the refusal"
        );

        let s = MemKv::new();
        call(&s, &[b"SET", b"alpha", b"1"]);
        call(&s, &[b"SET", b"beta", b"2"]);

        for cmd in [
            &[&b"MGET"[..], b"alpha", b"beta"][..],
            &[&b"MSET"[..], b"alpha", b"1", b"beta", b"2"][..],
        ] {
            let name = String::from_utf8_lossy(cmd[0]).into_owned();
            match call(&s, cmd) {
                Value::Error(e) => {
                    assert!(
                        e.starts_with("CROSSSLOT"),
                        "{name}: the refusal must carry the CROSSSLOT code clients \
                         key off, got {e}"
                    );
                    assert!(
                        e.contains("alpha") && e.contains("beta"),
                        "{name}: the error must name BOTH keys, or the caller \
                         cannot tell which pair collided: {e}"
                    );
                }
                other => panic!(
                    "{name}: answered a cross-slot request instead of refusing it, \
                     which is the silent-wrong-answer this guard exists to stop: {other:?}"
                ),
            }
        }
    }

    /// BUG-0187: a move between lists on two slots would pop from one node
    /// and push onto a list this node does not own. It is refused, and the
    /// refusal moves nothing.
    #[test]
    fn a_cross_slot_list_move_is_refused_and_moves_nothing() {
        assert_ne!(
            slot_for_key(b"alpha"),
            slot_for_key(b"beta"),
            "stale corpus: these keys no longer land in different slots"
        );
        let s = MemKv::new();
        call(&s, &[b"RPUSH", b"alpha", b"a", b"b"]);
        for cmd in [
            &[&b"LMOVE"[..], b"alpha", b"beta", b"LEFT", b"RIGHT"][..],
            &[&b"RPOPLPUSH"[..], b"alpha", b"beta"][..],
        ] {
            let name = String::from_utf8_lossy(cmd[0]).into_owned();
            match call(&s, cmd) {
                Value::Error(e) => assert!(
                    e.starts_with("CROSSSLOT") && e.contains("alpha") && e.contains("beta"),
                    "{name}: expected a CROSSSLOT refusal naming both keys, got {e}"
                ),
                other => panic!("{name}: moved across slots instead of refusing: {other:?}"),
            }
        }
        assert_eq!(call(&s, &[b"LLEN", b"alpha"]), Value::Integer(2));
    }

    /// Valkey refuses a float spelled past a double's range (strtod's
    /// ERANGE), where Rust's parser rounds it to infinity or zero. A score
    /// of `1e400` was stored as inf.
    #[test]
    fn a_float_spelled_past_the_range_of_a_double_is_not_a_float() {
        for bad in [
            &b"1e400"[..],
            b"-1e400",
            b"1e-400",
            b"-1e-400",
            b"nan",
            b"0x10",
            b"",
        ] {
            assert!(
                parse_f64(bad).is_err(),
                "{} must not parse",
                String::from_utf8_lossy(bad)
            );
        }
        for (good, want) in [
            (&b"inf"[..], f64::INFINITY),
            (b"-Infinity", f64::NEG_INFINITY),
            (b"+INF", f64::INFINITY),
            (b"0", 0.0),
            (b"-0.000e-400", 0.0),
            (b"1e-310", 1e-310),
            (b"1.5e2", 150.0),
        ] {
            assert_eq!(
                parse_f64(good),
                Ok(want),
                "{} must parse",
                String::from_utf8_lossy(good)
            );
        }
    }

    /// The other half, and the one that makes the refusal a discriminator
    /// rather than a blanket ban on multi-key calls: colocated keys must
    /// still be ANSWERED, and answered correctly.
    #[test]
    fn same_slot_mget_and_mset_are_answered_normally() {
        assert_eq!(
            slot_for_key(b"{s}alpha"),
            slot_for_key(b"{s}beta"),
            "hash tags no longer colocate, so this control proves nothing"
        );

        let s = MemKv::new();
        match call(&s, &[b"MSET", b"{s}alpha", b"1", b"{s}beta", b"2"]) {
            Value::Simple(ok) => assert_eq!(ok, "OK", "colocated MSET must succeed"),
            other => panic!("colocated MSET was refused: {other:?}"),
        }
        match call(&s, &[b"MGET", b"{s}alpha", b"{s}missing", b"{s}beta"]) {
            Value::Array(Some(v)) => {
                assert_eq!(v.len(), 3, "MGET must answer one element per key");
                assert_eq!(v[0], Value::Bulk(Some(b"1".to_vec())));
                // The nil stays IN ITS SLOT — the property the S3 accelerator
                // chunk-indexes against, confirmed here rather than assumed.
                assert_eq!(v[1], Value::Bulk(None), "a miss must not compact the reply");
                assert_eq!(v[2], Value::Bulk(Some(b"2".to_vec())));
            }
            other => panic!("colocated MGET was refused: {other:?}"),
        }
    }

    #[test]
    fn a_cross_slot_set_op_is_refused_rather_than_answered_wrongly() {
        // Capability assert: this corpus tests what it claims only if the
        // two keys genuinely hash apart. A hashing change must fail HERE,
        // naming the reason, instead of quietly turning the test into a
        // tautology that passes because nothing is cross-slot any more.
        assert_ne!(
            slot_for_key(b"alpha"),
            slot_for_key(b"beta"),
            "stale corpus: these keys no longer land in different slots, \
             so this test would pass without exercising the refusal"
        );

        let s = MemKv::new();
        call(&s, &[b"SADD", b"alpha", b"x", b"y"]);
        call(&s, &[b"SADD", b"beta", b"y", b"z"]);

        for op in [&b"SINTER"[..], b"SUNION", b"SDIFF"] {
            let name = String::from_utf8_lossy(op).into_owned();
            match call(&s, &[op, b"alpha", b"beta"]) {
                Value::Error(e) => {
                    assert!(
                        e.starts_with("CROSSSLOT"),
                        "{name}: the refusal must carry the CROSSSLOT code clients \
                         already know from Redis Cluster, got: {e}"
                    );
                    // An operator reading this should not have to compute
                    // slots by hand to find out which keys collided.
                    assert!(
                        e.contains("alpha") && e.contains("beta"),
                        "{name}: the error must name both offending keys, got: {e}"
                    );
                }
                other => panic!(
                    "{name}: a cross-slot request must be refused, not answered — got {other:?}"
                ),
            }
        }
    }

    /// The positive control for the refusal above.
    ///
    /// Without it, a build in which the set operations were broken outright
    /// — always erroring, never dispatching — would still pass the
    /// cross-slot test. This proves the same members under ONE hash tag are
    /// answered, and answered correctly, so the refusal is discriminating
    /// between slots rather than failing everything.
    #[test]
    fn the_same_members_under_one_hash_tag_are_answered() {
        // The error tells users to colocate with a hash tag. If that advice
        // ever stops working, this fails before the assertions below.
        assert_eq!(
            slot_for_key(b"{s}alpha"),
            slot_for_key(b"{s}beta"),
            "the hash tag the CROSSSLOT error recommends no longer colocates keys"
        );

        let s = MemKv::new();
        call(&s, &[b"SADD", b"{s}alpha", b"x", b"y"]);
        call(&s, &[b"SADD", b"{s}beta", b"y", b"z"]);

        assert_eq!(
            members(call(&s, &[b"SINTER", b"{s}alpha", b"{s}beta"])),
            vec![b"y".to_vec()]
        );
        assert_eq!(
            members(call(&s, &[b"SUNION", b"{s}alpha", b"{s}beta"])),
            vec![b"x".to_vec(), b"y".to_vec(), b"z".to_vec()]
        );
        assert_eq!(
            members(call(&s, &[b"SDIFF", b"{s}alpha", b"{s}beta"])),
            vec![b"x".to_vec()]
        );
    }

    /// The empty set is the answer a broken cross-slot path would forge, so
    /// assert the node can still produce it legitimately. A refusal that
    /// swallowed every empty intersection would be its own bug.
    #[test]
    fn a_genuinely_empty_intersection_is_still_an_empty_set() {
        let s = MemKv::new();
        call(&s, &[b"SADD", b"{s}alpha", b"x"]);
        call(&s, &[b"SADD", b"{s}beta", b"z"]);
        assert_eq!(
            members(call(&s, &[b"SINTER", b"{s}alpha", b"{s}beta"])),
            Vec::<Vec<u8>>::new()
        );
    }
}
