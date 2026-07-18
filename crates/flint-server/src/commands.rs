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
use flint_storage::hashes::HashStore;
use flint_storage::keyspace::{Keyspace, Ttl};
use flint_storage::lists::{ListStore, LsetOutcome};
use flint_storage::sets::SetStore;
use flint_storage::strings::{Clock, SetExpiry, SetOptions, SetOutcome, StoreError, StringStore};
use flint_storage::zsets::{ScoreBound, ZSetStore};

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
/// args[1]; FLINT* admin/replication commands are intercepted before this, so
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
        b"COMMAND",
        b"CLUSTER",
        b"INFO",
        b"SELECT",
        b"QUIT",
        b"HELLO",
    ];
    if NO_KEY.iter().any(|c| name.eq_ignore_ascii_case(c)) {
        return None;
    }
    args.get(1).map(|k| k.as_slice())
}

/// The default namespace: unauthenticated/direct connections and every
/// pre-tenancy tool operate here. Tenant connections select their own via
/// FLINTNS (set by the proxy after token auth).
pub const DEFAULT_NS: &[u8] = b"0";

/// Wire-facing policy limits, plumbed from the CLI.
#[derive(Clone, Copy)]
pub struct Limits {
    /// Cap on any single value's total payload; 0 disables.
    pub max_value_bytes: u64,
    /// Cap on user-key length. Always clamped to the envelope's
    /// structural ceiling (`flint_storage::MAX_KEY_BYTES`); 0 means
    /// "ceiling only".
    pub max_key_bytes: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_value_bytes: flint_storage::DEFAULT_MAX_VALUE_BYTES,
            max_key_bytes: flint_storage::MAX_KEY_BYTES,
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
    kv: &'a dyn Kv,
    clock: Clock,
    limits: Limits,
    ns: Vec<u8>,
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
            kv,
            clock,
            limits,
            ns: ns.to_vec(),
        }
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
    /// args[1]; DEL/EXISTS are all-keys; MSET keys sit at odd indices.
    /// Enforced for reads and writes alike — an oversized key must never
    /// reach the envelope builders (their length frame is 2 bytes).
    fn has_oversized_key(&self, name_upper: &[u8], args: &[Vec<u8>]) -> bool {
        let max = self.limits.effective_max_key() as usize;
        match name_upper {
            b"PING" | b"ECHO" | b"DBSIZE" | b"FLUSHALL" | b"COMMAND" | b"CLUSTER" | b"INFO"
            | b"SELECT" | b"QUIT" | b"HELLO" => false,
            b"DEL" | b"EXISTS" => args[1..].iter().any(|k| k.len() > max),
            b"MSET" => args[1..].iter().step_by(2).any(|k| k.len() > max),
            _ => args.get(1).is_some_and(|k| k.len() > max),
        }
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
            b"SETEX" => exact(args, 4, "setex", |a| match parse_i64(&a[2]) {
                Ok(secs) if secs > 0 => {
                    let at = ((self.clock)()).saturating_add(secs as u64 * 1000);
                    let opts = SetOptions {
                        expiry: SetExpiry::AtMs(at),
                        ..Default::default()
                    };
                    match self.strings.set(slot_for_key(&a[1]), &a[1], &a[3], opts) {
                        Ok(_) => Value::Simple("OK".into()),
                        Err(e) => store_err(e),
                    }
                }
                Ok(_) => err("ERR invalid expire time in 'setex' command"),
                Err(_) => err("ERR value is not an integer or out of range"),
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
                Err(_) => err("ERR value is not a valid float"),
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
                // Redis MGET yields nil (not an error) for wrong-type keys.
                Value::Array(Some(
                    args[1..]
                        .iter()
                        .map(|k| Value::Bulk(self.strings.get(slot_for_key(k), k).unwrap_or(None)))
                        .collect(),
                ))
            }

            // hashes
            b"HSET" => self.cmd_hset(args),
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
                Ok(delta) => reply(
                    self.hashes
                        .hincr_by(slot_for_key(&a[1]), &a[1], &a[2], delta),
                    Value::Integer,
                ),
                Err(_) => err("ERR value is not an integer or out of range"),
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
                    let mut out = Vec::with_capacity(pairs.len() * 2);
                    for (f, v) in pairs {
                        out.push(Value::Bulk(Some(f)));
                        out.push(Value::Bulk(Some(v)));
                    }
                    Value::Array(Some(out))
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
                    Value::Array(Some(ms.into_iter().map(|m| Value::Bulk(Some(m))).collect()))
                })
            }),
            b"SCARD" => exact(args, 2, "scard", |a| {
                reply(self.sets.scard(slot_for_key(&a[1]), &a[1]), |n| {
                    Value::Integer(n as i64)
                })
            }),
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
            b"LPOP" | b"RPOP" => exact(args, 2, "lpop", |a| {
                let left = name.eq_ignore_ascii_case(b"LPOP");
                reply(
                    self.lists.pop(slot_for_key(&a[1]), &a[1], left),
                    Value::Bulk,
                )
            }),
            b"LINDEX" => exact(args, 3, "lindex", |a| match parse_i64(&a[2]) {
                Ok(rank) => reply(
                    self.lists.lindex(slot_for_key(&a[1]), &a[1], rank),
                    Value::Bulk,
                ),
                Err(_) => err("ERR value is not an integer or out of range"),
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
                Err(_) => err("ERR value is not an integer or out of range"),
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
                    Value::Bulk(s.map(fmt_score))
                })
            }),
            b"ZINCRBY" => exact(args, 4, "zincrby", |a| match parse_f64(&a[2]) {
                Ok(delta) => reply(
                    self.zsets
                        .zincr_by(slot_for_key(&a[1]), &a[1], delta, &a[3]),
                    |sc| Value::Bulk(Some(fmt_score(sc))),
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
            b"ZRANK" => self.cmd_zrank(args, "zrank", false),
            b"ZREVRANK" => self.cmd_zrank(args, "zrevrank", true),
            b"ZCOUNT" => self.cmd_zcount(args),
            b"ZMSCORE" => self.cmd_zmscore(args),
            b"ZPOPMIN" => self.cmd_zpop(args, "zpopmin", false),
            b"ZPOPMAX" => self.cmd_zpop(args, "zpopmax", true),
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
            b"FLUSHALL" => {
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
            b"HELLO" => err("ERR unsupported protocol version; this server speaks RESP2"),
            other => {
                let name = String::from_utf8_lossy(other).to_lowercase();
                err(&format!("ERR unknown command '{name}'"))
            }
        }
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
        let mut i = 3;
        while i < args.len() {
            match args[i].to_ascii_uppercase().as_slice() {
                b"NX" => opts.nx = true,
                b"XX" => opts.xx = true,
                b"KEEPTTL" => opts.expiry = SetExpiry::Keep,
                b"GET" => want_get = true,
                b"EX" | b"PX" | b"EXAT" | b"PXAT" => {
                    let unit_ms =
                        matches!(args[i].to_ascii_uppercase().as_slice(), b"EX" | b"EXAT");
                    let absolute =
                        matches!(args[i].to_ascii_uppercase().as_slice(), b"EXAT" | b"PXAT");
                    let Some(raw) = args.get(i + 1) else {
                        return err("ERR syntax error");
                    };
                    let Ok(n) = parse_i64(raw) else {
                        return err("ERR value is not an integer or out of range");
                    };
                    if n <= 0 && !absolute {
                        return err("ERR invalid expire time in 'set' command");
                    }
                    let ms = if unit_ms { n.saturating_mul(1000) } else { n } as u64;
                    let at = if absolute {
                        ms
                    } else {
                        ((self.clock)()).saturating_add(ms)
                    };
                    opts.expiry = SetExpiry::AtMs(at);
                    i += 1;
                }
                _ => return err("ERR syntax error"),
            }
            i += 1;
        }
        if opts.nx && opts.xx {
            return err("ERR syntax error");
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

    fn cmd_hset(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 4 || !args.len().is_multiple_of(2) {
            return arity_err("hset");
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

    fn cmd_zadd(&self, args: &[Vec<u8>]) -> Value {
        if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
            return arity_err("zadd");
        }
        let mut pairs = Vec::with_capacity((args.len() - 2) / 2);
        for chunk in args[2..].chunks(2) {
            let Ok(score) = parse_f64(&chunk[0]) else {
                return err("ERR value is not a valid float");
            };
            pairs.push((score, chunk[1].clone()));
        }
        reply(
            self.zsets.zadd(slot_for_key(&args[1]), &args[1], &pairs),
            |n| Value::Integer(n as i64),
        )
    }

    fn cmd_zrange(&self, args: &[Vec<u8>]) -> Value {
        let withscores = match args.len() {
            4 => false,
            5 if args[4].eq_ignore_ascii_case(b"WITHSCORES") => true,
            5 => return err("ERR syntax error"),
            _ => return arity_err("zrange"),
        };
        match (parse_i64(&args[2]), parse_i64(&args[3])) {
            (Ok(start), Ok(stop)) => reply(
                self.zsets
                    .zrange(slot_for_key(&args[1]), &args[1], start, stop),
                |ranked| {
                    let mut out = Vec::new();
                    for (member, score) in ranked {
                        out.push(Value::Bulk(Some(member)));
                        if withscores {
                            out.push(Value::Bulk(Some(fmt_score(score))));
                        }
                    }
                    Value::Array(Some(out))
                },
            ),
            _ => err("ERR value is not an integer or out of range"),
        }
    }

    fn zrows(ranked: Vec<(Vec<u8>, f64)>, withscores: bool) -> Value {
        let mut out = Vec::new();
        for (member, score) in ranked {
            out.push(Value::Bulk(Some(member)));
            if withscores {
                out.push(Value::Bulk(Some(fmt_score(score))));
            }
        }
        Value::Array(Some(out))
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

    /// ZRANGEBYSCORE / ZREVRANGEBYSCORE key min max [WITHSCORES]
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

    fn cmd_zrank(&self, args: &[Vec<u8>], name: &str, rev: bool) -> Value {
        exact(args, 3, name, |a| {
            reply(
                self.zsets.zrank(slot_for_key(&a[1]), &a[1], &a[2], rev),
                |o| match o {
                    Some(rank) => Value::Integer(rank as i64),
                    None => Value::Bulk(None),
                },
            )
        })
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
                        .map(|s| Value::Bulk(s.map(fmt_score)))
                        .collect(),
                ))
            },
        )
    }

    fn cmd_zpop(&self, args: &[Vec<u8>], name: &str, max_end: bool) -> Value {
        let count = match args.len() {
            2 => 1usize,
            3 => match parse_i64(&args[2]) {
                Ok(n) if n >= 0 => n as usize,
                Ok(_) => return err("ERR value is out of range, must be positive"),
                Err(_) => return err("ERR value is not an integer or out of range"),
            },
            _ => return arity_err(name),
        };
        reply(
            self.zsets
                .zpop(slot_for_key(&args[1]), &args[1], count, max_end),
            |r| Self::zrows(r, true),
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

    /// HSCAN/SSCAN/ZSCAN key cursor [MATCH pat] [COUNT n] [NOVALUES].
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
                        vec![Value::Bulk(Some(m)), Value::Bulk(Some(fmt_score(sc)))]
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

    /// SPOP key [count]. Without count: single bulk (or nil). With count:
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
                    |ms| Value::Array(Some(ms.into_iter().map(|m| Value::Bulk(Some(m))).collect())),
                ),
                _ => err("ERR value is out of range, must be positive"),
            },
            _ => arity_err("spop"),
        }
    }

    /// SRANDMEMBER key [count]. Without count: single bulk (or nil). With
    /// count: array — positive is distinct-clamped, negative repeats.
    fn cmd_srandmember(&self, args: &[Vec<u8>]) -> Value {
        match args.len() {
            2 => match self.sets.srandmember(slot_for_key(&args[1]), &args[1], 1) {
                Ok(mut picks) => Value::Bulk(picks.pop()),
                Err(e) => store_err(e),
            },
            3 => match parse_i64(&args[2]) {
                Ok(n) => reply(
                    self.sets.srandmember(slot_for_key(&args[1]), &args[1], n),
                    |ms| Value::Array(Some(ms.into_iter().map(|m| Value::Bulk(Some(m))).collect())),
                ),
                Err(_) => err("ERR value is not an integer or out of range"),
            },
            _ => arity_err("srandmember"),
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
                            "ERR RANK can't be zero, use 1 to start searching from the first \
                             matching element in the head of the list or -1 for the tail",
                        );
                    }
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
        let cap = count.map_or(1, |c| c);
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
        exact(args, 3, name, |a| match parse_i64(&a[2]) {
            Ok(n) => {
                let delta = n.saturating_mul(unit_ms as i64);
                let at = if delta <= 0 {
                    1 // already in the past → delete-on-touch semantics
                } else {
                    ((self.clock)()).saturating_add(delta as u64)
                };
                Value::Integer(self.keyspace.expire_at(slot_for_key(&a[1]), &a[1], at) as i64)
            }
            Err(_) => err("ERR value is not an integer or out of range"),
        })
    }

    /// EXPIREAT/PEXPIREAT: the argument is an ABSOLUTE instant (s or ms).
    fn cmd_expire_at(&self, args: &[Vec<u8>], name: &str, unit_ms: u64) -> Value {
        exact(args, 3, name, |a| match parse_i64(&a[2]) {
            Ok(n) => {
                let at = (n.saturating_mul(unit_ms as i64)).max(1) as u64;
                Value::Integer(self.keyspace.expire_at(slot_for_key(&a[1]), &a[1], at) as i64)
            }
            Err(_) => err("ERR value is not an integer or out of range"),
        })
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
                Ttl::Ms(ms) => Value::Integer(ms.div_ceil(unit_ms) as i64),
            }
        })
    }

    fn cmd_incr_delta(&self, args: &[Vec<u8>], name: &str, sign: i64) -> Value {
        exact(args, 3, name, |a| match parse_i64(&a[2]) {
            Ok(delta) => reply(
                self.strings
                    .incr_by(slot_for_key(&a[1]), &a[1], delta.saturating_mul(sign)),
                Value::Integer,
            ),
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
        StoreError::NotInteger | StoreError::Overflow => {
            err("ERR value is not an integer or out of range")
        }
        StoreError::NotFloat => err("ERR value is not a valid float"),
        StoreError::NanOrInfinity => err("ERR increment would produce NaN or Infinity"),
        StoreError::WrongType => {
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
        }
        StoreError::ValueTooLarge => {
            err("ERR value exceeds maximum allowed size (max-value-bytes)")
        }
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

/// Redis-compatible score formatting: integers print without a decimal
/// point; everything else uses shortest-roundtrip.
fn fmt_score(s: f64) -> Vec<u8> {
    if s.fract() == 0.0 && s.is_finite() && s.abs() < 1e17 {
        format!("{}", s as i64).into_bytes()
    } else {
        format!("{s}").into_bytes()
    }
}

fn parse_f64(raw: &[u8]) -> Result<f64, ()> {
    let s = std::str::from_utf8(raw).map_err(|_| ())?;
    let v: f64 = s.parse().map_err(|_| ())?;
    if v.is_nan() { Err(()) } else { Ok(v) }
}

fn parse_i64(raw: &[u8]) -> Result<i64, ()> {
    std::str::from_utf8(raw)
        .map_err(|_| ())?
        .parse()
        .map_err(|_| ())
}

fn err(msg: &str) -> Value {
    Value::Error(msg.into())
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

    fn call(kv: &MemKv, parts: &[&[u8]]) -> Value {
        let d = Dispatcher::new(kv, system_clock);
        d.dispatch(&parts.iter().map(|p| p.to_vec()).collect::<Vec<_>>())
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
        let d = Dispatcher::new(&s, system_clock);
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
}
