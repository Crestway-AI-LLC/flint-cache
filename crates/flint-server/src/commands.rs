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
use flint_storage::lists::ListStore;
use flint_storage::sets::SetStore;
use flint_storage::strings::{Clock, SetExpiry, SetOptions, SetOutcome, StoreError, StringStore};
use flint_storage::zsets::ZSetStore;

/// v0 runs a single default namespace; tenancy arrives with the proxy.
const NS: &[u8] = b"0";

pub struct Dispatcher<'a> {
    keyspace: Keyspace<'a>,
    strings: StringStore<'a>,
    hashes: HashStore<'a>,
    sets: SetStore<'a>,
    lists: ListStore<'a>,
    zsets: ZSetStore<'a>,
    kv: &'a dyn Kv,
    clock: Clock,
}

impl<'a> Dispatcher<'a> {
    pub fn new(kv: &'a dyn Kv, clock: Clock) -> Self {
        Self {
            keyspace: Keyspace::new(kv, NS, clock),
            strings: StringStore::new(kv, NS, clock),
            hashes: HashStore::new(kv, NS, clock),
            sets: SetStore::new(kv, NS, clock),
            lists: ListStore::new(kv, NS, clock),
            zsets: ZSetStore::new(kv, NS, clock),
            kv,
            clock,
        }
    }

    pub fn dispatch(&self, args: &[Vec<u8>]) -> Value {
        let Some(name) = args.first() else {
            return err("ERR empty command");
        };
        match name.to_ascii_uppercase().as_slice() {
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

            // hashes
            b"HSET" => self.cmd_hset(args),
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

            // zsets
            b"ZADD" => self.cmd_zadd(args),
            b"ZSCORE" => exact(args, 3, "zscore", |a| {
                reply(self.zsets.zscore(slot_for_key(&a[1]), &a[1], &a[2]), |s| {
                    Value::Bulk(s.map(fmt_score))
                })
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

            // keyspace (type-agnostic)
            b"DEL" => multi_key(args, "del", |k| self.keyspace.del(slot_for_key(k), k)),
            b"EXISTS" => multi_key(args, "exists", |k| self.keyspace.exists(slot_for_key(k), k)),
            b"TYPE" => exact(args, 2, "type", |a| {
                match self.keyspace.value_type(slot_for_key(&a[1]), &a[1]) {
                    Some(t) => Value::Simple(t.name().into()),
                    None => Value::Simple("none".into()),
                }
            }),
            b"EXPIRE" => self.cmd_expire(args, "expire", 1000),
            b"PEXPIRE" => self.cmd_expire(args, "pexpire", 1),
            b"TTL" => self.cmd_ttl(args, "ttl", 1000),
            b"PTTL" => self.cmd_ttl(args, "pttl", 1),
            b"PERSIST" => exact(args, 2, "persist", |a| {
                Value::Integer(self.keyspace.persist(slot_for_key(&a[1]), &a[1]) as i64)
            }),

            // admin
            b"FLUSHALL" => {
                self.kv.clear();
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
        let mut i = 3;
        while i < args.len() {
            match args[i].to_ascii_uppercase().as_slice() {
                b"NX" => opts.nx = true,
                b"XX" => opts.xx = true,
                b"KEEPTTL" => opts.expiry = SetExpiry::Keep,
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
        match self.strings.set(slot_for_key(key), key, value, opts) {
            Ok(SetOutcome::Done) => Value::Simple("OK".into()),
            Ok(SetOutcome::Unchanged) => Value::Bulk(None),
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
            4..=5 => return err("ERR syntax error"),
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

fn store_err(e: StoreError) -> Value {
    match e {
        StoreError::NotInteger | StoreError::Overflow => {
            err("ERR value is not an integer or out of range")
        }
        StoreError::WrongType => {
            err("WRONGTYPE Operation against a key holding the wrong kind of value")
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
