//! Command dispatch: `Vec<arg-bytes>` in, RESP `Value` out.
//!
//! Commands route through the encoding layer (StringStore) with the slot
//! computed per key — the same data path the distributed system will use.
//! Reply shapes follow Redis exactly; the conformance oracle is the referee.

use flint_resp::Value;
use flint_slot::slot_for_key;
use flint_storage::Kv;
use flint_storage::strings::{
    Clock, SetExpiry, SetOptions, SetOutcome, StoreError, StringStore, Ttl,
};

/// v0 runs a single default namespace; tenancy arrives with the proxy.
const NS: &[u8] = b"0";

pub struct Dispatcher<'a> {
    strings: StringStore<'a>,
    kv: &'a dyn Kv,
}

impl<'a> Dispatcher<'a> {
    pub fn new(kv: &'a dyn Kv, clock: Clock) -> Self {
        Self {
            strings: StringStore::new(kv, NS, clock),
            kv,
        }
    }

    pub fn dispatch(&self, args: &[Vec<u8>]) -> Value {
        let Some(name) = args.first() else {
            return err("ERR empty command");
        };
        match name.to_ascii_uppercase().as_slice() {
            b"PING" => match args.len() {
                1 => Value::Simple("PONG".into()),
                2 => Value::Bulk(Some(args[1].clone())),
                _ => arity_err("ping"),
            },
            b"ECHO" => exact(args, 2, "echo", |a| Value::Bulk(Some(a[1].clone()))),
            b"SET" => self.cmd_set(args),
            b"SETNX" => exact(args, 3, "setnx", |a| {
                let opts = SetOptions {
                    nx: true,
                    ..Default::default()
                };
                match self.strings.set(slot_for_key(&a[1]), &a[1], &a[2], opts) {
                    SetOutcome::Done => Value::Integer(1),
                    SetOutcome::Unchanged => Value::Integer(0),
                }
            }),
            b"SETEX" => exact(args, 4, "setex", |a| match parse_i64(&a[2]) {
                Ok(secs) if secs > 0 => {
                    let at = self.now_ms().saturating_add(secs as u64 * 1000);
                    let opts = SetOptions {
                        expiry: SetExpiry::AtMs(at),
                        ..Default::default()
                    };
                    self.strings.set(slot_for_key(&a[1]), &a[1], &a[3], opts);
                    Value::Simple("OK".into())
                }
                Ok(_) => err("ERR invalid expire time in 'setex' command"),
                Err(_) => err("ERR value is not an integer or out of range"),
            }),
            b"GET" => exact(args, 2, "get", |a| {
                Value::Bulk(self.strings.get(slot_for_key(&a[1]), &a[1]))
            }),
            b"DEL" => multi_key(args, "del", |k| self.strings.del(slot_for_key(k), k)),
            b"EXISTS" => multi_key(args, "exists", |k| self.strings.exists(slot_for_key(k), k)),
            b"TYPE" => exact(args, 2, "type", |a| {
                match self.strings.value_type(slot_for_key(&a[1]), &a[1]) {
                    Some(t) => Value::Simple(t.name().into()),
                    None => Value::Simple("none".into()),
                }
            }),
            b"EXPIRE" => self.cmd_expire(args, "expire", 1000),
            b"PEXPIRE" => self.cmd_expire(args, "pexpire", 1),
            b"TTL" => self.cmd_ttl(args, "ttl", 1000),
            b"PTTL" => self.cmd_ttl(args, "pttl", 1),
            b"PERSIST" => exact(args, 2, "persist", |a| {
                Value::Integer(self.strings.persist(slot_for_key(&a[1]), &a[1]) as i64)
            }),
            b"INCR" => self.cmd_incr_by(args, "incr", 1),
            b"DECR" => self.cmd_incr_by(args, "decr", -1),
            b"INCRBY" => self.cmd_incr_delta(args, "incrby", 1),
            b"DECRBY" => self.cmd_incr_delta(args, "decrby", -1),
            b"APPEND" => exact(args, 3, "append", |a| {
                Value::Integer(self.strings.append(slot_for_key(&a[1]), &a[1], &a[2]) as i64)
            }),
            b"STRLEN" => exact(args, 2, "strlen", |a| {
                Value::Integer(self.strings.strlen(slot_for_key(&a[1]), &a[1]) as i64)
            }),
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

    fn now_ms(&self) -> u64 {
        flint_storage::strings::system_clock()
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
                        self.now_ms().saturating_add(ms)
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
            SetOutcome::Done => Value::Simple("OK".into()),
            SetOutcome::Unchanged => Value::Bulk(None),
        }
    }

    fn cmd_expire(&self, args: &[Vec<u8>], name: &str, unit_ms: u64) -> Value {
        exact(args, 3, name, |a| match parse_i64(&a[2]) {
            Ok(n) => {
                let delta = n.saturating_mul(unit_ms as i64);
                let at = if delta <= 0 {
                    1 // already in the past → delete-on-touch semantics
                } else {
                    self.now_ms().saturating_add(delta as u64)
                };
                Value::Integer(self.strings.expire_at(slot_for_key(&a[1]), &a[1], at) as i64)
            }
            Err(_) => err("ERR value is not an integer or out of range"),
        })
    }

    fn cmd_ttl(&self, args: &[Vec<u8>], name: &str, unit_ms: u64) -> Value {
        exact(args, 2, name, |a| {
            match self.strings.ttl(slot_for_key(&a[1]), &a[1]) {
                Ttl::Missing => Value::Integer(-2),
                Ttl::NoExpiry => Value::Integer(-1),
                // Seconds TTL rounds up like Redis ((ms + 999) / 1000 ≈ +500 rounding).
                Ttl::Ms(ms) => Value::Integer((ms.div_ceil(unit_ms)) as i64),
            }
        })
    }

    fn cmd_incr_by(&self, args: &[Vec<u8>], name: &str, sign: i64) -> Value {
        exact(args, 2, name, |a| {
            self.incr_reply(self.strings.incr_by(slot_for_key(&a[1]), &a[1], sign))
        })
    }

    fn cmd_incr_delta(&self, args: &[Vec<u8>], name: &str, sign: i64) -> Value {
        exact(args, 3, name, |a| match parse_i64(&a[2]) {
            Ok(delta) => self.incr_reply(self.strings.incr_by(
                slot_for_key(&a[1]),
                &a[1],
                delta.saturating_mul(sign),
            )),
            Err(_) => err("ERR value is not an integer or out of range"),
        })
    }

    fn incr_reply(&self, r: Result<i64, StoreError>) -> Value {
        match r {
            Ok(n) => Value::Integer(n),
            Err(StoreError::NotInteger) | Err(StoreError::Overflow) => {
                err("ERR value is not an integer or out of range")
            }
            Err(StoreError::WrongType) => {
                err("WRONGTYPE Operation against a key holding the wrong kind of value")
            }
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
    fn set_get_roundtrip() {
        let s = MemKv::new();
        assert_eq!(call(&s, &[b"SET", b"k", b"v"]), Value::Simple("OK".into()));
        assert_eq!(call(&s, &[b"GET", b"k"]), Value::Bulk(Some(b"v".to_vec())));
        assert_eq!(call(&s, &[b"GET", b"nope"]), Value::Bulk(None));
    }

    #[test]
    fn set_with_expiry_options() {
        let s = MemKv::new();
        assert_eq!(
            call(&s, &[b"SET", b"k", b"v", b"EX", b"100"]),
            Value::Simple("OK".into())
        );
        let Value::Integer(ttl) = call(&s, &[b"TTL", b"k"]) else {
            panic!("ttl should be integer");
        };
        assert!((95..=100).contains(&ttl), "ttl was {ttl}");
        // Plain SET clears the TTL.
        call(&s, &[b"SET", b"k", b"v2"]);
        assert_eq!(call(&s, &[b"TTL", b"k"]), Value::Integer(-1));
        assert!(matches!(
            call(&s, &[b"SET", b"k", b"v", b"EX", b"0"]),
            Value::Error(_)
        ));
        assert!(matches!(
            call(&s, &[b"SET", b"k", b"v", b"EX", b"abc"]),
            Value::Error(_)
        ));
    }

    #[test]
    fn incr_family_and_errors() {
        let s = MemKv::new();
        assert_eq!(call(&s, &[b"INCR", b"c"]), Value::Integer(1));
        assert_eq!(call(&s, &[b"INCRBY", b"c", b"11"]), Value::Integer(12));
        assert_eq!(call(&s, &[b"DECRBY", b"c", b"2"]), Value::Integer(10));
        assert_eq!(call(&s, &[b"DECR", b"c"]), Value::Integer(9));
        call(&s, &[b"SET", b"s", b"abc"]);
        assert!(matches!(call(&s, &[b"INCR", b"s"]), Value::Error(_)));
    }

    #[test]
    fn ttl_states() {
        let s = MemKv::new();
        assert_eq!(call(&s, &[b"TTL", b"nope"]), Value::Integer(-2));
        call(&s, &[b"SET", b"k", b"v"]);
        assert_eq!(call(&s, &[b"TTL", b"k"]), Value::Integer(-1));
        assert_eq!(call(&s, &[b"EXPIRE", b"k", b"100"]), Value::Integer(1));
        assert_eq!(call(&s, &[b"PERSIST", b"k"]), Value::Integer(1));
        assert_eq!(call(&s, &[b"PERSIST", b"k"]), Value::Integer(0));
        assert_eq!(call(&s, &[b"EXPIRE", b"nope", b"100"]), Value::Integer(0));
    }

    #[test]
    fn type_command() {
        let s = MemKv::new();
        call(&s, &[b"SET", b"k", b"v"]);
        assert_eq!(call(&s, &[b"TYPE", b"k"]), Value::Simple("string".into()));
        assert_eq!(call(&s, &[b"TYPE", b"no"]), Value::Simple("none".into()));
    }
}
