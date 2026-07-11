//! Command dispatch: `Vec<arg-bytes>` in, RESP `Value` out.
//!
//! v0 surface: PING, ECHO, SET (NX|XX), GET, DEL, EXISTS, FLUSHALL — enough
//! to stand up the conformance loop. Reply shapes follow Redis exactly; the
//! conformance oracle is the referee.

use flint_resp::Value;
use flint_storage::Kv;

pub fn dispatch(store: &dyn Kv, args: &[Vec<u8>]) -> Value {
    let Some(name) = args.first() else {
        return err("ERR empty command");
    };
    match name.to_ascii_uppercase().as_slice() {
        b"PING" => match args.len() {
            1 => Value::Simple("PONG".into()),
            2 => Value::Bulk(Some(args[1].clone())),
            _ => arity_err("ping"),
        },
        b"ECHO" => match args.len() {
            2 => Value::Bulk(Some(args[1].clone())),
            _ => arity_err("echo"),
        },
        b"SET" => cmd_set(store, args),
        b"GET" => match args.len() {
            2 => Value::Bulk(store.get(&args[1])),
            _ => arity_err("get"),
        },
        b"DEL" => match args.len() {
            0 | 1 => arity_err("del"),
            _ => Value::Integer(args[1..].iter().filter(|k| store.delete(k)).count() as i64),
        },
        b"EXISTS" => match args.len() {
            0 | 1 => arity_err("exists"),
            _ => Value::Integer(args[1..].iter().filter(|k| store.get(k).is_some()).count() as i64),
        },
        b"FLUSHALL" => {
            store.clear();
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

fn cmd_set(store: &dyn Kv, args: &[Vec<u8>]) -> Value {
    if args.len() < 3 {
        return arity_err("set");
    }
    let (key, value) = (&args[1], &args[2]);
    let mut nx = false;
    let mut xx = false;
    for opt in &args[3..] {
        match opt.to_ascii_uppercase().as_slice() {
            b"NX" => nx = true,
            b"XX" => xx = true,
            _ => return err("ERR syntax error"),
        }
    }
    if nx && xx {
        return err("ERR syntax error");
    }
    let exists = store.get(key).is_some();
    if (nx && exists) || (xx && !exists) {
        return Value::Bulk(None);
    }
    store.put(key, value);
    Value::Simple("OK".into())
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

    fn call(store: &MemKv, parts: &[&[u8]]) -> Value {
        dispatch(store, &parts.iter().map(|p| p.to_vec()).collect::<Vec<_>>())
    }

    #[test]
    fn set_get_roundtrip() {
        let s = MemKv::new();
        assert_eq!(call(&s, &[b"SET", b"k", b"v"]), Value::Simple("OK".into()));
        assert_eq!(call(&s, &[b"GET", b"k"]), Value::Bulk(Some(b"v".to_vec())));
        assert_eq!(call(&s, &[b"GET", b"nope"]), Value::Bulk(None));
    }

    #[test]
    fn set_nx_xx_semantics() {
        let s = MemKv::new();
        assert_eq!(
            call(&s, &[b"SET", b"k", b"a", b"NX"]),
            Value::Simple("OK".into())
        );
        assert_eq!(call(&s, &[b"SET", b"k", b"b", b"nx"]), Value::Bulk(None));
        assert_eq!(call(&s, &[b"GET", b"k"]), Value::Bulk(Some(b"a".to_vec())));
        assert_eq!(
            call(&s, &[b"SET", b"k", b"c", b"XX"]),
            Value::Simple("OK".into())
        );
        assert_eq!(call(&s, &[b"SET", b"new", b"d", b"XX"]), Value::Bulk(None));
        assert!(matches!(
            call(&s, &[b"SET", b"k", b"v", b"NX", b"XX"]),
            Value::Error(_)
        ));
    }

    #[test]
    fn del_and_exists_count_semantics() {
        let s = MemKv::new();
        call(&s, &[b"SET", b"a", b"1"]);
        call(&s, &[b"SET", b"b", b"2"]);
        // EXISTS counts duplicates; DEL counts each key once.
        assert_eq!(
            call(&s, &[b"EXISTS", b"a", b"a", b"nope"]),
            Value::Integer(2)
        );
        assert_eq!(
            call(&s, &[b"DEL", b"a", b"a", b"b", b"nope"]),
            Value::Integer(2)
        );
        assert_eq!(call(&s, &[b"EXISTS", b"a", b"b"]), Value::Integer(0));
    }

    #[test]
    fn errors() {
        let s = MemKv::new();
        assert!(matches!(call(&s, &[b"GET"]), Value::Error(_)));
        assert!(matches!(call(&s, &[b"SET", b"k"]), Value::Error(_)));
        assert!(matches!(call(&s, &[b"DEL"]), Value::Error(_)));
        assert!(matches!(call(&s, &[b"NOSUCH"]), Value::Error(_)));
    }
}
