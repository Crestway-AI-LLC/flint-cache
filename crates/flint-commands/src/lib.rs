// SPDX-License-Identifier: Elastic-2.0
//! Read/write command classification (ADR-0005 D1) — ONE definition shared
//! by every plane. The server gates `-READONLY` and the slot freeze on it;
//! the proxy splits traffic accounting on it and will route replica reads
//! (D7) and the async-write-queue bypass (D4) by it. Misclassification is
//! a correctness bug on the server (a write slipping past `-READONLY`) and
//! a routing bug at the proxy (a write sent to a replica), so the table
//! lives here, once.
//!
//! Unknown commands classify as WRITES: the conservative direction for
//! every consumer (a replica rejects them, the slot gate freezes them, a
//! future replica-read router keeps them on the master).

pub mod scripts;

/// The Redis version Flint reports, as Valkey 9.1 reports it: the last Redis
/// release Valkey forked from, frozen there so that clients gating features
/// on it keep working (ADR-0052 D1). The proxy's `INFO` and a script's
/// `redis.REDIS_VERSION` both read it, so the two cannot disagree.
pub const REDIS_COMPAT_VERSION: &str = "7.2.4";

/// True when `name` can only SHRINK the keyspace.
///
/// The one class of write that must stay allowed when the system is refusing
/// writes for lack of room — over a tenant's storage quota, or on a node out
/// of disk headroom. Both of those conditions are cured by deleting data, so
/// blocking deletes would make the state self-perpetuating: the operator is
/// told to free space by the same server that refuses to let them.
///
/// Shared for the same reason the read/write split is: the proxy applies it
/// for the per-tenant quota verdict and the server applies it for the
/// node-level disk verdict, and a command that frees space in one plane but
/// not the other is a trap that only shows up during an incident.
pub fn reduces_space(name: &[u8]) -> bool {
    matches!(
        name.to_ascii_uppercase().as_slice(),
        b"DEL"
            | b"UNLINK"
            | b"FLUSHALL"
            | b"FLUSHDB"
            | b"EXPIRE"
            | b"PEXPIRE"
            | b"EXPIREAT"
            | b"PEXPIREAT"
            // A stream's entries go, its key stays (ADR-0052 D6).
            | b"XDEL"
            | b"XTRIM"
    )
}

/// Writes that never make a value larger, which a store out of room may
/// still serve: `PFCOUNT`, which at most rewrites the eight bytes an HLL
/// keeps its last count in. Redis serves it at `maxmemory` as a read.
/// The quota and disk gates admit these beside [`reduces_space`].
pub fn never_grows(name: &[u8]) -> bool {
    name.eq_ignore_ascii_case(b"PFCOUNT")
}

/// The blocking pops (ADR-0052 D4). A seat answers each without waiting, as
/// Redis does inside `MULTI` or a script; the proxy makes a client wait, by
/// running that form until one answers or the timeout passes.
pub fn is_blocking_command(name: &[u8]) -> bool {
    matches!(
        name.to_ascii_uppercase().as_slice(),
        b"BLPOP" | b"BRPOP" | b"BZPOPMIN" | b"BZPOPMAX" | b"BLMOVE" | b"BRPOPLPUSH"
    )
}

/// `JSON.DEBUG`'s key (ADR-0055). It follows the subcommand, in
/// `JSON.DEBUG MEMORY key [path]`, so the server's `command_key` and the
/// proxy's `route_key` cannot take `args[1]`; `JSON.DEBUG HELP` has none.
/// `None` when `args` is not a JSON.DEBUG.
pub fn json_debug_key(args: &[Vec<u8>]) -> Option<Option<&[u8]>> {
    if !args.first()?.eq_ignore_ascii_case(b"JSON.DEBUG") {
        return None;
    }
    Some(match args.get(1) {
        Some(sub) if sub.eq_ignore_ascii_case(b"MEMORY") => args.get(2).map(|k| k.as_slice()),
        _ => None,
    })
}

/// XREAD's keys (ADR-0052 D6): the first half of what follows `STREAMS`,
/// the second half being their IDs. `None` when `args` is not an XREAD, or
/// has no `STREAMS` or an odd count after it, which the seat refuses. The
/// options before `STREAMS` each take one value, so a `STREAMS` among them
/// is a value and not the keyword, as Redis reads it.
pub fn xread_keys(args: &[Vec<u8>]) -> Option<&[Vec<u8>]> {
    if !args.first()?.eq_ignore_ascii_case(b"XREAD") {
        return None;
    }
    let mut i = 1;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"STREAMS") {
            let rest = &args[i + 1..];
            if rest.is_empty() || rest.len() % 2 == 1 {
                return None;
            }
            return Some(&rest[..rest.len() / 2]);
        }
        i += 2;
    }
    None
}

/// Whether an XREAD asks to wait (`BLOCK`): only then does the proxy make
/// its client wait, as for the blocking pops.
pub fn xread_blocks(args: &[Vec<u8>]) -> bool {
    let Some(keys) = xread_keys(args) else {
        return false;
    };
    let options = args.len() - 2 * keys.len() - 1;
    args[1..options]
        .iter()
        .step_by(2)
        .any(|o| o.eq_ignore_ascii_case(b"BLOCK"))
}

/// Whether a `FLUSHALL` or `FLUSHDB` has arguments Redis accepts: none, or
/// one `ASYNC` or `SYNC` (BUG-0213). The seat and the proxy both refuse the
/// rest before anything is flushed; the proxy fans a flush out to every
/// pair, so it must not send one that each seat would refuse.
pub fn flush_args_ok(args: &[Vec<u8>]) -> bool {
    match args {
        [_] => true,
        [_, mode] => mode.eq_ignore_ascii_case(b"ASYNC") || mode.eq_ignore_ascii_case(b"SYNC"),
        _ => false,
    }
}

/// True when `name` mutates the keyspace.
pub fn is_write_command(name: &[u8]) -> bool {
    matches!(
        name.to_ascii_uppercase().as_slice(),
        b"SET"
            | b"SETNX"
            | b"SETEX"
            | b"MSET"
            | b"COPY"
            // The STORE variants write their DESTINATION, which is args[1] —
            // so the proxy's default invalidation already drops the right key.
            | b"ZUNIONSTORE"
            | b"ZINTERSTORE"
            | b"SINTERSTORE"
            | b"SUNIONSTORE"
            | b"SDIFFSTORE"
            | b"ZREMRANGEBYLEX"
            | b"RENAME"
            | b"RENAMENX"
            | b"LMOVE"
            | b"RPOPLPUSH"
            | b"BLMOVE"
            | b"BRPOPLPUSH"
            | b"BLPOP"
            | b"BRPOP"
            | b"BZPOPMIN"
            | b"BZPOPMAX"
            | b"HINCRBYFLOAT"
            | b"DEL"
            | b"EXPIRE"
            | b"PEXPIRE"
            | b"EXPIREAT"
            | b"PEXPIREAT"
            | b"UNLINK"
            | b"GETDEL"
            // GETEX reads a value but may rewrite or clear the key's TTL, so
            // it is a write: it must stay on the master, and it must drop the
            // proxy's cached entry — a GETEX that shortens a TTL otherwise
            // leaves a cached value outliving the expiry it just set.
            | b"GETEX"
            | b"GETSET"
            | b"HSETNX"
            | b"PERSIST"
            | b"INCR"
            | b"DECR"
            | b"INCRBY"
            | b"DECRBY"
            | b"INCRBYFLOAT"
            | b"APPEND"
            | b"SETRANGE"
            | b"BITFIELD"
            | b"SETBIT"
            // HyperLogLog. PFCOUNT is here because it may write: it keeps
            // the count it computed in the HLL, as Redis does, which needs
            // the key's write lock. See `never_grows`.
            | b"PFADD"
            | b"PFMERGE"
            | b"PFCOUNT"
            // Writes its destination, args[2]; args[1] is the operator.
            | b"BITOP"
            | b"PSETEX"
            | b"LPUSHX"
            | b"RPUSHX"
            // Writes both of its keys (BUG-0188's rule applies).
            | b"SMOVE"
            | b"FLUSHALL"
            | b"FLUSHDB"
            | b"HSET"
            | b"HMSET"
            | b"EVAL"
            | b"EVALSHA"
            | b"HDEL"
            | b"HINCRBY"
            | b"SADD"
            | b"SREM"
            | b"SPOP"
            | b"LPUSH"
            | b"RPUSH"
            | b"LPOP"
            | b"RPOP"
            | b"LSET"
            | b"LTRIM"
            | b"LREM"
            | b"LINSERT"
            | b"ZADD"
            | b"ZREM"
            | b"ZINCRBY"
            | b"ZPOPMIN"
            | b"ZPOPMAX"
            | b"ZREMRANGEBYSCORE"
            | b"ZREMRANGEBYRANK"
            // JSON documents: every mutation is a read-modify-write of the
            // whole document row, so they classify like any other write.
            | b"JSON.SET"
            | b"JSON.DEL"
            | b"JSON.FORGET"
            | b"JSON.NUMINCRBY"
            | b"JSON.ARRAPPEND"
            // ADR-0055. JSON.ARRPOP, JSON.ARRTRIM and JSON.CLEAR shrink a
            // document but are not `reduces_space`, like a JSON.DEL of a
            // path: each rewrites the whole document row, which takes room
            // before compaction gives any back.
            | b"JSON.MSET"
            | b"JSON.MERGE"
            | b"JSON.NUMMULTBY"
            | b"JSON.STRAPPEND"
            | b"JSON.ARRINSERT"
            | b"JSON.ARRPOP"
            | b"JSON.ARRTRIM"
            | b"JSON.TOGGLE"
            | b"JSON.CLEAR"
            // Bloom filters (ADR-0016). BF.RESERVE creates the key and
            // BF.ADD sets bits, so both mutate. None of them is
            // `reduces_space`: a Bloom filter never shrinks — the only way
            // to free its bytes is DEL, which is already in that set.
            | b"BF.ADD"
            | b"BF.MADD"
            | b"BF.RESERVE"
            | b"BF.INSERT"
            // Streams (ADR-0052 D6).
            | b"XADD"
            | b"XDEL"
            | b"XTRIM"
    )
}

/// The keys an `EVAL` or `EVALSHA` names (`KEYS[...]`), or `None` when the
/// command is neither or its key count is malformed (ADR-0050, ADR-0051).
/// A script's keys are what it routes by, locks, and may touch; `args[1]`
/// is the script.
pub fn eval_keys(args: &[Vec<u8>]) -> Option<&[Vec<u8>]> {
    let name = args.first()?;
    if !name.eq_ignore_ascii_case(b"EVAL") && !name.eq_ignore_ascii_case(b"EVALSHA") {
        return None;
    }
    let n: usize = std::str::from_utf8(args.get(2)?).ok()?.parse().ok()?;
    args.get(3..3usize.checked_add(n)?)
}

/// True for commands a replica may serve / a replica-read router may move
/// off the master. NOT simply `!is_write_command`: unknown or admin
/// commands are neither reads nor writes and must stay on the master, so
/// the read set is explicit too.
pub fn is_read_command(name: &[u8]) -> bool {
    matches!(
        name.to_ascii_uppercase().as_slice(),
        b"GET"
            | b"TIME"
            | b"FLINTKEYSIZE"
            | b"FLINTKEYSTAMP"
            | b"MGET"
            | b"EXISTS"
            | b"TTL"
            | b"PTTL"
            | b"EXPIRETIME"
            | b"PEXPIRETIME"
            | b"STRLEN"
            | b"GETRANGE"
            | b"BITFIELD_RO"
            | b"GETBIT"
            | b"BITCOUNT"
            | b"BITPOS"
            | b"TOUCH"
            | b"HRANDFIELD"
            | b"ZRANDMEMBER"
            | b"HSTRLEN"
            | b"HGET"
            | b"HGETALL"
            | b"HLEN"
            | b"HEXISTS"
            | b"SISMEMBER"
            | b"SMISMEMBER"
            | b"SRANDMEMBER"
            | b"HSCAN"
            | b"SSCAN"
            | b"ZSCAN"
            | b"SCAN"
            | b"KEYS"
            | b"SCARD"
            | b"SMEMBERS"
            | b"SINTER"
            | b"SUNION"
            | b"SDIFF"
            | b"LLEN"
            | b"LRANGE"
            | b"LINDEX"
            | b"LPOS"
            | b"ZSCORE"
            | b"ZCARD"
            | b"ZRANGE"
            | b"ZREVRANGE"
            | b"ZRANGEBYSCORE"
            | b"ZREVRANGEBYSCORE"
            | b"ZRANGEBYLEX"
            | b"ZREVRANGEBYLEX"
            | b"ZLEXCOUNT"
            | b"ZRANK"
            | b"ZREVRANK"
            | b"ZCOUNT"
            | b"ZMSCORE"
            | b"DBSIZE"
            | b"JSON.GET"
            | b"JSON.TYPE"
            | b"JSON.ARRLEN"
            // ADR-0055.
            | b"JSON.MGET"
            | b"JSON.STRLEN"
            | b"JSON.ARRINDEX"
            | b"JSON.OBJKEYS"
            | b"JSON.OBJLEN"
            | b"JSON.RESP"
            | b"JSON.DEBUG"
            | b"BF.EXISTS"
            | b"BF.MEXISTS"
            | b"BF.CARD"
            | b"BF.INFO"
            | b"XLEN"
            | b"XRANGE"
            | b"XREVRANGE"
            | b"XREAD"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_are_writes_reads_are_reads() {
        assert!(is_write_command(b"set"));
        assert!(is_write_command(b"INCR"));
        assert!(!is_write_command(b"GET"));
        assert!(is_read_command(b"get"));
        assert!(is_read_command(b"ZRANGE"));
        assert!(!is_read_command(b"SET"));
    }

    #[test]
    fn json_debug_routes_by_the_key_after_its_subcommand() {
        let a = |v: &[&str]| v.iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>();
        assert_eq!(
            json_debug_key(&a(&["JSON.DEBUG", "MEMORY", "k", "$"])),
            Some(Some(&b"k"[..]))
        );
        assert_eq!(
            json_debug_key(&a(&["json.debug", "memory", "k"])),
            Some(Some(&b"k"[..]))
        );
        assert_eq!(json_debug_key(&a(&["JSON.DEBUG", "HELP"])), Some(None));
        assert_eq!(json_debug_key(&a(&["JSON.DEBUG"])), Some(None));
        assert_eq!(json_debug_key(&a(&["JSON.GET", "k"])), None);
    }

    #[test]
    fn every_json_command_is_classified() {
        // ADR-0055: an unclassified command skips the -READONLY gate and the
        // write lock on a seat, and on a replica its writes reach a store
        // that drops them, answering OK.
        for w in [
            "JSON.SET",
            "JSON.DEL",
            "JSON.FORGET",
            "JSON.NUMINCRBY",
            "JSON.NUMMULTBY",
            "JSON.ARRAPPEND",
            "JSON.MSET",
            "JSON.MERGE",
            "JSON.STRAPPEND",
            "JSON.ARRINSERT",
            "JSON.ARRPOP",
            "JSON.ARRTRIM",
            "JSON.TOGGLE",
            "JSON.CLEAR",
        ] {
            assert!(
                is_write_command(w.as_bytes()) && !is_read_command(w.as_bytes()),
                "{w}"
            );
        }
        for r in [
            "JSON.GET",
            "JSON.MGET",
            "JSON.TYPE",
            "JSON.ARRLEN",
            "JSON.STRLEN",
            "JSON.ARRINDEX",
            "JSON.OBJKEYS",
            "JSON.OBJLEN",
            "JSON.RESP",
            "JSON.DEBUG",
        ] {
            assert!(
                is_read_command(r.as_bytes()) && !is_write_command(r.as_bytes()),
                "{r}"
            );
        }
    }

    #[test]
    fn unknown_and_admin_are_neither() {
        // Conservative default: not a read (stays on master), and each
        // consumer decides what non-write means for it.
        for name in [b"FLINTINFO".as_slice(), b"AUTH", b"NOSUCH"] {
            assert!(!is_read_command(name), "{name:?}");
        }
        assert!(!is_write_command(b"FLINTINFO"));
    }

    #[test]
    fn no_command_is_both() {
        // The sets must be disjoint by construction; spot-check overlap.
        for name in [b"GET".as_slice(), b"SET", b"DEL", b"ZRANGE", b"DBSIZE"] {
            assert!(
                !(is_read_command(name) && is_write_command(name)),
                "{name:?} classified as both"
            );
        }
    }

    #[test]
    fn a_flush_takes_async_sync_or_nothing() {
        let args = |a: &[&str]| a.iter().map(|s| s.as_bytes().to_vec()).collect::<Vec<_>>();
        assert!(flush_args_ok(&args(&["FLUSHALL"])));
        assert!(flush_args_ok(&args(&["FLUSHDB", "async"])));
        assert!(flush_args_ok(&args(&["FLUSHALL", "SYNC"])));
        assert!(!flush_args_ok(&args(&["FLUSHALL", "FOO"])));
        assert!(!flush_args_ok(&args(&["FLUSHALL", "ASYNC", "SYNC"])));
    }
}
