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

/// True when `name` mutates the keyspace.
pub fn is_write_command(name: &[u8]) -> bool {
    matches!(
        name.to_ascii_uppercase().as_slice(),
        b"SET"
            | b"SETNX"
            | b"SETEX"
            | b"MSET"
            | b"DEL"
            | b"EXPIRE"
            | b"PEXPIRE"
            | b"EXPIREAT"
            | b"PEXPIREAT"
            | b"UNLINK"
            | b"GETDEL"
            | b"GETSET"
            | b"HSETNX"
            | b"PERSIST"
            | b"INCR"
            | b"DECR"
            | b"INCRBY"
            | b"DECRBY"
            | b"APPEND"
            | b"SETRANGE"
            | b"FLUSHALL"
            | b"HSET"
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
    )
}

/// True for commands a replica may serve / a replica-read router may move
/// off the master. NOT simply `!is_write_command`: unknown or admin
/// commands are neither reads nor writes and must stay on the master, so
/// the read set is explicit too.
pub fn is_read_command(name: &[u8]) -> bool {
    matches!(
        name.to_ascii_uppercase().as_slice(),
        b"GET"
            | b"MGET"
            | b"EXISTS"
            | b"TTL"
            | b"PTTL"
            | b"EXPIRETIME"
            | b"PEXPIRETIME"
            | b"STRLEN"
            | b"GETRANGE"
            | b"HSTRLEN"
            | b"HGET"
            | b"HGETALL"
            | b"HLEN"
            | b"HEXISTS"
            | b"SISMEMBER"
            | b"SMISMEMBER"
            | b"SRANDMEMBER"
            | b"SCARD"
            | b"SMEMBERS"
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
            | b"ZRANK"
            | b"ZREVRANK"
            | b"ZCOUNT"
            | b"ZMSCORE"
            | b"DBSIZE"
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
}
