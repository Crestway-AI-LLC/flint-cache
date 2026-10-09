// SPDX-License-Identifier: Elastic-2.0
//! flint-conformance: the compatibility oracle.
//!
//! Runs a table-driven corpus of Redis-semantics cases against any RESP2
//! endpoint and reports pass rates per command family. The same corpus runs
//! against a reference server (valkey/redis) to validate the oracle itself,
//! and against flint-server to measure conformance. Nonzero exit on any
//! failure, so CI can gate on it.
//!
//! Usage: `flint-conformance --target 127.0.0.1:6380`
//!
//! # Running it against a REAL deployment
//!
//! For most of this project's life the corpus could only reach a bare
//! endpoint: plaintext, no credential. Every production edge has both TLS and
//! auth, so the strongest compatibility evidence in the repository was
//! structurally unable to touch the thing customers connect to, and
//! `flintctl verify --probe`'s five data-plane assertions stood in for
//! ninety-nine cases.
//!
//! That gap is not academic. The `HELLO` reply carried the crate version
//! rather than the build to every client for the life of the project, and it
//! was found by hand-writing RESP to the playground edge — precisely the job
//! this binary exists to do, and could not.
//!
//! Run it as `flint-conformance --target try.example.com:7379 --tls --ca
//! /etc/pki/tls/certs/ca-bundle.crt --auth <tenant>:<token> --yes-flushall`.
//! Set apart, that block was an INDENTED doc block, which rustdoc compiles as
//! Rust and fails on -- the same trap BUG-0162 hit.
//!
//! `--yes-flushall` is mandatory with `--auth` and is not a formality: every
//! case starts by FLUSHALL-ing to get a clean keyspace, which through a proxy
//! means **erasing that tenant's namespace**. Point it at a throwaway tenant,
//! never at one holding data. Requiring the flag makes the destruction a
//! thing someone typed rather than a thing they discovered.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::process::ExitCode;
use std::sync::Arc;

use flint_resp::{Decoded, Proto, Value, decode, encode};

/// What a step's reply must look like.
#[derive(Debug, Clone)]
enum Expect {
    Ok,   // +OK
    Pong, // +PONG
    Nil,  // $-1
    /// `*-1` — a null ARRAY, which is a different reply from a null bulk
    /// and the one an aborted EXEC returns. Kept separate so a case cannot
    /// assert "nil" and accidentally accept either.
    NilArray,
    Int(i64),             // :n
    IntRange(i64, i64),   // :n where lo <= n <= hi (TTL imprecision)
    Simple(&'static str), // +text
    Str(&'static [u8]),   // $len\r\n<bytes>
    Bytes(Vec<u8>),       // like Str, for computed payloads
    AnyError,             // -...
    /// An error with exactly this text: a script's errors carry Valkey's
    /// wording and the line they were raised on (ADR-0051).
    Err(&'static str),
    /// Any bulk string: a value that depends on the clock (`TIME`, a rate
    /// limiter's reset time) where only its kind is agreed.
    AnyBulk,
    /// Exact array, in order (HMGET etc. where order is defined).
    Arr(Vec<Expect>),
    /// Flat field/value reply compared as an unordered map (HGETALL —
    /// Redis hash iteration order is unspecified).
    UnorderedPairs(Vec<(&'static [u8], &'static [u8])>),
    /// Array of bulk strings compared as an unordered set (SMEMBERS).
    UnorderedStrs(Vec<&'static [u8]>),
    /// Any array reply, contents unexamined. For a command whose SHAPE is
    /// agreed and whose contents legitimately differ between servers.
    AnyArray,
    /// Bulk reply that CONTAINS these bytes. For replies whose contract is a
    /// shape rather than a value -- FLINTINFO is a `field:value` blob whose
    /// numbers differ per host and per second, but whose FIELDS are the
    /// promise. Asserting a whole reply there would be a test of the clock.
    StrContains(&'static [u8]),
    /// A bulk reply holding JSON equal to this, object members in any
    /// order. JSON.GET with several paths answers one object keyed by path,
    /// and RedisJSON orders its members by its hash map (ADR-0055).
    Json(&'static [u8]),
}

struct Case {
    family: &'static str,
    name: &'static str,
    /// (command, expected reply, delay after step in ms)
    steps: Vec<(Vec<Vec<u8>>, Expect, u64)>,
}

/// Step with no delay.
fn s(parts: &[&[u8]], expect: Expect) -> (Vec<Vec<u8>>, Expect, u64) {
    (cmd(parts), expect, 0)
}

/// Step followed by a real-time delay (used only where semantics require
/// actual expiration; kept rare to avoid slow, flaky runs).
fn sd(parts: &[&[u8]], expect: Expect, delay_ms: u64) -> (Vec<Vec<u8>>, Expect, u64) {
    (cmd(parts), expect, delay_ms)
}

/// A stream entry as XRANGE and XREAD answer it: `[id, [field, value, ...]]`.
fn xentry(id: &'static [u8], fields: &[&'static [u8]]) -> Expect {
    Expect::Arr(vec![
        Expect::Str(id),
        Expect::Arr(fields.iter().map(|f| Expect::Str(f)).collect()),
    ])
}

fn cmd(parts: &[&[u8]]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.to_vec()).collect()
}

/// Families the reference implementation does not have, so it cannot serve
/// as their oracle. `--reference` skips them; against Flint targets they
/// run normally.
///
/// This distinction is load-bearing for how much the corpus PROVES. For
/// every other family, a green run against Valkey means the cases encode
/// real Redis behavior, and a green run against Flint means Flint matches
/// it — two independent facts. For a flint-only family the `--reference`
/// run proves neither, so it is skipped rather than reported as a failure
/// that would say nothing about either side.
///
/// Bloom is the same shape: `BF.*` comes from the RedisBloom module, so
/// `--reference` cannot serve it, and `tools/redisbloom_compare.sh` is the
/// script that turns "matches the contract we wrote" into "matches
/// RedisBloom". Its divergences are ADR-0016 D7.
///
/// JSON does have an oracle, just not one `--reference` can reach: the
/// RedisJSON module, which has to be built from source and loaded into a
/// module-capable server. `tools/redisjson_compare.sh` runs this same
/// corpus against it and asserts that the ONLY cases which fail are the
/// three divergences we chose on purpose (see docs/command-support.md).
/// Run it whenever these cases change; a green run there is what lets us
/// say "matches RedisJSON" rather than "matches the contract we wrote".
/// The Lua scripts Flint recognises (ADR-0050), byte for byte as the
/// libraries send them. The oracle runs them as Lua; a
/// Flint target runs them natively, and the two must agree.
const DJANGO_INCR_CHECKED: &[u8] = "\n                    local exists = redis.call('EXISTS', KEYS[1])\n                    if (exists == 1) then\n                        return redis.call('INCRBY', KEYS[1], ARGV[1])\n                    else return false end\n                    ".as_bytes();
const DJANGO_INCR: &[u8] =
    "\n                    return redis.call('INCRBY', KEYS[1], ARGV[1])\n                    "
        .as_bytes();
const LOCK_RELEASE: &[u8] = "\n        local token = redis.call('get', KEYS[1])\n        if not token or token ~= ARGV[1] then\n            return 0\n        end\n        redis.call('del', KEYS[1])\n        return 1\n    ".as_bytes();
const LOCK_EXTEND: &[u8] = "\n        local token = redis.call('get', KEYS[1])\n        if not token or token ~= ARGV[1] then\n            return 0\n        end\n        local expiration = redis.call('pttl', KEYS[1])\n        if not expiration then\n            expiration = 0\n        end\n        if expiration < 0 then\n            return 0\n        end\n\n        local newttl = ARGV[2]\n        if ARGV[3] == \"0\" then\n            newttl = ARGV[2] + expiration\n        end\n        redis.call('pexpire', KEYS[1], newttl)\n        return 1\n    ".as_bytes();
const LOCK_REACQUIRE: &[u8] = "\n        local token = redis.call('get', KEYS[1])\n        if not token or token ~= ARGV[1] then\n            return 0\n        end\n        redis.call('pexpire', KEYS[1], ARGV[2])\n        return 1\n    ".as_bytes();
// ADR-0050's amendment: the lock scripts of node redlock 4.2.0, redsync
// v4.13.0 and Ruby redlock 2.1.0, as captured on the wire.
const REDLOCK_ACQUIRE: &[u8] = "\n\t-- Return 0 if an entry already exists.\n\tfor i, key in ipairs(KEYS) do\n\t\tif redis.call(\"exists\", key) == 1 then\n\t\t\treturn 0\n\t\tend\n\tend\n\n\t-- Create an entry for each provided key.\n\tfor i, key in ipairs(KEYS) do\n\t\tredis.call(\"set\", key, ARGV[1], \"PX\", ARGV[2])\n\tend\n\n\t-- Return the number of entries added.\n\treturn #KEYS\n".as_bytes();
const REDLOCK_EXTEND: &[u8] = "\n\t-- Return 0 if an entry exists with a *different* lock value.\n\tfor i, key in ipairs(KEYS) do\n\t\tif redis.call(\"get\", key) ~= ARGV[1] then\n\t\t\treturn 0\n\t\tend\n\tend\n\n\t-- Update the entry for each provided key.\n\tfor i, key in ipairs(KEYS) do\n\t\tredis.call(\"set\", key, ARGV[1], \"PX\", ARGV[2])\n\tend\n\n\t-- Return the number of entries updated.\n\treturn #KEYS\n".as_bytes();
const REDLOCK_RELEASE: &[u8] = "\n\tlocal count = 0\n\tfor i, key in ipairs(KEYS) do\n\t\t-- Only remove entries for *this* lock value.\n\t\tif redis.call(\"get\", key) == ARGV[1] then\n\t\t\tredis.pcall(\"del\", key)\n\t\t\tcount = count + 1\n\t\tend\n\tend\n\n\t-- Return the number of entries removed.\n\treturn count\n".as_bytes();
const REDSYNC_EXTEND: &[u8] = "\n\tif redis.call(\"GET\", KEYS[1]) == ARGV[1] then\n\t\treturn redis.call(\"PEXPIRE\", KEYS[1], ARGV[2])\n\telse\n\t\treturn 0\n\tend\n".as_bytes();
const REDSYNC_RELEASE: &[u8] = "\n\tlocal val = redis.call(\"GET\", KEYS[1])\n\tif val == ARGV[1] then\n\t\treturn redis.call(\"DEL\", KEYS[1])\n\telseif val == false then\n\t\treturn -1\n\telse\n\t\treturn 0\n\tend\n".as_bytes();
const REDLOCK_RB_LOCK: &[u8] = "      if (redis.call(\"exists\", KEYS[1]) == 0 and ARGV[3] == \"yes\") or redis.call(\"get\", KEYS[1]) == ARGV[1] then\n        return redis.call(\"set\", KEYS[1], ARGV[1], \"PX\", ARGV[2])\n      end\n".as_bytes();
const REDLOCK_RB_UNLOCK: &[u8] = "      if redis.call(\"get\",KEYS[1]) == ARGV[1] then\n        return redis.call(\"del\",KEYS[1])\n      else\n        return 0\n      end\n".as_bytes();
const REDLOCK_RB_INFO: &[u8] =
    "      return { redis.call(\"get\", KEYS[1]), redis.call(\"pttl\", KEYS[1]) }\n".as_bytes();
// And from the published packages: node redlock 5.0.0-beta.2's acquire, and
// redsync's release before v4.12.0 and its `WithSetNXOnExtend` extend.
const REDLOCK5_ACQUIRE: &[u8] = "\n  -- Return 0 if an entry already exists.\n  for i, key in ipairs(KEYS) do\n    if redis.call(\"exists\", key) == 1 then\n      return 0\n    end\n  end\n\n  -- Create an entry for each provided key.\n  for i, key in ipairs(KEYS) do\n    redis.call(\"set\", key, ARGV[1], \"PX\", ARGV[2])\n  end\n\n  -- Return the number of entries added.\n  return #KEYS\n".as_bytes();
const REDSYNC_RELEASE_BEFORE_4_12: &[u8] = "\n\tif redis.call(\"GET\", KEYS[1]) == ARGV[1] then\n\t\treturn redis.call(\"DEL\", KEYS[1])\n\telse\n\t\treturn 0\n\tend\n".as_bytes();
/// asynq 0.26's `dequeueCmd` (internal/rdb/rdb.go), verbatim: the task's
/// key is built from ARGV and the popped id, in the queue's hash tag but not
/// in KEYS (ADR-0052 D2).
const ASYNQ_DEQUEUE: &[u8] = "\nif redis.call(\"EXISTS\", KEYS[2]) == 0 then\n\tlocal id = redis.call(\"RPOPLPUSH\", KEYS[1], KEYS[3])\n\tif id then\n\t\tlocal key = ARGV[2] .. id\n\t\tredis.call(\"HSET\", key, \"state\", \"active\")\n\t\tredis.call(\"HDEL\", key, \"pending_since\")\n\t\tredis.call(\"ZADD\", KEYS[4], ARGV[1], id)\n\t\treturn redis.call(\"HGET\", key, \"msg\")\n\tend\nend\nreturn nil".as_bytes();
const REDSYNC_EXTEND_SETNX: &[u8] = "\n\tif redis.call(\"GET\", KEYS[1]) == ARGV[1] then\n\t\treturn redis.call(\"PEXPIRE\", KEYS[1], ARGV[2])\n\telseif redis.call(\"SET\", KEYS[1], ARGV[1], \"PX\", ARGV[2], \"NX\") then\n\t\treturn 1\n\telse\n\t\treturn 0\n\tend\n".as_bytes();
// ADR-0051: the rate limiters' scripts, as captured on the wire from Python
// `limits` 4.2, Go `redis_rate` v10, node `rate-limiter-flexible` 11.2.1 and
// `rate-limit-redis` 5.0.0. Each writes more than once, or computes on TIME.
const LIMITS_FIXED: &[u8] = "local current\nlocal amount = tonumber(ARGV[2])\ncurrent = redis.call(\"incrby\", KEYS[1], amount)\n\nif tonumber(current) == amount then\n    redis.call(\"expire\", KEYS[1], ARGV[1])\nend\n\nreturn current\n".as_bytes();
const LIMITS_MOVING: &[u8] = "local timestamp = tonumber(ARGV[1])\nlocal limit = tonumber(ARGV[2])\nlocal expiry = tonumber(ARGV[3])\nlocal amount = tonumber(ARGV[4])\n\nif amount > limit then\n    return false\nend\n\nlocal entry = redis.call('lindex', KEYS[1], limit - amount)\n\nif entry and tonumber(entry) >= timestamp - expiry then\n    return false\nend\n\nfor i = 1, amount do\n    redis.call('lpush', KEYS[1], timestamp)\nend\n\nredis.call('ltrim', KEYS[1], 0, limit - 1)\nredis.call('expire', KEYS[1], expiry)\n\nreturn true\n".as_bytes();
const LIMITS_MOVING_STATS: &[u8] = "local items = redis.call('lrange', KEYS[1], 0, tonumber(ARGV[2]))\nlocal expiry = tonumber(ARGV[1])\nlocal a = 0\nlocal oldest = nil\n\nfor idx=1,#items do\n    if tonumber(items[idx]) >= expiry then\n        a = a + 1\n\n        local value = tonumber(items[idx])\n        if oldest == nil or value < oldest then\n            oldest = value\n        end\n    else\n        break\n    end\nend\n\nif oldest then\n    return {tostring(oldest), a}\nend".as_bytes();
const LIMITS_SLIDING: &[u8] = "-- Time is in milliseconds in this script: TTL, expiry...\n\nlocal limit = tonumber(ARGV[1])\nlocal expiry = tonumber(ARGV[2]) * 1000\nlocal amount = tonumber(ARGV[3])\n\nif amount > limit then\n    return false\nend\n\nlocal current_ttl = tonumber(redis.call('pttl', KEYS[2]))\n\nif current_ttl > 0 and current_ttl < expiry then\n    -- Current window expired, shift it to the previous window\n    redis.call('rename', KEYS[2], KEYS[1])\n    redis.call('set', KEYS[2], 0, 'PX', current_ttl + expiry)\nend\n\nlocal previous_count = tonumber(redis.call('get', KEYS[1])) or 0\nlocal previous_ttl = tonumber(redis.call('pttl', KEYS[1])) or 0\nlocal current_count = tonumber(redis.call('get', KEYS[2])) or 0\ncurrent_ttl = tonumber(redis.call('pttl', KEYS[2])) or 0\n\n-- If the values don't exist yet, consider the TTL is 0\nif previous_ttl <= 0 then\n    previous_ttl = 0\nend\nif current_ttl <= 0 then\n    current_ttl = 0\nend\nlocal weighted_count = math.floor(previous_count * previous_ttl / expiry) + current_count\n\nif (weighted_count + amount) > limit then\n    return false\nend\n\n-- If the current counter exists, increase its value\nif redis.call('exists', KEYS[2]) == 1 then\n    redis.call('incrby', KEYS[2], amount)\nelse\n    -- Otherwise, set the value with twice the expiry time\n    redis.call('set', KEYS[2], amount, 'PX', expiry * 2)\nend\n\nreturn true\n".as_bytes();
const LIMITS_SLIDING_STATS: &[u8] = "local expiry = tonumber(ARGV[1]) * 1000\nlocal previous_count = redis.call('get', KEYS[1])\nlocal previous_ttl = redis.call('pttl', KEYS[1])\nlocal current_count = redis.call('get', KEYS[2])\nlocal current_ttl = redis.call('pttl', KEYS[2])\n\nif current_ttl > 0 and current_ttl < expiry then\n    -- Current window expired, shift it to the previous window\n    redis.call('rename', KEYS[2], KEYS[1])\n    redis.call('set', KEYS[2], 0, 'PX', current_ttl + expiry)\n    previous_count = redis.call('get', KEYS[1])\n    previous_ttl = redis.call('pttl', KEYS[1])\n    current_count = redis.call('get', KEYS[2])\n    current_ttl = redis.call('pttl', KEYS[2])\nend\n\nreturn {previous_count, previous_ttl, current_count, current_ttl}\n".as_bytes();
const REDIS_RATE_ALLOW: &[u8] = "\n-- this script has side-effects, so it requires replicate commands mode\nredis.replicate_commands()\n\nlocal rate_limit_key = KEYS[1]\nlocal burst = ARGV[1]\nlocal rate = ARGV[2]\nlocal period = ARGV[3]\nlocal cost = tonumber(ARGV[4])\n\nlocal emission_interval = period / rate\nlocal increment = emission_interval * cost\nlocal burst_offset = emission_interval * burst\n\n-- redis returns time as an array containing two integers: seconds of the epoch\n-- time (10 digits) and microseconds (6 digits). for convenience we need to\n-- convert them to a floating point number. the resulting number is 16 digits,\n-- bordering on the limits of a 64-bit double-precision floating point number.\n-- adjust the epoch to be relative to Jan 1, 2017 00:00:00 GMT to avoid floating\n-- point problems. this approach is good until \"now\" is 2,483,228,799 (Wed, 09\n-- Sep 2048 01:46:39 GMT), when the adjusted value is 16 digits.\nlocal jan_1_2017 = 1483228800\nlocal now = redis.call(\"TIME\")\nnow = (now[1] - jan_1_2017) + (now[2] / 1000000)\n\nlocal tat = redis.call(\"GET\", rate_limit_key)\n\nif not tat then\n  tat = now\nelse\n  tat = tonumber(tat)\nend\n\ntat = math.max(tat, now)\n\nlocal new_tat = tat + increment\nlocal allow_at = new_tat - burst_offset\n\nlocal diff = now - allow_at\nlocal remaining = diff / emission_interval\n\nif remaining < 0 then\n  local reset_after = tat - now\n  local retry_after = diff * -1\n  return {\n    0, -- allowed\n    0, -- remaining\n    tostring(retry_after),\n    tostring(reset_after),\n  }\nend\n\nlocal reset_after = new_tat - now\nif reset_after > 0 then\n  redis.call(\"SET\", rate_limit_key, new_tat, \"EX\", math.ceil(reset_after))\nend\nlocal retry_after = -1\nreturn {cost, remaining, tostring(retry_after), tostring(reset_after)}\n".as_bytes();
const REDIS_RATE_ALLOW_AT_MOST: &[u8] = "\n-- this script has side-effects, so it requires replicate commands mode\nredis.replicate_commands()\n\nlocal rate_limit_key = KEYS[1]\nlocal burst = ARGV[1]\nlocal rate = ARGV[2]\nlocal period = ARGV[3]\nlocal cost = tonumber(ARGV[4])\n\nlocal emission_interval = period / rate\nlocal burst_offset = emission_interval * burst\n\n-- redis returns time as an array containing two integers: seconds of the epoch\n-- time (10 digits) and microseconds (6 digits). for convenience we need to\n-- convert them to a floating point number. the resulting number is 16 digits,\n-- bordering on the limits of a 64-bit double-precision floating point number.\n-- adjust the epoch to be relative to Jan 1, 2017 00:00:00 GMT to avoid floating\n-- point problems. this approach is good until \"now\" is 2,483,228,799 (Wed, 09\n-- Sep 2048 01:46:39 GMT), when the adjusted value is 16 digits.\nlocal jan_1_2017 = 1483228800\nlocal now = redis.call(\"TIME\")\nnow = (now[1] - jan_1_2017) + (now[2] / 1000000)\n\nlocal tat = redis.call(\"GET\", rate_limit_key)\n\nif not tat then\n  tat = now\nelse\n  tat = tonumber(tat)\nend\n\ntat = math.max(tat, now)\n\nlocal diff = now - (tat - burst_offset)\nlocal remaining = diff / emission_interval\n\nif remaining < 1 then\n  local reset_after = tat - now\n  local retry_after = emission_interval - diff\n  return {\n    0, -- allowed\n    0, -- remaining\n    tostring(retry_after),\n    tostring(reset_after),\n  }\nend\n\nif remaining < cost then\n  cost = remaining\n  remaining = 0\nelse\n  remaining = remaining - cost\nend\n\nlocal increment = emission_interval * cost\nlocal new_tat = tat + increment\n\nlocal reset_after = new_tat - now\nif reset_after > 0 then\n  redis.call(\"SET\", rate_limit_key, new_tat, \"EX\", math.ceil(reset_after))\nend\n\nreturn {\n  cost,\n  remaining,\n  tostring(-1),\n  tostring(reset_after),\n}\n".as_bytes();
const RATE_LIMITER_FLEXIBLE: &[u8] = "redis.call('set', KEYS[1], 0, 'EX', ARGV[2], 'NX') local consumed = redis.call('incrby', KEYS[1], ARGV[1]) local ttl = redis.call('pttl', KEYS[1]) if ttl == -1 then   redis.call('expire', KEYS[1], ARGV[2])   ttl = 1000 * ARGV[2] end return {consumed, ttl} ".as_bytes();
const RATE_LIMIT_REDIS_5_INCR: &[u8] = "local windowMs = tonumber(ARGV[2])\nlocal resetOnChange = ARGV[1] == \"1\"\nlocal timeToExpire = redis.call(\"PTTL\", KEYS[1])\nif timeToExpire <= 0 then\nredis.call(\"SET\", KEYS[1], 1, \"PX\", windowMs)\nreturn { 1, windowMs }\nend\nlocal totalHits = redis.call(\"INCR\", KEYS[1])\nif resetOnChange then\nredis.call(\"PEXPIRE\", KEYS[1], windowMs)\ntimeToExpire = windowMs\nend\nreturn { totalHits, timeToExpire }".as_bytes();
// node rate-limit-redis 6.0.1, as sent (the source strips each line's indent).
const RATE_LIMIT_REDIS_INCR: &[u8] = "local windowMs = tonumber(ARGV[1])\nlocal timeToExpire = redis.call(\"PTTL\", KEYS[1])\nif timeToExpire <= 0 then\nredis.call(\"SET\", KEYS[1], 1, \"PX\", windowMs)\nreturn { 1, windowMs }\nend\nlocal totalHits = redis.call(\"INCR\", KEYS[1])        \nreturn { totalHits, timeToExpire }".as_bytes();
const RATE_LIMIT_REDIS_GET: &[u8] = "local totalHits = redis.call(\"GET\", KEYS[1])\nlocal timeToExpire = redis.call(\"PTTL\", KEYS[1])\nreturn { totalHits, timeToExpire }".as_bytes();

fn flint_only(family: &str) -> bool {
    // "sandbox": what Flint does around a script that Valkey does not (the
    // time limit, declared keys, all-or-nothing writes), ADR-0051.
    matches!(family, "json" | "bloom" | "sandbox")
}

/// Flint-only families a `--foreign` target (a real Redis with the RedisJSON
/// or RedisBloom module loaded) has nothing to say about. The sandbox family
/// asserts what Flint does around a script that Valkey does not, and against
/// a stock server its runaway-script case never returns: the server answers
/// BUSY to everything after it. That hung both compare scripts from ADR-0051
/// (2026-09-26) until ADR-0054 ran one.
fn foreign_skips(family: &str) -> bool {
    matches!(family, "sandbox")
}

/// Families only a Flint SEAT can answer, which is a different question from
/// whether an oracle exists. `FLINT*` is the admin surface: the proxy refuses
/// the whole prefix by design (`ERR admin commands are not available through
/// the proxy` -- it is the tenant boundary), and no foreign server has the
/// commands at all. So these cases are skipped, and SAID to be skipped,
/// whenever the target is not a seat: under `--reference`, under `--foreign`,
/// and whenever the run authenticates as a tenant, which means a proxy.
///
/// Skipping is the honest answer rather than a widening: a run through the
/// edge genuinely cannot observe a seat's admin surface, and a case that
/// reported PASS there would be reporting on a reply it never made.
fn seat_only(family: &str) -> bool {
    matches!(family, "flint")
}

/// Families a bare seat cannot answer: what the proxy serves on a client's
/// connection itself, as the oracle does. A subscription is held by the
/// proxy, and a seat refuses `SUBSCRIBE` (ADR-0052 D5), so these run against
/// the reference and through an authenticated proxy, and are skipped, and
/// said to be, against a seat.
fn edge_only(family: &str) -> bool {
    matches!(family, "pubsub_edge")
}

/// What a seat answers a subscription command: a proxy holds subscriptions
/// (ADR-0052 D5).
const SEAT_SUBSCRIBE: &str =
    "ERR a seat serves subscriptions to proxies only: subscribe through the proxy (ADR-0052)";
/// What sharded pub/sub is answered with, at a seat and through a proxy.
const SHARDED: &str = "ERR sharded pub/sub is not served: use SUBSCRIBE and PUBLISH (ADR-0052)";

fn corpus() -> Vec<Case> {
    let big = vec![0xABu8; 1024];
    vec![
        Case {
            family: "connection",
            name: "ping and echo",
            steps: vec![
                s(&[b"PING"], Expect::Pong),
                s(&[b"PING", b"hello"], Expect::Str(b"hello")),
                s(&[b"ECHO", b"abc"], Expect::Str(b"abc")),
                s(&[b"ECHO"], Expect::AnyError),
            ],
        },
        Case {
            family: "strings",
            name: "set then get",
            steps: vec![
                s(&[b"SET", b"k1", b"v1"], Expect::Ok),
                s(&[b"GET", b"k1"], Expect::Str(b"v1")),
            ],
        },
        Case {
            family: "strings",
            name: "get missing is nil",
            steps: vec![s(&[b"GET", b"missing"], Expect::Nil)],
        },
        Case {
            family: "strings",
            name: "set overwrites",
            steps: vec![
                s(&[b"SET", b"k2", b"a"], Expect::Ok),
                s(&[b"SET", b"k2", b"b"], Expect::Ok),
                s(&[b"GET", b"k2"], Expect::Str(b"b")),
            ],
        },
        Case {
            family: "strings",
            name: "set nx",
            steps: vec![
                s(&[b"SET", b"k3", b"a", b"NX"], Expect::Ok),
                s(&[b"SET", b"k3", b"b", b"NX"], Expect::Nil),
                s(&[b"GET", b"k3"], Expect::Str(b"a")),
                s(&[b"SET", b"k3", b"c", b"nx"], Expect::Nil),
            ],
        },
        Case {
            family: "strings",
            name: "set xx",
            steps: vec![
                s(&[b"SET", b"k4", b"a", b"XX"], Expect::Nil),
                s(&[b"GET", b"k4"], Expect::Nil),
                s(&[b"SET", b"k4", b"a"], Expect::Ok),
                s(&[b"SET", b"k4", b"b", b"XX"], Expect::Ok),
                s(&[b"GET", b"k4"], Expect::Str(b"b")),
            ],
        },
        Case {
            family: "strings",
            name: "set nx xx together is an error",
            steps: vec![s(&[b"SET", b"k5", b"v", b"NX", b"XX"], Expect::AnyError)],
        },
        Case {
            family: "strings",
            name: "empty value roundtrips",
            steps: vec![
                s(&[b"SET", b"k6", b""], Expect::Ok),
                s(&[b"GET", b"k6"], Expect::Str(b"")),
            ],
        },
        Case {
            family: "strings",
            name: "binary value roundtrips",
            steps: vec![
                s(&[b"SET", b"k7", b"\x00\xff\r\n\x00"], Expect::Ok),
                s(&[b"GET", b"k7"], Expect::Str(b"\x00\xff\r\n\x00")),
            ],
        },
        Case {
            family: "strings",
            name: "1kb value roundtrips",
            steps: vec![
                s(&[b"SET", b"k8", &big], Expect::Ok),
                s(&[b"GET", b"k8"], Expect::Bytes(big.clone())),
            ],
        },
        Case {
            family: "strings",
            name: "binary-safe keys",
            steps: vec![
                s(&[b"SET", b"k\x00\x01", b"v"], Expect::Ok),
                s(&[b"GET", b"k\x00\x01"], Expect::Str(b"v")),
                s(&[b"GET", b"k"], Expect::Nil),
            ],
        },
        Case {
            family: "strings",
            name: "getrange windows",
            steps: vec![
                s(&[b"SET", b"gr1", b"Hello World"], Expect::Ok),
                s(&[b"GETRANGE", b"gr1", b"0", b"4"], Expect::Str(b"Hello")),
                s(&[b"GETRANGE", b"gr1", b"-5", b"-1"], Expect::Str(b"World")),
                s(
                    &[b"GETRANGE", b"gr1", b"0", b"-1"],
                    Expect::Str(b"Hello World"),
                ),
                s(&[b"GETRANGE", b"gr1", b"9", b"2"], Expect::Str(b"")),
                s(&[b"GETRANGE", b"gr1", b"50", b"60"], Expect::Str(b"")),
                s(&[b"GETRANGE", b"nosuchg", b"0", b"-1"], Expect::Str(b"")),
            ],
        },
        // BUG-0219: Redis reads the key before LINDEX's and LSET's index,
        // and INCRBYFLOAT checks the type before its increment; i64::MIN is
        // out of LREM's and SRANDMEMBER's symmetric range (BUG-0218), and
        // LREM removed matches with it.
        Case {
            family: "lists",
            name: "the key is read before a bad index, and i64::MIN is out of range",
            steps: vec![
                s(&[b"LINDEX", b"nosuchli", b"abc"], Expect::Nil),
                s(&[b"LSET", b"nosuchli", b"abc", b"v"], Expect::Err("ERR no such key")),
                s(&[b"SET", b"lstr", b"v"], Expect::Ok),
                s(
                    &[b"LINDEX", b"lstr", b"abc"],
                    Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                ),
                s(
                    &[b"LSET", b"lstr", b"abc", b"v"],
                    Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                ),
                s(&[b"SADD", b"lset", b"m"], Expect::Int(1)),
                s(
                    &[b"INCRBYFLOAT", b"lset", b"abc"],
                    Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                ),
                s(&[b"RPUSH", b"li", b"a", b"b", b"a"], Expect::Int(3)),
                s(&[b"LINDEX", b"li", b"abc"], Expect::Err("ERR value is not an integer or out of range")),
                s(
                    &[b"LREM", b"li", b"-9223372036854775808", b"a"],
                    Expect::Err(
                        "ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807",
                    ),
                ),
                s(&[b"LLEN", b"li"], Expect::Int(3)),
                s(&[b"SET", b"lnan", b"nan"], Expect::Ok),
                s(&[b"INCRBYFLOAT", b"lnan", b"1"], Expect::Err("ERR value is not a valid float")),
                s(
                    &[b"SRANDMEMBER", b"lset", b"-9223372036854775808"],
                    Expect::Err(
                        "ERR value is out of range, value must between -9223372036854775807 and 9223372036854775807",
                    ),
                ),
            ],
        },
        // BUG-0215: LPOP and RPOP take a count, Redis 6.2's.
        Case {
            family: "lists",
            name: "lpop and rpop take a count",
            steps: vec![
                s(&[b"RPUSH", b"lc", b"a", b"b", b"c", b"d", b"e"], Expect::Int(5)),
                s(&[b"LPOP", b"lc", b"2"], Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"b")])),
                s(&[b"RPOP", b"lc", b"2"], Expect::Arr(vec![Expect::Str(b"e"), Expect::Str(b"d")])),
                s(&[b"LPOP", b"lc", b"0"], Expect::Arr(vec![])),
                s(&[b"LPOP", b"lc", b"-1"], Expect::Err("ERR value is out of range, must be positive")),
                s(&[b"LPOP", b"lc", b"x"], Expect::Err("ERR value is out of range, must be positive")),
                s(&[b"RPOP", b"lc", b"5"], Expect::Arr(vec![Expect::Str(b"c")])),
                s(&[b"EXISTS", b"lc"], Expect::Int(0)),
                s(&[b"LPOP", b"lc", b"2"], Expect::NilArray),
                s(&[b"RPOP", b"lc", b"0"], Expect::NilArray),
                s(&[b"LPOP", b"lc"], Expect::Nil),
                s(&[b"LPOP", b"lc", b"1", b"2"], Expect::Err("ERR wrong number of arguments for 'lpop' command")),
                s(&[b"RPOP", b"lc", b"1", b"2"], Expect::Err("ERR wrong number of arguments for 'rpop' command")),
            ],
        },
        // BUG-0214, from a three-way differential against Redis 8.2 and
        // Valkey: GETRANGE is not LRANGE. An end before the string clamps
        // to its first byte; two negatives in the wrong order are empty.
        Case {
            family: "strings",
            name: "getrange clamps an end before the string",
            steps: vec![
                s(&[b"SET", b"gr2", b"Hello"], Expect::Ok),
                s(&[b"GETRANGE", b"gr2", b"0", b"-100"], Expect::Str(b"H")),
                s(&[b"GETRANGE", b"gr2", b"-100", b"-100"], Expect::Str(b"H")),
                s(&[b"GETRANGE", b"gr2", b"-3", b"-100"], Expect::Str(b"")),
                s(&[b"GETRANGE", b"gr2", b"-10", b"-20"], Expect::Str(b"")),
            ],
        },
        // BUG-0213: one kind of expiry option, and an instant that fits.
        Case {
            family: "strings",
            name: "set takes one kind of expiry and refuses one out of range",
            steps: vec![
                s(&[b"SET", b"se", b"v", b"EX", b"10", b"PX", b"10"], Expect::Err("ERR syntax error")),
                s(&[b"SET", b"se", b"v", b"KEEPTTL", b"EX", b"10"], Expect::Err("ERR syntax error")),
                s(&[b"SET", b"se", b"v", b"PX", b"10", b"KEEPTTL"], Expect::Err("ERR syntax error")),
                s(&[b"SET", b"se", b"v", b"EXAT", b"1", b"PXAT", b"1"], Expect::Err("ERR syntax error")),
                s(&[b"SET", b"se", b"v", b"NX", b"XX"], Expect::Err("ERR syntax error")),
                // Every option parses before the time is read.
                s(&[b"SET", b"se", b"v", b"EX", b"abc", b"NX", b"XX"], Expect::Err("ERR syntax error")),
                s(&[b"EXISTS", b"se"], Expect::Int(0)),
                // The same option twice is allowed, and the last wins.
                s(&[b"SET", b"se", b"v", b"EX", b"10", b"EX", b"100"], Expect::Ok),
                s(&[b"TTL", b"se"], Expect::IntRange(95, 100)),
                s(
                    &[b"SET", b"se", b"w", b"EX", b"9223372036854775807"],
                    Expect::Err("ERR invalid expire time in 'set' command"),
                ),
                s(&[b"SET", b"se", b"w", b"EXAT", b"0"], Expect::Err("ERR invalid expire time in 'set' command")),
                s(&[b"SET", b"se", b"w", b"PXAT", b"-1"], Expect::Err("ERR invalid expire time in 'set' command")),
                s(
                    &[b"SETEX", b"se", b"9223372036854775807", b"w"],
                    Expect::Err("ERR invalid expire time in 'setex' command"),
                ),
                s(
                    &[b"GETEX", b"se", b"EX", b"9223372036854775807"],
                    Expect::Err("ERR invalid expire time in 'getex' command"),
                ),
                s(&[b"GET", b"se"], Expect::Str(b"v")),
                s(&[b"TTL", b"se"], Expect::IntRange(95, 100)),
                s(&[b"GETEX", b"se", b"PERSIST", b"EX", b"10"], Expect::Err("ERR syntax error")),
                s(&[b"GETEX", b"se", b"EX", b"10", b"EX", b"20"], Expect::Str(b"v")),
                s(&[b"TTL", b"se"], Expect::IntRange(15, 20)),
            ],
        },
        // BUG-0213: Redis reads a canonical integer only, and names the
        // overflow.
        Case {
            family: "strings",
            name: "integers are canonical and overflow is named",
            steps: vec![
                s(&[b"SET", b"ci", b"01"], Expect::Ok),
                s(&[b"INCR", b"ci"], Expect::Err("ERR value is not an integer or out of range")),
                s(&[b"SET", b"ci", b"+1"], Expect::Ok),
                s(&[b"INCRBY", b"ci", b"1"], Expect::Err("ERR value is not an integer or out of range")),
                s(&[b"GET", b"ci"], Expect::Str(b"+1")),
                s(&[b"INCRBY", b"ci2", b"01"], Expect::Err("ERR value is not an integer or out of range")),
                s(&[b"SET", b"ci3", b"9223372036854775807"], Expect::Ok),
                s(&[b"INCR", b"ci3"], Expect::Err("ERR increment or decrement would overflow")),
                s(&[b"SET", b"ci4", b"5"], Expect::Ok),
                s(
                    &[b"DECRBY", b"ci4", b"-9223372036854775808"],
                    Expect::Err("ERR decrement would overflow"),
                ),
                s(&[b"GET", b"ci4"], Expect::Str(b"5")),
                s(&[b"HSET", b"ch", b"f", b"01", b"g", b"9223372036854775807"], Expect::Int(2)),
                s(&[b"HINCRBY", b"ch", b"f", b"1"], Expect::Err("ERR hash value is not an integer")),
                s(&[b"HINCRBY", b"ch", b"g", b"1"], Expect::Err("ERR increment or decrement would overflow")),
            ],
        },
        Case {
            family: "strings",
            name: "setrange pad overwrite ttl",
            steps: vec![
                // Missing key + offset: zero-padded creation.
                s(&[b"SETRANGE", b"sr1", b"5", b"World"], Expect::Int(10)),
                s(&[b"GET", b"sr1"], Expect::Str(b"\0\0\0\0\0World")),
                s(&[b"SET", b"sr2", b"Hello World"], Expect::Ok),
                s(&[b"SETRANGE", b"sr2", b"6", b"Redis"], Expect::Int(11)),
                s(&[b"GET", b"sr2"], Expect::Str(b"Hello Redis")),
                // Empty patch never creates the key.
                s(&[b"SETRANGE", b"srn", b"0", b""], Expect::Int(0)),
                s(&[b"EXISTS", b"srn"], Expect::Int(0)),
                // TTL survives the in-place mutation.
                s(&[b"SETEX", b"srt", b"100", b"hello"], Expect::Ok),
                s(&[b"SETRANGE", b"srt", b"0", b"H"], Expect::Int(5)),
                s(&[b"TTL", b"srt"], Expect::IntRange(95, 100)),
                s(&[b"GET", b"srt"], Expect::Str(b"Hello")),
                s(&[b"SETRANGE", b"sr2", b"-1", b"x"], Expect::AnyError),
            ],
        },
        Case {
            family: "strings",
            name: "bitfield (BUG-0192)",
            steps: vec![
                // Unsigned: WRAP by default, then SAT, then FAIL.
                s(
                    &[b"BITFIELD", b"bf1", b"SET", b"u8", b"0", b"255", b"GET", b"u8", b"0"],
                    Expect::Arr(vec![Expect::Int(0), Expect::Int(255)]),
                ),
                s(
                    &[b"BITFIELD", b"bf1", b"INCRBY", b"u8", b"0", b"10"],
                    Expect::Arr(vec![Expect::Int(9)]),
                ),
                s(
                    &[b"BITFIELD", b"bf1", b"OVERFLOW", b"SAT", b"INCRBY", b"u8", b"0", b"300"],
                    Expect::Arr(vec![Expect::Int(255)]),
                ),
                s(
                    &[b"BITFIELD", b"bf1", b"OVERFLOW", b"FAIL", b"INCRBY", b"u8", b"0", b"1"],
                    Expect::Arr(vec![Expect::Nil]),
                ),
                s(&[b"GET", b"bf1"], Expect::Str(b"\xff")),
                s(
                    &[b"BITFIELD_RO", b"bf1", b"GET", b"u8", b"0", b"GET", b"i8", b"0"],
                    Expect::Arr(vec![Expect::Int(255), Expect::Int(-1)]),
                ),
                // Signed: two's complement WRAP, and SAT at both ends.
                s(
                    &[b"BITFIELD", b"bf2", b"SET", b"i8", b"0", b"-128", b"INCRBY", b"i8", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Int(0), Expect::Int(127)]),
                ),
                s(
                    &[
                        b"BITFIELD", b"bf2", b"OVERFLOW", b"SAT", b"INCRBY", b"i8", b"0", b"200",
                        b"INCRBY", b"i8", b"0", b"-300",
                    ],
                    Expect::Arr(vec![Expect::Int(127), Expect::Int(-128)]),
                ),
                s(
                    &[
                        b"BITFIELD", b"bf7", b"SET", b"i64", b"0", b"9223372036854775807",
                        b"INCRBY", b"i64", b"0", b"1",
                    ],
                    Expect::Arr(vec![Expect::Int(0), Expect::Int(i64::MIN)]),
                ),
                // An out-of-range SET: WRAP keeps the low bits, FAIL refuses.
                s(
                    &[b"BITFIELD", b"bf3", b"SET", b"u8", b"0", b"-1", b"GET", b"u8", b"0"],
                    Expect::Arr(vec![Expect::Int(0), Expect::Int(255)]),
                ),
                s(
                    &[b"BITFIELD", b"bf3", b"OVERFLOW", b"FAIL", b"SET", b"u2", b"0", b"7", b"GET", b"u8", b"0"],
                    Expect::Arr(vec![Expect::Nil, Expect::Int(255)]),
                ),
                // `#n` offsets count in fields; fields straddle bytes.
                s(
                    &[
                        b"BITFIELD", b"bf4", b"SET", b"u4", b"#1", b"15", b"GET", b"u4", b"#0",
                        b"GET", b"u8", b"0", b"SET", b"u12", b"4", b"4095", b"GET", b"u16", b"0",
                    ],
                    Expect::Arr(vec![
                        Expect::Int(0),
                        Expect::Int(0),
                        Expect::Int(15),
                        Expect::Int(3840),
                        Expect::Int(4095),
                    ]),
                ),
                // Sidekiq's metrics flush, as it sends it.
                s(
                    &[
                        b"BITFIELD", b"h|Job-1", b"OVERFLOW", b"SAT", b"INCRBY", b"u16", b"#0", b"1",
                        b"INCRBY", b"u16", b"#3", b"5",
                    ],
                    Expect::Arr(vec![Expect::Int(1), Expect::Int(5)]),
                ),
                s(&[b"STRLEN", b"h|Job-1"], Expect::Int(8)),
                // Reads past the end read zeros and create nothing; a write
                // refused by FAIL still grows the string, as Valkey's does.
                s(
                    &[b"BITFIELD", b"bf5", b"GET", b"u8", b"100"],
                    Expect::Arr(vec![Expect::Int(0)]),
                ),
                s(&[b"EXISTS", b"bf5"], Expect::Int(0)),
                s(&[b"BITFIELD", b"bf5"], Expect::Arr(vec![])),
                s(
                    &[b"BITFIELD", b"bf6", b"OVERFLOW", b"FAIL", b"INCRBY", b"u2", b"8", b"9"],
                    Expect::Arr(vec![Expect::Nil]),
                ),
                s(&[b"STRLEN", b"bf6"], Expect::Int(2)),
                // In place: the TTL survives.
                s(&[b"SETEX", b"bft", b"100", b"a"], Expect::Ok),
                s(
                    &[b"BITFIELD", b"bft", b"SET", b"u8", b"0", b"66"],
                    Expect::Arr(vec![Expect::Int(97)]),
                ),
                s(&[b"TTL", b"bft"], Expect::IntRange(95, 100)),
                s(&[b"GET", b"bft"], Expect::Str(b"B")),
                // Errors, each with Valkey's text.
                s(&[b"BITFIELD", b"bf1", b"GET", b"u64", b"0"], Expect::Err("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")),
                s(&[b"BITFIELD", b"bf1", b"GET", b"i65", b"0"], Expect::Err("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")),
                s(&[b"BITFIELD", b"bf1", b"GET", b"u0", b"0"], Expect::Err("ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.")),
                s(
                    &[b"BITFIELD", b"bf1", b"GET", b"u8", b"-1"],
                    Expect::Err("ERR bit offset is not an integer or out of range"),
                ),
                s(
                    &[b"BITFIELD", b"bf1", b"GET", b"u8", b"4294967296000"],
                    Expect::Err("ERR bit offset is not an integer or out of range"),
                ),
                s(
                    &[b"BITFIELD", b"bf1", b"OVERFLOW", b"NOPE"],
                    Expect::Err("ERR Invalid OVERFLOW type specified"),
                ),
                s(&[b"BITFIELD", b"bf1", b"FROB"], Expect::Err("ERR syntax error")),
                s(&[b"BITFIELD", b"bf1", b"GET", b"u8"], Expect::Err("ERR syntax error")),
                s(
                    &[b"BITFIELD", b"bf1", b"SET", b"u8", b"0", b"x"],
                    Expect::Err("ERR value is not an integer or out of range"),
                ),
                s(
                    &[b"BITFIELD_RO", b"bf1", b"SET", b"u8", b"0", b"1"],
                    Expect::Err("ERR BITFIELD_RO only supports the GET subcommand"),
                ),
                s(&[b"GET", b"bf1"], Expect::Str(b"\xff")),
                s(&[b"LPUSH", b"bfl", b"a"], Expect::Int(1)),
                s(&[b"BITFIELD", b"bfl", b"GET", b"u8", b"0"], Expect::AnyError),
                s(&[b"BITFIELD"], Expect::AnyError),
            ],
        },
        Case {
            family: "strings",
            name: "bitmaps: SETBIT, GETBIT, BITCOUNT, BITPOS",
            steps: vec![
                s(&[b"SETBIT", b"bm", b"7", b"1"], Expect::Int(0)),
                s(&[b"SETBIT", b"bm", b"7", b"1"], Expect::Int(1)),
                s(&[b"SETBIT", b"bm", b"20", b"1"], Expect::Int(0)),
                s(&[b"GET", b"bm"], Expect::Str(b"\x01\x00\x08")),
                s(&[b"GETBIT", b"bm", b"7"], Expect::Int(1)),
                s(&[b"GETBIT", b"bm", b"6"], Expect::Int(0)),
                s(&[b"GETBIT", b"bm", b"1000"], Expect::Int(0)),
                s(&[b"GETBIT", b"nobm", b"0"], Expect::Int(0)),
                s(&[b"SETBIT", b"bm", b"-1", b"1"], Expect::Err("ERR bit offset is not an integer or out of range")),
                s(&[b"SETBIT", b"bm", b"4294967296", b"1"], Expect::Err("ERR bit offset is not an integer or out of range")),
                s(&[b"GETBIT", b"bm", b"x"], Expect::Err("ERR bit offset is not an integer or out of range")),
                s(
                    &[b"SETBIT", b"bm", b"0", b"2"],
                    Expect::Err("ERR bit is not an integer or out of range"),
                ),
                s(&[b"SET", b"bc", b"foobar"], Expect::Ok),
                s(&[b"BITCOUNT", b"bc"], Expect::Int(26)),
                s(&[b"BITCOUNT", b"bc", b"1", b"1"], Expect::Int(6)),
                s(&[b"BITCOUNT", b"bc", b"5", b"30", b"BIT"], Expect::Int(17)),
                s(&[b"BITCOUNT", b"bc", b"-2", b"-1"], Expect::Int(7)),
                // Two negative indexes the wrong way round count nothing.
                s(&[b"BITCOUNT", b"bc", b"-1", b"-2"], Expect::Int(0)),
                // A start without an end: Valkey 9.1 counts to the end
                // (Redis 8.2 answers a syntax error).
                s(&[b"BITCOUNT", b"bc", b"1"], Expect::Int(22)),
                s(&[b"BITCOUNT", b"bc", b"0", b"-1", b"WAT"], Expect::Err("ERR syntax error")),
                s(&[b"BITCOUNT", b"nobm"], Expect::Int(0)),
                s(&[b"SET", b"bp", b"\xff\xf0\x00"], Expect::Ok),
                s(&[b"BITPOS", b"bp", b"0"], Expect::Int(12)),
                s(&[b"BITPOS", b"bp", b"1", b"2"], Expect::Int(-1)),
                // No end: the string reads as padded with zeros on the right.
                s(&[b"BITPOS", b"bp", b"0", b"2"], Expect::Int(16)),
                s(&[b"BITPOS", b"bp", b"0", b"0", b"0"], Expect::Int(-1)),
                s(&[b"BITPOS", b"bp", b"0", b"0", b"-1", b"BIT"], Expect::Int(12)),
                s(&[b"BITPOS", b"nobm", b"0", b"5"], Expect::Int(0)),
                s(&[b"BITPOS", b"nobm", b"1"], Expect::Int(-1)),
                s(&[b"SET", b"be", b""], Expect::Ok),
                s(&[b"BITPOS", b"be", b"0"], Expect::Int(-1)),
                s(
                    &[b"BITPOS", b"bp", b"2"],
                    Expect::Err("ERR The bit argument must be 1 or 0."),
                ),
                s(&[b"RPUSH", b"bl", b"x"], Expect::Int(1)),
                s(&[b"SETBIT", b"bl", b"0", b"1"], Expect::AnyError),
                s(&[b"BITCOUNT", b"bl"], Expect::AnyError),
            ],
        },
        Case {
            family: "strings",
            name: "bitmaps: BITOP",
            steps: vec![
                s(&[b"SET", b"{bo}a", b"\xff\xf0\x00"], Expect::Ok),
                s(&[b"SET", b"{bo}b", b"\x0f\x0f"], Expect::Ok),
                s(&[b"BITOP", b"AND", b"{bo}d", b"{bo}a", b"{bo}b"], Expect::Int(3)),
                s(&[b"GET", b"{bo}d"], Expect::Str(b"\x0f\x00\x00")),
                s(&[b"BITOP", b"or", b"{bo}d", b"{bo}a", b"{bo}b"], Expect::Int(3)),
                s(&[b"GET", b"{bo}d"], Expect::Str(b"\xff\xff\x00")),
                s(&[b"BITOP", b"XOR", b"{bo}d", b"{bo}a", b"{bo}b"], Expect::Int(3)),
                s(&[b"GET", b"{bo}d"], Expect::Str(b"\xf0\xff\x00")),
                s(&[b"BITOP", b"NOT", b"{bo}d", b"{bo}b"], Expect::Int(2)),
                s(&[b"GET", b"{bo}d"], Expect::Str(b"\xf0\xf0")),
                // A missing source is an empty string; the destination
                // loses its TTL, and is deleted when the result is empty.
                s(&[b"EXPIRE", b"{bo}d", b"100"], Expect::Int(1)),
                s(&[b"BITOP", b"AND", b"{bo}d", b"{bo}a", b"{bo}none"], Expect::Int(3)),
                s(&[b"GET", b"{bo}d"], Expect::Str(b"\x00\x00\x00")),
                s(&[b"TTL", b"{bo}d"], Expect::Int(-1)),
                s(&[b"BITOP", b"OR", b"{bo}d", b"{bo}none"], Expect::Int(0)),
                s(&[b"EXISTS", b"{bo}d"], Expect::Int(0)),
                s(
                    &[b"BITOP", b"NOT", b"{bo}d", b"{bo}a", b"{bo}b"],
                    Expect::Err("ERR BITOP NOT must be called with a single source key."),
                ),
                s(&[b"BITOP", b"WAT", b"{bo}d", b"{bo}a"], Expect::Err("ERR syntax error")),
                s(&[b"RPUSH", b"{bo}l", b"x"], Expect::Int(1)),
                s(&[b"BITOP", b"AND", b"{bo}d", b"{bo}a", b"{bo}l"], Expect::AnyError),
            ],
        },
        // Streams (ADR-0052 D6), against Valkey 9.1. `~` trimming is not
        // here: Valkey trims whole internal nodes and Flint trims exactly
        // (a documented difference); only its argument errors are.
        Case {
            family: "streams",
            name: "streams: XADD's IDs, NOMKSTREAM, XLEN, TYPE",
            steps: vec![
                s(&[b"XADD", b"xs", b"1-1", b"f", b"v"], Expect::Str(b"1-1")),
                s(&[b"XADD", b"xs", b"1-1", b"f", b"v"], Expect::Err("ERR The ID specified in XADD is equal or smaller than the target stream top item")),
                s(&[b"XADD", b"xs", b"1-0", b"f", b"v"], Expect::Err("ERR The ID specified in XADD is equal or smaller than the target stream top item")),
                s(
                    &[b"XADD", b"xs", b"0-0", b"f", b"v"],
                    Expect::Err("ERR The ID specified in XADD must be greater than 0-0"),
                ),
                s(&[b"XADD", b"xs", b"1-*", b"a", b"1"], Expect::Str(b"1-2")),
                s(&[b"XADD", b"xs", b"2-5", b"a", b"1", b"b", b"2"], Expect::Str(b"2-5")),
                s(&[b"XADD", b"xs", b"2", b"a", b"1"], Expect::Err("ERR The ID specified in XADD is equal or smaller than the target stream top item")),
                s(
                    &[b"XADD", b"xs", b"3-0", b"a"],
                    Expect::Err("ERR wrong number of arguments for 'xadd' command"),
                ),
                s(
                    &[b"XADD", b"xs", b"*"],
                    Expect::Err("ERR wrong number of arguments for 'xadd' command"),
                ),
                s(&[b"XADD", b"xs", b"abc", b"f", b"v"], Expect::Err("ERR Invalid stream ID specified as stream command argument")),
                s(&[b"XADD", b"xs", b"1-x", b"f", b"v"], Expect::Err("ERR Invalid stream ID specified as stream command argument")),
                s(&[b"XADD", b"xs", b"NOMKSTREAM", b"9-0", b"f", b"v"], Expect::Str(b"9-0")),
                s(&[b"XADD", b"xnone", b"NOMKSTREAM", b"*", b"f", b"v"], Expect::Nil),
                s(&[b"EXISTS", b"xnone"], Expect::Int(0)),
                s(&[b"XLEN", b"xs"], Expect::Int(4)),
                s(&[b"XLEN", b"xnone"], Expect::Int(0)),
                s(&[b"TYPE", b"xs"], Expect::Simple("stream")),
                s(
                    &[b"XADD", b"xs", b"18446744073709551615-18446744073709551615", b"f", b"v"],
                    Expect::Str(b"18446744073709551615-18446744073709551615"),
                ),
                s(
                    &[b"XADD", b"xs", b"*", b"f", b"v"],
                    Expect::Err(
                        "ERR The stream has exhausted the last possible ID, unable to add more items",
                    ),
                ),
                s(&[b"SET", b"xstr", b"v"], Expect::Ok),
                s(&[b"XADD", b"xstr", b"*", b"f", b"v"], Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                s(&[b"XLEN", b"xstr"], Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                s(&[b"XRANGE", b"xstr", b"-", b"+"], Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value")),
            ],
        },
        Case {
            family: "streams",
            name: "streams: XRANGE and XREVRANGE",
            steps: vec![
                s(&[b"XADD", b"xr", b"1-1", b"f", b"v"], Expect::Str(b"1-1")),
                s(&[b"XADD", b"xr", b"1-2", b"a", b"1"], Expect::Str(b"1-2")),
                s(&[b"XADD", b"xr", b"2-5", b"a", b"1", b"b", b"2"], Expect::Str(b"2-5")),
                s(
                    &[b"XRANGE", b"xr", b"-", b"+"],
                    Expect::Arr(vec![
                        xentry(b"1-1", &[b"f", b"v"]),
                        xentry(b"1-2", &[b"a", b"1"]),
                        xentry(b"2-5", &[b"a", b"1", b"b", b"2"]),
                    ]),
                ),
                s(
                    &[b"XRANGE", b"xr", b"-", b"+", b"COUNT", b"2"],
                    Expect::Arr(vec![xentry(b"1-1", &[b"f", b"v"]), xentry(b"1-2", &[b"a", b"1"])]),
                ),
                s(
                    &[b"XRANGE", b"xr", b"(1-1", b"2"],
                    Expect::Arr(vec![
                        xentry(b"1-2", &[b"a", b"1"]),
                        xentry(b"2-5", &[b"a", b"1", b"b", b"2"]),
                    ]),
                ),
                s(
                    &[b"XRANGE", b"xr", b"1", b"1"],
                    Expect::Arr(vec![xentry(b"1-1", &[b"f", b"v"]), xentry(b"1-2", &[b"a", b"1"])]),
                ),
                s(&[b"XRANGE", b"xr", b"+", b"-"], Expect::Arr(vec![])),
                s(&[b"XRANGE", b"xr", b"-", b"+", b"COUNT", b"0"], Expect::NilArray),
                // COUNT 0 is read after the key: a missing one is empty, and
                // another type WRONGTYPE.
                s(&[b"XRANGE", b"xnone", b"-", b"+", b"COUNT", b"0"], Expect::Arr(vec![])),
                s(&[b"XREVRANGE", b"xnone", b"+", b"-", b"COUNT", b"-1"], Expect::Arr(vec![])),
                s(&[b"SET", b"xstr", b"x"], Expect::Ok),
                s(&[b"XRANGE", b"xstr", b"-", b"+", b"COUNT", b"0"], Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                s(&[b"XREVRANGE", b"xstr", b"+", b"-", b"COUNT", b"-1"], Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                s(&[b"XRANGE", b"xr", b"x", b"+"], Expect::Err("ERR Invalid stream ID specified as stream command argument")),
                s(&[b"XRANGE", b"xr", b"-", b"+", b"LIMIT", b"1"], Expect::Err("ERR syntax error")),
                s(&[b"XRANGE", b"xnone", b"-", b"+"], Expect::Arr(vec![])),
                s(
                    &[b"XRANGE", b"xr"],
                    Expect::Err("ERR wrong number of arguments for 'xrange' command"),
                ),
                s(
                    &[b"XREVRANGE", b"xr", b"+", b"-", b"COUNT", b"1"],
                    Expect::Arr(vec![xentry(b"2-5", &[b"a", b"1", b"b", b"2"])]),
                ),
                s(
                    &[b"XREVRANGE", b"xr", b"2-5", b"(1-1"],
                    Expect::Arr(vec![
                        xentry(b"2-5", &[b"a", b"1", b"b", b"2"]),
                        xentry(b"1-2", &[b"a", b"1"]),
                    ]),
                ),
            ],
        },
        Case {
            family: "streams",
            name: "streams: XDEL and exact XTRIM",
            steps: vec![
                s(&[b"XADD", b"xt", b"MAXLEN", b"2", b"1-1", b"f", b"v"], Expect::Str(b"1-1")),
                s(&[b"XADD", b"xt", b"MAXLEN", b"2", b"2-1", b"f", b"v"], Expect::Str(b"2-1")),
                s(&[b"XADD", b"xt", b"MAXLEN", b"=", b"2", b"3-1", b"f", b"v"], Expect::Str(b"3-1")),
                s(
                    &[b"XRANGE", b"xt", b"-", b"+"],
                    Expect::Arr(vec![xentry(b"2-1", &[b"f", b"v"]), xentry(b"3-1", &[b"f", b"v"])]),
                ),
                s(&[b"XADD", b"xt", b"MINID", b"3", b"4-1", b"f", b"v"], Expect::Str(b"4-1")),
                s(&[b"XLEN", b"xt"], Expect::Int(2)),
                s(
                    &[b"XADD", b"xt", b"MAXLEN", b"-1", b"7-1", b"f", b"v"],
                    Expect::Err("ERR The MAXLEN argument must be >= 0."),
                ),
                s(
                    &[b"XADD", b"xt", b"LIMIT", b"10", b"7-1", b"f", b"v"],
                    Expect::Err(
                        "ERR syntax error, LIMIT cannot be used without specifying a trimming strategy",
                    ),
                ),
                s(
                    &[b"XADD", b"xt", b"MAXLEN", b"1", b"LIMIT", b"10", b"7-1", b"f", b"v"],
                    Expect::Err("ERR syntax error, LIMIT cannot be used without the special ~ option"),
                ),
                s(
                    &[b"XADD", b"xt", b"MAXLEN", b"1", b"MINID", b"1", b"7-1", b"f", b"v"],
                    Expect::Err(
                        "ERR syntax error, MAXLEN and MINID options at the same time are not compatible",
                    ),
                ),
                s(&[b"XADD", b"xt", b"9-1", b"f", b"v"], Expect::Str(b"9-1")),
                s(&[b"XADD", b"xt", b"10-1", b"f", b"v"], Expect::Str(b"10-1")),
                s(&[b"XTRIM", b"xt", b"MAXLEN", b"1"], Expect::Int(3)),
                s(&[b"XTRIM", b"xt", b"MINID", b"0"], Expect::Int(0)),
                s(&[b"XTRIM", b"xt", b"FOO", b"1"], Expect::Err("ERR syntax error")),
                s(
                    &[b"XTRIM", b"xt"],
                    Expect::Err("ERR wrong number of arguments for 'xtrim' command"),
                ),
                s(&[b"XTRIM", b"xnone", b"MAXLEN", b"1"], Expect::Int(0)),
                s(&[b"XADD", b"xt", b"11-1", b"f", b"v"], Expect::Str(b"11-1")),
                s(&[b"XADD", b"xt", b"12-1", b"f", b"v"], Expect::Str(b"12-1")),
                s(&[b"XDEL", b"xt", b"11-1", b"99-9"], Expect::Int(1)),
                s(&[b"XDEL", b"xt", b"11-1"], Expect::Int(0)),
                s(&[b"XDEL", b"xt", b"x"], Expect::Err("ERR Invalid stream ID specified as stream command argument")),
                s(&[b"XDEL", b"xnone", b"1-1"], Expect::Int(0)),
                s(&[b"XTRIM", b"xt", b"MINID", b"12"], Expect::Int(1)),
                s(&[b"XRANGE", b"xt", b"-", b"+"], Expect::Arr(vec![xentry(b"12-1", &[b"f", b"v"])])),
                // An emptied stream stays, and its last ID still bounds XADD.
                s(&[b"XTRIM", b"xt", b"MAXLEN", b"0"], Expect::Int(1)),
                s(&[b"EXISTS", b"xt"], Expect::Int(1)),
                s(&[b"XADD", b"xt", b"12-1", b"f", b"v"], Expect::Err("ERR The ID specified in XADD is equal or smaller than the target stream top item")),
            ],
        },
        Case {
            family: "streams",
            name: "streams: XREAD without waiting",
            steps: vec![
                s(&[b"XADD", b"{xr}1", b"1-1", b"a", b"1"], Expect::Str(b"1-1")),
                s(&[b"XADD", b"{xr}1", b"2-1", b"a", b"2"], Expect::Str(b"2-1")),
                s(&[b"XADD", b"{xr}2", b"1-1", b"b", b"1"], Expect::Str(b"1-1")),
                s(
                    &[b"XREAD", b"STREAMS", b"{xr}1", b"0"],
                    Expect::Arr(vec![Expect::Arr(vec![
                        Expect::Str(b"{xr}1"),
                        Expect::Arr(vec![xentry(b"1-1", &[b"a", b"1"]), xentry(b"2-1", &[b"a", b"2"])]),
                    ])]),
                ),
                s(
                    &[b"XREAD", b"COUNT", b"1", b"STREAMS", b"{xr}1", b"{xr}2", b"0", b"0"],
                    Expect::Arr(vec![
                        Expect::Arr(vec![
                            Expect::Str(b"{xr}1"),
                            Expect::Arr(vec![xentry(b"1-1", &[b"a", b"1"])]),
                        ]),
                        Expect::Arr(vec![
                            Expect::Str(b"{xr}2"),
                            Expect::Arr(vec![xentry(b"1-1", &[b"b", b"1"])]),
                        ]),
                    ]),
                ),
                s(
                    &[b"XREAD", b"STREAMS", b"{xr}1", b"{xr}2", b"1-1", b"1-1"],
                    Expect::Arr(vec![Expect::Arr(vec![
                        Expect::Str(b"{xr}1"),
                        Expect::Arr(vec![xentry(b"2-1", &[b"a", b"2"])]),
                    ])]),
                ),
                s(&[b"XREAD", b"STREAMS", b"{xr}1", b"$"], Expect::NilArray),
                s(
                    &[b"XREAD", b"STREAMS", b"{xr}1", b"+"],
                    Expect::Arr(vec![Expect::Arr(vec![
                        Expect::Str(b"{xr}1"),
                        Expect::Arr(vec![xentry(b"2-1", &[b"a", b"2"])]),
                    ])]),
                ),
                s(&[b"XREAD", b"STREAMS", b"xnone", b"0"], Expect::NilArray),
                s(
                    &[b"XREAD", b"STREAMS", b"{xr}1", b"{xr}2", b"0"],
                    Expect::Err(
                        "ERR Unbalanced 'xread' list of streams: for each stream key an ID or '$' \
                         must be specified.",
                    ),
                ),
                s(&[b"XREAD", b"STREAMS", b"{xr}1", b"x"], Expect::Err("ERR Invalid stream ID specified as stream command argument")),
                s(&[b"XREAD", b"FOO", b"STREAMS", b"{xr}1", b"0"], Expect::Err("ERR syntax error")),
                s(
                    &[b"XREAD", b"BLOCK", b"-1", b"STREAMS", b"{xr}1", b"0"],
                    Expect::Err("ERR timeout is negative"),
                ),
                // A BLOCK with entries to read answers at once; with none,
                // after 10 ms, a null.
                s(
                    &[b"XREAD", b"BLOCK", b"10", b"STREAMS", b"{xr}2", b"0"],
                    Expect::Arr(vec![Expect::Arr(vec![
                        Expect::Str(b"{xr}2"),
                        Expect::Arr(vec![xentry(b"1-1", &[b"b", b"1"])]),
                    ])]),
                ),
                s(&[b"XREAD", b"BLOCK", b"10", b"STREAMS", b"{xr}1", b"$"], Expect::NilArray),
                s(&[b"SET", b"{xr}s", b"v"], Expect::Ok),
                s(&[b"XREAD", b"STREAMS", b"{xr}s", b"0"], Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value")),
            ],
        },
        // The libraries Redis loads into scripts (ADR-0052 D3), in Rust
        // here, against Valkey's C ones: each step's reply is Valkey 9.1's,
        // captured. `cmsgpack.pack(2^63)` is left out: the C library's cast
        // of 2^63 to int64 differs by CPU. So is an object's key order:
        // Valkey 9.1's tables order keys differently from Lua 5.1's, which
        // Redis 8.2 and Flint share.
        Case {
            family: "lua",
            name: "the cjson library, as Valkey bundles it",
            steps: vec![
                s(&[b"EVAL", r#"return cjson.encode({1,2,3})"#.as_bytes(), b"0"], Expect::Str(b"[1,2,3]")),
                s(&[b"EVAL", r#"return cjson.encode({a=1})"#.as_bytes(), b"0"], Expect::Str(b"{\"a\":1}")),
                s(&[b"EVAL", r#"return cjson.encode({})"#.as_bytes(), b"0"], Expect::Str(b"{}")),
                s(&[b"EVAL", r#"return cjson.encode({{}})"#.as_bytes(), b"0"], Expect::Str(b"[{}]")),
                s(&[b"EVAL", r#"return cjson.encode('a/b\"c\\d')"#.as_bytes(), b"0"], Expect::Str(b"\"a\\/b\\\"c\\\\d\"")),
                s(&[b"EVAL", r#"return cjson.encode('\0\1\8\9\10\12\13\31\127\128\255')"#.as_bytes(), b"0"], Expect::Str(b"\"\\u0000\\u0001\\b\\t\\n\\f\\r\\u001f\\u007f\x80\xff\"")),
                s(&[b"EVAL", r#"return cjson.encode(1)"#.as_bytes(), b"0"], Expect::Str(b"1")),
                s(&[b"EVAL", r#"return cjson.encode(0.1)"#.as_bytes(), b"0"], Expect::Str(b"0.1")),
                s(&[b"EVAL", r#"return cjson.encode(-0)"#.as_bytes(), b"0"], Expect::Str(b"-0")),
                s(&[b"EVAL", r#"return cjson.encode(1e20)"#.as_bytes(), b"0"], Expect::Str(b"1e+20")),
                s(&[b"EVAL", r#"return cjson.encode(3.14159265358979)"#.as_bytes(), b"0"], Expect::Str(b"3.1415926535898")),
                s(&[b"EVAL", r#"return cjson.encode(2^53)"#.as_bytes(), b"0"], Expect::Str(b"9.007199254741e+15")),
                s(&[b"EVAL", r#"return cjson.encode(123456789012345)"#.as_bytes(), b"0"], Expect::Str(b"1.2345678901234e+14")),
                s(&[b"EVAL", r#"return cjson.encode(1/3)"#.as_bytes(), b"0"], Expect::Str(b"0.33333333333333")),
                s(&[b"EVAL", r#"return cjson.encode(-1.5e-7)"#.as_bytes(), b"0"], Expect::Str(b"-1.5e-07")),
                s(&[b"EVAL", r#"return cjson.encode(1/0)"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Cannot serialise number: must not be NaN or Inf script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.encode(0/0)"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Cannot serialise number: must not be NaN or Inf script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.encode(true)"#.as_bytes(), b"0"], Expect::Str(b"true")),
                s(&[b"EVAL", r#"return cjson.encode(nil)"#.as_bytes(), b"0"], Expect::Str(b"null")),
                s(&[b"EVAL", r#"return cjson.encode(cjson.null)"#.as_bytes(), b"0"], Expect::Str(b"null")),
                s(&[b"EVAL", r#"return cjson.encode({[1]=1,[3]=3})"#.as_bytes(), b"0"], Expect::Str(b"[1,null,3]")),
                s(&[b"EVAL", r#"return cjson.encode({[1]=1,[20]=1})"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Cannot serialise table: excessively sparse array script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.encode({[1]=1,[11]=1})"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Cannot serialise table: excessively sparse array script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.encode({[2]=1})"#.as_bytes(), b"0"], Expect::Str(b"[null,1]")),
                s(&[b"EVAL", r#"return cjson.encode({1,a=2})"#.as_bytes(), b"0"], Expect::Str(b"{\"1\":1,\"a\":2}")),
                s(&[b"EVAL", r#"return cjson.encode({[1.5]=1})"#.as_bytes(), b"0"], Expect::Str(b"{\"1.5\":1}")),
                s(&[b"EVAL", r#"return cjson.encode({[true]=1})"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Cannot serialise boolean: table key must be a number or string script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.encode({x={y={z={1,2}}}})"#.as_bytes(), b"0"], Expect::Str(b"{\"x\":{\"y\":{\"z\":[1,2]}}}")),
                s(&[b"EVAL", r#"return cjson.encode({b=1,a=2,c=3,d={e=4}})"#.as_bytes(), b"0"], Expect::Str(b"{\"a\":2,\"d\":{\"e\":4},\"c\":3,\"b\":1}")),
                s(&[b"EVAL", r#"local t = {} local cur = t for i=1,1001 do cur[1] = {} cur = cur[1] end return cjson.encode(t)"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Cannot serialise, excessive nesting (1001) script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.encode({[-1]=1})"#.as_bytes(), b"0"], Expect::Str(b"{\"-1\":1}")),
                s(&[b"EVAL", r#"return cjson.encode({[0]=1})"#.as_bytes(), b"0"], Expect::Str(b"{\"0\":1}")),
                s(&[b"EVAL", r#"return cjson.encode('\226\130\172')"#.as_bytes(), b"0"], Expect::Str(b"\"\xe2\x82\xac\"")),
                s(&[b"EVAL", r#"return cjson.encode(cjson.decode('[1,2,{"a":null,"b":[true,false]}]'))"#.as_bytes(), b"0"], Expect::Str(b"[1,2,{\"a\":null,\"b\":[true,false]}]")),
                s(&[b"EVAL", r#"local v = cjson.decode('{"a":1.5e3,"b":"x\\u00e9\\ud83d\\ude00\\n"}') return {tostring(v.a), v.b}"#.as_bytes(), b"0"], Expect::Arr(vec![Expect::Str(b"1500"), Expect::Str(b"x\xc3\xa9\xf0\x9f\x98\x80\x0a")])),
                s(&[b"EVAL", r#"return type(cjson.decode('null'))"#.as_bytes(), b"0"], Expect::Str(b"userdata")),
                s(&[b"EVAL", r#"return tostring(cjson.decode('null') == cjson.null)"#.as_bytes(), b"0"], Expect::Str(b"true")),
                s(&[b"EVAL", r#"return cjson.decode('[1,2')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Expected comma or array end but found T_END at character 5 script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.decode('{"a":}')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Expected value but found T_OBJ_END at character 6 script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.decode('')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Expected value but found T_END at character 1 script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.decode('nul')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Expected value but found invalid token at character 1 script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.decode('"abc')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Expected value but found unexpected end of string at character 5 script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.decode('[1] x')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Expected the end but found invalid token at character 5 script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.decode(5)"#.as_bytes(), b"0"], Expect::Int(5)),
                s(&[b"EVAL", r#"return tostring(cjson.decode('12345678901234567890'))"#.as_bytes(), b"0"], Expect::Str(b"1.2345678901235e+19")),
                s(&[b"EVAL", r#"return tostring(cjson.decode('-0'))"#.as_bytes(), b"0"], Expect::Str(b"-0")),
                s(&[b"EVAL", r#"return tostring(cjson.decode('1e400'))"#.as_bytes(), b"0"], Expect::Str(b"inf")),
                s(&[b"EVAL", r#"return #cjson.decode('[]')"#.as_bytes(), b"0"], Expect::Int(0)),
                s(&[b"EVAL", r#"return cjson.encode(cjson.decode('{}'))"#.as_bytes(), b"0"], Expect::Str(b"{}")),
                s(&[b"EVAL", r#"return cjson.encode(cjson.decode('[]'))"#.as_bytes(), b"0"], Expect::Str(b"{}")),
                s(&[b"EVAL", r#"return cjson.decode('"\\x"')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Expected value but found invalid escape code at character 2 script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.decode('[0x10]')"#.as_bytes(), b"0"], Expect::Arr(vec![Expect::Int(16)])),
                s(&[b"EVAL", r#"return tostring(cjson.decode(' 7 '))"#.as_bytes(), b"0"], Expect::Str(b"7")),
            ],
        },
        Case {
            family: "lua",
            name: "the cmsgpack library, as Valkey bundles it",
            steps: vec![
                s(&[b"EVAL", r#"return cmsgpack.pack(1)"#.as_bytes(), b"0"], Expect::Str(b"\x01")),
                s(&[b"EVAL", r#"return cmsgpack.pack(-1)"#.as_bytes(), b"0"], Expect::Str(b"\xff")),
                s(&[b"EVAL", r#"return cmsgpack.pack(127)"#.as_bytes(), b"0"], Expect::Str(b"\x7f")),
                s(&[b"EVAL", r#"return cmsgpack.pack(128)"#.as_bytes(), b"0"], Expect::Str(b"\xcc\x80")),
                s(&[b"EVAL", r#"return cmsgpack.pack(255)"#.as_bytes(), b"0"], Expect::Str(b"\xcc\xff")),
                s(&[b"EVAL", r#"return cmsgpack.pack(256)"#.as_bytes(), b"0"], Expect::Str(b"\xcd\x01\x00")),
                s(&[b"EVAL", r#"return cmsgpack.pack(65536)"#.as_bytes(), b"0"], Expect::Str(b"\xce\x00\x01\x00\x00")),
                s(&[b"EVAL", r#"return cmsgpack.pack(2^32)"#.as_bytes(), b"0"], Expect::Str(b"\xcf\x00\x00\x00\x01\x00\x00\x00\x00")),
                s(&[b"EVAL", r#"return cmsgpack.pack(-32)"#.as_bytes(), b"0"], Expect::Str(b"\xe0")),
                s(&[b"EVAL", r#"return cmsgpack.pack(-33)"#.as_bytes(), b"0"], Expect::Str(b"\xd0\xdf")),
                s(&[b"EVAL", r#"return cmsgpack.pack(-129)"#.as_bytes(), b"0"], Expect::Str(b"\xd1\xff\x7f")),
                s(&[b"EVAL", r#"return cmsgpack.pack(-32769)"#.as_bytes(), b"0"], Expect::Str(b"\xd2\xff\xff\x7f\xff")),
                s(&[b"EVAL", r#"return cmsgpack.pack(-2^31-1)"#.as_bytes(), b"0"], Expect::Str(b"\xd3\xff\xff\xff\xff\x7f\xff\xff\xff")),
                s(&[b"EVAL", r#"return cmsgpack.pack(1.5)"#.as_bytes(), b"0"], Expect::Str(b"\xca?\xc0\x00\x00")),
                s(&[b"EVAL", r#"return cmsgpack.pack(0.1)"#.as_bytes(), b"0"], Expect::Str(b"\xcb?\xb9\x99\x99\x99\x99\x99\x9a")),
                s(&[b"EVAL", r#"return cmsgpack.pack(1/0)"#.as_bytes(), b"0"], Expect::Str(b"\xca\x7f\x80\x00\x00")),
                s(&[b"EVAL", r#"return cmsgpack.pack(-2^63)"#.as_bytes(), b"0"], Expect::Str(b"\xd3\x80\x00\x00\x00\x00\x00\x00\x00")),
                s(&[b"EVAL", r#"return cmsgpack.pack(2^64)"#.as_bytes(), b"0"], Expect::Str(b"\xca_\x80\x00\x00")),
                s(&[b"EVAL", r#"return cmsgpack.pack('abc')"#.as_bytes(), b"0"], Expect::Str(b"\xa3abc")),
                s(&[b"EVAL", r#"return cmsgpack.pack(string.rep('x',32))"#.as_bytes(), b"0"], Expect::Str(b"\xd9 xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")),
                s(&[b"EVAL", r#"return cmsgpack.pack(string.rep('x',256))"#.as_bytes(), b"0"], Expect::Str(b"\xda\x01\x00xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")),
                s(&[b"EVAL", r#"return cmsgpack.pack(nil)"#.as_bytes(), b"0"], Expect::Str(b"\xc0")),
                s(&[b"EVAL", r#"return cmsgpack.pack(true, false)"#.as_bytes(), b"0"], Expect::Str(b"\xc3\xc2")),
                s(&[b"EVAL", r#"return cmsgpack.pack({1,2,3})"#.as_bytes(), b"0"], Expect::Str(b"\x93\x01\x02\x03")),
                s(&[b"EVAL", r#"return cmsgpack.pack({a=1})"#.as_bytes(), b"0"], Expect::Str(b"\x81\xa1a\x01")),
                s(&[b"EVAL", r#"return cmsgpack.pack({})"#.as_bytes(), b"0"], Expect::Str(b"\x90")),
                s(&[b"EVAL", r#"return cmsgpack.pack({[1]=1,[3]=3})"#.as_bytes(), b"0"], Expect::Str(b"\x82\x01\x01\x03\x03")),
                s(&[b"EVAL", r#"local t={} for i=1,16 do t[i]=i end return cmsgpack.pack(t)"#.as_bytes(), b"0"], Expect::Str(b"\xdc\x00\x10\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f\x10")),
                s(&[b"EVAL", r#"local t = {} local cur = t for i=1,20 do cur[1] = {} cur = cur[1] end return cmsgpack.pack(t)"#.as_bytes(), b"0"], Expect::Str(b"\x91\x91\x91\x91\x91\x91\x91\x91\x91\x91\x91\x91\x91\x91\x91\x91\xc0")),
                s(&[b"EVAL", r#"return cmsgpack.pack()"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: bad argument #0 to 'pack' (MessagePack pack needs input.) script: on @user_script:1.")),
                s(&[b"EVAL", r#"return {cmsgpack.unpack(cmsgpack.pack(1, 'a', {1,2}, {x=1}))}"#.as_bytes(), b"0"], Expect::Arr(vec![Expect::Int(1), Expect::Str(b"a"), Expect::Arr(vec![Expect::Int(1), Expect::Int(2)]), Expect::Arr(vec![])])),
                s(&[b"EVAL", r#"local a = cmsgpack.unpack(cmsgpack.pack({x={y=2}})) return a.x.y"#.as_bytes(), b"0"], Expect::Int(2)),
                s(&[b"EVAL", r#"return cmsgpack.unpack('\145')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Missing bytes in input. script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cmsgpack.unpack('\193')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Bad data format in input. script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cmsgpack.unpack('')"#.as_bytes(), b"0"], Expect::Nil),
                s(&[b"EVAL", r#"return tostring(cmsgpack.unpack('\203\63\240\0\0\0\0\0\0'))"#.as_bytes(), b"0"], Expect::Str(b"1")),
                s(&[b"EVAL", r#"return tostring(cmsgpack.unpack('\207\255\255\255\255\255\255\255\255'))"#.as_bytes(), b"0"], Expect::Str(b"-1")),
                s(&[b"EVAL", r#"return tostring(cmsgpack.unpack('\211\128\0\0\0\0\0\0\0'))"#.as_bytes(), b"0"], Expect::Str(b"-9.2233720368548e+18")),
                s(&[b"EVAL", r#"return cmsgpack.unpack('\196\3abc')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Bad data format in input. script: on @user_script:1.")),
                s(&[b"EVAL", r#"return {cmsgpack.unpack_one(cmsgpack.pack(1,2,3))}"#.as_bytes(), b"0"], Expect::Arr(vec![Expect::Int(1), Expect::Int(1)])),
                s(&[b"EVAL", r#"return {cmsgpack.unpack_one(cmsgpack.pack(1,2,3), 1)}"#.as_bytes(), b"0"], Expect::Arr(vec![Expect::Int(2), Expect::Int(2)])),
                s(&[b"EVAL", r#"return {cmsgpack.unpack_limit(cmsgpack.pack(1,2,3), 2)}"#.as_bytes(), b"0"], Expect::Arr(vec![Expect::Int(2), Expect::Int(1), Expect::Int(2)])),
                s(&[b"EVAL", r#"return {cmsgpack.unpack_limit(cmsgpack.pack(1,2,3), 2, 1)}"#.as_bytes(), b"0"], Expect::Arr(vec![Expect::Int(-1), Expect::Int(2), Expect::Int(3)])),
                s(&[b"EVAL", r#"return {cmsgpack.unpack(cmsgpack.pack(nil, 1))}"#.as_bytes(), b"0"], Expect::Arr(vec![])),
                s(&[b"EVAL", r#"return {cmsgpack.unpack('\147\1\192\3')}"#.as_bytes(), b"0"], Expect::Arr(vec![Expect::Arr(vec![Expect::Int(1)])])),
            ],
        },
        Case {
            family: "lua",
            name: "the bit and struct libraries, as Valkey bundles them",
            steps: vec![
                s(&[b"EVAL", r#"return bit.band(0xff, 0x0f)"#.as_bytes(), b"0"], Expect::Int(15)),
                s(&[b"EVAL", r#"return bit.bor(1, 2, 4)"#.as_bytes(), b"0"], Expect::Int(7)),
                s(&[b"EVAL", r#"return bit.bxor(5, 3)"#.as_bytes(), b"0"], Expect::Int(6)),
                s(&[b"EVAL", r#"return bit.bnot(0)"#.as_bytes(), b"0"], Expect::Int(-1)),
                s(&[b"EVAL", r#"return bit.lshift(1, 31)"#.as_bytes(), b"0"], Expect::Int(-2147483648)),
                s(&[b"EVAL", r#"return bit.rshift(-1, 28)"#.as_bytes(), b"0"], Expect::Int(15)),
                s(&[b"EVAL", r#"return bit.arshift(-256, 4)"#.as_bytes(), b"0"], Expect::Int(-16)),
                s(&[b"EVAL", r#"return bit.rol(1, 33)"#.as_bytes(), b"0"], Expect::Int(2)),
                s(&[b"EVAL", r#"return bit.ror(1, 1)"#.as_bytes(), b"0"], Expect::Int(-2147483648)),
                s(&[b"EVAL", r#"return bit.bswap(0x12345678)"#.as_bytes(), b"0"], Expect::Int(2018915346)),
                s(&[b"EVAL", r#"return bit.tobit(2^32 + 1)"#.as_bytes(), b"0"], Expect::Int(1)),
                s(&[b"EVAL", r#"return bit.tohex(255)"#.as_bytes(), b"0"], Expect::Str(b"000000ff")),
                s(&[b"EVAL", r#"return bit.tohex(255, -4)"#.as_bytes(), b"0"], Expect::Str(b"00FF")),
                s(&[b"EVAL", r#"return bit.tohex(-1, 2)"#.as_bytes(), b"0"], Expect::Str(b"ff")),
                s(&[b"EVAL", r#"return bit.tobit(1.5)"#.as_bytes(), b"0"], Expect::Int(2)),
                s(&[b"EVAL", r#"return bit.band()"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: bad argument #1 to 'band' (number expected, got no value) script: on @user_script:1.")),
                s(&[b"EVAL", r#"return bit.band('x')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: bad argument #1 to 'band' (number expected, got string) script: on @user_script:1.")),
                s(&[b"EVAL", r#"return struct.pack('>I2', 258)"#.as_bytes(), b"0"], Expect::Str(b"\x01\x02")),
                s(&[b"EVAL", r#"return struct.pack('<i4', -2)"#.as_bytes(), b"0"], Expect::Str(b"\xfe\xff\xff\xff")),
                s(&[b"EVAL", r#"return {struct.unpack('>I2', '\1\2')}"#.as_bytes(), b"0"], Expect::Arr(vec![Expect::Int(258), Expect::Int(3)])),
                s(&[b"EVAL", r#"return struct.size('>i4I2')"#.as_bytes(), b"0"], Expect::Int(6)),
                s(&[b"EVAL", r#"return struct.pack('b', 300)"#.as_bytes(), b"0"], Expect::Str(b",")),
                s(&[b"EVAL", r#"return struct.pack('s', 'ab')"#.as_bytes(), b"0"], Expect::Str(b"ab\x00")),
                s(&[b"EVAL", r#"return {struct.unpack('c2', 'abcd')}"#.as_bytes(), b"0"], Expect::Arr(vec![Expect::Str(b"ab"), Expect::Int(3)])),
            ],
        },
        // The libraries at their edges, against Valkey's C: an error names
        // the script's line even when the script tail-calls, and the
        // function as the script called it; nesting and counts fail where
        // Valkey's Lua stack runs out (8,000 slots), with its message.
        Case {
            family: "lua",
            name: "the libraries at their limits, as Valkey's C reaches them",
            steps: vec![
                s(&[b"EVAL", r#"local ok, e = pcall(cjson.decode, '[1') return e"#.as_bytes(), b"0"], Expect::Str(b"Expected comma or array end but found T_END at character 3")),
                s(&[b"EVAL", r#"local ok, e = pcall(function() return cjson.decode('[1') end) return e"#.as_bytes(), b"0"], Expect::Str(b"user_script:1: Expected comma or array end but found T_END at character 3")),
                s(&[b"EVAL", r#"local ok, e = pcall(cjson.decode, {}) return e"#.as_bytes(), b"0"], Expect::Str(b"bad argument #1 to '?' (string expected, got table)")),
                s(&[b"EVAL", r#"local d = cjson.decode return d({})"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: bad argument #1 to 'd' (string expected, got table) script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson:decode('1')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: calling 'decode' on bad self (expected 1 argument) script: on @user_script:1.")),
                s(&[b"EVAL", r#"return bit.bor(1, 'x', 'y')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: bad argument #3 to 'bor' (number expected, got string) script: on @user_script:1.")),
                s(&[b"EVAL", r#"return bit.tohex(255, nil)"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: bad argument #2 to 'tohex' (number expected, got nil) script: on @user_script:1.")),
                s(&[b"EVAL", r#"return struct.pack('ii', 1)"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: bad argument #3 to 'pack' (number expected, got nil) script: on @user_script:1.")),
                s(&[b"EVAL", r#"return struct.pack('q')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: bad argument #1 to 'pack' (invalid format option 'q') script: on @user_script:1.")),
                s(&[b"EVAL", r#"return {struct.unpack('c0', '\3abc')}"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: format 'c0' needs a previous size script: on @user_script:1.")),
                s(&[b"EVAL", r#"return {struct.unpack('bc0', '\3abcd')}"#.as_bytes(), b"0"], Expect::Arr(vec![Expect::Str(b"abc"), Expect::Int(5)])),
                s(&[b"EVAL", r#"return {struct.unpack('sc0', '2\0abcd')}"#.as_bytes(), b"0"], Expect::Arr(vec![Expect::Str(b"ab"), Expect::Int(5)])),
                s(&[b"EVAL", r#"return cjson.decode('\0a')"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: JSON parser does not support UTF-16 or UTF-32 script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cjson.encode({[-1/0]=1})"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Cannot serialise number: must not be NaN or Inf script: on @user_script:1.")),
                s(&[b"EVAL", r#"return cmsgpack.unpack('\129\192\1')"#.as_bytes(), b"0"], Expect::Err("ERR table index is nil script: on @user_script:1.")),
                s(&[b"EVAL", r#"return type(cjson.decode(string.rep('[', 1000) .. string.rep(']', 1000)))"#.as_bytes(), b"0"], Expect::Str(b"table")),
                s(&[b"EVAL", r#"return cjson.decode(string.rep('{"a":', 1001) .. '1' .. string.rep('}', 1001))"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Found too many nested data structures (1001) at character 5001 script: on @user_script:1.")),
                s(&[b"EVAL", r#"local t = {} t[1] = t return cjson.encode(t)"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Cannot serialise, excessive nesting (1001) script: on @user_script:1.")),
                s(&[b"EVAL", r#"return type(cmsgpack.unpack(string.rep('\145', 3999) .. '\1'))"#.as_bytes(), b"0"], Expect::Str(b"table")),
                s(&[b"EVAL", r#"return type(cmsgpack.unpack(string.rep('\145', 4000) .. '\1'))"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: stack overflow (in function mp_decode_to_lua_array) script: on @user_script:1.")),
                s(&[b"EVAL", r#"return type(cmsgpack.unpack(string.rep('\129\1', 3999) .. '\1'))"#.as_bytes(), b"0"], Expect::Str(b"table")),
                s(&[b"EVAL", r#"return type(cmsgpack.unpack(string.rep('\129\1', 4000) .. '\1'))"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: stack overflow (too many return values at once; use unpack_one or unpack_limit instead.) script: on @user_script:1.")),
                s(&[b"EVAL", r#"return type(cmsgpack.unpack(string.rep('\145', 4000)))"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: stack overflow (in function mp_decode_to_lua_array) script: on @user_script:1.")),
                s(&[b"EVAL", r#"return type(cmsgpack.unpack(string.rep('\145', 3999)))"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Missing bytes in input. script: on @user_script:1.")),
                s(&[b"EVAL", r#"local t = {cmsgpack.unpack(string.rep('\1', 7999))} return #t"#.as_bytes(), b"0"], Expect::Int(7999)),
                s(&[b"EVAL", r#"local t = {cmsgpack.unpack(string.rep('\1', 8000))} return #t"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: stack overflow (too many return values at once; use unpack_one or unpack_limit instead.) script: on @user_script:1.")),
                s(&[b"EVAL", r#"local t = {cmsgpack.unpack_limit(string.rep('\1', 9000), 7999)} return #t"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: stack overflow (in function mp_unpack_full) script: on @user_script:1.")),
                s(&[b"EVAL", r#"local t = {} for i=1,4000 do t[i]='s' end return #cmsgpack.pack(unpack(t))"#.as_bytes(), b"0"], Expect::Int(8000)),
                s(&[b"EVAL", r#"local t = {} for i=1,4001 do t[i]='s' end return #cmsgpack.pack(unpack(t))"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: bad argument #0 to 'pack' (Too many arguments for MessagePack pack.) script: on @user_script:1.")),
                s(&[b"EVAL", r#"local t = {} for i=1,9000 do t['k'..i]=i end return #cmsgpack.pack(t)"#.as_bytes(), b"0"], Expect::Int(79514)),
                s(&[b"EVAL", r#"return select('#', struct.unpack(string.rep('c1', 7997), string.rep('x', 9000)))"#.as_bytes(), b"0"], Expect::Int(7998)),
                s(&[b"EVAL", r#"return select('#', struct.unpack(string.rep('c1', 7998), string.rep('x', 9000)))"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: stack overflow (too many results) script: on @user_script:1.")),
                s(&[b"EVAL", b"local a = 1\nlocal b = 2\nreturn bit.band('x')", b"0"], Expect::Err("ERR user_script:3: bad argument #1 to 'band' (number expected, got string) script: on @user_script:3.")),
            ],
        },
        // BUG-0247: the libraries and `redis` are read-only stand-ins, which
        // list, walk and encode as the tables behind them, as Valkey's
        // read-only tables do; writing to one is refused as Valkey refuses it.
        // The `redis` table's count is left out: Flint does not claim to be
        // Valkey (no SERVER_NAME, VALKEY_VERSION, VALKEY_VERSION_NUM), and
        // serves no `acl_check_cmd`.
        Case {
            family: "lua",
            name: "the libraries list and encode as Valkey's read-only tables do",
            steps: vec![
                s(&[b"EVAL", r#"local n=0 for k in pairs(cjson) do n=n+1 end return n"#.as_bytes(), b"0"], Expect::Int(13)),
                s(&[b"EVAL", r#"local n=0 for k in pairs(cmsgpack) do n=n+1 end return n"#.as_bytes(), b"0"], Expect::Int(8)),
                s(&[b"EVAL", r#"local n=0 for k in pairs(bit) do n=n+1 end return n"#.as_bytes(), b"0"], Expect::Int(12)),
                s(&[b"EVAL", r#"local n=0 for k in pairs(struct) do n=n+1 end return n"#.as_bytes(), b"0"], Expect::Int(3)),
                s(&[b"EVAL", r#"local n=0 for k in pairs(string) do n=n+1 end return n"#.as_bytes(), b"0"], Expect::Int(15)),
                s(&[b"EVAL", r#"local n=0 for k in pairs(table) do n=n+1 end return n"#.as_bytes(), b"0"], Expect::Int(9)),
                s(&[b"EVAL", r#"local n=0 for k in pairs(math) do n=n+1 end return n"#.as_bytes(), b"0"], Expect::Int(31)),
                s(&[b"EVAL", r#"local n=0 for k in pairs(coroutine) do n=n+1 end return n"#.as_bytes(), b"0"], Expect::Int(6)),
                s(&[b"EVAL", r#"local f, t, k = pairs(cjson) return {type(f), tostring(t == cjson), tostring(k)}"#.as_bytes(), b"0"], Expect::Arr(vec![Expect::Str(b"function"), Expect::Str(b"true"), Expect::Str(b"nil")])),
                s(&[b"EVAL", r#"local n = 0 for k, v in next, cjson do n = n + 1 end return n"#.as_bytes(), b"0"], Expect::Int(13)),
                s(&[b"EVAL", r#"return rawget(string, 'format') == string.format"#.as_bytes(), b"0"], Expect::Int(1)),
                s(&[b"EVAL", r#"return type(getmetatable(cjson))"#.as_bytes(), b"0"], Expect::Str(b"nil")),
                s(&[b"EVAL", r#"local ok, e = pcall(setmetatable, cjson, {}) return e"#.as_bytes(), b"0"], Expect::Str(b"Attempt to modify a readonly table")),
                s(&[b"EVAL", r#"local ok, e = pcall(rawset, cjson, 'x', 1) return e"#.as_bytes(), b"0"], Expect::Str(b"Attempt to modify a readonly table")),
                s(&[b"EVAL", r#"local ok, e = pcall(function() cjson.encode = 1 end) return e"#.as_bytes(), b"0"], Expect::Str(b"user_script:1: Attempt to modify a readonly table")),
                s(&[b"EVAL", r#"return #cmsgpack.pack(cjson)"#.as_bytes(), b"0"], Expect::Int(205)),
                s(&[b"EVAL", r#"return cjson.encode(bit)"#.as_bytes(), b"0"], Expect::Err("ERR user_script:1: Cannot serialise function: type not supported script: on @user_script:1.")),
                s(&[b"EVAL", r#"return redis.REDIS_VERSION"#.as_bytes(), b"0"], Expect::Str(b"7.2.4")),
                s(&[b"EVAL", r#"return redis.REDIS_VERSION_NUM"#.as_bytes(), b"0"], Expect::Int(459268)),
                s(&[b"EVAL", r#"return server == redis"#.as_bytes(), b"0"], Expect::Int(1)),
                s(&[b"EVAL", r#"return server.call('PING')"#.as_bytes(), b"0"], Expect::Simple("PONG")),
                s(&[b"EVAL", r#"return cjson.new() == cjson"#.as_bytes(), b"0"], Expect::Nil),
                s(&[b"EVAL", r#"return cjson.new().encode({1})"#.as_bytes(), b"0"], Expect::Str(b"[1]")),
                s(&[b"EVAL", r#"return cmsgpack._DESCRIPTION"#.as_bytes(), b"0"], Expect::Str(b"MessagePack C implementation for Lua")),
            ],
        },
        // BUG-0246: `redis.call` took one mlua reference per argument, and
        // ran out near 8,000; Lua's `unpack` stops at 7,997.
        Case {
            family: "lua",
            name: "redis.call takes as many arguments as unpack gives",
            steps: vec![
                s(&[b"EVAL", r#"local t = {} for i=1,7997 do t[i]='{a}x'..i end return redis.call('DEL', unpack(t))"#.as_bytes(), b"1", b"{a}k"], Expect::Int(0)),
                s(&[b"EVAL", r#"local t = {} for i=1,7997 do t[i]='{a}x'..i end return redis.pcall('DEL', unpack(t))"#.as_bytes(), b"1", b"{a}k"], Expect::Int(0)),
                s(&[b"EVAL", r#"local t = {} for i=1,7997 do t[i]='{a}x'..i end return #redis.call('MGET', unpack(t))"#.as_bytes(), b"1", b"{a}k"], Expect::Int(7997)),
            ],
        },
        // A seat holds no client's subscription: a proxy does, and
        // subscribes at the seat for it (ADR-0052 D5). Sharded pub/sub is
        // not served anywhere.
        Case {
            family: "flint",
            name: "a seat refuses subscriptions and sharded pub/sub",
            steps: vec![
                s(&[b"SUBSCRIBE", b"ch"], Expect::Err(SEAT_SUBSCRIBE)),
                s(&[b"PSUBSCRIBE", b"ch"], Expect::Err(SEAT_SUBSCRIBE)),
                s(&[b"UNSUBSCRIBE", b"ch"], Expect::Err(SEAT_SUBSCRIBE)),
                s(&[b"PUNSUBSCRIBE", b"ch"], Expect::Err(SEAT_SUBSCRIBE)),
                s(&[b"SSUBSCRIBE", b"ch"], Expect::Err(SHARDED)),
                s(&[b"SUNSUBSCRIBE", b"ch"], Expect::Err(SHARDED)),
                s(&[b"SPUBLISH", b"ch", b"m"], Expect::Err(SHARDED)),
            ],
        },
        // One connection's subscriptions, as the proxy holds them: each
        // confirmation counts the channels and patterns held, a RESP3 push
        // and a RESP2 array alike. Messages take two connections, which the
        // corpus does not have: tools/pubsub_drill.sh.
        Case {
            family: "pubsub_edge",
            name: "pubsub: subscription confirmations",
            steps: vec![
                s(
                    &[b"SUBSCRIBE", b"{pe}a"],
                    Expect::Arr(vec![
                        Expect::Str(b"subscribe"),
                        Expect::Str(b"{pe}a"),
                        Expect::Int(1),
                    ]),
                ),
                s(
                    &[b"SUBSCRIBE", b"{pe}a"],
                    Expect::Arr(vec![
                        Expect::Str(b"subscribe"),
                        Expect::Str(b"{pe}a"),
                        Expect::Int(1),
                    ]),
                ),
                s(
                    &[b"PSUBSCRIBE", b"{pe}*"],
                    Expect::Arr(vec![
                        Expect::Str(b"psubscribe"),
                        Expect::Str(b"{pe}*"),
                        Expect::Int(2),
                    ]),
                ),
                s(
                    &[b"UNSUBSCRIBE", b"{pe}zz"],
                    Expect::Arr(vec![
                        Expect::Str(b"unsubscribe"),
                        Expect::Str(b"{pe}zz"),
                        Expect::Int(2),
                    ]),
                ),
                s(
                    &[b"UNSUBSCRIBE", b"{pe}a"],
                    Expect::Arr(vec![
                        Expect::Str(b"unsubscribe"),
                        Expect::Str(b"{pe}a"),
                        Expect::Int(1),
                    ]),
                ),
                s(
                    &[b"PUNSUBSCRIBE", b"{pe}*"],
                    Expect::Arr(vec![
                        Expect::Str(b"punsubscribe"),
                        Expect::Str(b"{pe}*"),
                        Expect::Int(0),
                    ]),
                ),
                s(
                    &[b"UNSUBSCRIBE"],
                    Expect::Arr(vec![
                        Expect::Str(b"unsubscribe"),
                        Expect::Nil,
                        Expect::Int(0),
                    ]),
                ),
                s(
                    &[b"PUNSUBSCRIBE"],
                    Expect::Arr(vec![
                        Expect::Str(b"punsubscribe"),
                        Expect::Nil,
                        Expect::Int(0),
                    ]),
                ),
                s(
                    &[b"SUBSCRIBE"],
                    Expect::Err("ERR wrong number of arguments for 'subscribe' command"),
                ),
                s(&[b"PING"], Expect::Pong),
            ],
        },
        // Pub/sub on one connection with no subscriber (ADR-0052 D5): the
        // replies a publisher sees. Delivery takes two connections, which
        // the corpus does not have; tools/pubsub_drill.sh covers it.
        Case {
            family: "pubsub",
            name: "pubsub: PUBLISH and PUBSUB with no subscriber",
            steps: vec![
                s(&[b"PUBLISH", b"{ps}ch", b"hello"], Expect::Int(0)),
                s(
                    &[b"PUBLISH", b"{ps}ch"],
                    Expect::Err("ERR wrong number of arguments for 'publish' command"),
                ),
                s(
                    &[b"PUBSUB", b"NUMSUB", b"{ps}ch", b"{ps}x"],
                    Expect::Arr(vec![
                        Expect::Str(b"{ps}ch"),
                        Expect::Int(0),
                        Expect::Str(b"{ps}x"),
                        Expect::Int(0),
                    ]),
                ),
                s(&[b"PUBSUB", b"numsub"], Expect::Arr(vec![])),
                s(&[b"PUBSUB", b"CHANNELS", b"{ps}*"], Expect::Arr(vec![])),
                s(
                    &[b"PUBSUB", b"SHARDNUMSUB", b"{ps}ch"],
                    Expect::Arr(vec![Expect::Str(b"{ps}ch"), Expect::Int(0)]),
                ),
                s(&[b"PUBSUB", b"SHARDCHANNELS"], Expect::Arr(vec![])),
                s(
                    &[b"PUBSUB", b"CHANNELS", b"a", b"b"],
                    Expect::Err(
                        "ERR unknown subcommand or wrong number of arguments for 'CHANNELS'. \
                         Try PUBSUB HELP.",
                    ),
                ),
                s(
                    &[b"PUBSUB", b"NUMPAT", b"x"],
                    Expect::Err("ERR wrong number of arguments for 'pubsub|numpat' command"),
                ),
                s(
                    &[b"PUBSUB", b"BOGUS"],
                    Expect::Err("ERR unknown subcommand 'BOGUS'. Try PUBSUB HELP."),
                ),
                s(
                    &[b"PUBSUB"],
                    Expect::Err("ERR wrong number of arguments for 'pubsub' command"),
                ),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"SET", b"{ps}k", b"v"], Expect::Simple("QUEUED")),
                s(&[b"PUBLISH", b"{ps}k", b"done"], Expect::Simple("QUEUED")),
                s(&[b"EXEC"], Expect::Arr(vec![Expect::Ok, Expect::Int(0)])),
                s(&[b"DEL", b"{ps}k"], Expect::Int(1)),
            ],
        },
        Case {
            family: "strings",
            name: "PSETEX sets a value with a TTL in milliseconds",
            steps: vec![
                s(&[b"PSETEX", b"px", b"100000", b"v"], Expect::Ok),
                s(&[b"GET", b"px"], Expect::Str(b"v")),
                s(&[b"PTTL", b"px"], Expect::IntRange(90_000, 100_000)),
                s(
                    &[b"PSETEX", b"px", b"0", b"v"],
                    Expect::Err("ERR invalid expire time in 'psetex' command"),
                ),
                s(
                    &[b"PSETEX", b"px", b"-5", b"v"],
                    Expect::Err("ERR invalid expire time in 'psetex' command"),
                ),
                s(
                    &[b"PSETEX", b"px", b"x", b"v"],
                    Expect::Err("ERR value is not an integer or out of range"),
                ),
                s(&[b"PSETEX", b"px", b"100"], Expect::AnyError),
            ],
        },
        Case {
            family: "keyspace",
            name: "TOUCH counts the keys that exist, as EXISTS does",
            steps: vec![
                s(&[b"SET", b"t1", b"a"], Expect::Ok),
                s(&[b"SET", b"t2", b"b"], Expect::Ok),
                s(&[b"TOUCH", b"t1", b"t2", b"t3", b"t1"], Expect::Int(3)),
                s(&[b"TOUCH", b"t3"], Expect::Int(0)),
                s(&[b"TOUCH"], Expect::AnyError),
            ],
        },
        Case {
            family: "lists",
            name: "LPUSHX and RPUSHX push only onto a list that exists",
            steps: vec![
                s(&[b"LPUSHX", b"lx", b"a"], Expect::Int(0)),
                s(&[b"EXISTS", b"lx"], Expect::Int(0)),
                s(&[b"RPUSH", b"lx", b"a"], Expect::Int(1)),
                s(&[b"LPUSHX", b"lx", b"b", b"c"], Expect::Int(3)),
                s(&[b"RPUSHX", b"lx", b"d"], Expect::Int(4)),
                s(
                    &[b"LRANGE", b"lx", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"c"),
                        Expect::Str(b"b"),
                        Expect::Str(b"a"),
                        Expect::Str(b"d"),
                    ]),
                ),
                s(&[b"SET", b"lxs", b"v"], Expect::Ok),
                s(&[b"RPUSHX", b"lxs", b"a"], Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                s(&[b"LPUSHX", b"lx"], Expect::AnyError),
            ],
        },
        Case {
            family: "sets",
            name: "SMOVE moves a member, in upstream's order of checks",
            steps: vec![
                s(&[b"SADD", b"{sm}a", b"x", b"y"], Expect::Int(2)),
                s(&[b"SADD", b"{sm}b", b"z"], Expect::Int(1)),
                s(&[b"SMOVE", b"{sm}a", b"{sm}b", b"x"], Expect::Int(1)),
                s(&[b"SMOVE", b"{sm}a", b"{sm}b", b"nope"], Expect::Int(0)),
                s(&[b"SMEMBERS", b"{sm}a"], Expect::UnorderedStrs(vec![b"y"])),
                s(&[b"SMEMBERS", b"{sm}b"], Expect::UnorderedStrs(vec![b"x", b"z"])),
                // One key for both is a membership test.
                s(&[b"SMOVE", b"{sm}a", b"{sm}a", b"y"], Expect::Int(1)),
                s(&[b"SMOVE", b"{sm}a", b"{sm}a", b"nope"], Expect::Int(0)),
                // A source emptied by the move is gone.
                s(&[b"SMOVE", b"{sm}a", b"{sm}c", b"y"], Expect::Int(1)),
                s(&[b"EXISTS", b"{sm}a"], Expect::Int(0)),
                s(&[b"SMEMBERS", b"{sm}c"], Expect::UnorderedStrs(vec![b"y"])),
                s(&[b"SET", b"{sm}s", b"v"], Expect::Ok),
                s(&[b"SMOVE", b"{sm}b", b"{sm}s", b"nope"], Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                // A missing source answers 0 before any type is checked.
                s(&[b"SMOVE", b"{sm}none", b"{sm}s", b"z"], Expect::Int(0)),
            ],
        },
        Case {
            family: "hashes",
            name: "HRANDFIELD picks fields, in upstream's words",
            steps: vec![
                s(&[b"HSET", b"hr", b"f1", b"v1", b"f2", b"v2", b"f3", b"v3"], Expect::Int(3)),
                s(&[b"HRANDFIELD", b"hr", b"5"], Expect::UnorderedStrs(vec![b"f1", b"f2", b"f3"])),
                s(
                    &[b"HRANDFIELD", b"hr", b"5", b"WITHVALUES"],
                    Expect::UnorderedPairs(vec![(b"f1", b"v1"), (b"f2", b"v2"), (b"f3", b"v3")]),
                ),
                s(&[b"HRANDFIELD", b"hr", b"0"], Expect::Arr(vec![])),
                s(&[b"HRANDFIELD", b"hr", b"-2"], Expect::AnyArray),
                s(&[b"HRANDFIELD", b"hrnone"], Expect::Nil),
                s(&[b"HRANDFIELD", b"hrnone", b"3"], Expect::Arr(vec![])),
                s(&[b"HRANDFIELD", b"hr", b"2", b"WAT"], Expect::Err("ERR syntax error")),
                s(
                    &[b"HRANDFIELD", b"hr", b"x"],
                    Expect::Err("ERR value is not an integer or out of range"),
                ),
                s(
                    &[b"HRANDFIELD", b"hr", b"-4611686018427387905", b"WITHVALUES"],
                    Expect::Err("ERR value is out of range"),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "ZRANDMEMBER picks members, in upstream's words",
            steps: vec![
                s(&[b"ZADD", b"zr", b"1", b"a", b"2.5", b"b"], Expect::Int(2)),
                s(&[b"ZRANDMEMBER", b"zr", b"5"], Expect::UnorderedStrs(vec![b"a", b"b"])),
                s(
                    &[b"ZRANDMEMBER", b"zr", b"5", b"WITHSCORES"],
                    Expect::UnorderedPairs(vec![(b"a", b"1"), (b"b", b"2.5")]),
                ),
                s(&[b"ZRANDMEMBER", b"zr", b"0"], Expect::Arr(vec![])),
                s(&[b"ZRANDMEMBER", b"zrnone"], Expect::Nil),
                s(&[b"ZRANDMEMBER", b"zrnone", b"2"], Expect::Arr(vec![])),
                s(&[b"ZRANDMEMBER", b"zr", b"2", b"WITHVALUES"], Expect::Err("ERR syntax error")),
                s(&[b"SET", b"zrs", b"v"], Expect::Ok),
                s(&[b"ZRANDMEMBER", b"zrs", b"1"], Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value")),
            ],
        },
        Case {
            family: "keyspace",
            name: "del returns removal count",
            steps: vec![
                s(&[b"SET", b"d1", b"x"], Expect::Ok),
                s(&[b"SET", b"d2", b"y"], Expect::Ok),
                s(&[b"DEL", b"d1", b"d2", b"d3"], Expect::Int(2)),
                s(&[b"GET", b"d1"], Expect::Nil),
            ],
        },
        Case {
            family: "keyspace",
            name: "del counts a key once",
            steps: vec![
                s(&[b"SET", b"d4", b"x"], Expect::Ok),
                s(&[b"DEL", b"d4", b"d4"], Expect::Int(1)),
            ],
        },
        Case {
            family: "keyspace",
            name: "exists counts duplicates",
            steps: vec![
                s(&[b"SET", b"e1", b"x"], Expect::Ok),
                s(&[b"EXISTS", b"e1", b"e1", b"nope"], Expect::Int(2)),
                s(&[b"EXISTS", b"nope"], Expect::Int(0)),
            ],
        },
        Case {
            family: "protocol",
            name: "arity errors",
            steps: vec![
                s(&[b"GET"], Expect::AnyError),
                s(&[b"SET", b"only-key"], Expect::AnyError),
                s(&[b"DEL"], Expect::AnyError),
            ],
        },
        Case {
            family: "protocol",
            name: "unknown command errors",
            steps: vec![s(&[b"FLINTNOSUCH", b"x"], Expect::AnyError)],
        },
        Case {
            family: "protocol",
            name: "command name is case-insensitive",
            steps: vec![
                s(&[b"set", b"c1", b"v"], Expect::Ok),
                s(&[b"gEt", b"c1"], Expect::Str(b"v")),
            ],
        },
        Case {
            family: "ttl",
            name: "expire ttl persist lifecycle",
            steps: vec![
                s(&[b"SET", b"t1", b"v"], Expect::Ok),
                s(&[b"TTL", b"t1"], Expect::Int(-1)),
                s(&[b"EXPIRE", b"t1", b"100"], Expect::Int(1)),
                s(&[b"TTL", b"t1"], Expect::IntRange(95, 100)),
                s(&[b"PTTL", b"t1"], Expect::IntRange(95_000, 100_000)),
                s(&[b"PERSIST", b"t1"], Expect::Int(1)),
                s(&[b"TTL", b"t1"], Expect::Int(-1)),
                s(&[b"PERSIST", b"t1"], Expect::Int(0)),
            ],
        },
        // BUG-0214: TTL rounds to the nearest second. It rounded up here,
        // so a key with 1.4 s left answered 2.
        Case {
            family: "ttl",
            name: "ttl rounds to the nearest second",
            steps: vec![
                s(&[b"SET", b"tr1", b"v", b"PX", b"1400"], Expect::Ok),
                s(&[b"TTL", b"tr1"], Expect::Int(1)),
                s(&[b"SET", b"tr2", b"v", b"PX", b"1700"], Expect::Ok),
                s(&[b"TTL", b"tr2"], Expect::Int(2)),
            ],
        },
        // BUG-0213: an instant past the 64-bit range is refused, and leaves
        // the key alone. EXPIRE with i64::MIN deleted it here.
        Case {
            family: "ttl",
            name: "expire refuses an instant out of range",
            steps: vec![
                s(&[b"SET", b"eo", b"v"], Expect::Ok),
                s(
                    &[b"EXPIRE", b"eo", b"9223372036854775807"],
                    Expect::Err("ERR invalid expire time in 'expire' command"),
                ),
                s(
                    &[b"EXPIRE", b"eo", b"-9223372036854775808"],
                    Expect::Err("ERR invalid expire time in 'expire' command"),
                ),
                s(
                    &[b"PEXPIRE", b"eo", b"9223372036854775807"],
                    Expect::Err("ERR invalid expire time in 'pexpire' command"),
                ),
                s(
                    &[b"EXPIREAT", b"eo", b"9223372036854775807"],
                    Expect::Err("ERR invalid expire time in 'expireat' command"),
                ),
                s(&[b"TTL", b"eo"], Expect::Int(-1)),
                s(&[b"PEXPIREAT", b"eo", b"9223372036854775807"], Expect::Int(1)),
                s(&[b"EXPIRE", b"eo", b"-100"], Expect::Int(1)),
                s(&[b"EXISTS", b"eo"], Expect::Int(0)),
            ],
        },
        Case {
            family: "ttl",
            name: "expire conditions nx xx gt lt (redis 7)",
            // BUG-0185: every option was an arity error.
            steps: vec![
                s(&[b"SET", b"ec", b"v"], Expect::Ok),
                s(&[b"EXPIRE", b"ec", b"100", b"XX"], Expect::Int(0)),
                s(&[b"EXPIRE", b"ec", b"100", b"GT"], Expect::Int(0)),
                s(&[b"TTL", b"ec"], Expect::Int(-1)),
                s(&[b"EXPIRE", b"ec", b"100", b"NX"], Expect::Int(1)),
                s(&[b"EXPIRE", b"ec", b"200", b"NX"], Expect::Int(0)),
                s(&[b"EXPIRE", b"ec", b"50", b"GT"], Expect::Int(0)),
                s(&[b"TTL", b"ec"], Expect::IntRange(95, 100)),
                s(&[b"EXPIRE", b"ec", b"200", b"gt"], Expect::Int(1)),
                s(&[b"TTL", b"ec"], Expect::IntRange(195, 200)),
                s(&[b"EXPIRE", b"ec", b"300", b"LT"], Expect::Int(0)),
                s(&[b"PEXPIRE", b"ec", b"50000", b"XX", b"LT"], Expect::Int(1)),
                s(&[b"TTL", b"ec"], Expect::IntRange(45, 50)),
                s(&[b"SET", b"ep", b"v"], Expect::Ok),
                s(&[b"EXPIRE", b"ep", b"100", b"LT"], Expect::Int(1)),
                s(&[b"SET", b"ea", b"v"], Expect::Ok),
                s(&[b"EXPIREAT", b"ea", b"9999999999", b"NX"], Expect::Int(1)),
                s(&[b"EXPIREAT", b"ea", b"9999999998", b"GT"], Expect::Int(0)),
                s(
                    &[b"PEXPIREAT", b"ea", b"9999999999500", b"GT"],
                    Expect::Int(1),
                ),
                s(&[b"PEXPIRETIME", b"ea"], Expect::Int(9_999_999_999_500)),
                s(&[b"EXPIRE", b"enone", b"10", b"NX"], Expect::Int(0)),
                s(&[b"SET", b"ed", b"v"], Expect::Ok),
                s(&[b"EXPIRE", b"ed", b"-1", b"LT"], Expect::Int(1)),
                s(&[b"EXISTS", b"ed"], Expect::Int(0)),
                s(&[b"EXPIRE", b"ec", b"10", b"NX", b"XX"], Expect::AnyError),
                s(&[b"EXPIRE", b"ec", b"10", b"GT", b"LT"], Expect::AnyError),
                s(&[b"EXPIRE", b"ec", b"10", b"BOGUS"], Expect::AnyError),
                s(&[b"TTL", b"ec"], Expect::IntRange(45, 50)),
            ],
        },
        Case {
            family: "ttl",
            name: "missing keys",
            steps: vec![
                s(&[b"TTL", b"nope"], Expect::Int(-2)),
                s(&[b"PTTL", b"nope"], Expect::Int(-2)),
                s(&[b"EXPIRE", b"nope", b"10"], Expect::Int(0)),
                s(&[b"PERSIST", b"nope"], Expect::Int(0)),
            ],
        },
        Case {
            family: "ttl",
            name: "set with ex and px",
            steps: vec![
                s(&[b"SET", b"t2", b"v", b"EX", b"100"], Expect::Ok),
                s(&[b"TTL", b"t2"], Expect::IntRange(95, 100)),
                s(&[b"SET", b"t3", b"v", b"PX", b"100000"], Expect::Ok),
                s(&[b"TTL", b"t3"], Expect::IntRange(95, 100)),
                s(&[b"SET", b"t4", b"v", b"EX", b"0"], Expect::AnyError),
                s(&[b"SET", b"t4", b"v", b"EX", b"abc"], Expect::AnyError),
            ],
        },
        Case {
            family: "ttl",
            name: "plain set clears ttl, keepttl keeps it",
            steps: vec![
                s(&[b"SET", b"t5", b"v", b"EX", b"100"], Expect::Ok),
                s(&[b"SET", b"t5", b"v2"], Expect::Ok),
                s(&[b"TTL", b"t5"], Expect::Int(-1)),
                s(&[b"SET", b"t5", b"v3", b"EX", b"100"], Expect::Ok),
                s(&[b"SET", b"t5", b"v4", b"KEEPTTL"], Expect::Ok),
                s(&[b"TTL", b"t5"], Expect::IntRange(1, 100)),
                s(&[b"GET", b"t5"], Expect::Str(b"v4")),
            ],
        },
        Case {
            family: "ttl",
            name: "keys really expire",
            steps: vec![
                sd(&[b"SET", b"t6", b"v", b"PX", b"60"], Expect::Ok, 140),
                s(&[b"GET", b"t6"], Expect::Nil),
                s(&[b"TTL", b"t6"], Expect::Int(-2)),
                s(&[b"EXISTS", b"t6"], Expect::Int(0)),
            ],
        },
        Case {
            family: "ttl",
            name: "setex and setnx",
            steps: vec![
                s(&[b"SETEX", b"t7", b"100", b"v"], Expect::Ok),
                s(&[b"TTL", b"t7"], Expect::IntRange(95, 100)),
                s(&[b"SETEX", b"t8", b"0", b"v"], Expect::AnyError),
                s(&[b"SETNX", b"t9", b"a"], Expect::Int(1)),
                s(&[b"SETNX", b"t9", b"b"], Expect::Int(0)),
                s(&[b"GET", b"t9"], Expect::Str(b"a")),
            ],
        },
        Case {
            family: "strings",
            name: "incr decr family",
            steps: vec![
                s(&[b"INCR", b"c1"], Expect::Int(1)),
                s(&[b"INCR", b"c1"], Expect::Int(2)),
                s(&[b"INCRBY", b"c1", b"10"], Expect::Int(12)),
                s(&[b"DECR", b"c1"], Expect::Int(11)),
                s(&[b"DECRBY", b"c1", b"5"], Expect::Int(6)),
                s(&[b"INCRBY", b"c1", b"-2"], Expect::Int(4)),
            ],
        },
        Case {
            family: "strings",
            name: "incr on non-integer errors",
            steps: vec![
                s(&[b"SET", b"c2", b"abc"], Expect::Ok),
                s(&[b"INCR", b"c2"], Expect::AnyError),
                s(&[b"SET", b"c3", b"9223372036854775807"], Expect::Ok),
                s(&[b"INCR", b"c3"], Expect::AnyError),
                s(&[b"INCRBY", b"c4", b"notanum"], Expect::AnyError),
            ],
        },
        Case {
            family: "strings",
            name: "incr preserves ttl",
            steps: vec![
                s(&[b"SET", b"c5", b"5", b"EX", b"100"], Expect::Ok),
                s(&[b"INCR", b"c5"], Expect::Int(6)),
                s(&[b"TTL", b"c5"], Expect::IntRange(1, 100)),
            ],
        },
        Case {
            family: "strings",
            name: "append and strlen",
            steps: vec![
                s(&[b"APPEND", b"a1", b"he"], Expect::Int(2)),
                s(&[b"APPEND", b"a1", b"llo"], Expect::Int(5)),
                s(&[b"GET", b"a1"], Expect::Str(b"hello")),
                s(&[b"STRLEN", b"a1"], Expect::Int(5)),
                s(&[b"STRLEN", b"missing"], Expect::Int(0)),
            ],
        },
        Case {
            family: "keyspace",
            name: "type command",
            steps: vec![
                s(&[b"SET", b"y1", b"v"], Expect::Ok),
                s(&[b"TYPE", b"y1"], Expect::Simple("string")),
                s(&[b"TYPE", b"missing"], Expect::Simple("none")),
            ],
        },
        Case {
            family: "keyspace",
            name: "del removes ttl state too",
            steps: vec![
                s(&[b"SET", b"y2", b"v", b"EX", b"100"], Expect::Ok),
                s(&[b"DEL", b"y2"], Expect::Int(1)),
                s(&[b"SET", b"y2", b"v2"], Expect::Ok),
                s(&[b"TTL", b"y2"], Expect::Int(-1)),
            ],
        },
        Case {
            family: "ttl",
            name: "expire with past time deletes",
            steps: vec![
                s(&[b"SET", b"y3", b"v"], Expect::Ok),
                s(&[b"EXPIRE", b"y3", b"-1"], Expect::Int(1)),
                s(&[b"EXISTS", b"y3"], Expect::Int(0)),
                s(&[b"GET", b"y3"], Expect::Nil),
            ],
        },
        Case {
            family: "hashes",
            name: "hset counts new fields, hget reads",
            steps: vec![
                s(&[b"HSET", b"h1", b"a", b"1", b"b", b"2"], Expect::Int(2)),
                s(&[b"HSET", b"h1", b"a", b"9", b"c", b"3"], Expect::Int(1)),
                s(&[b"HGET", b"h1", b"a"], Expect::Str(b"9")),
                s(&[b"HGET", b"h1", b"nope"], Expect::Nil),
                s(&[b"HGET", b"nosuch", b"f"], Expect::Nil),
                s(&[b"HLEN", b"h1"], Expect::Int(3)),
                s(&[b"HEXISTS", b"h1", b"b"], Expect::Int(1)),
                s(&[b"HEXISTS", b"h1", b"zz"], Expect::Int(0)),
            ],
        },
        Case {
            family: "scripting",
            name: "the recognised lock and incr scripts do what their Lua does",
            steps: vec![
                s(&[b"SET", b"lk", b"tok", b"PX", b"10000"], Expect::Ok),
                s(
                    &[b"EVAL", LOCK_RELEASE, b"1", b"lk", b"other"],
                    Expect::Int(0),
                ),
                s(
                    &[b"EVAL", LOCK_EXTEND, b"1", b"lk", b"tok", b"5000", b"0"],
                    Expect::Int(1),
                ),
                s(&[b"PTTL", b"lk"], Expect::IntRange(10_001, 15_000)),
                s(
                    &[b"EVAL", LOCK_EXTEND, b"1", b"lk", b"tok", b"3000", b"1"],
                    Expect::Int(1),
                ),
                s(&[b"PTTL", b"lk"], Expect::IntRange(1, 3_000)),
                s(
                    &[b"EVAL", LOCK_REACQUIRE, b"1", b"lk", b"tok", b"8000"],
                    Expect::Int(1),
                ),
                s(
                    &[b"EVAL", LOCK_REACQUIRE, b"1", b"lk", b"other", b"8000"],
                    Expect::Int(0),
                ),
                s(
                    &[b"EVAL", LOCK_RELEASE, b"1", b"lk", b"tok"],
                    Expect::Int(1),
                ),
                s(&[b"EXISTS", b"lk"], Expect::Int(0)),
                s(&[b"SET", b"lk2", b"tok"], Expect::Ok),
                s(
                    &[b"EVAL", LOCK_EXTEND, b"1", b"lk2", b"tok", b"5000", b"0"],
                    Expect::Int(0),
                ),
                s(
                    &[b"EVAL", DJANGO_INCR_CHECKED, b"1", b"dn", b"1"],
                    Expect::Nil,
                ),
                s(&[b"SET", b"dn", b"5"], Expect::Ok),
                s(
                    &[b"EVAL", DJANGO_INCR_CHECKED, b"1", b"dn", b"2"],
                    Expect::Int(7),
                ),
                s(&[b"EVAL", DJANGO_INCR, b"1", b"dm", b"3"], Expect::Int(3)),
                s(
                    &[b"SCRIPT", b"LOAD", LOCK_RELEASE],
                    Expect::Str(b"c3f8721cbb97f72bc19e972846bd7aaf91901658"),
                ),
                s(
                    &[
                        b"EVALSHA",
                        b"c3f8721cbb97f72bc19e972846bd7aaf91901658",
                        b"1",
                        b"lk3",
                        b"tok",
                    ],
                    Expect::Int(0),
                ),
            ],
        },
        Case {
            family: "scripting",
            name: "the recognised lock-library scripts do what their Lua does",
            steps: vec![
                // node redlock: acquire when absent, extend and release with
                // the token.
                s(
                    &[b"EVAL", REDLOCK_ACQUIRE, b"1", b"rl", b"tok", b"10000"],
                    Expect::Int(1),
                ),
                s(&[b"PTTL", b"rl"], Expect::IntRange(9_000, 10_000)),
                s(
                    &[b"EVAL", REDLOCK_ACQUIRE, b"1", b"rl", b"x", b"10000"],
                    Expect::Int(0),
                ),
                s(
                    &[b"EVAL", REDLOCK_EXTEND, b"1", b"rl", b"x", b"30000"],
                    Expect::Int(0),
                ),
                s(
                    &[b"EVAL", REDLOCK_EXTEND, b"1", b"rl", b"tok", b"30000"],
                    Expect::Int(1),
                ),
                s(&[b"PTTL", b"rl"], Expect::IntRange(20_001, 30_000)),
                s(
                    &[b"EVAL", REDLOCK_RELEASE, b"1", b"rl", b"x"],
                    Expect::Int(0),
                ),
                s(
                    &[b"EVAL", REDLOCK_RELEASE, b"1", b"rl", b"tok"],
                    Expect::Int(1),
                ),
                s(&[b"EXISTS", b"rl"], Expect::Int(0)),
                s(
                    &[b"EVAL", REDLOCK5_ACQUIRE, b"1", b"rl", b"tok", b"10000"],
                    Expect::Int(1),
                ),
                s(
                    &[b"EVAL", REDLOCK5_ACQUIRE, b"1", b"rl", b"x", b"10000"],
                    Expect::Int(0),
                ),
                // redsync: PEXPIRE's and DEL's replies, and -1 when gone.
                s(&[b"SET", b"rs", b"tok", b"PX", b"8000"], Expect::Ok),
                s(
                    &[b"EVAL", REDSYNC_EXTEND, b"1", b"rs", b"x", b"30000"],
                    Expect::Int(0),
                ),
                s(
                    &[b"EVAL", REDSYNC_EXTEND, b"1", b"rs", b"tok", b"30000"],
                    Expect::Int(1),
                ),
                s(
                    &[b"EVAL", REDSYNC_RELEASE, b"1", b"rs", b"x"],
                    Expect::Int(0),
                ),
                s(
                    &[b"EVAL", REDSYNC_RELEASE, b"1", b"rs", b"tok"],
                    Expect::Int(1),
                ),
                s(
                    &[b"EVAL", REDSYNC_RELEASE, b"1", b"rs", b"tok"],
                    Expect::Int(-1),
                ),
                s(
                    &[b"EVAL", REDSYNC_RELEASE_BEFORE_4_12, b"1", b"rs", b"tok"],
                    Expect::Int(0),
                ),
                s(
                    &[b"EVAL", REDSYNC_EXTEND_SETNX, b"1", b"rs", b"tok", b"30000"],
                    Expect::Int(1),
                ),
                s(&[b"PTTL", b"rs"], Expect::IntRange(20_001, 30_000)),
                s(
                    &[b"EVAL", REDSYNC_EXTEND_SETNX, b"1", b"rs", b"x", b"30000"],
                    Expect::Int(0),
                ),
                s(
                    &[b"EVAL", REDSYNC_RELEASE_BEFORE_4_12, b"1", b"rs", b"tok"],
                    Expect::Int(1),
                ),
                // Ruby redlock: OK or nil, and info's [value, pttl].
                s(
                    &[
                        b"EVAL",
                        REDLOCK_RB_LOCK,
                        b"1",
                        b"rb",
                        b"tok",
                        b"10000",
                        b"no",
                    ],
                    Expect::Nil,
                ),
                s(
                    &[
                        b"EVAL",
                        REDLOCK_RB_LOCK,
                        b"1",
                        b"rb",
                        b"tok",
                        b"10000",
                        b"yes",
                    ],
                    Expect::Ok,
                ),
                s(
                    &[
                        b"EVAL",
                        REDLOCK_RB_LOCK,
                        b"1",
                        b"rb",
                        b"x",
                        b"10000",
                        b"yes",
                    ],
                    Expect::Nil,
                ),
                s(
                    &[
                        b"EVAL",
                        REDLOCK_RB_LOCK,
                        b"1",
                        b"rb",
                        b"tok",
                        b"30000",
                        b"no",
                    ],
                    Expect::Ok,
                ),
                s(
                    &[b"EVAL", REDLOCK_RB_INFO, b"1", b"rb"],
                    Expect::Arr(vec![Expect::Str(b"tok"), Expect::IntRange(20_001, 30_000)]),
                ),
                s(
                    &[b"EVAL", REDLOCK_RB_UNLOCK, b"1", b"rb", b"x"],
                    Expect::Int(0),
                ),
                s(
                    &[b"EVAL", REDLOCK_RB_UNLOCK, b"1", b"rb", b"tok"],
                    Expect::Int(1),
                ),
                s(
                    &[b"EVAL", REDLOCK_RB_INFO, b"1", b"rb"],
                    Expect::Arr(vec![Expect::Nil, Expect::Int(-2)]),
                ),
            ],
        },
        Case {
            family: "scripting",
            name: "the recognised rate-limit-redis scripts do what their Lua does",
            steps: vec![
                s(
                    &[b"EVAL", RATE_LIMIT_REDIS_INCR, b"1", b"rlr", b"60000"],
                    Expect::Arr(vec![Expect::Int(1), Expect::Int(60000)]),
                ),
                s(&[b"PTTL", b"rlr"], Expect::IntRange(55_000, 60_000)),
                s(
                    &[b"EVAL", RATE_LIMIT_REDIS_INCR, b"1", b"rlr", b"60000"],
                    Expect::Arr(vec![Expect::Int(2), Expect::IntRange(55_000, 60_000)]),
                ),
                s(
                    &[b"EVAL", RATE_LIMIT_REDIS_GET, b"1", b"rlr"],
                    Expect::Arr(vec![Expect::Str(b"2"), Expect::IntRange(55_000, 60_000)]),
                ),
                s(
                    &[b"EVAL", RATE_LIMIT_REDIS_GET, b"1", b"rlr-none"],
                    Expect::Arr(vec![Expect::Nil, Expect::Int(-2)]),
                ),
                s(&[b"SET", b"rlr-bare", b"7"], Expect::Ok),
                s(
                    &[b"EVAL", RATE_LIMIT_REDIS_INCR, b"1", b"rlr-bare", b"1000"],
                    Expect::Arr(vec![Expect::Int(1), Expect::Int(1000)]),
                ),
                s(
                    &[b"EVAL", RATE_LIMIT_REDIS_INCR, b"1", b"rlr-new", b"1.5"],
                    Expect::AnyError,
                ),
                s(&[b"EXISTS", b"rlr-new"], Expect::Int(0)),
            ],
        },
        Case {
            family: "lua",
            name: "a script's return value converts as redis converts it",
            steps: vec![
                s(&[b"EVAL", "return 3.99".as_bytes(), b"0"], Expect::Int(3)),
                s(&[b"EVAL", "return -0.5".as_bytes(), b"0"], Expect::Int(0)),
                s(&[b"EVAL", "return {1,2,nil,4}".as_bytes(), b"0"], Expect::Arr(vec![Expect::Int(1), Expect::Int(2)])),
                s(&[b"EVAL", "return true".as_bytes(), b"0"], Expect::Int(1)),
                s(&[b"EVAL", "return false".as_bytes(), b"0"], Expect::Nil),
                s(&[b"EVAL", "return nil".as_bytes(), b"0"], Expect::Nil),
                s(&[b"EVAL", "return {ok='FINE'}".as_bytes(), b"0"], Expect::Simple("FINE")),
                s(&[b"EVAL", "return {err='E1 x'}".as_bytes(), b"0"], Expect::Err("E1 x")),
                s(&[b"EVAL", "return {1, 'two', {ok='x'}, {err='E y'}}".as_bytes(), b"0"], Expect::Arr(vec![Expect::Int(1), Expect::Str(b"two"), Expect::Simple("x"), Expect::Err("E y")])),
                s(&[b"EVAL", "return {1, {2, {3}}}".as_bytes(), b"0"], Expect::Arr(vec![Expect::Int(1), Expect::Arr(vec![Expect::Int(2), Expect::Arr(vec![Expect::Int(3)])])])),
                s(&[b"EVAL", "local t = {}; t[1] = 1; t[3] = 3; return t".as_bytes(), b"0"], Expect::Arr(vec![Expect::Int(1)])),
                s(&[b"EVAL", "return {n=1}".as_bytes(), b"0"], Expect::Arr(vec![])),
                s(&[b"EVAL", "return 7".as_bytes(), b"0"], Expect::Int(7)),
                s(&[b"EVAL", "return '7'".as_bytes(), b"0"], Expect::Str(b"7")),
                s(&[b"EVAL", "return #KEYS .. ' ' .. #ARGV".as_bytes(), b"2", b"{lc}a", b"{lc}b", b"c"], Expect::Str(b"2 1")),
                s(&[b"HSET", b"{lc}h", b"a", b"1"], Expect::Int(1)),
                s(&[b"EVAL", "return redis.call('hgetall', KEYS[1])".as_bytes(), b"1", b"{lc}h"], Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"1")])),
                s(&[b"EVAL", "return redis.call('set', KEYS[1], 'v')".as_bytes(), b"1", b"{lc}k"], Expect::Ok),
                s(&[b"EVAL", "return type(redis.call('set', KEYS[1], 'v'))".as_bytes(), b"1", b"{lc}k"], Expect::Str(b"table")),
                s(&[b"EVAL", "return type(redis.call('get', KEYS[1]))".as_bytes(), b"1", b"{lc}none"], Expect::Str(b"boolean")),
                s(&[b"EVAL", "return redis.call('zadd', KEYS[1], 1.5, 'm')".as_bytes(), b"1", b"{lc}z"], Expect::Int(1)),
                s(&[b"EVAL", "return redis.call('zrange', KEYS[1], 0, -1, 'withscores')".as_bytes(), b"1", b"{lc}z"], Expect::Arr(vec![Expect::Str(b"m"), Expect::Str(b"1.5")])),
                s(&[b"EVAL", "return redis.call('incrbyfloat', KEYS[1], '1.5')".as_bytes(), b"1", b"{lc}f"], Expect::Str(b"1.5")),
            ],
        },
        Case {
            family: "lua",
            name: "a script's errors carry valkey's text and the line they were raised on",
            steps: vec![
                s(&[b"SET", b"{le}s", b"abc"], Expect::Ok),
                s(&[b"EVAL", "return redis.call('incr', KEYS[1])".as_bytes(), b"1", b"{le}s"], Expect::Err("ERR value is not an integer or out of range script: on @user_script:1.")),
                s(&[b"EVAL", "\nlocal a = 1\nreturn redis.call('incr', KEYS[1])".as_bytes(), b"1", b"{le}s"], Expect::Err("ERR value is not an integer or out of range script: on @user_script:3.")),
                s(&[b"EVAL", "local function f() return redis.call('incr', KEYS[1]) end\nreturn f()".as_bytes(), b"1", b"{le}s"], Expect::Err("ERR value is not an integer or out of range script: on @user_script:1.")),
                s(&[b"EVAL", "error('boom')".as_bytes(), b"0"], Expect::Err("ERR user_script:1: boom script: on @user_script:1.")),
                s(&[b"EVAL", "error({err='TBL boom'})".as_bytes(), b"0"], Expect::Err("TBL boom script: on @user_script:1.")),
                s(&[b"EVAL", "error(nil)".as_bytes(), b"0"], Expect::Err("ERR nil script: on @user_script:1.")),
                s(&[b"EVAL", "error({1,2})".as_bytes(), b"0"], Expect::Err("ERR unknown error script: on @user_script:1.")),
                s(&[b"EVAL", "error({err=5})".as_bytes(), b"0"], Expect::Err("5 script: on @user_script:1.")),
                s(&[b"EVAL", "return redis.error_reply('MYERR x')".as_bytes(), b"0"], Expect::Err("MYERR x")),
                s(&[b"EVAL", "return redis.error_reply('no code')".as_bytes(), b"0"], Expect::Err("no code")),
                s(&[b"EVAL", "x = 1".as_bytes(), b"0"], Expect::Err("ERR user_script:1: Attempt to modify a readonly table script: on @user_script:1.")),
                s(&[b"EVAL", "return y".as_bytes(), b"0"], Expect::Err("ERR user_script:1: Script attempted to access nonexistent global variable 'y' script: on @user_script:1.")),
                s(&[b"EVAL", "return redis.call('nosuch')".as_bytes(), b"0"], Expect::Err("ERR Unknown command called from script script: on @user_script:1.")),
                s(&[b"EVAL", "return redis.call('get')".as_bytes(), b"0"], Expect::Err("ERR Wrong number of args calling command from script script: on @user_script:1.")),
                s(&[b"EVAL", "return redis.call()".as_bytes(), b"0"], Expect::Err("ERR Please specify at least one argument for this call script: on @user_script:1.")),
                s(&[b"EVAL", "return redis.call('get', {})".as_bytes(), b"0"], Expect::Err("ERR Command arguments must be strings or integers script: on @user_script:1.")),
                s(&[b"EVAL", "return redis.pcall('incr', KEYS[1])".as_bytes(), b"1", b"{le}s"], Expect::Err("ERR value is not an integer or out of range")),
                s(&[b"EVAL", "return (".as_bytes(), b"0"], Expect::Err("ERR Error compiling script (new function): user_script:1: unexpected symbol near '<eof>'")),
                s(&[b"EVAL", "local r = redis.pcall('incr', KEYS[1]) return type(r) .. ':' .. tostring(r.err)".as_bytes(), b"1", b"{le}s"], Expect::Str(b"table:ERR value is not an integer or out of range")),
                s(&[b"EVAL", "local ok, e = pcall(redis.call, 'incr', KEYS[1]) return type(e) .. ':' .. e".as_bytes(), b"1", b"{le}s"], Expect::Str(b"string:ERR value is not an integer or out of range")),
            ],
        },
        Case {
            family: "lua",
            name: "a lua number reaches redis.call as valkey spells it",
            steps: vec![
                s(&[b"EVAL", "redis.call('set', KEYS[1], 0.1) return redis.call('get', KEYS[1])".as_bytes(), b"1", b"{ln}n"], Expect::Str(b"0.1")),
                s(&[b"EVAL", "redis.call('set', KEYS[1], 1e21) return redis.call('get', KEYS[1])".as_bytes(), b"1", b"{ln}n"], Expect::Str(b"1e+21")),
                s(&[b"EVAL", "redis.call('set', KEYS[1], 60000) return redis.call('get', KEYS[1])".as_bytes(), b"1", b"{ln}n"], Expect::Str(b"60000")),
                s(&[b"EVAL", "redis.call('set', KEYS[1], 1/3) return redis.call('get', KEYS[1])".as_bytes(), b"1", b"{ln}n"], Expect::Str(b"0.3333333333333333")),
                s(&[b"EVAL", "redis.call('set', KEYS[1], 123456789.123456789) return redis.call('get', KEYS[1])".as_bytes(), b"1", b"{ln}n"], Expect::Str(b"1.2345678912345679e+8")),
                s(&[b"EVAL", "redis.call('set', KEYS[1], 1e-7) return redis.call('get', KEYS[1])".as_bytes(), b"1", b"{ln}n"], Expect::Str(b"1e-7")),
                s(&[b"EVAL", "redis.call('set', KEYS[1], -2.5e300) return redis.call('get', KEYS[1])".as_bytes(), b"1", b"{ln}n"], Expect::Str(b"-2.5e+300")),
                s(&[b"EVAL", "redis.call('set', KEYS[1], 1e17) return redis.call('get', KEYS[1])".as_bytes(), b"1", b"{ln}n"], Expect::Str(b"100000000000000000")),
                s(&[b"EVAL", "redis.call('set', KEYS[1], 5e18) return redis.call('get', KEYS[1])".as_bytes(), b"1", b"{ln}n"], Expect::Str(b"5e+18")),
                s(&[b"EVAL", "redis.call('set', KEYS[1], -0.0) return redis.call('get', KEYS[1])".as_bytes(), b"1", b"{ln}n"], Expect::Str(b"0")),
                s(&[b"EVAL", "redis.call('set', KEYS[1], 0.000123) return redis.call('get', KEYS[1])".as_bytes(), b"1", b"{ln}n"], Expect::Str(b"0.000123")),
                s(&[b"EVAL", "redis.call('set', KEYS[1], 1.5) return redis.call('get', KEYS[1])".as_bytes(), b"1", b"{ln}n"], Expect::Str(b"1.5")),
                s(&[b"EVAL", "return tostring(1/3)".as_bytes(), b"0"], Expect::Str(b"0.33333333333333")),
            ],
        },
        Case {
            family: "lua",
            name: "redis helpers and the script cache",
            steps: vec![
                s(&[b"EVAL", "return redis.sha1hex('abc')".as_bytes(), b"0"], Expect::Str(b"a9993e364706816aba3e25717850c26c9cd0d89d")),
                s(&[b"EVAL", "return redis.status_reply('PONG')".as_bytes(), b"0"], Expect::Simple("PONG")),
                s(&[b"EVAL", "return redis.log(redis.LOG_WARNING, 'x')".as_bytes(), b"0"], Expect::Nil),
                s(&[b"EVAL", "return redis.REPL_ALL".as_bytes(), b"0"], Expect::Int(3)),
                s(&[b"EVAL", "return redis.replicate_commands()".as_bytes(), b"0"], Expect::Int(1)),
                s(&[b"EVAL", "return redis.setresp(2)".as_bytes(), b"0"], Expect::Nil),
                s(&[b"EVAL", "return redis.call('ping')".as_bytes(), b"0"], Expect::Simple("PONG")),
                s(&[b"EVAL", "return #redis.call('time')".as_bytes(), b"0"], Expect::Int(2)),
                s(&[b"SCRIPT", b"FLUSH"], Expect::Ok),
                s(&[b"SCRIPT", b"LOAD", b"return 1"], Expect::Str(b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db")),
                s(&[b"SCRIPT", b"EXISTS", b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db", b"0000000000000000000000000000000000000000"], Expect::Arr(vec![Expect::Int(1), Expect::Int(0)])),
                s(&[b"EVALSHA", b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db", b"0"], Expect::Int(1)),
                s(&[b"EVALSHA", b"E0E1F9FABFC9D4800C877A703B823AC0578FF8DB", b"1", b"{lh}a"], Expect::Int(1)),
                s(&[b"EVAL", b"return 2", b"1", b"{lh}b"], Expect::Int(2)),
                s(&[b"EVALSHA", b"7f923f79fe76194c868d7e1d0820de36700eb649", b"1", b"{lh}c"], Expect::Int(2)),
                s(&[b"SCRIPT", b"FLUSH", b"ASYNC"], Expect::Ok),
                s(&[b"EVALSHA", b"e0e1f9fabfc9d4800c877a703b823ac0578ff8db", b"0"], Expect::Err("NOSCRIPT No matching script.")),
                s(&[b"SCRIPT", b"LOAD", b"return ("], Expect::Err("ERR Error compiling script (new function): user_script:1: unexpected symbol near '<eof>'")),
                s(&[b"SCRIPT", b"KILL"], Expect::Err("NOTBUSY No scripts in execution right now.")),
                s(&[b"TIME"], Expect::Arr(vec![Expect::AnyBulk, Expect::AnyBulk])),
            ],
        },
        Case {
            family: "scripting",
            name: "the rate limiters' scripts do what their lua does",
            steps: vec![
                s(&[b"EVAL", LIMITS_FIXED, b"1", b"{rl}fixed", b"60", b"1"], Expect::Int(1)),
                s(&[b"EVAL", LIMITS_FIXED, b"1", b"{rl}fixed", b"60", b"1"], Expect::Int(2)),
                s(&[b"TTL", b"{rl}fixed"], Expect::IntRange(55, 60)),
                s(&[b"EVAL", LIMITS_MOVING, b"1", b"{rl}mv", b"1000", b"2", b"60", b"1"], Expect::Int(1)),
                s(&[b"EVAL", LIMITS_MOVING, b"1", b"{rl}mv", b"1001", b"2", b"60", b"1"], Expect::Int(1)),
                s(&[b"EVAL", LIMITS_MOVING, b"1", b"{rl}mv", b"1002", b"2", b"60", b"1"], Expect::Nil),
                s(&[b"LRANGE", b"{rl}mv", b"0", b"-1"], Expect::Arr(vec![Expect::Str(b"1001"), Expect::Str(b"1000")])),
                s(&[b"EVAL", LIMITS_MOVING_STATS, b"1", b"{rl}mv", b"900", b"2"], Expect::Arr(vec![Expect::Str(b"1000"), Expect::Int(2)])),
                s(&[b"EVAL", LIMITS_SLIDING, b"2", b"{rl}sl", b"{rl}sl/-1", b"3", b"60", b"1"], Expect::Int(1)),
                s(&[b"EVAL", LIMITS_SLIDING, b"2", b"{rl}sl", b"{rl}sl/-1", b"3", b"60", b"1"], Expect::Int(1)),
                s(&[b"EVAL", LIMITS_SLIDING, b"2", b"{rl}sl", b"{rl}sl/-1", b"3", b"60", b"1"], Expect::Int(1)),
                s(&[b"EVAL", LIMITS_SLIDING, b"2", b"{rl}sl", b"{rl}sl/-1", b"3", b"60", b"1"], Expect::Nil),
                s(&[b"EVAL", LIMITS_SLIDING_STATS, b"2", b"{rl}sl", b"{rl}sl/-1", b"60"], Expect::Arr(vec![Expect::Nil, Expect::Int(-2), Expect::Str(b"3"), Expect::IntRange(110_000, 120_000)])),
                s(&[b"EVAL", RATE_LIMITER_FLEXIBLE, b"1", b"{rl}f", b"1", b"60"], Expect::Arr(vec![Expect::Int(1), Expect::IntRange(55_000, 60_000)])),
                s(&[b"EVAL", RATE_LIMITER_FLEXIBLE, b"1", b"{rl}f", b"1", b"60"], Expect::Arr(vec![Expect::Int(2), Expect::IntRange(55_000, 60_000)])),
                s(&[b"EVAL", REDIS_RATE_ALLOW, b"1", b"{rl}g", b"3", b"3", b"60", b"1"], Expect::Arr(vec![Expect::Int(1), Expect::IntRange(0, 2), Expect::Str(b"-1"), Expect::AnyBulk])),
                s(&[b"EVAL", REDIS_RATE_ALLOW, b"1", b"{rl}g", b"3", b"3", b"60", b"1"], Expect::Arr(vec![Expect::Int(1), Expect::IntRange(0, 2), Expect::Str(b"-1"), Expect::AnyBulk])),
                s(&[b"EVAL", REDIS_RATE_ALLOW, b"1", b"{rl}g", b"3", b"3", b"60", b"1"], Expect::Arr(vec![Expect::Int(1), Expect::IntRange(0, 2), Expect::Str(b"-1"), Expect::AnyBulk])),
                s(&[b"EVAL", REDIS_RATE_ALLOW, b"1", b"{rl}g", b"3", b"3", b"60", b"1"], Expect::Arr(vec![Expect::Int(0), Expect::Int(0), Expect::AnyBulk, Expect::AnyBulk])),
                s(&[b"EVAL", REDIS_RATE_ALLOW_AT_MOST, b"1", b"{rl}g2", b"3", b"3", b"60", b"2"], Expect::Arr(vec![Expect::Int(2), Expect::IntRange(0, 1), Expect::Str(b"-1"), Expect::AnyBulk])),
                s(&[b"EVAL", RATE_LIMIT_REDIS_5_INCR, b"1", b"{rl}r5", b"0", b"60000"], Expect::Arr(vec![Expect::Int(1), Expect::Int(60000)])),
                s(&[b"EVAL", RATE_LIMIT_REDIS_5_INCR, b"1", b"{rl}r5", b"1", b"60000"], Expect::Arr(vec![Expect::Int(2), Expect::Int(60000)])),
            ],
        },
        // BUG-0233: a float that is negative zero after 17 places is `0`.
        Case {
            family: "strings",
            name: "incrbyfloat writes a negative zero as 0",
            steps: vec![
                s(&[b"INCRBYFLOAT", b"nz", b"-0.000000000000000001"], Expect::Str(b"0")),
                s(&[b"GET", b"nz"], Expect::Str(b"0")),
                s(&[b"HSET", b"nzh", b"f", b"-0"], Expect::Int(1)),
                s(&[b"HINCRBYFLOAT", b"nzh", b"f", b"-0"], Expect::Str(b"0")),
                s(&[b"HGET", b"nzh", b"f"], Expect::Str(b"0")),
            ],
        },
        Case {
            family: "scripting",
            name: "a script returns redis 7's typed replies",
            // BUG-0232: each answered an empty array. A double is a bulk
            // string under RESP2 and a map a flat array; a `double` that is
            // not a number leaves an ordinary table.
            steps: vec![
                s(&[b"EVAL", b"return {double=1.5}", b"0"], Expect::Str(b"1.5")),
                s(&[b"EVAL", b"return {double=3}", b"0"], Expect::Str(b"3")),
                s(&[b"EVAL", b"return {1, {double=2.5}}", b"0"], Expect::Arr(vec![Expect::Int(1), Expect::Str(b"2.5")])),
                s(&[b"EVAL", b"return {double='2.5'}", b"0"], Expect::Arr(vec![])),
                s(&[b"EVAL", b"return {map={a='x'}}", b"0"], Expect::UnorderedPairs(vec![(b"a", b"x")])),
                s(&[b"EVAL", b"return {set={a=true}, 'z'}", b"0"], Expect::UnorderedStrs(vec![b"a"])),
                s(&[b"EVAL", b"return {ok='fine', double=2}", b"0"], Expect::Simple("fine")),
            ],
        },
        Case {
            family: "scripting",
            name: "an empty error reply and the script verbs' refusals, in upstream's words",
            // BUG-0225.
            steps: vec![
                s(&[b"EVAL", b"return redis.error_reply('')", b"0"], Expect::Err("ERR ")),
                s(
                    &[b"EVAL", b"return redis.sha1hex()", b"0"],
                    Expect::Err("ERR wrong number of arguments script: on @user_script:1."),
                ),
                s(
                    &[b"EVAL", b"return redis.sha1hex('abc')", b"0"],
                    Expect::Str(b"a9993e364706816aba3e25717850c26c9cd0d89d"),
                ),
                s(
                    &[b"SCRIPT", b"EXISTS"],
                    Expect::Err("ERR wrong number of arguments for 'script|exists' command"),
                ),
                s(
                    &[b"SCRIPT", b"FLUSH", b"BOGUS"],
                    Expect::Err("ERR SCRIPT FLUSH only support SYNC|ASYNC option"),
                ),
            ],
        },
        Case {
            family: "scripting",
            name: "a script may touch a key it builds, in the slot of its keys",
            // ADR-0052 D2: Redis Cluster's rule. Flint runs such a script
            // again holding every writer; the answer is the same.
            steps: vec![
                s(
                    &[
                        b"EVAL",
                        "redis.call('set', KEYS[1], 'a') redis.call('set', KEYS[1] .. ':u', ARGV[1]) \
                         return redis.call('get', KEYS[1] .. ':u')"
                            .as_bytes(),
                        b"1",
                        b"{us}k",
                        b"v",
                    ],
                    Expect::Str(b"v"),
                ),
                s(&[b"MGET", b"{us}k", b"{us}k:u"], Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"v")])),
                // BUG-0189: a script longer than the key cap (BullMQ's are).
                s(
                    &[
                        b"EVAL",
                        format!("return redis.call('get', KEYS[1] .. ':u') -- {}", "x".repeat(5000)).as_bytes(),
                        b"1",
                        b"{us}k",
                    ],
                    Expect::Str(b"v"),
                ),
                // Reached inside pcall, as a caught call would be.
                s(
                    &[
                        b"EVAL",
                        "local ok = redis.pcall('incr', KEYS[1] .. ':n') return redis.call('get', KEYS[1] .. ':n')"
                            .as_bytes(),
                        b"1",
                        b"{us}k",
                    ],
                    Expect::Str(b"1"),
                ),
                // Inside a transaction, which already excludes every writer.
                s(&[b"MULTI"], Expect::Ok),
                s(
                    &[b"EVAL", "return redis.call('incr', KEYS[1] .. ':n')".as_bytes(), b"1", b"{us}k"],
                    Expect::Simple("QUEUED"),
                ),
                s(&[b"EXEC"], Expect::Arr(vec![Expect::Int(2)])),
                // asynq's dequeue, on asynq's own keys.
                s(&[b"RPUSH", b"asynq:{q}:pending", b"id1"], Expect::Int(1)),
                s(
                    &[b"HSET", b"asynq:{q}:t:id1", b"msg", b"hello", b"state", b"pending", b"pending_since", b"1"],
                    Expect::Int(3),
                ),
                s(
                    &[
                        b"EVAL",
                        ASYNQ_DEQUEUE,
                        b"4",
                        b"asynq:{q}:pending",
                        b"asynq:{q}:paused",
                        b"asynq:{q}:active",
                        b"asynq:{q}:lease",
                        b"1000",
                        b"asynq:{q}:t:",
                    ],
                    Expect::Str(b"hello"),
                ),
                s(
                    &[b"HGETALL", b"asynq:{q}:t:id1"],
                    Expect::UnorderedPairs(vec![(b"msg", b"hello"), (b"state", b"active")]),
                ),
                s(&[b"LRANGE", b"asynq:{q}:active", b"0", b"-1"], Expect::Arr(vec![Expect::Str(b"id1")])),
                s(&[b"ZRANGE", b"asynq:{q}:lease", b"0", b"-1"], Expect::Arr(vec![Expect::Str(b"id1")])),
                s(
                    &[
                        b"EVAL",
                        ASYNQ_DEQUEUE,
                        b"4",
                        b"asynq:{q}:pending",
                        b"asynq:{q}:paused",
                        b"asynq:{q}:active",
                        b"asynq:{q}:lease",
                        b"1000",
                        b"asynq:{q}:t:",
                    ],
                    Expect::Nil,
                ),
            ],
        },
        Case {
            family: "sandbox",
            name: "a script is limited, touches only its slot, and fails whole",
            steps: vec![
                s(&[b"EVAL", "while true do end".as_bytes(), b"0"], Expect::AnyError),
                s(&[b"EVAL", "return 1".as_bytes(), b"2", b"a", b"b"], Expect::AnyError),
                s(&[b"EVAL", "return redis.call('get', 'other')".as_bytes(), b"1", b"{sb}a"], Expect::AnyError),
                s(&[b"EVAL", "redis.call('set', KEYS[1], 'x') error('then failed')".as_bytes(), b"1", b"{sb}rb"], Expect::AnyError),
                s(&[b"EXISTS", b"{sb}rb"], Expect::Int(0)),
                s(&[b"EVAL", "redis.call('set', KEYS[1], 'x') while true do end".as_bytes(), b"1", b"{sb}rt"], Expect::AnyError),
                s(&[b"EXISTS", b"{sb}rt"], Expect::Int(0)),
                // A key it built, in its slot, is discarded with the rest
                // (Valkey keeps a failed script's writes; ADR-0051 does not).
                s(
                    &[b"EVAL", "redis.call('set', KEYS[1] .. ':f', 'x') error('then failed')".as_bytes(), b"1", b"{sb}k"],
                    Expect::AnyError,
                ),
                s(&[b"EXISTS", b"{sb}k:f"], Expect::Int(0)),
                s(&[b"EVAL", "return redis.call('dbsize')".as_bytes(), b"1", b"{sb}a"], Expect::AnyError),
                s(&[b"EVAL", "return type(loadstring)".as_bytes(), b"0"], Expect::AnyError),
                s(&[b"EVAL", "return redis.call('eval', 'return 1', '0')".as_bytes(), b"0"], Expect::AnyError),
            ],
        },
        Case {
            family: "lists",
            name: "lmove and rpoplpush move one element between lists",
            // BUG-0187: both were unknown; asynq and BullMQ move every job
            // with RPOPLPUSH, rq with LMOVE.
            steps: vec![
                s(&[b"RPUSH", b"{lm}s", b"a", b"b", b"c"], Expect::Int(3)),
                s(&[b"LMOVE", b"{lm}s", b"{lm}d", b"RIGHT", b"LEFT"], Expect::Str(b"c")),
                s(&[b"LMOVE", b"{lm}s", b"{lm}d", b"left", b"right"], Expect::Str(b"a")),
                s(
                    &[b"LRANGE", b"{lm}d", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"c"), Expect::Str(b"a")]),
                ),
                s(&[b"RPOPLPUSH", b"{lm}s", b"{lm}d"], Expect::Str(b"b")),
                s(
                    &[b"LRANGE", b"{lm}d", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"c"), Expect::Str(b"a")]),
                ),
                s(&[b"EXISTS", b"{lm}s"], Expect::Int(0)),
                s(&[b"RPOPLPUSH", b"{lm}s", b"{lm}d"], Expect::Nil),
                // One list: a rotation.
                s(&[b"RPOPLPUSH", b"{lm}d", b"{lm}d"], Expect::Str(b"a")),
                s(
                    &[b"LRANGE", b"{lm}d", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"b"), Expect::Str(b"c")]),
                ),
                s(&[b"LMOVE", b"{lm}d", b"{lm}d", b"LEFT", b"RIGHT"], Expect::Str(b"a")),
                s(
                    &[b"LRANGE", b"{lm}d", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"c"), Expect::Str(b"a")]),
                ),
                // A destination of another type is refused before anything
                // moves; a missing source answers nil whatever the destination.
                s(&[b"SET", b"{lm}str", b"x"], Expect::Ok),
                s(&[b"LMOVE", b"{lm}d", b"{lm}str", b"LEFT", b"LEFT"], Expect::AnyError),
                s(&[b"LLEN", b"{lm}d"], Expect::Int(3)),
                s(&[b"LMOVE", b"{lm}none", b"{lm}str", b"LEFT", b"LEFT"], Expect::Nil),
                s(&[b"RPOPLPUSH", b"{lm}str", b"{lm}d"], Expect::AnyError),
                s(&[b"LMOVE", b"{lm}d", b"{lm}d", b"UP", b"LEFT"], Expect::AnyError),
                s(&[b"LMOVE", b"{lm}d", b"{lm}d"], Expect::AnyError),
            ],
        },
        Case {
            family: "lists",
            name: "blocking list pops answer at once when an element is there",
            // ADR-0052 D4. A seat never waits: through the proxy a client
            // does. A short timeout on empty keys gives the same null array
            // from Valkey (after the wait) and from a seat (at once). A
            // BLMOVE or BRPOPLPUSH timeout is not here: outside MULTI Valkey
            // answers a null array and a seat the null bulk of its
            // non-blocking form, which is what the proxy turns into the null
            // array.
            steps: vec![
                s(&[b"BLPOP", b"{bl}a", b"0.01"], Expect::NilArray),
                s(&[b"BRPOP", b"{bl}a", b"{bl}b", b"0.01"], Expect::NilArray),
                s(&[b"RPUSH", b"{bl}b", b"x", b"y"], Expect::Int(2)),
                s(
                    &[b"BLPOP", b"{bl}a", b"{bl}b", b"0"],
                    Expect::Arr(vec![Expect::Str(b"{bl}b"), Expect::Str(b"x")]),
                ),
                s(
                    &[b"BRPOP", b"{bl}a", b"{bl}b", b"0"],
                    Expect::Arr(vec![Expect::Str(b"{bl}b"), Expect::Str(b"y")]),
                ),
                s(&[b"RPUSH", b"{bl}a", b"p", b"q"], Expect::Int(2)),
                s(&[b"BRPOPLPUSH", b"{bl}a", b"{bl}d", b"0"], Expect::Str(b"q")),
                s(&[b"BLMOVE", b"{bl}a", b"{bl}d", b"LEFT", b"LEFT", b"0"], Expect::Str(b"p")),
                s(
                    &[b"LRANGE", b"{bl}d", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"p"), Expect::Str(b"q")]),
                ),
                s(&[b"BLPOP", b"{bl}a", b"-1"], Expect::Err("ERR timeout is negative")),
                s(&[b"BLPOP", b"{bl}a", b"abc"], Expect::Err("ERR timeout is not a float or out of range")),
                s(&[b"BLPOP", b"{bl}a", b"nan"], Expect::Err("ERR timeout is not a float or out of range")),
                s(&[b"BLPOP", b"{bl}a", b"inf"], Expect::Err("ERR timeout is out of range")),
                s(&[b"BLPOP", b"{bl}a"], Expect::AnyError),
                s(&[b"BLMOVE", b"{bl}a", b"{bl}d", b"UP", b"LEFT", b"0"], Expect::Err("ERR syntax error")),
                s(&[b"BRPOPLPUSH", b"{bl}a", b"0"], Expect::AnyError),
                s(&[b"SET", b"{bl}s", b"v"], Expect::Ok),
                s(&[b"BLPOP", b"{bl}s", b"0"], Expect::AnyError),
                s(&[b"BLMOVE", b"{bl}s", b"{bl}d", b"LEFT", b"LEFT", b"0"], Expect::AnyError),
            ],
        },
        Case {
            family: "zsets",
            name: "blocking sorted-set pops answer at once when a member is there",
            // ADR-0052 D4; see the list case.
            steps: vec![
                s(&[b"BZPOPMIN", b"{bz}a", b"0.01"], Expect::NilArray),
                s(&[b"ZADD", b"{bz}z", b"1.5", b"m", b"2", b"n", b"3", b"o"], Expect::Int(3)),
                s(
                    &[b"BZPOPMIN", b"{bz}a", b"{bz}z", b"0"],
                    Expect::Arr(vec![Expect::Str(b"{bz}z"), Expect::Str(b"m"), Expect::Str(b"1.5")]),
                ),
                s(
                    &[b"BZPOPMAX", b"{bz}z", b"0"],
                    Expect::Arr(vec![Expect::Str(b"{bz}z"), Expect::Str(b"o"), Expect::Str(b"3")]),
                ),
                s(&[b"BZPOPMAX", b"{bz}z", b"-0.5"], Expect::Err("ERR timeout is negative")),
                s(&[b"SET", b"{bz}s", b"v"], Expect::Ok),
                s(&[b"BZPOPMIN", b"{bz}s", b"0"], Expect::AnyError),
            ],
        },
        Case {
            family: "hashes",
            name: "hincrbyfloat adds to a field and answers the new value",
            // BUG-0187: unknown before; rq keeps a job's timings with it.
            steps: vec![
                s(&[b"HINCRBYFLOAT", b"{hf}h", b"f", b"10.5"], Expect::Str(b"10.5")),
                s(&[b"HINCRBYFLOAT", b"{hf}h", b"f", b"0.25"], Expect::Str(b"10.75")),
                s(&[b"HINCRBYFLOAT", b"{hf}h", b"f", b"-0.75"], Expect::Str(b"10")),
                s(&[b"HINCRBYFLOAT", b"{hf}h", b"f", b"5.0e3"], Expect::Str(b"5010")),
                s(&[b"HGET", b"{hf}h", b"f"], Expect::Str(b"5010")),
                s(&[b"HSET", b"{hf}h", b"g", b"abc"], Expect::Int(1)),
                s(&[b"HINCRBYFLOAT", b"{hf}h", b"g", b"1"], Expect::Err("ERR hash value is not a float")),
                s(&[b"HINCRBYFLOAT", b"{hf}h", b"f", b"x"], Expect::Err("ERR value is not a valid float")),
                s(&[b"HINCRBYFLOAT", b"{hf}h", b"f", b"inf"], Expect::Err("ERR value is NaN or Infinity")),
                s(&[b"HINCRBYFLOAT", b"{hf}h", b"f", b"-inf"], Expect::Err("ERR value is NaN or Infinity")),
                s(&[b"HINCRBYFLOAT", b"{hf}h", b"f"], Expect::AnyError),
                s(&[b"SET", b"{hf}s", b"1"], Expect::Ok),
                s(&[b"HINCRBYFLOAT", b"{hf}s", b"f", b"1"], Expect::AnyError),
            ],
        },
        Case {
            family: "hashes",
            name: "hmset sets fields and answers OK",
            // BUG-0182: HMSET was unknown, and Spring Session and ASP.NET
            // Core's IDistributedCache write every entry with it.
            steps: vec![
                s(&[b"HMSET", b"hm1", b"a", b"1", b"b", b"2"], Expect::Ok),
                s(&[b"HMSET", b"hm1", b"a", b"9"], Expect::Ok),
                s(&[b"HGET", b"hm1", b"a"], Expect::Str(b"9")),
                s(&[b"HLEN", b"hm1"], Expect::Int(2)),
                s(&[b"HMSET", b"hm1", b"a"], Expect::AnyError),
                s(&[b"HMSET", b"hm1"], Expect::AnyError),
                s(&[b"SET", b"hm2", b"x"], Expect::Ok),
                s(&[b"HMSET", b"hm2", b"f", b"v"], Expect::AnyError),
                s(&[b"GET", b"hm2"], Expect::Str(b"x")),
            ],
        },
        Case {
            family: "hashes",
            name: "hdel to empty removes the key",
            steps: vec![
                s(&[b"HSET", b"h2", b"a", b"1", b"b", b"2"], Expect::Int(2)),
                s(&[b"HDEL", b"h2", b"a", b"nope"], Expect::Int(1)),
                s(&[b"HDEL", b"h2", b"b"], Expect::Int(1)),
                s(&[b"EXISTS", b"h2"], Expect::Int(0)),
                s(&[b"TYPE", b"h2"], Expect::Simple("none")),
                s(&[b"HDEL", b"nosuch", b"f"], Expect::Int(0)),
            ],
        },
        Case {
            family: "hashes",
            name: "hgetall hmget hkeys hvals",
            steps: vec![
                s(&[b"HSET", b"h3", b"x", b"10", b"y", b"20"], Expect::Int(2)),
                s(
                    &[b"HGETALL", b"h3"],
                    Expect::UnorderedPairs(vec![(b"x", b"10"), (b"y", b"20")]),
                ),
                s(&[b"HGETALL", b"nosuch"], Expect::UnorderedPairs(vec![])),
                s(
                    &[b"HMGET", b"h3", b"y", b"zz", b"x"],
                    Expect::Arr(vec![Expect::Str(b"20"), Expect::Nil, Expect::Str(b"10")]),
                ),
                s(
                    &[b"HMGET", b"nosuch", b"a", b"b"],
                    Expect::Arr(vec![Expect::Nil, Expect::Nil]),
                ),
                // HKEYS and HVALS were in this case's NAME and in none of its
                // steps, so a coverage audit read from case names -- which is
                // what the run summary prints -- counted two commands that
                // nothing exercised. Hash iteration order is unspecified, so
                // both are compared as sets, exactly as HGETALL is above.
                s(&[b"HKEYS", b"h3"], Expect::UnorderedStrs(vec![b"x", b"y"])),
                s(
                    &[b"HVALS", b"h3"],
                    Expect::UnorderedStrs(vec![b"10", b"20"]),
                ),
                s(&[b"HKEYS", b"nosuch"], Expect::UnorderedStrs(vec![])),
                s(&[b"HVALS", b"nosuch"], Expect::UnorderedStrs(vec![])),
            ],
        },
        Case {
            family: "hashes",
            name: "hash respects ttl machinery",
            steps: vec![
                s(&[b"HSET", b"h4", b"f", b"v"], Expect::Int(1)),
                s(&[b"EXPIRE", b"h4", b"100"], Expect::Int(1)),
                s(&[b"TTL", b"h4"], Expect::IntRange(95, 100)),
                s(&[b"PERSIST", b"h4"], Expect::Int(1)),
                s(&[b"HGET", b"h4", b"f"], Expect::Str(b"v")),
                s(&[b"DEL", b"h4"], Expect::Int(1)),
                s(&[b"HGETALL", b"h4"], Expect::UnorderedPairs(vec![])),
            ],
        },
        Case {
            family: "hashes",
            name: "recreate after del is a fresh hash",
            steps: vec![
                s(&[b"HSET", b"h5", b"old", b"x"], Expect::Int(1)),
                s(&[b"DEL", b"h5"], Expect::Int(1)),
                s(&[b"HSET", b"h5", b"new", b"y"], Expect::Int(1)),
                s(&[b"HGET", b"h5", b"old"], Expect::Nil),
                s(&[b"HLEN", b"h5"], Expect::Int(1)),
            ],
        },
        Case {
            family: "protocol",
            name: "wrongtype in both directions",
            steps: vec![
                s(&[b"SET", b"wt-s", b"v"], Expect::Ok),
                s(&[b"HSET", b"wt-h", b"f", b"v"], Expect::Int(1)),
                s(&[b"HGET", b"wt-s", b"f"], Expect::AnyError),
                s(&[b"HSET", b"wt-s", b"f", b"v"], Expect::AnyError),
                s(&[b"GET", b"wt-h"], Expect::AnyError),
                s(&[b"INCR", b"wt-h"], Expect::AnyError),
                s(&[b"APPEND", b"wt-h", b"x"], Expect::AnyError),
                s(&[b"STRLEN", b"wt-h"], Expect::AnyError),
                s(&[b"SET", b"wt-h", b"overwritten"], Expect::Ok),
                s(&[b"GET", b"wt-h"], Expect::Str(b"overwritten")),
            ],
        },
        Case {
            family: "sets",
            // Same-slot only, so every key carries one hash tag. That is not
            // a Flint restriction: Redis Cluster requires it too, and a
            // corpus case without the tag would pass here and fail against
            // any real cluster.
            name: "sinter sunion sdiff (same slot)",
            steps: vec![
                s(&[b"SADD", b"{so}a", b"1", b"2", b"3"], Expect::Int(3)),
                s(&[b"SADD", b"{so}b", b"2", b"3", b"4"], Expect::Int(3)),
                s(&[b"SADD", b"{so}c", b"3", b"9"], Expect::Int(2)),
                s(
                    &[b"SINTER", b"{so}a", b"{so}b"],
                    Expect::UnorderedStrs(vec![b"2", b"3"]),
                ),
                s(
                    &[b"SINTER", b"{so}a", b"{so}b", b"{so}c"],
                    Expect::UnorderedStrs(vec![b"3"]),
                ),
                s(
                    &[b"SUNION", b"{so}a", b"{so}c"],
                    Expect::UnorderedStrs(vec![b"1", b"2", b"3", b"9"]),
                ),
                s(
                    &[b"SDIFF", b"{so}a", b"{so}b"],
                    Expect::UnorderedStrs(vec![b"1"]),
                ),
                // A single key is the degenerate fold: the set itself.
                s(
                    &[b"SINTER", b"{so}a"],
                    Expect::UnorderedStrs(vec![b"1", b"2", b"3"]),
                ),
                // A missing key is an empty set, and it propagates: an
                // intersection with nothing is nothing, a difference against
                // nothing is unchanged, a union ignores it.
                s(
                    &[b"SINTER", b"{so}a", b"{so}gone"],
                    Expect::UnorderedStrs(vec![]),
                ),
                s(
                    &[b"SDIFF", b"{so}a", b"{so}gone"],
                    Expect::UnorderedStrs(vec![b"1", b"2", b"3"]),
                ),
                s(
                    &[b"SUNION", b"{so}a", b"{so}gone"],
                    Expect::UnorderedStrs(vec![b"1", b"2", b"3"]),
                ),
                // Wrong type is an error, not an empty set.
                s(&[b"SET", b"{so}str", b"x"], Expect::Ok),
                s(&[b"SINTER", b"{so}a", b"{so}str"], Expect::AnyError),
                s(&[b"SINTER"], Expect::AnyError),
                s(
                    &[b"DEL", b"{so}a", b"{so}b", b"{so}c", b"{so}str"],
                    Expect::Int(4),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "zunionstore zinterstore (same slot)",
            steps: vec![
                s(&[b"ZADD", b"{zu}a", b"1", b"a", b"2", b"b"], Expect::Int(2)),
                s(
                    &[b"ZADD", b"{zu}b", b"10", b"b", b"20", b"c"],
                    Expect::Int(2),
                ),
                // SUM is the default: b is 2 + 10.
                s(
                    &[b"ZUNIONSTORE", b"{zu}d", b"2", b"{zu}a", b"{zu}b"],
                    Expect::Int(3),
                ),
                s(
                    &[b"ZRANGE", b"{zu}d", b"0", b"-1", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"1"),
                        Expect::Str(b"b"),
                        Expect::Str(b"12"),
                        Expect::Str(b"c"),
                        Expect::Str(b"20"),
                    ]),
                ),
                s(
                    &[b"ZINTERSTORE", b"{zu}i", b"2", b"{zu}a", b"{zu}b"],
                    Expect::Int(1),
                ),
                s(
                    &[b"ZRANGE", b"{zu}i", b"0", b"-1", b"WITHSCORES"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"12")]),
                ),
                // WEIGHTS scale each input before aggregation.
                s(
                    &[
                        b"ZUNIONSTORE",
                        b"{zu}w",
                        b"2",
                        b"{zu}a",
                        b"{zu}b",
                        b"WEIGHTS",
                        b"2",
                        b"3",
                    ],
                    Expect::Int(3),
                ),
                s(
                    &[b"ZRANGE", b"{zu}w", b"0", b"-1", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"2"),
                        Expect::Str(b"b"),
                        Expect::Str(b"34"),
                        Expect::Str(b"c"),
                        Expect::Str(b"60"),
                    ]),
                ),
                s(
                    &[
                        b"ZUNIONSTORE",
                        b"{zu}mn",
                        b"2",
                        b"{zu}a",
                        b"{zu}b",
                        b"AGGREGATE",
                        b"MIN",
                    ],
                    Expect::Int(3),
                ),
                s(
                    &[b"ZRANGE", b"{zu}mn", b"0", b"-1", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"1"),
                        Expect::Str(b"b"),
                        Expect::Str(b"2"),
                        Expect::Str(b"c"),
                        Expect::Str(b"20"),
                    ]),
                ),
                s(
                    &[
                        b"ZUNIONSTORE",
                        b"{zu}mx",
                        b"2",
                        b"{zu}a",
                        b"{zu}b",
                        b"AGGREGATE",
                        b"MAX",
                    ],
                    Expect::Int(3),
                ),
                s(
                    &[b"ZRANGE", b"{zu}mx", b"0", b"-1", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"1"),
                        Expect::Str(b"b"),
                        Expect::Str(b"10"),
                        Expect::Str(b"c"),
                        Expect::Str(b"20"),
                    ]),
                ),
                // A plain SET is a legal input, each member scoring 1.
                s(&[b"SADD", b"{zu}s", b"b", b"q"], Expect::Int(2)),
                s(
                    &[b"ZUNIONSTORE", b"{zu}ds", b"2", b"{zu}a", b"{zu}s"],
                    Expect::Int(3),
                ),
                s(
                    &[b"ZRANGE", b"{zu}ds", b"0", b"-1", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"1"),
                        Expect::Str(b"q"),
                        Expect::Str(b"1"),
                        Expect::Str(b"b"),
                        Expect::Str(b"3"),
                    ]),
                ),
                // NaN has two ways in and upstream turns both into 0: a zero
                // weight on an infinite score, and SUM over both infinities.
                // A NaN score would order unpredictably against everything.
                s(&[b"ZADD", b"{zu}pi", b"inf", b"x"], Expect::Int(1)),
                s(&[b"ZADD", b"{zu}ni", b"-inf", b"x"], Expect::Int(1)),
                s(
                    &[b"ZUNIONSTORE", b"{zu}r", b"2", b"{zu}pi", b"{zu}ni"],
                    Expect::Int(1),
                ),
                s(&[b"ZSCORE", b"{zu}r", b"x"], Expect::Str(b"0")),
                s(
                    &[b"ZUNIONSTORE", b"{zu}rw", b"1", b"{zu}pi", b"WEIGHTS", b"0"],
                    Expect::Int(1),
                ),
                s(&[b"ZSCORE", b"{zu}rw", b"x"], Expect::Str(b"0")),
                // MIN/MAX carry the infinities through untouched.
                s(
                    &[
                        b"ZUNIONSTORE",
                        b"{zu}rmn",
                        b"2",
                        b"{zu}pi",
                        b"{zu}ni",
                        b"AGGREGATE",
                        b"MIN",
                    ],
                    Expect::Int(1),
                ),
                s(&[b"ZSCORE", b"{zu}rmn", b"x"], Expect::Str(b"-inf")),
                // An empty result RETIRES the destination rather than
                // leaving an empty sorted set that answers EXISTS 1.
                s(&[b"SET", b"{zu}pre", b"v"], Expect::Ok),
                s(
                    &[b"ZINTERSTORE", b"{zu}pre", b"2", b"{zu}a", b"{zu}gone"],
                    Expect::Int(0),
                ),
                s(&[b"EXISTS", b"{zu}pre"], Expect::Int(0)),
                s(&[b"TYPE", b"{zu}pre"], Expect::Simple("none")),
                // The destination is overwritten whatever it held before.
                s(&[b"SET", b"{zu}str", b"v"], Expect::Ok),
                s(
                    &[b"ZUNIONSTORE", b"{zu}str", b"1", b"{zu}a"],
                    Expect::Int(2),
                ),
                s(&[b"TYPE", b"{zu}str"], Expect::Simple("zset")),
                // The destination may BE one of the sources: its old
                // contents must be folded in before it is replaced.
                s(
                    &[b"ZUNIONSTORE", b"{zu}a", b"2", b"{zu}a", b"{zu}b"],
                    Expect::Int(3),
                ),
                s(
                    &[b"ZRANGE", b"{zu}a", b"0", b"-1", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"1"),
                        Expect::Str(b"b"),
                        Expect::Str(b"12"),
                        Expect::Str(b"c"),
                        Expect::Str(b"20"),
                    ]),
                ),
                // numkeys is checked against what is actually present, and
                // the two ways of getting it wrong fail differently.
                s(
                    &[b"ZUNIONSTORE", b"{zu}e", b"0", b"{zu}a"],
                    Expect::AnyError,
                ),
                s(
                    &[b"ZUNIONSTORE", b"{zu}e", b"3", b"{zu}a", b"{zu}b"],
                    Expect::AnyError,
                ),
                s(
                    &[b"ZUNIONSTORE", b"{zu}e", b"1", b"{zu}a", b"{zu}b"],
                    Expect::AnyError,
                ),
                s(
                    &[b"ZUNIONSTORE", b"{zu}e", b"abc", b"{zu}a"],
                    Expect::AnyError,
                ),
                // A short WEIGHTS list is an error, not a pad with ones.
                s(
                    &[
                        b"ZUNIONSTORE",
                        b"{zu}e",
                        b"2",
                        b"{zu}a",
                        b"{zu}b",
                        b"WEIGHTS",
                        b"1",
                    ],
                    Expect::AnyError,
                ),
                s(
                    &[
                        b"ZUNIONSTORE",
                        b"{zu}e",
                        b"2",
                        b"{zu}a",
                        b"{zu}b",
                        b"WEIGHTS",
                        b"1",
                        b"x",
                    ],
                    Expect::AnyError,
                ),
                s(
                    &[
                        b"ZUNIONSTORE",
                        b"{zu}e",
                        b"2",
                        b"{zu}a",
                        b"{zu}b",
                        b"AGGREGATE",
                        b"BOGUS",
                    ],
                    Expect::AnyError,
                ),
                s(&[b"ZUNIONSTORE", b"{zu}e"], Expect::AnyError),
                // A non-set, non-zset input is WRONGTYPE, and the failure
                // must leave the destination uncreated.
                s(&[b"RPUSH", b"{zu}L", b"a"], Expect::Int(1)),
                s(
                    &[b"ZUNIONSTORE", b"{zu}e", b"2", b"{zu}a", b"{zu}L"],
                    Expect::AnyError,
                ),
                s(&[b"EXISTS", b"{zu}e"], Expect::Int(0)),
                s(
                    &[
                        b"DEL", b"{zu}a", b"{zu}b", b"{zu}d", b"{zu}i", b"{zu}w", b"{zu}mn",
                        b"{zu}mx", b"{zu}s", b"{zu}ds", b"{zu}pi", b"{zu}ni", b"{zu}r", b"{zu}rw",
                        b"{zu}rmn", b"{zu}str", b"{zu}L",
                    ],
                    Expect::Int(16),
                ),
            ],
        },
        Case {
            family: "sets",
            name: "sinterstore sunionstore sdiffstore (same slot)",
            steps: vec![
                s(&[b"SADD", b"{ss}a", b"1", b"2", b"3"], Expect::Int(3)),
                s(&[b"SADD", b"{ss}b", b"2", b"3", b"4"], Expect::Int(3)),
                s(
                    &[b"SINTERSTORE", b"{ss}i", b"{ss}a", b"{ss}b"],
                    Expect::Int(2),
                ),
                s(
                    &[b"SMEMBERS", b"{ss}i"],
                    Expect::UnorderedStrs(vec![b"2", b"3"]),
                ),
                s(&[b"TYPE", b"{ss}i"], Expect::Simple("set")),
                s(
                    &[b"SUNIONSTORE", b"{ss}u", b"{ss}a", b"{ss}b"],
                    Expect::Int(4),
                ),
                s(
                    &[b"SMEMBERS", b"{ss}u"],
                    Expect::UnorderedStrs(vec![b"1", b"2", b"3", b"4"]),
                ),
                s(
                    &[b"SDIFFSTORE", b"{ss}d", b"{ss}a", b"{ss}b"],
                    Expect::Int(1),
                ),
                s(&[b"SMEMBERS", b"{ss}d"], Expect::UnorderedStrs(vec![b"1"])),
                // An empty result RETIRES the destination, so a pre-existing
                // one is removed rather than left holding stale contents.
                s(&[b"SET", b"{ss}pre", b"v"], Expect::Ok),
                s(
                    &[b"SINTERSTORE", b"{ss}pre", b"{ss}a", b"{ss}gone"],
                    Expect::Int(0),
                ),
                s(&[b"EXISTS", b"{ss}pre"], Expect::Int(0)),
                s(&[b"TYPE", b"{ss}pre"], Expect::Simple("none")),
                // The destination is overwritten whatever it held.
                s(&[b"SET", b"{ss}str", b"v"], Expect::Ok),
                s(&[b"SUNIONSTORE", b"{ss}str", b"{ss}a"], Expect::Int(3)),
                s(&[b"TYPE", b"{ss}str"], Expect::Simple("set")),
                // The destination may BE a source.
                s(
                    &[b"SUNIONSTORE", b"{ss}a", b"{ss}a", b"{ss}b"],
                    Expect::Int(4),
                ),
                s(
                    &[b"SMEMBERS", b"{ss}a"],
                    Expect::UnorderedStrs(vec![b"1", b"2", b"3", b"4"]),
                ),
                // A SORTED SET IS NOT A LEGAL INPUT HERE, even though
                // ZUNIONSTORE accepts a plain set. The asymmetry is
                // upstream's, and this is the assertion that pins it.
                s(&[b"ZADD", b"{ss}z", b"1", b"m"], Expect::Int(1)),
                s(&[b"SINTERSTORE", b"{ss}e", b"{ss}z"], Expect::AnyError),
                s(&[b"RPUSH", b"{ss}L", b"x"], Expect::Int(1)),
                s(&[b"SINTERSTORE", b"{ss}e", b"{ss}L"], Expect::AnyError),
                s(&[b"EXISTS", b"{ss}e"], Expect::Int(0)),
                s(&[b"SINTERSTORE", b"{ss}e"], Expect::AnyError),
                s(
                    &[
                        b"DEL", b"{ss}a", b"{ss}b", b"{ss}i", b"{ss}u", b"{ss}d", b"{ss}str",
                        b"{ss}z", b"{ss}L",
                    ],
                    Expect::Int(8),
                ),
            ],
        },
        // BUG-0215: ZADD's flags were read as scores.
        Case {
            family: "zsets",
            name: "zadd takes nx xx gt lt ch and incr",
            steps: vec![
                s(&[b"ZADD", b"zo", b"1", b"a", b"2", b"b", b"3", b"c"], Expect::Int(3)),
                s(&[b"ZADD", b"zo", b"CH", b"5", b"a", b"2", b"b"], Expect::Int(1)),
                s(&[b"ZADD", b"zo", b"NX", b"10", b"a", b"4", b"d"], Expect::Int(1)),
                s(&[b"ZADD", b"zo", b"XX", b"6", b"a", b"7", b"nope"], Expect::Int(0)),
                s(&[b"ZADD", b"zo", b"XX", b"CH", b"6", b"a"], Expect::Int(0)),
                s(&[b"ZADD", b"zo", b"GT", b"1", b"a"], Expect::Int(0)),
                s(&[b"ZADD", b"zo", b"GT", b"CH", b"8", b"a"], Expect::Int(1)),
                s(&[b"ZADD", b"zo", b"LT", b"100", b"a"], Expect::Int(0)),
                s(&[b"ZADD", b"zo", b"lt", b"ch", b"0", b"a"], Expect::Int(1)),
                s(
                    &[b"ZADD", b"zo", b"NX", b"XX", b"1", b"a"],
                    Expect::Err("ERR XX and NX options at the same time are not compatible"),
                ),
                s(
                    &[b"ZADD", b"zo", b"NX", b"GT", b"1", b"a"],
                    Expect::Err("ERR GT, LT, and/or NX options at the same time are not compatible"),
                ),
                s(&[b"ZADD", b"zo", b"INCR", b"2", b"a"], Expect::Str(b"2")),
                s(
                    &[b"ZADD", b"zo", b"INCR", b"2", b"a", b"3", b"b"],
                    Expect::Err("ERR INCR option supports a single increment-element pair"),
                ),
                s(&[b"ZADD", b"zo", b"NX", b"INCR", b"1", b"a"], Expect::Nil),
                s(&[b"ZADD", b"zo", b"XX", b"INCR", b"1", b"newm"], Expect::Nil),
                s(&[b"ZADD", b"zo", b"GT", b"INCR", b"-1", b"a"], Expect::Nil),
                s(&[b"ZADD", b"zo", b"1", b"a", b"2"], Expect::Err("ERR syntax error")),
                // Pairs apply in order: the second sees the first's result.
                s(&[b"ZADD", b"zo", b"GT", b"5", b"dup", b"3", b"dup"], Expect::Int(1)),
                s(&[b"ZSCORE", b"zo", b"dup"], Expect::Str(b"5")),
                s(&[b"ZADD", b"zo", b"CH", b"1", b"dup2", b"2", b"dup2"], Expect::Int(2)),
                s(
                    &[b"ZRANGE", b"zo", b"0", b"-1", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"2"),
                        Expect::Str(b"b"),
                        Expect::Str(b"2"),
                        Expect::Str(b"dup2"),
                        Expect::Str(b"2"),
                        Expect::Str(b"c"),
                        Expect::Str(b"3"),
                        Expect::Str(b"d"),
                        Expect::Str(b"4"),
                        Expect::Str(b"dup"),
                        Expect::Str(b"5"),
                    ]),
                ),
                // XX on a missing key leaves it missing.
                s(&[b"ZADD", b"zx", b"XX", b"1", b"a"], Expect::Int(0)),
                s(&[b"EXISTS", b"zx"], Expect::Int(0)),
            ],
        },
        // BUG-0215: ZRANGE's Redis 6.2 form.
        Case {
            family: "zsets",
            name: "zrange takes byscore bylex rev and limit",
            steps: vec![
                s(&[b"ZADD", b"zr", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d", b"5", b"e"], Expect::Int(5)),
                s(&[b"ZRANGE", b"zr", b"1", b"3", b"REV"], Expect::Arr(vec![Expect::Str(b"d"), Expect::Str(b"c"), Expect::Str(b"b")])),
                s(&[b"ZRANGE", b"zr", b"2", b"4", b"BYSCORE"], Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"c"), Expect::Str(b"d")])),
                s(&[b"ZRANGE", b"zr", b"(2", b"4", b"BYSCORE"], Expect::Arr(vec![Expect::Str(b"c"), Expect::Str(b"d")])),
                s(&[b"ZRANGE", b"zr", b"4", b"2", b"BYSCORE", b"REV"], Expect::Arr(vec![Expect::Str(b"d"), Expect::Str(b"c"), Expect::Str(b"b")])),
                s(
                    &[b"ZRANGE", b"zr", b"-inf", b"+inf", b"BYSCORE", b"LIMIT", b"1", b"2"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"c")]),
                ),
                s(
                    &[b"ZRANGE", b"zr", b"-inf", b"+inf", b"BYSCORE", b"LIMIT", b"1", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"c"), Expect::Str(b"d"), Expect::Str(b"e")]),
                ),
                s(&[b"ZRANGE", b"zr", b"-inf", b"+inf", b"BYSCORE", b"LIMIT", b"-1", b"2"], Expect::Arr(vec![])),
                s(
                    &[b"ZRANGE", b"zr", b"4", b"2", b"BYSCORE", b"REV", b"WITHSCORES", b"LIMIT", b"0", b"2"],
                    Expect::Arr(vec![Expect::Str(b"d"), Expect::Str(b"4"), Expect::Str(b"c"), Expect::Str(b"3")]),
                ),
                s(
                    &[b"ZRANGE", b"zr", b"0", b"-1", b"LIMIT", b"0", b"1"],
                    Expect::Err(
                        "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
                    ),
                ),
                s(
                    &[b"ZRANGE", b"zr", b"[a", b"[c", b"BYLEX", b"WITHSCORES"],
                    Expect::Err("ERR syntax error, WITHSCORES not supported in combination with BYLEX"),
                ),
                // A count of -1 reads as no LIMIT, so a rank range ignores it.
                s(
                    &[b"ZRANGE", b"zr", b"3", b"4", b"LIMIT", b"1", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"d"), Expect::Str(b"e")]),
                ),
                s(&[b"ZRANGE", b"zr", b"0", b"-1", b"BYSCORE", b"BYLEX"], Expect::Err("ERR syntax error")),
                s(&[b"ZRANGE", b"zr", b"0", b"-1", b"REV", b"REV"], Expect::Err("ERR syntax error")),
                s(&[b"ZRANGE", b"zr", b"0", b"-1", b"LIMIT", b"0"], Expect::Err("ERR syntax error")),
                s(&[b"ZRANGE", b"zr", b"1", b"abc", b"BYSCORE"], Expect::Err("ERR min or max is not a float")),
                // A score bound reads as Redis's does: an empty number is 0,
                // and NaN or a padded number is not a float.
                s(&[b"ZADD", b"zr", b"-1", b"neg", b"0", b"zero"], Expect::Int(2)),
                s(&[b"ZCOUNT", b"zr", b"(", b"+inf"], Expect::Int(5)),
                s(&[b"ZCOUNT", b"zr", b"", b"+inf"], Expect::Int(6)),
                s(&[b"ZCOUNT", b"zr", b"nan", b"+inf"], Expect::Err("ERR min or max is not a float")),
                s(&[b"ZCOUNT", b"zr", b" 1", b"+inf"], Expect::Err("ERR min or max is not a float")),
                s(&[b"ZREM", b"zr", b"neg", b"zero"], Expect::Int(2)),
                s(&[b"ZRANGE", b"zr", b"a", b"c", b"BYLEX"], Expect::Err("ERR min or max not valid string range item")),
                s(&[b"ZADD", b"zl", b"0", b"a", b"0", b"b", b"0", b"c", b"0", b"d"], Expect::Int(4)),
                s(&[b"ZRANGE", b"zl", b"[b", b"(d", b"BYLEX"], Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"c")])),
                s(
                    &[b"ZRANGE", b"zl", b"+", b"-", b"BYLEX", b"REV", b"LIMIT", b"0", b"2"],
                    Expect::Arr(vec![Expect::Str(b"d"), Expect::Str(b"c")]),
                ),
                s(&[b"ZRANGE", b"zl", b"-", b"+", b"BYLEX", b"LIMIT", b"1", b"1"], Expect::Arr(vec![Expect::Str(b"b")])),
                // A negative offset answers nothing, in the older forms too.
                s(&[b"ZRANGEBYLEX", b"zl", b"-", b"+", b"LIMIT", b"-1", b"2"], Expect::Arr(vec![])),
                s(&[b"ZRANGEBYSCORE", b"zr", b"-inf", b"+inf", b"LIMIT", b"-1", b"2"], Expect::Arr(vec![])),
                s(&[b"ZRANGE", b"nosuchzr", b"0", b"-1", b"BYSCORE"], Expect::Arr(vec![])),
                // Another type is WRONGTYPE whatever the LIMIT (BUG-0216's
                // first build answered an empty array to a count of 0).
                s(&[b"SET", b"zstr", b"v"], Expect::Ok),
                s(
                    &[b"ZRANGE", b"zstr", b"0", b"-1", b"BYSCORE", b"LIMIT", b"0", b"0"],
                    Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                ),
                s(
                    &[b"ZRANGEBYSCORE", b"zstr", b"-inf", b"+inf", b"LIMIT", b"-1", b"1"],
                    Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                ),
            ],
        },
        // BUG-0215: ZRANK's WITHSCORE, Redis 7.2's.
        Case {
            family: "zsets",
            name: "zrank and zrevrank take withscore",
            steps: vec![
                s(&[b"ZADD", b"zk", b"1", b"a", b"2.5", b"b"], Expect::Int(2)),
                s(&[b"ZRANK", b"zk", b"b", b"WITHSCORE"], Expect::Arr(vec![Expect::Int(1), Expect::Str(b"2.5")])),
                s(&[b"ZREVRANK", b"zk", b"b", b"withscore"], Expect::Arr(vec![Expect::Int(0), Expect::Str(b"2.5")])),
                s(&[b"ZRANK", b"zk", b"nope", b"WITHSCORE"], Expect::NilArray),
                s(&[b"ZRANK", b"zk", b"nope"], Expect::Nil),
                s(&[b"ZRANK", b"zk", b"a", b"FOO"], Expect::Err("ERR syntax error")),
                s(
                    &[b"ZRANK", b"zk", b"a", b"WITHSCORE", b"x"],
                    Expect::Err("ERR wrong number of arguments for 'zrank' command"),
                ),
                s(&[b"ZRANK", b"zk", b"a"], Expect::Int(0)),
            ],
        },
        // BUG-0212: +inf plus -inf is NaN, which Redis refuses to store.
        Case {
            family: "zsets",
            name: "zincrby refuses a nan score",
            steps: vec![
                s(&[b"ZADD", b"zn", b"inf", b"m"], Expect::Int(1)),
                s(
                    &[b"ZINCRBY", b"zn", b"-inf", b"m"],
                    Expect::Err("ERR resulting score is not a number (NaN)"),
                ),
                s(&[b"ZSCORE", b"zn", b"m"], Expect::Str(b"inf")),
            ],
        },
        // BUG-0228: -0 and 0 are one score, tied by member. A member moved
        // between them kept a stale index row, which ZRANGE then read.
        Case {
            family: "zsets",
            name: "a score of -0 is the score 0",
            steps: vec![
                s(&[b"ZADD", b"zz", b"0", b"b", b"-0", b"g"], Expect::Int(2)),
                s(
                    &[b"ZRANGE", b"zz", b"0", b"-1", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"b"),
                        Expect::Str(b"0"),
                        Expect::Str(b"g"),
                        Expect::Str(b"0"),
                    ]),
                ),
                s(&[b"ZADD", b"zz", b"0", b"g"], Expect::Int(0)),
                s(&[b"ZADD", b"zz", b"1", b"g"], Expect::Int(0)),
                s(
                    &[b"ZRANGE", b"zz", b"0", b"-1", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"b"),
                        Expect::Str(b"0"),
                        Expect::Str(b"g"),
                        Expect::Str(b"1"),
                    ]),
                ),
                s(&[b"ZINCRBY", b"zz", b"-0", b"b"], Expect::Str(b"0")),
                s(&[b"ZSCORE", b"zz", b"b"], Expect::Str(b"0")),
                s(
                    &[b"ZREVRANGEBYSCORE", b"zz", b"-0", b"-inf"],
                    Expect::Arr(vec![Expect::Str(b"b")]),
                ),
                s(&[b"ZCARD", b"zz"], Expect::Int(2)),
            ],
        },
        // BUG-0230: the range commands read their options before their
        // bounds, a ZPOPMIN count is refused in upstream's words, and a new
        // member's -0 increment answers -0.
        Case {
            family: "zsets",
            name: "sorted-set refusals in upstream's order, and a -0 reply",
            steps: vec![
                s(&[b"ZADD", b"zr", b"1", b"a"], Expect::Int(1)),
                s(
                    &[b"ZRANGEBYSCORE", b"zr", b"x", b"1", b"LIMIT", b"a", b"1"],
                    Expect::Err("ERR value is not an integer or out of range"),
                ),
                s(&[b"ZREVRANGEBYSCORE", b"zr", b"1", b"x", b"BOGUS"], Expect::Err("ERR syntax error")),
                s(
                    &[b"ZRANGEBYLEX", b"zr", b"x", b"+", b"LIMIT", b"a", b"1"],
                    Expect::Err("ERR value is not an integer or out of range"),
                ),
                s(&[b"ZREVRANGEBYLEX", b"zr", b"+", b"x", b"BOGUS"], Expect::Err("ERR syntax error")),
                s(
                    &[b"ZPOPMIN", b"zr", b"abc"],
                    Expect::Err("ERR value is out of range, must be positive"),
                ),
                s(&[b"ZINCRBY", b"zr", b"-0", b"new"], Expect::Str(b"-0")),
                s(&[b"ZADD", b"zr", b"INCR", b"-0", b"new2"], Expect::Str(b"-0")),
                s(&[b"ZSCORE", b"zr", b"new"], Expect::Str(b"0")),
                s(&[b"ZINCRBY", b"zr", b"-0", b"new"], Expect::Str(b"0")),
            ],
        },
        // BUG-0231: a zero weight against an infinite score is NaN, which
        // upstream zeroes for the intersection's first input only.
        Case {
            family: "zsets",
            name: "a later intersection input's nan goes into the aggregate",
            steps: vec![
                s(&[b"ZADD", b"{nw}a", b"5", b"m"], Expect::Int(1)),
                s(&[b"ZADD", b"{nw}b", b"inf", b"m"], Expect::Int(1)),
                s(
                    &[b"ZINTERSTORE", b"{nw}d", b"2", b"{nw}a", b"{nw}b", b"WEIGHTS", b"1", b"0"],
                    Expect::Int(1),
                ),
                s(&[b"ZSCORE", b"{nw}d", b"m"], Expect::Str(b"0")),
                s(
                    &[
                        b"ZINTERSTORE", b"{nw}d", b"2", b"{nw}a", b"{nw}b", b"WEIGHTS", b"1", b"0",
                        b"AGGREGATE", b"MIN",
                    ],
                    Expect::Int(1),
                ),
                s(&[b"ZSCORE", b"{nw}d", b"m"], Expect::Str(b"5")),
                s(
                    &[
                        b"ZINTERSTORE", b"{nw}d", b"2", b"{nw}b", b"{nw}a", b"WEIGHTS", b"0", b"1",
                        b"AGGREGATE", b"MIN",
                    ],
                    Expect::Int(1),
                ),
                s(&[b"ZSCORE", b"{nw}d", b"m"], Expect::Str(b"0")),
            ],
        },
        // BUG-0229: an intersection that empties early still type-checks
        // every input, so its STORE form leaves the destination alone.
        Case {
            family: "zsets",
            name: "an intersection checks every input's type before it answers",
            steps: vec![
                s(&[b"SADD", b"{it}set", b"x"], Expect::Int(1)),
                s(&[b"SET", b"{it}str", b"v"], Expect::Ok),
                s(&[b"SADD", b"{it}dst", b"kept"], Expect::Int(1)),
                s(&[b"ZADD", b"{it}zdst", b"1", b"kept"], Expect::Int(1)),
                s(
                    &[b"SINTER", b"{it}none", b"{it}set", b"{it}str"],
                    Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                ),
                s(
                    &[b"SDIFF", b"{it}none", b"{it}set", b"{it}str"],
                    Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                ),
                s(
                    &[b"SINTERSTORE", b"{it}dst", b"{it}none", b"{it}set", b"{it}str"],
                    Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                ),
                s(&[b"SCARD", b"{it}dst"], Expect::Int(1)),
                s(
                    &[b"ZINTERSTORE", b"{it}zdst", b"3", b"{it}none", b"{it}set", b"{it}str"],
                    Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                ),
                s(
                    &[b"ZUNIONSTORE", b"{it}zdst", b"1", b"{it}str", b"BOGUS"],
                    Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                ),
                s(
                    &[b"ZUNIONSTORE", b"{it}zdst", b"1", b"{it}set", b"BOGUS"],
                    Expect::Err("ERR syntax error"),
                ),
                s(&[b"ZCARD", b"{it}zdst"], Expect::Int(1)),
            ],
        },
        // BUG-0214: a score is spelled as Redis's d2string spells it. Large
        // and small values were written out in full here.
        Case {
            family: "zsets",
            name: "scores are spelled as redis spells them",
            steps: vec![
                s(
                    &[
                        b"ZADD", b"zf", b"1e20", b"a", b"1e-7", b"b", b"0.000123456789", b"c",
                        b"1.5e300", b"d", b"5e-324", b"e", b"4611686018427387904", b"f",
                        b"9.3e18", b"g", b"1234567.125", b"h", b"0.0001", b"i", b"-2.5e-9", b"j",
                    ],
                    Expect::Int(10),
                ),
                s(&[b"ZSCORE", b"zf", b"a"], Expect::Str(b"1e+20")),
                s(&[b"ZSCORE", b"zf", b"b"], Expect::Str(b"1e-7")),
                s(&[b"ZSCORE", b"zf", b"c"], Expect::Str(b"1.23456789e-4")),
                s(&[b"ZSCORE", b"zf", b"d"], Expect::Str(b"1.5e+300")),
                s(&[b"ZSCORE", b"zf", b"e"], Expect::Str(b"5e-324")),
                s(&[b"ZSCORE", b"zf", b"f"], Expect::Str(b"4611686018427387904")),
                s(&[b"ZSCORE", b"zf", b"g"], Expect::Str(b"9.3e+18")),
                s(&[b"ZSCORE", b"zf", b"h"], Expect::Str(b"1234567.125")),
                s(&[b"ZSCORE", b"zf", b"i"], Expect::Str(b"0.0001")),
                s(&[b"ZSCORE", b"zf", b"j"], Expect::Str(b"-2.5e-9")),
                s(
                    &[b"ZRANGE", b"zf", b"0", b"1", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"j"),
                        Expect::Str(b"-2.5e-9"),
                        Expect::Str(b"e"),
                        Expect::Str(b"5e-324"),
                    ]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "zlexcount zremrangebylex",
            steps: vec![
                s(
                    &[
                        b"ZADD", b"{zl}z", b"0", b"a", b"0", b"b", b"0", b"c", b"0", b"d", b"0",
                        b"e",
                    ],
                    Expect::Int(5),
                ),
                s(&[b"ZLEXCOUNT", b"{zl}z", b"-", b"+"], Expect::Int(5)),
                s(&[b"ZLEXCOUNT", b"{zl}z", b"[b", b"[d"], Expect::Int(3)),
                s(&[b"ZLEXCOUNT", b"{zl}z", b"(b", b"(d"], Expect::Int(1)),
                // An inverted range counts nothing rather than erroring.
                s(&[b"ZLEXCOUNT", b"{zl}z", b"+", b"-"], Expect::Int(0)),
                s(&[b"ZLEXCOUNT", b"{zl}nokey", b"-", b"+"], Expect::Int(0)),
                s(&[b"ZLEXCOUNT", b"{zl}z", b"b", b"d"], Expect::AnyError),
                s(&[b"ZLEXCOUNT", b"{zl}z", b"-"], Expect::AnyError),
                s(&[b"ZREMRANGEBYLEX", b"{zl}z", b"[b", b"[c"], Expect::Int(2)),
                s(
                    &[b"ZRANGE", b"{zl}z", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"d"),
                        Expect::Str(b"e"),
                    ]),
                ),
                // Emptying the set removes the key.
                s(&[b"ZREMRANGEBYLEX", b"{zl}z", b"-", b"+"], Expect::Int(3)),
                s(&[b"EXISTS", b"{zl}z"], Expect::Int(0)),
                s(
                    &[b"ZREMRANGEBYLEX", b"{zl}nokey", b"-", b"+"],
                    Expect::Int(0),
                ),
                s(&[b"SET", b"{zl}str", b"v"], Expect::Ok),
                s(&[b"ZLEXCOUNT", b"{zl}str", b"-", b"+"], Expect::AnyError),
                s(
                    &[b"ZREMRANGEBYLEX", b"{zl}str", b"-", b"+"],
                    Expect::AnyError,
                ),
                s(&[b"DEL", b"{zl}str"], Expect::Int(1)),
            ],
        },
        Case {
            family: "keyspace",
            name: "rename renamenx (same slot)",
            steps: vec![
                s(&[b"SET", b"{rn}a", b"v1", b"EX", b"100"], Expect::Ok),
                s(&[b"RENAME", b"{rn}a", b"{rn}b"], Expect::Ok),
                s(&[b"GET", b"{rn}b"], Expect::Str(b"v1")),
                // The TTL moves with the value.
                s(&[b"TTL", b"{rn}b"], Expect::IntRange(95, 100)),
                s(&[b"EXISTS", b"{rn}a"], Expect::Int(0)),
                // A missing source is an ERROR for both forms, not a 0.
                s(&[b"RENAME", b"{rn}gone", b"{rn}x"], Expect::AnyError),
                s(&[b"RENAMENX", b"{rn}gone", b"{rn}x"], Expect::AnyError),
                // RENAME overwrites an occupied destination; RENAMENX
                // refuses and leaves the SOURCE in place — the difference
                // that makes the outcome three-valued rather than a bool.
                s(&[b"SET", b"{rn}o1", b"a"], Expect::Ok),
                s(&[b"SET", b"{rn}o2", b"b"], Expect::Ok),
                s(&[b"RENAME", b"{rn}o1", b"{rn}o2"], Expect::Ok),
                s(&[b"GET", b"{rn}o2"], Expect::Str(b"a")),
                s(&[b"EXISTS", b"{rn}o1"], Expect::Int(0)),
                s(&[b"SET", b"{rn}n1", b"a"], Expect::Ok),
                s(&[b"SET", b"{rn}n2", b"b"], Expect::Ok),
                s(&[b"RENAMENX", b"{rn}n1", b"{rn}n2"], Expect::Int(0)),
                s(&[b"GET", b"{rn}n2"], Expect::Str(b"b")),
                s(&[b"EXISTS", b"{rn}n1"], Expect::Int(1)),
                s(&[b"RENAMENX", b"{rn}n1", b"{rn}n3"], Expect::Int(1)),
                s(&[b"EXISTS", b"{rn}n1"], Expect::Int(0)),
                // Onto ITSELF: a no-op success for RENAME, and 0 for
                // RENAMENX because the destination is by definition taken.
                // Routed through the copy path this would be destructive.
                s(&[b"SET", b"{rn}self", b"v"], Expect::Ok),
                s(&[b"RENAME", b"{rn}self", b"{rn}self"], Expect::Ok),
                s(&[b"GET", b"{rn}self"], Expect::Str(b"v")),
                s(&[b"RENAMENX", b"{rn}self", b"{rn}self"], Expect::Int(0)),
                s(&[b"GET", b"{rn}self"], Expect::Str(b"v")),
                // Collections rename whole, both zset row families included.
                s(&[b"RPUSH", b"{rn}L", b"a", b"b", b"c"], Expect::Int(3)),
                s(&[b"RENAME", b"{rn}L", b"{rn}L2"], Expect::Ok),
                s(
                    &[b"LRANGE", b"{rn}L2", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                    ]),
                ),
                s(&[b"EXISTS", b"{rn}L"], Expect::Int(0)),
                s(&[b"ZADD", b"{rn}Z", b"1", b"a", b"2", b"b"], Expect::Int(2)),
                s(&[b"RENAME", b"{rn}Z", b"{rn}Z2"], Expect::Ok),
                s(&[b"ZSCORE", b"{rn}Z2", b"b"], Expect::Str(b"2")),
                s(
                    &[b"ZRANGE", b"{rn}Z2", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"b")]),
                ),
                s(&[b"RENAME", b"{rn}b"], Expect::AnyError),
                s(
                    &[
                        b"DEL",
                        b"{rn}b",
                        b"{rn}o2",
                        b"{rn}n2",
                        b"{rn}n3",
                        b"{rn}self",
                        b"{rn}L2",
                        b"{rn}Z2",
                    ],
                    Expect::Int(7),
                ),
            ],
        },
        Case {
            family: "transactions",
            name: "a blocking pop inside multi does not wait",
            // ADR-0052 D4: as in Redis, and the null kinds are Redis's: an
            // array for BLPOP and BZPOPMIN, a bulk for BLMOVE and
            // BRPOPLPUSH.
            steps: vec![
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"BLPOP", b"{bt}l", b"0"], Expect::Simple("QUEUED")),
                s(&[b"BZPOPMIN", b"{bt}z", b"0"], Expect::Simple("QUEUED")),
                s(&[b"BLMOVE", b"{bt}l", b"{bt}d", b"LEFT", b"RIGHT", b"0"], Expect::Simple("QUEUED")),
                s(&[b"BRPOPLPUSH", b"{bt}l", b"{bt}d", b"0"], Expect::Simple("QUEUED")),
                s(
                    &[b"EXEC"],
                    Expect::Arr(vec![Expect::NilArray, Expect::NilArray, Expect::Nil, Expect::Nil]),
                ),
                s(
                    &[b"EVAL", "return redis.call('blpop', KEYS[1], 0)".as_bytes(), b"1", b"{bt}l"],
                    Expect::Nil,
                ),
            ],
        },
        Case {
            family: "transactions",
            name: "multi exec discard (same slot)",
            steps: vec![
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"SET", b"{tx}a", b"1"], Expect::Simple("QUEUED")),
                s(&[b"INCR", b"{tx}a"], Expect::Simple("QUEUED")),
                s(&[b"GET", b"{tx}a"], Expect::Simple("QUEUED")),
                // Each command sees its predecessors' effects, and the whole
                // reply arrives as one array.
                s(
                    &[b"EXEC"],
                    Expect::Arr(vec![Expect::Ok, Expect::Int(2), Expect::Str(b"2")]),
                ),
                s(&[b"GET", b"{tx}a"], Expect::Str(b"2")),
                // An empty transaction is an empty array, not an error.
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"EXEC"], Expect::Arr(vec![])),
                // DISCARD drops the queue and nothing is applied.
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"SET", b"{tx}d", b"9"], Expect::Simple("QUEUED")),
                s(&[b"DISCARD"], Expect::Ok),
                s(&[b"EXISTS", b"{tx}d"], Expect::Int(0)),
                // Both verbs outside a transaction are errors, and MULTI
                // inside one is too.
                s(&[b"EXEC"], Expect::AnyError),
                s(&[b"DISCARD"], Expect::AnyError),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"MULTI"], Expect::AnyError),
                s(&[b"DISCARD"], Expect::Ok),
                // A QUEUE-TIME error poisons the transaction: EXEC applies
                // NOTHING. This is the distinction that matters most —
                // collapsing it into a runtime error would partially apply a
                // transaction the client was told would abort.
                s(&[b"SET", b"{tx}keep", b"orig"], Expect::Ok),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"SET", b"{tx}keep", b"changed"], Expect::Simple("QUEUED")),
                s(&[b"NOSUCHCOMMAND", b"x"], Expect::AnyError),
                s(&[b"EXEC"], Expect::AnyError),
                s(&[b"GET", b"{tx}keep"], Expect::Str(b"orig")),
                // A wrong argument count is a queue-time error too, not a
                // runtime one.
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"SET", b"{tx}q", b"1"], Expect::Simple("QUEUED")),
                s(&[b"SET"], Expect::AnyError),
                s(&[b"EXEC"], Expect::AnyError),
                s(&[b"EXISTS", b"{tx}q"], Expect::Int(0)),
                // A RUNTIME error does NOT abort: it is one element of the
                // reply and every other command still applies.
                s(&[b"RPUSH", b"{tx}L", b"x"], Expect::Int(1)),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"SET", b"{tx}r", b"1"], Expect::Simple("QUEUED")),
                s(&[b"INCR", b"{tx}L"], Expect::Simple("QUEUED")),
                s(&[b"GET", b"{tx}r"], Expect::Simple("QUEUED")),
                s(
                    &[b"EXEC"],
                    Expect::Arr(vec![Expect::Ok, Expect::AnyError, Expect::Str(b"1")]),
                ),
                s(&[b"GET", b"{tx}r"], Expect::Str(b"1")),
                // COLLECTIONS read back their own in-transaction writes.
                // This is the property the batch overlay exists for, and it
                // is invisible from any string-only case: every collection
                // type reads through a prefix scan.
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"SADD", b"{tx}s", b"a", b"b"], Expect::Simple("QUEUED")),
                s(&[b"SCARD", b"{tx}s"], Expect::Simple("QUEUED")),
                s(&[b"HSET", b"{tx}h", b"f", b"v"], Expect::Simple("QUEUED")),
                s(&[b"HGET", b"{tx}h", b"f"], Expect::Simple("QUEUED")),
                s(&[b"RPUSH", b"{tx}l", b"x", b"y"], Expect::Simple("QUEUED")),
                s(
                    &[b"LRANGE", b"{tx}l", b"0", b"-1"],
                    Expect::Simple("QUEUED"),
                ),
                s(&[b"ZADD", b"{tx}z", b"1", b"m"], Expect::Simple("QUEUED")),
                s(&[b"ZSCORE", b"{tx}z", b"m"], Expect::Simple("QUEUED")),
                s(
                    &[b"EXEC"],
                    Expect::Arr(vec![
                        Expect::Int(2),
                        Expect::Int(2),
                        Expect::Int(1),
                        Expect::Str(b"v"),
                        Expect::Int(2),
                        Expect::Arr(vec![Expect::Str(b"x"), Expect::Str(b"y")]),
                        Expect::Int(1),
                        Expect::Str(b"1"),
                    ]),
                ),
                // A delete inside the transaction hides the key from a later
                // command in the same transaction.
                s(&[b"SET", b"{tx}del", b"v"], Expect::Ok),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"DEL", b"{tx}del"], Expect::Simple("QUEUED")),
                s(&[b"EXISTS", b"{tx}del"], Expect::Simple("QUEUED")),
                s(
                    &[b"EXEC"],
                    Expect::Arr(vec![Expect::Int(1), Expect::Int(0)]),
                ),
                s(
                    &[
                        b"DEL",
                        b"{tx}a",
                        b"{tx}keep",
                        b"{tx}L",
                        b"{tx}r",
                        b"{tx}s",
                        b"{tx}h",
                        b"{tx}l",
                        b"{tx}z",
                    ],
                    Expect::Int(8),
                ),
            ],
        },
        Case {
            family: "transactions",
            name: "watch unwatch (single connection)",
            steps: vec![
                // The multi-CLIENT races WATCH exists for cannot be
                // expressed here — this harness is one connection — so they
                // live in the differential probe instead. What IS expressible
                // is everything a single connection can observe, and the
                // self-write case below is the one that proves the mechanism
                // rather than just the plumbing.
                s(&[b"SET", b"{wt}k", b"1"], Expect::Ok),
                s(&[b"WATCH", b"{wt}k"], Expect::Ok),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"SET", b"{wt}k", b"2"], Expect::Simple("QUEUED")),
                // Untouched since WATCH: commits.
                s(&[b"EXEC"], Expect::Arr(vec![Expect::Ok])),
                s(&[b"GET", b"{wt}k"], Expect::Str(b"2")),
                // A write by THIS connection between WATCH and EXEC still
                // breaks the watch — WATCH tracks the key, not the author.
                s(&[b"WATCH", b"{wt}k"], Expect::Ok),
                s(&[b"SET", b"{wt}k", b"3"], Expect::Ok),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"SET", b"{wt}k", b"4"], Expect::Simple("QUEUED")),
                s(&[b"EXEC"], Expect::NilArray),
                s(&[b"GET", b"{wt}k"], Expect::Str(b"3")),
                // EXEC clears the watch, so the NEXT transaction is armed
                // by nothing and commits despite the intervening write.
                s(&[b"SET", b"{wt}k", b"5"], Expect::Ok),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"SET", b"{wt}k", b"6"], Expect::Simple("QUEUED")),
                s(&[b"EXEC"], Expect::Arr(vec![Expect::Ok])),
                // UNWATCH disarms.
                s(&[b"WATCH", b"{wt}k"], Expect::Ok),
                s(&[b"UNWATCH"], Expect::Ok),
                s(&[b"SET", b"{wt}k", b"7"], Expect::Ok),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"GET", b"{wt}k"], Expect::Simple("QUEUED")),
                s(&[b"EXEC"], Expect::Arr(vec![Expect::Str(b"7")])),
                // Inside a transaction UNWATCH is QUEUED, not run (BUG-0220):
                // the watch it would drop is the one EXEC checks, so a broken
                // watch still aborts. Without a watch, EXEC answers OK for it.
                s(&[b"WATCH", b"{wt}k"], Expect::Ok),
                s(&[b"SET", b"{wt}k", b"7"], Expect::Ok),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"UNWATCH"], Expect::Simple("QUEUED")),
                s(&[b"SET", b"{wt}k", b"lost"], Expect::Simple("QUEUED")),
                s(&[b"EXEC"], Expect::NilArray),
                s(&[b"GET", b"{wt}k"], Expect::Str(b"7")),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"UNWATCH"], Expect::Simple("QUEUED")),
                s(&[b"EXEC"], Expect::Arr(vec![Expect::Ok])),
                s(&[b"UNWATCH", b"x"], Expect::AnyError),
                // DISCARD clears watches as well as the queue.
                s(&[b"WATCH", b"{wt}k"], Expect::Ok),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"DISCARD"], Expect::Ok),
                s(&[b"SET", b"{wt}k", b"8"], Expect::Ok),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"GET", b"{wt}k"], Expect::Simple("QUEUED")),
                s(&[b"EXEC"], Expect::Arr(vec![Expect::Str(b"8")])),
                // A watched COLLECTION: the mutation lands in rows a watch
                // does not track, so this is what proves the watch follows
                // the metadata row every type updates.
                s(&[b"SADD", b"{wt}s", b"a"], Expect::Int(1)),
                s(&[b"WATCH", b"{wt}s"], Expect::Ok),
                s(&[b"SADD", b"{wt}s", b"b"], Expect::Int(1)),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"SCARD", b"{wt}s"], Expect::Simple("QUEUED")),
                s(&[b"EXEC"], Expect::NilArray),
                // WATCH is refused inside a transaction; UNWATCH outside one
                // is fine.
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"WATCH", b"{wt}k"], Expect::AnyError),
                s(&[b"DISCARD"], Expect::Ok),
                s(&[b"UNWATCH"], Expect::Ok),
                s(&[b"WATCH"], Expect::AnyError),
                s(&[b"DEL", b"{wt}k", b"{wt}s"], Expect::Int(2)),
            ],
        },
        Case {
            family: "connection",
            name: "select",
            steps: vec![
                // Index 0 is the database every connection is already in.
                s(&[b"SELECT", b"0"], Expect::Ok),
                // 99 is out of range on BOTH sides, so it is a shared
                // assertion. A small nonzero index is deliberately absent:
                // a stock Valkey has sixteen databases and would answer OK
                // to SELECT 7, while a namespace here has one — asserting
                // that against the oracle would be asserting the divergence,
                // not the behaviour.
                s(&[b"SELECT", b"99"], Expect::AnyError),
                s(&[b"SELECT", b"abc"], Expect::AnyError),
                s(&[b"SELECT"], Expect::AnyError),
            ],
        },
        Case {
            family: "zsets",
            name: "zrangebylex zrevrangebylex",
            steps: vec![
                // Every member at score 0: the lex family is only defined
                // over a uniformly-scored set, so a corpus with mixed scores
                // would be asserting behaviour upstream calls undefined.
                s(
                    &[
                        b"ZADD", b"zlex", b"0", b"a", b"0", b"b", b"0", b"c", b"0", b"d",
                    ],
                    Expect::Int(4),
                ),
                s(
                    &[b"ZRANGEBYLEX", b"zlex", b"-", b"+"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                        Expect::Str(b"d"),
                    ]),
                ),
                s(
                    &[b"ZRANGEBYLEX", b"zlex", b"[b", b"[c"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"c")]),
                ),
                // Exclusive on both ends leaves the interior only.
                s(
                    &[b"ZRANGEBYLEX", b"zlex", b"(a", b"(d"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"c")]),
                ),
                // A bare `[` is the inclusive EMPTY string, which sorts
                // below every member — not a malformed token.
                s(
                    &[b"ZRANGEBYLEX", b"zlex", b"[", b"[b"],
                    Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"b")]),
                ),
                s(
                    &[b"ZRANGEBYLEX", b"zlex", b"-", b"+", b"LIMIT", b"1", b"2"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"c")]),
                ),
                // Negative count means "to the end", as in the score forms.
                s(
                    &[b"ZRANGEBYLEX", b"zlex", b"-", b"+", b"LIMIT", b"2", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"c"), Expect::Str(b"d")]),
                ),
                s(
                    &[b"ZREVRANGEBYLEX", b"zlex", b"+", b"-"],
                    Expect::Arr(vec![
                        Expect::Str(b"d"),
                        Expect::Str(b"c"),
                        Expect::Str(b"b"),
                        Expect::Str(b"a"),
                    ]),
                ),
                // The reversed form takes (max, min): arguments in ascending
                // order return nothing rather than silently being reordered.
                s(
                    &[b"ZREVRANGEBYLEX", b"zlex", b"[c", b"[b"],
                    Expect::Arr(vec![Expect::Str(b"c"), Expect::Str(b"b")]),
                ),
                s(
                    &[b"ZREVRANGEBYLEX", b"zlex", b"-", b"+"],
                    Expect::Arr(vec![]),
                ),
                // An inverted range is empty, not an error.
                s(&[b"ZRANGEBYLEX", b"zlex", b"+", b"-"], Expect::Arr(vec![])),
                s(
                    &[b"ZRANGEBYLEX", b"nosuchz", b"-", b"+"],
                    Expect::Arr(vec![]),
                ),
                // A bound with no [ or ( prefix is malformed. This is the
                // case that makes bare `-`/`+` special rather than general.
                s(&[b"ZRANGEBYLEX", b"zlex", b"b", b"d"], Expect::AnyError),
                s(
                    &[b"ZRANGEBYLEX", b"zlex", b"-", b"+", b"WITHSCORES"],
                    Expect::AnyError,
                ),
                s(&[b"ZRANGEBYLEX", b"zlex", b"-"], Expect::AnyError),
                s(&[b"SET", b"zlexstr", b"x"], Expect::Ok),
                s(&[b"ZRANGEBYLEX", b"zlexstr", b"-", b"+"], Expect::AnyError),
                s(&[b"DEL", b"zlex", b"zlexstr"], Expect::Int(2)),
            ],
        },
        Case {
            family: "strings",
            name: "getex",
            steps: vec![
                s(&[b"SET", b"gx", b"v", b"EX", b"100"], Expect::Ok),
                // No option is a plain GET: the TTL must survive untouched.
                // Getting this backwards would make every GETEX a PERSIST.
                s(&[b"GETEX", b"gx"], Expect::Str(b"v")),
                s(&[b"TTL", b"gx"], Expect::IntRange(95, 100)),
                s(&[b"GETEX", b"gx", b"PERSIST"], Expect::Str(b"v")),
                s(&[b"TTL", b"gx"], Expect::Int(-1)),
                s(&[b"GETEX", b"gx", b"EX", b"50"], Expect::Str(b"v")),
                s(&[b"TTL", b"gx"], Expect::IntRange(45, 50)),
                s(&[b"GETEX", b"gx", b"PX", b"80000"], Expect::Str(b"v")),
                s(&[b"TTL", b"gx"], Expect::IntRange(75, 80)),
                // PERSIST after a TTL, then confirm a second bare GETEX does
                // not reintroduce one.
                s(&[b"GETEX", b"gx", b"PERSIST"], Expect::Str(b"v")),
                s(&[b"GETEX", b"gx"], Expect::Str(b"v")),
                s(&[b"TTL", b"gx"], Expect::Int(-1)),
                s(&[b"GETEX", b"gxmissing"], Expect::Nil),
                s(&[b"GETEX", b"gx", b"EX", b"0"], Expect::AnyError),
                // Two expiry options contradict; last-one-wins would half
                // apply a command the client did not mean.
                s(
                    &[b"GETEX", b"gx", b"EX", b"60", b"PERSIST"],
                    Expect::AnyError,
                ),
                s(&[b"GETEX", b"gx", b"BOGUS"], Expect::AnyError),
                s(&[b"GETEX"], Expect::AnyError),
                s(&[b"RPUSH", b"gxlist", b"a"], Expect::Int(1)),
                s(&[b"GETEX", b"gxlist"], Expect::AnyError),
                s(&[b"DEL", b"gx", b"gxlist"], Expect::Int(2)),
            ],
        },
        Case {
            family: "keyspace",
            name: "copy (same slot)",
            steps: vec![
                s(&[b"SET", b"{cp}s", b"v", b"EX", b"100"], Expect::Ok),
                s(&[b"COPY", b"{cp}s", b"{cp}d"], Expect::Int(1)),
                s(&[b"GET", b"{cp}d"], Expect::Str(b"v")),
                // The TTL travels with the value.
                s(&[b"TTL", b"{cp}d"], Expect::IntRange(95, 100)),
                // An occupied destination refuses without REPLACE.
                s(&[b"SET", b"{cp}e", b"old"], Expect::Ok),
                s(&[b"COPY", b"{cp}s", b"{cp}e"], Expect::Int(0)),
                s(&[b"GET", b"{cp}e"], Expect::Str(b"old")),
                s(&[b"COPY", b"{cp}s", b"{cp}e", b"REPLACE"], Expect::Int(1)),
                s(&[b"GET", b"{cp}e"], Expect::Str(b"v")),
                s(&[b"COPY", b"{cp}missing", b"{cp}x"], Expect::Int(0)),
                // Copying a key onto itself is an error, not a quiet 0.
                s(&[b"COPY", b"{cp}s", b"{cp}s"], Expect::AnyError),
                // DB 0 is the database the client is already in, so clients
                // that send it explicitly are accommodated. (A nonzero index
                // is deliberately NOT in this corpus: Valkey has sixteen
                // databases and would copy into one, while a namespace here
                // has exactly one — the divergence is by design, so asserting
                // it against the oracle would assert the wrong thing.)
                s(&[b"COPY", b"{cp}s", b"{cp}db", b"DB", b"0"], Expect::Int(1)),
                s(&[b"COPY", b"{cp}s", b"{cp}y", b"BOGUS"], Expect::AnyError),
                s(&[b"COPY", b"{cp}s"], Expect::AnyError),
                // Every collection type, because each keeps its contents in
                // a different shape of row and a copy that forgets one of
                // them is not visible from the string case at all.
                s(&[b"RPUSH", b"{cp}L", b"a", b"b", b"c"], Expect::Int(3)),
                s(&[b"COPY", b"{cp}L", b"{cp}L2"], Expect::Int(1)),
                s(
                    &[b"LRANGE", b"{cp}L2", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                    ]),
                ),
                s(
                    &[b"HSET", b"{cp}H", b"f1", b"v1", b"f2", b"v2"],
                    Expect::Int(2),
                ),
                s(&[b"COPY", b"{cp}H", b"{cp}H2"], Expect::Int(1)),
                s(
                    &[b"HGETALL", b"{cp}H2"],
                    Expect::UnorderedPairs(vec![(b"f1", b"v1"), (b"f2", b"v2")]),
                ),
                s(&[b"SADD", b"{cp}S", b"m1", b"m2"], Expect::Int(2)),
                s(&[b"COPY", b"{cp}S", b"{cp}S2"], Expect::Int(1)),
                s(
                    &[b"SMEMBERS", b"{cp}S2"],
                    Expect::UnorderedStrs(vec![b"m1", b"m2"]),
                ),
                s(&[b"ZADD", b"{cp}Z", b"1", b"a", b"2", b"b"], Expect::Int(2)),
                s(&[b"COPY", b"{cp}Z", b"{cp}Z2"], Expect::Int(1)),
                // Both zset row families must have come across: ZSCORE reads
                // the member rows, ZRANGE reads the score index. A copy that
                // moved only one would pass exactly one of these.
                s(&[b"ZSCORE", b"{cp}Z2", b"b"], Expect::Str(b"2")),
                s(
                    &[b"ZRANGE", b"{cp}Z2", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"b")]),
                ),
                // The copy is independent, not an alias: writing to one and
                // deleting the other must leave the survivor whole.
                s(&[b"RPUSH", b"{cp}L2", b"zz"], Expect::Int(4)),
                s(
                    &[b"LRANGE", b"{cp}L", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                    ]),
                ),
                s(&[b"DEL", b"{cp}L"], Expect::Int(1)),
                s(
                    &[b"LRANGE", b"{cp}L2", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                        Expect::Str(b"zz"),
                    ]),
                ),
                s(
                    &[
                        b"DEL", b"{cp}s", b"{cp}d", b"{cp}e", b"{cp}db", b"{cp}L2", b"{cp}H",
                        b"{cp}H2", b"{cp}S", b"{cp}S2", b"{cp}Z", b"{cp}Z2",
                    ],
                    Expect::Int(11),
                ),
            ],
        },
        Case {
            family: "sets",
            name: "sadd srem sismember smembers scard",
            steps: vec![
                s(&[b"SADD", b"s1", b"a", b"b", b"a"], Expect::Int(2)),
                s(&[b"SADD", b"s1", b"b", b"c"], Expect::Int(1)),
                s(&[b"SCARD", b"s1"], Expect::Int(3)),
                s(&[b"SISMEMBER", b"s1", b"a"], Expect::Int(1)),
                s(&[b"SISMEMBER", b"s1", b"zz"], Expect::Int(0)),
                s(
                    &[b"SMEMBERS", b"s1"],
                    Expect::UnorderedStrs(vec![b"a", b"b", b"c"]),
                ),
                s(&[b"SMEMBERS", b"nosuch"], Expect::UnorderedStrs(vec![])),
                s(&[b"SREM", b"s1", b"a", b"zz"], Expect::Int(1)),
                s(&[b"SREM", b"s1", b"b", b"c"], Expect::Int(2)),
                s(&[b"EXISTS", b"s1"], Expect::Int(0)),
                s(&[b"TYPE", b"s1"], Expect::Simple("none")),
            ],
        },
        Case {
            family: "sets",
            name: "spop srandmember deterministic shapes",
            steps: vec![
                // A one-member set pins the random pick.
                s(&[b"SADD", b"sp1", b"a"], Expect::Int(1)),
                s(&[b"SRANDMEMBER", b"sp1"], Expect::Str(b"a")),
                s(
                    &[b"SRANDMEMBER", b"sp1", b"5"],
                    Expect::Arr(vec![Expect::Str(b"a")]),
                ),
                s(
                    &[b"SRANDMEMBER", b"sp1", b"-3"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"a"),
                        Expect::Str(b"a"),
                    ]),
                ),
                s(&[b"SRANDMEMBER", b"sp1", b"0"], Expect::Arr(vec![])),
                s(&[b"SRANDMEMBER", b"nosuchs"], Expect::Nil),
                s(&[b"SRANDMEMBER", b"nosuchs", b"3"], Expect::Arr(vec![])),
                s(&[b"SPOP", b"sp1"], Expect::Str(b"a")),
                s(&[b"EXISTS", b"sp1"], Expect::Int(0)),
                s(&[b"SPOP", b"nosuchs"], Expect::Nil),
                s(&[b"SPOP", b"nosuchs", b"2"], Expect::Arr(vec![])),
                // Over-count pops everything — compare unordered.
                s(&[b"SADD", b"sp2", b"a", b"b", b"c"], Expect::Int(3)),
                s(
                    &[b"SPOP", b"sp2", b"5"],
                    Expect::UnorderedStrs(vec![b"a", b"b", b"c"]),
                ),
                s(&[b"EXISTS", b"sp2"], Expect::Int(0)),
                s(&[b"SADD", b"sp3", b"x"], Expect::Int(1)),
                s(&[b"SPOP", b"sp3", b"-1"], Expect::AnyError),
            ],
        },
        Case {
            family: "strings",
            name: "incrbyfloat human formatting",
            steps: vec![
                // Dyadic increments are exact in binary floating point, so
                // the human formatting is deterministic cross-platform.
                s(&[b"INCRBYFLOAT", b"fb1", b"10.5"], Expect::Str(b"10.5")),
                s(&[b"INCRBYFLOAT", b"fb1", b"0.25"], Expect::Str(b"10.75")),
                s(&[b"INCRBYFLOAT", b"fb1", b"-0.75"], Expect::Str(b"10")),
                s(&[b"GET", b"fb1"], Expect::Str(b"10")),
                // Exponent-form stored values parse; output is human form.
                s(&[b"SET", b"fb2", b"3.0e3"], Expect::Ok),
                s(&[b"INCRBYFLOAT", b"fb2", b"200"], Expect::Str(b"3200")),
                s(&[b"SET", b"fb3", b"hello"], Expect::Ok),
                s(&[b"INCRBYFLOAT", b"fb3", b"1"], Expect::AnyError),
                s(&[b"INCRBYFLOAT", b"fb4", b"notafloat"], Expect::AnyError),
                s(&[b"SET", b"fb5", b"inf"], Expect::Ok),
                s(&[b"INCRBYFLOAT", b"fb5", b"1"], Expect::AnyError),
            ],
        },
        Case {
            family: "hashes",
            name: "hscan single shot with match and novalues",
            steps: vec![
                s(
                    &[b"HSET", b"hs1", b"f1", b"v1", b"f2", b"v2", b"g1", b"v3"],
                    Expect::Int(3),
                ),
                s(
                    &[b"HSCAN", b"hs1", b"0"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedPairs(vec![
                            (b"f1", b"v1"),
                            (b"f2", b"v2"),
                            (b"g1", b"v3"),
                        ]),
                    ]),
                ),
                s(
                    &[b"HSCAN", b"hs1", b"0", b"MATCH", b"f*"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedPairs(vec![(b"f1", b"v1"), (b"f2", b"v2")]),
                    ]),
                ),
                s(
                    &[b"HSCAN", b"hs1", b"0", b"COUNT", b"100", b"NOVALUES"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"f1", b"f2", b"g1"]),
                    ]),
                ),
                s(
                    &[b"HSCAN", b"nosuchh", b"0"],
                    Expect::Arr(vec![Expect::Str(b"0"), Expect::Arr(vec![])]),
                ),
                s(&[b"HSCAN", b"hs1", b"notanumber"], Expect::AnyError),
                s(&[b"HSCAN", b"hs1", b"0", b"MATCH"], Expect::AnyError),
            ],
        },
        Case {
            family: "sets",
            name: "sscan single shot with match",
            steps: vec![
                s(&[b"SADD", b"ss1", b"a", b"ab", b"b"], Expect::Int(3)),
                s(
                    &[b"SSCAN", b"ss1", b"0"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"a", b"ab", b"b"]),
                    ]),
                ),
                s(
                    &[b"SSCAN", b"ss1", b"0", b"MATCH", b"a*"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"a", b"ab"]),
                    ]),
                ),
                s(
                    &[b"SSCAN", b"ss1", b"0", b"MATCH", b"?"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"a", b"b"]),
                    ]),
                ),
                s(
                    &[b"SSCAN", b"nosuchs2", b"0"],
                    Expect::Arr(vec![Expect::Str(b"0"), Expect::Arr(vec![])]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "zscan single shot with match",
            steps: vec![
                s(&[b"ZADD", b"zs1", b"1", b"a", b"2", b"b"], Expect::Int(2)),
                s(
                    &[b"ZSCAN", b"zs1", b"0"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedPairs(vec![(b"a", b"1"), (b"b", b"2")]),
                    ]),
                ),
                s(
                    &[b"ZSCAN", b"zs1", b"0", b"MATCH", b"b*"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedPairs(vec![(b"b", b"2")]),
                    ]),
                ),
                s(
                    &[b"ZSCAN", b"nosuchz", b"0"],
                    Expect::Arr(vec![Expect::Str(b"0"), Expect::Arr(vec![])]),
                ),
            ],
        },
        Case {
            family: "lists",
            name: "push pop order",
            steps: vec![
                s(&[b"RPUSH", b"l1", b"a", b"b"], Expect::Int(2)),
                s(&[b"LPUSH", b"l1", b"c"], Expect::Int(3)),
                s(&[b"TYPE", b"l1"], Expect::Simple("list")),
                s(
                    &[b"LRANGE", b"l1", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"c"),
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                    ]),
                ),
                s(&[b"LPOP", b"l1"], Expect::Str(b"c")),
                s(&[b"RPOP", b"l1"], Expect::Str(b"b")),
                s(&[b"LLEN", b"l1"], Expect::Int(1)),
                s(&[b"LPOP", b"l1"], Expect::Str(b"a")),
                s(&[b"EXISTS", b"l1"], Expect::Int(0)),
                s(&[b"LPOP", b"l1"], Expect::Nil),
            ],
        },
        Case {
            family: "lists",
            name: "lpush multi reverses",
            steps: vec![
                s(&[b"LPUSH", b"l2", b"a", b"b", b"c"], Expect::Int(3)),
                s(
                    &[b"LRANGE", b"l2", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"c"),
                        Expect::Str(b"b"),
                        Expect::Str(b"a"),
                    ]),
                ),
            ],
        },
        Case {
            family: "lists",
            name: "lrange negative indices and clamping",
            steps: vec![
                s(&[b"RPUSH", b"l3", b"a", b"b", b"c", b"d"], Expect::Int(4)),
                s(
                    &[b"LRANGE", b"l3", b"-2", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"c"), Expect::Str(b"d")]),
                ),
                s(
                    &[b"LRANGE", b"l3", b"0", b"99"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                        Expect::Str(b"d"),
                    ]),
                ),
                s(&[b"LRANGE", b"l3", b"3", b"1"], Expect::Arr(vec![])),
                s(&[b"LRANGE", b"nosuch", b"0", b"-1"], Expect::Arr(vec![])),
            ],
        },
        Case {
            family: "lists",
            name: "lset overwrite and range errors",
            steps: vec![
                s(&[b"RPUSH", b"l4", b"a", b"b", b"c"], Expect::Int(3)),
                s(&[b"LSET", b"l4", b"1", b"B"], Expect::Ok),
                s(&[b"LSET", b"l4", b"-1", b"C"], Expect::Ok),
                s(
                    &[b"LRANGE", b"l4", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"B"),
                        Expect::Str(b"C"),
                    ]),
                ),
                s(&[b"LSET", b"l4", b"3", b"x"], Expect::AnyError),
                s(&[b"LSET", b"l4", b"-4", b"x"], Expect::AnyError),
                s(&[b"LSET", b"nosuch", b"0", b"x"], Expect::AnyError),
            ],
        },
        Case {
            family: "lists",
            name: "ltrim keep window and empty deletes",
            steps: vec![
                s(
                    &[b"RPUSH", b"l5", b"a", b"b", b"c", b"d", b"e"],
                    Expect::Int(5),
                ),
                s(&[b"LTRIM", b"l5", b"1", b"-2"], Expect::Ok),
                s(
                    &[b"LRANGE", b"l5", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                        Expect::Str(b"d"),
                    ]),
                ),
                // Pushes after a trim behave normally.
                s(&[b"LPUSH", b"l5", b"z"], Expect::Int(4)),
                s(
                    &[b"LRANGE", b"l5", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"z"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                        Expect::Str(b"d"),
                    ]),
                ),
                // Inverted keep-range empties (and thus deletes) the key.
                s(&[b"LTRIM", b"l5", b"5", b"1"], Expect::Ok),
                s(&[b"EXISTS", b"l5"], Expect::Int(0)),
                // Missing key is still +OK.
                s(&[b"LTRIM", b"nosuch", b"0", b"-1"], Expect::Ok),
            ],
        },
        Case {
            family: "lists",
            name: "lpos rank count maxlen",
            steps: vec![
                s(
                    &[b"RPUSH", b"l6", b"a", b"b", b"c", b"a", b"b", b"c", b"a"],
                    Expect::Int(7),
                ),
                s(&[b"LPOS", b"l6", b"a"], Expect::Int(0)),
                s(&[b"LPOS", b"l6", b"a", b"RANK", b"2"], Expect::Int(3)),
                s(&[b"LPOS", b"l6", b"a", b"RANK", b"-1"], Expect::Int(6)),
                s(&[b"LPOS", b"l6", b"missing"], Expect::Nil),
                s(
                    &[b"LPOS", b"l6", b"a", b"COUNT", b"0"],
                    Expect::Arr(vec![Expect::Int(0), Expect::Int(3), Expect::Int(6)]),
                ),
                s(
                    &[b"LPOS", b"l6", b"a", b"RANK", b"-1", b"COUNT", b"2"],
                    Expect::Arr(vec![Expect::Int(6), Expect::Int(3)]),
                ),
                s(
                    &[b"LPOS", b"l6", b"a", b"COUNT", b"0", b"MAXLEN", b"2"],
                    Expect::Arr(vec![Expect::Int(0)]),
                ),
                s(
                    &[b"LPOS", b"l6", b"missing", b"COUNT", b"0"],
                    Expect::Arr(vec![]),
                ),
                s(&[b"LPOS", b"l6", b"a", b"RANK", b"0"], Expect::AnyError),
                s(&[b"LPOS", b"l6", b"a", b"COUNT", b"-1"], Expect::AnyError),
            ],
        },
        Case {
            family: "lists",
            name: "lrem counts and directions",
            steps: vec![
                s(
                    &[b"RPUSH", b"l7", b"a", b"b", b"a", b"c", b"a"],
                    Expect::Int(5),
                ),
                s(&[b"LREM", b"l7", b"1", b"a"], Expect::Int(1)),
                s(
                    &[b"LRANGE", b"l7", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"b"),
                        Expect::Str(b"a"),
                        Expect::Str(b"c"),
                        Expect::Str(b"a"),
                    ]),
                ),
                s(&[b"LREM", b"l7", b"-1", b"a"], Expect::Int(1)),
                s(
                    &[b"LRANGE", b"l7", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"b"),
                        Expect::Str(b"a"),
                        Expect::Str(b"c"),
                    ]),
                ),
                s(&[b"LREM", b"l7", b"0", b"a"], Expect::Int(1)),
                s(
                    &[b"LRANGE", b"l7", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"c")]),
                ),
                s(&[b"LREM", b"l7", b"0", b"zz"], Expect::Int(0)),
                s(&[b"LREM", b"nosuchl", b"0", b"x"], Expect::Int(0)),
            ],
        },
        Case {
            family: "lists",
            name: "linsert before after and misses",
            steps: vec![
                s(&[b"RPUSH", b"l8", b"a", b"c"], Expect::Int(2)),
                s(&[b"LINSERT", b"l8", b"BEFORE", b"c", b"b"], Expect::Int(3)),
                s(&[b"LINSERT", b"l8", b"AFTER", b"c", b"d"], Expect::Int(4)),
                s(
                    &[b"LRANGE", b"l8", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                        Expect::Str(b"d"),
                    ]),
                ),
                s(
                    &[b"LINSERT", b"l8", b"BEFORE", b"zz", b"x"],
                    Expect::Int(-1),
                ),
                s(
                    &[b"LINSERT", b"nosuchl", b"BEFORE", b"a", b"b"],
                    Expect::Int(0),
                ),
                s(
                    &[b"LINSERT", b"l8", b"SIDEWAYS", b"a", b"b"],
                    Expect::AnyError,
                ),
                // Ends still behave after interior rewrites.
                s(&[b"RPUSH", b"l8", b"e"], Expect::Int(5)),
                s(&[b"LPOP", b"l8"], Expect::Str(b"a")),
                s(
                    &[b"LRANGE", b"l8", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                        Expect::Str(b"d"),
                        Expect::Str(b"e"),
                    ]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "a score spelled past a double's range is not a float",
            // strtod reports ERANGE for these and Valkey refuses them; Rust's
            // parser rounded them to inf and 0 and stored the member. Scores
            // parse as a double on every platform, unlike INCRBYFLOAT's long
            // double, so the reference answer does not depend on the oracle.
            steps: vec![
                s(&[b"ZADD", b"zr", b"1e400", b"m"], Expect::Err("ERR value is not a valid float")),
                s(&[b"ZADD", b"zr", b"-1e400", b"m"], Expect::Err("ERR value is not a valid float")),
                s(&[b"ZADD", b"zr", b"1e-400", b"m"], Expect::Err("ERR value is not a valid float")),
                s(&[b"ZINCRBY", b"zr", b"1e400", b"m"], Expect::Err("ERR value is not a valid float")),
                s(&[b"EXISTS", b"zr"], Expect::Int(0)),
                s(&[b"ZADD", b"zr", b"0.000e-400", b"zero", b"inf", b"top", b"-Infinity", b"bottom"], Expect::Int(3)),
                s(
                    &[b"ZRANGE", b"zr", b"0", b"-1", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"bottom"),
                        Expect::Str(b"-inf"),
                        Expect::Str(b"zero"),
                        Expect::Str(b"0"),
                        Expect::Str(b"top"),
                        Expect::Str(b"inf"),
                    ]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "zadd zscore zrange ordering",
            steps: vec![
                s(
                    &[b"ZADD", b"z1", b"2", b"b", b"1", b"a", b"3", b"c"],
                    Expect::Int(3),
                ),
                s(&[b"ZSCORE", b"z1", b"b"], Expect::Str(b"2")),
                s(&[b"ZSCORE", b"z1", b"missing"], Expect::Nil),
                s(&[b"ZCARD", b"z1"], Expect::Int(3)),
                s(
                    &[b"ZRANGE", b"z1", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                    ]),
                ),
                s(
                    &[b"ZRANGE", b"z1", b"0", b"1", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"1"),
                        Expect::Str(b"b"),
                        Expect::Str(b"2"),
                    ]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "score update reorders without double count",
            steps: vec![
                s(&[b"ZADD", b"z2", b"1", b"a", b"2", b"b"], Expect::Int(2)),
                s(&[b"ZADD", b"z2", b"5", b"a"], Expect::Int(0)),
                s(&[b"ZCARD", b"z2"], Expect::Int(2)),
                s(
                    &[b"ZRANGE", b"z2", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"a")]),
                ),
                s(&[b"ZSCORE", b"z2", b"a"], Expect::Str(b"5")),
            ],
        },
        Case {
            family: "zsets",
            name: "zrem to empty removes key; decimal scores",
            steps: vec![
                s(&[b"ZADD", b"z3", b"1.5", b"a"], Expect::Int(1)),
                s(&[b"ZSCORE", b"z3", b"a"], Expect::Str(b"1.5")),
                s(&[b"ZREM", b"z3", b"a", b"zz"], Expect::Int(1)),
                s(&[b"EXISTS", b"z3"], Expect::Int(0)),
                s(&[b"ZADD", b"z3", b"nope", b"a"], Expect::AnyError),
            ],
        },
        Case {
            family: "protocol",
            name: "wrongtype across new families",
            steps: vec![
                s(&[b"SET", b"wt2-s", b"v"], Expect::Ok),
                s(&[b"SADD", b"wt2-s", b"m"], Expect::AnyError),
                s(&[b"LPUSH", b"wt2-s", b"m"], Expect::AnyError),
                s(&[b"ZADD", b"wt2-s", b"1", b"m"], Expect::AnyError),
                s(&[b"RPUSH", b"wt2-l", b"x"], Expect::Int(1)),
                s(&[b"GET", b"wt2-l"], Expect::AnyError),
                s(&[b"SMEMBERS", b"wt2-l"], Expect::AnyError),
                s(&[b"HGET", b"wt2-l", b"f"], Expect::AnyError),
                s(&[b"DEL", b"wt2-l"], Expect::Int(1)),
            ],
        },
        Case {
            family: "strings",
            name: "mset mget with missing and wrongtype",
            // COLOCATED UNDER A HASH TAG, exactly as the set-op cases above
            // are, and for the same reason. Flint refuses cross-slot
            // multi-key requests (BUG-0053); the oracle is a STANDALONE
            // valkey, which has no slots and answers them happily. Bare
            // m1/m2/m3/nosuch land in four different slots (6916, 11111,
            // 15174, 14872), so this case would compare Flint's deliberate
            // CROSSSLOT refusal against the oracle's answer and report a
            // divergence that is the documented design.
            //
            // What the case is FOR — a miss and a wrongtype both yielding
            // nil, in position — is untouched by colocating the keys.
            steps: vec![
                s(&[b"MSET", b"{m}1", b"a", b"{m}2", b"b"], Expect::Ok),
                s(&[b"RPUSH", b"{m}3", b"x"], Expect::Int(1)),
                s(
                    &[b"MGET", b"{m}1", b"{m}nosuch", b"{m}2", b"{m}3"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Nil,
                        Expect::Str(b"b"),
                        Expect::Nil,
                    ]),
                ),
                s(&[b"MSET", b"m4"], Expect::AnyError),
            ],
        },
        Case {
            family: "hashes",
            name: "hincrby",
            steps: vec![
                s(&[b"HINCRBY", b"hi", b"f", b"5"], Expect::Int(5)),
                s(&[b"HINCRBY", b"hi", b"f", b"-2"], Expect::Int(3)),
                s(&[b"HGET", b"hi", b"f"], Expect::Str(b"3")),
                s(&[b"HSET", b"hi", b"txt", b"abc"], Expect::Int(1)),
                s(&[b"HINCRBY", b"hi", b"txt", b"1"], Expect::AnyError),
            ],
        },
        Case {
            family: "zsets",
            name: "zincrby creates and reorders",
            steps: vec![
                s(&[b"ZADD", b"zi", b"5", b"a"], Expect::Int(1)),
                s(&[b"ZINCRBY", b"zi", b"3", b"a"], Expect::Str(b"8")),
                s(&[b"ZINCRBY", b"zi", b"2", b"new"], Expect::Str(b"2")),
                s(&[b"ZCARD", b"zi"], Expect::Int(2)),
                s(
                    &[b"ZRANGE", b"zi", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"new"), Expect::Str(b"a")]),
                ),
            ],
        },
        Case {
            family: "lists",
            name: "lindex",
            steps: vec![
                s(&[b"RPUSH", b"li", b"a", b"b", b"c"], Expect::Int(3)),
                s(&[b"LINDEX", b"li", b"0"], Expect::Str(b"a")),
                s(&[b"LINDEX", b"li", b"-1"], Expect::Str(b"c")),
                s(&[b"LINDEX", b"li", b"99"], Expect::Nil),
                s(&[b"LINDEX", b"li", b"-99"], Expect::Nil),
                s(&[b"LINDEX", b"nosuch", b"0"], Expect::Nil),
            ],
        },
        Case {
            family: "keyspace",
            name: "dbsize counts live keys",
            steps: vec![
                s(&[b"DBSIZE"], Expect::Int(0)),
                s(&[b"SET", b"d1", b"v"], Expect::Ok),
                s(&[b"SET", b"d2", b"v"], Expect::Ok),
                s(&[b"HSET", b"d3", b"f", b"v"], Expect::Int(1)),
                s(&[b"DBSIZE"], Expect::Int(3)),
                s(&[b"DEL", b"d1"], Expect::Int(1)),
                s(&[b"DBSIZE"], Expect::Int(2)),
            ],
        },
        // --- Tier-1 command coverage (validated vs Valkey) ---
        Case {
            family: "strings",
            name: "getdel returns and removes",
            steps: vec![
                s(&[b"SET", b"gd1", b"v"], Expect::Ok),
                s(&[b"GETDEL", b"gd1"], Expect::Str(b"v")),
                s(&[b"GET", b"gd1"], Expect::Nil),
                s(&[b"GETDEL", b"gd_missing"], Expect::Nil),
            ],
        },
        Case {
            family: "strings",
            name: "getset returns old",
            steps: vec![
                s(&[b"GETSET", b"gs1", b"a"], Expect::Nil),
                s(&[b"GETSET", b"gs1", b"b"], Expect::Str(b"a")),
                s(&[b"GET", b"gs1"], Expect::Str(b"b")),
            ],
        },
        Case {
            family: "strings",
            name: "set with get option",
            steps: vec![
                s(&[b"SET", b"sg1", b"a", b"GET"], Expect::Nil),
                s(&[b"SET", b"sg1", b"b", b"GET"], Expect::Str(b"a")),
                s(&[b"GET", b"sg1"], Expect::Str(b"b")),
                // NX + GET: write rejected, old value still returned.
                s(&[b"SET", b"sg1", b"c", b"NX", b"GET"], Expect::Str(b"b")),
                s(&[b"GET", b"sg1"], Expect::Str(b"b")),
            ],
        },
        Case {
            family: "keyspace",
            name: "expireat and expiretime",
            steps: vec![
                s(&[b"SET", b"ea1", b"v"], Expect::Ok),
                // Far-future absolute second; TTL reads back ~that window.
                s(&[b"EXPIREAT", b"ea1", b"9999999999"], Expect::Int(1)),
                s(&[b"EXPIRETIME", b"ea1"], Expect::Int(9999999999)),
                // A past instant deletes.
                s(&[b"SET", b"ea2", b"v"], Expect::Ok),
                s(&[b"EXPIREAT", b"ea2", b"1"], Expect::Int(1)),
                s(&[b"GET", b"ea2"], Expect::Nil),
                // No-expiry / missing sentinels.
                s(&[b"SET", b"ea3", b"v"], Expect::Ok),
                s(&[b"EXPIRETIME", b"ea3"], Expect::Int(-1)),
                s(&[b"EXPIRETIME", b"ea_missing"], Expect::Int(-2)),
            ],
        },
        // The server answers these and, until 2026-09-05, no corpus case did.
        // They are why `docs/command-support.md` could open by saying "every
        // supported command is gated" while six were not; the gate check that
        // now enforces that sentence is what keeps this list honest.
        Case {
            family: "connection",
            name: "flushall empties the keyspace",
            steps: vec![
                // Every case already FLUSHALLs in the harness preamble, so
                // the +OK was well covered and the EFFECT was not covered at
                // all -- a FLUSHALL that returned OK and deleted nothing
                // would have passed the whole corpus, while silently making
                // every other case depend on the case before it.
                s(&[b"SET", b"fa1", b"v"], Expect::Ok),
                s(&[b"SADD", b"fa2", b"m"], Expect::Int(1)),
                s(&[b"FLUSHALL"], Expect::Ok),
                s(&[b"GET", b"fa1"], Expect::Nil),
                s(&[b"EXISTS", b"fa2"], Expect::Int(0)),
                s(&[b"DBSIZE"], Expect::Int(0)),
            ],
        },
        // BUG-0178: FLUSHDB was an unknown command, so framework cache
        // stores' clear() failed (Django raised; Rails swallowed it and
        // cleared nothing). The EFFECT is asserted, for the reason the case
        // above gives, and the ASYNC form Redis also accepts.
        Case {
            family: "connection",
            name: "flushdb empties the keyspace, sync and async",
            steps: vec![
                s(&[b"SET", b"fd1", b"v"], Expect::Ok),
                s(&[b"SADD", b"fd2", b"m"], Expect::Int(1)),
                s(&[b"FLUSHDB"], Expect::Ok),
                s(&[b"GET", b"fd1"], Expect::Nil),
                s(&[b"EXISTS", b"fd2"], Expect::Int(0)),
                s(&[b"SET", b"fd3", b"v"], Expect::Ok),
                s(&[b"FLUSHDB", b"ASYNC"], Expect::Ok),
                s(&[b"DBSIZE"], Expect::Int(0)),
            ],
        },
        // BUG-0213: a flush with an argument it does not know flushed. Redis
        // refuses it and flushes nothing.
        Case {
            family: "connection",
            name: "a flush with an unknown argument flushes nothing",
            steps: vec![
                s(&[b"SET", b"fu1", b"v"], Expect::Ok),
                s(&[b"FLUSHALL", b"FOO"], Expect::Err("ERR syntax error")),
                s(&[b"FLUSHDB", b"ASYNC", b"SYNC"], Expect::Err("ERR syntax error")),
                s(&[b"GET", b"fu1"], Expect::Str(b"v")),
                s(&[b"FLUSHALL", b"sync"], Expect::Ok),
                s(&[b"EXISTS", b"fu1"], Expect::Int(0)),
            ],
        },
        Case {
            family: "flint",
            name: "flintinfo reports the fields operators read",
            steps: vec![
                // Numbers here are per-host and per-second; the FIELDS are
                // the contract. `loading` in particular is the documented way
                // to tell a seat that has BOUND from one that is READY, and
                // PING cannot answer that -- a loading seat still says PONG.
                s(&[b"FLINTINFO"], Expect::StrContains(b"role:")),
                s(&[b"FLINTINFO"], Expect::StrContains(b"loading:")),
                // The three fields BOTH engines owe. The rocks build adds
                // some eighty more; asserting one of those here would make
                // this case a test of which engine it happened to reach.
                // `live_replicas` is asserted by NAME on purpose: the mem
                // build spelled it `live_replica` with a 0/1 until
                // 2026-09-05, which is the spelling no consumer reads.
                s(&[b"FLINTINFO"], Expect::StrContains(b"live_replicas:")),
            ],
        },
        Case {
            family: "flint",
            name: "flintkeysize measures payload, not encoding",
            steps: vec![
                s(&[b"SET", b"fks1", b"hello"], Expect::Ok),
                s(&[b"FLINTKEYSIZE", b"fks1"], Expect::Int(5)),
                // A collection reports CUMULATIVE MEMBER BYTES: two 1-byte
                // members are 2, not the row overhead and not the count.
                s(&[b"SADD", b"fks2", b"a", b"b"], Expect::Int(2)),
                s(&[b"FLINTKEYSIZE", b"fks2"], Expect::Int(2)),
                // Missing is nil, not 0 -- a cleanup daemon ranking by size
                // must be able to tell "empty" from "gone".
                s(&[b"FLINTKEYSIZE", b"fks_missing"], Expect::Nil),
            ],
        },
        Case {
            family: "flint",
            name: "flintkeystamp distinguishes written from created",
            steps: vec![
                // Real unix-ms instants, asserted as a range because the
                // alternative is asserting the clock. The lower bound is
                // 2023-11; a stamp below it is a unit error, not a slow test.
                s(&[b"SADD", b"fkt1", b"m"], Expect::Int(1)),
                s(
                    &[b"FLINTKEYSTAMP", b"fkt1"],
                    Expect::Arr(vec![
                        Expect::IntRange(1_700_000_000_000, 4_000_000_000_000),
                        Expect::IntRange(1_700_000_000_000, 4_000_000_000_000),
                    ]),
                ),
                // A payload-in-metadata type has no separate creation row, so
                // `created_ms` is 0 -- "not tracked", stated as a value rather
                // than guessed at by the caller.
                s(&[b"SET", b"fkt2", b"v"], Expect::Ok),
                s(
                    &[b"FLINTKEYSTAMP", b"fkt2"],
                    Expect::Arr(vec![
                        Expect::IntRange(1_700_000_000_000, 4_000_000_000_000),
                        Expect::Int(0),
                    ]),
                ),
            ],
        },
        Case {
            family: "connection",
            name: "command answers an array",
            steps: vec![
                // Asserted as a SHAPE, which is the part every server agrees
                // on: Valkey returns its whole command table, Flint returns
                // an empty array on purpose. Both are arrays, and the
                // regression worth catching is COMMAND becoming an error or
                // an unknown command -- which is what a client's capability
                // probe would hit. That Flint's array is EMPTY is a
                // divergence a client should know about, so it is written
                // down in docs/command-support.md rather than pinned here,
                // where pinning it would cost this case its oracle.
                s(&[b"COMMAND"], Expect::AnyArray),
            ],
        },
        Case {
            family: "keyspace",
            name: "pexpireat and pexpiretime are milliseconds",
            steps: vec![
                // THE UNIT IS THE ENTIRE CONTENT OF THESE TWO COMMANDS. Both
                // share an implementation with their second-granularity twins
                // and differ from them by one argument: the multiplier
                // (`cmd_expire_at(args, "pexpireat", 1)` against 1000). So
                // every assertion below CROSSES the units -- a millisecond
                // instant read back in seconds, then the reverse. A
                // PEXPIREAT/PEXPIRETIME round trip would not do: it passes
                // unchanged with BOTH multipliers swapped, because the second
                // conversion undoes the first. That mutation used to survive
                // the whole corpus and all 105 server unit tests.
                s(&[b"SET", b"pea1", b"v"], Expect::Ok),
                s(&[b"PEXPIREAT", b"pea1", b"9999999999000"], Expect::Int(1)),
                s(&[b"EXPIRETIME", b"pea1"], Expect::Int(9999999999)),
                // The other direction: written in seconds, read in ms.
                s(&[b"SET", b"pea2", b"v"], Expect::Ok),
                s(&[b"EXPIREAT", b"pea2", b"9999999999"], Expect::Int(1)),
                s(&[b"PEXPIRETIME", b"pea2"], Expect::Int(9999999999000)),
                // A past instant deletes, as EXPIREAT does. 1 ms after the
                // epoch is past; 1 SECOND after it is also past, so this step
                // alone cannot tell the units apart -- it is here for the
                // delete, not the unit.
                s(&[b"SET", b"pea3", b"v"], Expect::Ok),
                s(&[b"PEXPIREAT", b"pea3", b"1"], Expect::Int(1)),
                s(&[b"GET", b"pea3"], Expect::Nil),
                // Sentinels: a live key with no expiry, and a missing key.
                s(&[b"SET", b"pea4", b"v"], Expect::Ok),
                s(&[b"PEXPIRETIME", b"pea4"], Expect::Int(-1)),
                s(&[b"PEXPIRETIME", b"pea_missing"], Expect::Int(-2)),
            ],
        },
        Case {
            family: "keyspace",
            name: "unlink removes like del",
            steps: vec![
                s(&[b"SET", b"ul1", b"v"], Expect::Ok),
                s(&[b"SET", b"ul2", b"v"], Expect::Ok),
                s(&[b"UNLINK", b"ul1", b"ul2", b"ul_missing"], Expect::Int(2)),
                s(&[b"GET", b"ul1"], Expect::Nil),
            ],
        },
        Case {
            family: "hashes",
            name: "hsetnx and hstrlen",
            steps: vec![
                s(&[b"HSETNX", b"hx1", b"f", b"hello"], Expect::Int(1)),
                s(&[b"HSETNX", b"hx1", b"f", b"other"], Expect::Int(0)),
                s(&[b"HGET", b"hx1", b"f"], Expect::Str(b"hello")),
                s(&[b"HSTRLEN", b"hx1", b"f"], Expect::Int(5)),
                s(&[b"HSTRLEN", b"hx1", b"nofield"], Expect::Int(0)),
            ],
        },
        Case {
            family: "sets",
            name: "smismember batch membership",
            steps: vec![
                s(&[b"SADD", b"sm1", b"a", b"b"], Expect::Int(2)),
                s(
                    &[b"SMISMEMBER", b"sm1", b"a", b"x", b"b"],
                    Expect::Arr(vec![Expect::Int(1), Expect::Int(0), Expect::Int(1)]),
                ),
            ],
        },
        // --- Tier-2: sorted-set range family (validated vs Valkey) ---
        Case {
            family: "zsets",
            name: "zrangebyscore inclusive and exclusive",
            steps: vec![
                s(
                    &[b"ZADD", b"zr", b"1", b"a", b"2", b"b", b"3", b"c"],
                    Expect::Int(3),
                ),
                s(
                    &[b"ZRANGEBYSCORE", b"zr", b"1", b"2"],
                    Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"b")]),
                ),
                s(
                    &[b"ZRANGEBYSCORE", b"zr", b"(1", b"3"],
                    Expect::Arr(vec![Expect::Str(b"b"), Expect::Str(b"c")]),
                ),
                s(
                    &[b"ZRANGEBYSCORE", b"zr", b"-inf", b"+inf"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"b"),
                        Expect::Str(b"c"),
                    ]),
                ),
                s(
                    &[b"ZRANGEBYSCORE", b"zr", b"-inf", b"+inf", b"WITHSCORES"],
                    Expect::Arr(vec![
                        Expect::Str(b"a"),
                        Expect::Str(b"1"),
                        Expect::Str(b"b"),
                        Expect::Str(b"2"),
                        Expect::Str(b"c"),
                        Expect::Str(b"3"),
                    ]),
                ),
                s(
                    &[
                        b"ZRANGEBYSCORE",
                        b"zr",
                        b"-inf",
                        b"+inf",
                        b"LIMIT",
                        b"1",
                        b"1",
                    ],
                    Expect::Arr(vec![Expect::Str(b"b")]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "zrevrange and zrevrangebyscore",
            steps: vec![
                s(
                    &[b"ZADD", b"zv", b"1", b"a", b"2", b"b", b"3", b"c"],
                    Expect::Int(3),
                ),
                s(
                    &[b"ZREVRANGE", b"zv", b"0", b"-1"],
                    Expect::Arr(vec![
                        Expect::Str(b"c"),
                        Expect::Str(b"b"),
                        Expect::Str(b"a"),
                    ]),
                ),
                s(
                    &[b"ZREVRANGEBYSCORE", b"zv", b"3", b"2"],
                    Expect::Arr(vec![Expect::Str(b"c"), Expect::Str(b"b")]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "zrank zrevrank zcount zmscore",
            steps: vec![
                s(
                    &[b"ZADD", b"zk", b"10", b"a", b"20", b"b", b"30", b"c"],
                    Expect::Int(3),
                ),
                s(&[b"ZRANK", b"zk", b"a"], Expect::Int(0)),
                s(&[b"ZRANK", b"zk", b"c"], Expect::Int(2)),
                s(&[b"ZRANK", b"zk", b"missing"], Expect::Nil),
                s(&[b"ZREVRANK", b"zk", b"a"], Expect::Int(2)),
                s(&[b"ZCOUNT", b"zk", b"15", b"30"], Expect::Int(2)),
                s(&[b"ZCOUNT", b"zk", b"(10", b"(30"], Expect::Int(1)),
                s(
                    &[b"ZMSCORE", b"zk", b"a", b"nope", b"c"],
                    Expect::Arr(vec![Expect::Str(b"10"), Expect::Nil, Expect::Str(b"30")]),
                ),
            ],
        },
        Case {
            family: "zsets",
            name: "zpopmin zpopmax",
            steps: vec![
                s(
                    &[b"ZADD", b"zp", b"1", b"a", b"2", b"b", b"3", b"c"],
                    Expect::Int(3),
                ),
                s(
                    &[b"ZPOPMIN", b"zp"],
                    Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"1")]),
                ),
                s(
                    &[b"ZPOPMAX", b"zp", b"2"],
                    Expect::Arr(vec![
                        Expect::Str(b"c"),
                        Expect::Str(b"3"),
                        Expect::Str(b"b"),
                        Expect::Str(b"2"),
                    ]),
                ),
                s(&[b"ZCARD", b"zp"], Expect::Int(0)),
            ],
        },
        Case {
            family: "zsets",
            name: "zremrangebyscore and byrank",
            steps: vec![
                s(
                    &[
                        b"ZADD", b"zd", b"1", b"a", b"2", b"b", b"3", b"c", b"4", b"d",
                    ],
                    Expect::Int(4),
                ),
                s(&[b"ZREMRANGEBYSCORE", b"zd", b"2", b"3"], Expect::Int(2)),
                s(
                    &[b"ZRANGE", b"zd", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"d")]),
                ),
                s(&[b"ZREMRANGEBYRANK", b"zd", b"0", b"0"], Expect::Int(1)),
                s(
                    &[b"ZRANGE", b"zd", b"0", b"-1"],
                    Expect::Arr(vec![Expect::Str(b"d")]),
                ),
            ],
        },
        // Keyspace SCAN. Frame-comparable cases only: with COUNT >= the
        // keyspace size both Flint and Valkey complete in ONE batch with
        // cursor "0", so the reply is deterministic modulo key order.
        // Multi-batch pagination is validated by scan_drill.sh + unit
        // tests (batch boundaries and cursor VALUES legitimately differ:
        // Valkey cursors are reversed-bit bucket indexes, Flint cursors
        // are server-side session ids). Known, documented divergence NOT
        // tested here: Valkey accepts any integer as a cursor (a bucket
        // index); Flint answers "ERR invalid cursor" for a cursor it never
        // issued — invisible to real clients, which only echo cursors.
        // JSON documents. FLINT-ONLY (see `flint_only`): the reference has
        // no JSON type, so these cases assert the contract we chose, and
        // prove mem/rocks agree on it — not that we match another
        // implementation. Kept frame-comparable: with `preserve_order`,
        // object key order is insertion order, so replies are deterministic.
        //
        // The expectations below were checked reply-by-reply against the
        // real RedisJSON module (built from source, loaded into Redis 8.2),
        // so "the contract we chose" is a verified match rather than a
        // reading of the docs. The places we knowingly differ are called
        // out inline, listed in tools/redisjson_compare.sh and in
        // docs/command-support.md.
        // BUG-0234: the number is read as JSON, as RedisJSON reads it.
        // BUG-0236: JSON errors in RedisJSON's words, each family once.
        Case {
            family: "json",
            name: "json errors are redisjson's words",
            steps: vec![
                s(&[b"JSON.SET", b"{je}d", b"$", br#"{"a":1,"b":[1],"s":"x"}"#], Expect::Ok),
                s(&[b"JSON.GET", b"{je}d", b".x"], Expect::Err("ERR Path '$.x' does not exist")),
                s(&[b"JSON.ARRAPPEND", b"{je}d", b".a", b"1"], Expect::Err("ERR Path '.a' does not exist or not an array")),
                s(&[b"JSON.ARRLEN", b"{je}d", b".x"], Expect::Err("ERR Path '.x' does not exist")),
                s(&[b"JSON.ARRPOP", b"{je}d", b"a"], Expect::Err("ERR Path '$.a' does not exist or not an array")),
                s(
                    &[b"JSON.NUMINCRBY", b"{je}d", b".s", b"1"],
                    Expect::Err("ERR Path '$.s' does not exist or does not contains a number"),
                ),
                s(&[b"JSON.TOGGLE", b"{je}d", b".a"], Expect::Err("ERR Path '$.a' does not exist or not a bool")),
                s(&[b"JSON.STRAPPEND", b"{je}d", b".a", br#""y""#], Expect::Err("ERR Path '$.a' does not exist or not a string")),
                s(&[b"JSON.STRAPPEND", b"{je}d", b"$.s", b"1"], Expect::Err("WRONGTYPE wrong type of path value - expected string but found 1")),
                s(&[b"JSON.SET", b"{je}d", b"$.a", b"bad"], Expect::Err("expected value at line 1 column 1")),
                s(&[b"JSON.ARRINDEX", b"{je}d", b"$.b", b"bad"], Expect::Err("ERR expected value at line 1 column 1")),
                s(&[b"JSON.ARRINSERT", b"{je}d", b"$.b", b"x", b"1"], Expect::Err("Couldn't parse as integer")),
                s(&[b"JSON.SET", b"{je}d", b"$.b[5]", b"1"], Expect::Err("ERR array index out of range")),
                s(&[b"JSON.SET", b"{je}d", b"$..a", b"1", b"NX"], Expect::Err("Err wrong static path")),
                s(&[b"JSON.SET", b"{je}d", b"$", b"1", b"NX", b"XX"], Expect::Err("ERR syntax error")),
                s(&[b"JSON.TYPE", b"{je}d", b".["], Expect::Nil),
                s(&[b"SET", b"{je}str", b"v"], Expect::Ok),
                s(&[b"JSON.GET", b"{je}str", b"$["], Expect::Err("Existing key has wrong Redis type")),
                s(&[b"JSON.GET", b"{je}none", b"$["], Expect::Nil),
                s(&[b"JSON.OBJLEN", b"{je}none", b"$.a"], Expect::Err("ERR Path '$.a' does not exist or not an object")),
                s(&[b"JSON.MGET", b"{je}d", b"{je}none", b"$["], Expect::Arr(vec![Expect::Nil, Expect::Nil])),
            ],
        },
        Case {
            family: "json",
            name: "numincrby reads its number as json",
            steps: vec![
                s(&[b"JSON.SET", b"jn", b"$", br#"{"i":5}"#], Expect::Ok),
                s(&[b"JSON.NUMINCRBY", b"jn", b"$.i", b"+1"], Expect::Err("ERR expected value at line 1 column 1")),
                s(&[b"JSON.NUMINCRBY", b"jn", b"$.i", b"01"], Expect::Err("ERR invalid number at line 1 column 2")),
                s(&[b"JSON.NUMINCRBY", b"jn", b"$.i", b"true"], Expect::Err("bad input number")),
                s(&[b"JSON.GET", b"jn", b"$.i"], Expect::Str(b"[5]")),
                s(&[b"JSON.NUMINCRBY", b"jn", b"$.i", b" 1"], Expect::Str(b"[6]")),
                s(&[b"JSON.NUMINCRBY", b"jn", b"$.i", b"-0"], Expect::Str(b"[6.0]")),
                s(&[b"JSON.SET", b"jn", b"$.s", br#""x""#], Expect::Ok),
                s(&[b"JSON.NUMINCRBY", b"jn", b"$.s", b"+1"], Expect::Str(b"[null]")),
                s(&[b"JSON.NUMINCRBY", b"jn", b"$.none", b"true"], Expect::Str(b"[]")),
                s(&[b"JSON.SET", b"jn", b"$.e", b"1e308"], Expect::Ok),
                s(&[b"JSON.NUMMULTBY", b"jn", b"$.e", b"10"], Expect::Err("result is not a number")),
            ],
        },
        Case {
            family: "json",
            name: "document roundtrip: root set, path get, TYPE vocabulary",
            steps: vec![
                s(
                    &[
                        b"JSON.SET",
                        b"doc",
                        b"$",
                        br#"{"n":1,"s":"x","a":[1,2],"o":{"k":true}}"#,
                    ],
                    Expect::Ok,
                ),
                // RedisJSON's module type name (Jeff, 2026-10-08). Until
                // then this was a deliberate difference answering `json`,
                // and every step after it in this case went unchecked
                // against RedisJSON, because a case stops at its first
                // failing step.
                s(&[b"TYPE", b"doc"], Expect::Simple("ReJSON-RL")),
                // JSONPath dialect: every reply is a container of matches.
                s(
                    &[b"JSON.TYPE", b"doc", b"$"],
                    Expect::Arr(vec![Expect::Str(b"object")]),
                ),
                s(
                    &[b"JSON.TYPE", b"doc", b"$.n"],
                    Expect::Arr(vec![Expect::Str(b"integer")]),
                ),
                s(
                    &[b"JSON.TYPE", b"doc", b"$.s"],
                    Expect::Arr(vec![Expect::Str(b"string")]),
                ),
                s(
                    &[b"JSON.TYPE", b"doc", b"$.a"],
                    Expect::Arr(vec![Expect::Str(b"array")]),
                ),
                s(
                    &[b"JSON.TYPE", b"doc", b"$.o"],
                    Expect::Arr(vec![Expect::Str(b"object")]),
                ),
                s(
                    &[b"JSON.TYPE", b"doc", b"$.o.k"],
                    Expect::Arr(vec![Expect::Str(b"boolean")]),
                ),
                s(&[b"JSON.GET", b"doc", b"$.n"], Expect::Str(b"[1]")),
                s(&[b"JSON.GET", b"doc", b"$.s"], Expect::Str(br#"["x"]"#)),
                s(&[b"JSON.GET", b"doc", b"$.a"], Expect::Str(b"[[1,2]]")),
                // A path that matches nothing is an EMPTY container here —
                // not nil, not an error. A missing KEY is still nil, in
                // either dialect.
                s(&[b"JSON.GET", b"doc", b"$.nope"], Expect::Str(b"[]")),
                s(&[b"JSON.GET", b"ghost", b"$"], Expect::Nil),
                s(&[b"JSON.TYPE", b"ghost"], Expect::Nil),
            ],
        },
        // The dialect rule itself, held side by side: the SAME document and
        // the SAME paths, spelled two ways. This is the case that would
        // catch a regression where one command forgets to shape its reply.
        Case {
            family: "json",
            name: "path dialects: $ replies in containers, legacy replies bare",
            steps: vec![
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"n":1,"a":[1,2],"o":{}}"#],
                    Expect::Ok,
                ),
                // JSON.GET / NUMINCRBY carry matches inside the JSON they
                // return; TYPE / ARRLEN / ARRAPPEND use a RESP array.
                s(&[b"JSON.GET", b"d", b"$.n"], Expect::Str(b"[1]")),
                s(&[b"JSON.GET", b"d", b".n"], Expect::Str(b"1")),
                s(&[b"JSON.GET", b"d", b"n"], Expect::Str(b"1")),
                // No path at all is the legacy root: the document itself.
                s(
                    &[b"JSON.GET", b"d"],
                    Expect::Str(br#"{"n":1,"a":[1,2],"o":{}}"#),
                ),
                s(
                    &[b"JSON.GET", b"d", b"."],
                    Expect::Str(br#"{"n":1,"a":[1,2],"o":{}}"#),
                ),
                s(
                    &[b"JSON.GET", b"d", b"$"],
                    Expect::Str(br#"[{"n":1,"a":[1,2],"o":{}}]"#),
                ),
                s(
                    &[b"JSON.TYPE", b"d", b"$.n"],
                    Expect::Arr(vec![Expect::Str(b"integer")]),
                ),
                s(&[b"JSON.TYPE", b"d", b".n"], Expect::Str(b"integer")),
                s(
                    &[b"JSON.ARRLEN", b"d", b"$.a"],
                    Expect::Arr(vec![Expect::Int(2)]),
                ),
                s(&[b"JSON.ARRLEN", b"d", b".a"], Expect::Int(2)),
                s(
                    &[b"JSON.ARRAPPEND", b"d", b"$.a", b"3"],
                    Expect::Arr(vec![Expect::Int(3)]),
                ),
                s(&[b"JSON.ARRAPPEND", b"d", b".a", b"4"], Expect::Int(4)),
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$.n", b"1"],
                    Expect::Str(b"[2]"),
                ),
                s(&[b"JSON.NUMINCRBY", b"d", b".n", b"1"], Expect::Str(b"3")),
                // A path matching nothing: empty container vs error. Same
                // question, two dialects, two answers — this asymmetry IS
                // the contract, not an oversight.
                s(&[b"JSON.GET", b"d", b"$.gone"], Expect::Str(b"[]")),
                s(&[b"JSON.GET", b"d", b".gone"], Expect::AnyError),
                s(&[b"JSON.TYPE", b"d", b"$.gone"], Expect::Arr(vec![])),
                // TYPE is the exception: its legacy dialect answers nil, not
                // an error — "what type is this" / "nothing" is an answer.
                // RedisJSON does the same.
                s(&[b"JSON.TYPE", b"d", b".gone"], Expect::Nil),
                s(&[b"JSON.ARRLEN", b"d", b"$.gone"], Expect::Arr(vec![])),
                s(&[b"JSON.ARRLEN", b"d", b".gone"], Expect::AnyError),
                // A path that matches the WRONG SHAPE: a null element vs an
                // error. Under multi-match one bad match must not fail the
                // rest, which is why the container holds a null.
                s(
                    &[b"JSON.ARRLEN", b"d", b"$.o"],
                    Expect::Arr(vec![Expect::Nil]),
                ),
                s(&[b"JSON.ARRLEN", b"d", b".o"], Expect::AnyError),
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$.o", b"1"],
                    Expect::Str(b"[null]"),
                ),
                s(&[b"JSON.NUMINCRBY", b"d", b".o", b"1"], Expect::AnyError),
                // The document survived every one of those refusals.
                s(&[b"JSON.GET", b"d", b".a"], Expect::Str(b"[1,2,3,4]")),
            ],
        },
        Case {
            family: "json",
            name: "path writes: leaf create, array index, negative index",
            steps: vec![
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"a":[10,20,30]}"#],
                    Expect::Ok,
                ),
                s(&[b"JSON.SET", b"d", b"$.a[0]", b"99"], Expect::Ok),
                s(&[b"JSON.GET", b"d", b"$.a[0]"], Expect::Str(b"[99]")),
                // Negative index counts from the end, like Redis.
                s(&[b"JSON.GET", b"d", b"$.a[-1]"], Expect::Str(b"[30]")),
                s(&[b"JSON.SET", b"d", b"$.a[-1]", b"31"], Expect::Ok),
                s(&[b"JSON.GET", b"d", b"$.a"], Expect::Str(b"[[99,20,31]]")),
                // DIVERGENCE (deliberate): index == len appends here, where
                // RedisJSON refuses it in both dialects. Past the end is
                // refused either way, leaving no hole.
                s(&[b"JSON.SET", b"d", b"$.a[3]", b"40"], Expect::Ok),
                s(
                    &[b"JSON.GET", b"d", b"$.a"],
                    Expect::Str(b"[[99,20,31,40]]"),
                ),
                s(&[b"JSON.SET", b"d", b"$.a[9]", b"0"], Expect::AnyError),
                s(
                    &[b"JSON.GET", b"d", b"$.a"],
                    Expect::Str(b"[[99,20,31,40]]"),
                ),
                // A new leaf is created; intermediates never are.
                s(&[b"JSON.SET", b"d", b"$.fresh", br#""v""#], Expect::Ok),
                s(&[b"JSON.GET", b"d", b"$.fresh"], Expect::Str(br#"["v"]"#)),
                // DIVERGENCE (deliberate): RedisJSON answers nil for a
                // missing intermediate — a silent no-op. We say why.
                s(&[b"JSON.SET", b"d", b"$.x.y", b"1"], Expect::AnyError),
                s(&[b"JSON.GET", b"d", b"$.x"], Expect::Str(b"[]")),
            ],
        },
        Case {
            family: "json",
            name: "NUMINCRBY keeps integers integral and rejects non-numbers",
            steps: vec![
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"i":10,"f":1.5,"s":"x"}"#],
                    Expect::Ok,
                ),
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$.i", b"5"],
                    Expect::Str(b"[15]"),
                ),
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$.i", b"-20"],
                    Expect::Str(b"[-5]"),
                ),
                // A float stays a float. Incremented to a NON-integral
                // value on purpose: RESP3's double type cannot distinguish
                // the float 2.0 from the integer 2 (it spells both `,2`),
                // and RedisJSON has exactly the same limitation — so
                // asserting `[2.0]` here would be asserting something the
                // protocol cannot carry. Float-ness is checked below with
                // JSON.GET, which is JSON text in both dialects and so
                // shows the `.0` either way.
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$.f", b"0.25"],
                    Expect::Str(b"[1.75]"),
                ),
                s(&[b"JSON.SET", b"d", b"$.g", b"2.0"], Expect::Ok),
                s(&[b"JSON.GET", b"d", b"$.g"], Expect::Str(b"[2.0]")),
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$.s", b"1"],
                    Expect::Str(b"[null]"),
                ),
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$.gone", b"1"],
                    Expect::Str(b"[]"),
                ),
                // A bad INCREMENT is a client error in either dialect — it
                // is the argument that is wrong, not the match.
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$.i", b"notanumber"],
                    Expect::AnyError,
                ),
                // The refused increments left the document untouched.
                s(&[b"JSON.GET", b"d", b"$.i"], Expect::Str(b"[-5]")),
            ],
        },
        Case {
            family: "json",
            name: "array ops: ARRAPPEND returns the new length, ARRLEN reads it",
            steps: vec![
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"a":[],"o":{}}"#],
                    Expect::Ok,
                ),
                s(
                    &[b"JSON.ARRLEN", b"d", b"$.a"],
                    Expect::Arr(vec![Expect::Int(0)]),
                ),
                s(
                    &[b"JSON.ARRAPPEND", b"d", b"$.a", b"1"],
                    Expect::Arr(vec![Expect::Int(1)]),
                ),
                s(
                    &[b"JSON.ARRAPPEND", b"d", b"$.a", b"2", b"3"],
                    Expect::Arr(vec![Expect::Int(3)]),
                ),
                s(
                    &[b"JSON.ARRLEN", b"d", b"$.a"],
                    Expect::Arr(vec![Expect::Int(3)]),
                ),
                s(&[b"JSON.GET", b"d", b"$.a"], Expect::Str(b"[[1,2,3]]")),
                // Under JSONPath a non-array match is a null element and a
                // missing path is an empty container; the legacy spellings
                // of the same three are errors (asserted in the dialect
                // case above).
                s(
                    &[b"JSON.ARRAPPEND", b"d", b"$.o", b"1"],
                    Expect::Arr(vec![Expect::Nil]),
                ),
                s(
                    &[b"JSON.ARRLEN", b"d", b"$.o"],
                    Expect::Arr(vec![Expect::Nil]),
                ),
                s(&[b"JSON.ARRLEN", b"d", b"$.gone"], Expect::Arr(vec![])),
            ],
        },
        Case {
            family: "json",
            name: "DEL: a path removes one member, the root removes the key",
            steps: vec![
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"a":1,"b":[1,2]}"#],
                    Expect::Ok,
                ),
                s(&[b"JSON.DEL", b"d", b"$.a"], Expect::Int(1)),
                s(&[b"JSON.GET", b"d", b"$.a"], Expect::Str(b"[]")),
                s(&[b"EXISTS", b"d"], Expect::Int(1)),
                // JSON.DEL counts what it removed in BOTH dialects — it
                // answers a number, not a set of matches, so there is no
                // container to shape.
                s(&[b"JSON.DEL", b"d", b"$.a"], Expect::Int(0)),
                s(&[b"JSON.DEL", b"d", b".a"], Expect::Int(0)),
                s(&[b"JSON.DEL", b"d", b"$.b[0]"], Expect::Int(1)),
                s(&[b"JSON.GET", b"d", b"$.b"], Expect::Str(b"[[2]]")),
                // Root delete removes the whole key; FORGET is the alias.
                s(&[b"JSON.DEL", b"d"], Expect::Int(1)),
                s(&[b"EXISTS", b"d"], Expect::Int(0)),
                s(&[b"JSON.DEL", b"gone"], Expect::Int(0)),
                s(&[b"JSON.SET", b"d2", b"$", b"[1]"], Expect::Ok),
                s(&[b"JSON.FORGET", b"d2"], Expect::Int(1)),
                s(&[b"EXISTS", b"d2"], Expect::Int(0)),
            ],
        },
        Case {
            family: "json",
            name: "NX/XX on the key and on a path",
            steps: vec![
                s(&[b"JSON.SET", b"d", b"$", br#"{"a":1}"#, b"NX"], Expect::Ok),
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"a":2}"#, b"NX"],
                    Expect::Nil,
                ),
                s(&[b"JSON.GET", b"d", b"$.a"], Expect::Str(b"[1]")),
                // A rejected NX/XX is nil in both dialects: the reply says
                // "I did not write", which is not a set of matches.
                s(&[b"JSON.SET", b"d", b"$.new", b"7", b"XX"], Expect::Nil),
                s(&[b"JSON.SET", b"d", b"$.new", b"7", b"NX"], Expect::Ok),
                s(&[b"JSON.GET", b"d", b"$.new"], Expect::Str(b"[7]")),
                s(&[b"JSON.SET", b"d", b"$.new", b"8", b"XX"], Expect::Ok),
                s(&[b"JSON.GET", b"d", b"$.new"], Expect::Str(b"[8]")),
                s(
                    &[b"JSON.SET", b"d", b"$", b"{}", b"BOGUS"],
                    Expect::AnyError,
                ),
            ],
        },
        Case {
            family: "json",
            name: "malformed paths and invalid JSON are refused, not guessed",
            steps: vec![
                s(&[b"JSON.SET", b"d", b"$", br#"{"a":{"b":1}}"#], Expect::Ok),
                // Malformed paths and payloads.
                s(&[b"JSON.GET", b"d", b"$.a["], Expect::AnyError),
                s(&[b"JSON.GET", b"d", b"$...b"], Expect::AnyError),
                s(&[b"JSON.GET", b"d", b"$.a[0:2:0]"], Expect::AnyError),
                s(&[b"JSON.GET", b"d", b"$.a[?(@.b = 1)]"], Expect::AnyError),
                s(&[b"JSON.SET", b"bad", b"$", b"{not json"], Expect::AnyError),
                s(&[b"EXISTS", b"bad"], Expect::Int(0)),
                // The document survived every refusal.
                s(&[b"JSON.GET", b"d", b"$.a.b"], Expect::Str(b"[1]")),
            ],
        },
        // ADR-0054: multi-match paths under `$`. Every reply below is
        // RedisJSON v8.2.8's to the same command; the deliberate
        // differences each have a case of their own after these, so one
        // cannot hide the steps that follow it in an oracle run.
        Case {
            family: "json",
            name: "multi-match reads: descent, wildcard, union, slice, filter, in document order",
            steps: vec![
                s(
                    &[
                        b"JSON.SET",
                        b"d",
                        b"$",
                        br#"{"a":{"n":1,"m":"x"},"b":{"n":2,"m":null},"c":[{"n":3,"s":"abc"},{"k":4,"s":"abd"}],"l":[1,2,3,4,5]}"#,
                    ],
                    Expect::Ok,
                ),
                s(&[b"JSON.GET", b"d", b"$..n"], Expect::Str(b"[1,2,3]")),
                s(&[b"JSON.GET", b"d", b"$..m"], Expect::Str(br#"["x",null]"#)),
                s(&[b"JSON.GET", b"d", b"$.c[*].s"], Expect::Str(br#"["abc","abd"]"#)),
                s(&[b"JSON.GET", b"d", b"$['a','b'].n"], Expect::Str(b"[1,2]")),
                s(&[b"JSON.GET", b"d", b"$.l[-1,0]"], Expect::Str(b"[5,1]")),
                s(&[b"JSON.GET", b"d", b"$.l[0,9]"], Expect::Str(b"[1]")),
                s(&[b"JSON.GET", b"d", b"$.l[1:3]"], Expect::Str(b"[2,3]")),
                s(&[b"JSON.GET", b"d", b"$.l[::2]"], Expect::Str(b"[1,3,5]")),
                s(&[b"JSON.GET", b"d", b"$.l[-2:]"], Expect::Str(b"[4,5]")),
                s(&[b"JSON.GET", b"d", b"$.l[5:1]"], Expect::Str(b"[]")),
                s(
                    &[b"JSON.GET", b"d", b"$.c[?(@.n)]"],
                    Expect::Str(br#"[{"n":3,"s":"abc"}]"#),
                ),
                s(
                    &[b"JSON.GET", b"d", br#"$.c[?(@.s > "abc")]"#],
                    Expect::Str(br#"[{"k":4,"s":"abd"}]"#),
                ),
                s(
                    &[b"JSON.GET", b"d", b"$.c[?(@.n >= 3 && @.s)]"],
                    Expect::Str(br#"[{"n":3,"s":"abc"}]"#),
                ),
                s(
                    &[b"JSON.GET", b"d", b"$.c[?(@.n < 3 || @.k > 3)]"],
                    Expect::Str(br#"[{"k":4,"s":"abd"}]"#),
                ),
                s(
                    &[b"JSON.GET", b"d", b"$.l[?(@ > $.a.n)]"],
                    Expect::Str(b"[2,3,4,5]"),
                ),
                // An absent operand equals nothing, another absent one
                // included (RedisJSON's rule, not RFC 9535's).
                s(
                    &[b"JSON.GET", b"d", b"$.c[?(@.zz == @.yy)]"],
                    Expect::Str(b"[]"),
                ),
                s(&[b"JSON.GET", b"d", b"$.nothing[*]"], Expect::Str(b"[]")),
                s(
                    &[b"JSON.TYPE", b"d", b"$..n"],
                    Expect::Arr(vec![
                        Expect::Str(b"integer"),
                        Expect::Str(b"integer"),
                        Expect::Str(b"integer"),
                    ]),
                ),
                s(
                    &[b"JSON.ARRLEN", b"d", b"$.*"],
                    Expect::Arr(vec![Expect::Nil, Expect::Nil, Expect::Int(2), Expect::Int(5)]),
                ),
            ],
        },
        Case {
            family: "json",
            name: "multi-match writes reach every match; SET replaces and adds none",
            steps: vec![
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"a":{"n":1,"m":"x"},"b":{"n":2},"l":[1,2]}"#],
                    Expect::Ok,
                ),
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$..n", b"10"],
                    Expect::Str(b"[11,12]"),
                ),
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$.a.*", b"1"],
                    Expect::Str(b"[12,null]"),
                ),
                // A location a union names twice is written twice.
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$.l[0,0]", b"1"],
                    Expect::Str(b"[2,3]"),
                ),
                s(
                    &[b"JSON.ARRAPPEND", b"d", br#"$["b","l"]"#, b"9"],
                    Expect::Arr(vec![Expect::Nil, Expect::Int(3)]),
                ),
                s(&[b"JSON.GET", b"d", b"$.l"], Expect::Str(b"[[3,2,9]]")),
                s(&[b"JSON.SET", b"d", b"$..n", b"0"], Expect::Ok),
                s(&[b"JSON.GET", b"d", b"$..n"], Expect::Str(b"[0,0]")),
                // Only a path naming one location adds a value: an
                // indefinite one matching nothing is refused, NX on one is
                // always refused, and XX matching nothing is nil.
                s(&[b"JSON.SET", b"d", b"$.*.z", b"true"], Expect::AnyError),
                s(&[b"JSON.SET", b"d", b"$..n", b"1", b"NX"], Expect::AnyError),
                s(&[b"JSON.SET", b"d", b"$.*.z", b"true", b"XX"], Expect::Nil),
                s(&[b"JSON.SET", b"d", b"$..n", b"5", b"XX"], Expect::Ok),
                s(
                    &[b"JSON.GET", b"d", b"$"],
                    Expect::Str(br#"[{"a":{"n":5,"m":"x"},"b":{"n":5},"l":[3,2,9]}]"#),
                ),
            ],
        },
        Case {
            family: "json",
            name: "DEL counts each location once, and emptying the document deletes the key",
            steps: vec![
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"a":{"n":1},"b":{"n":2,"k":3},"l":[1,2,3]}"#],
                    Expect::Ok,
                ),
                // Last index first, so no removal shifts one still to come.
                s(&[b"JSON.DEL", b"d", b"$.l[0,2]"], Expect::Int(2)),
                s(&[b"JSON.GET", b"d", b"$.l"], Expect::Str(b"[[2]]")),
                s(&[b"JSON.DEL", b"d", b"$.l[0,0]"], Expect::Int(1)),
                s(&[b"JSON.GET", b"d", b"$.l"], Expect::Str(b"[[]]")),
                s(&[b"JSON.DEL", b"d", b"$..n"], Expect::Int(2)),
                // A location inside another removed one goes with it,
                // uncounted: a, b and l, not b.k.
                s(&[b"JSON.DEL", b"d", b"$..*"], Expect::Int(3)),
                // BUG-0209: the document is now empty, so the key is gone.
                s(&[b"EXISTS", b"d"], Expect::Int(0)),
                s(&[b"JSON.SET", b"e", b"$", br#"{"a":1}"#], Expect::Ok),
                s(&[b"JSON.DEL", b"e", b"$.a"], Expect::Int(1)),
                s(&[b"EXISTS", b"e"], Expect::Int(0)),
                // An emptied member is not an emptied document.
                s(&[b"JSON.SET", b"g", b"$", br#"{"a":[1]}"#], Expect::Ok),
                s(&[b"JSON.DEL", b"g", b"$.a[0]"], Expect::Int(1)),
                s(&[b"JSON.GET", b"g", b"$"], Expect::Str(br#"[{"a":[]}]"#)),
            ],
        },
        Case {
            family: "json",
            name: "NUMINCRBY on integers stays exact past 2^53 (BUG-0208)",
            steps: vec![
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"i":9007199254740993,"j":1}"#],
                    Expect::Ok,
                ),
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$.i", b"0"],
                    Expect::Str(b"[9007199254740993]"),
                ),
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$.i", b"1"],
                    Expect::Str(b"[9007199254740994]"),
                ),
                s(
                    &[b"JSON.NUMINCRBY", b"d", b".i", b"-2"],
                    Expect::Str(b"9007199254740992"),
                ),
                // An increment written as a float makes a float, whole or
                // not. Asserted through JSON.GET: RESP3's double cannot
                // carry the `.0` (see the NUMINCRBY case above).
                s(&[b"JSON.NUMINCRBY", b"d", b"$.j", b"2.0"], Expect::AnyBulk),
                s(&[b"JSON.GET", b"d", b"$.j"], Expect::Str(b"[3.0]")),
            ],
        },
        // The deliberate differences from RedisJSON in the multi-match
        // work, each alone and each listed in tools/redisjson_compare.sh.
        Case {
            family: "json",
            name: "NUMINCRBY refuses an integer overflow (RedisJSON wraps)",
            steps: vec![
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"m":9223372036854775807}"#],
                    Expect::Ok,
                ),
                // DIVERGENCE (deliberate): RedisJSON answers
                // [-9223372036854775808] and stores it. Redis's INCRBY
                // refuses an overflow, and so do we.
                s(&[b"JSON.NUMINCRBY", b"d", b"$.m", b"1"], Expect::AnyError),
                s(
                    &[b"JSON.GET", b"d", b"$.m"],
                    Expect::Str(b"[9223372036854775807]"),
                ),
            ],
        },
        Case {
            family: "json",
            name: "multi-match in the legacy dialect is refused",
            steps: vec![
                s(&[b"JSON.SET", b"d", b"$", br#"{"a":{"b":1}}"#], Expect::Ok),
                // DIVERGENCE (deliberate): RedisJSON answers the first
                // match, 1. The `$` spelling answers every match.
                s(&[b"JSON.GET", b"d", b"..b"], Expect::AnyError),
                s(&[b"JSON.GET", b"d", b"$..b"], Expect::Str(b"[1]")),
            ],
        },
        Case {
            family: "json",
            name: "a regex filter is refused",
            steps: vec![
                s(&[b"JSON.SET", b"d", b"$", br#"{"a":["x","y"]}"#], Expect::Ok),
                // DIVERGENCE (deliberate, for now): RedisJSON matches `=~`.
                s(
                    &[b"JSON.GET", b"d", br#"$.a[?(@ =~ "x")]"#],
                    Expect::AnyError,
                ),
            ],
        },
        Case {
            family: "json",
            name: "a multi-match operand inside a filter is refused",
            steps: vec![
                s(&[b"JSON.SET", b"d", b"$", br#"{"a":[{"b":{"c":1}}]}"#], Expect::Ok),
                // DIVERGENCE (deliberate): RedisJSON evaluates `@..c`.
                s(&[b"JSON.GET", b"d", b"$.a[?(@..c)]"], Expect::AnyError),
            ],
        },
        // ADR-0055: the rest of RedisJSON's command family. Every reply is
        // RedisJSON v8.2.8's to the same command; the one deliberate
        // difference has a case of its own at the end.
        Case {
            family: "json",
            name: "STRLEN and STRAPPEND: lengths in bytes, an append per match",
            steps: vec![
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"a":{"s":"ab","n":2},"b":{"s":"c"}}"#],
                    Expect::Ok,
                ),
                s(
                    &[b"JSON.STRLEN", b"d", b"$..s"],
                    Expect::Arr(vec![Expect::Int(2), Expect::Int(1)]),
                ),
                s(&[b"JSON.STRLEN", b"d", b".a.s"], Expect::Int(2)),
                s(&[b"JSON.STRLEN", b"d", b"$.a.n"], Expect::Arr(vec![Expect::Nil])),
                s(&[b"JSON.STRLEN", b"d", b".a.n"], Expect::AnyError),
                s(&[b"JSON.STRLEN", b"d", b"$.zz"], Expect::Arr(vec![])),
                s(&[b"JSON.STRLEN", b"gone"], Expect::Nil),
                s(&[b"JSON.STRLEN", b"gone", b"$.a"], Expect::AnyError),
                s(
                    &[b"JSON.STRAPPEND", b"d", b"$..s", br#""xy""#],
                    Expect::Arr(vec![Expect::Int(4), Expect::Int(3)]),
                ),
                s(&[b"JSON.STRAPPEND", b"d", b".a.s", br#""z""#], Expect::Int(5)),
                s(
                    &[b"JSON.STRAPPEND", b"d", b"$.a.n", br#""z""#],
                    Expect::Arr(vec![Expect::Nil]),
                ),
                // A value that is not a JSON string fails at a string, and
                // text that is not JSON at all fails before anything.
                s(&[b"JSON.STRAPPEND", b"d", b"$.a.s", b"5"], Expect::AnyError),
                s(&[b"JSON.STRAPPEND", b"d", b"$.a.s", b"z"], Expect::AnyError),
                s(&[b"JSON.STRAPPEND", b"gone", b"$", br#""z""#], Expect::AnyError),
                s(&[b"JSON.GET", b"d", b"$..s"], Expect::Str(br#"["abxyz","cxy"]"#)),
                // With no path the legacy root is the target.
                s(&[b"JSON.SET", b"q", b"$", br#""str""#], Expect::Ok),
                s(&[b"JSON.STRAPPEND", b"q", br#""!""#], Expect::Int(4)),
                s(&[b"JSON.STRLEN", b"q"], Expect::Int(4)),
                // A location a union names twice is appended to twice.
                s(&[b"JSON.SET", b"u", b"$", br#"{"a":"x"}"#], Expect::Ok),
                s(
                    &[b"JSON.STRAPPEND", b"u", br#"$["a","a"]"#, br#""?""#],
                    Expect::Arr(vec![Expect::Int(2), Expect::Int(3)]),
                ),
            ],
        },
        Case {
            family: "json",
            name: "OBJLEN and OBJKEYS: an object's size and member names, in order",
            steps: vec![
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"a":{"o":{"x":1,"y":[1]}},"b":{"o":{}},"s":"t"}"#],
                    Expect::Ok,
                ),
                s(
                    &[b"JSON.OBJLEN", b"d", b"$..o"],
                    Expect::Arr(vec![Expect::Int(2), Expect::Int(0)]),
                ),
                s(&[b"JSON.OBJLEN", b"d", b".a.o"], Expect::Int(2)),
                s(&[b"JSON.OBJLEN", b"d"], Expect::Int(3)),
                s(&[b"JSON.OBJLEN", b"d", b"$.s"], Expect::Arr(vec![Expect::Nil])),
                s(&[b"JSON.OBJLEN", b"d", b".s"], Expect::AnyError),
                // A missing legacy path is nil here, not an error.
                s(&[b"JSON.OBJLEN", b"d", b".zz"], Expect::Nil),
                s(&[b"JSON.OBJLEN", b"gone"], Expect::Nil),
                s(&[b"JSON.OBJLEN", b"gone", b"$"], Expect::AnyError),
                s(
                    &[b"JSON.OBJKEYS", b"d", b"$..o"],
                    Expect::Arr(vec![
                        Expect::Arr(vec![Expect::Str(b"x"), Expect::Str(b"y")]),
                        Expect::Arr(vec![]),
                    ]),
                ),
                s(
                    &[b"JSON.OBJKEYS", b"d", b".a.o"],
                    Expect::Arr(vec![Expect::Str(b"x"), Expect::Str(b"y")]),
                ),
                s(
                    &[b"JSON.OBJKEYS", b"d"],
                    Expect::Arr(vec![Expect::Str(b"a"), Expect::Str(b"b"), Expect::Str(b"s")]),
                ),
                s(&[b"JSON.OBJKEYS", b"d", b"$.s"], Expect::Arr(vec![Expect::Nil])),
                s(&[b"JSON.OBJKEYS", b"d", b".s"], Expect::AnyError),
                s(&[b"JSON.OBJKEYS", b"d", b".zz"], Expect::Nil),
                s(&[b"JSON.OBJKEYS", b"gone"], Expect::Nil),
                s(&[b"JSON.OBJKEYS", b"gone", b"$"], Expect::AnyError),
            ],
        },
        Case {
            family: "json",
            name: "TOGGLE flips each boolean: 1 or 0 under $, true or false under legacy",
            steps: vec![
                s(&[b"JSON.SET", b"b", b"$", br#"{"t":true,"l":[true,false],"n":1}"#], Expect::Ok),
                // A location a union names twice is flipped twice.
                s(
                    &[b"JSON.TOGGLE", b"b", b"$.l[0,0,1]"],
                    Expect::Arr(vec![Expect::Int(0), Expect::Int(1), Expect::Int(1)]),
                ),
                s(&[b"JSON.GET", b"b", b"$.l"], Expect::Str(b"[[true,true]]")),
                s(&[b"JSON.TOGGLE", b"b", b".t"], Expect::Str(b"false")),
                s(&[b"JSON.TOGGLE", b"b", b"$.n"], Expect::Arr(vec![Expect::Nil])),
                s(&[b"JSON.TOGGLE", b"b", b".n"], Expect::AnyError),
                s(&[b"JSON.TOGGLE", b"b", b"$.zz"], Expect::Arr(vec![])),
                s(&[b"JSON.TOGGLE", b"b", b".zz"], Expect::AnyError),
                s(&[b"JSON.TOGGLE", b"gone", b"$"], Expect::AnyError),
                s(&[b"JSON.TOGGLE", b"b"], Expect::AnyError),
            ],
        },
        Case {
            family: "json",
            name: "ARRINDEX: first index of a value, type-strict, in a clamped range",
            steps: vec![
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"l":[1,{"k":[2]},2.5,"2",[1],null,2],"e":[],"s":"x"}"#],
                    Expect::Ok,
                ),
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", br#"{"k":[2]}"#], Expect::Arr(vec![Expect::Int(1)])),
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", b"[1]"], Expect::Arr(vec![Expect::Int(4)])),
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", b"null"], Expect::Arr(vec![Expect::Int(5)])),
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", br#""2""#], Expect::Arr(vec![Expect::Int(3)])),
                // The integer 2 is not the float 2.0.
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", b"2"], Expect::Arr(vec![Expect::Int(6)])),
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", b"2.0"], Expect::Arr(vec![Expect::Int(-1)])),
                s(&[b"JSON.ARRINDEX", b"d", b".l", b"2.5"], Expect::Int(2)),
                // stop is exclusive, 0 means the end, negatives count back.
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", b"2.5", b"0", b"2"], Expect::Arr(vec![Expect::Int(-1)])),
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", b"2.5", b"0", b"3"], Expect::Arr(vec![Expect::Int(2)])),
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", b"1", b"1"], Expect::Arr(vec![Expect::Int(-1)])),
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", b"1", b"-99"], Expect::Arr(vec![Expect::Int(0)])),
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", b"2", b"0", b"0"], Expect::Arr(vec![Expect::Int(6)])),
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", b"2", b"0", b"-1"], Expect::Arr(vec![Expect::Int(-1)])),
                s(&[b"JSON.ARRINDEX", b"d", b"$.e", b"1"], Expect::Arr(vec![Expect::Int(-1)])),
                s(&[b"JSON.ARRINDEX", b"d", b"$.s", b"1"], Expect::Arr(vec![Expect::Nil])),
                s(&[b"JSON.ARRINDEX", b"d", b".s", b"1"], Expect::AnyError),
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", b"notjson"], Expect::AnyError),
                s(&[b"JSON.ARRINDEX", b"d", b"$.l", b"1", b"x"], Expect::AnyError),
                s(&[b"JSON.ARRINDEX", b"gone", b"$.l", b"1"], Expect::AnyError),
            ],
        },
        Case {
            family: "json",
            name: "ARRINSERT, ARRPOP and ARRTRIM: positions, clamping and new lengths",
            steps: vec![
                s(&[b"JSON.SET", b"d", b"$", br#"{"a":[1,2,3,4,5],"e":[],"s":"x"}"#], Expect::Ok),
                // The length appends; past it, or before the start, refuses.
                s(&[b"JSON.ARRINSERT", b"d", b"$.a", b"5", b"6"], Expect::Arr(vec![Expect::Int(6)])),
                s(&[b"JSON.ARRINSERT", b"d", b"$.a", b"-6", b"0"], Expect::Arr(vec![Expect::Int(7)])),
                s(&[b"JSON.ARRINSERT", b"d", b".a", b"1", br#""x""#, br#""y""#], Expect::Int(9)),
                s(&[b"JSON.ARRINSERT", b"d", b"$.a", b"99", b"7"], Expect::AnyError),
                s(&[b"JSON.ARRINSERT", b"d", b"$.a", b"-99", b"7"], Expect::AnyError),
                s(&[b"JSON.ARRINSERT", b"d", b"$.s", b"0", b"7"], Expect::Arr(vec![Expect::Nil])),
                s(&[b"JSON.ARRINSERT", b"d", b".s", b"0", b"7"], Expect::AnyError),
                s(&[b"JSON.ARRINSERT", b"d", b"$.a", b"0"], Expect::AnyError),
                s(
                    &[b"JSON.GET", b"d", b"$.a"],
                    Expect::Str(br#"[[0,"x","y",1,2,3,4,5,6]]"#),
                ),
                // ARRPOP answers JSON text; the last by default, an index
                // clamped; an empty array answers nil.
                s(&[b"JSON.ARRPOP", b"d", b"$.a"], Expect::Arr(vec![Expect::Str(b"6")])),
                s(&[b"JSON.ARRPOP", b"d", b"$.a", b"1"], Expect::Arr(vec![Expect::Str(br#""x""#)])),
                s(&[b"JSON.ARRPOP", b"d", b"$.a", b"99"], Expect::Arr(vec![Expect::Str(b"5")])),
                s(&[b"JSON.ARRPOP", b"d", b".a", b"-99"], Expect::Str(b"0")),
                s(&[b"JSON.ARRPOP", b"d", b"$.e"], Expect::Arr(vec![Expect::Nil])),
                s(&[b"JSON.ARRPOP", b"d", b".e"], Expect::Nil),
                s(&[b"JSON.ARRPOP", b"d", b"$.s"], Expect::Arr(vec![Expect::Nil])),
                s(&[b"JSON.ARRPOP", b"d", b".s"], Expect::AnyError),
                s(&[b"JSON.ARRPOP", b"d"], Expect::AnyError),
                s(&[b"JSON.GET", b"d", b"$.a"], Expect::Str(br#"[["y",1,2,3,4]]"#)),
                // ARRTRIM keeps the inclusive range, clamped to the array.
                s(&[b"JSON.ARRTRIM", b"d", b"$.a", b"1", b"-2"], Expect::Arr(vec![Expect::Int(3)])),
                s(&[b"JSON.GET", b"d", b"$.a"], Expect::Str(b"[[1,2,3]]")),
                s(&[b"JSON.ARRTRIM", b"d", b".a", b"-2", b"99"], Expect::Int(2)),
                s(&[b"JSON.ARRTRIM", b"d", b"$.a", b"-99", b"-99"], Expect::Arr(vec![Expect::Int(1)])),
                s(&[b"JSON.GET", b"d", b"$.a"], Expect::Str(b"[[2]]")),
                s(&[b"JSON.ARRTRIM", b"d", b"$.a", b"99", b"100"], Expect::Arr(vec![Expect::Int(0)])),
                s(&[b"JSON.ARRTRIM", b"d", b"$.s", b"0", b"1"], Expect::Arr(vec![Expect::Nil])),
                s(&[b"JSON.ARRTRIM", b"d", b".s", b"0", b"1"], Expect::AnyError),
                s(&[b"JSON.ARRTRIM", b"d", b"$.a", b"0"], Expect::AnyError),
                s(&[b"JSON.ARRTRIM", b"gone", b"$.a", b"0", b"1"], Expect::AnyError),
                // Legacy root defaults: ARRPOP with no path pops the root.
                s(&[b"JSON.SET", b"r", b"$", b"[1,2,3]"], Expect::Ok),
                s(&[b"JSON.ARRPOP", b"r"], Expect::Str(b"3")),
                s(&[b"JSON.ARRPOP", b"r", b"$"], Expect::Arr(vec![Expect::Str(b"2")])),
            ],
        },
        Case {
            family: "json",
            name: "NUMMULTBY multiplies each number; integers stay exact",
            steps: vec![
                s(&[b"JSON.SET", b"v", b"$", br#"{"n":10,"f":1.5,"neg":-3,"s":"x"}"#], Expect::Ok),
                s(&[b"JSON.NUMMULTBY", b"v", b"$.n", b"-2"], Expect::Str(b"[-20]")),
                s(&[b"JSON.NUMMULTBY", b"v", b".neg", b"2"], Expect::Str(b"-6")),
                s(&[b"JSON.NUMMULTBY", b"v", b"$.f", b"3"], Expect::Str(b"[4.5]")),
                s(&[b"JSON.NUMMULTBY", b"v", b"$.s", b"2"], Expect::Str(b"[null]")),
                s(&[b"JSON.NUMMULTBY", b"v", b".s", b"2"], Expect::AnyError),
                s(&[b"JSON.NUMMULTBY", b"v", b"$.zz", b"2"], Expect::Str(b"[]")),
                s(&[b"JSON.NUMMULTBY", b"v", b"$.n", b"x"], Expect::AnyError),
                s(&[b"JSON.NUMMULTBY", b"gone", b"$.n", b"2"], Expect::AnyError),
                // A multiplier written as a float makes a float.
                s(&[b"JSON.NUMMULTBY", b"v", b"$.n", b"0.5"], Expect::AnyBulk),
                s(&[b"JSON.GET", b"v", b"$.n"], Expect::Str(b"[-10.0]")),
                s(&[b"JSON.SET", b"w", b"$", br#"{"i":9007199254740993}"#], Expect::Ok),
                s(&[b"JSON.NUMMULTBY", b"w", b"$.i", b"1"], Expect::Str(b"[9007199254740993]")),
            ],
        },
        Case {
            family: "json",
            name: "CLEAR empties containers and zeroes numbers, counting what changed",
            steps: vec![
                s(
                    &[b"JSON.SET", b"d", b"$", br#"{"a":{"o":{"x":1},"e":{},"n":2,"z":0,"s":"x","b":true,"l":[1]}}"#],
                    Expect::Ok,
                ),
                // Already empty, already zero, and non-containers count 0.
                s(&[b"JSON.CLEAR", b"d", b"$.a.e"], Expect::Int(0)),
                s(&[b"JSON.CLEAR", b"d", b"$.a.z"], Expect::Int(0)),
                s(&[b"JSON.CLEAR", b"d", b"$.a.s"], Expect::Int(0)),
                s(&[b"JSON.CLEAR", b"d", b"$.a.b"], Expect::Int(0)),
                s(&[b"JSON.CLEAR", b"d", b".a.o"], Expect::Int(1)),
                s(&[b"JSON.CLEAR", b"d", b"$.a.*"], Expect::Int(2)),
                s(&[b"JSON.CLEAR", b"d", b"$.zz"], Expect::Int(0)),
                s(&[b"JSON.CLEAR", b"d", b".zz"], Expect::Int(0)),
                s(
                    &[b"JSON.GET", b"d"],
                    Expect::Str(br#"{"a":{"o":{},"e":{},"n":0,"z":0,"s":"x","b":true,"l":[]}}"#),
                ),
                s(&[b"JSON.CLEAR", b"d"], Expect::Int(1)),
                s(&[b"JSON.GET", b"d"], Expect::Str(b"{}")),
                s(&[b"JSON.CLEAR", b"gone"], Expect::AnyError),
                s(&[b"JSON.SET", b"n", b"$", b"5"], Expect::Ok),
                s(&[b"JSON.CLEAR", b"n"], Expect::Int(1)),
                s(&[b"JSON.GET", b"n"], Expect::Str(b"0")),
            ],
        },
        Case {
            family: "json",
            name: "MERGE applies an RFC 7396 patch at each match",
            steps: vec![
                s(&[b"JSON.SET", b"g", b"$", br#"{"a":{"b":1},"l":[1,2]}"#], Expect::Ok),
                // A null member deletes; nulls inside a new object go too;
                // an array replaces.
                s(
                    &[b"JSON.MERGE", b"g", b"$", br#"{"a":{"c":{"d":null,"e":1}},"l":[3],"x":null}"#],
                    Expect::Ok,
                ),
                s(&[b"JSON.GET", b"g"], Expect::Str(br#"{"a":{"b":1,"c":{"e":1}},"l":[3]}"#)),
                // A non-object patch replaces; an object patch over a
                // non-object starts from an empty object.
                s(&[b"JSON.MERGE", b"g", b"$.a", b"5"], Expect::Ok),
                s(&[b"JSON.MERGE", b"g", b"$.l", br#"{"k":null,"j":1}"#], Expect::Ok),
                s(&[b"JSON.GET", b"g"], Expect::Str(br#"{"a":5,"l":{"j":1}}"#)),
                // A null patch at a path sets null there.
                s(&[b"JSON.MERGE", b"g", b"$.a", b"null"], Expect::Ok),
                // A missing last member is added as given.
                s(&[b"JSON.MERGE", b"g", b"$.new", br#"{"x":null}"#], Expect::Ok),
                s(&[b"JSON.GET", b"g"], Expect::Str(br#"{"a":null,"l":{"j":1},"new":{"x":null}}"#)),
                s(&[b"JSON.SET", b"h", b"$", br#"{"a":{"a":{"z":1}}}"#], Expect::Ok),
                s(&[b"JSON.MERGE", b"h", b"$..a", br#"{"b":2}"#], Expect::Ok),
                s(&[b"JSON.GET", b"h"], Expect::Str(br#"{"a":{"a":{"z":1,"b":2},"b":2}}"#)),
                s(&[b"JSON.MERGE", b"h", b"$.*.x", b"1"], Expect::AnyError),
                // A missing key takes the patch as its document, nulls and
                // all, as JSON.SET would.
                s(&[b"JSON.MERGE", b"k", b"$", br#"{"a":1,"b":null}"#], Expect::Ok),
                s(&[b"JSON.GET", b"k"], Expect::Str(br#"{"a":1,"b":null}"#)),
                s(&[b"JSON.MERGE", b"k2", b"$.a", b"1"], Expect::AnyError),
                s(&[b"JSON.MERGE", b"k", b"$", b"notjson"], Expect::AnyError),
            ],
        },
        Case {
            family: "json",
            name: "MGET reads one path from many keys; MSET writes all triples or none",
            steps: vec![
                s(&[b"JSON.SET", b"{j}a", b"$", br#"{"x":1,"y":{"x":2}}"#], Expect::Ok),
                s(&[b"JSON.SET", b"{j}b", b"$", br#"{"x":3}"#], Expect::Ok),
                s(&[b"SET", b"{j}s", b"plain"], Expect::Ok),
                // A missing key, or one that is not a document, is nil.
                s(
                    &[b"JSON.MGET", b"{j}a", b"{j}b", b"{j}gone", b"{j}s", b"$..x"],
                    Expect::Arr(vec![
                        Expect::Str(b"[1,2]"),
                        Expect::Str(b"[3]"),
                        Expect::Nil,
                        Expect::Nil,
                    ]),
                ),
                s(
                    &[b"JSON.MGET", b"{j}a", b"{j}b", b".x"],
                    Expect::Arr(vec![Expect::Str(b"1"), Expect::Str(b"3")]),
                ),
                s(&[b"JSON.MGET", b"{j}a", b".zz"], Expect::Arr(vec![Expect::Nil])),
                s(&[b"JSON.MGET", b"{j}a", b"$.zz"], Expect::Arr(vec![Expect::Str(b"[]")])),
                s(&[b"JSON.MGET", b"$.x"], Expect::AnyError),
                s(
                    &[b"JSON.MSET", b"{j}a", b"$.x", b"10", b"{j}b", b"$", br#"{"y":1}"#],
                    Expect::Ok,
                ),
                s(
                    &[b"JSON.MGET", b"{j}a", b"{j}b", b"$"],
                    Expect::Arr(vec![
                        Expect::Str(br#"[{"x":10,"y":{"x":2}}]"#),
                        Expect::Str(br#"[{"y":1}]"#),
                    ]),
                ),
                // Each triple is checked against the documents as they
                // were: a key the command itself creates is still missing to
                // a later sub-path triple, and nothing is written.
                s(
                    &[b"JSON.MSET", b"{j}c", b"$", b"{}", b"{j}c", b"$.y", b"1"],
                    Expect::AnyError,
                ),
                s(&[b"EXISTS", b"{j}c"], Expect::Int(0)),
                s(
                    &[b"JSON.MSET", b"{j}a", b"$.x", b"11", b"{j}s", b"$", b"1"],
                    Expect::AnyError,
                ),
                s(
                    &[b"JSON.MSET", b"{j}a", b"$.x", b"11", b"{j}b", b"$.q", b"notjson"],
                    Expect::AnyError,
                ),
                // A multi-match path adds nothing, so one matching nothing
                // refuses the command.
                s(&[b"JSON.MSET", b"{j}a", b"$.*.q", b"1"], Expect::AnyError),
                s(&[b"JSON.MSET", b"{j}a", b"$.x", b"1", b"{j}b"], Expect::AnyError),
                s(&[b"JSON.GET", b"{j}a", b"$.x"], Expect::Str(b"[10]")),
                // The same key twice applies in order.
                s(&[b"JSON.MSET", b"{j}a", b"$.x", b"8", b"{j}a", b"$.x", b"9"], Expect::Ok),
                s(&[b"JSON.MSET", b"{j}a", b"$..x", b"7"], Expect::Ok),
                s(&[b"JSON.GET", b"{j}a"], Expect::Str(br#"{"x":7,"y":{"x":7}}"#)),
            ],
        },
        Case {
            family: "json",
            name: "RESP renders a value in RESP terms; DEBUG reports sizes and help",
            steps: vec![
                s(
                    &[b"JSON.SET", b"r", b"$", br#"{"a":{"s":"x","n":1,"f":1.5,"t":true,"z":null,"l":[1,"a"]}}"#],
                    Expect::Ok,
                ),
                s(
                    &[b"JSON.RESP", b"r", b".a.l"],
                    Expect::Arr(vec![Expect::Simple("["), Expect::Int(1), Expect::Str(b"a")]),
                ),
                s(
                    &[b"JSON.RESP", b"r", b"$..n"],
                    Expect::Arr(vec![Expect::Int(1)]),
                ),
                s(
                    &[b"JSON.RESP", b"r", b"$.a.t"],
                    Expect::Arr(vec![Expect::Simple("true")]),
                ),
                s(&[b"JSON.RESP", b"r", b"$.a.z"], Expect::Arr(vec![Expect::Nil])),
                s(&[b"JSON.RESP", b"r", b"$.zz"], Expect::Arr(vec![])),
                s(&[b"JSON.RESP", b"r", b".zz"], Expect::AnyError),
                s(&[b"JSON.RESP", b"gone"], Expect::Nil),
                s(&[b"JSON.RESP", b"gone", b"$"], Expect::Nil),
                // DEBUG MEMORY's number is each server's own accounting.
                s(&[b"JSON.DEBUG", b"MEMORY", b"r"], Expect::IntRange(1, 1 << 20)),
                s(&[b"JSON.DEBUG", b"MEMORY", b"gone"], Expect::Int(0)),
                s(&[b"JSON.DEBUG", b"MEMORY", b"gone", b"$.a"], Expect::Arr(vec![])),
                s(&[b"JSON.DEBUG", b"MEMORY", b"r", b".zz"], Expect::AnyError),
                s(
                    &[b"JSON.DEBUG", b"HELP"],
                    Expect::Arr(vec![
                        Expect::Str(b"MEMORY <key> [path] - reports memory usage"),
                        Expect::Str(b"HELP                - this message"),
                    ]),
                ),
                s(&[b"JSON.DEBUG", b"FOO"], Expect::AnyError),
                s(&[b"JSON.DEBUG", b"MEMORY"], Expect::AnyError),
                s(&[b"JSON.DEBUG"], Expect::AnyError),
            ],
        },
        Case {
            family: "json",
            name: "GET takes several paths and RedisJSON's formatting arguments",
            steps: vec![
                s(&[b"JSON.SET", b"w", b"$", br#"{"a":{"b":1,"c":[]},"d":"\u00e9\""}"#], Expect::Ok),
                s(&[b"JSON.GET", b"w", b"$.a.b", b"$.zz"], Expect::Json(br#"{"$.a.b":[1],"$.zz":[]}"#)),
                // A legacy path among `$` ones answers under `$`.
                s(&[b"JSON.GET", b"w", b"$.a.b", b".d"], Expect::Json(br#"{"$.a.b":[1],".d":["\u00e9\""]}"#)),
                s(&[b"JSON.GET", b"w", b".a.b", b".d"], Expect::Json(br#"{".a.b":1,".d":"\u00e9\""}"#)),
                // Two path arguments answer the object form even when equal.
                s(&[b"JSON.GET", b"w", b".a.b", b".a.b"], Expect::Json(br#"{".a.b":1}"#)),
                s(&[b"JSON.GET", b"w", b".a.b", b".zz"], Expect::AnyError),
                s(
                    &[b"JSON.GET", b"w", b"INDENT", b"  ", b"NEWLINE", b"\n", b"SPACE", b" ", b".a"],
                    Expect::Str(b"{\n  \"b\": 1,\n  \"c\": []\n}"),
                ),
                // The options may follow the path, and the last one wins.
                s(
                    &[b"JSON.GET", b"w", b"$.a", b"newline", b"|", b"indent", b"<", b"indent", b">"],
                    Expect::Str(b"[|>{|>>\"b\":1,|>>\"c\":[]|>}|]"),
                ),
                s(&[b"JSON.GET", b"w", b"NOESCAPE", b".a.b"], Expect::Str(b"1")),
                s(&[b"JSON.GET", b"w", b"INDENT"], Expect::AnyError),
            ],
        },
        Case {
            family: "json",
            name: "ARRLEN and TYPE answer a missing key as RedisJSON does (BUG-0210)",
            steps: vec![
                s(&[b"JSON.ARRLEN", b"gone"], Expect::Nil),
                s(&[b"JSON.ARRLEN", b"gone", b".a"], Expect::Nil),
                s(&[b"JSON.ARRLEN", b"gone", b"$"], Expect::AnyError),
                s(&[b"JSON.ARRLEN", b"gone", b"$.a"], Expect::AnyError),
                s(&[b"JSON.TYPE", b"gone"], Expect::Nil),
                s(&[b"JSON.TYPE", b"gone", b"$.a"], Expect::Nil),
            ],
        },
        Case {
            family: "json",
            name: "NUMMULTBY refuses an integer overflow (RedisJSON wraps)",
            steps: vec![
                s(&[b"JSON.SET", b"m", b"$", br#"{"i":3037000500}"#], Expect::Ok),
                // DIVERGENCE (deliberate): RedisJSON answers
                // [-9223372036709301616] and stores it, as BUG-0208 records
                // for NUMINCRBY.
                s(&[b"JSON.NUMMULTBY", b"m", b"$.i", b"3037000500"], Expect::AnyError),
                s(&[b"JSON.GET", b"m", b"$.i"], Expect::Str(b"[3037000500]")),
            ],
        },
        // Three more kept by decision (Jeff, 2026-10-08): each is a place
        // RedisJSON stores something the caller did not write.
        Case {
            family: "json",
            name: "a value with trailing text is refused",
            steps: vec![
                s(&[b"JSON.SET", b"jt", b"$", br#"{"a":1}"#], Expect::Ok),
                // DIVERGENCE (deliberate): RedisJSON reads the first value,
                // stores 2, and drops the rest.
                s(
                    &[b"JSON.SET", b"jt", b"$.a", b"2 x"],
                    Expect::Err("trailing characters at line 1 column 3"),
                ),
                s(&[b"JSON.GET", b"jt", b"$.a"], Expect::Str(b"[1]")),
            ],
        },
        Case {
            family: "json",
            name: "a negative index past the start names nothing",
            steps: vec![
                s(&[b"JSON.SET", b"jn", b"$", br#"{"b":[10,20,30]}"#], Expect::Ok),
                // DIVERGENCE (deliberate): RedisJSON takes the first element
                // and deletes it.
                s(&[b"JSON.DEL", b"jn", b"$.b[-9]"], Expect::Int(0)),
                s(&[b"JSON.GET", b"jn", b"$.b"], Expect::Str(b"[[10,20,30]]")),
            ],
        },
        Case {
            family: "json",
            name: "JSON.SET takes no FORMAT option",
            steps: vec![
                // DIVERGENCE (deliberate): RedisJSON takes FORMAT.
                s(
                    &[b"JSON.SET", b"jf", b"$", b"1", b"FORMAT", b"STRING"],
                    Expect::Err("ERR syntax error"),
                ),
                s(&[b"EXISTS", b"jf"], Expect::Int(0)),
            ],
        },
        Case {
            family: "json",
            name: "type gate: WRONGTYPE in both directions",
            steps: vec![
                s(&[b"SET", b"str", b"v"], Expect::Ok),
                s(&[b"JSON.GET", b"str", b"$"], Expect::AnyError),
                s(&[b"JSON.TYPE", b"str"], Expect::AnyError),
                s(&[b"JSON.ARRLEN", b"str"], Expect::AnyError),
                s(&[b"JSON.SET", b"doc", b"$", br#"{"a":1}"#], Expect::Ok),
                s(&[b"GET", b"doc"], Expect::AnyError),
                s(&[b"HGET", b"doc", b"a"], Expect::AnyError),
                s(&[b"LLEN", b"doc"], Expect::AnyError),
                // JSON.SET does NOT overwrite a foreign type, even at the
                // root — unlike a plain SET, which clobbers anything. A
                // document write must not be a silent way to destroy a
                // string or a hash; the caller deletes first if that is
                // what they meant.
                s(&[b"JSON.SET", b"str", b"$", b"[1]"], Expect::AnyError),
                s(&[b"GET", b"str"], Expect::Str(b"v")),
                // The reverse direction stays Redis-normal: plain SET does
                // clobber a document.
                s(&[b"SET", b"doc", b"plain"], Expect::Ok),
                s(&[b"GET", b"doc"], Expect::Str(b"plain")),
            ],
        },
        Case {
            family: "json",
            name: "TTL survives every document write, root replacement included",
            steps: vec![
                s(&[b"JSON.SET", b"d", b"$", br#"{"a":1}"#], Expect::Ok),
                s(&[b"EXPIRE", b"d", b"100"], Expect::Int(1)),
                s(&[b"JSON.SET", b"d", b"$.a", b"2"], Expect::Ok),
                s(&[b"TTL", b"d"], Expect::IntRange(90, 100)),
                s(
                    &[b"JSON.NUMINCRBY", b"d", b"$.a", b"1"],
                    Expect::Str(b"[3]"),
                ),
                s(&[b"TTL", b"d"], Expect::IntRange(90, 100)),
                // Replacing the whole document is still a mutation of an
                // existing key, so the expiry stays — unlike a plain SET,
                // which clears it. Clearing here would silently promote a
                // TTL'd document to an immortal one, and in a cache that
                // leak is worse than the inconsistency with SET.
                s(&[b"JSON.SET", b"d", b"$", br#"{"a":9}"#], Expect::Ok),
                s(&[b"TTL", b"d"], Expect::IntRange(90, 100)),
                s(&[b"JSON.DEL", b"d"], Expect::Int(1)),
                // A fresh key has no expiry to keep.
                s(&[b"JSON.SET", b"d", b"$", br#"{"a":1}"#], Expect::Ok),
                s(&[b"TTL", b"d"], Expect::Int(-1)),
                // An expired document reads as gone.
                s(&[b"JSON.SET", b"t", b"$", b"[1]"], Expect::Ok),
                s(&[b"PEXPIRE", b"t", b"40"], Expect::Int(1)),
                sd(&[b"PING"], Expect::Pong, 80),
                s(&[b"JSON.GET", b"t", b"$"], Expect::Nil),
                s(&[b"EXISTS", b"t"], Expect::Int(0)),
            ],
        },
        Case {
            family: "json",
            name: "inside a transaction, JSON replies keep their shapes",
            // BUG-0242: through the proxy, JSON.TYPE came back one layer
            // deeper and NUMINCRBY as RESP3's array for its RESP2 text,
            // because EXEC's items skipped the repair a reply outside a
            // transaction gets.
            steps: vec![
                s(&[b"JSON.SET", b"{x}j", b"$", br#"{"a":1}"#], Expect::Ok),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"JSON.TYPE", b"{x}j", b"$.a"], Expect::Simple("QUEUED")),
                s(
                    &[b"JSON.NUMINCRBY", b"{x}j", b"$.a", b"1"],
                    Expect::Simple("QUEUED"),
                ),
                s(
                    &[b"JSON.NUMINCRBY", b"{x}j", b".a", b"1"],
                    Expect::Simple("QUEUED"),
                ),
                s(
                    &[b"EXEC"],
                    Expect::Arr(vec![
                        Expect::Arr(vec![Expect::Str(b"integer")]),
                        Expect::Str(b"[2]"),
                        Expect::Str(b"3"),
                    ]),
                ),
            ],
        },
        Case {
            family: "bloom",
            name: "BF lifecycle: add, exists, card, multi forms, TTL",
            steps: vec![
                // BF.ADD auto-creates the filter. 1 = newly added.
                s(&[b"BF.ADD", b"bf", b"a"], Expect::Int(1)),
                s(&[b"BF.ADD", b"bf", b"a"], Expect::Int(0)),
                s(&[b"BF.EXISTS", b"bf", b"a"], Expect::Int(1)),
                s(&[b"BF.EXISTS", b"bf", b"missing"], Expect::Int(0)),
                s(&[b"BF.CARD", b"bf"], Expect::Int(1)),
                s(
                    &[b"BF.MADD", b"bf", b"a", b"b", b"c"],
                    Expect::Arr(vec![Expect::Int(0), Expect::Int(1), Expect::Int(1)]),
                ),
                s(
                    &[b"BF.MEXISTS", b"bf", b"b", b"nope"],
                    Expect::Arr(vec![Expect::Int(1), Expect::Int(0)]),
                ),
                s(&[b"BF.CARD", b"bf"], Expect::Int(3)),
                // A missing key is 0 for EXISTS and CARD, an error for INFO.
                s(&[b"BF.EXISTS", b"gone", b"a"], Expect::Int(0)),
                s(&[b"BF.CARD", b"gone"], Expect::Int(0)),
                s(&[b"BF.INFO", b"gone"], Expect::AnyError),
                // A filter is an ordinary key for the generic commands.
                s(&[b"EXPIRE", b"bf", b"100"], Expect::Int(1)),
                s(&[b"TTL", b"bf"], Expect::IntRange(90, 100)),
                s(&[b"BF.ADD", b"bf", b"d"], Expect::Int(1)),
                s(&[b"TTL", b"bf"], Expect::IntRange(90, 100)),
                s(&[b"DEL", b"bf"], Expect::Int(1)),
                s(&[b"BF.CARD", b"bf"], Expect::Int(0)),
                // An expired filter reads as gone, not as an empty one.
                s(&[b"BF.ADD", b"t", b"x"], Expect::Int(1)),
                s(&[b"PEXPIRE", b"t", b"40"], Expect::Int(1)),
                sd(&[b"PING"], Expect::Pong, 80),
                s(&[b"BF.EXISTS", b"t", b"x"], Expect::Int(0)),
                s(&[b"EXISTS", b"t"], Expect::Int(0)),
            ],
        },
        // The three cases below are SEPARATE because each is a deliberate
        // divergence from RedisBloom (ADR-0016 D7), and `run_case` stops a
        // case at its first failing step. Folded into the lifecycle cases,
        // the first divergence would mask every step after it — which is
        // exactly what happened the first time this corpus met the real
        // module: `TYPE` failing at step 6 meant `BF.SCANDUMP` at step 20
        // was never sent, so a divergence we believed was under test had
        // never once been exercised against RedisBloom.
        Case {
            family: "bloom",
            name: "TYPE and SCAN TYPE name a filter as RedisBloom does",
            steps: vec![
                s(&[b"BF.ADD", b"{s}bf", b"a"], Expect::Int(1)),
                s(&[b"SET", b"{s}str", b"v"], Expect::Ok),
                // RedisBloom's module type name (Jeff, 2026-10-08). Until
                // then this was divergence D7.1, answering `bloom`.
                s(&[b"TYPE", b"{s}bf"], Expect::Simple("MBbloom--")),
                // BUG-0241: SCAN's TYPE filter knew only the core types,
                // so no name found a filter.
                s(
                    &[b"SCAN", b"0", b"TYPE", b"MBbloom--", b"COUNT", b"1000"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"{s}bf"]),
                    ]),
                ),
            ],
        },
        Case {
            family: "bloom",
            name: "DIVERGENCE D7.3: BF.INFO SIZE counts materialised bytes",
            steps: vec![
                s(&[b"BF.RESERVE", b"r", b"0.001", b"5000"], Expect::Ok),
                // Nothing on disk until a block is touched (ADR-0016 D3),
                // so a freshly reserved filter is 0 bytes. RedisBloom
                // allocates the whole filter up front and reports its own
                // number (~6992 in 2.8.16, 9984 in 8.2.8).
                // Both are honest answers to "how big is this filter"; they
                // are answers to different questions, and ours is the one
                // that matches what the tenant is billed for.
                s(
                    &[b"BF.INFO", b"r", b"SIZE"],
                    Expect::Arr(vec![Expect::Int(0)]),
                ),
                // The full form is asserted HERE, in the case already known
                // to diverge, because it carries Size inside it. Against
                // Flint's own engines this is a real regression test on the
                // whole reply — field names included, which is otherwise
                // untested anywhere.
                s(
                    &[b"BF.INFO", b"r"],
                    Expect::Arr(vec![
                        Expect::Simple("Capacity"),
                        Expect::Int(5000),
                        Expect::Simple("Size"),
                        Expect::Int(0),
                        Expect::Simple("Number of filters"),
                        Expect::Int(1),
                        Expect::Simple("Number of items inserted"),
                        Expect::Int(0),
                        Expect::Simple("Expansion rate"),
                        Expect::Int(2),
                    ]),
                ),
            ],
        },
        Case {
            family: "bloom",
            name: "DIVERGENCE D7.4: an unknown BF.RESERVE option is refused",
            steps: vec![
                // The one divergence where WE are the stricter side, and
                // deliberately. RedisBloom 2.8.16 ignores trailing tokens
                // it does not recognise: `BF.RESERVE z 0.01 100 WAT WAT
                // WAT` returns OK, and so does `EXPANSION notanum`.
                //
                // Matching that would mean a misspelled `NONSCALNG` quietly
                // produces a SCALING filter — the caller believes the size
                // is capped, and it grows. An error is recoverable in one
                // line; a filter that silently disobeys the flag it was
                // given is found much later, by capacity.
                s(
                    &[b"BF.RESERVE", b"q", b"0.01", b"100", b"WAT"],
                    Expect::AnyError,
                ),
            ],
        },
        Case {
            family: "bloom",
            name: "DIVERGENCE D7.2: BF.SCANDUMP/BF.LOADCHUNK are refused, not served",
            steps: vec![
                s(&[b"BF.ADD", b"i", b"x"], Expect::Int(1)),
                // RedisBloom serves these. Our block layout differs, so a
                // dump would be a blob that looks portable and is accepted
                // by nothing.
                s(&[b"BF.SCANDUMP", b"i", b"0"], Expect::AnyError),
                // The doc promises BOTH halves are refused, and only this one
                // was checked -- an import path could have been served while
                // the export path was refused, which is the worse direction.
                s(&[b"BF.LOADCHUNK", b"i", b"1", b"zz"], Expect::AnyError),
            ],
        },
        // D7.5 to D7.8, kept by Jeff on 2026-10-08 after an inventory against
        // RedisBloom 8.2.8, each a case to itself for the reason above.
        Case {
            family: "bloom",
            name: "DIVERGENCE D7.5: BF.EXISTS on another type is WRONGTYPE",
            steps: vec![
                s(&[b"SET", b"str", b"v"], Expect::Ok),
                // RedisBloom answers 0, though its BF.ADD and BF.CARD say
                // WRONGTYPE for the same key.
                s(&[b"BF.EXISTS", b"str", b"a"], Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value")),
                s(&[b"BF.MEXISTS", b"str", b"a", b"b"], Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value")),
            ],
        },
        Case {
            family: "bloom",
            name: "DIVERGENCE D7.6: an EXPANSION above 255 is refused",
            steps: vec![
                // RedisBloom takes up to 32768; the growth factor is one
                // byte on disk here.
                s(
                    &[b"BF.RESERVE", b"w", b"0.01", b"100", b"EXPANSION", b"256"],
                    Expect::Err("ERR expansion above 255 is not supported"),
                ),
                s(
                    &[b"BF.INSERT", b"w", b"EXPANSION", b"300", b"ITEMS", b"x"],
                    Expect::Err("ERR expansion above 255 is not supported"),
                ),
            ],
        },
        Case {
            family: "bloom",
            name: "DIVERGENCE D7.7: BF.INSERT option words are spelled out",
            steps: vec![
                // RedisBloom reads `I` as ITEMS, by its first letter.
                s(
                    &[b"BF.INSERT", b"p", b"I", b"x"],
                    Expect::Err("Unknown argument received"),
                ),
            ],
        },
        Case {
            family: "bloom",
            name: "DIVERGENCE D7.8: BF.DEBUG is not served",
            steps: vec![
                s(&[b"BF.ADD", b"d", b"x"], Expect::Int(1)),
                // It prints RedisBloom's in-memory layout, which ours is not.
                s(&[b"BF.DEBUG", b"d"], Expect::AnyError),
            ],
        },
        Case {
            family: "bloom",
            name: "BF.RESERVE parameters and BF.INSERT",
            steps: vec![
                // Error rate FIRST, then capacity — RedisBloom's order.
                s(&[b"BF.RESERVE", b"r", b"0.001", b"5000"], Expect::Ok),
                s(&[b"BF.RESERVE", b"r", b"0.001", b"5000"], Expect::AnyError),
                // A SINGLE-FIELD BF.INFO IS A ONE-ELEMENT ARRAY, not a bare
                // value — RedisBloom answers `*1\r\n:5000\r\n`, and its own
                // client libraries index [0]. Flint returned the bare
                // integer until this was run against the real module.
                s(
                    &[b"BF.INFO", b"r", b"CAPACITY"],
                    Expect::Arr(vec![Expect::Int(5000)]),
                ),
                s(
                    &[b"BF.INFO", b"r", b"ITEMS"],
                    Expect::Arr(vec![Expect::Int(0)]),
                ),
                s(
                    &[b"BF.INFO", b"r", b"FILTERS"],
                    Expect::Arr(vec![Expect::Int(1)]),
                ),
                s(&[b"BF.ADD", b"r", b"x"], Expect::Int(1)),
                s(&[b"BF.EXISTS", b"r", b"x"], Expect::Int(1)),
                // NONSCALING has no growth factor: a nil, and the nil is
                // wrapped in the one-element array like any other field.
                s(
                    &[b"BF.RESERVE", b"n", b"0.01", b"100", b"NONSCALING"],
                    Expect::Ok,
                ),
                s(
                    &[b"BF.INFO", b"n", b"EXPANSION"],
                    Expect::Arr(vec![Expect::Nil]),
                ),
                s(
                    &[b"BF.RESERVE", b"e", b"0.01", b"100", b"EXPANSION", b"4"],
                    Expect::Ok,
                ),
                s(
                    &[b"BF.INFO", b"e", b"EXPANSION"],
                    Expect::Arr(vec![Expect::Int(4)]),
                ),
                // An unknown section is a BARE error, not a wrapped one.
                s(&[b"BF.INFO", b"e", b"NOSUCH"], Expect::AnyError),
                s(&[b"BF.RESERVE", b"q", b"nope", b"100"], Expect::AnyError),
                // BF.INSERT reserves and adds in one round trip.
                s(
                    &[
                        b"BF.INSERT",
                        b"i",
                        b"CAPACITY",
                        b"1000",
                        b"ITEMS",
                        b"x",
                        b"y",
                    ],
                    Expect::Arr(vec![Expect::Int(1), Expect::Int(1)]),
                ),
                s(&[b"BF.EXISTS", b"i", b"y"], Expect::Int(1)),
                s(
                    &[b"BF.INSERT", b"absent", b"NOCREATE", b"ITEMS", b"x"],
                    Expect::AnyError,
                ),
                // WRONGTYPE both directions.
                s(&[b"SET", b"str", b"v"], Expect::Ok),
                s(&[b"BF.ADD", b"str", b"x"], Expect::AnyError),
                s(&[b"GET", b"i"], Expect::AnyError),
            ],
        },
        Case {
            family: "bloom",
            name: "inside a transaction, BF replies keep their shapes",
            // BUG-0242: the proxy repaired a one-field BF.INFO outside EXEC
            // and not inside it.
            steps: vec![
                s(&[b"BF.RESERVE", b"{x}b", b"0.01", b"10"], Expect::Ok),
                s(&[b"MULTI"], Expect::Ok),
                s(&[b"BF.INFO", b"{x}b", b"CAPACITY"], Expect::Simple("QUEUED")),
                s(&[b"BF.ADD", b"{x}b", b"a"], Expect::Simple("QUEUED")),
                s(&[b"BF.INFO", b"{x}b", b"ITEMS"], Expect::Simple("QUEUED")),
                s(
                    &[b"EXEC"],
                    Expect::Arr(vec![
                        Expect::Arr(vec![Expect::Int(10)]),
                        Expect::Int(1),
                        Expect::Arr(vec![Expect::Int(1)]),
                    ]),
                ),
            ],
        },
        Case {
            family: "bloom",
            name: "a batch that fills a filter answers each item, the error in its place",
            // BUG-0237: the items before the refusal are stored, so one
            // error for the whole batch told the caller nothing was.
            steps: vec![
                s(
                    &[b"BF.RESERVE", b"f", b"0.001", b"2", b"NONSCALING"],
                    Expect::Ok,
                ),
                s(&[b"BF.ADD", b"f", b"a"], Expect::Int(1)),
                s(
                    &[b"BF.MADD", b"f", b"a", b"b", b"c", b"d"],
                    Expect::Arr(vec![Expect::Int(0), Expect::Int(1), Expect::Err("ERR non scaling filter is full")]),
                ),
                s(&[b"BF.EXISTS", b"f", b"b"], Expect::Int(1)),
                s(&[b"BF.CARD", b"f"], Expect::Int(2)),
                s(
                    &[b"BF.INSERT", b"f", b"ITEMS", b"e", b"g"],
                    Expect::Arr(vec![Expect::Err("ERR non scaling filter is full")]),
                ),
                s(&[b"BF.ADD", b"f", b"e"], Expect::Err("ERR non scaling filter is full")),
            ],
        },
        Case {
            family: "bloom",
            name: "NONSCALING holds against EXPANSION, and EXPANSION 0 is NONSCALING",
            // BUG-0238: the later option won, so `NONSCALING EXPANSION 2`
            // made a filter that grows.
            steps: vec![
                s(
                    &[b"BF.INSERT", b"a", b"NONSCALING", b"EXPANSION", b"2", b"ITEMS", b"x"],
                    Expect::Arr(vec![Expect::Int(1)]),
                ),
                s(
                    &[b"BF.INFO", b"a", b"EXPANSION"],
                    Expect::Arr(vec![Expect::Nil]),
                ),
                s(
                    &[b"BF.INSERT", b"b", b"EXPANSION", b"0", b"ITEMS", b"x"],
                    Expect::Arr(vec![Expect::Int(1)]),
                ),
                s(
                    &[b"BF.INFO", b"b", b"EXPANSION"],
                    Expect::Arr(vec![Expect::Nil]),
                ),
                s(
                    &[b"BF.RESERVE", b"r", b"0.01", b"100", b"NONSCALING", b"EXPANSION", b"2"],
                    Expect::Err("Nonscaling filters cannot expand"),
                ),
                s(
                    &[b"BF.RESERVE", b"r", b"0.01", b"100", b"EXPANSION", b"0"],
                    Expect::Ok,
                ),
                s(
                    &[b"BF.INFO", b"r", b"EXPANSION"],
                    Expect::Arr(vec![Expect::Nil]),
                ),
            ],
        },
        Case {
            family: "bloom",
            name: "BF arguments are refused in RedisBloom's words, before the key",
            // BUG-0240: an existing filter or another type answered first,
            // and the words were Flint's own.
            steps: vec![
                s(&[b"SET", b"str", b"v"], Expect::Ok),
                s(&[b"BF.RESERVE", b"f", b"0.01", b"10"], Expect::Ok),
                s(
                    &[b"BF.RESERVE", b"f", b"0", b"100"],
                    Expect::Err("ERR error rate must be in the range (0.000000, 1.000000)"),
                ),
                s(
                    &[b"BF.RESERVE", b"str", b"0.01", b"0"],
                    Expect::Err("ERR capacity must be in the range [1, 1073741824]"),
                ),
                s(
                    &[b"BF.RESERVE", b"n", b"0.01", b"1073741825"],
                    Expect::Err("ERR capacity must be in the range [1, 1073741824]"),
                ),
                s(
                    &[b"BF.RESERVE", b"n", b"0.01", b"100", b"EXPANSION"],
                    Expect::Err("ERR no expansion"),
                ),
                s(
                    &[b"BF.RESERVE", b"n", b"0.01", b"100", b"EXPANSION", b"-1"],
                    Expect::Err("ERR expansion must be in the range [0, 32768]"),
                ),
                s(
                    &[b"BF.INSERT", b"f", b"CAPACITY", b"0", b"ITEMS", b"x"],
                    Expect::Err("Bad capacity"),
                ),
                s(
                    &[b"BF.INSERT", b"str", b"ERROR", b"1", b"ITEMS", b"x"],
                    Expect::Err("Bad error rate"),
                ),
                s(
                    &[b"BF.INSERT", b"n", b"WAT", b"ITEMS", b"x"],
                    Expect::Err("Unknown argument received"),
                ),
                s(
                    &[b"BF.INSERT", b"n", b"CAPACITY", b"10"],
                    Expect::Err("ERR wrong number of arguments for 'bf.insert' command"),
                ),
                s(
                    &[b"BF.RESERVE", b"f", b"0.01", b"100"],
                    Expect::Err("ERR item exists"),
                ),
            ],
        },
        Case {
            family: "scan",
            name: "one-shot enumeration returns every key, cursor 0",
            steps: vec![
                s(&[b"SET", b"sc:a", b"1"], Expect::Ok),
                s(&[b"SET", b"sc:b", b"1"], Expect::Ok),
                s(&[b"SET", b"sc:c", b"1"], Expect::Ok),
                s(
                    &[b"SCAN", b"0", b"COUNT", b"1000"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"sc:a", b"sc:b", b"sc:c"]),
                    ]),
                ),
            ],
        },
        Case {
            family: "scan",
            name: "scan options are read after the key, in upstream's words",
            // BUG-0225: a missing key answers an empty scan whatever follows
            // it, another type answers WRONGTYPE, and then a COUNT that is not
            // an integer and a NOVALUES outside HSCAN are named as such.
            steps: vec![
                s(&[b"SADD", b"{so}s", b"a"], Expect::Int(1)),
                s(
                    &[b"SSCAN", b"{so}none", b"0", b"BOGUS"],
                    Expect::Arr(vec![Expect::Str(b"0"), Expect::Arr(vec![])]),
                ),
                s(
                    &[b"HSCAN", b"{so}none", b"0", b"COUNT", b"abc"],
                    Expect::Arr(vec![Expect::Str(b"0"), Expect::Arr(vec![])]),
                ),
                s(
                    &[b"ZSCAN", b"{so}s", b"0", b"COUNT", b"abc"],
                    Expect::Err("WRONGTYPE Operation against a key holding the wrong kind of value"),
                ),
                s(
                    &[b"SSCAN", b"{so}s", b"0", b"COUNT", b"abc"],
                    Expect::Err("ERR value is not an integer or out of range"),
                ),
                s(&[b"SSCAN", b"{so}s", b"0", b"COUNT", b"0"], Expect::Err("ERR syntax error")),
                s(
                    &[b"SSCAN", b"{so}s", b"0", b"NOVALUES"],
                    Expect::Err("ERR NOVALUES option can only be used in HSCAN"),
                ),
                s(
                    &[b"SCAN", b"0", b"COUNT", b"abc"],
                    Expect::Err("ERR value is not an integer or out of range"),
                ),
                s(
                    &[b"SCAN", b"0", b"NOVALUES"],
                    Expect::Err("ERR NOVALUES option can only be used in HSCAN"),
                ),
                s(&[b"DEL", b"{so}s"], Expect::Int(1)),
            ],
        },
        Case {
            family: "scan",
            name: "empty keyspace scans clean",
            steps: vec![s(
                &[b"SCAN", b"0"],
                Expect::Arr(vec![Expect::Str(b"0"), Expect::UnorderedStrs(vec![])]),
            )],
        },
        Case {
            family: "scan",
            name: "MATCH filters with * ? and [] globs",
            steps: vec![
                s(&[b"SET", b"user:1", b"1"], Expect::Ok),
                s(&[b"SET", b"user:2", b"1"], Expect::Ok),
                s(&[b"SET", b"other", b"1"], Expect::Ok),
                s(
                    &[b"SCAN", b"0", b"MATCH", b"user:*", b"COUNT", b"1000"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"user:1", b"user:2"]),
                    ]),
                ),
                s(
                    &[b"SCAN", b"0", b"MATCH", b"user:?", b"COUNT", b"1000"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"user:1", b"user:2"]),
                    ]),
                ),
                s(
                    &[b"SCAN", b"0", b"MATCH", b"user:[1]", b"COUNT", b"1000"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"user:1"]),
                    ]),
                ),
            ],
        },
        Case {
            family: "scan",
            name: "TYPE filter selects by value type",
            steps: vec![
                s(&[b"SET", b"t:s", b"1"], Expect::Ok),
                s(&[b"HSET", b"t:h", b"f", b"v"], Expect::Int(1)),
                s(
                    &[b"SCAN", b"0", b"TYPE", b"hash", b"COUNT", b"1000"],
                    Expect::Arr(vec![Expect::Str(b"0"), Expect::UnorderedStrs(vec![b"t:h"])]),
                ),
            ],
        },
        Case {
            family: "scan",
            name: "expired keys are not enumerated",
            steps: vec![
                s(&[b"SET", b"gone", b"1", b"PX", b"40"], Expect::Ok),
                s(&[b"SET", b"stays", b"1"], Expect::Ok),
                sd(&[b"PING"], Expect::Pong, 80),
                s(
                    &[b"SCAN", b"0", b"COUNT", b"1000"],
                    Expect::Arr(vec![
                        Expect::Str(b"0"),
                        Expect::UnorderedStrs(vec![b"stays"]),
                    ]),
                ),
            ],
        },
    ]
}

/// The host part of a `host:port`, leaving a bracketed IPv6 literal intact.
fn endpoint_host(target: &str) -> &str {
    match target.strip_prefix('[').and_then(|r| r.split_once(']')) {
        Some((v6, _)) => v6,
        None => target.split(':').next().unwrap_or(target),
    }
}

/// Where the corpus runs, and what it takes to get in.
///
/// One value rather than three loose arguments, because the three are not
/// independent: TLS without a trust anchor cannot be validated, and auth
/// without TLS ships a token in the clear.
struct Endpoint {
    target: String,
    tls: Option<Arc<flint_tls::ClientConfig>>,
    /// `(tenant, token)` for the two-argument AUTH a proxy expects.
    auth: Option<(Vec<u8>, Vec<u8>)>,
}

struct Client {
    stream: flint_tls::Stream,
    buf: Vec<u8>,
    proto: Proto,
    /// The commands queued since MULTI, so EXEC's items fold as each
    /// command's own reply folds (BUG-0242). `None` outside a transaction.
    queued: Option<Vec<Vec<Vec<u8>>>>,
}

impl Client {
    fn connect(ep: &Endpoint, proto: Proto) -> std::io::Result<Self> {
        // The handshake runs with normalization still OFF: the server
        // answers HELLO 3 in the dialect it just switched to, and folding
        // that map down to an array here would hide the very thing we are
        // checking. Adopt the dialect only once it is confirmed.
        //
        // connect_edge with a None config IS a plain TcpStream, so the local
        // targets every drill uses behave exactly as before. Edge SNI (the
        // host part of the address) rather than the mesh's fixed name,
        // because this dials the tenant-facing listener.
        let mut c = Self {
            stream: flint_tls::connect_edge(&ep.target, &ep.tls)?,
            buf: Vec::new(),
            proto: Proto::Resp2,
            queued: None,
        };
        // Before HELLO, so a rejected credential fails here with -WRONGPASS
        // rather than as ninety-nine identical case failures.
        if let Some((user, token)) = &ep.auth {
            let auth = vec![b"AUTH".to_vec(), user.clone(), token.clone()];
            match c.call(&auth)? {
                Value::Simple(s) if s == "OK" => {}
                other => {
                    return Err(std::io::Error::other(format!("AUTH refused: {other:?}")));
                }
            }
        }
        if proto == Proto::Resp3 {
            let hello = vec![b"HELLO".to_vec(), b"3".to_vec()];
            match c.call(&hello)? {
                Value::Map(_) => {}
                other => {
                    return Err(std::io::Error::other(format!(
                        "target refused HELLO 3: {other:?}"
                    )));
                }
            }
        }
        c.proto = proto;
        Ok(c)
    }

    fn call(&mut self, args: &[Vec<u8>]) -> std::io::Result<Value> {
        let frame = Value::Array(Some(
            args.iter().map(|a| Value::Bulk(Some(a.clone()))).collect(),
        ));
        let mut out = Vec::new();
        encode(&frame, &mut out);
        self.stream.write_all(&out)?;
        let mut chunk = [0u8; 16 * 1024];
        loop {
            match decode(&self.buf) {
                Ok(Decoded::Complete(value, used)) => {
                    self.buf.drain(..used);
                    return Ok(self.track_and_normalize(args, value));
                }
                Ok(Decoded::NeedMore) => {
                    let n = self.stream.read(&mut chunk)?;
                    if n == 0 {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "server closed connection mid-reply",
                        ));
                    }
                    self.buf.extend_from_slice(&chunk[..n]);
                }
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("protocol error from server: {e:?}"),
                    ));
                }
            }
        }
    }
}

impl Client {
    /// Fold a RESP3 reply back to the RESP2 shape the corpus asserts.
    ///
    /// The corpus is written once, in RESP2 terms, and run under both
    /// dialects. That is deliberate: what a RESP3 pass should prove is not
    /// that the bytes differ — of course they do — but that they carry the
    /// SAME INFORMATION. Downgrading through the very encoder the server
    /// uses and re-decoding is the sharpest available check of that, and it
    /// catches the failures that matter: a map paired up wrong, a score
    /// that lost precision becoming a double, a set that dropped a member.
    fn normalize(&self, args: &[Vec<u8>], v: Value) -> Value {
        if self.proto == Proto::Resp2 {
            return v;
        }
        let v = Self::fold(args, v);
        self.downgrade(v)
    }

    /// Keep the commands a transaction queues, and fold each item of EXEC's
    /// reply as that command's reply folds outside a transaction. Without
    /// this a RESP3 run could not hold a JSON.TYPE, NUMINCRBY or one-field
    /// BF.INFO inside EXEC to the RESP2 shape, which is the case the proxy
    /// got wrong (BUG-0242).
    fn track_and_normalize(&mut self, args: &[Vec<u8>], v: Value) -> Value {
        let is = |n: &[u8]| args.first().is_some_and(|a| a.eq_ignore_ascii_case(n));
        if is(b"EXEC") || is(b"DISCARD") {
            let queued = self.queued.take().unwrap_or_default();
            let v = match v {
                Value::Array(Some(items)) if is(b"EXEC") && items.len() == queued.len() => {
                    Value::Array(Some(
                        items
                            .into_iter()
                            .zip(&queued)
                            .map(|(item, cmd)| match self.proto {
                                Proto::Resp3 => Self::fold(cmd, item),
                                Proto::Resp2 => item,
                            })
                            .collect(),
                    ))
                }
                other => other,
            };
            return self.normalize(args, v);
        }
        if is(b"MULTI") && matches!(&v, Value::Simple(s) if s == "OK") {
            self.queued = Some(Vec::new());
        } else if let Some(q) = self.queued.as_mut()
            && matches!(&v, Value::Simple(s) if s == "QUEUED")
        {
            q.push(args.to_vec());
        }
        self.normalize(args, v)
    }

    /// The per-command half of [`Client::normalize`]: the replies whose
    /// two dialects differ in a way the generic downgrade cannot recover.
    fn fold(args: &[Vec<u8>], v: Value) -> Value {
        // JSON.TYPE carries an extra array layer under RESP3 (RedisJSON's
        // quirk, which we match); peel it before comparing.
        let v = match args.first() {
            Some(n) if flint_resp::resp3_nests_reply(n) => match v {
                Value::Array(Some(mut items)) if items.len() == 1 => items.remove(0),
                other => other,
            },
            _ => v,
        };
        // JSON.NUMINCRBY answers a typed array under RESP3 and JSON text
        // under RESP2 — a difference in KIND, so it takes the same rebuild
        // the proxy uses rather than a generic re-render.
        let v = match args.first() {
            Some(n) if flint_resp::resp3_differs_in_kind(n) && !matches!(v, Value::Error(_)) => {
                flint_resp::json_numincrby_resp2(&v, args.get(2).map(|p| p.as_slice()))
            }
            _ => v,
        };
        // HRANDFIELD ... WITHVALUES nests its pairs under RESP3.
        let v = match flint_resp::hrandfield_withvalues(args) {
            true => flint_resp::flatten_pairs(&v),
            false => v,
        };
        // A one-field BF.INFO is a one-pair map under RESP3; RESP2's reply
        // is the value alone, in a one-element array (BUG-0239).
        let v = match flint_resp::bf_info_field(args) {
            true => flint_resp::bf_info_field_resp2(&v),
            false => v,
        };
        // XREAD is a map under RESP3 and a list of pairs under RESP2
        // (ADR-0052 D6).
        match args.first() {
            Some(n) if n.eq_ignore_ascii_case(b"XREAD") => flint_resp::xread_resp2(&v),
            _ => v,
        }
    }

    /// The generic half of [`Client::normalize`]: re-render through the
    /// server's own RESP2 encoder.
    fn downgrade(&self, v: Value) -> Value {
        // Score pairs are the one shape the wire cannot hand back as
        // itself: `ScorePairs` encodes to nested [member, double] arrays,
        // and decoding those yields exactly that — plain arrays, with no
        // way to tell they were pairs. So recognize the shape and flatten
        // it the way RESP2 would have. The recognizer is deliberately
        // narrow (every element a 2-tuple of bulk-then-double), which in
        // this command set only ZRANGE-family WITHSCORES and ZPOPMIN/MAX
        // with a count produce.
        let v = match v {
            Value::Array(Some(items))
                if !items.is_empty()
                    && items.iter().all(|it| {
                        matches!(it, Value::Array(Some(p))
                            if p.len() == 2
                                && matches!(p[0], Value::Bulk(Some(_)))
                                && matches!(p[1], Value::Double(_)))
                    }) =>
            {
                Value::Array(Some(
                    items
                        .into_iter()
                        .flat_map(|it| match it {
                            Value::Array(Some(p)) => p,
                            other => vec![other],
                        })
                        .collect(),
                ))
            }
            other => other,
        };
        // The one value the RESP2 downgrade must NOT touch. RESP3 has a
        // single null, `_`; RESP2 has two, `$-1` and `*-1`. Re-encoding a
        // top-level `Value::Null` as RESP2 has to pick one, it picks the
        // null bulk, and the null ARRAY an aborted EXEC returns then reads
        // as a null bulk — a case asserting NilArray fails against a server
        // that answered correctly.
        //
        // Kept as itself so `matches` can decide with the protocol in hand
        // and the failure text can say `Null` rather than a shape the
        // server never sent. Nulls NESTED inside an array still downgrade,
        // which is right: RESP2 spells those `$-1` and the corpus expects
        // Nil for them.
        if matches!(v, Value::Null) {
            return v;
        }
        let mut buf = Vec::new();
        flint_resp::encode_proto(&v, Proto::Resp2, &mut buf);
        match decode(&buf) {
            Ok(Decoded::Complete(down, _)) => down,
            // Un-downgradable is a real failure; hand the original back so
            // the mismatch is reported against what the server actually
            // sent rather than swallowed here.
            _ => v,
        }
    }
}

fn matches(expect: &Expect, got: &Value, proto: Proto) -> bool {
    // RESP3 has exactly ONE null. The `Nil` / `NilArray` split is a
    // statement about RESP2, where `$-1` and `*-1` are different replies and
    // a case must not be allowed to accept either; under RESP3 both are `_`,
    // and demanding a distinction the protocol deleted would fail a server
    // that answered correctly.
    //
    // So the strictness is preserved exactly where it means something and
    // dropped exactly where it cannot. This is NOT a widening of the RESP2
    // side: `Value::Null` only ever reaches here from a RESP3 run.
    let sole_resp3_null = proto == Proto::Resp3 && *got == Value::Null;
    match expect {
        Expect::Ok => *got == Value::Simple("OK".into()),
        Expect::Pong => *got == Value::Simple("PONG".into()),
        Expect::Nil => *got == Value::Bulk(None) || sole_resp3_null,
        Expect::NilArray => *got == Value::Array(None) || sole_resp3_null,
        Expect::Int(i) => *got == Value::Integer(*i),
        Expect::IntRange(lo, hi) => matches!(got, Value::Integer(n) if n >= lo && n <= hi),
        Expect::Simple(t) => *got == Value::Simple((*t).into()),
        Expect::Str(s) => *got == Value::Bulk(Some(s.to_vec())),
        Expect::Bytes(b) => *got == Value::Bulk(Some(b.clone())),
        Expect::AnyError => matches!(got, Value::Error(_)),
        Expect::Err(t) => matches!(got, Value::Error(e) if e == t),
        Expect::AnyBulk => matches!(got, Value::Bulk(Some(_))),
        Expect::Json(want) => match got {
            Value::Bulk(Some(b)) => {
                match (
                    serde_json::from_slice::<serde_json::Value>(b),
                    serde_json::from_slice::<serde_json::Value>(want),
                ) {
                    (Ok(g), Ok(w)) => g == w,
                    _ => false,
                }
            }
            _ => false,
        },
        Expect::AnyArray => matches!(got, Value::Array(Some(_))),
        Expect::StrContains(s) => matches!(
            got,
            Value::Bulk(Some(b)) if b.windows(s.len()).any(|w| w == *s)
        ),
        Expect::Arr(items) => match got {
            Value::Array(Some(vals)) if vals.len() == items.len() => {
                items.iter().zip(vals).all(|(e, v)| {
                    // A null NESTED in a RESP3 reply was downgraded to `$-1`
                    // by `normalize`, which cannot know that RESP2 would
                    // have spelled this one `*-1`: an EXEC reply holding a
                    // BLPOP that found nothing (ADR-0052 D4). Under RESP3
                    // it was the one null, so a nested null array takes it.
                    // Only here: a top-level `$-1` to a RESP3 client is a
                    // protocol violation and stays refused.
                    (proto == Proto::Resp3
                        && matches!(e, Expect::NilArray)
                        && *v == Value::Bulk(None))
                        || matches(e, v, proto)
                })
            }
            _ => false,
        },
        Expect::UnorderedStrs(items) => match got {
            Value::Array(Some(vals)) if vals.len() == items.len() => {
                let mut got_s: Vec<Vec<u8>> = Vec::new();
                for v in vals {
                    match v {
                        Value::Bulk(Some(b)) => got_s.push(b.clone()),
                        _ => return false,
                    }
                }
                got_s.sort();
                let mut want: Vec<Vec<u8>> = items.iter().map(|i| i.to_vec()).collect();
                want.sort();
                got_s == want
            }
            _ => false,
        },
        Expect::UnorderedPairs(pairs) => match got {
            Value::Array(Some(vals)) if vals.len() == pairs.len() * 2 => {
                let mut got_pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                for chunk in vals.chunks(2) {
                    match (&chunk[0], &chunk[1]) {
                        (Value::Bulk(Some(f)), Value::Bulk(Some(v))) => {
                            got_pairs.push((f.clone(), v.clone()));
                        }
                        _ => return false,
                    }
                }
                got_pairs.sort();
                let mut want: Vec<(Vec<u8>, Vec<u8>)> = pairs
                    .iter()
                    .map(|(f, v)| (f.to_vec(), v.to_vec()))
                    .collect();
                want.sort();
                got_pairs == want
            }
            _ => false,
        },
    }
}

fn render(args: &[Vec<u8>]) -> String {
    args.iter()
        .map(|a| String::from_utf8_lossy(a).into_owned())
        .collect::<Vec<_>>()
        .join(" ")
}

fn main() -> ExitCode {
    let target = std::env::args()
        .skip_while(|a| a != "--target")
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:6380".into());

    // --reference: the target is valkey/redis, which validates the corpus
    // itself. Families the reference does not implement are skipped rather
    // than reported as failures — a red line there would say nothing about
    // either implementation.
    let reference = std::env::args().any(|a| a == "--reference");
    // "the target is not a Flint seat", for a target that is not the
    // reference either -- a real Redis with a module loaded.
    let foreign = std::env::args().any(|a| a == "--foreign");

    // --proto 3 runs the whole corpus over RESP3, folding each reply back
    // to its RESP2 shape before matching (see `Client::normalize`).
    let proto = match std::env::args()
        .skip_while(|a| a != "--proto")
        .nth(1)
        .as_deref()
    {
        Some("3") => Proto::Resp3,
        _ => Proto::Resp2,
    };

    let flag = |name: &str| std::env::args().any(|a| a == name);
    let opt = |name: &'static str| -> Option<String> {
        std::env::args()
            .skip_while(move |a| a != name)
            .nth(1)
            .filter(|v| !v.starts_with("--"))
    };

    // TLS needs a trust anchor named explicitly. There is no "use the system
    // store" default on purpose: which bundle validates the edge is a fact
    // about the deployment (an internal CA for a default install, a public
    // bundle for a Let's Encrypt edge), and guessing it wrong fails as a
    // handshake error that reads like the server being down.
    let tls = if flag("--tls") {
        let ca = match opt("--ca") {
            Some(p) => p,
            None => {
                eprintln!(
                    "--tls needs --ca <trust-bundle> (e.g. /etc/pki/tls/certs/ca-bundle.crt)"
                );
                return ExitCode::FAILURE;
            }
        };
        match flint_tls::edge_client_config(&ca) {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("--ca {ca}: {e}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        None
    };

    let auth = match opt("--auth") {
        None => None,
        Some(spec) => match spec.split_once(':') {
            Some((u, t)) if !u.is_empty() && !t.is_empty() => {
                Some((u.as_bytes().to_vec(), t.as_bytes().to_vec()))
            }
            _ => {
                eprintln!("--auth takes <tenant>:<token>");
                return ExitCode::FAILURE;
            }
        },
    };

    // The guard that makes remote mode safe to hand to someone else. Every
    // case FLUSHALLs for a clean keyspace; through a proxy that erases the
    // authenticated tenant's namespace. Refusing without the acknowledgement
    // is the difference between destroying data someone chose to destroy and
    // destroying data they did not know was in scope.
    if auth.is_some() && !flag("--yes-flushall") {
        eprintln!("refusing to run against an authenticated endpoint without --yes-flushall.");
        eprintln!();
        eprintln!("Every case begins with FLUSHALL to get a clean keyspace. Through a proxy");
        eprintln!("that ERASES THE TENANT'S NAMESPACE. Use a throwaway tenant, then pass");
        eprintln!("--yes-flushall to say so.");
        return ExitCode::FAILURE;
    }
    // A token in clear text is a problem when it crosses a network, not when
    // it goes to a socket on this machine. Refusing loopback too would ban
    // the case this whole feature exists to enable in CI: the corpus through
    // a drill fleet's plaintext proxy, which is the ONLY way to exercise the
    // proxy's own RESP behaviour — every gate run before this dialled a node
    // directly and never saw the edge at all.
    let loopback = matches!(
        endpoint_host(&target),
        "127.0.0.1" | "::1" | "localhost" | "[::1]"
    );
    if auth.is_some() && tls.is_none() && !loopback {
        eprintln!(
            "--auth over a non-loopback target without --tls would send the token in clear text;"
        );
        eprintln!("add --tls --ca <bundle>.");
        return ExitCode::FAILURE;
    }

    let endpoint = Endpoint { target, tls, auth };

    let mut per_family: BTreeMap<&str, (u32, u32)> = BTreeMap::new();
    let mut failures: Vec<String> = Vec::new();
    let mut skipped = 0u32;
    let mut skipped_seat = 0u32;
    let mut skipped_edge = 0u32;

    // A proxy run authenticates as a tenant; a seat has no tenant auth. That
    // is the signal, and `--foreign` is the explicit form for a target that
    // is neither (the RedisJSON/RedisBloom compare scripts, which DO want the
    // json and bloom families to run and cannot serve `FLINT*`).
    let not_a_seat = reference || foreign || endpoint.auth.is_some();

    for case in corpus() {
        if (reference && flint_only(case.family)) || (foreign && foreign_skips(case.family)) {
            skipped += 1;
            continue;
        }
        if not_a_seat && seat_only(case.family) {
            skipped_seat += 1;
            continue;
        }
        if !not_a_seat && edge_only(case.family) {
            skipped_edge += 1;
            continue;
        }
        let entry = per_family.entry(case.family).or_insert((0, 0));
        entry.1 += 1;
        let result = run_case(&endpoint, &case, proto);
        match result {
            Ok(None) => entry.0 += 1,
            Ok(Some(failure)) => {
                failures.push(format!("[{}] {}: {failure}", case.family, case.name))
            }
            Err(e) => failures.push(format!("[{}] {}: io error: {e}", case.family, case.name)),
        }
    }

    // The transport is part of the result, not decoration: "99/99 against
    // localhost plaintext" and "99/99 through a TLS edge as a tenant" are
    // different claims, and a run that silently fell back to the weaker one
    // would read identically without this.
    println!(
        "target: {} (RESP{}{}{})",
        endpoint.target,
        proto.version(),
        if endpoint.tls.is_some() { ", TLS" } else { "" },
        if endpoint.auth.is_some() {
            ", authenticated"
        } else {
            ""
        },
    );
    let (mut pass, mut total) = (0, 0);
    for (family, (p, t)) in &per_family {
        println!("  {family:<12} {p}/{t}");
        pass += p;
        total += t;
    }
    println!(
        "overall: {pass}/{total} ({:.1}%)",
        100.0 * pass as f64 / total as f64
    );
    if skipped > 0 {
        println!("  ({skipped} flint-only case(s) skipped: no oracle on this target)");
    }
    if skipped_edge > 0 {
        println!(
            "  ({skipped_edge} edge-only case(s) skipped: subscriptions are held by the proxy, \
             and a seat refuses them)"
        );
    }
    if skipped_seat > 0 {
        println!(
            "  ({skipped_seat} seat-only case(s) skipped: the FLINT* admin surface \
             is not served to this target)"
        );
    }
    if failures.is_empty() {
        ExitCode::SUCCESS
    } else {
        println!("\nfailures:");
        for f in &failures {
            println!("  {f}");
        }
        ExitCode::FAILURE
    }
}

/// Runs one case on a fresh connection with a clean keyspace.
/// Ok(None) = pass; Ok(Some(msg)) = semantic failure; Err = transport failure.
fn run_case(ep: &Endpoint, case: &Case, proto: Proto) -> std::io::Result<Option<String>> {
    let mut client = Client::connect(ep, proto)?;
    let flushed = client.call(&cmd(&[b"FLUSHALL"]))?;
    if flushed != Value::Simple("OK".into()) {
        return Ok(Some(format!("FLUSHALL failed: {flushed:?}")));
    }
    for (step_no, (args, expect, delay_ms)) in case.steps.iter().enumerate() {
        let got = client.call(args)?;
        if !matches(expect, &got, proto) {
            return Ok(Some(format!(
                "step {}: `{}` expected {:?}, got {:?}",
                step_no + 1,
                render(args),
                expect,
                got
            )));
        }
        if *delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(*delay_ms));
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RESP3 has one null; RESP2 has two. The matcher must relax EXACTLY
    /// there and nowhere else — this is the whole content of the change, and
    /// the direction that matters is the RESP2 half: relaxing it would let a
    /// case assert `NilArray` and silently accept a null bulk, which is the
    /// bug that shipped through the proxy (an aborted EXEC answering `$-1`).
    #[test]
    fn resp2_keeps_the_two_nulls_apart() {
        assert!(matches(&Expect::Nil, &Value::Bulk(None), Proto::Resp2));
        assert!(matches(
            &Expect::NilArray,
            &Value::Array(None),
            Proto::Resp2
        ));
        // The crossings, which must all stay refused.
        assert!(!matches(&Expect::Nil, &Value::Array(None), Proto::Resp2));
        assert!(!matches(
            &Expect::NilArray,
            &Value::Bulk(None),
            Proto::Resp2
        ));
        // RESP2 never carries the RESP3 null, and must not learn to accept
        // it: that would make the relaxation leak across protocols.
        assert!(!matches(&Expect::Nil, &Value::Null, Proto::Resp2));
        assert!(!matches(&Expect::NilArray, &Value::Null, Proto::Resp2));
    }

    #[test]
    fn resp3_has_exactly_one_null_and_both_expectations_take_it() {
        assert!(matches(&Expect::Nil, &Value::Null, Proto::Resp3));
        assert!(matches(&Expect::NilArray, &Value::Null, Proto::Resp3));
    }

    /// The relaxation accepts the RESP3 null — NOT "any null-ish thing".
    /// A server that answers a RESP3 client with `$-1` is violating the
    /// protocol (a RESP3 parser sits waiting for a payload that never
    /// comes), and this is the assertion that keeps the EXEC bug class
    /// visible on the dialect most clients negotiate.
    #[test]
    fn resp3_still_refuses_the_resp2_spellings() {
        assert!(!matches(
            &Expect::NilArray,
            &Value::Bulk(None),
            Proto::Resp3
        ));
        assert!(!matches(&Expect::Nil, &Value::Array(None), Proto::Resp3));
    }

    /// Nested, a RESP3 null reaches the matcher already downgraded to `$-1`,
    /// so a nested null array takes it under RESP3 (an EXEC reply holding a
    /// BLPOP that found nothing, ADR-0052). Under RESP2 the crossing stays
    /// refused, and at the top level both dialects stay as they were.
    #[test]
    fn a_nested_null_array_takes_the_downgraded_resp3_null_only() {
        let want = Expect::Arr(vec![Expect::NilArray]);
        let nested = Value::Array(Some(vec![Value::Bulk(None)]));
        assert!(matches(&want, &nested, Proto::Resp3));
        assert!(!matches(&want, &nested, Proto::Resp2));
        assert!(!matches(
            &Expect::NilArray,
            &Value::Bulk(None),
            Proto::Resp3
        ));
    }

    /// Relaxing the null must not turn into relaxing anything else — a
    /// matcher that accepted a null for a real value would pass a server
    /// that answered nothing at all.
    #[test]
    fn a_null_still_satisfies_nothing_but_a_null() {
        for p in [Proto::Resp2, Proto::Resp3] {
            assert!(!matches(&Expect::Ok, &Value::Null, p));
            assert!(!matches(&Expect::Int(0), &Value::Null, p));
            assert!(!matches(&Expect::Str(b"x"), &Value::Null, p));
            assert!(!matches(&Expect::Arr(vec![]), &Value::Null, p));
            assert!(!matches(&Expect::AnyError, &Value::Null, p));
        }
        // And the reverse: a real value never satisfies a null expectation.
        assert!(!matches(
            &Expect::NilArray,
            &Value::Array(Some(vec![])),
            Proto::Resp3
        ));
        assert!(!matches(
            &Expect::Nil,
            &Value::Bulk(Some(b"".to_vec())),
            Proto::Resp3
        ));
    }

    /// The downgrade that produced the bug: re-encoding a top-level RESP3
    /// null as RESP2 has to pick one of the two spellings, so `normalize`
    /// leaves it alone. Nested nulls still downgrade, because RESP2 really
    /// does spell those `$-1`.
    #[test]
    fn a_nested_null_still_downgrades_to_the_null_bulk() {
        let mut buf = Vec::new();
        flint_resp::encode_proto(
            &Value::Array(Some(vec![Value::Null, Value::Bulk(Some(b"v".to_vec()))])),
            Proto::Resp2,
            &mut buf,
        );
        let Ok(Decoded::Complete(down, _)) = decode(&buf) else {
            panic!("array with a null did not round-trip through RESP2");
        };
        assert_eq!(
            down,
            Value::Array(Some(vec![
                Value::Bulk(None),
                Value::Bulk(Some(b"v".to_vec()))
            ]))
        );
    }
}
