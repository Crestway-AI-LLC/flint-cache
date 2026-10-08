# BUG-0223: a pipelined read through the proxy could see a write sent after it (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by `client_compat_drill` (redis-py
pipelines, 200 runs of a read then a write) and `near_cache_cross_client_drill`
(a write and a read in one pipeline through a caching proxy).
**Severity:** high. Redis runs one connection's commands in the order they
were sent, and clients build on it: a pipelined `LRANGE q 0 -1; DEL q` could
read the list empty and drop its items. node-redis pipelines the commands
issued in one tick by default, and redis-py, Jedis and Lettuce pipeline on
request.

## What happened

The proxy stages a run of pipelined commands onto its seat connections and
flushes them together (`prefetch_run`). ADR-0029 gave reads their own
connection, the read lane, so a stalled write cannot hold them up, and kept
read-your-own-writes by sending every command after a run's first write on
the write lane. The reads AHEAD of the first write stayed on the read lane.
The two lanes are two connections, flushed independently, so the write could
run first:

- `GET k; SET k new` answered the GET with `new` 101 times in 300 through a
  standalone proxy (Valkey 9.1.0: never). Measured 2026-10-07.
- A pipelined differential of 32,000 commands against Redis 8.2.8 and Valkey
  9.1.0 diverged on every seed; sent one at a time, the same commands agreed.

The same pass answered a GET from the near-cache (ADR-0031) wherever it stood
in the run. A write invalidates the cache only when its reply comes back, so a
GET behind a write in the same pipeline read the value the write had replaced,
for a tenant with the near-cache on.

## The fix

A run that writes anything goes on the write lane from its first command, in
order, on one connection; only a read-only run takes the read lane. A GET
behind a write in the run goes to its seat, never the cache. Measured after:
0 in 1,000 for each of `GET; SET`, `LRANGE; DEL` and `SET; GET`, and the
pipelined differential agrees on every seed.

What this costs: a pipeline that writes no longer gets the read lane's
isolation from a stalled write. A read-only pipeline, the cache shape ADR-0029
was for, keeps all of it. ADR-0029 carries the correction.
