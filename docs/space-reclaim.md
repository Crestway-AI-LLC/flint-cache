# Bring your own cleanup policy

Flint never evicts. An acked write stays until you delete it or its TTL
passes — under disk pressure the node sheds *new* writes early and keeps
serving reads and deletes, but it will not silently pick a victim for
you. (README, "What happens when the disk fills", explains why: eviction
breaks the durability an acked write promises, and LRU bookkeeping turns
every read into a write at exactly the moment the disk can least afford
it.)

That is a contract, not a limitation, **if** you can close the loop
yourself. This page is the loop: everything an external cleanup daemon —
a cron job, a sidecar, twenty lines of Python — needs to watch pressure,
decide what goes, delete it, and verify the space came back. Every
primitive here is a plain command over the same connection your
application already uses; nothing needs operator access.

The prerequisite that costs nothing now and everything later: **set TTLs
on data you already know is disposable.** TTL expiry is the built-in
cleanup policy, it needs no daemon, and the GC sweeper returns the space
on its own. A cleanup daemon is for the data whose lifetime you *couldn't*
declare up front.

## 1. Watch: know when you're under pressure

Poll `FLINTINFO` (any client, any time):

| field | meaning |
|---|---|
| `disk_free_bytes` / `disk_total_bytes` / `disk_free_pct` | the host volume, as the guard sees it |
| `disk_verdict` | `ok` or `shed` — `shed` means ordinary writes are being refused **right now** |
| `gc_swept_expired` / `gc_swept_orphans` | lifetime reclaim counters; climbing means expiry is returning space |

The same numbers are Prometheus metrics via the exporter, which is where
an alert belongs: page on `disk_free_pct` approaching the guard's
threshold (default 10% / 2 GiB), not on the verdict flip — by the flip,
clients are already seeing errors.

For edge-triggered tooling, the guard's flips are fleet journal events:
`DiskShed` and `DiskResumed`, each carrying `free <bytes> of <bytes>`.
Trigger your daemon on `DiskShed` instead of tight-polling if you run the
fleet journal.

Per-tenant attribution: `FLINTNSBYTES` reports stored bytes per
namespace, so a multi-tenant operator can decide *whose* data to trim
before deciding which keys.

## 2. Rank: decide what goes first

Two O(1) reads exist for exactly this (both also work on replicas):

- **`FLINTKEYSIZE key`** → the stored payload size in bytes. For a hash,
  set, zset or list this is the cumulative member bytes the server
  already tracks; for a string or JSON document, the payload length.
  Nil if the key is missing or expired.

- **`FLINTKEYSTAMP key`** → `[written_ms, created_ms]`, unix
  milliseconds. `written_ms` moves on every data mutation and
  deliberately **not** on `EXPIRE`/`PERSIST` — it stamps writes, not
  touches. `created_ms` is the current incarnation's creation instant for
  collections, `0` for strings and JSON. A `0` anywhere means "not
  tracked", never a guess; keys last written by a pre-stamp binary report
  `written_ms` as 0 until their next write, which for a
  least-recently-written policy is exactly the right bias — unknown means
  old.

Enumerate candidates with `SCAN` (cursor-stable across failover) and its
collection-level siblings. `TTL` tells you what will free itself soon
anyway — deleting a key that expires in a minute is wasted work.

What Flint does **not** track, on purpose: read recency. There is no
`OBJECT IDLETIME`. Least-recently-**written** plus size is what the
server maintains honestly, and it covers cache-shaped workloads without
making reads pay for bookkeeping.

Three policies these primitives support directly:

- **LRW**: scan, sort by `written_ms` ascending, delete from the front.
- **Size-weighted**: delete the largest keys not written recently —
  `size / (now - written_ms)` as a score gets both in one number.
- **Namespace quota**: `FLINTNSBYTES` per tenant; apply either policy
  only inside the namespace that's over its budget.

## 3. Act: delete under pressure

The load-bearing guarantee: **while the node is shedding writes,
`DEL`, `UNLINK`, `EXPIRE` and `FLUSHALL` keep working.** Reclaiming space
is never blocked by the very condition that requires it — that's
drill-proven, not aspirational (`tools/disk_pressure_drill.sh`).

Prefer setting a short TTL (`PEXPIRE key 1`) over `DEL` if you want an
undo window; prefer `UNLINK` semantics for very large collections. Batch
with pipelining, and pace yourself — the point is headroom, not a delete
storm competing with compaction.

## 4. Verify: trust, then check

Deleted bytes return through compaction and the GC sweep, not instantly:

- `disk_free_bytes` rising is the ground truth.
- `gc_swept_expired` / `gc_swept_orphans` climbing confirms expired
  collections are being reclaimed (cadence: `gc-sweep-ms`, default 10
  minutes, hot-settable via `FLINTCONFIG`).
- `disk_verdict` flipping back to `ok` (a `DiskResumed` journal event)
  is the exit condition — the guard reopens by itself with hysteresis,
  no operator action.

A policy daemon that stops at "I issued the deletes" is half a daemon.
Watch the free-space number you started from; if it isn't moving,
you're deleting the wrong thing (or something else is filling the disk
faster — the guard fires on the host volume, whoever's bytes they are).

## Sketch

```python
import redis, time

r = redis.Redis(host=..., port=..., password=..., protocol=3)

def free_pct():
    info = dict(line.split(":", 1) for line in
                r.execute_command("FLINTINFO").decode().splitlines() if ":" in line)
    return float(info["disk_free_pct"])

TARGET = 20.0          # start reclaiming well before the ~10% guard
while free_pct() < TARGET:
    batch = []
    cursor = 0
    while len(batch) < 1000:
        cursor, keys = r.scan(cursor, count=500)
        for k in keys:
            stamp = r.execute_command("FLINTKEYSTAMP", k)
            size = r.execute_command("FLINTKEYSIZE", k)
            if stamp and size:
                batch.append((stamp[0], -int(size), k))   # oldest, then biggest
        if cursor == 0:
            break
    for _, _, k in sorted(batch)[:200]:
        r.delete(k)
    time.sleep(5)      # let compaction breathe; re-measure, don't assume
```

Adapt freely — the sketch is the shape (watch → rank → delete a bounded
batch → re-measure), not a product.
