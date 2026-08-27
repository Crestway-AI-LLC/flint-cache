# BUG-0060: every resource limit is per-unit, and nothing bounds the aggregate (OPEN)

Status: OPEN, opened 2026-08-27 as an audit · Severity: unknown until the audit
runs, which is the point.

## The question this exists to answer

What system boundaries does Flint actually have, and where is a limit missing
that would let a node crash rather than refuse? Refusing is a behaviour this
product already has everywhere else — `-QUOTA` on a full disk, `-THROTTLED`
below write quorum, a closed connection on a hostile query buffer. A node that
OOMs instead has skipped all of that machinery.

## What EXISTS today, so the audit starts from an inventory rather than zero

Roughly two dozen named limits across the workspace. The protocol and
connection tier:

| limit | value | scope |
|---|---|---|
| `MAX_CONNS` | 2048 (runtime-adjustable) | per node |
| `MAX_QUERY_BUF` | 1 GiB | **per connection** |
| `OUT_FLUSH_THRESHOLD` | 1 MiB | **per connection** |
| `MAX_INLINE_LEN` | 64 KiB | per command |
| `MAX_BULK_LEN` | 512 MiB | per bulk string |
| `MAX_ARRAY_LEN` | 1 Mi elements | per array |

Plus, elsewhere: `MAX_VALUE_BYTES`, `MAX_KEY_BYTES`, `MAX_FULLSYNC`,
`MAX_MARKS`, `MIN_MARKS_TO_FORCE`, `MIN_FORCED_PASS_INTERVAL_MS`,
`MAX_LOG_FILE_SIZE`, `MAX_PREFETCH`, `MAX_DEPTH`, `MAX_SAMPLES`, per-tenant
quotas, and the disk guard's two thresholds.

This is not a system with no limits. It is a system whose limits were each
added where a specific thing went wrong.

## The gap the inventory makes visible

**Read the scope column.** `MAX_QUERY_BUF` is per connection and `MAX_CONNS`
is 2048, so the aggregate input buffer this node will accept is
2048 x 1 GiB = **2 TiB**, on a seat with 16 GiB of RAM. Nothing anywhere
bounds the sum.

The per-connection comment is correct on its own terms — "a legitimate
connection can never accumulate this much; hitting it means a hostile or broken
client, and the connection is closed". Each limit is defensible. **The product
of two defensible limits is not defended by either of them.**

`OUT_FLUSH_THRESHOLD` is the more likely one to bite: 1 MiB of reply buffer per
connection is entirely ordinary, and 2048 ordinary connections is 2 GiB of
reply buffers on a 16 GiB node, with no single client doing anything wrong.

That is the shape to look for everywhere else, and it is why this is an audit
rather than a fix: the defect is not in any one limit.

## How to run the audit

For each of the request path, the replication path, the admin/introspection
commands, and the background tasks, ask two questions:

1. **What grows here?** With dataset size, with client count, with key count,
   with pipeline depth, with the number of namespaces.
2. **What bounds it, and is that bound per-unit or aggregate?** A per-unit
   bound multiplied by an unbounded count is not a bound.

Known-good pattern to copy: `RocksKv::clear` (FLUSHALL) already chunks its
deletes at 10,000 rather than collecting the keyspace, with a comment naming
it "FLUSHALL's version of the DBSIZE OOM — two dataset-sized allocations". So
this class has bitten before and been fixed once, locally.

Known-good pattern from 2026-08-26: `MAX_MARKS` bounds the eviction mark set at
100k precisely because marks are full keys in memory and a node's keyspace does
not fit. That one was bounded before it shipped, because the question was asked.

## Candidates worth checking first

- `scan_prefix` returns a fully materialised `Vec<(Vec<u8>, Vec<u8>)>`. Where
  is it reachable from a user command against a user-sized prefix?
- Pipeline depth: `MAX_ARRAY_LEN` bounds one array; what bounds the number of
  commands in flight on one connection, and their accumulated replies?
- The async write queue's depth against value size.
- Per-namespace state that scales with the NUMBER of namespaces rather than
  with their contents.

## What a fix looks like

Not more per-unit limits. Either a node-level budget that the per-unit limits
draw against, or a per-unit limit derived from the node's memory and the
connection count rather than chosen as a constant. The disk guard is the
existing model for this: it reads the actual device and refuses early, rather
than trusting that each writer is individually reasonable.

## Audit pass 1, 2026-08-27 — candidate 1 resolved: the collection commands

`scan_prefix` returns a fully materialised `Vec<(Vec<u8>, Vec<u8>)>`, and the
question was where a USER command reaches it against a USER-sized prefix.
Answer: three of them, all ordinary tenant verbs, none capped.

| command | path | what it materialises |
|---|---|---|
| `HGETALL` | `hashes.rs:228` | every (field, value) pair in the hash |
| `SMEMBERS` | `sets.rs:172` | every member of the set |
| `ZRANGE` (and friends) | `zsets.rs:301`, `:602` via `all_ordered` | every (member, score) in the zset |

Each does it **twice**: `scan_prefix` builds one `Vec`, `.map(...).collect()`
builds a second, both live simultaneously. Then the reply is serialised on top.
Peak is roughly 2x the collection plus the encoded reply, per in-flight
command, with `MAX_CONNS` 2048 of them permitted concurrently.

**Nothing caps collection cardinality.** Grepped for `MAX_MEMBERS`,
`MAX_FIELDS`, `MAX_CARDINALITY` and the lowercase forms across the workspace:
no such limit exists. A tenant may build a hash of arbitrary size with ordinary
`HSET` calls and then ask for all of it.

### The comment that is the bug in miniature

`zsets.rs:300`, immediately above the call:

    /// zset lives in ONE key, so this is bounded by the zset's cardinality.

That is true, and it is exactly the reasoning this audit exists to reject.
"Bounded by cardinality" is not a bound when cardinality is what the user
chooses. It is the per-unit sentence from the inventory table written as
reassurance — the same shape as `MAX_QUERY_BUF`'s "a legitimate connection can
never accumulate this much", which is also true per connection and false at
2048 of them.

### Severity, stated honestly

Not yet measured, and the audit should not guess. What is established is
reachability and the absence of a bound; what is NOT established is the size a
real tenant reaches, or whether the RESP encoder's own buffering fails first
and more gracefully. A 10M-member set at ~20 bytes per member is ~200 MB
materialised twice on a 16 GiB seat — survivable once, not at concurrency.

### Why this is not "add MAX_MEMBERS"

Per the fix section above: another per-unit constant is the same mistake one
layer down. The two shapes worth considering, neither chosen here:

- **Stream the reply** rather than materialising — the collection commands are
  the natural cursor candidates, and `SSCAN`/`HSCAN`/`ZSCAN` already exist as
  the cursor-shaped answer. The gap may be that nothing REFUSES the
  materialising form when it would be large, not that the cursor form is
  missing.
- **A node-level budget** the per-unit limits draw against, as the disk guard
  does with the real device.

Refusing is the behaviour this product already has everywhere else. A
`-TOOLARGE` on `HGETALL` against a collection past a derived threshold, naming
`HSCAN`, would match `-QUOTA` and `-THROTTLED` exactly.

**Remaining candidates unaudited:** pipeline depth and accumulated replies,
the async write queue against value size, per-namespace state scaling with
namespace count.
