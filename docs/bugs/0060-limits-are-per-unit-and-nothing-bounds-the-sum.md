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
