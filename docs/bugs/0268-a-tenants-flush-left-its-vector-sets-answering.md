# BUG-0268: a tenant's FLUSHALL left its vector sets answering, and a write after it was lost at the next restart (FIXED 2026-10-19)

**Status:** **FIXED 2026-10-19.** After a flush, the proxy tells every
co-processor to forget the namespace, as `flintctl` tells them on a tenant's
removal (BUG-0255). Found by reading the flush path while building ADR-0060.
Held by `coproc_vec_drill`'s BUG-0268 section.
**Severity:** high where it applies. Writes were acknowledged and lost, and
reads were answered from data the tenant had deleted. It applies to a fleet
that declares a co-processor (`coproc VEC.`) whose tenant flushes its
namespace with `FLUSHALL` or `FLUSHDB`, then goes on using a vector set it
made before the flush.
**Area:** `crates/flint-proxy/src/main.rs`, the `FLUSHALL | FLUSHDB` arm of
`handle`.

## What happened

The co-processor keeps a set's index in its memory, and its durable rows in
the tenant's namespace under `\x00vec\x00`:
- the set-name index;
- each set's config;
- each set's id buckets;
- each vector's row.

A tenant's `FLUSHALL` through the proxy deletes every key of its namespace,
these rows included. The proxy told nothing else.

On public main's binaries (418f8dc9), one pair, one co-processor:

    VEC.CREATE fl DIM 3 METRIC l2           -> OK
    VEC.SET fl a 1,0,0                      -> OK
    DBSIZE                                  -> 4
    FLUSHALL                                -> OK
    DBSIZE                                  -> 0
    VEC.SEARCH fl 1,0,0 1                   -> a -0       (rows gone; answered from memory)
    VEC.SET fl b 0,1,0                      -> OK         (acknowledged)
    VEC.SEARCH fl 0,1,0 1                   -> b -0
    (co-processor restarted)
    VEC.SEARCH fl 0,1,0 1                   -> ERR no such set
    DBSIZE                                  -> 2          (b's row and its bucket, orphaned)

After the flush, the set went on answering from vectors the tenant had
deleted. The write to it after the flush was acknowledged and durable, but
its set's config and the set-name index were gone. The next rebuild could
not find the set, and the vector was lost, its rows left behind.

## Why

BUG-0255 made the co-processor forget a namespace (`FLINTNSDROP`) when an
operator removes its tenant, which flushes the namespace on every pair. A
tenant's own flush through the proxy is the same deletion, and the fix did
not reach it.

## Fix

- **After a flush succeeds on every pair, the proxy sends `FLINTNSDROP
  <ns>` to every endpoint of every co-processor family.** Each co-processor
  forgets the namespace, sets and all, and rebuilds it at its next touch from
  the rows that are left, which after a flush are none. Every endpoint is
  told, since any of them may have served the namespace, so every flush
  costs each co-processor one `FLINTNSDROP`, whether it held the namespace
  or not.
- **A co-processor that cannot be told turns the reply into an error.** The
  flush has happened, and the error says so and asks for the flush again:
  `ERR the keys are flushed, but the co-processor at <addr> could not be
  told (...), so its vector sets answer from memory until it is: send the
  flush again (BUG-0268)`. A retry is idempotent. While a co-processor is
  down, every flush through the proxy answers that error, for tenants with
  vectors or without. The flush itself still happens.
- **A `VEC.SET` concurrent with the flush may be lost.** That reads as the
  write coming before the flush, which deleted it.

## Evidence

- The transcript above, on public main's binaries.
- `coproc_vec_drill`, BUG-0268 section, with the fix:
  - after `FLUSHALL`, `VEC.SEARCH` and `VEC.SET` on the flushed set answer
    `ERR no such set`;
  - the set made again and written after the flush answers the same after a
    co-processor restart.

  The rest of the drill, every earlier section, still passes.
