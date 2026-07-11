# ADR-0002: Encoding as a swappable abstraction; the envelope is not

Status: accepted · Date: 2026-07-10

## Context
Read-latency optimizations we anticipate (small-collection inlining,
key-value separation, alternative zset layouts) change access patterns, not
just byte layouts. Encodings are persistence formats: swapping one means old
bytes must remain readable forever.

## Decision
1. Abstraction sits at the semantic-operation level (TypeStore per family:
   StringStore, later HashStore…), over a flat ordered `Kv` trait. Each
   TypeStore owns row layout AND access strategy.
2. The key envelope `cf-tag | ns_len | ns | slot(BE) | user_key` is a system
   invariant owned by encoding.rs, outside any variant: migration prefix
   scans, range deletes, and per-slot accounting depend on it.
3. Every metadata row carries type (low 4 bits) + encoding variant (bits
   4–5) in its flags byte: mixed variants coexist per key; writers choose by
   policy; readers dispatch on the tag. No flag-day migrations. Wholesale
   upgrades ride compaction rewrites.
4. Expiry is an absolute unix-ms `expire_at` in the row — deterministic
   under replication (expire-at replicates; wall clocks don't) — checked
   lazily on read, garbage-collected by compaction filters later.
5. Clocks are injected (fn() -> u64) so TTL semantics are testable without
   sleeping and the replicated apply path stays deterministic.

## Consequences
New encodings inherit the conformance oracle and can be differential-tested
against v1. The v0 single-keyspace Kv maps CF tags to real RocksDB column
families later without changes above the seam.

## Alternatives considered
Byte-codec-level abstraction (cannot express inlining); dyn-dispatch
per-encoding objects (enum/tag dispatch is exhaustive and free); relative
TTLs (non-deterministic under replication).
