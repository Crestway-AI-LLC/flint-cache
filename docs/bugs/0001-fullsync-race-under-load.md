# BUG-0001: full-sync loses data under concurrent load (OPEN)

Status: OPEN · Found: 2026-07-13 by flint-chaos `chain` workload · Severity: high (data loss)

## Symptom
The chain-traversal chaos test (build a 200k-element linked list, then walk
it pointer-by-pointer while randomly killing master/replica) fails on some
seeds: a promoted master is permanently missing a key in the middle of the
keyspace (`still nil after 56 retries over 3s`). The walk reaches, e.g.,
key0181291 and finds key0181292 truly absent. Reproducible on some seeds
(13 fails; 7, 21, 33, 41, 42 pass) — a race, not deterministic.

## Not the cause (ruled out)
- Single-hop full sync is complete (docs/... fullsync_check: 0 missing incl.
  the last 3000 keys, cursors match).
- Chained promotion WITHOUT read load and with a strict `lag_ms:0` gate:
  8 rounds, 0 missing. So chained promotion alone is sound.
- The KV chaos workload (small keyspace, random writes) never reproduces it.

## Likely cause
`FLINTFULLSYNC` streams a RocksDB checkpoint's files while the source master
is under concurrent traversal-read load (and therefore background
compaction). The seeded replica ends up with a TRUNCATED keyspace (missing
from some mid-key onward), which then propagates when it is later promoted.
Full-sync file counts vary 5–10 across nodes in a failing run. Suspects, in
order: (1) the checkpoint does not capture unflushed memtable/WAL and the
opening replica misses it; (2) a file is skipped or truncated during the
"buffer whole file in memory then RESP-encode" streaming; (3) `wait_healthy`
(live + lag<400) reports a replica caught up before it has applied the whole
checkpoint.

## Next steps (needs focused session + review)
- Force a memtable flush before `checkpoint_to`, or verify Checkpoint WAL
  inclusion + replica WAL replay.
- Compare exact key counts (not spot checks) source vs seeded replica right
  after each full sync under load.
- Consider a post-full-sync integrity assertion (latest_seq AND a key-count
  or range checksum) before a replica is eligible for promotion.

## Test to reproduce
`./target/release/chain --elements 200000 --kills 12 --seed 13`
(build release with `--features rocks` for flint-server first).
