# BUG-0224: a pipelined LPOP or RPOP with a count answered a null bulk (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by `proxy_conformance_drill`, which now
sends a pipeline as well as the corpus's one command at a time.
**Severity:** low. Most client libraries read both nulls as nil.

## What happened

The proxy reads its seats in RESP3, which has one null. For the commands whose
null is an array in RESP2 (`LPOP` and `RPOP` with a count, the blocking pops,
`ZRANK ... WITHSCORE`) it rebuilds `*-1` for a RESP2 client (BUG-0215), but
only on the path that answers one command at a time. A pipelined command is
answered by the staging path, which skipped it, so `LPOP missing 2` in a
pipeline answered `$-1` where Redis answers `*-1`. Found by the pipelined
differential that found BUG-0223.

## The fix

Both paths retype through one function.
