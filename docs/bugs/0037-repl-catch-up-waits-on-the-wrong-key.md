# BUG-0037: repl's catch-up probe does not cover what its parity check reads (FIXED)

Status: FIXED 2026-08-20 · Severity: medium — it reports a replication data
loss that did not happen, and only on a loaded box

## Symptom

`repl_drill` under real contention (load 8.5, a sibling project's write-ceiling
test running on the same disk):

    == waiting for replica catch-up
    == parity samples
    FAIL: hash mismatch ('v250' vs '')

Read as written, that says replication delivered the strings and lost a hash —
a silent, type-specific write loss. It is not. The replica had not received the
hash yet, and the drill had already decided it was caught up.

## The defect

The load writes 50000 strings and THEN 500 hashes. The catch-up loop waited on

    GET key:0049999

the last STRING. Replication is ordered, so that key's arrival proves the
stream reached that point and nothing more — the hashes are written after it
and were still in flight. The parity check immediately below reads
`HGET hash:0250 f1`.

So the readiness check covered a prefix of what the assertions read. On an idle
box the remaining 500 hashes land inside the 100 ms sampling interval and it
passed for months. Under contention it does not, and the drill reports the
product losing a write.

The repairs added for BUG-0035 sit between the load and the wait, which makes
the window slightly wider — they write after `key:0049999` too — but they did
not create it. The probe was behind the last write before that change and after
it.

## The fix

Write a sentinel AFTER everything else, and wait on the sentinel:

    fleet_retry_write "$MPORT" SET repl-drill-sentinel ready
    ... wait for repl-drill-sentinel on the replica ...

Ordered delivery means the sentinel's arrival implies every earlier write
landed, which is exactly the property the parity check needs. The wait also now
FAILS explicitly when the sentinel never arrives, instead of falling through
into parity samples that would be reporting timing as parity — the same
"cannot answer" guard as `fleet_load_resp` and the gate's flag check.

## Verification: same condition on both sides

| build | conditions | result |
|---|---|---|
| before | load 8.51, sibling `cold_modify` + `c1_write_ceiling` running | `FAIL: hash mismatch ('v250' vs '')` |
| after | load 5.45, sibling `cold-modify` + `d5_sst_shape` running | PASS, `parity OK (strings + hashes)` |

Both runs recorded their own environment via `fleet_env_note`, which is what
made "the same condition" a claim from the log rather than from memory.

## Why it was invisible

This is the third distinct defect this week whose whole existence depended on
the box being fast enough: BUG-0030 (a positive control calibrated to one
machine), BUG-0036 (a contention guard that never fired), and this. A drill
suite that only ever runs on an idle laptop cannot distinguish "correct" from
"fast enough to look correct", and the failures it eventually produces name the
product rather than the timing.
