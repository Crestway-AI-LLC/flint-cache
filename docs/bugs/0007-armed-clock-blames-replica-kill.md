# 0007: the armed kill clock blames a replica kill for a master kill's loss

## Symptom

`tools/chaos_drill.sh` at seed 7 failed intermittently — same seed, same
build, three different outcomes across consecutive runs. The serious one:

    iter 2: REPLICA kill lost acked write at key270: 216 < 239

right after iter 1 killed a master (harness-promoted, RTO 44ms, 14 acked
keys regressed). Replica kills permit zero loss, so this reads as the
engine losing an acked write on the one path with no async-contract excuse.

## The wrong conclusion

That the promotion in iter 1 left a stale seat serving reads, or that the
new master dropped a replicated write — i.e. a replication/promotion bug in
the product. The post-mortem node logs contradict it: the promoted master's
log shows a normal promote and two clean full syncs served, and the
replacement replicas boot `-READONLY` from their first line. No node ever
held seq 239 for key270 except the master that was killed.

## Root cause: two events, one clock

The harness stamps `kill_ms` and then calls `kill_master_hot()`. Before the
SIGKILL actually lands, that call does an epoch-read round-trip to the
survivor and spawns a `pkill` process — tens of milliseconds on a loaded
box. The old master is alive and acking the whole time, at ~40 writes/ms.

The ledger used `sent >= kill_ms` as proof an ack "belongs to the new
master" (the #130 fix, which correctly switched from ack time to send
time — but against the wrong clock). Every ack from the arming gap
therefore survived the post-kill retire. When one of those writes was also
in the dead master's unreplicated tail — which needs replication lag, hence
the intermittency and the RTO 44ms run being the one that tripped — the
ledger kept claiming a seq the survivor never had. The next REPLICA kill
re-judged the key against the true master and reported the master kill's
legitimate async loss as forbidden replica-kill loss.

Two sibling flakes in the same runs, same weather, different mechanisms:
`chain` died unwrapping a pipelined build write when the client's 1500ms
read timeout fired under load (EAGAIN is retryable, and the batch is
idempotent SETs), and bootstrap died at "master up" with an EMPTY node log
— which is not a slow boot but a SIGKILLed one: a second drill on the same
scope and port block reads the first drill's seats as its own and sweeps
them at startup (the 0003 collision through a different door). fleet_init
now takes a per-scope lock so the second run refuses instead.

## The check that now holds it

`kill_master_hot()` returns a second timestamp, `dead_ms`, stamped
immediately after the SIGKILL landed — the earliest instant at which no
write can have been served by the dead master. The armed `kill_ms` still
times RTO/stall from the writer's vantage; the LEDGER — the post-kill
retire, the unverifiable-key retire, and the loss-judging skip — uses only
`dead_ms`. An ack sent in the arming gap is now judged (and retired) as the
old master's, which is what it was.

The rule generalizes: when one logical event ("the kill") is really two
physical events (deciding to kill; the process dying), any accounting that
compares timestamps across it must say which of the two it means. An
interval, not an instant.
