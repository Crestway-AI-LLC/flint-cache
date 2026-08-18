# BUG-0016: `verify` reports OK on a pair whose replica holds 1% of the data (OPEN)

Status: OPEN, found 2026-08-18 on the playground · Severity: **high** — it is
the check an operator runs to decide whether a roll is safe, and it said yes
to a roll that would have destroyed the dataset

## Symptom

Playground, rc.51, live. Excluding WAL archive, the two members of pair 0 hold:

    node-7002 (master)   887 MB   2 sst
    node-7001 (replica)  8.3 MB   4 sst

107x apart. Every health surface says the pair is protected:

    $ flintctl -f /opt/flint/cluster.flint status
    pair 0  172.31.64.94:7002  master   seq_lag 2     live_replicas 1
    pair 0  172.31.64.94:7001  replica  seq_lag none  live_replicas 0

    $ flintctl -f /opt/flint/cluster.flint verify
      ok   pair 0 fully staffed  2 member(s) up
      ok   pair 0 replicating  1 replica(s) streaming from 172.31.64.94:7002
    VERIFY OK: 1 pair(s), 1 proxy(ies) — all views agree

## Why it matters right now

`verify` is the gate before a roll, and a roll of the master hands service to
the replica. Acting on this OK would have failed `try.crestwayai.com` over onto
8.3 MB of an 887 MB dataset. The command whose entire job is to answer "is this
pair really protected" answered yes.

## Root cause

`verify` establishes that a replica is **streaming**. It never establishes that
the replica **holds anything**. Those come apart exactly when the cursor is
wrong, and a wrong cursor is a real failure mode — see BUG-0015, where the
FLINTSYNC handshake admitted a cursor the WAL could not serve, and BUG-0012,
where retention pruned the span under a live replica. A replica that rejoined
at a cursor it should have been refused reports a near-current `seq_lag` with
none of the data behind it. That is this pair's signature: `seq_lag 2` against
1% of the bytes.

**So no cursor-derived check can catch this**, because the cursor is the thing
that is lying. `seq_lag`, `last_applied` and "is it streaming" are all
downstream of the same number.

## This is BUG-0002 recurring, one step subtler

`0002-verify-ok-on-single-copy.md` was "verify reported OK on a single-copy
pair" — a member that was ABSENT. The fix taught verify to count members. This
is the same lie with a member that is PRESENT, streaming, and empty. Counting
seats was never the property; holding the data was.

## The fix has to use an independent measure

Something that reads the bytes, not the bookkeeping:

- `FLINTNSBYTES` already exists (M5 storage metering) and is per-namespace. Ask
  both members and fail the pair on gross divergence. 887 MB vs 8.3 MB is not a
  threshold question.
- Not `DBSIZE`: it is O(keys) per node and already times out the fan-out at
  scale (#178/#179), so it cannot be the routine check.
- A tolerance band, not equality — a healthy replica legitimately trails, and
  compaction state differs between a master retaining WAL archive and a replica
  that does not. That difference is what made the raw `du` reading ambiguous
  here (910 MB vs 65 MB INCLUDING archive, which nearly explained itself away);
  the honest comparison excludes `archive/`.

## Where to start

1. `verify` gains a per-pair data-divergence check over `FLINTNSBYTES`,
   defaulting to fail beyond some multiple, with the numbers printed either way.
2. A drill: build a pair, hollow the replica's data dir while leaving its cursor
   current, and assert `verify` FAILS. Without that arm the fix is untested in
   the direction that matters, and the positive control (a healthy trailing
   replica must still pass) belongs with it.

## Related

- `0002-verify-ok-on-single-copy.md` — the same claim, absent member
- `0015-flintsync-probe-admits-a-cursor-the-wal-cannot-serve.md` — a mechanism
  that produces exactly this state
- `0012-walgap-livelock-retention-ignores-replicas.md` — the other one
- #178/#179 — why `DBSIZE` cannot be the measure
