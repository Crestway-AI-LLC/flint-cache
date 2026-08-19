# BUG-0031: `NEEDS_RESEED` is cleared by the very start that should honour it, so a marked replica loops forever (OPEN)

Status: OPEN, found 2026-08-19 on the playground · Severity: **high** — a
replica that needs a full sync can never get one. It restarts every two
minutes until a human wipes its data directory by hand, and the pair stays
single-copy the whole time.

## Symptom

`/var/lib/flint/logs/node-7002.log`, repeating verbatim every restart:

    cleared NEEDS_RESEED: this node is the lineage now
    marked copy verified against the lineage held by 172.31.64.94:7001:
      warm rejoin at seq 100817895 (epoch (0,39))
    marked rejoin continues; tailing from seq 100817895
    replicating from 172.31.64.94:7001 starting at seq 100817895 (epoch (0,39))
    FATAL: master's oldest batch starts at 100817900, past the 100817896 we
      still need — this link can never resume. Marking for re-seed and
      exiting; the next start will full-sync from a checkpoint.

Then the next start prints the first line again.

## The loop, and why it cannot break itself

1. Something marks the node: `flintctl host-mark-reseed` writes `NEEDS_RESEED`.
2. The node starts, **clears the marker**, and decides to warm-rejoin —
   before it has established that a warm rejoin is possible.
3. The warm rejoin needs seq 100817896. The master's oldest retained batch
   is 100817900. Four batches short, and no amount of waiting fixes it: the
   WAL only moves forward.
4. The node correctly diagnoses this, says *the next start will full-sync
   from a checkpoint*, re-writes `NEEDS_RESEED`, and exits.
5. The next start clears the marker. Go to 2.

The last line of the FATAL is a promise the next start breaks. The node is
right about what it needs and never does it.

## Root cause

The clear is unconditional and happens too early. `NEEDS_RESEED` is a
REQUEST for a full sync; clearing it on start turns it into a record that a
request was once made. The only state that should clear it is a completed
full sync.

Note the wording of the line that does the damage — *"this node is the
lineage now"*. That is a conclusion about data, drawn before the data was
checked, at the point where the marker was the only evidence available.

## What it looked like from outside

Pair 0 single-copy for 13 minutes. `flintctl status` showed `7002 DOWN`
while `flint-supervise` restarted it every two minutes; nothing said "this
node has asked for a re-seed 6 times". Recovery was manual:
`host-stop-seat`, `host-wipe-node`, `restart-node` — after which the full
sync completed in seconds and `verify` came back clean, which is the proof
that a full sync was both possible and sufficient the whole time.

## Fix

Clear `NEEDS_RESEED` only after a full sync completes. On start, a present
marker must force the checkpoint path and must not be consulted as evidence
about lineage.

Worth checking at the same time: whether the warm-rejoin decision can be
made against the master's retained WAL floor BEFORE committing to it. The
node already learns that floor one step later; asking first turns a fatal
into a branch.

## Verification the fix needs

- a replica stopped long enough for the master's WAL to advance past its
  position restarts and full-syncs WITHOUT intervention — the positive
  control is that it took a wipe by hand to recover on 2026-08-19
- `NEEDS_RESEED` survives a start that does not complete a full sync
- a marked node that is killed mid-full-sync still full-syncs on the next
  start rather than warm-rejoining

## Related

- [BUG-0032](0032-flintctl-start-asserts-pair-0-is-the-master.md) — why one
  dead replica became a crash loop instead of one dead replica
