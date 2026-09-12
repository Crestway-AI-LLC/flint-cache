# BUG-0133: a slot cutover hands ownership over before the imported data is durable (FIXED 2026-09-11)

Status: **FIXED 2026-09-11**, found the same day · Severity: **high** — a host
failure of the destination during a cutover loses acked rows from **both**
copies, permanently. Not reachable by a process kill, which is why every drill
passed while the window was open.

**This is NOT the BUG-0132 CI intermittent**, and the fix here does not close
it. See "What this does not explain" below. The two share a code path and
nothing else.

## The defect

`migrate_in` pulls the slot with the ordinary write path — `kv.put(k, v)` per
row, which is `db.write(wb)` with default `WriteOptions`. That is deliberate
and correct for ordinary writes; `flush_wal_sync`'s own doc states the model:

> Ordinary writes go to the WAL unsynced (OS page cache: zero acked loss
> across process crash/restart, proven by the chaos drills); this tick, driven
> by the server's `--wal-fsync-ms` cadence, is what bounds the loss window of
> a HOST failure (power, kernel, instance loss) to the cadence instead of
> "whenever the OS flushed".

**A cutover breaks that bound.** At step 5 the destination clears its
`Importing` record and calls `FLINTSLOTMOVED` on the source, and the source
does not merely disown the slot — it **purges every row of it**:

```rust
let purged = purge_slot_rows(kv.as_ref(), ns, slot);
```

The source's own ordering is sound, and its comment says why: the durable
`Moved` record lands before the deletes, in one WAL, so truncation cannot
reorder them. **But the destination is a different machine with a different
WAL, and nothing orders its unsynced rows against the source's durable
purge.** Lose the destination to power, kernel or instance failure between its
last `put` and its next cadence fsync, and the rows are gone from both copies:
the source deleted them because the destination said it had them, and it had
them only in page cache.

**So correctness depended on HOW the node died.** A process kill preserves the
page cache, so `pkill -9` — which is what every drill uses — can never expose
this. A host failure does not.

## The fix

One fsync, at the one point where ownership transfers, before the call that
makes the source disown and purge:

```rust
if let Err(e) = kv.flush_wal_sync() {
    rollback();
    return Value::Error(format!(
        "ERR cutover refused: the imported slot could not be made durable \
         before handing ownership over ({e}) -- the source still owns it \
         and nothing was purged"
    ));
}
```

A failed flush **refuses the cutover** rather than proceeding: the source keeps
the slot, nothing is purged, and the move can be retried. Cost is one fsync per
slot move, not per row.

### It also repairs a hazard BUG-0132 documented and did not fix

`recover_migrations` completes an interrupted flip when the destination is
merely **`reachable(dest)`** — reachable, not "has the data". BUG-0025 hardened
the neighbouring inference for exactly this reason, recording the cost as *"an
acked write on the source, absent on the dest, gone from both within seconds
with this loop logging success."*

That branch is sound **exactly when a destination that reached step 5 has its
rows on disk**, which is what this fix now guarantees. Before it, recovery
could complete a flip onto a node whose copy had evaporated with the page
cache.

## The guard

`tools/slot_cutover_drill.sh`, which is already the full freeze → drain → flip
protocol, samples `wal_fsync_total` either side of the cutover:

```
wal_fsync_total 0 -> 1 with the cadence off: the flip waited for the data
```

**`--wal-fsync-ms 0` on the destination is what makes the assertion mean
anything.** The 500 ms cadence raises that counter on its own, so without
disabling it a build with no barrier passes. With it off, any fsync is one the
cutover asked for.

Three assertions, because the claim is a counter moving:

1. the counter is readable at all — an unreadable pair fails rather than
   being treated as zero;
2. `FS_BEFORE` is **exactly 0** — if the cadence is still running, the check
   cannot tell a deliberate fsync from a scheduled one and says so;
3. `FS_AFTER >= 1`.

**Mutation-verified**: with the barrier removed the drill reports
`wal_fsync_total 0 -> 0` and fails naming the consequence. Five cutover drills
pass with the fix in place — `slot_cutover`, `slot_cutover_recovery`,
`slot_migrate`, `slot_moved`, and `rebalance_execute`, the last of which drives
real cutovers through the controller and asserts key conservation.

## What this does not explain

**BUG-0132's CI intermittent is untouched.** That failure reproduces under
`pkill -9`, which preserves the page cache, so a missing fsync cannot be its
cause. BUG-0132 remains open, still unreproduced outside the GitHub runner
after 26 EC2 runs, and still waiting on its next CI occurrence.

Recorded because the temptation is real: a durability fix landed in the same
function during the same investigation, CI will likely be green afterwards,
and green will mean nothing — the failure is 2-in-6.

## How it was found

Not by reproduction, which had already failed 26 times. By Jeff's framing:
*"correctness should be orthogonal to underlying hardware."* Taking that as a
property to audit rather than a symptom to chase turns the question from "why
does this runner fail" into "is there an interleaving where the flip completes
without the data" — and that one is answerable by reading, on any machine.
