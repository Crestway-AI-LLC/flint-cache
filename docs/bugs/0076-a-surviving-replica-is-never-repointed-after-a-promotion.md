# BUG-0076 — a surviving replica is never re-pointed, so a three-member pair loses a copy at every failover (OPEN)

**Found** 2026-08-29 by running the three-member chaos test — the validation
gap named at the end of BUG-0075, and the reason that recommendation was held
back. Six hosts, one pair of three members, `min-replicas-to-write=1`, kills
through `flintctl`, promotion by the fleet's own controller.

The chaos oracle passed: 16 kills, 901,275 writes, zero corruption, zero
time-travel, zero cross-key, zero acked loss on replica kills. Nothing was
corrupted. The pair was simply never three members again after the first
master kill.

## What happens

On a master kill the controller promotes one survivor. **The other survivor is
never told.** It goes on dialling the dead address, forever:

```
seat| replicating from 172.31.70.181:7001 starting at seq 1795351 (epoch (0,2))
seat| replication link lost (Connection refused (os error 111)); reconnecting in 1s
   ... unchanged for the rest of the run
```

`flintctl verify` catches it, and says it better than this write-up can:

```
before   ok   pair 0 replicating  2 replica(s) streaming from 172.31.70.181:7001
after    FAIL pair 0 replicating  SINGLE-COPY: every member up, but 1 of 2
              streaming from 172.31.67.167:7003 — one disk holds the only copy
```

Every member is UP. `status` shows three seats, right roles, right epochs. Only
the copy count gives it away.

## Why two members never showed this

There is no runtime re-point. No `FLINT*` verb changes a running replica's
master, and `--replica-of` is read once at startup (`flint-server`,
`let replica_of = arg("--replica-of")`). A replica learns its master exactly
once, from the command line.

On a two-member pair that is invisible, because the only other member is the
one `flintctl restart-node` brings back — and it is started with
`--replica-of <current master>`, computed at restart time. The re-point is a
side effect of the restart, and with two members every survivor gets restarted.
Add a third and the member that was neither killed nor promoted is the one
nobody restarts, so nobody re-points it.

The controller's own comment states the assumption without naming it: after
promoting it sets `converged_ever = false` because the "new master has no
replica yet" and "will not be able to demonstrate that until a replica
re-attaches". Re-attaching is left to the restart path. With three members,
one replica has no restart coming.

## Why this defeats the point of the third member

The third member was proposed so `min-replicas-to-write=1` survives a failover:
BUG-0074's arithmetic says live replicas after a failover are `members - 2`, so
three members leave one and the write path stays open. That arithmetic assumes
the survivor keeps streaming. It does not.

At the instant of promotion the pair holds a master and one STRANDED replica,
so live replicas is **0**, not 1 — and the write gate shuts exactly as it does
on a two-member pair. The run shed 1,021 writes `-THROTTLED` doing so. Writes
resumed only when the killed seat was restarted and re-attached, which is the
same recovery a two-member pair has. **The third member bought nothing.**

It is also worse than a wash afterwards: the pair runs single-copy on a
topology chosen for redundancy, and keeps doing so until an operator notices
`verify` and restarts the stranded seat by hand.

For the same reason a pair that took `add-replica` for D7 read fan-out silently
loses that extra read copy at its first failover.

## Evidence it is the topology, not the run

- Two-member fleets do this correctly and always have — soak9 rewound and
  re-attached on all 5 cycles the same day.
- The stranded seat is identifiable as the one neither killed nor promoted.
- It never recovered across the remainder of the run (many minutes, dozens of
  retries), so this is not a slow reconnect.
- Only 1 of 16 kills could be a master kill precisely BECAUSE of the bug: with
  a permanently stranded member the harness's health gate can never see both
  replicas live, so it correctly declined every later master kill and took a
  replica instead. The thin master-kill count is a symptom, not a sampling
  problem.

## Fix options

1. **The controller re-points survivors after it promotes.** The real fix, and
   it needs a runtime re-point on the seat, which does not exist yet. The new
   verb is the easy half; the hard half is that a re-pointed replica is a copy
   from the superseded lineage and must go through the same fence exchange a
   rejoining ex-master does (`FLINTFENCE`, BUG-0075), or it will attach across
   a branch point and diverge. This should not be built as a bare "set your
   master" command.
2. **`flintctl` re-points them**, as part of the same operator path that
   restarts the dead seat. Smaller, and it matches where the re-point already
   lives — but it only helps when a human or a script is driving, and a
   failover is exactly when nobody is.
3. **The seat consults the control plane** for its pair's current master after
   repeated connection failures. Removes the dependence on anyone noticing, at
   the cost of making seats CP-aware, which they deliberately are not.
4. **Refuse pairs larger than two** until one of the above lands. Honest, and
   it composes with BUG-0075's option 3 — but it withdraws `add-replica`, which
   ships today.

Option 1 is the one that makes three members mean what BUG-0074's arithmetic
already claims. Until it lands, **three members must not be recommended for
`min-replicas-to-write=1`** — not because it is unsafe, but because it does not
deliver the availability it is being chosen for.

## Not covered

- Whether a re-pointed replica can rejoin incrementally or must always
  re-seed. It holds a copy of the superseded lineage, so it is the same
  question BUG-0075 answered for ex-masters, and the same fence must decide it.
- Pairs larger than three. Nothing here is specific to the third member; an
  Nth member has the same problem N-2 times over.
- Whether the controller should re-point on every promotion or only when it
  observes a member streaming from a dead address. The second is narrower and
  self-healing; the first is simpler to reason about.
