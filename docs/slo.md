# Service levels — what Flint guarantees, and what it does not

Every number here was measured by a harness in this repository, with the
command to reproduce it. Where a claim is weaker than you might expect from
the word "durable", that is deliberate: it is better to publish the contract
the code actually signs than one it does not.

## The short version

Flint is a **cache with real durability**, not a system of record. It keeps
your working set on disk so it survives restarts and fails over in seconds,
and it will lose a bounded amount of recently-acknowledged data when a master
dies. If your data exists **only** in Flint and losing the last second of it
would be a problem, you want a database as well.

## What is guaranteed

| Event | Data outcome |
|---|---|
| Process restart (warm) | **Nothing lost.** Restart is a binary swap on the same data dir; `restart_drill.sh` asserts it. |
| Replica dies | **Nothing lost.** The master is untouched. The chaos oracle allows *zero* acked-write loss on a replica kill and fails the run otherwise. |
| Master dies, replica caught up | **Nothing lost.** |
| Master dies, replica behind | Acked writes in the un-replicated tail may be lost. Bounded — see below. |
| Both members of a pair lost | Everything on that pair, back to the last backup. |

## Failover time (RTO)

**Budget: 10 s. Measured: p50 506 ms, worst 586 ms**, over 7 promotions in a
12-kill run across 5 hosts on a real network (`v0.1.0-rc.28`). Single-host
runs measure lower and are not the number to quote; crossing a network is the
honest case.

RTO here means what a *client* experiences — from the kill to the first write
the client gets acknowledged — not the time for an internal probe to notice.
Through the proxy edge a client typically sees no error at all, only one slow
write, because the proxy chases the promotion and retries underneath.

```sh
packaging/aws/chaos-cluster/run.sh --hosts 5 --tag <tag>   # fleet repo, real hosts
tools/controller_drill.sh                                  # locally, budget asserted
```

## Data loss on failover (RPO)

**The bound is on VOLUME, not on age**, and the distinction matters enough
that we corrected the wording rather than keep the friendlier version.

> **At most one lag-cap window's worth of acked writes is ever at risk**,
> because past the cap (`--lag-hard-ms`, default 1000) the master stops
> accepting new writes and sheds them with `-THROTTLED`.

What we do **not** promise is that a lost write will be younger than the cap.
Once a write is acknowledged, no mechanism can retroactively protect it: if
replication stalls immediately afterwards, that write's age grows for as long
as the stall lasts. Measured, with the replica frozen for 1800 ms:

| stall | lag cap | writes shed `-THROTTLED` | deepest acked-write loss |
|---|---|---|---|
| none | 1000 ms | 0 | **0 ms** |
| 1800 ms | 1000 ms | 75 | 1757 ms |
| 1800 ms | 200 ms | 140 | 1753 ms |

Both halves are visible there. Tightening the cap fivefold nearly doubled the
shedding — the valve really does close harder — while the age of the oldest
lost write did not move, because it is set by the stall and not by the cap.

In healthy operation the loss is **0 ms**: both multi-host runs of rc.28
reported zero acked-write regression across 12 kills, because replication kept
up. The numbers above are what a deliberately stalled replica produces.

```sh
flint-chaos --iterations 6 --keys 300 --seed 5 --mode mixed \
  --stall-replica-ms 1800 --lag-hard-ms 200
```

`--stall-replica-ms` uses `SIGSTOP` on the replica process, so it works only
against a local cluster.

## Host loss and the fsync window

Replication is asynchronous and the WAL is fsynced on a bounded cadence
(`--wal-fsync-ms`, default 500). If the machine itself dies — power, kernel,
instance termination — up to that window of acked writes may not have reached
stable storage on that node. A live replica normally has them; a
simultaneous loss of both does not.

On AWS `i4i` and similar instance-store families this is sharper than it
sounds: **the NVMe instance store is ephemeral.** Stopping, terminating, or
retiring the host destroys that node's copy outright, so "lost a node" always
means "lost its data" and repair is always a full resync.

## What none of this covers

- **Both pair members in one failure domain.** Nothing today stops both from
  landing in the same availability zone or rack; the inventory requires
  separate *hosts*. Place them deliberately.
- **Backup.** Recovery from a whole-pair loss is a restore, and restore is not
  yet implemented — see [ADR-0011](adr/0011-backup-and-restore.md).
- **Partitions, and a single host filling its disk**, are untested by the
  chaos suite. Process kills across hosts is the fault class that is covered.

## How to check any of this yourself

```sh
tools/gates.sh          # fmt, clippy, tests, conformance, 27 drills, chaos
```

Every claim above is an assertion in a drill that exits non-zero when it
fails. [failover.md](failover.md) explains the mechanisms; this page is only
the numbers and their conditions.
