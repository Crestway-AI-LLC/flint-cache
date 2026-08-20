# Service levels — what Flint guarantees, and what it does not

Every number here was measured by a harness in this repository, with the
command to reproduce it. Where a claim is weaker than you might expect from
the word "durable", that is deliberate: it is better to publish the contract
the code actually signs than one it does not.

## The short version

Flint is a **persistent cache**, not a system of record. It keeps
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

## The recovery window — a SEPARATE promise from RTO

Failover ends when writes are served again. **Recovery** — getting the pair
back to two copies — starts there and lasts much longer, and for a while the
surviving master is doing three jobs at once:

1. serving ordinary traffic,
2. absorbing the **retry burst** that piled up during the blackout, because
   every client that queued or errored replays the moment the endpoint opens,
3. streaming a full checkpoint to the replacement replica.

So demand goes UP exactly as capacity goes DOWN — one node instead of two.
Conflating this with RTO hides it: measured through the proxy edge, the
reported figure is the worst gap between consecutive acknowledged writes
anywhere in the window, so a slow recovery is easily mistaken for a slow
failover. It is not the same event and it does not have the same cause.

**What is bounded today.** Two things.

The replacement's catch-up is bounded by the **snapshot cadence, not the
dataset**: a rejoining ex-master rewinds to its own newest local snapshot at
or before the promotion fence and tails the difference (`--rewind-snaps`,
wired automatically by `flintctl`; see docs/failover.md §Rejoin). Before
this, every rejoin was a full checkpoint transfer, and on a pair running a
10 s widowed grace the measured effect was brutal and exact: writes flowed
for the grace, then the widowed master shed everything until the transfer
finished — a **26.2 s** ack gap at a few GB (soak run 27), and growing
linearly with data. The full transfer remains only as the fallback when no
safe snapshot exists (a master killed before its first labeled snapshot, or
one whose snapshots all post-date the fence).

The fallback transfer itself is rate-limited (`--fullsync-rate-bytes`,
default 64 MiB/s) so a re-seed cannot take the disk and link the write path
needs. Uncapped, this was measured starving a freshly promoted master for
**11.9 s** while the failover itself had completed in 700 ms. The cap trades
recovery speed for write latency in the obvious direction: a slower re-seed
means longer at one copy, so raise it if you would rather have redundancy
back sooner and can absorb the latency.

**What bounds items 1 and 2** is not a recovery feature at all — it is the
write deadline in the next section, which refuses work the node cannot serve
in time whether or not a failover is involved. Recovery is simply where its
absence showed up first, because that is when demand rises as capacity falls.

## Overload — what we admit, and what we refuse

Two different things, and only one of them is a guarantee.

**Headroom is an opportunity reserve.** Running a fleet below its capacity
makes trouble less likely to bind. It mitigates; it does not promise. Traffic
grows into unenforced headroom precisely when you need it most.

**Backpressure is the guarantee.** What actually bounds behaviour is refusing
work we cannot serve, so that whatever we DO accept is served inside the
budget. That applies always, not only during a failover — recovery is just
where its absence shows up first, because that is when demand rises as
capacity falls.

**We should not hold a write for 30 s, because the client stopped waiting
long ago.** A write that completes after its caller timed out is worse than
one refused up front, for two reasons. It spends capacity that live traffic
needed. And it is ambiguous: the caller saw a timeout and retried, so the
mutation may apply twice — harmless for `SET`, a wrong answer for `INCR` and
`INCRBY`. A prompt refusal has no such ambiguity, and `-THROTTLED` already
means retry-with-backoff.

### The write deadline

**Budget: 2000 ms, enforced at admission** (`--write-deadline-ms`, hot-settable
via `FLINTCONFIG`; `0` disables it and restores unbounded queueing).

A node estimates what a write arriving now would wait — `inflight × recent
service time`, which is Little's law and nothing cleverer — and refuses it with
`-THROTTLED` if that already exceeds the deadline. Two things follow from
deciding at ARRIVAL rather than on expiry:

- **The refusal is prompt.** Queue-then-timeout spends a full deadline of
  capacity on work it then discards, on a node that by construction has none
  to spare. It is the worst of both: the client waits *and* the node is busy
  not serving anyone else.
- **The accepted set stays inside what the node can serve.** That is the
  actual guarantee. Refusing is not a failure mode here; it is the mechanism.

What it does NOT promise is that every write completes within 2000 ms. The
estimate is an estimate: a write admitted just before a stall still waits out
the stall. It bounds what is *accepted*, which is what bounds the queue.

`-THROTTLED` means retry with backoff, and it is the same signal the lag cap,
`min-replicas-to-write` and the widowed grace already use. The chaos ledger
counts a shed write as never-acked, so shedding can never be mistaken for data
loss.

Observable in `FLINTINFO`: `write_deadline_ms`, `write_inflight`,
`write_service_us`, `write_wait_est_ms` (the estimate the gate itself decides
on), and `writes_shed_deadline` (how often it has refused).

```sh
tools/write_deadline_drill.sh   # both arms: 0 shed at the default, sheds at 1ms
```

**Still local, not end-to-end.** The deadline is enforced per node, alongside
the proxy's own bounded buffer and retry budget and per-tenant `-QUOTA`. A
request crossing the proxy can therefore still spend more than one node's
deadline in total. One stated end-to-end budget spanning both hops is the
remaining work.

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
| none | 1000 ms | 0 † | **0 ms** |
| 1800 ms | 1000 ms | 75 | 1757 ms |
| 1800 ms | 200 ms | 140 | 1753 ms |

† **Every "0 shed" recorded before 2026-08-20 means NOT COUNTED, not none.**
`writes_shed_lag` did not exist until then, so the master kept no record of a
refusal; the only evidence was whatever a client happened to log. BUG-0035 had
to reconstruct "20328 of 50500" from a drill's error output for exactly this
reason. Read every historical roll summary and chaos run that says `writes
shed -THROTTLED: 0` as silent on the question.

The first counted figure came from an ordinary operation rather than a
harness: rolling the playground to rc.59 shed **210** writes on the demoted
seat during the controlled failover, canary replica at 0, shipped defaults,
nobody provoking it.

† **The margin behind this zero was ~370 ms until BUG-0038 was fixed; it is
now ~885 ms.** A sustained pipelined writer against a healthy pair on an idle
box — no stall, shipped defaults — used to peak at **631 ms of lag**, 63% of
the way to the cap, because the replica spent its CPU on one ACK syscall per
WAL batch rather than on applying. Acking once per read group instead moved
that peak to **115 ms**, and `writes_delayed_soft` from ~250 to zero: ordinary
traffic no longer enters the soft band at all. `lag_ms_max` in `FLINTINFO`
reports the peak, so this is checkable rather than assumed.

The row's zero was never wrong; what it hid was how little stood behind it.
Frozen for 1.2 s, the same pair still sheds — 323862 writes at these defaults —
because no amount of headroom survives a replica that has stopped running.
That is the shed working, and it is why the refusal stays.

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
tools/gates.sh          # fmt, clippy, tests, conformance, every core drill, chaos
```

Every claim above is an assertion in a drill that exits non-zero when it
fails. [failover.md](failover.md) explains the mechanisms; this page is only
the numbers and their conditions.
