# BUG-0282: a control-plane seat whose Raft core stopped went on answering as the leader, and renewed a fenced master's lease (FIXED)

**Status:** **FIXED 2026-11-11.** Found the same day by the error-handling
review's area C question, "Each Raft storage callback changes the
in-memory vote, log or registry and then flushes. If the flush fails after
the change, what does the running node serve next?"
(`docs/error-handling-review.md`), and measured on release binaries. In
public main (`f9ec46e9`) and v0.1.0-rc.87.
**Severity:** high. A master that a promotion has fenced can keep its write
lease, and go on accepting writes beside its successor, with no bound. It
needs a control-plane seat that was leader when one of its writes to its
own store failed (a full volume, a volume remounted read-only after an I/O
error), and a master whose renewal reaches that seat before the others.
**Area:** `flint-controlplane`, the HA (Raft) node: `ha.rs`.

## What happens

- **openraft stops its core on a storage error.** Every storage callback
  ends in `flush`, and a flush that fails returns a `StorageError`, which is
  fatal: the core stops, and the node takes no further part in the cluster.
  The other seats elect a leader without it.
- **The process went on serving.** Nothing watched for the core stopping.
  The node's metrics froze as they were when it stopped, so a seat that was
  leader went on reporting itself as leader: `CPINFO` answered `leader:` its
  own id.
- **`CPLEASE` is leader-only, and checked that by those frozen metrics.**
  It asks `raft.current_leader()`, which openraft documents as for routing
  only, and answers from the seat's own registry. That registry predates
  every promotion the new leader has recorded since. So a master the new
  leader had fenced, renewing against this seat, was answered OK.
- **A master's renewer takes the first seat that answers.** It walks
  `--lease-cp` in order and stops at the first definitive reply. A master
  whose list starts with the dead seat was renewed there on every attempt,
  and never heard `SUPERSEDED`.

## Measured, 2026-11-11

Release binaries of public `56bd9463` on a laptop: three Raft seats, each
with its store in its own directory. Seat 1 was elected. Through it: a pair
`127.0.0.1:6795,127.0.0.1:6796`, and `CPLEASE 127.0.0.1:6795`, which took the
lease. Seat 1's directory was then made read-only, and one write sent to it,
so that its next flush failed.

| step | seat 1 | seats 2 and 3 |
|---|---|---|
| the write to seat 1 | `ERR no leader elected yet, retry`; the process stays up | |
| about 5 s later | `CPINFO`: `leader:1` | elected seat 3 |
| `CPFENCE 127.0.0.1:6796` at seat 3 | | `OK fenced 127.0.0.1:6796` |
| `CPLEASE 127.0.0.1:6795`, the fenced master | **`OK`**, and again 10 s later | seat 3: `SUPERSEDED 127.0.0.1:6796` |

## The fix

**A Raft core that stops ends the process,** with status 70, as a seat
whose vital thread dies does (ADR-0065). A task watches the node's metrics,
and when `running_state` turns to an error, or the core goes away, it logs
`the Raft core stopped (<cause>)` and exits. The supervisor starts the seat
again, `Store::open` reads what is durable and refuses a store it cannot
read (BUG-0191), and the seat rejoins as a follower. It acknowledged nothing
after its core stopped, so nothing is lost by leaving.

On the same steps after the fix, seat 1 exited 70 naming
`Permission denied`. The fenced master's renewal: seat 1 refuses the
connection, seat 2 redirects to the leader, and seat 3 answers `SUPERSEDED`,
which is the renewer's path to fencing itself.

**Not in this fix, and open.** `CPLEASE` still trusts `current_leader()`.
A leader cut off from the other seats by a partition, with its core
running, may answer as leader until openraft makes it stand down.
`ensure_linearizable()` before answering would confirm leadership with a
quorum, at one round trip per renewal. It is in the review map as a
proposal, not measured.

## Acceptance

`cp_dead_core_drill`, on a real three-seat Raft control plane:
- the leader's directory made read-only, one write sent to it: its process
  exits 70, saying the Raft core stopped and why;
- the remaining seats elect a leader, which records `CPFENCE` of the
  successor;
- no seat answers OK to the fenced master's renewal, and the successor's
  renewal is answered OK (the control).

On the binaries of `56bd9463`, whose control plane `f9ec46e9` did not
change, the drill fails at its second step: the seat serves on, saying
`leader:` itself, and answers the renewal OK.
