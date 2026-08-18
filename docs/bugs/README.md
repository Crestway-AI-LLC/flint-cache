# Bug write-ups

One file per bug that was worth explaining rather than just fixing: the
symptom, the wrong conclusion that was drawn first, the root cause, and the
check that now holds it. They are kept because the misdiagnosis is usually
the expensive part, and a fix without it teaches nothing.

## About the `#NNN` references in comments

Code and commit messages across this repo cite bugs as `#118`, `#133`,
`#140` and so on. Those are ids in Crestway AI's internal work tracker, not
GitHub issue numbers and not links — they predate this repository being
public, and most have no write-up here.

Treat them as provenance, not as a lookup: a comment saying "(#133)" is
telling you the line exists because of a specific real incident, and the
comment itself carries what you need. Where a bug earned a full write-up,
it is a file in this directory and the comment points at the path instead.

New references should use the path, so a reader can follow them.

| File | What it is about |
|---|---|
| `0001-fullsync-race-under-load.md` | promotion of an unconverged replica loses data |
| `0002-verify-ok-on-single-copy.md` | `verify` called a pair with a dead member healthy |
| `0003-drill-port-overlap.md` | drills sharing ports adopt and kill each other's seats |
| `0004-start-replaces-a-starting-seat.md` | `start` wiped a seat that was mid-sync |
| `0005-oneshot-kills-its-own-seat.md` | a systemd oneshot killed the daemons it spawned |
| `0006-silent-bootstrap-fakes-auth-failures.md` | a silent drill bootstrap reports WRONGPASS on a token it never created |
| `0007-armed-clock-blames-replica-kill.md` | ledger judged old-master acks by the pre-kill clock; a replica kill got blamed for a master kill's loss |
| `0008-cold-start-of-a-failed-over-pair.md` | cold start of a failed-over pair replicated nothing |
| `0009-unknown-stage-passes-the-gate.md` | an unrecognised stage argument ran nothing and printed GATES PASSED |
| `0010-drill-port-overlap-recurs-on-7411.md` | BUG-0003 again: 7411 claimed by two drills, so every gate exits FAILED (FIXED) |
| `0011-conformance-drill-cp-never-pongs.md` | `flintctl bootstrap` spawns the CP seat, then its own probe never gets PONG (OPEN) |
| `0012-walgap-livelock-retention-ignores-replicas.md` | WAL retention ignores replica progress, so a lagging replica can never catch up |
| `0013-bulk-writes-stall-on-default-compaction.md` | compaction left at RocksDB defaults, so bulk ingest hits the write stall |
| `0014-chaos-unreadable-acked-loss-on-replica-kill.md` | chaos oracle fails an acked write on a REPLICA kill, intermittently; cause not established (OPEN) |
| `0015-flintsync-probe-admits-a-cursor-the-wal-cannot-serve.md` | marked boot clears its own re-seed marker on a probe that skips the WAL-retention check, then crash-loops (OPEN) |
| `0016-retracted-du-is-not-the-dataset.md` | RETRACTED: `verify` was right; `du` on a data dir measures RocksDB's info LOG, not the dataset |
| `0017-rocksdb-info-log-grows-without-bound.md` | 883 MB of RocksDB debug log against 248 KB of data; the disk guard, capacity model and meter all count it (OPEN) |
| `0018-upgrade-needs-the-new-cp-before-it-can-roll-masters.md` | `upgrade` rolls masters using a CP verb that only the not-yet-rolled CP understands (OPEN) |
