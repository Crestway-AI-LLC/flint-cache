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
