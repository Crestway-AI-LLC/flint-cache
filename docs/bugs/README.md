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
| `0015-flintsync-probe-admits-a-cursor-the-wal-cannot-serve.md` | marked boot clears its own re-seed marker on a probe that skips the WAL-retention check, then crash-loops (RESOLVED) |
| `0016-retracted-du-is-not-the-dataset.md` | RETRACTED: `verify` was right; `du` on a data dir measures RocksDB's info LOG, not the dataset |
| `0017-rocksdb-info-log-grows-without-bound.md` | 883 MB of RocksDB debug log against 248 KB of data, unbounded and fastest under replication churn (OPEN) |
| `0019-drill-root-on-a-mounted-volume-breaks-disk-pressure.md` | `FLINT_DRILL_ROOT` on a mounted volume fails `disk_pressure` at `hdiutil attach`, and the message blames the image (FIXED) |
| `0020-restart-drill-bypasses-fleet-sh.md` | `restart` declared no ports and cleaned up with a bare `pkill`, so it was invisible to the overlap preflight and leaked (FIXED) |
| `0021-gate-logs-overwrite-so-a-failing-run-erases-the-passing-one.md` | gate logs were per-step not per-run and the next run `rm -rf`'d the lot, so re-running to diagnose destroyed the evidence you were going to diff (FIXED) |
| `0022-rocksdb-tickers-read-zero-when-statistics-are-disabled.md` | PARTLY RETRACTED: the two stall counters are DB properties and were live all along, so BUG-0013 was never blocked; the real defect — `.unwrap_or(0)` discarding a readable "cannot answer" — is FIXED |
| `0023-chain-traversal-loses-a-link-but-only-inside-a-full-gate.md` | a chain link is nil after a master kill in a gate run; 5 solo runs never reproduce it, port collision ruled out (OPEN) |
| `0024-cutover-reports-a-handoff-failure-that-did-not-happen.md` | a 5 s read timeout on the cutover handoff was reported as "source not disowned"; the source disowns anyway, and the same call on the freeze stranded a slot write-frozen (FIXED; trigger still unexplained) |
| `0025-recovery-completes-a-flip-onto-a-destination-that-never-imported.md` | the recovery controller read a destination's absent Importing record as "it owns the slot" and completed the flip, purging the source; fixed with an `Aborted` phase so an abandoned import and a completed flip are no longer the same bytes (FIXED) |
| `0026-a-disk-guard-edit-spliced-df-into-six-pipelines-and-killed-a-check.md` | a disk-guard edit pasted `df -h` into six `| sed` pipelines, so every failing step reported disk usage as its reason and one preflight became unable to fail (FIXED) |
| `0027-the-gate-leak-check-kills-other-sessions-fleets.md` | the gate's leak check was an unscoped `pgrep`, so it blamed another session's fleet on an innocent drill and `kill -9`'d it — four times in one run, seconds after `fleet_guard` had correctly refused to touch the same processes (FIXED) |
| `0028-drills-ignore-their-own-build-exit-status.md` | 26 of 54 drills ran `cargo build` without checking it, so a standalone run silently tested whatever was already in `target/release` — masked under the gate, which pre-builds, and the reason a broken build line survived for months (FIXED) |
| `0029-cert-reload-census-counts-the-whole-machine.md` | `cert_reload_fleet`'s pid census was an unscoped `pgrep -f`, so it sampled the whole box and reported another session's fleet exiting as "pids changed — something restarted", a hot-reload regression that never happened; the same pattern also matches the agent processes themselves, so opening an editor mid-drill turned it red (FIXED) |
| `0030-write-deadline-positive-control-is-calibrated-to-one-machine.md` | `write_deadline`'s positive control drove a fixed 32-thread load, one or two above the ~30-in-flight threshold a 1ms deadline needs at 33µs/write — so arming is near a coin flip (measured 1 failure in 30 CI gate runs; 12/199/3059 refusals across three local runs), and it held main red with "nothing was refused", which reads as an unwired knob rather than a condition never created (FIXED) |
| `0031-needs-reseed-is-cleared-by-the-start-that-should-honour-it.md` | a replica marked for re-seed clears the marker on start, warm-rejoins into a WAL gap the master no longer retains, re-marks and exits — forever; the FATAL promises "the next start will full-sync" and the next start breaks it. Recovered only by wiping the data dir by hand (OPEN) |
| `0032-flintctl-start-asserts-pair-0-is-the-master.md` | `start` asserts the inventory's FIRST member PONGs and calls it "master", but after a failover that member is the replica — so a down replica panics the whole start path and disables `flint-supervise`, whose job is restarting it (OPEN) |
| `0033-wal-retention-window-cannot-be-bounded-for-tests.md` | WAL retention is RocksDB's `wal_ttl_seconds`/`wal_size_limit_mb`, fixed at 6h/8GiB by `RocksKv::open` and reachable by no flag, so the BUG-0031 boundary cannot be provoked without writing >8 GiB or waiting six hours — the seam `open_with_retention` exists and has no caller. `--wal-headroom-seq` is not the knob: it sheds writes to PREVENT this, counting only LIVE replicas (OPEN) |
| `0034-flint-server-help-starts-a-node.md` | `flint-server --help` was ignored like any unrecognised flag, so it bound the default port 6380 and served forever instead of printing usage; one stray copy sat outside every drill's scope and made `fleet_guard` refuse 64 drills in one gate (`--help` FIXED; unknown-argument rejection still open) |
