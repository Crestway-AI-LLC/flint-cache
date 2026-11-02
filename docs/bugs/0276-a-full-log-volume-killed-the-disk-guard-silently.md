# BUG-0276: a seat whose volume filled lost its disk guard, silently and for good, to a log line that could not be written (FIXED)

**Status:** **FIXED 2026-11-02** in a new crate, `flint-log`, used by every
daemon (below). **OPEN 2026-11-02** before that, found by the error-handling review's
cross-cutting sweep (`docs/error-handling-review.md`, area N) and measured the
same day on a gate box. In public main (`8d194841`, v0.1.0-rc.86) and every
release with a disk guard.
**Severity:** high when it happens, rare to reach. Nothing is lost at the
moment it happens. Afterwards the seat no longer sheds writes or reclaims
space as its volume fills again, and its `FLINTINFO` disk fields go on
reporting the last reading, so every check that reads them sees a healthy
disk.
**Area:** every daemon's logging: `eprintln!` and `println!`, 243 calls in
ten crates; the threads that log (the disk guard in `flint-server`
`main.rs`, and the WAL fsync, replication, lease and GC threads beside it).

## What happens

- **`eprintln!` and `println!` panic when their write fails.** That is their
  documented behaviour: "Panics if writing to `io::stderr` fails."
- **A seat's stderr and stdout are a file on its own data volume.** flintctl
  opens `{statedir}/logs/<seat>.log` for both, and the data directory is
  `{statedir}/node-<port>`, on the same filesystem.
- **So on a full volume, the first log line that needs a new block panics
  the thread that writes it,** whatever that thread is. A panic on a
  connection's thread ends that connection, and costs nothing else. A panic
  on a background thread ends the thread. None of them is restarted, and the
  panic's own message cannot be written either.
- **The disk guard is the thread most likely to log at that moment.** It logs
  a transition before it applies it (`elog!("disk guard: {last:?} -> {v:?}")`
  comes before `DISK.apply(usage, v)`). A fill faster than one sample takes
  it straight from `Ok` to a full volume, and the line saying so is the one
  that panics. The verdict is never applied, and the guard never samples
  again.
- **The same holds for every background thread that logs:** the WAL fsync
  thread logs a failed fsync, the replication thread its reconnects, lease
  renewal its failures, and the GC sweep its errors. A full log volume ends
  each of them on its next line.

## Measured, 2026-11-02

`flint-server` at public `8d194841` on a gate box (c7i.xlarge), its data
directory and its log on one 512 MB tmpfs as flintctl lays a seat out,
`--disk-min-free-pct 10 --disk-min-free-bytes 0`:

| step | `FLINTINFO` | `SET` | threads |
|---|---|---|---|
| A. a slow fill to 5% free | `disk_free_pct:4 disk_verdict:shed` | `-QUOTA` | 12 |
| A. freed | `disk_free_pct:99 disk_verdict:ok` | `OK` | 12 |
| B. a fast fill to 0 free, the log's last page full | `disk_free_pct:99 disk_verdict:ok` | | **11** |
| B. freed | `disk_free_pct:99 disk_verdict:ok` | `OK` | 11 |
| C. a slow fill to 5% free | `disk_free_pct:99 disk_verdict:ok` | **`OK`** | 11 |

After B the guard thread is gone, and the seat's log has no line from it.
At 5% free in C the seat accepts a write that A refused, and reports 99%
free. The first run, without the log's last page full, did not show it: the
`Ok -> Shed` line fitted in the block the log already had, so it was written
and the guard went on. Whether a line needs a new block is chance: a 58-byte
line, like the guard's, crosses a 4 KiB block about once in seventy, so on a volume that stays full the
guard's next transition, or any background thread's next line, is the one.

## The fix

**Logging cannot panic.** A new crate, `flint-log`, has `elog!` and `olog!`,
which take `eprintln!`'s and `println!`'s arguments and drop a failed write.
In a crate's own unit tests they go through the standard macros, so the test
harness still captures them. Every daemon crate (`flint-server`,
`flint-storage`, `flint-vec`, `flint-proxy`, `flint-controller`,
`flint-controlplane`, `flint-backup`, `flint-exporter`, `flint-journal`,
`flint-tls`) uses them for all 243 of its lines, and denies
`clippy::print_stderr` and `clippy::print_stdout`, so the gate refuses the
panicking forms if they come back. `flint-vec`'s bench binary keeps
`println!`: its stdout is its result, on a terminal. `flintctl` keeps its
own, being a command a person runs rather than a seat.

A lost line is the lesser harm: what the guard's line said is in
`FLINTINFO` and the journal.

## Acceptance

The measurement above, run again on the fix: after B the thread count is
unchanged, and at 5% free in C the seat sheds (`disk_verdict:shed`,
`-QUOTA`), as in A. And `flint-log`'s
`a_failed_write_is_dropped_not_a_panic`: a write to a pipe with no reader
fails (its control) and the line writer does not panic.

**Met 2026-11-02,** on a gate box, the fix on public `8d194841`:

| step | `FLINTINFO` | `SET` | threads |
|---|---|---|---|
| A. a slow fill to 5% free | `disk_free_pct:4 disk_verdict:shed` | `-QUOTA` | 12 |
| B. a fast fill to 0 free, the log's last page full | `disk_free_pct:0 disk_verdict:shed` | | 12 |
| B. freed | `disk_free_pct:99 disk_verdict:ok` | `OK` | 12 |
| C. a slow fill to 5% free | `disk_free_pct:4 disk_verdict:shed` | `-QUOTA` | 12 |

`cargo clippy --workspace --all-targets`, with and without the rocks
features, passes with the two lints denied, and both `flint-log` tests pass.
