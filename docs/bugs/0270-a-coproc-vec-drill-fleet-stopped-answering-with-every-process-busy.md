# BUG-0270: a `coproc_vec_drill` fleet stopped answering, every process busy, for 30 minutes (CLOSED, not reproduced)

**Status:** **CLOSED 2026-10-31, not reproduced** (Acceptance, below):
`coproc_vec_drill` ran 50 times on the gate box under the per-drill cap,
with the stall capture in place, and passed every time. Found 2026-10-20 at
16:27 PT on the laptop, in a local run of `coproc_vec_drill` whose fleet
had started at about 15:57 PT. The run was on the ADR-0060 branch
(`adr0060-ft`) with ADR-0017's D3 amendment, uncommitted then and committed
that evening as 228a1dfc. The cause is unknown, so it is not known whether
it lies in the branch's changes or in public main (3336b916) beneath them.
**Severity:** unknown. A fleet that stops answering would be high, but one
observation on a loaded laptop, with no stacks taken, does not show whether
the fault is Flint's.
**Area:** unknown. These ran: `flint-proxy` (4 workers), `flint-server`
(`--engine mem`, one pair) and `flint-vec` (`--vec-dir`), as the drill
starts them.

## What was seen

- The drill had passed its earlier sections. In its ADR-0059 filters
  section, a `VEC.SEARCH` through the proxy had waited about 30 minutes for
  a reply. Both layers on that path have 5-second timeouts.
- None of the three processes answered anything over a raw socket within 6
  seconds, not even `PING` on the proxy or `FLINTINFO` on the co-processor.
- All three were running and busy, by `ps` after 33 minutes:

  | process | state | CPU |
  |---|---|---|
  | `flint-proxy --workers 4` | R | 224% |
  | `flint-server --engine mem` | R | 78% |
  | `flint-vec` | R | 115% |

- Sockets on the proxy's and the co-processor's ports: 3 `CLOSE_WAIT`,
  4 `ESTABLISHED`, 3 `FIN_WAIT_2`, 2 `LISTEN`. A `CLOSE_WAIT` is a
  connection the peer closed and this side has not.
- The co-processor's last log lines were an ordinary restart and rebuild:
  `rebuilt ns "ns" (23 vectors) from durable rows`, after one row whose
  length did not match its set's DIM was left out, as the drill intends.
- No stacks were taken. `sample` printed nothing, and `lldb` was refused
  ("Not allowed to attach to process"). The fleet was stopped by its pids,
  and the drill's cleanup deleted its logs.

## Not reproduced since

`coproc_vec_drill` passed, through its ADR-0059 section, in each of these:
- two full local runs of the D3 amendment's code (2026-10-20 evening);
- the ADR-0061 local check (2026-10-21);
- gate g0060 on the box, on 4107f169 (2026-10-21; that gate was red only
  on clippy and `edge_roll`).

It also passed in gate g0269 on public main. One attempt to reproduce it, on
a small fleet of the same shape with a co-processor restart and retried
searches, rebuilt and went idle.

And 50 times in a row on the gate box (c7i.xlarge), on public main
abdb31c0, 2026-10-31 03:08 to 03:29 UTC: each run through `capped_drill`
under the 900-second cap, so a stall would have taken its evidence before
the kill. All 50 printed the drill's `PASS:` line and none a `FAIL`; they
took 1,277 seconds together, the first 135 with the build and the rest
about 23 each.

## What would settle it

- **Stacks the next time: in place 2026-10-21.** When the per-drill cap
  fires, `capped_drill` now calls `stall_evidence`
  (`tools/lib/stall-evidence.sh`) before its kill. The drill's log then
  holds:
  - its process tree, with each process's state and CPU;
  - each daemon's threads, with CPU, state and, on Linux, the kernel wait
    channel;
  - a stack from `eu-stack`, `gdb` or `sample`, or a line saying none could
    attach;
  - each daemon's sockets, from `lsof` or `ss`;
  - the last 40 lines of every log under the drill's `fleet_init` scope.

  On the laptop `sample` attaches and gives a call graph. On the gate box,
  ptrace may be refused, and then the thread states are what it keeps.
  Held by `stall_evidence_drill`. Its probe's EXIT trap deletes the log the
  evidence must show, so the drill fails if the evidence is taken after the
  kill (checked by moving the call after it).
- **The `CLOSE_WAIT` owner.** `lsof -i` on the ports names the process that
  holds each half-closed connection. If it is the proxy, a worker that loops
  on a connection whose peer has gone would explain both the busy proxy and
  its silence.
- **Acceptance:** the cause is found and held by a test, or `coproc_vec_drill`
  runs 50 times on the box with no stall now that the stack capture is in
  place. Then this closes as not reproduced.

  **Met 2026-10-31:** 50 runs on the box, 50 passed, no stall (above). The
  capture stays in place, in every drill run under the cap, so a second
  sighting would arrive with its stacks and reopen this.
