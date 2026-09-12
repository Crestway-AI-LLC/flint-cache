# BUG-0136: three drills killed each other's control plane, and two of them passed anyway (FIXED 2026-09-12)

Status: **FIXED 2026-09-12**, found the same day · Severity: **high** — for as
long as the gate has had a parallel path, two drills have been able to pass a
test they never performed. The third failed loudly, which is the only reason
this was found.

Found while verifying [BUG-0134](0134-a-failing-drill-assertion-hangs-the-job-instead-of-failing-it.md):
its fix lives in the `FLINT_GATE_JOBS>1` branch, so it needed a 4-wide run to
cover it, and the 4-wide run went red on a drill that change does not touch.

## What was observed

Two 4-wide `drills` runs on the gate box (`c7i.xlarge`, `FLINT_GATE_JOBS=4`),
one with the BUG-0134 commit and one on its unmodified parent:

| run | commit | PASS | FAIL | failed |
|---|---|---|---|---|
| BUG-0134 | `aeb10a6` | 134 | 1 | `cp_kill_datapath` (5.0s) |
| baseline | `6ecc790` | 134 | 1 | `cp_kill_datapath` (5.0s) |

Identical: same drill, same 5.0s, same message, ~1 GB peak memory in both.
**Deterministic, and present without the change under test.**

```
== kill -9 the CP LEADER (node 1) while the tenant is reading and writing
FAIL: could not kill seat 1
```

`pkill` exits 1 when nothing matched. The seat it was told to kill was gone.

## The defect

Three drills killed a control-plane seat with the *same* pattern:

```
control_tls_drill.sh:116       pkill -9 -f "flint-controlplane --raft --node-id $LEADER "
controlplane_ha_drill.sh:146   pkill -9 -f "flint-controlplane --raft --node-id $LEADER "
cp_kill_datapath_drill.sh:121  pkill -9 -f "flint-controlplane --raft --node-id $LEADER " || FAIL
```

No port, no path — only `--node-id`. All three run a three-seat raft control
plane, and all three elect node 1. **Any two of them running at once kill each
other's leader.** Demonstrated against the real command lines rather than
argued:

| pattern | matches its own seat | matches a peer's seat |
|---|---|---|
| old, `--node-id 1 ` | 1 | **1** |
| new, `--node-id 1 .*--state $D/` | 1 | 0 |

Ports were never the protection. `assert_no_duplicate_drill_ports` passes —
all 442 declarations are distinct, and these drills declare theirs (7501-7503,
7541-7543, 7571-7573 plus raft ports) — but a pattern that mentions no port
cannot be saved by the ports being distinct.

## Why only one of the three ever said anything

`cp_kill_datapath` is the only site that checked `pkill`'s exit status. The
other two killed and walked on:

```bash
pkill -9 -f "flint-controlplane --raft --node-id $LEADER "
NEW=""
for i in $(seq 1 40); do ...        # wait for a new leader
```

So when a peer's kill landed first, those drills killed **nothing**, waited,
found the new leader that the peer's kill had caused, and **passed**. The
promotion they assert on happened — for someone else's reason. That is worse
than the visible failure in every way that matters: a red drill costs an hour,
a green one that tested nothing costs however long nobody looks.

## Why it stayed hidden

- **The gate box ran the drills serially.** `GATE_JOBS` defaults to 1 and
  `packaging/aws/gate-box/run.sh` does not forward `FLINT_GATE_JOBS`, so every
  box gate exercised a path where these three never overlap.
- **CI runs 4-wide and passes them**, because a different set of drills is
  co-resident there — the same platform difference that makes `ns_escape` take
  383-387s on a GitHub runner and 37-56s on the box.

So the defect needed the parallel path *and* the box's particular scheduling,
and nothing routinely ran both.

## The check that existed, and could not see it

`assert_no_cross_drill_kill_patterns` was written for exactly this hazard, and
it reads only patterns that contain a port:

```bash
grep -hoE 'pkill[^|]*"[^"]*--port [0-9]{1,5}"'
```

It refuses a **truncated** port — `--port 757` reaching 7571 — which is a real
shape and was a real bug. The shape that shipped has no port at all, so the
grep matched nothing, the loop body never ran, and the check reported clean.

A check aimed at one shape of a defect, blind to the shape in front of it, is
the thing `tools/gates.sh` exists to stop shipping. It is the same failure as
BUG-0134's hung assertion one level up: the machinery was right and could not
speak.

## The fix

**1. Four patterns scoped to the drill's own state directory.** `$D` is under
`FLINT_DRILL_ROOT` and unique per drill, and every one of these seats spawns
with `--state $D/n<id>`:

```bash
pkill -9 -f "flint-controlplane --raft --node-id $LEADER .*--state $D/" \
  || { echo "FAIL: could not kill seat $LEADER — no CP process matched this drill's own state dir"; exit 1; }
```

The fourth is `ctl_cpha_drill.sh`, which matched `cp-state-n$LEADER_ID` —
flintctl's own naming, unique only because one drill uses it. Accident, not
construction, so it is now scoped too. Note its seats are spawned by
`flintctl`, which emits `--state` **before** `--raft`; the file already
carried a comment saying so, which is why its pattern reads differently.

**2. A kill that killed nothing fails, at all three sites.** This is what
turned a silent pass into a diagnosable red, and it is the half that would
have found this on the first 4-wide run years earlier than it was found.

**3. `assert_kill_patterns_name_their_own_fleet`.** A pattern naming a flint
component must also name something belonging to this drill: a port (as the
flag, the controller's `--nodes` list, or a variable whose name says port), or
a path under `FLINT_DRILL_ROOT`. **69 patterns** are examined; the check
refuses if it ever examines none. Verified by planting the original pattern
back — flagged with file, line and text — and removing it again.

Its python assembles its own quote characters (`chr(34)`/`chr(39)`) for a
mundane reason worth recording: the heredoc sits inside `$( )`, bash 3.2 scans
that body for quotes, and an **odd number of apostrophes swallowed the closing
paren** and stopped the whole file parsing. The bug-citation check already
builds its backtick with `chr(96)` for the same reason.

**And its first real run failed `gates_drill`** — `GATES FAILED:
kill-scope-examined-nothing`. That drill forges a tree holding nothing but
`tools/gates.sh` and runs a copy of the gate there, so "no patterns examined"
is the correct answer in that tree and the coverage guard called it a broken
glob. `_have_drill_files` exists for exactly this and its comment says why;
both checks added today now sit behind it. The repo's own self-test caught a
defect in the check written to catch a defect, which is the argument for
having it.

## The gap this came through, closed the same day

This was written saying `FLINT_GATE_JOBS` was still not forwarded by the
gate-box launcher, so the box and CI still ran different code paths and a
change to the parallel path had to name
`FLINT_GATE_CMD='FLINT_GATE_JOBS=4 tools/gates.sh drills'` explicitly — which
is how the runs above were done. **That is fixed.** The launcher now reads the
number out of this repo's `.github/workflows/gate.yml` and exports it, so a
plain `run.sh drills` goes as wide as CI: verified with nothing set in the
environment —

```
== run (public gate, in ~/flint): tools/gates.sh drills
   drills at once: 4 (from CI (.github/workflows/gate.yml))
== parallel: 4 drills at a time on 4 core(s) (1.00 drills/core)
GATES PASSED — 135 steps
```

The number is **derived** rather than copied, because `gate.yml`'s own input
documentation says it "lives in exactly one place" — it says that because a
dispatch input once carried its own default of 6 while the env said something
else, and a third copy in the launcher would have been the same defect a third
time. An explicit `FLINT_GATE_JOBS` still wins, which is the rollback.

The launcher lives in the ops repo, so the change and its reasoning are
recorded there; this section exists because this file told readers the gap was
open, and a write-up that describes a fixed defect as live is the failure mode
its own index check was built to stop.
