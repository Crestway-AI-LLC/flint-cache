# BUG-0159: `verify` has no controller assertion, so a fleet with no failover passes it (FIXED 2026-09-17)

Status: **FIXED 2026-09-17**, found 2026-09-16 by the peer session immediately
after the rc.73 roll; the code half confirmed here the same hour · Severity: **medium-high
as a verification gap, not as a defect** — nothing is broken, and that is the
problem: the command the runbooks and the release checklist use to say *the
fleet is healthy* cannot see the component whose absence turns one node death
into an outage.

## The observation, and it was live

Immediately after the roll, on the playground:

```
status : controller NONE REPORTING
verify : OK
```

Both correct. `verify` reports on the control plane, the pairs, the proxies and
the TLS posture, then declares the data plane skipped. It has no controller
assertion of any kind.

## Confirmed in the code, not inferred from the output

`verify_checks` in `crates/flint-ctl/src/main.rs` contains **zero occurrences of
the string `controller`**. What it does assert, read off its own messages:

| it checks | it does not check |
|---|---|
| cp `reachable`, `settled` | anything about the controller |
| each pair's `master` | |
| `single build across the fleet` | |
| `declared capacity fits the disk` | |
| `proxy up`, `registry has no stray proxies`, `every declared proxy is registered` | |
| `plaintext` (TLS posture) | |

The control plane already knows the answer — `cpinfo_controllers` exists three
functions away and its own comment says *"only one of them means the controller
might be missing"* — so this is a missing assertion rather than a missing
capability.

## Why it matters more than a missing row

The controller is what performs failover. A fleet with pairs up, a reachable CP
and no controller is **a fleet that will not promote a replica when a master
dies** — and it answers every other question correctly, which is what makes it
survive a check written as a list of components that are up.

`verify` is not an idle command: it is what `verify_after` runs following
`upgrade`, `expand`, `swap` and `roll`, and what the release checklist and the
playground runbook use to declare an operation good. All of those currently
declare success without ever asking whether failover exists.

## What this bug is NOT

- **Not a claim that the roll lost the controller.** It did not. The peer read
  `NONE REPORTING` thirteen seconds after the roll, inside the controller's
  first registration interval, and reported it as a loss before checking the
  process age — their own correction, recorded in the ops field notes. The
  transient is the reason the gap was noticed; it is not evidence of it.
- **Not a claim that `status` is wrong.** `status` reported exactly what the CP
  told it. The gap is in `verify`, which never asks.
- **Not a proposal to fail `verify` on `NONE REPORTING`.** That would red every
  verify run in the first seconds after a roll, which is precisely when
  `verify_after` runs. Whatever lands has to distinguish *not yet registered*
  from *gone*, and the controller's registration interval is the number that
  distinguishes them.

## The adjacent finding, recorded because it will mislead the next reader

**The last promotion of a roll is not in the controller's log.** `flintctl
upgrade` drives its handovers itself, and the controller only logs promotions it
makes from failure detection — so mid-roll the newest `PROMOTED` line names the
*other* node. That reads as a disagreement between the controller's log and
`status` and is not one. Also the peer's, from the same roll.

## The fix

A `== controller` section in `verify_checks`, and the difficulty was never the
assertion — it was not reddening every roll with it.

**An empty answer is waited out, not believed.** The CP holds the controller
registry in memory (`#[serde(skip)]`), so a control plane that restarted knows
of no controller until the next 30s tick, and `verify_after` runs inside
exactly that window. `CONTROLLER_REGISTER_WINDOW` (45s) is how long verify
waits for a row before calling the controller absent, and `gates.sh` asserts it
stays above the controller's own `REGISTER_EVERY` — the two constants live in
different crates with nothing else linking them, and a window that fell under
the interval would redden healthy rolls.

**The three states stay apart**, because collapsing them is what the sketch
warned about:

| state | what verify says |
|---|---|
| no CP answered | controller state is unknown — and the reachability failure above is the finding |
| rows exist, none parse | the rows are unreadable, named as such, not an absent controller |
| asked, none in 45s | FAIL, naming the consequence: this fleet does not fail over |
| rows exist, all STALE | FAIL — it registered once and stopped, the same outage as none |
| a live row | ok, and its build is compared against the fleet's |

**Two live rows are reported, not failed.** Rows are keyed `host:pid`, and a
replaced controller's row stays `live` for `CONTROLLER_STALE_MS` (90s) after its
process is gone — so a roll legitimately shows two, and failing on that would
have been the same false alarm one level along.

## How it was verified

`tools/verify_controller_drill.sh`, three arms, where arms 2 and 3 differ by a
single fact — whether the controller process is alive — with the same
inventory, the same CP restart and the same empty registry:

1. live controller → verify passes and names it
2. registry cleared, **controller alive** → verify still passes, having waited
   the window out. This is the false alarm a roll would otherwise get.
3. registry cleared, **controller killed** → verify FAILS, names the controller
   and states the consequence

**Two mutations kill it**, run against the drill rather than argued:

- `CONTROLLER_REGISTER_WINDOW` cut to 5s, below the controller's 30s tick →
  **arm 2 fails** (`verify refused a fleet whose controller is alive`), and the
  new `gates.sh` assertion fails independently.
- the absent-controller branch turned into a pass → **arm 3 fails**
  (`verify PASSED a fleet with no controller`).

## The sketch this was fixed from, kept

An assertion that a controller is registered AND its last report is younger than
some multiple of its heartbeat interval, with the *not yet registered* case
distinguished by the controller's own registration interval rather than by a
constant picked here. `cpinfo_controllers` already returns the three states this
needs, including the one that means "the CP could not be asked", which must not
read as "no controller".

Filed rather than fixed at the time: found during someone else's rollout, and
the threshold is a measurement question that deserved its own change. The
threshold it asked for turned out to be the controller's own `REGISTER_EVERY`,
read from the source and asserted by the gate rather than picked here.
