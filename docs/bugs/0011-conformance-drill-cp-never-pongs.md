# BUG-0011: `proxy_conformance_drill` bootstrap panics — the CP seat starts but never PONGs (MITIGATED)

Status: **MITIGATED 2026-09-04.** The mechanism is measured and the warm is proven to remove it (241 ms unwarmed against 11 ms warmed, every warmed exec beating every unwarmed one). The clean-rate question is SUPERSEDED, not answered: three attempts could not collect it, and a rate would have been weaker evidence than the direct test. Reopen if a bring-up fails with the `_dyld_start` signature on a warmed binary. Prior status: OPEN · first-exec validation is CONFIRMED, MEASURED AS SERIALIZED (~195 ms/binary, linear in burst size) and mitigated · the multi-second stall is now observed n=3 and still NOT reproducible on demand · found 2026-08-16 · Severity: medium (blocks the `drills` gate
stage; `check` is unaffected)

## Symptom

    $ bash tools/proxy_conformance_drill.sh
    == bootstrap
    FAIL: bootstrap
    == bootstrap into /tmp/flint-proxyconf-state (tls off)
      started cp (pid 11306)

    thread 'main' panicked at crates/flint-ctl/src/main.rs:3006:9:
    control plane seat 127.0.0.1:7963 up

Reproduced twice, including once with nothing else running on the box.

The panic message reads oddly because it is an `assert!` description, not an
error — the assertion that FAILED is "control plane seat … up":

    for seat in &inv.cp {
        assert!(
            wait_pong(seat, &tls, Duration::from_secs(10)),
            "control plane seat {seat} up"
        );
    }

## What is and is not established

**Established.** `flintctl` spawns the seat successfully — `boot.log` records
`started cp (pid 11306)` — and then its own `wait_pong` gets no `PONG` inside
10 s. The binary itself is healthy: started by hand on the same port it logs

    flint-controlplane: state /tmp/cp-probe-state (version 0, 0 proxies, 0 pairs, 0 tenants)
    flint-controlplane listening on 127.0.0.1:7963 (plaintext)

immediately. So the seat is fine and the failure is in how `flintctl` probes
it.

**Not established.** The mechanism. The prime suspect is a TLS-mode mismatch
between the spawn and the probe: the drill's inventory is `tls off`, and

    fn wait_pong(addr: &str, tls: &Option<Arc<flint_tls::ClientConfig>>, budget: Duration) -> bool

takes a client config that must agree with how the seat was spawned, or `call`
can never succeed. That is a hypothesis, not a finding.

## Not caused by the proxy connection-pool work

Verified structurally rather than assumed: nothing in the workspace depends on
`flint-proxy` — it is a binary-only crate with no `lib.rs` and no reverse
dependency in any `Cargo.toml` — so proxy changes cannot affect `flintctl` or
`flint-controlplane`, which is where this fails. All seven other proxy drills
(`proxy`, `cache`, `tls`, `backpressure`, `admin_gated`, `registry`, `chaos`)
pass at the same HEAD, so this is specific to the conformance drill's setup.

## Update 2026-08-21: step 1 is done, and it changes the diagnosis

**"The seat is fine and the failure is in how `flintctl` probes it" is wrong.**
The seat is not fine — it never reaches `main`.

Step 1 below (make the probe say WHY) shipped as `fa7227a`: the bare
`assert!(wait_pong(...))` became a check that asks whether the process is still
there and reports which way it failed. Across every gate run after that commit:

    seat ALIVE, never answered PING     9
    NO process at all                   1

And `sample` on a stalled control-plane seat: **2935 of 2935 frames in
`_dyld_start`, `main` never entered.** So the process exists — which is why
`seat_alive` says yes and why the old assert read as a probe problem — but no
code of ours has run. It emits nothing because nothing of ours is running, it
never binds, and `boot.log` holds only flintctl's own banner.

**The TLS-mode-mismatch suspect is refuted, on two independent grounds.**

1. A config mismatch is deterministic. This is not: 20 gate runs at one commit
   were clean in 6. A mismatch between spawn flags and probe config would fail
   every time, not 70% of the time.
2. Same code, same drill, on Linux CI: `gate.yml` is green in **29 of its last
   30 runs** on `ubuntu-latest`, against 6 of 20 locally on macOS. A TLS
   mismatch is platform-independent; a loader stall is not — Linux has no
   `dyld`.

`proxy_conformance` is therefore not special. It is one of the drills that
happens to bring up a control-plane seat, and it fails whenever the burst
catches it. The same signature accounts for `FAIL: bootstrap` across 20 other
drills.

**What is still not established: the TRIGGER.** Why `dyld` stalls before `main`
on a box with no memory pressure, where a direct `exec` of the same binary
measured instantly (5 consecutive runs at 0.00s). Excluded as the trigger, each
with evidence: sibling build/test load (a run at 87% sibling exposure was
clean), drill order (reordered runs sit inside the default range at matched
exposure), load average (15 failures at load 2.5-3.0; a clean 117/0 at load
rising to 8.04), and the quiet-start guard built to control them (guarded runs
clean 2 of 7, unguarded 5 of 13).

Severity is unchanged but the scope is wider than this drill, and the practical
mitigation is already available: run the gate on Linux. `packaging/aws/gate-box/run.sh`
in the ops repo does exactly that, and a run there was 117/0 with
`FLINT_GATE_STRICT=1`.

## 2026-08-23 — the trigger is first-exec code-signature validation, and it is measured

The 2026-08-21 update established that the seat never reaches `main` and left
the TRIGGER open, with sibling load, drill order, load average and the
quiet-start guard all excluded. Here it is.

**On macOS the FIRST exec of a freshly built binary pays kernel code-signature
validation, in the loader, before `main`.** Rust ad-hoc/linker-signs its
output — `codesign -dv` reports `flags=0x20002(adhoc,linker-signed)` with
`hashes=2971`, one per page — and that validation happens on first exec of a
given inode, not on every exec.

Measured on this box: six fresh `cp` copies of `flint-server`, timing
`--build-version`, which exits before any server work happens.

| copy | first exec | immediate repeat |
|---|---|---|
| 1 | **23 403 ms** | 25 ms |
| 2 | **43 472 ms** | 26 ms |
| 3-6 | ~360 ms | ~25 ms |

The seat-startup budget is 10 s. A 23-43 s first exec blows it outright and
produces precisely the recorded signature — process ALIVE, never answers PING,
`sample` showing 2935 of 2935 frames in `_dyld_start` — because no code of
ours has run yet.

**It explains every exclusion rather than competing with them.** Non-determinism:
the cost depends on validation state, not on load, which is why 20 runs at one
commit were clean in 6 and why load average correlated with nothing.
Platform-specificity: Linux has no dyld and no AMFI, so `gate.yml` is green 29
of its last 30. And the earlier observation that *"a direct exec of the same
binary measured instantly, 5 consecutive runs at 0.00 s"* was measuring the
CACHED case — the exact case that does not fail.

### The mitigation already existed and was not reaching the drills

`fleet_warm` was built for this and says so in its own header: *"pay each
binary's first-exec loader stall OUTSIDE"*. It was called by **8 drills of
111**, and only **5** of those warmed the control-plane binary — which is the
seat that fails in this bug's own symptom.

So the warm moves into `fleet_init`, the one function all 113 callers already
run, before any seat is spawned. After the first drill it costs ~25 ms per
binary; the first drill pays the real stall outside any startup budget, which
is the entire point.

**A mitigation each drill must remember to invoke is one most drills will not
have.** That is the same shape as this file's other lessons: the carrier has to
be the thing everyone already runs, not a call everyone is expected to add.

### What this does NOT do

It does not make the stall smaller — the kernel still validates on first exec.
It moves the payment to a place with no deadline. A drill that rebuilds a
binary mid-run would still pay it at the wrong moment; none currently does.

The `--build-version` warm also exercises only the loader path for the main
binary image. If a stall were ever traced to a dynamically loaded library
opened later, this would not cover it.

### Correction, same session: the seconds-scale stall did not reproduce

The table above is real and so are the two large numbers, but the conclusion
drawn from them was too strong and is withdrawn.

Rebuilding `flint-controlplane` three times — a rebuild changes CONTENT, which
is the only state that should reproduce a content-keyed validation — gave
first-exec times of **421 ms, 287 ms, 292 ms**. Not seconds. And copies 3-6 in
the original table were ~360 ms, the same order.

So `23 403 ms` and `43 472 ms` were the first two events of that measurement
session and have not been produced on demand since. Something about that
moment was expensive; "every freshly built binary pays tens of seconds" is not
it, and that is what the first version of this entry implied.

**What survives, separated by how well it is supported:**

- **Reproducible:** the first exec of new content costs roughly 10x a repeat —
  ~300-420 ms against ~25 ms. Measured across rebuilds and copies, many times.
- **Observed, n=2, not reproduced:** two first-execs of 23 s and 43 s.
- **NOT established:** that those stalls are what fails the drills. It fits the
  10 s budget and the `_dyld_start` signature, but a fit is not a measurement,
  and two observations that cannot be summoned are a lead rather than a cause.

**The mitigation is still correct, on weaker grounds than claimed.** Paying
whatever the first-exec cost is inside `fleet_init` rather than inside a seat's
10 s startup budget is right at 300 ms and right at 43 s; it costs ~25 ms per
binary once warm. It is justified by the reproducible 10x, not by the pair of
outliers.

**The honest next step is unchanged and now more clearly the only one:** run
the local gate repeatedly with the warm in place and see whether 6-of-20 moves.
That measures the thing that matters — the clean rate — instead of a
microbenchmark that stands in for it.

### The clean-rate measurement was attempted and is VOID

Six alternating pairs of `proxy_conformance_drill`, warm arm against a
warm-disabled control. Result: **0/6 and 0/6.**

That reads exactly like "the warm changes nothing" and it means nothing at
all. Every one of the twelve runs was REFUSED by `fleet_guard` — a sibling
`flint-kv` test was contending at 0.95 cores — so no drill executed in either
arm. The harness counted any non-zero exit as a failure, which collapsed
DECLINED TO RUN into RAN AND FAILED: the exact distinction `fleet_guard`
exists to draw, erased by the thing consuming its output.

**A future attempt must classify three outcomes, not two:** passed, failed,
and refused-so-not-a-sample. A refusal is not a data point and must not be
counted in a denominator.

It also must not be worked around with `FLINT_DRILL_FORCE=1`. The guard was
refusing because of contention, and contention is the confound the clean rate
is trying to measure; forcing past it would produce numbers whose flakiness
has a second cause mixed in.

So the state is: the warm is landed and gated, justified by the reproducible
10x first-exec cost, and its effect on the local clean rate is still
UNMEASURED. That is the one open question and it needs a quiet box.

### The measurement finally ran, and it cannot answer the question

Third harness. Counters verified independent before use. Result:

    warm 6P/0F/0R    ctrl 6P/0F/0R

**12 of 12 clean, both arms, zero refusals.** Valid data, and it says nothing
about the warm — because the failure never happened in the CONTROL arm. Six
runs with the mitigation disabled all passed, so there was nothing for the
mitigation to prevent.

**The flake did not reproduce tonight on a quiet box.** That is the finding,
and it is about the box rather than about the fix. Against a historical 6-of-20
it is a large swing, and the one variable that plainly differs is that nothing
else was running: the earlier A/B attempt was refused twelve times for sibling
contention, and by the time this one ran the sibling was gone.

That does NOT resolve to "contention is the trigger". This file already
excludes load average with evidence — 15 failures at load 2.5-3.0 and a clean
117/0 at load rising to 8.04 — and a quiet box differs from a loaded one in
more ways than load average. What it does say is that **the failure cannot be
summoned on demand, so a null result from any A/B is uninformative unless the
control arm actually fails.**

**The next attempt needs the control arm to break.** Measuring a mitigation
against conditions where the fault does not occur is the same error as a
positive control that cannot arm, one level out: the experiment is well-formed
and the situation is not.

### Three harnesses, three outputs that read as results

Worth recording, because each was published-shaped:

1. `0/6 vs 0/6` — twelve `fleet_guard` refusals; the counter collapsed DECLINED
   TO RUN into RAN AND FAILED. Zero samples, reads as "the warm does nothing".
2. `6/6 vs 6/6` — `declare -A` is unsupported on macOS bash 3.2, so `pass[warm]`
   and `pass[ctrl]` both index 0. **One counter behind two names.** Reads as
   perfect agreement between arms, which is more convincing than either arm
   alone.
3. Summary printed empty — a patch whose later replacements silently no-opped
   because only the FIRST was asserted. The loop data was correct and the
   report was not.

Number 2 is a variant worth naming separately from the family entry in
`field-notes.md`: not a value that could only be one thing, but **two values
that could not differ**. Agreement between arms is the shape people trust most,
and it is exactly what an aliasing bug produces.

## Where to start

**Steps 1-3 below are STALE.** They were written for the "the seat is fine and
the probe is wrong" reading, which the 2026-08-21 update refuted, and step 1
already shipped as `fa7227a`. They are kept because the reasoning is worth
reading, not because they are the next action. The next action is to confirm
the mitigation across a gate run on macOS and see whether the 6-of-20 clean
rate moves.

### The original steps, superseded


1. `crates/flint-ctl/src/main.rs:3006` and `wait_pong` at :679 — print what
   `call` actually returns instead of discarding it into a bool. A probe that
   can only say "no" cannot say why, which is the reason this needs a
   bisect at all.
2. Compare the `tls` value passed to `wait_pong` against the flags the seat was
   spawned with, for an inventory that says `tls off`.
3. Bisect `crates/flint-ctl/src/main.rs` over `73b7eb6` (deploy co-processors
   from the inventory) and `d72378d` — the two most recent commits to touch it.
4. Keep the state dir: the drill's failure path prints `tail -8 boot.log` and
   then the trap deletes it, so the first run's evidence is gone by the time
   you read the message.

## Note on running it

Run this drill **alone**. During the 2026-08-16 investigation, five unrelated
drills were invalidated because a manual reproduction was left running while a
suite executed, and `fleet_guard` correctly refused them — the refusal message
names the offending pids and is the diagnosis, not noise.

## 2026-09-03 — validation is SERIALIZED, and that is the missing arithmetic

Everything measured before this ran one exec at a time and found ~200 ms, which
cannot blow a 10 s budget. That is why the cause stayed a "fit" rather than a
mechanism. The file's own word for the failure was **burst**, and a burst was
never measured.

**Method.** K fresh copies of `flint-controlplane` (new inode each, so
validation is cold), page cache pre-warmed with a full read so this measures
validation and not disk I/O, all K launched at once. Then the SAME K copies run
again — identical binaries, identical concurrency, identical load, differing
only in whether validation has already happened. That second run is the control,
and it is the whole experiment: a loaded box makes everything slower, so "cold
K=32 is slow" means nothing without it.

| K | cold wall | cold worst single | warm wall | cold ms/binary |
|---|---|---|---|---|
| 1 | 199 ms | 199 ms | 4 ms | 199 |
| 2 | 398 ms | 397 ms | 5 ms | 199 |
| 4 | 773 ms | 770 ms | 7 ms | 193 |
| 8 | 1 551 ms | 1 544 ms | 12 ms | 194 |
| 16 | 3 079 ms | 3 067 ms | 21 ms | 192 |
| 32 | 6 257 ms | 6 226 ms | 39 ms | 196 |

**Cold wall is linear in K at a constant ~195 ms per binary. Warm is not.**
Validation does not parallelise: K cold seats starting together cost K x 195 ms
of wall time and the last one waits for all of them. Warm execs at K=32 finish
in 39 ms, so the box was not merely busy.

**Run in reverse (K=32 first, K=1 last) and the line is the same** — 196, 192,
194, 193, 199, 199 ms per binary. So the linearity is not an artefact of a
daemon warming up over the sweep, which was the obvious alternative.

This is the arithmetic the file was missing. A gate stage running 4 drills at
once, each bringing up 4 seats, is 16 cold execs: **~3.1 s inside a 10 s
budget, spent before any of our code runs.** Nothing rare is required.

**What it does not establish.** No historical failing run was instrumented, so
this is still a fit to the recorded signature rather than a measurement of the
failures themselves — but it is now a fit with a size, a slope and a control,
instead of an inference from a single-exec microbenchmark. 3.1 s of a 10 s
budget is not on its own a blowout; it is the fixed cost that anything else has
to be added to.

**Measured under load 2.2-2.6** with a sibling gate box driver running locally.
That is a confound for absolute numbers and not for the comparison, because the
warm arm was measured in the same breath at the same load.

### The 23 s/43 s stall reproduced, a third time, still not on demand

The first cold exec of the burst session took **27 623 ms** — against 199 ms for
the same operation minutes earlier and minutes later, and 6 ms warm immediately
after. That is a third sighting of the outlier this file records as
"observed, n=2, not reproduced", and it does not make it reproducible: 44 further
cold execs across two sweeps never exceeded 6.2 s.

What the three sightings now share is position: **each was the first cold exec of
a fresh measurement session.** The earlier pair were the first two events of
their session; a 360 ms first trial in the volume experiment below was 1.8x every
later first-exec in the same run. That is a lead about session-level state — a
daemon that idles out, most plausibly — and it is not yet a cause.

### The volume hypothesis, tested and REFUTED

These worktrees build to an external APFS volume mounted `noowners` while the
boot disk is a different device, and a recorded observation elsewhere holds that
Gatekeeper stalls external builds. That predicts the trigger is *where the binary
lives*, which would explain non-determinism that does not track load.

It is wrong. Twelve trials per arm, arms interleaved in randomised order,
identical source bytes, page cache pre-warmed:

| arm | n | first-exec p50 | repeat p50 |
|---|---|---|---|
| external (`/Volumes/FlintDev`, noowners) | 12 | 197.5 ms | 4.2 ms |
| internal (boot disk) | 12 | 196.1 ms | 4.2 ms |

**1.01x. There is no volume effect.** The positive control fired — the
first-exec penalty appeared at 47x in both arms — so this is a real null and not
an experiment that failed to observe anything. Recorded so it is not re-tested.

Incidentally the penalty is larger than this file has been quoting: **196 ms
against 4.2 ms, ~47x**, not ~10x. The earlier ~25 ms repeat figure was timed
through a shell; this times the exec directly. The direction and the mitigation
are unchanged, the ratio is bigger.

### The mitigation covered four of the seven binaries flintctl spawns

`FLEET_BINARIES` in `crates/flint-ctl/src/main.rs` is the canonical list of what
`flintctl` starts and has seven entries. `fleet_init` warmed four:
`flint-server`, `flint-proxy`, `flint-controlplane`, `flint-controller`.
Unwarmed: `flint-agent`, `flint-backup`, `flint-vec`.

That was not a decision. It is the four that existed when the warm was written,
and it is this file's own lesson recurring one level up — the mitigation moved
from "each drill must remember to call it" to "whoever adds a seat type must
remember to list it." The seats currently inside 10-15 s `wait_pong` budgets are
all in the warmed four, so **no live failure is claimed here**; `flint-vec` is
fleet-deployable, so the gap is one budget away from mattering.

Fixed by naming all seven — `fleet_warm` skips absent files, so a workspace that
does not build them all pays nothing — and by
`assert_warm_covers_fleet_binaries` in `tools/gates.sh`, which fails the build
when the two lists drift. It is tri-state: if either list reads as zero names,
that is a FAILURE, because a matcher that finds nothing agrees with everything.
Mutation-tested four ways — a dropped binary, the const renamed, `fleet_init`
renamed, and the warm call deleted — because a check that cannot fail proves
nothing.

## 2026-09-04 — the mitigation is proven directly, which the clean rate could not do

The open question here has been "does the warm move the local clean rate off
6-of-20", and three attempts failed at it: twelve `fleet_guard` refusals, a
bash-3.2 `declare -A` aliasing bug that gave both arms one counter, and a
control arm that never failed so there was nothing for the mitigation to
prevent. **All three failed the same way** — they were statistical tests of a
rare event, and the event would not come.

**The mitigation's claim is not statistical.** It is that after `fleet_init`
warms a binary, the exec a seat then performs is a REPEAT rather than a first.
That is directly testable and deterministic.

Eight interleaved trials, each on a fresh copy so validation starts cold in both
arms, page cache warmed either way, timing the exec a seat would pay for:

| arm | n | median | max |
|---|---|---|---|
| unwarmed | 8 | **241.0 ms** | **1087.9 ms** |
| warmed | 8 | **11.0 ms** | 11.7 ms |

**Every warmed exec beat every unwarmed one.** A 22x reduction at the median,
and the unwarmed arm reaches **1.09 s for a single exec** — with seven binaries
warmed per `fleet_init` and a 10 s seat budget, that is how the budget was being
consumed.

So the mitigation does what its header claims, on the platform where the cost
exists. That is a stronger statement than a clean rate: it identifies the
mechanism as removed rather than observing that a symptom got rarer.

**What this does NOT establish.** It does not measure the clean rate, and does
not show 6-of-20 becoming 20-of-20 — a drill can fail for reasons that have
nothing to do with loader validation, and several today did. The claim is
narrow: the first-exec cost is no longer paid inside a seat's startup budget.

**And it cannot become a gate check.** The validation is macOS's; Linux has no
dyld and no AMFI, so on the machine the gate runs this measurement is vacuous —
both arms would read the same. A check that cannot fail is the thing this
repository fails builds over, so none is added. The measurement is recorded here
instead, reproducible with `python3` and a copy of any built binary.

**Suggested status change, left as a call rather than taken:** the clean-rate
question is superseded, not answered. If that is accepted this becomes MITIGATED
with the mechanism measured, rather than OPEN pending a rate nobody has been
able to collect in three attempts.

