# BUG-0011: `proxy_conformance_drill` bootstrap panics — the CP seat starts but never PONGs (OPEN)

Status: OPEN · a first-exec cost is CONFIRMED and mitigated 2026-08-23, but the multi-second stall is observed (n=2) and NOT reproduced · found 2026-08-16 · Severity: medium (blocks the `drills` gate
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
