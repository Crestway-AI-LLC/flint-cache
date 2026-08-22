# BUG-0011: `proxy_conformance_drill` bootstrap panics — the CP seat starts but never PONGs (OPEN)

Status: OPEN, found 2026-08-16 · Severity: medium (blocks the `drills` gate
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

## Where to start

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
