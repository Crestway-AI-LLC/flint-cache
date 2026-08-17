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
