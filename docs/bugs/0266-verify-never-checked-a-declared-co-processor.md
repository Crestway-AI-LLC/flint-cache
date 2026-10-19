# BUG-0266: `flintctl verify` never checked a declared co-processor, so a dead flint-vec left it green (FIXED 2026-10-19)

**Status:** **FIXED 2026-10-19.** `verify` has a `== co-processors` section:
each `coproc` line in the inventory must answer FLINTINFO over the mesh, or
verify fails and names it. Found by ops while checking what would notice
BUG-0265 being used (ops OPS-0406; Jeff chose this fix and a supervisor
restart-loop page, 2026-10-19). Held by `coproc_family_drill` step 6.
**Severity:** medium. A co-processor serves its whole family for every
tenant: while flint-vec is down, every `VEC.*` command fails. verify read
that fleet as healthy, and ops `verify-watch`, which pages on a failed
verify, stayed quiet. Until BUG-0265's fix ships, a tenant can crash flint-vec
on demand.
**Area:** `crates/flint-ctl/src/main.rs`, `verify_checks`.

## What happened

On the playground (rc.84, 2026-10-19), `verify`'s sections were: control
plane, pairs, controller, failure domains, proxies, and the data plane
through the proxy. The inventory declares `coproc VEC.`, and no section asked
it anything. `status` does print a `coproc ... up/DOWN` row, but nothing pages
on `status`.

Ops measured what else could notice. The fleet agent exports
`flint_coproc_up`, and nothing alerts on it. The ops seat supervisor restarts
a dead co-processor within a minute, logs it, and exits 0. So a co-processor
that died once came back unnoticed, and one that kept dying read green
between deaths.

## Why

verify grew a section per seat kind as each kind needed one. Co-processors
came later (ADR-0010, ADR-0017) and were added to `start`, `status` and
`upgrade`, which all ask their FLINTINFO, but never to verify.

## Fix

- **A `== co-processors` section, before `== proxies`,** printed only when
  the inventory declares one. Each `coproc <family> <addr>` is asked
  FLINTINFO over the mesh, as `status` and `upgrade` ask it. A bulk reply is
  `ok co-processor up  VEC. <addr>`. No reply, or any other reply, is
  `FAIL co-processor up  VEC. <addr> does not answer (<why>): every VEC.*
  command fails until it does`, and verify exits 1.
- **It runs in every verify,** including the one each topology-changing verb
  ends with. `upgrade` already waits for a co-processor to answer after its
  swap, and those verifies retry for 30 seconds, so a seat that is still
  binding is waited for, not failed.
- `started_line`'s comment now says ops `supervise.sh` reads the seat name
  too (ops OPS-0406's restart-loop page).

## Evidence

`coproc_family_drill` step 6, on its fleet with `coproc VEC. 127.0.0.1:7402`:
- with flint-vec serving, `verify` passes and prints `ok   co-processor up
  VEC. 127.0.0.1:7402`. This is the control: the same fleet and the same
  verify, with one seat's state changed;
- with flint-vec killed, `verify` exits 1 and prints `FAIL co-processor up
  VEC. 127.0.0.1:7402 does not answer`;
- after `start` brings it back, `verify` passes again.

Before this fix the second arm fails: verify exits 0 with the seat dead.
