# BUG-0205: a co-processor `flintctl upgrade` starts is never routed, because only bootstrap told the control plane its family (FIXED 2026-10-05)

**Status:** **FIXED 2026-10-05.** Found by rehearsing ops ADR-0050 step 4 on a
gate box, which adds the vector co-processor to a running fleet the way the
runbook does. Held by `edge_roll_drill`, which now sends `VEC.*` through
the edge as a tenant after each roll.
**Severity:** medium. ADR-0050 D4 (`b2b9f17`, same day) made `flintctl
upgrade` start a co-processor the inventory gained. The seat started, but
no tenant could reach it: every `VEC.*` answered `ERR unknown command`.
Nothing was lost or misrouted. This is the only way to turn vector search
on for a fleet that already runs, and step 4 does exactly that on the
playground.

## What was measured

A disposable TLS fleet on a gate box (c7i.xlarge), bootstrapped with no
`coproc` line. Then the runbook's three lines, and `flintctl upgrade
--version-tag step4-rehearsal`:

- the upgrade log: `vec-7984 reports step4-rehearsal`, `proxy-7989 rolled
  and serving`;
- `status`: every seat on `step4-rehearsal`;
- the live proxy's argv: `--families VEC.=127.0.0.1:7984`;
- `valkey-cli -p 7989 --tls ... VEC.CREATE diag DIM 3 METRIC l2`:
  `ERR unknown command 'VEC.CREATE', with args beginning with: 'diag' ...`.

## Why

A proxy's family route table has two sources: its `--families` argument, and
element 7 of the control plane's `CPSNAPSHOT`. The proxy treats the
snapshot's element as authoritative whenever it is present, and present but
empty clears the table (ADR-0010 D1). `flintctl` wrote families to the
control plane (`CPFAMILY`) in one place, bootstrap's register block. Its
comment already said why: "a static flag alone survives exactly until the
first snapshot lands".

D4 gave the upgrade a co-processor loop and passed the rolled proxies the
new `--families`. The control plane's table stayed empty, and the first
snapshot after each proxy started erased the flag.

`edge_roll_drill`'s D4 section asserted the proxy's argv held
`VEC.=127.0.0.1:7974`, which was true. It never sent a `VEC.*` command.

## The fix

`roll_edge` makes the control plane's family table match the inventory
before the proxies roll (`sync_families`): `CPFAMILY` for each declared
family the table lacks or holds with other endpoints, and `CPFAMILYCLEAR`
for each the inventory no longer declares. The upgrade log says what
changed (`co-processor families on the control plane: VEC.=...`, or
`unchanged`). A failure to read or write the table aborts the upgrade before
the proxies roll.

Clearing matters for turning vectors off. Remove the `coproc` lines and
roll, and no proxy keeps routing `VEC.*` to a co-processor that is gone. The
undeclared co-processor process is not the upgrade's to stop; the operator
stops it.

## The drill

`edge_roll_drill`, under mesh and client TLS:
- after the first upgrade, the log shows `VEC.` registered, and a tenant's
  `VEC.CREATE`, `VEC.SET` and `VEC.SEARCH` answer through the edge;
- after the second, the set is still searched (rebuilt by the new
  co-processor);
- a third upgrade, with the `coproc` line removed, logs `VEC. cleared`. With
  the old co-processor stopped, `VEC.*` answers `unknown command` again.

On the unfixed `flintctl`, the first `VEC.CREATE` answers `unknown command`.
