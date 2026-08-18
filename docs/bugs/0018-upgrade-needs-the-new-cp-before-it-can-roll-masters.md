# BUG-0018: `upgrade` needs the NEW control plane to roll masters, and rolls the CP after them (OPEN)

Status: OPEN, found 2026-08-18 rolling the playground rc.51 → rc.52 ·
Severity: medium-high — the documented one-command rollout cannot complete on
any fleet whose CP predates `CPFENCE`, and it stops halfway with the pair
demoted

## Symptom

    == upgrade: canary 172.31.64.94:7001 first, soak 3000ms, then 0 more replica(s), masters last
      canary 172.31.64.94:7001 on new build, reconverged; soaking 3000ms
      soak clean: no unexpected transitions in the fleet journal
    == masters last (fenced controlled failover per pair)
    flintctl: CPFENCE 172.31.64.94:7001 did not commit — refusing to promote
    without a fencing record (the pair is demoted and drained; re-run once the
    CP is reachable, or the controller will finish the handoff):
    ERR unknown control-plane command

The CP was reachable. It did not understand the command: rc.52's `flintctl`
issues `CPFENCE`, and the rc.51 control plane has no such verb.

## Root cause — an ordering inversion

`upgrade` runs: canary replica → remaining replicas → **masters** (fenced
controlled failover) → `roll_edge`, which is what rolls the proxy, control
plane, controller and agent. So the master roll depends on a CP command that
only exists after `roll_edge`, which runs later. The sequence is unsatisfiable
in one pass whenever the CP is older than `flintctl`, which is every upgrade
that crosses the release that introduced `CPFENCE`.

**The refusal itself is correct** and worth keeping: declining to promote
without a fencing record is exactly right, and the pair was left serving on its
existing master rather than headless. The defect is the order, not the guard.

## Fix

Roll the control plane BEFORE masters. The CP is a dependency of the master
handoff, so it belongs with — or ahead of — the seats that need it, not in the
edge sweep at the end. Options:

- move the CP out of `roll_edge` into its own phase before `masters last`; or
- have the master roll detect an old CP up front and either roll it first or
  refuse EARLY with that reason, rather than after demoting the pair.

Either way the failure should be detected before anything is demoted. A
precondition check ("does the CP speak the verbs this roll needs?") costs one
round trip and would have turned a half-completed roll into a clean refusal.

## Workaround, verified

Roll the CP by hand, then re-run. The exposure is bounded by
`DEFAULT_LEASE_TTL_MS` (5000 ms): a CP absent longer than the lease self-fences
the master, and per BUG-0012's incident nothing then re-promotes. Measured on
the playground, a stop-and-restart of the CP with its own argv completed in
**90 ms**, and the master never lost its lease. The re-run then completed the
whole fleet.

## Secondary: the release notes contradict the CLI

`packaging/aws/release-box/run.sh` writes release notes instructing:

    flintctl -f <inventory> upgrade --manifest manifest.json --version-tag <tag>

rc.52's `flintctl --help` documents only `upgrade --version-tag <tag>`; there is
no `--manifest` in the summary. Anyone following the published notes reaches for
a flag the binary may not accept. One of the two is wrong and they should be
generated from the same place.

## Related

- #105 — `upgrade` must roll the whole fleet, not just pair nodes; this is the
  ordering half of the same concern
- #148 — HA control plane: CPINFO parity + roll every seat
- BUG-0012 — why the CP-absence window matters, and what happens if it is
  exceeded
