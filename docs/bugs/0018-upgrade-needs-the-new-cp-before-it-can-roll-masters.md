# BUG-0018: `upgrade` needs the NEW control plane to roll masters, and rolls the CP after them (FIXED)

Status: FIXED 2026-08-18, found the same day rolling the playground rc.51 →
rc.52 · Severity: medium-high — the documented one-command rollout could not
complete on any fleet whose CP predates `CPFENCE`, and it stopped halfway with
the pair demoted

Fixed by `3206013` (detect the old CP and roll it first), `bbdb563` (pin the
decision), `5131dd2` (pin the strings the decision reads). **Resolution**
below also records a second defect found while fixing this one, and a review
note that changed the shape of the fix.

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

## Second defect: the refusal's advice cannot work

Filed as part of the ordering bug, but it is separate and arguably worse. The
message says:

    the pair is demoted and drained; re-run once the CP is reachable, or the
    controller will finish the handoff

That describes a **transient outage**. This is **version skew**: the CP *is*
reachable, so a re-run fails identically, forever, and the controller — also
on the old build — cannot issue `CPFENCE` either. Nothing in the text tells
the operator that retrying is hopeless.

The ordering bug stops you. This one keeps you going. The text is unchanged
by the fix, because after the fix the situation it describes can no longer be
reached from `upgrade`; if that refusal is ever reworded, it should
distinguish "the CP did not answer" from "the CP does not know the verb".

## Resolution

Detect before mutating, and roll rather than refuse.

`upgrade` now probes the control plane with a **bare `CPFENCE`** before it
touches anything. That probe is side-effect free by construction: the
handler's arity check returns `ERR CPFENCE <addr>` and returns *before* it
proposes a Raft mutation, while a CP without the verb answers `ERR unknown
control-plane command`. Probing with a real address would commit a fencing
record as a side effect of asking a question.

**Refusing early was the other option in the Fix section above, and it was
withdrawn on inspection.** There is no CP-only roll verb to send an operator
to — `--nodes-only` does the opposite, leaving the CP on the old binary — so
a refusal would have named a remedy the tool does not offer. A refusal
pointing at a flag that makes things worse is a trap, not a guard. So on a
miss `upgrade` rolls the CP itself, using the same stop/spawn/wait primitives
as the verified manual workaround, then **re-probes** and aborts if the
*staged* binary is also too old — otherwise it would walk into the same
half-roll with the pairs untouched.

Additive rather than a reorder: `roll_edge` still rolls the CP at the end, so
a crossing upgrade restarts it twice. At the measured 90 ms per restart
against a 5000 ms lease TTL that is ~180 ms of CP absence, nowhere near the
self-fence window — cheaper than reordering a sequence whose current order
`roll_edge`'s own header reasons about carefully.

### Why the classification is pinned on both sides

The probe distinguishes the two cases by **error text**, which makes two
message literals load-bearing. Capability-sniffing beats a hardcoded
"`CPFENCE` landed in rc.N" gate, but a reword of either CP-side string would
silently reclassify every fleet — and the dangerous direction is the
non-obvious one: a *current* CP misread as old gets its control plane rolled
on every upgrade of every healthy fleet, because the probe fires before the
pairs are touched.

`flint-ctl` cannot import the strings (it does not depend on
`flint-controlplane`, and adding that would drag the Raft stack into the
operator tool), so they are named constants in `ha.rs` pinned by a test in
the crate where an edit would be made. A second test asserts the two stay
distinguishable by the substring `flintctl` actually matches on — a reword
making the arity error *also* contain "unknown control-plane command" would
satisfy equality checks while inverting the classification.

### Verification, and what is still unproven

- `tools/upgrade_drill.sh` — PASS. Fleet rolls, every seat reports the
  operator-chosen build, `HELLO` agrees, data survives the warm restart.
- `tools/cpha_roll_drill.sh` — PASS. Three-seat Raft CP rolls to completion,
  all three report the new build, controller registers on the HA path.
- Unit tests pin all four replies the probe must discriminate, plus both CP
  strings, each with a mutation control.

Those cover **the regression**, which is the half that threatens every fleet:
a CP that already speaks `CPFENCE` must not see the new phase at all.

**The crossing branch has never executed.** Reproducing it needs a
control-plane binary older than `CPFENCE` — the problem
`build_mismatch_drill` solves with an isolated stamped build. Until someone
does that, the remediation path is reasoned about and unit-tested, not run.
Anyone relying on it against a real pre-`CPFENCE` fleet should build a
stamped old CP first.

## Secondary: the release notes do NOT contradict the CLI — the help is incomplete

`packaging/aws/release-box/run.sh` writes release notes instructing:

    flintctl -f <inventory> upgrade --manifest manifest.json --version-tag <tag>

rc.52's `flintctl --help` documents only `upgrade --version-tag <tag>`; there is
no `--manifest` in the summary.

**Checked, and the notes are right — the risk points the other way.**
`--manifest` is a real flag, parsed in `crates/flint-ctl/src/main.rs` inside
the `"upgrade"` arm, and it implements the **format-break guard**: a manifest
declaring `format_break=true` refuses the canary fast path unless the operator
passes `--allow-format-break`, because that release cannot roll back and must
ship via the migration runbook.

So an operator who follows `--help` instead of the release notes does not hit
an unaccepted flag — they **silently skip a safety guard**. The defect is that
`--help` under-documents a real flag, not that the notes are wrong. Still worth
generating both from one place; still OPEN, and it lives in `flintctl`'s help
text rather than in `release-box/run.sh`.

## Related

- #105 — `upgrade` must roll the whole fleet, not just pair nodes; this is the
  ordering half of the same concern
- #148 — HA control plane: CPINFO parity + roll every seat
- BUG-0012 — why the CP-absence window matters, and what happens if it is
  exceeded
