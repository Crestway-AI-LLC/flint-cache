# BUG-0148 — a tenant-facing command exists only on the single-node control plane

**Status:** **FIXED 2026-09-15** — found while sizing BUG-0146, fixed in the
same change.

## The gap

The two control-plane dispatchers handled 43 and 42 verbs. The difference was
one verb, and it was a tenant-facing one:

```
main.rs (single-node)  43 arms
ha.rs   (raft)         42 arms
only in main.rs: CPMYSTATUS
```

`ha.rs`'s dispatch ends at `_ => Value::Error(ERR_UNKNOWN_CP_COMMAND)`, and
`--raft` enters through an entirely separate path (`run_raft` →
`ha::run_client`) that never reaches `main.rs`'s dispatch. So on a replicated
control plane, `CPMYSTATUS` — ADR-0014 D3, the one command a tenant has to ask
about itself — returned *unknown command*.

`CPMYUSAGE` and `CPMYCONFIG`, its two siblings from the same ADR, were present
in both. Nothing in ADR-0014 or in either file says D3 is single-node only, so
this was a missed arm rather than a decision. `CPADDTENANT` was compared arm
to arm before concluding that: those two agree, `[k]` default included.

## Why nothing caught it

`tools/tenant_status_drill.sh` covers D3 thoroughly — both directions of the
cross-tenant grep, an invalid token, the operator-topology check — and it
starts **one** control plane, without `--raft`. Every assertion it makes is
true. It is a complete test of half the product.

This is BUG-0146's prediction arriving on schedule. That write-up says *"a unit
test against `RegistryState` proves nothing about a single-node fleet"*; the
mirror image is just as true, and this is it. Two dispatchers, and each check
only ever meets one.

## The fix

**Not** a hand-ported second copy of the body — that closes this gap and opens
BUG-0146's, which is the more expensive one. `tenant::my_status_body` formats
it once and both arms call it, with `build` passed in the way
`refill_after_retire` takes its shuffle: the caller owns what genuinely differs
between the paths, the shared function owns what must not differ at all. Two
unit tests cover the body, including that the token digest — which is on the
struct the formatter is handed — does not appear in it.

`assert_cp_verbs_agree_across_paths` in `tools/gates.sh` compares the two
dispatchers' match arms. Its positive control is this bug: run against the
pre-fix tree it prints `single-node only  CPMYSTATUS` and `COVERAGE 43 42`;
against the fixed tree, `COVERAGE 43 43` and nothing else. It fails closed on
an unreadable file and on a match that finds no arms at all.

**What that check cannot do, said plainly because the number next door is the
reason.** It compares verb TABLES, not behaviour. BUG-0146 is a verb present in
both arms where a fix landed in one — six unit tests green, the product
unchanged — and no textual check reaches that. This catches the arm somebody
forgot to ADD. Nothing here catches the arm somebody forgot to UPDATE.

## Verified where it matters

`controlplane_ha_drill.sh` — the drill that actually runs three Raft nodes —
now asserts `CPMYSTATUS` answers with this tenant's own fields, and that an
invalid token still gets `WRONGPASS`. Asserted against a **follower**, because
a verb missing from the raft dispatcher is missing from every node and asking
the leader would leave that untested; placed after the drill's convergence
proof, because a follower that has not yet applied the `AddTenant` answers
`WRONGPASS` for a reason that is a race in the drill rather than a fact about
the product.

The unit test proves the body. The drill proves the arm is reachable on the
control plane every HA deployment runs.
