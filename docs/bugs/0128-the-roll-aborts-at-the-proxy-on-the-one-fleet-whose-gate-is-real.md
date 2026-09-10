# BUG-0128: the roll aborts at the proxy on the one fleet whose gate is real

**Status:** FIXED 2026-09-10 (repo). Found 2026-09-10, before it cost a roll.
Severity high — it makes a live fleet unrollable, and the failure lands after
every seat has already been replaced.

`flintctl upgrade --version-tag` rolls the controller, the control plane, both
pair seats and the proxy, waits for the proxy to serve, and then asks it for
its build. On the playground that question comes back

    -NOAUTH admin token required for this command

so `assert_build` aborts. The fleet has rolled. The command reports failure.
The retry it advises fails identically, so the roll can never be made to
succeed. `packaging/aws/roll-fleet.sh` in the ops repo then adds a second,
worse sentence — `no proxy reports <tag> -- the edge half did not land` —
which names a cause the check has not established (ADR-0028 O4).

Nothing had rolled this fleet since the gate went on, so nothing had found it.
The next roll would have.

## Why the token was missing

Three correct decisions, and the gap is between them.

**ADR-0006 D4 puts the admin token in the control plane.** The CP mints it,
holds it, and pushes DIGESTS to the proxies. `CPADMINTOKEN` hands the
plaintext back over the mTLS CP surface, and its own handler says who for:

> Fleet admin token: current (returned to the mesh-authenticated agent so it
> can present it to proxies) — this port is the mTLS CP surface, so the caller
> is already an operator.

**The operations agent uses exactly that**, and still does: it fetched the
token and kept publishing all 60 proxy-derived series straight through the
gating, which is why nothing looked wrong.

**`admin-token` in the inventory is an optional convenience**, and the ops
renderer deliberately never writes one (OPS-0158): rendering a credential into
a 0644 file out of the boot environment is a decision nobody has made.

`proxystats_field` read only the third of those. On a fleet configured the way
the design intends — token in the CP, nothing in the inventory — flintctl
presented nothing and got refused, while the agent standing beside it read the
same surface without difficulty.

## Why it looked fine until now

The gate was turned on by ops OPS-0163 on 2026-09-08, on a live playground
whose proxy port is open to the internet. That was the right thing to do. It
made a latent gap load-bearing the same day, and the only consumer that would
have noticed is a roll.

`tools/admin_gated_proxy_drill.sh` exists and covers an admin-gated proxy
end to end — bootstrap, `status`, and `upgrade`. Every one of its assertions
ran against an inventory containing

    admin-token seed-admin-token

so flintctl always had one to present. The drill certified the gated path in
the one configuration the live fleet does not have. That is the same shape as
the bug it was written for: coverage that exercises the mechanism and not the
deployment.

## The fix

`admin_token(inv)` — inventory first, then the CP — and the three proxy-facing
call sites use it: `proxystats_field`, the `PROXYCACHE` push in `apply`, and
the `proxy-cache` verb. `cp_args` deliberately does NOT, because that one is
about what the control plane is STARTED with; seeding from a token fetched
from the thing being seeded is circular.

flintctl is entitled to the token by the agent's argument and more strongly:
it already holds `{statedir}/certs/int.key`, root-only, which is the mesh
identity the CP authenticates. A fleet that trusts flintctl to spawn its seats
is not protected by withholding a secret it can mint a replacement for with
`rotate-admin`. Nothing new is written to disk and no operator has to handle a
credential.

Three outcomes, kept apart (ADR-0028 O4):

| outcome | meaning | what the caller does |
|---|---|---|
| `Ok(Some)` | a token to present | `AUTH` then the command |
| `Ok(None)` | this fleet has none | present nothing — right for every drill fleet and every ungated deployment |
| `Err` | the CP could not be asked | NOT "there is no token"; the read is still attempted, and if the proxy then refuses, the message carries both halves |

That last row is the reason this returns a `Result<Option<_>>` rather than an
`Option<_>`. Collapsing "could not ask" into "there is none" would present
nothing on a gated fleet and then blame the proxy for a lookup that never
happened — OPS-0037's rule, one repository over.

**Successes are memoised per fleet; failures are not.** `upgrade` stops and
restarts the control plane before it reaches the proxies, so a cached failure
from the moment the CP was legitimately between processes would abort the roll
at the last seat: this defect again, by a different route. The price is a
repeated lookup on a fleet whose CP is down — nothing for a single-seat CP,
which returns on the first connection error, and the full 24-attempt budget
for a Raft CP, which rotates.

## Verification

`tools/admin_gated_proxy_drill.sh` gains a second phase on the same fleet:
strip the `admin-token` line from the inventory, then read and roll again.

Two positive controls, because without them the phase proves nothing:

- **the proxy is still gated.** Removing the inventory line must not ungate
  it, and does not — the CP seeds from the flag only when its own state holds
  nothing (`flint-controlplane/src/main.rs:1546`) and this CP committed the
  token on first boot. An ungated proxy would let every assertion below pass
  while exercising none of them.
- **the CP still holds the token**, compared rather than printed.

Then `status` must read the proxy's build, and `upgrade --version-tag` must
complete and leave the proxy reporting the new tag. On the shipped binary the
first prints `build <unreadable: could not read PROXYSTATS: NOAUTH …>` and the
second aborts after rolling everything.

Three unit tests cover what happens before any dial: an inventory token
short-circuits (proved by pointing `cp` at a closed port — a lookup would
error, so `Ok(Some)` can only mean it never dialled), an unreachable CP is an
`Err` naming the command, and `parse_inventory` refuses an inventory with no
`cp` line, which is what lets `call_cp`'s `inv.cp[0]` stand without a guard.

The remaining 139 drills are the coverage for the `Ok(None)` path: they all
declare a `cp` and no `admin-token`, so every one of them now makes the lookup
and must get "this fleet has none".

## Related

- ops OPS-0163 — turned the gate on, and its still-open half is that a refused
  operator call increments no counter and writes no log line
- ops OPS-0158 — `admin-token` is deliberately unrendered
- BUG-0083 — made this failure sayable; the message that named `-NOAUTH` is
  how the cause was readable at all
- ADR-0006 D4 — where the token lives and how it reaches a proxy
