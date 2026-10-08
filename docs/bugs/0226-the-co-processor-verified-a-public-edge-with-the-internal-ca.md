# BUG-0226: the vector co-processor verified a publicly issued edge with the internal CA (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by `edge_ca_trust_drill`, which now
adds a co-processor to its foreign-CA fleet and requires a vector set to be
created, written and searched through the edge. With flintctl's half
reverted, it fails exactly as the playground did.
**Severity:** high for vectors on any real deployment: on a fleet whose edge
certificate is not the fleet CA's, no vector set could ever serve.

## What happened

Found starting ops ADR-0050 step 4 on the playground, 2026-10-08, right after
the upgrade that added its co-processor (v0.1.0-rc.81). A namespace's first
`VEC.*` command rebuilds its index from durable rows over a PROXYCHAN
dial-back to the proxy's edge. `flint-vec --client-tls` verified that edge
against `--internal-ca`. The playground's edge certificate is publicly
issued (its inventory says `edge-trust /etc/pki/tls/certs/ca-bundle.crt`),
so every dial-back failed:

```
flint-vec: ns "step4" rebuild chunk failed: channel open: invalid peer certificate: UnknownIssuer (will retry on next touch; index not served partial)
```

and every vector command answered `LOADING vector index is warming, retry`,
for ever. Nothing was written; the measurement's tenant stored nothing.

flintctl already knew the hazard. `edge_trust_path` exists because "every
component that dials the edge must be told SEPARATELY which CA to trust",
after five bugs in one day (2026-08-10) that were each that fact. The
co-processor was one more consumer it was never given to. No drill ran a
co-processor behind an edge that the fleet CA had not signed:
`edge_roll_drill` and `coproc_vec_tls_drill` let `bootstrap` mint the edge
certificate, so both halves agreed by construction.

## The fix

- `flint-vec` takes `--edge-ca`, the bundle the edge certificate chains to,
  and falls back to the internal CA without it, so an older flintctl keeps
  its behaviour.
- flintctl passes `edge_trust_path`: the inventory's `edge-trust` when
  declared, otherwise the internal CA.

A running co-processor keeps the trust it started with; the next `upgrade`
restarts it with the new argument. `start` leaves a live seat alone and
refuses one whose arguments differ (BUG-0138), as for any seat.
