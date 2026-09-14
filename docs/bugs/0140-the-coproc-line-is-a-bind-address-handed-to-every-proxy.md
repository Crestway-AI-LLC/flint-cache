# BUG-0140: the `coproc` line is a bind address and every proxy is handed it as a dial target (OPEN)

Status: **OPEN**, found 2026-09-14 · Severity: **low** — the same shape as
[BUG-0138](0138-the-cp-line-is-a-bind-address-and-eight-sites-dial-it.md),
one key over again, but **latent**: nothing generates the wildcard form, so
no fleet that exists today can be in this state. Filed because the pattern is
now known and a hand-written inventory reaches it.

## The shape

`coproc <family> <addr>` is used as **both** roles, in two functions:

- **BIND** — `coproc_args` derives the seat's listen address from it:

      let bind_host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or("127.0.0.1");

- **DIAL** — `families_arg` builds the `--families` routing table
  (`VEC.=addr1,addr2;OTHER.=addr3`) from the same string, and **every proxy
  is given it**. A proxy on another machine handed `VEC.=0.0.0.0:7411` dials
  its own loopback for a co-processor that is not there.

There is no `coproc-host`, so `coproc_runner` derives placement from the
address too — `runner_for(inv, &inv.coprocs[i].1)` — which for a wildcard
resolves LOCAL, exactly as the CP did before
[BUG-0139](0139-cp-host-named-a-machine-that-placement-ignored.md).

## Why it is latent, measured rather than assumed

**No generator emits a `coproc` line.** `packaging/aws/render-inventory.sh`
contains no `coproc` at all, and neither does `first-boot.sh` — checked, not
inferred. The ops key table declares it `harness`-owned:

> `coproc` · harness · the vector co-processor seats;
> `packaging/aws/chaos-cluster/run.sh` writes its own multi-host inventory.

And that writer resolves the host by hand, at `chaos-cluster/run.sh:653`:

    echo "coproc VEC. ${COPROC_HOST}:7411"

which is the same reason BUG-0138 never fired: the multi-host harness, the
one topology that would expose it, never writes the wildcard form.

## What it would take to fix

The pattern is established and this is a mechanical application of it:
`coproc_dial(inv, i)` and a `coproc-host` key, `coproc_runner` honouring it,
`families_arg` resolving through it, `coproc_args` keeping the literal
because it binds. `tools/bind_dial_sites_drill.sh` extends to it directly: a third field with
its own exempt set.

**Not done here on purpose.** The vector co-processor is ADR-0017 v0.2 work
with its own fleet story, and changing how proxies are told to reach it is
worth doing beside that rather than as a fourth item in a bind/dial sweep.
Nothing is at risk in the meantime.

## Not established

- Whether a multi-host coproc topology is intended at all outside
  `chaos-cluster`. If the answer is no, the honest fix is to make
  `coproc_args` refuse a wildcard rather than to add a `coproc-host` nobody
  will write.
