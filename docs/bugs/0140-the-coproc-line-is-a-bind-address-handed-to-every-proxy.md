# BUG-0140: the `coproc` line is a bind address and every proxy is handed it as a dial target (FIXED 2026-09-15)

Status: **FIXED 2026-09-15** by refusal, found 2026-09-14 · Severity: **low** — the same shape as
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

## What was done, 2026-09-15: refused, not keyed

Jeff's call on the question this file left open. A wildcard `coproc` address is
now refused at parse time:

```
`coproc VEC. 0.0.0.0:7411` names no machine: this address is handed to every
proxy as a DIAL target, so a wildcard sends each of them to its own loopback.
Give the co-processor's real address
```

**Refused rather than given a `coproc-host`**, because the key would be dead
surface: no generator emits a `coproc` line — `render-inventory.sh` and
`first-boot.sh` both checked — and `chaos-cluster/run.sh:653`, the only writer,
resolves the host itself. A multi-host co-processor topology is ADR-0017 v0.2
work with its own fleet story; if it ever wants the key, the key belongs beside
it rather than three months ahead of it.

**At parse time, not in `coproc_args`**, which is where this file proposed it.
The dial half is the harmful one, and it reaches proxies through `families_arg`
whether or not the local machine spawns a co-processor seat. Refusing where the
inventory is read covers both.

`0.0.0.0`, `[::]` and a bare `:port` are all refused; a real address is
untouched, so `chaos-cluster` is unaffected.

### The drill gained its third field, and that found one more thing

`bind_dial_sites_drill.sh` now covers `coprocs` alongside `cp` and `proxies`.
Listing the field immediately flagged `launch`, which destructured
`&inv.coprocs[i]` directly to build a seat name — something neither of the
other two fields does, because both go through helpers. So `coproc_family` and
`coproc_seat_name` now own that read, and `launch` touches no element.

The exempt set is the four functions that legitimately read the raw string:
`coproc_args` (binds), `families_arg` (dials an address the refusal has already
guaranteed), `coproc_runner` (placement), and `parse_inventory` (the refusal
itself — checking the literal is the job).

**A fourth reader appearing outside that set is the signal that the refusal is
no longer enough and the key is wanted after all.** That is the useful property
of fixing it this way rather than with a `coproc-host`: the next person who
needs multi-host coprocs will be told by a drill rather than by a silent
misroute.

The field has its own positive control — an injected `for c in &inv.coprocs`
must be caught — because a rule with no control has not been shown to fail, and
this one was added while the exempt set was being tuned.

## Not established

- Whether a multi-host coproc topology is intended at all outside
  `chaos-cluster`. If the answer is no, the honest fix is to make
  `coproc_args` refuse a wildcard rather than to add a `coproc-host` nobody
  will write.
