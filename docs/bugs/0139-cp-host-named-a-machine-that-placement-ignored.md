# BUG-0139: `cp-host` named the machine that runs the seat, and placement never read it (FIXED 2026-09-14)

Status: **FIXED 2026-09-14**, found the same day, hours after
[BUG-0138](0138-the-cp-line-is-a-bind-address-and-eight-sites-dial-it.md)
shipped · Severity: **medium** — nothing is live-affected, because no
inventory anywhere declares `cp-host`. What was wrong is the key BUG-0138
added: `self-hosting.md` documents it as *which host runs `cp[i]`*, and the
code that decides where a CP seat runs never looked at it. An operator
following the documentation would have got an incoherent fleet.

## Two defects, one cause

**1. `cp-host` did not move the seat.** Placement went through
`runner_for(inv, &inv.cp[i])`, which derives the machine from the `cp` line —
and for the wildcard that makes `cp-host` necessary in the first place,
`is_local_host("0.0.0.0")` is **true**. So `cp 0.0.0.0:7500` with
`cp-host 10.0.0.9` would spawn the control plane on the orchestrator and tell
every seat to dial `10.0.0.9`. `proxy_runner` had the right shape all along:

```rust
fn proxy_runner(inv: &Inventory, i: usize) -> Runner {
    match inv.proxy_hosts.get(i) {
        Some(h) => runner_for_host(inv, h),
        None => runner_for(inv, &inv.proxies[i]),
    }
}
```

**2. BUG-0138's conversion covered a third of its sites.** It claimed eleven,
and eleven is exactly what a grep for `inv.cp[0].clone()` and `inv.cp.join`
returns. `for seat in &inv.cp` and `inv.cp.iter().enumerate()` match neither,
and behind those live `status`, `status --json`, `verify`, `launch` (both the
PING pass and the wait-for-PONG pass), `upgrade`, `roll_edge`, and the
`--control-plane` list handed to every proxy. Most are dials.

**The cause is the same one BUG-0138 was written about.** That file's own
diagnosis is *a population taken from a convenient syntax rather than from
the authority*, and its fix was scoped by a grep instead of by every use of
the field. Writing the lesson down did not stop me applying the defect to the
fix for it, one hour later.

## What was audited and found clean

The `agent` line was listed as unaudited in BUG-0138. It is audited now and
it does **not** carry this defect: its address is only ever dialled by
flintctl itself (`surface_fresh`), bound by the agent (`--metrics-bind`), or
used to route the agent's own placement — and that routing derives from the
same string, so a wildcard means the agent runs here and a loopback dial
reaches it. There is no path that hands the `agent` address to a different
machine.

## The display, which was the same mistake one layer up

Auditing `status` turned up two rows that show a bind address to a person:

- **the proxy row named an address it never probed.** `proxy_up(inv, i)`
  decides up/DOWN against `proxy_dial(inv, i)`, and the row printed
  `inv.proxies[i]`. Measured on the playground: it reported
  `proxy 0.0.0.0:7379 up` having actually probed `try.crestwayai.com:7379` —
  hiding the tenant-facing name, the one carrying the DNS and the edge cert,
  behind a string that names no machine. This is BUG-0110's own fix stopping
  one line short of the place a human reads.
- **the agent metrics URL** printed `http://0.0.0.0:9464/metrics`, which
  nobody can open.

## The fix

`cp_runner(inv, i)` mirroring `proxy_runner`, used everywhere a CP seat's
machine is decided — `all_runners`, `launch`'s `seat_alive`, and the
stop/spawn pairs in `upgrade` and `roll_edge`. Every dial site resolved
through `cp_dial`. The only raw uses of `inv.cp` left are `cp_seat_args`,
which BINDS, and the two resolvers themselves.

`status`, `status --json` and `verify` now name the address they dialled, the
proxy row prints `proxy_dial`, and the agent URL resolves a wildcard to
loopback — correct because a wildcard `agent` line is exactly the case where
the agent runs on the machine reading the output.

`is_wildcard_host` is extracted and used by both the spawn refusal and the
display, so the two cannot drift about what counts as one.

## Guard

Three tests beyond BUG-0138's five. The one that matters asserts the property
that was violated — **placement and dial move together**: with
`cp-host 10.0.0.9`, `cp_dial` names it and `cp_runner` must be `Ssh`, not
`Local`. Its pair asserts the compatibility case, that a wildcard with no
`cp-host` still resolves `Local`, which is what makes refusing only remote
spawns safe. Mutation-verified: with `cp_runner` reverted to
`runner_for(inv, &inv.cp[i])`, exactly that test fails and the other seven
pass.

## Not established

Both bullets that stood here have since been answered, in the commit that
follows this one. They are kept rather than deleted, because what they said
was true when this was written.

- ~~No test asserts that a future dial site uses `cp_dial`.~~
  **`tools/cp_dial_sites_drill.sh` now does**, as a source assertion beside
  `kill_order`. Only `cp_dial`, `cp_dial_all`, `cp_runner` and
  `cp_seat_args` — the last because it BINDS — may read an element of
  `inv.cp`; collection-level uses are unrestricted. Verified against the tree
  as it stood at BUG-0138, where it names all thirteen sites this bug fixed,
  and carrying a positive control that injects `for seat in &inv.cp` into
  `status()` and requires the scan to catch it.
- ~~Whether any OTHER inventory key is read by one subsystem and ignored by
  another.~~ **Swept, and the sweep is complete for address-bearing keys.**
  `agent` is clean: its address is only dialled by flintctl itself, bound by
  the agent, or used to route the agent's own placement from that same
  string, so no path hands it to a different machine. `controller` is a
  boolean with no address and `controller-host` is honoured by
  `controller_runner`. `backup-to` is a path or `s3://` URL, not an address,
  and `backup-host` is honoured. **`coproc` carries the same shape and is
  filed as [BUG-0140](0140-the-coproc-line-is-a-bind-address-handed-to-every-proxy.md)**
  — bind in `coproc_args`, dial in `families_arg` which hands it to every
  proxy, and no `coproc-host`. It is latent: no generator emits a `coproc`
  line at all, and its only writer resolves the host by hand.
