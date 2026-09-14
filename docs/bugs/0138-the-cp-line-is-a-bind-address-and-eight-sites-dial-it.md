# BUG-0138: the `cp` line is a BIND address and eight sites hand it to a spawned process as a DIAL target (FIXED 2026-09-14)

Status: **FIXED 2026-09-14**, found the same day · Severity: **high** — the address a seat
is told to reach the control plane at is also the address the control plane
binds, and the two are the same string only when the CP shares that seat's
host. Every process spawned from a wildcard inventory carries `0.0.0.0:7500`
as its `--journal`, `--lease-cp`, `--control-plane` and `--commit-cp`. With a
co-located CP that resolves to the CP and nothing is wrong; give the CP its
own machine and each seat dials **its own** loopback for a control plane that
is not there. ADR-0018 makes `--lease-cp` the write lease's renewal path, so
this is a fencing target, not a telemetry one.

**This is [BUG-0110](0110-the-self-hosting-quickstart-cannot-work-as-written.md)
one inventory key over.** That bug was the `proxy` line, it cost the first
7-host run, and the fix was `proxy_dial()` plus `proxy-host` /
`proxy-advertise` to resolve it. The `cp` line got neither. `proxy_dial`'s own
doc comment — *"A bind address is not a destination"* — sits 2000 lines above
eight call sites that pass one as a destination.

## The pair on the playground is split right now

Measured with `ps` on the playground box (both seats v0.1.0-rc.72, same pair,
2026-09-14):

    node-7001  --journal 0.0.0.0:7500        --lease-cp 0.0.0.0:7500
    node-7002  --journal 172.31.64.94:7500   --lease-cp 172.31.64.94:7500

Two members of one pair renewing the same write lease against two different
spellings of the CP. Both work here because the CP is on that box, so this is
a latent split and not an outage — but it is the fleet's real state, not a
hypothetical, and nothing reports it.

## Where the two spellings come from

**Two inventories describe the same fleet and disagree.** Neither is wrong on
its own terms, which is why neither has been corrected:

    playground  /opt/flint/cluster.flint            cp 0.0.0.0:7500
    flint-ops-b /opt/flint-phase1/playground.flint  cp 172.31.64.94:7500

The local one is a **bind** directive and `0.0.0.0` is the correct thing to
bind. The remote one is a **dial** directive and a wildcard names no machine,
so whoever wrote it had to put the real address in. One key, both roles, and
the inventory format cannot express the difference.

The ops repo already says this in prose, in `packaging/aws/first-boot.sh`:

> flintctl binds the address the inventory names — "the difference between a
> control plane and an unreachable process", as `cp_args` puts it. Both ops
> boxes dial that CP at the box's private address.

Bind here, dial there, stated plainly, never separated in code.

## Why it has never fired

Because the multi-host harness resolves it by hand.
`packaging/aws/chaos-cluster/run.sh:510` emits

    echo "cp ${host}:${port}"

— a real host, always. So the 5-host and 7-host chaos runs, the ones built
precisely to catch what loopback hides, could not reach this: they never wrote
the wildcard form. The wildcard form is what the **single-host** generator
writes, which is the shipped quickstart path, so the defect sits exactly where
a fleet grows from one host to two.

## The occurrence that exposed it

2026-09-14 17:28:11–13Z, one fault on playground `node-7002`, repaired
**twice by two actors one second apart** — which is how the split became
visible:

    17:28:11.394  flintctl start node-7002  --journal 0.0.0.0:7500       (local flint-supervise.timer)
    17:28:11      host-stop-seat   node-7002                             (flint-ops-b, over ssh)
    17:28:12      host-mark-reseed node-7002
    17:28:12.442  host-spawn       node-7002 --journal 172.31.64.94:7500 (flint-ops-b, over ssh)
    17:28:13      supervise: RESTARTED 1 seat(s) that had stopped serving

The local timer used the bind-form inventory; flint-ops-b's agent Tier-2
`restart-node` used the resolved-form one and arrived over three separate
inbound ssh sessions from `172.31.14.109`. The agent's spawn is the copy that
survived, which is why node-7002 holds the resolved address and node-7001,
untouched since the rc.72 roll, still holds the wildcard.

This also explains `MemberRejoins=2` for a single fault: the metric is honest,
there were two rejoins. **The unguarded race between the local supervise timer
and the remote agent is a separate defect**, filed as OPS-0250, not here.

## Mechanism

`Inventory.cp` is read verbatim at eight sites that compose a spawned
process's command line, across four flags:

    2717  node_tuning_args  --lease-cp       inv.cp.join(",")
    3323  spawn, pair node  --journal        inv.cp[0]
    3616  agent             --control-plane  inv.cp[0]
    3691  controller        --journal        inv.cp[0]
    3696  controller        --commit-cp      inv.cp[0]
    5480  add_replica       --journal        inv.cp[0]
    5569  swap_node         --journal        inv.cp[0]
    6201  roll_node         --journal        inv.cp[0]

`is_local_host` already returns `true` for `0.0.0.0`, so runner routing sends
such a seat to whichever host its **own** address names, and `host-spawn`
carries the wildcard target there unchanged. Nothing between the inventory and
the remote process's argv examines it.

Three further uses dial the CP from inside flintctl itself — `admin_token`'s
memo key (551), `cp_call`'s retry target (3429), and the rollback record's
`cp` field (6767). Those are correct exactly when the orchestrator is
co-located with the CP, which is the same assumption in a different guise.

## The fix

`cp_dial(inv, i)` beside `proxy_dial`, and a `cp-host` inventory key
positional with the `cp` lines — the same shape the proxy line already has.
The **port** still comes from the `cp` line, so the bind port and the dial
port cannot drift apart. With no `cp-host` the line is used as written, which
is byte for byte what shipped before: every fleet that exists today is
co-located and composes exactly what it did.

All eleven uses of `inv.cp` as a destination now go through it — the eight
argv sites above, and the three that dial from inside flintctl
(`admin_token`'s memo key, `call_cp`'s retry target, `Roll::new`'s record).
The three matter because declaring `cp-host` is what makes them wrong: an
orchestrator that is not the CP's machine would otherwise dial the bind
address itself. `call_cp` rotates by index over its seat list and matches the
current target back against it, so the list it rotates within is now the
dialled forms, not the raw lines.

## Guard

Resolution alone fixes nothing for an operator who does not know the key
exists, so the wildcard is also **refused** rather than shipped.
`spawn_env`'s remote branch is the one choke point every remote spawn passes
through; `wildcard_cp_target` scans the composed argv for any of the four CP
flags carrying an address that names no machine, and `refuse_wildcard_cp_target`
dies naming the key that fixes it.

Only remote spawns are checked, and that is not a shortcut: a wildcard `cp`
makes `is_local_host` true, so the CP runs on the orchestrator and a LOCAL
seat dialling its own loopback reaches it correctly. The wildcard is wrong
exactly when it leaves the box.

The predicate is split from the refusal so it can be tested without exiting
the process. Five tests: the verbatim compatibility case, host substitution
with the port kept, positional-and-partial `cp-host` over three seats, the
catch across all four flags including one hidden mid-list in `--lease-cp`,
and — the half that makes the rest mean anything — a false-positive set that
must pass, containing the agent's FILE `--journal`, a bare `--bind 0.0.0.0`
(a genuine bind, which belongs on the wildcard), `--metrics-bind 0.0.0.0:9464`,
and a trailing flag with no value. Mutation-verified: with the wildcard test
changed to one that never matches, `a_wildcard_cp_target_is_caught_on_every_flag`
fails and the other four still pass.

**Not fixed here:** the playground's own split pair. node-7001 keeps
`0.0.0.0:7500` until it is next restarted, and correcting it means either
adding `cp-host 172.31.64.94` to `/opt/flint/cluster.flint` or restarting the
seat — a live-fleet change, and Jeff's call, not something this commit does.

## Not established

- **Not reproduced on a real two-host fleet.** The multi-host consequence is
  read off the address semantics and off BUG-0110's recorded history for the
  sibling key, not measured. What IS measured is the split pair, the two
  disagreeing inventories, and the eight call sites.
- Whether a seat whose `--journal` is unreachable degrades or refuses. The
  guard now makes that unreachable by construction on the spawn path, so it
  matters only for an inventory hand-edited after bootstrap — worth knowing,
  not blocking.
- Whether any OTHER inventory key carries the same bind/dial overload.
  `proxy` and `cp` are now both resolved. The `agent` line is the obvious next
  candidate — the playground binds it `0.0.0.0:9464` and it has eight uses in
  `flintctl` — but it was NOT audited here, so nothing above says it is clean.
