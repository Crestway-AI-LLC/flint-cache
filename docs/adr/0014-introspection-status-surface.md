# ADR-0014: One status surface — every seat says what it is running

Status: **Accepted and implemented in full — D1, D2, D3 — August 2026.**
Filed originally because the evidence was gathered during launch prep and
would otherwise be lost, and because one part of it (D1) is a gap in the
upgrade path rather than a feature request — which is why it was brought
forward ahead of the flip instead of waiting.

D1 as built: `flint_build` in the proxy, control plane and controller, each
with `--build-version`; `build:` in `PROXYSTATS` and `CPINFO`;
`registry_version:` with `version:` retained as an alias; the controller
registering via a new `CPCONTROLLER <host:pid> <build>` every 30s with a
90s staleness window; `flintctl status` rendering all five seat kinds; and
`roll_edge` asserting the observed build for the control plane, each proxy,
and the controller. Covered by `tools/build_stamp_drill.sh` (core gate).

One deliberate gap, recorded rather than papered over: the drill cannot
prove the MISMATCH abort, because `upgrade --version-tag T` exports
`FLINT_BUILD_VERSION=T` to the seats it spawns and an unbaked dev binary
reports the environment — the assertion would read back the value it just
set. That is the self-fulfilling check `flint_build` already documents, and
the reason a baked `FLINT_RELEASE_TAG` wins over the environment. Proving
it needs two release builds with different baked tags.

D2 as built: `flintctl status --json` renders the whole fleet — every
seat's build, each node's full `FLINTINFO`, the controller rows the CP is
repeating back — and carries a `drift` array comparing `lag_soft_ms`,
`lag_hard_ms`, `min_replicas_to_write`, `widowed_grace_ms`, `fullsync_max`,
`wal_fsync_ms` and `max_conns` between the members of each pair. State is
deliberately excluded from that comparison: role and lag differ between a
master and its replica by definition, and flagging them would be red on
every healthy fleet.

The cry-wolf problem the Consequences section anticipated needed one thing
that did not exist: `FLINTINFO` had no uptime. `heat::uptime_ms` was the
wrong clock — it measures time since the first OP, so an idle seat reports
zero and would have looked permanently young, suppressing real drift
forever. A separate process clock (`uptime_ms:`) was added, and drift is
held back while either member is younger than `ROLL_GRACE_MS` (120s,
`FLINT_ROLL_GRACE_MS` for drills). Covered by
`tools/config_drift_drill.sh` (core gate), which exercises both sides of
that boundary and asserts a healthy pair reports nothing.

`--json` exits 0 with drift present: D2 REPORTS, it does not reconcile. The
array is always emitted, empty when clean, so a caller can gate on it.

D3 as built: `CPMYSTATUS <token>` on the control plane, behind the same
token-digest lookup `CPMYUSAGE` already uses — no second authentication
path, so the scoping is the one already in production rather than a new
implementation of it. Returns the tenant's name, namespace, its own proxy
endpoint, quota (ops/sec and bytes) with current usage, `over_quota`, the
three feature flags plus `federated`, and the service build.

The tenant's own endpoint is included and node addresses are not: they
already dial that endpoint and already receive it via `CPSNAPSHOT`, while
the pair layout is not theirs to have. Covered by
`tools/tenant_status_drill.sh` (core gate), which follows the verification
this ADR specified — two tenants provisioned with deliberately distinctive
names, and the isolation asserted by grepping both responses in both
directions rather than by reading the code. The mirror direction is the
positive control: three greps that find nothing prove nothing unless the
same greps can find something.

> Numbering: 0005–0009 are private-plane records and 0010 is reserved for the
> co-processor extension model. See [README](README.md) for why the sequence
> is shared across two repositories.

## Context

Flint reports a lot about itself, and the per-node view is genuinely strong.
`FLINTINFO` on a data node returns the build stamp, every operational knob
(lag soft/hard caps, `min_replicas`, widowed grace, `wal_fsync_ms`,
`max_conns`), the disk verdict with free/total/percentage, cert days
remaining, GC counters, async queue depth and write-stall counts — and it
distinguishes a knob that is SET from a knob that is currently BITING
(`widowed_beyond_grace`), which is the distinction that actually explains a
`-THROTTLED` to someone reading it during an incident.

That strength made the gaps hard to see. Checked against the code and
against the running playground rather than memory:

- **Only three crates use `flint_build`**: `flint-server`, `flint-ctl` and
  `flint-backup`. The **proxy, control plane and controller carry no build
  stamp at all.** This is not a rendering gap in `flintctl status` — there
  is nothing for it to ask. `status` prints `proxy 0.0.0.0:7379 up` and
  `cp 127.0.0.1:7500 up` beside pair nodes that report
  `build v0.1.0-rc.33`.

- **That is a hole in the upgrade path, not a cosmetic one.** #105 made
  `flintctl upgrade` roll the whole fleet rather than only pair nodes. So
  the edge and control plane are now rolled by a mechanism that cannot
  verify what they landed on, and `roll_edge` reasons out loud about "the
  data plane is already on the new build" — the one tier it can see. A
  half-completed edge roll is indistinguishable from a finished one. The
  canary gates on build version; three of five seat kinds are invisible to
  that gate.

- **This has already happened, and is recorded in the source.** From
  `sweep_orphans`: *"Observed on the playground: a controller from a
  previous start survived two upgrade cycles."* It was harmless there
  because controllers are epoch-fenced and safe to run concurrently — but
  the reason nobody noticed for two cycles is precisely that nothing could
  be asked what build it was. This ADR is not anticipating a failure mode;
  it is closing one that has occurred.

- **The controller cannot be asked anything at all: it has no listener.**
  Its command surface is what it SENDS (`FLINTPROMOTE`, `CPPROMOTED`, …),
  not what it serves, and `flintctl` already notes the consequence
  elsewhere: *"the controller is worse: it has no port to lose, so the
  duplicate LIVES, and the pair gets two supervisors."* A network probe is
  therefore not available for this one seat, and none of the proxy, control
  plane or controller accepts a `--build-version` flag either — that exists
  only on `flint-server`.

- **`CPINFO`'s `version:` field is not a software version.** It is
  `st.version`, the registry generation counter that increments on every
  mutation and drives `CPWATCH`. It sits in an operator-visible response
  next to `cert_days_remaining`, and it reads exactly like a version.

- **Settings are per-node and nothing compares them.** Every knob is in
  each node's own `FLINTINFO`; no view aggregates them, and nothing checks
  that a master and its replica agree. Configuration drift between the two
  members of a pair is currently invisible — including the drift a partial
  roll would produce.

- **On the marketplace AMI, `valkey-cli` is not installed.** A customer who
  SSHes into their own instance cannot run `FLINTINFO`, `CPINFO` or
  `PROXYSTATS` at all. `flintctl` is the only client on the box, which
  makes it the only place a status surface can live for that audience.
  (`smoke-ami.sh` already had to route around this, switching to
  `flintctl verify --probe`.)

- **Tenants cannot read their own configuration.** `CPMYCONFIG` is a
  setter (`<token> <setting> <on|off>`); `CPMYUSAGE` returns usage bytes.
  There is no call that answers "what is my quota, which flags am I on,
  what am I connected to". The console's `/api/overview` covers some of
  this for the SaaS path only — self-hosted and marketplace tenants have no
  equivalent.

The common shape: **Flint tells you about the tier it was easiest to
instrument, and is quietest about the tiers that change during an upgrade.**

## Decision

### D1 — Every binary carries a build stamp, and `status` shows all of them

`flint_build::version` in the proxy, control plane and controller, plus a
`--build-version` flag on each, matching `flint-server`.

The two that have listeners report it in the info response they already
serve: `PROXYSTATS` and `CPINFO`. **The controller needs a different
answer, because it has no listener and should not gain one** — a supervisor
that accepts connections is a supervisor that can be asked to do things,
and its safety today rests on being unreachable. Two options, decided when
this is built rather than now:

- it already dials the CP (`CPPROMOTED`), so it can register its build
  there on startup and the CP reports it — no new listener, but the value
  is as fresh as the last thing it said; or
- `flintctl` reads it from the host, invoking `flint-controller
  --build-version` over the same runner it already uses for `host-*`
  subcommands — always current, but only answerable from a machine with
  ssh reach.

The first is preferred: `status` should not need ssh to answer, and a
controller that has said nothing since a roll is itself the signal.

`flintctl upgrade` then gates the edge and control-plane rolls on the
observed build the way it already gates pair nodes, so a partial roll fails
loudly instead of reporting success.

`CPINFO` gains `build:` and renames the counter to `registry_version:`.
The old key is kept as an alias for one release because `CPWATCH` clients
parse it — the rename is the point, but breaking the watch protocol to
achieve it is not.

**This is the part that is not a feature.** D1 closes a gap in the upgrade
mechanism and should land first, independently of D2 and D3.

### D2 — `flintctl status --json`: one machine-readable cluster answer

The same information `status` prints today, plus every seat's build, the
effective settings per node, disk/cert/quota state, and the fleet-journal
head — as a single JSON document. Rendered from the same query the human
form uses, so the two cannot disagree.

It carries one check no per-node view can perform: **the members of a pair
are compared, and any knob that differs between master and replica is
reported as drift.** A cluster whose replica has a different `lag_hard_ms`
or `wal_fsync_ms` than its master is misconfigured in a way that only shows
up during the failover that needed it.

`--json` rather than a new daemon or port: the AMI already ships
`flintctl`, no new listener means no new attack surface or firewall rule,
and a JSON document is what both a support bundle and a monitoring script
want. A status HTTP endpoint would duplicate the agent's `:9464/metrics`,
which already exists for time-series.

### D3 — `CPMYSTATUS <token>`: what a tenant may know about itself

Quota (ops/sec and bytes) with current usage against it, the tenant flags
actually in effect (replica-reads, near-cache, async-writes), the endpoint
serving them, and the service build. Scoped by the same token-digest lookup
`CPMYUSAGE` already uses, so it exposes nothing across tenant boundaries
and adds no new authentication path.

Deliberately **not** the operator's view: no other tenant's existence, no
node addresses, no cluster topology, no journal. A tenant asking "why am I
being throttled" needs their own limits and whether they are hitting them —
not the fleet.

## What we are explicitly NOT building

- **A status HTTP server or a new port.** The Prometheus exporter is the
  time-series surface and already exists. Adding a second network listener
  to every seat to answer questions `flintctl` can already ask is surface
  for no gain.
- **A config-push mechanism.** D2 REPORTS drift; it does not reconcile it.
  Detecting and fixing are separate decisions, and a reconciler that ran
  during an incident would be a second actor mutating the fleet.
- **Version negotiation or a compatibility matrix.** Reporting a build is
  not the same as deciding what mixed builds are allowed to do together.
  Mixed-build behaviour is the upgrade path's contract (#105) and stays
  there.

## Consequences

- `flintctl status` becomes the single place the whole fleet's identity is
  visible, and `upgrade` can verify what it did to all five seat kinds
  rather than one.
- Three more binaries link `flint-build`, which stamps at launch rather
  than at compile time (#111) — so the stamp must be threaded the same way
  it is in `flint-server`, or it will report the wrong thing in exactly the
  situation it exists for.
- `registry_version:` is a wire-visible rename with a deprecation window.
  It is the only backward-incompatible item here, which is an argument for
  doing it now rather than after there are external `CPWATCH` consumers.
- The drift check will find real drift the first time it runs, and some of
  it will be legitimate (a node mid-roll). It must distinguish "differs"
  from "differs and both have been up longer than a roll would take", or it
  will cry wolf during every upgrade — the failure mode that taught us a
  gate whose red means "unlucky timing" is a gate people re-run instead of
  read.

## Verification

Each item states the condition it must be shown to go red over, per the
standing rule that a check nobody has watched fail is not a check:

1. **D1**: roll the edge to a new build with the control plane left on the
   old one; `status` must show the two different builds and `upgrade` must
   refuse to report success. Positive control: the same run with the stamp
   removed must pass, proving the assertion is what caught it. Separately,
   reproduce the recorded case — leave an orphaned controller from a
   previous start across a roll — and confirm `status` now names it as
   stale rather than letting it survive two cycles unremarked.
2. **D2**: set `lag_hard_ms` differently on the two members of a pair;
   `--json` must report drift naming both the knob and the seats. Then
   restart one member and confirm no drift is reported during the window
   where it is legitimately behind.
3. **D3**: `CPMYSTATUS` with tenant A's token must return A's quota and
   flags, and must contain no string identifying tenant B — asserted by
   provisioning both and grepping the response, not by reading the code.
