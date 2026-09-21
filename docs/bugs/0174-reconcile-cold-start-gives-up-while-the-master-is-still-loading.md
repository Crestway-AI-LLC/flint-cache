# BUG-0174: `reconcile_cold_start` gives up while the master is still loading, and a failed-over pair replicates nothing (FIXED 2026-09-21)

**Status:** **FIXED 2026-09-21.** Found investigating the two `cold_start_roles`
failures that blocked the v0.1.0-rc.75 release gate.
**Severity:** high on a fleet with no ops agent, medium on one with it (see
*Severity* below). Present in **every release back to at least rc.40,
including rc.74**, since the [BUG-0008](0008-cold-start-of-a-failed-over-pair.md) fix on 2026-08-08.

## Proven by A/B, with only the probe differing

The drill now holds every node in LOADING for 6 s (`FLINT_TEST_HOLD_LOADING_MS`,
below), which makes the race certain instead of a matter of runner speed. Three
runs each side, on the EC2 gate box, identical except for `reconcile_cold_start`:

| | old probe + 6 s hold | fixed probe + 6 s hold |
|---|---|---|
| run 1 | **FAIL** — *no reachable master after cold start*, then `live_replicas 0` | **PASS** |
| run 2 | **FAIL** — same | **PASS** |
| run 3 | **FAIL** — same | **PASS** |

The negative side reproduces the two release-gate failures byte for byte:
same bail line, same `loading=0` / `live_replicas=0` diagnostic. A pass on the
positive side is specific evidence rather than an absence of failure: the drill
also asserts that `start` reported *durable roles disagree with inventory
order*, which prints only when the fixup **found** the master — so each pass
proves the fixed probe found it despite the hold.

**What is inferred, not observed:** the server logs *holding LOADING* to the
node's own log, which the drill does not print, so the knob firing is inferred
from the A/B rather than read. The A/B is what carries the claim.

## What happened

The v0.1.0-rc.75 release gate failed `cold_start_roles` on **both** attempts of
run 35613619139, while the `main` run at the **same sha** (`01b390f`, run
35557552420) passed it in 57.9 s. Identical diagnostic both times:

    seat 127.0.0.1:7403: role=replica loading=0 live_replicas=0
    seat 127.0.0.1:7404: role=master  loading=0 live_replicas=0
    pair has no reachable master after cold start — left for the controller

[BUG-0064](0064-cold-start-roles-cannot-say-whether-the-replica-was-loading.md)
had been left open for exactly this firing: *"only the next firing, now
instrumented, will say."* By its own reading rule, `loading=0` with
`live_replicas=0` is the did-not-attach case, which rules out the benign
explanation it was waiting to eliminate.

## Six hypotheses eliminated before the mechanism

The same sha passing on one branch and failing on another looked ref-dependent.
None of the ref-dependent explanations survived measurement:

| hypothesis | result |
|---|---|
| the rc.75 **tag** changes the build | nothing reads git tags; the version is `FLINT_RELEASE_TAG` via `option_env!`, which the gate never sets — both builds print *unstamped 0.0.1* |
| GitHub **cache** scoping | all three runs hit `valkey-9.1.0-tls-Linux` |
| **runner image** | all three `20260828.587` |
| the recent core changes (BUG-0169..0173) | every `main` run from BUG-0169 to tip passes, 57.9–60.1 s, 25 of 25 |
| **release branches** as such | rc.74's release branch passed twice at ~60 s |
| an orphan squatting a port | the `flint-kvorphan-server` seen once is `exec -a … sleep 60` from `fleet_guard_drill`; it binds nothing and was absent from the other failure |

What remained was one run failing twice while 25 others passed — which is what
a timing window produces, not a ref.

## The mechanism

`flintctl start`'s cold-start fixup, after spawning a pair:

```rust
for _ in 0..40 {                                           // 10 s
    roles = pair.iter().map(|a| info_field(a, tls, "role:")).collect();
    if roles.iter().all(Option::is_some) { break; }       // breaks on ANY role
    std::thread::sleep(Duration::from_millis(250));
}
let Some(master) = ... .find(|(_, r)| r.as_deref() == Some("master"))
else { eprintln!("pair has no reachable master after cold start — left for the controller"); return; };
```

Since #176 a node answers FLINTINFO from **inside** its load with
`role:loading` and `loading:1`. `Some("loading")` satisfies `is_some()`, so the
loop exits on the first answer while the master is still loading. The match for
`"master"` then correctly fails — the fixup never *mistakes* loading for a
master — and it gives up. **The defect is the loop exit alone.**

What that leaves is spelled out in the function's own doc comment: a failed-over
pair's `pair[0]` boots as **"replica of NOBODY"**, and this function is the only
thing that re-attaches it. `status` shows both members up, roles coherent,
epochs agreed; only `live_replicas 0` gives it away. That is
[BUG-0008](0008-cold-start-of-a-failed-over-pair.md)'s silent single-copy fleet, reintroduced by the probe that
BUG-0008's own fix added.

**Two intent/code mismatches in forty-five lines.** The comment reads *"give
the probe the same budget the rest of `start` gives a seat"*; `start` uses
`node_ready_budget` (15 s, `node-ready-s`), and the loop hardcoded 10 s. And that
budget existed to wait out loading, yet `is_some()` short-circuited it on the
loading state itself.

It is BUG-0064's own lesson — *"a role is DECIDED, not merely present"* —
learned for `role_of`, never carried to `replicas_of` (BUG-0064), and never
carried to this probe either.

## Why the drill almost never caught it

`start` sleeps 700 ms after spawning the first seat and calls
`reconcile_cold_start` immediately; there is no readiness wait between them.
The drill seeds **200 keys**, which load inside that pause, so the master
nearly always read `master` and the drill passed 25 runs in a row. Under
contention — both release attempts ran beside four peer drills — the load
outlasted the pause, and the fixup bailed. **A real dataset outlasts it every
time.** The drill's small dataset hid a defect that production would hit on
essentially every cold start of a failed-over pair.

## Severity, measured rather than asserted

Corrected twice by evidence during the investigation, both times downward and
in a specific way:

- **The live playground is not in this state** (measured on rc.74 by the peer
  session: master `live_replicas 1`, replica streaming). The defect fires only
  on a cold start of a *failed-over* pair.
- **Where the armed ops agent runs, it is detected, not silent.** The planner
  builds `Insight::WidowedMaster` from FLINTINFO — not from `flintctl`'s stderr
  — with evidence `live_replicas:0 (single-copy exposure)`, and
  `IncidentOverdue` pages a human. **At discovery it was not auto-repaired**: a
  detached pair (every member reachable, epochs equal, one non-master) routes to
  `ReattachReplica` (ADR-0035), which was armed on neither box, and tier2 refuses
  `AttachReplica` for a detached pair. **Since 2026-09-21 it is**: both ops boxes
  were armed `AttachReplica,PromoteReplica,ReattachReplica` the same day (ops
  `d12fdf52`), so on that fleet this state is now repaired rather than paged —
  with one caveat that is not a formality: the loop from detection through
  `ReattachReplica` to a re-attached replica has not been drilled end to end.
- **On a fleet with no agent** — a self-hoster running `flintctl` alone — it is
  silent and permanent.

| fleet | outcome |
|---|---|
| armed ops agent | detected; **repaired** since 2026-09-21 (`ReattachReplica` armed), paged before — end-to-end loop undrilled |
| no agent | silent single-copy, until a human notices `live_replicas` |

## The fix

Wait for each member with **`wait_pong`** — the existing readiness test, which
keys on `loading:1`. The server's own test names that as *"the field every
consumer keys on"*, and `wait_pong` already reads a pre-#176 node (no such
field) as ready, so a rolling upgrade is unaffected. The budget is
**`node_ready_budget`**, as the comment always promised. Roles are read only
once every member is serving.

Both give-up paths keep the rule that no-master means **do not re-seed blind**,
and now say plainly that the pair is **not replicating**:

- not ready inside the budget → names the members, says the master cannot be
  decided, points at `live_replicas` and `node-ready-s`;
- every member serving and none master → says so, and that `start` is not
  repairing it.

The old text, *"left for the controller"*, was false: a decision-only
controller attaches nobody. An early draft of the replacement overreached the
other way — *"nothing will repair it automatically"* — which is a claim about
the agent's arming that `flintctl` cannot know and that flips the moment
someone arms `ReattachReplica`. It says only what `start` knows.

## The test knob

`FLINT_TEST_HOLD_LOADING_MS` in `flint-server` sleeps immediately before the
one line that clears `LOADING`. The loading acceptor is still running there,
so for the whole hold FLINTINFO keeps answering `role:loading` and
`loading:1` — exactly the window a real dataset produces and a 200-key drill
does not. Test-only, like `FLINT_BATCH_COMMIT_FAIL`: unset, it costs one env
lookup at startup.

6 s was chosen to sit well outside the 700 ms pause `start` takes before the
probe, so the probe always lands inside the hold, and well inside
`node_ready_budget` (15 s), so a correct probe waits it out.

## Investigation notes worth keeping

Three of my own checks returned false negatives on the way, each caught by a
control before it reached a conclusion:

- the rc.75 **tag** was reported to Jeff and the peer session as the leading
  lead; nothing reads git tags, and it was not;
- a version sweep reported the defect **absent** from rc.74 because it grepped
  for the function by a name it did not have at that tag;
- a second sweep reported it absent everywhere because zsh parses `"$t:path"`
  as `$t` with the `:c` history modifier. A row where the code was **known**
  present read zero, which proved the instrument broken.

And one the gate caught rather than a control: the new server knob was not
added to `flintctl`'s `SEAT_ENV_NAMES`, so `node_env_names_match_the_seat` —
which scans the seat sources for every `"FLINT_*"` literal and requires the
two lists to agree — failed in both builds on the first full gate. The local
`cargo check` could not have seen it: it runs no tests. Beyond the red test,
the omission was real: `flintctl` warns about any `FLINT_` variable not on
that list, so the drill would have tripped a spurious "unknown knob".

The peer session independently verified every link of the mechanism, found the
server's own statement of the `loading:1` contract, measured the live
playground healthy, and raised the question that corrected the severity from
"silent everywhere" to "detected where the agent runs".
