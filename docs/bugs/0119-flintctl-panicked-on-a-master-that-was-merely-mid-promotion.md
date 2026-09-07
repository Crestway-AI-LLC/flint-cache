# BUG-0119 — flintctl panicked on a master that was merely mid-promotion (FIXED 2026-09-06)

**Status: FIXED 2026-09-06** · Severity: medium — it aborts an operator command
during exactly the condition the operator is most likely to be reacting to, and
it took a 449-kill soak to reach.

## How it surfaced

The first long run of the M2 failover soak. 448 kills completed cleanly — RTOs
550–600 ms, zero acked writes regressed throughout — and then:

    restart 172.31.74.168:7001: panicked at flint-ctl/src/main.rs
    pair 0 has no reachable master

**The status block taken moments later contradicts it:**

    pair 0  172.31.74.168:7001  DOWN
    pair 0  172.31.78.236:7002  master  epoch (0,214)  live_replicas 0

There *was* a master. It was mid-transition when the question was asked, and
the controller log shows both seats refusing connections in that window.

## The defect

`flintctl` had **five** places that look for a pair's master. Four did a
single-pass `find` over the members and panicked if neither answered
`role: master` at that instant:

    .find(|a| info_field(a, &tls, "role:").as_deref() == Some("master"))
    .unwrap_or_else(|| panic!("pair {pair_idx} has no reachable master"))

The fifth — the cold-start probe — runs the *same* check inside a
`for _ in 0..40 { … sleep(250ms) }` budget and then **degrades** rather than
panicking: *"pair has no reachable master after cold start — left for the
controller"*. Its comment says why: *"a member that boots into a full sync is
alive and not yet serving."*

**One path had learned that "no master right now" is a transient. Four had
not** — and a promotion takes hundreds of milliseconds, so a single probe is a
coin toss weighted by how recently something failed over.

## Why nothing caught it for so long

The affected verbs are `add-replica`, `swap-node`, `decommission-node` and
`migrate-slots`, and the drills that exercise them do so on a **quiet** fleet.
A 12-kill chaos run is over before it is likely to ask during a transition. At
a kill every ~3 seconds, 449 in, it is ordinary.

That is the soak doing the one thing it exists for, on its first long run — and
it is the argument for the soak being a soak rather than more drills.

## The fix

`master_of(members, tls, what)`, shared by all four, with the budget the fifth
already used (40 × 250 ms = 10 s — about 20× a promotion, wide enough to cover
one in flight and narrow enough not to hide a pair that is genuinely down).

**And it returns `Err`, not a panic.** A panic in an operator tool prints a
backtrace hint and no remedy. The refusal now names every member and what each
one answered:

    pair 0 has no reachable master after 10s. What each member answered:
          172.31.74.168:7001  role: <unreadable>
          172.31.78.236:7002  role: replica
    A promotion may be in flight -- the controller resolves that on its own, and
    `status` will show a master once it has. This is NOT the same as the pair
    being gone, and re-running is the right response to it.

ADR-0028's obligation: the old text named a **conclusion** and nothing it
observed, so an operator could not distinguish a pair that is gone from one
they asked about during a failover — which is the difference between paging
someone and running the command again.

## Controls

The message is split from the probing so it can be tested without a fleet, and
two tests pin what it must say: every member appears with the role it actually
answered (including `<unreadable>` for one that did not), and the wait is
stated so a reader is not left assuming it asked once. Both fail against the
old text, which contained none of it.

The retry itself is not unit-tested — it needs a seat whose role changes
mid-probe — and the honest coverage claim is that the soak is its test: the
next long run either reaches kill 449 or does not.
