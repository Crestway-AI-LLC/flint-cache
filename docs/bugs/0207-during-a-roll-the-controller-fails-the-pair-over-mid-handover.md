# BUG-0207: during a roll the controller fails the pair over mid-handover, and two nodes are master at one role epoch (FIXED)

**Status:** **FIXED 2026-10-05**, option A below (Jeff: "go with option A,
you build it"). Filed the same day by the ops session while reviewing the
playground's runs after the rc.79 roll. Held by the new CORE drill
`handover_hold_drill`. **It protects a roll only from the release after the
one that ships it**: `flintctl upgrade` rolls the controller after the pairs,
so during the roll that ships the fix the old controller is still watching.
`flintctl failover` is protected as soon as the controller is on the fixed
build.
**Severity:** medium, latent so far. On all four playground rolls checked
(rc.76, rc.77, rc.78, rc.79) the controller promoted the old master in the
middle of the roll's handover, and the roll then gave the new master the same
role epoch. No acknowledged write was lost on any of them (below). The
handover's design, "demote first, so the old master stops acking writes", is
undone for the length of that window: a write the old master acks there is
above the fence and is discarded when it rejoins.
**Area:** `flintctl upgrade` -> `controlled_failover` (`crates/flint-ctl`),
and the controller's promote rule (`should_promote`, `crates/flint-controller`).

## What happens

`controlled_failover` fences at an epoch above both members, DEMOTES the old
master, DRAINS (the replica applies every acked write), commits the CPFENCE
record, and only then PROMOTES the new master. Between the demote and the
promote, both nodes are replicas.

The controller is not told. It promotes after `confirm` ticks with no master:
on the playground that is `--poll-ms 100 --confirm 3`, about 300 ms. The drain
and the fence commit take longer than that, so the controller promotes the
"freshest survivor". That is the old master, which the roll has just
demoted and which holds the full lineage. Both actors then pick the next
epoch.

The rc.79 roll, 2026-10-05 16:16:32Z. The controller is still the rc.78
process here: `flintctl upgrade` rolls the controller AFTER the pairs.

    controller.log  [ctl][g0] no master for 3/3 ticks (t1: 172.31.64.94:7002 role:"replica" epoch:80,
                              172.31.64.94:7001 role:"replica" epoch:81 | ...)
    controller.log  [ctl][g0] PROMOTED 172.31.64.94:7001 at (0,82): OK promoted at (0,82)
    controller.log  [ctl][g0] re-pointed 172.31.64.94:7002 at 172.31.64.94:7001
    node-7001.log   promoted to master at role epoch (0,82)
    node-7001.log   demote: replication cursor reset to this copy's own seq 336924416 ...
    node-7001.log   demoted to replica at role epoch (0,81) (fenced; wipe + --replica-of to resync)
    roll output     pair 0: 172.31.64.94:7001 demoted + drained; 172.31.64.94:7002 promoted at (0,82)
    node-7002.log   promoted to master at role epoch (0,82)
    node-7001.log   rewound to .../snap-1791216967367-seq336923568-e0.80 (seq 336923568 <= fence 336924424,
                    epoch (0,80)): tailing incrementally instead of a full re-seed

The fleet journal (`cp-state.journal`) shows the same five events in the same
second at every roll since at least rc.76: `Detected` ("g0 master
unreachable, confirmed across required ticks"), `PromoteIssued` (the old
master, "epoch-fenced promotion of the freshest survivor"), `Promoted` (old
master), `Demoted` (old master), `Promoted` (new master).

| roll | controller promoted | roll promoted | old master's rewind |
|---|---|---|---|
| rc.76, 2026-09-21 19:51:30Z | 7001 | 7002 | to 271627120, fence 271627493 |
| rc.77, 2026-09-23 19:29:31Z | 7002 | 7001 | to 280599726, fence 280600723 |
| rc.78, 2026-09-30 20:18:01Z | 7002 | 7001 | to 313433694, fence 313434400 |
| rc.79, 2026-10-05 16:16:32Z | 7001 at (0,82) | 7002 at (0,82) | to 336923568, fence 336924424 |

BUG-0159 records that "the controller only logs promotions it makes from
failure detection" during a roll. These ARE failure-detection promotions, made
during a planned handover.

## Why nothing was lost on these four

A write is lost only if the proxy routes it to the old master while the
controller has it promoted, and the old master acks it before the roll stops
the process. That write lands above the fence, and the rejoin's rewind
discards it. On rc.79, 7001's own sequence at its demote was 336924416, below
the fence at 336924424. Its log had nothing past the fence, and the rewind
re-tailed everything from 7002. The window lasted from the controller's
promote to the roll restarting 7001 (`=== flintctl start node-7001
at_ms=1791216992980`), under a second, and no write landed in it.

That is timing, not a guarantee. A busier fleet, a slower drain, or a proxy
that re-learns the master faster widens the window.

One more line is unexplained and belongs to this bug. 7001's log has `demoted
to replica at role epoch (0,81)` AFTER `promoted to master at role epoch
(0,82)`, although the controller saw 7001 already a replica at epoch 81 before
it promoted. Either the node accepted a demote at a lower epoch than its
current one, or the line is the earlier demote logged late. A fix should say
which.

## Options

- **A (recommended): the handover tells the controller.** Before the demote,
  `controlled_failover` commits a "handover in progress" record for that pair
  to the CP, in the same place the CPFENCE record goes. The controller holds
  promotion for a pair with a fresh record (bounded, say 30 s, so a roll that
  dies mid-handover does not leave the pair unsupervised), and resumes when
  the CPFENCE record lands or the bound passes. The controller already talks
  to the CP: it registers there and journals to it. This changes the protocol, so it needs a
  backward-compatibility note: an old controller ignores the record (today's
  behaviour) and a new one honours it.
- **B: the controller infers the handover.** Hold promotion when both members
  are replicas and one carries a fence epoch above the last promotion the
  controller itself made, because a demote it did not issue means someone
  else is handing over. No protocol change, but it is inference: a real
  failure that looks like this would be delayed.
- **C: stop the controller for the handover.** `flintctl upgrade` already
  restarts it, so stop it before the masters phase and start it after. Simple,
  but every pair in the fleet loses auto-failover for the whole masters phase,
  not only the pair being handed over.
- **Not an option: promote first.** The design rejects it for the reason it
  gives: a promote-first window is exactly the lossy window this bug opens by
  accident.

## Fixed (option A)

ADR-0018's amendment of 2026-10-05 records the design. In short:

- **`CPHANDOVER <addr> [ttl_ms]`** (`crates/flint-controlplane`: the shared
  `state::handover`, dispatched by both `main.rs` and `ha.rs`). It sets, clears
  or queries a hold for the pair holding `addr`. The hold is node-local, never
  Rafted or persisted, served by the leader in Raft mode, and capped at 60 s.
- **`flintctl`'s `controlled_failover`** takes a 5 s hold before the demote
  and refreshes it once a second while the old master still answers the
  drain. It releases the hold after the promote and on every failure path. An
  older CP refuses the verb, and `flintctl` then prints a note and proceeds as
  before.
- **The controller** asks after `confirm` empty ticks, before announcing an
  outage. While a hold is live it stands down, logging "holding: a planned
  handover is in progress" once and asking again at most once a second. An
  old CP, an unreachable CP or no `--commit-cp` reads as no hold, so this
  fails open to the old behaviour.

**`handover_hold_drill`** runs one pair with a controller at the
playground's `poll-ms 100 confirm 3`. `FLINT_HANDOVER_DRAIN_FLOOR_MS`, a drill
knob like `FLINT_ROLL_GRACE_MS`, holds the drain open 2 s, which a laptop's
millisecond drain never does on its own.

- **Arm 1, a planned handover.** The controller logs that it is holding,
  which it does only after `confirm` empty ticks, so the line is the proof
  the gap was long enough to race. It does not promote the old master, and
  the pair ends with one master and the old one as its replica.
- **Arm 2, `flintctl` killed mid-handover.** The hold lapses and the
  controller promotes within 5 s on the gate box.

Both mutants reproduce the playground's failure, with the controller logging
`PROMOTED 127.0.0.1:7568 at (0,3)` into the planned gap:
- `flintctl` taking no hold;
- the controller ignoring holds.

The CP rules are unit-tested in `state.rs`, covering set, query by either
member, refresh, clear, expiry, the cap, the membership guard and a bad ttl.
The registry round-trip test asserts a hold does not survive a reload.

## The test, as first proposed

A CORE drill: a pair under constant writes with an oracle, a controller at
`--poll-ms 100 --confirm 3`, and `flintctl upgrade` with the drain slowed past
300 ms (a large backlog, or a test hook). Assert no `PROMOTED` from the
controller during the masters phase, exactly one node at the new epoch, and
every acked write present after the roll. The unfixed tree should show the
controller's promotion on most runs. The playground shows it on every roll.
