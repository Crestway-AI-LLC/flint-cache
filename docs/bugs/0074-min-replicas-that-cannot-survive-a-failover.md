# BUG-0074 — a min-replicas-to-write no failover can satisfy, accepted at deploy (FIXED 2026-08-29)

**Found** 2026-08-29, from Jeff's observation while reviewing BUG-0071: if
`min-replicas-to-write=1` is going to block, the fleet should have been shaped
so it does not — and nothing was checking that.

## The arithmetic

After a failover the survivors are `members - 1`, and one of them IS the new
master. So live replicas = **`members - 2`**.

| members | replicas | live replicas after a failover | `min-replicas=1` |
|---|---|---|---|
| 2 | 1 | **0** | sheds every write, every failover |
| 3 | 2 | 1 | rides through |

On a two-member pair `min-replicas-to-write 1` is not a trade that sometimes
costs something. It is a **guaranteed** write outage on every failover, lasting
until the dead seat rejoins — a rewind at best, a full re-seed at worst, and
BUG-0071 measured 94.2 s of one.

## Why this needed a guard rather than a doc

The server honours whatever value it is given, deliberately, and
`main.rs:1966` already argues the case: *"1 IS A REAL TRADE, NOT A STRICTLY
SAFER SETTING… A freshly promoted master has no replica either, and the gate
cannot tell 'my peer died' from 'I was just promoted'."*

So the value is legitimate and the default (0) is right. What was missing is
anyone saying so **at deploy**, while the fleet is still being shaped. Nothing
in `flintctl` related `min-replicas` to pair size: the inventory value was
passed straight to every seat, `verify` never looked, and the consequence
arrived months later at the worst possible moment.

That is BUG-0069's shape exactly — a policy the fleet cannot honour, with no
deploy step to refuse at.

## Fixed

`verify` refuses when a member's live `min_replicas_to_write` exceeds
`members - 2`, naming the pair, the seat, the arithmetic, and three remedies:
lower the value, add a member, or bound the exposure with `widowed-grace-ms`
instead (10 s by default, and the guard that IS on by default for pair members).

Read from the LIVE seat rather than the inventory, because the value is
hot-reloadable through FLINTCONFIG: the inventory is what was asked for,
FLINTINFO is what is true.

`--allow-blocking-min-replicas` carries a deliberate posture — "never accept a
write that is not on two copies" is a real choice — and says so in the output
rather than passing silently.

## Held by

`tools/min_replicas_survivable_drill.sh` (gate step `min_replicas_survivable`),
four arms: the default verifies clean (a guard red on a healthy fleet is one
people learn to skip); `min-replicas 1` on a TWO-member pair is refused with its
arithmetic; the override carries it and says so; and the same value on a
THREE-member pair is **accepted**, because a failover there still leaves one
replica.

That last arm is the one that matters most. Mutation-checked, failing DIFFERENT
arms: keying the guard on the number (`mr == 0`) instead of survivability
(`mr <= members - 2`) fails the three-member arm; disarming the refusal fails
the two-member arm.

## Not covered

Whether three-member pairs are SAFE is a separate question this does not answer.
They bootstrap, replicate and verify — the drill's own three-member arm reaches
two live replicas — but `manifest.rs:403` states that the promotion fence's
induction assumes two members and "a pair with more than two members breaks the
induction — revisit before that exists". Recommending three members as the way
to run `min-replicas=1` therefore needs that revisit first; this guard only
stops the configuration that CANNOT work from deploying silently.
