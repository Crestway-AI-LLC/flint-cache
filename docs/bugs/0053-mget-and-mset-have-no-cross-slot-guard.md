# BUG-0053: MGET and MSET have no cross-slot guard, and answer wrongly instead

Status: OPEN, found 2026-08-26 · Severity: MEDIUM, rising to HIGH for any fleet
with more than one pair — the wrong answer is silent, well-formed, and
positionally plausible.

## The gap

`crates/flint-server/src/commands.rs` refuses a cross-slot request for the set
operations, with a comment that states the reasoning exactly:

    // SINTER / SUNION / SDIFF: multi-key, and therefore same-slot only. The
    // node refuses a cross-slot request rather than answering it, because it
    // would answer WRONGLY: a key this node does not own reads as an empty
    // set, and an intersection against a phantom empty set is a
    // plausible-looking answer that is silently incorrect.
    //
    // NOTHING UPSTREAM CATCHES THIS. The proxy routes multi-key commands by
    // their FIRST key and never inspects the rest ... This check is the only
    // enforcement in the system, not a second line.

Every word of that applies to `MGET`. A key this node does not own reads as
**nil**, and an array of phantom nils in their correct positions is a
plausible-looking answer that is silently incorrect.

`MGET` and `MSET` call neither the shared helper at `commands.rs:1216` ("the
CROSSSLOT refusal every multi-key command here shares") nor the inline version
at `:563`. They iterate and hit the local store per key:

    b"MGET" => Value::Array(Some(args[1..].iter()
        .map(|k| Value::Bulk(self.strings.get(slot_for_key(k), k).unwrap_or(None)))
        .collect()))

    b"MSET" => for chunk in args[1..].chunks(2) { self.strings.set(slot_for_key(&chunk[0]), ...) }

`MSET` is the worse of the two: it does not read a phantom, it **writes** one,
onto a node that does not own the slot.

## What the proxy does, and does not do

`flint-proxy`'s `route_key` returns `args.get(1)` — the first key, full stop —
and `forward` derives the single slot from it. So a multi-key command is
shipped whole to one pair's master, whatever the remaining keys hash to. The
proxy has no `MGET` case at all; its only mention is a `may_stage` test.

That is by design (`main.rs:40`: "v0 scope, deliberately deferred: ...
cross-slot"), which is precisely why the node-side refusal was written. `MGET`
and `MSET` fall through both.

## Blast radius

- **One pair: correct.** Every slot is local, so nothing reads as a phantom.
  The playground is one pair today and its `MGET` is sound.
- **Two or more pairs: silent wrong answers.** Keys on the other pair come back
  nil, in position. A caller that maps reply index to request index — the
  natural way to use `MGET` — reads "absent" for data that exists.
- **During a slot migration, even growing 1 -> 2:** `check_slot_gate` derives
  the slot from `command_key`, which is `args[1]` alone. Keys 2..N in a
  handed-off slot read locally empty and emit **no `-MOVED`**, so the client is
  never told to look elsewhere. The set-op comment names this consequence for
  itself; it is unguarded for `MGET`/`MSET`.

## Why the existing test does not cover it

`a_cross_slot_set_op_is_refused_rather_than_answered_wrongly` is a good test —
it carries a capability assert that `alpha` and `beta` still hash apart, and a
separate positive control that the same members under one hash tag ARE
answered. It loops over exactly `[SINTER, SUNION, SDIFF]`. `MGET` and `MSET`
are not in the list, and nothing else asserts their cross-slot behaviour.

## Honest status of this report: READ, NOT RUN

I have not reproduced the wrong answer. A unit test cannot produce it: `MemKv`
owns every slot, so both keys resolve locally and `MGET` returns both values.
That is very likely why the gap survived — the cheap test is structurally
incapable of showing the SYMPTOM.

**But the FIX is cheaply testable, and an earlier draft of this section got
that wrong.** It claimed a two-pair fleet was needed. It is not, because the
refusal is a pure function of the KEYS — do they hash to one slot — and owes
nothing to which node owns what. That is exactly how
`a_cross_slot_set_op_is_refused_rather_than_answered_wrongly` tests the set
ops today, against `MemKv`, with no fleet at all.

So the property splits in two, and each half is cheap:

- **The refusal** — `MGET alpha beta` returns `CROSSSLOT` naming both keys.
  Unit test, `MemKv`, identical in shape to the set-op test, with the same
  capability assert that `alpha` and `beta` still hash apart.
- **The positive control** — `MGET {s}alpha {s}beta` is ANSWERED, and answered
  correctly, so the refusal discriminates between slots rather than failing
  every multi-key call.

Conflating the two is what made the earlier scoping expensive: proving the bug
needs a fleet, proving the fix does not. Only the first is blocked, and the
first is not what has to land.

Credit where due: the split came from the S3-accelerator session, which hit the
same `MemKv`-owns-every-slot wall from the client side and split its own
assertion the same way — key shape on one side of the wire, routing on the
other, neither half needing two pairs.

## The fix

Route `MGET`/`MSET`/`MSETNX` through the same `crossslot` helper the other
multi-key commands use. The error already tells the caller what to do —
colocate under a hash tag — and that advice is exactly right here.

Until then, any consumer issuing `MGET` across a multi-pair fleet must colocate
its keys under one hash tag (`{obj}chunk:0`, `{obj}chunk:1`, ...) so a single
slot owns the whole batch.

## What the wrong answer actually costs, per consumer

Severity depends on what the caller does with a nil, and the first consumer to
hit this is a useful worked example rather than the general case.

For a **content-addressed cache** (the S3 accelerator), a phantom nil reads as
a chunk MISS, the origin is authoritative, and each value is sealed to its own
etag and index — so a wrong-node answer cannot become wrong bytes, guarded
twice over. What degrades is EFFECTIVENESS: `MGET` misses chunks living on
other pairs, `MSET` writes fills onto nodes that do not own the slots, and a
later run routing from a different first key cannot find them. Net is a cache
that silently stops working, plus unreachable garbage on wrong nodes.

For a consumer WITHOUT content addressing and an authoritative origin — the
general case, and what a Redis-compatible cache must assume — a phantom nil is
indistinguishable from a real absence, and that is a correctness bug. `MSET`
is worse in both readings, because it writes.

The narrow reading does not lower the severity of the defect; it describes one
consumer that happens to be defended against it by its own design.
