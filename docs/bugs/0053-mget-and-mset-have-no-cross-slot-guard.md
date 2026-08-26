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

I have not reproduced it. A unit test cannot: `MemKv` owns every slot, so both
keys resolve locally and `MGET` returns both values. That is very likely why
the gap survived — the cheap test is structurally incapable of showing it.

Reproducing it needs a node owning a strict subset of slots, i.e. a two-pair
fleet, `MGET k_on_pair0 k_on_pair1` through the proxy, asserting the second is
NOT a nil. With a capability assert that the two keys really do land on
different pairs, or the test proves nothing.

## The fix

Route `MGET`/`MSET`/`MSETNX` through the same `crossslot` helper the other
multi-key commands use. The error already tells the caller what to do —
colocate under a hash tag — and that advice is exactly right here.

Until then, any consumer issuing `MGET` across a multi-pair fleet must colocate
its keys under one hash tag (`{obj}chunk:0`, `{obj}chunk:1`, ...) so a single
slot owns the whole batch.
