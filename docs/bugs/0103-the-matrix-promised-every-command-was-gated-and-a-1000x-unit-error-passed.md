# BUG-0103 — the matrix promised every command was gated by the oracle, and a 1000x unit error passed it (FIXED 2026-09-05)

**Status: FIXED 2026-09-05**, and held by
`assert_every_dispatched_command_is_gated` in `tools/gates.sh`. Found
2026-09-05 by auditing customer-facing docs for claims nothing checks ·
Severity: medium — the ungated commands include two that set expiry
deadlines, where the failure mode is silent and the unit is the whole
contract.

## Symptom

None. That is the defect: six commands the server answers had no conformance
case, while the first sentence of `docs/command-support.md` told customers

> Every supported command is gated by the conformance oracle: a corpus case
> run against both Flint engines (mem, rocks).

Nothing in `tools/gates.sh` or any drill read that file, so the sentence had
never been true or false — it had never been evaluated.

## What was ungated

`PEXPIREAT`, `PEXPIRETIME`, `COMMAND`, `FLINTKEYSIZE`, `FLINTKEYSTAMP`, and
`FLUSHALL`'s effect. Two more surfaced from the same audit and are separate
defects: `HKEYS`/`HVALS` (below) and `QUIT` (BUG-0106).

## Root cause of the risk: the unit is the only difference

`PEXPIREAT` and `PEXPIRETIME` share an implementation with their
second-granularity twins and differ by one argument:

```rust
b"EXPIREAT"    => self.cmd_expire_at(args, "expireat", 1000),
b"PEXPIREAT"   => self.cmd_expire_at(args, "pexpireat", 1),
b"EXPIRETIME"  => self.cmd_expire_time(args, "expiretime", 1000),
b"PEXPIRETIME" => self.cmd_expire_time(args, "pexpiretime", 1),
```

The seconds variants were gated; the millisecond variants were not. **This
was measured, not reasoned about**: both multipliers were swapped to 1000 and
the full corpus passed `105/105`, as did all 105 `flint-server` unit tests. A
key given a one-hour TTL would have been given a thousand hours, and nothing
this repo runs would have disagreed.

## The trap in the obvious test

A `PEXPIREAT` → `PEXPIRETIME` round trip does **not** catch this. The second
conversion undoes the first, so the round trip passes unchanged with both
multipliers swapped — which is the mutation most likely to happen, since
whoever edits one line edits the other. Every assertion in the new case
therefore CROSSES the units: a millisecond instant read back in seconds, then
the reverse. Each of the three mutations (either multiplier alone, and both
together) now fails at a named step with the wrong number printed.

## `HKEYS` and `HVALS`: named in a case that did not run them

The corpus case `"hgetall hmget hkeys hvals"` exercised `HGETALL` and
`HMGET`. `HKEYS` and `HVALS` appeared in its NAME and in none of its steps —
and the name is what the run summary prints, so a coverage audit read from
the output counted two commands nothing touched. Both were also missing from
`docs/command-support.md` entirely, having been served since the first
release: the matrix under-reported, so a reader would have written the
strictly more expensive `HGETALL`-and-discard workaround. Steps added, and
both commands added to the matrix.

## Fix: enforce the sentence instead of repeating it

`assert_every_dispatched_command_is_gated` reads the dispatcher's arms and
the corpus's steps and fails a build where a dispatched command is named by
no case. Two properties were deliberate:

- **No exemption list.** A command with no oracle — `COMMAND` returns an
  empty array where Valkey returns its whole table — still gets a case, in a
  family `flint_only` names, so it is pinned here and skipped under
  `--reference`. An exemption list is the shape BUG-0086 argues against, and
  the alternative cost four small cases.
- **It cannot pass by reading nothing.** The check anchors on the
  dispatcher's match SUBJECT (`name_upper.as_slice()`), which is what
  separates a command arm from an option arm — an earlier draft keyed on
  indentation and read `b"COUNT"`, a `SCAN` option, as a command. If that
  anchor is ever restructured away the check FAILS with "read no commands at
  all" rather than reporting agreement between two empty sets (OPS-0037).

Both failure modes were confirmed by planting them. The check reports
`125 dispatched, 133 exercised`.

## Scope, stated exactly

The check covers the dispatcher in `crates/flint-server/src/commands.rs`. The
four verbs handled in `serve_loading`'s pre-dispatch (`PING`, `HELLO`,
`FLINTINFO`, `QUIT`) are outside it; `FLINTINFO` gained a case anyway, and
`QUIT` is BUG-0106. A failure here establishes that no corpus step names the
command — not that the command is untested, since a drill may cover it — and
the failure text says so, because a check that overstates gets muted.
