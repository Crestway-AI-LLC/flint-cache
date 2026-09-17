# BUG-0160: the Raft control plane accepts eleven verbs the single-node plane refuses, and only the refusals are drilled (OPEN)

Status: **OPEN**, found 2026-09-17 while converting the single-node dispatch for
ADR-0032 step 3 · Severity: **medium** — nothing is corrupted and nothing is
lost, because every one of these commits is a no-op; the cost is that the
production topology tells an operator **OK** for a typo, and the drill suite
asserts a refusal only the drill topology implements.

## What differs

Step 3 put what each verb MEANS in one place, `RegistryState::apply_mutation`.
It deliberately left what each verb REFUSES in the two dispatchers, because a
refusal depends on what the handler found and a Raft entry cannot. So both
dispatchers have to ask, and for these verbs only one of them does.

| verb | single-node (`main.rs`) | Raft (`ha.rs`) |
|---|---|---|
| `CPDELPROXY <unknown>` | `ERR no such proxy` | commits, replies `OK retired <addr>` |
| `CPSETSUBSET <unknown>` | `ERR no such tenant` | commits, replies `OK subset = N` |
| `CPDROPPREV <unknown>` | `ERR no such tenant` | commits, replies `OK` |
| `CPTENANTREADS` / `ASYNC` / `FEDERATE` / `CACHE` `<unknown>` | `ERR no such tenant` | commits, replies `OK` |
| `CPTENANTQUOTA` / `OVERQUOTA` `<unknown>` | `ERR no such tenant` | commits, replies `OK` |
| `CPCLEARSLOT` with nothing covering the slot | `ERR no such exception` | commits, replies `OK` |
| `CPSETPAIR <idx past the end>` | `ERR no such pair index` | commits, replies `OK version <v>` |

That is eleven verbs. Each Raft arm parses its arguments and proposes, and none
checks the registry for the thing it names. `apply_mutation` is total, so the
entry applies as nothing. The version still bumps, so every watch loop renders
a view for its proxy and then suppresses the push as unchanged: work on the
control plane, a `suppressed push` log line per watcher, and nothing on the wire.

**What the operator sees.** `flintctl tenant-quota <typo> 100 0` prints the
reply and exits 0 (`crates/flint-ctl/src/main.rs`, the `tenant-quota` arm prints
any `Value::Simple`). Against a three-node control plane, which is the only
production topology, that is `OK` for a quota applied to nobody.
`retire-proxy <typo>` says `OK retired <typo>`.

## Why nothing caught it

**The one drill that asserts these refusals runs the other topology.**
`tools/ctl_error_drill.sh:89-92` requires `tenant remove ghost`,
`tenant-reads ghost on`, `tenant-quota ghost 100 1000` and `tenant-cache ghost on`
to fail cleanly with `no such tenant`. Its inventory has one `cp` line
(line 49), so every one of those assertions is about the single-node plane.
Three of the four are this bug. `tenant remove` passes on both: flintctl looks
the tenant up in `CPTENANTS` before it sends anything, and `CPDELTENANT`'s Raft
arm asks as well.

**The parity guard compares tables, and says so.** `cp-verb-parity` in
`tools/gates.sh` checks that both dispatchers have the same ARMS, and its own
comment says it "catches the arm somebody forgot to ADD. Nothing here catches
the arm somebody forgot to UPDATE." A refusal missing from one arm is the second
kind.

This is ADR-0032's premise one layer up. Production control planes are three
nodes, the drills run one, and so the drill suite certifies a behaviour the
shipped control plane does not have.

## Confirmed in the code, not yet reproduced live

Read in both dispatchers on 2026-09-17: the eleven Raft arms above go straight
from argument parsing to `ha.propose`. **Not yet reproduced against a running
three-node plane.** The reproduction is the assertions at `ctl_error_drill.sh`
lines 90-92, run against an inventory with three `cp` lines. All three should
fail.

## Also different, and not refusals

Recorded so the fix can decide each one rather than find them again:

- **No-op commits.** `CPADDPROXY` and `CPADDPAIR` for something already
  registered, and `CPADMINDROPPREV` with no previous token, are skipped on
  single-node: no commit, no version bump, no wakeup. On Raft all three propose
  anyway, so each bumps the version and every watch loop renders a view only to
  suppress it.
- **Reply text.** `CPFENCE` says `OK fenced <addr> gen <g>` on single-node and
  `OK fenced <addr>` on Raft. `CPPROMOTED` says `OK promoted <addr> gen <n>` and
  `OK`. Neither difference is read today: both `CPFENCE` callers, the controller
  and flintctl, match `Value::Simple(_)` only, and nothing sends `CPPROMOTED`
  any more.
- **Check order.** `CPROTATETOKEN` checks `token already in use` before
  `no such tenant` on single-node, and the reverse on Raft. That matters only
  when both are true, and then the error names a different fault.

## The fix, and what it cannot promise

Name each refusal once, the way step 3 did for slot coverage: `covers_slot` is
asked by `CPCLEARSLOT` in `main.rs`, and `ha.rs` should ask the same function of
`ha.store.registry()` before proposing. Then add the same checks for tenants,
proxies and pair indexes.

**On Raft the refusal is best-effort, and it should say so.** Read-then-propose
has a window. A `CPDELTENANT` committed between the registry read and the
proposal still produces a no-op commit and an `OK`. That is what happens today
for every request, so the fix narrows the window from always to a race, and it
does not close it. An atomic refusal would need the state machine to answer the
proposal, and the log entry would then mean different things depending on what
it found, which is the property step 3 exists to keep.

**The test that should hold it** drives both dispatchers with the same unknown
names and compares the replies. A textual check cannot see a missing `if`, as
the parity guard's own comment says. The cheap form is `ctl_error_drill.sh` run
a second time against a three-seat control plane.
