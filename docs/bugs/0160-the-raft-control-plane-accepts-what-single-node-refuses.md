# BUG-0160: the Raft control plane accepts eleven verbs the single-node plane refuses, and only the refusals are drilled (FIXED 2026-09-17)

Status: **FIXED 2026-09-17**, found the same day while converting the
single-node dispatch for ADR-0032 step 3 · Severity: **medium** — nothing is corrupted and nothing is
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

## Reproduced live, all eleven

Against a three-seat control plane built from this commit's control plane
(release, `rocks`; the only other change in that tree was the drill), the
refusal assertions this bug's fix adds to `ctl_cpha_drill.sh` were run once with
each failure counted instead of ending the run:

```
FAIL: flintctl tenant-reads ghost on exited 0 for a name that does not exist: OK
FAIL: flintctl tenant-async ghost on exited 0 for a name that does not exist: OK
FAIL: flintctl tenant-federate ghost on exited 0 for a name that does not exist: OK
FAIL: flintctl tenant-cache ghost on exited 0 for a name that does not exist: OK
FAIL: flintctl tenant-quota ghost 100 1000 exited 0 for a name that does not exist: OK
FAIL: flintctl retire-proxy 127.0.0.1:1 exited 0 for a name that does not exist: OK retired 127.0.0.1:1
FAIL: CPTENANTOVERQUOTA ghost on at the leader answered 'OK', want an ERR naming 'no such tenant'
FAIL: CPSETSUBSET ghost 127.0.0.1:7861 at the leader answered 'OK subset = 1 proxy(ies)', want an ERR naming 'no such tenant'
FAIL: CPDROPPREV ghost at the leader answered 'OK', want an ERR naming 'no such tenant'
FAIL: CPCLEARSLOT acme 100 at the leader answered 'OK', want an ERR naming 'no such exception'
FAIL: CPSETPAIR 7 127.0.0.1:6930,127.0.0.1:6931 at the leader answered 'OK version 15', want an ERR naming 'no such pair index'
MEASURED: 11 of 11 refusals missing
```

Every reply is what the code read predicted, down to the text: `CPDELPROXY`
says `OK retired`, `CPSETSUBSET` reports a placement for a tenant that does not
exist, and `CPSETPAIR` hands back a new registry version for an index past the
end.

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

## The fix

**`Ha::leader_view()`** returns the registry as the LEADER has applied it, or the
redirect to send instead. Every refusal in `ha.rs` asks it. A refusal claims that
something does not exist, and a follower that has not applied the entry creating
it makes that claim wrongly — telling an operator the tenant they added a moment
ago is not there, which is worse than the no-op this bug is about. A follower
redirects at `propose` anyway, so this costs it only the order of its two
answers, and `propose` returning after the entry is APPLIED is what makes the
leader's copy current for the caller's next command.

The eleven arms now refuse with single-node's own messages. `CPCLEARSLOT` asks
`tenant::covers_slot`, the same function the single-node arm asks, so the two
planes cannot come to disagree about what "covered" means.

**The eight arms that already refused** — `CPADDTENANT`, `CPDELTENANT`,
`CPROTATETOKEN`, `CPSETSLOT`, `CPFENCE`, `CPMYROTATE`, `CPMYCONFIG`,
`CPADMINROTATE` — read whichever seat answered and had exactly that exposure.
They read the leader now too.

**It is best-effort, and stays so.** Read-then-propose narrows the window from
always to a race: a `CPDELTENANT` committed between the read and the proposal
still lands a no-op with an `OK`. An atomic refusal would need the state machine
to answer the proposal, and then one log entry would mean different things
depending on what it found — the property ADR-0032 step 3 exists to keep.

**Left alone, decided rather than missed.** `CPADDPROXY`, `CPADDPAIR` and
`CPADMINDROPPREV` still propose no-ops for something already registered: the
version bump is suppressed by every watch loop, and skipping it would be the
same read-then-propose race for nothing an operator can see. The `CPFENCE` and
`CPPROMOTED` reply texts still differ between the planes, and nothing reads
them. `CPROTATETOKEN`'s two checks stay in opposite orders, which matters only
when both are true.

## How it was verified

`tools/ctl_cpha_drill.sh` gained the eleven assertions plus a control, and was
run against a three-seat control plane:

- **Before: 11 of 11 refusals missing** (the output above).
- **After: 11 of 11 refused**, and the control passes — `tenant-quota`,
  `CPTENANTOVERQUOTA`, `CPDROPPREV` and `CPSETSUBSET` still succeed for `acme`.
  The rest of the drill still passes, including the leader kill and the mutation
  that lands on the new leader, which is what would break if the leader read
  were wrong.
- **Two mutations kill it.** Deleting the `CPSETPAIR` guard reddens exactly that
  line (`answered 'OK version 5'`) with the other ten still refusing. Making
  `CPTENANTQUOTA`'s guard refuse every tenant leaves all eleven refusals passing
  and is caught by the control instead — which is the check that the control is
  not decorative.

**The metering agent tolerates the new error**: `world::call` for
`CPTENANTOVERQUOTA` (and `CPDROPPREV` in rotation) matches `Ok(Value::Simple(_))`
and otherwise logs `[CP REJECTED]` and carries on, which is what it already did
against a single-node plane.
