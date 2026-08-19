# BUG-0024: a slot cutover reports a handoff failure that did not happen, and the same call can strand a slot write-frozen (OPEN)

Status: OPEN, found 2026-08-18 · Severity: **medium as observed** (a false
failure report on a cutover that actually succeeded) · **high for the
untriggered half** (the identical pattern on the freeze call leaves a slot
shedding every write, indefinitely, with no path that clears it)

## Symptom

Local full gate at `f1a9f02`, 115 PASS / 1 FAIL. The `slot_cutover` drill
failed at its first assertion:

    ERR cutover handoff incomplete (we own; source not disowned):
    Err(Os { code: 35, kind: WouldBlock })

`code: 35` on macOS is `EAGAIN`/`EWOULDBLOCK`. The socket is blocking with a
5-second `SO_RCVTIMEO` (`crates/flint-server/src/migrate.rs:796`) and there is
no `set_nonblocking` anywhere in the dial path, so this is a read timeout
expiring — the destination waited 5 s for the source's reply to
`FLINTSLOTMOVED` and gave up.

## The failing log did not survive

Two copies of `drill-slot_cutover.log` still exist on disk:

    /Volumes/FlintDev/drillscratch/flint-gates/drill-slot_cutover.log  Aug 18 11:13
    /tmp/flint-gates/drill-slot_cutover.log                            Aug 18 15:29

**Both are PASS logs**, with different md5s and different server PIDs — two
later, passing runs. The failing run's log was overwritten. That is
`0021-gate-logs-overwrite-so-a-failing-run-erases-the-passing-one.md`
happening to the evidence for this bug, and it is why the trigger below is
still open: the only surviving record of the failure is the error string
quoted above.

## No base rate: this is a first observation, not a known-intermittent

`slot_cutover` has **never failed in CI.** Scored three ways across every
`gate.yml` run whose `gate-logs` artifact is still retrievable:

| bucket | runs |
|---|---|
| gate runs listed | 167 |
| artifacts retrievable (14-day retention) | 132 |
| — ran `slot_cutover`, **PASS** | **71** |
| — ran `slot_cutover`, **FAIL** | **0** |
| — ran it, no verdict line | 0 |
| — ABSENT: drill not in the run | 61 |

The 61 ABSENT runs are not passes and are not counted as any. They have a
clean boundary: the last run without the log is `31415998705` at
2026-08-10T17:49Z, the first with it is `31422957371` at 2026-08-10T19:12Z,
and the per-run log count jumps from 37–58 to 87–113 across that line. The
drill has existed since `030c85b` (2026-07-12) but only entered the CI gate's
stage list on 2026-08-10. So the CI evidence is **eight days and 71 runs**,
not sixteen days and 132.

71 clean runs put a 95% upper bound of ~4.1% on the per-run failure rate in
CI — enough to say this is not a BUG-0014-shaped intermittent (7.0%, which
would survive 71 clean runs with probability 0.006).

**But the bound may not transfer.** CI is `runs-on: ubuntu-latest`; the
failure was on macOS, and `code: 35` is macOS's `EAGAIN` (Linux's is 11). The
*trigger* — whatever stalled that reply for five seconds — may well be
platform-specific. The *defect* below is not: it is in the error path, and it
would render the same wrong claim on either platform.

One local failure, one time. Nothing here should be read as implying a known
firing rate.

## What is NOT the cause: the purge is fast

The source's `FLINTSLOTMOVED` handler (`migrate.rs:528`) does not reply until
it has run `purge_slot_rows` — a collect-then-delete over every row of the
slot across three CFs. The obvious hypothesis is that the purge overruns the
destination's 5 s budget. **Measured, on the same binary and the same disk as
the failing gate:**

| rows in slot | `FLINTSLOTMOVED` wall time | vs. the 5 s budget |
|---|---|---|
| 30 000 | 0.125 s | 40× under |
| 30 000 | 0.132 s | 38× under |
| 100 000 | 0.382 s | 13× under |
| 300 000 | 1.135 s | 4.4× under |

Linear at ~3.8 µs/row. The drill seeds 30 000 keys, so reaching 5 s at drill
size needs a **~40× stall**, not a slow purge. Extrapolated, the steady-state
purge only crosses 5 s at ~1.3 M rows in a single slot.

So the hypothesis is falsified as a steady-state explanation, and the trigger
is unexplained. Candidates, none of them distinguished by surviving evidence:
a RocksDB write stall on the source (which is `0013-bulk-writes-stall-on-
default-compaction.md`, filed, and the source has just absorbed 30 000 SETs
while serving a full bulk ship), disk contention on the drill volume, or a
scheduling stall under a full gate.

Worth noting for later: `purge_slot_rows` collects every key into a
`Vec<Vec<u8>>` before deleting, and rebalancing exists precisely to move the
largest slots, so the timing and the memory both scale with the worst case
the feature is aimed at.

## The actual defect: the error asserts a state it never checked

Whatever caused the stall, the destination's response to it is wrong.

`call_once` returning `Err(WouldBlock)` means exactly one thing: *I did not
hear back.* The code converts that non-answer into a positive claim about the
source — "source not disowned" — that it never asked the source about.

**Measured, with a positive control.** Send `FLINTSLOTMOVED` to a source and
close the socket without ever reading the reply — precisely what a destination
that timed out at 5 s does:

    NEGATIVE control (before cutover): GET -> valxx
    while FROZEN:  SET -> TRYAGAIN slot migrating, retry
      sent FLINTSLOTMOVED, closed the socket without reading the reply
    AFTER unread cutover: GET -> MOVED 13624 127.0.0.1:6593
    AFTER unread cutover: SET -> MOVED 13624 127.0.0.1:6593

The source disowns the slot **regardless of whether anyone reads the reply**.
The negative control (a plain `GET` returning the value beforehand) proves the
probe can show "not moved", so the `MOVED` afterwards is a result and not a
default.

The ordering that makes this true is deliberate and documented at
`migrate.rs:546-557`: `set_slot_phase(..., Moved, ...)` commits the durable
record *first*, and only then does the purge run. The comment says the record
landing first is what makes a crash mid-purge safe. The same ordering makes a
lost reply safe — but the error path was written as though it did not exist.

So in the one case this error can fire, its text is backwards. The cutover had
already completed. An operator reading "source not disowned" goes looking for a
stranded freeze that is not there, and any driver that reacts by retrying or
aborting is reacting to a lie.

## The worse half, same pattern, not yet observed

The freeze call one step earlier (`migrate.rs:366-381`) uses the same
`call_once`, inherits the same 5 s timeout, and on failure calls `rollback()`.

`rollback()` clears **only the destination's** `Importing` record
(`migrate.rs:293-297`). It never contacts the source. And nothing outside
`migrate.rs` ever clears a `Migrating` phase — the only `clear_migration`
callers in the tree are that rollback and the dest-side pre-flip clear. There
is no controller reconcile and no GC sweep for a stranded source-side freeze.

Same control, on the freeze:

    BEFORE freeze: SET -> OK
      sent FLINTSLOTFREEZE, closed the socket without reading the reply
    AFTER unread freeze + dest rollback: SET -> TRYAGAIN slot migrating, retry
    AFTER unread freeze + dest rollback: GET -> v2

A spurious timeout there leaves the source frozen forever while the
destination has rolled itself back and walked away: every write to that slot
shed with `-TRYAGAIN`, no migration in flight, and no code path that clears
it. That is a write outage for 1/16384 of the keyspace, healed only by
operator intervention.

The doc comment at `migrate.rs:274` reads "Any pre-flip failure rolls back our
Importing so we don't strand the slot." The word *our* is carrying the whole
claim: it is the source's freeze that gets stranded, and the rollback does not
reach it.

## Why this is the same failure class as the rest of today

A check that **cannot answer** producing output indistinguishable from an
answer. `Err(WouldBlock)` is "no reply". It was rendered as "source not
disowned", which is a measurement the code never took — and which the control
above shows to be false in every case where the error can fire.

## Fix

Both halves are cheap, and the code has already written down why they are safe:

1. **Retry the terminal call instead of asserting failure.** The comment at
   `migrate.rs:553-556` states `FLINTSLOTMOVED` is *idempotent at a bumped
   epoch*. A timeout should re-dial and re-send, not give up.
2. **When the retry budget is exhausted, ask before claiming.** Re-dial the
   source and read its phase. If it is `Moved`, the cutover succeeded and the
   destination should return `MIGRATEIN-OK ... cutover`. Only a source that
   really is not disowned should produce the current error text.
3. **Size the timeout to the work, not to a guess.** The reply is gated behind
   a purge whose duration scales with slot size. Either reply before purging
   (the durable record has already landed, which is what makes that correct)
   or derive the budget from the row count.
4. **The freeze rollback must reach the source.** If the destination rolls
   back its `Importing`, it must also unfreeze the source, or a reconcile must
   exist that clears a `Migrating` phase whose destination is gone.

Item 4 is the one that matters most and is the least like a timeout tweak.

## Check that will hold it

The `slot_cutover` drill cannot catch either half today: it only ever sees the
happy path, and a spurious timeout looks to it like a real failure. What is
needed is a fault-injection drill that drops or delays the source's reply to
`FLINTSLOTMOVED` and to `FLINTSLOTFREEZE`, then asserts the end state on
**both** nodes — that a lost `Moved` reply still ends with the destination
serving and the source redirecting, and that a lost freeze reply never leaves
the source shedding writes with no migration in flight.
