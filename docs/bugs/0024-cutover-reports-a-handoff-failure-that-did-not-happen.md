# BUG-0024: a slot cutover reports a handoff failure that did not happen, and the same call can strand a slot write-frozen (FIXED; trigger still unexplained)

Status: **FIXED 2026-08-19** (the trigger is still unexplained; the defect is not) · found 2026-08-18 · Severity: **medium as observed** (a false
failure report on a cutover that actually succeeded) · **high for the
untriggered half** (the identical pattern on the freeze call left a slot
shedding every write with nothing in a deployed fleet to clear it — see the
CORRECTION below: a reconcile exists but is not enabled, and enabling it as-is
would have been worse)

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
(`migrate.rs:293-297`). It never contacts the source.

> **CORRECTION (same day).** An earlier revision continued: "nothing outside
> `migrate.rs` ever clears a `Migrating` phase ... there is no controller
> reconcile and no GC sweep for a stranded source-side freeze." **That was
> wrong**, and it was reached by grepping for `clear_migration` callers — one
> channel — and reading the absence as proof. Both exist: `FLINTSLOTABORT`
> (`migrate.rs:723`) unfreezes a source, and `recover_migrations`
> (`flint-controller/src/main.rs:1378`) runs **every 2 seconds** and handles
> exactly this shape. `slot_cutover_recovery_drill.sh` tests it.
>
> What is true is narrower, and took reading `controller_args` to establish:
> **no deployed controller enables the reconcile.** `flintctl` never passes
> `--recover-nodes`; only the drill does. So the stranded freeze below does
> persist in a real fleet — because the recovery is not wired in, not because
> it does not exist.
>
> And enabling it is not the fix. Doing so makes this state **worse**: the
> reconcile reads the destination's absent `Importing` as "the destination
> owns it" and completes the flip onto a node that may hold nothing, purging
> the source. Measured, with acked-write loss —
> `0025-recovery-completes-a-flip-onto-a-destination-that-never-imported.md`.

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
   back its `Importing`, it must also unfreeze the source — `FLINTSLOTABORT`
   already exists and does exactly that, so this is a call the rollback path
   does not make, not a mechanism that has to be built.

   **Do NOT satisfy this by enabling the existing reconcile.** As filed, item
   4 said "or a reconcile must exist that clears a `Migrating` phase whose
   destination is gone". One does, it is not enabled in production, and
   enabling it as-is destroys acked writes in this exact state (BUG-0025).
   The destination unfreezing the source it froze is the safe half, because
   the destination knows it aborted; the controller only infers it.

Item 4 is the one that matters most and is the least like a timeout tweak.

## Check that will hold it

The `slot_cutover` drill cannot catch either half today: it only ever sees the
happy path, and a spurious timeout looks to it like a real failure. What is
needed is a fault-injection drill that drops or delays the source's reply to
`FLINTSLOTMOVED` and to `FLINTSLOTFREEZE`, then asserts the end state on
**both** nodes — that a lost `Moved` reply still ends with the destination
serving and the source redirecting, and that a lost freeze reply never leaves
the source shedding writes with no migration in flight.

## Fixed 2026-08-19

All four items landed, in the safe form. The trigger — whatever stalled that
reply for 5 s at a size the purge crosses in 0.125 s — is **still
unexplained**, and the failing log that would narrow it is gone (BUG-0021).
None of this depends on knowing it.

**1. Both control calls retry.** `call_retrying` replaces `call_once` at the
freeze and the flip. Only `Err` is retried: a `Value::Error` is the source
ANSWERING with a refusal, and re-running a decision it already made would hide
it behind the last attempt's text. Both commands are idempotent at a bumped
epoch, which the code already relied on for crash safety.

**2. The error no longer claims what it did not check.** Three outcomes now,
where there were two:

| outcome | what it says |
|---|---|
| `Ok(Simple)` | `MIGRATEIN-OK … cutover` |
| `Ok(other)` | `cutover handoff refused by source (we own)` — the source answered |
| `Err` | `cutover handoff UNCONFIRMED after retries` — plus the fact that the source's `Moved` record commits *before* it replies, so a lost reply is not a lost handoff |

The old text asserted "source not disowned" on the `Err` path, which is the one
case where the source has usually disowned anyway.

**3. The budget is sized to the work.** `call_once_with` takes the read budget
from the caller: 5 s for the freeze, **60 s** for the flip, whose reply waits
behind a purge of every row in the slot. A fixed 5 s was a guess about someone
else's slot size, and rebalancing exists to move the biggest ones.

**4. The rollback reaches the source.** `rollback()` now calls `FLINTSLOTABORT`
on the source when — and only when — this destination is the one that froze it.
`frozen` became a `Cell` so the closure reads the current value. A failure to
unfreeze is printed rather than swallowed; silence is what made the state
invisible.

The destination is the right actor because it KNOWS it aborted. The controller
can only infer that from an absent record, and inferring it is what destroys
data (BUG-0025) — which is why item 4 is emphatically *not* satisfied by
enabling the reconcile.

### Verified

The safety property first, because it is the one that could make things worse —
a rollback must never un-disown a handoff that really completed:

    === A. FROZEN source ===
      while frozen:      SET -> TRYAGAIN slot migrating, retry
      FLINTSLOTABORT  -> OK slot 13624 migration aborted
      after abort:       SET -> OK
      after abort:       GET -> v3

    === B. MOVED source (the safety case) ===
      after handoff:     GET -> MOVED 13624 127.0.0.1:6597
      FLINTSLOTABORT  -> ERR slot is Moved (settled ownership), not in-flight
      still disowned?    GET -> MOVED 13624 127.0.0.1:6597

The refusal in B is enforced by the source's own phase check, not by the
destination's timing — so the safety does not depend on the race being won.

Five drills on the cutover path pass: `slot_cutover`,
`slot_cutover_recovery` (both branches), `slot_migrate`, `migrate_slots`,
`rebalance_execute`. Zero leaked seats.

### Item 4 completed 2026-08-19: the destination now ASKS the source

`FLINTMIGRATIONS ALL` added — read-only, additive, and returning every record
regardless of phase, including the terminal `Moved` ones the bare form
deliberately hides. The controller's parser already ignores phases it does not
recognise, so an older controller against a newer node is unaffected.

The unconfirmed flip path uses it. Where it previously reported honest
uncertainty and stopped, it now re-queries the source and resolves two of the
four cases:

| source says | verdict |
|---|---|
| `moved` | **definite** — handoff completed, returns `MIGRATEIN-OK ... (reply lost; source confirms Moved on re-query)` |
| any other phase | **definite** — it has NOT disowned; retry the flip |
| no record at all | still UNCONFIRMED, and says so |
| could not be asked | still UNCONFIRMED, and names the re-query failure |

**Absence is still not disownership.** `NoRecord` is deliberately not folded
into "disowned" — that collapse is precisely BUG-0025, and doing it here would
have reintroduced the bug one layer up, in the code written to fix it. A source
that cleaned up after a completed move and a source that never held the record
produce the same empty answer, so the empty answer stays uncertain.

An unrecognised argument is an error rather than a silent fallback to the bare
form: `FLINTMIGRATIONS ALLL` returning the filtered set would be
indistinguishable from `ALL` finding no terminal records — the same
two-states-one-output defect the command exists to remove.

**Verified against a real cutover.** Before any migration both forms are empty
(so `ALL` is not inventing rows). After a completed move, asked of the source:

    bare : ''
    ALL  : '13624 moved 127.0.0.1:6571 0'
    typo : ERR FLINTMIGRATIONS takes no argument or ALL, got 'ALLL'

The lost-reply branch was then FORCED, because it is the branch that matters
and nothing reaches it in normal operation. Temporarily cutting the flip budget
to 1 ms against a 40 000-key slot made the source complete the move while the
destination timed out:

    MIGRATEIN-OK 40000 cutover (reply lost; source confirms Moved on re-query)

Before this change that same run returned `ERR cutover handoff UNCONFIRMED`.
Aiming the re-query at a dead address exercised the other reachable arm:

    ERR cutover handoff UNCONFIRMED after retries (... the re-query also
    failed: Connection refused (os error 61) ...)

### Still open

Nothing. The last item — "actually asking the source" — is the section above,
built 2026-08-19 and deliberately landed separately from the retry so the safe
core shipped without a protocol change attached to it.

Two cases remain genuinely UNCONFIRMED by design, and that is the correct
answer rather than a gap: a source with no record at all, and a source that
cannot be reached. Neither can be resolved by asking harder, because neither
distinguishes a completed handoff from one that never happened.
