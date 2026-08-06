# BUG-0004: `start` replaced a seat that was still starting (RESOLVED)

Status: RESOLVED 2026-08-06 · Severity: medium (destroys an in-progress re-seed)

## Symptom
`flintctl start` run on an interval never converged: a recovering replica
was restarted on tick after tick and never reached a serving state.

## Root cause
Liveness was decided by dialling — `FLINTINFO role:` for a pair node,
`PING` for a control-plane seat, `PROXYSTATS` for a proxy. Serving is
proved that way. NOT serving is not: a node doing wipe + full sync holds a
live process and an unbound port for as long as the sync takes, and reads
exactly like a dead one.

For the pair node that is destructive rather than merely wasteful, because
the branch it falls into WIPES the data dir before respawning. A `start`
issued during a sync deletes the sync.

## Fix
All three paths check for the seat's process before concluding it is gone,
and report `STARTING (process up, not serving yet) — left alone`. A wedged
seat remains an operator's call — `stop` then `start` — rather than a
decision a timer makes once a minute without being able to tell the two
apart.

The conservative half is deliberate: a genuinely wedged process is now left
alone indefinitely rather than replaced. A patience timeout would need a
number nobody has measured; `verify` reporting single-copy (BUG-0002) is
what surfaces it meanwhile.

## The check that holds it
`tools/start_guard_drill.sh` freezes a replica with SIGSTOP — alive, owns
its port, answers nothing, the same shape as mid-sync — then asserts the
pid is unchanged, no duplicate appeared, and a sentinel inside the data dir
survived. It control-checks its own setup first: if the frozen seat still
reads as up, it fails rather than proving nothing.

## Correction
The incident originally cited as evidence for this — a replica restarted
four times in four minutes on the managed playground — was NOT this bug. It
was BUG-0005. This fix stands on its own (wiping a mid-sync seat is wrong
regardless, and the drill proves the behaviour), but the symptom was
misattributed at the time.
