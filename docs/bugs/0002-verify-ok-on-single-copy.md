# BUG-0002: `verify` called a single-copy pair healthy (RESOLVED)

Status: RESOLVED 2026-08-06 · Found by reading a live fleet, not by a drill · Severity: high (silent loss of redundancy)

## Symptom
`flintctl verify` printed `ok pair 0 master 127.0.0.1:7001 (1 down)` and
ended with `VERIFY OK: 1 pair(s), 1 proxy(ies) — all views agree`. The
managed playground ran that way from 2026-08-01 to 2026-08-06: one node, no
failover target, one copy on one disk, behind a watch that said OK every
five minutes.

## Root cause
`verify_checks` matched on `masters.len()`. Exactly one master was the ok
case, and the count of DOWN members was interpolated into the message as a
parenthetical rather than being tested. Nothing consulted it, so the row
passed and `problems` stayed empty.

The underlying replica had exited correctly — it hit a WAL gap, marked
itself for re-seed and stopped (see the escalation added for BUG-0001's
neighbour) — and nothing ran the next start. The agent recommended
`AttachReplica` 963 times into its shadow journal, which nobody reads,
while the thing a human reads said OK.

## Fix
A declared member that is not reachable is now its own failed check:
`SINGLE-COPY: [...] down — no failover target, one copy on one disk`. The
inventory declares two members and reality has one; that disagreement is
what this command exists to surface. Operations ending in `verify_after`
now refuse to report success while a fleet is in that state.

No flow is left red by this: `failover` rejoins the ex-master,
`decommission-node` rewrites the inventory, and `upgrade` does not call
verify mid-roll.

## The check that holds it
`tools/decommission_drill.sh` kills the replica, requires `verify` to exit
non-zero naming SINGLE-COPY, restarts it, and requires green again. A check
never shown to go red over the condition it exists for is not a check.
