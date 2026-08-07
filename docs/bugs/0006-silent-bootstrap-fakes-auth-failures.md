# BUG-0006: a silent drill bootstrap manufactures auth failures (RESOLVED)

Status: RESOLVED 2026-08-07 · Found chasing "token/auth failures" that were neither · Severity: medium (spurious gate failures, misdirected debugging)

## Symptom

A fleet-repo gate run failed six drills at once with product-shaped
security errors: `WRONGPASS invalid token` on a token the drill had just
registered, `forged token was not refused`, `FAIL: tenant data path`. A
rerun failed a *different* pair of drills the same way. Every drill passed
when run alone.

## The wrong conclusion drawn first

That recent metering/auth commits had broken token handling. They hadn't:
the newest commit touched no auth path, and a hand-built cluster accepted
its tenant token immediately after `tenant add`.

## Root cause

Two silent failure paths in the drills' bootstrap, both only reachable on
a loaded box:

1. The control-plane wait was `for i in $(seq 1 30); do ... PING ... done`
   — a loop whose expiry FALLS THROUGH. When the CP missed its ~6s budget,
   the drill carried on against a socket nobody answered.
2. The bootstrap commands after it were `valkey-cli -p N CPADDTENANT ...
   >/dev/null`. valkey-cli exits 0 whether the reply was `OK` or `ERR` (or
   connection refused), so the redirect discarded the only evidence that
   the tenant exists.

Put together: CP slow → CPADDTENANT silently fails → every later
`AUTH tok-acme` gets a correct WRONGPASS for a token that was never
created. The drill then reports an auth failure ten lines from the actual
fault. The load that triggered it was itself a second drill suite running
on the same box (see BUG-0003's sibling problem: same-scope collisions
between two suites are invisible to `fleet_guard`, because the other
suite's seats look like our own).

## Fix

Two helpers in `tools/lib/fleet.sh` (both repos' copies, kept in step):

- `fleet_wait_ping <port> [cli opts]` — deadline-based like
  `fleet_wait_listen`, but proves RESP, and FAILS the drill loudly on
  expiry instead of falling through.
- `fleet_cp <port> [opts] <cmd ...>` — runs a CP bootstrap command and
  dies unless the reply starts with `OK`. (CPADDTENANT replies
  `OK tenant ...`, so the match is on the prefix.)

Every drill bootstrap in both repos was converted: 20 drills here, 14 in
the fleet repo. Mid-drill waits that already fail loudly were left alone.

## The check that holds it

The helpers ARE the check: a drill that cannot bootstrap now says
`FAIL: no PONG from 127.0.0.1:7680 after 30s` or
`FAIL: CP bootstrap ... CPADDTENANT ... -> ERR ...` at the moment of the
fault, and nothing later runs against a cluster that was never built. The
general rule, learned twice now (gates.sh's warm-up comment records the
first time): a wait whose expiry is not an error is a bet, and on a loaded
box the bet loses in the shape of whatever runs next.
