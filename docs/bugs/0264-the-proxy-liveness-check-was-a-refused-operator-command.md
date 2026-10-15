# BUG-0264: flintctl's proxy liveness check was a refused operator command, about 2,640 a day, so the admin-denied count could not show a real probe (FIXED 2026-10-15)

**Status:** **FIXED 2026-10-15.** `proxy_up` presents the fleet's admin token
when it knows one, the way `proxystats_field` already does, and the proxy
refuses nothing. Without a token, or when the proxy answers WRONGPASS
(a rotation in flight), it asks pre-auth as before, and the -NOAUTH still
proves the proxy is up. Found by ops in a review of the playground's ops runs
(Jeff, 2026-10-14).
**Severity:** medium, for what it hid. Nothing was refused that should have
been served, and no fleet read a live proxy as down. But
`flint_proxy_admin_denied_total`, the proxy's `errors{kind="NOAUTH"}` and its
throttled "operator command REFUSED" line exist to show someone probing the
operator surface of a port open to the world (ops OPS-0163). On the
playground our own tooling was all of it, so a real probe would have been
lost in it.
**Area:** `crates/flint-ctl/src/main.rs`, `proxy_up`.

## What happened

From the playground's primary Prometheus, 2026-10-15, over 24 h:

```
sum(increase(flint_proxy_admin_denied_total[1d]))        = 2641
sum(increase(flint_proxy_errors_total{kind="NOAUTH"}[1d])) = 2641
```

The two counters agree exactly, and the daily NOAUTH count had been flat at
about 2,650 for the 12 days on record. On the box, 10 runs of
`flintctl status` moved `admin_denied_total` from 2536 to 2546.

## Why

`proxy_up` sent `PROXYSTATS` with no AUTH and read `-NOAUTH` as "up". That
was deliberate: once the CP pushes the admin digest (ADR-0006 D4), a refusal
proves the proxy is serving. But the proxy counts every refusal in one
funnel, `admin_denied()`, which is right for a refusal. `status`, `verify`
and `start` call `proxy_up` once per proxy, and ops' supervisor runs
`flintctl start` every minute, so each fleet box refused itself about twice
a minute, all day.

`proxystats_field`, beside it, already presented the token (BUG-0128). Only
the liveness check asked without one.

## Fix

- **`proxy_up`** asks `[AUTH <token>, PROXYSTATS]` over one connection when
  `liveness_admin_token` returns a token. A Bulk reply is up.
- **A WRONGPASS** means the proxy holds an older digest than the CP gave
  flintctl: a rotation in flight. It falls through to the pre-auth probe,
  which still reads it up. This costs one counted refusal, and only then.
- **Any other answer** gives the verdict the pre-auth probe would give. A
  proxy with no snapshot answers LOADING to everything but QUIT, AUTH
  included (BUG-0211), so it is not up. No reply is not up.
- **`liveness_admin_token`** looks the token up through `admin_token` (the
  inventory's, else `CPADMINTOKEN`) at most once per process and fleet, and
  remembers a failure as "no token". `admin_token` does not memoise
  failures, on purpose, because `upgrade` re-asks after it restarts the CP.
  But `start` polls `proxy_up` every 150 ms against a 10 s deadline, and a
  failed lookup on a Raft CP spends about 8 s, so re-asking per poll would
  make "the CP is down" read as "the proxy never came up". Nothing depends on
  the token: without one the check answers exactly as it did before.

## Verification

`proxy_liveness_tests` runs `proxy_up` against a fake proxy whose admin gate
answers like the real one. A pre-auth PROXYSTATS gets the live proxy's exact
-NOAUTH and is counted. A token it holds no digest for gets -WRONGPASS.
`loading` answers LOADING to everything. Each case has its own unreachable CP
port, so the per-CP memo cannot cross between cases.
- **A known token:** three checks read up, and 0 refusals.
- **No token:** up, with 1 refusal, the old path, kept.
- **A rotated token:** WRONGPASS falls back, reads up, with 1 refusal.
- **LOADING, with or without a token, and a port with nothing behind it:**
  not up.
- **WRONGPASS is recognised,** and NOAUTH, LOADING and a timeout are not.
- **Controls**, each failing its own test:
  - `proxy_up` with the authenticated path disabled reads up but refuses
    every check (`the liveness check was refused as an operator command`);
  - without the WRONGPASS fallback, the rotating proxy reads as down.

All 121 `flint-ctl` tests pass, and fmt and clippy (`-D warnings`) are clean.
