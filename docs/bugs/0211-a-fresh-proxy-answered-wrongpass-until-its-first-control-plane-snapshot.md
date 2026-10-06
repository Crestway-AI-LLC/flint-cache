# BUG-0211: a fresh proxy answered WRONGPASS until its first control-plane snapshot (FIXED 2026-10-06)

**Status:** **FIXED 2026-10-06.** Held by the proxy unit test
`a_proxy_without_its_first_snapshot_answers_loading_not_wrongpass`, which
reads `-WRONGPASS invalid token` with the fix disabled.
**Severity:** high. It hits every roll of a fleet whose proxies are fed by a
control plane, which is every fleet `flintctl` builds. It is intermittent,
and it decides whether a roll reports success.

## Why

A proxy fed by a control plane binds its client port and serves at once.
Its tenant tokens and the admin token's digest arrive later, with the first
snapshot the control plane pushes over CPWATCH. Until then AUTH finds no
admin digest and no tenant, and answers `WRONGPASS invalid token`, even for
a valid token.

Two things land in that window.

**The roll aborts after every seat has rolled.** `flintctl upgrade` restarts
the control plane, then the proxy, waits for `proxy_up`, and reads the
proxy's build with the admin token. `proxy_up` probes PROXYSTATS
unauthenticated, and before the snapshot the operator surface is open, so
the proxy reads as up. The build read then presents the token and gets
WRONGPASS. Measured in public CI on `2c0baf3` (run 37414601258,
`admin_gated_proxy`):

    == UPGRADE ABORTED rolling proxy-7443: could not be asked for its build:
    could not read PROXYSTATS: WRONGPASS invalid token. This is a FAILED READ

The same drill passed on the gate box and in CI at the two commits before,
because the window is short unless the control plane has just restarted.
This is the transient BUG-0083 deliberately left the build read unretried
to identify.

**Tenant clients are told a valid token is wrong.** A client reconnecting to
a proxy that has just restarted, which happens in every roll, can get
WRONGPASS. That reply is about the client's credentials, where the truth is
that the proxy is not ready yet.

## The fix

The proxy keeps a `ready` flag:
- set from the start when no control plane feeds it;
- otherwise set by the CP watch thread only once a snapshot is fully
  applied (pairs, tenants, admin digest, promotion hint and families).

Until it is set, every command but QUIT answers `-LOADING the proxy has not
yet received its tenants from the control plane`. This is the reply a Redis
node gives while it loads its dataset, and Flint's nodes already give it in
that state (`command-support.md`): it says "not ready, retry", not "wrong
credentials".

`proxy_up` already reads anything but a reply or NOAUTH as not up, so the
roll's wait loop now waits for the snapshot before it asks for the build.
When that wait times out, its message names what the proxy answers.

## Not covered

A control plane that has never been written to pushes no snapshot, so a
proxy watching it answers LOADING until something is registered. `flintctl
bootstrap` registers the proxy and the pairs before it starts any proxy.
