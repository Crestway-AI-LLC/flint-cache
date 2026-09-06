# BUG-0107 — the self-hosting quickstart cannot work as written (FIXED 2026-09-05)

**Status: FIXED 2026-09-05.** Found the same day by running the quickstart
instead of reading it · Severity: medium-high — it is the first thing a new
self-hoster does, and it fails at `bootstrap` with a message that blames the
wrong component.

## Symptom

`docs/self-hosting.md`'s "Quick start (single box)", copied verbatim:

    == bootstrap into ./state (tls on)
      minted internal CA + component cert + edge cert
      started cp (pid 32398)
      started node-7001 (pid 32404)
      started node-7002 (pid 32413)
      pair 0: 127.0.0.1:7001 answering
      started proxy-7379 (pid 32418)

    thread 'main' panicked at crates/flint-ctl/src/main.rs:3727:13:
    proxy 0.0.0.0:7379 (dialled at 0.0.0.0:7379) never answered PROXYSTATS
    within 10s

Every seat starts. The fleet then dies on the proxy.

## The message blames the wrong thing

"never answered PROXYSTATS" reads as a proxy that is dead or wedged, and the
error's own suggestions point at the port being held or the proxy having
exited. It is neither. `state/logs/proxy-7379.log` says:

    tls: connection setup failed: received fatal alert: BadCertificate

A fatal alert *received* by the proxy is one the CLIENT sent. `flintctl`
rejected the proxy's certificate, and the proxy is behaving correctly
throughout.

## Root cause: the dial target is the BIND address

The quickstart declares `proxy 0.0.0.0:7379`, which is a wildcard bind and not
an address of any machine. With no `proxy-host` and no `proxy-advertise`,
`proxy_dial` returns the bind address verbatim:

```rust
match inv.proxy_hosts.get(i) {
    Some(host) => format!("{host}:{}", port_of(bind)),
    None => bind.clone(),
}
```

So `flintctl` connects to `0.0.0.0:7379` and validates the edge certificate
against the name `0.0.0.0`. `bootstrap` mints that cert with

    X509v3 Subject Alternative Name: IP Address:127.0.0.1, DNS:localhost

`0.0.0.0` is not among them, so name verification fails and the client sends
BadCertificate. **The failure is deterministic and platform-independent** —
nothing about it depends on how a host treats a connection to `0.0.0.0`.

## Why nothing caught it

Every other inventory in the tree binds loopback. `README.md`'s own quickstart
says `proxy 127.0.0.1:7379`, and so does every drill — `grep 'proxy 0.0.0.0'`
over `tools/` returns nothing. The two quickstarts disagreed, and the one that
was wrong is the one in the document named "self-hosting".

The shape is exercised constantly on fleets, where it works: `chaos-cluster`
declares `proxy 0.0.0.0:7379` **and** `proxy-host <ip>`, because a multi-host
fleet must say which machine the proxy is on. It is only the single-box case
that can omit `proxy-host` and reach the broken path.

## Fix

The quickstart binds loopback, matching `README.md` and every drill. Verified
by running the whole thing:

    bootstrap complete … verify: bootstrap left the cluster consistent
    OK tenant acme ns acme subset [127.0.0.1:7379]
    valkey-cli … SET hello world  ->  OK
    valkey-cli … GET hello        ->  world

The wildcard variant is documented in the Placement section as needing
`proxy-host`, and that combination was verified too — `proxy 0.0.0.0:7379` plus
`proxy-host 127.0.0.1` bootstraps clean. So both shapes work; only the wildcard
alone does not.

## Held by `assert_doc_inventories_are_runnable`

The check added hours earlier for the missing-`ssh-user` case gains a second
rule: **a documented inventory that binds a proxy to a wildcard must declare
`proxy-host` or `proxy-advertise` in the same block.** Static, for the same
reason as the first rule — running these would spawn seats for any example on
127.0.0.1.

## Not fixed here, and worth separating

**The error message still blames the proxy.** "never answered PROXYSTATS"
with hints about a held port and a crashed process is the right message for
the causes it lists, and this is a fourth cause it does not: the client
rejected the certificate, which the proxy's own log knows and the error does
not read. The message already prints the CA it validated against and mentions
`edge-trust`, so it is halfway there — it does not notice that the DIALLED
NAME is absent from the cert it just rejected. That is a code change in the
failure path, filed separately rather than folded in here, because this bug is
about a document and that one is about a diagnostic.
