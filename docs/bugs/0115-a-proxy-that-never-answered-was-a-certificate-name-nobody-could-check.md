# BUG-0115 — "the proxy never answered" could not say the name was wrong (FIXED 2026-09-05)

**Status: FIXED 2026-09-05.** Split out of
[BUG-0110](0110-the-self-hosting-quickstart-cannot-work-as-written.md), which
promised it and is the reason it exists · Severity: medium — nothing is
broken by it, and it sent the reader to the wrong machine on the one failure
a new self-hoster is most likely to hit.

## Symptom

`bootstrap` dies on a proxy that is running perfectly:

    proxy 0.0.0.0:7379 (dialled at 0.0.0.0:7379) never answered PROXYSTATS
    within 10s
      the edge speaks TLS, and flintctl validated its certificate against:
        ./state/certs/ca.crt
      which is this fleet's INTERNAL CA — the inventory declares no `edge-trust`.
      other causes: the port is held by something else, or the proxy exited at
      startup — check logs/proxy-*.log under the statedir.

Three causes offered. The actual one is not among them: `flintctl` validates
the edge chain against **the name it dialled**, and the edge certificate does
not carry `0.0.0.0`. The handshake is refused however good the trust bundle
is, and the proxy's own log says so — `received fatal alert: BadCertificate`,
an alert it RECEIVED, meaning one the client sent.

## Why the existing message could not get there

It was already most of the way. It prints the trust bundle it validated
against and offers `edge-trust`, which is the right advice for the cause it
names — a chain that does not verify. **It never compares the dialled name to
the certificate**, so the one cause that is a property of two things it
already has in hand goes unmentioned.

The gap is narrow and worth stating precisely: the message knew the trust
path and the dial address, and the missing step was reading the leaf.

## Fix

`flint_tls::cert_sans` — the names a leaf is valid for, rendered as strings a
human compares by eye (`127.0.0.1`, not `IPAddress([127, 0, 0, 1])`). The
proxy-timeout message now reads the edge leaf and, when the dialled host is
absent from it, says so:

    AND THE NAME DOES NOT MATCH. The edge certificate carries
    [127.0.0.1, localhost] and flintctl dialled `0.0.0.0`, so it is refused
    whatever the trust bundle says — the proxy's log will show BadCertificate,
    an alert it RECEIVED from this client. A wildcard `proxy` line needs a
    `proxy-host` (or `proxy-advertise`) naming the machine, or the bind
    address itself has to be a name the cert carries.

**Three outcomes, not two.** A cert with no SANs, and a cert that could not be
read, are different answers and are phrased differently — the unreadable case
says the cause is *unknown*, rather than staying silent and letting the
absence read as "the name was fine". `cert_sans` returns `Some(vec![])` for a
leaf with no SANs and `None` only when the file cannot be read or parsed, so
the caller can tell those apart.

## Verified against the reproduction it was written for

BUG-0110's exact inventory, which is the documented quickstart as it stood:

| arm | result |
|---|---|
| `proxy 0.0.0.0:7379`, no `proxy-host` | the message names the mismatch, printing both the SANs and the dialled name |
| `proxy 127.0.0.1:7379` | bootstrap completes; the new text does not appear |

The second arm is the one that matters. A diagnostic that fires on a healthy
fleet is worse than one that stays quiet on a broken one, because it trains
the reader to skip it.

## What this does not do

It does not prevent the failure — `docs/self-hosting.md` and
`assert_doc_inventories_are_runnable` do that, under BUG-0110. This makes the
failure legible when it happens for a reason no document anticipated: a
`proxy-advertise` naming a public DNS record the cert was not issued for, an
`edge-san` that missed an address clients actually use, a rotated leaf. Those
have the same signature and none of them is a documentation bug.

## Why it is a separate write-up

BUG-0110 is about a document that taught a shape the tool rejects. This is
about a message that could not name a cause it had the facts for. Fixing the
document does not improve the message, and improving the message does not fix
the document — and BUG-0110 said it would be filed separately, which is the
only reason it was: the promise was in a committed write-up and nothing had
been filed.
