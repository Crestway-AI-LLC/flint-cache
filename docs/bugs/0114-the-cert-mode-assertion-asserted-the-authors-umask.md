# BUG-0114 — the cert-mode assertion asserted the author's umask (FIXED 2026-09-06)

**Status: FIXED 2026-09-06.** `cert_reload_fleet` was the single red step in a
137-step gate on the box and passed locally · Severity: medium — it reddened
`main` for everyone against a fleet that was correct.

**SCOPE, AND WHO FIXED WHAT.** Two defects sat on top of each other here, and a
peer session found the first one independently and pushed it first, under
[BUG-0109](0109-private-keys-were-written-at-the-umask-while-the-doc-promised-0600.md)
— "fix the mode check's own portability bug — it reddened CI". Their fix is the
`stat` ordering and the third outcome for an unreadable mode, and it is the one
that shipped; §1 below is kept because this write-up is where the reasoning
about the ordering lives, and because the second defect is unreadable without
it. **What is this bug is §2**: with the mode finally readable, the cert
assertion was still a literal, and a literal is the umask.

Both of us went to the box because the box is the only place the defect is
visible, and neither of us knew the other was there. The cost was one duplicated
25-minute gate run.

## Symptom

    FAIL  cert_reload_fleet      (3.3s)
            FAIL: key modes wrong after bootstrap. docs/security.md states
            0600, and the

The files it went on to list were `edge.crt` and `int.crt` — **certificates,
under a heading about keys** — each followed by a block of `Block size:` and
`Inodes:` lines. The drill passed on macOS.

## Two defects, and the first hides the second

### 1. `stat -f` is not portable, and it FAILS INTO SUCCESS — fixed by the peer under BUG-0109

    m=$(stat -f '%OLp' "$f" 2>/dev/null || stat -c '%a' "$f")

macOS-first. On GNU coreutils `-f` means **filesystem status**, so it takes
`%OLp` as a *filename*, prints a filesystem block for the real file, and exits
1. The fallback does fire — but `$( )` has already captured that block, so `$m`
is a wall of filesystem lines with the mode appended, and can never equal
`"600"`. Proved on the box:

    $ stat -f "%OLp" probe.txt 2>/dev/null || stat -c "%a" probe.txt
      File: "probe.txt"
        ID: 1030100000000 Namelen: 255     Type: xfs
    Block size: 4096       …
    644

**So every mode comparison in this drill failed on Linux regardless of the true
mode.** The check could not read a mode on the platform it matters on, which is
[BUG-0035](0035-the-default-lag-cap-sheds-under-gate-load.md)'s neighbour and
rc.15's class exactly: *"`wait_port_free` bound both `0.0.0.0` and `127.0.0.1`,
which macOS permits and Linux refuses, so every local drill went green while the
first real roll killed a seat and refused to restart it."*

**The fix is an ordering, and the ordering is the whole point.** GNU first,
because the wrong tool must ERROR rather than succeed with a different meaning:

    file_mode () { stat -c '%a' "$1" 2>/dev/null || stat -f '%OLp' "$1" 2>/dev/null; }

BSD `stat` rejects `-c` outright (`stat: illegal option -- c`). GNU `stat -f`
does not reject `%OLp`; it reinterprets it. One of those two orderings is safe
and the other cannot be, and it is not a matter of taste.

### 2. Under it, the assertion was about the umask — this bug

With the mode readable, the certs on the box are **664** and the drill wanted
**644**. Both are correct: the box's umask is `0002`, macOS's is `022`.

`harden_key_modes` clamps `*.key` to `0600` and the directory to `0700`, and
**touches no certificate on purpose** — its own unit test asserts
`"certs must NOT be clamped"`. So the exact cert mode *is* the umask's, and
asserting `644` asserts the machine the drill last ran on.

The peer's fix keeps `[ "$m" = "644" ]`, so on the gate box it trades a check
that cannot read a mode for one that reads it and compares it to the wrong
number. That is strictly better — the failure now names the real value — and it
is still red on any host whose umask is `002`.

**Replaced with the property a clamp would break**, which is what the check was
added for: a certificate has to stay readable beyond its owner, or every dialer
that verifies against it stops.

    [ $(( 0$m & 0044 )) -ne 0 ]

`600` fails; `644` and `664` both pass. An unparseable mode is its own branch,
because after defect 1 an empty or garbage `$m` must not silently satisfy an
arithmetic test.

## Controls

| shape | expected | result |
|---|---|---|
| umask 022: key 600, cert 644 | pass | pass |
| umask 002: key 600, cert 664 — the box | pass | pass |
| cert clamped private (`chmod -R 600`) | catch | `ca.crt is 600 -- clamped private` |
| key widened to 644 | catch | `int.key is 644, want 600` |
| neither `stat` form works (stubbed to fail) | refuse | `could not read a mode ... This is not a pass` |

The third is the one the original cert check existed for, the fourth is the one
the whole function exists for, and the fifth is the peer's third outcome. All
five were re-run against the MERGED check — their loop, this property — because
the merge is what ships and neither half had been exercised in that form.

## The code was right the entire time

Nothing in `flint-ctl` changed. BUG-0109's fix is correct and its unit test says
exactly what it intends. What shipped
beside it was a drill assertion that held on one machine — and the failure it
produced named keys, listed certificates, and printed filesystem statistics.

**A check is a claim about the product only if it can observe the product.**
This one could not, on the platform the gate runs on, and it took a green
137-step run on the same box a day earlier to make that visible: the drill had
passed there before the assertion was added.
