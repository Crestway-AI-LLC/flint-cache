# BUG-0109 — Flint left private-key modes to whichever `openssl` minted them (FIXED 2026-09-06)

*Filed as "every private key was written at the umask while security.md
promised 0600". That title was refuted the same day by reading the fleet:
the deployed hosts were always 0600 and the exposure was LibreSSL-only.
The filename keeps the original slug, as BUG-0041 does.*

**Status: FIXED 2026-09-06**, and held by `cert_reload_fleet_drill.sh` plus a
unit test on the mode helper. Found 2026-09-06 by auditing `docs/security.md`
for claims nothing checks, the third doc in the run that produced BUG-0103,
BUG-0107 and BUG-0108 · Severity: **low in practice, corrected 2026-09-06
after checking the fleet** — see "What this was NOT" below. The mode was
inherited from whichever TLS toolchain minted the key rather than set by
Flint, so it was right on Linux and wrong on macOS, and no deployed box was
ever exposed.

## What this was NOT, stated first because the first version of this file got it wrong

**No deployed fleet was ever exposed, and no operator action is needed.** The
three live boxes — playground, `flint-ops`, `flint-ops-b` — all carry
`-rw-------` keys and always have.

This write-up initially claimed a world-readable CA key "on every deployment
shape" and rated the bug high. That was wrong, and wrong in the direction
that costs someone an unnecessary production change. It was written from the
modes on a developer laptop plus a correct reading of the code path, without
checking a single real host — the exact failure the repository's own field
notes call out, one layer up: the code was right about what it does not do,
and I did not measure where it mattered.

What decides the mode is the TLS toolchain, not Flint:

| host | toolchain | key mode from `openssl req -newkey -keyout` |
|---|---|---|
| Amazon Linux (every deployed box) | OpenSSL 3.x | `-rw-------` |
| this laptop | LibreSSL 3.3.6 | `-rw-r--r--` |

`ca.crt` is 0644 on those same hosts, so it is not a restrictive root umask
doing it: Linux OpenSSL explicitly restricts a generated private key and
LibreSSL leaves it to the umask.

So the real defect is narrower and still worth fixing: **Flint left a
security property to whichever `openssl` happened to be installed**, and
`docs/security.md` stated it as a fact Flint enforced. On macOS — every
developer machine, and any host with LibreSSL — the keys minted by a drill or
a local bootstrap were world-readable.

## Symptom (on a LibreSSL host)

    $ ls -l <statedir>/certs
    -rw-r--r--  ca.crt
    -rw-r--r--  ca.key      <-- the CA private key
    -rw-r--r--  edge.key
    -rw-r--r--  int.key

`docs/security.md` said:

> Mesh private key — `certs/int.key`, mode **0600**, inside a root-only
> statedir.

Nothing in the repository set a file mode. `grep -rn "set_permissions\|
PermissionsExt\|from_mode" crates/` returned **zero** matches, so every key
took the process umask, which is 0022 on a normal login shell and gives 0644.

## Root cause: the mode was declared where it could not be applied

`cert_manifest` returns exactly the right thing:

```rust
let mut out = vec![("ca.crt", "644"), ("int.crt", "644"), ("int.key", "600")];
```

and the only consumer is `push_certs`, which opens with

```rust
if !r.is_remote() {
    continue;
}
```

So the mode travelled with the COPIES pushed to other hosts and was never
applied to the ORIGINALS the orchestrator minted. On a **single-host**
deployment — what the AMI's first boot renders, and what the quickstart
produces — `push_certs` has no remote runner at all, so it applied nothing
anywhere.

`ca.key` is the sharpest case and it was uncovered on **every** deployment
shape, fleet included, because it is deliberately absent from the manifest:
the CA key stays on the orchestrator and is pushed nowhere, so nothing ever
carried a mode to it.

The comment above `cert_manifest` had already named the hazard for a
different bug — "push_certs skips every non-remote runner, and the local
drills have nothing else, so on a laptop this code does not run AT ALL". The
same sentence explains this one.

## Why a readable CA key matters more here than usual

The mesh has no per-host identity. Internal dials verify a fixed
`ServerName` (`flint_tls::INTERNAL_SNI = "flint-internal"`) rather than the
address dialed, and one leaf serves every component — which is what makes
distribution a file copy instead of a PKI. The same property means a leaf
minted by anyone holding `ca.key` is accepted by **every** component in the
fleet, with no host binding to get in the way.

## Fix

`harden_key_modes` sets 0600 on every `*.key` in the certs directory and 0700
on the directory, called from `mint_certs` and from `resign_leaves`, so both
bootstrap and `rotate-certs` are covered. Certificates are deliberately left
alone: several readers expect them public.

A failure to chmod is **fatal**, not a warning. The alternative is writing a
world-readable CA key and then printing the line that says a CA was minted,
which is the shape of the defects in this directory that took longest to
find.

## Checks, and the mutation that proves they work

- A unit test on the helper, starting from files deliberately created 0644 —
  a test that only asserted "0600 afterwards" would pass on a machine whose
  umask happened to be 0077 and prove nothing on the machines that matter.
  It also asserts `ca.crt` stays 0644.
- `cert_reload_fleet_drill.sh` asserts the modes on the two paths that write
  a key: after bootstrap, and again after `rotate-certs`. A rotation that
  re-widened a key would be invisible everywhere else.
- Both directions are checked: keys must be 600 **and** certificates must be
  644, so a blanket `chmod -R 600` does not satisfy the drill.

Confirmed by removing the fix and re-running: `ca.key is 644, want 600`, and
three more, followed by the drill's FAIL.

### The check's own portability bug, which reddened CI for four commits

The first version of the drill assertion read the mode with

    stat -f '%OLp' "$f" 2>/dev/null || stat -c '%a' "$f"

`-f` is the FORMAT flag on macOS and means **filesystem status** on GNU/Linux,
where it SUCCEEDS — so on the CI runner the fallback was never reached and the
comparison ran against a line of filesystem information. The failure text read

    600, want 600

which is the tell: a check reporting that a value does not equal itself is not
looking at the value it names. Four commits were red before it was read,
including a peer's.

Fixed by putting GNU first and BSD second, and by refusing to compare a mode
that matched neither form rather than treating an empty string as a
mismatch — "could not measure" is its own outcome (OPS-0037). Both branches
were then exercised against a stub `stat` emulating each platform, because
the bug was in the half this laptop cannot run.

## Operator note

**Nothing to do.** Every deployed box was already 0600, because its OpenSSL
restricts generated keys. Confirmed by reading all three rather than assuming
either way.

A host whose `openssl` is LibreSSL and which was bootstrapped before this
would keep 0644, since the fix runs at mint and rotation rather than at
startup. To check one:

    sudo ls -l <statedir>/certs/*.key

and if any is not `-rw-------`, `sudo chmod 600 <statedir>/certs/*.key` or run
`rotate-certs`, which now hardens as it re-signs. The files are valid either
way; only the modes would be wrong.
