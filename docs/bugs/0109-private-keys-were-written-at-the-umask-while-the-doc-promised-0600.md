# BUG-0109 — every private key was written at the umask while security.md promised 0600 (FIXED 2026-09-06)

**Status: FIXED 2026-09-06**, and held by `cert_reload_fleet_drill.sh` plus a
unit test on the mode helper. Found 2026-09-06 by auditing `docs/security.md`
for claims nothing checks, the third doc in the run that produced BUG-0103,
BUG-0107 and BUG-0108 · Severity: **high** — a world-readable CA private key
on every deployment shape, with the security posture page telling operators
it was 0600.

## Symptom

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

## Operator note

Fleets bootstrapped before this keep the old modes — the fix runs at mint and
rotation, not at startup. `ls -l <statedir>/certs` and tighten in place, or
run `rotate-certs`, which now hardens as it re-signs. The files themselves
are valid; only their modes were wrong.
