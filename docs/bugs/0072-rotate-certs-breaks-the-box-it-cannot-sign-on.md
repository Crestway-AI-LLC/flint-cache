# BUG-0072 — `rotate-certs` breaks the box it cannot sign on

**Status:** fixed, 2026-08-28.
**Area:** `flintctl rotate-certs` / `resign_leaves` (ADR-0006 D4).
**Severity:** would take a running box off the mesh, with no way for it to say so.

## What was wrong

`rotate_certs` guarded on the wrong file:

```rust
assert!(Path::new(&format!("{d}/ca.crt")).exists(), "no CA at {d}/ca.crt ...");
```

Signing needs `ca.key`. Both are usually together — and there are real boxes
that hold one and not the other. Flint's two ops boxes carry `ca.crt`,
`int.crt` and `int.key` so the read-only agent can dial the mesh, and
deliberately **no `ca.key`**: they are observers, not the certificate
authority. Verified by `ls` on `flint-ops` before this was written.

`resign_leaves` then ran in place, and its first step is:

```
openssl req -newkey rsa:2048 -nodes -keyout {d}/int.key -out {d}/int.csr
```

which **overwrites the live leaf key** and succeeds. The next step signs with
`-CAkey {d}/ca.key`, fails, and the `assert!` inside `sh` panics. What is left
on disk is a fresh private key beside the previous certificate — a mismatched
pair, which the components' TLS watchers hot-reload within ~2 s.

So the box loses its mesh identity, and having lost it cannot dial the fleet to
report anything.

Reproduced with openssl directly rather than on a live box:

```
step 1 (overwrites int.key)  exit 0
step 2 (needs ca.key)        exit 1
after: key/cert modulus MISMATCH
```

## How it was found

Not by an incident. `docs/agent-authority.md` in the ops repo had just nominated
`RotateCerts` as the best candidate for the agent to execute autonomously — one
cheap unambiguous signal, an idempotent repair, hot-reload so a wrong rotation
costs a certificate and no downtime. Reading `rotate_certs` to implement that
showed two things the nomination had got wrong: the repair is **fleet-wide**
rather than per-subject, and it cannot run where the agent runs at all.

The absence of executor support for `RotateCerts` — the "gap" that document
wanted to close — is what has been preventing this.

## The fix, and which half does what

Measured separately, because they are not the same fix:

- **Staging prevents the damage.** Every leaf is minted into `{d}/.rotate` and
  moved into place only after all three pairs exist and both EKU asserts pass.
  With the guards deliberately disabled, the leaf key now survives a missing
  `ca.key` untouched — nothing writes a final filename until every step has
  succeeded. This also covers the causes nobody enumerated: a full disk, a
  missing openssl, a bad SAN in `edge_sans`.
- **The guard makes the refusal legible.** Without it the operator gets
  `cert step failed: printf ... openssl x509 -req ...` and has to work out that
  a missing CA key is behind it. With it: *"no CA private key at …/ca.key —
  this box can verify the fleet's certs but cannot sign new ones."*

A bonus from staging: the co-processor and mesh EKU asserts (ADR-0010 D2) now
read the **staged** certificates. They used to fire after a wrong-EKU leaf was
already live and already reloading; the same assert is now a gate instead of a
post-mortem.

Residual, stated rather than hidden: the move is per-file, so a watcher polling
between a leaf's key and its cert can still observe a mismatched pair. That
window is two renames instead of several seconds of openssl, and the next poll
is correct. Closing it entirely needs components to load one bundle, which is a
format change and not this fix.

## The check that would have passed for the wrong reason

The first draft of the drill asserted the refusal by `grep -q "ca.key"`.
openssl's own failure echoes the command line, which contains
`-CAkey …/ca.key`, so that grep passes **with the guard removed entirely** —
measured, not assumed. It now greps the guard's own words, and asserts the
staging property separately, because deleting either half leaves a real defect
and an exit-code check would cover neither.
