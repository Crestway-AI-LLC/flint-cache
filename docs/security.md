# Security posture

What Flint protects, how, and — the part worth reading — what it does not do,
so you can decide what your platform has to supply.

## In transit

**The internal mesh is mutually authenticated TLS, everywhere.** Node↔node
replication, migration and cutover; proxy↔backend; proxy↔control-plane; and
the Raft RPCs between control-plane seats. There is no plaintext internal hop
to turn off, and no "trusted network" assumption.

Internal dials use a fixed `ServerName` (`flint-internal`) rather than the
dialed address, so one leaf certificate serves the whole mesh and
distribution is a file copy rather than a per-host PKI. Certificates
**hot-reload**: `flintctl rotate-certs` replaces them under live traffic, and
`cert_reload_fleet_drill.sh` proves the fleet keeps serving through it.

**The client edge is TLS when you ask for it** — `client-tls on` in the
inventory, terminated at the proxy with a certificate carrying real SANs
(`edge-san`). It is plaintext by default, which is right for a loopback
quickstart and wrong for anything else. Turn it on.

`cert_days_remaining` is exported by `flint-exporter` and surfaced in the info
commands, so expiry is a metric you can alert on rather than an outage you
discover.

## Credentials at rest

| Secret | How it is stored |
|---|---|
| Tenant tokens | **SHA-256 digests only.** The control plane never holds a plaintext token; authentication compares digests. |
| Mesh private key | `certs/int.key`, mode **0600**, inside a root-only statedir. `ca.crt` and `int.crt` are 0644 — they are public by nature. |
| Admin token | Held by the control plane, rotatable in place (`rotate-admin`) with a dual-version window so rotation is not an outage. |
| Release signing key | **Never on a build host.** It exists as a CI secret and an offline backup; `*.key` is gitignored repo-wide. See [release-signing.md](release-signing.md). |

Tenants rotate their own tokens through a dual-version window, so a rotation
does not require coordinating a restart with the application team.

## Data at rest — read this one carefully

**Flint does not encrypt your data at rest.** There is no envelope
encryption, no KMS integration, and no per-tenant key. Data on disk is
protected by the volume it sits on, and supplying that is the operator's job.

- **On AWS Nitro instance-store families (i4i and similar)**, the NVMe
  instance store is encrypted at rest by the hardware with keys the instance
  cannot export. Confirm the current guarantee for your specific instance
  family in AWS's documentation rather than taking this page's word for it.
- **On your own hardware**, use full-disk encryption — LUKS or dm-crypt. Cover
  the **whole statedir**, not just the data directories: node data, the WAL,
  checkpoints in `snaps/`, and the control-plane state file all contain
  tenant data or credentials.
- **Backups** leave the host, which is where encryption matters most. Flint
  delegates this to the object store (SSE-KMS or the equivalent for your
  provider) — a bucket policy is stronger and far easier to audit than a key
  we would manage ourselves. See [ADR-0011](adr/0011-backup-and-restore.md).

## What Flint does not provide

Stated plainly, because discovering these during a security review is worse
than reading them here:

- **No application-level encryption at rest** (above).
- **No KMS integration**, and therefore no customer-managed keys.
- **No Redis ACL / RBAC users.** Authorisation is per-tenant: a token grants
  access to one namespace, and the tenancy invariant keeps commands scoped to
  it. There is no notion of a read-only user or a per-command grant.
- **No IAM or SSO authentication.** Tokens are the only mechanism.
- **No data-access audit log.** The fleet journal is a typed record of
  *topology* events — promotions, migrations, rotations, with actor and cause
  — not a log of who read which key.

If any of these is a hard requirement for you, say so in an issue describing
the control you need to satisfy; requirements with real buyers behind them get
built, and speculative ones do not.

## Reporting a vulnerability

Please report privately rather than in a public issue — use GitHub's private
vulnerability reporting on this repository, which opens a channel visible only
to the maintainers. Include the version (`flintctl --version`) and, if you
can, a reproduction against `tools/quickstart.sh`, which stands up a full
cluster in seconds.
