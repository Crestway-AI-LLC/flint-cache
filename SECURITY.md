# Security policy

## Reporting a vulnerability

**Please report privately rather than opening a public issue.**

Use [GitHub's private vulnerability reporting][pvr] on this repository — the
"Report a vulnerability" button under the Security tab. It opens a channel
visible only to the maintainers.

If that button is not available to you for any reason, open a public issue
containing **nothing but a request for a private channel** — no version, no
reproduction, no description of the weakness — and a maintainer will open one.

Helpful in a report, none of it required:

- the version, from `flintctl --version` or `flint-server --build-version`
- whether the build came from a release bundle or from source
- a reproduction against `tools/quickstart.sh`, which stands up a full
  cluster — control plane, replicated pair, proxy, controller — in one command

[pvr]: https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability

## What is in scope

Flint is pre-1.0 and is not yet running anyone else's production data, so
treat this as a statement of intent rather than a track record.

In scope: anything that crosses a boundary the system claims to enforce —
one tenant reaching another tenant's keys, an unauthenticated client reaching
data, a client reaching an admin verb, mesh traffic that is not mutually
authenticated, a token recoverable from anything at rest, or an artifact that
verifies against the published signing key without having come from the
release pipeline.

Out of scope: findings that require an attacker who already has root on a
node, denial of service by resource exhaustion against a deployment you
control, and anything that depends on running with `disposable on`, which
exists precisely to mark a fleet as throwaway.

[docs/security.md](docs/security.md) is the posture in full — the trust
boundaries, what mutual TLS covers, how tokens are stored, and what is
deliberately not defended.

## Verifying what you downloaded

Every release from `v0.1.0-rc.28` onward is signed with
[minisign](https://jedisct1.github.io/minisign/), key ID
`6A8EB70496EA74A1`. `minisign.pub` is in the repository root and attached to
each release:

```sh
minisign -Vm flint-<version>-linux-x86_64.tar.gz -P "$(tail -1 minisign.pub)"
```

Pin that key once, out of band, and check later releases against your pinned
copy — a key fetched from the same place as the artifact proves nothing about
the artifact. [docs/release-signing.md](docs/release-signing.md) explains why
the sha256 in `manifest.json` is not a substitute.

A signature that does not verify is a security report in itself. Use the
private channel above.
