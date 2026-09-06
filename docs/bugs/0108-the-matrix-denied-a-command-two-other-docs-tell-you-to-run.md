# BUG-0108 — the command matrix denied a capability two other pages tell you to use (FIXED 2026-09-06)

**Status: FIXED 2026-09-06**, and held by
`assert_guides_only_name_documented_commands` in `tools/gates.sh`. Found
2026-09-06 by continuing the doc audit that produced BUG-0103 and BUG-0107 ·
Severity: medium — the wrong direction of wrong. The matrix did not merely
omit a command; it stated the capability does not exist, and the workaround
that follows from believing it is a restart.

## Symptom

`docs/command-support.md` said:

> There is no substitute for `CONFIG` or `SHUTDOWN`.

`FLINTCONFIG` is a substitute for `CONFIG`. With no arguments it dumps the
live tunables; with `<key> <value>` it hot-reloads one with no restart —
`wal-fsync-ms`, `lag-soft-ms`, `lag-hard-ms`, `wal-headroom-seq`,
`min-replicas-to-write`, `max-conns`, `migrate-rate-bytes`,
`fullsync-rate-bytes`, `write-deadline-ms`, `gc-sweep-ms`. Two other
customer-facing pages already said so: `slo.md` ("hot-settable via
`FLINTCONFIG`") and `space-reclaim.md` ("hot-settable via `FLINTCONFIG`").

So the three pages disagreed, and the one a reader is directed to as
authoritative was the wrong one. An operator following it takes a **restart**
— an outage on that seat — to change a value that is hot-settable.

## Four more the matrix omitted

The same audit found five commands the operating guides tell readers to run
and the matrix did not mention at all:

| Command | Told to run it by |
|---|---|
| `FLINTNSBYTES` | space-reclaim.md — per-tenant attribution, "whose data to trim" |
| `FLINTCONFIG` | slo.md, space-reclaim.md |
| `PROXYSTATS` | self-hosting.md — "on each proxy" |
| `PROXYLATENCY` | self-hosting.md, tenant-guide.md |
| `PROXYHOTKEYS` | tenant-guide.md — "**your** hot keys — no portal required" |

The last two are addressed to TENANTS, not operators, so they are missing
from a client command matrix that says of itself:

> If you are deciding whether Flint implements something, the list to consult
> is **Supported** above, or simply send it to a server and read the reply.

That sentence is the defect's whole weight. A page that claims to be the
authority has to be one.

## Fix

The `CONFIG` sentence now says what `FLINTCONFIG` does, and records that the
page was wrong until today — the failure mode is worth naming where someone
who once read the old sentence will see it. `SHUTDOWN` genuinely has no
substitute and still says so.

A new **Operator and per-tenant commands** table lists the five, with where
each is served, plus a note that `PROXYLATENCY`/`PROXYHOTKEYS` are per-tenant
and that `FLINT*` is seat-local because the proxy refuses the prefix.

The note also states what is deliberately NOT listed: `FLINTPROMOTE`,
`FLINTFENCE`, `FLINTLEASE`, `FLINTSYNC` and the rest are control-plane
machinery that `flintctl` and the controller drive. architecture.md and
failover.md name them as MECHANISM, not as things to send.

## The check, and the reason it is not an exclusion list

`assert_guides_only_name_documented_commands` classifies every `docs/*.md` as
a GUIDE (a reader is meant to run what it names) or as INTERNALS (it
describes mechanism), and fails if a page is in neither. Nine are guides;
three are internals — architecture.md, capacity-model.md, failover.md, which
between them name `CPLEASE`, `CPWATCH`, `FLINTDEMOTE`, `FLINTMIGRATEIN`,
`FLINTSLOTHEAT` and `FLINTSLOTSTATS` to explain how the system works.

Forcing every page into one list or the other is what keeps this from being
the shape BUG-0086 argues against: a new doc cannot be quietly uncovered, and
a renamed or deleted one fails rather than dropping out. Which token counts
as a command comes from the server and proxy sources — a doc-only heuristic
invents findings, which it did three times during BUG-0103.

All four failure branches were confirmed by planting them: a command removed
from the matrix, an unclassified new page, a renamed page, and a deleted one.
