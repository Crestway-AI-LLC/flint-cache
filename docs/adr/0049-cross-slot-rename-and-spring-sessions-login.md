# ADR-0049: Cross-slot RENAME, and Spring Session's login

Status: **ACCEPTED 2026-09-25: option A**, as recommended (Jeff). Cross-slot
`RENAME` stays refused; both configurations are documented in
`command-support.md` and the tenant guide. No code changed.

## Context

`RENAME` and `RENAMENX` are same-slot only (`command-support.md`): across slots
the command would be a move between pairs, and no step is atomic across pairs
(ADR-0012). A cross-slot `RENAME` is refused with `CROSSSLOT`.

**Measured 2026-09-25**, through the proxy on a two-pair fleet on a gate box,
Spring Session 3.4.1 (`RedisSessionRepository`) on Spring Data Redis 3.4.1 and
Lettuce 6.5.1, default options, with BUG-0182's `HMSET` in place:

| call | what it sends | what happened |
|---|---|---|
| `save`, `findById` | `HMSET`, `HGETALL`, `PEXPIRE` | works |
| `changeSessionId` then `save` | `RENAME spring:session:sessions:<old> spring:session:sessions:<new>` | **refused, CROSSSLOT**: the two ids hash to different slots |

Spring Security calls `changeSessionId` at every login by default: it is its
session-fixation protection on any Servlet 3.1+ container. So an application
keeping its sessions in Flint through Spring Session cannot log anyone in.
The failure is loud (an exception carrying the CROSSSLOT text), not silent.

Two configurations were measured working on Flint as it stands:

1. **A hash tag in the key namespace**:
   `spring.session.redis.namespace={spring}:session`. Every session key then
   hashes to one slot, so the rename is same-slot. The price: every session
   lives on one pair, whatever the fleet's size.
2. **Spring Security's `migrateSession` strategy**
   (`sessionFixation(f -> f.migrateSession())`). It creates a new session,
   copies the attributes and invalidates the old one, so the repository sees
   create, save and delete, never `RENAME` (measured as those repository
   calls). Sessions stay spread over the fleet, and the old id is dead after
   login, which is the property fixation protection exists for.

## Options

**A. Keep refusing, and document both configurations** where a Spring user
looks: `command-support.md` beside the rule, and the tenant guide.

**B. Emulate a cross-slot `RENAME` in the proxy, for strings and hashes.** Read
the source's value and TTL, delete the source, then write the destination and
its TTL on the destination's pair. Delete before write, so a failure can never
leave the old session id alive; a failure between the two loses the key (the
user is logged out). What it cannot give is `RENAME`'s contract, one atomic
step: a reader can find neither key for a moment, a write to the source
between the read and the delete is lost, and a hash destination is replaced in
two steps.

**C. B for every type.** The same trade, with a reader and a writer per type.

## Recommendation

**A.** `RENAME`'s contract is its atomicity, as `MSET`'s is, and ADR-0048
declined to split `MSET` for that reason. The one flow measured to need a
cross-slot rename has two supported configurations, both measured, and the
second keeps the sessions spread and the fixation protection whole. B would
make that flow work unconfigured, at the cost of quietly weakening `RENAME` for
everyone else; the common other use, building a key and renaming it over a
live one, relies on exactly the atomicity B gives up.

## Verification

- The documentation names both configurations and says which keeps sessions
  spread.
- No code changes, so no drill. If B is chosen instead: a two-pair drill for
  strings and hashes that checks the TTL survives and the source is gone
  before the destination appears, and that a destination pair already down
  fails the call before the source is touched.
