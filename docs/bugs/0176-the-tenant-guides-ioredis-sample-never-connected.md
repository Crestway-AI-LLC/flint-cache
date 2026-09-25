# BUG-0176: the tenant guide's ioredis sample never connected, because the proxy could not answer INFO (FIXED 2026-09-24)

**Status:** **FIXED 2026-09-24**, found the same day (ops OPS-0314's follow-up,
surveying which commands real clients send on their own). Not in
v0.1.0-rc.77: it ships with the next release.
**Severity:** high for onboarding. `docs/tenant-guide.md` shows ioredis, the most
widely used Node client, as one of three samples. Written as the guide writes
it, that sample never served a command against a Flint cluster.

## What was measured

On a gate box, through the proxy, 2026-09-24:

| client | as the tenant guide constructs it |
|---|---|
| ioredis 5.11.1 | **fails:** `ERR unknown command 'INFO', with args beginning with:` |
| ioredis 5.11.1, `enableReadyCheck: false` | works |
| node-redis 5.12.1 | works |
| go-redis v9.22.0 | works |

## The mechanism

ioredis's `enableReadyCheck` defaults to `true`. Its ready check sends `INFO`
before the first command, and on any error except `NOPERM` its connect handler
calls `recoverFromFatalError`: disconnect, reconnect, and check again. `INFO`
was implemented by no component. The proxy lists it in `NO_KEY`, which only
says it carries no routing key, so it was forwarded to pair 0's master, which
answered `ERR unknown command`. The client never became ready.

Nothing caught it because `tools/client_compat_drill.sh` ran redis-py and
node-redis only. The guide's other two samples had never been run against a
cluster. node-redis and go-redis send their own unimplemented handshake
(`CLIENT SETINFO`) and ignore the error; ioredis does not ignore this one.

## The fix

- **The proxy answers `INFO` itself** (`info_reply` in `flint-proxy`). It does
  not ask a seat: a seat's figures are shared by every tenant on its pair, and
  the fleet counters are the operator's (`PROXYSTATS`). The reply carries a
  Server section (`redis_mode:standalone`, `flint_version:<build>`) and a
  Persistence section (`loading:0`, the field ioredis reads). Sections filter
  as in Redis, and an unknown section is empty, not an error.
- **No `redis_version`.** Advertising a version is a claim about the whole
  command surface, and that is a product decision, not a field. (Decided
  2026-09-25: it stays out.) A client
  that insists on a version is not helped by this fix. Spring Boot's Redis
  health check is not one: it reads `INFO server` and reports the version as
  `unknown` when the field is absent (`DataRedisHealth.up`, read 2026-09-24),
  so it now reports UP where it used to fail on the error.
- **`client_compat_drill` runs all three samples** with the guide's own
  options: ioredis with its default ready check, and go-redis with defaults,
  beside redis-py and node-redis. CI installs ioredis and pins Go
  (`actions/setup-go`) so neither can quietly turn into a SKIP.

`INFO` at a seat is still unknown; operators use `FLINTINFO` there
(`docs/command-support.md`).
