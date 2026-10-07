# BUG-0217: a fleet bootstrapped before co-processors could not add one (FIXED 2026-10-07)

**Status:** **FIXED 2026-10-07**. Held by `edge_roll_drill`, which now
deletes the co-processor leaf from a running client-TLS fleet before adding
a `coproc` line, and asserts that the upgrade mints the leaf, serverAuth
only, and leaves the CA, mesh and edge files byte for byte as they were.
With the fix reverted, the drill's upgrade aborts:
`UPGRADE ABORTED rolling vec-7974: did not answer after the binary swap`,
with the data plane already rolled and the proxies not.
**Severity:** high for ADR-0050. Turning vectors on for a running fleet, the
preview's own procedure (ops agent-ops §7c, step 4 on the playground, step 5
on every fleet), failed on any fleet bootstrapped before 2026-08-11, and the
only way to the missing file could break the fleet's edge.

## Why

`flintctl` spawns a co-processor on a TLS fleet with
`--internal-cert <statedir>/certs/coproc.crt`. Bootstrap mints that leaf
since public `2973016` (2026-08-11); `upgrade` never did. On a fleet
bootstrapped earlier the file does not exist, the seat panics on it, and
every `VEC.` command answers `-COPROCUNAVAIL`.

Found 2026-10-07 starting ADR-0050 step 4 on the playground, bootstrapped
2026-07-24. Its `certs` directory has no `coproc.crt`. The step's command
checked for the file before anything else and changed nothing. Without that
check, its upgrade would have aborted part way, as the drill's does with
the fix reverted.

The only other way to the leaf is `flintctl rotate-certs`, which re-signs
every leaf from the fleet's CA, the edge leaf included. The playground's edge
certificate was installed separately (an EC key, dated 2026-09-28) and was
not issued by that CA, so rotating would have replaced the certificate its
tenants verify.

ops agent-ops §7c said a single-host fleet "has the co-processor's
certificate from bootstrap". True only of fleets bootstrapped since August.

## The fix

`ensure_coproc_leaf`, run by `upgrade` before a seat is touched and by
`start`: when the inventory declares a co-processor on a TLS fleet and
`coproc.crt` is missing, it mints that one leaf from the CA, through the
same function bootstrap and `rotate-certs` use, with the same serverAuth-only
assertion, and copies it to remote co-processor hosts. No other leaf is
touched. Without a CA key on the host, nothing is minted: `upgrade` refuses
before touching a seat if a co-processor would run there, and `start`,
which the supervisor runs to restart dead seats, warns and starts the rest.
