# BUG-0235: through the proxy, JSON.NUMINCRBY dropped a whole float's `.0` (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by `fmt_json_double_spells_a_double_as_the_seats_json_does`
(`flint-resp`), which pins the seat's spellings, and by `proxy_conformance_drill`,
which failed on BUG-0234's corpus case before this fix.
**Severity:** low. A RESP2 client through the proxy read `[3]` where the seat
and RedisJSON answer `[3.0]`, so a JSON parser saw an integer where the
document holds a float. Every client reaches Flint through the proxy.

## What happened

BUG-0234's first gate failed `proxy_conformance` and the RESP3 conformance
runs on one step, `JSON.NUMINCRBY jn $.i -0`. The seat answered `[6.0]`, the
proxy `[6]`. Measured on 2026-10-08 against the seat and RedisJSON 8.2.8:

```
JSON.SET j $ '{"i":1}'; JSON.NUMINCRBY j $.i 2.0   seat [3.0]   RedisJSON [3.0]   proxy (RESP2) [3]
```

The proxy speaks RESP3 to its seats. NUMINCRBY's two dialects differ in kind,
JSON text under RESP2 and a typed array under RESP3, so the proxy rebuilds
the RESP2 text from the typed reply (`json_numincrby_resp2`, ADR-0055). It
rendered each double with `fmt_double`, which spells a double as Redis does,
without `.0`. The conformance runner normalizes RESP3 replies with the same
function, which is how the RESP3 runs failed too.

## The fix

The rebuild renders a double with serde_json, the library the seat writes
that text with, so the proxy spells it exactly as the seat does: `3.0`,
`-0.0`, `1e+16`, `0.00001`. A hand-written copy of the rules is not enough.
serde_json's formatter and Rust's `{:e}` pick different digits where two
shortest spellings tie (`900719925474099.25` is `…099.2` in serde_json and
RedisJSON, and `…099.3` in `{:e}`). `flint-resp` takes serde_json, already
in the workspace, for this one function. On 6,000 random NUMINCRBY and
NUMMULTBY calls, the proxy's text and the seat's now match byte for byte.
