# BUG-0241: SCAN's TYPE filter matched no JSON or Bloom key (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by `scan_type_finds_json_and_bloom_keys`
(`flint-server/src/commands.rs`) and a step of the corpus case "TYPE and
SCAN TYPE name a filter as RedisBloom does", which RedisBloom 8.2.8 also
passes.
**Severity:** low. A type-filtered scan silently skipped two types.

## What happened

Found while weighing the `TYPE` name for filters (2026-10-08):

```
BF.ADD bf a; JSON.SET doc $ 1
TYPE bf -> bloom      SCAN 0 TYPE bloom -> (nothing)
TYPE doc -> json      SCAN 0 TYPE json  -> (nothing)
```

`cmd_scan` mapped the TYPE argument through its own list of names, which
had the five core types and nothing else, so every other name matched
nothing.

## The fix

SCAN looks the name up in `ValueType::ALL` by the name TYPE answers, so the
two read one list. With TYPE now answering the modules' names (`MBbloom--`
for a filter, `ReJSON-RL` for a document, both by Jeff's decision the same
day), `SCAN … TYPE MBbloom--` finds filters and `SCAN … TYPE ReJSON-RL`
documents, case-insensitively, as on Redis with the modules.
