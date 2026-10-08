# BUG-0234: JSON.NUMINCRBY and JSON.NUMMULTBY read their number with Rust's parsers, not as JSON (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by steps in
`json_numincrby_keeps_integers_exact` (`commands.rs`) and the corpus case
"numincrby reads its number as json", which the RedisJSON module passes
(`tools/redisjson_compare.sh`).
**Severity:** low. Some spellings RedisJSON refuses changed the document
here, and `-0` left an integer where RedisJSON stores a float.

## What happened

Found by a JSON differential against the RedisJSON 8.2.8 module
(2026-10-08), on `{"i":5}`:

| Argument | Flint | RedisJSON |
|---|---|---|
| `+1`, `01`, `.5`, `1.` | applied (`6`, `6`, `5.5`, `6.0`) | refused, document unchanged |
| ` 1` | refused | `6` |
| `-0` | `5`, an integer | `5.0`, a float |
| `true` | `ERR value is not a number` | `bad input number` |

RedisJSON parses the argument as a JSON value. Flint parsed it with
`str::parse::<f64>` and `str::parse::<i64>`, which take spellings JSON does
not (a leading `+`, leading zeros, a bare `.5` or `1.`), refuse
surrounding whitespace that JSON allows, and read `-0` as the integer 0.
serde reads `-0` as a float, so RedisJSON's `5 + -0` is the float `5.0`.

## The fix

The argument is parsed with serde_json, as RedisJSON parses it. A parse
failure answers serde's text, as RedisJSON's does
(`ERR expected value at line 1 column 1`). A value that is not a number
answers `bad input number`, and an infinite result `result is not a
number`, both RedisJSON's words.

As in RedisJSON, a bad number is refused only where it would be applied.
On a path that selects no number, the reply is what it always is, `[null]`
for a non-number and `[]` for nothing, whatever the argument. Flint used to
refuse the argument before it looked at the path.
