# BUG-0236: JSON errors were Flint's words, not RedisJSON's (FIXED 2026-10-08)

**Status:** **FIXED 2026-10-08**. Held by `json_errors_take_redisjsons_words_and_order`
(`commands.rs`, every case replayed against RedisJSON 8.2.8 with the same
answers; ten mutants, each killed), a `json_path` unit test for the path
spelling, a `flint-resp` test for the proxy's rebuild, and the corpus case
"json errors are redisjson's words", which the RedisJSON module passes
(`tools/redisjson_compare.sh`).
**Severity:** low per reply, broad in reach. No reply changed what a
document holds. In a 10/8 inventory (every JSON command × five document
shapes, a missing key and a key of another type × 51 paths × their
arguments), 6,554 refusals were worded differently from RedisJSON's.
Some of Flint's texts dropped the "does not exist" that client code
matches on.

## What differed

Where both refuse, Flint answered in its own words:

| Command | Flint | RedisJSON |
|---|---|---|
| `JSON.GET d .x` | `ERR Path does not exist` | `ERR Path '$.x' does not exist` |
| `JSON.ARRAPPEND d .a 1` | `ERR path does not hold an array` | `ERR Path '.a' does not exist or not an array` |
| `JSON.NUMINCRBY d .s 1` | `ERR path does not hold a number` | `ERR Path '$.s' does not exist or does not contains a number` |
| any command on a string key | `WRONGTYPE Operation against …` | `Existing key has wrong Redis type` |
| `JSON.SET d $.a bad` | `ERR value is not valid JSON` | `expected value at line 1 column 1` |
| `JSON.ARRINSERT d $.b x 1` | `ERR value is not an integer or out of range` | `Couldn't parse as integer` |
| `JSON.SET d $.b[5] 1` | `ERR path does not fit the document's shape …` | `ERR array index out of range` |
| `JSON.SET d $..a 1 NX` | `ERR a multi-match path replaces existing values …` | `Err wrong static path` |

And it checked things in a different order. RedisJSON reads the key before
the path, so on a missing key a path that does not parse answers what the
missing key answers: nil from GET, `could not perform this operation…`
from the writers. A key of another type wins over everything except the
arguments some commands read first. Flint parsed the path first.

## The fix

Each error is RedisJSON's text and comes in its order, measured against
the module on this Mac:

- **The path is named** as each command names it. ARRAPPEND and ARRLEN
  echo it as written, and so does JSON.GET for the first missing one of
  several paths; the rest rewrite a legacy path the module's way,
  `.a` and `a` to `$.a`, `.` to `$`, `[0]` to `$.[0]`
  (`flint_resp::json_fixed_path`, shared with the proxy, which rebuilds
  NUMINCRBY's legacy refusal for a RESP2 client).
- **The key is read first** (`json_load`): another type is
  `Existing key has wrong Redis type`, and a missing key answers before its
  path is parsed (`Path::unread`).
- **Arguments in the module's order.** JSON.SET reads its options, the
  key, the value, a missing key's XX, then the path. MERGE reads the key,
  the value, then the path. ARRAPPEND, ARRINDEX, ARRINSERT and ARRTRIM read
  their arguments before the key. MSET reads each triple in turn: its key,
  where it lands, then its value. STRAPPEND reads its value only at a
  string, and names a non-string value as RedisJSON does (`found [1]`).
- **Values in serde's words**, with `ERR ` where RedisJSON puts it
  (ARRINDEX, STRAPPEND) and without where it does not.
- **TYPE, OBJKEYS and OBJLEN** answer nil for a legacy path that does not
  parse, and MGET answers nil for each key, as the module does.

Not copied, and listed in `command-support.md` under "Where we differ from
RedisJSON": a path that does not parse is still `ERR malformed JSON path`,
because RedisJSON's texts there come from its parser generator; the
deliberate differences keep their own texts. Also listed there are three
behaviours the inventory found, which change what is stored and so are not
error texts: RedisJSON's SET, MSET, MERGE, ARRAPPEND and ARRINSERT accept
a value with trailing text (`1 x` is stored as `1`); a negative index past
the start of an array takes the first element (`JSON.DEL d $.b[-9]`
deletes `b[0]`); and JSON.SET takes a `FORMAT` option.
