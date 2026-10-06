# ADR-0054: JSONPath multi-match for the JSON type

Status: **ACCEPTED 2026-10-05** (Jeff chose "JSONPath multi-match" as the
next feature, after the production bugs). Built; the semantics below are
RedisJSON v8.2.8's, checked reply by reply against the module, except where
"Where we differ" says otherwise.

## Context

The JSON type took RedisJSON's two path dialects in July, along with its
reply shapes: a `$` path answers a container of matches, a legacy path the
bare value. Only single-match paths were evaluated. `$..a`, `$.a[*]`, `$.*`,
slices and filters were refused as UNSUPPORTED. The container shape was
chosen so that multi-match could be added later without changing any reply
type, only the number of elements.

Clients use these paths for ordinary work: `$..price` to read every price in
an order, `$.items[?(@.qty == 0)]` to delete sold-out lines, `$.*.updated` to
touch every member. Before this change, an application doing any of that
against Flint got an error where RedisJSON gave an answer.

## Decision

A `$` path is parsed into selectors: member, index, wildcard, union
(`['a','b']`, `[0,-1]`), slice (`[start:end:step]`, step positive),
recursive descent (`..`), and filter (`?(...)`). A path made only of members
and indexes is still *definite* and keeps the existing code, byte for byte.
Any other path is *indefinite*. `select` evaluates it to the concrete
locations it names, in document order: descent is pre-order with a node
before its descendants, a union is in the order written, and duplicates
are kept.

What each command does with an indefinite path:

| command | answer | write |
|---|---|---|
| GET | a JSON array of the matched values | — |
| TYPE, ARRLEN | one element per match, nil where ARRLEN meets a non-array | — |
| NUMINCRBY | one number per match, null for a non-number | every numeric match; a location named twice is incremented twice |
| ARRAPPEND | one new length per match, nil for a non-array | every array match; a location named twice is extended twice |
| DEL | how many locations were removed | each location once, last index first; one inside another removed one goes with it, uncounted |
| SET | OK, or nil under XX when nothing matched | every match replaced; nothing created |

The SET rule is RedisJSON's (`find_paths`): it adds a value only on a
*static* path, one naming at most one location. An indefinite SET matching
nothing is refused, NX with one is always refused (NX only adds), and XX
matching nothing is nil. A write that fails at any match (an integer
overflow, a non-finite result) fails the whole command and stores nothing.

Filters take `==`, `!=`, `<`, `<=`, `>`, `>=`, `&&`, `||`, `!`,
parentheses, definite `@` and `$` paths, and string, number, `true`,
`false` and `null` literals. Equality compares numbers by value (`3 ==
3.0`). An absent operand is equal to nothing, another absent one included;
`!=` is the negation of `==`. Ordering holds only between two numbers or two
strings. RFC 9535 treats two absent operands as equal; RedisJSON does not,
and RedisJSON is the contract here. Filter nesting is capped at 32 levels,
so a hostile path is refused instead of recursing the stack.

The legacy dialect stays single-match: its multi-match constructs remain
UNSUPPORTED.

## Where we differ from RedisJSON

Each is a corpus case of its own, listed in `tools/redisjson_compare.sh`:

1. **Legacy-dialect multi-match is refused.** RedisJSON answers the first
   match of `..a` and `.a[*]` for reads, and for writes acts on every match
   while answering the last. Those are two more contracts for a dialect
   RedisJSON keeps for compatibility. A client that wants multi-match
   writes `$`, which behaves the same on both.
2. **A regex filter (`=~`) is refused**, for now. Matching RedisJSON's
   pattern dialect (anchoring, flags, a pattern taken from a path) is a
   separate piece of work. It would also put a regex engine into the
   request path, and that needs its own look at cost bounds.
3. **A multi-match operand inside a filter (`@..a`, `@.*`) is refused.**
   What `@.* == 1` means (any member? all members?) is not settled across
   JSONPath implementations. Refusing it keeps a filter's answer unambiguous.

The other differences are wider acceptance, not different answers: `!` and
exponent literals (`1e3`) are taken where RedisJSON reports a syntax error.

## Found on the way

Building the oracle run turned up two defects in the single-match code,
filed and fixed with this change:
- BUG-0208: NUMINCRBY on an integer added in f64, so `+0` changed an
  integer above 2^53 and an overflow saturated.
- BUG-0209: a DEL that emptied the document kept `{}`, where RedisJSON
  deletes the key.

The oracle scripts themselves had stopped working.
`tools/redisjson_compare.sh` and `tools/redisbloom_compare.sh` run the corpus
with `--foreign`, which since ADR-0051 (2026-09-26) has included the sandbox
family. Against a stock Redis, the sandbox's runaway-script case never
returns, and the server answers BUSY from then on, so both scripts hung.
`--foreign` now skips that family and says so.

## Verification

- **Unit tests:** `json_path`'s parser and selector, and the command
  handlers (`json_multimatch_reads_and_writes_every_match`).
- **Conformance corpus:** four oracle-checked cases (reads, writes, DEL,
  BUG-0208), plus one case per divergence.
- **Oracle:** `tools/redisjson_compare.sh` against RedisJSON v8.2.8, built
  from source and loaded into Redis 8.2.8.
- **Differential:** an ad hoc run of 116 commands against both servers
  agreed on all but the listed divergences and error-text wording.

## Consequences

- Clients that read or write many locations with one `$` path work as they
  do on RedisJSON.
- A multi-match write rewrites the document's one row, as any sub-document
  write does. Its cost is the document's size, not the number of matches.
- The refused constructs stay refused with UNSUPPORTED, distinct from a
  malformed path, so a client can tell "not here" from "wrong".
