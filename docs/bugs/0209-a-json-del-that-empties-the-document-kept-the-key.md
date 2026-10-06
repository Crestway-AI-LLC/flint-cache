# BUG-0209: a JSON.DEL that empties the document kept the key (FIXED 2026-10-06)

**Status:** **FIXED 2026-10-06**, with ADR-0054. Held by the unit test
`json_del_that_empties_the_document_deletes_the_key` and the corpus case
"DEL counts each location once, and emptying the document deletes the key",
which is checked against RedisJSON.
**Severity:** low. No data is lost or corrupted. A client that tests
`EXISTS`, or that writes a sub-path after emptying the document, saw a
different answer from RedisJSON's.

## Why

RedisJSON deletes the key when a `JSON.DEL` of a path leaves the document an
empty object or array. Its own `testDelCommand` asserts it, and v8.2.8 does
it: `JSON.SET e $ '{"a":1}'`, then `JSON.DEL e $.a` answers 1, and `EXISTS
e` answers 0. Flint stored `{}` and kept the key. After that, Flint answered
`JSON.SET e $.b 1` with OK and RedisJSON with "new objects must be created
at the root".

The July oracle run did not catch this because no corpus case emptied a
document. ADR-0054's DEL cases did, and RedisJSON disagreed.

## The fix

`json_save_or_drop` deletes the key when a path delete leaves an empty
object or array at the root, and saves the document otherwise. An emptied
member (`{"a":[]}`) is not an emptied document, and the key stays.
