# BUG-0272: a TEXT value was split into words at other bytes than RediSearch splits it at, so an exact phrase missed a word before `?`, `/`, `|` or `` ` `` (FIXED 2026-10-22)

**Status:** **FIXED 2026-10-22**, before any release served `FT.*`. ADR-0060's
release N, on public main after v0.1.0-rc.85, had it. **Found by reading
RediSearch's source** for ADR-0063 (`toksep.h`, `tokenize.c`, the query
parser's `parser.y` and `util/strconv.h`), not seen in a run. None of the
three clients' suites writes a phrase that reaches it.
Held by `text_is_split_and_folded_as_redisearch_splits_and_folds_it` in
`crates/flint-vec/src/filter.rs`, and by
`a_filtered_knn_reads_text_words_as_the_index_holds_them` in
`crates/flint-vec/src/ftindex.rs`. Each fails with the old code put back.
**Severity:** low. A wrong answer, but only for an exact phrase,
`@f:("...")`, the one TEXT query `FT.SEARCH` serves, over text with the
bytes below. LangChain's chat history stores each message as TEXT, and a
question ends in `?`.
**Area:** `crates/flint-vec/src/filter.rs`, `tokenize`, `Parser::phrase`
and `extract`; `crates/flint-vec/src/ftindex.rs`, `meta_of`.

## What happened

ADR-0060 matches a phrase against a TEXT value's words, "split as RediSearch
splits them and lowercased". Flint's split differed from RediSearch's:

| | RediSearch | Flint before |
|---|---|---|
| `/`, `\|`, `` ` ``, `?` | end a word | part of the word |
| newline, CR, other control bytes | dropped, joining the bytes either side | ended a word (`\n`, `\r`, vertical tab, form feed) |
| `\` before a separator | keeps the separator in the word, and is dropped | ended the word at the separator, and kept the `\` |
| a NUL byte | ends the value | part of a word |
| a final capital sigma | `σ`, one code point at a time (`unicode_tolower`) | `ς`, Rust's `str::to_lowercase` |

So `@content:("today")` did not match "What's the weather like today?",
whose last word Flint held as `today?`. Nor did `@t:("tcp ip")` match
"TCP/IP".

The query side differed too. RediSearch's parser cuts a quoted phrase with
its own map, which leaves out `$`, and unescapes a word with `rm_normalize`,
which drops a `\` only before punctuation or whitespace. Flint dropped every
`\` while reading the phrase, and then cut it as it cut a document.

## The fix

- **A document's text** is cut and normalized as RediSearch's indexer does
  (`toksep`, `DefaultNormalize`): its separator map exactly, `\` escapes,
  control bytes dropped, and the value ended at a NUL.
- **A phrase** keeps its backslashes as RediSearch's lexer keeps them, and
  is cut and unescaped as its parser does (`toksep2` over
  `ToksepParserMap_g`, then `rm_normalize`).
- **Each word is lowercased** one character at a time.
- **A filtered KNN reads the same words.** An index with a VECTOR field
  hands its vector set each document's values as JSON, which the set reads
  back. A TEXT value went as its words joined by spaces, and was split
  again, so a word holding an escaped separator (`tcp\/ip`) came back as
  two, and a phrase beside a KNN missed it. The words now go as an array,
  read back as they are.

TAG values are not changed: they keep `fold`, and this did not check
RediSearch's TAG folding.

## Not changed

- **Stop words** are still kept in a TEXT value. RediSearch leaves them out
  of its index, so its quoted phrase with one in it matches nothing. That is
  part of full-text search, ADR-0063, not this fix: `docs/command-support.md`
  says Flint's TEXT has no stop words.
- **Bytes that are not UTF-8** are lowercased as ASCII, as before.
  RediSearch decodes them leniently with no check.
