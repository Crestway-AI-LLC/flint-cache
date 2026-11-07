# BUG-0281: a restore whose copy of an object was cut short, or read other bytes, said OK (FIXED)

**Status:** **FIXED 2026-11-06.** Found the same day by the error-handling
review's area D question, "Does restore verify every file before the node
serves restored data? What happens on an S3 5xx or a truncated body
mid-download?" (`docs/error-handling-review.md`), and measured on release
binaries. In public main (`93dcd0c3`) and v0.1.0-rc.86.
**Severity:** high where it happens: `flint-backup restore` prints `OK
restored` over a copy that is missing data, during the incident a restore
is run for. The trigger is narrower over TLS, which is how real S3 is
reached: rustls already reports a connection closed without TLS
`close_notify` as an error. So the silent case needs a plain-http endpoint
(`FLINT_S3_ENDPOINT=http://...`, for an S3-compatible store), a peer that
closes cleanly part way through a body, or an object that changes between
the two reads.
**Area:** `flint-backup`, `restore` and the S3 store's body reader.

## What happens

- **Restore reads every object twice.** `load_verified` reads each one to
  check its sha256 against the manifest, and refuses a set that does not
  match before anything is written (ADR-0011 D2). Then the copy reads each
  object again, into the destination. That second read was not checked.
- **The S3 body reader took a closed connection for the end of the
  object.** A response that declares a `Content-Length`, or a chunk size,
  and closes before sending it, read as a clean end. So a cut copy was a
  short file, written without an error.
- **RocksDB opens some short files.** A short SST or `CURRENT` fails at
  open, saying "Corruption", which sends the operator to an intact set. A
  short MANIFEST is read as far as its last whole record. That is an older
  version of the database, and RocksDB deletes the newer SSTs as obsolete.
  A short OPTIONS file is not read at all.

## Measured, 2026-11-06

Release binaries of public `93dcd0c3` on a laptop, restoring from an S3
stand-in over plain http (`tools/lib/s3_cut_stub.py`) that spoils the
second GET of one object, which is restore's copy. Each set held 20,000
keys from one rocks master. For the MANIFEST rows it was cut after four
flushes, so its MANIFEST records four SSTs; for the others, after one.

| the copy's read of | restore says | a seat on the copy serves |
|---|---|---|
| nothing spoiled (control) | `OK restored` | 20,000 keys |
| the MANIFEST, cut at 40% or 50% | `OK restored` | 5,000 keys |
| the MANIFEST, cut at 60% or 70% | `OK restored` | 10,000 keys |
| the MANIFEST, cut at 80%, 90% or 95% | `OK restored` | 15,000 keys |
| the MANIFEST, cut at 20% or 30% | exit 1: `had no role row to scrub ... this set is not what the manifest says it is` | |
| an SST, one byte changed, full length | `OK restored` | 10,608 keys |
| an OPTIONS file, cut at half | `OK restored` | every key; the file is half its size |
| an SST, cut at half | exit 1: `Corruption: Sst file size mismatch` | |
| `CURRENT`, cut at half | exit 1: `Corruption: CURRENT file does not end with newline` | |

The two refusals at 20% and 30% came from the restore's scrub finding no
role row, a check made for another reason, and they blame the set.

## The fix

- **The body reader errors when the connection closes early.** A
  `Content-Length` body or a chunk cut short is `UnexpectedEof`, `body cut:
  the connection closed with N of its Content-Length bytes unread`. The
  scheduled `verify` and `restore-ns`'s staging read through it too.
- **Restore checks what it writes.** `copy_verified` hashes each object as
  it is written and compares its length and sha256 with the manifest the
  verify checked. A copy that is not the verified object is refused,
  naming it and both hashes.
- **The refusal says what was left.** `copy <name>: ...; <dir> holds a
  partial restore, so remove it before running restore again`. Restore
  only ever creates its destination (ADR-0011 D3), so a re-run needs it
  gone.

## Acceptance

`restore_cut_drill`, with the real `flint-backup` against the stub:
- **the control:** an untouched set restores OK, and a seat on it serves
  all 20,000 keys;
- **the MANIFEST's copy cut part way:** refused, naming the object, the
  bytes never received, and the partial restore;
- **an SST's copy with one byte changed:** refused, comparing the copy's
  sha256 with the verified set's.

On the binaries of `93dcd0c3` the drill fails at its MANIFEST case: `OK
restored`, and a seat on the copy serves 5,000 of 20,000 keys.

Unit tests:
- `a_copy_that_is_not_the_verified_object_is_refused`;
- the S3 store's `a_body_cut_before_its_content_length_is_an_error_not_the_end`;
- `a_chunked_body_cut_inside_a_chunk_is_an_error_not_the_end`.
