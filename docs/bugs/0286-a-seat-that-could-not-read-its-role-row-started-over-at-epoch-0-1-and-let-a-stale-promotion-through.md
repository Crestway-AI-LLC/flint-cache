# BUG-0286: a seat that could not read its role row started over at epoch (0,1) and let a stale promotion through (FIXED)

**Status:** **FIXED 2026-11-11.** Found the same day by the error-handling
review's area A sweep, among the readers BUG-0284 left acting outside an
engine watch (`docs/error-handling-review.md`), and measured on a release
binary of branch `b0285` (main `006a103c` plus BUG-0285, which does not touch
the role).
**Severity:** high. The role epoch is the fence every role command and every
rejoining copy is measured against (ADR-0018, BUG-0280). A seat that lost it
took whatever role its flags named and the lowest epoch there is, and a
promotion or demotion from any stale actor then passed. It needs the block of
the seat's store that holds the role row to fail its checksum, or the row
not to decode: a bad sector, a bit flipped at rest.
**Area:** `flint-storage` (`manifest::read_role`, `set_role`) and
`flint-server` (startup, `FLINTPROMOTE`).

## What happens

- **`manifest::read_role` answered `None` for a row it could not read.** It
  goes through `Kv::get`, which answers `None` for RocksDB's error
  (BUG-0284), and it also answered `None` for a row that did not decode.
- **Startup took `None` for a fresh node.** It claimed a role at epoch
  (0,1): replica if the command line named `--replica-of`, master if not.
  `set_role`'s fence read the row the same way, measured (0,1) against
  `Epoch::ZERO`, and stored it. So a promoted master restarted with a stale
  `--replica-of` came back a replica (measured below), and a demoted one
  restarted without the flag would come back a master (by reading); either
  way at (0,1).
- **The fence then measured against (0,1).** `FLINTPROMOTE` and
  `FLINTDEMOTE` refuse an epoch at or below the stored one. With the stored
  one gone, a promotion from a controller that had been superseded long ago
  went through.
- **At runtime the same read let `set_role` measure against zero,** and
  `FLINTPROMOTE` recorded a promotion fence naming `Epoch::ZERO` as the
  lineage it superseded, before its engine watch started. Found by reading.

## Measured, 2026-11-11

Release `flint-server` of `b0285`, `--engine rocks`. One seat, 2,000 keys,
taken to epoch (0,7) by `FLINTDEMOTE 0 5` (`OK demoted at (0,5)`) and
`FLINTPROMOTE 0 7` (`OK promoted at (0,7)`, `role:master`). Killed, started
and killed again so recovery flushed everything to one 41,115-byte SST, eight
bytes of it overwritten with `0xff` at offset 100, in the first data block
(the role key, `\0flint\0role`, sorts first). Started again with
`--replica-of 127.0.0.1:1`, a flag a promoted master must ignore:

| | `FLINTINFO` | log |
|---|---|---|
| before the SST was touched | `role:master role_epoch:(0,7)` | |
| restarted | `role:replica role_epoch:(0,1)` | `storage engine read failed: Corruption: block checksum mismatch …` |
| `FLINTPROMOTE 0 2`, an epoch the fence must refuse | `OK promoted at (0,2)`, `role:master role_epoch:(0,2)` | `promoted to master at role epoch (0,2)` |

## The fix

**The role row is read checked where a decision rests on it.**
`manifest::read_role_checked` answers `Ok(None)` only when no row is stored;
a row the engine could not read is `ManifestError::Unreadable` with RocksDB's
message (through an engine watch, BUG-0284), and a row that does not decode is
`ManifestError::Corrupt`.

- **A seat that cannot read its role row does not start:** `the durable role
  row: the storage engine read failed: Corruption: … This seat cannot know its
  role or its epoch, so it does not start; replace its store from a healthy
  copy (BUG-0286)`. A seat that does not know its epoch has no fence to hold,
  and guessing one is what this bug was.
- **`set_role` refuses a transition it cannot measure,** so `FLINTPROMOTE`,
  `FLINTDEMOTE` and every other role write answer `ERR manifest: the storage
  engine read failed: …` and change nothing, durable or running.
- **`FLINTPROMOTE` reads the epoch it supersedes checked** and refuses before
  it records a promotion fence.
- `ERR manifest:` replies now carry readable text (`fenced: the stored epoch
  is …`, `the stored row does not decode`) where they printed Rust's debug
  form.

**Measured after the fix** by `tools/engine_read_fault_drill.sh` (CORE, port
6618), which now ends with the steps above: the seat at (0,7) with its first
block overwritten refused to start, naming BUG-0286. The same drill on the
binary before the fix: `FAIL (BUG-0286): the seat started without its role:
role:replica role_epoch:(0,1)`, and `FLINTPROMOTE 0 2` answered `OK promoted
at (0,2)`. The unit test
`an_unreadable_role_row_refuses_the_fence_rather_than_reading_as_none` holds
the storage half: `read_role_checked` and `set_role` are `Unreadable` on the
corrupt block, `Corrupt` on a row that does not decode, and a fresh store
still takes its first claim.

## What it does not do

- **Readers that report, or that reach the row only after startup has read
  it, still take a failed read for no role:** `FLINTINFO`'s `role_epoch`,
  `FLINTFOLLOW`'s and `FLINTSYNC`'s own epoch, the epoch a replica presents
  when it rejoins, a snapshot's epoch label. Startup now reads the row, so
  they meet a failed read only if its block goes bad while the seat runs. The
  review map lists them as open.
- **It does not repair the store.** A master that will not start fails over
  to its pair, and a replica is re-seeded; both are by hand here, and neither
  was exercised.
