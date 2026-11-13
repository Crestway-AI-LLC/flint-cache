# BUG-0287: a seat that could not read its slot records served the slots it had handed off (FIXED)

**Status:** **FIXED 2026-11-13.** Found by the error-handling review's area A
sweep, among the readers BUG-0284 left acting outside an engine watch
(`docs/error-handling-review.md`, "the slot records"), and measured on a
release binary of branch `b0285` (main `006a103c` plus BUG-0285 and
BUG-0286, neither of which touches the slot records).
**Severity:** high. A seat that had handed slots off to another node served
them again: a write to such a slot was answered `OK` and stored where the
slot's owner never sees it, and a read answered from the stale or purged copy.
It needs the block of the seat's store that holds its slot claim or its
migration records to fail its checksum, or a record not to decode: a bad
sector, a bit flipped at rest.
**Area:** `flint-storage` (`manifest`'s claim and migration readers and
fences) and `flint-server` (startup, the slot gate, `FLINTMIGRATEOUT`,
`FLINTSLOTABORT`).

## What happens

- **The slot gate was off.** A seat consults its slot's migration record on
  each keyed command only when `migration_active` is set, and startup sets it
  when `manifest::scan_all_migrations` finds a record. That scan stops at a
  block the engine cannot read (BUG-0284), so with the first records' block
  bad it found none, and the gate stayed off for every slot, readable records
  included.
- **A record read as no record.** With the gate on, `check_slot_gate` reads
  the slot's record through `Kv::get`, which answers `None` for RocksDB's
  error, and no record means serve. It runs in `admit_write_path`, before
  `execute`'s engine watch. Found by reading.
- **Startup re-claimed every slot.** A slot claim it could not read was no
  claim, so it claimed slots 0 to 16383 at epoch (0,1), and `set_claim` and
  `set_migration` measured their fences against `Epoch::ZERO`.
- **The role row did not catch it.** The claim and migration records sort
  before the role row (`claim` < `migration` < `role`), so a node with enough
  records keeps its role row in a later block, and BUG-0286's startup check
  passes.

## Measured, 2026-11-13

Release `flint-server` of `b0285`, `--engine rocks`. One seat handed off
slots 0 to 1499 (`FLINTSLOTMOVED <slot> 127.0.0.1:1`, each `OK slot … moved`).
A key in slot 3 then answered `SET` with `MOVED 3 127.0.0.1:1`. Killed, started
and killed again so recovery flushed everything to one 16,641-byte SST, eight
bytes of it overwritten with `0xff` at offset 100, in the first data block.
Started again:

| | without the overwrite (control) | with it |
|---|---|---|
| `role`, `role_epoch` | `master`, `(0,1)` | `master`, `(0,1)`: the role row was readable |
| `SET` of a key in slot 3, then `GET` | `MOVED 3 127.0.0.1:1`, both | `OK`, then the new value |
| `SET` in slots 0, 100, 700 and 1499, all handed off | `MOVED` | `OK` |
| `SET` in slot 2000, never handed off | `OK` | `OK` |
| log | | one `storage engine read failed: Corruption: block checksum mismatch …` |

## The fix

**The slot records are read checked where ownership rests on them.**
`manifest::read_claim_checked`, `read_migration_checked` and
`scan_all_migrations_checked` answer an error for a record the engine could
not read (`Unreadable`, through an engine watch) or one that does not decode
(`Corrupt`), and `Ok` with none only when none is stored.

- **A seat that cannot read its slot claim or its migration records does not
  start:** `the durable slot migration records: the storage engine read
  failed: Corruption: … This seat cannot know which slots it handed off, so it
  does not start; replace its store from a healthy copy (BUG-0287)`, as
  BUG-0286 does for the role row.
- **The slot gate turns on, never off, when the records cannot all be read,**
  and a keyed command whose slot's record cannot be read is refused: `ERR slot
  <n>'s migration record: the storage engine read failed: …; whether this node
  owns the slot is unknown, so the command is refused`. That holds for a block
  that goes bad after startup has read it.
- **`FLINTMIGRATEOUT` ships nothing, and `FLINTSLOTABORT` changes nothing,**
  for a slot whose record cannot be read, and `set_claim` and `set_migration`
  refuse a write whose fence they cannot measure.

**Measured after the fix** by `tools/engine_read_fault_drill.sh` (CORE, port
6618), which now ends with the steps above: the seat refused to start, naming
BUG-0287. The same drill on the binary before the fix: `FAIL (BUG-0287): the
seat started without its slot records, and a write to slot 3, which it handed
off, answers: +OK (GET: after)`. The unit test
`unreadable_slot_records_are_errors_rather_than_no_records` holds the storage
half.

## What it does not do

- **The refusal of a single command was not measured on a server.** A block
  that goes bad while the seat runs, after startup has read it, is what that
  path is for, and a drill cannot make one without reaching under the block
  cache; the unit test covers the checked read it rests on.
- **It does not repair the store.** A seat that will not start fails over to
  its pair, and its own copy is replaced; neither was exercised here.
- **Two readers still take an unreadable record for none:** the rebalance
  planner's view of which slots this node holds (`FLINTSLOTSTATS`, which
  would offer a handed-off slot for another move) and `FLINTMIGRATIONS`'s
  list for a recovering controller. Startup now refuses a store whose records
  cannot be read, so they meet one only if a block goes bad while the seat
  runs.
