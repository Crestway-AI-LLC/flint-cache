# BUG-0150 — BUG-0065's fix, and the guard holding it shut, both lived in one of the two control planes

**Status:** **FIXED 2026-09-15** — found by auditing BUG-0146's blast radius.
Severity: high if reached. The fencing record is the split-brain guard, and the
unfixed copy is the one the HA control plane runs.

## What BUG-0065 fixed, and where

A freshly promoted master fenced itself against its own pair's stale lease. The
mechanism: **the fence wrote by member-vector EQUALITY while the renewal read
by membership CONTAINMENT**, so with two rows for one pair the fence updated
one and the renewal read the other. Its fix was two changes —

1. **One key.** `lease_row_index(rows, addr)` became the only way a lease row
   is resolved, used by the renewal read and both fence writes.
2. **The root.** `CPADDPAIR` sorts the member vector before the dedupe, so
   `a,b` and `b,a` are one pair rather than two.

— and a structural test, `no_site_resolves_a_lease_row_by_member_vector_equality`,
described in its own write-up as *"the one that holds this shut"*.

**Every one of those landed in `main.rs`.** The raft control plane got none of
them:

| | single-node (`main.rs`) | raft (`registry.rs` / `ha.rs`) |
|---|---|---|
| fence write key | `lease_row_index` (containment) | `m == &members` (**equality**) |
| lease-adopt key | — | `m == &members` (**equality**) |
| renewal read key | `lease_row_index` (containment) | `m.contains(&addr)` (containment) |
| `CPADDPAIR` canonicalises | `pair.sort()` | **no sort**, in neither the handler nor `apply` |

So the equality-write / containment-read asymmetry that *is* BUG-0065 was live
on the raft path, together with the un-canonicalised registration that creates
the duplicate rows it needs.

## The part that is the actual lesson

The structural test reads:

```rust
let whole = include_str!("main.rs");
```

**Its own forbidden literal, `m == &members`, was sitting in `registry.rs`,
twice, while it passed.** A guard that reads one of two implementations reports
on the one that was already correct — and it reports confidently, which is
worse than not reporting. BUG-0146 says the control plane implements every
mutating verb twice; this is what that costs when the thing duplicated is a
safety check.

## The fix

`lease_row_index` moved to `tenant.rs`, where both paths reach it, and all six
resolution sites go through it — three in `main.rs`, `Mutation::Fence` and
`Mutation::LeaseAdopt` in `registry.rs`, the `CPLEASE` renewal in `ha.rs`. The
raft `CPADDPAIR` handler now sorts before proposing, mirroring the single-node
one; sorted in the HANDLER rather than in `apply()` so the state machine's
behaviour on already-committed log entries is unchanged.

The structural test now reads all four control-plane sources, and additionally
asserts each path still *references* the one key — a site that quietly stops
using it is the same defect arriving by omission.

**Mutation-confirmed.** Reverting the fence to `m == &members` fails both the
structural test and the new behavioural one, and the behavioural failure prints
BUG-0065 itself:

```
the fence must UPDATE the row the renewal will read, not add a second one
beside it: [(["b:2", "a:1"], "b:2", 3), (["a:1", "b:2"], "a:1", 1)]
```

The fence wrote row 1; the renewal reads row 0 and answers `SUPERSEDED b:2` to
the master that was just fenced in. Unmutated, 41 tests pass.

## Not fixed here

See BUG-0151. Containment keys on the address being fenced, so it cannot find a
row for a member that was not in the pair when the row was written — a hole
both control planes share, and a different question from this one.

## The same shape elsewhere, swept 2026-09-15

Every source-scanning test in the workspace was checked against the question
this bug asks — *does the scan read everywhere the property lives?*

- `flint-ctl` is one file, so its scan is complete by construction.
- `flint-server` has **nine** modules and four such tests. Three are safe for a
  reason rather than by luck: two assert an ORDER between two specific
  statements in one function, and `every_arg_call_site_is_listed` scans `arg()`
  call sites, which exist only in `main.rs` (verified, not assumed).
- The fourth, `plain_process_exit_stays_out_of_the_running_paths`, was the same
  shape as this bug. BUG-0048 is a plain `std::process::exit` racing RocksDB's
  static teardown, which a sibling module can do exactly as well as `main.rs`
  can — and the scan read `include_str!("main.rs")` and nothing else.

**There was no violation hiding behind it.** All eight siblings were clean, so
this is the latent form of what BUG-0150 was the live form of. The scan now
covers all eight, with a flat expected count of zero — `main.rs` tolerates its
eight because they run on the main thread before the store is opened, and a
call out in a module cannot be shown to — plus an emptiness control, because a
scan that read nothing reports exactly as a scan that found nothing.

Mutation-confirmed: a plain exit added to `heat.rs` fails the test by name.
