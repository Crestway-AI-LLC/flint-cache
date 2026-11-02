# BUG-0277: one tenant's command with a huge count poisoned the co-processor for every tenant (FIXED)

**Status:** **FIXED 2026-11-02.** Found the same day by the error-handling review's
cross-cutting sweep (`docs/error-handling-review.md`, area N), with a
randomized run of the co-processor's planners, and measured on release
binaries. In public main (`1f82b636`) and v0.1.0-rc.86.
**Severity:** high. Nothing is lost. But one command from any tenant leaves
every tenant on that co-processor with `-COPROCUNAVAIL` for every `VEC.` and
`FT.` command, until the process is restarted. The process stays up, so a
supervisor that restarts it when it exits never does.
**Area:** `flint-vec`, the five counted-argument checks in `ft.rs` and
`ftagg.rs`, and `VEC.SEARCH`'s flat search in `lib.rs`.

## What happens

- **A count near `usize::MAX` wrapped past its length check.** `FT.CREATE`'s
  `PREFIX` and `STOPWORDS`, a `VECTOR` field's attribute count, and
  `FT.SEARCH`'s `PARAMS` each checked `args.len() < i + 2 + n`. Every counted
  list in `FT.AGGREGATE` (`LOAD`, `GROUPBY`, `SORTBY`, `PARAMS`, and each
  reducer's argument count) checked `args.len() < start + n`. With `n` at
  `usize::MAX`, or `usize::MAX - 1` where the count must be even, the sum
  wraps to just past `i`, the check passes, and the slice
  `args[i + 2..i + 2 + n]` panics because it starts after it ends.
  `FT.SEARCH`'s `RETURN` used the same sum. In a release build it did not
  panic: it read the count as an option name instead.
- **`VEC.SEARCH` sized its heap by the tenant's `k`.** The flat search made a
  heap of capacity `k + 1` before it looked at the set. A `k` past about
  2.9 × 10^17 overflows that capacity, and `Vec` panics.
- **All six panics happen under the co-processor's one store lock.** The
  co-processor plans every command while it holds `Store` (`Store::plan` and
  `Store::plan_ft`). A panic there poisons the lock, and every later
  `lock().expect("store lock")` panics too, on every connection and on the
  TTL sweep thread. The connections close, and the proxy answers each
  command `-COPROCUNAVAIL`.

## Measured, 2026-11-02

Release binaries of public `1f82b636` on a laptop. One mem-engine seat, a
proxy, and `flint-vec --ft --vec-filters`, with two tenants. Tenant B has a
vector set `s` and an index `ib`. Each case runs on a fresh co-processor:
tenant B's `VEC.SEARCH s 1,1 1` and `FT.SEARCH ib world`, then the command,
then tenant B's two again 1 s and 11 s later.

| command | its reply | tenant B after, 1 s and 11 s | co-processor threads |
|---|---|---|---|
| A: `FT.AGGREGATE ia * GROUPBY 18446744073709551615 @t` | `-COPROCUNAVAIL` | both `-COPROCUNAVAIL` | 3 → 1 |
| A: `FT.SEARCH ia * PARAMS 18446744073709551614 a b` | `-COPROCUNAVAIL` | both `-COPROCUNAVAIL` | 3 → 1 |
| A: `FT.CREATE x ... PREFIX 18446744073709551615 z: ...` | `-COPROCUNAVAIL` | both `-COPROCUNAVAIL` | 3 → 1 |
| A: `FT.CREATE x ... STOPWORDS 18446744073709551615 ...` | `-COPROCUNAVAIL` | both `-COPROCUNAVAIL` | 3 → 1 |
| A: `FT.CREATE x ... VECTOR FLAT 18446744073709551614 ...` | `-COPROCUNAVAIL` | both `-COPROCUNAVAIL` | 3 → 1 |
| B: `VEC.SEARCH s 1,1 2907332838385882365` | `-COPROCUNAVAIL` | both `-COPROCUNAVAIL` | 3 → 1 |

The co-processor's log has the first panic (`capacity overflow`, or the slice
in `ft.rs` and `ftagg.rs`), then `store lock: PoisonError` from each thread
after it, the sweep thread's among them (`main.rs:292`). The process was
alive at 11 s in every case. A restart serves both tenants again.

## The fix

**Each count is taken by slice, never by sum.** `ft.rs` has `counted(args,
i, n)`, `args.get(i + 2..)?.get(..n)`, used by all five of its counted
arguments. `ftagg.rs`'s `var_args` does the same. A count past the arguments
is refused in the words it was refused in before. **The flat search sizes its
heap by what the set can return**, `k.min(entries.len()) + 1`, so a `k` past
the set is the whole set.

After the fix, a randomized run of 4 million commands through `plan`,
`plan_ft`, `search_reply` and `aggregate_page`, each under `catch_unwind`,
found no panic. It covered well-formed and mutated `FT.SEARCH`,
`FT.AGGREGATE` (with `APPLY` and `FILTER` expressions), `FT.CURSOR`,
`FT.CREATE`, routed writes and the `VEC.` commands, over HASH and JSON
documents.

**With it, the next such panic is contained** (ops ADR-0017's D2 amendment,
accepted 2026-11-02, option B):
- **A panic while a command is planned answers that command only.** `plan`
  and `plan_ft` run under `catch_unwind` while the store's guard is held, so
  the lock is not poisoned. The command is answered `-ERR the co-processor
  failed on this command and did not run it`, and `FLINTINFO` counts it in
  `panics_total`. A panic later in a command, outside the lock, is answered
  the same way, and keeps its connection.
- **A panic that leaves a lock poisoned ends the process,** with status 70,
  saying which lock. The seat supervisor starts it again, and every namespace
  rebuilds from its durable rows. Every lock in the binary goes through one
  helper, `held`, in place of `lock().expect(..)`.
- **A rebuild chunk that panics** is counted and left for the next command to
  resume, as a failed chunk is, rather than leave its namespace answering
  `-LOADING` for good.

`coproc_vec_drill` holds both on real binaries, with the test-only
`FLINT_TEST_PANIC`: a planning panic is answered while both tenants are
served and the process stays up, and a panic in a write's commit ends the
process with status 70, after which the restart's rebuild has that write.

## Acceptance

The measurement above, run again on the fix: each command gets its refusal
(`-ERR Bad arguments for GROUPBY: Expected an argument, but none provided`,
`-ERR FT.SEARCH PARAMS <n> needs n words, in name and value pairs`, and so
on). The huge `k` gets the set's one vector. Tenant B's two commands are
served at 1 s and 11 s, the thread count stays at 3, and the log has no
panic. Unit tests, each failing before the fix:
`ft::tests::a_count_near_usize_max_is_refused_not_a_panic`,
`ftagg::tests::a_count_near_usize_max_is_refused_not_a_panic` and
`tests::a_k_past_the_set_is_the_whole_set_not_a_panic`. The containment:
the binary's `a_panic_in_planning_answers_its_command_and_poisons_nothing`,
and the drill above.
