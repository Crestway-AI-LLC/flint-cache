# BUG-0278: a BLMPOP or BZMPOP count near 2^64 closed the client's connection at the proxy (FIXED)

**Status:** **FIXED 2026-11-02.** Found the same day by the error-handling
review's cross-cutting sweep (`docs/error-handling-review.md`, area N),
looking for BUG-0277's shape in the proxy, and measured on release binaries.
In public main (`1f82b636`) and v0.1.0-rc.86.
**Severity:** low. Only the connection that sent the command is affected:
it is closed, and the client gets no reply. The proxy goes on serving every
other connection.
**Area:** `flint-proxy`, `blocking_pop`, which runs `BLMPOP` and `BZMPOP` as
one attempt per key.

## What happens

The proxy splits a `BLMPOP timeout numkeys key... LEFT|RIGHT` into one
attempt per key when the count names keys that are there. It checked
`3 + n < args.len()`. For a count of `usize::MAX - 2`, `3 + n` wraps to 0,
the check passes, and `args[3..3 + n]` panics. The panic ends the task that
serves the client, so its connection closes.

## Measured, 2026-11-02

Release binaries of public `1f82b636`: one mem-engine seat and a proxy, on a
laptop.

| command | reply | next command on that connection |
|---|---|---|
| `BLMPOP 0.1 2 a b LEFT` (control) | `*-1` | |
| `BLMPOP 0 18446744073709551613 a b LEFT` | none: the connection closed | closed |
| `BLMPOP 0 18446744073709551612 a b LEFT` | `-ERR numkeys should be greater than 0` | `PING` → `PONG` |

The proxy's log has `panicked at crates/flint-proxy/src/main.rs:5734`. A new
connection was served.

## The fix

The count is compared with what is there, `n < args.len().saturating_sub(3)`,
and never added to. A count past the keys is one attempt, the whole command,
which the seat refuses. The attempts are now built by a
function of their own, `pop_attempts`, so they can be tested.

The proxy's other counts from a client (the `FT.*` page sizes, `XREAD
BLOCK`, the blocking pops' timeouts) already use saturating arithmetic or a
bounded wait, and were read for this.

## Acceptance

`route_tests::a_blmpop_count_near_usize_max_is_one_attempt_not_a_panic`,
which panics before the fix: counts `usize::MAX - 2` to `usize::MAX` are one
attempt each, the whole command, and the control's two keys are two. The
measurement above, run again on the fix: the three counts near 2^64 each get
`-ERR numkeys should be greater than 0`, and `PING` on the same connection
answers `PONG`.
