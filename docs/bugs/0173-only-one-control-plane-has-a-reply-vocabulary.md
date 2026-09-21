# BUG-0173: only one control plane has a reply vocabulary, and the two disagree on NOPAIR (FIXED 2026-09-21)

**Status:** **FIXED 2026-09-21.** Found by a fourth pass over
[BUG-0146](0146-the-control-plane-implements-every-mutating-verb-twice.md)'s
audit, asking a question the first three did not.
**Severity:** low and latent — nothing parses the string today. It is filed
because it is a SIXTH class of the duplication BUG-0146 is about, and because
the divergence was already real on the wire.
**Found:** 2026-09-21, comparing what each dispatcher *says back* rather than
what it computes.

## The class

`main.rs` has a reply vocabulary:

```rust
fn ok() -> Value { … }
fn err(msg: &str) -> Value { Value::Error(format!("ERR {msg}")) }
```

**`ha.rs` has neither.** 77 `Value::Error` sites and 20 `Value::Simple("OK")`
sites, each written out by hand. So the `ERR ` prefix is a rule applied
automatically on one path and copied by hand on the other — the same shape as
the five classes before it, in a dimension none of them covered:

1. the apply path (ADR-0032)
2. the verb tables (`assert_cp_verbs_agree_across_paths`)
3. the refusals ([BUG-0160](0160-the-raft-control-plane-accepts-eleven-verbs-the-single-node-plane-refuses.md) — *which* verbs refuse, not how they word it)
4. the mutation constructions (BUG-0146 pass 2)
5. the side effects beside them ([BUG-0171](0171-clear-the-tenants-usage-row-on-both-control-planes.md))
6. **the reply vocabulary** — this file

## The divergence it had already produced

Same condition, two wire strings:

| path | source | wire |
|---|---|---|
| single-node | `main.rs:1170,1221` — `err("NOPAIR …")` | `-ERR NOPAIR address is not a member of any registered pair` |
| Raft | `ha.rs:807,833` — `Value::Error("NOPAIR …")` | `-NOPAIR address is not a member of any registered pair` |

**Raft is the correct one.** `NOPAIR` is an error CODE, like `LEADER`,
`SUPERSEDED` and `WRONGPASS` written bare beside it in the same file. Routing
a coded error through `err()` prepends a second code, demoting `NOPAIR` to
message text. Fixed by returning `Value::Error` directly at both sites.

**Impact is latent and the write-up should not overstate it.** Nothing parses
`NOPAIR` today; its only other occurrence in the tree is a **test fixture** in
`flint-ctl`, which encodes the Raft spelling. But prefix-matching on the code
is an established pattern here — `starts_with("SUPERSEDED")`,
`starts_with("LEADER ")`, `starts_with("ERR unknown command")` — so a consumer
written against that fixture would have been wrong on single-node.

## The guard, and the rule it took two attempts to state

`err_is_not_given_a_message_that_carries_its_own_code`.

**The first version was wrong and its failure is the useful part.** It flagged
any `err("…")` whose first word was all-caps, and fired on **forty** usage
messages: `err("CPADDPROXY <addr>")` is correct, because `ha.rs` says
`ERR CPADDPROXY <addr>` too. A verb name echoed in a usage string looks
exactly like a protocol code, and no amount of syntax can separate them.

**So the code set is DERIVED from the other dispatcher**: the bare codes
`ha.rs` emits — first words of `Value::Error("…")` that are not `ERR`. The
defect is only ever a *disagreement*, so only the other path can say which
words are codes. That is the same move as every other check this bug produced:
take the population from the authority, not from a convenient syntax.

Controls, inside the test:

- a planted `err("NOPAIR …")` must be detected — without it the test passes
  for a detector that finds nothing;
- `err("state lock")` must NOT be detected — without it, a detector widened to
  everything looks healthy;
- **scope is asserted, not assumed**: the test fails if `ha.rs` grows an
  `err(` call, because scanning `main.rs` alone is complete only while the
  helper lives in one file. BUG-0150's guard read `include_str!("main.rs")`
  while the literal it forbade sat in another file, and that is the trap this
  line exists for.

Mutation: reverting both sites to `err("NOPAIR …")` fails the test with
`["NOPAIR", "NOPAIR"]` named.

## What is NOT fixed

`ha.rs` still has no `ok()`/`err()`. Giving it one is the class's real fix and
it is ~97 call sites; it belongs to BUG-0146's ledger rather than to a
write-up about one string. **The guard above closes the direction that bit
us** — a coded error going through `err()` — and leaves the reverse (a change
to `err()` that `ha.rs` does not follow) open and stated.
