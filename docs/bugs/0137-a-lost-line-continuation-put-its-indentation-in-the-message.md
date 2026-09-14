# BUG-0137 — a lost line continuation put its indentation inside the message

**Status:** OPEN — found 2026-09-14, not fixed here.

Three `panic!` messages in `flintctl`'s control-plane bring-up wait carry runs
of fourteen to eighteen literal spaces in the middle of a sentence. An operator
whose control plane will not come up reads this:

```
control plane seat 127.0.0.1:7620 (cp-1) did not answer PING in 10s,
                  but its PROCESS IS RUNNING — it started and something is
                  holding it. Not a slow start: 10s is ~370x the measured
                  23-27ms spawn-to-PONG.
```

— except not on four lines. It is one line, and the spaces are inside it.

`crates/flint-ctl/src/main.rs:3921`, `:3924`, `:3930`, and a test assertion at
`:8481`.

## Why this is not cosmetic

These particular three messages are the ones that exist to tell three causes
apart, and the comment above them says so: *"Naming either of the two above
here would be a guess with a fact's grammar, and the guess this code made was
the confident one."* The care went into distinguishing "the process is running
and something is holding it" from "nothing is running" from "we could not ask".
That distinction is the value of the message, and it is delivered mid-sentence
through a gap wide enough to read as two separate statements.

## The mechanism, which is worth knowing because it recurs

A Rust string literal split across source lines with a trailing `\` drops the
newline **and the leading whitespace of the next line**. Without the backslash
the indentation is content. Both spellings look almost identical in a diff, and
`rustfmt` then joins the literal onto one long line — so the source stops
looking multi-line at all and the runs become invisible at a glance.

This was found while checking my own strings for exactly this accident during
the ADR-0043 surface gate: a generated edit consumed my backslashes and
produced the identical artifact, caught only because a test asserted on a
phrase that the space run had split. Same shape, same invisibility.

## What is NOT this bug

A regex for "five or more spaces mid-sentence" flags 28 lines across 6 files in
the public repo. Four are this defect. The rest are **deliberate column
alignment** — `flintctl`'s usage block and `status` output, `flint-server`'s
`--help`, a throughput table in a comment — where the runs are the point.

So this is not gateable as written, and a check that flagged all 28 would be
worse than no check. If it is worth a guard at all, the discriminator is
narrower: a space run inside a `panic!`/`eprintln!` string that is not preceded
by `\n`. Filed as a judgement call rather than pretending otherwise.

## A second, ambiguous class, not counted above

`crates/flint-chaos/src/cluster.rs:551` and `:565` carry `\n` followed by 19-27
spaces in two `REFUSING TO RUN` messages. That renders as a deeply indented
continuation line rather than a garbled sentence, and the indent happens to
match the source's own. It is either intended or the same leak; whoever wrote
it can say in a sentence, and nobody else should guess.

## Fix

Restore the continuations. No behaviour change, no test change, and the
messages say what they were written to say.
